//! `mpg://`: an ISO/IEC 13818-1 program stream written from the PES IR (mpg-output-design
//! v5 §2, L2), one more `pes::Stream` sink.
//!
//! The pack layer is regenerated: packs, SCR, mux rate, system header, PSM, PES headers
//! and DTS. ES bytes are kept (DVD LPCM is re-packed losslessly, G8); PTS is kept exactly,
//! modulo one integer-tick origin (J16). J24: the carriage is the DVD core the 2000 edition
//! of H.222.0 covers (MPEG-1/2 video, MPEG audio and its 13818-3 extension, AC-3, DTS,
//! LPCM, subpictures); everything else is excluded with a reason, never silently.

mod fit;
pub(crate) mod pack;
mod pstd;
#[cfg(test)]
mod replay;
#[cfg(test)]
mod tests;

pub(crate) use fit::plan;
use fit::{Carriage, PrivateKind};
use pack::{Bound, PsmEntry, SubStreamInfo};
use pstd::{Au, BufferSpec, Mux, Payload, PstdCounters, StreamSpec};

use crate::disc::{Codec, DiscTitle, Stream as DiscStream};
use crate::mux::codec::ns_to_ticks;
use crate::mux::decode_ts::{DtsCounters, DtsDeriver};
use crate::pes::{PesFrame, Stream};
use std::collections::VecDeque;
use std::io::{self, Write};

/// Design §2.3 "every output tick is `input tick − lowest + 135 000` (1.5 s)".
const ORIGIN_TICKS: i64 = 135_000;
/// Design §2.3: "The sink queues ≥ 1 s of IR before its first pack".
const WINDOW_TICKS: i64 = 90_000;
/// The start-up hold cap (design §2.3 "1 s of IR timeline or 64 MiB").
const HOLD_CAP_BYTES: usize = 64 * 1024 * 1024;
/// Arrival-domain bias: ticks are held as `tick − first tick + BIAS`, positive for any
/// source that starts within ~13 h of its first frame, so the deriver never clamps.
const BIAS: i64 = 1 << 32;
/// R0 (design §2.4 step 5): 10.08 Mbit/s for SD, 128 Mbit/s otherwise, in 50 B/s units.
const R0_SD: u32 = 10_080_000 / 8 / 50;
const R0_HD: u32 = 128_000_000 / 8 / 50;
/// MPEG audio and extension buffers: 16 KiB, scale 0 (design §2.4 table).
const AUDIO_BOUND: u16 = 16 * 1024 / 128;
/// `0xBD`: the largest 13-bit bound, scale 1 (J11).
const PRIVATE_BOUND: u16 = 8191;
/// Video buffer when no `vbv_buffer_size` parses: the same 13-bit maximum.
const VIDEO_BOUND_MAX: u64 = 8191 * 1024;

/// Anomalies an `mpg://` mux resolved and counted (design §2.3-§2.4).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MpgCounters {
    pub dts: DtsCounters,
    pub pstd: PstdCounters,
    /// A track first seen after the origin window with a lower timestamp: saturated to 0.
    pub origin_saturated: u64,
}

// One output stream's static facts.
#[derive(Debug, Clone)]
struct Out {
    track: usize,
    spec: StreamSpec,
    kind: OutKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutKind {
    Video { mpeg1: bool },
    MpegAudio { has_ext: bool },
    Extension { base_out: usize },
    Private(PrivateKind),
}

// An AU in the arrival domain (ticks + BIAS), before the origin.
struct RelAu {
    pts: i64,
    dts: Option<i64>,
    mark: usize,
    data: Vec<u8>,
    lpcm_bits: u8,
}

#[derive(Default)]
struct LpcmState {
    carry: Vec<u8>,
    carry_pts: i64,
    bits: u8,
}

/// The `mpg://` sink.
pub struct MpgSink<W: Write + Send> {
    title: DiscTitle,
    route: Vec<Option<usize>>,
    outs: Vec<Out>,
    buffers: Vec<BufferSpec>,
    video_out: usize,
    deriver: DtsDeriver,
    anchor: Option<i64>,
    armed: bool,
    params_prepended: bool,
    pending_video: VecDeque<(i64, usize, Vec<u8>)>,
    window: Vec<(usize, RelAu)>,
    window_bytes: usize,
    span: Option<(i64, i64)>,
    offset: Option<i64>,
    lpcm: Vec<LpcmState>,
    writer: Option<W>,
    mux: Option<Mux<W>>,
    excluded: super::ps::UnstoredExtensions,
    origin_saturated: u64,
    frames: u64,
    finished: bool,
}

// The offset of the first picture start code (`00 00 01 00`): where a video AU commences
// for MS-15/MS-19. 0 when the AU has none.
fn picture_start(data: &[u8]) -> usize {
    data.windows(4).position(|w| w == [0, 0, 1, 0]).unwrap_or(0)
}

// `vbv_buffer_size` (with its sequence-extension high bits) in bytes: 13818-2 counts it in
// 16 384-bit units.
fn vbv_bytes(es: &[u8]) -> Option<u64> {
    let seq = es.windows(4).position(|w| w == [0, 0, 1, 0xB3])? + 4;
    let h = es.get(seq..seq + 8)?;
    // 12 + 12 + 4 + 4 + 18 + 1 = 51 bits precede the 10-bit vbv_buffer_size_value.
    let bits = u64::from_be_bytes(h.try_into().ok()?);
    let low = (bits >> (64 - 61)) & 0x3FF;
    let mut high = 0;
    let mut p = seq;
    while let Some(off) = es[p..].windows(4).position(|w| w == [0, 0, 1, 0xB5]) {
        let e = p + off + 4;
        if es.get(e).is_some_and(|b| b >> 4 == 1) {
            high = u64::from(*es.get(e + 4)?);
            break;
        }
        p = e;
    }
    Some(((high << 10) | low) * 2048)
}

// The 16 RGB entries of a VobSub `.idx` `palette:` line (the DvdSub codec_data).
fn idx_palette(codec_data: &[u8]) -> Option<[[u8; 3]; 16]> {
    let text = std::str::from_utf8(codec_data).ok()?;
    let line = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("palette:"))?;
    let mut out = [[0u8; 3]; 16];
    let mut n = 0;
    for hex in line.split(',').map(str::trim) {
        let v = u32::from_str_radix(hex, 16).ok()?;
        *out.get_mut(n)? = [(v >> 16) as u8, (v >> 8) as u8, v as u8];
        n += 1;
    }
    (n == 16).then_some(out)
}

fn lang3(lang: &str) -> [u8; 3] {
    match lang.as_bytes() {
        [a, b, c] if lang.bytes().all(|x| x.is_ascii_alphabetic()) => [*a, *b, *c],
        _ => *b"und",
    }
}

impl<W: Write + Send> MpgSink<W> {
    /// Plan `title`; `MpgNoVideoTrack` (E9074) when no carriable video is left (design §0).
    pub fn create(writer: W, title: &DiscTitle) -> io::Result<Self> {
        let plan = plan(title);
        let Some(video_track) = plan
            .carriage
            .iter()
            .position(|c| matches!(c, Some(Carriage::Video { .. })))
        else {
            return Err(crate::error::Error::MpgNoVideoTrack.into());
        };
        let mut outs: Vec<Out> = Vec::new();
        let mut buffers: Vec<BufferSpec> = Vec::new();
        let mut route = vec![None; title.streams.len()];
        let mut private_buffer = None;
        // Video first, then every other carried track in track order.
        let order = std::iter::once(video_track)
            .chain((0..title.streams.len()).filter(|&i| i != video_track));
        for i in order {
            let Some(c) = plan.carriage[i] else { continue };
            let (stream_id, payload, kind, sparse) = match c {
                Carriage::Video { mpeg1 } => (
                    pack::VIDEO_ID,
                    Payload::Plain,
                    OutKind::Video { mpeg1 },
                    false,
                ),
                Carriage::MpegAudio { stream_id } => {
                    let has_ext = plan.carriage.iter().any(
                        |c| matches!(c, Some(Carriage::Mp2Extension { base, .. }) if *base == i),
                    );
                    (
                        stream_id,
                        Payload::Plain,
                        OutKind::MpegAudio { has_ext },
                        false,
                    )
                }
                Carriage::Mp2Extension { stream_id, base } => {
                    let base_out = route[base].unwrap_or(0);
                    (
                        stream_id,
                        Payload::WholeAu,
                        OutKind::Extension { base_out },
                        false,
                    )
                }
                Carriage::Private { sub_id, kind } => {
                    let payload = match kind {
                        PrivateKind::Ac3 | PrivateKind::Dts => Payload::Frames { sub_id },
                        PrivateKind::Lpcm { channels, rate } => Payload::Lpcm {
                            sub_id,
                            channels,
                            rate,
                        },
                        PrivateKind::Subpicture => Payload::SubId { sub_id },
                    };
                    (
                        pack::PRIVATE_STREAM_1,
                        payload,
                        OutKind::Private(kind),
                        kind == PrivateKind::Subpicture,
                    )
                }
            };
            let buffer = match kind {
                OutKind::Private(_) => *private_buffer.get_or_insert_with(|| {
                    buffers.push(BufferSpec {
                        stream_id: pack::PRIVATE_STREAM_1,
                        scale_1024: true,
                        size: PRIVATE_BOUND,
                    });
                    buffers.len() - 1
                }),
                OutKind::Video { .. } => {
                    buffers.push(BufferSpec {
                        stream_id,
                        scale_1024: true,
                        size: 8191,
                    });
                    buffers.len() - 1
                }
                _ => {
                    buffers.push(BufferSpec {
                        stream_id,
                        scale_1024: false,
                        size: AUDIO_BOUND,
                    });
                    buffers.len() - 1
                }
            };
            route[i] = Some(outs.len());
            let av = !sparse;
            outs.push(Out {
                track: i,
                spec: StreamSpec {
                    stream_id,
                    payload,
                    buffer,
                    sparse,
                    av,
                },
                kind,
            });
        }
        let codec = match &title.streams[video_track] {
            DiscStream::Video(v) => v.codec,
            _ => Codec::Mpeg2,
        };
        let cp = title
            .codec_privates
            .get(video_track)
            .and_then(|c| c.as_deref());
        let mut excluded = super::ps::UnstoredExtensions::new(title, "MPG");
        excluded.retain(|i| route[i].is_none());
        Ok(Self {
            title: title.clone(),
            lpcm: (0..outs.len())
                .map(|_| LpcmState {
                    bits: 16,
                    ..Default::default()
                })
                .collect(),
            route,
            outs,
            buffers,
            video_out: 0,
            deriver: DtsDeriver::for_codec(codec, cp),
            anchor: None,
            armed: false,
            params_prepended: false,
            pending_video: VecDeque::new(),
            window: Vec::new(),
            window_bytes: 0,
            span: None,
            offset: None,
            writer: Some(writer),
            mux: None,
            excluded,
            origin_saturated: 0,
            frames: 0,
            finished: false,
        })
    }

    /// Anomalies counted so far.
    pub(crate) fn counters(&self) -> MpgCounters {
        MpgCounters {
            dts: self.deriver.counters(),
            pstd: self.mux.as_ref().map(Mux::counters).unwrap_or_default(),
            origin_saturated: self.origin_saturated,
        }
    }

    fn rel(&mut self, pts_ns: i64) -> i64 {
        let ticks = ns_to_ticks(pts_ns);
        let anchor = *self.anchor.get_or_insert(ticks);
        ticks.saturating_sub(anchor).saturating_add(BIAS)
    }

    // An AU resolved in the arrival domain: into the origin window, or on to the mux.
    fn accept(&mut self, out: usize, au: RelAu) -> io::Result<()> {
        let Some(offset) = self.offset else {
            self.window_bytes += au.data.len();
            self.window.push((out, au));
            return Ok(());
        };
        let pts = au.pts + offset;
        let dts = au.dts.map(|d| d + offset);
        if pts < 0 || dts.is_some_and(|d| d < 0) {
            self.origin_saturated += 1;
        }
        let pts = pts.max(0) as u64;
        let dts = dts.map(|d| d.max(0) as u64).filter(|&d| d != pts);
        let mux = self
            .mux
            .as_mut()
            .expect("the mux exists once the origin is set");
        mux.push(
            out,
            Au {
                pts,
                dts,
                mark: au.mark,
                data: au.data,
                lpcm_bits: au.lpcm_bits,
            },
        );
        Ok(())
    }

    fn drain_video(&mut self) -> io::Result<()> {
        while let Some(d) = self.deriver.pop() {
            let Some((pts, mark, data)) = self.pending_video.pop_front() else {
                break;
            };
            let au = RelAu {
                pts,
                dts: d,
                mark,
                data,
                lpcm_bits: 0,
            };
            self.accept(self.video_out, au)?;
        }
        Ok(())
    }

    // LPCM AUs are whole packing units at a sticky lossless depth (G8, design §2.3 MPG4-6).
    fn lpcm_aus(&mut self, out: usize, rel: i64, data: &[u8], flush: bool) -> Vec<RelAu> {
        let Payload::Lpcm { channels, rate, .. } = self.outs[out].spec.payload else {
            return Vec::new();
        };
        let st = &mut self.lpcm[out];
        let frame = channels * 3;
        if !data.is_empty() {
            // Re-anchor on every frame: the carried samples end where this frame starts.
            let carried = (st.carry.len() / frame) as i64;
            st.carry_pts = rel - (carried * 90_000 + i64::from(rate / 2)) / i64::from(rate);
        }
        st.carry.extend_from_slice(data);
        st.bits = st
            .bits
            .max(crate::mux::codec::lpcm::dvd_bits_needed(&st.carry));
        let unit = crate::mux::codec::lpcm::dvd_unit_frames(channels, st.bits) * frame;
        if flush && !st.carry.len().is_multiple_of(unit) {
            // EOF: pad the last partial unit (< 4 sample frames) with silence.
            let pad = unit - st.carry.len() % unit;
            st.carry.resize(st.carry.len() + pad, 0);
        }
        let whole = st.carry.len() - st.carry.len() % unit;
        if whole == 0 {
            return Vec::new();
        }
        let mut es = Vec::with_capacity(whole);
        crate::mux::codec::lpcm::dvd_pack(&st.carry[..whole], channels, st.bits, &mut es);
        let au = RelAu {
            pts: st.carry_pts,
            dts: None,
            mark: 0,
            data: es,
            lpcm_bits: st.bits,
        };
        st.carry.drain(..whole);
        // The carried samples start after the AU just cut.
        let frames = (whole / frame) as i64;
        st.carry_pts += (frames * 90_000 + i64::from(rate / 2)) / i64::from(rate);
        vec![au]
    }

    fn maybe_set_origin(&mut self, eof: bool) -> io::Result<()> {
        if self.offset.is_some() {
            return Ok(());
        }
        let pending: usize = self.pending_video.iter().map(|p| p.2.len()).sum();
        let spanned = self.span.is_some_and(|(lo, hi)| hi - lo >= WINDOW_TICKS);
        if self.window_bytes + pending > HOLD_CAP_BYTES {
            self.deriver.release_cap();
            self.drain_video()?;
        } else if !eof && !(spanned && !self.deriver.pending()) {
            return Ok(());
        }
        // Design §2.3 (MPG3-8): the lowest first DTS/PTS over ALL tracks maps to 1.5 s.
        let Some(lowest) = self
            .window
            .iter()
            .map(|(_, a)| a.dts.unwrap_or(a.pts).min(a.pts))
            .min()
        else {
            return Ok(());
        };
        self.offset = Some(ORIGIN_TICKS - lowest);
        let first = self.first_pack_prefix();
        let r0 = match &self.title.streams[self.outs[self.video_out].track] {
            DiscStream::Video(v) if v.resolution.pixels().is_some_and(|(_, h)| h <= 576) => R0_SD,
            _ => R0_HD,
        };
        let specs = self.outs.iter().map(|o| o.spec.clone()).collect();
        let writer = self.writer.take().expect("the writer is handed over once");
        self.mux = Some(Mux::new(writer, specs, self.buffers.clone(), first, r0));
        for (out, au) in std::mem::take(&mut self.window) {
            self.accept(out, au)?;
        }
        Ok(())
    }

    // The system header and program stream map of the first pack (MS-21 §2.7.8).
    fn first_pack_prefix(&mut self) -> Vec<u8> {
        // J23: an extension with no packets in the origin window is not described; its
        // packets, should they come later, are left out and reported like an orphan's.
        let unseen: Vec<usize> = (0..self.outs.len())
            .filter(|&o| matches!(self.outs[o].kind, OutKind::Extension { .. }))
            .filter(|&o| self.window.iter().all(|(w, _)| *w != o))
            .collect();
        for &o in &unseen {
            let track = self.outs[o].track;
            self.route[track] = None;
            if let DiscStream::Audio(a) = &self.title.streams[track] {
                self.excluded.add(track, a.pid);
            }
        }
        let unseen_buffers: Vec<usize> = unseen.iter().map(|&o| self.outs[o].spec.buffer).collect();
        let ext_of = |base: usize| {
            (0..self.outs.len()).any(|o| {
                matches!(self.outs[o].kind, OutKind::Extension { base_out } if base_out == base)
                    && !unseen.contains(&o)
            })
        };
        let first_au = |out: usize| self.window.iter().find(|(o, _)| *o == out).map(|(_, a)| a);
        // MS-7/MS-13: the video bound from vbv_buffer_size + 8 KiB (design §2.4 table).
        let video_track = self.outs[self.video_out].track;
        let cp = self
            .title
            .codec_privates
            .get(video_track)
            .and_then(|c| c.as_deref());
        let vbv = first_au(self.video_out)
            .and_then(|a| vbv_bytes(&a.data))
            .or_else(|| cp.and_then(vbv_bytes));
        let bs = vbv.map_or(VIDEO_BOUND_MAX, |b| (b + 8192).min(VIDEO_BOUND_MAX));
        let vb = self.outs[self.video_out].spec.buffer;
        self.buffers[vb].size = bs.div_ceil(1024) as u16;
        let mut entries: Vec<PsmEntry> = Vec::new();
        let mut subs: Vec<SubStreamInfo> = Vec::new();
        let mut palette = None;
        let mut private = false;
        let mut audio_bound = 0u8;
        for (oi, o) in self.outs.iter().enumerate() {
            let s = &self.title.streams[o.track];
            match o.kind {
                OutKind::Video { mpeg1 } => entries.push(PsmEntry {
                    // MS-22: 0x01 ISO/IEC 11172 Video, 0x02 H.262 | 13818-2 Video.
                    stream_type: if mpeg1 { 0x01 } else { 0x02 },
                    stream_id: o.spec.stream_id,
                    descriptors: Vec::new(),
                }),
                OutKind::MpegAudio { has_ext } => {
                    let has_ext = has_ext && ext_of(oi);
                    audio_bound += 1;
                    // MS-22: 0x03 11172 Audio (ID bit 1), 0x04 13818-3 Audio (LSF, or a base
                    // whose multichannel extension is carried).
                    let mpeg1 =
                        first_au(oi).is_none_or(|a| a.data.get(1).is_none_or(|b| b & 0x08 != 0));
                    let mut d = Vec::new();
                    if let DiscStream::Audio(a) = s
                        && let Some(l) = pack::iso639_descriptor(&a.language)
                    {
                        d.extend_from_slice(&l);
                    }
                    let n = o.spec.stream_id & 0x07;
                    if has_ext {
                        // MS-23: the base layer, hierarchy_type 15.
                        d.extend_from_slice(&pack::hierarchy_descriptor(
                            pack::HIERARCHY_BASE,
                            2 * n,
                            0,
                        ));
                    }
                    let stream_type = if has_ext || !mpeg1 { 0x04 } else { 0x03 };
                    entries.push(PsmEntry {
                        stream_type,
                        stream_id: o.spec.stream_id,
                        descriptors: d,
                    });
                }
                OutKind::Extension { .. } if unseen.contains(&oi) => {}
                OutKind::Extension { .. } => {
                    audio_bound += 1;
                    let n = o.spec.stream_id & 0x07;
                    // MS-23: type 5 "ISO/IEC 13818-3 Extension bitstream", embedded = its base.
                    let d = pack::hierarchy_descriptor(pack::HIERARCHY_EXTENSION, 2 * n + 1, 2 * n)
                        .to_vec();
                    entries.push(PsmEntry {
                        stream_type: 0x04,
                        stream_id: o.spec.stream_id,
                        descriptors: d,
                    });
                }
                OutKind::Private(kind) => {
                    private = true;
                    let sub_id = match o.spec.payload {
                        Payload::Frames { sub_id }
                        | Payload::Lpcm { sub_id, .. }
                        | Payload::SubId { sub_id } => sub_id,
                        _ => 0,
                    };
                    let (lang, forced) = match s {
                        DiscStream::Audio(a) => (lang3(&a.language), false),
                        DiscStream::Subtitle(t) => {
                            if palette.is_none() {
                                palette = t.codec_data.as_deref().and_then(idx_palette);
                            }
                            (lang3(&t.language), t.forced)
                        }
                        DiscStream::Video(_) => (*b"und", false),
                    };
                    if kind != PrivateKind::Subpicture {
                        audio_bound += 1;
                    }
                    subs.push(SubStreamInfo {
                        sub_id,
                        lang,
                        forced,
                    });
                }
            }
        }
        if private {
            // MS-22: 0x06 "PES packets containing private data"; the map cannot name sub-streams.
            entries.push(PsmEntry {
                stream_type: 0x06,
                stream_id: pack::PRIVATE_STREAM_1,
                descriptors: Vec::new(),
            });
        }
        let bounds: Vec<Bound> = (self.buffers.iter().enumerate())
            .filter(|(i, _)| !unseen_buffers.contains(i))
            .map(|(_, b)| Bound {
                stream_id: b.stream_id,
                scale_1024: b.scale_1024,
                size: b.size,
            })
            .collect();
        let mut prefix = pack::system_header(audio_bound, 1, &bounds);
        let info = pack::fmkv_descriptors(&subs, palette.as_ref());
        // Design §2.2: the worst case is ~420 B, under the 1018-byte cap (MS-8); a map that
        // cannot fit drops the FMKV table rather than the stream map itself.
        let map = pack::psm(&info, &entries).or_else(|| pack::psm(&[], &entries));
        if let Some(m) = map {
            prefix.extend_from_slice(&m);
        }
        prefix
    }
}

impl<W: Write + Send> Stream for MpgSink<W> {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        if self.finished {
            return Err(crate::error::Error::StreamClosed.into());
        }
        let Some(out) = self.route.get(frame.track).copied().flatten() else {
            self.excluded.drop_frame(frame.track);
            return Ok(());
        };
        // B1: an empty frame (a Matroska empty Block) carries no access unit.
        if frame.data.is_empty() {
            return Ok(());
        }
        let is_video = out == self.video_out;
        // Drop video before the first keyframe: nothing decodes without it (tsmux's guard).
        if is_video && !frame.keyframe && !self.armed {
            return Ok(());
        }
        let rel = self.rel(frame.pts);
        self.span = Some(
            self.span
                .map_or((rel, rel), |(lo, hi)| (lo.min(rel), hi.max(rel))),
        );
        self.frames += 1;
        if is_video {
            self.armed = true;
            let mut data = frame.data.clone();
            if !self.params_prepended {
                self.params_prepended = true;
                // 13818-2 decoding starts at a sequence header; a source that moved it to
                // codec_private (Matroska) gets it back ahead of the first picture.
                let pic = picture_start(&data);
                let has_seq = data[..pic].windows(4).any(|w| w == [0, 0, 1, 0xB3]);
                let cp = self
                    .title
                    .codec_privates
                    .get(frame.track)
                    .and_then(|c| c.clone());
                if let (false, Some(cp)) = (has_seq, cp)
                    && cp.windows(4).any(|w| w == [0, 0, 1, 0xB3])
                {
                    data.splice(0..0, cp);
                }
            }
            self.deriver.push(rel, &data);
            let mark = picture_start(&data);
            self.pending_video.push_back((rel, mark, data));
            self.drain_video()?;
        } else if matches!(self.outs[out].spec.payload, Payload::Lpcm { .. }) {
            for au in self.lpcm_aus(out, rel, &frame.data, false) {
                self.accept(out, au)?;
            }
        } else {
            let au = RelAu {
                pts: rel,
                dts: None,
                mark: 0,
                data: frame.data.clone(),
                lpcm_bits: 0,
            };
            self.accept(out, au)?;
        }
        self.maybe_set_origin(false)?;
        if let Some(m) = self.mux.as_mut() {
            m.pump(false)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.deriver.finish();
        self.drain_video()?;
        for out in 0..self.outs.len() {
            let rel = self.lpcm[out].carry_pts;
            if !self.lpcm[out].carry.is_empty() {
                for au in self.lpcm_aus(out, rel, &[], true) {
                    self.accept(out, au)?;
                }
            }
        }
        // Design §2.3 EOF: a short input is written, not failed; zero frames is MuxEmpty.
        if self.frames == 0 {
            return Err(crate::error::Error::MuxEmpty.into());
        }
        self.maybe_set_origin(true)?;
        let c = self.counters();
        if c != MpgCounters::default() {
            tracing::warn!(
                target: "mux",
                dts_order_violations = c.dts.order_violations,
                dts_hold_overflow = c.dts.hold_overflow,
                pstd_late_aus = c.pstd.late_aus,
                pts_gap_over_0_7s = c.pstd.pts_gaps,
                interleave_cap = c.pstd.interleave_cap,
                origin_saturated = c.origin_saturated,
                "mpg: program stream needed corrections (counted, not refused)"
            );
        }
        match self.mux.as_mut() {
            Some(m) => m.finish(),
            None => Err(crate::error::Error::MuxEmpty.into()),
        }
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn undelivered_streams(&self) -> Vec<usize> {
        // An extension whose base is not carried, once its packets arrived (J23).
        self.excluded.seen()
    }
}

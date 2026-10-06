//! M2tsStream — BD transport stream write sink.
//!
//! Write: prepends FMKV metadata header, then muxes PES frames into
//! BD-TS. The read direction lives on the pipeline highway —
//! `m2ts://` URLs route through
//! [`super::resolve::input`] → `build_m2ts_pipeline` →
//! [`super::pipelined_stream::PipelinedPesStream`], so this type is
//! write-only.

use super::meta;
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Write};

/// The BD LPCM header an `m2ts://` sink re-packs an LPCM track with, or `None` when BD
/// LPCM cannot carry its layout/rate and `create` leaves the track out. Shared with the
/// pre-mux plan ([`super::fit::fit_report`]) so the two cannot disagree.
pub(crate) fn lpcm_bd_header(a: &crate::disc::AudioStream, cp: Option<&[u8]>) -> Option<[u8; 2]> {
    let src = cp.and_then(super::codec::lpcm::layout_byte);
    let depth = super::codec::lpcm::output_depth(cp);
    super::codec::lpcm::bd_header(a.channels.count(), a.sample_rate.hz() as u32, src, depth)
}

/// The ADTS header an `m2ts://` sink re-frames an AAC track with, or `None` when its
/// AudioSpecificConfig is missing or not signallable in ADTS and `create` leaves it out.
pub(crate) fn aac_adts_template(cp: Option<&[u8]>) -> Option<[u8; 7]> {
    cp.and_then(super::codec::adts::adts_template)
}

// PMT stream_type (13818-1 Table 2-34; BD-ROM HDMV types under the HDMV registration);
// codecs with no TS mapping are PES private data (0x06).
fn stream_type(s: &DiscStream) -> u8 {
    use crate::disc::Codec as C;
    match s {
        DiscStream::Video(v) => match v.codec {
            C::H264 if v.is_mvc_dependent() => 0x20,
            C::H264 => 0x1B,
            C::Hevc => 0x24,
            C::Vc1 => 0xEA,
            C::Mpeg2 => 0x02,
            C::Mpeg1 => 0x01,
            _ => 0x06,
        },
        DiscStream::Audio(a) => match a.codec {
            C::Lpcm => 0x80,
            C::Ac3 => 0x81,
            C::Dts => 0x82,
            C::TrueHd => 0x83,
            C::Ac3Plus if a.secondary => 0xA1,
            C::Ac3Plus => 0x84,
            C::DtsHdHr if a.secondary => 0xA2,
            C::DtsHdHr => 0x85,
            C::DtsHdMa => 0x86,
            C::Aac => 0x0F,
            // As FFmpeg declares MPEG audio; its readers take 0x03 for either part.
            C::Mp2 | C::Mp3 => 0x03,
            _ => 0x06,
        },
        DiscStream::Subtitle(t) => match t.codec {
            C::Pgs => 0x90,
            _ => 0x06,
        },
    }
}

/// How the sink re-frames a track's IR frames for BD-TS.
#[derive(Clone, Copy)]
enum Repack {
    Verbatim,
    /// IR PCM → BD LPCM under this header.
    Lpcm([u8; 2]),
    /// Raw AAC access units → ADTS under this header.
    Adts([u8; 7]),
}

fn stream_pid(s: &DiscStream) -> u16 {
    match s {
        DiscStream::Video(v) => v.pid,
        DiscStream::Audio(a) => a.pid,
        DiscStream::Subtitle(s) => s.pid,
    }
}

// tsmux picks stream_id and NAL handling by PID, so video must sit in its video range and
// nothing else may; DVD ids (0xE0, 0xBD80) and the null PID are not carriable at all.
fn pid_fits(s: &DiscStream, pid: u16) -> bool {
    let video = matches!(s, DiscStream::Video(_));
    video == super::tsmux::is_video_pid(pid)
        && (0x0010..=0x1FFE).contains(&pid)
        && pid != super::tsmux::PMT_PID
        && pid != super::tsmux::PCR_PID
}

// Lowest free carriable PID for `s`, searched from its BD base.
fn free_pid(s: &DiscStream, used: &[bool]) -> Option<u16> {
    let base = match s {
        DiscStream::Video(_) => *super::tsmux::VIDEO_PID_RANGE.start(),
        DiscStream::Audio(_) => 0x1100,
        DiscStream::Subtitle(_) => 0x1200,
    };
    (base..=0x1FFE)
        .chain(0x0010..base)
        .find(|&p| pid_fits(s, p) && !used[p as usize])
}

/// BD transport stream write sink with embedded FMKV metadata
/// header.
pub struct M2tsStream {
    disc_title: DiscTitle,
    muxer: super::tsmux::TsMuxer<Box<dyn Write + Send>>,
    /// Per input track: output track index (None = dropped at create) and its re-framing.
    route: Vec<Option<(usize, Repack)>>,
    /// MPEG-2 multichannel extension tracks: no PMT descriptor binds them to their base.
    excluded: super::ps::UnstoredExtensions,
    /// Clip-join PTS correction shared with the other sinks: a multi-clip playlist's
    /// source PTS resets or jumps at each join, and one TS carries one timeline.
    timeline: super::timeline::TimelineContinuity,
    /// The input track (base-view video) that drives the timeline's epochs.
    ref_video: Option<usize>,
    /// Frames the timeline placed (the denominator for its drop count).
    frames_mapped: u64,
}

impl M2tsStream {
    /// Create for writing PES frames → BD-TS output.
    /// Writes FMKV metadata header, then muxes PES frames into BD transport stream.
    /// LPCM that BD LPCM can't carry (e.g. 44.1 kHz DVD) is dropped from the output.
    pub fn create(mut writer: impl Write + Send + 'static, title: &DiscTitle) -> io::Result<Self> {
        let mut out = title.clone();
        // The output is BD-TS (LPCM repacked) whatever the source; the header says so.
        out.content_format = crate::disc::ContentFormat::BdTs;
        out.streams.clear();
        out.codec_privates.clear();
        let mut route = Vec::with_capacity(title.streams.len());
        let excluded = super::ps::UnstoredExtensions::new(title, "M2TS");
        // Keep each carriable source PID (first use wins); the rest are remapped below and
        // the header records the PID actually written.
        let mut used = vec![false; 0x2000];
        let keep: Vec<bool> = title
            .streams
            .iter()
            .map(|s| {
                let pid = stream_pid(s);
                let k = pid_fits(s, pid) && !used[pid as usize];
                if k {
                    used[pid as usize] = true;
                }
                k
            })
            .collect();
        for (i, s) in title.streams.iter().enumerate() {
            let mut cp = title.codec_privates.get(i).cloned().flatten();
            // No descriptor binds a 13818-3 extension PID to its base, so a player could not
            // re-pair it: left out, and reported like refused LPCM.
            if excluded.contains(i) {
                route.push(None);
                continue;
            }
            let repack = match s {
                DiscStream::Audio(a) if a.codec == crate::disc::Codec::Aac => {
                    let Some(h) = aac_adts_template(cp.as_deref()) else {
                        tracing::warn!(
                            target: "mux",
                            track = i,
                            "AAC config not representable as ADTS; track omitted from M2TS"
                        );
                        route.push(None);
                        continue;
                    };
                    Repack::Adts(h)
                }
                DiscStream::Audio(a) if a.codec == crate::disc::Codec::Lpcm => {
                    let Some(h) = lpcm_bd_header(a, cp.as_deref()) else {
                        tracing::warn!(
                            target: "mux",
                            track = i,
                            "LPCM layout/rate not representable as BD LPCM; track omitted from M2TS"
                        );
                        route.push(None);
                        continue;
                    };
                    // Advertise the layout actually packed, not a rejected source byte.
                    let depth = super::codec::lpcm::output_depth(cp.as_deref());
                    cp = Some(super::codec::lpcm::tagged_layout(h[0], depth));
                    Repack::Lpcm(h)
                }
                _ => Repack::Verbatim,
            };
            let mut s = s.clone();
            if !keep[i] {
                let Some(pid) = free_pid(&s, &used) else {
                    tracing::warn!(target: "mux", track = i, "no free BD-TS PID; track omitted from M2TS");
                    route.push(None);
                    continue;
                };
                used[pid as usize] = true;
                match &mut s {
                    DiscStream::Video(v) => v.pid = pid,
                    DiscStream::Audio(a) => a.pid = pid,
                    DiscStream::Subtitle(t) => t.pid = pid,
                }
            }
            route.push(Some((out.streams.len(), repack)));
            out.streams.push(s);
            out.codec_privates.push(cp);
        }
        // Write FMKV header unconditionally: skipping it for a zero-stream title
        // would make the output indistinguishable from a non-FMKV file on
        // read-back (read_header → Ok(None) → PMT fallback). Empty array is valid.
        let m = meta::M2tsMeta::from_title(&out);
        meta::write_header(&mut writer, &m)?;
        let pids: Vec<u16> = out.streams.iter().map(stream_pid).collect();
        let boxed: Box<dyn Write + Send> = Box::new(writer);
        let mut muxer = super::tsmux::TsMuxer::new(boxed, &pids);
        muxer.set_program(out.streams.iter().map(stream_type).collect())?;
        // Declaring the codec decides both ES framing (HEVC/H.264 are length-
        // prefixed → need Annex-B conversion; MPEG-2/VC-1 are already start-code
        // ES) and which param-set parser applies (avcC vs hvcC) — kept as one fact.
        for (i, s) in out.streams.iter().enumerate() {
            if let DiscStream::Video(v) = s {
                muxer.set_video_codec(i, v.codec)?;
            }
        }
        // The MVC dependent view copies the base view's DTS per AU (design §2.3).
        let base = out
            .streams
            .iter()
            .position(|s| matches!(s, DiscStream::Video(v) if !v.is_mvc_dependent()));
        for (i, s) in out.streams.iter().enumerate() {
            if let (DiscStream::Video(v), Some(base)) = (s, base)
                && v.is_mvc_dependent()
            {
                muxer.set_mvc_base(i, base)?;
            }
        }
        for (i, cp) in out.codec_privates.iter().enumerate() {
            if let Some(data) = cp {
                muxer.set_codec_private(i, data.clone())?;
            }
        }
        let ref_video = title
            .streams
            .iter()
            .enumerate()
            .find(|&(i, s)| {
                matches!(s, DiscStream::Video(v) if !v.is_mvc_dependent())
                    && matches!(route.get(i), Some(Some(_)))
            })
            .map(|(i, _)| i);
        Ok(Self {
            disc_title: title.clone(),
            muxer,
            route,
            excluded,
            timeline: super::timeline::TimelineContinuity::with_clips(
                &title.clips,
                title.content_format,
            ),
            ref_video,
            frames_mapped: 0,
        })
    }
}

impl crate::pes::PesSink for M2tsStream {
    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        if self.excluded.drop_frame(frame.track) {
            return Ok(());
        }
        let route = self.route.get(frame.track).copied();
        let mut pts = frame.pts;
        if let Some(Some(_)) = route {
            // Onto the continuous timeline first; `None` is material outside the clip marks.
            let is_video = matches!(
                self.disc_title.streams.get(frame.track),
                Some(DiscStream::Video(_))
            );
            let Some(mapped) = self.timeline.map_picture(
                frame.pts,
                Some(frame.track) == self.ref_video,
                frame.track,
                is_video,
                frame.source.map(|s| s.byte),
                is_video.then_some(crate::mux::timeline::SeamPic {
                    keyframe: frame.keyframe,
                    coding: frame.coding,
                }),
            ) else {
                return Ok(());
            };
            pts = mapped;
            self.frames_mapped += 1;
        }
        match route {
            // Dropped at create (already warned): nothing to write.
            Some(None) => Ok(()),
            // Parser output is plain PCM (16 or 24-bit); BD-TS needs the BD LPCM framing back.
            Some(Some((track, Repack::Lpcm(header)))) => {
                for (offset_ns, payload) in super::codec::lpcm::bd_payloads(&frame.data, header) {
                    self.muxer.write_frame(
                        track,
                        pts.saturating_add(offset_ns),
                        frame.keyframe,
                        &payload,
                    )?;
                }
                Ok(())
            }
            Some(Some((track, Repack::Adts(header)))) => {
                let blocks = super::codec::adts::raw_data_blocks(header, frame.duration_ns);
                let Some(adts) = super::codec::adts::adts_frame(header, &frame.data, blocks) else {
                    tracing::warn!(target: "mux", track, len = frame.data.len(), "AAC access unit too long for ADTS; dropped");
                    return Ok(());
                };
                self.muxer.write_frame(track, pts, frame.keyframe, &adts)
            }
            Some(Some((track, Repack::Verbatim))) => {
                self.muxer
                    .write_frame(track, pts, frame.keyframe, &frame.data)
            }
            // Out of range: let the muxer report its usual error.
            None => self
                .muxer
                .write_frame(frame.track, frame.pts, frame.keyframe, &frame.data),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        // As the other sinks: a seam plan that dropped everything, or most of the title,
        // is a failed mux, not a short file.
        let seam_dropped = self.timeline.dropped_total();
        if self.frames_mapped == 0 && seam_dropped > 0 {
            return Err(crate::error::Error::SinkWroteNothing.into());
        }
        if seam_dropped > self.frames_mapped {
            return Err(crate::error::Error::SeamPlanDroppedMost {
                dropped: seam_dropped,
                written: self.frames_mapped,
            }
            .into());
        }
        if seam_dropped > 0 {
            tracing::info!(
                target: "mux",
                dropped = seam_dropped,
                "frames outside the playlist's clip marks were dropped at clip joins"
            );
        }
        self.muxer.finish()
    }

    // The source title: its PIDs, not the BD-range PIDs the file and FMKV header carry.
    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn undelivered_streams(&self) -> Vec<usize> {
        // LPCM/AAC BD-TS can't carry (known from create), and MPEG-2 extension tracks whose
        // packets arrived.
        let mut out: Vec<usize> = (0..self.route.len())
            .filter(|&i| self.route[i].is_none() && !self.excluded.contains(i))
            .chain(self.excluded.seen())
            .collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
#[path = "m2ts_tests.rs"]
mod tests;

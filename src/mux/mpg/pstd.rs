//! The pack scheduler (mpg-output-design v5 §2.4): earliest-deadline-first over a P-STD
//! model the sink keeps, one buffer per stream_id (`0xBD` shared by its sub-streams),
//! per-pack `program_mux_rate`, SCR spacing ≤ 0.7 s, and whole PES packets inside
//! fixed 2048-byte packs.
//!
//! Times are 27 MHz (`…27`); PTS/DTS are 90 kHz ticks. An access unit's decoding time is
//! its DTS, else its PTS (MS-16).

use super::pack::{self, PesFields};
use std::collections::VecDeque;
use std::io::{self, Write};

/// 27 MHz ticks per second.
pub(crate) const HZ27: u64 = 27_000_000;
/// A chunk that commences an AU may go when that AU decodes within 0.95 s (design §2.4
/// step 3), under MS-16's "less than or equal to one second" delay bound.
const LEAD27: u64 = HZ27 * 95 / 100;
/// 0.7 s in 90 kHz ticks: a PTS step over it is counted as a content gap.
pub(crate) const MAX_PTS_GAP_TICKS: u64 = 63_000;
/// MS-17: SCR fields in successive packs ≤ 0.7 s apart.
pub(crate) const MAX_SCR_GAP27: u64 = HZ27 * 7 / 10;
/// Widest timestamp gap bridged with padding packs; a wider one is re-based away. Above
/// the longest finite DVD cell still (254 s); a narrower gap is re-based only when the
/// padding budget runs out (under about 183 B of input per second of gap).
const MAX_PAD_GAP27: u64 = 300 * HZ27;
/// Padding allowed beyond `PAD_RATIO` × the input bytes pushed, so amplification stays
/// bounded while sparse low-bitrate stills are still padded.
const PAD_FLOOR_BYTES: u64 = 256 * 1024;
const PAD_RATIO: u64 = 16;
/// Per-track lookahead before the clock may pass `t` (design §2.4 step 1).
const LOOKAHEAD27: u64 = HZ27;
/// Max-interleave cap: a track this far behind the others is treated as sparse.
const INTERLEAVE_CAP27: u64 = 10 * HZ27;
const INTERLEAVE_CAP_BYTES: usize = 256 * 1024 * 1024;
/// ES bytes assumed per pack when sizing the rate (2048 less pack and PES headers).
const EST_PAYLOAD: u64 = 1_990;
/// 27 MHz ticks per byte at one `program_mux_rate` unit (50 B/s).
const TICKS_PER_BYTE_UNIT: u128 = (HZ27 / 50) as u128;

/// `ceil(bytes / (units × 50 B/s))` in 27 MHz ticks.
pub(crate) fn dur27(bytes: u64, units: u32) -> u64 {
    let n = u128::from(bytes) * TICKS_PER_BYTE_UNIT;
    n.div_ceil(u128::from(units)) as u64
}

/// How a stream's PES payload starts (the private_stream_1 sub-stream header, MS-29).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Payload {
    /// Video or MPEG audio: the ES alone.
    Plain,
    /// The MPEG-2 audio extension: one AU per PES, whole (design §2.3, MPG4-7).
    WholeAu,
    /// AC-3 / DTS: sub-id, number_of_frame_headers, first_access_unit_pointer.
    Frames { sub_id: u8 },
    /// DVD LPCM: the AC-3 style three bytes, then the 3-byte LPCM header.
    Lpcm {
        sub_id: u8,
        channels: usize,
        rate: u32,
    },
    /// Subpicture: the sub-id only.
    SubId { sub_id: u8 },
}

impl Payload {
    fn header_len(self) -> usize {
        match self {
            Payload::Plain | Payload::WholeAu => 0,
            Payload::Frames { .. } => 4,
            Payload::Lpcm { .. } => 7,
            Payload::SubId { .. } => 1,
        }
    }
}

/// One output stream (a stream_id, or a private_stream_1 sub-stream).
#[derive(Debug, Clone)]
pub(crate) struct StreamSpec {
    pub stream_id: u8,
    pub payload: Payload,
    /// Index into the buffer table.
    pub buffer: usize,
    /// Subpictures: not held to the lookahead (design §2.4 step 1).
    pub sparse: bool,
    /// Video or audio: its PTS spacing is §2.7.4's concern (MS-18).
    pub av: bool,
}

/// One P-STD buffer `Bn` and its bound (MS-7, MS-13).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferSpec {
    pub stream_id: u8,
    pub scale_1024: bool,
    pub size: u16,
}

impl BufferSpec {
    pub(crate) fn bytes(self) -> u64 {
        u64::from(self.size) * if self.scale_1024 { 1024 } else { 128 }
    }
}

/// An access unit ready to packetize.
#[derive(Debug, Clone)]
pub(crate) struct Au {
    pub pts: u64,
    /// Coded DTS, present only when it differs from the PTS (MS-19).
    pub dts: Option<u64>,
    /// Offset of the byte whose presence lets a PES carry this AU's PTS: its first
    /// byte, or for video the first picture start code (MS-15, MS-19).
    pub mark: usize,
    pub data: Vec<u8>,
    /// DVD LPCM quantization of `data` (16/20/24); 0 otherwise.
    pub lpcm_bits: u8,
}

struct QAu {
    au: Au,
    id: u64,
    dec27: u64,
    sent: usize,
}

struct Stream {
    spec: StreamSpec,
    queue: VecDeque<QAu>,
    first_pes_done: bool,
    active: bool,
    last_dec27: u64,
    last_pts: Option<u64>,
    lagging_counted: bool,
}

struct Entry {
    stream: usize,
    id: u64,
    dec27: u64,
    bytes: u64,
    complete: bool,
}

/// Anomalies the scheduler resolved rather than refusing the stream (design §2.4).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PstdCounters {
    /// AUs whose last byte arrived after their decoding time (`pstd_late_aus`).
    pub late_aus: u64,
    /// Consecutive coded PTS more than 0.7 s apart (`pts_gap_over_0_7s`, MPG2-11).
    pub pts_gaps: u64,
    /// Tracks the interleave cap treated as sparse.
    pub interleave_cap: u64,
    /// Padding-only packs written to keep SCR spacing ≤ 0.7 s.
    pub padding_packs: u64,
    /// EOF passes that lifted the lead and whole-frame waits to drain what was stuck.
    pub forced_eof: u64,
    /// Quiet timestamp gaps closed by shifting later timestamps back, not padding.
    pub rebased_gaps: u64,
}

/// The pack writer.
pub(crate) struct Mux<W: Write> {
    writer: W,
    streams: Vec<Stream>,
    buffers: Vec<BufferSpec>,
    entries: Vec<Entry>,
    /// System header + PSM for the first pack (MS-21 §2.7.8); `None` once written.
    first: Option<Vec<u8>>,
    /// Lowest `program_mux_rate` (design §2.4 step 5, R0).
    r0: u32,
    t: Option<u64>,
    last_scr: Option<u64>,
    next_id: u64,
    queued_bytes: usize,
    counters: PstdCounters,
    /// Total 27 MHz shift removed from timestamps by re-basing; applied to later AUs.
    shift27: u64,
    pushed_bytes: u64,
    /// EOF with nothing schedulable: the lead and whole-frame waits are lifted (B1).
    forcing: bool,
}

// What one PES will carry.
struct PesPlan {
    /// ES bytes (after any sub-stream header).
    len: usize,
    /// `Some((queue index, offset of its first byte in the payload))` when an AU commences.
    start: Option<(usize, usize)>,
}

impl<W: Write> Mux<W> {
    pub(crate) fn new(
        writer: W,
        streams: Vec<StreamSpec>,
        buffers: Vec<BufferSpec>,
        first: Vec<u8>,
        r0: u32,
    ) -> Self {
        Self {
            writer,
            streams: streams
                .into_iter()
                .map(|spec| Stream {
                    // Every carried track holds the lookahead from the start, so one that
                    // lags in the source interleave is waited for (design §2.4 step 1); the
                    // extension, which IFO coding mode 3 may only declare, from its first AU.
                    active: !spec.sparse && spec.payload != Payload::WholeAu,
                    spec,
                    queue: VecDeque::new(),
                    first_pes_done: false,
                    last_dec27: 0,
                    last_pts: None,
                    lagging_counted: false,
                })
                .collect(),
            buffers,
            entries: Vec::new(),
            first: Some(first),
            r0,
            t: None,
            last_scr: None,
            next_id: 0,
            queued_bytes: 0,
            counters: PstdCounters::default(),
            shift27: 0,
            pushed_bytes: 0,
            forcing: false,
        }
    }

    pub(crate) fn counters(&self) -> PstdCounters {
        self.counters
    }

    #[cfg(test)]
    pub(crate) fn into_writer(self) -> W {
        self.writer
    }

    /// Queue one AU of stream `si`, in that stream's decode order.
    pub(crate) fn push(&mut self, si: usize, mut au: Au) {
        let sh90 = self.shift27 / 300;
        au.pts = au.pts.saturating_sub(sh90);
        au.dts = au.dts.map(|d| d.saturating_sub(sh90));
        self.pushed_bytes += au.data.len() as u64;
        let s = &mut self.streams[si];
        let dec27 = au.dts.unwrap_or(au.pts).saturating_mul(300);
        // MS-18 / MPG2-11: a gap over 0.7 s is the content's own (a still or slideshow); count it.
        if s.spec.av
            && let Some(prev) = s.last_pts
            && au.pts.abs_diff(prev) > MAX_PTS_GAP_TICKS
        {
            self.counters.pts_gaps += 1;
        }
        s.last_pts = Some(au.pts);
        s.active = true;
        s.last_dec27 = s.last_dec27.max(dec27);
        self.queued_bytes += au.data.len();
        let id = self.next_id;
        self.next_id += 1;
        s.queue.push_back(QAu {
            au,
            id,
            dec27,
            sent: 0,
        });
    }

    // Design §2.4 step 1: every active non-sparse track has data decoding at or past
    // `t + 1 s`, or the interleave cap lets the clock pass a lagging one.
    fn ready(&mut self, t: u64, eof: bool) -> bool {
        if eof || self.queued_bytes > INTERLEAVE_CAP_BYTES {
            return true;
        }
        let lead = self.streams.iter().map(|s| s.last_dec27).max().unwrap_or(0);
        let mut ok = true;
        for s in &mut self.streams {
            if !s.active || s.spec.sparse || s.last_dec27 >= t + LOOKAHEAD27 {
                continue;
            }
            if s.last_dec27 + INTERLEAVE_CAP27 < lead {
                if !s.lagging_counted {
                    s.lagging_counted = true;
                    self.counters.interleave_cap += 1;
                }
                continue;
            }
            ok = false;
        }
        ok
    }

    // Bytes that may still enter buffer `b` at `t`, after removing every complete AU whose
    // decoding time has come (MS-16).
    fn room(&self, b: usize, t: u64) -> u64 {
        // MS-16: "0 ≤ Fn(t) ≤ BSn"; an AU leaves Bn at its decoding time.
        let fill: u64 = self
            .entries
            .iter()
            .filter(|e| self.streams[e.stream].spec.buffer == b && !(e.complete && e.dec27 <= t))
            .map(|e| e.bytes)
            .sum();
        self.buffers[b].bytes().saturating_sub(fill)
    }

    fn remove_decoded(&mut self, t: u64) {
        self.entries.retain(|e| !(e.complete && e.dec27 <= t));
    }

    // Distance from the chunk start to the first byte of queue item `k` (k ≥ 1).
    fn first_byte(s: &Stream, k: usize) -> Option<usize> {
        let head = s.queue.front()?;
        s.queue.get(k)?;
        (k >= 1).then(|| {
            head.au.data.len() - head.sent
                + s.queue
                    .iter()
                    .skip(1)
                    .take(k - 1)
                    .map(|q| q.au.data.len())
                    .sum::<usize>()
        })
    }

    // Distance from the chunk start to the commencement byte of queue item `k`.
    fn commencement(s: &Stream, k: usize) -> Option<usize> {
        let head = s.queue.front()?;
        let q = s.queue.get(k)?;
        let before = if k == 0 { 0 } else { Self::first_byte(s, k)? };
        let own = if k == 0 {
            q.au.mark.checked_sub(head.sent)?
        } else {
            q.au.mark
        };
        Some(before + own)
    }

    // The next AU to commence in stream `s`'s chunk: queue index and distance.
    fn next_commencement(s: &Stream) -> Option<(usize, usize)> {
        let head = s.queue.front()?;
        if head.sent <= head.au.mark {
            return Some((0, head.au.mark - head.sent));
        }
        Self::commencement(s, 1).map(|d| (1, d))
    }

    // Plan one PES of stream `si` in `avail` pack bytes at `t` (design §2.3 "PES packing").
    fn plan_pes(&self, si: usize, avail: usize, t: u64) -> Option<PesPlan> {
        let s = &self.streams[si];
        let head = s.queue.front()?;
        let hdr = s.spec.payload.header_len();
        let pstd = if s.first_pes_done { 0 } else { 3 };
        let buffer_bytes = self.buffers[s.spec.buffer].bytes();
        let mut room = self.room(s.spec.buffer, t);
        let unit = |q: &QAu| lpcm_unit(s.spec.payload, q.au.lpcm_bits);
        // Design §2.4 (MPG3-7): an AU larger than its buffer can never pass the overflow
        // test, so it takes the late path rather than deadlocking.
        if head.au.data.len() as u64 + hdr as u64 > buffer_bytes {
            room = u64::MAX;
        }
        let room = usize::try_from(room.saturating_sub(hdr as u64)).unwrap_or(usize::MAX);
        // Audio AUs go whole into one PES when a pack can hold them: PS readers time a
        // frame by the PES its first byte is in, some only once it completes there (the
        // extension additionally by MPG4-7).
        let whole = match s.spec.payload {
            Payload::WholeAu | Payload::Frames { .. } => true,
            Payload::Plain => (0xC0..=0xDF).contains(&s.spec.stream_id),
            _ => false,
        };
        // Never more than the bytes queued: the header states the payload length.
        let queued = s.queue.iter().map(|q| q.au.data.len()).sum::<usize>() - head.sent;
        let room = room.min(queued);
        if let Some((k, dist)) = Self::next_commencement(s) {
            let q = &s.queue[k];
            let ts = if q.au.dts.is_some() { 10 } else { 5 };
            // A PES that carries a PTS starts at its AU's first byte, as a DVD encoder
            // writes it: PS readers give a PES's PTS to the AU holding its first byte.
            let at_au = k == 0 && head.sent <= head.au.mark;
            let lead_ok = self.forcing || q.dec27.saturating_sub(t) <= LEAD27;
            let fresh = pack::PACK_BYTES - pack::PACK_HEADER_BYTES - (9 + pstd + ts + hdr);
            if at_au && lead_ok && dist >= fresh {
                // B1: no PES reaches the commencement byte (MS-15), so the bytes before it
                // go first without a PTS; the PES holding it carries the PTS.
                let cap = avail.checked_sub(9 + pstd + hdr)?;
                let len = cap.min(dist).min(room);
                return (len > 0).then_some(PesPlan { len, start: None });
            }
            if let Some(cap) = avail.checked_sub(9 + pstd + ts + hdr)
                && at_au
                && lead_ok
                && dist < cap
            {
                // At most one AU commences per PES: stop before the next AU's first byte.
                let until_next = Self::first_byte(s, k + 1).unwrap_or(usize::MAX);
                let mut len = cap.min(until_next).min(room);
                if whole && k == 0 && head.sent == 0 {
                    // Wait for a fresh pack when one could hold the whole AU; a larger AU
                    // (BD AC-3 at 640 kbit/s) spans PES.
                    if q.au.data.len() <= cap && q.au.data.len() <= room {
                        len = q.au.data.len();
                    } else if q.au.data.len() <= fresh && !self.forcing {
                        len = 0;
                    }
                }
                len = round_down(len, unit(q).max(1));
                if len > dist {
                    let first_byte = if k == 0 { 0 } else { dist - q.au.mark };
                    return Some(PesPlan {
                        len,
                        start: Some((k, first_byte)),
                    });
                }
            }
        }
        // A tail with no commencement (for the extension, only an AU too big for one pack).
        if head.sent > 0 {
            let cap = avail.checked_sub(9 + pstd + hdr)?;
            let mut len = cap.min(head.au.data.len() - head.sent).min(room);
            if let Some(next) = Self::first_byte(s, 1) {
                len = len.min(next);
            }
            // D1: bytes before the commencement byte go without a PTS; the PES holding
            // the mark carries the PTS (MS-15).
            if head.sent <= head.au.mark {
                len = len.min(head.au.mark - head.sent);
            }
            len = round_down(len, unit(head).max(1));
            if len > 0 {
                return Some(PesPlan { len, start: None });
            }
        }
        None
    }

    // Build one PES of stream `si` from `plan`; update queue and buffer model. Each AU it
    // completes is returned as `(decoding time, offset of its last byte, oversize)`.
    fn emit_pes(&mut self, si: usize, plan: PesPlan) -> (Vec<u8>, Vec<(u64, usize, bool)>) {
        let buffer_bytes = self.buffers[self.streams[si].spec.buffer].bytes();
        let mut done_aus = Vec::new();
        let s = &self.streams[si];
        let mut fields = PesFields::default();
        if let Some((k, _)) = plan.start {
            // MS-15/MS-19: the PTS names the AU commencing here; DTS only where it differs.
            fields.pts = Some(s.queue[k].au.pts);
            fields.dts = s.queue[k].au.dts;
        }
        if !s.first_pes_done {
            // MS-21 §2.7.7: P-STD fields in the first PES of each stream (and sub-stream).
            let b = self.buffers[s.spec.buffer];
            fields.pstd = Some((b.scale_1024, b.size));
        }
        let head_sent = s.queue.front().map_or(0, |q| q.sent);
        fields.data_alignment =
            matches!(s.spec.payload, Payload::Plain | Payload::WholeAu) && head_sent == 0;
        let hdr_len = s.spec.payload.header_len();
        let mut out = pack::pes_header(s.spec.stream_id, &fields, hdr_len + plan.len);
        // The private_stream_1 sub-stream header (MS-14 "user definable"; MS-29 FFmpeg).
        let frames = u8::from(plan.start.is_some());
        let pointer = plan.start.map_or(0u16, |(_, off)| off as u16 + 1);
        match s.spec.payload {
            Payload::Plain | Payload::WholeAu => {}
            Payload::Frames { sub_id } => {
                out.push(sub_id);
                out.push(frames);
                out.extend_from_slice(&pointer.to_be_bytes());
            }
            Payload::Lpcm {
                sub_id,
                channels,
                rate,
            } => {
                let bits = s.queue.front().map_or(16, |q| q.au.lpcm_bits);
                let lpcm = crate::mux::codec::lpcm::dvd_header(channels, rate, bits)
                    .unwrap_or([0x0C, 0, 0x80]);
                out.push(sub_id);
                out.push(frames);
                // The pointer counts the 3 LPCM header bytes too (MS-29: `avio_wb16(ctx->pb, 4)`).
                let pointer = plan.start.map_or(0u16, |(_, off)| off as u16 + 4);
                out.extend_from_slice(&pointer.to_be_bytes());
                out.extend_from_slice(&lpcm);
            }
            Payload::SubId { sub_id } => out.push(sub_id),
        }
        // Consume `plan.len` ES bytes, attributing them (and the sub-stream header) to AUs.
        let mut left = plan.len;
        let mut hdr_bytes = hdr_len as u64;
        while left > 0 {
            let s = &mut self.streams[si];
            let Some(q) = s.queue.front_mut() else { break };
            let take = left.min(q.au.data.len() - q.sent);
            out.extend_from_slice(&q.au.data[q.sent..q.sent + take]);
            q.sent += take;
            left -= take;
            let (id, dec27, done) = (q.id, q.dec27, q.sent == q.au.data.len());
            let bytes = take as u64 + std::mem::take(&mut hdr_bytes);
            match self
                .entries
                .iter_mut()
                .find(|e| e.stream == si && e.id == id)
            {
                Some(e) => {
                    e.bytes += bytes;
                    e.complete = done;
                }
                None => self.entries.push(Entry {
                    stream: si,
                    id,
                    dec27,
                    bytes,
                    complete: done,
                }),
            }
            if done {
                // Design §2.4 (MPG3-7): the AU plan_pes let past a buffer it cannot fit.
                let oversize = (q.au.data.len() + hdr_len) as u64 > buffer_bytes;
                done_aus.push((dec27, out.len() - 1, oversize));
                self.queued_bytes -= q.au.data.len();
                s.queue.pop_front();
            }
        }
        self.streams[si].first_pes_done = true;
        (out, done_aus)
    }

    // Design §2.4 step 5: the lowest rate, rounded up to 50 B/s units, that delivers every
    // queued AU by its decoding time; `RATE_BOUND` when one is already due.
    fn rate(&self, t: u64) -> u32 {
        let mut due: Vec<(u64, u64)> = self
            .streams
            .iter()
            .flat_map(|s| s.queue.iter())
            .filter(|q| q.dec27 <= t + LOOKAHEAD27)
            .map(|q| (q.dec27, (q.au.data.len() - q.sent) as u64))
            .collect();
        due.sort_unstable();
        let mut units = u128::from(self.r0);
        let mut bytes = 0u64;
        for (dec, b) in due {
            bytes += b;
            if dec <= t {
                return pack::RATE_BOUND;
            }
            let packs = bytes.div_ceil(EST_PAYLOAD) + 1;
            let need = (u128::from(packs) * pack::PACK_BYTES as u128 * TICKS_PER_BYTE_UNIT)
                .div_ceil(u128::from(dec - t));
            units = units.max(need);
        }
        units.min(u128::from(pack::RATE_BOUND)) as u32
    }

    // Build and write the pack at SCR `t`; `false` when it carried nothing (and was not
    // the first, which must carry the system header and map, MS-21 §2.7.8).
    fn write_pack(&mut self, t: u64, padding_only: bool) -> io::Result<bool> {
        self.remove_decoded(t);
        let rate = if padding_only { self.r0 } else { self.rate(t) };
        let prefix = self.first.take();
        let mut avail =
            pack::PACK_BYTES - pack::PACK_HEADER_BYTES - prefix.as_ref().map_or(0, Vec::len);
        let mut body: Vec<u8> = prefix.clone().unwrap_or_default();
        let mut any = false;
        let mut last_pes = None;
        // AUs completed in this pack: (decoding time, offset in `body`, oversize, PES start).
        let mut done_aus: Vec<(u64, usize, bool, usize)> = Vec::new();
        while !padding_only && avail >= 10 {
            let mut best: Option<(u64, usize, PesPlan)> = None;
            for si in 0..self.streams.len() {
                let Some(dec) = self.streams[si].queue.front().map(|q| q.dec27) else {
                    continue;
                };
                if best.as_ref().is_some_and(|(d, _, _)| *d <= dec) {
                    continue;
                }
                if let Some(p) = self.plan_pes(si, avail, t) {
                    best = Some((dec, si, p));
                }
            }
            let Some((_, si, plan)) = best else { break };
            let (pes, done) = self.emit_pes(si, plan);
            avail -= pes.len();
            let at = body.len();
            done_aus.extend(done.into_iter().map(|(d, o, big)| (d, at + o, big, at)));
            last_pes = Some(at);
            body.extend_from_slice(&pes);
            any = true;
        }
        if !any && prefix.is_none() && !padding_only {
            return Ok(false);
        }
        // Design §2.3 pack fill: a padding PES for ≥ 6 bytes; 1-5 bytes become PES-header
        // stuffing on the last PES (MS-13 "No more than 32"), keeping the pack header
        // unstuffed like a DVD VOB pack; pack stuffing (MS-3 ≤ 7) only with no PES at all.
        let mut stuffed = None;
        let stuffing = match (avail, last_pes) {
            (0, _) => 0,
            (a, _) if a >= pack::MIN_PADDING_PES => {
                body.extend_from_slice(&pack::padding_pes(a));
                0
            }
            (a, Some(at)) => {
                pack::stuff_pes_header(&mut body, at, a);
                stuffed = Some((at, a));
                0
            }
            (a, None) => a,
        };
        // MS-16: complete "at the decoding time"; later is counted, not refused. A byte's
        // arrival follows MS-4 eq. 2-21 from this pack's SCR and rate.
        for (dec27, off, oversize, pes_at) in done_aus {
            let shift = stuffed
                .filter(|(at, _)| *at == pes_at)
                .map_or(0, |(_, n)| n);
            let pos = pack::PACK_HEADER_BYTES + stuffing + off + shift;
            let arrival = t
                + ((pos - pack::SCR_BASE_LAST_BYTE) as u128 * TICKS_PER_BYTE_UNIT
                    / u128::from(rate)) as u64;
            if arrival > dec27 || oversize {
                self.counters.late_aus += 1;
            }
        }
        let mut p = pack::pack_header(t, rate, stuffing);
        p.extend_from_slice(&body);
        debug_assert_eq!(p.len(), pack::PACK_BYTES);
        self.writer.write_all(&p)?;
        self.last_scr = Some(t);
        // MS-4: every byte of this pack enters before any byte of the next; the next
        // pack's 8 bytes before its SCR byte arrive at no less than R0.
        self.t = Some(t + dur27(pack::PACK_BYTES as u64, rate) + dur27(8, self.r0));
        Ok(true)
    }

    // When stream `si`'s head can next go, if blocked only by time or buffer room.
    fn wake_time(&self, si: usize, t: u64) -> Option<u64> {
        let s = &self.streams[si];
        let head = s.queue.front()?;
        let removal = self
            .entries
            .iter()
            .filter(|e| {
                self.streams[e.stream].spec.buffer == s.spec.buffer && e.complete && e.dec27 > t
            })
            .map(|e| e.dec27)
            .min();
        // Not yet within the 0.95 s lead: wait for it; else (a tail, or an AU waiting for
        // room) only a removal can unblock it.
        let lead = Self::next_commencement(s).map(|(k, _)| s.queue[k].dec27.saturating_sub(LEAD27));
        let w = match lead {
            Some(l) if head.sent <= head.au.mark && l > t => Some(l),
            _ => removal,
        };
        w.map(|w| w.max(t + 1))
    }

    /// Write every pack the lookahead allows; at `eof`, everything.
    pub(crate) fn pump(&mut self, eof: bool) -> io::Result<()> {
        loop {
            let pending = self.streams.iter().any(|s| !s.queue.is_empty());
            if !pending && self.first.is_none() {
                return Ok(());
            }
            let t = match self.t {
                Some(t) => t,
                None => {
                    // Design §2.4 step 8: initial SCR = the lowest decoding time − 1 s.
                    let lo = self
                        .streams
                        .iter()
                        .filter_map(|s| s.queue.front())
                        .map(|q| q.dec27)
                        .min();
                    lo.unwrap_or(0).saturating_sub(HZ27)
                }
            };
            if !self.ready(t, eof) {
                self.t = Some(t);
                return Ok(());
            }
            if self.write_pack(t, false)? {
                continue;
            }
            // Nothing eligible: advance to the next removal or eligibility time, keeping
            // SCR spacing ≤ 0.7 s with padding packs (MS-17).
            let next = (0..self.streams.len())
                .filter_map(|si| self.wake_time(si, t))
                .min();
            let Some(next) = next else {
                if !eof {
                    return Ok(());
                }
                // B1: at EOF nothing is left behind. Lift the waits and drop empty AUs once;
                // what still cannot go is an error, never a silent Ok.
                if self.forcing {
                    let left: usize = self.streams.iter().map(|s| s.queue.len()).sum();
                    tracing::error!(
                        target: "mux",
                        access_units = left,
                        "mpg: access units could not be packetized"
                    );
                    return Err(crate::error::Error::MpgUnpacketized.into());
                }
                self.forcing = true;
                self.counters.forced_eof += 1;
                let left: usize = self.streams.iter().map(|s| s.queue.len()).sum();
                tracing::warn!(
                    target: "mux",
                    access_units = left,
                    "mpg: end of input with access units no pack could take; the 0.95 s lead \
                     and whole-frame waits are lifted to write them"
                );
                for s in &mut self.streams {
                    while s.queue.front().is_some_and(|q| q.au.data.is_empty()) {
                        s.queue.pop_front();
                    }
                }
                continue;
            };
            match self.last_scr {
                // A wake within the lead may be a written AU's removal, which no re-base
                // moves: pad it (one pack), so the loop always progresses.
                Some(last)
                    if next.saturating_sub(last) > MAX_PAD_GAP27
                        || (next.saturating_sub(last) > LEAD27
                            && self.pad_over_budget(next, last)) =>
                {
                    self.rebase(last, next);
                }
                Some(last) if next > last.saturating_add(MAX_SCR_GAP27) => {
                    let at = last.saturating_add(MAX_SCR_GAP27).max(t);
                    self.counters.padding_packs += 1;
                    self.write_pack(at, true)?;
                }
                _ => self.t = Some(next),
            }
        }
    }

    // Padding stays within a floor plus `PAD_RATIO` × the input, and at most one pack per
    // wait inside the 0.95 s lead.
    fn pad_over_budget(&self, next: u64, last: u64) -> bool {
        let packs = next.saturating_sub(last) / MAX_SCR_GAP27;
        (self.counters.padding_packs + packs) * pack::PACK_BYTES as u64
            > PAD_FLOOR_BYTES + PAD_RATIO * self.pushed_bytes
    }

    // Every stream is quiet until `next`: shift queued timestamps back so it lands within
    // one SCR step of `last` (rounded up). Written AUs keep their decoding times, except
    // those a forced EOF drain wrote past the lead: only moving them lets the loop progress.
    fn rebase(&mut self, last: u64, next: u64) {
        let d90 = next.saturating_sub(last + MAX_SCR_GAP27).div_ceil(300);
        let d27 = d90 * 300;
        if d27 == 0 {
            self.t = Some(next);
            return;
        }
        self.counters.rebased_gaps += 1;
        self.shift27 += d27;
        for s in &mut self.streams {
            s.last_dec27 = s.last_dec27.saturating_sub(d27);
            s.last_pts = s.last_pts.map(|p| p.saturating_sub(d90));
            for q in &mut s.queue {
                q.dec27 = q.dec27.saturating_sub(d27);
                q.au.pts = q.au.pts.saturating_sub(d90);
                q.au.dts = q.au.dts.map(|d| d.saturating_sub(d90));
            }
        }
        for e in self.entries.iter_mut().filter(|e| e.dec27 > last + LEAD27) {
            e.dec27 = e.dec27.saturating_sub(d27);
        }
        self.t = Some(next - d27);
    }

    /// Drain everything and write `MPEG_program_end_code`.
    pub(crate) fn finish(&mut self) -> io::Result<()> {
        self.pump(true)?;
        self.writer.write_all(&pack::PROGRAM_END)?;
        self.writer.flush()
    }
}

/// ES bytes of one DVD LPCM packing unit at `bits`; 1 for every other payload.
fn lpcm_unit(payload: Payload, bits: u8) -> usize {
    match payload {
        Payload::Lpcm { channels, .. } => {
            let samples = crate::mux::codec::lpcm::dvd_unit_frames(channels, bits) * channels;
            samples * 2
                + match bits {
                    24 => samples,
                    20 => samples / 2,
                    _ => 0,
                }
        }
        _ => 1,
    }
}

#[cfg(test)]
pub(super) fn lpcm_unit_for_test(channels: usize, bits: u8) -> usize {
    lpcm_unit(
        Payload::Lpcm {
            sub_id: 0,
            channels,
            rate: 48_000,
        },
        bits,
    )
}

fn round_down(n: usize, unit: usize) -> usize {
    n - n % unit
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video_mux() -> Mux<Vec<u8>> {
        let spec = StreamSpec {
            stream_id: 0xE0,
            payload: Payload::Plain,
            buffer: 0,
            sparse: false,
            av: true,
        };
        let buf = BufferSpec {
            stream_id: 0xE0,
            scale_1024: true,
            size: 232,
        };
        Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200)
    }

    fn au(pts: u64, len: usize, mark: usize) -> Au {
        Au {
            pts,
            dts: None,
            mark,
            data: vec![0x55; len],
            lpcm_bits: 0,
        }
    }

    fn scrs(out: &[u8]) -> Vec<u64> {
        let mut v = Vec::new();
        for i in (0..out.len().saturating_sub(10)).step_by(pack::PACK_BYTES) {
            let h = &out[i..];
            let b = |k: usize| u64::from(h[k]);
            let base = ((b(4) >> 3) & 7) << 30
                | (b(4) & 3) << 28
                | b(5) << 20
                | (b(6) >> 3) << 15
                | (b(6) & 3) << 13
                | b(7) << 5
                | b(8) >> 3;
            v.push(base * 300 + ((b(8) & 3) << 7 | b(9) >> 1));
        }
        v
    }

    // A far-future timestamp is re-based, not bridged with millions of padding packs.
    #[test]
    fn a_huge_forward_gap_is_rebased() {
        let mut m = video_mux();
        m.push(0, au(9_000, 100, 0));
        m.push(0, au(9_000 + 2 * 3_600 * 90_000, 100, 0));
        m.finish().unwrap();
        assert_eq!(m.counters().rebased_gaps, 1);
        let out = m.into_writer();
        assert!(out.len() < 1 << 20, "{}", out.len());
        let s = scrs(&out);
        assert!(
            s.windows(2)
                .all(|w| w[1] >= w[0] && w[1] - w[0] <= MAX_SCR_GAP27)
        );
    }

    // Many 59-minute steps: padding stays a small multiple of the input.
    #[test]
    fn repeated_huge_gaps_stay_bounded() {
        let mut m = video_mux();
        for k in 0..2_000u64 {
            m.push(0, au(9_000 + k * 59 * 60 * 90_000, 10, 0));
        }
        m.finish().unwrap();
        let out = m.into_writer();
        assert!(out.len() < 8 << 20, "{}", out.len());
    }

    // Video-only stills 10 s apart are real content: padded to full length, never re-based.
    #[test]
    fn ten_second_video_stills_keep_their_timing() {
        let mut m = video_mux();
        for k in 0..20u64 {
            m.push(0, au(90_000 + k * 900_000, 8_000, 0));
        }
        m.finish().unwrap();
        let c = m.counters();
        assert_eq!(c.rebased_gaps, 0);
        assert!(c.padding_packs >= 19 * 14, "{}", c.padding_packs);
        let s = scrs(&m.into_writer());
        assert!(s.iter().max().copied().unwrap_or(0) >= 190 * HZ27);
    }

    // Many gaps under the re-base threshold: the padding budget alone bounds the output.
    #[test]
    fn many_sub_threshold_gaps_stay_within_the_padding_budget() {
        let mut m = video_mux();
        for k in 0..100u64 {
            m.push(0, au(9_000 + k * 60 * 90_000, 10, 0));
        }
        m.finish().unwrap();
        let c = m.counters();
        assert!(c.rebased_gaps > 0);
        assert!(c.padding_packs * 2_048 <= PAD_FLOOR_BYTES + PAD_RATIO * 1_000);
    }

    // Over budget, a wait on a written AU's removal is padded, not re-based: no re-base
    // can move that removal, so re-basing it would loop forever.
    #[test]
    fn an_over_budget_removal_wait_still_progresses() {
        let d = 100 * HZ27;
        let mut m = video_mux();
        m.first = None;
        m.entries.push(Entry {
            stream: 0,
            id: u64::MAX,
            dec27: d,
            bytes: 232 * 1_024,
            complete: true,
        });
        m.last_scr = Some(d - HZ27 * 8 / 10);
        m.t = m.last_scr;
        m.counters.padding_packs = 1 << 20;
        m.push(0, au(d / 300 + 3_600, 1_000, 0));
        m.finish().unwrap();
        assert!(scrs(&m.into_writer()).iter().all(|&s| s <= d + HZ27));
    }

    // A forced EOF drain writes AUs ahead of the lead; a later wait on their removal must
    // still re-base and finish, not spin (run on a thread so a regression fails, not hangs).
    #[test]
    fn a_forced_drain_then_huge_gaps_still_finishes() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut m = video_mux();
            m.push(0, au(9_000, 0, 0));
            for k in 0..3u64 {
                m.push(0, au(12_600 + k * 400 * 90_000, 200_000, 0));
            }
            let _ = tx.send(m.finish().is_ok());
        });
        let done = rx.recv_timeout(std::time::Duration::from_secs(20));
        assert_eq!(done, Ok(true), "the forced drain never finished");
    }

    // A re-base lands the next SCR at most 0.7 s after the last one (MS-17), whatever
    // the alignment of the last SCR.
    #[test]
    fn a_rebase_never_steps_scr_past_the_limit() {
        for len in (100..4_000).step_by(37) {
            let mut m = video_mux();
            m.push(0, au(9_000, len, 0));
            m.push(0, au(9_000 + 2 * 3_600 * 90_000, 100, 0));
            m.finish().unwrap();
            let s = scrs(&m.into_writer());
            let step = s.windows(2).map(|w| w[1] - w[0]).max().unwrap_or(0);
            assert!(step <= MAX_SCR_GAP27, "len {len}: step {step}");
        }
    }

    // AUs already written keep their decoding times: a re-base must not let the next AU
    // into a buffer they still occupy (MS-16).
    #[test]
    fn a_rebase_keeps_written_aus_in_the_buffer() {
        let size = 232 * 1_024;
        let mut m = video_mux();
        m.push(0, au(90_000, 200_000, 0));
        m.push(0, au(90_000 + 2 * 3_600 * 90_000, 200_000, 0));
        m.finish().unwrap();
        assert_eq!(m.counters().rebased_gaps, 1);
        let out = m.into_writer();
        let (mut sent, mut a_dec) = (0usize, None);
        for p in out
            .chunks(pack::PACK_BYTES)
            .filter(|p| p.len() == pack::PACK_BYTES)
        {
            let scr = scrs(p)[0];
            let at = pack::PACK_HEADER_BYTES + usize::from(p[13] & 7);
            if p[at + 3] != 0xE0 {
                continue;
            }
            let len = usize::from(u16::from_be_bytes([p[at + 4], p[at + 5]]));
            if a_dec.is_none() && p[at + 7] & 0x80 != 0 {
                let t = &p[at + 9..];
                let pts = (u64::from(t[0]) >> 1 & 7) << 30
                    | u64::from(t[1]) << 22
                    | (u64::from(t[2]) >> 1) << 15
                    | u64::from(t[3]) << 7
                    | u64::from(t[4]) >> 1;
                a_dec = Some(pts * 300);
            }
            sent += len - 3 - usize::from(p[at + 8]);
            if a_dec.is_some_and(|d| scr < d) {
                assert!(
                    sent <= size,
                    "{sent} bytes in a {size}-byte buffer at {scr}"
                );
            }
        }
    }

    // Nit (r2): the forced EOF pass that lifts the 0.95 s lead is counted, not silent.
    #[test]
    fn a_forced_eof_drain_is_counted() {
        let mut m = video_mux();
        m.push(0, au(9_000, 0, 0));
        m.push(0, au(12_600, 100, 0));
        m.finish().unwrap();
        assert_eq!(m.counters().forced_eof, 1);
        let mut clean = video_mux();
        clean.push(0, au(9_000, 100, 0));
        clean.finish().unwrap();
        assert_eq!(clean.counters().forced_eof, 0);
    }

    // What no pack can ever take, even with the waits lifted, is an error at EOF, never a
    // silent Ok with the AUs dropped: an LPCM AU shorter than one packing unit.
    #[test]
    fn eof_with_untakeable_aus_is_an_error() {
        let spec = StreamSpec {
            stream_id: pack::PRIVATE_STREAM_1,
            payload: Payload::Lpcm {
                sub_id: 0xA0,
                channels: 2,
                rate: 48_000,
            },
            buffer: 0,
            sparse: false,
            av: true,
        };
        let buf = BufferSpec {
            stream_id: pack::PRIVATE_STREAM_1,
            scale_1024: true,
            size: 232,
        };
        let mut m = Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200);
        m.push(
            0,
            Au {
                lpcm_bits: 16,
                ..au(9_000, 3, 0)
            },
        );
        let e = m.finish().expect_err("an AU that cannot be packetized");
        assert!(
            e.to_string().contains(&crate::error::Error::MpgUnpacketized.to_string()),
            "{e}"
        );
    }

    // A declared audio track that never delivers is passed once the others lead it by the
    // interleave cap, counted; before the cap the clock waits for it.
    #[test]
    fn a_silent_audio_track_is_passed_at_the_interleave_cap() {
        let video = StreamSpec {
            stream_id: 0xE0,
            payload: Payload::Plain,
            buffer: 0,
            sparse: false,
            av: true,
        };
        let audio = StreamSpec {
            stream_id: 0xC0,
            buffer: 1,
            ..video.clone()
        };
        let bufs = vec![
            BufferSpec {
                stream_id: 0xE0,
                scale_1024: true,
                size: 232,
            },
            BufferSpec {
                stream_id: 0xC0,
                scale_1024: false,
                size: 128,
            },
        ];
        let mut m = Mux::new(Vec::new(), vec![video, audio], bufs, Vec::new(), 25_200);
        m.push(0, au(90_000, 100, 0));
        for k in 1..=7u64 {
            m.push(0, au(90_000 + k * 90_000, 100, 0));
            m.pump(false).unwrap();
        }
        assert_eq!(m.counters().interleave_cap, 0, "7 s: still waiting");
        assert!(m.writer.is_empty(), "nothing is written while audio lags");
        for k in 8..=12u64 {
            m.push(0, au(90_000 + k * 90_000, 100, 0));
            m.pump(false).unwrap();
        }
        assert_eq!(m.counters().interleave_cap, 1);
        assert!(!m.writer.is_empty());
    }

    // B1: at EOF nothing is left behind. A zero-length AU used to wedge its stream and
    // every AU after it; `finish` returned Ok with them queued.
    #[test]
    fn eof_never_leaves_aus_behind() {
        let mut m = video_mux();
        m.push(0, au(9_000, 0, 0));
        m.push(0, au(12_600, 100, 0));
        m.finish().unwrap();
        assert!(
            m.streams.iter().all(|s| s.queue.is_empty()),
            "AUs left queued"
        );
    }

    // B1: a picture start code no PES of a fresh pack can reach (a long sequence header or
    // user data ahead of it): the bytes before it go first, without a PTS.
    #[test]
    fn a_commencement_past_a_packs_reach_still_goes() {
        let mut m = video_mux();
        m.push(0, au(9_000, 5_000, 3_000));
        m.push(0, au(12_600, 100, 0));
        m.finish().unwrap();
        assert!(
            m.streams.iter().all(|s| s.queue.is_empty()),
            "AUs left queued"
        );
    }

    // D1: a tail that ends short of the picture start never crosses it without a PTS: with
    // 494 bytes left and the mark 484 away, the commencing PES cannot fit (cap 480) and the
    // tail stops at the mark.
    #[test]
    fn a_tail_never_crosses_the_picture_start() {
        let mut m = video_mux();
        m.push(0, au(9_000, 5_000, 3_000));
        m.streams[0].first_pes_done = true;
        m.streams[0].queue[0].sent = 3_000 - 484;
        let p = m
            .plan_pes(0, 494, 0)
            .expect("the tail is sent, not stalled");
        assert_eq!((p.len, p.start), (484, None));
    }

    // An AU's first byte and its commencement byte share one PES: a PES ends before the next
    // AU's first byte, never between its sequence header and its picture start code (MS-15).
    #[test]
    fn a_pes_never_splits_an_au_from_its_picture_start() {
        let mut m = video_mux();
        m.push(0, au(9_000, 100, 0));
        m.push(0, au(12_600, 1_000, 20));
        let p = m.plan_pes(0, 200, 0).unwrap();
        assert_eq!(p.start.map(|s| s.0), Some(0));
        assert_eq!(
            p.len, 100,
            "stops at the next AU's first byte, not its picture start"
        );
        m.streams[0].queue[0].sent = 90;
        let tail = m.plan_pes(0, 30, 0).unwrap();
        assert_eq!((tail.len, tail.start.map(|s| s.0)), (10, None));
    }

    // A PES that carries a PTS begins at its AU's first byte, as a DVD encoder writes it: PS
    // readers (our AuAssembler) give a PES's PTS to the AU holding its first byte, so a tail
    // of the previous AU in front would take the PTS.
    #[test]
    fn a_pts_pes_begins_at_its_au() {
        let mut m = video_mux();
        m.push(0, au(9_000, 100, 0));
        m.push(0, au(12_600, 1_000, 20));
        m.streams[0].queue[0].sent = 90;
        let p = m.plan_pes(0, 1_000, 0).unwrap();
        assert_eq!((p.len, p.start), (10, None), "the tail goes alone");
    }

    // An audio frame that fits one PES goes whole: PS readers time a frame by the PES its
    // first byte is in, and some (our Ac3Parser) only when it completes there.
    #[test]
    fn an_audio_frame_that_fits_goes_whole() {
        let spec = StreamSpec {
            stream_id: 0xBD,
            payload: Payload::Frames { sub_id: 0x80 },
            buffer: 0,
            sparse: false,
            av: true,
        };
        let buf = BufferSpec {
            stream_id: 0xBD,
            scale_1024: true,
            size: 8191,
        };
        let mut m = Mux::new(Vec::new(), vec![spec], vec![buf], Vec::new(), 25_200);
        m.push(0, au(9_000, 1_792, 0));
        assert!(
            m.plan_pes(0, 500, 0).is_none(),
            "no 480-byte start: wait for a pack"
        );
        assert_eq!(m.plan_pes(0, 2_034, 0).map(|p| p.len), Some(1_792));
        let mut big = Mux::new(
            Vec::new(),
            vec![m.streams[0].spec.clone()],
            vec![buf],
            Vec::new(),
            25_200,
        );
        big.push(0, au(9_000, 2_560, 0));
        assert!(
            big.plan_pes(0, 2_034, 0).is_some_and(|p| p.len < 2_560),
            "a frame no PES holds spans"
        );
    }
}

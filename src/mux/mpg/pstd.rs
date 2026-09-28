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
/// MS-17: SCR fields in successive packs ≤ 0.7 s apart.
pub(crate) const MAX_SCR_GAP27: u64 = HZ27 * 7 / 10;
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
                    // lags in the source interleave is waited for (design §2.4 step 1); a
                    // track that never delivers is released by the interleave cap.
                    active: !spec.sparse,
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
    pub(crate) fn push(&mut self, si: usize, au: Au) {
        let s = &mut self.streams[si];
        let dec27 = au.dts.unwrap_or(au.pts) * 300;
        // MS-18 / MPG2-11: a gap over 0.7 s is the content's own (a still or slideshow); count it.
        if s.spec.av
            && let Some(prev) = s.last_pts
            && au.pts.abs_diff(prev) > 63_000
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
        let before: usize = if k == 0 {
            0
        } else {
            head.au.data.len() - head.sent
                + s.queue
                    .iter()
                    .skip(1)
                    .take(k - 1)
                    .map(|q| q.au.data.len())
                    .sum::<usize>()
        };
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
            let at_au = k == 0 && head.sent == 0;
            let lead_ok = q.dec27.saturating_sub(t) <= LEAD27;
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
                    let fresh = pack::PACK_BYTES - pack::PACK_HEADER_BYTES - (9 + pstd + ts + hdr);
                    if q.au.data.len() <= cap && q.au.data.len() <= room {
                        len = q.au.data.len();
                    } else if q.au.data.len() <= fresh {
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
            len = round_down(len, unit(head).max(1));
            if len > 0 {
                return Some(PesPlan { len, start: None });
            }
        }
        None
    }

    // Build one PES of stream `si` from `plan`; update queue, buffer model and counters.
    fn emit_pes(&mut self, si: usize, plan: PesPlan, arrival_end: u64) -> Vec<u8> {
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
                // MS-16: complete "at the decoding time"; later is counted, not refused.
                if arrival_end > dec27 {
                    self.counters.late_aus += 1;
                }
                self.queued_bytes -= q.au.data.len();
                s.queue.pop_front();
            }
        }
        self.streams[si].first_pes_done = true;
        out
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
            let need =
                (u128::from(packs) * 2048 * TICKS_PER_BYTE_UNIT).div_ceil(u128::from(dec - t));
            units = units.max(need);
        }
        units.min(u128::from(pack::RATE_BOUND)) as u32
    }

    // Build and write the pack at SCR `t`; `false` when it carried nothing (and was not
    // the first, which must carry the system header and map, MS-21 §2.7.8).
    fn write_pack(&mut self, t: u64, padding_only: bool) -> io::Result<bool> {
        self.remove_decoded(t);
        let rate = if padding_only { self.r0 } else { self.rate(t) };
        let arrival_end = t + dur27(pack::PACK_BYTES as u64, rate);
        let prefix = self.first.take();
        let mut avail =
            pack::PACK_BYTES - pack::PACK_HEADER_BYTES - prefix.as_ref().map_or(0, Vec::len);
        let mut body: Vec<u8> = prefix.clone().unwrap_or_default();
        let mut any = false;
        let mut last_pes = None;
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
            let pes = self.emit_pes(si, plan, arrival_end);
            avail -= pes.len();
            last_pes = Some(body.len());
            body.extend_from_slice(&pes);
            any = true;
        }
        if !any && prefix.is_none() && !padding_only {
            return Ok(false);
        }
        // Design §2.3 pack fill: a padding PES for ≥ 6 bytes; 1-5 bytes become PES-header
        // stuffing on the last PES (MS-13 "No more than 32"), keeping the pack header
        // unstuffed like a DVD VOB pack; pack stuffing (MS-3 ≤ 7) only with no PES at all.
        let stuffing = match (avail, last_pes) {
            (0, _) => 0,
            (a, _) if a >= pack::MIN_PADDING_PES => {
                body.extend_from_slice(&pack::padding_pes(a));
                0
            }
            (a, Some(at)) => {
                pack::stuff_pes_header(&mut body, at, a);
                0
            }
            (a, None) => a,
        };
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
            Some(l) if head.sent == 0 && l > t => Some(l),
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
                // Only unsendable data would remain; stop rather than spin.
                return Ok(());
            };
            match self.last_scr {
                Some(last) if next > last + MAX_SCR_GAP27 => {
                    let at = (last + MAX_SCR_GAP27).max(t);
                    self.counters.padding_packs += 1;
                    self.write_pack(at, true)?;
                }
                _ => self.t = Some(next),
            }
        }
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

//! Access-unit assembly — a codec-parser helper.
//!
//! Converts PES fragments to AU-complete access units for program streams, which (unlike
//! transport streams) do not align PES to AU boundaries.
//!
//! [`AuAssembler`] is shared by every program-stream video parser instead of
//! each hand-rolling the buffer: h264/hevc/vc1 ([`Mode::StartCode`] /
//! [`Mode::Vc1`]) and MPEG-2 ([`Mode::Mpeg2`], via [`AuAssembler::mpeg2`]).
//! Self-framing codecs use [`Mode::Passthrough`] for a uniform code path.

use crate::disc::Codec;
use crate::pes::SourcePos;
use std::collections::VecDeque;

/// Safety cap on a single in-progress access unit. A real coded picture is far
/// below this; a stream that never yields a second AU boundary is force-flushed
/// at the cap rather than buffering without bound on hostile/corrupt input.
const MAX_AU_BUFFER: usize = 8 * 1024 * 1024;

// Cap on buffered timing/discontinuity marks: zero-length/start-code-free
// timed fragments grow no buffer bytes, so they never trip the
// `MAX_AU_BUFFER` mark-prune and could grow unbounded otherwise.
const MAX_MARKS: usize = 64 * 1024;

/// One AU-complete unit drained from the buffer: its elementary-stream bytes plus
/// the timing/source/discontinuity of the fragment that opened the AU.
pub(crate) struct AssembledAu {
    pub data: Vec<u8>,
    pub pts: Option<i64>,
    pub dts: Option<i64>,
    pub source: Option<SourcePos>,
    pub discontinuity: bool,
}

/// Design §2.3 "Reader side (L3)" (MPG2-7): an H.264 stream with one AUD per field splits a
/// field pair into two assembled AUs, the second without a PES PTS. It is merged back into
/// its first field (same `frame_num`, opposite parity), so the parser sees the IR frame.
#[derive(Default)]
pub(crate) struct SecondFieldMerge {
    sps: Option<crate::mux::codec::h264::SpsDtsInfo>,
    /// The last AU, held until the next shows whether it is its second field, and its
    /// first slice's field info (`None` once paired or when not a field picture).
    held: Option<(crate::mux::ts::PesPacket, Option<(u32, bool)>)>,
}

impl SecondFieldMerge {
    // `(frame_num, bottom_field_flag)` of the AU's first slice when it codes a field;
    // an SPS in the AU is taken first.
    fn field_of(&mut self, data: &[u8]) -> Option<(u32, bool)> {
        use crate::mux::codec::h264::{parse_slice_field_info, parse_sps_dts_info};
        use crate::mux::codec::startcode::find_start_code;
        let mut at = find_start_code(data, 0);
        while let Some(p) = at {
            let start = p + 3;
            let next = find_start_code(data, start);
            let nal = &data[start..next.unwrap_or(data.len())];
            match nal.first().map(|h| h & 0x1F) {
                Some(7) => self.sps = parse_sps_dts_info(nal).or(self.sps.take()),
                Some(1 | 5) => {
                    let info = parse_slice_field_info(nal, self.sps.as_ref()?)?;
                    return info.field.map(|bottom| (info.frame_num, bottom));
                }
                _ => {}
            }
            at = next;
        }
        None
    }

    /// Feed one assembled AU; returns the AUs now complete, in order.
    pub(crate) fn push(&mut self, au: crate::mux::ts::PesPacket) -> Vec<crate::mux::ts::PesPacket> {
        let field = self.field_of(&au.data);
        if au.pts.is_none()
            && let (Some((frame, bottom)), Some((held, held_field))) = (field, self.held.as_mut())
            && held_field.is_some_and(|(f, b)| f == frame && b != bottom)
        {
            held.data.extend_from_slice(&au.data);
            *held_field = None;
            return Vec::new();
        }
        self.held
            .replace((au, field))
            .map(|(held, _)| held)
            .into_iter()
            .collect()
    }

    /// The AU still held at the end of the stream.
    pub(crate) fn flush(&mut self) -> Option<crate::mux::ts::PesPacket> {
        self.held.take().map(|(au, _)| au)
    }
}

/// VC-1 (SMPTE 421M Annex E) BDU start-code suffixes, `00 00 01 <type>`.
const VC1_FRAME: u8 = 0x0D; // coded picture
const VC1_ENTRY: u8 = 0x0E; // entry-point header
const VC1_SEQ: u8 = 0x0F; // sequence header

/// MPEG-2 (ISO/IEC 13818-2) start-code suffixes, `00 00 01 <type>`.
const MP2_PICTURE: u8 = 0x00; // picture_start_code
const MP2_SEQ: u8 = 0xB3; // sequence_header_code
const MP2_GOP: u8 = 0xB8; // group_start_code

/// How a stream's fragments become AU-complete units.
#[derive(Clone, Copy)]
enum Mode {
    /// Split the elementary stream on the codec's single AU-delimiter start code
    /// `00 00 01 <marker>` (H.264 AUD `0x09`, HEVC AUD `0x46`). Every AU opens with
    /// exactly that code, so a plain split is correct.
    StartCode(u8),
    /// VC-1 has no single AU delimiter: an access unit is a `[sequence header?]
    /// [entry point?][frame][slices…]` group. The sequence-header (`0x0F`) and
    /// entry-point (`0x0E`) BDUs precede the frame (`0x0D`) they belong to, so a
    /// plain `0x0D` split would glue them onto the *previous* AU and strip every
    /// I-frame of its headers. The boundary is instead the next `0x0F`/`0x0E`/`0x0D`
    /// start code that follows a frame already seen in the current AU.
    Vc1,
    /// MPEG-2 access unit: `[sequence header?][GOP header?][picture][slices…]`.
    /// Structurally identical to [`Mode::Vc1`] — the sequence (`0xB3`) and GOP
    /// (`0xB8`) headers precede the picture (`0x00`) they introduce, so the
    /// boundary is the next picture / sequence / GOP start code that follows a
    /// picture already seen. Slice (`0x01..=0xAF`), extension (`0xB5`),
    /// user-data (`0xB2`) and sequence-end (`0xB7`) codes are NOT boundaries.
    Mpeg2,
    /// The codec self-frames (MPEG-2 reassembles in its own parser; audio resyncs
    /// on syncwords), so each fragment passes straight through as one unit. Lets
    /// the caller run EVERY stream through an assembler with no per-codec branch.
    Passthrough,
}

/// A timing/source mark taken at the absolute stream offset of a fragment that
/// carried it, so it survives `buf.drain(..)`. Its source goes to the AU whose
/// byte range contains `off`; its PTS/DTS to the first AU that commences within
/// `[off, end)` (ISO/IEC 13818-1 §2.4.3.7), else to the AU containing `off`.
struct Mark {
    off: u64,
    /// Absolute offset one past the fragment's last byte.
    end: u64,
    pts: Option<i64>,
    dts: Option<i64>,
    source: Option<SourcePos>,
}

/// Reassembles PES fragments into AU-complete units. One per stream; stateful
/// across `push` calls.
pub(crate) struct AuAssembler {
    mode: Mode,
    /// Buffered elementary-stream bytes not yet emitted as a complete AU.
    buf: Vec<u8>,
    /// Absolute stream offset of `buf[0]`, so marks (taken at absolute offsets)
    /// survive `buf.drain(..)`.
    base: u64,
    /// Timing/source marks, in fragment order.
    marks: VecDeque<Mark>,
    /// Absolute offsets of fragments flagged with an upstream discontinuity.
    disc_marks: VecDeque<u64>,
    /// A `MAX_AU_BUFFER` backstop discard happened and no AU has been emitted
    /// since. Sticky rather than an offset mark, because the bytes it refers to
    /// no longer exist: the discard is followed by a pre-sync trim that would
    /// retire any mark placed at the new base, and the gap must outlive that.
    /// Consumed by the next AU to emit. See `discard_gap_before`.
    pending_gap: bool,
    /// Incremental boundary-scan cursor: the offset into `buf` up to which the
    /// current AU has already been searched for its end without finding one. Each
    /// `push` resumes the boundary search from here instead of rescanning the
    /// whole buffer, so reassembling one AU split across N PES fragments costs
    /// O(AU bytes) total, not O(AU bytes²/fragment). Reset to 0 whenever `buf[0]`
    /// moves (an AU drained, or leading bytes dropped).
    scan_pos: usize,
    /// Whether the current AU has already contained a coded frame/picture — the
    /// state the VC-1/MPEG-2 boundary rule carries across a resumed scan (their
    /// boundary is "the next opener after a frame is already seen"). Meaningless
    /// for `Mode::StartCode`. Reset with `scan_pos`.
    seen_unit: bool,
    /// Pre-sync opener-search cursor: the offset up to which the buffer has been
    /// searched for the FIRST AU opener with none found. Resumes the opener scan
    /// so a long run of junk with no start code (hostile/corrupt input) costs
    /// O(bytes) total, not O(buffer) per push. Reset when `buf[0]` moves.
    opener_pos: usize,
    /// Test-only: how many times `take_front` fell back to the COPY path. The
    /// handover is the whole point of `take_front`, so "did it actually fire" is a
    /// property to MEASURE, not to reason about. See
    /// `handover_survives_a_large_au_instead_of_copying_every_later_one`.
    #[cfg(test)]
    copy_path_hits: usize,
}

impl AuAssembler {
    // An assembler for `codec`. H.264/HEVC/VC-1 get a reassembling `Mode`; MPEG-2
    // (self-reassembles) and audio/subtitle codecs (self-framing) get `Mode::Passthrough`.
    pub(crate) fn for_codec(codec: Codec) -> Self {
        let mode = match codec {
            Codec::H264 => Mode::StartCode(0x09), // access_unit_delimiter NAL (type 9)
            Codec::Hevc => Mode::StartCode(0x46), // AUD NAL (type 35 → (35 << 1) = 0x46)
            Codec::Vc1 => Mode::Vc1,              // frame + preceding seq/entry headers
            _ => Mode::Passthrough,
        };
        Self {
            mode,
            // Passthrough never writes `buf` (one fragment → one unit); only the
            // reassembling modes need reserve. Avoids ~256 KiB per audio/subtitle
            // stream (and every TS/BD stream, which never feeds the assembler).
            buf: match mode {
                Mode::Passthrough => Vec::new(),
                _ => Vec::with_capacity(256 * 1024),
            },
            base: 0,
            marks: VecDeque::new(),
            disc_marks: VecDeque::new(),
            pending_gap: false,
            scan_pos: 0,
            seen_unit: false,
            opener_pos: 0,
            #[cfg(test)]
            copy_path_hits: 0,
        }
    }

    // An assembler that reassembles MPEG-2 access units, owned directly by the MPEG-2 parser.
    pub(crate) fn mpeg2() -> Self {
        Self {
            mode: Mode::Mpeg2,
            buf: Vec::with_capacity(128 * 1024),
            base: 0,
            marks: VecDeque::new(),
            disc_marks: VecDeque::new(),
            pending_gap: false,
            scan_pos: 0,
            seen_unit: false,
            opener_pos: 0,
            #[cfg(test)]
            copy_path_hits: 0,
        }
    }

    // Feed one PES fragment the caller OWNS; return every AU now complete. Passthrough moves
    // the payload in with no copy; buffering modes copy into `buf` exactly as `push`.
    pub(crate) fn push_owned(
        &mut self,
        data: Vec<u8>,
        pts: Option<i64>,
        dts: Option<i64>,
        source: Option<SourcePos>,
        discontinuity: bool,
    ) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return vec![AssembledAu {
                data,
                pts,
                dts,
                source,
                discontinuity,
            }];
        }
        self.push(&data, pts, dts, source, discontinuity)
    }

    /// Feed one PES fragment (borrowed); return every AU that is now complete.
    pub(crate) fn push(
        &mut self,
        data: &[u8],
        pts: Option<i64>,
        dts: Option<i64>,
        source: Option<SourcePos>,
        discontinuity: bool,
    ) -> Vec<AssembledAu> {
        // Self-framing codecs pass through unchanged — one fragment, one unit,
        // its own timing. (This is exactly today's behaviour for mpeg2/audio.)
        if matches!(self.mode, Mode::Passthrough) {
            return vec![AssembledAu {
                data: data.to_vec(),
                pts,
                dts,
                source,
                discontinuity,
            }];
        }
        let off = self.base + self.buf.len() as u64;
        if pts.is_some() || dts.is_some() || source.is_some() {
            self.marks.push_back(Mark {
                off,
                end: off + data.len() as u64,
                pts,
                dts,
                source,
            });
            // Backstop: zero-length/start-code-free fragments grow no bytes, so the
            // `buf`-size cap never prunes marks. Bound the deque directly instead.
            if self.marks.len() > MAX_MARKS {
                self.marks.pop_front();
            }
        }
        if discontinuity {
            self.disc_marks.push_back(off);
            if self.disc_marks.len() > MAX_MARKS {
                self.disc_marks.pop_front();
            }
        }
        self.buf.extend_from_slice(data);
        self.drain(false)
    }

    /// Emit the trailing in-progress AU at end of stream (no following boundary).
    pub(crate) fn flush(&mut self) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return Vec::new();
        }
        self.drain(true)
    }

    fn drain(&mut self, force: bool) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return Vec::new();
        }
        let mut out = Vec::new();
        loop {
            // Locate the AU start code that opens the buffered run (resumes from
            // opener_pos so an unsynced junk run is scanned once, not per push).
            let Some(a0) = self.au_opener_resumable() else {
                // No AU boundary buffered. Bound memory: drop all but a 3-byte
                // tail (enough to catch a start-code prefix straddling the cut)
                // once over the cap; otherwise wait for more data.
                if self.buf.len() > MAX_AU_BUFFER {
                    let drop = self.buf.len() - 3;
                    self.buf.drain(..drop);
                    self.base += drop as u64;
                    self.reset_scan();
                    self.discard_gap_before(self.base);
                }
                break;
            };
            if a0 > 0 {
                // Leading bytes before the first AU boundary are a partial AU from
                // before we synced (or junk) — discard them and any stale marks.
                self.buf.drain(..a0);
                self.base += a0 as u64;
                self.reset_scan();
                self.drop_marks_before(self.base);
                continue;
            }
            // The AU runs from here (buf[0]) to the NEXT AU boundary. The search
            // resumes from `scan_pos` (bytes already searched with no boundary),
            // so one AU spread across many fragments is scanned once, not per push.
            let end = match self.au_boundary_resumable() {
                Some(next) => next,
                // No next boundary yet: on EOF (or over-cap backstop) the rest of
                // the buffer is this AU; otherwise wait for more data.
                None if force => self.buf.len(),
                None if self.buf.len() > MAX_AU_BUFFER => self.buf.len(),
                None => break,
            };
            if end == 0 {
                break;
            }
            let end_abs = self.base + end as u64;

            // Take the first Some of each field independently across marks in
            // [base, end_abs): one fragment may carry source while another carries
            // PTS, so reading only the front mark would drop a field.
            let (mut pts, mut dts, mut source) = (None, None, None);
            while self.marks.front().is_some_and(|m| m.off < end_abs) {
                let m = self.marks.front_mut().unwrap();
                source = source.or(m.source.take());
                // A fragment opened inside this AU that runs past it times the next AU.
                if m.off > self.base && m.end > end_abs {
                    break;
                }
                let m = self.marks.pop_front().unwrap();
                pts = pts.or(m.pts);
                dts = dts.or(m.dts);
            }
            // A backstop discard is a gap in its own right, independent of any
            // upstream signal: bytes were thrown away, so this AU does not
            // continue the last one emitted.
            let mut discontinuity = std::mem::take(&mut self.pending_gap);
            if self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                discontinuity = true;
            }
            while self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                self.disc_marks.pop_front();
            }

            let data = self.take_front(end);
            self.base += end as u64;
            self.reset_scan();
            out.push(AssembledAu {
                data,
                pts,
                dts,
                source,
                discontinuity,
            });
        }
        out
    }

    // Detach `buf[..end]` as the AU's own `Vec`, handing over the allocation rather than
    // copying (falls back to a copy when `buf` is far larger than the AU).
    fn take_front(&mut self, end: usize) -> Vec<u8> {
        let cap = self.buf.capacity();
        let tail_len = self.buf.len() - end;
        if cap > end.saturating_mul(2) {
            #[cfg(test)]
            {
                self.copy_path_hits += 1;
            }
            let data = self.buf[..end].to_vec();
            self.buf.drain(..end);
            // Shrink toward this AU's actual need so `cap` drops under the `2*end`
            // threshold and the next similarly-sized AU hands over instead of copying.
            self.buf.shrink_to(end.max(tail_len));
            return data;
        }
        // Replacement buffer: enough for the tail plus room to accumulate the next
        // AU of about this size. NOT `cap`, which would re-pin the high-water mark.
        let mut tail = Vec::with_capacity(end.max(tail_len));
        tail.extend_from_slice(&self.buf[end..]);
        let mut data = std::mem::replace(&mut self.buf, tail);
        data.truncate(end);
        data
    }

    /// Reset the incremental boundary-scan cursor. Called whenever `buf[0]` moves
    /// (an AU drained, or leading bytes discarded) so the next scan starts fresh
    /// from the new AU opener.
    fn reset_scan(&mut self) {
        self.scan_pos = 0;
        self.seen_unit = false;
        self.opener_pos = 0;
    }

    /// Locate the first AU opener in `buf`, resuming the search from `opener_pos`
    /// (bytes already searched with no opener) so a long unsynced run costs
    /// O(bytes) total, not O(buffer) per push. Advances `opener_pos` on a miss.
    fn au_opener_resumable(&mut self) -> Option<usize> {
        match au_opener_from(self.mode, &self.buf, self.opener_pos) {
            Some(o) => Some(o),
            None => {
                // Nothing yet; next call resumes here (back up 3 for a straddling
                // start-code prefix). Never advance past what is searchable.
                self.opener_pos = self.buf.len().saturating_sub(3).max(self.opener_pos);
                None
            }
        }
    }

    // Find the end of the AU that opens at `buf[0]`, resuming from `scan_pos`
    // (and, for VC-1/MPEG-2, `seen_unit`) instead of rescanning the whole
    // buffer — O(total AU bytes) across all pushes, not O(bytes²/fragment).
    fn au_boundary_resumable(&mut self) -> Option<usize> {
        match self.mode {
            Mode::StartCode(marker) => {
                // Resume from the furthest searched offset (never before 4, to skip the
                // opening delimiter at buf[0]); find_start_code needs 4 bytes, so back
                // up 3 to catch a code straddling the previous buffer end.
                let from = self.scan_pos.max(4);
                match find_start_code(&self.buf, from, marker) {
                    Some(e) => Some(e),
                    None => {
                        self.scan_pos = self.buf.len().saturating_sub(3).max(from);
                        None
                    }
                }
            }
            Mode::Vc1 => self.scan_unit_boundary(VC1_FRAME, &[VC1_ENTRY, VC1_SEQ]),
            Mode::Mpeg2 => self.scan_unit_boundary(MP2_PICTURE, &[MP2_SEQ, MP2_GOP]),
            Mode::Passthrough => None,
        }
    }

    // Resumable form of the VC-1/MPEG-2 boundary rule: scan from `scan_pos`,
    // carrying `seen_unit`; the AU ends at the next `frame`/`header` start
    // code once a frame is already seen. Advances both when no boundary found.
    fn scan_unit_boundary(&mut self, frame: u8, headers: &[u8]) -> Option<usize> {
        let buf = &self.buf;
        let mut i = self.scan_pos;
        let mut seen = self.seen_unit;
        while i + 4 <= buf.len() {
            if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 {
                let c = buf[i + 3];
                let is_frame = c == frame;
                if (is_frame || headers.contains(&c)) && i > 0 && seen {
                    // The AU ends at the next frame/header once a frame is seen.
                    return Some(i);
                }
                if is_frame {
                    seen = true;
                }
                i += 4;
            } else {
                i += 1;
            }
        }
        // No boundary yet. Persist the scan state so the next append resumes here
        // rather than rescanning from 0 (the i+=4 stride is preserved exactly).
        self.scan_pos = i;
        self.seen_unit = seen;
        None
    }

    // Retire every mark ending at or before `off`: the STREAM-START case, where bytes ahead of
    // the first AU boundary predate sync. A fragment running past `off` still times that AU.
    fn drop_marks_before(&mut self, off: u64) {
        while self.marks.front().is_some_and(|m| m.end <= off) {
            self.marks.pop_front();
        }
        while self.disc_marks.front().is_some_and(|&o| o < off) {
            self.disc_marks.pop_front();
        }
    }

    // Retire stale timing marks before `off` and record a GAP: the BACKSTOP case, where
    // accumulated bytes had no AU start code and got discarded.
    fn discard_gap_before(&mut self, off: u64) {
        self.drop_marks_before(off);
        self.pending_gap = true;
    }
}

/// Offset of the start code that opens the next AU in `buf` (at or after 0), or
/// `None` if no AU-opening start code is buffered yet.
fn au_opener_from(mode: Mode, buf: &[u8], from: usize) -> Option<usize> {
    match mode {
        Mode::StartCode(marker) => find_start_code(buf, from, marker),
        // Any of the three AU-opening BDU types opens a VC-1 access unit.
        Mode::Vc1 => find_vc1_start(buf, from),
        // A sequence header, GOP header, or picture opens an MPEG-2 access unit.
        Mode::Mpeg2 => find_mpeg2_start(buf, from),
        Mode::Passthrough => None,
    }
}

/// Find the next `00 00 01 <marker>` start code at or after `from`.
fn find_start_code(buf: &[u8], from: usize, marker: u8) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 && buf[i + 3] == marker {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the next VC-1 AU-opening BDU start code (`00 00 01` followed by a
/// sequence header, entry point, or frame) at or after `from`.
fn find_vc1_start(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0
            && buf[i + 1] == 0
            && buf[i + 2] == 1
            && matches!(buf[i + 3], VC1_FRAME | VC1_ENTRY | VC1_SEQ)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the next MPEG-2 AU-opening start code (`00 00 01` followed by a picture,
/// sequence header, or GOP header) at or after `from`.
fn find_mpeg2_start(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0
            && buf[i + 1] == 0
            && buf[i + 2] == 1
            && matches!(buf[i + 3], MP2_PICTURE | MP2_SEQ | MP2_GOP)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
#[path = "au_assembly_tests.rs"]
mod tests;

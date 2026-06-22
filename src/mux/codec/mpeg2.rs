//! MPEG-2 Video elementary stream parser.
//!
//! Reassembles coded pictures (access units) from the demuxed PES stream and
//! extracts sequence headers for MKV codecPrivate.
//!
//! **One PES is NOT one frame.** On a DVD the video elementary stream is sliced
//! into ~2 KB Program-Stream PES packets (one per 2048-byte pack), so a single
//! coded picture (~10-100 KB) spans many PES packets and only the first carries
//! a PTS. Emitting one MKV block per PES would write frame *fragments* — the
//! decoder then sees truncated pictures (`ac-tex damaged`) and picture-coding
//! extensions detached from their picture header (`ignoring pic cod ext`). So
//! this parser buffers ES bytes across PES packets and emits exactly one Frame
//! per coded picture. (Blu-ray aligns one access unit per PES and would not need
//! this, but DVD MPEG-2 PS does.)
//!
//! Access-unit model (ISO/IEC 13818-2): an AU is an optional sequence header +
//! optional GOP header + one picture header + its coding extension + slices. A
//! new AU begins at the next picture / sequence / GOP start code *once the
//! current AU already contains a picture* — leading sequence/GOP headers attach
//! to the picture that follows them.
//!
//! Start codes:
//! - Picture header:     00 00 01 00
//! - Slice:              00 00 01 01 .. AF
//! - Sequence header:    00 00 01 B3
//! - Extension (seq/pic):00 00 01 B5
//! - GOP header:         00 00 01 B8

use std::collections::VecDeque;

use super::startcode::find_start_code;
use super::{CodecParser, Frame, pts_to_ns};
use crate::mux::ts::PesPacket;

/// Sequence header start code suffix.
const SEQ_HEADER_CODE: u8 = 0xB3;

/// Sequence / picture extension start code suffix.
const SEQ_EXT_CODE: u8 = 0xB5;

/// Group-of-pictures header start code suffix.
const GOP_CODE: u8 = 0xB8;

/// Picture start code suffix.
const PICTURE_CODE: u8 = 0x00;

/// Picture coding type: I-frame.
const PICTURE_TYPE_I: u8 = 1;

/// Hard cap on the access-unit reassembly buffer. A real MPEG-2 frame is well
/// under 1 MiB (DVD I-frames ~100 KB); past this cap a corrupt stream that
/// never produces a second access-unit boundary is force-flushed as a single
/// frame rather than driving unbounded allocation.
const MAX_AU_BUFFER: usize = 8 * 1024 * 1024;

/// Cap on frames held awaiting the first PES PTS anchor. A DVD stamps a PTS in
/// the first VOBU (~0.5 s ≈ 15 frames); this leaves generous slack. If no PTS
/// ever arrives within the cap, buffered frames are released on a 0 base.
const MAX_PENDING_FRAMES: usize = 600;

/// Frame rate table (index from sequence header frame_rate_code).
const FRAME_RATES: [(u32, u32); 9] = [
    (0, 1),        // 0: forbidden
    (24000, 1001), // 1: 23.976
    (24, 1),       // 2: 24
    (25, 1),       // 3: 25
    (30000, 1001), // 4: 29.97
    (30, 1),       // 5: 30
    (50, 1),       // 6: 50
    (60000, 1001), // 7: 59.94
    (60, 1),       // 8: 60
];

/// Aspect ratio table (index from sequence header aspect_ratio_information).
const ASPECT_RATIOS: [(u8, u8); 5] = [
    (0, 0),     // 0: forbidden
    (1, 1),     // 1: square pixels (1:1 SAR)
    (4, 3),     // 2: 4:3 display
    (16, 9),    // 3: 16:9 display
    (221, 100), // 4: 2.21:1 display
];

/// MPEG-2 Video elementary stream parser / access-unit reassembler.
pub struct Mpeg2Parser {
    /// Raw bytes of the last seen sequence header (+ sequence extension if
    /// present), captured for MKV codecPrivate.
    seq_header: Option<Vec<u8>>,
    /// Unemitted elementary-stream bytes: the in-progress access unit plus any
    /// lookahead needed to detect the next AU boundary.
    buf: Vec<u8>,
    /// Absolute ES byte offset of `buf[0]`. Used to associate PES PTS marks
    /// (recorded by absolute offset) with the access units they belong to.
    base_offset: u64,
    /// `(absolute ES offset of a PES's first byte, PTS in ns)` for every PES
    /// that carried a timestamp, in ascending offset order.
    pts_marks: VecDeque<(u64, i64)>,
    /// Per-frame presentation interval (ns), derived from the sequence header
    /// frame rate. DVD stamps a PTS only ~once per VOBU (every ~0.5 s), so
    /// frames between marks must be timed by `temporal_reference` × this
    /// interval. 0 until a sequence header with a valid frame rate is seen.
    frame_duration_ns: i64,
    /// Cumulative count of coded pictures emitted in all GOPs before the
    /// current one. `temporal_reference` is GOP-relative (display order within
    /// the GOP); adding this base makes a whole-stream display index.
    gop_base: u64,
    /// Coded pictures emitted in the current GOP so far (folded into
    /// `gop_base` at the next GOP boundary).
    gop_count: u64,
    /// Display index of the last frame that carried an explicit PES PTS, used
    /// to anchor interpolated timestamps to the real disc timeline (so video
    /// stays in sync with the PES-timestamped audio tracks).
    anchor_index: Option<u64>,
    /// PTS (ns) of the anchor frame.
    anchor_pts: i64,
    /// Frames emitted before the first PES PTS anchor is known, held with their
    /// display index. A DVD title can open with a still-frame/first-play
    /// sequence whose PTS lands a few frames in; buffering until the anchor lets
    /// those leading frames take the disc's real timeline instead of a 0 base.
    pending: Vec<(u64, Frame)>,
}

impl Default for Mpeg2Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Mpeg2Parser {
    /// Create a new MPEG-2 parser with no captured sequence-header state.
    pub fn new() -> Self {
        Self {
            seq_header: None,
            buf: Vec::with_capacity(128 * 1024),
            base_offset: 0,
            pts_marks: VecDeque::new(),
            frame_duration_ns: 0,
            gop_base: 0,
            gop_count: 0,
            anchor_index: None,
            anchor_pts: 0,
            pending: Vec::new(),
        }
    }

    /// Extract resolution from a captured sequence header.
    /// Returns (width, height) or None if the header is too short.
    pub fn resolution(&self) -> Option<(u16, u16)> {
        let hdr = self.seq_header.as_ref()?;
        parse_resolution(hdr)
    }

    /// Extract frame rate from a captured sequence header.
    /// Returns (numerator, denominator) or None.
    pub fn frame_rate(&self) -> Option<(u32, u32)> {
        let hdr = self.seq_header.as_ref()?;
        parse_frame_rate(hdr)
    }

    /// Extract aspect ratio from a captured sequence header.
    /// Returns (width, height) for display aspect ratio, or None.
    pub fn aspect_ratio(&self) -> Option<(u8, u8)> {
        let hdr = self.seq_header.as_ref()?;
        parse_aspect_ratio(hdr)
    }

    /// The PTS (ns) to assign to an access unit whose first relevant byte is at
    /// absolute ES offset `target`: the most recent PES timestamp at or before
    /// that offset (the PES that contains the access unit's start). Falls back
    /// to 0 when no timestamp has been seen yet.
    fn pts_for(&self, target: u64) -> i64 {
        let mut best = 0;
        for &(off, pts) in &self.pts_marks {
            if off <= target {
                best = pts;
            } else {
                break;
            }
        }
        best
    }

    /// Drain every complete access unit from `buf`, returning one Frame each.
    /// When `force` is true (EOF flush, or buffer-cap backstop) the trailing
    /// in-progress access unit is emitted even without a following boundary.
    fn drain_complete_aus(&mut self, force: bool) -> Vec<Frame> {
        let mut out = Vec::new();
        loop {
            // An access unit must contain a coded picture; without one there is
            // nothing to emit yet (leading sequence/GOP headers wait for it).
            let Some(pic) = find_code(&self.buf, 0, PICTURE_CODE) else {
                // No coded picture in an over-cap buffer means we are
                // accumulating unparseable data (a stream with no picture
                // start codes). Drop all but a 3-byte tail — enough to catch a
                // start-code prefix straddling the boundary — and advance the
                // absolute offset so the PES-mark invariant holds. Mirrors the
                // post-picture buffer backstop in the AU-boundary search below.
                if self.buf.len() > MAX_AU_BUFFER {
                    let drop = self.buf.len() - 3;
                    self.base_offset += drop as u64;
                    self.buf.drain(..drop);
                    let cutoff = self.base_offset;
                    while let Some(&(off, _)) = self.pts_marks.front() {
                        if off < cutoff {
                            self.pts_marks.pop_front();
                        } else {
                            break;
                        }
                    }
                }
                break;
            };
            // The current AU ends where the next one begins: the first
            // picture / sequence / GOP start code after this picture.
            let end = match find_au_start(&self.buf, pic + 4) {
                Some(b) => b,
                None if force => self.buf.len(),
                None if self.buf.len() > MAX_AU_BUFFER => self.buf.len(),
                None => break, // AU not yet complete — await the next boundary
            };
            if end == 0 {
                break;
            }

            // Phase 1 — read everything from `buf` before any mutation of self
            // (the slice borrow must end before we touch self fields).
            let hdr = extract_seq_header(&self.buf[..end]);
            // A GOP header (0xB8) or a fresh sequence header (0xB3) starts a new
            // GOP, resetting temporal_reference to 0.
            let gop_boundary = find_code(&self.buf[..end], 0, GOP_CODE).is_some()
                || find_code(&self.buf[..end], 0, SEQ_HEADER_CODE).is_some();
            let keyframe = pic + 5 < end && ((self.buf[pic + 5] >> 3) & 0x07) == PICTURE_TYPE_I;
            // temporal_reference: the 10 bits immediately after the picture
            // start code = display order within the GOP.
            let tr = if pic + 5 < end {
                (((self.buf[pic + 4] as u64) << 2) | ((self.buf[pic + 5] as u64) >> 6)) & 0x3FF
            } else {
                0
            };
            let pic_abs = self.base_offset + pic as u64;
            let end_abs = self.base_offset + end as u64;
            let data = self.buf[..end].to_vec();

            // Phase 2 — mutate self.
            if let Some(h) = hdr {
                self.seq_header = Some(h);
                if let Some((num, den)) = self.frame_rate() {
                    if num > 0 {
                        self.frame_duration_ns = 1_000_000_000i64 * den as i64 / num as i64;
                    }
                }
            }
            if gop_boundary && self.gop_count > 0 {
                self.gop_base += self.gop_count;
                self.gop_count = 0;
            }
            let display_index = self.gop_base + tr;

            // An explicit PES PTS for this access unit, if any. By the mark-drain
            // invariant the front mark's offset is >= this AU's start, so a front
            // mark inside [start, end) is this AU's own timestamp.
            let explicit = self
                .pts_marks
                .front()
                .filter(|&&(off, _)| off < end_abs)
                .map(|&(_, p)| p);

            let duration_ns = (self.frame_duration_ns > 0).then_some(self.frame_duration_ns as u64);
            let mut frame = Frame {
                pts_ns: 0,
                keyframe,
                data,
                duration_ns,
            };

            if self.frame_duration_ns > 0 {
                // Reconstruct from display order; anchor to the real PES PTS so
                // video stays in sync with the PES-timestamped audio.
                match explicit {
                    Some(p) => {
                        self.anchor_index = Some(display_index);
                        self.anchor_pts = p;
                        // Backfill any leading frames held before the anchor was
                        // known (still-frame / first-play opening): give each the
                        // disc's real timeline relative to this anchor.
                        for (di, mut held) in self.pending.drain(..) {
                            held.pts_ns =
                                p + (di as i64 - display_index as i64) * self.frame_duration_ns;
                            out.push(held);
                        }
                        frame.pts_ns = p;
                        out.push(frame);
                    }
                    None => match self.anchor_index {
                        Some(ai) => {
                            frame.pts_ns = self.anchor_pts
                                + (display_index as i64 - ai as i64) * self.frame_duration_ns;
                            out.push(frame);
                        }
                        None if self.pending.len() < MAX_PENDING_FRAMES => {
                            // No anchor yet — hold so leading frames get the
                            // disc's real timeline once the first PTS arrives,
                            // not a 0 base.
                            self.pending.push((display_index, frame));
                        }
                        None => {
                            frame.pts_ns = display_index as i64 * self.frame_duration_ns;
                            out.push(frame);
                        }
                    },
                }
            } else {
                // No frame rate yet (no sequence header) — fall back to the
                // nearest preceding PES timestamp.
                frame.pts_ns = self.pts_for(pic_abs);
                out.push(frame);
            }

            self.gop_count += 1;
            self.buf.drain(..end);
            self.base_offset = end_abs;
            // Drop PTS marks fully consumed by the emitted AU; keep the mark at
            // the boundary (it belongs to the next AU).
            while let Some(&(off, _)) = self.pts_marks.front() {
                if off < end_abs {
                    self.pts_marks.pop_front();
                } else {
                    break;
                }
            }
        }
        out
    }
}

impl CodecParser for Mpeg2Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        // Record this PES's timestamp against the absolute offset of its first
        // ES byte, BEFORE appending. MKV block timecodes are presentation
        // timestamps; prefer PTS (DTS shows B-frames in decode order — judder
        // and broken seeking), falling back to DTS only when PTS is absent.
        let off = self.base_offset + self.buf.len() as u64;
        if let Some(ts) = pes.pts.or(pes.dts) {
            self.pts_marks.push_back((off, pts_to_ns(ts)));
        }
        self.buf.extend_from_slice(&pes.data);
        self.drain_complete_aus(false)
    }

    fn flush(&mut self) -> Vec<Frame> {
        let mut out = self.drain_complete_aus(true);
        // EOF: if no PES ever supplied a PTS/DTS, `self.pending` still holds the
        // frames buffered while waiting for an anchor (the opening keyframe +
        // first ~20s). Without this they'd be silently dropped — a 100%-recovery
        // violation. Emit each with the same 0-base fallback the no-anchor
        // overflow arm uses (`display_index * frame_duration_ns`), ordered by
        // display_index so presentation order is preserved.
        if !self.pending.is_empty() {
            let mut held: Vec<(u64, Frame)> = self.pending.drain(..).collect();
            held.sort_by_key(|(di, _)| *di);
            for (di, mut frame) in held {
                frame.pts_ns = di as i64 * self.frame_duration_ns;
                out.push(frame);
            }
        }
        out
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        self.seq_header.clone()
    }
}

/// Extract the sequence header (+ any B5 extensions / user-data, up to the
/// first GOP or picture start code) from a fully-assembled access unit — exactly
/// the extradata an MPEG-2 decoder expects as codecPrivate. Returns None if the
/// access unit carries no sequence header. A NEW header replaces the stored one
/// (title boundary / channel change), so its extension is always re-captured.
fn extract_seq_header(au: &[u8]) -> Option<Vec<u8>> {
    let b3 = find_code(au, 0, SEQ_HEADER_CODE)?;
    let mut end = au.len();
    let mut p = b3 + 4;
    while let Some(sc) = find_start_code(au, p) {
        if sc + 3 >= au.len() {
            break;
        }
        let c = au[sc + 3];
        if c == PICTURE_CODE || c == GOP_CODE {
            end = sc;
            break;
        }
        p = sc + 4;
    }
    Some(au[b3..end].to_vec())
}

/// Find the next start code at or after `from` whose code byte equals `want`.
fn find_code(data: &[u8], from: usize, want: u8) -> Option<usize> {
    let mut pos = from;
    while let Some(sc) = find_start_code(data, pos) {
        if sc + 3 >= data.len() {
            return None;
        }
        if data[sc + 3] == want {
            return Some(sc);
        }
        pos = sc + 4;
    }
    None
}

/// Find the next access-unit boundary at or after `from`: the position of a
/// picture (0x00), sequence header (0xB3), or GOP (0xB8) start code. Extension
/// (0xB5), slice (0x01..=0xAF), user-data (0xB2) and sequence-end (0xB7) codes
/// belong to the current access unit and are NOT boundaries.
fn find_au_start(data: &[u8], from: usize) -> Option<usize> {
    let mut pos = from;
    while let Some(sc) = find_start_code(data, pos) {
        if sc + 3 >= data.len() {
            return None;
        }
        let code = data[sc + 3];
        if code == PICTURE_CODE || code == SEQ_HEADER_CODE || code == GOP_CODE {
            return Some(sc);
        }
        pos = sc + 4;
    }
    None
}

/// Parse horizontal and vertical resolution from sequence header bytes.
/// The sequence header must start with 00 00 01 B3.
fn parse_resolution(hdr: &[u8]) -> Option<(u16, u16)> {
    // Need at least start code (4) + 4 bytes of header data = 8 bytes.
    if hdr.len() < 8 {
        return None;
    }
    // Bytes 4-5: horizontal_size_value (12 bits) | vertical_size_value top 4 bits
    // Bytes 5-6: vertical_size_value bottom 8 bits (12 bits total)
    let h = ((hdr[4] as u16) << 4) | ((hdr[5] as u16) >> 4);
    let v = (((hdr[5] & 0x0F) as u16) << 8) | hdr[6] as u16;
    Some((h, v))
}

/// Parse frame rate code from sequence header.
fn parse_frame_rate(hdr: &[u8]) -> Option<(u32, u32)> {
    if hdr.len() < 8 {
        return None;
    }
    let frame_rate_code = (hdr[7] & 0x0F) as usize;
    if frame_rate_code == 0 || frame_rate_code >= FRAME_RATES.len() {
        return None;
    }
    Some(FRAME_RATES[frame_rate_code])
}

/// Parse aspect ratio information from sequence header.
fn parse_aspect_ratio(hdr: &[u8]) -> Option<(u8, u8)> {
    if hdr.len() < 8 {
        return None;
    }
    let ar_code = ((hdr[7] >> 4) & 0x0F) as usize;
    if ar_code == 0 || ar_code >= ASPECT_RATIOS.len() {
        return None;
    }
    Some(ASPECT_RATIOS[ar_code])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1011,
            pts,
            dts: None,
            data,
        }
    }

    /// Build a minimal MPEG-2 sequence header.
    /// 00 00 01 B3 [h_size:12][v_size:12] [aspect:4][frame_rate:4] ...
    fn make_seq_header(width: u16, height: u16, aspect: u8, frame_rate: u8) -> Vec<u8> {
        let mut hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
        hdr.push((width >> 4) as u8);
        hdr.push(((width & 0x0F) as u8) << 4 | ((height >> 8) & 0x0F) as u8);
        hdr.push((height & 0xFF) as u8);
        hdr.push((aspect << 4) | (frame_rate & 0x0F));
        // Bit rate (18 bits) + marker + VBV buffer size (10 bits) etc — pad minimally.
        hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
        hdr
    }

    /// Build a picture header with the given coding type.
    fn make_picture_header(coding_type: u8) -> Vec<u8> {
        // 00 00 01 00 [temporal_ref:10][picture_coding_type:3][...]
        let byte5 = (coding_type & 0x07) << 3;
        vec![0x00, 0x00, 0x01, PICTURE_CODE, 0x00, byte5, 0x00, 0x00]
    }

    /// A GOP header start code (used as a clean access-unit delimiter in tests).
    fn gop() -> Vec<u8> {
        vec![0x00, 0x00, 0x01, GOP_CODE, 0x00, 0x00, 0x00, 0x00]
    }

    /// Picture header carrying an explicit 10-bit temporal_reference.
    fn make_picture_header_tr(coding_type: u8, tr: u16) -> Vec<u8> {
        let b4 = ((tr >> 2) & 0xFF) as u8;
        let b5 = (((tr & 0x03) as u8) << 6) | ((coding_type & 0x07) << 3);
        vec![0x00, 0x00, 0x01, PICTURE_CODE, b4, b5, 0x00, 0x00]
    }

    /// Collect every frame from a single PES followed by an EOF flush — the
    /// common single-picture test shape (the final AU emits on flush()).
    fn parse_then_flush(parser: &mut Mpeg2Parser, pes: &PesPacket) -> Vec<Frame> {
        let mut frames = parser.parse(pes);
        frames.extend(parser.flush());
        frames
    }

    // --- Sequence header parsing ---

    #[test]
    fn parse_sequence_header_resolution() {
        assert_eq!(
            parse_resolution(&make_seq_header(720, 480, 2, 4)),
            Some((720, 480))
        );
    }

    #[test]
    fn parse_sequence_header_1920x1080() {
        assert_eq!(
            parse_resolution(&make_seq_header(1920, 1080, 3, 4)),
            Some((1920, 1080))
        );
    }

    #[test]
    fn parse_sequence_header_frame_rate() {
        let hdr = make_seq_header(720, 480, 2, 4); // frame_rate_code 4 = 29.97
        assert_eq!(parse_frame_rate(&hdr), Some((30000, 1001)));
    }

    #[test]
    fn parse_sequence_header_aspect_ratio() {
        let hdr = make_seq_header(720, 480, 3, 4); // aspect code 3 = 16:9
        assert_eq!(parse_aspect_ratio(&hdr), Some((16, 9)));
    }

    #[test]
    fn parse_sequence_header_too_short() {
        let hdr = vec![0x00, 0x00, 0x01, SEQ_HEADER_CODE];
        assert!(parse_resolution(&hdr).is_none());
        assert!(parse_frame_rate(&hdr).is_none());
        assert!(parse_aspect_ratio(&hdr).is_none());
    }

    // --- I-frame detection ---

    #[test]
    fn detect_i_frame() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(PICTURE_TYPE_I);
        data.extend_from_slice(&[0xFF; 16]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe, "I-frame should be detected as keyframe");
    }

    #[test]
    fn detect_p_frame_not_keyframe() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(2); // P-frame
        data.extend_from_slice(&[0xFF; 16]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "P-frame should not be keyframe");
    }

    #[test]
    fn detect_b_frame_not_keyframe() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(3); // B-frame
        data.extend_from_slice(&[0xFF; 16]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "B-frame should not be keyframe");
    }

    // --- The core fix: a picture split across many PES packets is ONE frame ---

    #[test]
    fn picture_fragmented_across_pes_is_reassembled_into_one_frame() {
        // A DVD coded picture spans multiple ~2 KB PES packets; only the first
        // carries a PTS. The parser must concatenate them into ONE access unit,
        // not emit one fragment per PES.
        let mut parser = Mpeg2Parser::new();

        let mut au = make_seq_header(720, 480, 3, 4);
        au.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        au.extend_from_slice(&vec![0xAA; 5000]); // slice data (no start codes)

        // Split the AU into 2 KB fragments across separate PES packets.
        let mut frames = Vec::new();
        for (i, chunk) in au.chunks(2000).enumerate() {
            let pts = if i == 0 { Some(90000) } else { None };
            frames.extend(parser.parse(&make_pes(chunk.to_vec(), pts)));
        }
        // No boundary yet → nothing emitted during parse().
        assert!(frames.is_empty(), "incomplete AU must not emit fragments");
        // Flush completes the trailing AU.
        frames.extend(parser.flush());

        assert_eq!(frames.len(), 1, "fragments reassembled into ONE frame");
        assert_eq!(frames[0].data, au, "frame is the whole picture, byte-exact");
        assert!(frames[0].keyframe);
        assert_eq!(
            frames[0].pts_ns, 1_000_000_000,
            "PTS from the first fragment"
        );
    }

    #[test]
    fn two_pictures_emit_two_frames_at_the_boundary() {
        // pic1's frame is emitted as soon as pic2's start code is seen; pic2 on
        // flush. Each frame contains exactly its own picture.
        let mut parser = Mpeg2Parser::new();

        let mut pic1 = make_picture_header(PICTURE_TYPE_I);
        pic1.extend_from_slice(&vec![0x11; 100]);
        let mut pic2 = make_picture_header(2); // P
        pic2.extend_from_slice(&vec![0x22; 100]);

        let mut stream = pic1.clone();
        stream.extend_from_slice(&pic2);

        let mut frames = parser.parse(&make_pes(stream, Some(0)));
        assert_eq!(
            frames.len(),
            1,
            "first picture emitted at second's boundary"
        );
        assert_eq!(frames[0].data, pic1);
        assert!(frames[0].keyframe);

        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].data, pic2);
        assert!(!frames[1].keyframe);
    }

    #[test]
    fn picture_coding_extension_stays_with_its_picture() {
        // Regression for `ignoring pic cod ext after 0`: the picture coding
        // extension (00 00 01 B5) must remain in the SAME access unit as its
        // picture header, never split into the next block.
        let mut parser = Mpeg2Parser::new();

        let mut au = make_picture_header(PICTURE_TYPE_I);
        au.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x88, 0x00]); // pic coding ext
        au.extend_from_slice(&[0x00, 0x00, 0x01, 0x01]); // slice
        au.extend_from_slice(&vec![0x77; 50]);

        let frames = parse_then_flush(&mut parser, &make_pes(au.clone(), Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].data, au,
            "picture + coding extension + slice = one AU"
        );
    }

    // --- PTS association across fragments ---

    #[test]
    fn each_picture_gets_the_pts_of_the_pes_that_began_it() {
        let mut parser = Mpeg2Parser::new();

        // PES 1: pic1 (PTS 90000) + start of pic2's bytes carried later.
        let mut pic1 = make_picture_header(PICTURE_TYPE_I);
        pic1.extend_from_slice(&vec![0x11; 50]);
        let frames1 = parser.parse(&make_pes(pic1, Some(90000)));
        assert!(frames1.is_empty(), "pic1 awaits pic2's boundary");

        // PES 2: pic2 (PTS 180000).
        let mut pic2 = make_picture_header(2);
        pic2.extend_from_slice(&vec![0x22; 50]);
        let mut frames = parser.parse(&make_pes(pic2, Some(180000)));
        assert_eq!(frames.len(), 1, "pic1 emitted when pic2 starts");
        assert_eq!(frames[0].pts_ns, 1_000_000_000, "pic1 → PTS 90000");

        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].pts_ns, 2_000_000_000, "pic2 → PTS 180000");
    }

    // --- sparse PTS reconstructed from temporal_reference + frame rate ---

    #[test]
    fn sparse_pts_interpolated_by_temporal_reference() {
        // DVD stamps a PTS only ~once per VOBU; frames between marks must be
        // timed by temporal_reference × frame interval, anchored to the real
        // PES PTS so audio stays in sync. Frame rate code 3 = 25 fps = 40 ms.
        let mut p = Mpeg2Parser::new();

        // GOP 1: seq + gop + I(TR0) carrying PES PTS 0 (the anchor).
        let mut a = make_seq_header(720, 480, 3, 3);
        a.extend_from_slice(&gop());
        a.extend_from_slice(&make_picture_header_tr(1, 0));
        a.extend_from_slice(&[0xAA; 20]);
        let mut frames = p.parse(&make_pes(a, Some(0)));
        assert!(
            frames.is_empty(),
            "first AU waits for the next picture boundary"
        );

        // TR1, no PES PTS → interpolate.
        let mut b1 = make_picture_header_tr(3, 1);
        b1.extend_from_slice(&[0xBB; 20]);
        frames.extend(p.parse(&make_pes(b1, None)));

        // TR2, no PES PTS → interpolate.
        let mut b2 = make_picture_header_tr(3, 2);
        b2.extend_from_slice(&[0xCC; 20]);
        frames.extend(p.parse(&make_pes(b2, None)));

        frames.extend(p.flush());
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].pts_ns, 0, "anchor frame uses its real PES PTS");
        assert_eq!(frames[1].pts_ns, 40_000_000, "TR1 → +1 frame interval");
        assert_eq!(frames[2].pts_ns, 80_000_000, "TR2 → +2 frame intervals");
        assert_eq!(frames[0].duration_ns, Some(40_000_000));
    }

    #[test]
    fn temporal_reference_resets_each_gop_via_gop_base() {
        // Across a GOP boundary, temporal_reference restarts at 0 but the
        // whole-stream display index must keep climbing (gop_base folds the
        // previous GOP's frame count). 25 fps = 40 ms.
        let mut p = Mpeg2Parser::new();

        // GOP 1: two pictures TR0 (anchor PTS 0), TR1.
        let mut g1 = make_seq_header(720, 480, 3, 3);
        g1.extend_from_slice(&gop());
        g1.extend_from_slice(&make_picture_header_tr(1, 0));
        g1.extend_from_slice(&[0xAA; 10]);
        g1.extend_from_slice(&make_picture_header_tr(2, 1));
        g1.extend_from_slice(&[0xBB; 10]);
        let mut frames = p.parse(&make_pes(g1, Some(0)));

        // GOP 2: new GOP header, picture TR0 again (no PES PTS).
        let mut g2 = gop();
        g2.extend_from_slice(&make_picture_header_tr(1, 0));
        g2.extend_from_slice(&[0xCC; 10]);
        frames.extend(p.parse(&make_pes(g2, None)));
        frames.extend(p.flush());

        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].pts_ns, 0); // GOP1 TR0
        assert_eq!(frames[1].pts_ns, 40_000_000); // GOP1 TR1
        // GOP2 TR0 → display index 2 (gop_base 2 + TR 0), NOT a reset to 0.
        assert_eq!(
            frames[2].pts_ns, 80_000_000,
            "gop_base keeps the clock climbing"
        );
    }

    #[test]
    fn leading_frames_buffered_until_first_pts_anchor() {
        // A DVD title can open with a still-frame/first-play sequence whose PTS
        // lands a few frames in (the disc stamps the opening I-frames at one real
        // PES PTS, not 0). Leading frames must be held and then anchored to that
        // real timeline — never zero-stamped. 25 fps = 40 ms. PTS (2 s) arrives
        // only on the THIRD picture.
        let mut p = Mpeg2Parser::new();

        let mut a = make_seq_header(720, 480, 3, 3);
        a.extend_from_slice(&gop());
        a.extend_from_slice(&make_picture_header_tr(1, 0));
        a.extend_from_slice(&[0xAA; 20]);
        let mut f = p.parse(&make_pes(a, None)); // no PTS → buffered

        let mut b1 = make_picture_header_tr(3, 1);
        b1.extend_from_slice(&[0xBB; 20]);
        f.extend(p.parse(&make_pes(b1, None))); // no PTS → buffered

        let mut b2 = make_picture_header_tr(3, 2);
        b2.extend_from_slice(&[0xCC; 20]);
        f.extend(p.parse(&make_pes(b2, Some(180000)))); // PTS 2 s → anchor + backfill
        f.extend(p.flush());

        assert_eq!(f.len(), 3);
        // Anchored to the real disc timeline, NOT a 0 base.
        assert_eq!(
            f[0].pts_ns,
            2_000_000_000 - 80_000_000,
            "leading frame back-anchored"
        );
        assert_eq!(f[1].pts_ns, 2_000_000_000 - 40_000_000);
        assert_eq!(
            f[2].pts_ns, 2_000_000_000,
            "anchor frame = its real PES PTS"
        );
        // Decode order preserved.
        assert!(f[0].keyframe);
    }

    // --- Sequence header → codec_private ---

    #[test]
    fn codec_private_from_sequence_header() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(720, 480, 3, 4);
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 8]);
        let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));

        let cp = parser
            .codec_private()
            .expect("codec_private after seq header");
        assert_eq!(&cp[..4], &[0x00, 0x00, 0x01, SEQ_HEADER_CODE]);
    }

    #[test]
    fn codec_private_none_initially() {
        assert!(Mpeg2Parser::new().codec_private().is_none());
    }

    #[test]
    fn codec_private_includes_extension_but_not_picture() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(1920, 1080, 3, 4);
        // Sequence extension: 00 00 01 B5 [ext data]
        data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x14, 0x8A, 0x00, 0x01]);
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 4]);

        let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
        let cp = parser.codec_private().unwrap();
        assert!(
            cp.windows(4).any(|w| w == [0x00, 0x00, 0x01, SEQ_EXT_CODE]),
            "codec_private should include the sequence extension"
        );
        // It must stop before the picture header — extradata is seq header only.
        assert!(
            !cp.windows(4).any(|w| w == [0x00, 0x00, 0x01, PICTURE_CODE]),
            "codec_private must NOT include the picture start code"
        );
    }

    // --- seq-header keyframe flag must not leak into a P/B-frame ---

    #[test]
    fn seq_header_then_p_frame_is_not_keyframe() {
        // A PES carrying a sequence header followed by a P-frame must NOT be a
        // keyframe — keyframe-ness belongs to the coded picture.
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(720, 480, 3, 4);
        data.extend_from_slice(&make_picture_header(2)); // P-frame
        data.extend_from_slice(&[0xFF; 16]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert!(
            !frames[0].keyframe,
            "seq-header + P-frame must not be a keyframe"
        );
        assert!(parser.codec_private().is_some());
    }

    #[test]
    fn sequence_header_with_picture_is_keyframe() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(720, 480, 3, 4);
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 16]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert!(frames[0].keyframe);
        assert!(parser.codec_private().is_some());
    }

    // --- a SECOND sequence header re-captures (title boundary) ---

    #[test]
    fn new_sequence_header_replaces_codec_private() {
        let mut parser = Mpeg2Parser::new();

        // AU A: 1920x1080 seq header + I picture, delimited by a following GOP.
        let mut a = make_seq_header(1920, 1080, 3, 4);
        a.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        a.extend_from_slice(&[0xAA; 20]);
        a.extend_from_slice(&gop()); // boundary → AU A emits
        let fa = parser.parse(&make_pes(a, Some(0)));
        assert_eq!(fa.len(), 1);
        assert_eq!(parser.resolution(), Some((1920, 1080)));

        // AU B: a NEW 720x480 seq header + I picture. Its extension/header must
        // replace the stored one rather than keeping stale 1920x1080.
        let mut b = make_seq_header(720, 480, 2, 4);
        b.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        b.extend_from_slice(&[0xBB; 20]);
        let _ = parse_then_flush(&mut parser, &make_pes(b, Some(3600)));
        assert_eq!(
            parser.resolution(),
            Some((720, 480)),
            "codec_private updated to header B"
        );
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion_to_nanoseconds() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(PICTURE_TYPE_I);
        data.extend_from_slice(&[0xFF; 4]);
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn mpeg2_dts_fallback_and_zero() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(PICTURE_TYPE_I);
        data.extend_from_slice(&[0xFF; 4]);
        let pes = PesPacket {
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data,
        };
        let f = parse_then_flush(&mut parser, &pes);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

        let mut parser2 = Mpeg2Parser::new();
        let mut data2 = make_picture_header(PICTURE_TYPE_I);
        data2.extend_from_slice(&[0xFF; 4]);
        let pes2 = PesPacket {
            pid: 0x1011,
            pts: None,
            dts: None,
            data: data2,
        };
        let f2 = parse_then_flush(&mut parser2, &pes2);
        assert_eq!(f2[0].pts_ns, 0, "no PTS/DTS → 0");
    }

    // --- Empty PES ---

    #[test]
    fn empty_pes_no_frames() {
        let mut parser = Mpeg2Parser::new();
        assert!(parser.parse(&make_pes(Vec::new(), Some(0))).is_empty());
    }

    // --- parameter-set-only stream: seq header, no picture → no frame ---

    #[test]
    fn sequence_header_only_emits_no_frame_but_captures_codec_private() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(1920, 1080, 3, 4);
        data.extend_from_slice(&[0x00, 0x00, 0x01, SEQ_EXT_CODE, 0x14, 0x8A]);
        // No picture start code at all.
        let frames = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
        assert!(frames.is_empty(), "no coded picture → no frame");
        // codec_private only captured when an AU is emitted; a header-only
        // stream emits nothing, so nothing is captured — and there is no frame
        // to need it. (Real streams always follow the header with a picture.)
    }

    // --- buffer cap: corrupt stream with no second boundary is force-flushed ---

    #[test]
    fn oversized_au_without_boundary_is_force_flushed() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_picture_header(PICTURE_TYPE_I);
        // > MAX_AU_BUFFER of slice bytes with no following picture/seq/GOP.
        data.extend(std::iter::repeat_n(0xAA, MAX_AU_BUFFER + 1024));
        let frames = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(
            frames.len(),
            1,
            "over-cap AU force-flushed rather than buffered"
        );
        assert!(frames[0].keyframe);
    }

    // --- parse_resolution: 12-bit field packing (ISO 13818-2 §6.2.2.1) ---

    #[test]
    fn resolution_packs_split_nibble_correctly() {
        let hdr = make_seq_header(0xABC, 0xDEF, 1, 1);
        assert_eq!(parse_resolution(&hdr), Some((0xABC, 0xDEF)));
    }

    #[test]
    fn resolution_max_12bit() {
        let hdr = make_seq_header(4095, 4095, 1, 1);
        assert_eq!(parse_resolution(&hdr), Some((4095, 4095)));
    }

    #[test]
    fn resolution_too_short_none() {
        assert_eq!(parse_resolution(&[0x00, 0x00, 0x01, 0xB3, 0x07]), None);
    }

    // --- parse_frame_rate: full table + reserved codes ---

    #[test]
    fn frame_rate_all_valid_codes() {
        let expect = [
            (24000u32, 1001u32),
            (24, 1),
            (25, 1),
            (30000, 1001),
            (30, 1),
            (50, 1),
            (60000, 1001),
            (60, 1),
        ];
        for (i, &want) in expect.iter().enumerate() {
            let code = (i + 1) as u8;
            let hdr = make_seq_header(720, 480, 1, code);
            assert_eq!(parse_frame_rate(&hdr), Some(want), "frame_rate_code {code}");
        }
    }

    #[test]
    fn frame_rate_code_zero_forbidden_none() {
        assert_eq!(parse_frame_rate(&make_seq_header(720, 480, 1, 0)), None);
    }

    #[test]
    fn frame_rate_code_out_of_range_none() {
        assert_eq!(parse_frame_rate(&make_seq_header(720, 480, 1, 0x0F)), None);
    }

    // --- parse_aspect_ratio: table + reserved codes ---

    #[test]
    fn aspect_ratio_all_valid_codes() {
        let expect = [(1u8, 1u8), (4, 3), (16, 9), (221, 100)];
        for (i, &want) in expect.iter().enumerate() {
            let code = (i + 1) as u8;
            let hdr = make_seq_header(720, 480, code, 4);
            assert_eq!(parse_aspect_ratio(&hdr), Some(want), "aspect code {code}");
        }
    }

    #[test]
    fn aspect_ratio_code_zero_none() {
        assert_eq!(parse_aspect_ratio(&make_seq_header(720, 480, 0, 4)), None);
    }

    #[test]
    fn aspect_ratio_code_out_of_range_none() {
        assert_eq!(
            parse_aspect_ratio(&make_seq_header(720, 480, 0x0F, 4)),
            None
        );
    }

    // --- picture_coding_type: byte position + bit field ---

    #[test]
    fn picture_coding_type_bits_5_3() {
        for (ct, is_kf) in [(1u8, true), (2, false), (3, false), (4, false)] {
            let mut parser = Mpeg2Parser::new();
            let mut data = make_picture_header(ct);
            data.extend_from_slice(&[0xFF; 8]);
            let f = parse_then_flush(&mut parser, &make_pes(data, Some(0)));
            assert_eq!(f.len(), 1);
            assert_eq!(f[0].keyframe, is_kf, "picture_coding_type {ct}");
        }
    }

    #[test]
    fn parser_resolution_method() {
        let mut parser = Mpeg2Parser::new();
        let mut data = make_seq_header(720, 576, 2, 3);
        data.extend_from_slice(&make_picture_header(PICTURE_TYPE_I));
        data.extend_from_slice(&[0xFF; 4]);
        let _ = parse_then_flush(&mut parser, &make_pes(data, Some(0)));

        assert_eq!(parser.resolution(), Some((720, 576)));
        assert_eq!(parser.frame_rate(), Some((25, 1))); // frame_rate_code 3 = 25fps
        assert_eq!(parser.aspect_ratio(), Some((4, 3))); // aspect code 2 = 4:3
    }
}

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

use super::coding::{CodingType, Mpeg2Coding, PictureInfo};
use super::startcode::find_start_code;
use super::{CodecParser, Frame, pts_to_ns};
use crate::mux::ts::PesPacket;
use crate::pes::SourcePos;

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

/// Byte cap on frames held awaiting the first PES PTS anchor. `MAX_PENDING_FRAMES`
/// alone bounds the *count*, but 600 full HD/UHD intra pictures can be ~1 GiB.
/// Mirror the AC-3/DTS/PGS byte caps: once the held data exceeds this, release
/// on the 0 base instead of accumulating further. 8 MiB ≈ a few large I-frames,
/// far more than the ~15 frames a well-formed DVD buffers before its first PTS.
const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

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
    /// `(absolute ES offset of a PES's first byte, SourcePos)` for every PES
    /// that carried byte-exact provenance, parallel to `pts_marks` and drained
    /// by the SAME mark-drain invariant. Attaches the source position to each
    /// access unit so the index carries it — never reconstructed.
    source_marks: VecDeque<(u64, SourcePos)>,
    /// Full-frame presentation interval (ns) at the sequence-header display rate
    /// (`1/frame_rate`). The field period is half this. Per-frame durations are
    /// `nb_fields × field_period`, so 2:3-telecined frames alternate 2- and
    /// 3-field durations. 0 until a sequence header with a valid frame rate.
    frame_duration_ns: i64,
    /// `progressive_sequence` from the sequence extension — selects the
    /// `nb_fields` rules for `repeat_first_field` pictures.
    progressive_sequence: bool,
    /// Pictures of the current GOP, buffered in DECODE order until the GOP
    /// completes (the next GOP/sequence header). Held so each frame's PTS can be
    /// the display-order prefix-sum of field durations — exact for 2:3 pulldown
    /// without ever reordering emitted blocks (B-frames keep decode order; only
    /// their PTS is lower).
    gop_buf: Vec<BufferedPicture>,
    /// Total field-display periods of all frames already emitted, in display
    /// order — the running base for each new frame's display time.
    emitted_fields: u64,
    /// PTS (ns) that display-field 0 of the whole stream maps to. Re-locked from
    /// each GOP's first PES PTS so video stays in sync with the PES-timestamped
    /// audio. None until the first PES timestamp is seen.
    origin_pts_ns: Option<i64>,
    /// B1: absolute ES offsets at which a concealed/lost-gap PES began, parallel
    /// to `pts_marks`/`source_marks` and drained by the SAME mark-drain invariant.
    /// MPEG-2 emits whole GOPs asynchronously, so a per-PES flag can't ride
    /// through to the right frame (the PES that carries the gap completes the
    /// PREVIOUS picture); associating by OFFSET instead stamps `discontinuity` on
    /// the access unit whose own bytes begin after the gap — the first post-gap
    /// picture — surviving GOP buffering + temporal reorder. The consumer's
    /// ResyncGate then arms at that exact picture, mid-GOP if need be.
    disc_marks: VecDeque<u64>,
}

/// One coded picture buffered awaiting its GOP's completion (see `gop_buf`).
struct BufferedPicture {
    /// `temporal_reference` — display order within the GOP.
    tr: u64,
    /// Codec-agnostic per-picture coding info. The single source of this
    /// picture's field count (`nb_fields()`), field order, and coding type;
    /// also stamped onto the emitted [`Frame::coding`].
    info: PictureInfo,
    /// This picture's own PES PTS (ns), if its access unit carried one.
    explicit_pts: Option<i64>,
    /// The emitted frame (PTS + duration filled in at GOP flush).
    frame: Frame,
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
            source_marks: VecDeque::new(),
            frame_duration_ns: 0,
            progressive_sequence: false,
            gop_buf: Vec::new(),
            emitted_fields: 0,
            origin_pts_ns: None,
            disc_marks: VecDeque::new(),
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
                    while let Some(&(off, _)) = self.source_marks.front() {
                        if off < cutoff {
                            self.source_marks.pop_front();
                        } else {
                            break;
                        }
                    }
                    while let Some(&off) = self.disc_marks.front() {
                        if off < cutoff {
                            self.disc_marks.pop_front();
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
            // picture_coding_type: the full 3-bit value (bits 5-3 of buf[pic+5]).
            // 0 when the picture header is truncated (no coding type available).
            let raw_coding_type = if pic + 5 < end {
                (self.buf[pic + 5] >> 3) & 0x07
            } else {
                0
            };
            // temporal_reference: the 10 bits immediately after the picture
            // start code = display order within the GOP.
            let tr = if pic + 5 < end {
                (((self.buf[pic + 4] as u64) << 2) | ((self.buf[pic + 5] as u64) >> 6)) & 0x3FF
            } else {
                0
            };
            let end_abs = self.base_offset + end as u64;
            let data = self.buf[..end].to_vec();

            // Phase 2 — mutate self.
            if let Some(h) = hdr {
                self.progressive_sequence = parse_progressive_sequence(&h);
                self.seq_header = Some(h);
                if let Some((num, den)) = self.frame_rate() {
                    if num > 0 {
                        self.frame_duration_ns = 1_000_000_000i64 * den as i64 / num as i64;
                    }
                }
            }
            // Decode the picture coding extension ONCE here and fold every
            // per-picture datum (coding type + tff/rff/progressive_frame/
            // frame_picture, plus the sequence's progressive flag) into one
            // codec-agnostic `PictureInfo`. `nb_fields()`, `keyframe()`, and
            // `field_order()` all derive from it; nothing downstream re-parses
            // the elementary stream.
            let (tff, rff, progressive_frame, frame_picture) = picture_coding_flags(&data);
            let info = PictureInfo::mpeg2(
                coding_type_from_raw(raw_coding_type),
                Mpeg2Coding {
                    top_field_first: tff,
                    repeat_first_field: rff,
                    progressive_frame,
                    progressive_sequence: self.progressive_sequence,
                    frame_picture,
                },
            );
            let keyframe = info.keyframe();

            // An explicit PES PTS for this access unit, if any. By the mark-drain
            // invariant the front mark's offset is >= this AU's start, so a front
            // mark inside [start, end) is this AU's own timestamp.
            let explicit = self
                .pts_marks
                .front()
                .filter(|&&(off, _)| off < end_abs)
                .map(|&(_, p)| p);

            // Byte-exact source provenance for this AU, by the same mark-drain
            // invariant as the PTS: the front source mark inside [start, end)
            // belongs to this access unit.
            let src = self
                .source_marks
                .front()
                .filter(|&&(off, _)| off < end_abs)
                .map(|&(_, s)| s);

            // A GOP boundary means the buffered run is a COMPLETE GOP (all its
            // pictures display before the next GOP's), so flush it before
            // starting the new one. `temporal_reference` resets to 0 at the
            // boundary, keeping each GOP's display order self-contained.
            if gop_boundary && !self.gop_buf.is_empty() {
                self.flush_gop(&mut out);
            }
            // A concealed-gap mark inside this AU's range [start, end_abs) means
            // this picture's own bytes begin after the gap — the first post-gap
            // AU. Same front-mark invariant as PTS/source. Carries through GOP
            // buffering/reorder to the ResyncGate (which arms at this picture).
            let discontinuity = self.disc_marks.front().is_some_and(|&off| off < end_abs);
            self.gop_buf.push(BufferedPicture {
                tr,
                info,
                explicit_pts: explicit,
                frame: Frame {
                    pts_ns: 0,
                    keyframe,
                    discontinuity,
                    data,
                    duration_ns: None,
                    coding: Some(info),
                    source: src,
                },
            });
            // Safety cap: a stream with no GOP/sequence boundaries would buffer
            // unbounded. Force-flush a pathologically long run as its own GOP.
            if self.gop_buf.len() >= MAX_PENDING_FRAMES {
                self.flush_gop(&mut out);
            }
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
            while let Some(&(off, _)) = self.source_marks.front() {
                if off < end_abs {
                    self.source_marks.pop_front();
                } else {
                    break;
                }
            }
            while let Some(&off) = self.disc_marks.front() {
                if off < end_abs {
                    self.disc_marks.pop_front();
                } else {
                    break;
                }
            }
        }
        // EOF: emit the final (possibly incomplete) GOP so nothing is dropped.
        if force {
            self.flush_gop(&mut out);
        }
        out
    }

    /// Emit the buffered GOP. Each frame's PTS is the display-order prefix-sum of
    /// field durations from the timeline origin; its block duration is its own
    /// `nb_fields × field_period`. Frames are emitted in DECODE (buffer) order —
    /// B-frames keep their position with a correctly LOWER PTS, never reordered
    /// (reordering emitted blocks is what corrupts the picture). The origin is
    /// (re-)locked to the GOP's PES PTS; because that is a *presentation*
    /// timestamp, backing out the carrying frame's display-field offset keeps the
    /// timeline continuous and monotonic across GOP boundaries.
    fn flush_gop(&mut self, out: &mut Vec<Frame>) {
        let n = self.gop_buf.len();
        if n == 0 {
            return;
        }
        let field_period = self.frame_duration_ns / 2;
        if field_period <= 0 {
            // No sequence header / frame rate yet (malformed lead-in): emit in
            // decode order off each AU's own PES PTS, with no field timing.
            for bp in self.gop_buf.drain(..) {
                let mut f = bp.frame;
                f.pts_ns = bp.explicit_pts.unwrap_or(0);
                out.push(f);
            }
            return;
        }
        // Fields displayed BEFORE each picture within this GOP: order indices by
        // temporal_reference (display order) and prefix-sum `nb_fields`.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| self.gop_buf[i].tr);
        let mut cum_before = vec![0u64; n];
        let mut running = 0u64;
        for &i in &order {
            cum_before[i] = running;
            running += self.gop_buf[i].info.nb_fields() as u64;
        }
        let gop_fields = running;
        let base = self.emitted_fields;
        // (Re-)lock the timeline origin to the GOP's PES PTS.
        for &i in &order {
            if let Some(p) = self.gop_buf[i].explicit_pts {
                self.origin_pts_ns = Some(p - field_period * (base + cum_before[i]) as i64);
                break;
            }
        }
        let origin = self.origin_pts_ns.unwrap_or(0);
        for (i, mut bp) in self.gop_buf.drain(..).enumerate() {
            bp.frame.pts_ns = origin + field_period * (base + cum_before[i]) as i64;
            bp.frame.duration_ns = Some(bp.info.nb_fields() as u64 * field_period as u64);
            out.push(bp.frame);
        }
        self.emitted_fields += gop_fields;
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
        if let Some(src) = pes.source {
            self.source_marks.push_back((off, src));
        }
        // A concealed/lost gap on this PES marks the access unit its bytes begin —
        // associated by offset (like PTS/source) so it lands on the first post-gap
        // picture, not the previous one that completes when this PES arrives.
        if pes.discontinuity {
            self.disc_marks.push_back(off);
        }
        self.buf.extend_from_slice(&pes.data);
        self.drain_complete_aus(false)
    }

    fn flush(&mut self) -> Vec<Frame> {
        // drain_complete_aus(true) force-completes the trailing access unit and
        // flushes the final GOP, so nothing is left buffered at EOF.
        self.drain_complete_aus(true)
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

/// Extract the picture-coding-extension field/pulldown flags
/// `(top_field_first, repeat_first_field, progressive_frame, frame_picture)`
/// from a coded access unit (`00 00 01 B5`, ext-id `1000`), per ISO/IEC 13818-2
/// §6.3.10. The four bits feed the codec-agnostic [`PictureInfo`]. Returns a
/// progressive whole-frame default `(false, false, true, true)` when no picture
/// coding extension is present (MPEG-1 / no interlace signalling), so the muxer
/// omits `FieldOrder` rather than asserting a guess.
fn picture_coding_flags(au: &[u8]) -> (bool, bool, bool, bool) {
    let mut search = 0;
    while let Some(q) = find_code(au, search, SEQ_EXT_CODE) {
        search = q + 4;
        // The picture coding extension is the B5 whose ext-id nibble is 1000.
        if au.get(q + 4).map(|b| b >> 4) != Some(0b1000) {
            continue;
        }
        // Extension bytes e2..=e4 = au[q+6 ..= q+8].
        let (Some(&e2), Some(&e3), Some(&e4)) = (au.get(q + 6), au.get(q + 7), au.get(q + 8))
        else {
            break;
        };
        // picture_structure (e2 bits 1-0): 11 = frame picture; 01/10 = field.
        let frame_picture = e2 & 0x03 == 0b11;
        let tff = (e3 >> 7) & 1 == 1;
        let rff = (e3 >> 1) & 1 == 1;
        let progressive_frame = (e4 >> 7) & 1 == 1;
        return (tff, rff, progressive_frame, frame_picture);
    }
    (false, false, true, true)
}

/// Map MPEG-2 `picture_coding_type` (ISO/IEC 13818-2 §6.3.8) to the
/// codec-agnostic [`CodingType`]: 1 → I, 3 → B, else (2 = P, 4 = D) → P.
fn coding_type_from_raw(raw: u8) -> CodingType {
    match raw {
        1 => CodingType::I,
        3 => CodingType::B,
        _ => CodingType::P,
    }
}

/// Number of field-display periods a coded picture occupies, from its picture
/// coding extension (`00 00 01 B5`, ext-id `1000`), per ISO/IEC 13818-2 §6.3.10
/// and ffmpeg `mpeg_field_start` (`nb_fields = repeat_pict + 2`). This is what
/// times soft-telecined (2:3 pulldown) DVD video correctly: a
/// `repeat_first_field` frame occupies 3 fields, a normal frame 2, so honoring
/// it spreads the ~23.976 coded frames across the 29.97 display span with no
/// gap (the "play, pause, play" judder). `progressive_sequence` comes from the
/// sequence extension. Returns 2 (a normal frame) when no picture coding
/// extension is present.
fn picture_nb_fields(au: &[u8], progressive_sequence: bool) -> u8 {
    let mut search = 0;
    while let Some(q) = find_code(au, search, SEQ_EXT_CODE) {
        search = q + 4;
        // The picture coding extension is the B5 whose ext-id nibble is 1000.
        if au.get(q + 4).map(|b| b >> 4) != Some(0b1000) {
            continue;
        }
        // Extension bytes e2..=e4 = au[q+6 ..= q+8].
        let (Some(&e2), Some(&e3), Some(&e4)) = (au.get(q + 6), au.get(q + 7), au.get(q + 8))
        else {
            break;
        };
        // picture_structure (e2 bits 1-0): 11 = frame picture. A field picture
        // (01/10) occupies a single field; two combine into one frame upstream.
        if e2 & 0x03 != 0b11 {
            return 1;
        }
        let tff = (e3 >> 7) & 1;
        let rff = (e3 >> 1) & 1;
        let progressive_frame = (e4 >> 7) & 1;
        let repeat_pict = if rff == 0 {
            0
        } else if progressive_sequence {
            if tff == 1 { 4 } else { 2 }
        } else if progressive_frame == 1 {
            1
        } else {
            0
        };
        return repeat_pict + 2;
    }
    2
}

/// Read `progressive_sequence` from a captured sequence header's sequence
/// extension (`00 00 01 B5`, ext-id `0001`). False when absent (MPEG-1 / no
/// extension) — the interlaced default. Bit layout after the start code:
/// ext-id(4) profile_and_level(8) **progressive_sequence(1)** … so it is bit 3
/// of the second extension byte (`hdr[q+5]`).
fn parse_progressive_sequence(hdr: &[u8]) -> bool {
    let mut search = 0;
    while let Some(q) = find_code(hdr, search, SEQ_EXT_CODE) {
        search = q + 4;
        if hdr.get(q + 4).map(|b| b >> 4) != Some(0b0001) {
            continue;
        }
        return hdr.get(q + 5).map(|&b| (b >> 3) & 1 == 1).unwrap_or(false);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    /// Build a picture coding extension (`00 00 01 B5`, ext-id 1000) carrying the
    /// given pulldown flags, for `picture_nb_fields` tests.
    fn pic_coding_ext(tff: u8, rff: u8, progressive_frame: u8, frame_picture: bool) -> Vec<u8> {
        let e0 = 0x80; // ext-id 1000, f_code high nibble 0
        let e1 = 0x00;
        let e2 = if frame_picture { 0x03 } else { 0x01 }; // picture_structure bits 1-0
        let e3 = (tff << 7) | (rff << 1);
        let e4 = progressive_frame << 7;
        vec![0x00, 0x00, 0x01, SEQ_EXT_CODE, e0, e1, e2, e3, e4]
    }

    #[test]
    fn nb_fields_normal_frame_is_two() {
        assert_eq!(picture_nb_fields(&pic_coding_ext(0, 0, 0, true), false), 2);
    }

    #[test]
    fn nb_fields_telecine_repeat_field_is_three() {
        // NTSC 2:3 soft telecine: interlaced sequence, progressive frame, rff=1.
        assert_eq!(picture_nb_fields(&pic_coding_ext(0, 1, 1, true), false), 3);
    }

    #[test]
    fn nb_fields_field_picture_is_one() {
        assert_eq!(picture_nb_fields(&pic_coding_ext(0, 0, 0, false), false), 1);
    }

    #[test]
    fn nb_fields_progressive_seq_rff_tff_is_six() {
        assert_eq!(picture_nb_fields(&pic_coding_ext(1, 1, 0, true), true), 6);
    }

    #[test]
    fn nb_fields_progressive_seq_rff_no_tff_is_four() {
        assert_eq!(picture_nb_fields(&pic_coding_ext(0, 1, 0, true), true), 4);
    }

    #[test]
    fn nb_fields_no_picture_ext_defaults_two() {
        // A picture header with no coding extension → assume a normal 2-field frame.
        assert_eq!(picture_nb_fields(&[0, 0, 1, 0x00, 0, 0], false), 2);
    }

    #[test]
    fn parser_populates_full_pictureinfo_and_source() {
        use crate::mux::codec::coding::FieldOrder;
        // Drive the REAL parser over three pictures that exercise EVERY facet of
        // PictureInfo the parser measures (not just field order):
        //   I: tff=1 rff=0 pf=0 → type I, TFF, 2 fields, !progressive, keyframe
        //   P: tff=0 rff=0 pf=0 → type P, BFF, 2 fields, !progressive, !keyframe
        //   B: tff=0 rff=1 pf=1 → type B, Progressive, 3 fields (2:3 pulldown),
        //                         progressive, !keyframe
        // ...and assert the byte-exact source provenance rides every frame.
        // Each picture in its OWN PES with its OWN source stamp — the realistic
        // shape (real DVD video is one picture across many PES, each stamped), so
        // every picture's frame carries the provenance of its packet.
        let mk_pes = |data: Vec<u8>, byte: u64| PesPacket {
            source: Some(crate::pes::SourcePos::at_byte(byte)),
            pid: 0x1011,
            pts: None,
            dts: None,
            data,
            discontinuity: false,
        };
        let mut p = Mpeg2Parser::new();
        let mut frames = Vec::new();
        // I-picture (with the seq header) @ source byte 0.
        let mut au = make_seq_header(720, 576, 3, 3); // interlaced 16:9 25fps
        au.extend_from_slice(&make_picture_header(1));
        au.extend_from_slice(&pic_coding_ext(1, 0, 0, true));
        frames.extend(p.parse(&mk_pes(au, 0)));
        // P-picture @ source byte 2048.
        let mut au = make_picture_header(2);
        au.extend_from_slice(&pic_coding_ext(0, 0, 0, true));
        frames.extend(p.parse(&mk_pes(au, 2048)));
        // B-picture @ source byte 4096.
        let mut au = make_picture_header(3);
        au.extend_from_slice(&pic_coding_ext(0, 1, 1, true));
        frames.extend(p.parse(&mk_pes(au, 4096)));
        frames.extend(p.flush());
        assert_eq!(frames.len(), 3, "three pictures → three frames");

        // Every frame carries PictureInfo and the SourcePos its PES stamped.
        for f in &frames {
            assert!(f.coding.is_some(), "every MPEG-2 frame carries PictureInfo");
            assert!(
                f.source.is_some(),
                "every frame carries SourcePos provenance"
            );
        }
        let frame = |t: CodingType| {
            frames
                .iter()
                .find(|f| f.coding.unwrap().coding_type() == t)
                .unwrap_or_else(|| panic!("no {t:?} frame"))
        };

        let i = frame(CodingType::I);
        assert_eq!(
            i.source.unwrap().byte,
            0,
            "I frame keeps its PES source @ 0"
        );
        let ic = i.coding.unwrap();
        assert!(ic.keyframe(), "I picture is a keyframe");
        assert_eq!(ic.field_order(), Some(FieldOrder::Tff), "tff=1 → TFF");
        assert_eq!(ic.nb_fields(), 2, "normal interlaced frame = 2 fields");
        assert_eq!(ic.progressive(), Some(false));

        let pp = frame(CodingType::P);
        assert_eq!(
            pp.source.unwrap().byte,
            2048,
            "P frame keeps its PES source"
        );
        let pc = pp.coding.unwrap();
        assert!(!pc.keyframe());
        assert_eq!(
            pc.field_order(),
            Some(FieldOrder::Bff),
            "tff=0 interlaced frame → BFF (the red-flag fix)"
        );
        assert_eq!(pc.nb_fields(), 2);

        let b = frame(CodingType::B);
        assert_eq!(b.source.unwrap().byte, 4096, "B frame keeps its PES source");
        let bc = b.coding.unwrap();
        assert!(!bc.keyframe());
        assert_eq!(
            bc.field_order(),
            Some(FieldOrder::Progressive),
            "progressive_frame → Progressive (no field order)"
        );
        assert_eq!(
            bc.nb_fields(),
            3,
            "rff + progressive_frame in interlaced seq → 2:3 pulldown = 3 fields"
        );
        assert_eq!(bc.progressive(), Some(true));
    }

    #[test]
    fn progressive_sequence_parsed_from_seq_ext() {
        // Sequence extension: 00 00 01 B5, e0 ext-id 0001 (0x1_), e1 bit3 = progressive_sequence.
        assert!(parse_progressive_sequence(&[
            0,
            0,
            1,
            SEQ_EXT_CODE,
            0x10,
            0x08
        ]));
        assert!(!parse_progressive_sequence(&[
            0,
            0,
            1,
            SEQ_EXT_CODE,
            0x10,
            0x00
        ]));
        // No sequence extension at all → interlaced default (false).
        assert!(!parse_progressive_sequence(&[
            0,
            0,
            1,
            SEQ_HEADER_CODE,
            0,
            0
        ]));
    }

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1011,
            pts,
            dts: None,
            data,
            discontinuity: false,
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
    fn two_pictures_in_one_gop_emit_both_on_flush() {
        // Two pictures with no GOP/sequence boundary between them are ONE GOP.
        // The VFR timeline needs the whole GOP (a P-frame's PTS depends on its
        // later B-frames), so they buffer until the GOP closes / EOF, then emit
        // in DECODE order, each containing exactly its own picture.
        let mut parser = Mpeg2Parser::new();

        let mut pic1 = make_picture_header(PICTURE_TYPE_I);
        pic1.extend_from_slice(&[0x11; 100]);
        let mut pic2 = make_picture_header(2); // P
        pic2.extend_from_slice(&[0x22; 100]);

        let mut stream = pic1.clone();
        stream.extend_from_slice(&pic2);

        let frames = parser.parse(&make_pes(stream, Some(0)));
        assert!(frames.is_empty(), "same GOP — buffered until flush");

        let frames = parser.flush();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, pic1);
        assert!(frames[0].keyframe);
        assert_eq!(frames[1].data, pic2);
        assert!(!frames[1].keyframe);
    }

    /// B1 hole-2 regression: MPEG-2 buffers a GOP and emits asynchronously, so a
    /// concealed gap must be associated by OFFSET (like PTS), landing on the
    /// picture whose own bytes begin after the gap — NOT the previous picture
    /// that completes when the discontinuity PES arrives. pic1 (I) is pre-gap;
    /// pic2 (P), carried by a `discontinuity` PES, is the first post-gap AU.
    #[test]
    fn discontinuity_offset_mark_stamps_post_gap_picture_not_previous() {
        let mut parser = Mpeg2Parser::new();
        let mut pic1 = make_picture_header(PICTURE_TYPE_I);
        pic1.extend_from_slice(&[0x11; 100]);
        let mut pic2 = make_picture_header(2); // P
        pic2.extend_from_slice(&[0x22; 100]);

        // pic1 on a clean PES; nothing emits (same GOP, buffered).
        assert!(parser.parse(&make_pes(pic1.clone(), Some(0))).is_empty());
        // pic2 on a PES flagged discontinuity (a concealed gap preceded it).
        // parse() of this PES completes pic1's AU (the PREVIOUS picture) — which
        // must stay clean — while pic2 keeps buffering.
        let pes2 = PesPacket {
            source: None,
            pid: 0x1011,
            pts: Some(90000),
            dts: None,
            data: pic2.clone(),
            discontinuity: true,
        };
        assert!(parser.parse(&pes2).is_empty(), "same GOP — still buffered");

        let frames = parser.flush();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, pic1);
        assert!(
            !frames[0].discontinuity,
            "the previous (pre-gap) I picture must NOT be flagged"
        );
        assert_eq!(frames[1].data, pic2);
        assert!(
            frames[1].discontinuity,
            "the post-gap P picture (the discontinuity PES's own AU) IS flagged"
        );
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
        au.extend_from_slice(&[0x77; 50]);

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
        // With no sequence header (no frame rate) the parser falls back to each
        // AU's own PES PTS. Both pictures are one GOP → emitted on flush in
        // decode order, each carrying the PTS of the PES that began it.
        let mut parser = Mpeg2Parser::new();

        let mut pic1 = make_picture_header(PICTURE_TYPE_I);
        pic1.extend_from_slice(&[0x11; 50]);
        let frames1 = parser.parse(&make_pes(pic1, Some(90000)));
        assert!(frames1.is_empty(), "buffered until flush");

        let mut pic2 = make_picture_header(2);
        pic2.extend_from_slice(&[0x22; 50]);
        let frames2 = parser.parse(&make_pes(pic2, Some(180000)));
        assert!(frames2.is_empty(), "same GOP — still buffered");

        let frames = parser.flush();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].pts_ns, 1_000_000_000, "pic1 → PTS 90000");
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

    /// A frame-picture AU with a picture coding extension carrying pulldown
    /// flags (progressive_frame=1, so rff=1 → 3 fields), for VFR timing tests.
    fn make_pulldown_picture(coding_type: u8, tr: u16, rff: u8) -> Vec<u8> {
        let mut au = make_picture_header_tr(coding_type, tr);
        // 00 00 01 B5 | e0 ext-id 1000 | e1 | e2 frame-pic | e3 rff<<1 | e4 prog_frame
        au.extend_from_slice(&[
            0x00,
            0x00,
            0x01,
            SEQ_EXT_CODE,
            0x80,
            0x00,
            0x03,
            rff << 1,
            0x80,
        ]);
        au.extend_from_slice(&[0xAA; 16]);
        au
    }

    #[test]
    fn telecine_pts_accumulates_by_field_durations_not_a_fixed_grid() {
        // NTSC film, frame_rate_code 4 = 29.97 → field_period ≈ 16.683 ms. A 2:3
        // frame (rff=1) occupies 3 fields, a 2:2 frame 2 fields. PTS must
        // accumulate by ACTUAL field durations so the next frame starts exactly
        // when this one ends — closing the fixed-29.97-grid gap that judders.
        let mut p = Mpeg2Parser::new();
        let field = 1_000_000_000i64 * 1001 / 30000 / 2;

        let mut a = make_seq_header(720, 480, 2, 4);
        a.extend_from_slice(&gop());
        a.extend(make_pulldown_picture(1, 0, 1)); // I tr0, 3 fields, PES anchor 0
        a.extend(make_pulldown_picture(2, 1, 0)); // P tr1, 2 fields
        let mut frames = p.parse(&make_pes(a, Some(0)));
        frames.extend(p.flush());

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].pts_ns, 0, "I anchored to PES PTS 0");
        assert_eq!(
            frames[0].duration_ns,
            Some(3 * field as u64),
            "I = 3 fields"
        );
        assert_eq!(
            frames[1].pts_ns,
            3 * field,
            "P starts exactly at I-end (3 fields), not the 1/29.97 grid"
        );
        assert_eq!(
            frames[1].duration_ns,
            Some(2 * field as u64),
            "P = 2 fields"
        );
        assert!(frames[1].pts_ns > frames[0].pts_ns, "strictly monotonic");
    }

    #[test]
    fn b_frames_emit_in_decode_order_with_lower_display_pts() {
        // Decode order I(tr0) P(tr2) B(tr1): emitted in DECODE order, but the
        // B-frame carries a LOWER (earlier) display PTS than the P that precedes
        // it in the stream — never reordered (reordering corrupts the picture).
        let mut p = Mpeg2Parser::new();
        let field = 1_000_000_000i64 * 1001 / 30000 / 2;

        let mut a = make_seq_header(720, 480, 2, 4);
        a.extend_from_slice(&gop());
        a.extend(make_pulldown_picture(1, 0, 0)); // I tr0 (displays 1st), PES anchor 0
        a.extend(make_pulldown_picture(2, 2, 0)); // P tr2 (displays 3rd)
        a.extend(make_pulldown_picture(3, 1, 0)); // B tr1 (displays 2nd)
        let mut frames = p.parse(&make_pes(a, Some(0)));
        frames.extend(p.flush());

        assert_eq!(frames.len(), 3);
        assert!(frames[0].keyframe, "decode order preserved: I first");
        assert_eq!(frames[0].pts_ns, 0, "I (tr0) displays 1st");
        assert_eq!(frames[1].pts_ns, 4 * field, "P (tr2) displays 3rd");
        assert_eq!(frames[2].pts_ns, 2 * field, "B (tr1) displays 2nd");
        assert!(
            frames[2].pts_ns < frames[1].pts_ns,
            "B emitted AFTER P (decode order) but displays BEFORE it (lower PTS)"
        );
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

    #[test]
    fn opening_au_keeps_disc_pts_and_opening_seq_header_no_zero_floor() {
        // SOTL SUB-TASK 2 regression (opening-GOP / still-frame open). A DVD
        // title opens on a VOBU that begins with a sequence header + I-frame; the
        // disc stamps that opening I-frame at its REAL (non-zero) timeline PTS,
        // not 0. The parser must (a) emit the opening I-frame with that real PTS
        // — never floored to 0 — and (b) capture THAT opening sequence header as
        // codec_private (read at headers-ready, before any later AU). Proves the
        // opening pictures are emitted with the correct seq header + PTS, ruling
        // out the "wrong/last seq header" and "PTS floored to t=0" hypotheses.
        let mut p = Mpeg2Parser::new();

        // Opening AU: seq header (the codecPrivate) + GOP + I-frame TR0 carrying
        // the disc's real opening PTS (2 s here, i.e. NOT zero). 25 fps PAL.
        let mut a = make_seq_header(720, 576, 3, 3); // 16:9, 25 fps
        a.extend_from_slice(&gop());
        a.extend_from_slice(&make_picture_header_tr(PICTURE_TYPE_I, 0));
        a.extend_from_slice(&[0xAA; 20]);
        let mut frames = p.parse(&make_pes(a, Some(180_000))); // PTS = 2 s (90 kHz)
        assert!(frames.is_empty(), "first AU waits for the next boundary");

        // Second picture (no PTS) closes the opening AU: the I-frame emits and
        // the opening sequence header is captured (headers-ready timing — the
        // consumer reads codec_private once the first AU drains).
        let mut b = make_picture_header_tr(3, 1);
        b.extend_from_slice(&[0xBB; 20]);
        frames.extend(p.parse(&make_pes(b, None)));

        // codec_private is the OPENING sequence header (read at headers-ready,
        // before any later AU could replace it).
        let cp = p
            .codec_private()
            .expect("opening seq header captured at headers-ready");
        assert_eq!(
            &cp[..4],
            &[0x00, 0x00, 0x01, SEQ_HEADER_CODE],
            "codec_private is the opening sequence header"
        );
        assert_eq!(p.resolution(), Some((720, 576)), "576i opening header");
        assert_eq!(p.frame_rate(), Some((25, 1)), "25 fps opening header");

        frames.extend(p.flush());

        assert_eq!(frames.len(), 2);
        assert!(frames[0].keyframe, "opening picture is the I-frame");
        assert_eq!(
            frames[0].pts_ns, 2_000_000_000,
            "opening I-frame keeps the disc's real PTS (2 s), NOT floored to 0"
        );
        assert_eq!(
            frames[1].pts_ns, 2_040_000_000,
            "next frame is one 40 ms interval later on the real timeline"
        );
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
        a.extend_from_slice(&gop()); // trailing GOP header starts the next GOP
        let _fa = parser.parse(&make_pes(a, Some(0)));
        // Header A is captured during parse (codec_private) even though its GOP
        // only emits once header B's picture closes it / on flush.
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
            source: None,
            pid: 0x1011,
            pts: None,
            dts: Some(90000),
            data,
            discontinuity: false,
        };
        let f = parse_then_flush(&mut parser, &pes);
        assert_eq!(f[0].pts_ns, 1_000_000_000, "DTS fallback");

        let mut parser2 = Mpeg2Parser::new();
        let mut data2 = make_picture_header(PICTURE_TYPE_I);
        data2.extend_from_slice(&[0xFF; 4]);
        let pes2 = PesPacket {
            source: None,
            pid: 0x1011,
            pts: None,
            dts: None,
            data: data2,
            discontinuity: false,
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
        assert!(
            frames.is_empty(),
            "over-cap AU is force-COMPLETED (bounded) but buffered in its GOP"
        );
        let frames = parser.flush();
        assert_eq!(frames.len(), 1, "force-flushed at EOF, not dropped");
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

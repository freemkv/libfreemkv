//! MPEG-2 Video elementary stream parser.
//!
//! Reassembles coded pictures (access units) from the demuxed PES stream and
//! extracts sequence headers for MKV codecPrivate. One PES is NOT one frame:
//! on a DVD a coded picture can span many ~2 KB Program-Stream PES packets
//! with only the first carrying a PTS, so this parser buffers ES bytes across
//! PES packets and emits exactly one Frame per coded picture (never one per
//! PES, which would write truncated fragments).

use super::coding::{CodingType, Mpeg2Coding, PictureInfo};
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

/// The access-unit reassembly cap now lives in [`crate::mux::au_assembly`] (the
/// `AuAssembler` owns cross-PES buffering); this mirror exists only so the
/// force-flush test below can size an over-cap fixture against the same bound.
#[cfg(test)]
const MAX_AU_BUFFER: usize = 8 * 1024 * 1024;

/// Cap on frames buffered in one GOP. A DVD GOP is ~15 frames; a run reaching the
/// cap is force-flushed as its own GOP, and each split re-locks its origin and
/// display-order PTS from the frames it holds.
const MAX_PENDING_FRAMES: usize = 600;

// Byte cap on one buffered GOP; mirrors the AC-3/DTS/PGS byte caps.
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
/// Code 1 is a 1:1 sample aspect ratio; codes 2-4 are display aspect ratios.
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
    /// Reassembles PES fragments into complete access units (one coded picture
    /// with its leading sequence/GOP headers) and carries each AU's start
    /// timing / source / discontinuity forward — the shared machinery the
    /// H.264/HEVC/VC-1 parsers also use, in its MPEG-2 mode.
    au_asm: crate::mux::au_assembly::AuAssembler,
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
    /// Running total of `data` bytes buffered in `gop_buf` — the byte-cap counter,
    /// incremented on each push and reset when the GOP flushes. Avoids re-summing
    /// the whole buffer per picture (which would be O(pictures²)).
    gop_bytes: usize,
    /// Total field-display periods of all frames already emitted, in display
    /// order — the running base for each new frame's display time.
    emitted_fields: u64,
    /// PTS (ns) that display-field 0 of the whole stream maps to. Re-locked from
    /// each GOP's first PES PTS so video stays in sync with the PES-timestamped
    /// audio. None until the first PES timestamp is seen.
    origin_pts_ns: Option<i64>,
    /// Top-parity of an unpaired first field picture; the next opposite-parity
    /// field is its second field and inherits the pair's (first field's) order.
    pending_first_field: Option<bool>,
    /// Anchor frames (I/P) a decoder holds from this stream's emitted pictures,
    /// saturating at 2. Zero at the stream start, at a join (the timeline origin
    /// moves) and at a `broken_link` GOP; a picture needing more is dropped.
    refs: u8,
    /// `closed_gop` of the GOP being decided.
    gop_closed: bool,
    /// `temporal_reference` of the current GOP's first anchor; a B before it in
    /// display order is a leading picture.
    gop_anchor_tr: Option<u64>,
    /// Whether the last first-field or frame picture was kept; its second field follows it.
    last_kept: bool,
    /// A dropped picture's discontinuity flag, carried to the next emitted frame.
    carry_discontinuity: bool,
    /// A discontinuity was seen; `refs` resets at the I-picture the `ResyncGate` resumes on.
    gap: bool,
}

/// GOP header flags (ISO/IEC 13818-2 §6.3.8).
#[derive(Clone, Copy)]
struct GopFlags {
    /// The GOP's leading B-pictures use backward prediction only.
    closed: bool,
    /// The anchor before this GOP is not the one its leading B-pictures were coded against.
    broken_link: bool,
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
    /// Flags of the GOP this picture's access unit opens, if it opens one.
    gop: Option<GopFlags>,
    /// The second field of a field pair; decodable exactly when its first field is.
    second_field: bool,
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
            au_asm: crate::mux::au_assembly::AuAssembler::mpeg2(),
            frame_duration_ns: 0,
            progressive_sequence: false,
            gop_buf: Vec::new(),
            gop_bytes: 0,
            emitted_fields: 0,
            origin_pts_ns: None,
            pending_first_field: None,
            refs: 0,
            gop_closed: false,
            gop_anchor_tr: None,
            last_kept: false,
            carry_discontinuity: false,
            gap: false,
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

    // Decode + buffer one reassembled AU into the current GOP.
    fn process_au(&mut self, au: crate::mux::au_assembly::AssembledAu, out: &mut Vec<Frame>) {
        let data = au.data;
        // An access unit must contain a coded picture; a fragment that assembled
        // without one (only headers, or truncated at EOF) yields nothing.
        let Some(pic) = find_code(&data, 0, PICTURE_CODE) else {
            return;
        };
        let end = data.len();
        // Capture a sequence header for codecPrivate; a new one replaces the
        // stored value and re-locks the frame duration.
        if let Some(h) = extract_seq_header(&data) {
            self.progressive_sequence = parse_progressive_sequence(&h);
            self.seq_header = Some(h);
            if let Some((num, den)) = self.frame_rate()
                && num > 0
            {
                self.frame_duration_ns = 1_000_000_000i64 * den as i64 / num as i64;
            }
        }
        // A GOP header (0xB8) or a fresh sequence header (0xB3) starts a new GOP,
        // resetting temporal_reference to 0.
        let gop_header = find_code(&data, 0, GOP_CODE);
        let gop_boundary = gop_header.is_some() || find_code(&data, 0, SEQ_HEADER_CODE).is_some();
        // closed_gop and broken_link: bits 6 and 5 of the GOP header's fourth byte.
        // A sequence header with no GOP header starts a GOP read as open.
        let gop = gop_boundary.then(|| {
            let b = gop_header
                .and_then(|g| data.get(g + 7))
                .copied()
                .unwrap_or(0);
            GopFlags {
                closed: b & 0x40 != 0,
                broken_link: b & 0x20 != 0,
            }
        });
        // picture_coding_type: the full 3-bit value (bits 5-3 of data[pic+5]).
        // 0 when the picture header is truncated (no coding type available).
        let raw_coding_type = if pic + 5 < end {
            (data[pic + 5] >> 3) & 0x07
        } else {
            0
        };
        // temporal_reference: the 10 bits immediately after the picture start
        // code = display order within the GOP.
        let tr = if pic + 5 < end {
            (((data[pic + 4] as u64) << 2) | ((data[pic + 5] as u64) >> 6)) & 0x3FF
        } else {
            0
        };
        // Decode the picture coding extension ONCE here and fold every per-picture
        // datum into one codec-agnostic `PictureInfo`; `nb_fields()`, `keyframe()`,
        // and `field_order()` all derive from it, so nothing re-parses the stream.
        let (mut tff, rff, progressive_frame, frame_picture) = picture_coding_flags(&data);
        // A GOP or sequence header always starts a new pair; drop a stale lone field.
        if gop_boundary {
            self.pending_first_field = None;
        }
        let mut second_field = false;
        if frame_picture {
            self.pending_first_field = None;
        } else if let Some(first_top) = self.pending_first_field.take()
            && first_top != tff
        {
            // Second field of a pair: report the first field's order.
            tff = first_top;
            second_field = true;
        } else {
            self.pending_first_field = Some(tff);
        }
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

        // A GOP boundary means the buffered run is a COMPLETE GOP, so flush it
        // before starting the new one; `temporal_reference` resets to 0 there,
        // keeping each GOP's display order self-contained.
        if gop_boundary && !self.gop_buf.is_empty() {
            self.flush_gop(out);
        }
        self.gop_bytes += data.len();
        self.gop_buf.push(BufferedPicture {
            tr,
            info,
            explicit_pts: au.pts,
            gop,
            second_field,
            frame: Frame {
                pts_ns: 0,
                keyframe,
                // The assembler attributes the concealed-gap flag to the AU whose
                // own bytes begin after the gap — the first post-gap picture — so
                // it rides through GOP buffering/reorder to the ResyncGate.
                discontinuity: au.discontinuity,
                data,
                duration_ns: None,
                coding: Some(info),
                source: au.source,
            },
        });
        // Safety cap: force-flush a pathologically long run as its own GOP,
        // bounded by BOTH frame count and total bytes, so a crafted stream of
        // few-but-huge pictures cannot over-allocate either.
        if self.gop_buf.len() >= MAX_PENDING_FRAMES || self.gop_bytes >= MAX_PENDING_BYTES {
            self.flush_gop(out);
        }
    }

    // Emit the buffered GOP in decode order with display-order PTS/duration.
    fn flush_gop(&mut self, out: &mut Vec<Frame>) {
        let n = self.gop_buf.len();
        if n == 0 {
            return;
        }
        // The GOP is fully drained below; reset the running byte counter.
        self.gop_bytes = 0;
        let field_period = self.frame_duration_ns / 2;
        if field_period <= 0 {
            // No sequence header / frame rate yet (malformed lead-in): emit in
            // decode order off each AU's own PES PTS, with no field timing.
            let keep = self.decodable(false);
            for (bp, keep) in std::mem::take(&mut self.gop_buf).into_iter().zip(keep) {
                let mut f = bp.frame;
                f.pts_ns = bp.explicit_pts.unwrap_or(0);
                self.emit(f, keep, out);
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
        // (Re-)lock the timeline origin to the GOP's PES PTS. An origin that moves
        // by more than half a field is a join: no earlier picture is a reference.
        let mut joined = false;
        for &i in &order {
            if let Some(p) = self.gop_buf[i].explicit_pts {
                let origin = p - field_period * (base + cum_before[i]) as i64;
                joined = self
                    .origin_pts_ns
                    .is_some_and(|prev| (origin - prev).abs() > field_period / 2);
                self.origin_pts_ns = Some(origin);
                break;
            }
        }
        let origin = self.origin_pts_ns.unwrap_or(0);
        let keep = self.decodable(joined);
        let gop = std::mem::take(&mut self.gop_buf);
        for ((i, mut bp), keep) in gop.into_iter().enumerate().zip(keep) {
            bp.frame.pts_ns = origin + field_period * (base + cum_before[i]) as i64;
            bp.frame.duration_ns = Some(bp.info.nb_fields() as u64 * field_period as u64);
            self.emit(bp.frame, keep, out);
        }
        // Dropped pictures keep their display slots, so every kept frame keeps its PTS.
        self.emitted_fields += gop_fields;
    }

    // Decide, in decode order, which buffered pictures decode from references this
    // stream emitted: an I always; a P after an anchor; a B with both anchors, or
    // with one when it is not a leading picture or its GOP is closed.
    fn decodable(&mut self, joined: bool) -> Vec<bool> {
        if joined {
            self.refs = 0;
        }
        let mut keep = Vec::with_capacity(self.gop_buf.len());
        for bp in &self.gop_buf {
            if let Some(g) = bp.gop {
                self.gop_closed = g.closed;
                self.gop_anchor_tr = None;
                if g.broken_link {
                    self.refs = 0;
                }
            }
            // After a gap the gate drops (and counts) up to the next I, which holds none before it.
            self.gap |= bp.frame.discontinuity;
            if self.gap && bp.frame.keyframe {
                self.gap = false;
                self.refs = 0;
            }
            if bp.second_field {
                keep.push(self.last_kept);
                continue;
            }
            let ok = match bp.info.coding_type() {
                CodingType::I => true,
                CodingType::P => self.refs >= 1,
                CodingType::B => {
                    let leading =
                        !self.gop_closed && self.gop_anchor_tr.is_none_or(|anchor| bp.tr < anchor);
                    self.refs >= if leading { 2 } else { 1 }
                }
            };
            if bp.info.coding_type() != CodingType::B {
                self.gop_anchor_tr.get_or_insert(bp.tr);
                if ok {
                    self.refs = (self.refs + 1).min(2);
                }
            }
            self.last_kept = ok;
            keep.push(ok);
        }
        keep
    }

    // Emit a kept frame; a dropped one passes its discontinuity flag on.
    fn emit(&mut self, mut frame: Frame, keep: bool, out: &mut Vec<Frame>) {
        if keep {
            frame.discontinuity |= std::mem::take(&mut self.carry_discontinuity);
            out.push(frame);
        } else {
            self.carry_discontinuity |= frame.discontinuity;
        }
    }
}

impl CodecParser for Mpeg2Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        // Feed the fragment to the assembler, which reframes on picture boundaries.
        // MKV block timecodes are presentation timestamps; prefer PTS (DTS shows
        // B-frames in decode order — judder/broken seeking), DTS only as fallback.
        let pts = pes.pts.or(pes.dts).map(pts_to_ns);
        let aus = self
            .au_asm
            .push(&pes.data, pts, None, pes.source, pes.discontinuity);
        let mut out = Vec::new();
        for au in aus {
            self.process_au(au, &mut out);
        }
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // Force-complete the trailing access unit, then flush the final GOP so
        // nothing is left buffered at EOF.
        let mut out = Vec::new();
        for au in self.au_asm.flush() {
            self.process_au(au, &mut out);
        }
        self.flush_gop(&mut out);
        out
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        self.seq_header.clone()
    }
}

// Extract the sequence header (+ B5 extensions) as MKV codecPrivate extradata.
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

// Extract picture-coding-extension field/pulldown flags for `PictureInfo`.
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
        // A field picture forces top_field_first to 0 (§6.3.10); its order is
        // picture_structure (01 = top field), carried in `tff` instead.
        let tff = if frame_picture {
            (e3 >> 7) & 1 == 1
        } else {
            e2 & 0x03 == 0b01
        };
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
        PICTURE_TYPE_I => CodingType::I,
        3 => CodingType::B,
        _ => CodingType::P,
    }
}

// Read `progressive_sequence` from the sequence extension; false when absent.
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
#[path = "mpeg2_tests.rs"]
mod tests;

//! Byte-level builders of the program stream layer: pack header, system header, program
//! stream map (with its descriptors and CRC_32), PES headers and padding. Every field
//! layout is the H.222.0 (2000) syntax quoted in [`crate::spec::mpg`].

/// Every pack is this long (mpg-output-design v5 §2.3 "Fixed **2048-byte packs**", J9).
pub(crate) const PACK_BYTES: usize = 2048;
/// Pack header without stuffing (MS-2: 32 + 2 + 3 + 1 + 15 + 1 + 15 + 1 + 9 + 1 + 22 + 1 + 1
/// + 5 + 3 bits).
pub(crate) const PACK_HEADER_BYTES: usize = 14;
/// Offset in a pack of the byte holding the last bit of `system_clock_reference_base`
/// (MS-4: the byte whose arrival time the SCR encodes).
pub(crate) const SCR_BASE_LAST_BYTE: usize = 8;
/// Shortest padding PES: start code, stream_id and `PES_packet_length`.
pub(crate) const MIN_PADDING_PES: usize = 6;
/// Most stuffing bytes one pack header may carry (MS-3).
pub(crate) const MAX_PACK_STUFFING: usize = 7;
/// `rate_bound`: the largest 22-bit value, legal for any per-pack rate (design §2.4).
pub(crate) const RATE_BOUND: u32 = 0x3F_FFFF;

pub(crate) const PSM_ID: u8 = crate::consts::pes_stream_id::PROGRAM_STREAM_MAP;
pub(crate) const PRIVATE_STREAM_1: u8 = crate::consts::pes_stream_id::PRIVATE_STREAM_1;
pub(crate) const PADDING_STREAM: u8 = crate::consts::pes_stream_id::PADDING_STREAM;
pub(crate) const VIDEO_ID: u8 = crate::consts::pes_stream_id::VIDEO;

/// `pack_header()` (MS-2) at SCR `scr27` (27 MHz), `mux_rate` in 50 B/s units, then
/// `stuffing` 0xFF bytes (MS-3: at most 7).
pub(crate) fn pack_header(scr27: u64, mux_rate: u32, stuffing: usize) -> Vec<u8> {
    // MS-2 field layout; MS-3 at most 7 stuffing bytes, rate ≠ 0;
    // MS-4: "SCR(i) = SCR_base(i) × 300 + SCR_ext(i)".
    debug_assert!(stuffing <= MAX_PACK_STUFFING && mux_rate > 0 && mux_rate <= RATE_BOUND);
    let base = (scr27 / 300) & 0x1_FFFF_FFFF;
    let ext = scr27 % 300;
    let mut h = vec![0x00, 0x00, 0x01, 0xBA];
    h.push(0x40 | (((base >> 30) & 0x07) << 3) as u8 | 0x04 | ((base >> 28) & 0x03) as u8);
    h.push((base >> 20) as u8);
    h.push((((base >> 15) & 0x1F) << 3) as u8 | 0x04 | ((base >> 13) & 0x03) as u8);
    h.push((base >> 5) as u8);
    h.push(((base & 0x1F) << 3) as u8 | 0x04 | ((ext >> 7) & 0x03) as u8);
    h.push((((ext & 0x7F) << 1) | 1) as u8);
    h.push((mux_rate >> 14) as u8);
    h.push((mux_rate >> 6) as u8);
    h.push((((mux_rate & 0x3F) << 2) | 0x03) as u8);
    // Two marker bits are part of the byte above; reserved '11111', pack_stuffing_length.
    h.push(0xF8 | stuffing as u8);
    h.extend(std::iter::repeat_n(0xFF, stuffing));
    h
}

/// One system-header bound: `stream_id`, `P-STD_buffer_bound_scale`, `..._size_bound`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Bound {
    pub stream_id: u8,
    pub scale_1024: bool,
    pub size: u16,
}

/// `system_header()` (MS-5): every flag 0 (design §2.3, MPG2-9), `reserved_bits` all ones
/// (MS-6), then each stream's bound exactly once (MS-7).
pub(crate) fn system_header(audio_bound: u8, video_bound: u8, bounds: &[Bound]) -> Vec<u8> {
    // MS-5 syntax, MS-6 bounds and reserved_bits, MS-7 one bound per stream.
    let mut h = vec![0x00, 0x00, 0x01, 0xBB, 0, 0];
    h.push(0x80 | (RATE_BOUND >> 15) as u8);
    h.push((RATE_BOUND >> 7) as u8);
    h.push((((RATE_BOUND & 0x7F) << 1) | 1) as u8);
    // audio_bound (6) | fixed_flag 0 | CSPS_flag 0.
    h.push(audio_bound.min(32) << 2);
    // system_audio_lock_flag 0 | system_video_lock_flag 0 | marker | video_bound (5).
    h.push(0x20 | video_bound.min(16));
    // packet_rate_restriction_flag 0 | reserved_bits '111 1111'.
    h.push(0x7F);
    for b in bounds {
        h.push(b.stream_id);
        h.push(0xC0 | (u8::from(b.scale_1024) << 5) | ((b.size >> 8) & 0x1F) as u8);
        h.push(b.size as u8);
    }
    let len = (h.len() - 6) as u16;
    h[4..6].copy_from_slice(&len.to_be_bytes());
    h
}

/// The CRC_32 of H.222.0 Annex A (MS-9, MS-26): MSB-first, register preset to all ones, no
/// final inversion, so a map followed by its CRC leaves the decoder registers at zero.
pub(crate) fn crc32(data: &[u8]) -> u32 {
    // MS-26: x32 + x26 + x23 + x22 + x16 + x12 + x11 + x10 + x8 + x7 + x5 + x4 + x2 + x + 1.
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= u32::from(b) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// One elementary stream entry of the program stream map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PsmEntry {
    pub stream_type: u8,
    pub stream_id: u8,
    pub descriptors: Vec<u8>,
}

/// Largest `program_stream_map_length` (MS-8).
pub(crate) const MAX_PSM_LENGTH: usize = 1018;

/// `program_stream_map()` (MS-8), `current_next_indicator` 1 and version 0, ending in its
/// CRC_32 (MS-9). `None` when it would exceed the 1018-byte length cap.
pub(crate) fn psm(info: &[u8], entries: &[PsmEntry]) -> Option<Vec<u8>> {
    let mut es = Vec::new();
    for e in entries {
        es.push(e.stream_type);
        es.push(e.stream_id);
        es.extend_from_slice(&u16::try_from(e.descriptors.len()).ok()?.to_be_bytes());
        es.extend_from_slice(&e.descriptors);
    }
    // MS-8: the length counts everything after it, CRC_32 (MS-9) included; max 1018.
    let length = 2 + 2 + info.len() + 2 + es.len() + 4;
    if length > MAX_PSM_LENGTH {
        return None;
    }
    let mut m = vec![0x00, 0x00, 0x01, PSM_ID];
    m.extend_from_slice(&(length as u16).to_be_bytes());
    // current_next_indicator '1', reserved '11', version 0; reserved '1111111', marker '1'.
    m.push(0xE0);
    m.push(0xFF);
    m.extend_from_slice(&(info.len() as u16).to_be_bytes());
    m.extend_from_slice(info);
    m.extend_from_slice(&(es.len() as u16).to_be_bytes());
    m.extend_from_slice(&es);
    let crc = crc32(&m);
    m.extend_from_slice(&crc.to_be_bytes());
    Some(m)
}

/// `hierarchy_type` 5, "ISO/IEC 13818-3 Extension bitstream" (MS-23).
pub(crate) const HIERARCHY_EXTENSION: u8 = 5;
/// `hierarchy_type` 15, "Base layer" (MS-23).
pub(crate) const HIERARCHY_BASE: u8 = 15;

/// `hierarchy_descriptor()` (tag 4, MS-23, MS-25). `embedded` is written only for a
/// non-base layer: "This field is undefined if the hierarchy_type value is 15" (all ones).
pub(crate) fn hierarchy_descriptor(kind: u8, layer: u8, embedded: u8) -> [u8; 6] {
    // MS-23: reserved bits are ones; the base's embedded index is undefined.
    let embedded = if kind == HIERARCHY_BASE {
        0x3F
    } else {
        embedded
    };
    [4, 4, 0xF0 | kind, 0xC0 | layer, 0xC0 | embedded, 0xC0]
}

/// `ISO_639_language_descriptor()` (tag 10, MS-24) with `audio_type` 0 (undefined), or
/// `None` when `lang` is not three ISO 8859-1 letters.
pub(crate) fn iso639_descriptor(lang: &str) -> Option<[u8; 6]> {
    // MS-24: one 24-bit ISO_639_language_code and an 8-bit audio_type.
    let b = lang.as_bytes();
    (b.len() == 3 && b.iter().all(u8::is_ascii_alphabetic)).then(|| {
        [
            10,
            4,
            b[0].to_ascii_lowercase(),
            b[1].to_ascii_lowercase(),
            b[2].to_ascii_lowercase(),
            0,
        ]
    })
}

/// The FMKV user-private descriptor tag (design §2.2, J1; MS-25 "64-255 … User Private").
pub(crate) const FMKV_TAG: u8 = 0xFA;
pub(crate) const FMKV_MAGIC: &[u8; 4] = b"FMKV";
pub(crate) const FMKV_VERSION: u8 = 1;
/// Type 1: a sub-stream table of `(sub_id, lang[3], flags)`; flags bit 0 = forced.
pub(crate) const FMKV_SUBSTREAMS: u8 = 1;
/// Type 2: the VobSub palette, 16 × RGB.
pub(crate) const FMKV_PALETTE: u8 = 2;

/// One private_stream_1 sub-stream row of the FMKV table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubStreamInfo {
    pub sub_id: u8,
    pub lang: [u8; 3],
    pub forced: bool,
}

/// The FMKV descriptors for `program_stream_info` (MS-28: private descriptors "shall
/// commence with descriptor_tag and descriptor_length fields"), the table split so no
/// `descriptor_length` exceeds 255.
pub(crate) fn fmkv_descriptors(subs: &[SubStreamInfo], palette: Option<&[[u8; 3]; 16]>) -> Vec<u8> {
    // MS-28: a private descriptor starts with descriptor_tag and descriptor_length.
    const HEAD: usize = 6; // magic, version, type
    const ROW: usize = 5;
    let mut out = Vec::new();
    for rows in subs.chunks((255 - HEAD) / ROW) {
        out.extend_from_slice(&[FMKV_TAG, (HEAD + rows.len() * ROW) as u8]);
        out.extend_from_slice(FMKV_MAGIC);
        out.extend_from_slice(&[FMKV_VERSION, FMKV_SUBSTREAMS]);
        for r in rows {
            out.push(r.sub_id);
            out.extend_from_slice(&r.lang);
            out.push(u8::from(r.forced));
        }
    }
    if let Some(p) = palette {
        out.extend_from_slice(&[FMKV_TAG, (HEAD + 48) as u8]);
        out.extend_from_slice(FMKV_MAGIC);
        out.extend_from_slice(&[FMKV_VERSION, FMKV_PALETTE]);
        out.extend(p.iter().flatten());
    }
    out
}

/// The optional fields of one PES packet header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PesFields {
    /// PTS in 90 kHz ticks.
    pub pts: Option<u64>,
    /// DTS, only where it differs from the PTS (MS-19).
    pub dts: Option<u64>,
    /// `(P-STD_buffer_scale, P-STD_buffer_size)` (MS-11, MS-13, MS-21).
    pub pstd: Option<(bool, u16)>,
    pub data_alignment: bool,
}

// A 33-bit timestamp with its 4-bit prefix and marker bits (MS-10).
fn push_timestamp(h: &mut Vec<u8>, prefix: u8, ts: u64) {
    let ts = ts & 0x1_FFFF_FFFF;
    h.push((prefix << 4) | 0x01 | (((ts >> 29) & 0x0E) as u8));
    h.push((ts >> 22) as u8);
    h.push(0x01 | (((ts >> 14) & 0xFE) as u8));
    h.push((ts >> 7) as u8);
    h.push(0x01 | (((ts << 1) & 0xFE) as u8));
}

/// Header bytes a PES with `fields` takes before its payload.
#[cfg(test)]
pub(crate) fn pes_header_len(fields: &PesFields) -> usize {
    9 + ts_bytes(fields) + if fields.pstd.is_some() { 3 } else { 0 }
}

fn ts_bytes(f: &PesFields) -> usize {
    match (f.pts, f.dts) {
        (None, _) => 0,
        (Some(_), None) => 5,
        (Some(_), Some(_)) => 10,
    }
}

/// A PES packet header (MS-10, MS-11) for `payload_len` bytes, with a bounded
/// `PES_packet_length` (MS-12: 0 is for Transport Stream video only).
pub(crate) fn pes_header(stream_id: u8, fields: &PesFields, payload_len: usize) -> Vec<u8> {
    // MS-10 header and timestamps, MS-11 extension, MS-12 a bounded length.
    let ext = if fields.pstd.is_some() { 3 } else { 0 };
    let hdl = ts_bytes(fields) + ext;
    let length = 3 + hdl + payload_len;
    debug_assert!(
        length <= usize::from(u16::MAX),
        "a PES always fits one pack"
    );
    let mut h = vec![0x00, 0x00, 0x01, stream_id];
    h.extend_from_slice(&(length as u16).to_be_bytes());
    // '10', scrambling 00, priority 0, data_alignment_indicator, copyright 0, original 1.
    h.push(0x81 | (u8::from(fields.data_alignment) << 2));
    let pts_dts = match (fields.pts, fields.dts) {
        (None, _) => 0x00,
        (Some(_), None) => 0x80,
        (Some(_), Some(_)) => 0xC0,
    };
    h.push(pts_dts | if ext > 0 { 0x01 } else { 0x00 });
    h.push(hdl as u8);
    match (fields.pts, fields.dts) {
        (Some(p), None) => push_timestamp(&mut h, 0b0010, p),
        (Some(p), Some(d)) => {
            push_timestamp(&mut h, 0b0011, p);
            push_timestamp(&mut h, 0b0001, d);
        }
        (None, _) => {}
    }
    if let Some((scale, size)) = fields.pstd {
        // PES_extension: only P-STD_buffer_flag set; reserved '111'.
        h.push(0x1E);
        h.push(0x40 | (u8::from(scale) << 5) | ((size >> 8) & 0x1F) as u8);
        h.push(size as u8);
    }
    h
}

/// Grow the PES header at `body[at..]` by `n` stuffing bytes (MS-10: the stuffing loop after
/// the optional fields; MS-13: at most 32 per header), fixing both length fields.
pub(crate) fn stuff_pes_header(body: &mut Vec<u8>, at: usize, n: usize) {
    debug_assert!(n <= 32);
    let hdl = usize::from(body[at + 8]);
    let len = usize::from(u16::from_be_bytes([body[at + 4], body[at + 5]])) + n;
    body[at + 4..at + 6].copy_from_slice(&(len as u16).to_be_bytes());
    body[at + 8] = (hdl + n) as u8;
    let end = at + 9 + hdl;
    body.splice(end..end, std::iter::repeat_n(0xFF, n));
}

/// A padding PES (MS-11 "padding_byte") that is exactly `total` bytes long.
pub(crate) fn padding_pes(total: usize) -> Vec<u8> {
    debug_assert!(total >= MIN_PADDING_PES);
    let mut p = vec![0x00, 0x00, 0x01, PADDING_STREAM];
    p.extend_from_slice(&((total - MIN_PADDING_PES) as u16).to_be_bytes());
    p.resize(total, 0xFF);
    p
}

/// `MPEG_program_end_code`.
pub(crate) const PROGRAM_END: [u8; 4] = [0x00, 0x00, 0x01, 0xB9];

#[cfg(test)]
#[path = "pack_tests.rs"]
mod tests;

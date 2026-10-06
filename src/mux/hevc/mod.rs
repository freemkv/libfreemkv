//! H.264 / HEVC parameter-set helpers: decoder configuration records (`avcC` / `hvcC`) and
//! length-prefixed NAL units as an Annex B byte stream, for the elementary-stream sinks and
//! the TS muxer.

/// Annex B 4-byte start code.
pub(crate) const START_CODE: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

// Convert a HEVCDecoderConfigurationRecord (hvcC) into Annex B NAL units. Single source of
// truth for hvcC -> Annex B across all muxers (HEVC ES, BD-TS, standard MPEG-TS) — do not
// reimplement.
pub(crate) fn hvcc_to_annex_b(hvcc: &[u8]) -> Option<Vec<u8>> {
    let (out, truncated) = hvcc_parse(hvcc);
    if truncated {
        tracing::warn!(target: "mux", "hvcC truncated; parameter sets may be incomplete");
    }
    out
}

// Returns the Annex B bytes and whether the record was cut short.
fn hvcc_parse(hvcc: &[u8]) -> (Option<Vec<u8>>, bool) {
    if hvcc.len() < 23 {
        return (None, false);
    }
    let num_arrays = hvcc[22] as usize;
    let mut out = Vec::new();
    let mut offset = 23;
    // Set when an inner loop exits on truncation so the outer loop stops
    // too — otherwise it would re-interpret mid-NAL bytes as the next
    // array header and synthesize spurious parameter-set NALs.
    let mut truncated = false;
    for _ in 0..num_arrays {
        if truncated {
            break;
        }
        if offset + 3 > hvcc.len() {
            truncated = true;
            break;
        }
        offset += 1; // array_completeness + nal_type byte
        let num_nalus = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
        offset += 2;
        for _ in 0..num_nalus {
            if offset + 2 > hvcc.len() {
                truncated = true;
                break;
            }
            let nal_len = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
            offset += 2;
            if offset + nal_len > hvcc.len() {
                truncated = true;
                break;
            }
            // ISO/IEC 14496-15 disallows zero-length NAL entries; emitting
            // a bare start code with no RBSP yields an invalid Annex B NAL.
            if nal_len == 0 {
                continue;
            }
            out.extend_from_slice(&START_CODE);
            out.extend_from_slice(&hvcc[offset..offset + nal_len]);
            offset += nal_len;
        }
    }
    (if out.is_empty() { None } else { Some(out) }, truncated)
}

// Convert length-prefixed NALs ([u32-BE len][NAL] repeated) to Annex B; already-Annex-B input
// passes through unchanged. Truncation policy (shared across muxers): drop truncated trailing
// NAL.
#[cfg(test)]
pub(crate) fn length_prefixed_to_annex_b(data: &[u8]) -> Vec<u8> {
    // Probe for a leading Annex B start code before attempting to parse
    // length prefixes: `00 00 00 01` would otherwise parse as length 1.
    if starts_with_start_code(data) {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(data.len() + (data.len() / 32));
    append_length_prefixed_as_annex_b(&mut out, data);
    out
}

/// The NAL length-prefix width this crate's own parsers emit, and the width
/// ISO/IEC 14496-15 records declare as `lengthSizeMinusOne = 3`.
pub(crate) const DEFAULT_NAL_LENGTH_SIZE: usize = 4;

// Octets per NAL length prefix for `record`'s track, per avcC/hvcC's lengthSizeMinusOne field
// (ISO/IEC 14496-15). Falls back to DEFAULT_NAL_LENGTH_SIZE.
pub(crate) fn nal_length_size(codec: crate::disc::Codec, record: Option<&[u8]>) -> usize {
    use crate::disc::Codec;
    let field_offset = match codec {
        Codec::H264 => 4,
        Codec::Hevc => 21,
        _ => return DEFAULT_NAL_LENGTH_SIZE,
    };
    match record.and_then(|r| r.get(field_offset)) {
        Some(&b) => (b & 0x03) as usize + 1,
        None => DEFAULT_NAL_LENGTH_SIZE,
    }
}

// Annex B form of length-prefixed `data`, written into caller-owned `out` to avoid an
// allocation on hot per-frame paths. Non-length-prefixed input is appended unchanged (assumed
// already Annex B).
#[cfg(test)]
pub(crate) fn append_length_prefixed_as_annex_b(out: &mut Vec<u8>, data: &[u8]) {
    append_length_prefixed_as_annex_b_sized(out, data, DEFAULT_NAL_LENGTH_SIZE);
}

// Like append_length_prefixed_as_annex_b but for `length_size`-octet NAL prefixes (from
// nal_length_size); `length_size` outside 1..=4 clamps to DEFAULT_NAL_LENGTH_SIZE.
pub(crate) fn append_length_prefixed_as_annex_b_sized(
    out: &mut Vec<u8>,
    data: &[u8],
    length_size: usize,
) {
    let length_size = if (1..=4).contains(&length_size) {
        length_size
    } else {
        DEFAULT_NAL_LENGTH_SIZE
    };
    let mut offset = 0;
    // True once at least one well-formed length prefix is consumed (even a
    // zero-length one): distinguishes "all NALs empty" (emit nothing) from
    // "not length-prefixed at all" (pass through as already-Annex B).
    let mut parsed_any = false;
    while offset + length_size <= data.len() {
        // Big-endian over exactly `length_size` octets (ISO/IEC 14496-15: the
        // prefix is an unsigned integer of `lengthSizeMinusOne + 1` bytes).
        let len = data[offset..offset + length_size]
            .iter()
            .fold(0usize, |acc, &b| (acc << 8) | b as usize);
        offset += length_size;
        if offset + len > data.len() {
            // Mid-NAL truncation (e.g. bad disc sector): drop the trailing
            // NAL, emit only the valid Annex-B prefix so far — never leak
            // raw length-prefixed bytes into the Annex-B stream.
            break;
        }
        parsed_any = true;
        if len == 0 {
            // Zero-length prefix (e.g. damaged-sector padding) would emit an
            // invalid empty NAL; skip it (mirrors `hvcc_to_annex_b`'s
            // `nal_len == 0` guard, ISO/IEC 14496-15).
            continue;
        }
        out.extend_from_slice(&START_CODE);
        out.extend_from_slice(&data[offset..offset + len]);
        offset += len;
    }
    if !parsed_any && !data.is_empty() {
        // Nothing parsed and no leading start code: pass bytes through
        // rather than discard them (recover-100% goal; a decoder can
        // resync). Distinct from "parsed but every NAL was zero-length".
        out.extend_from_slice(data);
    }
}

/// Whether `data` begins with a 4-byte (`00 00 00 01`) or 3-byte
/// (`00 00 01`) Annex B start code.
#[cfg(test)]
fn starts_with_start_code(data: &[u8]) -> bool {
    data.starts_with(&START_CODE) || data.starts_with(&[0x00, 0x00, 0x01])
}

// Convert an AVCDecoderConfigurationRecord (avcC) into Annex B NAL units; H.264 counterpart to
// hvcc_to_annex_b and single source of truth for avcC -> Annex B across all muxers.
pub(crate) fn avcc_to_annex_b(avcc: &[u8]) -> Option<Vec<u8>> {
    let (out, truncated) = avcc_parse(avcc);
    if truncated {
        tracing::warn!(target: "mux", "avcC truncated; parameter sets incomplete");
    }
    out
}

// Returns the Annex B bytes and whether the record was cut short.
fn avcc_parse(avcc: &[u8]) -> (Option<Vec<u8>>, bool) {
    // avcC fixed header is 5 bytes; byte 5 carries the SPS count (low 5 bits),
    // then the SPS array begins at byte 6 (ISO/IEC 14496-15 §5.3.3.1.2).
    const AVCC_HEADER_LEN: usize = 5;
    const NUM_SPS_MASK: u8 = 0x1F; // numOfSequenceParameterSets: low 5 bits
    if avcc.len() < AVCC_HEADER_LEN + 1 {
        return (None, false);
    }
    let num_sps = (avcc[AVCC_HEADER_LEN] & NUM_SPS_MASK) as usize;
    let mut offset = AVCC_HEADER_LEN + 1;
    let mut out = Vec::new();

    // Extract `count` length-prefixed NALs into `out`. Returns `false` if a
    // length field or NAL body runs past the end, so the caller stops rather
    // than reading further length fields out of mid-NAL bytes.
    fn take(avcc: &[u8], count: usize, offset: &mut usize, out: &mut Vec<u8>) -> bool {
        for _ in 0..count {
            if *offset + 2 > avcc.len() {
                return false;
            }
            let nal_len = u16::from_be_bytes([avcc[*offset], avcc[*offset + 1]]) as usize;
            *offset += 2;
            if *offset + nal_len > avcc.len() {
                return false;
            }
            // ISO/IEC 14496-15 disallows zero-length NAL entries; emitting a
            // bare start code with no RBSP yields an invalid Annex B NAL.
            if nal_len == 0 {
                continue;
            }
            out.extend_from_slice(&START_CODE);
            out.extend_from_slice(&avcc[*offset..*offset + nal_len]);
            *offset += nal_len;
        }
        true
    }

    let mut truncated = !take(avcc, num_sps, &mut offset, &mut out);
    if !truncated {
        // numOfPictureParameterSets is mandatory, so a missing byte is truncation too.
        if offset < avcc.len() {
            let num_pps = avcc[offset] as usize;
            offset += 1;
            truncated = !take(avcc, num_pps, &mut offset, &mut out);
        } else {
            truncated = true;
        }
    }

    (if out.is_empty() { None } else { Some(out) }, truncated)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

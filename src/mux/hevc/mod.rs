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
mod tests {
    use super::*;

    #[test]
    fn avcc_extracts_sps_and_pps() {
        // header(5) numSPS=1 spsLen=2 SPS=[0x67,0x42] numPPS=1 ppsLen=1
        // PPS=[0x68].
        let avcc = [
            1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 1, 0x68,
        ];
        let out = avcc_to_annex_b(&avcc).expect("SPS+PPS");
        assert_eq!(out, vec![0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x68]);
    }

    #[test]
    fn avcc_too_short_is_none() {
        assert!(avcc_to_annex_b(&[]).is_none());
        assert!(avcc_to_annex_b(&[1, 0x42, 0, 0x1F, 0xFF]).is_none());
    }

    #[test]
    fn avcc_truncated_sps_stops_cleanly() {
        // numSPS=1, declares spsLen=5 but only 2 bytes follow → drop it, and
        // do NOT misread the trailing bytes as a PPS count.
        let avcc = [1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 5, 0xAA, 0xBB];
        assert!(avcc_to_annex_b(&avcc).is_none());
    }

    #[test]
    fn avcc_skips_zero_length_nal() {
        // numSPS=1 spsLen=0 (skipped) numPPS=1 ppsLen=1 PPS=[0x68].
        let avcc = [1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 0, 1, 0, 1, 0x68];
        let out = avcc_to_annex_b(&avcc).expect("just the PPS");
        assert_eq!(out, vec![0, 0, 0, 1, 0x68]);
    }

    #[test]
    fn length_prefixed_converts_to_annex_b() {
        // Two NALs: [3-byte payload AA BB CC] and [2-byte payload DD EE].
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&[0xDD, 0xEE]);

        let got = length_prefixed_to_annex_b(&buf);
        let want = [
            0x00, 0x00, 0x00, 0x01, 0xAA, 0xBB, 0xCC, // first NAL
            0x00, 0x00, 0x00, 0x01, 0xDD, 0xEE, // second NAL
        ];
        assert_eq!(&got[..], &want[..]);
    }

    #[test]
    fn already_annex_b_passes_through_when_no_lengths_match() {
        // A buffer < 4 bytes can't parse a length prefix at all →
        // pass-through path triggers.
        let raw = [0xAA, 0xBB, 0xCC];
        let got = length_prefixed_to_annex_b(&raw);
        assert_eq!(&got[..], &raw[..]);
    }

    #[test]
    fn mid_nal_truncation_drops_trailing_nal_keeps_prefix() {
        // Second NAL's length prefix claims 100 bytes with only 3 present.
        // Policy: emit the valid first NAL, drop the truncated trailing one
        // — never leak raw length-prefixed bytes into the Annex B stream.
        let mut raw = Vec::new();
        raw.extend_from_slice(&2u32.to_be_bytes());
        raw.extend_from_slice(&[0x11, 0x22]);
        raw.extend_from_slice(&100u32.to_be_bytes());
        raw.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let got = length_prefixed_to_annex_b(&raw);
        let want = [0x00, 0x00, 0x00, 0x01, 0x11, 0x22];
        assert_eq!(&got[..], &want[..]);
    }

    #[test]
    fn leading_annex_b_start_code_passes_through() {
        // Genuine Annex B beginning with 00 00 00 01 must NOT be reframed:
        // the start code would otherwise parse as a u32-BE length of 1.
        let raw = [
            0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0xDE, 0xAD, // NAL 1
            0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xBE, 0xEF, // NAL 2
        ];
        let got = length_prefixed_to_annex_b(&raw);
        assert_eq!(
            &got[..],
            &raw[..],
            "Annex B input must pass through verbatim"
        );
    }

    #[test]
    fn leading_three_byte_start_code_passes_through() {
        let raw = [0x00, 0x00, 0x01, 0x26, 0x01, 0xDE, 0xAD];
        let got = length_prefixed_to_annex_b(&raw);
        assert_eq!(&got[..], &raw[..]);
    }

    #[test]
    fn hvcc_skips_zero_length_nal_entries() {
        // hvcC with one array containing a zero-length NAL followed by a
        // valid one: the zero-length entry must be skipped, not emitted as
        // a bare start code.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1); // numArrays
        hvcc.push(33); // SPS
        hvcc.extend_from_slice(&2u16.to_be_bytes()); // numNalus = 2
        hvcc.extend_from_slice(&0u16.to_be_bytes()); // NAL 0: length 0
        hvcc.extend_from_slice(&3u16.to_be_bytes()); // NAL 1: length 3
        hvcc.extend_from_slice(&[0x42, 0x01, 0x01]);
        let annex_b = hvcc_to_annex_b(&hvcc).expect("one valid NAL");
        let want = [0x00, 0x00, 0x00, 0x01, 0x42, 0x01, 0x01];
        assert_eq!(&annex_b[..], &want[..]);
    }

    #[test]
    fn zero_length_nal_is_skipped_not_bare_start_code() {
        // A zero-length prefix between two real NALs must be skipped, not
        // turned into a bare `00 00 00 01` with no RBSP.
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        buf.extend_from_slice(&0u32.to_be_bytes()); // zero-length NAL
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&[0xDD, 0xEE]);

        let got = length_prefixed_to_annex_b(&buf);
        let want = [
            0x00, 0x00, 0x00, 0x01, 0xAA, 0xBB, 0xCC, // first NAL
            0x00, 0x00, 0x00, 0x01, 0xDD, 0xEE, // second NAL (zero-length skipped)
        ];
        assert_eq!(&got[..], &want[..]);
    }

    #[test]
    fn all_zero_length_nals_emit_nothing() {
        // A buffer of only zero-length prefixes parses as length-prefixed
        // but yields no NALs — output must be empty, not a pass-through of
        // the raw zero bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let got = length_prefixed_to_annex_b(&buf);
        assert!(got.is_empty(), "expected empty output, got {got:?}");
    }

    #[test]
    fn hvcc_extracts_vps_sps_pps() {
        // Build a minimal-but-valid hvcC: 22-byte header, then 3 arrays
        // (VPS / SPS / PPS), each with 1 NAL of a 4-byte payload that
        // we can spot in the output.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(3); // numOfArrays
        for (nal_type, payload) in [
            (32u8, [0x40, 0x01, 0x0C, 0x01]),
            (33, [0x42, 0x01, 0x01, 0x01]),
            (34, [0x44, 0x01, 0xC1, 0x72]),
        ] {
            hvcc.push(nal_type & 0x3F);
            hvcc.extend_from_slice(&1u16.to_be_bytes()); // numNalus
            hvcc.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            hvcc.extend_from_slice(&payload);
        }

        let annex_b = hvcc_to_annex_b(&hvcc).expect("at least one NAL");
        // Three NALs × (4-byte start + 4-byte payload) = 24 bytes.
        assert_eq!(annex_b.len(), 24);
        assert_eq!(&annex_b[..4], &START_CODE);
        assert_eq!(&annex_b[8..12], &START_CODE);
        assert_eq!(&annex_b[16..20], &START_CODE);
        assert_eq!(annex_b[4], 0x40); // VPS first byte
        assert_eq!(annex_b[12], 0x42); // SPS first byte
        assert_eq!(annex_b[20], 0x44); // PPS first byte
    }

    // --- hvcc_to_annex_b: truncation handling (ISO/IEC 14496-15 §8.3.3.1.2) ---

    #[test]
    fn hvcc_too_short_for_header_returns_none() {
        // < 23 bytes (22 fixed + numArrays) can't be a valid hvcC → None.
        assert!(hvcc_to_annex_b(&[0u8; 22]).is_none());
        assert!(hvcc_to_annex_b(&[]).is_none());
    }

    #[test]
    fn hvcc_zero_arrays_returns_none() {
        // numArrays = 0 → no NALs extracted → None (out.is_empty()).
        let mut hvcc = vec![0u8; 22];
        hvcc.push(0); // numArrays = 0
        assert!(hvcc_to_annex_b(&hvcc).is_none());
    }

    #[test]
    fn hvcc_array_with_multiple_nalus() {
        // One array, numNalus = 2: both NALs must be emitted, each with a start
        // code. (The inner numNalus loop, not just one NAL per array.)
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1); // numArrays
        hvcc.push(33); // SPS
        hvcc.extend_from_slice(&2u16.to_be_bytes()); // numNalus = 2
        hvcc.extend_from_slice(&2u16.to_be_bytes()); // NAL0 len 2
        hvcc.extend_from_slice(&[0x42, 0x01]);
        hvcc.extend_from_slice(&3u16.to_be_bytes()); // NAL1 len 3
        hvcc.extend_from_slice(&[0x44, 0x02, 0x03]);
        let out = hvcc_to_annex_b(&hvcc).expect("two NALs");
        let want = [
            0x00, 0x00, 0x00, 0x01, 0x42, 0x01, // NAL0
            0x00, 0x00, 0x00, 0x01, 0x44, 0x02, 0x03, // NAL1
        ];
        assert_eq!(&out[..], &want[..]);
    }

    #[test]
    fn hvcc_truncated_nal_length_stops_cleanly() {
        // A NAL length field claiming more bytes than remain must stop parsing
        // (truncated flag), emitting only the complete NALs — never a partial
        // NAL nor garbage from re-interpreting mid-NAL bytes as an array header.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(2); // numArrays = 2
        // Array 0: one valid 3-byte NAL.
        hvcc.push(32);
        hvcc.extend_from_slice(&1u16.to_be_bytes());
        hvcc.extend_from_slice(&3u16.to_be_bytes());
        hvcc.extend_from_slice(&[0x40, 0x01, 0x02]);
        // Array 1: one NAL declaring 100 bytes but only 2 present → truncated.
        hvcc.push(33);
        hvcc.extend_from_slice(&1u16.to_be_bytes());
        hvcc.extend_from_slice(&100u16.to_be_bytes());
        hvcc.extend_from_slice(&[0xAA, 0xBB]);
        let out = hvcc_to_annex_b(&hvcc).expect("the one valid NAL");
        // Only array 0's NAL is emitted.
        assert_eq!(
            &out[..],
            &[0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x02],
            "truncated trailing NAL dropped, valid prefix kept"
        );
    }

    #[test]
    fn hvcc_truncated_length_field_itself_stops() {
        // The 2-byte NAL length field itself runs past the buffer end → truncated
        // (offset + 2 > len guard). Emit only what completed.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1);
        hvcc.push(33);
        hvcc.extend_from_slice(&2u16.to_be_bytes()); // numNalus = 2
        hvcc.extend_from_slice(&2u16.to_be_bytes()); // NAL0 len 2
        hvcc.extend_from_slice(&[0x42, 0x01]);
        hvcc.push(0x00); // dangling single byte — can't form NAL1's length field
        let out = hvcc_to_annex_b(&hvcc).expect("NAL0");
        assert_eq!(&out[..], &[0x00, 0x00, 0x00, 0x01, 0x42, 0x01]);
    }

    #[test]
    fn hvcc_array_header_truncated_stops_outer_loop() {
        // numArrays claims 3 but only one array's header fits (offset + 3 > len).
        // The outer loop must break, not read out of bounds.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(3); // numArrays = 3 (lie)
        hvcc.push(32);
        hvcc.extend_from_slice(&1u16.to_be_bytes());
        hvcc.extend_from_slice(&2u16.to_be_bytes());
        hvcc.extend_from_slice(&[0x40, 0x01]);
        // No bytes for arrays 2 and 3 → outer loop's `offset + 3 > len` breaks.
        let out = hvcc_to_annex_b(&hvcc).expect("the one present NAL");
        assert_eq!(&out[..], &[0x00, 0x00, 0x00, 0x01, 0x40, 0x01]);
    }

    // --- length_prefixed_to_annex_b additional branches ---

    #[test]
    fn empty_input_yields_empty() {
        // Empty input → empty output (no pass-through of nothing).
        assert!(length_prefixed_to_annex_b(&[]).is_empty());
    }

    #[test]
    fn single_nal_length_prefix() {
        // One NAL: 4-byte len + body → one Annex B NAL.
        let mut buf = 5u32.to_be_bytes().to_vec();
        buf.extend_from_slice(&[0x26, 0x01, 0xAA, 0xBB, 0xCC]);
        let got = length_prefixed_to_annex_b(&buf);
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0x26, 0x01, 0xAA, 0xBB, 0xCC]);
        assert_eq!(got, want);
    }

    #[test]
    fn non_length_prefixed_three_plus_bytes_passes_through() {
        // 0xFFFFFFFF length with no body: exceeds remaining bytes on the very
        // first NAL, so parsed_any stays false and the buffer passes through.
        let raw = [0xFF, 0xFF, 0xFF, 0xFF, 0x11, 0x22];
        let got = length_prefixed_to_annex_b(&raw);
        assert_eq!(&got[..], &raw[..], "unparseable → passed through verbatim");
    }

    #[test]
    fn append_into_caller_buffer_preserves_existing() {
        // append_length_prefixed_as_annex_b writes into a caller buffer without
        // clobbering its existing contents (hot-path no-alloc API).
        let mut out = vec![0xDE, 0xAD];
        let mut nal = 2u32.to_be_bytes().to_vec();
        nal.extend_from_slice(&[0x11, 0x22]);
        append_length_prefixed_as_annex_b(&mut out, &nal);
        let mut want = vec![0xDE, 0xAD];
        want.extend_from_slice(&START_CODE);
        want.extend_from_slice(&[0x11, 0x22]);
        assert_eq!(out, want);
    }

    /// ISO/IEC 14496-15 §5.3.3.1.2 (avcC byte 4) and §8.3.3.1.2 (hvcC byte 21)
    /// each carry `lengthSizeMinusOne` in the low 2 bits. Nothing in the crate
    /// read it, so every conversion assumed a 4-octet prefix.
    #[test]
    fn nal_length_size_is_read_from_the_configuration_record() {
        use crate::disc::Codec;
        // avcC: byte 4 = 0xFF → lengthSizeMinusOne 3 → 4-octet prefixes.
        let mut avcc = vec![0x01, 0x64, 0x00, 0x28, 0xFF, 0xE1];
        assert_eq!(nal_length_size(Codec::H264, Some(&avcc)), 4);
        // 0xFD → lengthSizeMinusOne 1 → 2-octet prefixes (legal per §5.3.3.1.2).
        avcc[4] = 0xFD;
        assert_eq!(nal_length_size(Codec::H264, Some(&avcc)), 2);
        // 0xFC → lengthSizeMinusOne 0 → 1-octet prefixes.
        avcc[4] = 0xFC;
        assert_eq!(nal_length_size(Codec::H264, Some(&avcc)), 1);

        // hvcC: the field is byte 21, not byte 4.
        let mut hvcc = vec![0u8; 23];
        hvcc[21] = 0xFF;
        assert_eq!(nal_length_size(Codec::Hevc, Some(&hvcc)), 4);
        hvcc[21] = 0xFD;
        assert_eq!(nal_length_size(Codec::Hevc, Some(&hvcc)), 2);

        // Absent / too-short record, or a non-NAL codec → the crate's own width.
        assert_eq!(nal_length_size(Codec::Hevc, None), DEFAULT_NAL_LENGTH_SIZE);
        assert_eq!(
            nal_length_size(Codec::Hevc, Some(&hvcc[..8])),
            DEFAULT_NAL_LENGTH_SIZE
        );
        assert_eq!(
            nal_length_size(Codec::Mpeg2, Some(&avcc)),
            DEFAULT_NAL_LENGTH_SIZE
        );
    }

    // Regression (silent corruption): a source declaring 2-octet NAL lengths
    // was reframed by reading the first FOUR octets as one u32-BE length,
    // which passed the whole frame through verbatim with no start codes.
    #[test]
    fn two_octet_length_prefixes_convert_instead_of_leaking_raw_bytes() {
        // Two NALs with 2-octet prefixes: [0x00 0x03][3 bytes][0x00 0x02][2 bytes]
        let data = [
            0x00, 0x03, 0x67, 0x42, 0x1E, // NAL 1
            0x00, 0x02, 0x68, 0xCE, // NAL 2
        ];
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0x67, 0x42, 0x1E]);
        want.extend_from_slice(&START_CODE);
        want.extend_from_slice(&[0x68, 0xCE]);

        let mut got = Vec::new();
        append_length_prefixed_as_annex_b_sized(&mut got, &data, 2);
        assert_eq!(got, want, "2-octet prefixes must be reframed to Annex B");

        // What the 4-octet assumption produced: the raw bytes, verbatim, with no
        // start code anywhere.
        let mut assumed_four = Vec::new();
        append_length_prefixed_as_annex_b(&mut assumed_four, &data);
        assert_eq!(
            assumed_four,
            data.to_vec(),
            "the 4-octet assumption leaks the source bytes unconverted"
        );
        assert!(
            !assumed_four.starts_with(&START_CODE),
            "no start code at all — the video cannot decode"
        );
    }

    /// A 1-octet prefix width works the same way, and an out-of-range width
    /// falls back to the crate's own 4 rather than panicking or looping.
    #[test]
    fn one_octet_length_prefixes_and_out_of_range_width() {
        let data = [0x02, 0x40, 0x01, 0x01, 0x09];
        let mut got = Vec::new();
        append_length_prefixed_as_annex_b_sized(&mut got, &data, 1);
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0x40, 0x01]);
        want.extend_from_slice(&START_CODE);
        want.extend_from_slice(&[0x09]);
        assert_eq!(got, want);

        // width 0 and width 9 both clamp to DEFAULT_NAL_LENGTH_SIZE.
        let mut four = Vec::new();
        append_length_prefixed_as_annex_b(&mut four, &data);
        for bad in [0usize, 9] {
            let mut clamped = Vec::new();
            append_length_prefixed_as_annex_b_sized(&mut clamped, &data, bad);
            assert_eq!(clamped, four, "an impossible width clamps to 4");
        }
    }

    #[test]
    fn starts_with_start_code_detects_both_forms() {
        assert!(starts_with_start_code(&[0x00, 0x00, 0x00, 0x01, 0x42]));
        assert!(starts_with_start_code(&[0x00, 0x00, 0x01, 0x42]));
        assert!(!starts_with_start_code(&[0x00, 0x00, 0x02, 0x42]));
        assert!(!starts_with_start_code(&[0x42, 0x00, 0x00, 0x01]));
        assert!(!starts_with_start_code(&[]));
    }

    #[test]
    fn avcc_truncated_pps_keeps_sps() {
        // numPPS=1 but the PPS body runs past the end: SPS is salvaged.
        let avcc = [
            1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42, 1, 0, 9, 0x68,
        ];
        let (out, truncated) = avcc_parse(&avcc);
        assert_eq!(out.expect("SPS kept"), vec![0, 0, 0, 1, 0x67, 0x42]);
        assert!(truncated);
    }

    #[test]
    fn avcc_missing_pps_count_is_truncated() {
        let avcc = [1, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0, 2, 0x67, 0x42];
        let (out, truncated) = avcc_parse(&avcc);
        assert!(out.is_some());
        assert!(truncated);
    }

    #[test]
    fn hvcc_cut_array_header_is_truncated() {
        // Two arrays declared; the first (one 2-byte NAL) parses, the second header is absent.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(2);
        hvcc.extend_from_slice(&[0x20, 0, 1, 0, 2, 0x40, 0x01]);
        let (out, truncated) = hvcc_parse(&hvcc);
        assert!(out.is_some());
        assert!(truncated);
    }

    #[test]
    fn hvcc_complete_is_not_truncated() {
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1);
        hvcc.extend_from_slice(&[0x20, 0, 1, 0, 2, 0x40, 0x01]);
        assert!(!hvcc_parse(&hvcc).1);
    }
}

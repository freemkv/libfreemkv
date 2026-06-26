//! HEVC (H.265) elementary stream muxer — Annex B byte stream.
//!
//! Consumes [`PesFrame`](crate::pes::PesFrame)s for a single video track
//! and writes them as a raw `.hevc` / `.h265` Annex B byte stream:
//! `00 00 00 01 | NAL_unit | 00 00 00 01 | NAL_unit | …` with no
//! container framing.
//!
//! On the first frame the muxer emits the codec_private's VPS, SPS, PPS
//! (parsed from a `HEVCDecoderConfigurationRecord` in
//! `length-prefixed-in-hvcC` form), then converts each PES frame's
//! length-prefixed NAL units to Annex B and writes them.
//!
//! Sequential-only — no Cues, no backpatch. Target sink is any
//! [`SequentialSink`](crate::io::sink::SequentialSink): file, socket,
//! pipe, anything `Write + Send`.

use std::io::{self, Write};

/// Annex B 4-byte start code.
pub(crate) const START_CODE: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

/// HEVC NAL unit type bits live in `(byte0 >> 1) & 0x3F` in Annex B.
/// We don't filter NAL types here — the muxer is format-only — but we
/// keep the constant as documentation of the field layout.
#[allow(dead_code)]
const HEVC_NAL_TYPE_MASK: u8 = 0x3F;

/// Streaming HEVC Annex B muxer.
///
/// One instance per output stream. Tracks whether parameter sets have
/// already been emitted so they're written exactly once at the head of
/// the stream, mirroring the convention used by `ffmpeg -c:v copy -f
/// hevc`.
pub struct HevcMux<W: Write> {
    writer: W,
    /// `HEVCDecoderConfigurationRecord` payload (hvcC). Parsed lazily
    /// on the first `write_frame` so callers can set it after
    /// construction but before the first frame.
    codec_private: Option<Vec<u8>>,
    /// Set once VPS/SPS/PPS have been written to the stream. Subsequent
    /// frames write only their own NAL units.
    params_written: bool,
}

impl<W: Write> HevcMux<W> {
    /// Construct over `writer`. The muxer does not impose any extra
    /// buffering of its own — the sink owns its write buffering policy
    /// (see [`LocalFileSink`](crate::io::sink::LocalFileSink) and
    /// [`SocketSink`](crate::io::sink::SocketSink)).
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            codec_private: None,
            params_written: false,
        }
    }

    /// Provide the `HEVCDecoderConfigurationRecord` (hvcC) so the muxer
    /// can prepend VPS/SPS/PPS Annex B NALs at stream start. Optional —
    /// if the PES frames already carry inline parameter sets (some
    /// upstream demuxers do this), skipping this call is fine.
    pub fn set_codec_private(&mut self, data: Vec<u8>) {
        self.codec_private = Some(data);
    }

    /// Write one PES frame (= one access unit) as Annex B NAL units.
    ///
    /// Input may be either:
    ///   - Length-prefixed: `[u32-BE len][NAL bytes]` repeated. This is
    ///     the form emitted by libfreemkv's HEVC parser (the MKV-native
    ///     layout). Converted to Annex B.
    ///   - Already Annex B: a buffer beginning with a `00 00 00 01` or
    ///     `00 00 01` start code. Passed through unchanged.
    ///
    /// `_pts_ns` is accepted for symmetry with other muxers but ignored
    /// — Annex B has no timing layer.
    pub fn write_frame(&mut self, _pts_ns: i64, data: &[u8]) -> io::Result<()> {
        if !self.params_written {
            // Mark written *before* the write: a partial write that then
            // errors must not cause a later re-entry to re-emit the full
            // parameter set on top of the bytes the sink already
            // received (duplicate/split VPS/SPS/PPS). Callers discard the
            // mux on any write error.
            self.params_written = true;
            if let Some(cp) = &self.codec_private {
                match hvcc_to_annex_b(cp) {
                    Some(params) => self.writer.write_all(&params)?,
                    // A non-empty hvcC that yields no NAL is a caller
                    // contract violation: emitting the stream without
                    // VPS/SPS/PPS produces undecodable output. Surface it
                    // rather than dropping the parameter sets silently.
                    None if !cp.is_empty() => {
                        return Err(crate::error::Error::HevcParamParse.into());
                    }
                    None => {}
                }
            }
        }
        let annex_b = length_prefixed_to_annex_b(data);
        self.writer.write_all(&annex_b)
    }

    /// Flush the underlying writer. No trailer NAL is needed — an Annex
    /// B stream ends whenever the file/socket ends.
    pub fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Convert a `HEVCDecoderConfigurationRecord` (hvcC) into Annex B NAL
/// units. Returns `Some(bytes)` if at least one NAL was extracted, else
/// `None`.
///
/// Layout (per ISO/IEC 14496-15 §8.3.3.1.2):
///   - 22-byte fixed header
///   - byte 22 = `numOfArrays`
///   - each array: `array_completeness:1 | reserved:1 | NAL_unit_type:6`,
///     `numNalus:u16-BE`, then `numNalus` × `(nalUnitLength:u16-BE +
///     NAL bytes)`.
///
/// We don't filter on NAL type — VPS (32), SPS (33), PPS (34), and any
/// SEI arrays included in hvcC all get the same Annex B treatment.
///
/// This is the single source of truth for hvcC → Annex B across all
/// muxers (HEVC ES, BD-TS, standard MPEG-TS). Do not reimplement it.
pub(crate) fn hvcc_to_annex_b(hvcc: &[u8]) -> Option<Vec<u8>> {
    if hvcc.len() < 23 {
        return None;
    }
    let num_arrays = hvcc[22] as usize;
    let mut out = Vec::new();
    let mut offset = 23;
    // Set when an inner loop exits on truncation so the outer loop stops
    // too — otherwise it would re-interpret mid-NAL bytes as the next
    // array header and synthesize spurious parameter-set NALs.
    let mut truncated = false;
    for _ in 0..num_arrays {
        if truncated || offset + 3 > hvcc.len() {
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
    if out.is_empty() { None } else { Some(out) }
}

/// Convert length-prefixed NAL units (`[u32-BE len][NAL]` repeated) to
/// Annex B (`00 00 00 01 [NAL]` repeated).
///
/// Already-Annex-B input (a buffer beginning with a `00 00 00 01` or
/// `00 00 01` start code) is detected up front and passed through
/// unchanged — some upstream paths (raw HEVC ES from disc) hand Annex B
/// straight through the PES layer, and a genuine start code would
/// otherwise be misread as a u32-BE length prefix.
///
/// Truncation policy (single source of truth across all muxers): if a
/// length prefix runs past the end of the buffer (e.g. a NAL truncated
/// by a bad disc sector), the truncated trailing NAL is dropped and only
/// the valid Annex-B prefix accumulated so far is emitted. We never emit
/// a half-NAL nor leak raw length-prefixed bytes into the Annex-B stream.
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

/// Append the Annex B form of `data` (length-prefixed NALs) into `out`.
///
/// Same conversion as [`length_prefixed_to_annex_b`] but writes directly
/// into a caller-owned buffer, avoiding an intermediate allocation on
/// hot paths (e.g. per-frame video muxing). If `data` doesn't parse as
/// length-prefixed (no NALs extracted), it's appended unchanged on the
/// assumption it's already Annex B.
pub(crate) fn append_length_prefixed_as_annex_b(out: &mut Vec<u8>, data: &[u8]) {
    let mut offset = 0;
    // True once we've consumed at least one well-formed length prefix
    // (even a zero-length one). Distinguishes "parsed as length-prefixed,
    // all NALs empty" (emit nothing) from "not length-prefixed at all"
    // (pass through as already-Annex B).
    let mut parsed_any = false;
    while offset + 4 <= data.len() {
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        if offset + len > data.len() {
            // Mid-NAL truncation (e.g. a NAL cut by a bad disc sector) —
            // drop the truncated trailing NAL and emit only the valid
            // Annex-B prefix accumulated so far. We never emit a half-NAL
            // nor leak raw length-prefixed bytes into the Annex-B stream.
            break;
        }
        parsed_any = true;
        if len == 0 {
            // A zero-length prefix (e.g. pad bytes read off a damaged
            // sector) would otherwise emit a bare start code with no
            // RBSP — an invalid empty Annex B NAL. Skip it, mirroring
            // the `nal_len == 0` guard in `hvcc_to_annex_b` (ISO/IEC
            // 14496-15).
            continue;
        }
        out.extend_from_slice(&START_CODE);
        out.extend_from_slice(&data[offset..offset + len]);
        offset += len;
    }
    if !parsed_any && !data.is_empty() {
        // No length prefixes parsed at all and no leading start code:
        // pass the bytes through rather than discard them (recover-100%
        // goal — a decoder can attempt its own resync; dropping them
        // guarantees loss). This is distinct from "parsed as length-
        // prefixed but every NAL was zero-length", which emits nothing.
        out.extend_from_slice(data);
    }
}

/// Whether `data` begins with a 4-byte (`00 00 00 01`) or 3-byte
/// (`00 00 01`) Annex B start code.
fn starts_with_start_code(data: &[u8]) -> bool {
    data.starts_with(&START_CODE) || data.starts_with(&[0x00, 0x00, 0x01])
}

/// Convert an `AVCDecoderConfigurationRecord` (avcC) into Annex B NAL
/// units. Returns `Some(bytes)` if at least one NAL was extracted, else
/// `None`.
///
/// Layout (per ISO/IEC 14496-15 §5.3.3.1.2):
///   - 5-byte fixed header
///   - byte 5 = `[reserved:3 | numOfSequenceParameterSets:5]`
///   - `numOfSPS` × `(sequenceParameterSetLength:u16-BE + SPS bytes)`
///   - 1 byte = `numOfPictureParameterSets`
///   - `numOfPPS` × `(pictureParameterSetLength:u16-BE + PPS bytes)`
///
/// The H.264 counterpart to [`hvcc_to_annex_b`]: the single source of
/// truth for avcC → Annex B across all muxers (H.264 ES, BD-TS, standard
/// MPEG-TS, the `demux://` sink). Do not reimplement it.
pub(crate) fn avcc_to_annex_b(avcc: &[u8]) -> Option<Vec<u8>> {
    // avcC fixed header is 5 bytes; byte 5 carries the SPS count (low 5 bits),
    // then the SPS array begins at byte 6 (ISO/IEC 14496-15 §5.3.3.1.2).
    const AVCC_HEADER_LEN: usize = 5;
    const NUM_SPS_MASK: u8 = 0x1F; // numOfSequenceParameterSets: low 5 bits
    if avcc.len() < AVCC_HEADER_LEN + 1 {
        return None;
    }
    let num_sps = (avcc[AVCC_HEADER_LEN] & NUM_SPS_MASK) as usize;
    let mut offset = AVCC_HEADER_LEN + 1;
    let mut out = Vec::new();

    // Extract `count` length-prefixed NALs starting at `*offset` into `out`.
    // Returns `false` (truncated) if a length field or NAL body runs past the
    // end — the caller then stops, so it never reads further length fields out
    // of mid-NAL bytes (mirrors `hvcc_to_annex_b`).
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

    let sps_ok = take(avcc, num_sps, &mut offset, &mut out);
    if sps_ok && offset < avcc.len() {
        let num_pps = avcc[offset] as usize;
        offset += 1;
        take(avcc, num_pps, &mut offset, &mut out);
    }

    if out.is_empty() { None } else { Some(out) }
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
        // First NAL is valid (2-byte payload), second has a length prefix
        // claiming 100 bytes with only 3 present. Policy: emit the valid
        // first NAL as Annex B, drop the truncated trailing NAL — never
        // leak raw length-prefixed bytes into the Annex B stream.
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
    fn write_frame_errors_on_unparseable_non_empty_hvcc() {
        // A non-empty hvcC that yields no NAL must surface an error
        // instead of silently producing a parameter-set-less stream.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        mux.set_codec_private(vec![0xDE, 0xAD]); // too short to be valid hvcC
        let err = mux.write_frame(0, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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
        // A 4+ byte buffer that does NOT parse as length-prefixed (the first
        // u32 length exceeds the remaining bytes on the very first NAL, parsing
        // nothing) is passed through unchanged (parsed_any == false branch).
        // 0xFFFFFFFF length with no body → parsed_any stays false → pass-through.
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

    #[test]
    fn starts_with_start_code_detects_both_forms() {
        assert!(starts_with_start_code(&[0x00, 0x00, 0x00, 0x01, 0x42]));
        assert!(starts_with_start_code(&[0x00, 0x00, 0x01, 0x42]));
        assert!(!starts_with_start_code(&[0x00, 0x00, 0x02, 0x42]));
        assert!(!starts_with_start_code(&[0x42, 0x00, 0x00, 0x01]));
        assert!(!starts_with_start_code(&[]));
    }

    // --- HevcMux: params-once + error semantics ---

    #[test]
    fn mux_empty_codec_private_emits_no_params() {
        // An EMPTY (not absent) hvcC: hvcc_to_annex_b returns None, but cp is
        // empty so it's NOT a contract violation → no error, just no params.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        mux.set_codec_private(Vec::new());
        let mut frame = 2u32.to_be_bytes().to_vec();
        frame.extend_from_slice(&[0xAA, 0xBB]);
        mux.write_frame(0, &frame).unwrap();
        mux.finish().unwrap();
        // Only the frame NAL, no parameter sets.
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0xAA, 0xBB]);
        assert_eq!(sink, want);
    }

    #[test]
    fn mux_no_codec_private_writes_frames_only() {
        // No hvcC set at all: frames pass through, no params, no error.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        let mut frame = 2u32.to_be_bytes().to_vec();
        frame.extend_from_slice(&[0xAA, 0xBB]);
        mux.write_frame(0, &frame).unwrap();
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0xAA, 0xBB]);
        assert_eq!(sink, want);
    }

    #[test]
    fn mux_params_not_re_emitted_after_unparseable_error() {
        // params_written is set BEFORE the write, so after an error on the first
        // frame, a retry must NOT re-emit params (the comment's invariant).
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        mux.set_codec_private(vec![0xDE, 0xAD]); // unparseable, non-empty → error
        assert!(mux.write_frame(0, &[]).is_err(), "first frame errors");
        // A second frame must not retry the (already-marked) params.
        let mut frame = 2u32.to_be_bytes().to_vec();
        frame.extend_from_slice(&[0xAA, 0xBB]);
        mux.write_frame(0, &frame).unwrap();
        // Sink holds only the frame NAL — no parameter bytes, no duplication.
        let mut want = START_CODE.to_vec();
        want.extend_from_slice(&[0xAA, 0xBB]);
        assert_eq!(sink, want, "params not re-emitted after the error");
    }

    #[test]
    fn mux_annex_b_frame_passes_through() {
        // A frame already in Annex B (leading start code) is written verbatim,
        // not re-framed as length-prefixed.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        let frame = [0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0xDE];
        mux.write_frame(0, &frame).unwrap();
        assert_eq!(sink, frame);
    }

    #[test]
    fn mux_writes_params_then_frames() {
        // Build hvcC with one SPS to verify params-once semantics.
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1);
        hvcc.push(33);
        hvcc.extend_from_slice(&1u16.to_be_bytes());
        hvcc.extend_from_slice(&3u16.to_be_bytes());
        hvcc.extend_from_slice(&[0x42, 0x01, 0x01]);

        let mut frame_data = Vec::new();
        frame_data.extend_from_slice(&2u32.to_be_bytes());
        frame_data.extend_from_slice(&[0xAA, 0xBB]);

        let mut sink: Vec<u8> = Vec::new();
        let mut mux = HevcMux::new(&mut sink);
        mux.set_codec_private(hvcc);
        mux.write_frame(0, &frame_data).unwrap();
        // Second frame — no SPS re-emission.
        mux.write_frame(40_000_000, &frame_data).unwrap();
        mux.finish().unwrap();

        // SPS NAL (7 bytes) + 2× frame NAL (6 bytes) = 19 bytes.
        assert_eq!(sink.len(), 7 + 6 + 6);
        // Start codes at offsets 0 (SPS), 7 (frame1), 13 (frame2).
        assert_eq!(&sink[0..4], &START_CODE);
        assert_eq!(&sink[7..11], &START_CODE);
        assert_eq!(&sink[13..17], &START_CODE);
        assert_eq!(sink[4], 0x42);
        assert_eq!(sink[11], 0xAA);
        assert_eq!(sink[17], 0xAA);
    }
}

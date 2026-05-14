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
    ///   - Already Annex B: bytes containing `00 00 00 01` start codes
    ///     anywhere in the buffer. Passed through unchanged.
    ///
    /// `_pts_ns` is accepted for symmetry with other muxers but ignored
    /// — Annex B has no timing layer.
    pub fn write_frame(&mut self, _pts_ns: i64, data: &[u8]) -> io::Result<()> {
        if !self.params_written {
            if let Some(cp) = &self.codec_private {
                if let Some(params) = hvcc_to_annex_b(cp) {
                    self.writer.write_all(&params)?;
                }
            }
            self.params_written = true;
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
fn hvcc_to_annex_b(hvcc: &[u8]) -> Option<Vec<u8>> {
    if hvcc.len() < 23 {
        return None;
    }
    let num_arrays = hvcc[22] as usize;
    let mut out = Vec::new();
    let mut offset = 23;
    for _ in 0..num_arrays {
        if offset + 3 > hvcc.len() {
            break;
        }
        offset += 1; // array_completeness + nal_type byte
        let num_nalus = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
        offset += 2;
        for _ in 0..num_nalus {
            if offset + 2 > hvcc.len() {
                break;
            }
            let nal_len = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
            offset += 2;
            if offset + nal_len > hvcc.len() {
                break;
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
/// If the input doesn't parse as length-prefixed (no valid lengths
/// extracted), it's returned unchanged on the assumption that it's
/// already Annex B — some upstream paths (raw HEVC ES from disc) pass
/// Annex B straight through the PES layer.
pub(crate) fn length_prefixed_to_annex_b(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + (data.len() / 32));
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        if offset + len > data.len() {
            // Mid-NAL truncation — fall through to the pass-through path
            // rather than emitting a half-NAL.
            return data.to_vec();
        }
        out.extend_from_slice(&START_CODE);
        out.extend_from_slice(&data[offset..offset + len]);
        offset += len;
    }
    if out.is_empty() && !data.is_empty() {
        // No length prefixes found — input is likely already Annex B.
        return data.to_vec();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn mid_nal_truncation_returns_original() {
        // `[u32-BE 100][only 3 bytes]` — length prefix claims 100 bytes
        // but the input only has 3 after the prefix. We treat that as
        // malformed and pass the original buffer through so receivers
        // can attempt their own recovery.
        let mut raw = Vec::new();
        raw.extend_from_slice(&100u32.to_be_bytes());
        raw.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let got = length_prefixed_to_annex_b(&raw);
        assert_eq!(&got[..], &raw[..]);
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

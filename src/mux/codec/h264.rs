//! H.264 (AVC) elementary stream parser.
//!
//! Extracts SPS and PPS NAL units for MKV codecPrivate.
//! Detects keyframes (IDR slices).
//! Each PES packet = one access unit = one frame.

use super::startcode::{find_start_code, skip_start_code};
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// H.264 NAL unit types we care about.
const NAL_SLICE_IDR: u8 = 5;
const NAL_SPS: u8 = 7;
const NAL_PPS: u8 = 8;
const NAL_AUD: u8 = 9;

/// H.264 (AVC) Annex B → MKV codec parser: extracts SPS/PPS for the avcC
/// codecPrivate, detects IDR keyframes, and converts each PES access unit into
/// length-prefixed NAL units. Implements [`CodecParser`].
pub struct H264Parser {
    // First-seen SPS/PPS seed the MKV codecPrivate (avcC) — the only out-of-band
    // copy the player gets. BD H.264 repeats the parameter sets at every IDR;
    // a player re-applies the avcC copy at each keyframe. A stream may redefine
    // a parameter set mid-title under the SAME id with a different body. Any
    // occurrence whose body DIFFERS from the codecPrivate copy must therefore be
    // emitted IN-BAND at each point it appears so it overrides the re-applied
    // avcC set; otherwise those frames decode against the wrong parameter set.
    // (Same defect class as the HEVC PPS-redefinition bug.)
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

impl Default for H264Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl H264Parser {
    /// Create a fresh H.264 parser with no parameter sets captured yet.
    pub fn new() -> Self {
        Self {
            sps: None,
            pps: None,
        }
    }
}

/// Handle an SPS/PPS NAL (mirrors the HEVC fix):
/// - First of its type → seeds codecPrivate (`first`); stripped from frame data
///   (the player gets it from avcC).
/// - Identical to the codecPrivate copy → stripped (the player re-applies it
///   from avcC at each keyframe; BD streams repeat param sets at every IDR).
/// - DIFFERENT body from the codecPrivate copy (a mid-title redefinition of the
///   same id) → emitted IN-BAND (length-prefixed) at EVERY occurrence so it
///   overrides the avcC copy the player re-applies at each keyframe.
fn handle_param_set(first: &mut Option<Vec<u8>>, nal: &[u8], frame_data: &mut Vec<u8>) {
    match first {
        None => {
            first.replace(nal.to_vec()); // seeds codecPrivate; stripped here
        }
        Some(f) if f.as_slice() == nal => {} // == codecPrivate → player has it
        Some(_) => {
            // Differs from codecPrivate → emit in-band so it wins at this AU.
            // A NAL longer than u32::MAX cannot be length-prefixed in the
            // 4-byte field; skip it rather than emit a truncated length over
            // the full body (mis-framed NALU). Unreachable in practice — no
            // real access unit is >4 GiB.
            let Ok(len) = u32::try_from(nal.len()) else {
                return;
            };
            frame_data.extend_from_slice(&len.to_be_bytes());
            frame_data.extend_from_slice(nal);
        }
    }
}

impl CodecParser for H264Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // MKV block timecodes are PRESENTATION timestamps; frames are stored in
        // decode order and the player reorders by timecode. Use PTS, not DTS —
        // DTS presents B-frames in decode order (visible judder) and breaks
        // PTS-based seeking. Fall back to DTS only if PTS is absent.
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);

        // Single pass: detect IDR keyframes, seed/strip param sets, and convert
        // Annex B (start-code prefixed) NALUs to length-prefixed NALUs (MKV with
        // AVCDecoderConfigurationRecord expects a 4-byte length prefix per NAL).
        let mut keyframe = false;
        // Pre-size: output is ~input bytes plus a few 4-byte NAL length prefixes.
        // The unsized Vec growth chain otherwise reallocs several times per
        // frame in the mux hot path (mirrors the HEVC parser).
        let mut frame_data = Vec::with_capacity(pes.data.len() + 64);

        for nal in NalIterator::new(&pes.data) {
            let nal_type = nal[0] & 0x1F;

            match nal_type {
                // Param sets: seed avcC, strip if identical, emit in-band if a
                // mid-title redefinition differs from the avcC copy.
                NAL_SPS => handle_param_set(&mut self.sps, nal, &mut frame_data),
                NAL_PPS => handle_param_set(&mut self.pps, nal, &mut frame_data),
                // Access unit delimiters: drop. Intentional and spec-correct —
                // Matroska H.264 frame data omits AUDs (the container delimits
                // access units), so keeping them in-band is redundant. Mirrors
                // the HEVC parser.
                NAL_AUD => {}
                _ => {
                    if nal_type == NAL_SLICE_IDR {
                        keyframe = true;
                    }
                    // A NAL longer than u32::MAX can't be length-prefixed in the
                    // 4-byte field; skip it rather than mis-frame the output.
                    // Unreachable in practice (no real AU is >4 GiB).
                    let Ok(len) = u32::try_from(nal.len()) else {
                        continue;
                    };
                    frame_data.extend_from_slice(&len.to_be_bytes());
                    frame_data.extend_from_slice(nal);
                }
            }
        }

        if frame_data.is_empty() {
            return Vec::new();
        }

        vec![Frame {
            pts_ns,
            keyframe,
            data: frame_data,
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // Build AVCDecoderConfigurationRecord from SPS + PPS
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        if sps.len() < 4 {
            return None;
        }

        // avcC encodes each NAL's length in a 16-bit field. A param set larger
        // than 65535 bytes would truncate the length while the full bytes are
        // appended → mis-framed record. Refuse rather than emit a corrupt avcC
        // (param sets this large are non-conforming anyway).
        if sps.len() > 0xFFFF || pps.len() > 0xFFFF {
            return None;
        }

        // AVCDecoderConfigurationRecord (ISO 14496-15):
        // configurationVersion = 1
        // AVCProfileIndication = SPS[1]
        // profile_compatibility = SPS[2]
        // AVCLevelIndication = SPS[3]
        // lengthSizeMinusOne = 3 (4-byte length prefix)
        // numOfSequenceParameterSets = 1
        // sequenceParameterSetLength = sps.len()
        // sequenceParameterSetNALUnit = sps
        // numOfPictureParameterSets = 1
        // pictureParameterSetLength = pps.len()
        // pictureParameterSetNALUnit = pps

        let mut record = vec![
            1,      // configurationVersion
            sps[1], // profile
            sps[2], // compatibility
            sps[3], // level
            0xFF,   // 6 bits reserved (111111) + 2 bits lengthSizeMinusOne (11 = 3)
            0xE1,   // 3 bits reserved (111) + 5 bits numSPS (1)
            (sps.len() >> 8) as u8,
            sps.len() as u8,
        ];
        record.extend_from_slice(sps);
        record.push(1); // numPPS
        record.push((pps.len() >> 8) as u8);
        record.push(pps.len() as u8);
        record.extend_from_slice(pps);

        Some(record)
    }
}

/// Iterator over NAL units in Annex B byte stream.
/// Finds start codes (00 00 01 or 00 00 00 01) and yields the data between them.
struct NalIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> NalIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        // Skip to first start code
        let pos = find_start_code(data, 0).unwrap_or(data.len());
        Self { data, pos }
    }
}

impl<'a> Iterator for NalIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        // Loop (not tail-recursion) over empty NALs: a crafted/garbled Annex B
        // stream with many adjacent start codes (e.g. 00 00 01 00 00 01 ...)
        // yields empty NALs back-to-back; recursing once per empty NAL would
        // overflow the stack. `self.pos` advances to `nal_end` each iteration,
        // so the loop always terminates. Mirrors the HEVC parser's while-scan.
        loop {
            if self.pos >= self.data.len() {
                return None;
            }

            // Skip the start code at current position
            let nal_start = skip_start_code(self.data, self.pos)?;

            // Find next start code (or end of data)
            let nal_end = find_start_code(self.data, nal_start).unwrap_or(self.data.len());

            // Strip the leading zeros of the following start code. For a
            // conforming bitstream this is lossless: rbsp_trailing_bits() sets a
            // stop-one bit, so the final byte of any RBSP is never 0x00 — the only
            // trailing zeros here belong to the next 00 00 (00) 01 prefix, never to
            // the NAL's RBSP payload. (Mirrors the HEVC parser.)
            let mut end = nal_end;
            while end > nal_start && self.data[end - 1] == 0x00 {
                end -= 1;
            }

            self.pos = nal_end;

            if end > nal_start {
                return Some(&self.data[nal_start..end]);
            }
            // Empty NAL — continue scanning instead of recursing.
        }
    }
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

    // --- parse SPS+PPS → codec_private ---

    #[test]
    fn parse_sps_pps() {
        let mut parser = H264Parser::new();

        // Build PES with SPS (type 7) + PPS (type 8) + IDR slice (type 5)
        // SPS NAL: 0x67 = 0_11_00111 (nal_type = 7), followed by profile/compat/level + payload
        // PPS NAL: 0x68 = 0_11_01000 (nal_type = 8)
        let mut data = Vec::new();
        // SPS: 00 00 01 [67 42 00 1E <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67); // SPS
        data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB, 0xCD]); // profile=0x42, compat=0x00, level=0x1E
        // PPS: 00 00 01 [68 <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x68); // PPS
        data.extend_from_slice(&[0xCE, 0x01]);
        // IDR slice: 00 00 01 [65 <payload>]
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65); // IDR
        data.extend_from_slice(&[0x88, 0x00, 0x10]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        // codec_private should now be available
        let cp = parser.codec_private();
        assert!(
            cp.is_some(),
            "codec_private should be Some after seeing SPS+PPS"
        );
        let cp = cp.unwrap();

        // AVCDecoderConfigurationRecord checks
        assert_eq!(cp[0], 1, "configurationVersion");
        assert_eq!(cp[1], 0x42, "profile from SPS[1]");
        assert_eq!(cp[2], 0x00, "compatibility from SPS[2]");
        assert_eq!(cp[3], 0x1E, "level from SPS[3]");
        assert_eq!(cp[4], 0xFF, "reserved + lengthSizeMinusOne=3");
        assert_eq!(cp[5], 0xE1, "reserved + numSPS=1");

        // Frames should have been produced
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn codec_private_none_before_sps_pps() {
        let parser = H264Parser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- IDR keyframe detection ---

    #[test]
    fn parse_idr_keyframe() {
        let mut parser = H264Parser::new();

        // PES with IDR NAL (type 5 = 0x65)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65); // IDR slice (nal_type = 5)
        data.extend_from_slice(&[0x88, 0x00, 0x10, 0x20]);

        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(
            frames[0].keyframe,
            "IDR slice should be detected as keyframe"
        );
    }

    // --- non-IDR → not keyframe ---

    #[test]
    fn parse_non_idr() {
        let mut parser = H264Parser::new();

        // PES with non-IDR slice (type 1 = 0x61 or 0x41)
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41); // non-IDR coded slice (nal_type = 1)
        data.extend_from_slice(&[0x9A, 0x00, 0x10]);

        let pes = make_pes(data, Some(180000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert!(!frames[0].keyframe, "non-IDR slice should not be keyframe");
    }

    // --- length prefix conversion ---

    #[test]
    fn length_prefix_conversion() {
        let mut parser = H264Parser::new();

        // PES with a single non-IDR NAL
        let nal_payload = [0x41, 0xAA, 0xBB, 0xCC, 0xDD]; // type 1, 5 bytes
        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&nal_payload);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        let frame_data = &frames[0].data;

        // Should start with 4-byte big-endian length prefix
        assert!(
            frame_data.len() >= 4,
            "frame data should have length prefix"
        );
        let length =
            u32::from_be_bytes([frame_data[0], frame_data[1], frame_data[2], frame_data[3]]);
        assert_eq!(
            length as usize,
            nal_payload.len(),
            "length prefix should match NAL size"
        );

        // Followed by the NAL data itself
        assert_eq!(&frame_data[4..], &nal_payload);

        // No start code (00 00 01) should appear in the output
        for i in 0..frame_data.len().saturating_sub(2) {
            let is_sc =
                frame_data[i] == 0x00 && frame_data[i + 1] == 0x00 && frame_data[i + 2] == 0x01;
            assert!(!is_sc, "output should not contain Annex B start codes");
        }
    }

    // --- SPS/PPS/AUD are stripped from frame data ---

    #[test]
    fn sps_pps_aud_stripped_from_frame_data() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        // AUD (type 9)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x09);
        data.push(0xF0);
        // SPS (type 7)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67);
        data.extend_from_slice(&[0x42, 0x00, 0x1E, 0xAB]);
        // PPS (type 8)
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x68);
        data.extend_from_slice(&[0xCE, 0x01]);
        // IDR (type 5) - only this should appear in frame data
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x65);
        data.extend_from_slice(&[0x88, 0x00]);

        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);

        // Frame data should only contain the IDR NAL (length-prefixed)
        let fd = &frames[0].data;
        let length = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]);
        // IDR NAL is 0x65, 0x88 (trailing 0x00 is stripped as potential start code prefix)
        assert_eq!(length, 2);
        assert_eq!(fd[4], 0x65); // IDR NAL type byte
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41);
        data.extend_from_slice(&[0x00, 0x10]);

        // PTS = 90000 (1 second at 90kHz) → 1_000_000_000 ns
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // --- empty PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = H264Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS (presentation) used for the MKV block timecode, not DTS ---

    #[test]
    fn pts_preferred_over_dts() {
        let mut parser = H264Parser::new();

        let mut data = Vec::new();
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x41);
        data.extend_from_slice(&[0x00, 0x10]);

        let pes = PesPacket {
            pid: 0x1011,
            pts: Some(180000), // 2 seconds (presentation)
            dts: Some(90000),  // 1 second (decode)
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        // PTS must be used — MKV block timecodes are presentation timestamps.
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
    }

    // --- mid-title param-set redefinition emitted in-band ---

    /// Collect the NAL types from a length-prefixed frame_data buffer.
    fn frame_nal_types(fd: &[u8]) -> Vec<u8> {
        let mut types = Vec::new();
        let mut off = 0;
        while off + 4 <= fd.len() {
            let len = u32::from_be_bytes([fd[off], fd[off + 1], fd[off + 2], fd[off + 3]]) as usize;
            off += 4;
            if off + len > fd.len() {
                break;
            }
            types.push(fd[off] & 0x1F);
            off += len;
        }
        types
    }

    #[test]
    fn first_param_sets_stripped_redefinition_emitted_inline() {
        let mut parser = H264Parser::new();

        // AU 1: SPS(id0,bodyA) + PPS(id0,bodyA) + IDR. Both param sets are the
        // first of their type → seed avcC, stripped from frame data.
        let mut au1 = Vec::new();
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS body A
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x68, 0x11]); // PPS body A
        au1.extend_from_slice(&[0x00, 0x00, 0x01]);
        au1.extend_from_slice(&[0x65, 0x10, 0x20]); // IDR
        let f1 = parser.parse(&make_pes(au1, Some(0)));
        assert_eq!(f1.len(), 1);
        // Frame 1 carries only the IDR — param sets stripped (in avcC).
        assert_eq!(
            frame_nal_types(&f1[0].data),
            vec![5],
            "AU1: only IDR in-band"
        );

        // AU 2: SPS identical to avcC, PPS REDEFINED (same id, different body) +
        // IDR. The identical SPS is stripped; the redefined PPS must be emitted
        // in-band so it overrides the avcC copy at this keyframe.
        let mut au2 = Vec::new();
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]); // SPS == body A
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x68, 0x22]); // PPS body B (redefinition)
        au2.extend_from_slice(&[0x00, 0x00, 0x01]);
        au2.extend_from_slice(&[0x65, 0x30, 0x40]); // IDR
        let f2 = parser.parse(&make_pes(au2, Some(90000)));
        assert_eq!(f2.len(), 1);
        let types = frame_nal_types(&f2[0].data);
        assert!(
            types.contains(&8),
            "redefined PPS (type 8) must be emitted in-band, got {types:?}"
        );
        assert!(
            !types.contains(&7),
            "identical SPS (type 7) must stay stripped, got {types:?}"
        );
        assert!(types.contains(&5), "IDR (type 5) present, got {types:?}");
    }

    #[test]
    fn repeated_identical_param_sets_stay_stripped() {
        let mut parser = H264Parser::new();
        let mut au = Vec::new();
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x67, 0x42, 0x00, 0x1E, 0xAA]);
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x68, 0x11]);
        au.extend_from_slice(&[0x00, 0x00, 0x01]);
        au.extend_from_slice(&[0x65, 0x10]);
        // Two identical AUs.
        parser.parse(&make_pes(au.clone(), Some(0)));
        let f = parser.parse(&make_pes(au, Some(90000)));
        assert_eq!(
            frame_nal_types(&f[0].data),
            vec![5],
            "repeated identical SPS/PPS stay in avcC, not duplicated in-band"
        );
    }

    #[test]
    fn many_empty_nals_do_not_overflow_stack() {
        // Regression: NalIterator::next must iterate, not recurse, over empty
        // NALs. A crafted Annex B stream of tens of thousands of adjacent start
        // codes (each producing an empty NAL) would blow the stack under the old
        // tail-recursive implementation. Iterating handles it in bounded stack.
        let mut data = Vec::new();
        // 50_000 back-to-back 3-byte start codes → 50_000 empty NALs.
        for _ in 0..50_000 {
            data.extend_from_slice(&[0x00, 0x00, 0x01]);
        }
        // One real NAL at the end so the iterator yields something.
        data.extend_from_slice(&[0x41, 0xAA, 0xBB]);

        let mut parser = H264Parser::new();
        let frames = parser.parse(&make_pes(data, Some(0)));
        // Exactly one populated frame; the empty NALs are skipped without
        // overflowing.
        assert_eq!(frames.len(), 1);
        let fd = &frames[0].data;
        let len = u32::from_be_bytes([fd[0], fd[1], fd[2], fd[3]]) as usize;
        assert_eq!(len, 3, "the single real NAL is length-prefixed");
        assert_eq!(fd[4], 0x41);
    }

    #[test]
    fn avcc_oversized_param_set_returns_none() {
        // A param set > 65535 bytes can't be length-encoded in avcC's 16-bit
        // field; codec_private must refuse rather than emit a truncated record.
        let mut parser = H264Parser::new();
        let mut data = Vec::new();
        // Oversized SPS (header byte 0x67 + 70000 filler bytes).
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.push(0x67);
        data.extend_from_slice(&vec![0x11u8; 70_000]);
        // PPS
        data.extend_from_slice(&[0x00, 0x00, 0x01]);
        data.extend_from_slice(&[0x68, 0x11]);
        parser.parse(&make_pes(data, Some(0)));
        assert!(
            parser.codec_private().is_none(),
            "oversized SPS must not produce a truncated avcC"
        );
    }
}

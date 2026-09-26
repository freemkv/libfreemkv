//! AAC ADTS framing and validation.
//!
//! See docs/adts.md for framing, timestamp and validation rules.

use super::audio_frames::{AudioFrames, Header};
#[cfg(test)]
use super::pts_to_ns;
use super::{CodecParser, Frame, PesPacket};

/// ADTS `sampling_frequency_index` table (ISO/IEC 14496-3) — 13 valid entries;
/// indices 13/14/15 are 0 (reserved) and constitute a hard reject.
const ADTS_SAMPLE_RATE_VALID: [u32; 16] = [
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350, 0, 0,
    0,
];

/// ADTS header verdict for the packet head.
enum AdtsVerdict {
    /// No 12-bit ADTS sync at the head — not an ADTS frame we can validate.
    NoSync,
    /// Sync present and the three structural fields are legal.
    Valid,
    /// Sync present but a reserved sample-rate index or a sub-header
    /// frame-length — structurally invalid per the ADTS spec.
    Invalid,
}

fn adts_verdict(data: &[u8]) -> AdtsVerdict {
    // Need the full 7-byte fixed+variable header to read frame_length.
    if data.len() < 7 {
        return AdtsVerdict::NoSync;
    }
    // 12-bit syncword 0xFFF: byte0 == 0xFF and top nibble of byte1 == 0xF.
    if data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return AdtsVerdict::NoSync;
    }
    // sampling_frequency_index: byte2 bits 5..2.
    let sr_index = ((data[2] >> 2) & 0x0F) as usize;
    if ADTS_SAMPLE_RATE_VALID[sr_index] == 0 {
        return AdtsVerdict::Invalid;
    }
    // aac_frame_length: 13 bits = byte3[1:0] | byte4 | byte5[7:5].
    let frame_length =
        ((u32::from(data[3]) & 0x03) << 11) | (u32::from(data[4]) << 3) | (u32::from(data[5]) >> 5);
    // Floor depends on protection_absent: CRC-present (byte1 bit0 clear) adds a
    // 16-bit crc_check after the 7-byte header, so the min is 9, not a flat 7 —
    // else a CRC frame declaring length 7-8 wrongly passed as Valid.
    let header_bytes = if data[1] & 0x01 == 0 { 9 } else { 7 };
    if frame_length < header_bytes {
        return AdtsVerdict::Invalid;
    }
    AdtsVerdict::Valid
}

pub struct AdtsParser {
    frames: AudioFrames,
    config: Option<Vec<u8>>,
}

impl Default for AdtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl AdtsParser {
    pub fn new() -> Self {
        Self {
            frames: AudioFrames::new("aac"),
            config: None,
        }
    }
    pub fn dropped_frames(&self) -> u64 {
        self.frames.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.frames.dropped_duration_ns()
    }
}

impl CodecParser for AdtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let config = &mut self.config;
        self.frames.parse(pes, 7, |data| {
            if !matches!(adts_verdict(data), AdtsVerdict::Valid) {
                return None;
            }
            let rate_index = (data[2] >> 2) & 15;
            let object_type = (data[2] >> 6) + 1;
            let channels = ((data[2] & 1) << 2) | (data[3] >> 6);
            // MPEG-4 AudioSpecificConfig replaces the ADTS transport header in
            // Matroska. The raw AAC payload must not retain that header or CRC.
            config.get_or_insert_with(|| {
                vec![
                    (object_type << 3) | (rate_index >> 1),
                    (rate_index << 7) | (channels << 3),
                ]
            });
            Some(Header {
                bytes: (usize::from(data[3] & 3) << 11)
                    | (usize::from(data[4]) << 3)
                    | usize::from(data[5] >> 5),
                skip: if data[1] & 1 == 0 { 9 } else { 7 },
                samples: 1024 * (u32::from(data[6] & 3) + 1),
                rate: ADTS_SAMPLE_RATE_VALID[usize::from(rate_index)],
            })
        })
    }

    fn flush(&mut self) -> Vec<Frame> {
        self.frames.flush()
    }
    fn codec_private(&self) -> Option<Vec<u8>> {
        self.config.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1100,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    // A header claiming a CRC (protection_absent=0) but declaring a frame
    // length too short to contain one. See docs/adts.md for full rationale.
    #[test]
    fn a_crc_present_header_shorter_than_its_own_crc_is_invalid() {
        for declared in [7u32, 8] {
            let mut f = adts_frame(16);
            f[1] = 0xF0; // sync + MPEG-4, protection_absent = 0 => CRC present
            f[3] = (f[3] & 0xFC) | ((declared >> 11) & 0x03) as u8;
            f[4] = ((declared >> 3) & 0xFF) as u8;
            f[5] = (f[5] & 0x1F) | ((declared & 0x07) << 5) as u8;
            assert!(
                matches!(adts_verdict(&f), AdtsVerdict::Invalid),
                "protection_absent=0 declaring {declared} bytes cannot hold its \
                 own 7-byte header plus a 2-byte CRC"
            );
        }

        // 9 is the smallest length that CAN hold header + CRC, so it must pass
        // the structural gate — the floor moved, it did not become stricter
        // than the spec.
        let mut ok = adts_frame(16);
        ok[1] = 0xF0;
        let nine = 9u32;
        ok[3] = (ok[3] & 0xFC) | ((nine >> 11) & 0x03) as u8;
        ok[4] = ((nine >> 3) & 0xFF) as u8;
        ok[5] = (ok[5] & 0x1F) | ((nine & 0x07) << 5) as u8;
        assert!(matches!(adts_verdict(&ok), AdtsVerdict::Valid));

        // And with NO CRC the floor is still 7, unchanged.
        let mut no_crc = adts_frame(16);
        no_crc[1] = 0xF1; // protection_absent = 1
        let seven = 7u32;
        no_crc[3] = (no_crc[3] & 0xFC) | ((seven >> 11) & 0x03) as u8;
        no_crc[4] = ((seven >> 3) & 0xFF) as u8;
        no_crc[5] = (no_crc[5] & 0x1F) | ((seven & 0x07) << 5) as u8;
        assert!(matches!(adts_verdict(&no_crc), AdtsVerdict::Valid));
    }

    /// A valid ADTS header (AAC-LC, 44.1 kHz, stereo) + payload, with
    /// aac_frame_length set to the total size.
    fn adts_frame(payload: usize) -> Vec<u8> {
        let total = 7 + payload;
        let mut f = vec![0u8; total];
        f[0] = 0xFF;
        f[1] = 0xF1; // sync + MPEG-4 + no CRC (protection_absent=1)
        f[2] = 0x50; // profile=AAC-LC, sr_index=4 (44.1 kHz)
        f[3] = 0x80; // channel_config low + start of frame_length
        // frame_length (13 bits) = total.
        let fl = total as u32;
        f[3] = (f[3] & 0xFC) | ((fl >> 11) & 0x03) as u8;
        f[4] = ((fl >> 3) & 0xFF) as u8;
        f[5] = (((fl & 0x07) << 5) as u8) | 0x1F; // low 3 bits of len + buffer-fullness bits
        f
    }

    #[test]
    fn valid_adts_is_kept() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(adts_frame(400), Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000));
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn pes_without_pts_advances_by_sample_count() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
        // advance from the last timestamp by sample count — resetting to 0 would corrupt
        // A/V sync.
        let mut p = AdtsParser::new();
        p.parse(&make_pes(adts_frame(400), Some(90000)));
        let f = p.parse(&make_pes(adts_frame(400), None));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000) + 1024 * 1_000_000_000 / 44100,
            "continuation advances by one AAC frame"
        );
    }

    // A dropped frame's header failed validation, so duration is reported as
    // zero rather than guessed from untrustworthy fields. See docs/adts.md.
    #[test]
    fn dropped_frames_are_counted_but_their_duration_is_not_invented() {
        let mut parser = AdtsParser::new();
        // Three frames whose sampling_frequency_index is a reserved value (13),
        // so `adts_verdict` rejects each one.
        let mut bad = adts_frame(32);
        bad[2] = (bad[2] & 0b1100_0011) | (13 << 2);
        for i in 0..3 {
            let out = parser.parse(&make_pes(bad.clone(), Some(i * 90_000)));
            assert!(out.is_empty(), "an invalid ADTS frame is not emitted");
        }
        assert_eq!(parser.dropped_frames(), 3, "every drop is counted");
        assert_eq!(
            parser.dropped_duration_ns(),
            0,
            "the duration comes from the header that just failed validation, so \
             it is reported as unmeasured rather than guessed"
        );
    }

    #[test]
    fn reserved_sample_rate_index_is_dropped() {
        // sr_index = 13 (reserved). byte2 bits5..2 = 1101 → 0x34.
        let mut p = AdtsParser::new();
        let mut f = adts_frame(400);
        f[2] = (f[2] & 0xC3) | (13 << 2); // set sr_index = 13
        assert!(p.parse(&make_pes(f, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn subheader_frame_length_is_dropped() {
        // frame_length < 7 (here 0) is a sub-header length → reject.
        let mut p = AdtsParser::new();
        let mut f = adts_frame(400);
        f[3] &= 0xFC; // clear len high bits
        f[4] = 0;
        f[5] &= 0x1F; // clear len low bits → frame_length = 0
        assert!(p.parse(&make_pes(f, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn raw_aac_without_sync_passes_through() {
        // No ADTS sync (e.g. raw AAC from mp4) → cannot validate → keep.
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(
            vec![0x21, 0x00, 0x03, 0x40, 0x00, 0x00, 0x00],
            Some(0),
        ));
        assert_eq!(f.len(), 1);
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn drop_preserves_sync_via_own_pts() {
        let mut p = AdtsParser::new();
        let mut bad = adts_frame(400);
        bad[2] = (bad[2] & 0xC3) | (14 << 2); // reserved sr_index
        assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
        let f = p.parse(&make_pes(adts_frame(400), Some(96000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(96000),
            "next frame keeps its own PTS"
        );
    }

    #[test]
    fn short_sync_header_waits_for_continuation() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(vec![0xFF, 0xF1, 0x50], Some(0)));
        assert!(f.is_empty(), "partial ADTS header must be buffered");
    }

    /// One PES is one unit here, so the frame carries that packet's offset.
    #[test]
    fn a_frame_carries_its_packets_source() {
        let mut parser = AdtsParser::new();
        let mut p = make_pes(adts_frame(64), Some(90_000));
        p.source = Some(crate::pes::SourcePos::at_byte(4_242));
        let frames = parser.parse(&p);
        assert!(!frames.is_empty(), "a valid ADTS frame is emitted");
        assert_eq!(frames[0].source.map(|s| s.byte), Some(4_242));
    }
    #[test]
    fn every_pes_split_reassembles_and_strips_adts() {
        let data = adts_frame(40);
        for split in 1..data.len() {
            let mut p = AdtsParser::new();
            assert!(
                p.parse(&make_pes(data[..split].to_vec(), Some(90000)))
                    .is_empty()
            );
            let frames = p.parse(&make_pes(data[split..].to_vec(), None));
            assert_eq!(frames.len(), 1, "split {split}");
            assert_eq!(frames[0].data, data[7..]);
            assert_eq!(frames[0].pts_ns, 1_000_000_000);
            assert!(p.flush().is_empty());
            assert_eq!(p.dropped_frames(), 0);
        }
    }

    #[test]
    fn multiple_adts_frames_in_one_pes_have_sample_timestamps() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        let frames = p.parse(&make_pes(data.repeat(3), Some(90000)));
        assert_eq!(frames.len(), 3);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(
                f.pts_ns,
                1_000_000_000 + i as i64 * (1024 * 1_000_000_000i64 / 44100)
            );
            assert_eq!(f.data, data[7..]);
        }
        assert_eq!(p.codec_private(), Some(vec![0x12, 0x10]));
    }

    #[test]
    fn crc_bytes_are_removed_with_transport_header() {
        let mut data = adts_frame(20);
        data[1] &= !1;
        let mut p = AdtsParser::new();
        let frames = p.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(frames[0].data, data[9..]);
    }

    #[test]
    fn partial_tail_is_not_emitted_at_eof() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        assert!(p.parse(&make_pes(data[..10].to_vec(), Some(0))).is_empty());
        assert!(p.flush().is_empty());
    }

    #[test]
    fn discontinuity_drops_partial_frame_and_reanchors() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(data[..10].to_vec(), Some(0)));
        let mut fresh = make_pes(data.clone(), Some(180000));
        fresh.discontinuity = true;
        fresh.source = Some(crate::pes::SourcePos::at_byte(4096));
        let frames = p.parse(&fresh);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
        assert_eq!(frames[0].source, fresh.source);
        assert!(frames[0].discontinuity);
    }
}

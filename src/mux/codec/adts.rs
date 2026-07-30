//! AAC ADTS decodability gate.
//!
//! Per the ADTS framing defined in ISO/IEC 13818-7 / ISO/IEC 14496-3, a header
//! is structurally invalid in exactly three ways this gate treats as hard
//! rejects: syncword != 0xFFF, a reserved `sampling_frequency_index` (the sample
//! rate table has 13 valid entries, so index ≥ 13 is reserved), and
//! `aac_frame_length < 7` (shorter than the fixed+variable header itself). The
//! optional 16-bit ADTS CRC is not verified here — it is simply skipped. So the
//! gate enforces those three rejects: a packet that begins with the ADTS sync
//! but is otherwise malformed is dropped; a packet with no ADTS sync is raw AAC
//! (e.g. from an MP4 container, which carries no ADTS header) or a continuation
//! and passes through unchanged — never false-dropped. Raw AAC has no per-frame
//! integrity data, so like LPCM it cannot be gated.

use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

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
    if frame_length < 7 {
        return AdtsVerdict::Invalid;
    }
    AdtsVerdict::Valid
}

pub struct AdtsParser {
    tally: DropTally,
    /// Last emitted PTS (ns). A PES with no PTS (legal for audio, e.g. a
    /// post-discontinuity continuation) carries this forward rather than resetting
    /// the timeline to 0 — matching the AC-3/DTS parsers and preserving A/V sync.
    last_pts_ns: i64,
}

impl Default for AdtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl AdtsParser {
    pub fn new() -> Self {
        Self {
            tally: DropTally::new("aac"),
            last_pts_ns: 0,
        }
    }

    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }
}

impl CodecParser for AdtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes
            .pts
            .or(pes.dts)
            .map(pts_to_ns)
            .unwrap_or(self.last_pts_ns);
        self.last_pts_ns = pts_ns;

        let drop =
            self.tally.is_poisoned() || matches!(adts_verdict(&pes.data), AdtsVerdict::Invalid);
        if drop {
            let reason = if self.tally.is_poisoned() {
                "track-poisoned"
            } else {
                "header"
            };
            self.tally.record_drop(pts_ns, 0, pes.data.len(), reason);
            return Vec::new();
        }

        self.tally.record_kept();
        vec![Frame {
            discontinuity: pes.discontinuity,
            coding: None,
            source: None,
            pts_ns,
            keyframe: true,
            data: pes.data.clone(),
            duration_ns: None,
        }]
    }

    fn flush(&mut self) -> Vec<Frame> {
        self.tally.log_summary();
        Vec::new()
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
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
    fn pes_without_pts_carries_last_timestamp_not_zero() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
        // carry the last known timestamp forward — resetting to 0 would corrupt
        // A/V sync.
        let mut p = AdtsParser::new();
        p.parse(&make_pes(adts_frame(400), Some(90000)));
        let f = p.parse(&make_pes(adts_frame(400), None));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000),
            "carried forward, not reset to 0"
        );
    }

    /// A dropped ADTS frame is dropped BECAUSE its header failed validation, so
    /// the very fields a duration would come from (sampling_frequency_index, and
    /// the 1024-samples-per-AAC-frame constant applied to it) are the ones known
    /// to be untrustworthy. This gate therefore reports the drop's duration as
    /// zero rather than deriving a number from a header it has just rejected —
    /// the honest answer, and the one the count alongside it must be read with.
    /// A nonzero constant here would report silence that was never measured.
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
    fn short_packet_passes_through() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(vec![0xFF, 0xF1, 0x50], Some(0)));
        assert_eq!(f.len(), 1, "too short to validate → kept");
    }
}

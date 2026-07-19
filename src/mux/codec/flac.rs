//! FLAC elementary-stream decodability gate.
//!
//! FLAC frames carry no length field, so a raw stream is delimited only by
//! sync-scanning + CRC validation. In freemkv, though, FLAC never arrives raw:
//! it comes from mp4/mkv, where each packet is exactly one container-delimited
//! FLAC frame (the `PARSER_FLAG_COMPLETE_FRAMES` case in ffmpeg). So this parser
//! is a per-packet gate, not a framer: every FLAC frame ends with a 16-bit CRC
//! (poly 0x8005) computed so the residue over the whole frame is zero
//! (ffmpeg `flac_decode_frame`, `av_crc(AV_CRC_16_ANSI, 0, buf, len) == 0`). A
//! nonzero residue is definitive corruption → drop the frame (a silence gap,
//! never a shift — each packet keeps its own PTS), logged via the shared tally.
//!
//! A packet that does not begin with the FLAC frame sync is not a delimited
//! frame we can validate, so it is passed through unchanged (never false-dropped).

use super::crc::crc16_ansi;
use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// FLAC frame sync: 14-bit code `0x3FFE` + a mandatory-0 reserved bit; the next
/// bit (blocking strategy) is masked off. ffmpeg tests `(AV_RB16 & 0xFFFE) ==
/// 0xFFF8` (flac_parser.c).
fn has_flac_sync(data: &[u8]) -> bool {
    data.len() >= 2 && ((u16::from(data[0]) << 8 | u16::from(data[1])) & 0xFFFE) == 0xFFF8
}

/// Block-size code → samples, `ff_flac_blocksize_table` (0 = reserved/explicit).
const FLAC_BLOCKSIZE_TABLE: [u32; 16] = [
    0, 192, 576, 1152, 2304, 4608, 0, 0, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
];
/// Sample-rate code → Hz, `ff_flac_sample_rate_table` (0 = STREAMINFO/explicit).
const FLAC_SAMPLE_RATE_TABLE: [u32; 16] = [
    0, 88_200, 176_400, 192_000, 8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000, 96_000, 0,
    0, 0, 0,
];

/// Best-effort duration (ns) of a FLAC frame from its header block-size and
/// sample-rate codes (byte 2). Only the table-coded cases are resolved; the
/// explicit-in-trailing-bytes codes (block 6/7, rate 12/13/14) and
/// STREAMINFO-derived (code 0) return `None`. Used only for the dropped-audio
/// accounting, so a `None` (→ 0) is harmless.
fn flac_frame_duration_ns(frame: &[u8]) -> Option<i64> {
    if frame.len() < 3 {
        return None;
    }
    let bs_code = (frame[2] >> 4) & 0x0F;
    let sr_code = frame[2] & 0x0F;
    let blocksize = FLAC_BLOCKSIZE_TABLE[bs_code as usize];
    let rate = FLAC_SAMPLE_RATE_TABLE[sr_code as usize];
    if blocksize == 0 || rate == 0 {
        return None;
    }
    Some((blocksize as i64 * 1_000_000_000 + rate as i64 / 2) / rate as i64)
}

pub struct FlacParser {
    tally: DropTally,
}

impl Default for FlacParser {
    fn default() -> Self {
        Self::new()
    }
}

impl FlacParser {
    pub fn new() -> Self {
        Self {
            tally: DropTally::new("flac"),
        }
    }

    /// Access units dropped as undecodable so far.
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    /// Total decoded duration (ns) of dropped access units.
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }
}

impl CodecParser for FlacParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);

        // Gate: a packet that begins with a FLAC frame sync but whose whole-frame
        // CRC-16 residue is nonzero is corrupt → drop. Anything else passes
        // through (a non-sync packet is not a frame we can validate; a poisoned
        // track drops everything).
        let corrupt = has_flac_sync(&pes.data) && crc16_ansi(&pes.data) != 0;
        if self.tally.is_poisoned() || corrupt {
            let reason = if self.tally.is_poisoned() {
                "track-poisoned"
            } else {
                "crc"
            };
            let dur = flac_frame_duration_ns(&pes.data).unwrap_or(0);
            self.tally.record_drop(pts_ns, dur, pes.data.len(), reason);
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

    /// A minimal FLAC-frame-shaped buffer: sync `0xFFF8`, a plausible header
    /// (block code 1 = 192 samples, rate code 9 = 44.1 kHz), some payload, and a
    /// trailing CRC-16 so the whole-frame residue is zero (a valid frame).
    fn make_flac_frame(payload_len: usize) -> Vec<u8> {
        let mut f = vec![0u8; 6 + payload_len + 2];
        f[0] = 0xFF;
        f[1] = 0xF8; // sync + fixed blocksize
        f[2] = (1 << 4) | 9; // bs_code=1 (192), sr_code=9 (44100)
        // bytes 3..end-2 arbitrary; last two bytes carry the CRC-16.
        let n = f.len();
        let c = crc16_ansi(&f[..n - 2]);
        f[n - 2] = (c >> 8) as u8;
        f[n - 1] = (c & 0xFF) as u8;
        assert_eq!(crc16_ansi(&f), 0, "finalized frame has zero residue");
        f
    }

    #[test]
    fn valid_frame_is_kept() {
        let mut p = FlacParser::new();
        let f = p.parse(&make_pes(make_flac_frame(100), Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000));
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn corrupt_frame_is_dropped() {
        let mut p = FlacParser::new();
        let mut frame = make_flac_frame(100);
        frame[20] ^= 0xFF; // corrupt a payload byte → CRC residue nonzero
        assert!(crc16_ansi(&frame) != 0);
        let f = p.parse(&make_pes(frame, Some(90000)));
        assert!(f.is_empty(), "corrupt FLAC frame dropped");
        assert_eq!(p.dropped_frames(), 1);
        // 192 samples @ 44.1 kHz ≈ 4.354 ms of silence accounted.
        assert_eq!(
            p.dropped_duration_ns(),
            (192u64 * 1_000_000_000 + 44_100 / 2) / 44_100
        );
    }

    #[test]
    fn corrupt_drop_preserves_sync_via_own_pts() {
        // Each packet carries its own PTS, so dropping one leaves the next frame
        // on its true timeline — a gap, not a shift.
        let mut p = FlacParser::new();
        let mut bad = make_flac_frame(100);
        bad[20] ^= 0xFF;
        assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
        let f = p.parse(&make_pes(make_flac_frame(100), Some(96000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(96000),
            "surviving frame keeps its own container PTS — the drop is a gap"
        );
    }

    #[test]
    fn non_flac_packet_passes_through() {
        // A packet without the FLAC sync isn't a frame we can validate — never
        // false-drop it.
        let mut p = FlacParser::new();
        let f = p.parse(&make_pes(vec![0x00, 0x01, 0x02, 0x03], Some(0)));
        assert_eq!(f.len(), 1, "unrecognized packet passed through");
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn empty_pes_emits_nothing() {
        let mut p = FlacParser::new();
        assert!(p.parse(&make_pes(Vec::new(), Some(0))).is_empty());
    }
}

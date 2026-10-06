//! FLAC elementary-stream decodability gate.
//!
//! Per-packet gate, not a framer: each PES packet is one container-delimited
//! FLAC frame. Every frame ends with a 16-bit CRC (poly 0x8005, init 0,
//! non-reflected) over the whole frame including the footer; a valid frame
//! has zero residue (RFC 9639, frame footer). Nonzero residue → corruption:
//! drop the frame (silence gap, PTS preserved), logged via the shared tally.
//! A packet without the FLAC sync passes through unchanged (never false-dropped).

use super::crc::crc16_ansi;
use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// FLAC frame sync: 14-bit code `0x3FFE` + a mandatory-0 reserved bit; the next
/// bit (blocking strategy) is masked off. Test the top 15 bits of the first two
/// bytes: `(be16 & 0xFFFE) == 0xFFF8` (per RFC 9639, frame header).
fn has_flac_sync(data: &[u8]) -> bool {
    data.len() >= 2 && ((u16::from(data[0]) << 8 | u16::from(data[1])) & 0xFFFE) == 0xFFF8
}

/// Block-size code → samples (RFC 9639 block-size table; 0 = reserved/explicit).
const FLAC_BLOCKSIZE_TABLE: [u32; 16] = [
    0, 192, 576, 1152, 2304, 4608, 0, 0, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
];
/// Sample-rate code → Hz (RFC 9639 sample-rate table; 0 = STREAMINFO/explicit).
const FLAC_SAMPLE_RATE_TABLE: [u32; 16] = [
    0, 88_200, 176_400, 192_000, 8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000, 96_000, 0,
    0, 0, 0,
];

// Best-effort duration (ns) from header block-size/sample-rate codes; the
// explicit-in-trailing-bytes and STREAMINFO-derived codes return None. Used
// only for dropped-audio accounting, so a None (→ 0) is harmless.
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
    /// Last emitted PTS (ns), carried forward across a PES with no PTS rather than
    /// resetting the timeline to 0 (see the AC-3/DTS parsers) — preserves A/V sync.
    last_pts_ns: i64,
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
            last_pts_ns: 0,
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
        let pts_ns = pes
            .pts
            .or(pes.dts)
            .map(pts_to_ns)
            .unwrap_or(self.last_pts_ns);
        self.last_pts_ns = pts_ns;

        // Gate: a packet with a FLAC frame sync but nonzero whole-frame CRC-16
        // residue is corrupt → drop. Non-sync packets pass through unvalidated;
        // a poisoned track drops everything.
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
            source: super::pesbuf::PesFacts::of(pes).source,
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
#[path = "flac_tests.rs"]
mod tests;

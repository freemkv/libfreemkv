//! MPEG-1/2/2.5 audio (MP1/MP2/MP3) decodability gate.
//!
//! ffmpeg validates MPEG-audio frames by header sanity + framing resync, not a
//! payload CRC (`mpegaudiodecheader.c` `ff_mpa_check_header`; the optional CRC
//! covers only side-info and is off by default). Its `ff_mpa_decode_header`
//! additionally rejects free-format (`bitrate_index == 0`). So the gate mirrors
//! exactly those header rejects: a packet that begins with the 11-bit MPEG-audio
//! sync but whose version / layer / bitrate-index / sample-rate fields are the
//! reserved/invalid values is undecodable → drop it (a silence gap; each packet
//! keeps its own PTS). A packet with no leading sync is not a frame we can
//! validate (raw payload / continuation), so it passes through unchanged —
//! never false-dropped.

use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Decoded validity of a candidate MPEG-audio header.
enum MpaVerdict {
    /// No 11-bit sync at the packet head — not a frame we can validate.
    NoSync,
    /// Sync present and every field is legal — decodable.
    Valid,
    /// Sync present but a field is reserved/invalid (or free-format) — ffmpeg's
    /// parser rejects this exactly.
    Invalid,
}

/// Mirror ffmpeg's `ff_mpa_check_header` + the `ff_mpa_decode_header`
/// free-format reject. A dropped MPEG-audio frame has a corrupt header, so no
/// duration is computed (the fields it would come from are the invalid ones).
fn mpa_verdict(data: &[u8]) -> MpaVerdict {
    if data.len() < 4 {
        return MpaVerdict::NoSync;
    }
    let h = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    // 11-bit sync (0x7FF at the top).
    if (h & 0xffe0_0000) != 0xffe0_0000 {
        return MpaVerdict::NoSync;
    }
    // ff_mpa_check_header rejects: version field 01, layer field 00,
    // bitrate_index 15, sample-rate field 3.
    if (h & (3 << 19)) == (1 << 19)
        || (h & (3 << 17)) == 0
        || (h & (0xf << 12)) == (0xf << 12)
        || (h & (3 << 10)) == (3 << 10)
    {
        return MpaVerdict::Invalid;
    }
    // NOTE: bitrate_index == 0 (free format) is NOT rejected. It is a legal,
    // decodable MPEG-audio mode (ffmpeg's ff_mpa_check_header accepts it and the
    // decoder derives the frame size from the sync spacing). Dropping it would be
    // a false positive on a clean stream, so it passes the gate.
    MpaVerdict::Valid
}

pub struct MpegAudioParser {
    tally: DropTally,
}

impl Default for MpegAudioParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MpegAudioParser {
    pub fn new() -> Self {
        Self {
            tally: DropTally::new("mpegaudio"),
        }
    }

    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }
}

impl CodecParser for MpegAudioParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.or(pes.dts).map(pts_to_ns).unwrap_or(0);

        let drop =
            self.tally.is_poisoned() || matches!(mpa_verdict(&pes.data), MpaVerdict::Invalid);
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

    /// A valid MPEG-1 Layer III header: sync 0xFFF, version MPEG-1 (11), layer
    /// III (01), bitrate_index 9, sample-rate 0 (44.1 kHz), no CRC. Bytes:
    /// 0xFF 0xFB 0x90 0x00 — the canonical MP3 frame header.
    fn mp3_frame(payload: usize) -> Vec<u8> {
        let mut f = vec![0xFF, 0xFB, 0x90, 0x00];
        f.extend(std::iter::repeat(0xAA).take(payload));
        f
    }

    #[test]
    fn valid_header_is_kept() {
        let mut p = MpegAudioParser::new();
        let f = p.parse(&make_pes(mp3_frame(400), Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000));
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn reserved_sample_rate_is_dropped() {
        // Sync present but sample-rate field = 3 (reserved) → ffmpeg rejects.
        // 0xFF 0xFB then byte2 with bits 11..10 = 11: 0x9C.
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame(400);
        frame[2] = 0x9C; // freq field = 3
        let f = p.parse(&make_pes(frame, Some(90000)));
        assert!(f.is_empty(), "reserved sample rate dropped");
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn reserved_layer_is_dropped() {
        // Layer field 00 (reserved). byte1 bits 2..1 = 00 → 0xF9 keeps sync
        // (0xFFF needs byte1 top 3 bits set) and sets layer=00.
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame(400);
        frame[1] = 0xF9; // 1111_1001: sync ok (top 3 =111), version 11, layer 00
        let f = p.parse(&make_pes(frame, Some(0)));
        assert!(f.is_empty(), "reserved layer dropped");
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn bad_bitrate_index_15_is_dropped() {
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame(400);
        frame[2] = 0xF0; // bitrate_index = 1111
        assert!(p.parse(&make_pes(frame, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn free_format_bitrate_zero_is_kept() {
        // Free format (bitrate_index == 0) is legal and decodable — it must NOT
        // be dropped (that would be a false positive on a clean stream).
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame(400);
        frame[2] = 0x00; // bitrate_index = 0000 (free format); sync/layer/rate ok
        let f = p.parse(&make_pes(frame, Some(0)));
        assert_eq!(f.len(), 1, "free-format frame kept");
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn non_sync_packet_passes_through() {
        // No 11-bit sync → not a validatable frame → keep (conservative).
        let mut p = MpegAudioParser::new();
        let f = p.parse(&make_pes(vec![0x00, 0x11, 0x22, 0x33, 0x44], Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn drop_preserves_sync_via_own_pts() {
        let mut p = MpegAudioParser::new();
        let mut bad = mp3_frame(400);
        bad[2] = 0x9C; // reserved sample rate
        assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
        let f = p.parse(&make_pes(mp3_frame(400), Some(96000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(96000),
            "next frame keeps its own PTS"
        );
    }
}

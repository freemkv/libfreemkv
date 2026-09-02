//! MPEG-1/2/2.5 audio (MP1/MP2/MP3) decodability gate.
//!
//! Validates header sanity + framing resync per ISO/IEC 11172-3 / ISO/IEC
//! 13818-3, not a payload CRC. ACCEPTS free-format (`bitrate_index == 0`) as a
//! legal decodable mode (see `mpa_verdict`'s doc). Rejects only truly invalid
//! headers (bad version / layer / sample-rate / reserved bitrate index 15) →
//! dropped as a silence gap, each packet keeping its own PTS. A packet with no
//! leading sync passes through unchanged — never false-dropped.
//!
//! See docs/mpegaudio.md for the CRC and free-format-reject rationale.

use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Decoded validity of a candidate MPEG-audio header.
enum MpaVerdict {
    /// No 11-bit sync at the packet head — not a frame we can validate.
    NoSync,
    /// Sync present and every field is legal — decodable.
    Valid,
    /// Sync present but a field is reserved/invalid — a conformant header parser
    /// rejects this exactly.
    Invalid,
}

// Header-only check (ISO/IEC 11172-3 / 13818-3); ACCEPTS free-format
// (bitrate_index == 0), NOT the stricter reject a full decoder applies. A
// dropped frame's header is corrupt, so no duration is computed.
fn mpa_verdict(data: &[u8]) -> MpaVerdict {
    if data.len() < 4 {
        return MpaVerdict::NoSync;
    }
    let h = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    // 11-bit sync (0x7FF at the top).
    if (h & 0xffe0_0000) != 0xffe0_0000 {
        return MpaVerdict::NoSync;
    }
    // Reject per spec: version field 01, layer field 00, bitrate_index 15,
    // sample-rate field 3.
    if (h & (3 << 19)) == (1 << 19)
        || (h & (3 << 17)) == 0
        || (h & (0xf << 12)) == (0xf << 12)
        || (h & (3 << 10)) == (3 << 10)
    {
        return MpaVerdict::Invalid;
    }
    // bitrate_index == 0 (free format) is NOT rejected: it's a legal, decodable
    // mode (decoder derives frame size from sync spacing); rejecting it would
    // be a false positive on a clean stream.
    MpaVerdict::Valid
}

pub struct MpegAudioParser {
    tally: DropTally,
    /// Last emitted PTS (ns), carried forward across a PES with no PTS rather than
    /// resetting the timeline to 0 (see the AC-3/DTS parsers) — preserves A/V sync.
    last_pts_ns: i64,
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

impl CodecParser for MpegAudioParser {
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
        f.extend(std::iter::repeat_n(0xAA, payload));
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
    fn pes_without_pts_carries_last_timestamp_not_zero() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
        // carry the last known timestamp forward — resetting to 0 would corrupt
        // A/V sync. Mirrors the adts.rs guard test.
        let mut p = MpegAudioParser::new();
        p.parse(&make_pes(mp3_frame(400), Some(90000)));
        let f = p.parse(&make_pes(mp3_frame(400), None));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000),
            "carried forward, not reset to 0"
        );
    }

    // Same contract as ADTS: header is invalid → frame duration (which would
    // come from the header) is unavailable, so drop duration is reported as
    // zero, not derived/guessed.
    #[test]
    fn dropped_frames_are_counted_but_their_duration_is_not_invented() {
        let mut parser = MpegAudioParser::new();
        // Reserved layer field (00) — rejected per ISO/IEC 11172-3.
        let mut bad = mp3_frame(32);
        bad[1] &= !0b0000_0110;
        for i in 0..3 {
            let out = parser.parse(&make_pes(bad.clone(), Some(i * 90_000)));
            assert!(out.is_empty(), "an invalid MPEG-audio frame is not emitted");
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
    fn reserved_version_field_is_dropped() {
        // version field = 01 (reserved) → rejected. byte1 = 111_01_01_1 = 0xEB
        // keeps the 11-bit sync (0xFF + top 3 bits 111) but sets version bits to 01.
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame(400);
        frame[1] = 0xEB;
        let f = p.parse(&make_pes(frame, Some(90000)));
        assert!(f.is_empty(), "reserved version dropped");
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn reserved_sample_rate_is_dropped() {
        // Sync present but sample-rate field = 3 (reserved) → rejected per spec.
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

    // Self-framing at PES granularity: parse emits/drops immediately, buffers
    // nothing, so end-of-stream has nothing left to hand over.
    // See docs/mpegaudio.md — flush-no-phantom-frame rationale.
    #[test]
    fn flush_adds_no_phantom_frame_after_the_last_real_packet() {
        let mut p = MpegAudioParser::new();
        let mut emitted = Vec::new();
        emitted.extend(p.parse(&make_pes(mp3_frame(400), Some(90_000))));
        emitted.extend(p.parse(&make_pes(mp3_frame(400), Some(180_000))));
        // An invalid header (version field 01 = reserved) is dropped, not buffered.
        emitted.extend(p.parse(&make_pes(vec![0xFF, 0xEB, 0x90, 0x00, 0xAA], Some(270_000))));
        assert_eq!(emitted.len(), 2, "two valid packets out, one dropped");
        assert_eq!(p.dropped_frames(), 1);

        let tail = p.flush();
        assert!(
            tail.is_empty(),
            "nothing is buffered past the last packet; flush produced {:?}",
            tail.iter()
                .map(|f| (f.pts_ns, f.data.len()))
                .collect::<Vec<_>>()
        );
        // Total frame count over the whole stream equals the valid input count —
        // a manufactured tail frame would break this even if it were non-empty.
        assert_eq!(emitted.len() + tail.len(), 2);
    }

    // The `codec/mod.rs` text guard can't see `source: facts.source`; only a
    // runtime check proves an emitted frame carries the byte it was read from
    // (needed for multi-clip placement by byte, not timestamp inference).
    #[test]
    fn an_emitted_frame_carries_the_packets_source_offset() {
        let mut p = MpegAudioParser::new();
        let mut pes = make_pes(mp3_frame(400), Some(90_000));
        pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
        let f = p.parse(&pes);
        assert!(!f.is_empty(), "the frame is emitted");
        assert_eq!(f[0].source.map(|s| s.byte), Some(7_777));
    }
}

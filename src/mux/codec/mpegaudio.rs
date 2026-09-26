//! MPEG audio framing and validation.

use super::audio_frames::{AudioFrames, Header};
#[cfg(test)]
use super::pts_to_ns;
use super::{CodecParser, Frame, PesPacket};

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

// ISO/IEC 11172-3 and 13818-3 header bitrate tables, in kbit/s.
const MPEG1_L1: [u32; 15] = [
    0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448,
];
const MPEG1_L2: [u32; 15] = [
    0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
];
const MPEG1_L3: [u32; 15] = [
    0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
];
const MPEG2_L1: [u32; 15] = [
    0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256,
];
const MPEG2_L23: [u32; 15] = [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160];

fn frame_header(data: &[u8]) -> Option<Header> {
    if !matches!(mpa_verdict(data), MpaVerdict::Valid) {
        return None;
    }
    let version = (data[1] >> 3) & 3;
    let layer = (data[1] >> 1) & 3;
    let rate = [44100, 48000, 32000][usize::from((data[2] >> 2) & 3)]
        >> match version {
            3 => 0,
            2 => 1,
            _ => 2,
        };
    let rates = match (version == 3, layer) {
        (true, 3) => MPEG1_L1,
        (true, 2) => MPEG1_L2,
        (true, _) => MPEG1_L3,
        (false, 3) => MPEG2_L1,
        (false, _) => MPEG2_L23,
    };
    let bitrate = rates[usize::from(data[2] >> 4)] * 1000;
    let padding = u32::from((data[2] >> 1) & 1);
    let samples = match layer {
        3 => 384,
        1 if version != 3 => 576,
        _ => 1152,
    };
    let bytes = if bitrate == 0 {
        // Free-format has no signalled size; retain PES-granular passthrough.
        data.len()
    } else if layer == 3 {
        ((12 * bitrate / rate + padding) * 4) as usize
    } else {
        ((samples / 8) * bitrate / rate + padding) as usize
    };
    Some(Header {
        bytes,
        skip: 0,
        samples,
        rate,
    })
}

pub struct MpegAudioParser {
    frames: AudioFrames,
}
impl Default for MpegAudioParser {
    fn default() -> Self {
        Self::new()
    }
}
impl MpegAudioParser {
    pub fn new() -> Self {
        Self {
            frames: AudioFrames::new("mpegaudio"),
        }
    }
    pub fn dropped_frames(&self) -> u64 {
        self.frames.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.frames.dropped_duration_ns()
    }
}
impl CodecParser for MpegAudioParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        self.frames.parse(pes, 4, frame_header)
    }
    fn flush(&mut self) -> Vec<Frame> {
        self.frames.flush()
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
    fn mp3_frame(_payload: usize) -> Vec<u8> {
        let mut f = vec![0xFF, 0xFB, 0x90, 0x00];
        f.extend(std::iter::repeat_n(0xAA, 413));
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
    fn pes_without_pts_advances_by_sample_count() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
        // advance from the last timestamp by sample count — resetting to 0 would corrupt
        // A/V sync. Mirrors the adts.rs guard test.
        let mut p = MpegAudioParser::new();
        p.parse(&make_pes(mp3_frame(400), Some(90000)));
        let f = p.parse(&make_pes(mp3_frame(400), None));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000) + 1152 * 1_000_000_000 / 44100,
            "continuation advances by one MPEG audio frame"
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

    // Complete frames have already been emitted; EOF must not turn a trailing invalid header or
    // partial frame into another access unit.
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
    #[test]
    fn every_pes_split_reassembles_a_complete_mpeg_frame() {
        let data = mp3_frame(413);
        for split in 1..data.len() {
            let mut p = MpegAudioParser::new();
            assert!(
                p.parse(&make_pes(data[..split].to_vec(), Some(90000)))
                    .is_empty()
            );
            let frames = p.parse(&make_pes(data[split..].to_vec(), None));
            assert_eq!(frames.len(), 1, "split {split}");
            assert_eq!(frames[0].data, data);
            assert_eq!(frames[0].pts_ns, 1_000_000_000);
            assert_eq!(p.dropped_frames(), 0);
        }
    }

    #[test]
    fn multiple_mpeg_frames_in_one_pes_get_distinct_timestamps() {
        let data = mp3_frame(413);
        let mut p = MpegAudioParser::new();
        let frames = p.parse(&make_pes(data.repeat(3), Some(0)));
        assert_eq!(frames.len(), 3);
        for (i, frame) in frames.iter().enumerate() {
            assert_eq!(frame.data, data);
            assert_eq!(frame.pts_ns, i as i64 * (1152 * 1_000_000_000i64 / 44100));
        }
    }

    #[test]
    fn frame_sizes_cover_versions_layers_and_padding() {
        for (header, bytes, samples, rate) in [
            ([0xff, 0xfb, 0x90, 0], 417, 1152, 44100),
            ([0xff, 0xfb, 0x92, 0], 418, 1152, 44100),
            ([0xff, 0xfd, 0xa4, 0], 576, 1152, 48000),
            ([0xff, 0xff, 0x90, 0], 312, 384, 44100),
            ([0xff, 0xf3, 0x80, 0], 208, 576, 22050),
            ([0xff, 0xe3, 0x80, 0], 417, 576, 11025),
        ] {
            let h = frame_header(&header).unwrap();
            assert_eq!((h.bytes, h.samples, h.rate), (bytes, samples, rate));
        }
    }
}

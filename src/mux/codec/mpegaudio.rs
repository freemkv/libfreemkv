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

// Free-format search ceiling in bit/s: above every table bitrate (max 448 kbit/s), so a
// real free-format frame is always found within it and a false header is given up on.
const MAX_FREE_FORMAT_BITRATE: u32 = 640_000;

// Does a header continuing this free-format stream (same version/layer/bitrate/rate,
// padding and private bits ignored) start at `i`?
fn free_format_next_at(data: &[u8], i: usize) -> bool {
    data.get(i..i + 3)
        .is_some_and(|h| h[0] == 0xFF && h[1] == data[1] && h[2] & 0xFC == data[2] & 0xFC)
}

// Size of the free-format frame at `data[0]` (its size is the spacing to the next header),
// or `bytes > data.len()` to wait. `free_size` is the learned size without padding, trusted
// only while the next header is where it predicts. At `eos`, a lone last frame is emitted.
fn free_format_bytes(
    data: &[u8],
    free_size: &mut Option<usize>,
    pad: usize,
    max: usize,
    eos: bool,
) -> Option<usize> {
    let wait = data.len().saturating_add(1);
    if let Some(n) = *free_size {
        let b = n + pad;
        if free_format_next_at(data, b) {
            return Some(b);
        }
        if data.len() < b + 3 {
            return Some(if eos && data.len() >= b { b } else { wait });
        }
        *free_size = None; // no header where predicted: relearn
    }
    let last = max.min(data.len().saturating_sub(3));
    if let Some(d) = (pad + 5..=last).find(|&i| free_format_next_at(data, i)) {
        *free_size = Some(d - pad);
        return Some(d);
    }
    if data.len() >= max + 3 {
        return None; // no successor within the largest legal frame: not a real header
    }
    Some(if eos && data.len() > pad + 4 {
        data.len()
    } else {
        wait
    })
}

fn frame_header(data: &[u8], free_size: &mut Option<usize>, eos: bool) -> Option<Header> {
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
    let size = |bitrate: u32, padding: u32| -> usize {
        if layer == 3 {
            ((12 * bitrate / rate + padding) * 4) as usize
        } else {
            ((samples / 8) * bitrate / rate + padding) as usize
        }
    };
    let bytes = if bitrate == 0 {
        let pad = size(0, padding);
        let max = size(MAX_FREE_FORMAT_BITRATE, 1);
        free_format_bytes(data, free_size, pad, max, eos)?
    } else {
        size(bitrate, padding)
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
    // Free-format frame size (without padding), learned from the first sync spacing.
    free_size: Option<usize>,
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
            free_size: None,
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
        if pes.discontinuity {
            self.free_size = None;
        }
        let free_size = &mut self.free_size;
        self.frames
            .parse(pes, 4, |d| frame_header(d, free_size, false))
    }
    fn flush(&mut self) -> Vec<Frame> {
        let free_size = &mut self.free_size;
        self.frames
            .flush_with(4, |d| frame_header(d, free_size, true))
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
    /// 0xFF 0xFB 0x90 0x00 — the canonical MP3 frame header. The header fixes the
    /// frame at 417 bytes (128 kbit/s, 44.1 kHz), so there is no size argument.
    const MP3_FRAME_BYTES: usize = 417;
    fn mp3_frame() -> Vec<u8> {
        let mut f = vec![0xFF, 0xFB, 0x90, 0x00];
        f.resize(MP3_FRAME_BYTES, 0xAA);
        f
    }

    #[test]
    fn mp3_frame_fixture_is_exactly_one_frame() {
        let f = mp3_frame();
        assert_eq!(
            frame_header(&f, &mut None, false).map(|h| h.bytes),
            Some(f.len())
        );
    }

    #[test]
    fn valid_header_is_kept() {
        let mut p = MpegAudioParser::new();
        let f = p.parse(&make_pes(mp3_frame(), Some(90000)));
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
        p.parse(&make_pes(mp3_frame(), Some(90000)));
        let f = p.parse(&make_pes(mp3_frame(), None));
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
        let mut bad = mp3_frame();
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
        let mut frame = mp3_frame();
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
        let mut frame = mp3_frame();
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
        let mut frame = mp3_frame();
        frame[1] = 0xF9; // 1111_1001: sync ok (top 3 =111), version 11, layer 00
        let f = p.parse(&make_pes(frame, Some(0)));
        assert!(f.is_empty(), "reserved layer dropped");
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn bad_bitrate_index_15_is_dropped() {
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame();
        frame[2] = 0xF0; // bitrate_index = 1111
        assert!(p.parse(&make_pes(frame, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn free_format_bitrate_zero_is_kept() {
        // Free format (bitrate_index == 0) is legal and decodable — it must NOT
        // be dropped (that would be a false positive on a clean stream).
        let mut p = MpegAudioParser::new();
        let mut frame = mp3_frame();
        frame[2] = 0x00; // bitrate_index = 0000 (free format); sync/layer/rate ok
        // Its size is the spacing to the next header: the last frame waits for EOS.
        let mut f = p.parse(&make_pes(frame.repeat(2), Some(0)));
        f.extend(p.flush());
        assert_eq!(f.len(), 2, "free-format frames kept");
        assert_eq!(p.dropped_frames(), 0);
    }

    // Free-format frame: bitrate_index 0, 44.1 kHz, no padding, 300 bytes total.
    fn free_frame() -> Vec<u8> {
        let mut f = vec![0xFF, 0xFB, 0x00, 0x00];
        f.resize(300, 0xAA);
        f
    }

    // Free-format size comes from sync spacing, so several frames in one PES each get
    // one frame's duration instead of one AU swallowing the rest of the buffer.
    #[test]
    fn free_format_frames_in_one_pes_are_split_with_distinct_pts() {
        let mut p = MpegAudioParser::new();
        let mut f = p.parse(&make_pes(free_frame().repeat(3), Some(0)));
        assert_eq!(f.len(), 2, "the third awaits a confirming header");
        f.extend(p.flush());
        assert_eq!(f.len(), 3, "EOS emits the known-size third");
        for (i, fr) in f.iter().enumerate() {
            assert_eq!(fr.data, free_frame());
            assert_eq!(fr.pts_ns, i as i64 * (1152 * 1_000_000_000i64 / 44100));
        }
    }

    #[test]
    fn free_format_frame_split_across_pes_reassembles() {
        let data = free_frame().repeat(2);
        for split in [1, 4, 150, 299, 300, 301, 450] {
            let mut p = MpegAudioParser::new();
            let mut f = p.parse(&make_pes(data[..split].to_vec(), Some(0)));
            f.extend(p.parse(&make_pes(data[split..].to_vec(), None)));
            f.extend(p.parse(&make_pes(free_frame(), None)));
            f.extend(p.flush());
            assert_eq!(f.len(), 3, "split {split}");
            assert!(f.iter().all(|fr| fr.data == free_frame()), "split {split}");
        }
    }

    // A false free-format header in a CBR stream never finds a matching successor; the
    // wait is capped at the largest legal free-format frame, so CBR framing resumes.
    #[test]
    fn false_free_format_header_does_not_stall_cbr() {
        let mut p = MpegAudioParser::new();
        let mut first = vec![0xFF, 0xFB, 0x00, 0x00];
        first.extend_from_slice(&mp3_frame());
        let mut n = p.parse(&make_pes(first, Some(0))).len();
        for _ in 0..9 {
            n += p.parse(&make_pes(mp3_frame(), None)).len();
        }
        assert_eq!(
            n, 10,
            "every CBR frame emitted without waiting for the buffer cap"
        );
    }

    // A matching header closer than a minimal frame is not the spacing; keep looking.
    #[test]
    fn too_close_free_format_match_is_skipped() {
        let mut data = vec![0xFF, 0xFB, 0x00, 0x00];
        data.extend_from_slice(&free_frame().repeat(3));
        let f = MpegAudioParser::new().parse(&make_pes(data, Some(0)));
        assert!(f.len() >= 2, "framing proceeds, got {} frames", f.len());
    }

    // The learned size is re-checked against the next header: a new stream after a gap
    // (or a size change) is relearned rather than misframed.
    #[test]
    fn free_format_size_is_relearned() {
        let short = |n: usize| {
            let mut f = free_frame();
            f.truncate(n);
            f
        };
        let mut p = MpegAudioParser::new();
        assert!(
            !p.parse(&make_pes(free_frame().repeat(3), Some(0)))
                .is_empty()
        );
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes(short(200).repeat(3), Some(900_000))
        };
        let f = p.parse(&gap);
        assert!(!f.is_empty());
        assert!(
            f.iter().all(|fr| fr.data.len() == 200),
            "relearned after the gap"
        );
        let mut p = MpegAudioParser::new();
        let mut data = free_frame().repeat(2);
        data.extend_from_slice(&short(200).repeat(3));
        let f = p.parse(&make_pes(data, Some(0)));
        let sizes: Vec<usize> = f.iter().map(|fr| fr.data.len()).collect();
        assert_eq!(sizes, [300, 300, 200, 200], "relearned on a size change");
    }

    // A lone free-format frame has no successor to size it; EOS emits it.
    #[test]
    fn lone_free_format_frame_is_emitted_at_flush() {
        let mut p = MpegAudioParser::new();
        assert!(p.parse(&make_pes(free_frame(), Some(0))).is_empty());
        let f = p.flush();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, free_frame());
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
        let mut bad = mp3_frame();
        bad[2] = 0x9C; // reserved sample rate
        assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
        let f = p.parse(&make_pes(mp3_frame(), Some(96000)));
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
        emitted.extend(p.parse(&make_pes(mp3_frame(), Some(90_000))));
        emitted.extend(p.parse(&make_pes(mp3_frame(), Some(180_000))));
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
        let mut pes = make_pes(mp3_frame(), Some(90_000));
        pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
        let f = p.parse(&pes);
        assert!(!f.is_empty(), "the frame is emitted");
        assert_eq!(f[0].source.map(|s| s.byte), Some(7_777));
    }
    #[test]
    fn every_pes_split_reassembles_a_complete_mpeg_frame() {
        let data = mp3_frame();
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
        let data = mp3_frame();
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
            let h = frame_header(&header, &mut None, false).unwrap();
            assert_eq!((h.bytes, h.samples, h.rate), (bytes, samples, rate));
        }
    }
}

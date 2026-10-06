//! MPEG audio framing and validation.
//!
//! Spec text is quoted from ISO/IEC 11172-3 (the public CD text of §2.4) and ISO/IEC
//! 13818-3:1994 (lower sampling frequencies, ID '0'). The version field value '00' ("MPEG
//! 2.5") is a de facto extension that neither standard defines.

use super::audio_frames::{AudioFrames, Header, SyncSpec};
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
    // [11172-3 §2.4.2.3] syncword: "the bit string '1111 1111 1111'." MPEG 2.5 (not ISO)
    // reuses its last bit as a version bit, so only the first 11 are required here.
    if (h & 0xffe0_0000) != 0xffe0_0000 {
        return MpaVerdict::NoSync;
    }
    // [11172-3 §2.4.2.3] Layer: "00" reserved; sampling_frequency: '11' reserved;
    // [13818-3 §2.4.2.3] bitrate_index "'1111' forbidden"; version '01' is reserved.
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

// [11172-3 §2.4.2.3] bit_rate_index tables (ID '1') and [13818-3 §2.4.2.3] "for ID=0", kbit/s;
// index 0 is "'0000' free format".
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

// Free-format search ceilings in bytes (a practical bound from the frame syntax, not a spec
// limit): Layer I/II never usefully exceed their raw 16-bit stereo PCM (samples x 4); Layer III
// is header + CRC + side info + max part2_3_length for every granule/channel + reservoir.
const MAX_FREE_L1_BYTES: usize = 384 * 4;
const MAX_FREE_L2_BYTES: usize = 1152 * 4;
const MAX_FREE_L3_MPEG1_BYTES: usize = 4 + 2 + 32 + 2 * 2 * 4095 / 8 + 1 + 511;
const MAX_FREE_L3_MPEG2_BYTES: usize = 4 + 2 + 17 + 2 * 4095 / 8 + 1 + 255;

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
        // At EOS the learned size frames the last unit; trailing junk (ID3v1) stays out.
        if free_format_next_at(data, b) || (eos && data.len() >= b) {
            return Some(b);
        }
        if data.len() < b + 3 {
            return Some(wait);
        }
        // No header where predicted: relearn below (the old size stands until replaced).
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

// [11172-3 §2.4.3.1] "N = 12 * bit_rate / sampling_frequency." (Layer I; [§2.1] "In Layer I a
// slot equals four bytes") and "N = 144 * bit_rate / sampling_frequency." (Layers II/III, one
// byte); [§2.4.2.3] padding_bit "'1'": "the frame contains an additional slot".
fn frame_header(data: &[u8], free_size: &mut Option<usize>, eos: bool) -> Option<Header> {
    if !matches!(mpa_verdict(data), MpaVerdict::Valid) {
        return None;
    }
    let version = (data[1] >> 3) & 3;
    let layer = (data[1] >> 1) & 3;
    // [11172-3 §2.4.2.3] sampling_frequency '00' 44.1, '01' 48, '10' 32 kHz; [13818-3] for
    // ID '0': '00' 22,05, '01' 24, '10' 16 kHz.
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
    // [11172-3 §2.4.2.1] frame: "In Layer I it contains information for 384 samples and in Layer
    // II for 1152 samples", "In Layer III ... 1152 samples"; 576 for ID '0' Layer III.
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
        let max = match layer {
            3 => MAX_FREE_L1_BYTES,
            2 => MAX_FREE_L2_BYTES,
            _ if version == 3 => MAX_FREE_L3_MPEG1_BYTES,
            _ => MAX_FREE_L3_MPEG2_BYTES,
        };
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

// Header fields a stream keeps: [11172-3 §2.4.2.3] "To change the layer, a reset of the decoder
// is required." and likewise the sampling rate (and so ID). Mode and bitrate may change.
fn mpa_stream_key(d: &[u8]) -> u32 {
    u32::from(d[1] & 0x1E) << 8 | u32::from(d[2] & 0x0C)
}

// Size of a valid frame at the slice head, with no parser state touched.
pub(super) fn mpa_frame_len(d: &[u8]) -> Option<usize> {
    frame_header(d, &mut None, false).map(|h| h.bytes)
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
            frames: AudioFrames::new(
                "mpegaudio",
                SyncSpec {
                    mask: 0xe0,
                    frame_len: mpa_frame_len,
                    fixed: mpa_stream_key,
                    frame_ns: |d| {
                        let h = frame_header(d, &mut None, false)?;
                        Some(u64::from(h.samples) * 1_000_000_000 / u64::from(h.rate))
                    },
                    // Smallest fixed-rate frame: ID '0' Layer III, 8 kbit/s at 24 kHz, 24 bytes.
                    min_frame: 24,
                },
            ),
            free_size: None,
        }
    }
    /// Access units dropped as undecodable: a lower bound where no later timestamp measured a
    /// corrupt run (at EOS or a gap), which counts the fewest AUs it allows.
    pub fn dropped_frames(&self) -> u64 {
        self.frames.dropped_frames()
    }
    #[cfg(test)]
    pub(crate) fn verified_dropped(&self) -> u64 {
        self.frames.verified_dropped()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.frames.dropped_duration_ns()
    }
}
impl CodecParser for MpegAudioParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let mut out = Vec::new();
        if pes.discontinuity {
            // Emit the complete frame still awaiting its successor before the gap clears it.
            let free_size = &mut self.free_size;
            out = self
                .frames
                .drain_before_gap(4, |d| frame_header(d, free_size, true));
            self.free_size = None;
        }
        let free_size = &mut self.free_size;
        out.extend(
            self.frames
                .parse(pes, 4, |d| frame_header(d, free_size, false)),
        );
        out
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
#[path = "mpegaudio_tests.rs"]
mod tests;

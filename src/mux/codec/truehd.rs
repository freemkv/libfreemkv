//! Dolby TrueHD / Atmos elementary stream parser.
//!
//! BD-TS TrueHD PES packets contain interleaved AC-3 + TrueHD access units.
//! Access units span PES boundaries — must buffer and reassemble.
//!
//! TrueHD access unit header (4 bytes):
//!   [0..1] upper 4 bits = parity, lower 12 bits = length in 2-byte words
//!   [2..3] timing value
//!   [4..]  substream data (major sync 0xF8726FBA may appear at offset 4)
//!
//! AC-3 frames (interleaved, same PID): start with sync word 0x0B77.
//! We skip AC-3 frames and only emit TrueHD access units.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Duration of one TrueHD access unit in nanoseconds (1/1200 second).
const AU_DURATION_NS: i64 = 833_333;

/// Hard cap on the reassembly buffer. A valid TrueHD/MAT access unit is
/// well under 32 KiB; if the buffer grows far past that without yielding a
/// frame the stream is malformed, so we drop it and resync rather than grow
/// without bound. Parity with the AC-3 / DTS / PGS caps.
const MAX_TRUEHD_BUF: usize = 256 * 1024;

pub struct TrueHdParser {
    buf: Vec<u8>,
    next_pts_ns: i64,
}

impl Default for TrueHdParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TrueHdParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32768),
            next_pts_ns: 0,
        }
    }

    /// Skip an AC-3 frame starting at the current buffer position.
    /// Returns number of bytes consumed, or 0 if not enough data.
    fn skip_ac3_frame(&self) -> usize {
        if self.buf.len() < 6 {
            return 0;
        }
        // AC-3 frame size from frmsizcod + fscod
        // Byte 4: [fscod:2][frmsizecod:6]
        let fscod = (self.buf[4] >> 6) & 0x03;
        let frmsizecod = (self.buf[4] & 0x3F) as usize;
        // Frame size in 16-bit words per fscod (simplified table for common rates)
        let frame_words = match fscod {
            0 => {
                // 48 kHz
                static SIZES: [usize; 38] = [
                    64, 64, 80, 80, 96, 96, 112, 112, 128, 128, 160, 160, 192, 192, 224, 224, 256,
                    256, 320, 320, 384, 384, 448, 448, 512, 512, 640, 640, 768, 768, 896, 896,
                    1024, 1024, 1152, 1152, 1280, 1280,
                ];
                SIZES.get(frmsizecod).copied().unwrap_or(0)
            }
            1 => {
                // 44.1 kHz
                static SIZES: [usize; 38] = [
                    69, 70, 87, 88, 104, 105, 121, 122, 139, 140, 174, 175, 208, 209, 243, 244,
                    278, 279, 348, 349, 417, 418, 487, 488, 557, 558, 696, 697, 835, 836, 975, 976,
                    1114, 1115, 1253, 1254, 1393, 1394,
                ];
                SIZES.get(frmsizecod).copied().unwrap_or(0)
            }
            2 => {
                // 32 kHz
                static SIZES: [usize; 38] = [
                    96, 96, 120, 120, 144, 144, 168, 168, 192, 192, 240, 240, 288, 288, 336, 336,
                    384, 384, 480, 480, 576, 576, 672, 672, 768, 768, 960, 960, 1152, 1152, 1344,
                    1344, 1536, 1536, 1728, 1728, 1920, 1920,
                ];
                SIZES.get(frmsizecod).copied().unwrap_or(0)
            }
            _ => 0,
        };
        let frame_bytes = frame_words * 2;
        if frame_bytes == 0 || self.buf.len() < frame_bytes {
            return 0;
        }
        frame_bytes
    }
}

impl CodecParser for TrueHdParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Capture the PTS base ONLY at an access-unit boundary, i.e. when no AU
        // is mid-assembly in `buf`. TrueHD access units span PES packets; a PES
        // that merely continues an AU already in progress carries its own (later)
        // PTS, which must NOT override the running per-AU timestamp. Adopting it
        // mid-AU would snap that AU's PTS backward/forward and break the
        // monotonic +AU_DURATION_NS cadence (A/V drift). Once the buffer is empty
        // the next PES legitimately begins a new AU and seeds the base.
        if self.buf.is_empty() {
            if let Some(pts) = pes.pts {
                self.next_pts_ns = pts_to_ns(pts);
            }
        }

        self.buf.extend_from_slice(&pes.data);

        let mut frames = Vec::new();

        loop {
            if self.buf.len() < 4 {
                break;
            }

            // AC-3 frame (interleaved): starts with sync word 0x0B77
            if self.buf[0] == 0x0B && self.buf[1] == 0x77 {
                let skip = self.skip_ac3_frame();
                if skip == 0 {
                    break; // incomplete AC-3 frame, wait for more data
                }
                self.buf.drain(..skip);
                continue;
            }

            // TrueHD access unit: lower 12 bits of first 2 bytes = length in words
            let unit_words = (((self.buf[0] as usize) << 8) | self.buf[1] as usize) & 0xFFF;
            if unit_words == 0 {
                self.buf.drain(..2);
                continue;
            }
            let unit_bytes = unit_words * 2;
            if unit_bytes > 32768 {
                // Likely misaligned — try to resync by scanning for AC-3 sync or
                // a valid TrueHD length
                self.buf.drain(..2);
                continue;
            }
            if self.buf.len() < unit_bytes {
                break; // incomplete access unit, wait for more data
            }

            let is_major_sync = unit_bytes >= 8
                && (u32::from_be_bytes([self.buf[4], self.buf[5], self.buf[6], self.buf[7]])
                    & 0xFFFF_FFFE)
                    == 0xF872_6FBA;

            frames.push(Frame {
                pts_ns: self.next_pts_ns,
                keyframe: is_major_sync,
                data: self.buf[..unit_bytes].to_vec(),
                duration_ns: None,
            });
            self.buf.drain(..unit_bytes);
            self.next_pts_ns += AU_DURATION_NS;
        }

        // Bound memory on malformed input: a stream that never yields a
        // complete frame must not grow the buffer without limit.
        if self.buf.len() > MAX_TRUEHD_BUF {
            self.buf.clear();
        }

        frames
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Per-bit channel counts for the TrueHD 8-channel and 6-channel presentation
/// channel-assignment masks (per the MLP spec / FFmpeg `thd_channels`). Some
/// bits denote a stereo pair (2), others a single channel (1).
const THD_8CH: [u8; 13] = [2, 1, 1, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1];
const THD_6CH: [u8; 5] = [2, 1, 1, 2, 1];

/// Decode the true channel count from a TrueHD major-sync `format_info` word
/// (the 32 bits immediately after the 0xF8726FBA sync). Returns the richest
/// presentation's channel count — the 8-channel (e.g. 7.1) presentation when
/// present, else the 6-channel (5.1) one. This is the real layout that the MPLS
/// `audio_format` base field (often 5.1 even on a 7.1/Atmos track) understates.
pub fn truehd_channels(format_info: u32) -> Option<u8> {
    let ch8 = (format_info & 0x1FFF) as u16; // 8ch_presentation_channel_assignment (13 bits)
    let ch6 = ((format_info >> 15) & 0x1F) as u16; // 6ch_presentation_channel_assignment (5 bits)
    let count = |mask: u16, tbl: &[u8]| -> u8 {
        tbl.iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, &c)| c)
            .sum()
    };
    if ch8 != 0 {
        Some(count(ch8, &THD_8CH))
    } else if ch6 != 0 {
        Some(count(ch6, &THD_6CH))
    } else {
        None
    }
}

/// Scan a demuxed TrueHD elementary-stream chunk for the first major sync and
/// decode its true channel count. The stream may interleave AC-3; we scan for
/// the major-sync word anywhere and read the following `format_info`.
pub fn truehd_channels_from_stream(data: &[u8]) -> Option<u8> {
    let mut p = 0;
    while p + 8 <= data.len() {
        let w = u32::from_be_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        if (w & 0xFFFF_FFFE) == 0xF872_6FBA {
            let fi = u32::from_be_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
            return truehd_channels(fi);
        }
        p += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data,
        }
    }

    fn make_truehd_unit(size_bytes: usize) -> Vec<u8> {
        let words = size_bytes / 2;
        let mut data = vec![0u8; size_bytes];
        data[0] = ((words >> 8) & 0x0F) as u8;
        data[1] = (words & 0xFF) as u8;
        data
    }

    fn make_ac3_frame() -> Vec<u8> {
        // Minimal AC-3 frame: sync 0x0B77, fscod=0 (48kHz), frmsizecod=0 (64 words = 128 bytes)
        let mut data = vec![0u8; 128];
        data[0] = 0x0B;
        data[1] = 0x77;
        data[4] = 0x00; // fscod=0, frmsizecod=0
        data
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = TrueHdParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_unit() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let pes = make_pes(unit, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn parse_unit_spanning_two_pes() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let mid = 100;

        let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty());

        let pes2 = make_pes(unit[mid..].to_vec(), Some(93000));
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn parse_multiple_units_incrementing_pts() {
        let mut parser = TrueHdParser::new();
        let mut data = make_truehd_unit(100);
        data.extend_from_slice(&make_truehd_unit(120));
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data.len(), 100);
        assert_eq!(frames[1].data.len(), 120);
        assert_eq!(frames[1].pts_ns - frames[0].pts_ns, AU_DURATION_NS);
    }

    #[test]
    fn skip_interleaved_ac3() {
        let mut parser = TrueHdParser::new();
        let ac3 = make_ac3_frame();
        let truehd = make_truehd_unit(200);
        let mut data = ac3;
        data.extend_from_slice(&truehd);
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn continuation_pes_pts_does_not_override_au_in_progress() {
        // An AU split across two PES packets: the FIRST PES (pts 90000) begins
        // the AU; the SECOND PES (pts 99999, a later timestamp) merely continues
        // it. The emitted AU must keep the first PES's PTS, not adopt the
        // continuation PES's later timestamp.
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let mid = 100;

        let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty(), "AU held mid-assembly");

        // Continuation PES carries a later PTS that must be ignored for this AU.
        let pes2 = make_pes(unit[mid..].to_vec(), Some(99999));
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].pts_ns,
            pts_to_ns(90000),
            "AU keeps the PTS of the PES that began it, not the continuation PES"
        );
    }

    #[test]
    fn new_au_after_empty_buffer_takes_new_pes_pts() {
        // After an AU fully drains (buffer empty), the next PES legitimately
        // seeds a fresh PTS base.
        let mut parser = TrueHdParser::new();
        let f1 = parser.parse(&make_pes(make_truehd_unit(200), Some(90000)));
        assert_eq!(f1.len(), 1);
        assert_eq!(f1[0].pts_ns, pts_to_ns(90000));

        // Buffer is now empty; a new PES with a new PTS starts a new AU.
        let f2 = parser.parse(&make_pes(make_truehd_unit(200), Some(180000)));
        assert_eq!(f2.len(), 1);
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(180000),
            "new AU after empty buffer adopts the new PES PTS"
        );
    }

    #[test]
    fn codec_private_none() {
        let parser = TrueHdParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn truehd_channels_71_from_8ch_presentation() {
        // 8ch presentation assignment bits 0-4 (LR,C,LFE,LsRs,back-LR) = 2+1+1+2+2 = 8.
        let format_info = 0x1F; // low 13 bits = 0x1F
        assert_eq!(truehd_channels(format_info), Some(8));
    }

    #[test]
    fn truehd_channels_51_from_6ch_presentation() {
        // No 8ch presentation; 6ch bits 0-3 (LR,C,LFE,LsRs) = 2+1+1+2 = 6.
        let format_info = 0xF << 15; // 6ch field = 0xF, 8ch field = 0
        assert_eq!(truehd_channels(format_info), Some(6));
    }

    #[test]
    fn truehd_channels_scan_finds_major_sync() {
        // [junk][major sync 0xF8726FBA][format_info: 8ch=0x1F -> 7.1]
        let mut data = vec![0xAA, 0xBB];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
        assert_eq!(truehd_channels_from_stream(&data), Some(8));
    }
}

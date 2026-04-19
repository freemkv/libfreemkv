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

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

/// Duration of one TrueHD access unit in nanoseconds (1/1200 second).
const AU_DURATION_NS: i64 = 833_333;

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
                    64, 64, 80, 80, 96, 96, 112, 112, 128, 128,
                    160, 160, 192, 192, 224, 224, 256, 256, 320, 320,
                    384, 384, 448, 448, 512, 512, 640, 640, 768, 768,
                    896, 896, 1024, 1024, 1152, 1152, 1280, 1280,
                ];
                SIZES.get(frmsizecod).copied().unwrap_or(0)
            }
            1 => {
                // 44.1 kHz
                static SIZES: [usize; 38] = [
                    69, 70, 87, 88, 104, 105, 121, 122, 139, 140,
                    174, 175, 208, 209, 243, 244, 278, 279, 348, 349,
                    417, 418, 487, 488, 557, 558, 696, 697, 835, 836,
                    975, 976, 1114, 1115, 1253, 1254, 1393, 1394,
                ];
                SIZES.get(frmsizecod).copied().unwrap_or(0)
            }
            2 => {
                // 32 kHz
                static SIZES: [usize; 38] = [
                    96, 96, 120, 120, 144, 144, 168, 168, 192, 192,
                    240, 240, 288, 288, 336, 336, 384, 384, 480, 480,
                    576, 576, 672, 672, 768, 768, 960, 960, 1152, 1152,
                    1344, 1344, 1536, 1536, 1728, 1728, 1920, 1920,
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

        if let Some(pts) = pes.pts {
            self.next_pts_ns = pts_to_ns(pts);
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
            });
            self.buf.drain(..unit_bytes);
            self.next_pts_ns += AU_DURATION_NS;
        }

        frames
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
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
    fn codec_private_none() {
        let parser = TrueHdParser::new();
        assert!(parser.codec_private().is_none());
    }
}

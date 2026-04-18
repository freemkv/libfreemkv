//! Dolby TrueHD / Atmos elementary stream parser.
//!
//! TrueHD access units are 2560-byte fixed-size units (40 per major sync).
//! Each unit starts with a 4-byte header: [length_hi, length_lo, timestamp_hi, timestamp_lo].
//! Major sync: 0xF8726FBA appears within a unit.
//! Buffers across PES boundaries for complete unit delivery.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

/// TrueHD access unit size (fixed).
const TRUEHD_UNIT_SIZE: usize = 2560;

pub struct TrueHdParser {
    buf: Vec<u8>,
}

impl Default for TrueHdParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TrueHdParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(TRUEHD_UNIT_SIZE * 4),
        }
    }
}

impl CodecParser for TrueHdParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        self.buf.extend_from_slice(&pes.data);

        let mut frames = Vec::new();

        // BD-TS TrueHD access units:
        //   [0..1] length in 16-bit words (includes the 4-byte header)
        //   [2..3] timestamp (ignored — we use PES PTS)
        //   [4..]  MLP payload (raw TrueHD data for MKV)
        //
        // MKV stores raw MLP frames without the 4-byte access unit header.
        const AU_HEADER: usize = 4;

        while self.buf.len() >= AU_HEADER {
            let unit_words = ((self.buf[0] as usize) << 8) | self.buf[1] as usize;
            if unit_words == 0 {
                // Padding — skip 2 bytes
                self.buf.drain(..2);
                continue;
            }
            let unit_bytes = unit_words * 2;
            if unit_bytes > 65536 || unit_bytes < AU_HEADER {
                // Invalid — skip 2 bytes to resync
                self.buf.drain(..2);
                continue;
            }
            if self.buf.len() < unit_bytes {
                // Incomplete unit — wait for more data
                break;
            }

            // Strip the 4-byte access unit header, pass only MLP payload
            frames.push(Frame {
                pts_ns,
                keyframe: true,
                data: self.buf[AU_HEADER..unit_bytes].to_vec(),
            });
            self.buf.drain(..unit_bytes);
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
        // 4-byte header: [length_hi, length_lo, ts_hi, ts_lo]
        data[0] = (words >> 8) as u8;
        data[1] = (words & 0xFF) as u8;
        data[2] = 0; // timestamp
        data[3] = 0;
        // Fill payload with non-zero to distinguish from header
        for b in data[4..].iter_mut() {
            *b = 0xAA;
        }
        data
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = TrueHdParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_unit_strips_header() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let pes = make_pes(unit, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        // Output should be 200 - 4 = 196 bytes (header stripped)
        assert_eq!(frames[0].data.len(), 196);
        assert_eq!(frames[0].data[0], 0xAA); // payload, not header
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
        assert_eq!(frames[0].data.len(), 196); // 200 - 4 header
    }

    #[test]
    fn parse_multiple_units_in_one_pes() {
        let mut parser = TrueHdParser::new();
        let mut data = make_truehd_unit(100);
        data.extend_from_slice(&make_truehd_unit(120));
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data.len(), 96);  // 100 - 4
        assert_eq!(frames[1].data.len(), 116); // 120 - 4
    }

    #[test]
    fn codec_private_none() {
        let parser = TrueHdParser::new();
        assert!(parser.codec_private().is_none());
    }
}

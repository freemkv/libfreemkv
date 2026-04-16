//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! AC3 frames are self-contained and always start with syncword 0x0B77.
//! Buffers across PES boundaries so frames that span two PES packets
//! are emitted complete, not truncated.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

pub struct Ac3Parser {
    /// Leftover bytes from previous PES (incomplete frame at end).
    buf: Vec<u8>,
}

impl Default for Ac3Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Ac3Parser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }
}

impl CodecParser for Ac3Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // Prepend leftover from previous PES
        self.buf.extend_from_slice(&pes.data);

        let data = &self.buf;
        let mut frames = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            let sync = find_ac3_sync(&data[pos..]);
            let start = match sync {
                Some(offset) => pos + offset,
                None => break,
            };

            let remaining = &data[start..];

            if remaining.len() < 6 {
                // Not enough data to determine frame size — keep for next PES
                break;
            }

            let bsid = get_bsid(remaining);
            let frame_size = if bsid >= 11 {
                eac3_frame_size(remaining)
            } else {
                ac3_frame_size(remaining)
            };

            if frame_size == 0 || frame_size > 8192 {
                // Invalid frame size — skip this sync word
                pos = start + 2;
                continue;
            }

            if start + frame_size > data.len() {
                // Incomplete frame — keep for next PES
                break;
            }

            frames.push(Frame {
                pts_ns,
                keyframe: true,
                data: data[start..start + frame_size].to_vec(),
            });
            pos = start + frame_size;
        }

        // Keep unconsumed data for next call
        // `pos` points to the start of unconsumed data (either a partial sync or leftover)
        let keep_from = if pos < data.len() {
            // Find the last sync word position in the unconsumed region
            find_ac3_sync(&data[pos..])
                .map(|o| pos + o)
                .unwrap_or(data.len())
        } else {
            data.len()
        };

        if keep_from < data.len() {
            self.buf = data[keep_from..].to_vec();
        } else {
            self.buf.clear();
        }

        frames
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Find AC3/E-AC-3 syncword (0x0B77) in data.
fn find_ac3_sync(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| data[i] == 0x0B && data[i + 1] == 0x77)
}

/// Extract bsid from an AC-3/E-AC-3 frame starting at the syncword.
/// bsid is at byte 5, bits 7..3.
pub fn get_bsid(data: &[u8]) -> u8 {
    if data.len() < 6 {
        return 0;
    }
    (data[5] >> 3) & 0x1F
}

/// Calculate E-AC-3 frame size in bytes from the frmsiz field.
fn eac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 4 {
        return 0;
    }
    let frmsiz = ((data[2] as usize & 0x07) << 8) | data[3] as usize;
    (frmsiz + 1) * 2
}

/// Calculate AC-3 frame size in bytes from fscod and frmsizecod.
fn ac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 5 {
        return 0;
    }
    let fscod = (data[4] >> 6) & 0x03;
    let frmsizecod = (data[4] & 0x3F) as usize;
    if frmsizecod >= AC3_FRAME_SIZES.len() {
        return 0;
    }
    let words = AC3_FRAME_SIZES[frmsizecod];
    match fscod {
        0 => words[0] * 2,
        1 => words[1] * 2,
        2 => words[2] * 2,
        _ => 0,
    }
}

/// AC-3 frame size table: [frmsizecod] -> [48kHz words, 44.1kHz words, 32kHz words]
const AC3_FRAME_SIZES: [[usize; 3]; 38] = [
    [64, 69, 96],
    [64, 70, 96],
    [80, 87, 120],
    [80, 88, 120],
    [96, 104, 144],
    [96, 105, 144],
    [112, 121, 168],
    [112, 122, 168],
    [128, 139, 192],
    [128, 140, 192],
    [160, 174, 240],
    [160, 175, 240],
    [192, 208, 288],
    [192, 209, 288],
    [224, 243, 336],
    [224, 244, 336],
    [256, 278, 384],
    [256, 279, 384],
    [320, 348, 480],
    [320, 349, 480],
    [384, 417, 576],
    [384, 418, 576],
    [448, 487, 672],
    [448, 488, 672],
    [512, 557, 768],
    [512, 558, 768],
    [640, 696, 960],
    [640, 697, 960],
    [768, 835, 1152],
    [768, 836, 1152],
    [896, 975, 1344],
    [896, 976, 1344],
    [1024, 1114, 1536],
    [1024, 1115, 1536],
    [1152, 1253, 1728],
    [1152, 1254, 1728],
    [1280, 1393, 1920],
    [1280, 1394, 1920],
];

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ac3_frame(fscod: u8, frmsizecod: u8) -> Vec<u8> {
        let size = AC3_FRAME_SIZES[frmsizecod as usize][fscod as usize] * 2;
        let mut frame = vec![0u8; size];
        frame[0] = 0x0B;
        frame[1] = 0x77;
        frame[4] = (fscod << 6) | frmsizecod;
        frame[5] = 0x08 << 3; // bsid = 8 (AC-3)
        frame
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = Ac3Parser::new();
        let pes = PesPacket {
            pid: 0,
            pts: None,
            dts: None,
            data: vec![],
        };
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 48kHz, 80 words = 160 bytes
        let pes = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data.clone(),
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 160);
    }

    #[test]
    fn parse_frame_spanning_two_pes() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 160 bytes
        let mid = 80;

        // First PES: first half of frame
        let pes1 = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data[..mid].to_vec(),
        };
        let frames1 = parser.parse(&pes1);
        assert!(frames1.is_empty(), "partial frame should not emit");

        // Second PES: second half
        let pes2 = PesPacket {
            pid: 0,
            pts: Some(93000),
            dts: None,
            data: frame_data[mid..].to_vec(),
        };
        let frames2 = parser.parse(&pes2);
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0].data.len(), 160);
    }

    #[test]
    fn skip_garbage_before_sync() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2);
        let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF]; // garbage
        data.extend_from_slice(&frame_data);
        let pes = PesPacket {
            pid: 0,
            pts: None,
            dts: None,
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 160);
    }

    #[test]
    fn ac3_frame_size_table() {
        // fscod=0 (48kHz), frmsizecod=0: 64 words = 128 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x00, 0x40]), 128);
        // fscod=0 (48kHz), frmsizecod=2: 80 words = 160 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x02, 0x40]), 160);
    }
}

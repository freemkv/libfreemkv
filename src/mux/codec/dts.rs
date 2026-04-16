//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! Buffers across PES boundaries so frames spanning two PES packets
//! are emitted complete.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

const DTS_CORE_SYNC: [u8; 4] = [0x7F, 0xFE, 0x80, 0x01];
const DTS_HD_EXT_SYNC: [u8; 4] = [0x64, 0x58, 0x20, 0x25];

pub struct DtsParser {
    buf: Vec<u8>,
}

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32768),
        }
    }
}

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        self.buf.extend_from_slice(&pes.data);

        let data = &self.buf;
        let mut frames = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            // Find DTS core sync
            let start = match find_sync(&data[pos..], &DTS_CORE_SYNC) {
                Some(offset) => pos + offset,
                None => break,
            };

            // Need at least 10 bytes for core header to get frame size
            if start + 10 > data.len() {
                break;
            }

            let core_size = dts_core_frame_size(&data[start..]);
            if core_size == 0 || core_size > 32768 {
                pos = start + 4;
                continue;
            }

            if start + core_size > data.len() {
                // Incomplete core frame
                break;
            }

            // Check for DTS-HD extension after core
            let mut total_size = core_size;
            if start + core_size + 4 <= data.len() {
                if let Some(0) = find_sync(
                    &data[start + core_size..start + core_size + 4],
                    &DTS_HD_EXT_SYNC,
                ) {
                    let ext = &data[start + core_size..];
                    if ext.len() >= 9 {
                        let ext_size = dts_hd_ext_frame_size(ext);
                        if start + core_size + ext_size <= data.len() {
                            total_size = core_size + ext_size;
                        }
                        // If ext incomplete, just emit core
                    }
                }
            }

            frames.push(Frame {
                pts_ns,
                keyframe: true,
                data: data[start..start + total_size].to_vec(),
            });
            pos = start + total_size;
        }

        // Keep unconsumed data
        let keep_from = if pos < data.len() {
            find_sync(&data[pos..], &DTS_CORE_SYNC)
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

fn find_sync(data: &[u8], pattern: &[u8; 4]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    (0..=data.len() - 4).find(|&i| data[i..i + 4] == *pattern)
}

/// DTS core frame size from header bits.
/// fsize is at bits 46-59 (14 bits) of the header: bytes 5-7.
fn dts_core_frame_size(data: &[u8]) -> usize {
    if data.len() < 10 {
        return 0;
    }
    // fsize field: 14 bits starting at bit 46
    // byte 5 bits 1-0, byte 6 all 8, byte 7 bits 7-4
    let fsize =
        ((data[5] as usize & 0x03) << 12) | ((data[6] as usize) << 4) | ((data[7] as usize) >> 4);
    fsize + 1
}

/// DTS-HD extension frame size from extension header.
pub fn dts_hd_ext_frame_size(ext: &[u8]) -> usize {
    if ext.len() < 9 {
        return 0;
    }
    let raw =
        ((ext[6] as usize & 0x1F) << 11) | ((ext[7] as usize) << 3) | ((ext[8] as usize) >> 5);
    raw + 1
}

pub fn find_dts_hd_ext_sync(data: &[u8]) -> Option<usize> {
    find_sync(data, &DTS_HD_EXT_SYNC)
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

    fn make_dts_core(size: usize) -> Vec<u8> {
        let fsize = size - 1;
        let mut data = vec![0u8; size];
        data[0..4].copy_from_slice(&DTS_CORE_SYNC);
        data[5] = (data[5] & 0xFC) | ((fsize >> 12) & 0x03) as u8;
        data[6] = ((fsize >> 4) & 0xFF) as u8;
        data[7] = (data[7] & 0x0F) | (((fsize & 0x0F) << 4) as u8);
        data
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = DtsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let pes = make_pes(frame, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 512);
    }

    #[test]
    fn parse_frame_spanning_two_pes() {
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let mid = 256;

        let pes1 = make_pes(frame[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty());

        let pes2 = make_pes(frame[mid..].to_vec(), Some(93000));
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 512);
    }

    #[test]
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }
}

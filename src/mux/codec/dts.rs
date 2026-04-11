//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! The extension contains high-resolution audio data and is appended to the core frame.
//! All frames are keyframes (no inter-frame dependencies).
//! Each PES packet = one frame.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

/// DTS-HD extension syncword bytes.
const DTS_HD_EXT_SYNC: [u8; 4] = [0x64, 0x58, 0x20, 0x25];

pub struct DtsParser;

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        let data = &pes.data;

        // Look for a DTS-HD extension substream after the core.
        // If found, include both core + extension in the output frame.
        let frame_data = match find_dts_hd_ext_sync(data) {
            Some(ext_offset) => {
                let ext = &data[ext_offset..];
                if ext.len() >= 9 {
                    let ext_size = dts_hd_ext_frame_size(ext);
                    let total_end = ext_offset + ext_size;
                    let end = total_end.min(data.len());
                    data[..end].to_vec()
                } else {
                    // Extension header too short to parse size; include all data.
                    data.to_vec()
                }
            }
            None => data.to_vec(),
        };

        vec![Frame {
            pts_ns,
            keyframe: true,
            data: frame_data,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Find the DTS-HD extension syncword (0x64582025) in data.
/// Returns the byte offset of the sync, or None.
pub fn find_dts_hd_ext_sync(data: &[u8]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    for i in 0..=data.len() - 4 {
        if data[i] == DTS_HD_EXT_SYNC[0]
            && data[i + 1] == DTS_HD_EXT_SYNC[1]
            && data[i + 2] == DTS_HD_EXT_SYNC[2]
            && data[i + 3] == DTS_HD_EXT_SYNC[3]
        {
            return Some(i);
        }
    }
    None
}

/// Calculate DTS-HD extension frame size from the extension header.
/// The size field is at bytes 6-8 of the extension:
///   ((ext[6] & 0x1F) << 11) | (ext[7] << 3) | (ext[8] >> 5) + 1
pub fn dts_hd_ext_frame_size(ext: &[u8]) -> usize {
    debug_assert!(ext.len() >= 9);
    let raw =
        ((ext[6] as usize & 0x1F) << 11) | ((ext[7] as usize) << 3) | ((ext[8] as usize) >> 5);
    raw + 1
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

    /// Build a DTS core frame with given payload size.
    fn make_dts_core(payload_len: usize) -> Vec<u8> {
        let mut data = vec![0x7F, 0xFE, 0x80, 0x01];
        data.resize(4 + payload_len, 0xAA);
        data
    }

    /// Build a DTS-HD extension header + payload.
    /// ext_size is the value to encode (frame size = ext_size + 1 reported by dts_hd_ext_frame_size,
    /// but we encode raw = ext_size so that dts_hd_ext_frame_size returns ext_size + 1).
    fn make_dts_hd_ext(raw_size_field: usize, payload_fill: u8) -> Vec<u8> {
        let total = raw_size_field + 1; // the size dts_hd_ext_frame_size will return
        let byte6 = ((raw_size_field >> 11) & 0x1F) as u8;
        let byte7 = ((raw_size_field >> 3) & 0xFF) as u8;
        let byte8 = ((raw_size_field & 0x07) << 5) as u8;
        let mut data = vec![0x64, 0x58, 0x20, 0x25, 0x00, 0x00, byte6, byte7, byte8];
        while data.len() < total {
            data.push(payload_fill);
        }
        data.truncate(total);
        data
    }

    // --- DTS-HD extension sync detection ---

    #[test]
    fn find_ext_sync_at_offset() {
        let mut data = vec![0x7F, 0xFE, 0x80, 0x01, 0x00, 0x00];
        data.extend_from_slice(&[0x64, 0x58, 0x20, 0x25]);
        assert_eq!(find_dts_hd_ext_sync(&data), Some(6));
    }

    #[test]
    fn find_ext_sync_none() {
        let data = vec![0x7F, 0xFE, 0x80, 0x01, 0x00, 0x00];
        assert_eq!(find_dts_hd_ext_sync(&data), None);
    }

    #[test]
    fn find_ext_sync_at_start() {
        let data = vec![0x64, 0x58, 0x20, 0x25, 0x00];
        assert_eq!(find_dts_hd_ext_sync(&data), Some(0));
    }

    #[test]
    fn find_ext_sync_too_short() {
        let data = vec![0x64, 0x58, 0x20];
        assert_eq!(find_dts_hd_ext_sync(&data), None);
    }

    // --- DTS-HD extension frame size ---

    #[test]
    fn ext_frame_size_basic() {
        // raw_size_field = 100 → frame size = 101
        let ext = make_dts_hd_ext(100, 0xBB);
        assert_eq!(dts_hd_ext_frame_size(&ext), 101);
    }

    #[test]
    fn ext_frame_size_zero() {
        // raw_size_field = 0 → frame size = 1
        let ext = vec![0x64, 0x58, 0x20, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(dts_hd_ext_frame_size(&ext), 1);
    }

    #[test]
    fn ext_frame_size_large() {
        // raw = 0x1F << 11 | 0xFF << 3 | 0x07 = 0xFFFF = 65535
        // frame_size = 65536
        let ext = vec![0x64, 0x58, 0x20, 0x25, 0x00, 0x00, 0x1F, 0xFF, 0xFF];
        // byte6=0x1F, byte7=0xFF, byte8=0xFF
        // (0x1F << 11) | (0xFF << 3) | (0xFF >> 5) = 63488 | 2040 | 7 = 65535
        assert_eq!(dts_hd_ext_frame_size(&ext), 65536);
    }

    // --- parse: core + extension frame ---

    #[test]
    fn parse_core_plus_extension() {
        let mut parser = DtsParser::new();
        let core = make_dts_core(20); // 24 bytes total
        let ext = make_dts_hd_ext(50, 0xCC); // 51 bytes
        let mut data = core.clone();
        data.extend_from_slice(&ext);

        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        // Frame should include core (24) + extension (51) = 75 bytes
        assert_eq!(frames[0].data.len(), 24 + 51);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert!(frames[0].keyframe);
    }

    #[test]
    fn parse_core_only() {
        let mut parser = DtsParser::new();
        let data = make_dts_core(10);
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
    }

    #[test]
    fn parse_core_plus_extension_truncated_at_buffer_end() {
        let mut parser = DtsParser::new();
        let core = make_dts_core(4); // 8 bytes
                                     // Extension claims 200 bytes but we only provide 20
        let ext = make_dts_hd_ext(199, 0xDD); // wants 200 bytes
        let mut data = core;
        // Only append partial extension (first 20 bytes)
        data.extend_from_slice(&ext[..20.min(ext.len())]);

        let total_len = data.len();
        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        // Should be clamped to actual data length
        assert_eq!(frames[0].data.len(), total_len);
    }

    // --- basic tests (carried over) ---

    #[test]
    fn parse_basic_frame() {
        let mut parser = DtsParser::new();
        let data = vec![0x7F, 0xFE, 0x80, 0x01, 0xAA, 0xBB, 0xCC];
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn all_keyframes() {
        let mut parser = DtsParser::new();
        for i in 0..3 {
            let data = vec![0x7F, 0xFE, 0x80, 0x01, i];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "DTS frame should always be keyframe");
        }
    }

    #[test]
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = DtsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn no_pts() {
        let mut parser = DtsParser::new();
        let pes = make_pes(vec![0x7F, 0xFE, 0x80, 0x01], None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }
}

//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! AC3 frames are self-contained and always start with syncword 0x0B77.
//! Each PES packet typically contains exactly one AC3 frame.
//! All AC3 frames are effectively keyframes (no inter-frame dependencies).
//!
//! E-AC-3 shares the same syncword but uses bsid >= 11 (typically 16).
//! Frame size is derived from the frmsiz field instead of fscod/frmsizecod.

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

pub struct Ac3Parser;

impl Default for Ac3Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Ac3Parser {
    pub fn new() -> Self {
        Self
    }
}

impl CodecParser for Ac3Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.len() < 2 {
            return Vec::new();
        }

        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        let data = &pes.data;
        let mut frames = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            let sync = find_ac3_sync(&data[pos..]);
            let start = match sync {
                Some(offset) => pos + offset,
                None => break,
            };

            let remaining = &data[start..];

            // Need at least 6 bytes to inspect bsid / frame size fields
            if remaining.len() < 6 {
                // Emit whatever remains as a single frame
                frames.push(Frame {
                    pts_ns,
                    keyframe: true,
                    data: remaining.to_vec(),
                });
                break;
            }

            let bsid = get_bsid(remaining);

            if bsid >= 11 {
                // E-AC-3 frame size from frmsiz field (bytes 2-3)
                let frame_size = eac3_frame_size(remaining);

                let end = start + frame_size.min(data.len() - start);
                frames.push(Frame {
                    pts_ns,
                    keyframe: true,
                    data: data[start..end].to_vec(),
                });
                pos = end;
            } else {
                // AC-3: emit everything from syncword to next syncword (or end)
                let next_sync = find_ac3_sync(&data[start + 2..]).map(|o| start + 2 + o);
                let end = next_sync.unwrap_or(data.len());
                frames.push(Frame {
                    pts_ns,
                    keyframe: true,
                    data: data[start..end].to_vec(),
                });
                pos = end;
            }
        }

        // If we found no syncword at all, return empty — the data is not valid AC3.
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
/// AC-3: bsid <= 10, E-AC-3: bsid >= 11 (typically 16).
pub fn get_bsid(data: &[u8]) -> u8 {
    if data.len() < 6 {
        return 0;
    }
    (data[5] >> 3) & 0x1F
}

/// Calculate E-AC-3 frame size in bytes from the frmsiz field.
/// frmsiz is at bits [2:0] of byte 2 concatenated with byte 3.
/// Frame size = (frmsiz + 1) * 2 bytes.
pub fn eac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 4 {
        return 0;
    }
    let frmsiz = ((data[2] as usize & 0x07) << 8) | (data[3] as usize);
    (frmsiz + 1) * 2
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

    /// Build a minimal AC-3 header (bsid <= 10).
    fn make_ac3_header(bsid: u8) -> Vec<u8> {
        // 0x0B 0x77 <byte2> <byte3> <byte4> <byte5=bsid>
        let byte5 = (bsid & 0x1F) << 3;
        vec![0x0B, 0x77, 0x00, 0x00, 0x00, byte5, 0xAA, 0xBB]
    }

    /// Build a minimal E-AC-3 header with the given bsid and frmsiz.
    /// frmsiz encodes frame size: frame_bytes = (frmsiz + 1) * 2.
    fn make_eac3_header(bsid: u8, frmsiz: u16, payload_fill: u8) -> Vec<u8> {
        let byte2 = (frmsiz >> 8) as u8 & 0x07;
        let byte3 = (frmsiz & 0xFF) as u8;
        let byte5 = (bsid & 0x1F) << 3;
        let frame_size = (frmsiz as usize + 1) * 2;
        let mut data = vec![0x0B, 0x77, byte2, byte3, 0x00, byte5];
        // Pad to full frame size
        while data.len() < frame_size {
            data.push(payload_fill);
        }
        data.truncate(frame_size);
        data
    }

    // --- syncword detection ---

    #[test]
    fn find_ac3_sync_at_start() {
        let data = [0x0B, 0x77, 0x01, 0x02, 0x03];
        assert_eq!(find_ac3_sync(&data), Some(0));
    }

    #[test]
    fn find_ac3_sync_with_garbage_prefix() {
        let data = [0xFF, 0xFE, 0x0B, 0x77, 0x01, 0x02];
        assert_eq!(find_ac3_sync(&data), Some(2));
    }

    #[test]
    fn find_ac3_sync_none() {
        let data = [0x0B, 0x78, 0x00, 0x00];
        assert_eq!(find_ac3_sync(&data), None);
    }

    #[test]
    fn find_ac3_sync_empty() {
        let data: [u8; 0] = [];
        assert_eq!(find_ac3_sync(&data), None);
    }

    // --- bsid detection ---

    #[test]
    fn bsid_ac3() {
        let header = make_ac3_header(8);
        assert_eq!(get_bsid(&header), 8);
    }

    #[test]
    fn bsid_eac3() {
        let header = make_eac3_header(16, 99, 0x00);
        assert_eq!(get_bsid(&header), 16);
    }

    #[test]
    fn bsid_boundary_10() {
        let header = make_ac3_header(10);
        assert_eq!(get_bsid(&header), 10);
        // bsid 10 should be treated as AC-3 (<= 10)
        assert!(get_bsid(&header) <= 10);
    }

    #[test]
    fn bsid_boundary_11() {
        let header = make_eac3_header(11, 3, 0x00);
        assert_eq!(get_bsid(&header), 11);
        // bsid 11 should be treated as E-AC-3 (>= 11)
        assert!(get_bsid(&header) >= 11);
    }

    // --- E-AC-3 frame size calculation ---

    #[test]
    fn eac3_frame_size_basic() {
        // frmsiz = 99 → frame_size = (99+1)*2 = 200 bytes
        let header = make_eac3_header(16, 99, 0xDD);
        assert_eq!(eac3_frame_size(&header), 200);
    }

    #[test]
    fn eac3_frame_size_min() {
        // frmsiz = 0 → frame_size = (0+1)*2 = 2 bytes
        let data = [0x0B, 0x77, 0x00, 0x00, 0x00, 0x80];
        assert_eq!(eac3_frame_size(&data), 2);
    }

    #[test]
    fn eac3_frame_size_large() {
        // frmsiz = 0x7FF (max 11-bit) → (2047+1)*2 = 4096
        let data = [0x0B, 0x77, 0x07, 0xFF, 0x00, 0x80];
        assert_eq!(eac3_frame_size(&data), 4096);
    }

    // --- parse: E-AC-3 frame extraction ---

    #[test]
    fn parse_eac3_single_frame() {
        let mut parser = Ac3Parser::new();
        // frmsiz = 9 → frame_size = 20 bytes
        let data = make_eac3_header(16, 9, 0xCC);
        assert_eq!(data.len(), 20);
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 20);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert!(frames[0].keyframe);
    }

    #[test]
    fn parse_eac3_frame_with_garbage_prefix() {
        let mut parser = Ac3Parser::new();
        let mut data = vec![0xFF, 0xFE]; // garbage
        data.extend_from_slice(&make_eac3_header(16, 4, 0xAA)); // frmsiz=4 → 10 bytes
        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data[0], 0x0B);
        assert_eq!(frames[0].data[1], 0x77);
        assert_eq!(frames[0].data.len(), 10);
    }

    // --- parse syncword → frame extracted (AC-3) ---

    #[test]
    fn parse_syncword() {
        let mut parser = Ac3Parser::new();

        // AC3 frame starting with syncword (bsid=8)
        let data = make_ac3_header(8);
        let pes = make_pes(data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn parse_syncword_with_garbage_prefix() {
        let mut parser = Ac3Parser::new();

        // Garbage bytes before syncword
        let mut data = vec![0xFF, 0xFE];
        data.extend_from_slice(&make_ac3_header(8));
        let pes = make_pes(data, Some(0));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data[0], 0x0B);
        assert_eq!(frames[0].data[1], 0x77);
    }

    // --- all frames are keyframes ---

    #[test]
    fn all_keyframes() {
        let mut parser = Ac3Parser::new();

        for i in 0..5u8 {
            let mut data = make_ac3_header(8);
            data.push(i);
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "AC3 frame {} should be a keyframe", i);
        }
    }

    // --- codec_private is None ---

    #[test]
    fn codec_private_none() {
        let parser = Ac3Parser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- empty / too-short PES ---

    #[test]
    fn parse_empty_pes() {
        let mut parser = Ac3Parser::new();
        let pes = make_pes(Vec::new(), Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    #[test]
    fn parse_single_byte_pes() {
        let mut parser = Ac3Parser::new();
        let pes = make_pes(vec![0x0B], Some(0));
        let frames = parser.parse(&pes);
        assert!(frames.is_empty());
    }

    // --- PTS conversion ---

    #[test]
    fn pts_conversion() {
        let mut parser = Ac3Parser::new();
        let data = make_ac3_header(8);
        // 45000 ticks = 0.5 seconds → 500_000_000 ns
        let pes = make_pes(data, Some(45000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 500_000_000);
    }

    // --- None PTS ---

    #[test]
    fn no_pts() {
        let mut parser = Ac3Parser::new();
        let data = make_ac3_header(8);
        let pes = make_pes(data, None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }
}

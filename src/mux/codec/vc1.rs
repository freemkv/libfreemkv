//! VC-1 (SMPTE 421M) elementary stream parser.
//!
//! VC-1 uses start codes similar to MPEG-2.
//! Sequence header (0x0F) contains codec initialization data.
//! Frame start = Frame header start code (0x0D).
//! I-frames (keyframes) are identified from the frame header.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SC_SEQUENCE_HEADER: u8 = 0x0F;
const SC_ENTRY_POINT: u8 = 0x0E;
const SC_FRAME: u8 = 0x0D;

pub struct Vc1Parser {
    seq_header: Option<Vec<u8>>,
    entry_point: Option<Vec<u8>>,
}

impl Vc1Parser {
    pub fn new() -> Self {
        Self { seq_header: None, entry_point: None }
    }
}

impl CodecParser for Vc1Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Use DTS when available (monotonic for B-frame content), fall back to PTS
        let ts_ns = pes.dts.or(pes.pts).map(pts_to_ns).unwrap_or(0);
        let mut has_seq_header = false;
        let mut frame_start: Option<usize> = None;

        // Scan for start codes (00 00 01 XX)
        let data = &pes.data;
        let mut i = 0;
        while i + 3 < data.len() {
            if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 {
                let sc_type = data[i + 3];
                match sc_type {
                    SC_SEQUENCE_HEADER => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        self.seq_header = Some(data[i..end].to_vec());
                        has_seq_header = true;
                    }
                    SC_ENTRY_POINT => {
                        let end = find_next_sc(data, i + 4).unwrap_or(data.len());
                        self.entry_point = Some(data[i..end].to_vec());
                    }
                    SC_FRAME => {
                        // Frame data starts at this start code
                        if frame_start.is_none() {
                            frame_start = Some(i);
                        }
                    }
                    _ => {}
                }
                i += 4;
            } else {
                i += 1;
            }
        }

        // Keyframe = this PES contains a sequence header (I-frame indicator in BD)
        let keyframe = has_seq_header;

        // Strip sequence header + entry point from frame data — those are in codecPrivate.
        // Only include data from the frame start code onwards.
        let frame_data = match frame_start {
            Some(start) => &data[start..],
            None => data, // no frame start code found, pass through entire PES
        };

        vec![Frame {
            pts_ns: ts_ns,
            keyframe,
            data: frame_data.to_vec(),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        // MKV V_MS/VFW/FOURCC requires BITMAPINFOHEADER (40 bytes) + extra codec data.
        // The sequence header + entry point go as extra data after the header.
        let sh = self.seq_header.as_ref()?;
        let ep = self.entry_point.as_ref()?;

        let extra_len = sh.len() + ep.len();
        let header_size: u32 = 40 + extra_len as u32;

        let mut cp = Vec::with_capacity(header_size as usize);

        // BITMAPINFOHEADER (40 bytes, little-endian)
        cp.extend_from_slice(&header_size.to_le_bytes());   // biSize
        cp.extend_from_slice(&1920u32.to_le_bytes());        // biWidth (updated by player)
        cp.extend_from_slice(&1080u32.to_le_bytes());        // biHeight
        cp.extend_from_slice(&1u16.to_le_bytes());           // biPlanes
        cp.extend_from_slice(&24u16.to_le_bytes());          // biBitCount
        cp.extend_from_slice(b"WVC1");                       // biCompression = "WVC1" FOURCC
        cp.extend_from_slice(&0u32.to_le_bytes());           // biSizeImage
        cp.extend_from_slice(&0u32.to_le_bytes());           // biXPelsPerMeter
        cp.extend_from_slice(&0u32.to_le_bytes());           // biYPelsPerMeter
        cp.extend_from_slice(&0u32.to_le_bytes());           // biClrUsed
        cp.extend_from_slice(&0u32.to_le_bytes());           // biClrImportant

        // Extra codec data: sequence header + entry point (Annex B)
        cp.extend_from_slice(sh);
        cp.extend_from_slice(ep);

        Some(cp)
    }
}

fn find_next_sc(data: &[u8], from: usize) -> Option<usize> {
    for i in from..data.len().saturating_sub(2) {
        if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 {
            return Some(i);
        }
    }
    None
}

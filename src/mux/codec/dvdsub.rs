//! DVD bitmap subtitle (VobSub) parser.
//!
//! DVD subtitles are carried in PS private stream 1 with sub-stream IDs 0x20-0x3F.
//! A single subpicture unit (SPU — one displayed bitmap) may span multiple PES
//! packets: only the first PES carries a PTS, continuations carry PTS=0. The SPU
//! begins with a 2-byte big-endian `SPU_size` giving the total byte length of the
//! whole unit. We reassemble across PES boundaries into one Frame so large
//! subtitles aren't split/garbled, inheriting the head PES's PTS.
//!
//! For MKV: codec ID "S_VOBSUB".
//! All frames are keyframes (each is a complete bitmap).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Upper bound on a single reassembled SPU. The SPU_size field is 16 bits, so a
/// well-formed unit is at most 0xFFFF bytes; cap accumulation here to bound
/// memory if the field is corrupt or the stream never completes a unit.
const MAX_SPU_BYTES: usize = 0xFFFF;

pub struct DvdSubParser {
    /// Pre-formatted VobSub .idx palette header for codec_private.
    codec_data: Option<Vec<u8>>,
    /// In-progress SPU reassembly: (head PTS in ns, declared SPU_size, bytes).
    pending: Option<(i64, usize, Vec<u8>)>,
}

impl DvdSubParser {
    pub fn new(codec_data: Option<Vec<u8>>) -> Self {
        Self {
            codec_data,
            pending: None,
        }
    }

    /// Emit `pending` as a Frame if it is complete (or `force` at EOF),
    /// returning it and clearing the buffer. Returns None if nothing to emit.
    fn take_if_complete(&mut self, force: bool) -> Option<Frame> {
        let (pts_ns, size, buf) = self.pending.as_ref()?;
        if force || buf.len() >= *size {
            let (pts_ns, _, data) = self.pending.take().unwrap();
            return Some(Frame {
                pts_ns,
                keyframe: true,
                data,
                duration_ns: None,
            });
        }
        let _ = pts_ns;
        None
    }
}

impl CodecParser for DvdSubParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();

        if self.pending.is_some() {
            // Continuation of an in-progress SPU (PTS=0 on these). Append,
            // bounded by MAX_SPU_BYTES.
            if let Some((_, _, buf)) = self.pending.as_mut() {
                let room = MAX_SPU_BYTES.saturating_sub(buf.len());
                let take = room.min(pes.data.len());
                buf.extend_from_slice(&pes.data[..take]);
            }
            if let Some(frame) = self.take_if_complete(false) {
                out.push(frame);
            }
            return out;
        }

        // Start of a new SPU. The first 2 bytes are the big-endian total size.
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        let declared = if pes.data.len() >= 2 {
            ((pes.data[0] as usize) << 8) | pes.data[1] as usize
        } else {
            // Too short to carry SPU_size — pass through as a lone frame.
            return vec![Frame {
                pts_ns,
                keyframe: true,
                data: pes.data.clone(),
                duration_ns: None,
            }];
        };

        let mut buf = pes.data.clone();
        if buf.len() > MAX_SPU_BYTES {
            buf.truncate(MAX_SPU_BYTES);
        }
        self.pending = Some((pts_ns, declared, buf));
        if let Some(frame) = self.take_if_complete(false) {
            out.push(frame);
        }
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // At EOF, emit whatever SPU bytes remain even if the declared size was
        // never reached (truncated final subtitle is better than dropping it).
        self.take_if_complete(true).into_iter().collect()
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        self.codec_data.clone()
    }
}

// ── YCbCr → RGB conversion and palette formatting ─────────────────────────

/// Convert a single YCbCr color to RGB, clamping to [0, 255].
///
/// Input: `[padding, Y, Cb, Cr]` (as stored in DVD IFO PGC data).
/// Returns `[R, G, B]`.
pub fn ycbcr_to_rgb(color: &[u8; 4]) -> [u8; 3] {
    let y = color[1] as f64;
    let cb = color[2] as f64;
    let cr = color[3] as f64;

    let r = y + 1.402 * (cr - 128.0);
    let g = y - 0.344 * (cb - 128.0) - 0.714 * (cr - 128.0);
    let b = y + 1.772 * (cb - 128.0);

    [clamp_u8(r), clamp_u8(g), clamp_u8(b)]
}

fn clamp_u8(v: f64) -> u8 {
    if v < 0.0 {
        0
    } else if v > 255.0 {
        255
    } else {
        v.round() as u8
    }
}

/// Format a 16-color YCbCr palette as a VobSub .idx palette header.
///
/// Each entry is `[padding, Y, Cb, Cr]`. Output is a UTF-8 text block:
/// `palette: rrggbb, rrggbb, ...\n`
///
/// Returns the formatted bytes suitable for MKV codec_private.
pub fn format_palette(palette: &[[u8; 4]]) -> Vec<u8> {
    let mut parts: Vec<String> = Vec::with_capacity(palette.len());
    for color in palette {
        let [r, g, b] = ycbcr_to_rgb(color);
        parts.push(format!("{r:02x}{g:02x}{b:02x}"));
    }
    let line = format!("palette: {}\n", parts.join(", "));
    line.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1200,
            pts,
            dts: None,
            data,
        }
    }

    #[test]
    fn passthrough_data() {
        let mut parser = DvdSubParser::new(None);
        let sub_data = vec![0x00, 0x0A, 0x00, 0x08, 0x01, 0xFF, 0x02, 0x03, 0x04, 0x05];
        let pes = make_pes(sub_data.clone(), Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].data, sub_data,
            "VobSub data should pass through unmodified"
        );
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn always_keyframe() {
        let mut parser = DvdSubParser::new(None);
        for i in 0..3u8 {
            let data = vec![0x00, i, 0x00, i + 1];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(
                frames[0].keyframe,
                "DVD subtitle frames should always be keyframes"
            );
        }
    }

    #[test]
    fn empty_pes_returns_no_frames() {
        let mut parser = DvdSubParser::new(None);
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn codec_private_none_by_default() {
        let parser = DvdSubParser::new(None);
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn codec_private_returns_palette_when_set() {
        let palette_data = b"palette: 000000, ffffff\n".to_vec();
        let parser = DvdSubParser::new(Some(palette_data.clone()));
        let cp = parser.codec_private();
        assert!(cp.is_some());
        assert_eq!(cp.unwrap(), palette_data);
    }

    #[test]
    fn no_pts_defaults_to_zero() {
        let mut parser = DvdSubParser::new(None);
        // SPU_size = 2, single complete PES (the 2 size bytes themselves).
        let pes = make_pes(vec![0x00, 0x02], None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
    }

    #[test]
    fn multi_pes_spu_reassembled() {
        let mut parser = DvdSubParser::new(None);
        // Declared SPU_size = 12 bytes total. First PES carries the 2 size
        // bytes + 4 payload bytes and the only PTS; the next two PESs are
        // continuations with PTS=0.
        let head = vec![0x00, 0x0C, 0xAA, 0xBB, 0xCC, 0xDD];
        let cont1 = vec![0x11, 0x22, 0x33];
        let cont2 = vec![0x44, 0x55, 0x66];

        let f = parser.parse(&make_pes(head.clone(), Some(90000)));
        assert!(f.is_empty(), "incomplete SPU should not emit yet");
        let f = parser.parse(&make_pes(cont1.clone(), Some(0)));
        assert!(f.is_empty(), "still incomplete");
        let frames = parser.parse(&make_pes(cont2.clone(), Some(0)));
        assert_eq!(frames.len(), 1, "completed SPU emits exactly one frame");

        // Reassembled bytes = head + cont1 + cont2, in order.
        let mut expected = head;
        expected.extend_from_slice(&cont1);
        expected.extend_from_slice(&cont2);
        assert_eq!(frames[0].data, expected);
        // PTS inherited from the head PES (1s = 1e9 ns), not the PTS=0 tails.
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
        assert!(frames[0].keyframe);
    }

    #[test]
    fn flush_emits_truncated_trailing_spu() {
        let mut parser = DvdSubParser::new(None);
        // Declared 100 bytes but only 6 ever arrive before EOF.
        let head = vec![0x00, 0x64, 0xDE, 0xAD, 0xBE, 0xEF];
        let f = parser.parse(&make_pes(head.clone(), Some(90000)));
        assert!(f.is_empty(), "incomplete SPU should not emit during parse");
        let frames = parser.flush();
        assert_eq!(frames.len(), 1, "EOF flush emits the partial SPU");
        assert_eq!(frames[0].data, head);
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    // ── YCbCr → RGB conversion tests ──────────────────────────────────────

    #[test]
    fn ycbcr_to_rgb_white() {
        // White in YCbCr: Y=235, Cb=128, Cr=128 → R=235, G=235, B=235
        let color = [0x00, 235, 128, 128];
        let [r, g, b] = ycbcr_to_rgb(&color);
        assert_eq!(r, 235);
        assert_eq!(g, 235);
        assert_eq!(b, 235);
    }

    #[test]
    fn ycbcr_to_rgb_black() {
        // Black: Y=16, Cb=128, Cr=128 → R=16, G=16, B=16
        let color = [0x00, 16, 128, 128];
        let [r, g, b] = ycbcr_to_rgb(&color);
        assert_eq!(r, 16);
        assert_eq!(g, 16);
        assert_eq!(b, 16);
    }

    #[test]
    fn ycbcr_to_rgb_clamps_overflow() {
        // Y=255, Cr=255 → R would be 255 + 1.402*127 = ~433, should clamp to 255
        let color = [0x00, 255, 128, 255];
        let [r, _g, _b] = ycbcr_to_rgb(&color);
        assert_eq!(r, 255);
    }

    #[test]
    fn ycbcr_to_rgb_clamps_underflow() {
        // Y=0, Cr=0 → R = 0 + 1.402*(0-128) = -179, should clamp to 0
        let color = [0x00, 0, 128, 0];
        let [r, _g, _b] = ycbcr_to_rgb(&color);
        assert_eq!(r, 0);
    }

    #[test]
    fn ycbcr_to_rgb_red() {
        // Approximate red: Y=82, Cb=90, Cr=240
        let color = [0x00, 82, 90, 240];
        let [r, g, b] = ycbcr_to_rgb(&color);
        // R = 82 + 1.402*(240-128) = 82 + 156.9 ≈ 239
        // G = 82 - 0.344*(90-128) - 0.714*(240-128) = 82 + 13.1 - 79.97 ≈ 15
        // B = 82 + 1.772*(90-128) = 82 - 67.3 ≈ 15
        assert!(r > 200, "R should be high for red, got {}", r);
        assert!(g < 30, "G should be low for red, got {}", g);
        assert!(b < 30, "B should be low for red, got {}", b);
    }

    // ── Palette formatting tests ──────────────────────────────────────────

    #[test]
    fn format_palette_basic() {
        // Two colors: black and white (at neutral chroma)
        let palette = vec![
            [0x00, 0, 128, 128],   // Y=0 → RGB (0,0,0)
            [0x00, 255, 128, 128], // Y=255 → RGB (255,255,255)
        ];
        let result = format_palette(&palette);
        let text = String::from_utf8(result).unwrap();
        assert!(
            text.starts_with("palette: "),
            "should start with 'palette: '"
        );
        assert!(text.ends_with('\n'), "should end with newline");
        // First color: 000000
        assert!(
            text.contains("000000"),
            "black should be 000000, got: {}",
            text
        );
        // Second color: ffffff
        assert!(
            text.contains("ffffff"),
            "white should be ffffff, got: {}",
            text
        );
    }

    #[test]
    fn format_palette_16_colors() {
        let palette: Vec<[u8; 4]> = (0..16).map(|i| [0x00, (i * 16) as u8, 128, 128]).collect();
        let result = format_palette(&palette);
        let text = String::from_utf8(result).unwrap();
        // Should have exactly 15 commas (16 colors separated by ", ")
        let comma_count = text.matches(", ").count();
        assert_eq!(
            comma_count, 15,
            "16 colors should have 15 separators, got {}",
            comma_count
        );
    }

    #[test]
    fn format_palette_hex_format() {
        // Y=128, Cb=128, Cr=128 → R=128, G=128, B=128 → "808080"
        let palette = vec![[0x00, 128, 128, 128]];
        let result = format_palette(&palette);
        let text = String::from_utf8(result).unwrap();
        assert_eq!(text, "palette: 808080\n");
    }
}

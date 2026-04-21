//! DVD bitmap subtitle (VobSub) parser.
//!
//! DVD subtitles are carried in PS private stream 1 with sub-stream IDs 0x20-0x3F.
//! Each subtitle display set may span multiple PES packets, but at the MKV level
//! we pass through the raw VobSub packets as-is — the container wraps them.
//!
//! For MKV: codec ID "S_VOBSUB".
//! All frames are keyframes (each is a complete bitmap).

use super::{pts_to_ns, CodecParser, Frame, PesPacket};

pub struct DvdSubParser {
    /// Pre-formatted VobSub .idx palette header for codec_private.
    codec_data: Option<Vec<u8>>,
}

impl DvdSubParser {
    pub fn new(codec_data: Option<Vec<u8>>) -> Self {
        Self { codec_data }
    }
}

impl CodecParser for DvdSubParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: pes.data.clone(),
        }]
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
        let pes = make_pes(vec![0x01, 0x02], None);
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 0);
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

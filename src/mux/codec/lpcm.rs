//! BD/DVD LPCM (Linear PCM) audio parser.
//!
//! Both origins carry a per-PES audio header this parser consumes: 4 bytes on BD-TS,
//! 3 on DVD-PS (`PsDemuxer` strips only the sub_id/frames/pointer bytes). Output is
//! plain interleaved big-endian PCM for MKV "A_PCM/INT/BIG" (16-bit, or 24-bit for
//! 20/24-bit sources) in WAVEFORMATEXTENSIBLE channel order. Layouts follow ffmpeg's
//! pcm-bluray.c (pad channel, LFE/surround remap) and pcm-dvd.c (20/24-bit groups).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// BD LPCM header: payload_size(2), channel_assignment|rate, bits|start_flag.
const BD_LPCM_HEADER_SIZE: usize = 4;
/// DVD LPCM audio header: emphasis|frame#, quant|rate|channels-1, dynamic range.
const DVD_LPCM_HEADER_SIZE: usize = 3;

/// BD channel_assignment -> (channels, destination index of each source channel).
fn bd_layout(assign: u8) -> Option<(usize, &'static [usize])> {
    const ID: [usize; 5] = [0, 1, 2, 3, 4];
    Some(match assign {
        1 => (1, &ID[..1]),
        3 => (2, &ID[..2]),
        4 | 5 => (3, &ID[..3]),
        6 | 7 => (4, &ID[..4]),
        8 => (5, &ID[..5]),
        // L R C Ls Rs LFE -> L R C LFE Ls Rs
        9 => (6, &[0, 1, 2, 4, 5, 3]),
        // L R C Ls Lrs Rrs Rs -> L R C Lrs Rrs Ls Rs
        10 => (7, &[0, 1, 2, 5, 3, 4, 6]),
        // L R C Ls Lrs Rrs Rs LFE -> L R C LFE Lrs Rrs Ls Rs
        11 => (8, &[0, 1, 2, 6, 4, 5, 7, 3]),
        _ => return None,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    /// BD: coded channels padded to even; `bytes` per sample (2 or 3).
    Bd {
        channels: usize,
        map: &'static [usize],
        bytes: usize,
        rate: u32,
    },
    /// DVD: `bits` 16/20/24; 20/24-bit samples are stored in groups.
    Dvd {
        channels: usize,
        bits: u8,
        rate: u32,
    },
}

impl Format {
    fn bd(h: &[u8]) -> Option<Self> {
        let (channels, map) = bd_layout(h[2] >> 4)?;
        let rate = match h[2] & 0x0F {
            1 => 48_000,
            4 => 96_000,
            5 => 192_000,
            _ => return None,
        };
        let bytes = match h[3] >> 6 {
            1 => 2,
            2 | 3 => 3,
            _ => return None,
        };
        Some(Format::Bd {
            channels,
            map,
            bytes,
            rate,
        })
    }

    fn dvd(h: &[u8]) -> Option<Self> {
        let bits = match (h[1] >> 6) & 3 {
            0 => 16,
            1 => 20,
            2 => 24,
            _ => return None,
        };
        let rate = [48_000, 96_000, 44_100, 32_000][usize::from((h[1] >> 4) & 3)];
        let channels = 1 + usize::from(h[1] & 7);
        Some(Format::Dvd {
            channels,
            bits,
            rate,
        })
    }

    /// Input bytes per unit, and output bytes per interleaved sample.
    fn unit_and_out(self) -> (usize, usize) {
        match self {
            Format::Bd {
                channels, bytes, ..
            } => ((channels + (channels & 1)) * bytes, bytes),
            Format::Dvd {
                channels: c,
                bits: 16,
                ..
            } => (c * 2, 2),
            Format::Dvd { channels, bits, .. } => {
                let g = if channels == 1 { 2 } else { 4 };
                (g * 2 + if bits == 24 { g } else { g / 2 }, 3)
            }
        }
    }

    fn channels_rate(self) -> (usize, u32) {
        match self {
            Format::Bd { channels, rate, .. } | Format::Dvd { channels, rate, .. } => {
                (channels, rate)
            }
        }
    }

    /// Convert one whole input unit to output PCM.
    fn convert(self, u: &[u8], out: &mut Vec<u8>) {
        match self {
            Format::Bd {
                channels,
                map,
                bytes,
                ..
            } => {
                let base = out.len();
                out.resize(base + channels * bytes, 0);
                for (src, &dst) in map.iter().enumerate().take(channels) {
                    let (s, d) = (src * bytes, base + dst * bytes);
                    out[d..d + bytes].copy_from_slice(&u[s..s + bytes]);
                }
            }
            Format::Dvd { bits: 16, .. } => out.extend_from_slice(u),
            Format::Dvd { channels, bits, .. } => {
                // A group is g MSB16 words, then their low bits (a byte each at
                // 24-bit, a nibble each at 20-bit).
                let g = if channels == 1 { 2 } else { 4 };
                let lo = &u[g * 2..];
                for k in 0..g {
                    let low = if bits == 24 {
                        lo[k]
                    } else if k % 2 == 0 {
                        lo[k / 2] & 0xF0
                    } else {
                        lo[k / 2] << 4
                    };
                    out.extend_from_slice(&[u[k * 2], u[k * 2 + 1], low]);
                }
            }
        }
    }
}

pub struct LpcmParser {
    /// `true` for BD-TS (4-byte header), `false` for DVD-PS (3-byte header).
    bd: bool,
    /// Format of the previous PES; a change drops `carry`.
    format: Option<Format>,
    /// Trailing partial unit (DVD packs may split a sample group across PES).
    carry: Vec<u8>,
    /// Timeline anchor: last PES PTS (ns) and sample frames emitted since it.
    /// A PES with no PTS is stamped at the anchor plus the emitted duration,
    /// so it neither resets to 0 nor duplicates the previous timestamp.
    anchor_ns: i64,
    samples_since_anchor: u64,
}

impl Default for LpcmParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LpcmParser {
    fn with(bd: bool) -> Self {
        Self {
            bd,
            format: None,
            carry: Vec::new(),
            anchor_ns: 0,
            samples_since_anchor: 0,
        }
    }

    /// BD-TS LPCM parser (4-byte BD LPCM header per PES).
    pub fn new() -> Self {
        Self::with(true)
    }

    /// DVD-PS LPCM parser (3-byte DVD audio header per PES).
    pub fn new_dvd() -> Self {
        Self::with(false)
    }

    fn predicted_pts(&self) -> i64 {
        let rate = self.format.map_or(0, |f| f.channels_rate().1);
        if rate == 0 {
            return self.anchor_ns;
        }
        let ns = u128::from(self.samples_since_anchor) * 1_000_000_000 / u128::from(rate);
        self.anchor_ns
            .saturating_add(i64::try_from(ns).unwrap_or(i64::MAX))
    }
}

impl CodecParser for LpcmParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let hdr = if self.bd {
            BD_LPCM_HEADER_SIZE
        } else {
            DVD_LPCM_HEADER_SIZE
        };
        // If the PES is too short to contain header + data, return nothing.
        if pes.data.len() <= hdr {
            return Vec::new();
        }
        let format = if self.bd {
            Format::bd(&pes.data)
        } else {
            Format::dvd(&pes.data)
        };
        let pts_ns = match pes.pts.or(pes.dts).map(pts_to_ns) {
            Some(p) => {
                self.anchor_ns = p;
                self.samples_since_anchor = 0;
                p
            }
            None => self.predicted_pts(),
        };
        if format != self.format || pes.discontinuity {
            self.carry.clear();
        }
        if format != self.format {
            // Rate may differ; restart the prediction from this PES.
            self.anchor_ns = pts_ns;
            self.samples_since_anchor = 0;
        }
        self.format = format;
        let payload = &pes.data[hdr..];
        let data = match format {
            // Reserved/unknown header: pass the payload through unchanged.
            None => payload.to_vec(),
            Some(f) => {
                let (unit, out_bytes) = f.unit_and_out();
                let (channels, _) = f.channels_rate();
                self.carry.extend_from_slice(payload);
                let whole = self.carry.len() - self.carry.len() % unit;
                let mut out = Vec::with_capacity(whole / unit * channels * out_bytes);
                for u in self.carry[..whole].chunks_exact(unit) {
                    f.convert(u, &mut out);
                }
                self.carry.drain(..whole);
                // BD PES hold whole sample frames (ffmpeg drops a remainder); only DVD
                // groups legitimately straddle PES.
                if self.bd {
                    self.carry.clear();
                }
                self.samples_since_anchor += (out.len() / (channels * out_bytes)) as u64;
                out
            }
        };
        if data.is_empty() {
            return Vec::new();
        }
        vec![Frame {
            discontinuity: pes.discontinuity,
            coding: None,
            source: super::pesbuf::PesFacts::of(pes).source,
            pts_ns,
            keyframe: true,
            data,
            duration_ns: None,
        }]
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
            source: None,
            pid: 0x1100,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    #[test]
    fn header_skip_extracts_pcm_data() {
        let mut parser = LpcmParser::new();
        // 4-byte LPCM header + 6 bytes of PCM data
        let header = vec![0x00, 0x01, 0x00, 0b1001_0001]; // frame#=1, quant=24bit, rate=48k, ch=1
        let pcm_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let mut pes_data = header;
        pes_data.extend_from_slice(&pcm_data);

        let pes = make_pes(pes_data, Some(90000));
        let frames = parser.parse(&pes);

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcm_data);
        assert_eq!(frames[0].pts_ns, 1_000_000_000); // 90000 ticks = 1 second
    }

    #[test]
    fn bd_lpcm_strips_4_byte_header() {
        // BD-TS LPCM: the 4-byte BD header is part of the ES payload and must
        // be stripped, leaving exactly the PCM bytes.
        let mut parser = LpcmParser::new();
        let header = vec![0x00, 0x01, 0x00, 0b1001_0001];
        let pcm = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let mut data = header;
        data.extend_from_slice(&pcm);

        let frames = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, pcm, "BD must strip exactly 4 header bytes");
    }

    #[test]
    fn dvd_lpcm_preserves_all_pcm_bytes() {
        // DVD-PS LPCM 16-bit: only the 3-byte audio header is stripped — applying
        // the BD 4-byte strip would drop PCM bytes per PES and drift the audio.
        let mut parser = LpcmParser::new_dvd();
        let pcm = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x01, 0x02];
        let frames = parser.parse(&make_pes(dvd(0x01, &pcm), Some(90000)));

        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].data, pcm,
            "DVD must preserve every PCM byte — no second strip"
        );
        assert_eq!(frames[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn dvd_lpcm_emits_short_payload_bd_would_drop() {
        // A 4-byte DVD PES (3-byte header + 1 byte). The BD parser drops <= 4
        // bytes as "header only"; the DVD parser's header is 3 bytes.
        let mut bd = LpcmParser::new();
        let mut dvd = LpcmParser::new_dvd();
        let pcm = vec![0x00, 0xC0, 0x80, 0xDD]; // reserved quant -> passthrough

        assert!(
            bd.parse(&make_pes(pcm.clone(), Some(0))).is_empty(),
            "BD treats 4 bytes as header-only"
        );
        let frames = dvd.parse(&make_pes(pcm.clone(), Some(0)));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, vec![0xDD]);
    }

    #[test]
    fn always_keyframe() {
        let mut parser = LpcmParser::new();
        for i in 0..5u8 {
            let data = vec![0x00, 0x00, 0x00, 0x00, i, i + 1];
            let pes = make_pes(data, Some(90000 * i as i64));
            let frames = parser.parse(&pes);
            assert_eq!(frames.len(), 1);
            assert!(frames[0].keyframe, "LPCM frames should always be keyframes");
        }
    }

    #[test]
    fn empty_pes_returns_no_frames() {
        let mut parser = LpcmParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn header_only_pes_returns_no_frames() {
        let mut parser = LpcmParser::new();
        // Exactly 4 bytes = header only, no PCM data
        let pes = make_pes(vec![0x00, 0x01, 0x00, 0x00], Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn codec_private_none() {
        let parser = LpcmParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn pts_conversion() {
        let mut parser = LpcmParser::new();
        // PTS = 0 should give pts_ns = 0
        let pes = make_pes(vec![0; 8], Some(0));
        let frames = parser.parse(&pes);
        assert_eq!(frames[0].pts_ns, 0);

        // No PTS should default to 0
        let pes_no_pts = make_pes(vec![0; 8], None);
        let frames = parser.parse(&pes_no_pts);
        assert_eq!(frames[0].pts_ns, 0);
    }

    // --- BD strip offset boundary ---

    #[test]
    fn bd_five_bytes_yields_one_pcm_byte() {
        // BD strips exactly BD_LPCM_HEADER_SIZE (4). The guard is
        // `data.len() <= offset` (drop), so 5 bytes → 1 PCM byte emitted, not 0.
        let mut parser = LpcmParser::new();
        let f = parser.parse(&make_pes(vec![0x00, 0x01, 0x00, 0x91, 0xAB], Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, vec![0xAB], "5 BD bytes → 1 PCM byte");
    }

    #[test]
    fn bd_three_bytes_dropped() {
        // Fewer than the 4-byte header → dropped, no panic / no underflow slice.
        let mut parser = LpcmParser::new();
        assert!(
            parser
                .parse(&make_pes(vec![0x00, 0x01, 0x00], Some(0)))
                .is_empty()
        );
    }

    // --- DVD strips nothing ---

    #[test]
    fn dvd_one_byte_payload_emitted() {
        // DVD strips only its 3-byte header, so one trailing byte is emitted
        // (mono 16-bit with ch bits 0 would hold it; use a reserved quant -> passthrough).
        let mut parser = LpcmParser::new_dvd();
        let f = parser.parse(&make_pes(vec![0x00, 0xC0, 0x80, 0xAB], Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, vec![0xAB]);
    }

    #[test]
    fn dvd_empty_payload_dropped() {
        // DVD with a header-only payload → dropped.
        let mut parser = LpcmParser::new_dvd();
        assert!(parser.parse(&make_pes(Vec::new(), Some(0))).is_empty());
        assert!(
            parser
                .parse(&make_pes(vec![0, 1, 0x80], Some(0)))
                .is_empty()
        );
    }

    #[test]
    fn bd_default_constructor_strips_header() {
        // Default::default() must build the BD (strip) variant, matching new().
        let mut parser = LpcmParser::default();
        let header = vec![0x00, 0x01, 0x00, 0x91];
        let pcm = vec![0x11, 0x22, 0x33, 0x44];
        let mut data = header;
        data.extend_from_slice(&pcm);
        let f = parser.parse(&make_pes(data, Some(0)));
        assert_eq!(f[0].data, pcm, "default = BD variant, strips 4 bytes");
    }

    #[test]
    fn lpcm_no_pts_defaults_zero_dvd() {
        // DVD variant with no PTS and no prior timestamp → pts_ns 0.
        let mut parser = LpcmParser::new_dvd();
        let f = parser.parse(&make_pes(dvd(0x01, &[0xAA, 0xBB, 0xCC, 0xDD]), None));
        assert_eq!(f[0].pts_ns, 0);
    }

    #[test]
    fn pts_less_pes_carries_last_timestamp_and_propagates_discontinuity() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must continue
        // the timeline — not reset to 0 nor duplicate the last PTS — and carry the PES
        // discontinuity flag. Header: stereo 48 kHz 16-bit, so 4 bytes = 1 sample.
        let mut parser = LpcmParser::new();
        let header = vec![0x00, 0x04, 0x31, 0x40];
        let pcm = vec![0x11, 0x22, 0x33, 0x44];
        let mut data = header;
        data.extend_from_slice(&pcm);
        // Prime last_pts_ns with a real timestamp.
        parser.parse(&make_pes(data.clone(), Some(90_000)));

        // Next PES has no PTS and is flagged discontinuous.
        let mut pes = make_pes(data, None);
        pes.discontinuity = true;
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].pts_ns, 1_000_020_833,
            "advanced by one 48 kHz sample, not reset to 0 or duplicated"
        );
        assert!(
            frames[0].discontinuity,
            "PES discontinuity propagates into the access unit"
        );
    }

    // --- Layout conformance (cross-checked against ffmpeg pcm-bluray.c / pcm-dvd.c) ---

    fn bd(ch_assign: u8, bits_code: u8, pcm: &[u8]) -> Vec<u8> {
        // payload_size(2), channel_assignment<<4 | 48k(1), bits<<6.
        let mut v = vec![0x00, 0x00, (ch_assign << 4) | 0x01, bits_code << 6];
        v.extend_from_slice(pcm);
        v
    }

    #[test]
    fn bd_mono_drops_the_pad_channel() {
        // Mono is coded as 2 channels; the second is padding.
        let mut p = LpcmParser::new();
        let f = p.parse(&make_pes(
            bd(1, 1, &[0x11, 0x12, 0, 0, 0x21, 0x22, 0, 0]),
            Some(0),
        ));
        assert_eq!(f[0].data, vec![0x11, 0x12, 0x21, 0x22]);
    }

    #[test]
    fn bd_odd_channel_24bit_drops_the_pad_channel() {
        // 3/0 (L R C) at 24-bit: 4 coded channels, last is padding.
        let mut p = LpcmParser::new();
        let src = [1, 1, 1, 2, 2, 2, 3, 3, 3, 9, 9, 9];
        let f = p.parse(&make_pes(bd(4, 3, &src), Some(0)));
        assert_eq!(f[0].data, vec![1, 1, 1, 2, 2, 2, 3, 3, 3]);
    }

    #[test]
    fn bd_51_moves_lfe_to_wave_order() {
        // BD order L R C Ls Rs LFE -> Matroska/WAVEFORMATEXTENSIBLE L R C LFE Ls Rs.
        let mut p = LpcmParser::new();
        let src = [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5];
        let f = p.parse(&make_pes(bd(9, 1, &src), Some(0)));
        assert_eq!(f[0].data, vec![0, 0, 1, 1, 2, 2, 5, 5, 3, 3, 4, 4]);
    }

    #[test]
    fn bd_71_reorders_to_wave_order() {
        // BD L R C Ls Lrs Rrs Rs LFE -> L R C LFE Lrs Rrs Ls Rs (ffmpeg 7POINT1 mapping).
        let mut p = LpcmParser::new();
        let src: Vec<u8> = (0..8u8).flat_map(|c| [c; 3]).collect();
        let f = p.parse(&make_pes(bd(11, 3, &src), Some(0)));
        let want: Vec<u8> = [0u8, 1, 2, 7, 4, 5, 3, 6]
            .iter()
            .flat_map(|&c| [c; 3])
            .collect();
        assert_eq!(f[0].data, want);
    }

    #[test]
    fn bd_70_reorders_and_drops_pad() {
        // BD L R C Ls Lrs Rrs Rs <pad> -> L R C Lrs Rrs Ls Rs.
        let mut p = LpcmParser::new();
        let src: Vec<u8> = (0..8u8).flat_map(|c| [c; 2]).collect();
        let f = p.parse(&make_pes(bd(10, 1, &src), Some(0)));
        let want: Vec<u8> = [0u8, 1, 2, 4, 5, 3, 6]
            .iter()
            .flat_map(|&c| [c; 2])
            .collect();
        assert_eq!(f[0].data, want);
    }

    fn dvd(quant_freq_ch: u8, pcm: &[u8]) -> Vec<u8> {
        // DVD LPCM audio header: emphasis/frame#, quant|freq|ch-1, dynamic range.
        let mut v = vec![0x00, quant_freq_ch, 0x80];
        v.extend_from_slice(pcm);
        v
    }

    #[test]
    fn dvd_16bit_strips_audio_header() {
        let mut p = LpcmParser::new_dvd();
        let f = p.parse(&make_pes(dvd(0x01, &[1, 2, 3, 4]), Some(0)));
        assert_eq!(f[0].data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn dvd_24bit_stereo_unpacks_grouped_samples() {
        // Group = MSB16 of L0 R0 L1 R1, then their low bytes.
        let mut p = LpcmParser::new_dvd();
        let src = [
            0xA0, 0xA1, 0xB0, 0xB1, 0xC0, 0xC1, 0xD0, 0xD1, 0xA2, 0xB2, 0xC2, 0xD2,
        ];
        let f = p.parse(&make_pes(dvd(0x81, &src), Some(0)));
        assert_eq!(
            f[0].data,
            vec![
                0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2, 0xC0, 0xC1, 0xC2, 0xD0, 0xD1, 0xD2
            ]
        );
    }

    #[test]
    fn dvd_20bit_stereo_unpacks_nibbles() {
        let mut p = LpcmParser::new_dvd();
        let src = [0xA0, 0xA1, 0xB0, 0xB1, 0xC0, 0xC1, 0xD0, 0xD1, 0x12, 0x34];
        let f = p.parse(&make_pes(dvd(0x41, &src), Some(0)));
        assert_eq!(
            f[0].data,
            vec![
                0xA0, 0xA1, 0x10, 0xB0, 0xB1, 0x20, 0xC0, 0xC1, 0x30, 0xD0, 0xD1, 0x40
            ]
        );
    }

    #[test]
    fn dvd_24bit_mono_uses_two_sample_groups() {
        let mut p = LpcmParser::new_dvd();
        let src = [0xA0, 0xA1, 0xB0, 0xB1, 0xA2, 0xB2];
        let f = p.parse(&make_pes(dvd(0x80, &src), Some(0)));
        assert_eq!(f[0].data, vec![0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2]);
    }

    #[test]
    fn dvd_24bit_group_split_across_pes_is_carried() {
        let mut p = LpcmParser::new_dvd();
        let src = [
            0xA0, 0xA1, 0xB0, 0xB1, 0xC0, 0xC1, 0xD0, 0xD1, 0xA2, 0xB2, 0xC2, 0xD2,
        ];
        let a = p.parse(&make_pes(dvd(0x81, &src[..5]), Some(0)));
        assert!(
            a.iter().all(|f| f.data.is_empty()),
            "partial group held back"
        );
        let b = p.parse(&make_pes(dvd(0x81, &src[5..]), Some(90)));
        let got: Vec<u8> = a
            .iter()
            .chain(b.iter())
            .flat_map(|f| f.data.clone())
            .collect();
        assert_eq!(
            got,
            vec![
                0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2, 0xC0, 0xC1, 0xC2, 0xD0, 0xD1, 0xD2
            ]
        );
    }

    #[test]
    fn pts_less_pes_advances_by_previous_duration() {
        // 480 stereo 16-bit samples at 48 kHz = 10 ms; the PTS-less successor
        // must follow at +10 ms, not duplicate the previous timestamp.
        let mut p = LpcmParser::new();
        let pcm = vec![0u8; 480 * 4];
        p.parse(&make_pes(bd(3, 1, &pcm), Some(90_000)));
        let f = p.parse(&make_pes(bd(3, 1, &pcm), None));
        assert_eq!(f[0].pts_ns, 1_010_000_000);
    }

    // The text-based guard in codec/mod.rs can't see writes via `facts.source`;
    // only a runtime check proves an emitted frame carries the byte it came
    // from, needed for placing a track by byte instead of timestamp inference.
    #[test]
    fn an_emitted_frame_carries_the_packets_source_offset() {
        let mut parser = LpcmParser::new();
        let mut data = vec![0x00, 0x01, 0x00, 0b1001_0001];
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]);
        let mut pes = make_pes(data, Some(90_000));
        pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
        let frames = parser.parse(&pes);
        assert!(!frames.is_empty(), "the frame is emitted");
        assert_eq!(frames[0].source.map(|s| s.byte), Some(7_777));
    }
}

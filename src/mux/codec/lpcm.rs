//! BD/DVD LPCM (Linear PCM) audio parser.
//!
//! Both origins carry a per-PES audio header this parser consumes: 4 bytes on BD-TS,
//! 3 on DVD-PS (`PsDemuxer` strips only the sub_id/frames/pointer bytes). Output is
//! interleaved big-endian PCM in WAVE_FORMAT_EXTENSIBLE channel order at the source
//! depth: 16-bit stays 16-bit, 20- and 24-bit sources output 24-bit. The depth rides
//! in `codec_private` (`output_depth`) so "A_PCM/INT/BIG" BitDepth matches. Layouts follow ffmpeg pcm-bluray.c (pad channel, LFE/surround
//! remap) and pcm-dvd.c (20/24-bit sample groups and blocks).

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

fn bd_rate(code: u8) -> Option<u32> {
    match code {
        1 => Some(48_000),
        4 => Some(96_000),
        5 => Some(192_000),
        _ => None,
    }
}

/// DVD 20/24-bit block (ffmpeg pcm-dvd.c): (groups, samples per group, sample frames).
/// A group is `g` MSB16 words then their low bits; a block is whole sample frames.
fn dvd_block(channels: usize) -> (usize, usize, usize) {
    let g = if channels == 1 { 2 } else { 4 };
    let frames = match channels {
        1 => 4,
        2 => 2,
        4 | 8 => 1,
        _ => 4,
    };
    (frames * channels / g, g, frames)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    /// BD: coded channels padded to even; `bytes` per sample (2 = 16-bit, 3 = 24-bit).
    Bd {
        channels: usize,
        map: &'static [usize],
        bytes: usize,
        rate: u32,
    },
    /// DVD: `bits` 16/20/24.
    Dvd {
        channels: usize,
        bits: u8,
        rate: u32,
    },
}

impl Format {
    fn bd(h: &[u8]) -> Option<Self> {
        let (channels, map) = bd_layout(h[2] >> 4)?;
        let rate = bd_rate(h[2] & 0x0F)?;
        // 20-bit (code 2) sits in a 24-bit container, as 1.7.7 passed it: output 24-bit.
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

    /// (input bytes, sample frames) of one whole conversion unit.
    fn unit(self) -> (usize, usize) {
        match self {
            Format::Bd {
                channels, bytes, ..
            } => ((channels + (channels & 1)) * bytes, 1),
            Format::Dvd {
                channels: c,
                bits: 16,
                ..
            } => (c * 2, 1),
            Format::Dvd { channels, bits, .. } => {
                let (groups, g, frames) = dvd_block(channels);
                let low = if bits == 24 { g } else { g / 2 };
                (groups * (g * 2 + low), frames)
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

    /// Bytes per output sample: 2 for 16-bit sources, else 3 (20-bit pads to 24).
    fn out_bytes(self) -> usize {
        match self {
            Format::Bd { bytes, .. } => bytes,
            Format::Dvd { bits: 16, .. } => 2,
            Format::Dvd { .. } => 3,
        }
    }

    /// Convert one whole unit to output PCM.
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
                for (src, &dst) in map.iter().enumerate() {
                    let (s, d) = (src * bytes, base + dst * bytes);
                    out[d..d + bytes].copy_from_slice(&u[s..s + bytes]);
                }
            }
            Format::Dvd { bits: 16, .. } => out.extend_from_slice(u),
            Format::Dvd { channels, bits, .. } => {
                let (_, g, _) = dvd_block(channels);
                let size = g * 2 + if bits == 24 { g } else { g / 2 };
                for grp in u.chunks_exact(size) {
                    let lo = &grp[g * 2..];
                    for k in 0..g {
                        let low = if bits == 24 {
                            lo[k]
                        } else if k % 2 == 0 {
                            lo[k / 2] & 0xF0
                        } else {
                            lo[k / 2] << 4
                        };
                        out.extend_from_slice(&[grp[k * 2], grp[k * 2 + 1], low]);
                    }
                }
            }
        }
    }
}

fn samples_to_ns(samples: u64, rate: u32) -> i64 {
    if rate == 0 {
        return 0;
    }
    i64::try_from(u128::from(samples) * 1_000_000_000 / u128::from(rate)).unwrap_or(i64::MAX)
}

pub struct LpcmParser {
    /// `true` for BD-TS (4-byte header), `false` for DVD-PS (3-byte header).
    bd: bool,
    /// Format of the previous PES; a change drops `carry`.
    format: Option<Format>,
    /// Trailing partial unit (DVD packs may split a sample block across PES).
    carry: Vec<u8>,
    /// Timeline anchor (ns) and sample frames emitted since it. A PES with no PTS
    /// is stamped at the anchor plus the emitted duration — never 0 or a duplicate.
    anchor_ns: i64,
    samples_since_anchor: u64,
    /// Last BD header byte 2 (channel_assignment|rate), reported as codec_private so
    /// an M2TS re-mux keeps the exact layout (e.g. 2/2 vs 3/1).
    bd_layout_byte: Option<u8>,
    /// Output sample depth of the first packet (16 or 24), reported as codec_private.
    depth: Option<u8>,
    /// Packets refused for a reserved header code (counted, never poisoning).
    tally: super::dropgate::DropTally,
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
            bd_layout_byte: None,
            depth: None,
            tally: super::dropgate::DropTally::new("lpcm"),
        }
    }

    /// Packets dropped for a reserved or unsupported LPCM header.
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
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
        self.anchor_ns
            .saturating_add(samples_to_ns(self.samples_since_anchor, rate))
    }
}

impl CodecParser for LpcmParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let hdr = if self.bd {
            BD_LPCM_HEADER_SIZE
        } else {
            DVD_LPCM_HEADER_SIZE
        };
        if pes.data.len() <= hdr {
            return Vec::new();
        }
        let parsed = if self.bd {
            Format::bd(&pes.data)
        } else {
            Format::dvd(&pes.data)
        };
        // Reserved header codes: drop the packet, as ffmpeg does (INVALIDDATA).
        let Some(format) = parsed else {
            if self.tally.dropped_frames() == 0 {
                tracing::warn!(target: "mux", "lpcm: reserved/unsupported header; dropping packets");
            }
            let pts = pes.pts.or(pes.dts).map_or(0, pts_to_ns);
            self.tally
                .record_collateral_drop(pts, 0, pes.data.len(), "reserved-header");
            return Vec::new();
        };
        if self.bd {
            self.bd_layout_byte = Some(pes.data[2]);
        }
        let ob = format.out_bytes();
        self.depth.get_or_insert(ob as u8 * 8);
        let predicted = self.predicted_pts();
        if Some(format) != self.format || pes.discontinuity {
            self.carry.clear();
        }
        self.format = Some(format);
        let (unit, unit_frames) = format.unit();
        let (channels, rate) = format.channels_rate();
        // PTS belongs to the first unit STARTING here, so a carried unit starts one earlier.
        // DVD first_access_unit_pointer is ignored (as ffmpeg/VLC): <= 1 audio frame late.
        let lead = if self.carry.is_empty() {
            0
        } else {
            unit_frames
        };
        self.anchor_ns = match pes.pts.or(pes.dts).map(pts_to_ns) {
            Some(p) => p.saturating_sub(samples_to_ns(lead as u64, rate)),
            None => predicted,
        };
        self.samples_since_anchor = 0;
        let pts_ns = self.anchor_ns;

        self.carry.extend_from_slice(&pes.data[hdr..]);
        let whole = self.carry.len() - self.carry.len() % unit;
        let mut data = Vec::with_capacity(whole / unit * unit_frames * channels * ob);
        for u in self.carry[..whole].chunks_exact(unit) {
            format.convert(u, &mut data);
        }
        self.carry.drain(..whole);
        // BD PES hold whole sample frames (ffmpeg drops a remainder); only DVD
        // blocks legitimately straddle PES.
        if self.bd {
            self.carry.clear();
        }
        self.samples_since_anchor = (data.len() / (channels * ob)) as u64;
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
        let depth = self.depth?;
        Some(match self.bd_layout_byte {
            Some(b) => tagged_layout(b, depth),
            None => [DVD_TAG.as_slice(), &[depth]].concat(),
        })
    }
}

/// Tag on the parser's codec_private, so a foreign MKV `A_PCM` CodecPrivate never
/// passes for a BD layout byte. No released build wrote an untagged byte; any
/// untagged value falls back to the count-default layout.
const LAYOUT_TAG: &[u8; 4] = b"BDLP";

/// Tag of a DVD LPCM codec_private (depth only; DVD has no layout byte).
const DVD_TAG: &[u8; 4] = b"DVLP";

/// Tagged codec_private for BD layout byte `b` at output `depth` (16 or 24).
pub(crate) fn tagged_layout(b: u8, depth: u8) -> Vec<u8> {
    [LAYOUT_TAG.as_slice(), &[b, depth]].concat()
}

/// The BD layout byte (channel_assignment|rate) from a tagged LPCM codec_private.
pub(crate) fn layout_byte(cp: &[u8]) -> Option<u8> {
    match cp.strip_prefix(LAYOUT_TAG.as_slice()) {
        Some(&[b, _]) => Some(b),
        _ => None,
    }
}

/// Output sample depth (16 or 24) a track's codec_private declares; 24 when it
/// declares none (e.g. PCM read from Matroska, which is always widened to 24-bit).
pub(crate) fn output_depth(cp: Option<&[u8]>) -> u8 {
    let d = cp.and_then(|c| match c.strip_prefix(LAYOUT_TAG.as_slice()) {
        Some(&[_, d]) => Some(d),
        _ => match c.strip_prefix(DVD_TAG.as_slice()) {
            Some(&[d]) => Some(d),
            _ => None,
        },
    });
    if d == Some(16) { 16 } else { 24 }
}

/// Correct each BD LPCM track's channels/rate (and a default label) from its parser
/// layout byte: playlists label every multi-channel LPCM "5.1".
pub(crate) fn correct_title_layout(title: &mut crate::disc::DiscTitle) {
    use crate::disc::{AudioChannels, SampleRate, Stream};
    for (i, s) in title.streams.iter_mut().enumerate() {
        let Stream::Audio(a) = s else { continue };
        if a.codec != crate::disc::Codec::Lpcm {
            continue;
        }
        let Some(b) = title
            .codec_privates
            .get(i)
            .and_then(|c| c.as_deref())
            .and_then(layout_byte)
        else {
            continue;
        };
        let (Some((count, _)), Some(hz)) = (bd_layout(b >> 4), bd_rate(b & 0x0F)) else {
            continue;
        };
        // AudioChannels has no 3.0 / 7.0 variant: 3ch (3/0, 2/1) reads "2.1", 7ch "6.1".
        let basic = crate::labels::generate_audio_label(&a.codec, &a.channels, a.secondary);
        a.channels = AudioChannels::from_count(count as u8);
        a.sample_rate = SampleRate::from_hz(hz);
        if a.label == basic {
            a.label = crate::labels::generate_audio_label(&a.codec, &a.channels, a.secondary);
        }
    }
}

/// BD LPCM header bytes 2-3 for re-muxing parser output at `depth` to M2TS, or `None`
/// when BD LPCM can't carry it. Reuses the source layout byte (the parser's
/// codec_private) when it agrees; else the ffmpeg pcm-blurayenc.c default for the count.
pub(crate) fn bd_header(
    channels: u8,
    rate_hz: u32,
    source: Option<u8>,
    depth: u8,
) -> Option<[u8; 2]> {
    let bits = if depth == 16 { 1 << 6 } else { 3 << 6 };
    if let Some(b) = source
        && bd_layout(b >> 4).map(|(c, _)| c) == Some(usize::from(channels))
        && bd_rate(b & 0x0F) == Some(rate_hz)
    {
        return Some([b, bits]);
    }
    let assign = match channels {
        1 => 1,
        2 => 3,
        3 => 4,
        4 => 6,
        5 => 8,
        6 => 9,
        7 => 10,
        8 => 11,
        _ => return None,
    };
    let rate = match rate_hz {
        48_000 => 1,
        96_000 => 4,
        192_000 => 5,
        _ => return None,
    };
    Some([(assign << 4) | rate, bits])
}

/// Re-pack WAVE-order PCM (depth from `header`) as BD LPCM PES payloads (header + BD order + pad
/// channel) of 5 ms each (rate/200 samples), as BD authoring does. Returns
/// `(ns offset of the payload, payload)`; a trailing partial frame is dropped.
pub(crate) fn bd_payloads(pcm: &[u8], header: [u8; 2]) -> Vec<(i64, Vec<u8>)> {
    let (Some((channels, map)), Some(rate)) =
        (bd_layout(header[0] >> 4), bd_rate(header[0] & 0x0F))
    else {
        return Vec::new();
    };
    let w = if header[1] >> 6 == 1 { 2 } else { 3 };
    let coded = (channels + (channels & 1)) * w;
    let per_pes = rate as usize / 200;
    let mut out = Vec::new();
    for (n, chunk) in pcm.chunks(per_pes * channels * w).enumerate() {
        let frames = chunk.len() / (channels * w);
        if frames == 0 {
            continue;
        }
        let size = (frames * coded) as u16;
        let mut p = Vec::with_capacity(BD_LPCM_HEADER_SIZE + frames * coded);
        p.extend_from_slice(&size.to_be_bytes());
        p.extend_from_slice(&header);
        for f in chunk.chunks_exact(channels * w) {
            let base = p.len();
            p.resize(base + coded, 0);
            for (src, &dst) in map.iter().enumerate() {
                p[base + src * w..base + src * w + w].copy_from_slice(&f[dst * w..dst * w + w]);
            }
        }
        out.push((samples_to_ns((n * per_pes) as u64, rate), p));
    }
    out
}

// ── DVD LPCM re-pack (the `mpg://` sink; mpg-output-design v5 §1.1 G8) ─────────
// The exact inverse of `Format::Dvd::convert`, so DVD → IR → DVD is byte-identical.

/// The smallest DVD LPCM quantization (16, 20 or 24 bits) that holds every sample of the
/// 24-bit IR PCM `ir` exactly: the IR keeps no source depth, and a 16-bit source's IR has
/// every low byte zero.
pub(crate) fn dvd_bits_needed(ir: &[u8]) -> u8 {
    let low = ir.as_chunks::<3>().0.iter().fold(0u8, |acc, s| acc | s[2]);
    match low {
        0 => 16,
        l if l & 0x0F == 0 => 20,
        _ => 24,
    }
}

/// Sample frames in one DVD LPCM packing unit (a 20/24-bit block, or one 16-bit frame).
pub(crate) fn dvd_unit_frames(channels: usize, bits: u8) -> usize {
    if bits == 16 { 1 } else { dvd_block(channels).2 }
}

/// DVD LPCM bytes for 24-bit IR PCM `ir` (a whole number of units) at `bits`.
pub(crate) fn dvd_pack(ir: &[u8], channels: usize, bits: u8, out: &mut Vec<u8>) {
    let samples = ir.as_chunks::<3>().0;
    if bits == 16 {
        for s in samples {
            out.extend_from_slice(&s[..2]);
        }
        return;
    }
    // A group is `g` MSB16 words, then their low bits (a byte each at 24-bit, a nibble each
    // at 20-bit), as `Format::Dvd::convert` reads it.
    let (_, g, _) = dvd_block(channels);
    for grp in samples.chunks_exact(g) {
        for s in grp {
            out.extend_from_slice(&s[..2]);
        }
        if bits == 24 {
            out.extend(grp.iter().map(|s| s[2]));
        } else {
            out.extend(
                grp.as_chunks::<2>()
                    .0
                    .iter()
                    .map(|p| (p[0][2] & 0xF0) | (p[1][2] >> 4)),
            );
        }
    }
}

/// The 3-byte DVD LPCM audio header (frame number, quantization|rate|channels−1,
/// dynamic range), or `None` for a rate/channel count it cannot state.
pub(crate) fn dvd_header(channels: usize, rate: u32, bits: u8) -> Option<[u8; 3]> {
    let rate_code = match rate {
        48_000 => 0,
        96_000 => 1,
        _ => return None,
    };
    let quant = match bits {
        16 => 0,
        20 => 1,
        24 => 2,
        _ => return None,
    };
    if !(1..=8).contains(&channels) {
        return None;
    }
    // MS-30 (FFmpeg pcm_dvd): header[0] 0x0c, header[2] 0x80 (no dynamic range control).
    Some([
        0x0C,
        (quant << 6) | (rate_code << 4) | (channels as u8 - 1),
        0x80,
    ])
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

    /// BD PES: payload_size(2), channel_assignment<<4 | 48 kHz, bits_code<<6.
    fn bd(ch_assign: u8, bits_code: u8, pcm: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, (ch_assign << 4) | 0x01, bits_code << 6];
        v.extend_from_slice(pcm);
        v
    }

    /// DVD PES: emphasis/frame#, quant|freq|channels-1, dynamic range.
    fn dvd(quant_freq_ch: u8, pcm: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, quant_freq_ch, 0x80];
        v.extend_from_slice(pcm);
        v
    }

    /// One 16-bit sample per channel, each channel's bytes = its index.
    fn frame16(order: &[u8]) -> Vec<u8> {
        order.iter().flat_map(|&c| [c, c]).collect()
    }

    fn all(frames: &[Frame]) -> Vec<u8> {
        frames.iter().flat_map(|f| f.data.clone()).collect()
    }

    #[test]
    fn reserved_header_code_is_counted() {
        let mut p = LpcmParser::new();
        // bits_code 0 is reserved for BD LPCM.
        assert!(p.parse(&make_pes(bd(3, 0, &[0; 8]), Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn bd_stereo_16bit_keeps_16bit() {
        let mut p = LpcmParser::new();
        let pcm = [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x01, 0x02];
        let f = p.parse(&make_pes(bd(3, 1, &pcm), Some(90_000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, &pcm);
        assert_eq!(f[0].pts_ns, 1_000_000_000);
        assert!(f[0].keyframe);
    }

    #[test]
    fn bd_20bit_keeps_its_packet_as_24bit() {
        let mut p = LpcmParser::new();
        let pcm = [1, 2, 0x30, 4, 5, 0x60];
        let f = p.parse(&make_pes(bd(3, 2, &pcm), Some(0)));
        assert_eq!(f[0].data, pcm);
        assert_eq!(output_depth(p.codec_private().as_deref()), 24);
    }

    #[test]
    fn bd_stereo_24bit_passes_through() {
        let mut p = LpcmParser::default();
        let pcm = [1, 2, 3, 4, 5, 6];
        assert_eq!(p.parse(&make_pes(bd(3, 3, &pcm), Some(0)))[0].data, pcm);
    }

    #[test]
    fn bd_mono_drops_the_pad_channel() {
        // Mono is coded as 2 channels; the second is padding.
        let mut p = LpcmParser::new();
        let f = p.parse(&make_pes(
            bd(1, 1, &[0x11, 0x12, 0, 0, 0x21, 0x22, 0, 0]),
            Some(0),
        ));
        assert_eq!(f[0].data, &[0x11, 0x12, 0x21, 0x22]);
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
    fn bd_51_16bit_moves_lfe_to_wave_order() {
        // BD L R C Ls Rs LFE -> L R C LFE Ls Rs (ffmpeg 5POINT1 mapping).
        let mut p = LpcmParser::new();
        let f = p.parse(&make_pes(bd(9, 1, &frame16(&[0, 1, 2, 3, 4, 5])), Some(0)));
        assert_eq!(f[0].data, frame16(&[0, 1, 2, 5, 3, 4]));
    }

    #[test]
    fn bd_71_16bit_reorders_to_wave_order() {
        // BD L R C Ls Lrs Rrs Rs LFE -> L R C LFE Lrs Rrs Ls Rs (ffmpeg 7POINT1).
        let mut p = LpcmParser::new();
        let f = p.parse(&make_pes(
            bd(11, 1, &frame16(&[0, 1, 2, 3, 4, 5, 6, 7])),
            Some(0),
        ));
        assert_eq!(f[0].data, frame16(&[0, 1, 2, 7, 4, 5, 3, 6]));
    }

    #[test]
    fn bd_71_24bit_reorders_to_wave_order() {
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
        // BD L R C Ls Lrs Rrs Rs <pad> -> L R C Lrs Rrs Ls Rs (ffmpeg 7POINT0).
        let mut p = LpcmParser::new();
        let f = p.parse(&make_pes(
            bd(10, 1, &frame16(&[0, 1, 2, 3, 4, 5, 6, 7])),
            Some(0),
        ));
        assert_eq!(f[0].data, frame16(&[0, 1, 2, 4, 5, 3, 6]));
    }

    #[test]
    fn bd_short_or_header_only_pes_dropped() {
        let mut p = LpcmParser::new();
        assert!(p.parse(&make_pes(Vec::new(), Some(0))).is_empty());
        assert!(
            p.parse(&make_pes(vec![0x00, 0x01, 0x00], Some(0)))
                .is_empty()
        );
        assert!(p.parse(&make_pes(bd(3, 1, &[]), Some(0))).is_empty());
    }

    #[test]
    fn reserved_header_drops_the_packet() {
        // ffmpeg rejects reserved channel/rate/depth codes (INVALIDDATA); so do we.
        let mut p = LpcmParser::new();
        let pcm = [1, 2, 3, 4];
        assert!(
            p.parse(&make_pes(bd(0, 1, &pcm), Some(0))).is_empty(),
            "channels 0"
        );
        assert!(
            p.parse(&make_pes(bd(3, 0, &pcm), Some(0))).is_empty(),
            "depth 0"
        );
        let mut bad_rate = bd(3, 1, &pcm);
        bad_rate[2] = 0x32;
        assert!(p.parse(&make_pes(bad_rate, Some(0))).is_empty(), "rate 2");
        let mut d = LpcmParser::new_dvd();
        assert!(
            d.parse(&make_pes(dvd(0xC1, &pcm), Some(0))).is_empty(),
            "quant 3"
        );
    }

    #[test]
    fn no_pts_before_any_timestamp_starts_at_zero() {
        // No PTS and no earlier timestamp → 0 (both variants).
        let mut p = LpcmParser::new();
        assert_eq!(p.parse(&make_pes(bd(3, 1, &[0; 8]), None))[0].pts_ns, 0);
        let mut d = LpcmParser::new_dvd();
        assert_eq!(d.parse(&make_pes(dvd(0x01, &[0; 4]), None))[0].pts_ns, 0);
    }

    #[test]
    fn pts_less_pes_carries_timeline_and_propagates_discontinuity() {
        // No PTS (legal for audio): continue the timeline — not 0, not a duplicate —
        // and keep the PES discontinuity flag. 4 bytes stereo 16-bit = 1 sample.
        let mut p = LpcmParser::new();
        let data = bd(3, 1, &[0x11, 0x22, 0x33, 0x44]);
        p.parse(&make_pes(data.clone(), Some(90_000)));
        let mut pes = make_pes(data, None);
        pes.discontinuity = true;
        let f = p.parse(&pes);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_020_833, "advanced by one 48 kHz sample");
        assert!(f[0].discontinuity);
    }

    #[test]
    fn pts_less_pes_advances_by_previous_duration() {
        // 480 stereo samples at 48 kHz = 10 ms.
        let mut p = LpcmParser::new();
        let pcm = vec![0u8; 480 * 4];
        p.parse(&make_pes(bd(3, 1, &pcm), Some(90_000)));
        let f = p.parse(&make_pes(bd(3, 1, &pcm), None));
        assert_eq!(f[0].pts_ns, 1_010_000_000);
    }

    #[test]
    fn dvd_16bit_strips_audio_header() {
        let mut p = LpcmParser::new_dvd();
        let pcm = [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0x01, 0x02];
        let f = p.parse(&make_pes(dvd(0x01, &pcm), Some(90_000)));
        assert_eq!(f[0].data, &pcm);
        assert_eq!(f[0].pts_ns, 1_000_000_000);
    }

    #[test]
    fn dvd_header_only_pes_dropped() {
        let mut p = LpcmParser::new_dvd();
        assert!(p.parse(&make_pes(Vec::new(), Some(0))).is_empty());
        assert!(p.parse(&make_pes(vec![0, 1, 0x80], Some(0))).is_empty());
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
    fn dvd_24bit_mono_block_is_two_groups_of_two() {
        let mut p = LpcmParser::new_dvd();
        let src = [1, 1, 2, 2, 0x10, 0x20, 3, 3, 4, 4, 0x30, 0x40];
        let f = p.parse(&make_pes(dvd(0x80, &src), Some(0)));
        let want = vec![1, 1, 0x10, 2, 2, 0x20, 3, 3, 0x30, 4, 4, 0x40];
        assert_eq!(f[0].data, want);
    }

    #[test]
    fn dvd_20bit_mono_block_is_two_groups_of_two() {
        let mut p = LpcmParser::new_dvd();
        let src = [1, 1, 2, 2, 0x12, 3, 3, 4, 4, 0x34];
        let f = p.parse(&make_pes(dvd(0x40, &src), Some(0)));
        let want = vec![1, 1, 0x10, 2, 2, 0x20, 3, 3, 0x30, 4, 4, 0x40];
        assert_eq!(f[0].data, want);
    }

    /// Build a DVD 20/24-bit block for `ch` channels: sample n (interleaved) has
    /// MSB16 = [n, n] and low byte 0xn0 (24-bit) / nibble n (20-bit).
    fn dvd_block(ch: usize, bits: u8, frames: usize) -> (Vec<u8>, Vec<u8>) {
        let n = ch * frames;
        let (mut src, mut want) = (Vec::new(), Vec::new());
        for g in (0..n).step_by(4) {
            for k in g..g + 4 {
                src.extend_from_slice(&[k as u8, k as u8]);
            }
            if bits == 24 {
                src.extend((g..g + 4).map(|k| (k as u8) << 4));
            } else {
                src.push(((g as u8 & 0xF) << 4) | ((g as u8 + 1) & 0xF));
                src.push((((g as u8 + 2) & 0xF) << 4) | ((g as u8 + 3) & 0xF));
            }
        }
        for k in 0..n {
            let low = if bits == 24 {
                (k as u8) << 4
            } else {
                (k as u8 & 0xF) << 4
            };
            want.extend_from_slice(&[k as u8, k as u8, low]);
        }
        (src, want)
    }

    #[test]
    fn dvd_multichannel_blocks_emit_whole_sample_frames_only() {
        // ffmpeg pcm-dvd block: odd/6 channels = `ch` groups (4 frames), 8ch = 2 groups
        // (1 frame). A lone group is not a whole frame and must be held back.
        for (ch, frames) in [(3usize, 4usize), (5, 4), (6, 4), (8, 1)] {
            for (bits, q) in [(24u8, 0x80u8), (20, 0x40)] {
                let (src, want) = dvd_block(ch, bits, frames);
                let group = if bits == 24 { 12 } else { 10 };
                let hdr = q | (ch as u8 - 1);
                let mut p = LpcmParser::new_dvd();
                let a = p.parse(&make_pes(dvd(hdr, &src[..group]), Some(0)));
                assert!(all(&a).is_empty(), "{ch}ch {bits}-bit: one group held");
                let b = p.parse(&make_pes(dvd(hdr, &src[group..]), None));
                let got = all(&b);
                assert_eq!(got, want, "{ch}ch {bits}-bit block");
                assert_eq!(got.len() % (ch * 3), 0, "whole sample frames");
            }
        }
    }

    #[test]
    fn dvd_3ch_pts_less_timeline_counts_whole_frames() {
        // 3ch 24-bit block = 4 sample frames; two blocks per PES = 8 frames, so the
        // PTS-less successor sits exactly 8 samples later (no flooring drift).
        let (src, _) = dvd_block(3, 24, 4);
        let two: Vec<u8> = src.iter().chain(src.iter()).copied().collect();
        let mut p = LpcmParser::new_dvd();
        p.parse(&make_pes(dvd(0x82, &two), Some(0)));
        let f = p.parse(&make_pes(dvd(0x82, &two), None));
        assert_eq!(f[0].pts_ns, 166_666, "8 samples at 48 kHz");
        assert_eq!(f[0].data.len(), 8 * 3 * 3);
    }

    #[test]
    fn dvd_24bit_group_split_across_pes_is_carried() {
        let mut p = LpcmParser::new_dvd();
        let src = [
            0xA0, 0xA1, 0xB0, 0xB1, 0xC0, 0xC1, 0xD0, 0xD1, 0xA2, 0xB2, 0xC2, 0xD2,
        ];
        let a = p.parse(&make_pes(dvd(0x81, &src[..5]), Some(0)));
        assert!(all(&a).is_empty(), "partial block held back");
        let b = p.parse(&make_pes(dvd(0x81, &src[5..]), Some(90)));
        assert_eq!(
            all(&b),
            vec![
                0xA0, 0xA1, 0xA2, 0xB0, 0xB1, 0xB2, 0xC0, 0xC1, 0xC2, 0xD0, 0xD1, 0xD2
            ]
        );
    }

    // A carried partial sample belongs to the format and stretch it came from: a format change
    // or a gap drops it instead of gluing it to the next PES.
    #[test]
    fn dvd_carry_is_dropped_on_a_format_change_or_a_discontinuity() {
        let stale = [0xEEu8; 2];
        let next = [1u8, 2, 3, 4];
        // 16-bit stereo, then 2 bytes of the next sample held back.
        let seed = || {
            let mut p = LpcmParser::new_dvd();
            p.parse(&make_pes(
                dvd(0x01, &[[0u8; 4].as_slice(), &stale].concat()),
                Some(0),
            ));
            p
        };
        // Same format, gap: the carry must not lead the post-gap sample.
        let mut p = seed();
        let mut gap = make_pes(dvd(0x01, &next), Some(90_000));
        gap.discontinuity = true;
        assert_eq!(all(&p.parse(&gap)), &next, "gap drops the carry");

        // Format change (16-bit -> 24-bit stereo): one 12-byte block is 2 sample frames.
        let block = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let mut fresh = LpcmParser::new_dvd();
        let want = all(&fresh.parse(&make_pes(dvd(0x81, &block), Some(90_000))));
        let mut p = seed();
        let got = p.parse(&make_pes(dvd(0x81, &block), Some(90_000)));
        assert_eq!(all(&got), want, "format change drops the carry");
        assert_eq!(got[0].pts_ns, 1_000_000_000, "and the PTS is not led back");
    }

    // BD PES hold whole sample frames: a trailing partial one is discarded, never glued onto
    // the next PES.
    #[test]
    fn bd_trailing_partial_sample_is_not_carried_into_the_next_pes() {
        let mut p = LpcmParser::new();
        let first = p.parse(&make_pes(bd(3, 1, &[9, 9, 9, 9, 0xEE, 0xEE]), Some(0)));
        assert_eq!(all(&first), &[9, 9, 9, 9], "the whole sample only");
        let next = [1u8, 2, 3, 4];
        let got = p.parse(&make_pes(bd(3, 1, &next), Some(90_000)));
        assert_eq!(all(&got), &next, "the discarded tail does not lead it");
    }

    #[test]
    fn carried_bytes_are_stamped_before_the_pes_pts() {
        // DVD 16-bit stereo: 2 of a sample's 4 bytes arrive in the previous PES. The
        // PTS belongs to the first sample STARTING in this PES, so the emitted run
        // (led by the carried sample) is stamped one 48 kHz sample earlier.
        let mut p = LpcmParser::new_dvd();
        p.parse(&make_pes(dvd(0x01, &[0u8; 4 * 48 + 2]), Some(0)));
        let f = p.parse(&make_pes(dvd(0x01, &[0u8; 2 + 4 * 10]), Some(90_000)));
        assert_eq!(
            f[0].pts_ns,
            1_000_000_000 - 20_833,
            "one carried sample earlier"
        );
    }

    #[test]
    fn bd_payloads_split_to_fit_payload_size_and_round_trip() {
        let h = bd_header(8, 48_000, None, 24).unwrap();
        assert!(
            bd_header(2, 44_100, None, 24).is_none(),
            "BD LPCM has no 44.1 kHz"
        );
        assert!(bd_header(0, 48_000, None, 24).is_none());
        let pcm: Vec<u8> = (0..3000 * 8 * 3).map(|i| (i % 253) as u8).collect();
        let parts = bd_payloads(&pcm, h);
        assert_eq!(parts.len(), 13, "12 x 240 + 120 samples");
        assert_eq!(parts[1].0, 5_000_000);
        let mut p = LpcmParser::new();
        let got: Vec<u8> = parts
            .into_iter()
            .flat_map(|(_, d)| all(&p.parse(&make_pes(d, Some(0)))))
            .collect();
        assert_eq!(got, pcm);
    }

    // Odd counts are coded with a pad channel: the payload_size field and the layout must say so,
    // and the parser must read the bytes back.
    #[test]
    fn bd_payloads_pad_odd_channel_counts_and_round_trip() {
        for channels in 1u8..=8 {
            let h = bd_header(channels, 48_000, None, 24).unwrap();
            let n = usize::from(channels);
            let pcm: Vec<u8> = (0..5 * n * 3).map(|i| (i % 251) as u8 + 1).collect();
            let parts = bd_payloads(&pcm, h);
            assert_eq!(parts.len(), 1);
            let coded = (n + n % 2) * 3;
            let p = &parts[0].1;
            assert_eq!(p.len(), 4 + 5 * coded, "{channels} ch");
            assert_eq!(
                usize::from(u16::from_be_bytes([p[0], p[1]])),
                5 * coded,
                "payload_size of {channels} ch"
            );
            assert_eq!(&p[2..4], &h);
            let got = LpcmParser::new().parse(&make_pes(p.clone(), Some(0)));
            assert_eq!(got[0].data, pcm, "{channels} ch round trip");
        }
    }

    #[test]
    fn bd_payloads_are_5ms_pes_like_ffmpeg_blurayenc() {
        // 20000 stereo samples @ 48 kHz -> 83 full 240-sample PES + one of 80.
        let h = bd_header(2, 48_000, None, 24).unwrap();
        let parts = bd_payloads(&vec![0u8; 20_000 * 2 * 3], h);
        assert_eq!(parts.len(), 84);
        assert!(parts[..83].iter().all(|(_, p)| p.len() == 4 + 240 * 6));
        assert_eq!(parts[83].1.len(), 4 + 80 * 6);
        assert_eq!(parts[1].0, 5_000_000, "second PES 5 ms later");
    }

    #[test]
    fn bd_parser_reports_the_channel_assignment() {
        let mut p = LpcmParser::new();
        assert_eq!(p.codec_private(), None);
        p.parse(&make_pes(bd(7, 1, &[0; 8]), Some(0)));
        let cp = p.codec_private().unwrap();
        assert_eq!(
            cp,
            b"BDLP\x71\x10".to_vec(),
            "tagged so foreign CodecPrivate never matches"
        );
        assert_eq!(layout_byte(&cp), Some(0x71));
        assert_eq!(
            layout_byte(&[0x71]),
            None,
            "untagged (foreign MKV) byte ignored"
        );
        assert_eq!(LpcmParser::new_dvd().codec_private(), None);
    }

    #[test]
    fn output_depth_follows_the_source_for_bd_and_dvd() {
        let depth = |p: &mut LpcmParser, pes: Vec<u8>| {
            let f = p.parse(&make_pes(pes, Some(0)));
            (f[0].data.len(), output_depth(p.codec_private().as_deref()))
        };
        let (mut b, mut d) = (LpcmParser::new(), LpcmParser::new_dvd());
        assert_eq!(depth(&mut b, bd(3, 1, &[0; 4])), (4, 16));
        assert_eq!(depth(&mut LpcmParser::new(), bd(3, 3, &[0; 6])), (6, 24));
        assert_eq!(depth(&mut d, dvd(0x01, &[0; 4])), (4, 16));
        // 20-bit and 24-bit DVD sources both output 24-bit.
        assert_eq!(
            depth(&mut LpcmParser::new_dvd(), dvd(0x41, &[0; 10])),
            (12, 24)
        );
        assert_eq!(
            depth(&mut LpcmParser::new_dvd(), dvd(0x81, &[0; 12])),
            (12, 24)
        );
        assert_eq!(
            output_depth(None),
            24,
            "no tag: Matroska-read PCM is 24-bit"
        );
        assert_eq!(output_depth(Some(b"BDLP\x71\x10")), 16);
    }

    #[test]
    fn bd_16bit_repack_round_trips_at_16_bit() {
        let h = bd_header(6, 48_000, None, 16).unwrap();
        assert_eq!(h[1] >> 6, 1, "16-bit quantization code");
        let pcm: Vec<u8> = (0..500 * 6 * 2).map(|i| (i % 251) as u8).collect();
        let mut p = LpcmParser::new();
        let got: Vec<u8> = bd_payloads(&pcm, h)
            .into_iter()
            .flat_map(|(_, d)| all(&p.parse(&make_pes(d, Some(0)))))
            .collect();
        assert_eq!(got, pcm);
    }

    #[test]
    fn an_emitted_frame_carries_the_packets_source_offset() {
        let mut p = LpcmParser::new();
        let mut pes = make_pes(bd(3, 1, &[0xDE, 0xAD, 0xBE, 0xEF]), Some(90_000));
        pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
        let f = p.parse(&pes);
        assert!(!f.is_empty(), "the frame is emitted");
        assert_eq!(f[0].source.map(|s| s.byte), Some(7_777));
    }

    // G8 (mpg-output-design v5 §1.1): "DVD LPCM re-pack (mirror of bd_payloads)". The
    // re-pack must invert this parser exactly; per design, do not change without a citation.
    #[test]
    fn dvd_pack_inverts_the_dvd_parser_for_every_layout() {
        let mut seed = 0x1234_5678u32;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        };
        for channels in 1..=8usize {
            for bits in [16u8, 20, 24] {
                let frames = dvd_unit_frames(channels, bits) * 7;
                let mut ir = Vec::with_capacity(frames * channels * 3);
                for _ in 0..frames * channels {
                    let lo = match bits {
                        16 => 0,
                        20 => next() & 0xF0,
                        _ => next(),
                    };
                    ir.extend_from_slice(&[next(), next(), lo]);
                }
                assert!(dvd_bits_needed(&ir) <= bits);
                let hdr = dvd_header(channels, 48_000, bits).expect("48 kHz, 1-8 ch");
                let mut es = hdr.to_vec();
                dvd_pack(&ir, channels, bits, &mut es);
                let mut p = LpcmParser::new_dvd();
                let f = p.parse(&make_pes(es, Some(0)));
                assert_eq!(f.len(), 1, "{channels} ch {bits} bit");
                let want = if bits == 16 {
                    ir.as_chunks::<3>()
                        .0
                        .iter()
                        .flat_map(|s| [s[0], s[1]])
                        .collect()
                } else {
                    ir
                };
                assert_eq!(f[0].data, want, "{channels} ch {bits} bit round trip");
            }
        }
    }

    // The channel_assignment table: count and the reorder into WAVE channel order, by value.
    #[test]
    fn bd_channel_assignments_map_to_wave_order() {
        // (assignment, source channel values in BD order, expected WAVE-order values)
        let cases: [(u8, &[u8], &[u8]); 10] = [
            (1, &[1], &[1]),
            (3, &[1, 2], &[1, 2]),
            (4, &[1, 2, 3], &[1, 2, 3]),
            (5, &[1, 2, 3], &[1, 2, 3]),
            (6, &[1, 2, 3, 4], &[1, 2, 3, 4]),
            (7, &[1, 2, 3, 4], &[1, 2, 3, 4]),
            (8, &[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5]),
            (9, &[1, 2, 3, 4, 5, 6], &[1, 2, 3, 6, 4, 5]),
            (10, &[1, 2, 3, 4, 5, 6, 7], &[1, 2, 3, 5, 6, 4, 7]),
            (11, &[1, 2, 3, 4, 5, 6, 7, 8], &[1, 2, 3, 8, 5, 6, 4, 7]),
        ];
        for (assign, src, want) in cases {
            // 16-bit, coded channels padded to even.
            let mut pcm: Vec<u8> = src.iter().flat_map(|&c| [c, c]).collect();
            if src.len() % 2 == 1 {
                pcm.extend_from_slice(&[0, 0]);
            }
            let mut p = LpcmParser::new();
            let f = p.parse(&make_pes(bd(assign, 1, &pcm), Some(0)));
            let expect: Vec<u8> = want.iter().flat_map(|&c| [c, c]).collect();
            assert_eq!(f[0].data, expect, "assignment {assign}");
        }
    }

    // Timeline advance across a PTS-less PES uses the header's sample rate (10 ms of stereo
    // 16-bit audio per packet).
    #[test]
    fn a_ptsless_packet_advances_by_the_headers_sample_rate() {
        for (code, hz) in [(1u8, 48_000usize), (4, 96_000), (5, 192_000)] {
            let mut pes = vec![0x00, 0x00, (3 << 4) | code, 1 << 6];
            pes.extend(vec![0u8; hz / 100 * 4]);
            let mut p = LpcmParser::new();
            p.parse(&make_pes(pes.clone(), Some(0)));
            let f = p.parse(&make_pes(pes, None));
            assert_eq!(f[0].pts_ns, 10_000_000, "BD rate code {code} = {hz} Hz");
        }
        for (freq, hz) in [(0u8, 48_000usize), (1, 96_000), (2, 44_100), (3, 32_000)] {
            let mut pes = vec![0x00, (freq << 4) | 1, 0x80];
            pes.extend(vec![0u8; hz / 100 * 4]);
            let mut p = LpcmParser::new_dvd();
            p.parse(&make_pes(pes.clone(), Some(0)));
            let f = p.parse(&make_pes(pes, None));
            assert_eq!(f[0].pts_ns, 10_000_000, "DVD rate code {freq} = {hz} Hz");
        }
    }

    // The M2TS re-mux header, against the pcm-blurayenc.c tables: assignment for each count,
    // rate nibble, and the bits field.
    #[test]
    fn bd_header_encodes_assignment_rate_and_depth() {
        for (channels, assign) in [
            (1u8, 1u8),
            (2, 3),
            (3, 4),
            (4, 6),
            (5, 8),
            (6, 9),
            (7, 10),
            (8, 11),
        ] {
            for (hz, code) in [(48_000u32, 1u8), (96_000, 4), (192_000, 5)] {
                assert_eq!(
                    bd_header(channels, hz, None, 24),
                    Some([(assign << 4) | code, 0xC0]),
                    "{channels} ch {hz} Hz"
                );
            }
        }
        assert_eq!(bd_header(2, 48_000, None, 16).unwrap()[1], 0x40);
        assert_eq!(bd_header(9, 48_000, None, 24), None);
        // A source layout byte is reused only when it agrees with the count and rate.
        assert_eq!(bd_header(6, 48_000, Some(0x71), 24), Some([0x91, 0xC0]));
        assert_eq!(bd_header(4, 48_000, Some(0x71), 24), Some([0x71, 0xC0]));
    }

    #[test]
    fn dvd_bits_needed_is_the_smallest_lossless_depth() {
        assert_eq!(dvd_bits_needed(&[1, 2, 0, 3, 4, 0]), 16);
        assert_eq!(dvd_bits_needed(&[1, 2, 0x50, 3, 4, 0]), 20);
        assert_eq!(dvd_bits_needed(&[1, 2, 0x51, 3, 4, 0]), 24);
    }

    // MS-30 (FFmpeg pcm_dvd): "stream->lpcm_header[0] = 0x0c; … | st->codecpar->ch_layout.
    // nb_channels - 1; stream->lpcm_header[2] = 0x80;" — and the parser's own decode.
    #[test]
    fn dvd_header_matches_the_parser_and_ffmpeg() {
        assert_eq!(dvd_header(2, 48_000, 16), Some([0x0C, 0x01, 0x80]));
        assert_eq!(dvd_header(6, 96_000, 24), Some([0x0C, 0x95, 0x80]));
        assert_eq!(
            dvd_header(2, 44_100, 16),
            None,
            "mpg carries 48/96 kHz only"
        );
        assert_eq!(dvd_header(9, 48_000, 16), None);
        assert_eq!(dvd_header(0, 48_000, 16), None);
    }
}

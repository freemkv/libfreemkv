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
#[path = "lpcm_tests.rs"]
mod tests;

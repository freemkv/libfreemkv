//! MP4 audio sample entries and codec-config boxes for the `mp4://` muxer.
//!
//! Covers codecs that map cleanly into MP4: **AC-3** (`ac-3` + `dac3`),
//! **E-AC-3 / Dolby Digital Plus** (`ec-3` + `dec3`, incl. Atmos-in-DD+ JOC),
//! and **DTS / DTS-HD** (`dtsc`/`dtsh` + `ddts`, core described, whole access
//! units passed through so an HD decoder finds the extension). Config boxes
//! are derived from the first audio frame's bitstream (ISO/IEC 14496-12
//! amendments; ETSI TS 102 366 / 102 114). Codecs with no clean MP4 mapping
//! (TrueHD, LPCM, bitmap subtitles) are excluded by the fit oracle.
//
// See docs/mp4-audio.md — DD+/AC-3 fallback rationale and the fit oracle detail.

use super::boxes::bx;
use crate::disc::Codec;

/// AC-3 / E-AC-3 sample rates indexed by `fscod` (byte-4 bits 7-6).
const FSCOD_RATES: [u32; 3] = [48_000, 44_100, 32_000];
/// E-AC-3 reduced rates indexed by `fscod2` (byte-4 bits 5-4) when `fscod == 3`.
const EAC3_REDUCED_RATES: [u32; 4] = [24_000, 22_050, 16_000, 48_000];
/// Base channel count per `acmod` (A/52 Table 5.8), before the LFE.
const ACMOD_CHANNELS: [u8; 8] = [2, 1, 2, 3, 3, 4, 4, 5];
// Lowest `bsid` that is Annex-E (E-AC-3); 8 is AC-3, 9/10 are Annex D.
// See docs/mp4-audio.md — why parser and chooser share this one constant.
const EAC3_MIN_BSID: u8 = 11;
/// Width of the `dec3` `data_rate` field in bits (ETSI TS 102 366 Annex F.6.1).
const DEC3_DATA_RATE_BITS: u32 = 13;
/// Largest data rate the 13-bit `dec3` `data_rate` field can express, in kbit/s.
const DEC3_MAX_DATA_RATE_KBPS: u16 = (1 << DEC3_DATA_RATE_BITS) - 1;

/// A big-endian MSB-first bit reader over a byte slice.
struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit: 0 }
    }
    fn skip(&mut self, n: usize) {
        self.bit += n;
    }
    // Read `n` bits (n <= 32). Returns 0 past end of data (callers pre-check len).
    // `|` (not `^`) is intentional here and in the `push` closures below.
    // See docs/mp4-audio.md — BitReader::read.
    fn read(&mut self, n: usize) -> u32 {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = self.data.get(self.bit / 8).copied().unwrap_or(0);
            let shift = 7 - (self.bit % 8);
            v = (v << 1) | ((byte >> shift) & 1) as u32;
            self.bit += 1;
        }
        v
    }
}

/// Decoded (E-)AC-3 stream parameters needed for the `dac3`/`dec3` config box
/// and the audio sample entry.
pub(super) struct DolbyConfig {
    pub fscod: u8,
    pub bsid: u8,
    pub bsmod: u8,
    pub acmod: u8,
    pub lfeon: bool,
    /// AC-3 only: `bit_rate_code` (= `frmsizecod >> 1`). Unused for E-AC-3.
    pub bit_rate_code: u8,
    /// E-AC-3 only: nominal data rate in kbps (for `dec3`). 0 for AC-3.
    pub data_rate_kbps: u16,
    pub sample_rate: u32,
    pub channels: u16,
}

impl DolbyConfig {
    fn channel_count(acmod: u8, lfeon: bool) -> u16 {
        ACMOD_CHANNELS[acmod as usize] as u16 + lfeon as u16
    }
}

/// Parse the first (E-)AC-3 frame starting at the 0x0B77 syncword. Returns
/// `None` if the frame is too short or the syncword is absent.
pub(super) fn parse_dolby(frame: &[u8]) -> Option<DolbyConfig> {
    let start =
        (0..frame.len().saturating_sub(1)).find(|&i| frame[i] == 0x0B && frame[i + 1] == 0x77)?;
    let f = &frame[start..];
    if f.len() < 6 {
        return None;
    }
    // bsid lives in byte 5 bits 7-3 for both AC-3 and E-AC-3.
    let bsid = (f[5] >> 3) & 0x1F;
    if bsid >= EAC3_MIN_BSID {
        parse_eac3(f)
    } else {
        parse_ac3(f)
    }
}

/// Legacy AC-3 (A/52 §5.3.2): syncword | crc(16) | fscod(2) frmsizecod(6) |
/// bsid(5) bsmod(3) | acmod(3) …optional… lfeon.
fn parse_ac3(f: &[u8]) -> Option<DolbyConfig> {
    if f.len() < 8 {
        return None;
    }
    let fscod = (f[4] >> 6) & 0x03;
    let frmsizecod = f[4] & 0x3F;
    let bsid = (f[5] >> 3) & 0x1F;
    let bsmod = f[5] & 0x07;

    // acmod + trailing optional 2-bit fields, then lfeon (byte 6 onward).
    let mut r = BitReader::new(f);
    r.bit = 6 * 8;
    let acmod = r.read(3) as u8;
    if (acmod & 0x1) != 0 && acmod != 0x1 {
        r.skip(2); // cmixlev
    }
    if (acmod & 0x4) != 0 {
        r.skip(2); // surmixlev
    }
    if acmod == 0x2 {
        r.skip(2); // dsurmod
    }
    let lfeon = r.read(1) == 1;

    Some(DolbyConfig {
        fscod,
        bsid,
        bsmod,
        acmod,
        lfeon,
        bit_rate_code: frmsizecod >> 1,
        data_rate_kbps: 0,
        sample_rate: FSCOD_RATES.get(fscod as usize).copied().unwrap_or(48_000),
        channels: DolbyConfig::channel_count(acmod, lfeon),
    })
}

/// E-AC-3 (A/52 Annex E BSI): syncword | strmtyp(2) substreamid(3) frmsiz(11) |
/// fscod(2) numblkscod(2) acmod(3) lfeon(1) | bsid(5) …
fn parse_eac3(f: &[u8]) -> Option<DolbyConfig> {
    if f.len() < 6 {
        return None;
    }
    let frmsiz = (((f[2] & 0x07) as u32) << 8) | f[3] as u32; // words minus one
    let fscod = (f[4] >> 6) & 0x03;
    let numblkscod = (f[4] >> 4) & 0x03;
    let acmod = (f[4] >> 1) & 0x07;
    let lfeon = (f[4] & 0x01) == 1;
    let bsid = (f[5] >> 3) & 0x1F;

    let (sample_rate, blocks) = if fscod == 0x03 {
        let fscod2 = (f[4] >> 4) & 0x03; // shares bits with numblkscod when fscod==3
        (EAC3_REDUCED_RATES[fscod2 as usize], 6u32)
    } else {
        let blocks = [1u32, 2, 3, 6][numblkscod as usize];
        (FSCOD_RATES[fscod as usize], blocks)
    };
    // Nominal data rate (kbps): frame is (frmsiz+1) 16-bit words per (blocks·256)
    // samples at sample_rate. rate = bytes·8·sr / samples / 1000.
    let frame_bytes = (frmsiz as u64 + 1) * 2;
    let samples = blocks as u64 * 256;
    let data_rate_kbps = (frame_bytes * 8 * sample_rate as u64)
        .checked_div(samples)
        .map_or(0, |r| (r / 1000) as u16);

    Some(DolbyConfig {
        fscod,
        bsid,
        bsmod: 0, // not in the E-AC-3 main header; dec3 default
        acmod,
        lfeon,
        bit_rate_code: 0,
        data_rate_kbps,
        sample_rate,
        channels: DolbyConfig::channel_count(acmod, lfeon),
    })
}

/// The `dac3` config box (ETSI TS 102 366 Annex F.4): 24 bits —
/// fscod(2) bsid(5) bsmod(3) acmod(3) lfeon(1) bit_rate_code(5) reserved(5).
pub(super) fn dac3_box(c: &DolbyConfig) -> Vec<u8> {
    let mut v: u32 = 0;
    let mut push = |val: u32, bits: u32| v = (v << bits) | (val & ((1 << bits) - 1));
    push(c.fscod as u32, 2);
    push(c.bsid as u32, 5);
    push(c.bsmod as u32, 3);
    push(c.acmod as u32, 3);
    push(c.lfeon as u32, 1);
    push(c.bit_rate_code as u32, 5);
    push(0, 5); // reserved
    // 24 bits → the top 3 bytes of the big-endian u32.
    let b = v.to_be_bytes();
    bx(b"dac3", &[b[1], b[2], b[3]])
}

/// The `dec3` config box (ETSI TS 102 366 Annex G.3): single independent
/// substream, no dependents. data_rate(13) num_ind_sub(3) fscod(2) bsid(5)
/// reserved(1) asvc(1) bsmod(3) acmod(3) lfeon(1) reserved(3) num_dep_sub(4) reserved(1).
// data_rate must be non-zero; only parse_eac3 computes one (see docs/mp4-audio.md).
pub(super) fn dec3_box(c: &DolbyConfig) -> Vec<u8> {
    let mut v: u64 = 0;
    let mut push = |val: u64, bits: u32| v = (v << bits) | (val & ((1u64 << bits) - 1));
    // Saturate rather than let `push`'s mask wrap a rate that does not fit the
    // 13-bit field: a truncated/garbage frame yielding e.g. 9000 kbit/s would
    // otherwise be declared as 808.
    push(
        c.data_rate_kbps.min(DEC3_MAX_DATA_RATE_KBPS) as u64,
        DEC3_DATA_RATE_BITS,
    );
    push(0, 3); // num_ind_sub - 1 = 0 (one substream)
    push(c.fscod as u64, 2);
    push(c.bsid as u64, 5);
    push(0, 1); // reserved
    push(0, 1); // asvc
    push(c.bsmod as u64, 3);
    push(c.acmod as u64, 3);
    push(c.lfeon as u64, 1);
    push(0, 3); // reserved
    push(0, 4); // num_dep_sub = 0
    push(0, 1); // reserved (chan_loc absent when num_dep_sub == 0)
    // 40 bits → the low 5 bytes of the big-endian u64.
    let b = v.to_be_bytes();
    bx(b"dec3", &[b[3], b[4], b[5], b[6], b[7]])
}

/// Build an audio sample entry (`ac-3` / `ec-3` / `dtsc` / `dtsh`) with the
/// given config box. `AudioSampleEntry` per ISO/IEC 14496-12 §12.2.3.
pub(super) fn audio_sample_entry(
    fourcc: &[u8; 4],
    channels: u16,
    sample_rate: u32,
    config: &[u8],
) -> Vec<u8> {
    let mut e = Vec::new();
    e.extend_from_slice(&[0u8; 6]); // reserved
    e.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
    e.extend_from_slice(&[0u8; 8]); // reserved (version 0)
    e.extend_from_slice(&channels.to_be_bytes());
    e.extend_from_slice(&16u16.to_be_bytes()); // samplesize
    e.extend_from_slice(&0u16.to_be_bytes()); // pre_defined
    e.extend_from_slice(&0u16.to_be_bytes()); // reserved
    // samplerate is 16.16 fixed point; the integer rate in the high 16 bits. The
    // integer part is only 16 bits, so cap at 65535 — 96/192 kHz (DTS-HD) would
    // otherwise overflow u32 and write a garbage rate (the true rate is in ddts).
    e.extend_from_slice(&(sample_rate.min(0xFFFF) << 16).to_be_bytes());
    e.extend_from_slice(config);
    bx(fourcc, &e)
}

// ── DTS (dtsc/dtsh + ddts) ───────────────────────────────────────────────────

/// DTS core `SFREQ` (4-bit) → sample rate (Hz). Reserved indices → 48 kHz.
const DTS_SFREQ: [u32; 16] = [
    48_000, 8_000, 16_000, 32_000, 48_000, 48_000, 11_025, 22_050, 44_100, 48_000, 48_000, 12_000,
    24_000, 48_000, 96_000, 192_000,
];
// DTS core base channel count per AMODE (ETSI TS 102 114 §5.3.1); matches the
// decodability gate's DTS_AMODE_COUNT in dts.rs. See docs/mp4-audio.md.
const DTS_AMODE_CH: [u8; 16] = [1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 6, 6, 6, 7, 8, 8];

/// Decoded DTS core parameters needed for the `ddts` box.
struct DtsConfig {
    sample_rate: u32,
    channels: u16,
    amode: u8,
    lfe: bool,
    core_size: u32,
    /// Samples per frame ((NBLKS+1)·32).
    frame_samples: u32,
    /// Whether a DTS-HD extension substream follows the core.
    has_extension: bool,
    channel_layout: u16,
}

/// Parse the DTS core header (ETSI TS 102 114 §5.3.1), starting at the
/// 0x7FFE8001 big-endian core sync. Returns `None` if too short / no sync.
fn parse_dts(frame: &[u8]) -> Option<DtsConfig> {
    let start = (0..frame.len().saturating_sub(3)).find(|&i| {
        frame[i] == 0x7F && frame[i + 1] == 0xFE && frame[i + 2] == 0x80 && frame[i + 3] == 0x01
    })?;
    let f = &frame[start..];
    if f.len() < 11 {
        return None;
    }
    // Bit fields after the 32-bit sync (MSB-first):
    // FTYPE1 SHORT5 CPF1 NBLKS7 FSIZE14 AMODE6 SFREQ4 RATE5 ...
    let nblks = (((f[4] & 0x01) as u32) << 6) | ((f[5] >> 2) as u32 & 0x3F);
    let fsize = (((f[5] & 0x03) as u32) << 12) | ((f[6] as u32) << 4) | ((f[7] >> 4) as u32 & 0x0F);
    let amode = (((f[7] & 0x0F) << 2) | ((f[8] >> 6) & 0x03)) as usize;
    let sfreq = ((f[8] >> 2) & 0x0F) as usize;
    // LFF is 2 bits at bit offset 85 → byte10 bits 2-1.
    let lff = (f[10] >> 1) & 0x03;
    let lfe = lff == 1 || lff == 2;

    let sample_rate = DTS_SFREQ[sfreq];
    // AMODE is a 6-bit field, so 16..=63 are reachable but RESERVED (ETSI TS 102 114) —
    // no channel count or speaker mask is known for them. Refuse rather than guess: the
    // old `unwrap_or(6)` invented a count the speaker mask could not match.
    let base_ch = *DTS_AMODE_CH.get(amode)?;
    let channels = base_ch as u16 + lfe as u16;
    let channel_layout = dts_channel_layout(amode, lfe);
    // DTS-HD extension substream sync (0x64582025) after the core frame. Search only
    // at/after the core end (core_size = fsize+1): scanning the whole frame would
    // false-positive inside the compressed core payload, mislabeling dtsc as dtsh.
    let ext_sync = [0x64, 0x58, 0x20, 0x25];
    // The EXSS begins at byte core_size (= fsize + 1); start the window search
    // exactly there so no 4-byte window inside the compressed core is ever tested.
    let ext_sync_start = (fsize as usize + 1).min(f.len());
    let has_extension = f.windows(4).skip(ext_sync_start).any(|w| w == ext_sync);

    Some(DtsConfig {
        sample_rate,
        channels,
        amode: amode as u8,
        lfe,
        core_size: fsize + 1,
        frame_samples: (nblks + 1) * 32,
        has_extension,
        channel_layout,
    })
}

// `ddts` ChannelLayout speaker mask per core AMODE (bit0=C, bit1=L/R, ...
// bit15=Lhr/Rhr; paired bits = two speakers). Must stay consistent with
// DTS_AMODE_CH — see docs/mp4-audio.md for the invariant and history.
const DTS_AMODE_LAYOUT: [u16; 16] = [
    0x0001, // 0  A                      → C
    0x0002, // 1  A + B (dual mono)      → L/R
    0x0002, // 2  L + R (stereo)         → L/R
    0x0002, // 3  (L+R) + (L−R) (sum/difference) → L/R
    0x0002, // 4  LT + RT (left/right total)     → L/R
    0x0003, // 5  C + L/R
    0x0012, // 6  L/R + Cs
    0x0013, // 7  C + L/R + Cs
    0x0006, // 8  L/R + Ls/Rs
    0x0007, // 9  C + L/R + Ls/Rs        (5.0; the LFE bit is added separately)
    0x0206, // 10 L/R + Ls/Rs + Lc/Rc
    0x0143, // 11 C + L/R + Lsr/Rsr + Oh
    0x0053, // 12 C + L/R + Cs + Lsr/Rsr
    0x0207, // 13 C + L/R + Ls/Rs + Lc/Rc
    0x0246, // 14 L/R + Ls/Rs + Lsr/Rsr + Lc/Rc
    0x0217, // 15 C + L/R + Ls/Rs + Cs + Lc/Rc
];

/// `ddts` ChannelLayout (16-bit speaker mask) for a core `AMODE`, plus LFE.
// `amode` must be 0..=15; parse_dts rejects the reserved 16..=63 first.
fn dts_channel_layout(amode: usize, lfe: bool) -> u16 {
    let mut m = DTS_AMODE_LAYOUT[amode];
    if lfe {
        m |= 0x0008;
    }
    m
}

/// The `ddts` config box (ETSI TS 102 114 Annex; DTS-in-ISO registration).
/// Describes the DTS core; whole access units (core + any extension) are passed
/// through as samples, so a DTS-HD-aware decoder still finds the extension.
fn ddts_box(c: &DtsConfig) -> Vec<u8> {
    // avg/max bitrate: derived from core frame size × frame rate (core RATE reads
    // "open/variable" for lossless, so unusable directly). Rate isn't integral
    // (e.g. 93.75 frames/s), so multiply before dividing, rounding to nearest.
    let bitrate = if c.frame_samples > 0 {
        let num = c.core_size as u64 * 8 * c.sample_rate as u64;
        let den = c.frame_samples as u64;
        ((num + den / 2) / den).min(u32::MAX as u64) as u32
    } else {
        0
    };

    let mut out = Vec::new();
    out.extend_from_slice(&c.sample_rate.to_be_bytes()); // DTSSamplingFrequency
    out.extend_from_slice(&bitrate.to_be_bytes()); // maxBitrate
    out.extend_from_slice(&bitrate.to_be_bytes()); // avgBitrate
    out.push(if c.has_extension { 24 } else { 16 }); // pcmSampleDepth
    // Bit-packed tail (56 bits): FrameDuration2 StreamConstruction5 CoreLFEPresent1
    // CoreLayout6 CoreSize14 StereoDownmix1 RepresentationType3 ChannelLayout16
    // MultiAssetFlag1 LBRDurationMod1 ReservedBoxPresent1 Reserved5
    let frame_duration = match c.frame_samples {
        0..=512 => 0,
        513..=1024 => 1,
        1025..=2048 => 2,
        _ => 3,
    };
    // StreamConstruction: 1 = DTS core present. Whole-AU passthrough means an
    // HD decoder still parses the extension substreams from the stream itself.
    let stream_construction = 1u128;
    let mut v: u128 = 0;
    let mut push = |val: u128, bits: u32| v = (v << bits) | (val & ((1u128 << bits) - 1));
    push(frame_duration as u128, 2);
    push(stream_construction, 5);
    push(c.lfe as u128, 1);
    push(c.amode as u128, 6);
    // CoreSize is 14 bits; core_size = FSIZE+1 (FSIZE itself 14 bits) maxes at
    // 16384 — one past the field, which `push`'s mask would wrap to 0. Clamp to
    // 16383 (one byte short) rather than 0 (decoder thinks the core is empty).
    push((c.core_size as u128).min((1u128 << 14) - 1), 14);
    push(0, 1); // StereoDownmix
    push(0, 3); // RepresentationType
    push(c.channel_layout as u128, 16);
    // MultiAssetFlag signals a second audio ASSET — distinct from `has_extension`
    // (a DTS-HD MA/HRA extension substream is still ONE asset). Deriving the flag
    // from has_extension sent parsers hunting a nonexistent asset; 0 is the only honest value.
    push(0, 1); // MultiAssetFlag
    push(0, 1); // LBRDurationMod
    push(0, 1); // ReservedBoxPresent
    push(0, 5); // Reserved
    // 56 bits → the low 7 bytes of the big-endian u128.
    let b = v.to_be_bytes();
    out.extend_from_slice(&b[9..16]);
    bx(b"ddts", &out)
}

/// The MP4 fourcc + config box for an audio frame, or `None` if the codec has no
/// MP4 mapping here. Together with [`audio_fits`] this is the fit oracle for
/// audio: only what returns `Some` is muxable.
pub(super) fn dolby_sample_entry(codec: Codec, first_frame: &[u8]) -> Option<Vec<u8>> {
    match codec {
        Codec::Ac3 => {
            let c = parse_dolby(first_frame)?;
            Some(audio_sample_entry(
                b"ac-3",
                c.channels,
                c.sample_rate,
                &dac3_box(&c),
            ))
        }
        Codec::Ac3Plus => {
            let c = parse_dolby(first_frame)?;
            // Describe the syncframe actually found, not the codec the playlist claimed:
            // a legacy AC-3 bsid (≤10) has no data rate, so an EC3SpecificBox from it
            // would wrongly declare data_rate=0 (ETSI TS 102 366 F.6.1) — emit ac-3/dac3 instead.
            let (fourcc, config): (&[u8; 4], Vec<u8>) = if c.bsid >= EAC3_MIN_BSID {
                (b"ec-3", dec3_box(&c))
            } else {
                (b"ac-3", dac3_box(&c))
            };
            Some(audio_sample_entry(
                fourcc,
                c.channels,
                c.sample_rate,
                &config,
            ))
        }
        Codec::Dts | Codec::DtsHdMa | Codec::DtsHdHr => {
            let c = parse_dts(first_frame)?;
            // `dtsc` = DTS core; `dtsh` = DTS-HD (core + extension substreams).
            let fourcc: &[u8; 4] = if c.has_extension { b"dtsh" } else { b"dtsc" };
            Some(audio_sample_entry(
                fourcc,
                c.channels,
                c.sample_rate,
                &ddts_box(&c),
            ))
        }
        _ => None,
    }
}

/// Fit oracle for an audio codec: does `mp4://` currently carry it?
// TrueHD, LPCM, AAC are not yet mapped; skipped with a loud report, not silently dropped.
pub(super) fn audio_fits(codec: Codec) -> bool {
    matches!(
        codec,
        Codec::Ac3 | Codec::Ac3Plus | Codec::Dts | Codec::DtsHdMa | Codec::DtsHdHr
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic legacy AC-3 header: syncword, crc, fscod=0 (48k),
    /// frmsizecod, bsid=8, bsmod=0, acmod=7 (3/2), lfeon=1 → 5.1.
    // Underscores here mark BITFIELD boundaries in the header being built (e.g. a
    // 5-bit field then a 3-bit field), not thousands-style digit groups — regrouping
    // them uniformly would satisfy the lint by destroying the only thing they encode.
    #[allow(clippy::unusual_byte_groupings)]
    fn ac3_frame_5_1() -> Vec<u8> {
        let mut f = vec![0x0B, 0x77, 0x00, 0x00];
        // byte4: fscod(2)=0 | frmsizecod(6)=0b010110 (22)
        f.push(0b00_010110);
        // byte5: bsid(5)=8 (0b01000) | bsmod(3)=0
        f.push(0b01000_000);
        // byte6: acmod(3)=7 (0b111) | cmixlev(2) | surmixlev(2) | lfeon(1)...
        // acmod=7 has centre (needs cmixlev) and surround (needs surmixlev):
        // 111 | 00 | 00 | 1(lfeon) = 0b111_00_00_1
        f.push(0b111_00_00_1);
        f.push(0x00);
        f
    }

    #[test]
    fn ac3_bsi_and_dac3() {
        let c = parse_dolby(&ac3_frame_5_1()).expect("parsed");
        assert!(c.bsid < 11, "legacy AC-3");
        assert_eq!(c.fscod, 0);
        assert_eq!(c.sample_rate, 48_000);
        assert_eq!(c.bsid, 8);
        assert_eq!(c.acmod, 7);
        assert!(c.lfeon);
        assert_eq!(c.channels, 6, "3/2 + LFE = 5.1");
        assert_eq!(c.bit_rate_code, 22 >> 1);

        let dac3 = dac3_box(&c);
        // [size:4]["dac3"][3-byte payload] = 11 bytes.
        assert_eq!(dac3.len(), 11);
        assert_eq!(&dac3[4..8], b"dac3");
    }

    /// A synthetic Annex-E (E-AC-3) syncframe: bsid=16, fscod=0 (48 kHz),
    /// numblkscod=3 (6 blocks), acmod=7 (3/2), lfeon=1 → 5.1, frmsiz=63 (128 B).
    // Underscores here mark BITFIELD boundaries in the header being built (e.g. a
    // 5-bit field then a 3-bit field), not thousands-style digit groups — regrouping
    // them uniformly would satisfy the lint by destroying the only thing they encode.
    #[allow(clippy::unusual_byte_groupings)]
    fn eac3_frame_5_1() -> Vec<u8> {
        // E-AC-3: syncword | strmtyp/substreamid/frmsiz | fscod/numblks/acmod/lfeon | bsid
        let mut f = vec![0x0B, 0x77];
        f.push(0x00); // strmtyp=0, substreamid=0, frmsiz high=0
        f.push(0x3F); // frmsiz low = 63 → frame 128 bytes
        // byte4: fscod(2)=0 | numblkscod(2)=3 (6 blocks) | acmod(3)=7 | lfeon(1)=1
        f.push(0b00_11_111_1);
        // byte5: bsid(5)=16 (E-AC-3) | dialnorm high(3)
        f.push(0b10000_000);
        f.push(0x00);
        f
    }

    /// Decode `data_rate` (the leading 13 bits) back out of an emitted `dec3`
    /// box body, so a test measures the FILE and not this module's arithmetic.
    fn dec3_data_rate(dec3: &[u8]) -> u16 {
        assert_eq!(&dec3[4..8], b"dec3");
        (u16::from_be_bytes([dec3[8], dec3[9]]) >> 3) & 0x1FFF
    }

    #[test]
    fn eac3_bsi_and_dec3() {
        let f = eac3_frame_5_1();
        let c = parse_dolby(&f).expect("parsed");
        assert!(c.bsid >= 11, "E-AC-3");
        assert_eq!(c.bsid, 16);
        assert_eq!(c.fscod, 0);
        assert_eq!(c.sample_rate, 48_000);
        assert_eq!(c.acmod, 7);
        assert!(c.lfeon);
        assert_eq!(c.channels, 6);
        let dec3 = dec3_box(&c);
        // [size:4]["dec3"][5-byte payload] = 13 bytes.
        assert_eq!(dec3.len(), 13);
        assert_eq!(&dec3[4..8], b"dec3");
    }

    #[test]
    fn ac3plus_with_a_legacy_syncframe_is_declared_ac3_not_a_zero_rate_ec3() {
        // A Codec::Ac3Plus track whose first syncframe is legacy AC-3 (bsid 8 < 11)
        // has no nominal data rate; an `ec-3` entry built from it would declare
        // 0 kbps, violating ETSI TS 102 366 F.6.1 — emit ac-3/dac3 (Annex F.4) instead.
        let e = dolby_sample_entry(Codec::Ac3Plus, &ac3_frame_5_1()).expect("entry built");
        assert_eq!(
            &e[4..8],
            b"ac-3",
            "a legacy AC-3 syncframe gets the AC3SampleEntry"
        );
        assert!(
            e.windows(4).any(|w| w == b"dac3"),
            "AC3SpecificBox present in the emitted entry"
        );
        assert!(
            !e.windows(4).any(|w| w == b"dec3"),
            "no EC3SpecificBox may describe an AC-3 syncframe"
        );
    }

    #[test]
    fn ec3_entry_declares_a_nonzero_data_rate_decoded_from_the_box() {
        // A real Annex-E syncframe still gets `ec-3` + `dec3`, and the data_rate
        // read back out of the emitted box is the frame's nominal rate, not 0.
        // 128 B / 1536 samples @ 48 kHz = 32 kbit/s.
        let e = dolby_sample_entry(Codec::Ac3Plus, &eac3_frame_5_1()).expect("entry built");
        assert_eq!(&e[4..8], b"ec-3");
        let i = e
            .windows(4)
            .position(|w| w == b"dec3")
            .expect("EC3SpecificBox present");
        let rate = dec3_data_rate(&e[i - 4..]);
        assert_ne!(
            rate, 0,
            "ETSI TS 102 366 Annex F.6.1: data_rate is in kbit/s"
        );
        assert_eq!(rate, 32);
    }

    #[test]
    fn dec3_data_rate_saturates_at_the_13_bit_field_maximum() {
        // `data_rate` is a 13-bit field (max 8191 kbit/s). The `& mask` in `push`
        // wraps anything larger into a small, wrong rate — 9000 became 808.
        let c = DolbyConfig {
            fscod: 0,
            bsid: 16,
            bsmod: 0,
            acmod: 7,
            lfeon: true,
            bit_rate_code: 0,
            data_rate_kbps: 9000,
            sample_rate: 48_000,
            channels: 6,
        };
        let rate = dec3_data_rate(&dec3_box(&c));
        assert_eq!(
            rate, 8191,
            "an out-of-range rate saturates, it does not wrap"
        );
        assert_ne!(rate, 808, "9000 & 0x1FFF");
    }

    #[test]
    fn sample_entry_shape() {
        let c = parse_dolby(&ac3_frame_5_1()).unwrap();
        let e = audio_sample_entry(b"ac-3", c.channels, c.sample_rate, &dac3_box(&c));
        assert_eq!(&e[4..8], b"ac-3");
        // channelcount at entry-body offset 16 (after 6 reserved + 2 dri + 8 reserved).
        let ch = u16::from_be_bytes([e[8 + 16], e[8 + 17]]);
        assert_eq!(ch, 6);
    }

    #[test]
    fn fit_oracle_covers_dolby_and_dts() {
        assert!(audio_fits(Codec::Ac3));
        assert!(audio_fits(Codec::Ac3Plus));
        assert!(audio_fits(Codec::Dts));
        assert!(audio_fits(Codec::DtsHdMa));
        assert!(!audio_fits(Codec::TrueHd));
        assert!(!audio_fits(Codec::Lpcm));
    }

    #[test]
    fn dts_core_5_1_and_ddts() {
        // Synthetic DTS core: SFREQ=13 (48k), AMODE=9 (5ch), LFF=1 (LFE) → 5.1.
        let f = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x3C, 0x05, 0xF2, 0x77, 0x00, 0x02, 0x00,
        ];
        let c = parse_dts(&f).expect("dts core parsed");
        assert_eq!(c.sample_rate, 48_000);
        assert_eq!(c.amode, 9);
        assert!(c.lfe);
        assert_eq!(c.channels, 6, "5 core + LFE = 5.1");
        assert_eq!(c.channel_layout, 0x000F, "C + L/R + Ls/Rs + LFE");
        assert_eq!(c.core_size, 96);
        assert_eq!(c.frame_samples, 512);

        let ddts = ddts_box(&c);
        assert_eq!(&ddts[4..8], b"ddts");
        // DTSSamplingFrequency (first field) = 48000.
        assert_eq!(
            u32::from_be_bytes([ddts[8], ddts[9], ddts[10], ddts[11]]),
            48_000
        );
        // Sample entry uses dtsc (no extension in this synthetic frame).
        let e = dolby_sample_entry(Codec::DtsHdMa, &f).unwrap();
        assert_eq!(&e[4..8], b"dtsc");
    }

    #[test]
    fn dts_ext_sync_inside_core_is_not_a_false_positive() {
        // A 4-byte ext-sync pattern INSIDE the compressed core payload must NOT be
        // read as a DTS-HD extension → stays dtsc. fsize=0x3FFF makes core_size far
        // larger than the frame, so the search starts past the end and never tests it.
        let f = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x07, 0xFF, 0xF2, 0x74, 0x00, 0x00, 0x00, 0x64, 0x58,
            0x20, 0x25,
        ];
        let c = parse_dts(&f).expect("parses");
        assert!(!c.has_extension, "ext-sync inside core is not an extension");
        let e = dolby_sample_entry(Codec::DtsHdMa, &f).unwrap();
        assert_eq!(&e[4..8], b"dtsc");
    }

    #[test]
    fn dts_ext_sync_at_core_end_is_detected() {
        // fsize=8 → core_size=9; the EXSS sync sits exactly at byte 9 (right after
        // the core) and MUST be detected → dtsh. Guards the off-by-4 boundary.
        let f = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x00, 0x00, 0x80, 0x00, 0x64, 0x58, 0x20, 0x25,
        ];
        let c = parse_dts(&f).expect("parses");
        assert_eq!(c.core_size, 9);
        assert!(c.has_extension, "EXSS sync at core end is a real extension");
        let e = dolby_sample_entry(Codec::DtsHdMa, &f).unwrap();
        assert_eq!(&e[4..8], b"dtsh");
    }

    #[test]
    fn dts_high_amode_channel_counts_are_declared() {
        // DTS_AMODE_CH must declare the true count for AMODE 13→7, 14→8, 15→8 (ETSI
        // TS 102 114 §5.3.1) — the old 10-entry table fell through `unwrap_or(6)`.
        // Frame layout mirrors dts_core_5_1_and_ddts; AMODE splits f[7]<<2 | f[8]>>6.
        let frame = |f7: u8, f8: u8| {
            vec![
                0x7F, 0xFE, 0x80, 0x01, 0x00, 0x05, 0xF2, f7, f8, 0x00, 0x00, 0x00,
            ]
        };
        // AMODE 13 → base 7 channels.
        let c = parse_dts(&frame(0xF3, 0x74)).expect("amode 13 parses");
        assert_eq!(c.amode, 13);
        assert!(!c.lfe);
        assert_eq!(c.channels, 7, "AMODE 13 core is 7 channels, not 6");
        // AMODE 14 → base 8 channels.
        let c = parse_dts(&frame(0xF3, 0xB4)).expect("amode 14 parses");
        assert_eq!(c.amode, 14);
        assert_eq!(c.channels, 8, "AMODE 14 core is 8 channels, not 6");
        // AMODE 15 → base 8 channels.
        let c = parse_dts(&frame(0xF3, 0xF4)).expect("amode 15 parses");
        assert_eq!(c.amode, 15);
        assert_eq!(c.channels, 8, "AMODE 15 core is 8 channels, not 6");
    }

    #[test]
    fn ddts_core_size_clamps_instead_of_wrapping_to_zero() {
        // core_size = FSIZE + 1 with FSIZE 14 bits, so its maximum is 16384 — one
        // past the 14-bit ddts CoreSize field. Masking wrapped that to 0, telling a
        // decoder the core frame is empty.
        let c = DtsConfig {
            sample_rate: 48_000,
            channels: 6,
            amode: 9,
            lfe: true,
            core_size: 16_384,
            frame_samples: 512,
            has_extension: false,
            channel_layout: dts_channel_layout(9, true),
        };
        let b = ddts_box(&c);
        // Decode CoreSize back out of the emitted box: 8-byte header + 13 bytes
        // (freq/bitrates/depth) precede the 56-bit tail, where CoreSize sits after
        // FrameDuration(2)+StreamConstruction(5)+CoreLFEPresent(1)+CoreLayout(6) = bits 14..28.
        let tail = &b[8 + 13..];
        assert!(tail.len() >= 4, "packed tail present");
        let word = u32::from_be_bytes([tail[0], tail[1], tail[2], tail[3]]);
        let core_size = (word >> 4) & 0x3FFF;
        assert_eq!(
            core_size, 16_383,
            "a 16384-byte core must be declared as the 14-bit maximum, not wrapped"
        );
        assert_ne!(core_size, 0, "wrapping to 0 declares an empty core frame");
    }

    #[test]
    fn ddts_multi_asset_flag_is_clear_for_a_single_asset_with_an_extension() {
        // MultiAssetFlag signals more than one audio ASSET; a DTS-HD MA track's XLL
        // extension substream is still ONE asset, so deriving the flag from "EXSS
        // follows the core" told a parser to select a nonexistent second asset — 0 is honest.
        let f = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x00, 0x00, 0x80, 0x00, 0x64, 0x58, 0x20, 0x25,
        ];
        let c = parse_dts(&f).expect("parses");
        assert!(c.has_extension, "fixture must exercise the extension path");

        // Decode the flag from the emitted box: 8-byte header + 13 bytes precede the
        // 56-bit tail, where MultiAssetFlag is bit 48 — after FrameDuration+
        // StreamConstruction+CoreLFEPresent+CoreLayout+CoreSize+StereoDownmix+RepType+ChannelLayout — MSB of byte 7.
        let b = ddts_box(&c);
        let tail = &b[8 + 13..];
        assert_eq!(tail.len(), 7, "56-bit packed tail");
        assert_eq!(
            (tail[6] >> 7) & 1,
            0,
            "MultiAssetFlag must not be set from has_extension"
        );
    }

    #[test]
    // The loop variable is the DOMAIN VALUE being checked (a DTS AMODE), not a cursor
    // into a collection — it's what the table is keyed by and the assertion names.
    // `.iter().enumerate()` would rename the thing under test to `i` and read worse.
    #[allow(clippy::needless_range_loop)]
    fn ddts_channel_layout_speaker_count_matches_declared_channels() {
        // The `ddts` box carries BOTH a channel count and a 16-bit speaker mask; a
        // decoder may trust either, so they must agree for all 16 AMODEs. Paired
        // mask bits (ETSI TS 102 114) contribute 2 speakers, single bits 1; LFE (bit3) is added separately.
        const SPEAKERS_PER_BIT: [u8; 16] = [
            1, // bit0  C
            2, // bit1  L/R
            2, // bit2  Ls/Rs
            0, // bit3  LFE      (counted separately)
            1, // bit4  Cs
            2, // bit5  Lh/Rh
            2, // bit6  Lsr/Rsr
            1, // bit7  Ch
            1, // bit8  Oh
            2, // bit9  Lc/Rc
            2, // bit10 Lw/Rw
            2, // bit11 Lss/Rss
            0, // bit12 LFE2     (counted separately)
            2, // bit13 Lhs/Rhs
            1, // bit14 Chr
            2, // bit15 Lhr/Rhr
        ];
        let speakers = |mask: u16| -> u8 {
            (0..16)
                .filter(|b| mask & (1 << b) != 0)
                .map(|b| SPEAKERS_PER_BIT[b])
                .sum()
        };

        for amode in 0..=15usize {
            let mask = dts_channel_layout(amode, false);
            assert_eq!(
                speakers(mask),
                DTS_AMODE_CH[amode],
                "AMODE {amode}: mask {mask:#06x} describes {} speakers but \
                 DTS_AMODE_CH declares {} — the ddts box contradicts itself",
                speakers(mask),
                DTS_AMODE_CH[amode],
            );
            // LFE must be additive: same speakers plus the LFE bit, nothing else.
            let with_lfe = dts_channel_layout(amode, true);
            assert_eq!(
                with_lfe,
                mask | 0x0008,
                "AMODE {amode}: LFE must only set bit3"
            );
        }
    }

    #[test]
    fn dts_reserved_amode_is_rejected_not_guessed() {
        // AMODE (6 bits) 16..=63 are RESERVED — no channel count/mask is known; the
        // old code guessed 6 channels. amode = (f[7]&0x0F)<<2 | (f[8]>>6); f[7] low
        // nibble 0b0100 gives amode 16..19 depending on f[8]'s top bits (SFREQ stays 13).
        let frame = |f7: u8, f8: u8| {
            vec![
                0x7F, 0xFE, 0x80, 0x01, 0x00, 0x05, 0xF2, f7, f8, 0x00, 0x00, 0x00,
            ]
        };
        for (f7, f8, amode) in [(0xF4, 0x34, 16), (0xF4, 0xF4, 19), (0xFF, 0xF4, 63)] {
            assert!(
                parse_dts(&frame(f7, f8)).is_none(),
                "reserved AMODE {amode} must not parse"
            );
        }
        // The boundary below it still parses, so the guard is not over-broad.
        let c = parse_dts(&frame(0xF3, 0xF4)).expect("AMODE 15 is valid and must parse");
        assert_eq!(c.amode, 15);
    }

    // ── (E-)AC-3 bit-field extraction ──────────────────────────────────────
    // Fixtures give every BSI field a DISTINCT value (unlike `ac3_frame_5_1`, which
    // hides a dropped bit/wrong-lane read), read back from the EMITTED payload.

    /// dac3 payload (3 bytes) out of an emitted AC3SpecificBox.
    fn dac3_payload(b: &[u8]) -> [u8; 3] {
        assert_eq!(&b[4..8], b"dac3");
        [b[8], b[9], b[10]]
    }

    /// dec3 payload (5 bytes) out of an emitted EC3SpecificBox.
    fn dec3_payload(b: &[u8]) -> [u8; 5] {
        assert_eq!(&b[4..8], b"dec3");
        [b[8], b[9], b[10], b[11], b[12]]
    }

    /// AC-3 syncframe with every BSI field distinct: fscod=1 (44.1 kHz),
    /// frmsizecod=37 (bit_rate_code 18), bsid=8, bsmod=5, acmod=6 (2/2),
    /// lfeon=1 -> 4.1 = 5 channels.
    // Byte layout and the acmod-6 cmixlev/surmixlev derivation: docs/mp4-audio.md.
    fn ac3_frame_distinct() -> Vec<u8> {
        vec![0x0B, 0x77, 0x00, 0x00, 0x65, 0x45, 0xC4, 0x00]
    }

    #[test]
    fn ac3_bsi_fields_are_extracted_from_their_own_lanes() {
        let c = parse_dolby(&ac3_frame_distinct()).expect("parsed");
        // fscod is byte4 bits 7-6: shifting the other way yields 0 → 48 kHz.
        assert_eq!(c.fscod, 1, "fscod = byte4 >> 6");
        assert_eq!(c.sample_rate, 44_100, "FSCOD_RATES[1]");
        assert_eq!(c.bsid, 8);
        // bsmod is byte5 bits 2-0, MASKED off bsid: `|` instead of `&` leaks
        // bsid into it (0x45 | 0x07 = 71, not 5).
        assert_eq!(c.bsmod, 5, "bsmod = byte5 & 0x07");
        assert_eq!(c.acmod, 6, "2/2");
        assert!(c.lfeon);
        assert_eq!(c.channels, 5, "2/2 + LFE");
        assert_eq!(c.bit_rate_code, 18, "frmsizecod 37 >> 1");
    }

    #[test]
    fn dac3_payload_packs_each_field_at_its_spec_offset() {
        // ETSI TS 102 366 Annex F.4, AC3SpecificBox = 24 bits: fscod(2) bsid(5) bsmod(3)
        // acmod(3) lfeon(1) bit_rate_code(5) rsvd(5) = 01 01000 101 110 1 10010 00000
        // = 51 76 40. Nothing else in this module asserts the dac3 BODY, so this pins it.
        let c = parse_dolby(&ac3_frame_distinct()).expect("parsed");
        assert_eq!(dac3_payload(&dac3_box(&c)), [0x51, 0x76, 0x40]);
    }

    #[test]
    fn dac3_truncates_an_oversized_field_instead_of_bleeding_into_its_neighbour() {
        // Every field from `parse_ac3` is already masked to its width; this pins
        // `push`'s own contract of writing exactly `bits` bits — a mask one bit too
        // wide lets bsmod=8 set bsid's bit, silently renumbering the bsid.
        let cfg = |bsmod, bit_rate_code| DolbyConfig {
            fscod: 1,
            bsid: 8,
            bsmod,
            acmod: 6,
            lfeon: true,
            bit_rate_code,
            data_rate_kbps: 0,
            sample_rate: 44_100,
            channels: 5,
        };
        // bsmod 8 is one past the 3-bit field, bit_rate_code 32 one past the
        // 5-bit field; both must be written as 0, changing no other field.
        assert_eq!(
            dac3_payload(&dac3_box(&cfg(8, 32))),
            dac3_payload(&dac3_box(&cfg(0, 0))),
            "an out-of-range field is truncated to its own lane"
        );
    }

    #[test]
    fn ac3_optional_mix_level_fields_are_skipped_exactly_when_present() {
        // A/52 §5.4.2: cmixlev present iff (acmod&0x1) && acmod!=0x1; surmixlev iff
        // acmod&0x4. A wrongly skipped/kept 2-bit field shifts `lfeon`, reading LFE
        // and channel count from the wrong lane. acmod=3: byte6=011|cmixlev01|lfeon1|00=0x6C.
        let c = parse_dolby(&[0x0B, 0x77, 0x00, 0x00, 0x00, 0x40, 0x6C, 0x00]).expect("parsed");
        assert_eq!(c.acmod, 3);
        assert!(c.lfeon, "acmod 3 has no surmixlev to skip");
        assert_eq!(c.channels, 4, "3/0 + LFE");
        // acmod=4 (2/1): surround, no centre → surmixlev present, cmixlev not.
        // byte6 = 100 | surmixlev 01 | lfeon 1 | pad 00 = 0x8C
        let c = parse_dolby(&[0x0B, 0x77, 0x00, 0x00, 0x00, 0x40, 0x8C, 0x00]).expect("parsed");
        assert_eq!(c.acmod, 4);
        assert!(c.lfeon, "acmod 4's surmixlev must be skipped");
        assert_eq!(c.channels, 4, "2/1 + LFE");
        // acmod=6 (2/2): surround, no centre — same rule, LFE still found.
        let c = parse_dolby(&ac3_frame_distinct()).expect("parsed");
        assert!(c.lfeon);
    }

    #[test]
    fn dolby_syncword_needs_both_bytes() {
        // 0x0B alone is not a syncframe. Matching on either byte anchors the parse
        // two bytes early, so every BSI field reads from the wrong offset — bsid 0
        // instead of 8, an entirely different stream description from the same buffer.
        let mut f = vec![0x0B, 0x00];
        f.extend_from_slice(&ac3_frame_5_1());
        let c = parse_dolby(&f).expect("parsed at the real syncword");
        assert_eq!(c.bsid, 8, "sync is 0x0B77, not 0x0B or 0x77");
        assert_eq!(c.acmod, 7);
        assert_eq!(c.channels, 6);
    }

    #[test]
    fn dolby_frame_shorter_than_the_bsi_is_refused_not_indexed() {
        // The length guard is `< 6` because byte 5 (bsid) is read immediately
        // after it. Five bytes must be refused; six must still parse.
        assert!(
            parse_dolby(&[0x0B, 0x77, 0x00, 0x00, 0x00]).is_none(),
            "5 bytes cannot hold the bsid byte"
        );
        // Exactly 6 bytes is the smallest E-AC-3 header this can read, and it
        // must NOT be refused: the guard is `<`, not `<=`.
        let c = parse_dolby(&eac3_frame_fscod3()).expect("6 bytes is enough for the Annex-E BSI");
        assert_eq!(c.bsid, 16);
    }

    #[test]
    fn parse_eac3_refuses_a_frame_shorter_than_its_own_header() {
        // `parse_dolby` guards this today, but `parse_eac3` reads f[5] and so
        // owns the same precondition independently; if the caller's guard ever
        // moves, this one must still refuse rather than panic.
        assert!(parse_eac3(&[0x0B, 0x77, 0x00, 0x00, 0x00]).is_none());
        assert!(parse_eac3(&eac3_frame_fscod3()).is_some(), "6 bytes parses");
    }

    /// Annex-E syncframe with every field distinct: frmsiz=0x123 (292 words
    /// -> 584 bytes), fscod=1 (44.1 kHz), numblkscod=0 (1 block -> 256 samples),
    /// acmod=5 (3/1), lfeon=1 -> 5 channels, bsid=16.
    // Byte-by-byte field layout: docs/mp4-audio.md.
    fn eac3_frame_distinct() -> Vec<u8> {
        vec![0x0B, 0x77, 0x01, 0x23, 0x4B, 0x80, 0x00]
    }

    /// Annex-E syncframe with fscod=3 — the reduced-sample-rate path, which no
    /// other fixture reaches. fscod2=2 -> 16 kHz, 6 blocks; acmod=2, lfeon=0.
    // Byte layout and minimum-length note: docs/mp4-audio.md.
    fn eac3_frame_fscod3() -> Vec<u8> {
        vec![0x0B, 0x77, 0x00, 0x0F, 0xE4, 0x80]
    }

    #[test]
    fn eac3_bsi_fields_are_extracted_from_their_own_lanes() {
        let c = parse_dolby(&eac3_frame_distinct()).expect("parsed");
        assert_eq!(c.bsid, 16);
        assert_eq!(c.fscod, 1, "byte4 bits 7-6");
        assert_eq!(c.sample_rate, 44_100);
        assert_eq!(c.acmod, 5, "3/1: byte4 bits 3-1");
        assert!(c.lfeon, "byte4 bit 0");
        assert_eq!(c.channels, 5, "3/1 + LFE");
        // data_rate exercises frmsiz (11 bits across bytes 2-3) and numblkscod (byte4
        // bits 5-4) together: 584B/256 samples @44.1kHz = 584·8·44100/256/1000 = 804 kbit/s.
        // Dropping frmsiz's high 3 bits, or reading numblkscod as 3 blocks not 1, moves it.
        assert_eq!(c.data_rate_kbps, 804);
    }

    #[test]
    fn dec3_payload_packs_each_field_at_its_spec_offset() {
        // ETSI TS 102 366 Annex F.6.1, EC3SpecificBox (40 bits, one independent
        // substream): data_rate(13)=804 num_ind_sub(3)=0 fscod(2)=1 bsid(5)=16
        // reserved/asvc/bsmod(3)=0 acmod(3)=5 lfeon(1)=1 num_dep_sub(4)=0 → 19 20 60 0B 00.
        let c = parse_dolby(&eac3_frame_distinct()).expect("parsed");
        assert_eq!(dec3_payload(&dec3_box(&c)), [0x19, 0x20, 0x60, 0x0B, 0x00]);
    }

    #[test]
    fn eac3_fscod3_selects_the_reduced_rate_table_by_fscod2() {
        // ETSI TS 102 366 Annex E.1.3.4: when fscod==3, the numblkscod bits become
        // fscod2 instead, indexing a DIFFERENT rate table (frame always 6 blocks).
        // fscod2=2 → 16 kHz; read as numblkscod or wrong-masked, this lands on 24 kHz.
        let c = parse_dolby(&eac3_frame_fscod3()).expect("parsed");
        assert_eq!(c.fscod, 3);
        assert_eq!(c.sample_rate, 16_000, "EAC3_REDUCED_RATES[fscod2=2]");
        assert_eq!(c.acmod, 2);
        assert!(!c.lfeon);
        assert_eq!(c.channels, 2);
        // 6 blocks (1536 samples), 32 bytes → 32·8·16000/1536/1000 = 2 kbit/s.
        assert_eq!(c.data_rate_kbps, 2, "fscod3 frames are always 6 blocks");
    }

    // ── DTS core header + ddts arithmetic ────────────────────────────────────

    /// The 56-bit packed tail of an emitted `ddts` box: 8-byte box header, then
    /// DTSSamplingFrequency(4) + maxBitrate(4) + avgBitrate(4) +
    /// pcmSampleDepth(1) = 13 bytes.
    fn ddts_tail(b: &[u8]) -> [u8; 7] {
        assert_eq!(&b[4..8], b"ddts");
        let t = &b[8 + 13..];
        [t[0], t[1], t[2], t[3], t[4], t[5], t[6]]
    }

    /// maxBitrate / avgBitrate read back out of an emitted `ddts` box.
    fn ddts_bitrates(b: &[u8]) -> (u32, u32) {
        assert_eq!(&b[4..8], b"ddts");
        (
            u32::from_be_bytes([b[12], b[13], b[14], b[15]]),
            u32::from_be_bytes([b[16], b[17], b[18], b[19]]),
        )
    }

    #[test]
    fn dts_core_sync_needs_all_four_bytes() {
        // 0x7FFE8001 is a 32-bit sync; a partial match (leading 0x7F or trailing
        // 0x8001) anchors the parse wrong, so every field after it is read from the
        // wrong offset: AMODE 4 (2 ch) instead of 9 (5.1) out of the same buffer.
        let mut f = vec![0x7F, 0x00, 0x80, 0x01];
        f.extend_from_slice(&[
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x3C, 0x05, 0xF2, 0x77, 0x00, 0x02, 0x00,
        ]);
        let c = parse_dts(&f).expect("parsed at the real core sync");
        assert_eq!(c.amode, 9, "sync is 0x7FFE8001, not a prefix of it");
        assert_eq!(c.channels, 6);
        assert_eq!(c.sample_rate, 48_000);
        assert_eq!(c.core_size, 96);
    }

    #[test]
    fn dts_frame_shorter_than_the_core_header_is_refused_not_indexed() {
        // The guard is `< 11` because byte 10 (LFF) is read after it. Ten bytes
        // must be refused; eleven — the exact header length — must still parse.
        let short = vec![0x7F, 0xFE, 0x80, 0x01, 0x01, 0x3F, 0x00, 0x02, 0x74, 0x00];
        assert_eq!(short.len(), 10);
        assert!(parse_dts(&short).is_none(), "10 bytes cannot hold LFF");
        let exact = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x01, 0x3F, 0x00, 0x02, 0x74, 0x00, 0x0A,
        ];
        assert_eq!(exact.len(), 11);
        let c = parse_dts(&exact).expect("11 bytes is the full core header");
        assert!(c.lfe, "LFF is the last field the guard covers");
    }

    /// A DTS core whose split fields all have their high parts set, so a lost
    /// high bit is visible: NBLKS=79 -> 2560 samples; FSIZE=0x3000 -> core 12289.
    // Bit-position breakdown: docs/mp4-audio.md.
    fn dts_frame_high_bits_set() -> Vec<u8> {
        vec![
            0x7F, 0xFE, 0x80, 0x01, 0x01, 0x3F, 0x00, 0x02, 0x74, 0x00, 0x0A, 0x00,
        ]
    }

    #[test]
    fn dts_split_fields_keep_their_high_bits() {
        // NBLKS (7 bits, bytes 4-5) and FSIZE (14 bits, bytes 5-7; ETSI TS 102 114
        // §5.3.1) straddle byte boundaries. Shifting the high part wrong drops it
        // silently: 79 blocks becomes 15, a 12289-byte core becomes 1 — into the bitrate.
        let c = parse_dts(&dts_frame_high_bits_set()).expect("parsed");
        assert_eq!(c.frame_samples, 2560, "NBLKS 79 → (79+1)·32");
        assert_eq!(c.core_size, 12_289, "FSIZE 0x3000 + 1");
        assert_eq!(c.amode, 9);
        assert_eq!(c.sample_rate, 48_000);
    }

    #[test]
    fn dts_lff_is_masked_out_of_its_neighbours_not_xored() {
        // LFF is 2 bits at byte10 bits 2-1; byte10=0x0A also sets bit3, so `>>1`
        // gives 0b101 and only a MASK isolates LFF=1. The 5.1 fixtures can't catch
        // an XOR bug here (0b110=2 is also "LFE present"); this drops a channel instead.
        let c = parse_dts(&dts_frame_high_bits_set()).expect("parsed");
        assert!(c.lfe, "LFF = 1");
        assert_eq!(c.channels, 6, "AMODE 9 (5 ch) + LFE");
        assert_eq!(c.channel_layout & 0x0008, 0x0008, "LFE bit in the mask");
    }

    #[test]
    fn dts_ext_sync_in_the_last_core_byte_is_not_an_extension() {
        // EXSS search starts exactly at core_size=FSIZE+1. Here FSIZE=9 (core_size
        // 10) and the ext-sync begins at byte 9 — the LAST core byte, just before
        // the window. An off-by-one either way tests that byte and wrongly emits `dtsh`.
        let f = vec![
            0x7F, 0xFE, 0x80, 0x01, 0x00, 0x00, 0x00, 0x92, 0x74, 0x64, 0x58, 0x20, 0x25,
        ];
        let c = parse_dts(&f).expect("parsed");
        assert_eq!(c.core_size, 10, "FSIZE 9 + 1");
        assert!(
            !c.has_extension,
            "the pattern ends at the core's last byte, inside it"
        );
        let e = dolby_sample_entry(Codec::Dts, &f).expect("entry built");
        assert_eq!(&e[4..8], b"dtsc");
    }

    #[test]
    fn ddts_bitrate_is_the_rounded_core_rate() {
        // core_size·8·sample_rate/frame_samples, rounded to nearest. 8·8·44100 =
        // 2822400 bits per 512 samples → 5512.5 bit/s, must round UP to 5513 via
        // +den/2 — otherwise the declared rate is systematically low.
        let c = DtsConfig {
            sample_rate: 44_100,
            channels: 6,
            amode: 9,
            lfe: true,
            core_size: 8,
            frame_samples: 512,
            has_extension: false,
            channel_layout: dts_channel_layout(9, true),
        };
        let (max, avg) = ddts_bitrates(&ddts_box(&c));
        assert_eq!(max, 5513, "2822400/512 = 5512.5 → 5513");
        assert_eq!(avg, 5513, "both fields carry the same computed rate");
    }

    #[test]
    fn ddts_bitrate_is_zero_rather_than_a_division_by_zero() {
        // frame_samples is the divisor. A core header that yields 0 samples per
        // frame must produce a 0 bitrate, not divide by it.
        let c = DtsConfig {
            sample_rate: 48_000,
            channels: 6,
            amode: 9,
            lfe: true,
            core_size: 96,
            frame_samples: 0,
            has_extension: false,
            channel_layout: dts_channel_layout(9, true),
        };
        assert_eq!(ddts_bitrates(&ddts_box(&c)), (0, 0));
    }

    #[test]
    fn ddts_frame_duration_code_covers_every_band() {
        // FrameDuration (tail's top 2 bits) codes core length: 0=512, 1=1024,
        // 2=2048, 3=4096 samples. NBLKS is 7 bits, so (NBLKS+1)·32 reaches 4096
        // and every band is live — the wrong band declares the wrong duration.
        let dur = |frame_samples| {
            let c = DtsConfig {
                sample_rate: 48_000,
                channels: 6,
                amode: 9,
                lfe: true,
                core_size: 96,
                frame_samples,
                has_extension: false,
                channel_layout: dts_channel_layout(9, true),
            };
            ddts_tail(&ddts_box(&c))[0] >> 6
        };
        assert_eq!(dur(512), 0, "512 samples");
        assert_eq!(dur(1024), 1, "1024 samples");
        assert_eq!(dur(2048), 2, "2048 samples");
        assert_eq!(dur(4096), 3, "4096 samples");
        // Band boundaries, so a band cannot quietly absorb its neighbour.
        assert_eq!(dur(513), 1);
        assert_eq!(dur(1025), 2);
        assert_eq!(dur(2049), 3);
    }

    #[test]
    fn sample_entry_samplerate_does_not_overflow_at_96k() {
        // 96 kHz > 65535: the 16.16 integer part must saturate, not wrap to garbage.
        let e = audio_sample_entry(b"ac-3", 6, 96_000, &[]);
        // 8-byte box header + body offset 24 (6+2+8+2+2+2+2) → samplerate at 32;
        // high 16 bits = the integer rate.
        assert_eq!(&e[32..34], &[0xFF, 0xFF], "capped to 65535, not wrapped");
    }
}

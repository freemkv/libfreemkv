//! MP4 audio sample entries and codec-config boxes for the `mp4://` muxer.
//!
//! Covers codecs that map cleanly into MP4: **AC-3** (`ac-3` + `dac3`),
//! **E-AC-3 / Dolby Digital Plus** (`ec-3` + `dec3`, incl. Atmos-in-DD+ JOC),
//! **DTS / DTS-HD** (`dtsc`/`dtsh` + `ddts`, core described, whole access
//! units passed through so an HD decoder finds the extension), and AAC / MPEG-1/2
//! audio (`mp4a` + `esds`). Config boxes
//! are derived from the first audio frame's bitstream (ISO/IEC 14496-12
//! amendments; ETSI TS 102 366 / 102 114). Codecs with no clean MP4 mapping
//! (TrueHD, LPCM, bitmap subtitles) are excluded by the fit oracle.

use super::boxes::bx;
use crate::disc::Codec;

/// AC-3 / E-AC-3 sample rates indexed by `fscod` (byte-4 bits 7-6).
const FSCOD_RATES: [u32; 3] = [48_000, 44_100, 32_000];
/// E-AC-3 reduced rates indexed by `fscod2` (byte-4 bits 5-4) when `fscod == 3`.
const EAC3_REDUCED_RATES: [u32; 4] = [24_000, 22_050, 16_000, 48_000];
/// Base channel count per `acmod` (A/52 Table 5.8), before the LFE.
const ACMOD_CHANNELS: [u8; 8] = [2, 1, 2, 3, 3, 4, 4, 5];
// Lowest `bsid` that is Annex-E (E-AC-3); 8 is AC-3, 9/10 are Annex D.
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
    // Read `n` bits (n <= 32). Returns 0 past end of data (callers pre-check len). `|` (not
    // `^`) is intentional here and in the `push` closures below.
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
        sample_rate: *FSCOD_RATES.get(fscod as usize)?,
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
        if fscod2 == 0x03 {
            return None; // reserved
        }
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
// data_rate must be non-zero; only parse_eac3 computes one.
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

/// Build an audio sample entry (`ac-3` / `ec-3` / `dtsc` / `dtsh` / `mp4a`) with the
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

/// The integer sample rate `audio_sample_entry` wrote into `entry`, if it is long enough.
pub(super) fn entry_sample_rate(entry: &[u8]) -> Option<u32> {
    let rate = entry.get(32..36)?;
    Some(u32::from_be_bytes([rate[0], rate[1], rate[2], rate[3]]) >> 16)
}

// ── DTS (dtsc/dtsh + ddts) ───────────────────────────────────────────────────

/// DTS core `SFREQ` (4-bit) → sample rate (Hz); 0 marks an invalid code
/// (0, 4, 5, 9, 10). Codes 14 and 15 map to 96 kHz / 192 kHz.
const DTS_SFREQ: [u32; 16] = [
    0, 8_000, 16_000, 32_000, 0, 0, 11_025, 22_050, 44_100, 0, 0, 12_000, 24_000, 48_000, 96_000,
    192_000,
];
// DTS core base channel count per AMODE (ETSI TS 102 114 §5.3.1); matches the decodability
// gate's DTS_AMODE_COUNT in dts.rs.
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
    if sample_rate == 0 {
        return None;
    }
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

// `ddts` ChannelLayout speaker mask per core AMODE (bit0=C, bit1=L/R,... bit15=Lhr/Rhr; paired
// bits = two speakers). Must stay consistent with DTS_AMODE_CH.
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
fn ddts_box(c: &DtsConfig, max_rate: u32) -> Vec<u8> {
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
    out.extend_from_slice(&max_rate.to_be_bytes()); // DTSSamplingFrequency
    out.extend_from_slice(&bitrate.to_be_bytes()); // maxBitrate
    out.extend_from_slice(&bitrate.to_be_bytes()); // avgBitrate
    out.push(if c.has_extension { 24 } else { 16 }); // pcmSampleDepth
    // FrameDuration counts samples at DTSSamplingFrequency, not the core rate.
    let at_max = (u64::from(c.frame_samples) * u64::from(max_rate))
        .checked_div(u64::from(c.sample_rate))
        .unwrap_or(u64::from(c.frame_samples));
    // Bit-packed tail (56 bits): FrameDuration2 StreamConstruction5 CoreLFEPresent1
    // CoreLayout6 CoreSize14 StereoDownmix1 RepresentationType3 ChannelLayout16
    // MultiAssetFlag1 LBRDurationMod1 ReservedBoxPresent1 Reserved5
    let frame_duration = match at_max {
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

// DTS sample-entry samplerate: the family base of the maximum rate (ETSI TS 102 114
// E.2.2.2); the 4x core rates join their family per Table 5-6.
fn dts_entry_rate(max_rate: u32) -> u32 {
    match max_rate {
        12_000 | 24_000 | 48_000 | 96_000 | 192_000 => 48_000,
        11_025 | 22_050 | 44_100 | 88_200 | 176_400 => 44_100,
        8_000 | 16_000 | 32_000 | 64_000 | 128_000 => 32_000,
        other => other,
    }
}

/// The complete MP4 AudioSampleEntry (Dolby and DTS) for an audio frame, or `None`
/// if the codec has no MP4 mapping here. Together with [`audio_fits`] this is the fit oracle for
/// audio: only what returns `Some` is muxable.
/// `stream_hz` is the title's rate for the track (0 if unknown).
pub(super) fn dolby_sample_entry(
    codec: Codec,
    first_frame: &[u8],
    stream_hz: u32,
) -> Option<Vec<u8>> {
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
            // The core may run at a divisor of the stream rate (48 kHz core in 96 kHz MA).
            let max_rate = if stream_hz > c.sample_rate && stream_hz.is_multiple_of(c.sample_rate) {
                stream_hz
            } else {
                c.sample_rate
            };
            Some(audio_sample_entry(
                fourcc,
                c.channels,
                dts_entry_rate(max_rate),
                &ddts_box(&c, max_rate),
            ))
        }
        _ => None,
    }
}

/// (channels, rate) of an AudioSpecificConfig the `esds` can carry: a table rate and
/// channel configuration, no escaped object type, short enough for one-byte sizes
/// (the 100-byte cap keeps every enclosing descriptor body under 128).
pub(super) fn aac_config(asc: &[u8]) -> Option<(u16, u32)> {
    const RATES: [u32; 12] = [
        96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000,
    ];
    let (b0, b1) = (*asc.first()?, *asc.get(1)?);
    if b0 >> 3 == 31 || asc.len() > 100 {
        return None;
    }
    let rate = *RATES.get(usize::from(((b0 & 7) << 1) | (b1 >> 7)))?;
    let channels = match (b1 >> 3) & 0x0F {
        c @ 1..=6 => u16::from(c),
        7 => 8,
        _ => return None,
    };
    Some((channels, rate))
}

// One MPEG-4 descriptor (14496-1 §7.2.2.1): tag, one-byte size (every body here is < 128).
fn descriptor(tag: u8, body: &[u8]) -> Vec<u8> {
    [&[tag, body.len() as u8][..], body].concat()
}

/// The `mp4a` + `esds` AudioSampleEntry (14496-14 §5.6) for AAC (from its ASC) or MPEG-1/2
/// audio (from the first frame header), or `None` if neither describes the track.
pub(super) fn mpeg_sample_entry(codec: Codec, first_frame: &[u8], asc: &[u8]) -> Option<Vec<u8>> {
    // (objectTypeIndication, channels, rate, DecoderSpecificInfo)
    let (oti, channels, rate, dsi) = match codec {
        Codec::Aac => {
            let (channels, rate) = aac_config(asc)?;
            (0x40, channels, rate, descriptor(0x05, asc))
        }
        Codec::Mp2 | Codec::Mp3 => {
            let h = first_frame.get(..4)?;
            if h[0] != 0xFF || h[1] & 0xE0 != 0xE0 || h[1] & 0x06 == 0 {
                return None;
            }
            let base = [44_100, 48_000, 32_000].get(usize::from((h[2] >> 2) & 3))?;
            // version '11' MPEG-1 (11172-3), '10' MPEG-2 half rates, '00' MPEG-2.5 quarter.
            let (oti, rate) = match (h[1] >> 3) & 3 {
                3 => (0x6B, *base),
                2 => (0x69, base / 2),
                0 => (0x69, base / 4),
                _ => return None,
            };
            (oti, if h[3] >> 6 == 3 { 1 } else { 2 }, rate, Vec::new())
        }
        _ => return None,
    };
    // streamType 5 (audio) << 2 | reserved 1; bufferSizeDB, max/avg bitrate unknown (0).
    let mut config = vec![oti, 0x15];
    config.extend_from_slice(&[0; 11]);
    config.extend_from_slice(&dsi);
    let mut es = vec![0, 0, 0];
    es.extend(descriptor(0x04, &config));
    es.extend(descriptor(0x06, &[0x02]));
    let esds = bx(b"esds", &[&[0u8; 4][..], &descriptor(0x03, &es)].concat());
    Some(audio_sample_entry(b"mp4a", channels, rate, &esds))
}

/// Fit oracle for an audio codec: does `mp4://` currently carry it?
// TrueHD and LPCM are not mapped; skipped with a loud report, not silently dropped.
pub(super) fn audio_fits(codec: Codec) -> bool {
    matches!(
        codec,
        Codec::Ac3
            | Codec::Ac3Plus
            | Codec::Dts
            | Codec::DtsHdMa
            | Codec::DtsHdHr
            | Codec::Aac
            | Codec::Mp2
            | Codec::Mp3
    )
}

#[cfg(test)]
#[path = "audio_tests.rs"]
mod tests;

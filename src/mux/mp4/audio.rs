//! MP4 audio sample entries and codec-config boxes for the `mp4://` muxer.
//!
//! Covers the codecs that map cleanly into MP4 and play widely: **AC-3**
//! (`ac-3` + `dac3`), **E-AC-3 / Dolby Digital Plus** (`ec-3` + `dec3`, incl.
//! Atmos-in-DD+ JOC), and **DTS / DTS-HD** (`dtsc`/`dtsh` + `ddts`, describing
//! the core with whole access units passed through so an HD decoder finds the
//! extension). Config boxes are derived from the first audio frame's bitstream
//! (ISO/IEC 14496-12 amendments; ETSI TS 102 366 / 102 114). Codecs with no
//! clean MP4 mapping (TrueHD, LPCM, bitmap subtitles) are excluded by the fit
//! oracle in the sink.

use super::boxes::bx;
use crate::disc::Codec;

/// AC-3 / E-AC-3 sample rates indexed by `fscod` (byte-4 bits 7-6).
const FSCOD_RATES: [u32; 3] = [48_000, 44_100, 32_000];
/// E-AC-3 reduced rates indexed by `fscod2` (byte-4 bits 5-4) when `fscod == 3`.
const EAC3_REDUCED_RATES: [u32; 4] = [24_000, 22_050, 16_000, 48_000];
/// Base channel count per `acmod` (A/52 Table 5.8), before the LFE.
const ACMOD_CHANNELS: [u8; 8] = [2, 1, 2, 3, 3, 4, 4, 5];

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
    /// Read `n` bits (n ≤ 32). Returns 0 past end of data (callers pre-check len).
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
    if bsid >= 11 {
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
    let data_rate_kbps = if samples > 0 {
        ((frame_bytes * 8 * sample_rate as u64) / samples / 1000) as u16
    } else {
        0
    };

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

/// The `dec3` config box (ETSI TS 102 366 Annex G.3) for a single independent
/// substream, no dependent substreams: data_rate(13) num_ind_sub(3) then
/// fscod(2) bsid(5) reserved(1) asvc(1) bsmod(3) acmod(3) lfeon(1) reserved(3)
/// num_dep_sub(4) reserved(1).
pub(super) fn dec3_box(c: &DolbyConfig) -> Vec<u8> {
    let mut v: u64 = 0;
    let mut push = |val: u64, bits: u32| v = (v << bits) | (val & ((1u64 << bits) - 1));
    push(c.data_rate_kbps as u64, 13);
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

/// Build an audio sample entry (`ac-3` / `ec-3`) with the given config box.
/// `AudioSampleEntry` per ISO/IEC 14496-12 §12.2.3.
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
    // samplerate is 16.16 fixed point; the integer rate in the high 16 bits.
    e.extend_from_slice(&(sample_rate << 16).to_be_bytes());
    e.extend_from_slice(config);
    bx(fourcc, &e)
}

// ── DTS (dtsc/dtsh + ddts) ───────────────────────────────────────────────────

/// DTS core `SFREQ` (4-bit) → sample rate (Hz). Reserved indices → 48 kHz.
const DTS_SFREQ: [u32; 16] = [
    48_000, 8_000, 16_000, 32_000, 48_000, 48_000, 11_025, 22_050, 44_100, 48_000, 48_000, 12_000,
    24_000, 48_000, 96_000, 192_000,
];
/// DTS core base channel count per `AMODE` (0..=9); higher AMODEs are rare on disc.
const DTS_AMODE_CH: [u8; 10] = [1, 2, 2, 2, 2, 3, 3, 4, 4, 5];

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
    let base_ch = DTS_AMODE_CH.get(amode).copied().unwrap_or(6);
    let channels = base_ch as u16 + lfe as u16;
    let channel_layout = dts_channel_layout(amode, lfe);
    // DTS-HD extension substream sync (0x64582025) after the core frame.
    let ext_sync = [0x64, 0x58, 0x20, 0x25];
    let has_extension = f
        .windows(4)
        .skip((fsize as usize + 1).min(f.len()).saturating_sub(4))
        .any(|w| w == ext_sync)
        || f.windows(4).any(|w| w == ext_sync);

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

/// `ddts` ChannelLayout (16-bit speaker mask) for the common core layouts.
/// bit0=C, bit1=L/R, bit2=Ls/Rs, bit3=LFE.
fn dts_channel_layout(amode: usize, lfe: bool) -> u16 {
    let mut m = match amode {
        0 => 0x0001,     // C (mono)
        1..=4 => 0x0002, // L/R
        5 => 0x0003,     // C + L/R
        6 | 8 => 0x0006, // L/R + Ls/Rs (no centre)
        _ => 0x0007,     // C + L/R + surround (amode 7, 9, …)
    };
    if lfe {
        m |= 0x0008;
    }
    m
}

/// The `ddts` config box (ETSI TS 102 114 Annex; DTS-in-ISO registration).
/// Describes the DTS core; whole access units (core + any extension) are passed
/// through as samples, so a DTS-HD-aware decoder still finds the extension.
fn ddts_box(c: &DtsConfig) -> Vec<u8> {
    // avg/max bitrate: computed from the core frame size × frame rate (the core
    // RATE field reads "open/variable" for lossless, so it's not usable directly).
    let frames_per_sec = if c.frame_samples > 0 {
        c.sample_rate as u64 / c.frame_samples as u64
    } else {
        0
    };
    let bitrate = (c.core_size as u64 * 8 * frames_per_sec) as u32;

    let mut out = Vec::new();
    out.extend_from_slice(&c.sample_rate.to_be_bytes()); // DTSSamplingFrequency
    out.extend_from_slice(&bitrate.to_be_bytes()); // maxBitrate
    out.extend_from_slice(&bitrate.to_be_bytes()); // avgBitrate
    out.push(if c.has_extension { 24 } else { 16 }); // pcmSampleDepth
    // Bit-packed tail (56 bits):
    // FrameDuration2 StreamConstruction5 CoreLFEPresent1 CoreLayout6 CoreSize14
    // StereoDownmix1 RepresentationType3 ChannelLayout16 MultiAssetFlag1
    // LBRDurationMod1 ReservedBoxPresent1 Reserved5
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
    push(c.core_size as u128, 14);
    push(0, 1); // StereoDownmix
    push(0, 3); // RepresentationType
    push(c.channel_layout as u128, 16);
    push(c.has_extension as u128, 1); // MultiAssetFlag
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
            Some(audio_sample_entry(
                b"ec-3",
                c.channels,
                c.sample_rate,
                &dec3_box(&c),
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

/// Fit oracle for an audio codec: does `mp4://` currently carry it? Covers the
/// Dolby family (AC-3 / E-AC-3) and DTS (core / DTS-HD HRA / DTS-HD MA — the core
/// is described, whole access units pass through). TrueHD, LPCM, AAC are not yet
/// mapped and are skipped with a loud report (never silently dropped).
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

    #[test]
    fn eac3_bsi_and_dec3() {
        // E-AC-3: syncword | strmtyp/substreamid/frmsiz | fscod/numblks/acmod/lfeon | bsid
        let mut f = vec![0x0B, 0x77];
        f.push(0x00); // strmtyp=0, substreamid=0, frmsiz high=0
        f.push(0x3F); // frmsiz low = 63 → frame 128 bytes
        // byte4: fscod(2)=0 | numblkscod(2)=3 (6 blocks) | acmod(3)=7 | lfeon(1)=1
        f.push(0b00_11_111_1);
        // byte5: bsid(5)=16 (E-AC-3) | dialnorm high(3)
        f.push(0b10000_000);
        f.push(0x00);
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
}

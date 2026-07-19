//! MP4 audio sample entries and codec-config boxes for the `mp4://` muxer.
//!
//! Covers the Dolby family that maps cleanly into MP4 and plays widely:
//! **AC-3** (`ac-3` + `dac3`) and **E-AC-3 / Dolby Digital Plus** (`ec-3` +
//! `dec3`, which also carries Atmos-in-DD+ JOC). The config boxes are derived
//! from the first audio frame's bitstream (ISO/IEC 14496-12 amendments; ETSI TS
//! 102 366 Annexes F/G). Codecs with no clean MP4 mapping (TrueHD, DTS family,
//! LPCM, bitmap subtitles) are handled by the fit oracle in the sink, not here.

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

/// The MP4 fourcc + config box for a Dolby audio frame, or `None` if the codec
/// has no MP4 mapping here. Together with [`audio_fits`] this is the fit oracle
/// for audio: only what returns `Some` is muxable.
pub(super) fn dolby_sample_entry(codec: Codec, first_frame: &[u8]) -> Option<Vec<u8>> {
    let c = parse_dolby(first_frame)?;
    let (fourcc, config): (&[u8; 4], Vec<u8>) = match codec {
        Codec::Ac3 => (b"ac-3", dac3_box(&c)),
        Codec::Ac3Plus => (b"ec-3", dec3_box(&c)),
        _ => return None,
    };
    Some(audio_sample_entry(
        fourcc,
        c.channels,
        c.sample_rate,
        &config,
    ))
}

/// Fit oracle for an audio codec: does `mp4://` currently carry it? M2 covers
/// the Dolby family (AC-3 / E-AC-3). TrueHD, DTS(-HD), LPCM, AAC are not yet
/// mapped and are skipped with a loud report (never silently dropped).
pub(super) fn audio_fits(codec: Codec) -> bool {
    matches!(codec, Codec::Ac3 | Codec::Ac3Plus)
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
    fn fit_oracle_dolby_only() {
        assert!(audio_fits(Codec::Ac3));
        assert!(audio_fits(Codec::Ac3Plus));
        assert!(!audio_fits(Codec::TrueHd));
        assert!(!audio_fits(Codec::DtsHdMa));
        assert!(!audio_fits(Codec::Lpcm));
    }
}

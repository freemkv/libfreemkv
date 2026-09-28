//! Channel count a stored MPEG audio Layer II track decodes to. Review against, and only change
//! with a citation from:
//! - ISO/IEC 11172-3 (CD text): §2.4.2.3 header, §2.4.1.6 Layer II `audio_data()`, §2.4.2.6,
//!   §2.4.3.1 CRC, §2.4.3.3 nbal/sblimit, 3-Annex B Tables 3-B.2a-d and 3-B.4. Annex B is not in
//!   the public text: table data is the ISO reference software's (dist10 `tables/alloc_0..3`).
//!   Real-encoder test frames are FFmpeg 9.0.2 output of a synthetic sine
//!   (`testdata/ffmpeg_mp2/PROVENANCE` holds the commands).
//! - ISO/IEC 13818-3 second edition (WG11 N1519, 1997): §2.5.1.3 `base_frame()`, §2.5.1.13-15
//!   and §2.5.1.17 syntax, §2.5.2.13/15/17 semantics, §2.5.3.1 CRC-gated multichannel detection.
//!
//! Multichannel data is trusted only when its mandatory `mc_crc_check` verifies (§2.5.3.1) in
//! [`RUN_FRAMES`] frames in a row, and the base frame's own CRC when present. Only base frames
//! (PES `0xC0|n`) are muxed: with `ext_bit_stream_present` set the track holds just the MPEG-1
//! channels.

/// 11172-3 §2.4.2.3: "The first 32 bits (four bytes) are header information".
const HEADER_BITS: usize = 32;
/// 11172-3 §2.4.2.4: "crc_check - a 16 bit parity-check word".
const CRC_BITS: usize = 16;
/// 11172-3 §2.4.1.6: "for (gr=0; gr<12; gr++)" - twelve granules of samples per frame.
const GRANULES: usize = 12;
/// 11172-3 §2.4.1.6: "`scalefactor[ch][sb][0]` 6 bits".
const SCALEFACTOR_BITS: usize = 6;
/// Frames looked at before settling on the base count (freemkv policy, bounded work).
pub(crate) const MAX_FRAMES: u32 = 32;

/// Why a frame's multichannel count could not be trusted; the base count stands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fallback {
    /// ID '0' (13818-3 lower sampling frequencies) uses its Table B.1, not handled here.
    LowSamplingFrequency,
    /// Layer I/III place mc data differently (13818-3 §2.5.1.2, §2.5.1.4).
    NotLayerII,
    /// Free format carries no bitrate, which the Annex B table choice needs.
    FreeFormat,
    /// The frame ends inside the named field.
    Truncated(&'static str),
    /// 11172-3 §2.4.3.1 frame CRC does not match: the frame is damaged.
    FrameCrc,
    /// 13818-3 §2.5.3.1: no valid `mc_crc_check`, so no multichannel extension.
    McCrc,
    /// 13818-3 §2.5.2.13 centre "'10' not defined".
    UndefinedCentre,
    /// 13818-3 §2.5.2.15 dyn_cross_mode value marked "forbidden".
    ForbiddenDynCross,
}

/// The header fields the channel count depends on (11172-3 §2.4.2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    id: u8,
    layer: u8,
    protection_bit: u8,
    bitrate_kbps: Option<u32>,
    sampling_hz: u32,
    padding: bool,
    mode: u8,
    mode_extension: u8,
}

/// 11172-3 §2.4.2.3 mode: "'11' single_channel".
pub(crate) const MODE_SINGLE_CHANNEL: u8 = 0b11;
/// 11172-3 §2.4.2.3 mode: "'01' joint_stereo (intensity_stereo and/or ms_stereo)".
pub(crate) const MODE_JOINT_STEREO: u8 = 0b01;
/// 11172-3 §2.4.2.3 Layer: "\"10\" Layer II".
const LAYER_II: u8 = 0b10;

impl Header {
    /// Parses the 32-bit header, or `None` when it is not a valid MPEG audio header.
    pub(crate) fn parse(frame: &[u8]) -> Option<Header> {
        let h = u32::from_be_bytes(frame.get(..4)?.try_into().ok()?);
        if h >> 20 != 0xFFF {
            return None; // §2.4.2.3: "syncword - the bit string '1111 1111 1111'."
        }
        let layer = ((h >> 17) & 0b11) as u8;
        if layer == 0 {
            return None; // §2.4.2.3 Layer: "\"00\" reserved"
        }
        let bitrate_index = (h >> 12) & 0xF;
        let sampling_hz = match (h >> 10) & 0b11 {
            0b00 => 44_100,   // §2.4.2.3: "'00' 44.1 kHz"
            0b01 => 48_000,   // §2.4.2.3: "'01' 48 kHz"
            0b10 => 32_000,   // §2.4.2.3: "'10' 32 kHz"
            _ => return None, // §2.4.2.3: "'11' reserved"
        };
        let bitrate_kbps = match bitrate_index {
            0 => None,         // §2.4.2.3: "The all zero value indicates the 'free format' condition"
            15 => return None, // '1111' is absent from the §2.4.2.3 bitrate table
            i => Some(LAYER_II_KBPS[i as usize]),
        };
        let h = Header {
            id: ((h >> 19) & 1) as u8,
            layer,
            protection_bit: ((h >> 16) & 1) as u8,
            bitrate_kbps,
            sampling_hz,
            padding: (h >> 9) & 1 == 1,
            mode: ((h >> 6) & 0b11) as u8,
            mode_extension: ((h >> 4) & 0b11) as u8,
        };
        (!h.disallowed_layer_ii_combination()).then_some(h)
    }

    // §2.4.2.3 (1993 text): "For Layer II, not all combinations of total bitrate and mode are
    // allowed": 32, 48, 56, 80 "single_channel"; 224-384 "stereo, intensity stereo, dual
    // channel"; free format and 64, 96-192 "all modes". Rejected: not a Layer II frame.
    fn disallowed_layer_ii_combination(&self) -> bool {
        let (Some(kbps), true, 1) = (self.bitrate_kbps, self.layer == LAYER_II, self.id) else {
            return false;
        };
        if self.mode == MODE_SINGLE_CHANNEL {
            matches!(kbps, 224 | 256 | 320 | 384)
        } else {
            matches!(kbps, 32 | 48 | 56 | 80)
        }
    }

    /// 13818-3 symbols: "nch Number of channels; equal to 1 for single_channel mode, 2 in
    /// other modes."
    pub(crate) fn nch(&self) -> u8 {
        if self.mode == MODE_SINGLE_CHANNEL {
            1
        } else {
            2
        }
    }

    // 11172-3 §2.4.3.1 (Layer II): "N = 144 * bitrate / sampling_frequency" slots of one byte,
    // plus one when the padding bit is set.
    pub(crate) fn frame_bytes(&self) -> Option<usize> {
        let kbps = self.bitrate_kbps? as usize;
        Some(144_000 * kbps / self.sampling_hz as usize + usize::from(self.padding))
    }
}

/// 11172-3 §2.4.2.3 bit_rate_index table, "Layer II" column (index 0 free format, 15 unused).
const LAYER_II_KBPS: [u32; 15] = [
    0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
];

/// One Table 3-B.4 quantisation class: bits per codeword and whether 3 samples are grouped.
#[derive(Clone, Copy)]
struct Class {
    bits: u8,
    grouped: bool,
}

const fn c(bits: u8, grouped: bool) -> Class {
    Class { bits, grouped }
}

// Table 3-B.4 classes as dist10 lists them (steps bits group): 3 levels = 5 bits grouped,
// 5 = 7 grouped, 7 = 3, 9 = 10 grouped, then 15..65535 levels = 4..16 bits, one per sample.
const Q3: Class = c(5, true);
const Q5: Class = c(7, true);
const Q7: Class = c(3, false);
const Q9: Class = c(10, true);
const fn q(bits: u8) -> Class {
    c(bits, false)
}

/// One Table 3-B.2 row: `nbal`, then the class for allocation values 1..2^nbal-1.
struct Row {
    nbal: u8,
    classes: &'static [Class],
}

// Table 3-B.2a/b rows (dist10 alloc_0/alloc_1): sb 0-2, 3-10, 11-22, 23-29.
#[rustfmt::skip]
const AB_0_2: Row = Row { nbal: 4, classes: &[
    Q3, Q7, q(4), q(5), q(6), q(7), q(8), q(9), q(10), q(11), q(12), q(13), q(14), q(15), q(16),
] };
#[rustfmt::skip]
const AB_3_10: Row = Row { nbal: 4, classes: &[
    Q3, Q5, Q7, Q9, q(4), q(5), q(6), q(7), q(8), q(9), q(10), q(11), q(12), q(13), q(16),
] };
const AB_11_22: Row = Row {
    nbal: 3,
    classes: &[Q3, Q5, Q7, Q9, q(4), q(5), q(16)],
};
const AB_23_29: Row = Row {
    nbal: 2,
    classes: &[Q3, Q5, q(16)],
};
#[rustfmt::skip]
const TABLE_AB: [&Row; 30] = [
    &AB_0_2, &AB_0_2, &AB_0_2,
    &AB_3_10, &AB_3_10, &AB_3_10, &AB_3_10, &AB_3_10, &AB_3_10, &AB_3_10, &AB_3_10,
    &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22,
    &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22, &AB_11_22,
    &AB_23_29, &AB_23_29, &AB_23_29, &AB_23_29, &AB_23_29, &AB_23_29, &AB_23_29,
];

// Table 3-B.2c/d rows (dist10 alloc_2/alloc_3): sb 0-1, 2-11.
#[rustfmt::skip]
const CD_0_1: Row = Row { nbal: 4, classes: &[
    Q3, Q5, Q9, q(4), q(5), q(6), q(7), q(8), q(9), q(10), q(11), q(12), q(13), q(14), q(15),
] };
const CD_2_11: Row = Row {
    nbal: 3,
    classes: &[Q3, Q5, Q9, q(4), q(5), q(6), q(7)],
};
#[rustfmt::skip]
const TABLE_CD: [&Row; 12] = [
    &CD_0_1, &CD_0_1,
    &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11, &CD_2_11,
];

/// The Table 3-B.2 rows in use, `sblimit` long (a: 27, b: 30, c: 8, d: 12).
fn allocation_table(h: &Header, kbps: u32) -> &'static [&'static Row] {
    // dist10 pick_table: "decision rules refer to per-channel bitrates (kbits/sec/chan)"
    let per_ch = kbps / h.nch() as u32;
    let fs = h.sampling_hz;
    if (fs == 48_000 && per_ch >= 56) || (56..=80).contains(&per_ch) {
        &TABLE_AB[..27] // 3-B.2a, sblimit 27
    } else if fs != 48_000 && per_ch >= 96 {
        &TABLE_AB[..30] // 3-B.2b, sblimit 30
    } else if fs != 32_000 && per_ch <= 48 {
        &TABLE_CD[..8] // 3-B.2c, sblimit 8
    } else {
        &TABLE_CD[..12] // 3-B.2d, sblimit 12
    }
}

// 13818-3 §2.5.2.17 allocation[mch][sb]: "Table B.2.a shall be used if Fs equals 48 kHz,
// table B.2.b shall be used if Fs equals 44,1 kHz or 32 kHz, regardless of the bitrate."
fn mc_allocation_table(h: &Header) -> &'static [&'static Row] {
    if h.sampling_hz == 48_000 {
        &TABLE_AB[..27]
    } else {
        &TABLE_AB[..30]
    }
}

/// 11172-3 §2.4.3.1 CRC: "G(X) = X16 + X15 + X2 + 1"; "The initial state of the shift register
/// is '1111 1111 1111 1111'."
struct Crc16(u16);

impl Crc16 {
    fn new() -> Self {
        Crc16(0xFFFF)
    }

    // Feeds bits `start..end` of `data`, MSB first.
    fn feed(&mut self, data: &[u8], start: usize, end: usize) {
        for pos in start..end {
            let bit = u16::from((data[pos / 8] >> (7 - pos % 8)) & 1);
            let carry = self.0 >> 15;
            self.0 <<= 1;
            if carry ^ bit == 1 {
                self.0 ^= 0x8005; // X16 + X15 + X2 + 1
            }
        }
    }
}

/// A bounds-checked MSB-first bit cursor over one frame, ending at bit `end`.
struct Bits<'a> {
    data: &'a [u8],
    pos: usize,
    end: usize,
}

impl Bits<'_> {
    fn read(&mut self, n: usize, field: &'static str) -> Result<u32, Fallback> {
        if n > 32 || self.pos + n > self.end {
            return Err(Fallback::Truncated(field));
        }
        let mut v = 0u32;
        for _ in 0..n {
            let bit = (self.data[self.pos / 8] >> (7 - self.pos % 8)) & 1;
            v = (v << 1) | u32::from(bit);
            self.pos += 1;
        }
        Ok(v)
    }

    fn skip(&mut self, n: usize, field: &'static str) -> Result<(), Fallback> {
        match self.pos.checked_add(n) {
            Some(end) if end <= self.end => {
                self.pos = end;
                Ok(())
            }
            _ => Err(Fallback::Truncated(field)),
        }
    }
}

/// The base (MPEG-1 compatible) part of a Layer II frame, walked to its end.
struct Base {
    /// T0/T1 allocations (13818-3 §2.5.2.15: "T0 always contains Lo, and T1 always contains
    /// Ro"); zero above this frame's sblimit.
    alloc: [[u8; 32]; 2],
    /// First bit after `mpeg1_audio_data()`, where §2.5.1.3 puts `mc_extension_data_part1()`.
    mc_start: usize,
}

// Walks `mpeg1_audio_data()` (11172-3 §2.4.1.6) and checks the frame CRC when one is present.
fn walk_base(frame: &[u8], h: &Header, end: usize) -> Result<Base, Fallback> {
    if h.id == 0 {
        return Err(Fallback::LowSamplingFrequency);
    }
    if h.layer != LAYER_II {
        return Err(Fallback::NotLayerII);
    }
    let kbps = h.bitrate_kbps.ok_or(Fallback::FreeFormat)?;
    let table = allocation_table(h, kbps);
    let sblimit = table.len();
    let nch = h.nch() as usize;
    // §2.4.2.3 mode_extension "bound==4" ... "bound==16", capped at sblimit as FFmpeg
    // (mp_decode_layer2 "if (bound > sblimit) bound = sblimit") does for the joint loop.
    let bound = if h.mode == MODE_JOINT_STEREO {
        (4 * (h.mode_extension as usize + 1)).min(sblimit)
    } else {
        sblimit
    };
    let mut b = Bits {
        data: frame,
        pos: HEADER_BITS,
        end: end.min(frame.len() * 8),
    };
    // §2.4.2.3 protection_bit: "'0' if redundancy has been added" - the crc_check follows.
    let crc_word = if h.protection_bit == 0 {
        Some(b.read(CRC_BITS, "crc_check")? as u16)
    } else {
        None
    };
    let audio_start = b.pos;
    // §2.4.1.6: "for (sb=0; sb<bound; sb++) for (ch=0; ch<2; ch++) allocation[ch][sb]", then
    // "for (sb=bound; sb<sblimit; sb++) allocation[sb]" shared by both channels.
    let mut alloc = [[0u8; 32]; 2];
    for (sb, row) in table.iter().enumerate() {
        let per_sb = if sb < bound { nch } else { 1 };
        for a in alloc.iter_mut().take(per_sb) {
            a[sb] = b.read(row.nbal as usize, "allocation")? as u8;
        }
        if per_sb == 1 && nch == 2 {
            alloc[1][sb] = alloc[0][sb];
        }
    }
    // §2.4.1.6: "if (allocation[ch][sb]!=0) scfsi[ch][sb] 2 bits", for every channel.
    let mut scfsi = [[0u8; 32]; 2];
    for sb in 0..sblimit {
        for ch in 0..nch {
            if alloc[ch][sb] != 0 {
                scfsi[ch][sb] = b.read(2, "scfsi")? as u8;
            }
        }
    }
    if let Some(word) = crc_word {
        // §2.4.3.1: "16 bits of header(), starting with bit_rate_index and ending with emphasis"
        // and audio_data() bits per Table 3-B.5 - for Layer II the allocation and scfsi fields
        // (dist10 II_CRC_calc hashes exactly these).
        let mut crc = Crc16::new();
        crc.feed(frame, 16, HEADER_BITS);
        crc.feed(frame, audio_start, b.pos);
        if crc.0 != word {
            return Err(Fallback::FrameCrc);
        }
    }
    for sb in 0..sblimit {
        for ch in 0..nch {
            if alloc[ch][sb] != 0 {
                // §2.4.2.6 scfsi: "'00' three scalefactors transmitted", "'10' one scalefactor
                // transmitted", '01' and '11' "two scalefactors transmitted".
                let n = match scfsi[ch][sb] {
                    0b00 => 3,
                    0b10 => 1,
                    _ => 2,
                };
                b.skip(n * SCALEFACTOR_BITS, "scalefactor")?;
            }
        }
    }
    // §2.4.1.6: each granule carries "samplecode" (grouped) or 3 x "sample" per allocation,
    // once per channel below bound and once per shared subband above it.
    let mut granule_bits = 0usize;
    for (sb, row) in table.iter().enumerate() {
        let per_sb = if sb < bound { nch } else { 1 };
        for a in alloc.iter().take(per_sb) {
            if let Some(class) = a[sb].checked_sub(1).map(|i| row.classes[i as usize]) {
                let n = class.bits as usize;
                granule_bits += if class.grouped { n } else { 3 * n };
            }
        }
    }
    b.skip(GRANULES * granule_bits, "samples")?;
    Ok(Base {
        alloc,
        mc_start: b.pos,
    })
}

/// The 13818-3 §2.5.1.13 `mc_header()` fields that decide the channel configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct McHeader {
    pub ext_bit_stream_present: bool,
    pub centre: u8,
    pub surround: u8,
    pub lfe: bool,
}

// Per-configuration field lengths and channel count, 13818-3 §2.5.2.15 tables A-G.
struct Config {
    nmch: usize,
    tc_bits: usize,
    dyn_bits: usize,
    npred: &'static [u8],
}

// §2.5.2.15: "A) 3/2 configuration (nmch==3, length of tc_allocation field: 3 bits)", dyn_cross
// "length of field 'dyn_cross_mode': 4 bits", npred row "3/2 6 4 4 4 2 2 2 0 2 2 2 0 0 0 0".
fn config(mc: &McHeader) -> Config {
    let centre = mc.centre != 0b00;
    let cfg = |nmch, tc_bits, dyn_bits, npred| Config {
        nmch,
        tc_bits,
        dyn_bits,
        npred,
    };
    match (centre, mc.surround) {
        (true, 0b10) => cfg(3, 3, 4, &[6, 4, 4, 4, 2, 2, 2, 0, 2, 2, 2, 0, 0, 0, 0]), // A) 3/2
        (true, 0b01) => cfg(2, 3, 3, &[4, 2, 2, 0, 0]),                               // B) 3/1
        (true, 0b00) => cfg(1, 2, 1, &[2, 0]), // C) 3/0 "nmch==1 in 3/0 mode"
        (true, _) => cfg(3, 2, 1, &[2, 0]),    // C) "nmch==3 in 3/0+2/0 mode"
        (false, 0b10) => cfg(2, 2, 3, &[4, 2, 2, 0, 0]), // D) 2/2
        (false, 0b01) => cfg(1, 2, 1, &[2, 0]), // E) 2/1
        (false, 0b00) => cfg(0, 0, 0, &[]),    // F)/G) "nmch==0 in 2/0 mode"/"1/0 mode"
        (false, _) => cfg(2, 0, 0, &[]),       // F)/G) "nmch==2 in 2/0+2/0 mode"/"1/0+2/0"
    }
}

// §2.5.2.15 subband groups: 0..7 are subbands 0..7, "8 8...9", "9 10...11", "10 12...15",
// "11 16...31".
fn sbgr(sb: usize) -> usize {
    match sb {
        0..=7 => sb,
        8..=9 => 8,
        10..=11 => 9,
        12..=15 => 10,
        _ => 11,
    }
}

// Where a transmission channel's allocation comes from in one subband.
enum Source {
    Read,
    Copy(usize), // transmission channel index T0..T4
    Zero,
}

// Allocation source of T2+mch (13818-3 §2.5.2.15 dyn_cross tables; copy rules as in dist10
// II_decode_bitalloc_mc, which implements "if there is a term Tij ... copied from ... i" and
// "Lw and LSw ... from Lo, Rw and RSw ... from Ro, Cw and Sw ... per dyn_cross_LR").
fn allocation_source(
    mc: &McHeader,
    cfg: &Config,
    st: &Composite,
    mch: usize,
    sb: usize,
) -> Result<Source, Fallback> {
    let g = sbgr(sb);
    let t = mch + 2;
    // §2.5.2.13 centre "'11' centre bandwidth limited (Phantom coding)": the subbands above
    // 11 of the centre channel (T2) carry no allocation.
    if mc.centre == 0b11 && sb >= 12 && t == 2 {
        return Ok(Source::Zero);
    }
    let mode = if st.dyn_cross_on { st.dyn_mode[g] } else { 0 };
    let tc = st.tc[g];
    let lr = if st.dyn_cross_lr { 1 } else { 0 };
    if mode == 0 {
        // §2.5.2.15 dyn_second_stereo "'1' ... R2 (Transmission channel T3 in 2/0 + 2/0
        // configuration, T4 in 3/0 + 2/0 configuration) are copied from L2".
        if mc.surround == 0b11 && st.dyn_cross_on && st.second_stereo[g] {
            if mc.centre != 0 && t == 4 {
                return Ok(Source::Copy(3));
            }
            if mc.centre == 0 && t == 3 {
                return Ok(Source::Copy(2));
            }
        }
        return Ok(Source::Read);
    }
    Ok(match cfg.dyn_bits {
        // C) 3/0 (+2/0) and E) 2/1: "'1' −" - T2 is missing.
        1 => match t {
            2 => match tc {
                1 => Source::Copy(0),
                2 => Source::Copy(1),
                _ => Source::Copy(lr),
            },
            3 => Source::Read,
            _ if st.second_stereo[g] => Source::Copy(3),
            _ => Source::Read,
        },
        // B) 3/1 and D) 2/2: '001' T2 -, '010' - T3, '011' - -, '100' T23 -; '101'+ forbidden.
        3 => {
            if mode > 4 {
                return Err(Fallback::ForbiddenDynCross);
            }
            let two_two = mc.surround == 0b10;
            match t {
                2 if mode == 1 || mode == 4 => Source::Read,
                2 if two_two || tc == 1 || tc == 5 || (tc != 2 && lr == 0) => Source::Copy(0),
                2 => Source::Copy(1),
                _ if mode == 2 => Source::Read,
                _ if mode == 4 => Source::Copy(2),
                _ if two_two || tc == 4 || tc == 5 || (tc < 3 && lr == 1) => Source::Copy(1),
                _ => Source::Copy(0),
            }
        }
        // A) 3/2: '0001' T2 T3 - ... '1110' T234 - -; '1111' forbidden.
        _ => match (t, mode) {
            (_, 15) => return Err(Fallback::ForbiddenDynCross),
            (2, 1 | 2 | 4 | 8..=12 | 14) => Source::Read,
            (2, _) => match tc {
                1 | 7 => Source::Copy(0),
                2 | 6 => Source::Copy(1),
                _ => Source::Copy(lr),
            },
            (3, 1 | 3 | 5 | 8 | 10 | 13) => Source::Read,
            (3, 9 | 11 | 14) => Source::Copy(2),
            (3, _) => Source::Copy(0),
            (_, 2 | 3 | 6 | 9) => Source::Read,
            (_, 10 | 12 | 14) => Source::Copy(2),
            (_, 8 | 13) => Source::Copy(3),
            (_, _) => Source::Copy(1),
        },
    })
}

// §2.5.1.15 mc_composite_status_info() fields the allocation depends on.
struct Composite {
    dyn_cross_on: bool,
    dyn_cross_lr: bool,
    tc: [u8; 12],
    dyn_mode: [u8; 12],
    second_stereo: [bool; 12],
}

/// What one stored frame says about its channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Frame {
    /// No valid 11172-3 header: not a frame this module can judge.
    NoHeader,
    /// A clean base frame whose multichannel extension verified (§2.5.3.1).
    Multichannel { nch: u8, mc: McHeader },
    /// A clean base frame with no verified multichannel extension.
    Base { nch: u8, why: Fallback },
    /// The header parsed but the frame is damaged (base CRC) or cut short.
    Damaged { nch: u8, why: Fallback },
}

/// Judges one stored frame against 11172-3 and 13818-3.
pub(crate) fn inspect(frame: &[u8]) -> Frame {
    inspect_within(frame, frame.len() * 8)
}

// As `inspect`, reading no further than bit `end` (tests cut at exact field bits).
fn inspect_within(frame: &[u8], end: usize) -> Frame {
    let Some(h) = Header::parse(frame) else {
        return Frame::NoHeader;
    };
    let nch = h.nch();
    let base = match walk_base(frame, &h, end) {
        Ok(base) => base,
        Err(why @ (Fallback::FrameCrc | Fallback::Truncated(_))) => {
            return Frame::Damaged { nch, why };
        }
        Err(why) => return Frame::Base { nch, why },
    };
    // The frame's own length bounds mc_extension_data_part1 (11172-3 §2.4.3.1 slots).
    let frame_end = h
        .frame_bytes()
        .map_or(end, |n| (n * 8).min(end))
        .min(frame.len() * 8);
    match mc_extension(frame, &h, &base, frame_end) {
        Ok(mc) => Frame::Multichannel { nch, mc },
        Err(why) => Frame::Base { nch, why },
    }
}

// Reads 13818-3 §2.5.1.12.1 mc_extension() up to the end of its scfsi field and checks
// mc_crc_check over it (§2.5.2.14).
fn mc_extension(frame: &[u8], h: &Header, base: &Base, end: usize) -> Result<McHeader, Fallback> {
    let mut b = Bits {
        data: frame,
        pos: base.mc_start,
        end,
    };
    // §2.5.1.13: "ext_bit_stream_present 1", "if (ext_bit_stream_present == '1' || layer == 3)
    // n_ad_bytes 8", "centre 2", "surround 2", "lfe 1", then audio_mix 1, dematrix_procedure 2,
    // no_of_multi_lingual_ch 3, multi_lingual_fs 1, multi_lingual_layer 1, copyright 1+1.
    let ext = b.read(1, "ext_bit_stream_present")? == 1;
    let n_ad_bytes = if ext { b.read(8, "n_ad_bytes")? } else { 0 };
    let mc = McHeader {
        ext_bit_stream_present: ext,
        centre: b.read(2, "centre")? as u8,
        surround: b.read(2, "surround")? as u8,
        lfe: b.read(1, "lfe")? == 1,
    };
    b.skip(1 + 2 + 3 + 1 + 1 + 1 + 1, "mc_header")?;
    if ext {
        // §2.5.2.13 n_ad_bytes: "how many bytes are used for the MPEG-1 compatible ancillary
        // data field if an extension bit stream exists" - that field ends the base frame.
        b.end = b.end.saturating_sub(n_ad_bytes as usize * 8);
    }
    let header_end = b.pos;
    // §2.5.1.14: "mc_crc_check 16".
    let crc_word = b.read(CRC_BITS, "mc_crc_check")? as u16;
    let cfg = config(&mc);
    // §2.5.1.15: "tc_sbgr_select 1", "dyn_cross_on 1", "mc_prediction_on 1".
    let tc_sbgr_select = b.read(1, "tc_sbgr_select")? == 1;
    let dyn_cross_on = b.read(1, "dyn_cross_on")? == 1;
    let mc_prediction_on = b.read(1, "mc_prediction_on")? == 1;
    let mut st = Composite {
        dyn_cross_on,
        dyn_cross_lr: false,
        tc: [0; 12],
        dyn_mode: [0; 12],
        second_stereo: [false; 12],
    };
    // "if (tc_sbgr_select == '1') { tc_allocation 0..3 ... } else for (sbgr=0; sbgr<12; sbgr++)
    // tc_allocation[sbgr] 0..3".
    if tc_sbgr_select {
        st.tc = [b.read(cfg.tc_bits, "tc_allocation")? as u8; 12];
    } else {
        for t in st.tc.iter_mut() {
            *t = b.read(cfg.tc_bits, "tc_allocation")? as u8;
        }
    }
    if dyn_cross_on {
        // "dyn_cross_LR 1", "dyn_cross_mode[sbgr] 0..4", "if (surround=='11')
        // dyn_second_stereo[sbgr] 1".
        st.dyn_cross_lr = b.read(1, "dyn_cross_LR")? == 1;
        for g in 0..12 {
            st.dyn_mode[g] = b.read(cfg.dyn_bits, "dyn_cross_mode")? as u8;
            if mc.surround == 0b11 {
                st.second_stereo[g] = b.read(1, "dyn_second_stereo")? == 1;
            }
        }
    }
    if mc_prediction_on {
        // "for (sbgr=0; sbgr<8; sbgr++) { mc_prediction[sbgr] 1 ... for (px=0; px<npred; px++)
        // predsi[sbgr][px] 2", npred per §2.5.2.15's table.
        for g in 0..8 {
            if b.read(1, "mc_prediction")? == 1 {
                let npred = cfg.npred.get(st.dyn_mode[g] as usize).copied().unwrap_or(0);
                b.skip(2 * npred as usize, "predsi")?;
            }
        }
    }
    // §2.5.1.17: "if (lfe == '1') lfe_allocation 4".
    if mc.lfe {
        b.skip(4, "lfe_allocation")?;
    }
    let table = mc_allocation_table(h);
    let mut alloc = [[0u8; 32]; 5];
    alloc[..2].copy_from_slice(&base.alloc);
    // "for (sb=0; sb<msblimit; sb++) for (mch=0; mch<nmch; mch++) if (!centre_limited[mch][sb]
    // && !dyn_cross[mch][sb]) allocation[mch][sb] 2..4".
    for (sb, row) in table.iter().enumerate() {
        for mch in 0..cfg.nmch {
            alloc[mch + 2][sb] = match allocation_source(&mc, &cfg, &st, mch, sb)? {
                Source::Read => b.read(row.nbal as usize, "mc allocation")? as u8,
                Source::Copy(t) => alloc[t][sb],
                Source::Zero => 0,
            };
        }
    }
    // "if (allocation[mch][sb]!=0) scfsi[mch][sb] 2"; §2.5.2.15: a copied allocation of zero
    // means "the scalefactor select information and the scalefactors are not transmitted".
    for sb in 0..table.len() {
        for channel in &alloc[2..2 + cfg.nmch] {
            if channel[sb] != 0 {
                b.skip(2, "mc scfsi")?;
            }
        }
    }
    // §2.5.2.14: "the calculation begins with the first bit of the multichannel header and ends
    // with the last bit of the scfsi field, but excluding the mc_crc_check field itself."
    let mut crc = Crc16::new();
    crc.feed(frame, base.mc_start, header_end);
    crc.feed(frame, header_end + CRC_BITS, b.pos);
    if crc.0 != crc_word {
        return Err(Fallback::McCrc);
    }
    if mc.centre == 0b10 {
        return Err(Fallback::UndefinedCentre);
    }
    Ok(mc)
}

/// Channels of the main programme a verified multichannel frame stores: `nch` plus centre,
/// surround and LFE (13818-3 §2.5.2.13). A second stereo programme is not part of it.
pub(crate) fn main_programme_channels(nch: u8, mc: &McHeader) -> u8 {
    // centre "'01' centre channel present", "'11' centre bandwidth limited (Phantom coding)".
    let centre = u8::from(mc.centre == 0b01 || mc.centre == 0b11);
    let surround = match mc.surround {
        0b01 => 1, // "'01' mono surround"
        0b10 => 2, // "'10' stereo surround"
        _ => 0,    // "'00' no surround", "'11' no surround, but second stereo programme present"
    };
    nch + centre + surround + u8::from(mc.lfe)
}

/// Consecutive CRC-valid frames with one identical `mc_header` needed before a count above
/// `nch` is committed. 13818-3 2nd ed. §2.5.3.1: "If the mandatory CRC-check yields a valid
/// result, then multichannel decoding will be started." A 16-bit CRC over plain MPEG-1
/// ancillary data still matches 1 time in 2^16, so (freemkv policy) three matches in a row:
/// ~2^-48 per run start, ~2^-43 over the [`MAX_FRAMES`] look. A count equal to `nch`
/// over-claims nothing and commits on one frame.
pub(crate) const RUN_FRAMES: u32 = 3;

/// Settles a track's channel count from its stored frames: a verified count above `nch`
/// after [`RUN_FRAMES`] matching frames in a row, one equal to `nch` at once; failing that,
/// the base count once [`MAX_FRAMES`] frames (and any run under way) or the track end.
#[derive(Debug, Default)]
pub(crate) struct ChannelTracker {
    frames: u32,
    base: Option<u8>,
    settled: bool,
    extension: bool,
    last: Option<Fallback>,
    /// The verified `mc_header` of the current run and how many frames in a row carried it.
    run: Option<(McHeader, u32)>,
}

impl ChannelTracker {
    /// Feeds one stored frame; `Some(channels)` the one time a count is settled. After
    /// [`MAX_FRAMES`] frames it stops looking once no run is under way, settling on the base
    /// count if any frame had one.
    pub(crate) fn observe(&mut self, frame: &[u8]) -> Option<u8> {
        if self.settled {
            return None;
        }
        self.frames += 1;
        match inspect(frame) {
            Frame::Multichannel { nch, mc } => {
                self.base = Some(nch);
                // §2.5.2.13: "'1' extension bit stream present" - the rest is not in the track.
                let count = if mc.ext_bit_stream_present {
                    nch
                } else {
                    main_programme_channels(nch, &mc)
                };
                let len = match self.run {
                    Some((prev, n)) if prev == mc => n + 1,
                    _ => 1,
                };
                self.run = Some((mc, len));
                if count <= nch || len >= RUN_FRAMES {
                    self.settled = true;
                    self.extension = mc.ext_bit_stream_present;
                    return Some(count);
                }
            }
            // A clean frame without a verified mc_header (§2.5.3.1) breaks the run.
            Frame::Base { nch, why } => {
                self.base = Some(nch);
                self.last = Some(why);
                self.run = None;
            }
            // A failed frame CRC covers the header too, so its nch is not trusted; like a
            // header-less frame it breaks the run, which must be consecutive.
            Frame::Damaged { why, .. } => {
                self.last = Some(why);
                self.run = None;
            }
            Frame::NoHeader => self.run = None,
        }
        if self.frames >= MAX_FRAMES && self.run.is_none() {
            self.settled = true;
            return self.base;
        }
        None
    }

    /// Settles on the base count when the track ended before [`MAX_FRAMES`] frames.
    pub(crate) fn finish(&mut self) -> Option<u8> {
        if self.settled {
            return None;
        }
        self.settled = true;
        self.base
    }

    /// A verified `mc_header` said "'1' extension bit stream present" (§2.5.2.13).
    pub(crate) fn extension_signalled(&self) -> bool {
        self.extension
    }

    /// Frames examined and the last reason a frame gave no verified multichannel count.
    pub(crate) fn evidence(&self) -> (u32, Option<Fallback>) {
        (self.frames, self.last)
    }
}

#[cfg(test)]
#[path = "mp2_channels_tests.rs"]
pub(crate) mod tests;

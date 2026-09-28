//! Channel count a stored MPEG audio Layer II frame decodes to. Review against, and only change
//! with a citation from:
//! - ISO/IEC 11172-3 (CD text): §2.4.2.3 header fields, §2.4.1.6 Layer II `audio_data()`,
//!   §2.4.2.6 allocation/scfsi, §2.4.3.3 nbal and sblimit, 3-Annex B Tables 3-B.2a-d and 3-B.4.
//!   Annex B is not in the public text, so the table data is the ISO reference software's
//!   (dist10 `tables/alloc_0..3`, chosen as its `pick_table` does), matching FFmpeg's copy.
//! - ISO/IEC 13818-3:1994: §2.5.1.3 `frame()` (mc_extension_data_part1 follows
//!   mpeg1_audio_data), §2.5.1.8 `mc_header()`, §2.5.2.8 its semantics, and the symbol `nch`.
//!
//! Only base-layer frames (PES `0xC0|n`) are ever muxed, so an extension bit stream is never in
//! the track: with `ext_bit_stream_present` set, the track holds only the MPEG-1 channels.

/// 11172-3 §2.4.2.3: "The first 32 bits (four bytes) are header information".
const HEADER_BITS: usize = 32;
/// 11172-3 §2.4.2.4: "crc_check - a 16 bit parity-check word".
const CRC_BITS: usize = 16;
/// 11172-3 §2.4.1.6: "for (gr=0; gr<12; gr++)" - twelve granules of samples per frame.
const GRANULES: usize = 12;
/// 11172-3 §2.4.1.6: "scalefactor[ch][sb][0] 6 bits".
const SCALEFACTOR_BITS: usize = 6;

/// Why the multichannel count could not be read; the base-layer count is used instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fallback {
    /// ID '0' (13818-3 lower sampling frequencies) uses Table B.1, not handled here.
    LowSamplingFrequency,
    /// Layer I/III place mc data differently (13818-3 §2.5.1.2, §2.5.1.3).
    NotLayerII,
    /// Free format carries no bitrate, which the Annex B table choice needs.
    FreeFormat,
    /// Joint-stereo bound above sblimit reads allocations the table does not define.
    BoundAboveSblimit,
    /// The frame ends inside the named field.
    Truncated(&'static str),
    /// 13818-3 §2.5.2.8 centre: "'10' not defined".
    UndefinedCentre,
}

/// The four header fields the channel count depends on (11172-3 §2.4.2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    id: u8,
    layer: u8,
    protection_bit: u8,
    bitrate_kbps: Option<u32>,
    sampling_hz: u32,
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
        Some(Header {
            id: ((h >> 19) & 1) as u8,
            layer,
            protection_bit: ((h >> 16) & 1) as u8,
            bitrate_kbps,
            sampling_hz,
            mode: ((h >> 6) & 0b11) as u8,
            mode_extension: ((h >> 4) & 0b11) as u8,
        })
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

/// The 13818-3 §2.5.1.8 `mc_header()` fields that decide the channel configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct McHeader {
    pub ext_bit_stream_present: bool,
    pub centre: u8,
    pub surround: u8,
    pub lfe: bool,
}

/// Walks a Layer II frame's `audio_data()` (11172-3 §2.4.1.6) and reads the `mc_header()`
/// that 13818-3 §2.5.1.3 places right after it.
pub(crate) fn mc_header(frame: &[u8], h: &Header) -> Result<McHeader, Fallback> {
    mc_header_within(frame, h, frame.len() * 8)
}

// As `mc_header`, reading no further than bit `end` (tests truncate at exact field bits).
fn mc_header_within(frame: &[u8], h: &Header, end: usize) -> Result<McHeader, Fallback> {
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
    // §2.4.2.3 mode_extension: "'00' subbands 4-31 in intensity_stereo, bound==4" ... "==16".
    let bound = if h.mode == MODE_JOINT_STEREO {
        4 * (h.mode_extension as usize + 1)
    } else {
        sblimit
    };
    if bound > sblimit {
        return Err(Fallback::BoundAboveSblimit);
    }
    let end = end.min(frame.len() * 8);
    let mut b = Bits {
        data: frame,
        pos: HEADER_BITS,
        end,
    };
    if h.protection_bit == 0 {
        // §2.4.2.3 protection_bit: "'0' if redundancy has been added" - the crc_check.
        b.skip(CRC_BITS, "crc_check")?;
    }
    // §2.4.1.6: "for (sb=0; sb<bound; sb++) for (ch=0; ch<2; ch++) allocation[ch][sb]", then
    // "for (sb=bound; sb<sblimit; sb++) allocation[sb]" shared by both channels.
    let mut alloc = [[0u8; 30]; 2];
    for (sb, row) in table.iter().enumerate() {
        let per_sb = if sb < bound { nch } else { 1 };
        for a in alloc.iter_mut().take(per_sb) {
            a[sb] = b.read(row.nbal as usize, "allocation")? as u8;
        }
        if per_sb == 1 {
            alloc[1][sb] = alloc[0][sb];
        }
    }
    // §2.4.1.6: "if (allocation[ch][sb]!=0) scfsi[ch][sb] 2 bits", for every channel.
    let mut scfsi = [[0u8; 30]; 2];
    for sb in 0..sblimit {
        for ch in 0..nch {
            if alloc[ch][sb] != 0 {
                scfsi[ch][sb] = b.read(2, "scfsi")? as u8;
            }
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
    // 13818-3 §2.5.1.8: "ext_bit_stream_present 1", "if ext_bit_stream_present=='1'
    // n_ad_bytes 8", "centre 2", "surround 2", "lfe 1".
    let ext_bit_stream_present = b.read(1, "ext_bit_stream_present")? == 1;
    if ext_bit_stream_present {
        b.skip(8, "n_ad_bytes")?;
    }
    Ok(McHeader {
        ext_bit_stream_present,
        centre: b.read(2, "centre")? as u8,
        surround: b.read(2, "surround")? as u8,
        lfe: b.read(1, "lfe")? == 1,
    })
}

/// Total channels of a base frame whose multichannel data is all stored in it: the `nch`
/// MPEG-1 channels plus the centre, surround and LFE channels 13818-3 §2.5.2.8 declares.
pub(crate) fn mc_channels(nch: u8, mc: &McHeader) -> Result<u8, Fallback> {
    let centre = match mc.centre {
        0b00 => 0,                                  // "'00' no centre channel present"
        0b01 | 0b11 => 1, // "'01' centre channel present", "'11' ... (Phantom coding)"
        _ => return Err(Fallback::UndefinedCentre), // "'10' not defined"
    };
    let surround = match mc.surround {
        0b00 => 0, // "'00' no surround"
        0b01 => 1, // "'01' mono surround"
        _ => 2,    // "'10' stereo surround", "'11' ... second stereo programme present"
    };
    Ok(nch + centre + surround + u8::from(mc.lfe))
}

/// What the stored frame decodes to, and the reason when the base count had to be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stored {
    pub channels: u8,
    pub fallback: Option<Fallback>,
}

/// Channels of one stored Layer II frame. `multichannel_declared` (the source declares more
/// than the 2 an MPEG-1 base can carry) is the only case an `mc_header()` is looked for: the
/// reference decoder is likewise told, not left to guess, that a stream is multichannel.
/// `None` when the frame has no valid header at all.
pub(crate) fn stored_channels(frame: &[u8], multichannel_declared: bool) -> Option<Stored> {
    let h = Header::parse(frame)?;
    let base = h.nch();
    if !multichannel_declared {
        return Some(Stored {
            channels: base,
            fallback: None,
        });
    }
    let total = mc_header(frame, &h).and_then(|mc| {
        // §2.5.2.8: "'1' extension bit stream present" - the rest is in a stream not muxed.
        if mc.ext_bit_stream_present {
            Ok(base)
        } else {
            mc_channels(base, &mc)
        }
    });
    Some(match total {
        Ok(channels) => Stored {
            channels,
            fallback: None,
        },
        Err(e) => Stored {
            channels: base,
            fallback: Some(e),
        },
    })
}

#[cfg(test)]
#[path = "mp2_channels_tests.rs"]
pub(crate) mod tests;

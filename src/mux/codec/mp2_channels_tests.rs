use super::*;

// Spec quotes kept as reference text; the tests assert the behaviour directly.
/// 13818-3 (2nd ed.) §2.5.3.1, the rule the whole module follows.
#[allow(dead_code)]
const SPEC_DETECTION: &str = "The MPEG-1 ancillary data field is initially assumed to contain \
     the coded multichannel extension. If the mandatory CRC-check yields a valid result, then \
     multichannel decoding will be started.";
/// 13818-3 §2.5.1.3 (Layer II): the base frame's parts, in order.
#[allow(dead_code)]
const SPEC_BASE_FRAME: &str = "base_frame() { mpeg1_header() mpeg1_error_check() \
     mpeg1_audio_data() mc_extension_data_part1() mpeg1_ancillary_data() }";
/// 13818-3 §2.5.2.14: what `mc_crc_check` covers.
#[allow(dead_code)]
const SPEC_MC_CRC: &str = "In Layer I and II, the calculation begins with the first bit of the \
     multichannel header and ends with the last bit of the scfsi field, but excluding the \
     mc_crc_check field itself.";

// ── independent test writer ────────────────────────────────────────────────

// nbal per subband, from dist10 ("sb 0 0 nbal") independently of the parser.
const NBAL_AB: [u8; 30] = [
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2,
];
const NBAL_CD: [u8; 12] = [4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3];

// Bits per granule for base allocation 1 (dist10 "sb 1 3 5 1 0": one 5-bit code) or 2 ("0 2 7
// 3 3 2" = 3 x 3 bits in table a/b subbands 0-2, else "3 2 5 7 1 1" = one 7-bit code).
fn writer_granule_bits(table_ab: bool, sb: usize, alloc: u8) -> usize {
    match alloc {
        0 => 0,
        1 => 5,
        2 if table_ab && sb < 3 => 9,
        2 => 7,
        _ => unreachable!("writer only uses allocations 0..=2"),
    }
}

// 13818-3 §2.5.2.15 dyn_cross_mode rows, verbatim from the tables ('−' written '-').
const DYN_3_2: [&str; 15] = [
    "T2 T3 T4", "T2 T3 -", "T2 - T4", "- T3 T4", "T2 - -", "- T3 -", "- - T4", "- - -", "T2 T34 -",
    "T23 - T4", "T24 T3 -", "T23 - -", "T24 - -", "- T34 -", "T234 - -",
];
const DYN_3_1_2_2: [&str; 5] = ["T2 T3", "T2 -", "- T3", "- -", "T23 -"];
const DYN_3_0_2_1: [&str; 2] = ["T2", "-"];

/// One multichannel extension for the writer; field names follow §2.5.1.13/15.
#[derive(Clone, Copy)]
pub(crate) struct Mc {
    pub ext: bool,
    pub centre: u8,
    pub surround: u8,
    pub lfe: bool,
    pub n_ad_bytes: u8,
    pub tc: u8,
    pub tc_sbgr_select: bool,
    pub dyn_mode: Option<u8>,
    pub dyn_lr: bool,
    pub second_stereo: bool,
    pub prediction: bool,
    pub mc_alloc: u8,
}

/// 3/2 + LFE, everything in the base frame (ext '0').
pub(crate) const MC_3_2_LFE: Mc = Mc {
    ext: false,
    centre: 0b01,
    surround: 0b10,
    lfe: true,
    n_ad_bytes: 0,
    tc: 0,
    tc_sbgr_select: true,
    dyn_mode: None,
    dyn_lr: false,
    second_stereo: false,
    prediction: false,
    mc_alloc: 1,
};

/// A synthetic Layer II frame; `alloc` is written in every base subband and channel.
#[derive(Clone, Copy)]
pub(crate) struct Spec {
    pub id: u8,
    pub layer: u8,
    pub protection_bit: u8,
    pub bitrate_index: u8,
    pub fs: u8,
    pub mode: u8,
    pub mode_ext: u8,
    pub alloc: u8,
    pub scfsi: u8,
    pub mc: Option<Mc>,
    pub bad_frame_crc: bool,
    pub bad_mc_crc: bool,
}

/// 48 kHz, 256 kbit/s stereo, no CRC: table 3-B.2a (dist10 pick_table), sblimit 27.
pub(crate) const STEREO_256: Spec = Spec {
    id: 1,
    layer: 0b10,
    protection_bit: 1,
    bitrate_index: 0b1100,
    fs: 0b01,
    mode: 0b00,
    mode_ext: 0,
    alloc: 1,
    scfsi: 0,
    mc: None,
    bad_frame_crc: false,
    bad_mc_crc: false,
};

#[derive(Default)]
struct Writer {
    bits: Vec<u8>,
}

impl Writer {
    fn put(&mut self, v: u32, n: usize) {
        for i in (0..n).rev() {
            self.bits
                .push(v.checked_shr(i as u32).map_or(0, |b| (b & 1) as u8));
        }
    }
    fn set(&mut self, at: usize, v: u32, n: usize) {
        for i in 0..n {
            self.bits[at + i] = ((v >> (n - 1 - i)) & 1) as u8;
        }
    }
    // 11172-3 §2.4.3.1 CRC-16, G(X) = X16 + X15 + X2 + 1, register preset to all ones;
    // written here independently of the parser's.
    fn crc(&self, ranges: &[(usize, usize)]) -> u32 {
        let mut r: u32 = 0xFFFF;
        for &(a, b) in ranges {
            for &bit in &self.bits[a..b] {
                let fb = ((r >> 15) & 1) ^ u32::from(bit);
                r = (r << 1) & 0xFFFF;
                if fb == 1 {
                    r ^= 0x8005;
                }
            }
        }
        r
    }
    fn bytes(&self) -> Vec<u8> {
        self.bits
            .chunks(8)
            .map(|c| {
                c.iter()
                    .enumerate()
                    .fold(0u8, |a, (i, b)| a | (b << (7 - i)))
            })
            .collect()
    }
}

// (nmch, tc bits, dyn bits, dyn rows) per 13818-3 §2.5.2.15 A)-G), restated for the writer.
fn writer_config(mc: &Mc) -> (usize, usize, usize, &'static [&'static str]) {
    match (mc.centre != 0, mc.surround) {
        (true, 0b10) => (3, 3, 4, &DYN_3_2),
        (true, 0b01) => (2, 3, 3, &DYN_3_1_2_2),
        (true, 0b00) => (1, 2, 1, &DYN_3_0_2_1),
        (true, _) => (3, 2, 1, &DYN_3_0_2_1),
        (false, 0b10) => (2, 2, 3, &DYN_3_1_2_2),
        (false, 0b01) => (1, 2, 1, &DYN_3_0_2_1),
        (false, 0b00) => (0, 0, 0, &[]),
        (false, _) => (2, 0, 0, &[]),
    }
}

// npred per §2.5.2.15 (2nd ed.): "3/2 6 4 4 4 2 2 2 0 2 2 2 0 0 0 0", "3/1 4 2 2 0 0",
// "3/0 2 0", "2/2 4 2 2 0 0", "2/1 2 0".
fn writer_npred(mc: &Mc, mode: usize) -> usize {
    let row: &[usize] = match (mc.centre != 0, mc.surround) {
        (true, 0b10) => &[6, 4, 4, 4, 2, 2, 2, 0, 2, 2, 2, 0, 0, 0, 0],
        (true, 0b01) | (false, 0b10) => &[4, 2, 2, 0, 0],
        (true, _) | (false, 0b01) => &[2, 0],
        _ => &[],
    };
    row.get(mode).copied().unwrap_or(0)
}

// The value transmission channel `t` (2..=4) holds in subband `sb`, and whether it is sent:
// '-' = not sent; a "Tij" term = copied from Ti; otherwise from Lo/Ro (the base, `base`).
fn writer_channel(mc: &Mc, rows: &[&str], t: usize, sb: usize, base: u8) -> (bool, u8) {
    if mc.centre == 0b11 && sb >= 12 && t == 2 {
        return (false, 0); // §2.5.2.13 centre '11': "subbands above subband 11 are not transmitted"
    }
    let r2 = if mc.centre != 0 { 4 } else { 3 };
    let dyn_rows_apply = mc.dyn_mode.is_some() && !rows.is_empty();
    if mc.surround == 0b11 && mc.dyn_mode.is_some() && mc.second_stereo && t == r2 {
        return (false, mc.mc_alloc); // dyn_second_stereo: R2 "copied from L2"
    }
    let mode = mc.dyn_mode.unwrap_or(0) as usize;
    if !dyn_rows_apply || t - 2 >= rows[mode].split(' ').count() {
        return (true, mc.mc_alloc);
    }
    let cells: Vec<&str> = rows[mode].split(' ').collect();
    if cells[t - 2] != "-" {
        return (true, mc.mc_alloc);
    }
    let digit = char::from(b'0' + t as u8);
    let copied = cells.iter().any(|c| c.len() > 2 && c[2..].contains(digit));
    (false, if copied { mc.mc_alloc } else { base })
}

/// Writes `s` per 11172-3 §2.4.1.6 and 13818-3 §2.5.1.3/13/14/15/17; returns the frame and the
/// bit offset of each field (for truncation tests). The frame is padded to its §2.4.3.1 size.
pub(crate) fn write(s: Spec) -> (Vec<u8>, Vec<(&'static str, usize)>) {
    let mut w = Writer::default();
    let mut at = Vec::new();
    w.put(0xFFF, 12);
    w.put(u32::from(s.id), 1);
    w.put(u32::from(s.layer), 2);
    w.put(u32::from(s.protection_bit), 1);
    w.put(u32::from(s.bitrate_index), 4);
    w.put(u32::from(s.fs), 2);
    w.put(0, 2); // padding_bit, private_bit
    w.put(u32::from(s.mode), 2);
    w.put(u32::from(s.mode_ext), 2);
    w.put(0, 4); // copyright, original/home, emphasis
    if s.fs == 0b11 || s.bitrate_index == 15 || s.bitrate_index == 0 || s.layer != 0b10 {
        return (w.bytes(), at); // no Layer II body is defined for these
    }
    let crc_at = w.bits.len();
    if s.protection_bit == 0 {
        at.push(("crc_check", crc_at));
        w.put(0, 16);
    }
    let kbps = LAYER_II_KBPS[s.bitrate_index as usize];
    let nch = if s.mode == 0b11 { 1 } else { 2 };
    let hz = [44_100, 48_000, 32_000][s.fs as usize];
    let per_ch = kbps / nch as u32;
    let (table_ab, sblimit) = if (hz == 48_000 && per_ch >= 56) || (56..=80).contains(&per_ch) {
        (true, 27)
    } else if hz != 48_000 && per_ch >= 96 {
        (true, 30)
    } else if hz != 32_000 && per_ch <= 48 {
        (false, 8)
    } else {
        (false, 12)
    };
    let nbal = |sb: usize| if table_ab { NBAL_AB[sb] } else { NBAL_CD[sb] };
    let bound = if s.mode == 0b01 {
        (4 * (s.mode_ext as usize + 1)).min(sblimit)
    } else {
        sblimit
    };
    let audio_start = w.bits.len();
    at.push(("allocation", audio_start));
    for sb in 0..sblimit {
        for _ in 0..if sb < bound { nch } else { 1 } {
            w.put(u32::from(s.alloc), nbal(sb) as usize);
        }
    }
    if s.alloc != 0 {
        at.push(("scfsi", w.bits.len()));
        for _ in 0..sblimit * nch {
            w.put(u32::from(s.scfsi), 2);
        }
    }
    if s.protection_bit == 0 {
        let crc = w.crc(&[(16, 32), (audio_start, w.bits.len())]);
        w.set(crc_at, crc ^ u32::from(s.bad_frame_crc), 16);
    }
    if s.alloc != 0 {
        at.push(("scalefactor", w.bits.len()));
        let per = match s.scfsi {
            0 => 3,
            2 => 1,
            _ => 2,
        };
        w.put(0, sblimit * nch * per * 6);
    }
    let granule: usize = (0..sblimit)
        .map(|sb| (if sb < bound { nch } else { 1 }) * writer_granule_bits(table_ab, sb, s.alloc))
        .sum();
    if granule > 0 {
        at.push(("samples", w.bits.len()));
        w.put(0, 12 * granule);
    }
    if let Some(mc) = s.mc {
        let mc_start = w.bits.len();
        at.push(("ext_bit_stream_present", mc_start));
        w.put(u32::from(mc.ext), 1);
        if mc.ext {
            at.push(("n_ad_bytes", w.bits.len()));
            w.put(u32::from(mc.n_ad_bytes), 8);
        }
        at.push(("centre", w.bits.len()));
        w.put(u32::from(mc.centre), 2);
        at.push(("surround", w.bits.len()));
        w.put(u32::from(mc.surround), 2);
        at.push(("lfe", w.bits.len()));
        w.put(u32::from(mc.lfe), 1);
        at.push(("mc_header", w.bits.len()));
        w.put(0, 10); // audio_mix .. copyright_identification_start
        let mc_crc_at = w.bits.len();
        at.push(("mc_crc_check", mc_crc_at));
        w.put(0, 16);
        let (nmch, tc_bits, dyn_bits, rows) = writer_config(&mc);
        at.push(("tc_sbgr_select", w.bits.len()));
        w.put(u32::from(mc.tc_sbgr_select), 1);
        w.put(u32::from(mc.dyn_mode.is_some()), 1);
        w.put(u32::from(mc.prediction), 1);
        if tc_bits > 0 {
            at.push(("tc_allocation", w.bits.len()));
        }
        for _ in 0..if mc.tc_sbgr_select { 1 } else { 12 } {
            w.put(u32::from(mc.tc), tc_bits);
        }
        if let Some(mode) = mc.dyn_mode {
            at.push(("dyn_cross_LR", w.bits.len()));
            w.put(u32::from(mc.dyn_lr), 1);
            for _ in 0..12 {
                w.put(u32::from(mode), dyn_bits);
                if mc.surround == 0b11 {
                    w.put(u32::from(mc.second_stereo), 1);
                }
            }
        }
        if mc.prediction {
            at.push(("mc_prediction", w.bits.len()));
            for _ in 0..8 {
                w.put(1, 1);
                w.put(0, 2 * writer_npred(&mc, mc.dyn_mode.unwrap_or(0) as usize));
            }
        }
        if mc.lfe {
            at.push(("lfe_allocation", w.bits.len()));
            w.put(0, 4);
        }
        let msblimit = if hz == 48_000 { 27 } else { 30 };
        let mut scfsi = 0;
        let alloc_at = w.bits.len();
        for (sb, &nbal) in NBAL_AB.iter().enumerate().take(msblimit) {
            for t in 2..2 + nmch {
                let base = if sb < sblimit { s.alloc } else { 0 };
                let (sent, value) = writer_channel(&mc, rows, t, sb, base);
                if sent {
                    w.put(u32::from(value), nbal as usize);
                }
                scfsi += usize::from(value != 0);
            }
        }
        if w.bits.len() > alloc_at {
            at.push(("mc allocation", alloc_at));
        }
        if scfsi > 0 {
            at.push(("mc scfsi", w.bits.len()));
            w.put(0, 2 * scfsi);
        }
        let crc = w.crc(&[(mc_start, mc_crc_at), (mc_crc_at + 16, w.bits.len())]);
        w.set(mc_crc_at, crc ^ u32::from(s.bad_mc_crc), 16);
    }
    let frame_bits = 8 * (144_000 * kbps as usize / hz);
    assert!(w.bits.len() <= frame_bits, "content overflows the frame");
    w.bits.resize(frame_bits, 0);
    (w.bytes(), at)
}

// Parses the header of `s` written with no audio data (header-only tests).
fn head(s: Spec) -> Option<Header> {
    Header::parse(&frame(Spec { alloc: 0, ..s }))
}

fn frame(s: Spec) -> Vec<u8> {
    write(s).0
}

fn with_mc(s: Spec, mc: Mc) -> Spec {
    Spec { mc: Some(mc), ..s }
}

fn mc(centre: u8, surround: u8, lfe: bool) -> Mc {
    Mc {
        centre,
        surround,
        lfe,
        ..MC_3_2_LFE
    }
}

fn multichannel(f: &[u8]) -> Option<McHeader> {
    match inspect(f) {
        ChannelFrame::Multichannel { mc, .. } => Some(mc),
        _ => None,
    }
}

// A full run of one frame: RUN_FRAMES copies (a count above nch needs that many in a row).
fn run(f: Vec<u8>) -> Vec<Vec<u8>> {
    vec![f; RUN_FRAMES as usize]
}

fn settle(frames: &[Vec<u8>]) -> (Option<u8>, bool) {
    let mut t = ChannelTracker::default();
    for f in frames {
        if let Some(n) = t.observe(f) {
            return (Some(n), t.extension_signalled());
        }
    }
    (t.finish(), t.extension_signalled())
}

// ── header (11172-3 §2.4.2.3) ──────────────────────────────────────────────

#[test]
fn nch_follows_mode_for_all_four_modes() {
    for (mode, want) in [(0b00, 2), (0b01, 2), (0b10, 2), (0b11, 1)] {
        let h = head(Spec {
            mode,
            bitrate_index: 0b1010,
            ..STEREO_256
        })
        .unwrap();
        // 13818-3 symbols: "nch ... equal to 1 for single_channel mode, 2 in other modes."
        assert_eq!(h.nch(), want, "mode {mode:02b}");
    }
}

#[test]
fn header_rejects_what_2_4_2_3_does_not_define() {
    let good = frame(STEREO_256);
    let mut no_sync = good.clone();
    no_sync[1] &= 0x0F;
    // §2.4.2.3: "syncword - the bit string '1111 1111 1111'."
    assert_eq!(Header::parse(&no_sync), None);
    // §2.4.2.3 Layer: "\"00\" reserved".
    assert_eq!(
        head(Spec {
            layer: 0,
            ..STEREO_256
        }),
        None
    );
    // §2.4.2.3 sampling_frequency: "'11' reserved".
    assert_eq!(
        head(Spec {
            fs: 0b11,
            ..STEREO_256
        }),
        None
    );
    // §2.4.2.3 bitrate table lists '0000'..'1110' only.
    assert_eq!(
        head(Spec {
            bitrate_index: 15,
            ..STEREO_256
        }),
        None
    );
    // §2.4.2.3: "The first 32 bits (four bytes) are header information".
    assert_eq!(Header::parse(&good[..3]), None);
}

/// 11172-3 §2.4.2.3 (1993 text): "For Layer II, not all combinations of total bitrate and mode
/// are allowed." Per spec; do not change without a spec citation proving otherwise.
#[test]
fn layer_ii_bitrate_mode_combinations_follow_table_3_b_2() {
    // The table verbatim: (kbit/s, allowed for single_channel, allowed for the other modes).
    const MONO: (bool, bool) = (true, false); // "single_channel"
    const ALL: (bool, bool) = (true, true); // "all modes"
    const NOT_MONO: (bool, bool) = (false, true); // "stereo, intensity stereo, dual channel"
    #[rustfmt::skip]
    let table = [
        (32, MONO), (48, MONO), (56, MONO), (64, ALL), (80, MONO), (96, ALL), (112, ALL),
        (128, ALL), (160, ALL), (192, ALL), (224, NOT_MONO), (256, NOT_MONO), (320, NOT_MONO),
        (384, NOT_MONO),
    ];
    for (i, (kbps, (mono_ok, other_ok))) in (1..=14u8).zip(table) {
        assert_eq!(LAYER_II_KBPS[i as usize], kbps);
        let ok = |mode: u8| {
            head(Spec {
                bitrate_index: i,
                mode,
                ..STEREO_256
            })
            .is_some()
        };
        // mode '00' stereo, '01' joint_stereo, '10' dual_channel, '11' single_channel.
        let got = (ok(0b11), [ok(0b00), ok(0b01), ok(0b10)]);
        assert_eq!(got, (mono_ok, [other_ok; 3]), "{kbps} kbit/s");
    }
    // "free format" is a bitrate "which does not need to be in the list": never rejected.
    assert!(
        head(Spec {
            bitrate_index: 0,
            mode: 0b11,
            ..STEREO_256
        })
        .is_some()
    );
    // The restriction is Layer II's; Layer I at "'1110' 448 kbit/s" mono is untouched.
    assert!(
        head(Spec {
            layer: 0b11,
            bitrate_index: 14,
            mode: 0b11,
            ..STEREO_256
        })
        .is_some()
    );
}

#[test]
fn header_reads_sampling_frequency_bitrate_and_frame_size() {
    for (fs, hz) in [(0b00, 44_100), (0b01, 48_000), (0b10, 32_000)] {
        let h = head(Spec { fs, ..STEREO_256 }).unwrap();
        // §2.4.2.3: "'00' 44.1 kHz", "'01' 48 kHz", "'10' 32 kHz".
        assert_eq!(h.sampling_hz, hz);
    }
    for (i, kbps) in [(4, 64), (12, 256), (14, 384)] {
        let h = head(Spec {
            bitrate_index: i,
            ..STEREO_256
        })
        .unwrap();
        // §2.4.2.3 bitrate table, "Layer II" column.
        assert_eq!(h.bitrate_kbps, Some(kbps));
        // Layer II frame = 144 * bitrate / sampling_frequency bytes.
        assert_eq!(h.frame_bytes(), Some(144 * kbps as usize * 1000 / 48_000));
    }
    let free = head(Spec {
        bitrate_index: 0,
        ..STEREO_256
    })
    .unwrap();
    // §2.4.2.3: "The all zero value indicates the 'free format' condition".
    assert_eq!((free.bitrate_kbps, free.frame_bytes()), (None, None));
}

// ── Annex B tables ─────────────────────────────────────────────────────────

/// Row checks against dist10 lines "sb index steps bits group quant", quoted verbatim.
#[test]
fn table_rows_match_the_iso_reference_tables() {
    let ab = |sb: usize, i: usize| TABLE_AB[sb].classes[i - 1];
    let cd = |sb: usize, i: usize| TABLE_CD[sb].classes[i - 1];
    let rows: [(&str, Class, u8, bool, u8); 10] = [
        (
            "alloc_0: 0 15 65535 16 3 16",
            ab(0, 15),
            16,
            false,
            TABLE_AB[0].nbal,
        ),
        ("alloc_0: 0 2 7 3 3 2", ab(0, 2), 3, false, TABLE_AB[0].nbal),
        ("alloc_0: 3 3 7 3 3 2", ab(3, 3), 3, false, TABLE_AB[3].nbal),
        (
            "alloc_0: 3 4 9 10 1 3",
            ab(3, 4),
            10,
            true,
            TABLE_AB[3].nbal,
        ),
        (
            "alloc_0: 3 15 65535 16 3 16",
            ab(3, 15),
            16,
            false,
            TABLE_AB[3].nbal,
        ),
        (
            "alloc_0: 11 7 65535 16 3 16",
            ab(11, 7),
            16,
            false,
            TABLE_AB[11].nbal,
        ),
        (
            "alloc_0: 23 3 65535 16 3 16",
            ab(23, 3),
            16,
            false,
            TABLE_AB[23].nbal,
        ),
        (
            "alloc_2: 0 3 9 10 1 3",
            cd(0, 3),
            10,
            true,
            TABLE_CD[0].nbal,
        ),
        (
            "alloc_2: 0 15 32767 15 3 15",
            cd(0, 15),
            15,
            false,
            TABLE_CD[0].nbal,
        ),
        (
            "alloc_2: 2 7 127 7 3 7",
            cd(2, 7),
            7,
            false,
            TABLE_CD[2].nbal,
        ),
    ];
    for (quote, class, bits, grouped, nbal) in rows {
        assert_eq!((class.bits, class.grouped), (bits, grouped), "{quote}");
        // 11172-3 §2.4.3.3: "reading 'nbal' (2,3, or 4) bits"; 2^nbal-1 classes per row.
        assert!((2..=4).contains(&nbal), "{quote}");
    }
    for (sb, row) in TABLE_AB.iter().chain(TABLE_CD.iter()).enumerate() {
        assert_eq!(row.classes.len(), (1 << row.nbal) - 1, "row {sb}");
    }
}

/// Every entry of the four ISO reference tables (dist10 decoder `tables/alloc_0..3`, commit
/// ea3c5ee of joncampbell123/iso-dist10), row format "sb index steps bits group quant":
/// index 0 carries nbal in `bits`; `group` 1 is one grouped code, 3 is three codes.
#[test]
fn tables_equal_the_iso_reference_tables_entry_for_entry() {
    let files = [
        (include_str!("testdata/dist10/alloc_0"), &TABLE_AB[..], 27),
        (include_str!("testdata/dist10/alloc_1"), &TABLE_AB[..], 30),
        (include_str!("testdata/dist10/alloc_2"), &TABLE_CD[..], 8),
        (include_str!("testdata/dist10/alloc_3"), &TABLE_CD[..], 12),
    ];
    let mut entries = 0;
    for (text, rows, sblimit) in files {
        let mut lines = text.lines();
        // 11172-3 §2.4.3.3: "The number of the lowest subband that will not have bits
        // allocated to it is assigned to the identifier 'sblimit'."
        assert_eq!(
            lines.next().unwrap().trim().parse::<usize>().unwrap(),
            sblimit
        );
        for line in lines.filter(|l| !l.trim().is_empty()) {
            let v: Vec<u32> = line
                .split_whitespace()
                .map(|x| x.parse().unwrap())
                .collect();
            let (sb, idx, bits, group) = (v[0] as usize, v[1] as usize, v[3] as u8, v[4]);
            if idx == 0 {
                assert_eq!(rows[sb].nbal, bits, "nbal: {line}");
            } else {
                let c = rows[sb].classes[idx - 1];
                assert_eq!((c.bits, c.grouped), (bits, group == 1), "{line}");
            }
            entries += 1;
        }
    }
    assert_eq!(entries, 288 + 300 + 80 + 112);
}

/// dist10 pick_table picks 3-B.2a/b/c/d from the per-channel bitrate and sampling rate.
#[test]
fn allocation_table_selection_follows_the_reference_rule() {
    // (fs code, bitrate index, mode, expected sblimit): "sblimit" of 3-B.2a/b/c/d = 27/30/8/12.
    let cases = [
        (0b01, 0b1100, 0b00, 27), // 48 kHz, 128 kbit/s per channel
        (0b01, 0b0011, 0b11, 27), // 48 kHz mono 56: "sfrq == 48 && br_per_ch >= 56"
        (0b00, 0b0101, 0b11, 27), // 44.1 kHz mono 80: "br_per_ch >= 56 && br_per_ch <= 80"
        (0b00, 0b1100, 0b00, 30), // 44.1 kHz 128/ch: "sfrq != 48 && br_per_ch >= 96"
        (0b10, 0b0110, 0b11, 30), // 32 kHz mono 96
        (0b01, 0b0100, 0b00, 8),  // 48 kHz 32/ch: "sfrq != 32 && br_per_ch <= 48"
        (0b00, 0b0010, 0b11, 8),  // 44.1 kHz mono 48
        (0b10, 0b0100, 0b00, 12), // 32 kHz 32/ch: "else table = 3"
        (0b10, 0b0010, 0b11, 12), // 32 kHz mono 48
    ];
    for (fs, bitrate_index, mode, sblimit) in cases {
        let h = head(Spec {
            fs,
            bitrate_index,
            mode,
            ..STEREO_256
        })
        .unwrap();
        assert_eq!(allocation_table(&h, h.bitrate_kbps.unwrap()).len(), sblimit);
    }
    let h48 = head(STEREO_256).unwrap();
    let h441 = head(Spec {
        fs: 0b00,
        ..STEREO_256
    })
    .unwrap();
    // 13818-3 §2.5.2.17: "Table B.2.a ... if Fs equals 48 kHz, table B.2.b ... 44,1 kHz or
    // 32 kHz, regardless of the bitrate."
    assert_eq!(
        (
            mc_allocation_table(&h48).len(),
            mc_allocation_table(&h441).len()
        ),
        (27, 30)
    );
}

// ── real encoded frames ────────────────────────────────────────────────────

// Splits a raw Layer II file into frames by each header's §2.4.3.1 size.
fn split(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(n) = data
        .get(i..)
        .and_then(Header::parse)
        .and_then(|h| h.frame_bytes())
    {
        out.push(&data[i..(i + n).min(data.len())]);
        i += n;
    }
    out
}

/// FFmpeg 9.0.2 `-c:a mp2` output (plain MPEG-1, one per 3-B.2 table): the walk must end
/// where the encoder's own allocation filled the frame, not generate-then-parse its own layout.
#[test]
fn ffmpeg_encoded_frames_walk_to_the_end_of_every_frame() {
    let fixtures: [(&[u8], u8); 7] = [
        (include_bytes!("testdata/ffmpeg_mp2/st_48_256_a.mp2"), 2),
        (include_bytes!("testdata/ffmpeg_mp2/st_48_384_a.mp2"), 2),
        (include_bytes!("testdata/ffmpeg_mp2/mo_48_96_a.mp2"), 1),
        (include_bytes!("testdata/ffmpeg_mp2/st_441_192_b.mp2"), 2),
        (include_bytes!("testdata/ffmpeg_mp2/st_48_64_c.mp2"), 2),
        (include_bytes!("testdata/ffmpeg_mp2/st_32_64_d.mp2"), 2),
        (include_bytes!("testdata/ffmpeg_mp2/mo_32_32_d.mp2"), 1),
    ];
    let mut n = 0;
    for (data, nch) in fixtures {
        for f in split(data) {
            let h = Header::parse(f).unwrap();
            let base = walk_base(f, &h, f.len() * 8).expect("a real frame walks cleanly");
            // §2.4.1.6 consumed exactly the encoder's allocation: under a byte of stuffing
            // bits plus a byte of slack left, never past the frame.
            assert!(
                f.len() * 8 - base.mc_start <= 16,
                "{} spare bits",
                f.len() * 8 - base.mc_start
            );
            // §2.5.3.1: no valid mc_crc_check in MPEG-1 frames, so no multichannel.
            assert!(matches!(inspect(f), ChannelFrame::Base { nch: c, .. } if c == nch));
            n += 1;
        }
    }
    assert!(n >= 30, "{n}");
    let frames: Vec<Vec<u8>> = split(include_bytes!("testdata/ffmpeg_mp2/mo_48_96_a.mp2"))
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect();
    // A plain mono track settles on "nch ... equal to 1 for single_channel mode".
    assert_eq!(settle(&frames), (Some(1), false));
}

// ── base frame CRC (11172-3 §2.4.3.1) ──────────────────────────────────────

#[test]
fn frame_crc_is_checked_when_protection_bit_is_zero() {
    let crc = Spec {
        protection_bit: 0,
        ..STEREO_256
    };
    // §2.4.3.1: header bits "starting with bit_rate_index and ending with emphasis" plus the
    // Table 3-B.5 audio_data bits (allocation and scfsi for Layer II).
    assert!(matches!(
        inspect(&frame(crc)),
        ChannelFrame::Base { nch: 2, .. }
    ));
    let bad = Spec {
        bad_frame_crc: true,
        ..crc
    };
    // "If the words are not identical, a transmission error has occured in the protected field".
    assert_eq!(
        inspect(&frame(bad)),
        ChannelFrame::Damaged {
            nch: 2,
            why: Fallback::FrameCrc
        }
    );
    let mut flipped = frame(crc);
    flipped[3] ^= 0x01; // emphasis bit: inside the protected header bits
    assert_eq!(
        inspect(&flipped),
        ChannelFrame::Damaged {
            nch: 2,
            why: Fallback::FrameCrc
        }
    );
}

// ── multichannel detection (13818-3 §2.5.3.1) ──────────────────────────────

/// A frame whose mc_crc_check verifies is multichannel; one that does not, is not.
#[test]
fn multichannel_needs_a_valid_mc_crc_check() {
    let good = with_mc(STEREO_256, MC_3_2_LFE);
    // §2.5.3.1: "If the mandatory CRC-check yields a valid result, then multichannel decoding
    // will be started."
    assert!(multichannel(&frame(good)).is_some());
    let bad = Spec {
        bad_mc_crc: true,
        ..good
    };
    assert_eq!(
        inspect(&frame(bad)),
        ChannelFrame::Base {
            nch: 2,
            why: Fallback::McCrc
        }
    );
}

/// Coding mode 2 is "MPEG-1 or MPEG-2 without extension bit stream" (EP0867877A2): an IFO
/// declaring 6 channels on a plain MPEG-1 frame must not turn ancillary bytes into channels.
#[test]
fn mpeg1_ancillary_data_is_never_read_as_an_mc_header() {
    let mut f = frame(STEREO_256);
    let start = f.len() - 40;
    for (i, b) in f[start..].iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(37) ^ 0xA5; // arbitrary ancillary bytes
    }
    // 11172-3 §2.4.2.8: "Ancillary_bit - user definable"; §2.5.3.1 needs a valid CRC first.
    assert!(multichannel(&f).is_none());
    let lookalike = Spec {
        bad_mc_crc: true,
        ..with_mc(STEREO_256, MC_3_2_LFE)
    };
    // Even a well-formed 3/2 + LFE header without its CRC stays two channels.
    assert_eq!(settle(&[frame(lookalike)]), (Some(2), false));
}

/// Every centre x surround x lfe value, all stored in the base frame. This is per spec; do not
/// change without a spec citation proving otherwise.
#[test]
fn every_mc_configuration_counts_its_main_programme() {
    // (centre, surround, name, main-programme channels before LFE), §2.5.2.13 semantics.
    let configs = [
        (0b00, 0b00, "2/0", 2),
        (0b01, 0b00, "3/0", 3),
        (0b11, 0b00, "3/0 phantom centre", 3),
        (0b00, 0b01, "2/1", 3),
        (0b01, 0b01, "3/1", 4),
        (0b11, 0b01, "3/1 phantom centre", 4),
        (0b00, 0b10, "2/2", 4),
        (0b01, 0b10, "3/2", 5),
        (0b11, 0b10, "3/2 phantom centre", 5),
        (0b00, 0b11, "2/0 + 2/0", 2),
        (0b01, 0b11, "3/0 + 2/0", 3),
        (0b11, 0b11, "3/0 + 2/0 phantom centre", 3),
    ];
    for (centre, surround, name, main) in configs {
        for lfe in [false, true] {
            let f = frame(with_mc(STEREO_256, mc(centre, surround, lfe)));
            // §2.5.2.13: "'0' no extension stream present" - all of it is in this track.
            assert_eq!(
                settle(&run(f)),
                (Some(main + u8::from(lfe)), false),
                "{name} lfe {lfe}"
            );
        }
    }
}

/// §2.5.2.13 surround "'11' no surround, but second stereo programme present": the second
/// programme is not surround. This is per spec; do not change without a spec citation
/// proving otherwise.
#[test]
fn second_stereo_programme_is_not_counted_as_surround() {
    let f = frame(with_mc(STEREO_256, mc(0b00, 0b11, false)));
    assert_eq!(multichannel(&f).map(|m| m.surround), Some(0b11));
    assert_eq!(settle(&[f]), (Some(2), false));
}

/// ext '0' 3/2 + LFE is six channels, not the two most decoders play. This is per spec; do
/// not change without a spec citation proving otherwise.
#[test]
fn ext_zero_3_2_lfe_is_six_not_two() {
    // §2.5.2.13: "'0' no extension stream present"; centre '01', surround '10', lfe '1'.
    assert_eq!(
        settle(&run(frame(with_mc(STEREO_256, MC_3_2_LFE)))),
        (Some(6), false)
    );
}

#[test]
fn centre_10_is_not_defined_and_falls_back() {
    let f = frame(with_mc(STEREO_256, mc(0b10, 0b10, true)));
    // §2.5.2.13 centre: "'10' not defined".
    assert_eq!(
        inspect(&f),
        ChannelFrame::Base {
            nch: 2,
            why: Fallback::UndefinedCentre
        }
    );
}

/// With an extension bit stream the track holds only the base; count per mode.
#[test]
fn ext_one_is_the_base_count_in_every_mode() {
    for (mode, mode_ext, nch) in [
        (0b00, 0, 2),
        (0b01, 0, 2),
        (0b01, 3, 2),
        (0b10, 0, 2),
        (0b11, 0, 1),
    ] {
        let bitrate_index = if mode == 0b11 { 0b1010 } else { 0b1100 }; // 192 mono, 256 stereo
        let s = Spec {
            mode,
            mode_ext,
            bitrate_index,
            ..STEREO_256
        };
        let f = frame(with_mc(
            s,
            Mc {
                ext: true,
                ..MC_3_2_LFE
            },
        ));
        // §2.5.2.13: "'1' extension bit stream present" - not in the muxed 0xC0 track.
        assert_eq!(
            settle(&[f]),
            (Some(nch), true),
            "mode {mode:02b}/{mode_ext}"
        );
    }
}

/// §2.5.2.13 n_ad_bytes: "how many bytes are used for the MPEG-1 compatible ancillary data
/// field if an extension bit stream exists"; the rest of mc_extension() is in the ext frame.
#[test]
fn ext_one_mc_data_beyond_the_base_part_cannot_be_verified() {
    let huge = Mc {
        ext: true,
        n_ad_bytes: 250,
        ..MC_3_2_LFE
    };
    let f = frame(with_mc(STEREO_256, huge));
    assert!(matches!(
        inspect(&f),
        ChannelFrame::Base {
            why: Fallback::Truncated(_),
            ..
        }
    ));
}

// ── mc_composite_status_info and allocation (13818-3 §2.5.1.15, §2.5.1.17) ─

/// Every dyn_cross_mode of every configuration (§2.5.2.15 tables A-E), with and without
/// transmission-channel switching per subband group, verifies against its CRC.
#[test]
fn every_dyn_cross_mode_verifies() {
    let configs: [(u8, u8, usize); 6] = [
        (0b01, 0b10, 15), // A) 3/2: '0000'..'1110'
        (0b01, 0b01, 5),  // B) 3/1: '000'..'100'
        (0b01, 0b00, 2),  // C) 3/0
        (0b01, 0b11, 2),  // C) 3/0 + 2/0
        (0b00, 0b10, 5),  // D) 2/2
        (0b00, 0b01, 2),  // E) 2/1
    ];
    let mut n = 0;
    for (centre, surround, modes) in configs {
        for mode in 0..modes as u8 {
            for (tc_sbgr_select, second_stereo, prediction) in [
                (true, false, false),
                (false, true, false),
                (true, false, true),
            ] {
                let m = Mc {
                    dyn_mode: Some(mode),
                    tc_sbgr_select,
                    second_stereo,
                    prediction,
                    lfe: false,
                    ..mc(centre, surround, false)
                };
                let f = frame(with_mc(STEREO_256, m));
                // §2.5.2.14 covers header through scfsi; a wrong field length breaks it.
                assert!(
                    multichannel(&f).is_some(),
                    "{centre}/{surround} mode {mode}"
                );
                n += 1;
            }
        }
    }
    assert_eq!(n, 3 * (15 + 5 + 2 + 2 + 5 + 2));
}

/// "'1111' forbidden" (3/2) and "'101' forbidden" (3/1, 2/2) never verify as multichannel.
#[test]
fn forbidden_dyn_cross_modes_are_rejected() {
    for (centre, surround, mode) in [(0b01, 0b10, 15), (0b01, 0b01, 5), (0b00, 0b10, 7)] {
        let st = Composite {
            dyn_cross_on: true,
            dyn_cross_lr: false,
            tc: [0; 12],
            dyn_mode: [mode; 12],
            second_stereo: [false; 12],
        };
        let m = McHeader {
            ext_bit_stream_present: false,
            centre,
            surround,
            lfe: false,
        };
        let r = allocation_source(&m, &config(&m), &st, 0, 0);
        assert_eq!(
            r.err(),
            Some(Fallback::ForbiddenDynCross),
            "{surround} {mode}"
        );
    }
}

/// Copy sources for missing channels, per the §2.5.2.15 rules quoted in each row.
#[test]
fn missing_channels_copy_their_allocation_as_the_tables_say() {
    let st = |mode: u8, tc: u8, lr: bool| Composite {
        dyn_cross_on: true,
        dyn_cross_lr: lr,
        tc: [tc; 12],
        dyn_mode: [mode; 12],
        second_stereo: [false; 12],
    };
    let src = |c: u8, s: u8, st: &Composite, mch: usize| {
        let m = McHeader {
            ext_bit_stream_present: false,
            centre: c,
            surround: s,
            lfe: false,
        };
        match allocation_source(&m, &config(&m), st, mch, 0).unwrap() {
            Source::Read => "read".to_string(),
            Source::Copy(t) => format!("T{t}"),
            Source::Zero => "zero".to_string(),
        }
    };
    // 3/2 '1001' "T23 - T4": T3 copied from T2.
    assert_eq!(src(1, 2, &st(9, 0, false), 1), "T2");
    // 3/2 '1000' "T2 T34 -": T4 copied from T3.
    assert_eq!(src(1, 2, &st(8, 0, false), 2), "T3");
    // 3/2 '0001' "T2 T3 -" at tc 0 (T4 = RSw): "RSw shall be copied from Ro".
    assert_eq!(src(1, 2, &st(1, 0, false), 2), "T1");
    // 3/2 '0011' "- T3 T4" at tc 0 (T2 = Cw): "Cw ... from Lo if dyn_cross_LR=='0'".
    assert_eq!(src(1, 2, &st(3, 0, false), 0), "T0");
    assert_eq!(src(1, 2, &st(3, 0, true), 0), "T1");
    // 3/2 at tc 1 (T2 = Lw): "Lw and LSw shall be copied from Lo".
    assert_eq!(src(1, 2, &st(3, 1, true), 0), "T0");
    // 3/1 '100' "T23 -": T3 copied from T2.
    assert_eq!(src(1, 1, &st(4, 0, false), 1), "T2");
    // 2/2 '010' "- T3" (T2 = LSw at tc 0): "LSw shall be copied from Lo".
    assert_eq!(src(0, 2, &st(2, 0, true), 0), "T0");
    // 2/1 '1' "-" at tc 2 (T2 = Rw): "Rw ... from Ro".
    assert_eq!(src(0, 1, &st(1, 2, false), 0), "T1");
    // 3/0 '0' "T2": transmitted.
    assert_eq!(src(1, 0, &st(0, 0, false), 0), "read");
    // 3/2 '0011' T2 missing across every tc: 1|7 from Lo, 2|6 from Ro, the rest by dyn_cross_LR.
    for (tc, lr, want) in [
        (7, true, "T0"),
        (6, false, "T1"),
        (4, false, "T0"),
        (4, true, "T1"),
    ] {
        assert_eq!(src(1, 2, &st(3, tc, lr), 0), want, "3/2 tc {tc} lr {lr}");
    }
    // 3/1 '011' T3 missing: tc 4|5 copy from Ro, else Lo unless tc<3 with dyn_cross_LR.
    for (tc, lr, want) in [
        (4, false, "T1"),
        (5, false, "T1"),
        (3, false, "T0"),
        (7, true, "T0"),
        (0, true, "T1"),
    ] {
        assert_eq!(src(1, 1, &st(3, tc, lr), 1), want, "3/1 tc {tc} lr {lr}");
    }
}

/// §2.5.2.13 centre "'11' centre bandwidth limited (Phantom coding)": "the subbands above
/// subband 11 are not transmitted".
#[test]
fn phantom_centre_stops_centre_allocation_above_subband_11() {
    let m = McHeader {
        ext_bit_stream_present: false,
        centre: 0b11,
        surround: 0b10,
        lfe: false,
    };
    let st = Composite {
        dyn_cross_on: false,
        dyn_cross_lr: false,
        tc: [0; 12],
        dyn_mode: [0; 12],
        second_stereo: [false; 12],
    };
    let cfg = config(&m);
    assert!(matches!(
        allocation_source(&m, &cfg, &st, 0, 11),
        Ok(Source::Read)
    ));
    assert!(matches!(
        allocation_source(&m, &cfg, &st, 0, 12),
        Ok(Source::Zero)
    ));
    assert!(matches!(
        allocation_source(&m, &cfg, &st, 1, 12),
        Ok(Source::Read)
    ));
}

/// §2.5.2.15 subband groups: "8 8...9", "9 10...11", "10 12...15", "11 16...31".
#[test]
fn subband_groups_follow_the_table() {
    let want = [0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 9, 10, 10, 10, 10];
    for (sb, g) in want.iter().enumerate() {
        assert_eq!(sbgr(sb), *g, "sb {sb}");
    }
    assert!((16..32).all(|sb| sbgr(sb) == 11));
}

/// §2.5.2.15 field lengths: tc_allocation "3 bits" (3/2, 3/1), "2 bits" (3/0, 2/2, 2/1),
/// "0 bits" (2/0, 1/0); dyn_cross_mode "4 bits", "3 bits", "1 bit", "0 bits".
#[test]
fn configuration_field_lengths_follow_the_tables() {
    let m = |c, s| McHeader {
        ext_bit_stream_present: false,
        centre: c,
        surround: s,
        lfe: false,
    };
    let rows = [
        (1, 2, 3, 3, 4),
        (1, 1, 2, 3, 3),
        (1, 0, 1, 2, 1),
        (1, 3, 3, 2, 1),
        (0, 2, 2, 2, 3),
        (0, 1, 1, 2, 1),
        (0, 0, 0, 0, 0),
        (0, 3, 2, 0, 0),
    ];
    for (c, s, nmch, tc, dyn_bits) in rows {
        let cfg = config(&m(c, s));
        assert_eq!(
            (cfg.nmch, cfg.tc_bits, cfg.dyn_bits),
            (nmch, tc, dyn_bits),
            "{c}/{s}"
        );
    }
}

// ── joint stereo bound (Opus defect 4) ─────────────────────────────────────

/// "bound==16" above 3-B.2c's sblimit 8 is capped at sblimit (no subband past sblimit is
/// coded, so a larger bound means sblimit), so the frame walks on to its mc_header
/// instead of falling back. This is per spec; do not change without a citation otherwise.
#[test]
fn joint_stereo_bound_is_capped_at_sblimit() {
    let s = Spec {
        mode: 0b01,
        mode_ext: 3,
        bitrate_index: 0b0100,
        alloc: 0, // no samples, so the mc part fits a 64 kbit/s frame
        ..STEREO_256
    }; // 64 kbit/s
    let f = frame(with_mc(s, mc(0b01, 0b00, false)));
    // Verifying the 3/0 mc_header ("'01' centre channel present") needs a full base walk.
    assert_eq!(settle(&run(f)), (Some(3), false));
}

// ── fallbacks and the bounded look (Opus defect 1) ─────────────────────────

#[test]
fn unsupported_frames_give_the_header_count() {
    let cases = [
        // 13818-3 §2.4.2.3: ID "'0' for extension to lower sampling frequencies".
        (
            Spec {
                id: 0,
                ..STEREO_256
            },
            Fallback::LowSamplingFrequency,
        ),
        // 11172-3 §2.4.2.3 Layer "\"11\" Layer I" / "\"01\" Layer III".
        (
            Spec {
                layer: 0b11,
                ..STEREO_256
            },
            Fallback::NotLayerII,
        ),
        (
            Spec {
                layer: 0b01,
                bitrate_index: 0b1001,
                ..STEREO_256
            },
            Fallback::NotLayerII,
        ),
        // §2.4.2.3: "'free format' condition" - no bitrate to choose a 3-B.2 table by.
        (
            Spec {
                bitrate_index: 0,
                ..STEREO_256
            },
            Fallback::FreeFormat,
        ),
    ];
    for (s, why) in cases {
        assert_eq!(inspect(&frame(s)), ChannelFrame::Base { nch: 2, why });
    }
}

/// Cut at every field boundary §2.4.1.6 and §2.5.1.13-17 define; never a panic.
#[test]
fn truncation_at_each_field_boundary_names_the_field() {
    let m = Mc {
        ext: true,
        dyn_mode: Some(1),
        prediction: true,
        ..MC_3_2_LFE
    };
    let (f, at) = write(with_mc(
        Spec {
            protection_bit: 0,
            ..STEREO_256
        },
        m,
    ));
    let fields: Vec<&str> = at.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        fields,
        [
            "crc_check",
            "allocation",
            "scfsi",
            "scalefactor",
            "samples",
            "ext_bit_stream_present",
            "n_ad_bytes",
            "centre",
            "surround",
            "lfe",
            "mc_header",
            "mc_crc_check",
            "tc_sbgr_select",
            "tc_allocation",
            "dyn_cross_LR",
            "mc_prediction",
            "lfe_allocation",
            "mc allocation",
            "mc scfsi",
        ]
    );
    for (field, bit) in at {
        let base_field = matches!(
            field,
            "crc_check" | "allocation" | "scfsi" | "scalefactor" | "samples"
        );
        let got = inspect_within(&f, bit);
        // The frame ends at the first bit of `field`.
        let why = Fallback::Truncated(match field {
            "tc_sbgr_select" => "tc_sbgr_select",
            "mc_prediction" => "mc_prediction",
            f => f,
        });
        let want = if base_field {
            ChannelFrame::Damaged { nch: 2, why }
        } else {
            ChannelFrame::Base { nch: 2, why }
        };
        assert_eq!(got, want, "{field}");
    }
    assert_eq!(inspect(&f[..3]), ChannelFrame::NoHeader);
}

/// Opus defect 1: a failed first frame must not lock in the base count.
#[test]
fn a_failed_first_frame_does_not_lock_in_the_base_count() {
    let good = frame(with_mc(STEREO_256, MC_3_2_LFE));
    // A zero-filled frame has no "syncword - the bit string '1111 1111 1111'".
    assert_eq!(
        settle(&[
            vec![0; good.len()],
            good.clone(),
            good.clone(),
            good.clone()
        ]),
        (Some(6), false)
    );
    let damaged = frame(Spec {
        bad_frame_crc: true,
        protection_bit: 0,
        ..with_mc(STEREO_256, MC_3_2_LFE)
    });
    // §2.4.3.1: a CRC mismatch means "a transmission error has occured".
    assert_eq!(
        settle(&[&[damaged][..], &run(good.clone())].concat()),
        (Some(6), false)
    );
    let bad_mc = frame(Spec {
        bad_mc_crc: true,
        ..with_mc(STEREO_256, MC_3_2_LFE)
    });
    assert_eq!(
        settle(&[&[bad_mc.clone(), bad_mc][..], &run(good)].concat()),
        (Some(6), false)
    );
}

// ── Scenario A: a forged-valid mc_crc_check (review round 3) ────────────────

fn bad_mc() -> Vec<u8> {
    frame(Spec {
        bad_mc_crc: true,
        ..with_mc(STEREO_256, MC_3_2_LFE)
    })
}

/// One frame whose ancillary bits happen to pass mc_crc_check (1 in 2^16) between plain
/// stereo frames is not multichannel: more than `nch` needs RUN_FRAMES in a row. Per the
/// run rule; do not change without a spec citation proving otherwise.
#[test]
fn a_single_forged_valid_frame_between_stereo_frames_stays_at_nch() {
    let forged = frame(with_mc(STEREO_256, MC_3_2_LFE));
    let stereo = bad_mc(); // "mandatory CRC-check" fails: plain MPEG-1 ancillary data
    let mut frames = vec![stereo.clone(); 5];
    frames.insert(2, forged);
    assert_eq!(settle(&frames), (Some(2), false));
}

/// Real ext '0' 5.1 (§2.5.2.13 "'0' no extension stream present") commits on the third
/// consecutive CRC-valid frame with the same mc_header, not before.
#[test]
fn real_ext_zero_5_1_commits_after_three_frames() {
    let good = frame(with_mc(STEREO_256, MC_3_2_LFE));
    let mut t = ChannelTracker::default();
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), Some(6));
}

/// A failed mc_crc_check in the middle of a run resets it, and so does a damaged or
/// header-less frame: the three CRC-valid frames must be consecutive.
#[test]
fn an_mc_crc_failure_or_damage_mid_run_resets_it() {
    let good = frame(with_mc(STEREO_256, MC_3_2_LFE));
    let mut t = ChannelTracker::default();
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&bad_mc()), None, "reset");
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), Some(6));
    let damaged = frame(Spec {
        bad_frame_crc: true,
        protection_bit: 0,
        ..with_mc(STEREO_256, MC_3_2_LFE)
    });
    let mut t = ChannelTracker::default();
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&damaged), None, "reset");
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&[0u8; 16]), None, "reset");
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), Some(6));
}

/// A different verified mc_header restarts the run instead of extending it.
#[test]
fn a_changed_mc_header_restarts_the_run() {
    let a = frame(with_mc(STEREO_256, MC_3_2_LFE));
    let b = frame(with_mc(STEREO_256, mc(0b01, 0b00, false)));
    assert_eq!(
        settle(&[a.clone(), a.clone(), b.clone(), b.clone(), b]),
        (Some(3), false)
    );
}

/// A verified count equal to `nch` (ext '1': the rest "is not in the track") commits on one
/// frame, as does a 2/0 + second stereo programme.
#[test]
fn a_verified_count_equal_to_nch_commits_on_one_frame() {
    let ext = frame(with_mc(
        STEREO_256,
        Mc {
            ext: true,
            ..MC_3_2_LFE
        },
    ));
    let mut t = ChannelTracker::default();
    assert_eq!((t.observe(&ext), t.extension_signalled()), (Some(2), true));
    let second = frame(with_mc(STEREO_256, mc(0b00, 0b11, false)));
    assert_eq!(ChannelTracker::default().observe(&second), Some(2));
}

/// A run under way at MAX_FRAMES is followed to its end: it commits if it completes.
#[test]
fn a_run_under_way_at_the_bound_is_followed_to_its_end() {
    let good = frame(with_mc(STEREO_256, MC_3_2_LFE));
    let mut t = ChannelTracker::default();
    for i in 1..MAX_FRAMES {
        assert_eq!(t.observe(&bad_mc()), None, "frame {i}");
    }
    assert_eq!(t.observe(&good), None, "frame MAX_FRAMES starts a run");
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&good), Some(6));
    // A run that breaks past the bound settles on the base count at once.
    let mut t = ChannelTracker::default();
    for _ in 1..MAX_FRAMES {
        t.observe(&bad_mc());
    }
    assert_eq!(t.observe(&good), None);
    assert_eq!(t.observe(&bad_mc()), Some(2));
    assert_eq!(t.observe(&good), None, "settled");
}

/// The look is bounded: base count after MAX_FRAMES clean MPEG-1 frames, and nothing
/// settled (the IFO value stands) when no frame ever had a header.
#[test]
fn the_look_is_bounded_to_max_frames() {
    let plain = frame(Spec {
        mode: 0b11,
        bitrate_index: 0b1010,
        ..STEREO_256
    });
    let mut t = ChannelTracker::default();
    for i in 1..MAX_FRAMES {
        assert_eq!(t.observe(&plain), None, "frame {i}");
    }
    assert_eq!(t.observe(&plain), Some(1));
    assert_eq!(
        t.observe(&frame(with_mc(STEREO_256, MC_3_2_LFE))),
        None,
        "already settled"
    );
    let mut z = ChannelTracker::default();
    for _ in 0..MAX_FRAMES + 5 {
        assert_eq!(z.observe(&[0u8; 16]), None);
    }
    assert_eq!(z.finish(), None);
    assert_eq!(
        z.evidence().0,
        MAX_FRAMES,
        "stops examining after the bound"
    );
}

/// A track shorter than the bound settles on its base count at the end.
#[test]
fn a_short_track_settles_on_finish() {
    let mut t = ChannelTracker::default();
    assert_eq!(t.observe(&frame(STEREO_256)), None);
    assert_eq!(t.finish(), Some(2));
    assert_eq!(t.finish(), None, "settles once");
}

#[test]
fn random_bytes_never_panic() {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let seeds = [
        frame(with_mc(STEREO_256, MC_3_2_LFE)),
        frame(with_mc(
            STEREO_256,
            Mc {
                dyn_mode: Some(9),
                prediction: true,
                ..MC_3_2_LFE
            },
        )),
        frame(with_mc(
            Spec {
                protection_bit: 0,
                ..STEREO_256
            },
            mc(0b01, 0b11, true),
        )),
    ];
    for i in 0..30_000 {
        let len = (next() % 1600) as usize;
        let mut f: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        if i % 2 == 0 && len >= 4 {
            f[0] = 0xFF;
            f[1] = 0xFC | (f[1] & 0x01); // syncword, ID 1, Layer II ('10')
        }
        if i % 3 == 0 {
            f = seeds[i % seeds.len()].clone();
            let at = (next() as usize) % f.len();
            f[at] ^= next() as u8;
            f.truncate((next() as usize) % (f.len() + 1));
        }
        let mut t = ChannelTracker::default();
        if let Some(n) = t.observe(&f).or_else(|| t.finish()) {
            assert!((1..=6).contains(&n), "{n}");
        }
    }
}

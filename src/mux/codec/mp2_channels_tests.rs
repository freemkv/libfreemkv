use super::*;

/// 13818-3 §2.5.1.3 (Layer II): the frame is these parts, in this order.
const SPEC_FRAME_LAYER_II: &str = "frame() { mpeg1_header() mpeg1_error_check() \
     mpeg1_audio_data() mc_extension_data_part1() if layer<3 mpeg1_ancillary_data() }";
/// 13818-3 §2.5.1.8 syntax, the fields this module reads.
const SPEC_MC_HEADER: &str = "mc_header() { ext_bit_stream_present 1 \
     if ext_bit_stream_present=='1' n_ad_bytes 8 centre 2 surround 2 lfe 1 ...";
/// 13818-3 §0.2.3.1: what an MPEG-1 decoder gets from a multichannel stream.
const SPEC_BACKWARDS: &str = "an ISO/IEC 11172-3 audio decoder properly decodes the basic \
     stereo information";

// Writer tables, from dist10 independently of the parser: nbal per subband ("sb 0 0 nbal").
const NBAL_AB: [u8; 30] = [
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2,
];
const NBAL_CD: [u8; 12] = [4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3];

/// Bits per granule the writer spends on one allocated subband: dist10 "sb 1 3 5 1 0" (3
/// levels, one 5-bit code) for allocation 1; allocation 2 is "0 2 7 3 3 2" (3 x 3 bits) in
/// table a/b subbands 0-2 and "3 2 5 7 1 1" (one 7-bit code) everywhere else.
fn writer_granule_bits(table_ab: bool, sb: usize, alloc: u8) -> usize {
    match alloc {
        0 => 0,
        1 => 5,
        2 if table_ab && sb < 3 => 9,
        2 => 7,
        _ => unreachable!("writer only uses allocations 0..=2"),
    }
}

/// One `mc_header()`; `n_ad_bytes` is written only when `ext` is set (§2.5.1.8).
#[derive(Clone, Copy)]
pub(crate) struct Mc {
    pub ext: bool,
    pub centre: u8,
    pub surround: u8,
    pub lfe: bool,
}

/// A synthetic Layer II frame. `alloc` is written in every subband and channel.
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
};

pub(crate) const MC_3_2_LFE: Mc = Mc {
    ext: false,
    centre: 0b01,
    surround: 0b10,
    lfe: true,
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

/// Writes `s` following 11172-3 §2.4.1.6 and 13818-3 §2.5.1.3; returns the frame and the bit
/// offset of each field (for truncation tests).
pub(crate) fn write(s: Spec) -> (Vec<u8>, Vec<(&'static str, usize)>) {
    let mut w = Writer::default();
    let mut at = Vec::new();
    w.put(0xFFF, 12); // §2.4.2.3 syncword
    w.put(u32::from(s.id), 1);
    w.put(u32::from(s.layer), 2);
    w.put(u32::from(s.protection_bit), 1);
    w.put(u32::from(s.bitrate_index), 4);
    w.put(u32::from(s.fs), 2);
    w.put(0, 2); // padding_bit, private_bit
    w.put(u32::from(s.mode), 2);
    w.put(u32::from(s.mode_ext), 2);
    w.put(0, 4); // copyright, original/home, emphasis
    if s.protection_bit == 0 {
        at.push(("crc_check", w.bits.len()));
        w.put(0, 16);
    }
    if s.fs == 0b11 || s.bitrate_index == 15 {
        return (w.bytes(), at); // reserved header values: no body layout exists
    }
    let kbps = LAYER_II_KBPS[s.bitrate_index as usize];
    let nch = if s.mode == 0b11 { 1 } else { 2 };
    let fs = [44_100, 48_000, 32_000][s.fs as usize];
    let per_ch = kbps / nch as u32;
    // dist10 pick_table, restated so the writer does not borrow the parser's choice.
    let (table_ab, sblimit) = if (fs == 48_000 && per_ch >= 56) || (56..=80).contains(&per_ch) {
        (true, 27)
    } else if fs != 48_000 && per_ch >= 96 {
        (true, 30)
    } else if fs != 32_000 && per_ch <= 48 {
        (false, 8)
    } else {
        (false, 12)
    };
    let nbal = |sb: usize| if table_ab { NBAL_AB[sb] } else { NBAL_CD[sb] };
    let bound = if s.mode == 0b01 {
        4 * (s.mode_ext as usize + 1)
    } else {
        sblimit
    };
    at.push(("allocation", w.bits.len()));
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
        at.push(("scalefactor", w.bits.len()));
        let per = match s.scfsi {
            0 => 3,
            2 => 1,
            _ => 2,
        };
        w.put(0, sblimit * nch * per * 6);
    }
    let mut granule = 0;
    for sb in 0..sblimit {
        let per_sb = if sb < bound { nch } else { 1 };
        granule += per_sb * writer_granule_bits(table_ab, sb, s.alloc);
    }
    if granule > 0 {
        at.push(("samples", w.bits.len()));
        w.put(0, 12 * granule);
    }
    if let Some(mc) = s.mc {
        at.push(("ext_bit_stream_present", w.bits.len()));
        w.put(u32::from(mc.ext), 1);
        if mc.ext {
            at.push(("n_ad_bytes", w.bits.len()));
            w.put(0, 8);
        }
        at.push(("centre", w.bits.len()));
        w.put(u32::from(mc.centre), 2);
        at.push(("surround", w.bits.len()));
        w.put(u32::from(mc.surround), 2);
        at.push(("lfe", w.bits.len()));
        w.put(u32::from(mc.lfe), 1);
        w.put(0, 11); // audio_mix .. copyright_identification_start
    }
    (w.bytes(), at)
}

fn frame(s: Spec) -> Vec<u8> {
    write(s).0
}

fn with_mc(s: Spec, mc: Mc) -> Spec {
    Spec { mc: Some(mc), ..s }
}

// ── header (11172-3 §2.4.2.3) ──────────────────────────────────────────────

#[test]
fn nch_follows_mode_for_all_four_modes() {
    for (mode, want) in [(0b00, 2), (0b01, 2), (0b10, 2), (0b11, 1)] {
        let h = Header::parse(&frame(Spec { mode, ..STEREO_256 })).unwrap();
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
        Header::parse(&frame(Spec {
            layer: 0,
            ..STEREO_256
        })),
        None
    );
    // §2.4.2.3 sampling_frequency: "'11' reserved".
    assert_eq!(
        Header::parse(&frame(Spec {
            fs: 0b11,
            ..STEREO_256
        })),
        None
    );
    // §2.4.2.3 bitrate table lists '0000'..'1110' only.
    assert_eq!(
        Header::parse(&frame(Spec {
            bitrate_index: 15,
            ..STEREO_256
        })),
        None
    );
    // §2.4.2.3: "The first 32 bits (four bytes) are header information".
    assert_eq!(Header::parse(&good[..3]), None);
}

#[test]
fn header_reads_sampling_frequency_and_bitrate_columns() {
    for (fs, hz) in [(0b00, 44_100), (0b01, 48_000), (0b10, 32_000)] {
        let h = Header::parse(&frame(Spec { fs, ..STEREO_256 })).unwrap();
        // §2.4.2.3: "'00' 44.1 kHz", "'01' 48 kHz", "'10' 32 kHz".
        assert_eq!(h.sampling_hz, hz);
    }
    for (i, kbps) in [(1, 32), (4, 64), (12, 256), (14, 384)] {
        let h = Header::parse(&frame(Spec {
            bitrate_index: i,
            ..STEREO_256
        }))
        .unwrap();
        // §2.4.2.3 bitrate table, "Layer II" column.
        assert_eq!(h.bitrate_kbps, Some(kbps));
    }
    let free = Header::parse(&frame(Spec {
        bitrate_index: 0,
        ..STEREO_256
    }))
    .unwrap();
    // §2.4.2.3: "The all zero value indicates the 'free format' condition".
    assert_eq!(free.bitrate_kbps, None);
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
        (0b01, 0b0011, 0b11, 27), // 48 kHz mono 56 kbit/s: "sfrq == 48 && br_per_ch >= 56"
        (0b00, 0b0101, 0b11, 27), // 44.1 kHz mono 80: "br_per_ch >= 56 && br_per_ch <= 80"
        (0b00, 0b1100, 0b00, 30), // 44.1 kHz 128/ch: "sfrq != 48 && br_per_ch >= 96"
        (0b10, 0b0110, 0b11, 30), // 32 kHz mono 96
        (0b01, 0b0100, 0b00, 8),  // 48 kHz 32/ch: "sfrq != 32 && br_per_ch <= 48"
        (0b00, 0b0010, 0b11, 8),  // 44.1 kHz mono 48
        (0b10, 0b0100, 0b00, 12), // 32 kHz 32/ch: "else table = 3"
        (0b10, 0b0010, 0b11, 12), // 32 kHz mono 48
    ];
    for (fs, bitrate_index, mode, sblimit) in cases {
        let h = Header::parse(&frame(Spec {
            fs,
            bitrate_index,
            mode,
            ..STEREO_256
        }))
        .unwrap();
        assert_eq!(
            allocation_table(&h, h.bitrate_kbps.unwrap()).len(),
            sblimit,
            "fs {fs:02b} br {bitrate_index} mode {mode:02b}"
        );
    }
}

// ── mc_header location (13818-3 §2.5.1.3) ──────────────────────────────────

/// Every table, mode, CRC setting and allocation lands on the written mc_header exactly.
#[test]
fn mc_header_found_after_audio_data_in_every_layout() {
    let _ = (SPEC_FRAME_LAYER_II, SPEC_MC_HEADER);
    let tables = [
        (0b01, 0b1100),
        (0b00, 0b1100),
        (0b01, 0b0100),
        (0b10, 0b0100),
    ];
    let modes = [
        (0b00, 0),
        (0b10, 0),
        (0b11, 0),
        (0b01, 0),
        (0b01, 1),
        (0b01, 2),
        (0b01, 3),
    ];
    let mut checked = 0;
    for (fs, bitrate_index) in tables {
        for (mode, mode_ext) in modes {
            for protection_bit in [0, 1] {
                for (alloc, scfsi) in [(0, 0), (1, 0), (1, 1), (1, 2), (1, 3), (2, 0)] {
                    let mc = Mc {
                        ext: alloc == 2,
                        centre: 0b11,
                        surround: 0b01,
                        lfe: true,
                    };
                    let s = Spec {
                        fs,
                        bitrate_index,
                        mode,
                        mode_ext,
                        protection_bit,
                        alloc,
                        scfsi,
                        mc: Some(mc),
                        ..STEREO_256
                    };
                    let f = frame(s);
                    let h = Header::parse(&f).unwrap();
                    let sblimit = allocation_table(&h, h.bitrate_kbps.unwrap()).len();
                    let got = mc_header(&f, &h);
                    if mode == 0b01 && 4 * (mode_ext as usize + 1) > sblimit {
                        assert_eq!(got, Err(Fallback::BoundAboveSblimit));
                        continue;
                    }
                    // §2.5.1.8 fields read back exactly as §2.5.1.3 placed them.
                    let want = McHeader {
                        ext_bit_stream_present: mc.ext,
                        centre: 0b11,
                        surround: 0b01,
                        lfe: true,
                    };
                    assert_eq!(
                        got,
                        Ok(want),
                        "fs {fs} br {bitrate_index} mode {mode}/{mode_ext} crc {protection_bit} alloc {alloc}/{scfsi}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 250, "{checked}");
}

// ── channel configuration (13818-3 §2.5.2.8) ───────────────────────────────

/// Every centre x surround x lfe value with the multichannel data stored in the base frame.
/// This is per spec; do not change without a spec citation proving otherwise.
#[test]
fn every_mc_configuration_counts_its_channels() {
    // (centre, surround, name, channels before LFE) per §2.5.2.8 and dist10 mc_hdr_to_frps.
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
        (0b00, 0b11, "2/0 + 2/0", 4),
        (0b01, 0b11, "3/0 + 2/0", 5),
        (0b11, 0b11, "3/0 + 2/0 phantom centre", 5),
    ];
    for (centre, surround, name, main) in configs {
        for lfe in [false, true] {
            let f = frame(with_mc(
                STEREO_256,
                Mc {
                    ext: false,
                    centre,
                    surround,
                    lfe,
                },
            ));
            let want = main + u8::from(lfe);
            // §2.5.2.8: "'0' no extension stream present" - all channels are in this track.
            assert_eq!(
                stored_channels(&f, true),
                Some(Stored {
                    channels: want,
                    fallback: None
                }),
                "{name} lfe {lfe}"
            );
        }
    }
}

/// 1/0 (13818-3 §0.2.2.2 lists "3/1, 3/0, 2/2, 2/1, 2/0, and 1/0"): a single_channel base.
#[test]
fn mono_base_with_no_mc_channels_is_one() {
    let f = frame(with_mc(
        Spec {
            mode: 0b11,
            ..STEREO_256
        },
        Mc {
            ext: false,
            centre: 0,
            surround: 0,
            lfe: false,
        },
    ));
    // 13818-3 §0.2.3.2 i): "One channel, using the 1/0 configuration".
    assert_eq!(stored_channels(&f, true).unwrap().channels, 1);
}

/// This is per spec; do not change without a spec citation proving otherwise.
#[test]
fn ext_zero_3_2_lfe_is_six_not_two() {
    let f = frame(with_mc(STEREO_256, MC_3_2_LFE));
    // §2.5.2.8: "'0' no extension stream present"; centre '01', surround '10', lfe '1'.
    assert_eq!(stored_channels(&f, true).unwrap().channels, 6);
}

#[test]
fn centre_10_is_not_defined_and_falls_back() {
    let f = frame(with_mc(
        STEREO_256,
        Mc {
            ext: false,
            centre: 0b10,
            surround: 0b10,
            lfe: true,
        },
    ));
    // §2.5.2.8 centre: "'10' not defined".
    assert_eq!(
        stored_channels(&f, true),
        Some(Stored {
            channels: 2,
            fallback: Some(Fallback::UndefinedCentre)
        })
    );
}

/// With an extension bit stream the track holds only the base; count per mode.
#[test]
fn ext_one_is_the_base_count_in_every_mode() {
    let _ = SPEC_BACKWARDS;
    for (mode, mode_ext, nch) in [
        (0b00, 0, 2),
        (0b01, 0, 2),
        (0b01, 3, 2),
        (0b10, 0, 2),
        (0b11, 0, 1),
    ] {
        let mc = Mc {
            ext: true,
            ..MC_3_2_LFE
        };
        let f = frame(with_mc(
            Spec {
                mode,
                mode_ext,
                ..STEREO_256
            },
            mc,
        ));
        // §2.5.2.8: "'1' extension bit stream present"; §0.2.3.1 base = "basic stereo".
        assert_eq!(
            stored_channels(&f, true),
            Some(Stored {
                channels: nch,
                fallback: None
            }),
            "mode {mode:02b}"
        );
    }
}

/// Plain MPEG-1 Layer II: ancillary bits are never read as an mc_header.
/// This is per spec; do not change without a spec citation proving otherwise.
#[test]
fn undeclared_multichannel_is_the_header_count() {
    for (mode, nch) in [(0b00, 2), (0b01, 2), (0b10, 2), (0b11, 1)] {
        let f = frame(with_mc(Spec { mode, ..STEREO_256 }, MC_3_2_LFE));
        // 11172-3 §2.4.2.8: "Ancillary_bit - user definable" - no channels; 13818-3 "nch".
        assert_eq!(
            stored_channels(&f, false),
            Some(Stored {
                channels: nch,
                fallback: None
            })
        );
    }
}

// ── fallbacks ──────────────────────────────────────────────────────────────

#[test]
fn unsupported_frames_fall_back_to_the_header_count() {
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
        // §2.4.2.3 "bound==16" above 3-B.2c's sblimit 8.
        (
            Spec {
                mode: 0b01,
                mode_ext: 3,
                bitrate_index: 0b0100,
                ..STEREO_256
            },
            Fallback::BoundAboveSblimit,
        ),
    ];
    for (s, why) in cases {
        let f = frame(with_mc(s, MC_3_2_LFE));
        assert_eq!(
            stored_channels(&f, true),
            Some(Stored {
                channels: 2,
                fallback: Some(why)
            })
        );
    }
}

/// Cut at every field boundary §2.4.1.6 and §2.5.1.8 define; never a panic.
#[test]
fn truncation_at_each_field_boundary_names_the_field() {
    let s = with_mc(
        Spec {
            protection_bit: 0,
            ..STEREO_256
        },
        Mc {
            ext: true,
            ..MC_3_2_LFE
        },
    );
    let (f, at) = write(s);
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
            "lfe"
        ]
    );
    let h = Header::parse(&f).unwrap();
    let lfe_end = at.last().unwrap().1 + 1;
    for (field, bit) in at {
        // §2.5.1.8 / §2.4.1.6: the frame ends at the first bit of `field`.
        let got = mc_header_within(&f, &h, bit);
        assert_eq!(got.map(|_| ()), Err(Fallback::Truncated(field)), "{field}");
    }
    // Byte-level cuts short of `lfe`, as a real short frame arrives: base count, no panic.
    for len in 4..lfe_end / 8 {
        let got = stored_channels(&f[..len], true).unwrap();
        assert_eq!(
            (got.channels, got.fallback.is_some()),
            (2, true),
            "len {len}"
        );
    }
    assert_eq!(stored_channels(&f[..3], true), None, "no full header");
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
    let valid = frame(with_mc(STEREO_256, MC_3_2_LFE));
    for i in 0..20_000 {
        let len = (next() % 1600) as usize;
        let mut f: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        if i % 2 == 0 && len >= 4 {
            f[0] = 0xFF; // force a syncword so the walk runs
            f[1] |= 0xF0;
        }
        if i % 3 == 0 {
            f = valid.clone();
            let at = (next() as usize) % f.len();
            f[at] ^= next() as u8;
            f.truncate((next() as usize) % (f.len() + 1));
        }
        for declared in [false, true] {
            if let Some(s) = stored_channels(&f, declared) {
                assert!((1..=8).contains(&s.channels), "{s:?}");
            }
        }
    }
}

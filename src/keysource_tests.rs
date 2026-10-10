use super::*;
use crate::aacs::types::UnitKey;

fn units(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| vec![i as u8; 4]).collect()
}

// ── DecodeSampleSet: the online request can't be built under-sized ─────────

/// Fewer than MIN_SAMPLE_UNITS → no set. Mutation: accepting a short slice
/// resurrects the exact autorip bug (a 4-sample request silently skipped /
/// read as "service down").
#[test]
fn decode_sample_set_rejects_under_min() {
    for n in 0..MIN_SAMPLE_UNITS {
        assert!(
            DecodeSampleSet::new(units(n)).is_none(),
            "{n} samples (< {MIN_SAMPLE_UNITS}) must not build a DecodeSampleSet"
        );
    }
}

/// Exactly the minimum, and above it, construct — and expose all samples.
#[test]
fn decode_sample_set_accepts_min_and_above() {
    let exact = DecodeSampleSet::new(units(MIN_SAMPLE_UNITS)).expect("min builds");
    assert_eq!(exact.len(), MIN_SAMPLE_UNITS);
    assert_eq!(exact.units().len(), MIN_SAMPLE_UNITS);
    assert!(!exact.is_empty());

    let more = DecodeSampleSet::new(units(MIN_SAMPLE_UNITS + 5)).expect("above min builds");
    assert_eq!(more.len(), MIN_SAMPLE_UNITS + 5);
}

/// The wrapped units round-trip byte-for-byte (the request carries exactly what
/// was gathered — no reordering/truncation).
#[test]
fn decode_sample_set_preserves_units() {
    let raw = units(MIN_SAMPLE_UNITS);
    let set = DecodeSampleSet::new(raw.clone()).unwrap();
    assert_eq!(set.units(), raw.as_slice());
}

// ── KeySource default-method behaviour ────────────────────────────────────

/// KeySource::host_certs() defaults to empty regardless of the MKB argument. Mutation
/// guard: a non-empty default would inject phantom certs into the OEM handshake.
#[test]
fn key_source_host_certs_defaults_to_empty() {
    struct MinimalSource;
    impl KeySource for MinimalSource {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(Vec::new())
        }
    }
    let s = MinimalSource;
    assert!(s.host_certs(None).is_empty());
    assert!(s.host_certs(Some(68)).is_empty());
}

/// The trait defaults are the contract external sources rely on (retry, per-piece asking,
/// VID use); a flipped default changes every foreign source's behaviour.
#[test]
fn key_source_defaults_are_conservative() {
    struct MinimalSource;
    impl KeySource for MinimalSource {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(Vec::new())
        }
    }
    let s = MinimalSource;
    assert!(!s.last_failure_was_transport());
    assert!(s.answer_depends_on_samples());
    assert!(!s.uses_vid());
}

/// DiscInputsCtx maps DiscInputs faithfully: zero VID → None, non-zero VID →
/// Some; title from volume_label; samples truncate to n; enc_title_keys
/// parses Unit_Key_RO.inf at the version stride.
#[test]
fn disc_inputs_ctx_maps_fields() {
    // Build a minimal V10 Unit_Key_RO.inf with one key (stride 48):
    // uk_pos = 32, num_uk = 1, key at uk_pos + 48 = 80.
    let mut uk_ro = vec![0u8; 96];
    let uk_pos = 32usize;
    uk_ro[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    uk_ro[uk_pos] = 0x00;
    uk_ro[uk_pos + 1] = 0x01; // num_unit_keys = 1
    let key_bytes = [0x7Eu8; 16];
    uk_ro[80..96].copy_from_slice(&key_bytes);

    let inputs = DiscInputs {
        disc_hash: "0xABC".into(),
        volume_id: [0u8; 16],
        version: crate::aacs::mkb::AACS_MAJOR_BD,
        mkb: vec![1, 2, 3],
        unit_key_ro: uk_ro,
        samples: vec![vec![9u8; 4], vec![8u8; 4], vec![7u8; 4]],
        volume_label: Some("TITLE_X".into()),
    };

    // Zero VID → None.
    let ctx = DiscInputsCtx::new(&inputs);
    assert_eq!(ctx.disc_hash(), "0xABC");
    assert_eq!(ctx.title(), Some("TITLE_X"));
    assert!(ctx.vid().is_none(), "all-zero VID is the no-VID sentinel");
    assert_eq!(ctx.mkb().unwrap(), &[1, 2, 3]);
    assert_eq!(ctx.enc_title_keys().unwrap(), &[key_bytes]);
    assert_eq!(ctx.samples(2).unwrap().len(), 2, "samples truncates to n");
    assert_eq!(ctx.unit_key_ro(), &inputs.unit_key_ro[..]);

    // Non-zero VID → Some(vid).
    let mut inputs2 = inputs.clone();
    inputs2.volume_id = [0x42u8; 16];
    let ctx2 = DiscInputsCtx::new(&inputs2);
    assert_eq!(ctx2.vid(), Some(Vid([0x42u8; 16])));
}

// The documented contract: garbage Unit_Key_RO parses to no keys, not an error.
#[test]
fn malformed_unit_key_ro_yields_an_empty_key_set() {
    for garbage in [vec![0xFFu8; 7], vec![0xFF; 96], vec![0u8; 3]] {
        let mut inputs = empty_inputs();
        inputs.unit_key_ro = garbage;
        let ctx = DiscInputsCtx::new(&inputs);
        assert!(ctx.enc_title_keys().unwrap().is_empty());
    }
}

// A source scripted per read: `Ok` fills every unit CPI-set; errors are by call index.
struct ScriptedSource {
    calls: Vec<u32>,
    fail: fn(usize) -> Option<crate::error::Error>,
}
impl crate::sector::SectorSource for ScriptedSource {
    fn capacity_sectors(&self) -> u32 {
        u32::MAX
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _r: bool,
    ) -> crate::error::Result<usize> {
        self.calls.push(lba);
        if let Some(e) = (self.fail)(self.calls.len() - 1) {
            return Err(e);
        }
        let bytes = count as usize * 2048;
        for c in buf[..bytes].chunks_mut(crate::aacs::content::ALIGNED_UNIT_LEN) {
            c.fill(0xAB);
            c[0] = 0xC0;
        }
        Ok(bytes)
    }
}

fn title_at(start_lba: u32, units: u32) -> crate::disc::DiscTitle {
    crate::disc::DiscTitle {
        selection_evidence: Default::default(),
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents: vec![crate::disc::Extent {
            start_lba,
            sector_count: units * crate::aacs::content::ALIGNED_UNIT_SECTORS,
        }],
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    }
}

// An ordinary read error skips that probe only; a Stop ends sampling at once.
#[test]
fn read_encrypted_units_skips_errors_but_stops_on_halt() {
    let title = title_at(1000, 600);
    let mut src = ScriptedSource {
        calls: Vec::new(),
        fail: |i| {
            (i == 0).then_some(crate::error::Error::DiscRead {
                sector: 0,
                status: None,
                sense: None,
            })
        },
    };
    let got = read_encrypted_units(&mut src, &title, 3);
    assert_eq!(got.len(), 3, "later probes still sampled after one error");

    let mut src = ScriptedSource {
        calls: Vec::new(),
        fail: |_| Some(crate::error::Error::Halted),
    };
    let got = read_encrypted_units(&mut src, &title, 3);
    assert!(got.is_empty());
    assert_eq!(src.calls.len(), 1, "no probe after Halted");
}

// `n == 0` reads nothing and returns nothing.
#[test]
fn read_encrypted_units_zero_wanted_reads_nothing() {
    let mut src = ScriptedSource {
        calls: Vec::new(),
        fail: |_| None,
    };
    assert!(read_encrypted_units(&mut src, &title_at(1000, 600), 0).is_empty());
    assert!(src.calls.is_empty());
}

// Extent LBAs are untrusted: near u32::MAX the arithmetic saturates, never panics.
#[test]
fn read_encrypted_units_saturates_lba_near_u32_max() {
    let mut src = ScriptedSource {
        calls: Vec::new(),
        fail: |_| {
            Some(crate::error::Error::DiscRead {
                sector: 0,
                status: None,
                sense: None,
            })
        },
    };
    let title = title_at(u32::MAX - 10, 600);
    assert!(read_encrypted_units(&mut src, &title, 3).is_empty());
    assert!(!src.calls.is_empty());
    assert!(src.calls.iter().all(|&l| l >= u32::MAX - 10));
}

// ── fetch_unit_keys (the one shared fetch path) ───────────────────────────

fn empty_inputs() -> DiscInputs {
    DiscInputs {
        disc_hash: String::new(),
        volume_id: [0u8; 16],
        version: crate::aacs::mkb::AACS_MAJOR_UHD,
        mkb: Vec::new(),
        unit_key_ro: Vec::new(),
        samples: Vec::new(),
        volume_label: None,
    }
}

/// #4 regression: content NOT at the extent midpoint (late-starting, or midpoint in clear
/// nav) must still be sampled. The old midpoint-forward sampler returned empty.
#[test]
fn read_encrypted_units_finds_scrambled_content_off_the_midpoint() {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    use crate::error::Result;
    use crate::sector::SectorSource;

    // Units in the FIRST SIXTH of the extent are scrambled (0xFF → no TS
    // sync); everything else (incl. the midpoint) is clear (0x47 syncs).
    struct BandSource {
        ext_start: u32,
        total_units: u32,
    }
    impl SectorSource for BandSource {
        fn capacity_sectors(&self) -> u32 {
            self.ext_start + self.total_units * ALIGNED_UNIT_SECTORS + 64
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _r: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            for (i, chunk) in buf[..bytes].chunks_mut(ALIGNED_UNIT_LEN).enumerate() {
                if chunk.len() < ALIGNED_UNIT_LEN {
                    break;
                }
                let abs_unit = (lba - self.ext_start) / ALIGNED_UNIT_SECTORS + i as u32;
                if abs_unit < self.total_units / 6 {
                    chunk.fill(0xFF); // scrambled: no TS sync
                } else {
                    chunk.fill(0);
                    let mut o = 4;
                    while o < ALIGNED_UNIT_LEN {
                        chunk[o] = 0x47; // clear TS syncs
                        o += 192;
                    }
                }
            }
            Ok(bytes)
        }
    }

    let total_units = 600u32;
    let ext_start = 1000u32;
    let mut src = BandSource {
        ext_start,
        total_units,
    };
    let title = crate::disc::DiscTitle {
        selection_evidence: Default::default(),
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents: vec![crate::disc::Extent {
            start_lba: ext_start,
            sector_count: total_units * ALIGNED_UNIT_SECTORS,
        }],
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let samples = read_encrypted_units(&mut src, &title, 4);
    assert!(
        !samples.is_empty(),
        "the probe-spread must sample the early scrambled band the midpoint misses"
    );
    for s in &samples {
        assert!(
            aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs),
            "every sample is a CPI-flagged encrypted unit (byte0 & 0xC0 != 0)"
        );
    }
}

/// DISCRIMINATING: selection is by AACS CPI (byte 0), NOT TS-sync clarity — half the units
/// lack TS syncs but are CPI-clear (genuinely unencrypted). Must return ONLY CPI-flagged
/// units.
#[test]
fn read_encrypted_units_selects_by_cpi_not_ts_sync() {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    use crate::error::Result;
    use crate::sector::SectorSource;

    // Even units: CPI-clear, sync-destroyed. Odd units: CPI-set, scrambled body.
    // Neither has clean TS syncs (`is_clean` is FALSE for both), but
    // `aacs_unit_encrypted` must flag only the odd (CPI-set) units.
    struct MixSource {
        ext_start: u32,
        total_units: u32,
    }
    impl SectorSource for MixSource {
        fn capacity_sectors(&self) -> u32 {
            self.ext_start + self.total_units * ALIGNED_UNIT_SECTORS + 64
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _r: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            for (i, chunk) in buf[..bytes].chunks_mut(ALIGNED_UNIT_LEN).enumerate() {
                if chunk.len() < ALIGNED_UNIT_LEN {
                    break;
                }
                let abs = (lba - self.ext_start) / ALIGNED_UNIT_SECTORS + i as u32;
                if abs.is_multiple_of(2) {
                    chunk.fill(0x11); // CPI-clear (0x11 & 0xC0 == 0), no TS sync
                } else {
                    chunk.fill(0xAB); // scrambled body (no TS sync)
                    chunk[0] = 0xC0; // CPI set -> encrypted
                }
            }
            Ok(bytes)
        }
    }

    let total_units = 400u32;
    let ext_start = 500u32;
    let mut src = MixSource {
        ext_start,
        total_units,
    };
    let title = crate::disc::DiscTitle {
        selection_evidence: Default::default(),
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents: vec![crate::disc::Extent {
            start_lba: ext_start,
            sector_count: total_units * ALIGNED_UNIT_SECTORS,
        }],
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };

    let samples = read_encrypted_units(&mut src, &title, 8);
    assert!(
        !samples.is_empty(),
        "the CPI-flagged (odd) units must still be collected"
    );
    for s in &samples {
        assert!(
            aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs),
            "only CPI-flagged units are selected"
        );
        assert_eq!(
            s[0] & 0xC0,
            0xC0,
            "a CPI-clear sync-destroyed unit must never be sampled"
        );
    }
}

/// Audit #5 — DISCRIMINATING test for the version→stride fix: a 2-key `Unit_Key_RO.inf`
/// whose 2nd key sits at the V20 offset, so a V10 parse reads a DIFFERENT region.
#[test]
fn disc_inputs_ctx_parses_unit_keys_at_the_version_stride() {
    use crate::aacs::mkb::{AACS_MAJOR_BD, AACS_MAJOR_UHD};
    const UK_POS: usize = 64;
    let mut inf = vec![0u8; 200];
    inf[0..4].copy_from_slice(&(UK_POS as u32).to_be_bytes()); // uk_pos
    inf[UK_POS..UK_POS + 2].copy_from_slice(&2u16.to_be_bytes()); // num_uk = 2
    let key0_at = UK_POS + 48; // first key — same for both strides
    let key1_v10_at = key0_at + 48; // second key if parsed at V10 stride
    let key1_v20_at = key0_at + 64; // second key if parsed at V20 stride
    inf[key0_at..key0_at + 16].fill(0xA0);
    inf[key1_v10_at..key1_v10_at + 16].fill(0x10);
    inf[key1_v20_at..key1_v20_at + 16].fill(0x20);

    let base = DiscInputs {
        disc_hash: String::new(),
        volume_id: [0u8; 16],
        version: AACS_MAJOR_UHD,
        mkb: Vec::new(),
        unit_key_ro: inf,
        samples: Vec::new(),
        volume_label: None,
    };
    let k20 = DiscInputsCtx::new(&base).enc_title_keys().unwrap().to_vec();
    let v10_inputs = DiscInputs {
        version: AACS_MAJOR_BD,
        ..base.clone()
    };
    let k10 = DiscInputsCtx::new(&v10_inputs)
        .enc_title_keys()
        .unwrap()
        .to_vec();

    assert_eq!(k20.len(), 2);
    assert_eq!(k10.len(), 2);
    assert_eq!(k20[0], [0xA0; 16], "first key is at +48 for both strides");
    assert_eq!(k10[0], [0xA0; 16]);
    assert_eq!(k20[1], [0x20; 16], "V20 reads the 2nd key at +64");
    assert_eq!(k10[1], [0x10; 16], "V10 reads the 2nd key at +48");
    assert_ne!(k20[1], k10[1], "the parse stride follows inputs.version");
}

/// `DiscInputs` is public; a derived `Debug` printed the Volume ID, the
/// whole `Unit_Key_RO.inf`, MKB and every sample verbatim. Sentinel 0xD5 =
/// 213. Mutation guard: restoring `#[derive(Debug)]` fails this.
#[test]
fn disc_inputs_debug_is_redacted() {
    let inputs = DiscInputs {
        disc_hash: "0xAA".into(),
        volume_id: [0xD5; 16],
        version: 2,
        mkb: vec![0xD5; 64],
        unit_key_ro: vec![0xD5; 48],
        samples: vec![vec![0xD5; 6144]],
        volume_label: Some("TITLE_2024".into()),
    };
    let dbg = format!("{inputs:?}");
    assert!(
        !dbg.contains("213"),
        "DiscInputs Debug leaked key material (decimal 213): {dbg}"
    );
    assert!(
        dbg.contains("redacted"),
        "DiscInputs Debug missing redaction marker: {dbg}"
    );
    // Non-secret identity and shape stay printable for diagnostics.
    assert!(dbg.contains("0xAA"), "{dbg}");
    assert!(dbg.contains("mkb_len: 64"), "{dbg}");
    assert!(dbg.contains("unit_key_ro_len: 48"), "{dbg}");
    assert!(dbg.contains("samples_len: 1"), "{dbg}");
    assert!(dbg.contains("TITLE_2024"), "{dbg}");
}

/// `DecodeSampleSet` wraps the same on-disc ciphertext `DiscInputs` redacts; a derived
/// `Debug` would dump it verbatim. Sentinel 0xD5 = 213, matching the `DiscInputs` test
/// above.
#[test]
fn decode_sample_set_debug_is_redacted() {
    let set = DecodeSampleSet::new(vec![vec![0xD5; 6144]; MIN_SAMPLE_UNITS])
        .expect("MIN_SAMPLE_UNITS units is a valid set");
    let dbg = format!("{set:?}");
    assert!(
        !dbg.contains("213"),
        "DecodeSampleSet Debug leaked ciphertext (decimal 213): {dbg}"
    );
    assert!(
        dbg.contains("redacted"),
        "DecodeSampleSet Debug missing redaction marker: {dbg}"
    );
    // Non-secret shape stays printable for diagnostics.
    assert!(dbg.contains("units_len: 8"), "{dbg}");
}

/// A source whose every unit is CPI-encrypted, for exercising the count cap
/// and extent-skip logic without the clarity/CPI selection getting in the way.
struct AllEncryptedSource {
    cap: u32,
}
impl crate::sector::SectorSource for AllEncryptedSource {
    fn capacity_sectors(&self) -> u32 {
        self.cap
    }
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _r: bool,
    ) -> crate::error::Result<usize> {
        let bytes = count as usize * 2048;
        // 0xC0 in byte 0 of every unit → CPI-encrypted for BdTs.
        buf[..bytes].fill(0xC0);
        Ok(bytes)
    }
}

fn title_with_extents(extents: Vec<crate::disc::Extent>) -> crate::disc::DiscTitle {
    crate::disc::DiscTitle {
        selection_evidence: Default::default(),
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents,
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    }
}

/// `read_encrypted_units` returns AT MOST `n` units and stops as soon as it
/// has them — an all-encrypted extent must not spill every probe's worth of
/// samples when the caller asked for a few.
#[test]
fn read_encrypted_units_returns_at_most_n() {
    use crate::aacs::content::{ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    let total_units = 600u32;
    let ext_start = 1000u32;
    let mut src = AllEncryptedSource {
        cap: ext_start + total_units * ALIGNED_UNIT_SECTORS + 64,
    };
    let title = title_with_extents(vec![crate::disc::Extent {
        start_lba: ext_start,
        sector_count: total_units * ALIGNED_UNIT_SECTORS,
    }]);

    for n in [1usize, 3, 8] {
        let samples = read_encrypted_units(&mut src, &title, n);
        assert_eq!(samples.len(), n, "must return exactly the {n} requested");
        for s in &samples {
            assert!(aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs));
        }
    }
}

/// An extent too small to hold a single 3-sector aligned unit yields zero
/// units and must be SKIPPED, with sampling continuing into the next extent —
/// not abandoned, and not a panic on the empty extent.
#[test]
fn read_encrypted_units_skips_a_zero_unit_extent_and_samples_the_next() {
    use crate::aacs::content::{ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    let good_units = 400u32;
    let good_start = 5000u32;
    let mut src = AllEncryptedSource {
        cap: good_start + good_units * ALIGNED_UNIT_SECTORS + 64,
    };
    // Extent 0: only 2 sectors — fewer than one 3-sector aligned unit → 0 units.
    // Extent 1: a normal, sampleable extent.
    let title = title_with_extents(vec![
        crate::disc::Extent {
            start_lba: 10,
            sector_count: ALIGNED_UNIT_SECTORS - 1,
        },
        crate::disc::Extent {
            start_lba: good_start,
            sector_count: good_units * ALIGNED_UNIT_SECTORS,
        },
    ]);

    let samples = read_encrypted_units(&mut src, &title, 4);
    assert_eq!(
        samples.len(),
        4,
        "the empty first extent is skipped; the second extent supplies the samples"
    );
    for s in &samples {
        assert!(aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs));
    }
}

// ── codeaudit keys cluster ────────────────────────────────────────────────

// HD DVD: `unit_key_ro` carries a VTKF (DVD_HD_V_TKF); the ctx must parse it
// with the magic-dispatching parser, not the BD-only Unit_Key_RO layout.
#[test]
fn disc_inputs_ctx_parses_hddvd_vtkf_title_keys() {
    use crate::aacs::inf::VTKF_MAGIC;
    let key = [0x3Cu8; 16];
    let mut vtkf = Vec::new();
    vtkf.extend_from_slice(VTKF_MAGIC);
    vtkf.resize(0x80, 0);
    let mut entry = [0u8; 0x24];
    entry[0] = 0x80; // AV_FLG: present
    entry[4..20].copy_from_slice(&key);
    vtkf.extend_from_slice(&entry);
    vtkf.resize(0x80 + 64 * 0x24, 0);
    let mut inputs = empty_inputs();
    inputs.version = crate::aacs::mkb::AACS_MAJOR_BD;
    inputs.unit_key_ro = vtkf;
    let ctx = DiscInputsCtx::new(&inputs);
    assert_eq!(ctx.enc_title_keys().unwrap(), &[key]);
}

// A small extent's probe windows overlap: each aligned unit must still be
// sampled at most once (duplicates dilute the request's evidence).
#[test]
fn read_encrypted_units_never_returns_the_same_unit_twice() {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS};
    struct Stamped;
    impl crate::sector::SectorSource for Stamped {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _r: bool,
        ) -> crate::error::Result<usize> {
            let bytes = count as usize * 2048;
            for (i, s) in buf[..bytes].chunks_mut(2048).enumerate() {
                s.fill(0xC0);
                s[8..12].copy_from_slice(&(lba + i as u32).to_be_bytes());
            }
            Ok(bytes)
        }
    }
    let units = 30u32;
    let title = title_with_extents(vec![crate::disc::Extent {
        start_lba: 900,
        sector_count: units * ALIGNED_UNIT_SECTORS,
    }]);
    let got = read_encrypted_units(&mut Stamped, &title, 1000);
    let mut lbas: Vec<u32> = got
        .iter()
        .map(|u| {
            assert_eq!(u.len(), ALIGNED_UNIT_LEN);
            u32::from_be_bytes([u[8], u[9], u[10], u[11]])
        })
        .collect();
    let n = lbas.len();
    lbas.sort_unstable();
    lbas.dedup();
    assert_eq!(lbas.len(), n, "no unit may be sampled twice");
    assert!(n as u32 <= units);
}

fn sampling_extent(sectors: u32) -> Vec<crate::disc::Extent> {
    vec![crate::disc::Extent {
        start_lba: 500,
        sector_count: sectors,
    }]
}

/// A gone source ends sampling at the first failed probe: later probes would only hit the
/// dead device, and the empty result must not be mistaken for a clear title by more reads.
#[test]
fn encrypted_units_in_stops_at_a_gone_source() {
    use crate::aacs::content::ALIGNED_UNIT_SECTORS;
    struct Gone(u32);
    impl crate::sector::SectorSource for Gone {
        fn read_sectors(
            &mut self,
            _l: u32,
            _c: u16,
            _b: &mut [u8],
            _r: bool,
        ) -> crate::error::Result<usize> {
            self.0 += 1;
            Err(crate::error::Error::SourceTerminated)
        }
    }
    let mut src = Gone(0);
    let ext = sampling_extent(400 * ALIGNED_UNIT_SECTORS);
    let out = encrypted_units_in(&mut src, &ext, crate::disc::ContentFormat::BdTs, 8);
    assert!(out.is_empty());
    assert_eq!(src.0, 1, "no probe after the source is gone");
}

/// An HD DVD title is classified by the PS scrambling bits, not the BD CPI byte.
#[test]
fn encrypted_units_in_uses_the_title_format() {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS};
    struct Fill;
    impl crate::sector::SectorSource for Fill {
        fn read_sectors(
            &mut self,
            _l: u32,
            c: u16,
            b: &mut [u8],
            _r: bool,
        ) -> crate::error::Result<usize> {
            let n = c as usize * 2048;
            for unit in b[..n].chunks_mut(ALIGNED_UNIT_LEN) {
                unit.fill(0);
                unit[0] = 0xC0; // BD CPI set, PS bits clear
            }
            Ok(n)
        }
    }
    let ext = sampling_extent(400 * ALIGNED_UNIT_SECTORS);
    let ps = encrypted_units_in(&mut Fill, &ext, crate::disc::ContentFormat::MpegPs, 8);
    assert!(ps.is_empty(), "a BD CPI byte is not a PS scramble flag");
    let ts = encrypted_units_in(&mut Fill, &ext, crate::disc::ContentFormat::BdTs, 8);
    assert_eq!(ts.len(), 8);
}

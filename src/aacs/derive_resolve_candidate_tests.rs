use super::*;
use crate::aacs::crypto::aes_ecb_encrypt;

// A Class II MKB is reported as an unsupported class (typed, E7109), not as "no key
// matched": derivation stops before reading its tables. A pre-recorded MKB passes.
#[test]
fn a_class_ii_mkb_is_an_explicit_unsupported_class() {
    let mkb = |t: u32| {
        let mut m = vec![0x10, 0x00, 0x00, 0x0C];
        m.extend_from_slice(&t.to_be_bytes());
        m.extend_from_slice(&[0, 0, 0, 1]);
        m
    };
    let class2 = mkb(MKB_TYPE_10_CLASS_II);
    let err = check_mkb_class(&class2).expect_err("class II is unsupported");
    assert_eq!(err, MkbClassError::Unsupported(MkbType::ClassII));
    assert_eq!(err.code(), crate::error::E_MKB_CLASS_UNSUPPORTED);
    assert!(MkbTables::parse(&class2).is_none());
    assert_eq!(derive_media_key_from_pk(&class2, &[[0u8; 16]]), None);
    assert_eq!(check_mkb_class(&mkb(MKB_TYPE_4_PRERECORDED)), Ok(()));
    assert_eq!(check_mkb_class(&mkb(MKB_20_CATEGORY_C)), Ok(()));
    assert_eq!(check_mkb_class(&[]), Ok(()));
}

// km_verifies gates every candidate Media Key; mutation testing found `-> true` surviving
// all 2,556 tests, i.e. unverified verification.
#[test]
fn km_verifies_accepts_only_the_key_its_record_was_built_for() {
    use crate::aacs::mkb::mkb_find_mk_dv;

    let km: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
        0xFF,
    ];

    let mut plain = [0u8; 16];
    plain[..8].copy_from_slice(&VERIFY_MAGIC);
    plain[8..].copy_from_slice(&[0xA5; 8]);
    let mk_dv = aes_ecb_encrypt(&km, &plain);

    let mut mkb = vec![
        0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    ];
    mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x18]);
    mkb.extend_from_slice(&mk_dv);
    mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    assert_eq!(
        mkb_find_mk_dv(&mkb),
        Some(mk_dv),
        "fixture malformed — the verify record is not being found at all"
    );

    assert!(
        probe::km_verifies(&mkb, &km),
        "the key the record was built for must verify"
    );

    // The half that kills the `-> true` mutant. One flipped bit is the
    // strongest form of wrong key: a near-miss, not a random one.
    let mut wrong = km;
    wrong[15] ^= 0x01;
    assert!(
        !probe::km_verifies(&mkb, &wrong),
        "a key differing by ONE BIT must not verify; if it did, the MK-pool \
             brute force would accept whichever candidate it happened to try first"
    );

    // No verify record means UNVERIFIABLE, which is not the same as verified.
    let bare = vec![0x10, 0x00, 0x00, 0x0C, 0, 0, 0, 0, 0, 0, 0, 1];
    assert!(
        !probe::km_verifies(&bare, &km),
        "an MKB with no verify record must not default to yes"
    );
}

/// `ResolvedChain.unit_keys` holds raw title-key bytes (the other rungs are
/// self-redacting `types` newtypes). `Debug` must not leak the title keys.
#[test]
fn resolved_chain_debug_is_redacted() {
    let c = ResolvedChain {
        unit_keys: vec![(1, [0xD5; 16])],
        vuk: None,
        mk: None,
        pk: None,
        dk: None,
    };
    let dbg = format!("{c:?}");
    assert!(
        !dbg.contains("213"),
        "ResolvedChain leaked unit keys: {dbg}"
    );
    assert!(
        dbg.contains("unit_keys_len"),
        "ResolvedChain missing redaction: {dbg}"
    );
}

/// Minimal AACS-1.0 (48-byte stride) `Unit_Key_RO.inf` with `n` encrypted
/// unit keys — `parse_unit_key_ro` numbers CPS units 1..=n.
fn synth_inf(encs: &[[u8; 16]]) -> Vec<u8> {
    let uk_pos = 32usize;
    let stride = 48usize;
    let n = encs.len();
    let total = uk_pos + 48 + n.saturating_sub(1) * stride + 16;
    let mut inf = vec![0u8; total.max(20)];
    inf[..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    inf[uk_pos..uk_pos + 2].copy_from_slice(&(n as u16).to_be_bytes());
    for (i, k) in encs.iter().enumerate() {
        let o = uk_pos + 48 + i * stride;
        inf[o..o + 16].copy_from_slice(k);
    }
    inf
}

/// A VUK candidate boils to ALL the disc's unit keys, each paired with its
/// declared CPS-unit number, and each key equals the VUK-decrypt of its slot.
#[test]
fn resolve_candidate_vuk_returns_all_cps_units() {
    let vuk = Vuk([0x33u8; 16]);
    let encs = [[0x11u8; 16], [0x22u8; 16], [0x44u8; 16]];
    let inf = synth_inf(&encs);
    let r = resolve_candidate(&KeyCandidate::Vuk(vuk), &[], &inf, None, AacsVersion::V10)
        .expect("vuk derives");
    let cps: Vec<u32> = r.unit_keys.iter().map(|(c, _)| *c).collect();
    assert_eq!(
        cps,
        vec![1, 2, 3],
        "every CPS unit surfaced, numbered from the inf"
    );
    for ((_, key), enc) in r.unit_keys.iter().zip(encs.iter()) {
        assert_eq!(
            *key,
            decrypt_unit_key(&vuk.0, enc),
            "key = VUK-decrypt of its slot"
        );
    }
    assert_eq!(r.vuk, Some(vuk));
    assert!(r.mk.is_none() && r.pk.is_none() && r.dk.is_none());
}

/// A bare UK candidate is terminal — it returns itself keyed by its own idx.
#[test]
fn resolve_candidate_uk_is_itself() {
    let uk = UnitKey::new(2, [0x9u8; 16]);
    let r = resolve_candidate(&KeyCandidate::Uk(uk), &[], &[], None, AacsVersion::V10)
        .expect("uk is terminal");
    assert_eq!(r.unit_keys, vec![(2, uk.key)]);
    assert!(r.vuk.is_none() && r.mk.is_none());
}

/// A UK candidate's positional idx is reported as the declared CPS-unit number.
#[test]
fn resolve_candidate_uk_reports_the_declared_cps_unit_number() {
    let inf = synth_inf(&[[0x11u8; 16], [0x22u8; 16]]);
    let uk = UnitKey::new(1, [0x9u8; 16]);
    let r = resolve_candidate(&KeyCandidate::Uk(uk), &[], &inf, None, AacsVersion::V10)
        .expect("terminal");
    assert_eq!(r.unit_keys, vec![(2, uk.key)]);
}

/// The declared number comes from the file's own numbering (an HD DVD VTKF with a gap), and
/// falls back to the position when the file does not parse or lacks that slot.
#[test]
fn resolve_candidate_uk_declared_number_follows_the_file_with_positional_fallback() {
    let mut vtkf = vec![0u8; 0x80 + 64 * 0x24];
    vtkf[..12].copy_from_slice(b"DVD_HD_V_TKF");
    // Slots 2 and 5 (1-based) are present; slot 1 is a gap.
    for slot in [1usize, 4] {
        vtkf[0x80 + slot * 0x24] = 0x80;
    }
    let at = |idx: u32| {
        let uk = UnitKey::new(idx, [0x9u8; 16]);
        resolve_candidate(&KeyCandidate::Uk(uk), &[], &vtkf, None, AacsVersion::V10)
            .expect("terminal")
            .unit_keys[0]
            .0
    };
    assert_eq!(at(0), 2);
    assert_eq!(at(1), 5, "the file's number, not idx + 1");
    assert_eq!(at(7), 7, "no such slot: the position");
    let uk = UnitKey::new(3, [0x9u8; 16]);
    let r = resolve_candidate(
        &KeyCandidate::Uk(uk),
        &[],
        &[0u8; 4],
        None,
        AacsVersion::V10,
    )
    .unwrap();
    assert_eq!(r.unit_keys[0].0, 3, "unparseable file: the position");
}

/// MK/PK/DK paths derive the VUK from a VID; without one, derivation stops.
#[test]
fn resolve_candidate_mk_requires_vid() {
    let r = resolve_candidate(
        &KeyCandidate::Mk(MediaKey([1u8; 16])),
        &[],
        &[],
        None,
        AacsVersion::V10,
    );
    assert!(r.is_none(), "MK path returns None without a VID");
}

/// A planted Processing Key resolves against a synthetic MKB and drives the
/// FULL chain PK → MK → VUK → UK — proving a PK candidate yields real keys.
#[test]
fn resolve_candidate_pk_drives_full_chain() {
    let pk: [u8; 16] = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        0x00,
    ];
    let mk: [u8; 16] = [
        0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE,
        0xAF,
    ];
    let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

    let mut mk_raw = mk;
    for a in 0..4 {
        mk_raw[12 + a] ^= uv[a];
    }
    let cv = aes_ecb_encrypt(&pk, &mk_raw);

    let mut vd = [0x11u8; 16];
    vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    // 4-byte record header (type + BE24 total length) + body.
    let rec = |t: u8, body: &[u8]| -> Vec<u8> {
        let total = 4 + body.len();
        let mut r = vec![
            t,
            ((total >> 16) & 0xFF) as u8,
            ((total >> 8) & 0xFF) as u8,
            (total & 0xFF) as u8,
        ];
        r.extend_from_slice(body);
        r
    };
    let mut sd = vec![0u8];
    sd.extend_from_slice(&uv);
    let mut mkb = Vec::new();
    mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
    mkb.extend_from_slice(&rec(0x86, &mk_dv));
    mkb.extend_from_slice(&rec(0x04, &sd));
    mkb.extend_from_slice(&rec(0x05, &cv));

    let vid = Vid([0x42u8; 16]);
    let plain_uk = [0x7Eu8; 16];
    let vuk = derive_vuk(&mk, &vid.0);
    let enc = aes_ecb_encrypt(&vuk, &plain_uk);
    let inf = synth_inf(std::slice::from_ref(&enc));

    let r = resolve_candidate(
        &KeyCandidate::Pk(ProcessingKey(pk)),
        &mkb,
        &inf,
        Some(vid),
        AacsVersion::V10,
    )
    .expect("planted PK resolves the full chain");
    assert_eq!(r.mk, Some(MediaKey(mk)), "PK recovers the planted MK");
    assert_eq!(r.unit_keys.len(), 1);
    assert_eq!(
        r.unit_keys[0].1, plain_uk,
        "PK chain recovers the title key"
    );
}

// VUK relation `Kvu = AES-128D(Km, IDv) XOR IDv` ([PR]/[BD] §3.3), anchored
// to the FIPS-197 AES-128 known-answer vector so the test pins BOTH the AES
// primitive and the trailing XOR (a dropped XOR or encrypt-swap changes it).
#[test]
fn derive_vuk_matches_the_fips197_aes_decrypt_xor_relation() {
    use crate::aacs::crypto::aes_ecb_decrypt;

    // FIPS-197 Appendix example: AES-128D(key, ciphertext) == plaintext.
    let key: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    let ciphertext: [u8; 16] = [
        0x69, 0xc4, 0xe0, 0xd8, 0x6a, 0x7b, 0x04, 0x30, 0xd8, 0xcd, 0xb7, 0x80, 0x70, 0xb4, 0xc5,
        0x5a,
    ];
    let plaintext: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    // The known AES-128 decrypt result — pins the primitive derive_vuk builds on.
    assert_eq!(
        aes_ecb_decrypt(&key, &ciphertext),
        plaintext,
        "FIPS-197 AES-128 decrypt vector"
    );

    // Treating `key` as the Media Key and `ciphertext` as the Volume ID, the
    // VUK is the known plaintext XOR the Volume ID.
    let mut expected = plaintext;
    for i in 0..16 {
        expected[i] ^= ciphertext[i];
    }
    assert_eq!(
        derive_vuk(&key, &ciphertext),
        expected,
        "VUK = AES-128D(Km, IDv) XOR IDv against a known AES vector"
    );

    // And it is genuinely XOR: a Volume ID sharing set bits with the decrypt
    // must CLEAR them, which an OR could never do.
    let mk = [0xFFu8; 16];
    let vid = [0xABu8; 16];
    let mut want = aes_ecb_decrypt(&mk, &vid);
    for i in 0..16 {
        want[i] ^= vid[i];
    }
    assert_eq!(derive_vuk(&mk, &vid), want, "XOR, not OR, of IDv");
}

/// A single-CPS `Unit_Key_RO.inf` in the real on-disc shape (Bruges/Titanic:
/// one encrypted title key at offset 0x50) parses to exactly one key numbered
/// CPS unit 1, and `derive_unit_keys` unwraps it with the VUK.
#[test]
fn single_cps_inf_parses_one_key_at_offset_0x50_and_boils_it() {
    // uk_pos = 0x20; first (only) key at uk_pos + 48 = 0x50.
    let mut inf = vec![0u8; 0x50 + 16];
    inf[0..4].copy_from_slice(&0x20u32.to_be_bytes());
    inf[0x20..0x22].copy_from_slice(&1u16.to_be_bytes()); // num_unit_keys = 1
    let enc = [0x9Au8; 16];
    inf[0x50..0x60].copy_from_slice(&enc);

    let ukf = parse_unit_key_ro(&inf, AacsVersion::V10).expect("valid single-CPS inf");
    assert_eq!(
        ukf.encrypted_keys,
        vec![(1u32, enc)],
        "one CPS unit, numbered 1, read from offset 0x50"
    );

    let vuk = [0x5Cu8; 16];
    let boiled = derive_unit_keys(&ukf, &vuk);
    assert_eq!(
        boiled,
        vec![(1u32, decrypt_unit_key(&vuk, &enc))],
        "the single unit key is the VUK-decrypt of its slot, keyed by CPS 1"
    );
}

/// Build an `n`-key `Unit_Key_RO.inf` at a chosen stride, each slot filled
/// with a distinct byte so a mis-strided read is visible.
fn multi_cps_inf(n: usize, stride: usize) -> (Vec<u8>, Vec<[u8; 16]>) {
    let uk_pos = 0x20usize;
    let key0 = uk_pos + 48;
    let total = key0 + (n.saturating_sub(1)) * stride + 16;
    let mut inf = vec![0u8; total];
    inf[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    inf[uk_pos..uk_pos + 2].copy_from_slice(&(n as u16).to_be_bytes());
    let mut encs = Vec::new();
    for i in 0..n {
        let k = [(0x10 + i as u8); 16];
        let o = key0 + i * stride;
        inf[o..o + 16].copy_from_slice(&k);
        encs.push(k);
    }
    (inf, encs)
}

/// A MULTI-CPS inf yields one key per CPS unit, numbered 1..=n in on-disc
/// order, and each boils to the VUK-decrypt of its own slot — the map cannot
/// drift or renumber.
#[test]
fn multi_cps_inf_numbers_each_unit_and_maps_to_its_slot() {
    let (inf, encs) = multi_cps_inf(4, 48); // V10 stride
    let ukf = parse_unit_key_ro(&inf, AacsVersion::V10).expect("valid multi-CPS inf");
    assert_eq!(ukf.encrypted_keys.len(), 4, "one key per CPS unit");
    let cps: Vec<u32> = ukf.encrypted_keys.iter().map(|(c, _)| *c).collect();
    assert_eq!(cps, vec![1, 2, 3, 4], "CPS units numbered 1..=n in order");

    let vuk = [0x33u8; 16];
    let boiled = derive_unit_keys(&ukf, &vuk);
    assert_eq!(boiled.len(), 4, "every CPS unit boils to a key");
    for (i, (num, key)) in boiled.iter().enumerate() {
        assert_eq!(*num, (i + 1) as u32, "CPS number is the 1-based slot index");
        assert_eq!(
            *key,
            decrypt_unit_key(&vuk, &encs[i]),
            "each unit key is the VUK-decrypt of its own encrypted slot"
        );
    }
}

/// The stride `parse_unit_key_ro` walks is driven by the AACS generation:
/// V10 reads 48-byte spacing, V20/V21 read 64-byte spacing. Same buffer, two
/// versions, DIFFERENT second key — the V10-vs-2.x distinction the on-disc
/// layout hinges on.
#[test]
fn parse_unit_key_ro_stride_follows_the_aacs_version() {
    // Two keys spaced at the V20 (64-byte) stride. Key 0 is shared (both
    // strides read +48); key 1 sits at +64, which a V10 parse (+48) misses.
    let uk_pos = 0x20usize;
    let key0 = uk_pos + 48;
    let v10_key1 = key0 + 48;
    let v20_key1 = key0 + 64;
    let mut inf = vec![0u8; v20_key1 + 16];
    inf[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    inf[uk_pos..uk_pos + 2].copy_from_slice(&2u16.to_be_bytes());
    inf[key0..key0 + 16].fill(0xA0);
    inf[v10_key1..v10_key1 + 16].fill(0x10);
    inf[v20_key1..v20_key1 + 16].fill(0x20);

    let v10 = parse_unit_key_ro(&inf, AacsVersion::V10).expect("v10");
    let v20 = parse_unit_key_ro(&inf, AacsVersion::V20).expect("v20");
    let v21 = parse_unit_key_ro(&inf, AacsVersion::V21).expect("v21");

    assert_eq!(v10.encrypted_keys[0].1, [0xA0; 16], "key 0 shared");
    assert_eq!(v20.encrypted_keys[0].1, [0xA0; 16]);
    assert_eq!(v10.encrypted_keys[1].1, [0x10; 16], "V10 reads +48");
    assert_eq!(v20.encrypted_keys[1].1, [0x20; 16], "V20 reads +64");
    assert_eq!(
        v21.encrypted_keys[1].1, [0x20; 16],
        "V21 shares the V20 64-byte stride"
    );
    assert_ne!(v10.encrypted_keys[1].1, v20.encrypted_keys[1].1);
}

/// `resolve_candidate` parses the inf at the version it is given (from the shared
/// `resolve_aacs_version`), not at its own guess from the MKB: an unreadable MKB on a
/// UHD still reads the second key at +64, and a V10 cert outranks a 2.0 MKB.
#[test]
fn resolve_candidate_boils_at_the_resolved_version_stride() {
    // A two-key inf whose 2nd key differs by stride (see the parse test).
    let uk_pos = 0x20usize;
    let key0 = uk_pos + 48;
    let v10_key1 = key0 + 48;
    let v20_key1 = key0 + 64;
    let mut inf = vec![0u8; v20_key1 + 16];
    inf[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    inf[uk_pos..uk_pos + 2].copy_from_slice(&2u16.to_be_bytes());
    inf[key0..key0 + 16].fill(0xA0);
    inf[v10_key1..v10_key1 + 16].fill(0x10);
    inf[v20_key1..v20_key1 + 16].fill(0x20);

    let vuk = Vuk([0x44u8; 16]);

    // A minimal Category-C 2.0 Type-and-Version record → generation V20.
    let v20_mkb: [u8; 12] = [
        0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
    ];
    assert_eq!(
        mkb_type(&v20_mkb).map(|t| t.generation()),
        Some(AacsVersion::V20),
        "fixture check: this MKB declares AACS 2.0"
    );
    let boil = |mkb: &[u8], version| {
        resolve_candidate(&KeyCandidate::Vuk(vuk), mkb, &inf, None, version)
            .expect("boils the inf")
            .unit_keys[1]
            .1
    };
    let at64 = decrypt_unit_key(&vuk.0, &[0x20; 16]);
    let at48 = decrypt_unit_key(&vuk.0, &[0x10; 16]);
    // The stride follows the version handed in, never a re-read of the MKB: a UHD whose
    // MKB is unreadable (empty) still reads key 2 at +64 ...
    let uhd = resolve_aacs_version(None, &[], Some(true));
    assert_eq!(boil(&[], uhd), at64);
    // ... and when the certificate (V10) disagrees with the MKB (2.0), the cert wins.
    let bd = resolve_aacs_version(Some(1), &v20_mkb, Some(true));
    assert_eq!(bd, AacsVersion::V10);
    assert_eq!(boil(&v20_mkb, bd), at48);
    assert_eq!(
        boil(&v20_mkb, resolve_aacs_version(None, &v20_mkb, None)),
        at64
    );
}

/// PIN: a `Mk` candidate is PURE DERIVATION — `resolve_candidate` does NOT
/// validate the Media Key against the MKB. A WRONG MK (one the MKB's verify
/// record would reject) still boils a full set of unit keys, from the VUK
/// that wrong MK produces. This documents the "computed-but-wrong-UK" case:
/// the caller's sample/`is_clean_ts` step, not this function, is the gate.
#[test]
fn resolve_candidate_mk_does_not_validate_the_media_key_against_the_mkb() {
    // An MKB carrying a 0x86 verify record built for a DIFFERENT media key,
    // so `km_verifies` would reject the candidate below.
    let real_km = [0xC3u8; 16];
    let mut vd = [0x11u8; 16];
    vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
    let mk_dv = aes_ecb_encrypt(&real_km, &vd);
    let mut mkb = vec![
        0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
    ];
    mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
    mkb.extend_from_slice(&mk_dv);
    // The MK we hand in is NOT the one the verify record was built for.
    let wrong_mk = MediaKey([0x77u8; 16]);
    assert!(
        !probe::km_verifies(&mkb, &wrong_mk.0),
        "fixture check: the wrong MK must fail the MKB verify record"
    );

    let vid = Vid([0x42u8; 16]);
    let enc = [0x9Au8; 16];
    let inf = synth_inf(std::slice::from_ref(&enc));

    let r = resolve_candidate(
        &KeyCandidate::Mk(wrong_mk),
        &mkb,
        &inf,
        Some(vid),
        AacsVersion::V10,
    )
    .expect("resolve_candidate boils regardless of MK-vs-MKB validity");
    // The chain is derived straight from the (wrong) MK, no gate applied.
    let expected_vuk = derive_vuk(&wrong_mk.0, &vid.0);
    assert_eq!(
        r.vuk,
        Some(Vuk(expected_vuk)),
        "VUK is derived from the given MK"
    );
    assert_eq!(
        r.mk,
        Some(wrong_mk),
        "the candidate MK is carried through unchecked"
    );
    assert_eq!(
        r.unit_keys,
        vec![(1u32, decrypt_unit_key(&expected_vuk, &enc))],
        "unit keys are boiled from the wrong VUK — derivation does not validate"
    );
}

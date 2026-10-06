use super::*;
use crate::aacs::crypto::aes_ecb_encrypt;

/// An MKB record: 1-byte type + BE24 total length (header included) + body.
fn rec(t: u8, body: &[u8]) -> Vec<u8> {
    let total = 4 + body.len();
    let mut r = vec![
        t,
        ((total >> 16) & 0xFF) as u8,
        ((total >> 8) & 0xFF) as u8,
        (total & 0xFF) as u8,
    ];
    r.extend_from_slice(body);
    r
}

/// The planted fixture: an MKB whose single subset-difference slot is opened
/// by `dkey` sitting EXACTLY at that slot (zero descent), yielding `mk`.
struct Planted {
    mkb: Vec<u8>,
    dkey: [u8; 16],
    mk: [u8; 16],
    mk_dv: [u8; 16],
    cv: [u8; 16],
    uv: u32,
    u_mask_shift: u8,
}

// Build the fixture by inverting the AACS relations. uv/u_mask_shift are chosen so a gating
// device node exists; uv stays under 0x10000 since DeviceKey::node is u16.
fn plant_mkb() -> Planted {
    let dkey: [u8; 16] = [
        0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1,
        0xF0,
    ];
    let mk: [u8; 16] = [
        0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE,
        0xAF,
    ];
    let uv: u32 = 0x0000_0400;
    let u_mask_shift: u8 = 12;

    // The Processing Key a device sitting AT the slot produces: [C] §3.2.4
    // makes it the AES-G3(.,1) of its own node, with no descent.
    let pk = aesg3(&dkey, 1);

    // Invert [C] §3.2.4: mk = AES-D(pk, cvalue) then XOR uv into mk[12..16].
    let mut mk_raw = mk;
    for (a, b) in mk_raw[12..16].iter_mut().zip(uv.to_be_bytes()) {
        *a ^= b;
    }
    let cv = aes_ecb_encrypt(&pk, &mk_raw);

    // Invert [C] §3.2.5.1.4: AES-D(mk, mk_dv) must start with the magic.
    let mut vd = [0x5Au8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    let mut subdiff = vec![u_mask_shift];
    subdiff.extend_from_slice(&uv.to_be_bytes());

    let mut mkb = Vec::new();
    mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
    mkb.extend_from_slice(&rec(0x86, &mk_dv));
    mkb.extend_from_slice(&rec(0x04, &subdiff));
    mkb.extend_from_slice(&rec(0x05, &cv));

    Planted {
        mkb,
        dkey,
        mk,
        mk_dv,
        cv,
        uv,
        u_mask_shift,
    }
}

// The Class II gate is what refuses the derivation: the planted MKB derives its media key
// as a pre-recorded block, and the same records under the Class II type derive nothing.
#[test]
fn a_class_ii_type_refuses_an_otherwise_derivable_mkb() {
    let p = plant_mkb();
    let pk = aesg3(&p.dkey, 1);
    assert_eq!(
        derive_media_key_from_pk(&p.mkb, &[pk]),
        Some(p.mk),
        "control"
    );
    let mut class2 = p.mkb.clone();
    class2[4..8].copy_from_slice(&MKB_TYPE_10_CLASS_II.to_be_bytes());
    assert_eq!(mkb_type(&class2), Some(MkbType::ClassII));
    assert!(MkbTables::parse(&class2).is_none());
    assert_eq!(derive_media_key_from_pk(&class2, &[pk]), None);
}

/// Sanity-check the fixture itself before anything is asserted about the
/// functions under test: an MKB the parser cannot read would make every
/// "returns None" body look correct.
#[test]
fn the_planted_mkb_is_a_parseable_mkb() {
    let p = plant_mkb();
    assert_eq!(mkb_find_mk_dv(&p.mkb), Some(p.mk_dv), "verify record");
    assert_eq!(
        mkb_find_cvalues(&p.mkb).as_deref(),
        Some(&p.cv[..]),
        "cvalue record"
    );
    assert_eq!(
        mkb_find_subdiff_records(&p.mkb).map(|v| v.len()),
        Some(5),
        "one 5-byte subset-difference slot"
    );
}

// `None` here means "key does not apply", indistinguishable from a position
// that was simply never found — the feature silently stops working. The
// load-bearing assertion is that the recovered position walks to the planted MK.
#[test]
fn recover_dk_position_finds_a_position_that_derives_the_planted_media_key() {
    let p = plant_mkb();

    let recovered =
        recover_dk_position(&p.mkb, &p.dkey).expect("the planted key applies to this MKB");

    assert_eq!(
        recovered.uv, p.uv,
        "uv is invariant for the key across discs and must be the slot's"
    );
    assert_eq!(
        recovered.u_mask_shift, p.u_mask_shift,
        "u_mask_shift must be the slot's"
    );
    assert_eq!(recovered.key, p.dkey, "the key bytes are carried through");

    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
        Some(p.mk),
        "the recovered position must walk the MKB to the planted Media Key \
             — a position that does not is no better than None"
    );
}

// The other direction: a key the MKB does NOT open must not be given a
// position — wrongly banking one would derive a wrong Media Key on every
// future disc.
#[test]
fn recover_dk_position_rejects_a_key_the_mkb_does_not_open() {
    let p = plant_mkb();
    let mut stranger = p.dkey;
    stranger[0] ^= 0x01; // one bit off — the strongest form of wrong key
    assert!(
        recover_dk_position(&p.mkb, &stranger).is_none(),
        "a key differing by one bit must not be handed a position"
    );
}

// `None` here strands a key whose position was already recovered — the
// last step of position recovery, failing the same way: usable key, discarded.
// Asserted through the derived Media Key, not the node value, since any gating node works.
#[test]
fn resolve_dk_node_returns_a_node_that_passes_the_walk_gate() {
    let p = plant_mkb();

    let dk = resolve_dk_node(&p.mkb, &p.dkey, p.uv, p.u_mask_shift)
        .expect("a gating node exists for the planted slot");

    assert_eq!(dk.uv, p.uv);
    assert_eq!(dk.u_mask_shift, p.u_mask_shift);
    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&dk)),
        Some(p.mk),
        "the resolved node must actually pass the gate and derive the \
             planted Media Key"
    );

    // The gate is the point: the node must differ from uv inside v_mask.
    // (v_mask for uv=0x400 is 0xFFFF_F800.)
    let v_mask = calc_v_mask(p.uv);
    assert_ne!(
        (dk.node as u32) & v_mask,
        p.uv & v_mask,
        "a node equal to uv under v_mask does not gate — the walk would \
             skip the slot entirely"
    );
}

// Parsed tables where no node gates (key not in this MKB's slot): resolving must
// fail rather than return a key that can never derive.
#[test]
fn resolve_dk_node_rejects_an_unwalkable_position() {
    let p = plant_mkb();
    assert!(
        resolve_dk_node(&p.mkb, &[0x11; 16], p.uv, p.u_mask_shift).is_none(),
        "no node gates for a key the MKB does not open"
    );
}

// `u_mask_shift` is a disc/keydb-controlled u8; a value >= 32 would drive
// `1u32 << b` past a u32's width and panic (debug) / wrap (release). The `.min(32)`
// guard must bound the search so a hostile disc can never trip the shift.
#[test]
fn resolve_dk_node_does_not_shift_past_u32_on_a_huge_u_mask_shift() {
    // Empty MKB → the inner derive returns None every iteration, so the loop runs
    // its full range; with u_mask_shift = 200 the unguarded loop panics at b = 32.
    let dk = resolve_dk_node(&[], &[0u8; 16], 0x0001_2345, 200)
        .expect("must fall back to the node itself, not panic on the shift");
    assert_eq!(
        dk.node, 0x2345,
        "no gating bit → the fallback is uv's own node"
    );
}

// probe::mkb_mk_dv feeds km_verifies for reproduction harnesses; a fixed
// block would "verify" against a record no disc carries, and always-None
// would make every verification report "unverifiable".
#[test]
fn probe_mkb_mk_dv_returns_the_records_actual_bytes() {
    let p = plant_mkb();
    assert_eq!(
        probe::mkb_mk_dv(&p.mkb),
        Some(p.mk_dv),
        "mk_dv must be the bytes the 0x86 record carries"
    );
    assert_ne!(
        probe::mkb_mk_dv(&p.mkb),
        Some([0u8; 16]),
        "and not a constant block"
    );
    assert_eq!(
        probe::mkb_mk_dv(&[0x10, 0x00, 0x00, 0x04]),
        None,
        "an MKB with no verify record has no mk_dv"
    );
}

// A MULTI-SLOT MKB (unlike plant_mkb's zero-descent fixture) pinning slot
// INDEXING and the DESCENT branch. v-masks below are literals from `[C]`
// §3.2.3, not computed with calc_v_mask, which is itself under test.
const UV_SLOT: u32 = 0x0000_9400; // lowest set bit 10
const V_MASK_SLOT: u32 = 0xFFFF_F800; // 0xFFFF_FFFF << 11
const UV_ANCESTOR: u32 = 0x0000_9800; // lowest set bit 11
const V_MASK_ANCESTOR: u32 = 0xFFFF_F000; // 0xFFFF_FFFF << 12
const U_MASK_SHIFT: u8 = 16;

// calc_v_mask implements `[C]` §3.2.3; every gate and descent is masked by
// its result, so a wrong mask matches the wrong slots — pinned against
// literal expectations, not a re-computation.
#[test]
fn calc_v_mask_is_all_ones_above_the_lowest_set_bit() {
    // (uv, expected v_mask) — expected = 0xFFFF_FFFF << (trailing_zeros+1).
    let cases: &[(u32, u32)] = &[
        (0x0000_0001, 0xFFFF_FFFE),
        (0x0000_0002, 0xFFFF_FFFC),
        (0x0000_0400, 0xFFFF_F800),
        (UV_SLOT, V_MASK_SLOT),
        (UV_ANCESTOR, V_MASK_ANCESTOR),
        (0x0000_00FF, 0xFFFF_FFFE), // lowest set bit is 0
    ];
    for &(uv, expected) in cases {
        assert_eq!(
            calc_v_mask(uv),
            expected,
            "v_mask for uv={uv:#010x} must be all-ones above its lowest set bit"
        );
    }
}

/// The planted multi-slot fixture.
struct PlantedDescent {
    mkb: Vec<u8>,
    dkey: [u8; 16],
    mk: [u8; 16],
}

// Build an MKB with THREE subset-difference slots where only slot 2 is
// keyed, and the device key sits one level ABOVE it (at UV_ANCESTOR). The
// two decoy slots carry real-looking uvs so wrong indexing validates nothing.
fn plant_descent_mkb() -> PlantedDescent {
    let dkey: [u8; 16] = [
        0x5A, 0x4B, 0x3C, 0x2D, 0x1E, 0x0F, 0xF0, 0xE1, 0xD2, 0xC3, 0xB4, 0xA5, 0x96, 0x87, 0x78,
        0x69,
    ];
    let mk: [u8; 16] = [
        0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE,
        0xBF,
    ];

    // Processing Key from descending UV_ANCESTOR -> slot, via the same descent the
    // walk uses but anchored to the FIXED ancestor above — so a walk computing a
    // different candidate position derives a different Kp and fails.
    let pk = calc_pk_from_dk(&dkey, UV_SLOT, V_MASK_SLOT, V_MASK_ANCESTOR);

    // Invert [C] §3.2.4 for slot 2's cvalue.
    let mut mk_raw = mk;
    for (a, b) in mk_raw[12..16].iter_mut().zip(UV_SLOT.to_be_bytes()) {
        *a ^= b;
    }
    let cv2 = aes_ecb_encrypt(&pk, &mk_raw);

    // Invert [C] §3.2.5.1.4.
    let mut vd = [0x33u8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    // Three 5-byte slots: two decoys, then the keyed one.
    let mut subdiff = Vec::new();
    for uv in [0x0000_1100u32, 0x0000_2200, UV_SLOT] {
        subdiff.push(U_MASK_SHIFT);
        subdiff.extend_from_slice(&uv.to_be_bytes());
    }
    // Three 16-byte cvalues, 1:1 with the slots; only index 2 is real.
    let mut cvalues = vec![0x11u8; 16];
    cvalues.extend_from_slice(&[0x22u8; 16]);
    cvalues.extend_from_slice(&cv2);

    let mut mkb = Vec::new();
    mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
    mkb.extend_from_slice(&rec(0x86, &mk_dv));
    mkb.extend_from_slice(&rec(0x04, &subdiff));
    mkb.extend_from_slice(&rec(0x05, &cvalues));

    PlantedDescent { mkb, dkey, mk }
}

/// Fixture sanity: three slots, three cvalues, and the keyed slot is NOT
/// index 0 (otherwise the indexing this fixture exists to pin is trivial).
#[test]
fn the_planted_descent_mkb_has_three_slots_and_is_keyed_at_the_last() {
    let p = plant_descent_mkb();
    assert_eq!(
        mkb_find_subdiff_records(&p.mkb).map(|v| v.len()),
        Some(15),
        "three 5-byte subset-difference slots"
    );
    assert_eq!(
        mkb_find_cvalues(&p.mkb).map(|v| v.len()),
        Some(48),
        "three 16-byte cvalues"
    );
    assert_ne!(UV_SLOT, UV_ANCESTOR, "the device is not at the slot");
}

// Pins two things the single-slot fixture cannot: recovered uv is the
// ancestor (proof the descent branch ran, not the zero-descent shortcut),
// and the keyed slot is index 2 (slot/cvalue table offsets both correct).
#[test]
fn recover_dk_position_finds_an_ancestor_position_in_a_multi_slot_mkb() {
    let p = plant_descent_mkb();

    let recovered = recover_dk_position(&p.mkb, &p.dkey)
        .expect("the planted key opens slot 2 from one level above it");

    assert_eq!(
        recovered.uv, UV_ANCESTOR,
        "the recovered position is the device's ancestor node, not the slot's"
    );
    assert_ne!(
        recovered.uv, UV_SLOT,
        "a zero-descent answer would mean the descent branch never ran"
    );
    assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT);
    assert_eq!(recovered.key, p.dkey);

    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
        Some(p.mk),
        "the recovered ancestor position must walk to the planted Media Key"
    );
}

/// The same multi-slot MKB must not hand a position to a key it does not
/// open — including one that differs by a single bit.
#[test]
fn recover_dk_position_rejects_a_stranger_against_the_multi_slot_mkb() {
    let p = plant_descent_mkb();
    let mut stranger = p.dkey;
    stranger[15] ^= 0x01;
    assert!(recover_dk_position(&p.mkb, &stranger).is_none());
}

// A FOUR-LEVEL descent taking both branches, pinning the per-level decision ([C]
// §3.2.4: RIGHT `aesg3(.,2)` if uv bit set, else LEFT `aesg3(.,0)`, PK `aesg3(.,1)`).
// Expected PK is an EXPLICIT `aesg3` chain, not `calc_pk_from_dk` (which would drift).

/// Slot `uv` for the four-level fixture: bits 8, 6 and 4 set. Lowest set
/// bit 4 → the descent reads bits 8, 7, 6, 5 (set, clear, set, clear).
const UV_SLOT4: u32 = 0x0000_0150;
const V_MASK_SLOT4: u32 = 0xFFFF_FFE0; // 0xFFFF_FFFF << 5
/// The device's ancestor position: lowest set bit 8, four levels above.
const UV_ANC4: u32 = 0x0000_0100;
const V_MASK_ANC4: u32 = 0xFFFF_FE00; // 0xFFFF_FFFF << 9

struct PlantedDescent4 {
    mkb: Vec<u8>,
    dkey: [u8; 16],
    mk: [u8; 16],
    /// The Processing Key the four-level descent must produce.
    pk: [u8; 16],
}

fn plant_four_level_mkb() -> PlantedDescent4 {
    let dkey: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ];
    let mk: [u8; 16] = [
        0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE,
        0xDF,
    ];

    // [C] §3.2.4 level by level: ancestor -> slot reads UV_SLOT4 bits 8,7,6,5 =
    // 1,0,1,0 -> right(2),left(0),right(2),left(0), then PK = aesg3(final_node, 1).
    let n1 = aesg3(&dkey, 2);
    let n2 = aesg3(&n1, 0);
    let n3 = aesg3(&n2, 2);
    let n4 = aesg3(&n3, 0);
    let pk = aesg3(&n4, 1);

    let mut mk_raw = mk;
    for (a, b) in mk_raw[12..16].iter_mut().zip(UV_SLOT4.to_be_bytes()) {
        *a ^= b;
    }
    let cv1 = aes_ecb_encrypt(&pk, &mk_raw);

    let mut vd = [0x77u8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    // Two slots; the keyed one is index 1.
    let mut subdiff = Vec::new();
    for uv in [0x0000_1100u32, UV_SLOT4] {
        subdiff.push(U_MASK_SHIFT);
        subdiff.extend_from_slice(&uv.to_be_bytes());
    }
    let mut cvalues = vec![0x44u8; 16];
    cvalues.extend_from_slice(&cv1);

    let mut mkb = Vec::new();
    mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
    mkb.extend_from_slice(&rec(0x86, &mk_dv));
    mkb.extend_from_slice(&rec(0x04, &subdiff));
    mkb.extend_from_slice(&rec(0x05, &cvalues));

    PlantedDescent4 { mkb, dkey, mk, pk }
}

// calc_pk_from_dk is the tree descent every device-key path runs. A wrong
// branch, level count, or terminal increment yields a PK that validates
// against nothing — the disc reports no key though the operator's DK is good.
#[test]
fn calc_pk_from_dk_walks_the_uv_bits_right_left_right_left() {
    let p = plant_four_level_mkb();
    assert_eq!(
        calc_pk_from_dk(&p.dkey, UV_SLOT4, V_MASK_SLOT4, V_MASK_ANC4),
        p.pk,
        "the four-level descent must be aesg3(.,2), (.,0), (.,2), (.,0) then (.,1)"
    );

    // Zero levels to descend (device sits AT the slot) → the terminal step
    // alone, with no descent.
    assert_eq!(
        calc_pk_from_dk(&p.dkey, UV_SLOT4, V_MASK_SLOT4, V_MASK_SLOT4),
        aesg3(&p.dkey, 1),
        "no descent needed → Kp is aesg3(dk, 1)"
    );
}

/// End-to-end through the four-level fixture: the position recovered for an
/// unpositioned key must be the ancestor four levels up, and it must walk
/// the MKB to the planted Media Key.
#[test]
fn recover_dk_position_descends_four_levels_to_the_planted_media_key() {
    let p = plant_four_level_mkb();

    let recovered = recover_dk_position(&p.mkb, &p.dkey)
        .expect("the planted key opens the slot from four levels above it");

    assert_eq!(
        recovered.uv, UV_ANC4,
        "the recovered position is four levels above the slot"
    );
    assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT);
    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
        Some(p.mk),
        "the recovered position must walk to the planted Media Key"
    );
}

// MALFORMED MKBs: a truncated cvalue table, and a revoked-marker slot. Both are
// reachable from a corrupt/crafted (disc-controlled) MKB, and in both the walk must
// decline to derive a key rather than index past the end of a record.

/// Assemble an MKB from an explicit slot list and cvalue table.
/// `slots` is `(u_mask_shift, uv)` per subset-difference entry.
fn build_mkb(slots: &[(u8, u32)], cvalues: &[u8], mk_dv: &[u8; 16]) -> Vec<u8> {
    let mut subdiff = Vec::new();
    for &(shift, uv) in slots {
        subdiff.push(shift);
        subdiff.extend_from_slice(&uv.to_be_bytes());
    }
    let mut mkb = Vec::new();
    mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
    mkb.extend_from_slice(&rec(0x86, mk_dv));
    mkb.extend_from_slice(&rec(0x04, &subdiff));
    mkb.extend_from_slice(&rec(0x05, cvalues));
    mkb
}

/// `(dkey, mk, pk, cvalue, mk_dv)` — the five 16-byte AACS keys the
/// four-level fixture plants. Named so the return type says what it is
/// rather than repeating `[u8; 16]` five times.
type FourLevelParts = ([u8; 16], [u8; 16], [u8; 16], [u8; 16], [u8; 16]);

/// The planted slot-2 material from the four-level fixture, reusable for
/// the malformed-MKB shapes below.
fn four_level_parts() -> FourLevelParts {
    let p = plant_four_level_mkb();
    let cvalues = mkb_find_cvalues(&p.mkb).expect("cvalues");
    let mut cv = [0u8; 16];
    cv.copy_from_slice(&cvalues[16..32]); // the keyed slot's cvalue
    let mk_dv = mkb_find_mk_dv(&p.mkb).expect("mk_dv");
    (p.dkey, p.mk, p.pk, cv, mk_dv)
}

// A cvalue table with FEWER entries than the subset-difference index has
// slots — a truncated 0x05 record. The slot whose cvalue is missing must be
// skipped, not read past the end: "no key, no panic".
#[test]
fn a_cvalue_table_shorter_than_the_slot_index_is_not_read_past() {
    let (dkey, _mk, _pk, cv, mk_dv) = four_level_parts();

    // Three slots; the keyed one is index 2 — but only TWO cvalues exist.
    let slots = [
        (U_MASK_SHIFT, 0x0000_1100u32),
        (U_MASK_SHIFT, 0x0000_2200u32),
        (U_MASK_SHIFT, UV_SLOT4),
    ];
    let mut cvalues = vec![0x44u8; 16];
    cvalues.extend_from_slice(&[0x55u8; 16]);
    assert_eq!(cvalues.len(), 32, "two cvalues for three slots");
    let mkb = build_mkb(&slots, &cvalues, &mk_dv);

    let dk = DeviceKey {
        key: dkey,
        node: 0x0101,
        uv: UV_ANC4,
        u_mask_shift: U_MASK_SHIFT,
    };
    assert_eq!(
        derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&dk)),
        None,
        "slot 2 has no cvalue → no Media Key, and no read past the table"
    );

    // The unpositioned-key scan walks the same tables and must also stop at
    // the last cvalue rather than at the last slot.
    assert!(
        recover_dk_position(&mkb, &dkey).is_none(),
        "the position scan must stop at the last cvalue, not the last slot"
    );

    // The bare-PK table scan likewise: a PK that matches nothing must sweep
    // every slot and return None without reading past the cvalue table.
    let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");
    assert_eq!(
        try_pk_against_tables(&[[0x00u8; 16]], &uvs, &cvalues, &mk_dv),
        None,
        "a non-matching PK sweeps all slots without over-reading"
    );

    // …and when the keyed slot IS inside the truncated table, it resolves —
    // proving the guard skips only the missing entries.
    let ok_slots = [(U_MASK_SHIFT, UV_SLOT4), (U_MASK_SHIFT, 0x0000_1100u32)];
    let mut ok_cvalues = cv.to_vec();
    ok_cvalues.extend_from_slice(&[0x55u8; 16]);
    let ok_mkb = build_mkb(&ok_slots, &ok_cvalues, &mk_dv);
    let ok_uvs = mkb_find_subdiff_records(&ok_mkb).expect("subdiff");
    assert!(
        try_pk_against_tables(&[_pk], &ok_uvs, &ok_cvalues, &mk_dv).is_some(),
        "sanity: the same PK/cvalue pair does resolve when present"
    );
}

// A keydb device key with u_mask_shift >= 32 must be skipped at the DK-side guard;
// an unguarded `0xFFFF_FFFF << 200` panics in debug on keydb-supplied input.
#[test]
fn dk_walk_skips_a_device_key_with_u_mask_shift_past_u32() {
    let (dkey, _mk, _pk, cv, mk_dv) = four_level_parts();
    let mkb = build_mkb(&[(U_MASK_SHIFT, UV_SLOT4)], &cv, &mk_dv);
    let dk = DeviceKey {
        key: dkey,
        node: 0x0101,
        uv: UV_ANC4,
        u_mask_shift: 200,
    };
    assert_eq!(derive_media_key_and_pk_from_dk(&mkb, &[dk]), None);
}

// ── L104: cvalue-loop cipher hoist — equivalence, call-count, and spec quote ──────

/// Test-only ORACLE: `try_pk_against_tables` as it read before L104 — rebuilds an
/// AES-128 schedule for `pk` on every candidate via `validate_processing_key`
/// (UNCHANGED by L104; still builds its own schedule per call, since `dk_walk` and
/// `recover_dk_position` keep calling it that way). Used only to prove the hoisted
/// loop agrees with it, never as production code.
fn try_pk_against_tables_oracle(
    processing_keys: &[[u8; 16]],
    uvs: &[u8],
    cvalues: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();
    for pk in processing_keys {
        for i in 0..num_uvs {
            if (i + 1) * 16 > cvalues.len() {
                continue;
            }
            let record_start = i * 5;
            if record_start + 5 > uvs.len() {
                continue;
            }
            let uv = &uvs[record_start + 1..record_start + 5];
            let cv = &cvalues[i * 16..(i + 1) * 16];
            if let Some(mk) = validate_processing_key(pk, cv, uv, mk_dv) {
                return Some(mk);
            }
        }
    }
    None
}

/// L104 equivalence: the hoisted-cipher loop must return byte-identical results to the
/// pre-hoist oracle — a matching PK behind decoys, a one-bit-away PK, two PKs tried in
/// order, and an empty table.
#[test]
fn try_pk_against_tables_matches_the_pre_hoist_oracle() {
    let (_dkey, mk, pk, cv, mk_dv) = four_level_parts();
    let slots = [
        (U_MASK_SHIFT, 0x0000_1100u32),
        (U_MASK_SHIFT, 0x0000_2200u32),
        (U_MASK_SHIFT, UV_SLOT4),
    ];
    let mut cvalues = vec![0x44u8; 16];
    cvalues.extend_from_slice(&[0x55u8; 16]);
    cvalues.extend_from_slice(&cv);
    let mkb = build_mkb(&slots, &cvalues, &mk_dv);
    let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");

    assert_eq!(
        try_pk_against_tables(&[pk], &uvs, &cvalues, &mk_dv),
        try_pk_against_tables_oracle(&[pk], &uvs, &cvalues, &mk_dv),
    );
    assert_eq!(
        try_pk_against_tables(&[pk], &uvs, &cvalues, &mk_dv),
        Some(mk)
    );

    let mut stranger = pk;
    stranger[0] ^= 0x01;
    assert_eq!(
        try_pk_against_tables(&[stranger], &uvs, &cvalues, &mk_dv),
        try_pk_against_tables_oracle(&[stranger], &uvs, &cvalues, &mk_dv),
    );
    assert_eq!(
        try_pk_against_tables(&[stranger], &uvs, &cvalues, &mk_dv),
        None
    );

    assert_eq!(
        try_pk_against_tables(&[stranger, pk], &uvs, &cvalues, &mk_dv),
        try_pk_against_tables_oracle(&[stranger, pk], &uvs, &cvalues, &mk_dv),
    );

    assert_eq!(
        try_pk_against_tables(&[pk], &[], &[], &mk_dv),
        try_pk_against_tables_oracle(&[pk], &[], &[], &mk_dv),
    );
}

/// L104 "red first" observable: `KEY_EXPANSIONS` (crypto.rs, test-only) counts AES-128
/// key schedules built through `new_cipher`/`new_cipher_for`. Before this fix the count
/// would read 0 here — `validate_processing_key`'s internal `Aes128::new` was never
/// routed through the counted constructor — so this exact assertion (`== pks tried`,
/// not 0 and not `pks × candidates`) only holds once the loop shares one schedule per
/// `pk` across every `(uv, cvalue)` candidate instead of rebuilding it per candidate.
#[test]
fn try_pk_against_tables_builds_one_key_schedule_per_pk_not_per_candidate() {
    let (_dkey, mk, pk, cv, mk_dv) = four_level_parts();
    // Five decoy slots plus the keyed one: several candidates per PK, so a
    // per-candidate rebuild (6) would disagree with a per-pk one (1).
    let mut slots: Vec<(u8, u32)> = (0..5u32)
        .map(|i| (U_MASK_SHIFT, 0x0000_1100u32 + (i << 8)))
        .collect();
    slots.push((U_MASK_SHIFT, UV_SLOT4));
    let mut cvalues = Vec::new();
    for i in 0..5u8 {
        cvalues.extend_from_slice(&[i; 16]);
    }
    cvalues.extend_from_slice(&cv);
    let mkb = build_mkb(&slots, &cvalues, &mk_dv);
    let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");

    let stranger = [0xFFu8; 16]; // matches nothing: forces a full 6-candidate sweep
    KEY_EXPANSIONS.with(|c| c.set(0));
    let got = try_pk_against_tables(&[stranger, pk], &uvs, &cvalues, &mk_dv);
    assert_eq!(got, Some(mk), "sanity: the real pk still resolves");
    assert_eq!(
        KEY_EXPANSIONS.with(|c| c.get()),
        2,
        "one AES-128 schedule per pk tried (2), not one per (pk, candidate) pair (up to 12)"
    );
}

/// Verbatim from `[C]` (AACS Introduction and Common Cryptographic Elements, Final Rev
/// 0.953) §3.2.4 "Calculation of Media Key", PDF p.14. Per spec; do not change this
/// relation without a spec citation proving otherwise.
const SPEC_MEDIA_KEY_FROM_CVALUE: &str = "Using that Processing Key K and the appropriate \
        16 bytes of encrypted key data C, the device calculates the 128-bit Media Key Km as \
        follows: Km = AES-128D(K, C) \u{2295} (00000000000000000000000016 || uv)";

/// The hoisted cvalue loop must still satisfy the `[C]` §3.2.4 relation above, checked by
/// building the expected Km directly from the spec formula (inverted to plant `C`),
/// independent of any production code path.
#[test]
fn try_pk_against_tables_matches_the_c_dot_3_2_4_media_key_relation() {
    let pk = [0x2Bu8; 16];
    const UV: u32 = 0x0000_0007;
    let mk: [u8; 16] = [
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0x00, 0x00, 0x00,
        0x00,
    ];
    // Invert the spec relation: C = AES-128E(K, Km ⊕ (0^96 || uv)).
    let mut pre = mk;
    for (b, u) in pre[12..16].iter_mut().zip(UV.to_be_bytes()) {
        *b ^= u;
    }
    let cv = aes_ecb_encrypt(&pk, &pre);
    let mut vd = [0x22u8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    let mut uvs = vec![0u8];
    uvs.extend_from_slice(&UV.to_be_bytes());

    assert_eq!(
        try_pk_against_tables(&[pk], &uvs, &cv, &mk_dv),
        Some(mk),
        "{SPEC_MEDIA_KEY_FROM_CVALUE}"
    );
}

// The 0xC0 revoked marker (`[C]` §3.2.5.1.5) TERMINATES the subset-difference
// table; slots after it must not be walked. Fixture puts the keyed slot
// AFTER the marker; removing it resolves the same MKB, proving the gate works.
#[test]
fn a_revoked_marker_slot_terminates_the_subset_difference_table() {
    let (dkey, mk, pk, cv, mk_dv) = four_level_parts();

    // Slot 0 = ordinary decoy, slot 1 = revoked marker, slot 2 = the keyed
    // slot (unreachable), each with its own cvalue.
    let barred = [
        (U_MASK_SHIFT, 0x0000_1100u32),
        (0xC0u8, 0x0000_2200u32),
        (U_MASK_SHIFT, UV_SLOT4),
    ];
    let mut cvalues = vec![0x44u8; 16];
    cvalues.extend_from_slice(&[0x55u8; 16]);
    cvalues.extend_from_slice(&cv);
    let mkb = build_mkb(&barred, &cvalues, &mk_dv);

    let dk = DeviceKey {
        key: dkey,
        node: 0x0101,
        uv: UV_ANC4,
        u_mask_shift: U_MASK_SHIFT,
    };
    assert_eq!(
        derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&dk)),
        None,
        "the table ends at the revoked marker; slot 2 is not in it"
    );
    assert!(
        recover_dk_position(&mkb, &dkey).is_none(),
        "the position scan must stop at the marker too"
    );
    let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");
    assert_eq!(
        try_pk_against_tables(&[pk], &uvs, &cvalues, &mk_dv),
        None,
        "the terminal-PK scan must stop at the marker too"
    );

    // Same MKB with the marker cleared → the keyed slot is in the table and
    // every one of the three paths resolves the planted Media Key.
    let open = [
        (U_MASK_SHIFT, 0x0000_1100u32),
        (U_MASK_SHIFT, 0x0000_2200u32),
        (U_MASK_SHIFT, UV_SLOT4),
    ];
    let mkb_open = build_mkb(&open, &cvalues, &mk_dv);
    assert_eq!(
        derive_media_key_from_dk(&mkb_open, std::slice::from_ref(&dk)),
        Some(mk),
        "sanity: without the marker the same slot derives the Media Key"
    );
    let uvs_open = mkb_find_subdiff_records(&mkb_open).expect("subdiff");
    assert_eq!(
        try_pk_against_tables(&[pk], &uvs_open, &cvalues, &mk_dv),
        Some(mk)
    );
}

// A device key applies only when BOTH gates hold (`[C]` §3.2.4): u-mask
// equal AND uv agrees under the device's v-mask. Wrong u_mask_shift
// describes a different tree region and must not derive a Media Key.
#[test]
fn a_device_key_with_the_wrong_u_mask_shift_does_not_apply() {
    let p = plant_four_level_mkb();

    let good = DeviceKey {
        key: p.dkey,
        node: 0x0101,
        uv: UV_ANC4,
        u_mask_shift: U_MASK_SHIFT,
    };
    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&good)),
        Some(p.mk),
        "sanity: the correctly-filed key derives the planted Media Key"
    );

    // Identical in every way except the u-mask.
    let wrong_u_mask = DeviceKey {
        u_mask_shift: U_MASK_SHIFT - 1,
        ..good.clone()
    };
    assert_eq!(
        derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&wrong_u_mask)),
        None,
        "a mismatched u-mask must fail the subset-difference gate"
    );
}

// validate_processing_key XORs uv into mk[12..16] (`[C]` §3.2.4 step 2).
// XOR, not OR: must be reversible and able to CLEAR a bit AES set — a uv
// and MK sharing set bits in those bytes is what tells the two operators apart.
#[test]
fn validate_processing_key_xors_the_uv_into_the_media_key_tail() {
    // uv with all four bytes non-zero and overlapping the planted mk tail.
    const UV: u32 = 0xF0F0_F0F0;
    let pk = [0x5Au8; 16];
    // Choose a Media Key whose tail shares bits with uv, so XOR and OR
    // differ, and invert the relation to build the cvalue and verify block.
    let mk: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xFF, 0xFF, 0xFF,
        0xFF,
    ];
    let mut pre = mk;
    for (a, b) in pre[12..16].iter_mut().zip(UV.to_be_bytes()) {
        *a ^= b;
    }
    let cvalue = aes_ecb_encrypt(&pk, &pre);
    let mut vd = [0x0Fu8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    assert_eq!(
        validate_processing_key(&pk, &cvalue, &UV.to_be_bytes(), &mk_dv),
        Some(mk),
        "uv must be XORed (not ORed) into the Media Key's low 4 bytes"
    );
}

/// `validate_processing_key` is handed slices straight out of MKB records,
/// so its length guards are what stand between a short/truncated record and
/// an out-of-bounds read. Under-length inputs must yield `None`.
#[test]
fn validate_processing_key_refuses_short_cvalue_or_uv() {
    let p = plant_mkb();
    let pk = aesg3(&p.dkey, 1);

    assert!(
        validate_processing_key(&pk, &p.cv[..15], &p.uv.to_be_bytes(), &p.mk_dv).is_none(),
        "a cvalue shorter than 16 bytes is not usable"
    );
    assert!(
        validate_processing_key(&pk, &p.cv, &p.uv.to_be_bytes()[..3], &p.mk_dv).is_none(),
        "a uv shorter than 4 bytes is not usable"
    );
    // Exactly-sized inputs are accepted and yield the planted Media Key.
    assert_eq!(
        validate_processing_key(&pk, &p.cv, &p.uv.to_be_bytes(), &p.mk_dv),
        Some(p.mk),
        "the exactly-sized planted inputs must still validate"
    );
}

// probe::aes_dec is the sole verify primitive a reproduction harness has;
// a fixed-block body would answer the same way for every key/disc — the
// km_verifies failure one layer out. Asserted for the planted key and a stranger.
#[test]
fn probe_aes_dec_reproduces_the_verify_relation_for_the_planted_key() {
    let p = plant_mkb();

    let plain = probe::aes_dec(&p.mk, &p.mk_dv);
    assert_eq!(
        &plain[..8],
        &VERIFY_MAGIC[..],
        "AES-D(Km, mk_dv) must open with the Verify-Media-Key magic"
    );

    let mut stranger = p.mk;
    stranger[0] ^= 0x01;
    assert_ne!(
        &probe::aes_dec(&stranger, &p.mk_dv)[..8],
        &VERIFY_MAGIC[..],
        "a key one bit away must not reproduce the magic"
    );

    // It is a decryption, not a transformation of its own choosing: it must
    // invert the forward primitive for an arbitrary block.
    let block = [0x5Cu8; 16];
    assert_eq!(
        probe::aes_dec(&p.mk, &aes_ecb_encrypt(&p.mk, &block)),
        block,
        "aes_dec must be the exact inverse of AES-128-ECB encrypt"
    );
}

/// `probe::mkb_cvalues` is the Media-Key-Data table the whole PK×cvalue
/// scan iterates. An empty or one-byte table makes every scan find nothing,
/// so a harness would report a good key as non-working.
#[test]
fn probe_mkb_cvalues_returns_the_records_actual_bytes() {
    let p = plant_mkb();
    let cvalues = probe::mkb_cvalues(&p.mkb).expect("the 0x05 record is present");
    assert_eq!(
        cvalues.len(),
        16,
        "one 16-byte cvalue was planted; the table must be that long"
    );
    assert_eq!(
        &cvalues[..],
        &p.cv[..],
        "cvalue bytes must be the planted ones"
    );

    // The table is what the terminal-PK scan consumes; prove it drives the
    // real scan to the planted Media Key.
    let uvs = probe::mkb_subdiff(&p.mkb).expect("subdiff record present");
    let pk = aesg3(&p.dkey, 1);
    assert_eq!(
        try_pk_against_tables(&[pk], &uvs, &cvalues, &p.mk_dv),
        Some(p.mk),
        "the probe's cvalue table must be the one the PK scan can use"
    );
}

// An ODD subset-difference `uv` — the legal depth-0 slot. Descent starts at
// `uv_r.trailing_zeros() + 1`; other fixtures use even `uv`, so this alone exercises
// `trailing_zeros() == 0` and the `p == 0` boundary where `+ 1` blocks a `(p-1)` underflow.

/// Slot `uv` with bits 8, 6, 4 AND 0 set: lowest set bit 0, so
/// `trailing_zeros() == 0` and the descent must start at level 1.
const UV_ODD: u32 = 0x0000_0151;
/// The ancestor one level up — what the descent's first candidate
/// (`k == 1`) resolves to: `(UV_ODD & !0b11) | 0b10`.
const UV_ODD_ANC: u32 = 0x0000_0152;
const U_MASK_SHIFT_ODD: u8 = 12;

// An MKB with a single ODD-uv slot, keyed one level above it. Expected PK
// is the EXPLICIT aesg3 chain (`[C]` §3.2.4), not calc_pk_from_dk — a
// fixture built by the walk would move with the walk's own mutations.
fn plant_odd_uv_mkb() -> (Vec<u8>, [u8; 16], [u8; 16], [u8; 16]) {
    let dkey: [u8; 16] = [
        0x2F, 0x3E, 0x4D, 0x5C, 0x6B, 0x7A, 0x89, 0x98, 0xA7, 0xB6, 0xC5, 0xD4, 0xE3, 0xF2, 0x01,
        0x10,
    ];
    let mk: [u8; 16] = [
        0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE,
        0xEF,
    ];

    let pk = aesg3(&aesg3(&dkey, 0), 1);

    let mut mk_raw = mk;
    for (a, b) in mk_raw[12..16].iter_mut().zip(UV_ODD.to_be_bytes()) {
        *a ^= b;
    }
    let cv = aes_ecb_encrypt(&pk, &mk_raw);

    let mut vd = [0x27u8; 16];
    vd[..8].copy_from_slice(&VERIFY_MAGIC);
    let mk_dv = aes_ecb_encrypt(&mk, &vd);

    let mkb = build_mkb(&[(U_MASK_SHIFT_ODD, UV_ODD)], &cv, &mk_dv);
    (mkb, dkey, mk, pk)
}

/// Fixture sanity: the slot really is odd, and the ancestor really is the
/// level-1 candidate. If either drifted, the test below would silently stop
/// covering the depth-0 descent it exists for.
#[test]
fn the_odd_uv_fixture_sits_at_tree_depth_zero() {
    assert_eq!(UV_ODD.trailing_zeros(), 0, "an odd uv is at depth 0");
    assert_eq!(
        UV_ODD_ANC,
        (UV_ODD & (0xFFFF_FFFFu32 << 2)) | (1u32 << 1),
        "the level-1 ancestor of an odd uv"
    );
    // The walk's own gate: the device's position must agree with the slot's
    // above the ancestor's own lowest set bit.
    let dev_v_mask = calc_v_mask(UV_ODD_ANC);
    assert_eq!(UV_ODD & dev_v_mask, UV_ODD_ANC & dev_v_mask);
}

// Depth 0 is where the descent's lower bound is at its arithmetic edge; a
// wrong bound either underflows or starts the scan at the slot's own level,
// both reporting a valid key as not applying.
#[test]
fn recover_dk_position_descends_from_an_odd_uv_slot_at_tree_depth_zero() {
    let (mkb, dkey, mk, pk) = plant_odd_uv_mkb();

    let recovered =
        recover_dk_position(&mkb, &dkey).expect("the planted key opens the odd-uv slot");

    assert_eq!(
        recovered.uv, UV_ODD_ANC,
        "the recovered position is the level-1 ancestor, not the slot itself"
    );
    assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT_ODD);
    assert_eq!(
        derive_media_key_from_dk(&mkb, std::slice::from_ref(&recovered)),
        Some(mk),
        "the recovered position must walk the odd-uv slot to its Media Key"
    );

    // The Processing Key the walk produces at that position is the explicit
    // one-level descent, not the zero-descent key.
    assert_eq!(
        derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&recovered)),
        Some((mk, pk))
    );
    assert_ne!(pk, aesg3(&dkey, 1), "this is NOT the zero-descent key");
}

// Negative direction: a key that does not open the slot must sweep every
// descent level (1..32, since depth 0) and return None, without
// underflowing the lower bound or shifting a u32 by 32 at the top.
#[test]
fn an_odd_uv_slot_sweeps_every_descent_level_without_arithmetic_overflow() {
    let (mkb, dkey, _mk, _pk) = plant_odd_uv_mkb();
    let mut stranger = dkey;
    stranger[0] ^= 0x01;
    assert!(
        recover_dk_position(&mkb, &stranger).is_none(),
        "a key one bit off must not be handed a position"
    );
}

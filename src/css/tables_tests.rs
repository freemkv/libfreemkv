use super::*;

/// Pins the documented relationship `TAB5[i] == TAB4[i] ^ 0xFF` so the
/// table doc cannot drift from the data.
#[test]
fn tab5_is_complement_of_tab4() {
    for i in 0..256 {
        assert_eq!(
            TAB5[i],
            TAB4[i] ^ 0xFF,
            "TAB5[{i:#04x}] != TAB4[{i:#04x}] ^ 0xFF"
        );
    }
}

// TAB1 is a bijection on 0..256; CSS uses it as an invertible output permutation in
// css_DecryptKey.
#[test]
fn tab1_is_a_permutation() {
    let mut seen = [false; 256];
    for (i, &v) in TAB1.iter().enumerate() {
        assert!(
            !seen[v as usize],
            "TAB1 maps two inputs to {v:#04x} (collision at index {i:#04x})"
        );
        seen[v as usize] = true;
    }
}

// TAB1's fixed structural anchors: TAB1[0x00] == 0x33 and TAB1[0x33] == 0x00, the canonical
// CSS-spec landmarks pinning the table's orientation.
#[test]
fn tab1_known_spec_anchors() {
    assert_eq!(TAB1[0x00], 0x33, "TAB1[0] is the published 0x33");
    assert_eq!(TAB1[0x33], 0x00, "TAB1[0x33] is the published 0x00");
}

// FNV-1a over a whole table: a swapped pair keeps a permutation and the anchors, so only
// a full-content pin catches a mistyped or regenerated table.
fn fnv1a(table: &[u8]) -> u64 {
    table.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[test]
fn tab1_and_tab2_contents_are_pinned() {
    assert_eq!(fnv1a(&TAB1), 0xcdb8_7228_1c08_c265, "TAB1 content changed");
    assert_eq!(fnv1a(&TAB2), 0x79e0_69d7_0f3d_2f25, "TAB2 content changed");
}

// TAB2 is a permutation of 0..256 (LFSR1 high-byte feedback substitution); a non-bijective
// table would bias the LFSR1 keystream.
#[test]
fn tab2_is_a_permutation() {
    let mut seen = [false; 256];
    for (i, &v) in TAB2.iter().enumerate() {
        assert!(
            !seen[v as usize],
            "TAB2 maps two inputs to {v:#04x} (collision at index {i:#04x})"
        );
        seen[v as usize] = true;
    }
}

// TAB3 is CSS's LFSR1 low-word table: BASE = [0x00,0x24,0x49,0x6d,0x92, 0xb6,0xdb,0xff]
// repeated 64x (TAB3[i] == BASE[i & 7]); only the low 3 bits of the 9-bit index matter.
#[test]
fn tab3_matches_lfsr1_generating_formula() {
    const BASE: [u8; 8] = [0x00, 0x24, 0x49, 0x6d, 0x92, 0xb6, 0xdb, 0xff];
    for i in 0..512usize {
        let expected = BASE[i & 7];
        assert_eq!(
            TAB3[i], expected,
            "TAB3[{i:#05x}] = {:#04x}, formula BASE[i&7] = {expected:#04x}",
            TAB3[i]
        );
    }
}

// TAB4 is the exact bit-reversal of each byte (CSS permutes LFSR0 bytes with it on
// seed/output), hence also an involution: TAB4[TAB4[b]] == b.
#[test]
fn tab4_is_exact_bit_reversal_and_involution() {
    for b in 0u16..256 {
        let rev = (0..8).fold(0u8, |acc, k| acc | (((b as u8 >> k) & 1) << (7 - k)));
        assert_eq!(
            TAB4[b as usize], rev,
            "TAB4[{b:#04x}] is not the bit-reversal {rev:#04x}"
        );
    }
    for b in 0..256usize {
        assert_eq!(
            TAB4[TAB4[b] as usize], b as u8,
            "TAB4 not an involution at {b:#04x}"
        );
    }
    // Spec landmark entries.
    assert_eq!(TAB4[0x01], 0x80);
    assert_eq!(TAB4[0x80], 0x01);
    assert_eq!(TAB4[0x00], 0x00);
    assert_eq!(TAB4[0xFF], 0xFF);
}

// TAB4 is a permutation (bit-reversal is bijective); distinct from the reversal test since
// "reversal except swapped/duplicated entries" cases would pass one test but fail the
// other.
#[test]
fn tab4_is_a_permutation() {
    let mut seen = [false; 256];
    for &v in TAB4.iter() {
        assert!(!seen[v as usize], "TAB4 maps two inputs to {v:#04x}");
        seen[v as usize] = true;
    }
}

// TAB5 is a permutation (complement of a bijection) with orientation anchors
// TAB5[0x00]==0xFF and TAB5[0xFF]==0x00, independent of the complement-loop test.
#[test]
fn tab5_is_permutation_with_anchors() {
    let mut seen = [false; 256];
    for &v in TAB5.iter() {
        assert!(!seen[v as usize], "TAB5 maps two inputs to {v:#04x}");
        seen[v as usize] = true;
    }
    assert_eq!(TAB5[0x00], 0xFF, "TAB5[0] = TAB4[0]^0xFF = 0xFF");
    assert_eq!(TAB5[0xFF], 0x00, "TAB5[0xFF] = TAB4[0xFF]^0xFF = 0x00");
}

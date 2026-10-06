use super::align_up;

#[test]
fn mask_zero_is_identity() {
    // AlignmentMask 0 (USB optical bridges) — no alignment required.
    for p in [0usize, 1, 2, 3, 7, 8, 13, 4096, 0x7fff_ffff] {
        assert_eq!(align_up(p, 0), p);
    }
}

#[test]
fn already_aligned_is_unchanged() {
    // DWORD (mask 3): multiples of 4 are already aligned.
    assert_eq!(align_up(0, 3), 0);
    assert_eq!(align_up(4, 3), 4);
    assert_eq!(align_up(8, 3), 8);
    // 8-byte (mask 7): multiples of 8.
    assert_eq!(align_up(0, 7), 0);
    assert_eq!(align_up(16, 7), 16);
}

#[test]
fn rounds_up_to_next_boundary() {
    // mask 1 (2-byte): odd → next even.
    assert_eq!(align_up(1, 1), 2);
    assert_eq!(align_up(3, 1), 4);
    // mask 3 (DWORD): 1,2,3 → 4; 5,6,7 → 8.
    assert_eq!(align_up(1, 3), 4);
    assert_eq!(align_up(2, 3), 4);
    assert_eq!(align_up(3, 3), 4);
    assert_eq!(align_up(5, 3), 8);
    // mask 7 (8-byte): 1..=7 → 8; 9 → 16.
    assert_eq!(align_up(1, 7), 8);
    assert_eq!(align_up(7, 7), 8);
    assert_eq!(align_up(9, 7), 16);
}

#[test]
fn result_always_satisfies_mask() {
    for &mask in &[0usize, 1, 3, 7, 15, 31, 63] {
        for p in 0usize..256 {
            let a = align_up(p, mask);
            assert!(a >= p, "align_up({p},{mask})={a} went backwards");
            assert_eq!(a & mask, 0, "align_up({p},{mask})={a} not aligned");
            // Smallest such value: anything in (p-1-mask, a) would be < p
            // or unaligned; check a - p never exceeds mask.
            assert!(a - p <= mask, "align_up({p},{mask})={a} overshot");
        }
    }
}

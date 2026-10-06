use super::*;

// Shrunk != Probed: distinct meanings (error vs. recovery), must not compare equal.
#[test]
fn batch_size_reason_variants_are_not_equal() {
    assert_ne!(BatchSizeReason::Shrunk, BatchSizeReason::Probed);
}

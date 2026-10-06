use super::*;

/// The maximum every backend declares (`K_MAX_CDB_SIZE`).
const MAX: usize = 16;

// Oversized CDB must be rejected, never truncated. This is the only
// place the guard can be tested on every platform's CI, since
// linux.rs/macos.rs/windows.rs each compile on one host only.
#[test]
fn oversized_cdb_is_rejected_not_truncated() {
    let cdb = [0u8; MAX + 1];
    match checked_cdb_len(&cdb, MAX) {
        Err(Error::InvalidCdbLength { len, max }) => {
            assert_eq!(len, MAX + 1);
            assert_eq!(max, MAX);
        }
        Err(other) => panic!("expected InvalidCdbLength, got {other:?}"),
        Ok(n) => panic!(
            "over-length CDB was accepted and truncated to {n} bytes — the drive \
                 would execute a DIFFERENT command than the caller asked for"
        ),
    }
}

/// A CDB exactly at the field width is legal and passes through untouched.
#[test]
fn max_length_cdb_is_accepted() {
    let cdb = [0u8; MAX];
    assert_eq!(checked_cdb_len(&cdb, MAX).ok(), Some(MAX as u8));
}

/// Every real CDB length (SPC-4 groups 0-5: 6, 10, 12, 16 bytes) is
/// accepted and reported verbatim.
#[test]
fn in_range_cdb_lengths_pass_through_verbatim() {
    for len in [6usize, 10, 12, 16] {
        let cdb = vec![0u8; len];
        assert_eq!(
            checked_cdb_len(&cdb, MAX).ok(),
            Some(len as u8),
            "CDB of {len} bytes must be accepted verbatim"
        );
    }
}

// Empty CDB must be rejected by the shared helper; per-backend guards
// previously missed it (macOS/Windows passed a zero-length descriptor
// straight to the driver before this existed).
#[test]
fn empty_cdb_is_rejected_by_the_shared_helper() {
    match checked_cdb_len(&[], MAX) {
        Err(Error::InvalidCdbLength { len, max }) => {
            assert_eq!(len, 0);
            assert_eq!(max, MAX);
        }
        Err(other) => panic!("expected InvalidCdbLength, got {other:?}"),
        Ok(n) => panic!(
            "empty CDB accepted with length {n} — every backend would then \
                 index cdb[0] or issue a zero-length command descriptor"
        ),
    }
}

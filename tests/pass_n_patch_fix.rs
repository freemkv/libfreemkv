//! Regression tests for Pass N (patch) fix — decrypt key inversion bug.
//!
//! Tests that decrypt_sectors is invoked correctly when opts.decrypt=true.
//! The 2026-05-03 bug at `libfreemkv/src/disc/mod.rs:1938-1942` inverted
//! the decrypt key arms, causing patch to pass DecryptKeys::None on encrypted discs.

use libfreemkv::{aacs, decrypt::DecryptKeys};

/// Test: decrypt_sectors with DecryptKeys::None is a no-op.
#[test]
fn decrypt_sectors_with_none_keys_is_noop() {
    let mut sector = vec![0x42u8; 2048];

    let mut keys = DecryptKeys::None;
    let result = libfreemkv::decrypt::decrypt_sectors(&mut sector, &mut keys, 0);

    assert!(result.is_ok());
    assert_eq!(
        &sector[..],
        &[0x42u8; 2048][..],
        "DecryptKeys::None should not modify buffer"
    );
}

/// Test: decrypt_sectors with CSS keys descrambles sectors.
#[test]
fn css_decrypt_of_an_unkeyable_sector_fails_instead_of_emitting_data() {
    // A scrambled sector whose header is uniformly periodic yields a crib, so
    // the supplied key IS validated — and this arbitrary key is not the right
    // one, so the crib check rejects it and the re-crack from this synthetic
    // body finds nothing.
    //
    // CSS has no external key source: the title key comes only from cracking
    // the data. So "no key" on a readable sector is recovery failing on bytes
    // we can see, not a missing input — the same condition AACS answers with
    // DecryptFailed rather than applying a neighbouring unit's key. Emitting
    // the sector either way is bad data reported as success: descrambled with
    // the rejected key it is garbage behind an intact clear header, and passed
    // through untouched it is ciphertext where plaintext is meant to be.
    //
    // This test previously asserted the scramble flag was cleared, which pinned
    // the old behaviour of descrambling with whatever key happened to be held.
    let mut sector = vec![0xFFu8; 2048];
    sector[0x14] |= 0x30; // CSS scramble flag, bits 4-5

    let title_key: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let mut keys = DecryptKeys::Css { title_key };

    let err = libfreemkv::decrypt::decrypt_sectors(&mut sector, &mut keys, 0)
        .expect_err("an unkeyable CSS sector must fail loud");
    assert_eq!(
        err.code(),
        libfreemkv::error::Error::DecryptFailed.code(),
        "CSS and AACS must give the SAME verdict for 'no provable key'"
    );
}

/// Test: AACS unit encryption detection works.
#[test]
fn aacs_encryption_flag_detection() {
    // A clear unit: TS syncs (0x47) intact at every 192-byte packet.
    let mut unit = vec![0u8; aacs::content::ALIGNED_UNIT_LEN];
    let mut off = 4;
    while off < aacs::content::ALIGNED_UNIT_LEN {
        unit[off] = 0x47;
        off += 192;
    }
    // Encryption is the scrambled body (TS syncs destroyed), NOT a flag bit.
    assert!(aacs::content::is_clean(
        &unit,
        libfreemkv::disc::ContentFormat::BdTs
    ));

    // Flag bits on a synced unit do not make it look encrypted.
    unit[0] = 0xC0;
    unit[7] = 0xC0;
    assert!(aacs::content::is_clean(
        &unit,
        libfreemkv::disc::ContentFormat::BdTs
    ));

    // Scrambled body (syncs gone) → encrypted.
    let scrambled = vec![0x99u8; aacs::content::ALIGNED_UNIT_LEN];
    assert!(!aacs::content::is_clean(
        &scrambled,
        libfreemkv::disc::ContentFormat::BdTs
    ));
}

/// Test: DecryptKeys::is_encrypted() correctly identifies encrypted state.
#[test]
fn decrypt_keys_is_encrypted_variants() {
    let none = DecryptKeys::None;
    assert!(!none.is_encrypted());

    let aacs = DecryptKeys::Aacs {
        unit_keys: vec![],
        read_data_key: None,
        format: libfreemkv::disc::ContentFormat::BdTs,
    };
    assert!(aacs.is_encrypted());

    let css = DecryptKeys::Css {
        title_key: [0u8; 5],
    };
    assert!(css.is_encrypted());
}

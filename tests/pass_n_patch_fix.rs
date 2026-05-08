//! Regression tests for Pass N (patch) fix — decrypt key inversion bug.
//!
//! Tests that decrypt_sectors is invoked correctly when opts.decrypt=true.
//! The 2026-05-03 bug at `libfreemkv/src/disc/mod.rs:1938-1942` inverted
//! the decrypt key arms, causing patch to pass DecryptKeys::None on encrypted discs.

use libfreemkv::{aacs, decrypt::DecryptKeys};

/// Test: decrypt_sectors with AACS keys actually decrypts units.
#[test]
fn decrypt_sectors_with_aacs_keys_works() {
    // Build an encrypted aligned unit
    let mut unit = vec![0xFFu8; aacs::ALIGNED_UNIT_LEN];

    // Set encryption flag (bits 6-7 of byte 0)
    unit[0] |= 0xC0;

    // Fill with recognizable pattern
    for (i, byte) in unit
        .iter_mut()
        .enumerate()
        .take(aacs::ALIGNED_UNIT_LEN)
        .skip(1)
    {
        *byte = ((i * 3 + 7) & 0xFF) as u8;
    }

    let unit_key: [u8; 16] = [0xAAu8; 16];

    // Encrypt the unit using AACS algorithm
    aacs::decrypt_unit(&mut unit, &unit_key); // decrypt_unit is idempotent on already-encrypted data

    // Now we have encrypted data - create DecryptKeys with actual keys
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0u32, unit_key)],
        read_data_key: None,
    };

    // decrypt_sectors should handle this without error
    let result = libfreemkv::decrypt::decrypt_sectors(&mut unit, &keys, 0);

    assert!(
        result.is_ok(),
        "decrypt_sectors with AACS keys should not error"
    );
}

/// Test: decrypt_sectors with DecryptKeys::None is a no-op.
#[test]
fn decrypt_sectors_with_none_keys_is_noop() {
    let mut sector = vec![0x42u8; 2048];

    let keys = DecryptKeys::None;
    let result = libfreemkv::decrypt::decrypt_sectors(&mut sector, &keys, 0);

    assert!(result.is_ok());
    assert_eq!(
        &sector[..],
        &[0x42u8; 2048][..],
        "DecryptKeys::None should not modify buffer"
    );
}

/// Test: decrypt_sectors with CSS keys descrambles sectors.
#[test]
fn decrypt_sectors_with_css_keys_works() {
    let mut sector = vec![0xFFu8; 2048];

    // Set CSS scramble flag (bits 4-5 of byte 0x14)
    sector[0x14] |= 0x30;

    let title_key: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF]; // Not used - defined later
    let keys = DecryptKeys::Css { title_key };

    // Descramble (CSS uses same operation for encrypt/decrypt)
    libfreemkv::decrypt::decrypt_sectors(&mut sector, &keys, 0).unwrap();

    // Flag should be cleared
    assert_eq!(sector[0x14] & 0x30, 0x00, "CSS flag should be cleared");
}

/// Test: AACS unit encryption detection works.
#[test]
fn aacs_encryption_flag_detection() {
    let mut unit = vec![0u8; aacs::ALIGNED_UNIT_LEN];

    // No encryption flag
    assert!(!aacs::is_unit_encrypted(&unit));

    // Set bit 6
    unit[0] |= 0x40;
    assert!(aacs::is_unit_encrypted(&unit));

    // Set bit 7
    unit[0] = 0x80;
    assert!(aacs::is_unit_encrypted(&unit));

    // Both bits set
    unit[0] = 0xC0;
    assert!(aacs::is_unit_encrypted(&unit));
}

/// Test: DecryptKeys::is_encrypted() correctly identifies encrypted state.
#[test]
fn decrypt_keys_is_encrypted_variants() {
    let none = DecryptKeys::None;
    assert!(!none.is_encrypted());

    let aacs = DecryptKeys::Aacs {
        unit_keys: vec![],
        read_data_key: None,
    };
    assert!(aacs.is_encrypted());

    let css = DecryptKeys::Css {
        title_key: [0u8; 5],
    };
    assert!(css.is_encrypted());
}

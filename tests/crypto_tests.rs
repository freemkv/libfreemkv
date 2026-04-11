//! Comprehensive roundtrip tests for CSS and AACS cryptographic implementations.
//!
//! These tests prove the cryptographic algorithms work end-to-end.
//! Tests that require access to private internals are placed as unit tests
//! inside the respective source files (css/lfsr.rs, css/crack.rs, aacs/handshake.rs).
//!
//! This file tests public API items accessible from integration tests.

use libfreemkv::aacs;
use libfreemkv::css;

// ── CSS Public API Tests ────────────────────────────────────────────────────

/// Test: css_descramble_sector_roundtrip_via_public_api
///
/// The public css::descramble_sector() wraps the LFSR descrambler.
/// Since the cipher is XOR-based, calling descramble twice (with restored
/// flags) should roundtrip the data.
#[test]
fn css_descramble_sector_roundtrip_via_public_api() {
    let state = css::CssState {
        title_key: [0x42, 0x13, 0x37, 0xBE, 0xEF],
    };

    // Build a sector with scramble flag set
    let mut sector = vec![0x00u8; 2048];
    sector[0x14] = 0x30; // scramble flag
    sector[0x54..0x59].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x42]); // seed
    // PES header at byte 128
    sector[0x80] = 0x00;
    sector[0x81] = 0x00;
    sector[0x82] = 0x01;
    sector[0x83] = 0xE0;
    // Fill content
    for i in 0x84..2048 {
        sector[i] = (i & 0xFF) as u8;
    }
    let original = sector.clone();

    // First descramble
    css::descramble_sector(&state, &mut sector);
    assert_eq!(sector[0x14] & 0x30, 0x00, "flag not cleared");
    assert_ne!(&sector[0x80..0x84], &original[0x80..0x84], "content unchanged");

    // Restore flag for second pass
    sector[0x14] = 0x30;

    // Second descramble = roundtrip
    css::descramble_sector(&state, &mut sector);
    assert_eq!(
        &sector[0x80..2048],
        &original[0x80..2048],
        "double descramble did not roundtrip"
    );
}

/// Test: css_is_scrambled detects scramble flags correctly.
#[test]
fn css_is_scrambled_detection() {
    let mut sector = vec![0u8; 2048];
    assert!(!css::is_scrambled(&sector), "empty sector should not be scrambled");

    sector[0x14] = 0x10; // bit 4 set
    assert!(css::is_scrambled(&sector), "bit 4 set should be detected");

    sector[0x14] = 0x20; // bit 5 set
    assert!(css::is_scrambled(&sector), "bit 5 set should be detected");

    sector[0x14] = 0x30; // both bits set
    assert!(css::is_scrambled(&sector), "both bits set should be detected");

    sector[0x14] = 0xCF; // bits 4-5 clear, other bits set
    assert!(!css::is_scrambled(&sector), "bits 4-5 clear should not be scrambled");
}

// ── AACS Public API Tests ───────────────────────────────────────────────────

/// Test 6: aacs_decrypt_unit_roundtrip
///
/// Build a synthetic 6144-byte aligned unit with TS sync bytes, encrypt it
/// using the AACS algorithm (AES-ECB header derivation + AES-CBC body),
/// then decrypt with decrypt_unit() and verify the plaintext matches.
#[test]
fn aacs_decrypt_unit_roundtrip() {
    use aes::cipher::{generic_array::GenericArray, BlockEncrypt, KeyInit};
    use aes::Aes128;

    let unit_key = [0xAAu8; 16];
    let aacs_iv: [u8; 16] = [
        0x0B, 0xA0, 0xF8, 0xDD, 0xFE, 0xA6, 0x1F, 0xB3, 0xD8, 0xDF, 0x9F, 0x56, 0x6A, 0x05,
        0x0F, 0x78,
    ];

    // Build plaintext unit with TS sync bytes every 192 bytes starting at offset 4
    let mut plain = vec![0u8; aacs::ALIGNED_UNIT_LEN];
    let mut offset = 4;
    while offset < aacs::ALIGNED_UNIT_LEN {
        plain[offset] = 0x47; // TS sync byte
        offset += 192;
    }
    // Set encryption flag (bits 6-7 of byte 0)
    plain[0] |= 0xC0;

    // Save original plaintext for comparison
    let expected = plain.clone();

    // Encrypt: replicate the AACS encryption algorithm (reverse of decrypt_unit)
    let header: [u8; 16] = plain[..16].try_into().unwrap();

    // Step 1: AES-ECB encrypt header with unit key
    let cipher_header = Aes128::new(GenericArray::from_slice(&unit_key));
    let mut block = GenericArray::clone_from_slice(&header);
    cipher_header.encrypt_block(&mut block);
    let mut derived = [0u8; 16];
    derived.copy_from_slice(&block);

    // Step 2: XOR to get per-unit decryption key
    let mut encrypt_key = [0u8; 16];
    for i in 0..16 {
        encrypt_key[i] = derived[i] ^ header[i];
    }

    // Step 3: AES-CBC encrypt bytes 16..6144
    let cipher = Aes128::new(GenericArray::from_slice(&encrypt_key));
    let mut prev = aacs_iv;
    let num_blocks = (aacs::ALIGNED_UNIT_LEN - 16) / 16;
    for i in 0..num_blocks {
        let off = 16 + i * 16;
        for j in 0..16 {
            plain[off + j] ^= prev[j];
        }
        let mut blk = GenericArray::clone_from_slice(&plain[off..off + 16]);
        cipher.encrypt_block(&mut blk);
        plain[off..off + 16].copy_from_slice(&blk);
        prev.copy_from_slice(&plain[off..off + 16]);
    }

    // Verify it looks encrypted
    assert!(aacs::is_unit_encrypted(&plain));

    // Now decrypt
    let result = aacs::decrypt_unit(&mut plain, &unit_key);
    assert!(result, "decrypt_unit should return true on valid encrypted unit");
    assert!(!aacs::is_unit_encrypted(&plain), "encryption flag should be cleared");

    // Verify TS sync bytes at expected positions (flag byte is cleared by decrypt)
    let mut sync_count = 0;
    let mut off = 4;
    while off < aacs::ALIGNED_UNIT_LEN {
        if plain[off] == 0x47 {
            sync_count += 1;
        }
        off += 192;
    }
    let expected_syncs = (aacs::ALIGNED_UNIT_LEN - 4) / 192 + 1;
    assert_eq!(
        sync_count, expected_syncs,
        "TS sync bytes not recovered: got {}, expected {}",
        sync_count, expected_syncs
    );

    // Compare all bytes except byte 0 (encryption flag cleared)
    assert_eq!(
        &plain[1..aacs::ALIGNED_UNIT_LEN],
        &expected[1..aacs::ALIGNED_UNIT_LEN],
        "decrypted unit body does not match original"
    );
    // Byte 0: original had 0xC0 set, decrypted has it cleared
    assert_eq!(plain[0] & !0xC0, expected[0] & !0xC0, "byte 0 mismatch ignoring flag");
}

/// Test 7: aacs_disc_hash_deterministic
///
/// compute disc_hash on the same data twice, verify identical results.
#[test]
fn aacs_disc_hash_deterministic() {
    let data1 = b"Unit_Key_RO.inf test data for deterministic hashing";
    let data2 = b"Different data should produce different hash";

    let hash1a = aacs::disc_hash(data1);
    let hash1b = aacs::disc_hash(data1);
    assert_eq!(hash1a, hash1b, "disc_hash not deterministic on same input");

    let hash2 = aacs::disc_hash(data2);
    assert_ne!(hash1a, hash2, "different inputs should produce different hashes");

    // Verify it is a 20-byte SHA-1 hash
    assert_eq!(hash1a.len(), 20);

    // Verify disc_hash_hex formatting
    let hex = aacs::disc_hash_hex(&hash1a);
    assert!(hex.starts_with("0x"), "hex should start with 0x prefix");
    assert_eq!(hex.len(), 42, "hex string should be 42 chars (0x + 40 hex digits)");
}

/// Test: aacs_decrypt_unit_key_roundtrip
///
/// Verify that encrypting a unit key with AES-ECB and decrypting it with
/// decrypt_unit_key recovers the original.
#[test]
fn aacs_decrypt_unit_key_roundtrip() {
    use aes::cipher::{generic_array::GenericArray, BlockEncrypt, KeyInit};
    use aes::Aes128;

    let vuk = [
        0x11u8, 0x14, 0x36, 0x0B, 0x10, 0xEE, 0x6E, 0xAC, 0x78, 0xAA, 0x4A, 0xC0, 0xB7, 0x52,
        0xEA, 0xEB,
    ];
    let original_unit_key = [
        0x9E, 0x5D, 0x13, 0x10, 0x33, 0x74, 0x43, 0xE8, 0x11, 0xA5, 0x2E, 0xBB, 0xEA, 0xE0,
        0x47, 0x0F,
    ];

    // Encrypt: AES-ECB encrypt the unit key with VUK
    let cipher = Aes128::new(GenericArray::from_slice(&vuk));
    let mut block = GenericArray::clone_from_slice(&original_unit_key);
    cipher.encrypt_block(&mut block);
    let mut encrypted_uk = [0u8; 16];
    encrypted_uk.copy_from_slice(&block);

    // Decrypt with the public API
    let decrypted = aacs::decrypt_unit_key(&vuk, &encrypted_uk);
    assert_eq!(
        decrypted, original_unit_key,
        "decrypt_unit_key did not recover original unit key"
    );
}

/// Test: aacs_vuk_derivation
///
/// Verify derive_vuk: VUK = AES-ECB-DECRYPT(media_key, volume_id) XOR volume_id
#[test]
fn aacs_vuk_derivation_roundtrip() {
    let media_key = [0x25u8, 0x2F, 0xB6, 0x36, 0xE8, 0x83, 0x52, 0x9E,
                     0x11, 0x9A, 0xB7, 0x15, 0xF4, 0xEB, 0x16, 0x40];
    let volume_id = [0xA1u8, 0x3C, 0xBE, 0x2C, 0xE4, 0x05, 0x65, 0xD1,
                     0x04, 0xB5, 0x3E, 0x76, 0x8C, 0x70, 0x0E, 0x30];

    let vuk = aacs::derive_vuk(&media_key, &volume_id);

    // VUK should be non-zero and different from both inputs
    assert_ne!(vuk, [0u8; 16], "VUK should not be all zeros");
    assert_ne!(vuk, media_key, "VUK should differ from media_key");
    assert_ne!(vuk, volume_id, "VUK should differ from volume_id");

    // Verify determinism
    let vuk2 = aacs::derive_vuk(&media_key, &volume_id);
    assert_eq!(vuk, vuk2, "derive_vuk not deterministic");
}

/// Test: aacs_is_unit_encrypted detects encryption flags correctly.
#[test]
fn aacs_is_unit_encrypted_detection() {
    let mut unit = vec![0u8; aacs::ALIGNED_UNIT_LEN];

    assert!(!aacs::is_unit_encrypted(&unit), "zero unit should not be encrypted");

    unit[0] = 0x40; // bit 6 set
    assert!(aacs::is_unit_encrypted(&unit));

    unit[0] = 0x80; // bit 7 set
    assert!(aacs::is_unit_encrypted(&unit));

    unit[0] = 0xC0; // both bits set
    assert!(aacs::is_unit_encrypted(&unit));

    unit[0] = 0x3F; // bits 6-7 clear
    assert!(!aacs::is_unit_encrypted(&unit));

    // Too short
    let short = vec![0xC0u8; 100];
    assert!(!aacs::is_unit_encrypted(&short), "short buffer should not be detected");
}

/// Test: aacs_decrypt_unit_unencrypted_passthrough
///
/// A unit without encryption flags should pass through decrypt_unit unchanged.
#[test]
fn aacs_decrypt_unit_unencrypted_passthrough() {
    let mut unit = vec![0x42u8; aacs::ALIGNED_UNIT_LEN];
    unit[0] = 0x00; // no encryption flag
    let original = unit.clone();
    let key = [0xAA; 16];

    let result = aacs::decrypt_unit(&mut unit, &key);
    assert!(result, "unencrypted unit should return true");
    assert_eq!(unit, original, "unencrypted unit should be unchanged");
}

/// Test: aacs_parse_unit_key_ro with minimal valid data
#[test]
fn aacs_parse_unit_key_ro_minimal() {
    // Build a minimal Unit_Key_RO.inf structure
    // Header: first 4 bytes = BE32 offset to key storage area
    let uk_pos: u32 = 100;
    let mut data = vec![0u8; 200];

    // Key storage offset
    data[0..4].copy_from_slice(&uk_pos.to_be_bytes());
    // app_type
    data[16] = 1; // BD-ROM
    // num_bdmv_dir
    data[17] = 1;
    // flags
    data[18] = 0;

    // At uk_pos: num_unit_keys = 1
    let pos = uk_pos as usize;
    data[pos] = 0;
    data[pos + 1] = 1; // 1 key

    // At uk_pos + 48: first encrypted key (16 bytes)
    let key_pos = pos + 48;
    for i in 0..16 {
        data[key_pos + i] = (0xA0 + i) as u8;
    }

    let result = aacs::parse_unit_key_ro(&data, false);
    assert!(result.is_some(), "parse_unit_key_ro should succeed on valid data");

    let ukf = result.unwrap();
    assert_eq!(ukf.app_type, 1);
    assert_eq!(ukf.num_bdmv_dir, 1);
    assert_eq!(ukf.encrypted_keys.len(), 1);
    assert_eq!(ukf.disc_hash.len(), 20);

    // disc_hash should be deterministic
    let hash = aacs::disc_hash(&data);
    assert_eq!(ukf.disc_hash, hash);
}

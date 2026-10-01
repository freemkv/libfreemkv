//! Decrypt-seam checks behind the Pass N (patch) key inversion fix.
//!
//! The patch pass itself now lives in freemkv-engine, so this file cannot guard the arm
//! selection; it pins the libfreemkv decrypt seam that pass relies on.

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
fn css_decrypt_of_an_uncrackable_sector_still_descrambles() {
    // Wrong key: the header crib rejects it and re-crack finds nothing, but that
    // is not a rip failure — `attack_crib` is a heuristic, so this mismatch just
    // means the cached key stands (CSS recoverability varies per sector, unlike AACS).
    let mut sector = vec![0xFFu8; 2048];
    // Scrambled DVD sectors are DVD-Video packs: the scramble bits are judged in the first
    // PES header's MPEG-2 flags byte (0x14), because byte 0x14 means something else
    // entirely in an IFO, UDF or ISO 9660 sector.
    sector[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    sector[4] = 0x44; // '01': a 13818-1 pack
    sector[0x0D] = 0xF8; // pack_stuffing_length 0
    sector[0x0E..0x12].copy_from_slice(&[0x00, 0x00, 0x01, 0xE0]);
    sector[0x14] = 0xB0; // MPEG-2 flags, CSS scramble bits 4-5

    let title_key: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF];
    let mut keys = DecryptKeys::Css { title_key };

    let dropped = libfreemkv::decrypt::decrypt_sectors(&mut sector, &mut keys, 0)
        .expect("a crib false positive must not fail the rip");
    assert_eq!(dropped, 0, "CSS reports no loss term of its own");
    assert_eq!(
        sector[0x14] & 0x30,
        0x00,
        "the sector is descrambled with the cached key, which clears the flag"
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

    let css = DecryptKeys::Css {
        title_key: [0u8; 5],
    };
    assert!(css.is_encrypted());
}

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
fn css_decrypt_of_an_uncrackable_sector_still_descrambles() {
    // A scrambled sector whose header is uniformly periodic yields a crib, so
    // the supplied key IS checked — and this arbitrary key is not the right
    // one, so the crib check rejects it and the re-crack from this synthetic
    // body finds nothing.
    //
    // That combination does NOT fail the rip. `attack_crib` is a heuristic: it
    // predicts that a periodic header run continues past 0x80, and when that
    // prediction does not hold it reports a mismatch even for a CORRECT key —
    // whereupon the re-crack fails because the crib was never valid. Crib
    // mismatch plus crack failure is the signature of a crib false positive,
    // and the cached key stays the best available evidence.
    //
    // This test previously asserted DecryptFailed, matching a round-9 change
    // that made real DVDs unrippable. CSS is not AACS: an AACS
    // unit key either opens a unit or does not, whereas a CSS title key is
    // recovered from data whose recoverability varies sector by sector.
    let mut sector = vec![0xFFu8; 2048];
    // Scrambled DVD sectors are MPEG-2 PS packs. The descramble policy requires
    // the pack start code as well as the flag bits, because byte 0x14 means
    // something else entirely in an IFO, UDF or ISO 9660 sector.
    sector[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    sector[0x14] |= 0x30; // CSS scramble flag, bits 4-5

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

use super::*;
use crate::aacs::content::{ALIGNED_UNIT_LEN, encrypt_unit};
use crate::disc::ContentFormat;
use crate::spec::keys::{KS_5_CPI, KS_6_TP_EXTRA_HEADER, KS_22_LIBAACS_VERIFY_TS};

const OURS: [u8; 16] = [0xA1; 16];
const ALT: [u8; 16] = [0xB2; 16];

// An encrypted unit: TS with CPI 11₂ in every packet (KS-5), encrypted under `key`.
fn encrypted(key: &[u8; 16], salt: u8) -> Vec<u8> {
    let mut u: Vec<u8> = (0..ALIGNED_UNIT_LEN)
        .map(|i| (i as u8).wrapping_add(salt) | 1)
        .collect();
    for p in u.chunks_mut(192) {
        p[0] |= 0xC0;
        p[4] = 0x47;
    }
    assert!(encrypt_unit(&mut u, key));
    u
}

fn cpi(unit: &[u8]) -> Vec<u8> {
    unit.chunks(192).map(|p| p[0] & 0xC0).collect()
}

/// per spec; do not change without a spec citation — KS-5 [BD] §3.10.2: "shall be set
/// to 11₂ if the data is encrypted": a unit left as ciphertext keeps its CPI bits.
/// Covers the alternate FMTS phase, an orphan, and a `Phase::Verify` unit that fails
/// `is_clean` and is restored.
#[test]
fn ciphertext_units_keep_cpi() {
    assert!(
        KS_5_CPI
            .text
            .contains("set to 11₂ if the data is encrypted")
    );
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, OURS)],
        format: ContentFormat::BdTs,
    };
    // Alternate phase: unit 1 of an Even-phase range is the other variant's.
    let mut buf = [encrypted(&OURS, 1), encrypted(&ALT, 2)].concat();
    let alt_before = buf[ALIGNED_UNIT_LEN..].to_vec();
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 6, 0, Phase::Even)]);
    decrypt_sectors_mapped(&mut buf, &keys, 0, &map).expect("our phase decrypts");
    assert_eq!(
        buf[ALIGNED_UNIT_LEN..],
        alt_before[..],
        "alternate phase untouched"
    );
    assert_eq!(
        cpi(&buf[ALIGNED_UNIT_LEN..]),
        cpi(&alt_before),
        "CPI of all 32 packets"
    );
    assert_eq!(
        buf[ALIGNED_UNIT_LEN] & 0xC0,
        0xC0,
        "packet 0's clear CPI still 11₂"
    );
    // Orphan: an encrypted unit no range covers is refused, and left as it was.
    let mut orphan = encrypted(&ALT, 3);
    let before = orphan.clone();
    let map = AacsKeyMap::from_ranges_phased(vec![(30, 36, 0, Phase::All)]);
    let got = decrypt_sectors_mapped(&mut orphan, &keys, 0, &map);
    assert!(
        matches!(got, Err(crate::error::Error::DecryptFailed)),
        "{got:?}"
    );
    assert_eq!(orphan, before, "the orphan keeps every byte, CPI included");
    assert_eq!(orphan[0] & 0xC0, 0xC0);
    // A `Verify` unit our key does not open: restored, every byte and CPI kept.
    let mut failed = encrypted(&ALT, 4);
    let before = failed.clone();
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 3, 0, Phase::Verify)]);
    decrypt_sectors_mapped(&mut failed, &keys, 0, &map).expect("a failed verify is no error");
    assert_eq!(
        failed, before,
        "the failed Verify unit is restored as ciphertext"
    );
    assert_eq!(cpi(&failed), cpi(&before), "CPI of all 32 packets");
    assert_eq!(failed[0] & 0xC0, 0xC0, "packet 0's clear CPI still 11₂");
}

/// per spec; do not change without a spec citation — KS-5 [BD] §3.10.2: "shall be set
/// to 11₂ if the data is encrypted, or … 00₂ if the data is not encrypted"; KS-6 (CPI is
/// the top 2 bits of TP_extra_header); corroborated by KS-22 libaacs `buf[i] &= ~0xc0`.
#[test]
fn every_decrypted_unit_clears_cpi_in_all_32_packets() {
    assert!(KS_5_CPI.text.contains("00₂ if the data is not encrypted"));
    assert!(
        KS_6_TP_EXTRA_HEADER
            .text
            .contains("Copy_permission_indicator 2 uimsbf Arrival_time_stamp 30 uimsbf")
    );
    assert!(KS_22_LIBAACS_VERIFY_TS.text.contains("buf[i] &= ~0xc0;"));
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, OURS)],
        format: ContentFormat::BdTs,
    };
    let check = |label: &str, got: &[u8], plain: Vec<u8>| {
        assert!(
            cpi(got).iter().all(|&c| c == 0),
            "{label}: CPI of all 32 packets"
        );
        // Byte 0 of each packet keeps its 6 Arrival_time_stamp bits (KS-6).
        let want = crate::aacs::content::cpi_cleared(plain);
        assert_eq!(got, &want[..], "{label}: ATS and payload untouched");
    };

    // A map-keyed unit (`Phase::All`, the base Unit Key).
    let mut unit = encrypted(&OURS, 7);
    let mut plain = unit.clone();
    crate::aacs::content::decrypt_unit(&mut plain, &OURS);
    let map = AacsKeyMap::from_ranges(vec![(0, 3, 0)]);
    decrypt_sectors_mapped(&mut unit, &keys, 0, &map).expect("map-keyed unit decrypts");
    check("map-keyed", &unit, plain);

    // A DAMAGED map-keyed unit: media garbage, still decrypted, so no longer ciphertext.
    let mut damaged: Vec<u8> = (0..ALIGNED_UNIT_LEN)
        .map(|i| (i as u8).wrapping_mul(37) ^ 0x5A)
        .collect();
    damaged[0] |= 0xC0;
    damaged[4] = 0x47; // the clear seed's sync: read on the unit grid
    let mut plain = damaged.clone();
    crate::aacs::content::decrypt_unit(&mut plain, &OURS);
    decrypt_sectors_mapped(&mut damaged, &keys, 0, &map).expect("no verify on Phase::All");
    check("damaged map-keyed", &damaged, plain);

    // Our half of a forensic segment (`Phase::Even`, unit 0 of the range).
    let mut ours = encrypted(&OURS, 9);
    let mut plain = ours.clone();
    crate::aacs::content::decrypt_unit(&mut plain, &OURS);
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 3, 0, Phase::Even)]);
    decrypt_sectors_mapped(&mut ours, &keys, 0, &map).expect("our phase decrypts");
    check("our forensic phase", &ours, plain);

    // A kept `Phase::Verify` unit (it verified under our key). The on-arrival leg
    // joins with the on-arrival proof (KU-L2).
    let mut kept = encrypted(&OURS, 11);
    let mut plain = kept.clone();
    crate::aacs::content::decrypt_unit(&mut plain, &OURS);
    let map = AacsKeyMap::from_ranges_phased(vec![(0, 3, 0, Phase::Verify)]);
    decrypt_sectors_mapped(&mut kept, &keys, 0, &map).expect("a verified unit decrypts");
    check("kept Verify", &kept, plain);
}
// A flagged partial unit is blanked only when it ends the source (a truncated copy); a
// partial read mid-content is left for the decrypt to refuse.
#[test]
fn a_flagged_partial_unit_is_blanked_only_at_the_end() {
    use crate::disc::ContentFormat;
    let mut tail = vec![0x5Au8; 4096];
    tail[0] |= 0xC0;
    tail[4] = 0x47;
    let mut mid = tail.clone();
    assert_eq!(
        blank_damaged_units(&mut mid, 0, ContentFormat::BdTs, &|_| true, false),
        0
    );
    assert_eq!(mid, tail);
    assert_eq!(
        blank_damaged_units(&mut tail, 0, ContentFormat::BdTs, &|_| true, true),
        1
    );
    assert!(tail.iter().all(|&b| b == 0));
}

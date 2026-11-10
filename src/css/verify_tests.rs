use super::*;
use crate::css::{checkable_sectors, dvd_pack_header};

const KEY: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF];

fn chunks(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    buf.chunks(2048)
}

#[test]
fn the_right_key_passes() {
    let (wire, _) = checkable_sectors(&KEY, 4, 1);
    assert_eq!(verify_key(&KEY, chunks(&wire)), Verdict::Pass);
}

#[test]
fn a_wrong_key_never_passes() {
    let (wire, _) = checkable_sectors(&KEY, 8, 2);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..2000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut key = [0u8; 5];
        key.copy_from_slice(&state.to_be_bytes()[..5]);
        if key == KEY {
            continue;
        }
        assert_ne!(verify_key(&key, chunks(&wire)), Verdict::Pass, "{key:02x?}");
    }
}

// A pack with no structure a key can be checked against: the PES fills the pack, its
// payload starts in the clear, it has no crib and its plaintext is start-code-free noise.
#[test]
fn uncheckable_packs_are_inconclusive() {
    let mut buf = Vec::new();
    for n in 0..8u8 {
        let mut plain = vec![0u8; 2048];
        for (i, b) in plain.iter_mut().enumerate() {
            *b = ((i * 37 + 11 + usize::from(n)) % 251) as u8 | 1;
        }
        plain[0x14] = 0;
        dvd_pack_header(&mut plain, 0xE0);
        plain[0x15..0x17].fill(0);
        let mut wire = plain;
        lfsr::scramble_sector(&KEY, &mut wire);
        assert!(keyless::attack_crib(&wire).is_none());
        buf.extend_from_slice(&wire);
    }
    assert_eq!(verify_key(&KEY, chunks(&buf)), Verdict::Inconclusive);
    assert_eq!(
        verify_key(&[1, 2, 3, 4, 5], chunks(&buf)),
        Verdict::Inconclusive
    );
}

#[test]
fn clear_packs_and_too_few_checks_are_inconclusive() {
    let (_, plain) = checkable_sectors(&KEY, 8, 3);
    assert_eq!(verify_key(&KEY, chunks(&plain)), Verdict::Inconclusive);
    // One AC-3 pack: a single passing check is below MIN_PASSES.
    let (wire, _) = checkable_sectors(&KEY, 2, 4);
    assert_eq!(
        verify_key(&KEY, chunks(&wire[2048..])),
        Verdict::Inconclusive
    );
}

// Each check fails a key whose plaintext breaks it, even among passing packs.
#[test]
fn one_broken_check_fails_the_key() {
    for (offset, byte) in [(0x700, 0x7F), (0x300, 0x0C), (0x200 + 3, 0xB9)] {
        let (_, mut plain) = checkable_sectors(&KEY, 8, 5);
        // Packs 0 and 1 are video and AC-3; break the one this offset belongs to.
        let pack = usize::from(offset == 0x300);
        plain[pack * 2048 + offset] = byte;
        let mut wire = plain;
        for s in wire.chunks_mut(2048) {
            s[0x14] &= 0xCF;
            lfsr::scramble_sector(&KEY, s);
        }
        assert_eq!(
            verify_key(&KEY, chunks(&wire)),
            Verdict::Fail,
            "{offset:#x}"
        );
    }
}

#[test]
fn a_crib_match_passes_and_a_mismatch_is_no_verdict() {
    let mut plain = vec![0u8; 2048];
    plain[0x14] = 0;
    dvd_pack_header(&mut plain, 0xE0);
    for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
        *b = (i % 4) as u8 | 0x10;
    }
    let mut wire = plain.clone();
    lfsr::scramble_sector(&KEY, &mut wire);
    assert!(keyless::attack_crib(&wire).is_some());
    let four = wire.repeat(MIN_PASSES);
    assert_eq!(verify_key(&KEY, chunks(&four)), Verdict::Pass);
    assert_eq!(
        verify_key(&[9, 9, 9, 9, 9], chunks(&four)),
        Verdict::Inconclusive
    );
}

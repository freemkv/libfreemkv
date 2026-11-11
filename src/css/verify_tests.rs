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

// Scramble each 2048-byte plaintext pack under `key`.
fn scramble(plain: &[u8], key: &[u8; 5]) -> Vec<u8> {
    let mut wire = plain.to_vec();
    for s in wire.chunks_mut(2048) {
        s[0x14] &= 0xCF;
        lfsr::scramble_sector(key, s);
    }
    wire
}

// Pseudo-random keys other than KEY.
fn wrong_keys(n: usize) -> impl Iterator<Item = [u8; 5]> {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    std::iter::repeat_with(move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut key = [0u8; 5];
        key.copy_from_slice(&state.to_be_bytes()[..5]);
        key
    })
    .filter(|k| *k != KEY)
    .take(n)
}

// A video pack whose PES fills it, start-code-free non-zero noise from 0x17 on.
fn noise_video_pack(n: u8) -> Vec<u8> {
    let mut plain = vec![0u8; 2048];
    for (i, b) in plain.iter_mut().enumerate() {
        *b = ((i * 37 + 11 + usize::from(n)) % 251) as u8 | 1;
    }
    plain[0x14] = 0;
    dvd_pack_header(&mut plain, 0xE0);
    plain[0x15..0x17].fill(0);
    plain
}

// A start code whose `00 00 01` lies in the clear bytes holds under any key: only its value
// byte is decrypted, so it can fail a key but never count towards one.
#[test]
fn a_start_code_prefix_in_the_clear_bytes_never_passes_a_key() {
    let mut plain = Vec::new();
    for n in 0..MIN_PASSES as u8 {
        let mut p = noise_video_pack(n);
        p[0x7D..0x81].copy_from_slice(&[0, 0, 1, 0x00]);
        plain.extend_from_slice(&p);
    }
    let wire = scramble(&plain, &KEY);
    assert!(wire.chunks(2048).all(|s| keyless::attack_crib(s).is_none()));
    assert_ne!(verify_key(&KEY, chunks(&wire)), Verdict::Fail);
    for key in wrong_keys(500) {
        assert_ne!(verify_key(&key, chunks(&wire)), Verdict::Pass, "{key:02x?}");
    }
}

#[test]
fn a_dts_substream_is_checked_at_its_sync_word() {
    let (_, mut plain) = checkable_sectors(&KEY, 8, 6);
    for s in plain.chunks_mut(2048).skip(1).step_by(2) {
        s[0x17] = 0x88;
        s[0x300..0x304].copy_from_slice(&[0x7F, 0xFE, 0x80, 0x01]);
    }
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&plain, &KEY))),
        Verdict::Pass
    );
    // The AC-3 sync word is no DTS sync word.
    let mut ac3 = plain.clone();
    ac3[2048 + 0x300..2048 + 0x304].copy_from_slice(&[0x0B, 0x77, 0x01, 0x02]);
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&ac3, &KEY))),
        Verdict::Fail
    );
    plain[2048 + 0x302] = 0x81;
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&plain, &KEY))),
        Verdict::Fail
    );
}

// With data_alignment_indicator, a payload past the clear bytes must open with a start code.
#[test]
fn an_aligned_video_payload_past_the_clear_bytes_opens_with_a_start_code() {
    let mut plain = Vec::new();
    for n in 0..4u8 {
        let mut p = noise_video_pack(n);
        p[0x14] |= 0x04;
        p[0x16] = 0x70; // header data: payload at 0x87
        p[0x87..0x8B].copy_from_slice(&[0, 0, 1, 0xB3]);
        plain.extend_from_slice(&p);
    }
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&plain, &KEY))),
        Verdict::Pass
    );
    plain[0x89] = 0x02;
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&plain, &KEY))),
        Verdict::Fail
    );
}

// The packet after the PES needs a stream_id of at least 0xBC and a length that fits the pack.
#[test]
fn the_next_packet_needs_a_valid_stream_id_and_a_length_that_fits() {
    let (_, plain) = checkable_sectors(&KEY, 8, 7);
    let fits = (0x800u16 - 0x706).to_be_bytes();
    let over = (0x800u16 - 0x706 + 1).to_be_bytes();
    for (offset, bytes) in [(0x703, &[0xBB][..]), (0x704, &over[..])] {
        let mut broken = plain.clone();
        broken[offset..offset + bytes.len()].copy_from_slice(bytes);
        let wire = scramble(&broken, &KEY);
        assert_eq!(
            verify_key(&KEY, chunks(&wire)),
            Verdict::Fail,
            "{offset:#x}"
        );
    }
    let mut lowest = plain.clone();
    lowest[0x703] = 0xBC;
    lowest[0x704..0x706].copy_from_slice(&fits);
    assert_eq!(
        verify_key(&KEY, chunks(&scramble(&lowest, &KEY))),
        Verdict::Pass
    );
}

// A PES length running past the pack is damaged metadata: no verdict, never a panic.
#[test]
fn a_pes_length_past_the_pack_is_no_verdict() {
    let (_, mut plain) = checkable_sectors(&KEY, 8, 8);
    for s in plain.chunks_mut(2048) {
        s[0x12..0x14].copy_from_slice(&0xFFFFu16.to_be_bytes());
    }
    let wire = scramble(&plain, &KEY);
    assert_eq!(verify_key(&KEY, chunks(&wire)), Verdict::Inconclusive);
    assert_eq!(
        verify_key(&[1, 2, 3, 4, 5], chunks(&wire)),
        Verdict::Inconclusive
    );
}

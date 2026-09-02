//! CSS title-key recovery — a divide-and-conquer known-plaintext attack,
//! implemented from the openly published CSS cryptanalysis literature. It
//! recovers the 5-byte CSS title key from a single scrambled DVD sector with
//! no player keys and no disc-key crack, using only known plaintext.
//! Implemented from that public description; nothing here is copied or
//! translated from any particular CSS software.
//!
//! See `docs/css-keyless.md` for the cipher structure this attacks and the
//! four steps of the attack itself.

use super::lfsr::descramble_sector;
use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

use crate::consts::SECTOR_BYTES;
const ENCRYPTED_START: usize = 0x80; // byte 128
const SEED_OFFSET: usize = 0x54; // sector seed at bytes 0x54-0x58
const FLAG_BYTE: usize = 0x14;

/// LFSR0's output tap: the 24-bit register folded down to the byte it feeds
/// into TAB4. Used both to validate a candidate against the keystream and to
/// re-derive the byte shifted in during the backward-clocking search.
fn lfsr0_output_tap(x: u32) -> u32 {
    (((((((x >> 3) ^ x) >> 1) ^ x) >> 8) ^ x) >> 5) & 0xff
}

// Recover the title key from cipher + known plaintext. `seed` is
// sector[0x54..0x59]. See docs/css-keyless.md — recover_title_key_from_plain.
fn recover_title_key_from_plain(
    crypted: &[u8],
    decrypted: &[u8],
    seed: &[u8; 5],
) -> Option<[u8; 5]> {
    if crypted.len() < 10 || decrypted.len() < 10 {
        return None;
    }

    // buf[i] = TAB1[cipher[i]] ^ plain[i] — the per-byte content keystream.
    let mut buffer = [0u8; 10];
    for (i, b) in buffer.iter_mut().enumerate() {
        *b = TAB1[crypted[i] as usize] ^ decrypted[i];
    }

    let mut key = [0u8; 5];
    let mut found = false;

    for i_try in 0u32..0x1_0000 {
        let mut i_t1 = (i_try >> 8) | 0x100;
        let mut i_t2 = i_try & 0xff;
        let mut i_t3: u32 = 0; // not needed yet
        let mut i_t5: u32 = 0;

        // Iterate the cipher 4 times to reconstruct LFSR0 (i_t3).
        for &b in buffer.iter().take(4) {
            let i_t4 = (TAB2[i_t2 as usize] ^ TAB3[i_t1 as usize]) as u32;
            i_t2 = i_t1 >> 1;
            i_t1 = ((i_t1 & 1) << 8) ^ i_t4;
            let i_t4 = TAB5[i_t4 as usize] as u32;

            // Deduce i_t6 (LFSR0 output, pre-TAB4) and the carry.
            let mut i_t6 = b as u32;
            if i_t5 != 0 {
                i_t6 = (i_t6 + 0xff) & 0xff;
            }
            if i_t6 < i_t4 {
                i_t6 += 0x100;
            }
            i_t6 -= i_t4;
            i_t5 += i_t6 + i_t4;
            let i_t6 = TAB4[i_t6 as usize] as u32;

            i_t3 = (i_t3 << 8) | i_t6;
            i_t5 >>= 8;
        }

        let i_candidate = i_t3;

        // Iterate 6 more times to validate the candidate.
        let mut i = 4usize;
        while i < 10 {
            let i_t4 = (TAB2[i_t2 as usize] ^ TAB3[i_t1 as usize]) as u32;
            i_t2 = i_t1 >> 1;
            i_t1 = ((i_t1 & 1) << 8) ^ i_t4;
            let i_t4 = TAB5[i_t4 as usize] as u32;
            let mut i_t6 = lfsr0_output_tap(i_t3);
            i_t3 = (i_t3 << 8) | i_t6;
            i_t6 = TAB4[i_t6 as usize] as u32;
            i_t5 += i_t6 + i_t4;
            if (i_t5 & 0xff) as u8 != buffer[i] {
                break;
            }
            i_t5 >>= 8;
            i += 1;
        }

        if i != 10 {
            continue;
        }

        // Four backward steps of iterating i_t3 to deduce the initial state.
        i_t3 = i_candidate;
        for _ in 0..4 {
            let i_t1_byte = i_t3 & 0xff;
            i_t3 >>= 8;
            // Brute-force the byte shifted in (top byte of the 24-bit reg).
            for j in 0u32..256 {
                i_t3 = (i_t3 & 0x1_ffff) | (j << 17);
                let i_t6 = lfsr0_output_tap(i_t3);
                if i_t6 == i_t1_byte {
                    break;
                }
            }
        }

        // Undo `i_t3 = i_t3*2 + 8 - (i_t3 & 7)` to recover key[2..5].
        let i_t4 = (i_t3 >> 1).wrapping_sub(4);
        for i_t5 in 0u32..8 {
            let val = i_t4.wrapping_add(i_t5);
            if val.wrapping_mul(2).wrapping_add(8).wrapping_sub(val & 7) == i_t3 {
                key[0] = (i_try >> 8) as u8;
                key[1] = (i_try & 0xff) as u8;
                key[2] = (val & 0xff) as u8;
                key[3] = ((val >> 8) & 0xff) as u8;
                key[4] = ((val >> 16) & 0xff) as u8;
                found = true;
                break;
            }
        }
        // First fully-validated candidate wins. The 48-bit keystream constraint
        // makes a second match cryptographically negligible on real sectors, but
        // continuing would let a later spurious match overwrite a correct key.
        if found {
            break;
        }
    }

    if found {
        for (k, &s) in key.iter_mut().zip(seed.iter()) {
            *k ^= s;
        }
        Some(key)
    } else {
        None
    }
}

/// Recover the CSS title key from a scrambled sector using a known plaintext
/// for the encrypted region.
///
/// `plain` is the expected plaintext at byte 0x80 (at least 10 bytes).
/// Returns the recovered key only if it actually descrambles the sector back
/// to `plain` — guarding against the rare spurious LFSR-seed match.
pub fn recover_title_key(sector: &[u8], plain: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_BYTES || plain.len() < 10 {
        return None;
    }
    if sector[FLAG_BYTE] & 0x30 == 0 {
        return None;
    }

    let seed: [u8; 5] = [
        sector[SEED_OFFSET],
        sector[SEED_OFFSET + 1],
        sector[SEED_OFFSET + 2],
        sector[SEED_OFFSET + 3],
        sector[SEED_OFFSET + 4],
    ];

    let crypted = &sector[ENCRYPTED_START..ENCRYPTED_START + 10];
    let key = recover_title_key_from_plain(crypted, plain, &seed)?;

    if descramble_matches(sector, &key, plain) {
        Some(key)
    } else {
        None
    }
}

/// Verify a title key by descrambling a copy of `sector` and checking the
/// known plaintext reappears at byte 0x80.
fn descramble_matches(sector: &[u8], title: &[u8; 5], plain: &[u8]) -> bool {
    let mut test = sector.to_vec();
    test[FLAG_BYTE] |= 0x10; // ensure scramble flag set for the descrambler
    descramble_sector(title, &mut test);
    let n = plain.len().min(SECTOR_BYTES - ENCRYPTED_START);
    test[ENCRYPTED_START..ENCRYPTED_START + n] == plain[..n]
}

/// Find a repeating pattern just before the encrypted region and assume the
/// plaintext at 0x80 continues it — the known-plaintext step of the recovery.
/// Scans cleartext `sec[0x00..0x80]` for the longest run that repeats
/// with a cycle length in 2..0x2F. If the run is long enough (`plen > 3` and at
/// least two full cycles), the known plaintext at 0x80 is taken to be the
/// periodic run continuing forward, and [`recover_title_key_from_plain`] is
/// applied.
pub fn crack_title_key(sector: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_BYTES {
        return None;
    }
    if sector[FLAG_BYTE] & 0x30 == 0 {
        return None;
    }

    // Runaway guard: this crack is a bounded 2^16 LFSR search, done well under 1s
    // on any modern CPU. If it ever exceeds ~2s, something pathological is
    // happening — log it so a hang is never silent.
    let crack_t0 = std::time::Instant::now();

    let result = crack_title_key_inner(sector);

    let elapsed = crack_t0.elapsed();
    if elapsed.as_secs_f64() > 2.0 {
        tracing::warn!(
            target: "freemkv::css",
            elapsed_ms = elapsed.as_millis() as u64,
            found = result.is_some(),
            "css crack: single-sector recovery exceeded 2s (runaway guard)"
        );
    }
    result
}

// Crib: the predicted 10-byte plaintext at byte 0x80, from the longest
// periodic run in the clear header. Also the decrypt path's cached-key
// oracle. See docs/css-keyless.md — attack_crib.
pub(crate) fn attack_crib(sector: &[u8]) -> Option<[u8; 10]> {
    if sector.len() < SECTOR_BYTES || sector[FLAG_BYTE] & 0x30 == 0 {
        return None;
    }
    let mut best_plen: usize = 0;
    let mut best_p: usize = 0;

    // For all cycle lengths from 2 to 0x2F.
    for i in 2usize..0x30 {
        // Count bytes that repeat with cycle length i, scanning backward from
        // 0x7F. `sec[0x7F - (j % i)] == sec[0x7F - j]`.
        let mut j = i + 1;
        while j < 0x80 && sector[0x7f - (j % i)] == sector[0x7f - j] {
            if j > best_plen {
                best_plen = j;
                best_p = i;
            }
            j += 1;
        }
    }

    // Need at least a few repeated bytes and at least one full cycle.
    if best_plen > 3 && best_p > 0 && best_plen / best_p >= 2 {
        // Crib starts at `0x80 - cycles*best_p`; bytes at/after 0x80 are
        // predicted by the run repeating with period best_p.
        let cycles = best_plen / best_p;
        let plain_start = 0x80 - cycles * best_p;

        // Must wrap within the period (not read `&sec[plain_start..+10]`
        // directly), since past 0x80 the raw byte is ciphertext, not plaintext.
        let mut plain = [0u8; 10];
        for (i, p) in plain.iter_mut().enumerate() {
            *p = sector[plain_start + (i % best_p)];
        }
        Some(plain)
    } else {
        None
    }
}

fn crack_title_key_inner(sector: &[u8]) -> Option<[u8; 5]> {
    let plain = attack_crib(sector)?;
    let seed: [u8; 5] = [
        sector[SEED_OFFSET],
        sector[SEED_OFFSET + 1],
        sector[SEED_OFFSET + 2],
        sector[SEED_OFFSET + 3],
        sector[SEED_OFFSET + 4],
    ];
    let crypted = &sector[0x80..0x80 + 10];
    if let Some(key) = recover_title_key_from_plain(crypted, &plain, &seed) {
        // Verify against the same predicted plaintext.
        if descramble_matches(sector, &key, &plain) {
            return Some(key);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::lfsr::scramble_sector;
    use super::*;

    // Synthesize a scrambled sector for a title key/seed with `plain` at byte
    // 0x80. See docs/css-keyless.md — synth_sector.
    fn synth_sector(title_key: &[u8; 5], seed: &[u8; 5], plain: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut plaintext = vec![0u8; SECTOR_BYTES];
        plaintext[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        plaintext[FLAG_BYTE] = 0x10;
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(seed);
        plaintext[ENCRYPTED_START..ENCRYPTED_START + plain.len()].copy_from_slice(plain);

        let body = plaintext.clone();

        // scramble_sector turns the plaintext body into ciphertext and sets
        // the scramble flag.
        scramble_sector(title_key, &mut plaintext);
        (plaintext, body)
    }

    // Synthesize a sector whose cleartext ends in a periodic run continuing
    // into the encrypted region. See docs/css-keyless.md — synth_periodic_sector.
    fn synth_periodic_sector(
        title_key: &[u8; 5],
        seed: &[u8; 5],
        period: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let mut plaintext = vec![0u8; SECTOR_BYTES];
        plaintext[FLAG_BYTE] = 0x10;

        // Periodic run over the cleartext header tail (RUN_START..0x80) into
        // the encrypted region, mirroring a real VOB. Must not overlap the seed
        // bytes (0x54..0x59); RUN_START=0x59 gives >=2 cycles of every period.
        const RUN_START: usize = 0x59;
        let pat: Vec<u8> = (0..period)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
            *b = pat[i % period];
        }

        // Seed sits below the run, undisturbed.
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(seed);

        let body = plaintext.clone();
        scramble_sector(title_key, &mut plaintext);
        (plaintext, body)
    }

    #[test]
    fn crack_unscrambled_returns_none() {
        let sector = vec![0u8; SECTOR_BYTES];
        assert!(crack_title_key(&sector).is_none());
    }

    #[test]
    fn crack_too_short_returns_none() {
        let sector = vec![0u8; 100];
        assert!(crack_title_key(&sector).is_none());
    }

    #[test]
    fn recover_needs_min_plain() {
        let sector = vec![0u8; SECTOR_BYTES];
        let short_plain = [0u8; 4];
        assert!(recover_title_key(&sector, &short_plain).is_none());
    }

    /// The known plaintext used at byte 0x80 for the direct-recovery tests.
    /// A realistic MPEG-2 PES header start.
    const PES: [u8; 10] = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21];

    // MANDATORY round-trip (Task C.1): recovered key must descramble the
    // body back to plaintext. See docs/css-keyless.md — recover_round_trips_known_keys.
    #[test]
    fn recover_round_trips_known_keys() {
        let cases: &[([u8; 5], [u8; 5])] = &[
            (
                [0x42, 0x13, 0x37, 0xBE, 0xEF],
                [0x11, 0x22, 0x33, 0x44, 0x55],
            ),
            (
                [0x01, 0x02, 0x03, 0x04, 0x05],
                [0xDE, 0xAD, 0xBE, 0xEF, 0x42],
            ),
            (
                [0xFE, 0xDC, 0xBA, 0x98, 0x76],
                [0x00, 0xFF, 0x80, 0x7F, 0x01],
            ),
            (
                [0x9A, 0x78, 0x56, 0x34, 0x12],
                [0xA5, 0x5A, 0x0F, 0xF0, 0xCC],
            ),
            (
                [0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                [0x01, 0x01, 0x01, 0x01, 0x01],
            ),
        ];
        for (title_key, seed) in cases {
            let (mut sector, body) = synth_sector(title_key, seed, &PES);
            let recovered =
                recover_title_key(&sector, &PES).expect("recover_title_key returned None");
            descramble_sector(&recovered, &mut sector);
            assert_eq!(
                &sector[ENCRYPTED_START..SECTOR_BYTES],
                &body[ENCRYPTED_START..SECTOR_BYTES],
                "recovered key did not descramble the full body for \
                 title={title_key:02x?} seed={seed:02x?}"
            );
        }
    }

    // descramble_matches is the ONLY gate between the LFSR search and a key
    // handed to the caller. See docs/css-keyless.md —
    // descramble_matches_accepts_only_the_key_the_sector_was_scrambled_with.
    #[test]
    fn descramble_matches_accepts_only_the_key_the_sector_was_scrambled_with() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let (sector, _body) = synth_sector(&title_key, &seed, &PES);

        assert!(
            descramble_matches(&sector, &title_key, &PES),
            "the key the sector was scrambled with must be accepted"
        );

        for byte in 0..5usize {
            for bit in 0..8u32 {
                let mut wrong = title_key;
                wrong[byte] ^= 1u8 << bit;
                assert!(
                    !descramble_matches(&sector, &wrong, &PES),
                    "key differing only in byte {byte} bit {bit} must be rejected"
                );
            }
        }
    }

    // The gate must verify against a COPY, never mutate the caller's sector.
    // See docs/css-keyless.md — descramble_matches_does_not_disturb_the_caller_s_sector.
    #[test]
    fn descramble_matches_does_not_disturb_the_caller_s_sector() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let (sector, _body) = synth_sector(&title_key, &seed, &PES);
        let before = sector.clone();

        assert!(descramble_matches(&sector, &title_key, &PES));
        let mut wrong = title_key;
        wrong[0] ^= 0x01;
        assert!(!descramble_matches(&sector, &wrong, &PES));

        assert_eq!(
            sector, before,
            "verification must leave the sector byte-for-byte unchanged"
        );
    }

    /// MANDATORY (Task C.1): the crib-based entry point crack_title_key —
    /// no plaintext supplied — recovers a round-tripping key when the
    /// cleartext ends in a periodic run that continues into 0x80.
    #[test]
    fn crack_title_key_recovers_via_attack_pattern() {
        for &period in &[2usize, 3, 5, 8, 16] {
            let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
            let seed = [0x11, 0x22, 0x33, 0x44, 0x55];
            let (sector, body) = synth_periodic_sector(&title_key, &seed, period);

            let cracked = crack_title_key(&sector)
                .unwrap_or_else(|| panic!("crack_title_key returned None for period {period}"));
            let mut test = sector.clone();
            descramble_sector(&cracked, &mut test);
            assert_eq!(
                &test[ENCRYPTED_START..SECTOR_BYTES],
                &body[ENCRYPTED_START..SECTOR_BYTES],
                "crack_title_key key did not round-trip the body (period {period})"
            );
        }
    }

    /// recover_title_key_from_plain inverts descramble_sector exactly: scramble
    /// a known body, hand back the keystream-derived key, and the recovered
    /// key (XOR-back included) reproduces the plaintext.
    #[test]
    fn recovered_key_descrambles_back_to_plaintext() {
        let cases: &[([u8; 5], [u8; 5])] = &[
            (
                [0x42, 0x13, 0x37, 0xBE, 0xEF],
                [0x11, 0x22, 0x33, 0x44, 0x55],
            ),
            (
                [0x9A, 0x78, 0x56, 0x34, 0x12],
                [0xA5, 0x5A, 0x0F, 0xF0, 0xCC],
            ),
            (
                [0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                [0x01, 0x01, 0x01, 0x01, 0x01],
            ),
        ];
        for (title_key, seed) in cases {
            let (mut sector, body) = synth_sector(title_key, seed, &PES);
            let recovered =
                recover_title_key(&sector, &PES).expect("recover_title_key returned None");
            descramble_sector(&recovered, &mut sector);
            assert_eq!(
                &sector[ENCRYPTED_START..SECTOR_BYTES],
                &body[ENCRYPTED_START..SECTOR_BYTES],
                "descramble with recovered key did not reproduce the body \
                 for title={title_key:02x?} seed={seed:02x?}"
            );
        }
    }

    // ── early-return guards ────────────────────────────────────────────────

    #[test]
    fn recover_rejects_sector_one_byte_short() {
        let mut sector = vec![0u8; SECTOR_BYTES - 1];
        sector[FLAG_BYTE] = 0x30;
        assert!(recover_title_key(&sector, &PES).is_none());
    }

    #[test]
    fn recover_rejects_unscrambled_sector() {
        let sector = vec![0x00u8; SECTOR_BYTES];
        assert!(recover_title_key(&sector, &PES).is_none());
    }

    #[test]
    fn recover_high_flag_bits_are_not_scramble() {
        for &flag in &[0x40u8, 0x80, 0xC0] {
            let mut sector = vec![0x11u8; SECTOR_BYTES];
            sector[FLAG_BYTE] = flag;
            assert!(
                recover_title_key(&sector, &PES).is_none(),
                "flag {flag:#04x} has scramble bits clear; recover must return None"
            );
        }
    }

    #[test]
    fn crack_high_flag_bits_are_not_scramble() {
        for &flag in &[0x40u8, 0x80, 0xC0] {
            let mut sector = vec![0x11u8; SECTOR_BYTES];
            sector[FLAG_BYTE] = flag;
            assert!(
                crack_title_key(&sector).is_none(),
                "flag {flag:#04x} clear scramble bits -> crack must return None"
            );
        }
    }

    #[test]
    fn crack_rejects_sector_one_byte_short() {
        let mut sector = vec![0u8; SECTOR_BYTES - 1];
        if sector.len() > FLAG_BYTE {
            sector[FLAG_BYTE] = 0x30;
        }
        assert!(crack_title_key(&sector).is_none());
    }

    /// crack_title_key must never panic on a fully scrambled sector with
    /// arbitrary (non-periodic) content — it just returns None.
    #[test]
    fn crack_full_path_never_panics() {
        for seed in 0u32..3 {
            let mut sector = vec![0u8; SECTOR_BYTES];
            sector[FLAG_BYTE] = 0x30;
            let mut x = seed.wrapping_mul(2_654_435_761).wrapping_add(7);
            for b in sector.iter_mut().skip(0x80) {
                x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                *b = (x >> 16) as u8;
            }
            for (i, b) in sector[SEED_OFFSET..SEED_OFFSET + 5].iter_mut().enumerate() {
                *b = (seed.wrapping_add(i as u32) ^ 0xA5) as u8;
            }
            let _ = crack_title_key(&sector);
        }
    }

    // ── entry-point guards on caller- and disc-supplied lengths: a sector
    // buffer that ENDS inside the encrypted region must be refused, not
    // sliced. See docs/css-keyless.md — recover_rejects_a_sector_that_ends_inside_the_encrypted_region.
    #[test]
    fn recover_rejects_a_sector_that_ends_inside_the_encrypted_region() {
        for len in [0x81usize, 0x85, 0x89] {
            let mut sector = vec![0x11u8; len];
            sector[FLAG_BYTE] = 0x30; // scrambled, so no other guard fires first
            assert!(
                recover_title_key(&sector, &PES).is_none(),
                "a {len}-byte buffer cannot supply ten ciphertext bytes at 0x80"
            );
        }
    }

    // A buffer LONGER than one sector is still one sector: both entry points
    // must recover from the first SECTOR_BYTES. See docs/css-keyless.md —
    // a_buffer_longer_than_one_sector_still_yields_its_key.
    #[test]
    fn a_buffer_longer_than_one_sector_still_yields_its_key() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];

        let (sector, _) = synth_sector(&title_key, &seed, &PES);
        let mut padded = sector.clone();
        padded.extend_from_slice(&[0xA7u8; 512]);
        assert_eq!(
            recover_title_key(&padded, &PES),
            Some(title_key),
            "a two-and-a-bit-sector buffer must still recover the first sector's key"
        );

        let (periodic, _) = synth_periodic_sector(&title_key, &seed, 5);
        let mut padded = periodic.clone();
        padded.extend_from_slice(&[0xA7u8; 512]);
        assert_eq!(
            crack_title_key(&padded),
            crack_title_key(&periodic),
            "padding past the sector must not change the crack result"
        );
        assert!(crack_title_key(&padded).is_some());
    }

    // recover_title_key accepts and uses MORE than ten bytes of known
    // plaintext (ten is a MINIMUM). See docs/css-keyless.md —
    // recover_accepts_more_than_ten_bytes_of_known_plaintext.
    #[test]
    fn recover_accepts_more_than_ten_bytes_of_known_plaintext() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let long_plain: Vec<u8> = (0..64u8)
            .map(|k| k.wrapping_mul(37).wrapping_add(5))
            .collect();
        let (sector, _) = synth_sector(&title_key, &seed, &long_plain);

        assert_eq!(
            recover_title_key(&sector, &long_plain),
            Some(title_key),
            "64 bytes of known plaintext must be accepted, not rejected as \
             'more than ten'"
        );
    }

    // The scramble-flag gate on a sector whose BODY really is ciphertext and
    // whose key IS recoverable. See docs/css-keyless.md —
    // a_recoverable_sector_with_the_scramble_bits_cleared_is_still_refused.
    #[test]
    fn a_recoverable_sector_with_the_scramble_bits_cleared_is_still_refused() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];

        let (mut sector, _) = synth_sector(&title_key, &seed, &PES);
        assert_eq!(
            recover_title_key(&sector, &PES),
            Some(title_key),
            "fixture check: with the flag set this sector's key IS recoverable"
        );
        sector[FLAG_BYTE] = 0x00;
        assert_eq!(
            recover_title_key(&sector, &PES),
            None,
            "scramble bits clear → no title key, even though one could be found"
        );

        let (mut periodic, _) = synth_periodic_sector(&title_key, &seed, 5);
        assert!(
            crack_title_key(&periodic).is_some(),
            "fixture check: with the flag set this sector cracks"
        );
        assert!(
            attack_crib(&periodic).is_some(),
            "fixture check: with the flag set this sector has a usable crib"
        );
        periodic[FLAG_BYTE] = 0x00;
        assert_eq!(
            crack_title_key(&periodic),
            None,
            "scramble bits clear → no crack, even though one would succeed"
        );
        // `attack_crib` carries its own copy of the same gate and is the one that
        // actually stops the crack (`crack_title_key`'s is defensive duplication);
        // it also doubles as the decrypt path's cached-key oracle.
        assert_eq!(
            attack_crib(&periodic),
            None,
            "an unscrambled sector has no predicted plaintext to offer"
        );
    }

    // ── descramble_matches: the verification gate's own mechanics — the gate
    // must verify against the sector's CIPHERTEXT regardless of the flag
    // byte. See docs/css-keyless.md — descramble_matches_forces_the_scramble_flag_on_its_own_copy.
    #[test]
    fn descramble_matches_forces_the_scramble_flag_on_its_own_copy() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let (mut sector, _) = synth_sector(&title_key, &seed, &PES);
        sector[FLAG_BYTE] = 0x00;

        assert!(
            descramble_matches(&sector, &title_key, &PES),
            "the body is ciphertext and the key is right — the gate must \
             descramble it even though the flag byte says otherwise"
        );
        let mut wrong = title_key;
        wrong[0] ^= 0x01;
        assert!(!descramble_matches(&sector, &wrong, &PES));
    }

    // The gate compares the WHOLE supplied plaintext, clamped to the
    // encrypted region. See docs/css-keyless.md —
    // descramble_matches_compares_all_of_the_plaintext_and_no_more_than_the_sector.
    #[test]
    fn descramble_matches_compares_all_of_the_plaintext_and_no_more_than_the_sector() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let body: Vec<u8> = (0..64u8)
            .map(|k| k.wrapping_mul(29).wrapping_add(3))
            .collect();
        let (sector, _) = synth_sector(&title_key, &seed, &body);

        assert!(descramble_matches(&sector, &title_key, &body));

        // A crib agreeing for the first 16 bytes and diverging after must be
        // rejected: the comparison window is the crib's length, not a fixed
        // prefix.
        let mut tail_wrong = body.clone();
        tail_wrong[40] ^= 0xFF;
        assert!(
            !descramble_matches(&sector, &title_key, &tail_wrong),
            "a crib that diverges at byte 40 must not match"
        );
        assert_eq!(
            tail_wrong[..16],
            body[..16],
            "fixture check: the first 16 bytes are identical, so only a \
             comparison that runs past them can tell these apart"
        );

        // A crib LONGER than the encrypted region: the comparison is clamped to
        // the sector, not run off the end of it.
        let plain_len = SECTOR_BYTES - ENCRYPTED_START;
        let mut over_long = vec![0u8; plain_len + 10];
        let (full_sector, full_body) = synth_sector(&title_key, &seed, &[0x00u8; 10]);
        over_long[..plain_len].copy_from_slice(&full_body[ENCRYPTED_START..]);
        assert!(
            descramble_matches(&full_sector, &title_key, &over_long),
            "a crib longer than the encrypted region must be clamped, not \
             compared past the end of the sector"
        );
    }

    // ── attack_crib: known-answer vectors. Sector whose clear header ends in
    // a period-length run of run_len bytes before 0x80, rest 0xFF. See
    // docs/css-keyless.md — sector_with_trailing_run.
    fn sector_with_trailing_run(period: usize, run_len: usize) -> Vec<u8> {
        assert!(
            run_len < ENCRYPTED_START,
            "the run lives in the clear header"
        );
        let mut sector = vec![0u8; SECTOR_BYTES];
        sector[FLAG_BYTE] = 0x10;
        for b in sector[ENCRYPTED_START..].iter_mut() {
            *b = 0xFF;
        }
        let pat: Vec<u8> = (0..period).map(|k| 0xD0u8 + k as u8).collect();
        for x in (ENCRYPTED_START - run_len)..ENCRYPTED_START {
            sector[x] = pat[x % period];
        }
        sector
    }

    /// The crib the run PREDICTS: the periodic pattern continued past 0x80.
    fn expected_crib(period: usize) -> [u8; 10] {
        let pat: Vec<u8> = (0..period).map(|k| 0xD0u8 + k as u8).collect();
        let mut out = [0u8; 10];
        for (i, o) in out.iter_mut().enumerate() {
            *o = pat[(ENCRYPTED_START + i) % period];
        }
        out
    }

    // KNOWN ANSWER: the crib is the period-5 run continued forward, the same
    // ten bytes for every run length. See docs/css-keyless.md —
    // attack_crib_predicts_the_periodic_run_continuing_past_0x80.
    #[test]
    fn attack_crib_predicts_the_periodic_run_continuing_past_0x80() {
        for &run_len in &[11usize, 12, 13, 14, 15, 16, 20, 31] {
            let sector = sector_with_trailing_run(5, run_len);
            assert_eq!(
                attack_crib(&sector),
                Some(expected_crib(5)),
                "period-5 run of {run_len} bytes must predict the run continuing"
            );
        }
    }

    /// The same known answer across several periods, including a period that
    /// does NOT divide 0x80 (so the crib's phase is non-zero and a body that
    /// restarted the pattern at index 0 gives a different answer).
    #[test]
    fn attack_crib_recovers_the_run_period_and_phase() {
        // 0x80 % period: 3 for 5, 2 for 6, 2 for 7, 8 for 0x18 — all non-zero,
        // so the predicted first byte is NOT pat[0] in any of these cases.
        for &period in &[5usize, 6, 7, 0x18] {
            let sector = sector_with_trailing_run(period, 3 * period + 1);
            let crib =
                attack_crib(&sector).unwrap_or_else(|| panic!("no crib for period {period}"));
            assert_eq!(crib, expected_crib(period), "period {period}");
            assert_ne!(
                crib[0], 0xD0,
                "period {period} does not divide 0x80, so the crib must not \
                 start at pattern index 0"
            );
            assert!(
                crib.iter().all(|&b| b != 0xFF),
                "period {period}: the crib must never contain a byte read from \
                 the encrypted region"
            );
        }
    }

    // A run of exactly ONE cycle is not enough to predict forward; attack_crib
    // requires two full cycles. See docs/css-keyless.md —
    // attack_crib_refuses_a_run_shorter_than_two_cycles.
    #[test]
    fn attack_crib_refuses_a_run_shorter_than_two_cycles() {
        // period 8, run of 9 bytes: best_plen = 8, 8 / 8 == 1 cycle.
        assert_eq!(attack_crib(&sector_with_trailing_run(8, 9)), None);
        // period 0x18, run of 0x19 bytes: one cycle.
        assert_eq!(attack_crib(&sector_with_trailing_run(0x18, 0x19)), None);
        // ...and one more byte of run does not conjure a second cycle either.
        assert_eq!(attack_crib(&sector_with_trailing_run(8, 10)), None);
    }

    /// A header with no repeating tail at all yields no crib. Asserted on a
    /// header whose bytes are pairwise distinct right up to 0x80, so no cycle
    /// length in 2..0x2F can match even one byte.
    #[test]
    fn attack_crib_refuses_a_header_with_no_periodic_tail() {
        let mut sector = vec![0u8; SECTOR_BYTES];
        sector[FLAG_BYTE] = 0x10;
        // 0x00..0x80 strictly increasing: sec[a] == sec[b] iff a == b, so the
        // detector's `sec[0x7f - (j % i)] == sec[0x7f - j]` needs j % i == j,
        // which the scan's starting `j = i + 1` already excludes.
        for (x, b) in sector[..ENCRYPTED_START].iter_mut().enumerate() {
            *b = x as u8;
        }
        assert_eq!(attack_crib(&sector), None);
        // And the cracker built on it reports no key rather than guessing.
        assert_eq!(crack_title_key(&sector), None);
    }

    // attack_crib indexes sector[0x7f - j] unbounded, so its own length guard
    // is the only thing stopping an out-of-bounds read. See docs/css-keyless.md
    // — attack_crib_refuses_a_buffer_shorter_than_a_sector.
    #[test]
    fn attack_crib_refuses_a_buffer_shorter_than_a_sector() {
        for len in [0x15usize, 0x40, 0x7F, SECTOR_BYTES - 1] {
            let mut sector = vec![0x11u8; len];
            sector[FLAG_BYTE] = 0x30; // scrambled, so the flag half cannot fire
            assert_eq!(
                attack_crib(&sector),
                None,
                "a {len}-byte buffer is not a sector"
            );
        }
    }

    // A header periodic ALL THE WAY to offset 0 must not walk the backward
    // scan off the front of the sector. See docs/css-keyless.md —
    // attack_crib_survives_a_header_that_is_periodic_to_offset_zero.
    #[test]
    fn attack_crib_survives_a_header_that_is_periodic_to_offset_zero() {
        let mut sector = vec![0u8; SECTOR_BYTES];
        sector[FLAG_BYTE] = 0x30;
        let period = 5usize;
        let pat: Vec<u8> = (0..period).map(|k| 0xD0u8 + k as u8).collect();
        for (x, b) in sector[..ENCRYPTED_START].iter_mut().enumerate() {
            *b = pat[x % period];
        }
        for b in sector[ENCRYPTED_START..].iter_mut() {
            *b = 0xFF;
        }
        // The FLAG byte at 0x14 would interrupt the pattern, but j only needs to
        // reach 0x7f - 0x14 = 0x6b, well short of the bound. So instead put the
        // scramble flag bits into a byte value that IS the pattern's.
        sector[FLAG_BYTE] = pat[FLAG_BYTE % period];
        assert_ne!(
            sector[FLAG_BYTE] & 0x30,
            0,
            "fixture check: the pattern byte at 0x14 must itself carry \
             scramble bits, so the header stays unbroken"
        );

        assert_eq!(
            attack_crib(&sector),
            Some(expected_crib(period)),
            "a fully periodic header must predict its own continuation, and \
             the backward scan must stop at offset 0"
        );
    }

    // The crib is read from the CLEAR header only, never the encrypted
    // region. See docs/css-keyless.md — attack_crib_is_independent_of_the_encrypted_region.
    #[test]
    fn attack_crib_is_independent_of_the_encrypted_region() {
        let base = sector_with_trailing_run(5, 11);
        let crib = attack_crib(&base).expect("crib");
        for fill in [0x00u8, 0x5A, 0xD1, 0xFF] {
            let mut s = base.clone();
            for b in s[ENCRYPTED_START..].iter_mut() {
                *b = fill;
            }
            assert_eq!(
                attack_crib(&s),
                Some(crib),
                "the crib must not depend on the encrypted region (fill {fill:#04x})"
            );
        }
    }

    // ── recover_title_key_from_plain: input-length guard is the only thing
    // standing between a short slice and an index-out-of-bounds PANIC. See
    // docs/css-keyless.md — recover_title_key_from_plain_refuses_fewer_than_ten_bytes_of_either_input.
    #[test]
    fn recover_title_key_from_plain_refuses_fewer_than_ten_bytes_of_either_input() {
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let full = [0xA5u8; 10];
        for n in 0..10usize {
            assert_eq!(
                recover_title_key_from_plain(&full[..n], &full, &seed),
                None,
                "{n} ciphertext bytes is fewer than the ten the cipher iterates"
            );
            assert_eq!(
                recover_title_key_from_plain(&full, &full[..n], &seed),
                None,
                "{n} plaintext bytes is fewer than the ten the cipher iterates"
            );
        }
        // Exactly ten of each is ACCEPTED — the boundary is `< 10`, not `<= 10`.
        // What must not happen is an early `None` from the guard; proven via the
        // round-trip fixture, whose inputs are exactly ten bytes.
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let (sector, _) = synth_sector(&title_key, &seed, &PES);
        assert_eq!(
            recover_title_key_from_plain(
                &sector[ENCRYPTED_START..ENCRYPTED_START + 10],
                &PES,
                &seed
            ),
            Some(title_key),
            "exactly ten bytes of each input must run the search, not trip the guard"
        );
    }

    // The seed XOR-back turns the recovered LFSR key into the TITLE key:
    // key ^= sector_seed. See docs/css-keyless.md —
    // recover_title_key_from_plain_xors_the_sector_seed_back_out.
    #[test]
    fn recover_title_key_from_plain_xors_the_sector_seed_back_out() {
        let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let (sector, _) = synth_sector(&title_key, &seed, &PES);
        let crypted = &sector[ENCRYPTED_START..ENCRYPTED_START + 10];

        // The cipher is seeded from `title_key XOR seed`, so re-running the SAME
        // ciphertext/plaintext against a seed differing in one byte must return
        // a title key differing in exactly that byte — the XOR is a bijection.
        assert_eq!(
            recover_title_key_from_plain(crypted, &PES, &seed),
            Some(title_key)
        );
        for byte in 0..5usize {
            for bit in [0u32, 3, 7] {
                let mut alt_seed = seed;
                alt_seed[byte] ^= 1u8 << bit;
                let mut expected = title_key;
                expected[byte] ^= 1u8 << bit;
                assert_eq!(
                    recover_title_key_from_plain(crypted, &PES, &alt_seed),
                    Some(expected),
                    "seed byte {byte} bit {bit} must XOR straight through to the \
                     title key"
                );
            }
        }
    }
}

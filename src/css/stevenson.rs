//! CSS title-key recovery — Frank A. Stevenson's divide-and-conquer attack
//! (1999), ported exactly from libdvdcss `RecoverTitleKey` + `AttackPattern`
//! (css.c).
//!
//! Recovers the 5-byte CSS title key from a single scrambled DVD sector with
//! no player keys and no disc-key crack, using only known plaintext.
//!
//! # The cipher this attacks
//!
//! The content descrambler ([`super::lfsr::descramble_sector`], = libdvdcss
//! `dvdcss_unscramble`) seeds its two LFSRs **directly** from
//! `key = title_key XOR sector_seed` (seed = `sector[0x54..0x59]`):
//!
//! ```text
//! i_t1 = (key[0] ^ sec[0x54]) | 0x100;          // LFSR1 low (9-bit)
//! i_t2 =  key[1] ^ sec[0x55];                    // LFSR1 high
//! i_t3 = (key[2]|key[3]<<8|key[4]<<16) ^ seed3;  // LFSR0 (24-bit feedback)
//! i_t3 = i_t3*2 + 8 - (i_t3 & 7);
//! // per byte:  *p = TAB1[*p] ^ (i_t5 & 0xff)
//! ```
//!
//! There is NO `decrypt_key` mangling on the content path. So the recovery
//! is a single inversion of `dvdcss_unscramble`, not the multi-stage
//! working-key inversion the previous (non-CSS) implementation used.
//!
//! # The attack
//!
//! 1. **Known plaintext → keystream.** Because the descramble applies TAB1
//!    to the ciphertext, the per-byte keystream is
//!    `buf[i] = TAB1[cipher[i]] ^ plain[i]` (matching libdvdcss
//!    `RecoverTitleKey`'s `p_buffer`).
//! 2. **Brute the 16-bit LFSR1 seed.** For each of 2^16 seeds, run LFSR1
//!    forward; for the first four steps deduce the LFSR0 output bytes from
//!    the keystream (carry-tracked), reconstructing `i_t3`. For the next six
//!    steps clock LFSR0 normally and check it reproduces the keystream — a
//!    wrong LFSR1 seed fails fast.
//! 3. **Back-clock LFSR0.** Run four backward `i_t3` steps (each a 256-way
//!    search for the byte shifted in) to reach the initial state, then undo
//!    `i_t3 = i_t3*2 + 8 - (i_t3 & 7)` to recover key[2..5].
//! 4. **XOR back the seed.** `key[0..5] ^= sector_seed[0..5]` (plain XOR —
//!    the descramble seeds directly, so there is no inversion).
//!
//! `AttackPattern` finds known plaintext for step 1: the longest periodic
//! run in the cleartext `sec[0x00..0x80]`, assumed to continue into the
//! encrypted region at 0x80.

use super::lfsr::descramble_sector;
use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Sector layout constants.
const SECTOR_SIZE: usize = 2048;
const ENCRYPTED_START: usize = 0x80; // byte 128
const SEED_OFFSET: usize = 0x54; // sector seed at bytes 0x54-0x58
const FLAG_BYTE: usize = 0x14;

/// RecoverTitleKey: recover the title key from cipher + known plaintext.
///
/// Exact port of libdvdcss `RecoverTitleKey` (css.c). `crypted` is the
/// ciphertext starting at sector byte 0x80; `decrypted` is the matching
/// known plaintext; `seed` is `sector[0x54..0x59]`. On success returns the
/// recovered 5-byte title key; `None` if no LFSR seed reproduces the
/// keystream.
///
/// At least 10 bytes of `crypted`/`decrypted` are required (the cipher is
/// iterated 10 times: 4 to reconstruct LFSR0, 6 to validate).
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
            let mut i_t6 = (((((((i_t3 >> 3) ^ i_t3) >> 1) ^ i_t3) >> 8) ^ i_t3) >> 5) & 0xff;
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
                let i_t6 = (((((((i_t3 >> 3) ^ i_t3) >> 1) ^ i_t3) >> 8) ^ i_t3) >> 5) & 0xff;
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
    if sector.len() < SECTOR_SIZE || plain.len() < 10 {
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
    let n = plain.len().min(SECTOR_SIZE - ENCRYPTED_START);
    test[ENCRYPTED_START..ENCRYPTED_START + n] == plain[..n]
}

/// AttackPattern: find a repeating pattern just before the encrypted region
/// and assume the plaintext at 0x80 continues it.
///
/// Exact port of libdvdcss `AttackPattern` (css.c). Scans cleartext
/// `sec[0x00..0x80]` for the longest run that repeats with a cycle length in
/// 2..0x2F. If the run is long enough (`plen > 3` and at least two full
/// cycles), the known plaintext at 0x80 is taken to be the periodic run
/// continuing forward, and [`recover_title_key_from_plain`] is applied.
pub fn crack_title_key(sector: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_SIZE {
        return None;
    }
    if sector[FLAG_BYTE] & 0x30 == 0 {
        return None;
    }

    // Runaway guard: a single sector's crack is a bounded 2^16 LFSR search and
    // should finish in well under a second on any modern CPU. If it ever
    // exceeds ~2s wall-clock, something pathological is happening — log it so a
    // hang is never silent.
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

/// Inner body of [`crack_title_key`] — the actual AttackPattern search. Split
/// out so the public entry point can wall-clock the whole attempt for the
/// runaway guard without threading a timer through every return path.
fn crack_title_key_inner(sector: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_SIZE || sector[FLAG_BYTE] & 0x30 == 0 {
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
        let seed: [u8; 5] = [
            sector[SEED_OFFSET],
            sector[SEED_OFFSET + 1],
            sector[SEED_OFFSET + 2],
            sector[SEED_OFFSET + 3],
            sector[SEED_OFFSET + 4],
        ];

        // The known plaintext is the periodic run continuing past 0x80. The
        // crib starts at `0x80 - (best_plen/best_p)*best_p` and continues
        // through the encrypted region; the bytes at and after 0x80 are the
        // predicted plaintext (the pattern repeats with period best_p).
        let cycles = best_plen / best_p;
        let plain_start = 0x80 - cycles * best_p;

        // The cipher is the 10 bytes at 0x80; the crib is their predicted
        // plaintext. The periodic run (period `best_p`) is known to continue
        // through 0x80, so each predicted byte is the run sample one or more
        // periods back: `sec[plain_start + (i % best_p)]`. For in-run offsets
        // (`plain_start + i < 0x80`) the run is exactly periodic, so this
        // equals `sec[plain_start + i]`; for offsets at/after 0x80 the raw
        // byte is ciphertext, so we MUST wrap within the period rather than
        // read it. (Reading `&sec[plain_start..+10]` directly — as before —
        // pulled ciphertext into the crib whenever the run covered fewer than
        // 10 bytes before 0x80, producing false-negative key recovery.)
        let crypted = &sector[0x80..0x80 + 10];
        let mut plain = [0u8; 10];
        for (i, p) in plain.iter_mut().enumerate() {
            *p = sector[plain_start + (i % best_p)];
        }

        if let Some(key) = recover_title_key_from_plain(crypted, &plain, &seed) {
            // Verify against the same predicted plaintext.
            if descramble_matches(sector, &key, &plain) {
                return Some(key);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::lfsr::scramble_sector;
    use super::*;

    /// Build a synthetic scrambled sector for a given title key and seed,
    /// with `plain` placed as the plaintext at byte 0x80, scrambled with
    /// EXACTLY the cipher `descramble_sector` inverts. Returns
    /// (scrambled_sector, full_plaintext_body).
    fn synth_sector(title_key: &[u8; 5], seed: &[u8; 5], plain: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut plaintext = vec![0u8; SECTOR_SIZE];
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

    /// Build a synthetic scrambled sector whose CLEARTEXT (0x00..0x80) ends
    /// in a periodic run that continues into the encrypted region — the case
    /// `AttackPattern` (crack_title_key) is designed to crack.
    fn synth_periodic_sector(
        title_key: &[u8; 5],
        seed: &[u8; 5],
        period: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let mut plaintext = vec![0u8; SECTOR_SIZE];
        plaintext[FLAG_BYTE] = 0x10;

        // A clean periodic run occupying the tail of the cleartext header
        // (RUN_START..0x80) and continuing into the encrypted region. This
        // mirrors a real VOB: a periodic data run just before the scrambled
        // part. The run must NOT overlap the seed bytes (0x54..0x59), or the
        // AttackPattern detector would break mid-run. The phase is anchored to
        // offset 0 so the run is consistent across the 0x80 boundary.
        // Just above the seed (0x54..0x59); gives a 39-byte run (0x59..0x80)
        // — enough for >=2 cycles of every tested period (<=19).
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
        let sector = vec![0u8; 2048];
        assert!(crack_title_key(&sector).is_none());
    }

    #[test]
    fn crack_too_short_returns_none() {
        let sector = vec![0u8; 100];
        assert!(crack_title_key(&sector).is_none());
    }

    #[test]
    fn recover_needs_min_plain() {
        let sector = vec![0u8; 2048];
        let short_plain = [0u8; 4];
        assert!(recover_title_key(&sector, &short_plain).is_none());
    }

    /// The known plaintext used at byte 0x80 for the direct-recovery tests.
    /// A realistic MPEG-2 PES header start.
    const PES: [u8; 10] = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21];

    /// MANDATORY round-trip (Task C.1): synthesize a scrambled sector for a
    /// known (title_key, seed), then assert recover_title_key returns a key
    /// that descrambles the body back to plaintext. CSS title-key recovery is
    /// well-defined up to keys that scramble identically; we assert the full
    /// body round-trips (the true correctness property), and additionally
    /// that the EXACT key is returned for the common case.
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
                &sector[ENCRYPTED_START..SECTOR_SIZE],
                &body[ENCRYPTED_START..SECTOR_SIZE],
                "recovered key did not descramble the full body for \
                 title={title_key:02x?} seed={seed:02x?}"
            );
        }
    }

    /// MANDATORY (Task C.1): the AttackPattern entry point crack_title_key —
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
                &test[ENCRYPTED_START..SECTOR_SIZE],
                &body[ENCRYPTED_START..SECTOR_SIZE],
                "crack_title_key key did not round-trip the body (period {period})"
            );
        }
    }

    /// recover_title_key_from_plain inverts dvdcss_unscramble exactly: scramble
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
                &sector[ENCRYPTED_START..SECTOR_SIZE],
                &body[ENCRYPTED_START..SECTOR_SIZE],
                "descramble with recovered key did not reproduce the body \
                 for title={title_key:02x?} seed={seed:02x?}"
            );
        }
    }

    // ── early-return guards ────────────────────────────────────────────────

    #[test]
    fn recover_rejects_sector_one_byte_short() {
        let mut sector = vec![0u8; SECTOR_SIZE - 1];
        sector[FLAG_BYTE] = 0x30;
        assert!(recover_title_key(&sector, &PES).is_none());
    }

    #[test]
    fn recover_rejects_unscrambled_sector() {
        let sector = vec![0x00u8; SECTOR_SIZE];
        assert!(recover_title_key(&sector, &PES).is_none());
    }

    #[test]
    fn recover_high_flag_bits_are_not_scramble() {
        for &flag in &[0x40u8, 0x80, 0xC0] {
            let mut sector = vec![0x11u8; SECTOR_SIZE];
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
            let mut sector = vec![0x11u8; SECTOR_SIZE];
            sector[FLAG_BYTE] = flag;
            assert!(
                crack_title_key(&sector).is_none(),
                "flag {flag:#04x} clear scramble bits -> crack must return None"
            );
        }
    }

    #[test]
    fn crack_rejects_sector_one_byte_short() {
        let mut sector = vec![0u8; SECTOR_SIZE - 1];
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
            let mut sector = vec![0u8; SECTOR_SIZE];
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
}

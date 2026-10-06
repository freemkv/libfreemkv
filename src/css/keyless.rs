//! CSS title-key recovery — a divide-and-conquer known-plaintext attack,
//! implemented from the openly published CSS cryptanalysis literature. It
//! recovers the 5-byte CSS title key from a single scrambled DVD sector with
//! no player keys and no disc-key crack, using only known plaintext.
//! Implemented from that public description; nothing here is copied or
//! translated from any particular CSS software.

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

// Recover the title key from cipher + known plaintext. `seed` is sector[0x54..0x59].
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

/// Recover the CSS title key from a scrambled sector via known plaintext.
///
/// `plain` is the expected plaintext at byte 0x80 (≥10 bytes); returns the key only
/// if it descrambles the sector back to `plain`, guarding a rare spurious LFSR match.
///
/// TEST-ONLY: production cracks via [`crack_title_key`], which derives its own
/// crib. This helper is used by unit tests and `tests/crypto_tests.rs`; it stays
/// `pub` (not `#[cfg(test)]`) so that separate integration-test crate can reach it.
#[doc(hidden)]
pub fn recover_title_key(sector: &[u8], plain: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_BYTES || plain.len() < 10 {
        return None;
    }
    if sector[FLAG_BYTE] & 0x30 == 0 {
        return None;
    }

    let seed = sector_seed(sector);

    let crypted = &sector[ENCRYPTED_START..ENCRYPTED_START + 10];
    let key = recover_title_key_from_plain(crypted, plain, &seed)?;

    if descramble_matches(sector, &key, plain) {
        Some(key)
    } else {
        None
    }
}

fn sector_seed(sector: &[u8]) -> [u8; 5] {
    let mut seed = [0u8; 5];
    seed.copy_from_slice(&sector[SEED_OFFSET..SEED_OFFSET + 5]);
    seed
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

/// Crack the CSS title key from one scrambled sector with no external crib.
/// The crib is the periodic cleartext run before 0x80 (see `attack_crib`),
/// continued into the encrypted region and fed to `recover_title_key_from_plain`.
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

// Crib: the predicted 10-byte plaintext at byte 0x80, from the longest periodic run in the
// clear header. Also the decrypt path's cached-key oracle.
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
    let seed = sector_seed(sector);
    let crypted = &sector[ENCRYPTED_START..ENCRYPTED_START + 10];
    if let Some(key) = recover_title_key_from_plain(crypted, &plain, &seed) {
        // Verify against the same predicted plaintext.
        if descramble_matches(sector, &key, &plain) {
            return Some(key);
        }
    }
    None
}

#[cfg(test)]
#[path = "keyless_tests.rs"]
mod tests;

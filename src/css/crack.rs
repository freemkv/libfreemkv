//! CSS title key recovery — Stevenson's divide-and-conquer attack (1999).
//!
//! Given a scrambled DVD sector with known plaintext (MPEG-2 PES headers),
//! recovers the 5-byte title key by:
//!
//! 1. XORing ciphertext with TAB1[ciphertext] to cancel the mangling
//! 2. Iterating all 2^16 LFSR1 states
//! 3. For each: deducing what LFSR0 must produce, then verifying
//!
//! Total work: ~65536 iterations with 10-byte validation = instant.
//!
//! Algorithm: Frank A. Stevenson, "Divide and conquer attack" (1999).

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Sector layout constants.
const SECTOR_SIZE: usize = 2048;
const ENCRYPTED_START: usize = 0x80; // byte 128
const SEED_OFFSET: usize = 0x54;     // sector seed at bytes 0x54-0x58
const FLAG_BYTE: usize = 0x14;

/// Recover the CSS title key from a scrambled sector using known plaintext.
///
/// The `plain` slice should contain the expected plaintext of the encrypted
/// region (bytes 0x80+). For MPEG-2 sectors, the first bytes are typically
/// a PES header: `00 00 01 [stream_id] ...`
///
/// Returns the recovered 5-byte title key, or None if recovery fails.
pub fn recover_title_key(
    sector: &[u8],
    plain: &[u8],
) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_SIZE || plain.len() < 10 {
        return None;
    }

    let flags = (sector[FLAG_BYTE] >> 4) & 0x03;
    if flags == 0 {
        return None;
    }

    let crypted = &sector[ENCRYPTED_START..];
    let seed = &sector[SEED_OFFSET..SEED_OFFSET + 5];

    // Phase 1: Cancel the TAB1 mangling layer
    // The CSS cipher applies TAB1 as an output permutation.
    // XORing ciphertext with TAB1[ciphertext] and plaintext removes it,
    // leaving the raw LFSR combination output.
    let mut buf = [0u8; 10];
    for i in 0..10 {
        if i >= crypted.len() || i >= plain.len() {
            return None;
        }
        buf[i] = TAB1[crypted[i] as usize] ^ plain[i];
    }

    // Phase 2: Stevenson attack — iterate all 2^16 LFSR1 initial states
    let mut result_key = [0u8; 5];
    let mut found = false;

    for i_try in 0u32..0x10000 {
        let mut t1 = (i_try >> 8) | 0x100;
        let mut t2 = i_try & 0xFF;
        let mut t5: u32 = 0;

        // Clock LFSR1 forward 4 steps to reconstruct LFSR0 state
        let mut t3: u32 = 0;
        let mut ok = true;

        for i in 0..4 {
            // Advance LFSR1
            let t4 = TAB2[t2 as usize] ^ TAB3[t1 as usize];
            t2 = t1 >> 1;
            t1 = ((t1 & 1) << 8) ^ t4 as u32;
            let t4_perm = TAB5[t4 as usize];

            // Deduce LFSR0 output from the buffer and LFSR1 output
            let mut t6 = buf[i] as u32;
            if t5 > 0 {
                t6 = (t6 + 0xFF) & 0xFF;
            }
            if t6 < t4_perm as u32 {
                t6 += 0x100;
            }
            t6 -= t4_perm as u32;
            t5 += t6 + t4_perm as u32;
            let t6_inv = TAB4[t6 as usize & 0xFF];

            // Build LFSR0 candidate from deduced output bytes
            t3 = (t3 << 8) | t6_inv as u32;
            t5 >>= 8;
        }

        let candidate = t3;

        // Phase 3: Validate — clock 6 more steps and check against buffer
        let mut valid = true;
        for i in 4..10 {
            let t4 = TAB2[t2 as usize] ^ TAB3[t1 as usize];
            t2 = t1 >> 1;
            t1 = ((t1 & 1) << 8) ^ t4 as u32;
            let t4_perm = TAB5[t4 as usize];

            // Clock LFSR0 forward
            let t6 = ((((((t3 >> 3) ^ t3) >> 1) ^ t3) >> 8) ^ t3) >> 5;
            t3 = (t3 << 8) | (t6 & 0xFF);
            let t6_perm = TAB4[(t6 & 0xFF) as usize];

            t5 += t6_perm as u32 + t4_perm as u32;
            if (t5 & 0xFF) as u8 != buf[i] {
                valid = false;
                break;
            }
            t5 >>= 8;
        }

        if !valid {
            continue;
        }

        // Phase 4: Recover the initial LFSR0 state from the candidate
        t3 = candidate;
        for _ in 0..4 {
            let t1_byte = t3 & 0xFF;
            t3 >>= 8;
            // Brute-force the byte that was shifted in
            let mut found_j = false;
            for j in 0u32..256 {
                t3 = (t3 & 0x1FFFF) | (j << 17);
                let t6 = ((((((t3 >> 3) ^ t3) >> 1) ^ t3) >> 8) ^ t3) >> 5;
                if (t6 & 0xFF) == t1_byte {
                    found_j = true;
                    break;
                }
            }
            if !found_j {
                continue;
            }
        }

        // Convert LFSR0 initial state back to key bytes
        let t4 = (t3 >> 1).wrapping_sub(4);
        for t5_off in 0u32..8 {
            let val = t4.wrapping_add(t5_off);
            if (val * 2 + 8 - (val & 7)) == t3 {
                result_key[0] = (i_try >> 8) as u8;
                result_key[1] = (i_try & 0xFF) as u8;
                result_key[2] = (val & 0xFF) as u8;
                result_key[3] = ((val >> 8) & 0xFF) as u8;
                result_key[4] = ((val >> 16) & 0xFF) as u8;
                found = true;
            }
        }
    }

    if !found {
        return None;
    }

    // XOR with sector seed to get the actual title key
    result_key[0] ^= seed[0];
    result_key[1] ^= seed[1];
    result_key[2] ^= seed[2];
    result_key[3] ^= seed[3];
    result_key[4] ^= seed[4];

    Some(result_key)
}

/// Crack the CSS title key from an encrypted sector using MPEG-2 pattern attack.
///
/// Detects the PES header pattern at byte 0x80 and uses it as known plaintext.
pub fn crack_title_key(sector: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_SIZE {
        return None;
    }

    let flags = (sector[FLAG_BYTE] >> 4) & 0x03;
    if flags == 0 {
        return None;
    }

    // The PES header at byte 0x80 typically starts with 00 00 01 [stream_id].
    // The next bytes are PES length and flags. We need at least 10 bytes of
    // known plaintext for the Stevenson attack.
    //
    // Strategy: try common PES patterns. The first 3 bytes are always 00 00 01.
    // The stream_id varies. Bytes 4-9 depend on PES header structure.
    //
    // For a standard PES with PTS:
    //   00 00 01 [id] [len_hi] [len_lo] [flags] [flags2] [hdr_len] [PTS...]
    //
    // We try multiple stream IDs and use zeros for unknown bytes (most common).

    let stream_ids: &[u8] = &[
        0xE0, // video
        0xBD, // private stream 1 (AC3/DTS)
        0xC0, // MPEG audio
        0xBE, // padding
    ];

    for &sid in stream_ids {
        // Build candidate plaintext (10 bytes)
        // Bytes 0-2: PES start code 00 00 01
        // Byte 3: stream ID
        // Bytes 4-9: we try with zeros first (common for padding streams)
        //            and with typical PES header bytes
        let patterns: &[[u8; 10]] = &[
            [0x00, 0x00, 0x01, sid, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21],
            [0x00, 0x00, 0x01, sid, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x01, sid, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ];

        for pattern in patterns {
            if let Some(key) = recover_title_key(sector, pattern) {
                // Verify: the key should produce valid MPEG-2 when used to descramble
                let mut test = sector.to_vec();
                super::lfsr::descramble_sector(&key, &mut test);
                if test[0x80] == 0x00 && test[0x81] == 0x00 && test[0x82] == 0x01 {
                    return Some(key);
                }
            }
        }
    }

    None
}

/// Crack CSS key from multiple sectors.
pub fn crack_from_sectors(sectors: &[Vec<u8>]) -> Option<[u8; 5]> {
    for sector in sectors {
        if sector.len() < SECTOR_SIZE {
            continue;
        }
        let flags = (sector[FLAG_BYTE] >> 4) & 0x03;
        if flags == 0 {
            continue;
        }
        if let Some(key) = crack_title_key(sector) {
            return Some(key);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn recover_needs_10_bytes_plain() {
        let sector = vec![0u8; 2048];
        let short_plain = [0u8; 5];
        assert!(recover_title_key(&sector, &short_plain).is_none());
    }
}

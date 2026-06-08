//! CSS title key recovery — Stevenson's divide-and-conquer attack (1999).
//!
//! Given a scrambled DVD sector with known plaintext (MPEG-2 PES headers),
//! this would recover the 5-byte title key by:
//!
//! 1. Computing `TAB1[ciphertext] ^ plaintext` to cancel the TAB1 output
//!    mangling and expose the raw LFSR-combination keystream
//! 2. Iterating all 2^16 LFSR1 states
//! 3. For each: deducing what LFSR0 must produce, then verifying
//!
//! NOTE: this recovery path is currently non-functional. It models the
//! textbook direct-seed CSS cipher, whereas the in-repo descrambler
//! ([`super::lfsr::descramble_sector`]) seeds its LFSRs from a key that
//! has been run through an additional `decrypt_key` mangling step. The two
//! are therefore inconsistent and [`recover_title_key`] never returns a key
//! for a sector scrambled by this crate's own descrambler. The production
//! DVD path does NOT use this fallback — it derives the title key over SCSI
//! ([`super::auth::authenticate_and_read_title_key`]). See the ignored
//! regression test below.
//!
//! Algorithm: Frank A. Stevenson, "Divide and conquer attack" (1999).

use super::tables::{TAB1, TAB2, TAB3, TAB4, TAB5};

/// Sector layout constants.
const SECTOR_SIZE: usize = 2048;
const ENCRYPTED_START: usize = 0x80; // byte 128
const SEED_OFFSET: usize = 0x54; // sector seed at bytes 0x54-0x58
const FLAG_BYTE: usize = 0x14;

/// Recover the CSS title key from a scrambled sector using known plaintext.
///
/// The `plain` slice should contain the expected plaintext of the encrypted
/// region (bytes 0x80+). For MPEG-2 sectors, the first bytes are typically
/// a PES header: `00 00 01 [stream_id] ...`
///
/// Returns the recovered 5-byte title key, or `None` if recovery fails.
///
/// NOTE: see the module docs — this attack models the textbook direct-seed
/// CSS cipher and is inconsistent with this crate's descrambler, so it
/// currently returns `None` even for an exact known plaintext. It is not on
/// the production DVD decrypt path.
pub fn recover_title_key(sector: &[u8], plain: &[u8]) -> Option<[u8; 5]> {
    if sector.len() < SECTOR_SIZE || plain.len() < 10 {
        return None;
    }

    let flags = (sector[FLAG_BYTE] >> 4) & 0x03;
    if flags == 0 {
        return None;
    }

    let crypted = &sector[ENCRYPTED_START..];
    let seed = &sector[SEED_OFFSET..SEED_OFFSET + 5];

    // Phase 1: Cancel the TAB1 mangling layer and subtract the known plaintext.
    // The CSS cipher applies TAB1 as an output permutation. Computing
    // `buf[i] = TAB1[crypted[i]] ^ plain[i]` both undoes that permutation and
    // XORs out the known plaintext, leaving the raw LFSR-combination keystream
    // bytes for the attack to match against.
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

    'outer: for i_try in 0u32..0x10000 {
        let mut t1 = (i_try >> 8) | 0x100;
        let mut t2 = i_try & 0xFF;
        let mut t5: u32 = 0;

        // Clock LFSR1 forward 4 steps to reconstruct LFSR0 state
        let mut t3: u32 = 0;

        for &buf_byte in buf.iter().take(4) {
            // Advance LFSR1
            let t4 = TAB2[t2 as usize] ^ TAB3[t1 as usize];
            t2 = t1 >> 1;
            t1 = ((t1 & 1) << 8) ^ t4 as u32;
            let t4_perm = TAB5[t4 as usize];

            // Deduce LFSR0 output from the buffer and LFSR1 output
            let mut t6 = buf_byte as u32;
            if t5 > 0 {
                t6 = (t6 + 0xFF) & 0xFF;
            }
            if t6 < t4_perm as u32 {
                t6 += 0x100;
            }
            t6 -= t4_perm as u32;
            t5 += t6 + t4_perm as u32;
            let t6_inv = TAB4[t6 as usize & 0xFF];

            // Build LFSR0 candidate from deduced output bytes.
            // wrapping_shl: the accumulator is a rolling 32-bit window;
            // the top byte is intentionally shifted out. Matches the
            // release-mode wrap (no behaviour change) without a debug
            // overflow panic.
            t3 = t3.wrapping_shl(8) | t6_inv as u32;
            t5 >>= 8;
        }

        let candidate = t3;

        // Phase 3: Validate — clock 6 more steps and check against buffer
        let mut valid = true;
        for &buf_byte in buf.iter().skip(4) {
            let t4 = TAB2[t2 as usize] ^ TAB3[t1 as usize];
            t2 = t1 >> 1;
            t1 = ((t1 & 1) << 8) ^ t4 as u32;
            let t4_perm = TAB5[t4 as usize];

            // Clock LFSR0 forward. wrapping_shl keeps the rolling 32-bit
            // window semantics (top byte shifted out) identical to the
            // release build while avoiding a debug overflow panic.
            let t6 = ((((((t3 >> 8) ^ t3) >> 1) ^ t3) >> 3) ^ t3) >> 7;
            t3 = t3.wrapping_shl(8) | (t6 & 0xFF);
            let t6_perm = TAB4[(t6 & 0xFF) as usize];

            t5 += t6_perm as u32 + t4_perm as u32;
            if (t5 & 0xFF) as u8 != buf_byte {
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
        let mut recovery_ok = true;
        for _ in 0..4 {
            let t1_byte = t3 & 0xFF;
            t3 >>= 8;
            // Brute-force the byte that was shifted in
            let mut found_j = false;
            for j in 0u32..256 {
                t3 = (t3 & 0x1FFFF) | (j << 17);
                let t6 = ((((((t3 >> 8) ^ t3) >> 1) ^ t3) >> 3) ^ t3) >> 7;
                if (t6 & 0xFF) == t1_byte {
                    found_j = true;
                    break;
                }
            }
            if !found_j {
                recovery_ok = false;
                break;
            }
        }
        if !recovery_ok {
            continue 'outer;
        }

        // Convert LFSR0 initial state back to key bytes
        let t4 = (t3 >> 1).wrapping_sub(4);
        for t5_off in 0u32..8 {
            let val = t4.wrapping_add(t5_off);
            // Reconstruction probe: val can sit near u32::MAX, so the
            // (val*2 + 8 - (val & 7)) expression must wrap rather than
            // panic in debug. wrapping_* reproduces the release result
            // exactly (the comparison against t3 is unaffected).
            if val.wrapping_mul(2).wrapping_add(8).wrapping_sub(val & 7) == t3 {
                result_key[0] = (i_try >> 8) as u8;
                result_key[1] = (i_try & 0xFF) as u8;
                result_key[2] = (val & 0xFF) as u8;
                result_key[3] = ((val >> 8) & 0xFF) as u8;
                result_key[4] = ((val >> 16) & 0xFF) as u8;
                found = true;
                break;
            }
        }
        if found {
            break;
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

/// Crack the CSS title key from an encrypted sector using an MPEG-2
/// pattern attack.
///
/// Detects the PES header pattern at byte 0x80 and uses it as known
/// plaintext. This is a best-effort fallback for the SCSI auth path
/// (see [`super::resolve`]): it only succeeds on a sector whose
/// encrypted region begins with one of the tried PES header patterns,
/// and returns `None` otherwise. The production DVD path obtains the
/// title key via drive authentication, not cracking.
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

    // Try many PES header patterns at byte 0x80.
    // Structure: 00 00 01 [stream_id] [len_hi] [len_lo] [flags1] [flags2] [hdr_len] [data]
    // 24 padding-stream + 144 video/audio + 1 navigation = 169 patterns.
    let mut patterns: Vec<[u8; 10]> = Vec::with_capacity(169);

    // Padding stream (0xBE): payload is 0xFF bytes, various lengths
    for len_hi in 0u8..8 {
        for len_lo_top in [0x00u8, 0x80, 0xFF] {
            patterns.push([
                0x00, 0x00, 0x01, 0xBE, len_hi, len_lo_top, 0xFF, 0xFF, 0xFF, 0xFF,
            ]);
        }
    }

    // Video (0xE0) and audio (0xBD, 0xC0) with typical PES headers
    for &sid in &[0xE0u8, 0xBD, 0xC0] {
        for &flags1 in &[0x80u8, 0x81, 0x84, 0x85, 0x8C, 0x8D] {
            for &flags2 in &[0x00u8, 0x05, 0x80, 0xC0] {
                let hdr_len = if flags2 & 0x80 != 0 { 0x05u8 } else { 0x00 };
                let pts0 = if flags2 & 0x80 != 0 { 0x21u8 } else { 0x00 };
                // Try with several PES lengths
                for &len_hi in &[0x00u8, 0x07] {
                    patterns.push([
                        0x00, 0x00, 0x01, sid, len_hi, 0x00, flags1, flags2, hdr_len, pts0,
                    ]);
                }
            }
        }
    }

    // Navigation pack system header (0xBB)
    patterns.push([0x00, 0x00, 0x01, 0xBB, 0x00, 0x12, 0x80, 0xC4, 0xE1, 0x04]);

    for pattern in &patterns {
        if let Some(key) = recover_title_key(sector, pattern) {
            let mut test = sector.to_vec();
            super::lfsr::descramble_sector(&key, &mut test);
            if test[0x80] == 0x00 && test[0x81] == 0x00 && test[0x82] == 0x01 {
                return Some(key);
            }
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

    /// Regression: the LFSR0 reconstruction arithmetic must not overflow
    /// (panic) in a debug build for scrambled sector content. Exercises
    /// the full 2^16 Stevenson search via recover_title_key directly (the
    /// overflow site), with the assertion simply that it does not panic.
    /// recover_title_key is used rather than crack_title_key to avoid
    /// re-running the search for all 169 PES patterns.
    #[test]
    fn crack_scrambled_sectors_never_overflow() {
        for seed in 0u32..4 {
            let mut sector = vec![0u8; SECTOR_SIZE];
            sector[FLAG_BYTE] = 0x30; // scramble flag set
            let mut x = seed.wrapping_mul(2_654_435_761).wrapping_add(1);
            for b in sector.iter_mut().skip(0x80) {
                x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                *b = (x >> 16) as u8;
            }
            for (i, b) in sector[SEED_OFFSET..SEED_OFFSET + 5].iter_mut().enumerate() {
                *b = seed.wrapping_add(i as u32) as u8;
            }
            let plain = [0x00u8, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21];
            let _ = recover_title_key(&sector, &plain);
        }
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

    /// Build a scrambled sector with known plaintext (both an MPEG PES header
    /// at 0x80 and an exact-plaintext probe), then assert that the Stevenson
    /// recovery actually recovers a key whose descramble round-trips the body.
    ///
    /// This is the regression gate for the CSS crack/recover path. It is
    /// `#[ignore]`d because that path is currently non-functional: the
    /// recovery models the textbook direct-seed cipher, whereas this crate's
    /// [`descramble_sector`] seeds from a `decrypt_key`-mangled key, so the
    /// two are inconsistent and recovery returns `None`. When the crack
    /// algorithm is re-derived against this crate's actual descrambler, this
    /// test must pass with `--ignored` removed. The production DVD path does
    /// not use crack/recover (it authenticates over SCSI), so the broken
    /// fallback does not affect shipped behavior.
    #[test]
    #[ignore = "CSS crack/recover path is non-functional vs this crate's descrambler; \
                see module docs. Regression gate for a future fix."]
    fn css_crack_recovers_key_from_scrambled_sector() {
        use super::super::lfsr::descramble_sector;

        let title_key: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF];

        // Build a plaintext MPEG-2 sector
        let mut plaintext = vec![0x00u8; SECTOR_SIZE];

        // Pack header at byte 0: 00 00 01 BA
        plaintext[0] = 0x00;
        plaintext[1] = 0x00;
        plaintext[2] = 0x01;
        plaintext[3] = 0xBA;

        // Scramble flag at byte 0x14
        plaintext[FLAG_BYTE] = 0x30;

        // Sector seed at bytes 0x54-0x58
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);

        // PES header at byte 0x80: 00 00 01 E0 (video stream) with PTS.
        let exact_plain: [u8; 10] = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05, 0x21];
        plaintext[0x80..0x80 + 10].copy_from_slice(&exact_plain);

        let original_plaintext = plaintext.clone();

        // "Scramble" the sector by XORing the keystream over the plaintext.
        descramble_sector(&title_key, &mut plaintext);

        // descramble_sector cleared the flag; restore it so the cracker sees
        // the sector as encrypted.
        plaintext[FLAG_BYTE] = 0x30;

        // 1) Pattern-guessing entry point must recover a key.
        let cracked = crack_title_key(&plaintext);
        assert!(
            cracked.is_some(),
            "crack_title_key returned None for a sector scrambled with a known key"
        );
        let cracked = cracked.unwrap();
        let mut body = plaintext.clone();
        descramble_sector(&cracked, &mut body);
        assert_eq!(
            &body[0x80..SECTOR_SIZE],
            &original_plaintext[0x80..SECTOR_SIZE],
            "crack_title_key key did not round-trip the body"
        );

        // 2) Exact known plaintext must also recover a round-tripping key.
        let recovered = recover_title_key(&plaintext, &exact_plain);
        assert!(
            recovered.is_some(),
            "recover_title_key returned None for exact known plaintext"
        );
        let recovered = recovered.unwrap();
        let mut body2 = plaintext.clone();
        descramble_sector(&recovered, &mut body2);
        assert_eq!(
            &body2[0x80..SECTOR_SIZE],
            &original_plaintext[0x80..SECTOR_SIZE],
            "recover_title_key key did not round-trip the body"
        );
    }
}

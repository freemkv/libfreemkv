//! AACS content decryption — AES primitives, unit decryption, bus encryption.

use aes::Aes128;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};

// ── AACS constants ──────────────────────────────────────────────────────────

/// Fixed IV used by AACS for all AES-CBC operations.
pub(crate) const AACS_IV: [u8; 16] = [
    0x0B, 0xA0, 0xF8, 0xDD, 0xFE, 0xA6, 0x1F, 0xB3, 0xD8, 0xDF, 0x9F, 0x56, 0x6A, 0x05, 0x0F, 0x78,
];

/// Size of an AACS aligned unit (3 × 2048-byte sectors).
pub const ALIGNED_UNIT_LEN: usize = 6144;

/// An AACS aligned unit spans this many 2048-byte sectors (3).
pub const ALIGNED_UNIT_SECTORS: u32 = (ALIGNED_UNIT_LEN / SECTOR_LEN) as u32;

/// Whether `lba` sits on an AACS aligned-unit boundary, measured **relative to
/// the encrypted region's base LBA** (`unit_base` = the clip/extent `start_lba`,
/// NOT absolute disc LBA 0).
///
/// AACS aligned units (6144 B / 3 sectors) are anchored at the start of each
/// clip's encrypted region, so a read must begin a whole number of units past
/// that base for `decrypt_sectors` (which anchors units at buffer offset 0) to
/// align the CBC correctly. This is the SINGLE source of truth for the test —
/// the decrypt-on-read gate, the inline and highway mux read paths, and the
/// key-validation sample reader all key off this, never absolute `lba % 3`. A
/// disc whose clip `start_lba` is not itself 3-aligned would otherwise mis-gate
/// (reject readable units, then report "Decryption failed") on exactly the
/// titles whose clips land off a 3-boundary.
pub fn is_unit_aligned(lba: u32, unit_base: u32) -> bool {
    lba.wrapping_sub(unit_base) % ALIGNED_UNIT_SECTORS == 0
}

/// Size of one sector.
const SECTOR_LEN: usize = 2048;

/// Transport stream packet spacing in Blu-ray m2ts (192 bytes = 4 TP_extra + 188 TS).
const TS_PACKET_LEN: usize = 192;

/// TS sync byte.
const TS_SYNC: u8 = 0x47;

// ── AES primitives ──────────────────────────────────────────────────────────

/// AES-128-ECB encrypt a single 16-byte block.
pub(crate) fn aes_ecb_encrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let mut block = GenericArray::clone_from_slice(data);
    cipher.encrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

/// AES-128-ECB decrypt a single 16-byte block.
pub(crate) fn aes_ecb_decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let mut block = GenericArray::clone_from_slice(data);
    cipher.decrypt_block(&mut block);
    let mut out = [0u8; 16];
    out.copy_from_slice(&block);
    out
}

/// AES-128-CBC decrypt in-place with the fixed AACS IV.
///
/// Precondition: `data.len()` is a multiple of 16. Any trailing partial
/// block is silently ignored; all callers pass aligned regions (6128 and
/// 2032 bytes), and the assert documents/enforces that contract.
pub(crate) fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
    debug_assert!(
        data.len() % 16 == 0,
        "aes_cbc_decrypt requires a block-aligned slice"
    );
    let cipher = Aes128::new(GenericArray::from_slice(key));
    let num_blocks = data.len() / 16;
    // Process blocks in reverse to avoid clobbering ciphertext needed for XOR
    for i in (0..num_blocks).rev() {
        let offset = i * 16;
        let prev = if i == 0 {
            AACS_IV
        } else {
            let mut p = [0u8; 16];
            p.copy_from_slice(&data[(i - 1) * 16..i * 16]);
            p
        };
        let mut block = GenericArray::clone_from_slice(&data[offset..offset + 16]);
        cipher.decrypt_block(&mut block);
        for j in 0..16 {
            data[offset + j] = block[j] ^ prev[j];
        }
    }
}

// ── Content decryption ──────────────────────────────────────────────────────

/// True if a 6144-byte aligned unit is AACS-scrambled on disc.
///
/// AACS encrypts the unit body, which destroys the MPEG-TS sync bytes (`0x47`)
/// a clear unit carries at offsets 4, 196, 388, … (one per 192-byte source
/// packet). So "scrambled" = "the TS syncs are NOT intact". This is
/// flag-independent: it does NOT read the TP_extra copy-control bits (byte 0)
/// or the TS scrambling-control bits (byte 7) — AACS sets neither reliably
/// across discs/players.
///
/// This is the single shared definition of "encrypted" for the whole ecosystem
/// — libfreemkv's decrypt gate, autorip's sample selection, and the online key
/// service's validation gate all call THIS, so they always agree on what is
/// encrypted. A correctly-decrypted (or natively-clear) unit reports `false`,
/// so the decrypt path never double-decrypts and there is no flag to clear.
pub fn is_aacs_scrambled(unit: &[u8]) -> bool {
    unit.len() >= ALIGNED_UNIT_LEN && !ts_syncs_intact(unit)
}

/// Count the MPEG-TS sync bytes (`0x47`) present at the BD-TS packet stride
/// (offset 4 and every 192 bytes after — 4-byte TP_extra_header + 188-byte
/// TS packet). A clear or correctly-decrypted m2ts unit shows ~one per
/// packet; an encrypted unit, or a non-content unit decrypted under a key
/// that doesn't apply, shows ~none.
pub fn ts_sync_count(unit: &[u8]) -> usize {
    let mut count = 0;
    let mut offset = 4;
    while offset < unit.len() {
        if unit[offset] == TS_SYNC {
            count += 1;
        }
        offset += TS_PACKET_LEN;
    }
    count
}

/// Number of BD-TS packets in the unit — the maximum possible sync count.
pub fn ts_packet_total(unit: &[u8]) -> usize {
    // One sync byte per 192-byte BD-TS packet (at offset 4 of each). The old
    // `(len - 4) / TS_PACKET_LEN + 1` over-counted by one for lengths of the
    // form `4 + k·192`.
    unit.len() / TS_PACKET_LEN
}

fn ts_syncs_intact(unit: &[u8]) -> bool {
    ts_sync_count(unit) > ts_packet_total(unit) / 2
}

/// Verify a decrypted unit looks like clear MPEG-TS (sync bytes intact).
fn verify_ts(unit: &[u8]) -> bool {
    ts_syncs_intact(unit)
}

/// Decrypt one AACS aligned unit (6144 bytes) in-place.
/// Returns true if decryption succeeded (verified by TS sync bytes).
///
/// Algorithm:
/// 1. AES-128-ECB encrypt first 16 bytes with unit_key → derived
/// 2. XOR derived with original 16 bytes → unit_decrypt_key
/// 3. AES-128-CBC decrypt bytes 16..6143 with unit_decrypt_key and AACS IV
///
/// Decryption restores the TS sync bytes, so the unit reads as clear afterward;
/// there is no flag to clear.
pub fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    if !is_aacs_scrambled(unit) {
        return true; // not encrypted
    }

    // Save original first 16 bytes (they're plaintext TP_extra_header)
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);

    // Step 1: Encrypt header with unit key to derive per-unit key
    let derived = aes_ecb_encrypt(unit_key, &header);

    // Step 2: XOR to get the actual decryption key
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }

    // Step 3: Decrypt bytes 16..6143 with AES-CBC
    aes_cbc_decrypt(&decrypt_key, &mut unit[16..ALIGNED_UNIT_LEN]);

    // Decryption restored the TS syncs; verify the unit now looks like clear TS.
    verify_ts(unit)
}

/// Fast, NON-MUTATING unit-key validation for the brute-force key search.
///
/// `decrypt_unit` pays a full 6128-byte (383-block) CBC decrypt before
/// `verify_ts` can reject a wrong key — but in a brute scan ~every candidate is
/// wrong. In CBC the plaintext of block *i* is `AES_dec(C_i) XOR C_{i-1}`, so
/// the FIRST restored TS sync byte (payload offset 196, which lands in CBC
/// block 11 of the `unit[16..]` region) can be recovered with a SINGLE block
/// decrypt instead of 383. A wrong key fails this 1-byte gate ~255/256 of the
/// time for the cost of one AES block; the rare survivor is then confirmed with
/// the full [`decrypt_unit`], so the set of accepted keys is bit-for-bit
/// identical to the slow path.
///
/// The caller MUST pass an aligned, already-[`is_aacs_scrambled`] unit
/// (`unit.len() >= ALIGNED_UNIT_LEN`). The brute pre-filters its units, so the
/// per-candidate scramble re-scan is intentionally skipped here.
///
/// NOTE: this is a search accelerator — it never writes the input and never
/// participates in the content decrypt path. Aggregate correctness (does a key
/// validate against *any* of the disc's units) is preserved because a true key
/// restores offset-196 on every standard BD-TS unit.
pub fn unit_key_validates(unit: &[u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    // Per-unit decrypt key: AES-ECB-encrypt the 16-byte plaintext header with
    // the unit key, XOR with the header (same derivation as `decrypt_unit`).
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }

    // Cheap gate: recover ONLY payload byte 196 (the 2nd BD-TS packet's sync).
    // The CBC region is `unit[16..]`; payload offset 196 → region offset 180 =
    // block 11, byte 4. P[11] = AES_dec(C[11]) XOR C[10]; C[10] is raw
    // ciphertext (no decrypt needed). Constant offsets for the fixed 6144 unit.
    const SYNC_PAYLOAD_OFF: usize = 196;
    let region_off = SYNC_PAYLOAD_OFF - 16; // 180
    let blk = region_off / 16; // 11
    let byte = region_off % 16; // 4
    let c11 = 16 + blk * 16; // absolute offset of C[11] in `unit` (=192)
    let cipher = Aes128::new(GenericArray::from_slice(&decrypt_key));
    let mut b = GenericArray::clone_from_slice(&unit[c11..c11 + 16]);
    cipher.decrypt_block(&mut b);
    let prev = unit[c11 - 16 + byte]; // C[10] byte (region block 10)
    if b[byte] ^ prev != TS_SYNC {
        return false;
    }

    // Survivor (~1/256 of candidates): confirm with the authoritative full
    // decrypt + verify, so the verdict matches `decrypt_unit` exactly.
    let mut full = [0u8; ALIGNED_UNIT_LEN];
    full.copy_from_slice(&unit[..ALIGNED_UNIT_LEN]);
    decrypt_unit(&mut full, unit_key)
}

/// Outcome of [`decrypt_unit_try_keys`].
///
/// Distinguishes "the unit was already clear, no key was consumed" from "key
/// at index `i` decrypted it" — the bare `Option<usize>` form conflated the two
/// (a clear unit reported `Some(0)`, indistinguishable from key index 0, and
/// possibly out of range when `unit_keys` is empty).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitKeyResult {
    /// The unit was not scrambled; it was left untouched and no key was used.
    AlreadyClear,
    /// The unit was decrypted in place by `unit_keys[index]`.
    DecryptedWith(usize),
}

/// Decrypt one aligned unit trying multiple unit keys.
///
/// Returns [`UnitKeyResult::AlreadyClear`] if the unit was not scrambled (no key
/// consumed), [`UnitKeyResult::DecryptedWith(i)`] if key `i` decrypted it, or
/// `None` if no key worked (the unit is restored to its original bytes).
pub fn decrypt_unit_try_keys(unit: &mut [u8], unit_keys: &[[u8; 16]]) -> Option<UnitKeyResult> {
    if !is_aacs_scrambled(unit) {
        return Some(UnitKeyResult::AlreadyClear);
    }

    // Save original for retry. Stack-backed buffer — no heap allocation, and the
    // restore-on-failure contract holds uniformly regardless of key count.
    let mut original = [0u8; ALIGNED_UNIT_LEN];
    original.copy_from_slice(&unit[..ALIGNED_UNIT_LEN]);

    for (i, key) in unit_keys.iter().enumerate() {
        unit[..ALIGNED_UNIT_LEN].copy_from_slice(&original);
        if decrypt_unit(unit, key) {
            return Some(UnitKeyResult::DecryptedWith(i));
        }
    }

    // Restore original on failure
    unit[..ALIGNED_UNIT_LEN].copy_from_slice(&original);
    None
}

/// Remove bus encryption from an aligned unit (AACS 2.0 / UHD).
/// Bus encryption uses read_data_key, decrypting bytes 16..2047 of each 2048-byte sector.
pub fn decrypt_bus(unit: &mut [u8], read_data_key: &[u8; 16]) {
    for sector_start in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_LEN) {
        if sector_start + SECTOR_LEN > unit.len() {
            break;
        }
        // First 16 bytes of each sector are plaintext
        aes_cbc_decrypt(
            read_data_key,
            &mut unit[sector_start + 16..sector_start + SECTOR_LEN],
        );
    }
}

/// Full decrypt of an aligned unit: bus decrypt (if needed) then AACS decrypt.
pub fn decrypt_unit_full(
    unit: &mut [u8],
    unit_key: &[u8; 16],
    read_data_key: Option<&[u8; 16]>,
) -> bool {
    if !is_aacs_scrambled(unit) {
        return true;
    }
    if let Some(rdk) = read_data_key {
        decrypt_bus(unit, rdk);
    }
    decrypt_unit(unit, unit_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_ecb_roundtrip() {
        let key = [
            0x15u8, 0x66, 0x5F, 0x98, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C,
        ];
        let plain = [0x41u8; 16];
        let enc = aes_ecb_encrypt(&key, &plain);
        let dec = aes_ecb_decrypt(&key, &enc);
        assert_eq!(dec, plain);
    }

    #[test]
    fn test_decrypt_unit_unencrypted() {
        // A clear unit (TS syncs intact) is not scrambled → passes through.
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            unit[off] = TS_SYNC;
            off += TS_PACKET_LEN;
        }
        let key = [0u8; 16];
        assert!(!is_aacs_scrambled(&unit));
        assert!(decrypt_unit(&mut unit, &key));
    }

    #[test]
    fn ts_packet_total_no_off_by_one() {
        // The maximum sync count is exactly the number of stride
        // positions the counting loop visits (offset 4, 196, ...), i.e.
        // len / 192, NOT (len - 4) / 192 + 1. For the 6144-byte aligned unit
        // the loop checks offsets 4..=5956 → 32 positions.
        let unit = vec![0u8; ALIGNED_UNIT_LEN];
        assert_eq!(ts_packet_total(&unit), 32);
        // Confirm the loop visits exactly that many stride positions.
        let visited = (4..ALIGNED_UNIT_LEN).step_by(TS_PACKET_LEN).count();
        assert_eq!(visited, ts_packet_total(&unit));
    }

    #[test]
    fn scramble_detection_at_16_32_boundary() {
        // With 32 stride positions the majority threshold is
        // total/2 = 16. A unit with EXACTLY half its syncs intact (16) must
        // NOT be over-counted into the "scrambled" bucket by an inflated
        // total: 16 > 16 is false → not-intact → scrambled. 17 intact → clear.
        // The fix is that `total` is 32 (not 33), so the boundary sits cleanly
        // at the real midpoint.
        let set_syncs = |n: usize| {
            let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
            let mut off = 4;
            let mut placed = 0;
            while off < ALIGNED_UNIT_LEN && placed < n {
                unit[off] = TS_SYNC;
                off += TS_PACKET_LEN;
                placed += 1;
            }
            unit
        };

        assert_eq!(ts_sync_count(&set_syncs(16)), 16);
        assert_eq!(ts_sync_count(&set_syncs(17)), 17);

        // Exactly half intact → classified scrambled (16 > 16 is false).
        assert!(is_aacs_scrambled(&set_syncs(16)));
        // One past half → classified clear.
        assert!(!is_aacs_scrambled(&set_syncs(17)));
    }

    #[test]
    fn scramble_detection_extremes() {
        // Detection semantics for the clear-cut cases must be preserved:
        // a fully-clear unit (all 32 syncs) is NOT scrambled; a unit with no
        // syncs (fully scrambled body) IS scrambled.
        let mut clear = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            clear[off] = TS_SYNC;
            off += TS_PACKET_LEN;
        }
        assert_eq!(ts_sync_count(&clear), 32);
        assert!(
            !is_aacs_scrambled(&clear),
            "fully-clear unit → not scrambled"
        );

        let scrambled = vec![0u8; ALIGNED_UNIT_LEN];
        assert_eq!(ts_sync_count(&scrambled), 0);
        assert!(is_aacs_scrambled(&scrambled), "no syncs → scrambled");
    }

    #[test]
    fn test_aes_cbc_roundtrip() {
        let key = [
            0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let original = vec![0x42u8; 128]; // 8 blocks
        let mut data = original.clone();

        // Encrypt with CBC manually (forward direction)
        fn aes_cbc_encrypt(key: &[u8; 16], data: &mut [u8]) {
            let cipher = Aes128::new(GenericArray::from_slice(key));
            let mut prev = super::AACS_IV;
            let num_blocks = data.len() / 16;
            for i in 0..num_blocks {
                let offset = i * 16;
                for j in 0..16 {
                    data[offset + j] ^= prev[j];
                }
                let mut block = GenericArray::clone_from_slice(&data[offset..offset + 16]);
                cipher.encrypt_block(&mut block);
                data[offset..offset + 16].copy_from_slice(&block);
                prev.copy_from_slice(&data[offset..offset + 16]);
            }
        }

        aes_cbc_encrypt(&key, &mut data);
        assert_ne!(data, original); // should be different after encrypt

        super::aes_cbc_decrypt(&key, &mut data);
        assert_eq!(data, original); // should match after roundtrip
    }

    #[test]
    fn test_decrypt_unit_synthetic() {
        // Build a fake 6144-byte aligned unit with known TS sync pattern,
        // encrypt it with the AACS algorithm, then decrypt and verify.
        let unit_key = [0xAAu8; 16];

        // Build plaintext unit with TS sync bytes every 192 bytes starting at offset 4
        let mut plain = vec![0u8; ALIGNED_UNIT_LEN];
        let mut offset = 4;
        while offset < ALIGNED_UNIT_LEN {
            plain[offset] = TS_SYNC;
            offset += TS_PACKET_LEN;
        }
        // No flag set: CBC-encrypting the body below scrambles packets 1..31's
        // TS syncs, which is exactly what `is_aacs_scrambled` (raw-sync) detects.

        // Now encrypt bytes 16..6143 using the AACS algorithm (reverse of decrypt)
        let header: [u8; 16] = plain[..16].try_into().unwrap();
        let derived = aes_ecb_encrypt(&unit_key, &header);
        let mut encrypt_key = [0u8; 16];
        for i in 0..16 {
            encrypt_key[i] = derived[i] ^ header[i];
        }

        // CBC encrypt bytes 16..6143
        let cipher = Aes128::new(GenericArray::from_slice(&encrypt_key));
        let mut prev = AACS_IV;
        let num_blocks = (ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..num_blocks {
            let off = 16 + i * 16;
            for j in 0..16 {
                plain[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&plain[off..off + 16]);
            cipher.encrypt_block(&mut block);
            plain[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&plain[off..off + 16]);
        }

        // Now plain contains encrypted data. Decrypt it.
        let mut unit = plain;
        assert!(is_aacs_scrambled(&unit));
        assert!(decrypt_unit(&mut unit, &unit_key));
        assert!(!is_aacs_scrambled(&unit)); // decrypted: TS syncs restored

        // Verify TS sync bytes
        let mut count = 0;
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            if unit[off] == TS_SYNC {
                count += 1;
            }
            off += TS_PACKET_LEN;
        }
        // Assert against the single canonical packet count, not the old
        // `(len - 4) / 192 + 1` form that `ts_packet_total` corrected away from.
        assert_eq!(count, ts_packet_total(&unit));
    }

    // ── Helpers for the hardening tests below ──────────────────────────────

    /// Encrypt an aligned unit in place with the AACS unit-decrypt
    /// algorithm run in reverse, so [`decrypt_unit`] with the same
    /// `unit_key` recovers the plaintext. This is the exact inverse of
    /// the production decrypt: derive `decrypt_key = AES-ECB-E(unit_key,
    /// header) XOR header`, then CBC-encrypt bytes 16..6144 under the
    /// fixed AACS IV.
    fn aacs_encrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = AACS_IV;
        let num_blocks = (ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..num_blocks {
            let off = 16 + i * 16;
            for j in 0..16 {
                unit[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&unit[off..off + 16]);
            cipher.encrypt_block(&mut block);
            unit[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&unit[off..off + 16]);
        }
    }

    /// Build a clear aligned unit with TS sync bytes at offset 4 + k*192.
    fn clear_unit() -> Vec<u8> {
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            unit[off] = TS_SYNC;
            off += TS_PACKET_LEN;
        }
        unit
    }

    // ── AES-ECB KAT (FIPS-197 Appendix C.1) ────────────────────────────────

    #[test]
    fn aes_ecb_matches_fips197_known_answer() {
        // FIPS-197 Appendix C.1 AES-128 KAT:
        //   key       = 000102030405060708090a0b0c0d0e0f
        //   plaintext = 00112233445566778899aabbccddeeff
        //   ciphertext= 69c4e0d86a7b0430d8cdb78070b4c55a
        // This pins the AES primitive against a published vector — a wrong
        // cipher (or a key/plaintext byte-order slip) fails it.
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F,
        ];
        let pt = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ];
        let expected = [
            0x69, 0xC4, 0xE0, 0xD8, 0x6A, 0x7B, 0x04, 0x30, 0xD8, 0xCD, 0xB7, 0x80, 0x70, 0xB4,
            0xC5, 0x5A,
        ];
        assert_eq!(aes_ecb_encrypt(&key, &pt), expected);
        // And decrypt is the exact inverse.
        assert_eq!(aes_ecb_decrypt(&key, &expected), pt);
    }

    // ── CBC decrypt: first-block uses fixed AACS IV ────────────────────────

    #[test]
    fn cbc_decrypt_first_block_xors_aacs_iv() {
        // CBC: P[0] = AES-D(K, C[0]) XOR IV, and the IV is the fixed AACS
        // constant (not zero). Encrypt a single block forward with IV, then
        // confirm aes_cbc_decrypt recovers it — proving the IV used on block
        // 0 is exactly AACS_IV. A mutation that swaps AACS_IV for [0u8;16]
        // makes the recovered block wrong.
        let key = [0x24u8; 16];
        let plain = [0x5Au8; 16];
        // Forward CBC for one block: C = AES-E(K, P XOR IV).
        let mut x = plain;
        for j in 0..16 {
            x[j] ^= AACS_IV[j];
        }
        let ct = aes_ecb_encrypt(&key, &x);
        let mut buf = ct;
        aes_cbc_decrypt(&key, &mut buf);
        assert_eq!(buf, plain, "block-0 CBC must XOR the fixed AACS IV");
    }

    // ── decrypt_unit: full round trip restores TS syncs ────────────────────

    #[test]
    fn decrypt_unit_roundtrip_restores_all_syncs() {
        // Encrypt a clear unit, confirm it reads as scrambled, then decrypt
        // and confirm every TS sync byte at the 192-byte stride is restored.
        let unit_key = [0x37u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &unit_key);
        assert!(
            is_aacs_scrambled(&unit),
            "encrypted unit must look scrambled"
        );

        assert!(decrypt_unit(&mut unit, &unit_key));
        // All 32 stride positions carry sync after decrypt.
        assert_eq!(ts_sync_count(&unit), ts_packet_total(&unit));
        assert!(!is_aacs_scrambled(&unit));
    }

    #[test]
    fn decrypt_unit_wrong_key_fails_and_does_not_falsely_clear() {
        // A wrong unit key fails verify_ts (the body stays scrambled), so
        // decrypt_unit returns false. Grounds the brute-force gate: a bad key
        // must NOT report success.
        let good = [0x11u8; 16];
        let bad = [0x22u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &good);
        assert!(!decrypt_unit(&mut unit, &bad), "wrong key must not verify");
    }

    #[test]
    fn decrypt_unit_rejects_short_unit() {
        // unit.len() < ALIGNED_UNIT_LEN → false (no panic on the 16.. slice).
        let mut short = vec![0u8; ALIGNED_UNIT_LEN - 1];
        assert!(!decrypt_unit(&mut short, &[0u8; 16]));
    }

    #[test]
    fn decrypt_unit_only_touches_bytes_16_onward() {
        // The first 16 bytes are the plaintext TP_extra header and must be
        // left untouched by decrypt (only unit[16..] is CBC-processed).
        let unit_key = [0x9Au8; 16];
        let mut clear = clear_unit();
        // Put a distinctive header so we can confirm it survives.
        clear[..16].copy_from_slice(&[
            0xA0, 0xA1, 0xA2, 0xA3, 0x47, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ]);
        let header_before: [u8; 16] = clear[..16].try_into().unwrap();
        let mut unit = clear;
        aacs_encrypt_unit(&mut unit, &unit_key);
        // Encryption also leaves the header untouched (only 16.. is encrypted).
        assert_eq!(&unit[..16], &header_before);
        decrypt_unit(&mut unit, &unit_key);
        assert_eq!(
            &unit[..16],
            &header_before,
            "header bytes must be preserved"
        );
    }

    // ── decrypt_unit_try_keys: AlreadyClear vs DecryptedWith vs None ───────

    #[test]
    fn try_keys_reports_already_clear_without_consuming_a_key() {
        // A clear unit returns AlreadyClear even with an empty key list — the
        // old Option<usize> form conflated this with Some(0). Grounds the
        // UnitKeyResult enum distinction.
        let mut unit = clear_unit();
        assert_eq!(
            decrypt_unit_try_keys(&mut unit, &[]),
            Some(UnitKeyResult::AlreadyClear)
        );
    }

    #[test]
    fn try_keys_reports_correct_index_among_several() {
        // Three keys, only the 3rd (index 2) decrypts → DecryptedWith(2).
        let real = [0x44u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &real);
        let keys = [[0x01u8; 16], [0x02u8; 16], real];
        assert_eq!(
            decrypt_unit_try_keys(&mut unit, &keys),
            Some(UnitKeyResult::DecryptedWith(2))
        );
        assert!(
            !is_aacs_scrambled(&unit),
            "unit must be clear after the hit"
        );
    }

    #[test]
    fn try_keys_restores_original_bytes_on_total_failure() {
        // When no key works, the unit must be byte-identical to the input
        // (the function CBC-mangles it per attempt, then restores). A buggy
        // restore would leave the unit corrupted — silent data damage.
        let real = [0x55u8; 16];
        let mut unit = clear_unit();
        aacs_encrypt_unit(&mut unit, &real);
        let snapshot = unit.clone();
        let wrong = [[0xAAu8; 16], [0xBBu8; 16]];
        assert_eq!(decrypt_unit_try_keys(&mut unit, &wrong), None);
        assert_eq!(unit, snapshot, "failed try must restore the original bytes");
    }

    // ── unit_key_validates: matches decrypt_unit's verdict exactly ─────────

    #[test]
    fn unit_key_validates_agrees_with_decrypt_unit() {
        // The fast 1-byte gate's accept/reject set must be identical to the
        // authoritative decrypt_unit. Confirm: correct key → true on both;
        // wrong key → false on both.
        let good = [0x6Au8; 16];
        let bad = [0x6Bu8; 16];
        let mut enc = clear_unit();
        aacs_encrypt_unit(&mut enc, &good);

        assert!(unit_key_validates(&enc, &good));
        let mut probe = enc.clone();
        assert!(decrypt_unit(&mut probe, &good));

        assert!(!unit_key_validates(&enc, &bad));
        let mut probe2 = enc.clone();
        assert!(!decrypt_unit(&mut probe2, &bad));
    }

    #[test]
    fn unit_key_validates_is_non_mutating() {
        // The accelerator must never write its input (it operates on the
        // ciphertext and confirms on a copy). A mutation that decrypted in
        // place would corrupt the caller's buffer.
        let good = [0x7Cu8; 16];
        let mut enc = clear_unit();
        aacs_encrypt_unit(&mut enc, &good);
        let snapshot = enc.clone();
        let _ = unit_key_validates(&enc, &good);
        assert_eq!(enc, snapshot, "unit_key_validates must not mutate input");
    }

    #[test]
    fn unit_key_validates_rejects_short_unit() {
        let short = vec![0u8; ALIGNED_UNIT_LEN - 16];
        assert!(!unit_key_validates(&short, &[0u8; 16]));
    }

    // ── bus decryption (AACS 2.0 / UHD) ────────────────────────────────────

    #[test]
    fn decrypt_bus_roundtrips_per_sector_skipping_first_16_bytes() {
        // Bus encryption CBC-encrypts bytes 16..2048 of EACH 2048-byte sector
        // (3 sectors per aligned unit), leaving the first 16 plaintext. Build
        // the forward transform, then confirm decrypt_bus inverts it and
        // leaves each sector's first 16 bytes untouched.
        let rdk = [0x13u8; 16];
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        // Fill with a recognisable pattern.
        for (i, b) in unit.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let plain = unit.clone();

        // Forward: CBC-encrypt unit[s+16 .. s+2048] per sector under AACS IV.
        let cipher = Aes128::new(GenericArray::from_slice(&rdk));
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_LEN) {
            let mut prev = AACS_IV;
            let body = s + 16;
            let end = s + SECTOR_LEN;
            let nblocks = (end - body) / 16;
            for i in 0..nblocks {
                let off = body + i * 16;
                for j in 0..16 {
                    unit[off + j] ^= prev[j];
                }
                let mut blk = GenericArray::clone_from_slice(&unit[off..off + 16]);
                cipher.encrypt_block(&mut blk);
                unit[off..off + 16].copy_from_slice(&blk);
                prev.copy_from_slice(&unit[off..off + 16]);
            }
        }
        assert_ne!(unit, plain, "forward bus-encrypt must change the body");

        decrypt_bus(&mut unit, &rdk);
        assert_eq!(
            unit, plain,
            "decrypt_bus must invert per-sector bus encrypt"
        );
        // Each sector's first 16 bytes equal the original (never touched).
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_LEN) {
            assert_eq!(&unit[s..s + 16], &plain[s..s + 16]);
        }
    }

    // ── decrypt_unit_full: bus-then-AACS ordering, and clear passthrough ───

    #[test]
    fn decrypt_unit_full_passthrough_when_already_clear() {
        // A clear unit returns true and is not modified, regardless of keys.
        let mut unit = clear_unit();
        let snapshot = unit.clone();
        assert!(decrypt_unit_full(
            &mut unit,
            &[0u8; 16],
            Some(&[0xFFu8; 16])
        ));
        assert_eq!(unit, snapshot, "clear unit must pass through untouched");
    }

    #[test]
    fn decrypt_unit_full_applies_bus_then_aacs() {
        // AACS 2.0 pipeline: content is first AACS-unit-encrypted, then
        // bus-encrypted on top. Decrypt must undo bus FIRST, then AACS.
        // Build that exact two-layer ciphertext and confirm full recovery.
        let unit_key = [0x21u8; 16];
        let rdk = [0x84u8; 16];

        let mut unit = clear_unit();
        // Layer 1: AACS unit-encrypt.
        aacs_encrypt_unit(&mut unit, &unit_key);
        // Layer 2: bus-encrypt on top (per-sector, bytes 16..2048).
        let cipher = Aes128::new(GenericArray::from_slice(&rdk));
        for s in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_LEN) {
            let mut prev = AACS_IV;
            for i in 0..((SECTOR_LEN - 16) / 16) {
                let off = s + 16 + i * 16;
                for j in 0..16 {
                    unit[off + j] ^= prev[j];
                }
                let mut blk = GenericArray::clone_from_slice(&unit[off..off + 16]);
                cipher.encrypt_block(&mut blk);
                unit[off..off + 16].copy_from_slice(&blk);
                prev.copy_from_slice(&unit[off..off + 16]);
            }
        }
        assert!(is_aacs_scrambled(&unit));
        assert!(decrypt_unit_full(&mut unit, &unit_key, Some(&rdk)));
        assert_eq!(ts_sync_count(&unit), ts_packet_total(&unit));
    }

    // ── is_aacs_scrambled / ts_sync_count edge cases ───────────────────────

    #[test]
    fn is_aacs_scrambled_false_for_sub_unit_length() {
        // The function guards on `len >= ALIGNED_UNIT_LEN` first; anything
        // shorter is reported NOT scrambled (so the decrypt gate skips it)
        // rather than indexing past the end.
        assert!(!is_aacs_scrambled(&[]));
        assert!(!is_aacs_scrambled(&vec![0u8; ALIGNED_UNIT_LEN - 1]));
        // A scrambled-looking buffer that is one byte short is still "not
        // scrambled" by the length guard.
        let mut almost = vec![0u8; ALIGNED_UNIT_LEN - 1];
        almost[4] = 0x00; // no syncs
        assert!(!is_aacs_scrambled(&almost));
    }

    #[test]
    fn ts_sync_count_only_samples_the_192_byte_stride() {
        // A 0x47 placed OFF the stride (e.g. offset 5) must not be counted —
        // the detector samples exactly offset 4, 196, 388, ... A mutation that
        // scanned every byte would over-count and misclassify scrambled units.
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        unit[5] = TS_SYNC; // off-stride
        unit[197] = TS_SYNC; // off-stride
        assert_eq!(ts_sync_count(&unit), 0, "off-stride 0x47 must not count");
        unit[4] = TS_SYNC; // on-stride
        assert_eq!(ts_sync_count(&unit), 1);
    }

    #[test]
    fn ts_packet_total_for_various_lengths() {
        // total = len / 192 (BD-TS packet size). Pin a few lengths.
        assert_eq!(ts_packet_total(&[0u8; 192]), 1);
        assert_eq!(ts_packet_total(&[0u8; 384]), 2);
        assert_eq!(ts_packet_total(&[0u8; 191]), 0);
        // 6144 = 32 packets.
        assert_eq!(ts_packet_total(&[0u8; ALIGNED_UNIT_LEN]), 32);
    }
}

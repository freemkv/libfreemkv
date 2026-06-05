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
/// AES-128-CBC decrypt in-place with the fixed AACS IV.
pub(crate) fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
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

/// Most TS packet positions in `unit` carry the `0x47` sync byte — i.e. the
/// unit looks like clear MPEG-TS. Syncs sit at offset 4 and every 192 bytes
/// after (4-byte TP_extra_header + 188-byte TS packet). An encrypted body
/// scrambles all but the first (which lives in the clear 16-byte seed).
fn ts_syncs_intact(unit: &[u8]) -> bool {
    let mut count = 0;
    let mut offset = 4;
    while offset < unit.len() {
        if unit[offset] == TS_SYNC {
            count += 1;
        }
        offset += TS_PACKET_LEN;
    }
    // One sync byte is checked per 192-byte BD-TS packet (at offset 4 of
    // each). `total` is exactly that packet count; the old
    // `(len - 4) / TS_PACKET_LEN + 1` over-counted by one for lengths of
    // the form `4 + k·192` (harmless for the always-6144 aligned unit, but
    // wrong in general and it biased the majority threshold).
    let total = unit.len() / TS_PACKET_LEN;
    count > total / 2
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

/// Decrypt one aligned unit trying multiple unit keys. Returns the key index that worked.
pub fn decrypt_unit_try_keys(unit: &mut [u8], unit_keys: &[[u8; 16]]) -> Option<usize> {
    if !is_aacs_scrambled(unit) {
        return Some(0);
    }

    // Save original for retry
    let original = unit[..ALIGNED_UNIT_LEN].to_vec();

    for (i, key) in unit_keys.iter().enumerate() {
        unit[..ALIGNED_UNIT_LEN].copy_from_slice(&original);
        if decrypt_unit(unit, key) {
            return Some(i);
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
        assert_eq!(count, (ALIGNED_UNIT_LEN - 4) / TS_PACKET_LEN + 1);
    }
}

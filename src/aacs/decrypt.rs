//! AACS content decryption — AES primitives, unit decryption, bus encryption.

use aes::cipher::{generic_array::GenericArray, BlockDecrypt, BlockEncrypt, KeyInit};
use aes::Aes128;

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
pub fn aes_ecb_decrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
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

/// Check if a 6144-byte aligned unit is encrypted (copy_permission_indicator bits).
pub fn is_unit_encrypted(unit: &[u8]) -> bool {
    unit.len() >= ALIGNED_UNIT_LEN && (unit[0] & 0xC0) != 0
}

/// Verify decrypted unit by checking TS sync bytes at expected offsets.
fn verify_ts(unit: &[u8]) -> bool {
    // In a 6144-byte unit, TS packets start at byte 0 with 4-byte TP_extra_header
    // then 188-byte TS packet, repeating every 192 bytes.
    // Sync byte 0x47 should appear at offset 4, 196, 388, ...
    let mut count = 0;
    let mut offset = 4;
    while offset < unit.len() {
        if unit[offset] == TS_SYNC {
            count += 1;
        }
        offset += TS_PACKET_LEN;
    }
    // Expect at least most packets to have sync bytes
    let total = (unit.len() - 4) / TS_PACKET_LEN + 1;
    count > total / 2
}

/// Decrypt one AACS aligned unit (6144 bytes) in-place.
/// Returns true if decryption succeeded (verified by TS sync bytes).
///
/// Algorithm:
/// 1. AES-128-ECB encrypt first 16 bytes with unit_key → derived
/// 2. XOR derived with original 16 bytes → unit_decrypt_key
/// 3. AES-128-CBC decrypt bytes 16..6143 with unit_decrypt_key and AACS IV
/// 4. Clear encryption flag bits
pub fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    if !is_unit_encrypted(unit) {
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

    // Step 4: Clear encryption flag
    unit[0] &= !0xC0;

    // Verify
    verify_ts(unit)
}

/// Decrypt one aligned unit trying multiple unit keys. Returns the key index that worked.
pub fn decrypt_unit_try_keys(unit: &mut [u8], unit_keys: &[[u8; 16]]) -> Option<usize> {
    if !is_unit_encrypted(unit) {
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
    if !is_unit_encrypted(unit) {
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
        // Unit with 0xC0 bits clear should pass through unchanged
        let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
        unit[0] = 0x00; // not encrypted
        let key = [0u8; 16];
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
        // Set encryption flag
        plain[0] |= 0xC0;

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
        assert!(is_unit_encrypted(&unit));
        assert!(decrypt_unit(&mut unit, &unit_key));
        assert!(!is_unit_encrypted(&unit)); // flag should be cleared

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

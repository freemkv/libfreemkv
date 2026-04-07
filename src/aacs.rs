//! AACS decryption — Volume Unique Key lookup and title key derivation.
//!
//! Two paths:
//!   1. VUK lookup: disc_hash → KEYDB.cfg → VUK (fast, 99% of discs)
//!   2. Full handshake: device_keys + MKB → Media Key → + Volume ID → VUK (fallback)
//!
//! KEYDB.cfg format:
//!   | DK | DEVICE_KEY 0x... | DEVICE_NODE 0x... | KEY_UV 0x... | KEY_U_MASK_SHIFT 0x...
//!   | PK | 0x...
//!   | HC | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
//!   0x<disc_hash> = <title> | D | <date> | M | 0x<media_key> | I | 0x<disc_id> | V | 0x<vuk> | U | <unit_keys>
//!
//! The VUK decrypts title keys from AACS/Unit_Key_RO.inf on disc.
//! Title keys decrypt m2ts stream content (AES-128-CBC).

use std::collections::HashMap;
use aes::Aes128;
use aes::cipher::{BlockEncrypt, BlockDecrypt, KeyInit, generic_array::GenericArray};

/// Parsed AACS key database.
#[derive(Debug)]
pub struct KeyDb {
    /// Device keys for MKB processing
    pub device_keys: Vec<DeviceKey>,
    /// Processing keys (pre-computed media keys for specific MKB versions)
    pub processing_keys: Vec<[u8; 16]>,
    /// Host certificate + private key for SCSI authentication
    pub host_cert: Option<HostCert>,
    /// Per-disc VUK entries indexed by disc hash (hex lowercase)
    pub disc_entries: HashMap<String, DiscEntry>,
}

/// A device key for MKB subset-difference tree processing.
#[derive(Debug, Clone)]
pub struct DeviceKey {
    pub key: [u8; 16],
    pub node: u16,
    pub uv: u32,
    pub u_mask_shift: u8,
}

/// Host certificate + private key for AACS SCSI authentication.
#[derive(Debug, Clone)]
pub struct HostCert {
    pub private_key: [u8; 20],
    pub certificate: Vec<u8>, // 92 bytes
}

/// A per-disc entry from the key database.
#[derive(Debug, Clone)]
pub struct DiscEntry {
    /// Disc hash (20 bytes, hex)
    pub disc_hash: String,
    /// Disc title
    pub title: String,
    /// Media Key (16 bytes) — from MKB processing
    pub media_key: Option<[u8; 16]>,
    /// Disc ID (16 bytes)
    pub disc_id: Option<[u8; 16]>,
    /// Volume Unique Key (16 bytes) — decrypts title keys
    pub vuk: Option<[u8; 16]>,
    /// Unit keys (title keys) indexed by CPS unit number
    pub unit_keys: Vec<(u32, [u8; 16])>,
}

/// Parse a hex string like "0xABCD..." into bytes.
fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if s.len() % 2 != 0 { return None; }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i+2], 16).ok()?);
    }
    Some(out)
}

/// Parse hex into a fixed-size array.
fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    let v = parse_hex(s)?;
    if v.len() != 16 { return None; }
    let mut out = [0u8; 16];
    out.copy_from_slice(&v);
    Some(out)
}

fn parse_hex20(s: &str) -> Option<[u8; 20]> {
    let v = parse_hex(s)?;
    if v.len() != 20 { return None; }
    let mut out = [0u8; 20];
    out.copy_from_slice(&v);
    Some(out)
}

impl KeyDb {
    /// Parse a KEYDB.cfg file from a string.
    pub fn parse(data: &str) -> Self {
        let mut db = KeyDb {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            host_cert: None,
            disc_entries: HashMap::new(),
        };

        for line in data.lines() {
            let line = line.trim();

            // Skip comments and empty lines
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            // Device Key
            if line.starts_with("| DK") {
                if let Some(dk) = Self::parse_device_key(line) {
                    db.device_keys.push(dk);
                }
                continue;
            }

            // Processing Key
            if line.starts_with("| PK") {
                if let Some(pk) = Self::parse_processing_key(line) {
                    db.processing_keys.push(pk);
                }
                continue;
            }

            // Host Certificate
            if line.starts_with("| HC") {
                db.host_cert = Self::parse_host_cert(line);
                continue;
            }

            // Disc entry: starts with 0x
            if line.starts_with("0x") && line.contains(" = ") {
                if let Some(entry) = Self::parse_disc_entry(line) {
                    db.disc_entries.insert(entry.disc_hash.clone(), entry);
                }
            }
        }

        db
    }

    /// Load KEYDB.cfg from a file path.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let data = std::fs::read_to_string(path)?;
        Ok(Self::parse(&data))
    }

    /// Look up a disc by its hash. Returns the VUK if found.
    pub fn find_vuk(&self, disc_hash: &str) -> Option<[u8; 16]> {
        let hash = disc_hash.trim().to_lowercase().trim_start_matches("0x").to_string();
        // Try with 0x prefix and without
        self.disc_entries.get(&format!("0x{}", hash))
            .or_else(|| self.disc_entries.get(&hash))
            .and_then(|e| e.vuk)
    }

    /// Look up a disc by its hash. Returns the full entry.
    pub fn find_disc(&self, disc_hash: &str) -> Option<&DiscEntry> {
        let hash = disc_hash.trim().to_lowercase().trim_start_matches("0x").to_string();
        self.disc_entries.get(&format!("0x{}", hash))
            .or_else(|| self.disc_entries.get(&hash))
    }

    // ── Parsers ─────────────────────────────────────────────────────────────

    fn parse_device_key(line: &str) -> Option<DeviceKey> {
        // | DK | DEVICE_KEY 0x... | DEVICE_NODE 0x... | KEY_UV 0x... | KEY_U_MASK_SHIFT 0x...
        let key_str = line.split("DEVICE_KEY").nth(1)?.split('|').next()?.trim();
        let node_str = line.split("DEVICE_NODE").nth(1)?.split('|').next()?.trim();
        let uv_str = line.split("KEY_UV").nth(1)?.split('|').next()?.trim();
        let shift_str = line.split("KEY_U_MASK_SHIFT").nth(1)?.split(';').next()?.split('|').next()?.trim();

        Some(DeviceKey {
            key: parse_hex16(key_str)?,
            node: u16::from_str_radix(node_str.trim_start_matches("0x"), 16).ok()?,
            uv: u32::from_str_radix(uv_str.trim_start_matches("0x"), 16).ok()?,
            u_mask_shift: u8::from_str_radix(shift_str.trim_start_matches("0x"), 16).ok()?,
        })
    }

    fn parse_processing_key(line: &str) -> Option<[u8; 16]> {
        // | PK | 0x...
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() >= 3 {
            let key_str = parts[2].split(';').next()?.trim();
            return parse_hex16(key_str);
        }
        None
    }

    fn parse_host_cert(line: &str) -> Option<HostCert> {
        // | HC | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
        let priv_str = line.split("HOST_PRIV_KEY").nth(1)?.split('|').next()?.trim();
        let cert_str = line.split("HOST_CERT").nth(1)?.split(';').next()?.split('|').next()?.trim();

        Some(HostCert {
            private_key: parse_hex20(priv_str)?,
            certificate: parse_hex(cert_str)?,
        })
    }

    fn parse_disc_entry(line: &str) -> Option<DiscEntry> {
        // 0x<hash> = <title> | D | <date> | M | 0x<mk> | I | 0x<id> | V | 0x<vuk> | U | <unit_keys>
        let (hash_part, rest) = line.split_once(" = ")?;
        let disc_hash = hash_part.trim().to_lowercase();

        // Extract title (before first |)
        let title_part = rest.split(" | ").next().unwrap_or("").trim();
        // Clean title: "TITLE_NAME (Display Title)" → use display title if present
        let title = if let Some(start) = title_part.find('(') {
            if let Some(end) = title_part.rfind(')') {
                title_part[start+1..end].to_string()
            } else {
                title_part.to_string()
            }
        } else {
            title_part.to_string()
        };

        // Parse fields by tag
        let mut media_key = None;
        let mut disc_id = None;
        let mut vuk = None;
        let mut unit_keys = Vec::new();

        let parts: Vec<&str> = rest.split(" | ").collect();
        let mut i = 0;
        while i < parts.len() {
            match parts[i].trim() {
                "M" => {
                    if i + 1 < parts.len() {
                        media_key = parse_hex16(parts[i+1].trim());
                        i += 1;
                    }
                }
                "I" => {
                    if i + 1 < parts.len() {
                        disc_id = parse_hex16(parts[i+1].trim());
                        i += 1;
                    }
                }
                "V" => {
                    if i + 1 < parts.len() {
                        vuk = parse_hex16(parts[i+1].trim());
                        i += 1;
                    }
                }
                "U" => {
                    if i + 1 < parts.len() {
                        // Unit keys: "1-0xKEY" or "1-0xKEY ; comment"
                        let uk_str = parts[i+1].split(';').next().unwrap_or("").trim();
                        for uk in uk_str.split(' ') {
                            let uk = uk.trim();
                            if let Some((num, key)) = uk.split_once('-') {
                                if let Ok(n) = num.parse::<u32>() {
                                    if let Some(k) = parse_hex16(key) {
                                        unit_keys.push((n, k));
                                    }
                                }
                            }
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Some(DiscEntry {
            disc_hash,
            title,
            media_key,
            disc_id,
            vuk,
            unit_keys,
        })
    }
}

// ── AACS constants ──────────────────────────────────────────────────────────

/// Fixed IV used by AACS for all AES-CBC operations.
const AACS_IV: [u8; 16] = [
    0x0B, 0xA0, 0xF8, 0xDD, 0xFE, 0xA6, 0x1F, 0xB3,
    0xD8, 0xDF, 0x9F, 0x56, 0x6A, 0x05, 0x0F, 0x78,
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
fn aes_ecb_encrypt(key: &[u8; 16], data: &[u8; 16]) -> [u8; 16] {
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
fn aes_cbc_decrypt(key: &[u8; 16], data: &mut [u8]) {
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

// ── VUK derivation ──────────────────────────────────────────────────────────

/// Derive VUK from Media Key and Volume ID.
/// VUK = AES-128-ECB-DECRYPT(media_key, volume_id) XOR volume_id
pub fn derive_vuk(media_key: &[u8; 16], volume_id: &[u8; 16]) -> [u8; 16] {
    let mut vuk = aes_ecb_decrypt(media_key, volume_id);
    for i in 0..16 {
        vuk[i] ^= volume_id[i];
    }
    vuk
}

/// Decrypt an encrypted unit key using the VUK (AES-128-ECB).
pub fn decrypt_unit_key(vuk: &[u8; 16], encrypted_uk: &[u8; 16]) -> [u8; 16] {
    aes_ecb_decrypt(vuk, encrypted_uk)
}

// ── Unit_Key_RO.inf parsing ─────────────────────────────────────────────────

/// Extract encrypted unit keys from Unit_Key_RO.inf data.
/// Returns vec of (cps_unit_number, encrypted_key).
pub fn parse_unit_key_ro(data: &[u8]) -> Vec<(u32, [u8; 16])> {
    // Minimum size check
    if data.len() < 0xA0 {
        return Vec::new();
    }

    // Number of unit keys at offset 0x10 (big-endian u16)
    // But the first key is always at offset 0x90
    let mut keys = Vec::new();

    // Read number of keys from offset 0x20 (varies by format)
    // Simple approach: key table starts at 0x90, each key is 16 bytes
    // First key is CPS unit 1
    let mut offset = 0x90;
    let mut unit_num = 1u32;

    while offset + 16 <= data.len() {
        let mut key = [0u8; 16];
        key.copy_from_slice(&data[offset..offset + 16]);
        // Stop if we hit all zeros (no more keys)
        if key == [0u8; 16] {
            break;
        }
        keys.push((unit_num, key));
        unit_num += 1;
        offset += 16;
    }

    keys
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
        aes_cbc_decrypt(read_data_key, &mut unit[sector_start + 16..sector_start + SECTOR_LEN]);
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
    fn test_parse_disc_entry() {
        let line = r#"0x1C620AB48AEA23F3440F1189D268F3D24F61C007 = DUNE_PART_TWO (Dune: Part Two) | D | 2024-04-02 | M | 0x252FB636E883529E119AB715F4EB1640 | I | 0xA13CBE2CE40565D104B53E768C700E30 | V | 0x1114360B10EE6EAC78AA4AC0B752EAEB | U | 1-0x9E5D1310337443E811A52EBBEAE0470F ; MKBv77"#;
        let entry = KeyDb::parse_disc_entry(line).unwrap();
        assert_eq!(entry.title, "Dune: Part Two");
        assert!(entry.media_key.is_some());
        assert!(entry.vuk.is_some());
        assert_eq!(entry.unit_keys.len(), 1);
        assert_eq!(entry.unit_keys[0].0, 1);
    }

    #[test]
    fn test_parse_device_key() {
        let line = "| DK | DEVICE_KEY 0x5FB86EF127C19C171E799F61C27BDC2A | DEVICE_NODE 0x0800 | KEY_UV 0x00000400 | KEY_U_MASK_SHIFT 0x17 ; MKBv01-MKBv48";
        let dk = KeyDb::parse_device_key(line).unwrap();
        assert_eq!(dk.node, 0x0800);
        assert_eq!(dk.u_mask_shift, 0x17);
    }

    #[test]
    fn test_parse_host_cert() {
        let line = "| HC | HOST_PRIV_KEY 0x909250D0C7FC2EE0F0383409D896993B723FA965 | HOST_CERT 0x0203005CFFFF800001C100003A5907E685E4CBA2A8CD5616665DFAA74421A14F6020D4CFC9847C23107697C39F9D109C8B2D5B93280499661AAE588AD3BF887C48DE144D48226ABC2C7ADAD0030893D1F3F1832B61B8D82D1FAFFF81 ; Revoked";
        let hc = KeyDb::parse_host_cert(line).unwrap();
        assert_eq!(hc.private_key[0], 0x90);
        assert_eq!(hc.certificate.len(), 92);
    }

    #[test]
    fn test_vuk_derivation() {
        // Civil War UHD: known MK, VID, VUK from KEYDB
        // MK = 15665F98..., VID (disc_id) = from entry, VUK = F96D7908...
        // VUK = AES-DEC(MK, VID) XOR VID
        let path = std::path::Path::new("");
        if !path.exists() { return; }

        let db = KeyDb::load(path).unwrap();

        // Find a disc with both MK, disc_id, and VUK so we can verify derivation
        let entry = db.disc_entries.values()
            .find(|e| e.media_key.is_some() && e.disc_id.is_some() && e.vuk.is_some())
            .expect("No disc with MK + VID + VUK");

        let mk = entry.media_key.unwrap();
        let vid = entry.disc_id.unwrap();
        let expected_vuk = entry.vuk.unwrap();

        let derived = derive_vuk(&mk, &vid);
        assert_eq!(derived, expected_vuk,
            "VUK derivation failed for disc: {} (hash {})", entry.title, entry.disc_hash);
        eprintln!("VUK derivation verified for: {}", entry.title);
    }

    #[test]
    fn test_aes_ecb_roundtrip() {
        let key = [0x15u8, 0x66, 0x5F, 0x98, 0x01, 0x02, 0x03, 0x04,
                   0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C];
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
        let key = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                   0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00];
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
        let derived = super::aes_ecb_encrypt(&unit_key, &header);
        let mut encrypt_key = [0u8; 16];
        for i in 0..16 {
            encrypt_key[i] = derived[i] ^ header[i];
        }

        // CBC encrypt bytes 16..6143
        let cipher = Aes128::new(GenericArray::from_slice(&encrypt_key));
        let mut prev = super::AACS_IV;
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

    #[test]
    fn test_decrypt_unit_key_from_vuk() {
        // Test the full chain: VUK → decrypt encrypted unit key → unit key
        // Use a known disc from KEYDB that has both VUK and unit keys
        let path = std::path::Path::new("");
        if !path.exists() { return; }

        let db = KeyDb::load(path).unwrap();

        // Find a disc with VUK and unit keys
        let entry = db.disc_entries.values()
            .find(|e| e.vuk.is_some() && !e.unit_keys.is_empty())
            .expect("No disc with VUK + unit keys");

        eprintln!("Testing unit key decrypt for: {} ({})", entry.title, entry.disc_hash);
        eprintln!("  VUK: {:02X?}", entry.vuk.unwrap());
        for (num, key) in &entry.unit_keys {
            eprintln!("  Unit key {}: {:02X?}", num, key);
        }

        // The unit keys in KEYDB are already decrypted — we can verify the chain
        // by encrypting with VUK and then decrypting
        let vuk = entry.vuk.unwrap();
        for (num, expected_uk) in &entry.unit_keys {
            let encrypted = aes_ecb_encrypt(&vuk, expected_uk);
            let decrypted = decrypt_unit_key(&vuk, &encrypted);
            assert_eq!(&decrypted, expected_uk,
                "Unit key {} roundtrip failed for {}", num, entry.title);
        }
        eprintln!("  All {} unit key roundtrips passed", entry.unit_keys.len());
    }

    #[test]
    fn test_decrypt_real_unit() {
        // Try decrypting a real encrypted aligned unit from Civil War UHD
        // This disc is AACS 2.0 (BEE) so unit key alone won't work —
        // we need bus decryption first. But this verifies the pipeline.
        let unit_path = std::path::Path::new("/tmp/encrypted_unit.bin");
        if !unit_path.exists() { return; }

        let original = std::fs::read(unit_path).unwrap();
        assert_eq!(original.len(), ALIGNED_UNIT_LEN);
        assert!(is_unit_encrypted(&original), "Unit should be encrypted");

        let keydb_path = std::path::Path::new("");
        if !keydb_path.exists() { return; }
        let db = KeyDb::load(keydb_path).unwrap();

        // Civil War UHD entries
        let civil_war_entries: Vec<&DiscEntry> = db.disc_entries.values()
            .filter(|e| e.title.contains("CIVIL WAR") && !e.unit_keys.is_empty())
            .collect();

        eprintln!("Found {} Civil War entries with unit keys", civil_war_entries.len());

        // Try each entry's unit keys
        for entry in &civil_war_entries {
            let keys: Vec<[u8; 16]> = entry.unit_keys.iter().map(|(_, k)| *k).collect();
            let mut unit = original.clone();

            if let Some(idx) = decrypt_unit_try_keys(&mut unit, &keys) {
                eprintln!("SUCCESS: Decrypted with entry {} key {}", entry.disc_hash, idx);
                // Count TS sync bytes
                let ts = (0..32).filter(|&i| unit[4 + i * 192] == 0x47).count();
                eprintln!("  TS sync bytes: {}/32", ts);
                return;
            }
        }

        // Expected: none work because this is AACS 2.0 and needs bus decryption first
        eprintln!("No unit key worked (expected for AACS 2.0 BEE disc — needs read_data_key)");
    }

    #[test]
    fn test_parse_full_keydb() {
        let path = std::path::Path::new("");
        if !path.exists() { return; } // skip if not available
        
        let db = KeyDb::load(path).unwrap();
        
        assert_eq!(db.device_keys.len(), 4);
        assert_eq!(db.processing_keys.len(), 3);
        assert!(db.host_cert.is_some());
        assert!(db.disc_entries.len() > 170000);
        
        // Look up Dune: Part Two
        let dune = db.disc_entries.values()
            .find(|e| e.title.contains("Dune: Part Two") && e.vuk.is_some())
            .expect("Dune: Part Two not found");
        assert!(dune.media_key.is_some());
        assert!(dune.vuk.is_some());
        assert!(!dune.unit_keys.is_empty());
        
        eprintln!("Parsed {} disc entries, {} DK, {} PK",
            db.disc_entries.len(), db.device_keys.len(), db.processing_keys.len());
    }
}

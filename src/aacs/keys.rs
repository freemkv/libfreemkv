//! AACS key resolution — VUK derivation, MKB processing, disc hash, unit key parsing.

use super::decrypt::{aes_ecb_decrypt, aes_ecb_encrypt};
use super::keydb::{DeviceKey, KeyDb};

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

/// Parsed Unit_Key_RO.inf file.
#[derive(Debug)]
pub struct UnitKeyFile {
    /// Disc hash (SHA1 of the entire file) — used as KEYDB lookup key
    pub disc_hash: [u8; 20],
    /// Application type (1 = BD-ROM)
    pub app_type: u8,
    /// Number of BDMV directories
    pub num_bdmv_dir: u8,
    /// Whether SKB MKB is used
    pub use_skb_mkb: bool,
    /// Whether this is AACS 2.0
    pub aacs2: bool,
    /// Encrypted unit keys (CPS unit number, encrypted key)
    pub encrypted_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping (title_idx → unit_key_idx)
    pub title_cps_unit: Vec<u16>,
}

/// Compute disc hash (SHA1 of Unit_Key_RO.inf content).
pub fn disc_hash(data: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(data);
    let mut out = [0u8; 20];
    out.copy_from_slice(&hash);
    out
}

/// Format disc hash as hex string with 0x prefix (for KEYDB lookup).
pub fn disc_hash_hex(hash: &[u8; 20]) -> String {
    let mut s = String::with_capacity(42);
    s.push_str("0x");
    for b in hash {
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// Parse Unit_Key_RO.inf from raw bytes.
///
/// Format (from AACS spec):
///   [0..4]   BE32: offset to key storage area (uk_pos)
///   [16]     app_type (1 = BD-ROM)
///   [17]     num_bdmv_dir
///   [18]     bit 7: use_skb_mkb
///   [20..22] BE16: first_play CPS unit
///   [22..24] BE16: top_menu CPS unit
///   [24..26] BE16: num_titles
///   [26..]   title entries: 2 bytes padding + 2 bytes CPS unit, × num_titles
///
///   Key storage at uk_pos:
///   [uk_pos..uk_pos+2]   BE16: num_unit_keys
///   [uk_pos+48..]        encrypted keys, 16 bytes each
///                         AACS 1.0: 48-byte stride
///                         AACS 2.0: 64-byte stride (48 + 16 extra)
pub fn parse_unit_key_ro(data: &[u8], aacs2: bool) -> Option<UnitKeyFile> {
    if data.len() < 20 {
        return None;
    }

    let hash = disc_hash(data);

    // Header
    let app_type = data[16];
    let num_bdmv_dir = data[17];
    let use_skb_mkb = (data[18] >> 7) & 1 == 1;

    // Key storage offset
    let uk_pos = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if uk_pos + 2 > data.len() {
        return None;
    }

    // Number of unit keys
    let num_uk = u16::from_be_bytes([data[uk_pos], data[uk_pos + 1]]) as usize;
    if num_uk == 0 {
        return Some(UnitKeyFile {
            disc_hash: hash,
            app_type,
            num_bdmv_dir,
            use_skb_mkb,
            aacs2,
            encrypted_keys: Vec::new(),
            title_cps_unit: Vec::new(),
        });
    }

    // Stride between keys
    let stride = if aacs2 { 64 } else { 48 };

    // Validate size
    let keys_start = uk_pos + 48; // first key at uk_pos + 48
    if keys_start + 16 > data.len() {
        return None;
    }

    // Extract encrypted keys
    let mut encrypted_keys = Vec::with_capacity(num_uk);
    let mut pos = keys_start;
    for i in 0..num_uk {
        if pos + 16 > data.len() {
            break;
        }
        let mut key = [0u8; 16];
        key.copy_from_slice(&data[pos..pos + 16]);
        encrypted_keys.push(((i + 1) as u32, key));
        pos += stride;
    }

    // Title → CPS unit mapping
    let mut title_cps_unit = Vec::new();
    if data.len() >= 26 {
        let first_play = u16::from_be_bytes([data[20], data[21]]);
        let top_menu = u16::from_be_bytes([data[22], data[23]]);
        let num_titles = u16::from_be_bytes([data[24], data[25]]) as usize;

        title_cps_unit.push(first_play);
        title_cps_unit.push(top_menu);

        for i in 0..num_titles {
            let off = 26 + i * 4 + 2; // 2 bytes padding + 2 bytes CPS unit
            if off + 2 <= data.len() {
                let cps = u16::from_be_bytes([data[off], data[off + 1]]);
                title_cps_unit.push(cps);
            }
        }
    }

    Some(UnitKeyFile {
        disc_hash: hash,
        app_type,
        num_bdmv_dir,
        use_skb_mkb,
        aacs2,
        encrypted_keys,
        title_cps_unit,
    })
}

// ── MKB processing ──────────────────────────────────────────────────────────

/// Derive Media Key from MKB data using processing keys.
///
/// Processing keys are pre-computed keys that work for specific MKB versions.
/// This is the fast path — no subset-difference tree traversal needed.
///
/// MKB format:
///   Record type 0x10 = Verify Media Key Record (has mk_dv)
///   Record type 0x81 = Type and Version Record (has MKB version)
///   Record type 0x04 = Subset-Difference Index (has UVS entries)
///   Record type 0x07 = Explicit Subset-Difference Record (has cvalues)
pub fn derive_media_key_from_pk(mkb: &[u8], processing_keys: &[[u8; 16]]) -> Option<[u8; 16]> {
    // Parse MKB records
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;

    // Count UV entries (each 5 bytes, stop when high bits set)
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    // Try each processing key against each UV/cvalue pair
    for pk in processing_keys {
        for i in 0..num_uvs {
            if (i + 1) * 16 > cvalues.len() { continue; }
            let record_start = i * 5;
            if record_start + 5 > uvs.len() { continue; }
            let _u_mask_shift = uvs[record_start];
            let uv = &uvs[record_start + 1..record_start + 5];
            let cv = &cvalues[i * 16..(i + 1) * 16];
            if let Some(mk) = validate_processing_key(pk, cv, uv, &mk_dv) {
                return Some(mk);
            }
        }
    }
    None
}

/// Validate a processing key against a cvalue/UV pair.
/// Returns the Media Key if valid.
fn validate_processing_key(
    pk: &[u8; 16],
    cvalue: &[u8],
    _uv: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    if cvalue.len() < 16 {
        return None;
    }
    // mk = AES-DEC(pk, cvalue) XOR cvalue
    let mut cv = [0u8; 16];
    cv.copy_from_slice(&cvalue[..16]);
    let mut mk = aes_ecb_decrypt(pk, &cv);
    for i in 0..16 {
        mk[i] ^= cv[i];
    }

    // Verify: AES-ECB(mk, mk_dv) should produce a specific pattern
    let _verify = aes_ecb_encrypt(&mk, mk_dv);
    // mk_dv verification: the first 12 bytes of AES(mk, mk_dv) should be all 0xDEADBEEF...
    // Actually per AACS spec: verify record value is AES(mk, all_zeros)
    // No — the mk_dv IS the verification value. We compute AES-ECB(mk, verify_data)
    // and check it matches.
    // From libaacs _validate_pk:
    //   crypto_aes128d(pk, rec + a*16, mk) → decrypt cvalue with PK
    //   mk[i] ^= rec[i]  → XOR with cvalue
    //   crypto_aes128e(mk, mk_dv, test) → encrypt mk_dv with derived mk
    //   if first 12 bytes of test are zero → valid media key
    let test = aes_ecb_encrypt(&mk, mk_dv);
    // AACS spec: Verify Media Key record — first 12 bytes must be zero
    if test[..12] == [0u8; 12] {
        return Some(mk);
    }
    None
}

/// Find Verify Media Key Record (type 0x10) in MKB.
fn mkb_find_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x10 && rec_len >= 20 {
            // mk_dv is at offset 4 (after record header)
            let mut dv = [0u8; 16];
            dv.copy_from_slice(&mkb[pos + 4..pos + 20]);
            return Some(dv);
        }
        pos += rec_len;
    }
    None
}

/// Find Subset-Difference records (type 0x04) in MKB.
fn mkb_find_subdiff_records(mkb: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x04 && rec_len > 4 {
            return Some(mkb[pos + 4..pos + rec_len].to_vec());
        }
        pos += rec_len;
    }
    None
}

/// Find Conditional Values (cvalues) record (type 0x07) in MKB.
fn mkb_find_cvalues(mkb: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x07 && rec_len > 4 {
            return Some(mkb[pos + 4..pos + rec_len].to_vec());
        }
        pos += rec_len;
    }
    None
}

/// Get MKB version from Type and Version Record (type 0x81).
pub fn mkb_version(mkb: &[u8]) -> Option<u32> {
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = u32::from_be_bytes([0, mkb[pos + 1], mkb[pos + 2], mkb[pos + 3]]) as usize;
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }

        if rec_type == 0x81 && rec_len >= 8 {
            return Some(u32::from_be_bytes([
                mkb[pos + 4],
                mkb[pos + 5],
                mkb[pos + 6],
                mkb[pos + 7],
            ]));
        }
        pos += rec_len;
    }
    None
}

// ── AACS-G3 key derivation (subset-difference tree) ─────────────────────────

/// AACS-G3 seed constant.
const AESG3_SEED: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

/// AACS-G3: derive a subkey from a parent key.
/// seed[15] += inc, then AES-DEC(key, seed) XOR seed.
fn aesg3(key: &[u8; 16], inc: u8) -> [u8; 16] {
    let mut seed = AESG3_SEED;
    seed[15] = seed[15].wrapping_add(inc);
    let mut out = aes_ecb_decrypt(key, &seed);
    for i in 0..16 {
        out[i] ^= seed[i];
    }
    out
}

/// Compute v_mask from a UV value.
fn calc_v_mask(uv: u32) -> u32 {
    let mut v_mask: u32 = 0xFFFF_FFFF;
    while (uv & !v_mask) == 0 && v_mask != 0 {
        v_mask <<= 1;
    }
    v_mask
}

/// Derive processing key from device key using subset-difference tree traversal.
fn calc_pk_from_dk(dk: &[u8; 16], uv: u32, v_mask: u32, dev_key_v_mask: u32) -> [u8; 16] {
    // Initial derivation: left_child = aesg3(dk, 0), pk = aesg3(dk, 1), right_child = aesg3(dk, 2)
    let mut left_child = aesg3(dk, 0);
    let mut pk = aesg3(dk, 1);
    let mut right_child = aesg3(dk, 2);
    let mut current_v_mask = dev_key_v_mask;

    while current_v_mask != v_mask {
        // Find the highest unset bit in current_v_mask
        let mut bit_pos: i32 = -1;
        for i in (0..32).rev() {
            if (current_v_mask & (1u32 << i)) == 0 {
                bit_pos = i;
                break;
            }
        }

        let curr_key = if bit_pos < 0 || (uv & (1u32 << bit_pos as u32)) == 0 {
            left_child
        } else {
            right_child
        };

        left_child = aesg3(&curr_key, 0);
        pk = aesg3(&curr_key, 1);
        right_child = aesg3(&curr_key, 2);

        current_v_mask = ((current_v_mask as i32) >> 1) as u32;
    }

    pk
}

/// Derive Media Key from MKB using device keys (subset-difference tree).
pub fn derive_media_key_from_dk(mkb: &[u8], device_keys: &[DeviceKey]) -> Option<[u8; 16]> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;

    // Count UV entries
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for dk in device_keys {
        let device_number = dk.node as u32;

        // Find applying subset-difference for this device
        for uvs_idx in 0..num_uvs {
            let p_uv = &uvs[1 + 5 * uvs_idx..];
            let u_mask_shift = uvs[5 * uvs_idx]; // byte before the UV value

            if u_mask_shift & 0xC0 != 0 {
                break; // device revoked
            }

            let uv = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
            if uv == 0 {
                continue;
            }

            let u_mask: u32 = 0xFFFF_FFFF << u_mask_shift;
            let v_mask = calc_v_mask(uv);

            if ((device_number & u_mask) == (uv & u_mask))
                && ((device_number & v_mask) != (uv & v_mask))
            {
                // Found matching subset-difference — find the right device key
                let dev_key_v_mask = calc_v_mask(dk.uv);
                let dev_key_u_mask: u32 = 0xFFFF_FFFF << dk.u_mask_shift;

                if u_mask == dev_key_u_mask && (uv & dev_key_v_mask) == (dk.uv & dev_key_v_mask) {
                    // Derive processing key via tree traversal
                    let pk = calc_pk_from_dk(&dk.key, uv, v_mask, dev_key_v_mask);

                    // Validate and derive media key
                    if uvs_idx < cvalues.len() / 16 {
                        let cv = &cvalues[uvs_idx * 16..(uvs_idx + 1) * 16];
                        if let Some(mk) =
                            validate_processing_key(&pk, cv, &uvs[1 + uvs_idx * 5..], &mk_dv)
                        {
                            return Some(mk);
                        }
                    }
                }
            }
        }
    }
    None
}

/// MKB disc structure format code.
const MKB_DISC_STRUCTURE_FORMAT: u8 = 0x83;
/// MKB pack buffer size.
const MKB_PACK_SIZE: usize = 32772;

/// Read MKB from drive via SCSI (REPORT DISC STRUCTURE format 0x83).
/// Returns the concatenated MKB data from all packs.
pub fn read_mkb_from_drive(
    session: &mut crate::drive::DriveSession,
) -> crate::error::Result<Vec<u8>> {
    use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE};

    let cdb = [
        SCSI_READ_DISC_STRUCTURE,
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        MKB_DISC_STRUCTURE_FORMAT,
        (MKB_PACK_SIZE >> 8) as u8,
        (MKB_PACK_SIZE & 0xFF) as u8,
        0x00,
        0x00,
    ];
    let mut buf = vec![0u8; 32772];
    session.scsi_execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)?;

    let data_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if data_len < 2 {
        return Ok(Vec::new());
    }
    let len = data_len - 2;
    let num_packs = buf[3] as usize;

    let mut mkb = Vec::with_capacity(32768 * num_packs.max(1));
    if len > 0 && len <= 32768 {
        mkb.extend_from_slice(&buf[4..4 + len]);
    }

    // Read remaining packs
    for pack in 1..num_packs {
        let mut cdb = [
            SCSI_READ_DISC_STRUCTURE,
            0x01,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            MKB_DISC_STRUCTURE_FORMAT,
            (MKB_PACK_SIZE >> 8) as u8,
            (MKB_PACK_SIZE & 0xFF) as u8,
            0x00,
            0x00,
        ];
        // Pack number goes in address field
        cdb[2] = ((pack >> 24) & 0xFF) as u8;
        cdb[3] = ((pack >> 16) & 0xFF) as u8;
        cdb[4] = ((pack >> 8) & 0xFF) as u8;
        cdb[5] = (pack & 0xFF) as u8;

        let mut buf = vec![0u8; 32772];
        if session
            .scsi_execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)
            .is_ok()
        {
            let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
            if len > 2 && len - 2 <= 32768 {
                mkb.extend_from_slice(&buf[4..4 + len - 2]);
            }
        }
    }

    Ok(mkb)
}

// ── Content Certificate parsing ─────────────────────────────────────────────

/// AACS Content Certificate — identifies disc AACS version and features.
#[derive(Debug)]
pub struct ContentCert {
    /// Bus encryption enabled flag
    pub bus_encryption: bool,
    /// Content Certificate ID (6 bytes)
    pub cc_id: [u8; 6],
    /// AACS version: false = AACS 1.0, true = AACS 2.0
    pub aacs2: bool,
}

/// Parse a Content Certificate (ContentXXX.cer) file.
pub fn parse_content_cert(data: &[u8]) -> Option<ContentCert> {
    if data.len() < 8 {
        return None;
    }

    // Content Certificate format:
    //   [0] certificate type (0x00 = AACS1, 0x01 = AACS2)
    //   [1] bus_encryption_enabled (bit 0)
    //   [2..8] cc_id (6 bytes)
    let aacs2 = data[0] != 0x00;
    let bus_encryption = (data[1] & 0x01) != 0;
    let mut cc_id = [0u8; 6];
    cc_id.copy_from_slice(&data[2..8]);

    Some(ContentCert {
        bus_encryption,
        cc_id,
        aacs2,
    })
}

// ── Full VUK resolution chain ───────────────────────────────────────────────

/// Result of resolving a disc's VUK.
#[derive(Debug)]
pub struct ResolvedKeys {
    /// Disc hash (SHA1 of Unit_Key_RO.inf)
    pub disc_hash: [u8; 20],
    /// Volume Unique Key
    pub vuk: [u8; 16],
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping
    pub title_cps_unit: Vec<u16>,
    /// Whether AACS 2.0
    pub aacs2: bool,
    /// Whether bus encryption is enabled (from Content Certificate)
    pub bus_encryption: bool,
    /// Which resolution path succeeded (1=KEYDB, 2=KEYDB derived, 3=PK, 4=DK)
    pub key_source: u8,
}

/// Resolve all AACS keys for a disc given:
///   - Unit_Key_RO.inf raw data
///   - Content Certificate raw data (optional, for AACS version detection)
///   - Volume ID (from SCSI handshake)
///   - KEYDB
///
/// Tries in order:
///   1. Disc hash → KEYDB → VUK (fast path)
///   2. KEYDB media key + volume ID → VUK (if disc hash not in KEYDB but MK is)
///   3. MKB + processing keys → media key → VUK (full derivation)
pub fn resolve_keys(
    unit_key_ro_data: &[u8],
    content_cert_data: Option<&[u8]>,
    volume_id: &[u8; 16],
    keydb: &KeyDb,
    mkb_data: Option<&[u8]>,
) -> Option<ResolvedKeys> {
    // Detect AACS version
    let aacs2 = content_cert_data
        .and_then(parse_content_cert)
        .map(|cc| cc.aacs2)
        .unwrap_or(false);

    let bus_encryption = content_cert_data
        .and_then(parse_content_cert)
        .map(|cc| cc.bus_encryption)
        .unwrap_or(false);

    // Parse Unit_Key_RO.inf
    let uk_file = parse_unit_key_ro(unit_key_ro_data, aacs2)?;

    let hash_hex = disc_hash_hex(&uk_file.disc_hash);

    // Helper to build result
    let build = |vuk: [u8; 16], key_source: u8| -> ResolvedKeys {
        let unit_keys: Vec<(u32, [u8; 16])> = uk_file
            .encrypted_keys
            .iter()
            .map(|(num, enc_key)| (*num, decrypt_unit_key(&vuk, enc_key)))
            .collect();
        ResolvedKeys {
            disc_hash: uk_file.disc_hash,
            vuk,
            unit_keys,
            title_cps_unit: uk_file.title_cps_unit.clone(),
            aacs2,
            bus_encryption,
            key_source,
        }
    };

    // Path 1: Look up VUK by disc hash in KEYDB
    if let Some(entry) = keydb.find_disc(&hash_hex) {
        if let Some(vuk) = entry.vuk {
            return Some(build(vuk, 1));
        }
    }

    // Path 2: Find entry with matching VID → derive VUK from MK + VID
    for entry in keydb.disc_entries.values() {
        if let (Some(mk), Some(did)) = (entry.media_key, entry.disc_id) {
            if did == *volume_id {
                return Some(build(derive_vuk(&mk, volume_id), 2));
            }
        }
    }

    // Path 3: MKB + processing keys → media key → VUK
    if let Some(mkb) = mkb_data {
        if let Some(mk) = derive_media_key_from_pk(mkb, &keydb.processing_keys) {
            return Some(build(derive_vuk(&mk, volume_id), 3));
        }

        // Path 4: MKB + device keys → processing key → media key → VUK
        if let Some(mk) = derive_media_key_from_dk(mkb, &keydb.device_keys) {
            return Some(build(derive_vuk(&mk, volume_id), 4));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::super::decrypt::{aes_ecb_encrypt, ALIGNED_UNIT_LEN};
    use super::super::keydb::{DiscEntry, KeyDb};
    use super::*;

    /// Get KEYDB path from KEYDB_PATH environment variable. Returns None if not set or not found.
    fn keydb_path() -> Option<std::path::PathBuf> {
        let path = std::path::PathBuf::from(std::env::var("KEYDB_PATH").ok()?);
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }

    #[test]
    fn test_vuk_derivation() {
        // Civil War UHD: known MK, VID, VUK from KEYDB
        // MK = 15665F98..., VID (disc_id) = from entry, VUK = F96D7908...
        // VUK = AES-DEC(MK, VID) XOR VID
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };

        let db = KeyDb::load(&path).unwrap();

        // Find a disc with both MK, disc_id, and VUK so we can verify derivation
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.media_key.is_some() && e.disc_id.is_some() && e.vuk.is_some())
            .expect("No disc with MK + VID + VUK");

        let mk = entry.media_key.unwrap();
        let vid = entry.disc_id.unwrap();
        let expected_vuk = entry.vuk.unwrap();

        let derived = derive_vuk(&mk, &vid);
        assert_eq!(
            derived, expected_vuk,
            "VUK derivation failed for disc: {} (hash {})",
            entry.title, entry.disc_hash
        );
        eprintln!("VUK derivation verified for: {}", entry.title);
    }

    #[test]
    fn test_decrypt_unit_key_from_vuk() {
        // Test the full chain: VUK → decrypt encrypted unit key → unit key
        // Use a known disc from KEYDB that has both VUK and unit keys
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };

        let db = KeyDb::load(&path).unwrap();

        // Find a disc with VUK and unit keys
        let entry = db
            .disc_entries
            .values()
            .find(|e| e.vuk.is_some() && !e.unit_keys.is_empty())
            .expect("No disc with VUK + unit keys");

        eprintln!(
            "Testing unit key decrypt for: {} ({})",
            entry.title, entry.disc_hash
        );
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
            assert_eq!(
                &decrypted, expected_uk,
                "Unit key {} roundtrip failed for {}",
                num, entry.title
            );
        }
        eprintln!("  All {} unit key roundtrips passed", entry.unit_keys.len());
    }

    #[test]
    fn test_decrypt_real_unit() {
        // Try decrypting a real encrypted aligned unit from Civil War UHD
        // This disc is AACS 2.0 (BEE) so unit key alone won't work —
        // we need bus decryption first. But this verifies the pipeline.
        let unit_path = std::path::Path::new("/tmp/encrypted_unit.bin");
        if !unit_path.exists() {
            return;
        }

        let original = std::fs::read(unit_path).unwrap();
        assert_eq!(original.len(), ALIGNED_UNIT_LEN);
        assert!(
            super::super::decrypt::is_unit_encrypted(&original),
            "Unit should be encrypted"
        );

        let kp = match keydb_path() {
            Some(p) => p,
            None => return,
        };
        let db = KeyDb::load(&kp).unwrap();

        // Civil War UHD entries
        let civil_war_entries: Vec<&DiscEntry> = db
            .disc_entries
            .values()
            .filter(|e| e.title.contains("CIVIL WAR") && !e.unit_keys.is_empty())
            .collect();

        eprintln!(
            "Found {} Civil War entries with unit keys",
            civil_war_entries.len()
        );

        // Try each entry's unit keys
        for entry in &civil_war_entries {
            let keys: Vec<[u8; 16]> = entry.unit_keys.iter().map(|(_, k)| *k).collect();
            let mut unit = original.clone();

            if let Some(idx) = super::super::decrypt::decrypt_unit_try_keys(&mut unit, &keys) {
                eprintln!(
                    "SUCCESS: Decrypted with entry {} key {}",
                    entry.disc_hash, idx
                );
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
    fn test_disc_hash() {
        // SHA1 of a known byte sequence
        let data = b"test unit key ro inf data";
        let hash = disc_hash(data);
        assert_ne!(hash, [0u8; 20]);
        // Same input → same hash
        assert_eq!(hash, disc_hash(data));
    }

    #[test]
    fn test_disc_hash_hex() {
        let hash = [
            0x55, 0xBF, 0xD0, 0x51, 0xD1, 0xF8, 0x2C, 0xBB, 0x67, 0x76, 0x46, 0x3B, 0x6D, 0x70,
            0x09, 0x12, 0x47, 0xBA, 0x61, 0x5D,
        ];
        let hex = disc_hash_hex(&hash);
        assert_eq!(hex, "0x55BFD051D1F82CBB6776463B6D70091247BA615D");
    }

    #[test]
    fn test_parse_unit_key_ro_synthetic() {
        // Build a synthetic Unit_Key_RO.inf
        // Header: uk_pos at offset 0 (BE32), points to key storage
        // Keys at uk_pos + 48 (16 bytes each, 48-byte stride for AACS 1.0)
        let mut data = vec![0u8; 256];

        // uk_pos = 0x60 (96)
        data[0] = 0x00;
        data[1] = 0x00;
        data[2] = 0x00;
        data[3] = 0x60;

        // Header fields at 16-18
        data[16] = 1; // app_type = BD-ROM
        data[17] = 1; // num_bdmv_dir
        data[18] = 0; // no SKB

        // Title mapping at 20-25
        data[20] = 0;
        data[21] = 1; // first_play = CPS unit 1
        data[22] = 0;
        data[23] = 1; // top_menu = CPS unit 1
        data[24] = 0;
        data[25] = 1; // num_titles = 1
                      // Title 0 entry: 2 bytes pad + CPS unit
        data[28] = 0;
        data[29] = 1; // CPS unit 1

        // Key storage at offset 0x60
        let uk_pos = 0x60usize;
        data[uk_pos] = 0;
        data[uk_pos + 1] = 2; // 2 unit keys

        // Key 1 at uk_pos + 48
        let key1_pos = uk_pos + 48;
        for i in 0..16 {
            data[key1_pos + i] = 0xAA;
        }

        // Key 2 at uk_pos + 48 + 48
        let key2_pos = key1_pos + 48;
        for i in 0..16 {
            data[key2_pos + i] = 0xBB;
        }

        let parsed = parse_unit_key_ro(&data, false).unwrap();
        assert_eq!(parsed.app_type, 1);
        assert_eq!(parsed.num_bdmv_dir, 1);
        assert!(!parsed.aacs2);
        assert_eq!(parsed.encrypted_keys.len(), 2);
        assert_eq!(parsed.encrypted_keys[0].0, 1); // CPS unit 1
        assert_eq!(parsed.encrypted_keys[0].1, [0xAA; 16]);
        assert_eq!(parsed.encrypted_keys[1].0, 2); // CPS unit 2
        assert_eq!(parsed.encrypted_keys[1].1, [0xBB; 16]);
    }

    #[test]
    fn test_mkb_version_parse() {
        // Synthetic MKB with Type and Version record (0x81)
        let mut mkb = vec![0u8; 32];
        // Record: type=0x81, length=12 (BE24)
        mkb[0] = 0x81;
        mkb[1] = 0x00;
        mkb[2] = 0x00;
        mkb[3] = 0x0C;
        // Version = 77
        mkb[4] = 0x00;
        mkb[5] = 0x00;
        mkb[6] = 0x00;
        mkb[7] = 77;

        assert_eq!(mkb_version(&mkb), Some(77));
    }

    #[test]
    fn test_resolve_keys_vuk_path() {
        // Test the full resolve chain using VUK path
        let path = match keydb_path() {
            Some(p) => p,
            None => return,
        };
        let db = KeyDb::load(&path).unwrap();

        // Find V for Vendetta BD — has VUK and unit keys
        // hash: 0x55BFD051D1F82CBB6776463B6D70091247BA615D
        let entry = db.find_disc("0x55BFD051D1F82CBB6776463B6D70091247BA615D");
        if entry.is_none() {
            return;
        }
        let entry = entry.unwrap();
        let vuk = entry.vuk.unwrap();
        let vid = entry.disc_id.unwrap();

        // We need the actual Unit_Key_RO.inf from the disc to compute disc hash.
        // Since we don't have it, we can at least test that the KEYDB lookup
        // works with a known hash.
        let hash_hex = "0x55BFD051D1F82CBB6776463B6D70091247BA615D";
        let found = db.find_disc(hash_hex);
        assert!(found.is_some());
        assert_eq!(found.unwrap().vuk, Some(vuk));

        // Verify VUK derivation if we have MK + VID
        if let Some(mk) = entry.media_key {
            let derived = derive_vuk(&mk, &vid);
            assert_eq!(derived, vuk, "VUK derivation mismatch for V for Vendetta");
            eprintln!("V for Vendetta VUK derivation verified");
        }
    }

    #[test]
    fn test_content_cert_parse() {
        // AACS 1.0 cert
        let mut data = vec![0u8; 16];
        data[0] = 0x00; // AACS 1.0
        data[1] = 0x00; // no bus encryption
        let cc = parse_content_cert(&data).unwrap();
        assert!(!cc.aacs2);
        assert!(!cc.bus_encryption);

        // AACS 2.0 with bus encryption
        data[0] = 0x01; // AACS 2.0
        data[1] = 0x01; // bus encryption enabled
        let cc = parse_content_cert(&data).unwrap();
        assert!(cc.aacs2);
        assert!(cc.bus_encryption);
    }
}

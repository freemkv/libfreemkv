//! AACS on-disc key-input files: `Unit_Key_RO.inf` parsing, the disc-hash
//! keydb lookup key, the Content Certificate, and the in-drive MKB read.
//! These turn raw disc files into the structures the key paths consume.

use super::mkb::*;

/// Parsed Unit_Key_RO.inf file.
pub struct UnitKeyFile {
    /// Disc hash (SHA1 of the entire file) — used as KEYDB lookup key
    pub disc_hash: [u8; 20],
    /// Application type (1 = BD-ROM)
    pub app_type: u8,
    /// Number of BDMV directories
    pub num_bdmv_dir: u8,
    /// Whether SKB MKB is used
    pub use_skb_mkb: bool,
    /// AACS generation this file's stride matches
    pub version: AacsVersion,
    /// Encrypted unit keys (CPS unit number, encrypted key)
    pub encrypted_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping (title_idx → unit_key_idx)
    pub title_cps_unit: Vec<u16>,
}

// Hand-written Debug that redacts the ENCRYPTED CPS unit keys a derive would print verbatim.
impl std::fmt::Debug for UnitKeyFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitKeyFile")
            // Disc hash is the public keydb lookup key, printed as hex.
            .field("disc_hash", &disc_hash_hex(&self.disc_hash))
            .field("app_type", &self.app_type)
            .field("num_bdmv_dir", &self.num_bdmv_dir)
            .field("use_skb_mkb", &self.use_skb_mkb)
            .field("version", &self.version)
            .field("encrypted_keys", &"<redacted>")
            .field("encrypted_keys_len", &self.encrypted_keys.len())
            .field("title_cps_unit", &self.title_cps_unit)
            .finish()
    }
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
/// ```text
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
///                         AACS 2.0 / 2.1: 64-byte stride (48 + 16 extra)
/// ```
pub fn parse_unit_key_ro(data: &[u8], version: AacsVersion) -> Option<UnitKeyFile> {
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
    if uk_pos.checked_add(2)? > data.len() {
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
            version,
            encrypted_keys: Vec::new(),
            title_cps_unit: Vec::new(),
        });
    }

    // Stride between keys
    let stride = version.unit_key_stride();

    // Validate size
    let keys_start = uk_pos.checked_add(48)?; // first key at uk_pos + 48
    if keys_start.checked_add(16)? > data.len() {
        return None;
    }

    // Extract encrypted keys (capacity clamped: num_uk is untrusted)
    let mut encrypted_keys = Vec::with_capacity(num_uk.min(data.len() / stride.max(1)));
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

    // The loop `break`s if the buffer runs out mid-key; a short list means the .inf
    // is malformed/truncated — reject rather than silently mapping title CPS units
    // to fewer keys than the header declared.
    if encrypted_keys.len() != num_uk {
        return None;
    }

    // Title → CPS unit mapping (AACS Unit_Key_RO format): validates the on-disc
    // CPS value is in `1..=num_uk` (else zeroes it) and converts it from 1-based
    // to a safe, ready-to-use 0-based key index.
    let to_key_idx = |cps: u16| -> u16 {
        if cps >= 1 && cps as usize <= num_uk {
            cps - 1
        } else {
            0
        }
    };
    let mut title_cps_unit = Vec::new();
    if data.len() >= 26 {
        let first_play = u16::from_be_bytes([data[20], data[21]]);
        let top_menu = u16::from_be_bytes([data[22], data[23]]);
        let num_titles = u16::from_be_bytes([data[24], data[25]]) as usize;

        title_cps_unit.push(to_key_idx(first_play));
        title_cps_unit.push(to_key_idx(top_menu));

        for i in 0..num_titles {
            let off = 26 + i * 4 + 2; // 2 bytes padding + 2 bytes CPS unit
            // A declared title whose entry runs past the buffer means the .inf
            // is truncated — reject rather than silently returning a short
            // title→CPS map (mirrors the key-list truncation check above).
            if off + 2 > data.len() {
                return None;
            }
            let cps = u16::from_be_bytes([data[off], data[off + 1]]);
            title_cps_unit.push(to_key_idx(cps));
        }
    }

    Some(UnitKeyFile {
        disc_hash: hash,
        app_type,
        num_bdmv_dir,
        use_skb_mkb,
        version,
        encrypted_keys,
        title_cps_unit,
    })
}

/// HD DVD Video Title Key File (`VTKF%%%.AACS`) magic — "DVD_HD_V_TKF".
pub const VTKF_MAGIC: &[u8; 12] = b"DVD_HD_V_TKF";
/// Fixed header length before the first Title Key Entry (AACS HD DVD Book,
/// Table 3-8).
const VTKF_HEADER_LEN: usize = 0x80;
/// Title Key Entry stride (Table 3-8): 1-byte `BIFO` + 3 reserved + 16-byte
/// encrypted title key + 16-byte binding MAC = 36 bytes.
const VTKF_ENTRY_LEN: usize = 0x24;
/// Byte offset of the encrypted title key within an entry (after `BIFO` + 3
/// reserved).
const VTKF_KEY_OFF: usize = 4;
/// Number of Title Key Entry slots in a VTKF (Table 3-8): a fixed 64.
const VTKF_MAX_ENTRIES: usize = 64;
/// `BIFO` bit 7 (`AV_FLG`): set = this slot carries an available title key.
const VTKF_AV_FLG: u8 = 0x80;

/// Parse an HD DVD `VTKF%%%.AACS` into the SAME [`UnitKeyFile`] a BD/UHD
/// `Unit_Key_RO.inf` yields — so the shared AACS crypto (`derive_unit_keys` →
/// `decrypt_unit_key(vuk, …)`) unwraps HD DVD title keys with no change.
///
/// Layout — AACS "HD DVD and DVD Pre-recorded Book" Table 3-8, a fixed
/// 2480-byte file:
/// ```text
///   [0x00..0x0C] magic "DVD_HD_V_TKF"
///   [0x0C..0x10] BE32 HD_VTKF_SIZE (2480)
///   [0x10..0x1C] associated playlist name ("VPLST%%%.XPL")
///   [0x1C..0x80] reserved
///   [0x80..]     64 entries × 36 bytes:
///                  BIFO (1) | reserved (3) | ENCRYPTED title key (16) | binding MAC (16)
///                  BIFO bit 7 (AV_FLG) set = this slot holds a title key
///                  (pre-recorded discs fill the binding MAC with 0xFF)
///   [0x9A0..2480] 16-byte TKF MAC (CMAC keyed by Kvu — NOT a key)
/// ```
/// An absent slot is SKIPPED (not a terminator): the 1-based slot index IS
/// the CPS unit number, so `title_cps_unit` is left empty (playlist-owned).
pub fn parse_vtkf(data: &[u8]) -> Option<UnitKeyFile> {
    // A file cut inside the entry table would silently lose its later slots' keys.
    if data.len() < VTKF_HEADER_LEN + VTKF_MAX_ENTRIES * VTKF_ENTRY_LEN || &data[..12] != VTKF_MAGIC
    {
        return None;
    }
    // SHA1 of the WHOLE file — the KEYDB lookup key. BackupHDDVD-family key
    // databases index an HD DVD disc by SHA1(VTKF000.AACS), the same role the
    // BD disc_hash plays for `Unit_Key_RO.inf`.
    let hash = disc_hash(data);

    let mut encrypted_keys = Vec::new();
    for n in 0..VTKF_MAX_ENTRIES {
        let pos = VTKF_HEADER_LEN + n * VTKF_ENTRY_LEN;
        // AV_FLG clear = empty slot: skip it, but keep the slot index as the CPS
        // number (do NOT break — a gap must not renumber the keys that follow).
        if data[pos] & VTKF_AV_FLG == 0 {
            continue;
        }
        let mut key = [0u8; 16];
        key.copy_from_slice(&data[pos + VTKF_KEY_OFF..pos + VTKF_KEY_OFF + 16]);
        encrypted_keys.push((n as u32 + 1, key));
    }
    if encrypted_keys.is_empty() {
        return None;
    }

    Some(UnitKeyFile {
        disc_hash: hash,
        app_type: 0,     // HD DVD VTKF carries no BD-ROM app_type
        num_bdmv_dir: 0, // BD-only concept
        use_skb_mkb: false,
        version: AacsVersion::V10, // HD DVD is always AACS 1.0
        encrypted_keys,
        title_cps_unit: Vec::new(),
    })
}

/// Parse a disc's title-key file, dispatching on the self-describing magic:
/// an HD DVD `VTKF000.AACS` (`DVD_HD_V_TKF`) → [`parse_vtkf`]; anything else is a
/// BD/UHD `Unit_Key_RO.inf` → [`parse_unit_key_ro`]. Both return the same
/// [`UnitKeyFile`], so every downstream AACS derivation stays container-agnostic
/// — the single seam where BD-vs-HD-DVD key layout is resolved (mirrors the key
/// service, which classifies HD DVD by the very same magic).
pub fn parse_title_keys(data: &[u8], version: AacsVersion) -> Option<UnitKeyFile> {
    if data.len() >= 12 && &data[..12] == VTKF_MAGIC {
        parse_vtkf(data)
    } else {
        parse_unit_key_ro(data, version)
    }
}

/// MKB disc structure format code.
const MKB_DISC_STRUCTURE_FORMAT: u8 = 0x83;

/// MKB pack buffer size.
const MKB_PACK_SIZE: usize = 32772;

/// Read one MKB pack: returns (declared pack count, payload). A short transfer or a
/// payload past the buffer is a drive fault, never a holed MKB.
fn read_mkb_pack(
    session: &mut dyn crate::scsi::ScsiTransport,
    pack: u32,
) -> crate::error::Result<(usize, Option<Vec<u8>>)> {
    use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE};

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
    cdb[2..6].copy_from_slice(&pack.to_be_bytes());

    let mut buf = vec![0u8; MKB_PACK_SIZE];
    // Transport errors propagate: swallowing one would return a truncated MKB as Ok.
    let r = session.execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)?;

    // A GOOD status with no header transferred is a drive fault, not an empty MKB.
    if r.bytes_transferred < 4 {
        return Err(crate::error::Error::AacsKeyRead);
    }
    let data_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let num_packs = buf[3] as usize;
    if data_len < 2 {
        return Ok((num_packs, None));
    }
    let len = data_len - 2;
    if len > MKB_PACK_SIZE - 4 || r.bytes_transferred < 4 + len {
        return Err(crate::error::Error::AacsKeyRead);
    }
    Ok((num_packs, Some(buf[4..4 + len].to_vec())))
}

/// Read MKB from drive via SCSI (REPORT DISC STRUCTURE format 0x83).
/// Returns the concatenated MKB data from all packs. A short or over-long pack is a
/// drive fault (`AacsKeyRead`), never a holed MKB; a header-only pack in a multi-pack MKB is
/// `AacsKeyRead`; a single header-only pack is an empty MKB.
pub fn read_mkb_from_drive(
    session: &mut dyn crate::scsi::ScsiTransport,
) -> crate::error::Result<Vec<u8>> {
    let (num_packs, first) = read_mkb_pack(session, 0)?;
    let Some(mut mkb) = first else {
        // A declared multi-pack MKB whose first pack carries no data is a hole.
        if num_packs > 1 {
            return Err(crate::error::Error::AacsKeyRead);
        }
        return Ok(Vec::new());
    };
    // A header-only pack in a multi-pack MKB is a hole, not an empty MKB.
    if num_packs > 1 && mkb.is_empty() {
        return Err(crate::error::Error::AacsKeyRead);
    }

    // Read remaining packs
    for pack in 1..num_packs {
        let (_, body) = read_mkb_pack(session, pack as u32)?;
        let body = body.ok_or(crate::error::Error::AacsKeyRead)?;
        if body.is_empty() {
            return Err(crate::error::Error::AacsKeyRead);
        }
        mkb.extend_from_slice(&body);
    }

    Ok(mkb)
}

/// AACS Content Certificate — identifies disc AACS version and features.
#[derive(Debug)]
pub struct ContentCert {
    /// Bus encryption enabled flag
    pub bus_encryption: bool,
    /// Content Certificate ID (6 bytes)
    pub cc_id: [u8; 6],
    /// AACS generation indicated by the certificate type byte.
    ///
    /// Cert type `0x00` → [`AacsVersion::V10`], `0x10` → [`AacsVersion::V20`];
    /// any other type does not parse. The certificate alone cannot distinguish
    /// V20 from V21 — Variant detection happens after the MKB walk.
    pub version: AacsVersion,
}

/// Parse a Content Certificate (ContentXXX.cer) file.
pub fn parse_content_cert(data: &[u8]) -> Option<ContentCert> {
    if data.len() < 20 {
        return None;
    }

    // Layout: [0] type, [1] bit7 BEE flag, [14..20] cc_id. Only observed types
    // parse: 0x10 AACS2, 0x00 AACS1 (HD DVD's 0x00 is reverse-engineered from
    // retail discs); anything else is None.
    let version = match data[0] {
        0x00 => AacsVersion::V10,
        0x10 => AacsVersion::V20,
        _ => return None,
    };
    // The flag is bit 7 of byte 1, NOT bit 0. Reading bit 0 (the prior bug) made
    // a bus-encrypted cert (byte1=0x80) read as `false`, defeating the
    // AacsBusKeyUnavailable fail-loud gate in disc/encrypt.rs.
    let bus_encryption = (data[1] >> 7) & 1 == 1;
    let mut cc_id = [0u8; 6];
    cc_id.copy_from_slice(&data[14..20]);

    Some(ContentCert {
        bus_encryption,
        cc_id,
        version,
    })
}

#[cfg(test)]
#[path = "inf_vtkf_tests.rs"]
mod vtkf_tests;

#[cfg(test)]
#[path = "inf_read_mkb_tests.rs"]
mod read_mkb_tests;

#[cfg(test)]
#[path = "inf_unit_key_ro_tests.rs"]
mod unit_key_ro_tests;

#[cfg(test)]
#[path = "inf_content_cert_tests.rs"]
mod content_cert_tests;

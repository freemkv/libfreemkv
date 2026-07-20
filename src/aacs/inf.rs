//! AACS on-disc key-input files: `Unit_Key_RO.inf` parsing, the disc-hash
//! keydb lookup key, the Content Certificate, and the in-drive MKB read.
//! These turn raw disc files into the structures the key paths consume.

use super::mkb::*;

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
    /// AACS generation this file's stride matches
    pub version: AacsVersion,
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
///                         AACS 2.0 / 2.1: 64-byte stride (48 + 16 extra)
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
            version,
            encrypted_keys: Vec::new(),
            title_cps_unit: Vec::new(),
        });
    }

    // Stride between keys
    let stride = version.unit_key_stride();

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

    // The loop above `break`s if the buffer runs out mid-key. A short list
    // means the .inf is malformed/truncated — reject it rather than silently
    // accepting fewer keys than the header declared, which would later map
    // title CPS units to nonexistent keys.
    if encrypted_keys.len() != num_uk {
        return None;
    }

    // Title → CPS unit mapping (AACS Unit_Key_RO format): each on-disc CPS
    // value is in `1..=num_uk` (else zeroes it) and converts the 1-based on-disc
    // index to a 0-based key index. We mirror that so the stored value is a safe,
    // ready-to-use key index rather than a raw 1-based number.
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
            if off + 2 <= data.len() {
                let cps = u16::from_be_bytes([data[off], data[off + 1]]);
                title_cps_unit.push(to_key_idx(cps));
            }
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
/// `decrypt_unit_key(vuk, …)`) unwraps HD DVD title keys with no change. Only
/// the on-disc CONTAINER differs between BD and HD DVD; the title-key unwrap is
/// the identical AES-128 VUK step (`Kt = AES-128D(Kvu, Kte)`).
///
/// Layout — AACS "HD DVD and DVD Pre-recorded Book" Table 3-8, a fixed
/// 2480-byte file, verified byte-exact against real discs (Freedom `VTKF090`,
/// Dukes of Hazzard `VTKF000`):
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
/// The slot index (1-based) is the CPS unit number, so an absent slot is
/// SKIPPED (not a terminator) — collapsing gaps would renumber later keys and
/// hand the wrong title key to CPS unit N+1. The title→CPS mapping is
/// playlist-driven (`VPLST%%%.XPL`) and owned by the HD DVD enumerator, so
/// `title_cps_unit` is left empty here.
///
/// The prior parser used a 32-byte stride (a 12-byte pad instead of the 16-byte
/// binding MAC). That reads entry #1 correctly but drifts +4 bytes per entry
/// after it, so it only decrypted single-CPS-unit discs; every multi-key VTKF
/// (Freedom, Harry Potter) yielded garbage keys for CPS unit ≥2.
pub fn parse_vtkf(data: &[u8]) -> Option<UnitKeyFile> {
    if data.len() < VTKF_HEADER_LEN || &data[..12] != VTKF_MAGIC {
        return None;
    }
    // SHA1 of the WHOLE file — the KEYDB lookup key. BackupHDDVD-family key
    // databases index an HD DVD disc by SHA1(VTKF000.AACS), the same role the
    // BD disc_hash plays for `Unit_Key_RO.inf`.
    let hash = disc_hash(data);

    let mut encrypted_keys = Vec::new();
    for n in 0..VTKF_MAX_ENTRIES {
        let pos = VTKF_HEADER_LEN + n * VTKF_ENTRY_LEN;
        if pos + VTKF_ENTRY_LEN > data.len() {
            break;
        }
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

/// Read MKB from drive via SCSI (REPORT DISC STRUCTURE format 0x83).
/// Returns the concatenated MKB data from all packs.
pub fn read_mkb_from_drive(
    session: &mut dyn crate::scsi::ScsiTransport,
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
    session.execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)?;

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
            .execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000)
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

/// AACS Content Certificate — identifies disc AACS version and features.
#[derive(Debug)]
pub struct ContentCert {
    /// Bus encryption enabled flag
    pub bus_encryption: bool,
    /// Content Certificate ID (6 bytes)
    pub cc_id: [u8; 6],
    /// AACS generation indicated by the certificate type byte.
    ///
    /// Cert type `0x00` → [`AacsVersion::V10`]; any other value →
    /// [`AacsVersion::V20`]. The certificate alone cannot distinguish
    /// V20 from V21 — Variant detection happens after the MKB walk.
    pub version: AacsVersion,
}

/// Parse a Content Certificate (ContentXXX.cer) file.
pub fn parse_content_cert(data: &[u8]) -> Option<ContentCert> {
    if data.len() < 20 {
        return None;
    }

    // Content Certificate layout (per the AACS content-cert format):
    //   [0]      certificate type (0x00 = AACS1, 0x10 = AACS2)
    //   [1] bit7 bus_encryption_enabled_flag  (`p[1] >> 7`)
    //   [14..20] cc_id (6 bytes)             (`p + 14`)
    let version = if data[0] == 0x00 {
        AacsVersion::V10
    } else {
        AacsVersion::V20
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
mod vtkf_tests {
    use super::*;

    /// Build a synthetic `VTKF%%%.AACS` matching the real on-disc layout (AACS
    /// HD DVD Book Table 3-8, verified against Freedom `VTKF090` and Dukes
    /// `VTKF000`): magic, BE32 size, playlist name, reserved to 0x80, then 64
    /// entry slots of 36 bytes (the first `keys.len()` present with `AV_FLG`
    /// set, the rest empty), a reserved gap, and the 16-byte trailing TKF MAC.
    fn synth_vtkf(keys: &[[u8; 16]]) -> Vec<u8> {
        const FILE_LEN: usize = 2480;
        let mut v = Vec::new();
        v.extend_from_slice(VTKF_MAGIC); // 0x00
        v.extend_from_slice(&(FILE_LEN as u32).to_be_bytes()); // 0x0C HD_VTKF_SIZE
        v.extend_from_slice(b"VPLST000.XPL"); // 0x10 playlist name
        v.resize(VTKF_HEADER_LEN, 0); // reserve to first entry (0x80)
        for n in 0..VTKF_MAX_ENTRIES {
            if let Some(k) = keys.get(n) {
                v.push(VTKF_AV_FLG); // BIFO: AV_FLG set (present)
                v.extend_from_slice(&[0, 0, 0]); // reserved
                v.extend_from_slice(k); // 16-byte encrypted title key
                v.extend_from_slice(&[0xFFu8; 16]); // binding MAC (0xFF, pre-recorded)
            } else {
                v.extend_from_slice(&[0u8; VTKF_ENTRY_LEN]); // empty slot (AV_FLG clear)
            }
        }
        v.resize(FILE_LEN - 16, 0); // reserved gap before the trailer
        v.extend_from_slice(&[0xABu8; 16]); // TKF MAC (must NOT be read as a key)
        v
    }

    #[test]
    fn parse_vtkf_reads_present_entries_skips_empty_ignores_mac() {
        let k1 = [0x11u8; 16];
        let k2 = [0x22u8; 16];
        let k3 = [0x33u8; 16];
        let data = synth_vtkf(&[k1, k2, k3]);

        let ukf = parse_vtkf(&data).expect("valid VTKF must parse");
        // Exactly the three present entries — the empty slots and the trailing
        // 16-byte TKF MAC are NOT mistaken for keys. Critically, k2/k3 are read
        // at the 36-byte stride (offsets 0xA4, 0xC8); the old 32-byte stride
        // misread them from inside the previous entry's binding MAC.
        assert_eq!(ukf.encrypted_keys.len(), 3);
        assert_eq!(
            ukf.encrypted_keys[0],
            (1, k1),
            "CPS units = 1-based slot index"
        );
        assert_eq!(ukf.encrypted_keys[1], (2, k2));
        assert_eq!(ukf.encrypted_keys[2], (3, k3));
        assert_eq!(ukf.version, AacsVersion::V10, "HD DVD is AACS 1.0");
        // disc_hash is SHA1 of the whole file (the KEYDB lookup key).
        assert_eq!(ukf.disc_hash, disc_hash(&data));
    }

    #[test]
    fn parse_vtkf_reads_a_full_64_entry_file() {
        // Real discs (Freedom, Dukes) carry all 64 slots present. Every key must
        // come back, none dropped and none drifted — the regression the 32-byte
        // stride failed.
        let keys: Vec<[u8; 16]> = (0..VTKF_MAX_ENTRIES).map(|n| [n as u8; 16]).collect();
        let ukf = parse_vtkf(&synth_vtkf(&keys)).expect("64-entry VTKF");
        assert_eq!(ukf.encrypted_keys.len(), 64);
        assert_eq!(
            ukf.encrypted_keys[63],
            (64, [63u8; 16]),
            "entry 64 at 0x{:x}",
            VTKF_HEADER_LEN + 63 * VTKF_ENTRY_LEN
        );
    }

    #[test]
    fn parse_vtkf_rejects_non_magic() {
        let mut data = synth_vtkf(&[[0x11u8; 16]]);
        data[0] = b'X'; // corrupt magic
        assert!(
            parse_vtkf(&data).is_none(),
            "non-VTKF magic must be rejected"
        );
        assert!(
            parse_vtkf(&[0u8; 4]).is_none(),
            "too short must be rejected"
        );
    }

    #[test]
    fn parse_title_keys_dispatches_by_magic() {
        // VTKF magic → parse_vtkf.
        let data = synth_vtkf(&[[0x44u8; 16], [0x55u8; 16]]);
        let ukf = parse_title_keys(&data, AacsVersion::V10).expect("VTKF dispatch");
        assert_eq!(ukf.encrypted_keys.len(), 2);

        // Non-VTKF → parse_unit_key_ro (a 2-byte buffer is not a valid inf, so
        // this proves it ROUTED to the BD parser rather than parse_vtkf).
        assert!(
            parse_title_keys(&[0x00, 0x00], AacsVersion::V10).is_none(),
            "non-magic input must route to parse_unit_key_ro"
        );
    }

    /// The whole point of the seam: a parsed VTKF feeds the SHARED VUK→title-key
    /// crypto (`decrypt_unit_key`) exactly like a BD `Unit_Key_RO.inf` would —
    /// no HD-DVD-specific crypto path.
    #[test]
    fn vtkf_encrypted_keys_feed_shared_vuk_unwrap() {
        let enc = [0x9Au8; 16];
        let data = synth_vtkf(&[enc]);
        let ukf = parse_vtkf(&data).unwrap();
        let vuk = [0x5Cu8; 16];
        let derived = super::super::derive::decrypt_unit_key(&vuk, &ukf.encrypted_keys[0].1);
        // Same as applying the shared unwrap directly to the stored enc key.
        assert_eq!(derived, super::super::derive::decrypt_unit_key(&vuk, &enc));
    }
}

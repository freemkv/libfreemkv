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

// Hand-written Debug that redacts the ENCRYPTED CPS unit keys a derive would
// print verbatim. See docs/inf.md — UnitKeyFile's hand-written Debug.
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

// See docs/inf.md — parse_vtkf: byte-exact real-disc verification and why
// the prior 32-byte stride corrupted multi-key VTKFs.
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

    // Content Certificate layout: [0] cert type (0x00 AACS1/0x10 AACS2),
    // [1] bit7 bus_encryption_enabled_flag, [14..20] cc_id (6 bytes).
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

    // Synthesize a `VTKF%%%.AACS`: magic, BE32 size, playlist name, reserved
    // to 0x80, 64 entry slots (first `keys.len()` present), MAC trailer.
    // See docs/inf.md — synth_vtkf test helper.
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
        // Exactly the three present entries — empty slots and the trailing 16-byte
        // TKF MAC are not mistaken for keys. k2/k3 are read at the 36-byte stride
        // (0xA4, 0xC8); the old 32-byte stride misread them from the prior MAC.
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

    // Sentinel byte 0xD5 = decimal 213: a derived Debug would render it in
    // decimal. See docs/inf.md — unit_key_file_debug_is_redacted test.
    #[test]
    fn unit_key_file_debug_is_redacted() {
        let f = UnitKeyFile {
            disc_hash: [0xD5; 20],
            app_type: 1,
            num_bdmv_dir: 1,
            use_skb_mkb: false,
            version: AacsVersion::V20,
            encrypted_keys: vec![(0, [0xD5; 16]), (1, [0xD5; 16])],
            title_cps_unit: vec![0, 1],
        };
        let dbg = format!("{f:?}");
        assert!(
            !dbg.contains("213"),
            "UnitKeyFile Debug leaked key bytes (decimal 213): {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "UnitKeyFile Debug missing redaction marker: {dbg}"
        );
        // Non-secret shape is still useful for diagnostics.
        assert!(dbg.contains("encrypted_keys_len: 2"), "{dbg}");
    }
}

#[cfg(test)]
mod read_mkb_tests {
    use super::*;
    use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE, ScsiResult, ScsiTransport};

    /// A drive that answers READ DISC STRUCTURE format 0x83 from a scripted set
    /// of packs and records every CDB it was handed.
    struct MkbDrive {
        /// One entry per pack: the pack's MKB payload bytes.
        packs: Vec<Vec<u8>>,
        cdbs: Vec<Vec<u8>>,
    }

    impl ScsiTransport for MkbDrive {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            self.cdbs.push(cdb.to_vec());
            // Pack number is carried in the CDB address field (bytes 2..6),
            // MMC-6 READ DISC STRUCTURE.
            let pack = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]) as usize;
            let body = self.packs.get(pack).cloned().unwrap_or_default();
            // Header: BE16 data length (counts the 2 header bytes that follow
            // it plus the payload), reserved byte, pack count, then payload.
            let data_len = body.len() + 2;
            data[0..2].copy_from_slice(&(data_len as u16).to_be_bytes());
            data[2] = 0x00;
            data[3] = self.packs.len() as u8;
            data[4..4 + body.len()].copy_from_slice(&body);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 4 + body.len(),
                sense: [0u8; 32],
            })
        }
    }

    // Pins the CONTENT: concatenated pack payloads, in pack order, byte for
    // byte. See docs/inf.md — read_mkb_from_drive_returns_the_concatenated…
    #[test]
    fn read_mkb_from_drive_returns_the_concatenated_pack_payload() {
        let pack0: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
        let pack1: Vec<u8> = (0..300u32).map(|i| (i % 253) as u8 ^ 0xA5).collect();
        let mut drive = MkbDrive {
            packs: vec![pack0.clone(), pack1.clone()],
            cdbs: Vec::new(),
        };

        let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");

        let mut expected = pack0.clone();
        expected.extend_from_slice(&pack1);
        assert_eq!(
            mkb.len(),
            expected.len(),
            "every pack's payload must be concatenated, none dropped"
        );
        assert!(
            mkb == expected,
            "MKB bytes must be the drive's payload in pack order; first \
             mismatch at {:?}",
            (0..expected.len()).find(|&i| mkb[i] != expected[i])
        );

        // MMC-6 READ DISC STRUCTURE with the AACS MKB format code, one command
        // per pack, pack number in the address field.
        assert_eq!(drive.cdbs.len(), 2, "one command per declared pack");
        for (i, cdb) in drive.cdbs.iter().enumerate() {
            assert_eq!(cdb[0], SCSI_READ_DISC_STRUCTURE, "opcode");
            assert_eq!(cdb[7], 0x83, "AACS MKB disc-structure format code");
            assert_eq!(
                u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]),
                i as u32,
                "pack {i} must be requested by number"
            );
        }
    }

    // Pins the WHOLE 12-byte CDB (MMC-6 READ DISC STRUCTURE, AACS MKB
    // format) so no field can drift. See docs/inf.md — this test's layout.
    #[test]
    fn read_mkb_from_drive_issues_the_exact_mmc_cdb_for_each_pack() {
        let mut drive = MkbDrive {
            packs: vec![vec![0x11u8; 64], vec![0x22u8; 64], vec![0x33u8; 64]],
            cdbs: Vec::new(),
        };
        read_mkb_from_drive(&mut drive).expect("scripted drive answers");

        assert_eq!(drive.cdbs.len(), 3, "one command per declared pack");
        for (pack, cdb) in drive.cdbs.iter().enumerate() {
            let p = pack as u32;
            let expected: [u8; 12] = [
                SCSI_READ_DISC_STRUCTURE,
                0x01,
                (p >> 24) as u8,
                (p >> 16) as u8,
                (p >> 8) as u8,
                p as u8,
                0x00,
                0x83, // AACS MKB disc-structure format
                0x80, // allocation length 32772 = 0x8004, high byte
                0x04, // …low byte
                0x00,
                0x00,
            ];
            assert_eq!(
                cdb.as_slice(),
                &expected[..],
                "CDB for pack {pack} must match the MMC-6 READ DISC STRUCTURE layout"
            );
        }
    }

    // A full 32768-byte pack must come back whole; small payloads elsewhere
    // never exercise this bound. See docs/inf.md — this test.
    #[test]
    fn read_mkb_from_drive_accepts_a_full_size_pack() {
        let full: Vec<u8> = (0..32768u32).map(|i| (i % 251) as u8).collect();
        let other: Vec<u8> = (0..32768u32).map(|i| (i % 241) as u8 ^ 0x5A).collect();
        // TWO maximal packs: the first-pack read and the per-pack loop carry
        // separate bounds, so both must accept a full-window payload.
        let mut drive = MkbDrive {
            packs: vec![full.clone(), other.clone()],
            cdbs: Vec::new(),
        };
        let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");
        assert_eq!(
            mkb.len(),
            65536,
            "neither maximal pack may be dropped at the size bound"
        );
        let mut expected = full.clone();
        expected.extend_from_slice(&other);
        assert!(mkb == expected, "both maximal packs' bytes must be intact");
    }

    /// A pack that declares only the 2-byte header and NO payload contributes
    /// nothing, and must not push a phantom byte into the MKB — an off-by-one
    /// at the zero-length boundary corrupts every following pack's alignment.
    #[test]
    fn read_mkb_from_drive_zero_length_pack_contributes_nothing() {
        let mut drive = MkbDrive {
            packs: vec![Vec::new(), vec![0xABu8; 32]],
            cdbs: Vec::new(),
        };
        let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");
        assert_eq!(
            mkb.len(),
            32,
            "an empty pack adds no bytes; only pack 1's payload is present"
        );
        assert!(mkb == vec![0xABu8; 32], "and the bytes are pack 1's");
    }

    // A drive-declared length past the 32772-byte buffer must not be
    // trusted. See docs/inf.md — this test.
    #[test]
    fn read_mkb_from_drive_ignores_a_pack_declaring_more_than_the_buffer_holds() {
        /// Pack 0 is honest; pack 1 declares a 60000-byte payload it never sent.
        struct LyingDrive {
            honest: Vec<u8>,
        }
        impl ScsiTransport for LyingDrive {
            fn execute(
                &mut self,
                cdb: &[u8],
                _direction: DataDirection,
                data: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                let pack = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
                data[3] = 2; // two packs declared
                if pack == 0 {
                    let dl = self.honest.len() + 2;
                    data[0..2].copy_from_slice(&(dl as u16).to_be_bytes());
                    data[4..4 + self.honest.len()].copy_from_slice(&self.honest);
                } else {
                    // A length far beyond the 32772-byte response buffer.
                    data[0..2].copy_from_slice(&60_000u16.to_be_bytes());
                }
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 4,
                    sense: [0u8; 32],
                })
            }
        }

        let honest = vec![0xC7u8; 256];
        let mut drive = LyingDrive {
            honest: honest.clone(),
        };
        let mkb = read_mkb_from_drive(&mut drive).expect("an over-declared pack is not an error");
        assert_eq!(
            mkb.len(),
            honest.len(),
            "only the honest pack's bytes may be taken; the over-declared pack \
             contributes nothing and must not be read past the buffer"
        );
        assert!(mkb == honest, "and those bytes are pack 0's");
    }

    /// The same over-declaration on the FIRST pack, which uses a separate bound
    /// from the loop's.
    #[test]
    fn read_mkb_from_drive_ignores_a_first_pack_declaring_more_than_the_buffer() {
        struct LyingFirst;
        impl ScsiTransport for LyingFirst {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _direction: DataDirection,
                data: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                data[0..2].copy_from_slice(&60_000u16.to_be_bytes());
                data[3] = 1;
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 4,
                    sense: [0u8; 32],
                })
            }
        }
        let mkb = read_mkb_from_drive(&mut LyingFirst).expect("not an error");
        assert!(
            mkb.is_empty(),
            "a first pack declaring more than the buffer holds yields no bytes"
        );
    }

    /// A single-pack disc still yields that pack's bytes — the common case, and
    /// the one where a body returning an empty vector looks most plausible.
    #[test]
    fn read_mkb_from_drive_returns_a_single_packs_payload() {
        let pack: Vec<u8> = (0..1024u32).map(|i| (i * 7 % 256) as u8).collect();
        let mut drive = MkbDrive {
            packs: vec![pack.clone()],
            cdbs: Vec::new(),
        };
        let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");
        assert_eq!(mkb.len(), pack.len(), "single pack payload length");
        assert!(mkb == pack, "single pack payload bytes");
    }

    /// A drive that reports a header-only response (`data_len < 2`) has no MKB
    /// to give. That must be an EMPTY vec, not a partial one — the distinction
    /// matters because the AACS paths treat a non-empty MKB as parseable.
    #[test]
    fn read_mkb_from_drive_empty_response_is_empty() {
        struct NoMkb;
        impl ScsiTransport for NoMkb {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _direction: DataDirection,
                data: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                data[0..2].copy_from_slice(&0u16.to_be_bytes());
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 4,
                    sense: [0u8; 32],
                })
            }
        }
        let mkb = read_mkb_from_drive(&mut NoMkb).expect("no-MKB drive still returns Ok");
        assert!(
            mkb.is_empty(),
            "a header-only response carries no MKB bytes"
        );
    }

    /// A transport failure on the FIRST pack must propagate as an error — the
    /// MKB is the root of the whole AACS ladder, so an unreadable one cannot be
    /// downgraded to "an MKB with no records".
    #[test]
    fn read_mkb_from_drive_propagates_the_first_pack_failure() {
        struct DeadDrive;
        impl ScsiTransport for DeadDrive {
            fn execute(
                &mut self,
                _cdb: &[u8],
                _direction: DataDirection,
                _data: &mut [u8],
                _timeout_ms: u32,
            ) -> crate::error::Result<ScsiResult> {
                Err(crate::error::Error::ScsiError {
                    opcode: SCSI_READ_DISC_STRUCTURE,
                    status: 0x02,
                    sense: None,
                })
            }
        }
        assert!(
            read_mkb_from_drive(&mut DeadDrive).is_err(),
            "an unreadable MKB must surface as an error, not an empty MKB"
        );
    }
}

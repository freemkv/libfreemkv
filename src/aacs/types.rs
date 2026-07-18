//! AACS primitive types shared across the resolve chain.
//!
//! These structs describe AACS key material (device keys, host
//! certificates, per-disc entries). They carry no parsing logic — the
//! keydb.cfg format lives in the `freemkv-keysources` crate. libfreemkv
//! owns only the crypto and these value types that flow through it.

/// A device key for MKB subset-difference tree processing.
#[derive(Clone)]
pub struct DeviceKey {
    pub key: [u8; 16],
    pub node: u16,
    pub uv: u32,
    pub u_mask_shift: u8,
}

/// Host certificate + private key for AACS SCSI authentication.
#[derive(Clone)]
pub struct HostCert {
    /// AACS 1.0: 20 bytes. AACS 2.0: 32 bytes.
    pub private_key: [u8; 20],
    /// AACS 1.0: 92 bytes. AACS 2.0: 132 bytes.
    pub certificate: Vec<u8>,
    /// AACS 2.0 host private key (P-256, 32 bytes). None for AACS 1.0 only.
    pub private_key_v2: Option<[u8; 32]>,
    /// AACS 2.0 host certificate (type 0x11). None for AACS 1.0 only.
    pub certificate_v2: Option<Vec<u8>>,
}

/// Volume ID (16 bytes) — read from the disc via the SCSI handshake / OEM path.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Vid(pub [u8; 16]);

/// Media Key (Km, 16 bytes) — the MKB-scoped key derived from device keys.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MediaKey(pub [u8; 16]);

/// Volume Unique Key (VUK / Kvu, 16 bytes) — derived from `MediaKey` + `Vid`,
/// decrypts the per-disc encrypted title keys in `Unit_Key_RO.inf`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Vuk(pub [u8; 16]);

/// Processing Key (Kp, 16 bytes) — an MKB Subset-Difference key that yields the
/// Media Key. A leaked/precomputed PK in the keydb, or the intermediate PK a
/// device-key walk derives at its matching SD node.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ProcessingKey(pub [u8; 16]);

/// One decrypted per-CPS-unit AACS title key.
///
/// `idx` is the POSITIONAL index of the encrypted title key within the slice
/// handed to the VUK→UK step (i.e. its order in `Unit_Key_RO.inf`'s key-storage
/// area). The CPS-unit *number* association is a higher-level concern owned by
/// [`super::inf::parse_unit_key_ro`], which pairs each positional key with its
/// declared CPS unit; this primitive only does the AES, so it surfaces position.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct UnitKey {
    pub idx: u32,
    pub key: [u8; 16],
    /// AACS 2.1 (FMTS) forensic **index** tag (see [`crate::aacs::segment`]).
    ///
    /// `0` = ordinary (non-forensic) content — the value for every 1.0 / 2.0
    /// key and for the bulk of a 2.1 title. `1..=32` = a forensic index key that
    /// decrypts the `IndividualSegment.tbl` segments tagged with that same index.
    /// This is the per-segment index (1..32), NOT the AACS 2.1 Media Key Variant
    /// (the 65536-value device selector), which is a separate MKB-layer concern.
    pub index_number: u8,
}

impl UnitKey {
    /// An ordinary (non-forensic) unit key: `index_number == 0`. The value
    /// for every AACS 1.0 / 2.0 key and the bulk of a 2.1 title.
    pub const fn new(idx: u32, key: [u8; 16]) -> Self {
        Self {
            idx,
            key,
            index_number: 0,
        }
    }

    /// A forensic index key: `index_number` in `1..=32`, decrypting the
    /// `IndividualSegment.tbl` segments tagged with that index.
    pub const fn forensic(idx: u32, key: [u8; 16], index_number: u8) -> Self {
        Self {
            idx,
            key,
            index_number,
        }
    }

    /// Whether this key decrypts ordinary (non-forensic) content (index 0).
    pub const fn is_default_index(&self) -> bool {
        self.index_number == 0
    }
}

/// A per-disc entry from the key database.
#[derive(Clone)]
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

// ── Redacting `Debug` impls ──────────────────────────────────────────────────
//
// Every type above carries AACS secret material (device keys, host PRIVATE keys,
// media/volume/processing/unit keys). `#[derive(Debug)]` would print those bytes
// verbatim, so a stray `debug!("{:?}", …)` or a panic message would leak the
// keys. These hand-written impls print only NON-secret shape (presence, lengths,
// tree coordinates, indices) — never key bytes. `decrypt::DecryptKeys` follows
// the same policy by omitting `Debug` entirely; here we keep `Debug` because
// these are `PartialEq`/`Eq` value types used in `assert_eq!` and nested inside
// other `#[derive(Debug)]` structs, so the trait must exist — just not leak.
// Guarded by `redaction_tests` below.

impl std::fmt::Debug for DeviceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceKey")
            .field("key", &"<redacted>")
            .field("node", &self.node)
            .field("uv", &self.uv)
            .field("u_mask_shift", &self.u_mask_shift)
            .finish()
    }
}

impl std::fmt::Debug for HostCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostCert")
            .field("private_key", &"<redacted>")
            .field("certificate_len", &self.certificate.len())
            .field("private_key_v2", &self.private_key_v2.map(|_| "<redacted>"))
            .field(
                "certificate_v2_len",
                &self.certificate_v2.as_ref().map(|c| c.len()),
            )
            .finish()
    }
}

impl std::fmt::Debug for Vid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Vid(<redacted>)")
    }
}

impl std::fmt::Debug for MediaKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MediaKey(<redacted>)")
    }
}

impl std::fmt::Debug for Vuk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Vuk(<redacted>)")
    }
}

impl std::fmt::Debug for ProcessingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProcessingKey(<redacted>)")
    }
}

impl std::fmt::Debug for UnitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitKey")
            .field("idx", &self.idx)
            .field("key", &"<redacted>")
            .field("index_number", &self.index_number)
            .finish()
    }
}

impl std::fmt::Debug for DiscEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscEntry")
            .field("disc_hash", &self.disc_hash)
            .field("title", &self.title)
            .field("media_key", &self.media_key.map(|_| "<redacted>"))
            .field("disc_id", &self.disc_id.map(|_| "<redacted>"))
            .field("vuk", &self.vuk.map(|_| "<redacted>"))
            .field("unit_keys_len", &self.unit_keys.len())
            .finish()
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    // Sentinel key byte 0xD5 = decimal 213. A derived `Debug` prints `[u8;N]`
    // as decimal, so a leaked key surfaces the substring "213"; the redacting
    // impls must not. No non-secret field below is 213, so "213" appearing means
    // key bytes leaked. Each type must also carry a "redacted" marker (or omit
    // the secret entirely) so re-adding `#[derive(Debug)]` fails this test.
    const S: u8 = 0xD5;

    fn assert_redacted(what: &str, dbg: &str) {
        assert!(
            !dbg.contains("213"),
            "{what}: Debug leaked key bytes (found decimal 213): {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "{what}: Debug missing redaction marker: {dbg}"
        );
    }

    #[test]
    fn device_key_debug_is_redacted() {
        let d = DeviceKey {
            key: [S; 16],
            node: 1,
            uv: 2,
            u_mask_shift: 3,
        };
        assert_redacted("DeviceKey", &format!("{d:?}"));
    }

    #[test]
    fn host_cert_debug_is_redacted() {
        let h = HostCert {
            private_key: [S; 20],
            certificate: vec![0u8; 92],
            private_key_v2: Some([S; 32]),
            certificate_v2: None,
        };
        assert_redacted("HostCert", &format!("{h:?}"));
    }

    #[test]
    fn newtype_keys_debug_is_redacted() {
        assert_redacted("Vid", &format!("{:?}", Vid([S; 16])));
        assert_redacted("MediaKey", &format!("{:?}", MediaKey([S; 16])));
        assert_redacted("Vuk", &format!("{:?}", Vuk([S; 16])));
        assert_redacted("ProcessingKey", &format!("{:?}", ProcessingKey([S; 16])));
    }

    #[test]
    fn unit_key_debug_is_redacted() {
        assert_redacted("UnitKey", &format!("{:?}", UnitKey::new(0, [S; 16])));
    }

    #[test]
    fn disc_entry_debug_is_redacted() {
        let e = DiscEntry {
            disc_hash: "0xAA".into(),
            title: "T".into(),
            media_key: Some([S; 16]),
            disc_id: Some([S; 16]),
            vuk: Some([S; 16]),
            unit_keys: vec![(1, [S; 16])],
        };
        assert_redacted("DiscEntry", &format!("{e:?}"));
    }
}

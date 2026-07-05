//! AACS primitive types shared across the resolve chain.
//!
//! These structs describe AACS key material (device keys, host
//! certificates, per-disc entries). They carry no parsing logic — the
//! keydb.cfg format lives in the `freemkv-keysources` crate. libfreemkv
//! owns only the crypto and these value types that flow through it.

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vid(pub [u8; 16]);

/// Media Key (Km, 16 bytes) — the MKB-scoped key derived from device keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaKey(pub [u8; 16]);

/// Volume Unique Key (VUK / Kvu, 16 bytes) — derived from `MediaKey` + `Vid`,
/// decrypts the per-disc encrypted title keys in `Unit_Key_RO.inf`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vuk(pub [u8; 16]);

/// Processing Key (Kp, 16 bytes) — an MKB Subset-Difference key that yields the
/// Media Key. A leaked/precomputed PK in the keydb, or the intermediate PK a
/// device-key walk derives at its matching SD node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessingKey(pub [u8; 16]);

/// One decrypted per-CPS-unit AACS title key.
///
/// `idx` is the POSITIONAL index of the encrypted title key within the slice
/// handed to the VUK→UK step (i.e. its order in `Unit_Key_RO.inf`'s key-storage
/// area). The CPS-unit *number* association is a higher-level concern owned by
/// [`super::inf::parse_unit_key_ro`], which pairs each positional key with its
/// declared CPS unit; this primitive only does the AES, so it surfaces position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitKey {
    pub idx: u32,
    pub key: [u8; 16],
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

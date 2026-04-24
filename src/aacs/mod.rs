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

pub mod decrypt;
pub mod handshake;
pub mod keydb;
pub mod keys;

// Explicit re-exports — only items needed by external consumers and sibling crate modules.
// AES primitives (aes_ecb_encrypt, aes_ecb_decrypt, aes_cbc_decrypt) are pub(crate) in decrypt.rs.
pub use decrypt::{
    ALIGNED_UNIT_LEN, decrypt_bus, decrypt_unit, decrypt_unit_full, decrypt_unit_try_keys,
    is_unit_encrypted,
};
pub use keydb::{DeviceKey, DiscEntry, HostCert, KeyDb};
pub use keys::{
    ContentCert, ResolvedKeys, UnitKeyFile, decrypt_unit_key, derive_media_key_from_dk,
    derive_media_key_from_pk, derive_vuk, disc_hash, disc_hash_hex, mkb_version,
    parse_content_cert, parse_unit_key_ro, read_mkb_from_drive, resolve_keys,
};

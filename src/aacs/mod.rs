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
//!   | HC2 | HOST_PRIV_KEY 0x... | HOST_CERT 0x...
//!   0x<disc_hash> = <title> | D | <date> | M | 0x<media_key> | I | 0x<disc_id> | V | 0x<vuk> | U | <unit_keys>
//!
//! The VUK decrypts title keys from AACS/Unit_Key_RO.inf on disc.
//! Title keys decrypt m2ts stream content (AES-128-CBC).

pub mod decrypt;
pub mod handshake;
pub mod keydb;
pub mod keys;
pub mod provider;
pub mod variants;

// Explicit re-exports — only items needed by external consumers and sibling crate modules.
// AES primitives (aes_ecb_encrypt, aes_ecb_decrypt, aes_cbc_decrypt) are pub(crate) in decrypt.rs.
pub use decrypt::{
    ALIGNED_UNIT_LEN, UnitKeyResult, decrypt_bus, decrypt_unit, decrypt_unit_full,
    decrypt_unit_try_keys, is_aacs_scrambled, ts_packet_total, ts_sync_count, unit_key_validates,
};
pub use keydb::{DeviceKey, DiscEntry, HostCert, KeyDb};
pub use keys::probe;
pub use keys::{
    AacsVersion, ContentCert, ResolveContext, ResolvedKeys, UnitKeyFile, decrypt_unit_key,
    derive_media_key_from_dk, derive_media_key_from_pk, derive_media_key_from_pk_walked,
    derive_vuk, disc_hash, disc_hash_hex, mkb_content_len, mkb_version, parse_content_cert,
    parse_unit_key_ro, read_mkb_from_drive, resolve_keys_v1, resolve_keys_v2, resolve_keys_v21,
    trim_mkb,
};
pub use provider::KeyProvider;
pub use variants::{
    KEY_CORRECTION_DATA_PLACEHOLDER, MediaKeyVariantError, MkbRecord, ProcessingKeyMatch,
    derive_media_key_variant, is_variant_mkb, variant_nonce, walk_mkb, walk_processing_key,
};

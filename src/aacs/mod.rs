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

pub mod boil;
pub mod decrypt;
pub mod host_certs;
pub mod keys;
pub mod provider;
pub mod trace;
pub mod types;
pub mod variants;

/// On-disc UDF paths to the AACS key-input files (with their fallbacks).
/// Centralised so every reader (`resolve_vid_only`, `read_aacs_inputs`,
/// `read_mkb_content`, `read_aacs_version`) walks the exact same files — adding
/// or changing a fallback in one place can then never silently diverge the
/// disc_hash / MKB / VID that another reader feeds a key service.
pub const PATH_UNIT_KEY_RO: &str = "/AACS/Unit_Key_RO.inf";
pub const PATH_UNIT_KEY_RO_DUPLICATE: &str = "/AACS/DUPLICATE/Unit_Key_RO.inf";
pub const PATH_MKB_RO: &str = "/AACS/MKB_RO.inf";
pub const PATH_MKB_RW: &str = "/AACS/MKB_RW.inf";
pub const PATH_CONTENT_CERT: &str = "/AACS/Content000.cer";
pub const PATH_CONTENT_CERT_ALT: &str = "/AACS/Content001.cer";

// Boil-down derivation primitives (thin newtypes + wrappers over the crypto).
pub use boil::{MediaKey, UnitKey, Vid, Vuk, mk_from_dk, mk_from_pk, uk_from_vuk, vuk_from_mk};
// Structured, English-free resolution trace.
pub use trace::{KeyNode, KeyOutcome, KeyStep, ResolutionTrace, UnlockOutcome, UnlockStep};

// Explicit re-exports — only items needed by external consumers and sibling crate modules.
// AES primitives (aes_ecb_encrypt, aes_ecb_decrypt, aes_cbc_decrypt) are pub(crate) in decrypt.rs.
pub use decrypt::{
    ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, UnitKeyResult, aacs_unit_encrypted,
    aacs_unit_needs_decrypt, aacs_unit_still_ciphertext, decrypt_bus, decrypt_unit,
    decrypt_unit_checked, decrypt_unit_full, decrypt_unit_try_keys, fill_null_ts_unit,
    is_unit_aligned, ts_packet_total, ts_sync_count, ts_sync_destroyed, unit_is_clean_ps,
    unit_is_clean_ts, unit_key_validates,
};
// `probe` is a reproduction-harness helper (see keys.rs), not part of the
// documented 1.0 surface; keep it reachable but off the rendered docs so we
// don't commit semver stability to test primitives.
#[doc(hidden)]
pub use keys::probe;
pub use keys::{
    AACS_MAJOR_BD, AACS_MAJOR_UHD, AacsVersion, ContentCert, MKB_20_CATEGORY_C, MKB_21_CATEGORY_C,
    MKB_TYPE_3_RECORDABLE, MKB_TYPE_4_PRERECORDED, MKB_TYPE_10_CLASS_II, MkbType, ResolveContext,
    ResolveFailure, ResolvedKeys, UnitKeyFile, decrypt_unit_key, derive_media_key_and_pk_from_dk,
    derive_media_key_from_dk, derive_media_key_from_pk, derive_vuk, disc_hash, disc_hash_hex,
    mkb_content_len, mkb_is_uhd, mkb_type, mkb_type_raw, mkb_version, parse_content_cert,
    parse_unit_key_ro, read_mkb_from_drive, recover_dk_position, resolve_keys_v1, resolve_keys_v2,
    resolve_keys_v21, resolve_keys_with_reason, trim_mkb,
};
pub use provider::KeyProvider;
pub use types::{DeviceKey, DiscEntry, HostCert};
pub use variants::{
    KEY_CORRECTION_DATA_PLACEHOLDER, MediaKeyVariantError, MkbRecord, ProcessingKeyMatch,
    derive_media_key_variant, is_variant_mkb, variant_nonce, walk_mkb, walk_processing_key,
};

#[cfg(test)]
mod tests {
    //! Re-export surface guards. The module's public API is the set of
    //! `pub use` items above. A regression that drops or renames an export
    //! (the class of bug that shipped in 0.31.0 by silently changing a
    //! surface) breaks compilation of these references, so they act as a
    //! compile-time contract for the crate's AACS surface.

    use super::*;

    #[test]
    fn aligned_unit_len_is_three_2048_byte_sectors() {
        // ALIGNED_UNIT_LEN is the AACS aligned-unit size: 3 × 2048 = 6144.
        // Re-exported from decrypt; pin the value here so the public constant
        // and the spec stay in lockstep.
        assert_eq!(ALIGNED_UNIT_LEN, 6144);
        assert_eq!(ALIGNED_UNIT_LEN, 3 * 2048);
    }

    #[test]
    fn version_strides_are_reexported_and_distinct() {
        // The three AACS generations are part of the public surface, and the
        // V10 (48) vs V20/V21 (64) stride distinction is the load-bearing
        // difference. Confirm the enum re-export is usable and the variants
        // are distinct values.
        assert_ne!(AacsVersion::V10, AacsVersion::V20);
        assert_ne!(AacsVersion::V20, AacsVersion::V21);
    }

    #[test]
    fn key_correction_data_placeholder_is_all_zero() {
        // The variant chain refuses to run against this all-zero placeholder
        // KCD; the public constant must therefore be exactly 16 zero bytes.
        assert_eq!(KEY_CORRECTION_DATA_PLACEHOLDER, [0u8; 16]);
    }

    #[test]
    fn public_helpers_are_callable_through_the_facade() {
        // Touch a representative function from each re-export group so a
        // dropped/renamed export fails to compile. These are smoke calls, not
        // behavioural assertions (behaviour is covered in each module).
        let _ = ts_sync_destroyed(&[0u8; ALIGNED_UNIT_LEN]);
        let _ = mkb_content_len(&[]);
        let _ = is_variant_mkb(&walk_mkb(&[]));
        let _ = disc_hash_hex(&disc_hash(b"x"));
        let _ = mk_from_pk(&[[0u8; 16]], &[]);
    }
}

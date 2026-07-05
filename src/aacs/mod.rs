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
//!
//! ## Spec provenance
//!
//! The crypto below carries `[TAG] §x.y` citations back to the published AACS
//! specification (Final Rev 0.953), so each primitive links to the section it
//! implements:
//!   - `[C]`  — AACS Introduction and Common Cryptographic Elements Book (primitives, MKB/key-management).
//!   - `[PR]` — AACS Pre-recorded Video Book (Volume/Title Key layer).
//!   - `[BD]` — AACS Blu-ray Disc Pre-recorded Book (CPS Unit Key, Aligned Unit, Block Key).
//!   - `[libaacs]` — the libaacs reference implementation, cited only where the spec
//!     is silent (the `0x86` verify record and the Category-C MKBType names).

pub mod boil;
pub mod content;
pub mod crypto;
pub mod derive;
pub mod host_certs;
pub mod inf;
pub mod mkb;
pub mod provider;
pub mod resolve;
pub mod trace;
pub mod types;
pub mod variant;

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

// No facade: the module structure IS the public API. Consumers import from the
// owning module directly — e.g. `aacs::content::decrypt_unit`, `aacs::mkb::MkbType`,
// `aacs::derive::derive_vuk`, `aacs::boil::mk_from_dk`, `aacs::resolve::resolve_keys_v2`.
// The `derive::probe` reproduction harness stays reachable via its module path.

#[cfg(test)]
mod tests {
    //! Surface guards. The public API is the module tree itself (no facade).
    //! Touching one representative item per module keeps these as a
    //! compile-time contract that the module paths stay stable.

    use super::content::{ALIGNED_UNIT_LEN, ts_sync_destroyed};
    use super::inf::{disc_hash, disc_hash_hex};
    use super::mkb::{AacsVersion, mkb_content_len, walk_mkb};
    use super::variant::is_variant_mkb;

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
    fn public_helpers_are_callable_by_module_path() {
        // Touch a representative function from each module so a dropped/renamed
        // item fails to compile. Smoke calls, not behavioural assertions.
        let _ = ts_sync_destroyed(&[0u8; ALIGNED_UNIT_LEN]);
        let _ = mkb_content_len(&[]);
        let _ = is_variant_mkb(&walk_mkb(&[]));
        let _ = disc_hash_hex(&disc_hash(b"x"));
        let _ = super::boil::mk_from_pk(&[[0u8; 16]], &[]);
    }
}

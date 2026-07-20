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
//!   - `[RE]` — reverse-engineered from real discs, cited only where the public
//!     spec is silent (the `0x86` verify record and the Category-C MKB type values).

pub mod content;
pub mod crypto;
pub mod derive;
pub mod host_certs;
pub mod index_select;
pub mod inf;
pub mod mkb;
pub mod provider;
pub mod resolve;
pub mod segment;
pub mod segment_key;
pub mod trace;
pub mod types;
pub mod variant;

/// On-disc UDF paths to the AACS key-input files, plus HD DVD AACS-directory
/// discovery.
///
/// BD and UHD keep their key material under a fixed `/AACS/…` tree, so those
/// paths are constants. HD DVD keeps the equivalents in a reserved root
/// directory whose NAME is authoring-house-specific — observed `ANY!` (Dukes
/// of Hazzard) and `AAC!` (Freedom / Memory-Tech), each with a `<name>!_BAK`
/// mirror — and whose title-key file is NOT always `VTKF000.AACS` (Freedom
/// ships `VTKF090.AACS` + `VTKF100.AACS`). So the HD DVD files are DISCOVERED
/// from the parsed UDF tree ([`find_hddvd_aacs_dir`] + [`role_paths`]), never
/// hardcoded.
///
/// Each key ROLE ([`AacsRole`]) resolves to an ordered candidate list — the
/// BD/UHD constants first, then whatever the HD DVD directory actually holds —
/// which every reader walks with [`read_first`], first-that-reads. No reader
/// ever branches on disc type: a BD/UHD disc has the `/AACS/` files so those
/// win; an HD DVD has none of them, so it falls through to the discovered
/// entries. Centralised so `resolve_vid_only`, `read_aacs_inputs`,
/// `read_mkb_content`, and `read_aacs_version` can never silently diverge the
/// disc_hash / MKB / VID that another reader feeds a key service.
pub const PATH_UNIT_KEY_RO: &str = "/AACS/Unit_Key_RO.inf";
pub const PATH_UNIT_KEY_RO_DUPLICATE: &str = "/AACS/DUPLICATE/Unit_Key_RO.inf";
pub const PATH_MKB_RO: &str = "/AACS/MKB_RO.inf";
pub const PATH_MKB_RW: &str = "/AACS/MKB_RW.inf";
pub const PATH_CONTENT_CERT: &str = "/AACS/Content000.cer";
pub const PATH_CONTENT_CERT_ALT: &str = "/AACS/Content001.cer";

/// An AACS key-input role. [`role_paths`] maps it to an ordered candidate path
/// list (BD/UHD constants, then the discovered HD DVD files).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AacsRole {
    /// Title-key file: BD/UHD `Unit_Key_RO.inf`, HD DVD `VTKF*.AACS`
    /// (magic `DVD_HD_V_TKF`). The disc_hash is `SHA1` of this file.
    UnitKey,
    /// Media Key Block: BD/UHD `MKB_RO/RW.inf`, HD DVD `MKBROM.AACS`.
    Mkb,
    /// Content certificate: BD/UHD `Content000/001.cer`, HD DVD
    /// `CONTENT_CERT.AACS` (byte 0 gives the AACS major).
    ContentCert,
}

/// The HD DVD AACS directory in a parsed UDF tree, if present.
///
/// Identified structurally, NOT by a hardcoded name: the root child directory
/// whose name ends in `!` (so the `<name>!_BAK` backup mirror, which also ends
/// in a non-`!` char, is not mistaken for it) and which contains `MKBROM.AACS`.
/// Observed real names: `ANY!` (Dukes of Hazzard), `AAC!` (Freedom). A BD/UHD
/// disc has no such directory → `None`.
pub(crate) fn find_hddvd_aacs_dir(udf: &crate::udf::UdfFs) -> Option<&crate::udf::DirEntry> {
    udf.root.entries.iter().find(|e| {
        e.is_dir
            && e.name.ends_with('!')
            && e.entries
                .iter()
                .any(|c| !c.is_dir && c.name.eq_ignore_ascii_case("MKBROM.AACS"))
    })
}

/// Ordered candidate paths for an AACS key [`AacsRole`]: the fixed BD/UHD
/// `/AACS/…` paths first, then the actual HD DVD files discovered in the disc's
/// AACS directory (see [`find_hddvd_aacs_dir`]). A disc has only one family, so
/// the other family's entries simply never read.
///
/// For [`AacsRole::UnitKey`] every `VTKF*.AACS` in the directory is appended in
/// sorted name order — a disc may carry more than one variant (Freedom:
/// `VTKF090` + `VTKF100`), not just `VTKF000`.
pub(crate) fn role_paths(udf: &crate::udf::UdfFs, role: AacsRole) -> Vec<String> {
    let mut v: Vec<String> = match role {
        AacsRole::UnitKey => vec![PATH_UNIT_KEY_RO, PATH_UNIT_KEY_RO_DUPLICATE],
        AacsRole::Mkb => vec![PATH_MKB_RO, PATH_MKB_RW],
        AacsRole::ContentCert => vec![PATH_CONTENT_CERT, PATH_CONTENT_CERT_ALT],
    }
    .into_iter()
    .map(String::from)
    .collect();

    if let Some(dir) = find_hddvd_aacs_dir(udf) {
        let d = &dir.name;
        match role {
            AacsRole::Mkb => v.push(format!("/{d}/MKBROM.AACS")),
            AacsRole::ContentCert => v.push(format!("/{d}/CONTENT_CERT.AACS")),
            AacsRole::UnitKey => {
                // Glob VTKF*.AACS — the title-key filename is not fixed at
                // VTKF000 (Freedom ships VTKF090 + VTKF100). Sorted for a
                // deterministic try order.
                //
                // TODO(hddvd-encrypted): when a disc carries MULTIPLE VTKF
                // variants, the CORRECT one is chosen by validating its
                // VUK-derived key against a real encrypted unit — not by
                // first-that-reads (all read). Wire that selection here once a
                // genuinely encrypted HD DVD image exists to validate against
                // (see `content::aacs_unit_encrypted` UNVERIFIED-HDDVD-DECRYPT).
                let mut names: Vec<&str> = dir
                    .entries
                    .iter()
                    .filter(|e| !e.is_dir)
                    .filter(|e| {
                        let u = e.name.to_ascii_uppercase();
                        u.starts_with("VTKF") && u.ends_with(".AACS")
                    })
                    .map(|e| e.name.as_str())
                    .collect();
                names.sort_unstable();
                v.extend(names.into_iter().map(|n| format!("/{d}/{n}")));
            }
        }
    }
    v
}

/// Walk an AACS role's candidate paths (from [`role_paths`]) and return the
/// first that reads.
///
/// `read` performs the actual per-path read (full file or bounded prefix), so
/// callers share the same first-present walk regardless of read style. Returns
/// [`Error::AacsNoKeys`] if no candidate is present. Generic over the path
/// element (`&str` or owned `String`) so it accepts the `Vec<String>` that
/// [`role_paths`] builds from the discovered HD DVD directory.
pub(crate) fn read_first<S, F>(candidates: &[S], mut read: F) -> crate::error::Result<Vec<u8>>
where
    S: AsRef<str>,
    F: FnMut(&str) -> crate::error::Result<Vec<u8>>,
{
    for path in candidates {
        if let Ok(buf) = read(path.as_ref()) {
            return Ok(buf);
        }
    }
    Err(crate::error::Error::AacsNoKeys)
}

// The module structure IS the public API — consumers import from the owning
// module directly (e.g. `aacs::content::decrypt_unit`, `aacs::mkb::MkbType`,
// `aacs::derive::{derive_vuk, resolve_candidate}`, `aacs::resolve::resolve_keys_v2`).
// The `derive::probe` reproduction harness stays reachable via its module path.
//
// A small set of flat re-exports is kept for the typed key primitives and the
// content-decrypt entry points that downstream key-source crates import through
// the `aacs::` path. These are the stable, load-bearing names; keeping them here
// lets those crates track the module refactor without a lockstep re-pin.
pub use content::ALIGNED_UNIT_LEN;
pub use derive::derive_vuk;
pub use types::{DeviceKey, HostCert, MediaKey, ProcessingKey, UnitKey, Vid, Vuk};

#[cfg(test)]
mod tests {
    //! Surface guards. The public API is the module tree itself (no facade).
    //! Touching one representative item per module keeps these as a
    //! compile-time contract that the module paths stay stable.

    use super::content::ALIGNED_UNIT_LEN;
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
        let _ = !crate::aacs::content::is_clean(
            &[0u8; ALIGNED_UNIT_LEN],
            crate::disc::ContentFormat::BdTs,
        );
        let _ = mkb_content_len(&[]);
        let _ = is_variant_mkb(&walk_mkb(&[]));
        let _ = disc_hash_hex(&disc_hash(b"x"));
        let _ = super::derive::resolve_candidate(
            &super::derive::KeyCandidate::Uk(super::types::UnitKey::new(0, [0u8; 16])),
            &[],
            &[],
            None,
        );
    }

    // ── HD DVD AACS directory / filename discovery ────────────────────────
    //
    // The HD DVD AACS dir name and title-key filename are authoring-specific
    // and were previously hardcoded to `/ANY!/VTKF000.AACS`. These verify the
    // discovery replacement against both real-disc shapes: Freedom (`AAC!` +
    // `VTKF090`/`VTKF100`) and a BD/UHD disc (no HD DVD dir).

    #[test]
    fn role_paths_discovers_hddvd_dir_and_globs_all_vtkf_variants() {
        use crate::udf::fixture::*;
        // Freedom-shaped: an `AAC!` dir (NOT `ANY!`) holding MKBROM + two VTKF
        // variants (090/100, NOT 000) + a VTUF usage file (must be excluded),
        // plus the `AAC!_BAK` mirror (must NOT be picked as the AACS dir).
        let mut disc = MemDisc::new();
        let aacs_files = vec![
            file("MKBROM.AACS", 100, 5000, 4096, true),
            file("CONTENT_CERT.AACS", 101, 5100, 2048, true),
            file("VTKF100.AACS", 102, 5200, 2048, true),
            file("VTKF090.AACS", 103, 5300, 2048, true),
            file("VTUF090.AACS", 104, 5400, 2048, true),
        ];
        let bak_files = vec![file("MKBROM.AACS", 110, 6000, 4096, true)];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "AAC!".to_string(),
                    icb_lba: 20,
                    dir_data_lba: 21,
                    files: aacs_files,
                    subdirs: vec![],
                },
                DirSpec {
                    name: "AAC!_BAK".to_string(),
                    icb_lba: 30,
                    dir_data_lba: 31,
                    files: bak_files,
                    subdirs: vec![],
                },
            ],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        // Discovered structurally (ends in '!', holds MKBROM.AACS) — the real
        // AACS dir, never the `_BAK` mirror.
        let dir = super::find_hddvd_aacs_dir(&udf).expect("aacs dir");
        assert_eq!(dir.name, "AAC!");

        // UnitKey: BD/UHD paths first, then EVERY VTKF*.AACS in sorted order
        // (090 before 100) — NOT hardcoded VTKF000; VTUF (usage) excluded.
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::UnitKey),
            vec![
                super::PATH_UNIT_KEY_RO.to_string(),
                super::PATH_UNIT_KEY_RO_DUPLICATE.to_string(),
                "/AAC!/VTKF090.AACS".to_string(),
                "/AAC!/VTKF100.AACS".to_string(),
            ]
        );
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::Mkb)
                .last()
                .unwrap(),
            "/AAC!/MKBROM.AACS"
        );
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::ContentCert)
                .last()
                .unwrap(),
            "/AAC!/CONTENT_CERT.AACS"
        );
    }

    #[test]
    fn role_paths_bd_uhd_disc_yields_no_hddvd_candidates() {
        use crate::udf::fixture::*;
        // A `/AACS/` tree (BD/UHD) has no '!' directory → discovery finds none
        // and the candidate list is exactly the static BD/UHD paths.
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "AACS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![
                    file("Unit_Key_RO.inf", 100, 5000, 2048, true),
                    file("MKB_RO.inf", 101, 5100, 2048, true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        assert!(super::find_hddvd_aacs_dir(&udf).is_none());
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::UnitKey),
            vec![
                super::PATH_UNIT_KEY_RO.to_string(),
                super::PATH_UNIT_KEY_RO_DUPLICATE.to_string(),
            ]
        );
    }
}

//! AACS primitives: MKB, title-key file, derivation, unit crypto and the resolution trace.
//! Keys are acquired only through [`crate::keys::KeyRing::acquire`]; title keys decrypt
//! m2ts stream content (AES-128-CBC).

pub mod content;
pub mod crypto;
pub mod derive;
// HD DVD per-pack content encryption (`[HD]` §4.3).
pub(crate) mod hddvd;
pub mod host_certs;
// No production caller; not the live FMTS path.
#[doc(hidden)]
pub mod index_select;
pub mod inf;
pub mod mkb;
pub mod segment;
// No production caller; not the live FMTS path.
#[doc(hidden)]
pub mod segment_key;
pub mod trace;
pub mod types;
pub mod variant;

// On-disc UDF paths to the AACS key-input files; each AacsRole resolves to an ordered
// candidate list walked by read_first (see role_paths, find_hddvd_aacs_dir).
/// Primary Unit Key file path.
pub const PATH_UNIT_KEY_RO: &str = "/AACS/Unit_Key_RO.inf";
pub const PATH_UNIT_KEY_RO_DUPLICATE: &str = "/AACS/DUPLICATE/Unit_Key_RO.inf";
pub const PATH_MKB_RO: &str = "/AACS/MKB_RO.inf";
pub const PATH_MKB_RO_DUPLICATE: &str = "/AACS/DUPLICATE/MKB_RO.inf";
/// The recordable-media MKB — a DIFFERENT MKB, never a fallback for `MKB_RO`.
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
    /// Media Key Block: BD/UHD `MKB_RO.inf` (then its DUPLICATE copy), HD DVD `MKBROM.AACS`.
    Mkb,
    /// Content certificate: BD/UHD `Content000/001.cer`, HD DVD
    /// `CONTENT_CERT.AACS` (byte 0 gives the AACS major).
    ContentCert,
}

// The HD DVD AACS directory in a parsed UDF tree, if present: any root directory holding
// MKBROM.AACS, whatever its name (`ANY!`, `AAC!`, plain `AACS` on "300"). A `*_BAK` mirror
// is used only when no primary copy exists.
pub(crate) fn find_hddvd_aacs_dir(udf: &crate::udf::UdfFs) -> Option<&crate::udf::DirEntry> {
    let mut dirs = udf.root.entries.iter().filter(|e| {
        e.is_dir
            && e.entries
                .iter()
                .any(|c| !c.is_dir && c.name.eq_ignore_ascii_case("MKBROM.AACS"))
    });
    let is_bak = |e: &crate::udf::DirEntry| e.name.to_ascii_uppercase().ends_with("_BAK");
    let first = dirs.next()?;
    if !is_bak(first) {
        return Some(first);
    }
    dirs.find(|e| !is_bak(e)).or(Some(first))
}

// Ordered candidate paths for an AACS key role: fixed BD/UHD `/AACS/…` paths
// first, then discovered HD DVD files (see `find_hddvd_aacs_dir`). UnitKey
// appends every VTKF*.AACS found, sorted, since a disc may ship >1 variant.
pub(crate) fn role_paths(udf: &crate::udf::UdfFs, role: AacsRole) -> Vec<String> {
    let mut v: Vec<String> = match role {
        AacsRole::UnitKey => vec![PATH_UNIT_KEY_RO, PATH_UNIT_KEY_RO_DUPLICATE],
        AacsRole::Mkb => vec![PATH_MKB_RO, PATH_MKB_RO_DUPLICATE],
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
                // Glob VTKF*.AACS (not fixed at VTKF000; Freedom ships VTKF090 +
                // VTKF100), sorted for a deterministic try order. Each is bound to
                // ONE playlist; caller tries in order, uses whichever's keys verify.
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

/// Walk an AACS role's candidate paths, returning the first that reads.
///
/// AACS ships a `/AACS/DUPLICATE/` copy of every managed file so a bad primary
/// read can fall through to the backup. A missing candidate (`UdfNotFound`) and a
/// failed read (`DiscRead`) are both retriable: the walk moves on rather than
/// aborting. After all candidates are tried, a remembered `DiscRead` propagates
/// (more informative than `AacsNoKeys`); if every candidate was merely absent, the
/// result is `AacsNoKeys`. Any other error is a hard failure (parse, corruption)
/// and propagates immediately.
pub(crate) fn read_first<S, F>(candidates: &[S], mut read: F) -> crate::error::Result<Vec<u8>>
where
    S: AsRef<str>,
    F: FnMut(&str) -> crate::error::Result<Vec<u8>>,
{
    let mut deferred: Option<crate::error::Error> = None;
    for path in candidates {
        match read(path.as_ref()) {
            Ok(buf) => return Ok(buf),
            Err(crate::error::Error::UdfNotFound { .. }) => continue,
            Err(e @ crate::error::Error::DiscRead { .. }) => {
                deferred = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(deferred.unwrap_or(crate::error::Error::AacsNoKeys))
}

// The module structure IS the public API — consumers import from the owning module
// (e.g. `aacs::content::is_clean`). A small set of flat re-exports below is kept
// for typed key primitives and content-decrypt entry points that downstream crates rely on.
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
        // ALIGNED_UNIT_LEN is the AACS aligned-unit size (3 × 2048 = 6144);
        // pin it here so the public constant tracks the spec.
        assert_eq!(ALIGNED_UNIT_LEN, 6144);
        assert_eq!(ALIGNED_UNIT_LEN, 3 * 2048);
    }

    // Any error but UdfNotFound / DiscRead is a hard failure: a good DUPLICATE copy must not
    // mask it.
    #[test]
    fn read_first_propagates_a_hard_error_without_trying_the_duplicate() {
        use crate::error::Error;
        let candidates = ["/AACS/Unit_Key_RO.inf", "/AACS/DUPLICATE/Unit_Key_RO.inf"];
        let mut tried = Vec::new();
        let out = super::read_first(&candidates, |p| {
            tried.push(p.to_string());
            if p == "/AACS/DUPLICATE/Unit_Key_RO.inf" {
                Ok(vec![0xAA])
            } else {
                Err(Error::DecryptFailed)
            }
        });
        assert!(matches!(out, Err(Error::DecryptFailed)));
        assert_eq!(tried, vec!["/AACS/Unit_Key_RO.inf".to_string()]);
    }

    // AACS `/AACS/DUPLICATE/` redundancy: a `DiscRead` on the primary managed
    // file must fall through to the backup copy, not abort the whole read.
    #[test]
    fn read_first_falls_through_a_primary_disc_read_to_the_duplicate() {
        use crate::error::Error;
        let candidates = ["/AACS/Unit_Key_RO.inf", "/AACS/DUPLICATE/Unit_Key_RO.inf"];
        let out = super::read_first(&candidates, |p| {
            if p == "/AACS/DUPLICATE/Unit_Key_RO.inf" {
                Ok(vec![0xAA, 0xC5])
            } else {
                Err(Error::DiscRead {
                    sector: 42,
                    status: Some(0x02),
                    sense: None,
                })
            }
        });
        assert_eq!(
            out.unwrap(),
            vec![0xAA, 0xC5],
            "a primary DiscRead must fall through to the DUPLICATE copy"
        );
    }

    // When every candidate fails to read, the remembered `DiscRead` propagates
    // (more informative than a bare `AacsNoKeys`); a purely absent set yields
    // `AacsNoKeys`.
    #[test]
    fn read_first_propagates_disc_read_only_after_all_candidates_fail() {
        use crate::error::Error;
        let candidates = ["/AACS/Unit_Key_RO.inf", "/AACS/DUPLICATE/Unit_Key_RO.inf"];
        let err = super::read_first(&candidates, |p| {
            if p.contains("DUPLICATE") {
                Err(Error::UdfNotFound { path: p.into() })
            } else {
                Err(Error::DiscRead {
                    sector: 7,
                    status: None,
                    sense: None,
                })
            }
        })
        .unwrap_err();
        assert!(
            matches!(err, Error::DiscRead { sector: 7, .. }),
            "the primary DiscRead must propagate once the DUPLICATE is absent, got {err:?}"
        );

        let all_absent =
            super::read_first(&candidates, |p| Err(Error::UdfNotFound { path: p.into() }))
                .unwrap_err();
        assert!(
            matches!(all_absent, Error::AacsNoKeys),
            "an entirely absent candidate set yields AacsNoKeys, got {all_absent:?}"
        );
    }

    #[test]
    fn version_strides_are_reexported_and_distinct() {
        // V10 (48) vs V20/V21 (64) stride is the load-bearing distinction;
        // confirm the enum re-export is usable and variants are distinct.
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
            super::mkb::AacsVersion::V10,
        );
    }

    // ── HD DVD AACS directory / filename discovery ────────────────────────
    // Dir/filename are authoring-specific (previously hardcoded to `/ANY!/VTKF000.AACS`);
    // verify against Freedom (`AAC!` + `VTKF090`/`VTKF100`) and a BD/UHD disc (no HD DVD dir).

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

        // Discovered structurally (holds MKBROM.AACS) — the real
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

    // "300" HD DVDs keep MKBROM/VTKF/CONTENT_CERT in a plain `/AACS` (mirror `AACS_BAK`):
    // found by MKBROM.AACS, not the name, and the mirror is skipped even when listed first.
    #[test]
    fn a_plain_aacs_directory_holding_mkbrom_is_the_hddvd_aacs_directory() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let dir = |name: &str, lba: u32, files| DirSpec {
            name: name.to_string(),
            icb_lba: lba,
            dir_data_lba: lba + 1,
            files,
            subdirs: vec![],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                dir(
                    "AACS_BAK",
                    30,
                    vec![file("MKBROM.AACS", 110, 6000, 4096, true)],
                ),
                dir(
                    "AACS",
                    20,
                    vec![
                        file("MKBROM.AACS", 100, 5000, 4096, true),
                        file("CONTENT_CERT.AACS", 101, 5100, 2048, true),
                        file("VTKF000.AACS", 102, 5200, 2048, true),
                    ],
                ),
            ],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert_eq!(super::find_hddvd_aacs_dir(&udf).expect("dir").name, "AACS");
        let mkb = super::role_paths(&udf, super::AacsRole::Mkb);
        assert_eq!(mkb.last().unwrap(), "/AACS/MKBROM.AACS");
        let uk = super::role_paths(&udf, super::AacsRole::UnitKey);
        assert_eq!(uk.last().unwrap(), "/AACS/VTKF000.AACS");
    }

    // A mirror alone is still used: a damaged primary must not leave the disc unkeyable.
    #[test]
    fn a_lone_bak_mirror_is_used_when_no_primary_exists() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "ANY!_BAK".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file("MKBROM.AACS", 100, 5000, 4096, true)],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert_eq!(
            super::find_hddvd_aacs_dir(&udf).expect("dir").name,
            "ANY!_BAK"
        );
    }

    // MKBROM.AACS presence is required — else the HD DVD path resolves key files under a
    // dir holding none.
    #[test]
    fn a_bang_suffixed_directory_without_mkbrom_is_not_the_aacs_directory() {
        use crate::udf::fixture::*;
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                // Ends in '!' — but carries no MKBROM.AACS, so it is not the
                // HD DVD AACS directory.
                name: "AAC!".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![
                    file("VTKF090.AACS", 102, 5200, 2048, true),
                    file("CONTENT_CERT.AACS", 103, 5300, 2048, true),
                ],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        assert!(
            super::find_hddvd_aacs_dir(&udf).is_none(),
            "a '!' directory without MKBROM.AACS is not the AACS directory"
        );
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::UnitKey),
            vec![
                super::PATH_UNIT_KEY_RO.to_string(),
                super::PATH_UNIT_KEY_RO_DUPLICATE.to_string(),
            ],
            "no HD DVD candidates may be appended from a directory that was \
             never identified as the AACS directory"
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
        // MKB fallback is the DUPLICATE copy of MKB_RO (libaacs `_mkb_open`), never
        // MKB_RW.inf — a different MKB that derives a different Media Key.
        assert_eq!(
            super::role_paths(&udf, super::AacsRole::Mkb),
            vec![
                "/AACS/MKB_RO.inf".to_string(),
                "/AACS/DUPLICATE/MKB_RO.inf".to_string(),
            ]
        );
    }
}

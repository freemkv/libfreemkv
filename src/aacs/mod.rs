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

/// An AACS key-input role. `role_paths` maps it to an ordered candidate path
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
#[path = "mod_tests.rs"]
mod tests;

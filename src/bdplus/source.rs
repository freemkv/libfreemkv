//! Where conversion tables come from: the [`ConversionTableSource`] seam.

use super::table::{ConversionTable, MAX_TABLE_BYTES};
use crate::disc::Disc;
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// What a table source can key a disc by. Built from a scan with [`Self::of`].
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BdPlusDiscId {
    /// SHA-1 of `Unit_Key_RO.inf`, 40 lowercase hex digits without `0x` (the KEYDB disc
    /// hash); empty when the disc has no AACS identity.
    pub disc_hash: String,
    /// The AACS Volume ID, when the drive handshake read one.
    pub volume_id: Option<[u8; 16]>,
    /// The AACS Media Key, when the caller derived one (the scan never does).
    pub media_key: Option<[u8; 16]>,
}

// Redacting: the Volume ID and Media Key are key material.
impl std::fmt::Debug for BdPlusDiscId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusDiscId")
            .field("disc_hash", &self.disc_hash)
            .field("volume_id", &self.volume_id.map(|_| "<redacted>"))
            .field("media_key", &self.media_key.map(|_| "<redacted>"))
            .finish()
    }
}

impl BdPlusDiscId {
    /// The identity `disc`'s scan knows: its disc hash and, if read, its Volume ID.
    pub fn of(disc: &Disc) -> Self {
        let hash = disc.media_id().disc_hash;
        let vid = disc
            .aacs
            .as_ref()
            .map(|a| a.volume_id)
            .filter(|v| v.iter().any(|&b| b != 0));
        BdPlusDiscId {
            disc_hash: crate::hex::strip_hex_prefix(&hash).to_ascii_lowercase(),
            volume_id: vid,
            media_key: None,
        }
    }

    /// The same identity carrying `media_key`, for sources keyed by it.
    pub fn with_media_key(mut self, media_key: [u8; 16]) -> Self {
        self.media_key = Some(media_key);
        self
    }
}

/// A source of BD+ conversion tables, consulted in order by
/// [`BdPlusResolution::resolve`](super::BdPlusResolution::resolve).
///
/// `Ok(None)` means "not here, ask the next source"; `Err` is logged and the next source
/// asked. A future source may run the disc's BD+ program instead of looking a table up;
/// it returns the table that program delivers, its segments decrypted.
pub trait ConversionTableSource: Send + Sync {
    /// A short stable name, reported in [`BdPlusStatus::Resolved`](super::BdPlusStatus).
    fn name(&self) -> &str;
    /// The disc's table, if this source has it.
    fn lookup(&self, id: &BdPlusDiscId) -> Result<Option<ConversionTable>>;
}

/// The source that has no tables: every BD+ disc stays unresolved.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTables;

impl ConversionTableSource for NoTables {
    fn name(&self) -> &str {
        "none"
    }
    fn lookup(&self, _: &BdPlusDiscId) -> Result<Option<ConversionTable>> {
        Ok(None)
    }
}

/// Activated tables (libbdplus `convtab.bin` layout) in a directory the caller chooses.
///
/// Looked up, first match wins: `<dir>/<disc hash>.bin` (40 hex digits, the KEYDB disc
/// hash); `<dir>/<Volume ID>/convtab.bin` (32 hex digits, libbdplus's own cache layout);
/// `<dir>/<Media Key>.bin` (32 hex digits, libbdplus's lookup name). Hex is lowercase or
/// uppercase. A file over 4 MiB or that does not parse is an error, not a miss.
#[derive(Debug, Clone)]
pub struct TableDir {
    dir: PathBuf,
}

impl TableDir {
    /// Tables under `dir`.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        TableDir { dir: dir.into() }
    }

    /// The directory searched.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // Candidate paths for `id`, in lookup order.
    fn candidates(&self, id: &BdPlusDiscId) -> Vec<PathBuf> {
        let mut names: Vec<PathBuf> = Vec::new();
        let mut both = |stem: String, tail: Option<&str>| {
            for s in [stem.to_ascii_lowercase(), stem.to_ascii_uppercase()] {
                names.push(match tail {
                    Some(t) => self.dir.join(&s).join(t),
                    None => self.dir.join(format!("{s}.bin")),
                });
            }
        };
        if is_hex(&id.disc_hash, 40) {
            both(id.disc_hash.clone(), None);
        }
        if let Some(vid) = id.volume_id {
            both(hex(&vid), Some("convtab.bin"));
        }
        if let Some(mk) = id.media_key {
            both(hex(&mk), None);
        }
        names.dedup();
        names
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

impl ConversionTableSource for TableDir {
    fn name(&self) -> &str {
        "dir"
    }

    fn lookup(&self, id: &BdPlusDiscId) -> Result<Option<ConversionTable>> {
        for path in self.candidates(id) {
            let meta = match std::fs::metadata(&path) {
                Ok(m) if m.is_file() => m,
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::IoError { source: e }),
            };
            if meta.len() > MAX_TABLE_BYTES as u64 {
                return Err(Error::BdPlusTableInvalid { kind: "too_large" });
            }
            let bytes = std::fs::read(&path).map_err(|source| Error::IoError { source })?;
            tracing::info!(target: "freemkv::bdplus", path = %path.display(), "conversion table found");
            return ConversionTable::parse_activated(&bytes).map(Some);
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;

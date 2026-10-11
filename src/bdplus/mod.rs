//! BD+ (Self-Protecting Digital Content): detect it, resolve a conversion table, patch titles.
//!
//! A BD+ disc carries `BDSVM/00000.svm`. Its clips were mastered with small regions
//! overwritten; a player puts the original bytes back from a conversion table, after AACS
//! decryption, by byte offset within each `.m2ts`. Without the table the stream decodes
//! with glitches at those spots.
//!
//! The scan records what is on the disc ([`BdPlusInfo`] on `Disc::bdplus`). The caller
//! resolves a table up front ([`BdPlusResolution::resolve`] over [`ConversionTableSource`]s)
//! and attaches it with `Source::with_bdplus`. Title reads (every mux sink) are patched;
//! whole-disc images (`iso://` copy, `dir://` extract) never are: an image stays a faithful
//! copy, and is patched when a title is muxed from it. With no table the mux passes the
//! bytes through and emits `Event::BdPlusUnresolved`.

mod fixup;
mod header;
mod source;
mod table;

pub use fixup::apply_patches;
pub(crate) use fixup::{ClipFile, DiscFixups};
pub use header::{SVM_HEADER_LEN, SvmHeader, estimated_generation};
pub use source::{BdPlusDiscId, ConversionTableSource, NoTables, TableDir};
pub use table::{
    ClipTable, ConversionTable, Entry, EntryKind, MAX_TABLE_BYTES, PATCH_LEN, Patch, PatchSet,
    Segment,
};

use crate::disc::Disc;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::sync::Arc;

/// One `.svm` file under `BDSVM/`: its path from the disc root and its size in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SvmFile {
    pub path: String,
    pub size: u64,
}

/// What a scan found of BD+ on a disc: facts read from it, nothing derived but
/// [`Self::estimated_generation`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BdPlusInfo {
    /// The header of `BDSVM/00000.svm`; `None` when it could not be read.
    pub header: Option<SvmHeader>,
    /// Every `.svm` file in `BDSVM/` and `BDSVM/BACKUP/`, sorted by path.
    pub files: Vec<SvmFile>,
    // The stream files' disc extents, to place a table's patches.
    pub(crate) clips: Vec<ClipFile>,
}

impl BdPlusInfo {
    /// Detection facts with no clip layout (a resolution over it patches nothing).
    pub fn new(header: Option<SvmHeader>, files: Vec<SvmFile>) -> Self {
        BdPlusInfo {
            header,
            files,
            clips: Vec::new(),
        }
    }

    /// The header date `(year, month, day)`.
    pub fn date(&self) -> Option<(u16, u8, u8)> {
        self.header.map(|h| (h.year, h.month, h.day))
    }

    /// The generation the header date suggests (see [`estimated_generation`]): a
    /// heuristic for display and reports only.
    pub fn estimated_generation(&self) -> Option<u8> {
        self.header.map(|h| h.estimated_generation())
    }

    /// `BDSVM/BACKUP/` holds `.svm` files.
    pub fn has_backup(&self) -> bool {
        self.files.iter().any(|f| f.path.contains("/BACKUP/"))
    }

    /// Content-code files beyond `00000`-`00002` in `BDSVM/` (later discs add `00003.svm`
    /// and up).
    pub fn has_extra_svm(&self) -> bool {
        self.files
            .iter()
            .any(|f| !f.path.contains("/BACKUP/") && svm_number(&f.path).is_some_and(|n| n >= 3))
    }
}

fn svm_number(path: &str) -> Option<u32> {
    let name = path.rsplit('/').next()?;
    let stem = name.get(..name.len().checked_sub(4)?)?;
    name[stem.len()..]
        .eq_ignore_ascii_case(".svm")
        .then(|| stem.parse().ok())
        .flatten()
}

// `.svm` files directly in `dir` (an absolute UDF path), as `(root-relative path, size)`.
fn svm_files(udf: &UdfFs, dir: &str) -> Vec<SvmFile> {
    udf.find_dir(dir)
        .map(|d| {
            d.entries
                .iter()
                .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".svm"))
                .map(|e| SvmFile {
                    path: format!("{}/{}", dir.trim_start_matches('/'), e.name),
                    size: e.size,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The disc's BD+ facts, `None` when `BDSVM/00000.svm` is absent (the presence rule
/// players use). Reads one sector of `00000.svm` and the stream files' File Entries;
/// only [`Error::Halted`] fails it, anything else unreadable is left out and logged.
pub(crate) fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> Result<Option<BdPlusInfo>> {
    let mut files = svm_files(udf, "/BDSVM");
    let Some(first) = files
        .iter()
        .find(|f| svm_number(&f.path) == Some(0))
        .map(|f| format!("/{}", f.path))
    else {
        return Ok(None);
    };
    files.extend(svm_files(udf, "/BDSVM/BACKUP"));
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let header = match udf.read_file_prefix(reader, &first, SVM_HEADER_LEN) {
        Ok(bytes) => SvmHeader::parse(&bytes),
        Err(Error::Halted) => return Err(Error::Halted),
        Err(e) => {
            tracing::warn!(target: "freemkv::bdplus", code = e.code(), "BD+ header unreadable");
            None
        }
    };
    let clips = clip_files(reader, udf)?;
    let info = BdPlusInfo {
        header,
        files,
        clips,
    };
    tracing::info!(
        target: "freemkv::bdplus",
        date = ?info.date(),
        estimated_generation = ?info.estimated_generation(),
        files = info.files.len(),
        "BD+ detected"
    );
    Ok(Some(info))
}

// Each `BDMV/STREAM/NNNNN.m2ts`'s extents. A file with an unrecorded extent is left out.
fn clip_files(reader: &mut dyn SectorSource, udf: &UdfFs) -> Result<Vec<ClipFile>> {
    let Some(dir) = udf.find_dir("/BDMV/STREAM") else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for e in dir.entries.iter().filter(|e| !e.is_dir) {
        let name = e.name.to_ascii_lowercase();
        let Some(clip) = name
            .strip_suffix(".m2ts")
            .filter(|s| s.len() == 5)
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        match udf.extents_abs_at(reader, e.meta_lba) {
            Ok(ext) if ext.iter().all(|x| x.recorded || x.len == 0) => {
                let mut extents: Vec<(u32, u64)> = ext
                    .iter()
                    .filter(|x| x.len > 0)
                    .map(|x| (x.lba, u64::from(x.len)))
                    .collect();
                trim_to(&mut extents, e.size);
                out.push(ClipFile { clip, extents });
            }
            Ok(_) => {
                tracing::warn!(target: "freemkv::bdplus", clip, "clip has an unrecorded extent")
            }
            Err(Error::Halted) => return Err(Error::Halted),
            Err(err) => {
                tracing::warn!(target: "freemkv::bdplus", clip, code = err.code(), "clip extents unreadable")
            }
        }
    }
    out.sort_by_key(|c| c.clip);
    Ok(out)
}

// Cut `extents` to the file's declared `size` bytes.
fn trim_to(extents: &mut Vec<(u32, u64)>, size: u64) {
    let mut left = size;
    extents.retain_mut(|e| {
        e.1 = e.1.min(left);
        left -= e.1;
        e.1 > 0
    });
}

/// Whether a BD+ disc's conversion table resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BdPlusStatus {
    /// No source had a table: titles pass through unpatched and may show glitches.
    Unresolved,
    /// Source `source` supplied a table: `entries` entries over `clips` clips of this disc
    /// will be applied.
    Resolved {
        source: String,
        clips: usize,
        entries: usize,
    },
}

/// A BD+ disc's table resolved (or not) up front, for `Source::with_bdplus`.
///
/// Cheap to clone; clones share [`Self::patches_applied`].
#[derive(Debug, Clone)]
pub struct BdPlusResolution(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    info: BdPlusInfo,
    disc_hash: String,
    capacity: u32,
    status: BdPlusStatus,
    fixups: Option<Arc<DiscFixups>>,
}

impl BdPlusResolution {
    /// Ask `sources` in order for `disc`'s table. `None` when the disc is not BD+. A source
    /// error is logged and the next source asked; with no table the result is
    /// [`BdPlusStatus::Unresolved`].
    pub fn resolve(disc: &Disc, sources: &[&dyn ConversionTableSource]) -> Option<Self> {
        Self::resolve_id(disc, &BdPlusDiscId::of(disc), sources)
    }

    /// [`Self::resolve`] keyed by `id` (e.g. [`BdPlusDiscId::with_media_key`]).
    pub fn resolve_id(
        disc: &Disc,
        id: &BdPlusDiscId,
        sources: &[&dyn ConversionTableSource],
    ) -> Option<Self> {
        let info = disc.bdplus.as_ref()?;
        for s in sources {
            match s.lookup(id) {
                Ok(Some(table)) => return Some(Self::build(disc, info, s.name(), &table)),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(target: "freemkv::bdplus", source = s.name(), code = e.code(), "conversion table source failed")
                }
            }
        }
        Some(Self::unresolved(disc, info))
    }

    /// `disc`'s resolution from a table already in hand, credited to `source`. `None`
    /// when the disc is not BD+.
    pub fn from_table(disc: &Disc, source: &str, table: &ConversionTable) -> Option<Self> {
        disc.bdplus
            .as_ref()
            .map(|info| Self::build(disc, info, source, table))
    }

    pub(crate) fn unresolved(disc: &Disc, info: &BdPlusInfo) -> Self {
        BdPlusResolution(Arc::new(Inner {
            info: info.clone(),
            disc_hash: BdPlusDiscId::of(disc).disc_hash,
            capacity: disc.capacity_sectors,
            status: BdPlusStatus::Unresolved,
            fixups: None,
        }))
    }

    fn build(disc: &Disc, info: &BdPlusInfo, source: &str, table: &ConversionTable) -> Self {
        let mut placed: Vec<(&ClipFile, Vec<Patch>)> = Vec::new();
        let (mut entries, mut rejected, mut encrypted) = (0, 0, 0);
        for t in &table.clips {
            let set = t.patches();
            rejected += set.rejected;
            encrypted += set.encrypted;
            match info.clips.iter().find(|c| c.clip == t.clip) {
                Some(file) if !set.patches.is_empty() => {
                    entries += set.entries;
                    placed.push((file, set.patches));
                }
                Some(_) => {}
                None => {
                    tracing::warn!(target: "freemkv::bdplus", clip = t.clip, "table names a clip the disc lacks")
                }
            }
        }
        let (fixups, past_end) = DiscFixups::build(placed.iter().map(|(f, p)| (*f, p.as_slice())));
        tracing::info!(
            target: "freemkv::bdplus",
            source, clips = placed.len(), entries, rejected, encrypted, past_end,
            "BD+ conversion table resolved"
        );
        BdPlusResolution(Arc::new(Inner {
            info: info.clone(),
            disc_hash: BdPlusDiscId::of(disc).disc_hash,
            capacity: disc.capacity_sectors,
            status: BdPlusStatus::Resolved {
                source: source.to_string(),
                clips: placed.len(),
                entries,
            },
            fixups: Some(Arc::new(fixups)),
        }))
    }

    /// Resolved or not.
    pub fn status(&self) -> &BdPlusStatus {
        &self.0.status
    }

    /// The disc's BD+ facts.
    pub fn info(&self) -> &BdPlusInfo {
        &self.0.info
    }

    /// Patches written into title reads so far, counted once each when their last byte
    /// lands (a re-read counts again). 0 when unresolved.
    pub fn patches_applied(&self) -> u64 {
        self.0.fixups.as_ref().map_or(0, |f| f.applied())
    }

    /// Built for `disc`: the same AACS disc hash and capacity (0 is not compared).
    pub fn is_for(&self, disc: &Disc) -> bool {
        let (a, b) = (self.0.capacity, disc.capacity_sectors);
        BdPlusDiscId::of(disc).disc_hash == self.0.disc_hash && (a == 0 || b == 0 || a == b)
    }

    pub(crate) fn fixups(&self) -> Option<Arc<DiscFixups>> {
        self.0.fixups.clone()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

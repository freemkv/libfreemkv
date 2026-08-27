//! `dir://` as an image-level SOURCE: a synthetic UDF volume over a folder.
//!
//! A user's extracted disc — a DVD `VIDEO_TS/` or a Blu-ray `BDMV/`, typically
//! a MakeMKV-style backup — has files but no sectors, and everything above the
//! sector layer in this crate wants sectors: `Disc::scan_image`, `UdfFs`,
//! `ifo.rs`, `mpls.rs`, `clpi.rs` and the mux all read through a
//! [`SectorSource`]. [`DirImage`] supplies one.
//!
//! The trick is that nothing is emulated. A real, minimal, valid UDF 1.02
//! volume is synthesized over the folder:
//!
//! * **Metadata sectors** (anchors, the volume descriptor sequences, the File
//!   Set Descriptor, every File Entry, every directory's FID list) are encoded
//!   into RAM by [`encode`] — a few MiB even for a large Blu-ray.
//! * **Data sectors** are not materialized at all. Each one maps to a byte
//!   range of a real file, read on demand.
//!
//! So `udf::read_filesystem` parses this image by exactly the same code path it
//! parses a real disc with, and every consumer above it is unchanged. The cost
//! is that a single-partition synthetic volume never exercises the UDF 2.50
//! Metadata Partition path (`udf.rs:946-991`) that every real BD-ROM uses —
//! this module's tests do not cover that block and must not be read as if they
//! did.
//!
//! What this module deliberately does NOT do:
//!
//! * **3D / SSIF** — rejected up front ([`Error::DirImageSsifUnsupported`]).
//!   An SSIF aliases the same sectors as its base and dependent `.m2ts`; the
//!   planner allocates disjoint extents, so a 3D folder would produce silently
//!   wrong output.
//! * **HD-DVD `HVDVD_TS/`** — no title enumerator constraint is modelled.
//! * **Encrypted folders** — a folder whose content is still AACS-scrambled is
//!   rejected by the caller-side probe, not decrypted here.

mod encode;
mod layout;

use crate::error::{Error, Result};
#[cfg(target_os = "linux")]
use crate::io::file_sector_source::linux::drop_window;
#[cfg(target_os = "macos")]
use crate::io::file_sector_source::macos::drop_window;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use crate::io::file_sector_source::other::drop_window;
#[cfg(target_os = "windows")]
use crate::io::file_sector_source::windows::drop_window;
use crate::sector::SectorSource;
use encode::{MetaSectors, SECTOR};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// How many host files may be held open at once.
///
/// A Blu-ray `BDMV/` can exceed a thousand files while macOS `RLIMIT_NOFILE`
/// defaults to 256, so "open every file up front" is not available. Reads are
/// overwhelmingly sequential through one large stream file at a time, so a
/// small LRU keeps the hit rate near 1 while bounding descriptors.
const HANDLE_CACHE: usize = 16;

/// One file's bytes at one place in the image.
#[derive(Debug, Clone)]
struct DataRange {
    /// Absolute first block.
    start_lba: u32,
    /// Blocks covered (the last one may be partially used, and is zero-padded).
    sectors: u32,
    /// Index into [`DirImage::files`].
    file: usize,
    /// Byte offset within the file at which this range's bytes begin.
    offset: u64,
    /// Byte length of the range.
    bytes: u64,
}

/// A file the image reads through.
#[derive(Debug)]
struct FileRef {
    host: PathBuf,
    disc_path: String,
    size: u64,
    /// Host mtime at plan time — see `layout::FileNode::mtime` for why size
    /// alone is not enough.
    mtime: Option<std::time::SystemTime>,
}

/// A synthesized UDF disc image over a host directory.
///
/// Owns everything it reads through (`PathBuf`s and its own file handles), so
/// it is `Send + 'static` and can be moved into `build_iso_pipeline`, which
/// hands it to `PrefetchedSectorSource`'s producer thread.
pub struct DirImage {
    meta: MetaSectors,
    /// Sorted by `start_lba`, non-overlapping.
    ranges: Vec<DataRange>,
    files: Vec<FileRef>,
    open: Vec<(usize, File)>,
    total_sectors: u32,
    volume_id: String,
    data_bytes: u64,
}

impl std::fmt::Debug for DirImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirImage")
            .field("volume_id", &self.volume_id)
            .field("total_sectors", &self.total_sectors)
            .field("files", &self.files.len())
            .field("meta_sectors", &self.meta.len())
            .finish()
    }
}

impl DirImage {
    /// Plan and encode an image over `root`.
    ///
    /// Every error is decided here, at plan time, where it can name the file
    /// responsible — the read path is deliberately left with nothing to decide
    /// except "this file changed underneath me".
    pub fn open(root: &Path) -> Result<Self> {
        let plan = layout::plan(root)?;
        let meta = encode::encode(&plan)?;

        let mut nodes = Vec::new();
        layout::flatten(&plan.root, &mut nodes);

        let mut files = Vec::with_capacity(nodes.len());
        let mut ranges = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            // Carry plan-time mtime ONLY for files whose CONTENT the plan read (DVD
            // IFOs, whose 0xC0/0xC4 bytes place every VOB); others depend only on
            // SIZE, and comparing mtime there false-positives on exFAT/FAT32 + DST.
            let content_sensitive = node
                .disc_path
                .rsplit('.')
                .next()
                .is_some_and(|e| e.eq_ignore_ascii_case("IFO"));
            files.push(FileRef {
                host: node.host.clone(),
                disc_path: node.disc_path.clone(),
                size: node.size,
                mtime: content_sensitive.then_some(node.mtime).flatten(),
            });
            let mut offset = 0u64;
            for e in &node.extents {
                ranges.push(DataRange {
                    start_lba: plan.part_start + e.lba,
                    sectors: (e.bytes as u64).div_ceil(SECTOR as u64) as u32,
                    file: idx,
                    offset,
                    bytes: e.bytes as u64,
                });
                offset += e.bytes as u64;
            }
        }
        ranges.sort_by_key(|r| r.start_lba);
        debug_assert!(
            ranges
                .windows(2)
                .all(|w| w[0].start_lba + w[0].sectors <= w[1].start_lba),
            "planned data ranges must not overlap"
        );

        let data_bytes = layout::total_data_bytes(&plan.root);
        tracing::info!(
            target: "freemkv::dirimage",
            volume_id = %plan.volume_id,
            files = files.len(),
            dirs = plan.dir_count,
            meta_blocks = layout::metadata_block_count(&plan.root),
            total_sectors = plan.total_sectors,
            "synthesized UDF image over directory"
        );

        Ok(Self {
            meta,
            ranges,
            files,
            open: Vec::new(),
            total_sectors: plan.total_sectors,
            volume_id: plan.volume_id,
            data_bytes,
        })
    }

    /// UDF volume identifier the image declares (the folder's own name).
    pub fn volume_id(&self) -> &str {
        &self.volume_id
    }

    /// Total bytes of real file content the image carries — the folder's size,
    /// not the image's (which also counts metadata and inter-file gaps).
    pub fn data_bytes(&self) -> u64 {
        self.data_bytes
    }

    /// The range covering `lba`, if any.
    fn range_at(&self, lba: u32) -> Option<&DataRange> {
        let i = self.ranges.partition_point(|r| r.start_lba <= lba);
        let r = self.ranges.get(i.checked_sub(1)?)?;
        (lba < r.start_lba + r.sectors).then_some(r)
    }

    /// Borrow an open handle for `file`, opening it (and evicting the
    /// least-recently-used handle) if necessary.
    ///
    /// Opening is also where the plan is revalidated. A folder is not a disc:
    /// a file can be shortened or replaced between planning and reading, and
    /// zero-filling the difference would turn "the user deleted something"
    /// into corrupt output at exit 0. The size is re-checked here, and a
    /// truncation that happens while the handle is already open is caught by
    /// the short read in [`Self::fill`].
    fn handle(&mut self, file: usize) -> Result<&mut File> {
        if let Some(pos) = self.open.iter().position(|(i, _)| *i == file) {
            // `open` is ordered most-recently-used first.
            let entry = self.open.remove(pos);
            self.open.insert(0, entry);
            return Ok(&mut self.open[0].1);
        }
        let f = File::open(&self.files[file].host).map_err(Error::from)?;
        let md = f.metadata().map_err(Error::from)?;
        // Size AND mtime: size alone is content-blind, and VOB placement depends on
        // IFO bytes 0xC0/0xC4 — an IFO rewritten in place keeps its sector-aligned
        // length, so size alone would miss it. mtime only compared if both present.
        let changed_size = md.len() != self.files[file].size;
        let changed_mtime = match (self.files[file].mtime, md.modified().ok()) {
            (Some(planned), Some(live)) => planned != live,
            _ => false,
        };
        if changed_size || changed_mtime {
            return Err(Error::DirImageFileChanged {
                path: self.files[file].disc_path.clone(),
            });
        }
        if self.open.len() >= HANDLE_CACHE {
            self.open.pop();
        }
        self.open.insert(0, (file, f));
        Ok(&mut self.open[0].1)
    }

    /// Fill `out` (a whole number of sectors) from one data range, starting at
    /// `lba`. `out` is already zeroed, so a file's tail sector comes back
    /// zero-padded — which is exactly what `file_extents`' `div_ceil(2048)`
    /// (`udf.rs:816`) makes every consumer expect.
    fn fill(&mut self, r: &DataRange, lba: u32, out: &mut [u8]) -> Result<()> {
        let within = (lba - r.start_lba) as u64 * SECTOR as u64;
        let want = (r.bytes.saturating_sub(within)).min(out.len() as u64) as usize;
        if want == 0 {
            return Ok(());
        }
        let at = r.offset + within;
        let file = r.file;
        let h = self.handle(file)?;
        h.seek(SeekFrom::Start(at)).map_err(Error::from)?;
        let res = h.read_exact(&mut out[..want]);
        if res.is_ok() {
            // Release the window just read, every time (unlike the ISO source's
            // accumulate-and-drop, which relies on one linear cursor — reads here
            // jump between files, so an accumulated count would leave bytes pinned).
            if let Some((_, fh)) = self.open.iter().find(|(i, _)| *i == file) {
                drop_window(fh, at, want as u64);
            }
        }
        match res {
            Ok(()) => Ok(()),
            // The file shrank while the handle was open. Same verdict as the
            // size check in `handle`, reached the other way.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(Error::DirImageFileChanged {
                    path: self.files[file].disc_path.clone(),
                })
            }
            Err(e) => Err(Error::from(e)),
        }
    }
}

impl SectorSource for DirImage {
    fn capacity_sectors(&self) -> u32 {
        self.total_sectors
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let need = count as usize * SECTOR;
        if buf.len() < need {
            return Err(Error::UdfBufferTooSmall);
        }
        buf[..need].fill(0);
        // Walk the request in RUNS, not sector by sector: a mux batch (8192
        // sectors) almost always lands in one extent, so per-sector seek+read
        // would cost 8192 syscalls for what is one 16 MiB sequential read.
        let mut i = 0u32;
        while i < count as u32 {
            // Checked: callers saturate their LBAs, so a crafted IFO can present a
            // request at the top of the address space. Wrapping here would fold
            // `at` to a LOW sector, handing the muxer a different file's bytes.
            let Some(at) = lba.checked_add(i) else {
                break;
            };
            let off = i as usize * SECTOR;
            if let Some(s) = self.meta.get(&at) {
                buf[off..off + SECTOR].copy_from_slice(&s[..]);
                i += 1;
                continue;
            }
            // Metadata blocks all sit below the data floor, so a data range is
            // never interrupted by one.
            match self.range_at(at).cloned() {
                Some(r) => {
                    let run = (r.start_lba + r.sectors - at).min(count as u32 - i);
                    let end = off + run as usize * SECTOR;
                    self.fill(&r, at, &mut buf[off..end])?;
                    i += run;
                }
                // A gap between planned extents. Reads as zeros, exactly as an
                // unrecorded sector of a real image does.
                None => i += 1,
            }
        }
        Ok(need)
    }
}

#[cfg(test)]
mod tests;

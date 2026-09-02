//! Layout planner: a host directory tree in, a full block assignment out.
//!
//! Two phases, kept apart because they fail for different reasons:
//!
//! 1. **Walk** the folder into a tree of names and sizes. Rejects what the
//!    image model cannot represent (3D SSIF, an unrecognized tree, a
//!    case-collision inside one directory).
//! 2. **Assign** blocks. Metadata first, then file data — see
//!    [`place_video_ts`] for the DVD placement constraint.
//!
//! See `docs/dirimage.md` for the full rationale (Blu-ray vs. DVD placement).

use super::encode::{ANCHOR_LBA, MIN_PART_START, SECTOR};
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

// First block any FILE DATA may occupy, absolute. Far from zero: bluray.rs
// drops any clip extent whose LBA is 0. See docs/dirimage.md — DATA_FLOOR.
const DATA_FLOOR: u32 = 4096;

// Largest byte length a single AD may record: 30-bit length field, rounded
// down to a whole number of blocks. See docs/dirimage.md — MAX_AD_BYTES.
pub(super) const MAX_AD_BYTES: u64 = 0x3FFF_F800;

// Deepest directory nesting represented. Matches `udf.rs`'s `MAX_DIR_DEPTH`;
// anything deeper is recorded but never descended into. Refuse rather than
// silently drop the files under it.
const MAX_DEPTH: u32 = 8;

/// Upper bound on entries in the synthesized tree, mirroring `udf.rs`'s
/// `MAX_TOTAL_DIR_ENTRIES`.
const MAX_ENTRIES: usize = 100_000;

// Longest OSTA CS0 encoding a FID can describe. Name-length field is one
// byte (255 ceiling); 254 leaves no way to produce a value that wraps to 0.
const MAX_CS0_NAME_BYTES: usize = 254;

// Most subdirectories one directory may hold: link count is u16 (children +
// self), so `u16::MAX - 1` is the last usable value. Lowered under
// cfg(test) only so the guard is actually exercisable. See docs/dirimage.md.
#[cfg(not(test))]
const MAX_SUBDIRS: usize = (u16::MAX - 1) as usize;
#[cfg(test)]
const MAX_SUBDIRS: usize = 4;

// One entry per child plus the parent's own must fit the 16-bit field. Must
// stay at MODULE scope: it previously lived inside `#[cfg(test)] mod tests`
// with its own `#[cfg(not(test))]`, so it never compiled at all. See docs/dirimage.md.
#[cfg(not(test))]
const _: () = assert!(MAX_SUBDIRS + 1 == u16::MAX as usize);

// Largest image this planner will synthesize, in sectors (128 GiB). A
// regenerated/hand-assembled folder can name a DVD VOB offset far beyond the
// content; without a ceiling the image grows to wherever it points. See docs/dirimage.md.
const MAX_IMAGE_SECTORS: u32 = (128u64 * 1024 * 1024 * 1024 / SECTOR as u64) as u32;

// Ceiling on the in-memory metadata region (64 MiB): the entry cap alone
// permits ~205 MB, and the mux holds two of these at once while probing.
// See docs/dirimage.md — MAX_META_BYTES.
const MAX_META_BYTES: u64 = 64 * 1024 * 1024;

/// The fan-out cap must bite before the global entry cap, or it never fires.
const _: () = assert!(MAX_SUBDIRS < MAX_ENTRIES);

/// One contiguous run of a file's bytes at a partition-relative block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Extent {
    pub(super) lba: u32,
    pub(super) bytes: u32,
}

/// A planned file: where its bytes come from on the host, and where they live
/// in the synthesized image.
#[derive(Debug)]
pub(super) struct FileNode {
    pub(super) name: String,
    /// Disc path (`/BDMV/STREAM/00000.m2ts`) — used only for error reporting.
    pub(super) disc_path: String,
    pub(super) host: PathBuf,
    pub(super) size: u64,
    /// Host mtime at PLAN time, when the platform reports one.
    ///
    /// Size alone is content-blind, and the plan depends on CONTENT: a DVD's
    /// VOB placement is derived from bytes 0xC0/0xC4 of its IFO (`read_head`).
    /// A re-authoring tool rewriting an IFO in place keeps the length — IFOs
    /// are a whole number of sectors — so a size check passes while the
    /// placement the image was built around is stale, and every cell of the
    /// title then resolves to the wrong sectors at exit 0.
    pub(super) mtime: Option<std::time::SystemTime>,
    pub(super) icb_lba: u32,
    pub(super) unique_id: u64,
    pub(super) extents: Vec<Extent>,
}

/// A planned directory.
#[derive(Debug)]
pub(super) struct DirNode {
    pub(super) name: String,
    pub(super) icb_lba: u32,
    pub(super) parent_icb_lba: u32,
    pub(super) data_lba: u32,
    pub(super) data_bytes: u32,
    pub(super) unique_id: u64,
    pub(super) dirs: Vec<DirNode>,
    pub(super) files: Vec<FileNode>,
}

/// A complete image plan.
#[derive(Debug)]
pub(super) struct Layout {
    pub(super) part_start: u32,
    pub(super) part_sectors: u32,
    pub(super) total_sectors: u32,
    pub(super) volume_id: String,
    pub(super) file_count: u32,
    pub(super) dir_count: u32,
    pub(super) next_unique_id: u64,
    pub(super) root: DirNode,
}

/// Serialized length of one File Identifier Descriptor, before its 4-byte
/// alignment padding. Shared with the encoder so the planner's directory size
/// and the encoder's output cannot drift apart.
fn fid_len(name: &str, is_parent: bool) -> usize {
    let l_fi = if is_parent {
        0
    } else {
        crate::dirimage::encode::encode_cs0(name).len()
    };
    (38 + l_fi).div_ceil(4) * 4
}

/// Total FID bytes a directory's data extent must hold.
fn dir_bytes(dirs: &[DirNode], files: &[FileNode]) -> usize {
    let mut n = fid_len("", true);
    for d in dirs {
        n += fid_len(&d.name, false);
    }
    for f in files {
        n += fid_len(&f.name, false);
    }
    n
}

// ── Phase 1: walk ───────────────────────────────────────────────────────────

// Whether a host directory entry belongs in the synthesized image (Finder's
// `.DS_Store`/`._*`, or freemkv's own `.partial`). See docs/dirimage.md.
fn is_excluded(name: &str) -> bool {
    name.starts_with('.') || name.ends_with(".partial")
}

fn walk(dir: &Path, disc_path: &str, depth: u32, entries: &mut usize) -> Result<DirNode> {
    if depth > MAX_DEPTH {
        return Err(Error::DirImageTooLarge);
    }
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    let mut names: Vec<String> = Vec::new();

    let mut read: Vec<_> = std::fs::read_dir(dir)
        .map_err(Error::from)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::from)?;
    // Deterministic order: the same folder must always produce the same image
    // (readdir order is filesystem- and even mount-dependent).
    read.sort_by_key(|e| e.file_name());

    for entry in read {
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_excluded(&name) {
            continue;
        }
        // `file_type()` does NOT follow symlinks; `metadata()` does. A symlink to
        // a file is materialized as that file; a symlink to a directory is
        // skipped — following one can loop, and UDF has no link this maps onto.
        let ft = entry.file_type().map_err(Error::from)?;
        let child_path = format!("{}/{}", disc_path.trim_end_matches('/'), name);
        // A FID records the encoded name length in ONE byte; a longer name would
        // wrap it and desynchronise the directory, so refuse while planning. 255
        // ASCII bytes (legal on ext4/APFS/NTFS) already exceeds it with CS0 added.
        if crate::dirimage::encode::encode_cs0(&name).len() > MAX_CS0_NAME_BYTES {
            return Err(Error::DirNameTooLong { path: child_path });
        }
        *entries += 1;
        if *entries > MAX_ENTRIES {
            return Err(Error::DirImageTooLarge);
        }
        // Key uniqueness on the name AS THE READER WILL SEE IT (round-trip through
        // the same encoder/parser it uses): `parse_udf_name` trims/drops chars, so
        // distinct hosts (" A.M2TS" vs "A.M2TS") can collapse and `find` mismatches.
        let as_read = crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0(&name));
        // Nothing left after the reader drops it (e.g. a name of a single emoji):
        // it would exist in the image but be addressable by nothing. SKIP rather
        // than fail the plan (1.6.0 handled such extras fine); not pushed to `names`.
        if as_read.is_empty() {
            tracing::warn!(
                target: "freemkv::dirimage",
                path = %child_path,
                "name is unrepresentable in UDF after encoding; entry omitted from the image"
            );
            continue;
        }
        names.push(as_read.to_ascii_uppercase());
        if ft.is_dir() {
            // Link count (child dirs + 1) is stored in 16 bits; the global entry
            // cap alone permits a single directory holding more than that, which
            // would wrap the count.
            if dirs.len() >= MAX_SUBDIRS {
                return Err(Error::DirImageFanout {
                    path: disc_path.to_string(),
                });
            }
            dirs.push(walk(&entry.path(), &child_path, depth + 1, entries)?);
        } else {
            let meta = match std::fs::metadata(entry.path()) {
                Ok(m) => m,
                // A broken symlink or a file gone between readdir and stat: skip it
                // rather than plan an unreadable extent. Anything else (e.g. perm
                // denied) is NOT a missing file, so skipping it would drop it silently.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };
            if !meta.is_file() {
                // Not a plain file after following the link: commonly a SYMLINK TO A
                // DIRECTORY, silently vanishing before (clips → no extents, near-empty
                // MKV at exit 0). Still skipped, not followed, but now SAID.
                tracing::warn!(
                    target: "freemkv::dirimage",
                    path = %child_path,
                    kind = if meta.is_dir() { "directory link" } else { "special file" },
                    "entry is not a plain file; omitted from the image"
                );
                continue;
            }
            files.push(FileNode {
                name,
                disc_path: child_path,
                host: entry.path(),
                size: meta.len(),
                mtime: meta.modified().ok(),
                icb_lba: 0,
                unique_id: 0,
                extents: Vec::new(),
            });
        }
    }

    // `find_dir`/`read_file` match path components case-insensitively, so two
    // entries differing only in case are indistinguishable to every consumer —
    // the second would silently shadow the first. Only on a case-sensitive host.
    names.sort();
    for pair in names.windows(2) {
        if pair[0] == pair[1] {
            return Err(Error::DirNameCollision {
                host: format!("{disc_path}/{}", pair[0]),
            });
        }
    }

    Ok(DirNode {
        name: dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        icb_lba: 0,
        parent_icb_lba: 0,
        data_lba: 0,
        data_bytes: 0,
        unique_id: 0,
        dirs,
        files,
    })
}

/// Find a child directory by ASCII-case-insensitive name.
fn child_dir<'a>(dir: &'a DirNode, name: &str) -> Option<&'a DirNode> {
    dir.dirs.iter().find(|d| d.name.eq_ignore_ascii_case(name))
}

// ── Phase 2: block assignment ───────────────────────────────────────────────

/// Assign metadata blocks depth-first: this node's File Entry, then its
/// children's, then the directory data extents. Returns the next free block.
fn assign_metadata(dir: &mut DirNode, parent_icb: u32, next: &mut u32, uid: &mut u64) {
    dir.icb_lba = *next;
    *next += 1;
    dir.parent_icb_lba = parent_icb;
    dir.unique_id = *uid;
    *uid += 1;
    for f in &mut dir.files {
        f.icb_lba = *next;
        *next += 1;
        f.unique_id = *uid;
        *uid += 1;
    }
    let icb = dir.icb_lba;
    for sub in &mut dir.dirs {
        assign_metadata(sub, icb, next, uid);
    }
    // Directory data after every File Entry of this subtree, so a directory's
    // FIDs can name ICBs that were assigned after it.
    let bytes = dir_bytes(&dir.dirs, &dir.files);
    dir.data_bytes = bytes as u32;
    dir.data_lba = *next;
    *next += bytes.div_ceil(SECTOR) as u32;
}

/// Split a file into allocation descriptors and place them starting at `lba`.
/// Returns the first free block after the file.
fn place_file(f: &mut FileNode, lba: u32) -> Result<u32> {
    f.extents.clear();
    if f.size == 0 {
        return Ok(lba);
    }
    let mut remaining = f.size;
    let mut cur = lba;
    while remaining > 0 {
        let chunk = remaining.min(MAX_AD_BYTES);
        f.extents.push(Extent {
            lba: cur,
            bytes: chunk as u32,
        });
        let blocks = chunk.div_ceil(SECTOR as u64);
        cur = u32::try_from(cur as u64 + blocks).map_err(|_| Error::DirImageTooLarge)?;
        remaining -= chunk;
    }
    Ok(cur)
}

/// Big-endian u32 at `off`, as every DVD-Video structure field is stored.
fn be_u32(buf: &[u8], off: usize) -> Option<u32> {
    let b = buf.get(off..off + 4)?;
    Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

// Read the first `n` bytes of an IFO so its placement offsets can be
// resolved. Errors propagate — an empty buffer would record NO placement
// constraint and the rip would read the wrong sectors. See docs/dirimage.md.
fn read_head(path: &Path, n: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut buf = vec![0u8; n];
    // `read_exact`, not `read`: a single `read` may legally return fewer bytes on
    // a network/FUSE mount (a NAS-hosted backup is normal here). A short buffer
    // records NO constraint, so the rip reads the wrong sectors at exit 0.
    let mut f = std::fs::File::open(path).map_err(Error::from)?;
    f.read_exact(&mut buf).map_err(Error::from)?;
    Ok(buf)
}

// Placement order and constraints for a `VIDEO_TS` folder. DVD-Video records
// VOB positions INSIDE the IFOs, as offsets from the IFO's own first sector;
// placement must reproduce them exactly or fail. See docs/dirimage.md.
fn place_video_ts(vts: &mut DirNode, start: u32) -> Result<u32> {
    let mut order: Vec<usize> = (0..vts.files.len()).collect();
    // Canonical on-disc order. Files the naming scheme does not cover (stray
    // extras) sort last and are placed unconstrained.
    order.sort_by_key(|&i| {
        let r = classify(&vts.files[i].name);
        (
            r.is_none(),
            r.map(|c| (c.group, c.order))
                .unwrap_or((u32::MAX, u32::MAX)),
            i,
        )
    });

    // Required start blocks, resolved as each IFO is placed.
    let mut menu_req: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    let mut title_req: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    // Groups whose IFO has already been placed, kept separately from the
    // constraint maps: an IFO declaring no offsets inserts into neither, so
    // keying the duplicate check on those maps would miss the collision.
    let mut seen_ifo: std::collections::HashSet<u32> = std::collections::HashSet::new();

    let mut cursor = start;
    for &i in &order {
        let class = classify(&vts.files[i].name);
        let required = match class.map(|c| (c.group, c.role)) {
            Some((g, Role::MenuVob)) => menu_req.get(&g).copied(),
            Some((g, Role::TitleVob)) => title_req.get(&g).copied(),
            _ => None,
        };
        let lba = match required {
            // No placement satisfies this: the VOB must begin below the end of
            // the file that precedes it. Naming the file is the whole point —
            // the alternative is an image freemkv reads at the wrong offset.
            Some(req) if req < cursor => {
                return Err(Error::DirImagePlacement {
                    path: vts.files[i].disc_path.clone(),
                });
            }
            Some(req) => req,
            None => cursor,
        };
        cursor = place_file(&mut vts.files[i], lba)?;

        // An IFO just landed: resolve the offsets it declares. Both are
        // relative to the IFO's own first sector.
        if let Some(c) = class
            && c.role == Role::Ifo
        {
            let head = read_head(&vts.files[i].host, 0xC8)?;
            let menu = be_u32(&head, 0xC0).unwrap_or(0);
            // One group per title set: `VTS_01_0.IFO` and `VTS_1_0.IFO` parse to
            // the same group, and a second insert would overwrite the first's
            // constraint, placing a VOB where the reader's IFO doesn't point.
            if !seen_ifo.insert(c.group) {
                return Err(Error::DirNameCollision {
                    host: vts.files[i].disc_path.clone(),
                });
            }
            if menu != 0 {
                menu_req.insert(c.group, lba.saturating_add(menu));
            }
            // Only a VTS IFO carries a title VOBS pointer at 0xC4; in the VMG that
            // offset is TT_SRPT (a sector offset INSIDE the IFO, not a file
            // pointer). `ifo.rs::parse_vmg` reads the same field; must not drift.
            if c.group > 0 {
                let title = be_u32(&head, 0xC4).unwrap_or(0);
                if title != 0 {
                    title_req.insert(c.group, lba.saturating_add(title));
                }
            }
        }
    }
    Ok(cursor)
}

/// What a DVD-Video filename is, for placement purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// `VIDEO_TS.IFO` / `VTS_nn_0.IFO` — the file the offsets are relative to.
    Ifo,
    /// `VIDEO_TS.VOB` / `VTS_nn_0.VOB` — constrained by the 0xC0 offset.
    MenuVob,
    /// `VTS_nn_1.VOB` — constrained by the 0xC4 offset. `_2 … _9` follow it
    /// with no gap by virtue of sorting immediately after.
    TitleVob,
    /// Backups and continuation VOBs: placed sequentially, no constraint.
    Sequential,
}

#[derive(Debug, Clone, Copy)]
struct Class {
    /// 0 = the Video Manager (`VIDEO_TS.*`), n = title set n.
    group: u32,
    /// Sort key within the group.
    order: u32,
    role: Role,
}

fn classify(name: &str) -> Option<Class> {
    let up = name.to_ascii_uppercase();
    if let Some(ext) = up.strip_prefix("VIDEO_TS.") {
        let (order, role) = match ext {
            "IFO" => (0, Role::Ifo),
            "VOB" => (1, Role::MenuVob),
            "BUP" => (2, Role::Sequential),
            _ => return None,
        };
        return Some(Class {
            group: 0,
            order,
            role,
        });
    }
    let rest = up.strip_prefix("VTS_")?;
    let (num, rest) = rest.split_once('_')?;
    let set: u32 = num.parse().ok()?;
    let (part, ext) = rest.split_once('.')?;
    let part: u32 = part.parse().ok()?;
    let (order, role) = match (ext, part) {
        ("IFO", 0) => (0, Role::Ifo),
        ("VOB", 0) => (1, Role::MenuVob),
        ("VOB", 1) => (2, Role::TitleVob),
        // Checked: `n` is parsed verbatim out of a filename, so a folder may
        // legitimately contain `VTS_01_4294967295.VOB`. Wrapping would sort a
        // stray VOB ahead of its own title set's IFO.
        ("VOB", n) => (n.checked_add(1)?, Role::Sequential),
        ("BUP", 0) => (100, Role::Sequential),
        _ => return None,
    };
    Some(Class {
        // Checked for the same reason: a wrap here lands on group 0, which is
        // the Video Manager's, so a stray file would contend for the VMG's
        // placement slot.
        group: set.checked_add(1)?,
        order,
        role,
    })
}

/// Place every file's data, depth-first, packed with no gaps.
fn place_generic(dir: &mut DirNode, cursor: &mut u32) -> Result<()> {
    for f in &mut dir.files {
        *cursor = place_file(f, *cursor)?;
    }
    for sub in &mut dir.dirs {
        place_generic(sub, cursor)?;
    }
    Ok(())
}

fn count_nodes(dir: &DirNode, dirs: &mut u32, files: &mut u32) {
    *dirs += 1;
    *files += dir.files.len() as u32;
    for sub in &dir.dirs {
        count_nodes(sub, dirs, files);
    }
}

/// Total blocks the metadata region needs: File Set Descriptor, its
/// Terminating Descriptor, one File Entry per node, and each directory's FID
/// list.
fn metadata_blocks(dir: &DirNode) -> u64 {
    let mut n = 1 + dir.files.len() as u64;
    n += dir_bytes(&dir.dirs, &dir.files).div_ceil(SECTOR) as u64;
    for sub in &dir.dirs {
        n += metadata_blocks(sub);
    }
    n
}

// Reject a Blu-ray 3D tree: a real 3D disc's `.ssif` ALIASES the same
// sectors as its base/dependent `.m2ts`, which this planner cannot express
// (it would allocate disjoint copies). See docs/dirimage.md — reject_ssif.
fn reject_ssif(root: &DirNode) -> Result<()> {
    let Some(bdmv) = child_dir(root, "BDMV") else {
        return Ok(());
    };
    let Some(stream) = child_dir(bdmv, "STREAM") else {
        return Ok(());
    };
    if child_dir(stream, "SSIF").is_some() {
        return Err(Error::DirImageSsifUnsupported);
    }
    Ok(())
}

// Plan an image over `root`. `total_sectors` feeds the oversize gate that
// decides what `-t 1` selects; it is the image's OWN size, never padded to
// a media tier. See docs/dirimage.md — plan / Capacity, and what it changes.
pub(super) fn plan(root: &Path) -> Result<Layout> {
    let mut entries = 0usize;
    let mut tree = walk(root, "", 0, &mut entries)?;
    tree.name = String::new();

    if child_dir(&tree, "BDMV").is_none() && child_dir(&tree, "VIDEO_TS").is_none() {
        return Err(Error::DirImageUnsupportedTree);
    }
    reject_ssif(&tree)?;

    // Metadata: block 0 is the FSD, block 1 its Terminating Descriptor.
    let mut next = 2u32;
    let mut uid = 0u64;
    assign_metadata(&mut tree, 0, &mut next, &mut uid);
    tree.parent_icb_lba = tree.icb_lba; // root's parent FID points at itself

    let part_start = MIN_PART_START;
    debug_assert!(part_start > ANCHOR_LBA);
    // Data starts after the metadata region AND above the floor, so nothing
    // lands at a low LBA a consumer treats as "no extent".
    let meta_end = part_start as u64 + next as u64;
    let data_start_abs = meta_end.max(DATA_FLOOR as u64);
    let mut cursor =
        u32::try_from(data_start_abs - part_start as u64).map_err(|_| Error::DirImageTooLarge)?;

    if let Some(idx) = tree
        .dirs
        .iter()
        .position(|d| d.name.eq_ignore_ascii_case("VIDEO_TS"))
    {
        cursor = place_video_ts(&mut tree.dirs[idx], cursor)?;
        // `place_video_ts` places only the FILES directly inside VIDEO_TS (the ones
        // with IFO-relative constraints); subdirectories still need placing, or
        // their File Entry has real size but no extents and reads as nothing.
        for sub in tree.dirs[idx].dirs.iter_mut() {
            place_generic(sub, &mut cursor)?;
        }
        for f in &mut tree.files {
            cursor = place_file(f, cursor)?;
        }
        for (i, sub) in tree.dirs.iter_mut().enumerate() {
            if i != idx {
                place_generic(sub, &mut cursor)?;
            }
        }
    } else {
        place_generic(&mut tree, &mut cursor)?;
    }

    let mut dir_count = 0;
    let mut file_count = 0;
    count_nodes(&tree, &mut dir_count, &mut file_count);

    let part_sectors = cursor;
    let total = part_start as u64 + part_sectors as u64 + 1; // + trailing anchor
    let total_sectors = u32::try_from(total).map_err(|_| Error::DirImageTooLarge)?;
    if total_sectors > MAX_IMAGE_SECTORS {
        return Err(Error::DirImageTooLarge);
    }
    // Metadata is materialized up front and held for the life of the image, so
    // its size is bounded here rather than discovered when memory runs out.
    let meta_bytes = (dir_count as u64 + file_count as u64).saturating_mul(SECTOR as u64);
    if meta_bytes > MAX_META_BYTES {
        return Err(Error::DirImageTooLarge);
    }

    let volume_id = root
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "FREEMKV".to_string());
    // UDF volume identifiers are a 32-byte d-string: one compression byte, the
    // characters, and a trailing length byte. Trim rather than let
    // `put_dstring` cut a multi-byte character in half.
    let volume_id: String = volume_id.chars().take(30).collect();

    Ok(Layout {
        part_start,
        part_sectors,
        total_sectors,
        volume_id,
        file_count,
        dir_count,
        next_unique_id: uid,
        root: tree,
    })
}

/// Total bytes of file data the plan covers — the honest "how big is this
/// folder" number, used for progress and for the capacity the scan sees.
pub(super) fn total_data_bytes(dir: &DirNode) -> u64 {
    let mut n: u64 = dir.files.iter().map(|f| f.size).sum();
    for sub in &dir.dirs {
        n += total_data_bytes(sub);
    }
    n
}

/// Every file in the tree, depth-first, paired with its extents. The read path
/// turns this into a sorted LBA → (file, offset) map.
pub(super) fn flatten<'a>(dir: &'a DirNode, out: &mut Vec<&'a FileNode>) {
    for f in &dir.files {
        out.push(f);
    }
    for sub in &dir.dirs {
        flatten(sub, out);
    }
}

/// Metadata footprint in blocks, for diagnostics.
pub(super) fn metadata_block_count(root: &DirNode) -> u64 {
    2 + metadata_blocks(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A file above the 30-bit AD ceiling must split on a block boundary, or
    // the next extent's bytes start mid-sector. See docs/dirimage.md — Tests.
    #[test]
    fn a_file_past_the_ad_ceiling_splits_on_a_block_boundary() {
        let mut f = FileNode {
            name: "BIG.M2TS".into(),
            disc_path: "/BIG.M2TS".into(),
            host: PathBuf::new(),
            size: MAX_AD_BYTES + 4096,
            mtime: None,
            icb_lba: 0,
            unique_id: 0,
            extents: Vec::new(),
        };
        let end = place_file(&mut f, 1000).unwrap();
        assert_eq!(f.extents.len(), 2);
        assert_eq!(f.extents[0].bytes as u64, MAX_AD_BYTES);
        assert_eq!(f.extents[0].bytes % SECTOR as u32, 0, "block multiple");
        assert!(f.extents[0].bytes <= 0x3FFF_FFFF);
        assert_eq!(f.extents[1].bytes, 4096);
        assert_eq!(
            f.extents[1].lba,
            1000 + (MAX_AD_BYTES / SECTOR as u64) as u32,
            "the second extent starts where the first ends"
        );
        assert_eq!(end, f.extents[1].lba + 2);
        assert_eq!(
            f.extents.iter().map(|e| e.bytes as u64).sum::<u64>(),
            f.size,
            "no bytes lost or invented"
        );
    }

    /// A zero-byte file records no allocation descriptors at all and consumes
    /// no blocks.
    #[test]
    fn an_empty_file_gets_no_extents() {
        let mut f = FileNode {
            name: "EMPTY".into(),
            disc_path: "/EMPTY".into(),
            host: PathBuf::new(),
            size: 0,
            mtime: None,
            icb_lba: 0,
            unique_id: 0,
            extents: Vec::new(),
        };
        assert_eq!(place_file(&mut f, 500).unwrap(), 500);
        assert!(f.extents.is_empty());
    }

    #[test]
    fn host_artefacts_are_not_disc_content() {
        assert!(is_excluded(".DS_Store"));
        assert!(is_excluded("._00000.m2ts"));
        assert!(is_excluded("00000.m2ts.partial"));
        assert!(!is_excluded("00000.m2ts"));
        assert!(!is_excluded("VTS_01_1.VOB"));
    }

    // Two host names the READER collapses into one must be refused by the
    // planner. Calls `plan` on a real folder. See docs/dirimage.md — Tests.
    #[test]
    fn two_names_the_reader_cannot_tell_apart_are_refused() {
        let dir = std::env::temp_dir().join(format!(
            "fmkv-shadow-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let stream = dir.join("BDMV/STREAM");
        std::fs::create_dir_all(&stream).expect("mkdir");
        std::fs::write(stream.join("00000.m2ts"), b"a").expect("write");
        // Same name to the reader: it trims the leading space.
        std::fs::write(stream.join(" 00000.m2ts"), b"b").expect("write");

        // Precondition — if this stops holding the fixture is wrong, not the code.
        assert_eq!(
            crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0(" 00000.m2ts")),
            crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0("00000.m2ts")),
            "fixture: these two host names must read back identically"
        );

        let got = plan(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            matches!(got, Err(Error::DirNameCollision { .. })),
            "a name the reader cannot distinguish must be refused, got {got:?}"
        );
    }

    // A name too long for the FID's one-byte length field is refused by the
    // planner, on a real folder. See docs/dirimage.md — Tests.
    #[test]
    fn an_over_long_name_is_refused_by_the_planner() {
        let dir = std::env::temp_dir().join(format!(
            "fmkv-longname-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("BDMV/STREAM")).expect("mkdir");
        // 255 bytes: the exact length that used to narrow to zero.
        let name = "a".repeat(255);
        assert_eq!(
            crate::dirimage::encode::encode_cs0(&name).len(),
            256,
            "fixture: NAME_MAX encodes to 256 bytes with the compression byte"
        );
        std::fs::write(dir.join("BDMV/STREAM").join(&name), b"x").expect("write");

        let err = plan(&dir).expect_err("the planner must refuse this folder");
        assert!(
            matches!(err, Error::DirNameTooLong { .. }),
            "expected DirNameTooLong, got {err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A name at the cap is accepted, so the guard rejects only what it must.
    #[test]
    fn a_name_at_the_cap_is_accepted() {
        let dir = std::env::temp_dir().join(format!(
            "fmkv-okname-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("BDMV/STREAM")).expect("mkdir");
        // One byte of name shorter, so the encoding lands exactly on the cap.
        let name = "a".repeat(MAX_CS0_NAME_BYTES - 1);
        assert_eq!(
            crate::dirimage::encode::encode_cs0(&name).len(),
            MAX_CS0_NAME_BYTES
        );
        std::fs::write(dir.join("BDMV/STREAM").join(&name), b"x").expect("write");
        assert!(
            plan(&dir).is_ok(),
            "a name whose encoding equals the cap must be accepted"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // The subdirectory cap must keep the 16-bit link count representable.
    // Pins the arithmetic; does NOT exercise `walk`. See docs/dirimage.md.
    #[test]
    fn the_subdir_cap_refuses_a_folder_with_too_many_subdirectories() {
        // Executes the guard rather than restating the constant: link count (child
        // dirs + 1) is 16 bits, so exceeding it wraps and the image lies about
        // its own directory structure.
        let dir = std::env::temp_dir().join(format!("fmkv-fanout-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..=MAX_SUBDIRS {
            std::fs::create_dir_all(dir.join(format!("d{i}"))).unwrap();
        }

        let err = plan(&dir).expect_err("more subdirectories than the link count can represent");
        assert!(
            matches!(err, Error::DirImageFanout { .. }),
            "expected DirImageFanout, got {err:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

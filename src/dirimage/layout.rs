//! Layout planner: a host directory tree in, a full block assignment out.
//!
//! Two phases, kept apart because they fail for different reasons:
//!
//! 1. **Walk** the folder into a tree of names and sizes. Rejects what the
//!    image model cannot represent (3D SSIF, an unrecognized tree, a
//!    case-collision inside one directory).
//! 2. **Assign** blocks. Metadata first (File Set Descriptor, one File Entry
//!    per node, then the directory FID lists), then file data.
//!
//! Data placement is the part with real constraints. For a Blu-ray there are
//! none — `.mpls`/`.clpi` address clips by name, never by LBA — so files are
//! packed sequentially. For a DVD the IFOs record VOB positions as sector
//! offsets from the IFO's own start, and `ifo.rs` re-derives every title extent
//! from them, so the placement must reproduce those offsets exactly or the rip
//! reads the wrong sectors. See [`place_video_ts`].

use super::encode::{ANCHOR_LBA, MIN_PART_START, SECTOR};
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// First block any FILE DATA may occupy, absolute.
///
/// Well clear of the volume-space descriptors, and — the load-bearing part —
/// far from zero: `disc/bluray.rs:137` drops any clip extent whose LBA is 0,
/// so a file that landed at block 0 would vanish from the title with no error.
const DATA_FLOOR: u32 = 4096;

/// Largest byte length a single allocation descriptor may record.
///
/// The AD length field is 30 bits (the top two are the ECMA-167 4/14.14.1.1
/// extent type), so 0x3FFF_FFFF is the arithmetic ceiling — but a non-final
/// extent must be a whole number of blocks, so the usable ceiling is the
/// largest multiple of 2048 below it. Files larger than this are split across
/// several ADs; `udf.rs` reads them back as a multi-extent file, which is the
/// same shape a dual-layer disc produces.
pub(super) const MAX_AD_BYTES: u64 = 0x3FFF_F800;

/// Deepest directory nesting represented. Matches `udf.rs`'s `MAX_DIR_DEPTH`:
/// anything deeper would be recorded but never descended into, so the files
/// under it would be invisible to every consumer. Refuse instead of silently
/// dropping.
const MAX_DEPTH: u32 = 8;

/// Upper bound on entries in the synthesized tree, mirroring `udf.rs`'s
/// `MAX_TOTAL_DIR_ENTRIES`.
const MAX_ENTRIES: usize = 100_000;

/// Longest OSTA CS0 encoding a File Identifier Descriptor can describe.
///
/// The FID's name-length field is one byte, so 255 is the ceiling; 254 leaves
/// the encoder no way to produce a value that wraps to zero.
const MAX_CS0_NAME_BYTES: usize = 254;

/// Most subdirectories one directory may hold.
///
/// A directory File Entry's link count is `u16` and counts one per child
/// directory plus one for its own entry in the parent, so the last usable
/// value is `u16::MAX - 1`.
const MAX_SUBDIRS: usize = (u16::MAX - 1) as usize;

/// Largest image this planner will synthesize, in sectors (128 GiB).
///
/// A DVD title set records where its VOBS begins as an offset in its own IFO,
/// and that offset is read verbatim out of a file in the folder. A regenerated
/// `.BUP`, a tool that rewrote an IFO, or a hand-assembled folder can therefore
/// name an offset far beyond the content — and the planner honours it, because
/// honouring it is what makes a real backup readable. Without a ceiling the
/// image grows to wherever that offset points: a `u32` sector count reaches
/// ~8.8 TB, and writing one to an `iso://` destination would fill a disk with
/// zeros before anything noticed.
///
/// 128 GiB clears the largest real medium (BD-100) with room to spare, so a
/// genuine disc folder never meets it.
const MAX_IMAGE_SECTORS: u32 = (128u64 * 1024 * 1024 * 1024 / SECTOR as u64) as u32;

/// Ceiling on the in-memory metadata region (64 MiB).
///
/// Every node costs a 2 KiB File Entry sector held for the life of the image,
/// so the entry cap alone permits ~205 MB of metadata for content of no size at
/// all — and the mux holds two of these at once while probing. The module
/// documents a budget of "a few MiB even for a large Blu-ray"; this is what
/// enforces it rather than merely asserting it.
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

/// Whether a host directory entry belongs in the synthesized image.
///
/// Dot-files are host artefacts, never disc content: macOS sprays `.DS_Store`
/// and `._*` resource forks through any folder a Finder window has touched,
/// and including them would put files on the "disc" that were never on the
/// disc. `.partial` is freemkv's own in-flight extraction suffix (see
/// `disc/extract.rs`) — picking one up would mean planning an extent over a
/// file that is still being written.
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
        // `file_type()` does NOT follow symlinks; `metadata()` does. A symlink
        // to a file is materialized as that file (a legitimate way to assemble
        // a folder), a symlink to a directory is skipped — following one can
        // loop, and UDF has no link this maps onto.
        let ft = entry.file_type().map_err(Error::from)?;
        let child_path = format!("{}/{}", disc_path.trim_end_matches('/'), name);
        // A File Identifier Descriptor records the encoded name length in ONE
        // byte. A longer name would wrap that field and desynchronise every
        // later entry in the directory, so refuse while planning rather than
        // encode something unreadable. 255 ASCII bytes is a legal name on
        // ext4/APFS/NTFS and already exceeds this once the CS0 compression
        // byte is added, so it is reachable without anything exotic.
        if crate::dirimage::encode::encode_cs0(&name).len() > MAX_CS0_NAME_BYTES {
            return Err(Error::DirNameTooLong { path: child_path });
        }
        *entries += 1;
        if *entries > MAX_ENTRIES {
            return Err(Error::DirImageTooLarge);
        }
        // Key the uniqueness check on the name AS THE READER WILL SEE IT, by
        // round-tripping through the very encoder and parser that will be used.
        //
        // The host name is not that name. `parse_udf_name` trims leading and
        // trailing whitespace and drops any code unit `char::from_u32` rejects
        // (a lone surrogate half, i.e. every non-BMP character the encoder
        // emits as a surrogate pair). So " A.M2TS" and "A.M2TS", or "A<astral>.M2TS"
        // and "A.M2TS", are distinct hosts that collapse to ONE name on read —
        // and `find`/`read_file` take the first match, so a title silently
        // resolves to the wrong file's extents and muxes the wrong bytes at
        // exit 0. Comparing the raw host names cannot see that; deriving the
        // key from the same two functions cannot drift from it.
        let as_read = crate::udf::parse_udf_name(&crate::dirimage::encode::encode_cs0(&name));
        // Nothing left after the reader is done with it: the entry would exist
        // in the image and be unaddressable by any name. Refuse rather than
        // write something no consumer can reach.
        if as_read.is_empty() {
            return Err(Error::DirNameCollision { host: child_path });
        }
        names.push(as_read.to_ascii_uppercase());
        if ft.is_dir() {
            // A directory's File Entry records its link count in 16 bits: one
            // per child directory plus one for its own entry in the parent.
            // The global entry cap alone permits a single directory holding
            // more than that, which would wrap the count.
            if dirs.len() >= MAX_SUBDIRS {
                return Err(Error::DirImageFanout {
                    path: disc_path.to_string(),
                });
            }
            dirs.push(walk(&entry.path(), &child_path, depth + 1, entries)?);
        } else {
            let meta = match std::fs::metadata(entry.path()) {
                Ok(m) => m,
                // A broken symlink or a file that vanished between readdir and
                // stat: skip it rather than plan an extent that cannot be read.
                // Anything else — permission denied, an I/O error on the host
                // volume — is NOT a missing file, and skipping it would drop a
                // real file out of the image with nothing reported.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)),
            };
            if !meta.is_file() {
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

    // `UdfFs::find_dir` / `read_file` match path components case-insensitively,
    // so two entries in one directory differing only in case are
    // indistinguishable to every consumer — the second would silently shadow
    // the first. Only reachable on a case-sensitive host volume.
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

/// Read the first `n` bytes of a host file.
/// Read the first `n` bytes of an IFO so its placement offsets can be resolved.
///
/// Errors propagate. Returning an empty buffer instead would send every offset
/// through `unwrap_or(0)`, recording NO placement constraint — and a VOB placed
/// without its constraint yields an image that reads at the wrong offset with
/// nothing reported. An IFO that cannot be read is a folder that cannot be
/// planned.
fn read_head(path: &Path, n: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut buf = vec![0u8; n];
    // `read_exact`, not `read`: a single `read` may legally return fewer bytes
    // than asked on a network or FUSE mount — and a NAS-hosted backup is the
    // normal case for this feature. A short buffer sends every placement offset
    // through `unwrap_or(0)`, recording NO constraint, so the planner and the
    // reader disagree about where a VOB begins and the rip reads the wrong
    // sectors for a whole title at exit 0.
    let mut f = std::fs::File::open(path).map_err(Error::from)?;
    f.read_exact(&mut buf).map_err(Error::from)?;
    Ok(buf)
}

/// Placement order and constraints for a `VIDEO_TS` folder.
///
/// DVD-Video records VOB positions INSIDE the IFOs, as sector offsets from the
/// IFO file's own first sector:
///
/// * `VIDEO_TS.IFO` + 0xC0 (`VMGM_VOBS`)  → `VIDEO_TS.VOB`
/// * `VTS_nn_0.IFO` + 0xC0 (`vtsm_vobs`)  → `VTS_nn_0.VOB` (the VTS menu)
/// * `VTS_nn_0.IFO` + 0xC4 (`vtstt_vobs`) → `VTS_nn_1.VOB` (the title stream)
///
/// `ifo.rs:554-556` re-derives every title extent as
/// `file_start_lba(IFO) + vtstt_vobs + cell.first_sector`, so only the third
/// of those is load-bearing for freemkv itself — but the other two are
/// load-bearing for DVD PLAYERS, which read the menus freemkv ignores. All
/// three are honoured.
///
/// `VTS_nn_1.VOB … VTS_nn_9.VOB` are one logical stream split at the 1 GB
/// file-size limit, and the cell sector addresses run continuously across the
/// split, so they are placed back-to-back with no gap. Laying the group out in
/// its canonical on-disc order gives exactly that for free; the constraint is
/// then a CHECK, not a search.
///
/// The check can fail — a regenerated `.BUP`, a tool that rewrote an IFO, a
/// folder assembled by hand — and when it does the required position lies
/// below the end of the file that must precede it. There is no placement that
/// satisfies it, so it is a typed error naming the file rather than a silent
/// misplacement.
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
    // Groups whose IFO has already been placed. Kept separately from the
    // constraint maps because an IFO that declares no offsets inserts into
    // neither, so keying the duplicate check on those maps would miss a second
    // IFO for the same set — which is the collision the check exists for.
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
            // One group per title set. Two files whose names differ only in
            // how the number is written — `VTS_01_0.IFO` and `VTS_1_0.IFO` —
            // parse to the same group, and the second insert would overwrite
            // the first's constraint, placing a VOB at an address the IFO the
            // reader uses does not point to. Refuse instead of picking one.
            if !seen_ifo.insert(c.group) {
                return Err(Error::DirNameCollision {
                    host: vts.files[i].disc_path.clone(),
                });
            }
            if menu != 0 {
                menu_req.insert(c.group, lba.saturating_add(menu));
            }
            // Only a VTS IFO carries a title VOBS pointer at 0xC4. In the VMG
            // (`VIDEO_TS.IFO`) that offset is TT_SRPT, the title search pointer
            // table — a sector offset INSIDE the IFO, not a pointer to another
            // file, so treating it as one would place `VIDEO_TS.VOB` at a
            // meaningless address. (`ifo.rs::parse_vmg` reads the same field as
            // TT_SRPT; the two must not drift.)
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

/// Reject a Blu-ray 3D tree.
///
/// `disc/bluray.rs:124-130` probes `/BDMV/STREAM/SSIF/{clip}.ssif` and sets
/// `is_3d` the moment one resolves — unconditionally, before any capability
/// check. On a real 3D disc the `.ssif` and the base/dependent `.m2ts` files
/// ALIAS the same sectors (the SSIF interleave IS the two m2ts streams), which
/// this planner cannot express: it would allocate three disjoint copies, so
/// the clip's extents and the m2ts's extents would disagree and the rip would
/// read the wrong bytes at exit 0. Reject the folder instead.
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

/// Plan an image over `root`.
///
/// # Capacity, and what it changes
///
/// `total_sectors` becomes the `Disc`'s `capacity_sectors` / `capacity_bytes`,
/// and `capacity_bytes` is not inert: `canonical_title_order`'s oversize gate
/// (`disc/mod.rs:2039-2040`, body `:2209-2210`) demotes any title whose
/// `size_bytes` exceeds it, which decides which title sorts first and therefore
/// what `-t 1` selects.
///
/// The capacity reported here is **the synthesized image's own size** — what an
/// ISO built from this folder would report — and nothing else. Specifically it
/// is NOT padded up to a media tier (BD25/BD50/…), which was considered and
/// does not work: a folder does not record the tier of the disc it came from,
/// so a 22 GB folder off a BD50 would pad to BD25 and diverge anyway, and the
/// padding would inflate any pre-sized output written from the image.
///
/// The consequence, stated plainly: **for a folder that is missing files the
/// source disc had — a selective MakeMKV backup being the normal case — the
/// capacity is smaller than the source disc's, the oversize gate is therefore
/// stricter, and a borderline "play-all" composite title that the ISO kept can
/// be demoted here. `-t 1` can select a different title from `dir://FOLDER`
/// than from an `iso://` of the same disc.** For a complete folder the two
/// agree, because the capacities agree.
///
/// This is chosen over the alternatives because it is the only one that is
/// self-consistent: `dir://X` and an `iso://` built from `X` describe the same
/// image and must answer the same way.
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
        // `place_video_ts` places only the FILES directly inside VIDEO_TS,
        // because only those carry the IFO-relative constraints. Anything in a
        // subdirectory of VIDEO_TS still needs data placed: without this its
        // File Entry is written with the file's real size but no extents, so it
        // appears in the tree at full length and reads back as nothing, at
        // exit 0. The same folder under BDMV/ was always placed correctly,
        // which is what made the gap easy to miss.
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

    /// A file above the 30-bit AD ceiling must split into MULTIPLE
    /// descriptors, and every non-final one must be a whole number of blocks —
    /// otherwise the next extent's bytes start mid-sector and the file
    /// reassembles wrong. Kills a mutant that emits one oversized AD (whose
    /// length would collide with the extent-TYPE bits `udf.rs:642` reads).
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

    /// A name too long for the FID's one-byte length field is refused by the
    /// PLANNER, on a real folder.
    ///
    /// Audit finding: the length was narrowed with `as u8`, so a 255-byte ASCII
    /// name — POSIX NAME_MAX, legal on ext4/APFS/NTFS — encoded to 256 bytes
    /// with the CS0 compression byte and wrote a length of ZERO, making every
    /// later entry in that directory read from the wrong offset.
    ///
    /// An earlier version of this test asserted arithmetic about the constants
    /// and never called `plan`, so it would have passed with the guard deleted.
    /// Two host names that the READER collapses into one must be refused by
    /// the planner, not written into the image.
    ///
    /// Audit finding: the uniqueness check compared raw host names, while
    /// `parse_udf_name` trims whitespace and drops code units `char::from_u32`
    /// rejects. So " 00000.m2ts" and "00000.m2ts" were two distinct entries at
    /// plan time and ONE name at read time; `find`/`read_file` take the first
    /// match, so a title resolved to the wrong file's extents and muxed the
    /// wrong bytes at exit 0.
    ///
    /// This calls `plan` on a real folder. It fails if the round-trip key is
    /// reverted to comparing host names.
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

    /// The subdirectory cap must keep a directory's 16-bit link count
    /// representable. Creating 65,535 directories in a test is not reasonable,
    /// so this pins the arithmetic relationship the guard relies on — and says
    /// plainly that it does NOT exercise `walk`.
    #[test]
    fn the_subdir_cap_keeps_the_link_count_representable() {
        assert_eq!(
            (MAX_SUBDIRS as u16).checked_add(1),
            Some(u16::MAX),
            "the largest permitted fan-out must still fit the link count"
        );
    }
}

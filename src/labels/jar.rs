//! BD-J jar utilities — common scaffolding for parsers that read
//! `/BDMV/JAR/*.jar`.
//!
//! Composes with [`class_reader`](super::class_reader) for structured
//! `.class` access. Used by `dbp` (string-pool scan via constant pool)
//! and `deluxe` (bytecode pattern matching) — those parsers express
//! "open every top-level jar, look at every .class inside" without
//! repeating the zip-archive boilerplate.

use super::class_reader::ClassFile;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::io::{Cursor, Read, Seek, SeekFrom};
use zip::ZipArchive;

// Cap on bytes read from one `.class` entry — the jar's declared size is attacker-controlled,
// so the buffer grows incrementally instead of pre-sizing.
const MAX_CLASS_BYTES: u64 = 64 * 1024 * 1024;

/// Cap on total bytes one label parse inflates, shared by every sweep over every jar: the
/// per-entry cap alone lets hostile high-ratio entries drive unbounded inflation.
pub(crate) const PARSE_INFLATE_BUDGET: u64 = 128 * 1024 * 1024;

/// In-memory zip archive: backed by a `Vec<u8>` read from UDF. Owns
/// the buffer; callers pass it to [`has_path_prefix`], [`for_each_class_budgeted`],
/// etc.
pub type Jar = ZipArchive<Cursor<Vec<u8>>>;

/// Open every top-level `*.jar` entry in `/BDMV/JAR/` and yield each
/// `(entry_name, Jar)` to `f`. Returns the first `Some(R)` the callback
/// produces, or `None` if every jar was visited without a hit.
///
/// "Top-level" means entries directly under `/BDMV/JAR/`, not nested
/// under a subdir.
///
/// Entries that fail to read from UDF or that aren't valid zips are
/// silently skipped — same defensive shape as the existing dbp parser.
pub fn for_each_jar<R, F>(reader: &mut dyn SectorSource, udf: &UdfFs, mut f: F) -> Option<R>
where
    F: FnMut(&str, &mut Jar) -> Option<R>,
{
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in jar_dir.entries.iter().filter(|e| is_jar(e)) {
        let path = format!("/BDMV/JAR/{}", entry.name);
        let Ok(bytes) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(mut archive) = ZipArchive::new(Cursor::new(bytes)) else {
            continue;
        };
        if let Some(r) = f(&entry.name, &mut archive) {
            return Some(r);
        }
    }
    None
}

fn is_jar(e: &crate::udf::DirEntry) -> bool {
    !e.is_dir && e.name.to_ascii_lowercase().ends_with(".jar")
}

/// A seekable byte source for a jar: in memory, or read from disc on demand.
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// A jar opened by [`visit_jars`].
pub type DiscJar<'a> = ZipArchive<Box<dyn ReadSeek + 'a>>;

// Jars up to this size are read whole; larger ones (image-heavy menus, past the
// UDF whole-file cap) are opened in place, reading only the directory and entries used.
const IN_MEMORY_JAR_BYTES: u64 = 64 * 1024 * 1024;

/// Like [`for_each_jar`] but also yields jars that could not be read or opened
/// (as `None`), so a caller can tell a skipped jar from an absent one. Large jars
/// are read in place rather than skipped.
pub fn visit_jars<R, F>(reader: &mut dyn SectorSource, udf: &UdfFs, f: F) -> Option<R>
where
    F: FnMut(&str, Option<&mut DiscJar<'_>>) -> Option<R>,
{
    visit_jars_limited(reader, udf, IN_MEMORY_JAR_BYTES, f)
}

pub(crate) fn visit_jars_limited<R, F>(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    in_memory_max: u64,
    mut f: F,
) -> Option<R>
where
    F: FnMut(&str, Option<&mut DiscJar<'_>>) -> Option<R>,
{
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in jar_dir.entries.iter().filter(|e| is_jar(e)) {
        let path = format!("/BDMV/JAR/{}", entry.name);
        let src: Option<Box<dyn ReadSeek + '_>> = if entry.size <= in_memory_max {
            udf.read_file(reader, &path)
                .ok()
                .map(|b| Box::new(Cursor::new(b)) as Box<dyn ReadSeek>)
        } else {
            udf.extents_abs_at(reader, entry.meta_lba)
                .ok()
                .filter(|x| x.iter().all(|e| e.recorded || e.len == 0))
                .map(|x| x.iter().map(|e| (e.lba, e.len as u64)).collect())
                .map(|x| ExtentReader::new(&mut *reader, x, entry.size))
                .and_then(|mut r| r.has_zip_tail().then(|| Box::new(r) as Box<dyn ReadSeek>))
        };
        let mut archive = src.and_then(|s| ZipArchive::new(s).ok());
        if let Some(r) = f(&entry.name, archive.as_mut()) {
            return Some(r);
        }
    }
    None
}

// Sectors fetched per on-demand read.
const CHUNK_SECTORS: u64 = 32;

// A zip's end-of-central-directory record: signature, within the last 64 KiB + 22 bytes.
const EOCD_SIG: &[u8; 4] = b"PK\x05\x06";
const EOCD_SEARCH: u64 = 65_535 + 22;
// Cap on EOCD candidates checked (each may read the CD start): stray signatures
// in a hostile tail cannot multiply reads.
pub(crate) const MAX_EOCD_PROBES: usize = 4;
// Central-directory file header signature.
const CDFH_SIG: &[u8; 4] = b"PK\x01\x02";

/// `Read + Seek` over a UDF file's absolute `(lba, byte length)` extents, fetching
/// sectors on demand (one cached chunk, aligned to the read direction). Reads end at
/// the file's byte `size`.
pub struct ExtentReader<'a> {
    reader: &'a mut dyn SectorSource,
    extents: Vec<(u32, u64)>,
    size: u64,
    pos: u64,
    buf: Vec<u8>,
    buf_start: u64,
}

impl<'a> ExtentReader<'a> {
    pub fn new(reader: &'a mut dyn SectorSource, extents: Vec<(u32, u64)>, size: u64) -> Self {
        Self {
            reader,
            extents,
            size,
            pos: 0,
            buf: Vec::new(),
            buf_start: 0,
        }
    }

    /// Whether the tail holds a consistent EOCD: comment within the file and a CDFH
    /// signature at `cd_offset` or at `eocd - cd_size` (prepended data). Checked before
    /// the zip reader's backward search (which rescans the whole file on a bad EOCD),
    /// so a damaged jar costs a few bounded reads. Leaves the tail cached.
    pub fn has_zip_tail(&mut self) -> bool {
        self.find_eocd().is_some() && self.seek(SeekFrom::Start(0)).is_ok()
    }

    // Offset of the last EOCD in the tail that passes the consistency checks.
    fn find_eocd(&mut self) -> Option<u64> {
        let n = self.size.min(EOCD_SEARCH);
        let tail_start = self.size - n;
        let tail = self.cache_span(tail_start)?;
        let le32 = |b: &[u8], o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        let mut probes = 0;
        for at in (0..tail.len().saturating_sub(21)).rev() {
            let rec = &tail[at..];
            if &rec[..4] != EOCD_SIG {
                continue;
            }
            probes += 1;
            if probes > MAX_EOCD_PROBES {
                return None;
            }
            let eocd = tail_start + at as u64;
            let comment = u16::from_le_bytes([rec[20], rec[21]]) as u64;
            let (cd_size, cd_offset) = (le32(rec, 12) as u64, le32(rec, 16) as u64);
            if eocd + 22 + comment > self.size {
                continue;
            }
            let at_offset = cd_offset + cd_size <= eocd;
            if (at_offset && self.cdfh_at(cd_offset, &tail, tail_start))
                || (cd_size <= eocd && self.cdfh_at(eocd - cd_size, &tail, tail_start))
            {
                self.cache_span(tail_start)?;
                return Some(eocd);
            }
        }
        None
    }

    // Whether a CDFH signature sits at `off` (from the cached tail when inside it).
    fn cdfh_at(&mut self, off: u64, tail: &[u8], tail_start: u64) -> bool {
        if off >= tail_start {
            let i = (off - tail_start) as usize;
            return tail.get(i..i + 4) == Some(CDFH_SIG.as_slice());
        }
        let mut sig = [0u8; 4];
        self.seek(SeekFrom::Start(off)).is_ok()
            && self.read_exact(&mut sig).is_ok()
            && &sig == CDFH_SIG
    }

    // Cache the sectors covering [from, size) as one chunk and return those bytes.
    fn cache_span(&mut self, from: u64) -> Option<Vec<u8>> {
        let covered = self.buf_start <= from
            && self.buf_start + self.buf.len() as u64 >= self.size
            && !self.buf.is_empty();
        if !covered {
            self.fill_sectors(from, self.size).ok()?;
        }
        if self.buf_start <= from && self.buf_start + self.buf.len() as u64 >= self.size {
            return self
                .buf
                .get((from - self.buf_start) as usize..)
                .map(<[u8]>::to_vec);
        }
        // Tail straddles extents: read it through the chunked path.
        let mut tail = vec![0u8; (self.size - from) as usize];
        self.seek(SeekFrom::Start(from)).ok()?;
        self.read_exact(&mut tail).ok()?;
        Some(tail)
    }

    // Load the sectors covering [from, to) when they lie in one extent; otherwise a
    // regular forward chunk at `from`.
    fn fill_sectors(&mut self, from: u64, to: u64) -> std::io::Result<()> {
        let mut base = 0u64;
        for &(lba, len) in &self.extents {
            if from < base + len {
                if to > base + len {
                    return self.fill(false);
                }
                let first = (from - base) / 2048;
                let end = (to - base).div_ceil(2048);
                return self.load(lba, base, len, first, end);
            }
            base += len;
        }
        Err(std::io::ErrorKind::UnexpectedEof.into())
    }

    // Load a chunk holding `self.pos`: ending at it when reading backwards, else
    // starting at it. Only the extent's own bytes are exposed.
    fn fill(&mut self, backward: bool) -> std::io::Result<()> {
        let mut base = 0u64;
        for &(lba, len) in &self.extents {
            if self.pos < base + len {
                let sec = (self.pos - base) / 2048;
                let first = if backward {
                    sec.saturating_sub(CHUNK_SECTORS - 1)
                } else {
                    sec
                };
                let end = (first + CHUNK_SECTORS).min(len.div_ceil(2048));
                return self.load(lba, base, len, first, end);
            }
            base += len;
        }
        Err(std::io::ErrorKind::UnexpectedEof.into())
    }

    // Read sectors [first, end) of the extent at `lba` (file offset `base`, `len`
    // bytes) into the cache, exposing only the extent's own bytes.
    fn load(&mut self, lba: u32, base: u64, len: u64, first: u64, end: u64) -> std::io::Result<()> {
        let count = u16::try_from(end - first).map_err(|_| std::io::ErrorKind::InvalidData)?;
        let start = u32::try_from(first)
            .ok()
            .and_then(|f| lba.checked_add(f))
            .ok_or(std::io::ErrorKind::InvalidData)?;
        let mut buf = vec![0u8; count as usize * 2048];
        self.reader.read_sectors(start, count, &mut buf, true)?;
        buf.truncate((len - first * 2048).min(buf.len() as u64) as usize);
        self.buf = buf;
        self.buf_start = base + first * 2048;
        Ok(())
    }
}

impl Read for ExtentReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.size || out.is_empty() {
            return Ok(0);
        }
        let cached =
            self.pos >= self.buf_start && self.pos < self.buf_start + self.buf.len() as u64;
        if !cached {
            // Fill backwards only for a seek just below the cached chunk (a backward
            // scan); any other jump fills forward from the target.
            let backward = !self.buf.is_empty()
                && self.pos < self.buf_start
                && self.buf_start - self.pos <= CHUNK_SECTORS * 2048;
            self.fill(backward)?;
        }
        let off = (self.pos - self.buf_start) as usize;
        let n = out
            .len()
            .min(self.buf.len() - off)
            .min((self.size - self.pos) as usize);
        out[..n].copy_from_slice(&self.buf[off..off + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for ExtentReader<'_> {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let pos = match to {
            SeekFrom::Start(p) => Some(p),
            SeekFrom::End(d) => self.size.checked_add_signed(d),
            SeekFrom::Current(d) => self.pos.checked_add_signed(d),
        };
        self.pos = pos.ok_or(std::io::ErrorKind::InvalidInput)?;
        Ok(self.pos)
    }
}

/// True if any entry in this jar's central directory starts with
/// `prefix`. Fast — only reads filenames, never extracts bytes.
///
/// Used by parsers as a cheap "is this MY framework's jar?" check
/// (e.g. `has_path_prefix(archive, "com/dbp/")` for dbp,
/// `has_path_prefix(archive, "com/bydeluxe/")` for Deluxe).
pub fn has_path_prefix<Z: Read + Seek>(archive: &ZipArchive<Z>, prefix: &str) -> bool {
    archive.file_names().any(|n| n.starts_with(prefix))
}

// Jars up to this size are read whole by `any_jar_has_prefix` (one chunk); larger ones
// are opened in place so only the tail and central directory are read.
const DETECT_IN_MEMORY_JAR_BYTES: u64 = CHUNK_SECTORS * 2048;

/// True if any top-level `/BDMV/JAR/*.jar` has a central-directory entry starting
/// with `prefix`: a framework detector that never reads entry data.
pub fn any_jar_has_prefix(reader: &mut dyn SectorSource, udf: &UdfFs, prefix: &str) -> bool {
    // A jar the in-place open cannot handle (unrecorded extent, ZIP64, odd tail) still
    // detects through the whole-file read `for_each_jar` uses.
    let mut unopened: Vec<String> = Vec::new();
    let hit = visit_jars_limited(
        reader,
        udf,
        DETECT_IN_MEMORY_JAR_BYTES,
        |name, jar| match jar {
            Some(j) => has_path_prefix(j, prefix).then_some(()),
            None => {
                unopened.push(name.to_string());
                None
            }
        },
    );
    if hit.is_some() {
        return true;
    }
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return false;
    };
    jar_dir
        .entries
        .iter()
        .filter(|e| e.size <= IN_MEMORY_JAR_BYTES && unopened.contains(&e.name))
        .any(|e| {
            udf.read_file(reader, &format!("/BDMV/JAR/{}", e.name))
                .ok()
                .and_then(|b| ZipArchive::new(Cursor::new(b)).ok())
                .is_some_and(|z| has_path_prefix(&z, prefix))
        })
}

/// Iterate every `.class` entry, parse it with [`class_reader`](super::class_reader)
/// and call `f` until it returns `Some`. Entries that fail to read or parse are
/// skipped. Every inflated byte is charged to `budget`; the walk stops once it is
/// spent, so a caller sweeping many jars bounds total inflation.
pub fn try_each_class_budgeted<Z: Read + Seek, R, F>(
    archive: &mut ZipArchive<Z>,
    budget: &mut u64,
    mut f: F,
) -> Option<R>
where
    F: FnMut(&str, &ClassFile) -> Option<R>,
{
    let is_class = |n: &str| n.ends_with(".class");
    try_each_entry(archive, is_class, MAX_CLASS_BYTES, budget, |name, bytes| {
        f(name, &ClassFile::parse(bytes).ok()?)
    })
}

/// Parse the one `.class` entry named `entry_name` (its full jar path) and call `f` on it.
/// Every other entry is skipped by name, so it is neither inflated nor charged to `budget`.
pub fn try_class_budgeted<Z: Read + Seek, R, F>(
    archive: &mut ZipArchive<Z>,
    entry_name: &str,
    budget: &mut u64,
    mut f: F,
) -> Option<R>
where
    F: FnMut(&ClassFile) -> Option<R>,
{
    let is_target = |n: &str| n == entry_name;
    try_each_entry(archive, is_target, MAX_CLASS_BYTES, budget, |_, bytes| {
        f(&ClassFile::parse(bytes).ok()?)
    })
}

/// [`try_each_class_budgeted`] visiting every class (no short-circuit).
pub fn for_each_class_budgeted<Z: Read + Seek, F>(
    archive: &mut ZipArchive<Z>,
    budget: &mut u64,
    mut f: F,
) where
    F: FnMut(&str, &ClassFile),
{
    let _: Option<()> = try_each_class_budgeted(archive, budget, |name, class| {
        f(name, class);
        None
    });
}

/// Iterate the NON-`.class`, non-directory entries whose name satisfies `want`
/// (checked BEFORE anything is inflated), reading at most `cap` bytes of each
/// and charging them to `budget`. Returns the first `Some(R)` from `f`.
///
/// This is the Tier-1 menu-walk sweep: newer discs embed the same
/// `dcx.xml`/`playlists.xml` manifests INSIDE a jar rather than as loose
/// `/BDMV/JAR/<id>/` files. Unreadable entries are silently ignored.
pub fn try_each_resource<Z: Read + Seek, R, F>(
    archive: &mut ZipArchive<Z>,
    want: impl Fn(&str) -> bool,
    cap: u64,
    budget: &mut u64,
    f: F,
) -> Option<R>
where
    F: FnMut(&str, &[u8]) -> Option<R>,
{
    let is_resource =
        |n: &str| !n.ends_with('/') && !n.to_ascii_lowercase().ends_with(".class") && want(n);
    try_each_entry(archive, is_resource, cap, budget, f)
}

// Filter by central-directory name, then inflate at most `cap` bytes (declared size is
// untrusted). An entry the budget cannot cover is not offered and stops the walk (logged
// once, by the walk that ran it out); one that exactly fits is complete.
fn try_each_entry<Z: Read + Seek, R>(
    archive: &mut ZipArchive<Z>,
    want: impl Fn(&str) -> bool,
    cap: u64,
    budget: &mut u64,
    mut f: impl FnMut(&str, &[u8]) -> Option<R>,
) -> Option<R> {
    let had_budget = *budget > 0;
    let spent = |i: usize| {
        if had_budget {
            tracing::warn!(entry = i, "jar: inflate budget exhausted, sweep truncated");
        }
    };
    for i in 0..archive.len() {
        match archive.name_for_index(i) {
            Some(n) if want(n) => {}
            _ => continue,
        }
        if *budget == 0 {
            spent(i);
            return None;
        }
        let Ok(entry) = archive.by_index(i) else {
            continue;
        };
        let name = entry.name().to_string();
        let mut bytes = Vec::new();
        let read = entry
            .take(cap.min(budget.saturating_add(1)))
            .read_to_end(&mut bytes);
        if bytes.len() as u64 > *budget {
            *budget = 0;
            spent(i);
            return None;
        }
        *budget -= bytes.len() as u64;
        if read.is_err() {
            continue;
        }
        if let Some(r) = f(&name, &bytes) {
            return Some(r);
        }
    }
    None
}

#[cfg(test)]
#[path = "jar_tests.rs"]
mod tests;

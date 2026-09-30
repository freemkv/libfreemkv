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
    visit_jars_limited(reader, udf, DETECT_IN_MEMORY_JAR_BYTES, |_, jar| {
        jar.filter(|j| has_path_prefix(j, prefix)).map(|_| ())
    })
    .is_some()
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
mod tests {
    use super::*;

    // A budget no test fixture can exhaust.
    fn unbounded() -> u64 {
        u64::MAX
    }

    /// Smallest constant-pool-empty `.class`: magic, versions, cp_count=1
    /// (zero real entries), then empty access/this/super/interfaces/
    /// fields/methods/attributes.
    const MINIMAL_CLASS: &[u8] = &[
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, // minor
        0x00, 0x00, // major
        0x00, 0x01, // constant_pool_count = 1 -> no entries
        0x00, 0x00, // access_flags
        0x00, 0x00, // this_class
        0x00, 0x00, // super_class
        0x00, 0x00, // interfaces_count
        0x00, 0x00, // fields_count
        0x00, 0x00, // methods_count
        0x00, 0x00, // attributes_count
    ];

    // Build a raw, single-entry, Stored ZIP whose header declares
    // `declared_size` as the uncompressed size while the actual stored
    // payload is `payload` — forges an attacker-controlled size field.
    fn build_stored_zip(name: &str, payload: &[u8], declared_size: u32) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let crc: u32 = {
            // CRC-32 (IEEE) over payload.
            let mut crc = 0xFFFF_FFFFu32;
            for &b in payload {
                crc ^= b as u32;
                for _ in 0..8 {
                    let mask = (crc & 1).wrapping_neg();
                    crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                }
            }
            !crc
        };
        let mut out = Vec::new();
        // ----- Local file header -----
        let lfh_offset = out.len() as u32;
        out.extend_from_slice(&0x0403_4b50u32.to_le_bytes()); // signature
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&0u16.to_le_bytes()); // method = Stored
        out.extend_from_slice(&0u16.to_le_bytes()); // mod time
        out.extend_from_slice(&0u16.to_le_bytes()); // mod date
        out.extend_from_slice(&crc.to_le_bytes()); // crc-32
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // compressed size
        out.extend_from_slice(&declared_size.to_le_bytes()); // uncompressed size (forged)
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // extra len
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(payload);
        // ----- Central directory header -----
        let cd_offset = out.len() as u32;
        out.extend_from_slice(&0x0201_4b50u32.to_le_bytes()); // signature
        out.extend_from_slice(&20u16.to_le_bytes()); // version made by
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&0u16.to_le_bytes()); // method = Stored
        out.extend_from_slice(&0u16.to_le_bytes()); // mod time
        out.extend_from_slice(&0u16.to_le_bytes()); // mod date
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // compressed size
        out.extend_from_slice(&declared_size.to_le_bytes()); // uncompressed size (forged)
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // extra len
        out.extend_from_slice(&0u16.to_le_bytes()); // comment len
        out.extend_from_slice(&0u16.to_le_bytes()); // disk number start
        out.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
        out.extend_from_slice(&0u32.to_le_bytes()); // external attrs
        out.extend_from_slice(&lfh_offset.to_le_bytes()); // local header offset
        out.extend_from_slice(name_bytes);
        let cd_size = out.len() as u32 - cd_offset;
        // ----- End of central directory -----
        out.extend_from_slice(&0x0605_4b50u32.to_le_bytes()); // signature
        out.extend_from_slice(&0u16.to_le_bytes()); // disk number
        out.extend_from_slice(&0u16.to_le_bytes()); // cd start disk
        out.extend_from_slice(&1u16.to_le_bytes()); // entries on this disk
        out.extend_from_slice(&1u16.to_le_bytes()); // total entries
        out.extend_from_slice(&cd_size.to_le_bytes());
        out.extend_from_slice(&cd_offset.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // comment len
        out
    }

    fn open(bytes: Vec<u8>) -> Jar {
        ZipArchive::new(Cursor::new(bytes)).expect("valid zip")
    }

    // Pins the exact 64 MiB value as a literal (not derived from the
    // same expression under test) so an arithmetic mutation is caught
    // even though no test builds a full 64 MiB buffer.
    #[test]
    fn max_class_bytes_is_64_mebibytes() {
        assert_eq!(MAX_CLASS_BYTES, 67_108_864);
    }

    #[test]
    fn has_path_prefix_matches_only_declared_prefix() {
        let jar = open(build_stored_zip(
            "com/dbp/Loader.class",
            MINIMAL_CLASS,
            MINIMAL_CLASS.len() as u32,
        ));
        assert!(has_path_prefix(&jar, "com/dbp/"));
        assert!(!has_path_prefix(&jar, "com/bydeluxe/"));
    }

    #[test]
    fn try_each_class_reads_minimal_class() {
        let mut jar = open(build_stored_zip(
            "Foo.class",
            MINIMAL_CLASS,
            MINIMAL_CLASS.len() as u32,
        ));
        let mut seen = Vec::new();
        let r: Option<()> = try_each_class_budgeted(&mut jar, &mut unbounded(), |name, _class| {
            seen.push(name.to_string());
            None
        });
        assert!(r.is_none());
        assert_eq!(seen, vec!["Foo.class".to_string()]);
    }

    #[test]
    fn for_each_class_visits_every_class() {
        let mut jar = open(build_stored_zip(
            "Bar.class",
            MINIMAL_CLASS,
            MINIMAL_CLASS.len() as u32,
        ));
        let mut count = 0usize;
        for_each_class_budgeted(&mut jar, &mut unbounded(), |_, _| count += 1);
        assert_eq!(count, 1);
    }

    // Attacker-controlled size field (0xFFFF_FFFF ≈ 4 GiB) must not
    // trigger a 4 GiB pre-allocation; incremental read completes and
    // the real (small) payload parses fine.
    #[test]
    fn forged_huge_uncompressed_size_does_not_preallocate() {
        let mut jar = open(build_stored_zip("Evil.class", MINIMAL_CLASS, 0xFFFF_FFFF));
        let mut parsed = false;
        for_each_class_budgeted(&mut jar, &mut unbounded(), |name, _class| {
            assert_eq!(name, "Evil.class");
            parsed = true;
        });
        // Reached here without OOM/abort, and the real bytes parsed.
        assert!(parsed);
    }

    #[test]
    fn read_is_bounded_by_cap() {
        // Padding past MINIMAL_CLASS is harmless trailing data the parser
        // ignores; the point is that read_to_end stops at the cap rather
        // than following a (potentially huge) declared size.
        let mut payload = MINIMAL_CLASS.to_vec();
        payload.extend(std::iter::repeat_n(0u8, 4096));
        let mut jar = open(build_stored_zip(
            "Padded.class",
            &payload,
            // Forge a size far larger than the real payload.
            0xFFFF_FFFF,
        ));
        let mut visited = 0usize;
        for_each_class_budgeted(&mut jar, &mut unbounded(), |_, _| visited += 1);
        assert_eq!(visited, 1);
    }

    #[test]
    fn try_each_resource_reads_non_class_entries_and_skips_classes() {
        let xml = b"<dcx><disc/></dcx>";
        // A .class entry must be skipped; the .xml resource must be surfaced.
        let mut jar = {
            use std::io::{Cursor, Write as _};
            let mut buf = Vec::new();
            {
                let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
                let opts = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);
                w.start_file("com/x/Menu.class", opts).unwrap();
                w.write_all(MINIMAL_CLASS).unwrap();
                w.start_file("00000/dcx.xml", opts).unwrap();
                w.write_all(xml).unwrap();
                w.finish().unwrap();
            }
            ZipArchive::new(Cursor::new(buf)).expect("valid zip")
        };

        let mut seen: Vec<String> = Vec::new();
        let mut budget = u64::MAX;
        let found: Option<Vec<u8>> = try_each_resource(
            &mut jar,
            |_| true,
            1024,
            &mut budget,
            |name, bytes| {
                seen.push(name.to_string());
                name.ends_with("dcx.xml").then(|| bytes.to_vec())
            },
        );
        assert_eq!(found.as_deref(), Some(&xml[..]));
        assert!(
            !seen.iter().any(|n| n.ends_with(".class")),
            "a .class entry must never be offered as a resource"
        );
    }

    // An entry the name filter rejects is never inflated: it costs no budget.
    #[test]
    fn unwanted_resources_are_not_inflated() {
        let payload = vec![0u8; 64 * 1024];
        let mut jar = open(build_stored_zip(
            "menu/bg.png",
            &payload,
            payload.len() as u32,
        ));
        let mut budget = 1_000_000u64;
        let r: Option<()> = try_each_resource(
            &mut jar,
            |n| n.ends_with(".xml"),
            1024,
            &mut budget,
            |_, _| Some(()),
        );
        assert!(r.is_none());
        assert_eq!(budget, 1_000_000, "a filtered-out entry must not be read");
    }

    // Inflated bytes are charged to the shared budget; once it is spent the walk
    // stops and the truncated entry is not offered.
    #[test]
    fn budget_bounds_total_inflation() {
        let mut payload = MINIMAL_CLASS.to_vec();
        payload.extend(std::iter::repeat_n(0u8, 4096));
        let mut jar = open(build_stored_zip(
            "Big.class",
            &payload,
            payload.len() as u32,
        ));
        let mut budget = 100u64;
        let mut visited = 0usize;
        let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
            visited += 1;
            None
        });
        assert_eq!((visited, budget), (0, 0));

        // An entry that exactly fills the remaining budget is complete: offered.
        let mut budget = payload.len() as u64;
        let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
            visited += 1;
            None
        });
        assert_eq!((visited, budget), (1, 0));

        let mut budget = 1_000_000u64;
        let _: Option<()> = try_each_class_budgeted(&mut jar, &mut budget, |_, _| {
            visited += 1;
            None
        });
        assert_eq!(visited, 2);
        assert_eq!(budget, 1_000_000 - payload.len() as u64);
    }

    // Counts sectors read through it.
    struct Counting<'a>(&'a mut crate::udf::fixture::MemDisc, u64);

    impl SectorSource for Counting<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            self.1 += u64::from(count);
            self.0.read_sectors(lba, count, buf, recovery)
        }
    }

    // Framework detection answers from each jar's central directory: a large jar's
    // entry data is never read.
    #[test]
    fn framework_detect_reads_only_the_central_directory() {
        use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
        use std::io::Write as _;
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            w.start_file("assets/bg.png", opts).expect("start_file");
            w.write_all(&vec![0x5Au8; 1024 * 1024]).expect("write");
            for n in [
                "com/bydeluxe/A.class",
                "com/dbp/B.class",
                "com/foxbd/C.class",
            ] {
                w.start_file(n, opts).expect("start_file");
                w.write_all(MINIMAL_CLASS).expect("write");
            }
            w.finish().expect("finish");
        }
        let dir = |name: &str, icb, files, subdirs| DirSpec {
            name: name.to_string(),
            icb_lba: icb,
            dir_data_lba: icb + 1,
            files,
            subdirs,
        };
        let jar_dir = dir(
            "JAR",
            30,
            vec![file_with("00000.jar", 32, 4000, buf, true)],
            vec![],
        );
        let root = dir("", 10, vec![], vec![dir("BDMV", 20, vec![], vec![jar_dir])]);
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        let detects: [fn(&mut dyn SectorSource, &UdfFs) -> bool; 3] = [
            super::super::deluxe::detect,
            super::super::dbp::detect,
            super::super::fox::detect,
        ];
        for detect in detects {
            let mut counting = Counting(&mut disc, 0);
            assert!(detect(&mut counting, &udf));
            assert!(
                counting.1 < 128,
                "read {} sectors of a 512-sector jar",
                counting.1
            );
        }
    }

    // High-ratio deflate entries (each under the per-entry cap) stop being offered
    // past the per-parse cap.
    #[test]
    fn try_each_class_bounds_total_inflation() {
        use std::io::Write as _;
        let mut payload = MINIMAL_CLASS.to_vec();
        payload.extend(std::iter::repeat_n(0u8, 50 * 1024 * 1024));
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            for n in ["A.class", "B.class", "C.class"] {
                w.start_file(n, opts).expect("start_file");
                w.write_all(&payload).expect("write");
            }
            w.finish().expect("finish");
        }
        let mut jar = open(buf);
        let mut visited = 0usize;
        let mut budget = PARSE_INFLATE_BUDGET;
        for_each_class_budgeted(&mut jar, &mut budget, |_, _| visited += 1);
        assert_eq!(visited, 2, "3 x 50 MiB exceeds the 128 MiB inflate budget");
    }
}

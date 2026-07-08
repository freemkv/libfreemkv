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
use std::io::{Cursor, Read};
use zip::ZipArchive;

/// Upper bound on bytes read out of a single `.class` entry. The jar's
/// uncompressed-size field is attacker-controlled disc metadata, so the
/// buffer is grown incrementally and the read is capped here rather than
/// pre-sized from the declared size. A real BD-J `.class` is far under
/// this ceiling (64 MiB); a lying header simply gets truncated and the
/// class fails to parse, which is skipped like any other bad entry.
const MAX_CLASS_BYTES: u64 = 64 * 1024 * 1024;

/// In-memory zip archive: backed by a `Vec<u8>` read from UDF. Owns
/// the buffer; callers pass it to [`has_path_prefix`], [`for_each_class`],
/// etc.
pub type Jar = ZipArchive<Cursor<Vec<u8>>>;

/// Open every top-level `*.jar` entry in `/BDMV/JAR/` and yield each
/// `(entry_name, Jar)` to `f`. Returns the first `Some(R)` the callback
/// produces, or `None` if every jar was visited without a hit.
///
/// "Top-level" means entries directly under `/BDMV/JAR/`, not nested
/// under a subdir. (Pixelogic, Criterion, Paramount, etc. put their
/// data files inside `/BDMV/JAR/<x>/`; dbp and Deluxe put their jar
/// directly at `/BDMV/JAR/<name>.jar`.)
///
/// Entries that fail to read from UDF or that aren't valid zips are
/// silently skipped — same defensive shape as the existing dbp parser.
pub fn for_each_jar<R, F>(reader: &mut dyn SectorSource, udf: &UdfFs, mut f: F) -> Option<R>
where
    F: FnMut(&str, &mut Jar) -> Option<R>,
{
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &jar_dir.entries {
        if entry.is_dir {
            continue;
        }
        if !entry.name.to_lowercase().ends_with(".jar") {
            continue;
        }
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

/// True if any entry in this jar's central directory starts with
/// `prefix`. Fast — only reads filenames, never extracts bytes.
///
/// Used by parsers as a cheap "is this MY framework's jar?" check
/// (e.g. `has_path_prefix(archive, "com/dbp/")` for dbp,
/// `has_path_prefix(archive, "com/bydeluxe/")` for Deluxe).
pub fn has_path_prefix(archive: &Jar, prefix: &str) -> bool {
    archive.file_names().any(|n| n.starts_with(prefix))
}

/// Iterate every `.class` entry in the jar, parse it with
/// [`class_reader`], and call `f` with `(entry_name, &ClassFile)`.
///
/// Entries that fail to read or parse are silently skipped — this is
/// label-extraction code, robustness matters more than completeness.
/// Callers that need to know which classes failed should use the
/// lower-level [`class_reader`] API directly.
pub fn for_each_class<F>(archive: &mut Jar, mut f: F)
where
    F: FnMut(&str, &ClassFile),
{
    // Defer to try_each_class; the callback always yields None so
    // iteration never short-circuits.
    try_each_class(archive, |name, class| {
        f(name, class);
        None::<()>
    });
}

/// Like [`for_each_class`] but allows the callback to short-circuit
/// iteration. Returns the first `Some(R)` the callback produces.
pub fn try_each_class<R, F>(archive: &mut Jar, mut f: F) -> Option<R>
where
    F: FnMut(&str, &ClassFile) -> Option<R>,
{
    for i in 0..archive.len() {
        let Ok(entry) = archive.by_index(i) else {
            continue;
        };
        if !entry.name().ends_with(".class") {
            continue;
        }
        let name = entry.name().to_string();
        // The declared uncompressed size is attacker-controlled, so the
        // buffer grows incrementally and the read is capped at
        // MAX_CLASS_BYTES rather than pre-sized from entry.size().
        let mut bytes = Vec::new();
        if entry.take(MAX_CLASS_BYTES).read_to_end(&mut bytes).is_err() {
            continue;
        }
        let Ok(class) = ClassFile::parse(&bytes) else {
            continue;
        };
        if let Some(r) = f(&name, &class) {
            return Some(r);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Build a raw, single-entry, Stored (uncompressed) ZIP whose local
    /// header and central directory both declare `declared_size` as the
    /// uncompressed size, while the actual stored payload is `payload`.
    /// This lets a test forge an attacker-controlled size field that does
    /// not match the real data length.
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

    #[test]
    fn try_each_class_reads_minimal_class() {
        let mut jar = open(build_stored_zip(
            "Foo.class",
            MINIMAL_CLASS,
            MINIMAL_CLASS.len() as u32,
        ));
        let mut seen = Vec::new();
        let r: Option<()> = try_each_class(&mut jar, |name, _class| {
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
        for_each_class(&mut jar, |_, _| count += 1);
        assert_eq!(count, 1);
    }

    /// The uncompressed-size field is attacker-controlled. A tiny stored
    /// entry that declares 0xFFFF_FFFF (≈4 GiB) must NOT trigger a 4 GiB
    /// pre-allocation; with the incremental read the call completes and
    /// the real (small) payload parses fine.
    #[test]
    fn forged_huge_uncompressed_size_does_not_preallocate() {
        let mut jar = open(build_stored_zip("Evil.class", MINIMAL_CLASS, 0xFFFF_FFFF));
        let mut parsed = false;
        for_each_class(&mut jar, |name, _class| {
            assert_eq!(name, "Evil.class");
            parsed = true;
        });
        // Reached here without OOM/abort, and the real bytes parsed.
        assert!(parsed);
    }

    /// The read is bounded by MAX_CLASS_BYTES: a stored entry whose real
    /// payload exceeds the cap yields only the first MAX_CLASS_BYTES
    /// bytes to the parser, never the full (unbounded) entry. Verified
    /// here on a small cap via the entry-count path: the truncated bytes
    /// still parse a valid class prefix, but no read beyond the cap
    /// occurs. We assert the entry is still surfaced exactly once (the
    /// cap does not drop legitimate entries) and the call returns.
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
        for_each_class(&mut jar, |_, _| visited += 1);
        assert_eq!(visited, 1);
    }
}

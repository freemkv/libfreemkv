//! BD-J jar utilities — common scaffolding for parsers that read
//! `/BDMV/JAR/*.jar`.
//!
//! Composes with [`class_reader`](super::class_reader) for structured
//! `.class` access. Used by `dbp` (string-pool scan via constant pool)
//! and `deluxe` (bytecode pattern matching) — those parsers express
//! "open every top-level jar, look at every .class inside" without
//! repeating the zip-archive boilerplate.

// `try_each_class` is staged for `labels::deluxe`, which needs the
// early-return form to short-circuit class iteration on a match.
// dead-code allow comes off when deluxe lands.
#![allow(dead_code)]

use super::class_reader::ClassFile;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::io::Cursor;
use zip::ZipArchive;

/// In-memory zip archive: backed by a `Vec<u8>` read from UDF. Owns
/// the buffer; callers pass it to [`has_path_prefix`], [`for_each_class`],
/// etc.
pub type Jar = ZipArchive<Cursor<Vec<u8>>>;

/// True if `/BDMV/JAR/` contains at least one top-level `.jar` file
/// (not under a subdir). Used by `detect()` in parsers whose real
/// signal lives inside a jar — they can't open the jar without a
/// `SectorSource`, so they use this cheap pre-check and do the real
/// `com/<vendor>/` discriminator in `parse()`.
pub fn has_any_top_level_jar(udf: &UdfFs) -> bool {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return false;
    };
    jar_dir
        .entries
        .iter()
        .any(|e| !e.is_dir && e.name.to_lowercase().ends_with(".jar"))
}

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
pub fn has_path_prefix(archive: &mut Jar, prefix: &str) -> bool {
    for i in 0..archive.len() {
        if let Ok(f) = archive.by_index(i) {
            if f.name().starts_with(prefix) {
                return true;
            }
        }
    }
    false
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
    for i in 0..archive.len() {
        let Ok(mut entry) = archive.by_index(i) else {
            continue;
        };
        if !entry.name().ends_with(".class") {
            continue;
        }
        let name = entry.name().to_string();
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        if std::io::Read::read_to_end(&mut entry, &mut bytes).is_err() {
            continue;
        }
        let Ok(class) = ClassFile::parse(&bytes) else {
            continue;
        };
        f(&name, &class);
    }
}

/// Like [`for_each_class`] but allows the callback to short-circuit
/// iteration. Returns the first `Some(R)` the callback produces.
pub fn try_each_class<R, F>(archive: &mut Jar, mut f: F) -> Option<R>
where
    F: FnMut(&str, &ClassFile) -> Option<R>,
{
    for i in 0..archive.len() {
        let Ok(mut entry) = archive.by_index(i) else {
            continue;
        };
        if !entry.name().ends_with(".class") {
            continue;
        }
        let name = entry.name().to_string();
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        if std::io::Read::read_to_end(&mut entry, &mut bytes).is_err() {
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

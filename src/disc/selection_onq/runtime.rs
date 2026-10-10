//! Exact runtime-code identity, separate from authored resources. A matching
//! inventory identifies reviewed code; it does not prove QCO/data effects.
use super::{Reject, Result};
use crate::labels::class_reader::ClassFile;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};

const MAX_ARCHIVES: usize = 8;
const MAX_ARCHIVE_BYTES: usize = 32 * 1024 * 1024;
const MAX_CODE_BYTES: usize = 32 * 1024 * 1024;
const MAX_CLASS_BYTES: usize = 2 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClassSet(BTreeMap<String, [u8; 32]>);

/// Executable-code version only; this token does not certify authored data,
/// stack safety, native effects, or whole-presentation playback.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CodeVersion {
    OnqUhdV1,
}

pub(super) fn version(runtime: &ClassSet, dispatcher: &ClassSet) -> Result<CodeVersion> {
    const RUNTIME: [u8; 32] = [
        0xc0, 0x9d, 0x1e, 0x36, 0xd7, 0x99, 0x9e, 0xc0, 0xf8, 0xa1, 0x61, 0xe6, 0x90, 0x91, 0x3d,
        0xed, 0x86, 0xea, 0x31, 0x20, 0x01, 0x0c, 0x7d, 0x16, 0x6b, 0x0b, 0x87, 0x66, 0x53, 0x88,
        0xcd, 0x5a,
    ];
    const DISPATCHER: [u8; 32] = [
        0x25, 0x0e, 0xb4, 0xd4, 0x56, 0xdf, 0x0c, 0x61, 0xed, 0x72, 0x2e, 0xd2, 0xb4, 0x6d, 0xd2,
        0xff, 0xb3, 0x51, 0x0f, 0xad, 0xe8, 0x3b, 0x30, 0xf5, 0xc1, 0xc0, 0x19, 0xf0, 0xb9, 0xe1,
        0xf6, 0xb0,
    ];
    if runtime.fingerprint() != RUNTIME || dispatcher.fingerprint() != DISPATCHER {
        return Err(Reject::Unsupported);
    }
    Ok(CodeVersion::OnqUhdV1)
}

impl ClassSet {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"freemkv/onq/runtime-class-set/v1\0");
        hash.update((self.0.len() as u32).to_be_bytes());
        for (name, digest) in &self.0 {
            hash.update((name.len() as u32).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update(digest);
        }
        hash.finalize().into()
    }

    /// `archives` must be the complete effective bootstrap/classpath set, not
    /// every JAR found on the disc. Bootstrap validation owns that obligation.
    pub fn read(archives: &[&[u8]]) -> Result<Self> {
        if archives.is_empty() || archives.len() > MAX_ARCHIVES {
            return Err(Reject::Budget);
        }
        let mut classes = BTreeMap::new();
        let mut code_bytes = 0usize;
        let mut entries = 0usize;
        for bytes in archives {
            if bytes.len() > MAX_ARCHIVE_BYTES {
                return Err(Reject::Budget);
            }
            let raw_count = super::archive::entries(bytes, MAX_ENTRIES)?;
            let mut archive =
                zip::ZipArchive::new(Cursor::new(bytes)).map_err(|_| Reject::Invalid)?;
            if archive.len() != raw_count {
                return Err(Reject::Invalid);
            }
            entries = entries.checked_add(raw_count).ok_or(Reject::Budget)?;
            if entries > MAX_ENTRIES {
                return Err(Reject::Budget);
            }
            let mut names = BTreeSet::new();
            let mut manifest_seen = false;
            for index in 0..archive.len() {
                let file = archive.by_index(index).map_err(|_| Reject::Invalid)?;
                let name = file.name();
                if name.is_empty()
                    || name.starts_with('/')
                    || name.contains('\\')
                    || name.split('/').any(|part| part == "." || part == "..")
                    || !names.insert(name.to_owned())
                {
                    return Err(Reject::Invalid);
                }
                // Alternate class versions/service loaders are not part of
                // this finite runtime contract.
                if name.starts_with("META-INF/versions/") || name.starts_with("META-INF/services/")
                {
                    return Err(Reject::Unsupported);
                }
                if name.eq_ignore_ascii_case("META-INF/MANIFEST.MF") {
                    if manifest_seen || file.size() > 1024 * 1024 {
                        return Err(Reject::Unsupported);
                    }
                    manifest_seen = true;
                    let mut bytes = Vec::new();
                    file.take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| Reject::Invalid)?;
                    manifest(&bytes)?;
                    continue;
                }
                let Some(binary_name) = name.strip_suffix(".class") else {
                    continue;
                };
                if binary_name.is_empty()
                    || binary_name.split('/').any(str::is_empty)
                    || file.size() > MAX_CLASS_BYTES as u64
                {
                    return Err(Reject::Invalid);
                }
                let binary_name = binary_name.to_owned();
                let mut code = Vec::new();
                file.take((MAX_CLASS_BYTES + 1) as u64)
                    .read_to_end(&mut code)
                    .map_err(|_| Reject::Invalid)?;
                code_bytes = code_bytes.checked_add(code.len()).ok_or(Reject::Budget)?;
                if code.len() > MAX_CLASS_BYTES || code_bytes > MAX_CODE_BYTES {
                    return Err(Reject::Budget);
                }
                let parsed = ClassFile::parse(&code).map_err(|_| Reject::Invalid)?;
                if parsed.this_class_name() != Some(binary_name.as_str()) {
                    return Err(Reject::Invalid);
                }
                // Includes the constant pool: changed resolved members cannot
                // hide behind unchanged Code bytes/constant-pool indices.
                if classes
                    .insert(binary_name, Sha256::digest(&code).into())
                    .is_some()
                {
                    return Err(Reject::Invalid);
                }
            }
        }
        if classes.is_empty() {
            return Err(Reject::Unsupported);
        }
        Ok(Self(classes))
    }

    /// Exact set equality rejects missing, added, shadowed and modified code.
    /// Only a statically reviewed manifest may be passed by the producer.
    #[cfg(test)]
    pub fn matches(&self, reviewed: &[(&str, [u8; 32])]) -> Result<()> {
        let mut expected = BTreeMap::new();
        for &(name, digest) in reviewed {
            if expected.insert(name.to_owned(), digest).is_some() {
                return Err(Reject::Invalid);
            }
        }
        if !expected.is_empty() && self.0 == expected {
            Ok(())
        } else {
            Err(Reject::Unsupported)
        }
    }
}

/// The reviewed JARs use only version and per-entry digest metadata. Reject
/// classpath/agent/alternate-version attributes rather than ignoring them.
fn manifest(bytes: &[u8]) -> Result<()> {
    if bytes.len() > 1024 * 1024 || !bytes.is_ascii() || bytes.contains(&0) {
        return Err(Reject::Invalid);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| Reject::Invalid)?;
    let mut main = true;
    let mut keys = BTreeSet::new();
    let mut previous = false;
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.contains('\r') {
            return Err(Reject::Invalid);
        }
        if line.is_empty() {
            if main && !keys.contains("Manifest-Version") {
                return Err(Reject::Invalid);
            }
            main = false;
            keys.clear();
            previous = false;
            continue;
        }
        if line.starts_with(' ') {
            if main || !previous {
                return Err(Reject::Unsupported);
            }
            continue;
        }
        let (key, value) = line.split_once(": ").ok_or(Reject::Invalid)?;
        if !keys.insert(key) {
            return Err(Reject::Invalid);
        }
        if main {
            if key != "Manifest-Version" || value != "1.0" {
                return Err(Reject::Unsupported);
            }
        } else if !matches!(key, "Name" | "SHA1-Digest" | "SHA-256-Digest") {
            return Err(Reject::Unsupported);
        }
        previous = true;
    }
    if main {
        return Err(Reject::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    #[ignore = "local extracted runtime inventory; no JVM execution"]
    fn local_runtime_inventory_is_unambiguous() {
        let path = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
        let runtime = std::fs::read(&path).unwrap();
        let dispatcher = std::fs::read(path.with_file_name("00000.jar")).unwrap();
        let extension = std::fs::read(path.with_file_name("44444.jar")).unwrap();
        for (name, bytes) in [
            ("runtime", &runtime),
            ("dispatcher", &dispatcher),
            ("extension", &extension),
        ] {
            eprintln!(
                "{name}: preflight {:?}",
                super::super::archive::entries(bytes, MAX_ENTRIES)
            );
            eprintln!(
                "{name}: class inventory {:?}",
                ClassSet::read(&[bytes]).map(|v| v.0.len())
            );
        }
        // Separate BDJO applications have separate effective classpaths. The
        // dispatcher classes also present in EntryPoint's JAR are not shadows
        // across those application namespaces.
        let inventory = ClassSet::read(&[&runtime, &extension]).unwrap();
        let dispatcher_inventory = ClassSet::read(&[&dispatcher]).unwrap();
        assert_eq!(
            version(&inventory, &dispatcher_inventory),
            Ok(CodeVersion::OnqUhdV1)
        );
        assert_eq!(
            version(&dispatcher_inventory, &inventory),
            Err(Reject::Unsupported)
        );
        for original in [&inventory, &dispatcher_inventory] {
            for mutation in 0..3 {
                let mut changed = ClassSet(original.0.clone());
                let name = changed.0.keys().next().unwrap().clone();
                match mutation {
                    0 => {
                        changed.0.remove(&name);
                    }
                    1 => {
                        changed
                            .0
                            .insert("unreviewed/AdditionalClass".into(), [0; 32]);
                    }
                    _ => {
                        changed.0.get_mut(&name).unwrap()[0] ^= 1;
                    }
                }
                let result = if std::ptr::eq(original, &inventory) {
                    version(&changed, &dispatcher_inventory)
                } else {
                    version(&inventory, &changed)
                };
                assert_eq!(result, Err(Reject::Unsupported));
            }
        }
        assert!(!inventory.0.is_empty());
        eprintln!(
            "unambiguous runtime class count: {} (not an approved manifest)",
            inventory.0.len()
        );
        assert!(!dispatcher_inventory.0.is_empty());
        // Separate applications deliberately contain incompatible obfuscated
        // class names. They must not be flattened into one classpath.
        assert_ne!(dispatcher_inventory.0.get("acv"), inventory.0.get("acv"));
        eprintln!(
            "runtime code-set fingerprint {:02x?}",
            inventory.fingerprint()
        );
        eprintln!(
            "dispatcher code-set fingerprint {:02x?}",
            dispatcher_inventory.fingerprint()
        );
    }

    fn class(name: &str) -> Vec<u8> {
        let mut bytes = vec![0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 49, 0, 5, 1];
        bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&[7, 0, 1, 1, 0, 16]);
        bytes.extend_from_slice(b"java/lang/Object");
        bytes.extend_from_slice(&[7, 0, 3, 0, 0x21, 0, 2, 0, 4]);
        bytes.extend_from_slice(&[0; 8]);
        bytes
    }

    fn jar(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for &(name, bytes) in entries {
            zip.start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn exact_code_inventory_excludes_authored_data_but_not_constant_pool() {
        let code = class("Runtime");
        let reviewed = [("Runtime", Sha256::digest(&code).into())];
        for data in [&b"two episodes"[..], &b"four different episodes"[..]] {
            let bytes = jar(&[("Runtime.class", &code), ("FS.QCO", data)]);
            ClassSet::read(&[&bytes])
                .unwrap()
                .matches(&reviewed)
                .unwrap();
        }
        let changed = class("Mutated");
        let bytes = jar(&[("Mutated.class", &changed)]);
        assert!(
            ClassSet::read(&[&bytes])
                .unwrap()
                .matches(&reviewed)
                .is_err()
        );
        let mut changed_pool = code.clone();
        let offset = changed_pool
            .windows(6)
            .position(|w| w == b"Object")
            .unwrap();
        changed_pool[offset + 4] = b'C';
        let bytes = jar(&[("Runtime.class", &changed_pool)]);
        assert!(
            ClassSet::read(&[&bytes])
                .unwrap()
                .matches(&reviewed)
                .is_err()
        );
        let extra = jar(&[("Runtime.class", &code), ("Mutated.class", &changed)]);
        assert!(
            ClassSet::read(&[&extra])
                .unwrap()
                .matches(&reviewed)
                .is_err()
        );
        assert!(ClassSet::read(&[&jar(&[("FS.QCO", b"no classes")])]).is_err());
    }

    #[test]
    fn code_set_fingerprint_has_order_independent_unambiguous_membership() {
        let a = class("A");
        let b = class("B");
        let one = jar(&[("A.class", &a), ("B.class", &b)]);
        let reversed = jar(&[
            ("B.class", &b),
            ("A.class", &a),
            ("FS.QCO", b"different data"),
        ]);
        let expected = ClassSet::read(&[&one]).unwrap().fingerprint();
        assert_eq!(
            ClassSet::read(&[&reversed]).unwrap().fingerprint(),
            expected
        );
        assert_eq!(
            ClassSet::read(&[&jar(&[("B.class", &b)]), &jar(&[("A.class", &a)])])
                .unwrap()
                .fingerprint(),
            expected
        );
        assert_ne!(
            ClassSet::read(&[&jar(&[("A.class", &a)])])
                .unwrap()
                .fingerprint(),
            expected
        );
        let changed = class("C");
        assert_ne!(
            ClassSet::read(&[&jar(&[("A.class", &a), ("C.class", &changed)])])
                .unwrap()
                .fingerprint(),
            expected
        );
    }

    #[test]
    fn manifest_cannot_extend_the_reviewed_executable_classpath() {
        let code = class("Runtime");
        let good =
            b"Manifest-Version: 1.0\r\n\r\nName: Runtime.class\r\nSHA1-Digest: ignored\r\n\r\n";
        let clean = jar(&[("Runtime.class", &code), ("META-INF/MANIFEST.MF", good)]);
        ClassSet::read(&[&clean]).unwrap();
        for attr in [
            "Class-Path: evil.jar",
            "Class-Path: ev\r\n il.jar",
            "Multi-Release: true",
            "Launcher-Agent-Class: Evil",
        ] {
            let body = format!("Manifest-Version: 1.0\r\n{attr}\r\n\r\n");
            let bad = jar(&[
                ("Runtime.class", &code),
                ("META-INF/MANIFEST.MF", body.as_bytes()),
            ]);
            assert!(ClassSet::read(&[&bad]).is_err());
        }
        let aliases = jar(&[
            ("Runtime.class", &code),
            ("META-INF/MANIFEST.MF", good),
            ("meta-inf/manifest.mf", good),
        ]);
        assert!(ClassSet::read(&[&aliases]).is_err());
        for bytes in [
            &b""[..],
            &b"Manifest-Version: 1.0\0\n\n"[..],
            &b"Manifest-Version: 1.0\n stray\n\n"[..],
        ] {
            assert!(manifest(bytes).is_err());
        }
    }

    #[test]
    fn class_shadowing_aliases_and_unreviewed_loading_fail_closed() {
        let code = class("Runtime");
        let bytes = jar(&[("Runtime.class", &code)]);
        assert!(ClassSet::read(&[&bytes, &bytes]).is_err());
        for name in [
            "Other.class",
            "../Runtime.class",
            "/Runtime.class",
            "x/../Runtime.class",
        ] {
            assert!(ClassSet::read(&[&jar(&[(name, &code)])]).is_err());
        }
        for name in [
            "META-INF/versions/9/Runtime.class",
            "META-INF/services/Factory",
        ] {
            assert!(ClassSet::read(&[&jar(&[("Runtime.class", &code), (name, &code)])]).is_err());
        }
        assert!(ClassSet::read(&[&jar(&[("Runtime.class", b"malformed")])]).is_err());
        let set = ClassSet::read(&[&bytes]).unwrap();
        let entry = ("Runtime", Sha256::digest(&code).into());
        assert!(set.matches(&[entry, entry]).is_err());
        assert!(set.matches(&[]).is_err());
    }

    #[test]
    fn raw_duplicate_entries_reject_before_zip_name_index_coalesces_them() {
        let code = class("Runtime");
        let different = class("Otherxx");
        let mut bytes = jar(&[("Otherxx.class", &different), ("Runtime.class", &code)]);
        let locations: Vec<_> = bytes
            .windows(13)
            .enumerate()
            .filter_map(|(i, name)| (name == b"Otherxx.class").then_some(i))
            .collect();
        assert_eq!(locations.len(), 2); // local header and central directory
        for i in locations {
            bytes[i..i + 13].copy_from_slice(b"Runtime.class");
        }
        // This is the dangerous library behavior that preflight must precede.
        assert_eq!(zip::ZipArchive::new(Cursor::new(&bytes)).unwrap().len(), 1);
        assert!(ClassSet::read(&[&bytes]).is_err());
    }

    #[test]
    fn archive_truncation_and_local_name_disagreement_reject() {
        let bytes = jar(&[("Runtime.class", &class("Runtime"))]);
        for n in 0..bytes.len() {
            assert!(ClassSet::read(&[&bytes[..n]]).is_err());
        }
        let mut bad = bytes.clone();
        bad[30] = b'X';
        assert!(ClassSet::read(&[&bad]).is_err());
        assert!(super::super::archive::entries(&bytes, 0).is_err());
    }

    #[test]
    fn redundant_local_64bit_sizes_must_match_central_sizes() {
        let mut bytes = jar(&[("Runtime.class", &class("Runtime"))]);
        let compressed = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
        let uncompressed = u32::from_le_bytes(bytes[22..26].try_into().unwrap());
        let name_len = u16::from_le_bytes(bytes[26..28].try_into().unwrap()) as usize;
        assert_eq!(&bytes[28..30], &[0, 0]);
        let mut extra = vec![1, 0, 16, 0];
        extra.extend_from_slice(&u64::from(uncompressed).to_le_bytes());
        extra.extend_from_slice(&u64::from(compressed).to_le_bytes());
        bytes.splice(30 + name_len..30 + name_len, extra);
        bytes[28..30].copy_from_slice(&20u16.to_le_bytes());
        let eocd = bytes.len() - 22;
        let offset = u32::from_le_bytes(bytes[eocd + 16..eocd + 20].try_into().unwrap()) + 20;
        bytes[eocd + 16..eocd + 20].copy_from_slice(&offset.to_le_bytes());
        assert!(ClassSet::read(&[&bytes]).is_ok());
        bytes[30 + name_len + 4] ^= 1;
        assert!(ClassSet::read(&[&bytes]).is_err());
    }
}

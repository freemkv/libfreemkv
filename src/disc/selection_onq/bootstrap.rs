//! Bounded active-application inputs for the reviewed onQ runtime. No search
//! through unrelated JARs and no configuration fallback after a parse failure.
use super::binary::Reader;
use super::{Reject, Result};
use std::collections::{BTreeMap, BTreeSet};

const ENTRY: &str = "com.ensequence.client.bluray.EntryPoint";
const DISPATCHER: &str = "com.ensequence.client.bluray.TitleChangeDispatcher";

fn jar_id(value: &[u8]) -> Result<String> {
    if value.len() != 5 || !value.iter().all(u8::is_ascii_digit) {
        return Err(Reject::Unsupported);
    }
    String::from_utf8(value.to_vec()).map_err(|_| Reject::Invalid)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ActiveIndex {
    pub first_play: u16,
    pub top_menu: String,
    pub bdjos: BTreeSet<String>,
}

pub(super) fn index(data: &[u8]) -> Result<ActiveIndex> {
    let mut r = Reader::new(data)?;
    if r.take(8)? != b"INDX0300" {
        return Err(Reject::Unsupported);
    }
    let start = r.u32()? as usize;
    if start < 40 {
        return Err(Reject::Invalid);
    }
    let mut table = r.at(start)?;
    let length = table.u32()? as usize;
    let end = table.pos.checked_add(length).ok_or(Reject::Budget)?;
    data.get(table.pos..end).ok_or(Reject::Truncated)?;
    let mut bdjos = BTreeSet::new();
    let mut object = |record: &[u8]| -> Result<Option<String>> {
        match record[0] >> 6 {
            1 => Ok(None),
            2 => {
                let name = jar_id(&record[6..11])?;
                if record[11] != 0 {
                    return Err(Reject::Unsupported);
                }
                bdjos.insert(name.clone());
                Ok(Some(name))
            }
            _ => Err(Reject::Unsupported),
        }
    };
    let first_record = table.take(12)?;
    if first_record[0] >> 6 != 1 {
        return Err(Reject::Unsupported);
    }
    let first_play = u16::from_be_bytes([first_record[6], first_record[7]]);
    if first_play == u16::MAX {
        return Err(Reject::Unsupported);
    }
    let top_menu = object(table.take(12)?)?.ok_or(Reject::Unsupported)?;
    let count = table.u16()? as usize;
    if count == 0 || count > 4096 {
        return Err(Reject::Budget);
    }
    for _ in 0..count {
        object(table.take(12)?)?;
    }
    if table.pos != end {
        return Err(Reject::Invalid);
    }
    Ok(ActiveIndex {
        first_play,
        top_menu,
        bdjos,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Applications {
    pub runtime: String,
    pub extension: String,
    pub dispatcher: String,
    pub runtime_identity: (u32, u16),
    pub dispatcher_identity: (u32, u16),
}

fn app_string(r: &mut Reader<'_>) -> Result<String> {
    let n = r.u8()? as usize;
    let bytes = r.take(n)?;
    if !bytes.is_ascii() || bytes.contains(&0) {
        return Err(Reject::Unsupported);
    }
    if n.is_multiple_of(2) && r.u8()? != 0 {
        return Err(Reject::Invalid);
    }
    String::from_utf8(bytes.to_vec()).map_err(|_| Reject::Invalid)
}

/// Strict descriptor extents supplement the general-purpose BDJO label reader:
/// authority must not depend on ignored section addresses or descriptor lengths.
pub(super) fn applications(data: &[u8]) -> Result<Applications> {
    let mut r = Reader::new(data)?;
    if r.take(8)? != b"BDJO0300" {
        return Err(Reject::Unsupported);
    }
    let mut sections = Vec::new();
    for _ in 0..6 {
        sections.push(r.u32()? as usize);
    }
    if r.take(16)?.iter().any(|v| *v != 0) || sections[0] != 48 {
        return Err(Reject::Unsupported);
    }
    for pair in sections.windows(2).take(4) {
        let length = r.at(pair[0])?.u32()? as usize;
        if pair[0].checked_add(4 + length) != Some(pair[1]) {
            return Err(Reject::Invalid);
        }
    }
    // The supported profile has no key-interest override and only the default
    // file-access root. Application code/config determine resources below it.
    if sections[4].checked_add(4) != Some(sections[5])
        || r.at(sections[4])?.u32()? != 0
        || data.get(sections[5]..) != Some(&[0, 1, b'.', 0][..])
    {
        return Err(Reject::Unsupported);
    }
    let mut cache = r.at(sections[1] + 4)?;
    let count = cache.u8()? as usize;
    if cache.u8()? != 0 {
        return Err(Reject::Invalid);
    }
    let mut cached = BTreeSet::new();
    for _ in 0..count {
        if cache.u8()? != 1 {
            return Err(Reject::Unsupported);
        }
        let id = jar_id(cache.take(5)?)?;
        if cache.take(3)? != b"*.*" || cache.take(3)? != [0, 0, 0] || !cached.insert(id) {
            return Err(Reject::Unsupported);
        }
    }
    if cache.pos != sections[2] {
        return Err(Reject::Invalid);
    }
    // Access-all, no automatically started playlist outside the runtime path.
    if data.get(sections[2] + 4..sections[3]) != Some(&[0, 0x10, 0, 0][..]) {
        return Err(Reject::Unsupported);
    }
    let mut apps = r.at(sections[3] + 4)?;
    if apps.u8()? != 2 || apps.u8()? != 0 {
        return Err(Reject::Unsupported);
    }
    let mut identities = BTreeSet::new();
    let mut runtime = None;
    let mut dispatcher = None;
    for _ in 0..2 {
        if apps.u8()? != 1 || apps.u8()? != 0x10 {
            return Err(Reject::Unsupported);
        }
        let identity = (apps.u32()?, apps.u16()?);
        if !identities.insert(identity) || apps.u16()? != 0 {
            return Err(Reject::Invalid);
        }
        let length = apps.u32()? as usize;
        let end = apps.pos.checked_add(length).ok_or(Reject::Budget)?;
        if end > sections[4] || apps.u32()? != 0 {
            return Err(Reject::Invalid);
        }
        let profiles = apps.u16()?;
        if profiles != 0x1000 {
            return Err(Reject::Unsupported);
        }
        if apps.take(6)? != [0, 6, 1, 0, 0, 0] {
            return Err(Reject::Unsupported);
        }
        apps.u8()?; // priority does not select a different application namespace
        let binding_visibility = apps.u8()?;
        let names = apps.u16()? as usize;
        apps.take(names)?;
        if !names.is_multiple_of(2) {
            apps.take(1)?;
        }
        app_string(&mut apps)?; // icon locator
        apps.u16()?; // icon flags
        let base = jar_id(app_string(&mut apps)?.as_bytes())?;
        let extension = app_string(&mut apps)?;
        let entry = app_string(&mut apps)?;
        // No BDJO argument may override effective config or module loading.
        if apps.u8()? != 0 || apps.u8()? != 0 || apps.pos != end {
            return Err(Reject::Unsupported);
        }
        match entry.as_str() {
            ENTRY if runtime.is_none() && binding_visibility == 0x50 => {
                let extension = jar_id(
                    extension
                        .strip_prefix('/')
                        .ok_or(Reject::Unsupported)?
                        .as_bytes(),
                )?;
                if extension == base {
                    return Err(Reject::Invalid);
                }
                runtime = Some((base, extension, identity));
            }
            DISPATCHER
                if dispatcher.is_none() && extension.is_empty() && binding_visibility == 0xd0 =>
            {
                dispatcher = Some((base, identity))
            }
            _ => return Err(Reject::Unsupported),
        }
    }
    if apps.pos != sections[4] {
        return Err(Reject::Invalid);
    }
    let (runtime, extension, runtime_identity) = runtime.ok_or(Reject::Unsupported)?;
    let (dispatcher, dispatcher_identity) = dispatcher.ok_or(Reject::Unsupported)?;
    let expected: BTreeSet<_> = [&runtime, &extension, &dispatcher]
        .into_iter()
        .cloned()
        .collect();
    if expected.len() != 3 || expected != cached {
        return Err(Reject::Unsupported);
    }
    Ok(Applications {
        runtime,
        extension,
        dispatcher,
        runtime_identity,
        dispatcher_identity,
    })
}

/// Strict Java-properties subset: no escapes, continuations, duplicate keys,
/// custom modules, folder/config overrides, or debug console entry paths.
pub(super) fn configuration(data: &[u8], runtime: &str) -> Result<String> {
    if data.len() > 16384 || !data.is_ascii() || data.contains(&b'\\') {
        return Err(Reject::Unsupported);
    }
    let text = std::str::from_utf8(data).map_err(|_| Reject::Invalid)?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or(Reject::Unsupported)?;
        if key.trim() != key || value.trim() != value || values.insert(key, value).is_some() {
            return Err(Reject::Unsupported);
        }
    }
    for (key, expected) in [
        ("debug.enableConsole", "false"),
        ("debug.showFatal", "false"),
        ("debug.showWatermark", "false"),
        ("log.default.appenders", "none"),
    ] {
        if values.remove(key) != Some(expected) {
            return Err(Reject::Unsupported);
        }
    }
    let metadata = values
        .remove("app.playlist-meta.file")
        .ok_or(Reject::Unsupported)?;
    let prefix = format!("file:///vfs/BDMV/JAR/{runtime}/");
    let leaf = metadata.strip_prefix(&prefix).ok_or(Reject::Unsupported)?;
    if !values.is_empty()
        || leaf.is_empty()
        || !leaf
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
        || matches!(leaf, "." | "..")
    {
        return Err(Reject::Unsupported);
    }
    Ok(format!("/BDMV/JAR/{runtime}/{leaf}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short(out: &mut Vec<u8>, value: u16) {
        out.extend_from_slice(&value.to_be_bytes());
    }
    fn word(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_be_bytes());
    }
    fn string(out: &mut Vec<u8>, value: &str) {
        out.push(value.len() as u8);
        out.extend_from_slice(value.as_bytes());
        if value.len().is_multiple_of(2) {
            out.push(0);
        }
    }
    fn bdjo(runtime: &str, extension: &str, dispatcher: &str) -> Vec<u8> {
        let mut out = b"BDJO0300".to_vec();
        out.resize(48, 0);
        let mut sections = Vec::new();
        sections.push(out.len());
        word(&mut out, 10);
        out.extend_from_slice(&[0; 10]);
        sections.push(out.len());
        word(&mut out, 38);
        out.extend_from_slice(&[3, 0]);
        for id in [dispatcher, runtime, extension] {
            out.push(1);
            out.extend_from_slice(id.as_bytes());
            out.extend_from_slice(b"*.*\0\0\0");
        }
        sections.push(out.len());
        word(&mut out, 4);
        out.extend_from_slice(&[0, 0x10, 0, 0]);
        sections.push(out.len());
        let length_at = out.len();
        word(&mut out, 0);
        out.extend_from_slice(&[2, 0]);
        for (i, entry, base, extension, binding) in [
            (1, DISPATCHER, dispatcher, String::new(), 0xd0),
            (2, ENTRY, runtime, format!("/{extension}"), 0x50),
        ] {
            let mut descriptor = vec![0; 4];
            short(&mut descriptor, 0x1000);
            descriptor.extend_from_slice(&[0, 6, 1, 0, 0, 0, 1, binding]);
            short(&mut descriptor, 0);
            string(&mut descriptor, "");
            short(&mut descriptor, 0);
            string(&mut descriptor, base);
            string(&mut descriptor, &extension);
            string(&mut descriptor, entry);
            descriptor.extend_from_slice(&[0, 0]);
            out.extend_from_slice(&[1, 0x10]);
            word(&mut out, 123);
            short(&mut out, i);
            short(&mut out, 0);
            word(&mut out, descriptor.len() as u32);
            out.extend_from_slice(&descriptor);
        }
        let length = (out.len() - length_at - 4) as u32;
        out[length_at..length_at + 4].copy_from_slice(&length.to_be_bytes());
        sections.push(out.len());
        word(&mut out, 0);
        sections.push(out.len());
        out.extend_from_slice(&[0, 1, b'.', 0]);
        for (i, offset) in sections.into_iter().enumerate() {
            out[8 + i * 4..12 + i * 4].copy_from_slice(&(offset as u32).to_be_bytes());
        }
        out
    }

    #[test]
    fn indexed_application_roles_accept_renumbering_not_extra_or_merged_apps() {
        for (runtime, extension, dispatcher) in
            [("12345", "67890", "11111"), ("98765", "11111", "22222")]
        {
            let data = bdjo(runtime, extension, dispatcher);
            let apps = applications(&data).unwrap();
            assert_eq!(
                (
                    apps.runtime.as_str(),
                    apps.extension.as_str(),
                    apps.dispatcher.as_str()
                ),
                (runtime, extension, dispatcher)
            );
            for end in 0..data.len() {
                assert!(applications(&data[..end]).is_err());
            }
            let mut bad = data.clone();
            bad[12..16].copy_from_slice(&49u32.to_be_bytes());
            assert!(applications(&bad).is_err());
            let mut bad = data.clone();
            let amt = u32::from_be_bytes(bad[20..24].try_into().unwrap()) as usize;
            bad[amt + 4] = 3;
            assert!(applications(&bad).is_err());
        }
        assert!(applications(&bdjo("12345", "12345", "11111")).is_err());
        assert!(applications(&bdjo("12345", "67890", "12345")).is_err());
    }

    #[test]
    fn top_menu_and_all_bdj_titles_are_resolved_from_index_not_jar_presence() {
        let mut data = b"INDX0300".to_vec();
        data.resize(40, 0);
        data[8..12].copy_from_slice(&40u32.to_be_bytes());
        word(&mut data, 38);
        let mut hdmv = [0; 12];
        hdmv[0] = 0x40;
        data.extend_from_slice(&hdmv);
        let mut bdj = [0; 12];
        bdj[0] = 0x80;
        bdj[6..11].copy_from_slice(b"12345");
        data.extend_from_slice(&bdj);
        short(&mut data, 1);
        bdj[6..11].copy_from_slice(b"67890");
        data.extend_from_slice(&bdj);
        let active = index(&data).unwrap();
        assert_eq!(active.first_play, 0);
        assert_eq!(active.top_menu, "12345");
        assert_eq!(
            active.bdjos,
            BTreeSet::from(["12345".into(), "67890".into()])
        );
        for end in 0..data.len() {
            assert!(index(&data[..end]).is_err());
        }
        data[56] = 0x40;
        assert!(index(&data).is_err());
    }

    #[test]
    fn config_overrides_ambiguous_properties_and_debug_paths_reject() {
        let config = b"app.playlist-meta.file=file:///vfs/BDMV/JAR/12345/data.xml\r\ndebug.enableConsole=false\r\ndebug.showFatal=false\r\ndebug.showWatermark=false\r\nlog.default.appenders=none\r\n";
        assert_eq!(
            configuration(config, "12345").unwrap(),
            "/BDMV/JAR/12345/data.xml"
        );
        assert!(configuration(config, "98765").is_err());
        for extra in [
            "app.startupModule=custom\n",
            "app.map.folderOverride=other\n",
            "config=other\n",
            "debug.enableConsole=true\n",
            "debug.enableConsole=false\n",
            "app\\.startupModule=x\n",
        ] {
            let mut bad = config.to_vec();
            bad.extend_from_slice(extra.as_bytes());
            assert!(configuration(&bad, "12345").is_err());
        }
        let bad = String::from_utf8(config.to_vec())
            .unwrap()
            .replace("data.xml", "../data.xml");
        assert!(configuration(bad.as_bytes(), "12345").is_err());
        let bad = String::from_utf8(config.to_vec())
            .unwrap()
            .replace("data.xml", "data.xml ");
        assert!(configuration(bad.as_bytes(), "12345").is_err());
    }

    #[test]
    #[ignore = "local scoped bootstrap metadata; no media or JVM execution"]
    fn local_bootstrap_selects_only_indexed_application_namespaces() {
        let path = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
        let bytes = std::fs::read(path.with_file_name("index.bdmv")).unwrap();
        let index = index(&bytes).unwrap();
        assert_eq!(index.top_menu, "00000");
        assert_eq!(index.bdjos.len(), 3);
        let mut expected = None;
        for name in index.bdjos {
            let bytes = std::fs::read(path.with_file_name(format!("{name}.bdjo"))).unwrap();
            let apps = applications(&bytes).unwrap();
            if let Some(previous) = &expected {
                assert_eq!(previous, &apps);
            } else {
                expected = Some(apps);
            }
            for end in 0..bytes.len() {
                assert!(applications(&bytes[..end]).is_err());
            }
        }
        let apps = expected.unwrap();
        assert_eq!(apps.runtime, "00002");
        assert_eq!(apps.extension, "44444");
        assert_eq!(apps.dispatcher, "00000");
        let config = std::fs::read(path.with_file_name("onQClient.cfg")).unwrap();
        assert_eq!(
            configuration(&config, &apps.runtime).unwrap(),
            "/BDMV/JAR/00002/playlists.xml"
        );
    }
}

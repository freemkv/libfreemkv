//! Generic BD-J feature-playlist resolver — the issue #45 "menu-walk"
//! authoritative layer, run when no framework parser surfaced a manifest hint.
//!
//! Two tiers, most reliable first:
//!
//! * **Tier 1 — jar-space resource sweep (High confidence).** Newer discs embed
//!   the same `dcx.xml` / `playlists.xml` / `*.properties` manifests that Fox
//!   and Paramount ship loose, but INSIDE a `/BDMV/JAR/*.jar` instead. The
//!   feature id is still statically recoverable: reuse
//!   [`fox::feature_hint`](super::fox::feature_hint) /
//!   [`paramount::feature_hint_from_xml`](super::paramount::feature_hint_from_xml).
//!
//! * **Tier 2 — autostart-Xlet locator scan (best-effort, Medium).** Parse the
//!   `/BDMV/BDJO/*.bdjo` Application Management Table for the AUTOSTART app's
//!   jar(s) ([`crate::bdnav::bdjo`]), harvest `NNNNN.mpls` / `bd://…PLAYLIST:N`
//!   String constants from that jar's `.class` constant pools, intersect them
//!   with the real `/BDMV/PLAYLIST/*.mpls` set, and disambiguate by parsed
//!   duration — the feature is a single candidate that dominates the runner-up
//!   and carries multiple audio streams. A hint is emitted ONLY when a single
//!   dominant candidate survives; integer-only evidence never emits.
//!
//! Everything failing here (modern-Fox pure-bytecode `StandardMenuXlet` with no
//! usable `.mpls` string, no usable integer constant) is left to the chapter
//! failsafe in `disc::mod`. This module is read-only, bounded, and never panics
//! (the caller also wraps it in `catch_unwind`).

use super::class_reader::CpInfo;
use super::{FeaturePlaylistHint, fox, jar, paramount};
use crate::bdnav::bdjo;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashSet;

/// Resolve the disc's feature playlist by walking the BD-J jar space. Returns a
/// hint only when one is unambiguously recoverable; `None` otherwise (defer to
/// the failsafe).
pub(crate) fn resolve(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<FeaturePlaylistHint> {
    if let Some(hint) = tier1_manifest_sweep(reader, udf).filter(|h| !h.is_empty()) {
        tracing::info!(?hint, "bdj menu-walk: Tier-1 embedded manifest hint");
        return Some(hint);
    }
    if let Some(hint) = tier2_locator_scan(reader, udf).filter(|h| !h.is_empty()) {
        tracing::info!(?hint, "bdj menu-walk: Tier-2 autostart-Xlet locator hint");
        return Some(hint);
    }
    None
}

// ── Tier 1 ───────────────────────────────────────────────────────────────────

// Cap on bytes decoded from a single embedded manifest as UTF-8. Manifests are
// small; a hostile jar entry can't force an unbounded scan.
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

fn tier1_manifest_sweep(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<FeaturePlaylistHint> {
    jar::for_each_jar(reader, udf, |_entry, archive| {
        jar::try_each_resource(archive, |name, bytes| {
            if bytes.len() > MAX_MANIFEST_BYTES {
                return None;
            }
            let text = std::str::from_utf8(bytes).ok()?;
            hint_from_manifest(name, text)
        })
    })
}

// Derive a feature hint from one embedded manifest, dispatched by filename.
fn hint_from_manifest(name: &str, text: &str) -> Option<FeaturePlaylistHint> {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("dcx.xml") {
        fox::feature_hint(text)
    } else if lower.ends_with("playlists.xml") {
        paramount::feature_hint_from_xml(text)
    } else if lower.ends_with(".properties") || lower.ends_with(".version") {
        props_hint(text)
    } else {
        None
    }
}

// Scan a `key=value` properties/version manifest for a feature-playlist id. A
// key naming both a feature/playlist AND an id (or playlist) whose value carries
// a u16 playlist number yields the canonical `NNNNN.mpls` hint.
fn props_hint(text: &str) -> Option<FeaturePlaylistHint> {
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let names_playlist = key.contains("feature") || key.contains("playlist");
        let names_id = key.contains("id") || key.contains("playlist");
        if !(names_playlist && names_id) {
            continue;
        }
        if let Some(id) = playlist_id_from_str(val.trim()) {
            return Some(hint_for(id));
        }
    }
    None
}

// ── Tier 2 ───────────────────────────────────────────────────────────────────

// Cap on distinct harvested playlist-id candidates — bounds work on a hostile
// jar. Real menus reference a handful.
const MAX_CANDIDATES: usize = 1024;

// The feature must run at least this many times longer than the runner-up to be
// unambiguous; a closer field is not resolvable here and defers to the failsafe.
const DOMINANCE_RATIO: f64 = 1.5;

fn tier2_locator_scan(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<FeaturePlaylistHint> {
    // 1. AUTOSTART app's jar(s) from the BDJO AMT. Empty → scan every jar.
    let targets = autostart_jar_ids(reader, udf);

    // 2. Harvest playlist-id candidates from those jars' `.class` constant pools.
    let candidates = harvest_candidates(reader, udf, &targets);
    if candidates.is_empty() {
        return None;
    }

    // 3. Intersect with the playlists that actually exist on the disc.
    let real = real_playlist_ids(udf);
    let hits: Vec<u16> = candidates
        .into_iter()
        .filter(|id| real.contains(id))
        .collect();
    if hits.is_empty() {
        return None;
    }

    // 4. Score each survivor by (duration secs, primary-audio count).
    let mut scored: Vec<(u16, u64, usize)> = hits
        .into_iter()
        .filter_map(|id| mpls_stats(reader, udf, id).map(|(secs, aud)| (id, secs, aud)))
        .collect();
    if scored.is_empty() {
        return None;
    }
    // Longest first; lowest id breaks a duration tie (deterministic).
    scored.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    // 5. Emit only a single dominant, multi-audio candidate.
    choose_dominant(&scored).map(hint_for)
}

// The dominant feature id, or None when the field is ambiguous. Requires the top
// candidate to carry multiple audio streams and (when a runner-up exists) to run
// at least DOMINANCE_RATIO times longer than it.
fn choose_dominant(scored: &[(u16, u64, usize)]) -> Option<u16> {
    let (top_id, top_secs, top_aud) = *scored.first()?;
    if top_aud < 2 {
        return None;
    }
    match scored.get(1) {
        None => Some(top_id), // single candidate: unambiguous
        Some(&(_, runner_secs, _)) => {
            (top_secs as f64 >= DOMINANCE_RATIO * runner_secs as f64).then_some(top_id)
        }
    }
}

// The jar ids of every AUTOSTART application across all `.bdjo` files. Empty when
// there is no readable BDJO — Tier-2 then scans every jar.
fn autostart_jar_ids(reader: &mut dyn SectorSource, udf: &UdfFs) -> HashSet<String> {
    let mut out = HashSet::new();
    let Some(dir) = udf.find_dir("/BDMV/BDJO") else {
        return out;
    };
    let names: Vec<String> = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".bdjo"))
        .map(|e| e.name.clone())
        .collect();
    for name in names {
        let path = format!("/BDMV/BDJO/{name}");
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Some(apps) = bdjo::parse(&data) else {
            continue;
        };
        for app in apps.iter().filter(|a| a.is_autostart()) {
            for id in app.jar_ids() {
                out.insert(id);
            }
        }
    }
    out
}

// Harvest playlist-id candidates from `.class` constant pools. When `targets` is
// non-empty, only jars whose id is in it are scanned (the autostart classpath);
// otherwise every jar is scanned.
fn harvest_candidates(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    targets: &HashSet<String>,
) -> HashSet<u16> {
    let mut ids = HashSet::new();
    let _: Option<()> = jar::for_each_jar(reader, udf, |entry_name, archive| {
        if !targets.is_empty() && !targets.contains(jar_stem(entry_name)) {
            return None;
        }
        jar::for_each_class(archive, |_class_name, class| {
            for (_, cp) in class.constant_pool.iter() {
                if let CpInfo::Utf8(s) = cp {
                    scan_locator_ids(s, &mut ids);
                }
            }
        });
        None
    });
    ids
}

// The jar id of a `/BDMV/JAR/*.jar` entry name ("00000.jar" -> "00000").
fn jar_stem(entry_name: &str) -> &str {
    let lower = entry_name.to_ascii_lowercase();
    if lower.ends_with(".jar") {
        &entry_name[..entry_name.len() - 4]
    } else {
        entry_name
    }
}

// Harvest playlist ids from one string: `NNNNN.mpls` filenames and
// `PLAYLIST:NNNNN` locator constants. Integer-only evidence is intentionally NOT
// harvested here — the design leaves bare integers to the failsafe.
fn scan_locator_ids(s: &str, out: &mut HashSet<u16>) {
    if out.len() >= MAX_CANDIDATES {
        return;
    }
    let lower = s.to_ascii_lowercase();
    let bytes = s.as_bytes(); // same byte positions as `lower` (ASCII case only)

    // `NNNNN.mpls`: digits immediately preceding a ".mpls".
    let mut from = 0;
    while let Some(rel) = lower[from..].find(".mpls") {
        let at = from + rel;
        let mut start = at;
        while start > 0 && bytes[start - 1].is_ascii_digit() {
            start -= 1;
        }
        if start < at
            && let Ok(id) = s[start..at].parse::<u16>()
        {
            out.insert(id);
            if out.len() >= MAX_CANDIDATES {
                return;
            }
        }
        from = at + ".mpls".len();
    }

    // `PLAYLIST:NNNNN`: digits immediately following a "playlist:" token.
    let mut from = 0;
    while let Some(rel) = lower[from..].find("playlist:") {
        let at = from + rel + "playlist:".len();
        let mut end = at;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > at
            && let Ok(id) = s[at..end].parse::<u16>()
        {
            out.insert(id);
            if out.len() >= MAX_CANDIDATES {
                return;
            }
        }
        from = at.max(from + 1);
    }
}

// The playlist ids present as `/BDMV/PLAYLIST/NNNNN.mpls`.
fn real_playlist_ids(udf: &UdfFs) -> HashSet<u16> {
    let mut out = HashSet::new();
    let Some(dir) = udf.find_dir("/BDMV/PLAYLIST") else {
        return out;
    };
    for e in &dir.entries {
        if e.is_dir || !e.name.to_ascii_lowercase().ends_with(".mpls") {
            continue;
        }
        let stem = &e.name[..e.name.len() - ".mpls".len()];
        if let Ok(id) = stem.parse::<u16>() {
            out.insert(id);
        }
    }
    out
}

// (duration_secs, primary-audio-stream count) for one playlist id, or None if it
// cannot be read/parsed.
fn mpls_stats(reader: &mut dyn SectorSource, udf: &UdfFs, id: u16) -> Option<(u64, usize)> {
    let path = format!("/BDMV/PLAYLIST/{id:05}.mpls");
    let data = udf.read_file(reader, &path).ok()?;
    let pl = crate::mpls::parse(&data).ok()?;
    let ticks: u64 = pl
        .play_items
        .iter()
        .map(|pi| pi.out_time.saturating_sub(pi.in_time) as u64)
        .sum();
    let secs = ticks / 45_000;
    // stream_type 2 == primary audio (see mpls::StreamEntry).
    let audio = pl.streams.iter().filter(|s| s.stream_type == 2).count();
    Some((secs, audio))
}

// ── Shared helpers ───────────────────────────────────────────────────────────

fn hint_for(id: u16) -> FeaturePlaylistHint {
    FeaturePlaylistHint {
        playlist_id: Some(id),
        filename: Some(format!("{id:05}.mpls")),
    }
}

// A playlist id from a free-form string value: keep leading digits, parse u16.
fn playlist_id_from_str(v: &str) -> Option<u16> {
    let digits: String = v.chars().take_while(|c| c.is_ascii_digit()).collect();
    let digits = if digits.is_empty() {
        v.chars().filter(|c| c.is_ascii_digit()).collect()
    } else {
        digits
    };
    digits.parse::<u16>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::fixture::{DirSpec, FileSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    use std::io::{Cursor, Write as _};

    // ── scan_locator_ids ────────────────────────────────────────────────────
    #[test]
    fn scan_finds_mpls_filenames() {
        let mut ids = HashSet::new();
        scan_locator_ids("bd://JAR:00000/00800.mpls", &mut ids);
        assert!(ids.contains(&800));
        let mut ids = HashSet::new();
        scan_locator_ids("00243.MPLS", &mut ids);
        assert!(ids.contains(&243));
    }

    #[test]
    fn scan_finds_playlist_locator_constants() {
        let mut ids = HashSet::new();
        scan_locator_ids("bd://0.PLAYLIST:00800", &mut ids);
        assert!(ids.contains(&800));
    }

    #[test]
    fn scan_ignores_bare_integers() {
        // An integer-only string carries no ".mpls"/"PLAYLIST:" locator, so it
        // must NOT become a candidate — bare integers defer to the failsafe.
        let mut ids = HashSet::new();
        scan_locator_ids("800", &mut ids);
        scan_locator_ids("theNumberIs 00800 done", &mut ids);
        assert!(ids.is_empty());
    }

    // ── choose_dominant ─────────────────────────────────────────────────────
    #[test]
    fn dominant_single_multi_audio_candidate_wins() {
        assert_eq!(choose_dominant(&[(800, 7000, 6)]), Some(800));
    }

    #[test]
    fn dominant_requires_multiple_audio() {
        // A lone candidate with a single audio stream is not a feature.
        assert_eq!(choose_dominant(&[(800, 7000, 1)]), None);
    }

    #[test]
    fn dominant_needs_1_5x_over_runner_up() {
        // 7000 vs 5000 = 1.4x → ambiguous → None.
        assert_eq!(choose_dominant(&[(800, 7000, 6), (801, 5000, 6)]), None);
        // 7000 vs 4000 = 1.75x → dominant.
        assert_eq!(
            choose_dominant(&[(800, 7000, 6), (801, 4000, 6)]),
            Some(800)
        );
    }

    #[test]
    fn dominant_two_equal_durations_is_none() {
        assert_eq!(choose_dominant(&[(800, 7000, 6), (801, 7000, 6)]), None);
    }

    // ── props_hint ──────────────────────────────────────────────────────────
    #[test]
    fn props_hint_reads_feature_playlist_key() {
        let text = "menu.jar=00001\nfeature.playlist.id=00800\nfoo=bar\n";
        let h = props_hint(text).expect("hint");
        assert_eq!(h.playlist_id, Some(800));
        assert_eq!(h.filename.as_deref(), Some("00800.mpls"));
    }

    #[test]
    fn props_hint_ignores_unrelated_keys() {
        assert!(props_hint("version=1\nbuild=42\n").is_none());
    }

    // ── mpls fixture builder (compact single-play-item playlist) ─────────────
    fn audio_stream_entry(pid: u16) -> Vec<u8> {
        let mut out = vec![3u8, 0x01]; // se_len, type = PlayItem clip
        out.extend_from_slice(&pid.to_be_bytes());
        // attrs: coding_type(TrueHD) + (5.1<<4|48k) + language
        let attrs = [0x83u8, (6 << 4) | 1, b'e', b'n', b'g'];
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }

    // A playlist: one play item of `secs` seconds with `n_audio` audio streams.
    fn build_mpls(secs: u32, n_audio: u8) -> Vec<u8> {
        let playlist_start: u32 = 40;
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MPLS0200");
        buf.extend_from_slice(&playlist_start.to_be_bytes());
        buf.extend_from_slice(&[0u8; 28]); // mark_start + pad to 40

        let pl_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // length placeholder
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&1u16.to_be_bytes()); // num_play_items
        buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

        let mut item = Vec::new();
        item.extend_from_slice(b"00000"); // clip_id
        item.extend_from_slice(b"M2TS");
        item.push(0); // reserved
        item.push(0); // is_multi_angle + connection_condition
        item.push(0); // stc_id
        item.extend_from_slice(&0u32.to_be_bytes()); // in_time
        item.extend_from_slice(&(secs * 45000).to_be_bytes()); // out_time
        item.extend_from_slice(&[0u8; 8]); // UO mask
        item.push(0); // misc
        item.push(0); // still_mode
        item.extend_from_slice(&[0u8; 2]); // still_time

        // STN table
        let stn_start = item.len();
        item.extend_from_slice(&[0u8; 2]); // length placeholder
        item.extend_from_slice(&[0u8; 2]); // reserved
        item.push(0); // n_video
        item.push(n_audio); // n_audio
        item.extend_from_slice(&[0u8; 6]); // pg/ig/sec.../dv counts
        item.extend_from_slice(&[0u8; 4]); // reserved
        for i in 0..n_audio {
            item.extend_from_slice(&audio_stream_entry(0x1100 + i as u16));
        }
        let stn_len = (item.len() - stn_start - 2) as u16;
        item[stn_start..stn_start + 2].copy_from_slice(&stn_len.to_be_bytes());

        buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
        buf.extend_from_slice(&item);

        let pl_len = (buf.len() - pl_start - 4) as u32;
        buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());
        buf
    }

    // Minimal `.class` carrying the given Utf8 constants (no fields/methods).
    fn build_class(utf8: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0xCAFEBABEu32.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes()); // minor
        out.extend_from_slice(&52u16.to_be_bytes()); // major
        out.extend_from_slice(&((utf8.len() + 1) as u16).to_be_bytes());
        for s in utf8 {
            out.push(1);
            out.extend_from_slice(&(s.len() as u16).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        out.extend_from_slice(&[0u8; 14]); // access/this/super/interfaces/fields/methods/attrs (7 u16)
        out
    }

    // Zip entries into an in-memory jar (Stored).
    fn build_jar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for (name, data) in entries {
                w.start_file(*name, opts).unwrap();
                w.write_all(data).unwrap();
            }
            w.finish().unwrap();
        }
        buf
    }

    // Lay a BDMV tree with the given PLAYLIST + JAR (+ optional BDJO) files onto
    // an in-memory disc and return (disc, udf). LBAs are hand-assigned and never
    // overlap.
    fn build_disc(
        playlists: Vec<(&str, Vec<u8>)>,
        jars: Vec<(&str, Vec<u8>)>,
        bdjos: Vec<(&str, Vec<u8>)>,
    ) -> (MemDisc, crate::udf::UdfFs) {
        let mut icb = 100u32;
        let mut data = 2000u32;
        let mut next = |sz: u64| {
            let (i, d) = (icb, data);
            icb += 1;
            data += (sz / 2048) as u32 + 2;
            (i, d)
        };

        let mk = |files: Vec<(&str, Vec<u8>)>, next: &mut dyn FnMut(u64) -> (u32, u32)| {
            files
                .into_iter()
                .map(|(name, bytes)| {
                    let (i, d) = next(bytes.len() as u64);
                    file_with(name, i, d, bytes, true)
                })
                .collect::<Vec<FileSpec>>()
        };

        let playlist_dir = DirSpec {
            name: "PLAYLIST".to_string(),
            icb_lba: 50,
            dir_data_lba: 51,
            files: mk(playlists, &mut next),
            subdirs: vec![],
        };
        let jar_dir = DirSpec {
            name: "JAR".to_string(),
            icb_lba: 52,
            dir_data_lba: 53,
            files: mk(jars, &mut next),
            subdirs: vec![],
        };
        let mut subdirs = vec![playlist_dir, jar_dir];
        if !bdjos.is_empty() {
            subdirs.push(DirSpec {
                name: "BDJO".to_string(),
                icb_lba: 54,
                dir_data_lba: 55,
                files: mk(bdjos, &mut next),
                subdirs: vec![],
            });
        }
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 12,
            dir_data_lba: 13,
            files: vec![],
            subdirs,
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: vec![],
            subdirs: vec![bdmv],
        };
        let mut disc = MemDisc::new();
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        (disc, udf)
    }

    // ── Tier 1 integration ──────────────────────────────────────────────────
    #[test]
    fn tier1_resolves_from_embedded_playlists_xml() {
        let xml = br#"<playlists>
            <playlist name="Feature" id="00800" aud="eng,fra,spa" duration="7000" />
            <playlist name="Preview" id="00050" aud="eng" duration="120" />
        </playlists>"#;
        let jar = build_jar(&[
            ("com/studio/Menu.class", build_class(&["unrelated"])),
            ("00000/playlists.xml", xml.to_vec()),
        ]);
        let (mut disc, udf) = build_disc(vec![], vec![("00000.jar", jar)], vec![]);
        let hint = resolve(&mut disc, &udf).expect("Tier-1 hint");
        assert_eq!(hint.playlist_id, Some(800));
    }

    // ── Tier 2 integration ──────────────────────────────────────────────────
    #[test]
    fn tier2_resolves_dominant_mpls_locator() {
        // Autostart jar's class references "00800.mpls"; the disc has a long
        // 00800 (multi-audio) and a short 00801. No manifest → Tier-2 resolves.
        let class = build_class(&["bd://JAR:00000/00800.mpls", "some/other/String"]);
        let jar = build_jar(&[("com/studio/MainXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![
                ("00800.mpls", build_mpls(7000, 6)),
                ("00801.mpls", build_mpls(120, 2)),
            ],
            vec![("00000.jar", jar)],
            vec![], // no BDJO → scan all jars
        );
        let hint = resolve(&mut disc, &udf).expect("Tier-2 hint");
        assert_eq!(hint.playlist_id, Some(800));
        assert_eq!(hint.filename.as_deref(), Some("00800.mpls"));
    }

    #[test]
    fn tier2_abstains_when_candidate_not_a_real_playlist() {
        // The class points at 09999.mpls, which is not on the disc → no hit.
        let class = build_class(&["09999.mpls"]);
        let jar = build_jar(&[("com/studio/MainXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 6))],
            vec![("00000.jar", jar)],
            vec![],
        );
        assert!(resolve(&mut disc, &udf).is_none());
    }

    #[test]
    fn tier2_abstains_on_integer_only_bytecode() {
        // The autostart class carries only a bare integer constant (800) and no
        // .mpls / PLAYLIST: locator string — modern-Fox StandardMenuXlet shape.
        // No candidate is harvested → defer to the chapter failsafe.
        let mut class = Vec::new();
        class.extend_from_slice(&0xCAFEBABEu32.to_be_bytes());
        class.extend_from_slice(&0u16.to_be_bytes());
        class.extend_from_slice(&52u16.to_be_bytes());
        class.extend_from_slice(&2u16.to_be_bytes()); // cp_count = 2 (one entry)
        class.push(3); // CONSTANT_Integer
        class.extend_from_slice(&800u32.to_be_bytes());
        class.extend_from_slice(&[0u8; 14]);
        let jar = build_jar(&[("com/foxbd/StandardMenuXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 6))],
            vec![("00000.jar", jar)],
            vec![],
        );
        assert!(resolve(&mut disc, &udf).is_none());
    }

    #[test]
    fn tier2_uses_bdjo_autostart_jar_targeting() {
        // Two jars; only the autostart one (00000) names the feature. A decoy jar
        // (00009) points at a different real playlist. The BDJO AMT pins 00000 as
        // autostart, so only its candidate (00800) is harvested.
        let auto_class = build_class(&["00800.mpls"]);
        let decoy_class = build_class(&["00801.mpls"]);
        let auto_jar = build_jar(&[("com/studio/MainXlet.class", auto_class)]);
        let decoy_jar = build_jar(&[("com/studio/Decoy.class", decoy_class)]);

        let bdjo = bdjo_with_autostart("00000");
        let (mut disc, udf) = build_disc(
            vec![
                ("00800.mpls", build_mpls(7000, 6)),
                // 00801 is LONGER, so if the decoy jar were scanned it would win.
                ("00801.mpls", build_mpls(9000, 6)),
            ],
            vec![("00000.jar", auto_jar), ("00009.jar", decoy_jar)],
            vec![("00000.bdjo", bdjo)],
        );
        let hint = resolve(&mut disc, &udf).expect("Tier-2 hint via BDJO targeting");
        assert_eq!(
            hint.playlist_id,
            Some(800),
            "only the autostart jar's candidate should be considered"
        );
    }

    // Build a minimal single-app BDJO whose AUTOSTART app has the given base_dir.
    fn bdjo_with_autostart(base_dir: &str) -> Vec<u8> {
        fn app_string(buf: &mut Vec<u8>, s: &str) {
            buf.push(s.len() as u8);
            buf.extend_from_slice(s.as_bytes());
            if s.len().is_multiple_of(2) {
                buf.push(0);
            }
        }
        let mut b = Vec::new();
        b.extend_from_slice(b"BDJO");
        b.extend_from_slice(b"0200");
        b.extend_from_slice(&[0u8; 40]); // section-address table
        b.extend_from_slice(&[0u8; 4]); // TerminalInfo length
        b.extend_from_slice(&[0u8; 5]); // default_font
        b.extend_from_slice(&[0u8; 5]); // havi/masks + padding (40 bits)
        b.extend_from_slice(&[0u8; 4]); // AppCacheInfo length
        b.push(0); // num_item
        b.push(0); // padding
        b.extend_from_slice(&[0u8; 4]); // AccessiblePlaylists length
        b.extend_from_slice(&[0u8; 4]); // num_pl(11)+flags(2)+pad(19) = 0
        b.extend_from_slice(&[0u8; 4]); // AMT length
        b.push(1); // num_app
        b.push(0); // padding
        // App record
        b.push(1); // control_code = AUTOSTART
        b.push(0); // type(4)+reserved(4)
        b.extend_from_slice(&[0u8; 4]); // org_id
        b.extend_from_slice(&[0u8; 2]); // app_id
        b.extend_from_slice(&[0u8; 10]); // descriptor tag+length
        b.push(0); // num_profile(4)+pad
        b.push(0);
        b.push(0); // priority
        b.push(0); // binding/visibility/reserved
        b.extend_from_slice(&[0u8; 2]); // app_name data_length = 0
        app_string(&mut b, ""); // icon_locator
        b.extend_from_slice(&[0u8; 2]); // icon_flags
        app_string(&mut b, base_dir); // base_dir
        app_string(&mut b, ""); // classpath_extension
        app_string(&mut b, "com.studio.MainXlet"); // initial_class
        b.push(0); // params data_length = 0
        b.push(0); // word-align pad
        b
    }
}

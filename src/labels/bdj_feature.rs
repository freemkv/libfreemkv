//! Resolve a feature playlist from BD-J resources when framework parsers have no hint.
//!
//! Embedded XML/properties manifests take precedence. Otherwise, scan autostart
//! Xlet jars for playlist string constants and intersect them with actual playlists.
//! Emit a hint only for one dominant-duration candidate with multiple audio streams;
//! integer constants alone are insufficient evidence. Return `None` on ambiguity
//! (including unreadable evidence or a spent work budget) so the caller can use its
//! chapter-based fallback. Heuristic over application-defined space, not a spec.

use super::class_reader::CpInfo;
use super::{FeaturePlaylistHint, fox, jar, paramount};
use crate::bdnav::bdjo;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::{HashMap, HashSet};

/// A playlist's (duration, primary-audio count) as already scanned by the caller.
pub(crate) struct PlaylistStat {
    pub id: u16,
    pub secs: u64,
    pub audio: usize,
}

// Total bytes the menu-walk may inflate across every jar (both tiers). A hostile
// jar (decompression bomb, overlapping entries) spends it and the walk abstains.
const INFLATE_BUDGET: u64 = 256 * 1024 * 1024;

/// Resolve the disc's feature playlist by walking the BD-J jar space. Returns a
/// hint only when one is unambiguously recoverable; `None` otherwise (defer to
/// the failsafe). `known` supplies stats for playlists the scan already parsed.
pub(crate) fn resolve(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    known: &[PlaylistStat],
) -> Option<FeaturePlaylistHint> {
    resolve_with_budget(reader, udf, known, INFLATE_BUDGET)
}

// Why the single jar pass stopped early.
enum Stop {
    Tier1(FeaturePlaylistHint),
    BudgetSpent,
}

fn resolve_with_budget(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    known: &[PlaylistStat],
    mut budget: u64,
) -> Option<FeaturePlaylistHint> {
    udf.find_dir("/BDMV/JAR")?;
    let real = real_playlist_ids(udf);
    // AUTOSTART app's jar(s) from the BDJO AMT. Empty → Tier 2 harvests every jar.
    let targets = autostart_jar_ids(reader, udf);

    // One pass: each jar is read once, swept for a Tier-1 manifest, then (when it
    // is a Tier-2 target) harvested for playlist locators.
    let mut candidates = HashSet::new();
    let mut complete = true;
    let stop = jar::visit_jars(reader, udf, |name, archive| {
        let is_target = targets.is_empty() || targets.contains(jar_stem(name));
        let Some(archive) = archive else {
            complete &= !is_target; // an unreadable target hides candidates
            return None;
        };
        if let Some(hint) = tier1_manifest_sweep(archive, &real, &mut budget) {
            return Some(Stop::Tier1(hint));
        }
        if is_target && candidates.len() < MAX_CANDIDATES {
            harvest_candidates(archive, &mut candidates, &mut budget);
        }
        (budget == 0).then_some(Stop::BudgetSpent)
    });
    match stop {
        Some(Stop::Tier1(hint)) => {
            tracing::info!(?hint, "bdj menu-walk: Tier-1 embedded manifest hint");
            Some(hint)
        }
        Some(Stop::BudgetSpent) => None,
        None if !complete => None,
        None => {
            let hint = tier2_score(reader, udf, &candidates, &real, known)?;
            tracing::info!(?hint, "bdj menu-walk: Tier-2 autostart-Xlet locator hint");
            Some(hint)
        }
    }
}

// ── Tier 1 ───────────────────────────────────────────────────────────────────

// Cap on bytes decoded from a single embedded manifest as UTF-8. Manifests are
// small; a hostile jar entry can't force an unbounded scan.
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

// The first embedded manifest in this jar that names a real playlist.
fn tier1_manifest_sweep(
    archive: &mut jar::Jar,
    real: &HashSet<u16>,
    budget: &mut u64,
) -> Option<FeaturePlaylistHint> {
    // Read one byte past the cap so an oversized manifest is recognised and skipped.
    let cap = MAX_MANIFEST_BYTES as u64 + 1;
    jar::try_each_resource(archive, is_manifest_name, cap, budget, |name, bytes| {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return None;
        }
        let text = std::str::from_utf8(bytes).ok()?;
        hint_from_manifest(name, text)
            .filter(|h| h.playlist_id.is_some_and(|id| real.contains(&id)))
    })
}

// Manifest kinds, by filename suffix (lower-case).
#[derive(PartialEq)]
enum Manifest {
    FoxDcx,
    ParamountPlaylists,
    Properties,
}

fn manifest_kind(name: &str) -> Option<Manifest> {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("dcx.xml") {
        Some(Manifest::FoxDcx)
    } else if lower.ends_with("playlists.xml") {
        Some(Manifest::ParamountPlaylists)
    } else if lower.ends_with(".properties") || lower.ends_with(".version") {
        Some(Manifest::Properties)
    } else {
        None
    }
}

fn is_manifest_name(name: &str) -> bool {
    manifest_kind(name).is_some()
}

// Derive a feature hint from one embedded manifest, dispatched by filename.
fn hint_from_manifest(name: &str, text: &str) -> Option<FeaturePlaylistHint> {
    match manifest_kind(name)? {
        Manifest::FoxDcx => fox::feature_hint(text),
        Manifest::ParamountPlaylists => paramount::feature_hint(text),
        Manifest::Properties => props_hint(text),
    }
}

// Feature-playlist id from a `key=value` manifest: the key is only feature/playlist
// identity words and names the feature (`featurePlaylistId`); the value is a bare
// playlist number (optionally quoted or `.mpls`-suffixed).
fn props_hint(text: &str) -> Option<FeaturePlaylistHint> {
    text.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with('!') {
            return None;
        }
        let (key, val) = line.split_once('=')?;
        let words = super::name_words(key);
        let names_feature = words.iter().any(|w| w == "feature");
        let identity_only = words.iter().all(|w| {
            matches!(
                w.as_str(),
                "feature" | "main" | "movie" | "playlist" | "pl" | "id" | "mpls" | "file"
            )
        });
        if !(names_feature && identity_only && words.len() > 1) {
            return None;
        }
        playlist_number(val).map(FeaturePlaylistHint::for_playlist)
    })
}

// A bare playlist number: 1-5 digits, optionally quoted and/or `.mpls`-suffixed.
fn playlist_number(v: &str) -> Option<u16> {
    let v = v.trim().trim_matches(|c| c == '"' || c == '\'');
    let v = match v.len().checked_sub(".mpls".len()) {
        Some(at) if v.is_char_boundary(at) && v[at..].eq_ignore_ascii_case(".mpls") => &v[..at],
        _ => v,
    };
    if v.is_empty() || v.len() > 5 || !v.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    v.parse().ok()
}

// ── Tier 2 ───────────────────────────────────────────────────────────────────

// Cap on distinct harvested playlist-id candidates — bounds work on a hostile
// jar. Real menus reference a handful.
const MAX_CANDIDATES: usize = 1024;

// Cap on candidate playlists re-read from disc for scoring (those the scan did not
// already parse — e.g. sub-30 s). More than this is not a resolvable field.
const MAX_STAT_READS: usize = 32;

// The feature must run at least this many times longer than the runner-up to be
// unambiguous; a closer field is not resolvable here and defers to the failsafe.
const DOMINANCE_RATIO: f64 = 1.5;

// Score the harvested candidates that are real playlists and pick a dominant one.
// Any candidate that cannot be scored makes the field unknown → abstain.
fn tier2_score(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    candidates: &HashSet<u16>,
    real: &HashSet<u16>,
    known: &[PlaylistStat],
) -> Option<FeaturePlaylistHint> {
    let known: HashMap<u16, (u64, usize)> =
        known.iter().map(|s| (s.id, (s.secs, s.audio))).collect();
    let mut hits: Vec<u16> = candidates.intersection(real).copied().collect();
    hits.sort_unstable();
    let mut reads = 0usize;
    let mut scored: Vec<(u16, u64, usize)> = Vec::with_capacity(hits.len());
    for id in hits {
        let (secs, aud) = match known.get(&id) {
            Some(&stat) => stat,
            None => {
                reads += 1;
                if reads > MAX_STAT_READS {
                    return None;
                }
                mpls_stats(reader, udf, id)?
            }
        };
        scored.push((id, secs, aud));
    }
    // Longest first; lowest id breaks a duration tie (deterministic).
    scored.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    choose_dominant(&scored).map(FeaturePlaylistHint::for_playlist)
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
// any BDJO is unreadable (a partial set would hide jars) — Tier 2 then scans all.
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
        let Some(apps) = udf
            .read_file(reader, &path)
            .ok()
            .and_then(|data| bdjo::parse(&data))
        else {
            return HashSet::new();
        };
        for app in apps.iter().filter(|a| a.is_autostart()) {
            for id in app.jar_ids() {
                out.insert(id);
            }
        }
    }
    out
}

// Harvest playlist-id candidates from one jar's `.class` constant pools, stopping
// at MAX_CANDIDATES or when the inflation budget is spent.
fn harvest_candidates(archive: &mut jar::Jar, ids: &mut HashSet<u16>, budget: &mut u64) {
    let _: Option<()> = jar::try_each_class_budgeted(archive, budget, |_class_name, class| {
        for (_, cp) in class.constant_pool.iter() {
            if let CpInfo::Utf8(s) = cp {
                scan_locator_ids(s, ids);
            }
        }
        (ids.len() >= MAX_CANDIDATES).then_some(())
    });
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

// Byte offsets of ASCII-case-insensitive matches of `needle_lower` in `hay`.
fn find_ci<'a>(hay: &'a [u8], needle_lower: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    hay.windows(needle_lower.len())
        .enumerate()
        .filter(|(_, w)| w.eq_ignore_ascii_case(needle_lower))
        .map(|(i, _)| i)
}

// Harvest playlist ids from one string: `NNNNN.mpls` filenames and
// `PLAYLIST:NNNNN` locator constants. Integer-only evidence is intentionally NOT
// harvested here — the design leaves bare integers to the failsafe.
fn scan_locator_ids(s: &str, out: &mut HashSet<u16>) {
    let bytes = s.as_bytes();
    // `NNNNN.mpls`: digits immediately preceding a ".mpls".
    for at in find_ci(bytes, b".mpls") {
        let start = bytes[..at]
            .iter()
            .rposition(|b| !b.is_ascii_digit())
            .map_or(0, |p| p + 1);
        if out.len() >= MAX_CANDIDATES {
            return;
        }
        if start < at
            && let Ok(id) = s[start..at].parse::<u16>()
        {
            out.insert(id);
        }
    }
    // `PLAYLIST:NNNNN`: digits immediately following a "playlist:" token.
    for at in find_ci(bytes, b"playlist:") {
        let at = at + "playlist:".len();
        let end = bytes[at..]
            .iter()
            .position(|b| !b.is_ascii_digit())
            .map_or(bytes.len(), |p| at + p);
        if out.len() >= MAX_CANDIDATES {
            return;
        }
        if end > at
            && let Ok(id) = s[at..end].parse::<u16>()
        {
            out.insert(id);
        }
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
    let audio = pl
        .streams
        .iter()
        .filter(|s| s.stream_type == crate::mpls::STREAM_CATEGORY_AUDIO)
        .count();
    Some((pl.duration_ticks() / 45_000, audio))
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

    // Keys that merely mention "playlist"/"feature" (or contain "id" inside another
    // word) are not a feature-playlist id; values must be a bare playlist number.
    #[test]
    fn props_hint_rejects_loose_keys_and_values() {
        for text in [
            "playlist.count=12",
            "playlistCount=12",
            "feature.video.width=1920",
            "trailer.playlist=00010",
            "menu_playlist=00005",
            "playlist.api.version=2",
            "playlist.version=v2.1",
            "feature.playlist=PL_2_v10",
            "feature.audio.id=2",
        ] {
            assert_eq!(props_hint(text), None, "{text}");
        }
    }

    #[test]
    fn props_hint_accepts_feature_playlist_id_spellings() {
        for text in [
            "featurePlaylistId=00800",
            "feature_playlist=00800.mpls",
            "main.feature.id = \"00800\"",
        ] {
            let h = props_hint(text).unwrap_or_else(|| panic!("{text}"));
            assert_eq!(h.playlist_id, Some(800), "{text}");
        }
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
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 3))],
            vec![("00000.jar", jar)],
            vec![],
        );
        let hint = resolve(&mut disc, &udf, &[]).expect("Tier-1 hint");
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
        let hint = resolve(&mut disc, &udf, &[]).expect("Tier-2 hint");
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
        assert!(resolve(&mut disc, &udf, &[]).is_none());
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
        assert!(resolve(&mut disc, &udf, &[]).is_none());
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
        let hint = resolve(&mut disc, &udf, &[]).expect("Tier-2 hint via BDJO targeting");
        assert_eq!(
            hint.playlist_id,
            Some(800),
            "only the autostart jar's candidate should be considered"
        );
    }

    // A Tier-1 hint naming a playlist that is not on the disc is not evidence:
    // the sweep continues and Tier 2 decides.
    #[test]
    fn tier1_hint_must_name_a_real_playlist() {
        let jar = build_jar(&[
            ("app.properties", b"feature.playlist.id=00012\n".to_vec()),
            ("com/studio/MainXlet.class", build_class(&["00800.mpls"])),
        ]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 6))],
            vec![("00000.jar", jar)],
            vec![],
        );
        let hint = resolve(&mut disc, &udf, &[]).expect("Tier-2 hint");
        assert_eq!(hint.playlist_id, Some(800));
    }

    // A candidate that cannot be read/parsed makes the field unknown: abstain
    // rather than crown the lone readable survivor.
    #[test]
    fn tier2_abstains_when_a_candidate_cannot_be_scored() {
        let class = build_class(&["00800.mpls", "00801.mpls"]);
        let jar = build_jar(&[("com/studio/MainXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![
                ("00800.mpls", b"not an mpls".to_vec()),
                ("00801.mpls", build_mpls(6500, 6)),
            ],
            vec![("00000.jar", jar)],
            vec![],
        );
        assert_eq!(resolve(&mut disc, &udf, &[]), None);
    }

    // Stats for playlists the scan already parsed are reused, not re-read.
    #[test]
    fn tier2_uses_known_playlist_stats() {
        let class = build_class(&["00800.mpls"]);
        let jar = build_jar(&[("com/studio/MainXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", b"unreadable here".to_vec())],
            vec![("00000.jar", jar)],
            vec![],
        );
        let known = [PlaylistStat {
            id: 800,
            secs: 7000,
            audio: 6,
        }];
        let hint = resolve(&mut disc, &udf, &known).expect("hint from known stats");
        assert_eq!(hint.playlist_id, Some(800));
    }

    // An unreadable Tier-2 jar may hold the real feature's locator: abstain.
    #[test]
    fn tier2_abstains_when_a_target_jar_is_unreadable() {
        let jar = build_jar(&[("com/studio/MainXlet.class", build_class(&["00800.mpls"]))]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 6))],
            vec![("00000.jar", b"not a zip".to_vec()), ("00001.jar", jar)],
            vec![],
        );
        assert_eq!(resolve(&mut disc, &udf, &[]), None);
    }

    // One unreadable BDJO makes the autostart set partial: scan every jar instead,
    // so the decoy jar's longer 00801 keeps the field ambiguous.
    #[test]
    fn unreadable_bdjo_widens_tier2_to_every_jar() {
        let auto_jar = build_jar(&[("com/studio/MainXlet.class", build_class(&["00800.mpls"]))]);
        let other_jar = build_jar(&[("com/studio/Other.class", build_class(&["00801.mpls"]))]);
        let (mut disc, udf) = build_disc(
            vec![
                ("00800.mpls", build_mpls(7000, 6)),
                ("00801.mpls", build_mpls(9000, 6)),
            ],
            vec![("00000.jar", auto_jar), ("00009.jar", other_jar)],
            vec![
                ("00000.bdjo", bdjo_with_autostart("00000")),
                ("00001.bdjo", b"garbage".to_vec()),
            ],
        );
        assert_eq!(resolve(&mut disc, &udf, &[]), None);
    }

    // A sector source that records every LBA it is asked for.
    struct Counting<'a>(&'a mut MemDisc, Vec<u32>);
    impl SectorSource for Counting<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            self.1.extend(lba..lba + count as u32);
            self.0.read_sectors(lba, count, buf, recovery)
        }
    }

    // Both tiers share one pass over /BDMV/JAR: each jar is read from disc once.
    #[test]
    fn menu_walk_reads_each_jar_once() {
        let jar = build_jar(&[("com/studio/MainXlet.class", build_class(&["09999.mpls"]))]);
        let (mut disc, udf) = build_disc(vec![], vec![("00000.jar", jar)], vec![]);
        let mut counting = Counting(&mut disc, Vec::new());
        assert_eq!(resolve(&mut counting, &udf, &[]), None);
        // No playlists, so the jar's data extent is the first one laid out (2000).
        let jar_lba = udf.partition_start() + 2000;
        let reads = counting.1.iter().filter(|&&l| l == jar_lba).count();
        assert_eq!(reads, 1);
    }

    // A spent inflation budget abstains instead of scanning on.
    #[test]
    fn exhausted_inflate_budget_abstains() {
        let class = build_class(&["00800.mpls"]);
        let jar = build_jar(&[("com/studio/MainXlet.class", class)]);
        let (mut disc, udf) = build_disc(
            vec![("00800.mpls", build_mpls(7000, 6))],
            vec![("00000.jar", jar)],
            vec![],
        );
        assert!(resolve_with_budget(&mut disc, &udf, &[], 1 << 20).is_some());
        assert_eq!(resolve_with_budget(&mut disc, &udf, &[], 8), None);
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

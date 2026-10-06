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
    // A full candidate set means later jars were skipped: like a spent budget, abstain.
    complete &= candidates.len() < MAX_CANDIDATES;
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
    archive: &mut jar::DiscJar<'_>,
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
        let exists = |id: u16| real.contains(&id);
        hint_from_manifest(name, text, &exists).filter(|h| h.playlist_id.is_some_and(exists))
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
fn hint_from_manifest(
    name: &str,
    text: &str,
    exists: &dyn Fn(u16) -> bool,
) -> Option<FeaturePlaylistHint> {
    match manifest_kind(name)? {
        Manifest::FoxDcx => fox::feature_hint(text),
        Manifest::ParamountPlaylists => paramount::feature_hint(text),
        Manifest::Properties => props_hint(text, exists),
    }
}

// Feature-playlist id from a `key=value` manifest: the key is only identity words and
// names the feature plus a playlist (`featurePlaylistId`), unless the value itself is a
// locator (`00800.mpls`, `bd://0.PLAYLIST:00800`). First line naming a real playlist wins.
fn props_hint(text: &str, exists: &dyn Fn(u16) -> bool) -> Option<FeaturePlaylistHint> {
    text.lines().find_map(|line| {
        let line = line.trim();
        if line.starts_with('#') || line.starts_with('!') {
            return None;
        }
        let (key, val) = line.split_once('=')?;
        let words = super::name_words(key);
        let has = |set: &[&str]| words.iter().any(|w| set.contains(&w.as_str()));
        let identity_only = words.iter().all(|w| {
            matches!(
                w.as_str(),
                "feature"
                    | "main"
                    | "movie"
                    | "title"
                    | "disc"
                    | "playlist"
                    | "pl"
                    | "id"
                    | "mpls"
                    | "file"
            )
        });
        if !(identity_only && has(&["feature"])) {
            return None;
        }
        let (id, locator) = playlist_number(val)?;
        (exists(id) && (locator || has(&["playlist", "pl", "mpls"])))
            .then(|| FeaturePlaylistHint::for_playlist(id))
    })
}

// A playlist number: 1-5 digits, optionally quoted, `.mpls`-suffixed or after a
// `PLAYLIST:` locator (which may continue, e.g. `.MARK:00001`). `.1` is true when
// the value itself names a playlist.
fn playlist_number(v: &str) -> Option<(u16, bool)> {
    let v = v.trim().trim_matches(|c| c == '"' || c == '\'');
    if let Some(at) = v.to_ascii_lowercase().rfind("playlist:") {
        let rest = &v[at + "playlist:".len()..];
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let tail = &rest[digits..];
        if !(1..=5).contains(&digits) || !(tail.is_empty() || tail.starts_with('.')) {
            return None;
        }
        return Some((rest[..digits].parse().ok()?, true));
    }
    let (v, mpls) = match v.len().checked_sub(".mpls".len()) {
        Some(at) if v.is_char_boundary(at) && v[at..].eq_ignore_ascii_case(".mpls") => {
            (&v[..at], true)
        }
        _ => (v, false),
    };
    if v.is_empty() || v.len() > 5 || !v.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((v.parse().ok()?, mpls))
}

// ── Tier 2 ───────────────────────────────────────────────────────────────────

// Cap on distinct harvested playlist-id candidates — bounds work on a hostile
// jar. Real menus reference a handful.
const MAX_CANDIDATES: usize = 1024;

// Cap on candidate playlists re-read from disc for scoring (those missing from the
// scan's stats); past it, Tier 2 abstains.
const MAX_STAT_READS: usize = 32;

// The feature must run at least this many times longer than the runner-up to be
// unambiguous; a closer field is not resolvable here and defers to the failsafe.
const DOMINANCE_RATIO: f64 = 1.5;

// Score the harvested candidates that are real playlists and pick a dominant one.
// A candidate missing from the scan's stats is re-read; an unreadable one → abstain.
fn tier2_score(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    candidates: &HashSet<u16>,
    real: &HashSet<u16>,
    known: &[PlaylistStat],
) -> Option<FeaturePlaylistHint> {
    let stats: HashMap<u16, (u64, usize)> =
        known.iter().map(|s| (s.id, (s.secs, s.audio))).collect();
    let mut hits: Vec<u16> = candidates.intersection(real).copied().collect();
    hits.sort_unstable();
    let mut reads = 0usize;
    let mut scored: Vec<(u16, u64, usize)> = Vec::with_capacity(hits.len());
    for id in hits {
        let (secs, aud) = match stats.get(&id) {
            Some(&stat) => stat,
            // Past the read budget the field is not resolvable.
            None if reads >= MAX_STAT_READS => return None,
            // The scan may have dropped it for a read failure: confirm by re-reading.
            None => {
                reads += 1;
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
fn harvest_candidates(archive: &mut jar::DiscJar<'_>, ids: &mut HashSet<u16>, budget: &mut u64) {
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
        if stem.len() == 5
            && stem.bytes().all(|b| b.is_ascii_digit())
            && let Ok(id) = stem.parse::<u16>()
        {
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
#[path = "bdj_feature_tests.rs"]
mod tests;

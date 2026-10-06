//! Paramount/onQ — `playlists.xml`. Richest structured format: complete
//! language lists with forced flags and commentary indices per playlist,
//! all in XML attributes.
//!
//! NOT A SPECIFICATION: `/BDMV/JAR/` is application-defined space; every field meaning here was
//! derived by measuring real discs.
//!
//! ```xml
//! <playlist name="Feature" id="00222"
//!   aud="eng,deu,spa,spa,fra"
//!   sub="eng,eng,zho,ces,dan"
//!   forced_sub="0,0,0,1,3"
//!   aud_com1_idx="10"
//!   sub_com1_idx="23,24,25" />
//! ```
//!
//! `forced_sub` is an ENUMERATION, not a boolean — see [`ForcedSub`].

use super::MIN_FEATURE_SECS;
use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::HashSet;

pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_file_exists(udf, "playlists.xml")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "playlists.xml")?;
    let text = std::str::from_utf8(&data).ok()?;

    // Find the feature playlist (three tiers, see find_feature_playlist).
    let feature = find_feature_playlist(text)?;

    let labels = labels_from_feature(feature);

    if labels.is_empty() {
        return None;
    }

    // High confidence: this format is fully structured and we extract
    // every field whose meaning the corpus establishes. "Documented" would
    // be the wrong word — see the module note; nothing about it is.
    let mut result = ParseResult::high(labels);
    result.feature_playlist = element_hint(feature);
    Some(result)
}

/// The feature playlist hint for a `playlists.xml` `<playlist>` element: its
/// authoring id, surfaced so title selection can prefer it over a size-inflated decoy.
fn element_hint(feature: &str) -> Option<super::FeaturePlaylistHint> {
    super::FeaturePlaylistHint::from_authoring_id(&xml::attr(feature, "id")?)
}

// One cell of the `forced_sub` CSV. Reads like a boolean but is an enumeration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ForcedSub {
    /// No forced-narrative content, or an unrecognised cell.
    None,
    /// A full dialogue track that also carries forced-narrative segments.
    ContainsForcedSegments,
    /// A dedicated forced-narrative track.
    ForcedNarrative,
}

// The highest CSV cell position that can ever be addressed — caps both the VALUE set size and
// the WORK done per cell.
const MAX_COM_INDICES: usize = u16::MAX as usize;

// Parse a `*_com1_idx` attribute into the set the labelling loops query. Extracted so the bound
// is independently testable.
fn com_indices(attr: Option<String>) -> HashSet<usize> {
    attr.map(|s| {
        s.split(',')
            .take(MAX_COM_INDICES)
            .filter_map(|i| i.trim().parse().ok())
            .filter(|&i| i < MAX_COM_INDICES)
            .collect()
    })
    .unwrap_or_default()
}

// Parse `forced_sub` into the cell list the subtitle loop queries. Bounded by POSITION rather
// than value (nothing to filter on a classification).
fn forced_subs(attr: Option<String>) -> Vec<ForcedSub> {
    attr.map(|s| {
        s.split(',')
            .take(MAX_COM_INDICES)
            .map(forced_sub_cell)
            .collect()
    })
    .unwrap_or_default()
}

fn forced_sub_cell(cell: &str) -> ForcedSub {
    match cell.trim() {
        "1" => ForcedSub::ContainsForcedSegments,
        "2" | "3" => ForcedSub::ForcedNarrative,
        _ => ForcedSub::None,
    }
}

// Build the stream labels from a single `<playlist .../>` feature element.
// Split out from `parse` so numbering/commentary/forced-index logic is
// unit-testable without a `SectorSource`/`UdfFs`.
fn labels_from_feature(feature: &str) -> Vec<StreamLabel> {
    let mut labels = Vec::new();

    // Parse audio streams
    if let Some(aud) = xml::attr(feature, "aud") {
        // aud_com1_idx: trimmed CSV positions, symmetric with sub_com1_idx.
        // HashSet not Vec: parsed from an attacker-controlled attribute and
        // only membership-tested; a Vec would make large inputs quadratic.
        let com_indices = com_indices(xml::attr(feature, "aud_com1_idx"));

        // The CSV *is* the STN list, so `stream_number` is each cell's 1-based
        // position, not a counter over labeled cells (empty cells still occupy
        // their slot). `u16::try_from`: stop rather than wrap onto `u16::MAX`.
        for (i, lang) in aud.split(',').enumerate() {
            let Ok(stream_number) = u16::try_from(i + 1) else {
                break;
            };
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }
            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };
            labels.push(StreamLabel {
                stream_id: None,
                stream_number,
                stream_type: StreamLabelType::Audio,
                language: lang.to_string(),
                name: String::new(),
                purpose,
                qualifier: LabelQualifier::None,
                codec_hint: String::new(),
                variant: String::new(),
            });
        }
    }

    // Parse subtitle streams
    if let Some(sub) = xml::attr(feature, "sub") {
        let forced = forced_subs(xml::attr(feature, "forced_sub"));

        // HashSet for the same reason as the audio side above: unbounded
        // parsed input, membership-only use, linear scan once per stream.
        let com_indices = com_indices(xml::attr(feature, "sub_com1_idx"));

        // As with audio: the cell position IS the STN slot, indexed by
        // `forced_sub`/`sub_com1_idx`, so an empty cell must not renumber
        // the rest — else a forced marker lands on the wrong subtitle track.
        for (i, lang) in sub.split(',').enumerate() {
            let Ok(stream_number) = u16::try_from(i + 1) else {
                break;
            };
            let lang = lang.trim();
            if lang.is_empty() {
                continue;
            }

            let purpose = if com_indices.contains(&i) {
                LabelPurpose::Commentary
            } else {
                LabelPurpose::Normal
            };

            // Only a DEDICATED forced-narrative slot earns the forced flag.
            // A cell marking a full track as merely containing forced segments
            // is dropped, not weakened into a forced label (see [`ForcedSub`]).
            let qualifier = match forced.get(i).copied().unwrap_or(ForcedSub::None) {
                ForcedSub::ForcedNarrative => LabelQualifier::Forced,
                ForcedSub::ContainsForcedSegments | ForcedSub::None => LabelQualifier::None,
            };

            labels.push(StreamLabel {
                stream_id: None,
                stream_number,
                stream_type: StreamLabelType::Subtitle,
                language: lang.to_string(),
                name: String::new(),
                purpose,
                qualifier,
                codec_hint: String::new(),
                variant: String::new(),
            });
        }
    }

    labels
}

/// The feature playlist hint for a whole `playlists.xml` document — selects the
/// feature element and derives its id/filename. Exposed for the generic BD-J
/// menu-walk (`bdj_feature`) Tier-1 sweep, which finds this manifest embedded
/// inside a jar rather than as a loose file.
pub(crate) fn feature_hint(text: &str) -> Option<super::FeaturePlaylistHint> {
    element_hint(find_feature_playlist(text)?)
}

// A `<playlist>` element's stated running time in seconds, if any. Read from the
// first present of a set of duration-like attributes (the corpus is not a spec;
// `durs` matches the sibling Fox manifest's seconds convention).
fn playlist_duration_secs(element: &str) -> Option<u64> {
    super::stated_duration_secs(
        element,
        &["duration", "durs", "dur", "runtime", "length", "len"],
    )
}

// Whether a playlist name has the WORD "feature" (`_Feature`, `MainFeature`) and no
// extras word (`Feature_Trailer`, `FeatureCommentary`); `Featurette` is not "feature".
fn names_feature(name: &str) -> bool {
    const EXTRAS: &[&str] = &[
        "trailer",
        "trailers",
        "teaser",
        "commentary",
        "bonus",
        "featurette",
        "promo",
        "preview",
        "previews",
        "extra",
        "extras",
        "making",
        "deleted",
        "scenes",
        "behind",
        "interview",
        "recap",
        "sneak",
    ];
    let words = super::name_words(name);
    words.iter().any(|w| w == "feature") && !words.iter().any(|w| EXTRAS.contains(&w.as_str()))
}

// A feature-selection candidate: the element text, its non-empty audio-slot
// count, and its stated duration (seconds) when known.
#[derive(Clone, Copy)]
struct Candidate<'a> {
    element: &'a str,
    aud: usize,
    dur: Option<u64>,
}

impl Candidate<'_> {
    // A candidate whose duration is stated AND sub-minute can never be the
    // feature — the _Start_Angle decoy guard.
    fn is_sub_minute(&self) -> bool {
        self.dur.is_some_and(|d| d < MIN_FEATURE_SECS)
    }
}

/// Find the feature playlist element. Order, refined for Sony SM3 UHD:
///   1. exact `name="Feature"` (case-insensitive) — longest-duration among any,
///      else first;
///   2. else any name containing the word "feature" (`_Feature`, `Feature_A`, …)
///      with at least one audio slot — longest-duration, else most audio;
///   3. else the most-audio playlist overall, breaking ties by longest duration.
///
/// A playlist whose stated duration is sub-minute is never returned (the
/// `_Start_Angle` decoy).
fn find_feature_playlist(text: &str) -> Option<&str> {
    let mut exact: Vec<Candidate> = Vec::new();
    let mut feature_like: Vec<Candidate> = Vec::new();
    let mut all: Vec<Candidate> = Vec::new();
    let mut from = 0;

    while let Some((start, end)) = xml::find_element(text, "playlist", from) {
        let element = &text[start..end];
        from = end;

        // Count only non-empty audio slots so a malformed `aud=",,,,,"` can't
        // outscore a legitimate feature.
        let aud = xml::attr(element, "aud")
            .map(|a| a.split(',').filter(|s| !s.trim().is_empty()).count())
            .unwrap_or(0);
        let dur = playlist_duration_secs(element);
        let cand = Candidate { element, aud, dur };

        match xml::attr(element, "name") {
            Some(name) if name.eq_ignore_ascii_case("Feature") => exact.push(cand),
            Some(name) if names_feature(&name) => feature_like.push(cand),
            _ => {}
        }
        all.push(cand);
    }

    // Tier 1: exact name="Feature" — longest duration, else first. A
    // zero-audio exact feature is still valid (its id feeds the hint).
    if let Some(el) = pick(&exact, Key::Duration, false) {
        return Some(el);
    }
    // Tier 2: "feature"-word names (Sony `_Feature`, `Feature_A`, `Feature_B`) —
    // longest duration, else most audio. Unlike the exact tier, a fuzzy name needs
    // audio: a zero-audio `_Feature` is weaker evidence than tier 3's audio count.
    if let Some(el) = pick(&feature_like, Key::Duration, true) {
        return Some(el);
    }
    // Tier 3: most audio across all playlists, longest duration as the tiebreak.
    pick(&all, Key::Audio, true)
}

// Which signal dominates candidate ranking: duration (tiers 1-2) or audio-slot
// count (tier 3). The other is the tiebreak.
#[derive(Clone, Copy)]
enum Key {
    Duration,
    Audio,
}

// Choose the best candidate under `key`: skip sub-minute decoys; first-wins on a
// full tie (strict `>`). When `require_audio`, a candidate needs at least one
// audio slot to be eligible. `None` duration sorts below any stated duration.
fn pick<'a>(cands: &[Candidate<'a>], key: Key, require_audio: bool) -> Option<&'a str> {
    let rank = |c: &Candidate| match key {
        Key::Duration => (c.dur, c.aud as u64),
        Key::Audio => (Some(c.aud as u64), c.dur.unwrap_or(0)),
    };
    let mut best: Option<&Candidate<'a>> = None;
    for c in cands {
        if c.is_sub_minute() {
            continue;
        }
        if require_audio && c.aud == 0 {
            continue;
        }
        if best.is_none_or(|b| rank(c) > rank(b)) {
            best = Some(c);
        }
    }
    best.map(|c| c.element)
}

#[cfg(test)]
#[path = "paramount_tests.rs"]
mod tests;

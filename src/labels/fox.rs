//! Fox — loose `/BDMV/JAR/<id>/dcx.xml` plain-XML manifest.
//!
//! Root `<dcx><disc>` lists playlists; the feature playlist's nested `<audio>`/`<subtitle>`
//! elements name language, purpose and forced/SDH state directly, with no bytecode to decode.
//! NOT A SPECIFICATION — this is one authoring house's internal metadata; every field meaning
//! was read off a real disc.
//!
//! ```xml
//! <dcx>
//!   <disc>
//!     <playlist id="00001" lang="eng" name="topmenu"/>
//!     ...
//!     <playlist id="00800" lang="eng" name="feature" vers="1" durs="7628">
//!       <audio id="01" lang="eng" type="feature"/>
//!       <audio id="02" lang="eng" type="rnib"/>
//!       <audio id="03" lang="spa" dial="lat" type="feature"/>
//!       ...
//!       <subtitle id="01" lang="eng" type="feature" form="sdh"/>
//!       <subtitle id="02" lang="spa" dial="lat" type="embed"/>
//!       ...
//!       <subtitle id="11" lang="eng" type="text"/>
//!       <properties> ...chapter marks... </properties>
//!     </playlist>
//!     <playlist id="00801" lang="jpn" name="feature" vers="1" durs="7628"> ... </playlist>
//!   </disc>
//! </dcx>
//! ```

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

/// Detect a Fox disc.
///
/// Primary signal: a loose `dcx.xml` under `/BDMV/JAR/<id>/` (the manifest
/// [`parse`] reads), checked first as a cheap directory walk.
///
/// Secondary signal: a `com/foxbd/` prefix in a top-level BD-J jar (newer Fox
/// discs ship no loose `dcx.xml`). This attributes the disc to Fox even
/// though [`parse`] cannot decode that bytecode form yet — see the Phase 2
/// note at the bottom of this file.
pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    if super::jar_file_exists(udf, "dcx.xml") {
        return true;
    }
    super::jar::any_jar_has_prefix(reader, udf, "com/foxbd/")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "dcx.xml")?;
    let text = std::str::from_utf8(&data).ok()?;

    let feature = select_feature_playlist(text)?;
    let labels = labels_from_feature(feature);
    if labels.is_empty() {
        return None;
    }

    // High confidence: the manifest is fully structured and every field
    // extracted here has its meaning fixed by real discs.
    let mut result = ParseResult::high(labels);
    // Surface the feature playlist's authoring id (e.g. "00800") so title
    // selection can prefer the disc's own feature over a size-inflated decoy —
    // the same signal `paramount::parse` provides.
    result.feature_playlist = element_hint(feature);
    Some(result)
}

/// The feature playlist hint for a `dcx.xml` doc: the selected feature's
/// authoring id (see [`super::FeaturePlaylistHint::from_authoring_id`]).
pub(crate) fn feature_hint(text: &str) -> Option<super::FeaturePlaylistHint> {
    element_hint(select_feature_playlist(text)?)
}

fn element_hint(feature: &str) -> Option<super::FeaturePlaylistHint> {
    super::FeaturePlaylistHint::from_authoring_id(&xml::attr(feature, "id")?)
}

// Build stream labels from a `dcx.xml` doc (test/harness entry). Scoped to the
// richest `<playlist name="feature">` element.
#[cfg(test)]
pub(crate) fn labels_from_dcx(text: &str) -> Vec<StreamLabel> {
    select_feature_playlist(text)
        .map(labels_from_feature)
        .unwrap_or_default()
}

/// Per-type label cap; dcx.xml is untrusted disc input.
const MAX_LABELS_PER_TYPE: usize = 512;

// Stream labels from the selected `<playlist name="feature">` element.
fn labels_from_feature(feature: &str) -> Vec<StreamLabel> {
    let mut labels = Vec::new();

    // Audio streams.
    let mut from = 0;
    while let Some((s, e)) = xml::find_element(feature, "audio", from) {
        if labels.len() >= MAX_LABELS_PER_TYPE {
            break;
        }
        let el = &feature[s..e];
        from = e;
        let Some(stream_number) = stream_number_from_id(&xml::attr(el, "id")) else {
            continue;
        };
        let Some(language) = xml::attr(el, "lang") else {
            continue;
        };
        let ty = xml::attr(el, "type")
            .unwrap_or_default()
            .to_ascii_lowercase();
        labels.push(StreamLabel {
            stream_id: None,
            stream_number,
            stream_type: StreamLabelType::Audio,
            language: normalize_language(&language),
            name: String::new(),
            purpose: audio_purpose(&ty),
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        });
    }

    // Subtitle streams.
    let mut from = 0;
    let audio_count = labels.len();
    while let Some((s, e)) = xml::find_element(feature, "subtitle", from) {
        if labels.len() - audio_count >= MAX_LABELS_PER_TYPE {
            break;
        }
        let el = &feature[s..e];
        from = e;
        let Some(stream_number) = stream_number_from_id(&xml::attr(el, "id")) else {
            continue;
        };
        let Some(language) = xml::attr(el, "lang") else {
            continue;
        };
        let ty = xml::attr(el, "type")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let form = xml::attr(el, "form")
            .unwrap_or_default()
            .to_ascii_lowercase();
        labels.push(StreamLabel {
            stream_id: None,
            stream_number,
            stream_type: StreamLabelType::Subtitle,
            language: normalize_language(&language),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: subtitle_qualifier(&ty, &form),
            codec_hint: String::new(),
            variant: String::new(),
        });
    }

    labels
}

// A feature runs at least this long (`durs` is seconds — durs=7628 is a 2h7m
// feature). A stated sub-minute `name="feature"` is a decoy and is never
// selected; the guard is inert when no `durs` is present. Shared with paramount.
use super::MIN_FEATURE_SECS;

// The `durs` (seconds) of a `<playlist>` element, digits only, when stated.
fn playlist_duration_secs(element: &str) -> Option<u64> {
    super::stated_duration_secs(element, &["durs"])
}

// Pick the richest `<playlist name="feature">` (most nested streams; a `durs` tiebreak breaks a
// count tie by longest time; first feature wins a full tie; a stated sub-minute feature is
// skipped).
fn select_feature_playlist(text: &str) -> Option<&str> {
    // rank = (nested stream count, durs seconds). None durs sorts as 0 so it
    // never displaces a stream-count-equal sibling that states a longer time.
    let mut best: Option<(&str, (usize, u64))> = None;
    let mut from = 0;
    while let Some((s, e)) = xml::find_element(text, "playlist", from) {
        let element = &text[s..e];
        from = e;
        // `name` is read from the element; the opening tag's `name="feature"`
        // is the first `name=` in document order, ahead of any nested child
        // (nested `<audio>`/`<subtitle>` carry no `name`).
        let is_feature =
            xml::attr(element, "name").is_some_and(|n| n.eq_ignore_ascii_case("feature"));
        if !is_feature {
            continue;
        }
        let dur = playlist_duration_secs(element);
        // A stated sub-minute feature is a decoy — never the real feature.
        if dur.is_some_and(|d| d < MIN_FEATURE_SECS) {
            continue;
        }
        let streams = count_elements(element, "audio") + count_elements(element, "subtitle");
        let rank = (streams, dur.unwrap_or(0));
        // Strictly-better rank displaces; a full tie keeps the first feature.
        // `best.is_none()` so a feature with zero nested streams is still
        // selected — its id feeds the hint.
        if best.is_none_or(|(_, br)| rank > br) {
            best = Some((element, rank));
        }
    }
    best.map(|(el, _)| el)
}

/// Count `<tag>` elements inside `element`.
fn count_elements(element: &str, tag: &str) -> usize {
    let mut n = 0;
    let mut from = 0;
    while let Some((_, e)) = xml::find_element(element, tag, from) {
        n += 1;
        from = e;
    }
    n
}

// Parse a nested stream `id` into its 1-based STN slot; digits only. `None`
// when it names no slot — 0 is the module's NO_STN_SLOT sentinel, not
// bindable by the ordinal path.
fn stream_number_from_id(id: &Option<String>) -> Option<u16> {
    let id = id.as_deref()?;
    let digits: String = id.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse::<u16>().ok().filter(|&n| n != 0)
}

/// Map an `<audio type>` value to a [`LabelPurpose`]. `rnib` is described-video
/// (descriptive/narration); a `comment*` value is commentary; everything else
/// (`feature`, unknown) is a normal program track.
fn audio_purpose(ty: &str) -> LabelPurpose {
    if ty == "rnib" {
        LabelPurpose::Descriptive
    } else if ty.contains("comment") {
        LabelPurpose::Commentary
    } else {
        LabelPurpose::Normal
    }
}

// Map subtitle `form`/`type` to a LabelQualifier: `form="sdh"` takes
// precedence; else `type="embed"` is forced; `feature`/`text` carry no
// qualifier.
fn subtitle_qualifier(ty: &str, form: &str) -> LabelQualifier {
    if form == "sdh" {
        LabelQualifier::Sdh
    } else if ty == "embed" {
        LabelQualifier::Forced
    } else {
        LabelQualifier::None
    }
}

/// Trim + lowercase the raw ISO 639-2 code (`"eng"`, `"fra"`, ...).
fn normalize_language(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

// Phase 2 (design only, not implemented): newer Fox discs ship no loose `dcx.xml`,
// wrapping the same per-stream data in `com/foxbd` BD-J `.class` bytecode instead. A
// follow-on parser would reuse `super::class_reader`/`jar` (as `dbp`/`deluxe` do).

#[cfg(test)]
#[path = "fox_tests.rs"]
mod tests;

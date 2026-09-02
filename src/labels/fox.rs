//! Fox — loose `/BDMV/JAR/<id>/dcx.xml` plain-XML manifest.
//!
//! Root `<dcx><disc>` lists playlists; the feature playlist's nested
//! `<audio>`/`<subtitle>` elements name language, purpose and forced/SDH
//! state directly, with no bytecode to decode. NOT A SPECIFICATION — this is
//! one authoring house's internal metadata; every field meaning was read off
//! a real disc. See docs/fox.md for the full field mapping, confirmed
//! schema, and feature-playlist scoping rationale.
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
    super::jar::for_each_jar(reader, udf, |_, jar| {
        super::jar::has_path_prefix(jar, "com/foxbd/").then_some(())
    })
    .is_some()
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let data = super::read_jar_file(reader, udf, "dcx.xml")?;
    let text = std::str::from_utf8(&data).ok()?;

    let labels = labels_from_dcx(text);
    if labels.is_empty() {
        return None;
    }

    // High confidence: the manifest is fully structured and every field
    // extracted here has its meaning fixed by real discs.
    let mut result = ParseResult::high(labels);
    // Surface the feature playlist's authoring id (e.g. "00800") so title
    // selection can prefer the disc's own feature over a size-inflated decoy —
    // the same signal `paramount::parse` provides.
    result.feature_playlist = feature_playlist_id(text).map(|id| super::FeaturePlaylistHint {
        playlist_id: id.parse::<u16>().ok(),
        filename: Some(format!("{id}.mpls")),
    });
    Some(result)
}

// Build stream labels from a `dcx.xml` doc; split out from `parse` for unit
// testing. Scoped to the richest `<playlist name="feature">` element — see
// docs/fox.md for why merging playlists is wrong.
pub(crate) fn labels_from_dcx(text: &str) -> Vec<StreamLabel> {
    let Some(feature) = select_feature_playlist(text) else {
        return Vec::new();
    };

    let mut labels = Vec::new();

    // Audio streams.
    let mut from = 0;
    while let Some((s, e)) = xml::find_element(feature, "audio", from) {
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
    while let Some((s, e)) = xml::find_element(feature, "subtitle", from) {
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

// Pick the richest `<playlist name="feature">` element (ties to first) —
// Fox presses one per regional variant. Returning ONE avoids merging two
// STN tables. See docs/fox.md for full rationale.
fn select_feature_playlist(text: &str) -> Option<&str> {
    let mut best: Option<&str> = None;
    let mut best_streams = 0usize;
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
        let streams = count_elements(element, "audio") + count_elements(element, "subtitle");
        // First feature wins the tie (matches the registry's first-wins rule);
        // a strictly richer table displaces it. `best.is_none()` so a feature
        // with zero nested streams is still selected — its id feeds the hint.
        if best.is_none() || streams > best_streams {
            best_streams = streams;
            best = Some(element);
        }
    }
    best
}

/// The authoring id (digits only, e.g. "00800") of the selected feature
/// playlist, or `None` when no feature playlist / no id is present.
fn feature_playlist_id(text: &str) -> Option<String> {
    let feature = select_feature_playlist(text)?;
    let id = xml::attr(feature, "id")?;
    let digits: String = id.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    Some(digits)
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
mod tests {
    use super::*;

    // Real Fox-release manifest, reduced only by truncating chapter-mark
    // `<properties>` blocks. Verbatim `/BDMV/JAR/05001/dcx.xml` audio/subtitle
    // data exercising feature selection, scoping, and rnib/sdh/embed flags.
    const FOX_DCX_SAMPLE: &str = r#"<dcx>
	<disc>
		<properties region="ABC" regioncheckon="false" parentallevel="PG" hdronlydisc="true" bootstrap.bdjo="88888"
		            vstaskenabled="false" defaultversion="1" topmenushowmarkid="0" topmenuloopmarkid="0"/>

		<playlist id="00001" lang="eng" name="topmenu"/>
		<playlist id="00100" lang="eng" name="foxlogo"/>
		<playlist id="00300" lang="eng" name="copyright"/> <!-- English -->
		<playlist id="00600" name="black" lang="eng"/>

		<playlist id="00800" lang="eng" name="feature" vers="1" durs="7628">
			<audio id="01" lang="eng" type="feature"/>
			<audio id="02" lang="eng" type="rnib"/>
			<audio id="03" lang="spa" dial="lat" type="feature"/>
			<audio id="04" lang="fra" dial="par" type="feature"/>
			<audio id="05" lang="dan" type="feature"/>
			<audio id="06" lang="nld" type="feature"/>
			<audio id="07" lang="fin" type="feature"/>
			<audio id="08" lang="deu" type="feature"/>
			<audio id="09" lang="ita" type="feature"/>
			<audio id="10" lang="nor" type="feature"/>
			<audio id="11" lang="swe" type="feature"/>
			<subtitle id="01" lang="eng" type="feature" form="sdh"/>
			<subtitle id="02" lang="spa" dial="lat" type="embed"/>
			<subtitle id="03" lang="fra" dial="par" type="embed"/>
			<subtitle id="04" lang="dan" type="embed"/>
			<subtitle id="05" lang="nld" type="embed"/>
			<subtitle id="06" lang="fin" type="feature"/>
			<subtitle id="07" lang="deu" type="embed"/>
			<subtitle id="08" lang="ita" type="embed"/>
			<subtitle id="09" lang="nor" type="embed"/>
			<subtitle id="10" lang="swe" type="embed"/>
			<subtitle id="11" lang="eng" type="text"/>
			<properties>
				<entry.marks ids="0,2,4,6,8,10"/>
				<playlist.marks timecodes="00:00:00:00,00:03:58:18"/>
			</properties>
		</playlist>

		<playlist id="00801" lang="jpn" name="feature" vers="1" durs="7628">
			<audio id="01" lang="eng" type="feature"/>
			<audio id="02" lang="jpn" type="feature"/>
			<subtitle id="01" lang="jpn" type="feature"/>
			<subtitle id="02" lang="eng" type="feature" form="sdh"/>
			<subtitle id="03" lang="jpn" type="text"/>
			<subtitle id="04" lang="eng" type="text"/>
			<properties>
				<entry.marks ids="0,2,4"/>
			</properties>
		</playlist>
	</disc>
</dcx>"#;

    fn audio(labels: &[StreamLabel]) -> Vec<&StreamLabel> {
        labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect()
    }

    fn subs(labels: &[StreamLabel]) -> Vec<&StreamLabel> {
        labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect()
    }

    /// The primary feature is the richer `00800` (eng) table, not `00801`
    /// (jpn). Both are `name="feature"`; selection must pick by stream count,
    /// and the id we surface for the (future) FeaturePlaylistHint is `00800`.
    #[test]
    fn selects_richest_feature_playlist_and_its_id() {
        assert_eq!(
            feature_playlist_id(FOX_DCX_SAMPLE),
            Some("00800".to_string())
        );
    }

    /// Full real-disc parse: the 00800 audio table. Eleven tracks, id order =
    /// STN slot, and slot 2 (`eng rnib`) is the descriptive/narration track.
    #[test]
    fn fox_dcx_audio_labels() {
        let labels = labels_from_dcx(FOX_DCX_SAMPLE);
        let a = audio(&labels);
        assert_eq!(
            a.len(),
            11,
            "00800 has eleven audio tracks, 00801 not merged"
        );

        // Slots and languages, in id order.
        let got: Vec<(u16, &str, LabelPurpose)> = a
            .iter()
            .map(|l| (l.stream_number, l.language.as_str(), l.purpose))
            .collect();
        assert_eq!(
            got,
            vec![
                (1, "eng", LabelPurpose::Normal),
                (2, "eng", LabelPurpose::Descriptive), // rnib = described video
                (3, "spa", LabelPurpose::Normal),
                (4, "fra", LabelPurpose::Normal),
                (5, "dan", LabelPurpose::Normal),
                (6, "nld", LabelPurpose::Normal),
                (7, "fin", LabelPurpose::Normal),
                (8, "deu", LabelPurpose::Normal),
                (9, "ita", LabelPurpose::Normal),
                (10, "nor", LabelPurpose::Normal),
                (11, "swe", LabelPurpose::Normal),
            ]
        );
        // Vendor labels: no StreamId, they bind by ordinal STN slot.
        assert!(a.iter().all(|l| l.stream_id.is_none()));
    }

    /// Full real-disc parse: the 00800 subtitle table. `form="sdh"` → Sdh,
    /// `type="embed"` → Forced, `feature`/`text` → no qualifier.
    #[test]
    fn fox_dcx_subtitle_labels() {
        let labels = labels_from_dcx(FOX_DCX_SAMPLE);
        let s = subs(&labels);
        assert_eq!(s.len(), 11, "00800 has eleven subtitle tracks");

        let got: Vec<(u16, &str, LabelQualifier)> = s
            .iter()
            .map(|l| (l.stream_number, l.language.as_str(), l.qualifier))
            .collect();
        assert_eq!(
            got,
            vec![
                (1, "eng", LabelQualifier::Sdh),    // feature + form=sdh
                (2, "spa", LabelQualifier::Forced), // embed
                (3, "fra", LabelQualifier::Forced),
                (4, "dan", LabelQualifier::Forced),
                (5, "nld", LabelQualifier::Forced),
                (6, "fin", LabelQualifier::None), // feature
                (7, "deu", LabelQualifier::Forced),
                (8, "ita", LabelQualifier::Forced),
                (9, "nor", LabelQualifier::Forced),
                (10, "swe", LabelQualifier::Forced),
                (11, "eng", LabelQualifier::None), // text
            ]
        );
    }

    // Nested-scope rule: audio/subtitle come from ONE feature playlist, never
    // a document-wide scan merging 00800/00801 id="01" slots. Regression
    // would double slot-1 audio and push the count past 11.
    #[test]
    fn does_not_merge_regional_feature_playlists() {
        let labels = labels_from_dcx(FOX_DCX_SAMPLE);
        let a = audio(&labels);
        // Exactly one audio label per STN slot 1..=11.
        let slot1: Vec<_> = a.iter().filter(|l| l.stream_number == 1).collect();
        assert_eq!(slot1.len(), 1, "one stream on slot 1, not one per playlist");
        assert_eq!(audio(&labels).len() + subs(&labels).len(), 22);
    }

    /// The `rnib` described-video mapping in isolation, plus the commentary
    /// path (no track in the sample manifest uses it, so it is pinned synthetically).
    #[test]
    fn audio_purpose_mapping() {
        assert_eq!(audio_purpose("feature"), LabelPurpose::Normal);
        assert_eq!(audio_purpose("rnib"), LabelPurpose::Descriptive);
        assert_eq!(audio_purpose("commentary"), LabelPurpose::Commentary);
        assert_eq!(
            audio_purpose("director-commentary"),
            LabelPurpose::Commentary
        );
        assert_eq!(audio_purpose("unknown"), LabelPurpose::Normal);
    }

    /// `form="sdh"` outranks `type`; `embed` is forced; full tracks are None.
    #[test]
    fn subtitle_qualifier_mapping() {
        assert_eq!(subtitle_qualifier("feature", "sdh"), LabelQualifier::Sdh);
        assert_eq!(subtitle_qualifier("embed", ""), LabelQualifier::Forced);
        assert_eq!(subtitle_qualifier("feature", ""), LabelQualifier::None);
        assert_eq!(subtitle_qualifier("text", ""), LabelQualifier::None);
        // An SDH embedded track (hypothetical) still flags SDH, the richer fact.
        assert_eq!(subtitle_qualifier("embed", "sdh"), LabelQualifier::Sdh);
    }

    /// `id="NN"` becomes the 1-based STN slot; a missing/zero id names no slot.
    #[test]
    fn stream_number_from_id_digits_only() {
        assert_eq!(stream_number_from_id(&Some("01".into())), Some(1));
        assert_eq!(stream_number_from_id(&Some("11".into())), Some(11));
        assert_eq!(stream_number_from_id(&Some(" 07 ".into())), Some(7));
        assert_eq!(stream_number_from_id(&Some("00".into())), None);
        assert_eq!(stream_number_from_id(&None), None);
        assert_eq!(stream_number_from_id(&Some("".into())), None);
    }

    // ── Negative detection: a non-Fox-feature document yields no labels —
    // empty, all-menu/logo playlists (no name="feature"), and unrelated XML,
    // none mistaken for a feature table.
    #[test]
    fn non_feature_manifest_yields_no_labels() {
        assert!(labels_from_dcx("").is_empty());
        assert!(labels_from_dcx("<not-dcx><playlist name=\"x\"/></not-dcx>").is_empty());
        let menus_only = r#"<dcx><disc>
            <playlist id="00001" lang="eng" name="topmenu"/>
            <playlist id="00100" lang="eng" name="foxlogo"/>
            <playlist id="00600" name="black" lang="eng"/>
        </disc></dcx>"#;
        assert!(
            labels_from_dcx(menus_only).is_empty(),
            "no <playlist name=\"feature\"> → nothing to label"
        );
        assert_eq!(feature_playlist_id(menus_only), None);
    }

    /// A feature playlist with no nested streams (an authoring edge) produces
    /// no labels rather than a spurious empty-slot entry.
    #[test]
    fn feature_without_streams_yields_no_labels() {
        let doc = r#"<dcx><disc>
            <playlist id="00800" lang="eng" name="feature" durs="7628"/>
        </disc></dcx>"#;
        assert!(labels_from_dcx(doc).is_empty());
        // ...but the id is still recoverable for the feature hint.
        assert_eq!(feature_playlist_id(doc), Some("00800".to_string()));
    }
}

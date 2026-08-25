//! Fox — loose `/BDMV/JAR/<id>/dcx.xml` plain-XML manifest.
//!
//! Older Fox authoring (e.g. *Life of Pi*, `/BDMV/JAR/05001/dcx.xml`) ships a
//! human-readable XML manifest alongside the BD-J jar. Its root is `<dcx>`, and
//! under `<disc>` it lists every playlist the disc plays. The main-feature
//! playlists carry nested per-stream `<audio>`/`<subtitle>` elements naming
//! language, editorial purpose and forced/SDH state outright — everything a
//! label parser wants, in attributes, with no bytecode to walk.
//!
//! NOT A SPECIFICATION. `/BDMV/JAR/` is application-defined space, so this file
//! is one authoring house's internal metadata that happens to press onto the
//! disc. Every field meaning below was read off a real disc; treat an
//! unfamiliar value as unknown rather than guessing.
//!
//! Confirmed schema (Life of Pi, `/BDMV/JAR/05001/dcx.xml`):
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
//!
//! Field meanings (read off the disc, not documented):
//!   * `<audio type>`: `feature` = a normal program track; `rnib` = a
//!     descriptive/narration track ("Royal National Institute of Blind People"
//!     described-video), mapped to [`LabelPurpose::Descriptive`]. A `type`
//!     containing "comment" maps to [`LabelPurpose::Commentary`].
//!   * `<subtitle form>`: `sdh` marks a subtitles-for-the-deaf-and-hard-of-
//!     hearing track ([`LabelQualifier::Sdh`]).
//!   * `<subtitle type>`: `embed` marks a dedicated forced/embedded-subtitle
//!     track ([`LabelQualifier::Forced`]); `feature`/`text` are full tracks.
//!   * `id` on a nested stream is its 1-based STN slot WITHIN ITS TYPE
//!     (audio ids 01..N, subtitle ids 01..N independently), matching the vendor
//!     `stream_number` convention the ordinal binder in [`super::apply_labels`]
//!     reads.
//!
//! The `<audio>`/`<subtitle>` elements are NESTED inside one feature playlist,
//! so extraction is scoped to a single `<playlist name="feature">` element —
//! never a document-wide `<audio>` scan, which would merge the regional
//! `00800` (eng) and `00801` (jpn) tables into one and collide their slots.

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, xml};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

/// Detect a Fox disc.
///
/// Primary, cheap signal: a loose `dcx.xml` under some `/BDMV/JAR/<id>/`
/// subdirectory — the manifest [`parse`] reads. Checked first because it is a
/// directory walk with no sector reads.
///
/// Secondary signal: a `com/foxbd/` prefix in a top-level BD-J jar, which newer
/// Fox discs (Deadpool et al.) carry instead of a loose `dcx.xml`. Recognising
/// it attributes the disc to Fox rather than the generic deluxe stub even
/// though [`parse`] cannot decode that bytecode form yet (see the Phase 2 note
/// at the bottom of this file); such a disc detects here but yields no labels,
/// so a lower-confidence parser still wins.
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

    // Surface the feature playlist's authoring id (e.g. "00800"). On this
    // branch `ParseResult` carries no `feature_playlist` field — that
    // `FeaturePlaylistHint` slot was added on the `selection-hardening` branch
    // and does not exist on `dev` — so we record the id in the trace stream for
    // now rather than clashing with that type. AT RECONCILIATION: populate
    // `ParseResult.feature_playlist = Some(FeaturePlaylistHint { playlist_id:
    // digits.parse(), filename: Some("<id>.mpls") })` here, exactly as
    // `paramount::parse` does, so title selection can prefer the authoring
    // feature over a size-inflated decoy.
    if let Some(id) = feature_playlist_id(text) {
        tracing::debug!(
            target: "freemkv::labels",
            fox_feature_playlist = %id,
            "Fox dcx.xml feature playlist id (wire to FeaturePlaylistHint at reconciliation)",
        );
    }

    // High confidence: the manifest is fully structured and every field
    // extracted here has its meaning fixed by real discs.
    Some(ParseResult::high(labels))
}

/// Build the stream labels from a `dcx.xml` document. Split out from [`parse`]
/// so the schema mapping is unit-testable without a `SectorSource`/`UdfFs`.
///
/// Scoped to ONE feature playlist element (see the module note): the richest
/// `<playlist name="feature">`. Its nested `<audio>`/`<subtitle>` children are
/// the vendor's STN table for that title.
fn labels_from_dcx(text: &str) -> Vec<StreamLabel> {
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

/// Pick the disc's primary feature playlist element (the full `<playlist ...>
/// ... </playlist>` text). Among all `<playlist name="feature">` elements —
/// Fox presses one per regional variant (`00800` eng, `00801` jpn) — the one
/// carrying the most `<audio>`/`<subtitle>` streams wins, ties to the first.
///
/// Returning ONE element is the whole point: each feature playlist is its own
/// STN table with its own 1-based slot ids, so merging two would put two
/// different streams on slot `01`. The richest table is the fullest label set
/// and anchors to its matching title by language sequence in
/// [`super::apply_labels`].
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

/// Parse a nested stream `id` (e.g. "01", "11") into its 1-based STN slot.
/// Digits only, so a stray quote/space cannot poison it; `None` (skip the
/// stream) when it names no slot — 0 is the module's `NO_STN_SLOT` sentinel and
/// cannot be bound by the ordinal path.
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

/// Map a `<subtitle>`'s `form`/`type` to a [`LabelQualifier`]. `form="sdh"`
/// takes precedence (an SDH track is the editorially meaningful flag);
/// otherwise `type="embed"` is a dedicated forced/embedded track. `feature`
/// and `text` full tracks carry no qualifier.
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

// ── Phase 2 (design only, not implemented here) ─────────────────────────────
//
// Newer Fox discs (Deadpool, etc., surveyed as 11 discs currently mis-owned by
// the deluxe stub) ship NO loose `dcx.xml`. They wrap the same per-stream data
// inside `com/foxbd` BD-J `.class` bytecode. The follow-on parser would reuse
// `super::class_reader` the way `dbp`/`deluxe` already do:
//
//   * `detect` already recognises the `com/foxbd/` central-directory prefix
//     (above), so Phase 2 only adds a parse path.
//   * Iterate the jar's classes via `super::jar::for_each_class` and read the
//     constant pool (`ClassFile`), which holds the authored strings verbatim:
//       - `FeatureIntroPlaylist.play: PLAYLIST ID:` → the feature playlist id
//         (→ the same `FeaturePlaylistHint` the dcx path should wire up),
//       - `RNIB` → a descriptive/narration audio track (→ Descriptive),
//       - `primaryAudioId=` and the parallel subtitle-id constants → the STN
//         slot numbering, mirroring the `id="NN"` mapping here.
//   * Emit the identical `StreamLabel` shape at High confidence so the two Fox
//     forms are indistinguishable downstream.

#[cfg(test)]
mod tests {
    use super::*;

    /// The real Life of Pi manifest, reduced only by truncating the giant
    /// chapter-mark `<properties>` blocks (which carry no stream labels). Every
    /// `<audio>`/`<subtitle>` element and both regional feature playlists are
    /// verbatim from `/BDMV/JAR/05001/dcx.xml`, so this fixture exercises the
    /// exact bytes production sees: feature selection across two `name="feature"`
    /// playlists, the nested-scope rule, and the rnib/sdh/embed flags.
    const LIFE_OF_PI_DCX: &str = r#"<dcx>
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
            feature_playlist_id(LIFE_OF_PI_DCX),
            Some("00800".to_string())
        );
    }

    /// Full real-disc parse: the 00800 audio table. Eleven tracks, id order =
    /// STN slot, and slot 2 (`eng rnib`) is the descriptive/narration track.
    #[test]
    fn life_of_pi_audio_labels() {
        let labels = labels_from_dcx(LIFE_OF_PI_DCX);
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
    fn life_of_pi_subtitle_labels() {
        let labels = labels_from_dcx(LIFE_OF_PI_DCX);
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

    /// The nested-scope rule: audio/subtitle come from ONE feature playlist,
    /// never a document-wide scan that would merge 00800 and 00801 and collide
    /// their `id="01"` slots. If scoping regressed, slot 1 audio would appear
    /// twice and the count would jump past 11.
    #[test]
    fn does_not_merge_regional_feature_playlists() {
        let labels = labels_from_dcx(LIFE_OF_PI_DCX);
        let a = audio(&labels);
        // Exactly one audio label per STN slot 1..=11.
        let slot1: Vec<_> = a.iter().filter(|l| l.stream_number == 1).collect();
        assert_eq!(slot1.len(), 1, "one stream on slot 1, not one per playlist");
        assert_eq!(audio(&labels).len() + subs(&labels).len(), 22);
    }

    /// The `rnib` described-video mapping in isolation, plus the commentary
    /// path (no real Life of Pi track uses it, so it is pinned synthetically).
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

    // ── Negative detection ─────────────────────────────────────────────────

    /// A document that is not a Fox feature manifest yields no labels. Three
    /// shapes: empty, a `<dcx>` whose playlists are all non-feature menu/logo
    /// clips (no `name="feature"`), and unrelated XML. None must be mistaken
    /// for a Fox feature table.
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

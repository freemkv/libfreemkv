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
        feature_hint(FOX_DCX_SAMPLE).and_then(|h| h.playlist_id),
        Some(800)
    );
}

// An id above u16::MAX yields no hint at all, never a filename-only half-hint.
#[test]
fn feature_hint_rejects_an_id_above_u16() {
    let doc =
        r#"<playlist name="feature" id="70000" durs="7000"><audio id="1" lang="eng"/></playlist>"#;
    assert_eq!(feature_hint(doc), None);
}

/// The feature hint's numeric id and filename are derived from ONE parsed
/// number, so they always name the same playlist — the id parses to 800 and
/// the filename is the canonical 5-digit `00800.mpls`, never a half-hint.
#[test]
fn feature_hint_id_and_filename_agree() {
    let h = feature_hint(FOX_DCX_SAMPLE).expect("the sample has a feature id");
    assert_eq!(h.playlist_id, Some(800));
    assert_eq!(h.filename.as_deref(), Some("00800.mpls"));
    assert!(!h.is_empty());
    // The two fields point at the same playlist.
    assert!(h.matches(800, "00800.mpls"));
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
fn labels_per_type_are_capped() {
    let mut f = String::new();
    for i in 1..=2000 {
        f.push_str(&format!("<audio id=\"{i}\" lang=\"eng\"/>"));
        f.push_str(&format!("<subtitle id=\"{i}\" lang=\"eng\"/>"));
    }
    let labels = labels_from_feature(&f);
    assert_eq!(labels.len(), 2 * MAX_LABELS_PER_TYPE);
}

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
    assert_eq!(feature_hint(menus_only), None);
}

/// On an equal nested-stream count, the longer `durs` (seconds) wins the
/// tiebreak rather than document order.
#[test]
fn feature_duration_breaks_a_stream_count_tie() {
    let doc = r#"<dcx><disc>
            <playlist id="00800" lang="eng" name="feature" durs="6000">
                <audio id="01" lang="eng" type="feature"/>
                <subtitle id="01" lang="eng" type="feature"/>
            </playlist>
            <playlist id="00900" lang="eng" name="feature" durs="8000">
                <audio id="01" lang="eng" type="feature"/>
                <subtitle id="01" lang="eng" type="feature"/>
            </playlist>
        </disc></dcx>"#;
    assert_eq!(feature_hint(doc).and_then(|h| h.playlist_id), Some(900));
}

/// A full tie (same stream count, same `durs`) keeps the first feature.
#[test]
fn full_tie_keeps_the_first_feature() {
    let doc = r#"<dcx><disc>
            <playlist id="00800" name="feature" durs="7000"><audio id="01" lang="eng"/></playlist>
            <playlist id="00900" name="feature" durs="7000"><audio id="01" lang="eng"/></playlist>
        </disc></dcx>"#;
    assert_eq!(feature_hint(doc).and_then(|h| h.playlist_id), Some(800));
}

/// Stream count outranks duration: a shorter but richer feature wins.
#[test]
fn stream_count_outranks_duration() {
    let doc = r#"<dcx><disc>
            <playlist id="00800" name="feature" durs="9000"><audio id="01" lang="eng"/></playlist>
            <playlist id="00900" name="feature" durs="7000">
                <audio id="01" lang="eng"/><audio id="02" lang="fra"/>
            </playlist>
        </disc></dcx>"#;
    assert_eq!(feature_hint(doc).and_then(|h| h.playlist_id), Some(900));
}

/// Attribute values from the untrusted manifest are matched case-insensitively.
#[test]
fn manifest_attribute_values_are_case_insensitive() {
    let doc = r#"<dcx><disc>
            <playlist id="00800" name="FEATURE" durs="7000">
                <audio id="01" lang="ENG" type="RNIB"/>
                <subtitle id="01" lang="ENG" type="Feature" form="SDH"/>
                <subtitle id="02" lang="FRA" type="EMBED"/>
            </playlist>
        </disc></dcx>"#;
    let labels = labels_from_dcx(doc);
    assert_eq!(labels.len(), 3);
    assert_eq!(labels[0].purpose, LabelPurpose::Descriptive);
    assert_eq!(labels[1].qualifier, LabelQualifier::Sdh);
    assert_eq!(labels[2].qualifier, LabelQualifier::Forced);
}

/// A stated sub-minute `name="feature"` is a decoy and is skipped in favour
/// of the real (longer) feature.
#[test]
fn sub_minute_feature_is_skipped() {
    let doc = r#"<dcx><disc>
            <playlist id="00050" lang="eng" name="feature" durs="4">
                <audio id="01" lang="eng" type="feature"/>
                <audio id="02" lang="fra" type="feature"/>
                <subtitle id="01" lang="eng" type="feature"/>
            </playlist>
            <playlist id="00800" lang="eng" name="feature" durs="7628">
                <audio id="01" lang="eng" type="feature"/>
            </playlist>
        </disc></dcx>"#;
    // 00050 has more nested streams but is sub-minute → rejected; 00800 wins.
    assert_eq!(feature_hint(doc).and_then(|h| h.playlist_id), Some(800));
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
    assert_eq!(feature_hint(doc).and_then(|h| h.playlist_id), Some(800));
}

use super::*;

#[test]
fn extracts_confirmed_samples() {
    assert_eq!(
        filename_lang("Feature_UHD01_Eng_Composite1.png"),
        Some("eng")
    );
    assert_eq!(
        filename_lang("Feature_UHD01_Ger_Composite2.png"),
        Some("deu")
    );
    assert_eq!(
        filename_lang("AltFeature_UHD01_FRE_Composite2.png"),
        Some("fra")
    );
}

#[test]
fn ignores_non_language_composites() {
    assert_eq!(filename_lang("KeyComposite4.png"), None);
    assert_eq!(filename_lang("LoadingComposite1.png"), None);
    assert_eq!(
        filename_lang("FourKWarningsComposite1_bt2020_HDR.png"),
        None
    );
    assert_eq!(filename_lang("Fast9_UPK75_Composite1.png"), None);
}

#[test]
fn unknown_language_token_is_none() {
    // A UHD01 marker but a token the vocab does not recognize must not
    // produce a bogus language.
    assert_eq!(filename_lang("Movie_UHD01_Zzz_Composite1.png"), None);
}

// A recognised language after the marker still needs the `_Composite` asset suffix.
#[test]
fn marker_without_composite_suffix_is_none() {
    assert_eq!(filename_lang("Feature_UHD01_Eng_Background.png"), None);
    assert_eq!(filename_lang("Feature_UHD01_Eng.png"), None);
}

#[test]
fn dedups_and_numbers_distinct_languages() {
    let names = vec![
        "Feature_UHD01_Eng_Composite1.png".to_string(),
        "Feature_UHD01_Eng_Composite2.png".to_string(),
        "Feature_UHD01_Ger_Composite1.png".to_string(),
        "LoadingComposite1.png".to_string(),
    ];
    let labels = labels_from_filenames(&names);
    assert_eq!(labels.len(), 2);
    assert_eq!(labels[0].language, "eng");
    assert_eq!(labels[0].stream_number, 1);
    assert_eq!(labels[1].language, "deu");
    assert_eq!(labels[1].stream_number, 2);
    assert!(
        labels
            .iter()
            .all(|l| l.stream_type == StreamLabelType::Audio)
    );
}

#[test]
fn no_matching_names_yields_empty() {
    let names = vec![
        "KeyComposite4.png".to_string(),
        "disc.properties".to_string(),
    ];
    assert!(labels_from_filenames(&names).is_empty());
}

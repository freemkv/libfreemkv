//! Menu-graphic filename language hints.
//!
//! Some BD-J discs encode per-language menu artwork with the language in the
//! filename, e.g. `Dune_UHD01_Eng_Composite1.png`,
//! `VForVendetta_UHD01_FRE_Composite2.png`. The `_UHD01_{LANG}_Composite`
//! marker is authored deliberately, so the set of `{LANG}` tokens is the set
//! of menu languages the disc ships.
//!
//! This is a language-only hint (no per-stream purpose/codec), so it runs at
//! [`Confidence::Low`] — it never displaces a real framework parser, and it
//! sits at the same tier as the MPLS floor. It is here so the pattern is a
//! first-class, testable parser that keeps picking up discs as the corpus
//! grows, rather than lost logic. Detection is precise: it fires only on the
//! `_UHD01_{LANG}_Composite` grammar with a `{LANG}` the vocab recognizes.

use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

pub fn detect(_reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    super::jar_inventory(udf)
        .iter()
        .any(|f| filename_lang(f).is_some())
}

pub fn parse(_reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    let names = super::jar_inventory(udf);
    let labels = labels_from_filenames(&names);
    if labels.is_empty() {
        return None;
    }
    // Low: language-only, derived from menu-asset filenames. A real framework
    // parser (and even the MPLS floor's per-stream data) is preferred; this is
    // a hint of which languages the disc menus offer.
    Some(ParseResult::low(labels))
}

/// One audio [`StreamLabel`] per distinct menu language found, in first-seen
/// order, numbered 1-based. Split out from `parse` so it is unit-testable
/// without a `UdfFs`.
fn labels_from_filenames(names: &[String]) -> Vec<StreamLabel> {
    let mut seen: Vec<&'static str> = Vec::new();
    for name in names {
        if let Some(code) = filename_lang(name) {
            if !seen.contains(&code) {
                seen.push(code);
            }
        }
    }
    seen.into_iter()
        .enumerate()
        .map(|(i, code)| StreamLabel {
            stream_number: (i as u16).saturating_add(1),
            stream_type: StreamLabelType::Audio,
            language: code.to_string(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        })
        .collect()
}

/// Extract the ISO-639-2 language code from a `{title}_UHD01_{LANG}_Composite`
/// menu-graphic filename, or `None` if the name does not match the grammar or
/// carries a `{LANG}` the vocab does not recognize.
///
/// The `_UHD01_` marker plus the `_Composite` suffix keep this from firing on
/// unrelated PNGs (`KeyComposite4.png`, `LoadingComposite1.png` have no
/// `_UHD01_{LANG}_` segment).
fn filename_lang(name: &str) -> Option<&'static str> {
    // Case-fold once; the marker/suffix are matched case-insensitively.
    let lower = name.to_ascii_lowercase();
    let marker = "_uhd01_";
    let m = lower.find(marker)?;
    let after = m + marker.len();
    // The language token runs from `after` up to the next `_`.
    let rest = &lower[after..];
    let end = rest.find('_')?;
    if !rest[end..].starts_with("_composite") {
        return None;
    }
    let token = &name[after..after + end];
    vocab::menu_lang(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_confirmed_samples() {
        assert_eq!(filename_lang("Dune_UHD01_Eng_Composite1.png"), Some("eng"));
        assert_eq!(filename_lang("Dune_UHD01_Ger_Composite2.png"), Some("deu"));
        assert_eq!(
            filename_lang("VForVendetta_UHD01_FRE_Composite2.png"),
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

    #[test]
    fn dedups_and_numbers_distinct_languages() {
        let names = vec![
            "Dune_UHD01_Eng_Composite1.png".to_string(),
            "Dune_UHD01_Eng_Composite2.png".to_string(),
            "Dune_UHD01_Ger_Composite1.png".to_string(),
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
}

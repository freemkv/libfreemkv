//! Shared label vocabulary — canonical mappings used by ≥1 label parser.
//!
//! Labels come from BD-J authoring tool files (bluray_project.bin,
//! playlists.xml, menu_base.prop, .class string pools, etc.) — NOT
//! from BD spec fields. This module is the central, regression-tested
//! source of truth for:
//!
//! - Codec brand name aliases (`MLP` → `TrueHD`).
//! - English / multi-word language name → ISO 639-2 code.
//! - English text → [`LabelPurpose`] (Commentary / Descriptive / etc.).
//! - English text → [`LabelQualifier`] (SDH / Forced / Descriptive Service).
//!
//! Rules of engagement:
//!
//! 1. Only map values we are 100% certain about — published codec
//!    names, well-known ISO 639-2 mappings, vendor-documented purpose
//!    keywords.
//! 2. Unknown codes / unrecognized phrases pass through raw or return
//!    `None`. We never guess.
//! 3. Matching is case-insensitive and word-boundary-aware where
//!    relevant (so "Commenter" doesn't match "commentary"). Anchoring
//!    on whole tokens is the responsibility of this module — callers
//!    pass raw text, we handle it.
//!
//! This module is NOT for BD spec STN codec IDs; those decode in
//! `mpls.rs` separately.

use super::{LabelPurpose, LabelQualifier};

// ── Codec aliases ────────────────────────────────────────────────────────────

/// Map a codec identifier found in label data to its display name.
///
/// These are well-known codec identifiers used across multiple BD-J
/// authoring tools. Matching is case-insensitive (on-disc tokens vary:
/// `ATMOS`, `Atmos`, `atmos`). Unknown codes pass through unchanged (in
/// their original casing) so callers can still surface vendor-specific
/// tokens we haven't catalogued.
pub fn codec(code: &str) -> &str {
    match code.to_ascii_uppercase().as_str() {
        "MLP" => "TrueHD",
        "AC3" | "AC" => "Dolby Digital",
        "DDL" => "Dolby Digital Plus",
        "WAV" => "PCM",
        "ATMOS" => "Dolby Atmos",
        // "DTS" is recognized but has no distinct display alias — return
        // the original token rather than a re-cased copy.
        _ => code,
    }
}

// ── Language: English / multi-word names → ISO 639-2 ─────────────────────────

/// Result of [`lang`] — ISO code + human-readable regional variant.
///
/// `code` is ISO 639-2 (always 3 lowercase letters).
/// `variant` is the regional dialect as a human-readable English word
/// (`"Brazilian"`, `"Castilian"`, `"Canadian"`, `"Simplified"`, ...)
/// or `""` when the input names just a bare language without
/// dialect ("Spanish" → variant=""). It is a short display token
/// suitable for the [`StreamLabel::variant`](super::StreamLabel) field,
/// to be surfaced verbatim by the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LangInfo {
    pub code: &'static str,
    pub variant: &'static str,
}

/// Map a free-form language label fragment to an ISO 639-2 code AND
/// (where applicable) its regional variant.
///
/// Handles both bare English names ("English", "Spanish") and the
/// multi-word vendor variants we've seen in the corpus ("Brazilian
/// Portuguese", "Castilian Spanish", "Canadian French"). Match is
/// case-insensitive. Compound phrases are scanned BEFORE bare names, so
/// "Brazilian Portuguese" returns
/// `LangInfo { code: "por", variant: "Brazilian" }` rather than being
/// consumed by the bare "Portuguese" entry. Within `COMPOUND_LANGS` the
/// scan is positional (first `contains` hit wins), so that table MUST be
/// maintained longest-first — a longer phrase must precede any shorter
/// phrase it contains (e.g. "latin american spanish" before
/// "latin spanish").
///
/// Bare-name matches return `variant: ""`.
///
/// Returns `None` for unrecognized input — callers decide whether to
/// fall back to MPLS spec codes, pass through raw, or drop the stream.
/// Never guesses.
///
/// Why the variant: returning only the ISO code would silently drop
/// regional dialect info — "Brazilian Portuguese 5.1" would become
/// `language="por", variant=""` and the UI would display plain
/// "Portuguese" even though the disc explicitly labeled the stream
/// Brazilian. Returning the variant lets callers populate
/// [`StreamLabel::variant`](super::StreamLabel) with the dialect.
pub fn lang(text: &str) -> Option<LangInfo> {
    let lower = text.to_lowercase();
    // Multi-word compounds first. Scan is positional (first hit wins),
    // so COMPOUND_LANGS MUST stay ordered longest-first.
    for (needle, code, variant) in COMPOUND_LANGS {
        if lower.contains(needle) {
            return Some(LangInfo { code, variant });
        }
    }
    // Bare names: word-boundary match (avoid "english" inside "englishman"
    // or any other accidental substring).
    for (needle, code) in BARE_LANGS {
        if has_word(&lower, needle) {
            return Some(LangInfo { code, variant: "" });
        }
    }
    None
}

const COMPOUND_LANGS: &[(&str, &str, &str)] = &[
    ("brazilian portuguese", "por", "Brazilian"),
    ("euro portuguese", "por", "European"),
    ("european portuguese", "por", "European"),
    ("castilian spanish", "spa", "Castilian"),
    ("latin american spanish", "spa", "Latin American"),
    ("latin spanish", "spa", "Latin American"),
    ("canadian french", "fra", "Canadian"),
    ("parisian french", "fra", "Parisian"),
    ("australian english", "eng", "Australian"),
    ("austrailian english", "eng", "Australian"), // disc-corpus typo, keep matching
    ("british english", "eng", "British"),
    ("simplified chinese", "zho", "Simplified"),
    ("traditional chinese", "zho", "Traditional"),
    ("mandarin chinese", "zho", "Mandarin"),
    ("cantonese chinese", "zho", "Cantonese"),
];

const BARE_LANGS: &[(&str, &str)] = &[
    ("english", "eng"),
    ("spanish", "spa"),
    ("french", "fra"),
    ("german", "deu"),
    ("italian", "ita"),
    ("japanese", "jpn"),
    ("chinese", "zho"),
    ("mandarin", "zho"),
    ("cantonese", "zho"),
    ("portuguese", "por"),
    ("polish", "pol"),
    ("czech", "ces"),
    ("hungarian", "hun"),
    ("dutch", "nld"),
    ("korean", "kor"),
    ("arabic", "ara"),
    ("hindi", "hin"),
    ("turkish", "tur"),
    ("thai", "tha"),
    ("swedish", "swe"),
    ("norwegian", "nor"),
    ("danish", "dan"),
    ("finnish", "fin"),
    ("hebrew", "heb"),
    ("russian", "rus"),
    ("greek", "ell"),
    ("vietnamese", "vie"),
    ("indonesian", "ind"),
    ("malay", "msa"),
    ("ukrainian", "ukr"),
    ("romanian", "ron"),
    ("bulgarian", "bul"),
    ("croatian", "hrv"),
    ("serbian", "srp"),
    ("slovak", "slk"),
    ("slovenian", "slv"),
    ("estonian", "est"),
    ("latvian", "lav"),
    ("lithuanian", "lit"),
    ("icelandic", "isl"),
    ("basque", "eus"),
    ("catalan", "cat"),
    ("galician", "glg"),
];

// ── Purpose ──────────────────────────────────────────────────────────────────

/// Classify a free-form English label string into a [`LabelPurpose`].
///
/// Recognized keywords (case-insensitive, word-boundary matched):
/// - "commentary", "director's commentary" → `Commentary`
/// - "descriptive", "description", "audio description", "described" → `Descriptive`
/// - "score", "music only" → `Score`
/// - "ime" (alternate music for closing themes etc.) → `Ime`
/// - anything else → `Normal`
///
/// Word-boundary matching means "Commentary track" matches but
/// "Commenter Pro audio" does not.
pub fn purpose(text: &str) -> LabelPurpose {
    let lower = text.to_lowercase();
    // Multi-word compounds first — they're more specific.
    if lower.contains("audio description") || lower.contains("descriptive service") {
        return LabelPurpose::Descriptive;
    }
    if lower.contains("music only") {
        return LabelPurpose::Score;
    }
    if has_word(&lower, "commentary") {
        return LabelPurpose::Commentary;
    }
    if has_word(&lower, "descriptive")
        || has_word(&lower, "description")
        || has_word(&lower, "described")
    {
        return LabelPurpose::Descriptive;
    }
    if has_word(&lower, "score") {
        return LabelPurpose::Score;
    }
    if has_word(&lower, "ime") {
        return LabelPurpose::Ime;
    }
    LabelPurpose::Normal
}

// ── Qualifier ────────────────────────────────────────────────────────────────

/// Classify a free-form English label string into a [`LabelQualifier`].
///
/// Recognized keywords (case-insensitive, word-boundary matched):
/// - "sdh", "captions" → `Sdh`
/// - "forced", "forced narrative" → `Forced`
/// - "rnib", "descriptive service" → `DescriptiveService`
/// - anything else → `None`
///
/// SDH (Subtitles for the Deaf and Hard of hearing) wins over Forced
/// when both keywords are present, because an SDH track is its own
/// stream regardless of whether the player flags it as "forced".
pub fn qualifier(text: &str) -> LabelQualifier {
    let lower = text.to_lowercase();
    if has_word(&lower, "sdh") || has_word(&lower, "captions") {
        return LabelQualifier::Sdh;
    }
    if lower.contains("descriptive service") || has_word(&lower, "rnib") {
        return LabelQualifier::DescriptiveService;
    }
    if has_word(&lower, "forced") {
        return LabelQualifier::Forced;
    }
    LabelQualifier::None
}

// ── Internal: word-boundary matching ────────────────────────────────────────

/// True if `needle` appears in `haystack` surrounded by non-alphanumeric
/// boundaries (or string ends). `haystack` is assumed lowercase already.
///
/// This is the load-bearing primitive for `lang` / `purpose` /
/// `qualifier`: bare-token matchers MUST use it, otherwise we match
/// "english" inside "englishman" and "sdh" inside "lambdash". The
/// existing parsers used `.contains()` and got lucky on the corpus;
/// vocab guarantees the boundary.
fn has_word(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    // Boundary check is char-aware (not byte-level): a non-ASCII letter
    // adjacent to the match (e.g. an accented or CJK char, which is
    // multiple UTF-8 bytes) is alphanumeric and so is NOT a boundary,
    // preventing false positives like "sdh" inside "cafésch". Needles
    // are ASCII tokens, so a byte-offset match aligns with char
    // boundaries in `haystack`.
    for (idx, _) in haystack.match_indices(needle) {
        // Char immediately before the match.
        let before_is_alnum = haystack[..idx]
            .chars()
            .next_back()
            .is_some_and(char::is_alphanumeric);
        // Char immediately after the match.
        let after_is_alnum = haystack[idx + needle.len()..]
            .chars()
            .next()
            .is_some_and(char::is_alphanumeric);
        if !before_is_alnum && !after_is_alnum {
            return true;
        }
    }
    false
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_known_aliases() {
        assert_eq!(codec("MLP"), "TrueHD");
        assert_eq!(codec("AC3"), "Dolby Digital");
        assert_eq!(codec("AC"), "Dolby Digital");
        assert_eq!(codec("DDL"), "Dolby Digital Plus");
        assert_eq!(codec("atmos"), "Dolby Atmos");
        assert_eq!(codec("WAV"), "PCM");
        assert_eq!(codec("DTS"), "DTS");
    }

    #[test]
    fn codec_case_insensitive() {
        // On-disc casing varies; all forms must canonicalize.
        assert_eq!(codec("ATMOS"), "Dolby Atmos");
        assert_eq!(codec("Atmos"), "Dolby Atmos");
        assert_eq!(codec("atmos"), "Dolby Atmos");
        assert_eq!(codec("mlp"), "TrueHD");
        assert_eq!(codec("ac3"), "Dolby Digital");
    }

    #[test]
    fn codec_unknown_passes_through() {
        assert_eq!(codec("FX9"), "FX9");
        assert_eq!(codec(""), "");
        // Unknown tokens keep their original casing.
        assert_eq!(codec("Vendor_X"), "Vendor_X");
    }

    fn li(code: &'static str, variant: &'static str) -> LangInfo {
        LangInfo { code, variant }
    }

    #[test]
    fn lang_bare_names_have_empty_variant() {
        assert_eq!(lang("English"), Some(li("eng", "")));
        assert_eq!(lang("english"), Some(li("eng", "")));
        assert_eq!(lang("Spanish 5.1 Dolby Digital"), Some(li("spa", "")));
        assert_eq!(lang("japanese"), Some(li("jpn", "")));
        assert_eq!(lang("Italian"), Some(li("ita", "")));
    }

    #[test]
    fn lang_compounds_carry_variant() {
        assert_eq!(
            lang("Brazilian Portuguese 5.1"),
            Some(li("por", "Brazilian"))
        );
        assert_eq!(lang("Castilian Spanish"), Some(li("spa", "Castilian")));
        assert_eq!(
            lang("Canadian French Dolby Digital"),
            Some(li("fra", "Canadian"))
        );
        assert_eq!(
            lang("Latin American Spanish"),
            Some(li("spa", "Latin American"))
        );
        assert_eq!(lang("Simplified Chinese"), Some(li("zho", "Simplified")));
        assert_eq!(lang("British English"), Some(li("eng", "British")));
    }

    #[test]
    fn lang_compounds_win_over_bare() {
        // Brazilian Portuguese must map to (por, Brazilian) via the
        // compound rule, not be intercepted by bare "portuguese"
        // (which would yield (por, "") and lose the variant).
        assert_eq!(lang("Brazilian Portuguese").unwrap().variant, "Brazilian");
        assert_eq!(lang("Canadian French").unwrap().variant, "Canadian");
    }

    #[test]
    fn lang_unknown_returns_none() {
        assert_eq!(lang("Klingon Dolby Atmos"), None);
        assert_eq!(lang(""), None);
        assert_eq!(lang("eng"), None); // 3-letter codes are not English names
    }

    #[test]
    fn lang_word_boundary_avoids_substring_false_positive() {
        // No false positive — "engineering" must NOT match "english".
        assert_eq!(lang("Audio Engineering Demo"), None);
    }

    #[test]
    fn purpose_recognizes_commentary() {
        assert_eq!(purpose("English Commentary"), LabelPurpose::Commentary);
        assert_eq!(purpose("Director's Commentary"), LabelPurpose::Commentary);
    }

    #[test]
    fn purpose_recognizes_descriptive() {
        assert_eq!(
            purpose("English Descriptive Audio"),
            LabelPurpose::Descriptive
        );
        assert_eq!(purpose("Audio Description"), LabelPurpose::Descriptive);
        assert_eq!(purpose("Described Video"), LabelPurpose::Descriptive);
    }

    #[test]
    fn purpose_descriptive_service_routes_to_descriptive() {
        // "Descriptive Service" is qualifier territory but the purpose
        // implication is Descriptive — vocab::purpose treats it as such.
        assert_eq!(
            purpose("English Descriptive Service"),
            LabelPurpose::Descriptive
        );
    }

    #[test]
    fn purpose_word_boundary_avoids_commenter_false_positive() {
        // "Commenter Pro audio" does NOT match "commentary" — the
        // existing dbp/ctrm hand-rolls would have. Vocab is stricter.
        assert_eq!(purpose("Commenter Pro audio track"), LabelPurpose::Normal);
    }

    #[test]
    fn purpose_recognizes_score() {
        assert_eq!(purpose("Music Only"), LabelPurpose::Score);
        assert_eq!(purpose("Isolated Score"), LabelPurpose::Score);
    }

    #[test]
    fn purpose_unknown_is_normal() {
        assert_eq!(purpose("English Dolby Atmos"), LabelPurpose::Normal);
        assert_eq!(purpose(""), LabelPurpose::Normal);
    }

    #[test]
    fn purpose_recognizes_ime() {
        assert_eq!(purpose("IME"), LabelPurpose::Ime);
        assert_eq!(purpose("English ime"), LabelPurpose::Ime);
        // Word-boundary: "ime" inside "time" must not match.
        assert_eq!(purpose("Showtime audio"), LabelPurpose::Normal);
    }

    #[test]
    fn has_word_treats_non_ascii_letter_as_a_letter_boundary() {
        // A non-ASCII (multi-byte) letter glued to the needle is NOT a
        // word boundary, so the needle must not match there.
        assert!(!has_word("cafésdh", "sdh")); // 'é' precedes "sdh"
        assert!(!has_word("日本sdh", "sdh"));
        // But a real boundary (space / punctuation / non-letter) matches.
        assert!(has_word("café sdh", "sdh"));
        assert!(has_word("日本 sdh", "sdh"));
        assert!(has_word("sdh", "sdh"));
    }

    #[test]
    fn qualifier_recognizes_sdh() {
        assert_eq!(qualifier("English SDH"), LabelQualifier::Sdh);
        assert_eq!(qualifier("English Captions"), LabelQualifier::Sdh);
    }

    #[test]
    fn qualifier_recognizes_forced() {
        assert_eq!(qualifier("English Forced"), LabelQualifier::Forced);
        assert_eq!(qualifier("Forced Narrative"), LabelQualifier::Forced);
    }

    #[test]
    fn qualifier_recognizes_descriptive_service() {
        assert_eq!(
            qualifier("English RNIB"),
            LabelQualifier::DescriptiveService
        );
        assert_eq!(
            qualifier("English Descriptive Service"),
            LabelQualifier::DescriptiveService
        );
    }

    #[test]
    fn qualifier_sdh_wins_over_forced_when_both_present() {
        // SDH track is its own stream regardless of forced flag.
        assert_eq!(qualifier("English Forced SDH"), LabelQualifier::Sdh);
    }

    #[test]
    fn qualifier_unknown_is_none() {
        assert_eq!(qualifier("English"), LabelQualifier::None);
        assert_eq!(qualifier(""), LabelQualifier::None);
    }

    #[test]
    fn has_word_basic() {
        assert!(has_word("english forced", "english"));
        assert!(has_word("english forced", "forced"));
        assert!(has_word("english", "english"));
    }

    #[test]
    fn has_word_rejects_substring() {
        assert!(!has_word("engineering", "english"));
        assert!(!has_word("englishman", "english"));
        assert!(!has_word("aenglish", "english"));
    }

    #[test]
    fn has_word_punctuation_boundary() {
        // "(SDH)" is a valid boundary — parentheses count as non-alphanum.
        assert!(has_word("english (sdh)", "sdh"));
        assert!(has_word("commentary,extra,info", "commentary"));
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: `MLP` is the Pixelogic token for Dolby TrueHD.
    /// AUDIO_CODECS in pixelogic lists it; vocab maps it to "TrueHD".
    /// Mutation: remove "MLP" from the codec match → "MLP" passes through.
    #[test]
    fn codec_mlp_maps_to_truehd() {
        assert_eq!(codec("MLP"), "TrueHD");
        assert_eq!(codec("mlp"), "TrueHD");
        assert_eq!(codec("Mlp"), "TrueHD");
    }

    /// Spec: `AC` (without the `3` suffix) is also a recognized alias for
    /// Dolby Digital in Pixelogic tokens.
    /// Mutation: remove `"AC"` from the match arm → "AC" passes through.
    #[test]
    fn codec_ac_without_3_maps_to_dolby_digital() {
        assert_eq!(codec("AC"), "Dolby Digital");
        assert_eq!(codec("ac"), "Dolby Digital");
    }

    /// Spec: `DDL` is Dolby's internal token for Dolby Digital Plus (EAC-3).
    /// Mutation: remove `"DDL"` arm → "DDL" passes through.
    #[test]
    fn codec_ddl_maps_to_dolby_digital_plus() {
        assert_eq!(codec("DDL"), "Dolby Digital Plus");
        assert_eq!(codec("ddl"), "Dolby Digital Plus");
    }

    /// Spec: `WAV` (PCM WAV) maps to "PCM" display string.
    /// Mutation: remove `"WAV"` arm → "WAV" passes through.
    #[test]
    fn codec_wav_maps_to_pcm() {
        assert_eq!(codec("WAV"), "PCM");
        assert_eq!(codec("wav"), "PCM");
    }

    /// Spec: `ATMOS` maps to "Dolby Atmos" (the brand string).
    /// Mutation: remove `"ATMOS"` arm → "ATMOS" passes through unchanged.
    #[test]
    fn codec_atmos_maps_to_dolby_atmos() {
        assert_eq!(codec("ATMOS"), "Dolby Atmos");
        assert_eq!(codec("Atmos"), "Dolby Atmos");
        assert_eq!(codec("atmos"), "Dolby Atmos");
    }

    /// Spec: `DTS` is recognized but passes through unchanged (no alias needed).
    /// Unknown codes return IN THEIR ORIGINAL CASING (the match branch is `_ => code`).
    /// Mutation: add `"DTS" => "DTS-HD"` → DTS incorrectly upgraded.
    #[test]
    fn codec_dts_passes_through_unchanged() {
        assert_eq!(codec("DTS"), "DTS");
        // Lowercase input returns lowercase — unknown codes pass through raw.
        assert_eq!(codec("dts"), "dts");
    }

    /// Spec: all 36 bare-lang entries must resolve correctly.
    /// Mutation: swap two entries in BARE_LANGS → wrong code returned.
    #[test]
    fn lang_bare_all_entries_spot_check() {
        let cases = [
            ("English", "eng"),
            ("Spanish", "spa"),
            ("French", "fra"),
            ("German", "deu"),
            ("Italian", "ita"),
            ("Japanese", "jpn"),
            ("Chinese", "zho"),
            ("Korean", "kor"),
            ("Portuguese", "por"),
            ("Polish", "pol"),
            ("Czech", "ces"),
            ("Hungarian", "hun"),
            ("Dutch", "nld"),
            ("Arabic", "ara"),
            ("Russian", "rus"),
            ("Swedish", "swe"),
            ("Finnish", "fin"),
        ];
        for (name, code) in cases {
            let r = lang(name).unwrap_or_else(|| panic!("lang({:?}) must be Some", name));
            assert_eq!(r.code, code, "wrong code for {}", name);
            assert_eq!(r.variant, "", "bare lang {} must have empty variant", name);
        }
    }

    /// Word boundary: "sdh" inside "lambdash" must not match.
    /// Mutation: use `contains("sdh")` → "lambdash" falsely triggers SDH.
    #[test]
    fn qualifier_no_substring_sdh() {
        assert_eq!(qualifier("lambdash"), LabelQualifier::None);
        assert_eq!(qualifier("Swedish"), LabelQualifier::None); // "swe" not "sdh"
    }

    /// ISO 639-2 codes as input (e.g. "eng") must NOT match via `lang()` because
    /// the function maps English *names*, not ISO codes.
    /// Mutation: add an ISO-code lookup table → "eng" returned for iso input.
    #[test]
    fn lang_iso_code_input_returns_none() {
        assert_eq!(lang("eng"), None);
        assert_eq!(lang("fra"), None);
        assert_eq!(lang("jpn"), None);
        assert_eq!(lang("zho"), None);
    }

    /// Compound lang "Australian English" → (eng, Australian).
    /// Mutation: put "australian english" after "english" → bare "English" wins.
    #[test]
    fn compound_lang_australian_english() {
        let r = lang("Australian English").unwrap();
        assert_eq!(r.code, "eng");
        assert_eq!(r.variant, "Australian");
    }

    /// Compound lang corpus typo "Austrailian English" (missing 'l') must still match.
    /// Mutation: remove the typo entry → no variant info.
    #[test]
    fn compound_lang_austrailian_typo_matched() {
        let r = lang("Austrailian English").unwrap();
        assert_eq!(r.code, "eng");
        assert_eq!(r.variant, "Australian");
    }

    /// Euro Portuguese vs European Portuguese: both map to (por, European).
    /// Mutation: remove "euro portuguese" → "Euro Portuguese" returns (por, "").
    #[test]
    fn compound_lang_euro_portuguese() {
        let r = lang("Euro Portuguese").unwrap();
        assert_eq!(r.code, "por");
        assert_eq!(r.variant, "European");

        let r = lang("European Portuguese").unwrap();
        assert_eq!(r.code, "por");
        assert_eq!(r.variant, "European");
    }

    /// `has_word` empty needle returns false (guard against infinite loop).
    /// Mutation: remove empty-needle early return → always returns true for empty needle.
    #[test]
    fn has_word_empty_needle_is_false() {
        assert!(!has_word("anything", ""));
        assert!(!has_word("", ""));
    }

    /// `codec()` with empty string passes through as empty (no panic).
    /// Mutation: remove guard → match panics on empty.
    #[test]
    fn codec_empty_passes_through() {
        assert_eq!(codec(""), "");
    }
}

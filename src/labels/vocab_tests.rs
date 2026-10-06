use super::*;

// ISO 639-1 -> 639-2/T, checked against an independently written list so a mistyped or
// swapped row cannot pass on the table's own shape.
#[test]
fn iso639_1_values_match_the_standard() {
    let want = [
        "aa:aar", "ab:abk", "ae:ave", "af:afr", "ak:aka", "am:amh", "an:arg", "ar:ara", "as:asm",
        "av:ava", "ay:aym", "az:aze", "ba:bak", "be:bel", "bg:bul", "bh:bih", "bi:bis", "bm:bam",
        "bn:ben", "bo:bod", "br:bre", "bs:bos", "ca:cat", "ce:che", "ch:cha", "co:cos", "cr:cre",
        "cs:ces", "cu:chu", "cv:chv", "cy:cym", "da:dan", "de:deu", "dv:div", "dz:dzo", "ee:ewe",
        "el:ell", "en:eng", "eo:epo", "es:spa", "et:est", "eu:eus", "fa:fas", "ff:ful", "fi:fin",
        "fj:fij", "fo:fao", "fr:fra", "fy:fry", "ga:gle", "gd:gla", "gl:glg", "gn:grn", "gu:guj",
        "gv:glv", "ha:hau", "he:heb", "hi:hin", "ho:hmo", "hr:hrv", "ht:hat", "hu:hun", "hy:hye",
        "hz:her", "ia:ina", "id:ind", "ie:ile", "ig:ibo", "ii:iii", "ik:ipk", "io:ido", "is:isl",
        "it:ita", "iu:iku", "ja:jpn", "jv:jav", "ka:kat", "kg:kon", "ki:kik", "kj:kua", "kk:kaz",
        "kl:kal", "km:khm", "kn:kan", "ko:kor", "kr:kau", "ks:kas", "ku:kur", "kv:kom", "kw:cor",
        "ky:kir", "la:lat", "lb:ltz", "lg:lug", "li:lim", "ln:lin", "lo:lao", "lt:lit", "lu:lub",
        "lv:lav", "mg:mlg", "mh:mah", "mi:mri", "mk:mkd", "ml:mal", "mn:mon", "mr:mar", "ms:msa",
        "mt:mlt", "my:mya", "na:nau", "nb:nob", "nd:nde", "ne:nep", "ng:ndo", "nl:nld", "nn:nno",
        "no:nor", "nr:nbl", "nv:nav", "ny:nya", "oc:oci", "oj:oji", "om:orm", "or:ori", "os:oss",
        "pa:pan", "pi:pli", "pl:pol", "ps:pus", "pt:por", "qu:que", "rm:roh", "rn:run", "ro:ron",
        "ru:rus", "rw:kin", "sa:san", "sc:srd", "sd:snd", "se:sme", "sg:sag", "si:sin", "sk:slk",
        "sl:slv", "sm:smo", "sn:sna", "so:som", "sq:sqi", "sr:srp", "ss:ssw", "st:sot", "su:sun",
        "sv:swe", "sw:swa", "ta:tam", "te:tel", "tg:tgk", "th:tha", "ti:tir", "tk:tuk", "tl:tgl",
        "tn:tsn", "to:ton", "tr:tur", "ts:tso", "tt:tat", "tw:twi", "ty:tah", "ug:uig", "uk:ukr",
        "ur:urd", "uz:uzb", "ve:ven", "vi:vie", "vo:vol", "wa:wln", "wo:wol", "xh:xho", "yi:yid",
        "yo:yor", "za:zha", "zh:zho", "zu:zul",
    ];
    assert_eq!(want.len(), 184);
    for pair in want {
        let (two, three) = pair.split_once(':').unwrap();
        assert_eq!(iso639_1_to_iso639_2(two), Some(three), "{two}");
    }
}

#[test]
fn iso639_1_withdrawn_dvd_codes_resolve_to_their_replacements() {
    for (old, three) in [
        ("iw", "heb"),
        ("in", "ind"),
        ("ji", "yid"),
        ("jw", "jav"),
        ("mo", "ron"),
    ] {
        assert_eq!(iso639_1_to_iso639_2(old), Some(three), "{old}");
    }
}

#[test]
fn mandarin_chinese_compound_keeps_its_variant() {
    assert_eq!(
        lang("Mandarin Chinese"),
        Some(LangInfo {
            code: "zho",
            variant: "Mandarin"
        })
    );
}

#[test]
fn description_word_marks_descriptive_purpose() {
    assert_eq!(purpose("English Description"), LabelPurpose::Descriptive);
    assert_eq!(purpose("Descriptions"), LabelPurpose::Normal);
}

#[test]
fn iso639_1_accepts_jw_and_mo_dvd_aliases() {
    assert_eq!(iso639_1_to_iso639_2("jw"), Some("jav"));
    assert_eq!(iso639_1_to_iso639_2("mo"), Some("ron"));
}

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

/// Spec: every BARE_LANGS entry must resolve to its code.
/// Mutation: swap two entries' codes in BARE_LANGS -> wrong code returned.
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
        ("Mandarin", "zho"),
        ("Cantonese", "zho"),
        ("Portuguese", "por"),
        ("Polish", "pol"),
        ("Czech", "ces"),
        ("Hungarian", "hun"),
        ("Dutch", "nld"),
        ("Korean", "kor"),
        ("Arabic", "ara"),
        ("Hindi", "hin"),
        ("Turkish", "tur"),
        ("Thai", "tha"),
        ("Swedish", "swe"),
        ("Norwegian", "nor"),
        ("Danish", "dan"),
        ("Finnish", "fin"),
        ("Hebrew", "heb"),
        ("Russian", "rus"),
        ("Greek", "ell"),
        ("Vietnamese", "vie"),
        ("Indonesian", "ind"),
        ("Malay", "msa"),
        ("Ukrainian", "ukr"),
        ("Romanian", "ron"),
        ("Bulgarian", "bul"),
        ("Croatian", "hrv"),
        ("Serbian", "srp"),
        ("Slovak", "slk"),
        ("Slovenian", "slv"),
        ("Estonian", "est"),
        ("Latvian", "lav"),
        ("Lithuanian", "lit"),
        ("Icelandic", "isl"),
        ("Basque", "eus"),
        ("Catalan", "cat"),
        ("Galician", "glg"),
    ];
    assert_eq!(
        cases.len(),
        BARE_LANGS.len(),
        "table and cases must stay in step"
    );
    for (name, code) in cases {
        let r = lang(name).unwrap_or_else(|| panic!("lang({:?}) must be Some", name));
        assert_eq!(r.code, code, "wrong code for {}", name);
        assert_eq!(r.variant, "", "bare lang {} must have empty variant", name);
    }
}

/// Needles embedded in longer words must not match in qualifier/purpose/lang.
/// Mutation: has_word -> contains() in any of the three -> a false hit.
#[test]
fn embedded_needles_do_not_match_word_classifiers() {
    assert_eq!(qualifier("xsdhx"), LabelQualifier::None);
    assert_eq!(qualifier("uncaptions"), LabelQualifier::None);
    assert_eq!(purpose("uncommentary"), LabelPurpose::Normal);
    assert_eq!(purpose("scorecard"), LabelPurpose::Normal);
    assert_eq!(lang("englishman"), None);
    assert_eq!(lang("thaiwan"), None);
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

// Guards the `||` (not `&&`) in purpose()'s compound-phrase fast path: catches a substring
// match with no word boundary that has_word() would miss.
#[test]
fn purpose_descriptive_service_substring_without_word_boundary() {
    assert_eq!(
        purpose("nondescriptive service track"),
        LabelPurpose::Descriptive
    );
}

// Exhaustive per-arm check: every menu_lang() table token maps to its
// canonical /T code; deleting any arm makes that arm's tokens return None.
#[test]
fn menu_lang_covers_every_table_entry() {
    let cases: &[(&str, &str)] = &[
        ("eng", "eng"),
        ("en", "eng"),
        ("ger", "deu"),
        ("deu", "deu"),
        ("de", "deu"),
        ("fre", "fra"),
        ("fra", "fra"),
        ("fr", "fra"),
        ("spa", "spa"),
        ("es", "spa"),
        ("ita", "ita"),
        ("it", "ita"),
        ("por", "por"),
        ("pt", "por"),
        ("jpn", "jpn"),
        ("jap", "jpn"),
        ("ja", "jpn"),
        ("kor", "kor"),
        ("ko", "kor"),
        ("chi", "zho"),
        ("zho", "zho"),
        ("zh", "zho"),
        ("rus", "rus"),
        ("ru", "rus"),
        ("dut", "nld"),
        ("nld", "nld"),
        ("nl", "nld"),
        ("pol", "pol"),
        ("pl", "pol"),
        ("cze", "ces"),
        ("ces", "ces"),
        ("cs", "ces"),
        ("dan", "dan"),
        ("da", "dan"),
        ("fin", "fin"),
        ("fi", "fin"),
        ("nor", "nor"),
        ("no", "nor"),
        ("swe", "swe"),
        ("sv", "swe"),
        ("hun", "hun"),
        ("hu", "hun"),
        ("gre", "ell"),
        ("ell", "ell"),
        ("el", "ell"),
        ("tur", "tur"),
        ("tr", "tur"),
        ("ara", "ara"),
        ("ar", "ara"),
        ("hin", "hin"),
        ("hi", "hin"),
        ("tha", "tha"),
        ("th", "tha"),
        ("ukr", "ukr"),
        ("uk", "ukr"),
        ("cat", "cat"),
        ("ca", "cat"),
    ];
    for (token, expected) in cases {
        assert_eq!(
            menu_lang(token),
            Some(*expected),
            "menu_lang({:?}) should map to {:?}",
            token,
            expected
        );
    }
    // Case-insensitive and trimmed.
    assert_eq!(menu_lang("ENG"), Some("eng"));
    assert_eq!(menu_lang("  Eng  "), Some("eng"));
    // Unrecognized token -> None, never a guess.
    assert_eq!(menu_lang("xyz"), None);
    assert_eq!(menu_lang(""), None);
}

// `ISO_639_1_TO_2` structural invariants: complete 184-code set, distinct keys, well-formed
// values.
#[test]
fn iso639_1_table_is_complete_and_well_formed() {
    assert_eq!(
        ISO_639_1_TO_2.len(),
        184,
        "ISO 639-1 defines 184 two-letter codes; the table must hold all \
             of them"
    );
    let mut keys: Vec<&str> = ISO_639_1_TO_2.iter().map(|(two, _)| *two).collect();
    keys.sort_unstable();
    let unique = keys.len();
    keys.dedup();
    assert_eq!(unique, keys.len(), "no ISO 639-1 code may appear twice");
    for (two, three) in ISO_639_1_TO_2 {
        assert!(
            two.len() == 2 && two.bytes().all(|b| b.is_ascii_lowercase()),
            "{two:?} is not a two-letter lowercase ISO 639-1 code"
        );
        assert!(
            three.len() == 3 && three.bytes().all(|b| b.is_ascii_lowercase()),
            "{three:?} is not a three-letter lowercase ISO 639-2 code"
        );
    }
    // The withdrawn DVD-era spellings resolve, and are not themselves
    // rows in the main table (they are aliases, not codes).
    for (old, new) in ISO_639_1_DEPRECATED {
        assert!(
            !ISO_639_1_TO_2.iter().any(|(two, _)| two == old),
            "withdrawn code {old:?} must not be a table row"
        );
        assert_eq!(
            iso639_1_to_iso639_2(old),
            iso639_1_to_iso639_2(new),
            "withdrawn code {old:?} must resolve exactly as {new:?}"
        );
    }
}

// The two tables must not disagree: every token menu_lang() accepts must
// yield the same code as iso639_1_to_iso639_2(), so DVD and Blu-ray never
// produce different `Language` elements for the same tongue.
#[test]
fn iso639_1_agrees_with_menu_lang() {
    for (two, three) in ISO_639_1_TO_2 {
        if let Some(via_menu) = menu_lang(two) {
            assert_eq!(
                via_menu, *three,
                "menu_lang({two:?}) = {via_menu:?} disagrees with the ISO \
                     639-1 table's {three:?}"
            );
        }
    }
    // Spot-check the /T choice itself, on the languages where /B differs.
    for (two, t_code) in [
        ("de", "deu"),
        ("fr", "fra"),
        ("zh", "zho"),
        ("cs", "ces"),
        ("nl", "nld"),
        ("el", "ell"),
        ("ro", "ron"),
        ("sk", "slk"),
        ("is", "isl"),
        ("hy", "hye"),
        ("ka", "kat"),
        ("fa", "fas"),
    ] {
        assert_eq!(
            iso639_1_to_iso639_2(two),
            Some(t_code),
            "the crate standardises on ISO 639-2/T, so {two:?} is \
                 {t_code:?} and never the bibliographic form"
        );
    }
}

/// Trimming, case-insensitivity, and the no-guess contract.
#[test]
fn iso639_1_normalizes_input_and_never_guesses() {
    assert_eq!(iso639_1_to_iso639_2("RO"), Some("ron"));
    assert_eq!(iso639_1_to_iso639_2("  Ro  "), Some("ron"));
    assert_eq!(iso639_1_to_iso639_2("IW"), Some("heb"));
    assert_eq!(iso639_1_to_iso639_2("zz"), None);
    assert_eq!(iso639_1_to_iso639_2(""), None);
    assert_eq!(iso639_1_to_iso639_2("e"), None);
    // A three-letter code is not ISO 639-1 input — that is menu_lang's job.
    assert_eq!(iso639_1_to_iso639_2("eng"), None);
}

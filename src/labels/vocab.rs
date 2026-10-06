//! Shared label vocabulary — canonical mappings used by ≥1 label parser.
//!
//! Labels come from BD-J authoring tool files, NOT BD spec fields. Central,
//! regression-tested source of truth for: codec brand aliases, English /
//! multi-word language name → ISO 639-2, and English text →
//! [`LabelPurpose`] / [`LabelQualifier`].
//!
//! Only maps values we are 100% certain about; unknown input passes through raw or returns
//! `None` — never guesses. Not for BD spec STN codec IDs; those decode in `mpls.rs`.

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
/// Handles bare English names ("English") and multi-word vendor variants ("Brazilian
/// Portuguese"); compounds are checked before bare names, so the latter returns `variant:
/// "Brazilian"` rather than a bare "Portuguese" match. Bare matches return `variant: ""`;
/// `None` means unrecognized input (never a guess).
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

/// Map a short menu-graphic language token (as embedded in authoring
/// filenames like `Feature_UHD01_Eng_Composite1.png`) to an ISO-639-2/T code.
///
/// These filename tokens are compact 2/3-letter abbreviations, NOT the full
/// language names [`lang`] handles, so they get their own certain table.
/// Accepts the ISO-639-2/B spellings some tools emit (`ger`, `fre`, `chi`)
/// and normalizes to /T (`deu`, `fra`, `zho`). Case-insensitive. Returns
/// `None` for anything not in the table — never guesses.
pub fn menu_lang(token: &str) -> Option<&'static str> {
    let t = token.trim().to_ascii_lowercase();
    let code = match t.as_str() {
        "eng" | "en" => "eng",
        "ger" | "deu" | "de" => "deu",
        "fre" | "fra" | "fr" => "fra",
        "spa" | "es" => "spa",
        "ita" | "it" => "ita",
        "por" | "pt" => "por",
        "jpn" | "jap" | "ja" => "jpn",
        "kor" | "ko" => "kor",
        "chi" | "zho" | "zh" => "zho",
        "rus" | "ru" => "rus",
        "dut" | "nld" | "nl" => "nld",
        "pol" | "pl" => "pol",
        "cze" | "ces" | "cs" => "ces",
        "dan" | "da" => "dan",
        "fin" | "fi" => "fin",
        "nor" | "no" => "nor",
        "swe" | "sv" => "swe",
        "hun" | "hu" => "hun",
        "gre" | "ell" | "el" => "ell",
        "tur" | "tr" => "tur",
        "ara" | "ar" => "ara",
        "hin" | "hi" => "hin",
        "tha" | "th" => "tha",
        "ukr" | "uk" => "ukr",
        "cat" | "ca" => "cat",
        _ => return None,
    };
    Some(code)
}

// ── ISO 639-1 → ISO 639-2 ────────────────────────────────────────────────────

// Complete ISO 639-1 set, paired with its ISO 639-2/T code (each code once).
const ISO_639_1_TO_2: &[(&str, &str)] = &[
    ("aa", "aar"),
    ("ab", "abk"),
    ("ae", "ave"),
    ("af", "afr"),
    ("ak", "aka"),
    ("am", "amh"),
    ("an", "arg"),
    ("ar", "ara"),
    ("as", "asm"),
    ("av", "ava"),
    ("ay", "aym"),
    ("az", "aze"),
    ("ba", "bak"),
    ("be", "bel"),
    ("bg", "bul"),
    ("bh", "bih"),
    ("bi", "bis"),
    ("bm", "bam"),
    ("bn", "ben"),
    ("bo", "bod"),
    ("br", "bre"),
    ("bs", "bos"),
    ("ca", "cat"),
    ("ce", "che"),
    ("ch", "cha"),
    ("co", "cos"),
    ("cr", "cre"),
    ("cs", "ces"),
    ("cu", "chu"),
    ("cv", "chv"),
    ("cy", "cym"),
    ("da", "dan"),
    ("de", "deu"),
    ("dv", "div"),
    ("dz", "dzo"),
    ("ee", "ewe"),
    ("el", "ell"),
    ("en", "eng"),
    ("eo", "epo"),
    ("es", "spa"),
    ("et", "est"),
    ("eu", "eus"),
    ("fa", "fas"),
    ("ff", "ful"),
    ("fi", "fin"),
    ("fj", "fij"),
    ("fo", "fao"),
    ("fr", "fra"),
    ("fy", "fry"),
    ("ga", "gle"),
    ("gd", "gla"),
    ("gl", "glg"),
    ("gn", "grn"),
    ("gu", "guj"),
    ("gv", "glv"),
    ("ha", "hau"),
    ("he", "heb"),
    ("hi", "hin"),
    ("ho", "hmo"),
    ("hr", "hrv"),
    ("ht", "hat"),
    ("hu", "hun"),
    ("hy", "hye"),
    ("hz", "her"),
    ("ia", "ina"),
    ("id", "ind"),
    ("ie", "ile"),
    ("ig", "ibo"),
    ("ii", "iii"),
    ("ik", "ipk"),
    ("io", "ido"),
    ("is", "isl"),
    ("it", "ita"),
    ("iu", "iku"),
    ("ja", "jpn"),
    ("jv", "jav"),
    ("ka", "kat"),
    ("kg", "kon"),
    ("ki", "kik"),
    ("kj", "kua"),
    ("kk", "kaz"),
    ("kl", "kal"),
    ("km", "khm"),
    ("kn", "kan"),
    ("ko", "kor"),
    ("kr", "kau"),
    ("ks", "kas"),
    ("ku", "kur"),
    ("kv", "kom"),
    ("kw", "cor"),
    ("ky", "kir"),
    ("la", "lat"),
    ("lb", "ltz"),
    ("lg", "lug"),
    ("li", "lim"),
    ("ln", "lin"),
    ("lo", "lao"),
    ("lt", "lit"),
    ("lu", "lub"),
    ("lv", "lav"),
    ("mg", "mlg"),
    ("mh", "mah"),
    ("mi", "mri"),
    ("mk", "mkd"),
    ("ml", "mal"),
    ("mn", "mon"),
    ("mr", "mar"),
    ("ms", "msa"),
    ("mt", "mlt"),
    ("my", "mya"),
    ("na", "nau"),
    ("nb", "nob"),
    ("nd", "nde"),
    ("ne", "nep"),
    ("ng", "ndo"),
    ("nl", "nld"),
    ("nn", "nno"),
    ("no", "nor"),
    ("nr", "nbl"),
    ("nv", "nav"),
    ("ny", "nya"),
    ("oc", "oci"),
    ("oj", "oji"),
    ("om", "orm"),
    ("or", "ori"),
    ("os", "oss"),
    ("pa", "pan"),
    ("pi", "pli"),
    ("pl", "pol"),
    ("ps", "pus"),
    ("pt", "por"),
    ("qu", "que"),
    ("rm", "roh"),
    ("rn", "run"),
    ("ro", "ron"),
    ("ru", "rus"),
    ("rw", "kin"),
    ("sa", "san"),
    ("sc", "srd"),
    ("sd", "snd"),
    ("se", "sme"),
    ("sg", "sag"),
    ("si", "sin"),
    ("sk", "slk"),
    ("sl", "slv"),
    ("sm", "smo"),
    ("sn", "sna"),
    ("so", "som"),
    ("sq", "sqi"),
    ("sr", "srp"),
    ("ss", "ssw"),
    ("st", "sot"),
    ("su", "sun"),
    ("sv", "swe"),
    ("sw", "swa"),
    ("ta", "tam"),
    ("te", "tel"),
    ("tg", "tgk"),
    ("th", "tha"),
    ("ti", "tir"),
    ("tk", "tuk"),
    ("tl", "tgl"),
    ("tn", "tsn"),
    ("to", "ton"),
    ("tr", "tur"),
    ("ts", "tso"),
    ("tt", "tat"),
    ("tw", "twi"),
    ("ty", "tah"),
    ("ug", "uig"),
    ("uk", "ukr"),
    ("ur", "urd"),
    ("uz", "uzb"),
    ("ve", "ven"),
    ("vi", "vie"),
    ("vo", "vol"),
    ("wa", "wln"),
    ("wo", "wol"),
    ("xh", "xho"),
    ("yi", "yid"),
    ("yo", "yor"),
    ("za", "zha"),
    ("zh", "zho"),
    ("zu", "zul"),
];

// Withdrawn ISO 639-1 codes DVD-Video still carries (frozen at the 1988 edition).
const ISO_639_1_DEPRECATED: &[(&str, &str)] = &[
    ("iw", "he"),
    ("in", "id"),
    ("ji", "yi"),
    ("jw", "jv"),
    ("mo", "ro"),
];

/// Map an ISO 639-1 two-letter language code to its ISO 639-2/T three-letter
/// code, accepting the withdrawn DVD-era spellings (`iw`, `in`, `ji`, `jw`, `mo`) as
/// aliases for their replacements.
///
/// Covers the WHOLE of ISO 639-1, unlike [`menu_lang`], whose table only spans the languages
/// seen in Blu-ray menu-graphic filenames. Case-insensitive and trimmed. Returns `None` for
/// anything that is not an ISO 639-1 code — callers decide the fallback.
pub fn iso639_1_to_iso639_2(code: &str) -> Option<&'static str> {
    let c = code.trim().to_ascii_lowercase();
    let c = ISO_639_1_DEPRECATED
        .iter()
        .find(|(old, _)| *old == c)
        .map_or(c.as_str(), |(_, new)| new);
    ISO_639_1_TO_2
        .iter()
        .find(|(two, _)| *two == c)
        .map(|(_, three)| *three)
}

// ── Purpose ──────────────────────────────────────────────────────────────────

/// Classify a free-form English label string into a [`LabelPurpose`].
/// Case-insensitive; single words word-boundary matched, phrases substring
/// matched:
/// - "commentary", "director's commentary" → `Commentary`
/// - "descriptive", "description", "audio description", "described" → `Descriptive`
/// - "score", "music only" → `Score`
/// - "ime" (alternate music for closing themes etc.) → `Ime`
/// - anything else → `Normal`
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
/// SDH wins over Forced when both are present.
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

// True if `needle` appears in `haystack` at non-alphanumeric boundaries (`haystack` assumed
// lowercase).
fn has_word(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    // Char-aware, not byte-level: an accented/CJK char adjacent to the match is
    // alphanumeric and so NOT a boundary, preventing false positives like "sdh"
    // inside "cafésch". Needles are ASCII, so byte offsets align with char bounds.
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
#[path = "vocab_tests.rs"]
mod tests;

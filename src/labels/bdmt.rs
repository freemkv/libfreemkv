//! BDMV disc-library metadata (`/BDMV/META/DL/bdmt_<lang>.xml`).
//!
//! Parses the Blu-ray disc-library metadata XML (a `urn:BDA:bdmv;disclib` root
//! holding a `di:discinfo` element in `urn:BDA:bdmv;discinfo`) into title,
//! description, and disc-set position per language. Invoked from the disc-scan path in [`labels`](super)
//! ([`detect`] then [`parse`]); [`DiscMetadata`] is re-exported there.
//! Extraction is best-effort — a malformed file yields `None`, other
//! sibling-language files can still supply metadata.

use super::xml;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

// Upper bound on a single bdmt_<lang>.xml we will read. Size is attacker-controlled UDF
// metadata; real files are a few KB.
const MAX_BDMT_BYTES: u64 = 1024 * 1024;

// Retained title/description cap (bytes): bounds memory across up to 26^3 languages.
const MAX_BDMT_TEXT: usize = 1024;

// Truncate to at most `MAX_BDMT_TEXT` bytes on a char boundary.
fn cap_text(mut s: String) -> String {
    if s.len() > MAX_BDMT_TEXT {
        let mut end = MAX_BDMT_TEXT;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

/// Disc-level metadata extracted from `/BDMV/META/DL/bdmt_*.xml`.
///
/// All maps are keyed by 3-char ISO 639-2 language code (e.g.
/// `"eng"`, `"fra"`, `"jpn"`) — the same key segment used in the
/// `bdmt_<lang>.xml` filename.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DiscMetadata {
    /// Localized titles, keyed by 3-char ISO 639-2 lang code
    /// (e.g. "eng" → "Aurora Drift")
    pub titles: BTreeMap<String, String>,
    /// Text of `<di:description>` per lang. On real discs that element is a
    /// container (`di:thumbnail` / `di:tableOfContents`), not text, so this is
    /// normally empty.
    pub descriptions: BTreeMap<String, String>,
    /// Disc N of M, returned whenever both `<di:setNumber>` (or the
    /// `<di:discNumber>` fallback) and `<di:numSets>` are present and sane (including `(1, 1)` for a
    /// single-disc release); `None` if either is missing or invalid.
    pub disc_number: Option<(u32, u32)>,
}

/// True if `/BDMV/META/DL/` exists and contains at least one
/// `bdmt_*.xml` file.
pub fn detect(udf: &UdfFs) -> bool {
    let Some(dir) = udf.find_dir("/BDMV/META/DL") else {
        return false;
    };
    dir.entries
        .iter()
        .any(|e| !e.is_dir && is_bdmt_filename(&e.name))
}

/// Read every `bdmt_<lang>.xml` under `/BDMV/META/DL/` and return the
/// aggregated [`DiscMetadata`]. Returns `None` if no titles could be
/// extracted from any file.
pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<DiscMetadata> {
    let dir = udf.find_dir("/BDMV/META/DL")?;
    let mut out = DiscMetadata::default();

    for entry in &dir.entries {
        if entry.is_dir {
            continue;
        }
        let Some(lang) = lang_code_from_filename(&entry.name) else {
            continue;
        };
        // entry.size is attacker-controlled and flows into a Vec::with_capacity
        // in read_file. Cap well above a real few-KB bdmt XML so a crafted
        // multi-GB size can't trigger a huge allocation before parsing.
        if !bdmt_size_acceptable(entry.size) {
            continue;
        }
        let path = format!("/BDMV/META/DL/{}", entry.name);
        let Ok(bytes) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            continue;
        };
        let Some((title, description, disc_set)) = parse_bdmt_xml(text) else {
            continue;
        };
        out.titles.insert(lang.clone(), title);
        if let Some(desc) = description {
            out.descriptions.insert(lang.clone(), desc);
        }
        // Disc-set position is disc-global; first one we successfully
        // read wins. (All bdmt_*.xml on a given disc carry the same
        // value in practice.)
        if out.disc_number.is_none()
            && let Some(ds) = disc_set
        {
            out.disc_number = Some(ds);
        }
    }

    if out.titles.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Gate a `bdmt_<lang>.xml` file by its declared (untrusted) UDF size
/// before reading it. Anything over [`MAX_BDMT_BYTES`] is skipped to
/// avoid an oversized allocation in `read_file`.
fn bdmt_size_acceptable(size: u64) -> bool {
    size <= MAX_BDMT_BYTES
}

/// True if `name` matches the `bdmt_<lang>.xml` convention with a
/// 3-character ISO 639-2 lang code segment. Case-insensitive.
fn is_bdmt_filename(name: &str) -> bool {
    lang_code_from_filename(name).is_some()
}

/// Extract the 3-char language code from a `bdmt_<lang>.xml` filename.
/// Returns `None` if the filename doesn't match. Lang code is
/// lowercased so callers always see e.g. `"eng"` not `"ENG"`.
fn lang_code_from_filename(name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".xml")?;
    let lang = stem.strip_prefix("bdmt_")?;
    // ISO 639-2 codes are exactly 3 ASCII letters. Be strict — keeps
    // us from picking up unrelated `bdmt_foo.xml` siblings.
    if lang.len() != 3 || !lang.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    Some(lang.to_string())
}

/// Tuple returned by [`parse_bdmt_xml`]: `(title, description?, disc_set?)`.
/// Aliased so the function signature isn't a clippy::type-complexity offender.
pub(crate) type BdmtFields = (String, Option<String>, Option<(u32, u32)>);

// Parse one bdmt_<lang>.xml document into (title, description?, disc_set?). None if no title
// could be located.
pub(crate) fn parse_bdmt_xml(xml_text: &str) -> Option<BdmtFields> {
    let title = cap_text(extract_title(xml_text)?);
    let description = xml::text(xml_text, "description")
        .and_then(|s| field_text(&s))
        .map(cap_text);
    let disc_set = extract_disc_set(xml_text);
    Some((title, description, disc_set))
}

/// Reject description candidates that are themselves XML fragments (e.g. only <di:thumbnail/>
/// children, no prose) OR mixed content — prose with an embedded child element, which
/// `xml::text` returns verbatim (tags and all). The old `starts_with('<')` only caught
/// fragments that LED with a tag, so a description like `Real prose <di:thumbnail/>` leaked its
/// markup through. Any `<` that begins an element / close tag / comment / PI marks the value as
/// XML-tainted; a bare `<` used as prose (e.g. `a < b`) is left alone.
fn looks_like_xml(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.iter().enumerate().any(|(i, &b)| {
        b == b'<'
            && matches!(
                bytes.get(i + 1),
                Some(&c) if c.is_ascii_alphabetic() || matches!(c, b'/' | b'!' | b'?')
            )
    })
}

/// Turn a raw element body into display text: a whole-value CDATA section is
/// unwrapped, otherwise the predefined and numeric character entities are decoded.
/// `None` for empty values and for values carrying element markup.
fn field_text(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let out = match raw
        .strip_prefix("<![CDATA[")
        .and_then(|r| r.strip_suffix("]]>"))
    {
        Some(inner) if !inner.contains("]]>") => display_text(inner),
        Some(_) => return None,
        None if looks_like_xml(raw) => return None,
        None => display_text(&decode_entities(raw)),
    };
    if out.is_empty() || looks_like_xml(&out) {
        return None;
    }
    Some(out)
}

// Decode `&amp; &lt; &gt; &quot; &apos; &#N; &#xH;`; anything else is kept verbatim.
fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let decoded = rest.find(';').filter(|&e| e <= 10).and_then(|e| {
            let c = match &rest[1..e] {
                "amp" => '&',
                "lt" => '<',
                "gt" => '>',
                "quot" => '"',
                "apos" => '\'',
                n => {
                    let n = n.strip_prefix('#')?;
                    let cp = match n.strip_prefix(['x', 'X']) {
                        Some(h) => u32::from_str_radix(h, 16).ok()?,
                        None => n.parse().ok()?,
                    };
                    char::from_u32(cp).filter(|c| !c.is_control())?
                }
            };
            Some((c, e + 1))
        });
        match decoded {
            Some((c, len)) => {
                out.push(c);
                rest = &rest[len..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Disc-authored text as shown and put in file names: control characters (an escape
/// sequence, a raw newline) dropped, then trimmed.
pub(crate) fn display_text(s: &str) -> String {
    let kept: String = s.chars().filter(|c| !c.is_control()).collect();
    kept.trim().to_string()
}

/// True for the generic `Blu-ray` some discs (Warner's non-English bdmt files) put where
/// the title belongs: not a title, so callers skip to the next candidate.
pub(crate) fn is_placeholder_title(title: &str) -> bool {
    title.trim().eq_ignore_ascii_case("Blu-ray")
}

/// Try title-bearing element variants in priority order. The `xml`
/// helpers are case- and namespace-insensitive, so callers pass the
/// bare local name (no `di:` prefix).
fn extract_title(xml_text: &str) -> Option<String> {
    // Priority: <di:name> (Paramount-style), then <di:title>, then nested
    // tableOfContents/titleName. xml::text trims, so an empty string here
    // means a genuinely empty element.
    for tag in ["name", "title"] {
        if let Some(s) = xml::text(xml_text, tag)
            .and_then(|s| field_text(&s))
            .filter(|s| !is_placeholder_title(s))
        {
            return Some(s);
        }
    }
    // tableOfContents/titleName: search inside the toc block so we
    // don't accidentally pick a stray <titleName> from elsewhere.
    if let Some((s, e)) = xml::find_element(xml_text, "tableOfContents", 0) {
        let block = &xml_text[s..e];
        if let Some(t) = xml::text(block, "titleName")
            .and_then(|s| field_text(&s))
            .filter(|s| !is_placeholder_title(s))
        {
            return Some(t);
        }
    }
    None
}

/// Extract `(setNumber, numSets)` if both are present and parse as
/// `u32`. Accepts either `<di:numSets>` or `<di:numberOfSets>` for
/// the denominator (both forms appear in the wild).
fn extract_disc_set(xml_text: &str) -> Option<(u32, u32)> {
    // Real discs carry <di:setNumber>; <di:discNumber> is kept as a fallback.
    let num = |tag| xml::text(xml_text, tag).and_then(|t| t.trim().parse::<u32>().ok());
    let n = num("setNumber").or_else(|| num("discNumber"))?;
    let total = xml::text(xml_text, "numSets")
        .or_else(|| xml::text(xml_text, "numberOfSets"))?
        .trim()
        .parse::<u32>()
        .ok()?;
    // Reject nonsensical "Disc N of M" values: (0,0), (0,5), (5,2)...
    // These serialize to JSON and reach downstream consumers as
    // meaningless metadata.
    if n < 1 || total < 1 || n > total {
        return None;
    }
    Some((n, total))
}

#[cfg(test)]
#[path = "bdmt_tests.rs"]
mod tests;

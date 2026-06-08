//! BDMV disc-library metadata (`/BDMV/META/DL/bdmt_<lang>.xml`).
//!
//! Every commercial Blu-ray carries a disc-library metadata directory
//! with one XML file per shipped language. The schema is the Blu-ray
//! "disc library metadata" namespace (`urn:BDA:bdmv;disclibmeta`),
//! conventionally prefixed `di:`. Fields commonly present:
//!
//! - `<di:title>` or `<di:name>` — the title string. Vendor practice
//!   varies (Paramount discs tend to use `<di:name>`).
//! - `<di:description>` — optional synopsis (often absent on retail
//!   discs; common on box sets and special editions).
//! - `<di:discNumber>` / `<di:numSets>` (or `<di:numberOfSets>`) —
//!   set position for multi-disc releases.
//!
//! This module is intentionally separate from the BD-J `StreamLabel`
//! parsers under `labels/*.rs`. The XML here is disc-level (title,
//! description, set position), not per-stream. It is invoked from the
//! disc-scan path in [`labels`](super) ([`detect`] then [`parse`]),
//! and [`DiscMetadata`] is re-exported there.
//!
//! Real-world XML is irregular: missing description elements, multiple
//! title elements (first one wins), and occasional malformed content.
//! Extraction is best-effort — a malformed file is treated as "no
//! metadata" (returns `None` from the helper), and the caller can
//! still get metadata from sibling-language XML files.

use super::xml;
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::BTreeMap;

/// Upper bound on the size of a single `bdmt_<lang>.xml` we will read.
/// The size comes from attacker-controlled UDF metadata; real files are
/// a few KB, so 1 MiB is generous while preventing a crafted huge-size
/// entry from triggering an oversized allocation in `read_file`.
const MAX_BDMT_BYTES: u64 = 1024 * 1024;

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
    /// First-line / short description, per lang
    pub descriptions: BTreeMap<String, String>,
    /// Disc N of M for box sets (None if not a box set)
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
        // entry.size is attacker-controlled UDF metadata and flows into
        // a Vec::with_capacity in read_file. A real BDMV bdmt XML is a
        // few KB; cap well above that so a crafted multi-GB size can't
        // trigger a huge allocation before any parsing.
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
        if out.disc_number.is_none() {
            if let Some(ds) = disc_set {
                out.disc_number = Some(ds);
            }
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

/// Parse one `bdmt_<lang>.xml` document and return
/// `(title, description?, disc_set?)`. Returns `None` if no title
/// could be located — the caller treats this as "skip this file".
///
/// Title-element preference: `<di:name>` → `<di:title>` →
/// `<di:tableOfContents>/<di:titleName>` (first match wins, per the
/// authoring-tool conventions documented at the module level).
pub(crate) fn parse_bdmt_xml(xml_text: &str) -> Option<BdmtFields> {
    let title = extract_title(xml_text)?;
    // xml::text already returns a trimmed string (see xml::text), so the
    // description is only filtered for emptiness and XML-fragment noise.
    let description = xml::text(xml_text, "description")
        .filter(|s| !s.is_empty())
        .filter(|s| !looks_like_xml(s));
    let disc_set = extract_disc_set(xml_text);
    Some((title, description, disc_set))
}

/// Reject candidate description strings that are themselves XML
/// fragments — observed on a captured disc, where
/// `<di:description>` contained `<di:thumbnail href="…"/>` child
/// elements and no actual prose. Surfacing that raw to the JSON
/// output is worse than dropping the field entirely.
fn looks_like_xml(s: &str) -> bool {
    let t = s.trim_start();
    t.starts_with('<')
}

/// Try title-bearing element variants in priority order. The `xml`
/// helpers are case- and namespace-insensitive, so callers pass the
/// bare local name (no `di:` prefix).
fn extract_title(xml_text: &str) -> Option<String> {
    // Order matches the module-level convention: <di:name> first
    // (Paramount-style), then <di:title>, then the nested
    // tableOfContents/titleName form.
    // xml::text already trims its result, so an empty string after
    // extraction means a genuinely empty element.
    for tag in ["name", "title"] {
        if let Some(s) = xml::text(xml_text, tag) {
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    // tableOfContents/titleName: search inside the toc block so we
    // don't accidentally pick a stray <titleName> from elsewhere.
    if let Some((s, e)) = xml::find_element(xml_text, "tableOfContents", 0) {
        let block = &xml_text[s..e];
        if let Some(t) = xml::text(block, "titleName") {
            if !t.is_empty() {
                return Some(t);
            }
        }
    }
    None
}

/// Extract `(discNumber, numSets)` if both are present and parse as
/// `u32`. Accepts either `<di:numSets>` or `<di:numberOfSets>` for
/// the denominator (both forms appear in the wild).
fn extract_disc_set(xml_text: &str) -> Option<(u32, u32)> {
    let n = xml::text(xml_text, "discNumber")?
        .trim()
        .parse::<u32>()
        .ok()?;
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
mod tests {
    use super::*;

    #[test]
    fn extract_simple_title() {
        // Minimal Paramount-style document: <di:name> as the title
        // carrier inside a <discInfo> root.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>Aurora Drift</di:name>
</discInfo>"#;
        let (title, desc, set) = parse_bdmt_xml(xml).expect("title should parse");
        assert_eq!(title, "Aurora Drift");
        assert_eq!(desc, None);
        assert_eq!(set, None);
    }

    #[test]
    fn extract_title_element_variant() {
        // <di:title> is the alternate carrier; should be picked up
        // when <di:name> is absent.
        let xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:title>Echo Chamber</di:title>
  <di:description>A film about machines.</di:description>
</discInfo>"#;
        let (title, desc, _) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(title, "Echo Chamber");
        assert_eq!(desc.as_deref(), Some("A film about machines."));
    }

    #[test]
    fn extract_title_from_table_of_contents_fallback() {
        // Some authoring tools nest the title under tableOfContents.
        // No <di:name> or <di:title> at top level → fall back to
        // titleName inside tableOfContents.
        let xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:tableOfContents>
    <di:titleName>Feelings Two</di:titleName>
  </di:tableOfContents>
</discInfo>"#;
        let (title, _, _) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(title, "Feelings Two");
    }

    #[test]
    fn bdmt_size_gate_rejects_oversized_entries() {
        assert!(bdmt_size_acceptable(0));
        assert!(bdmt_size_acceptable(4096));
        assert!(bdmt_size_acceptable(MAX_BDMT_BYTES));
        assert!(!bdmt_size_acceptable(MAX_BDMT_BYTES + 1));
        // A crafted multi-GB size is rejected before any allocation.
        assert!(!bdmt_size_acceptable(8 * 1024 * 1024 * 1024));
        assert!(!bdmt_size_acceptable(u64::MAX));
    }

    #[test]
    fn disc_set_rejects_nonsensical_pairs() {
        // n > total, zero numerator, zero denominator → all None.
        let over = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>5</di:discNumber>
  <di:numSets>2</di:numSets>
</discInfo>"#;
        assert_eq!(parse_bdmt_xml(over).unwrap().2, None);

        let zero_n = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>0</di:discNumber>
  <di:numSets>5</di:numSets>
</discInfo>"#;
        assert_eq!(parse_bdmt_xml(zero_n).unwrap().2, None);

        let zero_total = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>1</di:discNumber>
  <di:numSets>0</di:numSets>
</discInfo>"#;
        assert_eq!(parse_bdmt_xml(zero_total).unwrap().2, None);

        // A valid pair still passes.
        let ok = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>2</di:discNumber>
  <di:numSets>3</di:numSets>
</discInfo>"#;
        assert_eq!(parse_bdmt_xml(ok).unwrap().2, Some((2, 3)));
    }

    #[test]
    fn extract_box_set_position() {
        let xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>Box Set Disc 2</di:name>
  <di:discNumber>2</di:discNumber>
  <di:numSets>5</di:numSets>
</discInfo>"#;
        let (_, _, set) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(set, Some((2, 5)));
    }

    #[test]
    fn extract_box_set_position_alternate_total_tag() {
        // <di:numberOfSets> is an alternate spelling we've seen.
        let xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>3</di:discNumber>
  <di:numberOfSets>6</di:numberOfSets>
</discInfo>"#;
        let (_, _, set) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(set, Some((3, 6)));
    }

    #[test]
    fn extract_box_set_requires_both_fields() {
        // discNumber alone (no total) yields None — we don't fabricate
        // a denominator.
        let xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>X</di:name>
  <di:discNumber>1</di:discNumber>
</discInfo>"#;
        let (_, _, set) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(set, None);
    }

    #[test]
    fn multiple_languages_keyed_correctly() {
        // Simulate driving parse_bdmt_xml from two synthetic XML
        // blobs and aggregating into DiscMetadata the same way parse()
        // would. This exercises the BTreeMap key handling without
        // needing a UdfFs.
        let eng_xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>Aurora Drift</di:name>
</discInfo>"#;
        let fra_xml = r#"<discInfo xmlns:di="urn:BDA:bdmv;disclibmeta">
  <di:name>Aurora Drift (Partie Deux)</di:name>
  <di:description>Suite du film fictif.</di:description>
</discInfo>"#;

        let mut meta = DiscMetadata::default();
        for (lang, blob) in [("eng", eng_xml), ("fra", fra_xml)] {
            let (title, desc, ds) = parse_bdmt_xml(blob).unwrap();
            meta.titles.insert(lang.to_string(), title);
            if let Some(d) = desc {
                meta.descriptions.insert(lang.to_string(), d);
            }
            if meta.disc_number.is_none() {
                if let Some(d) = ds {
                    meta.disc_number = Some(d);
                }
            }
        }

        assert_eq!(
            meta.titles.get("eng").map(String::as_str),
            Some("Aurora Drift")
        );
        assert_eq!(
            meta.titles.get("fra").map(String::as_str),
            Some("Aurora Drift (Partie Deux)")
        );
        assert!(meta.descriptions.get("eng").is_none());
        assert_eq!(
            meta.descriptions.get("fra").map(String::as_str),
            Some("Suite du film fictif.")
        );
        assert_eq!(meta.disc_number, None);
    }

    #[test]
    fn malformed_xml_returns_none() {
        // Random gibberish has no recognizable title element. We
        // document the contract: parse_bdmt_xml returns None, and
        // parse() (the caller) skips the file. Aggregating across
        // zero files leaves DiscMetadata::default() — which parse()
        // surfaces as None to its caller. Either is documented as
        // acceptable per the module spec.
        let bad = "this is not xml &&& <<< nope";
        assert!(parse_bdmt_xml(bad).is_none());

        // Half-open tag, no body, no close: also yields no title.
        let truncated = "<discInfo><di:name>";
        assert!(parse_bdmt_xml(truncated).is_none());
    }

    #[test]
    fn description_with_only_child_xml_is_dropped() {
        // Real-world bug: <di:description> contained only
        // <di:thumbnail/> child elements with no actual prose. The
        // previous parser surfaced the raw XML fragment as the
        // description string. Now we reject candidates that begin
        // with `<`.
        let xml = r#"<discInfo>
            <di:name>Skyline Run</di:name>
            <di:description>
              <di:thumbnail href="sample_meta_sm.jpg" />
              <di:thumbnail href="sample_meta_lg.jpg" />
            </di:description>
        </discInfo>"#;
        let (title, description, _) =
            parse_bdmt_xml(xml).expect("title is present so parse must succeed");
        assert_eq!(title, "Skyline Run");
        assert!(
            description.is_none(),
            "description containing only XML children must be dropped, got {description:?}"
        );
    }

    #[test]
    fn description_with_plain_text_passes_through() {
        // The legitimate case still works: a description with actual
        // prose survives the looks_like_xml filter.
        let xml = r#"<discInfo>
            <di:name>Some Movie</di:name>
            <di:description>An epic tale of one man's quest for tea.</di:description>
        </discInfo>"#;
        let (_, description, _) = parse_bdmt_xml(xml).expect("must parse");
        assert_eq!(
            description.as_deref(),
            Some("An epic tale of one man's quest for tea.")
        );
    }

    #[test]
    fn whitespace_in_title_is_trimmed() {
        let xml = r#"<discInfo><di:name>
            Aurora Drift
        </di:name></discInfo>"#;
        let (title, _, _) = parse_bdmt_xml(xml).unwrap();
        assert_eq!(title, "Aurora Drift");
    }

    #[test]
    fn lang_code_extraction() {
        assert_eq!(lang_code_from_filename("bdmt_eng.xml"), Some("eng".into()));
        assert_eq!(lang_code_from_filename("BDMT_FRA.XML"), Some("fra".into()));
        assert_eq!(lang_code_from_filename("bdmt_jpn.xml"), Some("jpn".into()));
        // Non-matching cases:
        assert_eq!(lang_code_from_filename("bdmt_.xml"), None);
        assert_eq!(lang_code_from_filename("bdmt_engl.xml"), None);
        assert_eq!(lang_code_from_filename("bdmt_e1g.xml"), None);
        assert_eq!(lang_code_from_filename("bdmt_eng.txt"), None);
        assert_eq!(lang_code_from_filename("foo.xml"), None);
    }
}

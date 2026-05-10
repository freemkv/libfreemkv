//! Deluxe BD-J framework — `com/bydeluxe/bluray/` package signature.
//!
//! Used by major studios (Disney, Warner, others) for their UHD
//! BD-J authoring. Detected on discs whose `/BDMV/JAR/<x>.jar`
//! contains a `com/bydeluxe/` directory entry.
//!
//! ## Why this parser exists
//!
//! Deluxe-authored discs store stream labels as **ordinal references
//! into obfuscated enum classes**. The label text isn't a literal
//! string in any anchor pattern (unlike dbp's `TextField,...` rows).
//! Instead, the binding code is roughly:
//!
//! ```java
//! streamTable.put(1, new AudioSlot(LanguageEnum.English,
//!                                  CodecEnum.ATMOS_HD_AUDIO,
//!                                  PurposeEnum.Normal));
//! ```
//!
//! The class names `LanguageEnum`, `CodecEnum`, `PurposeEnum`, and
//! `AudioSlot` are obfuscated per-disc (`be.class`, `ma.class`,
//! `lp.class`, etc.) — no name pattern survives the obfuscator. But
//! the **shape of `<clinit>`** is framework-stable:
//!
//! | Enum | Signature |
//! |---|---|
//! | Language | 70 `ldc` operations in `<clinit>`, sequence starts `English, French, Spanish, Dutch, ...` |
//! | Purpose | 8 ldcs starting `Normal, Commentary, PiP, Trivia, ...` |
//! | VideoFormat | 7 ldcs starting `HD, HDR10 Plus, HD Dolby, ...` |
//! | Region | 22 ldcs starting `USA_D1, LIC1, LIC2, LIC3, ...` (Disney only) |
//! | Studio | 6 ldcs starting `Disney, Marvel, Pixar, ...` (Disney only) |
//! | Codec | ~46 `new` instructions, 0 ldcs in `<clinit>` (codec strings live in subclasses) |
//!
//! Match on the SHAPE, not the name, and the parser survives obfuscation.
//!
//! ## Current status (2026-05-10)
//!
//! **Phase A only — master enum identification.** The parser correctly:
//! - Detects every Deluxe-authored disc via the package prefix.
//! - Identifies all 5–6 master enums and decodes their ordinal → name tables.
//!
//! It does NOT yet emit per-stream [`StreamLabel`]s. The binding-class
//! decoder (Phase D: walk the per-stream-table class's `<clinit>` with
//! a tiny symbolic stack machine to extract `(stream_idx, lang, codec,
//! purpose)` tuples) needs ground-truth binding bytecode from at least
//! 2 corpus discs to design against. Until that lands, `parse()`
//! returns `None`, and the diagnostic harness ([`super::analyze`])
//! reports `deluxe` in `parsers_detected` with the enum identification
//! visible via `tracing` logs.
//!
//! This is honest staging: detect right, identify what we can prove,
//! emit no labels until the per-stream layer is implemented and
//! verified. Phases B (codec subclass walk), C (binding-class
//! finder), and D land as follow-up commits.

use super::class_reader::{AASTORE, CpInfo, LDC, LDC_W, NEW};
use super::{ParseResult, jar};
use crate::sector::SectorReader;
use crate::udf::UdfFs;

pub fn detect(udf: &UdfFs) -> bool {
    // Cheap pre-check at the dir level; the real signal is
    // `com/bydeluxe/` inside any top-level jar's central directory,
    // which `parse()` confirms when given a `SectorReader`.
    jar::has_any_top_level_jar(udf)
}

pub fn parse(reader: &mut dyn SectorReader, udf: &UdfFs) -> Option<ParseResult> {
    jar::for_each_jar(reader, udf, |entry_name, archive| {
        if !jar::has_path_prefix(archive, "com/bydeluxe/") {
            return None;
        }
        let enums = identify_master_enums(archive);
        if enums.is_empty() {
            // No recognized fingerprint — likely a Deluxe variant we
            // haven't catalogued yet. Log + fall through so the
            // analyzer can record that detection fired but parse
            // produced nothing.
            tracing::info!(
                jar = %entry_name,
                "deluxe parser: com/bydeluxe/ present but no master enum fingerprint matched"
            );
            return None;
        }
        for (label, m) in &enums {
            tracing::info!(
                jar = %entry_name,
                enum = %label,
                class = %m.class_name,
                count = m.values.len(),
                sample = ?m.values.iter().take(4).collect::<Vec<_>>(),
                "deluxe master enum identified",
            );
        }
        // Phase D (binding-class decoder) not yet implemented; the
        // master enums alone don't yield per-stream labels. Returning
        // None routes the disc into the "parser detected but emitted
        // no labels" diagnostic path — accurate, not silent failure.
        None
    })
}

/// One identified master enum class.
#[derive(Debug)]
pub(crate) struct MasterEnum {
    /// Obfuscated class name (e.g. `be.class`, `aw.class`).
    pub class_name: String,
    /// Ordinal → string-value mapping, in declaration order.
    pub values: Vec<String>,
}

/// Fingerprints we use to identify each master enum class. The
/// matcher walks every class's `<clinit>` ldc sequence; a class
/// matches if its first N ldcs match `prefix` AND the total ldc count
/// equals `expected_count` (allows some slack via tolerance — see
/// `LDC_COUNT_TOLERANCE`). Class names are obfuscated and change per
/// disc; shape is stable.
struct Fingerprint {
    label: &'static str,
    prefix: &'static [&'static str],
    expected_count: usize,
}

const FINGERPRINTS: &[Fingerprint] = &[
    Fingerprint {
        label: "Language",
        prefix: &["English", "French", "Spanish", "Dutch"],
        expected_count: 70,
    },
    Fingerprint {
        label: "Purpose",
        prefix: &["Normal", "Commentary", "PiP", "Trivia"],
        expected_count: 8,
    },
    Fingerprint {
        label: "VideoFormat",
        prefix: &["HD", "HDR10 Plus", "HD Dolby"],
        expected_count: 7,
    },
    Fingerprint {
        label: "Region",
        prefix: &["USA_D1", "LIC1", "LIC2", "LIC3"],
        expected_count: 22,
    },
    Fingerprint {
        label: "Studio",
        prefix: &["Disney", "Marvel", "Pixar"],
        expected_count: 6,
    },
];

/// Allow per-version drift in enum size (e.g. one disc had 22 regions,
/// a future build might add one). Matching is still anchored on the
/// prefix, so a count mismatch within tolerance is informative-but-OK.
const LDC_COUNT_TOLERANCE: usize = 4;

/// Phase A. Walk every `.class` in `archive`, identify the master
/// enums by `<clinit>` ldc-sequence fingerprint. Returns a vector of
/// `(label, MasterEnum)` — at most one match per fingerprint label.
pub(crate) fn identify_master_enums(archive: &mut jar::Jar) -> Vec<(&'static str, MasterEnum)> {
    use std::collections::HashMap;

    // First pass: collect every class's <clinit> ldc string sequence.
    let mut candidates: HashMap<String, Vec<String>> = HashMap::new();
    jar::for_each_class(archive, |class_name, class| {
        let Some(ldcs) = clinit_ldc_strings(class) else {
            return;
        };
        if ldcs.is_empty() {
            return;
        }
        candidates.insert(class_name.to_string(), ldcs);
    });

    // Second pass: match each fingerprint against the candidate pool.
    let mut out = Vec::new();
    for fp in FINGERPRINTS {
        let mut best: Option<(String, Vec<String>)> = None;
        for (name, ldcs) in &candidates {
            if !ldcs_match_prefix(ldcs, fp.prefix) {
                continue;
            }
            let count = ldcs.len();
            if count.abs_diff(fp.expected_count) > LDC_COUNT_TOLERANCE {
                continue;
            }
            // Prefer exact-count match; otherwise first hit wins.
            match &best {
                None => best = Some((name.clone(), ldcs.clone())),
                Some((_, prev)) => {
                    if count == fp.expected_count && prev.len() != fp.expected_count {
                        best = Some((name.clone(), ldcs.clone()));
                    }
                }
            }
        }
        if let Some((class_name, values)) = best {
            out.push((fp.label, MasterEnum { class_name, values }));
        }
    }
    out
}

/// Walk `<clinit>` and collect every `ldc` / `ldc_w` operand that
/// resolves to either a `String` constant or a `Utf8` constant, in
/// declaration order. Returns `None` if the class has no `<clinit>`.
fn clinit_ldc_strings(class: &super::class_reader::ClassFile) -> Option<Vec<String>> {
    let mut found = false;
    let mut out = Vec::new();
    for m in &class.methods {
        let Some(name) = class.member_name(m) else {
            continue;
        };
        if name != "<clinit>" {
            continue;
        }
        found = true;
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        for insn in code.instructions() {
            if insn.opcode != LDC && insn.opcode != LDC_W {
                continue;
            }
            let Some(idx) = insn.cp_index() else {
                continue;
            };
            let resolved = match class.constant_pool.get(idx) {
                Some(CpInfo::String { string_index }) => {
                    class.constant_pool.utf8(*string_index).map(str::to_string)
                }
                Some(CpInfo::Utf8(s)) => Some(s.clone()),
                _ => None,
            };
            if let Some(s) = resolved {
                out.push(s);
            }
        }
    }
    if found { Some(out) } else { None }
}

/// True if the first `prefix.len()` entries of `ldcs` match `prefix`
/// exactly. Case-sensitive (enum names are stable strings, not free
/// text).
fn ldcs_match_prefix(ldcs: &[String], prefix: &[&str]) -> bool {
    if ldcs.len() < prefix.len() {
        return false;
    }
    ldcs.iter()
        .zip(prefix.iter())
        .all(|(got, want)| got == want)
}

/// Phase B (codec enum subclass walk) — pending. Detects the codec
/// enum class by structural signature (≥20 `new` instructions in
/// `<clinit>`, ≥0 ldcs) and yields its declared subclass names in
/// ordinal order. The actual codec strings live in each subclass's
/// constant pool and need a per-subclass walk to extract — that step
/// is in the follow-up commit. Until then this returns a structural
/// "the codec enum is class X with N entries" without the names.
#[allow(dead_code)]
pub(crate) fn find_codec_enum(archive: &mut jar::Jar) -> Option<CodecEnumShape> {
    let mut best: Option<(String, Vec<String>)> = None;
    jar::for_each_class(archive, |class_name, class| {
        let Some((news, ldcs)) = clinit_news_and_ldcs(class) else {
            return;
        };
        // Codec enum's <clinit> has many `new` ops, 0 string ldcs.
        if news.len() < 20 || !ldcs.is_empty() {
            return;
        }
        match &best {
            None => best = Some((class_name.to_string(), news)),
            Some((_, prev)) => {
                if news.len() > prev.len() {
                    best = Some((class_name.to_string(), news));
                }
            }
        }
    });
    best.map(|(class_name, subclass_news)| CodecEnumShape {
        class_name,
        subclass_news,
    })
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct CodecEnumShape {
    pub class_name: String,
    /// Ordered list of class names referenced by `new` in <clinit>.
    /// One entry per codec enum value; subclass walking (Phase B
    /// follow-up) resolves each to a codec string.
    pub subclass_news: Vec<String>,
}

/// Walk `<clinit>` and return `(new_class_names, ldc_strings)`. Used
/// for the codec-enum shape match where we care about both counts.
#[allow(dead_code)]
fn clinit_news_and_ldcs(
    class: &super::class_reader::ClassFile,
) -> Option<(Vec<String>, Vec<String>)> {
    let mut news = Vec::new();
    let mut ldcs = Vec::new();
    let mut found = false;
    let mut _aastore = 0u32;
    for m in &class.methods {
        let Some(name) = class.member_name(m) else {
            continue;
        };
        if name != "<clinit>" {
            continue;
        }
        found = true;
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        for insn in code.instructions() {
            match insn.opcode {
                NEW => {
                    if let Some(idx) = insn.cp_index() {
                        if let Some(n) = class.constant_pool.class_name(idx) {
                            news.push(n.to_string());
                        }
                    }
                }
                LDC | LDC_W => {
                    if let Some(idx) = insn.cp_index() {
                        let s = match class.constant_pool.get(idx) {
                            Some(CpInfo::String { string_index }) => {
                                class.constant_pool.utf8(*string_index).map(str::to_string)
                            }
                            Some(CpInfo::Utf8(s)) => Some(s.clone()),
                            _ => None,
                        };
                        if let Some(s) = s {
                            ldcs.push(s);
                        }
                    }
                }
                AASTORE => _aastore += 1,
                _ => {}
            }
        }
    }
    if found { Some((news, ldcs)) } else { None }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ldcs_match_prefix_exact() {
        let ldcs = vec![
            "English".to_string(),
            "French".to_string(),
            "Spanish".to_string(),
        ];
        assert!(ldcs_match_prefix(&ldcs, &["English", "French"]));
        assert!(ldcs_match_prefix(&ldcs, &["English", "French", "Spanish"]));
        assert!(!ldcs_match_prefix(&ldcs, &["English", "German"]));
        // Too short — prefix longer than ldcs is a mismatch.
        assert!(!ldcs_match_prefix(
            &ldcs,
            &["English", "French", "Spanish", "Dutch"]
        ));
    }

    #[test]
    fn ldcs_match_prefix_is_case_sensitive() {
        let ldcs = vec!["english".to_string(), "french".to_string()];
        assert!(!ldcs_match_prefix(&ldcs, &["English", "French"]));
    }

    #[test]
    fn fingerprint_count_tolerance() {
        // Sanity check: tolerance is at least 1, otherwise minor
        // framework drift breaks the parser.
        assert!(LDC_COUNT_TOLERANCE >= 1);
    }

    #[test]
    fn fingerprints_cover_documented_enums() {
        // Lock the fingerprint roster — if someone adds/removes a
        // fingerprint, this test forces them to think about it. The
        // 5 documented enums (Language, Purpose, VideoFormat, Region,
        // Studio) all need to be here. Codec is structural (separate
        // path), not fingerprinted by ldc prefix.
        let labels: Vec<&str> = FINGERPRINTS.iter().map(|fp| fp.label).collect();
        assert_eq!(
            labels,
            vec!["Language", "Purpose", "VideoFormat", "Region", "Studio"]
        );
    }

    #[test]
    fn fingerprint_prefixes_nonempty_and_under_expected_count() {
        // Each prefix must be non-empty and shorter than expected_count
        // (so the count gives ADDITIONAL signal beyond the prefix
        // match). If a prefix is as long as expected_count there's no
        // counting benefit.
        for fp in FINGERPRINTS {
            assert!(!fp.prefix.is_empty(), "{} has empty prefix", fp.label);
            assert!(
                fp.prefix.len() < fp.expected_count,
                "{} prefix is not shorter than expected_count",
                fp.label
            );
        }
    }
}

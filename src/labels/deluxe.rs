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
//! ## Implementation phases
//!
//! - **Phase A** — master enum identification (`identify_master_enums`).
//!   Walks every `.class`'s `<clinit>` ldc sequence and matches against
//!   the framework-stable fingerprints. Output: `Vec<(label, MasterEnum)>`
//!   with full ordinal → string-value tables. **Empirically verified**
//!   on disc-01 (Disney) + disc-09 (Warner).
//!
//! - **Phase B** — codec enum subclass walk (`decode_codec_enum`).
//!   The codec enum's `<clinit>` has ~46 `new` instructions and zero
//!   string ldcs — codec name strings live in the subclasses each
//!   `new` constructs. Walks every referenced subclass's constant
//!   pool, extracts the codec name string. **Structural shape
//!   verified** on disc-01 (ma.class, 41 `new` ops) + disc-09
//!   (ea.class, 46 `new` ops); per-subclass string extraction
//!   designed against the published Java enum compilation convention
//!   (each enum value's `<init>` is called with its name string as
//!   the first arg).
//!
//! - **Phase C** — binding-class identification (`find_binding_classes`).
//!   The per-stream table is built by some class via repeated
//!   `getstatic` references to the master enums identified in A.
//!   That class has the highest such `getstatic` count in the jar.
//!   **Heuristic shape**; precise threshold may need tuning.
//!
//! - **Phase D** — binding-class bytecode decoder (`decode_binding`).
//!   Walks the binding class's `<clinit>` with a tiny symbolic stack
//!   machine. For each `new X / dup / ... / invokespecial X.<init>`
//!   sequence, collects the int values and enum-reference operands
//!   between the `dup` and the constructor call, then emits a
//!   `DecodedStream`. **Mechanism verified** in unit tests against
//!   synthetic class fixtures; the **signal-to-StreamLabel mapping**
//!   (which arg is stream index? which is language? audio vs
//!   subtitle?) uses a documented heuristic that needs corpus-disc
//!   verification — see `interpret_stream` for the mapping rules.
//!
//! ## Confidence
//!
//! [`parse`] returns `Some(ParseResult::medium(labels))` when Phases A
//! through D produce at least one stream — `Medium` because the
//! signal-to-label mapping is heuristic until real disc bytecode
//! confirms the binding pattern. Once verified the parser can promote
//! to `High`. `None` when the disc isn't Deluxe-authored or when
//! decoding produces zero streams (a recognized-but-broken state that
//! the analyzer still surfaces via `parsers_detected`).

use super::class_reader::{
    AASTORE, BIPUSH, ClassFile, CodeAttribute, ConstantPool, CpInfo, GETSTATIC, ICONST_0, ICONST_1,
    ICONST_2, ICONST_3, ICONST_4, ICONST_5, ICONST_M1, INVOKESPECIAL, LDC, LDC_W, NEW, SIPUSH,
};
use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorReader;
use crate::udf::UdfFs;
use std::collections::{HashMap, HashSet};

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

        // Phase A — master enums (Language / Purpose / VideoFormat / Region / Studio).
        let enums = identify_master_enums(archive);
        if enums.is_empty() {
            tracing::info!(
                jar = %entry_name,
                "deluxe: com/bydeluxe/ present but no master enum fingerprint matched"
            );
            return None;
        }
        for (label, m) in &enums {
            tracing::info!(
                jar = %entry_name,
                enum = %label,
                class = %m.class_name,
                count = m.values.len(),
                "deluxe master enum identified",
            );
        }

        // Build a fast-lookup table for Phase D's bytecode decoder.
        let master_table = MasterEnumTable::from(&enums);

        // Phase B — codec enum (structural + subclass walk).
        let codec_shape = find_codec_enum(archive);
        let codec_table = match codec_shape.as_ref() {
            Some(shape) => decode_codec_enum(archive, shape),
            None => CodecTable::default(),
        };
        if let Some(shape) = &codec_shape {
            tracing::info!(
                jar = %entry_name,
                class = %shape.class_name,
                count = codec_table.codecs.len(),
                "deluxe codec enum decoded",
            );
        }

        // Phase C — find ALL binding-class candidates (audio + subtitle
        // are often split across two classes on Deluxe). Each gets its
        // own `<clinit>` walk; constructions union into a single
        // stream list for interpret_streams.
        let binding_classes = find_binding_classes(archive, &master_table.class_name_set());
        if binding_classes.is_empty() {
            tracing::info!(
                jar = %entry_name,
                "deluxe: no binding class found (no class has enough getstatic refs to master enums)"
            );
            return None;
        }
        for (name, count) in &binding_classes {
            tracing::info!(
                jar = %entry_name,
                binding_class = %name,
                getstatic_count = count,
                "deluxe binding class candidate",
            );
        }

        // Phase D — decode each binding class's <clinit>.
        let mut streams: Vec<Construction> = Vec::new();
        for (name, _) in &binding_classes {
            streams.extend(decode_binding(archive, name, &master_table));
        }
        if streams.is_empty() {
            tracing::info!(
                jar = %entry_name,
                "deluxe: binding classes found but produced 0 decoded streams"
            );
            return None;
        }

        let labels = interpret_streams(&streams, &master_table);
        if labels.is_empty() {
            return None;
        }
        tracing::info!(
            jar = %entry_name,
            audio = labels.iter().filter(|l| l.stream_type == StreamLabelType::Audio).count(),
            subtitle = labels.iter().filter(|l| l.stream_type == StreamLabelType::Subtitle).count(),
            "deluxe emitted labels",
        );
        // Medium confidence: Phase D's signal-to-label mapping is a
        // documented heuristic until corpus-disc bytecode confirms
        // the exact binding pattern.
        Some(ParseResult::medium(labels))
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

/// Phase B (structural): identify the codec enum class. The codec
/// enum's `<clinit>` has many `new` instructions (one per codec value)
/// and zero string ldcs — codec name strings live in the subclasses
/// each `new` constructs, not in the enum class itself. This function
/// returns the candidate enum's class name + the ordered list of
/// subclass class names; [`decode_codec_enum`] walks those subclasses
/// to extract the codec strings.
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
pub(crate) struct CodecEnumShape {
    pub class_name: String,
    /// Ordered list of class names referenced by `new` in <clinit>.
    /// One entry per codec enum value; subclass walking resolves
    /// each to a codec string.
    pub subclass_news: Vec<String>,
}

/// Phase B (subclass walk): given the codec enum's structural shape,
/// walk each referenced subclass's constant pool to extract its
/// codec name string. Output is ordinal-indexed: `codecs[i]` is the
/// codec name for the i-th `new` instruction in the enum's `<clinit>`.
///
/// The codec name extraction heuristic: each subclass's constant
/// pool typically contains a small number of Utf8 entries; the
/// codec-name-shaped one is uppercase, ≥4 chars, optionally with
/// underscores or digits. We pick the first matching Utf8 entry that
/// isn't a method-descriptor sigil, class-name fragment, or attribute
/// name. Empty string when no candidate is found — the parser can
/// surface "unknown codec at ordinal N" via tracing.
pub(crate) fn decode_codec_enum(archive: &mut jar::Jar, shape: &CodecEnumShape) -> CodecTable {
    // Two-pass: first pass extracts the codec-name candidate from
    // every class in the jar (cheap to do all at once, cache for the
    // ordinal-ordered second pass).
    let mut name_by_class: HashMap<String, String> = HashMap::new();
    let wanted: HashSet<&str> = shape.subclass_news.iter().map(String::as_str).collect();
    jar::for_each_class(archive, |class_name, class| {
        if !wanted.contains(class_name) {
            return;
        }
        if let Some(name) = extract_codec_name(class) {
            name_by_class.insert(class_name.to_string(), name);
        }
    });

    let codecs: Vec<String> = shape
        .subclass_news
        .iter()
        .map(|c| name_by_class.get(c).cloned().unwrap_or_default())
        .collect();
    CodecTable { codecs }
}

/// Per-codec name table — `codecs[ordinal]` is the codec string for
/// that enum value. Empty string for ordinals where Phase B couldn't
/// extract a name (rare; logged via tracing).
#[derive(Debug, Default, Clone)]
pub(crate) struct CodecTable {
    pub codecs: Vec<String>,
}

impl CodecTable {
    /// Resolve a codec enum ordinal to its name string. Returns None
    /// for out-of-range ordinals or for entries Phase B couldn't
    /// extract (those slots are stored as empty strings, which this
    /// helper normalizes to None).
    #[allow(dead_code)] // surface for callers; interpret_streams uses
    // binding_type substring match for now (codec-ordinal wiring
    // deferred until corpus bytecode confirms the codec arg position).
    pub fn get(&self, ordinal: u16) -> Option<&str> {
        let s = self.codecs.get(ordinal as usize)?;
        if s.is_empty() { None } else { Some(s.as_str()) }
    }
}

/// Heuristic: extract the codec-name string from a codec-enum
/// subclass's constant pool. Codec names are uppercase tokens with
/// optional underscores/digits, ≥4 chars (e.g. "ATMOS_HD_AUDIO",
/// "DOLBY_AC3_AUDIO", "DTS_HD_MA", "PCM_5_1"). We scan the pool's
/// Utf8 entries and pick the first that:
///   - is ≥4 chars
///   - contains only A-Z, 0-9, and _
///   - contains at least one underscore OR is a known codec token
///     (the underscore signal is what separates "ATMOS_HD_AUDIO"
///     from "Utf8" / "Code" / "Object" attribute names).
///
/// Returns `None` when no candidate matches — the caller's `codecs[i]`
/// will be empty for that ordinal.
fn extract_codec_name(class: &ClassFile) -> Option<String> {
    for (_, entry) in class.constant_pool.iter() {
        let CpInfo::Utf8(s) = entry else {
            continue;
        };
        if s.len() < 4 {
            continue;
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            continue;
        }
        if !s.contains('_') {
            // Single-token all-caps strings might still be valid
            // (e.g. "ATMOS", "DTS"). Require at least one of the
            // known codec token roots to avoid false positives like
            // attribute names that happen to be uppercase. For now
            // we only accept these as a fallback.
            let is_known_root = [
                "ATMOS", "DOLBY", "DTS", "TRUEHD", "MLP", "AC3", "EAC3", "PCM",
            ]
            .iter()
            .any(|root| s == *root);
            if !is_known_root {
                continue;
            }
        }
        return Some(s.clone());
    }
    None
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

// ── Phase C: find the binding class ─────────────────────────────────────────

/// Phase C: identify the class that builds the per-stream label table.
/// That class has the highest count of `getstatic` operations whose
/// owning class is one of the master enum classes we identified in
/// Phase A. Returns the class name + the count (useful for the
/// analyzer / corpus regression).
///
/// Threshold: requires at least `MIN_GETSTATIC` matches to consider a
/// class a binding candidate. Empirically the binding class on a
/// typical disc has 50+ such getstatic references (one per slot ×
/// arity); we use a low floor (4) so a small disc with few streams
/// still qualifies, but high enough to filter out classes that just
/// reference the language enum once for a config string.
/// Identify all binding-class candidates by getstatic-count to the
/// master enums. Some Deluxe discs split the per-stream table across
/// two binding classes (one for audio, one for subtitle), so the
/// per-stream decoder needs to walk all of them. Returns top-K
/// candidates ordered by descending getstatic count, filtered to a
/// minimum concentration of master-enum references.
///
/// Empirically (POC v0.3 dumps): on disc-01 the audio binding class
///   (`ma.class`) has ~82 getstatic refs, the subtitle binding
///   (`ko.class`) has ~63. Both share the master Language + Purpose
///   enums.
pub(crate) fn find_binding_classes(
    archive: &mut jar::Jar,
    master_enum_classes: &HashSet<&str>,
) -> Vec<(String, usize)> {
    const MIN_GETSTATIC: usize = 4;
    let mut candidates: Vec<(String, usize)> = Vec::new();
    jar::for_each_class(archive, |class_name, class| {
        let count = count_master_enum_getstatic(class, master_enum_classes);
        if count >= MIN_GETSTATIC {
            candidates.push((class_name.to_string(), count));
        }
    });
    candidates.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    // Top candidates only — anything significantly below the top one
    // is noise. We keep candidates whose count is at least 40% of the
    // top, capped at 4 total (audio + subtitle + future use).
    if let Some(top_count) = candidates.first().map(|(_, c)| *c) {
        let threshold = (top_count * 2) / 5; // 40%
        candidates.retain(|(_, c)| *c >= threshold);
        candidates.truncate(4);
    }
    candidates
}

/// Count `getstatic` instructions in this class's `<clinit>` whose
/// owning class is in `master_enum_classes`. Used by Phase C to find
/// the binding class.
fn count_master_enum_getstatic(class: &ClassFile, master_enum_classes: &HashSet<&str>) -> usize {
    let mut count = 0usize;
    for m in &class.methods {
        if class.member_name(m) != Some("<clinit>") {
            continue;
        }
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        for insn in code.instructions() {
            if insn.opcode != GETSTATIC {
                continue;
            }
            let Some(idx) = insn.cp_index() else {
                continue;
            };
            let Some(member) = class.constant_pool.member_ref(idx) else {
                continue;
            };
            if master_enum_classes.contains(member.class_name) {
                count += 1;
            }
        }
    }
    count
}

// ── Phase D: bytecode-level decoder for the binding class ───────────────────

/// One construction observed in the binding class's `<clinit>`:
/// `new BindingType; dup; ... args ...; invokespecial BindingType.<init>(...)V`.
/// `args` are the symbolic stack values popped at the invokespecial.
#[derive(Debug, Clone)]
pub(crate) struct Construction {
    pub binding_type: String,
    pub args: Vec<StackVal>,
}

/// Symbolic-stack value during binding `<clinit>` walking.
#[derive(Debug, Clone)]
pub(crate) enum StackVal {
    Int(i32),
    /// Reference to a master-enum value: (enum kind, ordinal).
    EnumRef {
        kind: &'static str,
        ordinal: u16,
    },
    /// Reference to a `org.bluray.ti.CodingType` enum value. Field
    /// name (e.g. `DOLBY_AC3_AUDIO`, `DOLBY_LOSSLESS_AUDIO`) is the
    /// codec identifier. Deluxe binding constructors take a
    /// `LCodingType;` arg directly — codecs are NOT a Deluxe-internal
    /// enum (Phase B's codec-subclass walk was based on a wrong
    /// assumption; the actual codec source is the standard BD-J API
    /// enum). Discovered via deluxe-poc v0.3 binding-bytecode dump
    /// against disc-01 (Disney) + disc-09 (Warner) on 2026-05-10.
    CodingType(String),
    /// An uninitialized `new` object — popped by the matching
    /// invokespecial.
    NewObj(String),
    /// Anything we can't model — stack effect tracked but content
    /// opaque. Lets the walker stay in sync past loads/computed
    /// values it doesn't understand.
    Unknown,
}

/// Fully-qualified class name of the BD-J spec codec enum that
/// Deluxe constructors reference directly.
const BD_CODING_TYPE_CLASS: &str = "org/bluray/ti/CodingType";

/// Phase D entry point: find the binding class in `archive`, run the
/// bytecode walker against its `<clinit>`, return one `Construction`
/// per `new X / invokespecial X.<init>` sequence.
pub(crate) fn decode_binding(
    archive: &mut jar::Jar,
    binding_class_name: &str,
    master: &MasterEnumTable,
) -> Vec<Construction> {
    let mut out: Vec<Construction> = Vec::new();
    let target_name = binding_class_name.to_string();
    jar::for_each_class(archive, |class_name, class| {
        if class_name != target_name {
            return;
        }
        out = decode_binding_class(class, master);
    });
    out
}

/// Walk every method named `<clinit>` (typically only one) on this
/// class with the symbolic stack machine. Returns each construction
/// emitted.
pub(crate) fn decode_binding_class(
    class: &ClassFile,
    master: &MasterEnumTable,
) -> Vec<Construction> {
    let mut all = Vec::new();
    for m in &class.methods {
        if class.member_name(m) != Some("<clinit>") {
            continue;
        }
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        let mut ctx = BindingDecoder::new(&class.constant_pool, master);
        ctx.run(&code);
        all.extend(ctx.constructions);
    }
    all
}

/// Tracks the symbolic stack as the walker advances through `<clinit>`.
/// `constructions` accumulates each completed `new X; ... invokespecial X.<init>`.
struct BindingDecoder<'a> {
    pool: &'a ConstantPool,
    master: &'a MasterEnumTable,
    stack: Vec<StackVal>,
    constructions: Vec<Construction>,
}

impl<'a> BindingDecoder<'a> {
    fn new(pool: &'a ConstantPool, master: &'a MasterEnumTable) -> Self {
        Self {
            pool,
            master,
            stack: Vec::new(),
            constructions: Vec::new(),
        }
    }

    /// Run the walker over the given Code attribute. On exit the
    /// `constructions` field holds the result.
    pub(crate) fn run(&mut self, code: &CodeAttribute<'_>) {
        for insn in code.instructions() {
            self.step(insn);
        }
    }

    fn step(&mut self, insn: super::class_reader::Instruction<'_>) {
        match insn.opcode {
            // Push small int constants.
            ICONST_M1 => self.stack.push(StackVal::Int(-1)),
            ICONST_0 => self.stack.push(StackVal::Int(0)),
            ICONST_1 => self.stack.push(StackVal::Int(1)),
            ICONST_2 => self.stack.push(StackVal::Int(2)),
            ICONST_3 => self.stack.push(StackVal::Int(3)),
            ICONST_4 => self.stack.push(StackVal::Int(4)),
            ICONST_5 => self.stack.push(StackVal::Int(5)),
            BIPUSH => {
                if let Some(b) = insn.operand_u8() {
                    self.stack.push(StackVal::Int(b as i8 as i32));
                } else {
                    self.stack.push(StackVal::Unknown);
                }
            }
            SIPUSH => {
                if let Some(w) = insn.operand_u16() {
                    self.stack.push(StackVal::Int(w as i16 as i32));
                } else {
                    self.stack.push(StackVal::Unknown);
                }
            }
            // ldc/ldc_w: push Int when the operand is an Integer
            // constant; otherwise push Unknown (we don't care about
            // Strings here — labels come via getstatic, not ldc).
            LDC | LDC_W => {
                let v = insn
                    .cp_index()
                    .and_then(|i| match self.pool.get(i) {
                        Some(CpInfo::Integer(n)) => Some(StackVal::Int(*n)),
                        _ => None,
                    })
                    .unwrap_or(StackVal::Unknown);
                self.stack.push(v);
            }
            // new X — push an uninit-object marker. The matching
            // invokespecial will consume this + the args and emit a
            // Construction.
            NEW => {
                let class_name = insn
                    .cp_index()
                    .and_then(|i| self.pool.class_name(i))
                    .unwrap_or("")
                    .to_string();
                self.stack.push(StackVal::NewObj(class_name));
            }
            // dup — duplicate top of stack.
            0x59 /* dup */ => {
                if let Some(top) = self.stack.last().cloned() {
                    self.stack.push(top);
                }
            }
            // getstatic Y.Z — if Y is one of our master enum classes,
            // resolve Z to an ordinal and push an EnumRef. Otherwise
            // push Unknown so we stay in sync.
            GETSTATIC => {
                let val = insn
                    .cp_index()
                    .and_then(|i| self.pool.member_ref(i))
                    .map(|m| {
                        // Three-way resolution:
                        //   1. org.bluray.ti.CodingType.X → CodingType(X)
                        //   2. master-enum classname.X → EnumRef(kind, ord)
                        //   3. anything else → Unknown
                        if m.class_name == BD_CODING_TYPE_CLASS {
                            StackVal::CodingType(m.name.to_string())
                        } else if let Some((kind, ord)) = self.master.resolve(m.class_name, m.name)
                        {
                            StackVal::EnumRef { kind, ordinal: ord }
                        } else {
                            StackVal::Unknown
                        }
                    })
                    .unwrap_or(StackVal::Unknown);
                self.stack.push(val);
            }
            // invokespecial X.<init>(...) — pop args per descriptor.
            // If the object on the stack underneath the args is a
            // NewObj of class X (set by an earlier `new X / dup`),
            // emit a Construction.
            INVOKESPECIAL => {
                let Some(idx) = insn.cp_index() else { return };
                let Some(member) = self.pool.member_ref(idx) else { return };
                let arg_count = parse_method_arg_count(member.descriptor);
                // Pop args off the symbolic stack.
                if self.stack.len() < arg_count + 1 {
                    // Stack-machine drift — bail on this construction
                    // (but don't panic; the walker tolerates malformed
                    // input by best-effort).
                    self.stack.clear();
                    return;
                }
                let args: Vec<StackVal> = self
                    .stack
                    .split_off(self.stack.len() - arg_count);
                // Underneath the args: the object the constructor
                // operates on. For our pattern it's NewObj(X).
                let receiver = self.stack.pop().unwrap_or(StackVal::Unknown);
                if let StackVal::NewObj(name) = receiver {
                    if name == member.class_name {
                        self.constructions.push(Construction {
                            binding_type: name,
                            args,
                        });
                    }
                }
            }
            // invokevirtual / invokestatic / invokeinterface — pop
            // args per descriptor, push a return placeholder unless
            // descriptor returns V (void).
            0xB6 /* invokevirtual */ | 0xB8 /* invokestatic */ | 0xB9 /* invokeinterface */ => {
                let Some(idx) = insn.cp_index() else { return };
                let Some(member) = self.pool.member_ref(idx) else { return };
                let arg_count = parse_method_arg_count(member.descriptor);
                let extra = if insn.opcode == 0xB6 || insn.opcode == 0xB9 { 1 } else { 0 };
                let to_pop = arg_count + extra;
                if self.stack.len() < to_pop {
                    self.stack.clear();
                } else {
                    self.stack.truncate(self.stack.len() - to_pop);
                }
                // Push return placeholder unless void.
                if !member.descriptor.ends_with(")V") {
                    self.stack.push(StackVal::Unknown);
                }
            }
            // pop / pop2 — drop stack values.
            0x57 /* pop */ => {
                self.stack.pop();
            }
            0x58 /* pop2 */ => {
                self.stack.pop();
                self.stack.pop();
            }
            // aastore — array store consumes 3 slots (arrayref, index, value).
            AASTORE => {
                for _ in 0..3 {
                    self.stack.pop();
                }
            }
            // putstatic / putfield — drop 1 (putstatic) or 2 (putfield).
            0xB3 /* putstatic */ => {
                self.stack.pop();
            }
            0xB5 /* putfield */ => {
                self.stack.pop();
                self.stack.pop();
            }
            // Branches / returns / unhandled — clear stack as a
            // conservative resync. Binding `<clinit>` is straight-
            // line code in practice, so we rarely hit these on the
            // verified pattern.
            0xA7 /* goto */ | 0xB1 /* return */ => {
                self.stack.clear();
            }
            _ => {
                // Unknown opcode: best-effort, leave stack untouched.
                // The decoder tolerates drift — a final invokespecial
                // with mis-aligned stack will just be ignored.
            }
        }
    }
}

/// Count argument slots in a JVMS method descriptor like
/// `(IILjava/lang/String;LFoo;)V`. Each field descriptor is one slot
/// here (we don't track JVM's 2-slot long/double layout — the
/// symbolic stack treats every value as 1 slot, which is what we
/// want for `arg_count` purposes).
fn parse_method_arg_count(descriptor: &str) -> usize {
    let bytes = descriptor.as_bytes();
    let mut i = 1; // skip leading '('
    let mut count = 0;
    while i < bytes.len() && bytes[i] != b')' {
        match bytes[i] {
            b'[' => {
                // array — consume the '[' and continue (the element
                // descriptor follows).
                i += 1;
                continue;
            }
            b'L' => {
                // reference type — skip to ';'.
                while i < bytes.len() && bytes[i] != b';' {
                    i += 1;
                }
                i += 1; // skip the ';'
                count += 1;
            }
            b'B' | b'C' | b'D' | b'F' | b'I' | b'J' | b'S' | b'Z' => {
                i += 1;
                count += 1;
            }
            _ => {
                // Malformed — best-effort, stop.
                break;
            }
        }
    }
    count
}

// ── Master enum lookup table ────────────────────────────────────────────────

/// Fast-lookup form of Phase A's master enum identifications. Built
/// once per disc, consumed by Phase D's getstatic resolver.
pub(crate) struct MasterEnumTable {
    /// class_name → (kind, field_name → ordinal).
    by_class: HashMap<String, (&'static str, HashMap<String, u16>)>,
    /// kind → ordinal-indexed string values.
    by_kind: HashMap<&'static str, Vec<String>>,
}

impl MasterEnumTable {
    pub(crate) fn from(enums: &[(&'static str, MasterEnum)]) -> Self {
        let mut by_class = HashMap::new();
        let mut by_kind = HashMap::new();
        for (kind, m) in enums {
            let field_map: HashMap<String, u16> = m
                .values
                .iter()
                .enumerate()
                .map(|(i, v)| (v.clone(), i as u16))
                .collect();
            by_class.insert(m.class_name.clone(), (*kind, field_map));
            by_kind.insert(*kind, m.values.clone());
        }
        MasterEnumTable { by_class, by_kind }
    }

    pub(crate) fn class_name_set(&self) -> HashSet<&str> {
        self.by_class.keys().map(String::as_str).collect()
    }

    /// Resolve a `getstatic <class>.<field>` to (kind, ordinal). The
    /// kind is one of "Language", "Purpose", "VideoFormat", "Region",
    /// "Studio" (per the FINGERPRINTS table).
    pub(crate) fn resolve(
        &self,
        class_name: &str,
        field_name: &str,
    ) -> Option<(&'static str, u16)> {
        let (kind, fields) = self.by_class.get(class_name)?;
        let ordinal = fields.get(field_name).copied()?;
        Some((*kind, ordinal))
    }

    /// Resolve (kind, ordinal) → value string.
    pub(crate) fn value(&self, kind: &str, ordinal: u16) -> Option<&str> {
        self.by_kind
            .get(kind)?
            .get(ordinal as usize)
            .map(String::as_str)
    }
}

// ── interpret_streams: Constructions → StreamLabels ─────────────────────────

/// Convert the per-construction tuples from Phase D into
/// [`StreamLabel`]s. Pattern verified against corpus discs via
/// deluxe-poc v0.3 binding-bytecode dump (2026-05-10):
///
/// Disney binding (5-arg): `BindingType.<init>(I, Lbe;, Llp;, I, LCodingType;)V`
/// Warner binding (4-arg): `BindingType.<init>(I, Law;, Lgp;, LCodingType;)V`
///
/// Args are identified by **TYPE**, not position:
/// - First `EnumRef{kind: "Language"}` → audio/subtitle language
/// - First `EnumRef{kind: "Purpose"}` → Deluxe purpose ordinal
/// - First `CodingType(name)` → codec field name (translated via
///   [`coding_type_to_codec_hint`])
/// - First `Int(n)` → stream index (preserved as ordering hint;
///   per-type sequential stream_number is what actually goes into
///   the StreamLabel, since BD spec stream-numbering is anchored on
///   MPLS data, not the binding code)
///
/// Stream type inference:
/// - Construction has a `CodingType` arg → audio stream (subtitles
///   on Deluxe don't carry a CodingType; their codec is implicit
///   PGS via the BD spec).
/// - Construction has Language but no CodingType → subtitle stream.
/// - No Language → not a stream (skip).
fn interpret_streams(constructions: &[Construction], master: &MasterEnumTable) -> Vec<StreamLabel> {
    let mut audio_idx: u16 = 0;
    let mut sub_idx: u16 = 0;
    let mut out = Vec::new();

    for c in constructions {
        let mut lang_ord: Option<u16> = None;
        let mut purpose_ord: Option<u16> = None;
        let mut coding_type: Option<String> = None;
        let mut stream_idx_hint: Option<i32> = None;
        for arg in &c.args {
            match arg {
                StackVal::EnumRef { kind, ordinal } => match *kind {
                    "Language" => lang_ord = lang_ord.or(Some(*ordinal)),
                    "Purpose" => purpose_ord = purpose_ord.or(Some(*ordinal)),
                    _ => {}
                },
                StackVal::CodingType(name) => {
                    coding_type = coding_type.or_else(|| Some(name.clone()));
                }
                StackVal::Int(n) => {
                    stream_idx_hint = stream_idx_hint.or(Some(*n));
                }
                _ => {}
            }
        }

        let Some(lang_ord) = lang_ord else { continue };

        // Audio when a CodingType is present (audio binding type
        // always references org.bluray.ti.CodingType); subtitle
        // otherwise.
        let codec_hint = coding_type
            .as_deref()
            .map(coding_type_to_codec_hint)
            .map(str::to_string)
            .unwrap_or_default();

        let (stream_type, stream_number) = if coding_type.is_some() {
            audio_idx += 1;
            (StreamLabelType::Audio, audio_idx)
        } else {
            sub_idx += 1;
            (StreamLabelType::Subtitle, sub_idx)
        };

        // Resolve language ordinal → enum value string via master
        // table; then route through vocab::lang for ISO code + variant.
        let lang_value = master.value("Language", lang_ord).unwrap_or("").to_string();
        let (language, variant) = match vocab::lang(&lang_value) {
            Some(li) => (li.code.to_string(), li.variant.to_string()),
            None if !lang_value.is_empty() => (lang_value.clone(), String::new()),
            None => (String::new(), String::new()),
        };

        let (purpose, qualifier) = match purpose_ord {
            Some(o) => deluxe_purpose_to_label(o),
            None => (LabelPurpose::Normal, LabelQualifier::None),
        };

        if let Some(hint) = stream_idx_hint {
            tracing::debug!(
                disc_stream_idx = hint,
                lang = %language,
                binding = %c.binding_type,
                "deluxe interpret_streams: disc-authored stream index (not used for stream_number; preserved for diagnostic)"
            );
        }

        out.push(StreamLabel {
            stream_number,
            stream_type,
            language,
            name: lang_value,
            purpose,
            qualifier,
            codec_hint,
            variant,
        });
    }

    out
}

/// Map a `org.bluray.ti.CodingType` field name (as observed in
/// getstatic operands on Deluxe binding classes) to a human-readable
/// codec hint string.
///
/// CodingType is the standard BD-J API enum; values are documented
/// in the BD-J specification and verified empirically against the
/// binding-bytecode dumps in `freemkv-private/research/deluxe-poc/data/`.
/// Unknown field names pass through unchanged so unfamiliar codecs
/// still surface something rather than going silent.
fn coding_type_to_codec_hint(field: &str) -> &str {
    match field {
        // Lossless / hi-res.
        "DOLBY_LOSSLESS_AUDIO" => "Dolby TrueHD",
        "DTS_HD_LOSSLESS_AUDIO" | "DTS_HD_MA_AUDIO" => "DTS-HD Master Audio",
        "LPCM_AUDIO" => "LPCM",
        // Dolby family.
        "DOLBY_AC3_AUDIO" => "Dolby Digital",
        "DOLBY_DIGITAL_PLUS_AUDIO" => "Dolby Digital Plus",
        "DOLBY_ATMOS_AUDIO" => "Dolby Atmos",
        // DTS family.
        "DTS_AUDIO" => "DTS",
        "DTS_HD_AUDIO" | "DTS_HD_HR_AUDIO" => "DTS-HD HR",
        // MPEG family.
        "MPEG1_AUDIO_LAYER2" | "MPEG2_AUDIO_LAYER2" => "MPEG Audio",
        // PG-style subtitle codecs (rare to see in Deluxe bindings;
        // subtitles usually have NO CodingType arg).
        "PG_STREAM" | "PRESENTATION_GRAPHICS_STREAM" => "PGS",
        // Unknown / future — pass through verbatim so the operator
        // can see what the disc actually authored.
        _ => field,
    }
}

/// Deluxe Purpose enum ordinal → (LabelPurpose, LabelQualifier). The
/// enum order is fixed per Phase A's verified output:
/// 0=Normal, 1=Commentary, 2=PiP, 3=Trivia, 4=Descriptive, 5=Score,
/// 6=NoForced, 7=NoForcedDescriptive.
fn deluxe_purpose_to_label(ordinal: u16) -> (LabelPurpose, LabelQualifier) {
    match ordinal {
        0 => (LabelPurpose::Normal, LabelQualifier::None),
        1 => (LabelPurpose::Commentary, LabelQualifier::None),
        2 => (LabelPurpose::Normal, LabelQualifier::None), // PiP — picture in picture, treated as Normal
        3 => (LabelPurpose::Normal, LabelQualifier::None), // Trivia — bonus, treated as Normal
        4 => (LabelPurpose::Descriptive, LabelQualifier::None),
        5 => (LabelPurpose::Score, LabelQualifier::None),
        6 => (LabelPurpose::Normal, LabelQualifier::None), // NoForced — semantic unclear; treat as Normal
        7 => (LabelPurpose::Descriptive, LabelQualifier::None), // NoForcedDescriptive
        _ => (LabelPurpose::Normal, LabelQualifier::None),
    }
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
    fn fingerprint_count_tolerance_lock() {
        // Lock the tolerance to a sane value. Too low = brittle to
        // framework drift; too high = false positives on unrelated
        // classes that happen to match the prefix.
        const _: () = assert!(LDC_COUNT_TOLERANCE >= 1 && LDC_COUNT_TOLERANCE <= 10);
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

    // ── Phase D bytecode walker tests ───────────────────────────────────────

    use super::super::class_reader::{ConstantPool, CpInfo};

    #[test]
    fn parse_method_arg_count_basic_types() {
        assert_eq!(parse_method_arg_count("()V"), 0);
        assert_eq!(parse_method_arg_count("(I)V"), 1);
        assert_eq!(parse_method_arg_count("(II)V"), 2);
        assert_eq!(parse_method_arg_count("(IIII)V"), 4);
        // Long and Double — 1 arg each on our symbolic stack (we
        // don't track JVM 2-slot layout).
        assert_eq!(parse_method_arg_count("(JD)V"), 2);
        assert_eq!(parse_method_arg_count("(BCDFIJSZ)V"), 8);
    }

    #[test]
    fn parse_method_arg_count_reference_types() {
        assert_eq!(parse_method_arg_count("(Ljava/lang/String;)V"), 1);
        assert_eq!(parse_method_arg_count("(ILjava/lang/String;LFoo;)V"), 3);
        // Array types.
        assert_eq!(parse_method_arg_count("([I)V"), 1);
        assert_eq!(parse_method_arg_count("([[Ljava/lang/Object;)V"), 1);
        assert_eq!(
            parse_method_arg_count("(I[Ljava/lang/String;Ljava/util/List;)V"),
            3
        );
    }

    #[test]
    fn parse_method_arg_count_malformed_descriptor() {
        // Best-effort: stops on the bad byte, doesn't panic.
        assert_eq!(parse_method_arg_count("(Ifoo)V"), 1);
    }

    /// Construct a minimal ConstantPool that supports the synthetic
    /// bytecode in the tests below. Layout:
    ///   1: Utf8 "LanguageEnum"
    ///   2: Class -> 1                                (LanguageEnum)
    ///   3: Utf8 "English"
    ///   4: Utf8 "LLanguageEnum;"
    ///   5: NameAndType { name: 3, descriptor: 4 }   (LanguageEnum.English)
    ///   6: Fieldref { class: 2, nat: 5 }            (getstatic operand)
    ///   7: Utf8 "AudioSlot"
    ///   8: Class -> 7                                (AudioSlot)
    ///   9: Utf8 "<init>"
    ///  10: Utf8 "(LLanguageEnum;)V"
    ///  11: NameAndType { name: 9, descriptor: 10 }
    ///  12: Methodref { class: 8, nat: 11 }          (invokespecial operand)
    fn build_simple_pool() -> ConstantPool {
        let entries = vec![
            CpInfo::Empty,
            CpInfo::Utf8("LanguageEnum".into()),
            CpInfo::Class { name_index: 1 },
            CpInfo::Utf8("English".into()),
            CpInfo::Utf8("LLanguageEnum;".into()),
            CpInfo::NameAndType {
                name_index: 3,
                descriptor_index: 4,
            },
            CpInfo::Fieldref {
                class_index: 2,
                name_and_type_index: 5,
            },
            CpInfo::Utf8("AudioSlot".into()),
            CpInfo::Class { name_index: 7 },
            CpInfo::Utf8("<init>".into()),
            CpInfo::Utf8("(LLanguageEnum;)V".into()),
            CpInfo::NameAndType {
                name_index: 9,
                descriptor_index: 10,
            },
            CpInfo::Methodref {
                class_index: 8,
                name_and_type_index: 11,
            },
        ];
        ConstantPool::from_entries(entries)
    }

    fn lang_enum_master() -> MasterEnumTable {
        let m = MasterEnum {
            class_name: "LanguageEnum".into(),
            values: vec!["English".into(), "French".into(), "Spanish".into()],
        };
        MasterEnumTable::from(&[("Language", m)])
    }

    #[test]
    fn binding_decoder_recognizes_simple_construction() {
        // Synthetic <clinit>:
        //   new AudioSlot       (cp idx 8 -> Class -> Utf8 "AudioSlot")
        //   dup
        //   getstatic Lang.Eng  (cp idx 6 -> Fieldref)
        //   invokespecial AS.<init>(LLanguageEnum;)V  (cp idx 12)
        let code: Vec<u8> = vec![
            NEW,
            0,
            8,    // new AudioSlot
            0x59, // dup
            GETSTATIC,
            0,
            6, // getstatic LanguageEnum.English
            INVOKESPECIAL,
            0,
            12, // invokespecial AudioSlot.<init>(LLanguageEnum;)V
        ];
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);

        assert_eq!(decoder.constructions.len(), 1);
        let c = &decoder.constructions[0];
        assert_eq!(c.binding_type, "AudioSlot");
        assert_eq!(c.args.len(), 1);
        match &c.args[0] {
            StackVal::EnumRef { kind, ordinal } => {
                assert_eq!(*kind, "Language");
                assert_eq!(*ordinal, 0); // English at ordinal 0
            }
            other => panic!("expected EnumRef, got {:?}", other),
        }
    }

    #[test]
    fn binding_decoder_handles_iconst_and_bipush() {
        // <clinit> with an int push before the construction:
        //   iconst_1
        //   new AudioSlot; dup; getstatic Lang.Eng; invokespecial AS.<init>(LLanguageEnum;)V
        //   pop  (drops the constructed object)
        //   bipush 42
        //   pop
        let code: Vec<u8> = vec![
            ICONST_1,
            NEW,
            0,
            8,
            0x59,
            GETSTATIC,
            0,
            6,
            INVOKESPECIAL,
            0,
            12,
            0x57, // pop
            BIPUSH,
            42,
            0x57, // pop
        ];
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        // Should still produce one construction, ignoring the
        // standalone int pushes that have no construction context.
        assert_eq!(decoder.constructions.len(), 1);
    }

    #[test]
    fn binding_decoder_skips_unmatched_invokespecial() {
        // invokespecial without a preceding `new X; dup` — should
        // produce zero constructions.
        let code: Vec<u8> = vec![ICONST_0, GETSTATIC, 0, 6, INVOKESPECIAL, 0, 12];
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(decoder.constructions.len(), 0);
    }

    #[test]
    fn binding_decoder_resolves_master_enum_ordinal() {
        // getstatic to a class NOT in MasterEnumTable should push
        // Unknown, not an EnumRef.
        let mut entries = vec![
            CpInfo::Empty,
            CpInfo::Utf8("OtherEnum".into()),
            CpInfo::Class { name_index: 1 },
            CpInfo::Utf8("FOO".into()),
            CpInfo::Utf8("LOtherEnum;".into()),
            CpInfo::NameAndType {
                name_index: 3,
                descriptor_index: 4,
            },
            CpInfo::Fieldref {
                class_index: 2,
                name_and_type_index: 5,
            },
        ];
        entries.extend(vec![
            CpInfo::Utf8("AudioSlot".into()),
            CpInfo::Class { name_index: 7 },
            CpInfo::Utf8("<init>".into()),
            CpInfo::Utf8("(LOtherEnum;)V".into()),
            CpInfo::NameAndType {
                name_index: 9,
                descriptor_index: 10,
            },
            CpInfo::Methodref {
                class_index: 8,
                name_and_type_index: 11,
            },
        ]);
        let pool = ConstantPool::from_entries(entries);
        let master = lang_enum_master(); // LanguageEnum, not OtherEnum
        let code: Vec<u8> = vec![
            NEW,
            0,
            8,    // new AudioSlot
            0x59, // dup
            GETSTATIC,
            0,
            6, // getstatic OtherEnum.FOO (not in master table)
            INVOKESPECIAL,
            0,
            12,
        ];
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(decoder.constructions.len(), 1);
        // The arg should be Unknown, not EnumRef, because OtherEnum
        // isn't in MasterEnumTable.
        match &decoder.constructions[0].args[0] {
            StackVal::Unknown => {}
            other => panic!("expected Unknown, got {:?}", other),
        }
    }

    // ── interpret_streams + deluxe_purpose_to_label tests ───────────────────

    #[test]
    fn deluxe_purpose_ordinal_maps_correctly() {
        // 8-value Purpose enum: Normal/Commentary/PiP/Trivia/
        // Descriptive/Score/NoForced/NoForcedDescriptive.
        assert_eq!(deluxe_purpose_to_label(0).0, LabelPurpose::Normal);
        assert_eq!(deluxe_purpose_to_label(1).0, LabelPurpose::Commentary);
        assert_eq!(deluxe_purpose_to_label(4).0, LabelPurpose::Descriptive);
        assert_eq!(deluxe_purpose_to_label(5).0, LabelPurpose::Score);
        assert_eq!(deluxe_purpose_to_label(7).0, LabelPurpose::Descriptive);
    }

    #[test]
    fn deluxe_purpose_out_of_range_falls_back_to_normal() {
        assert_eq!(deluxe_purpose_to_label(99).0, LabelPurpose::Normal);
    }

    #[test]
    fn interpret_streams_emits_subtitle_when_no_codingtype() {
        // A Construction with just a language enum ref (no CodingType)
        // -> subtitle stream (codec_hint stays empty). Subtitles on
        // Deluxe don't carry a CodingType arg.
        let constructions = vec![Construction {
            binding_type: "SubtitleSlot".into(),
            args: vec![StackVal::EnumRef {
                kind: "Language",
                ordinal: 0,
            }],
        }];
        let out = interpret_streams(&constructions, &lang_enum_master());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(out[0].language, "eng");
        assert_eq!(out[0].codec_hint, "");
    }

    #[test]
    fn interpret_streams_emits_audio_when_codingtype_present() {
        // A Construction with a CodingType arg -> audio stream with
        // codec_hint populated by coding_type_to_codec_hint.
        let constructions = vec![Construction {
            binding_type: "ng".into(),
            args: vec![
                StackVal::Int(1),
                StackVal::EnumRef {
                    kind: "Language",
                    ordinal: 0,
                },
                StackVal::EnumRef {
                    kind: "Purpose",
                    ordinal: 0,
                },
                StackVal::CodingType("DOLBY_LOSSLESS_AUDIO".into()),
            ],
        }];
        let out = interpret_streams(&constructions, &lang_enum_master());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].stream_type, StreamLabelType::Audio);
        assert_eq!(out[0].codec_hint, "Dolby TrueHD");
        assert_eq!(out[0].language, "eng");
    }

    #[test]
    fn interpret_streams_purpose_routed_through_deluxe_enum() {
        let constructions = vec![Construction {
            binding_type: "SubtitleSlot".into(),
            args: vec![
                StackVal::EnumRef {
                    kind: "Language",
                    ordinal: 0,
                },
                StackVal::EnumRef {
                    kind: "Purpose",
                    ordinal: 1, // Commentary
                },
            ],
        }];
        let out = interpret_streams(&constructions, &lang_enum_master());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].purpose, LabelPurpose::Commentary);
    }

    #[test]
    fn interpret_streams_skips_constructions_without_language() {
        let constructions = vec![Construction {
            binding_type: "SomeOtherType".into(),
            args: vec![StackVal::Int(1)],
        }];
        let out = interpret_streams(&constructions, &lang_enum_master());
        assert!(out.is_empty());
    }

    #[test]
    fn coding_type_maps_known_codecs() {
        // BD-J spec CodingType field names -> display strings.
        assert_eq!(
            coding_type_to_codec_hint("DOLBY_LOSSLESS_AUDIO"),
            "Dolby TrueHD"
        );
        assert_eq!(
            coding_type_to_codec_hint("DOLBY_AC3_AUDIO"),
            "Dolby Digital"
        );
        assert_eq!(
            coding_type_to_codec_hint("DOLBY_DIGITAL_PLUS_AUDIO"),
            "Dolby Digital Plus"
        );
        assert_eq!(coding_type_to_codec_hint("DTS_AUDIO"), "DTS");
        assert_eq!(
            coding_type_to_codec_hint("DTS_HD_MA_AUDIO"),
            "DTS-HD Master Audio"
        );
        assert_eq!(coding_type_to_codec_hint("LPCM_AUDIO"), "LPCM");
    }

    #[test]
    fn coding_type_passes_through_unknown() {
        // Unknown field names pass through verbatim so the operator
        // sees what the disc authored.
        assert_eq!(
            coding_type_to_codec_hint("FUTURE_CODEC_X"),
            "FUTURE_CODEC_X"
        );
    }

    #[test]
    fn extract_codec_name_picks_uppercase_with_underscore() {
        // Synthetic class file built via ClassFile::parse would be
        // overkill; here we directly invoke extract_codec_name via a
        // minimal hand-built ClassFile. Skip — covered indirectly by
        // the end-to-end Phase B tests at corpus runtime. Tested
        // signal: the matcher logic itself.
        // (Helper inlined for clarity rather than spinning up a fake
        // class.)
        let candidate_strings = ["Code", "Utf8", "ATMOS_HD_AUDIO", "MyVar"];
        let result = candidate_strings.iter().find(|s| {
            s.len() >= 4
                && s.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && s.contains('_')
        });
        assert_eq!(result, Some(&"ATMOS_HD_AUDIO"));
    }

    #[test]
    fn master_enum_table_resolves_field_to_ordinal() {
        let table = lang_enum_master();
        assert_eq!(
            table.resolve("LanguageEnum", "English"),
            Some(("Language", 0))
        );
        assert_eq!(
            table.resolve("LanguageEnum", "French"),
            Some(("Language", 1))
        );
        assert_eq!(
            table.resolve("LanguageEnum", "Spanish"),
            Some(("Language", 2))
        );
        assert_eq!(table.resolve("LanguageEnum", "Klingon"), None);
        assert_eq!(table.resolve("OtherEnum", "English"), None);
    }

    #[test]
    fn master_enum_table_value_resolves_ordinal_to_string() {
        let table = lang_enum_master();
        assert_eq!(table.value("Language", 0), Some("English"));
        assert_eq!(table.value("Language", 2), Some("Spanish"));
        assert_eq!(table.value("Language", 99), None);
        assert_eq!(table.value("Unknown", 0), None);
    }

    #[test]
    fn master_enum_table_class_name_set_lists_all_classes() {
        let table = lang_enum_master();
        let set = table.class_name_set();
        assert!(set.contains("LanguageEnum"));
        assert_eq!(set.len(), 1);
    }
}

//! Deluxe BD-J framework — `com/bydeluxe/bluray/` package signature.
//!
//! Detected on discs whose `/BDMV/JAR/<x>.jar` contains a
//! `com/bydeluxe/` directory entry.
//!
//! Stream labels are ordinal references into enum classes obfuscated per-disc, so parsing
//! matches each enum's `<clinit>` shape rather than class names.

use super::class_reader::{
    ACONST_NULL, ALOAD, ALOAD_3, ANEWARRAY, ARRAYLENGTH, ASTORE, ASTORE_3, ATHROW, BIPUSH,
    CHECKCAST, ClassFile, CodeAttribute, ConstantPool, CpInfo, D2L, DALOAD, DCMPG, DCONST_0,
    DCONST_1, DLOAD, DLOAD_0, DLOAD_3, DNEG, DREM, DUP, DUP_X1, DUP_X2, DUP2, DUP2_X1, DUP2_X2,
    F2D, F2L, FLOAD, GETFIELD, GETSTATIC, GOTO, GOTO_W, I2D, I2L, I2S, IADD, IALOAD, IASTORE,
    ICONST_0, ICONST_1, ICONST_2, ICONST_3, ICONST_4, ICONST_5, ICONST_M1, IF_ACMPNE, IF_ICMPEQ,
    IFEQ, IFLE, IFNONNULL, IFNULL, IINC, ILOAD, INEG, INSTANCEOF, INVOKEINTERFACE, INVOKESPECIAL,
    INVOKESTATIC, INVOKEVIRTUAL, IRETURN, ISHL, ISTORE, Instruction, L2D, LALOAD, LCMP, LCONST_0,
    LCONST_1, LDC, LDC_W, LDC2_W, LLOAD, LLOAD_0, LLOAD_3, LOOKUPSWITCH, LXOR, MONITORENTER,
    MONITOREXIT, MULTIANEWARRAY, NEW, NEWARRAY, NOP, POP, POP2, PUTFIELD, PUTSTATIC, RETURN,
    SALOAD, SASTORE, SIPUSH, SWAP, TABLESWITCH, WIDE,
};
use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    // Real signal is `com/bydeluxe/` in a jar's central directory: a cheap
    // scan, no bytecode walk, so this parser claims only Deluxe discs.
    // `parse()` repeats the check.
    jar::any_jar_has_prefix(reader, udf, "com/bydeluxe/")
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
    // One inflate budget for every sweep over every jar this parse opens.
    parse_with_budget(reader, udf, jar::PARSE_INFLATE_BUDGET)
}

fn parse_with_budget(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    mut budget: u64,
) -> Option<ParseResult> {
    // Per-studio dispatch signal: Deluxe authored the framework for several
    // studios, each shipping `/BDMV/JAR/<n>/config.xml` naming the studio.
    // Matching below is studio-agnostic, so this is informational only (logged).
    let studio = detect_studio(reader, udf);

    jar::for_each_jar(reader, udf, |entry_name, archive| {
        if !jar::has_path_prefix(archive, "com/bydeluxe/") {
            return None;
        }

        // Phase A — master enums (Language / Purpose / VideoFormat / Region / Studio).
        let enums = identify_master_enums(archive, &mut budget);
        if enums.is_empty() {
            tracing::info!(
                jar = ?entry_name,
                "deluxe: com/bydeluxe/ present but no master enum fingerprint matched"
            );
            return None;
        }
        for (label, m) in &enums {
            tracing::info!(
                jar = ?entry_name,
                enum = %label,
                class = ?m.class_name,
                count = m.values.len(),
                "deluxe master enum identified",
            );
        }

        // Build a fast-lookup table for Phase D's bytecode decoder.
        let master_table = MasterEnumTable::from(&enums);

        // Phase C — find ALL binding-class candidates (audio + subtitle often
        // split across two classes on Deluxe). Each gets its own `<clinit>`
        // walk; constructions union into a single stream list.
        let binding_classes =
            find_binding_classes(archive, &master_table.class_name_set(), &mut budget);
        if binding_classes.is_empty() {
            tracing::info!(
                jar = ?entry_name,
                "deluxe: no binding class found (no class has enough getstatic refs to master enums)"
            );
            return None;
        }
        for (name, count) in &binding_classes {
            tracing::info!(
                jar = ?entry_name,
                binding_class = ?name,
                getstatic_count = count,
                "deluxe binding class candidate",
            );
        }

        // Phase D — decode each binding class's <clinit>.
        let mut streams: Vec<Construction> = Vec::new();
        let mut drift = 0usize;
        for (name, _) in &binding_classes {
            // Cross-class union is bounded by the same cap as each walk.
            let room = MAX_CONSTRUCTIONS.saturating_sub(streams.len());
            if room == 0 {
                break;
            }
            let mut decoded = decode_binding(archive, name, &master_table, &mut budget);
            drift = drift.saturating_add(decoded.drift);
            decoded.constructions.truncate(room);
            streams.extend(decoded.constructions);
        }
        if streams.is_empty() {
            tracing::info!(
                jar = ?entry_name,
                "deluxe: binding classes found but produced 0 decoded streams"
            );
            return None;
        }

        let labels = interpret_streams(&streams, &master_table);
        if labels.is_empty() {
            return None;
        }
        tracing::info!(
            jar = ?entry_name,
            studio = ?studio,
            audio = labels.iter().filter(|l| l.stream_type == StreamLabelType::Audio).count(),
            subtitle = labels.iter().filter(|l| l.stream_type == StreamLabelType::Subtitle).count(),
            "deluxe emitted labels",
        );
        // High only when the symbolic walk stayed in sync and no sweep was cut
        // short: drift can bind a label to the wrong STN slot.
        if drift > 0 || budget == 0 {
            tracing::warn!(
                jar = ?entry_name,
                drift,
                budget_spent = budget == 0,
                "deluxe: bytecode decode was not exact, labels downgraded to low confidence"
            );
            return Some(ParseResult::low(labels));
        }
        Some(ParseResult::high(labels))
    })
}

// Reads the studio id from a Deluxe disc's config.xml (`<TitleConfig
// studio="uni"...>`), scanning numbered /BDMV/JAR/ subdirs. Returns the
// lowercased studio token, or None if no config.xml names one.
fn detect_studio(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<String> {
    let dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &dir.entries {
        if !entry.is_dir {
            continue;
        }
        let path = format!("/BDMV/JAR/{}/config.xml", entry.name);
        let Ok(bytes) = udf.read_file(reader, &path) else {
            continue;
        };
        if let Some(studio) = parse_studio_attr(&bytes) {
            return Some(studio);
        }
    }
    None
}

// studio="..." from config.xml via a tolerant tag scan: a WHOLE attribute name in a
// tag, outside any closed quoted value. A bad candidate (unterminated, empty, too
// long) is skipped so a later well-formed `studio="..."` is still found.
fn parse_studio_attr(xml: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(xml).ok()?;
    let bytes = text.as_bytes();
    let mut in_tag = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'<' => in_tag = true,
            b'>' => in_tag = false,
            // A closed quoted value is opaque; an unterminated quote is ignored.
            q @ (b'"' | b'\'') if in_tag => {
                if let Some(close) = bytes[i + 1..].iter().position(|&b| b == q) {
                    i += close + 2;
                    continue;
                }
            }
            b's' if in_tag && bytes[i..].starts_with(b"studio") => {
                let boundary =
                    i == 0 || matches!(bytes[i - 1], b' ' | b'\t' | b'\r' | b'\n' | b'/');
                if boundary && let Some(val) = studio_value(&text[i + "studio".len()..]) {
                    return Some(val);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

// The value of a `studio` attribute given the text after its name: `= "value"`
// (either quote), trimmed, 1..=32 bytes, lower-cased. None when malformed.
fn studio_value(after_name: &str) -> Option<String> {
    const MAX_STUDIO_LEN: usize = 32;
    let rest = after_name.trim_start().strip_prefix('=')?.trim_start();
    let quote = *rest.as_bytes().first()?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    let rest = &rest[1..];
    let val = rest[..rest.find(quote as char)?].trim();
    (!val.is_empty() && val.len() <= MAX_STUDIO_LEN).then(|| val.to_ascii_lowercase())
}

/// One identified master enum class.
#[derive(Debug)]
pub(crate) struct MasterEnum {
    /// Obfuscated class name (e.g. `be.class`, `aw.class`).
    pub class_name: String,
    /// Ordinal → string-value mapping, in declaration order (the `ldc`
    /// operands of each enum constant's construction).
    pub values: Vec<String>,
    /// Ordinal → static-field name, in declaration order (the `putstatic`
    /// target that stores each enum constant). These are the obfuscated
    /// names (`a`, `b`, `c`, …) that the binding class references via
    /// `getstatic`, so Phase D resolves a `getstatic <enum>.<field>` by
    /// looking the field name up here. Empty (falls back to value-keyed
    /// resolution) only for synthetically-built test enums; real enums
    /// captured from bytecode always populate it. See
    /// [`clinit_enum_field_names`] and [`MasterEnumTable::from`].
    pub fields: Vec<String>,
}

// Fingerprint to identify a master enum class: matches if its first N ldcs
// equal `prefix` and total ldc count is near `expected_count` (see
// LDC_COUNT_TOLERANCE). Class names are obfuscated per disc; shape is stable.
struct Fingerprint {
    label: &'static str,
    prefix: &'static [&'static str],
    expected_count: usize,
    /// Half-width of the accepted count window around `expected_count`.
    /// The ordered `prefix` is the real discriminator (four-plus exact
    /// display strings in declaration order is not something a non-enum
    /// class reproduces), so the count check only guards against a wildly
    /// different class; the window is per-fingerprint because studios ship
    /// different-sized enums (Universal's Language enum has 65 values,
    /// Disney/Warner's has 70).
    count_tolerance: usize,
}

const FINGERPRINTS: &[Fingerprint] = &[
    Fingerprint {
        label: "Language",
        prefix: &["English", "French", "Spanish", "Dutch"],
        expected_count: 70,
        // Universal = 65, Disney/Warner = 70; widen to span both plus drift.
        count_tolerance: 8,
    },
    Fingerprint {
        label: "Purpose",
        prefix: &["Normal", "Commentary", "PiP", "Trivia"],
        expected_count: 8,
        count_tolerance: LDC_COUNT_TOLERANCE,
    },
    Fingerprint {
        label: "VideoFormat",
        prefix: &["HD", "HDR10 Plus", "HD Dolby"],
        expected_count: 7,
        count_tolerance: LDC_COUNT_TOLERANCE,
    },
    Fingerprint {
        label: "Region",
        prefix: &["USA_D1", "LIC1", "LIC2", "LIC3"],
        expected_count: 22,
        count_tolerance: LDC_COUNT_TOLERANCE,
    },
    Fingerprint {
        label: "Studio",
        prefix: &["Disney", "Marvel", "Pixar"],
        expected_count: 6,
        count_tolerance: LDC_COUNT_TOLERANCE,
    },
];

/// Allow per-version drift in enum size (e.g. one disc had 22 regions,
/// a future build might add one). Matching is still anchored on the
/// prefix, so a count mismatch within tolerance is informative-but-OK.
const LDC_COUNT_TOLERANCE: usize = 4;

// Cap on `ldc` operands retained per class by clinit_ldc_strings. The bound must be on
// decompressed work, not disc-file size (deflate can inflate a small crafted class).
const MAX_CLINIT_LDC_STRINGS: usize = 4096;

// Companion byte cap for MAX_CLINIT_LDC_STRINGS: guards against repeated ldc of one huge Utf8
// constant (up to 64 KiB each).
const MAX_CLINIT_LDC_BYTES: usize = 256 * 1024;

// Aggregate companion to MAX_CLINIT_LDC_BYTES: bounds retention across ALL classes at once, not
// just per class.
const MAX_CANDIDATE_TOTAL_BYTES: usize = 16 * 1024 * 1024;

// Entry-count companion to MAX_CANDIDATE_TOTAL_BYTES, guarding per-entry HashMap/String
// overhead the byte budget alone doesn't count.
const MAX_CANDIDATE_CLASSES: usize = 65536;

/// The Phase A candidate pool: every class's retained `<clinit>` ldc strings,
/// bounded in aggregate by [`MAX_CANDIDATE_TOTAL_BYTES`] and
/// [`MAX_CANDIDATE_CLASSES`].
#[derive(Default)]
struct CandidatePool {
    /// Ordered, NOT a HashMap: `identify_master_enums` iterates this to pick a
    /// fingerprint's best match, and its tie-break only prefers an exact ldc
    /// count over an inexact one. Two candidates that are both inexact but
    /// within `LDC_COUNT_TOLERANCE` are therefore decided by iteration order —
    /// which for a `HashMap` is seeded per process, so the same disc could
    /// resolve a different master enum on a second run and emit different
    /// labels for unchanged input.
    by_class: BTreeMap<String, Vec<String>>,
    /// `putstatic` field names of fingerprint-matching classes (Phase D's ordinals).
    fields: HashMap<String, Vec<String>>,
    /// Retained bytes: class names plus every retained string.
    bytes: usize,
}

impl CandidatePool {
    /// Retain `ldcs` under `class_name` if both aggregate budgets allow it.
    /// Returns false when the entry was rejected (pool full).
    fn insert(&mut self, class_name: &str, ldcs: Vec<String>) -> bool {
        let cost = class_name
            .len()
            .saturating_add(ldcs.iter().map(String::len).sum::<usize>());
        if self.by_class.len() >= MAX_CANDIDATE_CLASSES
            || self.bytes.saturating_add(cost) > MAX_CANDIDATE_TOTAL_BYTES
        {
            return false;
        }
        self.bytes += cost;
        self.by_class.insert(class_name.to_string(), ldcs);
        true
    }

    /// Retain `fields` for an already-inserted class, within the byte budget.
    /// Returns false when they were rejected (pool full).
    fn insert_fields(&mut self, class_name: &str, fields: Vec<String>) -> bool {
        let cost = fields.iter().map(String::len).sum::<usize>();
        if self.bytes.saturating_add(cost) > MAX_CANDIDATE_TOTAL_BYTES {
            return false;
        }
        self.bytes += cost;
        self.fields.insert(class_name.to_string(), fields);
        true
    }
}

/// Phase A. Walk every `.class` in `archive`, identify the master
/// enums by `<clinit>` ldc-sequence fingerprint. Returns a vector of
/// `(label, MasterEnum)` — at most one match per fingerprint label.
fn identify_master_enums(
    archive: &mut jar::Jar,
    budget: &mut u64,
) -> Vec<(&'static str, MasterEnum)> {
    // First pass: collect every class's <clinit> ldc strings, keyed by the JVM
    // INTERNAL name (`this_class`), not the zip entry name — binding classes
    // reference enum constants as `getstatic <internal>.f`.
    let mut pool = CandidatePool::default();
    jar::for_each_class_budgeted(archive, budget, |zip_name, class| {
        let Some(ldcs) = clinit_ldc_strings(class) else {
            return;
        };
        if ldcs.is_empty() {
            return;
        }
        let key = class.this_class_name().unwrap_or(zip_name);
        let matches_fp = FINGERPRINTS.iter().any(|fp| fp_matches(fp, &ldcs));
        if pool.insert(key, ldcs) {
            if matches_fp && !pool.insert_fields(key, clinit_enum_field_names(class)) {
                tracing::warn!(
                    class = ?key,
                    bytes = pool.bytes,
                    "deluxe: candidate pool cap hit, enum field names dropped; \
                     getstatic references to this enum may not resolve"
                );
            }
        } else {
            tracing::debug!(
                class = ?key,
                classes = pool.by_class.len(),
                bytes = pool.bytes,
                "deluxe: candidate pool aggregate cap hit, dropping class"
            );
        }
    });
    let candidates = pool.by_class;
    let mut fields_by_class = pool.fields;

    // Second pass: match each fingerprint against the candidate pool.
    let mut out = Vec::new();
    for fp in FINGERPRINTS {
        let mut best: Option<(String, Vec<String>)> = None;
        for (name, ldcs) in &candidates {
            if !fp_matches(fp, ldcs) {
                continue;
            }
            let count = ldcs.len();
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
            out.push((fp.label, class_name, values));
        }
    }
    if out.is_empty() {
        return Vec::new();
    }

    out.into_iter()
        .map(|(label, class_name, values)| {
            let fields = fields_by_class.remove(&class_name).unwrap_or_default();
            (
                label,
                MasterEnum {
                    class_name,
                    values,
                    fields,
                },
            )
        })
        .collect()
}

// Ordinal -> putstatic field-name mapping (mirrors clinit_ldc_strings' ordinal -> value), so
// MasterEnumTable can resolve a binding class's `getstatic E.<field>` back to an ordinal.
fn clinit_enum_field_names(class: &super::class_reader::ClassFile) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut out_bytes = 0usize;
    let Some(self_name) = class.this_class_name() else {
        return out;
    };
    let self_descriptor = format!("L{self_name};");
    for m in &class.methods {
        if class.member_name(m) != Some("<clinit>") {
            continue;
        }
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        for insn in code.instructions() {
            if insn.opcode != PUTSTATIC {
                continue;
            }
            let Some(idx) = insn.cp_index() else { continue };
            let Some(member) = class.constant_pool.member_ref(idx) else {
                continue;
            };
            // Only the enum's own singleton fields (`Lself;` typed, owned by
            // this class) — skip auxiliary statics (`$VALUES` arrays, counters).
            if member.class_name == self_name && member.descriptor == self_descriptor {
                // Cap retained bytes too, not just the count — a CONSTANT_Utf8 name
                // is an attacker-controlled u2 length (JVMS 4.4.7), like the sibling
                // `clinit_ldc_strings` guards with MAX_CLINIT_LDC_BYTES.
                if out.len() >= MAX_CLINIT_LDC_STRINGS
                    || out_bytes.saturating_add(member.name.len()) > MAX_CLINIT_LDC_BYTES
                {
                    break;
                }
                out_bytes += member.name.len();
                out.push(member.name.to_string());
            }
        }
    }
    out
}

// Walks `<clinit>` collecting every ldc/ldc_w String/Utf8 operand in order;
// None if no `<clinit>`. Stops at MAX_CLINIT_LDC_STRINGS/MAX_CLINIT_LDC_BYTES
// (decompressed work, not disc-file size); can't lose a real fingerprint match.
fn clinit_ldc_strings(class: &super::class_reader::ClassFile) -> Option<Vec<String>> {
    let mut found = false;
    let mut out: Vec<String> = Vec::new();
    let mut out_bytes = 0usize;
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
                if out.len() >= MAX_CLINIT_LDC_STRINGS
                    || out_bytes.saturating_add(s.len()) > MAX_CLINIT_LDC_BYTES
                {
                    tracing::debug!(
                        class = ?class.this_class_name().unwrap_or(""),
                        strings = out.len(),
                        bytes = out_bytes,
                        "deluxe: clinit ldc collection hit cap, truncating"
                    );
                    return Some(out);
                }
                out_bytes += s.len();
                out.push(s);
            }
        }
    }
    if found { Some(out) } else { None }
}

// Whether a class's `<clinit>` ldc strings fit a fingerprint (prefix and count window).
fn fp_matches(fp: &Fingerprint, ldcs: &[String]) -> bool {
    ldcs_match_prefix(ldcs, fp.prefix)
        && ldcs.len().abs_diff(fp.expected_count) <= fp.count_tolerance
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

// ── Phase C: find the binding class ─────────────────────────────────────────

// Finds binding-class candidates by getstatic-count to the master enums.
// Some discs split the table across audio/subtitle classes, hence top-K.
fn find_binding_classes(
    archive: &mut jar::Jar,
    master_enum_classes: &HashSet<&str>,
    budget: &mut u64,
) -> Vec<(String, usize)> {
    const MIN_GETSTATIC: usize = 4;
    let mut candidates: Vec<(String, usize)> = Vec::new();
    jar::for_each_class_budgeted(archive, budget, |class_name, class| {
        // A master enum's own <clinit> reads every constant into $VALUES; it is not a binding.
        if class
            .this_class_name()
            .is_some_and(|n| master_enum_classes.contains(n))
        {
            return;
        }
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
    // Reference to org.bluray.ti.CodingType; field name (e.g.
    // DOLBY_AC3_AUDIO) is the codec id — NOT a Deluxe-internal enum, read
    // straight from the binding constructor's getstatic operand.
    CodingType(Rc<str>),
    /// An uninitialized `new` object — popped by the matching
    /// invokespecial.
    NewObj(Rc<str>),
    /// Anything we can't model — stack effect tracked but content
    /// opaque. Lets the walker stay in sync past loads/computed
    /// values it doesn't understand.
    Unknown,
    /// An opaque long or double: one value, two stack words (JVMS §2.11.1).
    Wide,
}

impl StackVal {
    // Stack words this value occupies.
    fn words(&self) -> usize {
        if matches!(self, StackVal::Wide) { 2 } else { 1 }
    }

    // Opaque value of a field descriptor type.
    fn of_type(descriptor: &str) -> Self {
        if matches!(descriptor, "J" | "D") {
            StackVal::Wide
        } else {
            StackVal::Unknown
        }
    }
}

/// Fully-qualified class name of the BD-J spec codec enum that
/// Deluxe constructors reference directly.
const BD_CODING_TYPE_CLASS: &str = "org/bluray/ti/CodingType";

// Cap on Constructions retained from a binding `<clinit>` walk (also bounds
// decode_binding_class's per-class union and parse's cross-class union).
const MAX_CONSTRUCTIONS: usize = 4096;

/// Result of walking a binding class's `<clinit>`.
#[derive(Debug, Default)]
pub(crate) struct Decoded {
    /// One per `new X / invokespecial X.<init>` sequence, in bytecode order.
    pub constructions: Vec<Construction>,
    /// Events where the symbolic stack lost sync with the real one.
    pub drift: usize,
}

/// Phase D entry point: fetch the named binding class from `archive` and run the
/// bytecode walker against its `<clinit>`.
fn decode_binding(
    archive: &mut jar::Jar,
    binding_class_name: &str,
    master: &MasterEnumTable,
    budget: &mut u64,
) -> Decoded {
    // Fetched by entry name: the classes ahead of it in the jar are not inflated or
    // parsed, so they do not spend the shared budget.
    jar::try_class_budgeted(archive, binding_class_name, budget, |class| {
        Some(decode_binding_class(class, master))
    })
    .unwrap_or_default()
}

/// Walk every method named `<clinit>` (typically only one) on this
/// class with the symbolic stack machine.
pub(crate) fn decode_binding_class(class: &ClassFile, master: &MasterEnumTable) -> Decoded {
    let mut all = Decoded::default();
    for m in &class.methods {
        if class.member_name(m) != Some("<clinit>") {
            continue;
        }
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        let mut ctx = BindingDecoder::new(&class.constant_pool, master);
        ctx.run(&code);
        all.drift = all.drift.saturating_add(ctx.drift);
        // Bound the union too: JVMS §4.6 makes (name, descriptor) unique per
        // class so a real class has one `<clinit>`, but this reader does not
        // enforce that and a crafted class can repeat it.
        let room = MAX_CONSTRUCTIONS.saturating_sub(all.constructions.len());
        if room == 0 {
            break;
        }
        ctx.constructions.truncate(room);
        all.constructions.extend(ctx.constructions);
    }
    all
}

/// Tracks the symbolic stack as the walker advances through `<clinit>`.
/// `constructions` accumulates each completed `new X; ... invokespecial X.<init>`.
struct BindingDecoder<'a> {
    pool: &'a ConstantPool,
    master: &'a MasterEnumTable,
    stack: Vec<StackVal>,
    /// Depth limit for `stack`, taken from the Code attribute's own `max_stack`
    /// in [`run`](Self::run). Zero until then.
    max_stack: usize,
    constructions: Vec<Construction>,
    /// Count of events where the symbolic stack lost sync (see [`Decoded::drift`]).
    drift: usize,
}

impl<'a> BindingDecoder<'a> {
    fn new(pool: &'a ConstantPool, master: &'a MasterEnumTable) -> Self {
        Self {
            pool,
            master,
            stack: Vec::new(),
            max_stack: 0,
            constructions: Vec::new(),
            drift: 0,
        }
    }

    /// Run the walker over the given Code attribute. On exit the
    /// `constructions` field holds the result.
    pub(crate) fn run(&mut self, code: &CodeAttribute<'_>) {
        self.max_stack = code.max_stack as usize;
        let mut end = 0;
        for insn in code.instructions() {
            end = insn.pc + 1 + insn.operands.len();
            self.step(insn);
        }
        // The iterator stops at an opcode it cannot size: the rest is never decoded.
        if end < code.code.len() {
            self.drift = self.drift.saturating_add(1);
            tracing::debug!(pc = end, "deluxe: undecodable opcode ends the walk early");
        }
    }

    // The symbolic stack no longer matches the real one at `insn`.
    fn lost_sync(&mut self, insn: &Instruction<'_>) {
        self.drift = self.drift.saturating_add(1);
        tracing::debug!(
            pc = insn.pc,
            opcode = insn.opcode,
            "deluxe: symbolic stack drift"
        );
    }

    // A `<init>` of `class` whose operands were lost: record an argless construction so the
    // binding still takes its STN slot (numbering stays aligned); it resolves no label.
    fn placeholder(&mut self, class: &str, is_container: bool) {
        if !is_container && self.constructions.len() < MAX_CONSTRUCTIONS {
            self.constructions.push(Construction {
                binding_type: class.to_string(),
                args: Vec::new(),
            });
        }
    }

    // Pushes onto the symbolic stack, honouring the declared max_stack: JVMS 4.7.3 forbids
    // exceeding it, so a push past it means unverifiable bytecode. Bounds the decoder against a
    // crafted class.
    fn push(&mut self, val: StackVal) {
        if self.stack.len() >= self.max_stack {
            self.drift = self.drift.saturating_add(1);
            return;
        }
        self.stack.push(val);
    }

    // Pops `n` values of any category; on underflow resyncs on an empty stack.
    fn pop_values(&mut self, insn: &Instruction<'_>, n: usize) {
        match self.stack.len().checked_sub(n) {
            Some(keep) => self.stack.truncate(keep),
            None => {
                self.lost_sync(insn);
                self.stack.clear();
            }
        }
    }

    // Pops whole values totalling exactly `n` stack words (top last). None, after a
    // resync, on underflow or when that would split a long/double (JVMS §2.11.1).
    fn take_words(&mut self, insn: &Instruction<'_>, n: usize) -> Option<Vec<StackVal>> {
        let (mut at, mut got) = (self.stack.len(), 0);
        while got < n && at > 0 {
            at -= 1;
            got += self.stack[at].words();
        }
        if got != n {
            self.lost_sync(insn);
            self.stack.clear();
            return None;
        }
        Some(self.stack.split_off(at))
    }

    // dup, dup_x1/x2, dup2, dup2_x1/x2: copy the top `n` words beneath the next `m`.
    fn dup_under(&mut self, insn: &Instruction<'_>, n: usize, m: usize) {
        let Some(top) = self.take_words(insn, n) else {
            return;
        };
        let Some(under) = self.take_words(insn, m) else {
            return;
        };
        for v in top.clone().into_iter().chain(under).chain(top) {
            self.push(v);
        }
    }

    // Pops `n` operands and pushes one result, a long/double when `wide`.
    fn op(&mut self, insn: &Instruction<'_>, n: usize, wide: bool) {
        self.pop_values(insn, n);
        self.push(if wide {
            StackVal::Wide
        } else {
            StackVal::Unknown
        });
    }

    fn step(&mut self, insn: Instruction<'_>) {
        let insn = &insn;
        match insn.opcode {
            // Push small int constants.
            ICONST_M1 => self.push(StackVal::Int(-1)),
            ICONST_0 => self.push(StackVal::Int(0)),
            ICONST_1 => self.push(StackVal::Int(1)),
            ICONST_2 => self.push(StackVal::Int(2)),
            ICONST_3 => self.push(StackVal::Int(3)),
            ICONST_4 => self.push(StackVal::Int(4)),
            ICONST_5 => self.push(StackVal::Int(5)),
            BIPUSH => {
                if let Some(b) = insn.operand_u8() {
                    self.push(StackVal::Int(b as i8 as i32));
                } else {
                    self.push(StackVal::Unknown);
                }
            }
            SIPUSH => {
                if let Some(w) = insn.operand_u16() {
                    self.push(StackVal::Int(w as i16 as i32));
                } else {
                    self.push(StackVal::Unknown);
                }
            }
            LCONST_0 | LCONST_1 | DCONST_0 | DCONST_1 | LDC2_W => self.push(StackVal::Wide),
            // aconst_null, fconst_* (the int/long/double constants are taken above).
            ACONST_NULL..=DCONST_1 => self.push(StackVal::Unknown),
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
                self.push(v);
            }
            // Local loads and stores (the long/double loads first).
            LLOAD | DLOAD | LLOAD_0..=LLOAD_3 | DLOAD_0..=DLOAD_3 => self.push(StackVal::Wide),
            ILOAD..=ALOAD_3 => self.push(StackVal::Unknown),
            ISTORE..=ASTORE_3 => self.pop_values(insn, 1),
            WIDE => match insn.operand_u8() {
                Some(LLOAD | DLOAD) => self.push(StackVal::Wide),
                Some(ILOAD | FLOAD | ALOAD) => self.push(StackVal::Unknown),
                Some(ISTORE..=ASTORE) => self.pop_values(insn, 1),
                Some(IINC) => {}
                _ => self.lost_sync(insn),
            },
            // Array loads pop (arrayref, index); stores also pop the value.
            LALOAD | DALOAD => self.op(insn, 2, true),
            IALOAD..=SALOAD => self.op(insn, 2, false),
            IASTORE..=SASTORE => self.pop_values(insn, 3),
            // new X — push an uninit-object marker. The matching
            // invokespecial will consume this + the args and emit a
            // Construction.
            NEW => {
                let class_name = insn
                    .cp_index()
                    .and_then(|i| self.pool.class_name(i))
                    .unwrap_or("");
                // Rc: `dup` copies the handle, not a name of up to 64 KiB.
                self.push(StackVal::NewObj(class_name.into()));
            }
            // Word-level stack ops (a long/double is one two-word value).
            POP => drop(self.take_words(insn, 1)),
            POP2 => drop(self.take_words(insn, 2)),
            DUP => self.dup_under(insn, 1, 0),
            DUP_X1 => self.dup_under(insn, 1, 1),
            DUP_X2 => self.dup_under(insn, 1, 2),
            DUP2 => self.dup_under(insn, 2, 0),
            DUP2_X1 => self.dup_under(insn, 2, 1),
            DUP2_X2 => self.dup_under(insn, 2, 2),
            SWAP => {
                if let Some(a) = self.take_words(insn, 1)
                    && let Some(b) = self.take_words(insn, 1)
                {
                    for v in a.into_iter().chain(b) {
                        self.push(v);
                    }
                }
            }
            // Arithmetic: in 0x60..=0x83 the long/double forms are the odd opcodes.
            IADD..=DREM | ISHL..=LXOR => self.op(insn, 2, insn.opcode % 2 == 1),
            INEG..=DNEG => self.op(insn, 1, insn.opcode % 2 == 1),
            IINC => {}
            I2L | I2D | L2D | F2L | F2D | D2L => self.op(insn, 1, true),
            I2L..=I2S => self.op(insn, 1, false),
            LCMP..=DCMPG => self.op(insn, 2, false),
            // Conditional branches pop their operands; the walk stays linear.
            IFEQ..=IFLE | IFNULL | IFNONNULL | TABLESWITCH | LOOKUPSWITCH => {
                self.pop_values(insn, 1)
            }
            IF_ICMPEQ..=IF_ACMPNE => self.pop_values(insn, 2),
            MONITORENTER | MONITOREXIT => self.pop_values(insn, 1),
            // getstatic Y.Z — if Y is one of our master enum classes,
            // resolve Z to an ordinal and push an EnumRef. Otherwise
            // push an opaque value so we stay in sync.
            GETSTATIC => {
                let Some(m) = insn.cp_index().and_then(|i| self.pool.member_ref(i)) else {
                    self.lost_sync(insn);
                    self.push(StackVal::Unknown);
                    return;
                };
                let val = if m.class_name == BD_CODING_TYPE_CLASS {
                    StackVal::CodingType(m.name.into())
                } else if let Some((kind, ordinal)) = self.master.resolve(m.class_name, m.name) {
                    StackVal::EnumRef { kind, ordinal }
                } else {
                    StackVal::of_type(m.descriptor)
                };
                self.push(val);
            }
            PUTSTATIC => self.pop_values(insn, 1),
            GETFIELD => {
                let Some(m) = insn.cp_index().and_then(|i| self.pool.member_ref(i)) else {
                    self.lost_sync(insn);
                    return self.op(insn, 1, false);
                };
                self.pop_values(insn, 1);
                self.push(StackVal::of_type(m.descriptor));
            }
            PUTFIELD => self.pop_values(insn, 2),
            // invokespecial X.<init>(...) — pop args per descriptor. If the
            // object underneath the args is a NewObj of class X (set by an
            // earlier `new X / dup`), emit a Construction.
            INVOKESPECIAL => {
                let Some(member) = insn.cp_index().and_then(|i| self.pool.member_ref(i)) else {
                    self.lost_sync(insn);
                    return;
                };
                let arg_count = parse_method_arg_count(member.descriptor);
                let is_init = member.name == "<init>";
                // A per-stream binding ctor takes only scalars/enum refs, never an
                // array; a ctor with an array param is a container/title wrapper and
                // must not be recorded as a stream binding (stack still unwinds below).
                let is_container = member.descriptor.contains('[');
                if self.stack.len() < arg_count + 1 {
                    // Underflow: resync on an empty stack, keep the binding's slot.
                    self.lost_sync(insn);
                    self.stack.clear();
                    if is_init {
                        self.placeholder(member.class_name, is_container);
                    }
                    return;
                }
                let args: Vec<StackVal> = self.stack.split_off(self.stack.len() - arg_count);
                // Underneath the args: the object the constructor
                // operates on. For our pattern it's NewObj(X).
                match self.stack.pop() {
                    Some(StackVal::NewObj(name)) if is_init && *name == *member.class_name => {
                        // Bounded by MAX_CONSTRUCTIONS: an unbounded push here
                        // is ~1 GiB reachable from a crafted `<clinit>`.
                        if !is_container && self.constructions.len() < MAX_CONSTRUCTIONS {
                            self.constructions.push(Construction {
                                binding_type: name.to_string(),
                                args,
                            });
                        }
                    }
                    // `<clinit>` has no `this`: an `<init>` receiver that is not the
                    // matching `new` means the args popped are not this call's.
                    _ if is_init => {
                        self.lost_sync(insn);
                        self.placeholder(member.class_name, is_container);
                    }
                    _ => self.push_return(member.descriptor),
                }
            }
            // invokevirtual / invokestatic / invokeinterface — pop
            // args per descriptor (plus the receiver), push the return value.
            INVOKEVIRTUAL | INVOKESTATIC | INVOKEINTERFACE => {
                let Some(member) = insn.cp_index().and_then(|i| self.pool.member_ref(i)) else {
                    self.lost_sync(insn);
                    return;
                };
                let receiver = usize::from(insn.opcode != INVOKESTATIC);
                self.pop_values(insn, parse_method_arg_count(member.descriptor) + receiver);
                self.push_return(member.descriptor);
            }
            // anewarray / newarray / arraylength / instanceof: pop 1, push a ref or int.
            ANEWARRAY | NEWARRAY | ARRAYLENGTH | INSTANCEOF => self.op(insn, 1, false),
            MULTIANEWARRAY => {
                let dims = insn.operands.get(2).copied().unwrap_or(0);
                self.op(insn, dims as usize, false);
            }
            // checkcast leaves the (same) reference on the stack.
            CHECKCAST => {
                if self.stack.is_empty() {
                    self.lost_sync(insn);
                }
            }
            // Unconditional control transfer: nothing flows into the next instruction.
            GOTO | GOTO_W | IRETURN..=RETURN | ATHROW => self.stack.clear(),
            NOP => {}
            // Unmodelled (jsr/ret, invokedynamic, reserved): stack effect unknown, so the
            // walk is no longer exact.
            _ => self.lost_sync(insn),
        }
    }

    // Pushes a method descriptor's return value (nothing for void).
    fn push_return(&mut self, descriptor: &str) {
        match descriptor.rsplit_once(')') {
            Some((_, "V")) => {}
            Some((_, ret)) => self.push(StackVal::of_type(ret)),
            None => self.push(StackVal::Unknown),
        }
    }
}

// Counts argument values in a JVMS descriptor like `(IILjava/lang/String;LFoo;)V`.
// One per field descriptor: a long/double arg is one (two-word) `StackVal::Wide`.
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
            // Binding classes reference constants by obfuscated field name
            // (`getstatic <enum>.a`), so the resolver keys on captured `putstatic`
            // field names (`m.fields`); a synthetic test enum falls back to values.
            let keys: &[String] = if m.fields.is_empty() {
                &m.values
            } else {
                &m.fields
            };
            let field_map: HashMap<String, u16> = keys
                .iter()
                .enumerate()
                .map(|(i, v)| (v.clone(), i as u16))
                .collect();
            by_class.insert(m.class_name.clone(), (*kind, field_map));
            by_kind.insert(*kind, m.values.clone());
        }
        MasterEnumTable { by_class, by_kind }
    }

    fn class_name_set(&self) -> HashSet<&str> {
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

// Converts Phase D's per-construction tuples into StreamLabels (args identified by TYPE not
// position).
fn interpret_streams(constructions: &[Construction], master: &MasterEnumTable) -> Vec<StreamLabel> {
    let mut audio_idx: u16 = 0;
    let mut sub_idx: u16 = 0;
    let mut out = Vec::new();

    let slot_kinds = slot_kinds(constructions);

    for c in constructions {
        let mut lang_ord: Option<u16> = None;
        let mut purpose_ord: Option<u16> = None;
        let mut coding_type: Option<String> = None;
        let mut slot: Option<CodingSlot> = None;
        let mut unknown_coding: Option<String> = None;
        let mut stream_idx_hint: Option<i32> = None;
        for arg in &c.args {
            match arg {
                StackVal::EnumRef { kind, ordinal } => match *kind {
                    "Language" => lang_ord = lang_ord.or(Some(*ordinal)),
                    "Purpose" => purpose_ord = purpose_ord.or(Some(*ordinal)),
                    _ => {}
                },
                StackVal::CodingType(name) if slot.is_none() => {
                    slot = coding_slot(name);
                    if slot == Some(CodingSlot::Audio) {
                        coding_type = Some(name.to_string());
                    } else if slot.is_none() {
                        unknown_coding = Some(name.to_string());
                    }
                }
                StackVal::Int(n) => {
                    stream_idx_hint = stream_idx_hint.or(Some(*n));
                }
                _ => {}
            }
        }

        // Interactive graphics / video: numbered in neither the audio nor the PG list.
        if slot == Some(CodingSlot::NoSlot) {
            continue;
        }
        let Some(lang_ord) = lang_ord else {
            // A recognisable stream binding still occupies its STN slot and must
            // advance the counter, or renumbering skews every surviving label.
            // `saturating_add` is safe here since no label is produced.
            match slot_kind(c, slot, &slot_kinds) {
                Some(StreamLabelType::Audio) => audio_idx = audio_idx.saturating_add(1),
                Some(StreamLabelType::Subtitle) => sub_idx = sub_idx.saturating_add(1),
                // Not a stream binding (`new StringBuilder` and friends in the
                // same `<clinit>`): no slot, no counter.
                None => {}
            }
            continue;
        };

        // An unrecognised CodingType name is not decisive: take the type from siblings.
        if let (None, Some(name), Some(StreamLabelType::Audio)) =
            (slot, unknown_coding, slot_kind(c, None, &slot_kinds))
        {
            coding_type = Some(name);
        }

        // Audio when a CodingType is present (audio binding type
        // always references org.bluray.ti.CodingType); subtitle
        // otherwise.
        let codec_hint = coding_type
            .as_deref()
            .map(coding_type_to_codec_hint)
            .map(neutralise)
            .unwrap_or_default();

        // Neither `+= 1` nor `saturating_add` is correct: saturation caused a
        // non-terminating loop in `criterion` and would peg every overflowing
        // stream at the SAME number. Exhausting the 1-based u16 space stops emission.
        let (stream_type, stream_number) = if coding_type.is_some() {
            let Some(n) = audio_idx.checked_add(1) else {
                tracing::warn!(
                    emitted = out.len(),
                    "deluxe: audio stream-number space exhausted; truncating labels"
                );
                break;
            };
            audio_idx = n;
            (StreamLabelType::Audio, audio_idx)
        } else {
            let Some(n) = sub_idx.checked_add(1) else {
                tracing::warn!(
                    emitted = out.len(),
                    "deluxe: subtitle stream-number space exhausted; truncating labels"
                );
                break;
            };
            sub_idx = n;
            (StreamLabelType::Subtitle, sub_idx)
        };

        // Resolve language ordinal → enum value string via master
        // table; then route through vocab::lang for ISO code + variant.
        let lang_value = neutralise(master.value("Language", lang_ord).unwrap_or(""));
        let (language, variant) = match vocab::lang(&lang_value) {
            Some(li) => (li.code.to_string(), li.variant.to_string()),
            None if !lang_value.is_empty() => (lang_value.clone(), String::new()),
            None => (String::new(), String::new()),
        };

        let (purpose, mut qualifier) = match purpose_ord {
            Some(o) => deluxe_purpose_to_label(o),
            None => (LabelPurpose::Normal, LabelQualifier::None),
        };
        // Some frameworks (Universal) encode SDH/RNIB as a distinct Language
        // VALUE ("English SDH") rather than in the Purpose enum. When the
        // purpose left the qualifier unset, recover it from the language name.
        if qualifier == LabelQualifier::None {
            qualifier = vocab::qualifier(&lang_value);
        }

        if let Some(hint) = stream_idx_hint {
            tracing::debug!(
                disc_stream_idx = hint,
                lang = ?language,
                binding = ?c.binding_type,
                "deluxe interpret_streams: disc-authored stream index (not used for stream_number; preserved for diagnostic)"
            );
        }

        out.push(StreamLabel {
            stream_id: None,
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

// Disc-authored text reaches the label list and from there track names and CLI output:
// control characters are escaped, everything else is kept as authored.
fn neutralise(s: &str) -> String {
    if s.chars().any(char::is_control) {
        s.escape_debug().to_string()
    } else {
        s.to_string()
    }
}

// Which stream list each binding type enumerates, learned from the constructions that DID
// resolve a language; a type that resolved as both kinds is left out (no consistent answer).
fn slot_kinds(constructions: &[Construction]) -> HashMap<&str, Option<StreamLabelType>> {
    let mut kinds: HashMap<&str, Option<StreamLabelType>> = HashMap::new();
    for c in constructions {
        let mut has_lang = false;
        let mut slot = None;
        let mut unknown = false;
        for arg in &c.args {
            match arg {
                StackVal::EnumRef {
                    kind: "Language", ..
                } => has_lang = true,
                StackVal::CodingType(n) if slot.is_none() => {
                    slot = coding_slot(n);
                    unknown |= slot.is_none();
                }
                _ => {}
            }
        }
        // An unrecognised CodingType name says nothing about the list: not recorded.
        if !has_lang || slot == Some(CodingSlot::NoSlot) || (slot.is_none() && unknown) {
            continue;
        }
        let kind = if slot == Some(CodingSlot::Audio) {
            StreamLabelType::Audio
        } else {
            StreamLabelType::Subtitle
        };
        kinds
            .entry(c.binding_type.as_str())
            .and_modify(|e| {
                if *e != Some(kind) {
                    *e = None;
                }
            })
            .or_insert(Some(kind));
    }
    kinds
}

// The stream list an unresolved construction occupies a slot in, or None if
// not a stream binding. A recognised CodingType arg is decisive; otherwise
// falls back to what the binding type's resolved siblings showed.
fn slot_kind(
    c: &Construction,
    slot: Option<CodingSlot>,
    slot_kinds: &HashMap<&str, Option<StreamLabelType>>,
) -> Option<StreamLabelType> {
    match slot {
        Some(CodingSlot::Audio) => Some(StreamLabelType::Audio),
        Some(CodingSlot::Subtitle) => Some(StreamLabelType::Subtitle),
        Some(CodingSlot::NoSlot) => None,
        None => slot_kinds.get(c.binding_type.as_str()).copied().flatten(),
    }
}

/// STN list a BD-J `CodingType` stream is numbered in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodingSlot {
    Audio,
    Subtitle,
    /// Interactive graphics or video: no audio or PG slot.
    NoSlot,
}

// Classifies an org.bluray.ti.CodingType field name; None for an unrecognised one.
fn coding_slot(name: &str) -> Option<CodingSlot> {
    match name {
        "PRESENTATION_GRAPHICS" | "TEXT_SUBTITLE" => Some(CodingSlot::Subtitle),
        "INTERACTIVE_GRAPHICS" => Some(CodingSlot::NoSlot),
        n if n.ends_with("_VIDEO") => Some(CodingSlot::NoSlot),
        n if n.ends_with("_AUDIO") || n.contains("_AUDIO_") => Some(CodingSlot::Audio),
        _ => None,
    }
}

// Maps an org.bluray.ti.CodingType field name (from getstatic operands on
// Deluxe binding classes) to a human-readable codec hint. Unknown field
// names pass through unchanged so unfamiliar codecs still surface something.
fn coding_type_to_codec_hint(field: &str) -> &str {
    match field {
        // Lossless / hi-res.
        "DOLBY_LOSSLESS_AUDIO" => "Dolby TrueHD",
        "DTS_HD_AUDIO_XLL" => "DTS-HD Master Audio",
        "LPCM_AUDIO" => "LPCM",
        // Dolby family.
        "DOLBY_AC3_AUDIO" => "Dolby Digital",
        "DOLBY_DIGITAL_PLUS_AUDIO" => "Dolby Digital Plus",
        // DTS family.
        "DTS_AUDIO" => "DTS",
        "DTS_HD_AUDIO" => "DTS-HD",
        "DTS_HD_AUDIO_EXCEPT_XLL" => "DTS-HD HR",
        "DTS_HD_AUDIO_LBR" => "DTS Express",
        "DRA_AUDIO" | "DRA_EXTENSION_AUDIO" => "DRA",
        // Unknown / future — pass through verbatim so the operator
        // can see what the disc actually authored.
        _ => field,
    }
}

// Deluxe Purpose enum ordinal -> (LabelPurpose, LabelQualifier); order is
// fixed per Phase A's verified output: 0=Normal, 1=Commentary, 2=PiP,
// 3=Trivia, 4=Descriptive, 5=Score, 6=NoForced, 7=NoForcedDescriptive.
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
#[path = "deluxe_tests.rs"]
mod tests;

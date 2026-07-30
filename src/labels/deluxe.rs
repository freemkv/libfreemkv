//! Deluxe BD-J framework — `com/bydeluxe/bluray/` package signature.
//!
//! Detected on discs whose `/BDMV/JAR/<x>.jar` contains a
//! `com/bydeluxe/` directory entry.
//!
//! ## What this parser reads
//!
//! Deluxe-authored discs store stream labels as ordinal references into
//! enum classes whose names are obfuscated per-disc, so a name-based
//! match won't work. The label data is instead recovered by matching on
//! the **shape of each enum's `<clinit>`**, which is framework-stable:
//!
//! | Enum | Signature |
//! |---|---|
//! | Language | 70 `ldc` operations in `<clinit>`, sequence starts `English, French, Spanish, Dutch, ...` |
//! | Purpose | 8 ldcs starting `Normal, Commentary, PiP, Trivia, ...` |
//! | VideoFormat | 7 ldcs starting `HD, HDR10 Plus, HD Dolby, ...` |
//! | Region | 22 ldcs starting `USA_D1, LIC1, LIC2, LIC3, ...` |
//! | Studio | 6 ldcs in `<clinit>` |
//!
//! Matching on the shape rather than the class name keeps the parser
//! working across obfuscation variants. Codec strings come from the
//! standard BD-J `org/bluray/ti/CodingType` enum referenced directly by
//! the binding constructors (see [`StackVal::CodingType`]), not from a
//! Deluxe-internal enum.
//!
//! ## Implementation phases
//!
//! - **Phase A** — master enum identification (`identify_master_enums`).
//!   Walks every `.class`'s `<clinit>` ldc sequence and matches against
//!   the framework-stable fingerprints. Output: `Vec<(label, MasterEnum)>`
//!   with full ordinal → string-value tables.
//!
//! - **Phase C** — binding-class identification (`find_binding_classes`).
//!   The per-stream table is built by some class via repeated
//!   `getstatic` references to the master enums identified in A.
//!   That class has the highest such `getstatic` count in the jar.
//!   Heuristic shape; precise threshold may need tuning.
//!
//! - **Phase D** — binding-class bytecode decoder (`decode_binding`).
//!   Walks the binding class's `<clinit>` with a tiny symbolic stack
//!   machine. For each `new X / dup / ... / invokespecial X.<init>`
//!   sequence, collects the int values and enum-reference operands
//!   between the `dup` and the constructor call, then emits a
//!   `DecodedStream`. The signal-to-StreamLabel mapping (which arg is
//!   stream index? which is language? audio vs subtitle?) uses a
//!   heuristic — see `interpret_streams` for the mapping rules.
//!
//! ## Confidence
//!
//! [`parse`] returns `Some(ParseResult::medium(labels))` when Phases A
//! through D produce at least one stream — `Medium` because the
//! signal-to-label mapping is heuristic. `None` when the disc isn't
//! Deluxe-authored or when decoding produces zero streams (a
//! recognized-but-broken state that the analyzer still surfaces via
//! `parsers_detected`).

use super::class_reader::{
    AASTORE, BIPUSH, ClassFile, CodeAttribute, ConstantPool, CpInfo, GETSTATIC, ICONST_0, ICONST_1,
    ICONST_2, ICONST_3, ICONST_4, ICONST_5, ICONST_M1, INVOKESPECIAL, LDC, LDC_W, NEW, SIPUSH,
};
use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::{HashMap, HashSet};

pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool {
    // The real signal is `com/bydeluxe/` inside a top-level jar's central
    // directory. With a reader in detect we check it directly (cheap
    // central-directory scan, no bytecode walk) so this parser claims only
    // Deluxe discs; `parse()` repeats the check.
    jar::for_each_jar(reader, udf, |_entry, archive| {
        jar::has_path_prefix(archive, "com/bydeluxe/").then_some(())
    })
    .is_some()
}

pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult> {
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
            // Cross-class union is bounded by the same cap as each walk.
            let room = MAX_CONSTRUCTIONS.saturating_sub(streams.len());
            if room == 0 {
                break;
            }
            let mut decoded = decode_binding(archive, name, &master_table);
            decoded.truncate(room);
            streams.extend(decoded);
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

/// Cap on the `ldc` operands retained per class by [`clinit_ldc_strings`].
///
/// Unlike every other count cap in this crate, the paired byte cap here cannot
/// be the disc-file size: a `.class` entry gated only by a `com/bydeluxe/` path
/// prefix deflates from ~100 KB up to the 64 MiB `MAX_CLASS_BYTES` read ceiling,
/// so ~33M two-byte `ldc` instructions — one retained `String` each — are
/// reachable from a small crafted disc. The bound has to be on the decompressed
/// work, so it is applied here.
///
/// Headroom: the largest framework-stable enum is `Language` at 70 values
/// (`FINGERPRINTS`), and no fingerprint matches a count more than
/// `LDC_COUNT_TOLERANCE` away from its expected size, so anything past ~74 can
/// never identify a master enum. 4096 leaves ~55x headroom over the largest
/// real enum for framework drift.
const MAX_CLINIT_LDC_STRINGS: usize = 4096;

/// Companion byte cap for [`MAX_CLINIT_LDC_STRINGS`]: the count cap alone still
/// admits 4096 x 64 KiB of `Utf8` (a JVMS `CONSTANT_Utf8_info` length is a u16),
/// i.e. ~268 MB per class from repeated `ldc` of one huge constant.
///
/// Headroom: master-enum values are short display names ("English",
/// "HDR10 Plus", "USA_D1") well under 32 bytes, so a real Language enum retains
/// ~1 KB. 256 KiB admits 4096 values averaging 64 bytes each.
const MAX_CLINIT_LDC_BYTES: usize = 256 * 1024;

/// Aggregate companion to [`MAX_CLINIT_LDC_BYTES`], which bounds retention PER
/// CLASS only. `identify_master_enums` holds every class's retained strings in
/// one map SIMULTANEOUSLY, so the per-class cap alone still admits
/// `classes x 256 KiB`: a 64 MiB jar of minimal `.class` entries reaches tens
/// of GiB. This is the same bound one level up.
///
/// Headroom: the five `FINGERPRINTS` enums together hold ~113 short values
/// (~1.2 KB). Every other class in a real BD-J jar contributes only whatever
/// string constants its own `<clinit>` loads — resource paths, config keys —
/// so a large authored jar lands in the low hundreds of KB. 16 MiB leaves
/// ~40x headroom over a deliberately generous 400 KB estimate for real media.
const MAX_CANDIDATE_TOTAL_BYTES: usize = 16 * 1024 * 1024;

/// Entry-count companion to [`MAX_CANDIDATE_TOTAL_BYTES`]. The byte budget
/// alone still admits ~16M map entries when every class retains a single
/// one-byte `ldc`, and the per-entry `HashMap` + `String` overhead is not
/// counted by that budget.
///
/// Headroom: a large retail BD-J title ships on the order of 1-3k classes,
/// and only those with a non-empty `<clinit>` ldc sequence become candidates.
/// 65536 leaves >20x headroom over the class count of any real jar.
const MAX_CANDIDATE_CLASSES: usize = 65536;

/// The Phase A candidate pool: every class's retained `<clinit>` ldc strings,
/// bounded in aggregate by [`MAX_CANDIDATE_TOTAL_BYTES`] and
/// [`MAX_CANDIDATE_CLASSES`].
#[derive(Default)]
struct CandidatePool {
    by_class: HashMap<String, Vec<String>>,
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
}

/// Phase A. Walk every `.class` in `archive`, identify the master
/// enums by `<clinit>` ldc-sequence fingerprint. Returns a vector of
/// `(label, MasterEnum)` — at most one match per fingerprint label.
pub(crate) fn identify_master_enums(archive: &mut jar::Jar) -> Vec<(&'static str, MasterEnum)> {
    // First pass: collect every class's <clinit> ldc string sequence.
    let mut pool = CandidatePool::default();
    jar::for_each_class(archive, |class_name, class| {
        let Some(ldcs) = clinit_ldc_strings(class) else {
            return;
        };
        if ldcs.is_empty() {
            return;
        }
        if !pool.insert(class_name, ldcs) {
            tracing::debug!(
                class = class_name,
                classes = pool.by_class.len(),
                bytes = pool.bytes,
                "deluxe: candidate pool aggregate cap hit, dropping class"
            );
        }
    });
    let candidates = pool.by_class;

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
///
/// Collection stops at [`MAX_CLINIT_LDC_STRINGS`] operands or
/// [`MAX_CLINIT_LDC_BYTES`] of retained text, whichever comes first: the walk
/// is driven by the DECOMPRESSED class, so it is not bounded by the disc-file
/// size cap the way the rest of this module's counts are. Truncation cannot
/// lose a real match — a truncated sequence is far longer than any
/// `FINGERPRINTS` entry's `expected_count + LDC_COUNT_TOLERANCE`, so it would
/// have been rejected on count anyway.
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
                        class = class.this_class_name().unwrap_or(""),
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
/// A disc that splits the table commonly has one audio binding class
/// with the most getstatic refs and a subtitle binding class with
/// somewhat fewer; both share the master Language + Purpose enums.
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
    /// enum; the codec source is the standard BD-J API `CodingType`
    /// enum, so the binding constructor's codec arg is read straight
    /// from that getstatic operand.
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

/// Cap on `Construction`s retained from a binding `<clinit>` walk.
///
/// One entry is appended per matched `new X / dup / invokespecial X.<init>`
/// with no other bound: a `.class` gated only by a `com/bydeluxe/` path prefix
/// deflates to the 64 MiB `MAX_CLASS_BYTES` ceiling, and the shortest matching
/// sequence is a handful of bytes, so millions of `Construction`s — each a
/// `String` plus an arg `Vec` — are reachable from a small crafted disc
/// (~1 GiB). The same cap bounds the per-class union in
/// [`decode_binding_class`] (a crafted class may repeat `<clinit>`, which JVMS
/// §4.6 forbids but this reader tolerates) and the cross-class union in
/// [`parse`], so the whole phase retains at most this many.
///
/// Headroom: the BD STN_table (BDAV, `STN_table` stream-entry counts) admits
/// at most 32 primary audio and 32 PG streams per playlist, and a Deluxe
/// binding table covers the disc's playlists — low hundreds of entries on the
/// largest retail titles. 4096 leaves >20x headroom.
const MAX_CONSTRUCTIONS: usize = 4096;

/// Phase D entry point: find the binding class in `archive`, run the
/// bytecode walker against its `<clinit>`, return one `Construction`
/// per `new X / invokespecial X.<init>` sequence.
pub(crate) fn decode_binding(
    archive: &mut jar::Jar,
    binding_class_name: &str,
    master: &MasterEnumTable,
) -> Vec<Construction> {
    let target_name = binding_class_name.to_string();
    // Short-circuit on the name match: try_each_class stops iterating
    // (and stops decompressing/parsing remaining .class entries) as soon
    // as the closure returns Some, instead of walking the whole jar past
    // the target.
    jar::try_each_class(archive, |class_name, class| {
        if class_name != target_name {
            return None;
        }
        Some(decode_binding_class(class, master))
    })
    .unwrap_or_default()
}

/// Walk every method named `<clinit>` (typically only one) on this
/// class with the symbolic stack machine. Returns each construction
/// emitted.
pub(crate) fn decode_binding_class(
    class: &ClassFile,
    master: &MasterEnumTable,
) -> Vec<Construction> {
    let mut all: Vec<Construction> = Vec::new();
    for m in &class.methods {
        if class.member_name(m) != Some("<clinit>") {
            continue;
        }
        let Some(code) = m.code(&class.constant_pool) else {
            continue;
        };
        let mut ctx = BindingDecoder::new(&class.constant_pool, master);
        ctx.run(&code);
        // Bound the union too: JVMS §4.6 makes (name, descriptor) unique per
        // class so a real class has one `<clinit>`, but this reader does not
        // enforce that and a crafted class can repeat it.
        let room = MAX_CONSTRUCTIONS.saturating_sub(all.len());
        if room == 0 {
            break;
        }
        ctx.constructions.truncate(room);
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
    /// Depth limit for `stack`, taken from the Code attribute's own `max_stack`
    /// in [`run`](Self::run). Zero until then.
    max_stack: usize,
    constructions: Vec<Construction>,
}

impl<'a> BindingDecoder<'a> {
    fn new(pool: &'a ConstantPool, master: &'a MasterEnumTable) -> Self {
        Self {
            pool,
            master,
            stack: Vec::new(),
            max_stack: 0,
            constructions: Vec::new(),
        }
    }

    /// Run the walker over the given Code attribute. On exit the
    /// `constructions` field holds the result.
    pub(crate) fn run(&mut self, code: &CodeAttribute<'_>) {
        self.max_stack = code.max_stack as usize;
        for insn in code.instructions() {
            self.step(insn);
        }
    }

    /// Push onto the symbolic stack, honouring the Code attribute's declared
    /// `max_stack`.
    ///
    /// JVMS 4.7.3 requires that a method's operand stack never exceed
    /// `max_stack` at any point, so a push past it can only come from bytecode
    /// that would fail JVM verification. Dropping it costs nothing on real
    /// bytecode and bounds the decoder: a `.class` gated only by a
    /// `com/bydeluxe/` path prefix deflates from ~100 KB to the 64 MiB
    /// `MAX_CLASS_BYTES` ceiling, i.e. ~67M single-byte `iconst_0` (~2 GiB of
    /// `StackVal`) on an unbounded `Vec`.
    ///
    /// Headroom: `max_stack` is exactly what javac computed for the real
    /// binding `<clinit>`, so no real construction can be clipped. Our symbolic
    /// stack counts `long`/`double` as one slot where the JVM counts two, so
    /// our depth is never greater than the verified depth.
    fn push(&mut self, val: StackVal) {
        if self.stack.len() >= self.max_stack {
            return;
        }
        self.stack.push(val);
    }

    fn step(&mut self, insn: super::class_reader::Instruction<'_>) {
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
            // new X — push an uninit-object marker. The matching
            // invokespecial will consume this + the args and emit a
            // Construction.
            NEW => {
                let class_name = insn
                    .cp_index()
                    .and_then(|i| self.pool.class_name(i))
                    .unwrap_or("")
                    .to_string();
                self.push(StackVal::NewObj(class_name));
            }
            // dup — duplicate top of stack.
            0x59 /* dup */ => {
                if let Some(top) = self.stack.last().cloned() {
                    self.push(top);
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
                self.push(val);
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
                if let StackVal::NewObj(name) = receiver
                    && name == member.class_name {
                        // Bounded by MAX_CONSTRUCTIONS: an unbounded push here
                        // is ~1 GiB reachable from a crafted `<clinit>`.
                        if self.constructions.len() >= MAX_CONSTRUCTIONS {
                            return;
                        }
                        self.constructions.push(Construction {
                            binding_type: name,
                            args,
                        });
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
                    self.push(StackVal::Unknown);
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
/// [`StreamLabel`]s. Two binding-constructor shapes are handled:
///
/// 5-arg: `BindingType.<init>(I, Lang;, Lpurpose;, I, LCodingType;)V`
/// 4-arg: `BindingType.<init>(I, Lang;, Lpurpose;, LCodingType;)V`
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

        // Neither `+= 1` (panics in debug, wraps in release) nor
        // `saturating_add` is correct here. Saturation is what turned an
        // overflow guard into a non-terminating loop in `criterion`, and here
        // it would peg every stream past 65535 at the SAME number — silently
        // mislabelling tracks, since `apply_labels` binds on
        // `(type, stream_number)`. The 1-based u16 numbering space is a hard
        // ceiling, so exhausting it stops label emission instead.
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
/// CodingType is the standard BD-J API enum; values are documented in
/// the BD-J specification. Unknown field names pass through unchanged
/// so unfamiliar codecs still surface something rather than going
/// silent.
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

    // ── Decompression-amplification bounds ──────────────────────────────────

    /// Build a `ClassFile` whose single `<clinit>` has the given bytecode and
    /// `max_stack`, over the given constant pool. The pool must hold
    /// "<clinit>" at 1, "()V" at 2 and "Code" at 3.
    fn class_with_clinit(pool: ConstantPool, max_stack: u16, code: &[u8]) -> ClassFile {
        let mut info = Vec::with_capacity(8 + code.len());
        info.extend_from_slice(&max_stack.to_be_bytes());
        info.extend_from_slice(&0u16.to_be_bytes()); // max_locals
        info.extend_from_slice(&(code.len() as u32).to_be_bytes());
        info.extend_from_slice(code);
        ClassFile {
            minor_version: 0,
            major_version: 49,
            constant_pool: pool,
            access_flags: 0,
            this_class: 0,
            super_class: 0,
            interfaces: Vec::new(),
            fields: Vec::new(),
            methods: vec![super::super::class_reader::Member {
                access_flags: 0,
                name_index: 1,
                descriptor_index: 2,
                attributes: vec![super::super::class_reader::Attribute {
                    name_index: 3,
                    info,
                }],
            }],
            attributes: Vec::new(),
        }
    }

    /// Pool for `class_with_clinit`: 1 "<clinit>", 2 "()V", 3 "Code",
    /// 4 String -> 5, 5 Utf8(`value`).
    fn ldc_pool(value: &str) -> ConstantPool {
        ConstantPool::from_entries(vec![
            CpInfo::Empty,
            CpInfo::Utf8("<clinit>".into()),
            CpInfo::Utf8("()V".into()),
            CpInfo::Utf8("Code".into()),
            CpInfo::String { string_index: 5 },
            CpInfo::Utf8(value.into()),
        ])
    }

    #[test]
    fn clinit_ldc_string_count_is_capped() {
        // A `.class` gated only by a `com/bydeluxe/` path prefix deflates from
        // ~100 KB up to the 64 MiB MAX_CLASS_BYTES ceiling, giving ~33M 2-byte
        // `ldc` instructions. Every resolved operand is retained as an owned
        // String, and identify_master_enums keeps the whole vector per class in
        // a HashMap — so the allocation scales with the DECOMPRESSED size while
        // the only byte cap is on the compressed disc file.
        const N: usize = 200_000;
        let mut code = Vec::with_capacity(N * 2);
        for _ in 0..N {
            code.push(LDC);
            code.push(4); // cp index 4 -> String -> "English"
        }
        let class = class_with_clinit(ldc_pool("English"), 2, &code);
        let ldcs = clinit_ldc_strings(&class).expect("<clinit> present");
        // Asserted against a LITERAL, not against MAX_CLINIT_LDC_STRINGS. A test
        // that compares the result to the very constant under test passes
        // vacuously the moment someone raises that constant — which is the most
        // likely future regression here, and exactly the tautology class this
        // audit has now found six times. 8192 is double the current cap, so this
        // still allows the cap to be tuned, but not removed.
        assert!(
            ldcs.len() <= 8192,
            "retained {} ldc strings from {N} ldc instructions — the cap is not \
             bounding the walk",
            ldcs.len()
        );
        assert!(
            ldcs.len() < N,
            "nothing was truncated at all: retained all {N} strings"
        );
    }

    #[test]
    fn clinit_ldc_string_bytes_are_capped() {
        // Few instructions, huge operands: the count cap alone still admits
        // MAX_CLINIT_LDC_STRINGS x 64 KiB of Utf8. Bound the retained bytes too.
        let big = "A".repeat(32 * 1024);
        const N: usize = 512;
        let mut code = Vec::with_capacity(N * 2);
        for _ in 0..N {
            code.push(LDC);
            code.push(4);
        }
        let class = class_with_clinit(ldc_pool(&big), 2, &code);
        let ldcs = clinit_ldc_strings(&class).expect("<clinit> present");
        let bytes: usize = ldcs.iter().map(|s| s.len()).sum();
        // Literal, not the constant under test — see the sibling test above.
        assert!(
            bytes <= 512 * 1024,
            "retained {bytes} bytes of ldc strings — the byte cap is not bounding \
             the walk"
        );
    }

    #[test]
    fn clinit_ldc_strings_admits_largest_real_fingerprint() {
        // The biggest framework-stable enum is Language at 70 values; the cap
        // must not clip a real one.
        let n = FINGERPRINTS
            .iter()
            .map(|fp| fp.expected_count)
            .max()
            .unwrap()
            + LDC_COUNT_TOLERANCE;
        let mut code = Vec::with_capacity(n * 2);
        for _ in 0..n {
            code.push(LDC);
            code.push(4);
        }
        let class = class_with_clinit(ldc_pool("English"), 2, &code);
        let ldcs = clinit_ldc_strings(&class).expect("<clinit> present");
        assert_eq!(ldcs.len(), n, "real-size enum must survive the cap");
    }

    // ── Aggregate (cross-class) candidate-pool bounds ───────────────────────

    /// `MAX_CLINIT_LDC_BYTES` bounds retention PER CLASS; the candidate pool
    /// holds every class's strings at once, so without an aggregate a 64 MiB
    /// jar reaches tens of GiB.
    ///
    /// Fixture arithmetic (deliberately NOT expressed in terms of the constant
    /// under test — raising the constant must FAIL this test, not silently
    /// widen it): each entry costs a 5-byte class name plus a 65536-byte
    /// string = 65541 bytes. 65541 x 255 = 16 712 955 fits in the budget;
    /// 65541 x 256 = 16 778 496 does not, and the leftover 64 261 bytes admit
    /// no further entry. So exactly 255 of the 1024 offered entries are kept.
    #[test]
    fn candidate_pool_bounds_bytes_retained_across_classes() {
        let payload = "x".repeat(64 * 1024);
        let mut pool = CandidatePool::default();
        let mut accepted = 0usize;
        for i in 0..1024u32 {
            // Fixed-width 5-byte names so the cost per entry is uniform.
            if pool.insert(&format!("c{i:04}"), vec![payload.clone()]) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted, 255,
            "candidate pool retained {accepted} x 64 KiB classes — the \
             cross-class byte aggregate is not bounding retention"
        );
        assert_eq!(pool.by_class.len(), 255);
    }

    /// The byte budget alone still admits millions of map entries when each
    /// class retains one tiny string, and per-entry `HashMap`/`String`
    /// overhead is not charged against it. The entry-count cap binds there.
    ///
    /// Fixture: 1-byte payloads, so ~7 bytes per entry — the byte budget is
    /// nowhere near reached and the count cap is the only thing that can stop
    /// this at 65536.
    #[test]
    fn candidate_pool_bounds_entry_count_for_tiny_classes() {
        let mut pool = CandidatePool::default();
        let mut accepted = 0usize;
        for i in 0..70_000u32 {
            if pool.insert(&format!("c{i}"), vec!["x".to_string()]) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted, 65_536,
            "candidate pool retained {accepted} entries — the entry-count \
             aggregate is not bounding retention"
        );
    }

    /// Headroom check: a jar far larger than any real BD-J title (3000
    /// classes, 200 bytes of `<clinit>` strings each — the five master enums
    /// together are ~1.2 KB) must be retained in full. A cap that rejects real
    /// media is a defect in the other direction.
    #[test]
    fn candidate_pool_admits_a_generously_sized_real_jar() {
        let mut pool = CandidatePool::default();
        let mut accepted = 0usize;
        for i in 0..3000u32 {
            if pool.insert(&format!("com/bydeluxe/x{i}"), vec!["y".repeat(200)]) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted, 3000,
            "a 3000-class jar with 200 bytes of clinit strings per class must \
             not be truncated"
        );
    }

    // ── Construction accumulation bounds ────────────────────────────────────

    /// One `new X / dup / ... / invokespecial X.<init>` per 11 code bytes, so
    /// a 64 MiB decompressed class reaches ~6M `Construction`s (~1 GiB of
    /// `String` + arg `Vec`). Offer 5000 and exactly 4096 must be retained.
    ///
    /// 4096 is asserted as a literal, not as `MAX_CONSTRUCTIONS`: raising the
    /// constant must fail this test rather than pass vacuously.
    #[test]
    fn binding_decoder_construction_count_is_capped() {
        // new AudioSlot; dup; getstatic Lang.English; invokespecial <init>; pop
        let one: [u8; 11] = [
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
            0x57, // pop the leftover NewObj so the stack returns to empty
        ];
        let code: Vec<u8> = one.iter().copied().cycle().take(one.len() * 5000).collect();
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(
            decoder.constructions.len(),
            4096,
            "5000 constructions offered, {} retained — the accumulation is \
             not bounded",
            decoder.constructions.len()
        );
    }

    /// Headroom: the BD STN_table admits at most 32 primary audio + 32 PG
    /// streams per playlist, so even a disc binding several hundred stream
    /// slots must survive the cap untouched.
    #[test]
    fn binding_decoder_admits_a_large_real_binding_table() {
        let one: [u8; 11] = [NEW, 0, 8, 0x59, GETSTATIC, 0, 6, INVOKESPECIAL, 0, 12, 0x57];
        let code: Vec<u8> = one.iter().copied().cycle().take(one.len() * 512).collect();
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(
            decoder.constructions.len(),
            512,
            "a 512-slot binding table must not be clipped"
        );
    }

    /// `interpret_streams` numbers streams with a 1-based `u16` counter. An
    /// unguarded `+= 1` panics in debug and wraps in release past 65535;
    /// `saturating_add` would be worse still (every stream past the ceiling
    /// pegged to the SAME number, and `apply_labels` binds on
    /// `(type, stream_number)` — silent mislabelling). Emission must stop at
    /// the end of the numbering space instead.
    ///
    /// 65535 is the size of the 1-based u16 domain, not a tunable constant.
    #[test]
    fn interpret_streams_stops_at_the_u16_numbering_ceiling() {
        let master = lang_enum_master();
        let one = Construction {
            binding_type: "SubSlot".into(),
            args: vec![StackVal::EnumRef {
                kind: "Language",
                ordinal: 0,
            }],
        };
        // No CodingType arg => every construction is a subtitle stream.
        let constructions: Vec<Construction> = std::iter::repeat_n(one, 70_000).collect();
        let labels = interpret_streams(&constructions, &master);
        assert_eq!(
            labels.len(),
            65_535,
            "emitted {} labels from 70000 constructions — the 1-based u16 \
             stream-number space holds 65535",
            labels.len()
        );
        assert_eq!(labels[0].stream_number, 1);
        assert_eq!(labels[65_534].stream_number, 65_535);
    }

    #[test]
    fn binding_decoder_stack_is_bounded_by_max_stack() {
        // ~67M single-byte `iconst_0` fit in a 64 MiB decompressed class, and
        // each pushes a StackVal onto a Vec with no depth limit (~2 GiB). The
        // Code attribute's own max_stack is parsed and must be honoured: JVMS
        // 4.7.3 requires the operand stack never exceed it.
        const MAX_STACK: u16 = 4;
        let code = vec![ICONST_0; 200_000];
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: MAX_STACK,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert!(
            decoder.stack.len() <= MAX_STACK as usize,
            "symbolic stack grew to {} with max_stack {}",
            decoder.stack.len(),
            MAX_STACK
        );
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

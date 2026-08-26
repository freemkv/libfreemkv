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
//! [`parse`] returns `Some(ParseResult::high(labels))` when Phases A
//! through D produce at least one stream: the master enums matched their
//! framework-stable fingerprints (a strong ordered-prefix signature), the
//! binding class decoded, and at least one per-stream binding resolved to a
//! real (language, purpose, codec) tuple — the schema was fully recovered,
//! not guessed. `None` when the disc isn't Deluxe-authored or when decoding
//! produces zero streams (a recognized-but-broken state that the analyzer
//! still surfaces via `parsers_detected`).
//!
//! ## Confirmed studio variants
//!
//! - **Universal** (`studio="uni"`): Language enum `pd` (65 values,
//!   `English, French, Spanish, Dutch, …`), Purpose enum `lp`
//!   (`Normal, Commentary, PiP, Trivia, Descriptive, Score`), audio binding
//!   `np.<init>(I, Lpd;, Llp;, Lorg/bluray/ti/CodingType;)`, subtitle binding
//!   `wb.<init>(I, Lpd;, Llp;, Lmi;)`, all built in one binding class's
//!   `<clinit>` alongside a title-wrapper object whose constructor takes the
//!   per-stream arrays (filtered out by the array-parameter guard in the
//!   decoder). SDH/RNIB is encoded as a distinct Language VALUE
//!   ("English SDH", "English RNIB"), recovered into the qualifier from the
//!   name. Grounded on the F&F cluster (fixtures from *Fast Five*).
//! - **Disney/Warner**: 70-value Language enum, same binding shape; the
//!   original corpus this parser was written against.

use super::class_reader::{
    AASTORE, BIPUSH, ClassFile, CodeAttribute, ConstantPool, CpInfo, GETSTATIC, ICONST_0, ICONST_1,
    ICONST_2, ICONST_3, ICONST_4, ICONST_5, ICONST_M1, INVOKESPECIAL, LDC, LDC_W, NEW, PUTSTATIC,
    SIPUSH,
};
use super::{LabelPurpose, LabelQualifier, ParseResult, StreamLabel, StreamLabelType, jar, vocab};
use crate::sector::SectorSource;
use crate::udf::UdfFs;
use std::collections::{BTreeMap, HashMap, HashSet};

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
    // Per-studio dispatch signal. Deluxe authored the framework for several
    // studios (Universal `studio="uni"`, Fox, WB, Disney/Pixar); each ships a
    // `/BDMV/JAR/<n>/config.xml` naming the studio. The structural enum/binding
    // matching below is studio-agnostic, so this is currently informational
    // (logged, and available for future per-studio special-casing), not a gate.
    let studio = detect_studio(reader, udf);

    jar::for_each_jar(reader, udf, |entry_name, archive| {
        if !jar::has_path_prefix(archive, "com/bydeluxe/") {
            return None;
        }

        // Phase A — master enums (Language / Purpose / VideoFormat / Region / Studio).
        let enums = identify_master_enums(archive);
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

        // Phase C — find ALL binding-class candidates (audio + subtitle
        // are often split across two classes on Deluxe). Each gets its
        // own `<clinit>` walk; constructions union into a single
        // stream list for interpret_streams.
        let binding_classes = find_binding_classes(archive, &master_table.class_name_set());
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
        // High confidence: the master enums matched their framework-stable
        // fingerprints, the binding class decoded its `<clinit>`, and at least
        // one per-stream binding resolved to a real (language, purpose, codec)
        // tuple. Un-named STN slots are still back-filled from the MPLS floor
        // by the registry (`merge_mpls_floor`).
        Some(ParseResult::high(labels))
    })
}

/// Read the studio identifier from a Deluxe disc's `config.xml`
/// (`<TitleConfig studio="uni"…>`), if present. The file lives under a
/// numbered subdirectory of `/BDMV/JAR/` (e.g. `/BDMV/JAR/99999/config.xml`),
/// so this scans the subdirectories for the first one that carries it.
///
/// Returns the lowercased studio token (`"uni"`, `"fox"`, `"wb"`, …) or `None`
/// when no readable `config.xml` names a studio.
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

/// Extract the `studio="…"` attribute value from a `config.xml` byte buffer.
/// Deliberately a tolerant substring scan rather than a full XML parse: the
/// file is tiny attacker-controlled disc metadata and the only field of
/// interest is this one attribute. Rejects an empty or implausibly long value.
fn parse_studio_attr(xml: &[u8]) -> Option<String> {
    const MAX_STUDIO_LEN: usize = 32;
    let text = std::str::from_utf8(xml).ok()?;
    let anchor = text.find("studio")?;
    let after_key = &text[anchor + "studio".len()..];
    // Skip whitespace / '=' up to the opening quote.
    let open = after_key.find(['"', '\''])?;
    let quote = after_key.as_bytes()[open];
    let rest = &after_key[open + 1..];
    let close = rest.find(quote as char)?;
    let val = rest[..close].trim();
    if val.is_empty() || val.len() > MAX_STUDIO_LEN {
        return None;
    }
    Some(val.to_ascii_lowercase())
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
    /// Ordered, NOT a HashMap: `identify_master_enums` iterates this to pick a
    /// fingerprint's best match, and its tie-break only prefers an exact ldc
    /// count over an inexact one. Two candidates that are both inexact but
    /// within `LDC_COUNT_TOLERANCE` are therefore decided by iteration order —
    /// which for a `HashMap` is seeded per process, so the same disc could
    /// resolve a different master enum on a second run and emit different
    /// labels for unchanged input.
    by_class: BTreeMap<String, Vec<String>>,
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
    // First pass: collect every class's <clinit> ldc string sequence, keyed by
    // the class's JVM INTERNAL name (`this_class`), NOT the zip entry name.
    // The binding class references an enum constant as `getstatic <internal>.f`
    // (e.g. `pd`, or `com/foo/pd`), so the master enum has to be identified by
    // that same internal name or Phase C's getstatic count never matches it.
    // The zip entry name (`pd.class`) is a different namespace, used only to
    // locate a class for decoding.
    let mut pool = CandidatePool::default();
    jar::for_each_class(archive, |zip_name, class| {
        let Some(ldcs) = clinit_ldc_strings(class) else {
            return;
        };
        if ldcs.is_empty() {
            return;
        }
        let key = class.this_class_name().unwrap_or(zip_name);
        if !pool.insert(key, ldcs) {
            tracing::debug!(
                class = key,
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
            if count.abs_diff(fp.expected_count) > fp.count_tolerance {
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
            out.push((fp.label, class_name, values));
        }
    }
    if out.is_empty() {
        return Vec::new();
    }

    // Second pass: capture the obfuscated `putstatic` field names for the
    // classes we matched, so Phase D can resolve `getstatic <enum>.<field>`
    // to an ordinal. This is a targeted re-walk of only the matched enum
    // classes (at most one per fingerprint), not the whole jar again.
    let want: HashSet<&str> = out.iter().map(|(_, name, _)| name.as_str()).collect();
    let mut fields_by_class: HashMap<String, Vec<String>> = HashMap::new();
    jar::for_each_class(archive, |zip_name, class| {
        let iname = class.this_class_name().unwrap_or(zip_name);
        if want.contains(iname) && !fields_by_class.contains_key(iname) {
            fields_by_class.insert(iname.to_string(), clinit_enum_field_names(class));
        }
    });

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

/// Collect the static-field names an enum class's `<clinit>` stores its
/// own instances into, in declaration order — the ordinal → field-name
/// mapping that mirrors [`clinit_ldc_strings`]'s ordinal → value mapping.
///
/// An obfuscated Deluxe enum constant compiles to
/// `new E; dup; ldc "Value"; invokespecial E.<init>(…); putstatic E.<field>`,
/// so the `putstatic` whose owning class AND field descriptor are both this
/// class's own type names the field that ordinal. The binding class then
/// references that constant as `getstatic E.<field>`; without this mapping
/// the resolver in [`MasterEnumTable`] cannot turn the obfuscated field name
/// back into an ordinal.
///
/// Bounded by [`MAX_CLINIT_LDC_STRINGS`] like the value walk: a `.class`
/// gated only by a path prefix can inflate to the read ceiling, and each
/// retained field name is a heap `String`.
fn clinit_enum_field_names(class: &super::class_reader::ClassFile) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
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
                if out.len() >= MAX_CLINIT_LDC_STRINGS {
                    break;
                }
                out.push(member.name.to_string());
            }
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
                // A per-stream binding constructor takes only scalars and enum
                // references — never an array. A constructor with an array
                // parameter is a container/title wrapper (e.g. Universal's
                // title object `oq.<init>(…, [Lnp;, [Lwb;, [Loq;, [J)`, which
                // holds the per-stream arrays) and must NOT be recorded as a
                // stream binding: it carries a Language (the title's primary
                // language) but is not itself a stream, and emitting it would
                // land a spurious label in the subtitle list. The stack is
                // still unwound below so the following real bindings stay in
                // sync; only the `Construction` push is suppressed.
                let is_container = member.descriptor.contains('[');
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
                    && name == member.class_name
                    && !is_container {
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
            // anewarray / newarray — pop the count, push the array reference.
            // Modelled (rather than left to the `_` no-op) so the array
            // constructions inside a container's argument list keep the
            // symbolic stack aligned for the per-stream bindings built
            // alongside them.
            0xBD /* anewarray */ | 0xBC /* newarray */ => {
                self.stack.pop();
                self.push(StackVal::Unknown);
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
            // The binding class references enum constants by their obfuscated
            // static-field name (`getstatic <enum>.a`), so the resolver map is
            // keyed on the `putstatic` field names captured alongside the
            // values (`m.fields`), NOT on the value strings. A synthetic test
            // enum with no captured field names falls back to keying on the
            // values themselves, which is how those tests reference it.
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
/// - Neither, and its binding type never yielded a stream → not a
///   stream (skip). See [`slot_kind`] for why the binding type is
///   consulted rather than the language alone.
fn interpret_streams(constructions: &[Construction], master: &MasterEnumTable) -> Vec<StreamLabel> {
    let mut audio_idx: u16 = 0;
    let mut sub_idx: u16 = 0;
    let mut out = Vec::new();

    let slot_kinds = slot_kinds(constructions);

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

        let Some(lang_ord) = lang_ord else {
            // No language resolved. If the construction is still recognisably
            // a stream binding it OCCUPIES its STN slot and must advance the
            // counter — there is just nothing to label. Numbering only the
            // slots that resolve renumbers the rest 1..N and lands every
            // surviving label on the wrong stream.
            //
            // `saturating_add` is safe here where it would not be on the
            // emitting path below: no label is produced, so parking the
            // counter at `u16::MAX` binds nothing. The next slot that DOES
            // resolve hits the `checked_add` guard and stops emission.
            match slot_kind(c, coding_type.is_some(), &slot_kinds) {
                Some(StreamLabelType::Audio) => audio_idx = audio_idx.saturating_add(1),
                Some(StreamLabelType::Subtitle) => sub_idx = sub_idx.saturating_add(1),
                // Not a stream binding (`new StringBuilder` and friends in the
                // same `<clinit>`): no slot, no counter.
                None => {}
            }
            continue;
        };

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

        let (purpose, mut qualifier) = match purpose_ord {
            Some(o) => deluxe_purpose_to_label(o),
            None => (LabelPurpose::Normal, LabelQualifier::None),
        };
        // Some frameworks (notably Universal) do not carry SDH/RNIB in the
        // Purpose enum at all — they encode it as a distinct Language enum
        // VALUE ("English SDH", "English RNIB"). When the purpose left the
        // qualifier unset, recover it from the language display name so those
        // tracks are still flagged.
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

/// Which stream list each binding type enumerates, learned from the
/// constructions that DID resolve a language.
///
/// A `<clinit>` walk emits a [`Construction`] for every `new X; … ;
/// invokespecial X.<init>` it sees, so the list mixes real stream bindings
/// with whatever else the class initializer builds. `binding_type` is the
/// constructed class name, which is how the two are told apart: the stream
/// bindings all share one class (Deluxe splits audio and subtitle across two),
/// and that class is identifiable from the slots that resolved.
///
/// A binding type that resolved as both kinds is left out — with no consistent
/// answer, guessing a list to advance would be worse than not advancing.
fn slot_kinds(constructions: &[Construction]) -> HashMap<&str, Option<StreamLabelType>> {
    let mut kinds: HashMap<&str, Option<StreamLabelType>> = HashMap::new();
    for c in constructions {
        let mut has_lang = false;
        let mut has_coding = false;
        for arg in &c.args {
            match arg {
                StackVal::EnumRef {
                    kind: "Language", ..
                } => has_lang = true,
                StackVal::CodingType(_) => has_coding = true,
                _ => {}
            }
        }
        if !has_lang {
            continue;
        }
        let kind = if has_coding {
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

/// The stream list an unresolved construction occupies a slot in, or `None`
/// when it is not a stream binding.
///
/// A `org.bluray.ti.CodingType` argument is decisive on its own: nothing but
/// an audio stream binding is handed one. Otherwise fall back to what the
/// binding type's resolved siblings showed (see [`slot_kinds`]).
fn slot_kind(
    c: &Construction,
    has_coding_type: bool,
    slot_kinds: &HashMap<&str, Option<StreamLabelType>>,
) -> Option<StreamLabelType> {
    if has_coding_type {
        return Some(StreamLabelType::Audio);
    }
    slot_kinds.get(c.binding_type.as_str()).copied().flatten()
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

    // ── Raw .class / .jar fixture builders ──────────────────────────────────
    //
    // `identify_master_enums`, `find_binding_classes` and `decode_binding`
    // operate on `jar::Jar` (a real `ZipArchive`), not on the in-memory
    // `ClassFile` struct the rest of this module's tests build directly (see
    // `class_with_clinit`). To exercise them we need real serialized
    // `.class` bytes inside a real (stored, uncompressed) zip — this is the
    // inverse of `ClassFile::parse` / JVMS §4.

    /// Serialize a constant pool (no Long/Double entries — those need the
    /// post-slot `Empty` padding this helper doesn't handle) to the on-disk
    /// `cp_info` sequence, prefixed by `constant_pool_count`.
    fn encode_cp(entries: &[CpInfo]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
        for e in &entries[1..] {
            match e {
                CpInfo::Utf8(s) => {
                    out.push(1);
                    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
                CpInfo::Integer(n) => {
                    out.push(3);
                    out.extend_from_slice(&n.to_be_bytes());
                }
                CpInfo::Class { name_index } => {
                    out.push(7);
                    out.extend_from_slice(&name_index.to_be_bytes());
                }
                CpInfo::String { string_index } => {
                    out.push(8);
                    out.extend_from_slice(&string_index.to_be_bytes());
                }
                CpInfo::Fieldref {
                    class_index,
                    name_and_type_index,
                } => {
                    out.push(9);
                    out.extend_from_slice(&class_index.to_be_bytes());
                    out.extend_from_slice(&name_and_type_index.to_be_bytes());
                }
                CpInfo::NameAndType {
                    name_index,
                    descriptor_index,
                } => {
                    out.push(12);
                    out.extend_from_slice(&name_index.to_be_bytes());
                    out.extend_from_slice(&descriptor_index.to_be_bytes());
                }
                CpInfo::Methodref {
                    class_index,
                    name_and_type_index,
                } => {
                    out.push(10);
                    out.extend_from_slice(&class_index.to_be_bytes());
                    out.extend_from_slice(&name_and_type_index.to_be_bytes());
                }
                other => unimplemented!("fixture builder doesn't need {other:?}"),
            }
        }
        out
    }

    /// One method's worth of `Code` attribute bytecode, keyed by the cp
    /// index of the `"Code"` Utf8 entry.
    struct MethodSpec {
        name_index: u16,
        descriptor_index: u16,
        code_attr_name_index: u16,
        max_stack: u16,
        code: Vec<u8>,
    }

    /// Serialize a minimal but real `.class` byte buffer: magic, versions,
    /// constant pool, an empty interfaces/fields table, the given methods
    /// (each with exactly one `Code` attribute), and no class attributes.
    fn encode_class(cp: &[CpInfo], this_class: u16, methods: &[MethodSpec]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0xCAFEBABEu32.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes()); // minor
        out.extend_from_slice(&52u16.to_be_bytes()); // major
        out.extend_from_slice(&encode_cp(cp));
        out.extend_from_slice(&0u16.to_be_bytes()); // access_flags
        out.extend_from_slice(&this_class.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes()); // super_class
        out.extend_from_slice(&0u16.to_be_bytes()); // interfaces_count
        out.extend_from_slice(&0u16.to_be_bytes()); // fields_count
        out.extend_from_slice(&(methods.len() as u16).to_be_bytes());
        for m in methods {
            out.extend_from_slice(&0u16.to_be_bytes()); // access_flags
            out.extend_from_slice(&m.name_index.to_be_bytes());
            out.extend_from_slice(&m.descriptor_index.to_be_bytes());
            out.extend_from_slice(&1u16.to_be_bytes()); // attributes_count = 1 (Code)
            out.extend_from_slice(&m.code_attr_name_index.to_be_bytes());
            let info_len = 2 + 2 + 4 + m.code.len();
            out.extend_from_slice(&(info_len as u32).to_be_bytes());
            out.extend_from_slice(&m.max_stack.to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes()); // max_locals
            out.extend_from_slice(&(m.code.len() as u32).to_be_bytes());
            out.extend_from_slice(&m.code);
        }
        out.extend_from_slice(&0u16.to_be_bytes()); // attributes_count (class)
        out
    }

    /// Build a raw, multi-entry, Stored (uncompressed) ZIP — same format as
    /// `jar::tests::build_stored_zip`, generalized to N entries (that helper
    /// is private to `jar.rs`).
    fn build_zip(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        fn crc32(payload: &[u8]) -> u32 {
            let mut crc = 0xFFFF_FFFFu32;
            for &b in payload {
                crc ^= b as u32;
                for _ in 0..8 {
                    let mask = (crc & 1).wrapping_neg();
                    crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                }
            }
            !crc
        }
        let mut out = Vec::new();
        let mut central = Vec::new();
        let mut offsets = Vec::new();
        for (name, payload) in entries {
            let name_bytes = name.as_bytes();
            let crc = crc32(payload);
            offsets.push(out.len() as u32);
            out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
            out.extend_from_slice(&20u16.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes()); // Stored
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(name_bytes);
            out.extend_from_slice(payload);
        }
        for ((name, payload), &lfh_offset) in entries.iter().zip(&offsets) {
            let name_bytes = name.as_bytes();
            let crc = crc32(payload);
            central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
            central.extend_from_slice(&20u16.to_le_bytes());
            central.extend_from_slice(&20u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&crc.to_le_bytes());
            central.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            central.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            central.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u16.to_le_bytes());
            central.extend_from_slice(&0u32.to_le_bytes());
            central.extend_from_slice(&lfh_offset.to_le_bytes());
            central.extend_from_slice(name_bytes);
        }
        let cd_offset = out.len() as u32;
        let cd_size = central.len() as u32;
        out.extend_from_slice(&central);
        out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        out.extend_from_slice(&cd_size.to_le_bytes());
        out.extend_from_slice(&cd_offset.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out
    }

    fn open_jar(bytes: Vec<u8>) -> jar::Jar {
        jar::Jar::new(std::io::Cursor::new(bytes)).expect("valid zip")
    }

    /// Build a `.class` fixture whose `<clinit>` does N `ldc` of distinct
    /// Utf8 constants `values[0..N]` — i.e. a class matching a
    /// `FINGERPRINTS` shape by ldc-sequence.
    fn class_with_ldc_strings(class_name: &str, values: &[&str]) -> Vec<u8> {
        // cp layout: 1 "<clinit>", 2 "()V", 3 "Code", then one Utf8 +
        // one String per value, in pairs (4,5), (6,7), ...
        let mut cp = vec![
            CpInfo::Empty,
            CpInfo::Utf8("<clinit>".into()),
            CpInfo::Utf8("()V".into()),
            CpInfo::Utf8("Code".into()),
        ];
        let mut code = Vec::new();
        for v in values {
            let utf8_idx = cp.len() as u16;
            cp.push(CpInfo::Utf8((*v).to_string()));
            let str_idx = cp.len() as u16;
            cp.push(CpInfo::String {
                string_index: utf8_idx,
            });
            code.push(LDC);
            code.push(str_idx as u8);
        }
        cp.push(CpInfo::Utf8(class_name.to_string()));
        let this_class_name_idx = (cp.len() - 1) as u16;
        cp.push(CpInfo::Class {
            name_index: this_class_name_idx,
        });
        let this_class_idx = (cp.len() - 1) as u16;
        let methods = vec![MethodSpec {
            name_index: 1,
            descriptor_index: 2,
            code_attr_name_index: 3,
            max_stack: 2,
            code,
        }];
        encode_class(&cp, this_class_idx, &methods)
    }

    /// Build a `.class` fixture whose `<clinit>` does N `getstatic`
    /// references to `enum_class.FIELD_i`, for `count_master_enum_getstatic`
    /// / `find_binding_classes` Jar-level fixtures.
    fn class_with_getstatic_refs(class_name: &str, enum_class: &str, n: usize) -> Vec<u8> {
        let mut cp = vec![
            CpInfo::Empty,
            CpInfo::Utf8("<clinit>".into()),
            CpInfo::Utf8("()V".into()),
            CpInfo::Utf8("Code".into()),
            CpInfo::Utf8(enum_class.to_string()),
        ];
        let enum_class_name_idx = 4u16;
        cp.push(CpInfo::Class {
            name_index: enum_class_name_idx,
        });
        let enum_class_idx = (cp.len() - 1) as u16;
        cp.push(CpInfo::Utf8("Lsome/Enum;".into()));
        let descriptor_idx = (cp.len() - 1) as u16;
        let mut code = Vec::new();
        for i in 0..n {
            let field_name_idx = cp.len() as u16;
            cp.push(CpInfo::Utf8(format!("F{i}")));
            let nat_idx = cp.len() as u16;
            cp.push(CpInfo::NameAndType {
                name_index: field_name_idx,
                descriptor_index: descriptor_idx,
            });
            let fieldref_idx = cp.len() as u16;
            cp.push(CpInfo::Fieldref {
                class_index: enum_class_idx,
                name_and_type_index: nat_idx,
            });
            code.push(GETSTATIC);
            code.extend_from_slice(&fieldref_idx.to_be_bytes());
            code.push(0x57); // pop, so the symbolic stack doesn't matter here
        }
        cp.push(CpInfo::Utf8(class_name.to_string()));
        let this_name_idx = (cp.len() - 1) as u16;
        cp.push(CpInfo::Class {
            name_index: this_name_idx,
        });
        let this_class_idx = (cp.len() - 1) as u16;
        let methods = vec![MethodSpec {
            name_index: 1,
            descriptor_index: 2,
            code_attr_name_index: 3,
            max_stack: 2,
            code,
        }];
        encode_class(&cp, this_class_idx, &methods)
    }

    /// A `.class` fixture whose `<clinit>` is exactly `new AudioSlot; dup;
    /// getstatic LanguageEnum.English; invokespecial AudioSlot.<init>
    /// (LLanguageEnum;)V` — one real `Construction`, for Jar-level
    /// `decode_binding` tests. `class_name` only affects the class's own
    /// `this_class` entry (informational); the Jar-level lookup key is the
    /// zip entry path passed to `build_zip`, not this name.
    fn class_with_simple_construction(class_name: &str) -> Vec<u8> {
        let cp = vec![
            CpInfo::Empty,
            CpInfo::Utf8("<clinit>".into()),       // 1
            CpInfo::Utf8("()V".into()),            // 2
            CpInfo::Utf8("Code".into()),           // 3
            CpInfo::Utf8("LanguageEnum".into()),   // 4
            CpInfo::Class { name_index: 4 },       // 5
            CpInfo::Utf8("English".into()),        // 6
            CpInfo::Utf8("LLanguageEnum;".into()), // 7
            CpInfo::NameAndType {
                name_index: 6,
                descriptor_index: 7,
            }, // 8
            CpInfo::Fieldref {
                class_index: 5,
                name_and_type_index: 8,
            }, // 9
            CpInfo::Utf8("AudioSlot".into()),      // 10
            CpInfo::Class { name_index: 10 },      // 11
            CpInfo::Utf8("<init>".into()),         // 12
            CpInfo::Utf8("(LLanguageEnum;)V".into()), // 13
            CpInfo::NameAndType {
                name_index: 12,
                descriptor_index: 13,
            }, // 14
            CpInfo::Methodref {
                class_index: 11,
                name_and_type_index: 14,
            }, // 15
            CpInfo::Utf8(class_name.to_string()),  // 16
            CpInfo::Class { name_index: 16 },      // 17
        ];
        let this_class_idx = 17u16;
        let code: Vec<u8> = vec![
            NEW,
            0,
            11,   // new AudioSlot
            0x59, // dup
            GETSTATIC,
            0,
            9, // getstatic LanguageEnum.English
            INVOKESPECIAL,
            0,
            15, // invokespecial AudioSlot.<init>(LLanguageEnum;)V
        ];
        let methods = vec![MethodSpec {
            name_index: 1,
            descriptor_index: 2,
            code_attr_name_index: 3,
            max_stack: 4,
            code,
        }];
        encode_class(&cp, this_class_idx, &methods)
    }

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

    // ── Phase A: identify_master_enums (Jar-level) ──────────────────────────

    #[test]
    fn identify_master_enums_matches_purpose_fingerprint() {
        // Exact match: 8 ldcs, first 4 = the Purpose prefix, count ==
        // expected_count exactly (abs_diff == 0). A decoy class with the
        // same prefix but a wildly different count must be rejected and
        // must NOT win over the exact match.
        let good = class_with_ldc_strings(
            "GoodPurpose",
            &[
                "Normal",
                "Commentary",
                "PiP",
                "Trivia",
                "Descriptive",
                "Score",
                "NoForced",
                "NoForcedDescriptive",
            ],
        );
        // Prefix matches but count is 100 — abs_diff(100, 8) = 92, far
        // outside LDC_COUNT_TOLERANCE (4). Real logic must reject this
        // class as a Purpose candidate entirely.
        let mut decoy_values: Vec<&str> = vec!["Normal", "Commentary", "PiP", "Trivia"];
        let filler: Vec<String> = (0..96).map(|i| format!("Filler{i}")).collect();
        decoy_values.extend(filler.iter().map(String::as_str));
        let decoy = class_with_ldc_strings("DecoyPurpose", &decoy_values);

        let zip = build_zip(&[
            ("com/bydeluxe/Good.class", good),
            ("com/bydeluxe/Decoy.class", decoy),
        ]);
        let mut archive = open_jar(zip);
        let enums = identify_master_enums(&mut archive);
        let purpose = enums
            .iter()
            .find(|(label, _)| *label == "Purpose")
            .unwrap_or_else(|| panic!("Purpose fingerprint not matched: {enums:?}"));
        // The identified class is keyed by its JVM INTERNAL name (`this_class`,
        // here "GoodPurpose"), NOT the zip entry name — that internal name is
        // what the binding class's `getstatic` operands reference.
        assert_eq!(purpose.1.class_name, "GoodPurpose");
        assert_eq!(purpose.1.values.len(), 8);
        assert_eq!(purpose.1.values[0], "Normal");
        assert_eq!(purpose.1.values[7], "NoForcedDescriptive");
    }

    /// Two candidates that BOTH match a fingerprint's prefix and are BOTH
    /// within `LDC_COUNT_TOLERANCE` but neither exact must resolve the same
    /// way on every run.
    ///
    /// The tie-break only prefers an exact ldc count over an inexact one, so
    /// between two inexact candidates the winner is whichever the pool yields
    /// first. Backed by a `HashMap` that is per-process seeded, meaning the
    /// same disc image could pick a different master enum on a second run and
    /// emit different commentary/SDH labels for byte-identical input — with
    /// nothing in the output to say the choice was arbitrary.
    ///
    /// Running the whole identification repeatedly is what makes this a test
    /// rather than a hope: a single run cannot distinguish "deterministic"
    /// from "got lucky", and the hash seed does not change within one process.
    #[test]
    fn identify_master_enums_breaks_a_tie_between_two_inexact_candidates_deterministically() {
        // Both are prefix-matching Purpose candidates at diff 2 and 3 from the
        // expected count of 8 — inside LDC_COUNT_TOLERANCE (4), neither exact,
        // so the exact-count tie-break never fires and only ordering decides.
        let mut a_vals: Vec<&str> = vec!["Normal", "Commentary", "PiP", "Trivia"];
        let a_fill: Vec<String> = (0..6).map(|i| format!("Afill{i}")).collect(); // 10, diff 2
        a_vals.extend(a_fill.iter().map(String::as_str));
        let mut b_vals: Vec<&str> = vec!["Normal", "Commentary", "PiP", "Trivia"];
        let b_fill: Vec<String> = (0..7).map(|i| format!("Bfill{i}")).collect(); // 11, diff 3
        b_vals.extend(b_fill.iter().map(String::as_str));

        let mut winners = std::collections::BTreeSet::new();
        for _ in 0..16 {
            let zip = build_zip(&[
                (
                    "com/bydeluxe/Alpha.class",
                    class_with_ldc_strings("AlphaPurpose", &a_vals),
                ),
                (
                    "com/bydeluxe/Beta.class",
                    class_with_ldc_strings("BetaPurpose", &b_vals),
                ),
            ]);
            let mut archive = open_jar(zip);
            let enums = identify_master_enums(&mut archive);
            let purpose = enums
                .iter()
                .find(|(label, _)| *label == "Purpose")
                .expect("both candidates match the Purpose prefix within tolerance");
            winners.insert(purpose.1.class_name.clone());
        }

        assert_eq!(
            winners.len(),
            1,
            "the same jar must resolve the same master enum every time; got {winners:?}. \
             A per-process-seeded map here means one disc can emit different labels on \
             different runs, with nothing in the output saying the choice was arbitrary"
        );
    }

    #[test]
    fn identify_master_enums_accepts_count_at_the_tolerance_boundary() {
        // abs_diff(expected_count, count) == LDC_COUNT_TOLERANCE (4) exactly
        // must still be accepted (`> tolerance` rejects, so `== tolerance`
        // is the last accepted value). This is the boundary `327:50`
        // mutants (`>` -> `==`/`<`/`>=`) disagree on.
        let mut values: Vec<&str> = vec!["Normal", "Commentary", "PiP", "Trivia"];
        let filler: Vec<String> = (0..8).map(|i| format!("Filler{i}")).collect(); // 4+8=12, diff=4
        values.extend(filler.iter().map(String::as_str));
        assert_eq!(values.len(), 12);
        let class = class_with_ldc_strings("BoundaryPurpose", &values);
        let zip = build_zip(&[("com/bydeluxe/B.class", class)]);
        let mut archive = open_jar(zip);
        let enums = identify_master_enums(&mut archive);
        assert!(
            enums.iter().any(|(label, _)| *label == "Purpose"),
            "a class exactly LDC_COUNT_TOLERANCE away from expected_count must still match"
        );
    }

    #[test]
    fn identify_master_enums_finds_nothing_without_com_bydeluxe_signal() {
        // No FINGERPRINTS-matching class in the jar at all -> empty result
        // (kills the `vec![]` mutant only vacuously if paired with the
        // positive tests above proving non-emptiness on a real match).
        let unrelated = class_with_ldc_strings("Unrelated", &["Foo", "Bar"]);
        let zip = build_zip(&[("x/Unrelated.class", unrelated)]);
        let mut archive = open_jar(zip);
        assert!(identify_master_enums(&mut archive).is_empty());
    }

    // ── Phase C: find_binding_classes / count_master_enum_getstatic ────────

    #[test]
    fn count_master_enum_getstatic_counts_only_master_classes() {
        // Directly exercises count_master_enum_getstatic on a synthetic
        // ClassFile (no Jar needed — this function takes &ClassFile).
        let master: HashSet<&str> = ["LanguageEnum"].into_iter().collect();
        let code_bytes = class_with_getstatic_refs("X", "LanguageEnum", 5);
        // Round-trip through ClassFile::parse to get a real &ClassFile.
        let class =
            super::super::class_reader::ClassFile::parse(&code_bytes).expect("fixture must parse");
        assert_eq!(count_master_enum_getstatic(&class, &master), 5);

        // getstatic refs to a class NOT in master_enum_classes must not count.
        let other_master: HashSet<&str> = ["SomeOtherEnum"].into_iter().collect();
        assert_eq!(count_master_enum_getstatic(&class, &other_master), 0);
    }

    #[test]
    fn find_binding_classes_picks_top_candidates_above_threshold() {
        // Class A: 100 getstatic refs (the top / binding class). B: 45
        // (>40% of top, kept). F: 40 (EXACTLY the 40% threshold — pins
        // both the `(top_count * 2) / 5` arithmetic and the `>=`
        // comparison: any of the `460`/`461` arithmetic mutants shift
        // the threshold away from exactly 40, and a `>= -> <` mutant at
        // 461 would drop this exact-boundary entry). E: 39 (just BELOW
        // the true 40% threshold — a mutant that shrinks the threshold
        // below 39 would wrongly keep this). C: 10 (well below, always
        // dropped). D: 3 — below MIN_GETSTATIC(4), never even a raw
        // candidate.
        let master_classes: HashSet<&str> = ["LanguageEnum"].into_iter().collect();
        let a = class_with_getstatic_refs("A", "LanguageEnum", 100);
        let b = class_with_getstatic_refs("B", "LanguageEnum", 45);
        let f = class_with_getstatic_refs("F", "LanguageEnum", 40);
        let e = class_with_getstatic_refs("E", "LanguageEnum", 39);
        let c = class_with_getstatic_refs("C", "LanguageEnum", 10);
        let d = class_with_getstatic_refs("D", "LanguageEnum", 3);
        let zip = build_zip(&[
            ("com/bydeluxe/A.class", a),
            ("com/bydeluxe/B.class", b),
            ("com/bydeluxe/F.class", f),
            ("com/bydeluxe/E.class", e),
            ("com/bydeluxe/C.class", c),
            ("com/bydeluxe/D.class", d),
        ]);
        let mut archive = open_jar(zip);
        let candidates = find_binding_classes(&mut archive, &master_classes);
        let names: Vec<&str> = candidates.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "com/bydeluxe/A.class",
                "com/bydeluxe/B.class",
                "com/bydeluxe/F.class"
            ],
            "expected [A(100), B(45), F(40)] retained (>=40% of top, descending \
             order, E(39)/C(10)/D(3) dropped), got {names:?}"
        );
        assert_eq!(candidates[0].1, 100);
        assert_eq!(candidates[1].1, 45);
    }

    #[test]
    fn find_binding_classes_empty_master_set_yields_no_candidates() {
        let master_classes: HashSet<&str> = HashSet::new();
        let a = class_with_getstatic_refs("A", "LanguageEnum", 100);
        let zip = build_zip(&[("com/bydeluxe/A.class", a)]);
        let mut archive = open_jar(zip);
        assert!(find_binding_classes(&mut archive, &master_classes).is_empty());
    }

    // ── Phase D: decode_binding (Jar-level short-circuit wrapper) ───────────

    #[test]
    fn decode_binding_finds_named_class_and_stops_at_first_match() {
        // `decode_binding` matches by the Jar entry path (the same string
        // `find_binding_classes` returns), not by the class's own
        // `this_class` name. Two entries: the target path carries a real
        // `new AudioSlot; dup; getstatic; invokespecial` construction; a
        // decoy at a different path carries none (and, being pure
        // getstatic/pop, would also match nothing if walked). If the name
        // comparison is broken (`!=` mutated to `==`), decode_binding would
        // either never match the real target (empty result) or would match
        // and decode the WRONG entry.
        let target = class_with_simple_construction("Ignored");
        let decoy = class_with_getstatic_refs("Ignored2", "LanguageEnum", 2);
        let zip = build_zip(&[
            ("com/bydeluxe/Target.class", target),
            ("com/bydeluxe/Decoy.class", decoy),
        ]);
        let mut archive = open_jar(zip);
        let master = lang_enum_master();

        let ctors = decode_binding(&mut archive, "com/bydeluxe/Target.class", &master);
        assert_eq!(
            ctors.len(),
            1,
            "expected the Target entry's one construction"
        );
        assert_eq!(ctors[0].binding_type, "AudioSlot");

        // A name with no matching entry must yield nothing (try_each_class
        // never finds a Some).
        assert!(decode_binding(&mut archive, "com/bydeluxe/NoSuchClass.class", &master).is_empty());
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
    fn clinit_ldc_string_bytes_boundary_matches_256kib_not_1280() {
        // `MAX_CLINIT_LDC_BYTES = 256 * 1024` (262144). A `* -> +` mutant at
        // that computation collapses the cap to `256 + 1024` (1280) — 205x
        // smaller. 1000-byte strings make the two cap values discriminate
        // sharply: correct code retains 262 of them (262000 bytes, the
        // 263rd would push to 263000 > 262144); the mutant retains only 1
        // (the 2nd would push to 2000 > 1280).
        const N: usize = 400;
        let one = "x".repeat(1000);
        let mut code = Vec::with_capacity(N * 2);
        for _ in 0..N {
            code.push(LDC);
            code.push(4);
        }
        let class = class_with_clinit(ldc_pool(&one), 2, &code);
        let ldcs = clinit_ldc_strings(&class).expect("<clinit> present");
        assert_eq!(
            ldcs.len(),
            262,
            "expected 262 retained 1000-byte strings under a 256 KiB cap, got {} \
             — either the cap value or the truncation arithmetic changed",
            ldcs.len()
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

    /// `insert` rejects when `bytes.saturating_add(cost) > MAX_CANDIDATE_TOTAL_BYTES`
    /// — i.e. landing EXACTLY on the cap is still accepted; only strictly
    /// exceeding it is rejected. A `>` -> `>=` mutant would reject the
    /// exact-cap entry too. Two entries are sized so the second brings
    /// `bytes` to precisely `MAX_CANDIDATE_TOTAL_BYTES`, not one byte over.
    #[test]
    fn candidate_pool_insert_accepts_landing_exactly_on_the_cap() {
        let mut pool = CandidatePool::default();
        // cost = name.len() + payload.len() = 1 + (CAP - 2) = CAP - 1.
        let first_payload = "a".repeat(MAX_CANDIDATE_TOTAL_BYTES - 2);
        assert!(pool.insert("a", vec![first_payload]));
        assert_eq!(pool.bytes, MAX_CANDIDATE_TOTAL_BYTES - 1);

        // cost = 1 (name "b") + 0 (empty string) = 1. bytes becomes exactly
        // MAX_CANDIDATE_TOTAL_BYTES — must be ACCEPTED, not rejected.
        let accepted = pool.insert("b", vec![String::new()]);
        assert!(
            accepted,
            "an entry landing exactly on MAX_CANDIDATE_TOTAL_BYTES must be accepted, \
             only entries that exceed it should be rejected"
        );
        assert_eq!(pool.bytes, MAX_CANDIDATE_TOTAL_BYTES);

        // One more byte of cost now genuinely exceeds the cap and must be rejected.
        assert!(!pool.insert("c", vec!["x".to_string()]));
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
            // Empty: this synthetic enum resolves by value (the fallback in
            // `MasterEnumTable::from`), so the resolve tests below key on
            // "English"/"French"/"Spanish" directly.
            fields: Vec::new(),
        };
        MasterEnumTable::from(&[("Language", m)])
    }

    #[test]
    fn decode_binding_class_finds_the_clinit_method_and_emits_its_construction() {
        // decode_binding_class wraps BindingDecoder over every method literally
        // named "<clinit>" on the class. Exercises the method-selection
        // (`member_name(m) != Some("<clinit>")`) and per-method-union
        // truncation (`room == 0`) logic that decode_binding_class adds on
        // top of the already-tested BindingDecoder::step/run.
        //
        // Pool layout (must hold "<clinit>"/"()V"/"Code" at 1/2/3 per
        // `class_with_clinit`'s contract, while ALSO matching the fixed cp
        // indices — 6/8/12 — the reused `new AudioSlot; dup; getstatic;
        // invokespecial` bytecode below references):
        //   1 Utf8 "<clinit>"          2 Utf8 "()V"            3 Utf8 "Code"
        //   4 Utf8 "LanguageEnum"      5 Class->4
        //   6 Fieldref{class:5,nat:9}  7 Utf8 "English"
        //   8 Class->10 (AudioSlot)    9 NameAndType{name:7,desc:11}
        //  10 Utf8 "AudioSlot"        11 Utf8 "LLanguageEnum;"
        //  12 Methodref{class:8,nat:13}
        //  13 NameAndType{name:14,desc:15}
        //  14 Utf8 "<init>"           15 Utf8 "(LLanguageEnum;)V"
        let pool = ConstantPool::from_entries(vec![
            CpInfo::Empty,
            CpInfo::Utf8("<clinit>".into()),
            CpInfo::Utf8("()V".into()),
            CpInfo::Utf8("Code".into()),
            CpInfo::Utf8("LanguageEnum".into()),
            CpInfo::Class { name_index: 4 },
            CpInfo::Fieldref {
                class_index: 5,
                name_and_type_index: 9,
            },
            CpInfo::Utf8("English".into()),
            CpInfo::Class { name_index: 10 },
            CpInfo::NameAndType {
                name_index: 7,
                descriptor_index: 11,
            },
            CpInfo::Utf8("AudioSlot".into()),
            CpInfo::Utf8("LLanguageEnum;".into()),
            CpInfo::Methodref {
                class_index: 8,
                name_and_type_index: 13,
            },
            CpInfo::NameAndType {
                name_index: 14,
                descriptor_index: 15,
            },
            CpInfo::Utf8("<init>".into()),
            CpInfo::Utf8("(LLanguageEnum;)V".into()),
        ]);
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
        let class = class_with_clinit(pool, 4, &code);
        let master = lang_enum_master();
        let constructions = decode_binding_class(&class, &master);
        assert_eq!(
            constructions.len(),
            1,
            "expected exactly 1 Construction from the single <clinit>, got {}",
            constructions.len()
        );
        assert_eq!(constructions[0].binding_type, "AudioSlot");
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
    fn binding_decoder_dup_duplicates_top_of_stack() {
        // JVMS §3.11.7 `dup` (0x59): duplicate the top stack value. Checked
        // directly on `decoder.stack` (not via emitted Constructions, which
        // a single `new X; dup; invokespecial` sequence can satisfy either
        // way — the leftover copy `dup` is responsible for only matters
        // once something ELSE consumes it afterward). `new AudioSlot; dup`
        // with no invokespecial must leave exactly two NewObj("AudioSlot")
        // entries.
        let pool = build_simple_pool();
        let master = lang_enum_master();
        let code: Vec<u8> = vec![NEW, 0, 8, 0x59 /* dup */];
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 4,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(
            decoder.stack.len(),
            2,
            "dup must duplicate, not skip, the top value"
        );
        for v in &decoder.stack {
            match v {
                StackVal::NewObj(name) => assert_eq!(name, "AudioSlot"),
                other => panic!("expected NewObj(\"AudioSlot\") x2, got {other:?}"),
            }
        }
    }

    /// Pool with a single Methodref (cp index 6) to `AnyClass.m<descriptor>`,
    /// for the `invokevirtual`/`invokestatic`/`invokeinterface` arg-popping
    /// tests below.
    fn call_ref_pool(descriptor: &str) -> ConstantPool {
        ConstantPool::from_entries(vec![
            CpInfo::Empty,
            CpInfo::Utf8("AnyClass".into()),      // 1
            CpInfo::Class { name_index: 1 },      // 2
            CpInfo::Utf8("m".into()),             // 3
            CpInfo::Utf8(descriptor.to_string()), // 4
            CpInfo::NameAndType {
                name_index: 3,
                descriptor_index: 4,
            }, // 5
            CpInfo::Methodref {
                class_index: 2,
                name_and_type_index: 5,
            }, // 6
        ])
    }

    #[test]
    fn binding_decoder_invokevirtual_pops_receiver_plus_args() {
        // JVMS §6.5 `invokevirtual`/`invokeinterface` pop the receiver
        // PLUS the descriptor's args (`extra = 1` for opcodes 0xB6/0xB9);
        // `invokestatic` (0xB8) pops ONLY the args (no receiver). Each
        // case below pushes exactly `to_pop` placeholder ints and checks
        // the stack is fully drained — a wrong `extra`/`arg_count+extra`
        // computation leaves a wrong number of leftovers.
        let master = lang_enum_master();
        let run_stack_len = |opcode: u8, descriptor: &str, n_pushes: usize| -> usize {
            let pool = call_ref_pool(descriptor);
            let mut code = Vec::new();
            for i in 0..n_pushes {
                code.push(ICONST_0 + i as u8); // distinct placeholder ints
            }
            code.push(opcode);
            code.push(0);
            code.push(6);
            if opcode == 0xB9 {
                // invokeinterface (JVMS §6.5): 2 extra operand bytes —
                // `count` (here: arg slot count + 1 for the receiver, per
                // spec) and a reserved zero byte.
                code.push((n_pushes) as u8);
                code.push(0);
            }
            let attr = super::super::class_reader::CodeAttribute {
                max_stack: 8,
                max_locals: 0,
                code: &code,
            };
            let mut decoder = BindingDecoder::new(&pool, &master);
            decoder.run(&attr);
            decoder.stack.len()
        };

        // invokevirtual, 1-arg descriptor: pops receiver + 1 arg = 2.
        assert_eq!(
            run_stack_len(0xB6, "(I)V", 2),
            0,
            "invokevirtual must pop receiver + args"
        );
        // invokeinterface, 1-arg descriptor: same as invokevirtual.
        assert_eq!(
            run_stack_len(0xB9, "(I)V", 2),
            0,
            "invokeinterface must pop receiver + args"
        );
        // invokestatic, 2-arg descriptor: pops ONLY the 2 args, no receiver.
        assert_eq!(
            run_stack_len(0xB8, "(II)V", 2),
            0,
            "invokestatic must pop exactly the arg count, no receiver"
        );
        // invokestatic with a leftover value UNDER the args: only the args
        // are popped, the leftover survives. Distinguishes a `>` mutant at
        // the `len < to_pop` guard (which would incorrectly `clear()` the
        // whole stack here instead of leaving the leftover).
        assert_eq!(
            run_stack_len(0xB8, "(I)V", 2), // 1 leftover + 1 real arg pushed
            1,
            "only the descriptor's args must be popped, not the whole stack"
        );
    }

    #[test]
    fn binding_decoder_invoke_family_defensively_clears_on_stack_underflow() {
        // If the symbolic stack has FEWER entries than the call needs to
        // pop (malformed/adversarial bytecode, or earlier drift), the
        // decoder must defensively clear rather than underflow-subtract
        // (`len - to_pop` with `len < to_pop` would panic on the `usize`
        // subtraction).
        let pool = call_ref_pool("(II)V"); // needs to_pop = 2
        let code: Vec<u8> = vec![ICONST_0, 0xB8, 0, 6]; // only 1 value on stack
        let master = lang_enum_master();
        let attr = super::super::class_reader::CodeAttribute {
            max_stack: 8,
            max_locals: 0,
            code: &code,
        };
        let mut decoder = BindingDecoder::new(&pool, &master);
        decoder.run(&attr);
        assert_eq!(
            decoder.stack.len(),
            0,
            "stack-underflowing invoke must clear defensively, not underflow-subtract"
        );
    }

    /// Pool for a single-int-arg constructor `AudioSlot.<init>(I)V`, used by
    /// `binding_decoder_int_push_opcodes_produce_the_right_value` to isolate
    /// each int-push opcode's produced VALUE (not just "a construction
    /// happened") — JVMS §3.11.3 (`iconst_<i>`, `bipush`, `sipush`, `ldc` of
    /// a `CONSTANT_Integer`) each push a specific known int.
    fn int_ctor_pool() -> ConstantPool {
        ConstantPool::from_entries(vec![
            CpInfo::Empty,
            CpInfo::Utf8("AudioSlot".into()), // 1
            CpInfo::Class { name_index: 1 },  // 2
            CpInfo::Utf8("<init>".into()),    // 3
            CpInfo::Utf8("(I)V".into()),      // 4
            CpInfo::NameAndType {
                name_index: 3,
                descriptor_index: 4,
            }, // 5
            CpInfo::Methodref {
                class_index: 2,
                name_and_type_index: 5,
            }, // 6
            CpInfo::Integer(12345),           // 7 — for the `ldc`/Integer case
        ])
    }

    #[test]
    fn binding_decoder_int_push_opcodes_produce_the_right_value() {
        // JVMS §3.11.3: iconst_<i> pushes exactly i (i in -1..=5); bipush
        // sign-extends its i8 operand; sipush sign-extends its i16 operand;
        // ldc of a CONSTANT_Integer pushes that constant. Each is checked
        // as the sole arg of `new AudioSlot; dup; <push>; invokespecial
        // AudioSlot.<init>(I)V` so a wrong (or absent, if the opcode's match
        // arm were deleted) push shows up as a wrong (or missing/Unknown)
        // arg value, not just "some construction happened".
        let cases: Vec<(&str, Vec<u8>, i32)> = vec![
            ("iconst_m1", vec![ICONST_M1], -1),
            ("iconst_0", vec![ICONST_0], 0),
            ("iconst_1", vec![ICONST_1], 1),
            ("iconst_2", vec![ICONST_2], 2),
            ("iconst_3", vec![ICONST_3], 3),
            ("iconst_4", vec![ICONST_4], 4),
            ("iconst_5", vec![ICONST_5], 5),
            ("bipush -100", vec![BIPUSH, 0x9C], -100), // 0x9C as i8 = -100
            ("sipush 4660", vec![SIPUSH, 0x12, 0x34], 4660), // 0x1234
            ("ldc Integer(12345)", vec![LDC, 7], 12345),
        ];
        let pool = int_ctor_pool();
        let master = lang_enum_master();
        for (label, push, expected) in cases {
            let mut code = vec![NEW, 0, 2, 0x59 /* dup */];
            code.extend_from_slice(&push);
            code.extend_from_slice(&[INVOKESPECIAL, 0, 6]);
            let attr = super::super::class_reader::CodeAttribute {
                max_stack: 4,
                max_locals: 0,
                code: &code,
            };
            let mut decoder = BindingDecoder::new(&pool, &master);
            decoder.run(&attr);
            assert_eq!(
                decoder.constructions.len(),
                1,
                "{label}: expected exactly 1 construction"
            );
            match &decoder.constructions[0].args[0] {
                StackVal::Int(n) => assert_eq!(*n, expected, "{label}: wrong int value"),
                other => panic!("{label}: expected StackVal::Int({expected}), got {other:?}"),
            }
        }
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

    /// Each stream binding in a binding class's `<clinit>` is one STN slot,
    /// in STN order — that is the whole basis for numbering them positionally
    /// here. Whether the abstract interpreter managed to RESOLVE a slot's
    /// language does not change how many slots the disc has: a `getstatic`
    /// whose owning class was not fingerprinted as a master enum, or whose
    /// field is missing from the resolved ordinal map, arrives as
    /// `StackVal::Unknown`.
    ///
    /// A slot that resolved nothing has no label to emit, but it must still
    /// consume its number. Skipping it renumbers every slot behind it and
    /// binds their labels — language, commentary, descriptive-audio — one
    /// stream early.
    #[test]
    fn interpret_streams_unresolved_slot_still_consumes_its_number() {
        let audio_slot = |lang: Option<u16>| Construction {
            binding_type: "AudioSlot".into(),
            args: vec![
                match lang {
                    Some(ordinal) => StackVal::EnumRef {
                        kind: "Language",
                        ordinal,
                    },
                    // Language `getstatic` the decoder could not resolve.
                    None => StackVal::Unknown,
                },
                StackVal::CodingType("DOLBY_AC3_AUDIO".into()),
            ],
        };
        let sub_slot = |lang: Option<u16>| Construction {
            binding_type: "SubtitleSlot".into(),
            args: vec![match lang {
                Some(ordinal) => StackVal::EnumRef {
                    kind: "Language",
                    ordinal,
                },
                None => StackVal::Unknown,
            }],
        };

        let constructions = vec![
            audio_slot(Some(0)), // audio STN 1 — English
            audio_slot(None),    // audio STN 2 — unresolved
            audio_slot(Some(1)), // audio STN 3 — French
            sub_slot(Some(0)),   // PG STN 1 — English
            sub_slot(None),      // PG STN 2 — unresolved
            sub_slot(Some(2)),   // PG STN 3 — Spanish
            // Not a stream binding at all: no language, no CodingType, and a
            // binding type that never yielded a stream. Must not take a slot.
            Construction {
                binding_type: "java/lang/StringBuilder".into(),
                args: Vec::new(),
            },
            sub_slot(Some(1)), // PG STN 4 — French
        ];

        let out = interpret_streams(&constructions, &lang_enum_master());

        let audio: Vec<_> = out
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .map(|l| (l.language.as_str(), l.stream_number))
            .collect();
        assert_eq!(
            audio,
            vec![("eng", 1), ("fra", 3)],
            "the unresolved audio slot owns STN 2"
        );

        let sub: Vec<_> = out
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .map(|l| (l.language.as_str(), l.stream_number))
            .collect();
        assert_eq!(
            sub,
            vec![("eng", 1), ("spa", 3), ("fra", 4)],
            "the unresolved PG slot owns STN 2; the non-stream construction \
             owns nothing"
        );
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

    // ── Real-disc fixtures: Universal (studio="uni"), Fast Five ──────────────
    //
    // Captured from `FastFive.iso` `/BDMV/JAR/00000.jar` (`com/bydeluxe/…`).
    // These are verbatim, unmodified `.class` files — the format we READ, never
    // execute — exercising the full Phase A→D pipeline against real obfuscated
    // Deluxe bytecode rather than synthetic fixtures.
    //
    //   pd.class = Language enum (65 values: English, French, Spanish, Dutch …)
    //   lp.class = Purpose enum  (Normal, Commentary, PiP, Trivia, Descriptive, Score)
    //   tl.class = binding class: audio `np.<init>(I,Lpd;,Llp;,LCodingType;)`,
    //              subtitle `wb.<init>(I,Lpd;,Llp;,Lmi;)`, and a title-wrapper
    //              `oq.<init>(…,[Lnp;,[Lwb;,[Loq;,[J)` that must NOT leak a label.
    const UNI_PD_CLASS: &[u8] = include_bytes!("testdata/deluxe_uni/pd.class");
    const UNI_LP_CLASS: &[u8] = include_bytes!("testdata/deluxe_uni/lp.class");
    const UNI_TL_CLASS: &[u8] = include_bytes!("testdata/deluxe_uni/tl.class");

    fn parse_fixture(bytes: &[u8]) -> super::super::class_reader::ClassFile {
        super::super::class_reader::ClassFile::parse(bytes).expect("fixture .class parses")
    }

    #[test]
    fn universal_language_enum_pd_matches_the_language_fingerprint() {
        let pd = parse_fixture(UNI_PD_CLASS);
        let values = clinit_ldc_strings(&pd).expect("pd has a <clinit>");
        // 65-value Universal Language enum — the count the old 70±4 window
        // wrongly rejected.
        assert_eq!(values.len(), 65);
        assert_eq!(&values[..4], &["English", "French", "Spanish", "Dutch"]);
        assert!(
            ldcs_match_prefix(&values, FINGERPRINTS[0].prefix),
            "pd must match the Language prefix"
        );
        assert!(
            values.len().abs_diff(FINGERPRINTS[0].expected_count)
                <= FINGERPRINTS[0].count_tolerance,
            "65 must fall inside the Language count window (regression guard on \
             the widened tolerance)"
        );
    }

    #[test]
    fn universal_enum_field_names_map_ordinals_to_obfuscated_fields() {
        let pd = parse_fixture(UNI_PD_CLASS);
        let fields = clinit_enum_field_names(&pd);
        // One putstatic per enum constant, in declaration order: a,b,c,d,…
        assert_eq!(fields.len(), 65);
        assert_eq!(&fields[..4], &["a", "b", "c", "d"]);
    }

    /// Build the master table the way `parse` would, but from the fixtures.
    fn universal_master() -> MasterEnumTable {
        let pd = parse_fixture(UNI_PD_CLASS);
        let lp = parse_fixture(UNI_LP_CLASS);
        let pd_enum = MasterEnum {
            class_name: pd.this_class_name().unwrap().to_string(),
            values: clinit_ldc_strings(&pd).unwrap(),
            fields: clinit_enum_field_names(&pd),
        };
        let lp_enum = MasterEnum {
            class_name: lp.this_class_name().unwrap().to_string(),
            values: clinit_ldc_strings(&lp).unwrap(),
            fields: clinit_enum_field_names(&lp),
        };
        MasterEnumTable::from(&[("Language", pd_enum), ("Purpose", lp_enum)])
    }

    #[test]
    fn universal_binding_class_decodes_real_per_stream_labels() {
        let tl = parse_fixture(UNI_TL_CLASS);
        let master = universal_master();
        let constructions = decode_binding_class(&tl, &master);
        assert!(
            !constructions.is_empty(),
            "tl.<clinit> must yield per-stream constructions"
        );
        // The title-wrapper `oq` takes array parameters and must be filtered:
        // every retained construction is a scalar/enum-only binding.
        // (np = 4 args incl. CodingType; wb = 4-5 args incl. `mi`.)

        let labels = interpret_streams(&constructions, &master);
        assert!(!labels.is_empty(), "must emit at least one stream label");

        let audio: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio)
            .collect();
        let subs: Vec<_> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Subtitle)
            .collect();
        assert!(!audio.is_empty(), "expected audio labels");
        assert!(!subs.is_empty(), "expected subtitle labels");

        // Real languages recovered via the obfuscated-field-name resolver.
        let langs: std::collections::HashSet<&str> =
            labels.iter().map(|l| l.language.as_str()).collect();
        assert!(langs.contains("eng"), "English audio/subtitle expected");
        assert!(
            langs.contains("spa") || langs.contains("fra"),
            "expected at least one non-English language (Spanish/French)"
        );

        // The display name is the disc's OWN label, not just an ISO code.
        assert!(
            labels.iter().any(|l| l.name == "English"),
            "expected the disc-authored display name 'English'"
        );

        // Audio bindings carry a decoded BD-J CodingType → codec hint.
        assert!(
            audio.iter().any(|l| l.codec_hint == "Dolby Digital"),
            "DOLBY_AC3_AUDIO must translate to 'Dolby Digital'; got {:?}",
            audio.iter().map(|l| &l.codec_hint).collect::<Vec<_>>()
        );

        // Subtitle bindings (`wb`, no CodingType) carry no codec hint.
        assert!(
            subs.iter().all(|l| l.codec_hint.is_empty()),
            "subtitle codec hints should be empty (PG implied)"
        );
    }

    #[test]
    fn universal_config_xml_studio_attribute_is_parsed() {
        // The real config.xml from FastFive.iso /BDMV/JAR/99999/config.xml.
        let xml = br#"<TitleConfig studio="uni" >
  <Type><Rental>-</Rental><Single>-</Single></Type>
</TitleConfig>"#;
        assert_eq!(parse_studio_attr(xml).as_deref(), Some("uni"));
        // Single-quoted and whitespace-padded variants.
        assert_eq!(
            parse_studio_attr(b"<TitleConfig studio = 'fox'>").as_deref(),
            Some("fox")
        );
        // Missing / empty attribute → None (never panics).
        assert_eq!(parse_studio_attr(b"<TitleConfig>"), None);
        assert_eq!(parse_studio_attr(b"studio=\"\""), None);
        assert_eq!(parse_studio_attr(&[0xff, 0xfe, 0x00]), None);
    }
}

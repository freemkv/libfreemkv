use super::*;

// A budget no test fixture can exhaust.
fn unbounded() -> u64 {
    u64::MAX
}

// Raw .class/.jar fixture builders: `identify_master_enums`, `find_binding_classes`
// and `decode_binding` operate on `jar::Jar` (a real `ZipArchive`), not the
// in-memory `ClassFile` struct other tests build, so these need real `.class` bytes.

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

// A .class fixture whose `<clinit>` is `new AudioSlot; dup; getstatic
// LanguageEnum.English; invokespecial AudioSlot.<init>(LLanguageEnum;)V`
// — one real Construction, for Jar-level decode_binding tests.
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
    // Lock the fingerprint roster so adding/removing one is deliberate.
    // All 5 documented enums must be here; Codec is structural (separate
    // path), not fingerprinted by ldc prefix.
    let labels: Vec<&str> = FINGERPRINTS.iter().map(|fp| fp.label).collect();
    assert_eq!(
        labels,
        vec!["Language", "Purpose", "VideoFormat", "Region", "Studio"]
    );
}

#[test]
fn fingerprint_prefixes_nonempty_and_under_expected_count() {
    // Each prefix must be non-empty and shorter than expected_count so the
    // count gives additional signal beyond the prefix match; a prefix as
    // long as expected_count has no counting benefit.
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
    // Exact match: 8 ldcs, first 4 = Purpose prefix, count == expected_count
    // exactly. A decoy class with the same prefix but a wildly different
    // count must be rejected and must NOT win over the exact match.
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
    let enums = identify_master_enums(&mut archive, &mut unbounded());
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

// Two candidates that BOTH match a fingerprint's prefix and are BOTH within
// LDC_COUNT_TOLERANCE but neither exact must resolve the same way on every
// run. Repeated runs are what distinguish "deterministic" from "got lucky".
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
        let enums = identify_master_enums(&mut archive, &mut unbounded());
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
    // abs_diff(expected_count, count) == LDC_COUNT_TOLERANCE (4) exactly must
    // still be accepted (`> tolerance` rejects, so `== tolerance` is the last
    // accepted value) — the boundary `>` vs `==`/`<`/`>=` mutants disagree on.
    let mut values: Vec<&str> = vec!["Normal", "Commentary", "PiP", "Trivia"];
    let filler: Vec<String> = (0..8).map(|i| format!("Filler{i}")).collect(); // 4+8=12, diff=4
    values.extend(filler.iter().map(String::as_str));
    assert_eq!(values.len(), 12);
    let class = class_with_ldc_strings("BoundaryPurpose", &values);
    let zip = build_zip(&[("com/bydeluxe/B.class", class)]);
    let mut archive = open_jar(zip);
    let enums = identify_master_enums(&mut archive, &mut unbounded());
    assert!(
        enums.iter().any(|(label, _)| *label == "Purpose"),
        "a class exactly LDC_COUNT_TOLERANCE away from expected_count must still match"
    );
}

#[test]
fn identify_master_enums_finds_nothing_without_com_bydeluxe_signal() {
    // No FINGERPRINTS-matching class in the jar -> empty result (kills the
    // `vec![]` mutant only vacuously, paired with positive tests above).
    let unrelated = class_with_ldc_strings("Unrelated", &["Foo", "Bar"]);
    let zip = build_zip(&[("x/Unrelated.class", unrelated)]);
    let mut archive = open_jar(zip);
    assert!(identify_master_enums(&mut archive, &mut unbounded()).is_empty());
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
    // A: 100 refs (top). B: 45 (>40% of top, kept). F: 40 (EXACTLY the 40%
    // threshold). E: 39 (just below, dropped). D: 3 (below MIN_GETSTATIC(4)).
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
    let candidates = find_binding_classes(&mut archive, &master_classes, &mut unbounded());
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

// Phase A, C and D share one inflate budget: bytes Phase A inflated are not
// available again to Phase C.
#[test]
fn deluxe_sweeps_share_one_inflate_budget() {
    let class = class_with_getstatic_refs("com/bydeluxe/Bind", "com/bydeluxe/Lang", 5);
    let len = class.len() as u64;
    let mut archive = open_jar(build_zip(&[("com/bydeluxe/Bind.class", class)]));
    let master_classes: HashSet<&str> = ["com/bydeluxe/Lang"].into_iter().collect();
    let mut budget = len;
    assert!(identify_master_enums(&mut archive, &mut budget).is_empty());
    assert_eq!(budget, 0, "Phase A spent the budget");
    assert!(find_binding_classes(&mut archive, &master_classes, &mut budget).is_empty());
    let mut fresh = len;
    assert_eq!(
        find_binding_classes(&mut archive, &master_classes, &mut fresh).len(),
        1
    );
}

// A master enum's own <clinit> getstatics every constant (old-javac $VALUES):
// counting it would set the 40% bar and drop the real binding classes.
#[test]
fn find_binding_classes_skips_the_master_enum_itself() {
    let master_classes: HashSet<&str> = ["LanguageEnum"].into_iter().collect();
    let lang = class_with_getstatic_refs("LanguageEnum", "LanguageEnum", 70);
    let audio = class_with_getstatic_refs("Audio", "LanguageEnum", 6);
    let subs = class_with_getstatic_refs("Subs", "LanguageEnum", 8);
    let zip = build_zip(&[
        ("LanguageEnum.class", lang),
        ("Audio.class", audio),
        ("Subs.class", subs),
    ]);
    let mut archive = open_jar(zip);
    let candidates = find_binding_classes(&mut archive, &master_classes, &mut unbounded());
    let names: Vec<&str> = candidates.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["Subs.class", "Audio.class"]);
}

#[test]
fn find_binding_classes_empty_master_set_yields_no_candidates() {
    let master_classes: HashSet<&str> = HashSet::new();
    let a = class_with_getstatic_refs("A", "LanguageEnum", 100);
    let zip = build_zip(&[("com/bydeluxe/A.class", a)]);
    let mut archive = open_jar(zip);
    assert!(find_binding_classes(&mut archive, &master_classes, &mut unbounded()).is_empty());
}

// ── Phase D: decode_binding (Jar-level short-circuit wrapper) ───────────

// Classes ahead of the binding class in the jar are not inflated, so they cannot
// spend the budget the walk needs for the binding class itself.
#[test]
fn decode_binding_does_not_charge_classes_ahead_of_the_target() {
    let target = class_with_simple_construction("Ignored");
    let need = target.len() as u64;
    let decoy = class_with_getstatic_refs("Ignored2", "LanguageEnum", 40);
    assert!(decoy.len() as u64 > need);
    let zip = build_zip(&[
        ("com/bydeluxe/Decoy.class", decoy),
        ("com/bydeluxe/Target.class", target),
    ]);
    let mut archive = open_jar(zip);
    let mut budget = need;
    let ctors = decode_binding(
        &mut archive,
        "com/bydeluxe/Target.class",
        &lang_enum_master(),
        &mut budget,
    )
    .constructions;
    assert_eq!(ctors.len(), 1);
    assert_eq!(budget, 0);
}

#[test]
fn decode_binding_finds_named_class_and_stops_at_first_match() {
    // Matches by Jar entry path, not `this_class` name. Target path has a
    // real construction; decoy has none. A broken path comparison would
    // match nothing or decode the WRONG entry.
    let target = class_with_simple_construction("Ignored");
    let decoy = class_with_getstatic_refs("Ignored2", "LanguageEnum", 2);
    let zip = build_zip(&[
        ("com/bydeluxe/Target.class", target),
        ("com/bydeluxe/Decoy.class", decoy),
    ]);
    let mut archive = open_jar(zip);
    let master = lang_enum_master();

    let ctors = decode_binding(
        &mut archive,
        "com/bydeluxe/Target.class",
        &master,
        &mut unbounded(),
    )
    .constructions;
    assert_eq!(
        ctors.len(),
        1,
        "expected the Target entry's one construction"
    );
    assert_eq!(ctors[0].binding_type, "AudioSlot");

    // A name with no matching entry must yield nothing (the sweep
    // never finds a Some).
    assert!(
        decode_binding(
            &mut archive,
            "com/bydeluxe/NoSuchClass.class",
            &master,
            &mut unbounded()
        )
        .constructions
        .is_empty()
    );
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
    // A `.class` gated only by path prefix deflates up to the 64 MiB
    // MAX_CLASS_BYTES ceiling (~33M 2-byte `ldc` instructions, each retained
    // as an owned String), so allocation scales with decompressed size.
    const N: usize = 200_000;
    let mut code = Vec::with_capacity(N * 2);
    for _ in 0..N {
        code.push(LDC);
        code.push(4); // cp index 4 -> String -> "English"
    }
    let class = class_with_clinit(ldc_pool("English"), 2, &code);
    let ldcs = clinit_ldc_strings(&class).expect("<clinit> present");
    // Asserted against a LITERAL, not MAX_CLINIT_LDC_STRINGS: comparing to the
    // constant under test passes vacuously once someone raises it. 8192 is
    // double the current cap, allowing the cap to be tuned but not removed.
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
    // `MAX_CLINIT_LDC_BYTES = 256 * 1024`. A `* -> +` mutant collapses the
    // cap to `256 + 1024` (1280), 205x smaller. 1000-byte strings discriminate
    // sharply: correct code retains 262 (262000 bytes); mutant retains only 1.
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

// MAX_CLINIT_LDC_BYTES bounds PER CLASS; this checks the cross-class
// aggregate (255 of 1024 kept, a literal so a raise fails loud).
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

// The byte budget alone still admits millions of map entries when each
// class retains one tiny string (per-entry overhead isn't charged), so
// this checks the entry-count cap binds independently, at 65536.
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

// Headroom check: a jar far larger than any real BD-J title (3000
// classes, 200 bytes of clinit strings each) must be retained in full.
// A cap that rejects real media is a defect in the other direction.
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

// insert rejects on `bytes.saturating_add(cost) > MAX_CANDIDATE_TOTAL_BYTES`,
// so landing EXACTLY on the cap must still be accepted (a `>` -> `>=`
// mutant would reject it); only strictly exceeding it is rejected.
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

// One construction per 11 code bytes reaches millions on a crafted class.
// Offer 5000; expect 4096 (a literal, so raising the constant fails loud).
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

// Headroom: the BD STN_table admits at most 32 primary audio + 32 PG
// streams per playlist, so even several hundred stream slots must survive.
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

// interpret_streams numbers streams with a 1-based u16 counter: unguarded
// `+= 1` wraps/panics past 65535, and `saturating_add` would silently peg
// every overflow stream to the same number. Emission must stop instead.
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
    // ~67M single-byte `iconst_0` fit in a 64 MiB decompressed class, each
    // pushing a StackVal with no depth limit (~2 GiB). Code's max_stack must
    // be honoured: JVMS 4.7.3 requires the operand stack never exceed it.
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

// Minimal ConstantPool for the synthetic bytecode in the tests below:
// entries 1-6 encode LanguageEnum.English (getstatic operand at 6),
// entries 7-12 encode AudioSlot.<init> (invokespecial operand at 12).
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
    // Exercises method-selection and per-method-union truncation logic.
    // Pool must hold "<clinit>"/"()V"/"Code" at 1/2/3 per `class_with_clinit`,
    // and match cp indices 6/8/12 used by the reused construction bytecode.
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
    let constructions = decode_binding_class(&class, &master).constructions;
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
    // Synthetic <clinit>: new AudioSlot (cp 8); dup; getstatic Lang.Eng (cp
    // 6); invokespecial AS.<init>(LLanguageEnum;)V (cp 12).
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
    // <clinit>: iconst_1; new AudioSlot; dup; getstatic Lang.Eng;
    // invokespecial AS.<init>(LLanguageEnum;)V; pop; bipush 42; pop.
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
    // JVMS §3.11.7 `dup` (0x59): duplicate top stack value, checked directly
    // on `decoder.stack`. `new AudioSlot; dup` with no invokespecial must
    // leave exactly two NewObj entries.
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
            StackVal::NewObj(name) => assert_eq!(&**name, "AudioSlot"),
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
    // JVMS §6.5: invokevirtual/invokeinterface pop receiver PLUS args
    // (`extra = 1` for 0xB6/0xB9); invokestatic (0xB8) pops ONLY args. Each
    // case pushes exactly `to_pop` placeholders and checks full drain.
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
    // invokestatic with a leftover value UNDER the args: only the args pop,
    // the leftover survives. Distinguishes a `>` mutant at the `len < to_pop`
    // guard (which would wrongly `clear()` the whole stack).
    assert_eq!(
        run_stack_len(0xB8, "(I)V", 2), // 1 leftover + 1 real arg pushed
        1,
        "only the descriptor's args must be popped, not the whole stack"
    );
}

#[test]
fn binding_decoder_invoke_family_defensively_clears_on_stack_underflow() {
    // If the symbolic stack has FEWER entries than the call needs to pop
    // (malformed bytecode or earlier drift), the decoder must defensively
    // clear rather than underflow-subtract (`len - to_pop` would panic).
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

// The symbolic stack after running `code` (pool: `AnyClass.m()J` at cp 6),
// rendered as ints, "W" (long/double), "?" (opaque) or "new X".
fn stack_after(code: &[u8]) -> Vec<String> {
    let pool = call_ref_pool("()J");
    let master = lang_enum_master();
    let attr = super::super::class_reader::CodeAttribute {
        max_stack: 16,
        max_locals: 0,
        code,
    };
    let mut decoder = BindingDecoder::new(&pool, &master);
    decoder.run(&attr);
    decoder
        .stack
        .iter()
        .map(|v| match v {
            StackVal::Int(n) => n.to_string(),
            StackVal::Wide => "W".into(),
            StackVal::NewObj(c) => format!("new {c}"),
            _ => "?".into(),
        })
        .collect()
}

// JVMS §6.5 dup_x1/dup_x2/dup2/dup2_x1/dup2_x2/swap, every category-1/2 form.
#[test]
fn binding_decoder_models_the_dup_family_and_swap() {
    let (c1, c2, c3, c4, l0) = (ICONST_1, ICONST_2, ICONST_3, ICONST_4, 0x09);
    let cases: &[(&[u8], &[&str])] = &[
        (&[c1, c2, 0x5A], &["2", "1", "2"]),
        (&[c1, c2, c3, 0x5B], &["3", "1", "2", "3"]),
        (&[l0, c1, 0x5B], &["1", "W", "1"]),
        (&[c1, c2, 0x5C], &["1", "2", "1", "2"]),
        (&[l0, 0x5C], &["W", "W"]),
        (&[c1, c2, c3, 0x5D], &["2", "3", "1", "2", "3"]),
        (&[c1, l0, 0x5D], &["W", "1", "W"]),
        (&[c1, c2, c3, c4, 0x5E], &["3", "4", "1", "2", "3", "4"]),
        (&[c1, c2, l0, 0x5E], &["W", "1", "2", "W"]),
        (&[l0, c1, c2, 0x5E], &["1", "2", "W", "1", "2"]),
        (&[l0, l0, 0x5E], &["W", "W", "W"]),
        (&[c1, c2, 0x5F], &["2", "1"]),
    ];
    for (code, want) in cases {
        assert_eq!(stack_after(code), *want, "{code:02x?}");
    }
}

// pop2 removes one long/double or two category-1 values; pop never splits one.
#[test]
fn binding_decoder_pop2_respects_value_categories() {
    assert_eq!(stack_after(&[ICONST_1, 0x09, POP2]), ["1"]);
    assert_eq!(stack_after(&[ICONST_1, 0x0E, POP2]), ["1"]);
    assert_eq!(stack_after(&[ICONST_1, LDC2_W, 0, 1, POP2]), ["1"]);
    // A discarded J-returning call.
    assert_eq!(stack_after(&[ICONST_1, INVOKESTATIC, 0, 6, POP2]), ["1"]);
    assert_eq!(stack_after(&[ICONST_1, ICONST_2, ICONST_3, POP2]), ["1"]);
}

// Local loads push, stores pop (JVMS §6.5 xload/xstore, incl. wide forms).
#[test]
fn binding_decoder_models_local_loads_and_stores() {
    assert_eq!(
        stack_after(&[0x1A, 0x15, 3, 0x2A, 0x19, 3]),
        ["?", "?", "?", "?"]
    );
    assert_eq!(
        stack_after(&[0x1E, 0x16, 3, 0x26, 0x18, 3]),
        ["W", "W", "W", "W"]
    );
    assert_eq!(
        stack_after(&[ICONST_1, ICONST_2, 0x3B, 0x36, 1]),
        [] as [&str; 0]
    );
    assert_eq!(stack_after(&[ICONST_1, 0x09, 0x3F]), ["1"]);
    assert_eq!(
        stack_after(&[0xC4, 0x16, 0, 3, 0xC4, 0x15, 0, 4]),
        ["W", "?"]
    );
    // Array loads pop arrayref + index.
    assert_eq!(stack_after(&[ICONST_1, ICONST_2, 0x2F]), ["W"]);
    assert_eq!(stack_after(&[ICONST_1, ICONST_2, 0x2E]), ["?"]);
}

// Arithmetic, conversions, compares and conditional branches (JVMS §6.5).
#[test]
fn binding_decoder_models_arithmetic_compares_and_branches() {
    assert_eq!(stack_after(&[ICONST_1, ICONST_2, 0x60]), ["?"]);
    assert_eq!(stack_after(&[0x09, 0x0A, 0x61]), ["W"]);
    assert_eq!(stack_after(&[0x0E, 0x0E, 0x6F]), ["W"]);
    assert_eq!(stack_after(&[0x09, ICONST_1, 0x79]), ["W"]);
    assert_eq!(stack_after(&[ICONST_3, 0x74]), ["?"]);
    assert_eq!(stack_after(&[ICONST_1, 0x85]), ["W"]);
    assert_eq!(stack_after(&[0x09, 0x88]), ["?"]);
    assert_eq!(stack_after(&[0x09, 0x0A, 0x94]), ["?"]);
    assert_eq!(stack_after(&[ICONST_1, 0x84, 0, 1]), ["1"]);
    assert_eq!(stack_after(&[ICONST_1, ICONST_2, 0x99, 0, 3]), ["1"]);
    assert_eq!(
        stack_after(&[ICONST_1, ICONST_2, 0x9F, 0, 3]),
        [] as [&str; 0]
    );
    assert_eq!(stack_after(&[ICONST_1, ACONST_NULL, 0xC6, 0, 3]), ["1"]);
    assert_eq!(stack_after(&[ICONST_1, ACONST_NULL, 0xBE]), ["1", "?"]);
}

// A local load feeding a binding ctor stays aligned with the ctor's args.
#[test]
fn binding_decoder_local_load_feeds_ctor_args() {
    let pool = int_ctor_pool();
    let master = lang_enum_master();
    // new; dup; bipush 7; iconst_1; istore_0; lload_1; pop2; invokespecial (I)V
    let code = [
        NEW,
        0,
        2,
        DUP,
        BIPUSH,
        7,
        ICONST_1,
        0x3B,
        0x1F,
        POP2,
        INVOKESPECIAL,
        0,
        6,
    ];
    let attr = super::super::class_reader::CodeAttribute {
        max_stack: 4,
        max_locals: 2,
        code: &code,
    };
    let mut decoder = BindingDecoder::new(&pool, &master);
    decoder.run(&attr);
    assert_eq!(decoder.constructions.len(), 1);
    assert!(matches!(
        decoder.constructions[0].args[..],
        [StackVal::Int(7)]
    ));
    assert_eq!(decoder.drift, 0);
}

// Pool for a single-int-arg constructor `AudioSlot.<init>(I)V`, used to
// isolate each int-push opcode's (iconst_<i>, bipush, sipush, ldc) exact
// produced value, not just "a construction happened".
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
    // JVMS §3.11.3: iconst_<i> pushes exactly i; bipush/sipush sign-extend
    // their operand; ldc of a CONSTANT_Integer pushes that constant. Each is
    // checked as the sole ctor arg so a wrong/missing push arm is visible.
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
    // invokespecial without a preceding `new X; dup`: no construction with
    // args, only a drift event and an argless slot placeholder.
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
    assert!(decoder.constructions.iter().all(|c| c.args.is_empty()));
    assert_eq!(decoder.drift, 1);
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

// A `<init>` whose receiver is not its own `new` lost its operands: it still
// owns its STN slot, so later bindings keep their numbers.
#[test]
fn mismatched_ctor_receiver_keeps_later_slots_aligned() {
    let base = build_simple_pool();
    let mut entries: Vec<CpInfo> = (0..13).filter_map(|i| base.get(i).cloned()).collect();
    entries.extend([
        CpInfo::Utf8("French".into()), // 13
        CpInfo::NameAndType {
            name_index: 13,
            descriptor_index: 4,
        }, // 14
        CpInfo::Fieldref {
            class_index: 2,
            name_and_type_index: 14,
        }, // 15
        CpInfo::Utf8("Other".into()),  // 16
        CpInfo::Class { name_index: 16 }, // 17
    ]);
    let pool = ConstantPool::from_entries(entries);
    let ctor = |lang: u8| {
        [
            NEW,
            0,
            8,
            DUP,
            GETSTATIC,
            0,
            lang,
            INVOKESPECIAL,
            0,
            12,
            POP,
        ]
    };
    let mut code = ctor(6).to_vec();
    // new AudioSlot; dup; new Other; getstatic English; invokespecial AudioSlot.<init>
    code.extend([
        NEW,
        0,
        8,
        DUP,
        NEW,
        0,
        17,
        GETSTATIC,
        0,
        6,
        INVOKESPECIAL,
        0,
        12,
        POP,
        POP,
    ]);
    code.extend(ctor(15));
    let master = lang_enum_master();
    let attr = super::super::class_reader::CodeAttribute {
        max_stack: 4,
        max_locals: 0,
        code: &code,
    };
    let mut decoder = BindingDecoder::new(&pool, &master);
    decoder.run(&attr);
    let labels = interpret_streams(&decoder.constructions, &master);
    let got: Vec<_> = labels
        .iter()
        .map(|l| (l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(got, vec![("eng", 1), ("fra", 3)]);
}

// Two-class Deluxe jar on a disc: a 70-value Language enum and a binding class
// of four `new Slot(Lang.English)`, each preceded by `extra` bytecode.
fn deluxe_disc(extra: &[u8]) -> (crate::udf::fixture::MemDisc, crate::udf::UdfFs) {
    deluxe_disc_with(&[(extra, 4)])
}

// As `deluxe_disc`, over one binding class per `(extra bytecode, binding count)`.
fn deluxe_disc_with(binds: &[(&[u8], usize)]) -> (crate::udf::fixture::MemDisc, crate::udf::UdfFs) {
    use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};
    let mut values = vec!["English", "French", "Spanish", "Dutch"];
    let rest: Vec<String> = (4..70).map(|i| format!("L{i}")).collect();
    values.extend(rest.iter().map(String::as_str));
    let lang = class_with_ldc_strings("com/bydeluxe/Lang", &values);
    let cp = vec![
        CpInfo::Empty,
        CpInfo::Utf8("<clinit>".into()),
        CpInfo::Utf8("()V".into()),
        CpInfo::Utf8("Code".into()),
        CpInfo::Utf8("com/bydeluxe/Lang".into()),
        CpInfo::Class { name_index: 4 },
        CpInfo::Utf8("English".into()),
        CpInfo::Utf8("Lcom/bydeluxe/Lang;".into()),
        CpInfo::NameAndType {
            name_index: 6,
            descriptor_index: 7,
        },
        CpInfo::Fieldref {
            class_index: 5,
            name_and_type_index: 8,
        },
        CpInfo::Utf8("Slot".into()),
        CpInfo::Class { name_index: 10 },
        CpInfo::Utf8("<init>".into()),
        CpInfo::Utf8("(Lcom/bydeluxe/Lang;)V".into()),
        CpInfo::NameAndType {
            name_index: 12,
            descriptor_index: 13,
        },
        CpInfo::Methodref {
            class_index: 11,
            name_and_type_index: 14,
        },
        CpInfo::Utf8("com/bydeluxe/Bind".into()),
        CpInfo::Class { name_index: 16 },
    ];
    let bind_class = |extra: &[u8], count: usize| {
        let mut code = Vec::new();
        for _ in 0..count {
            code.extend([NEW, 0, 11, DUP]);
            code.extend_from_slice(extra);
            code.extend([GETSTATIC, 0, 9, INVOKESPECIAL, 0, 15, POP]);
        }
        code.push(RETURN);
        encode_class(
            &cp,
            17,
            &[MethodSpec {
                name_index: 1,
                descriptor_index: 2,
                code_attr_name_index: 3,
                max_stack: 4,
                code,
            }],
        )
    };
    let mut entries = vec![("com/bydeluxe/Lang.class".to_string(), lang)];
    for (i, (extra, count)) in binds.iter().enumerate() {
        entries.push((
            format!("com/bydeluxe/Bind{i}.class"),
            bind_class(extra, *count),
        ));
    }
    let entries: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(n, b)| (n.as_str(), b.clone()))
        .collect();
    let jar = build_zip(&entries);
    let dir = |name: &str, icb, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: icb + 1,
        files,
        subdirs,
    };
    let jar_dir = dir(
        "JAR",
        30,
        vec![file_with("00000.jar", 32, 4000, jar, true)],
        vec![],
    );
    let root = dir("", 10, vec![], vec![dir("BDMV", 20, vec![], vec![jar_dir])]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

#[test]
fn exact_binding_decode_is_high_confidence() {
    let (mut disc, udf) = deluxe_disc(&[]);
    let r = parse(&mut disc, &udf).expect("deluxe labels");
    assert_eq!(r.labels.len(), 4);
    assert_eq!(r.confidence, super::super::Confidence::High);
}

// An opcode the walker does not model (here `jsr`, which pushes) before a
// binding ctor: its labels may sit on the wrong slots, so never High.
#[test]
fn unmodelled_opcode_before_a_binding_ctor_is_not_high_confidence() {
    let (mut disc, udf) = deluxe_disc(&[0xA8, 0, 3]);
    let r = parse(&mut disc, &udf).expect("deluxe labels");
    assert_eq!(r.confidence, super::super::Confidence::Low);
}

// The inflate budget decides confidence: the smallest budget that still completes
// the parse spends it to zero, so the result is not High; one more byte is.
#[test]
fn a_spent_inflate_budget_is_not_high_confidence() {
    let (mut disc, udf) = deluxe_disc(&[]);
    let (mut lo, mut hi) = (0u64, 1u64 << 20);
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if parse_with_budget(&mut disc, &udf, mid).is_some() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    let tight = parse_with_budget(&mut disc, &udf, hi).expect("completes at the minimum");
    assert_eq!(tight.confidence, super::super::Confidence::Low);
    let roomy = parse_with_budget(&mut disc, &udf, hi + 1).expect("completes");
    assert_eq!(roomy.confidence, super::super::Confidence::High);
}

// Drift in any binding class downgrades the union, whichever class carries it.
#[test]
fn drift_in_any_binding_class_is_not_high_confidence() {
    let drifty: &[u8] = &[0xA8, 0, 3];
    for binds in [[(&[][..], 4), (drifty, 4)], [(drifty, 4), (&[][..], 4)]] {
        let (mut disc, udf) = deluxe_disc_with(&binds);
        let r = parse(&mut disc, &udf).expect("deluxe labels");
        assert_eq!(r.confidence, super::super::Confidence::Low);
    }
}

// The union across binding classes is bounded by the same cap as each walk.
#[test]
fn constructions_across_binding_classes_are_capped() {
    let half = MAX_CONSTRUCTIONS / 2 + 100;
    let (mut disc, udf) = deluxe_disc_with(&[(&[], half), (&[], half)]);
    let r = parse(&mut disc, &udf).expect("deluxe labels");
    assert_eq!(r.labels.len(), MAX_CONSTRUCTIONS);
}

// A control character in a CodingType name or language value never reaches the
// label text raw.
#[test]
fn disc_authored_control_characters_are_escaped_in_labels() {
    let master = MasterEnumTable::from(&[(
        "Language",
        MasterEnum {
            class_name: "LanguageEnum".into(),
            values: vec!["Xx\x1b[31mYy".into()],
            fields: Vec::new(),
        },
    )]);
    let constructions = vec![Construction {
        binding_type: "ng".into(),
        args: vec![
            StackVal::EnumRef {
                kind: "Language",
                ordinal: 0,
            },
            StackVal::CodingType("\x1b]0;pwn\x07ATMOS_AUDIO".into()),
        ],
    }];
    let out = interpret_streams(&constructions, &master);
    assert_eq!(out.len(), 1);
    for text in [&out[0].codec_hint, &out[0].language, &out[0].name] {
        assert!(!text.chars().any(char::is_control), "{text:?}");
    }
    assert!(out[0].codec_hint.contains("ATMOS_AUDIO"));
}

// An "English SDH" Language value carries the qualifier the Purpose enum left unset.
#[test]
fn language_value_supplies_the_qualifier_when_purpose_does_not() {
    let master = MasterEnumTable::from(&[(
        "Language",
        MasterEnum {
            class_name: "LanguageEnum".into(),
            values: vec!["English SDH".into(), "English RNIB".into()],
            fields: Vec::new(),
        },
    )]);
    let slot = |ord: u16| Construction {
        binding_type: "SubtitleSlot".into(),
        args: vec![StackVal::EnumRef {
            kind: "Language",
            ordinal: ord,
        }],
    };
    let out = interpret_streams(&[slot(0), slot(1)], &master);
    assert_eq!(out[0].qualifier, LabelQualifier::Sdh);
    assert_eq!(out[1].qualifier, LabelQualifier::DescriptiveService);
}

// The documented Purpose ordinal mapping, every row: purpose and qualifier.
#[test]
fn deluxe_purpose_every_ordinal_maps_as_documented() {
    use LabelPurpose::*;
    for (ord, want) in [
        (0, Normal),
        (1, Commentary),
        (2, Normal),
        (3, Normal),
        (4, Descriptive),
        (5, Score),
        (6, Normal),
        (7, Descriptive),
        (8, Normal),
    ] {
        assert_eq!(
            deluxe_purpose_to_label(ord),
            (want, LabelQualifier::None),
            "ordinal {ord}"
        );
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

// Each stream binding is one STN slot in STN order, regardless of whether
// the interpreter resolved its language. A slot that resolved nothing
// still consumes its number, or every slot behind it renumbers wrong.
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
        coding_type_to_codec_hint("DTS_HD_AUDIO_XLL"),
        "DTS-HD Master Audio"
    );
    assert_eq!(coding_type_to_codec_hint("LPCM_AUDIO"), "LPCM");
    assert_eq!(coding_type_to_codec_hint("DRA_AUDIO"), "DRA");
    assert_eq!(coding_type_to_codec_hint("DRA_EXTENSION_AUDIO"), "DRA");
}

#[test]
fn graphics_coding_type_binding_is_not_audio() {
    let master = lang_enum_master();
    let mk = |ct: &str| Construction {
        binding_type: "x".into(),
        args: vec![
            StackVal::EnumRef {
                kind: "Language",
                ordinal: 0,
            },
            StackVal::CodingType(ct.into()),
        ],
    };
    let out = interpret_streams(&[mk("PRESENTATION_GRAPHICS"), mk("DTS_AUDIO")], &master);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].stream_type, StreamLabelType::Subtitle);
    assert_eq!(
        (out[1].stream_type, out[1].stream_number),
        (StreamLabelType::Audio, 1)
    );
}

// Interactive-graphics and video bindings have no audio or PG STN slot: with or
// without a resolved language they must not shift later subtitle numbers.
#[test]
fn interactive_graphics_and_video_bindings_take_no_subtitle_slot() {
    let master = lang_enum_master();
    let mk = |ct: &str, lang: Option<u16>| Construction {
        binding_type: "x".into(),
        args: vec![
            lang.map_or(StackVal::Unknown, |ordinal| StackVal::EnumRef {
                kind: "Language",
                ordinal,
            }),
            StackVal::CodingType(ct.into()),
        ],
    };
    let out = interpret_streams(
        &[
            mk("INTERACTIVE_GRAPHICS", Some(2)),
            mk("PRESENTATION_GRAPHICS", Some(0)),
            mk("MPEG4_AVC_VIDEO", Some(2)),
            mk("INTERACTIVE_GRAPHICS", None),
            mk("TEXT_SUBTITLE", Some(1)),
        ],
        &master,
    );
    let got: Vec<_> = out
        .iter()
        .map(|l| (l.stream_type, l.language.as_str(), l.stream_number))
        .collect();
    assert_eq!(
        got,
        vec![
            (StreamLabelType::Subtitle, "eng", 1),
            (StreamLabelType::Subtitle, "fra", 2),
        ]
    );
}

// An unrecognised CodingType name is not decisive: a resolved-language construction
// takes its list from its binding type's siblings, and records nothing itself.
#[test]
fn unknown_coding_type_takes_sibling_kind_and_is_not_recorded() {
    let mk = |bt: &str, ct: &str, ordinal: u16| Construction {
        binding_type: bt.into(),
        args: vec![
            StackVal::EnumRef {
                kind: "Language",
                ordinal,
            },
            StackVal::CodingType(ct.into()),
        ],
    };
    let cs = [
        mk("T", "DTS_AUDIO", 0),
        mk("T", "FUTURE_CODEC_X", 1),
        mk("U", "FUTURE_CODEC_X", 2),
    ];
    assert!(!slot_kinds(&cs).contains_key("U"));
    let out = interpret_streams(&cs[..2], &lang_enum_master());
    let got: Vec<_> = out
        .iter()
        .map(|l| (l.stream_type, l.stream_number))
        .collect();
    assert_eq!(
        got,
        vec![(StreamLabelType::Audio, 1), (StreamLabelType::Audio, 2)]
    );
}

// Bytecode the iterator cannot size (a truncated ldc) ends the walk early: the rest
// goes undecoded, so it counts as drift.
#[test]
fn binding_decoder_counts_early_end_of_walk_as_drift() {
    let code: Vec<u8> = vec![0x00, LDC]; // ldc lacks its operand
    let pool = build_simple_pool();
    let master = lang_enum_master();
    let attr = super::super::class_reader::CodeAttribute {
        max_stack: 4,
        max_locals: 0,
        code: &code,
    };
    let mut decoder = BindingDecoder::new(&pool, &master);
    decoder.run(&attr);
    assert_eq!(decoder.drift, 1);
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

// ── Real-disc fixtures: Universal (studio="uni") — verbatim `.class` files from
// a real `/BDMV/JAR/00000.jar`, exercising Phase A→D against real obfuscated
// bytecode: pd=Language enum, lp=Purpose enum, tl=binding class (np/wb/oq).
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
        values.len().abs_diff(FINGERPRINTS[0].expected_count) <= FINGERPRINTS[0].count_tolerance,
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
    let decoded = decode_binding_class(&tl, &master);
    assert_eq!(decoded.drift, 0, "the real binding table decodes exactly");
    let constructions = decoded.constructions;
    assert!(
        !constructions.is_empty(),
        "tl.<clinit> must yield per-stream constructions"
    );
    // Every `np` audio binding (21) and `wb` subtitle binding (46) must survive
    // the long-constant / array-store stack modelling.
    let count = |t: &str| constructions.iter().filter(|c| c.binding_type == t).count();
    let (np, wb) = (count("np"), count("wb"));
    assert_eq!((np, wb), (21, 46), "np/wb constructions recovered");
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
    // A real config.xml from a Universal Blu-ray release's
    // /BDMV/JAR/99999/config.xml.
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

/// The `studio` anchor is a WHOLE attribute name, not a bare substring: a
/// longer attribute that merely CONTAINS "studio", or the word inside some
/// other attribute's value, must not be read as the studio — and the real
/// `studio="..."` on the same tag must still win.
#[test]
fn studio_anchor_is_a_whole_attribute_not_a_substring() {
    // `substudio` / `studioId` are not `studio`.
    assert_eq!(parse_studio_attr(b"<C substudio=\"nope\">"), None);
    assert_eq!(parse_studio_attr(b"<C studioId=\"nope\">"), None);
    // "studio" inside another attribute's value must be skipped, and the
    // real attribute later on the tag must be found.
    assert_eq!(
        parse_studio_attr(b"<C title=\"a studio film\" studio=\"fox\">").as_deref(),
        Some("fox"),
        "the word 'studio' in a value must not steal a later attribute's quotes"
    );
    // A `studio`-containing value with no real attribute anywhere → None,
    // not the value's own quotes.
    assert_eq!(parse_studio_attr(b"<C title=\"studio ghibli\">"), None);
}

// "studio=" inside another attribute's quoted value is not the attribute, and
// an empty or over-long studio value is skipped rather than ending the scan.
#[test]
fn studio_inside_a_quoted_value_and_bad_values_are_skipped() {
    assert_eq!(
        parse_studio_attr(b"<C title='see studio=\"evil\"' studio=\"fox\">").as_deref(),
        Some("fox")
    );
    assert_eq!(
        parse_studio_attr(
            b"<A studio=\"thisvalueiswaymorethanthirtytwocharacterslong\"> <B studio=\"fox\">"
        )
        .as_deref(),
        Some("fox")
    );
    assert_eq!(
        parse_studio_attr(b"<A studio=\"\"> <B studio=\"uni\">").as_deref(),
        Some("uni")
    );
}

/// A malformed `studio=` candidate whose quote is never closed must be
/// SKIPPED, not abort the scan: a later well-formed `studio="..."` still
/// resolves. Pre-fix the `?` on the missing close-quote returned None for the
/// whole document, dropping the real studio.
#[test]
fn unterminated_studio_quote_is_skipped_and_a_later_studio_resolves() {
    // First `studio="` opens a double-quoted value with NO other `"` until
    // the second attribute; only the single-quoted `studio='uni'` is valid.
    let xml = b"<C studio=\"unclosed studio='uni'>";
    assert_eq!(
        parse_studio_attr(xml).as_deref(),
        Some("uni"),
        "a bad studio candidate must be skipped, not abort the whole scan"
    );
}

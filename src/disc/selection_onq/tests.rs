use super::*;

fn short(out: &mut Vec<u8>, n: u16) {
    out.extend_from_slice(&n.to_be_bytes());
}
fn word(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_be_bytes());
}

fn qco_fixture() -> Vec<u8> {
    let mut b = b"QCOF".to_vec();
    word(&mut b, 5);
    word(&mut b, 0);
    b.push(13);
    b.push(15);
    short(&mut b, 0x8001);
    short(&mut b, 0);
    b.extend_from_slice(&[0, 0]);
    short(&mut b, 0x8001);
    short(&mut b, 0);
    short(&mut b, 0);
    short(&mut b, 1);
    b.push(21);
    word(&mut b, 3);
    short(&mut b, 0);
    let screen = b.len() as u32;
    b[8..12].copy_from_slice(&screen.to_be_bytes());
    b.push(1);
    short(&mut b, 1);
    short(&mut b, 0);
    b.extend_from_slice(&[0; 35]);
    b.push(0);
    short(&mut b, 2);
    short(&mut b, 1);
    short(&mut b, 1);
    short(&mut b, 1);
    b.push(28);
    word(&mut b, 0);
    short(&mut b, 0);
    short(&mut b, 4);
    short(&mut b, 1);
    short(&mut b, 1);
    for n in [273, 274, 275, 0] {
        word(&mut b, n);
    }
    word(&mut b, 8);
    word(&mut b, 4);
    b.extend_from_slice(&[0, 0, 3, 63]);
    b.push(3);
    short(&mut b, 17);
    short(&mut b, 1);
    b.extend_from_slice(&[0; 7]);
    b.push(1);
    short(&mut b, 31);
    short(&mut b, 0);
    short(&mut b, 0x4001);
    b
}

fn qcs_fixture() -> Vec<u8> {
    let mut b = b"QCSF".to_vec();
    word(&mut b, 1);
    word(&mut b, 2);
    word(&mut b, 3);
    word(&mut b, 46);
    word(&mut b, 40);
    word(&mut b, 49);
    short(&mut b, 5);
    b.resize(40, 0);
    for n in [0, 1, 3] {
        short(&mut b, n);
    }
    b.extend_from_slice(&[17, 18, 22]);
    b.push(68);
    short(&mut b, 0xffff);
    short(&mut b, 59);
    b.push(70);
    short(&mut b, 265);
    short(&mut b, 0);
    b.extend_from_slice(b"label\0");
    let metadata_at = b.len() as u16;
    b[32..34].copy_from_slice(&metadata_at.to_be_bytes());
    for target in [1, 15, 31] {
        word(&mut b, 0);
        short(&mut b, target);
        short(&mut b, 1);
    }
    b
}

fn qcd_fixture() -> Vec<u8> {
    let mut b = b"QCDF".to_vec();
    word(&mut b, 4);
    b.resize(28, 0);
    b[16..20].copy_from_slice(&28u32.to_be_bytes());
    b[26..28].copy_from_slice(&2u16.to_be_bytes());
    word(&mut b, 28);
    word(&mut b, 0);
    b.extend_from_slice(&[4, 0, 0]);
    b.extend_from_slice(b"table\0file:///jar/table.qcs\0");
    let next = b.len() as u32 - 8;
    b[32..36].copy_from_slice(&next.to_be_bytes());
    b.extend_from_slice(&[64, 0, 0]);
    b.extend_from_slice(b"video\0bd://PLAYLIST:00166.ITEM:0.V1:1\0");
    b.extend_from_slice(&[0; 14]);
    b
}

#[test]
fn parses_typed_qco_without_conferring_authority() {
    let b = qco_fixture();
    let p = qco::parse(&b).unwrap();
    assert_eq!(p.global.id, 0x8001);
    assert_eq!(p.screen.id, 2);
    assert_eq!(p.global.variables, [qco::Value::Integer(3)]);
    assert_eq!(
        p.screen.variables,
        [qco::Value::Integers {
            rows: 4,
            cols: 1,
            values: vec![273, 274, 275, 0]
        }]
    );
    assert_eq!(p.screen.functions, [&[0, 0, 3, 63][..]]);
    assert_eq!(
        p.screen.objects[1].events,
        [qco::Event {
            event: 31,
            target: 0,
            function: 0x4001
        }]
    );
    assert_eq!(p.screen.objects[1].payload, [0; 7]);
    assert_eq!(p.screen.end, b.len());
    assert_eq!(
        assess(&b, &qcs_fixture(), &qcd_fixture()),
        Assessment::ReviewRequired(Reject::Unproven)
    );
}

#[test]
fn typed_fill_allowance_is_exact_and_does_not_allow_aliases_or_direct_writes() {
    use std::collections::BTreeSet;
    let bytes = qco_fixture();
    let mut program = qco::parse(&bytes).unwrap();
    let fill = [
        1, 0, 0x19, 0x20, 1, 0x46, 3, 0x4b, 1, 0x0f, 0x45, 0, 0x0e, 0x20, 2, 0x20, 1, 0x1a, 3,
        0x41, 1, 1, 0x14, 0xff, 0xec, 0x3f,
    ];
    let mut wrapper = [
        0, 0, 0x2a, 2, 0x40, 1, 0x47, 3, 0xc0, 1, 0x2c, 3, 0xc0, 1, 0x16, 2, 2, 0x40, 2, 0x47, 3,
        0xc0, 1, 0x2c, 3, 0xc0, 1, 0x16, 2, 2, 0x40, 3, 0x47, 3, 0xc0, 1, 0x2c, 3, 0xc0, 1, 0x16,
        2, 0x3f,
    ];
    wrapper[22] = 2;
    wrapper[35] = 3;
    program.global.variables = (0..3).map(|_| qco::Value::Integer(1)).collect();
    program.global.functions = vec![&fill];
    program.screen.functions = vec![&wrapper];
    program.screen.variables = (0..3)
        .map(|_| qco::Value::Integers {
            rows: 1,
            cols: 1,
            values: vec![0],
        })
        .collect();
    let allowed = BTreeSet::from([0x4001]);
    let check = |p: &qco::Program<'_>| {
        invariants::array_candidates_with_typed_fills(p, &[0x4001], &allowed)
    };
    assert!(invariants::array_candidates(&program, &[0x4001]).is_err());
    assert!(check(&program).is_ok());
    assert!(
        invariants::array_candidates_with_typed_fills(
            &program,
            &[0x4001],
            &BTreeSet::from([0x4002])
        )
        .is_err()
    );
    assert!(
        invariants::array_candidates_with_typed_fills(
            &program,
            &[0x4001],
            &BTreeSet::from([0x4001, 0x4002])
        )
        .is_err()
    );
    let direct = [0, 0, 0, 1, 8, 1, 0, 2, 0x40, 1, 0x26, 0x3f];
    program.screen.functions.push(&direct);
    assert!(check(&program).is_err());
    let alias = [0, 0, 0, 1, 0, 2, 0x40, 2, 0x2e, 0x3f];
    program.screen.functions[1] = &alias;
    assert!(check(&program).is_err());
    program.screen.functions.pop();
    let mut bad_wrapper = wrapper;
    bad_wrapper[15] = 1; // wrong DROP in otherwise identical wrapper
    program.screen.functions[0] = &bad_wrapper;
    assert!(check(&program).is_err());
    program.screen.functions[0] = &wrapper;
    program.screen.variables[2] = qco::Value::Integer(0);
    assert!(check(&program).is_err());
}

#[test]
fn array_invariants_reject_backing_slot_aliases_and_unknown_writers() {
    let bytes = qco_fixture();
    let mut program = qco::parse(&bytes).unwrap();
    program.screen.variables.push(qco::Value::Integers {
        rows: 1,
        cols: 1,
        values: vec![0],
    });
    assert!(invariants::array_candidates(&program, &[0x4001]).is_ok());
    // A scalar write to a different array slot can alias its backing storage.
    let alias = [0, 0, 0, 1, 0, 2, 0x40, 2, 0x2e, 0x3f];
    program.screen.functions[0] = &alias;
    assert!(invariants::array_candidates(&program, &[0x4001]).is_err());
    // Direct cell writes to a distinct, unaliased typed array remain legal.
    let other_array = [0, 0, 0, 1, 8, 1, 0, 2, 0x40, 2, 0x26, 0x3f];
    program.screen.functions[0] = &other_array;
    assert!(invariants::array_candidates(&program, &[0x4001]).is_ok());
    let protected_array = [0, 0, 0, 1, 8, 1, 0, 2, 0x40, 1, 0x26, 0x3f];
    program.screen.functions[0] = &protected_array;
    assert!(invariants::array_candidates(&program, &[0x4001]).is_err());
    // Reading a destination from a local is unresolved, not a harmless write.
    let dynamic = [0, 0, 0, 0x20, 1, 0x2e, 0x3f];
    program.screen.functions[0] = &dynamic;
    assert!(invariants::array_candidates(&program, &[0x4001]).is_err());
    let callback_dynamic = [0, 0, 0, 0x20, 1, 1, 17, 0x49, 3, 0x3f];
    program.screen.functions[0] = &callback_dynamic;
    assert!(invariants::array_candidates(&program, &[0x4001]).is_err());
}

#[test]
fn table_effects_validate_every_row_and_reject_resource_callback_and_type_aliases() {
    let bytes = qco_fixture();
    let mut p = qco::parse(&bytes).unwrap();
    p.screen.variables = (0..8)
        .map(|i| {
            if matches!(i, 1 | 2) {
                qco::Value::String(b"")
            } else {
                qco::Value::Integer(0)
            }
        })
        .collect();
    p.screen.objects.truncate(1);
    for (id, kind, payload) in [
        (3, 12, &[][..]),
        (20, 8, &[2, 255, 255, 0, 1][..]),
        (10, 3, &[0; 7][..]),
        (11, 3, &[0; 7][..]),
        (12, 3, &[0; 7][..]),
        (13, 3, &[0; 7][..]),
    ] {
        p.screen.objects.push(qco::Object {
            span: 0..0,
            id,
            parent: 1,
            kind,
            payload,
            events: vec![],
        });
    }
    // Independent straight-line emitter, with synthetic variable/resource IDs.
    let mut code = vec![0, 0, 189];
    for col in 0..13 {
        let string = matches!(col, 6 | 7);
        code.extend_from_slice(&[
            32,
            1,
            1,
            col,
            32,
            2,
            23,
            if string { 2 } else { 1 },
            2,
            if string { 53 } else { 52 },
        ]);
        match col {
            0 => code.extend_from_slice(&[4, 0, 2, 0, 1, 50, 13]),
            2..=4 => code.extend_from_slice(&[1, 3, 73, [100, 90, 104][usize::from(col - 2)]]),
            5 => code.extend_from_slice(&[2, 0, 20, 50, 2]),
            _ => {
                let variable = match col {
                    1 => 1,
                    6 => 2,
                    7 => 3,
                    _ => col - 4,
                };
                code.extend_from_slice(&[2, 64, variable, if string { 47 } else { 46 }]);
            }
        }
    }
    code.push(63);
    let modes = qcs::Table {
        program: None,
        targets: vec![(1, 1), (10, 1), (11, 1), (12, 1), (13, 1)],
        rows: vec![vec![
            qcs::Cell::Integer(0),
            qcs::Cell::Integer(0),
            qcs::Cell::Integer(0),
            qcs::Cell::Integer(0),
            qcs::Cell::Integer(0),
        ]],
    };
    let mut rows = Vec::new();
    for _ in 0..4 {
        rows.push(
            (0..13)
                .map(|col| {
                    if matches!(col, 6 | 7) {
                        qcs::Cell::String(b"")
                    } else {
                        qcs::Cell::Integer(match col {
                            0 => 2,
                            2 => 0x4001,
                            _ => 0,
                        })
                    }
                })
                .collect(),
        );
    }
    let mut table = qcs::Table {
        program: Some(&code),
        targets: vec![],
        rows,
    };
    let resources = vec![
        qcd::Resource {
            id: 1,
            kind: 4,
            flags: 0,
            name: b"",
            locator: b"file:///jar/m.qcs",
            parameters: &[],
        },
        qcd::Resource {
            id: 2,
            kind: 64,
            flags: 0,
            name: b"",
            locator: b"bd://PLAYLIST:00042.ITEM:0.V1:1",
            parameters: &[0; 14],
        },
    ];
    let result = table_effects::sequencer(&p.screen, &table, &modes, &resources).unwrap();
    assert_eq!(result.resources.into_iter().collect::<Vec<_>>(), [2]);
    for (column, bad) in [(0, 3), (2, 0xc001), (5, 1)] {
        let old = std::mem::replace(&mut table.rows[3][column], qcs::Cell::Integer(bad));
        assert!(table_effects::sequencer(&p.screen, &table, &modes, &resources).is_err());
        table.rows[3][column] = old;
    }
    p.screen.variables[0] = qco::Value::Integers {
        rows: 1,
        cols: 1,
        values: vec![0],
    };
    assert!(table_effects::sequencer(&p.screen, &table, &modes, &resources).is_err());
}

#[test]
fn parses_qcs_signed_numbers_unsigned_bytes_and_strings() {
    let b = qcs_fixture();
    let t = qcs::parse(&b).unwrap();
    assert!(t.program.is_none());
    assert_eq!(t.targets, [(1, 1), (15, 1), (31, 1)]);
    assert_eq!(
        t.rows,
        vec![
            vec![
                qcs::Cell::Integer(68),
                qcs::Cell::Integer(-1),
                qcs::Cell::String(b"label")
            ],
            vec![
                qcs::Cell::Integer(70),
                qcs::Cell::Integer(265),
                qcs::Cell::String(b"")
            ]
        ]
    );
}

#[test]
fn qcs_embedded_program_cannot_be_referenced_as_string_data() {
    let mut b = qcs_fixture();
    let program_at = u16::from_be_bytes([b[32], b[33]]) as usize;
    b.truncate(program_at);
    b[30..32].copy_from_slice(&2u16.to_be_bytes());
    b[32..34].copy_from_slice(&(program_at as u16).to_be_bytes());
    let program = [0; 24];
    b.extend_from_slice(&program);
    assert_eq!(qcs::parse(&b).unwrap().program, Some(&program[..]));
    b[52..54].copy_from_slice(&(program_at as u16).to_be_bytes());
    assert!(qcs::parse(&b).is_err());
}

#[test]
fn qcs_metadata_and_midstring_aliases_are_not_string_data() {
    let b = qcs_fixture();
    let metadata_at = u16::from_be_bytes([b[32], b[33]]);
    for pointer in [60, metadata_at, metadata_at + 4] {
        let mut bad = b.clone();
        bad[52..54].copy_from_slice(&pointer.to_be_bytes());
        assert!(qcs::parse(&bad).is_err());
    }
    for end in metadata_at as usize..b.len() {
        assert!(qcs::parse(&b[..end]).is_err());
    }
}

#[test]
#[ignore = "local extracted authoring asset; no media or JVM execution"]
fn local_peii_structure_candidate() {
    use std::io::Read;
    let path = std::env::var("ONQ_TEST_JAR").expect("explicit local extracted JAR path");
    let mut zip = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
    let mut read = |name| {
        let mut b = Vec::new();
        zip.by_name(name)
            .unwrap()
            .take((binary::MAX_BYTES + 1) as u64)
            .read_to_end(&mut b)
            .unwrap();
        b
    };
    let fs = read("FS.QCO");
    let table = read("VideoSequencer_res.qcs");
    let resources = read("RESDIR.QCD");
    let mode_bytes = read("stblMode_res.qcs");
    let p = qco::parse(&fs).unwrap();
    for (group, functions) in [
        ("global", &p.global.functions),
        ("screen", &p.screen.functions),
    ] {
        let mut indirect = 0;
        let mut unresolved_global = Vec::new();
        for (index, body) in functions.iter().enumerate() {
            let instructions = bytecode::decode(body)
                .unwrap_or_else(|error| panic!("{group} fn{}: {error:?}", index + 1));
            for instruction in &instructions {
                if matches!(instruction.opcode, 0x40 | 0x49 | 0x4a) {
                    eprintln!(
                        "{group} fn{} escape opcode{:x} at{}",
                        index + 1,
                        instruction.opcode,
                        instruction.pc
                    );
                }
            }
            for (i, instruction) in instructions.iter().enumerate() {
                if instruction.opcode == 0x32 && instruction.operands == [1] {
                    let prefix: Vec<_> = instructions[i.saturating_sub(3)..i]
                        .iter()
                        .map(|ins| (ins.opcode, ins.integer(), ins.operands))
                        .collect();
                    eprintln!(
                        "{group} fn{} property1 at{} prefix{prefix:?}",
                        index + 1,
                        instruction.pc
                    );
                }
            }
            let array_stores: Vec<_> = instructions
                .iter()
                .filter(|i| {
                    matches!(
                        i.opcode,
                        0x1a | 0x1b | 0x1e | 0x1f | 0x26 | 0x27 | 0x2a | 0x2b
                    )
                })
                .map(|i| i.pc)
                .collect();
            if !array_stores.is_empty() {
                eprintln!("{group} fn{} array stores {array_stores:?}", index + 1);
            }
            for pair in instructions.windows(2) {
                if pair[1].opcode == 0x16
                    && (pair[0].integer().is_none() || pair[0].integer() == Some(0xc047))
                {
                    eprintln!(
                        "{group} fn{} call71/indirect at{} target{:?}",
                        index + 1,
                        pair[1].pc,
                        pair[0].integer()
                    );
                }
                if pair[1].opcode == 0x47
                    && pair[0]
                        .integer()
                        .is_some_and(|id| [0x418d, 0x4198, 0x4199].contains(&id))
                {
                    eprintln!(
                        "{group} fn{} protected address passed at{}",
                        index + 1,
                        pair[1].pc
                    );
                }
            }
            for effect in bytecode::writes(&instructions) {
                if effect.literal_destination.is_none()
                    && matches!(
                        effect.opcode,
                        0x26 | 0x27 | 0x2a | 0x2b | 0x2e | 0x2f | 0x42
                    )
                {
                    unresolved_global.push((index + 1, effect.pc, effect.opcode));
                }
                if matches!(effect.opcode, 0x1a | 0x1b | 0x1e | 0x1f | 0x3a | 0x3b) {
                    eprintln!(
                        "{group} fn{} localalias mutation at{} opcode{:x}",
                        index + 1,
                        effect.pc,
                        effect.opcode
                    );
                }
                if effect.literal_destination.is_none() {
                    indirect += 1;
                }
                if effect
                    .literal_destination
                    .is_some_and(|id| [0x418d, 0x4198, 0x4199].contains(&id))
                {
                    panic!(
                        "protected literal write {group} fn{} at{} opcode{:x}",
                        index + 1,
                        effect.pc,
                        effect.opcode
                    );
                }
            }
        }
        eprintln!("{group} unresolved global destinations {unresolved_global:?}");
        eprintln!(
            "{group}: {} functions fully tokenized, {indirect} unresolved/local write sites (not discharged)",
            functions.len()
        );
    }
    let candidates: Vec<_> = (1..=p.screen.functions.len())
        .filter_map(|id| template::selected_action_rows(&p.screen, id).ok())
        .collect();
    assert!(candidates.iter().any(|c| c.rows == [2, 3, 4]));
    // global45 retains its string argument across call16(drop=0), then 3e
    // drops it explicitly. Treating drop=0 as argc=0 rejects valid authoring.
    frames::check_call(p.global.functions[1], 1, 0).unwrap();
    assert!(frames::check_call(p.global.functions[1], 0, 0).is_err());
    assert_eq!(
        template::selection_count(&p.screen, 304, 310).unwrap(),
        (0x4190, 3)
    );
    template::index_clamp(p.screen.functions[42]).unwrap();
    let previous = template::navigation_handler(p.screen.functions[302], false).unwrap();
    let next = template::navigation_handler(p.screen.functions[304], true).unwrap();
    assert_eq!(previous, next);
    let selection = template::selection_handler(p.screen.functions[303]).unwrap();
    for role in ["s:v_buttons", "s:v_selected"] {
        assert_eq!(selection[role], previous[role]);
    }
    let show = template::visibility_helper(p.screen.functions[49], true).unwrap();
    let hide = template::visibility_helper(p.screen.functions[50], false).unwrap();
    for (role, value) in hide {
        assert_eq!(show.get(role), Some(&value));
    }
    let invariant = invariants::array_candidates(&p, &[0x418d, 0x4198, 0x4199]).unwrap();
    eprintln!("array invariants: {invariant:?}");
    assert!(!invariant.fills.is_empty());
    assert!(invariant.screen_loads.is_empty());
    assert_eq!(invariant.callbacks.len(), 4);
    let mut t = qcs::parse(&table).unwrap();
    assert_eq!(t.program.unwrap().len(), 190);
    template::table_program(t.program.unwrap()).unwrap();
    let capacity = qcd::stack_capacity(&resources).unwrap();
    assert_eq!(capacity, 772);
    // Local normal-path peak only. Mode/native exception and re-entry effects
    // remain independent obligations, never silently inferred from this value.
    let frame = frames::table_frame(&t, None).unwrap();
    assert_eq!(frames::peaks(&[frame], capacity).unwrap(), [4]);
    let original = std::mem::replace(&mut t.rows[0][6], qcs::Cell::Integer(0));
    assert!(frames::table_frame(&t, None).is_err());
    t.rows[0][6] = original;
    let r = qcd::parse(&resources).unwrap();
    let modes = qcs::parse(&mode_bytes).unwrap();
    let effects = table_effects::sequencer(&p.screen, &t, &modes, &r).unwrap();
    assert_eq!(effects.mode_object, 311);
    assert_eq!(effects.mode_resource, 55);
    assert!(effects.callbacks.contains(&16805));
    assert!(effects.resources.contains(&68));
    for (row, locator) in [
        (2, b"bd://PLAYLIST:00166.ITEM:0.V1:1"),
        (3, b"bd://PLAYLIST:00167.ITEM:0.V1:1"),
        (4, b"bd://PLAYLIST:00168.ITEM:0.V1:1"),
    ] {
        let qcs::Cell::Integer(id) = t.rows[row][0] else {
            panic!("integer resource");
        };
        assert_eq!(r[id as usize - 1].locator, locator);
    }
    // This is deliberately not a full producer success test.
    assert_eq!(
        assess(&fs, &table, &resources),
        Assessment::ReviewRequired(Reject::Unproven)
    );
}

#[test]
fn qcd_ids_are_one_based_and_locators_are_not_reinterpreted() {
    let b = qcd_fixture();
    let r = qcd::parse(&b).unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].id, 1);
    assert_eq!(r[1].id, 2);
    assert_eq!(r[0].kind, 4);
    assert_eq!(r[0].flags, 0);
    assert_eq!(r[0].name, b"table");
    assert_eq!(r[1].locator, b"bd://PLAYLIST:00166.ITEM:0.V1:1");
    assert!(r[0].parameters.is_empty());
    assert_eq!(r[1].parameters.len(), 14);
}

#[test]
fn qcd_playback_fields_are_retained_for_timeline_verification() {
    let mut bytes = qcd_fixture();
    let tail = bytes.len() - 14;
    let fields = [3, 113, 93, 192, 3, 233, 0, 46, 216, 115, 0, 0, 0, 0];
    bytes[tail..].copy_from_slice(&fields);
    let resources = qcd::parse(&bytes).unwrap();
    assert_eq!(resources[1].parameters, fields);
    // A timing/selection mutation must remain visible to the consumer.
    bytes[tail + 13] = 1;
    assert_ne!(qcd::parse(&bytes).unwrap()[1].parameters, fields);
}

#[test]
fn qcd_table_kind_and_semantic_flags_are_retained() {
    let mut b = qcd_fixture();
    b[36] = 5;
    b[37..39].copy_from_slice(&0x10u16.to_be_bytes());
    let r = qcd::parse(&b).unwrap();
    assert_eq!(r[0].kind, 5);
    assert_eq!(r[0].flags, 0x10);
    // Retaining flags is parsing only, never permission to ignore their effects.
    assert_eq!(
        assess(&qco_fixture(), &qcs_fixture(), &b),
        Assessment::ReviewRequired(Reject::Unproven)
    );
}

#[test]
fn every_truncated_prefix_rejects() {
    let b = qco_fixture();
    for n in 0..b.len() {
        assert!(qco::parse(&b[..n]).is_err(), "qco {n}");
    }
    let b = qcs_fixture();
    for n in 0..b.len() {
        assert!(qcs::parse(&b[..n]).is_err(), "qcs {n}");
    }
    let b = qcd_fixture();
    for n in 0..b.len() {
        assert!(qcd::parse(&b[..n]).is_err(), "qcd {n}");
    }
}

#[test]
fn rejects_overflow_layout_and_unsupported_types() {
    let mut b = qcs_fixture();
    b[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(qcs::parse(&b).is_err());
    let mut b = qcs_fixture();
    b[46] = 255;
    assert!(qcs::parse(&b).is_err());
    let mut b = qcs_fixture();
    b[42..44].copy_from_slice(&0u16.to_be_bytes());
    assert!(qcs::parse(&b).is_err());
    let mut b = qcs_fixture();
    b[52..54].copy_from_slice(&1u16.to_be_bytes());
    assert!(qcs::parse(&b).is_err());
    let mut b = qcd_fixture();
    b[32..36].copy_from_slice(&28u32.to_be_bytes());
    assert!(qcd::parse(&b).is_err());
    let mut b = qco_fixture();
    b[28] = 255;
    assert!(qco::parse(&b).is_err());
}

#[test]
fn rejects_oversized_inputs_and_cursor_overflow() {
    let b = vec![0; binary::MAX_BYTES + 1];
    assert!(binary::Reader::new(&b).is_err());
    let mut r = binary::Reader::new(&[]).unwrap();
    r.pos = usize::MAX;
    assert!(r.take(1).is_err());
}

#[test]
fn unsupported_versions_and_empty_input_never_accept() {
    for bytes in [vec![], b"QCOF\0\0\0\x06".to_vec()] {
        assert!(matches!(
            assess(&bytes, &qcs_fixture(), &qcd_fixture()),
            Assessment::ReviewRequired(_)
        ));
    }
}

#[test]
fn qco_spans_are_absolute_and_negative_dimensions_reject() {
    let b = qco_fixture();
    let p = qco::parse(&b).unwrap();
    assert_eq!(p.global.span.start, 13);
    assert_eq!(p.global.span.end, p.screen.span.start);
    assert_eq!(
        &b[p.screen.function_spans[0].clone()],
        p.screen.functions[0]
    );
    assert_eq!(b[p.screen.objects[1].span.start], 3);
    let array = p.screen.variable_spans[0].start;
    for offset in [array, array + 2] {
        for negative in [-1_i16, i16::MIN] {
            let mut changed = b.clone();
            changed[offset..offset + 2].copy_from_slice(&negative.to_be_bytes());
            assert!(qco::parse(&changed).is_err());
        }
    }
}

#[test]
fn qco_string_pool_rejects_header_code_objects_and_middle_aliases() {
    let mut b = qco_fixture();
    let p = qco::parse(&b).unwrap();
    let code = p.screen.function_spans[0].start;
    let object = p.screen.objects[1].span.start;
    let pool = b.len();
    b.extend_from_slice(b"entry\0");
    b[28] = 22;
    b[29..33].copy_from_slice(&(pool as u32).to_be_bytes());
    assert!(qco::parse(&b).is_ok());
    for pointer in [1, code, code + 2, object, pool + 1] {
        let mut changed = b.clone();
        changed[29..33].copy_from_slice(&(pointer as u32).to_be_bytes());
        assert!(qco::parse(&changed).is_err(), "pointer {pointer}");
    }
    b.pop();
    assert!(qco::parse(&b).is_err());
}

#[test]
fn qco_string_pool_entry_budget_and_array_pointer_validation() {
    let mut b = qco_fixture();
    b.extend(std::iter::repeat_n(0, binary::MAX_RECORDS + 1));
    assert!(matches!(qco::parse(&b), Err(Reject::Budget)));

    let mut b = qco_fixture();
    let p = qco::parse(&b).unwrap();
    let array = p.screen.variable_spans[0].start;
    let kind = array - 7; // one type, one value word, ignored short
    let pool = b.len();
    b[kind] = 29;
    for cell in 0..4 {
        b[array + 6 + cell * 4..array + 10 + cell * 4].copy_from_slice(&0u32.to_be_bytes());
    }
    b.extend_from_slice(b"valid\0");
    b[array + 6..array + 10].copy_from_slice(&(pool as u32).to_be_bytes());
    assert!(qco::parse(&b).is_ok());
    for pointer in [1, array, pool + 1] {
        let mut changed = b.clone();
        changed[array + 6..array + 10].copy_from_slice(&(pointer as u32).to_be_bytes());
        assert!(qco::parse(&changed).is_err());
    }
}

#[test]
fn qco_table_object_preserves_flags_initial_row_and_resource() {
    let mut b = qco_fixture();
    let p = qco::parse(&b).unwrap();
    let start = p.screen.objects[1].span.start;
    b.truncate(start);
    // Type 8, symbolic object ID 310, parent 1, flags 2, initial row -1,
    // one-based resource 66, zero event bindings. Flags choose fi, not sj.
    b.extend_from_slice(&[8, 1, 54, 0, 1, 2, 255, 255, 0, 66, 0]);
    let parsed = qco::parse(&b).unwrap();
    let table = &parsed.screen.objects[1];
    assert_eq!(table.kind, 8);
    assert_eq!(table.id, 310);
    assert_eq!(table.payload, [2, 255, 255, 0, 66]);
    assert_eq!(table.span, start..b.len());
    for n in start..b.len() {
        assert!(qco::parse(&b[..n]).is_err());
    }
}

use super::super::{Reject, assets, qco};

fn cached_assets() -> assets::Assets {
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    assets::load(|path, limit| {
        let path = jar.parent().unwrap().join(path.rsplit('/').next().unwrap());
        let file = std::fs::File::open(path).map_err(|_| Reject::MissingAsset)?;
        let mut bytes = Vec::new();
        std::io::Read::take(file, limit as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Reject::Invalid)?;
        Ok(bytes)
    })
    .unwrap()
}

use std::io::Read;

#[test]
#[ignore = "cached authored style resources only; no media or JVM execution"]
fn selected_style_fill_rejects_wrong_resources_and_mutable_sources() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let resource_bytes = assets.authored_file("RESDIR.QCD").unwrap();
    let resources = super::qcd::parse(&resource_bytes).unwrap();
    let writer = [0, 0, 0, 1, 0, 3, 0xc0, 23, 0x2e, 0x3f];
    for mutation in 0..5 {
        let mut program = qco::parse(&bytes).unwrap();
        match mutation {
            1 => program.global.variables[22] = qco::Value::Integer(36),
            2 => program.screen.functions.push(&writer),
            3 => program.global.variables[24] = qco::Value::Integer(i32::MAX),
            4 => {
                let qco::Value::Integers { values, .. } = &mut program.screen.variables[421] else {
                    panic!("bitmap array");
                };
                values[0] = 29;
            }
            _ => {}
        }
        let result = super::selection_resources(&program, 0x4130, &resources);
        assert_eq!(
            result.is_ok(),
            mutation == 0,
            "mutation {mutation}: {result:?}"
        );
    }
}

#[test]
#[ignore = "bounded cached startup data; no media or JVM execution"]
fn startup_timing_bounds_reject_bad_dimensions_and_arithmetic() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&bytes).unwrap();
    super::super::startup_data::timing_tables(&program.global).unwrap();
    for mutation in 0..5 {
        let mut program = qco::parse(&bytes).unwrap();
        match mutation {
            0 => program.global.variables[642] = qco::Value::Integer(i32::MAX),
            1 => program.global.variables[237] = qco::Value::Integer(i32::MAX),
            2 => {
                program.global.variables[10] = qco::Value::Integers {
                    rows: 1,
                    cols: 1,
                    values: vec![0],
                }
            }
            3 => {
                program.global.variables[653] = qco::Value::Integers {
                    rows: 0,
                    cols: 3,
                    values: vec![],
                }
            }
            _ => {
                program.global.variables[651] = qco::Value::Integers {
                    rows: 3,
                    cols: 2,
                    values: vec![0, i32::MAX, 1, i32::MIN, 2, 0],
                }
            }
        }
        assert!(
            super::super::startup_data::timing_tables(&program.global).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
#[ignore = "read-only cached authoring code; no disc or JVM execution"]
fn actual_menu_contract_preserves_authored_roles_and_order() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&bytes).unwrap();
    let menu = super::menu_contract(&program).unwrap();
    assert_eq!(menu.object, 265);
    assert_eq!(menu.selection, 0x4130);
    assert_eq!(menu.count_setup, 0x4136);
    assert_eq!(menu.rows, [2, 3, 4]);
    assert_eq!(menu.playback, 94);
    let setter = super::template::playback_row_setter(program.screen.functions[390]).unwrap();
    assert_eq!(setter["o:o_sequencer"], 310);
    let mut changed = program.screen.functions[390].to_vec();
    changed[39] = 3;
    assert!(super::template::playback_row_setter(&changed).is_err());
    assert_eq!(
        super::table_contract(&assets, &program, &menu).unwrap(),
        [166, 167, 168]
    );
}

#[test]
#[ignore = "read-only cached authoring code; no disc or JVM execution"]
fn menu_contract_rejects_role_aliases_extra_writers_and_invalid_initial_state() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let extra_write = [0, 0, 0, 1, 9, 2, 0x41, 0x90, 0x2e, 0x3f];
    for mutation in 0..5 {
        let mut program = qco::parse(&bytes).unwrap();
        match mutation {
            0 => program.screen.variables[399] = qco::Value::Integer(4),
            1 => program.screen.variables[400] = qco::Value::Integer(-1),
            2 => program.screen.functions.push(&extra_write),
            3 => {
                let menu = program
                    .screen
                    .objects
                    .iter_mut()
                    .find(|object| object.id == 265)
                    .unwrap();
                menu.events
                    .iter_mut()
                    .find(|event| event.event == 29)
                    .unwrap()
                    .function = 0x4131;
            }
            _ => program.screen.variables[408] = qco::Value::Integer(61),
        }
        assert!(
            super::menu_contract(&program).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
#[ignore = "bounded cached playback code; no media or JVM execution"]
fn playback_contract_rejects_chapter_requests_and_redirected_setters() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    for mutation in 0..3 {
        let mut program = qco::parse(&bytes).unwrap();
        let menu = super::menu_contract(&program).unwrap();
        let slot = if mutation == 2 { 93 } else { 388 };
        let mut changed = program.screen.functions[slot].to_vec();
        match mutation {
            0 => changed[30..32].copy_from_slice(&[0, 0]), // Explicit chapter zero.
            1 => changed[36] ^= 1,                         // Redirect one row-setter branch.
            _ => changed[7] ^= 1,                          // Redirect the menu's forwarding call.
        }
        program.screen.functions[slot] = &changed;
        assert!(
            super::table_contract(&assets, &program, &menu).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
#[ignore = "bounded cached root data; no media or JVM execution"]
fn canonical_root_rejects_popup_bypass_and_mutable_episode_slot() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    for mutation in 0..4 {
        let mut program = qco::parse(&bytes).unwrap();
        match mutation {
            0 => {
                let qco::Value::Integers { values, .. } = &mut program.screen.variables[92] else {
                    panic!()
                };
                values.swap(0, 1);
            }
            1 => {
                for slot in [74, 75] {
                    let qco::Value::Integers { values, .. } = &mut program.screen.variables[slot]
                    else {
                        panic!()
                    };
                    values.swap(1, 2);
                }
            }
            2 => program.screen.variables[72] = qco::Value::Integer(9),
            _ => {
                let qco::Value::Integers { values, .. } = &mut program.screen.variables[71] else {
                    panic!()
                };
                values[1] = 39;
            }
        }
        assert!(
            super::canonical_roots(&program, 265).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
#[ignore = "bounded cached callback table; no media or JVM execution"]
fn canonical_callbacks_reject_old_menu_handler_and_changed_episode_state() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&bytes).unwrap();
    let table_bytes = assets.authored_file("VideoSequencer_res.qcs").unwrap();
    let modes_bytes = assets.authored_file("stblMode_res.qcs").unwrap();
    let resource_bytes = assets.authored_file("RESDIR.QCD").unwrap();
    let resources = super::qcd::parse(&resource_bytes).unwrap();
    let modes = super::qcs::parse(&modes_bytes).unwrap();
    for mutation in 0..4 {
        let mut table = super::qcs::parse(&table_bytes).unwrap();
        match mutation {
            0 => {}
            1 => table.rows[2][2] = super::qcs::Cell::Integer(0x4184),
            2 => table.rows[11][2] = super::qcs::Cell::Integer(0x41a5),
            _ => table.rows[2][9] = super::qcs::Cell::Integer(1),
        }
        let effects =
            super::table_effects::sequencer(&program.screen, &table, &modes, &resources).unwrap();
        assert_eq!(
            super::canonical_callbacks(&program, &table, &[2, 3, 4], &effects).is_ok(),
            mutation == 0
        );
    }
}

#[test]
#[ignore = "bounded cached opcode inventory; no Java execution"]
fn inspect_register_writer_calls() {
    let assets = cached_assets();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&bytes).unwrap();
    for (name, group) in [("g", &program.global), ("s", &program.screen)] {
        for (function, body) in group.functions.iter().enumerate() {
            let code = super::bytecode::decode(body).unwrap();
            for (index, pair) in code.windows(2).enumerate() {
                if pair[1].opcode == 0x15 && pair[1].operands.first() == Some(&21) {
                    eprintln!(
                        "{name}{} native21@{} id={:?}",
                        function + 1,
                        pair[1].pc,
                        pair[0].integer()
                    );
                }
                if pair[0].integer() == Some(0xc00f) && pair[1].opcode == 0x16 {
                    eprintln!(
                        "{name}{} writer@{} preceding {:?}",
                        function + 1,
                        pair[1].pc,
                        &code[index.saturating_sub(8)..=index + 1]
                    );
                }
            }
        }
    }
}

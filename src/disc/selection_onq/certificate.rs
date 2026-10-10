//! Executable obligations for a finite authored-program certificate.
//! Structural candidates alone never confer authored-selection authority.
//! The certificate concerns canonical whole-resource intent under normal
//! platform operation, not uninterrupted playback or saved seek position.
use super::{
    Reject, Result, assets::Assets, bytecode, frames, invariants, qcd, qco, qcs, registers,
    table_effects, template,
};
use std::collections::BTreeMap;

pub(super) fn verify(assets: &Assets) -> Result<Vec<u16>> {
    match assets.code_version {
        super::runtime::CodeVersion::OnqUhdV1 => {}
    }
    playlist_metadata(&assets.playlist_metadata)?;
    let bytes = assets.authored_file("FS.QCO")?;
    let program = qco::parse(&bytes)?;
    for group in [&program.global, &program.screen] {
        for function in &group.functions {
            let instructions = bytecode::decode(function)?;
            if instructions.windows(2).any(|pair| {
                pair[1].opcode == 0x15
                    && pair[1].operands.first() == Some(&21)
                    && matches!(pair[0].integer(), Some(16008 | 16041 | 16042))
            }) {
                return Err(Reject::Unsupported);
            }
        }
    }
    super::startup_data::timing_tables(&program.global)?;
    let cleared = registers::zero_writes(&program)?;
    registers::hdmv_preserves(
        &assets.movie_objects,
        assets.first_play,
        &cleared.into_iter().collect::<Vec<_>>(),
    )?;
    let menu = menu_contract(&program)?;
    table_contract(assets, &program, &menu)
}

fn playlist_metadata(bytes: &[u8]) -> Result<()> {
    if bytes.len() > super::binary::MAX_BYTES {
        return Err(Reject::Budget);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| Reject::Invalid)?;
    let document = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 65536,
            ..Default::default()
        },
    )
    .map_err(|_| Reject::Invalid)?;
    if document.root_element().tag_name().name() != "playlist-metadata" {
        return Err(Reject::Unsupported);
    }
    Ok(())
}

fn resource_file(assets: &Assets, resource: &qcd::Resource<'_>, kind: u8) -> Result<Vec<u8>> {
    if resource.kind != kind || resource.flags != 0 {
        return Err(Reject::Unsupported);
    }
    let path = resource
        .locator
        .strip_prefix(b"file:///jar/")
        .ok_or(Reject::Unsupported)?;
    let path = std::str::from_utf8(path).map_err(|_| Reject::Invalid)?;
    assets.authored_file(path)
}

fn playlist(locator: &[u8]) -> Result<u16> {
    let id = locator
        .strip_prefix(b"bd://PLAYLIST:")
        .and_then(|rest| rest.strip_suffix(b".ITEM:0.V1:1"))
        .ok_or(Reject::Unsupported)?;
    if id.len() != 5 || !id.iter().all(u8::is_ascii_digit) {
        return Err(Reject::Unsupported);
    }
    std::str::from_utf8(id)
        .map_err(|_| Reject::Invalid)?
        .parse()
        .map_err(|_| Reject::Invalid)
}

fn table_contract(
    assets: &Assets,
    program: &qco::Program<'_>,
    menu: &MenuContract,
) -> Result<Vec<u16>> {
    let bytes = assets.authored_file("RESDIR.QCD")?;
    let resources = qcd::parse(&bytes)?;
    let mut candidates = Vec::new();
    let mut read_bytes = 0usize;
    for object in &program.screen.objects {
        if object.kind != 8 {
            continue;
        }
        let [2, 255, 255, hi, lo] = object.payload else {
            return Err(Reject::Unsupported);
        };
        let id = usize::from(u16::from_be_bytes([*hi, *lo]));
        let resource = resources
            .get(id.checked_sub(1).ok_or(Reject::Invalid)?)
            .ok_or(Reject::Invalid)?;
        let table_bytes = resource_file(assets, resource, 4)?;
        read_bytes = read_bytes
            .checked_add(table_bytes.len())
            .ok_or(Reject::Budget)?;
        if read_bytes > super::binary::MAX_BYTES {
            return Err(Reject::Budget);
        }
        let table = qcs::parse(&table_bytes)?;
        if table
            .program
            .is_some_and(|code| template::table_program(code).is_ok())
        {
            candidates.push((object.id, table_bytes));
        }
    }
    if candidates.len() != 1 {
        return Err(Reject::Unsupported);
    }
    let (sequencer_object, table_bytes) = candidates.pop().ok_or(Reject::Invalid)?;
    let setters: Vec<_> = program
        .screen
        .functions
        .iter()
        .filter_map(|body| template::playback_row_setter(body).ok())
        .collect();
    if setters.len() != 1 || setters[0]["o:o_sequencer"] != sequencer_object {
        return Err(Reject::Unsupported);
    }
    let forward = template::playback_forward(body(&program.screen, menu.playback | 0x4000)?)?;
    let dispatch = template::playback_dispatch(body(&program.screen, forward["s:f_dispatch"])?)?;
    let setter = template::playback_row_setter(body(&program.screen, dispatch["s:f_set_row"])?)?;
    if setter != setters[0] {
        return Err(Reject::Invalid);
    }
    let table = qcs::parse(&table_bytes)?;
    super::program_version::recognize(program, menu.playback, table.rows.len())?;
    canonical_roots(program, menu.object)?;
    selection_resources(program, menu.selection, &resources)?;
    let roles = template::table_program(table.program.ok_or(Reject::Unsupported)?)?;
    let expected_roles = [
        ("b:o_media", 3),
        ("o:o_mode", 311),
        ("o:o_player_root", 1),
        ("r:g_player_group", 2),
        ("s:v_menu", 0x41f4),
        ("s:v_state_a", 0x41ea),
        ("s:v_state_b", 0x41fa),
        ("s:v_state_c", 0x41eb),
        ("s:v_state_d", 0x41f8),
        ("s:v_state_e", 0x4204),
        ("s:v_text_a", 0x41dd),
        ("s:v_text_b", 0x4201),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    if roles != expected_roles || dispatch["o:o_modes"] != roles["o:o_mode"] {
        return Err(Reject::Unsupported);
    }
    let mode = program
        .screen
        .objects
        .iter()
        .find(|object| object.id == roles["o:o_mode"])
        .ok_or(Reject::Invalid)?;
    let [2, 255, 255, hi, lo] = mode.payload else {
        return Err(Reject::Unsupported);
    };
    let mode_id = usize::from(u16::from_be_bytes([*hi, *lo]));
    let mode_resource = resources
        .get(mode_id.checked_sub(1).ok_or(Reject::Invalid)?)
        .ok_or(Reject::Invalid)?;
    let mode_bytes = resource_file(assets, mode_resource, 4)?;
    let modes = qcs::parse(&mode_bytes)?;
    let effects = table_effects::sequencer(&program.screen, &table, &modes, &resources)?;
    if effects.mode_object != mode.id || effects.mode_resource != mode_id {
        return Err(Reject::Invalid);
    }
    canonical_callbacks(program, &table, &menu.rows, &effects)?;
    for &id in &effects.resources {
        qcd::whole_resource_parameters(resources.get(id - 1).ok_or(Reject::Invalid)?)?;
    }
    frames::peaks(
        &[frames::table_frame(&table, None)?],
        qcd::stack_capacity(&bytes)?,
    )?;
    super::canonical_frames::verify(
        program,
        menu.selection,
        menu.playback | 0x4000,
        &table,
        qcd::stack_capacity(&bytes)?,
    )?;
    let mut order = Vec::new();
    for &row in &menu.rows {
        let cells = table.rows.get(usize::from(row)).ok_or(Reject::Invalid)?;
        if cells.get(1) != Some(&qcs::Cell::Integer(i32::from(menu.object))) {
            return Err(Reject::Invalid);
        }
        let Some(qcs::Cell::Integer(resource_id)) = cells.first() else {
            return Err(Reject::Invalid);
        };
        let index = usize::try_from(*resource_id)
            .map_err(|_| Reject::Invalid)?
            .checked_sub(1)
            .ok_or(Reject::Invalid)?;
        let resource = resources.get(index).ok_or(Reject::Invalid)?;
        qcd::whole_resource_parameters(resource)?;
        let id = playlist(resource.locator)?;
        if order.contains(&id) {
            return Err(Reject::Invalid);
        }
        order.push(id);
    }
    Ok(order)
}

/// These executable predicates establish the local menu's data/role contract.
/// Prior-handler frame safety and playback closure are separate obligations.
#[derive(Debug)]
struct MenuContract {
    object: u16,
    selection: u16,
    #[cfg(test)]
    count_setup: u16,
    rows: Vec<u16>,
    playback: u16,
}

fn body<'a>(group: &qco::Group<'a>, reference: u16) -> Result<&'a [u8]> {
    if reference & 0xc000 != 0x4000 {
        return Err(Reject::Invalid);
    }
    group
        .functions
        .get(
            usize::from(reference & 0x3fff)
                .checked_sub(1)
                .ok_or(Reject::Invalid)?,
        )
        .copied()
        .ok_or(Reject::Invalid)
}

fn integer(group: &qco::Group<'_>, reference: u16) -> Result<i32> {
    if reference & 0xc000 != 0x4000 {
        return Err(Reject::Invalid);
    }
    match group.variables.get(
        usize::from(reference & 0x3fff)
            .checked_sub(1)
            .ok_or(Reject::Invalid)?,
    ) {
        Some(qco::Value::Integer(value)) => Ok(*value),
        _ => Err(Reject::Invalid),
    }
}

fn same_roles(a: &BTreeMap<&str, u16>, b: &BTreeMap<&str, u16>, names: &[&str]) -> Result<()> {
    for name in names {
        if a.get(name).is_none() || a.get(name) != b.get(name) {
            return Err(Reject::Invalid);
        }
    }
    Ok(())
}

fn canonical_callbacks(
    program: &qco::Program<'_>,
    table: &qcs::Table<'_>,
    episodes: &[u16],
    effects: &table_effects::Effects,
) -> Result<()> {
    let candidates = invariants::array_candidates(program, &[0x41f1, 0x41f2])?;
    if !candidates.screen_loads.is_empty()
        || !effects.callbacks.is_disjoint(&candidates.fills)
        || candidates
            .callbacks
            .iter()
            .any(|(object, event, function)| {
                *object != 3
                    || !matches!(*event, 90 | 100 | 104)
                    || candidates.fills.contains(function)
            })
    {
        return Err(Reject::Unsupported);
    }
    // The fixed runtime resolves callbacks at dequeue, after serialized QCS
    // installation. These are compiler roles, not playlist/resource IDs.
    for (slot, callbacks) in [(497, [387, 0, 381]), (498, [388, 378, 382])] {
        for &row in column(&program.screen, slot)? {
            let row = u16::try_from(row).map_err(|_| Reject::Invalid)?;
            if episodes.contains(&row) {
                return Err(Reject::Invalid);
            }
            let cells = table.rows.get(usize::from(row)).ok_or(Reject::Invalid)?;
            for (column, function) in callbacks.into_iter().enumerate() {
                let reference = if function == 0 { 0 } else { function | 0x4000 };
                if cells.get(column + 2) != Some(&qcs::Cell::Integer(reference)) {
                    return Err(Reject::Unsupported);
                }
            }
        }
    }
    for &row in episodes {
        let cells = table.rows.get(usize::from(row)).ok_or(Reject::Invalid)?;
        if cells.get(2..5)
            != Some(
                &[
                    qcs::Cell::Integer(0x41a5),
                    qcs::Cell::Integer(0x41a6),
                    qcs::Cell::Integer(0x417c),
                ][..],
            )
        {
            return Err(Reject::Unsupported);
        }
        if cells
            .get(8..13)
            .ok_or(Reject::Invalid)?
            .iter()
            .any(|cell| *cell != qcs::Cell::Integer(0))
        {
            return Err(Reject::Unsupported);
        }
    }
    Ok(())
}

fn column<'a>(group: &'a qco::Group<'_>, slot: u16) -> Result<&'a [i32]> {
    match group
        .variables
        .get(usize::from(slot).checked_sub(1).ok_or(Reject::Invalid)?)
    {
        Some(qco::Value::Integers {
            rows,
            cols: 1,
            values,
        }) if *rows == values.len() => Ok(values),
        _ => Err(Reject::Invalid),
    }
}

fn selection_resources(
    program: &qco::Program<'_>,
    selection: u16,
    resources: &[qcd::Resource<'_>],
) -> Result<()> {
    let roles = template::selection_handler(body(&program.screen, selection)?)?;
    let mut protected = Vec::new();
    for (role, bitmap) in [
        ("s:v_animation", true),
        ("s:v_transition", true),
        ("s:v_style_a", false),
        ("s:v_style_base", false),
    ] {
        let reference = roles[role];
        for &id in column(&program.screen, reference & 0x3fff)? {
            if id <= 0 {
                continue;
            }
            let resource = resources
                .get(usize::try_from(id - 1).map_err(|_| Reject::Invalid)?)
                .ok_or(Reject::Invalid)?;
            if if bitmap {
                resource.flags != 0 || !matches!(resource.kind, 2 | 16)
            } else {
                !matches!((resource.kind, resource.flags), (1, 16) | (8, 0))
            } {
                return Err(Reject::Unsupported);
            }
        }
        protected.push(reference);
    }
    let fonts = [roles["s:v_style_a"], roles["s:v_style_base"]];
    let mut wrappers = std::collections::BTreeSet::new();
    let mut sources = std::collections::BTreeSet::new();
    for (index, function) in program.screen.functions.iter().enumerate() {
        let Ok(bindings) = template::array_fill_wrapper(function) else {
            continue;
        };
        if !["s:v_array_a", "s:v_array_b", "s:v_array_c"]
            .iter()
            .any(|role| fonts.contains(&bindings[role]))
        {
            continue;
        }
        for (array, source) in [
            ("s:v_array_a", "g:v_value_a"),
            ("s:v_array_b", "g:v_value_b"),
            ("s:v_array_c", "g:v_value_c"),
        ] {
            if protected.contains(&bindings[array]) && !fonts.contains(&bindings[array]) {
                return Err(Reject::Unsupported);
            }
            let reference = bindings[source];
            if reference & 0xc000 != 0xc000 {
                return Err(Reject::Invalid);
            }
            let Some(qco::Value::Integer(id)) = program.global.variables.get(
                usize::from(reference & 0x3fff)
                    .checked_sub(1)
                    .ok_or(Reject::Invalid)?,
            ) else {
                return Err(Reject::Invalid);
            };
            if *id > 0 {
                let resource = resources
                    .get(usize::try_from(id - 1).map_err(|_| Reject::Invalid)?)
                    .ok_or(Reject::Invalid)?;
                if !matches!((resource.kind, resource.flags), (1, 16) | (8, 0)) {
                    return Err(Reject::Unsupported);
                }
            }
            sources.insert(reference);
        }
        wrappers.insert(0x4000 | u16::try_from(index + 1).map_err(|_| Reject::Budget)?);
    }
    for group in [&program.global, &program.screen] {
        for function in &group.functions {
            for write in bytecode::writes(&bytecode::decode(function)?) {
                if let Some(destination) = write.literal_destination
                    && u16::try_from(destination).is_ok_and(|id| sources.contains(&id))
                {
                    return Err(Reject::Unsupported);
                }
            }
        }
    }
    invariants::array_candidates_with_typed_fills(program, &protected, &wrappers)?;
    Ok(())
}

// Slots here are compiler-layout roles, gated by program_version, not disc IDs.
fn canonical_roots(program: &qco::Program<'_>, episode_menu: u16) -> Result<()> {
    let group = &program.screen;
    let mut protected = Vec::new();
    let mut allowed = std::collections::BTreeSet::new();
    for (root, setup, buttons, flags, actions, count, selected) in
        [(20, 66, 72, 75, 76, 73, 74), (37, 88, 93, 96, 97, 94, 95)]
    {
        let roster = column(group, buttons)?;
        let flags_data = column(group, flags)?;
        let actions_data = column(group, actions)?;
        let active = roster
            .iter()
            .position(|id| *id == 0)
            .unwrap_or(roster.len());
        if active == 0
            || roster[active..].iter().any(|id| *id != 0)
            || flags_data.len() != roster.len()
            || actions_data.len() != roster.len()
            || flags_data.iter().any(|flag| !matches!(flag, 0 | 1))
            || integer(group, count | 0x4000)? != 0
            || !(0..i32::try_from(active).map_err(|_| Reject::Budget)?)
                .contains(&integer(group, selected | 0x4000)?)
        {
            return Err(Reject::Invalid);
        }
        let roles = template::count_setup(body(group, setup | 0x4000)?)?;
        if roles["s:v_buttons"] != buttons | 0x4000 || roles["s:v_count"] != count | 0x4000 {
            return Err(Reject::Invalid);
        }
        template::first_zero_count(body(group, roles["s:f_count"])?)?;
        sole_count_writer(program, setup | 0x4000, count | 0x4000)?;
        let mut found = 0;
        let mut episode_index = None;
        let mut seen = std::collections::BTreeSet::new();
        for index in 0..active {
            let id = u16::try_from(roster[index]).map_err(|_| Reject::Invalid)?;
            if !seen.insert(id)
                || !group
                    .objects
                    .iter()
                    .any(|object| object.id == id && object.parent == root)
            {
                return Err(Reject::Invalid);
            }
            if flags_data[index] == 1 && actions_data[index] == i32::from(episode_menu) {
                if root == 37 && matches!(id, 38 | 44 | 42 | 43) {
                    return Err(Reject::Unsupported);
                }
                found += 1;
                episode_index = Some(index);
            }
        }
        if found != 1 {
            return Err(Reject::Unsupported);
        }
        protected.extend([buttons | 0x4000, flags | 0x4000, actions | 0x4000]);
        for (function, code) in group.functions.iter().enumerate() {
            let code = bytecode::decode(code)?;
            for (index, instruction) in code.iter().enumerate() {
                if instruction.opcode != 0x26
                    || index < 3
                    || code[index - 1].integer() != Some(i32::from(buttons | 0x4000))
                {
                    continue;
                }
                let window = &code[index - 3..=index];
                if window
                    .iter()
                    .skip(1)
                    .any(|instruction| instruction.branch_target)
                {
                    return Err(Reject::Invalid);
                }
                let cell = window[1]
                    .integer()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(Reject::Invalid)?;
                let replacement = window[0]
                    .integer()
                    .and_then(|value| u16::try_from(value).ok())
                    .ok_or(Reject::Invalid)?;
                if cell >= active
                    || Some(cell) == episode_index
                    || replacement == 0
                    || !group
                        .objects
                        .iter()
                        .any(|object| object.id == replacement && object.parent == root)
                {
                    return Err(Reject::Unsupported);
                }
                allowed.insert((
                    0x4000 | u16::try_from(function + 1).map_err(|_| Reject::Budget)?,
                    instruction.pc,
                ));
            }
        }
    }
    invariants::array_candidates_with_literal_writes(program, &protected, &allowed)?;
    Ok(())
}

fn sole_count_writer(program: &qco::Program<'_>, setup: u16, destination: u16) -> Result<()> {
    let mut count = 0usize;
    for (prefix, group) in [(0xc000, &program.global), (0x4000, &program.screen)] {
        for (index, body) in group.functions.iter().enumerate() {
            let function = prefix | u16::try_from(index + 1).map_err(|_| Reject::Budget)?;
            for write in bytecode::writes(&bytecode::decode(body)?) {
                if !matches!(write.opcode, 0x2e | 0x2f) {
                    continue;
                }
                let target = write.literal_destination.ok_or(Reject::Unsupported)?;
                if target == i32::from(destination) {
                    if function != setup || write.opcode != 0x2e {
                        return Err(Reject::Invalid);
                    }
                    count += 1;
                }
            }
        }
    }
    if count != 1 {
        return Err(Reject::Invalid);
    }
    Ok(())
}

fn menu_contract(program: &qco::Program<'_>) -> Result<MenuContract> {
    let group = &program.screen;
    let mut candidates = Vec::new();
    for object in &group.objects {
        if object.kind != 3 {
            continue;
        }
        for event in &object.events {
            if event.event != 31 || event.target != 0 {
                continue;
            }
            let Ok(code) = body(group, event.function) else {
                continue;
            };
            if let Ok(roles) = template::selection_handler(code)
                && let Ok(actions) =
                    template::selected_action_rows(group, usize::from(event.function & 0x3fff))
            {
                candidates.push((object, event.function, roles, actions));
            }
        }
    }
    if candidates.len() != 1 {
        return Err(Reject::Unsupported);
    }
    let (object, selection, selected, actions) = candidates.pop().ok_or(Reject::Invalid)?;
    let event_function = |number| -> Result<u16> {
        let event = object
            .events
            .iter()
            .find(|event| event.event == number)
            .ok_or(Reject::Invalid)?;
        if event.target != 0 {
            return Err(Reject::Unsupported);
        }
        Ok(event.function)
    };
    let previous = template::navigation_handler(body(group, event_function(29)?)?, false)?;
    let next = template::navigation_handler(body(group, event_function(30)?)?, true)?;
    if previous != next {
        return Err(Reject::Invalid);
    }
    same_roles(&selected, &previous, &["s:v_buttons", "s:v_selected"])?;
    let mut setups = Vec::new();
    for (index, code) in group.functions.iter().enumerate() {
        if let Ok(roles) = template::count_setup(code)
            && roles.get("s:v_buttons") == selected.get("s:v_buttons")
        {
            setups.push((index + 1, roles));
        }
    }
    if setups.len() != 1 {
        return Err(Reject::Unsupported);
    }
    let (setup, setup_roles) = setups.pop().ok_or(Reject::Invalid)?;
    same_roles(&previous, &setup_roles, &["s:v_buttons", "s:v_count"])?;
    let (count_ref, count) =
        template::selection_count(group, usize::from(selection & 0x3fff), setup)?;
    if usize::try_from(integer(group, count_ref)?).ok() != Some(count)
        || integer(group, selected["s:v_selected"])? != 0
        || count != actions.rows.len()
    {
        return Err(Reject::Invalid);
    }
    let writes = invariants::array_candidates(
        program,
        &[
            selected["s:v_buttons"],
            selected["s:v_submenus"],
            selected["s:v_actions"],
        ],
    )?;
    if !writes.screen_loads.is_empty() {
        return Err(Reject::Unsupported);
    }
    let count_setup = 0x4000 | u16::try_from(setup).map_err(|_| Reject::Budget)?;
    sole_count_writer(program, count_setup, count_ref)?;
    Ok(MenuContract {
        object: object.id,
        selection,
        #[cfg(test)]
        count_setup,
        rows: actions.rows,
        playback: actions.playback_function,
    })
}

#[cfg(test)]
#[path = "certificate_tests.rs"]
mod tests;

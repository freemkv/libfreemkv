//! Typed, all-row effects of the reviewed sequencer program. Returned callbacks
//! are obligations for the authored control-flow verifier, not an allow-list.
use super::qco::{Group, Value};
use super::qcs::{Cell, Table};
use super::{Reject, Result, qcd, template};
use std::collections::BTreeSet;

#[derive(Debug)]
pub(super) struct Effects {
    pub callbacks: BTreeSet<u16>,
    pub resources: BTreeSet<usize>,
    pub mode_object: u16,
    pub mode_resource: usize,
}

fn integer(cell: &Cell<'_>) -> Result<i32> {
    match cell {
        Cell::Integer(n) => Ok(*n),
        _ => Err(Reject::Invalid),
    }
}

pub(super) fn sequencer(
    group: &Group<'_>,
    table: &Table<'_>,
    modes: &Table<'_>,
    resources: &[qcd::Resource<'_>],
) -> Result<Effects> {
    let roles = template::table_program(table.program.ok_or(Reject::Unsupported)?)?;
    if roles["r:g_player_group"] != group.id || !table.targets.is_empty() || table.rows.is_empty() {
        return Err(Reject::Invalid);
    }
    let object = |id| {
        group
            .objects
            .iter()
            .find(|o| o.id == id)
            .ok_or(Reject::Invalid)
    };
    if object(roles["o:o_player_root"])?.kind != 1 || object(roles["b:o_media"])?.kind != 12 {
        return Err(Reject::Unsupported);
    }
    for role in [
        "s:v_menu",
        "s:v_text_a",
        "s:v_text_b",
        "s:v_state_a",
        "s:v_state_b",
        "s:v_state_c",
        "s:v_state_d",
        "s:v_state_e",
    ] {
        let index = usize::from(roles[role] & 0x3fff) - 1;
        let value = group.variables.get(index).ok_or(Reject::Invalid)?;
        let valid = if role.starts_with("s:v_text_") {
            matches!(value, Value::String(_))
        } else {
            matches!(value, Value::Integer(_))
        };
        if !valid {
            return Err(Reject::Invalid);
        }
    }
    let mode = object(roles["o:o_mode"])?;
    // fi initial=-1, resource binding; no alternate flags or eager initial row.
    let [2, 255, 255, hi, lo] = mode.payload else {
        return Err(Reject::Unsupported);
    };
    if mode.kind != 8 {
        return Err(Reject::Invalid);
    }
    let mode_resource = usize::from(u16::from_be_bytes([*hi, *lo]));
    let resource = resources
        .get(mode_resource.checked_sub(1).ok_or(Reject::Invalid)?)
        .ok_or(Reject::Invalid)?;
    if resource.kind != 4
        || resource.id != mode_resource
        || resource.flags != 0
        || modes.program.is_some()
        || modes.rows.is_empty()
        || modes.targets.len() != 5
    {
        return Err(Reject::Unsupported);
    }
    // Flags0 dispatch is restricted to screen focus and four visibility slots.
    for (column, &(target, property)) in modes.targets.iter().enumerate() {
        if property != 1 || object(target)?.kind != if column == 0 { 1 } else { 3 } {
            return Err(Reject::Unsupported);
        }
        if column != 0
            && (object(target)?.parent != roles["o:o_player_root"]
                || object(target)?.payload != [0; 7])
        {
            return Err(Reject::Unsupported);
        }
    }
    if modes.targets[0].0 != roles["o:o_player_root"]
        || modes
            .targets
            .iter()
            .map(|t| t.0)
            .collect::<BTreeSet<_>>()
            .len()
            != 5
    {
        return Err(Reject::Invalid);
    }
    for row in &modes.rows {
        if row.len() != 5 {
            return Err(Reject::Invalid);
        }
        let focus = integer(&row[0])?;
        if focus != 0 {
            let focus = u16::try_from(focus).map_err(|_| Reject::Invalid)?;
            if !modes.targets[1..].iter().any(|t| t.0 == focus) {
                return Err(Reject::Invalid);
            }
        }
        for cell in &row[1..] {
            if !matches!(integer(cell)?, 0 | 1) {
                return Err(Reject::Unsupported);
            }
        }
    }
    let mut callbacks = BTreeSet::new();
    let mut used = BTreeSet::new();
    for row in &table.rows {
        if row.len() != 13 {
            return Err(Reject::Invalid);
        }
        for (column, cell) in row.iter().enumerate() {
            if matches!(column, 6 | 7) {
                if !matches!(cell, Cell::String(_)) {
                    return Err(Reject::Invalid);
                }
            } else {
                integer(cell)?;
            }
        }
        let id = usize::try_from(integer(&row[0])?).map_err(|_| Reject::Invalid)?;
        if id != 0 {
            let resource = resources.get(id - 1).ok_or(Reject::Invalid)?;
            if resource.id != id || resource.kind != 64 || resource.flags != 0 {
                return Err(Reject::Unsupported);
            }
            used.insert(id);
        }
        let menu = integer(&row[1])?;
        if menu != 0 && object(u16::try_from(menu).map_err(|_| Reject::Invalid)?)?.kind != 3 {
            return Err(Reject::Invalid);
        }
        for cell in &row[2..5] {
            let reference = u16::try_from(integer(cell)?).map_err(|_| Reject::Invalid)?;
            if reference == 0 {
                continue;
            }
            let index = reference & 0x3fff;
            if reference & 0xc000 != 0x4000
                || index == 0
                || usize::from(index) > group.functions.len()
            {
                return Err(Reject::Invalid);
            }
            callbacks.insert(reference);
        }
        let mode_row = integer(&row[5])?;
        if mode_row < -1 || mode_row >= modes.rows.len() as i32 {
            return Err(Reject::Invalid);
        }
    }
    Ok(Effects {
        callbacks,
        resources: used,
        mode_object: mode.id,
        mode_resource,
    })
}

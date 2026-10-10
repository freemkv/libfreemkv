//! Register-write exclusion for the fixed runtime dispatch contract. These
//! predicates preserve an initial register bank; they do not establish it.
use super::{Reject, Result, bytecode, qco};
use std::collections::BTreeSet;

const SET_GPR: &[u8] = &[
    0, 0, 0x17, 0x20, 2, 1, 0x15, 0x20, 1, 1, 0x15, 1, 2, 1, 0x15, 2, 0x3a, 0x9c, 0x15, 0x15, 7,
    0x34, 0x36, 0x3f,
];

/// Every native21 dispatch is literal, and the only GPR writer is the exact
/// two-argument adapter. Every authored invocation must write zero through an
/// immutable register-number variable; callbacks cannot invoke the adapter.
pub(super) fn zero_writes(program: &qco::Program<'_>) -> Result<BTreeSet<u32>> {
    let mut functions = Vec::new();
    let mut writer = None;
    for (prefix, group) in [(0xc000, &program.global), (0x4000, &program.screen)] {
        for (index, body) in group.functions.iter().enumerate() {
            let reference = prefix | u16::try_from(index + 1).map_err(|_| Reject::Budget)?;
            let code = bytecode::decode(body)?;
            for (pc, instruction) in code.iter().enumerate() {
                if instruction.opcode != 0x15 || instruction.operands[0] != 21 {
                    continue;
                }
                if instruction.branch_target {
                    return Err(Reject::Unsupported);
                }
                let id = pc
                    .checked_sub(1)
                    .and_then(|i| code[i].integer())
                    .ok_or(Reject::Unsupported)?;
                if id == 15004
                    && (prefix != 0xc000 || *body != SET_GPR || writer.replace(reference).is_some())
                {
                    return Err(Reject::Unsupported);
                }
            }
            functions.push((reference, code));
        }
    }
    let writer = writer.ok_or(Reject::Unsupported)?;
    if program
        .global
        .objects
        .iter()
        .chain(&program.screen.objects)
        .flat_map(|object| &object.events)
        .any(|event| event.function == writer)
    {
        return Err(Reject::Unsupported);
    }
    let mut selectors = BTreeSet::new();
    let mut registers = BTreeSet::new();
    for (_, code) in &functions {
        for (index, instruction) in code.iter().enumerate() {
            if instruction.opcode == 0x49 {
                let callback = index
                    .checked_sub(2)
                    .and_then(|i| code[i].integer())
                    .ok_or(Reject::Unsupported)?;
                if callback == i32::from(writer) {
                    return Err(Reject::Unsupported);
                }
            }
            if instruction.opcode != 0x16 {
                continue;
            }
            let target = index
                .checked_sub(1)
                .and_then(|i| code[i].integer())
                .ok_or(Reject::Unsupported)?;
            if target != i32::from(writer) {
                continue;
            }
            let start = index.checked_sub(4).ok_or(Reject::Invalid)?;
            let call = &code[start..=index];
            if call[0].integer() != Some(0)
                || call[2].opcode != 0x2c
                || instruction.operands != [2]
                || call[1..].iter().any(|op| op.branch_target)
            {
                return Err(Reject::Unsupported);
            }
            let selector = call[1]
                .integer()
                .and_then(|n| u16::try_from(n).ok())
                .ok_or(Reject::Invalid)?;
            let group = match selector & 0xc000 {
                0x4000 => &program.screen,
                0xc000 => &program.global,
                _ => return Err(Reject::Invalid),
            };
            let variable = usize::from(selector & 0x3fff)
                .checked_sub(1)
                .ok_or(Reject::Invalid)?;
            let Some(qco::Value::Integer(register)) = group.variables.get(variable) else {
                return Err(Reject::Invalid);
            };
            let register = u32::try_from(*register).map_err(|_| Reject::Invalid)?;
            if register >= 4096 {
                return Err(Reject::Invalid);
            }
            selectors.insert(selector);
            registers.insert(register);
        }
    }
    for (_, code) in functions {
        for write in bytecode::writes(&code) {
            if matches!(write.opcode, 0x26 | 0x27 | 0x2a | 0x2b | 0x2e | 0x2f | 0x42) {
                let reference = write
                    .literal_destination
                    .and_then(|n| u16::try_from(n).ok())
                    .ok_or(Reject::Unsupported)?;
                if selectors.contains(&reference) {
                    return Err(Reject::Unsupported);
                }
            }
        }
    }
    Ok(registers)
}

/// Scan every HDMV object, not merely the First-Play normal branch. This
/// subset has no indirect destinations, swaps, or system-register writes.
pub(super) fn hdmv_preserves(data: &[u8], first_play: u16, protected: &[u32]) -> Result<()> {
    if data.len() > super::binary::MAX_BYTES
        || data.get(..8) != Some(b"MOBJ0300")
        || data
            .get(8..40)
            .is_none_or(|bytes| bytes.iter().any(|byte| *byte != 0))
    {
        return Err(Reject::Unsupported);
    }
    let objects = crate::bdnav::mobj::parse(data).ok_or(Reject::Invalid)?;
    if usize::from(first_play) >= objects.len() {
        return Err(Reject::Invalid);
    }
    let length_bytes: [u8; 4] = data
        .get(40..44)
        .ok_or(Reject::Truncated)?
        .try_into()
        .map_err(|_| Reject::Invalid)?;
    if usize::try_from(u32::from_be_bytes(length_bytes))
        .map_err(|_| Reject::Budget)?
        .checked_add(44)
        != Some(data.len())
        || data.get(44..48) != Some(&[0; 4])
    {
        return Err(Reject::Invalid);
    }
    let mut extent = 50usize;
    let mut commands = 0usize;
    for object in objects {
        let header = data.get(extent..extent + 4).ok_or(Reject::Truncated)?;
        if header[0] & 0x1f != 0 || header[1] != 0 {
            return Err(Reject::Unsupported);
        }
        for raw in data
            .get(extent + 4..extent + 4 + object.cmds.len() * 12)
            .ok_or(Reject::Truncated)?
            .as_chunks::<12>()
            .0
            .iter()
        {
            if raw[1] & 0x30 != 0 || raw[2] & 0xf0 != 0 || raw[3] & 0xe0 != 0 {
                return Err(Reject::Unsupported);
            }
        }
        commands = commands
            .checked_add(object.cmds.len())
            .ok_or(Reject::Budget)?;
        extent = extent
            .checked_add(4 + object.cmds.len() * 12)
            .ok_or(Reject::Budget)?;
        for command in object.cmds {
            if (command.grp != 0 && command.branch_opt != 0)
                || (command.grp != 1 && command.cmp_opt != 0)
                || (command.grp != 2 && command.set_opt != 0)
            {
                return Err(Reject::Unsupported);
            }
            let allowed = match (command.grp, command.sub_grp, command.op_cnt) {
                (0, 0, 0) => matches!(command.branch_opt, 0 | 2),
                (0, 0, 1) => command.branch_opt == 1,
                (0, 1, 1) => command.branch_opt == 1,
                (0, 2, 1) => command.branch_opt == 0,
                (1, 0, 2) => matches!(command.cmp_opt, 1..=7),
                (2, 0, 2) => {
                    !command.imm_op1
                        && command.dst < 4096
                        && !protected.contains(&command.dst)
                        && matches!(command.set_opt, 1 | 9..=15)
                }
                _ => false,
            };
            if !allowed {
                return Err(Reject::Unsupported);
            }
        }
    }
    if commands > 65536 || extent != data.len() {
        return Err(Reject::Budget);
    }
    Ok(())
}

#[cfg(test)]
#[path = "registers_tests.rs"]
mod tests;

//! Finite normal-stack envelope for the reviewed canonical action graph.
//! This is not an interpreter or a proof of exceptional execution. Code-version,
//! data/type/bounds and native-effect obligations are independent prerequisites.
use super::{Reject, Result, bytecode, frames, qco, qcs, template};
use std::collections::{BTreeMap, VecDeque};

struct Builder<'a, 'b> {
    program: &'a qco::Program<'b>,
    frames: Vec<frames::Frame>,
    entries: BTreeMap<(u16, usize), Option<usize>>,
    sequencer: u16,
    table_frame: usize,
}

fn body<'a>(program: &qco::Program<'a>, reference: u16) -> Result<&'a [u8]> {
    let group = match reference & 0xc000 {
        0x4000 => &program.screen,
        0xc000 => &program.global,
        _ => return Err(Reject::Invalid),
    };
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

fn step(pop: usize, push: usize, next: Vec<usize>) -> frames::Step {
    frames::Step {
        pop,
        push,
        call: None,
        next,
    }
}

impl Builder<'_, '_> {
    fn build(&mut self, reference: u16, incoming: usize) -> Result<usize> {
        if let Some(entry) = self.entries.get(&(reference, incoming)) {
            return entry.ok_or(Reject::Unsupported);
        }
        if self.entries.len() >= 128 {
            return Err(Reject::Budget);
        }
        self.entries.insert((reference, incoming), None);
        let bytes = body(self.program, reference)?;
        frames::check_locals(bytes, incoming)?;
        let code = bytecode::decode(bytes)?;
        let mut graph = Vec::new();
        let mut heights = vec![None; code.len()];
        let positions: BTreeMap<_, _> = code.iter().enumerate().map(|(i, op)| (op.pc, i)).collect();
        for i in 0..code.len() {
            graph.push(step(0, 0, vec![3 * i + 1]));
            graph.push(step(0, 0, vec![3 * i + 2]));
            graph.push(step(0, 0, vec![]));
        }
        heights[0] = Some(0usize);
        let mut pending = VecDeque::from([0]);
        while let Some(i) = pending.pop_front() {
            let op = &code[i];
            let before = heights[i].ok_or(Reject::Invalid)?;
            let previous = i.checked_sub(1).and_then(|j| code[j].integer());
            let mut next = if op.opcode == 0x3f {
                vec![]
            } else {
                vec![i + 1]
            };
            let (pop, push) = match op.opcode {
                0x00 | 0x3c | 0x41 => (0, 0),
                0x01..=0x05 | 0x20 | 0x21 | 0x34 | 0x35 | 0x46 => (0, 1),
                0x06..=0x11 | 0x4d | 0x4e | 0x50..=0x53 => (2, 1),
                0x18 | 0x19 | 0x24 | 0x25 => (if op.opcode < 0x24 { 1 } else { 2 }, 1),
                0x1c | 0x1d | 0x28 | 0x29 => (if op.opcode < 0x24 { 2 } else { 3 }, 1),
                0x22 | 0x23 | 0x36 | 0x37 => (1, 0),
                0x2c | 0x2d | 0x30 | 0x31 | 0x47 | 0x4b | 0x4c | 0x4f => (1, 1),
                0x2e | 0x2f => (2, 0),
                0x3d | 0x3e => (usize::from(op.operands[0]), 0),
                0x13 | 0x14 | 0x43..=0x45 => {
                    let offset = i16::from_be_bytes([op.operands[0], op.operands[1]]);
                    let target = (op.pc + 1)
                        .checked_add_signed(isize::from(offset))
                        .ok_or(Reject::Invalid)?;
                    let target = *positions.get(&target).ok_or(Reject::Invalid)?;
                    if op.opcode == 0x14 {
                        next.clear();
                    }
                    next.push(target);
                    // 13/43 test by peeking; 44/45 consume the condition.
                    if matches!(op.opcode, 0x13 | 0x43) {
                        if before == 0 {
                            return Err(Reject::Invalid);
                        }
                        (0, 0)
                    } else if op.opcode == 0x14 {
                        (0, 0)
                    } else {
                        (1, 0)
                    }
                }
                0x16 => {
                    if op.branch_target {
                        return Err(Reject::Unsupported);
                    }
                    let callee = u16::try_from(previous.ok_or(Reject::Unsupported)?)
                        .map_err(|_| Reject::Invalid)?;
                    let args = before.checked_sub(1).ok_or(Reject::Invalid)?;
                    frames::check_call(body(self.program, callee)?, args, op.operands[0])?;
                    graph[3 * i].call = Some(if reference == 0x4130 && callee == 0xc058 {
                        self.silent_selection_sound(args)?
                    } else {
                        self.build(callee, args)?
                    });
                    graph[3 * i + 1].pop = usize::from(op.operands[0]);
                    (1, 0)
                }
                0x15 => {
                    let required = native(op.operands[0], previous)?;
                    if before < required {
                        return Err(Reject::Invalid);
                    }
                    (usize::from(op.operands[1]), 0)
                }
                0x32 if previous == Some(i32::from(self.sequencer)) && op.operands == [2] => {
                    // ps -> fi -> yb -> pb pushes two arguments and invokes
                    // the flags2 table inline; mode flags0 adds no pb frame.
                    graph[3 * i + 1].call = Some(self.table_frame);
                    graph[3 * i + 2].pop = 2;
                    (2, 2)
                }
                0x32 if self.render_property(reference, previous, op.operands[0])? => (2, 0),
                0x17 if self.command(reference, previous, op.operands)? => {
                    (1 + usize::from(op.operands[1]), 0)
                }
                // A command/property write can synchronously re-enter QCO.
                // No generic zero-cost fallback is sound here.
                0x17 | 0x32 | 0x33 => {
                    return Err(Reject::Unproven);
                }
                0x3f => (0, 0),
                _ => return Err(Reject::Unsupported),
            };
            graph[3 * i].pop = pop;
            graph[3 * i].push = push;
            let after = before
                .checked_sub(pop)
                .and_then(|h| h.checked_add(push))
                .and_then(|h| h.checked_sub(graph[3 * i + 1].pop))
                .and_then(|h| h.checked_sub(graph[3 * i + 2].pop))
                .ok_or(Reject::Invalid)?;
            if next.is_empty() && after != 0 {
                return Err(Reject::Invalid);
            }
            for &target in &next {
                let old = heights.get_mut(target).ok_or(Reject::Invalid)?;
                match *old {
                    Some(height) if height != after => return Err(Reject::Invalid),
                    Some(_) => {}
                    None => {
                        *old = Some(after);
                        pending.push_back(target);
                    }
                }
            }
            graph[3 * i + 2].next = next.iter().map(|index| index * 3).collect();
        }
        // The reviewed compiler emits redundant GOTOs after unconditional
        // branches (e.g. g64). Their bytes remain version-gated, but are not
        // execution edges. Compact only after the complete CFG walk.
        let reachable: BTreeMap<_, _> = (0..graph.len())
            .filter(|index| heights[index / 3].is_some())
            .enumerate()
            .map(|(new, old)| (old, new))
            .collect();
        let graph = graph
            .into_iter()
            .enumerate()
            .filter_map(|(index, mut step)| {
                reachable.contains_key(&index).then(|| {
                    step.next = step
                        .next
                        .iter()
                        .map(|next| reachable.get(next).copied().ok_or(Reject::Invalid))
                        .collect::<Result<_>>()?;
                    Ok(step)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let index = self.frames.len();
        self.frames.push(frames::Frame {
            locals: usize::from(bytes[0]),
            argc: incoming,
            steps: graph,
        });
        self.entries.insert((reference, incoming), Some(index));
        Ok(index)
    }

    fn render_property(&self, function: u16, target: Option<i32>, property: u8) -> Result<bool> {
        // The pinned transition helper receives only the validated button,
        // cursor and highlight roles. No arbitrary dynamic receiver is allowed.
        if function == 0x4026 && target.is_none() && matches!(property, 2 | 3 | 5 | 17 | 27) {
            return Ok(true);
        }
        if target == Some(0x20001) && property == 1 && function == 0x405d {
            return Ok(true);
        }
        let Some(target) = target.and_then(|n| u16::try_from(n).ok()) else {
            return Ok(false);
        };
        let Some(object) = self.program.screen.objects.iter().find(|o| o.id == target) else {
            return Ok(false);
        };
        // class3 visibility uses the normal rendering interface, not fi/yb.
        if object.kind == 3 && property == 1 {
            return Ok(true);
        }
        // Reviewed flags0 mode table; its effects are checked by table_effects.
        Ok(function == 0x4185 && target == 311 && object.kind == 8 && property == 2)
    }

    fn command(&self, function: u16, target: Option<i32>, operands: &[u8]) -> Result<bool> {
        Ok(match (function, target, operands) {
            (0x401d, Some(8), [1, 0]) => self.program.screen.objects.iter().any(|o| {
                o.id == 8
                    && o.kind == 7
                    && o.events
                        .iter()
                        .any(|e| e.event == 8 && e.target == 0 && e.function == 0x401e)
            }),
            // s30 consumes retained s41 to request focus; focus gained is an
            // asynchronous action event, not a nested call under this frame.
            (0x401e, None, [1, 0]) => true,
            (0x4185, Some(3), [2, 0]) => true,
            (0x4187, Some(target), [4, 0]) => target == i32::from(self.sequencer),
            _ => false,
        })
    }

    fn silent_selection_sound(&mut self, incoming: usize) -> Result<usize> {
        // The pinned SELECT pushes g57, not an arbitrary caller argument.
        // g88 tests arg!=0 before its loop/native audio operation. With this
        // immutable zero, both possible g193 branches return without a call.
        if incoming != 1 || self.program.global.variables.get(56) != Some(&qco::Value::Integer(0)) {
            return Err(Reject::Unproven);
        }
        for group in [&self.program.global, &self.program.screen] {
            for function in &group.functions {
                for write in bytecode::writes(&bytecode::decode(function)?) {
                    if matches!(write.opcode, 0x2e | 0x2f | 0x42)
                        && (write.literal_destination.is_none()
                            || write.literal_destination == Some(0xc039))
                    {
                        return Err(Reject::Unproven);
                    }
                }
            }
        }
        frames::check_locals(body(self.program, 0xc058)?, incoming)?;
        let index = self.frames.len();
        self.frames.push(silent_sound_frame());
        Ok(index)
    }
}

fn silent_sound_frame() -> frames::Frame {
    // Pinned g88: load g193; consuming conditional exit, or load local2
    // (the incoming zero), push zero, compare !=, consume condition, exit.
    // Only the audio body is dead; both executed tests still use operands.
    frames::Frame {
        locals: 1,
        argc: 1,
        steps: vec![
            step(0, 1, vec![1]),
            step(1, 0, vec![2, 6]),
            step(0, 1, vec![3]),
            step(0, 1, vec![4]),
            step(2, 1, vec![5]),
            step(1, 0, vec![6]),
            step(0, 0, vec![]),
        ],
    }
}

fn native(id: u8, preceding: Option<i32>) -> Result<usize> {
    // ao's reviewed register/rendering wrappers peek their arguments and write
    // a separate return register; they do not push a return operand.
    match (id, preceding) {
        // Reviewed normal-return interfaces. Rendering/logging platform
        // failures are not claimed impossible by this normal-stack proof.
        (2 | 5, _) => Ok(1),
        (3 | 16, _) => Ok(2),
        (8, _) => Ok(0),
        (21, Some(15003 | 15005 | 15775)) => Ok(5),
        (21, Some(15002 | 15004 | 1058)) => Ok(7),
        // Other natives require explicit effect closure, not an inference
        // from the opcode's post-call DROP count.
        _ => Err(Reject::Unproven),
    }
}

fn selection_data(program: &qco::Program<'_>, selection: u16) -> Result<()> {
    let roles = template::selection_handler(body(program, selection)?)?;
    let variable = |name| -> Result<&qco::Value<'_>> {
        let reference = roles.get(name).copied().ok_or(Reject::Invalid)?;
        if reference & 0xc000 != 0x4000 {
            return Err(Reject::Invalid);
        }
        program
            .screen
            .variables
            .get(
                usize::from(reference & 0x3fff)
                    .checked_sub(1)
                    .ok_or(Reject::Invalid)?,
            )
            .ok_or(Reject::Invalid)
    };
    let qco::Value::Integers {
        rows,
        cols: 1,
        values,
    } = variable("s:v_buttons")?
    else {
        return Err(Reject::Invalid);
    };
    if *rows != values.len() {
        return Err(Reject::Invalid);
    }
    let count = values.iter().position(|v| *v == 0).unwrap_or(values.len());
    if count == 0 {
        return Err(Reject::Invalid);
    }
    for &id in &values[..count] {
        if !program
            .screen
            .objects
            .iter()
            .any(|o| i32::from(o.id) == id && o.kind == 6)
        {
            return Err(Reject::Invalid);
        }
    }
    for (role, needed_cols) in [
        ("s:v_animation", 1),
        ("s:v_submenus", 1),
        ("s:v_style_c", 1),
        ("s:v_style_b", 1),
        ("s:v_style_d", 2),
        ("s:v_style_a", 1),
        ("s:v_style_base", 1),
        ("s:v_transition", 1),
    ] {
        match variable(role)? {
            qco::Value::Integers { rows, cols, values }
                if *rows >= count
                    && *cols >= needed_cols
                    && rows.checked_mul(*cols) == Some(values.len()) => {}
            _ => return Err(Reject::Invalid),
        }
    }
    for (role, kind) in [("o:o_cursor", 3), ("o:o_highlight", 4)] {
        let id = roles.get(role).copied().ok_or(Reject::Invalid)?;
        if !program
            .screen
            .objects
            .iter()
            .any(|o| o.id == id && o.kind == kind)
        {
            return Err(Reject::Invalid);
        }
    }
    Ok(())
}

pub(super) fn verify(
    program: &qco::Program<'_>,
    selection_ref: u16,
    playback_ref: u16,
    table: &qcs::Table<'_>,
    capacity: usize,
) -> Result<()> {
    if selection_ref & 0xc000 != 0x4000 || playback_ref & 0xc000 != 0x4000 {
        return Err(Reject::Invalid);
    }
    super::program_version::recognize(program, playback_ref & 0x3fff, table.rows.len())?;
    template::selection_handler(body(program, selection_ref)?)?;
    selection_data(program, selection_ref)?;
    let forward = template::playback_forward(body(program, playback_ref)?)?;
    let dispatch = template::playback_dispatch(body(program, forward["s:f_dispatch"])?)?;
    let setter_ref = dispatch["s:f_set_row"];
    let setter = template::playback_row_setter(body(program, setter_ref)?)?;
    let table_frame = frames::table_frame(table, None)?;
    let mut builder = Builder {
        program,
        frames: vec![table_frame],
        entries: BTreeMap::new(),
        sequencer: setter["o:o_sequencer"],
        table_frame: 0,
    };
    // pb.a(Laaf;Z)V pushes pressedFlag, zero, keyCode, focusedID at JVM
    // 0x67/0x6f/0x7a/0x83, dispatches at 0x96, then drops four at 0x9e.
    builder.build(selection_ref, 4)?;
    // Version-gated timer/action events are separate entries, not SELECT children.
    // aet.d() calls pb.a(II)V: elapsed/id pushes at 0x05/0x0d,
    // event8 dispatch at 0x14, DROP2 at 0x1c.
    builder.build(0x401e, 2)?;
    // pb.a(Ll;)V pushes focusedID at 0x0f, dispatches event3 at 0x15,
    // then pops at 0x1c. Event numbers themselves do not imply an arity.
    for function in [0x407d, 0x407e, 0x407f] {
        builder.build(function, 1)?;
    }
    // QCO dispatch pushes [mark,row] before calling the setter. This is the
    // setter's incoming pair, not an arity inferred from its DROP operand.
    builder.build(setter_ref, 2)?;
    frames::peaks(&builder.frames, capacity)?;
    Ok(())
}

#[cfg(test)]
#[path = "canonical_frames_tests.rs"]
mod tests;

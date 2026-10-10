//! Stack-envelope arithmetic for reviewed template effects, not a QCO VM.
//! Callers must prove every effect/edge (including synchronous native re-entry)
//! and absence of exceptional stack changes before using this result.
use super::{Reject, Result};
use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub(super) struct Step {
    pub pop: usize,
    pub push: usize,
    /// Callee executes after `pop`, before `push`. Arguments still on the
    /// caller stack are counted there; callee peak excludes those arguments.
    pub call: Option<usize>,
    pub next: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct Frame {
    pub locals: usize,
    pub argc: usize,
    pub steps: Vec<Step>,
}

impl Frame {
    pub fn check_local(&self, index: usize) -> Result<()> {
        if index == 0 || index > self.locals + self.argc {
            return Err(Reject::Invalid);
        }
        Ok(())
    }
}

/// Reject local0 (saved caller frame pointer) and accesses beyond the incoming
/// arguments. This must be checked for every possible caller arity, not merely
/// the largest observed call. It does not prove types or operand-stack safety.
pub(super) fn check_locals(body: &[u8], argc: usize) -> Result<()> {
    let locals = usize::from(*body.first().ok_or(Reject::Truncated)?);
    if locals > 127 || argc > 255 {
        return Err(Reject::Invalid);
    }
    let frame = Frame {
        locals,
        argc,
        steps: vec![],
    };
    for instruction in super::bytecode::decode(body)? {
        if matches!(instruction.opcode, 0x18..=0x23 | 0x38..=0x3c | 0x41 | 0x46) {
            frame.check_local(usize::from(instruction.operands[0]))?;
        }
    }
    Ok(())
}

/// Call16's operand is a post-call DROP count, not the number of incoming
/// arguments. Authored callers may retain arguments and drop them later with
/// 3d/3e. The actual available slots must come from the caller's stack proof.
pub(super) fn check_call(body: &[u8], incoming_slots: usize, drop_count: u8) -> Result<()> {
    if usize::from(drop_count) > incoming_slots {
        return Err(Reject::Invalid);
    }
    check_locals(body, incoming_slots)
}

/// Exact flags2 table program: two incoming arguments, no locals, thirteen
/// column reads. Each read pushes row/column/receiver, invokes native1/2 with
/// two arguments, then writes the returned cell. ps(mode,row) is a separate
/// re-entry obligation: callers must supply its reviewed nested frame, if any.
pub(super) fn table_frame(
    table: &super::qcs::Table<'_>,
    mode_reentry: Option<usize>,
) -> Result<Frame> {
    let body = table.program.ok_or(Reject::Unsupported)?;
    if !table.targets.is_empty() || table.rows.is_empty() {
        return Err(Reject::Invalid);
    }
    for row in &table.rows {
        if row.len() != 13 {
            return Err(Reject::Invalid);
        }
        for (column, cell) in row.iter().enumerate() {
            let correct = if matches!(column, 6 | 7) {
                matches!(cell, super::qcs::Cell::String(_))
            } else {
                matches!(cell, super::qcs::Cell::Integer(_))
            };
            if !correct {
                return Err(Reject::Invalid);
            }
        }
    }
    super::template::table_program(body)?;
    check_locals(body, 2)?;
    let mut steps = Vec::new();
    for column in 0..13 {
        steps.push(Step {
            pop: 0,
            push: 3,
            call: None,
            next: vec![steps.len() + 1],
        });
        steps.push(Step {
            pop: 3,
            push: 2,
            call: None,
            next: vec![steps.len() + 1],
        });
        // The last two entries are the returned cell and its destination.
        steps.push(Step {
            pop: 2,
            push: 0,
            call: if column == 5 { mode_reentry } else { None },
            next: vec![steps.len() + 1],
        });
    }
    steps.push(Step {
        pop: 0,
        push: 0,
        call: None,
        next: vec![],
    });
    Ok(Frame {
        locals: 0,
        argc: 2,
        steps,
    })
}

/// Peak slots above each entry's incoming arguments. Saved frame-base occupies
/// one additional slot. Reject recursive synchronous re-entry rather than
/// pretending the outer event queue makes calls non-reentrant.
pub(super) fn peaks(frames: &[Frame], capacity: usize) -> Result<Vec<usize>> {
    if frames.is_empty() || frames.len() > 4096 || capacity == 0 || capacity > 65535 {
        return Err(Reject::Budget);
    }
    let total: usize = frames.iter().map(|f| f.steps.len()).sum();
    if total > 65536 {
        return Err(Reject::Budget);
    }
    let mut marks = vec![0_u8; frames.len()];
    let mut result = vec![0; frames.len()];
    for index in 0..frames.len() {
        visit(index, frames, capacity, &mut marks, &mut result, 0)?;
    }
    Ok(result)
}

fn visit(
    index: usize,
    frames: &[Frame],
    capacity: usize,
    marks: &mut [u8],
    result: &mut [usize],
    depth: usize,
) -> Result<usize> {
    if depth >= 128 {
        return Err(Reject::Budget);
    }
    let frame = frames.get(index).ok_or(Reject::Invalid)?;
    if marks[index] == 1 {
        return Err(Reject::Unsupported);
    }
    if marks[index] == 2 {
        return Ok(result[index]);
    }
    if frame.locals > 127 || frame.argc > 255 || frame.steps.is_empty() {
        return Err(Reject::Invalid);
    }
    marks[index] = 1;
    let base = frame.locals + 1;
    let mut peak = base;
    let mut heights = vec![None; frame.steps.len()];
    heights[0] = Some(0usize);
    let mut pending = VecDeque::from([0]);
    let mut returned = false;
    while let Some(pc) = pending.pop_front() {
        let height = heights[pc].ok_or(Reject::Invalid)?;
        let step = &frame.steps[pc];
        let remaining = height.checked_sub(step.pop).ok_or(Reject::Invalid)?;
        let nested = if let Some(callee) = step.call {
            // Callee arguments must be present above our saved frame base.
            if frames.get(callee).ok_or(Reject::Invalid)?.argc > remaining {
                return Err(Reject::Invalid);
            }
            visit(callee, frames, capacity, marks, result, depth + 1)?
        } else {
            0
        };
        peak = peak.max(
            base.checked_add(remaining)
                .and_then(|n| n.checked_add(nested))
                .ok_or(Reject::Budget)?,
        );
        let after = remaining.checked_add(step.push).ok_or(Reject::Budget)?;
        peak = peak.max(base.checked_add(after).ok_or(Reject::Budget)?);
        if peak.checked_add(frame.argc).ok_or(Reject::Budget)? > capacity {
            return Err(Reject::Budget);
        }
        if step.next.is_empty() {
            if after != 0 {
                return Err(Reject::Invalid);
            }
            returned = true;
        }
        for &next in &step.next {
            let old = heights.get_mut(next).ok_or(Reject::Invalid)?;
            match *old {
                Some(previous) if previous != after => return Err(Reject::Invalid),
                Some(_) => {}
                None => {
                    *old = Some(after);
                    pending.push_back(next);
                }
            }
        }
    }
    // Do not permit unreviewed dead blocks to conceal unsupported effects.
    if !returned || heights.iter().any(Option::is_none) {
        return Err(Reject::Unsupported);
    }
    marks[index] = 2;
    result[index] = peak;
    Ok(peak)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(pop: usize, push: usize, call: Option<usize>, next: &[usize]) -> Step {
        Step {
            pop,
            push,
            call,
            next: next.to_vec(),
        }
    }

    #[test]
    fn synchronous_table_reentry_counts_live_caller_and_arguments() {
        let frames = vec![
            Frame {
                locals: 3,
                argc: 0,
                steps: vec![
                    step(0, 2, None, &[1]),
                    step(0, 0, Some(1), &[2]),
                    step(2, 0, None, &[]),
                ],
            },
            Frame {
                locals: 1,
                argc: 2,
                steps: vec![step(0, 5, None, &[1]), step(5, 0, None, &[])],
            },
        ];
        assert_eq!(peaks(&frames, 13).unwrap(), vec![13, 7]);
        assert_eq!(peaks(&frames, 12), Err(Reject::Budget));
        let mut recursive = frames.clone();
        recursive[1].steps[0].call = Some(0);
        assert_eq!(peaks(&recursive, 772), Err(Reject::Unsupported));
    }

    #[test]
    fn bad_frame_indices_underflow_join_drift_and_unbalanced_return_reject() {
        let mut frame = Frame {
            locals: 2,
            argc: 1,
            steps: vec![step(0, 0, None, &[])],
        };
        for local in 1..=3 {
            frame.check_local(local).unwrap();
        }
        for local in [0, 4, 255] {
            assert!(frame.check_local(local).is_err());
        }
        frame.steps = vec![step(1, 0, None, &[])];
        assert!(peaks(&[frame.clone()], 772).is_err());
        frame.steps = vec![step(0, 1, None, &[])];
        assert!(peaks(&[frame.clone()], 772).is_err());
        frame.steps = vec![
            step(0, 0, None, &[1, 2]),
            step(0, 1, None, &[2]),
            step(0, 0, None, &[]),
        ];
        assert!(peaks(&[frame.clone()], 772).is_err());
        frame.steps = vec![step(0, 1, None, &[0, 1]), step(1, 0, None, &[])];
        assert!(peaks(&[frame], 772).is_err());
    }

    #[test]
    fn local_operands_cannot_escape_to_saved_base_or_caller_frame() {
        for opcode in [
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x38, 0x39,
            0x3a, 0x3b, 0x3c, 0x41, 0x46,
        ] {
            for index in [0, 1, 2, 3, 4, 255] {
                let mut body = vec![2, 0, 0, opcode, index];
                if opcode == 0x41 {
                    body.push(1);
                }
                body.push(0x3f);
                assert_eq!(check_locals(&body, 1).is_ok(), (1..=3).contains(&index));
                if index == 3 {
                    assert!(check_locals(&body, 0).is_err());
                }
            }
        }
        assert!(check_locals(&[128, 0, 0, 0x3f], 0).is_err());
    }

    #[test]
    fn retained_arguments_are_not_confused_with_post_call_drop_count() {
        let callee = [0, 0, 5, 0x20, 1, 0x3f];
        check_call(&callee, 1, 0).unwrap();
        check_call(&callee, 1, 1).unwrap();
        assert!(check_call(&callee, 0, 0).is_err());
        assert!(check_call(&callee, 1, 2).is_err());
        let parent = Frame {
            locals: 0,
            argc: 0,
            steps: vec![
                step(0, 1, None, &[1]),
                step(0, 0, Some(1), &[2]),
                step(1, 0, None, &[]),
            ],
        };
        let child = Frame {
            locals: 0,
            argc: 1,
            steps: vec![step(0, 0, None, &[])],
        };
        assert_eq!(peaks(&[parent, child], 3).unwrap(), [3, 1]);
    }
}

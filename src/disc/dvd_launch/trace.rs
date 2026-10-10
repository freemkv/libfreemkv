//! Finite, path-sensitive execution of the explicitly supported command subset.

use super::vm::{self, Flow, Registers};
use crate::disc::{DvdLaunchReviewReason as Reason, DvdLaunchStep};

#[derive(Clone, Debug)]
pub(super) struct Outcome {
    pub flow: Flow,
    pub registers: Registers,
    pub steps: Vec<DvdLaunchStep>,
}

/// Unknown comparisons explore both branches. They are not proof of either
/// branch: callers must require agreement of all terminal destinations.
pub(super) fn run(
    commands: &[DvdLaunchStep],
    registers: Registers,
) -> Result<Vec<Outcome>, Reason> {
    let mut pending = vec![(0, registers, Vec::new())];
    let mut out = Vec::new();
    let mut budget = 512usize;
    while let Some((mut pc, mut registers, mut steps)) = pending.pop() {
        loop {
            if pending.len() + out.len() >= 64 || steps.len() >= 256 {
                return Err(Reason::BudgetExceeded);
            }
            budget = budget.checked_sub(1).ok_or(Reason::BudgetExceeded)?;
            let Some(step) = commands.get(pc) else {
                if pc != commands.len() {
                    return Err(Reason::IncompleteNavigation);
                }
                out.push(Outcome {
                    flow: Flow::Next,
                    registers,
                    steps,
                });
                break;
            };
            let decoded = vm::decode(&step.command)?;
            let condition = registers.condition(decoded.compare);
            steps.push(step.clone());
            if condition == Some(false) {
                pc += 1;
                continue;
            }
            if condition.is_none() {
                pending.push((pc + 1, registers.clone(), steps.clone()));
            }
            match vm::execute(&step.command, &mut registers)? {
                Flow::Next => pc += 1,
                Flow::Goto(line) => {
                    pc = usize::from(line) - 1;
                    if pc >= commands.len() {
                        return Err(Reason::IncompleteNavigation);
                    }
                }
                flow => {
                    out.push(Outcome {
                        flow,
                        registers,
                        steps,
                    });
                    break;
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commands(raw: &[[u8; 8]]) -> Vec<DvdLaunchStep> {
        raw.iter()
            .enumerate()
            .map(|(i, command)| DvdLaunchStep {
                vts: 1,
                menu_vob: false,
                byte_offset: i as u32 * 8,
                command: *command,
            })
            .collect()
    }

    #[test]
    fn register_dispatch_preserves_authored_destinations() {
        let raw = commands(&[
            [0, 0xa1, 0, 0, 0, 3, 0, 3],
            [0x30, 5, 0, 1, 0, 1, 0, 0],
            [0x51, 0, 0, 0x81, 0, 0, 0, 0],
            [0x30, 5, 0, 1, 0, 2, 0, 0],
        ]);
        for (value, title) in [(1, 1), (3, 2)] {
            let mut r = Registers::default();
            r.gprm[0] = Some(value);
            let out = run(&raw, r).unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].flow, Flow::VtsTitle { title, part: 1 });
        }
        let out = run(&raw, Registers::default()).unwrap();
        assert_eq!(out.len(), 2);
        assert_ne!(out[0].flow, out[1].flow);
    }

    #[test]
    fn loops_and_out_of_list_gotos_fail_closed() {
        for raw in [[0, 1, 0, 0, 0, 0, 0, 1], [0, 1, 0, 0, 0, 0, 0, 2]] {
            assert!(run(&commands(&[raw]), Registers::default()).is_err());
        }
    }
}

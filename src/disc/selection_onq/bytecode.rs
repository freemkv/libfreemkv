//! Bounded instruction index for the reviewed lj dispatcher. This does not
//! execute code or infer effects of dynamic operands/calls.
use super::{Reject, Result};
use std::collections::BTreeSet;

const MAX_INSTRUCTIONS: usize = 65536;

#[derive(Debug)]
pub(super) struct Instruction<'a> {
    pub pc: usize,
    pub opcode: u8,
    pub operands: &'a [u8],
    pub branch_target: bool,
}

impl Instruction<'_> {
    pub fn integer(&self) -> Option<i32> {
        match (self.opcode, self.operands) {
            (1, [a]) => Some(i32::from(*a)),
            (2, [a, b]) => Some(i32::from(i16::from_be_bytes([*a, *b]))),
            (3, [a, b]) => Some(i32::from(u16::from_be_bytes([*a, *b]))),
            (4, [a, b, c, d]) => Some(i32::from_be_bytes([*a, *b, *c, *d])),
            _ => None,
        }
    }
}

pub(super) fn decode(body: &[u8]) -> Result<Vec<Instruction<'_>>> {
    if body.len() < 4 || body.len() > super::binary::MAX_BYTES {
        return Err(Reject::Invalid);
    }
    let mut result = Vec::new();
    let mut branches = Vec::new();
    let mut pc = 3;
    while pc < body.len() {
        if result.len() >= MAX_INSTRUCTIONS {
            return Err(Reject::Budget);
        }
        let opcode = body[pc];
        // Widths follow lj.a(Lh;[BII)I, including the distinction between
        // signed (02) and unsigned (03) short constants. 0b/12 throw.
        let width = match opcode {
            0x01
            | 0x16
            | 0x18..=0x23
            | 0x30..=0x33
            | 0x38..=0x3e
            | 0x42
            | 0x46
            | 0x48
            | 0x49
            | 0x4b => 1,
            0x02 | 0x03 | 0x13 | 0x14 | 0x15 | 0x17 | 0x41 | 0x43..=0x45 => 2,
            0x04 => 4,
            0x05 => body[pc + 1..]
                .iter()
                .take(4097)
                .position(|b| *b == 0)
                .map(|n| n + 1)
                .ok_or(Reject::Invalid)?,
            0x00
            | 0x06..=0x0a
            | 0x0c..=0x11
            | 0x24..=0x2f
            | 0x34..=0x37
            | 0x3f
            | 0x40
            | 0x47
            | 0x4a
            | 0x4c..=0x53 => 0,
            // 54 has callee-dependent width (authored call consumes argc;
            // native call does not). Never guess its boundary from raw bytes.
            _ => return Err(Reject::Unsupported),
        };
        let next = pc + 1 + width;
        let operands = body.get(pc + 1..next).ok_or(Reject::Truncated)?;
        if matches!(opcode, 0x13 | 0x14 | 0x43..=0x45) {
            let displacement = i16::from_be_bytes([operands[0], operands[1]]);
            // Dispatcher cursor points at the operand, not after its two bytes.
            let target = (pc + 1)
                .checked_add_signed(displacement as isize)
                .ok_or(Reject::Invalid)?;
            branches.push(target);
        }
        result.push(Instruction {
            pc,
            opcode,
            operands,
            branch_target: false,
        });
        pc = next;
    }
    let boundaries: BTreeSet<_> = result.iter().map(|i| i.pc).collect();
    if branches.iter().any(|target| !boundaries.contains(target))
        || result.last().map(|i| i.opcode) != Some(0x3f)
    {
        return Err(Reject::Invalid);
    }
    let branches: BTreeSet<_> = branches.into_iter().collect();
    for instruction in &mut result {
        instruction.branch_target = branches.contains(&instruction.pc);
    }
    Ok(result)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Write {
    pub pc: usize,
    pub opcode: u8,
    /// Syntactic hint only, not a substitute for a reviewed stack/effect
    /// contract. Branch entry at the store invalidates even this local hint.
    /// None is unresolved, never evidence that a write is harmless.
    pub literal_destination: Option<i32>,
}

pub(super) fn writes(code: &[Instruction<'_>]) -> Vec<Write> {
    code.iter()
        .enumerate()
        .filter(|(_, instruction)| {
            matches!(
                instruction.opcode,
                0x1a | 0x1b
                    | 0x1e
                    | 0x1f
                    | 0x22
                    | 0x23
                    | 0x26
                    | 0x27
                    | 0x2a
                    | 0x2b
                    | 0x2e
                    | 0x2f
                    | 0x32
                    | 0x33
                    | 0x38..=0x3c | 0x41 | 0x42 | 0x49
            )
        })
        .map(|(index, instruction)| Write {
            pc: instruction.pc,
            opcode: instruction.opcode,
            literal_destination: if !instruction.branch_target
                && matches!(
                    instruction.opcode,
                    0x26 | 0x27 | 0x2a | 0x2b | 0x2e | 0x2f | 0x32 | 0x33 | 0x42 | 0x49
                ) {
                index.checked_sub(1).and_then(|i| code[i].integer())
            } else {
                None
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branches_use_signed_operand_relative_offsets_and_exact_boundaries() {
        let body = [0, 0, 0, 1, 9, 0x14, 0xff, 0xfd, 0x3f];
        let code = decode(&body).unwrap();
        assert_eq!(code.iter().map(|i| i.pc).collect::<Vec<_>>(), [3, 5, 8]);
        for displacement in [-6_i16, -5, -4, -2, 0, 1, 3] {
            let mut bad = body;
            bad[6..8].copy_from_slice(&displacement.to_be_bytes());
            assert!(decode(&bad).is_err(), "{displacement}");
        }
        let mut self_loop = body;
        self_loop[6..8].copy_from_slice(&(-1_i16).to_be_bytes());
        assert!(decode(&self_loop).is_ok()); // syntactically valid, not termination proof
        for end in 0..body.len() {
            assert!(decode(&body[..end]).is_err());
        }
    }

    #[test]
    fn dynamic_writes_are_retained_not_silently_excluded() {
        let code = decode(&[0, 0, 0, 2, 0x41, 0x8d, 0x2e, 0x20, 1, 0x2e, 0x3f]).unwrap();
        let effects = writes(&code);
        assert_eq!(effects.len(), 2);
        assert_eq!(effects[0].literal_destination, Some(0x418d));
        assert_eq!(effects[1].literal_destination, None);
        assert!(decode(&[0, 0, 0, 0xff, 0x3f]).is_err());
        assert!(decode(&[0, 0, 0, 0x54, 0, 0x3f]).is_err());
        assert!(decode(&[0, 0, 0, 5, b'x', 0x3f]).is_err());
        assert_eq!(
            decode(&[0, 0, 0, 2, 0xff, 0xff, 0x3f]).unwrap()[0].integer(),
            Some(-1)
        );
        assert_eq!(
            decode(&[0, 0, 0, 3, 0xff, 0xff, 0x3f]).unwrap()[0].integer(),
            Some(65535)
        );
    }

    #[test]
    fn intrinsic_arity_is_an_operand_and_branch_entry_invalidates_write_hint() {
        // Independent excerpt: intrinsic 8 with zero args occupies three bytes.
        let body = [0, 0, 0, 0x15, 8, 0, 0x14, 0xff, 0xfc, 0x3f];
        assert_eq!(decode(&body).unwrap()[0].operands, [8, 0]);
        let mut bad = body;
        bad[7..9].copy_from_slice(&(-2_i16).to_be_bytes()); // into argc
        assert!(decode(&bad).is_err());
        let body = [0, 0, 0, 2, 0x41, 0x8d, 0x2e, 0x14, 0xff, 0xfe, 0x3f];
        assert_eq!(writes(&decode(&body).unwrap())[0].literal_destination, None);
    }
}

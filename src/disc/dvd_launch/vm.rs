//! Strict command subset for bounded menu traces, not a general DVD player.

use crate::disc::DvdLaunchReviewReason as Reason;
use crate::dvdnav::vmcmd::{self, Compare, Instr};

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct Registers {
    pub gprm: [Option<u16>; 16],
    pub sprm: [Option<u16>; 24],
}
impl Registers {
    fn read(&self, reg: u8) -> Option<u16> {
        match reg {
            0..=15 => self.gprm[reg as usize],
            128..=151 => self.sprm[(reg - 128) as usize],
            _ => None,
        }
    }
    pub fn condition(&self, c: Option<Compare>) -> Option<bool> {
        let Some(c) = c else { return Some(true) };
        let a = self.read(c.lhs_reg)?;
        let b = if c.immediate {
            c.imm
        } else {
            self.read(c.rhs_reg)?
        };
        match c.op {
            1 => Some(a & b != 0),
            2 => Some(a == b),
            3 => Some(a != b),
            4 => Some(a >= b),
            5 => Some(a > b),
            6 => Some(a <= b),
            7 => Some(a < b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Flow {
    Next,
    Goto(u8),
    Pgc(u16),
    Program(u8),
    Tail,
    TopProgram,
    VtsMenu { vts: u8, menu: u8 },
    VmgMenu(u8),
    VtsTitle { title: u8, part: u16 },
    ReturnMenu,
}

fn valid_reg(reg: u8) -> bool {
    reg < 16 || (128..=151).contains(&reg)
}
fn bad<T>() -> Result<T, Reason> {
    Err(Reason::UnsupportedNavigation)
}

/// Validate ALL fields used by this subset before delegating operand decoding.
/// Unknown commands must never inherit the movie resolver's NOP fallback.
pub(super) fn decode(b: &[u8; 8]) -> Result<vmcmd::Command, Reason> {
    let c = vmcmd::decode(b);
    if let Some(p) = c.compare
        && (!valid_reg(p.lhs_reg) || (!p.immediate && (!valid_reg(p.rhs_reg) || b[4] != 0)))
    {
        return bad();
    }
    match b[0] {
        0 if b[1] & 15 == 1 && b[2] == 0 && b[6] == 0 && b[7] > 0 => {
            if c.compare.is_none() && b[3..6] != [0; 3] {
                return bad();
            }
        }
        0 if *b == [0; 8] => {}
        0x61 | 0x71 | 0x79 if b[2] == 0 && b[3] < 16 && b[1] & 0xf0 == 0 => {
            if b[0] == 0x61 && (b[4] != 0 || !valid_reg(b[5])) {
                return bad();
            }
            link(b)?;
        }
        0x41 | 0x51 if b[1] & 0xf0 == 0 && b[2] == 0 => {
            // SetSTN: each enabled operand is seven-bit immediate or four-bit GPRM.
            for &v in &b[3..6] {
                if v & 0x80 == 0 && v != 0 {
                    return bad();
                }
                if b[0] == 0x41 && v & 0x70 != 0 {
                    return bad();
                }
            }
            link(b)?;
        }
        0x46 | 0x56 if b[1] & 0xf0 == 0 && b[2..4] == [0; 2] => {
            if b[0] == 0x46 && (b[4] != 0 || b[5] > 15) {
                return bad();
            }
            link(b)?;
        }
        0x20 if b[2] == 0 => {
            if c.compare.is_none() && b[3..6] != [0; 3] {
                return bad();
            }
            if !matches!(b[1] & 15, 1 | 4 | 6) {
                return bad();
            }
            link(b)?;
        }
        0x30 if b[1] == 5
            && b[4] == 0
            && b[6..] == [0; 2]
            && b[2] & 0xfc == 0
            && b[5] > 0
            && b[5] <= 99 => {}
        0x30 if b[1] == 6 && b[6..] == [0; 2] => match b[5] {
            0x42 if b[2..5] == [0; 3] => {}
            0x83 if b[2] == 0 && b[3] > 0 && b[4] > 0 && b[4] <= 99 => {}
            _ => return bad(),
        },
        // Only explicit title returns to a menu, with no hidden comparison.
        0x30 if b[1] == 8 && b[2] == 0 && b[6..] == [0; 2] => match b[5] {
            0x42 | 0x83 if b[3] == 0 => {}
            0xc0 if (1..=99).contains(&b[3]) => {}
            _ => return bad(),
        },
        _ => return bad(),
    }
    Ok(c)
}

fn link(b: &[u8; 8]) -> Result<Flow, Reason> {
    match b[1] & 15 {
        0 if b[6..] == [0; 2] => Ok(Flow::Next),
        1 if b[6] == 0 && b[7] == 13 => Ok(Flow::Tail),
        1 if b[6] == 0 && b[7] == 5 => Ok(Flow::TopProgram),
        4 if b[6] & 0x80 == 0 && u16::from_be_bytes([b[6], b[7]]) > 0 => {
            Ok(Flow::Pgc(u16::from_be_bytes([b[6], b[7]])))
        }
        6 if b[6] & 3 == 0 && b[6] >> 2 <= 36 && b[7] > 0 && b[7] <= 127 => Ok(Flow::Program(b[7])),
        _ => bad(),
    }
}

fn apply_link(b: &[u8; 8], r: &mut Registers) -> Result<Flow, Reason> {
    let flow = link(b)?;
    if matches!(flow, Flow::Program(_)) && b[6] != 0 {
        r.sprm[8] = Some(u16::from(b[6] >> 2) << 10);
    }
    Ok(flow)
}

/// Apply an already-decided branch. Unknown operands remain unknown, never zero.
pub(super) fn execute(b: &[u8; 8], r: &mut Registers) -> Result<Flow, Reason> {
    let c = decode(b)?;
    match c.instr {
        Instr::Nop => Ok(Flow::Next),
        Instr::Goto { line } => Ok(Flow::Goto(line)),
        Instr::SetGprm {
            reg,
            op,
            immediate,
            imm,
            src,
        } => {
            let v = if immediate { Some(imm) } else { r.read(src) };
            r.gprm[reg as usize] = match op {
                1 => v,
                9 => r.gprm[reg as usize].zip(v).map(|(a, b)| a & b),
                _ => return bad(),
            };
            apply_link(b, r)
        }
        Instr::SetSystem => {
            if b[0] & 15 == 1 {
                for i in 1..=3 {
                    let v = b[i + 2];
                    if v & 0x80 != 0 {
                        r.sprm[i] = if b[0] & 0x10 != 0 {
                            Some(u16::from(v & 0x7f))
                        } else {
                            r.gprm[(v & 15) as usize]
                        };
                    }
                }
            } else {
                r.sprm[8] = if b[0] & 0x10 != 0 {
                    Some(u16::from_be_bytes([b[4], b[5]]))
                } else {
                    r.gprm[b[5] as usize]
                };
            }
            apply_link(b, r)
        }
        Instr::LinkPgcn { .. } | Instr::LinkPgn { .. } | Instr::LinkSub { .. } => apply_link(b, r),
        Instr::JumpVtsPtt { ttn, pttn } => Ok(Flow::VtsTitle {
            title: ttn,
            part: pttn,
        }),
        Instr::JumpSsVtsm { vts, menu, .. } => Ok(Flow::VtsMenu { vts, menu }),
        Instr::JumpSsVmgm { menu } => Ok(Flow::VmgMenu(menu)),
        Instr::CallSs { .. } => Ok(Flow::ReturnMenu),
        _ => bad(),
    }
}

#[cfg(test)]
#[path = "vm_tests.rs"]
mod tests;

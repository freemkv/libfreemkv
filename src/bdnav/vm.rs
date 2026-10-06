//! The HDMV navigation VM — a bounded re-implementation of the subset of HDMV
//! navigation semantics needed to follow First-Play to the feature
//! `PlayPlayList`. Never panics; hard step/switch caps.
//!
//! It resolves generically — there is no per-disc special-casing. When
//! First-Play is a BD-J title (the feature is chosen by a Java Xlet), or the
//! program does not reach a `PlayPL` on a caller-approved feature candidate, it
//! abstains (`None`) and selection falls back to the structural/heuristic order.

use super::index::{Index, PlaybackObj};
use super::mobj::MovieObject;

/// Top bit of an operand selects PSR (else GPR).
const PSR_FLAG: u32 = 0x8000_0000;
/// Upper bound on VM steps before declaring non-convergence.
const MAX_STEPS: usize = 1_000_000;
/// Cap on object/title switches — a dispatcher runs in one object; this only
/// bounds pathological jump chains.
const MAX_SWITCHES: usize = 1024;

// BD player power-on register defaults (indices 0..=61); unlisted = 0.
// Load-bearing: PSR4/5 = 0xffff ("no title/chapter selected"), PSR6/7/8 = 0 —
// these steer dispatcher compares; rest set for fidelity with real player.
fn psr_init() -> [u32; 128] {
    let mut p = [0u32; 128];
    let vals: &[(usize, u32)] = &[
        (0, 1),
        (1, 0xff),
        (2, 0x0fff_0fff),
        (3, 1),
        (4, 0xffff),
        (5, 0xffff),
        (10, 0xffff),
        (12, 0xff),
        (13, 0xff),
        (14, 0xffff),
        (15, 0xAAAA),
        (16, 0x00ff_ffff),
        (17, 0x00ff_ffff),
        (18, 0x00ff_ffff),
        (19, 0xffff),
        (20, 2),
        (29, 0x0000_0003),
        (30, 0x0001_ffff),
        (31, 0x0003_0200),
        (36, 0xffff),
        (37, 0xffff),
        (42, 0xffff),
        (44, 0xff),
    ];
    for &(i, v) in vals {
        p[i] = v;
    }
    for slot in p.iter_mut().take(62).skip(48) {
        *slot = 0xffff_ffff;
    }
    p
}

struct Vm<'a> {
    mobjs: &'a [MovieObject],
    index: &'a Index,
    gpr: [u32; 4096],
    psr: [u32; 128],
    // Per-GPR: value derived from RND, so any branch on it is undecidable.
    gpr_random: Box<[bool; 4096]>,
}

impl<'a> Vm<'a> {
    fn new(mobjs: &'a [MovieObject], index: &'a Index) -> Self {
        Self {
            mobjs,
            index,
            gpr: [0; 4096],
            psr: psr_init(),
            gpr_random: Box::new([false; 4096]),
        }
    }
    // Whether a non-immediate operand reads an RND-derived GPR.
    fn random(&self, imm: bool, raw: u32) -> bool {
        !imm && raw & PSR_FLAG == 0 && self.gpr_random[(raw & 0xfff) as usize]
    }
    fn set_random(&mut self, raw: u32, r: bool) {
        if raw & PSR_FLAG == 0 {
            self.gpr_random[(raw & 0xfff) as usize] = r;
        }
    }
    fn rd(&self, val: u32) -> u32 {
        if val & PSR_FLAG != 0 {
            self.psr[(val & 0x7f) as usize]
        } else {
            self.gpr[(val & 0xfff) as usize]
        }
    }
    fn wr(&mut self, val: u32, x: u32) {
        // Per the HDMV nav spec, a store to a PSR-tagged register is refused;
        // only GPR writes take effect. PSR values come only from power-on init,
        // never from a nav SET/SWAP operand.
        if val & PSR_FLAG == 0 {
            self.gpr[(val & 0xfff) as usize] = x;
        }
    }
    fn fetch(&self, imm: bool, raw: u32) -> u32 {
        if imm { raw } else { self.rd(raw) }
    }
    /// Resolve a JumpTitle target: title 0 = Top Menu, `1..=N` = `titles[i-1]`,
    /// `0xFFFF` = First Play (BD-ROM title numbering). Any other value is an
    /// invalid title reference and abstains (`None`).
    fn title_obj(&self, title: u32) -> Option<PlaybackObj> {
        let n = self.index.titles.len() as u32;
        if title == 0 {
            Some(self.index.top_menu)
        } else if title <= n {
            self.index.titles.get((title - 1) as usize).copied()
        } else if title == 0xFFFF {
            Some(self.index.first_play)
        } else {
            None
        }
    }
}

/// Run First-Play and return the first `PlayPL`/`PlayPL_PM`/`PlayPL_PI` whose
/// playlist id passes `is_feature`. `None` = BD-J boundary, non-convergence, or
/// no approved feature playlist reached.
pub(crate) fn resolve(
    index: &Index,
    mobjs: &[MovieObject],
    is_feature: &dyn Fn(u16) -> bool,
) -> Option<u16> {
    // Enter at First-Play when it is an HDMV object that exists; otherwise abstain
    // (BD-J boundary, or `0xffff` "no object"). We don't chase Top-Menu's "Play
    // Movie" target here — it lives in menu .m2ts IG button commands (future work).
    let start = match index.first_play {
        PlaybackObj::Hdmv { id_ref } if (id_ref as usize) < mobjs.len() => id_ref as usize,
        _ => return None,
    };
    let mut vm = Vm::new(mobjs, index);
    run(&mut vm, start, is_feature)
}

fn run(vm: &mut Vm, mut obj_id: usize, is_feature: &dyn Fn(u16) -> bool) -> Option<u16> {
    let mut pc = 0usize;
    let mut steps = 0usize;
    let mut switches = 0usize;
    loop {
        steps += 1;
        if steps > MAX_STEPS {
            return None;
        }
        let obj = vm.mobjs.get(obj_id)?;
        // Ran off the end without a feature PlayPL → abstain.
        let c = *obj.cmds.get(pc)?;
        let mut npc = pc + 1;
        let dst = if c.op_cnt > 0 {
            vm.fetch(c.imm_op1, c.dst)
        } else {
            0
        };
        let src = if c.op_cnt > 1 {
            vm.fetch(c.imm_op2, c.src)
        } else {
            0
        };
        let dst_rnd = c.op_cnt > 0 && vm.random(c.imm_op1, c.dst);
        let src_rnd = c.op_cnt > 1 && vm.random(c.imm_op2, c.src);
        // A branch, play or compare on an RND-derived value is undecidable.
        if (c.grp == 0 && dst_rnd) || (c.grp == 1 && (dst_rnd || src_rnd)) {
            return None;
        }
        match c.grp {
            // BRANCH
            0 => match c.sub_grp {
                // GOTO
                0 => match c.branch_opt {
                    0x01 => npc = dst as usize, // GOTO
                    0x02 => return None,        // BREAK — terminate
                    _ => {}                     // NOP / other
                },
                // JUMP
                1 => match c.branch_opt {
                    0x00 | 0x02 => {
                        // JumpObject / CallObject (return address not modelled —
                        // for feature resolution we only follow the play path).
                        switches += 1;
                        if switches > MAX_SWITCHES || dst as usize >= vm.mobjs.len() {
                            return None;
                        }
                        obj_id = dst as usize;
                        pc = 0;
                        continue;
                    }
                    0x01 | 0x03 => {
                        // JumpTitle / CallTitle — resolve through the index table.
                        switches += 1;
                        if switches > MAX_SWITCHES {
                            return None;
                        }
                        match vm.title_obj(dst) {
                            Some(PlaybackObj::Hdmv { id_ref })
                                if (id_ref as usize) < vm.mobjs.len() =>
                            {
                                obj_id = id_ref as usize;
                                pc = 0;
                                continue;
                            }
                            // BD-J / unknown / invalid title → abstain.
                            _ => return None,
                        }
                    }
                    _ => {} // RESUME / other — fall through
                },
                // PLAY: PlayPL / PlayPL_PI / PlayPL_PM emit a playlist id (the
                // rest — Terminate / Link — fall through). A logo/pre-roll whose
                // id isn't a feature candidate lets autoplay resume at pc + 1.
                2 if matches!(c.branch_opt, 0x00..=0x02) => {
                    // A playlist id is 16-bit; an operand that doesn't fit is not one.
                    if let Ok(id) = u16::try_from(dst)
                        && is_feature(id)
                    {
                        return Some(id);
                    }
                }
                _ => {}
            },
            // CMP — skip the next command when the compare is false.
            1 => {
                let truth = match c.cmp_opt {
                    0x01 => (dst & !src) == 0, // BC: every dst bit set in src (hdmv_vm.c)
                    0x02 => dst == src,
                    0x03 => dst != src,
                    0x04 => dst >= src,
                    0x05 => dst > src,
                    0x06 => dst <= src,
                    0x07 => dst < src,
                    _ => true,
                };
                if !truth {
                    npc = pc + 2;
                }
            }
            // SET (sub_grp 0). SETSYSTEM (sub_grp 1) only mutates system PSRs
            // whose values the feature path does not branch on — skip it.
            2 if c.sub_grp == 0 => {
                let r: Option<u32> = match c.set_opt {
                    0x01 => Some(src), // MOVE
                    0x02 => {
                        // SWAP exchanges the two operands; a store to an operand
                        // flagged immediate is refused, and `wr` already drops
                        // PSR stores.
                        if !c.imm_op1 {
                            vm.wr(c.dst, src);
                            vm.set_random(c.dst, src_rnd);
                        }
                        if !c.imm_op2 {
                            vm.wr(c.src, dst);
                            vm.set_random(c.src, dst_rnd);
                        }
                        None
                    }
                    0x03 => Some(dst.saturating_add(src)), // libbluray ADD_u32 saturates
                    0x04 => Some(dst.saturating_sub(src)),
                    0x05 => Some(dst.saturating_mul(src)), // libbluray MUL_u32 saturates
                    0x06 => Some(dst.checked_div(src).unwrap_or(0xffff_ffff)),
                    0x07 => Some(dst.checked_rem(src).unwrap_or(0xffff_ffff)),
                    0x08 => Some(dst), // RND: value unknown, tainted below
                    0x09 => Some(dst & src),
                    0x0a => Some(dst | src),
                    0x0b => Some(dst ^ src),
                    // libbluray: bit numbers / shift counts >= 32 are not masked.
                    0x0c => Some(1u32.checked_shl(src).map_or(dst, |b| dst | b)),
                    0x0d => Some(1u32.checked_shl(src).map_or(dst, |b| dst & !b)),
                    0x0e => Some(dst.checked_shl(src).unwrap_or(0)),
                    0x0f => Some(dst.checked_shr(src).unwrap_or(0)),
                    _ => None,
                };
                if let Some(r) = r
                    && !c.imm_op1
                {
                    vm.wr(c.dst, r);
                    let rnd = c.set_opt == 0x08 || src_rnd || (c.set_opt != 0x01 && dst_rnd);
                    vm.set_random(c.dst, rnd);
                }
            }
            _ => {}
        }
        pc = npc;
    }
}

#[cfg(test)]
#[path = "vm_tests.rs"]
mod tests;

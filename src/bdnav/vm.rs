//! The HDMV navigation VM — a faithful, bounded re-implementation of the subset
//! of libbluray's `hdmv_vm.c` needed to follow First-Play to the feature
//! `PlayPlayList`. It "mimics a real player": correct power-on register state
//! (libbluray `register.c` `bd_psr_init`) is what makes an obfuscated dispatcher
//! (e.g. Sony's) converge rather than loop. Never panics; hard step/switch caps.
//!
//! It resolves generically — there is no vendor special-casing. When First-Play
//! is a BD-J title (the feature is chosen by a Java Xlet), or the program does
//! not reach a `PlayPL` on a caller-approved feature candidate, it abstains
//! (`None`) and selection falls back to the structural/heuristic order.

use super::index::{Index, PlaybackObj};
use super::mobj::MovieObject;

/// Top bit of an operand selects PSR (else GPR).
const PSR_FLAG: u32 = 0x8000_0000;
/// Mirror of libbluray `MAX_LOOP`.
const MAX_STEPS: usize = 1_000_000;
/// Cap on object/title switches — the obfuscated dispatcher runs in one object;
/// this only bounds pathological jump chains.
const MAX_SWITCHES: usize = 1024;

/// libbluray `bd_psr_init` power-on defaults (indices 0..=61); unlisted = 0. The
/// load-bearing values are PSR4/5 = `0xffff` ("no title/chapter selected") and
/// PSR6/7/8 = 0, which steer the dispatcher's compares; the rest are copied for
/// fidelity.
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
}

impl Vm<'_> {
    fn rd(&self, val: u32) -> u32 {
        if val & PSR_FLAG != 0 {
            self.psr[(val & 0x7f) as usize]
        } else {
            self.gpr[(val & 0xfff) as usize]
        }
    }
    fn wr(&mut self, val: u32, x: u32) {
        if val & PSR_FLAG != 0 {
            self.psr[(val & 0x7f) as usize] = x;
        } else {
            self.gpr[(val & 0xfff) as usize] = x;
        }
    }
    fn fetch(&self, imm: bool, raw: u32) -> u32 {
        if imm { raw } else { self.rd(raw) }
    }
    /// Resolve a JumpTitle target: title 0 = Top Menu, `1..=N` = `titles[i-1]`,
    /// `N+1` = First Play (BD spec title numbering).
    fn title_obj(&self, title: u32) -> Option<PlaybackObj> {
        let n = self.index.titles.len() as u32;
        if title == 0 {
            Some(self.index.top_menu)
        } else if title <= n {
            self.index.titles.get((title - 1) as usize).copied()
        } else if title == n + 1 {
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
    // Enter at First-Play when it is an HDMV object that exists; otherwise
    // abstain (BD-J boundary, or a `0xffff` "no object"). We deliberately do not
    // chase the Top-Menu program here — its "Play Movie" target usually lives in
    // the IG-stream button navigation commands inside the menu `.m2ts`, not in
    // `MovieObject.bdmv` (future work).
    let start = match index.first_play {
        PlaybackObj::Hdmv { id_ref } if (id_ref as usize) < mobjs.len() => id_ref as usize,
        _ => return None,
    };
    let mut vm = Vm {
        mobjs,
        index,
        gpr: [0; 4096],
        psr: psr_init(),
    };
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
        let c = match obj.cmds.get(pc) {
            Some(c) => *c,
            None => return None, // ran off the end without a feature PlayPL
        };
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
                    let id = dst as u16;
                    if is_feature(id) {
                        return Some(id);
                    }
                }
                _ => {}
            },
            // CMP — skip the next command when the compare is false.
            1 => {
                let truth = match c.cmp_opt {
                    0x01 => (dst & !src) == 0,
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
            2 => {
                if c.sub_grp == 0 {
                    let r: Option<u32> = match c.set_opt {
                        0x01 => Some(src), // MOVE
                        0x02 => {
                            vm.wr(c.dst, src); // SWAP
                            vm.wr(c.src, dst);
                            None
                        }
                        0x03 => Some(dst.wrapping_add(src)),
                        0x04 => Some(dst.saturating_sub(src)),
                        0x05 => Some(dst.wrapping_mul(src)),
                        0x06 => Some(if src > 0 { dst / src } else { 0xffff_ffff }),
                        0x07 => Some(if src > 0 { dst % src } else { 0xffff_ffff }),
                        0x08 => Some(dst), // RND — deterministic stand-in
                        0x09 => Some(dst & src),
                        0x0a => Some(dst | src),
                        0x0b => Some(dst ^ src),
                        0x0c => Some(dst | (1u32 << (src & 31))),
                        0x0d => Some(dst & !(1u32 << (src & 31))),
                        0x0e => Some(dst.wrapping_shl(src & 31)),
                        0x0f => Some(dst.wrapping_shr(src & 31)),
                        _ => None,
                    };
                    if let Some(r) = r
                        && !c.imm_op1
                    {
                        vm.wr(c.dst, r);
                    }
                }
            }
            _ => {}
        }
        pc = npc;
    }
}

#[cfg(test)]
mod tests {
    use super::super::index::{Index, PlaybackObj};
    use super::super::mobj::{self, tests::build, tests::cmd};
    use super::*;

    fn play_pl(id: u16) -> [u8; 12] {
        // op_cnt=1, grp=BRANCH(0), sub_grp=PLAY(2), branch_opt=PLAY_PL(0), imm dst.
        cmd((1 << 5) | 2, 0x80, 0, 0, id as u32, 0)
    }
    fn jump_object(obj: u16) -> [u8; 12] {
        // grp=BRANCH(0), sub_grp=JUMP(1), branch_opt=JUMP_OBJECT(0), imm dst.
        cmd((1 << 5) | 1, 0x80, 0, 0, obj as u32, 0)
    }
    fn jump_title(title: u16) -> [u8; 12] {
        // sub_grp=JUMP(1), branch_opt=JUMP_TITLE(1), imm dst.
        cmd((1 << 5) | 1, 0x81, 0, 0, title as u32, 0)
    }
    fn set_move_gpr(reg: u16, imm: u16) -> [u8; 12] {
        // grp=SET(2), sub_grp=SET(0), set_opt=MOVE(1); dst=reg (GPR), imm src.
        // op_cnt=2, imm_op2=1 (src immediate), imm_op1=0 (dst is a register).
        cmd((2 << 5) | (2 << 3), 0x40, 0, 0x01, reg as u32, imm as u32)
    }
    fn play_pl_reg(reg: u16) -> [u8; 12] {
        // PLAY_PL with dst = GPR[reg] (op_cnt=1, imm_op1=0).
        cmd((1 << 5) | 2, 0x00, 0, 0, reg as u32, 0)
    }

    fn idx(first: PlaybackObj, titles: Vec<PlaybackObj>) -> Index {
        Index {
            first_play: first,
            top_menu: PlaybackObj::BdJ,
            titles,
        }
    }

    #[test]
    fn resolves_immediate_playpl() {
        // First-Play HDMV obj0 → JumpObject 1 → PlayPL 11.
        let d = build(&[&[jump_object(1)], &[play_pl(11)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|id| id == 11), Some(11));
    }

    #[test]
    fn skips_non_feature_playpl_then_returns_feature() {
        // Autoplay chain: logo PlayPL 99 (not a candidate) → feature PlayPL 1.
        let d = build(&[&[play_pl(99), play_pl(1)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        // Only 1 is a feature candidate; 99 is a logo.
        assert_eq!(resolve(&index, &mobjs, &|id| id == 1), Some(1));
    }

    #[test]
    fn abstains_when_first_play_jumps_to_bdj_title() {
        // SM3 shape: First-Play HDMV computes a title number in a GPR and
        // JumpTitles to it; the target title is BD-J → abstain (None).
        let d = build(&[&[set_move_gpr(0xFEB & 0xfff, 2), {
            // JumpTitle with dst = GPR[0xFEB].
            cmd((1 << 5) | 1, 0x01, 0, 0, 0x0000_0FEB, 0)
        }]]);
        let mobjs = mobj::parse(&d).unwrap();
        // title 2 → titles[1] = BD-J.
        let index = idx(
            PlaybackObj::Hdmv { id_ref: 0 },
            vec![PlaybackObj::Hdmv { id_ref: 5 }, PlaybackObj::BdJ],
        );
        assert_eq!(resolve(&index, &mobjs, &|_| true), None);
    }

    #[test]
    fn abstains_when_first_play_is_bdj() {
        let d = build(&[&[play_pl(1)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::BdJ, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|_| true), None);
    }

    #[test]
    fn register_fold_and_cmp_skip_reach_playpl() {
        // SET GPR[3]=7; CMP GPR[3]==7 (true → do NOT skip); PlayPL GPR[3].
        // grp=CMP(1), op_cnt=2, imm_op2=1 (src=7 immediate), cmp_opt=EQ(2); dst=GPR3.
        let cmp_eq_reg = cmd((2 << 5) | (1 << 3), 0x40, 0x02, 0, 3, 0x0000_0007);
        let d = build(&[&[set_move_gpr(3, 7), cmp_eq_reg, play_pl_reg(3), play_pl(999)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        // If the compare wrongly skipped, we'd hit PlayPL 999 instead of 7.
        assert_eq!(resolve(&index, &mobjs, &|id| id == 7 || id == 999), Some(7));
    }

    #[test]
    fn jumptitle_namespace_differs_from_jumpobject() {
        // JumpTitle 1 → titles[0] = HDMV obj 1 (which PlayPLs 42). Proves title
        // numbers resolve through the index, not as object indices.
        let d = build(&[&[jump_title(1)], &[play_pl(42)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(
            PlaybackObj::Hdmv { id_ref: 0 },
            vec![PlaybackObj::Hdmv { id_ref: 1 }],
        );
        assert_eq!(resolve(&index, &mobjs, &|id| id == 42), Some(42));
    }
}

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
                    0x01 => (src & !dst) == 0, // BC: every src bit set in dst
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
    fn call_object(obj: u16) -> [u8; 12] {
        // sub_grp=JUMP(1), branch_opt=CALL_OBJECT(0x02), imm dst. For play-path
        // resolution a Call follows the same target as a Jump (the return
        // address is not modelled).
        cmd((1 << 5) | 1, 0x82, 0, 0, obj as u32, 0)
    }
    fn call_title(title: u16) -> [u8; 12] {
        // sub_grp=JUMP(1), branch_opt=CALL_TITLE(0x03), imm dst.
        cmd((1 << 5) | 1, 0x83, 0, 0, title as u32, 0)
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
    fn call_object_follows_the_play_path_like_jump_object() {
        // First-Play HDMV obj0 → CallObject 1 → PlayPL 11. CallObject follows
        // the same target as JumpObject for feature resolution.
        let d = build(&[&[call_object(1)], &[play_pl(11)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|id| id == 11), Some(11));
    }

    #[test]
    fn call_title_resolves_through_the_index_like_jump_title() {
        // CallTitle 1 → titles[0] = HDMV obj 1 (which PlayPLs 42).
        let d = build(&[&[call_title(1)], &[play_pl(42)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(
            PlaybackObj::Hdmv { id_ref: 0 },
            vec![PlaybackObj::Hdmv { id_ref: 1 }],
        );
        assert_eq!(resolve(&index, &mobjs, &|id| id == 42), Some(42));
    }

    #[test]
    fn abstains_when_first_play_jumps_to_bdj_title() {
        // Computed-title-number shape: First-Play computes a title number in a
        // GPR and JumpTitles to it; the target title is BD-J → abstain (None).
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
    fn jumptitle_first_play_is_0xffff_not_n_plus_1() {
        // BD-ROM title numbering: 0 = Top Menu, 0xFFFF = First Play,
        // 1..=N = titles. `N+1` is an INVALID title ref.
        let index = idx(
            PlaybackObj::Hdmv { id_ref: 7 },
            vec![PlaybackObj::Hdmv { id_ref: 1 }],
        );
        let vm = Vm::new(&[], &index);
        assert_eq!(vm.title_obj(0), Some(index.top_menu), "title 0 = Top Menu");
        assert_eq!(
            vm.title_obj(1),
            Some(PlaybackObj::Hdmv { id_ref: 1 }),
            "title 1 = titles[0]"
        );
        assert_eq!(
            vm.title_obj(0xFFFF),
            Some(PlaybackObj::Hdmv { id_ref: 7 }),
            "0xFFFF = First Play (abstained wrongly before the fix)"
        );
        assert_eq!(
            vm.title_obj(2),
            None,
            "N+1 is an invalid title ref, not First Play (resolved wrongly before the fix)"
        );
    }

    #[test]
    fn set_to_psr_is_refused() {
        // SET MOVE to PSR6 must be a no-op (PSR stores are refused by spec).
        // Program: MOVE PSR6<-5; CMP PSR6==5; PlayPL 100 else 200. Refused write
        // keeps PSR6 at 0 -> PlayPL 200; an illegal write would wrongly pick 100.
        let set_psr6 = cmd((2 << 5) | (2 << 3), 0x40, 0, 0x01, 0x8000_0006, 5);
        let cmp_psr6_eq_5 = cmd((2 << 5) | (1 << 3), 0x40, 0x02, 0, 0x8000_0006, 5);
        let d = build(&[&[set_psr6, cmp_psr6_eq_5, play_pl(100), play_pl(200)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(
            resolve(&index, &mobjs, &|id| id == 100 || id == 200),
            Some(200),
            "PSR store must be refused, so the compare is false and 200 is reached"
        );
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

    // ADD/MUL saturate at 0xFFFFFFFF (libbluray hdmv_vm.c ADD_u32/MUL_u32,
    // reverse-engineered player behaviour), like SUB and DIV/MOD by 0 already do.
    #[test]
    fn add_and_mul_saturate() {
        let set = |opt: u8, reg: u32, imm: u32| cmd((2 << 5) | (2 << 3), 0x40, 0, opt, reg, imm);
        let cmp_eq_0 = |reg: u32| cmd((2 << 5) | (1 << 3), 0x40, 0x02, 0, reg, 0);
        for ops in [
            [set(0x01, 0, 0xffff_ffff), set(0x03, 0, 1)],
            [set(0x01, 0, 0x1_0000), set(0x05, 0, 0x1_0000)],
        ] {
            // A wrap to 0 makes the compare true and plays 1; saturation plays 800.
            let d = build(&[&[ops[0], ops[1], cmp_eq_0(0), play_pl(1), play_pl(800)]]);
            let mobjs = mobj::parse(&d).unwrap();
            let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
            assert_eq!(
                resolve(&index, &mobjs, &|id| id == 1 || id == 800),
                Some(800)
            );
        }
    }

    #[test]
    fn swap_exchanges_two_registers() {
        // MOVE g0=11; MOVE g1=22; SWAP g0<->g1; PlayPL GPR[0]. SWAP: grp=SET(2),
        // sub_grp=SET(0), set_opt=SWAP(2), both operands registers (imm_op*=0).
        let swap01 = cmd((2 << 5) | (2 << 3), 0x00, 0, 0x02, 0, 1);
        let d = build(&[&[
            set_move_gpr(0, 11),
            set_move_gpr(1, 22),
            swap01,
            play_pl_reg(0),
        ]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        let mut vm = Vm::new(&mobjs, &index);
        // After the swap GPR0 holds the old GPR1 (22) and PlayPL plays it.
        assert_eq!(run(&mut vm, 0, &|id| id == 22), Some(22));
        // Full exchange: GPR1 now holds the old GPR0 (11).
        assert_eq!(vm.gpr[1], 11);
    }

    #[test]
    fn non_convergent_self_goto_bails_via_max_steps() {
        // An unconditional GOTO to its own line never reaches a PlayPL: the
        // MAX_STEPS guard must bail (None) rather than loop forever. If the
        // guard were removed this would hang instead of returning.
        let self_goto = cmd(1 << 5, 0x81, 0, 0, 0, 0);
        let d = build(&[&[self_goto]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|_| true), None);
    }

    #[test]
    fn swap_immediate_operand_is_not_written_back() {
        // SWAP dst=GPR0 with src=IMMEDIATE 5 (imm_op1=0, imm_op2=1) must refuse
        // writing to the aliased register: GPR[5] stays 77, only GPR0 gets 5.
        // Dropping the `imm_op1`/`imm_op2` guard makes this assertion fail.
        let swap_imm = cmd((2 << 5) | (2 << 3), 0x40, 0, 0x02, 0, 5);
        let d = build(&[&[set_move_gpr(5, 77), swap_imm]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        let mut vm = Vm::new(&mobjs, &index);
        // No PlayPL → program runs off the end (None); inspect registers after.
        assert_eq!(run(&mut vm, 0, &|_| false), None);
        assert_eq!(
            vm.gpr[5], 77,
            "immediate operand must not be a write target"
        );
        assert_eq!(vm.gpr[0], 5, "register operand receives the immediate");
    }

    // libbluray hdmv_vm.c: BITSET/BITCLR with a bit number >= 32 are no-ops and
    // SHL/SHR by >= 32 give 0 (no `& 31` masking).
    #[test]
    fn bit_ops_and_shifts_past_31_follow_libbluray() {
        let set = |opt: u8, reg: u32, imm: u32| cmd((2 << 5) | (2 << 3), 0x40, 0, opt, reg, imm);
        let d = build(&[&[
            set(0x0c, 0, 32),
            set(0x01, 1, 1),
            set(0x0e, 1, 32),
            set(0x01, 2, 0x8000_0000),
            set(0x0f, 2, 40),
            set(0x01, 3, 0xffff_ffff),
            set(0x0d, 3, 33),
        ]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        let mut vm = Vm::new(&mobjs, &index);
        assert_eq!(run(&mut vm, 0, &|_| false), None);
        assert_eq!(vm.gpr[..4], [0, 0, 0, 0xffff_ffff]);
    }

    // RND is non-deterministic: a compare on its result abstains.
    #[test]
    fn compare_on_rnd_result_abstains() {
        let set = |opt: u8, reg: u32, imm: u32| cmd((2 << 5) | (2 << 3), 0x40, 0, opt, reg, imm);
        let cmp_eq = cmd((2 << 5) | (1 << 3), 0x40, 0x02, 0, 0, 5);
        let d = build(&[&[
            set(0x01, 0, 5),
            set(0x08, 0, 10),
            cmp_eq,
            play_pl(1),
            play_pl(800),
        ]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|id| id == 1 || id == 800), None);
    }

    fn run_gpr0(prog: &[[u8; 12]], gprs: usize) -> Vec<u32> {
        let d = build(&[prog]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        let mut vm = Vm::new(&mobjs, &index);
        assert_eq!(run(&mut vm, 0, &|_| false), None);
        vm.gpr[..gprs].to_vec()
    }

    fn set_imm(opt: u8, reg: u32, imm: u32) -> [u8; 12] {
        cmd((2 << 5) | (2 << 3), 0x40, 0, opt, reg, imm)
    }

    // CMP dst(GPR0 = a) against imm b: true falls through to PlayPL 1, false skips to 2.
    fn cmp_picks_true(opt: u8, a: u32, b: u32) -> bool {
        let cmp = cmd((2 << 5) | (1 << 3), 0x40, opt, 0, 0, b);
        let d = build(&[&[set_imm(0x01, 0, a), cmp, play_pl(1), play_pl(2)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        resolve(&index, &mobjs, &|id| id == 1 || id == 2) == Some(1)
    }

    #[test]
    fn cmp_ops_ge_gt_le_lt_ne() {
        // (cmp_opt, a, b, expected)
        let cases = [
            (0x04, 5, 5, true),
            (0x04, 4, 5, false),
            (0x04, 6, 5, true),
            (0x05, 5, 5, false),
            (0x05, 6, 5, true),
            (0x05, 4, 5, false),
            (0x06, 5, 5, true),
            (0x06, 6, 5, false),
            (0x06, 4, 5, true),
            (0x07, 5, 5, false),
            (0x07, 4, 5, true),
            (0x07, 6, 5, false),
            (0x03, 5, 5, false),
            (0x03, 5, 6, true),
        ];
        for (opt, a, b, want) in cases {
            assert_eq!(cmp_picks_true(opt, a, b), want, "opt {opt:#x} {a} vs {b}");
        }
    }

    // libbluray INSN_BC: true iff every bit of src is set in dst (src & ~dst == 0).
    #[test]
    fn cmp_bc_tests_src_bits_within_dst() {
        assert!(cmp_picks_true(0x01, 0b1110, 0b0110));
        assert!(!cmp_picks_true(0x01, 0b0100, 0b0110));
        assert!(!cmp_picks_true(0x01, 0b0110, 0b1110));
    }

    #[test]
    fn set_arithmetic_and_logic_ops() {
        // (set_opt, a, b, expected)
        let cases = [
            (0x03, 2, 3, 5),
            (0x04, 10, 3, 7),
            (0x04, 3, 10, 0),
            (0x05, 2, 3, 6),
            (0x06, 10, 3, 3),
            (0x06, 10, 0, 0xffff_ffff),
            (0x07, 10, 3, 1),
            (0x07, 10, 0, 0xffff_ffff),
            (0x09, 0b1100, 0b1010, 0b1000),
            (0x0a, 0b1100, 0b1010, 0b1110),
            (0x0b, 0b1100, 0b1010, 0b0110),
            (0x0c, 0, 3, 8),
            (0x0d, 0xf, 1, 0xd),
            (0x0e, 1, 4, 16),
            (0x0f, 0x100, 4, 0x10),
        ];
        for (opt, a, b, want) in cases {
            let g = run_gpr0(&[set_imm(0x01, 0, a), set_imm(opt, 0, b)], 1);
            assert_eq!(g[0], want, "set_opt {opt:#x} on {a}, {b}");
        }
    }

    #[test]
    fn branch_or_compare_on_rnd_value_abstains() {
        // GPR0 = RND; then PlayPL GPR0 (branch grp, dst tainted).
        let rnd = set_imm(0x08, 0, 10);
        let d = build(&[&[rnd, play_pl_reg(0), play_pl(1)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(resolve(&index, &mobjs, &|_| true), None);
        // Compare with a clean dst (GPR1) and a tainted register src (GPR0).
        let cmp = cmd((2 << 5) | (1 << 3), 0x00, 0x02, 0, 1, 0);
        let d = build(&[&[rnd, cmp, play_pl(1), play_pl(2)]]);
        let mobjs = mobj::parse(&d).unwrap();
        assert_eq!(resolve(&index, &mobjs, &|_| true), None);
    }

    #[test]
    fn playpl_operand_over_16_bits_is_not_a_playlist_id() {
        // 0x1_0001 must not alias playlist 1; the VM falls through to 800.
        let wide = cmd((1 << 5) | 2, 0x80, 0, 0, 0x1_0001, 0);
        let d = build(&[&[wide, play_pl(800)]]);
        let mobjs = mobj::parse(&d).unwrap();
        let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
        assert_eq!(
            resolve(&index, &mobjs, &|id| id == 1 || id == 800),
            Some(800)
        );
    }

    #[test]
    fn power_on_psr_defaults_are_visible_to_compares() {
        // (psr, expected power-on value)
        let cases: [(u32, u32); 12] = [
            (0, 1),
            (1, 0xff),
            (3, 1),
            (4, 0xffff),
            (5, 0xffff),
            (6, 0),
            (7, 0),
            (8, 0),
            (20, 2),
            (31, 0x0003_0200),
            (48, 0xffff_ffff),
            (62, 0),
        ];
        for (psr, want) in cases {
            // CMP PSR == want (imm): true plays 1, false plays 2.
            let cmp = cmd((2 << 5) | (1 << 3), 0x40, 0x02, 0, 0x8000_0000 | psr, want);
            let d = build(&[&[cmp, play_pl(1), play_pl(2)]]);
            let mobjs = mobj::parse(&d).unwrap();
            let index = idx(PlaybackObj::Hdmv { id_ref: 0 }, vec![]);
            let got = resolve(&index, &mobjs, &|id| id == 1 || id == 2);
            assert_eq!(got, Some(1), "PSR{psr} != {want:#x}");
        }
    }
}

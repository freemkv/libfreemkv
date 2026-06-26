//! DVD-Video VM command decoder.
//!
//! An 8-byte navigation command as found in PGC command tables (pre/post/cell)
//! and PCI button info. Decoded per the DVD-Video VM instruction set and
//! verified against libdvdnav's command decoder.
//!
//! Bit model: the 8 bytes are a big-endian 64-bit word. `byte0` bits 7-5 are the
//! command **type**; for type 1, `byte0` bit 4 selects Link (0) vs Jump (1), and
//! `byte1` bits 3-0 are the sub-command. Compare predicates live in `byte1`
//! bits 6-4 with the operands in bytes 2-5.
//!
//! This module is pure decode + a register model — no I/O, no English (numeric
//! semantics only), matching libfreemkv conventions. The navigation *executor*
//! and IFO/PCI parsing build on top of this.

/// A decoded navigation instruction. Only the variants freemkv's start-point
/// resolver needs are modelled explicitly; everything else is [`Instr::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Instr {
    Nop,
    /// Stop executing the current command list (resume cell playback).
    Break,
    /// Goto command line within the same list (1-based).
    Goto {
        line: u8,
    },
    /// Leave the current domain.
    Exit,
    /// Jump to a VMG title (1-based TT_SRPT index).
    JumpTt {
        ttn: u8,
    },
    /// Jump to a title within the current VTS (1-based VTS title index).
    JumpVtsTt {
        ttn: u8,
    },
    /// Jump to a part-of-title (chapter) within a VTS title.
    JumpVtsPtt {
        ttn: u8,
        pttn: u16,
    },
    /// Jump to the First-Play PGC.
    JumpSsFp,
    /// Jump to a Video-Manager menu (`menu` = menu id).
    JumpSsVmgm {
        menu: u8,
    },
    /// Jump to a Video-Title-Set menu.
    JumpSsVtsm {
        vts: u8,
        ttn: u8,
        menu: u8,
    },
    /// Jump to a specific VMGM menu PGC.
    JumpSsVmgmPgc {
        pgcn: u16,
    },
    /// Call a sub-domain (raw retained; resume handled by the executor).
    CallSs {
        sub: u8,
    },
    /// Link to a PGC number within the current domain.
    LinkPgcn {
        pgcn: u16,
    },
    /// Link to a part-of-title within the current PGC's title.
    LinkPttn {
        pttn: u16,
    },
    /// Link to a program number within the current PGC (1-based).
    LinkPgn {
        pgn: u8,
    },
    /// Link to a cell number within the current PGC (1-based).
    LinkCn {
        cn: u8,
    },
    /// A link "subset" op (LinkTopCell/NextPG/RSM/…); `sub` is the raw code.
    LinkSub {
        sub: u8,
    },
    /// Set a GPRM. `op` is the set-op code (1=mov, 3=add, …); value is immediate
    /// (`imm`) when `immediate`, else the contents of register `src`.
    SetGprm {
        reg: u8,
        op: u8,
        immediate: bool,
        imm: u16,
        src: u8,
    },
    /// Set a system parameter / unmodelled set — executor may ignore.
    SetSystem,
    /// Anything not individually modelled (kept as raw bytes).
    Other([u8; 8]),
}

/// A compare predicate carried by a command (`byte1` bits 6-4). `None` = always.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compare {
    /// Compare op: 1=&,2===,3=!=,4=>=,5=>,6=<=,7=<.
    pub op: u8,
    /// Left register index (GPRM 0-15, SPRM 128+).
    pub lhs_reg: u8,
    /// Right side: immediate when `immediate`, else register `rhs_reg`.
    pub immediate: bool,
    pub imm: u16,
    pub rhs_reg: u8,
}

/// A fully decoded command: its predicate (if any) and the instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Command {
    pub compare: Option<Compare>,
    pub instr: Instr,
}

// Command types — `byte0` bits 7-5.
const TYPE_SPECIAL: u8 = 0;
const TYPE_LINK_JUMP: u8 = 1;
const TYPE_SET_SYSTEM: u8 = 2;
const TYPE_SET_GPRM: u8 = 3;

// Special (type 0) sub-commands — `byte1` bits 3-0.
const SP_GOTO: u8 = 1;
const SP_BREAK: u8 = 2;

// Jump/Call (type 1, direct=1) sub-commands.
const JP_EXIT: u8 = 1;
const JP_JUMP_TT: u8 = 2;
const JP_JUMP_VTS_TT: u8 = 3;
const JP_JUMP_VTS_PTT: u8 = 5;
const JP_JUMP_SS: u8 = 6;
const JP_CALL_SS: u8 = 8;

// Link (type 1, direct=0) sub-commands. NOTE: sub-op 0 is NOP/no-link and 1 is
// the LinkSub form (libdvdnav `decoder.c` `eval_link_instruction`).
const LK_SUB: u8 = 1;
const LK_PGCN: u8 = 4;
const LK_PTTN: u8 = 5;
const LK_PGN: u8 = 6;
const LK_CN: u8 = 7;

// JumpSS sub-domain selector — `byte5` bits 7-6.
const SS_FP: u8 = 0;
const SS_VMGM_MENU: u8 = 1;
const SS_VTSM: u8 = 2;

// Operand field widths (spec-defined bit counts).
const MASK_TTN: u8 = 0x7F; // 7-bit title number
const MASK_PGN: u8 = 0x7F; // 7-bit program number
const MASK_LINKOP: u8 = 0x1F; // 5-bit link sub-op
const MASK_REG: u8 = 0x0F; // 4-bit GPRM index
const MASK_MENU: u8 = 0x0F; // 4-bit menu id
const MASK_PTTN: u16 = 0x03FF; // 10-bit part-of-title
const MASK_PGCN: u16 = 0x7FFF; // 15-bit PGC number

#[inline]
fn be16(b: &[u8; 8], o: usize) -> u16 {
    ((b[o] as u16) << 8) | b[o + 1] as u16
}

// Compare-operand layouts ("if_version"s) per libdvdnav `decoder.c`. The op
// nibble is always `byte1` bits 6-4; the immediate flag is `byte1` bit 7. The
// operand *offsets* differ by command family.
//
// v1 (special + link): lhs reg = b[3]; rhs imm = bytes4-5 / rhs reg = b[4].
// v2 (jump + system-set): lhs reg = b[6]; rhs reg = b[7] (registers only).
// v3 (set-GPRM): lhs reg = b[2]; rhs imm = bytes6-7 / rhs reg = b[6].
fn if_v1(b: &[u8; 8]) -> Option<Compare> {
    let op = (b[1] >> 4) & 7;
    (op != 0).then(|| Compare {
        op,
        lhs_reg: b[3],
        immediate: b[1] >> 7 != 0,
        imm: be16(b, 4),
        rhs_reg: b[4],
    })
}
fn if_v2(b: &[u8; 8]) -> Option<Compare> {
    let op = (b[1] >> 4) & 7;
    (op != 0).then(|| Compare {
        op,
        lhs_reg: b[6],
        immediate: false,
        imm: 0,
        rhs_reg: b[7],
    })
}
fn if_v3(b: &[u8; 8]) -> Option<Compare> {
    let op = (b[1] >> 4) & 7;
    (op != 0).then(|| Compare {
        op,
        lhs_reg: b[2],
        immediate: b[1] >> 7 != 0,
        imm: be16(b, 6),
        rhs_reg: b[6],
    })
}

/// Decode an 8-byte VM command.
pub fn decode(b: &[u8; 8]) -> Command {
    let typ = b[0] >> 5;
    let direct = (b[0] >> 4) & 1;
    let setop = b[0] & 0x0F;
    let cmd = b[1] & 0x0F;

    // Compare predicate, with the operand layout for this command family
    // (libdvdnav `decoder.c` `vm_eval_command` type dispatch).
    let compare = match (typ, direct) {
        (TYPE_SPECIAL, _) => if_v1(b),
        (TYPE_LINK_JUMP, 1) => if_v2(b), // jump
        (TYPE_LINK_JUMP, 0) => if_v1(b), // link
        (TYPE_SET_SYSTEM, _) => if_v2(b),
        (TYPE_SET_GPRM, _) => if_v3(b),
        _ => None, // 4/5/6 compound — not needed by the resolver
    };

    // JumpSS sub-domain selector lives in byte5 bits 7-6.
    let ss_sel = b[5] >> 6;

    let instr = match typ {
        TYPE_LINK_JUMP if direct == 1 => match cmd {
            JP_EXIT => Instr::Exit,
            JP_JUMP_TT => Instr::JumpTt {
                ttn: b[5] & MASK_TTN,
            },
            JP_JUMP_VTS_TT => Instr::JumpVtsTt {
                ttn: b[5] & MASK_TTN,
            },
            JP_JUMP_VTS_PTT => Instr::JumpVtsPtt {
                ttn: b[5] & MASK_TTN,
                pttn: be16(b, 2) & MASK_PTTN,
            },
            JP_JUMP_SS => match ss_sel {
                SS_FP => Instr::JumpSsFp,
                SS_VMGM_MENU => Instr::JumpSsVmgm {
                    menu: b[5] & MASK_MENU,
                },
                SS_VTSM => Instr::JumpSsVtsm {
                    vts: b[4],
                    ttn: b[3],
                    menu: b[5] & MASK_MENU,
                },
                _ => Instr::JumpSsVmgmPgc {
                    pgcn: be16(b, 2) & MASK_PGCN,
                },
            },
            JP_CALL_SS => Instr::CallSs { sub: ss_sel },
            _ => Instr::Nop,
        },
        TYPE_LINK_JUMP => match cmd {
            // direct == 0 (link). sub-op 0 = NOP/no-link.
            LK_SUB => Instr::LinkSub {
                sub: b[7] & MASK_LINKOP,
            },
            LK_PGCN => Instr::LinkPgcn {
                pgcn: be16(b, 6) & MASK_PGCN,
            },
            LK_PTTN => Instr::LinkPttn {
                pttn: be16(b, 6) & MASK_PTTN,
            },
            LK_PGN => Instr::LinkPgn {
                pgn: b[7] & MASK_PGN,
            },
            LK_CN => Instr::LinkCn { cn: b[7] },
            _ => Instr::Nop,
        },
        TYPE_SPECIAL => match cmd {
            SP_GOTO => Instr::Goto { line: b[7] },
            SP_BREAK => Instr::Break,
            _ => Instr::Nop,
        },
        TYPE_SET_GPRM => Instr::SetGprm {
            reg: b[3] & MASK_REG,
            op: setop,
            immediate: direct != 0,
            imm: be16(b, 4),
            src: b[5],
        },
        TYPE_SET_SYSTEM => Instr::SetSystem,
        _ => Instr::Other(*b),
    };

    Command { compare, instr }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> [u8; 8] {
        let v: Vec<u8> = (0..8)
            .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    // KATs taken from the real SOTL / Greenland discs (decoded in the PoC).
    #[test]
    fn greenland_first_play_is_jumptt_1() {
        let c = decode(&h("3002000000010000"));
        assert_eq!(c.instr, Instr::JumpTt { ttn: 1 });
        assert!(c.compare.is_none());
    }

    #[test]
    fn sotl_first_play_is_jumpss_vtsm_root() {
        // 30 06 ... byte5=0x83 -> sub 2 (VTSM), vts=byte4=1, menu=byte5&0xF=3 (root)
        let c = decode(&h("3006000101830000"));
        assert_eq!(
            c.instr,
            Instr::JumpSsVtsm {
                vts: 1,
                ttn: 1,
                menu: 3
            }
        );
    }

    #[test]
    fn sotl_title_dispatch_is_conditional_linkpgn_2() {
        // 20 a6 ... CmpLink: if GPRM0 == 2 -> LinkPGN 2  (cell 2 = the 5:02 start)
        let c = decode(&h("20a6000000020002"));
        assert_eq!(c.instr, Instr::LinkPgn { pgn: 2 });
        let cmp = c.compare.expect("conditional");
        assert_eq!(cmp.op, 2); // ==
        assert_eq!(cmp.lhs_reg, 0); // GPRM0
        assert!(cmp.immediate);
        assert_eq!(cmp.imm, 2);
    }

    #[test]
    fn sotl_root_button_is_linkpgcn_37() {
        assert_eq!(
            decode(&h("2004000000000025")).instr,
            Instr::LinkPgcn { pgcn: 37 }
        );
    }

    #[test]
    fn greenland_scene_button_is_linkpgn() {
        assert_eq!(
            decode(&h("2006000000001401")).instr,
            Instr::LinkPgn { pgn: 1 }
        );
    }

    #[test]
    fn jumpvts_ptt_decodes_ttn_and_pttn() {
        // synthetic: 30 05 | ptt(bytes2-3)=0x0002 | ttn(byte5)=1
        let c = decode(&h("3005000200010000"));
        assert_eq!(c.instr, Instr::JumpVtsPtt { ttn: 1, pttn: 2 });
    }

    #[test]
    fn setgprm_immediate_mov() {
        // SOTL First-Play pre[0]: 71 00 | reg=byte3=6 | imm(bytes4-5)=0x03e8 -> g6 = 1000
        match decode(&h("7100000603e80000")).instr {
            Instr::SetGprm {
                reg,
                op,
                immediate,
                imm,
                ..
            } => {
                assert_eq!(reg, 6);
                assert_eq!(op, 1); // mov
                assert!(immediate);
                assert_eq!(imm, 1000);
            }
            other => panic!("expected SetGprm, got {other:?}"),
        }
    }

    // Regression for the libdvdnav cross-check: link sub-op 0 = NOP, 1 = LinkSub.
    #[test]
    fn link_subop_zero_is_nop_one_is_linksub() {
        assert_eq!(decode(&h("2000000000000000")).instr, Instr::Nop);
        assert_eq!(
            decode(&h("2001000000000010")).instr,
            Instr::LinkSub { sub: 0x10 }
        );
    }

    // if_version_1 register compare: rhs register is byte4 (not byte5).
    #[test]
    fn link_register_compare_rhs_is_byte4() {
        // 20 26: link, cmp=EQ(2), dircmp=0(register) ; cmd=6 LinkPGN
        let c = decode(&h("2026000304000002"));
        assert_eq!(c.instr, Instr::LinkPgn { pgn: 2 });
        let cmp = c.compare.expect("conditional");
        assert!(!cmp.immediate);
        assert_eq!(cmp.lhs_reg, 3);
        assert_eq!(cmp.rhs_reg, 4);
    }

    // if_version_2 jump compare: both operands are registers in byte6 / byte7.
    #[test]
    fn jump_compare_uses_bytes6_and_7() {
        // 30 22: jump, cmp=EQ(2) ; cmd=2 JumpTT ttn=byte5=5
        let c = decode(&h("3022000000050607"));
        assert_eq!(c.instr, Instr::JumpTt { ttn: 5 });
        let cmp = c.compare.expect("conditional");
        assert!(!cmp.immediate);
        assert_eq!(cmp.lhs_reg, 6);
        assert_eq!(cmp.rhs_reg, 7);
    }
}

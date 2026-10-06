//! DVD-Video VM command decoder.
//!
//! An 8-byte navigation command as found in PGC command tables (pre/post/cell)
//! and PCI button info. Decoded per the DVD-Video VM instruction set.
//!
//! Bit model: the 8 bytes are a big-endian 64-bit word. `byte0` bits 7-5 are the
//! command **type**; for type 1, `byte0` bit 4 selects Link (0) vs Jump (1), and
//! `byte1` bits 3-0 are the sub-command. Compare predicates live in `byte1`
//! bits 6-4 with the operands in bytes 2-5.

// Decoding verified against real discs. Pure decode + register model — no I/O,
// no English (numeric semantics only). The navigation executor and IFO/PCI
// parsing build on top of this.

/// A decoded navigation instruction. Only the variants freemkv's start-point
/// resolver needs are modelled explicitly; everything else is [`Instr::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Instr {
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
    /// SetSystem op 3 (SetGPRMMD): store into GPRM `reg` and set its mode
    /// (`counter` = counter mode). Value as for [`Instr::SetGprm`].
    SetGprmMd {
        reg: u8,
        counter: bool,
        immediate: bool,
        imm: u16,
        src: u8,
    },
    /// Any other SetSystem op: writes SPRMs only — executor may ignore.
    SetSystem,
    /// Anything not individually modelled (kept as raw bytes).
    Other([u8; 8]),
}

/// A compare predicate carried by a command (`byte1` bits 6-4). `None` = always.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Compare {
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
pub(crate) struct Command {
    pub compare: Option<Compare>,
    pub instr: Instr,
    /// Type 2/3 only: a trailing link sub-instruction transfers control
    /// after the set (libdvdnav eval_link_instruction; reverse-engineered).
    pub link: bool,
}

// Command types — `byte0` bits 7-5.
const TYPE_SPECIAL: u8 = 0;
const TYPE_LINK_JUMP: u8 = 1;
const TYPE_SET_SYSTEM: u8 = 2;
const TYPE_SET_GPRM: u8 = 3;

// Special (type 0) sub-commands — `byte1` bits 3-0.
const SP_GOTO: u8 = 1;
const SP_BREAK: u8 = 2;
// SetTmpPML: sets SPRM13 (parental level), then gotos like SP_GOTO when cond holds.
const SP_SET_TMP_PML: u8 = 3;

// Jump/Call (type 1, direct=1) sub-commands.
const JP_EXIT: u8 = 1;
const JP_JUMP_TT: u8 = 2;
const JP_JUMP_VTS_TT: u8 = 3;
const JP_JUMP_VTS_PTT: u8 = 5;
const JP_JUMP_SS: u8 = 6;
const JP_CALL_SS: u8 = 8;

// Link (type 1, direct=0) sub-commands. NOTE: sub-op 0 is NOP/no-link and 1 is
// the LinkSub form (the DVD-Video VM link instruction).
const LK_SUB: u8 = 1;
const LK_PGCN: u8 = 4;
const LK_PTTN: u8 = 5;
const LK_PGN: u8 = 6;
const LK_CN: u8 = 7;

// SetSystem (type 2) op code — `byte0` bits 3-0.
const SS_SET_GPRMMD: u8 = 3;
// Link sub-instruction codes (byte7 bits 4-0); above 0x10 unknown (no link). Even
// LinkNoLink (0) ends the command list in libdvdnav (eval_link_subins returns cond).
const LINKSUB_MAX: u8 = 0x10;

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

// Compare-operand layouts ("if_version"s): op nibble = byte1 bits 6-4, imm
// flag = byte1 bit 7; offsets differ by family: v1(special/link) lhs=b[3],
// rhs=b[4:6]/b[5]; v2(jump/sys-set) lhs=b[6],rhs=b[7]; v3(set-GPRM) lhs=b[2],rhs=b[6:8]/b[7].
fn if_v1(b: &[u8; 8]) -> Option<Compare> {
    let op = (b[1] >> 4) & 7;
    (op != 0).then(|| Compare {
        op,
        lhs_reg: b[3],
        immediate: b[1] >> 7 != 0,
        imm: be16(b, 4),
        rhs_reg: b[5],
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
        rhs_reg: b[7],
    })
}

/// Decode an 8-byte VM command.
pub(crate) fn decode(b: &[u8; 8]) -> Command {
    let typ = b[0] >> 5;
    let direct = (b[0] >> 4) & 1;
    let setop = b[0] & 0x0F;
    let cmd = b[1] & 0x0F;

    // Compare predicate, with the operand layout for this command family
    // (the DVD-Video VM command type dispatch).
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
            // No parental model: SPRM13 is not tracked, only the goto is followed.
            SP_GOTO | SP_SET_TMP_PML => Instr::Goto { line: b[7] },
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
        TYPE_SET_SYSTEM if setop == SS_SET_GPRMMD => Instr::SetGprmMd {
            reg: b[5] & MASK_REG,
            counter: b[5] >> 7 != 0,
            immediate: direct != 0,
            imm: be16(b, 2),
            src: b[3],
        },
        TYPE_SET_SYSTEM => Instr::SetSystem,
        _ => Instr::Other(*b),
    };

    // Set commands (types 2/3) carry a link in byte1 bits 3-0 (same codes as type 1).
    let link = matches!(typ, TYPE_SET_SYSTEM | TYPE_SET_GPRM)
        && match cmd {
            LK_SUB => (b[7] & MASK_LINKOP) <= LINKSUB_MAX,
            LK_PGCN..=LK_CN => true,
            _ => false,
        };

    Command {
        compare,
        instr,
        link,
    }
}

#[cfg(test)]
#[path = "vmcmd_tests.rs"]
mod tests;

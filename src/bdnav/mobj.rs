//! Parse `/BDMV/MovieObject.bdmv` — the HDMV navigation programs, per their
//! documented binary layout. Read as a documented format, never executed here:
//! bounds-checked, never panics.
//!
//! Layout: `"MOBJ"` + version(4) + reserved… ; `MovieObjects()` at byte 40 is
//! `length`(u32) + reserved(u32) + `num_objects`(u16 @48), then objects from
//! byte 50. Each object is `flags`(1) + reserved(1) + `num_cmds`(u16) followed
//! by `num_cmds` 12-byte navigation commands.

use super::be_u16;

// One decoded 12-byte navigation command.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Cmd {
    pub op_cnt: u8,
    pub grp: u8,
    pub sub_grp: u8,
    pub imm_op1: bool,
    pub imm_op2: bool,
    pub branch_opt: u8,
    pub cmp_opt: u8,
    pub set_opt: u8,
    pub dst: u32,
    pub src: u32,
}

/// One HDMV navigation program.
#[derive(Debug, Clone)]
pub(crate) struct MovieObject {
    pub cmds: Vec<Cmd>,
}

const CMD_LEN: usize = 12;
/// Sanity cap on object count (real discs are far under this; some densely-
/// branched dispatchers run several thousand commands in one object). There is
/// deliberately NO command-count cap: `num_cmds` is a u16 (≤ 65535) and each
/// object's command span is bounded against the input length below, so a
/// separate cap would be dead code.
const MAX_OBJECTS: usize = 4096;

/// Decode one 12-byte command. Caller guarantees `b.len() == 12`.
fn decode_cmd(b: &[u8]) -> Cmd {
    let (b0, b1, b2, b3) = (b[0], b[1], b[2], b[3]);
    Cmd {
        op_cnt: (b0 >> 5) & 0x7,
        grp: (b0 >> 3) & 0x3,
        sub_grp: b0 & 0x7,
        imm_op1: (b1 >> 7) & 1 == 1,
        imm_op2: (b1 >> 6) & 1 == 1,
        branch_opt: b1 & 0x0f,
        cmp_opt: b2 & 0x0f,
        set_opt: b3 & 0x1f,
        dst: u32::from_be_bytes([b[4], b[5], b[6], b[7]]),
        src: u32::from_be_bytes([b[8], b[9], b[10], b[11]]),
    }
}

/// Parse `MovieObject.bdmv` into its programs. Returns `None` on any structural
/// problem.
pub(crate) fn parse(d: &[u8]) -> Option<Vec<MovieObject>> {
    if d.get(0..4)? != b"MOBJ" {
        return None;
    }
    let num = be_u16(d, 48)? as usize;
    if num > MAX_OBJECTS {
        return None;
    }
    let mut off = 50usize;
    let mut objs = Vec::with_capacity(num);
    for _ in 0..num {
        // Object header: flags(1) + reserved(1) + num_cmds(u16).
        let num_cmds = be_u16(d, off + 2)? as usize;
        off = off.checked_add(4)?;
        let span = num_cmds.checked_mul(CMD_LEN)?;
        let end = off.checked_add(span)?;
        if end > d.len() {
            return None;
        }
        let mut cmds = Vec::with_capacity(num_cmds);
        for i in 0..num_cmds {
            let s = off + i * CMD_LEN;
            cmds.push(decode_cmd(&d[s..s + CMD_LEN]));
        }
        off = end;
        objs.push(MovieObject { cmds });
    }
    Some(objs)
}

#[cfg(test)]
#[path = "mobj_tests.rs"]
pub(crate) mod tests;

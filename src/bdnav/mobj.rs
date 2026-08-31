//! Parse `/BDMV/MovieObject.bdmv` — the HDMV navigation programs, per their
//! documented binary layout. Read as a documented format, never executed here:
//! bounds-checked, never panics.
//!
//! Layout: `"MOBJ"` + version(4) + reserved… ; `MovieObjects()` at byte 40 is
//! `length`(u32) + reserved(u32) + `num_objects`(u16 @48), then objects from
//! byte 50. Each object is `flags`(1) + reserved(1) + `num_cmds`(u16) followed
//! by `num_cmds` 12-byte navigation commands.

/// One decoded 12-byte navigation command. Field layout (big-endian): byte 0 =
/// `op_cnt(3) grp(2) sub_grp(3)`; byte 1 = `imm_op1(1) imm_op2(1) branch_opt(4)`
/// (low nibble); byte 2 low nibble = `cmp_opt`; byte 3 low 5 bits = `set_opt`;
/// then `dst`(u32) and `src`(u32).
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
/// Sanity caps (real discs are far under these; some densely-branched
/// dispatchers run several thousand commands in one object).
const MAX_OBJECTS: usize = 4096;
const MAX_CMDS: usize = 200_000;

fn be_u16(d: &[u8], o: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*d.get(o)?, *d.get(o + 1)?]))
}

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
        if num_cmds > MAX_CMDS {
            return None;
        }
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
pub(crate) mod tests {
    use super::*;

    /// Encode one command from its opcode bytes + operands.
    pub(crate) fn cmd(b0: u8, b1: u8, b2: u8, b3: u8, dst: u32, src: u32) -> [u8; 12] {
        let mut b = [0u8; 12];
        b[0] = b0;
        b[1] = b1;
        b[2] = b2;
        b[3] = b3;
        b[4..8].copy_from_slice(&dst.to_be_bytes());
        b[8..12].copy_from_slice(&src.to_be_bytes());
        b
    }

    pub(crate) fn build(objects: &[&[[u8; 12]]]) -> Vec<u8> {
        let mut d = vec![0u8; 50];
        d[0..4].copy_from_slice(b"MOBJ");
        d[48..50].copy_from_slice(&(objects.len() as u16).to_be_bytes());
        for obj in objects {
            d.push(0); // flags
            d.push(0); // reserved
            d.extend_from_slice(&(obj.len() as u16).to_be_bytes());
            for c in *obj {
                d.extend_from_slice(c);
            }
        }
        d
    }

    #[test]
    fn parses_objects_and_commands() {
        // op_cnt=1, grp=BRANCH(0), sub_grp=PLAY(2), branch_opt=PLAY_PL(0), imm dst.
        let b0 = (1 << 5) | 2;
        let play = cmd(b0, 0x80, 0, 0, 11, 0);
        let d = build(&[&[play]]);
        let objs = parse(&d).expect("parses");
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0].cmds.len(), 1);
        assert_eq!(objs[0].cmds[0].dst, 11);
        assert_eq!(objs[0].cmds[0].sub_grp, 2);
    }

    #[test]
    fn rejects_truncation() {
        let play = cmd((1 << 5) | 2, 0x80, 0, 0, 11, 0);
        let d = build(&[&[play]]);
        assert!(parse(&d[..d.len() - 3]).is_none());
        assert!(parse(b"NOPE").is_none());
    }

    #[test]
    fn rejects_object_count_over_the_cap() {
        // A real (non-truncated) buffer declaring MAX_OBJECTS + 1 empty objects:
        // if the cap were removed, this would parse fine (nothing to truncate on),
        // so only the cap itself can reject it.
        let empty: Vec<&[[u8; 12]]> = vec![&[]; MAX_OBJECTS + 1];
        let d = build(&empty);
        assert!(
            parse(&d).is_none(),
            "object count over MAX_OBJECTS must be rejected"
        );
    }

    // NOTE: no "rejects_cmd_count_over_the_cap" test — `num_cmds` is a u16
    // field (max 65535), always < MAX_CMDS, so that branch is unreachable via
    // honest bytes; a test for it would be tautological. Flagged, not faked.
}

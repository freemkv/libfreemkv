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

// No command-count cap exists to test: `num_cmds` is a u16 (max 65535) and
// each object's span is bounded against the input length, so the former
// MAX_CMDS check was dead code and was removed rather than left flagged.

#[test]
fn decode_cmd_masks_ignore_reserved_bits() {
    // b2/b3 upper bits are reserved and must not leak into cmp_opt/set_opt.
    let c = decode_cmd(&cmd(0xFF, 0xFF, 0xF3, 0xE5, 0, 0));
    assert_eq!((c.op_cnt, c.grp, c.sub_grp), (7, 3, 7));
    assert!(c.imm_op1 && c.imm_op2);
    assert_eq!((c.branch_opt, c.cmp_opt, c.set_opt), (0x0f, 0x03, 0x05));
}

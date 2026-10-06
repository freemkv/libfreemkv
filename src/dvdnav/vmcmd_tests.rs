use super::*;

fn h(s: &str) -> [u8; 8] {
    let v: Vec<u8> = (0..8)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
        .collect();
    v.try_into().unwrap()
}

// A set's link flag covers sub-ops 0..=LINKSUB_MAX inclusive; above it, no link.
#[test]
fn set_link_flag_boundary_at_linksub_max() {
    assert!(decode(&h("7101000000010010")).link, "sub-op 0x10 is a link");
    assert!(!decode(&h("7101000000010011")).link, "sub-op 0x11 is not");
}

// Every link code from LinkPGCN through LinkCN leaves the pre list; neighbours do not.
#[test]
fn set_link_flag_covers_linkpgcn_through_linkcn() {
    for (cmd, link) in [
        (3, false),
        (4, true),
        (5, true),
        (6, true),
        (7, true),
        (8, false),
    ] {
        let c = decode(&h(&format!("710{cmd:x}000000000001")));
        assert_eq!(c.link, link, "cmd {cmd}");
    }
}

// Special sub-command 3 (SetTmpPML) sets SPRM13 then gotos byte7 when cond holds.
#[test]
fn set_tmp_pml_decodes_as_goto() {
    let c = decode(&h("0003000000000005"));
    assert_eq!(c.instr, Instr::Goto { line: 5 });
}

// KATs taken from real-world First-Play command streams.
#[test]
fn first_play_jumptt_1_decodes() {
    let c = decode(&h("3002000000010000"));
    assert_eq!(c.instr, Instr::JumpTt { ttn: 1 });
    assert!(c.compare.is_none());
}

#[test]
fn first_play_jumpss_vtsm_root_decodes() {
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
fn title_dispatch_is_conditional_linkpgn_2() {
    // 20 a6 ... CmpLink: if GPRM0 == 2 -> LinkPGN 2 (the cell where the
    // feature begins).
    let c = decode(&h("20a6000000020002"));
    assert_eq!(c.instr, Instr::LinkPgn { pgn: 2 });
    let cmp = c.compare.expect("conditional");
    assert_eq!(cmp.op, 2); // ==
    assert_eq!(cmp.lhs_reg, 0); // GPRM0
    assert!(cmp.immediate);
    assert_eq!(cmp.imm, 2);
}

#[test]
fn root_button_is_linkpgcn_37() {
    assert_eq!(
        decode(&h("2004000000000025")).instr,
        Instr::LinkPgcn { pgcn: 37 }
    );
}

#[test]
fn scene_button_is_linkpgn() {
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
    // First-Play pre[0]: 71 00 | reg=byte3=6 | imm(bytes4-5)=0x03e8 -> g6 = 1000
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

// Regression for the link sub-op decode: 0 = NOP, 1 = LinkSub.
#[test]
fn link_subop_zero_is_nop_one_is_linksub() {
    assert_eq!(decode(&h("2000000000000000")).instr, Instr::Nop);
    assert_eq!(
        decode(&h("2001000000000010")).instr,
        Instr::LinkSub { sub: 0x10 }
    );
}

// if_version_1 register compare: the register operand is the LOW byte of the
// reg-or-immediate field (byte5), NOT the high byte (byte4). Distinct values
// in byte4 (0xAA) vs byte5 (0x05) pin the field down.
#[test]
fn link_register_compare_rhs_is_low_byte() {
    // 20 26: link, cmp=EQ(2), dircmp=0(register) ; cmd=6 LinkPGN (pgn=byte7=2)
    let c = decode(&h("20260003aa050002"));
    assert_eq!(c.instr, Instr::LinkPgn { pgn: 2 });
    let cmp = c.compare.expect("conditional");
    assert!(!cmp.immediate);
    assert_eq!(cmp.lhs_reg, 3);
    assert_eq!(cmp.rhs_reg, 5); // low byte of bytes4-5, not the 0xAA high byte
}

// if_version_3 (set-GPRM) register compare: the register operand is the LOW
// byte of the reg-or-immediate field (byte7), NOT the high byte (byte6).
// Distinct values in byte6 (0xBB) vs byte7 (0x07) pin the field down.
#[test]
fn setgprm_register_compare_rhs_is_low_byte() {
    // 71 20: set-GPRM (type 3), cmp=EQ(2), dircmp=0(register); lhs=byte2=2.
    let c = decode(&h("712002000000bb07"));
    let cmp = c.compare.expect("conditional");
    assert_eq!(cmp.op, 2);
    assert!(!cmp.immediate);
    assert_eq!(cmp.lhs_reg, 2);
    assert_eq!(cmp.rhs_reg, 7); // low byte of bytes6-7, not the 0xBB high byte
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

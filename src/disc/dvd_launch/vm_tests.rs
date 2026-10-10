use super::*;

#[test]
fn program_link_sets_authored_highlight_without_losing_register_state() {
    let mut r = Registers::default();
    r.gprm[8] = Some(2);
    assert_eq!(
        execute(&[0x20, 6, 0, 0, 0, 0, 4, 1], &mut r),
        Ok(Flow::Program(1))
    );
    assert_eq!(r.sprm[8], Some(1024));
    assert_eq!(r.gprm[8], Some(2));
    assert_eq!(
        execute(&[0x20, 6, 0, 0, 0, 0, 0, 1], &mut r),
        Ok(Flow::Program(1))
    );
    assert_eq!(r.sprm[8], Some(1024));
    for byte in [1, 2, 3, 145, 148, 252] {
        assert!(decode(&[0x20, 6, 0, 0, 0, 0, byte, 1]).is_err());
    }
}

#[test]
fn authored_set_then_link_retains_register_and_destination() {
    let mut r = Registers::default();
    assert_eq!(
        execute(&[0x71, 4, 0, 0, 0, 3, 0, 8], &mut r),
        Ok(Flow::Pgc(8))
    );
    assert_eq!(r.gprm[0], Some(3));
    assert_eq!(
        execute(&[0x51, 0, 0, 0x81, 0, 0, 0, 0], &mut r),
        Ok(Flow::Next)
    );
    assert_eq!(r.sprm[1], Some(1));
    assert_eq!(
        execute(&[0x71, 1, 0, 8, 0, 2, 0, 13], &mut r),
        Ok(Flow::Tail)
    );
    assert_eq!(r.gprm[8], Some(2));
}

#[test]
fn unknown_predicates_are_not_cold_zero_and_unknown_opcodes_fail_closed() {
    let c = decode(&[0, 0xa1, 0, 0, 0, 3, 0, 6]).unwrap();
    let mut r = Registers::default();
    assert_eq!(r.condition(c.compare), None);
    r.gprm[0] = Some(1);
    assert_eq!(r.condition(c.compare), Some(false));
    r.gprm[0] = Some(3);
    assert_eq!(r.condition(c.compare), Some(true));
    for cmd in [
        [0x30, 15, 0, 0, 0, 0, 0, 0],
        [0, 3, 0, 0, 0, 0, 0, 1],
        [0x78, 0, 0, 0, 0, 1, 0, 0],
        [0x71, 4, 1, 0, 0, 3, 0, 8],
        [0x20, 6, 0, 0, 0, 0, 1, 1],
        [0x20, 1, 0, 0, 0, 0, 1, 5],
    ] {
        assert!(decode(&cmd).is_err());
    }
}

fn cmp(op: u8, immediate: bool, imm: u16, rhs_reg: u8) -> Option<Compare> {
    Some(Compare {
        op,
        lhs_reg: 0,
        immediate,
        imm,
        rhs_reg,
    })
}

#[test]
fn condition_evaluates_every_operator_with_immediate_and_register_rhs() {
    let mut r = Registers::default();
    r.gprm[0] = Some(0b0110);
    r.gprm[1] = Some(0b0001);
    // (op, rhs value, expected for lhs 0b0110)
    let table = [
        (1, 0b0010, true),
        (1, 0b0001, false),
        (2, 6, true),
        (2, 5, false),
        (3, 5, true),
        (3, 6, false),
        (4, 6, true),
        (4, 7, false),
        (5, 5, true),
        (5, 6, false),
        (6, 6, true),
        (6, 5, false),
        (7, 7, true),
        (7, 6, false),
    ];
    for (op, rhs, want) in table {
        assert_eq!(
            r.condition(cmp(op, true, rhs, 0)),
            Some(want),
            "imm op {op} {rhs}"
        );
        r.gprm[2] = Some(rhs);
        assert_eq!(
            r.condition(cmp(op, false, 0, 2)),
            Some(want),
            "reg op {op} {rhs}"
        );
    }
    assert_eq!(r.condition(None), Some(true));
}

#[test]
fn condition_unknown_operands_and_invalid_ops_fail_closed() {
    let mut r = Registers::default();
    assert_eq!(r.condition(cmp(2, true, 0, 0)), None);
    r.gprm[0] = Some(1);
    assert_eq!(r.condition(cmp(2, false, 0, 3)), None);
    assert_eq!(r.condition(cmp(2, false, 0, 100)), None);
    for op in [0, 8, 255] {
        assert_eq!(r.condition(cmp(op, true, 1, 0)), None);
    }
}

#[test]
fn after_wait_clears_gprms_and_highlight_only() {
    let mut r = Registers::default();
    r.gprm[0] = Some(3);
    r.sprm[1] = Some(2);
    r.sprm[8] = Some(1024);
    let w = r.after_wait();
    assert!(w.gprm.iter().all(Option::is_none));
    assert_eq!((w.sprm[1], w.sprm[8]), (Some(2), None));
}

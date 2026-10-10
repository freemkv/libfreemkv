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

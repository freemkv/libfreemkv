use super::*;

pub(super) fn nav(at: u32, ea: u32, next: u32) -> Vec<u8> {
    let mut b = vec![0; 2048];
    b[..4].copy_from_slice(&[0, 0, 1, 0xba]);
    b[4] = 0x44;
    b[0x400..0x407].copy_from_slice(&[0, 0, 1, 0xbf, 3, 0xfa, 1]);
    long(&mut b, 0x407 + 4, at);
    long(&mut b, 0x407 + 8, ea);
    word(&mut b, 0x407 + 24, 1);
    b[0x407 + 27] = 1;
    long(&mut b, 0x407 + 34, 30);
    long(&mut b, 0x407 + 38, 40);
    long(&mut b, 0x407 + 314, next);
    b
}
fn memory() -> Memory {
    let mut m = Memory::default();
    m.put(110, &nav(10, 4, 0x8000_0005));
    m.put(115, &nav(15, 4, 0x3fff_ffff));
    m
}
fn walk(
    m: &mut Memory,
    budget: &mut usize,
) -> std::result::Result<Vec<crate::disc::Extent>, produce::Failure> {
    super::super::interleave::walk(m, 100, 10, 19, &[0, 1, 0, 1], budget)
}

#[test]
fn terminal_vobu_chain_proves_exact_cell_end_without_trusting_ilvu_end() {
    assert_eq!(
        walk(&mut memory(), &mut 8).unwrap(),
        vec![crate::disc::Extent {
            start_lba: 110,
            sector_count: 10
        }]
    );
    let mut m = memory();
    m.put(110, &nav(10, 9, 0x3fff_ffff));
    assert_eq!(
        walk(&mut m, &mut 1).unwrap(),
        vec![crate::disc::Extent {
            start_lba: 110,
            sector_count: 10
        }]
    );
}

#[test]
fn terminal_vobu_rejects_missing_premature_markers_gaps_overlaps_and_wrong_ids() {
    for (lba, at, value) in [
        (110, 38, 4),
        (110, 314, 0x3fff_ffff),
        (115, 314, 0x8000_0005),
        (110, 314, 0x8000_0006),
        (110, 314, 0x8000_0004),
        (110, 314, 0),
        (110, 314, 0xc000_0005),
        (115, 4, 16),
        (115, 24, 0x0002_0001),
        (115, 24, 0x0001_0002),
        (115, 8, 5),
        (115, 8, 3),
    ] {
        let mut m = memory();
        let pack = m.sectors.get_mut(&lba).unwrap();
        long(pack, 0x407 + at, value);
        assert!(
            walk(&mut m, &mut 8).is_err(),
            "accepted mutation lba={lba} at={at} value={value:#x}"
        );
    }
}

#[test]
fn terminal_vobu_propagates_cancellation_short_read_and_budget_exhaustion() {
    let mut m = memory();
    m.halt_at = Some(115);
    assert!(matches!(
        walk(&mut m, &mut 8),
        Err(produce::Failure::Io(crate::Error::Halted))
    ));
    let mut m = memory();
    m.short_at = Some(115);
    assert!(matches!(
        walk(&mut m, &mut 8),
        Err(produce::Failure::Review(
            DvdLaunchReviewReason::IncompleteNavigation
        ))
    ));
    assert!(matches!(
        walk(&mut memory(), &mut 1),
        Err(produce::Failure::Review(
            DvdLaunchReviewReason::BudgetExceeded
        ))
    ));
}

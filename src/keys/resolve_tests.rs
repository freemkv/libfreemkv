use super::*;

fn ext(start_lba: u32, sector_count: u32) -> Extent {
    Extent {
        start_lba,
        sector_count,
    }
}

#[test]
fn sector_at_walks_extents_and_reports_the_run_left() {
    let e = [ext(100, 10), ext(500, 5)];
    assert_eq!(sector_at(&e, 0), Some((100, 10)));
    assert_eq!(sector_at(&e, 9), Some((109, 1)));
    assert_eq!(
        sector_at(&e, 10),
        Some((500, 5)),
        "first sector of the next extent"
    );
    assert_eq!(sector_at(&e, 14), Some((504, 1)));
    assert_eq!(sector_at(&e, 15), None, "past the end");
    assert_eq!(sector_at(&[], 0), None);
}

#[test]
fn sector_at_refuses_an_lba_beyond_u32() {
    let e = [ext(u32::MAX, 10)];
    assert_eq!(sector_at(&e, 0), Some((u32::MAX, 10)));
    assert_eq!(sector_at(&e, 1), None);
}

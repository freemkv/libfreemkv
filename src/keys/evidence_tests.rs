use super::*;

#[test]
fn sorted_ranges_merge_overlapping_and_touching_runs() {
    assert_eq!(
        sorted_ranges(vec![(20, 30), (1, 5), (5, 9), (8, 12), (40, 41)]),
        vec![(1, 12), (20, 30), (40, 41)]
    );
    assert_eq!(
        sorted_ranges(vec![(1, 10), (2, 3)]),
        vec![(1, 10)],
        "contained run"
    );
    assert_eq!(
        sorted_ranges(vec![(1, 5), (6, 9)]),
        vec![(1, 5), (6, 9)],
        "a gap stays"
    );
    assert!(sorted_ranges(vec![]).is_empty());
}

#[test]
fn in_segment_is_start_inclusive_end_exclusive() {
    let s = [(10, 20), (30, 40)];
    for (lba, want) in [
        (9, false),
        (10, true),
        (19, true),
        (20, false),
        (29, false),
        (30, true),
        (39, true),
        (40, false),
    ] {
        assert_eq!(in_segment(&s, lba), want, "lba {lba}");
    }
    assert!(!in_segment(&[], 5));
}

#[test]
fn fatal_reads_are_a_stop_a_gone_source_or_a_dead_bus() {
    assert!(fatal_read(&Error::Halted));
    assert!(fatal_read(&Error::ScsiError {
        opcode: 0x28,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }));
    assert!(!fatal_read(&Error::ScsiError {
        opcode: 0x28,
        status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
        sense: None,
    }));
    assert!(!fatal_read(&Error::IoError {
        source: std::io::Error::from_raw_os_error(5),
    }));
    assert!(!fatal_read(&Error::DiscRead {
        sector: 1,
        status: None,
        sense: None,
    }));
}

struct Short(usize);

impl SectorSource for Short {
    fn read_sectors(&mut self, _: u32, _: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        buf[..self.0].fill(7);
        Ok(self.0)
    }
}

#[test]
fn read_unit_needs_the_whole_unit() {
    let full = crate::aacs::content::ALIGNED_UNIT_LEN;
    let got = read_unit(&mut Short(full), 0).unwrap().unwrap();
    assert_eq!((got.len(), got[full - 1]), (full, 7));
    assert!(read_unit(&mut Short(full - 1), 0).unwrap().is_none());
    assert!(read_unit(&mut Short(0), 0).unwrap().is_none());
}

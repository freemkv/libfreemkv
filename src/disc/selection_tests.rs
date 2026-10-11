use super::*;
use crate::disc::{Clip, Extent};

fn clip(id: &str, in_time: u32, out_time: u32) -> Clip {
    Clip {
        marks_on_pes_clock: true,
        clip_id: id.into(),
        in_time,
        out_time,
        duration_secs: 0.0,
        source_packets: 0,
        feed_span: None,
    }
}

fn spans(segments: &[ContentSegment]) -> Vec<(&str, u64, u64)> {
    segments
        .iter()
        .map(|s| (s.unit.as_str(), s.start, s.end))
        .collect()
}

#[test]
fn clips_describe_content_only_when_every_item_is_exact() {
    let clips = [clip("00002", 90, 900), clip("00001", 0, u32::MAX)];
    let segments = ContentSegment::from_clips(&clips);
    assert_eq!(
        spans(&segments),
        [("00002", 90, 900), ("00001", 0, u64::from(u32::MAX))]
    );
    for bad in [clip(" ", 0, 9), clip("00003", 9, 9), clip("00003", 9, 1)] {
        assert!(ContentSegment::from_clips(&[clips[0].clone(), bad]).is_empty());
    }
}

#[test]
fn cell_extents_are_half_open_sector_ranges_in_one_unit() {
    let extent = |start_lba, sector_count| Extent {
        start_lba,
        sector_count,
    };
    let segments = ContentSegment::from_cell_extents(&[extent(u32::MAX, 1), extent(10, 5)]);
    let top = u64::from(u32::MAX);
    assert_eq!(spans(&segments), [("", top, top + 1), ("", 10, 15)]);
    assert!(ContentSegment::from_cell_extents(&[extent(10, 5), extent(20, 0)]).is_empty());
}

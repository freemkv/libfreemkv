use super::*;

fn arrival(spans: Vec<(u32, u32, u64, usize)>) -> Arrival {
    Arrival {
        spans,
        pieces: Vec::new(),
        base: 0,
        segments: Vec::new(),
        cache: ProofCache::default(),
        format: ContentFormat::BdTs,
        stop: StopKind::Image,
        strikes: Mutex::new(Vec::new()),
    }
}

#[test]
fn a_span_holds_its_start_and_excludes_its_end() {
    let a = arrival(vec![(100, 200, 7, 0), (300, 310, 9, 1)]);
    assert_eq!(a.span_at(99), None);
    assert_eq!(a.span_at(100), Some((100, 200, 7, 0)));
    assert_eq!(a.span_at(199), Some((100, 200, 7, 0)));
    assert_eq!(a.span_at(200), None, "end is exclusive");
    assert_eq!(a.span_at(250), None, "between spans");
    assert_eq!(a.span_at(300), Some((300, 310, 9, 1)));
    assert_eq!(a.span_at(309), Some((300, 310, 9, 1)));
    assert_eq!(a.span_at(310), None);
    assert_eq!(arrival(Vec::new()).span_at(0), None);
}

#[test]
fn a_proof_covers_exactly_the_lbas_in_its_pieces() {
    let a = arrival(vec![(100, 200, 7, 0)]);
    assert!(a.covers(100));
    assert!(a.covers(199));
    assert!(!a.covers(99));
    assert!(!a.covers(200));
}

#[test]
fn segment_lbas_are_start_inclusive_end_exclusive() {
    let mut a = arrival(Vec::new());
    a.segments = vec![(10, 20), (30, 40)];
    for (lba, want) in [
        (9, false),
        (10, true),
        (19, true),
        (20, false),
        (35, true),
        (40, false),
    ] {
        assert_eq!(a.in_segment(lba), want, "lba {lba}");
    }
}

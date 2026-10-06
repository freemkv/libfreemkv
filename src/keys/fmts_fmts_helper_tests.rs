use super::*;

// ── layout decision helpers (behaviors flagged by audit) ──

// BEHAVIOR 1 — segment filter: a segment mapping inside the title's extents is kept,
// past-clip dropped, all-outside -> empty.
#[test]
fn filter_addressable_segments_keeps_only_in_title_segments() {
    use crate::aacs::segment::Segment;
    // One extent covering clip bytes [0, 60*2048) = [0, 122880).
    let extents = vec![Extent {
        start_lba: 500,
        sector_count: 60,
    }];
    // start_spn 100 → clip byte 19200 < 122880 → maps to an LBA → KEEP.
    let inside = Segment {
        index: 1,
        start_spn: 100,
        end_spn: 199,
    };
    // start_spn 1000 → clip byte 192000 >= 122880 → clip_byte_to_lba None → DROP.
    let outside = Segment {
        index: 2,
        start_spn: 1000,
        end_spn: 1099,
    };
    let kept = super::filter_addressable_segments(vec![inside, outside], &extents);
    assert_eq!(kept, vec![inside], "only the in-title segment survives");
    // All-outside → empty; `layout` maps this to Ok(None).
    assert!(
        super::filter_addressable_segments(vec![outside], &extents).is_empty(),
        "no addressable segment → empty (→ resolver Ok(None))"
    );
    // Boundary: a segment whose start is the LAST clip byte still maps (Some);
    // one exactly at the clip end (122880) does not.
    let at_last = Segment {
        index: 3,
        start_spn: (122_879 / 192) as u32, // 639 → byte 122688 < 122880
        end_spn: 700,
    };
    let at_end = Segment {
        index: 4,
        start_spn: (122_880 / 192) as u32, // 640 → byte 122880 == clip end → None
        end_spn: 700,
    };
    assert_eq!(
        super::filter_addressable_segments(vec![at_last, at_end], &extents),
        vec![at_last],
        "start inside the clip is kept; start at/after the clip end is dropped"
    );
}

// forensic + fills must cover every LBA of every extent EXACTLY once: no gap, no overlap.
fn assert_gapless(
    extents: &[Extent],
    forensic: &[(u32, u32, usize, crate::decrypt::Phase)],
    fills: &[(u32, u32, usize, crate::decrypt::Phase)],
) {
    let mut spans: Vec<(u32, u32)> = forensic.iter().map(|&(s, e, _, _)| (s, e)).collect();
    spans.extend(fills.iter().map(|&(s, e, _, _)| (s, e)));
    spans.sort_unstable();
    for w in spans.windows(2) {
        assert!(w[0].1 <= w[1].0, "spans overlap: {:?} vs {:?}", w[0], w[1]);
    }
    for ext in extents {
        let end = ext.start_lba + ext.sector_count;
        for lba in ext.start_lba..end {
            let covering = spans.iter().filter(|&&(s, e)| lba >= s && lba < e).count();
            assert_eq!(
                covering, 1,
                "LBA {lba} covered {covering}× (want exactly 1)"
            );
        }
    }
}

// BEHAVIOR 3 — gap-fill range arithmetic, exhaustive over segment positions.
#[test]
fn fill_base_key_gaps_is_gapless_over_every_extent() {
    use crate::decrypt::Phase::{All, Even, Odd};
    let base = 0usize;

    // No segments → the whole extent is base key.
    let ext = vec![Extent {
        start_lba: 100,
        sector_count: 60,
    }];
    let forensic: Vec<(u32, u32, usize, crate::decrypt::Phase)> = vec![];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert_eq!(fills, vec![(100, 160, base, All)], "no segments → all base");
    assert_gapless(&ext, &forensic, &fills);

    // One segment mid-extent → base | forensic | base, gapless.
    let forensic = vec![(120, 130, 5, Even)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert_eq!(
        fills,
        vec![(100, 120, base, All), (130, 160, base, All)],
        "mid-extent segment → leading + trailing base"
    );
    assert_gapless(&ext, &forensic, &fills);

    // Segment at extent START → only a trailing base fill (no zero-length lead).
    let forensic = vec![(100, 130, 5, Even)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert_eq!(
        fills,
        vec![(130, 160, base, All)],
        "segment at start → no leading base, one trailing"
    );
    assert_gapless(&ext, &forensic, &fills);

    // Segment at extent END → only a leading base fill (no zero-length trail).
    let forensic = vec![(130, 160, 5, Even)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert_eq!(
        fills,
        vec![(100, 130, base, All)],
        "segment at end → one leading base, no trailing"
    );
    assert_gapless(&ext, &forensic, &fills);

    // Whole extent is one segment → no base fill at all, still gapless.
    let forensic = vec![(100, 160, 5, Even)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert!(
        fills.is_empty(),
        "segment spans whole extent → no base fill"
    );
    assert_gapless(&ext, &forensic, &fills);

    // Adjacent segments (touching, no gap between) → NO zero-length base range
    // between them (guards the `cs > cur` off-by-one).
    let forensic = vec![(110, 120, 5, Even), (120, 130, 6, Odd)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, base);
    assert_eq!(
        fills,
        vec![(100, 110, base, All), (130, 160, base, All)],
        "adjacent segments → no zero-length fill between them"
    );
    assert_gapless(&ext, &forensic, &fills);

    // Multi-extent: a segment mid-first-extent and one at the start of the
    // second. Fills are per-extent and the union is gapless across both.
    let exts = vec![
        Extent {
            start_lba: 100,
            sector_count: 60,
        }, // [100, 160)
        Extent {
            start_lba: 1000,
            sector_count: 40,
        }, // [1000, 1040)
    ];
    let forensic = vec![(120, 130, 5, Even), (1000, 1010, 7, Odd)];
    let fills = super::fill_base_key_gaps(&exts, &forensic, base);
    assert_eq!(
        fills,
        vec![
            (100, 120, base, All),
            (130, 160, base, All),
            (1010, 1040, base, All),
        ],
        "each extent filled independently"
    );
    assert_gapless(&exts, &forensic, &fills);
}

// Forensic ranges arrive in table RECORD order, not LBA order; the gap walk's forward sweep
// needs them sorted.
#[test]
fn fill_base_key_gaps_sorts_cuts_that_arrive_in_table_order_not_lba_order() {
    use crate::decrypt::Phase::{All, Even, Odd};
    let ext = vec![Extent {
        start_lba: 100,
        sector_count: 60,
    }];
    // Table order: the HIGH segment recorded before the LOW one.
    let forensic = vec![(140, 150, 6, Odd), (110, 120, 5, Even)];
    let fills = super::fill_base_key_gaps(&ext, &forensic, 0);
    assert_eq!(
        fills,
        vec![(100, 110, 0, All), (120, 140, 0, All), (150, 160, 0, All),],
        "the gaps around BOTH forensic cuts must be filled, whatever order the \
             segment table listed them in"
    );
    // The load-bearing invariant: exactly one range covers every content LBA.
    assert_gapless(&ext, &forensic, &fills);
}

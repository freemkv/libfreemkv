use super::*;
use crate::mux::ts::PesPacket;

/// A PCS display-set block with one composition object; `forced` sets the
/// forced_on_flag (0x40) in its flags byte at offset 17.
fn pcs_display(forced: bool) -> Vec<u8> {
    let mut d = vec![0u8; 18];
    d[0] = SEGMENT_PCS;
    d[PCS_NUM_OBJECTS_OFFSET] = 1;
    d[PCS_FIRST_OBJECT_FLAGS_OFFSET] = if forced { PCS_FORCED_ON_FLAG } else { 0 };
    d
}

#[test]
fn display_set_forced_flag_detection() {
    assert_eq!(display_set_is_forced(&pcs_display(true)), Some(true));
    assert_eq!(display_set_is_forced(&pcs_display(false)), Some(false));
    // Other flag bits set but not forced_on_flag → still not forced.
    let mut cropped = pcs_display(false);
    cropped[PCS_FIRST_OBJECT_FLAGS_OFFSET] = 0x80; // object_cropped_flag only
    assert_eq!(display_set_is_forced(&cropped), Some(false));
}

#[test]
fn display_set_forced_none_for_non_display() {
    // Clear PCS (0 objects) → None.
    let mut clear = pcs_display(false);
    clear[PCS_NUM_OBJECTS_OFFSET] = 0;
    assert_eq!(display_set_is_forced(&clear), None);
    // Non-PCS segment → None.
    let mut ods = pcs_display(true);
    ods[0] = 0x15; // ODS
    assert_eq!(display_set_is_forced(&ods), None);
    // Truncated (no flags byte) → None, no panic.
    assert_eq!(display_set_is_forced(&pcs_display(true)[..15]), None);
    assert_eq!(display_set_is_forced(&[]), None);
}

// One non-forced display set anywhere settles the track as not forced, even when the LAST
// set is forced.
#[test]
fn a_forced_last_display_set_does_not_make_a_mixed_track_forced() {
    let mut t = ForcedTracker::new();
    for forced in [true, false, true] {
        t.observe(&pcs_display(forced));
    }
    assert!(!t.is_forced());
    assert!(t.settled_not_forced());
    assert_eq!(t.facts().forced_displays, 2);
}

// `observed()` distinguishes "unknown" (leave the vendor flag alone) from a settled
// verdict, so an unread/undecrypted track can't overwrite a correct vendor "forced" flag
// with "not forced".
#[test]
fn observed_stays_false_until_a_real_display_set_is_seen() {
    let mut t = ForcedTracker::new();
    assert!(!t.observed(), "a fresh tracker has seen nothing");
    assert!(!t.is_forced(), "and has no verdict to give");

    // Blocks that carry no display set must not count as observation: a clear
    // PCS (zero composition objects), a non-PCS segment, a truncated PCS, and
    // an empty frame.
    let mut clear = pcs_display(false);
    clear[PCS_NUM_OBJECTS_OFFSET] = 0;
    let mut ods = pcs_display(true);
    ods[0] = 0x15;
    for block in [clear, ods, pcs_display(true)[..15].to_vec(), Vec::new()] {
        t.observe(&block);
        assert!(
            !t.observed(),
            "a block with no display set leaves the verdict unknown"
        );
    }

    // The first real display set is what flips it.
    t.observe(&pcs_display(true));
    assert!(t.observed());
    assert!(t.is_forced(), "the only display set seen was forced");
}

fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0x1200,
        pts,
        dts: None,
        data,
        discontinuity: false,
    }
}

// Minimum-viable PCS bytes: type 0x16, segment_length (2 bytes),
// then 11 bytes of PCS fields ending in number_of_composition_objects.
fn pcs_bytes(num_objects: u8) -> Vec<u8> {
    let mut v = vec![SEGMENT_PCS, 0x00, 0x0B];
    v.extend_from_slice(&[0x07, 0x80, 0x04, 0x38]); // 1920x1080
    v.push(0x10); // frame_rate
    v.extend_from_slice(&[0x00, 0x01]); // composition_number
    v.push(0x80); // composition_state = EpochStart
    v.push(0x00); // palette_update + reserved
    v.push(0x00); // palette_id_ref
    v.push(num_objects);
    v
}

#[test]
fn display_then_clear_yields_duration() {
    let mut parser = PgsParser::new();

    // Display PCS at PTS 90000 (= 1s)
    let display = pcs_bytes(1);
    let frames = parser.parse(&make_pes(display.clone(), Some(90000)));
    assert!(frames.is_empty(), "display PCS should be pending");

    // Empty PCS at PTS 270000 (= 3s)
    let clear = pcs_bytes(0);
    let frames = parser.parse(&make_pes(clear, Some(270000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
    assert_eq!(frames[0].duration_ns, Some(2_000_000_000));
    assert_eq!(frames[0].data, display);
}

#[test]
fn replace_without_clear_still_emits_prior_with_duration() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let frames = parser.parse(&make_pes(pcs_bytes(1), Some(180000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
    assert_eq!(frames[0].duration_ns, Some(1_000_000_000));
}

#[test]
fn non_pcs_segment_appends_to_pending() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    // ODS-like segment (type 0x15)
    let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA, 0xBB], Some(90000)));
    assert!(frames.is_empty());
    // Clear closes the set; data should include the appended bytes.
    let frames = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
    assert_eq!(frames.len(), 1);
    let data = &frames[0].data;
    assert!(data.windows(5).any(|w| w == [0x15, 0x00, 0x02, 0xAA, 0xBB]));
}

#[test]
fn post_gap_segment_is_not_spliced_onto_pending_set() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let mut pes = make_pes(vec![0x15, 0x00, 0x02, 0xAA, 0xBB], None);
    pes.discontinuity = true;
    assert!(parser.parse(&pes).is_empty());
    assert!(parser.flush().is_empty(), "the truncated set is dropped");
}

#[test]
fn post_gap_pts_segments_are_skipped_until_next_pcs() {
    // Complete pending set (PCS + END) survives the gap; the PTS-bearing
    // post-gap orphans are dropped up to the next PCS.
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let _ = parser.parse(&make_pes(vec![0x80, 0x00, 0x00], Some(90000)));
    let mut gap = make_pes(vec![0x15, 0x00, 0x02, 0xAA, 0xBB], Some(180000));
    gap.discontinuity = true;
    let out = parser.parse(&gap);
    assert_eq!(out.len(), 1, "complete held subtitle is emitted");
    assert_eq!(out[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
    assert!(
        parser
            .parse(&make_pes(vec![0x80, 0x00, 0x00], Some(180000)))
            .is_empty()
    );
    assert!(
        parser
            .parse(&make_pes(pcs_bytes(1), Some(270000)))
            .is_empty()
    );
    let tail = parser.flush();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].pts_ns, 3_000_000_000);
}

#[test]
fn post_gap_pts_segments_drop_incomplete_pending() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let mut gap = make_pes(vec![0x15, 0x00, 0x02, 0xAA, 0xBB], Some(180000));
    gap.discontinuity = true;
    assert!(parser.parse(&gap).is_empty());
    assert!(
        parser
            .parse(&make_pes(vec![0x80, 0x00, 0x00], Some(180000)))
            .is_empty()
    );
    assert!(parser.flush().is_empty());
}

#[test]
fn pcs_after_gap_drops_an_incomplete_pending_set() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let mut next = make_pes(pcs_bytes(1), Some(180000));
    next.discontinuity = true;
    assert!(
        parser.parse(&next).is_empty(),
        "a set cut off before its END is not emitted"
    );
    let tail = parser.flush();
    assert_eq!(tail.len(), 1, "the post-gap PCS opens a fresh set");
    assert_eq!(tail[0].pts_ns, 2_000_000_000);
}

#[test]
fn pcs_after_gap_emits_a_complete_pending_set() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let _ = parser.parse(&make_pes(vec![0x80, 0x00, 0x00], Some(90000)));
    let mut next = make_pes(pcs_bytes(1), Some(180000));
    next.discontinuity = true;
    let out = parser.parse(&next);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].duration_ns, Some(1_000_000_000));
}

#[test]
fn segments_after_a_dropped_pcs_are_skipped_until_the_next_pcs() {
    let mut short_pcs = pcs_bytes(1);
    short_pcs.truncate(PCS_NUM_OBJECTS_OFFSET);
    for (dropped, pts) in [(short_pcs, Some(90000)), (pcs_bytes(1), None)] {
        let mut parser = PgsParser::new();
        assert!(parser.parse(&make_pes(dropped, pts)).is_empty());
        for seg in [
            vec![0x17, 0x00, 0x00],
            vec![0x15, 0x00, 0x02, 0xAA, 0xBB],
            vec![0x80, 0x00, 0x00],
        ] {
            assert!(
                parser.parse(&make_pes(seg, Some(90000))).is_empty(),
                "orphan segments of a lost PCS are not emitted"
            );
        }
        let _ = parser.parse(&make_pes(pcs_bytes(1), Some(180000)));
        assert_eq!(parser.flush().len(), 1, "the next PCS resyncs");
    }
}

#[test]
fn a_gap_resets_the_clear_scan_offset() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(0), Some(90000)));
    assert_ne!(parser.clear_scan_offset, 0, "the clear PCS was walked");
    let mut gap = make_pes(vec![0x17, 0x00, 0x00], None);
    gap.discontinuity = true;
    let _ = parser.parse(&gap);
    assert_eq!(parser.clear_scan_offset, 0);
}

#[test]
fn pending_buffer_is_capped() {
    let mut parser = PgsParser::new();
    // Open a display set.
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));

    // Flood with non-PCS segments far exceeding the cap.
    let chunk = vec![0x15u8; 256 * 1024]; // 256 KB ODS-like segment
    let floods = (MAX_PGS_PENDING_BYTES / chunk.len()) + 32;
    for _ in 0..floods {
        let frames = parser.parse(&make_pes(chunk.clone(), Some(90000)));
        assert!(frames.is_empty(), "non-PCS appends should not emit");
    }

    // The pending buffer must not have grown without bound.
    let pending_len = parser.pending.as_ref().map(|(_, b)| b.len()).unwrap_or(0);
    assert!(
        pending_len <= MAX_PGS_PENDING_BYTES,
        "pending buffer {pending_len} exceeded cap {MAX_PGS_PENDING_BYTES}"
    );

    // A following PCS still resyncs and emits the (capped) pending set.
    let frames = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
    assert_eq!(frames.len(), 1);
}

#[test]
fn flush_emits_final_pending_subtitle() {
    let mut parser = PgsParser::new();

    // Display PCS at PTS 90000 — buffered as pending, no follower.
    let display = pcs_bytes(1);
    let frames = parser.parse(&make_pes(display.clone(), Some(90000)));
    assert!(frames.is_empty(), "display PCS should be pending");

    // EOF: without flush() this last subtitle would be dropped.
    let frames = parser.flush();
    assert_eq!(frames.len(), 1, "final pending subtitle must flush");
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
    assert_eq!(frames[0].data, display);
    // Trailing block has no follower PCS, so its real end is unknown; it now
    // carries the synthesized fallback dwell (was `None`) so every subtitle
    // block gets a BlockDuration (issue #52).
    assert_eq!(frames[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
}

#[test]
fn display_pcs_without_pts_is_not_stored_with_zero_start() {
    // A display PCS with no PTS has an unknown start time; must NOT store it
    // with a 0 sentinel, or a later clear PCS at real PTS T would emit
    // pts_ns=0, duration_ns=T. The malformed display PCS is skipped instead.
    let mut parser = PgsParser::new();
    let frames = parser.parse(&make_pes(pcs_bytes(1), None));
    assert!(frames.is_empty(), "no-PTS display PCS emits nothing");
    assert!(
        parser.pending.is_none(),
        "no-PTS display PCS must not be stored as pending"
    );

    // A subsequent well-formed display + clear pair must time correctly,
    // unpolluted by the skipped no-PTS PCS.
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(270000)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, 1_000_000_000);
    assert_eq!(f[0].duration_ns, Some(2_000_000_000));
}

#[test]
fn clear_pcs_without_pts_emits_fallback_duration() {
    // A clear PCS that lacks a PTS can't compute a real duration; the pending
    // display is still emitted, now with the synthesized fallback dwell (was
    // `None`) so the subtitle block carries a BlockDuration (issue #52).
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let f = parser.parse(&make_pes(pcs_bytes(0), None));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, 1_000_000_000, "pending keeps its real start");
    assert_eq!(
        f[0].duration_ns,
        Some(DEFAULT_PGS_DURATION_NS),
        "fallback duration when the clear PCS carries no PTS"
    );
}

#[test]
fn truncated_pcs_flushes_pending_and_resyncs() {
    // A PCS too short to carry number_of_composition_objects arriving with a
    // pending display must close that display (undurated) and drop the
    // truncated header, not append its bytes into the pending bitmap.
    let mut parser = PgsParser::new();
    let display = pcs_bytes(1);
    assert!(
        parser
            .parse(&make_pes(display.clone(), Some(90000)))
            .is_empty()
    );

    // A 13-byte (<= PCS_NUM_OBJECTS_OFFSET) PCS: truncated.
    let truncated = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET];
    let frames = parser.parse(&make_pes(truncated, Some(180000)));
    assert_eq!(frames.len(), 1, "pending display flushed on truncated PCS");
    assert_eq!(frames[0].data, display, "pending bitmap not polluted");
    assert_eq!(
        frames[0].duration_ns,
        Some(DEFAULT_PGS_DURATION_NS),
        "flushed with fallback duration (issue #52)"
    );
    assert!(parser.pending.is_none(), "parser resynced");
}

#[test]
fn lone_non_pcs_without_pts_is_dropped() {
    // A non-PCS segment with no pending set and no PTS must be dropped, not
    // emitted at pts_ns = 0 (which would land a stray bitmap at time zero).
    let mut parser = PgsParser::new();
    let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA], None));
    assert!(frames.is_empty(), "no pending + no PTS → dropped");
}

#[test]
fn lone_non_pcs_with_pts_passes_through() {
    // A lone non-PCS segment WITH a PTS still passes through.
    let mut parser = PgsParser::new();
    let frames = parser.parse(&make_pes(vec![0x15, 0x00, 0x02, 0xAA], Some(90000)));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].pts_ns, 1_000_000_000);
}

#[test]
fn flush_with_nothing_pending_is_empty() {
    let mut parser = PgsParser::new();
    assert!(parser.flush().is_empty());
}

#[test]
fn codec_private_none() {
    let parser = PgsParser::new();
    assert!(parser.codec_private().is_none());
}

#[test]
fn parse_empty_pes() {
    let mut parser = PgsParser::new();
    let pes = make_pes(Vec::new(), Some(0));
    assert!(parser.parse(&pes).is_empty());
}

// --- number_of_composition_objects lives at byte 13 ---

#[test]
fn num_objects_read_from_offset_13() {
    // PCS_NUM_OBJECTS_OFFSET = 3-byte seg header + 10 PCS field bytes = 13.
    // A byte at offset 13 of 0 = clear, > 0 = display. Build a PCS where
    // every byte before 13 is non-zero noise and byte 13 alone decides.
    let mut display = vec![SEGMENT_PCS];
    display.extend_from_slice(&[0xFF; 12]); // bytes 1..=12 noise
    display.push(1); // byte 13: num_objects = 1 → display
    let mut parser = PgsParser::new();
    assert!(
        parser.parse(&make_pes(display, Some(90000))).is_empty(),
        "byte 13 == 1 → display PCS (pending), no emit yet"
    );
    // Now a clear: byte 13 == 0.
    let mut clear = vec![SEGMENT_PCS];
    clear.extend_from_slice(&[0xFF; 12]);
    clear.push(0); // byte 13 = 0 → clear
    let f = parser.parse(&make_pes(clear, Some(270000)));
    assert_eq!(f.len(), 1, "byte 13 == 0 closes the pending display");
}

// --- duration computation and clamping ---

#[test]
fn duration_clamps_to_zero_when_clear_precedes_display() {
    // A clear PTS earlier than the display PTS (corrupt/out-of-order stream)
    // must clamp duration to 0 via saturating_sub, never wrap to a huge u64.
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(270000))); // display @ 3s
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000))); // clear @ 1s
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, 3_000_000_000, "keeps display start");
    assert_eq!(
        f[0].duration_ns,
        Some(0),
        "clear-before-display clamps to 0, no u64 wrap"
    );
}

#[test]
fn duration_zero_when_equal_pts() {
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000)));
    assert_eq!(f[0].duration_ns, Some(0));
}

#[test]
fn pathologically_large_computed_duration_clamps_to_cap() {
    // A span past MAX_PGS_DURATION_NS is untrusted, but clamps to the
    // 30s cap itself (L053), not the unrelated 5s fallback dwell.
    let display_pts = 90_000_i64; // 1 s
    // 40 s later in 90 kHz ticks (> 30 s cap).
    let clear_pts = display_pts + 40 * 90_000;
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(display_pts)));
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(clear_pts)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].duration_ns,
        Some(MAX_PGS_DURATION_NS),
        "a >30s computed span is clamped to the 30s cap, not the 5s fallback"
    );
}

#[test]
fn legitimate_long_duration_is_not_clamped() {
    // A real, in-range dwell (10 s, well under the 30 s cap) is preserved
    // exactly — the clamp must never clip a legitimately long subtitle.
    let display_pts = 90_000_i64; // 1 s
    let clear_pts = display_pts + 10 * 90_000; // +10 s
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(1), Some(display_pts)));
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(clear_pts)));
    assert_eq!(f[0].duration_ns, Some(10_000_000_000));
}

// --- clear / replace edge cases ---

#[test]
fn clear_with_no_pending_is_preserved_until_end_or_flush() {
    // Preserve a leading clear too: decoder state can outlive a seek.
    let mut parser = PgsParser::new();
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(90000)));
    assert!(f.is_empty(), "wait for the clear's END");
    let tail = parser.flush();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].data, pcs_bytes(0));
    assert_eq!(tail[0].pts_ns, 1_000_000_000);
    assert_eq!(tail[0].duration_ns, Some(0));
}

#[test]
fn three_displays_each_close_the_previous() {
    // Successive display PCS (no intervening clear) each emit the prior one
    // timed to the new display's PTS. display@1s, display@2s, display@3s →
    // emits [1s dur 1s], [2s dur 1s]; the last (3s) is held.
    let mut parser = PgsParser::new();
    let f0 = parser.parse(&make_pes(pcs_bytes(1), Some(90000)));
    assert!(f0.is_empty());
    let f1 = parser.parse(&make_pes(pcs_bytes(1), Some(180000)));
    assert_eq!(f1.len(), 1);
    assert_eq!(f1[0].pts_ns, 1_000_000_000);
    assert_eq!(f1[0].duration_ns, Some(1_000_000_000));
    let f2 = parser.parse(&make_pes(pcs_bytes(1), Some(270000)));
    assert_eq!(f2.len(), 1);
    assert_eq!(f2[0].pts_ns, 2_000_000_000);
    assert_eq!(f2[0].duration_ns, Some(1_000_000_000));
    // Third held; flush emits it with the fallback dwell (no follower PCS).
    let tail = parser.flush();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].pts_ns, 3_000_000_000);
    assert_eq!(tail[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
}

#[test]
fn pcs_exactly_at_offset_boundary_is_truncated() {
    // A PCS of EXACTLY PCS_NUM_OBJECTS_OFFSET (13) bytes has no byte at index
    // 13 → treated as truncated (`<= PCS_NUM_OBJECTS_OFFSET`). With a pending
    // display it flushes that undurated and resyncs.
    let mut parser = PgsParser::new();
    let display = pcs_bytes(1);
    let _ = parser.parse(&make_pes(display.clone(), Some(90000)));
    let exactly_13 = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET]; // 13 bytes
    let f = parser.parse(&make_pes(exactly_13, Some(180000)));
    assert_eq!(f.len(), 1, "13-byte PCS is truncated → flush pending");
    assert_eq!(f[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
    assert!(parser.pending.is_none());
}

#[test]
fn pcs_one_byte_past_offset_reads_num_objects() {
    // A PCS of PCS_NUM_OBJECTS_OFFSET + 1 (14) bytes is the minimum that can
    // carry number_of_composition_objects (index 13 exists). It must be read
    // as a real PCS, not truncated.
    let mut parser = PgsParser::new();
    let mut display = vec![SEGMENT_PCS; PCS_NUM_OBJECTS_OFFSET];
    display.push(1); // index 13 = 1 → display, 14 bytes total
    assert!(
        parser.parse(&make_pes(display, Some(90000))).is_empty(),
        "14-byte display PCS is pending (not truncated)"
    );
    assert!(parser.pending.is_some(), "stored as pending display");
}

#[test]
fn non_pcs_without_pending_with_pts_passes_through_keyframe() {
    // A lone non-PCS segment (first byte != 0x16) with a PTS and no pending
    // set passes through as a keyframe frame at its PTS.
    let mut parser = PgsParser::new();
    let f = parser.parse(&make_pes(vec![0x14, 0x00, 0x01, 0xAA], Some(90000)));
    assert_eq!(f.len(), 1);
    assert!(f[0].keyframe);
    assert_eq!(f[0].pts_ns, 1_000_000_000);
    // A pass-through segment has no known end; it now carries the fallback
    // dwell (was `None`) so it too gets a BlockDuration (issue #52).
    assert_eq!(f[0].duration_ns, Some(DEFAULT_PGS_DURATION_NS));
}

#[test]
fn display_pcs_data_preserved_verbatim() {
    // The emitted frame data is the display PCS bytes (plus any appended
    // non-PCS continuation), verbatim — the bitmap must not be altered.
    let mut parser = PgsParser::new();
    let display = pcs_bytes(2); // num_objects = 2
    let _ = parser.parse(&make_pes(display.clone(), Some(90000)));
    let f = parser.parse(&make_pes(pcs_bytes(0), Some(180000)));
    assert_eq!(f[0].data, display, "display PCS data emitted verbatim");
}

// ── the demotion guard ──────────────────────────────────────────────────

fn facts(displays: u32, forced: u32) -> ForcedFacts {
    ForcedFacts {
        displays,
        forced_displays: forced,
    }
}

/// The case the guard exists for: a disc whose authoring never sets
/// `forced_on_flag`. Nothing about the absence of a flag nobody uses can
/// contradict a vendor label, however many display sets confirm the absence.
#[test]
fn nothing_is_demotable_on_a_disc_that_never_sets_the_flag() {
    for displays in [1u32, DEMOTE_MIN_DISPLAY_SETS, 2_000, u32::MAX] {
        assert!(
            !demotable(facts(displays, 0), false, displays),
            "{displays} unflagged display sets on a flagless disc prove nothing"
        );
    }
}

// The share test multiplies a disc-derived count; it must not wrap at the top of u32.
#[test]
fn the_share_test_does_not_overflow_on_huge_counts() {
    assert!(demotable(facts(u32::MAX, 1), true, u32::MAX));
    assert!(demotable(facts(u32::MAX / 2, 1), true, u32::MAX));
    assert!(!demotable(
        facts(DEMOTE_MIN_DISPLAY_SETS, 1),
        true,
        u32::MAX
    ));
}

// A track that mixes forced and non-forced sets needs no sibling corroboration: the flag is
// in use ON THIS TRACK.
#[test]
fn a_mixed_track_corroborates_the_flag_itself() {
    assert!(demotable(facts(108, 2), false, 137));
}

/// ...but the shape test still applies to it. A SMALL track with a couple of
/// flagged sets is a forced track whose authoring flagged some of its signs —
/// demoting that is the exact mistake the shape test exists to prevent.
#[test]
fn a_small_mixed_track_is_not_demotable_against_a_busy_disc() {
    assert!(!demotable(facts(30, 1), true, 2_000));
    assert!(
        !demotable(facts(4, 1), true, 4),
        "and too few sets to say anything either way"
    );
}

/// With the flag in use elsewhere on the disc, the shape decides. Measured:
/// a forced-narrative track carries tens of display sets, a full dialogue
/// track one to two thousand.
#[test]
fn shape_decides_once_the_disc_is_known_to_use_the_flag() {
    assert!(
        demotable(facts(2_000, 0), true, 2_000),
        "the busiest track on the disc, with no forced set on it, is a full track"
    );
    assert!(
        !demotable(facts(20, 0), true, 2_000),
        "a track at one percent of the busiest is the forced track its label claims"
    );
    assert!(
        !demotable(facts(DEMOTE_MIN_DISPLAY_SETS - 1, 0), true, 8),
        "too few display sets for their absence of flags to mean anything"
    );
    assert!(
        demotable(facts(DEMOTE_MIN_DISPLAY_SETS, 0), true, 8),
        "at the threshold, with the shape of the busiest track, it is demotable"
    );
}

// The share rule at its edge against a 2 000-set busiest track: 500 sets is exactly one
// quarter (in), 499 is under it (out). Literal counts, so a changed divisor fails.
#[test]
fn the_share_of_busiest_boundary_is_one_quarter() {
    assert!(demotable(facts(500, 0), true, 2_000));
    assert!(!demotable(facts(499, 0), true, 2_000));
    // A vendor-forced track at 30% of the busiest is a full track, not a forced one.
    assert!(demotable(facts(600, 0), true, 2_000));
}

/// Never on no evidence at all: a track nobody observed cannot contradict
/// anything.
#[test]
fn an_unobserved_track_is_never_demotable() {
    assert!(!demotable(facts(0, 0), true, 2_000));
}

/// Saturating counters: a pathological stream must pin the counts, never wrap
/// them (and never panic on overflow in a debug build).
#[test]
fn display_counts_saturate_instead_of_wrapping() {
    let mut t = ForcedTracker::new();
    t.displays = u32::MAX;
    t.forced_displays = u32::MAX;
    let mut pcs = vec![0u8; 18];
    pcs[0] = SEGMENT_PCS;
    pcs[PCS_NUM_OBJECTS_OFFSET] = 1;
    pcs[PCS_FIRST_OBJECT_FLAGS_OFFSET] = PCS_FORCED_ON_FLAG;
    t.observe(&pcs);
    assert_eq!(t.facts().displays, u32::MAX);
    assert_eq!(t.facts().forced_displays, u32::MAX);
}

// A lone non-PCS segment with a PTS is emitted straight through rather than accumulated,
// and must still carry provenance.
#[test]
fn a_lone_segment_emitted_directly_still_carries_provenance() {
    let mut parser = PgsParser::new();
    // A non-PCS segment (type 0x15 = ODS) with a PTS and no pending set.
    let mut p = make_pes(vec![0x15, 0x00, 0x00, 0x00, 0x04, 1, 2, 3, 4], Some(90_000));
    p.source = Some(crate::pes::SourcePos::at_byte(7_777));
    let frames = parser.parse(&p);
    assert!(
        !frames.is_empty(),
        "a lone segment with a PTS is passed through"
    );
    assert_eq!(
        frames[0].source.map(|s| s.byte),
        Some(7_777),
        "emitted straight from this packet, so it carries this packet's offset"
    );
}

#[test]
fn clear_scan_offset_tracks_confirmed_segments_across_appends() {
    // Deterministic (no wall-clock): after each append, `clear_scan_offset`
    // must sit at the buffer's confirmed end, proving the walk resumes
    // rather than rescanning from byte 0 every call (L026).
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(0), Some(0)));
    // The opening clear PCS is itself one complete (non-END) segment, so
    // the very first call already confirms the whole 14-byte buffer.
    assert_eq!(parser.clear_scan_offset, pcs_bytes(0).len());

    let seg = vec![0x17u8, 0x00, 0x02, 0xAA, 0xBB]; // one complete 5-byte segment
    for i in 1..=20u32 {
        let _ = parser.parse(&make_pes(seg.clone(), None));
        let data_len = parser.pending.as_ref().unwrap().1.len();
        assert_eq!(
            parser.clear_scan_offset, data_len,
            "after {i} appends the confirmed offset should track to the buffer end"
        );
    }

    // A segment split across two PES: the declared payload (7 bytes,
    // size 10) arrives incomplete first. The confirmed offset must stay
    // at THAT segment's start, not advance past it.
    let before = parser.pending.as_ref().unwrap().1.len();
    let split_head = vec![0x17u8, 0x00, 0x07, 0xAA, 0xAA]; // 5 of 10 bytes
    let _ = parser.parse(&make_pes(split_head, None));
    assert_eq!(
        parser.clear_scan_offset, before,
        "an incomplete trailing segment must not be confirmed"
    );

    // Completing it confirms the whole segment.
    let _ = parser.parse(&make_pes(vec![0xBB; 5], None)); // remaining 5 of 10
    let data_len = parser.pending.as_ref().unwrap().1.len();
    assert_eq!(
        parser.clear_scan_offset, data_len,
        "completed split segment is now confirmed"
    );
}

#[test]
fn complete_clear_pts_scan_steps_stay_linear_in_appends() {
    // Deterministic (no wall-clock): N appends must cost O(N) walk
    // iterations, not O(N^2) (L026). A 2x-per-append budget has slack yet
    // still catches a quadratic regression, which blows it by orders of magnitude.
    let mut parser = PgsParser::new();
    let _ = parser.parse(&make_pes(pcs_bytes(0), Some(0)));
    let seg = vec![0x17u8, 0x00, 0x02, 0xAA, 0xBB];
    let n: u64 = 5_000;
    for _ in 0..n {
        let _ = parser.parse(&make_pes(seg.clone(), None));
    }
    assert!(
        parser.scan_steps <= 2 * n,
        "scan_steps {} exceeded the O(n) budget 2*n={} for n={n} appends",
        parser.scan_steps,
        2 * n
    );
}

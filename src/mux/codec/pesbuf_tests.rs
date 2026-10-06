use super::*;

fn pes(data: &[u8], pts: Option<i64>, byte: Option<u64>) -> PesPacket {
    PesPacket {
        pid: 0x1100,
        pts,
        dts: None,
        data: data.to_vec(),
        source: byte.map(SourcePos::at_byte),
        discontinuity: false,
    }
}

/// The point of the type: a unit assembled from two packets is attributed
/// to the one that carried its FIRST byte, not the one that completed it.
#[test]
fn a_unit_spanning_two_packets_belongs_to_the_packet_it_started_in() {
    let mut b = PesBuf::with_capacity(64);
    b.push(&pes(&[1, 2, 3], Some(90_000), Some(1_000)));
    b.push(&pes(&[4, 5, 6], Some(180_000), Some(2_000)));
    let f = b.front();
    assert_eq!(f.presentation_ns(), Some(pts_to_ns(90_000)));
    assert_eq!(f.source.unwrap().byte, 1_000, "the packet it STARTED in");
}

/// And the timestamp and the source come from the SAME packet — the
/// property that cannot hold when each is derived separately.
#[test]
fn timestamp_and_source_always_come_from_one_packet() {
    let mut b = PesBuf::with_capacity(64);
    b.push(&pes(&[1, 2], Some(90_000), Some(10)));
    b.push(&pes(&[3, 4], Some(180_000), Some(20)));
    b.push(&pes(&[5, 6], Some(270_000), Some(30)));
    for off in 0..6usize {
        let f = b.facts_at(off);
        let want = match off {
            0 | 1 => (Some(pts_to_ns(90_000)), 10),
            2 | 3 => (Some(pts_to_ns(180_000)), 20),
            _ => (Some(pts_to_ns(270_000)), 30),
        };
        assert_eq!(
            (f.presentation_ns(), f.source.unwrap().byte),
            want,
            "offset {off}"
        );
    }
}

/// Draining a completed unit must leave the REMAINDER attributed to the
/// packet that carried it, not to whichever packet starts next.
#[test]
fn draining_keeps_the_remainder_attributed_to_its_own_packet() {
    let mut b = PesBuf::with_capacity(64);
    b.push(&pes(&[1, 2, 3, 4], Some(90_000), Some(1_000)));
    b.push(&pes(&[5, 6], Some(180_000), Some(2_000)));
    // Emit a 2-byte unit: bytes 3 and 4 are still the FIRST packet's.
    b.drain(2);
    assert_eq!(b.front().source.unwrap().byte, 1_000);
    // Emit those two: now the front is genuinely the second packet's.
    b.drain(2);
    assert_eq!(b.front().source.unwrap().byte, 2_000);
    assert_eq!(b.front().presentation_ns(), Some(pts_to_ns(180_000)));
}

/// A payload-less PES contributed no byte, so it must not shadow the packet
/// that did — otherwise the next unit takes a timestamp from a packet whose
/// bytes are not in it.
#[test]
fn an_empty_packet_does_not_claim_the_next_units_bytes() {
    let mut b = PesBuf::with_capacity(64);
    b.push(&pes(&[1, 2], Some(90_000), Some(1_000)));
    b.push(&pes(&[], Some(180_000), Some(2_000)));
    assert_eq!(b.front().source.unwrap().byte, 1_000);
    assert_eq!(b.len(), 2, "an empty packet adds no bytes");
}

/// Draining everything and refilling must not resurrect a stale mark.
#[test]
fn a_fully_drained_buffer_takes_its_next_packets_facts() {
    let mut b = PesBuf::with_capacity(64);
    b.push(&pes(&[1, 2], Some(90_000), Some(1_000)));
    b.drain(2);
    assert!(b.is_empty());
    b.push(&pes(&[9], Some(450_000), Some(9_000)));
    assert_eq!(b.front().source.unwrap().byte, 9_000);
    assert_eq!(b.front().presentation_ns(), Some(pts_to_ns(450_000)));
}

/// The two ways a parser can obtain facts must agree when the unit begins
/// in the packet just handed over — otherwise "one pattern" is two.
#[test]
fn a_unit_starting_in_this_packet_reads_the_same_either_way() {
    let p = pes(&[1, 2, 3], Some(90_000), Some(4_242));
    let mut b = PesBuf::with_capacity(16);
    b.push(&p);
    assert_eq!(b.front(), PesFacts::of(&p));
}

/// Defense-in-depth: a caller that pushes without ever draining must not
/// grow the buffer — or its marks — without bound. Past MAX_BUFFERED_BYTES
/// the oldest bytes are dropped, and the mark count is bounded with them.
#[test]
fn a_caller_that_never_drains_cannot_grow_the_buffer_without_bound() {
    let mut b = PesBuf::with_capacity(64);
    let chunk = vec![0u8; 1024 * 1024]; // 1 MiB per push
    // Push well past the 16 MiB cap (24 MiB total) without ever draining.
    for i in 0..24u64 {
        b.push(&pes(&chunk, Some(90_000 + i as i64), Some(i * 1000)));
    }
    assert!(
        b.len() <= MAX_BUFFERED_BYTES,
        "buffer stayed capped at {MAX_BUFFERED_BYTES}, got {}",
        b.len()
    );
    // Marks are bounded with the bytes: at most one per surviving MiB (+1
    // for the retained covering mark), never one per push.
    assert!(
        b.mark_count() <= MAX_BUFFERED_BYTES / (1024 * 1024) + 2,
        "mark count stayed bounded, got {}",
        b.mark_count()
    );
}

/// Over-draining is clamped rather than panicking: a parser that
/// mis-sizes a unit must not take the process down.
#[test]
fn draining_past_the_end_is_clamped() {
    let mut b = PesBuf::with_capacity(16);
    b.push(&pes(&[1, 2], Some(90_000), Some(1)));
    b.drain(99);
    assert!(b.is_empty());
}

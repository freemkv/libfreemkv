use super::*;

fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
    PesPacket {
        source: None,
        pid: 0x1100,
        pts,
        dts: None,
        data,
        discontinuity: false,
    }
}

/// A minimal FLAC-frame-shaped buffer: sync `0xFFF8`, a plausible header
/// (block code 1 = 192 samples, rate code 9 = 44.1 kHz), some payload, and a
/// trailing CRC-16 so the whole-frame residue is zero (a valid frame).
fn make_flac_frame(payload_len: usize) -> Vec<u8> {
    let mut f = vec![0u8; 6 + payload_len + 2];
    f[0] = 0xFF;
    f[1] = 0xF8; // sync + fixed blocksize
    f[2] = (1 << 4) | 9; // bs_code=1 (192), sr_code=9 (44100)
    // bytes 3..end-2 arbitrary; last two bytes carry the CRC-16.
    let n = f.len();
    let c = crc16_ansi(&f[..n - 2]);
    f[n - 2] = (c >> 8) as u8;
    f[n - 1] = (c & 0xFF) as u8;
    assert_eq!(crc16_ansi(&f), 0, "finalized frame has zero residue");
    f
}

#[test]
fn valid_frame_is_kept() {
    let mut p = FlacParser::new();
    let f = p.parse(&make_pes(make_flac_frame(100), Some(90000)));
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].pts_ns, pts_to_ns(90000));
    assert_eq!(p.dropped_frames(), 0);
}

#[test]
fn pes_without_pts_carries_last_timestamp_not_zero() {
    // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
    // carry the last known timestamp forward — resetting to 0 would corrupt
    // A/V sync. Mirrors the adts.rs guard test.
    let mut p = FlacParser::new();
    p.parse(&make_pes(make_flac_frame(100), Some(90000)));
    let f = p.parse(&make_pes(make_flac_frame(100), None));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(90000),
        "carried forward, not reset to 0"
    );
}

#[test]
fn corrupt_frame_is_dropped() {
    let mut p = FlacParser::new();
    let mut frame = make_flac_frame(100);
    frame[20] ^= 0xFF; // corrupt a payload byte → CRC residue nonzero
    assert!(crc16_ansi(&frame) != 0);
    let f = p.parse(&make_pes(frame, Some(90000)));
    assert!(f.is_empty(), "corrupt FLAC frame dropped");
    assert_eq!(p.dropped_frames(), 1);
    // 192 samples @ 44.1 kHz ≈ 4.354 ms of silence accounted.
    assert_eq!(
        p.dropped_duration_ns(),
        (192u64 * 1_000_000_000 + 44_100 / 2) / 44_100
    );
}

#[test]
fn corrupt_drop_preserves_sync_via_own_pts() {
    // Each packet carries its own PTS, so dropping one leaves the next frame
    // on its true timeline — a gap, not a shift.
    let mut p = FlacParser::new();
    let mut bad = make_flac_frame(100);
    bad[20] ^= 0xFF;
    assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
    let f = p.parse(&make_pes(make_flac_frame(100), Some(96000)));
    assert_eq!(f.len(), 1);
    assert_eq!(
        f[0].pts_ns,
        pts_to_ns(96000),
        "surviving frame keeps its own container PTS — the drop is a gap"
    );
}

#[test]
fn pes_dts_used_when_pts_absent() {
    let mut p = FlacParser::new();
    let mut pes = make_pes(make_flac_frame(100), None);
    pes.dts = Some(180_000);
    let f = p.parse(&pes);
    assert_eq!(f[0].pts_ns, pts_to_ns(180_000));
}

#[test]
fn pes_discontinuity_propagates_to_frame() {
    let mut p = FlacParser::new();
    let mut pes = make_pes(make_flac_frame(100), Some(90_000));
    pes.discontinuity = true;
    assert!(p.parse(&pes)[0].discontinuity);
    assert!(!p.parse(&make_pes(make_flac_frame(100), Some(96_000)))[0].discontinuity);
}

#[test]
fn poisoned_track_drops_even_valid_frames() {
    let mut p = FlacParser::new();
    // A run of verified-corrupt frames poisons the track (200-AU verdict gate).
    for i in 0..400 {
        let mut bad = make_flac_frame(100);
        bad[20] ^= 0xFF;
        assert!(p.parse(&make_pes(bad, Some(i * 90))).is_empty());
    }
    assert!(p.tally.is_poisoned());
    let before = p.dropped_frames();
    assert!(
        p.parse(&make_pes(make_flac_frame(100), Some(1_000_000)))
            .is_empty()
    );
    assert_eq!(p.dropped_frames(), before + 1);
}

// The blocking-strategy bit (0xFFF9 = variable blocksize) is masked off the sync test.
#[test]
fn variable_blocksize_frames_are_validated_too() {
    let mut p = FlacParser::new();
    let mut good = make_flac_frame(100);
    good[1] = 0xF9;
    let n = good.len();
    let c = crc16_ansi(&good[..n - 2]);
    good[n - 2] = (c >> 8) as u8;
    good[n - 1] = c as u8;
    assert_eq!(p.parse(&make_pes(good.clone(), Some(0))).len(), 1);
    let mut bad = good;
    bad[20] ^= 0xFF;
    assert!(p.parse(&make_pes(bad, Some(90_000))).is_empty());
    assert_eq!(p.dropped_frames(), 1);
}

#[test]
fn a_set_reserved_bit_is_not_a_flac_sync() {
    let mut p = FlacParser::new();
    let mut f = make_flac_frame(100);
    f[1] = 0xFA; // the mandatory-0 reserved bit set
    f[20] ^= 0xFF;
    assert_eq!(p.parse(&make_pes(f, Some(0))).len(), 1, "passed through");
    assert_eq!(p.dropped_frames(), 0);
}

#[test]
fn non_flac_packet_passes_through() {
    // A packet without the FLAC sync isn't a frame we can validate — never
    // false-drop it.
    let mut p = FlacParser::new();
    let f = p.parse(&make_pes(vec![0x00, 0x01, 0x02, 0x03], Some(0)));
    assert_eq!(f.len(), 1, "unrecognized packet passed through");
    assert_eq!(p.dropped_frames(), 0);
}

#[test]
fn empty_pes_emits_nothing() {
    let mut p = FlacParser::new();
    assert!(p.parse(&make_pes(Vec::new(), Some(0))).is_empty());
}

// FLAC packets are self-framing: parse() emits/drops on the spot, buffering
// nothing, so flush() delivers nothing. A manufactured tail frame would be a
// backwards-timestamp phantom (RFC 9559 §5.1.3.2) with no real FLAC frame.
#[test]
fn flush_adds_no_phantom_frame_after_the_last_real_packet() {
    let mut p = FlacParser::new();
    let mut emitted = Vec::new();
    emitted.extend(p.parse(&make_pes(make_flac_frame(100), Some(90_000))));
    emitted.extend(p.parse(&make_pes(make_flac_frame(120), Some(180_000))));
    // A frame whose CRC-16 residue is nonzero is dropped, not buffered.
    let mut corrupt = make_flac_frame(100);
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xFF;
    emitted.extend(p.parse(&make_pes(corrupt, Some(270_000))));
    assert_eq!(emitted.len(), 2, "two valid frames out, one dropped");
    assert_eq!(p.dropped_frames(), 1);

    let tail = p.flush();
    assert!(
        tail.is_empty(),
        "nothing is buffered past the last packet; flush produced {:?}",
        tail.iter()
            .map(|f| (f.pts_ns, f.data.len()))
            .collect::<Vec<_>>()
    );
    assert_eq!(emitted.len() + tail.len(), 2);
}

// The text guard in codec/mod.rs can't see `source: facts.source` writing a
// missing offset; only a runtime check proves the emitted frame carries the
// byte it was read from — needed for multi-clip placement by byte, not PTS.
#[test]
fn an_emitted_frame_carries_the_packets_source_offset() {
    let mut p = FlacParser::new();
    let mut pes = make_pes(make_flac_frame(100), Some(90_000));
    pes.source = Some(crate::pes::SourcePos::at_byte(7_777));
    let f = p.parse(&pes);
    assert!(!f.is_empty(), "the frame is emitted");
    assert_eq!(f[0].source.map(|s| s.byte), Some(7_777));
}

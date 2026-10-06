use super::*;

// L044: once poisoned, later drops are collateral; they must not add to the verified count.
#[test]
fn drops_after_poison_are_collateral() {
    let mut af = AudioFrames::new(
        "test",
        SyncSpec {
            mask: 0xf0,
            frame_len: |_| None,
            fixed: |_| 0,
            frame_ns: |_| None,
            min_frame: 7,
        },
    );
    while !af.tally.is_poisoned() {
        af.tally.record_drop(0, 0, 1, "bad");
    }
    let verified = af.tally.verified_dropped();
    let pes = PesPacket {
        source: None,
        pid: 0x1100,
        pts: Some(0),
        dts: None,
        data: vec![0xFF; 16],
        discontinuity: false,
    };
    assert!(af.parse(&pes, 7, |_| None).is_empty());
    assert_eq!(
        af.dropped_frames(),
        verified + 1,
        "the poisoned PES is counted"
    );
    assert_eq!(af.tally.verified_dropped(), verified);
}

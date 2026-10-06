use super::*;

#[test]
fn zero_timeout_means_one_default_everywhere() {
    assert_eq!(effective_timeout_ms(0), DEFAULT_TIMEOUT_MS);
    assert_eq!(effective_timeout_ms(1), 1);
    assert_eq!(effective_timeout_ms(12_345), 12_345);
    assert_eq!(spti_timeout_secs(0), 60);
}

#[test]
fn only_check_condition_carries_sense() {
    let mut sense = [0u8; 32];
    sense[0] = 0x70;
    sense[2] = 0x05;
    sense[12] = 0x24;
    let s = sense_for_status(SCSI_STATUS_CHECK_CONDITION, &sense, 32).expect("CHECK CONDITION");
    assert_eq!((s.sense_key, s.asc), (0x05, 0x24));
    // BUSY (08h), RESERVATION CONFLICT (18h), TASK SET FULL (28h): no sense on the wire.
    for status in [0x08u8, 0x18, 0x28] {
        assert!(
            sense_for_status(status, &[0u8; 32], 0).is_none(),
            "{status:#x}"
        );
    }
}

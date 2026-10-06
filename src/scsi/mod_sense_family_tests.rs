use super::*;

#[test]
fn classifies_each_named_sense_key() {
    assert_eq!(
        SenseFamily::from_sense_key(SENSE_KEY_NOT_READY),
        SenseFamily::NotReady
    );
    assert_eq!(
        SenseFamily::from_sense_key(SENSE_KEY_MEDIUM_ERROR),
        SenseFamily::Medium
    );
    assert_eq!(
        SenseFamily::from_sense_key(SENSE_KEY_HARDWARE_ERROR),
        SenseFamily::Hardware
    );
    assert_eq!(
        SenseFamily::from_sense_key(SENSE_KEY_ILLEGAL_REQUEST),
        SenseFamily::IllegalRequest
    );
    assert_eq!(
        SenseFamily::from_sense_key(SENSE_KEY_ABORTED_COMMAND),
        SenseFamily::Other
    );
}

#[test]
fn wedge_family_is_hardware_and_illegal_request_only() {
    assert!(SenseFamily::Hardware.is_wedge_family());
    assert!(SenseFamily::IllegalRequest.is_wedge_family());
    assert!(!SenseFamily::Medium.is_wedge_family());
    assert!(!SenseFamily::NotReady.is_wedge_family());
    assert!(!SenseFamily::Other.is_wedge_family());
}

use super::*;
use crate::spec::stop::{SS_1_SENSE_PROGRESS, SS_2_DESCRIPTOR_SENSE};

// Fixed format (70h): sense key, ASC/ASCQ, and the SKSV + progress bytes 15-17.
fn fixed(key: u8, sksv: bool, progress: u16) -> Vec<u8> {
    let mut s = vec![0u8; 18];
    s[0] = 0x70;
    s[2] = key;
    s[7] = 10;
    s[12] = 0x04;
    s[13] = 0x01;
    s[15] = if sksv { 0x80 } else { 0x00 };
    s[16..18].copy_from_slice(&progress.to_be_bytes());
    s
}

// Descriptor format (72h) with `before` other descriptors ahead of the
// sense-key specific one (type 02h, length 06h).
fn descriptor(key: u8, sksv: bool, progress: u16, before: usize) -> Vec<u8> {
    let mut s = vec![0x72, key, 0x04, 0x01, 0, 0, 0, 0];
    for _ in 0..before {
        s.extend_from_slice(&[0x00, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
    s.extend_from_slice(&[0x02, 0x06, 0, 0, if sksv { 0x80 } else { 0 }]);
    s.extend_from_slice(&progress.to_be_bytes());
    s.push(0);
    s[7] = (s.len() - 8) as u8;
    s
}

fn parse(s: &[u8]) -> Option<u16> {
    parse_sense_progress(s, s.len() as u8)
}

/// LD12e: fixed and descriptor formats; SKSV set/clear; NOT READY vs other keys;
/// short buffers → `None`.
#[test]
fn parse_sense_progress_fixed_and_descriptor() {
    assert_eq!(
        parse(&fixed(SENSE_KEY_NOT_READY, true, 0x1234)),
        Some(0x1234)
    );
    assert_eq!(
        parse(&descriptor(SENSE_KEY_NOT_READY, true, 0x1234, 0)),
        Some(0x1234)
    );
    let f = fixed(SENSE_KEY_NOT_READY, true, 7);
    for n in 0..f.len() {
        assert_eq!(parse_sense_progress(&f, n as u8), None, "fixed, {n} bytes");
    }
    let d = descriptor(SENSE_KEY_NOT_READY, true, 7, 0);
    for n in 0..d.len() - 1 {
        assert_eq!(
            parse_sense_progress(&d, n as u8),
            None,
            "descriptor, {n} bytes"
        );
    }
}

/// SP1: corroborated by SS-1 (SKSV set to one "indicates the SENSE KEY SPECIFIC
/// field contains valid information"): SKSV clear → `None`, whatever bytes 16-17 hold.
#[test]
fn progress_ignored_when_sksv_clear() {
    assert!(
        SS_1_SENSE_PROGRESS
            .text
            .contains("(SKSV) bit set to one indicates")
    );
    assert_eq!(parse(&fixed(SENSE_KEY_NOT_READY, false, 0xFFFF)), None);
    assert_eq!(
        parse(&descriptor(SENSE_KEY_NOT_READY, false, 0xFFFF, 0)),
        None
    );
}

/// SP2: corroborated by SS-1 Table 18 ("NO SENSE or NOT READY … Progress
/// indication"): the same bytes under another sense key are not progress.
#[test]
fn progress_only_for_not_ready_and_no_sense() {
    assert!(
        SS_1_SENSE_PROGRESS
            .text
            .starts_with("NO SENSE or NOT READY")
    );
    assert_eq!(parse(&fixed(SENSE_KEY_NO_SENSE, true, 9)), Some(9));
    for key in [
        SENSE_KEY_MEDIUM_ERROR,
        SENSE_KEY_ILLEGAL_REQUEST,
        SENSE_KEY_UNIT_ATTENTION,
    ] {
        assert_eq!(parse(&fixed(key, true, 9)), None, "fixed key {key}");
        assert_eq!(
            parse(&descriptor(key, true, 9, 0)),
            None,
            "descriptor key {key}"
        );
    }
}

/// SP3: corroborated by SS-1 ("a numerator that has 65 536 (10000h) as its
/// denominator"): big-endian, 0x8000 is half, 0xFFFF the maximum.
#[test]
fn progress_big_endian_fraction() {
    assert!(
        SS_1_SENSE_PROGRESS
            .text
            .contains("65 536 (10000h) as its denominator")
    );
    assert_eq!(
        parse(&fixed(SENSE_KEY_NOT_READY, true, 0x8000)),
        Some(32768)
    );
    assert_eq!(
        parse(&fixed(SENSE_KEY_NOT_READY, true, 0xFFFF)),
        Some(u16::MAX)
    );
    let mut s = fixed(SENSE_KEY_NOT_READY, true, 0);
    s[16] = 0x01;
    assert_eq!(parse(&s), Some(0x0100), "byte 16 is the MSB");
}

/// SP4: corroborated by SS-2: the sense key specific descriptor (type 02h) is
/// found at any position in the descriptor list; a truncated one → `None`.
#[test]
fn descriptor_format_progress_descriptor() {
    assert!(
        SS_2_DESCRIPTOR_SENSE
            .text
            .contains("72h (current errors) and 73h")
    );
    for before in 0..3 {
        let d = descriptor(SENSE_KEY_NOT_READY, true, 0x0203, before);
        assert_eq!(parse(&d), Some(0x0203), "{before} descriptors ahead");
        let mut deferred = d.clone();
        deferred[0] = 0x73;
        assert_eq!(parse(&deferred), Some(0x0203));
    }
    let mut cut = descriptor(SENSE_KEY_NOT_READY, true, 0x0203, 1);
    cut.truncate(cut.len() - 3);
    assert_eq!(parse(&cut), None, "truncated descriptor");
}

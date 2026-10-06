use super::*;

fn sense(sense_key: u8, asc: u8, ascq: u8) -> ScsiSense {
    ScsiSense {
        sense_key,
        asc,
        ascq,
    }
}

fn tur_err(sense_key: u8, asc: u8, ascq: u8) -> Result<()> {
    Err(Error::ScsiError {
        opcode: SCSI_TEST_UNIT_READY,
        status: SCSI_STATUS_CHECK_CONDITION,
        sense: Some(sense(sense_key, asc, ascq)),
    })
}

// Runs `tur_disc_presence` over scripted TUR outcomes; returns the verdict
// and how many TURs it issued.
fn presence(replies: Vec<Result<()>>) -> (Result<DiscPresence>, usize) {
    let mut replies = replies.into_iter();
    let mut calls = 0;
    let r = tur_disc_presence(|| {
        calls += 1;
        replies.next().unwrap_or(Ok(()))
    });
    (r, calls)
}

fn one(sense_key: u8, asc: u8, ascq: u8) -> Result<DiscPresence> {
    presence(vec![tur_err(sense_key, asc, ascq)]).0
}

// MMC-6 Table F.3: only 3Ah is MEDIUM NOT PRESENT. 04/01 is a mounted disc
// spinning up or changing Format-layer (§6.22.3), so it is Settling, never
// Absent; states that only exist with a medium loaded are Present.
#[test]
fn tur_disc_presence_follows_the_mmc6_readiness_table() {
    use DiscPresence::*;
    assert!(matches!(presence(vec![Ok(())]), (Ok(Present), 1)));
    for ascq in [0x00, 0x01, 0x02] {
        assert!(matches!(one(2, 0x3A, ascq), Ok(Absent)), "3A/{ascq:02x}");
    }
    for (asc, ascq) in [(0x04, 0x02), (0x04, 0x04), (0x04, 0x07), (0x04, 0x08)] {
        assert!(
            matches!(one(2, asc, ascq), Ok(Present)),
            "{asc:02x}/{ascq:02x}"
        );
    }
    for (asc, ascq) in [(0x0C, 0x07), (0x0C, 0x0F), (0x30, 0x00), (0x30, 0x02)] {
        assert!(
            matches!(one(2, asc, ascq), Ok(Present)),
            "{asc:02x}/{ascq:02x}"
        );
    }
    for (asc, ascq) in [(0x04, 0x00), (0x04, 0x01), (0x04, 0x03), (0x04, 0x09)] {
        assert!(
            matches!(one(2, asc, ascq), Ok(Settling)),
            "{asc:02x}/{ascq:02x}"
        );
    }
    assert!(matches!(one(2, 0x3E, 0x00), Ok(Settling)), "3E/00");
    // A cleaning cartridge (30/03) or cleaning failure (30/07) is nothing to
    // rip and does not resolve like a spin-up: no disc.
    for ascq in [0x03, 0x07] {
        assert!(matches!(one(2, 0x30, ascq), Ok(Absent)), "30/{ascq:02x}");
    }
    assert!(one(3, 0x11, 0).is_err(), "not a NOT READY key");
}

// A transport failure carries no sense: it must bubble up on the first
// poll (a wedged drive is not "no disc"), never map to Absent or retry.
#[test]
fn tur_disc_presence_bubbles_a_no_sense_transport_failure() {
    let wedge = Err(Error::ScsiError {
        opcode: SCSI_TEST_UNIT_READY,
        status: SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    });
    let (r, calls) = presence(vec![wedge]);
    assert!(r.is_err(), "no-sense failure must be an error, got {r:?}");
    assert_eq!(calls, 1, "a wedge is not retried");
}

// A UNIT ATTENTION says nothing about the medium, and several can be queued
// (06/29 reset, then 06/28 medium change): re-issue TUR up to 4 times.
#[test]
fn tur_disc_presence_retries_queued_unit_attentions() {
    use DiscPresence::*;
    let ua = |asc| tur_err(6, asc, 0);
    assert!(matches!(
        presence(vec![ua(0x28), tur_err(2, 0x3A, 0)]),
        (Ok(Absent), 2)
    ));
    assert!(matches!(
        presence(vec![ua(0x29), ua(0x28), Ok(())]),
        (Ok(Present), 3)
    ));
    assert!(matches!(
        presence(vec![ua(0x28), tur_err(2, 4, 1)]),
        (Ok(Settling), 2)
    ));
    let five = (0..5).map(|_| ua(0x28)).collect();
    assert!(
        matches!(presence(five), (Err(_), 5)),
        "attentions never clear"
    );
}

// drive_has_disc keeps its bool: Settling reads as "still there", so a poll
// loop never tears down a session while a mounted disc re-spins.
#[test]
fn drive_has_disc_maps_settling_to_present() {
    assert!(DiscPresence::Present.has_disc());
    assert!(DiscPresence::Settling.has_disc());
    assert!(!DiscPresence::Absent.has_disc());
}

// Without sysfs, an sg node stays listed unless it is definitively not an
// optical drive or not there: a wedged or busy drive must read as present
// but unresponsive (autorip's "firmware unresponsive"), not as unplugged.
#[test]
fn unfiltered_sg_node_is_dropped_only_when_definitively_not_a_drive() {
    assert!(keep_unfiltered_node(NodeProbe::Optical));
    assert!(keep_unfiltered_node(NodeProbe::Unresponsive));
    assert!(!keep_unfiltered_node(NodeProbe::NotOptical));
    assert!(!keep_unfiltered_node(NodeProbe::Absent));
    // Without sysfs a non-root user sees EACCES on every root-owned sg node
    // (disks, tapes): unprovable as optical, so not listed.
    assert!(!keep_unfiltered_node(NodeProbe::Denied));
}

// How a failed open(2) of an unfiltered sg node is classified.
#[cfg(unix)]
#[test]
fn unfiltered_open_errno_classification() {
    use NodeProbe::*;
    let probe = |e| open_failure_probe(Some(e));
    assert!(matches!(probe(libc::ENOENT), Absent));
    assert!(matches!(probe(libc::ENXIO), Absent));
    assert!(matches!(probe(libc::ENODEV), Absent));
    assert!(matches!(probe(libc::EACCES), Denied));
    assert!(matches!(probe(libc::EPERM), Denied));
    assert!(matches!(probe(libc::EBUSY), Unresponsive));
    assert!(matches!(open_failure_probe(None), Unresponsive));
}

// Drop unlocks the tray only for a transport that issued a PREVENT; the
// CDB decode is what tracks that.
#[test]
fn prevent_allow_request_decodes_only_1eh() {
    // SPC-4 §6.13 PREVENT field, bits 1:0: 00 allow, 01 prevent,
    // 10 persistent allow, 11 persistent prevent. Upper bits are reserved.
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x00, 0]), Some(0b00));
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x01, 0]), Some(0b01));
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x02, 0]), Some(0b10));
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0x03, 0]), Some(0b11));
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0, 0, 0xFD, 0]), Some(0b01));
    assert_eq!(prevent_allow_request(&[0x12, 0, 0, 0, 0x01, 0]), None);
    assert_eq!(prevent_allow_request(&[0x1E, 0, 0]), None, "short CDB");
}

// AlignmentMask is adapter-reported: it must be 2^n-1 and bounded, or a bogus
// 0xFFFF_FFFF sizes every bounce buffer at data.len() + 4 GiB.
#[test]
fn alignment_mask_is_validated_and_capped() {
    assert_eq!(sanitize_alignment_mask(0), 0);
    assert_eq!(sanitize_alignment_mask(0x3), 0x3);
    assert_eq!(sanitize_alignment_mask(0x1FF), 0x1FF);
    assert_eq!(sanitize_alignment_mask(0x5), 0x7, "not 2^n-1: widen");
    assert_eq!(sanitize_alignment_mask(0xFFFF_FFFF), MAX_ALIGNMENT_MASK);
    assert_eq!(sanitize_alignment_mask(0x0010_0000), MAX_ALIGNMENT_MASK);
}

// SPTI's TimeOutValue is whole seconds, rounded up, never 0 — and a huge
// timeout must saturate, not wrap to 1 s (or panic in debug).
#[test]
fn spti_timeout_rounds_up_without_overflow() {
    assert_eq!(spti_timeout_secs(0), 60);
    assert_eq!(spti_timeout_secs(1_500), 2);
    assert_eq!(spti_timeout_secs(5_000), 5);
    assert_eq!(spti_timeout_secs(u32::MAX), u32::MAX.div_ceil(1000));
}

// A GOOD reply that moved no data must not parse as a blank identity.
#[test]
fn inquiry_rejects_a_short_reply_and_parses_a_full_one() {
    struct Inq(usize);
    impl ScsiTransport for Inq {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            let mut reply = [0u8; 36];
            reply[0] = 0x05;
            reply[8..32].fill(b' ');
            reply[8..16].copy_from_slice(b"HL-DT-ST");
            reply[16..27].copy_from_slice(b"BD-RE BU40N");
            reply[32..36].copy_from_slice(b"1.03");
            data[..36].copy_from_slice(&reply);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: self.0,
                sense: [0u8; 32],
            })
        }
    }
    assert!(inquiry(&mut Inq(0)).is_err(), "zero-byte reply");
    assert!(inquiry(&mut Inq(35)).is_err(), "truncated before firmware");
    let r = inquiry(&mut Inq(36)).unwrap();
    assert_eq!(
        (r.vendor_id.as_str(), r.model.as_str()),
        ("HL-DT-ST", "BD-RE BU40N")
    );
    assert_eq!(r.firmware, "1.03");
}

// Feature 010Ch is an 8-byte header plus a 20-byte descriptor (Additional
// Length 10h): the allocation length must cover all 28 bytes.
#[test]
fn get_config_010c_returns_the_whole_descriptor() {
    struct Fw(Vec<u8>);
    impl ScsiTransport for Fw {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            let alloc = usize::from(u16::from_be_bytes([cdb[7], cdb[8]]));
            let mut reply = vec![0, 0, 0, 24, 0, 0, 0, 0, 0x01, 0x0C, 0x00, 0x10];
            reply.extend_from_slice(b"202101311259\0\0\0\0");
            let n = reply.len().min(alloc).min(data.len());
            data[..n].copy_from_slice(&reply[..n]);
            self.0 = cdb.to_vec();
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            })
        }
    }
    let mut t = Fw(Vec::new());
    let v = get_config_010c(&mut t).unwrap();
    assert_eq!(v.len(), 28, "header + full 010Ch descriptor");
    assert_eq!(&v[12..24], b"202101311259", "date through the minute");
    // MMC-6 §6.6: RT=10b (one feature), Starting Feature 010Ch, Allocation Length 28.
    assert_eq!(
        t.0,
        [SCSI_GET_CONFIGURATION, 0x02, 0x01, 0x0C, 0, 0, 0, 0, 28, 0]
    );
}

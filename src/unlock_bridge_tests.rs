use super::*;
use freemkv_unlock::scsi::ScsiTransport as _; // brings `execute` into scope

/// A fake libfreemkv transport whose `execute` always fails with a
/// freshly-built error (`crate::error::Error` isn't `Clone` — `io::Error`).
struct ErrTransport<F>(F);
impl<F: FnMut() -> crate::error::Error + Send> crate::scsi::ScsiTransport for ErrTransport<F> {
    fn execute(
        &mut self,
        _cdb: &[u8],
        _dir: crate::scsi::DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> std::result::Result<crate::scsi::ScsiResult, crate::error::Error> {
        Err((self.0)())
    }
}

fn adapt(e: impl FnMut() -> crate::error::Error + Send + 'static) -> fu::scsi::ScsiError {
    let mut d = crate::drive::Drive::from_transport_for_test(Box::new(ErrTransport(e)));
    let mut adapter = ScsiAdapter::new(&mut d);
    let mut buf = [0u8; 0];
    adapter
        .execute(&[0u8; 12], fu::scsi::DataDirection::None, &mut buf, 1_000)
        .expect_err("error path")
}

/// The user-facing unlocker matrix must be DERIVED from the real unlocker
/// instances, never a hand-maintained list that can silently drift. Pin the
/// dispatch order (firmware set, then disc set) and cross-check that every
/// name comes from an actual `.name()`.
#[test]
fn unlocker_names_are_derived_from_the_real_unlockers() {
    assert_eq!(
        unlocker_names(),
        vec!["freemkv", "LD", "Renesas", "AACS", "DVD"],
        "the matrix is the firmware set then the disc set, in dispatch order"
    );
    // Split point: the leading names are exactly the firmware set.
    let fw: Vec<&str> = firmware_unlockers().iter().map(|u| u.name()).collect();
    assert_eq!(fw, vec!["freemkv", "LD", "Renesas"]);
}

// `is_drive_unlocker` classifies a matched unlocker name against the real
// firmware set: firmware unlockers unlock AT THE DRIVE (true), disc-keyed
// ones do not (false), and an empty/unknown name is not a drive unlock.
#[test]
fn is_drive_unlocker_credits_only_the_firmware_set() {
    for u in firmware_unlockers() {
        assert!(
            is_drive_unlocker(u.name()),
            "{} is a firmware/drive unlocker",
            u.name()
        );
    }
    for u in disc_unlockers(Vec::new()) {
        assert!(
            !is_drive_unlocker(u.name()),
            "{} is disc-keyed, not a drive unlock",
            u.name()
        );
    }
    assert!(!is_drive_unlocker(""), "no match is not a drive unlock");
}

/// A CHECK CONDITION carrying sense crosses the seam with status + parsed
/// sense intact, so the unlock crate's ILLEGAL_REQUEST wedge guard can fire.
#[test]
fn check_condition_preserves_status_and_sense() {
    let err = adapt(|| crate::error::Error::ScsiError {
        opcode: 0xA3,
        status: 0x02,
        sense: Some(crate::scsi::ScsiSense {
            sense_key: 0x05,
            asc: 0x24,
            ascq: 0x00,
        }),
    });
    assert_eq!(err.status, 0x02);
    let sense = err.sense.expect("sense preserved");
    assert!(fu::scsi::ScsiSense::from_buf(&sense).is_illegal_request());
}

/// A `DiscRead` carrying a real status + sense is a drive answer like any CHECK CONDITION:
/// it must not collapse to a dead bus.
#[test]
fn disc_read_preserves_status_and_sense() {
    let err = adapt(|| crate::error::Error::DiscRead {
        sector: 7,
        status: Some(0x02),
        sense: Some(crate::scsi::ScsiSense {
            sense_key: 0x05,
            asc: 0x24,
            ascq: 0x00,
        }),
    });
    assert_eq!(err.status, 0x02);
    let sense = err.sense.expect("sense preserved");
    assert!(fu::scsi::ScsiSense::from_buf(&sense).is_illegal_request());
}

/// Each host-cert field lands in its own slot: v1 and v2 material never swap.
#[test]
fn host_certs_map_field_for_field() {
    let cert = crate::aacs::types::HostCert {
        private_key: [1; 20],
        certificate: vec![2; 92],
        private_key_v2: Some([3; 32]),
        certificate_v2: Some(vec![4; 8]),
    };
    let mapped = map_host_certs(std::slice::from_ref(&cert));
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].private_key, [1; 20]);
    assert_eq!(mapped[0].certificate, vec![2; 92]);
    assert_eq!(mapped[0].private_key_v2, Some([3; 32]));
    assert_eq!(mapped[0].certificate_v2, Some(vec![4; 8]));
}

/// A drive-tagged transport fault (status 0xFF) crosses unchanged.
#[test]
fn scsi_transport_fault_maps_unchanged() {
    let err = adapt(|| crate::error::Error::ScsiError {
        opcode: 0,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    });
    assert_eq!(err.status, 0xFF);
    assert!(err.sense.is_none());
}

/// A non-SCSI IO fault (ioctl SG_IO == -1: ENODEV/EIO) is a dead bus —
/// surfaced as 0xFF so the unlock crate bails instead of hammering it.
#[test]
fn io_error_maps_to_transport_failure() {
    let err = adapt(|| crate::error::Error::IoError {
        source: std::io::Error::from(std::io::ErrorKind::NotConnected),
    });
    assert_eq!(err.status, crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE);
    assert!(err.sense.is_none());
}

/// Device-gone (fd closed) likewise maps to the transport-failure status.
#[test]
fn device_not_found_maps_to_transport_failure() {
    let err = adapt(|| crate::error::Error::DeviceNotFound {
        path: "/dev/sg9".into(),
    });
    assert_eq!(err.status, 0xFF);
    assert!(err.sense.is_none());
}

/// LD14: `begin_critical` after a cancel is refused (0xFF); a CDB inside a
/// critical section entered before the cancel still runs; `pause` is refused on a
/// cancel; the clean-up path admits the ALLOW for a tray this Drive locked.
#[test]
fn adapter_critical_and_pause() {
    use crate::test_util::FakeTransport;
    let tur = [0u8; 6];
    let h = crate::halt::Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t.watch(&h).allow_after_cancel(|c| c[0] == 0x00);
    let mut d = crate::drive::Drive::from_transport_with(Box::new(t), &h);
    d.lock_tray();
    let mut a = ScsiAdapter::new(&mut d);
    a.begin_critical().expect("not cancelled yet");
    h.cancel();
    let mut buf = [0u8; 0];
    assert!(
        a.execute(&tur, fu::scsi::DataDirection::None, &mut buf, 5_000)
            .is_ok()
    );
    a.end_critical();
    let e = a
        .execute(&tur, fu::scsi::DataDirection::None, &mut buf, 5_000)
        .expect_err("refused outside the span");
    assert_eq!((e.status, e.sense), (0xFF, None));
    let e = a.begin_critical().expect_err("no span after the cancel");
    assert_eq!(e.status, 0xFF);
    let t0 = std::time::Instant::now();
    let e = a
        .pause(std::time::Duration::from_secs(10))
        .expect_err("pause refused");
    assert_eq!(e.status, 0xFF);
    assert!(t0.elapsed() < std::time::Duration::from_secs(1));
    let allow = [0x1E, 0, 0, 0, 0, 0];
    assert!(
        a.execute_cleanup(&allow, fu::scsi::DataDirection::None, &mut buf, 5_000)
            .is_ok()
    );
    assert_eq!(fake.count(|c| c[0] == 0x00), 1, "one TUR, inside the span");
    assert_eq!(fake.count(|c| c == allow), 1, "one ALLOW");
}

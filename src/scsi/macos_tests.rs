use super::{
    K_MAX_CDB_SIZE, MacScsiTransport, OPEN, SHIM_CANCELLED, ShimDriveInfo, bsd_name_of,
    cancel_byte, cstr_to_str, device_path_for_selector, disc_presence, drive_info_from_shim,
    map_shim_open_error,
};
use crate::error::Error;
use crate::halt::{Halt, WAIT_SLICE};
use crate::scsi::{DataDirection, ScsiTransport};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

// Serializes the tests that read or take the process-global OPEN bit and shim handle.
static SHIM_GLOBALS: Mutex<()> = Mutex::new(());

fn shim_globals() -> std::sync::MutexGuard<'static, ()> {
    SHIM_GLOBALS.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn bsd_name_strips_dev_and_raw_dev_prefixes() {
    assert_eq!(bsd_name_of(Path::new("/dev/disk4")).unwrap(), "disk4");
    assert_eq!(bsd_name_of(Path::new("/dev/rdisk4")).unwrap(), "disk4");
    assert_eq!(bsd_name_of(Path::new("disk4")).unwrap(), "disk4");
    assert_eq!(
        bsd_name_of(Path::new("ioreg:4294967295")).unwrap(),
        "ioreg:4294967295"
    );
}

#[test]
fn list_path_preserves_registry_selector_and_uses_dev_for_bsd_name() {
    assert_eq!(device_path_for_selector("disk4"), "/dev/disk4");
    assert_eq!(device_path_for_selector("disk0"), "/dev/disk0");
    assert_eq!(
        device_path_for_selector("ioreg:4294967295"),
        "ioreg:4294967295"
    );
    // Registry IDs are uint64 values and the shim emits the complete
    // decimal value into its fixed-width selector field.
    assert_eq!(
        device_path_for_selector("ioreg:18446744073709551615"),
        "ioreg:18446744073709551615"
    );
}

fn shim_drive(selector: &[u8], vendor: &[u8], model: &[u8], firmware: &[u8]) -> ShimDriveInfo {
    let mut info = ShimDriveInfo {
        device_selector: [0; 32],
        vendor: [0; 32],
        model: [0; 48],
        firmware: [0; 16],
    };
    info.device_selector[..selector.len()].copy_from_slice(selector);
    info.vendor[..vendor.len()].copy_from_slice(vendor);
    info.model[..model.len()].copy_from_slice(model);
    info.firmware[..firmware.len()].copy_from_slice(firmware);
    info
}

#[test]
fn shim_drive_record_maps_bsd_selector_and_identity_fields() {
    let info = shim_drive(b"disk4", b"HL-DT-ST", b"BD-RE BU40N", b"1.03");
    let mapped = drive_info_from_shim(&info).expect("non-empty selector");
    assert_eq!(mapped.path, "/dev/disk4");
    assert_eq!(mapped.vendor, "HL-DT-ST");
    assert_eq!(mapped.model, "BD-RE BU40N");
    assert_eq!(mapped.firmware, "1.03");
}

#[test]
fn shim_drive_record_keeps_empty_tray_registry_selector() {
    let info = shim_drive(
        b"ioreg:18446744073709551615",
        b"HL-DT-ST",
        b"BD-RE BU40N",
        b"1.03",
    );
    let mapped = drive_info_from_shim(&info).expect("registry selector");
    assert_eq!(mapped.path, "ioreg:18446744073709551615");
    assert_eq!(mapped.vendor, "HL-DT-ST");
    assert_eq!(mapped.model, "BD-RE BU40N");
    assert_eq!(mapped.firmware, "1.03");
}

#[test]
fn shim_drive_record_with_empty_selector_is_skipped() {
    let info = shim_drive(b"", b"HL-DT-ST", b"BD-RE BU40N", b"1.03");
    assert!(drive_info_from_shim(&info).is_none());
}

// Shim fixed-width fields are NUL-terminated C strings with garbage
// after the terminator; cstr_to_str must stop at the first NUL, not
// read the full width, and never panic on a non-UTF-8 tail.
#[test]
fn cstr_to_str_stops_at_first_nul() {
    let mut bytes = [0xAAu8; 8]; // 0xAA is not valid UTF-8 on its own
    bytes[..5].copy_from_slice(b"BU40N");
    bytes[5] = 0; // terminator; bytes[6..8] remain 0xAA "garbage"
    assert_eq!(cstr_to_str(&bytes), "BU40N");
}

#[test]
fn cstr_to_str_no_nul_uses_whole_buffer() {
    let bytes = *b"HL-DT-ST";
    assert_eq!(cstr_to_str(&bytes), "HL-DT-ST");
}

#[test]
fn cstr_to_str_invalid_utf8_returns_empty_not_panic() {
    let bytes = [0xFFu8, 0xFE, 0x00, 0x00];
    assert_eq!(cstr_to_str(&bytes), "");
}

// Regression test: the presence probe used to open a FULL exclusive transport (force-unmount +
// 500ms sleep) on every poll tick. Checks it now answers Absent fast with no lock held.
#[test]
fn presence_probe_does_not_open_a_transport() {
    let path = Path::new("/dev/freemkv-no-such-device");
    let _globals = shim_globals();
    // Snapshot rather than assume `false`: OPEN is a process-global bit any
    // concurrent test's live transport can hold, so compare the delta — it
    // scopes the check to this call, can't flake, still catches a held lock.
    let open_before = OPEN.load(Ordering::Acquire);
    let t0 = std::time::Instant::now();
    let r = disc_presence(path);
    let elapsed = t0.elapsed();

    assert!(
        matches!(r, Ok(crate::scsi::DiscPresence::Absent)),
        "a device with no IOMedia must report absent media, got {r:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(250),
        "probe took {elapsed:?}: the transport path's unconditional 500 ms \
             post-unmount sleep means this budget can only be met without it"
    );
    assert_eq!(
        OPEN.load(Ordering::Acquire),
        open_before,
        "the probe must not change the exclusive-transport lock state"
    );
}

// Every negative shim_open_exclusive sentinel must map to its own typed
// error, not collapse into DeviceNotFound: -2..=-4 (IOKit plugin chain)
// and -5 (exclusive access held elsewhere) are the two at risk.
#[test]
fn map_shim_open_error_distinguishes_every_sentinel() {
    for rc in [-2, -3, -4] {
        match map_shim_open_error(rc, "disk4".into(), 0xE00002C7) {
            Error::IoKitPluginFailed { path, kr } => {
                assert_eq!(path, "disk4");
                assert_eq!(kr, 0xE00002C7, "the real IOReturn is carried, not 0");
            }
            other => panic!("rc={rc}: expected IoKitPluginFailed, got {other:?}"),
        }
    }
    match map_shim_open_error(-5, "disk4".into(), 0xE00002C5) {
        Error::DeviceLocked { path, kr } => {
            assert_eq!(path, "disk4");
            assert_eq!(kr, 0xE00002C5);
        }
        other => panic!("expected DeviceLocked, got {other:?}"),
    }
    for rc in [-1, -7, i32::MIN] {
        match map_shim_open_error(rc, "disk4".into(), 0) {
            Error::DeviceNotFound { path } => assert_eq!(path, "disk4"),
            other => panic!("rc={rc}: expected DeviceNotFound, got {other:?}"),
        }
    }
}

// A CDB longer than K_MAX_CDB_SIZE must be rejected with
// InvalidCdbLength before the shim is called; exercises the real
// guard MacScsiTransport::execute uses, without opening an IOKit handle.
#[test]
fn oversized_cdb_returns_invalid_cdb_length() {
    // Build a CDB one byte over the limit.
    let long_cdb = [0u8; K_MAX_CDB_SIZE + 1];
    match crate::scsi::checked_cdb_len(&long_cdb, K_MAX_CDB_SIZE) {
        Err(Error::InvalidCdbLength { len, max }) => {
            assert_eq!(len, K_MAX_CDB_SIZE + 1);
            assert_eq!(max, K_MAX_CDB_SIZE);
        }
        other => panic!("expected InvalidCdbLength, got {other:?}"),
    }
}

/// `execute` itself rejects an over-length CDB; the fake device would accept
/// it if the guard were bypassed and the length truncated.
#[test]
fn execute_rejects_an_oversized_cdb_before_the_shim() {
    let _globals = FakeDeviceGuard(shim_globals());
    assert!(
        !OPEN.swap(true, Ordering::Acquire),
        "OPEN held outside SHIM_GLOBALS"
    );
    assert_eq!(unsafe { shim_selftest_install_fake_device() }, 0);
    let mut transport = MacScsiTransport {
        last_progress: None,
    };
    for len in [K_MAX_CDB_SIZE + 1, 256 + 6] {
        let r = transport.execute(&vec![0u8; len], DataDirection::None, &mut [], 1_000);
        assert!(
            matches!(r, Err(Error::InvalidCdbLength { .. })),
            "{len}-byte CDB: {r:?}"
        );
    }
}

/// A CDB exactly at the limit must not trigger the guard, and its length
/// reaches the shim verbatim.
#[test]
fn max_length_cdb_does_not_trigger_guard() {
    let cdb = [0u8; K_MAX_CDB_SIZE];
    assert_eq!(
        crate::scsi::checked_cdb_len(&cdb, K_MAX_CDB_SIZE).ok(),
        Some(K_MAX_CDB_SIZE as u8),
        "CDB of exactly K_MAX_CDB_SIZE should not trigger guard"
    );
}

// ── shim_selftest (stop design §5.1 LM1; qa release-tests on macOS, D6) ──

unsafe extern "C" {
    fn shim_selftest_wait_slice_ms() -> i32;
    fn shim_selftest_sleep(ms: u32, cancel: *const u8) -> i32;
    fn shim_selftest_sem_wait(ms: u32, signalled: i32, cancel: *const u8) -> i32;
    fn shim_selftest_run_and_reap(
        path: *const u8,
        arg: *const u8,
        budget_ms: u32,
        cancel: *const u8,
        pid_out: *mut i32,
    ) -> i32;
    fn shim_selftest_reaped(pid: i32) -> i32;
    fn shim_selftest_install_fake_device() -> i32;
    fn shim_selftest_last_timeout_ms() -> u32;
    fn shim_selftest_set_execute(kr: i32, status: u8, count: u64, sense: *const u8);
    fn shim_selftest_fake_optical(on: i32);
}

/// A wait that is never cancelled would run this long; every cancelled wait must end far sooner.
const LONG_MS: u32 = 10_000;
/// Cancel lands this long after the wait starts.
const CANCEL_AFTER: Duration = Duration::from_millis(50);
/// The unmount child is spawned well before a cancel this late, even on a loaded runner.
const SPAWN_CANCEL_AFTER: Duration = Duration::from_millis(500);
/// A cancelled wait returns within this of the cancel: a few 20 ms slices plus a kill and
/// reap, with a 5× margin over a 200 ms scheduler stall, and far under `LONG_MS` (§3.2).
const WAKE_BOUND: Duration = Duration::from_secs(1);

// Run `wait` against a token cancelled CANCEL_AFTER in; returns its result and the time
// from the cancel to its return.
fn cancel_during(wait: impl FnOnce(*const u8) -> i32) -> (i32, Duration) {
    cancel_after(CANCEL_AFTER, wait)
}

// `cancel_during` with the cancel landing `after` into the wait.
fn cancel_after(after: Duration, wait: impl FnOnce(*const u8) -> i32) -> (i32, Duration) {
    let halt = Halt::new();
    let canceller = {
        let halt = halt.clone();
        thread::spawn(move || {
            thread::sleep(after);
            let at = Instant::now();
            halt.cancel();
            at
        })
    };
    let rc = wait(cancel_byte(&halt));
    let returned = Instant::now();
    let cancelled_at = canceller.join().expect("canceller thread");
    (rc, returned.saturating_duration_since(cancelled_at))
}

/// Per design §2.9 M2 ("every wait is sliced at ≤ 20 ms") and §2.1 `WAIT_SLICE`: the shim's
/// slice is the library's. Guard; do not change without a design citation.
#[test]
fn shim_selftest_wait_slice_is_the_library_wait_slice() {
    let slice = unsafe { shim_selftest_wait_slice_ms() };
    assert!(slice <= 20, "shim slice {slice} ms exceeds the 20 ms bound");
    assert_eq!(Duration::from_millis(slice as u64), WAIT_SLICE);
}

/// §2.9 M2: the settle and ObtainExclusiveAccess retry sleeps end within a slice of a cancel.
#[test]
fn shim_selftest_cancel_ends_the_settle_sleep() {
    let (rc, wake) = cancel_during(|c| unsafe { shim_selftest_sleep(LONG_MS, c) });
    assert_eq!(rc, SHIM_CANCELLED);
    assert!(
        wake <= WAKE_BOUND,
        "settle sleep woke {wake:?} after the cancel"
    );

    let halt = Halt::new();
    halt.cancel();
    let t0 = Instant::now();
    let rc = unsafe { shim_selftest_sleep(LONG_MS, cancel_byte(&halt)) };
    assert_eq!(
        rc, SHIM_CANCELLED,
        "a token cancelled before the wait ends it at once"
    );
    assert!(t0.elapsed() <= WAKE_BOUND);

    let t0 = Instant::now();
    let rc = unsafe { shim_selftest_sleep(60, cancel_byte(&Halt::new())) };
    assert_eq!(rc, 0, "an uncancelled sleep runs out normally");
    assert!(t0.elapsed() >= Duration::from_millis(60));
}

/// §2.9 M2: the DiskArbitration claim wait (5 s) is sliced and ends on a cancel.
/// Apple dispatch/semaphore.h: "Returns zero on success, or non-zero if the timeout occurred."
#[test]
fn shim_selftest_cancel_ends_the_da_claim_wait() {
    let (rc, wake) = cancel_during(|c| unsafe { shim_selftest_sem_wait(LONG_MS, 0, c) });
    assert_eq!(rc, SHIM_CANCELLED);
    assert!(
        wake <= WAKE_BOUND,
        "DA claim wait woke {wake:?} after the cancel"
    );

    let never = Halt::new();
    let rc = unsafe { shim_selftest_sem_wait(60, 0, cancel_byte(&never)) };
    assert_eq!(rc, 1, "an unsignalled, uncancelled wait times out");
    let t0 = Instant::now();
    let rc = unsafe { shim_selftest_sem_wait(LONG_MS, 1, cancel_byte(&never)) };
    assert_eq!(rc, 0, "a signalled claim is taken");
    assert!(t0.elapsed() <= WAKE_BOUND);
}

/// §2.9 M2: "On cancel, diskutil is killed and reaped". The child is `sleep` standing in for
/// a wedged `diskutil unmountDisk`, run through the same spawn-and-reap path.
#[test]
fn shim_selftest_cancel_kills_and_reaps_the_unmount() {
    let mut pid = 0;
    let (rc, wake) = cancel_after(SPAWN_CANCEL_AFTER, |c| unsafe {
        shim_selftest_run_and_reap(
            c"/bin/sleep".as_ptr().cast(),
            c"30".as_ptr().cast(),
            LONG_MS,
            c,
            &mut pid,
        )
    });
    assert_eq!(rc, SHIM_CANCELLED);
    assert!(
        wake <= WAKE_BOUND,
        "unmount wait woke {wake:?} after the cancel"
    );
    assert!(pid > 0, "the child was spawned");
    // wait(2) ECHILD: "The process specified by pid does not exist or is not a child of
    // the calling process" — the shim reaped it, leaving no zombie.
    assert_eq!(
        unsafe { shim_selftest_reaped(pid) },
        1,
        "child {pid} not reaped"
    );
}

/// The unmount budget still kills and reaps a wedged child with no cancel, and a child
/// that exits on its own is reaped as a normal exit.
#[test]
fn shim_selftest_unmount_budget_and_normal_exit_reap() {
    let never = Halt::new();
    let mut pid = 0;
    let rc = unsafe {
        shim_selftest_run_and_reap(
            c"/bin/sleep".as_ptr().cast(),
            c"30".as_ptr().cast(),
            60,
            cancel_byte(&never),
            &mut pid,
        )
    };
    assert_eq!(rc, 1, "a wedged child is killed when the budget is spent");
    assert_eq!(
        unsafe { shim_selftest_reaped(pid) },
        1,
        "child {pid} not reaped"
    );

    let rc = unsafe {
        shim_selftest_run_and_reap(
            c"/bin/sleep".as_ptr().cast(),
            c"0".as_ptr().cast(),
            LONG_MS,
            cancel_byte(&never),
            &mut pid,
        )
    };
    assert_eq!(rc, 0, "a child that exits is a normal exit");
    assert_eq!(
        unsafe { shim_selftest_reaped(pid) },
        1,
        "child {pid} not reaped"
    );
}

/// §2.9 M2: `SHIM_CANCELLED` → `Halted`. A cancelled open is `Halted` before any side
/// effect, and the single-instance lock is released.
#[test]
fn shim_selftest_cancelled_open_is_halted() {
    assert!(matches!(
        map_shim_open_error(SHIM_CANCELLED, "disk4".into(), 0),
        Error::Halted
    ));

    let _globals = shim_globals();
    let halt = Halt::new();
    halt.cancel();
    let t0 = Instant::now();
    let r = MacScsiTransport::open(Path::new("/dev/freemkv-no-such-device"), &halt);
    assert!(
        matches!(r, Err(Error::Halted)),
        "expected Halted, got {:?}",
        r.err()
    );
    assert!(
        t0.elapsed() <= WAKE_BOUND,
        "cancelled open took {:?}",
        t0.elapsed()
    );
    assert!(
        !OPEN.load(Ordering::Acquire),
        "a cancelled open left OPEN held"
    );
}

/// A selector that is not an optical drive is refused before anything is unmounted: the
/// open fails at once (no 500 ms settle sleep, no diskutil) and releases the lock.
#[test]
fn open_of_a_non_optical_selector_is_refused_before_any_unmount() {
    let _globals = shim_globals();
    let t0 = Instant::now();
    let r = MacScsiTransport::open(Path::new("/dev/freemkv-no-such-device"), &Halt::new());
    assert!(
        matches!(r, Err(Error::DeviceNotFound { .. })),
        "expected DeviceNotFound, got {:?}",
        r.err()
    );
    assert!(
        t0.elapsed() < Duration::from_millis(450),
        "refusal took {:?}: the post-unmount settle sleep ran",
        t0.elapsed()
    );
    assert!(
        !OPEN.load(Ordering::Acquire),
        "a failed open left OPEN held"
    );
}

/// §2.9 M2 end to end: a resolved drive's open hands its token to the unmount and 500 ms
/// settle waits. The unmount of a missing disk ends at once, so the cancel lands in the settle.
#[test]
fn shim_selftest_cancel_mid_open_is_halted() {
    struct FakeOptical;
    impl Drop for FakeOptical {
        fn drop(&mut self) {
            unsafe { shim_selftest_fake_optical(0) };
        }
    }
    let _globals = shim_globals();
    unsafe { shim_selftest_fake_optical(1) };
    let _fake = FakeOptical;
    let (r, wake) = {
        let halt = Halt::new();
        let canceller = {
            let halt = halt.clone();
            thread::spawn(move || {
                thread::sleep(CANCEL_AFTER);
                let at = Instant::now();
                halt.cancel();
                at
            })
        };
        let r = MacScsiTransport::open(Path::new("/dev/freemkv-no-such-device"), &halt);
        let returned = Instant::now();
        let cancelled_at = canceller.join().expect("canceller thread");
        (r, returned.saturating_duration_since(cancelled_at))
    };
    assert!(
        matches!(r, Err(Error::Halted)),
        "expected Halted, got {:?}",
        r.err()
    );
    assert!(
        wake <= WAKE_BOUND,
        "open returned {wake:?} after the cancel"
    );
    assert!(
        !OPEN.load(Ordering::Acquire),
        "a cancelled open left OPEN held"
    );
}

/// §2.9 M1: "`timeout_ms` is passed through to the shim", reaching the task unchanged.
/// Apple SCSITaskLib.h SetTimeoutDuration: "The timeout duration is counted in milliseconds."
#[test]
fn shim_selftest_timeout_ms_is_passed_through() {
    let _globals = shim_globals();
    assert!(
        !OPEN.swap(true, Ordering::Acquire),
        "OPEN held outside SHIM_GLOBALS"
    );
    assert_eq!(unsafe { shim_selftest_install_fake_device() }, 0);
    let mut transport = MacScsiTransport {
        last_progress: None,
    };
    for timeout_ms in [1, 5_000, 10_000, 12_345, 60_000] {
        let r = transport.execute(&[0u8; 6], DataDirection::None, &mut [], timeout_ms);
        assert!(r.is_ok(), "fake task failed: {:?}", r.err());
        assert_eq!(unsafe { shim_selftest_last_timeout_ms() }, timeout_ms);
    }
    // SCSITaskLib.h SetTimeoutDuration: "A value of zero is equivalent to "Wait Forever"",
    // so 0 must never reach the task; it becomes Linux SG_IO's 60 s default.
    let r = transport.execute(&[0u8; 6], DataDirection::None, &mut [], 0);
    assert!(r.is_ok(), "fake task failed: {:?}", r.err());
    assert_eq!(unsafe { shim_selftest_last_timeout_ms() }, 60_000);
    drop(transport);
    assert!(!OPEN.load(Ordering::Acquire), "drop released OPEN");
}

// Holds SHIM_GLOBALS for a test that scripts the fake device, and puts the knob back to
// its defaults on drop (panic included) before the lock is released.
struct FakeDeviceGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);
impl Drop for FakeDeviceGuard {
    fn drop(&mut self) {
        unsafe { shim_selftest_set_execute(0, 0, 0, std::ptr::null()) };
    }
}

// Runs `execute` on the fake device after `shim_selftest_set_execute(kr, status, count, sense)`.
fn execute_on_fake(
    kr: i32,
    status: u8,
    count: u64,
    sense: Option<&[u8; 32]>,
    data: &mut [u8],
) -> (
    MacScsiTransport,
    crate::error::Result<crate::scsi::ScsiResult>,
) {
    assert!(
        !OPEN.swap(true, Ordering::Acquire),
        "OPEN held outside SHIM_GLOBALS"
    );
    assert_eq!(unsafe { shim_selftest_install_fake_device() }, 0);
    unsafe {
        shim_selftest_set_execute(
            kr,
            status,
            count,
            sense.map_or(std::ptr::null(), |s| s.as_ptr()),
        )
    };
    let mut transport = MacScsiTransport {
        last_progress: None,
    };
    let r = transport.execute(&[0u8; 6], DataDirection::FromDevice, data, 1_000);
    (transport, r)
}

/// CHECK CONDITION is an error carrying the parsed sense and its progress indication;
/// any other nonzero status is an error with no sense.
#[test]
fn shim_selftest_check_condition_and_other_statuses_are_errors() {
    let _globals = FakeDeviceGuard(shim_globals());
    // Fixed format, NOT READY / 04h 01h (becoming ready), SKSV set, progress 0x1234.
    let mut sense = [0u8; 32];
    sense[0] = 0x70;
    sense[2] = 0x02;
    sense[12] = 0x04;
    sense[13] = 0x01;
    sense[15] = 0x80;
    sense[16..18].copy_from_slice(&0x1234u16.to_be_bytes());
    let (t, r) = execute_on_fake(0, 0x02, 0, Some(&sense), &mut [0u8; 8]);
    match r {
        Err(Error::ScsiError {
            status: 0x02,
            sense: Some(s),
            ..
        }) => assert_eq!((s.sense_key, s.asc, s.ascq), (2, 0x04, 0x01)),
        other => panic!("expected CHECK CONDITION with sense, got {other:?}"),
    }
    assert_eq!(t.last_sense_progress(), Some(0x1234));
    drop(t);

    // BUSY (08h) carries no sense on the wire, and no progress.
    let (t, r) = execute_on_fake(0, 0x08, 0, Some(&sense), &mut [0u8; 8]);
    assert!(matches!(
        r,
        Err(Error::ScsiError {
            status: 0x08,
            sense: None,
            ..
        })
    ));
    assert_eq!(t.last_sense_progress(), None);
}

/// A failing IOKit return is a transport failure, whatever status the task reported.
#[test]
fn shim_selftest_iokit_failure_is_a_transport_failure() {
    let _globals = FakeDeviceGuard(shim_globals());
    let (_t, r) = execute_on_fake(0x2c2, 0, 8, None, &mut [0u8; 8]);
    assert!(matches!(
        r,
        Err(Error::ScsiError {
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
            ..
        })
    ));
}

/// `bytes_transferred` is what the device moved, never more than the buffer holds.
#[test]
fn shim_selftest_transfer_count_is_clamped_to_the_buffer() {
    let _globals = FakeDeviceGuard(shim_globals());
    for (count, want) in [(100u64, 100usize), (512, 512), (4096, 512), (u64::MAX, 512)] {
        let (_t, r) = execute_on_fake(0, 0, count, None, &mut [0u8; 512]);
        assert_eq!(r.expect("GOOD").bytes_transferred, want, "count {count}");
    }
}

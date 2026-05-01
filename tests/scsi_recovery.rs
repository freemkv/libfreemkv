//! Tests for SgIoTransport timeout recovery (Fix 2).
//!
//! When `execute()` detects a transport-level failure (kernel timeout /
//! USB bridge wedge — `host_status != 0`), it spawns two background
//! threads to (1) close the old fd and (2) open a fresh one, then
//! stores the new fd in `fd_recovery`. The next `execute()` call picks
//! up the recovered fd without blocking on close().
//!
//! These tests require a real /dev/sg* device and are therefore #[ignore].

use std::path::Path;
use std::time::Duration;

#[test]
#[ignore]
fn test_sgio_transport_timeout_does_not_kill_transport() {
    let device = "/dev/sg2";
    let device = std::env::var("FREEMKV_TEST_SG_DEVICE").unwrap_or(device.to_string());
    let _path = Path::new(&device);

    #[cfg(target_os = "linux")]
    {
        use libfreemkv::scsi::ScsiTransport;
        use libfreemkv::scsi::linux::SgIoTransport;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI32, Ordering};

        let mut transport = SgIoTransport::open(path).expect("open device");
        let fd_before = transport.fd;

        // READ_10 with 1 ms timeout to force kernel timeout.
        let cdb = [0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let mut data = vec![0u8; 2048];

        let start = std::time::Instant::now();
        let result = transport.execute(&cdb, DataDirection::FromDevice, &mut data, 1);
        let _elapsed = start.elapsed();

        assert!(result.is_err(), "expected Err on timeout, got {:?}", result);
        let err = result.unwrap_err();
        assert!(
            matches!(&err, libfreemkv::Error::ScsiError { status, .. } if *status == SCSI_STATUS_TRANSPORT_FAILURE),
            "expected ScsiError(TRANSPORT_FAILURE), got {:?}",
            err
        );
        assert_eq!(transport.fd, -1, "fd should be -1 after timeout");

        // Wait for recovery thread to store new fd.
        let recovered = Arc::new(AtomicI32::new(-1));
        let recovery_ref = transport.fd_recovery.clone();
        for _ in 0..100 {
            let v = recovery_ref.load(Ordering::Acquire);
            if v >= 0 {
                recovered.store(v, Ordering::Release);
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let new_fd = recovered.load(Ordering::Acquire);
        assert!(new_fd >= 0, "recovery thread should have produced a new fd");

        // Next execute() must pick up recovered fd quickly.
        let mut data2 = vec![0u8; 2048];
        let start2 = std::time::Instant::now();
        let result2 = transport.execute(&cdb, DataDirection::FromDevice, &mut data2, 5000);
        let elapsed2 = start2.elapsed();

        assert!(
            result2.is_ok(),
            "execute() with recovered fd should succeed, got {:?}",
            result2
        );
        assert!(
            elapsed2 < Duration::from_secs(2),
            "should return quickly, took {:?}",
            elapsed2
        );
        assert_ne!(transport.fd, -1, "fd should be valid after recovery");
        assert_ne!(transport.fd, fd_before, "fd should be fresh after recovery");
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("SKIP: test requires Linux / SgIoTransport");
    }
}

#[test]
#[ignore]
fn test_drive_read_per_cdb_timeout_bounds_call() {
    let device = "/dev/sg2";
    let device = std::env::var("FREEMKV_TEST_SG_DEVICE").unwrap_or(device.to_string());
    let _path = Path::new(&device);

    #[cfg(target_os = "linux")]
    {
        let mut drive = libfreemkv::Drive::open(path).expect("open drive");
        let timeout_ms: u32 = 5_000;

        let start = std::time::Instant::now();
        let _ = drive.read(0, 1, &mut [0u8; 2048], false);
        let elapsed = start.elapsed();

        let overhead = Duration::from_millis(500);
        assert!(
            elapsed < Duration::from_millis(timeout_ms as u64) + overhead,
            "Drive::read should return within timeout_ms + overhead, took {:?}",
            elapsed
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("SKIP: test requires Linux / SgIoTransport");
    }
}

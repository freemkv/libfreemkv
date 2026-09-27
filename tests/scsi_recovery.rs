//! Drive-level timeout bound on a real device. The SgIoTransport fd-recovery
//! test lives beside the transport (its fields are private) in
//! `src/scsi/linux.rs`. Requires a real /dev/sg* device, hence #[ignore].

use std::path::Path;

#[test]
#[ignore]
fn test_drive_read_per_cdb_timeout_bounds_call() {
    let device = "/dev/sg2";
    let device = std::env::var("FREEMKV_TEST_SG_DEVICE").unwrap_or(device.to_string());
    let _path = Path::new(&device);

    #[cfg(target_os = "linux")]
    {
        use std::time::Duration;

        let mut drive = libfreemkv::Drive::open(_path).expect("open drive");
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

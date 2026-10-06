use super::*;
use std::time::Duration;

// After a transport failure the fd is reopened in the background and the next
// execute() adopts it. Needs a real device: FREEMKV_TEST_SG_DEVICE (default sg2).
#[test]
#[ignore]
fn timeout_does_not_kill_transport() {
    let device = std::env::var("FREEMKV_TEST_SG_DEVICE").unwrap_or_else(|_| "/dev/sg2".to_string());
    let mut transport = SgIoTransport::open(Path::new(&device)).expect("open device");
    let fd_before = transport.fd;
    // READ(10) with a 1 ms timeout forces a kernel timeout.
    let cdb = [0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
    let mut data = vec![0u8; 2048];
    let err = transport
        .execute(&cdb, DataDirection::FromDevice, &mut data, 1)
        .expect_err("1 ms timeout must fail");
    assert!(err.is_scsi_transport_failure(), "got {err:?}");
    assert_eq!(transport.fd, -1, "fd handed to recovery");

    let mut published = false;
    for _ in 0..100 {
        if transport
            .fd_recovery
            .load(std::sync::atomic::Ordering::Acquire)
            >= 0
        {
            published = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(published, "recovery thread should have produced a new fd");

    let t0 = std::time::Instant::now();
    let r = transport.execute(&cdb, DataDirection::FromDevice, &mut data, 5_000);
    assert!(r.is_ok(), "recovered fd should work: {r:?}");
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    assert_ne!(transport.fd, -1, "fd valid after recovery");
    assert_ne!(transport.fd, fd_before, "fd fresh after recovery");
}

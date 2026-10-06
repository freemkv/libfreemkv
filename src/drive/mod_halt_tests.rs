use super::*;
use std::time::{Duration, Instant};

struct Idle;
impl ScsiTransport for Idle {
    fn execute(
        &mut self,
        _: &[u8],
        _: crate::scsi::DataDirection,
        _: &mut [u8],
        _: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        unreachable!("pause issues no CDB")
    }
}

fn idle() -> Drive {
    Drive::from_transport_for_test(Box::new(Idle))
}

// `Drive::pause` (was `sleep_until_halted`) is `Halt::wait` on the attached token.
#[test]
fn pause_completes_when_not_halted() {
    let t0 = Instant::now();
    assert!(idle().pause(Duration::from_millis(150)).is_ok());
    assert!(t0.elapsed() >= Duration::from_millis(140));
}

#[test]
fn pause_returns_immediately_if_prehalted() {
    let d = idle();
    d.halt();
    let t0 = Instant::now();
    assert!(matches!(
        d.pause(Duration::from_secs(10)),
        Err(Error::Halted)
    ));
    assert!(t0.elapsed() < Duration::from_millis(200));
}

#[test]
fn pause_wakes_mid_sleep() {
    let d = idle();
    let flag = d.halt_flag();
    let t0 = Instant::now();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        flag.store(true, std::sync::atomic::Ordering::Release);
    });
    let r = d.pause(Duration::from_secs(10));
    stopper.join().unwrap();
    assert!(matches!(r, Err(Error::Halted)));
    let waited = t0.elapsed();
    assert!(waited < Duration::from_secs(2), "waited {waited:?}");
    assert!(waited >= Duration::from_millis(140), "waited {waited:?}");
}

#[test]
fn pause_zero_duration_is_noop_when_not_halted() {
    assert!(idle().pause(Duration::ZERO).is_ok());
}

#[test]
fn read_capacity_short_transfer_is_rejected() {
    // bytes_transferred < 4 must NOT decode to capacity=1 from
    // zero-init bytes.
    let buf = [0u8; 8];
    assert!(matches!(
        decode_read_capacity(&buf, 0),
        Err(Error::DiscCapacityMalformed)
    ));
    assert!(matches!(
        decode_read_capacity(&buf, 3),
        Err(Error::DiscCapacityMalformed)
    ));
}

#[test]
fn read_capacity_full_transfer_decodes_last_lba_plus_one() {
    // last_lba = 0x00012344 -> capacity 0x00012345.
    let buf = [0x00, 0x01, 0x23, 0x44, 0, 0, 0, 0];
    assert_eq!(decode_read_capacity(&buf, 8).unwrap(), 0x0001_2345);
}

#[test]
fn read_capacity_overflow_is_rejected() {
    // last_lba = u32::MAX (the "capacity exceeds 32-bit" sentinel) -> +1
    // overflows; reported as the distinct DiscCapacityOverflow, not the
    // short-transfer DiscCapacityMalformed.
    let buf = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];
    assert!(matches!(
        decode_read_capacity(&buf, 8),
        Err(Error::DiscCapacityOverflow)
    ));
}

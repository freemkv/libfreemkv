use super::Heartbeat;
use std::time::Duration;

/// A fresh heartbeat does not beat on the first tick — the interval has not
/// elapsed — so a fast loop is not spammed.
#[test]
fn first_tick_does_not_beat() {
    let mut hb = Heartbeat::with_interval("test", Duration::from_secs(60));
    assert!(!hb.tick(0, 100));
    assert!(!hb.tick(50, 100));
}

/// Once the interval elapses, exactly one beat fires, then the throttle
/// resets.
#[test]
fn beats_once_per_interval() {
    let mut hb = Heartbeat::with_interval("test", Duration::from_secs(1));
    assert!(!hb.tick(1, 100));
    std::thread::sleep(Duration::from_millis(1100));
    assert!(hb.tick(2, 100), "should beat after interval elapsed");
    // Immediately after, throttle suppresses the next.
    assert!(!hb.tick(3, 100));
}

/// tick_cpu only consults the clock every 256 calls: the first 255 calls
/// never beat even with a zero interval.
#[test]
fn tick_cpu_throttles_clock_reads() {
    let mut hb = Heartbeat::with_interval("test", Duration::from_nanos(0));
    for _ in 0..255 {
        assert!(!hb.tick_cpu(0, 100));
    }
    // 256th call consults the clock; with a zero interval it beats.
    assert!(hb.tick_cpu(0, 100));
}

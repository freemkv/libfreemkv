//! Shared by the read-path benches: input sizing, CPU and peak-RSS accounting,
//! and one report line per case. Not every bench uses every helper.
#![allow(dead_code)]

use std::time::{Duration, Instant};

/// MiB of data each case processes: `FREEMKV_BENCH_MIB`, else `default`.
pub fn bench_mib(default: usize) -> usize {
    std::env::var("FREEMKV_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(default)
}

/// User plus system CPU seconds this process has used so far.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn cpu_seconds() -> f64 {
    let ru = rusage();
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn cpu_seconds() -> f64 {
    f64::NAN
}

/// Peak resident set size in MiB (`ru_maxrss`: KiB on Linux, bytes on macOS).
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn peak_rss_mib() -> f64 {
    let max = rusage().ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        max / (1024.0 * 1024.0)
    } else {
        max / 1024.0
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn peak_rss_mib() -> f64 {
    f64::NAN
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rusage() -> libc::rusage {
    // SAFETY: `getrusage` fills the zeroed struct it is handed and keeps no pointer.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    }
}

/// Wall time and CPU seconds spent in `f`.
pub fn measure(f: impl FnOnce()) -> (Duration, f64) {
    let cpu0 = cpu_seconds();
    let t0 = Instant::now();
    f();
    (t0.elapsed(), cpu_seconds() - cpu0)
}

/// One result line: throughput, and CPU seconds per GiB (the cost wall time hides).
pub fn report(name: &str, bytes: u64, wall: Duration, cpu: f64) {
    let secs = wall.as_secs_f64().max(1e-9);
    let mib = bytes as f64 / (1024.0 * 1024.0);
    let gib = mib / 1024.0;
    println!(
        "{name:<44} {:>9.1} MiB/s  {:>7.3} cpu-s/GiB  {:>7.1} MiB peak RSS",
        mib / secs,
        cpu / gib.max(1e-9),
        peak_rss_mib()
    );
}

/// One encrypted BD-TS aligned unit: 32 source packets, CPI `11` and a TS sync in each,
/// a payload varied by `n`, then encrypted under `key`.
pub fn encrypted_unit(n: u32, key: &[u8; 16]) -> Vec<u8> {
    let mut u: Vec<u8> = (0..6144u32)
        .map(|i| (n.wrapping_mul(7919).wrapping_add(i) % 251) as u8 | 1)
        .collect();
    for p in u.chunks_mut(192) {
        p[0] |= 0xC0;
        p[4] = 0x47;
    }
    assert!(libfreemkv::test_util::encrypt_unit(&mut u, key));
    u
}

/// `units` encrypted units back to back.
pub fn encrypted_units(units: u32, key: &[u8; 16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(units as usize * 6144);
    for n in 0..units {
        out.extend_from_slice(&encrypted_unit(n, key));
    }
    out
}

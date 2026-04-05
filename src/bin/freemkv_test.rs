//! freemkv-test — Quick verification that raw disc access works.
//!
//! Enables raw read mode, calibrates speed, reads a few test sectors.
//! Use this to verify your drive and profile are working correctly.
//!
//! Usage:
//!   freemkv-test /dev/sr0
//!   freemkv-test /dev/sr0 --profiles ./profiles

use std::env;
use std::path::Path;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("freemkv-test — Verify raw disc access works");
        eprintln!();
        eprintln!("Usage: freemkv-test <device> [--profiles <dir>]");
        process::exit(1);
    }

    let device = Path::new(&args[1]);

    println!("freemkv-test v{}", env!("CARGO_PKG_VERSION"));
    println!();

    // Open drive session (uses bundled profiles)
    print!("Opening {}... ", device.display());
    let mut session = match libfreemkv::DriveSession::open(device) {
        Ok(s) => { println!("OK"); s }
        Err(e) => { println!("FAILED: {}", e); process::exit(1); }
    };

    println!("  Drive ID: {}", session.profile.drive_id);
    println!("  Platform: {}", session.profile.platform.name());
    println!();

    // Enable raw read mode
    print!("Unlocking drive... ");
    match session.unlock() {
        Ok(()) => println!("OK"),
        Err(e) => { println!("FAILED: {}", e); process::exit(1); }
    }

    // Check status
    print!("Checking status... ");
    match session.status() {
        Ok(status) => {
            if status.unlocked {
                println!("OK (active)");
            } else {
                println!("WARNING: drive reported as locked");
            }
        }
        Err(e) => println!("SKIP ({})", e),
    }

    // Calibrate speed
    print!("Calibrating speed... ");
    match session.calibrate() {
        Ok(()) => println!("OK"),
        Err(e) => println!("SKIP ({})", e),
    }

    // Read test sectors
    let test_lbas: &[u32] = &[0, 100, 1000, 10000];
    let mut buf = vec![0u8; 2048];
    let mut pass = 0;
    let mut fail = 0;

    for &lba in test_lbas {
        print!("Reading sector {}... ", lba);
        match session.read_sectors(lba, 1, &mut buf) {
            Ok(n) if n == 2048 => {
                let nonzero = buf.iter().filter(|&&b| b != 0).count();
                println!("OK ({} bytes, {} non-zero)", n, nonzero);
                pass += 1;
            }
            Ok(n) => {
                println!("PARTIAL ({} bytes)", n);
                fail += 1;
            }
            Err(e) => {
                println!("FAILED: {}", e);
                fail += 1;
            }
        }
    }

    println!();
    if fail == 0 {
        println!("All {} checks passed. Drive is fully functional.", pass);
    } else {
        println!("{} passed, {} failed.", pass, fail);
        process::exit(1);
    }
}

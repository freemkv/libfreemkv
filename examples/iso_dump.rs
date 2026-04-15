// Minimal ISO dumper — find exact stall point
use libfreemkv::Drive;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: iso_dump <device> <output>");
        std::process::exit(1);
    }

    let mut drive = Drive::open(Path::new(&args[1])).unwrap();
    drive.wait_ready().unwrap();
    let _ = drive.init();
    let _ = drive.probe_disc();

    // AACS handshake — required to read past the protected area
    eprint!("Scanning disc... ");
    let _ = libfreemkv::Disc::scan(&mut drive, &libfreemkv::ScanOptions::default());
    eprintln!("OK");

    let cap = drive.read_capacity().unwrap();
    let batch = libfreemkv::disc::detect_max_batch_sectors(drive.device_path());

    eprintln!("Device: {} | {} sectors | batch {}", args[1], cap, batch);

    let file = std::fs::File::create(&args[2]).unwrap();
    let mut w = BufWriter::with_capacity(4 * 1024 * 1024, file);
    let mut buf = vec![0u8; batch as usize * 2048];
    let mut lba: u32 = 0;
    let start = Instant::now();
    let mut last = Instant::now();
    let mut bytes: u64 = 0;
    let mut last_bytes: u64 = 0;

    while lba < cap {
        let count = ((cap - lba) as u16).min(batch);
        let n = count as usize * 2048;

        // Tiny yield between reads — test if pacing prevents firmware throttle
        std::thread::yield_now();
        let t0 = Instant::now();
        let ok = drive.read(lba, count, &mut buf[..n]).is_ok();
        let read_ms = t0.elapsed().as_millis();

        // Flag slow reads
        if read_ms > 2000 {
            eprintln!("\n  SLOW READ: LBA {} took {}ms (ok={})", lba, read_ms, ok);
        }

        if !ok {
            buf[..n].fill(0);
        }
        w.write_all(&buf[..n]).unwrap();
        lba += count as u32;
        bytes += n as u64;

        if last.elapsed().as_millis() >= 1000 {
            let delta = bytes - last_bytes;
            let speed = delta as f64 / last.elapsed().as_secs_f64() / 1_048_576.0;
            let avg = bytes as f64 / start.elapsed().as_secs_f64() / 1_048_576.0;
            let pct = bytes as f64 / (cap as f64 * 2048.0) * 100.0;
            eprint!(
                "\r  {:.1}% LBA {} | {:.0} MB/s (avg {:.0}) | {:.1} GB    ",
                pct,
                lba,
                speed,
                avg,
                bytes as f64 / 1e9
            );
            last_bytes = bytes;
            last = Instant::now();
        }
    }
    w.flush().unwrap();
    eprintln!(
        "\nDone: {:.1} GB in {:.0}s",
        bytes as f64 / 1e9,
        start.elapsed().as_secs_f64()
    );
}

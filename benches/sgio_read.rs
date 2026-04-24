// Mimics ISO dump exactly — read + write + progress
use libfreemkv::Drive;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

fn main() {
    let device = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .unwrap_or_else(|| {
            let drives = libfreemkv::find_drives();
            if drives.is_empty() {
                eprintln!("No drives found");
                std::process::exit(1);
            }
            drives[0].device_path().to_string()
        });

    let mut drive = Drive::open(Path::new(&device)).unwrap_or_else(|e| {
        eprintln!("Cannot open {}: {}", device, e);
        std::process::exit(1);
    });
    eprintln!("wait_ready...");
    let _ = drive.wait_ready();
    eprintln!("read_capacity...");
    let cap = drive.read_capacity().unwrap();
    eprintln!("capacity: {} sectors", cap);

    let batch = libfreemkv::disc::detect_max_batch_sectors(drive.device_path());
    let mut buf = vec![0u8; batch as usize * 2048];

    // Open /dev/null writer like ISO dump does
    let file = std::fs::File::create("/dev/null").unwrap();
    let mut writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);

    eprintln!(
        "Reading 1000 batches ({:.1} MB) with write + progress...",
        1000.0 * batch as f64 * 2048.0 / 1_048_576.0
    );

    let start = Instant::now();
    let mut ok = 0u32;
    let mut fail = 0u32;
    let mut bytes: u64 = 0;

    // Recovery flag: true matches pre-0.11.13 bench behavior — full SCSI
    // ECC retry loop on errors (slower, what the rip path used before the
    // adaptive batch sizer landed). Flip to `false` for the fast-fail path
    // that current rips use; benches are configurable via this constant.
    const READ_WITH_RECOVERY: bool = true;

    for i in 0..1000u32 {
        let lba = i * batch as u32;
        match drive.read(lba, batch, &mut buf, READ_WITH_RECOVERY) {
            Ok(_) => {
                writer.write_all(&buf).unwrap();
                ok += 1;
            }
            Err(e) => {
                fail += 1;
                if fail <= 5 {
                    eprintln!("  FAIL LBA {}: {}", lba, e);
                }
                buf.fill(0);
                writer.write_all(&buf).unwrap();
            }
        }
        bytes += buf.len() as u64;

        if i % 50 == 0 && i > 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let mb = bytes as f64 / 1_048_576.0;
            eprint!("\r  {:.1} MB | {:.1} MB/s    ", mb, mb / elapsed);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mb = ok as f64 * batch as f64 * 2048.0 / 1_048_576.0;
    eprintln!(
        "\n{} ok, {} fail, {:.1} MB in {:.1}s = {:.1} MB/s",
        ok,
        fail,
        mb,
        elapsed,
        mb / elapsed
    );
}

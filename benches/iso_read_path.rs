//! End-to-end ISO read bench, no drive: a synthetic AACS image on disk, read through
//! `FileSectorSource`, the keyed whole-disc reader and the prefetcher, drained by a
//! consumer that recycles every buffer, as the demux does.
//!
//! `cargo bench --bench iso_read_path` (`FREEMKV_BENCH_MIB` sets the image size;
//! `FREEMKV_BENCH_DIR` the directory it is written to, default the temp dir).
//! Each line reports MiB/s, CPU seconds per GiB and the process peak RSS so far.

mod common;

use common::{bench_mib, encrypted_unit, measure, report};
use libfreemkv::disc::Extent;
use libfreemkv::sector::{FileSectorSource, PrefetchedSectorSource};
use libfreemkv::test_util::keyed_reader;
use std::io::Write;

const KEY: [u8; 16] = [0xA5; 16];
const SECTOR: usize = 2048;
// Clear sectors before the stream file, as a UDF image's metadata sits before its data.
const LEAD: u32 = 300;

fn main() {
    let mib = bench_mib(1024);
    let dir = match std::env::var_os("FREEMKV_BENCH_DIR") {
        Some(d) => tempfile::tempdir_in(d),
        None => tempfile::tempdir(),
    }
    .expect("bench dir");
    let path = dir.path().join("bench.iso");
    let units = (mib * 1024 * 1024 / 6144) as u32;
    write_image(&path, units);
    let file_sectors = units * 3;
    println!("iso read path bench: {mib} MiB image at {}", path.display());
    for threads in [0, 1] {
        for batch in [510u16, 1536, 3072, 8190] {
            run(&path, file_sectors, batch, threads);
        }
    }
    libfreemkv::set_decrypt_threads(0);
}

fn write_image(path: &std::path::Path, units: u32) {
    let f = std::fs::File::create(path).expect("create image");
    let mut w = std::io::BufWriter::with_capacity(8 << 20, f);
    for _ in 0..LEAD {
        w.write_all(&[0u8; SECTOR]).expect("write lead");
    }
    for n in 0..units {
        w.write_all(&encrypted_unit(n, &KEY)).expect("write unit");
    }
    w.flush().expect("flush image");
}

fn run(path: &std::path::Path, file_sectors: u32, batch: u16, threads: usize) {
    libfreemkv::set_decrypt_threads(threads);
    let file = FileSectorSource::open(path).expect("open image");
    let reader = keyed_reader(file, KEY, &[(LEAD, LEAD + file_sectors)]);
    let extents = vec![Extent {
        start_lba: LEAD,
        sector_count: file_sectors,
    }];
    let ctx = libfreemkv::Ctx::default();
    let mut bytes = 0u64;
    let (wall, cpu) = measure(|| {
        let src = PrefetchedSectorSource::new(reader, extents, batch, &ctx).expect("prefetch");
        let (rx, recycle, shell) = src.into_channels();
        while let Ok(item) = rx.recv() {
            let buf = item.expect("batch");
            bytes += buf.len() as u64;
            std::hint::black_box(&buf);
            let _ = recycle.send(buf);
        }
        drop(shell);
    });
    assert_eq!(
        bytes,
        u64::from(file_sectors) * SECTOR as u64,
        "whole file read"
    );
    let threads = match threads {
        0 => format!("default({})", libfreemkv::decrypt_threads()),
        n => n.to_string(),
    };
    report(
        &format!("iso prefetch batch {batch} threads {threads}"),
        bytes,
        wall,
        cpu,
    );
}

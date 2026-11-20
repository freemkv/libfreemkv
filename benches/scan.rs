//! Scan-time bench over a synthetic Blu-ray tree (no drive, no criterion).
//!
//! Builds a BDMV folder (playlists, CLPI files with a padded EP-map area, stream files),
//! lays it out as a UDF image with `DirImage`, then times and counts allocations for:
//! - `Disc::scan_image` (the whole scan: playlists, clips, extents, labels);
//! - the live scan's metadata prefetch plus reading every MPLS/CLPI back through it
//!   (`test_util::prefetch_scan_files`).
//!
//! Two shapes: `typical` (60 playlists of 10 clips) and `obfuscated` (1000 playlists
//! of 40 clips over 2000 stream files). Run: `cargo bench --bench scan`.
//! `SCAN_BENCH_ITERS` overrides the iteration count (default 5).

use libfreemkv::{DirImage, Disc, ScanOptions, SectorSource};
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

// Counts allocations and tracks live and peak heap bytes.
struct Counting;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        let live = LIVE.fetch_add(l.size(), Relaxed) + l.size();
        PEAK.fetch_max(live, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        if new > l.size() {
            let live = LIVE.fetch_add(new - l.size(), Relaxed) + (new - l.size());
            PEAK.fetch_max(live, Relaxed);
        } else {
            LIVE.fetch_sub(l.size() - new, Relaxed);
        }
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

struct Shape {
    name: &'static str,
    playlists: usize,
    clips_per_playlist: usize,
    stream_files: usize,
    clpi_pad: usize,
}

const SHAPES: [Shape; 2] = [
    Shape {
        name: "typical",
        playlists: 60,
        clips_per_playlist: 10,
        stream_files: 100,
        clpi_pad: 64 * 1024,
    },
    Shape {
        name: "obfuscated",
        playlists: 1000,
        clips_per_playlist: 40,
        stream_files: 2000,
        clpi_pad: 16 * 1024,
    },
];

// One STN stream entry plus attributes (see src/disc/bluray_tests.rs).
fn stream_entry(pid: u16, attrs: &[u8]) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    out.push(attrs.len() as u8);
    out.extend_from_slice(attrs);
    out
}

// An MPLS whose first PlayItem carries 1 video, 8 audio and 8 PG streams; one chapter
// mark per PlayItem.
fn mpls(clips: &[u32]) -> Vec<u8> {
    let langs: [&[u8; 3]; 8] = [
        b"eng", b"fra", b"deu", b"spa", b"ita", b"jpn", b"por", b"nld",
    ];
    let mut entries = vec![stream_entry(0x1011, &[0x24, 0x61, 0x12])];
    for (i, l) in langs.iter().enumerate() {
        entries.push(stream_entry(
            0x1100 + i as u16,
            &[0x83, 0x61, l[0], l[1], l[2]],
        ));
    }
    for (i, l) in langs.iter().enumerate() {
        entries.push(stream_entry(0x1200 + i as u16, &[0x90, l[0], l[1], l[2]]));
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&58u32.to_be_bytes());
    buf.extend_from_slice(&[0u8; 28]);
    buf.extend_from_slice(&14u32.to_be_bytes());
    buf.extend_from_slice(&[0, 1]);
    buf.extend_from_slice(&[0u8; 12]);
    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 6]);
    buf.extend_from_slice(&(clips.len() as u16).to_be_bytes());
    buf.extend_from_slice(&[0u8; 2]);
    for (idx, clip) in clips.iter().enumerate() {
        let mut item = Vec::new();
        item.extend_from_slice(format!("{clip:05}").as_bytes());
        item.extend_from_slice(b"M2TS");
        item.extend_from_slice(&[0, 0, 0]);
        item.extend_from_slice(&0u32.to_be_bytes());
        item.extend_from_slice(&(120 * 45_000u32).to_be_bytes());
        item.extend_from_slice(&[0u8; 12]);
        if idx == 0 {
            let stn = item.len();
            item.extend_from_slice(&[0u8; 4]);
            item.extend_from_slice(&[1, 8, 8, 0, 0, 0, 0, 0]);
            item.extend_from_slice(&[0u8; 4]);
            for e in &entries {
                item.extend_from_slice(e);
            }
            let len = (item.len() - stn - 2) as u16;
            item[stn..stn + 2].copy_from_slice(&len.to_be_bytes());
        }
        buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
        buf.extend_from_slice(&item);
    }
    let pl_len = (buf.len() - pl_start - 4) as u32;
    buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());
    let mark_start = buf.len() as u32;
    buf[12..16].copy_from_slice(&mark_start.to_be_bytes());
    buf.extend_from_slice(&((2 + clips.len() * 14) as u32).to_be_bytes());
    buf.extend_from_slice(&(clips.len() as u16).to_be_bytes());
    for i in 0..clips.len() {
        buf.extend_from_slice(&[0, 1]);
        buf.extend_from_slice(&(i as u16).to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 6]);
    }
    buf
}

// A CLPI with a valid header and `pad` bytes standing in for the CPI/EP map.
fn clpi(packets: u32, pad: usize) -> Vec<u8> {
    let mut d = vec![0u8; 60 + pad];
    d[0..4].copy_from_slice(b"HDMV");
    d[4..8].copy_from_slice(b"0200");
    d[56..60].copy_from_slice(&packets.to_be_bytes());
    d
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn build_tree(root: &Path, s: &Shape) {
    let bdmv = root.join("BDMV");
    write(&bdmv.join("index.bdmv"), b"INDX0200");
    for c in 0..s.stream_files as u32 {
        write(
            &bdmv.join(format!("CLIPINF/{c:05}.clpi")),
            &clpi(32, s.clpi_pad),
        );
        write(&bdmv.join(format!("STREAM/{c:05}.m2ts")), &[0u8; 6144]);
    }
    for p in 0..s.playlists {
        let clips: Vec<u32> = (0..s.clips_per_playlist)
            .map(|i| ((p * 7 + i * 13) % s.stream_files) as u32)
            .collect();
        write(&bdmv.join(format!("PLAYLIST/{p:05}.mpls")), &mpls(&clips));
    }
}

fn measure<T>(label: &str, iters: usize, mut f: impl FnMut() -> T) -> T {
    let mut out = None;
    let mut best = f64::MAX;
    let (a0, base) = (ALLOCS.load(Relaxed), LIVE.load(Relaxed));
    PEAK.store(base, Relaxed);
    for _ in 0..iters {
        let t = Instant::now();
        out = Some(f());
        best = best.min(t.elapsed().as_secs_f64());
    }
    let allocs = (ALLOCS.load(Relaxed) - a0) / iters;
    let peak = PEAK.load(Relaxed).saturating_sub(base);
    println!(
        "{label:<40} best {:>9.2} ms  {allocs:>9} allocs/iter  peak +{:>8.2} MiB",
        best * 1e3,
        peak as f64 / 1_048_576.0
    );
    out.unwrap()
}

fn main() {
    let iters = std::env::var("SCAN_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5usize);
    for s in &SHAPES {
        let dir = std::env::temp_dir().join(format!(
            "libfreemkv-scan-bench-{}-{}",
            std::process::id(),
            s.name
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let root = dir.join("BENCH_DISC");
        build_tree(&root, s);
        let mut img = DirImage::open(&root).expect("DirImage");
        let cap = img.capacity_sectors();

        let titles = measure(&format!("scan_image/{}", s.name), iters, || {
            Disc::scan_image(&mut img, cap, &ScanOptions::default())
                .expect("scan")
                .titles
                .len()
        });
        let (ranges, bytes) = measure(&format!("prefetch_scan_files/{}", s.name), iters, || {
            libfreemkv::test_util::prefetch_scan_files(&mut img, 64).expect("prefetch")
        });
        println!("  {titles} titles; prefetch {ranges} ranges, {bytes} metadata bytes read back");
        drop(img);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

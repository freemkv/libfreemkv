//! Integration tests for progress reporting, halt behavior, drop safety,
//! and the file-backed sector reader round trip.

use libfreemkv::disc::{CopyOptions, DiscRegion};
use libfreemkv::error::Result;
use libfreemkv::pes::Stream as PesStream;
use libfreemkv::{
    ContentFormat, Disc, DiscFormat, DiscStream, DiscTitle, EventKind, Extent, FileSectorReader,
    SectorReader,
};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SECTOR_SIZE: usize = 2048;

// ── helpers ────────────────────────────────────────────────────────────────

/// Returns zeroed sectors. Always succeeds. Counts each call.
struct ZeroSectorReader {
    capacity: u32,
    calls: Arc<AtomicU64>,
}

impl ZeroSectorReader {
    fn new(capacity: u32) -> Self {
        Self {
            capacity,
            calls: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl SectorReader for ZeroSectorReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let bytes = count as usize * SECTOR_SIZE;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

/// Like ZeroSectorReader but sleeps a configurable duration per call.
/// Used by the halt test so the copy takes >1 s.
struct SlowZeroSectorReader {
    capacity: u32,
    sleep_per_call: Duration,
}

impl SlowZeroSectorReader {
    fn new(capacity: u32, sleep_per_call: Duration) -> Self {
        Self {
            capacity,
            sleep_per_call,
        }
    }
}

impl SectorReader for SlowZeroSectorReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        std::thread::sleep(self.sleep_per_call);
        let bytes = count as usize * SECTOR_SIZE;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

/// Build a Disc instance with a known capacity, no titles, no encryption.
/// Sufficient for `Disc::copy` (which only uses capacity_sectors + decrypt keys).
fn synthetic_disc(capacity_sectors: u32) -> Disc {
    Disc {
        volume_id: String::new(),
        meta_title: None,
        format: DiscFormat::BluRay,
        capacity_sectors,
        capacity_bytes: capacity_sectors as u64 * SECTOR_SIZE as u64,
        layers: 1,
        titles: Vec::new(),
        region: DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: false,
        content_format: ContentFormat::BdTs,
    }
}

/// Build a DiscTitle with a single extent of `sector_count` sectors and no
/// streams (DiscStream still iterates sectors and would emit BytesRead).
fn synthetic_title(sector_count: u32) -> DiscTitle {
    DiscTitle {
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: sector_count as u64 * SECTOR_SIZE as u64,
        clips: Vec::new(),
        streams: Vec::new(),
        chapters: Vec::new(),
        extents: vec![Extent {
            start_lba: 0,
            sector_count,
        }],
        content_format: ContentFormat::BdTs,
        codec_privates: Vec::new(),
    }
}

// ── 1. BytesRead events emitted during disc copy (TDD red) ────────────────

#[test]
fn test_bytes_read_emitted_during_disc_copy() {
    // Build a tiny synthetic disc and stream it through DiscStream.
    let reader = ZeroSectorReader::new(64);
    let title = synthetic_title(64);
    let keys = libfreemkv::DecryptKeys::None;

    let mut stream = DiscStream::new(Box::new(reader), title, keys, 60, ContentFormat::BdTs);

    let count = Arc::new(AtomicU64::new(0));
    let count_cb = count.clone();
    stream.on_event(move |ev| {
        if let EventKind::BytesRead { .. } = ev.kind {
            count_cb.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Drive the stream to EOF. With no streams configured, read() returns
    // Ok(None) once all extents are exhausted.
    loop {
        match stream.read() {
            Ok(Some(_frame)) => {}
            Ok(None) => break,
            Err(e) => panic!("stream read failed: {e:?}"),
        }
    }

    let n = count.load(Ordering::Relaxed);
    // EXPECTED TO FAIL until BytesRead emission is wired up. TDD red.
    assert!(
        n > 0,
        "expected at least one BytesRead event, got {n} (lib does not yet emit BytesRead)"
    );
}

// ── 2. Disc::copy on_progress callback fires (regression guard) ───────────

#[test]
fn test_disc_copy_progress_callback_fires() {
    let disc = synthetic_disc(64);
    let mut reader = ZeroSectorReader::new(64);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp); // we want the path, not the file handle

    let calls = Arc::new(AtomicU64::new(0));
    let last_bytes = Arc::new(AtomicU64::new(0));
    let calls_cb = calls.clone();
    let last_bytes_cb = last_bytes.clone();

    let progress = move |bytes: u64, _total: u64| {
        calls_cb.fetch_add(1, Ordering::Relaxed);
        last_bytes_cb.store(bytes, Ordering::Relaxed);
    };

    let opts = CopyOptions {
        decrypt: false,
        on_progress: Some(&progress),
        ..Default::default()
    };

    let result = disc.copy(&mut reader, &iso_path, &opts).expect("copy ok");

    // Cleanup any sidecar mapfile + ISO before assertions.
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    assert!(result.complete, "copy should be complete");
    let n = calls.load(Ordering::Relaxed);
    let last = last_bytes.load(Ordering::Relaxed);
    assert!(n > 0, "on_progress should fire at least once, got {n}");
    assert!(
        last > 0,
        "final progress bytes should be non-zero, got {last}"
    );
}

// ── 3. Halt aborts disc copy promptly ─────────────────────────────────────

#[test]
fn test_halt_aborts_disc_copy_promptly() {
    // 6000 sectors, 60-sector batches → 100 read_sectors() calls.
    // 10 ms sleep per call → ~1 s total without halt.
    let capacity_sectors: u32 = 6000;
    let mut reader = SlowZeroSectorReader::new(capacity_sectors, Duration::from_millis(10));
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let halt = Arc::new(AtomicBool::new(false));
    let halt_for_thread = halt.clone();
    let iso_path_for_thread = iso_path.clone();

    let join = std::thread::spawn(move || {
        let opts = CopyOptions {
            decrypt: false,
            halt: Some(halt_for_thread),
            ..Default::default()
        };
        let t0 = Instant::now();
        let res = disc.copy(&mut reader, &iso_path_for_thread, &opts);
        (res, t0.elapsed())
    });

    // Let copy run, then halt.
    std::thread::sleep(Duration::from_millis(200));
    halt.store(true, Ordering::Relaxed);

    // Bound the join: should exit far before the full 1 s otherwise needed.
    let started = Instant::now();
    let mut joined = None;
    while started.elapsed() < Duration::from_millis(2000) {
        if join.is_finished() {
            joined = Some(join.join().expect("thread join"));
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let (result, elapsed) = joined.expect("copy thread did not exit within 2s of halt");

    // Cleanup
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    let copy_result = result.expect("copy returns Ok with halted=true on halt");
    assert!(
        copy_result.halted,
        "copy_result.halted should be true after halt"
    );
    assert!(
        !copy_result.complete,
        "copy_result.complete should be false when halted"
    );
    assert!(
        elapsed < Duration::from_millis(2000),
        "copy thread exit elapsed {elapsed:?} exceeded 2s"
    );
}

// ── 4. DiscStream Drop does not panic or block ────────────────────────────

#[test]
fn test_drop_impls_do_not_panic_or_block() {
    let reader = ZeroSectorReader::new(64);
    let title = synthetic_title(64);
    let keys = libfreemkv::DecryptKeys::None;
    let stream = DiscStream::new(Box::new(reader), title, keys, 60, ContentFormat::BdTs);

    // Drop on a worker thread; main thread enforces the timeout.
    let handle = std::thread::spawn(move || {
        drop(stream);
    });

    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(100) {
        if handle.is_finished() {
            handle.join().expect("drop thread join");
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("DiscStream drop did not complete within 100ms");
}

// ── 5. FileSectorReader round trip ────────────────────────────────────────

#[test]
fn test_file_sector_reader_round_trip() {
    // Build 8 sectors of pseudo-random bytes (sector-aligned).
    const N_SECTORS: usize = 8;
    let mut data = vec![0u8; N_SECTORS * SECTOR_SIZE];
    for (i, b) in data.iter_mut().enumerate() {
        // Cheap PRNG: just a multiplicative pattern, deterministic for asserts.
        *b = ((i as u64).wrapping_mul(2654435761) >> 16) as u8;
    }

    let mut tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    tmp.write_all(&data).expect("write data");
    tmp.flush().expect("flush");

    let path = tmp.path().to_str().expect("path utf-8").to_string();
    let mut fsr = FileSectorReader::open(&path).expect("open FileSectorReader");

    assert_eq!(fsr.capacity(), N_SECTORS as u32, "capacity mismatch");

    // Read each sector individually and compare.
    let mut buf = vec![0u8; SECTOR_SIZE];
    for lba in 0..N_SECTORS as u32 {
        let n = fsr
            .read_sectors(lba, 1, &mut buf, false)
            .expect("read_sectors");
        assert_eq!(n, SECTOR_SIZE);
        let off = lba as usize * SECTOR_SIZE;
        assert_eq!(
            &buf[..],
            &data[off..off + SECTOR_SIZE],
            "sector {lba} mismatch"
        );
    }

    // Read all sectors at once and compare.
    let mut all = vec![0u8; N_SECTORS * SECTOR_SIZE];
    let n = fsr
        .read_sectors(0, N_SECTORS as u16, &mut all, false)
        .expect("read all sectors");
    assert_eq!(n, N_SECTORS * SECTOR_SIZE);
    assert_eq!(all, data, "bulk read mismatch");
}

// ── 6. Disc::copy stall detection triggers skip-forward (TDD red) ─────────
//
// Regression guard for the Dell-host hang where `read_sectors` blocked inside
// a kernel-level USB stall and `Disc::copy` sat frozen for 10+ minutes with
// no progress and no error. The fix introduces `CopyOptions::stall_secs:
// Option<u64>` — when elapsed-since-last-`bytes_good`-advance exceeds the
// threshold, `Disc::copy` treats the current block as a read failure and
// triggers the skip-forward path so the rip can advance.
//
// THIS TEST IS EXPECTED TO FAIL UNTIL THE PARALLEL FIX LANDS.
// - Until `stall_secs` exists on `CopyOptions`, the test will not compile.
// - Once the field exists but the stall guard isn't wired, the spawned copy
//   thread will never exit (test fails on the 5s join bound).
// - Once the guard is wired, copy returns within ~stall_secs with
//   `complete=false` and `bytes_pending>0`.

/// Reader that returns Ok for sectors `< block_after`, then returns Err for
/// any sector `>= block_after` after a small per-call delay. Models the
/// realistic Dell-host symptom: reads keep returning Err (skip-forward fires)
/// but no `bytes_good` ever accrues; without a stall guard, Pass 1 grinds
/// silently for tens of minutes.
struct StallingSectorReader {
    capacity: u32,
    block_after: u32,
    /// Per-call delay for sectors >= block_after (simulates slow reads).
    err_delay_ms: u64,
    release: Arc<AtomicBool>,
    /// Retained so callers can release the reader; unused now that the
    /// reader returns Err instead of blocking, but kept so the test's
    /// existing release plumbing compiles.
    park: Arc<(Mutex<()>, std::sync::Condvar)>,
}

impl StallingSectorReader {
    fn new(capacity: u32, block_after: u32) -> Self {
        Self {
            capacity,
            block_after,
            err_delay_ms: 100,
            release: Arc::new(AtomicBool::new(false)),
            park: Arc::new((Mutex::new(()), std::sync::Condvar::new())),
        }
    }

    fn release_handle(&self) -> (Arc<AtomicBool>, Arc<(Mutex<()>, std::sync::Condvar)>) {
        (self.release.clone(), self.park.clone())
    }
}

impl SectorReader for StallingSectorReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        if lba >= self.block_after {
            // Realistic stall model: read takes err_delay_ms then returns
            // Err. With skip_on_error+skip_forward, Disc::copy will keep
            // skip-forwarding through this region — no bytes_good accrues.
            // The stall guard fires when bytes_good is unchanged for
            // stall_secs.
            std::thread::sleep(Duration::from_millis(self.err_delay_ms));
            return Err(libfreemkv::error::Error::DiscRead { sector: lba as u64 });
        }
        let bytes = count as usize * SECTOR_SIZE;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

#[test]
fn test_disc_copy_stall_detection_triggers_skip_forward() {
    // 1024 sectors total. Reader serves the first 64 sectors instantly, then
    // every later read blocks forever. With stall_secs=2, copy should bail
    // out of the stalled block within ~2s and either skip forward or finish
    // with bytes_pending > 0 / complete=false.
    let capacity_sectors: u32 = 1024;
    let block_after: u32 = 64;

    let reader = StallingSectorReader::new(capacity_sectors, block_after);
    let (release_flag, park) = reader.release_handle();
    let mut reader = reader;

    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let iso_path_for_thread = iso_path.clone();

    let join = std::thread::spawn(move || {
        let opts = CopyOptions {
            decrypt: false,
            skip_on_error: true,
            skip_forward: true,
            // ASSUMPTION: parallel fix adds `pub stall_secs: Option<u64>` to
            // CopyOptions. If the field name differs, update here.
            stall_secs: Some(2),
            ..Default::default()
        };
        let t0 = Instant::now();
        let res = disc.copy(&mut reader, &iso_path_for_thread, &opts);
        (res, t0.elapsed())
    });

    // Bound the join to ~5s. With stall_secs=2 the copy should exit well
    // within this window. If it doesn't, the stall guard isn't working.
    let started = Instant::now();
    let mut joined = None;
    while started.elapsed() < Duration::from_millis(5000) {
        if join.is_finished() {
            joined = Some(join.join().expect("thread join"));
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Whether or not the join succeeded, release the parked reader thread so
    // it can exit (its &mut reader is owned by the spawned thread; releasing
    // lets that thread unwind cleanly).
    release_flag.store(true, Ordering::Relaxed);
    park.1.notify_all();

    let (result, elapsed) = match joined {
        Some(v) => v,
        None => {
            // Wait a bit longer for the thread to drain after release so we
            // don't leave it dangling, then fail the test.
            std::thread::sleep(Duration::from_millis(500));
            panic!(
                "Disc::copy did not return within 5s of stall_secs=2 — \
                 stall guard not wired (TDD red until fix lands)"
            );
        }
    };

    // Cleanup
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    let copy_result = result.expect("copy returns Ok with stall handling");

    assert!(
        elapsed < Duration::from_millis(5000),
        "copy elapsed {elapsed:?} exceeded 5s bound (stall_secs=2)"
    );
    assert!(
        copy_result.bytes_pending > 0,
        "expected bytes_pending > 0 after stall-triggered skip; got {}",
        copy_result.bytes_pending
    );
    assert!(
        !copy_result.complete,
        "expected complete=false after stall-triggered skip"
    );
}

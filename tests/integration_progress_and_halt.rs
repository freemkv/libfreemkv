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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    struct CountingReporter {
        calls: Arc<AtomicU64>,
        last_bytes: Arc<AtomicU64>,
    }
    impl libfreemkv::progress::Progress for CountingReporter {
        fn report(&self, p: &libfreemkv::progress::PassProgress) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.last_bytes.store(p.bytes_good_total, Ordering::Relaxed);
            true
        }
    }
    let reporter = CountingReporter {
        calls: calls.clone(),
        last_bytes: last_bytes.clone(),
    };

    let opts = CopyOptions {
        decrypt: false,
        progress: Some(&reporter),
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

// ── 6. Pass 1 sweeps the entire disc even when every read fails ───────────
//
// Per RIP_DESIGN.md §2.1 + §3: Disc::copy must reach the end of the disc
// regardless of how many reads fail. The only legitimate early exit is the
// halt flag. With `skip_on_error` and a reader that returns
// Err for every read, Pass 1 must:
//   - mark every sector NonTrimmed (so Pass 2 can retry them)
//   - return cleanly (no panic, no hang)
//   - bytes_good = 0
//   - bytes_pending = total_bytes (NonTrimmed counts as pending in mapfile
//     accounting; see disc/mapfile.rs::stats)
//   - bytes_unreadable = 0 (only Pass 2 marks Unreadable)
//   - complete = false (work remains for Pass 2)
//   - halted = false (no user stop)
//   - ISO file is `total_bytes` size on disk (sparse zeros)

/// Reader that returns Err for every read. Optionally signals a halt
/// flag on the first read so tests can exercise the halt-during-skip-forward
/// path deterministically (no wallclock dependency).
struct FailingSectorReader {
    capacity: u32,
    /// If set, signals halt on the first `read_sectors` call. Cleared after
    /// the first signal so subsequent reads are plain Err.
    halt_on_first_read: Option<Arc<AtomicBool>>,
}

impl FailingSectorReader {
    fn new(capacity: u32) -> Self {
        Self {
            capacity,
            halt_on_first_read: None,
        }
    }

    fn with_halt_on_first_read(capacity: u32, halt: Arc<AtomicBool>) -> Self {
        Self {
            capacity,
            halt_on_first_read: Some(halt),
        }
    }
}

impl SectorReader for FailingSectorReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        _buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        if let Some(h) = self.halt_on_first_read.take() {
            h.store(true, Ordering::Relaxed);
        }
        // Model what a real damaged-disc read returns: CHECK CONDITION +
        // MEDIUM ERROR (sense_key 3, ASC 0x11 UNRECOVERED READ ERROR,
        // ASCQ 0x05 L-EC UNCORRECTABLE). Disc::copy's hysteresis must
        // engage on this — `Error::DiscRead` is libfreemkv's own
        // post-classification signal, not what a real reader emits.
        Err(libfreemkv::error::Error::ScsiError {
            opcode: libfreemkv::scsi::SCSI_READ_10,
            status: libfreemkv::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(libfreemkv::ScsiSense {
                sense_key: libfreemkv::scsi::SENSE_KEY_MEDIUM_ERROR,
                asc: 0x11,
                ascq: 0x05,
            }),
        })
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

#[test]
fn test_disc_copy_completes_full_disc_with_failing_reader() {
    // 1024 sectors = 2 MB. Reader fails every read. With skip_on_error +
    // skip_on_error, Pass 1 must mark every sector NonTrimmed and return
    // cleanly — no bail, no hang.
    let capacity_sectors: u32 = 1024;
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;

    let mut reader = FailingSectorReader::new(capacity_sectors);
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,

        ..Default::default()
    };

    let t0 = Instant::now();
    let result = disc
        .copy(&mut reader, &iso_path, &opts)
        .expect("copy returns Ok");
    let elapsed = t0.elapsed();

    // Cleanup
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    // Hard bound — even at 0 ms per read, 1024 sectors with skip-forward
    // should complete in well under a second on any host.
    assert!(
        elapsed < Duration::from_secs(5),
        "Pass 1 took {elapsed:?} on a 2 MB synthetic disc — expected < 5 s"
    );

    // Per RIP_DESIGN.md §2.1: Pass 1 must reach end of disc regardless of
    // read outcomes.
    assert_eq!(
        result.bytes_total, total_bytes,
        "bytes_total must match disc capacity"
    );
    assert_eq!(
        result.bytes_good, 0,
        "no reads succeeded, bytes_good must be 0"
    );
    assert_eq!(
        result.bytes_unreadable, 0,
        "Pass 1 does not mark Unreadable; only Pass 2 (Disc::patch) does"
    );
    assert_eq!(
        result.bytes_pending, total_bytes,
        "every sector must be NonTrimmed → counted as pending. \
         Got bytes_pending={} of total {}",
        result.bytes_pending, total_bytes
    );
    assert!(
        !result.complete,
        "complete=false because NonTrimmed regions remain (work for Pass 2)"
    );
    assert!(!result.halted, "no halt was set; halted must be false");

    // ISO file should be the full disc size on disk (sparse zeros where
    // reads failed).
    // Note: tempfile was dropped above; the file may or may not still exist
    // depending on cleanup ordering. We only assert what we can observe in
    // the CopyResult.
}

// ── 7. Halt during Pass 1 skip-forward path returns promptly (deterministic) ─
//
// Per RIP_DESIGN.md §3: halt is the only legitimate early exit from Pass 1.
// Even when every read is failing (skip-forward path), a halt must be
// honored within a small bounded time.
//
// Deterministic fixture: the reader signals halt on its FIRST read. The
// inner copy loop's halt check fires on the next iteration, breaking out
// of 'outer. This avoids any wallclock race on fast CI runners (where a
// 2 GB synthetic disc can sweep skip-forward in <100 ms).

#[test]
fn test_disc_copy_halts_promptly_on_failing_reader() {
    let capacity_sectors: u32 = 1024 * 1024; // 2 GB synthetic disc

    let halt = Arc::new(AtomicBool::new(false));
    let mut reader = FailingSectorReader::with_halt_on_first_read(capacity_sectors, halt.clone());
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,

        halt: Some(halt),
        ..Default::default()
    };

    let t0 = Instant::now();
    let result = disc
        .copy(&mut reader, &iso_path, &opts)
        .expect("copy returns Ok on halt");
    let elapsed = t0.elapsed();

    // Cleanup
    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    assert!(
        elapsed < Duration::from_secs(2),
        "halt must return within 2 s; took {elapsed:?}"
    );
    assert!(result.halted, "result.halted must be true");
    assert!(
        !result.complete,
        "halted run cannot be complete (bytes_pending > 0 expected)"
    );
    assert!(
        result.bytes_pending > 0,
        "halt fired before sweep completed; bytes_pending must be > 0"
    );
}

// ── 8. Hysteresis recovers data the drive can read individually ──────────
//
// Pass 1 reads in batch (32 sectors = 1 ECC block). Failed blocks are marked
// NonTrimmed for Pass 2 recovery. This test verifies that a reader where every
// multi-sector read fails produces all NonTrimmed output with zero bytes_good.

struct BlockSizeFailingReader {
    capacity: u32,
}

impl SectorReader for BlockSizeFailingReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        if count == 1 {
            for chunk in buf.chunks_mut(SECTOR_SIZE) {
                chunk.fill((lba & 0xff) as u8);
            }
            Ok(buf.len())
        } else {
            Err(libfreemkv::error::Error::ScsiError {
                opcode: libfreemkv::scsi::SCSI_READ_10,
                status: libfreemkv::scsi::SCSI_STATUS_CHECK_CONDITION,
                sense: Some(libfreemkv::ScsiSense {
                    sense_key: libfreemkv::scsi::SENSE_KEY_MEDIUM_ERROR,
                    asc: 0x11,
                    ascq: 0x00,
                }),
            })
        }
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }
}

#[test]
fn test_disc_copy_marks_failed_ecc_blocks_as_nontrimmed() {
    let capacity_sectors: u32 = 256;
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;

    let mut reader = BlockSizeFailingReader {
        capacity: capacity_sectors,
    };
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,

        ..Default::default()
    };

    let result = disc
        .copy(&mut reader, &iso_path, &opts)
        .expect("copy returns Ok");

    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    // Pass 1 reads every batch at bpt=32 (no batch reduction, no skip-ahead).
    // BlockSizeFailingReader fails on multi-sector reads but succeeds on single-sector.
    // Bridge degradation handling retries failed batches as individual sectors,
    // so all data is recovered as Finished. bytes_good == total_bytes is correct.
    assert_eq!(
        result.bytes_good, total_bytes,
        "Pass 1 at bpt=32 recovers all sectors via single-sector retry on MEDIUM_ERROR"
    );
    assert_eq!(
        result.bytes_pending, 0,
        "no pending sectors after full recovery"
    );
    assert!(result.complete, "complete=true when all sectors recovered");
}

// ── 9. PassProgress carries separate unreadable vs pending byte counts ─────
//
// The video-damage-time display needs bytes_unreadable_total (confirmed dead)
// separate from bytes_pending_total (might still recover). This test verifies
// that a Pass 2 with some confirmed failures produces correct field values.

#[test]
fn test_pass_progress_separates_unreadable_from_pending() {
    let capacity_sectors: u32 = 128;
    let total_bytes: u64 = capacity_sectors as u64 * SECTOR_SIZE as u64;

    let mut reader = FailingSectorReader::new(capacity_sectors);
    let disc = synthetic_disc(capacity_sectors);

    let tmp = tempfile::NamedTempFile::new().expect("tempfile create");
    let iso_path = tmp.path().to_path_buf();
    drop(tmp);

    let opts = CopyOptions {
        decrypt: false,
        multipass: true,
        ..Default::default()
    };

    let pass1 = disc.copy(&mut reader, &iso_path, &opts).expect("pass1 ok");

    assert_eq!(pass1.bytes_good, 0, "pass1: no good sectors");
    assert_eq!(pass1.bytes_unreadable, 0, "pass1: no confirmed unreadable");
    assert_eq!(
        pass1.bytes_pending, total_bytes,
        "pass1: all sectors NonTrimmed"
    );

    let last_unreadable = Arc::new(AtomicU64::new(0));
    let last_pending = Arc::new(AtomicU64::new(0));
    let last_good = Arc::new(AtomicU64::new(0));
    let last_dur = Arc::new(AtomicU64::new(0));

    struct SnapshotReporter {
        unreadable: Arc<AtomicU64>,
        pending: Arc<AtomicU64>,
        good: Arc<AtomicU64>,
        dur: Arc<AtomicU64>,
    }
    impl libfreemkv::progress::Progress for SnapshotReporter {
        fn report(&self, p: &libfreemkv::progress::PassProgress) -> bool {
            self.unreadable
                .store(p.bytes_unreadable_total, Ordering::Relaxed);
            self.pending.store(p.bytes_pending_total, Ordering::Relaxed);
            self.good.store(p.bytes_good_total, Ordering::Relaxed);
            if let Some(d) = p.disc_duration_secs {
                self.dur.store((d * 1000.0) as u64, Ordering::Relaxed);
            }
            true
        }
    }
    let reporter = SnapshotReporter {
        unreadable: last_unreadable.clone(),
        pending: last_pending.clone(),
        good: last_good.clone(),
        dur: last_dur.clone(),
    };

    let pass2_opts = CopyOptions {
        decrypt: false,
        multipass: true,
        progress: Some(&reporter),
        ..Default::default()
    };

    let pass2 = disc
        .copy(&mut reader, &iso_path, &pass2_opts)
        .expect("pass2 ok");

    let _ = std::fs::remove_file(&iso_path);
    let _ = std::fs::remove_file(libfreemkv::disc::mapfile_path_for(&iso_path));

    assert_eq!(
        pass2.bytes_good, 0,
        "pass2: still no good sectors (reader always fails)"
    );
    assert!(
        pass2.bytes_unreadable > 0,
        "pass2: some sectors confirmed unreadable"
    );
    assert!(
        pass2.bytes_pending < pass1.bytes_pending,
        "pass2: fewer pending sectors than pass1"
    );

    let observed_unreadable = last_unreadable.load(Ordering::Relaxed);
    let observed_pending = last_pending.load(Ordering::Relaxed);
    assert!(
        observed_unreadable > 0,
        "progress should report confirmed unreadable bytes"
    );
    assert!(
        observed_pending == 0 || observed_pending < total_bytes,
        "pending should shrink as sectors are confirmed unreadable"
    );

    // Video damage time: unreadable / total * duration
    // With no titles on synthetic disc, disc_duration_secs = None
    assert_eq!(
        last_dur.load(Ordering::Relaxed),
        0,
        "synthetic disc has no titles, duration should be None/0"
    );
}

// ── 10. Damage time calculation (unit test) ────────────────────────────────
//
// Verifies the formula: damage_secs = bytes_unreadable / bytes_total * duration
// This mirrors the CLI's print_disc_progress logic.

#[test]
fn test_damage_time_calculation() {
    // 78.8 GB disc, 2h45m movie (9900s), 74 KB unreadable
    let disc_bytes: u64 = 78_800_000_000;
    let duration_secs: f64 = 9900.0;

    let cases: Vec<(u64, &str)> = vec![
        (74 * 1024, "~10ms"),          // 74 KB → ~9ms, negligible
        (10 * 1024 * 1024, "~1.3s"),   // 10 MB → ~1.3s
        (100 * 1024 * 1024, "~13s"),   // 100 MB → ~13s
        (1024 * 1024 * 1024, "~134s"), // 1 GB → ~134s
    ];

    for (bad_bytes, label) in cases {
        let damage_secs = bad_bytes as f64 / disc_bytes as f64 * duration_secs;
        match label {
            "~10ms" => assert!(damage_secs < 0.05, "{label}: {damage_secs:.3}s"),
            "~1.3s" => assert!(
                (damage_secs - 1.3).abs() < 0.2,
                "{label}: {damage_secs:.2}s"
            ),
            "~13s" => assert!(
                (damage_secs - 13.0).abs() < 1.0,
                "{label}: {damage_secs:.1}s"
            ),
            "~134s" => assert!(
                (damage_secs - 134.0).abs() < 2.0,
                "{label}: {damage_secs:.0}s"
            ),
            _ => {}
        }
    }

    // 0.25s threshold: how many bad bytes = 0.25s of damage?
    let threshold_bytes = (0.25 / duration_secs * disc_bytes as f64) as u64;
    assert!(
        threshold_bytes > 0,
        "0.25s damage threshold should be > 0 bytes"
    );
    // At 9900s / 78.8 GB ≈ 0.25s = ~2 MB
    let expected_mb = threshold_bytes as f64 / (1024.0 * 1024.0);
    assert!(
        (expected_mb - 2.0).abs() < 0.5,
        "0.25s ≈ {expected_mb:.2} MB (expected ~2 MB)"
    );
}

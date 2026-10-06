use super::*;
use tempfile::NamedTempFile;

// Helper: build a `WritebackPipeline` over a local tempfile (always
// non-NFS on test rigs), so `is_nfs=false` and `skip_wait` is false
// until the pipeline is explicitly marked degraded.
fn local_pipeline(chunk_bytes: u64) -> (NamedTempFile, WritebackPipeline) {
    let f = NamedTempFile::new().expect("tempfile create");
    let pipeline = WritebackPipeline::new(f.as_file(), 0, chunk_bytes);
    (f, pipeline)
}

// A lost worker ran no WAIT_AFTER: it must not count as a completed (0 ms) wait.
#[test]
fn lost_wait_worker_is_skipped_not_done() {
    fn panicking(_: RawFd, _: u64, _: u64) -> i32 {
        panic!("intentional test panic");
    }
    let out = wait_after_with_timeout(None, -1, 0, 1, panicking, Duration::from_secs(2));
    assert!(matches!(out, WaitOutcome::Skipped));
}

#[test]
fn new_pipeline_starts_active() {
    let (_f, p) = local_pipeline(32 * 1024 * 1024);
    assert!(!p.is_nfs, "local tempfile must not classify as NFS");
    assert!(!p.degraded.load(Ordering::Relaxed));
    assert!(!p.skip_wait(), "fresh local pipeline must not skip wait");
}

#[test]
fn degraded_flag_short_circuits_wait() {
    let (_f, p) = local_pipeline(32 * 1024 * 1024);
    assert!(!p.skip_wait());
    p.degraded.store(true, Ordering::Relaxed);
    assert!(
        p.skip_wait(),
        "degraded flag must force the wait+dontneed bypass"
    );
}

#[test]
fn record_wait_grows_chunk_on_high_p95() {
    let (_f, mut p) = local_pipeline(16 * 1024 * 1024);
    // Fill the window with samples above the grow threshold.
    for _ in 0..ADAPTIVE_WINDOW {
        p.record_wait(ADAPTIVE_GROW_MS + 50);
    }
    assert!(
        p.chunk_bytes > 16 * 1024 * 1024,
        "chunk should have grown; got {}",
        p.chunk_bytes
    );
    assert!(p.chunk_bytes <= CHUNK_BYTES_MAX);
}

#[test]
fn record_wait_shrinks_chunk_on_low_p95() {
    let (_f, mut p) = local_pipeline(64 * 1024 * 1024);
    for _ in 0..ADAPTIVE_WINDOW {
        p.record_wait(1); // well under ADAPTIVE_SHRINK_MS
    }
    assert!(
        p.chunk_bytes < 64 * 1024 * 1024,
        "chunk should have shrunk; got {}",
        p.chunk_bytes
    );
    assert!(p.chunk_bytes >= CHUNK_BYTES_MIN);
}

#[test]
fn record_wait_no_op_below_window_fill() {
    let (_f, mut p) = local_pipeline(16 * 1024 * 1024);
    let initial = p.chunk_bytes;
    // Only push a few samples; window not full → no adaptation.
    for _ in 0..(ADAPTIVE_WINDOW - 1) {
        p.record_wait(ADAPTIVE_GROW_MS + 100);
    }
    assert_eq!(
        p.chunk_bytes, initial,
        "chunk must not change before window is full"
    );
}

// p95 of a full window is its top sample (index 15 of 16): one slow wait among fast
// ones grows the chunk. Uniform samples cannot tell `sorted[15]` from `sorted[0]`.
#[test]
fn record_wait_takes_the_top_sample_as_p95() {
    let (_f, mut p) = local_pipeline(16 * 1024 * 1024);
    for _ in 0..ADAPTIVE_WINDOW - 1 {
        p.record_wait(1);
    }
    p.record_wait(ADAPTIVE_GROW_MS + 50);
    assert_eq!(p.chunk_bytes, 32 * 1024 * 1024);
}

// The window evicts its OLDEST sample: a slow wait stays in the window for exactly
// ADAPTIVE_WINDOW pushes, then leaves and the chunk shrinks.
#[test]
fn record_wait_evicts_the_oldest_sample() {
    let (_f, mut p) = local_pipeline(16 * 1024 * 1024);
    for _ in 0..ADAPTIVE_WINDOW - 1 {
        p.record_wait(1);
    }
    p.record_wait(ADAPTIVE_GROW_MS + 50); // push #16: grows, stays in the window
    for _ in 0..ADAPTIVE_WINDOW - 1 {
        p.record_wait(1); // pushes #17..#31: the slow sample is still inside
    }
    assert_eq!(p.chunk_bytes, CHUNK_BYTES_MAX);
    p.record_wait(1); // push #32 evicts it: all fast, so shrink
    assert_eq!(p.chunk_bytes, CHUNK_BYTES_MAX / 2);
}

#[test]
fn record_wait_clamps_to_chunk_bounds() {
    // Grow past the max.
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MAX);
    for _ in 0..ADAPTIVE_WINDOW {
        p.record_wait(ADAPTIVE_GROW_MS + 1000);
    }
    assert_eq!(p.chunk_bytes, CHUNK_BYTES_MAX, "must clamp to MAX");

    // Shrink past the min.
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    for _ in 0..ADAPTIVE_WINDOW {
        p.record_wait(0);
    }
    assert_eq!(p.chunk_bytes, CHUNK_BYTES_MIN, "must clamp to MIN");
}

#[test]
fn detect_nfs_local_file_is_false() {
    // Local tempfile must not classify as NFS. This locks in the
    // consolidation through `crate::platform::fs_type::detect_fd`.
    let f = NamedTempFile::new().expect("tempfile create");
    use std::os::unix::io::AsRawFd;
    assert!(!detect_nfs(f.as_file().as_raw_fd()));
}

#[test]
fn note_progress_below_chunk_is_noop() {
    let (_f, mut p) = local_pipeline(32 * 1024 * 1024);
    // No-op return before crossing the first chunk boundary.
    let before = p.chunk_count;
    p.note_progress(1024); // < 32 MiB
    assert_eq!(p.chunk_count, before);
    assert!(p.pending.is_none());
}

// ── Bug-fix regression tests ────────────────────────────────────────

fn failing_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    libc::EIO
}

// A failed WAIT_AFTER consumes the file's writeback error, so the pipeline must
// latch it; dropping the rc let the final fsync report success.
#[test]
fn failed_wait_after_is_latched_in_note_progress() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = failing_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    assert!(
        p.error().is_none(),
        "first chunk has no predecessor to wait on"
    );
    p.note_progress(2 * CHUNK_BYTES_MIN);
    assert_eq!(p.error().and_then(|e| e.raw_os_error()), Some(libc::EIO));
    assert!(
        !p.skip_wait(),
        "an I/O error is not a timeout; must not degrade"
    );
}

#[test]
fn failed_wait_after_is_latched_in_finalize() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = failing_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    p.finalize();
    assert_eq!(p.error().and_then(|e| e.raw_os_error()), Some(libc::EIO));
}

fn espipe_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    libc::ESPIPE
}

fn erofs_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    libc::EROFS
}

fn einval_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    libc::EINVAL
}

// EROFS (aborted ext4 journal, btrfs abort) is data loss and must latch.
#[test]
fn erofs_is_latched_einval_is_not() {
    for errno in [libc::ENOMEM, libc::EINTR, libc::EAGAIN] {
        assert!(!is_writeback_errno(errno), "errno {errno} is transient");
    }
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = erofs_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    p.finalize();
    assert_eq!(p.error().and_then(|e| e.raw_os_error()), Some(libc::EROFS));

    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = einval_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    p.finalize();
    assert!(p.error().is_none());
}

static SEEK_WAITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn counting_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    SEEK_WAITS.fetch_add(1, Ordering::SeqCst);
    0
}

// Seeks happen every MKV cluster: they must not block on WAIT_AFTER.
#[test]
fn seek_never_waits() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.note_progress(CHUNK_BYTES_MIN);
    p.note_progress(CHUNK_BYTES_MIN + 4096);
    assert!(p.pending.is_some());
    p.wait_op = counting_wait;
    p.handle_seek(0);
    assert_eq!(
        SEEK_WAITS.load(Ordering::SeqCst),
        0,
        "seek called WAIT_AFTER"
    );
    assert!(
        p.pending.is_some(),
        "the pending chunk waits at a later boundary"
    );
    assert!(p.error().is_none());
}

// A write smaller than one chunk never crosses a boundary; finalize must
// still wait on it, or a small file's writeback error is never seen.
#[test]
fn finalize_waits_on_the_partial_tail() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = failing_wait;
    p.note_progress(4096);
    assert!(p.pending.is_none());
    p.finalize();
    assert_eq!(p.error().and_then(|e| e.raw_os_error()), Some(libc::EIO));
}

static ERRNO_WAITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn eio_then_erofs_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    match ERRNO_WAITS.fetch_add(1, Ordering::SeqCst) {
        0 => libc::EIO,
        _ => libc::EROFS,
    }
}

// The first data-loss errno is the cause; a later one never replaces it.
#[test]
fn the_first_latched_errno_is_kept() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = eio_then_erofs_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    p.note_progress(2 * CHUNK_BYTES_MIN);
    p.note_progress(3 * CHUNK_BYTES_MIN);
    p.finalize();
    assert!(ERRNO_WAITS.load(Ordering::SeqCst) >= 2, "a second wait ran");
    assert_eq!(p.error().and_then(|e| e.raw_os_error()), Some(libc::EIO));
}

fn ok_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    0
}

// Each completed WAIT_AFTER credits exactly the waited chunk's length as durable, at a
// boundary and at finalize.
#[test]
fn completed_waits_credit_the_waited_length() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    let flush = crate::io::flush::FlushProgress::default();
    flush.note_total(10 * CHUNK_BYTES_MIN);
    p.set_flush_progress(flush.clone());
    p.wait_op = ok_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    assert_eq!(flush.bytes_durable(), 0, "nothing waited yet");
    // A second chunk twice as long: the wait on the first credits only its length.
    p.note_progress(3 * CHUNK_BYTES_MIN);
    assert_eq!(flush.bytes_durable(), CHUNK_BYTES_MIN);
    p.finalize();
    assert_eq!(flush.bytes_durable(), 3 * CHUNK_BYTES_MIN);
}

// Only data-loss errnos latch: ESPIPE/EINVAL mean the call was unsupported.
#[test]
fn non_writeback_errno_is_not_latched() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = espipe_wait;
    p.note_progress(CHUNK_BYTES_MIN);
    p.note_progress(2 * CHUNK_BYTES_MIN);
    p.finalize();
    assert!(p.error().is_none());
}

// `/dev/null` (a sweep target) is a char device: sync_file_range rejects it,
// so the pipeline must skip the wait path instead of failing the write.
#[test]
fn char_device_skips_wait_and_never_latches() {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .expect("open /dev/null");
    let mut p = WritebackPipeline::new(&f, 0, CHUNK_BYTES_MIN);
    p.wait_op = failing_wait;
    assert!(p.skip_wait(), "a char device must not be waited on");
    for i in 1..=4 {
        p.note_progress(i * CHUNK_BYTES_MIN);
    }
    p.finalize();
    assert!(p.error().is_none());
}

#[test]
fn regular_file_is_waitable() {
    let f = NamedTempFile::new().expect("tempfile create");
    assert!(fd_is_waitable(f.as_file()));
    let null = std::fs::File::open("/dev/null").expect("open /dev/null");
    assert!(!fd_is_waitable(&null));
}

#[test]
fn successful_wait_after_latches_nothing() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.note_progress(CHUNK_BYTES_MIN);
    p.note_progress(2 * CHUNK_BYTES_MIN);
    p.finalize();
    assert!(p.error().is_none());
}

// Regression for the fd-reuse fix: `new` clones the fd into `wait_file`,
// so `clone_for_worker` gives the worker an owned `File`, not a raw fd.
#[test]
fn wait_file_clone_is_present_for_local_tempfile() {
    let (_f, p) = local_pipeline(32 * 1024 * 1024);
    assert!(
        p.wait_file.is_some(),
        "wait_file must be Some for a normal local tempfile (try_clone should not fail)"
    );
    // clone_for_worker must return Some — the worker will get an
    // owned File, not fall through to the raw-fd fallback.
    let worker_clone = p.clone_for_worker();
    assert!(
        worker_clone.is_some(),
        "clone_for_worker must return Some when wait_file is Some"
    );
}

// Structural: `clone_for_worker`'s `File` has a distinct fd number but
// refers to the same underlying file, which stays open via the OS's
// file-description refcount even after the original tempfile closes.
#[test]
fn worker_clone_has_distinct_fd_from_original() {
    let f = NamedTempFile::new().expect("tempfile create");
    let original_fd = f.as_file().as_raw_fd();
    let pipeline = WritebackPipeline::new(f.as_file(), 0, 32 * 1024 * 1024);

    let clone = pipeline
        .clone_for_worker()
        .expect("clone_for_worker returned None");
    let clone_fd = clone.as_raw_fd();

    // The clone must have a different fd number — it is a separate
    // open file description (dup'd by try_clone).
    assert_ne!(
        clone_fd, original_fd,
        "worker clone must have a distinct fd number from the original"
    );
    // The clone fd must be valid (non-negative on Unix).
    assert!(clone_fd >= 0, "clone fd must be non-negative");
}

fn slow_wait(_fd: RawFd, _off: u64, _len: u64) -> i32 {
    std::thread::sleep(Duration::from_secs(2));
    0
}

/// LP15 / G6 (T11): a `WAIT_AFTER` that outlives its bound degrades the pipeline
/// (no more waits) and never fails a write, `finalize` or `error()`. Guard: per
/// the stop design §3.1 T11, do not change without a design change.
#[test]
fn wait_after_timeout_degrades_never_fails() {
    let (_f, mut p) = local_pipeline(CHUNK_BYTES_MIN);
    p.wait_op = slow_wait;
    p.wait_timeout = Duration::from_millis(30);
    p.note_progress(CHUNK_BYTES_MIN);
    p.note_progress(2 * CHUNK_BYTES_MIN);
    assert!(p.skip_wait(), "a timed-out WAIT_AFTER degrades");
    assert_eq!(p.chunk_bytes, CHUNK_BYTES_MIN);
    p.note_progress(3 * CHUNK_BYTES_MIN);
    p.finalize();
    assert!(p.error().is_none(), "a timeout is never an error");
}

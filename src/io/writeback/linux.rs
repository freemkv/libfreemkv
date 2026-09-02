//! Linux writeback pipeline using `sync_file_range` + `posix_fadvise`.
//!
//! Bounds dirty page cache during large sequential writes: every
//! `chunk_bytes`, kicks async writeback on the just-completed chunk and
//! finalises the previous one via `WAIT_AFTER` + `posix_fadvise(DONTNEED)`.
//! Chunk size adapts to storage speed from a rolling p95 of `WAIT_AFTER`
//! latency, bounded to [4 MiB, 256 MiB]. NFS mounts, and any local storage
//! that times out inside `WAIT_AFTER` (30s), skip the wait+dontneed step.
//!
//! See docs/writeback-linux.md for rationale, NFS handling, and timeout details.

use std::collections::VecDeque;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const ADAPTIVE_WINDOW: usize = 16;
const CHUNK_BYTES_MIN: u64 = 4 * 1024 * 1024;
const CHUNK_BYTES_MAX: u64 = 256 * 1024 * 1024;
const ADAPTIVE_GROW_MS: u64 = 200;
const ADAPTIVE_SHRINK_MS: u64 = 20;
/// Every N chunks, emit a `debug!` snapshot of the current chunk
/// size so operators tailing the log can see where the autoscaler
/// settled.
const SIZE_LOG_INTERVAL: u64 = 32;
/// Hard upper bound on a single `sync_file_range(WAIT_AFTER)` call.
/// Beyond this we declare the pipeline degraded and stop calling
/// WAIT_AFTER for the rest of its life.
const WAIT_AFTER_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct WritebackPipeline {
    /// Aliases the wrapping `WritebackFile::file`. Only valid for the
    /// lifetime of that struct — moving the `File` independently
    /// would silently UAF this fd. The pipeline is a private field of
    /// `WritebackFile` and never exposed outside that wrapper, which
    /// is what keeps the alias sound.
    fd: RawFd,
    /// An owned clone of the file descriptor, held so that any
    /// leaked WAIT_AFTER worker thread retains a valid reference to
    /// the underlying file description for the duration of its
    /// syscall — even if the original `WritebackFile` is closed first
    /// and the OS reuses its fd number. `None` only when `try_clone`
    /// failed at construction (rare); the pipeline falls back to the
    /// pre-clone `fd` integer in that case, which carries the original
    /// fd-reuse risk but is no worse than the previous behaviour.
    wait_file: Option<File>,
    chunk_bytes: u64,
    last_flush_pos: u64,
    pending: Option<(u64, u64)>,
    /// Rolling window of recent `WAIT_AFTER` elapsed_ms measurements.
    wait_after_window: VecDeque<u64>,
    /// Count of chunks emitted (used to space out periodic
    /// `debug!` size snapshots).
    chunk_count: u64,
    /// True when the underlying file is on an NFS mount. NFS makes
    /// WAIT_AFTER unsafe (can block forever on missing server ack), so
    /// we skip it entirely and let the NFS client handle commit on
    /// close.
    is_nfs: bool,
    /// Set the first time WAIT_AFTER exceeds [`WAIT_AFTER_TIMEOUT`].
    /// Once set, behaviour matches the NFS path for the rest of the
    /// pipeline's life. A plain `AtomicBool`: the flag is only ever
    /// touched on the owning thread (the spawned WAIT_AFTER worker never
    /// reads or writes it). `AtomicBool` over `bool` only because the
    /// load/store sites read cleanly; no sharing is needed today.
    degraded: AtomicBool,
}

impl WritebackPipeline {
    // Aliases `file`'s fd; MUST be dropped before `file` itself, or kept
    // inside the same struct that owns `file` — the alias is unchecked.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes: u64) -> Self {
        let fd = file.as_raw_fd();
        let is_nfs = detect_nfs(fd);
        // Clone the fd so any leaked WAIT_AFTER worker thread keeps the
        // file description alive. Log but continue on clone failure.
        let wait_file = match file.try_clone() {
            Ok(f) => Some(f),
            Err(e) => {
                tracing::warn!(
                    target: "mux",
                    "WritebackPipeline fd={fd}: try_clone failed ({e}), WAIT_AFTER workers \
                     will use raw fd (fd-reuse risk on timeout)"
                );
                None
            }
        };
        tracing::info!(
            target: "mux",
            "WritebackPipeline fd={fd} is_nfs={is_nfs} chunk_bytes={chunk_bytes} strategy={}",
            if is_nfs { "nfs-skip-wait" } else { "wait+dontneed" }
        );
        Self {
            fd,
            wait_file,
            chunk_bytes,
            last_flush_pos: start_pos,
            pending: None,
            wait_after_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
            chunk_count: 0,
            is_nfs,
            degraded: AtomicBool::new(false),
        }
    }

    /// True if we should bypass the WAIT_AFTER + DONTNEED finalisation
    /// step. NFS always bypasses; local storage bypasses once the
    /// pipeline has flipped to degraded after a WAIT_AFTER timeout.
    #[inline]
    fn skip_wait(&self) -> bool {
        self.is_nfs || self.degraded.load(Ordering::Relaxed)
    }

    // Fresh per-call `File` clone for the WAIT_AFTER worker so the worker
    // thread keeps the file description alive for the syscall's duration.
    // `None` only if `wait_file` is `None` or the clone itself fails.
    #[inline]
    fn clone_for_worker(&self) -> Option<File> {
        self.wait_file.as_ref().and_then(|f| f.try_clone().ok())
    }

    /// Caller advanced the file position to `pos`. If a chunk boundary
    /// was crossed, kick async writeback for the just-completed chunk
    /// and finalise the previous one.
    pub(crate) fn note_progress(&mut self, pos: u64) {
        if pos < self.last_flush_pos.saturating_add(self.chunk_bytes) {
            return;
        }
        // Byte offsets are unsigned throughout; the signed cast happens only at the
        // libc call boundary where the kernel ABI requires `i64`. `saturating_sub`
        // hardens the line-above guard that `pos >= last_flush_pos`.
        let chunk_off: u64 = self.last_flush_pos;
        let chunk_len: u64 = pos.saturating_sub(self.last_flush_pos);
        let mut wait_ms: u64 = 0;
        let mut fadvise_ms: u64 = 0;
        // Async kickoff for the just-completed chunk runs on every path (NFS,
        // degraded, normal) — non-blocking by spec, an early hint that this
        // range is ready to flush.
        let kickoff_rc = unsafe {
            libc::sync_file_range(
                self.fd,
                chunk_off as i64,
                chunk_len as i64,
                libc::SYNC_FILE_RANGE_WRITE,
            )
        };
        if kickoff_rc != 0 {
            // Non-fatal: the async write-out hint failed, but the data is
            // still in the page cache and will be flushed by later fsync /
            // kernel writeback. Surface it for diagnosability.
            tracing::warn!(
                target: "freemkv::io",
                errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                "sync_file_range(WRITE) kickoff failed"
            );
        }
        if let Some((prev_off, prev_len)) = self.pending.take() {
            if self.skip_wait() {
                // NFS branch (or degraded fallback after a prior timeout): the
                // WAIT_AFTER + DONTNEED dance hangs on NFS — skip it, but still
                // advance `pending` so the next call has a stable cycle.
            } else {
                // Normal local-storage branch with belt-and-braces timeout: if
                // WAIT_AFTER hangs > WAIT_AFTER_TIMEOUT, mark degraded, log loudly,
                // and fall through to the skip path on subsequent calls.
                match wait_after_with_timeout(self.clone_for_worker(), self.fd, prev_off, prev_len)
                {
                    Some(ms) => {
                        wait_ms = ms;
                        let t_fadv = Instant::now();
                        unsafe {
                            libc::posix_fadvise(
                                self.fd,
                                prev_off as i64,
                                prev_len as i64,
                                libc::POSIX_FADV_DONTNEED,
                            );
                        }
                        fadvise_ms = t_fadv.elapsed().as_millis() as u64;
                        self.record_wait(wait_ms);
                    }
                    None => {
                        // Timeout branch: switch to NFS-style skip for the rest of the
                        // pipeline's life. Do NOT call DONTNEED — if WAIT_AFTER hasn't
                        // returned, the pages aren't safely flushed.
                        self.degraded.store(true, Ordering::Relaxed);
                        // Skipping DONTNEED leaves pages resident until close (same
                        // exposure as NFS), so shrink chunk_bytes to the floor rather
                        // than whatever adaptive sizing had grown it to (up to 256 MiB).
                        self.chunk_bytes = CHUNK_BYTES_MIN;
                        tracing::error!(
                            target: "mux",
                            "WritebackPipeline WAIT_AFTER timed out after {}s on chunk off={} len={}, marking writeback degraded (subsequent chunks will skip WAIT_AFTER + DONTNEED, chunk_bytes lowered to floor)",
                            WAIT_AFTER_TIMEOUT.as_secs(),
                            prev_off,
                            prev_len
                        );
                    }
                }
            }
        }
        self.pending = Some((chunk_off, chunk_len));
        self.last_flush_pos = pos;
        self.chunk_count += 1;
        tracing::trace!(
            target: "mux",
            "WritebackPipeline chunk off={} len={} wait_after_ms={wait_ms} fadvise_ms={fadvise_ms} chunk_bytes={} skip_wait={}",
            chunk_off,
            chunk_len,
            self.chunk_bytes,
            self.skip_wait(),
        );
        if self.chunk_count.is_multiple_of(SIZE_LOG_INTERVAL) {
            tracing::debug!(
                target: "mux",
                "WritebackPipeline chunk_bytes={} after {} chunks is_nfs={} degraded={}",
                self.chunk_bytes,
                self.chunk_count,
                self.is_nfs,
                self.degraded.load(Ordering::Relaxed),
            );
        }
    }

    /// Push a new `WAIT_AFTER` measurement into the rolling window
    /// and, if the window is full, adapt `chunk_bytes` based on p95.
    fn record_wait(&mut self, wait_ms: u64) {
        if self.wait_after_window.len() == ADAPTIVE_WINDOW {
            self.wait_after_window.pop_front();
        }
        self.wait_after_window.push_back(wait_ms);
        if self.wait_after_window.len() < ADAPTIVE_WINDOW {
            return;
        }
        // p95 index, derived from window size so it stays valid if ADAPTIVE_WINDOW
        // changes (a hard-coded `[14]` would panic OOB for a window <= 14). For the
        // default 16 this is index 15, i.e. the top sample.
        let mut sorted: Vec<u64> = self.wait_after_window.iter().copied().collect();
        sorted.sort_unstable();
        let p95_idx = (ADAPTIVE_WINDOW * 95).div_ceil(100).min(ADAPTIVE_WINDOW) - 1;
        let p95 = sorted[p95_idx];
        let old = self.chunk_bytes;
        let new = if p95 > ADAPTIVE_GROW_MS && self.chunk_bytes < CHUNK_BYTES_MAX {
            (self.chunk_bytes * 2).min(CHUNK_BYTES_MAX)
        } else if p95 < ADAPTIVE_SHRINK_MS && self.chunk_bytes > CHUNK_BYTES_MIN {
            (self.chunk_bytes / 2).max(CHUNK_BYTES_MIN)
        } else {
            self.chunk_bytes
        };
        if new != old {
            self.chunk_bytes = new;
            tracing::info!(
                target: "mux",
                "WritebackPipeline adaptive chunk_bytes {} -> {} p95_ms={p95}",
                old,
                new
            );
        }
    }

    /// Caller is about to seek away from the current write region.
    /// Drain any in-flight chunk and reset tracking.
    pub(crate) fn handle_seek(&mut self, new_pos: u64) {
        self.finalize();
        self.last_flush_pos = new_pos;
    }

    /// Drain any in-flight chunk. Idempotent. Call before `sync_all()`
    /// or when discarding the pipeline.
    pub(crate) fn finalize(&mut self) {
        if let Some((prev_off, prev_len)) = self.pending.take() {
            tracing::debug!(
                target: "mux",
                "WritebackPipeline finalize chunk off={prev_off} len={prev_len} skip_wait={} is_nfs={} degraded={}",
                self.skip_wait(),
                self.is_nfs,
                self.degraded.load(Ordering::Relaxed),
            );
            if self.skip_wait() {
                // NFS / degraded: skip WAIT_AFTER + DONTNEED. close()
                // / sync_all() handle commit through their normal
                // paths.
                return;
            }
            match wait_after_with_timeout(self.clone_for_worker(), self.fd, prev_off, prev_len) {
                Some(_ms) => unsafe {
                    libc::posix_fadvise(
                        self.fd,
                        prev_off as i64,
                        prev_len as i64,
                        libc::POSIX_FADV_DONTNEED,
                    );
                },
                None => {
                    self.degraded.store(true, Ordering::Relaxed);
                    tracing::error!(
                        target: "mux",
                        "WritebackPipeline finalize WAIT_AFTER timed out after {}s on chunk off={prev_off} len={prev_len}, marking writeback degraded",
                        WAIT_AFTER_TIMEOUT.as_secs(),
                    );
                }
            }
        }
    }
}

// Probe whether `fd` lives on an NFS mount via `crate::platform::fs_type::detect_fd`.
// Fails open: any non-NFS classification (incl. `Unknown` on `fstatfs` error) runs the
// normal local path — WAIT_AFTER_TIMEOUT surfaces a misdetected freeze instead of us.
fn detect_nfs(fd: RawFd) -> bool {
    matches!(
        crate::platform::fs_type::detect_fd(fd),
        crate::platform::fs_type::FsType::Nfs
    )
}

// Runs `sync_file_range(WAIT_AFTER)` on a worker thread, waiting up to
// WAIT_AFTER_TIMEOUT; `Some(elapsed_ms)` on success, `None` on timeout
// (worker leaked). See docs/writeback-linux.md — fd lifetime / fd-reuse safety.
fn wait_after_with_timeout(
    worker_file: Option<File>,
    fallback_fd: RawFd,
    off: u64,
    len: u64,
) -> Option<u64> {
    let started = Instant::now();
    let result = if let Some(owned) = worker_file {
        // Happy path: the closure owns a cloned File that keeps the
        // file description alive until the worker drops it.
        crate::io::bounded::bounded_syscall(None, WAIT_AFTER_TIMEOUT, move || unsafe {
            let fd = owned.as_raw_fd();
            libc::sync_file_range(fd, off as i64, len as i64, libc::SYNC_FILE_RANGE_WAIT_AFTER);
            // `owned` drops here, closing the cloned fd.
        })
    } else {
        // Fallback: try_clone failed at construction; use the raw fd.
        // This carries the pre-fix fd-reuse risk on timeout, but is no
        // regression from the original behaviour.
        crate::io::bounded::bounded_syscall(None, WAIT_AFTER_TIMEOUT, move || unsafe {
            libc::sync_file_range(
                fallback_fd,
                off as i64,
                len as i64,
                libc::SYNC_FILE_RANGE_WAIT_AFTER,
            );
        })
    };
    match result {
        Ok(()) => Some(started.elapsed().as_millis() as u64),
        Err(crate::io::bounded::BoundedError::Timeout)
        | Err(crate::io::bounded::BoundedError::Halted) => None,
        Err(crate::io::bounded::BoundedError::WorkerLost) => {
            // Worker thread spawn failed or panicked before sending. Treat as benign
            // success (no syscall ran), not a degrade trigger — elapsed_ms=0 matches no-op.
            Some(0)
        }
    }
}

#[cfg(test)]
mod tests {
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
}

//! Linux writeback pipeline using `sync_file_range` + `posix_fadvise`.
//!
//! Pathology this fixes: the kernel's default `vm.dirty_ratio` (~20 %
//! of RAM) lets dirty pages accumulate to hundreds of MB during a
//! big sequential write, then bursts a flush at 99 % disk utilisation.
//! While the burst runs, app writes block on the writeback queue —
//! observed empirically as instantaneous speed dropping from ~15 MB/s
//! to ~1 MB/s every ~30 s during a Pass 1 sweep.
//!
//! Strategy: every `chunk_bytes` of new sequential output, kick async
//! writeback (`SYNC_FILE_RANGE_WRITE`) on the just-completed chunk and
//! finalise the *previous* chunk via `WAIT_AFTER` + `posix_fadvise
//! (DONTNEED)`. By the time we finalise, that previous chunk has had
//! a full chunk's worth of work to flush — the wait is near-instant.
//! Dirty cache stays bounded at ~2 × `chunk_bytes` and writes drain
//! continuously instead of in bursts.
//!
//! The chunk size is adaptive: we measure the elapsed time of the
//! `WAIT_AFTER` call over a rolling window of the last 16 chunks and
//! resize the chunk based on the p95. Slow storage (NFS, network
//! shares, HDD) sees larger chunks to amortise per-chunk overhead;
//! fast storage (NVMe) sees smaller chunks to keep cache pressure
//! tight. Bounds: [4 MiB, 256 MiB].
//!
//! ## NFS escape hatch
//!
//! `sync_file_range(WAIT_AFTER)` on an NFS-mounted file can block
//! indefinitely waiting for the server's commit ack. If the server
//! never acks (network partition, server-side hang, slow commit), the
//! syscall never returns and the consumer thread is stuck inside the
//! kernel — `/api/stop` can't reach it because halt is cooperative.
//!
//! When `fstatfs` reports the file lives on an NFS mount
//! (`f_type == NFS_SUPER_MAGIC`), the pipeline skips the WAIT_AFTER +
//! `posix_fadvise(DONTNEED)` dance entirely. NFS clients have their
//! own buffering and commit semantics that handle dirty-page bounds
//! without us forcing the issue. The async `SYNC_FILE_RANGE_WRITE`
//! kickoff still runs (non-blocking by spec) so writeback still gets
//! a nudge.
//!
//! ## Defence in depth: WAIT_AFTER timeout
//!
//! Even on local storage, a degraded disk or odd filesystem driver
//! could in principle wedge inside WAIT_AFTER. Each WAIT_AFTER call
//! runs on a worker thread with a 30s recv_timeout on its result
//! channel. On timeout we log a loud error, set a `degraded` flag,
//! and from then on skip WAIT_AFTER + DONTNEED for the rest of the
//! pipeline's life (same shape as the NFS path). The worker thread
//! is intentionally leaked — it unwinds whenever the syscall
//! eventually returns or the process exits. The mux continues; the
//! original dirty-burst pathology re-emerges but the rip can still
//! finish instead of freezing.

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
    /// Construct a pipeline aliasing `file`'s file descriptor. The
    /// returned `WritebackPipeline` MUST be dropped before `file`
    /// itself, or kept inside the same struct that owns `file` — the
    /// alias is unchecked.
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

    /// Produce a fresh per-call `File` clone for the WAIT_AFTER worker.
    ///
    /// Each call to `wait_after_with_timeout` needs its own owned clone
    /// so the worker thread keeps the file description alive for the
    /// duration of the syscall. We clone from `self.wait_file` (itself a
    /// clone taken at construction) rather than from the original file.
    ///
    /// Returns `None` only if `wait_file` is `None` (construction
    /// try_clone failed) or if the second-level try_clone fails — both
    /// rare; the fallback raw-fd path in `wait_after_with_timeout`
    /// handles that case.
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
        // Byte offsets are unsigned throughout; the signed cast happens
        // only at the libc call boundary where the kernel ABI requires
        // `i64`. `saturating_sub` documents and hardens the line-above
        // guard that `pos >= last_flush_pos`.
        let chunk_off: u64 = self.last_flush_pos;
        let chunk_len: u64 = pos.saturating_sub(self.last_flush_pos);
        let mut wait_ms: u64 = 0;
        let mut fadvise_ms: u64 = 0;
        // Async kickoff for the just-completed chunk runs on every
        // path (NFS, degraded, normal) — it's nominally non-blocking
        // by spec and gives the kernel an early hint that this range
        // is ready to flush.
        unsafe {
            libc::sync_file_range(
                self.fd,
                chunk_off as i64,
                chunk_len as i64,
                libc::SYNC_FILE_RANGE_WRITE,
            );
        }
        if let Some((prev_off, prev_len)) = self.pending.take() {
            if self.skip_wait() {
                // NFS branch (or degraded fallback after a prior
                // timeout): the WAIT_AFTER + DONTNEED dance is what
                // hangs on NFS — skip it. We still advance `pending`
                // so the next call has a stable cycle.
            } else {
                // Normal local-storage branch with belt-and-braces
                // timeout. If WAIT_AFTER hangs > WAIT_AFTER_TIMEOUT
                // we mark the pipeline degraded, log a loud error,
                // and fall through to the skip path on subsequent
                // calls.
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
                        // Timeout branch: switch to NFS-style skip
                        // for the rest of the pipeline's life. Do
                        // NOT call DONTNEED — if WAIT_AFTER hasn't
                        // returned, the pages aren't safely flushed.
                        self.degraded.store(true, Ordering::Relaxed);
                        tracing::error!(
                            target: "mux",
                            "WritebackPipeline WAIT_AFTER timed out after {}s on chunk off={} len={}, marking writeback degraded (subsequent chunks will skip WAIT_AFTER + DONTNEED)",
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
        if self.chunk_count % SIZE_LOG_INTERVAL == 0 {
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
        // p95 index, derived from the window size so it stays valid if
        // ADAPTIVE_WINDOW changes (a hard-coded `[14]` would panic OOB
        // for a window <= 14). For the default 16 this is index 15
        // (ceil(16 * 95 / 100) - 1 = 15), i.e. the top sample.
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

/// Probe whether `fd` lives on an NFS mount. Thin wrapper around
/// [`crate::platform::fs_type::detect_fd`] so writeback policy and
/// general-purpose fs-type classification stay in sync (same magic
/// numbers, same musl-vs-glibc cast handling).
///
/// Fails open: any classification other than NFS counts as "not NFS"
/// (including `Unknown` on `fstatfs` error) — better to run the
/// normal local-storage path on a misdetected NFS mount and surface
/// the freeze loudly via [`WAIT_AFTER_TIMEOUT`] than to needlessly
/// disable writeback bounding on every local file because of a
/// transient stat error.
fn detect_nfs(fd: RawFd) -> bool {
    matches!(
        crate::platform::fs_type::detect_fd(fd),
        crate::platform::fs_type::FsType::Nfs
    )
}

/// Run `sync_file_range(WAIT_AFTER)` on a worker thread and wait up
/// to [`WAIT_AFTER_TIMEOUT`] for it to return. `Some(elapsed_ms)` on
/// success; `None` on timeout. On timeout the worker thread is
/// intentionally leaked — it unwinds whenever the syscall eventually
/// returns or the process exits.
///
/// This delegates to [`crate::io::bounded::bounded_syscall`], the
/// generic worker-thread + `recv_timeout` primitive, and just adapts it
/// to the WAIT_AFTER call shape: it returns `elapsed_ms` instead of the
/// syscall's `()`, and treats `WorkerLost` as a benign no-op to match
/// the original semantics.
///
/// ## fd lifetime / fd-reuse safety
///
/// `worker_file` is an *owned* `File` (produced by `File::try_clone` at
/// pipeline construction). It is moved into the worker closure so the
/// file description stays alive for exactly as long as the worker thread
/// lives — even if the original `WritebackFile` is closed and the OS
/// reuses its fd number before the worker's syscall returns.
///
/// `fallback_fd` is used only when `worker_file` is `None` (i.e. the
/// `try_clone` at construction failed). In that case the worker captures
/// the raw fd integer, which carries the original fd-reuse risk but is
/// no worse than the pre-fix behaviour.
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
            // Worker thread spawn failed or panicked before sending.
            // Treat as a benign success (no syscall ran) rather than
            // a degrade trigger — falling through with elapsed_ms=0
            // matches the no-op behaviour.
            Some(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper: build a `WritebackPipeline` over a local tempfile. On
    /// every test rig (linux dev box, CI) the tempfile lives on a
    /// local FS, so `is_nfs=false` and `skip_wait` returns false until
    /// we explicitly mark the pipeline degraded.
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

    /// Regression for the fd-reuse / use-after-close fix. Verifies that
    /// `WritebackPipeline::new` successfully clones the fd into
    /// `wait_file` (i.e. `try_clone` doesn't fail for a normal
    /// tempfile) and that `clone_for_worker` returns `Some` — meaning
    /// the WAIT_AFTER worker will capture an owned `File` rather than a
    /// raw fd integer.
    ///
    /// A deterministic test for the actual fd-reuse race is not clean to
    /// write (it would require simultaneously closing the original File
    /// and re-opening a new one to steal the fd number while the worker
    /// is mid-syscall, which is inherently racy). This test instead pins
    /// the structural invariant: on a normal local file, the pipeline
    /// holds a valid clone and will give the worker an owned File.
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

    /// Structural: the worker `File` clone returned by `clone_for_worker`
    /// is a distinct file descriptor (different fd number) that refers to
    /// the same underlying file. Closing the original tempfile must not
    /// affect the clone's validity — the OS keeps the file description
    /// alive until all file descriptors referring to it are closed.
    ///
    /// We verify "distinct fd number" and "still usable as a raw fd"
    /// without actually racing a syscall.
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

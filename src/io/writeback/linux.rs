//! Linux writeback pipeline using `sync_file_range` + `posix_fadvise`.
//!
//! Bounds dirty page cache during large sequential writes: every
//! `chunk_bytes`, kicks async writeback on the just-completed chunk and
//! finalises the previous one via `WAIT_AFTER` + `posix_fadvise(DONTNEED)`.
//! Chunk size adapts to storage speed from a rolling p95 of `WAIT_AFTER`
//! latency, bounded to [4 MiB, 256 MiB]. NFS mounts, and any local storage
//! that times out inside `WAIT_AFTER` (30s), skip the wait+dontneed step.

use std::collections::VecDeque;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const ADAPTIVE_WINDOW: usize = 16;
use super::CHUNK_BYTES_MIN;
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
    /// Latest write position; `[last_flush_pos, pos)` is the unflushed tail.
    pos: u64,
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
    /// Set when WAIT_AFTER exceeds [`WAIT_AFTER_TIMEOUT`]; behaves like NFS from then on.
    /// Owning-thread access only.
    degraded: AtomicBool,
    /// False for fds `sync_file_range` rejects (pipes, char devices such as
    /// `/dev/null`): those skip kickoff, WAIT_AFTER and DONTNEED entirely.
    waitable: bool,
    /// First writeback errno a `WAIT_AFTER` reported. Sticky: that call may
    /// consume the file's error state, so a later `fsync` can return 0.
    wb_errno: Option<i32>,
    /// The `WAIT_AFTER` syscall; a seam so tests can inject a writeback error.
    wait_op: WaitOp,
    /// T11's bound on one `WAIT_AFTER` ([`WAIT_AFTER_TIMEOUT`]); a seam for tests.
    wait_timeout: Duration,
    /// Each completed `WAIT_AFTER` is flush progress (§2.10 Linux local row).
    flush: crate::io::flush::FlushProgress,
}

/// `(fd, off, len) -> 0 or errno`.
type WaitOp = fn(RawFd, u64, u64) -> i32;

fn sys_wait_after(fd: RawFd, off: u64, len: u64) -> i32 {
    // SAFETY: a plain syscall on an fd; no user memory is touched.
    let rc = unsafe {
        libc::sync_file_range(fd, off as i64, len as i64, libc::SYNC_FILE_RANGE_WAIT_AFTER)
    };
    if rc == 0 {
        0
    } else {
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO)
    }
}

enum WaitOutcome {
    Done(u64),
    Failed(i32),
    TimedOut,
    // No WAIT_AFTER ran (worker lost): nothing durable, nothing to time.
    Skipped,
}

impl WritebackPipeline {
    // Aliases `file`'s fd; MUST be dropped before `file` itself, or kept
    // inside the same struct that owns `file` — the alias is unchecked.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes: u64) -> Self {
        let fd = file.as_raw_fd();
        let is_nfs = detect_nfs(fd);
        let waitable = fd_is_waitable(file);
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
            pos: start_pos,
            pending: None,
            wait_after_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
            chunk_count: 0,
            is_nfs,
            degraded: AtomicBool::new(false),
            waitable,
            wb_errno: None,
            wait_op: sys_wait_after,
            wait_timeout: WAIT_AFTER_TIMEOUT,
            flush: crate::io::flush::FlushProgress::default(),
        }
    }

    pub(crate) fn set_flush_progress(&mut self, flush: crate::io::flush::FlushProgress) {
        self.flush = flush;
    }

    /// NFS or degraded (`skip_wait`) on a waitable fd: nothing bounds the dirty pages, so
    /// the §2.10 flusher runs.
    pub(crate) fn needs_flusher(&self) -> bool {
        self.waitable && self.skip_wait()
    }

    /// The latched writeback error, if any `WAIT_AFTER` failed.
    pub(crate) fn error(&self) -> Option<std::io::Error> {
        self.wb_errno.map(std::io::Error::from_raw_os_error)
    }

    #[cfg(test)]
    pub(crate) fn inject_error(&mut self, errno: i32) {
        self.wb_errno.get_or_insert(errno);
    }

    // Latch possible data loss; an "unsupported call" errno is only a warning.
    fn latch_error(&mut self, errno: i32, off: u64, len: u64) {
        if !is_writeback_errno(errno) {
            tracing::warn!(
                target: "mux",
                errno,
                "WritebackPipeline WAIT_AFTER rejected on chunk off={off} len={len}"
            );
            return;
        }
        tracing::error!(
            target: "mux",
            errno,
            "WritebackPipeline WAIT_AFTER failed on chunk off={off} len={len}"
        );
        self.wb_errno.get_or_insert(errno);
    }

    fn wait_after(&self, off: u64, len: u64) -> WaitOutcome {
        let worker = self.clone_for_worker();
        wait_after_with_timeout(worker, self.fd, off, len, self.wait_op, self.wait_timeout)
    }

    /// True if we should bypass the WAIT_AFTER + DONTNEED finalisation
    /// step. NFS always bypasses; local storage bypasses once the
    /// pipeline has flipped to degraded after a WAIT_AFTER timeout.
    #[inline]
    fn skip_wait(&self) -> bool {
        !self.waitable || self.is_nfs || self.degraded.load(Ordering::Relaxed)
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
        self.pos = pos;
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
        // Async kickoff for the just-completed chunk runs on every waitable path
        // (NFS, degraded, normal): non-blocking, a hint that the range can flush.
        if self.waitable {
            self.kickoff(chunk_off, chunk_len);
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
                match self.wait_after(prev_off, prev_len) {
                    WaitOutcome::Done(ms) => {
                        wait_ms = ms;
                        self.flush.add_durable(prev_len);
                        let t_fadv = Instant::now();
                        // SAFETY: a valid fd; an advisory call.
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
                    WaitOutcome::Failed(errno) => self.latch_error(errno, prev_off, prev_len),
                    WaitOutcome::Skipped => {}
                    WaitOutcome::TimedOut => {
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

    fn kickoff(&self, chunk_off: u64, chunk_len: u64) {
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

    /// Caller is about to seek away from the current write region. Kicks off
    /// the tail and resets tracking; never waits (MKV seeks every cluster), so
    /// the pending chunk is waited at the next boundary or `finalize`, and an
    /// un-waited range keeps its error for the final fsync.
    pub(crate) fn handle_seek(&mut self, new_pos: u64) {
        let tail_len = self.pos.saturating_sub(self.last_flush_pos);
        if tail_len > 0 && self.waitable {
            self.kickoff(self.last_flush_pos, tail_len);
        }
        self.last_flush_pos = new_pos;
        self.pos = new_pos;
    }

    /// Drain any in-flight chunk. Idempotent. Call before `sync_all()`
    /// or when discarding the pipeline.
    pub(crate) fn finalize(&mut self) {
        // The partial tail below a chunk boundary is waited on too, so a small
        // file or the last partial chunk still reports its writeback error.
        let tail_off = self.last_flush_pos;
        let tail_len = self.pos.saturating_sub(tail_off);
        if tail_len > 0 && !self.skip_wait() {
            self.kickoff(tail_off, tail_len);
        }
        self.last_flush_pos = self.last_flush_pos.max(self.pos);
        let tail = (tail_len > 0).then_some((tail_off, tail_len));
        for (off, len) in [self.pending.take(), tail].into_iter().flatten() {
            self.finalize_range(off, len);
        }
    }

    fn finalize_range(&mut self, prev_off: u64, prev_len: u64) {
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
        match self.wait_after(prev_off, prev_len) {
            WaitOutcome::Done(_ms) => {
                self.flush.add_durable(prev_len);
                // SAFETY: a valid fd; an advisory call.
                unsafe {
                    libc::posix_fadvise(
                        self.fd,
                        prev_off as i64,
                        prev_len as i64,
                        libc::POSIX_FADV_DONTNEED,
                    );
                }
            }
            WaitOutcome::Failed(errno) => self.latch_error(errno, prev_off, prev_len),
            WaitOutcome::Skipped => {}
            WaitOutcome::TimedOut => {
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

// Denylist: these mean the call was unsupported or transiently refused. Every
// other errno (EIO, ENOSPC, EROFS, ESTALE, ENOTCONN, ...) may mean lost data.
fn is_writeback_errno(errno: i32) -> bool {
    !matches!(
        errno,
        libc::EINVAL
            | libc::ESPIPE
            | libc::EBADF
            | libc::ENOSYS
            | libc::EOPNOTSUPP
            | libc::ENOMEM
            | libc::EINTR
            | libc::EAGAIN
    )
}

// Regular files and block devices support `sync_file_range`; anything else
// (or an fstat failure) does not.
fn fd_is_waitable(file: &File) -> bool {
    use std::os::unix::fs::FileTypeExt;
    file.metadata()
        .map(|m| m.file_type().is_file() || m.file_type().is_block_device())
        .unwrap_or(false)
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

// Runs `op` (`sync_file_range(WAIT_AFTER)`) on a worker thread, waiting up to
// WAIT_AFTER_TIMEOUT. On timeout the worker is leaked.
fn wait_after_with_timeout(
    worker_file: Option<File>,
    fallback_fd: RawFd,
    off: u64,
    len: u64,
    op: WaitOp,
    timeout: Duration,
) -> WaitOutcome {
    let started = Instant::now();
    // The owned clone keeps the file description alive until the worker drops it;
    // without one (try_clone failed) the raw fd carries the fd-reuse risk on timeout.
    let result = crate::io::bounded::bounded_syscall(None, timeout, move || {
        let fd = worker_file
            .as_ref()
            .map(|f| f.as_raw_fd())
            .unwrap_or(fallback_fd);
        op(fd, off, len)
    });
    match result {
        Ok(0) => WaitOutcome::Done(started.elapsed().as_millis() as u64),
        Ok(errno) => WaitOutcome::Failed(errno),
        Err(crate::io::bounded::BoundedError::Timeout)
        | Err(crate::io::bounded::BoundedError::Halted) => WaitOutcome::TimedOut,
        // Worker spawn failed or panicked before sending: no syscall ran, so
        // nothing was consumed. Benign, not a degrade trigger.
        Err(crate::io::bounded::BoundedError::WorkerLost) => WaitOutcome::Skipped,
    }
}

#[cfg(test)]
#[path = "linux_tests.rs"]
mod tests;

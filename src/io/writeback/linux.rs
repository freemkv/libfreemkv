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
//! ## NFS — same path, same safety net
//!
//! Earlier revisions of this code unconditionally skipped WAIT_AFTER +
//! `posix_fadvise(DONTNEED)` on NFS-mounted files, on the premise that
//! "NFS clients have their own buffering and commit semantics that
//! handle dirty-page bounds without us forcing the issue." That premise
//! was empirically wrong: on Linux NFS clients dirty pages still
//! accumulate up to `vm.dirty_ratio` (default 20% of RAM, ~6.6 GB on a
//! 32 GB box) before the kernel forces writeback and throttles app
//! writes. Mux throughput on NFS therefore cycled between ~50 MB/s
//! (cache absorbing) and ~10 MB/s (cache draining under throttle) on
//! a ~100 s period — exactly the pathology this pipeline was built to
//! fix, but disabled on the medium where it actually mattered.
//!
//! `is_nfs` is still detected (for logging and observability) but
//! `skip_wait` no longer keys off it. WAIT_AFTER + DONTNEED run on NFS
//! exactly as on local storage. The safety net described below
//! (`WAIT_AFTER_TIMEOUT`) catches the original NFS-hang concern.
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
use std::sync::Arc;
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
    /// pipeline's life. Wrapped in `Arc` only because both this
    /// struct and the spawned worker thread (which itself doesn't
    /// touch the flag) share-via-fd patterns might one day need it;
    /// today it's effectively a single-owner cell — the `Arc` shape
    /// keeps the door open for moving the read side into a worker
    /// without re-plumbing types.
    degraded: Arc<AtomicBool>,
}

impl WritebackPipeline {
    /// Construct a pipeline aliasing `file`'s file descriptor. The
    /// returned `WritebackPipeline` MUST be dropped before `file`
    /// itself, or kept inside the same struct that owns `file` — the
    /// alias is unchecked.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes: u64) -> Self {
        let fd = file.as_raw_fd();
        let is_nfs = detect_nfs(fd);
        tracing::info!(
            target: "mux",
            "WritebackPipeline fd={fd} is_nfs={is_nfs} chunk_bytes={chunk_bytes} strategy=wait+dontneed (falls back to skip if WAIT_AFTER timeouts past {}s)",
            WAIT_AFTER_TIMEOUT.as_secs(),
        );
        Self {
            fd,
            chunk_bytes,
            last_flush_pos: start_pos,
            pending: None,
            wait_after_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
            chunk_count: 0,
            is_nfs,
            degraded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// True if we should bypass the WAIT_AFTER + DONTNEED finalisation
    /// step. The pipeline starts in the normal path on every medium
    /// (NFS, local, etc.) and flips here only if a real
    /// `WAIT_AFTER` call exceeds [`WAIT_AFTER_TIMEOUT`] — at which
    /// point we conclude this particular FS/server combination cannot
    /// safely service WAIT_AFTER and fall back to skip mode for the
    /// rest of the pipeline's life. See module-level comment.
    #[inline]
    fn skip_wait(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    /// Caller advanced the file position to `pos`. If a chunk boundary
    /// was crossed, kick async writeback for the just-completed chunk
    /// and finalise the previous one.
    pub(crate) fn note_progress(&mut self, pos: u64) {
        if pos < self.last_flush_pos.saturating_add(self.chunk_bytes) {
            return;
        }
        let chunk_off = self.last_flush_pos as i64;
        let chunk_len = (pos - self.last_flush_pos) as i64;
        let mut wait_ms: u64 = 0;
        let mut fadvise_ms: u64 = 0;
        // Async kickoff for the just-completed chunk runs on every
        // path (NFS, degraded, normal) — it's nominally non-blocking
        // by spec and gives the kernel an early hint that this range
        // is ready to flush.
        unsafe {
            libc::sync_file_range(self.fd, chunk_off, chunk_len, libc::SYNC_FILE_RANGE_WRITE);
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
                match wait_after_with_timeout(self.fd, prev_off, prev_len) {
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
        self.pending = Some((chunk_off as u64, chunk_len as u64));
        self.last_flush_pos = pos;
        self.chunk_count += 1;
        tracing::trace!(
            target: "mux",
            "WritebackPipeline chunk off={} len={} sync_file_range_ms={wait_ms} fadvise_ms={fadvise_ms} chunk_bytes={} skip_wait={}",
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
        // p95 of 16 samples ≈ sorted[14] (5 % of 16 = 0.8 ≈ 1 above).
        let mut sorted: Vec<u64> = self.wait_after_window.iter().copied().collect();
        sorted.sort_unstable();
        let p95 = sorted[14];
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
            match wait_after_with_timeout(self.fd, prev_off, prev_len) {
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

/// Probe whether `fd` lives on an NFS mount via `fstatfs`. Returns
/// `false` on any error — we fail open, not closed: better to run the
/// normal local-storage path on a misdetected NFS mount (and surface
/// the freeze loudly via the timeout) than to needlessly disable
/// writeback bounding on every local file because of a transient
/// stat error.
fn detect_nfs(fd: RawFd) -> bool {
    // `libc::statfs` is repr(C) with a fixed layout; zeroing is the
    // documented init pattern for the kernel uapi struct.
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstatfs(fd, &mut buf) };
    if rc != 0 {
        let errno = std::io::Error::last_os_error();
        tracing::warn!(
            target: "mux",
            "WritebackPipeline fstatfs(fd={fd}) failed: {errno} — defaulting is_nfs=false",
        );
        return false;
    }
    // `f_type` is signed (`__fsword_t`) on glibc and unsigned
    // (`c_ulong`) on musl. Cast both sides to i64 for a portable
    // comparison. On glibc x86_64 both already are i64 — clippy flags
    // the cast as unnecessary on that target only, but we need it for
    // musl, so silence the lint.
    #[allow(clippy::unnecessary_cast)]
    let f_type = buf.f_type as i64;
    #[allow(clippy::unnecessary_cast)]
    let nfs_magic = libc::NFS_SUPER_MAGIC as i64;
    f_type == nfs_magic
}

/// Run `sync_file_range(WAIT_AFTER)` on a worker thread and wait up
/// to [`WAIT_AFTER_TIMEOUT`] for it to return. `Some(elapsed_ms)` on
/// success; `None` on timeout. On timeout the worker thread is
/// 0.20.6 generalizes the worker-thread + recv_timeout pattern into
/// [`crate::io::bounded::bounded_syscall`]; this helper now just adapts
/// the generic primitive to the WAIT_AFTER call shape (returns elapsed_ms
/// instead of the syscall's `()` return, treats `WorkerLost` as a benign
/// no-op to match the original semantics).
fn wait_after_with_timeout(fd: RawFd, off: u64, len: u64) -> Option<u64> {
    let started = Instant::now();
    match crate::io::bounded::bounded_syscall(None, WAIT_AFTER_TIMEOUT, move || unsafe {
        libc::sync_file_range(fd, off as i64, len as i64, libc::SYNC_FILE_RANGE_WAIT_AFTER);
    }) {
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

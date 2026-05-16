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
//! ## Medium-agnostic by construction
//!
//! `sync_file_range(WAIT_AFTER)` runs on every medium (local FS, NFS,
//! etc.). It bounds the dirty-page set at ~2 × `chunk_bytes` by
//! waiting for each chunk's kernel writeback to complete before
//! advancing. There is no `if is_nfs` branch in the hot path. The
//! safety net described below ([`WAIT_AFTER_TIMEOUT`]) handles wedged
//! filesystems generically: if a particular FS proves unable to ack
//! `WAIT_AFTER` within the deadline, the pipeline flips to `degraded`
//! and skips for the rest of its life. That covers the original
//! NFS-hang concern (network partition, server stuck on commit)
//! without baking the failure mode into a per-FS branch.
//!
//! **No `posix_fadvise(DONTNEED)` after the wait.** Earlier revisions
//! evicted the just-WAIT_AFTER'd range from the page cache on the
//! theory that bounding *dirty* pages required also bounding *clean*
//! pages. That was unnecessary and counterproductive: clean cached
//! pages cost nothing (the kernel reclaims them under LRU when memory
//! is needed) and DONTNEED forces a read-modify-write on any
//! subsequent in-window write — exactly the pattern matroska
//! cluster-size backpatches produce, costing ~40-50% of measured NFS
//! write bandwidth in empirical tests 2026-05-15/16.
//!
//! Medium detection happens *exactly once*, at construction, and
//! produces *exactly one* value: the initial `chunk_bytes` seed for
//! the autotuner. The detector's job is to give the autotuner a
//! decent starting point on cold start; from there p95 of WAIT_AFTER
//! latency drives all subsequent decisions, identically on every
//! medium.
//!
//! ## Defence in depth: WAIT_AFTER timeout
//!
//! A degraded disk, wedged NFS server, or odd FS driver could in
//! principle hang inside WAIT_AFTER. Each call runs on a worker
//! thread with a 30s recv_timeout on its result channel
//! ([`crate::io::bounded::bounded_syscall`]). On timeout the pipeline
//! flips to `degraded`, logs a loud error, and skips WAIT_AFTER for
//! the rest of its life. The worker thread is intentionally leaked —
//! it unwinds whenever the syscall eventually returns or the process
//! exits. The mux continues; the original dirty-burst pathology
//! re-emerges only on the specific (medium, server, FS) combination
//! that actually wedged.

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
    /// What `detect_storage_class` saw at construction. Carried for
    /// diagnostic logging only — *not* keyed off in the hot path.
    storage_class: StorageClass,
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
    ///
    /// `chunk_bytes_hint` is the caller's preferred starting chunk
    /// size; the constructor overrides it with a medium-specific
    /// initial seed if `detect_storage_class` recognises the fd's
    /// filesystem. The autotuner then drives the value from there
    /// based on measured `WAIT_AFTER` latency, identically on every
    /// medium.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes_hint: u64) -> Self {
        let fd = file.as_raw_fd();
        let (class, seed) = detect_storage_class(fd);
        // Medium-aware initial seed: bias toward bigger chunks on
        // slow-commit media so the autotuner doesn't waste a minute
        // climbing from a too-small starting value. A returned seed
        // of 0 means "the detector has no opinion; use the caller's
        // hint". All subsequent tuning is medium-agnostic.
        let chunk_bytes = if seed > 0 { seed } else { chunk_bytes_hint };
        tracing::info!(
            target: "mux",
            "WritebackPipeline fd={fd} storage_class={class:?} chunk_bytes={chunk_bytes}"
        );
        Self {
            fd,
            chunk_bytes,
            last_flush_pos: start_pos,
            pending: None,
            wait_after_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
            chunk_count: 0,
            storage_class: class,
            degraded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// True if we should bypass the WAIT_AFTER finalisation step. The
    /// pipeline only skips after `WAIT_AFTER` has *empirically* hung
    /// past [`WAIT_AFTER_TIMEOUT`] on this particular fd. No per-FS
    /// branch — the failure is detected, not predicted.
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
                // Degraded path (set only after an empirical
                // WAIT_AFTER timeout on this fd). We still advance
                // `pending` so the next call has a stable cycle.
            } else {
                match wait_after_with_timeout(self.fd, prev_off, prev_len) {
                    Some(ms) => {
                        wait_ms = ms;
                        // Deliberately NO `posix_fadvise(DONTNEED)`
                        // here. See module-level docs: evicting clean
                        // pages costs us read-modify-write on
                        // matroska cluster backpatches and gains
                        // nothing — the kernel will reclaim clean
                        // pages under LRU when memory is needed.
                        self.record_wait(wait_ms);
                    }
                    None => {
                        // Timeout branch: this fd's FS or server is
                        // unable to ack WAIT_AFTER. Flip to skip mode
                        // for the rest of the pipeline's life.
                        self.degraded.store(true, Ordering::Relaxed);
                        tracing::error!(
                            target: "mux",
                            "WritebackPipeline WAIT_AFTER timed out after {}s on chunk off={} len={}, marking writeback degraded (subsequent chunks will skip WAIT_AFTER)",
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
        let _ = fadvise_ms;
        tracing::trace!(
            target: "mux",
            "WritebackPipeline chunk off={} len={} sync_file_range_ms={wait_ms} chunk_bytes={} skip_wait={}",
            chunk_off,
            chunk_len,
            self.chunk_bytes,
            self.skip_wait(),
        );
        if self.chunk_count % SIZE_LOG_INTERVAL == 0 {
            tracing::debug!(
                target: "mux",
                "WritebackPipeline chunk_bytes={} after {} chunks storage_class={:?} degraded={}",
                self.chunk_bytes,
                self.chunk_count,
                self.storage_class,
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
                "WritebackPipeline finalize chunk off={prev_off} len={prev_len} skip_wait={} storage_class={:?} degraded={}",
                self.skip_wait(),
                self.storage_class,
                self.degraded.load(Ordering::Relaxed),
            );
            if self.skip_wait() {
                return;
            }
            if wait_after_with_timeout(self.fd, prev_off, prev_len).is_none() {
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

/// Coarse classification of the medium an open file is on. Used only
/// at construction to seed the autotuner with a sensible initial
/// `chunk_bytes`. Never branched on in the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageClass {
    /// `fstatfs.f_type == NFS_SUPER_MAGIC`. Slow-commit; seed a
    /// generous initial chunk so the autotuner doesn't waste cycles
    /// climbing from a too-small value.
    Nfs,
    /// Anything else we successfully stat'd. Local FS, tmpfs, ZFS,
    /// network FS we don't have a constant for, etc. Use the caller's
    /// hint as the seed.
    Other,
    /// `fstatfs` failed. Caller's hint wins.
    Unknown,
}

/// Probe the FS class via `fstatfs` and return both the class label
/// and a recommended `chunk_bytes` seed. `seed == 0` means "no
/// medium-specific recommendation; use the caller's hint." Failure
/// modes default to `Unknown` + seed 0 so the caller's hint applies.
///
/// This is the single medium-detection point in the whole writeback
/// pipeline. Everything else is medium-agnostic.
fn detect_storage_class(fd: RawFd) -> (StorageClass, u64) {
    // `libc::statfs` is repr(C) with a fixed layout; zeroing is the
    // documented init pattern for the kernel uapi struct.
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstatfs(fd, &mut buf) };
    if rc != 0 {
        let errno = std::io::Error::last_os_error();
        tracing::warn!(
            target: "mux",
            "WritebackPipeline fstatfs(fd={fd}) failed: {errno} — defaulting to Unknown",
        );
        return (StorageClass::Unknown, 0);
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
    if f_type == nfs_magic {
        // NFS commit ack typically ~10-30 ms RTT. The autotuner grows
        // by doubling when p95 > 200 ms — so starting at 32 MiB it
        // would never grow on a healthy NFS. Seed at 64 MiB so the
        // commit cadence amortizes properly from the first chunk.
        (StorageClass::Nfs, 64 * 1024 * 1024)
    } else {
        // Local FS / unknown remote FS. Caller's hint (typically 32
        // MiB) is a fine starting point; autotuner adjusts from
        // measured latency.
        (StorageClass::Other, 0)
    }
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

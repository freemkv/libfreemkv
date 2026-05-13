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

use std::collections::VecDeque;
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

const ADAPTIVE_WINDOW: usize = 16;
const CHUNK_BYTES_MIN: u64 = 4 * 1024 * 1024;
const CHUNK_BYTES_MAX: u64 = 256 * 1024 * 1024;
const ADAPTIVE_GROW_MS: u64 = 200;
const ADAPTIVE_SHRINK_MS: u64 = 20;
/// Every N chunks, emit a `debug!` snapshot of the current chunk
/// size so operators tailing the log can see where the autoscaler
/// settled.
const SIZE_LOG_INTERVAL: u64 = 32;

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
}

impl WritebackPipeline {
    /// Construct a pipeline aliasing `file`'s file descriptor. The
    /// returned `WritebackPipeline` MUST be dropped before `file`
    /// itself, or kept inside the same struct that owns `file` — the
    /// alias is unchecked.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes: u64) -> Self {
        Self {
            fd: file.as_raw_fd(),
            chunk_bytes,
            last_flush_pos: start_pos,
            pending: None,
            wait_after_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
            chunk_count: 0,
        }
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
        unsafe {
            libc::sync_file_range(self.fd, chunk_off, chunk_len, libc::SYNC_FILE_RANGE_WRITE);
            if let Some((prev_off, prev_len)) = self.pending.take() {
                let t_wait = Instant::now();
                libc::sync_file_range(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::SYNC_FILE_RANGE_WAIT_AFTER,
                );
                wait_ms = t_wait.elapsed().as_millis() as u64;
                let t_fadv = Instant::now();
                libc::posix_fadvise(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::POSIX_FADV_DONTNEED,
                );
                fadvise_ms = t_fadv.elapsed().as_millis() as u64;
                self.record_wait(wait_ms);
            }
            self.pending = Some((chunk_off as u64, chunk_len as u64));
        }
        self.last_flush_pos = pos;
        self.chunk_count += 1;
        tracing::trace!(
            target: "mux",
            "WritebackPipeline chunk off={} len={} sync_file_range_ms={wait_ms} fadvise_ms={fadvise_ms} chunk_bytes={}",
            chunk_off,
            chunk_len,
            self.chunk_bytes
        );
        if self.chunk_count % SIZE_LOG_INTERVAL == 0 {
            tracing::debug!(
                target: "mux",
                "WritebackPipeline chunk_bytes={} after {} chunks",
                self.chunk_bytes,
                self.chunk_count
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
            unsafe {
                libc::sync_file_range(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::SYNC_FILE_RANGE_WAIT_AFTER,
                );
                libc::posix_fadvise(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
        }
    }
}

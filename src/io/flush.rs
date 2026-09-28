//! Durable flush with measured progress (stop design §2.10, §4.5; T12).
//!
//! No OS reports progress inside one `fsync`, so a flush is made of bounded pieces and
//! every completed piece is progress. A flush fails with `SyncTimeout` (E9056) only after
//! 60 s with no progress (HR1, Q-B: "fail 60s after writing. detect, if writing keep going
//! do nothing."); a Stop ends the wait at once.

// Stubs until the ST-L2 flush commit lands the flusher.
#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::halt::{Halt, Progress};

/// T12's window: this long with no flush progress is `SyncTimeout` (E9056).
pub(crate) const FLUSH_STALL: Duration = Duration::from_secs(60);

/// A shared view of a file's durable-flush progress: the [`Progress`] every accepted
/// write and completed flush bumps, the bytes made durable, and the file length.
/// Clones share the counters; hand one to [`WritebackFile::set_flush_progress`] and
/// read it from another thread.
///
/// [`WritebackFile::set_flush_progress`]: crate::io::WritebackFile::set_flush_progress
#[derive(Clone, Debug, Default)]
pub struct FlushProgress {
    progress: Progress,
    durable: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
}

impl FlushProgress {
    /// Counters that bump `progress` (share it with the pipeline consumer, §2.10 item 3).
    pub fn new(progress: Progress) -> Self {
        Self {
            progress,
            ..Self::default()
        }
    }

    /// The forward-progress counter.
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    /// Bytes made durable (or, on Linux local storage, written out) so far; never more
    /// than [`bytes_total`](Self::bytes_total).
    pub fn bytes_durable(&self) -> u64 {
        let total = self.bytes_total();
        self.durable.load(Ordering::Relaxed).min(total)
    }

    /// The file's length as written so far.
    pub fn bytes_total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    // `n` more bytes are durable: one unit of progress.
    pub(crate) fn add_durable(&self, n: u64) {
        self.durable.fetch_add(n, Ordering::Relaxed);
        self.progress.bump();
    }

    // At least `v` bytes are durable.
    pub(crate) fn durable_at_least(&self, v: u64) {
        if self.durable.fetch_max(v, Ordering::Relaxed) < v {
            self.progress.bump();
        }
    }

    // The file now reaches `len` bytes.
    pub(crate) fn note_total(&self, len: u64) {
        self.total.fetch_max(len, Ordering::Relaxed);
    }
}

/// The flush primitives, per OS; a seam so tests run a flush in milliseconds.
pub(crate) trait FlushOps: Send + Sync + 'static {
    /// Make the data written so far durable: the in-write chunk flush, and the whole-file
    /// piece where there is no range sync.
    fn chunk(&self, file: &File) -> io::Result<()>;
    /// Sync `[off, off + len)` (Linux local); `None` where there is no range sync.
    fn range(&self, file: &File, off: u64, len: u64) -> Option<io::Result<()>>;
    /// The final flush.
    fn finish(&self, file: &File) -> io::Result<()>;
    /// A counter that moves while one flush is blocked (Linux NFS `mountstats`).
    fn sample(&self) -> Option<u64>;
}

/// Every duration and size of the flusher (§2.10), production values by default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushTiming {
    /// T12: no progress for this long is `SyncTimeout`.
    pub(crate) stall: Duration,
    /// A chunk flush slower than this halves the chunk.
    pub(crate) slow_chunk: Duration,
    pub(crate) chunk_start: u64,
    pub(crate) chunk_min: u64,
    /// How often a blocked wait samples [`FlushOps::sample`].
    pub(crate) sample_every: Duration,
}

impl Default for FlushTiming {
    fn default() -> Self {
        Self {
            stall: FLUSH_STALL,
            slow_chunk: Duration::from_secs(15),
            chunk_start: 64 * 1024 * 1024,
            chunk_min: super::writeback::CHUNK_BYTES_MIN,
            sample_every: Duration::from_secs(1),
        }
    }
}

/// [`durable_sync_file`]'s piece sizing (§4.5), production values by default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableTiming {
    pub(crate) stall: Duration,
    /// A piece slower than this halves the window; one under a quarter of it doubles it.
    pub(crate) piece_target: Duration,
    pub(crate) window_start: u64,
    pub(crate) window_min: u64,
    pub(crate) sample_every: Duration,
}

impl Default for DurableTiming {
    fn default() -> Self {
        Self {
            stall: FLUSH_STALL,
            piece_target: Duration::from_secs(2),
            window_start: 64 * 1024 * 1024,
            window_min: 1024 * 1024,
            sample_every: Duration::from_secs(1),
        }
    }
}

/// Make `file` durable, reporting `on_progress(bytes_done, bytes_total)` on every piece
/// that completes (stop design §4.5).
///
/// Pieces are sized to finish in about 2 s: Linux local storage syncs consecutive windows
/// (starting at 64 MiB, halving when a piece takes over 2 s, floor 1 MiB, growing back when
/// fast) and then the file; Linux NFS runs one flush while the mount's `mountstats` write
/// counters report bytes; macOS (`F_FULLFSYNC`) and Windows (`FlushFileBuffers`) report the
/// one call's completion. Fails with `SyncTimeout` (E9056) only after 60 s with no
/// progress; a cancelled `halt` returns `Halted` at once, leaking the worker.
pub fn durable_sync_file(
    file: &File,
    halt: Option<&Halt>,
    on_progress: impl FnMut(u64, u64),
) -> io::Result<()> {
    durable_sync_file_with(
        file,
        halt,
        on_progress,
        os_ops(file),
        DurableTiming::default(),
    )
}

// `durable_sync_file` over `ops`, with its timing as a parameter.
pub(crate) fn durable_sync_file_with(
    file: &File,
    _halt: Option<&Halt>,
    _on_progress: impl FnMut(u64, u64),
    _ops: Arc<dyn FlushOps>,
    _timing: DurableTiming,
) -> io::Result<()> {
    file.sync_all()
}

// The production primitives for `file`.
pub(crate) fn os_ops(_file: &File) -> Arc<dyn FlushOps> {
    Arc::new(OsFlushOps)
}

struct OsFlushOps;

impl FlushOps for OsFlushOps {
    fn chunk(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }
    fn range(&self, _file: &File, _off: u64, _len: u64) -> Option<io::Result<()>> {
        None
    }
    fn finish(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }
    fn sample(&self) -> Option<u64> {
        None
    }
}

// The NFS client's write counter for the NFS mount holding `path` (the longest mount
// point prefix) in a `/proc/self/mountstats` dump: server-write bytes + WRITE + COMMIT ops.
pub(crate) fn parse_mountstats(_text: &str, _path: &str) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests;

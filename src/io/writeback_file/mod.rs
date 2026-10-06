//! `WritebackFile` — a `File` wrapper that drives a continuous
//! [`super::writeback::WritebackPipeline`] so large sequential writes (sweep, patch, mux) don't
//! accumulate unbounded dirty pages before a stalling burst-flush. It implements `Write` and
//! `Seek` so any call site that wrote to a plain `File` can swap in `WritebackFile` unchanged.
//! Platform-specific preallocation/durable-flush primitives live in per-OS sibling modules,
//! dispatched via the cfg-gated `mod` decls below.

mod flusher;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod other;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use other as platform;
#[cfg(target_os = "windows")]
use windows as platform;

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use super::flush::{DurableTiming, FlushOps, FlushProgress, FlushTiming};
use super::writeback::WritebackPipeline;
use crate::halt::Halt;
use flusher::Flusher;

// Linux sync_file_range granularity. 32 MiB measured best on a 1 GbE NFS
// mount (8/64/128 MiB all worse); override via FREEMKV_WRITEBACK_CHUNK_MIB.
const WRITEBACK_CHUNK_BYTES_DEFAULT: u64 = 32 * 1024 * 1024;

// Max accepted FREEMKV_WRITEBACK_CHUNK_MIB: generous, and small enough that
// `n * 1024 * 1024` cannot overflow u64. Out-of-range falls back to default.
const WRITEBACK_CHUNK_MIB_MAX: u64 = 64 * 1024;

fn writeback_chunk_bytes() -> u64 {
    std::env::var("FREEMKV_WRITEBACK_CHUNK_MIB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0 && n <= WRITEBACK_CHUNK_MIB_MAX)
        .map(|n| n * 1024 * 1024)
        .unwrap_or(WRITEBACK_CHUNK_BYTES_DEFAULT)
}

pub struct WritebackFile {
    file: File,
    pipeline: WritebackPipeline,
    pos: u64,
    /// Count of position-moving seeks (for the finalize summary). The MKV muxer
    /// seeks back occasionally (cluster size patching, Cues, Segment header
    /// backpatch); the per-seek DEBUG line is trace-level now, and this rolls
    /// the total into one finalize summary.
    seek_count: u64,
    /// Sum of |delta| over all position-moving seeks, in bytes.
    seek_bytes: u64,
    /// Shared flush counters (§2.10): accepted writes and completed flushes.
    flush: FlushProgress,
    /// Bytes accepted so far (any position): what the flusher must make durable.
    written: u64,
    /// The in-write flusher (§2.10): started at the first write where the OS page cache
    /// has no bounded writeback of its own (Linux NFS/degraded, macOS, Windows).
    flusher: Option<Flusher>,
    /// Whether a flusher applies at all (a regular file); tests force it.
    flushable: bool,
    force_flusher: bool,
    ops: Arc<dyn FlushOps>,
    timing: FlushTiming,
    halt: Option<Halt>,
    /// A size hint reserved extents past EOF on a local FS; released at `sync_all` or drop.
    hinted: bool,
}

impl WritebackFile {
    /// Wrap an open `File`. The current OS file position is queried
    /// once so the pipeline starts tracking from wherever the file
    /// already is (typically 0 for fresh files; non-zero for resumed
    /// or appended files).
    pub fn new(file: File) -> io::Result<Self> {
        let ops = super::flush::os_ops(&file);
        Self::with_ops(file, ops, FlushTiming::default(), false)
    }

    fn with_ops(
        mut file: File,
        ops: Arc<dyn FlushOps>,
        timing: FlushTiming,
        force_flusher: bool,
    ) -> io::Result<Self> {
        let pos = file.stream_position()?;
        let mut pipeline = WritebackPipeline::new(&file, pos, writeback_chunk_bytes());
        let flush = FlushProgress::default();
        pipeline.set_flush_progress(flush.clone());
        flush.note_total(pos);
        let flushable = file.metadata().is_ok_and(|m| m.is_file());
        Ok(Self {
            file,
            pipeline,
            pos,
            seek_count: 0,
            seek_bytes: 0,
            flush,
            written: 0,
            flusher: None,
            flushable,
            force_flusher,
            ops,
            timing,
            halt: None,
            hinted: false,
        })
    }

    /// The token a write blocked on flush backpressure, or [`sync_all`](Self::sync_all),
    /// observes: a cancel returns [`E_HALTED`](crate::error::E_HALTED) at once. `flush`
    /// (a container's `close()`) is not interrupted: a committed close completes.
    pub fn set_halt(&mut self, halt: Halt) {
        self.halt = Some(halt);
    }

    /// Share `flush`'s counters (§2.10 item 3): give it the pipeline consumer's
    /// [`Progress`](crate::halt::Liveness) so a flushing `close()` counts as progress.
    /// Call before the first write.
    pub fn set_flush_progress(&mut self, flush: FlushProgress) {
        flush.note_total(self.flush.bytes_total());
        self.pipeline.set_flush_progress(flush.clone());
        if let Some(f) = &self.flusher {
            f.set_flush_progress(flush.clone());
        }
        self.flush = flush;
    }

    /// This file's flush counters: accepted writes and completed flushes.
    pub fn flush_progress(&self) -> &FlushProgress {
        &self.flush
    }

    // Flusher mode over `ops` whatever the OS, with the timing as a parameter.
    #[cfg(test)]
    pub(crate) fn with_flush_ops(
        file: File,
        ops: Arc<dyn FlushOps>,
        timing: FlushTiming,
    ) -> io::Result<Self> {
        Self::with_ops(file, ops, timing, true)
    }

    // The flusher's current chunk size.
    #[cfg(test)]
    pub(crate) fn chunk_bytes(&self) -> u64 {
        self.flusher
            .as_ref()
            .map_or(self.timing.chunk_min, Flusher::chunk)
    }

    // Start the flusher once this file needs one (Linux: only NFS or a degraded pipeline).
    fn ensure_flusher(&mut self) {
        let wanted = self.force_flusher || (self.flushable && self.pipeline.needs_flusher());
        if self.flusher.is_some() || !wanted {
            return;
        }
        let (ops, flush) = (self.ops.clone(), self.flush.clone());
        match Flusher::spawn(&self.file, ops, self.timing, flush, self.written) {
            Ok(f) => self.flusher = Some(f),
            Err(e) => tracing::warn!(
                target: "freemkv::io",
                error = %e,
                "WritebackFile flusher did not start; flushing at sync_all only"
            ),
        }
    }

    // Before a write: a latched error, then the flusher's backpressure (halt-aware).
    fn before_write(&mut self) -> io::Result<()> {
        self.check_writeback()?;
        self.ensure_flusher();
        match &self.flusher {
            Some(f) => f.wait_room(self.written, self.halt.as_ref()),
            None => Ok(()),
        }
    }

    // After `n` bytes were accepted: progress ("writing"), and a chunk for the flusher.
    fn after_write(&mut self, n: usize) {
        self.pos += n as u64;
        self.written += n as u64;
        self.pipeline.note_progress(self.pos);
        self.flush.note_total(self.pos);
        self.flush.progress().bump();
        if let Some(f) = &self.flusher {
            f.note_written(self.written);
        }
    }

    /// Create a new file at `path` (truncating any existing contents)
    /// and wrap it. Convenience for the common
    /// `File::create(path)` + `WritebackFile::new(file)` pair so callers
    /// don't have to assemble a `File` first.
    ///
    /// Callers that know the target output size should prefer
    /// [`Self::create_with_size_hint`] so the kernel can pre-reserve
    /// extents.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Self::new(file)
    }

    /// Like [`Self::create`] but pre-reserves `size_bytes` of disk space via the platform's
    /// extent-preallocation primitive (Linux `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`;
    /// no-op on platforms without one). The reported file size is unchanged — only the on-disk
    /// extent allocation is preallocated.
    pub fn create_with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = File::create(path)?;
        let reserved = platform::preallocate(&file, size_bytes);
        let mut w = Self::new(file)?;
        w.hinted = reserved;
        Ok(w)
    }

    // Release the KEEP_SIZE reservation past EOF (a same-length truncate); `hinted` is set
    // only on a detected local FS. Skipped once degraded or failed: the setattr could then be
    // a halt-blind full flush. A failure only warns.
    fn release_reservation(&mut self) {
        let clean = self.flusher.is_none() && self.check_writeback().is_ok();
        if !std::mem::take(&mut self.hinted) || self.pipeline.needs_flusher() || !clean {
            return;
        }
        if let Err(e) = self
            .file
            .metadata()
            .and_then(|m| self.file.set_len(m.len()))
        {
            tracing::warn!(target: "mux", error = %e, "WritebackFile size-hint reservation kept");
        }
    }

    /// Open an existing file at `path` for writing (no truncation) and
    /// wrap it. Mirrors `File::open` semantics for the writable case
    /// — used by patch / resume paths that mutate an existing ISO in
    /// place.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).open(path)?;
        Self::new(file)
    }

    /// Drain in-flight writeback, flush the rest, then the final flush
    /// ([`durable_sync_file`](crate::io::durable_sync_file)), in place of `File::sync_all`.
    /// It waits while the flush makes progress and fails with
    /// [`E_SYNC_TIMEOUT`](crate::error::E_SYNC_TIMEOUT) only after 60 s without any (§2.10);
    /// a cancelled [`set_halt`](Self::set_halt) token is [`E_HALTED`](crate::error::E_HALTED)
    /// at once. Also [`E_SYNC_WORKER_LOST`](crate::error::E_SYNC_WORKER_LOST), or the OS error
    /// of a failed chunk writeback (sticky: later writes fail with it too).
    pub fn sync_all(&mut self) -> io::Result<()> {
        if self.seek_count > 0 {
            tracing::debug!(
                target: "mux",
                "WritebackFile finalize: {} seeks, {} bytes seeked total",
                self.seek_count,
                self.seek_bytes
            );
        }
        self.pipeline.finalize();
        let synced = self.drain(self.halt.clone().as_ref()).and_then(|()| {
            let timing = DurableTiming {
                stall: self.timing.stall,
                sample_every: self.timing.sample_every,
                ..DurableTiming::default()
            };
            let flush = &self.flush;
            super::flush::durable_sync_file_with(
                &self.file,
                self.halt.as_ref(),
                |done, _| flush.durable_at_least(done),
                self.ops.clone(),
                timing,
            )
        });
        // A latched writeback error outranks fsync's verdict: the failed
        // WAIT_AFTER consumed it, so fsync can return 0 over lost data.
        match self.pipeline.error() {
            Some(e) => Err(e),
            None => {
                if synced.is_ok() {
                    self.release_reservation();
                }
                synced
            }
        }
    }

    // Everything written handed to the flusher and made durable, if one runs. After a Stop
    // only this file's chunk completions are progress (a close on NFS stays bounded).
    fn drain(&self, abort: Option<&Halt>) -> io::Result<()> {
        match &self.flusher {
            Some(f) => f.drain(self.written, abort, self.halt.as_ref()),
            None => Ok(()),
        }
    }

    fn check_writeback(&self) -> io::Result<()> {
        if let Some(e) = self.pipeline.error() {
            return Err(e);
        }
        self.flusher
            .as_ref()
            .and_then(Flusher::error)
            .map_or(Ok(()), Err)
    }
}

impl Write for WritebackFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.before_write()?;
        let n = self.file.write(buf)?;
        self.after_write(n);
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.before_write()?;
        self.file.write_all(buf)?;
        self.after_write(buf.len());
        Ok(())
    }

    // Drains the in-flight chunk and the flusher so their verdict is known: sinks finished
    // by `flush` alone (m2ts behind a BufWriter) must still see a latched error. Stall-bounded
    // but not halt-aware: this is a container's close, which a Stop does not abandon (§2.5).
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.pipeline.finalize();
        let drained = self.drain(None);
        self.check_writeback().and(drained)
    }
}

impl Seek for WritebackFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let p = self.file.seek(from)?;
        // Only treat seeks that actually move the position as boundaries — sweep does
        // a redundant `seek(Current(pos))` before every write, which shouldn't drain
        // the pipeline on every iteration.
        if p != self.pos {
            // Diagnostic for the NFS mux hang: MKV requires occasional backward seeks
            // (cluster patching, Cues, Segment backpatch), each invalidating writeback
            // chunk tracking. Logging the delta correlates hang offsets with muxer ops.
            let from_pos = self.pos;
            let to_pos = p;
            let delta: i64 = (to_pos as i64).wrapping_sub(from_pos as i64);
            // Per-seek detail is trace-level (L4) — benign and high-frequency.
            // The aggregate (count + total bytes) is logged once at finalize.
            tracing::trace!(
                target: "mux",
                "WritebackFile seek from={from_pos} to={to_pos} delta={delta}"
            );
            self.seek_count += 1;
            self.seek_bytes += delta.unsigned_abs();
            self.pipeline.handle_seek(p);
            self.pos = p;
        }
        Ok(p)
    }
}

impl Drop for WritebackFile {
    fn drop(&mut self) {
        // Run the pipeline's tail finalize (WAIT_AFTER + DONTNEED); otherwise a drop
        // without `sync_all` leaves the trailing chunk in cache. No `self.file.sync_all()`
        // here — `Drop`-triggered fsync would swallow errors; `finalize` is idempotent.
        self.pipeline.finalize();
        if let Some(e) = self.pipeline.error() {
            tracing::error!(target: "mux", error = %e, "WritebackFile dropped with a writeback error");
        }
        // Muxers close via `flush` + drop, never `sync_all`: release the reservation here too.
        self.release_reservation();
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

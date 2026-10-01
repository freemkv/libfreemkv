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
mod tests {
    use super::*;
    use std::io::Read;

    fn read_back(path: &Path) -> Vec<u8> {
        let mut f = File::open(path).unwrap();
        let mut v = Vec::new();
        f.read_to_end(&mut v).unwrap();
        v
    }

    #[test]
    fn write_then_drop_persists_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        {
            let mut w = WritebackFile::create(&p).unwrap();
            w.write_all(b"hello world").unwrap();
            // Drop drains the pipeline tail.
        }
        assert_eq!(read_back(&p), b"hello world");
    }

    #[test]
    fn sync_all_drains_and_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("b.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        for _ in 0..32 {
            w.write_all(&[0x5au8; 1024]).unwrap();
        }
        // After sync_all, the bytes MUST be visible to a separate
        // reader. The pipeline has been finalised and durable-sync has
        // run.
        w.sync_all().unwrap();
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 32 * 1024);
        assert!(bytes.iter().all(|&b| b == 0x5a));
        drop(w);
    }

    #[test]
    fn seek_then_patch_roundtrip() {
        // Write A; seek back; patch with B; read back; the patch lands
        // at the right offset.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        let big = vec![b'A'; 4096];
        w.write_all(&big).unwrap();
        // Seek back to offset 1000 and overwrite 8 bytes.
        w.seek(SeekFrom::Start(1000)).unwrap();
        w.write_all(b"PATCHED!").unwrap();
        w.sync_all().unwrap();
        drop(w);
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 4096);
        assert_eq!(&bytes[1000..1008], b"PATCHED!");
        // Bytes outside the patch are still 'A'.
        assert_eq!(bytes[999], b'A');
        assert_eq!(bytes[1008], b'A');
    }

    #[test]
    fn flush_is_observed_in_order() {
        // `Write::flush` should not panic or reorder; verify the bytes
        // land in order through interleaved flushes.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"one").unwrap();
        w.flush().unwrap();
        w.write_all(b"two").unwrap();
        w.flush().unwrap();
        w.write_all(b"three").unwrap();
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(read_back(&p), b"onetwothree");
    }

    // A writeback error the pipeline latched (Linux: a failed WAIT_AFTER already
    // consumed it, so fsync returns 0) must fail sync_all and every later write.
    #[test]
    fn latched_writeback_error_fails_sync_all_and_later_writes() {
        const EIO: i32 = 5;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("wb-err.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"chunk").unwrap();
        w.pipeline.inject_error(EIO);
        let e = w
            .sync_all()
            .expect_err("sync_all must surface the latched error");
        assert_eq!(e.raw_os_error(), Some(EIO));
        assert!(
            w.write_all(b"more").is_err(),
            "write_all after a latched error"
        );
        assert!(w.write(b"more").is_err(), "write after a latched error");
        assert!(w.sync_all().is_err(), "the error is sticky");
    }

    // Sinks that only flush (m2ts: BufWriter<WritebackFile>, finished by flush)
    // must still see a latched writeback error.
    #[test]
    fn latched_writeback_error_fails_flush_through_bufwriter() {
        const EIO: i32 = 5;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("wb-flush.bin");
        let mut w = std::io::BufWriter::new(WritebackFile::create(&p).unwrap());
        w.write_all(b"frames").unwrap();
        // Buffer drained: the final flush issues no write, only `flush`.
        w.flush().unwrap();
        w.get_mut().pipeline.inject_error(EIO);
        let e = w.flush().expect_err("flush must surface the latched error");
        assert_eq!(e.raw_os_error(), Some(EIO));
    }

    // ── Added hardening tests ───────────────────────────────────────

    // `write` must return the inner File's reported count and advance `pos` by exactly that
    // count (not `buf.len()`).
    #[test]
    fn write_returns_byte_count_and_advances_pos() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("wc.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        let n = w.write(b"twelve bytes").unwrap();
        assert_eq!(n, 12, "write must report bytes written");
        // pos is private; observe it via the public Seek impl's
        // stream_position (which resolves to seek(Current(0))).
        let pos = w.stream_position().unwrap();
        assert_eq!(pos, 12, "pos not advanced by write count");
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(read_back(&p), b"twelve bytes");
    }

    // Redundant seek to the CURRENT position (sweep's `seek(Current(pos))` before every write)
    // must not be treated as a boundary.
    #[test]
    fn seek_to_current_position_is_noop_for_data() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("noop-seek.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"AAAA").unwrap();
        // Seek to the current end (offset 4) — a no-move seek.
        let off = w.seek(SeekFrom::Start(4)).unwrap();
        assert_eq!(off, 4);
        // A no-move seek must not reset the pipeline's chunk tracking.
        assert_eq!(w.seek_count, 0, "redundant seek counted as a boundary");
        w.write_all(b"BBBB").unwrap();
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(
            read_back(&p),
            b"AAAABBBB",
            "redundant seek corrupted contiguous write"
        );
    }

    // `open` (no-truncate) must preserve existing file contents, distinct from `create`'s
    // truncating path.
    #[test]
    fn open_preserves_existing_contents() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("reopen.bin");
        std::fs::write(&p, b"ORIGINAL-CONTENT").unwrap();
        let mut w = WritebackFile::open(&p).unwrap();
        // open() does NOT truncate; pos starts at 0. Overwrite the
        // first 8 bytes only.
        w.write_all(b"PATCHED!").unwrap();
        w.sync_all().unwrap();
        drop(w);
        // First 8 bytes overwritten; the rest of ORIGINAL-CONTENT
        // ("-CONTENT") survives because there was no truncation.
        assert_eq!(read_back(&p), b"PATCHED!-CONTENT");
    }

    // `new` queries stream_position() rather than hardcoding pos=0, so a non-zero starting
    // offset stays in sync.
    #[test]
    fn new_tracks_initial_position() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("pos-init.bin");
        std::fs::write(&p, b"0123456789").unwrap();
        let mut w = WritebackFile::open(&p).unwrap();
        let start = w.stream_position().unwrap();
        assert_eq!(start, 0, "freshly opened file should start at offset 0");
        w.write_all(b"XY").unwrap();
        let after = w.stream_position().unwrap();
        assert_eq!(after, 2, "pos must advance by written length");
    }

    // Seek past EOF then write must create a sparse hole reading back as
    // zeros — standard POSIX semantics forwarded to the inner File.
    #[test]
    fn seek_past_eof_creates_zero_hole() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hole.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"head").unwrap(); // bytes 0..4
        w.seek(SeekFrom::Start(20)).unwrap(); // jump past EOF
        w.write_all(b"tail").unwrap(); // bytes 20..24
        w.sync_all().unwrap();
        drop(w);
        let bytes = read_back(&p);
        assert_eq!(
            bytes.len(),
            24,
            "file should extend to the last written byte"
        );
        assert_eq!(&bytes[0..4], b"head");
        // The 4..20 gap must read back as zeros (sparse hole).
        assert!(bytes[4..20].iter().all(|&b| b == 0), "hole not zero-filled");
        assert_eq!(&bytes[20..24], b"tail");
    }

    // `SeekFrom::End` must resolve against the actual file length; after
    // writing 10 bytes, `seek(End(-2))` lands at offset 8.
    #[test]
    fn seek_from_end_resolves_against_length() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("end-seek.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"0123456789").unwrap();
        let landed = w.seek(SeekFrom::End(-2)).unwrap();
        assert_eq!(landed, 8, "End(-2) of a 10-byte file is offset 8");
        w.write_all(b"XY").unwrap();
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(read_back(&p), b"01234567XY");
    }

    // `create_with_size_hint`'s hint reserves extents only; it must NOT pre-grow the logical
    // file length.
    #[test]
    fn create_with_size_hint_does_not_inflate_logical_length() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hint-len.bin");
        let mut w = WritebackFile::create_with_size_hint(&p, 1024 * 1024).unwrap();
        w.write_all(b"hello").unwrap();
        w.sync_all().unwrap();
        drop(w);
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 5, "size hint must not inflate logical length");
        assert_eq!(&bytes, b"hello");
    }

    // The size-hint reservation past EOF must be released once writing ends.
    #[cfg(target_os = "linux")]
    #[test]
    fn size_hint_reservation_is_released_after_sync() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hint-trim.bin");
        let mut w = WritebackFile::create_with_size_hint(&p, 64 * 1024 * 1024).unwrap();
        w.write_all(b"hello").unwrap();
        w.sync_all().unwrap();
        let blocks = std::fs::metadata(&p).unwrap().blocks();
        assert!(blocks < 1024, "reservation kept: {blocks} 512-byte blocks");
    }

    // The mux path (resolve.rs `writeback_file`): BufWriter over the hinted file, closed by
    // flush + drop only, never `sync_all`. The reservation must still be released.
    #[cfg(target_os = "linux")]
    #[test]
    fn size_hint_reservation_is_released_on_mux_close() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hint-mux.bin");
        let f = WritebackFile::create_with_size_hint(&p, 64 * 1024 * 1024).unwrap();
        let mut w = std::io::BufWriter::with_capacity(64 * 1024, f);
        w.write_all(&[7u8; 100 * 1024]).unwrap();
        w.flush().unwrap();
        drop(w);
        assert_eq!(std::fs::metadata(&p).unwrap().len(), 100 * 1024);
        let blocks = std::fs::metadata(&p).unwrap().blocks();
        assert!(blocks < 4096, "reservation kept: {blocks} 512-byte blocks");
    }

    // The reservation is released only after a successful sync: a failed (or halted) sync
    // must not truncate, as that setattr is a halt-blind full flush on NFS.
    #[test]
    fn failed_sync_keeps_size_hint_reservation() {
        struct FailFinish;
        impl FlushOps for FailFinish {
            fn chunk(&self, _: &File) -> io::Result<()> {
                Ok(())
            }
            fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
                None
            }
            fn finish(&self, _: &File) -> io::Result<()> {
                Err(io::Error::from_raw_os_error(5))
            }
            fn sample(&self) -> Option<u64> {
                None
            }
        }
        let file = tempfile::tempfile().unwrap();
        let mut w =
            WritebackFile::with_flush_ops(file, Arc::new(FailFinish), FlushTiming::default())
                .unwrap();
        w.hinted = true;
        w.write_all(b"partial").unwrap();
        assert!(w.sync_all().is_err(), "the final flush failed");
        assert!(w.hinted, "reservation released before a successful sync");
    }

    // Rebinding the flush counters carries the length so far, makes the new counters the
    // file's own, and moves a running flusher's durable credit onto them.
    #[test]
    fn set_flush_progress_rebinds_every_holder() {
        struct OkOps;
        impl FlushOps for OkOps {
            fn chunk(&self, _: &File) -> io::Result<()> {
                Ok(())
            }
            fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
                None
            }
            fn finish(&self, _: &File) -> io::Result<()> {
                Ok(())
            }
            fn sample(&self) -> Option<u64> {
                None
            }
        }
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"12345").unwrap();
        let timing = FlushTiming {
            chunk_min: 100,
            chunk_max: 100,
            ..FlushTiming::default()
        };
        let mut w = WritebackFile::with_flush_ops(file, Arc::new(OkOps), timing).unwrap();
        w.write_all(&[0u8; 100]).unwrap();
        let fresh = FlushProgress::default();
        w.set_flush_progress(fresh.clone());
        assert_eq!(
            fresh.bytes_total(),
            105,
            "the length so far is carried over"
        );
        w.write_all(&[0u8; 300]).unwrap();
        assert_eq!(w.flush_progress().bytes_total(), 405);
        assert_eq!(fresh.bytes_total(), 405, "writes count on the new counters");
        let start = std::time::Instant::now();
        while fresh.bytes_durable() == 0 && start.elapsed() < std::time::Duration::from_secs(2) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            fresh.bytes_durable() > 0,
            "the flusher credits the new counters"
        );
    }

    // `sync_all` is idempotent: calling it twice, then Drop (also
    // finalizes), must not corrupt data or panic.
    #[test]
    fn double_sync_all_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("double-sync.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"idempotent").unwrap();
        w.sync_all().unwrap();
        w.sync_all().unwrap(); // second call must be safe
        drop(w); // Drop also finalizes
        assert_eq!(read_back(&p), b"idempotent");
    }

    // Pins the WRITEBACK_CHUNK_* constants and the MiB->byte multiply that
    // `writeback_chunk_bytes` relies on.
    #[test]
    fn writeback_chunk_constants_and_conversion() {
        // Default is exactly 32 MiB.
        assert_eq!(WRITEBACK_CHUNK_BYTES_DEFAULT, 32 * 1024 * 1024);
        // Max MiB bound is 64 GiB expressed in MiB, and the byte value
        // it maps to must not overflow u64.
        assert_eq!(WRITEBACK_CHUNK_MIB_MAX, 64 * 1024);
        let max_bytes = (WRITEBACK_CHUNK_MIB_MAX as u128) * 1024 * 1024;
        assert!(
            max_bytes <= u64::MAX as u128,
            "max chunk MiB * 1MiB must fit in u64"
        );
    }

    // All `writeback_chunk_bytes` env-var branches in ONE test (avoids a data race between
    // parallel tests mutating the same env var).
    #[test]
    fn writeback_chunk_env_override_branches() {
        // SAFETY: this is the only test touching this env var, and it
        // sets+reads+clears synchronously within its own body.
        let set = |v: &str| unsafe { std::env::set_var("FREEMKV_WRITEBACK_CHUNK_MIB", v) };
        let clear = || unsafe { std::env::remove_var("FREEMKV_WRITEBACK_CHUNK_MIB") };

        set("8");
        assert_eq!(
            writeback_chunk_bytes(),
            8 * 1024 * 1024,
            "in-range mis-converted"
        );

        set("0");
        assert_eq!(
            writeback_chunk_bytes(),
            WRITEBACK_CHUNK_BYTES_DEFAULT,
            "zero must fall back (n > 0 filter)"
        );

        set("not-a-number");
        assert_eq!(
            writeback_chunk_bytes(),
            WRITEBACK_CHUNK_BYTES_DEFAULT,
            "unparseable must fall back"
        );

        // One past the max: WRITEBACK_CHUNK_MIB_MAX + 1.
        set(&(WRITEBACK_CHUNK_MIB_MAX + 1).to_string());
        assert_eq!(
            writeback_chunk_bytes(),
            WRITEBACK_CHUNK_BYTES_DEFAULT,
            "over-max must fall back (n <= MAX filter)"
        );

        // Exactly at the max boundary is accepted (inclusive bound).
        set(&WRITEBACK_CHUNK_MIB_MAX.to_string());
        assert_eq!(
            writeback_chunk_bytes(),
            WRITEBACK_CHUNK_MIB_MAX * 1024 * 1024,
            "max boundary must be accepted (inclusive)"
        );

        clear();
        // With the var cleared, the default is returned.
        assert_eq!(writeback_chunk_bytes(), WRITEBACK_CHUNK_BYTES_DEFAULT);
    }
}

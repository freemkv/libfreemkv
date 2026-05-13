//! `WritebackFile` — a `File` wrapper whose reason for existing is the
//! bounded-cache writeback pipeline.
//!
//! Why: large sequential writes (sweep, patch, mux on UHD-scale output)
//! left to the kernel's default writeback policy accumulate hundreds of
//! megabytes of dirty pages and then burst-flush, stalling subsequent
//! writes for seconds at a time. `WritebackFile` drives a continuous
//! [`super::writeback::WritebackPipeline`] that on Linux issues
//! incremental `sync_file_range` + `posix_fadvise(DONTNEED)` calls at
//! 32 MB granularity so dirty pages drain at the same rate they're
//! produced. macOS and Windows fall through to a no-op pipeline — their
//! default cache policies have not been shown to exhibit the same
//! pathology for this access pattern.
//!
//! It implements `Write` and `Seek` so any call site that wrote to a
//! plain `File` through those traits (sweep, patch, mux) can swap in
//! `WritebackFile` without touching the body of the loop. The wrapper
//! also tracks the current file position to feed the pipeline with
//! progress + seek boundaries.
//!
//! See `super::writeback::linux` for the underlying pathology and the
//! strategy.

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use super::writeback::WritebackPipeline;

const WRITEBACK_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) struct WritebackFile {
    file: File,
    pipeline: WritebackPipeline,
    pos: u64,
}

impl WritebackFile {
    /// Wrap an open `File`. The current OS file position is queried
    /// once so the pipeline starts tracking from wherever the file
    /// already is (typically 0 for fresh files; non-zero for resumed
    /// or appended files).
    pub(crate) fn new(mut file: File) -> io::Result<Self> {
        let pos = file.stream_position()?;
        let pipeline = WritebackPipeline::new(&file, pos, WRITEBACK_CHUNK_BYTES);
        Ok(Self {
            file,
            pipeline,
            pos,
        })
    }

    /// Create a new file at `path` (truncating any existing contents)
    /// and wrap it. Convenience for the common
    /// `File::create(path)` + `WritebackFile::new(file)` pair so callers
    /// don't have to assemble a `File` first.
    ///
    /// Callers that know the target output size should prefer
    /// [`Self::create_with_size_hint`] so the kernel can pre-reserve
    /// extents.
    #[allow(dead_code)]
    pub(crate) fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Self::new(file)
    }

    /// Like [`Self::create`] but pre-reserves `size_bytes` of disk
    /// space via `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux. The
    /// reported file size is unchanged (writes still grow the file
    /// naturally) — only the on-disk extent allocation is preallocated,
    /// which reduces extent fragmentation on large sequential writes
    /// (mux output, especially on slow storage / NFS).
    ///
    /// On macOS / Windows the size hint is ignored and this is
    /// equivalent to `create`.
    pub(crate) fn create_with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = File::create(path)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            // FALLOC_FL_KEEP_SIZE = 0x01 — keep the reported file
            // size at 0 (writes grow it normally) while still
            // pre-reserving the extents.
            let rc = unsafe {
                libc::fallocate(
                    file.as_raw_fd(),
                    libc::FALLOC_FL_KEEP_SIZE,
                    0,
                    size_bytes as i64,
                )
            };
            tracing::debug!(
                target: "mux",
                "WritebackFile fallocate size_hint={size_bytes} rc={rc} ok={}",
                rc == 0
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            tracing::debug!(
                target: "mux",
                "WritebackFile fallocate size_hint={size_bytes} skipped (non-linux)"
            );
        }
        Self::new(file)
    }

    /// Open an existing file at `path` for writing (no truncation) and
    /// wrap it. Mirrors `File::open` semantics for the writable case
    /// — used by patch / resume paths that mutate an existing ISO in
    /// place.
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).open(path)?;
        Self::new(file)
    }

    /// Drain in-flight writeback then issue a full fsync. Use this in
    /// place of `File::sync_all`.
    pub(crate) fn sync_all(&mut self) -> io::Result<()> {
        self.pipeline.finalize();
        self.file.sync_all()
    }
}

impl Write for WritebackFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write(buf)?;
        self.pos += n as u64;
        self.pipeline.note_progress(self.pos);
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all(buf)?;
        self.pos += buf.len() as u64;
        self.pipeline.note_progress(self.pos);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for WritebackFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let p = self.file.seek(from)?;
        // Only treat seeks that actually move the position as
        // boundaries — sweep does a redundant `seek(Current(pos))`
        // before every write, and we don't want that to drain the
        // pipeline on every iteration.
        if p != self.pos {
            self.pipeline.handle_seek(p);
            self.pos = p;
        }
        Ok(p)
    }
}

impl Drop for WritebackFile {
    fn drop(&mut self) {
        // Run the pipeline's tail finalize so the last in-flight chunk
        // gets its `WAIT_AFTER` + `posix_fadvise(DONTNEED)`. Without
        // this, callers that drop a `WritebackFile` without calling
        // `sync_all` (panic, early-return, idiomatic `let _ = w;`)
        // leave the trailing chunk in cache; the kernel still flushes
        // on close, but the bounded-cache invariant fails at the tail.
        // We deliberately do *not* call `self.file.sync_all()` here —
        // close already triggers a flush, and an `fsync` from `Drop`
        // would silently swallow its `io::Error` anyway. `finalize` is
        // idempotent so an explicit `sync_all` followed by drop is
        // still safe.
        self.pipeline.finalize();
    }
}

//! `Writer` — a drop-in `File` wrapper that keeps the kernel page
//! cache bounded during large sequential output.
//!
//! Implements `Write` and `Seek`, so any call site that uses `File`
//! through those traits (sweep, mux, patch) can swap to `Writer`
//! without touching the body of the loop. The wrapper tracks the
//! current file position and forwards each write to a
//! [`super::writeback::WritebackPipeline`], which on Linux schedules
//! incremental `sync_file_range` + `posix_fadvise(DONTNEED)` calls
//! to drain dirty pages continuously instead of letting the kernel
//! burst-flush hundreds of MB at a time.
//!
//! See `super::writeback::linux` for the pathology and the strategy.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use super::writeback::WritebackPipeline;

const CHUNK_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) struct Writer {
    file: File,
    pipeline: WritebackPipeline,
    pos: u64,
}

impl Writer {
    /// Wrap an open `File`. The current OS file position is queried
    /// once so the pipeline starts tracking from wherever the file
    /// already is (typically 0 for fresh files; non-zero for resumed
    /// or appended files).
    pub(crate) fn new(mut file: File) -> io::Result<Self> {
        let pos = file.stream_position()?;
        let pipeline = WritebackPipeline::new(&file, pos, CHUNK_BYTES);
        Ok(Self {
            file,
            pipeline,
            pos,
        })
    }

    /// Drain in-flight writeback then issue a full fsync. Use this in
    /// place of `File::sync_all`.
    pub(crate) fn sync_all(&mut self) -> io::Result<()> {
        self.pipeline.finalize();
        self.file.sync_all()
    }
}

impl Write for Writer {
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

impl Seek for Writer {
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

//! `LocalFileSink` — `BufWriter<File>` for the common local-disk case.
//!
//! 4 MiB internal `BufWriter` coalesces small per-PES muxer writes into
//! kernel-page-aligned flushes. `Seek` flushes the buffer first so a
//! seek can't leapfrog buffered data and corrupt the file — required
//! for MKV's seek-back patches (cluster size, Cues, segment header).
//!
//! [`SequentialSink`](super::SequentialSink) is implemented explicitly
//! so `finish()` flushes and `fsync`s even through a `dyn` trait object;
//! [`RandomAccessSink`](super::RandomAccessSink) is over the `Seek` impl.
// See docs/local-file-sink.md — buffering & seek-flush rationale.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use super::preallocate;
use super::{RandomAccessSink, SequentialSink};

const BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// Random-access write sink for local disks.
///
/// Wraps a `BufWriter<File>` with a 4 MiB internal buffer and forwards
/// `Write`/`Seek` so any call site that previously held a `File` or
/// `WritebackFile` can drop this in. `finish()` flushes the buffer and
/// `sync_all`s the underlying file before the caller drops it.
///
/// Opens the file `create + truncate + read + write`; `read` allows
/// reusing the handle for a post-mux verification re-read. On Linux,
/// `Self::with_size_hint` additionally pre-reserves extents via `fallocate`.
pub struct LocalFileSink {
    inner: BufWriter<File>,
}

impl LocalFileSink {
    /// Open `path` for writing, truncating any existing contents.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            inner: BufWriter::with_capacity(BUFFER_BYTES, file),
        })
    }

    /// Like [`Self::create`] but additionally calls the per-OS
    /// preallocate path with `size_bytes`. On Linux this is
    /// `fallocate(FALLOC_FL_KEEP_SIZE)` so the on-disk extents are
    /// reserved up front (reducing fragmentation for big sequential
    /// muxer output); on other OSes it is a no-op today. Failures
    /// from the preallocate call are non-fatal — the file is still
    /// returned, just without the size reservation.
    pub fn with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        preallocate::preallocate(&file, size_bytes);
        Ok(Self {
            inner: BufWriter::with_capacity(BUFFER_BYTES, file),
        })
    }

    /// Drain the internal buffer and `fsync` the underlying file.
    /// Idempotent with `Drop` (the `BufWriter` also flushes on drop;
    /// this call additionally surfaces fsync errors to the caller).
    /// [`SequentialSink::finish`](super::SequentialSink::finish)
    /// delegates here so the durable flush happens through a trait
    /// object too.
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        self.inner.get_ref().sync_all()
    }
}

impl SequentialSink for LocalFileSink {
    /// Flush the 4 MiB `BufWriter` and `fsync` the file. Overriding the
    /// trait default is what makes a `dyn SequentialSink` / `dyn
    /// RandomAccessSink` `finish()` actually durable instead of a no-op.
    fn finish(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

impl RandomAccessSink for LocalFileSink {}

impl Write for LocalFileSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for LocalFileSink {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        // Flush before seeking so buffered bytes land at the offset
        // they were written for, not the new one.
        self.inner.flush()?;
        self.inner.get_mut().seek(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn write_seek_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rt.bin");
        let mut s = LocalFileSink::create(&p).unwrap();
        s.write_all(b"AAAA").unwrap();
        s.write_all(b"BBBB").unwrap();
        // Seek back over the second word and overwrite.
        s.seek(SeekFrom::Start(4)).unwrap();
        s.write_all(b"CCCC").unwrap();
        s.sync_all().unwrap();
        drop(s);

        let mut f = File::open(&p).unwrap();
        let mut got = Vec::new();
        f.read_to_end(&mut got).unwrap();
        assert_eq!(&got[..], b"AAAACCCC");
    }

    #[test]
    fn drop_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("drop.bin");
        {
            let mut s = LocalFileSink::create(&p).unwrap();
            s.write_all(b"buffered").unwrap();
            // No explicit flush / sync_all — BufWriter drop runs the
            // flush and the file should land on disk.
        }
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(&bytes[..], b"buffered");
    }

    #[test]
    fn with_size_hint_creates_writable_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sz.bin");
        let mut s = LocalFileSink::with_size_hint(&p, 64 * 1024).unwrap();
        s.write_all(b"hint-ok").unwrap();
        s.sync_all().unwrap();
        drop(s);
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(&bytes[..], b"hint-ok");
    }

    // ── Added hardening tests ───────────────────────────────────────

    // `create` must truncate: pre-seed a long file, recreate via the sink
    // with a shorter payload, and confirm the old tail is gone.
    #[test]
    fn create_truncates_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("trunc.bin");
        std::fs::write(&p, vec![0xFFu8; 4096]).unwrap();
        let mut s = LocalFileSink::create(&p).unwrap();
        s.write_all(b"short").unwrap();
        s.sync_all().unwrap();
        drop(s);
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(
            bytes.len(),
            5,
            "create must truncate the pre-existing 4096 bytes"
        );
        assert_eq!(&bytes, b"short");
    }

    // Seek must flush the BufWriter first so buffered bytes land at their
    // intended offset, not the post-seek one. Removing the flush in `seek`
    // would corrupt the file by writing the buffered bytes at the wrong offset.
    #[test]
    fn seek_flushes_buffer_before_moving() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("seek-flush.bin");
        let mut s = LocalFileSink::create(&p).unwrap();
        // These bytes sit in the 4 MiB BufWriter, unflushed.
        s.write_all(b"HEAD").unwrap();
        // Seek forward to offset 10; the buffered HEAD must be flushed
        // to offset 0 BEFORE the position moves.
        s.seek(SeekFrom::Start(10)).unwrap();
        s.write_all(b"TAIL").unwrap();
        s.sync_all().unwrap();
        drop(s);
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(
            &bytes[0..4],
            b"HEAD",
            "buffered head landed at the wrong offset"
        );
        assert_eq!(&bytes[10..14], b"TAIL");
    }
}

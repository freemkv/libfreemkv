//! `WritebackFile` — a `File` wrapper that drives a continuous
//! [`super::writeback::WritebackPipeline`] so large sequential writes
//! (sweep, patch, mux) don't accumulate unbounded dirty pages before a
//! stalling burst-flush. It implements `Write` and `Seek` so any call
//! site that wrote to a plain `File` can swap in `WritebackFile`
//! unchanged. Platform-specific preallocation/durable-flush primitives
//! live in per-OS sibling modules, dispatched via the cfg-gated `mod`
//! decls below — see docs/writeback-file.md for the full rationale.

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

use super::writeback::WritebackPipeline;

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
}

impl WritebackFile {
    /// Wrap an open `File`. The current OS file position is queried
    /// once so the pipeline starts tracking from wherever the file
    /// already is (typically 0 for fresh files; non-zero for resumed
    /// or appended files).
    pub fn new(mut file: File) -> io::Result<Self> {
        let pos = file.stream_position()?;
        let pipeline = WritebackPipeline::new(&file, pos, writeback_chunk_bytes());
        Ok(Self {
            file,
            pipeline,
            pos,
            seek_count: 0,
            seek_bytes: 0,
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
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Self::new(file)
    }

    /// Like [`Self::create`] but pre-reserves `size_bytes` of disk space
    /// via the platform's extent-preallocation primitive (Linux
    /// `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`; no-op on platforms
    /// without one). The reported file size is unchanged — only the
    /// on-disk extent allocation is preallocated. See
    /// docs/writeback-file.md for rationale.
    pub fn create_with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = File::create(path)?;
        platform::preallocate(&file, size_bytes);
        Self::new(file)
    }

    /// Open an existing file at `path` for writing (no truncation) and
    /// wrap it. Mirrors `File::open` semantics for the writable case
    /// — used by patch / resume paths that mutate an existing ISO in
    /// place.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).open(path)?;
        Self::new(file)
    }

    /// Drain in-flight writeback then issue a full fsync, in place of
    /// `File::sync_all`. Bounded by
    /// [`crate::io::bounded::bounded_syscall`] on Linux/macOS (60 s), so
    /// a wedged NFS server cannot trap the caller indefinitely;
    /// `Ok(())` is a durability barrier. Failure is
    /// [`E_SYNC_TIMEOUT`](crate::error::E_SYNC_TIMEOUT),
    /// [`E_HALTED`](crate::error::E_HALTED), or
    /// [`E_SYNC_WORKER_LOST`](crate::error::E_SYNC_WORKER_LOST); see docs/writeback-file.md.
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
        platform::durable_sync(&self.file)
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

impl super::sink::SequentialSink for WritebackFile {
    // Same work as sync_all(); implemented explicitly (no blanket impl) so
    // a `dyn SequentialSink`/`dyn RandomAccessSink` finish() finalises +
    // fsyncs instead of hitting a no-op default.
    fn finish(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

impl super::sink::RandomAccessSink for WritebackFile {}

impl Drop for WritebackFile {
    fn drop(&mut self) {
        // Run the pipeline's tail finalize (WAIT_AFTER + DONTNEED); otherwise a drop
        // without `sync_all` leaves the trailing chunk in cache. No `self.file.sync_all()`
        // here — `Drop`-triggered fsync would swallow errors; `finalize` is idempotent.
        self.pipeline.finalize();
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

    // finish() through a `dyn RandomAccessSink` must dispatch to
    // WritebackFile's override (finalize + durable_sync), not a no-op.
    #[test]
    fn finish_through_trait_object_persists() {
        use crate::io::sink::RandomAccessSink;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("finish-dyn.bin");
        let w = WritebackFile::create(&p).unwrap();
        let mut boxed: Box<dyn RandomAccessSink> = Box::new(w);
        boxed.write_all(b"durable-tail").unwrap();
        boxed.finish().unwrap();
        assert_eq!(read_back(&p), b"durable-tail");
    }

    // ── Added hardening tests ───────────────────────────────────────

    // `write` must return the inner File's reported count and advance
    // `pos` by exactly that count (not `buf.len()`). See docs/writeback-file.md.
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

    // Redundant seek to the CURRENT position (sweep's `seek(Current(pos))`
    // before every write) must not be treated as a boundary. See
    // docs/writeback-file.md.
    #[test]
    fn seek_to_current_position_is_noop_for_data() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("noop-seek.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"AAAA").unwrap();
        // Seek to the current end (offset 4) — a no-move seek.
        let off = w.seek(SeekFrom::Start(4)).unwrap();
        assert_eq!(off, 4);
        w.write_all(b"BBBB").unwrap();
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(
            read_back(&p),
            b"AAAABBBB",
            "redundant seek corrupted contiguous write"
        );
    }

    // `open` (no-truncate) must preserve existing file contents, distinct
    // from `create`'s truncating path. See docs/writeback-file.md.
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

    // `new` queries stream_position() rather than hardcoding pos=0, so a
    // non-zero starting offset stays in sync. See docs/writeback-file.md.
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

    // `create_with_size_hint`'s hint reserves extents only; it must NOT
    // pre-grow the logical file length. See docs/writeback-file.md.
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
    // `writeback_chunk_bytes` relies on. See docs/writeback-file.md.
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

    // All `writeback_chunk_bytes` env-var branches in ONE test (avoids a
    // data race between parallel tests mutating the same env var). See
    // docs/writeback-file.md for the branch list.
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

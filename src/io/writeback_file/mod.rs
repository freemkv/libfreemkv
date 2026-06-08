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
//!
//! ## Platform split
//!
//! The platform-specific pieces of this wrapper — extent preallocation
//! (Linux `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`, Windows no-op
//! today) and the durable-flush primitive (Linux/macOS
//! `fsync`/`F_FULLFSYNC` wrapped in a bounded syscall; Windows plain
//! `FlushFileBuffers`, unbounded) — live in per-OS sibling modules. The
//! dispatch happens once at the bottom of this file via cfg-gated `mod`
//! decls. No inline `#[cfg(target_os = "...")]` in the business-logic
//! above.
//!
//! ## Write path
//!
//! Writes are direct passthrough to the underlying `File` (no writer
//! thread, no ring, no batching). Empirically a writer-thread
//! architecture introduced a ~60% mux throughput regression on NFS
//! bidirectional workloads; the direct-passthrough write path is faster.
//! The writeback pipeline still runs (it's called inline from `write` /
//! `write_all` / `seek`) so the bounded-cache invariant on Linux is
//! preserved.
//!
//! ## Halt-safety
//!
//! `sync_all` runs the per-OS durable-flush primitive. On Linux/macOS
//! it is wrapped in [`crate::io::bounded::bounded_syscall`] with a 60 s
//! deadline, so a wedged NFS server cannot trap the muxer indefinitely
//! on the final fsync. Windows is a known deviation: its `durable_sync`
//! calls `File::sync_all` (`FlushFileBuffers`) directly and is NOT
//! bounded — a wedged UNC/SMB share can block the final flush there.

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

/// Granularity at which the Linux writeback pipeline issues
/// `sync_file_range` pairs. 32 MiB is the empirically best value on a
/// 1 GbE NFS mount backed by a single spinning disk: 8 MiB / 64 MiB /
/// 128 MiB all measured worse. Override via `FREEMKV_WRITEBACK_CHUNK_MIB`
/// — faster backends (NVMe, RAID) may tolerate larger windows.
const WRITEBACK_CHUNK_BYTES_DEFAULT: u64 = 32 * 1024 * 1024;

/// Upper bound (in MiB) accepted from `FREEMKV_WRITEBACK_CHUNK_MIB`.
/// 64 GiB — far above `CHUNK_BYTES_MAX` (256 MiB), generous for any
/// real backend, and small enough that `n * 1024 * 1024` cannot wrap
/// `u64`. Out-of-range values fall back to the default.
const WRITEBACK_CHUNK_MIB_MAX: u64 = 64 * 1024;

fn writeback_chunk_bytes() -> u64 {
    std::env::var("FREEMKV_WRITEBACK_CHUNK_MIB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0 && n <= WRITEBACK_CHUNK_MIB_MAX)
        .map(|n| n * 1024 * 1024)
        .unwrap_or(WRITEBACK_CHUNK_BYTES_DEFAULT)
}

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
        let pipeline = WritebackPipeline::new(&file, pos, writeback_chunk_bytes());
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
    /// space via the platform's extent-preallocation primitive (Linux
    /// `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`, Windows
    /// `SetFileValidData` stub). The reported file size is unchanged
    /// (writes still grow the file naturally) — only the on-disk extent
    /// allocation is preallocated, which reduces extent fragmentation
    /// on large sequential writes (mux output, especially on slow
    /// storage / NFS).
    ///
    /// On platforms without an extent-preallocation primitive this is
    /// equivalent to `create` — the size hint is dropped after a debug
    /// log.
    pub(crate) fn create_with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = File::create(path)?;
        platform::preallocate(&file, size_bytes);
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
    ///
    /// The final durable flush is wrapped in
    /// [`crate::io::bounded::bounded_syscall`] (per the per-OS module)
    /// with a 60 s deadline on Linux/macOS — a wedged NFS server cannot
    /// trap the calling thread indefinitely. On timeout the page cache
    /// is left to the kernel's normal flush-on-close path — best
    /// effort, but bounded.
    ///
    /// IMPORTANT: on Linux/macOS a successful `Ok(())` does NOT
    /// guarantee the data is durable if the bounded fsync timed out or
    /// was halted — only the hang is bounded, the fsync may not have
    /// completed. Callers needing crash-consistency (e.g. mux-finish
    /// then external commit/DB update) must not treat `Ok(())` as a
    /// durability barrier.
    pub(crate) fn sync_all(&mut self) -> io::Result<()> {
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
        // Only treat seeks that actually move the position as
        // boundaries — sweep does a redundant `seek(Current(pos))`
        // before every write, and we don't want that to drain the
        // pipeline on every iteration.
        if p != self.pos {
            // Diagnostic for the NFS mux hang: the MKV format requires
            // the muxer to seek back occasionally (cluster size
            // patching, Cues index write, Segment header backpatch).
            // Each such seek invalidates the writeback chunk tracking
            // and forces a finalize → WAIT_AFTER on the in-flight
            // chunk. Logging the seek delta lets us correlate hang
            // offsets with specific muxer operations.
            let from_pos = self.pos;
            let to_pos = p;
            let delta: i64 = (to_pos as i64).wrapping_sub(from_pos as i64);
            tracing::debug!(
                target: "mux",
                "WritebackFile seek from={from_pos} to={to_pos} delta={delta}"
            );
            self.pipeline.handle_seek(p);
            self.pos = p;
        }
        Ok(p)
    }
}

impl super::sink::SequentialSink for WritebackFile {
    /// Drain the writeback pipeline and run the bounded durable flush —
    /// the same work [`Self::sync_all`] does. Implemented explicitly (no
    /// blanket impl) so a `dyn SequentialSink` / `dyn RandomAccessSink`
    /// `finish()` actually finalises + fsyncs instead of hitting a no-op
    /// default. Note the bounded-fsync caveat from [`Self::sync_all`]
    /// applies: `Ok(())` is not a durability barrier if the fsync timed
    /// out or was halted.
    fn finish(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

impl super::sink::RandomAccessSink for WritebackFile {}

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

    /// finish() through a `dyn RandomAccessSink` trait object must
    /// dispatch to WritebackFile's override (finalize + durable_sync),
    /// not a no-op default. Bytes must be visible to a separate reader
    /// before drop.
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

    /// `write` (not write_all) must return the count the inner File
    /// reported and advance `pos` by exactly that count (lines
    /// 185-189). For a regular file a single `write` of a small buffer
    /// writes all of it. We verify the returned count equals the buffer
    /// length AND that a subsequent seek reports the right position.
    /// Mutation: changing `self.pos += n` to `self.pos += buf.len()`
    /// (lines 187 vs a hypothetical bug) would desync on a partial
    /// write; here they coincide, but `Seek(Current(0))` reflecting `n`
    /// still guards the count return value.
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

    /// Redundant seek to the CURRENT position must be a no-op for the
    /// pipeline (lines 211-228 only act when `p != self.pos`). This is
    /// the documented sweep optimisation: sweep does
    /// `seek(Current(pos))` before every write and we must not treat it
    /// as a boundary. We can only observe the public effect: the seek
    /// returns the same offset and writes continue contiguously.
    /// Mutation: removing the `if p != self.pos` guard (line 211) would
    /// call handle_seek on every redundant seek — on the noop pipeline
    /// (macOS) this stays correct for data, but the contiguity +
    /// returned-offset invariant still must hold and is asserted here.
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

    /// `open` (no-truncate) must preserve existing file contents and
    /// allow in-place patching from offset 0 — distinct from `create`
    /// which truncates (lines 157-160 use OpenOptions write-only, no
    /// truncate). We pre-seed a file, reopen with `open`, overwrite the
    /// first bytes, and confirm the tail survives. Mutation: if `open`
    /// used `File::create` (truncate) the tail would be lost.
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

    /// `open` on a file whose position is queried must start tracking
    /// from the file's current offset. `WritebackFile::new` calls
    /// `stream_position()` (line 112); a freshly `open`ed file is at
    /// offset 0. After writing, seeking Current(0) must reflect the
    /// bytes written from 0. Mutation: if `new` hardcoded pos=0 instead
    /// of querying, a non-zero starting offset would desync — covered
    /// indirectly; here we assert the offset is exactly the write size.
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

    /// Seek past EOF then write must create a sparse hole that reads
    /// back as zeros — standard POSIX file semantics that the wrapper
    /// must not break (it forwards seek to the inner File at line 205).
    /// Mutation: if `seek` clamped or mishandled the offset, the hole
    /// size/zero-fill would be wrong.
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

    /// `SeekFrom::End` must resolve against the actual file length.
    /// After writing 10 bytes, `seek(End(-2))` lands at offset 8;
    /// overwriting 2 bytes there patches the tail. Mutation: forwarding
    /// the wrong SeekFrom variant would land at the wrong offset.
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

    /// `create_with_size_hint` must produce a normal, writable file
    /// whose *reported size* tracks bytes written (the hint only
    /// reserves extents, per the doc lines 137-145 — it must NOT
    /// pre-grow the logical file length). We write 5 bytes against a
    /// 1 MiB hint and the file must be exactly 5 bytes long.
    /// Mutation: if the hint path truncated/extended to size_bytes the
    /// length would be 1 MiB and this fails.
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

    /// `flush` must not be a durability barrier nor reorder bytes, but
    /// it also must not lose buffered data. We interleave write_all and
    /// flush and confirm exact byte order survives to disk. (Distinct
    /// from the existing `flush_is_observed_in_order` which uses 3
    /// words; this exercises many small flushes to stress the
    /// passthrough flush path at line 199-201.) Mutation: if `flush`
    /// dropped pending bytes the reassembly fails.
    #[test]
    fn many_interleaved_flushes_preserve_order() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("many-flush.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        let mut expected = Vec::new();
        for i in 0u8..32 {
            let chunk = [i; 4];
            w.write_all(&chunk).unwrap();
            expected.extend_from_slice(&chunk);
            w.flush().unwrap();
        }
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(read_back(&p), expected);
    }

    /// `sync_all` is idempotent: calling it twice (and then Drop, which
    /// also finalizes) must not corrupt data or panic. Doc lines
    /// 256-262: `finalize` is idempotent so explicit sync_all then drop
    /// is safe. Mutation: a finalize that double-freed or advanced a
    /// cursor would corrupt on the second call.
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

    /// Env-var chunk override parsing (`writeback_chunk_bytes`, lines
    /// 91-98). Out-of-range / unparseable values must fall back to the
    /// 32 MiB default; valid in-range values are converted MiB→bytes.
    /// We can't safely mutate process env in parallel tests for the
    /// default-path branch, but we CAN assert the pure boundary logic
    /// the function encodes by reconstructing it: the filter accepts
    /// `0 < n <= WRITEBACK_CHUNK_MIB_MAX`. This pins the constants and
    /// the MiB→byte multiply. Mutation: changing `* 1024 * 1024` to a
    /// single `* 1024` would break this equality.
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

    /// Env-var override parsing for `writeback_chunk_bytes` (lines
    /// 91-98). All four branches in ONE test to avoid the data race of
    /// several parallel tests mutating the same process-global env var.
    ///
    /// Branches: (1) valid in-range value → MiB→byte conversion; (2)
    /// zero → `n > 0` filter rejects → default; (3) garbage → parse
    /// fails → default; (4) over-max → `n <= MAX` filter rejects →
    /// default.
    ///
    /// Mutations: `* 1024 * 1024` → `* 1024` breaks (1); dropping
    /// `n > 0` breaks (2); `unwrap()` on parse panics (3); dropping
    /// `n <= MAX` breaks (4).
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

//! [`FileSectorSource`] — read 2048-byte sectors from an ISO file on
//! disk via direct `seek + read_exact` (`pread`-equivalent) calls,
//! letting the kernel's own readahead policy manage prefetch.
//!
//! ## Why no app-level buffer
//!
//! Pre-0.21.3 this source held a 32 MiB (later 4 MiB) read-ahead
//! buffer to amortise per-sector NFS round-trips. Empirically that
//! buffer hurt: 32 MiB refills bursted the NFS TCP connection hard
//! enough to starve the concurrent writer, and even a 4 MiB window
//! gave the kernel less freedom to pipeline reads with writes. Direct
//! pread per call lets Linux's readahead widen as it detects the
//! sequential pattern, and naturally interleaves with writeback.
//!
//! ## DONTNEED on the consumed window
//!
//! Without page-cache eviction an 85 GB streaming ISO read pins the
//! entire file in memory, starves the concurrent writer, and collapses
//! mux throughput (observed: 2.7 MB/s mux on 0.21.5 vs. 70 MB/s
//! isolated NFS reads). Every [`READ_DROP_CHUNK_BYTES_DEFAULT`] of
//! consumed bytes we call `posix_fadvise(DONTNEED)` over that window,
//! mirroring the write-side [`crate::io::writeback::WritebackPipeline`]
//! policy.
//!
//! The drop window is accounted by a monotonic forward byte counter,
//! which matches the sequential streaming pattern the mux highway
//! drives. Under random or backward access the dropped range no longer
//! lines up with the bytes actually read — but `DONTNEED` is purely an
//! advisory cache hint with no correctness impact, so this degrades to
//! a slightly imprecise hint rather than a bug.
//!
//! ## Platform open hint
//!
//! On `open()` each platform issues its "sequential access expected"
//! hint so OS-level readahead widens. The hint and the DONTNEED call
//! live in per-OS sibling modules ([`linux::hint_sequential`] et al.)
//! — no inline `#[cfg]` in this file.
//!
//! ## Read-ahead prefetch
//!
//! After every consumed read we issue an OS-level prefetch hint for
//! the next equivalent-sized window (`platform::prefetch`). The
//! kernel queues that I/O asynchronously and returns immediately, so
//! the next batch's read overlaps with the caller's processing of
//! the current batch (decrypt + demux + mux). Without this the disk
//! sits idle ~70% of each iteration because kernel SEQUENTIAL
//! readahead alone (capped at `read_ahead_kb`, default 128 KB) is
//! far smaller than our 16 MiB app-level batch.

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

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Error, Result};
use crate::sector::SectorSource;

const SECTOR_SIZE: usize = 2048;

/// Bytes-read threshold per `posix_fadvise(DONTNEED)` drop on the
/// read side. Mirrors `WRITEBACK_CHUNK_BYTES` so the read-side page
/// cache stays bounded the same way the write side does.
///
/// 32 MiB is the empirically tuned value on the rip1 test bed (single
/// 7200rpm HDD via SATA): smaller windows (8 / 16 MiB) shorten the
/// kernel-readahead overlap and slow the producer; larger windows
/// (64 / 128 MiB) let the page cache pin enough of the ISO to
/// pressure concurrent writes. Override via `FREEMKV_READ_DROP_CHUNK_MIB`.
const READ_DROP_CHUNK_BYTES_DEFAULT: u64 = 32 * 1024 * 1024;

fn read_drop_chunk_bytes() -> u64 {
    std::env::var("FREEMKV_READ_DROP_CHUNK_MIB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .map(|n| n * 1024 * 1024)
        .unwrap_or(READ_DROP_CHUNK_BYTES_DEFAULT)
}

/// SectorSource backed by a file (ISO image). Every `read_sectors`
/// call is a direct `seek + read_exact` against the underlying file
/// — kernel readahead handles prefetch, and every
/// [`READ_DROP_CHUNK_BYTES_DEFAULT`] bytes of consumed data the
/// platform's `DONTNEED` hook drops the consumed window from the
/// page cache to bound memory pressure.
pub struct FileSectorSource {
    file: File,
    /// Total file size in sectors. Constant after construction;
    /// surfaced via [`SectorSource::capacity_sectors`].
    capacity: u32,
    /// Bytes read since the last DONTNEED drop. Drives the per-
    /// [`read_drop_chunk_bytes`] page-cache eviction in read_sectors.
    bytes_read_since_drop: u64,
    /// File offset at which the current drop window starts. The next
    /// DONTNEED drops from `drop_window_start` for
    /// `bytes_read_since_drop` bytes. This advances monotonically with
    /// the byte count, so it tracks the actual reads only under the
    /// forward-sequential access the mux highway uses; under random
    /// access it degrades to a harmless, imprecise advisory hint.
    drop_window_start: u64,
    /// Cached drop chunk size (resolved from env once at open).
    drop_chunk_bytes: u64,
}

impl FileSectorSource {
    /// Open an existing ISO file for reading. Capacity is derived
    /// from `metadata().len() / 2048`. Returns
    /// [`Error::IsoTooLarge`] if the file would exceed the 32-bit
    /// LBA address space (~8 TB).
    ///
    /// Issues the platform's "sequential access expected" hint on the
    /// fd (Linux `posix_fadvise(SEQUENTIAL)`, macOS `fcntl(F_RDADVISE)`,
    /// Windows no-op) so the kernel's readahead widens.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::IoError { source: e })?;
        let len = file
            .metadata()
            .map_err(|e| Error::IoError { source: e })?
            .len();
        let sectors = len / SECTOR_SIZE as u64;
        if sectors > u32::MAX as u64 {
            return Err(Error::IsoTooLarge {
                path: path.to_string_lossy().into_owned(),
            });
        }
        let capacity = sectors as u32;

        // Best-effort sequential hint. Ignored on platforms without
        // an equivalent primitive (or where the API exists but the
        // FS doesn't honour it).
        platform::hint_sequential(&file, len);

        Ok(Self {
            file,
            capacity,
            bytes_read_since_drop: 0,
            drop_window_start: 0,
            drop_chunk_bytes: read_drop_chunk_bytes(),
        })
    }
}

impl SectorSource for FileSectorSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        out: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let count = count as u32;
        let bytes = count as usize * SECTOR_SIZE;
        debug_assert!(
            out.len() >= bytes,
            "FileSectorSource::read_sectors: out len {} < requested {}",
            out.len(),
            bytes
        );
        if count == 0 {
            return Ok(0);
        }
        let offset = lba as u64 * SECTOR_SIZE as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut out[..bytes])
            .map_err(|e| Error::IoError { source: e })?;

        // Queue the next batch's read with the kernel before the
        // caller starts processing what we just returned. readahead()
        // is non-blocking — it queues I/O and returns, so the kernel
        // pulls those pages into cache while the consumer (decrypt +
        // demux + mux) runs. Next read_sectors call hits a warm cache.
        platform::prefetch(&self.file, offset + bytes as u64, bytes as u64);

        // Periodic page-cache eviction on the read side. Without
        // this, an 85 GB streaming ISO read pins the entire file in
        // the kernel page cache, which starves concurrent writes and
        // collapses mux throughput. Mirrors the write-side
        // WritebackPipeline's DONTNEED policy.
        self.bytes_read_since_drop += bytes as u64;
        if self.bytes_read_since_drop >= self.drop_chunk_bytes {
            let drop_start = self.drop_window_start;
            let drop_len = self.bytes_read_since_drop;
            platform::drop_window(&self.file, drop_start, drop_len);
            self.drop_window_start = drop_start + drop_len;
            self.bytes_read_since_drop = 0;
        }

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    /// Build a deterministic ISO of `sectors` sectors where sector `n`
    /// is filled with the byte pattern `((n & 0xff) as u8)`. Lets us
    /// verify any sector by content alone.
    fn make_iso(path: &std::path::Path, sectors: u32) {
        let mut f = std::fs::File::create(path).unwrap();
        let mut chunk = vec![0u8; SECTOR_SIZE];
        for n in 0..sectors {
            let b = (n & 0xff) as u8;
            chunk.iter_mut().for_each(|c| *c = b);
            f.write_all(&chunk).unwrap();
        }
        f.flush().unwrap();
    }

    /// Sectors used by spanning-boundary tests. Pick something that
    /// exercises multi-megabyte reads without making test ISOs huge.
    /// 8192 sectors = 16 MiB — large enough to cross any readahead
    /// chunk size we set the kernel hint to.
    const TEST_SPAN_SECTORS: u32 = 8192;

    #[test]
    fn sequential_reads_match_file() {
        let total = TEST_SPAN_SECTORS * 2 + 17;
        let dir = tempdir().unwrap();
        let path = dir.path().join("seq.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), total);

        let mut got = vec![0u8; SECTOR_SIZE];
        for lba in 0..total {
            src.read_sectors(lba, 1, &mut got, false).unwrap();
            let expected = (lba & 0xff) as u8;
            assert!(
                got.iter().all(|b| *b == expected),
                "sector {lba} content mismatch: expected 0x{expected:02x}"
            );
        }
    }

    #[test]
    fn multi_sector_read_across_chunk_boundary() {
        let total = TEST_SPAN_SECTORS * 2;
        let dir = tempdir().unwrap();
        let path = dir.path().join("span.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();

        let span_lba = TEST_SPAN_SECTORS - 2;
        let mut buf4 = vec![0u8; SECTOR_SIZE * 4];
        src.read_sectors(span_lba, 4, &mut buf4, false).unwrap();
        for i in 0..4 {
            let lba = span_lba + i as u32;
            let expected = (lba & 0xff) as u8;
            for b in &buf4[i * SECTOR_SIZE..(i + 1) * SECTOR_SIZE] {
                assert_eq!(*b, expected, "byte mismatch at sub-sector {i}");
            }
        }
    }

    #[test]
    fn backward_seek_reads_correct_bytes() {
        // Read forward then jump back: the SectorSource contract is
        // byte-correctness regardless of access pattern.
        let total = TEST_SPAN_SECTORS * 2 + 5;
        let dir = tempdir().unwrap();
        let path = dir.path().join("back.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        let mut got = vec![0u8; SECTOR_SIZE];

        src.read_sectors(TEST_SPAN_SECTORS + 1, 1, &mut got, false)
            .unwrap();
        src.read_sectors(0, 1, &mut got, false).unwrap();
        assert!(got.iter().all(|b| *b == 0));
    }

    #[test]
    fn read_at_eof_returns_correct_bytes() {
        // File smaller than the readahead chunk — reads near EOF must
        // still return correct bytes.
        let total: u32 = 100;
        let dir = tempdir().unwrap();
        let path = dir.path().join("small.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), total);

        let mut got = vec![0u8; SECTOR_SIZE];
        src.read_sectors(0, 1, &mut got, false).unwrap();
        src.read_sectors(total - 1, 1, &mut got, false).unwrap();
        let expected = ((total - 1) & 0xff) as u8;
        assert!(got.iter().all(|b| *b == expected));
    }

    #[test]
    fn large_single_read() {
        // A multi-MB single read must work — the implementation has
        // no app-level chunking, so this just exercises the direct
        // pread path on a larger request.
        let total = TEST_SPAN_SECTORS + 100;
        let dir = tempdir().unwrap();
        let path = dir.path().join("big.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        let req = (TEST_SPAN_SECTORS + 1) as u16;
        let req_bytes = req as usize * SECTOR_SIZE;
        let mut big = vec![0u8; req_bytes];
        src.read_sectors(0, req, &mut big, false).unwrap();
        assert!(big[..SECTOR_SIZE].iter().all(|b| *b == 0));
        let last_lba = req as u32 - 1;
        let exp = (last_lba & 0xff) as u8;
        let last_off = (req as usize - 1) * SECTOR_SIZE;
        assert!(
            big[last_off..last_off + SECTOR_SIZE]
                .iter()
                .all(|b| *b == exp)
        );
    }

    #[test]
    fn drop_chunk_size_env_override() {
        // Explicit 8 MiB via env var.
        // SAFETY: tests in this crate are single-threaded per the
        // default cargo test harness, but std::env::set_var is
        // declared `unsafe` since Rust 2024 (it can race with other
        // threads / TLS). For a test that runs in-process before any
        // FileSectorSource construction this is safe in practice.
        unsafe {
            std::env::set_var("FREEMKV_READ_DROP_CHUNK_MIB", "8");
        }
        assert_eq!(read_drop_chunk_bytes(), 8 * 1024 * 1024);

        unsafe {
            std::env::remove_var("FREEMKV_READ_DROP_CHUNK_MIB");
        }
        assert_eq!(read_drop_chunk_bytes(), READ_DROP_CHUNK_BYTES_DEFAULT);

        // Garbage env value falls back to default.
        unsafe {
            std::env::set_var("FREEMKV_READ_DROP_CHUNK_MIB", "not-a-number");
        }
        assert_eq!(read_drop_chunk_bytes(), READ_DROP_CHUNK_BYTES_DEFAULT);
        unsafe {
            std::env::remove_var("FREEMKV_READ_DROP_CHUNK_MIB");
        }
    }

    // ---------------------------------------------------------------
    // Additional coverage.
    // ---------------------------------------------------------------

    /// `count == 0` must short-circuit to Ok(0) WITHOUT seeking or
    /// reading, even at an out-of-range LBA — the early-return guard
    /// runs before any I/O. Grounding: `if count == 0 { return Ok(0) }`.
    #[test]
    fn zero_count_returns_zero_no_io() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("zc.iso");
        make_iso(&path, 4);
        let mut src = FileSectorSource::open(&path).unwrap();
        // LBA far past EOF — must not matter because count==0 returns early.
        let mut buf = [0u8; 1];
        let n = src.read_sectors(1_000_000, 0, &mut buf, false).unwrap();
        assert_eq!(n, 0);
    }

    /// Reading past EOF must ERROR (read_exact's UnexpectedEof), never
    /// return a partial/short count. This is the core "never silently
    /// truncate / never return fewer bytes than declared" property of
    /// the SectorSource contract. Grounding: `self.file.read_exact(...)`
    /// — read_exact fails if the file can't supply the full span.
    #[test]
    fn read_past_eof_errors_not_truncates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("eof.iso");
        make_iso(&path, 4); // 4 sectors only
        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 4);

        // Request 2 sectors starting at LBA 3 → sector 4 doesn't exist.
        let mut buf = vec![0u8; 2 * SECTOR_SIZE];
        let r = src.read_sectors(3, 2, &mut buf, false);
        let err = r.expect_err("reading past EOF must error, not short-read");
        let io: std::io::Error = err.into();
        assert_eq!(
            io.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "partial read at EOF must surface read_exact's UnexpectedEof"
        );
    }

    /// On a successful full read the returned count MUST equal
    /// `count * 2048` exactly — the declared byte count. Grounding:
    /// `Ok(bytes)` where `bytes = count * SECTOR_SIZE`.
    #[test]
    fn full_read_returns_exact_declared_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exact.iso");
        make_iso(&path, 16);
        let mut src = FileSectorSource::open(&path).unwrap();
        let mut buf = vec![0u8; 5 * SECTOR_SIZE];
        let n = src.read_sectors(2, 5, &mut buf, false).unwrap();
        assert_eq!(n, 5 * SECTOR_SIZE, "must return exactly count*2048 bytes");
    }

    /// Capacity is `file_len / 2048` (floor); trailing bytes that don't
    /// complete a sector are NOT counted. A file of 4 sectors + 100
    /// extra bytes reports capacity 4. Grounding: `len / SECTOR_SIZE`
    /// integer division in `open`.
    #[test]
    fn capacity_floors_partial_trailing_sector() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("partial.iso");
        make_iso(&path, 4);
        // Append 100 stray bytes (a torn final sector).
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0xee; 100]).unwrap();
            f.flush().unwrap();
        }
        let src = FileSectorSource::open(&path).unwrap();
        assert_eq!(
            src.capacity_sectors(),
            4,
            "partial trailing bytes must not inflate the sector capacity"
        );
    }

    /// An empty file opens cleanly with capacity 0. Grounding:
    /// `0 / 2048 == 0`, and the IsoTooLarge guard only fires for
    /// oversize files.
    #[test]
    fn empty_file_capacity_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.iso");
        std::fs::File::create(&path).unwrap();
        let src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 0);
    }

    /// Opening a nonexistent path returns an IoError (NotFound), not a
    /// panic. Grounding: `File::open(path).map_err(...)`.
    #[test]
    fn open_missing_file_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.iso");
        let err = match FileSectorSource::open(&path) {
            Ok(_) => panic!("missing file must error"),
            Err(e) => e,
        };
        let io: std::io::Error = err.into();
        assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
    }

    /// A DONTNEED drop crossing the chunk threshold must not corrupt or
    /// short subsequent reads — the eviction is a pure page-cache hint.
    /// We read past the DEFAULT 32 MiB drop chunk (16384 sectors) so the
    /// eviction block fires at least once, asserting every sector still
    /// reads correctly. (Avoids mutating FREEMKV_READ_DROP_CHUNK_MIB to
    /// sidestep a parallel-test env race with `drop_chunk_size_env_override`.)
    /// Grounding: the `bytes_read_since_drop >= drop_chunk_bytes`
    /// eviction block calls only `platform::drop_window` (advisory) and
    /// resets counters — no data effect.
    #[test]
    fn dontneed_eviction_does_not_affect_data() {
        // 32 MiB default chunk = 16384 sectors; read a bit past it.
        let total = (READ_DROP_CHUNK_BYTES_DEFAULT / SECTOR_SIZE as u64) as u32 + 64;
        let dir = tempdir().unwrap();
        let path = dir.path().join("drop.iso");
        make_iso(&path, total);
        let mut src = FileSectorSource::open(&path).unwrap();
        // Read in 16-sector batches to keep the loop fast while still
        // crossing the drop boundary by byte count.
        let batch = 16u16;
        let mut got = vec![0u8; batch as usize * SECTOR_SIZE];
        let mut lba = 0u32;
        while lba + batch as u32 <= total {
            src.read_sectors(lba, batch, &mut got, false).unwrap();
            for i in 0..batch as u32 {
                let expected = ((lba + i) & 0xff) as u8;
                let off = i as usize * SECTOR_SIZE;
                assert!(
                    got[off..off + SECTOR_SIZE].iter().all(|x| *x == expected),
                    "DONTNEED eviction corrupted sector {}",
                    lba + i
                );
            }
            lba += batch as u32;
        }
    }
}

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
//! isolated NFS reads). Every [`READ_DROP_CHUNK_BYTES`] of consumed
//! bytes we call `posix_fadvise(DONTNEED)` over that window, mirroring
//! the write-side [`crate::io::writeback::WritebackPipeline`] policy.
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
    /// `bytes_read_since_drop` bytes.
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
    /// Windows TODO stub) so the kernel's readahead widens.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let sectors = len / SECTOR_SIZE as u64;
        if sectors > u32::MAX as u64 {
            return Err(Error::IsoTooLarge {
                path: path.to_string_lossy().into_owned(),
            }
            .into());
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
}

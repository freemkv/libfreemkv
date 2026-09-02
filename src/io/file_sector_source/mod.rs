//! [`FileSectorSource`] — read 2048-byte sectors from an ISO file on
//! disk via direct `seek + read_exact`, letting the kernel's own
//! readahead policy handle prefetch instead of an app-level buffer.
//!
//! Issues a platform "sequential access" hint on open, prefetches the
//! next window after each read, and periodically evicts the consumed
//! byte range via `posix_fadvise(DONTNEED)` to bound page-cache pressure.
//!
//! See docs/file-sector-source.md for design rationale (no app-level
//! buffer, DONTNEED window, platform hint, prefetch).

#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "macos")]
pub(crate) mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub(crate) mod other;
#[cfg(target_os = "windows")]
pub(crate) mod windows;

// The page-cache hints are shared with any other file-backed sector source:
// `dirimage` reads host files the same way and needs the same eviction, or a
// large rip pins every byte it has read (see this module's DONTNEED note).
#[cfg(target_os = "linux")]
pub(crate) use linux as platform;
#[cfg(target_os = "macos")]
pub(crate) use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub(crate) use other as platform;
#[cfg(target_os = "windows")]
pub(crate) use windows as platform;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Error, Result};
use crate::sector::SectorSource;

use crate::consts::{SECTOR_BYTES, SECTOR_BYTES_U64};

// Bytes-read threshold per `posix_fadvise(DONTNEED)` drop; mirrors
// WRITEBACK_CHUNK_BYTES. 32 MiB is empirically tuned (7200rpm HDD/SATA);
// see docs/file-sector-source.md. Override: FREEMKV_READ_DROP_CHUNK_MIB.
const READ_DROP_CHUNK_BYTES_DEFAULT: u64 = 32 * 1024 * 1024;

// Max MiB from FREEMKV_READ_DROP_CHUNK_MIB: 64 GiB, and small enough
// n*1024*1024 can't overflow u64 (mirrors WRITEBACK_CHUNK_MIB_MAX).
// See docs/file-sector-source.md; out-of-range falls back to default.
const READ_DROP_CHUNK_MIB_MAX: u64 = 64 * 1024;

fn read_drop_chunk_bytes() -> u64 {
    resolve_read_drop_chunk(
        std::env::var("FREEMKV_READ_DROP_CHUNK_MIB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok()),
    )
}

/// The pure part of [`read_drop_chunk_bytes`], split out so the bound is
/// testable without mutating process environment.
fn resolve_read_drop_chunk(mib: Option<u64>) -> u64 {
    mib.filter(|&n| n > 0 && n <= READ_DROP_CHUNK_MIB_MAX)
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
        let sectors = len / SECTOR_BYTES_U64;
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
        let bytes = count as usize * SECTOR_BYTES;
        // A real check, not debug_assert: `out` is caller input to a public
        // `SectorSource` impl, and `out[..bytes]` below would panic in release
        // where the assert is compiled away. `Drive::read_fua` carries this guard.
        if out.len() < bytes {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        if count == 0 {
            return Ok(0);
        }
        let offset = lba as u64 * SECTOR_BYTES_U64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::IoError { source: e })?;
        self.file
            .read_exact(&mut out[..bytes])
            .map_err(|e| Error::IoError { source: e })?;

        // Queue the next batch's read before the caller processes what we returned.
        // readahead() is non-blocking; the kernel pulls pages into cache while the
        // consumer runs, so the next read_sectors call hits a warm cache.
        platform::prefetch(&self.file, offset + bytes as u64, bytes as u64);

        // Periodic page-cache eviction on the read side: an 85 GB streaming ISO
        // read would otherwise pin the whole file in cache, starving concurrent
        // writes. Mirrors WritebackPipeline's DONTNEED policy on the write side.
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

    // Undersized `out` must error, not panic: `debug_assert!` alone is compiled
    // out in release, letting `out[..bytes]` panic. See docs/file-sector-source.md
    // (also guarded in Drive::read_fua and PrefetchedSectorSource).
    #[test]
    fn read_sectors_with_an_undersized_buffer_errors_rather_than_panicking() {
        let dir = tempdir().unwrap();
        let iso = dir.path().join("t.iso");
        make_iso(&iso, 8);
        let mut src = FileSectorSource::open(&iso).expect("iso opens");

        // Ask for four sectors but supply room for barely more than one.
        let mut out = vec![0u8; SECTOR_BYTES + 1];
        let err = src
            .read_sectors(0, 4, &mut out, false)
            .expect_err("an undersized buffer must be an error, not a panic");
        assert!(
            matches!(err, Error::DiscRead { .. }),
            "expected DiscRead, got {err:?}"
        );

        // Exactly-sized still works, so the guard is not off by one.
        let mut out = vec![0u8; 4 * SECTOR_BYTES];
        assert_eq!(
            src.read_sectors(0, 4, &mut out, false).unwrap(),
            4 * SECTOR_BYTES
        );
    }

    // Build a deterministic ISO: sector `n` filled with byte `(n & 0xff)`,
    // so any sector can be verified by content alone.
    fn make_iso(path: &std::path::Path, sectors: u32) {
        let mut f = std::fs::File::create(path).unwrap();
        let mut chunk = vec![0u8; SECTOR_BYTES];
        for n in 0..sectors {
            let b = (n & 0xff) as u8;
            chunk.iter_mut().for_each(|c| *c = b);
            f.write_all(&chunk).unwrap();
        }
        f.flush().unwrap();
    }

    // Sectors used by spanning-boundary tests: 8192 sectors = 16 MiB,
    // large enough to cross any readahead chunk size we set the kernel
    // hint to, without making test ISOs huge.
    const TEST_SPAN_SECTORS: u32 = 8192;

    #[test]
    fn sequential_reads_match_file() {
        let total = TEST_SPAN_SECTORS * 2 + 17;
        let dir = tempdir().unwrap();
        let path = dir.path().join("seq.iso");
        make_iso(&path, total);

        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), total);

        let mut got = vec![0u8; SECTOR_BYTES];
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
        let mut buf4 = vec![0u8; SECTOR_BYTES * 4];
        src.read_sectors(span_lba, 4, &mut buf4, false).unwrap();
        for i in 0..4 {
            let lba = span_lba + i as u32;
            let expected = (lba & 0xff) as u8;
            for b in &buf4[i * SECTOR_BYTES..(i + 1) * SECTOR_BYTES] {
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
        let mut got = vec![0u8; SECTOR_BYTES];

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

        let mut got = vec![0u8; SECTOR_BYTES];
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
        let req_bytes = req as usize * SECTOR_BYTES;
        let mut big = vec![0u8; req_bytes];
        src.read_sectors(0, req, &mut big, false).unwrap();
        assert!(big[..SECTOR_BYTES].iter().all(|b| *b == 0));
        let last_lba = req as u32 - 1;
        let exp = (last_lba & 0xff) as u8;
        let last_off = (req as usize - 1) * SECTOR_BYTES;
        assert!(
            big[last_off..last_off + SECTOR_BYTES]
                .iter()
                .all(|b| *b == exp)
        );
    }

    #[test]
    fn drop_chunk_size_env_override() {
        // Explicit 8 MiB via env var. SAFETY: set_var is `unsafe` since Rust 2024
        // (can race with other threads/TLS), but this test runs single-threaded
        // and in-process before any FileSectorSource construction.
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

    // Additional coverage. count==0 must short-circuit to Ok(0) before any
    // seek/read, even at an out-of-range LBA — Ok(0) alone doesn't prove it,
    // so we assert the file cursor is unmoved instead.
    #[test]
    fn zero_count_returns_zero_no_io() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("zc.iso");
        make_iso(&path, 4);
        let mut src = FileSectorSource::open(&path).unwrap();
        let before = src.file.stream_position().expect("cursor readable");
        assert_eq!(before, 0, "a freshly opened file starts at offset 0");
        // LBA far past EOF — must not matter because count==0 returns early.
        let mut buf = [0u8; 1];
        let n = src.read_sectors(1_000_000, 0, &mut buf, false).unwrap();
        assert_eq!(n, 0);
        assert_eq!(
            src.file.stream_position().expect("cursor readable"),
            before,
            "count == 0 must return before the seek — an unmoved cursor is the \
             only observable proof that no I/O was issued"
        );
        // And the drop-window accounting must not have advanced either.
        assert_eq!(src.bytes_read_since_drop, 0);
        assert_eq!(src.drop_window_start, 0);
    }

    // Reading past EOF must ERROR (read_exact's UnexpectedEof), never a
    // partial/short count — the "never silently truncate" SectorSource
    // contract, backed by read_exact failing on a short span.
    #[test]
    fn read_past_eof_errors_not_truncates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("eof.iso");
        make_iso(&path, 4); // 4 sectors only
        let mut src = FileSectorSource::open(&path).unwrap();
        assert_eq!(src.capacity_sectors(), 4);

        // Request 2 sectors starting at LBA 3 → sector 4 doesn't exist.
        let mut buf = vec![0u8; 2 * SECTOR_BYTES];
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
    /// `Ok(bytes)` where `bytes = count * SECTOR_BYTES`.
    #[test]
    fn full_read_returns_exact_declared_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exact.iso");
        make_iso(&path, 16);
        let mut src = FileSectorSource::open(&path).unwrap();
        let mut buf = vec![0u8; 5 * SECTOR_BYTES];
        let n = src.read_sectors(2, 5, &mut buf, false).unwrap();
        assert_eq!(n, 5 * SECTOR_BYTES, "must return exactly count*2048 bytes");
    }

    // Capacity is `file_len / 2048` (floor); a torn trailing sector (4
    // sectors + 100 stray bytes) must not inflate it — verified against
    // `len / SECTOR_BYTES` integer division in `open`.
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

    // A DONTNEED drop crossing the chunk threshold must not corrupt/short
    // reads — it's an advisory hint. Reads past the default 32 MiB chunk
    // (16384 sectors) so eviction fires; see docs/file-sector-source.md.
    #[test]
    fn dontneed_eviction_does_not_affect_data() {
        // 32 MiB default chunk = 16384 sectors; read a bit past it.
        let total = (READ_DROP_CHUNK_BYTES_DEFAULT / SECTOR_BYTES_U64) as u32 + 64;
        let dir = tempdir().unwrap();
        let path = dir.path().join("drop.iso");
        make_iso(&path, total);
        let mut src = FileSectorSource::open(&path).unwrap();
        // Read in 16-sector batches to keep the loop fast while still
        // crossing the drop boundary by byte count.
        let batch = 16u16;
        let mut got = vec![0u8; batch as usize * SECTOR_BYTES];
        let mut lba = 0u32;
        while lba + batch as u32 <= total {
            src.read_sectors(lba, batch, &mut got, false).unwrap();
            for i in 0..batch as u32 {
                let expected = ((lba + i) & 0xff) as u8;
                let off = i as usize * SECTOR_BYTES;
                assert!(
                    got[off..off + SECTOR_BYTES].iter().all(|x| *x == expected),
                    "DONTNEED eviction corrupted sector {}",
                    lba + i
                );
            }
            lba += batch as u32;
        }
    }

    // FREEMKV_READ_DROP_CHUNK_MIB must be bounded before the MiB→byte
    // multiply (mirrors WRITEBACK_CHUNK_MIB_MAX): unbounded, values above
    // 2^44 overflow/wrap — see docs/file-sector-source.md.
    #[test]
    fn read_drop_chunk_env_is_bounded_before_the_multiply() {
        // Default when unset / zero / out of range.
        assert_eq!(resolve_read_drop_chunk(None), READ_DROP_CHUNK_BYTES_DEFAULT);
        assert_eq!(
            resolve_read_drop_chunk(Some(0)),
            READ_DROP_CHUNK_BYTES_DEFAULT
        );
        // The overflow value: `u64::MAX * 1024 * 1024` panicked here.
        assert_eq!(
            resolve_read_drop_chunk(Some(u64::MAX)),
            READ_DROP_CHUNK_BYTES_DEFAULT
        );
        assert_eq!(
            resolve_read_drop_chunk(Some(READ_DROP_CHUNK_MIB_MAX + 1)),
            READ_DROP_CHUNK_BYTES_DEFAULT
        );
        // In-range values convert MiB→bytes. Mutation: `* 1024` breaks this.
        assert_eq!(resolve_read_drop_chunk(Some(1)), 1024 * 1024);
        assert_eq!(
            resolve_read_drop_chunk(Some(READ_DROP_CHUNK_MIB_MAX)),
            READ_DROP_CHUNK_MIB_MAX * 1024 * 1024
        );
        // And the bound itself keeps the multiply inside u64.
        assert!((READ_DROP_CHUNK_MIB_MAX as u128) * 1024 * 1024 <= u64::MAX as u128);
    }
}

//! [`FileSectorSource`] — read 2048-byte sectors from an ISO file on
//! disk via direct `seek + read_exact`, letting the kernel's own
//! readahead policy handle prefetch instead of an app-level buffer.
//!
//! Issues a platform "sequential access" hint on open, prefetches the
//! next window (batched on macOS), and periodically evicts the consumed
//! byte range via `posix_fadvise(DONTNEED)` to bound page-cache pressure.

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

// Bytes-read threshold per `posix_fadvise(DONTNEED)` drop; mirrors WRITEBACK_CHUNK_BYTES_DEFAULT.
// 32 MiB is empirically tuned (7200rpm HDD/SATA).
const READ_DROP_CHUNK_BYTES_DEFAULT: u64 = 32 * 1024 * 1024;

// Max MiB from FREEMKV_READ_DROP_CHUNK_MIB: 64 GiB, and small enough n*1024*1024 can't overflow
// u64 (mirrors WRITEBACK_CHUNK_MIB_MAX).
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
    /// `(start, end)` file range read contiguously and not yet dropped.
    drop_window: (u64, u64),
    /// Cached drop chunk size (resolved from env once at open).
    drop_chunk_bytes: u64,
    /// `Some(file length)` for [`open_padded`](Self::open_padded): a partial tail sector is
    /// read, zero-filled to 2048 bytes.
    padded_len: Option<u64>,
    #[cfg(target_os = "macos")]
    prefetch_window: macos::PrefetchWindow,
}

impl FileSectorSource {
    /// Open an existing ISO file for reading. Capacity is derived
    /// from `metadata().len() / 2048`. Returns
    /// [`Error::IsoTooLarge`] if the file would exceed the 32-bit
    /// LBA address space (~8 TB).
    ///
    /// Issues the platform's "sequential access expected" hint on the
    /// fd (Linux `posix_fadvise(SEQUENTIAL)`, other platforms no-op).
    /// macOS read advice is deferred until a stream has been observed.
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
            drop_window: (0, 0),
            drop_chunk_bytes: read_drop_chunk_bytes(),
            padded_len: None,
            #[cfg(target_os = "macos")]
            prefetch_window: macos::PrefetchWindow::default(),
        })
    }

    /// Like [`open`](Self::open), but a file whose size is not a multiple of 2048 keeps its
    /// tail: the last sector is zero-padded, synthetic and not counted as loss (an `mpg://`
    /// source, mpg-output-design v5 §4 step 2, J13).
    pub(crate) fn open_padded(path: &Path) -> Result<Self> {
        let mut s = Self::open(path)?;
        let len = s
            .file
            .metadata()
            .map_err(|e| Error::IoError { source: e })?
            .len();
        let sectors = len.div_ceil(SECTOR_BYTES_U64);
        if sectors > u32::MAX as u64 {
            return Err(Error::IsoTooLarge {
                path: path.to_string_lossy().into_owned(),
            });
        }
        s.capacity = sectors as u32;
        s.padded_len = Some(len);
        Ok(s)
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
        // A padded source reads what the file holds and zero-fills the rest of the sector.
        let real = match self.padded_len {
            // Clamp in u64 first: on a 32-bit target `as usize` would truncate the distance.
            Some(len) => len.saturating_sub(offset).min(bytes as u64) as usize,
            None => bytes,
        };
        if let Err(e) = self.file.read_exact(&mut out[..real]) {
            return Err(self.read_error(e, lba, offset + bytes as u64));
        }
        out[real..bytes].fill(0);

        // Amortize macOS advice over a stream, not every 2 KiB metadata sector.
        #[cfg(target_os = "macos")]
        if let Some((start, len)) = self.prefetch_window.after_read(
            offset,
            real as u64,
            self.padded_len
                .unwrap_or(self.capacity as u64 * SECTOR_BYTES_U64),
        ) {
            platform::prefetch(&self.file, start, len);
        }
        #[cfg(not(target_os = "macos"))]
        platform::prefetch(&self.file, offset + bytes as u64, bytes as u64);

        // Periodic page-cache eviction on the read side: an 85 GB streaming ISO
        // read would otherwise pin the whole file in cache, starving concurrent
        // writes. Mirrors WritebackPipeline's DONTNEED policy on the write side.
        let file = &self.file;
        take_drop_window(
            &mut self.drop_window,
            self.drop_chunk_bytes,
            offset,
            bytes as u64,
            |start, len| platform::drop_window(file, start, len),
        );

        Ok(bytes)
    }
}

impl FileSectorSource {
    // A short read means the image ends before `want` (a truncated rip), not a dead bus.
    fn read_error(&self, e: std::io::Error, lba: u32, want: u64) -> Error {
        if e.kind() != std::io::ErrorKind::UnexpectedEof {
            return Error::IoError { source: e };
        }
        let have = self
            .file
            .metadata()
            .map(|m| m.len())
            .unwrap_or(self.capacity as u64 * SECTOR_BYTES_U64);
        Error::ImageEndsBeforeRead { lba, have, want }
    }
}

// Extend the contiguous read window `(start, end)` by `[offset, offset+len)`, calling
// `drop(start, len)` for each range to evict.
fn take_drop_window(
    window: &mut (u64, u64),
    chunk: u64,
    offset: u64,
    len: u64,
    mut drop: impl FnMut(u64, u64),
) {
    if offset != window.1 {
        // A jump: evict what was read so far, then restart at `offset`.
        if window.1 > window.0 {
            drop(window.0, window.1 - window.0);
        }
        window.0 = offset;
    }
    window.1 = offset.saturating_add(len);
    let span = window.1 - window.0;
    if span >= chunk {
        drop(window.0, span);
        window.0 = window.1;
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

//! Sector-level I/O traits.
//!
//! The sector layer is direction-typed: [`SectorSource`] reads
//! 2048-byte sectors, [`SectorSink`] writes them. Concrete impls
//! never do both — physical drives are read-only, file-backed
//! ISO images are opened for read OR write at construction time.
//!
//! - [`SectorSource`] is implemented by `Drive` (hardware) and
//!   [`FileSectorSource`] / `IsoSectorReader` (file-backed).
//! - [`SectorSink`] is implemented by [`FileSectorSink`]
//!   (ISO-backed) and sweep/patch consumer adapters.
//! - [`DecryptingSectorSource`] is a decorator that wraps any
//!   `SectorSource` and applies AACS / CSS in-place decrypt to
//!   yield plaintext sectors.

pub mod decrypting;
pub mod file;

use crate::error::Result;

/// Read 2048-byte sectors from a disc, image, or composed source.
///
/// Wrap the inner source in [`DecryptingSectorSource`] to get
/// plaintext sectors out of an encrypted disc.
pub trait SectorSource: Send {
    /// Total capacity in sectors, if known. Default `0` = unknown
    /// (e.g. live drives that haven't completed `READ CAPACITY` yet).
    fn capacity_sectors(&self) -> u32 {
        0
    }

    /// Read `count` sectors starting at `lba` into `buf`.
    /// `buf` must be at least `count * 2048` bytes.
    /// `recovery`: true = full retry/reset loop (ripping),
    /// false = single attempt (verify). File-backed sources ignore
    /// the flag.
    ///
    /// Returns the number of bytes written into `buf` on success.
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize>;

    /// Optional speed control for sources that map to a physical
    /// drive. No-op for everything else.
    fn set_speed(&mut self, _kbs: u16) {}
}

// Forwarding impls so `Box<dyn SectorSource>` and `&mut dyn SectorSource`
// satisfy the `SectorSource` trait bound when wrapped by generic
// decorators like `DecryptingSectorSource<S: SectorSource>`.
impl SectorSource for Box<dyn SectorSource> {
    fn capacity_sectors(&self) -> u32 {
        (**self).capacity_sectors()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        (**self).read_sectors(lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        (**self).set_speed(kbs)
    }
}

impl SectorSource for &mut (dyn SectorSource + '_) {
    fn capacity_sectors(&self) -> u32 {
        (**self).capacity_sectors()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        (**self).read_sectors(lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        (**self).set_speed(kbs)
    }
}

/// Write 2048-byte sectors to a disc image or composed sink.
///
/// The terminal [`finish`] takes `Box<Self>` so it can run on `dyn
/// SectorSink` and consume the sink (`fsync` + close).
///
/// [`finish`]: SectorSink::finish
pub trait SectorSink: Send {
    /// Write the sectors in `buf` starting at `lba`. `buf.len()`
    /// must be a multiple of 2048; the implementation seeks to
    /// `lba * 2048` before writing.
    fn write_sectors(&mut self, lba: u32, buf: &[u8]) -> Result<()>;

    /// Flush, fsync, and close. Consumes the sink. Always called
    /// last; subsequent operations are not defined.
    fn finish(self: Box<Self>) -> Result<()>;
}

pub use crate::io::file_sector_source::FileSectorSource;
pub use decrypting::DecryptingSectorSource;
pub use file::FileSectorSink;

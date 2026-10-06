//! Sector-level read I/O traits.
//!
//! [`SectorSource`] reads 2048-byte sectors from a disc.
//!
//! - [`SectorSource`] is implemented by `Drive` (hardware) and
//!   [`FileSectorSource`] (file-backed).
//! - [`DecryptingSectorSource`] is a decorator that wraps any
//!   `SectorSource` and applies AACS / CSS in-place decrypt to
//!   yield plaintext sectors.

pub mod bus_removal;
pub mod decrypting;
pub mod prefetched;
pub mod read_stage;
pub(crate) mod stage;

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
    /// `recovery`: true = full retry/reset loop (ripping), false = single
    /// attempt (verify). File-backed sources ignore the flag.
    ///
    /// Returns the number of bytes written into `buf` on success.
    ///
    /// # Panics
    ///
    /// Implementations may panic if `buf` is undersized; `FileSectorSource`
    /// returns `DiscRead` instead.
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize>;

    /// Like [`read_sectors`], but with an explicit Force Unit Access request.
    ///
    /// `fua = true` asks the drive to bypass its readahead cache and
    /// physically re-fetch the medium.
    ///
    /// The default ignores `fua` and delegates to [`read_sectors`]: only a
    /// live [`Drive`] sets the CDB bit; file-backed sources have no drive
    /// cache to bypass.
    ///
    /// [`read_sectors`]: SectorSource::read_sectors
    /// [`Drive`]: crate::drive::Drive
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        let _ = fua;
        self.read_sectors(lba, count, buf, recovery)
    }

    /// Optional speed control for sources that map to a physical
    /// drive. No-op for everything else.
    fn set_speed(&mut self, _kbs: u16) {}

    /// Set the base LBA an AACS unit-alignment gate measures against — the
    /// `start_lba` of the extent/clip about to be read. Aligned AACS units
    /// (6144 B / 3 sectors) are anchored at each clip's encrypted-region start,
    /// so a decrypt-on-read source gates `lba` relative to this base, not
    /// absolute disc LBA 0. Mux read paths call this when they advance to a new
    /// extent. No-op for everything except [`DecryptingSectorSource`], the only
    /// source that applies the unit-alignment gate.
    ///
    /// [`DecryptingSectorSource`]: crate::sector::DecryptingSectorSource
    fn set_unit_base(&mut self, _lba: u32) {}

    /// Stream files this source's bus-removal stage could not locate, so their
    /// sectors come through still bus-encrypted (see
    /// [`bus_removal::ensure_image_debussable`]). Empty for every source without
    /// a host-key bus stage; wrappers must forward it.
    fn unmapped_stream_files(&self) -> &[bus_removal::UnmappedStreamFile] {
        &[]
    }

    /// Whether `read_sectors` honours the requested `lba`/`count` (random access). `false`
    /// for a prefetcher, whose "lba/count are advisory". The key set's decrypting readers
    /// side-read neighbouring units, so they refuse a non-random-access inner source (KU
    /// §2.4); wrappers must forward it.
    fn random_access(&self) -> bool {
        true
    }
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

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        (**self).read_sectors_fua(lba, count, buf, recovery, fua)
    }

    fn set_speed(&mut self, kbs: u16) {
        (**self).set_speed(kbs)
    }

    fn set_unit_base(&mut self, lba: u32) {
        (**self).set_unit_base(lba)
    }

    fn unmapped_stream_files(&self) -> &[bus_removal::UnmappedStreamFile] {
        (**self).unmapped_stream_files()
    }

    fn random_access(&self) -> bool {
        (**self).random_access()
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

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        (**self).read_sectors_fua(lba, count, buf, recovery, fua)
    }

    fn set_speed(&mut self, kbs: u16) {
        (**self).set_speed(kbs)
    }

    fn set_unit_base(&mut self, lba: u32) {
        (**self).set_unit_base(lba)
    }

    fn unmapped_stream_files(&self) -> &[bus_removal::UnmappedStreamFile] {
        (**self).unmapped_stream_files()
    }

    fn random_access(&self) -> bool {
        (**self).random_access()
    }
}

pub use crate::io::file_sector_source::FileSectorSource;
pub use decrypting::{DecryptingSectorSource, Keying};
pub use prefetched::PrefetchedSectorSource;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

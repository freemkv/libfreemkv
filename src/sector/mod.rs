//! Sector-level I/O traits.
//!
//! 0.18 splits the unidirectional read trait from a write trait at
//! the sector layer, so the type system catches "wrong direction"
//! mistakes at compile time instead of runtime. See
//! `freemkv-private/memory/0_18_redesign.md`.
//!
//! - [`SectorSource`] reads 2048-byte sectors. Implemented by
//!   `Drive` (via the legacy [`SectorReader`] alias) and
//!   [`FileSectorSource`] (ISO-backed).
//! - [`SectorSink`] writes 2048-byte sectors. Implemented by
//!   [`FileSectorSink`] (ISO-backed) and, in later commits, by
//!   sweep/patch consumer adapters.
//! - [`DecryptingSectorSource`] is a decorator that wraps any
//!   `SectorSource` and applies the existing AACS / CSS in-place
//!   decrypt to plaintext-out.
//!
//! [`SectorReader`] is the 0.17 read trait. It stays on through
//! the 0.18 migration window so existing call sites
//! (`Drive`, `IsoSectorReader`, `BufferedSectorReader`,
//! `DiscStream`, `verify`) compile unchanged. A blanket impl
//! forwards every `SectorReader` impl to `SectorSource`, so new
//! code should target `SectorSource` / `SectorSink` directly. The
//! formal `#[deprecated]` attribute lands once the internal
//! callers have migrated; see the comment on `SectorReader` for
//! why this commit holds it back.

pub mod decrypting;
pub mod file;

use crate::error::Result;

/// Read 2048-byte sectors from a disc, image, or composed source.
///
/// Direction-typed: a `SectorSource` cannot be written to. Wrap the
/// inner source in [`DecryptingSectorSource`] to get plaintext
/// sectors out of an encrypted disc.
pub trait SectorSource: Send {
    /// Total capacity in sectors, if known. Returns 0 when unknown
    /// (e.g. live drives that haven't completed `READ CAPACITY` yet).
    fn capacity_sectors(&self) -> u32;

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

/// Write 2048-byte sectors to a disc image or composed sink.
///
/// Direction-typed: a `SectorSink` cannot be read from. The
/// terminal [`finish`] takes `Box<Self>` so it can run on `dyn
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

/// 0.17 read trait. Slated for removal once internal call sites
/// migrate to [`SectorSource`] in follow-up commits; until then
/// it remains the trait that `Drive`, `IsoSectorReader`,
/// `BufferedSectorReader`, and existing `&mut dyn SectorReader`
/// signatures use unchanged.
///
/// New code should implement [`SectorSource`] directly. The
/// blanket impl below makes any `SectorReader` automatically
/// usable wherever a `SectorSource` is expected, so a one-way
/// migration off `SectorReader` is possible per-callsite without
/// touching the impls.
//
// NOTE: not marked `#[deprecated]` in this commit — `cargo clippy
// -- -D warnings` (the CI gauntlet) treats deprecation as an
// error, and the existing `Drive` / `udf::BufferedSectorReader` /
// `mux::DiscStream` / `verify` call sites all go through this
// trait. The deprecation attribute lands together with the
// migration commits that move those call sites to
// `SectorSource`. The behavioural contract — "this trait is
// going away in 0.18" — is documented above and tracked in
// `freemkv-private/memory/0_18_redesign.md`.
pub trait SectorReader: Send {
    /// Read `count` sectors starting at `lba` into `buf`.
    /// See [`SectorSource::read_sectors`] for semantics.
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize>;

    /// Total capacity in sectors, if known.
    fn capacity(&self) -> u32 {
        0
    }

    fn set_speed(&mut self, _kbs: u16) {}
}

// Blanket impl: anything implementing the legacy `SectorReader`
// trait automatically satisfies `SectorSource`. This is what keeps
// existing impls (`Drive`, `IsoSectorReader`, `BufferedSectorReader`,
// etc.) compiling without source changes during the migration. The
// reverse direction (impl SectorReader for SectorSource) is
// intentionally NOT provided — new code targets the new trait.
impl<T: SectorReader + ?Sized> SectorSource for T {
    fn capacity_sectors(&self) -> u32 {
        <T as SectorReader>::capacity(self)
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        <T as SectorReader>::read_sectors(self, lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        <T as SectorReader>::set_speed(self, kbs)
    }
}

// Forwarding impls so callers can wrap `&mut dyn SectorReader` /
// `Box<dyn SectorReader>` in [`DecryptingSectorSource`] without
// having to unbox or re-borrow inside the lib's hot paths. The
// generic `&mut T` / `Box<T>` blankets would conflict with the
// `SectorReader → SectorSource` blanket above (a downstream crate
// could `impl SectorReader for &mut U`); the specific
// `dyn SectorReader` instantiations are unambiguous because
// `SectorReader` is the very trait whose `dyn` we're targeting.
impl SectorSource for &mut (dyn SectorReader + '_) {
    fn capacity_sectors(&self) -> u32 {
        <dyn SectorReader as SectorReader>::capacity(*self)
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        <dyn SectorReader as SectorReader>::read_sectors(*self, lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        <dyn SectorReader as SectorReader>::set_speed(*self, kbs)
    }
}

impl SectorSource for Box<dyn SectorReader> {
    fn capacity_sectors(&self) -> u32 {
        <dyn SectorReader as SectorReader>::capacity(&**self)
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        <dyn SectorReader as SectorReader>::read_sectors(&mut **self, lba, count, buf, recovery)
    }

    fn set_speed(&mut self, kbs: u16) {
        <dyn SectorReader as SectorReader>::set_speed(&mut **self, kbs)
    }
}

pub use decrypting::DecryptingSectorSource;
pub use file::{FileSectorSink, FileSectorSource};

// Backwards-compat alias for the public API. `FileSectorReader` is
// the 0.17 name; new code uses `FileSectorSource`. Both point at
// the same type. The `#[deprecated]` attribute lands together with
// the migration commits that retire the alias from internal uses.
pub type FileSectorReader = FileSectorSource;

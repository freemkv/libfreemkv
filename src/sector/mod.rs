//! Sector-level I/O traits.
//!
//! The sector layer is direction-typed: [`SectorSource`] reads
//! 2048-byte sectors, [`SectorSink`] writes them. Concrete impls
//! never do both — physical drives are read-only, file-backed
//! ISO images are opened for read OR write at construction time.
//!
//! - [`SectorSource`] is implemented by `Drive` (hardware) and
//!   [`FileSectorSource`] (file-backed).
//! - [`SectorSink`] is implemented by [`FileSectorSink`]
//!   (ISO-backed).
//! - [`DecryptingSectorSource`] is a decorator that wraps any
//!   `SectorSource` and applies AACS / CSS in-place decrypt to
//!   yield plaintext sectors.

pub mod decrypting;
pub mod file;
pub mod prefetched;

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
    ///
    /// # Panics
    ///
    /// Implementations may panic if `buf.len() < count * 2048`. This
    /// is a caller contract enforced via `debug_assert!` in the
    /// primary impl ([`FileSectorSource`]); in release builds an
    /// undersized buffer panics on the slice. Callers must size `buf`
    /// to at least `count * 2048` bytes.
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
    /// `lba as u64 * 2048` before writing (the `u64` cast is required —
    /// a bare `u32` `lba * 2048` wraps past ~4 GB on UHD-scale images).
    fn write_sectors(&mut self, lba: u32, buf: &[u8]) -> Result<()>;

    /// Flush, fsync, and close. Consumes the sink. Always called
    /// last; subsequent operations are not defined.
    fn finish(self: Box<Self>) -> Result<()>;
}

pub use crate::io::file_sector_source::FileSectorSource;
pub use decrypting::DecryptingSectorSource;
pub use file::FileSectorSink;
pub use prefetched::PrefetchedSectorSource;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A fully-instrumented SectorSource: records every read's
    /// (lba, count, recovery), reports a known capacity, and records
    /// set_speed calls. Lets the forwarding-impl tests prove each
    /// trait method is delegated, not stubbed.
    struct Spy {
        capacity: u32,
        reads: Arc<Mutex<Vec<(u32, u16, bool)>>>,
        speeds: Arc<Mutex<Vec<u16>>>,
    }

    impl Spy {
        fn new(
            capacity: u32,
        ) -> (
            Self,
            Arc<Mutex<Vec<(u32, u16, bool)>>>,
            Arc<Mutex<Vec<u16>>>,
        ) {
            let reads = Arc::new(Mutex::new(Vec::new()));
            let speeds = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    capacity,
                    reads: reads.clone(),
                    speeds: speeds.clone(),
                },
                reads,
                speeds,
            )
        }
    }

    impl SectorSource for Spy {
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            self.reads.lock().unwrap().push((lba, count, recovery));
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0xa5);
            Ok(bytes)
        }
        fn set_speed(&mut self, kbs: u16) {
            self.speeds.lock().unwrap().push(kbs);
        }
    }

    /// The default `capacity_sectors` is 0 (unknown). Grounding: trait
    /// default body `fn capacity_sectors(&self) -> u32 { 0 }`.
    #[test]
    fn default_capacity_is_zero() {
        struct Minimal;
        impl SectorSource for Minimal {
            fn read_sectors(
                &mut self,
                _lba: u32,
                _count: u16,
                _buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                Ok(0)
            }
        }
        assert_eq!(Minimal.capacity_sectors(), 0);
    }

    /// The default `set_speed` is a no-op that must not panic.
    /// Grounding: trait default body `fn set_speed(&mut self, _kbs) {}`.
    #[test]
    fn default_set_speed_is_noop() {
        struct Minimal;
        impl SectorSource for Minimal {
            fn read_sectors(
                &mut self,
                _lba: u32,
                _count: u16,
                _buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                Ok(0)
            }
        }
        let mut m = Minimal;
        m.set_speed(12345); // must not panic
    }

    /// `Box<dyn SectorSource>` must forward ALL three trait methods to
    /// the inner source (capacity, read_sectors args + return, speed) —
    /// the blanket impl exists so boxed sources satisfy generic
    /// decorator bounds. Grounding: `impl SectorSource for
    /// Box<dyn SectorSource>` forwarding bodies.
    #[test]
    fn boxed_dyn_forwards_all_methods() {
        let (spy, reads, speeds) = Spy::new(777);
        let mut boxed: Box<dyn SectorSource> = Box::new(spy);

        assert_eq!(boxed.capacity_sectors(), 777, "capacity must forward");

        let mut buf = vec![0u8; 3 * 2048];
        let n = boxed.read_sectors(99, 3, &mut buf, true).unwrap();
        assert_eq!(n, 3 * 2048, "read return must forward");
        assert!(buf.iter().all(|b| *b == 0xa5), "inner must have filled buf");

        boxed.set_speed(5400);

        assert_eq!(
            *reads.lock().unwrap(),
            vec![(99, 3, true)],
            "read args (lba/count/recovery) must forward unchanged"
        );
        assert_eq!(
            *speeds.lock().unwrap(),
            vec![5400],
            "set_speed must forward"
        );
    }

    /// `&mut dyn SectorSource` must likewise forward all three methods.
    /// Grounding: `impl SectorSource for &mut (dyn SectorSource + '_)`.
    #[test]
    fn mut_ref_dyn_forwards_all_methods() {
        let (mut spy, reads, speeds) = Spy::new(123);
        let r: &mut dyn SectorSource = &mut spy;

        assert_eq!(r.capacity_sectors(), 123);

        let mut buf = vec![0u8; 2 * 2048];
        let n = r.read_sectors(7, 2, &mut buf, false).unwrap();
        assert_eq!(n, 2 * 2048);

        r.set_speed(8800);

        assert_eq!(*reads.lock().unwrap(), vec![(7, 2, false)]);
        assert_eq!(*speeds.lock().unwrap(), vec![8800]);
    }
}

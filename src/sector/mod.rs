//! Sector-level read I/O traits.
//!
//! [`SectorSource`] reads 2048-byte sectors from a disc.
//!
//! - [`SectorSource`] is implemented by `Drive` (hardware) and
//!   [`FileSectorSource`] (file-backed).
//! - [`DecryptingSectorSource`] is a decorator that wraps any
//!   `SectorSource` and applies AACS / CSS in-place decrypt to
//!   yield plaintext sectors.

pub mod decrypting;
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
    /// `recovery`: true = full retry/reset loop (ripping), false = single
    /// attempt (verify). File-backed sources ignore the flag.
    ///
    /// Returns the number of bytes written into `buf` on success.
    ///
    /// # Panics
    ///
    /// Implementations may panic if `buf` is undersized.
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize>;
    // Enforced via `debug_assert!` in the primary impl (`FileSectorSource`);
    // release builds panic on the out-of-bounds slice instead.

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
        // See docs/sector.md — Pass-N marginal-sector FUA rationale and the
        // ~10x throughput cost of applying it to the bulk path.
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
}

pub use crate::io::file_sector_source::FileSectorSource;
pub use decrypting::{DecryptingSectorSource, KeyFetch, KeyFetchFn};
pub use prefetched::PrefetchedSectorSource;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // Instrumented SectorSource: records reads, capacity, and set_speed
    // calls so the forwarding-impl tests can prove each trait method is
    // delegated, not stubbed.
    struct Spy {
        capacity: u32,
        reads: Arc<Mutex<Vec<(u32, u16, bool)>>>,
        speeds: Arc<Mutex<Vec<u16>>>,
        unit_bases: Arc<Mutex<Vec<u32>>>,
    }

    /// A `Spy` under test plus the handles recording its reads, speed sets,
    /// and unit-base sets.
    type SpyHarness = (
        Spy,
        Arc<Mutex<Vec<(u32, u16, bool)>>>,
        Arc<Mutex<Vec<u16>>>,
        Arc<Mutex<Vec<u32>>>,
    );

    impl Spy {
        fn new(capacity: u32) -> SpyHarness {
            let reads = Arc::new(Mutex::new(Vec::new()));
            let speeds = Arc::new(Mutex::new(Vec::new()));
            let unit_bases = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    capacity,
                    reads: reads.clone(),
                    speeds: speeds.clone(),
                    unit_bases: unit_bases.clone(),
                },
                reads,
                speeds,
                unit_bases,
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
        fn set_unit_base(&mut self, lba: u32) {
            self.unit_bases.lock().unwrap().push(lba);
        }
    }

    // Routes through a generic `S: SectorSource` bound so the forwarding
    // impls (not the vtable) are exercised. See docs/sector.md.
    fn set_unit_base_generic<S: SectorSource>(mut s: S, base: u32) {
        s.set_unit_base(base);
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

    // `Box<dyn SectorSource>` must forward capacity, read_sectors, and
    // set_speed to the inner source, so boxed sources satisfy generic
    // decorator bounds.
    #[test]
    fn boxed_dyn_forwards_all_methods() {
        let (spy, reads, speeds, unit_bases) = Spy::new(777);
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

        // set_unit_base through the generic bound exercises the forwarding impl
        // (a direct `boxed.set_unit_base()` would vtable-dispatch instead). A
        // missing forwarding body would silently no-op and record nothing.
        set_unit_base_generic(boxed, 64);
        assert_eq!(
            *unit_bases.lock().unwrap(),
            vec![64],
            "set_unit_base must forward through Box<dyn>"
        );
    }

    /// `&mut dyn SectorSource` must likewise forward every method.
    /// Grounding: `impl SectorSource for &mut (dyn SectorSource + '_)`.
    #[test]
    fn mut_ref_dyn_forwards_all_methods() {
        let (mut spy, reads, speeds, unit_bases) = Spy::new(123);

        {
            let r: &mut dyn SectorSource = &mut spy;
            assert_eq!(r.capacity_sectors(), 123);

            let mut buf = vec![0u8; 2 * 2048];
            let n = r.read_sectors(7, 2, &mut buf, false).unwrap();
            assert_eq!(n, 2 * 2048);

            r.set_speed(8800);
        }

        // Pass `&mut dyn` as a generic S so the forwarding impl's set_unit_base
        // is the one under test, not the vtable path.
        let r2: &mut dyn SectorSource = &mut spy;
        set_unit_base_generic(r2, 128);

        assert_eq!(*reads.lock().unwrap(), vec![(7, 2, false)]);
        assert_eq!(*speeds.lock().unwrap(), vec![8800]);
        assert_eq!(
            *unit_bases.lock().unwrap(),
            vec![128],
            "set_unit_base must forward through &mut dyn"
        );
    }

    // Records every read, incl. whether it arrived via the FUA entry point
    // (`Spy` leaves `read_sectors_fua` to the trait default, so it can't
    // tell). `fua` is `None` when the plain `read_sectors` path was hit.
    type ReadLog = Arc<Mutex<Vec<(u32, u16, bool, Option<bool>)>>>;

    #[derive(Default)]
    struct ReadSpy {
        calls: ReadLog,
        fill: u8,
    }

    impl SectorSource for ReadSpy {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            self.calls
                .lock()
                .unwrap()
                .push((lba, count, recovery, None));
            let bytes = count as usize * 2048;
            buf[..bytes].fill(self.fill);
            Ok(bytes)
        }
        fn read_sectors_fua(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
            fua: bool,
        ) -> Result<usize> {
            self.calls
                .lock()
                .unwrap()
                .push((lba, count, recovery, Some(fua)));
            let bytes = count as usize * 2048;
            buf[..bytes].fill(self.fill);
            Ok(bytes)
        }
    }

    // Routes through a generic `S: SectorSource` bound so the forwarding
    // impl (not the vtable) runs. See docs/sector.md.
    fn read_generic<S: SectorSource>(
        mut s: S,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        s.read_sectors(lba, count, buf, recovery)
    }

    /// Same, for the speed lever. The trait's own `set_speed` default is a
    /// no-op, so a forwarding body that also did nothing is indistinguishable
    /// from the default unless the call is routed through the generic bound.
    fn set_speed_generic<S: SectorSource>(mut s: S, kbs: u16) {
        s.set_speed(kbs);
    }

    /// Same, for the FUA entry point.
    fn read_fua_generic<S: SectorSource>(
        mut s: S,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        s.read_sectors_fua(lba, count, buf, recovery, fua)
    }

    // Proves the forwarding impl actually delegates `read_sectors`, unlike
    // `mut_ref_dyn_forwards_all_methods` (vtable dispatch). See
    // docs/sector.md.
    #[test]
    fn mut_ref_dyn_forwards_read_sectors_to_the_inner_source() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut inner = ReadSpy {
            calls: calls.clone(),
            fill: 0x5C,
        };

        let mut buf = vec![0u8; 4 * 2048];
        let r: &mut dyn SectorSource = &mut inner;
        let n = read_generic(r, 0x1234, 4, &mut buf, true).expect("delegated read succeeds");

        assert_eq!(
            n,
            4 * 2048,
            "the forwarding impl must return the INNER source's byte count"
        );
        assert!(
            buf.iter().all(|b| *b == 0x5C),
            "the inner source's bytes must land in the caller's buffer; an \
             undelegated read leaves it untouched and the caller muxes zeroes"
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(0x1234, 4, true, None)],
            "lba/count/recovery must reach the inner source unchanged, on the \
             non-FUA entry point"
        );
    }

    // Same, for `read_sectors_fua`: the forwarder must reach the inner
    // source's FUA entry point, carrying the `fua` bit through. See
    // docs/sector.md.
    #[test]
    fn mut_ref_dyn_forwards_read_sectors_fua_to_the_inner_source() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut inner = ReadSpy {
            calls: calls.clone(),
            fill: 0x3B,
        };

        let mut buf = vec![0u8; 2 * 2048];
        let r: &mut dyn SectorSource = &mut inner;
        let n = read_fua_generic(r, 42, 2, &mut buf, false, true).expect("delegated FUA read");

        assert_eq!(n, 2 * 2048, "byte count must come from the inner source");
        assert!(
            buf.iter().all(|b| *b == 0x3B),
            "the inner source's bytes must land in the caller's buffer"
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(42, 2, false, Some(true))],
            "the FUA entry point must be the one reached, with fua=true intact"
        );
    }

    // The forwarding impl must delegate `set_speed`, which hides worse than
    // the read methods because the trait default is also a no-op. See
    // docs/sector.md.
    #[test]
    fn mut_ref_dyn_forwards_set_speed_to_the_inner_source() {
        let (mut spy, _reads, speeds, _bases) = Spy::new(0);
        let r: &mut dyn SectorSource = &mut spy;
        set_speed_generic(r, 5540);

        assert_eq!(
            *speeds.lock().unwrap(),
            vec![5540],
            "the forwarding impl must pass set_speed through to the inner \
             source; swallowing it is indistinguishable from the trait default \
             and silently disables recovery-path throttling"
        );
    }

    /// The same for `Box<dyn SectorSource>`, which is the receiver the mux read
    /// paths actually hold.
    #[test]
    fn boxed_dyn_forwards_set_speed_to_the_inner_source() {
        let (spy, _reads, speeds, _bases) = Spy::new(0);
        let b: Box<dyn SectorSource> = Box::new(spy);
        set_speed_generic(b, 11080);

        assert_eq!(
            *speeds.lock().unwrap(),
            vec![11080],
            "the boxed forwarding impl must pass set_speed through unchanged"
        );
    }
}

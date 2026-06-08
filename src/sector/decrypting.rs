//! `DecryptingSectorSource` — wrap any [`SectorSource`] to apply
//! AACS / CSS in-place decryption on every read.
//!
//! This is the single source of truth for decrypt-on-read: every
//! decrypt-on-read caller (e.g. `DiscStream`) wraps its source in this
//! decorator. The actual cipher code lives in [`crate::aacs`] and
//! [`crate::css`]; we just call the existing
//! [`crate::decrypt::decrypt_sectors`] helper that drives both of them
//! in-place after each read (a no-op for [`DecryptKeys::None`]).
//!
//! Composition: `Drive` → `DecryptingSectorSource` → caller sees
//! plaintext. For `DecryptKeys::None` discs the decorator is a
//! pass-through, so callers can wire it unconditionally and keep
//! their pipeline shape uniform regardless of encryption state.

use crate::decrypt::{DecryptKeys, decrypt_sectors};
use crate::error::Result;

use super::SectorSource;

/// Decorator: read from `inner`, then run the configured
/// AACS / CSS decrypt over the bytes that landed in `buf`.
///
/// `unit_key_idx` selects the AACS unit key for the disc (0 for
/// the vast majority of titles; the rare multi-CPS-unit discs pick
/// the index that covers the title being read). For
/// [`DecryptKeys::None`] and [`DecryptKeys::Css`] the index is
/// ignored.
pub struct DecryptingSectorSource<S: SectorSource> {
    inner: S,
    keys: DecryptKeys,
    unit_key_idx: usize,
}

impl<S: SectorSource> DecryptingSectorSource<S> {
    /// Wrap `inner` with the given keys. The default unit-key
    /// index is 0; use [`with_unit_key_idx`] for the multi-CPS-unit
    /// case.
    ///
    /// [`with_unit_key_idx`]: Self::with_unit_key_idx
    pub fn new(inner: S, keys: DecryptKeys) -> Self {
        Self {
            inner,
            keys,
            unit_key_idx: 0,
        }
    }

    /// Override the AACS unit-key index. Only meaningful for
    /// [`DecryptKeys::Aacs`]; other variants ignore it.
    pub fn with_unit_key_idx(mut self, idx: usize) -> Self {
        self.unit_key_idx = idx;
        self
    }

    /// Replace the configured keys without unwrapping the decorator.
    /// Used by `DiscStream::set_raw()` to flip from encrypted-disc
    /// decryption to a pass-through after the inner reader is already
    /// owned by the wrapper. For new construction prefer [`new`].
    ///
    /// [`new`]: Self::new
    pub fn set_keys(&mut self, keys: DecryptKeys) {
        self.keys = keys;
    }

    /// Borrow the inner source. Useful for tests and for adapters
    /// that want to introspect the underlying drive / file without
    /// unwrapping the decorator.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Mutable borrow of the inner source.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume the decorator and return the underlying source.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: SectorSource> SectorSource for DecryptingSectorSource<S> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        let n = self.inner.read_sectors(lba, count, buf, recovery)?;
        // Apply the crate-wide AACS/CSS/None decrypt entry point in-place
        // over the bytes just read. No-op for DecryptKeys::None.
        decrypt_sectors(&mut buf[..n], &self.keys, self.unit_key_idx)?;
        Ok(n)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    /// Synthetic SectorSource that yields a deterministic byte
    /// pattern keyed by LBA. Used to verify the decorator's
    /// pass-through behaviour for `DecryptKeys::None`.
    struct PatternedSource {
        capacity: u32,
    }

    impl PatternedSource {
        fn fill(lba: u32, count: u16, buf: &mut [u8]) {
            let bytes = count as usize * 2048;
            for (i, slot) in buf[..bytes].iter_mut().enumerate() {
                let abs = lba as u64 * 2048 + i as u64;
                *slot = ((abs.wrapping_mul(2654435761) >> 16) & 0xff) as u8;
            }
        }
    }

    impl SectorSource for PatternedSource {
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }

        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Self::fill(lba, count, buf);
            Ok(count as usize * 2048)
        }
    }

    #[test]
    fn passthrough_with_no_keys() {
        let src = PatternedSource { capacity: 16 };
        let mut wrapped = DecryptingSectorSource::new(src, DecryptKeys::None);

        // capacity_sectors delegates.
        assert_eq!(wrapped.capacity_sectors(), 16);

        let mut got = vec![0u8; 4 * 2048];
        let n = wrapped.read_sectors(3, 4, &mut got, false).unwrap();
        assert_eq!(n, 4 * 2048);

        let mut expected = vec![0u8; 4 * 2048];
        PatternedSource::fill(3, 4, &mut expected);
        assert_eq!(got, expected);
    }

    #[test]
    fn passthrough_set_speed_delegates() {
        struct SpeedRecorder {
            last: Option<u16>,
        }
        impl SectorSource for SpeedRecorder {
            fn capacity_sectors(&self) -> u32 {
                0
            }
            fn read_sectors(
                &mut self,
                _lba: u32,
                _count: u16,
                _buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                Ok(0)
            }
            fn set_speed(&mut self, kbs: u16) {
                self.last = Some(kbs);
            }
        }

        let mut wrapped =
            DecryptingSectorSource::new(SpeedRecorder { last: None }, DecryptKeys::None);
        wrapped.set_speed(7200);
        assert_eq!(wrapped.inner().last, Some(7200));
    }

    // TODO: AACS round-trip test — needs a fixture-encrypted unit
    // (6144-byte aligned) plus the matching unit key. The cipher
    // path itself is exercised by `crate::aacs` unit tests; here
    // we only assert the decorator wires the existing helper, not
    // that AES-128 is correct.

    // ---------------------------------------------------------------
    // Additional coverage.
    // ---------------------------------------------------------------

    use std::sync::{Arc, Mutex};

    /// Source that fills the FULL requested span with a CSS-scrambled-
    /// FLAGGED sector pattern (byte 0x14 scramble bits set, non-zero
    /// data) but reports a SHORTER read (`report_n`). With a CSS key the
    /// decorator must descramble ONLY `buf[..report_n]`; the bytes
    /// beyond `report_n` must stay exactly as filled. A whole-`buf`
    /// decrypt would clear the flagged sector's scramble bits and XOR
    /// its data region — observable here.
    struct ShortReportSource {
        report_n: usize,
    }
    impl ShortReportSource {
        fn fill_one(buf: &mut [u8]) {
            for (i, b) in buf.iter_mut().enumerate() {
                *b = (i as u8).wrapping_mul(29).wrapping_add(3);
            }
            buf[0x14] = 0x30; // scramble-control bits set → flags == 0x03
        }
    }
    impl SectorSource for ShortReportSource {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            for s in 0..count as usize {
                Self::fill_one(&mut buf[s * 2048..(s + 1) * 2048]);
            }
            Ok(self.report_n)
        }
    }

    /// Records the (lba, count, recovery) the decorator forwarded.
    struct ArgRecorder {
        calls: Arc<Mutex<Vec<(u32, u16, bool)>>>,
    }
    impl SectorSource for ArgRecorder {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            self.calls.lock().unwrap().push((lba, count, recovery));
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }
    }

    /// A source whose read returns an error — the decorator must
    /// propagate it and NOT call decrypt afterward (decrypt over an
    /// unwritten buffer would be at best wasted work, at worst a panic
    /// for a missing AACS key). Grounding: `read_sectors` uses `?` on
    /// the inner read before `decrypt_sectors`.
    struct FailingSource;
    impl SectorSource for FailingSource {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Err(crate::error::Error::IoError {
                source: std::io::Error::from(std::io::ErrorKind::TimedOut),
            })
        }
    }

    /// The CSS path is a no-op for sectors whose scrambling-control
    /// bits are clear. Per CSS, the sector's mode-2 subheader byte at
    /// offset 0x14 carries the copyright/scramble flags; descrambling
    /// only runs when `(byte[0x14] >> 4) & 0x03 != 0`. With those bits
    /// clear (byte 0x14 == 0) the descrambler returns immediately, so
    /// the decorator must hand back the bytes unchanged. Grounding:
    /// `css::lfsr::descramble_sector` early-return on `flags == 0`.
    #[test]
    fn css_unscrambled_sector_passes_through() {
        struct FixedSector {
            template: [u8; 2048],
        }
        impl SectorSource for FixedSector {
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                for s in 0..count as usize {
                    buf[s * 2048..(s + 1) * 2048].copy_from_slice(&self.template);
                }
                Ok(bytes)
            }
        }

        let mut template = [0u8; 2048];
        for (i, b) in template.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(13).wrapping_add(7);
        }
        // Byte 0x14: clear the scramble-control bits (bits 4-5) so the
        // descrambler treats the sector as already in the clear.
        template[0x14] = 0x00;
        let expected = template;

        let mut wrapped = DecryptingSectorSource::new(
            FixedSector { template },
            DecryptKeys::Css {
                title_key: [0x11, 0x22, 0x33, 0x44, 0x55],
            },
        );
        let mut got = [0u8; 2048];
        let n = wrapped.read_sectors(0, 1, &mut got, false).unwrap();
        assert_eq!(n, 2048);
        assert_eq!(
            got, expected,
            "unscrambled CSS sector (flags=0) must pass through untouched"
        );
    }

    /// The decorator must decrypt ONLY the `n` bytes the inner source
    /// reported as read — never the full `buf`. We use a CSS key and a
    /// sector whose flags ARE set (so descramble would mutate bytes if
    /// applied), but the inner source reports a short `n` of 0. With
    /// n=0 the decrypt span is empty, so the whole buffer must come
    /// back exactly as the inner source filled it. Grounding:
    /// `decrypt_sectors(&mut buf[..n], ...)`.
    #[test]
    fn decrypt_span_bounded_by_reported_n() {
        // Inner fills a CSS-scrambled-FLAGGED sector but reports n=0, so
        // the decrypt span is empty and the buffer must come back
        // byte-identical to what the inner source wrote. A whole-`buf`
        // decrypt would clear byte 0x14's scramble bits and XOR the data
        // region — this asserts that does NOT happen for the n=0 span.
        let mut wrapped = DecryptingSectorSource::new(
            ShortReportSource { report_n: 0 },
            DecryptKeys::Css {
                title_key: [1, 2, 3, 4, 5],
            },
        );
        let mut expected = vec![0u8; 2048];
        ShortReportSource::fill_one(&mut expected);

        let mut got = vec![0u8; 2048];
        let n = wrapped.read_sectors(5, 1, &mut got, false).unwrap();
        assert_eq!(n, 0, "decorator must return the inner source's n");
        assert_eq!(
            got, expected,
            "with n=0 the decrypt span is empty; buffer must be untouched"
        );
        // Belt-and-braces: the scramble flag bits must still be set
        // (a whole-buf descramble would have cleared them).
        assert_eq!(got[0x14] & 0x30, 0x30, "scramble flags must remain set");
    }

    /// lba / count / recovery must be forwarded to the inner source
    /// verbatim. Grounding: `read_sectors` calls
    /// `self.inner.read_sectors(lba, count, buf, recovery)`.
    #[test]
    fn args_forwarded_verbatim() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut wrapped = DecryptingSectorSource::new(
            ArgRecorder {
                calls: calls.clone(),
            },
            DecryptKeys::None,
        );
        let mut buf = vec![0u8; 2 * 2048];
        wrapped.read_sectors(12345, 2, &mut buf, true).unwrap();
        wrapped.read_sectors(0, 1, &mut buf, false).unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(12345, 2, true), (0, 1, false)],
            "lba/count/recovery must pass through unchanged"
        );
    }

    /// A read error from the inner source must propagate unchanged and
    /// the decrypt step must NOT run after it. Grounding: the `?` on the
    /// inner read in `read_sectors`.
    #[test]
    fn inner_read_error_propagates() {
        let mut wrapped = DecryptingSectorSource::new(FailingSource, DecryptKeys::None);
        let mut buf = vec![0u8; 2048];
        let r = wrapped.read_sectors(0, 1, &mut buf, false);
        let err = r.expect_err("inner error must propagate");
        let io: std::io::Error = err.into();
        assert_eq!(io.kind(), std::io::ErrorKind::TimedOut);
    }

    /// With AACS keys but an out-of-range `unit_key_idx`, the decrypt
    /// step must fail (DecryptFailed) rather than silently returning
    /// still-encrypted bytes. Grounding: `decrypt_sectors`' unit-key
    /// lookup — `unit_keys.get(idx)` → None → Error::DecryptFailed.
    #[test]
    fn aacs_missing_unit_key_errors() {
        let src = PatternedSource { capacity: 16 };
        // idx 0 requested, but unit_keys is empty → get(0) == None.
        let mut wrapped = DecryptingSectorSource::new(
            src,
            DecryptKeys::Aacs {
                unit_keys: Vec::new(),
                read_data_key: None,
            },
        );
        let mut buf = vec![0u8; 2048];
        let r = wrapped.read_sectors(0, 1, &mut buf, false);
        let err = r.expect_err("missing unit key must error, not pass through encrypted");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "must surface DecryptFailed"
        );
    }

    /// A source that yields exactly one CLEAR AACS aligned unit (6144
    /// bytes = 3 sectors) with MPEG-TS sync bytes (0x47) at the BD-TS
    /// stride (offset 4, then every 192 bytes). `is_aacs_scrambled`
    /// reports such a unit as NOT scrambled, so the AACS decrypt path
    /// reaches the per-unit closure and leaves it untouched — letting
    /// us prove the unit-key LOOKUP (not the cipher) is what fails for
    /// an out-of-range index.
    struct ClearUnitSource;
    impl SectorSource for ClearUnitSource {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            // BD-TS sync byte at offset 4 of every 192-byte packet.
            let mut off = 4usize;
            while off < bytes {
                buf[off] = 0x47;
                off += 192;
            }
            Ok(bytes)
        }
    }

    /// `with_unit_key_idx` selects which unit key the AACS path uses.
    /// idx=2 against a single populated key is out of range → the
    /// `unit_keys.get(idx)` lookup returns None → DecryptFailed. idx=0
    /// is in range → the lookup succeeds, and on a clear (TS-sync
    /// intact) full unit the cipher is a no-op, so the read returns Ok
    /// with the bytes unchanged. Grounding: `decrypt_sectors`'
    /// `unit_keys.get(unit_key_idx)`.
    #[test]
    fn with_unit_key_idx_selects_key() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0u32, [0u8; 16])],
            read_data_key: None,
        };
        // 3 sectors = one 6144-byte aligned unit (so partial_len == 0).
        let mut buf = vec![0u8; 3 * 2048];

        // idx=2 out of range → lookup fails.
        let mut bad =
            DecryptingSectorSource::new(ClearUnitSource, keys.clone()).with_unit_key_idx(2);
        assert!(
            bad.read_sectors(0, 3, &mut buf, false).is_err(),
            "out-of-range unit_key_idx must fail the lookup"
        );

        // idx=0 in range → lookup ok, clear unit left untouched.
        let mut good = DecryptingSectorSource::new(ClearUnitSource, keys).with_unit_key_idx(0);
        let mut buf2 = vec![0u8; 3 * 2048];
        let n = good.read_sectors(0, 3, &mut buf2, false).unwrap();
        assert_eq!(n, 3 * 2048);
        // Clear unit: sync byte preserved at offset 4.
        assert_eq!(
            buf2[4], 0x47,
            "clear unit must be left intact under valid idx"
        );
    }

    /// `set_keys` must replace the active keys mid-life. We use a
    /// CSS-SCRAMBLED-flagged sector (byte 0x14 scramble bits set) so the
    /// effect of the active key is observable: under a CSS key the
    /// descrambler XORs a keystream into bytes 128..2048 AND clears the
    /// scramble flags (`sector[0x14] &= 0xCF`); under `None` the bytes
    /// pass through unchanged. Flipping keys mid-life must change which
    /// behavior runs. Grounding: `set_keys` + `css::lfsr::descramble_sector`
    /// (keystream XOR + flag-clear on flags != 0).
    #[test]
    fn set_keys_swaps_active_keys() {
        struct ScrambledSector {
            template: [u8; 2048],
        }
        impl SectorSource for ScrambledSector {
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                for s in 0..count as usize {
                    buf[s * 2048..(s + 1) * 2048].copy_from_slice(&self.template);
                }
                Ok(bytes)
            }
        }

        // Build a sector flagged as scrambled (bits 4-5 of byte 0x14
        // set) with non-zero payload so the keystream XOR is visible.
        let mut template = [0u8; 2048];
        for (i, b) in template.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(29).wrapping_add(3);
        }
        template[0x14] = 0x30; // scramble bits (4-5) set → flags == 0x03
        let pristine = template;

        // Start with None → pass-through (no descramble, flags stay set).
        let mut wrapped =
            DecryptingSectorSource::new(ScrambledSector { template }, DecryptKeys::None);
        let mut got = [0u8; 2048];
        wrapped.read_sectors(0, 1, &mut got, false).unwrap();
        assert_eq!(
            got, pristine,
            "None keys must pass the sector through unchanged"
        );
        assert_eq!(
            got[0x14] & 0x30,
            0x30,
            "None must leave the scramble flags set"
        );

        // Swap to a CSS key: now the descrambler runs and must clear the
        // scramble flags (and XOR the data region), so the bytes differ.
        wrapped.set_keys(DecryptKeys::Css {
            title_key: [0xa1, 0xb2, 0xc3, 0xd4, 0xe5],
        });
        let mut got2 = [0u8; 2048];
        wrapped.read_sectors(0, 1, &mut got2, false).unwrap();
        assert_eq!(
            got2[0x14] & 0x30,
            0x00,
            "CSS descramble must clear the scramble-control bits"
        );
        assert_ne!(
            &got2[128..2048],
            &pristine[128..2048],
            "CSS descramble must alter the encrypted data region"
        );
    }

    /// `into_inner` / `inner` / `inner_mut` must hand back the original
    /// source unchanged. Grounding: the accessor methods.
    #[test]
    fn inner_accessors_round_trip() {
        let src = PatternedSource { capacity: 42 };
        let mut wrapped = DecryptingSectorSource::new(src, DecryptKeys::None);
        assert_eq!(wrapped.inner().capacity_sectors(), 42);
        assert_eq!(wrapped.inner_mut().capacity_sectors(), 42);
        let recovered = wrapped.into_inner();
        assert_eq!(recovered.capacity_sectors(), 42);
    }
}

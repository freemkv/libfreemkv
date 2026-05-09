//! `DecryptingSectorSource` — wrap any [`SectorSource`] to apply
//! AACS / CSS in-place decryption on every read.
//!
//! This is the 0.18 single-source-of-truth for decrypt-on-read. The
//! actual cipher code lives in [`crate::aacs`] and [`crate::css`];
//! we just call the existing [`crate::decrypt::decrypt_sectors`]
//! helper that already drives both of them. In follow-up commits
//! `sweep_pipeline` and `DiscStream` migrate onto this decorator
//! and delete their duplicate decrypt call sites.
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
        // Reuse the existing crate-wide decrypt entry point — same
        // path the 0.17 sweep_pipeline and DiscStream call, so we
        // inherit their AACS / CSS / None semantics verbatim. The
        // helper is a no-op for DecryptKeys::None.
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
}

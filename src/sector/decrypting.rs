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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Base LBA of the encrypted region currently being read — the clip /
    /// extent `start_lba` that AACS aligned units are anchored at. The unit-
    /// alignment gate measures `lba` relative to THIS, not absolute disc LBA 0,
    /// so a clip whose `start_lba` is not 3-aligned still gates correctly. Set
    /// per-extent by the mux read paths via [`set_unit_base`]; defaults to 0
    /// (absolute alignment) for callers that read from a 3-aligned base.
    ///
    /// [`set_unit_base`]: Self::set_unit_base
    unit_base: u32,
    /// Cumulative bytes of scrambled AACS units that no key could decrypt.
    /// `decrypt_sectors` restores those bytes to their original ciphertext (so a
    /// clear nav-file is never corrupted), but for genuine encrypted content the
    /// still-encrypted bytes are silently dropped by the downstream TS assembler
    /// — real, unaccounted loss. Mux read paths share this counter into their
    /// loss accounting (via [`decrypt_loss`]) so a partial AACS/CSS decrypt
    /// failure can't be reported as a perfect rip. Shared `Arc` so the highway's
    /// producer thread and the consuming `Stream` see the same tally.
    ///
    /// [`decrypt_loss`]: Self::decrypt_loss
    decrypt_dropped: Arc<AtomicU64>,
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
            unit_base: 0,
            decrypt_dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A handle to this decorator's decrypt-loss counter — the cumulative bytes
    /// of scrambled AACS units that no key could decrypt (see
    /// [`decrypt_dropped`](Self::decrypt_dropped)). The mux pipelines read this
    /// to fold decrypt-time loss into their `lost_bytes` accounting; the highway
    /// shares it across the producer thread and the consuming `Stream`. Returns
    /// the live counter, so reads after a decrypt observe the updated total.
    pub fn decrypt_loss(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.decrypt_dropped)
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
        // Defense-in-depth: AACS aligned units are 3 sectors (6144 bytes) and
        // `decrypt_sectors` anchors units at buffer offset 0. A read that does
        // not begin a whole number of units past the encrypted region's base
        // (`unit_base`, the clip/extent start_lba) would decrypt every unit
        // under the wrong CBC/unit alignment and silently mis-decrypt. Reject
        // loud (DecryptFailed) BEFORE reading rather than ever mis-decrypting.
        // The gate is measured RELATIVE to `unit_base` (set per-extent by the
        // mux read paths via `set_unit_base`), never absolute `lba % 3` — a clip
        // whose start_lba is not itself 3-aligned must still gate on its own
        // units (else its readable units are wrongly rejected → "Decryption
        // failed" on exactly those titles).
        if matches!(self.keys, DecryptKeys::Aacs { .. })
            && !crate::aacs::is_unit_aligned(lba, self.unit_base)
        {
            return Err(crate::error::Error::DecryptFailed);
        }
        let n = self.inner.read_sectors(lba, count, buf, recovery)?;
        // Apply the crate-wide AACS/CSS/None decrypt entry point in-place
        // over the bytes just read. No-op for DecryptKeys::None. The returned
        // count is bytes of scrambled units no key could decrypt — silent
        // decrypt loss the TS assembler will drop. Tally it so the mux loss
        // accounting (and the abort gate) can see partial decrypt failure.
        let dropped = decrypt_sectors(&mut buf[..n], &mut self.keys, self.unit_key_idx)?;
        if dropped > 0 {
            self.decrypt_dropped
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
        Ok(n)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }

    fn set_unit_base(&mut self, lba: u32) {
        self.unit_base = lba;
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

    /// Defense-in-depth: an AACS decrypting read whose START LBA is not
    /// unit-aligned (lba % 3 != 0) must be rejected with DecryptFailed BEFORE
    /// touching the cipher — a mid-unit start would decrypt every unit under the
    /// wrong CBC/unit alignment and silently mis-decrypt. A unit-aligned start
    /// (lba % 3 == 0) must pass the guard and proceed normally.
    ///
    /// Grounding: the `lba % UNIT_SECTORS != 0` guard in `read_sectors`.
    #[test]
    fn aacs_unaligned_start_lba_rejected() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0u32, [0u8; 16])],
            read_data_key: None,
        };
        // Unaligned starts (1, 2, 4, 5, 32 — note 32 % 3 == 2) must all reject.
        for lba in [1u32, 2, 4, 5, 32, 64] {
            let mut wrapped = DecryptingSectorSource::new(ClearUnitSource, keys.clone());
            let mut buf = vec![0u8; 3 * 2048];
            let r = wrapped.read_sectors(lba, 3, &mut buf, false);
            let err = r.expect_err("unaligned AACS start LBA must reject");
            assert_eq!(
                err.code(),
                crate::error::Error::DecryptFailed.code(),
                "lba {lba} (% 3 = {}) must reject with DecryptFailed",
                lba % 3
            );
        }
        // Unit-aligned starts (0, 3, 33, 66) must pass the guard. ClearUnitSource
        // yields TS-clear units, so decrypt is a no-op and the read succeeds.
        for lba in [0u32, 3, 33, 66] {
            let mut wrapped = DecryptingSectorSource::new(ClearUnitSource, keys.clone());
            let mut buf = vec![0u8; 3 * 2048];
            let n = wrapped
                .read_sectors(lba, 3, &mut buf, false)
                .unwrap_or_else(|_| panic!("aligned lba {lba} must pass the guard"));
            assert_eq!(n, 3 * 2048);
        }
    }

    /// Clip-anchored gate (the Watership Down "Decryption failed" regression):
    /// AACS aligned units are anchored at the clip's encrypted-region start
    /// (`unit_base`), NOT absolute disc LBA 0. A clip whose `start_lba` is not
    /// itself 3-aligned must gate on ITS OWN units, so the clip's base LBA
    /// (which the old `lba % 3` gate wrongly rejected) now passes, and only
    /// reads off the clip-relative unit grid reject.
    #[test]
    fn aacs_gate_is_clip_anchored_not_absolute() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0u32, [0u8; 16])],
            read_data_key: None,
        };
        // base = 64 (abs % 3 == 1): the non-3-aligned clip start that triggered
        // the bug. The old absolute gate rejected every read here; the clip-
        // anchored gate must accept the clip's own unit grid.
        let base = 64u32;

        // Clip-relative aligned starts (base + {0,3,6,30}) pass.
        for off in [0u32, 3, 6, 30] {
            let mut w = DecryptingSectorSource::new(ClearUnitSource, keys.clone());
            w.set_unit_base(base);
            let mut buf = vec![0u8; 3 * 2048];
            let n = w
                .read_sectors(base + off, 3, &mut buf, false)
                .unwrap_or_else(|_| panic!("clip-relative aligned lba {} must pass", base + off));
            assert_eq!(n, 3 * 2048);
        }

        // The clip's base LBA itself (abs % 3 == 1) — the exact read the old gate
        // wrongly rejected — must now decrypt.
        let mut w = DecryptingSectorSource::new(ClearUnitSource, keys.clone());
        w.set_unit_base(base);
        let mut buf = vec![0u8; 3 * 2048];
        assert!(
            w.read_sectors(base, 3, &mut buf, false).is_ok(),
            "a clip starting at a non-3-aligned LBA must decrypt from its own base"
        );

        // Clip-relative MISaligned starts (base + {1,2,4,5}) still reject.
        for off in [1u32, 2, 4, 5] {
            let mut w = DecryptingSectorSource::new(ClearUnitSource, keys.clone());
            w.set_unit_base(base);
            let mut buf = vec![0u8; 3 * 2048];
            let err = w
                .read_sectors(base + off, 3, &mut buf, false)
                .expect_err("clip-relative unaligned start must reject");
            assert_eq!(
                err.code(),
                crate::error::Error::DecryptFailed.code(),
                "base+{off} is off the clip-relative unit grid"
            );
        }
    }

    /// The unit-alignment guard is AACS-only. A CSS decrypting read (per-sector,
    /// stateless — DVDs) must NOT be gated on a 3-sector boundary: a single
    /// sector at lba 1 must read fine. Grounding: the guard is inside
    /// `matches!(self.keys, DecryptKeys::Aacs { .. })`.
    #[test]
    fn css_start_lba_not_unit_gated() {
        let mut wrapped = DecryptingSectorSource::new(
            ClearUnitSource,
            DecryptKeys::Css {
                title_key: [0u8; 5],
            },
        );
        let mut buf = vec![0u8; 2048];
        // lba 1 (not a multiple of 3) must succeed under CSS — no AACS gate.
        let n = wrapped.read_sectors(1, 1, &mut buf, false).unwrap();
        assert_eq!(n, 2048, "CSS reads must not be unit-alignment gated");
    }

    /// Build a clear 6144-byte AACS unit (TS syncs at the BD-TS stride) then
    /// encrypt it under `unit_key` so `aacs::decrypt_unit` recovers it. Mirrors
    /// the encrypt helper in `crate::decrypt`'s tests.
    fn encrypt_aacs_unit(unit_key: &[u8; 16]) -> Vec<u8> {
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        let mut unit = vec![0u8; crate::aacs::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < unit.len() {
            unit[off] = 0x47;
            off += 192;
        }
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = crate::aacs::decrypt::aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = crate::aacs::decrypt::AACS_IV;
        let blocks = (crate::aacs::ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..blocks {
            let o = 16 + i * 16;
            for j in 0..16 {
                unit[o + j] ^= prev[j];
            }
            let mut blk = GenericArray::clone_from_slice(&unit[o..o + 16]);
            cipher.encrypt_block(&mut blk);
            unit[o..o + 16].copy_from_slice(&blk);
            prev.copy_from_slice(&unit[o..o + 16]);
        }
        unit
    }

    /// Regression: when the decrypt step can't decrypt a scrambled AACS unit
    /// (wrong/missing key), the decorator must accumulate the dropped bytes in
    /// its `decrypt_loss()` counter while STILL returning `Ok` (per-unit
    /// tolerance). The mux pipelines read this counter into `lost_bytes()` so a
    /// partial decrypt failure can't be reported as a perfect rip. A
    /// decryptable unit must leave the counter at zero.
    ///
    /// Grounding: `read_sectors` folds `decrypt_sectors`' dropped count into
    /// `decrypt_dropped`; `decrypt_loss()` exposes it.
    #[test]
    fn decrypt_loss_counter_accumulates_undecryptable_units() {
        let real_key = [0x33u8; 16];
        let wrong_key = [0x44u8; 16];

        // A source that always yields one unit encrypted under `real_key`.
        struct EncUnitSource {
            unit: Vec<u8>,
        }
        impl SectorSource for EncUnitSource {
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                assert_eq!(bytes, self.unit.len(), "test reads one whole unit");
                buf[..bytes].copy_from_slice(&self.unit);
                Ok(bytes)
            }
        }

        let unit = encrypt_aacs_unit(&real_key);

        // Wrong key → undecryptable → loss counted, read still Ok.
        let mut wrapped = DecryptingSectorSource::new(
            EncUnitSource { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)],
                read_data_key: None,
            },
        );
        let loss = wrapped.decrypt_loss();
        assert_eq!(loss.load(Ordering::Relaxed), 0, "starts at zero");

        let mut buf = vec![0u8; 3 * 2048];
        wrapped
            .read_sectors(0, 3, &mut buf, false)
            .expect("undecryptable unit must NOT hard-error (per-unit tolerance)");
        assert_eq!(
            loss.load(Ordering::Relaxed),
            crate::aacs::ALIGNED_UNIT_LEN as u64,
            "one undecryptable unit must add its byte length to the loss counter"
        );

        // A second read of the same bad unit accumulates further.
        wrapped.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(
            loss.load(Ordering::Relaxed),
            2 * crate::aacs::ALIGNED_UNIT_LEN as u64,
            "loss must accumulate across reads"
        );

        // Correct key → no loss.
        let mut good = DecryptingSectorSource::new(
            EncUnitSource { unit },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, real_key)],
                read_data_key: None,
            },
        );
        let good_loss = good.decrypt_loss();
        good.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(
            good_loss.load(Ordering::Relaxed),
            0,
            "a decryptable unit must not register any loss"
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

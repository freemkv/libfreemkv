//! `DecryptingSectorSource` — wrap any [`SectorSource`] to apply
//! AACS / CSS in-place decryption on every read.
//!
//! The cipher code lives in [`crate::aacs`] and [`crate::css`]; this
//! decorator calls [`crate::decrypt::decrypt_sectors`] after each read
//! (a no-op for [`DecryptKeys::None`]).
//!
//! Composition: `Drive` → `DecryptingSectorSource` → caller sees
//! plaintext; `DecryptKeys::None` discs pass through unconditionally.
//!
//! See docs/decrypting-sector-source.md for the single-source-of-truth rationale.

use crate::decrypt::{DecryptKeys, decrypt_sectors, decrypt_sectors_in_content};
use crate::error::Result;
use std::sync::Arc;

use super::SectorSource;

/// A closure resolving keys from encrypted-content samples — the shape of both
/// [`KeyFetch`] operations. Named so the two constructors (and the struct fields)
/// read clearly.
pub type KeyFetchFn = std::sync::Arc<dyn Fn(&[Vec<u8>]) -> Vec<[u8; 16]> + Send + Sync>;

/// Resolves keys from encrypted-content samples for [`DecryptingSectorSource`].
///
/// Two operations: [`unit_keys`](Self::unit_keys) resolves the base Unit
/// Key(s) for a CPS unit from real encrypted samples; [`fmts_indexes`](Self::fmts_indexes)
/// resolves the disc's AACS 2.1 forensic index key set from an index-1
/// anchor batch. Both return additional keys to add to the pool and retry
/// with; empty if the source can't help. The library does no key lookup or
/// network I/O itself — this is the caller's seam to its key source.
///
/// See docs/decrypting-sector-source.md for the two-operation rationale.
#[derive(Clone)]
pub struct KeyFetch {
    unit: KeyFetchFn,
    fmts: KeyFetchFn,
}

impl KeyFetch {
    /// Build a resolver from its two operations: `unit` resolves base Unit Keys
    /// from a CPS unit's samples; `fmts` resolves the forensic index set from an
    /// index-1 anchor batch.
    pub fn new(unit: KeyFetchFn, fmts: KeyFetchFn) -> Self {
        Self { unit, fmts }
    }

    /// A resolver that serves ONLY base Unit Keys; [`fmts_indexes`](Self::fmts_indexes)
    /// is always empty. For read paths that never resolve forensic keys — the
    /// sweep/patch recovery decorator, which handles CPS units only.
    pub fn unit_only(unit: KeyFetchFn) -> Self {
        Self::new(unit, std::sync::Arc::new(|_| Vec::new()))
    }

    /// Resolve the base Unit Key(s) for a CPS unit from `samples` (real encrypted
    /// units drawn from it). Normally one key; the caller adds whatever it returns
    /// to the pool.
    pub fn unit_keys(&self, samples: &[Vec<u8>]) -> Vec<[u8; 16]> {
        (self.unit)(samples)
    }

    /// Resolve the disc's AACS 2.1 forensic index keys from an index-1 single-
    /// phase `anchor` batch. The source returns the COMPLETE ordered set (index i
    /// = element i); the caller trusts any non-empty result as all of them.
    pub fn fmts_indexes(&self, anchor: &[Vec<u8>]) -> Vec<[u8; 16]> {
        (self.fmts)(anchor)
    }
}

/// Decorator: read from `inner`, then run the configured
/// AACS / CSS decrypt over the bytes that landed in `buf`.
///
/// AACS decrypts EXCLUSIVELY through the installed [`key_map`](Self::key_map)
/// (one key per CPS unit / segment, resolved up front); CSS self-descrambles on
/// its per-sector scramble flag; [`DecryptKeys::None`] is a pass-through.
pub struct DecryptingSectorSource<S: SectorSource> {
    inner: S,
    keys: DecryptKeys,
    /// Base LBA of the encrypted region currently being read — the clip /
    /// extent `start_lba` that AACS aligned units are anchored at. The unit-
    /// alignment gate measures `lba` relative to THIS, not absolute disc LBA 0,
    /// so a clip whose `start_lba` is not 3-aligned still gates correctly. Set
    /// per-extent by the mux read paths via [`set_unit_base`]; defaults to 0
    /// (absolute alignment) for callers that read from a 3-aligned base.
    ///
    /// [`set_unit_base`]: Self::set_unit_base
    unit_base: u32,
    /// Encrypted-content extent map — the disc's m2ts ranges as sorted/merged
    /// `(start_lba, sector_count)` (see
    /// [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    /// When `Some`, a unit whose absolute LBA is OUTSIDE these ranges is clear
    /// (UDF filesystem / BDMV nav) and is passed through untouched: never
    /// decrypted, verified, or counted as loss. `None` means "the caller only
    /// reads encrypted content" (the mux reads title extents only) → every unit
    /// is treated as content (the legacy behaviour).
    content_ranges: Option<Arc<[(u32, u32)]>>,
    /// Proactive AACS key map (see [`crate::decrypt::AacsKeyMap`]). When set, the
    /// caller resolved one key per CPS unit / segment UP FRONT, so this read
    /// decrypts each aligned unit with its MAPPED key and TRUSTS it — no per-unit
    /// `is_clean` verdict, no key-server storm. `None` is a clear / CSS source
    /// (CSS self-descrambles in `decrypt_sectors`); an AACS source without a map
    /// is a bug and fails loud on the first unit (AACS decrypts only via the map).
    key_map: Option<Arc<crate::decrypt::AacsKeyMap>>,
}

impl<S: SectorSource> DecryptingSectorSource<S> {
    /// Wrap `inner` with the given keys. For an AACS source, install a key map
    /// via [`with_key_map`](Self::with_key_map) before reading — AACS decrypts
    /// only through the map and fails loud without one.
    pub fn new(inner: S, keys: DecryptKeys) -> Self {
        Self {
            inner,
            keys,
            unit_base: 0,
            content_ranges: None,
            key_map: None,
        }
    }

    /// Install a proactive [`AacsKeyMap`](crate::decrypt::AacsKeyMap): the caller
    /// resolved one key per CPS unit / segment up front, so every aligned unit is
    /// decrypted with its MAPPED key and trusted — no per-unit `is_clean` check.
    /// AACS-only; a CSS / clear disc ignores it.
    pub fn with_key_map(mut self, map: Arc<crate::decrypt::AacsKeyMap>) -> Self {
        self.key_map = Some(map);
        self
    }

    /// `&mut` counterpart of [`with_key_map`](Self::with_key_map): install the
    /// proactive map on an already-constructed source (the inline live-drive
    /// [`DiscStream`](crate::mux::DiscStream) builds the decorator first, then
    /// installs the map via its own `with_key_map`).
    pub fn set_key_map(&mut self, map: Arc<crate::decrypt::AacsKeyMap>) {
        self.key_map = Some(map);
    }

    /// Restrict decrypt to the disc's encrypted-content extents
    /// (sorted/merged `(start_lba, sector_count)` — see
    /// [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    /// Units outside content (UDF filesystem / BDMV nav) pass through untouched,
    /// so the TS-sync content check is never consulted
    /// about non-content bytes. Whole-disc readers (sweep / patch) set this; the
    /// mux leaves it unset because it only ever reads title extents.
    pub fn with_content_ranges(mut self, ranges: Arc<[(u32, u32)]>) -> Self {
        self.content_ranges = Some(ranges);
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

    // Decrypt `buf` with the active keys, gated by content ranges when set.
    // Shared by the first read and post-fetch retry so both agree on which
    // units are content and the unit-key try order.
    fn decrypt_buf(
        buf: &mut [u8],
        keys: &mut DecryptKeys,
        lba: u32,
        content: Option<&[(u32, u32)]>,
    ) -> Result<usize> {
        // The `unit_key_idx` arg on `decrypt_sectors[_in_content]` is a legacy
        // inert param (AACS is map-only; CSS/None ignore it) — pass 0.
        match content {
            Some(ranges) => decrypt_sectors_in_content(buf, keys, 0, lba, ranges),
            None => decrypt_sectors(buf, keys, 0),
        }
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
        // Bulk path: no Force Unit Access (the cache IS the streaming
        // throughput). FUA is a Pass-N recovery lever threaded through
        // `read_sectors_fua`.
        self.read_sectors_fua(lba, count, buf, recovery, false)
    }

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        // Defense-in-depth: AACS units are 3-sector aligned; misalignment would
        // silently mis-decrypt, so reject loud (DecryptFailed) before reading.
        // Gated relative to `unit_base` (per-extent), not raw `lba % 3`.
        if matches!(self.keys, DecryptKeys::Aacs { .. })
            && !crate::aacs::content::is_unit_aligned(lba, self.unit_base)
        {
            return Err(crate::error::Error::DecryptFailed);
        }
        let n = self
            .inner
            .read_sectors_fua(lba, count, buf, recovery, fua)?;

        // Proactive map path (storm-free mux): keys were resolved per unit up
        // front, so decrypt with the mapped key and trust it, no per-unit
        // `is_clean` check. A resolver gap fails loud; bad TS passes through.
        if let Some(map) = self.key_map.clone() {
            crate::decrypt::decrypt_sectors_mapped(&mut buf[..n], &self.keys, lba, &map)?;
            return Ok(n);
        }

        // Decrypt `buf` in place (None / CSS / AACS); with a content map, units
        // outside the encrypted extents pass through untouched. A can't-decrypt
        // fails loud; broken-TS output is the muxer's concern, not a read failure.
        let content = self.content_ranges.clone(); // cheap Arc bump; frees the &self borrow
        let content_ref = content.as_deref();

        // No map installed: CSS self-descramble / clear pass-through. A can't-
        // decrypt (misalignment, or mapless AACS reaching here — a bug) fails
        // loud; otherwise units pass through, same as above.
        Self::decrypt_buf(&mut buf[..n], &mut self.keys, lba, content_ref)?;
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

    // TODO: AACS round-trip test needs a fixture-encrypted unit + matching key
    // (`crate::aacs` tests already exercise the cipher itself).

    // Additional coverage:

    use std::sync::{Arc, Mutex};

    // Fills the full span with a CSS-scrambled-flagged sector but reports a
    // shorter read (`report_n`); with a CSS key, only `buf[..report_n]` must
    // be descrambled — bytes beyond it must stay exactly as filled.
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

    // A source whose read errors — the decorator must propagate it and NOT
    // call decrypt afterward (over an unwritten buffer, at best wasted work,
    // at worst a panic for a missing AACS key).
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

    // CSS is a no-op when the mode-2 subheader byte 0x14 scramble-control
    // bits are clear (`css::lfsr::descramble_sector` early-returns on
    // `flags == 0`), so the decorator must hand bytes back unchanged.
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

    // Decorator must decrypt only the reported `n` bytes, never full `buf`.
    // With a CSS-flagged sector but n=0, the whole buffer must come back
    // exactly as filled (`decrypt_sectors(&mut buf[..n], ...)`).
    #[test]
    fn decrypt_span_bounded_by_reported_n() {
        // Inner fills a CSS-scrambled-FLAGGED sector but reports n=0, so the
        // decrypt span is empty and the buffer must come back byte-identical.
        // A whole-`buf` decrypt would clear the scramble bits / XOR the data.
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

    // AACS reaching decrypt without an installed key map must fail loud
    // (DecryptFailed), not silently return still-encrypted bytes — the
    // map-only model: decrypt_sectors' AACS arm always errors without one.
    #[test]
    fn aacs_missing_unit_key_errors() {
        let src = PatternedSource { capacity: 16 };
        // idx 0 requested, but unit_keys is empty → get(0) == None.
        let mut wrapped = DecryptingSectorSource::new(
            src,
            DecryptKeys::Aacs {
                unit_keys: Vec::new(),
                read_data_key: None,
                format: crate::disc::ContentFormat::BdTs,
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

    // Yields one clear AACS aligned unit (6144 bytes = 3 sectors) with TS
    // sync bytes at the BD-TS stride; `is_clean` reports it unscrambled, so
    // decrypt reaches the per-unit closure — isolating key LOOKUP failures.
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

    // set_keys must replace the active keys mid-life. Uses a CSS-scrambled
    // sector: under CSS the descrambler XORs data and clears scramble flags;
    // under None bytes pass through — flipping keys must change which runs.
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
        // Real scrambled DVD sectors are MPEG-2 PS packs; the scramble policy
        // requires the pack start code as well as the flag bits.
        template[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
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

    // Unit-alignment guard is AACS-only; a CSS read (per-sector, stateless)
    // must not be gated on a 3-sector boundary — lba 1 must read fine. Guard
    // is inside `matches!(self.keys, DecryptKeys::Aacs { .. })`.
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

    // The clear 6144-byte AACS unit `encrypt_aacs_unit` encrypts: zeroes
    // except TS sync 0x47 at the BD-TS stride and CPI bits on byte 0. Exposed
    // separately so decrypt tests can assert byte-exact plaintext recovery.
    fn clear_aacs_unit() -> Vec<u8> {
        let mut unit = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < unit.len() {
            unit[off] = 0x47;
            off += 192;
        }
        // CPI bits on byte 0 so it reads as encrypted; set before key derivation.
        unit[0] |= 0xC0;
        unit
    }

    /// Build a clear 6144-byte AACS unit (TS syncs at the BD-TS stride) then
    /// encrypt it under `unit_key` so `aacs::content::decrypt_unit` recovers it.
    fn encrypt_aacs_unit(unit_key: &[u8; 16]) -> Vec<u8> {
        let mut unit = clear_aacs_unit();
        assert!(
            crate::aacs::content::encrypt_unit(&mut unit, unit_key),
            "a full-length unit must encrypt"
        );
        unit
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

    /// Source that returns a fixed unit's bytes for any read.
    struct FixedUnit {
        unit: Vec<u8>,
    }
    impl SectorSource for FixedUnit {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let bytes = count as usize * 2048;
            buf[..bytes].copy_from_slice(&self.unit);
            Ok(bytes)
        }
    }

    // End-to-end AACS: an encrypted unit read through the decorator with a
    // matching AacsKeyMap comes back as the known plaintext — the shipping
    // mapped-decrypt path, previously covered only via the deleted reactive path.
    #[test]
    fn aacs_decorator_decrypts_encrypted_unit_via_map() {
        let key = [0x5Au8; 16];
        let unit = encrypt_aacs_unit(&key);
        let src = FixedUnit { unit };
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = std::sync::Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            0,
            u32::MAX,
            0,
        )]));
        let mut dec = DecryptingSectorSource::new(src, keys).with_key_map(map);
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let n = dec.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n, crate::aacs::content::ALIGNED_UNIT_LEN);
        // The plaintext is fully known, so assert byte-exact recovery rather than
        // spot-checking the TS syncs: checking only 0x47 at the 192-byte stride let
        // corruption anywhere in the other 6112 bytes pass undetected.
        assert_eq!(
            buf,
            clear_aacs_unit(),
            "the decrypted unit must equal the known plaintext byte-for-byte"
        );
    }

    /// An AACS decorator built WITHOUT a key map must fail loud on the first unit —
    /// the map is mandatory for AACS (it decrypts only via the mapped path). Guards
    /// the class of bug the TrueHD probe shipped (a mapless AACS `DecryptingSectorSource`).
    #[test]
    fn aacs_decorator_without_map_fails_loud() {
        let key = [0x5Au8; 16];
        let unit = encrypt_aacs_unit(&key);
        let src = FixedUnit { unit };
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut dec = DecryptingSectorSource::new(src, keys); // no with_key_map
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let err = dec
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("AACS decorator with no key map must fail loud");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
    }
}

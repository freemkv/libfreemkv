//! `DecryptingSectorSource` — wrap any [`SectorSource`] to apply
//! AACS / CSS in-place decryption on every read.
//!
//! The cipher code lives in [`crate::aacs`] and [`crate::css`]; this
//! decorator calls [`crate::decrypt::decrypt_sectors`] after each read
//! (a no-op for [`DecryptKeys::None`]).
//!
//! Composition: `Drive` → `DecryptingSectorSource` → caller sees
//! plaintext; `DecryptKeys::None` discs pass through unconditionally.

use crate::decrypt::{DecryptKeys, decrypt_sectors, decrypt_sectors_in_content};
use crate::error::{E_CSS_KEY_MISSING, Result};
use std::sync::Arc;

use super::SectorSource;

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
    /// per-extent via [`set_unit_base`]. `None` (the default): there is no implicit
    /// disc-LBA-0 grid, so an AACS content read fails loud until a base is set —
    /// callers of [`new`](Self::new) must anchor before reading AACS content.
    ///
    /// [`set_unit_base`]: Self::set_unit_base
    unit_base: Option<u32>,
    /// Encrypted-content map: every stream file's sorted/merged `(start_lba,
    /// sector_count)` (e.g. [`Disc::stream_content_ranges`](crate::Disc::stream_content_ranges)).
    /// When `Some`, a unit outside these ranges is clear and passed through
    /// untouched: never decrypted, verified, or counted as loss. `None` means
    /// the caller reads only encrypted content, so every unit is content.
    content_ranges: Option<Arc<[(u32, u32)]>>,
    /// Proactive AACS key map (see [`crate::decrypt::AacsKeyMap`]). When set, the
    /// caller resolved one key per CPS unit / segment UP FRONT, so this read
    /// decrypts each aligned unit with its MAPPED key and TRUSTS it — no per-unit
    /// `is_clean` verdict, no key-server storm. `None` is a clear / CSS source
    /// (CSS self-descrambles in `decrypt_sectors`); an AACS source without a map
    /// is a bug and fails loud on the first unit (AACS decrypts only via the map).
    key_map: Option<Arc<crate::decrypt::AacsKeyMap>>,
    /// The key set's on-arrival proof for pieces `resolve` could not prove up front
    /// (KU §2.4). `None` for every reader not built by a `ResolvedKeySet`.
    arrival: Option<Box<crate::keys::Arrival>>,
    /// Damaged AACS units blanked so far (see [`blanked_units`](Self::blanked_units)).
    blanked: BlankTally,
    /// The run that also hears of each blanked unit (`UnitBlanked`, `Stats`).
    ctx: Option<crate::ctx::Ctx>,
    /// Content-detected stage: `Some` until the first read reaches the verdict.
    detect: Option<Box<StageOptions>>,
    /// Loose BD-TS: a CPI-flagged unit that is clean TS is clear (its flag is stale).
    stale_cpi: bool,
    /// A PS the crack left keyless: a scrambled pack read later is refused (E7023).
    watch_css: bool,
    /// The crack's verdict was scrambled-but-uncrackable: every read refuses (E7023).
    refused: bool,
}

/// What the content-detected stage may do with what it finds (from `InputOptions`).
#[derive(Clone, Default)]
pub(crate) struct StageOptions {
    /// Pass ciphertext through: never crack, decrypt or refuse.
    pub(crate) raw: bool,
    /// Held AACS keys for a loose BD-TS file.
    pub(crate) keys: Option<crate::keys::ResolvedKeySet>,
    /// The run: its halt ends the crack scan, its stats count blanked units.
    pub(crate) ctx: crate::ctx::Ctx,
}

// Sectors per CSS crack-scan read (the mpg:// batch).
const CRACK_BATCH: u16 = 8192;

// The blanked-unit count, shared with the stream that reports it as loss; logged once when
// the reader is dropped, so a damaged rip never reads as clean.
struct BlankTally(Arc<std::sync::atomic::AtomicU64>);

impl Drop for BlankTally {
    fn drop(&mut self) {
        let n = self.0.load(std::sync::atomic::Ordering::Relaxed);
        if n > 0 {
            tracing::warn!(target: "freemkv::decrypt", units = n, "{n} damaged AACS units blanked");
        }
    }
}

/// Does the sector span `[lba, lba+count)` intersect any encrypted-content range?
/// `None` content (the mux reads title extents only) means "always content" so the
/// AACS unit-alignment gate stays enforced. When a content map IS set (whole-disc
/// readers), a span touching NO content range is clear filesystem/nav and must be
/// exempt from the alignment gate — it will pass through undecrypted.
fn span_touches_content(content: Option<&[(u32, u32)]>, lba: u32, count: u16) -> bool {
    content.is_none_or(|r| crate::decrypt::span_in_content_ranges(lba, count as u32, r))
}

impl<S: SectorSource> DecryptingSectorSource<S> {
    /// Wrap `inner` with the given keys. AACS decrypts only through a key map, which
    /// only a [`ResolvedKeySet`](crate::keys::ResolvedKeySet) reader installs; an AACS
    /// source built here fails loud on its first encrypted unit.
    pub fn new(inner: S, keys: DecryptKeys) -> Self {
        Self {
            inner,
            keys,
            unit_base: None,
            content_ranges: None,
            key_map: None,
            arrival: None,
            blanked: BlankTally(Arc::default()),
            ctx: None,
            detect: None,
            stale_cpi: false,
            watch_css: false,
            refused: false,
        }
    }

    /// The content-detected stage (every input but a disc, image or folder): the first read
    /// classifies the head and resolves once (D3). A PS is cracked, a BD-TS read through
    /// [`ResolvedKeySet::loose_file`](crate::keys::ResolvedKeySet::loose_file), else clear.
    pub(crate) fn detecting(inner: S, opts: StageOptions) -> Self {
        #[cfg(test)]
        super::stage::STAGES.with(|n| n.set(n.get() + 1));
        let mut s = Self::new(inner, DecryptKeys::None);
        s.observe(&opts.ctx);
        s.detect = Some(Box::new(opts));
        s
    }

    /// Report each blanked unit to `ctx` too: an `UnitBlanked` event and its loss counter.
    pub(crate) fn observe(&mut self, ctx: &crate::ctx::Ctx) {
        self.ctx = Some(ctx.clone());
    }

    // Reach the verdict and install what it needs. On `Err` the stage stays undecided.
    fn resolve_detect(&mut self) -> Result<()> {
        use super::stage::{Kind, classify_sectors};
        let Some(opts) = self.detect.as_deref() else {
            return Ok(());
        };
        let cap = self.inner.capacity_sectors();
        let head = detect_head(&mut self.inner, cap)?;
        match classify_sectors(&head) {
            Kind::Ps { mpeg2 } if !opts.raw => {
                // B2: an 11172-1 stream cannot be CSS; it is never cracked.
                if mpeg2 {
                    let whole = [crate::disc::Extent {
                        start_lba: 0,
                        sector_count: cap,
                    }];
                    let halt = Some(&opts.ctx.halt);
                    let cracked =
                        crate::css::crack_title_key(&mut self.inner, &whole, CRACK_BATCH, halt);
                    self.keys = match cracked {
                        Ok(keys) => keys,
                        // A verdict, not a fault: kept, so a retry does not scan again (D3).
                        Err(e) if crate::error::error_code(&e) == Some(E_CSS_KEY_MISSING) => {
                            self.detect = None;
                            self.refused = true;
                            return Err(crate::error::Error::CssKeyMissing);
                        }
                        Err(e) => return Err(e.into()),
                    };
                }
                self.watch_css = !self.keys.is_encrypted();
            }
            Kind::BdTs if !opts.raw => {
                let set = crate::keys::ResolvedKeySet::loose_file(opts.keys.as_ref(), cap);
                self.keys = set.decrypt_keys();
                self.key_map = Some(set.key_map());
                self.arrival = set.arrival(set.title_stop()).map(Box::new);
                self.unit_base = Some(0);
                self.stale_cpi = true;
            }
            // Nothing seen that a key opens, yet a scrambled pack later is still refused.
            _ => self.watch_css = !opts.raw,
        }
        self.detect = None;
        Ok(())
    }

    /// Install the key set's on-arrival proof (KU §2.4): an encrypted unit of a piece
    /// `resolve` left unproven is decrypted with a held key proven on it here.
    pub(crate) fn with_arrival(mut self, arrival: crate::keys::Arrival) -> Self {
        self.arrival = Some(Box::new(arrival));
        self
    }

    /// `&mut` counterpart of [`with_arrival`](Self::with_arrival).
    pub(crate) fn set_arrival(&mut self, arrival: crate::keys::Arrival) {
        self.arrival = Some(Box::new(arrival));
    }

    /// Install a proactive [`AacsKeyMap`](crate::decrypt::AacsKeyMap): the caller
    /// resolved one key per CPS unit / segment up front, so every aligned unit is
    /// decrypted with its MAPPED key and trusted — no per-unit `is_clean` check.
    /// AACS-only; a CSS / clear disc ignores it.
    pub(crate) fn with_key_map(mut self, map: Arc<crate::decrypt::AacsKeyMap>) -> Self {
        self.key_map = Some(map);
        self
    }

    /// `&mut` counterpart of [`with_key_map`](Self::with_key_map): install the
    /// proactive map on an already-constructed source (the inline live-drive
    /// [`DiscStream`](crate::mux::DiscStream) builds the decorator first, then
    /// installs the map via its own `with_key_map`).
    pub(crate) fn set_key_map(&mut self, map: Arc<crate::decrypt::AacsKeyMap>) {
        self.key_map = Some(map);
    }

    /// Restrict decrypt to every stream file's extents (e.g.
    /// [`Disc::stream_content_ranges`](crate::Disc::stream_content_ranges),
    /// not the title-only [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    /// Units outside pass through untouched, never TS-sync checked. Whole-disc
    /// readers (sweep / patch) set this; the mux, reading title extents, does not.
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

    /// Drop the unit base: AACS content reads fail loud until
    /// [`set_unit_base`](SectorSource::set_unit_base) anchors the next one.
    pub fn clear_unit_base(&mut self) {
        self.unit_base = None;
    }

    /// Damaged AACS units this reader blanked (zero-filled) so far: flagged units no key can
    /// open, read damage the rip carries on past.
    pub fn blanked_units(&self) -> u64 {
        self.blanked.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The shared blanked-unit counter, for a stream that reports it after this reader moves.
    pub(crate) fn blanked_counter(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.blanked.0.clone()
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

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
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
        if self.refused {
            return Err(crate::error::Error::CssKeyMissing);
        }
        if self.detect.is_some() {
            self.resolve_detect()?;
        }
        // Content-extent map (whole-disc readers): units outside the encrypted
        // extents are clear filesystem / nav and pass through untouched. Cheap Arc
        // bump; frees the &self borrow so we can decrypt against `&mut buf`.
        let content = self.content_ranges.clone();
        let content_ref = content.as_deref();

        // AACS units are 3-sector aligned on each file's grid; a content read with no
        // base, or off it, silently mis-decrypts, so reject loud before reading. Only
        // spans that TOUCH content are gated (UDF/nav passes through).
        if matches!(self.keys, DecryptKeys::Aacs { .. })
            && span_touches_content(content_ref, lba, count)
            && !self
                .unit_base
                .is_some_and(|b| crate::aacs::content::is_unit_aligned(lba, b))
        {
            tracing::warn!(target: "freemkv::decrypt", lba, base = ?self.unit_base, "AACS content read off the unit grid");
            return Err(crate::error::Error::DecryptFailed);
        }
        let n = self
            .inner
            .read_sectors_fua(lba, count, buf, recovery, fua)?;
        self.judge_detected(&mut buf[..n])?;

        // Read damage before any key sees the units: a damaged unit is blanked and counted,
        // never a key verdict (E7013 here, E7022 on arrival). "We rip bad discs."
        self.blank_damage(lba, &mut buf[..n], content_ref);

        // KU §2.4: units of a piece left unproven are proven now, from the held keys only.
        if let (Some(arrival), DecryptKeys::Aacs { unit_keys, .. }) =
            (self.arrival.as_deref(), &self.keys)
        {
            let blanked = arrival.process(&mut self.inner, lba, &mut buf[..n], unit_keys)?;
            self.count_blanked(lba, blanked);
        }

        // Proactive map path (storm-free mux): keys were resolved per unit up front,
        // so decrypt with the mapped key and trust it (no per-unit `is_clean`). A
        // resolver gap fails loud; bad TS and units outside content extents pass through.
        if let Some(map) = self.key_map.clone() {
            let blanked = crate::decrypt::decrypt_sectors_mapped_in_content(
                &mut buf[..n],
                &self.keys,
                lba,
                &map,
                content_ref,
            )?;
            self.count_blanked(lba, blanked);
            return Ok(n);
        }

        // No map installed: `decrypt_buf` does CSS self-descramble / clear pass-through
        // (units outside the content extents pass through untouched). A can't-decrypt
        // (misalignment, or mapless AACS here — a bug) fails loud; broken TS is the muxer's concern.
        Self::decrypt_buf(&mut buf[..n], &mut self.keys, lba, content_ref)?;
        Ok(n)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }

    fn set_unit_base(&mut self, lba: u32) {
        self.unit_base = Some(lba);
    }
}

// The verdict's head: `HEAD_SECTORS` from the first written sector on the AACS unit grid, so
// a zero-filled bad-read start hides nothing. Blank sectors are skipped up to the crack budget.
fn detect_head(inner: &mut impl SectorSource, cap: u32) -> Result<Vec<u8>> {
    use super::stage::HEAD_SECTORS;
    const BLANK_BUDGET: u32 = 50_000;
    const UNIT: u32 = crate::aacs::content::ALIGNED_UNIT_SECTORS;
    let mut at = 0;
    loop {
        let head = read_head_at(inner, at, cap)?;
        let blank = head
            .chunks(crate::consts::SECTOR_BYTES)
            .take_while(|s| s.iter().all(|&b| b == 0))
            .count() as u32;
        let full = HEAD_SECTORS as usize * crate::consts::SECTOR_BYTES;
        if (blank as usize) * crate::consts::SECTOR_BYTES < head.len() {
            let start = (at + blank) / UNIT * UNIT;
            return match start == at {
                true => Ok(head),
                false => read_head_at(inner, start, cap),
            };
        }
        if head.len() < full || at + HEAD_SECTORS >= BLANK_BUDGET {
            return Ok(head);
        }
        at += HEAD_SECTORS;
    }
}

// Up to `HEAD_SECTORS` sectors at `lba`, fewer at the end of the source.
fn read_head_at(inner: &mut impl SectorSource, lba: u32, cap: u32) -> Result<Vec<u8>> {
    let n = cap.saturating_sub(lba).min(super::stage::HEAD_SECTORS);
    let mut head = vec![0u8; n as usize * crate::consts::SECTOR_BYTES];
    let got = match n {
        0 => 0,
        n => inner.read_sectors(lba, n as u16, &mut head, false)?,
    };
    head.truncate(got);
    Ok(head)
}

impl<S: SectorSource> DecryptingSectorSource<S> {
    // The content-detected stage's per-read rules, before any key sees the bytes: a stale CPI
    // flag on clear TS is cleared (KS-5: "00₂ if the data is not encrypted"), and a
    // keyless PS refuses a scrambled pack rather than pass ciphertext as clear.
    fn judge_detected(&self, buf: &mut [u8]) -> Result<()> {
        use crate::aacs::content::{ALIGNED_UNIT_LEN, aacs_unit_seed_encrypted};
        if self.stale_cpi {
            let ts = crate::disc::ContentFormat::BdTs;
            for unit in buf.chunks_mut(ALIGNED_UNIT_LEN) {
                if aacs_unit_seed_encrypted(unit, ts) && super::stage::clear_ts(unit) {
                    for p in unit.chunks_mut(crate::consts::BD_SOURCE_PACKET_BYTES) {
                        p[0] &= 0x3F;
                    }
                }
            }
        }
        if self.watch_css
            && buf
                .chunks(crate::consts::SECTOR_BYTES)
                .any(|c| crate::css::scrambled_at(c).is_some())
        {
            return Err(crate::error::Error::CssKeyMissing);
        }
        Ok(())
    }

    // Blank and count the damaged AACS units of a read at `lba` (see
    // `decrypt::blank_damaged_units`): judged are content units the map keys or arrival covers.
    fn blank_damage(&mut self, lba: u32, buf: &mut [u8], content: Option<&[(u32, u32)]>) {
        let DecryptKeys::Aacs { format, .. } = self.keys else {
            return;
        };
        let (map, arrival) = (self.key_map.as_deref(), self.arrival.as_deref());
        let covered = |at: u32| {
            content.is_none_or(|r| crate::decrypt::span_in_content_ranges(at, 1, r))
                && (map.is_some_and(|m| m.entry_for(at).is_some())
                    || arrival.is_some_and(|a| a.covers(at)))
        };
        let cap = self.inner.capacity_sectors();
        let end = lba as u64 + buf.len().div_ceil(crate::consts::SECTOR_BYTES) as u64;
        let at_end = cap != 0 && end >= cap as u64;
        let n = crate::decrypt::blank_damaged_units(buf, lba, format, &covered, at_end);
        self.count_blanked(lba, n);
    }

    fn count_blanked(&self, lba: u32, n: usize) {
        self.blanked
            .0
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        if let (Some(ctx), 1..) = (&self.ctx, n) {
            ctx.stats.add_blanked(n as u64);
            ctx.emit(crate::event::Event::UnitBlanked {
                lba: u64::from(lba),
                units: n as u64,
            });
        }
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

    // N8: an uncrackable scrambled stream's E7023 is a verdict: a retry refuses again without
    // a second crack scan (D3).
    #[test]
    fn a_css_refusal_is_kept_for_the_retry() {
        let mut pack: Vec<u8> = (0..2048u32)
            .map(|i| (i as u8).wrapping_mul(7) ^ 0x3C)
            .collect();
        pack[0x14] = 0x10;
        crate::css::dvd_pack_header(&mut pack, 0xE0);
        crate::css::lfsr::scramble_sector(&[0x11, 0x22, 0x33, 0x44, 0x55], &mut pack);
        let src = crate::test_util::MemSource::new(pack.repeat(4));
        let opts = StageOptions {
            raw: false,
            keys: None,
            ctx: Default::default(),
        };
        let mut stage = DecryptingSectorSource::detecting(src, opts);
        let scans = crate::css::CRACK_SCANS.with(|n| n.get());
        let mut buf = vec![0u8; 2048];
        for _ in 0..2 {
            let e = stage.read_sectors(0, 1, &mut buf, false).unwrap_err();
            assert_eq!(e.code(), crate::error::E_CSS_KEY_MISSING);
        }
        assert_eq!(crate::css::CRACK_SCANS.with(|n| n.get()), scans + 1);
    }

    // Stale CPI is cleared only on clear TS: ciphertext that keeps a few syncs (the `is_clean`
    // proof floor) stays flagged, while damaged packets do not hide a clear unit.
    #[test]
    fn stale_cpi_needs_half_the_packets_synced() {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        use crate::sector::stage::clear_ts;
        let pkt = crate::consts::BD_SOURCE_PACKET_BYTES;
        let mut unit: Vec<u8> = (0..ALIGNED_UNIT_LEN)
            .map(|i| (i * 13 + 5) as u8 | 1)
            .collect();
        for p in unit.chunks_mut(pkt) {
            p[0] |= 0xC0;
            p[4] = 0x47;
        }
        assert!(clear_ts(&unit));
        assert!(clear_ts(&unit[..20 * pkt]), "a clear partial tail");
        // Review #2: ten damaged packets of 31 leave a clear unit clear.
        for p in unit.chunks_mut(pkt).skip(1).take(10) {
            p[4] = 0x9D;
        }
        assert!(clear_ts(&unit), "ten damaged packets");
        for (i, p) in unit.chunks_mut(pkt).enumerate().skip(1) {
            p[4] = if i < 5 { 0x47 } else { 0x9D };
        }
        assert!(crate::aacs::content::is_clean(
            &unit,
            crate::disc::ContentFormat::BdTs
        ));
        assert!(!clear_ts(&unit), "four synced packets are not clear TS");
    }

    // A DecryptingSectorSource must relay its inner source's unmapped list.
    #[test]
    fn decrypting_source_forwards_unmapped_stream_files() {
        use crate::sector::bus_removal::test_support::{Reports, m2ts1};
        let w = DecryptingSectorSource::new(Reports(vec![m2ts1()]), DecryptKeys::None);
        crate::sector::bus_removal::test_support::assert_forwards(w);
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
            // A real DVD-Video pack header, or `is_scrambled_pack` skips the sector.
            buf[0x14] = 0x30; // scramble-control bits set → flags == 0x03
            crate::css::dvd_pack_header(buf, 0xE0);
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

    // Records the fua flag of every read; the default read_sectors_fua drops it.
    struct FuaProbe {
        fua: Vec<bool>,
    }
    impl SectorSource for FuaProbe {
        fn read_sectors(&mut self, _: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
            let n = count as usize * 2048;
            buf[..n].fill(0);
            Ok(n)
        }
        fn read_sectors_fua(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
            fua: bool,
        ) -> Result<usize> {
            self.fua.push(fua);
            self.read_sectors(lba, count, buf, recovery)
        }
    }

    // Pass-N FUA recovery must reach the drive through the decrypting decorator.
    #[test]
    fn read_sectors_fua_forwards_fua_to_the_inner_source() {
        let mut d = DecryptingSectorSource::new(FuaProbe { fua: vec![] }, DecryptKeys::None);
        let mut buf = vec![0u8; 2048];
        d.read_sectors_fua(0, 1, &mut buf, true, true).unwrap();
        d.read_sectors_fua(0, 1, &mut buf, true, false).unwrap();
        assert_eq!(d.inner().fua, [true, false]);
    }

    // The decorator reports its inner's answer, not a constant: over a prefetcher it is false.
    #[test]
    fn random_access_follows_the_inner_source() {
        let _serial = crate::sector::prefetched::holder_test_lock();
        let ext = vec![crate::disc::Extent {
            start_lba: 0,
            sector_count: 6,
        }];
        let inner = ArgRecorder {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let pf =
            crate::sector::PrefetchedSectorSource::new(inner, ext, 3, &crate::ctx::Ctx::default())
                .unwrap();
        let d = DecryptingSectorSource::new(pf, DecryptKeys::None);
        assert!(!d.random_access());
        let d = DecryptingSectorSource::new(FuaProbe { fua: vec![] }, DecryptKeys::None);
        assert!(d.random_access());
    }

    /// A read error from the inner source must propagate unchanged.
    /// Grounding: the `?` on the inner read in `read_sectors`.
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
    // (DecryptFailed), not silently return still-encrypted bytes — even with an
    // empty key pool (the map-only model: no map ⇒ always an error).
    #[test]
    fn aacs_without_key_map_and_empty_pool_errors() {
        let src = PatternedSource { capacity: 16 };
        let mut wrapped = DecryptingSectorSource::new(
            src,
            DecryptKeys::Aacs {
                unit_keys: Vec::new(),
                format: crate::disc::ContentFormat::BdTs,
            },
        );
        // On the unit grid, so the alignment gate passes and the mapless arm answers.
        wrapped.set_unit_base(0);
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
        template[4] = 0x44; // '01': a 13818-1 pack
        template[0x0D] = 0xF8; // pack_stuffing_length 0
        template[0x14] = 0x30; // scramble bits (4-5) set → flags == 0x03
        crate::css::dvd_pack_header(&mut template, 0xE0);
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
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = std::sync::Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            0,
            u32::MAX,
            0,
        )]));
        let mut dec = DecryptingSectorSource::new(src, keys).with_key_map(map);
        dec.set_unit_base(0);
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let n = dec.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n, crate::aacs::content::ALIGNED_UNIT_LEN);
        // The plaintext is fully known, so assert byte-exact recovery rather than
        // spot-checking the TS syncs: checking only 0x47 at the 192-byte stride let
        // corruption anywhere in the other 6112 bytes pass undetected.
        assert_eq!(
            buf,
            crate::aacs::content::cpi_cleared(clear_aacs_unit()),
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
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut dec = DecryptingSectorSource::new(src, keys); // no with_key_map
        dec.set_unit_base(0);
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let err = dec
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("AACS decorator with no key map must fail loud");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
    }

    // `with_content_ranges` contract: an encrypted unit whose LBA is OUTSIDE the
    // disc's content extents passes through untouched (never decrypted). Before the
    // fix the mapped path ignored the content map, so this unit was decrypted.
    #[test]
    fn content_ranges_pass_through_units_outside_encrypted_extents() {
        let key = [0x5Au8; 16];
        let unit = encrypt_aacs_unit(&key);
        let ciphertext = unit.clone();
        let src = FixedUnit { unit };
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = std::sync::Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            0,
            u32::MAX,
            0,
        )]));
        // Content covers a DIFFERENT extent (LBA 300..303); LBA 0 is outside it.
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(300u32, 3u32)].into_boxed_slice());
        let mut dec = DecryptingSectorSource::new(src, keys)
            .with_key_map(map)
            .with_content_ranges(ranges);
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let n = dec.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n, crate::aacs::content::ALIGNED_UNIT_LEN);
        assert_eq!(
            buf, ciphertext,
            "a unit outside the content extents must pass through untouched"
        );
    }

    // With content ranges set, a NON-aligned read that touches no content extent is
    // clear filesystem — it must NOT trip the AACS unit-alignment gate (which would
    // otherwise fail loud for any AACS read at a misaligned LBA).
    #[test]
    fn content_ranges_exempt_out_of_content_read_from_alignment_gate() {
        let src = PatternedSource { capacity: 16 };
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0u8; 16])],
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = std::sync::Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(300, 303, 0)]));
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(300u32, 3u32)].into_boxed_slice());
        let mut dec = DecryptingSectorSource::new(src, keys)
            .with_key_map(map)
            .with_content_ranges(ranges);
        let mut buf = vec![0u8; 2048];
        // LBA 1 is misaligned AND outside content → passes through, no DecryptFailed.
        let n = dec
            .read_sectors(1, 1, &mut buf, false)
            .expect("a misaligned clear read outside content must not be gated");
        assert_eq!(n, 2048);
        let mut expected = vec![0u8; 2048];
        PatternedSource::fill(1, 1, &mut expected);
        assert_eq!(buf, expected, "clear out-of-content bytes pass through");
    }

    // Records whether the inner source was read at all.
    struct CountingSource(Arc<std::sync::atomic::AtomicUsize>);
    impl SectorSource for CountingSource {
        fn read_sectors(&mut self, _l: u32, c: u16, b: &mut [u8], _r: bool) -> Result<usize> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let n = c as usize * 2048;
            b[..n].fill(0);
            Ok(n)
        }
    }

    // The AACS unit-alignment gate rejects a misaligned content read BEFORE the
    // inner read (misaligned units would silently mis-decrypt), measured from
    // `unit_base`, not absolute LBA 0.
    #[test]
    fn aacs_alignment_gate_rejects_misaligned_reads_relative_to_unit_base() {
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0u8; 16])],
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = std::sync::Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            0,
            u32::MAX,
            0,
        )]));
        let mut dec =
            DecryptingSectorSource::new(CountingSource(reads.clone()), keys).with_key_map(map);
        dec.set_unit_base(0);
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let err = dec
            .read_sectors(1, 3, &mut buf, false)
            .expect_err("LBA 1 is misaligned against base 0");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "rejected before touching the drive"
        );

        // Base 1: LBA 4 is aligned (4 - 1 = 3), LBA 3 is not.
        dec.set_unit_base(1);
        dec.read_sectors(4, 3, &mut buf, false)
            .expect("aligned relative to unit_base");
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(dec.read_sectors(3, 3, &mut buf, false).is_err());
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // A file whose first sector is LBA 1001 (1001 % 3 == 2): two encrypted units on
    // its own grid (1001, 1004), zeroes elsewhere.
    struct MisalignedFile(Vec<u8>);
    const FILE_LBA: u32 = 1001;
    impl SectorSource for MisalignedFile {
        fn read_sectors(&mut self, lba: u32, c: u16, b: &mut [u8], _r: bool) -> Result<usize> {
            for i in 0..c as usize {
                let rel = (lba as usize + i).wrapping_sub(FILE_LBA as usize) * 2048;
                let dst = &mut b[i * 2048..(i + 1) * 2048];
                match self.0.get(rel..rel + 2048) {
                    Some(src) => dst.copy_from_slice(src),
                    None => dst.fill(0),
                }
            }
            Ok(c as usize * 2048)
        }
    }

    fn misaligned_file_source() -> DecryptingSectorSource<MisalignedFile> {
        let key = [0x5Au8; 16];
        let mut data = encrypt_aacs_unit(&key);
        data.extend(encrypt_aacs_unit(&key));
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            format: crate::disc::ContentFormat::BdTs,
        };
        let map = Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            FILE_LBA,
            FILE_LBA + 6,
            0,
        )]));
        DecryptingSectorSource::new(MisalignedFile(data), keys)
            .with_key_map(map)
            .with_content_ranges(Arc::from(vec![(FILE_LBA, 6u32)]))
    }

    // No default grid: an AACS content read before any `set_unit_base` fails loud,
    // never decrypts on the disc-LBA-0 grid (1002 % 3 == 0 once passed the gate).
    #[test]
    fn aacs_content_read_without_unit_base_fails_loud() {
        let mut dec = misaligned_file_source();
        let mut buf = vec![0u8; 2 * crate::aacs::content::ALIGNED_UNIT_LEN];
        let r = dec.read_sectors(FILE_LBA + 1, 6, &mut buf, false);
        assert!(
            matches!(r, Err(crate::error::Error::DecryptFailed)),
            "no unit base must be DecryptFailed, got {r:?}"
        );
    }

    // A wrong explicit base (disc grid) cuts chunks across real units: a flagged chunk's seed
    // has no TS sync at byte 4, so no key opens it. Like any damaged unit it is blanked and
    // counted, never E7013 (1.7.7 muxed through; "multi pass shouldn't error").
    #[test]
    fn aacs_read_on_wrong_unit_grid_is_blanked_not_e7013() {
        let mut dec = misaligned_file_source();
        let mut buf = vec![0u8; 2 * crate::aacs::content::ALIGNED_UNIT_LEN];
        dec.set_unit_base(FILE_LBA + 1);
        let mut raw = vec![0u8; buf.len()];
        dec.inner_mut()
            .read_sectors(FILE_LBA + 1, 6, &mut raw, false)
            .unwrap();
        let flagged: Vec<usize> = (0..2).filter(|&u| raw[u * 6144] & 0xC0 != 0).collect();
        assert!(
            !flagged.is_empty(),
            "the fixture cuts at least one flagged chunk"
        );
        dec.read_sectors(FILE_LBA + 1, 6, &mut buf, false)
            .expect("an off-grid read is blanked, never E7013");
        for &u in &flagged {
            assert!(
                buf[u * 6144..(u + 1) * 6144].iter().all(|&b| b == 0),
                "unit {u} blanked"
            );
        }
        assert_eq!(dec.blanked_units(), flagged.len() as u64);

        // The file's own grid decrypts both units byte-exact.
        dec.set_unit_base(FILE_LBA);
        dec.read_sectors(FILE_LBA, 6, &mut buf, false)
            .expect("file-grid read decrypts");
        let mut want = clear_aacs_unit();
        want.extend(clear_aacs_unit());
        assert_eq!(buf, crate::aacs::content::cpi_cleared(want));
    }

    // A file at FILE_LBA of `units` units under one key; `damaged` units have a garbage seed.
    fn damaged_file_source(units: u32, damaged: &[u32]) -> DecryptingSectorSource<MisalignedFile> {
        let key = [0x5Au8; 16];
        let mut data = Vec::new();
        for u in 0..units {
            let mut unit = encrypt_aacs_unit(&key);
            if damaged.contains(&u) {
                crate::test_util::damage_unit_seed(&mut unit);
            }
            data.extend(unit);
        }
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            format: crate::disc::ContentFormat::BdTs,
        };
        let end = FILE_LBA + units * 3;
        let map = Arc::new(crate::decrypt::AacsKeyMap::from_ranges(vec![(
            FILE_LBA, end, 0,
        )]));
        let mut dec = DecryptingSectorSource::new(MisalignedFile(data), keys).with_key_map(map);
        dec.set_unit_base(FILE_LBA);
        dec
    }

    // Read `units` units from unit `from` of the damaged file.
    fn read_units(
        dec: &mut DecryptingSectorSource<MisalignedFile>,
        from: u32,
        units: u32,
    ) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; units as usize * crate::aacs::content::ALIGNED_UNIT_LEN];
        dec.read_sectors(FILE_LBA + from * 3, (units * 3) as u16, &mut buf, false)?;
        Ok(buf)
    }

    /// A lone unit whose seed is damaged, among units that decrypt on the grid, is a hole
    /// (zeros), never E7013. KS-4 [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is
    /// used as the seed for calculating the Block Key." — no key opens a damaged seed.
    #[test]
    fn a_damaged_unit_seed_on_the_grid_is_a_hole() {
        assert!(
            crate::spec::keys::KS_4_SEED
                .text
                .contains("used as the seed")
        );
        let mut dec = damaged_file_source(3, &[1]);
        let buf = read_units(&mut dec, 0, 3).expect("read damage is not a key failure");
        let clear = crate::aacs::content::cpi_cleared(clear_aacs_unit());
        let ul = crate::aacs::content::ALIGNED_UNIT_LEN;
        assert_eq!(&buf[..ul], &clear[..]);
        assert!(
            buf[ul..2 * ul].iter().all(|&b| b == 0),
            "the damaged unit is a hole"
        );
        assert_eq!(&buf[2 * ul..], &clear[..]);
        assert_eq!(dec.blanked_units(), 1);
    }

    /// A cluster of damaged units is blanked and counted however it is read: alone, first,
    /// with no intact unit around it. KS-2 [BD] §3.10.1: "Each MPEG source packet consists of
    /// the TP_extra_header (4 bytes) and an MPEG Transport packet".
    #[test]
    fn a_cluster_of_damaged_units_is_blanked_and_counted() {
        assert!(
            crate::spec::keys::KS_2_ALIGNED_UNIT
                .text
                .contains("TP_extra_header (4 bytes)")
        );
        let mut dec = damaged_file_source(4, &[1, 2]);
        let buf = read_units(&mut dec, 1, 2).expect("a damage cluster is blanked, never E7013");
        assert_eq!(dec.blanked_units(), 2);
        assert!(buf.iter().all(|&b| b == 0), "both damaged units are holes");
    }

    /// A sweep's zero run need not be unit-aligned. A unit whose tail sectors are zero keeps
    /// its seed: its head sector decrypts and the zeros stay zeros. A unit whose head sector is
    /// zero lost its seed (KS-4 [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is used
    /// as the seed"), so its ciphertext rest is a hole too — never E7013, never ciphertext out.
    #[test]
    fn partial_zero_units_at_both_edges_of_a_run_are_holes() {
        let mut dec = damaged_file_source(4, &[]);
        dec.inner_mut().0[4 * 2048..7 * 2048].fill(0); // unit 1 sectors 1-2, unit 2 sector 0
        let buf = read_units(&mut dec, 0, 4).expect("a partial zero run is read damage");
        let clear = crate::aacs::content::cpi_cleared(clear_aacs_unit());
        let ul = crate::aacs::content::ALIGNED_UNIT_LEN;
        assert_eq!(&buf[..ul], &clear[..]);
        // Packets 0-9 lie wholly in unit 1's intact head sector (10 × 192 = 1920 bytes).
        assert_eq!(
            &buf[ul..ul + 1920],
            &clear[..1920],
            "the kept head decrypts"
        );
        assert!(
            buf[ul + 2112..2 * ul].iter().all(|&b| b == 0),
            "its zero tail stays zero"
        );
        assert!(
            buf[2 * ul..3 * ul].iter().all(|&b| b == 0),
            "a lost seed is a hole"
        );
        assert_eq!(&buf[3 * ul..], &clear[..]);
        assert_eq!(
            dec.blanked_units(),
            1,
            "the lost-seed unit; the kept head is not blanked"
        );
    }

    // An FMTS Even-phase segment of `units` units under `key`, the map pointing at `map_key`;
    // units in `garbled` read back with a garbage head that kept the CPI flag and TS sync.
    fn fmts_file_source(
        units: u32,
        garbled: &[u32],
        key: [u8; 16],
        map_key: [u8; 16],
    ) -> DecryptingSectorSource<MisalignedFile> {
        let mut data = Vec::new();
        for u in 0..units {
            let mut unit = encrypt_aacs_unit(&key);
            if garbled.contains(&u) {
                crate::test_util::damage_unit_seed(&mut unit);
                unit[4] = 0x47;
            }
            data.extend(unit);
        }
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, map_key)],
            format: crate::disc::ContentFormat::BdTs,
        };
        let end = FILE_LBA + units * 3;
        let map = crate::decrypt::AacsKeyMap::from_ranges_phased(vec![(
            FILE_LBA,
            end,
            0,
            crate::decrypt::Phase::Even,
        )]);
        let mut dec =
            DecryptingSectorSource::new(MisalignedFile(data), keys).with_key_map(Arc::new(map));
        dec.set_unit_base(FILE_LBA);
        dec
    }

    /// A lone FMTS unit that fails the correct-phase verify (a garbled head that kept its
    /// sync) is damage: blanked and counted, never E7013, read with its segment or alone.
    /// KS-3 [BD] §3.10.1: "A new CBC cipher chain is started for each Aligned Unit".
    #[test]
    fn a_lone_fmts_verify_failure_is_blanked_never_e7013() {
        let key = [0x5Au8; 16];
        let mut dec = fmts_file_source(4, &[2], key, key);
        let buf = read_units(&mut dec, 0, 4).expect("damage, not E7013");
        let ul = crate::aacs::content::ALIGNED_UNIT_LEN;
        let fmt = crate::disc::ContentFormat::BdTs;
        assert!(
            crate::aacs::content::is_clean(&buf[..ul], fmt),
            "unit 0 decrypts"
        );
        assert!(
            buf[2 * ul..3 * ul].iter().all(|&b| b == 0),
            "unit 2 blanked"
        );
        assert_eq!(dec.blanked_units(), 1);
        let alone = read_units(&mut dec, 2, 1).expect("alone, still damage");
        assert!(alone.iter().all(|&b| b == 0));
        assert_eq!(dec.blanked_units(), 2);
    }

    /// A wrong FMTS key fails every unit it keys: that stops E7013, never a blank.
    #[test]
    fn a_wrong_fmts_key_still_stops_e7013() {
        let mut dec = fmts_file_source(4, &[], [0x5Au8; 16], [0xCCu8; 16]);
        assert!(matches!(
            read_units(&mut dec, 0, 4),
            Err(crate::error::Error::DecryptFailed)
        ));
    }
}

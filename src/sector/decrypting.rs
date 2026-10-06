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
    /// (KU §2.4). `None` for every reader not built by a `KeyRing`.
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
    /// HD DVD: the sector after the last read and the CPI in force there, so a read that
    /// continues it decrypts the packs before its first NV_PCK.
    hd_cpi: Option<(u32, Option<crate::aacs::hddvd::Cpi>)>,
}

// HD DVD: how far back a read that starts mid-EVOBU looks for the NV_PCK leading it, and in
// what steps. An EVOBU lasts at most about a second (under 2000 packs at 30 Mbit/s).
const HD_LOOKBACK_SECTORS: u32 = 4096;
const HD_LOOKBACK_STEP: u32 = 64;

/// What the content-detected stage may do with what it finds (from `InputOptions`).
#[derive(Clone, Default)]
pub(crate) struct StageOptions {
    /// Pass ciphertext through: never crack, decrypt or refuse.
    pub(crate) raw: bool,
    /// Held AACS keys for a loose BD-TS file.
    pub(crate) keys: Option<crate::keys::KeyRing>,
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

/// What a [`DecryptingSectorSource`] decrypts with: the one input of its one constructor.
///
/// Built from [`DecryptKeys`] (CSS title keys, or none: clear or raw), from a
/// [`KeyRing`](crate::keys::KeyRing)'s view (its per-unit map and on-arrival proof), or as
/// the content-detected stage that reaches its verdict on the first read.
pub struct Keying(KeyingKind);

enum KeyingKind {
    Keys(DecryptKeys),
    Ring {
        keys: DecryptKeys,
        map: Arc<crate::decrypt::AacsKeyMap>,
        arrival: Option<crate::keys::Arrival>,
    },
    Detect(Box<StageOptions>),
}

impl From<DecryptKeys> for Keying {
    fn from(keys: DecryptKeys) -> Self {
        Keying(KeyingKind::Keys(keys))
    }
}

impl Keying {
    /// A key ring's view: `keys` decrypts each unit with its `map` key, and `arrival`
    /// proves the units of pieces the ring could not prove up front (KU §2.4).
    pub(crate) fn ring(
        keys: DecryptKeys,
        map: Arc<crate::decrypt::AacsKeyMap>,
        arrival: Option<crate::keys::Arrival>,
    ) -> Self {
        Keying(KeyingKind::Ring { keys, map, arrival })
    }

    /// The content-detected stage (every input but a disc, image or folder): the first read
    /// classifies the head and resolves once (D3). A PS is cracked, a BD-TS read through
    /// [`KeyRing::loose_file`](crate::keys::KeyRing::loose_file), else clear.
    pub(crate) fn detect(opts: StageOptions) -> Self {
        Keying(KeyingKind::Detect(Box::new(opts)))
    }
}

impl<S: SectorSource> DecryptingSectorSource<S> {
    /// Wrap `inner` as the decryption stage `keying` describes. AACS decrypts only through a
    /// key ring's map; an AACS source built from bare [`DecryptKeys`] fails loud on its
    /// first encrypted unit.
    pub fn new(inner: S, keying: impl Into<Keying>) -> Self {
        let mut s = Self {
            inner,
            keys: DecryptKeys::None,
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
            hd_cpi: None,
        };
        match keying.into().0 {
            KeyingKind::Keys(keys) => s.keys = keys,
            KeyingKind::Ring { keys, map, arrival } => {
                s.keys = keys;
                s.key_map = Some(map);
                s.arrival = arrival.map(Box::new);
            }
            KeyingKind::Detect(opts) => {
                #[cfg(test)]
                super::stage::STAGES.with(|n| n.set(n.get() + 1));
                s.observe(&opts.ctx);
                s.detect = Some(opts);
            }
        }
        s
    }

    // A test's own map on an already-built reader (the tests outside `keys`).
    #[cfg(test)]
    pub(crate) fn keyed_for_test(mut self, map: Arc<crate::decrypt::AacsKeyMap>) -> Self {
        self.key_map = Some(map);
        self
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
                let set = crate::keys::KeyRing::loose_file(opts.keys.as_ref(), cap);
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
        if let Some(map) = self.key_map.clone()
            && matches!(
                self.keys,
                DecryptKeys::Aacs {
                    format: crate::disc::ContentFormat::MpegPs,
                    ..
                }
            )
        {
            let lead = self.hd_lead_cpi(lba, &buf[..n]);
            let run = crate::decrypt::decrypt_hddvd_packs(
                &mut buf[..n],
                &self.keys,
                lba,
                &map,
                content_ref,
                lead,
            )?;
            let sectors = (n / crate::consts::SECTOR_BYTES) as u32;
            self.hd_cpi = Some((lba.saturating_add(sectors), run.last_cpi));
            self.count_blanked(lba, run.blanked);
            return Ok(n);
        }
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

    // The CPI in force at the first pack of a read at `lba`: carried from the read it continues,
    // else from the nearest NV_PCK before it. `None` when no pack before the read's first
    // NV_PCK needs a key, or when none is found within `HD_LOOKBACK_SECTORS`.
    fn hd_lead_cpi(&mut self, lba: u32, buf: &[u8]) -> Option<crate::aacs::hddvd::Cpi> {
        use crate::aacs::hddvd::{PackKind, classify};
        use crate::consts::SECTOR_BYTES;
        if let Some((next, cpi)) = self.hd_cpi
            && next == lba
        {
            return cpi;
        }
        let wanted = buf
            .as_chunks::<SECTOR_BYTES>()
            .0
            .iter()
            .map(|p| classify(p))
            .take_while(|k| !matches!(k, PackKind::Nav(_)))
            .any(|k| matches!(k, PackKind::Scrambled | PackKind::Highlight));
        if !wanted {
            return None;
        }
        let floor = lba.saturating_sub(HD_LOOKBACK_SECTORS);
        let mut end = lba;
        let mut tmp = vec![0u8; HD_LOOKBACK_STEP as usize * SECTOR_BYTES];
        while end > floor {
            let start = end.saturating_sub(HD_LOOKBACK_STEP).max(floor);
            let len = (end - start) as usize * SECTOR_BYTES;
            let got = self
                .inner
                .read_sectors(start, (end - start) as u16, &mut tmp[..len], false)
                .ok()?;
            for pack in tmp[..got.min(len)]
                .as_chunks::<SECTOR_BYTES>()
                .0
                .iter()
                .rev()
            {
                if let PackKind::Nav(cpi) = classify(pack) {
                    return cpi;
                }
            }
            end = start;
        }
        tracing::warn!(target: "freemkv::decrypt", lba, "no NV_PCK found before an HD DVD read");
        None
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
#[path = "decrypting_tests.rs"]
mod tests;

//! Decrypt-on-read layer.
//!
//! Decrypts sectors in-place using resolved keys from disc scanning.
//! Handles AACS 1.0, AACS 2.0, and CSS transparently.
//! The caller never sees encrypted data unless explicitly bypassed.
//!
//! ## Parallel AACS decrypt
//!
//! Each AACS aligned unit (6144 bytes) is decrypted INDEPENDENTLY of
//! every other unit — per-unit key derivation from the unit_key plus
//! the unit's own first-16-byte header. There is no cross-unit
//! dependency, so a buffer of N units can be decrypted on N threads
//! in parallel via a persistent rayon thread pool.
//!
//! Small buffers (< [`PARALLEL_MIN_UNITS`] units) fall through to the
//! serial path to avoid pool dispatch overhead beating the per-unit
//! AES work.
//!
//! ## Thread-count configuration — three layers
//!
//! Resolution order (highest wins):
//! 1. The most recent [`set_decrypt_threads`] call with `n > 0`.
//!    Calling this *replaces* the live thread pool — useful for a
//!    settings-page slider in a long-running daemon.
//! 2. `FREEMKV_THREADS` env var, if set and `> 0`. Single knob
//!    covering decrypt today, intended to also drive any future
//!    input-side / output-side worker pools.
//! 3. Default: all available cores. Algorithm optimisation comes
//!    first — we measure single-thread performance to find serial
//!    bottlenecks before throwing parallelism at it — but once a
//!    pool is engaged we use the whole box. Hard cap at
//!    [`MAX_THREADS`] (rayon stack memory).

use crate::aacs;
use crate::css;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

/// Minimum units in a buffer before we pay the pool-dispatch cost of
/// fanning out. Below this, serial is faster.
const PARALLEL_MIN_UNITS: usize = 8;

/// Hard upper bound on configurable thread count. Anything larger is
/// almost certainly a misconfiguration; rayon would happily allocate
/// thousands of worker stacks otherwise.
pub const MAX_THREADS: usize = 64;

/// Process-wide decrypt thread count override. `0` means "use env
/// var, else default" — see [`decrypt_threads`] for the resolution
/// order.
static DECRYPT_THREADS: AtomicUsize = AtomicUsize::new(0);

/// Current rayon pool. `RwLock<Option<Arc<...>>>` so that
/// [`set_decrypt_threads`] can swap the pool out without leaking the
/// old one and without blocking ongoing decrypt work (in-flight calls
/// hold an `Arc` clone via [`decrypt_pool`] and finish on the old
/// pool; new calls pick up the new pool).
static DECRYPT_POOL: RwLock<Option<Arc<rayon::ThreadPool>>> = RwLock::new(None);

/// Configure how many threads to use for AACS unit decryption. A value
/// of `0` resets to the env / default resolution. `1` forces serial.
/// `N > 1` builds a new rayon pool of size N (capped at [`MAX_THREADS`])
/// and atomically replaces the live pool.
///
/// Thread-safe. Live decrypt calls keep their previously-acquired
/// pool reference for the rest of the call — no mid-call pool
/// switch. Subsequent calls see the new pool.
///
/// Pool construction is ~ms-scale; safe to call from a settings POST
/// handler.
pub fn set_decrypt_threads(n: usize) {
    let clamped = n.min(MAX_THREADS);
    DECRYPT_THREADS.store(clamped, Ordering::Relaxed);
    // Drop the existing pool. Next decrypt_pool() call rebuilds with
    // the new resolved thread count.
    //
    // Recover the guard on poisoning, exactly as `decrypt_pool` does. Skipping
    // the swap on a poisoned lock silently kept the STALE pool alive while the
    // atomic above already reported the new thread count, so the setting appeared
    // to take effect and never did. The pool Arc is immutable once stored, so a
    // prior panic cannot have left it half-written.
    let mut guard = DECRYPT_POOL.write().unwrap_or_else(|e| e.into_inner());
    *guard = None;
}

/// Get (or lazily build) the active rayon thread pool. Returns an
/// `Arc` so in-flight work survives a concurrent
/// [`set_decrypt_threads`] swap.
///
/// Returns `None` if the pool cannot be built (e.g. the OS refuses the
/// worker threads under a pid/thread limit). The caller falls back to
/// the serial decrypt path — library code never panics here.
fn decrypt_pool() -> Option<Arc<rayon::ThreadPool>> {
    // Fast path: pool already built. A poisoned read lock still yields a
    // usable guard (the pool Arc is immutable once stored).
    {
        let guard = DECRYPT_POOL.read().unwrap_or_else(|e| e.into_inner());
        if let Some(pool) = guard.as_ref() {
            return Some(Arc::clone(pool));
        }
    }
    // Slow path: build a new one under the write lock. Recover the guard
    // on poisoning (a prior panic) rather than propagating a secondary
    // panic — we simply rebuild. Double-check after acquiring in case
    // another caller built it first.
    let mut guard = DECRYPT_POOL.write().unwrap_or_else(|e| e.into_inner());
    if let Some(pool) = guard.as_ref() {
        return Some(Arc::clone(pool));
    }
    let n = decrypt_threads();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .thread_name(|i| format!("freemkv-decrypt-{i}"))
        .build()
        .ok()
        .map(Arc::new)?;
    *guard = Some(Arc::clone(&pool));
    Some(pool)
}

/// Current effective decrypt thread count. Resolution order:
/// 1. Most recent [`set_decrypt_threads`] value (if > 0)
/// 2. `FREEMKV_THREADS` env var (if set and > 0)
/// 3. Default: all available cores, capped at [`MAX_THREADS`].
pub fn decrypt_threads() -> usize {
    let explicit = DECRYPT_THREADS.load(Ordering::Relaxed);
    if explicit > 0 {
        return explicit;
    }
    // Resolve the `FREEMKV_THREADS` env var + `available_parallelism()` ONCE and
    // cache it — this runs on the per-buffer decrypt hot path, and a getenv +
    // String alloc + parallelism syscall per call is pure overhead. The explicit
    // `set_decrypt_threads` override above still takes effect dynamically.
    static DEFAULT_THREADS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *DEFAULT_THREADS.get_or_init(|| {
        let env = std::env::var("FREEMKV_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        if env > 0 {
            return env.min(MAX_THREADS);
        }
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        cores.clamp(1, MAX_THREADS)
    })
}

/// Resolved decryption state from disc scanning.
/// Passed to `decrypt_sectors()` — the caller doesn't need to know
/// which encryption scheme is in use.
#[derive(Clone)]
pub enum DecryptKeys {
    /// No encryption on this disc.
    None,
    /// AACS (Blu-ray / UHD / HD-DVD). Unit keys + optional read data key. The
    /// `format` is the disc's content container (BD/UHD/FMTS = Transport Stream,
    /// HD-DVD `.evo` = Program Stream); it travels with the keys because both are
    /// resolved once per disc, and the key SELECTOR (`is_clean`) needs it to prove
    /// a key structurally against the right container.
    Aacs {
        unit_keys: Vec<(u32, [u8; 16])>,
        read_data_key: Option<[u8; 16]>,
        format: crate::disc::ContentFormat,
    },
    /// CSS (DVD). Title key for sector descrambling.
    Css { title_key: [u8; 5] },
}

impl DecryptKeys {
    /// True if there are keys to decrypt with.
    pub fn is_encrypted(&self) -> bool {
        !matches!(self, DecryptKeys::None)
    }
}

/// Which aligned units of a range a key decrypts. AACS 2.1 FMTS forensic segments
/// interleave TWO variants at the unit level; `Even`/`Odd` selects the variant's
/// half (parity of the unit's index within the segment) and the ALTERNATE half is
/// left untouched (ciphertext) for the muxer to drop. Every non-forensic range —
/// the base Unit Key, a multi-CPS unit — is `All` (decrypt every unit), so the
/// common disc is byte-for-byte unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    All,
    Even,
    Odd,
}

/// Proactive AACS key-selection map: which held unit key decrypts each LBA of a
/// title's encrypted content, decided ONCE before mux from the disc's CPS-unit
/// (and, later, FMTS segment) structure — never by trial-decrypt-and-check per
/// unit at mux time.
///
/// This is the pivot that ends the mux "key-server storm": the old path decrypts
/// a unit, checks whether the plaintext looks like clean MPEG-TS, and — because
/// authored-bad content never reaches that bar — concludes "wrong key, fetch a
/// fresh one" and re-asks the key service for units it already holds the correct
/// key for. There is NO per-unit byte pattern that separates "correctly decrypted
/// but authored-bad" from "still encrypted", so that check is unanswerable. The
/// map removes the question: we resolve one key per CPS unit / segment up front
/// (see `resolve_mux_key_map`), record which LBA ranges each covers, and at mux
/// time simply "decrypt this LBA with key K" and trust it — bad TS is the muxer's
/// concern, exactly as for a physically-read clear disc.
///
/// Ranges are `[start_lba, end_lba)` → index into the `Aacs { unit_keys }` pool,
/// sorted and disjoint. The map is a POSITIVE list: an LBA in no range is passed
/// through untouched (no default key). How a single-CPS disc is mapped depends on
/// the caller: the whole-disc EXTRACT path uses one blanket range `(0, u32::MAX,
/// 0)` so every encrypted unit — parsed title or orphan clip — resolves to key 0;
/// the per-title MUX/sweep path (`resolve_mux_key_map` → `content_map`) maps only
/// the title's own extents, so an orphan clip outside them is left as pass-through.
/// Either way, clear nav/filesystem sectors (encrypted-flag off) pass through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AacsKeyMap {
    // (start_lba, end_lba, key_idx, phase). An LBA in NO range is passed through
    // untouched — the map is a positive list of "this key here", nothing more.
    ranges: Vec<(u32, u32, usize, Phase)>,
    // Distinct, sorted key indices the map selects — derived from `ranges` once at
    // construction so the per-batch decrypt bounds check does not re-allocate/sort
    // it on every read. Kept in sync by building both in `from_ranges_phased`.
    key_indices: Vec<usize>,
}

impl AacsKeyMap {
    /// Build from `[start_lba, end_lba) → key_idx` ranges that decrypt EVERY unit
    /// (single- or multi-CPS): each range is [`Phase::All`]. An LBA in no range is
    /// passed through untouched.
    pub fn from_ranges(ranges: Vec<(u32, u32, usize)>) -> Self {
        let phased = ranges
            .into_iter()
            .map(|(s, e, i)| (s, e, i, Phase::All))
            .collect();
        Self::from_ranges_phased(phased)
    }

    /// Build a PHASE-AWARE map (FMTS): each range carries which unit-parity its key
    /// opens ([`Phase::Even`]/[`Phase::Odd`] for a forensic segment, [`Phase::All`]
    /// for base/CPS). Ranges are sorted; an LBA in no range is passed through.
    pub fn from_ranges_phased(mut ranges: Vec<(u32, u32, usize, Phase)>) -> Self {
        ranges.sort_by_key(|&(start, _, _, _)| start);
        let mut key_indices: Vec<usize> = ranges.iter().map(|&(_, _, i, _)| i).collect();
        key_indices.sort_unstable();
        key_indices.dedup();
        Self {
            ranges,
            key_indices,
        }
    }

    /// The `(key_idx, phase, range_start_lba)` for the aligned unit at `lba`, or
    /// `None` when no range covers it (not encrypted content this map keys — pass
    /// the unit through untouched). O(log n). `range_start_lba` lets the mapped
    /// decrypt compute a unit's parity WITHIN a forensic segment (`Even`/`Odd`).
    pub fn entry_for(&self, lba: u32) -> Option<(usize, Phase, u32)> {
        match self
            .ranges
            .binary_search_by(|&(start, _, _, _)| start.cmp(&lba))
        {
            Ok(i) => {
                let (start, _, idx, ph) = self.ranges[i];
                Some((idx, ph, start))
            }
            Err(0) => None,
            Err(i) => {
                let (start, end, idx, ph) = self.ranges[i - 1];
                (lba >= start && lba < end).then_some((idx, ph, start))
            }
        }
    }

    /// The unit-key index for the aligned unit at `lba`, or `None` when no range
    /// covers it (pass through). See [`entry_for`](Self::entry_for) for the phase.
    pub fn key_idx_for(&self, lba: u32) -> Option<usize> {
        self.entry_for(lba).map(|(idx, _, _)| idx)
    }

    /// The `[start_lba, end_lba) → (key_idx, phase)` ranges (sorted, disjoint).
    pub fn ranges(&self) -> &[(u32, u32, usize, Phase)] {
        &self.ranges
    }

    /// The distinct key indices this map selects — the CPS units / segments the
    /// title actually reaches. The resolver secures exactly these up front. Computed
    /// once at construction (see [`from_ranges_phased`](Self::from_ranges_phased)).
    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    /// Build the FMTS **read plan**: the title's aligned units filtered down to
    /// only the units this rip must actually read — every default / CPS unit,
    /// plus, inside each forensic segment, ONLY our-phase ([`Phase::Even`] /
    /// [`Phase::Odd`]) units. The alternate-phase units are a different device
    /// group's variant: a licensed player never reads them, and neither do we.
    /// They are omitted from the plan entirely, so they are never fetched,
    /// decrypted, or handed to the demux — the demux therefore sees one gapless
    /// our-variant stream, with no ciphertext to trip a concealed-gap resync (the
    /// old behaviour that dropped good frames around every segment).
    ///
    /// `extents` are the title's clip extents (unit-aligned in the interior;
    /// a shorter tail is ordinary content and always kept). `unit_sectors` is the
    /// AACS aligned-unit size in sectors (3). Contiguous kept units coalesce into
    /// as few extents as possible so the producer still issues large sequential
    /// reads across default content; only inside a ~480 KB forensic segment do
    /// reads become unit-granular (every other unit). A map with no forensic
    /// (Even/Odd) range returns `extents` unchanged — the common disc is not
    /// touched.
    ///
    /// The parity test is byte-identical to the decrypt hot loop
    /// (`(unit_lba - range_start) / unit_sectors`), so a unit kept here is exactly
    /// a unit [`decrypt_sectors_mapped`] would open, and vice-versa.
    pub fn read_plan(
        &self,
        extents: &[crate::disc::Extent],
        unit_sectors: u32,
    ) -> Vec<crate::disc::Extent> {
        // No forensic segment → read everything, unchanged (byte-for-byte).
        if !self
            .ranges
            .iter()
            .any(|&(_, _, _, p)| matches!(p, Phase::Even | Phase::Odd))
        {
            return extents.to_vec();
        }
        let us = unit_sectors.max(1);
        let mut plan: Vec<crate::disc::Extent> = Vec::new();
        // Append `sectors` at `lba`, coalescing with the previous extent when they
        // are physically contiguous so default runs stay one big sequential read.
        let mut push = |lba: u32, sectors: u32| {
            if sectors == 0 {
                return;
            }
            if let Some(last) = plan.last_mut()
                && last.start_lba.saturating_add(last.sector_count) == lba
            {
                last.sector_count += sectors;
                return;
            }
            plan.push(crate::disc::Extent {
                start_lba: lba,
                sector_count: sectors,
            });
        };
        for e in extents {
            let mut off = 0u32;
            while off < e.sector_count {
                let lba = e.start_lba.saturating_add(off);
                let remaining = e.sector_count - off;
                if remaining < us {
                    // Extent tail shorter than a whole unit: ordinary content
                    // (nothing follows to desync), always read.
                    push(lba, remaining);
                    break;
                }
                // A unit in NO range is pass-through content (base/default) — read
                // it. Only an alternate-phase forensic unit is dropped from the plan.
                let keep = match self.entry_for(lba) {
                    None | Some((_, Phase::All, _)) => true,
                    Some((_, phase, range_start)) => {
                        let unit_ix = (lba - range_start) / us;
                        let is_odd = unit_ix % 2 == 1;
                        is_odd == matches!(phase, Phase::Odd)
                    }
                };
                if keep {
                    push(lba, us);
                }
                off += us;
            }
        }
        plan
    }
}

/// Decrypt a buffer of sectors in-place using a resolved [`AacsKeyMap`] — the
/// mux's TRUSTED decrypt. `base_lba` is the absolute LBA of `buf`'s first sector;
/// each aligned unit (3 sectors) is decrypted with the key the map assigns to its
/// LBA. There is NO key trial and NO `is_clean` verdict: the map already decided
/// the key from disc structure, so we apply it and move on — a unit that decrypts
/// to authored-bad TS passes through for the muxer to drop, never re-fetched.
///
/// Only [`DecryptKeys::Aacs`] uses a map (CSS self-cracks per region inside
/// [`decrypt_sectors`]; `None` is clear) — other variants are a no-op here so the
/// decorator can dispatch uniformly. A map index outside the held pool is a
/// fail-loud [`Error::DecryptFailed`]: the resolver's job is to guarantee every
/// selectable index is present, so a gap here is a resolver bug, not silent loss.
pub(crate) fn decrypt_sectors_mapped(
    buf: &mut [u8],
    keys: &DecryptKeys,
    base_lba: u32,
    map: &AacsKeyMap,
) -> Result<(), crate::error::Error> {
    let (unit_keys, rdk, format) = match keys {
        DecryptKeys::Aacs {
            unit_keys,
            read_data_key,
            format,
        } => (unit_keys, *read_data_key, *format),
        // Clear / CSS: the mapped path is AACS-only. Leave the buffer untouched;
        // CSS descrambles via `decrypt_sectors` and `None` is already clear.
        _ => return Ok(()),
    };

    let unit_len = aacs::content::ALIGNED_UNIT_LEN;
    let unit_sectors = (unit_len / 2048) as u32;

    // Validate every selectable index up front (fail loud) so the per-unit hot
    // loop can index without bounds churn and a resolver gap never silently
    // passes ciphertext through as "decrypted".
    for &idx in map.key_indices() {
        if unit_keys.get(idx).is_none() {
            return Err(crate::error::Error::DecryptFailed);
        }
    }

    // Cheap safety net for the "map must be right" model: with a correct
    // phase-aware map, every CORRECT-PHASE forensic unit decrypts to clean TS, so
    // this never fires in the happy path — but a map bug (wrong phase/key for a
    // segment) surfaces as a loud DecryptFailed instead of silent corruption. Only
    // forensic (Even/Odd) ranges are verified; base / multi-CPS (All) stays
    // trust-only, so the common disc is byte-for-byte unchanged.
    let verify_failed = std::sync::atomic::AtomicBool::new(false);

    let decrypt_one = |idx_in_buf: usize, chunk: &mut [u8]| {
        if chunk.len() != unit_len {
            // Trailing partial unit (buffer/region tail shorter than a whole unit).
            // Normally a genuinely-clear content tail (source-zero padding or a
            // short final fragment) — leave as-is. But a partial that is BOTH inside
            // a mapped (encrypted) range AND flagged encrypted in its clear seed is
            // an encrypted unit split across a boundary: a CBC fragment we cannot
            // decrypt, so emitting it verbatim would ship ciphertext as clear. Fail
            // loud instead (restores the guard the removed `decrypt_sectors` had).
            let unit_lba = base_lba.saturating_add((idx_in_buf as u32) * unit_sectors);
            if map.entry_for(unit_lba).is_some()
                && aacs::content::aacs_unit_seed_encrypted(chunk, format)
            {
                verify_failed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }
        let unit_lba = base_lba.saturating_add((idx_in_buf as u32) * unit_sectors);
        // No range covers this LBA. That is expected for clear filesystem / nav
        // on a whole-disc read — but "the map has no key here" and "there is
        // nothing to decrypt here" are different statements, and only the
        // second makes passing the unit through correct.
        //
        // An ENCRYPTED unit outside every range is content we cannot key: on a
        // multi-CPS disc that is an orphan clip referenced by no playlist, so
        // it sits in no title extent and therefore in no range. Emitting it
        // verbatim ships ciphertext where plaintext is meant to be, and extract
        // then counts those bytes as good and reports the file complete.
        //
        // The split-unit branch immediately above already draws exactly this
        // distinction. This one did not, so it was reached before the
        // `aacs_unit_encrypted` gate below ever ran.
        let Some((key_idx, phase, range_start)) = map.entry_for(unit_lba) else {
            if aacs::content::aacs_unit_seed_encrypted(chunk, format) {
                verify_failed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        };
        // PHASE GATE (FMTS forensic segment): the segment interleaves two variants
        // at the unit level. Decrypt ONLY our parity; leave the alternate half as
        // ciphertext (the muxer drops untouched ciphertext cleanly — no garble).
        if matches!(phase, Phase::Even | Phase::Odd) {
            let unit_ix = (unit_lba - range_start) / unit_sectors;
            let is_odd = unit_ix % 2 == 1;
            if is_odd != matches!(phase, Phase::Odd) {
                return; // alternate half — leave as-is
            }
        }
        // Gate on the authoritative encrypted flag ONLY (CPI bits in the clear
        // seed): a clear unit is left untouched; an encrypted unit is decrypted
        // with its MAPPED key and trusted.
        if !aacs::content::aacs_unit_encrypted(chunk, format) {
            return;
        }
        // Bounds already proven above; index directly.
        let key = &unit_keys[key_idx].1;
        if let Some(ref rdk_key) = rdk {
            aacs::content::decrypt_bus(chunk, rdk_key);
        }
        aacs::content::decrypt_unit(chunk, key);
        // Correct-phase forensic verify (silent unless the map is wrong).
        if matches!(phase, Phase::Even | Phase::Odd) && !aacs::content::is_clean(chunk, format) {
            verify_failed.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    };

    let nthreads = decrypt_threads();
    let nunits = buf.len() / unit_len;
    if nthreads <= 1 || nunits < PARALLEL_MIN_UNITS {
        for (i, chunk) in buf.chunks_mut(unit_len).enumerate() {
            decrypt_one(i, chunk);
        }
    } else {
        match decrypt_pool() {
            Some(pool) => pool.install(|| {
                buf.par_chunks_mut(unit_len)
                    .enumerate()
                    .for_each(|(i, chunk)| decrypt_one(i, chunk));
            }),
            None => {
                for (i, chunk) in buf.chunks_mut(unit_len).enumerate() {
                    decrypt_one(i, chunk);
                }
            }
        }
    }
    if verify_failed.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(crate::error::Error::DecryptFailed);
    }
    Ok(())
}

/// Decrypt a buffer of sectors in-place — the CSS / clear path only.
///
/// For CSS: descrambles per 2048-byte sector, self-cracking the title key from the
/// data (no external input). For `None`: a no-op. For AACS: **always** returns
/// `Err(DecryptFailed)` — AACS decrypts exclusively through the resolved key map
/// ([`decrypt_sectors_mapped`]), which keys every content unit up front and fails
/// at RESOLVE time when a key is missing. Reaching this arm with AACS keys means a
/// reader was built without installing its map (a bug), so it fails loud rather
/// than apply a guessed key.
///
/// `unit_key_idx` and `content` are legacy parameters kept so the CSS / `None`
/// wrapper signatures stay stable; they are ignored (the CSS arm self-gates on its
/// per-sector scramble flag). Returns `Err` if decryption was expected but
/// impossible; never produces silently corrupted output. The `usize` return is a
/// legacy unverified-byte count that is always `0` for the CSS / `None` arms.
pub fn decrypt_sectors(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    unit_key_idx: usize,
) -> Result<usize, crate::error::Error> {
    decrypt_sectors_impl(buf, keys, unit_key_idx, None)
}

/// Legacy alias of [`decrypt_sectors`]. Under the keymap-only model AACS decrypts
/// EXCLUSIVELY through the resolved key map (`decrypt_sectors_mapped`), so there is
/// no per-unit content-extent gate here any more: the AACS arm fails loud and the
/// CSS / `None` arm self-gates on its per-sector scramble flag. `base_lba` and
/// `content_ranges` are therefore inert — retained only so the wrapper signature
/// stays stable for the `DecryptingSectorSource` dispatch. Prefer
/// [`decrypt_sectors`] in new code.
pub fn decrypt_sectors_in_content(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    unit_key_idx: usize,
    base_lba: u32,
    content_ranges: &[(u32, u32)],
) -> Result<usize, crate::error::Error> {
    decrypt_sectors_impl(buf, keys, unit_key_idx, Some((base_lba, content_ranges)))
}

fn decrypt_sectors_impl(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    // Unused now that AACS decrypts via the key map only; the CSS arm self-gates on
    // its per-sector scramble flag and `None` is a no-op. Kept so the wrapper
    // signatures (decrypt_sectors / _in_content) stay stable for CSS/None callers.
    _unit_key_idx: usize,
    _content: Option<(u32, &[(u32, u32)])>,
) -> Result<usize, crate::error::Error> {
    let dropped: usize = match keys {
        DecryptKeys::None => 0,
        DecryptKeys::Aacs { .. } => {
            // AACS decrypts EXCLUSIVELY through the resolved key map
            // (`decrypt_sectors_mapped`): the map keys every content unit up front,
            // and a missing key fails at RESOLVE time. The old trial-decrypt path
            // (try each held key, keep the first-tried plaintext on a miss) is gone
            // — reaching it means an AACS reader was built without installing its
            // key map, which would silently apply a wrong key. Fail loud instead.
            return Err(crate::error::Error::DecryptFailed);
        }
        DecryptKeys::Css { title_key } => {
            // CSS SELF-recovers: the title key changes per VOB region and is
            // re-cracked constantly, but always FROM THE DATA ITSELF — no external
            // input. So the whole descramble-and-rekey is self-contained here (see
            // `css::descramble_region`), and CSS does not need the post-decrypt
            // recovery seam that AACS key-fetch / FMTS segment-skip use (those DO
            // consume external inputs a `decrypt_sectors` caller cannot supply).
            css::descramble_region(buf, title_key)?
        }
    };
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a clear-TS region: a 0x47 sync byte at offset 4 of every 192-byte
    /// BD-TS packet (matching `ts_sync_count`'s probe stride), filler elsewhere.
    /// Reads as NOT scrambled.
    fn clear_ts_region(len: usize) -> Vec<u8> {
        let mut v: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
        let mut off = 4;
        while off < len {
            v[off] = 0x47;
            off += 192;
        }
        v
    }

    /// Build a scrambled region: the 192-byte-stride sync positions are NOT
    /// 0x47 (encrypted content destroys them), so it reads as scrambled.
    fn scrambled_region(len: usize) -> Vec<u8> {
        let mut v: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31)).collect();
        let mut off = 4;
        while off < len {
            // Force a non-sync byte at every probe position.
            v[off] = 0xA5;
            off += 192;
        }
        // Flag every aligned unit's CPI bits (byte 0) so it reads as encrypted
        // under the authoritative `aacs_unit_encrypted`/`aacs_unit_needs_decrypt`
        // gate — real encrypted content always carries these.
        let mut u = 0;
        while u < len {
            v[u] |= 0xC0;
            u += aacs::content::ALIGNED_UNIT_LEN;
        }
        v
    }

    // ── `decrypt_sectors_in_content` (now a legacy alias of `decrypt_sectors`) ──

    /// `DecryptKeys::None` is a no-op even with a content map + scrambled bytes.
    #[test]
    fn content_gate_none_keys_is_noop() {
        let mut keys = DecryptKeys::None;
        let original = scrambled_region(aacs::content::ALIGNED_UNIT_LEN);
        let mut buf = original.clone();
        let dropped = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]).unwrap();
        assert_eq!(dropped, 0);
        assert_eq!(buf, original);
    }

    /// CSS ignores the content gate (it lives in the AACS arm) and always reports
    /// `0` — confirming the gate is a no-op for CSS and the read stays
    /// scheme-agnostic (the litmus test: adding CSS verify touches only the CSS
    /// arm, never the read).
    #[test]
    fn content_gate_css_keys_is_noop() {
        let mut keys = DecryptKeys::Css { title_key: [0; 5] };
        let mut buf = vec![0u8; 2048];
        let dropped = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]).unwrap();
        assert_eq!(
            dropped, 0,
            "CSS arm returns 0; content gate is a no-op for CSS"
        );
    }

    /// `decrypt_sectors_in_content` is the entry point `DecryptingSectorSource`
    /// dispatches to whenever a content map is installed (`sector/decrypting.rs`
    /// line ~211), so it is on the live read path for every mapped rip. It must
    /// actually DECRYPT. The two `_is_noop` tests above only assert its `usize`
    /// return is `0` — which is what a body replaced by `Ok(0)` also returns, so
    /// neither one constrains it at all.
    ///
    /// Here a genuinely scrambled CSS sector goes in and the CONSTRUCTED
    /// plaintext must come out. Anything that skips `css::descramble_region` —
    /// including a body that just reports `Ok(0)` — leaves ciphertext in the
    /// buffer and the caller muxes scrambled MPEG at exit 0.
    ///
    /// Expected bytes come from the plaintext this test built BEFORE scrambling
    /// (CSS scrambles only 0x80..2048; the header stays clear), not from
    /// re-running any descramble routine.
    #[test]
    fn content_gate_css_actually_descrambles_the_buffer() {
        const RUN_START: usize = 0x59;
        const SEED_OFFSET: usize = 0x54;
        const PERIOD: usize = 8;
        let title_key = [0x11u8, 0x22, 0x33, 0x44, 0x55];

        let mut plaintext = vec![0u8; 2048];
        plaintext[0x00..0x04].copy_from_slice(&css::PACK_START);
        plaintext[0x14] = 0x10; // CSS scramble flag (DVD-Video sector header)
        let pat: Vec<u8> = (0..PERIOD)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
            *b = pat[i % PERIOD];
        }
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05]);

        let mut buf = plaintext.clone();
        css::lfsr::scramble_sector(&title_key, &mut buf);
        let ciphertext = buf.clone();
        assert_ne!(
            &ciphertext[0x80..],
            &plaintext[0x80..],
            "fixture malformed — the sector was not actually scrambled"
        );

        let mut keys = DecryptKeys::Css { title_key };
        decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 1)])
            .expect("CSS descramble must not fail");

        // Report the first differing offset rather than dumping 1.9 KB.
        let mismatch = (0x80..2048).find(|&i| buf[i] != plaintext[i]);
        assert!(
            mismatch.is_none(),
            "the scrambled body must come back as the plaintext it was built \
             from; first mismatch at offset {mismatch:?} (buf={:#04x} \
             expected={:#04x}) — a wrapper that decrypts nothing leaves the \
             ciphertext in place and the caller muxes scrambled MPEG",
            buf[mismatch.unwrap_or(0x80)],
            plaintext[mismatch.unwrap_or(0x80)],
        );
    }

    /// The AACS arm of the same entry point must fail LOUD. Under the
    /// keymap-only model AACS decrypts exclusively through
    /// `decrypt_sectors_mapped`; reaching this wrapper with AACS keys means a
    /// reader was built without installing its key map, and continuing would
    /// hand the caller ciphertext under an `Ok`. `DecryptFailed` is the correct
    /// verdict per the function's own contract — it must not be softened into a
    /// success with a zero count.
    #[test]
    fn content_gate_aacs_keys_fail_loud_not_ok_zero() {
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(1, [0xAB; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let original = scrambled_region(aacs::content::ALIGNED_UNIT_LEN);
        let mut buf = original.clone();
        let r = decrypt_sectors_in_content(&mut buf, &mut keys, 0, 0, &[(0, 3)]);
        assert!(
            matches!(r, Err(crate::error::Error::DecryptFailed)),
            "AACS without an installed key map must be DecryptFailed, got {r:?}"
        );
        assert_eq!(
            buf, original,
            "and it must not have half-decrypted the buffer on the way out"
        );
    }

    /// Build a Stevenson-crackable scrambled CSS sector for `title_key` (mirrors
    /// `crackable_sector` in the css::mod tests): a periodic run in the clear
    /// header continues past 0x80 into the encrypted region, so
    /// `stevenson::crack_title_key` recovers the key. Distinct `seed` values give
    /// two sectors different cribs, standing in for two VOB regions.
    fn crackable_css_sector(title_key: &[u8; 5], seed: &[u8; 5]) -> Vec<u8> {
        const RUN_START: usize = 0x59;
        const SEED_OFFSET: usize = 0x54;
        const PERIOD: usize = 8;
        let mut plaintext = vec![0u8; 2048];
        plaintext[0x00..0x04].copy_from_slice(&css::PACK_START);
        plaintext[0x14] = 0x10; // scramble flag
        let pat: Vec<u8> = (0..PERIOD)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
            *b = pat[i % PERIOD];
        }
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(seed);
        css::lfsr::scramble_sector(title_key, &mut plaintext);
        plaintext
    }

    /// CHARACTERIZATION (recovery refactor safety net): the CSS arm's per-region
    /// re-crack (the `title_key` cache is stale for a new VOB region → restore
    /// ciphertext, `crack_title_key` this sector, re-descramble). Two crackable
    /// sectors scrambled under DIFFERENT keys sit back-to-back; the cache is
    /// primed to the FIRST key. Sector 0 rides the cache (crib matches); sector 1
    /// must trip the crib mismatch and re-crack to its own key. Both must land
    /// correct plaintext, and the cache must end on region 1's key.
    ///
    /// This behaviour currently lives inline in `decrypt_sectors` (the `Css`
    /// arm). It is the delicate logic the recovery refactor will move to the
    /// input-stream seam, so it must stay green byte-for-byte across that move.
    #[test]
    fn css_region_change_recracks_the_title_key() {
        let key_a = [0x11, 0x22, 0x33, 0x44, 0x55];
        let key_b = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        let sector_a = crackable_css_sector(&key_a, &[0x01, 0x02, 0x03, 0x04, 0x05]);
        let sector_b = crackable_css_sector(&key_b, &[0x09, 0x08, 0x07, 0x06, 0x05]);

        // Expected plaintext bodies: each sector descrambled under its true key.
        let mut plain_a = sector_a.clone();
        css::lfsr::descramble_sector(&key_a, &mut plain_a);
        let mut plain_b = sector_b.clone();
        css::lfsr::descramble_sector(&key_b, &mut plain_b);

        let mut buf = Vec::with_capacity(4096);
        buf.extend_from_slice(&sector_a);
        buf.extend_from_slice(&sector_b);

        // Cache primed to region A's key (as if A was the last crack). CSS
        // descramble-and-rekey lives in `css::descramble_region` (the recovery
        // seam calls it); the region change must re-crack region B's key.
        let mut ended = key_a;
        css::descramble_region(&mut buf, &mut ended);

        assert_eq!(
            &buf[0x80..2048],
            &plain_a[0x80..2048],
            "sector 0 rides the cached key (crib matches, no re-crack)"
        );
        assert_eq!(
            &buf[2048 + 0x80..4096],
            &plain_b[0x80..2048],
            "sector 1 re-cracks its own region key and descrambles correctly"
        );
        // The cache must have advanced to a key that descrambles region B.
        let mut check_b = sector_b.clone();
        css::lfsr::descramble_sector(&ended, &mut check_b);
        assert_eq!(
            &check_b[0x80..2048],
            &plain_b[0x80..2048],
            "the ended cache key must round-trip region B's body"
        );
    }

    /// Whole leading unit plus a SCRAMBLED trailing partial that is FLAGGED
    /// encrypted in its clear seed (the malformed danger case): an encrypted unit
    /// split across an extent boundary cannot be CBC-decrypted standalone. The
    /// mapped decrypt must fail loud with `DecryptFailed` rather than emit the
    /// ciphertext partial as clear. Exercises the real shipping path
    /// (`decrypt_sectors_mapped`) and its trailing-partial guard.
    #[test]
    fn aacs_scrambled_trailing_partial_is_rejected() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        // One CLEAR leading unit (passes through) + a 4096-byte (two-sector) tail
        // whose seed byte flags it encrypted, inside the mapped range.
        let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
        let mut tail = scrambled_region(4096);
        tail[0] |= 0xC0; // CPI bits → flagged encrypted on the partial
        buf.extend_from_slice(&tail);

        let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
        let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
            .expect_err("scrambled encrypted trailing partial must be rejected");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "scrambled trailing partial must fail with DecryptFailed"
        );
    }

    /// A CLEAR trailing partial (encrypted flag NOT set) is a legitimate content
    /// tail and must pass through, never trip the guard above.
    ///
    /// "Passes through" means byte-for-byte unchanged, not merely `Ok`. Asserting
    /// only `is_ok()` let a mutant that corrupts the clear partial while still
    /// returning `Ok` pass — which is the whole failure this test names.
    /// Mutation: XOR any byte of the tail before returning -> the snapshot
    /// comparison fails.
    #[test]
    fn aacs_clear_trailing_partial_passes_through() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
        let mut tail = clear_ts_region(4096);
        tail[0] &= 0x3F; // ensure the CPI bits are clear
        buf.extend_from_slice(&tail);
        let snapshot = buf.clone();
        let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
            .expect("a clear trailing partial is legitimate content");
        assert_eq!(
            buf, snapshot,
            "a clear trailing partial must pass through byte-for-byte, not just return Ok"
        );
    }

    // ── DecryptKeys::None and is_encrypted ─────────────────────────────────

    /// DecryptKeys::None is a pure no-op: the buffer must be returned
    /// byte-for-byte unchanged with Ok, regardless of content (even content
    /// that looks scrambled).
    ///
    /// Grounding: the `DecryptKeys::None => {}` match arm does nothing.
    /// Mutation: replace the empty arm with a call that mutates buf -> the
    /// unchanged assert fails.
    #[test]
    fn none_keys_is_noop() {
        let mut buf: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let snapshot = buf.clone();
        decrypt_sectors(&mut buf, &mut DecryptKeys::None, 0).expect("None is always Ok");
        assert_eq!(buf, snapshot, "None must not touch the buffer");
    }

    /// is_encrypted reflects the variant: None -> false, Css/Aacs -> true.
    ///
    /// Grounding: `!matches!(self, DecryptKeys::None)`.
    /// Mutation: invert the `!` -> None reports true, this fails.
    #[test]
    fn is_encrypted_matches_variant() {
        assert!(!DecryptKeys::None.is_encrypted());
        assert!(DecryptKeys::Css { title_key: [0; 5] }.is_encrypted());
        assert!(
            DecryptKeys::Aacs {
                unit_keys: vec![(0, [0; 16])],
                read_data_key: None,
                format: crate::disc::ContentFormat::BdTs,
            }
            .is_encrypted()
        );
    }

    // ── CSS dispatch (DecryptKeys::Css) ────────────────────────────────────

    /// Build a CSS-scrambled 2048-byte sector by scrambling a known plaintext
    /// body with the exact inverse of `descramble_sector`, so decrypt_sectors
    /// will descramble it back to the plaintext. The content cipher applies
    /// TAB1 to the ciphertext (`plain = TAB1[cipher] ^ ks`), so it is NOT a
    /// self-inverse XOR — `scramble_sector` is the true inverse and sets the
    /// scramble flag.
    fn make_css_sector(title_key: &[u8; 5], seed: &[u8; 5], body_fill: u8) -> (Vec<u8>, Vec<u8>) {
        let mut sector = vec![body_fill; 2048];
        sector[0x14] = 0x30; // scramble flag (bits 4-5)
        sector[0x54..0x59].copy_from_slice(seed);
        let plaintext = sector.clone();
        css::lfsr::scramble_sector(title_key, &mut sector);
        (sector, plaintext)
    }

    /// The CSS path descrambles each 2048-byte sector with the title key. A
    /// scrambled sector run through decrypt_sectors must come back to its
    /// plaintext body (keystream XOR is involutive), proving the title key is
    /// actually applied.
    ///
    /// Grounding: `DecryptKeys::Css { title_key } => for chunk in
    /// buf.chunks_mut(2048) { descramble_sector(title_key, chunk) }`.
    /// Mutation: change `chunks_mut(2048)` to `chunks_mut(2049)` or pass a
    /// fixed wrong key -> the body no longer matches the plaintext.
    #[test]
    fn css_descrambles_with_title_key() {
        let mut title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        let (mut sector, plaintext) = make_css_sector(&title_key, &seed, 0xA5);
        // CSS descramble lives in `css::descramble_region` (the recovery seam
        // calls it); `decrypt_sectors` only flags CSS sectors for recovery.
        css::descramble_region(&mut sector, &mut title_key);
        assert_eq!(
            &sector[0x80..2048],
            &plaintext[0x80..2048],
            "CSS body must round-trip to plaintext"
        );
        // Flag cleared by the descrambler.
        assert_eq!(
            sector[0x14] & 0x30,
            0,
            "scramble flag cleared after CSS decrypt"
        );
    }

    /// The CSS path processes EACH 2048-byte sector independently in a
    /// multi-sector buffer. Two scrambled sectors (with different seeds) in
    /// one buffer must both round-trip — pinning that the loop steps by 2048
    /// and applies the key to every sector, not just the first.
    ///
    /// Grounding: `for chunk in buf.chunks_mut(2048)`.
    /// Mutation: change the loop to descramble only the first chunk (e.g.
    /// `.next()`) -> the second sector stays scrambled, assert fails.
    #[test]
    fn css_processes_every_sector_in_buffer() {
        let title_key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let (s0, p0) = make_css_sector(&title_key, &[0x11, 0x22, 0x33, 0x44, 0x55], 0x3C);
        let (s1, p1) = make_css_sector(&title_key, &[0x66, 0x77, 0x88, 0x99, 0xAA], 0xC3);
        let mut buf = s0;
        buf.extend_from_slice(&s1);
        let mut title_key = title_key;
        css::descramble_region(&mut buf, &mut title_key);
        assert_eq!(
            &buf[0x80..2048],
            &p0[0x80..2048],
            "sector 0 body must round-trip"
        );
        assert_eq!(
            &buf[2048 + 0x80..4096],
            &p1[0x80..2048],
            "sector 1 body must round-trip (loop must reach the 2nd sector)"
        );
    }

    /// Build a CSS sector whose clear header ends in a periodic run that
    /// continues into the encrypted region — the crackable shape `attack_crib`/
    /// `crack_title_key` recover a key from (a constant body fill gives a
    /// degenerate crib the cracker can't pin a unique key on). Returns
    /// (scrambled_sector, plaintext_body).
    fn make_crackable_css_sector(
        title_key: &[u8; 5],
        seed: &[u8; 5],
        period: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let mut plaintext = vec![0u8; 2048];
        plaintext[0x14] = 0x10; // scramble flag
        // Periodic run from 0x59 (just above the seed) through 0x80 and on into
        // the encrypted region; phase anchored to offset 0 so it is continuous
        // across the 0x80 boundary.
        let pat: Vec<u8> = (0..period)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plaintext.iter_mut().enumerate().skip(0x59) {
            *b = pat[i % period];
        }
        plaintext[0x54..0x59].copy_from_slice(seed); // seed sits below the run
        let body = plaintext.clone();
        css::lfsr::scramble_sector(title_key, &mut plaintext);
        (plaintext, body)
    }

    /// CSS title keys are per-VTS/VOB region: a real disc holds DIFFERENT keys
    /// for different regions and the only way to get each is to crack it. The
    /// decrypt path must re-crack when the cached key stops descrambling (its
    /// crib no longer reappears at 0x80) instead of blindly applying one key
    /// across a region boundary — the bug that pixelated every freemkv DVD rip.
    ///
    /// Two sectors scrambled under DIFFERENT keys, cache primed to ONLY the
    /// first (exactly what the one-shot scan crack leaves). Sector 0 validates +
    /// descrambles with the cached key; sector 1's cached-key descramble fails
    /// the crib, so the path re-cracks sector 1's own key and recovers its
    /// plaintext. Before the fix (blind single-key apply) sector 1 was garbage.
    ///
    /// Grounding: the CSS arm's `attack_crib` → `chunk[0x80..] != crib` →
    /// `crack_title_key` → `*title_key = fresh` rekey.
    /// Mutation: drop the rekey branch (apply the cached key always) → sector 1's
    /// body no longer matches its plaintext; this fails.
    #[test]
    fn css_rekeys_when_title_key_region_changes() {
        let key_a = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let key_b = [0x07, 0x5A, 0xC3, 0x10, 0x88]; // a DIFFERENT region's key
        let (s0, p0) = make_crackable_css_sector(&key_a, &[0x11, 0x22, 0x33, 0x44, 0x55], 4);
        let (s1, p1) = make_crackable_css_sector(&key_b, &[0x66, 0x77, 0x88, 0x99, 0xAA], 4);
        // Precondition: each sector must be crackable on its own (the rekey
        // depends on it). If this fails the fixture, not the path, is at fault.
        assert_eq!(
            crate::css::stevenson::crack_title_key(&s0),
            Some(key_a),
            "fixture s0 must crack to key_a standalone"
        );
        assert_eq!(
            crate::css::stevenson::crack_title_key(&s1),
            Some(key_b),
            "fixture s1 must crack to key_b standalone"
        );
        let mut buf = s0;
        buf.extend_from_slice(&s1);

        // Cache primed to key_a only — exactly what the one-shot scan crack yields.
        let mut title_key = key_a;
        css::descramble_region(&mut buf, &mut title_key);

        assert_eq!(
            &buf[0x80..2048],
            &p0[0x80..2048],
            "region A sector descrambles with the cached (primed) key"
        );
        assert_eq!(
            &buf[2048 + 0x80..4096],
            &p1[0x80..2048],
            "region B sector must descramble after the path re-cracks its own key"
        );
        // The cache must have advanced to region B's key.
        assert_eq!(
            title_key, key_b,
            "cache must hold region B's key after the rekey"
        );
    }

    /// The CSS path leaves UNSCRAMBLED sectors (flag clear) byte-for-byte
    /// untouched — descramble_sector early-returns on a zero flag. A clear
    /// sector mixed into the buffer must not be corrupted.
    ///
    /// Grounding: descramble_sector returns immediately when
    /// `(sector[0x14] >> 4) & 0x03 == 0`.
    /// Mutation: remove that early return in lfsr.rs -> a clear sector would
    /// be XORed with a keystream and change; this fails.
    #[test]
    fn css_leaves_clear_sector_unchanged() {
        let title_key = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut sector = vec![0x77u8; 2048];
        sector[0x14] = 0x00; // not scrambled
        let snapshot = sector.clone();
        let mut keys = DecryptKeys::Css { title_key };
        decrypt_sectors(&mut sector, &mut keys, 0).unwrap();
        assert_eq!(sector, snapshot, "clear CSS sector must be left untouched");
    }

    /// CSS decrypt always returns Ok (it cannot fail — descrambling is XOR,
    /// no key validity check), even for an empty buffer.
    ///
    /// Grounding: the CSS arm has no `return Err` path; `chunks_mut` over an
    /// empty slice is a no-op; the function ends `Ok(())`.
    /// Mutation: make the CSS arm return Err -> this fails.
    #[test]
    fn css_empty_buffer_is_ok() {
        let mut buf: Vec<u8> = Vec::new();
        let mut keys = DecryptKeys::Css { title_key: [0; 5] };
        assert!(decrypt_sectors(&mut buf, &mut keys, 0).is_ok());
    }

    // ── AACS unit-key index selection ──────────────────────────────────────

    /// A map that selects a key index OUTSIDE the held pool must fail loud with
    /// DecryptFailed — never silently apply a wrong key or pass ciphertext through.
    /// This validates `decrypt_sectors_mapped`'s up-front `key_indices()` bounds
    /// check (the real shipping AACS decrypt path).
    ///
    /// Mutation: drop the `unit_keys.get(idx).is_none()` guard → the out-of-range
    /// index would not error; this fails.
    #[test]
    fn aacs_mapped_out_of_range_key_idx_errors() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
        let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 5)]); // idx 5, pool holds 1 key
        let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
            .expect_err("map index 5 is out of range for a 1-key pool");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "out-of-range mapped key index must be DecryptFailed"
        );
    }

    /// A non-empty map over an EMPTY unit_keys pool has no key to satisfy its
    /// selected index → DecryptFailed (via the same bounds check).
    #[test]
    fn aacs_mapped_empty_unit_keys_errors() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
        let map = AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]);
        let err = decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
            .expect_err("empty unit_keys cannot satisfy map idx 0");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
    }

    /// SAFETY NET: reaching the CSS/`None` wrapper (`decrypt_sectors`) with AACS
    /// keys means a reader was built with no map — a bug. It must fail loud, never
    /// apply a guessed key. (AACS decrypts exclusively via `decrypt_sectors_mapped`.)
    #[test]
    fn aacs_via_unmapped_decrypt_sectors_fails_loud() {
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut buf = clear_ts_region(aacs::content::ALIGNED_UNIT_LEN);
        let err = decrypt_sectors(&mut buf, &mut keys, 0)
            .expect_err("AACS through the unmapped path must fail loud");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
    }

    // ── Multi-CPS-unit key selection ──────────────────────────────────────

    /// Encrypt an aligned unit so `aacs::content::decrypt_unit` with the same key
    /// recovers the plaintext, flagging it encrypted first (bytes 0..16 are the key
    /// seed, so the flag must be set before the crypto runs).
    fn aacs_encrypt_unit_for_test(unit: &mut [u8], unit_key: &[u8; 16]) {
        unit[0] |= 0xC0;
        assert!(
            aacs::content::encrypt_unit(unit, unit_key),
            "a full-length unit must encrypt"
        );
    }

    /// Build a clear aligned unit with TS sync bytes placed at the BD-TS stride
    /// (offset 4 + k*192) so `is_clean` reports true and
    /// `decrypt_unit` verifies it as clear after decryption.
    fn clear_ts_unit() -> Vec<u8> {
        let mut unit = vec![0u8; aacs::content::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < aacs::content::ALIGNED_UNIT_LEN {
            unit[off] = 0x47;
            off += 192;
        }
        unit
    }

    // ── FMTS phase-aware map ──────────────────────────────────────────────────

    /// `entry_for` returns Some((idx, phase, range_start)) inside a range;
    /// `from_ranges` is All, `from_ranges_phased` carries the phase; an uncovered
    /// LBA is `None` (pass through).
    #[test]
    fn aacskeymap_phase_entry_for() {
        let all = AacsKeyMap::from_ranges(vec![(100, 200, 3)]);
        assert_eq!(all.entry_for(150), Some((3, Phase::All, 100)));
        assert_eq!(all.entry_for(50), None);

        let phased = AacsKeyMap::from_ranges_phased(vec![(100, 200, 3, Phase::Odd)]);
        assert_eq!(phased.entry_for(150), Some((3, Phase::Odd, 100)));
        assert_eq!(phased.entry_for(250), None);
        assert_eq!(phased.key_idx_for(150), Some(3));
    }

    /// A map with no forensic (Even/Odd) range is the common disc: `read_plan`
    /// returns the extents unchanged, so nothing but FMTS is affected.
    #[test]
    fn read_plan_non_forensic_is_unchanged() {
        use crate::disc::Extent;
        let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3
        let ext = vec![
            Extent {
                start_lba: 1000,
                sector_count: 300,
            },
            Extent {
                start_lba: 5000,
                sector_count: 60,
            },
        ];
        // A non-forensic map (empty, or multi-CPS All) leaves the plan untouched.
        assert_eq!(AacsKeyMap::from_ranges(vec![]).read_plan(&ext, us), ext);
        let multi = AacsKeyMap::from_ranges(vec![(1000, 1150, 2)]);
        assert_eq!(multi.read_plan(&ext, us), ext);
    }

    /// FMTS: a forensic Even segment drops exactly its alternate (odd) units from
    /// the read plan — they are never fetched — while default content on either
    /// side stays one coalesced sequential run. The kept units are byte-identical
    /// to the ones the decrypt hot loop opens.
    #[test]
    fn read_plan_forensic_reads_only_our_phase_units() {
        use crate::disc::Extent;
        let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3
        // One extent, 100 units [1000, 1300). A 10-unit Even forensic segment at
        // LBA [1030, 1060): kept even units are ix 0,2,4,6,8 → LBA 1030,1036,1042,
        // 1048,1054; dropped odd units → 1033,1039,1045,1051,1057.
        let ext = vec![Extent {
            start_lba: 1000,
            sector_count: 300,
        }];
        let map = AacsKeyMap::from_ranges_phased(vec![(1030, 1060, 5, Phase::Even)]);
        let plan = map.read_plan(&ext, us);
        let expected = vec![
            Extent {
                start_lba: 1000,
                sector_count: 33,
            }, // 1000..1030 default + the ix-0 even unit at 1030
            Extent {
                start_lba: 1036,
                sector_count: 3,
            },
            Extent {
                start_lba: 1042,
                sector_count: 3,
            },
            Extent {
                start_lba: 1048,
                sector_count: 3,
            },
            Extent {
                start_lba: 1054,
                sector_count: 3,
            },
            Extent {
                start_lba: 1060,
                sector_count: 240,
            }, // default resumes, coalesced to the extent end
        ];
        assert_eq!(plan, expected);
        // Exactly the 5 odd units (15 sectors) are omitted; nothing else.
        let kept: u32 = plan.iter().map(|e| e.sector_count).sum();
        assert_eq!(
            kept,
            300 - 5 * us,
            "only the alternate-phase units are dropped"
        );
        // Every kept LBA is one the decrypt loop would decrypt (All or our parity),
        // and no dropped LBA is: the plan and the decrypt gate agree unit-for-unit.
        for e in &plan {
            let mut off = 0;
            while off < e.sector_count {
                let lba = e.start_lba + off;
                if let Some((_, phase @ (Phase::Even | Phase::Odd), rs)) = map.entry_for(lba) {
                    let is_odd = ((lba - rs) / us) % 2 == 1;
                    assert!(
                        is_odd == matches!(phase, Phase::Odd),
                        "plan kept an alternate-phase unit at LBA {lba}"
                    );
                }
                off += us;
            }
        }
    }

    /// A forensic range does NOT start on an aligned-unit boundary. Its start LBA
    /// comes from a source-packet number — `start_spn * 192` put through
    /// `clip_byte_to_lba` (`mux/resolve.rs`) — and 192-byte packets have no
    /// relationship to the 3-sector aligned unit, so `range_start % 3` is
    /// whatever the disc says.
    ///
    /// That makes `unit_ix = (lba - range_start) / us` load-bearing in both of
    /// its operations, and the existing coverage used a range starting exactly
    /// on the extent's first unit, where several wrong formulas agree with the
    /// right one by arithmetic accident.
    ///
    /// Getting the parity wrong is not a crash. It reads and decrypts the
    /// ALTERNATE variant's half of a forensic segment: the units this disc's
    /// key does not open decrypt to garbage, and the units it does open are
    /// skipped. AACS 2.1 forensic marking is exactly the mechanism that makes
    /// the two halves different, so a phase inversion is silent — it produces a
    /// full-length rip carrying the wrong variant.
    #[test]
    fn read_plan_phase_parity_is_measured_from_an_unaligned_range_start() {
        use crate::disc::Extent;
        let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3

        // Case A — range_start is itself unaligned (1001 % 3 == 2) and the extent
        // begins on it, so unit offsets are 0, 3, 6, ... Under this shape the
        // formula `(lba + range_start) / us` shifts every index by an ODD amount
        // and inverts the kept half.
        let ext = vec![Extent {
            start_lba: 1001,
            sector_count: 12,
        }];
        let map = AacsKeyMap::from_ranges_phased(vec![(1001, 1013, 5, Phase::Even)]);
        assert_eq!(
            map.read_plan(&ext, us),
            vec![
                Extent {
                    start_lba: 1001,
                    sector_count: 3
                }, // ix 0, even
                Extent {
                    start_lba: 1007,
                    sector_count: 3
                }, // ix 2, even
            ],
            "unit index must be measured as (lba - range_start), so the kept \
             units are the even-indexed ones counting from the range start"
        );

        // Case B — the extent begins one unit-remainder away from the range start
        // (1001 - 1000 = 1), so offsets are 1, 4, 7, 10. Here `(lba - range_start)
        // * us` inverts the halves instead: the division is what maps a byte
        // offset onto a unit index, and multiplying happens to preserve parity
        // only when the offset is already a multiple of the unit size.
        let ext = vec![Extent {
            start_lba: 1001,
            sector_count: 12,
        }];
        let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1013, 5, Phase::Even)]);
        assert_eq!(
            map.read_plan(&ext, us),
            vec![
                Extent {
                    start_lba: 1001,
                    sector_count: 3
                }, // (1001-1000)/3 = 0, even
                Extent {
                    start_lba: 1007,
                    sector_count: 3
                }, // (1007-1000)/3 = 2, even
            ],
            "the offset must be DIVIDED by the unit size to become a unit index"
        );
    }

    /// An extent whose last whole unit is an alternate-phase unit must still drop
    /// it. The tail guard exists for a REMNANT shorter than a unit — bytes with
    /// no following unit to desync — and an extent ending exactly on a unit
    /// boundary has no remnant at all.
    ///
    /// With the guard widened to `remaining <= us`, the final unit of every
    /// extent bypasses the phase gate and is read unconditionally. On a forensic
    /// segment that lands at an extent end, that is one alternate-variant unit
    /// pulled into the rip and decrypted with a key that does not open it.
    #[test]
    fn read_plan_gates_the_last_whole_unit_of_an_extent_not_just_the_remnant() {
        use crate::disc::Extent;
        let us = (aacs::content::ALIGNED_UNIT_LEN / 2048) as u32; // 3

        // Two whole units, no remnant. ix 0 is even (kept), ix 1 is odd (dropped)
        // and it is the LAST thing in the extent.
        let ext = vec![Extent {
            start_lba: 1000,
            sector_count: 6,
        }];
        let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1006, 5, Phase::Even)]);
        assert_eq!(
            map.read_plan(&ext, us),
            vec![Extent {
                start_lba: 1000,
                sector_count: 3
            }],
            "the trailing odd-phase unit is a whole unit and must be dropped; the \
             short-tail guard is for a remnant SMALLER than a unit"
        );

        // And the remnant case the guard is actually for: 4 sectors = one whole
        // unit plus a 1-sector tail. The tail is ordinary content and is kept
        // even though the unit before it was dropped.
        let ext = vec![Extent {
            start_lba: 1000,
            sector_count: 4,
        }];
        let map = AacsKeyMap::from_ranges_phased(vec![(1000, 1004, 5, Phase::Odd)]);
        assert_eq!(
            map.read_plan(&ext, us),
            vec![Extent {
                start_lba: 1003,
                sector_count: 1
            }],
            "a sub-unit remnant is ordinary content and is always read"
        );
    }

    /// An ENCRYPTED unit that falls outside every key-map range must fail, not
    /// pass through as ciphertext.
    ///
    /// "The map has no key here" and "there is nothing to decrypt here" are
    /// different statements, and only the second makes passing the unit through
    /// correct. On a multi-CPS disc an orphan clip — referenced by no playlist,
    /// so in no title extent and therefore in no range — hits the first and was
    /// treated as the second. `extract_tree` then counted those bytes as GOOD,
    /// dropped the `.partial` suffix, and reported `complete: true`, exit 0:
    /// a scrambled file on disk with a clean bill of health.
    ///
    /// A CLEAR unit outside every range is the ordinary case (filesystem and
    /// nav on a whole-disc read) and must still pass through untouched — so
    /// this asserts both directions.
    #[test]
    fn an_encrypted_unit_outside_every_key_range_fails_instead_of_passing_through() {
        use crate::disc::ContentFormat;
        let key = [0xAAu8; 16];
        let ul = aacs::content::ALIGNED_UNIT_LEN;
        let usz = (ul / 2048) as u32;

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        // The map covers unit 0 only. Unit 1 is the orphan.
        let map = AacsKeyMap::from_ranges(vec![(0, usz, 0)]);

        // Clear orphan: untouched, no error. This is nav/filesystem.
        let mut clear_buf = vec![0u8; 2 * ul];
        let mut u0 = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u0, &key);
        clear_buf[..ul].copy_from_slice(&u0);
        clear_buf[ul..].copy_from_slice(&clear_ts_unit());
        let orphan_before = clear_buf[ul..].to_vec();
        decrypt_sectors_mapped(&mut clear_buf, &keys, 0, &map)
            .expect("a CLEAR unit outside the map is ordinary nav and must pass");
        assert_eq!(
            &clear_buf[ul..],
            &orphan_before[..],
            "a clear out-of-range unit must be left byte-identical"
        );

        // Encrypted orphan: must fail loud.
        let mut enc_buf = vec![0u8; 2 * ul];
        let mut v0 = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut v0, &key);
        enc_buf[..ul].copy_from_slice(&v0);
        let mut orphan = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut orphan, &[0xCCu8; 16]);
        enc_buf[ul..].copy_from_slice(&orphan);

        let err = decrypt_sectors_mapped(&mut enc_buf, &keys, 0, &map)
            .expect_err("an encrypted unit we hold no key for must not be emitted");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "same verdict CSS and the split-unit branch give for 'no provable key'"
        );
    }

    /// Phase::Even → only even-index units in the range are decrypted; the odd
    /// (alternate variant) half is left BYTE-FOR-BYTE as ciphertext for the muxer.
    #[test]
    fn mapped_phase_even_decrypts_even_leaves_odd_ciphertext() {
        use crate::disc::ContentFormat;
        let key_a = [0xAAu8; 16];
        let key_b = [0xBBu8; 16];
        let ul = aacs::content::ALIGNED_UNIT_LEN;
        let usz = (ul / 2048) as u32;
        let mut buf = vec![0u8; 8 * ul];
        let mut odd_cipher = Vec::new();
        for i in 0..8 {
            let mut u = clear_ts_unit();
            aacs_encrypt_unit_for_test(&mut u, if i % 2 == 0 { &key_a } else { &key_b });
            if i % 2 == 1 {
                odd_cipher.push(u.clone());
            }
            buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
        }
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let map = AacsKeyMap::from_ranges_phased(vec![(0, 8 * usz, 0, Phase::Even)]);
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map).expect("even phase decrypts clean");
        for i in 0..8 {
            let u = &buf[i * ul..(i + 1) * ul];
            if i % 2 == 0 {
                assert!(
                    aacs::content::is_clean(u, ContentFormat::BdTs),
                    "even unit {i} decrypted to clean TS"
                );
            } else {
                assert_eq!(
                    u,
                    odd_cipher[i / 2].as_slice(),
                    "odd unit {i} left as ciphertext"
                );
            }
        }
    }

    /// The mapped descramble indexes the committed key pool POSITIONALLY
    /// (`unit_keys[key_idx].1`), so the ORDER of the `Vec<UnitKey>` a
    /// `KeySource` returns is load-bearing — it is NOT "cosmetic, the decrypt path
    /// strips it and tries every key", as `keysource::resolve_and_apply_traced`'s
    /// doc used to claim. Trial-decrypt was deliberately deleted; nothing here
    /// searches the pool. Reordering the same two keys therefore sends each range
    /// to the WRONG key: the range that decrypted clean now fails the correct-phase
    /// `is_clean` net loudly (or, off a forensic phase, would decrypt a whole span
    /// under a neighbour's key). Pins the corrected doc.
    #[test]
    fn mapped_key_selection_is_positional_so_pool_order_matters() {
        use crate::disc::ContentFormat;
        let key_a = [0xAAu8; 16];
        let key_b = [0xBBu8; 16];
        let ul = aacs::content::ALIGNED_UNIT_LEN;
        let usz = (ul / 2048) as u32;
        // Unit 0 encrypted under key_a, unit 1 under key_b.
        let build = || {
            let mut buf = vec![0u8; 2 * ul];
            for (i, k) in [key_a, key_b].iter().enumerate() {
                let mut u = clear_ts_unit();
                aacs_encrypt_unit_for_test(&mut u, k);
                buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
            }
            buf
        };
        // Map: unit 0 → pool position 0, unit 1 → pool position 1.
        let map = AacsKeyMap::from_ranges_phased(vec![
            (0, usz, 0, Phase::Even),
            (usz, 2 * usz, 1, Phase::Even),
        ]);
        // Pool in CPS-unit order: each range gets its own key, both come clean.
        let mut buf = build();
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        decrypt_sectors_mapped(&mut buf, &keys, 0, &map)
            .expect("pool in CPS-unit order decrypts clean");
        // SAME keys, SAME CPS-unit numbers, swapped POSITIONS. If the number were
        // what mattered (or if the path searched the pool) this would be
        // equivalent; positional indexing makes it decrypt both units wrong.
        let mut buf = build();
        let swapped = DecryptKeys::Aacs {
            unit_keys: vec![(1, key_b), (0, key_a)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        assert!(
            decrypt_sectors_mapped(&mut buf, &swapped, 0, &map).is_err(),
            "a reordered pool must fail loud — key selection is positional, so the \
             ORDER a KeySource returns its keys in is part of the contract"
        );
    }

    /// The correct-phase safety `is_clean` fires loud: an even unit whose mapped
    /// key is wrong does NOT come clean → `DecryptFailed` (not silent corruption).
    #[test]
    fn mapped_phase_verify_fails_loud_on_wrong_key() {
        use crate::disc::ContentFormat;
        let ul = aacs::content::ALIGNED_UNIT_LEN;
        let usz = (ul / 2048) as u32;
        let mut buf = vec![0u8; 2 * ul];
        let mut u0 = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut u0, &[0xAAu8; 16]); // encrypted under A
        buf[..ul].copy_from_slice(&u0);
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xCCu8; 16])], // map slot points at the WRONG key
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let map = AacsKeyMap::from_ranges_phased(vec![(0, 2 * usz, 0, Phase::Even)]);
        assert!(matches!(
            decrypt_sectors_mapped(&mut buf, &keys, 0, &map),
            Err(crate::error::Error::DecryptFailed)
        ));
    }

    /// Phase::All (multi-CPS / base) decrypts EVERY unit and never runs the verify
    /// — the common-disc path is byte-for-byte unchanged.
    #[test]
    fn mapped_all_phase_decrypts_every_unit() {
        use crate::disc::ContentFormat;
        let key = [0x11u8; 16];
        let ul = aacs::content::ALIGNED_UNIT_LEN;
        let mut buf = vec![0u8; 4 * ul];
        for i in 0..4 {
            let mut u = clear_ts_unit();
            aacs_encrypt_unit_for_test(&mut u, &key);
            buf[i * ul..(i + 1) * ul].copy_from_slice(&u);
        }
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        decrypt_sectors_mapped(
            &mut buf,
            &keys,
            0,
            &AacsKeyMap::from_ranges(vec![(0, u32::MAX, 0)]),
        )
        .expect("all-phase decrypts");
        for i in 0..4 {
            assert!(
                aacs::content::is_clean(&buf[i * ul..(i + 1) * ul], ContentFormat::BdTs),
                "unit {i} decrypted (All)"
            );
        }
    }

    // ── decrypt_threads resolution (read-only; no global mutation) ─────────

    /// The default (auto) decrypt thread count is always a usable pool size:
    /// at least 1 (a 0-thread rayon pool is invalid) and never above
    /// MAX_THREADS (rayon stack-memory cap). This test reads the resolved
    /// value without mutating the process-global override, so it is safe to
    /// run in parallel with other tests.
    ///
    /// Grounding: `cores.clamp(1, MAX_THREADS)` in the default branch;
    /// `env.min(MAX_THREADS)` in the env branch.
    /// Mutation: change `.clamp(1, MAX_THREADS)` to `.clamp(0, MAX_THREADS)`
    /// on a 0-core probe (unlikely) — more robustly, change the cap to
    /// `MAX_THREADS * 2` -> on a many-core CI box the upper-bound assert can
    /// fail. The lower-bound (>=1) guard is the load-bearing invariant.
    #[test]
    fn decrypt_threads_within_valid_pool_range() {
        let n = decrypt_threads();
        assert!(n >= 1, "decrypt thread count must be at least 1, got {n}");
        assert!(
            n <= MAX_THREADS,
            "decrypt thread count must not exceed MAX_THREADS ({MAX_THREADS}), got {n}"
        );
    }
}

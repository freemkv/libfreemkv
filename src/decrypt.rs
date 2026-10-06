//! Decrypt-on-read layer. Decrypts sectors in-place using resolved keys
//! from disc scanning; handles AACS 1.0/2.0 and CSS transparently, and
//! the caller never sees encrypted data unless explicitly bypassed.
//!
//! AACS aligned units decrypt independently, so buffers of at least [`PARALLEL_MIN_UNITS`]
//! units parallelize across a rayon pool; smaller buffers use the serial path. Thread count
//! resolves from [`set_decrypt_threads`], else `FREEMKV_THREADS`, else all cores (capped at
//! [`MAX_THREADS`]).

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

// Current rayon pool. `set_decrypt_threads` swaps it without leaking the old one; in-flight
// calls hold their own `Arc` via `decrypt_pool` and finish on it.
static DECRYPT_POOL: RwLock<Option<Arc<rayon::ThreadPool>>> = RwLock::new(None);
// Set once a pool build fails (e.g. OS thread limit) so later reads go serial without retrying
// the spawn every buffer; `set_decrypt_threads` clears it.
static POOL_BUILD_FAILED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Configure how many threads to use for AACS unit decryption. A value
/// of `0` resets to the env / default resolution. `1` forces serial.
/// `N > 1` builds a new rayon pool of size N (capped at [`MAX_THREADS`])
/// and atomically replaces the live pool.
///
/// Thread-safe. Live decrypt calls keep their previously-acquired pool
/// reference for the call; subsequent calls see the new pool. Pool
/// construction is ~ms-scale; safe to call from a settings POST handler.
pub fn set_decrypt_threads(n: usize) {
    let clamped = n.min(MAX_THREADS);
    DECRYPT_THREADS.store(clamped, Ordering::Relaxed);
    // Drop the existing pool so the next decrypt_pool() call rebuilds with the
    // new thread count. Recover the guard on poisoning like `decrypt_pool` does —
    // skipping the swap would keep the STALE pool alive, making the setting no-op.
    let mut guard = DECRYPT_POOL.write().unwrap_or_else(|e| e.into_inner());
    *guard = None;
    POOL_BUILD_FAILED.store(false, Ordering::Relaxed);
}

// Get (or lazily build) the pool; `Arc` so in-flight work survives a concurrent
// `set_decrypt_threads` swap. `None` if unbuildable (e.g. OS thread limit) — caller falls back
// to serial.
fn decrypt_pool() -> Option<Arc<rayon::ThreadPool>> {
    pool_or_build(&DECRYPT_POOL, &POOL_BUILD_FAILED, || {
        rayon::ThreadPoolBuilder::new()
            .num_threads(decrypt_threads())
            .thread_name(|i| format!("freemkv-decrypt-{i}"))
            .build()
            .ok()
    })
}

// `slot` holds the pool; `failed` remembers a failed `build` so it is attempted (and logged) once.
fn pool_or_build(
    slot: &RwLock<Option<Arc<rayon::ThreadPool>>>,
    failed: &std::sync::atomic::AtomicBool,
    build: impl FnOnce() -> Option<rayon::ThreadPool>,
) -> Option<Arc<rayon::ThreadPool>> {
    // Fast path: pool already built. A poisoned read lock still yields a
    // usable guard (the pool Arc is immutable once stored).
    {
        let guard = slot.read().unwrap_or_else(|e| e.into_inner());
        if let Some(pool) = guard.as_ref() {
            return Some(Arc::clone(pool));
        }
    }
    if failed.load(Ordering::Relaxed) {
        return None;
    }
    // Slow path: build a new one under the write lock, recovering the guard on
    // poisoning (a prior panic) rather than propagating a secondary panic — we
    // simply rebuild. Double-check after acquiring in case another caller won.
    let mut guard = slot.write().unwrap_or_else(|e| e.into_inner());
    if let Some(pool) = guard.as_ref() {
        return Some(Arc::clone(pool));
    }
    if failed.load(Ordering::Relaxed) {
        return None;
    }
    let Some(pool) = build().map(Arc::new) else {
        failed.store(true, Ordering::Relaxed);
        tracing::warn!("decrypt thread pool could not be built; decrypting serially");
        return None;
    };
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
    // Resolve `FREEMKV_THREADS` + `available_parallelism()` ONCE and cache it:
    // this runs on the per-buffer decrypt hot path, so a getenv/alloc/syscall
    // per call is pure overhead (the `set_decrypt_threads` override still works).
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
    /// AACS (Blu-ray / UHD / HD-DVD). Unit keys only — bus encryption is ALREADY
    /// removed upstream by the drive's single de-bus point
    /// ([`crate::sector::bus_removal::BusStage`]), so no read data key is threaded
    /// here. The `format` is the disc's content container (BD/UHD/FMTS = Transport
    /// Stream, HD-DVD `.evo` = Program Stream); it travels with the keys because
    /// both are resolved once per disc, and the key SELECTOR (`is_clean`) needs it
    /// to prove a key structurally against the right container. Only a
    /// [`KeyRing`](crate::keys::KeyRing) builds one (KU §2.2).
    #[non_exhaustive]
    Aacs {
        unit_keys: Vec<(u32, [u8; 16])>,
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
/// common disc is byte-for-byte unchanged. `Verify` is a forensic index whose every
/// phase probe faulted (KU design §5.3): each unit is kept only if it verifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    All,
    Even,
    Odd,
    /// Phase unknown: decrypt each unit, keep it (CPI cleared) only if [`is_clean`]
    /// passes, else restore its ciphertext for the muxer to drop. No public spec for
    /// FMTS (KS-25, KS-26); per unit because each unit is its own CBC chain (KS-3).
    ///
    /// [`is_clean`]: crate::aacs::content::is_clean
    Verify,
}

// Does this unit belong to the phase we hold the key for? `Phase::All` means the whole range is
// ours; a wrong index gets the wrong half.
fn unit_is_our_phase(unit_lba: u32, range_start: u32, unit_sectors: u32, phase: Phase) -> bool {
    let want_odd = match phase {
        // `Verify` tries every unit; the per-unit verify decides what is ours.
        Phase::All | Phase::Verify => return true,
        Phase::Even => false,
        Phase::Odd => true,
    };
    // `saturating_sub`/`max(1)` guard against a malformed key map (unit below its
    // range start, or zero unit size) — must not panic (overflow/div-by-zero) in
    // a long-running service. Unit 0 of the range is even, the safe default.
    let unit_ix = unit_lba.saturating_sub(range_start) / unit_sectors.max(1);
    (unit_ix % 2 == 1) == want_odd
}

/// Proactive AACS key-selection map: which held unit key decrypts each LBA of a title's
/// encrypted content, decided ONCE before mux from the disc's CPS-unit (and, later, FMTS
/// segment) structure — never by trial-decrypt-and-check per unit at mux time. Ends the mux
/// "key-server storm" caused by that per-unit re-derivation.
///
/// Ranges are `[start_lba, end_lba)` → index into the `Aacs { unit_keys }`
/// pool, sorted and disjoint. An LBA in no range (incl. clear nav/filesystem
/// sectors) is passed through untouched — the map is a POSITIVE list only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AacsKeyMap {
    // (start_lba, end_lba, key_idx, phase). An LBA in NO range is passed through
    // untouched — the map is a positive list of "this key here", nothing more.
    ranges: Vec<(u32, u32, usize, Phase)>,
    // Parallel to `ranges`: the original start each piece's unit parity is measured from.
    anchors: Vec<u32>,
    // Distinct, sorted key indices the map selects — derived from `ranges` once at
    // construction so the per-batch decrypt bounds check does not re-allocate/sort
    // it on every read. Kept in sync by building both in `from_ranges_phased`.
    key_indices: Vec<usize>,
}

impl AacsKeyMap {
    /// Build from `[start_lba, end_lba) → key_idx` ranges that decrypt EVERY unit
    /// (single- or multi-CPS): each range is [`Phase::All`]. An LBA in no range is
    /// passed through untouched.
    pub(crate) fn from_ranges(ranges: Vec<(u32, u32, usize)>) -> Self {
        let phased = ranges
            .into_iter()
            .map(|(s, e, i)| (s, e, i, Phase::All))
            .collect();
        Self::from_ranges_phased(phased)
    }

    /// Build a PHASE-AWARE map (FMTS): each range carries which unit-parity its key
    /// opens ([`Phase::Even`]/[`Phase::Odd`] for a forensic segment, [`Phase::All`]
    /// for base/CPS). Ranges are sorted; an LBA in no range is passed through.
    pub(crate) fn from_ranges_phased(mut ranges: Vec<(u32, u32, usize, Phase)>) -> Self {
        ranges.sort_by_key(|&(start, _, _, _)| start);
        // Disjointness (entry_for relies on it): each boundary-to-boundary stretch goes to
        // the covering range that starts last; `anchors` keeps the ORIGINAL start that
        // FMTS unit parity is measured from.
        let mut bounds: Vec<u32> = ranges.iter().flat_map(|r| [r.0, r.1]).collect();
        bounds.sort_unstable();
        bounds.dedup();
        let mut disjoint: Vec<(u32, u32, usize, Phase)> = Vec::with_capacity(ranges.len());
        let mut anchors: Vec<u32> = Vec::with_capacity(ranges.len());
        let mut srcs: Vec<usize> = Vec::with_capacity(ranges.len());
        let mut overlapped = false;
        for w in bounds.windows(2) {
            let (a, b) = (w[0], w[1]);
            let mut cover = (0..ranges.len()).rev().filter(|&i| {
                let r = ranges[i];
                r.0 <= a && b <= r.1 && r.0 < r.1
            });
            let Some(win) = cover.next() else { continue };
            overlapped |= cover.next().is_some();
            if srcs.last() == Some(&win)
                && let Some(last) = disjoint.last_mut()
                && last.1 == a
            {
                last.1 = b;
                continue;
            }
            let r = ranges[win];
            disjoint.push((a, b, r.2, r.3));
            anchors.push(r.0);
            srcs.push(win);
        }
        if overlapped {
            tracing::warn!("overlapping AACS key ranges; the later-starting range takes over");
        }
        let ranges = disjoint;
        let mut key_indices: Vec<usize> = ranges.iter().map(|&(_, _, i, _)| i).collect();
        key_indices.sort_unstable();
        key_indices.dedup();
        Self {
            ranges,
            anchors,
            key_indices,
        }
    }

    /// The `(key_idx, phase, range_start_lba)` for the aligned unit at `lba`, or
    /// `None` when no range covers it (not encrypted content this map keys — pass
    /// the unit through untouched). O(log n). `range_start_lba` lets the mapped
    /// decrypt compute a unit's parity WITHIN a forensic segment (`Even`/`Odd`).
    pub fn entry_for(&self, lba: u32) -> Option<(usize, Phase, u32)> {
        let i = match self
            .ranges
            .binary_search_by(|&(start, _, _, _)| start.cmp(&lba))
        {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        let (start, end, idx, ph) = self.ranges[i];
        (lba >= start && lba < end)
            .then(|| (idx, ph, self.anchors.get(i).copied().unwrap_or(start)))
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
    /// once at construction (see `from_ranges_phased`).
    pub fn key_indices(&self) -> &[usize] {
        &self.key_indices
    }

    /// Build the FMTS **read plan**: the title's aligned units filtered down to only the units
    /// this rip must actually read — every default / CPS unit, plus, inside each forensic
    /// segment, ONLY our-phase ([`Phase::Even`] / [`Phase::Odd`]) units; alternate-phase units
    /// belong to a different device group's variant and are omitted entirely. `extents` are the
    /// title's clip extents; `unit_sectors` is the AACS aligned-unit size (3 sectors). A map
    /// with no forensic range returns `extents` unchanged, and a kept unit is always one
    /// `decrypt_sectors_mapped` would open.
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
                    // A `Verify` range reads both halves: which one is ours is unknown.
                    None | Some((_, Phase::All | Phase::Verify, _)) => true,
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

// Decrypt `buf` with a resolved AACS key map: the content-less convenience form of
// `decrypt_sectors_mapped_in_content`, used only by tests (production reads always go
// through that one, which honours the content extents).
#[cfg(test)]
pub(crate) fn decrypt_sectors_mapped(
    buf: &mut [u8],
    keys: &DecryptKeys,
    base_lba: u32,
    map: &AacsKeyMap,
) -> Result<(), crate::error::Error> {
    decrypt_sectors_mapped_in_content(buf, keys, base_lba, map, None).map(|_| ())
}

/// `decrypt_sectors_mapped` restricted to the disc's encrypted-content extents.
/// `content` is the sorted/merged `(start_lba, sector_count)` content map: an
/// aligned unit whose absolute LBA falls in NO content range is clear
/// (UDF filesystem / BDMV nav) and is passed through untouched — never decrypted,
/// verified, or counted as loss. `None` means "the caller only reads encrypted
/// content", so every unit is treated as content (the legacy behaviour). This is
/// how [`crate::sector::DecryptingSectorSource::with_content_ranges`] honours its contract.
/// Returns how many damaged FMTS units (a lone failed verify) it blanked.
pub(crate) fn decrypt_sectors_mapped_in_content(
    buf: &mut [u8],
    keys: &DecryptKeys,
    base_lba: u32,
    map: &AacsKeyMap,
    content: Option<&[(u32, u32)]>,
) -> Result<usize, crate::error::Error> {
    match keys {
        // The AACS arm only reads the keys: no per-batch deep clone.
        DecryptKeys::Aacs { .. } => apply_aacs_map(buf, keys, base_lba, map, content),
        // CSS re-cracks into its title key, so it needs its own mutable copy.
        _ => {
            let mut keys = keys.clone();
            decrypt_span(buf, &mut keys, base_lba, Some(map), content).map(|_| 0)
        }
    }
}

/// Is `lba` inside any `(start_lba, sector_count)` content range? Used to gate the
/// mapped decrypt so clear filesystem/nav units outside every encrypted-content
/// extent are passed through untouched.
fn lba_in_content_ranges(lba: u32, ranges: &[(u32, u32)]) -> bool {
    span_in_content_ranges(lba, 1, ranges)
}

/// Does the sector span `[lba, lba + count)` intersect any content range? The ONE
/// range lookup the decrypt paths share. `ranges` must be sorted and merged
/// (non-overlapping), as [`crate::Disc::encrypted_content_ranges`] returns them.
pub(crate) fn span_in_content_ranges(lba: u32, count: u32, ranges: &[(u32, u32)]) -> bool {
    debug_assert!(
        ranges.windows(2).all(|w| w[0].0 <= w[1].0),
        "content ranges must be sorted ascending by start LBA for the binary search"
    );
    let end = lba as u64 + count as u64;
    // Merged ranges have ascending ends too, so the last range starting before
    // `end` is the only candidate — O(log n), not O(ranges).
    let i = ranges.partition_point(|&(start, _)| (start as u64) < end);
    i > 0 && {
        let (start, cnt) = ranges[i - 1];
        (lba as u64) < start as u64 + cnt as u64
    }
}

// AACS scheme step: apply `map`'s per-unit keys to `buf`, in-place — no key trial; the one
// `is_clean` verdict is `Phase::Verify`'s keep-or-restore. Refusal belongs to `decrypt_span`.
fn apply_aacs_map(
    buf: &mut [u8],
    keys: &DecryptKeys,
    base_lba: u32,
    map: &AacsKeyMap,
    content: Option<&[(u32, u32)]>,
) -> Result<usize, crate::error::Error> {
    let (unit_keys, format) = match keys {
        DecryptKeys::Aacs { unit_keys, format } => (unit_keys, *format),
        // Clear / CSS: the mapped path is AACS-only. Leave the buffer untouched;
        // CSS descrambles via `decrypt_sectors` and `None` is already clear.
        _ => return Ok(0),
    };
    if format == crate::disc::ContentFormat::MpegPs {
        return decrypt_hddvd_packs(buf, keys, base_lba, map, content, None).map(|r| r.blanked);
    }

    let unit_len = aacs::content::ALIGNED_UNIT_LEN;
    let unit_sectors = aacs::content::ALIGNED_UNIT_SECTORS;

    // Validate every selectable index up front (fail loud) so the per-unit hot
    // loop can index without bounds churn and a resolver gap never silently
    // passes ciphertext through as "decrypted".
    for &idx in map.key_indices() {
        if unit_keys.get(idx).is_none() {
            return Err(crate::error::Error::DecryptFailed);
        }
    }

    // Cheap safety net: a correct map decrypts CORRECT-PHASE forensic units to clean TS, so
    // a map bug surfaces as loud DecryptFailed, not silent corruption (verdict at the end).
    let verify_failed = std::sync::atomic::AtomicBool::new(false);
    // Per mapped key: (failed, verified) FMTS units of this read.
    let tally: Vec<[AtomicUsize; 2]> = unit_keys.iter().map(|_| Default::default()).collect();
    // A failing FMTS unit another held key opens: a wrong mapped key, never damage.
    let opened_elsewhere = std::sync::atomic::AtomicBool::new(false);

    let decrypt_one = |idx_in_buf: usize, chunk: &mut [u8]| {
        let unit_lba = base_lba.saturating_add((idx_in_buf as u32) * unit_sectors);
        // Content-extent gate: a unit outside the disc's encrypted-content ranges is
        // clear filesystem / BDMV nav — pass it through untouched, never decrypting,
        // verifying, or counting it as loss (the `with_content_ranges` contract).
        if let Some(ranges) = content
            && !lba_in_content_ranges(unit_lba, ranges)
        {
            return;
        }
        if chunk.len() != unit_len {
            // Trailing partial unit: normally a genuinely-clear tail, left as-is. But
            // one that is BOTH inside a mapped range AND flagged encrypted is a CBC
            // fragment we cannot decrypt — fail loud instead of shipping it as clear.
            if map.entry_for(unit_lba).is_some()
                && aacs::content::aacs_unit_seed_encrypted(chunk, format)
            {
                verify_failed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }
        // No range covers this LBA — expected for clear filesystem/nav, but an
        // ENCRYPTED unit outside every range is an orphan clip we cannot key;
        // emitting it verbatim would ship ciphertext as clear content.
        let Some((key_idx, phase, range_start)) = map.entry_for(unit_lba) else {
            if aacs::content::aacs_unit_seed_encrypted(chunk, format) {
                verify_failed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        };
        // A flagged unit whose seed lacks its sync (damage, or a read cut off its grid) opens
        // under no key: blanked, never refused (`blank_damaged_units`, which readers run first).
        if !aacs::content::aacs_unit_on_grid(chunk, format) {
            chunk.fill(0);
            return;
        }
        // PHASE GATE (FMTS forensic segment): the segment interleaves two variants
        // at the unit level. Decrypt ONLY our parity; leave the alternate half as
        // ciphertext (the muxer drops untouched ciphertext cleanly — no garble).
        if !unit_is_our_phase(unit_lba, range_start, unit_sectors, phase) {
            return; // alternate half — leave as-is
        }
        // Gate on the authoritative encrypted flag ONLY (CPI bits in the clear
        // seed): a clear unit is left untouched; an encrypted unit is decrypted
        // with its MAPPED key and trusted.
        if !aacs::content::aacs_unit_encrypted(chunk, format) {
            return;
        }
        // Bounds already proven above; index directly. Bus encryption was already
        // removed by the drive's single de-bus point before this buffer arrived —
        // this path only applies the CPS unit key.
        let key = &unit_keys[key_idx].1;
        if phase == Phase::Verify {
            // KU design §5.3 (no public FMTS spec, KS-25/KS-26): keep a unit only if it
            // verifies; KS-3 [BD] §3.10.1 "A new CBC cipher chain is started for each Aligned Unit".
            let mut ciphertext = [0u8; aacs::content::ALIGNED_UNIT_LEN];
            ciphertext.copy_from_slice(chunk);
            aacs::content::decrypt_unit(chunk, key);
            if aacs::content::is_clean(chunk, format) {
                aacs::content::clear_copy_permission_indicator(chunk, format);
            } else {
                chunk.copy_from_slice(&ciphertext); // flagged ciphertext: the muxer drops it
            }
            return;
        }
        // Correct-phase forensic verify: a failing unit is blanked; the read is judged below.
        if matches!(phase, Phase::Even | Phase::Odd) {
            let mut ciphertext = [0u8; aacs::content::ALIGNED_UNIT_LEN];
            ciphertext.copy_from_slice(chunk);
            aacs::content::decrypt_unit(chunk, key);
            let clean = aacs::content::is_clean(chunk, format);
            tally[key_idx][clean as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if !clean {
                let opens = |k: &[u8; 16]| {
                    let mut c = ciphertext;
                    aacs::content::decrypt_unit(&mut c, k);
                    aacs::content::is_clean(&c, format)
                };
                if unit_keys
                    .iter()
                    .enumerate()
                    .any(|(j, (_, k))| j != key_idx && opens(k))
                {
                    opened_elsewhere.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                chunk.fill(0);
                return;
            }
        } else {
            aacs::content::decrypt_unit(chunk, key);
        }
        // KS-5 [BD] §3.10.2: CPI "shall be set to 00₂ if the data is not encrypted": a unit
        // we decrypted (damaged or not) is no longer ciphertext (KU design §5.4, K-13).
        aacs::content::clear_copy_permission_indicator(chunk, format);
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
    // A wrong key fails every unit it keys (resolve's phase probe catches it first); damage
    // fails one. E7013 per key only: two or more failures, none verifying, or a failing unit
    // another held key opens. Lone failures under different keys are damage.
    let tally: Vec<[usize; 2]> = tally
        .into_iter()
        .map(|t| t.map(|n| n.into_inner()))
        .collect();
    let wrong = |t: &[usize; 2]| t[0] >= WRONG_KEY_FAILURES && t[1] == 0;
    let failed: usize = tally.iter().map(|t| t[0]).sum();
    if opened_elsewhere.into_inner() || tally.iter().any(wrong) {
        tracing::error!(
            target: "freemkv::decrypt",
            lba = base_lba,
            failed,
            code = crate::error::E_DECRYPT_FAILED,
            "no FMTS unit of the read verifies under its mapped key: wrong key"
        );
        return Err(crate::error::Error::DecryptFailed);
    }
    if failed > 0 {
        tracing::warn!(
            target: "freemkv::decrypt",
            lba = base_lba,
            units = failed,
            "damaged AACS unit (fails its FMTS verify): blanked, the rip carries on"
        );
    }
    Ok(failed)
}

/// What a per-pack HD DVD decrypt of one read found.
#[derive(Debug)]
pub(crate) struct HdPacks {
    /// The CPI in force after the read's last pack: the lead CPI of a read that follows it.
    pub(crate) last_cpi: Option<aacs::hddvd::Cpi>,
    /// Encrypted packs blanked because no NV_PCK before them gave their EVOBU's CPI.
    pub(crate) blanked: usize,
}

/// Decrypt the HD DVD packs of `buf` (read at `base_lba`) in place, per `[HD]` §4.3.4.
///
/// Each NV_PCK sets the CPI for the packs after it; `lead` is the CPI in force at the first
/// pack (from an earlier read). A pack flagged scrambled, or an HL_PCK under a valid KEY_VF, is
/// decrypted with the Title Key its CPI's `TITLE_KEY_PTR` names (`unit_keys` numbers keys by
/// that pointer); the map only says which sectors are keyed content. An encrypted pack with no
/// CPI is blanked and counted. Ciphertext outside the map, under a Segment Key (KEY_VF `01`),
/// or under a Title Key not held fails loud: shipping it would pass ciphertext as clear.
pub(crate) fn decrypt_hddvd_packs(
    buf: &mut [u8],
    keys: &DecryptKeys,
    base_lba: u32,
    map: &AacsKeyMap,
    content: Option<&[(u32, u32)]>,
    lead: Option<aacs::hddvd::Cpi>,
) -> Result<HdPacks, crate::error::Error> {
    use aacs::hddvd::{PACK_LEN, PackKind, classify, decrypt_pack, needs_key};
    let DecryptKeys::Aacs { unit_keys, .. } = keys else {
        return Ok(HdPacks {
            last_cpi: lead,
            blanked: 0,
        });
    };
    let mut cur = lead;
    let mut jobs: Vec<Option<(&[u8; 16], aacs::hddvd::Cpi)>> = Vec::new();
    let mut blank = Vec::new();
    let mut refusal: Option<&'static str> = None;
    for (i, pack) in buf.as_chunks::<PACK_LEN>().0.iter().enumerate() {
        jobs.push(None);
        let lba = base_lba.saturating_add(i as u32);
        let kind = classify(pack);
        if let PackKind::Nav(cpi) = kind {
            cur = cpi;
            continue;
        }
        if content.is_some_and(|r| !lba_in_content_ranges(lba, r)) || !needs_key(kind, cur.as_ref())
        {
            continue;
        }
        if map.entry_for(lba).is_none() {
            refusal.get_or_insert("encrypted pack outside every keyed range");
            continue;
        }
        let Some(cpi) = cur else {
            blank.push(i);
            continue;
        };
        if cpi.key_vf() != 0b10 {
            refusal.get_or_insert("pack under a Segment Key or no valid key pointer");
            continue;
        }
        match unit_keys.iter().find(|(n, _)| *n == cpi.title_key_ptr()) {
            Some((_, kt)) => jobs[i] = Some((kt, cpi)),
            None => {
                refusal.get_or_insert("the pack's Title Key is not held");
            }
        }
    }
    if let Some(why) = refusal {
        tracing::error!(target: "freemkv::decrypt", lba = base_lba, code = crate::error::E_DECRYPT_FAILED, why, "HD DVD pack cannot be decrypted");
        return Err(crate::error::Error::DecryptFailed);
    }
    let run = |(i, pack): (usize, &mut [u8])| {
        if let Some((kt, cpi)) = jobs[i] {
            decrypt_pack(pack, kt, &cpi);
        }
    };
    let pool = (decrypt_threads() > 1 && jobs.len() >= PARALLEL_MIN_UNITS * 3)
        .then(decrypt_pool)
        .flatten();
    match pool {
        Some(pool) => pool.install(|| {
            buf.par_chunks_exact_mut(PACK_LEN).enumerate().for_each(run);
        }),
        None => buf
            .as_chunks_mut::<PACK_LEN>()
            .0
            .iter_mut()
            .map(|p| p.as_mut_slice())
            .enumerate()
            .for_each(run),
    }
    for &i in &blank {
        buf[i * PACK_LEN..(i + 1) * PACK_LEN].fill(0);
    }
    if let Some(&first) = blank.first() {
        tracing::warn!(
            target: "freemkv::decrypt",
            lba = base_lba.saturating_add(first as u32),
            packs = blank.len(),
            "encrypted HD DVD pack with no NV_PCK before it: blanked, the rip carries on"
        );
    }
    Ok(HdPacks {
        last_cpi: cur,
        blanked: blank.len(),
    })
}

/// FMTS units of one read that must fail their verify, none verifying, under one key before
/// it is a wrong key rather than damage: a wrong key fails them all.
const WRONG_KEY_FAILURES: usize = 2;

/// Blank the damaged BD-TS units of `buf` (read at `base_lba` on the caller's unit grid):
/// zero-fill them, like a sweep's unread sector, and return how many. Damaged: no TS sync at
/// byte 4, flagged (CPI) or not; unflagged only when not clean TS either (a zeroed head over
/// clear TS is a sweep hole), or a flagged partial unit ending the source (`tail_at_end`).
/// No key opens them: read damage, never a key verdict or E7013, alone, clustered, or off the
/// grid (1.7.7 muxed through all three). Only units `covered` (keyed or proven on arrival) are
/// judged. A flagged garbage seed keeping 0x47 at byte 4 (~1 in 256) is not caught here.
pub(crate) fn blank_damaged_units(
    buf: &mut [u8],
    base_lba: u32,
    format: crate::disc::ContentFormat,
    covered: &dyn Fn(u32) -> bool,
    tail_at_end: bool,
) -> usize {
    if format != crate::disc::ContentFormat::BdTs {
        return 0;
    }
    let unit_len = aacs::content::ALIGNED_UNIT_LEN;
    let mut damaged = Vec::new();
    for (i, unit) in buf.chunks_exact(unit_len).enumerate() {
        let lba = base_lba.saturating_add(i as u32 * aacs::content::ALIGNED_UNIT_SECTORS);
        if !covered(lba) {
            continue;
        }
        // KS-4 [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is used as the seed";
        // KS-2: each source packet is "the TP_extra_header (4 bytes) and an MPEG Transport
        // packet", so an intact unit, clear or not, has the TS sync at byte 4 (KS-22 too).
        let lost = if aacs::content::aacs_unit_seed_encrypted(unit, format) {
            !aacs::content::aacs_unit_on_grid(unit, format)
        } else {
            // A zeroed or garbled head over a rest that is not TS: ciphertext no key opens.
            // A trailing zero run keeps its head; an all-zero unit is clean (a sweep hole).
            unit[4] != 0x47 && !aacs::content::is_clean(unit, format)
        };
        if lost {
            damaged.push(i);
        }
    }
    // A truncated copy's flagged last partial unit cannot be opened as a unit (KS-3); a partial
    // read mid-content is a caller bug, left for the decrypt to refuse.
    let whole = buf.len() / unit_len;
    let tail = &buf[whole * unit_len..];
    let at = base_lba.saturating_add(whole as u32 * aacs::content::ALIGNED_UNIT_SECTORS);
    if tail_at_end
        && !tail.is_empty()
        && covered(at)
        && aacs::content::aacs_unit_seed_encrypted(tail, format)
    {
        damaged.push(whole);
    }
    // KS-3 [BD] §3.10.1: "A new CBC cipher chain is started for each Aligned Unit", so the
    // loss is this unit alone.
    for &i in &damaged {
        let end = ((i + 1) * unit_len).min(buf.len());
        buf[i * unit_len..end].fill(0);
    }
    if let Some(&first) = damaged.first() {
        tracing::warn!(
            target: "freemkv::decrypt",
            lba = base_lba.saturating_add(first as u32 * aacs::content::ALIGNED_UNIT_SECTORS),
            units = damaged.len(),
            "damaged AACS unit (no key can open it): blanked, the rip carries on"
        );
    }
    damaged.len()
}

/// Decrypt a buffer of sectors in-place — the CSS / clear path only.
///
/// For CSS: descrambles per 2048-byte sector, self-cracking the title key from the data. For
/// `None`: a no-op. For AACS: **always** returns `Err(DecryptFailed)` — AACS decrypts
/// exclusively through the resolved key map (`decrypt_sectors_mapped`); reaching this arm with
/// AACS keys means a reader was built without installing its map (a bug). `unit_key_idx` is a
/// legacy parameter, ignored.
pub fn decrypt_sectors(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    unit_key_idx: usize,
) -> Result<usize, crate::error::Error> {
    let _ = unit_key_idx;
    decrypt_span(buf, keys, 0, None, None)
}

/// Legacy alias of [`decrypt_sectors`]. Under the keymap-only model AACS decrypts
/// EXCLUSIVELY through the resolved key map (`decrypt_sectors_mapped`), so there is
/// no per-unit content-extent gate on THIS map-less path: the AACS arm fails loud
/// and the CSS / `None` arm self-gates on its per-sector scramble flag. `base_lba`
/// and `content_ranges` are therefore inert here — retained only so the wrapper
/// signature stays stable for the `DecryptingSectorSource` dispatch. The mapped
/// AACS path honours content extents via
/// [`decrypt_sectors_mapped_in_content`]. Prefer [`decrypt_sectors`] in new code.
pub fn decrypt_sectors_in_content(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    unit_key_idx: usize,
    base_lba: u32,
    content_ranges: &[(u32, u32)],
) -> Result<usize, crate::error::Error> {
    let _ = unit_key_idx;
    decrypt_span(buf, keys, base_lba, None, Some(content_ranges))
}

// THE decrypt orchestrator: every path into this crate's decryption goes through here. Resolve
// a key for the span, apply it, refuse if none can be proven.
fn decrypt_span(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    base_lba: u32,
    map: Option<&AacsKeyMap>,
    content: Option<&[(u32, u32)]>,
) -> Result<usize, crate::error::Error> {
    let dropped: usize = match keys {
        DecryptKeys::None => 0,
        DecryptKeys::Aacs { .. } => {
            // AACS decrypts EXCLUSIVELY via a resolved key map (missing key fails at
            // RESOLVE time). No map here means the reader was built without one — the
            // old trial-decrypt path is gone: it silently applied wrong keys on a miss.
            let Some(map) = map else {
                return Err(crate::error::Error::DecryptFailed);
            };
            // Honour the content-extent gate when present: units outside the disc's
            // encrypted-content ranges are clear filesystem/nav and pass through.
            apply_aacs_map(buf, keys, base_lba, map, content)?;
            0
        }
        DecryptKeys::Css { title_key } => {
            // CSS SELF-recovers: the title key changes per VOB region and is re-cracked
            // constantly, but always FROM THE DATA ITSELF (see `css::descramble_region`),
            // so it needs none of the external-input recovery seam AACS key-fetch uses.
            css::descramble_region(buf, title_key)?
        }
    };
    Ok(dropped)
}

#[cfg(test)]
#[path = "decrypt_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "decrypt_spec_guards_tests.rs"]
mod spec_guards;

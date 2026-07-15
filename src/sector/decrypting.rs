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

use crate::decrypt::{DecryptKeys, decrypt_sectors, decrypt_sectors_in_content};
use crate::error::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::SectorSource;

/// Application-supplied "fetch a fresh key for THIS data" callback.
///
/// Invoked by [`DecryptingSectorSource`] when a read contains scrambled AACS
/// units that NONE of the currently-held unit keys could decrypt. The argument
/// is those still-scrambled 6144-byte aligned units (real on-disc ciphertext);
/// the return is any additional unit keys to add to the pool and retry with —
/// empty if the source can't help. Mirrors the DVD model (try the held key,
/// then ask the key source for the failing data) generalised to AACS.
///
/// The library performs NO key lookup or network I/O itself; this closure is
/// the seam an application uses to call its key source (e.g. an online key
/// service) with the exact ciphertext that failed. A **stateless, shared**
/// `Arc<Fn>` — the decorator owns the only mutable state (its call-count cap and
/// spent flag), so one closure is built once and cloned cheaply into every read
/// path (sweep / patch / mux); no per-decorator factory is needed. `Send + Sync`
/// so it can ride the mux highway's producer thread.
pub type KeyFetch = std::sync::Arc<dyn Fn(&[Vec<u8>]) -> Vec<[u8; 16]> + Send + Sync>;

/// Cap on how many per-unit decrypt-verify-failure diagnostics one read emits.
/// The diagnostic runs only on the failure (cold) path and bounds log volume so
/// a large undecryptable range can't flood the device log; the first few units
/// of any failed read fully characterise it (all-zero vs ciphertext, latency,
/// best-key-fit).
const MAX_DIAG_UNITS_PER_READ: usize = 4;

/// Master switch: "a read is not successful unless it also DECRYPTS."
///
/// When `true`, a read that returns scrambled AACS units which NO held key
/// (after any fetch) could decrypt fails loud with [`Error::DecryptFailed`]
/// instead of silently passing the still-encrypted bytes downstream. This turns
/// an undecryptable unit into a *read failure*, so:
///   * the rip's existing read-error recovery (sweep skip-ahead → patch
///     re-read) re-reads it off the disc while the disc is still present, and
///   * the mux path hard-fails (there is no clean data to mux) rather than
///     dropping content without a TS sync and reporting a clean rip.
///
/// All-zero (zero-filled) units are NOT `ts_sync_destroyed`, so they never trip
/// this — allowed-loss zero-fill that some authoring deliberately leaves stays
/// allowed (logged loud + continue elsewhere). Only the keyless raw sweep is
/// unaffected: it carries [`DecryptKeys::None`], so the `Aacs` guard below is
/// never met there and ciphertext is written verbatim.
///
/// Hardcoded `true`. Flip to `false` to ship without the behaviour — the unit
/// is then counted as decrypt loss exactly as before (the prior contract).
pub const DECRYPT_VERIFY_READ: bool = true;

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
    /// Cumulative bytes of AACS units that did not reassemble to clean TS on the
    /// VERIFY paths (sweep / patch / --no-raw verify). `decrypt_sectors` is a pure
    /// decrypt (it leaves the applied-key plaintext and never restores/nulls), so
    /// this counter is populated only on the fail-loud verify path — the mux path
    /// returns pass-through before tallying, treating broken TS as a muxer concern
    /// rather than decrypt loss. Where it is populated it feeds [`decrypt_loss`]
    /// so a partial verify failure can't be reported as a perfect rip. Shared `Arc`
    /// so the highway's producer thread and the consuming `Stream` see one tally.
    ///
    /// [`decrypt_loss`]: Self::decrypt_loss
    decrypt_dropped: Arc<AtomicU64>,
    /// The miss policy (see [`crate::sector::recovery::Recover`]) — a generic,
    /// scheme-neutral recovery the input stream (L3) installs and this decorator
    /// (L2) executes at the one seam when a content unit will not decrypt. `None`
    /// = no recovery (a miss is loss). Installed via
    /// [`with_key_fetch`](Self::with_key_fetch).
    recovery: Option<crate::sector::recovery::Recover>,
    /// Verify-only mode: a read decrypt-CHECKS a scratch copy of the bytes (to
    /// detect undecryptable units) but NEVER mutates `buf` — the inner
    /// ciphertext is returned unchanged. This is what makes a multipass sweep
    /// decrypt-aware: the sweep must write the *encrypted* bytes to the ISO, yet
    /// a unit that won't decrypt must still fail the read (`DECRYPT_VERIFY_READ`)
    /// so the existing read-error recovery (skip / NonTrimmed / patch) handles
    /// it. Default `false` (decrypt in place, the mux / `--no-raw` path).
    verify_only: bool,
    /// Encrypted-content extent map — the disc's m2ts ranges as sorted/merged
    /// `(start_lba, sector_count)` (see
    /// [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    /// When `Some`, a unit whose absolute LBA is OUTSIDE these ranges is clear
    /// (UDF filesystem / BDMV nav) and is passed through untouched: never
    /// decrypted, verified, or counted as loss. `None` means "the caller only
    /// reads encrypted content" (the mux reads title extents only) → every unit
    /// is treated as content (the legacy behaviour).
    content_ranges: Option<Arc<[(u32, u32)]>>,
    /// Reused scratch buffer for verify-only decrypt checks — avoids a per-read
    /// allocation on the sweep's hot path. Grown on demand, never shrunk.
    scratch: Vec<u8>,
    /// MUX pass-through switch (read > decrypt > mux). When `true`, every
    /// encrypted unit is decrypted in place and the bytes pass straight to the
    /// muxer — a unit that decrypts to broken TS is the muxer's concern, so the mux
    /// never conceals, re-fetches a key, or counts it as loss, and the read returns
    /// `Ok` (it can never abort over a bad-encoded unit). It hard-fails only on a
    /// genuine can't-decrypt (no key / misaligned unit), which surfaces as `Err`.
    /// This is "decrypt-verify is a RIP gate, not a MUX gate": the rip/verify path
    /// leaves this `false` (default) and fails loud via [`DECRYPT_VERIFY_READ`] so
    /// its read-error recovery re-reads the disc. Ciphertext is NEVER passed
    /// downstream either way — with a keyed disc every unit has a key applied.
    ///
    /// Mutually meaningful only with `!verify_only` (the in-place decrypt path
    /// the mux uses); a verify-only sweep keeps the rip's fail-loud contract.
    tolerate_decrypt_loss: bool,
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
            // No recovery by default. CSS self-decrypts in `decrypt_sectors`
            // (needs no external input); AACS installs a key-fetch via
            // `with_key_fetch`.
            recovery: None,
            verify_only: false,
            content_ranges: None,
            scratch: Vec::new(),
            tolerate_decrypt_loss: false,
        }
    }

    /// Opt into the MUX pass-through policy (read > decrypt > mux): decrypt every
    /// unit in place and pass the bytes to the muxer, never conceal / re-fetch /
    /// count broken TS as loss; fail loud only on a genuine can't-decrypt. See the
    /// [`tolerate_decrypt_loss`](Self::tolerate_decrypt_loss) field. The rip/verify
    /// path must NOT set this (it relies on fail-loud read-error recovery).
    pub fn tolerate_decrypt_loss(mut self) -> Self {
        self.tolerate_decrypt_loss = true;
        self
    }

    /// Restrict decrypt/verify to the disc's encrypted-content extents
    /// (sorted/merged `(start_lba, sector_count)` — see
    /// [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    /// Units outside content (UDF filesystem / BDMV nav) pass through untouched,
    /// so [`ts_sync_destroyed`](crate::aacs::content::ts_sync_destroyed) is never consulted
    /// about non-content bytes. Whole-disc readers (sweep / patch) set this; the
    /// mux leaves it unset because it only ever reads title extents.
    pub fn with_content_ranges(mut self, ranges: Arc<[(u32, u32)]>) -> Self {
        self.content_ranges = Some(ranges);
        self
    }

    /// Switch to verify-only mode: decrypt-CHECK each read on a scratch copy and
    /// fail the read (`DECRYPT_VERIFY_READ`) when a scrambled AACS unit won't
    /// decrypt, but leave `buf` as the original ciphertext. The multipass sweep
    /// uses this so its ISO stays encrypted while still rejecting silent-bad
    /// reads. No-op effect for `DecryptKeys::None` (nothing to check).
    pub fn verify_only(mut self) -> Self {
        self.verify_only = true;
        self
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

    /// Install a [`KeyFetch`] callback: when a read holds scrambled AACS units
    /// that no current key decrypts, the decorator hands those units to `cb` and
    /// adds any keys it returns to the pool, then re-decrypts. Only meaningful
    /// for [`DecryptKeys::Aacs`]; ignored otherwise. The library makes no network
    /// call — `cb` is the application's seam to its key source.
    pub fn with_key_fetch(mut self, cb: KeyFetch) -> Self {
        self.recovery = Some(crate::sector::recovery::key_fetch(cb));
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

    /// Decrypt `buf` in place with the active keys, applying the content gate
    /// when one is installed (whole-disc readers) or running ungated (the mux).
    /// The single dispatch both the first read and the post-fetch retry share,
    /// so they agree on which units are content and on the unit-key try order.
    fn decrypt_buf(
        buf: &mut [u8],
        keys: &mut DecryptKeys,
        unit_key_idx: usize,
        lba: u32,
        content: Option<&[(u32, u32)]>,
    ) -> Result<usize> {
        match content {
            Some(ranges) => decrypt_sectors_in_content(buf, keys, unit_key_idx, lba, ranges),
            None => decrypt_sectors(buf, keys, unit_key_idx),
        }
    }

    /// Emit a bounded, structured diagnostic for each undecryptable unit in a
    /// failed verify read. Called only on the failure (cold) path. On a fresh
    /// rip `buf` holds the post-decrypt bytes straight off the drive, so the
    /// per-unit signature is source ground truth (see the call site).
    ///
    /// Fields, per failing in-content unit:
    /// * `lba` — absolute disc LBA of the unit
    /// * `read_ms` — how long the inner drive read took (recovery grind vs clean
    ///   fast read)
    /// * `all_zero` — the unit is every-byte-`0x00` (source zero-fill, seen fresh
    ///   off the disc — no ISO ambiguity)
    /// * `ts_sync`/`ts_total` — TS sync bytes present vs possible (0/32 ⇒
    ///   scrambled-looking)
    /// * `distinct` — distinct byte values (entropy proxy: 1 ⇒ constant fill,
    ///   ~256 ⇒ ciphertext/garbage)
    /// * `best_sync` — the most TS syncs ANY held key restores (≈0 ⇒ no key fits
    ///   → missing key / garbage; high ⇒ a key nearly works → marginal bytes)
    /// * `head` — first 16 bytes (the plaintext TP_extra header) in hex
    fn diagnose_decrypt_failure(
        base_lba: u32,
        buf: &[u8],
        read_ms: u64,
        content: Option<&[(u32, u32)]>,
        keys: &DecryptKeys,
    ) {
        let unit_len = crate::aacs::content::ALIGNED_UNIT_LEN;
        let unit_sectors = (unit_len / 2048) as u32;
        // Only AACS produces decrypt-verify failures; None / CSS never reach here
        // with a non-zero dropped count.
        let (unit_keys, rdk) = match keys {
            DecryptKeys::Aacs {
                unit_keys,
                read_data_key,
            } => (unit_keys, *read_data_key),
            _ => return,
        };
        let mut emitted = 0usize;
        for (i, chunk) in buf.chunks_exact(unit_len).enumerate() {
            if emitted >= MAX_DIAG_UNITS_PER_READ {
                break;
            }
            let unit_lba = base_lba.saturating_add(i as u32 * unit_sectors);
            let in_content = match content {
                Some(r) => crate::decrypt::lba_in_ranges(unit_lba, r),
                None => true,
            };
            // A unit that decrypted is no longer sync-destroyed; a CPI-clear or
            // non-content unit is gated out. Only undecryptable in-content units
            // that are flagged encrypted carry signal.
            if !in_content || !crate::aacs::content::aacs_unit_needs_decrypt(chunk) {
                continue;
            }
            let all_zero = chunk.iter().all(|&b| b == 0);
            let ts_sync = crate::aacs::content::ts_sync_count(chunk);
            let ts_total = crate::aacs::content::ts_packet_total(chunk);
            let mut seen = [false; 256];
            for &b in chunk {
                seen[b as usize] = true;
            }
            let distinct = seen.iter().filter(|&&x| x).count();
            // Does ANY held key get this unit closer to clear TS?
            let mut best_sync = ts_sync;
            for (_, k) in unit_keys.iter() {
                let mut attempt = chunk.to_vec();
                if let Some(ref rdk_key) = rdk {
                    crate::aacs::content::decrypt_bus(&mut attempt, rdk_key);
                }
                crate::aacs::content::decrypt_unit(&mut attempt, k);
                let s = crate::aacs::content::ts_sync_count(&attempt);
                if s > best_sync {
                    best_sync = s;
                }
            }
            let head: String = chunk[..16].iter().map(|b| format!("{b:02x}")).collect();
            tracing::warn!(
                target: "freemkv::decrypt",
                lba = unit_lba,
                in_content,
                read_ms,
                all_zero,
                ts_sync,
                ts_total,
                distinct,
                best_sync,
                keys_held = unit_keys.len(),
                head,
                "decrypt-verify fail"
            );
            emitted += 1;
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
            && !crate::aacs::content::is_unit_aligned(lba, self.unit_base)
        {
            return Err(crate::error::Error::DecryptFailed);
        }
        let read_t0 = std::time::Instant::now();
        let n = self
            .inner
            .read_sectors_fua(lba, count, buf, recovery, fua)?;
        let read_ms = read_t0.elapsed().as_millis() as u64;
        // Decrypt the bytes just read. Scheme-agnostic: `decrypt_sectors*`
        // dispatches on the keys (None / CSS / AACS) and returns the count of
        // bytes that SHOULD have decrypted but couldn't — the silent-bad-read
        // signal. Only AACS ever produces a non-zero count, so nothing below
        // needs a per-scheme check. When a content map is installed (whole-disc
        // readers), the `*_in_content` entry skips units OUTSIDE the encrypted
        // content extents, so clear filesystem / nav bytes are never mistaken for
        // ciphertext. The mux installs no map (it reads title extents only).
        //
        // VERIFY-ONLY (multipass sweep): decrypt a reused SCRATCH copy so `buf`
        // keeps its ciphertext (the ISO stays encrypted) and the hot path pays no
        // per-read allocation. NORMAL: decrypt in place; a fetch callback may
        // recover a unit no held key opened.
        let content = self.content_ranges.clone(); // cheap Arc bump; frees the &self borrow
        let content_ref = content.as_deref();
        // Copy out the small Copy fields the seam needs, so the `&mut self.recovery`
        // borrow below does not collide with reads of other `self` fields. The
        // recovery closure self-limits (its budget lives in its captures), so the
        // decorator simply calls it whenever there is a miss.
        let unit_key_idx = self.unit_key_idx;

        // ── MUX: read > decrypt > mux ──────────────────────────────────────────
        // `tolerate_decrypt_loss` (and not verify_only) is the mux path: it is fed
        // already-captured data and reads only content extents. `decrypt_buf`
        // applies the CPS unit key to every encrypted unit IN PLACE; the resulting
        // bytes — clean MPEG-TS or a bad-encoded region — belong to the muxer, which
        // drops broken packets and resyncs. TS validity is NOT a decrypt verdict, so
        // the mux never conceals, never re-fetches a key, and never counts broken TS
        // as loss. Its ONLY failure is a genuine can't-decrypt (no key for a unit, or
        // a misaligned unit), which `decrypt_buf` surfaces as `Err` — propagate it
        // (fail loud), because a mux over captured data must otherwise always succeed.
        if self.tolerate_decrypt_loss && !self.verify_only {
            Self::decrypt_buf(
                &mut buf[..n],
                &mut self.keys,
                self.unit_key_idx,
                lba,
                content_ref,
            )?;
            return Ok(n);
        }

        // ── SWEEP / PATCH / VERIFY: decrypt to verify the disc read ─────────────
        // First decrypt, then the FRESH-KEY-ON-FAILURE retry (read → decrypt → on
        // fail fetch a new key → retry → CACHE or fail). This runs in BOTH modes:
        //   * VERIFY-ONLY (multipass sweep): decrypt a reused SCRATCH copy so `buf`
        //     keeps its ciphertext (the ISO stays encrypted), but STILL fetch —
        //     the whole point is to CACHE the key. The fetched key is added to the
        //     pool, so the unit that triggered it now verifies clean (no false
        //     read-failure / damage-jump) and every later unit this pass — and any
        //     later read on this decorator — reuses it instead of re-asking the key
        //     server. Without this a CPS unit whose key wasn't sampled up front
        //     (an orphan clip not reachable from any playlist) hard-fails the whole
        //     range even though one key fetch would recover it.
        //   * NORMAL (mux / --no-raw): decrypt `buf` in place, same retry.
        // The fetch re-decrypt targets the post-decrypt buffer (scratch / buf),
        // whose still-scrambled units ARE the failures.
        let outcome = if self.verify_only {
            let mut scratch = std::mem::take(&mut self.scratch);
            scratch.clear();
            scratch.extend_from_slice(&buf[..n]);
            let d = match Self::decrypt_buf(
                &mut scratch,
                &mut self.keys,
                self.unit_key_idx,
                lba,
                content_ref,
            ) {
                Ok(d) => d,
                Err(e) => {
                    self.scratch = scratch;
                    return Err(e);
                }
            };
            let o = match (d, self.recovery.as_mut()) {
                (0, _) | (_, None) => crate::sector::recovery::MissOutcome { dropped: d },
                (d, Some(r)) => {
                    let rctx = crate::sector::recovery::RecoverCtx {
                        unit_key_idx,
                        lba,
                        content: content.clone(),
                        prev_dropped: d,
                    };
                    // Target = the decrypted scratch; ciphertext = the untouched
                    // `buf` (verify-only keeps the ISO encrypted).
                    r(&mut scratch, &buf[..n], &mut self.keys, &rctx)
                }
            };
            self.scratch = scratch;
            o
        } else {
            // In-place decrypt (decrypt-sweep / decrypt-patch). Pure decrypt
            // overwrites `buf` with plaintext, so keep the on-disc ciphertext for
            // the recovery retry BEFORE decrypting — but only when recovery is
            // installed (the retry's sole consumer).
            let ciphertext: Option<Vec<u8>> = if self.recovery.is_some() {
                Some(buf[..n].to_vec())
            } else {
                None
            };
            let d = Self::decrypt_buf(
                &mut buf[..n],
                &mut self.keys,
                self.unit_key_idx,
                lba,
                content_ref,
            )?;
            match (d, self.recovery.as_mut()) {
                (0, _) | (_, None) => crate::sector::recovery::MissOutcome { dropped: d },
                (d, Some(r)) => {
                    let rctx = crate::sector::recovery::RecoverCtx {
                        unit_key_idx,
                        lba,
                        content: content.clone(),
                        prev_dropped: d,
                    };
                    let cipher = ciphertext
                        .as_deref()
                        .expect("recovery installed → captured");
                    r(&mut buf[..n], cipher, &mut self.keys, &rctx)
                }
            }
        };
        // A loss is a loss: whatever recovery could not decrypt (a missing unit
        // key, or an AACS 2.1 forensic-segment unit with no variant key — same
        // thing to the read path) is concealed and counted the same way.
        let dropped = outcome.dropped;
        if dropped > 0 {
            self.decrypt_dropped
                .fetch_add(dropped as u64, Ordering::Relaxed);
            // The mux path already returned above (read > decrypt > mux); only the
            // verify callers (sweep / patch / --no-raw verify) reach here. An
            // unverified unit means this disc read did NOT prove out.
            // DECRYPT_VERIFY_READ: a unit that SHOULD have decrypted but didn't
            // means this read did NOT truly succeed — it returned ciphertext the
            // TS assembler would silently drop. Fail the read loud so the caller's
            // read-error recovery re-reads it off the disc (rip) or the mux hard-
            // fails (no clean data to mux). Scheme-agnostic (only AACS reaches a
            // non-zero count); clear filesystem (gated out) and zero-fill (not
            // scrambled) never get here.
            //
            // An undecryptable unit is an undecryptable unit whatever the scheme —
            // a missing unit key or an AACS 2.1 forensic-segment unit with no
            // variant key both land here and fail the verify read the same way.
            // (`dropped > 0` already holds inside the enclosing block.)
            if DECRYPT_VERIFY_READ {
                // FACT-FINDING: on a fresh rip these bytes came straight off the
                // drive, so each failing unit's signature (all-zero? entropy?
                // does any held key get it closer to clear TS?) plus the inner
                // read latency are ground truth about the SOURCE — enough to
                // classify the failure as source-zeros, marginal-media garbage,
                // or a clean read no held key opens. In verify-only mode `buf` is
                // untouched ciphertext, so the post-decrypt `scratch` is what
                // distinguishes failed units (still TS-destroyed) from succeeded
                // ones (now clean TS).
                let diag: &[u8] = if self.verify_only {
                    &self.scratch
                } else {
                    &buf[..n]
                };
                Self::diagnose_decrypt_failure(
                    lba,
                    diag,
                    read_ms,
                    self.content_ranges.as_deref(),
                    &self.keys,
                );
                return Err(crate::error::Error::DecryptFailed);
            }
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
    /// stride (offset 4, then every 192 bytes). `ts_sync_destroyed`
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
    /// encrypt it under `unit_key` so `aacs::content::decrypt_unit` recovers it. Mirrors
    /// the encrypt helper in `crate::decrypt`'s tests.
    fn encrypt_aacs_unit(unit_key: &[u8; 16]) -> Vec<u8> {
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        let mut unit = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < unit.len() {
            unit[off] = 0x47;
            off += 192;
        }
        // CPI bits on byte 0 so it reads as encrypted; set before key derivation.
        unit[0] |= 0xC0;
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = crate::aacs::crypto::aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = crate::aacs::crypto::AACS_IV;
        let blocks = (crate::aacs::content::ALIGNED_UNIT_LEN - 16) / 16;
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

        // Wrong key → undecryptable → loss counted AND the read fails loud
        // (DECRYPT_VERIFY_READ: a read that returns an undecryptable AACS unit
        // did not truly succeed). The loss counter is still bumped before the
        // error so the abort accounting sees the byte count.
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
        let err = wrapped
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("DECRYPT_VERIFY_READ: an undecryptable AACS unit fails the read loud");
        assert!(
            matches!(err, crate::error::Error::DecryptFailed),
            "undecryptable unit errors with DecryptFailed, got {err:?}"
        );
        assert_eq!(
            loss.load(Ordering::Relaxed),
            crate::aacs::content::ALIGNED_UNIT_LEN as u64,
            "the undecryptable unit is tallied as loss before the read errors"
        );

        // A second read of the same bad unit accumulates further (and errors).
        assert!(
            wrapped.read_sectors(0, 3, &mut buf, false).is_err(),
            "the same bad unit fails the read again"
        );
        assert_eq!(
            loss.load(Ordering::Relaxed),
            2 * crate::aacs::content::ALIGNED_UNIT_LEN as u64,
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

    /// MUX (read > decrypt > mux): with `tolerate_decrypt_loss()` an undecryptable
    /// AACS content unit must NOT fail the read and must NOT be nulled. The best key
    /// is applied and the (bad) bytes pass through to the muxer; broken TS is a
    /// muxer concern, so it is NOT counted as decrypt loss. The mux only hard-fails
    /// on a genuine can't-decrypt (no key at all / misaligned unit).
    #[test]
    fn tolerate_decrypt_loss_passes_undecryptable_unit_through() {
        let real_key = [0x33u8; 16];
        let wrong_key = [0x44u8; 16];

        // One unit encrypted under real_key, plus one trailing CLEAR (TS-sync)
        // unit so we can confirm conceal touches ONLY the undecryptable unit.
        let enc = encrypt_aacs_unit(&real_key);
        let mut clear = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let mut o = 4;
        while o < clear.len() {
            clear[o] = 0x47;
            o += 192;
        }
        let mut two_units = enc;
        two_units.extend_from_slice(&clear);

        struct TwoUnitSource {
            data: Vec<u8>,
        }
        impl SectorSource for TwoUnitSource {
            fn capacity_sectors(&self) -> u32 {
                (self.data.len() / 2048) as u32
            }
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                buf[..bytes].copy_from_slice(&self.data[..bytes]);
                Ok(bytes)
            }
        }

        let mut wrapped = DecryptingSectorSource::new(
            TwoUnitSource { data: two_units },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)], // can't open the encrypted unit
                read_data_key: None,
            },
        )
        .tolerate_decrypt_loss();
        let loss = wrapped.decrypt_loss();

        let mut buf = vec![0u8; 6 * 2048];
        // Must SUCCEED (no DecryptFailed) — the mux never aborts on bad decrypt.
        let n = wrapped
            .read_sectors(0, 6, &mut buf, false)
            .expect("the mux never aborts on a bad-decrypt unit");
        assert_eq!(n, 6 * 2048);

        // Broken TS is a muxer concern, not decrypt loss — the mux counts none.
        assert_eq!(
            loss.load(Ordering::Relaxed),
            0,
            "the mux does not count broken TS as decrypt loss"
        );

        // Unit 0 is passed through DECRYPTED (the wrong key was applied), NOT
        // null-TS concealed: it is not the all-0x47/PID-0x1FFF null pattern.
        let unit0 = &buf[..crate::aacs::content::ALIGNED_UNIT_LEN];
        let all_null = (0..32).all(|p| unit0[p * 192 + 4] == 0x47 && unit0[p * 192 + 6] == 0xFF);
        assert!(
            !all_null,
            "the undecryptable unit is passed through, never null-TS concealed"
        );

        // Unit 1 (clear) passed through untouched.
        let unit1 = &buf
            [crate::aacs::content::ALIGNED_UNIT_LEN..2 * crate::aacs::content::ALIGNED_UNIT_LEN];
        assert_eq!(unit1, &clear[..], "the clear unit is left exactly as read");
    }

    /// MUX pass-through, mixed buffer: a unit the pool CAN decrypt (a
    /// content-fragment TAIL — a few real packets + source-zero padding, the 1.2.0
    /// shape, <16 TS syncs) comes out byte-for-byte correct, and a unit it CANNOT
    /// (encrypted under an absent key) is passed through best-effort — never
    /// null-TS filled, never counted as loss. The old path nulled the good tail
    /// (silent data loss) whenever it shared a buffer with an undecryptable unit.
    #[test]
    fn mux_passes_both_decryptable_and_undecryptable_units_through() {
        let bad_key = [0x77u8; 16]; // encrypts the undecryptable unit (NOT provided)
        let good_key = [0x33u8; 16]; // encrypts the padding-tail unit (provided)

        // Unit A: a full content unit encrypted under `bad_key` — with only
        // `good_key` in the pool it cannot be decrypted → restored to ciphertext.
        let bad_unit = encrypt_aacs_unit(&bad_key);

        // Unit B: a SHORT-PADDING-TAIL unit — encrypt a full clear unit under
        // `good_key`, then zero the trailing source packets (from packet 11 on) so
        // they decrypt back to clean zero padding. Only 11 of 32 packets are real
        // content → 11 TS syncs after decrypt (well under the majority-vote 16).
        const KEEP: usize = 11;
        let mut good_tail = encrypt_aacs_unit(&good_key);
        for b in good_tail[KEEP * 192..].iter_mut() {
            *b = 0;
        }

        // The byte-exact expected post-decrypt form of unit B (independent decrypt).
        let mut expected_tail = good_tail.clone();
        assert!(
            crate::aacs::content::decrypt_unit(&mut expected_tail, &good_key),
            "padding-tail must decrypt under good_key"
        );

        let mut two_units = bad_unit;
        two_units.extend_from_slice(&good_tail);

        struct TwoUnitSource {
            data: Vec<u8>,
        }
        impl SectorSource for TwoUnitSource {
            fn capacity_sectors(&self) -> u32 {
                (self.data.len() / 2048) as u32
            }
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                buf[..bytes].copy_from_slice(&self.data[..bytes]);
                Ok(bytes)
            }
        }

        let mut wrapped = DecryptingSectorSource::new(
            TwoUnitSource { data: two_units },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, good_key)], // opens unit B, NOT unit A
                read_data_key: None,
            },
        )
        .tolerate_decrypt_loss();
        let loss = wrapped.decrypt_loss();

        let mut buf = vec![0u8; 6 * 2048];
        let n = wrapped
            .read_sectors(0, 6, &mut buf, false)
            .expect("the mux never aborts on a bad-decrypt unit");
        assert_eq!(n, 6 * 2048);

        // Broken TS is a muxer concern — the mux counts no loss.
        assert_eq!(
            loss.load(Ordering::Relaxed),
            0,
            "the mux does not count broken TS as decrypt loss"
        );

        // Unit A (absent key) → passed through best-effort, NOT null-TS concealed.
        let unit0 = &buf[..crate::aacs::content::ALIGNED_UNIT_LEN];
        let all_null = (0..32).all(|p| unit0[p * 192 + 4] == 0x47 && unit0[p * 192 + 6] == 0xFF);
        assert!(
            !all_null,
            "the undecryptable unit is passed through, never null-TS concealed"
        );

        // Unit B → the GOOD decrypted padding tail, byte-for-byte intact.
        let unit1 = &buf
            [crate::aacs::content::ALIGNED_UNIT_LEN..2 * crate::aacs::content::ALIGNED_UNIT_LEN];
        assert_eq!(
            unit1,
            &expected_tail[..],
            "the decryptable padding-tail unit comes out byte-for-byte correct"
        );
        // Sanity: its real content packets carry their TS sync; its padding is zero.
        for p in 0..KEEP {
            assert_eq!(unit1[p * 192 + 4], 0x47, "content pkt {p} sync preserved");
        }
        for p in KEEP..32 {
            let o = p * 192;
            assert!(
                unit1[o..o + 192].iter().all(|&b| b == 0),
                "padding pkt {p} stayed zero (not NULL-TS-filled)"
            );
        }
    }

    /// `fill_null_ts_unit` round-trip: every BD source packet in the unit becomes
    /// a well-formed TS null packet (PID 0x1FFF, invisible to any real PID) that
    /// carries the B1 adaptation-field discontinuity_indicator — the marker
    /// `mux::ts` reads as a concealed gap.
    #[test]
    fn null_ts_fill_is_well_formed_and_invisible_to_real_pids() {
        let mut unit = vec![0xAAu8; crate::aacs::content::ALIGNED_UNIT_LEN];
        crate::aacs::content::fill_null_ts_unit(&mut unit);
        // 32 packets, each: sync 0x47, PID 0x1FFF, adaptation-only (0b10) with a
        // discontinuity_indicator in the adaptation field.
        let mut off = 0;
        let mut pkts = 0;
        while off + 192 <= unit.len() {
            assert_eq!(unit[off + 4], 0x47, "sync");
            let pid = ((unit[off + 5] as u16 & 0x1F) << 8) | unit[off + 6] as u16;
            assert_eq!(pid, 0x1FFF, "null PID");
            assert_eq!(
                (unit[off + 7] >> 4) & 0x03,
                0x02,
                "adaptation_field_control = AF only (no payload)"
            );
            assert!(unit[off + 8] > 0, "adaptation_field_length > 0");
            assert_eq!(
                unit[off + 9] & 0x80,
                0x80,
                "adaptation-field discontinuity_indicator set (the B1 marker)"
            );
            off += 192;
            pkts += 1;
        }
        assert_eq!(pkts, 32, "32 source packets per aligned unit");
    }

    /// Fresh-key-on-failure: a unit encrypted under a key NOT in the initial set
    /// would normally count as decrypt loss. With a [`with_key_fetch`] callback
    /// that returns that key, the decorator must hand the still-scrambled unit to
    /// the callback, add the returned key, re-decrypt, and register ZERO loss.
    /// Without the callback the same read accumulates loss (the baseline).
    ///
    /// Grounding: `read_sectors` invokes `fetch_failed_units` when
    /// `decrypt_sectors` leaves a scrambled unit and a callback is installed.
    #[test]
    fn key_fetch_recovers_unit_with_a_fresh_key() {
        let real_key = [0x5au8; 16]; // the key the unit is actually under
        let wrong_key = [0x11u8; 16]; // the only key we start with

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
                buf[..bytes].copy_from_slice(&self.unit);
                Ok(bytes)
            }
        }

        let unit = encrypt_aacs_unit(&real_key);

        // Capture what the callback was handed, and how many times it fired.
        let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_cb = Arc::clone(&seen);
        let fetch: super::KeyFetch = std::sync::Arc::new(move |samples: &[Vec<u8>]| {
            seen_cb.lock().unwrap().extend_from_slice(samples);
            vec![real_key]
        });

        let mut wrapped = DecryptingSectorSource::new(
            EncUnitSource { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)],
                read_data_key: None,
            },
        )
        .with_key_fetch(fetch);
        let loss = wrapped.decrypt_loss();

        let mut buf = vec![0u8; 3 * 2048];
        wrapped.read_sectors(0, 3, &mut buf, false).unwrap();

        assert_eq!(
            loss.load(Ordering::Relaxed),
            0,
            "fetch supplied the key → the unit decrypts → zero loss"
        );
        let got = seen.lock().unwrap();
        assert_eq!(
            got.len(),
            1,
            "callback must be invoked once with the failing unit"
        );
        assert!(
            crate::aacs::content::ts_sync_destroyed(&got[0]),
            "the sample handed to the callback is the still-scrambled ciphertext"
        );
        assert_eq!(
            got[0], unit,
            "the exact on-disc unit is forwarded for fetch"
        );

        // Baseline: same setup WITHOUT a callback accumulates loss.
        let mut nocb = DecryptingSectorSource::new(
            EncUnitSource { unit },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)],
                read_data_key: None,
            },
        );
        let nocb_loss = nocb.decrypt_loss();
        let mut buf2 = vec![0u8; 3 * 2048];
        assert!(
            nocb.read_sectors(0, 3, &mut buf2, false).is_err(),
            "without a fetch callback the undecryptable unit fails the read (DECRYPT_VERIFY_READ)"
        );
        assert_eq!(
            nocb_loss.load(Ordering::Relaxed),
            crate::aacs::content::ALIGNED_UNIT_LEN as u64,
            "without a fetch callback the undecryptable unit is loss"
        );
    }

    /// A fetch that comes back EMPTY for one unit must NOT block a later fetch
    /// for a DIFFERENT unit (the multi-CPS case). The old global `fetch_spent`
    /// latch wrongly blocked it; the per-sample `fetch_dry` set must let unit B
    /// be asked for after unit A came back dry.
    #[test]
    fn fetch_dry_does_not_block_a_distinct_later_unit() {
        let key_a = [0x5au8; 16];
        let key_b = [0x77u8; 16];
        let unit_a = encrypt_aacs_unit(&key_a);
        let unit_b = encrypt_aacs_unit(&key_b);
        assert_ne!(unit_a, unit_b, "distinct ciphertext under distinct keys");

        struct AltSource {
            units: Vec<Vec<u8>>,
            idx: usize,
        }
        impl SectorSource for AltSource {
            fn capacity_sectors(&self) -> u32 {
                6
            }
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _r: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                let u = &self.units[self.idx.min(self.units.len() - 1)];
                buf[..bytes].copy_from_slice(u);
                self.idx += 1;
                Ok(bytes)
            }
        }

        // Callback serves key_b only when asked about unit B; nothing for A.
        let unit_b_cb = unit_b.clone();
        let calls = Arc::new(Mutex::new(0usize));
        let calls_cb = Arc::clone(&calls);
        let fetch: super::KeyFetch = std::sync::Arc::new(move |samples: &[Vec<u8>]| {
            *calls_cb.lock().unwrap() += 1;
            if samples.iter().any(|s| *s == unit_b_cb) {
                vec![key_b]
            } else {
                vec![]
            }
        });

        let mut wrapped = DecryptingSectorSource::new(
            AltSource {
                units: vec![unit_a, unit_b],
                idx: 0,
            },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, [0x11u8; 16])], // neither real key held up front
                read_data_key: None,
            },
        )
        .with_key_fetch(fetch);

        // Read A: fetch fires, returns nothing → A undecryptable (read errors).
        let mut buf = vec![0u8; 3 * 2048];
        let _ = wrapped.read_sectors(0, 3, &mut buf, false);
        // Read B: fetch must STILL fire (B's sample isn't in the dry set) and
        // recover key_b → B decrypts cleanly.
        let mut buf2 = vec![0u8; 3 * 2048];
        wrapped
            .read_sectors(3, 3, &mut buf2, false)
            .expect("unit B recovers via its own fetch");

        assert_eq!(
            *calls.lock().unwrap(),
            2,
            "fetch fired for BOTH units — the dry result for A did not latch off B"
        );
        assert!(
            !crate::aacs::content::ts_sync_destroyed(&buf2),
            "unit B is decrypted after its on-demand fetch"
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

    /// Verify-only mode (the multipass sweep/patch path): a read decrypt-CHECKS
    /// the bytes but NEVER mutates `buf`, so the ISO keeps its ciphertext. An
    /// undecryptable unit still fails the read (DECRYPT_VERIFY_READ) so the
    /// existing read-error recovery treats it like a SCSI failure; a decryptable
    /// unit returns Ok with the ciphertext intact (the check is non-destructive).
    #[test]
    fn verify_only_checks_without_mutating_and_fails_on_undecryptable() {
        let real_key = [0x33u8; 16];
        let wrong_key = [0x44u8; 16];

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
                buf[..bytes].copy_from_slice(&self.unit);
                Ok(bytes)
            }
        }

        let unit = encrypt_aacs_unit(&real_key);

        // Wrong key → undecryptable → read FAILS, but buf is untouched ciphertext.
        let mut bad = DecryptingSectorSource::new(
            EncUnitSource { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)],
                read_data_key: None,
            },
        )
        .verify_only();
        let mut buf = vec![0u8; 3 * 2048];
        let err = bad
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("verify-only: an undecryptable unit must fail the read");
        assert!(matches!(err, crate::error::Error::DecryptFailed));
        assert_eq!(
            buf, unit,
            "verify-only must NOT mutate buf — ISO stays ciphertext"
        );

        // Right key → read OK, and buf is STILL the original ciphertext (the
        // decrypt happened on a scratch copy, not in place).
        let mut good = DecryptingSectorSource::new(
            EncUnitSource { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, real_key)],
                read_data_key: None,
            },
        )
        .verify_only();
        let mut buf2 = vec![0u8; 3 * 2048];
        good.read_sectors(0, 3, &mut buf2, false)
            .expect("verify-only: a decryptable unit reads OK");
        assert_eq!(
            buf2, unit,
            "verify-only leaves ciphertext in buf even when the unit decrypts"
        );
    }

    /// THE first-2 GB regression at the READ level. With a content map installed,
    /// a verify-only read of a scrambled-LOOKING but CLEAR region (UDF filesystem
    /// OUTSIDE the content extents) must read OK — not false-fail — while a read
    /// INSIDE content that won't decrypt still fails. Before the content gate, the
    /// filesystem read was mis-classified as undecryptable ciphertext and the
    /// whole opening of every disc was marked NonTrimmed.
    #[test]
    fn verify_only_content_gate_passes_clear_filesystem_fails_content() {
        // Source returns sync-destroyed bytes (looks like ciphertext) for any LBA.
        struct ScrambledSource;
        impl SectorSource for ScrambledSource {
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                for (i, b) in buf[..bytes].iter_mut().enumerate() {
                    *b = (i as u8).wrapping_mul(31);
                }
                let mut off = 4;
                while off < bytes {
                    buf[off] = 0xA5; // force a NON-sync byte at every TS probe stride
                    off += 192;
                }
                // CPI bits on each aligned unit's byte 0 so it reads as encrypted.
                let mut u = 0;
                while u < bytes {
                    buf[u] |= 0xC0;
                    u += crate::aacs::content::ALIGNED_UNIT_LEN;
                }
                Ok(bytes)
            }
        }

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        // Content lives at LBA 1002..1101 (3-aligned start so reads pass the
        // unit-alignment gate). Everything before it is "filesystem".
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(1002u32, 99u32)]);
        let mut dec = DecryptingSectorSource::new(ScrambledSource, keys)
            .verify_only()
            .with_content_ranges(ranges);
        let mut buf = vec![0u8; 3 * 2048];

        // LBA 0 — OUTSIDE content (filesystem). Scrambled-looking, but clear by
        // position → must read OK (the regression that broke the first 2 GB).
        dec.read_sectors(0, 3, &mut buf, false)
            .expect("a clear filesystem region must read OK — no false decrypt-fail");

        // LBA 1002 — INSIDE content, undecryptable → the read must fail loud.
        let err = dec
            .read_sectors(1002, 3, &mut buf, false)
            .expect_err("an undecryptable content unit must fail the read");
        assert!(matches!(err, crate::error::Error::DecryptFailed));
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

    /// verify-only + content map: an in-content unit that DOES decrypt reads OK,
    /// and `buf` keeps its CIPHERTEXT (the verify is non-mutating).
    #[test]
    fn verify_only_content_gate_decryptable_unit_keeps_ciphertext() {
        let key = [0x5a; 16];
        let unit = encrypt_aacs_unit(&key);
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(0u32, 3u32)]); // LBA 0..3 is content
        let mut dec = DecryptingSectorSource::new(
            FixedUnit { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, key)],
                read_data_key: None,
            },
        )
        .verify_only()
        .with_content_ranges(ranges);
        let mut buf = vec![0u8; 3 * 2048];
        dec.read_sectors(0, 3, &mut buf, false)
            .expect("a decryptable content unit reads OK");
        assert_eq!(
            buf, unit,
            "verify-only keeps ciphertext even when the unit decrypts"
        );
    }

    /// NO content map (None) ⇒ ungated legacy behaviour: a scrambled-looking read
    /// fails. This is what the mux relies on (it only reads content), and the very
    /// reason the whole-disc sweep MUST install the map.
    #[test]
    fn verify_only_without_content_map_is_ungated() {
        struct ScrambledSource;
        impl SectorSource for ScrambledSource {
            fn read_sectors(
                &mut self,
                _lba: u32,
                count: u16,
                buf: &mut [u8],
                _r: bool,
            ) -> Result<usize> {
                let b = count as usize * 2048;
                for (i, x) in buf[..b].iter_mut().enumerate() {
                    *x = (i as u8).wrapping_mul(31);
                }
                let mut o = 4;
                while o < b {
                    buf[o] = 0xA5;
                    o += 192;
                }
                let mut u = 0;
                while u < b {
                    buf[u] |= 0xC0; // CPI bits → reads as encrypted
                    u += crate::aacs::content::ALIGNED_UNIT_LEN;
                }
                Ok(b)
            }
        }
        let mut dec = DecryptingSectorSource::new(
            ScrambledSource,
            DecryptKeys::Aacs {
                unit_keys: vec![(0, [0xAB; 16])],
                read_data_key: None,
            },
        )
        .verify_only(); // no content map installed
        let mut buf = vec![0u8; 3 * 2048];
        let err = dec
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("ungated verify fails on scrambled bytes (legacy / mux behaviour)");
        assert!(matches!(err, crate::error::Error::DecryptFailed));
    }

    /// In-place decrypt + content map: a NON-content read passes through unchanged
    /// (ciphertext, not decrypted); an in-content read is decrypted IN PLACE.
    #[test]
    fn inplace_decrypt_content_gate_passes_clear_decrypts_content() {
        let key = [0x5a; 16];
        let cipher_unit = encrypt_aacs_unit(&key);
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(1002u32, 99u32)]); // content @ 1002..
        let mut dec = DecryptingSectorSource::new(
            FixedUnit {
                unit: cipher_unit.clone(),
            },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, key)],
                read_data_key: None,
            },
        )
        .with_content_ranges(ranges); // in-place (NOT verify_only)

        // Non-content read (LBA 0): not decrypted → buf stays ciphertext.
        let mut buf = vec![0u8; 3 * 2048];
        dec.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(
            buf, cipher_unit,
            "a non-content read is passed through, not decrypted"
        );

        // In-content read (LBA 1002): decrypted in place → TS sync restored.
        let mut buf2 = vec![0u8; 3 * 2048];
        dec.read_sectors(1002, 3, &mut buf2, false).unwrap();
        assert_ne!(
            buf2, cipher_unit,
            "an in-content read is decrypted in place"
        );
        assert_eq!(buf2[4], 0x47, "decrypted content carries the TS sync byte");
    }

    /// A source that returns a fixed encrypted unit for ANY read — used to drive
    /// the verify-only fetch + cache tests below.
    struct AnyLbaUnit {
        unit: Vec<u8>,
    }
    impl SectorSource for AnyLbaUnit {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _r: bool,
        ) -> Result<usize> {
            let b = count as usize * 2048;
            buf[..b].copy_from_slice(&self.unit);
            Ok(b)
        }
    }

    /// THE cps2 fix at the read level. Verify-only (sweep) mode now fetches: a
    /// content unit no HELD key opens hands its ciphertext to the fetch closure,
    /// the returned key is added to the pool (the CACHE), the unit re-verifies
    /// clean — and `buf` is left as ciphertext (the ISO stays encrypted). Then the
    /// cached key serves the NEXT unit WITHOUT another callback (≈one fetch per CPS
    /// unit). This is what stops an orphan CPS unit from hard-failing the sweep.
    #[test]
    fn verify_only_fetch_recovers_caches_and_keeps_ciphertext() {
        let real_key = [0x5au8; 16]; // the key the unit is actually under
        let wrong_key = [0x11u8; 16]; // the only key we start with
        let unit = encrypt_aacs_unit(&real_key);

        let calls = Arc::new(Mutex::new(0usize));
        let calls_cb = Arc::clone(&calls);
        let fetch: super::KeyFetch = std::sync::Arc::new(move |samples: &[Vec<u8>]| {
            *calls_cb.lock().unwrap() += 1;
            // The closure is handed the still-scrambled on-disc ciphertext.
            assert!(!samples.is_empty(), "fetch receives the failing units");
            vec![real_key]
        });

        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(0u32, 6u32)]); // LBA 0..6 content
        let mut dec = DecryptingSectorSource::new(
            AnyLbaUnit { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong_key)],
                read_data_key: None,
            },
        )
        .verify_only()
        .with_content_ranges(ranges)
        .with_key_fetch(fetch);

        // First read (LBA 0): wrong key fails → fetch supplies real_key → Ok,
        // and buf is still ciphertext (verify-only never mutates the ISO bytes).
        let mut buf = vec![0u8; 3 * 2048];
        dec.read_sectors(0, 3, &mut buf, false)
            .expect("fetch recovers the orphan unit's key");
        assert_eq!(buf, unit, "verify-only keeps ciphertext even after a fetch");
        assert_eq!(*calls.lock().unwrap(), 1, "fetch called exactly once");

        // Second read (LBA 3): real_key now CACHED → decrypts with no new callback.
        let mut buf2 = vec![0u8; 3 * 2048];
        dec.read_sectors(3, 3, &mut buf2, false)
            .expect("cached key serves the next unit");
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "cache hit — the fetch callback must NOT fire again"
        );
    }

    /// Verify-only fetch that comes back empty (the key source can't help) must
    /// still hard-fail the read (DECRYPT_VERIFY_READ) — recovery, not silent loss.
    #[test]
    fn verify_only_fetch_exhausted_still_hard_fails() {
        let real_key = [0x5au8; 16];
        let wrong = [0x11u8; 16];
        let unit = encrypt_aacs_unit(&real_key);
        let fetch: super::KeyFetch = std::sync::Arc::new(|_: &[Vec<u8>]| Vec::new());
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(0u32, 3u32)]);
        let mut dec = DecryptingSectorSource::new(
            FixedUnit { unit: unit.clone() },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong)],
                read_data_key: None,
            },
        )
        .verify_only()
        .with_content_ranges(ranges)
        .with_key_fetch(fetch);
        let mut buf = vec![0u8; 3 * 2048];
        let err = dec
            .read_sectors(0, 3, &mut buf, false)
            .expect_err("a fetch that returns no key must still fail the read");
        assert!(matches!(err, crate::error::Error::DecryptFailed));
    }

    /// The fetch is content-gated: a scrambled unit OUTSIDE the content extents
    /// is clear filesystem, not ciphertext, so the read succeeds and the fetch
    /// callback is never consulted (no wasted key-server traffic on nav/UDF).
    #[test]
    fn verify_only_fetch_not_called_outside_content() {
        let real_key = [0x5au8; 16];
        let wrong = [0x11u8; 16];
        let unit = encrypt_aacs_unit(&real_key);
        let calls = Arc::new(Mutex::new(0usize));
        let calls_cb = Arc::clone(&calls);
        let fetch: super::KeyFetch = std::sync::Arc::new(move |_: &[Vec<u8>]| {
            *calls_cb.lock().unwrap() += 1;
            vec![real_key]
        });
        // Content lives far away; LBA 0 is "filesystem".
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(1002u32, 99u32)]);
        let mut dec = DecryptingSectorSource::new(
            AnyLbaUnit { unit },
            DecryptKeys::Aacs {
                unit_keys: vec![(0, wrong)],
                read_data_key: None,
            },
        )
        .verify_only()
        .with_content_ranges(ranges)
        .with_key_fetch(fetch);
        let mut buf = vec![0u8; 3 * 2048];
        dec.read_sectors(0, 3, &mut buf, false)
            .expect("non-content scrambled-looking bytes read OK (gated out)");
        assert_eq!(
            *calls.lock().unwrap(),
            0,
            "fetch must NOT fire for a non-content unit"
        );
    }
}

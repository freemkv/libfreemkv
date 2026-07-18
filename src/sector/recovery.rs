//! The recovery seam: what a read does when a content unit will not decrypt.
//!
//! Per-format miss policy does NOT belong in the generic decrypt decorator
//! (L2). The input stream (L3, e.g. [`crate::mux::disc::DiscStream`]) knows what
//! it is reading and installs a [`Recover`] at construction; the decorator
//! executes it at the one seam and honours the returned outcome. This keeps
//! "a DVD re-cracks, a BD/UHD fetches a fresh key" out of the decryptor, where
//! it would otherwise smear across `if`-branches.
//!
//! The recovery type ([`Recover`]) names **no encryption scheme**. It is a
//! generic `FnMut(&mut [u8], &mut DecryptKeys, &RecoverCtx) -> MissOutcome` that
//! operates on the generic [`DecryptKeys`] the whole decrypt path already uses,
//! so a scheme is never baked into the type — only into the factory that builds
//! a recovery:
//!   * [`none`] — no recovery; a miss is loss (raw sweep / clear).
//!   * [`key_fetch`] — AACS key-fetch: hand the failing ciphertext to the
//!     application's key source and add any returned keys to the pool. An AACS
//!     2.1 forensic-segment unit that no key opens is just an undecryptable unit
//!     like any other — a loss is a loss, with no FMTS-specific branch here.
//!
//! CSS is deliberately NOT on this seam — and the reason is precise: this seam is
//! for recovery that needs something `decrypt_sectors` does not have (an EXTERNAL
//! key source for AACS, a segment map for FMTS). CSS's title key changes per VOB
//! region and is re-cracked constantly, but always FROM THE DATA ITSELF — no
//! external input — so CSS SELF-recovers inside `decrypt_sectors` (see
//! [`crate::css::descramble_region`]). The generic type here would accept a CSS
//! recovery, but CSS has no reason to use it.

use crate::decrypt::DecryptKeys;
use crate::sector::KeyFetch;
use std::collections::HashSet;
use std::sync::Arc;

/// The result of running a recovery on a read's still-scrambled units: how many
/// bytes remain loss after recovery ran. A loss is a loss — an undecryptable
/// unit is concealed and counted the same whatever the scheme (an AACS 2.1
/// forensic-segment unit with no variant key is just another undecryptable
/// unit).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MissOutcome {
    /// Bytes that remain loss after recovery.
    pub dropped: usize,
}

impl MissOutcome {
    /// All `n` bytes are loss.
    fn loss(n: usize) -> Self {
        Self { dropped: n }
    }
}

/// Cap on how many times one recovery will call its fetch closure over its
/// lifetime — bounds key-server traffic to ~O(distinct CPS units) even if
/// scrambled units keep arriving. A disc has only a handful of unit keys.
const MAX_FETCH_CALLS: usize = 16;

/// Cap on how many still-scrambled sample units are handed to the fetch closure
/// per call — a few samples suffice for a key service to identify and validate
/// the key, and it bounds the request size.
const MAX_FETCH_SAMPLES: usize = 8;

/// Stable per-run fingerprint of a failing unit's ciphertext, for the dedup set.
/// `DefaultHasher` is fixed-seed, so equal samples map to equal fingerprints
/// within a process — all the dedup needs.
fn sample_fp(sample: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sample.hash(&mut h);
    h.finish()
}

/// Re-decrypt `buf` after the key pool grew, content-gated identically to the
/// first read so a non-content unit is never re-attempted. Mirrors the
/// decorator's `decrypt_buf` dispatch.
fn redecrypt(
    buf: &mut [u8],
    keys: &mut DecryptKeys,
    unit_key_idx: usize,
    lba: u32,
    content: Option<&[(u32, u32)]>,
    prev_dropped: usize,
) -> usize {
    match content {
        Some(ranges) => {
            crate::decrypt::decrypt_sectors_in_content(buf, keys, unit_key_idx, lba, ranges)
        }
        None => crate::decrypt::decrypt_sectors(buf, keys, unit_key_idx),
    }
    .unwrap_or(prev_dropped)
}

/// What a read hands a recovery on a miss: the disc's decrypt parameters and how
/// many bytes the held keys could not decrypt. Scheme-neutral — a recovery reads
/// only the generic [`DecryptKeys`] and these fields.
pub struct RecoverCtx {
    /// Which AACS unit-key index the read decrypts with (ignored by non-AACS).
    pub unit_key_idx: usize,
    /// Base LBA of the read.
    pub lba: u32,
    /// The encrypted-content extent map, when the read is content-gated.
    pub content: Option<Arc<[(u32, u32)]>>,
    /// Bytes the held keys could not decrypt before recovery ran.
    pub prev_dropped: usize,
}

/// A recovery: given a read's post-decrypt `target` (pure decrypt leaves the
/// applied-key plaintext), the matching on-disc `ciphertext`, and the **generic**
/// [`DecryptKeys`], make units decrypt (fetch a key into `keys` and retry) and/or
/// classify the loss (see [`MissOutcome`]). Decryption itself lives in ONE place
/// (`decrypt_sectors`); a recovery only supplies the missing KEY and re-runs it.
/// `ciphertext` is separate from `target` because a pure decrypt overwrites the
/// target with plaintext — the key server still needs the original on-disc bytes,
/// and the retry re-decrypts from them. The type names NO encryption scheme; any
/// scheme is just a different [`Recover`] the input stream installs. `FnMut` so
/// per-recovery state (dedup set / call budget) lives in the closure's captures;
/// `Send` so it can ride the mux highway's producer thread.
pub type Recover =
    Box<dyn FnMut(&mut [u8], &[u8], &mut DecryptKeys, &RecoverCtx) -> MissOutcome + Send>;

/// The AACS key-fetch step used by [`key_fetch`]: gather the units the pool did
/// NOT open, ask `fetch` for keys, add any new ones to the pool and re-decrypt.
/// `dry` / `calls` are the caller-owned dedup set and call budget. Returns the
/// post-retry unverified-byte count.
fn aacs_fetch_step(
    dry: &mut HashSet<u64>,
    calls: &mut usize,
    fetch: &KeyFetch,
    target: &mut [u8],
    ciphertext: &[u8],
    keys: &mut DecryptKeys,
    ctx: &RecoverCtx,
) -> usize {
    let prev_dropped = ctx.prev_dropped;
    if *calls >= MAX_FETCH_CALLS {
        return prev_dropped;
    }
    let unit_len = crate::aacs::content::ALIGNED_UNIT_LEN;
    // Container of this disc's content — travels with the keys; drives the
    // encrypted-flag / structure check below (TS vs PS).
    let format = match &*keys {
        DecryptKeys::Aacs { format, .. } => *format,
        _ => crate::disc::ContentFormat::BdTs,
    };
    // Gather up to MAX_FETCH_SAMPLES units the current pool did NOT open. Detect
    // them on the post-decrypt TARGET (a failed unit stays TS-destroyed; an opened
    // one is now clean TS and is skipped), but SAMPLE the matching on-disc
    // `ciphertext` — the exact bytes the key server needs. A trailing partial unit
    // (chunks_exact remainder) can't be a whole scrambled unit, so skipping it is
    // correct.
    let mut samples: Vec<Vec<u8>> = Vec::new();
    for (t, c) in target
        .chunks_exact(unit_len)
        .zip(ciphertext.chunks_exact(unit_len))
    {
        if crate::aacs::content::aacs_unit_needs_decrypt(t, format) {
            samples.push(c.to_vec());
            if samples.len() >= MAX_FETCH_SAMPLES {
                break;
            }
        }
    }
    if samples.is_empty() {
        return prev_dropped;
    }
    // Skip the call when EVERY failing unit here is one a prior fetch already
    // came back empty for — re-asking identical ciphertext only burns a request.
    // A unit not asked about yet (e.g. a second CPS unit) still gets its chance.
    let fps: Vec<u64> = samples.iter().map(|s| sample_fp(s)).collect();
    if fps.iter().all(|fp| dry.contains(fp)) {
        return prev_dropped;
    }
    *calls += 1;
    let fresh = fetch.unit_keys(&samples);
    // Add only keys we don't already hold (dedup by value).
    let mut added = 0usize;
    if let DecryptKeys::Aacs { unit_keys, .. } = keys {
        for k in fresh {
            if !unit_keys.iter().any(|(_, have)| *have == k) {
                let idx = unit_keys.len() as u32;
                unit_keys.push((idx, k));
                added += 1;
            }
        }
    }
    if added == 0 {
        // Nothing new for THESE units — remember them so we don't re-ask the same
        // ciphertext, but leave the door open for other units.
        dry.extend(fps);
        return prev_dropped;
    }
    // Retry now that the pool has grown. Reset the target to the on-disc
    // ciphertext first (a pure decrypt already overwrote it with the failed
    // plaintext), then re-run the ONE decrypt. A unit that still won't reach clean
    // TS stays unverified; a retry error must not mask the original count.
    target.copy_from_slice(ciphertext);
    redecrypt(
        target,
        keys,
        ctx.unit_key_idx,
        ctx.lba,
        ctx.content.as_deref(),
        prev_dropped,
    )
}

/// No recovery: a miss is loss. Equivalent to installing nothing — provided so a
/// caller that wants an explicit "give up" recovery has one.
pub fn none() -> Recover {
    Box::new(|_target, _ciphertext, _keys, ctx| MissOutcome::loss(ctx.prev_dropped))
}

/// AACS key-fetch recovery (BD / UHD): on a miss, ask the application's key
/// source for a key that opens the failing ciphertext and add it to the pool.
pub fn key_fetch(fetch: KeyFetch) -> Recover {
    let mut dry: HashSet<u64> = HashSet::new();
    let mut calls: usize = 0;
    Box::new(move |target, ciphertext, keys, ctx| {
        MissOutcome::loss(aacs_fetch_step(
            &mut dry, &mut calls, &fetch, target, ciphertext, keys, ctx,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::content::ALIGNED_UNIT_LEN;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A 6144-byte aligned unit that reads as still-scrambled: CPI bits set on
    /// byte 0 (so `aacs_unit_encrypted` flags it) and every 192-byte TS-sync
    /// probe position forced off 0x47. `tag` varies the whole body so distinct
    /// tags produce distinct fingerprints (mirrors decrypt.rs `scrambled_region`).
    fn scrambled_unit(tag: u8) -> Vec<u8> {
        let len = ALIGNED_UNIT_LEN;
        let mut v: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31) ^ tag).collect();
        let mut off = 4;
        while off < len {
            v[off] = 0xA5; // never a 0x47 sync
            off += 192;
        }
        v[0] |= 0xC0; // CPI: reads as encrypted content
        v
    }

    /// A recovery context reading at clip-relative `lba` with `prev` bytes the
    /// held keys could not decrypt.
    fn ctx(lba: u32, prev: usize) -> RecoverCtx {
        RecoverCtx {
            unit_key_idx: 0,
            lba,
            content: None,
            prev_dropped: prev,
        }
    }

    #[test]
    fn none_recovers_nothing() {
        let mut r = none();
        let mut buf = scrambled_unit(0x33);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let cipher = buf.clone();
        let out = r(&mut buf, &cipher, &mut keys, &ctx(0, 6144));
        assert_eq!(out.dropped, 6144);
    }

    #[test]
    fn key_fetch_adds_returned_keys_to_the_pool() {
        // The fetch returns one key; it must be appended to the (empty) pool. We
        // assert the pool grew (the decrypt itself is exercised end-to-end by the
        // decorator's integration tests); here we pin the seam's key-plumbing.
        let calls = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&calls);
        let fetch: KeyFetch = KeyFetch::unit_only(Arc::new(move |samples: &[Vec<u8>]| {
            c2.fetch_add(1, Ordering::SeqCst);
            assert!(!samples.is_empty(), "failing ciphertext is forwarded");
            vec![[0xAB; 16]]
        }));
        let mut r = key_fetch(fetch);
        let mut buf = scrambled_unit(0x33);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let cipher = buf.clone();
        r(&mut buf, &cipher, &mut keys, &ctx(0, ALIGNED_UNIT_LEN));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "fetch called once");
        let DecryptKeys::Aacs { unit_keys, .. } = &keys else {
            unreachable!()
        };
        assert_eq!(unit_keys.len(), 1, "returned key added to the pool");
        assert_eq!(unit_keys[0].1, [0xAB; 16]);
    }

    #[test]
    fn key_fetch_does_not_re_ask_dry_ciphertext() {
        // A fetch that returns nothing marks the ciphertext dry; a second miss on
        // the SAME ciphertext must not call the fetch again.
        let calls = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&calls);
        let fetch: KeyFetch = KeyFetch::unit_only(Arc::new(move |_: &[Vec<u8>]| {
            c2.fetch_add(1, Ordering::SeqCst);
            Vec::new() // never helps
        }));
        let mut r = key_fetch(fetch);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        let mut buf = scrambled_unit(0x44);
        let cipher = buf.clone();
        r(&mut buf, &cipher, &mut keys, &ctx(0, ALIGNED_UNIT_LEN));
        let mut buf2 = scrambled_unit(0x44); // identical ciphertext
        let cipher2 = buf2.clone();
        r(&mut buf2, &cipher2, &mut keys, &ctx(0, ALIGNED_UNIT_LEN));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "identical dry ciphertext is not re-asked"
        );
    }

    #[test]
    fn key_fetch_call_budget_bounds_fetches() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&calls);
        let fetch: KeyFetch = KeyFetch::unit_only(Arc::new(move |_: &[Vec<u8>]| {
            c2.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }));
        let mut r = key_fetch(fetch);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
            format: crate::disc::ContentFormat::BdTs,
        };
        // Distinct ciphertext each time so the dry-set never short-circuits; only
        // the internal call budget should stop the fetch. The closure self-limits,
        // so the decorator can call it unconditionally.
        for i in 0..(MAX_FETCH_CALLS as u8 + 5) {
            let mut buf = scrambled_unit(i);
            let cipher = buf.clone();
            r(&mut buf, &cipher, &mut keys, &ctx(0, ALIGNED_UNIT_LEN));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_FETCH_CALLS,
            "fetch is capped at MAX_FETCH_CALLS"
        );
    }
}

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
    if let Ok(mut guard) = DECRYPT_POOL.write() {
        *guard = None;
    }
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
}

/// Resolved decryption state from disc scanning.
/// Passed to `decrypt_sectors()` — the caller doesn't need to know
/// which encryption scheme is in use.
#[derive(Clone)]
pub enum DecryptKeys {
    /// No encryption on this disc.
    None,
    /// AACS (Blu-ray / UHD). Unit keys + optional read data key.
    Aacs {
        unit_keys: Vec<(u32, [u8; 16])>,
        read_data_key: Option<[u8; 16]>,
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

/// Decrypt a buffer of sectors in-place.
///
/// For AACS: processes in 6144-byte aligned units (3 sectors).
/// For CSS: processes per 2048-byte sector.
/// For None: no-op.
///
/// `unit_key_idx` is the initial AACS unit-key hint (0 for most discs). On a
/// multi-CPS-unit disc every key is tried per unit until the TS-sync verify
/// passes; `unit_key_idx` is tried first so single-CPS-unit discs pay zero
/// overhead. An out-of-range `unit_key_idx` is always an error.
///
/// Returns `Err` if decryption was expected but keys are missing or invalid.
/// Never produces silently corrupted output.
///
/// On success returns the number of bytes belonging to scrambled AACS units
/// that **no available key could decrypt** — those units are restored to their
/// original encrypted bytes (so a clear nav-file is never corrupted), but for
/// genuine encrypted content this is silent data loss the downstream TS
/// assembler will drop without a sync. The decrypt-on-read decorator folds this
/// count into the mux loss accounting so a partial key failure can't be reported
/// as a perfect rip. `0` for `None` / `Css` and for any AACS buffer where every
/// scrambled unit decrypted.
pub fn decrypt_sectors(
    buf: &mut [u8],
    keys: &DecryptKeys,
    unit_key_idx: usize,
) -> Result<usize, crate::error::Error> {
    let dropped: usize = match keys {
        DecryptKeys::None => 0,
        DecryptKeys::Aacs {
            unit_keys,
            read_data_key,
        } => {
            // Validate that unit_key_idx is in-range before doing anything else.
            // This preserves the existing contract: an out-of-range explicit index
            // is always an error (tested by `aacs_out_of_range_unit_key_idx_errors`).
            if unit_keys.get(unit_key_idx).is_none() {
                return Err(crate::error::Error::DecryptFailed);
            }

            // Strip CPS-unit IDs — the decrypt primitives only want the raw key bytes.
            let raw_keys: Vec<[u8; 16]> = unit_keys.iter().map(|(_, k)| *k).collect();
            let rdk: Option<[u8; 16]> = *read_data_key;
            let unit_len = aacs::ALIGNED_UNIT_LEN;
            // AACS decrypts whole 6144-byte aligned units. The live mux path
            // (mux/disc.rs::fill_extents) issues 1- or 2-sector reads at every
            // extent tail, so a buffer is commonly NOT a multiple of the unit
            // length. We process the whole leading units exactly as a fully
            // aligned buffer would be, then make a deliberate decision about any
            // trailing partial unit.
            //
            // Trailing-partial contract:
            //   * A clear partial (incomplete final unit / clear nav-TS tail) is
            //     what AACS legitimately leaves in the clear on disc, so we leave
            //     it untouched and return Ok. This is the proven, shipped
            //     behavior every production UHD MKV was made with — no regression
            //     on conformant discs.
            //   * A *scrambled* partial can only arise from a structurally
            //     malformed UDF layout that splits an encrypted unit across an
            //     extent boundary. Those bytes are encrypted content that cannot
            //     be decrypted standalone; passing them through as clear would be
            //     silent corruption. We fail loud (Error::DecryptFailed), matching
            //     the highway path's Error::ExtentNotUnitAligned policy.
            //
            // Detection: is_aacs_scrambled() short-circuits to false for any
            // buffer shorter than a full unit, so it cannot judge a partial. We
            // instead apply the same TS-sync-intactness test it uses internally
            // (ts_sync_count vs ts_packet_total) directly to the available
            // partial bytes. A clear TS tail carries 0x47 syncs at the 192-byte
            // stride (> half the packets) → intact → not scrambled → tolerate. An
            // encrypted tail has those syncs destroyed (≤ half) → scrambled →
            // reject. If the partial is too short to hold even one TS packet
            // (< 192 bytes, ts_packet_total == 0) we cannot judge confidently and
            // tolerate rather than risk a false positive on conformant tails.
            let partial_len = buf.len() % unit_len;
            if partial_len != 0 {
                let partial = &buf[buf.len() - partial_len..];
                let packets = aacs::ts_packet_total(partial);
                if packets > 0 && aacs::ts_sync_count(partial) <= packets / 2 {
                    return Err(crate::error::Error::DecryptFailed);
                }
            }
            let nthreads = decrypt_threads();
            let nunits = buf.len() / unit_len;

            // Cache the last successfully-validated key index so that runs of
            // units under the same CPS unit hit on the first try. Initialised to
            // unit_key_idx (the caller's hint — 0 for almost all discs). An
            // AtomicUsize lets the parallel path share it cheaply; relaxed
            // ordering is fine because a stale read just causes one extra try,
            // never a wrong result (TS-sync verify gates correctness).
            let last_key_idx = AtomicUsize::new(unit_key_idx);

            // Count bytes of scrambled units that NO key could decrypt. Shared
            // across the rayon workers (relaxed is fine — it's a pure tally, not
            // a synchronisation point). A non-zero total is silent decrypt loss:
            // the bytes pass downstream still encrypted and the TS assembler
            // drops them without a sync. The caller folds this into mux loss
            // accounting so a partial key failure isn't reported as a clean rip.
            let dropped_bytes = AtomicUsize::new(0);

            // Per-unit decrypt closure. For a scrambled full aligned unit:
            //   1. Try the cached key index first (avoids scanning all keys on the
            //      common case where a disc run uses one CPS unit throughout).
            //   2. On miss, try every key in order (multi-CPS-unit discs).
            //   3. Accept the first key whose output passes the TS-sync verify.
            //   4. Only restore-to-original if NO key validates (non-m2ts unit or
            //      genuine decrypt failure). See test
            //      `nav_file_unit_survives_decrypt_attempt`.
            //
            // If a read_data_key is present (AACS 2.0 bus encryption), bus-decrypt
            // must happen first — it's a shared layer on top that is key-independent
            // across all CPS units on the disc.
            let decrypt_one = |chunk: &mut [u8]| {
                if chunk.len() != unit_len || !aacs::is_aacs_scrambled(chunk) {
                    return;
                }
                // Save original bytes so we can restore if no key validates.
                let original: Vec<u8> = chunk.to_vec();

                // Build a bus-decrypted copy to try unit keys against, or work
                // in-place when there is no bus layer.
                if let Some(ref rdk_key) = rdk {
                    aacs::decrypt_bus(chunk, rdk_key);
                }

                // Reorder the key iterator: try the cached hint first, then fall
                // back to the full list skipping the hint.
                let hint = last_key_idx.load(Ordering::Relaxed);
                let try_order =
                    std::iter::once(hint).chain((0..raw_keys.len()).filter(move |&i| i != hint));

                for idx in try_order {
                    if let Some(key) = raw_keys.get(idx) {
                        // Work on a per-key copy so a failing attempt doesn't
                        // clobber the bus-decrypted base we'll retry on.
                        let mut attempt: Vec<u8> = chunk.to_vec();
                        if aacs::decrypt_unit(&mut attempt, key) {
                            chunk.copy_from_slice(&attempt);
                            last_key_idx.store(idx, Ordering::Relaxed);
                            return;
                        }
                    }
                }

                // No key validated — restore the original encrypted bytes and
                // tally the loss. The unit was scrambled (we only reach here past
                // the `is_aacs_scrambled` gate) but no key applied: a clear
                // nav-file unit that legitimately fails the cipher, or genuine
                // encrypted content with a missing/wrong sub-key. We can't tell
                // them apart here, so we always tally; the mux read path treats
                // the count as loss (its extents are real content), while
                // metadata-probe callers that don't install a loss sink ignore it.
                chunk.copy_from_slice(&original);
                dropped_bytes.fetch_add(chunk.len(), Ordering::Relaxed);
            };

            if nthreads <= 1 || nunits < PARALLEL_MIN_UNITS {
                // Serial path: avoids thread-pool overhead for tiny
                // buffers; also the only path when caller pinned
                // single-threaded via FREEMKV_THREADS=1. Iterate the
                // chunks directly — no Vec of slice pointers needed.
                for chunk in buf.chunks_mut(unit_len) {
                    decrypt_one(chunk);
                }
            } else {
                // Parallel path via rayon's persistent thread pool.
                // The pool is built once on first use and reused across
                // every decrypt_sectors call — no per-call OS thread
                // spawn. Each unit decrypts independently (own key
                // derivation), so par_iter is sound. On a pool-build
                // failure (e.g. thread/pid-limit exhaustion) we fall
                // back to the serial path rather than panic.
                match decrypt_pool() {
                    Some(pool) => {
                        let chunks: Vec<&mut [u8]> = buf.chunks_mut(unit_len).collect();
                        pool.install(|| {
                            chunks.into_par_iter().for_each(|chunk| {
                                decrypt_one(chunk);
                            });
                        });
                    }
                    None => {
                        for chunk in buf.chunks_mut(unit_len) {
                            decrypt_one(chunk);
                        }
                    }
                }
            }
            dropped_bytes.into_inner()
        }
        DecryptKeys::Css { title_key } => {
            for chunk in buf.chunks_mut(2048) {
                css::lfsr::descramble_sector(title_key, chunk);
            }
            0
        }
    };
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression for the 0.18.1 nav-file scramble bug. A non-m2ts unit (here
    /// an MPLS file: starts "MPLS", carries no TS syncs) reads as scrambled
    /// under `is_aacs_scrambled`, gets AES-decrypted with the unit key, fails
    /// the TS-sync verification, and must be restored to its original bytes —
    /// not left scrambled.
    #[test]
    fn nav_file_unit_survives_decrypt_attempt() {
        let mut unit = vec![0u8; aacs::ALIGNED_UNIT_LEN];
        unit[0] = b'M';
        unit[1] = b'P';
        unit[2] = b'L';
        unit[3] = b'S';
        for (i, b) in unit.iter_mut().enumerate().skip(4) {
            *b = (i as u8).wrapping_mul(31);
        }
        let snapshot = unit.clone();

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        decrypt_sectors(&mut unit, &keys, 0).unwrap();
        assert_eq!(
            unit, snapshot,
            "non-m2ts unit must be restored after failed decrypt"
        );
    }

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
        v
    }

    /// Whole leading units plus a CLEAR trailing partial (the benign,
    /// conformant case): AACS leaves an incomplete final unit / clear nav-TS
    /// tail in the clear on disc. We must return `Ok` and leave the partial
    /// bytes byte-for-byte unchanged — no regression on real discs.
    #[test]
    fn aacs_clear_trailing_partial_is_tolerated_unchanged() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        // One full scrambled unit + a 2048-byte (single-sector) CLEAR tail.
        let unit = scrambled_region(aacs::ALIGNED_UNIT_LEN);
        let tail = clear_ts_region(2048);
        let mut buf = unit;
        buf.extend_from_slice(&tail);

        decrypt_sectors(&mut buf, &keys, 0).expect("clear trailing partial is Ok");

        assert_eq!(
            &buf[aacs::ALIGNED_UNIT_LEN..],
            &tail[..],
            "clear trailing partial unit must be left unchanged"
        );
    }

    /// Whole leading units plus a SCRAMBLED trailing partial (the malformed
    /// danger case): an encrypted unit split across an extent boundary cannot be
    /// decrypted standalone. Passing it through as clear would be silent
    /// corruption, so we must fail loud with `DecryptFailed`.
    #[test]
    fn aacs_scrambled_trailing_partial_is_rejected() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        // One full unit + a 4096-byte (two-sector) SCRAMBLED tail.
        let unit = clear_ts_region(aacs::ALIGNED_UNIT_LEN);
        let tail = scrambled_region(4096);
        let mut buf = unit;
        buf.extend_from_slice(&tail);

        let err = decrypt_sectors(&mut buf, &keys, 0)
            .expect_err("scrambled trailing partial must be rejected");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "scrambled trailing partial must fail with DecryptFailed"
        );
    }

    /// An empty buffer is a valid no-op (zero units), not an error.
    #[test]
    fn aacs_empty_buffer_is_ok() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        let mut buf: Vec<u8> = Vec::new();
        assert!(decrypt_sectors(&mut buf, &keys, 0).is_ok());
    }

    /// An exact multiple of the unit length has no trailing partial: behavior
    /// is unchanged — clear units stay clear, scrambled units are decrypt-
    /// attempted. Two clear units must round-trip untouched and return `Ok`.
    #[test]
    fn aacs_exact_multiple_unchanged() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        let mut buf = clear_ts_region(aacs::ALIGNED_UNIT_LEN * 2);
        let snapshot = buf.clone();

        decrypt_sectors(&mut buf, &keys, 0).expect("exact-multiple buffer is Ok");

        assert_eq!(
            buf, snapshot,
            "clear exact-multiple buffer must be left unchanged"
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
        decrypt_sectors(&mut buf, &DecryptKeys::None, 0).expect("None is always Ok");
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
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0xDE, 0xAD, 0xBE, 0xEF, 0x42];
        let (mut sector, plaintext) = make_css_sector(&title_key, &seed, 0xA5);
        let keys = DecryptKeys::Css { title_key };
        decrypt_sectors(&mut sector, &keys, 0).expect("CSS decrypt is Ok");
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
        let keys = DecryptKeys::Css { title_key };
        decrypt_sectors(&mut buf, &keys, 0).expect("CSS multi-sector decrypt is Ok");
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
        let keys = DecryptKeys::Css { title_key };
        decrypt_sectors(&mut sector, &keys, 0).unwrap();
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
        let keys = DecryptKeys::Css { title_key: [0; 5] };
        assert!(decrypt_sectors(&mut buf, &keys, 0).is_ok());
    }

    // ── AACS unit-key index selection ──────────────────────────────────────

    /// AACS decrypt with an out-of-range unit_key_idx must fail loud with
    /// DecryptFailed — never silently fall back to a wrong key or pass
    /// encrypted data through as clear.
    ///
    /// Grounding: `let uk = match unit_keys.get(unit_key_idx) { Some => ...,
    /// None => return Err(DecryptFailed) }`.
    /// Mutation: change `unit_keys.get(unit_key_idx)` to `unit_keys.get(0)` or
    /// `.unwrap_or` a default -> the out-of-range index would not error; this
    /// fails.
    #[test]
    fn aacs_out_of_range_unit_key_idx_errors() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        let mut buf = clear_ts_region(aacs::ALIGNED_UNIT_LEN);
        let err = decrypt_sectors(&mut buf, &keys, 5)
            .expect_err("unit_key_idx 5 is out of range for a 1-key list");
        assert_eq!(
            err.code(),
            crate::error::Error::DecryptFailed.code(),
            "out-of-range unit key index must be DecryptFailed"
        );
    }

    /// AACS with an empty unit_keys list and any index errors (no key to use).
    ///
    /// Grounding: `unit_keys.get(0)` on an empty Vec is None -> DecryptFailed.
    /// Mutation: defaulting to [0u8;16] on None would proceed; this fails.
    #[test]
    fn aacs_empty_unit_keys_errors() {
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![],
            read_data_key: None,
        };
        let mut buf = clear_ts_region(aacs::ALIGNED_UNIT_LEN);
        let err = decrypt_sectors(&mut buf, &keys, 0).expect_err("empty unit_keys must error");
        assert_eq!(err.code(), crate::error::Error::DecryptFailed.code());
    }

    // ── Multi-CPS-unit key selection ──────────────────────────────────────

    /// Encrypt an aligned unit with the AACS algorithm run in reverse so that
    /// `aacs::decrypt_unit` with the same key recovers the plaintext. Mirrors
    /// the `aacs_encrypt_unit` helper in `aacs::decrypt::tests`.
    fn aacs_encrypt_unit_for_test(unit: &mut [u8], unit_key: &[u8; 16]) {
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = crate::aacs::decrypt::aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = crate::aacs::decrypt::AACS_IV;
        let num_blocks = (aacs::ALIGNED_UNIT_LEN - 16) / 16;
        for i in 0..num_blocks {
            let off = 16 + i * 16;
            for j in 0..16 {
                unit[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&unit[off..off + 16]);
            cipher.encrypt_block(&mut block);
            unit[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&unit[off..off + 16]);
        }
    }

    /// Build a clear aligned unit with TS sync bytes placed at the BD-TS stride
    /// (offset 4 + k*192) so `is_aacs_scrambled` reports false and
    /// `decrypt_unit` verifies it as clear after decryption.
    fn clear_ts_unit() -> Vec<u8> {
        let mut unit = vec![0u8; aacs::ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < aacs::ALIGNED_UNIT_LEN {
            unit[off] = 0x47;
            off += 192;
        }
        unit
    }

    /// A unit encrypted under unit_keys[1] (the second CPS unit) on a
    /// two-key disc must be correctly decrypted — not left as garbage —
    /// when `decrypt_sectors` is called with unit_key_idx=0 (the default).
    ///
    /// Before the fix, `decrypt_one` used only `unit_keys[unit_key_idx]`
    /// (i.e. always key 0). On a multi-CPS-unit disc this produced silent
    /// garbage for content under key ≥ 1. The fix tries every key and
    /// accepts the one whose output passes the TS-sync verify.
    ///
    /// Grounding: `for idx in try_order { … if aacs::decrypt_unit(&mut attempt, key) { … } }`
    /// Mutation: revert to the pre-fix `decrypt_unit_full(chunk, &uk, …)` where
    /// `uk = raw_keys[unit_key_idx]` (always key 0) → the unit comes out as
    /// garbled bytes that still look scrambled, failing the `!is_aacs_scrambled`
    /// assert.
    #[test]
    fn aacs_multi_cps_unit_disc_decrypts_under_non_zero_key() {
        let key0 = [0x11u8; 16]; // CPS unit 0 key — NOT the correct key for this unit
        let key1 = [0x22u8; 16]; // CPS unit 1 key — the correct key

        // Build and encrypt a clear unit under key1 (the non-default CPS unit).
        let mut unit = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit, &key1);
        assert!(
            aacs::is_aacs_scrambled(&unit),
            "encrypted unit must look scrambled before decrypt"
        );

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key0), (1, key1)], // two CPS units
            read_data_key: None,
        };

        // Call with the default hint (idx 0) — the fix must fall back to key1.
        let mut buf = unit;
        decrypt_sectors(&mut buf, &keys, 0).expect("multi-CPS decrypt must succeed");

        assert!(
            !aacs::is_aacs_scrambled(&buf),
            "unit encrypted under key1 must be fully decrypted (TS syncs restored)"
        );
        // Every sync position must carry 0x47.
        assert_eq!(
            aacs::ts_sync_count(&buf),
            aacs::ts_packet_total(&buf),
            "all TS sync bytes must be restored after decrypting under key1"
        );
    }

    /// Single-key disc: the common case is unaffected — the single key is
    /// tried first (via the hint) and validates, so no second-pass overhead.
    ///
    /// Grounding: the `hint = last_key_idx.load(…)` path returns on the first
    /// `try_order` iteration. A regression that always tried all keys (instead
    /// of accepting the first hit) would still pass this test — correctness is
    /// the invariant here, not the performance shortcut.
    #[test]
    fn aacs_single_key_disc_still_decrypts_correctly() {
        let key = [0x55u8; 16];
        let mut unit = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit, &key);

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
        };
        let mut buf = unit;
        decrypt_sectors(&mut buf, &keys, 0).expect("single-key disc must decrypt");
        assert!(
            !aacs::is_aacs_scrambled(&buf),
            "single-key disc: TS syncs must be restored"
        );
        assert_eq!(
            aacs::ts_sync_count(&buf),
            aacs::ts_packet_total(&buf),
            "all TS sync bytes must be restored for single-key disc"
        );
    }

    /// Regression for the silent partial-decrypt-loss defect: a scrambled AACS
    /// unit that NO supplied key can decrypt is restored to its original
    /// ciphertext (so a clear nav-file is never corrupted) AND `decrypt_sectors`
    /// returns the unit's byte length as the dropped count. Before the fix this
    /// returned `()` and the still-encrypted bytes flowed downstream to be
    /// silently dropped by the TS assembler with zero loss accounting — a rip
    /// missing real content reported `lost_video_secs=0` and passed the abort
    /// gate even under `abort_on_lost_secs=0`.
    ///
    /// Grounding: the `dropped_bytes.fetch_add(chunk.len(), …)` on the
    /// no-key-validated restore path; the function returns that tally.
    /// Mutation: drop the `fetch_add` (or return a constant 0) → dropped == 0,
    /// this fails.
    #[test]
    fn aacs_undecryptable_unit_reports_dropped_bytes() {
        let real_key = [0x33u8; 16];
        let wrong_key = [0x44u8; 16]; // not the encrypting key

        // Encrypt a clear unit under real_key, then offer ONLY the wrong key.
        let mut unit = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit, &real_key);
        let ciphertext = unit.clone();
        assert!(
            aacs::is_aacs_scrambled(&unit),
            "encrypted unit must look scrambled going in"
        );

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, wrong_key)],
            read_data_key: None,
        };
        let mut buf = unit;
        let dropped =
            decrypt_sectors(&mut buf, &keys, 0).expect("undecryptable unit is not a hard error");

        assert_eq!(
            dropped,
            aacs::ALIGNED_UNIT_LEN,
            "the whole scrambled unit must be reported as dropped when no key validates"
        );
        assert_eq!(
            buf, ciphertext,
            "an undecryptable unit must be restored to its original ciphertext, not garbled"
        );
    }

    /// The dropped-byte tally accumulates across a multi-unit buffer where some
    /// units decrypt and others don't: a 2-unit buffer with one good and one
    /// bad unit reports exactly one unit's worth of loss, and the good unit is
    /// fully decrypted. Confirms the count is per-unit, not all-or-nothing.
    ///
    /// Grounding: the per-chunk `decrypt_one` closure tallies only the units
    /// that fail; the good unit takes the `return` before the tally.
    #[test]
    fn aacs_mixed_buffer_tallies_only_failed_units() {
        let key = [0x55u8; 16];
        let wrong = [0x66u8; 16];

        // Unit A: encrypted under `key` (decryptable). Unit B: encrypted under
        // `wrong` (NOT in the key list → undecryptable).
        let mut unit_a = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit_a, &key);
        let mut unit_b = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit_b, &wrong);
        let unit_b_ciphertext = unit_b.clone();

        let mut buf = Vec::with_capacity(2 * aacs::ALIGNED_UNIT_LEN);
        buf.extend_from_slice(&unit_a);
        buf.extend_from_slice(&unit_b);

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
        };
        let dropped = decrypt_sectors(&mut buf, &keys, 0).expect("partial decrypt is Ok");

        assert_eq!(
            dropped,
            aacs::ALIGNED_UNIT_LEN,
            "exactly one unit's worth of bytes must be reported dropped"
        );
        assert!(
            !aacs::is_aacs_scrambled(&buf[..aacs::ALIGNED_UNIT_LEN]),
            "the decryptable unit must come out clear"
        );
        assert_eq!(
            &buf[aacs::ALIGNED_UNIT_LEN..],
            &unit_b_ciphertext[..],
            "the undecryptable unit must be restored to ciphertext"
        );
    }

    /// A fully-decryptable single-key buffer reports zero dropped bytes — the
    /// loss tally must not fire on the clean path.
    #[test]
    fn aacs_all_units_decrypt_reports_zero_dropped() {
        let key = [0x77u8; 16];
        let mut unit = clear_ts_unit();
        aacs_encrypt_unit_for_test(&mut unit, &key);
        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key)],
            read_data_key: None,
        };
        let mut buf = unit;
        let dropped = decrypt_sectors(&mut buf, &keys, 0).expect("clean decrypt");
        assert_eq!(dropped, 0, "a fully-decrypted buffer must report no loss");
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

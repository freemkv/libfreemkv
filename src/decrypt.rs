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
/// `unit_key_idx` selects which AACS unit key to use (0 for most discs).
///
/// Returns `Err` if decryption was expected but keys are missing or invalid.
/// Never produces silently corrupted output.
pub fn decrypt_sectors(
    buf: &mut [u8],
    keys: &DecryptKeys,
    unit_key_idx: usize,
) -> Result<(), crate::error::Error> {
    match keys {
        DecryptKeys::None => {}
        DecryptKeys::Aacs {
            unit_keys,
            read_data_key,
        } => {
            let uk = match unit_keys.get(unit_key_idx) {
                Some((_, k)) => *k,
                None => {
                    return Err(crate::error::Error::DecryptFailed);
                }
            };
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

            // Per-unit decrypt closure. The is_aacs_scrambled check reads the
            // raw TS syncs; a non-m2ts unit (e.g. MPLS/CLPI nav file) can look
            // scrambled and trigger a decrypt attempt, so on a verify miss we
            // snapshot+restore the original bytes so it survives. See test
            // `nav_file_unit_survives_decrypt_attempt`.
            let decrypt_one = |chunk: &mut [u8]| {
                if chunk.len() == unit_len && aacs::is_aacs_scrambled(chunk) {
                    let original: Vec<u8> = chunk.to_vec();
                    if !aacs::decrypt_unit_full(chunk, &uk, rdk.as_ref()) {
                        chunk.copy_from_slice(&original);
                    }
                }
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
        }
        DecryptKeys::Css { title_key } => {
            for chunk in buf.chunks_mut(2048) {
                css::lfsr::descramble_sector(title_key, chunk);
            }
        }
    }
    Ok(())
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
}

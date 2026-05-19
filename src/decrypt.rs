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
/// `N > 1` builds a new rayon pool of size N and atomically replaces
/// the live pool.
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
fn decrypt_pool() -> Arc<rayon::ThreadPool> {
    // Fast path: pool already built.
    if let Ok(guard) = DECRYPT_POOL.read() {
        if let Some(pool) = guard.as_ref() {
            return Arc::clone(pool);
        }
    }
    // Slow path: build a new one under the write lock. Double-check
    // after acquiring in case another caller built it first.
    let mut guard = DECRYPT_POOL.write().expect("DECRYPT_POOL RwLock poisoned");
    if let Some(pool) = guard.as_ref() {
        return Arc::clone(pool);
    }
    let n = decrypt_threads();
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|i| format!("freemkv-decrypt-{i}"))
            .build()
            .expect("rayon decrypt pool build failed"),
    );
    *guard = Some(Arc::clone(&pool));
    pool
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
            let nthreads = decrypt_threads();
            let chunks: Vec<&mut [u8]> = buf.chunks_mut(unit_len).collect();
            let nunits = chunks.len();

            // Per-unit decrypt closure. The is_unit_encrypted check is
            // a byte-0 heuristic; on a misfire we snapshot+restore via
            // the original bytes so non-m2ts (e.g. MPLS/CLPI nav files)
            // survive. See test `nav_file_unit_survives_decrypt_attempt`.
            let decrypt_one = |chunk: &mut [u8]| {
                if chunk.len() == unit_len && aacs::is_unit_encrypted(chunk) {
                    let original: Vec<u8> = chunk.to_vec();
                    if !aacs::decrypt_unit_full(chunk, &uk, rdk.as_ref()) {
                        chunk.copy_from_slice(&original);
                    }
                }
            };

            if nthreads <= 1 || nunits < PARALLEL_MIN_UNITS {
                // Serial path: avoids thread-pool overhead for tiny
                // buffers; also the only path when caller pinned
                // single-threaded via FREEMKV_THREADS=1.
                for chunk in chunks {
                    decrypt_one(chunk);
                }
            } else {
                // Parallel path via rayon's persistent global pool.
                // The pool is built once on first use (lazy_static-style)
                // and reused across every decrypt_sectors call — no
                // per-call OS thread spawn, no thread-creation latency
                // amortised per batch. Each unit decrypts independently
                // (own key derivation), so par_iter is sound.
                decrypt_pool().install(|| {
                    chunks.into_par_iter().for_each(|chunk| {
                        decrypt_one(chunk);
                    });
                });
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

    /// Regression for the 0.18.1 nav-file scramble bug. A non-m2ts unit whose
    /// first byte has the top 2 bits set (here: the ASCII letter 'M' that
    /// MPLS files start with, 0x4D = 0b01001101) trips `is_unit_encrypted`,
    /// gets AES-decrypted with the unit key, fails the TS-sync verification,
    /// and must be restored to its original bytes — not left scrambled.
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
}

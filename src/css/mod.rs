//! CSS (Content Scramble System) — DVD disc encryption.
//!
//! CSS uses a weak 40-bit LFSR stream cipher (broken since 1999).
//!
//! The title key is recovered keylessly: [`crack_key`] runs the Stevenson
//! known-plaintext attack (see the [`stevenson`] module) on the scrambled
//! data, needing no player keys, disc-key crack, or external key file.
//! Sectors are then decrypted with [`descramble_sector`].
//!
//! Usage:
//! ```rust,ignore
//! if let Some(state) = css::crack_key(reader, extents, batch) {
//!     css::descramble_sector(&state, &mut sector);
//! }
//! ```

pub mod auth;
pub mod lfsr;
pub mod stevenson;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::sector::SectorSource;

/// CSS decryption state for a DVD title.
#[derive(Debug, Clone)]
pub struct CssState {
    /// 5-byte CSS title key (from SCSI auth or the crack fallback).
    pub title_key: [u8; 5],
    /// LBA half-open span `[start, end)` of the extent set this key was
    /// cracked from. CSS title keys are per-VTS: a key cracked from one
    /// VTS does NOT descramble a title living in a different VTS. The mux
    /// path checks whether the title being opened overlaps this span; if
    /// not, it re-cracks from that title's own extents. `None` for keys
    /// of unknown provenance (e.g. test fixtures) — treated as "applies
    /// everywhere" for backward compatibility.
    pub crack_span: Option<(u32, u32)>,
}

/// Recover the CSS title key with no keys, by scanning scrambled sectors and
/// running the Stevenson known-plaintext attack (see the [`stevenson`] module).
///
/// The crib comes from `AttackPattern`: a scrambled sector's cleartext region
/// (bytes 0x00..0x80) often ends in a short-period repeating run (stuffing /
/// constant fill); the attack assumes that run continues across the 0x80
/// boundary into the encrypted region, giving the known plaintext the 2^16
/// LFSR recovery needs. We scan up to 50000 scrambled sectors across the
/// extents and return the first sector that yields a key — no player keys, no
/// disc-key crack. Works on a live drive (after bus-auth unlocks reads) and on
/// disc images alike.
pub fn crack_key(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
) -> Option<CssState> {
    crack_key_halt(reader, extents, batch_sectors, None)
}

/// [`crack_key`] with an optional cooperative-cancellation token.
///
/// "No silent hangs": the crack scans up to 50_000 sectors, which on a live
/// drive hitting bad sectors can take a long time. This variant polls `halt`
/// once per batch (the same cadence sweep/patch use) so an operator Stop or a
/// scan-level watchdog can interrupt the scan, and emits a
/// `freemkv::heartbeat` beat ("css_crack") each batch so a stuck scan is
/// visible in the log.
pub fn crack_key_halt(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> Option<CssState> {
    // Batch the reads: a live optical drive at 1 sector/read is glacial, and the
    // crack only needs to FIND one scrambled sector whose 0x80 plaintext matches
    // a known PES header. `batch_sectors` MUST be sized to the source — a drive
    // rejects a READ(10) larger than its per-command max (DVD = 16) and
    // `Drive::read` does not chunk, so an over-large batch fails every read and
    // scans nothing. Callers pass `detect_max_batch_sectors(device_path)` for a
    // live drive, a file-safe value for an image, or 1 to force per-sector.
    let batch = (batch_sectors.max(1)) as u32;
    // Record the LBA span the key is being cracked from so the per-title mux
    // path can tell whether a later title lives in the same VTS (overlaps the
    // span → key applies) or a different one (→ re-crack). Half-open [min,max).
    let crack_span = extents
        .iter()
        .filter(|e| e.sector_count > 0)
        .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        .reduce(|(amin, amax), (bmin, bmax)| (amin.min(bmin), amax.max(bmax)));
    let mut tried = 0u32;
    let max_tries = 50_000u32;
    let mut buf = vec![0u8; batch as usize * 2048];
    let mut hb = crate::progress::Heartbeat::new("css_crack");

    'outer: for (extent_idx, ext) in extents.iter().enumerate() {
        let mut i = 0u32;
        while i < ext.sector_count && tried < max_tries {
            // Cooperative cancellation — poll once per batch, the same cadence
            // sweep/patch use, so a Stop / watchdog can interrupt the scan.
            if let Some(h) = halt {
                if h.is_cancelled() {
                    break 'outer;
                }
            }
            // Liveness beacon: a long scan over a damaged disc stays visible.
            // The heartbeat is time-throttled; only when it actually beats do
            // we emit the crack-specific context (tried/lba/extent_idx).
            if hb.tick(tried as u64, max_tries as u64) {
                tracing::debug!(
                    target: "freemkv::heartbeat",
                    phase = "css_crack",
                    tried,
                    lba = ext.start_lba + i,
                    extent_idx,
                    "scanning"
                );
            }
            let n = (ext.sector_count - i).min(batch);
            let want = n as usize * 2048;
            match reader.read_sectors(ext.start_lba + i, n as u16, &mut buf[..want], true) {
                Ok(_) => {
                    for s in 0..n as usize {
                        tried += 1;
                        let sect = &buf[s * 2048..(s + 1) * 2048];
                        if is_scrambled(sect) {
                            if let Some(key) = stevenson::crack_title_key(sect) {
                                return Some(CssState {
                                    title_key: key,
                                    crack_span,
                                });
                            }
                        }
                        if tried >= max_tries {
                            break 'outer;
                        }
                    }
                }
                // A failed batch (bad sectors) still counts toward the budget so a
                // damaged region can't loop forever; skip ahead by the batch.
                Err(_) => tried += n,
            }
            i += n;
        }
    }

    None
}

/// Descramble a single CSS-encrypted sector in place.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    lfsr::descramble_sector(&state.title_key, sector);
}

/// Check if a sector has the CSS scramble flag set.
pub fn is_scrambled(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};

    // ── is_scrambled ───────────────────────────────────────────────────────

    /// is_scrambled returns false for any buffer shorter than one sector,
    /// WITHOUT indexing byte 0x14 (which would panic on a tiny buffer). The
    /// length guard is short-circuited before the flag read.
    ///
    /// Grounding: `sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0` —
    /// `&&` short-circuits so a 20-byte buffer never reads index 0x14.
    /// Mutation: swap the operands so the flag is read first
    /// (`(sector[0x14]...) && sector.len() >= 2048`) -> panics indexing a
    /// 20-byte slice; this test catches it.
    #[test]
    fn is_scrambled_short_buffer_is_false_no_panic() {
        assert!(!is_scrambled(&[]));
        assert!(!is_scrambled(&[0u8; 20])); // shorter than 0x14+1 even
        assert!(!is_scrambled(&[0xFFu8; 2047])); // one byte short of a sector
    }

    /// is_scrambled keys on bits 4-5 of byte 0x14 (the CSS scramble field).
    /// A full sector flagged 0x10/0x20/0x30 is scrambled; 0x00 and the
    /// high-bit-only values 0x40/0x80 are clear.
    ///
    /// Grounding: `(sector[0x14] >> 4) & 0x03`.
    /// Mutation: widen mask to `& 0x0F` -> 0x40 reports scrambled, the 0x40
    /// assert fails.
    #[test]
    fn is_scrambled_uses_bits_4_5_only() {
        let mut s = vec![0u8; 2048];
        for (flag, expected) in [
            (0x00u8, false),
            (0x10, true),
            (0x20, true),
            (0x30, true),
            (0x40, false),
            (0x80, false),
            (0xC0, false),
            (0xFF, true), // bits 4-5 set within 0xFF
        ] {
            s[0x14] = flag;
            assert_eq!(
                is_scrambled(&s),
                expected,
                "flag byte {flag:#04x} scramble detection"
            );
        }
    }

    /// is_scrambled accepts exactly 2048 bytes as the minimum (boundary at the
    /// inclusive value 2048).
    ///
    /// Grounding: `sector.len() >= 2048`.
    /// Mutation: change `>= 2048` to `> 2048` -> an exact 2048-byte scrambled
    /// sector reports false; this fails.
    #[test]
    fn is_scrambled_exact_sector_length_accepted() {
        let mut s = vec![0u8; 2048];
        s[0x14] = 0x30;
        assert!(is_scrambled(&s), "exactly 2048 bytes must be eligible");
    }

    // ── crack_key scanning over a mock SectorSource ────────────────────────

    /// Records every (lba, count) read; returns a caller-supplied flag byte at
    /// 0x14 so we can drive scrambled/clear sectors, or an injected error.
    struct MockSource {
        reads: std::cell::RefCell<Vec<u32>>,
        flag_byte: u8,
        fail_all: bool,
    }

    impl MockSource {
        fn new(flag_byte: u8) -> Self {
            Self {
                reads: std::cell::RefCell::new(Vec::new()),
                flag_byte,
                fail_all: false,
            }
        }
    }

    impl SectorSource for MockSource {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.reads.borrow_mut().push(lba);
            if self.fail_all {
                return Err(Error::DecryptFailed);
            }
            let n = count as usize * 2048;
            let end = n.min(buf.len());
            for b in buf[..end].iter_mut() {
                *b = 0;
            }
            if buf.len() > 0x14 {
                buf[0x14] = self.flag_byte;
            }
            Ok(n)
        }
    }

    /// crack_key caps total scanned sectors at 50_000 even when extents are
    /// far larger, and counts EVERY scanned sector (clear ones included)
    /// toward the budget. With one 200_000-sector extent of clear sectors, it
    /// must read exactly 50_000 sectors and return None — never run away.
    ///
    /// Grounding: `let max_tries = 50_000; ... tried += 1` before the read,
    /// loop guard `tried < max_tries`.
    /// Mutation: change `50_000` to `500_000` -> read count exceeds 50_000;
    /// the exact-count assert fails. Removing the `tried += 1` increment ->
    /// would read all 200_000; also fails.
    #[test]
    fn crack_key_caps_total_tries_at_50000() {
        let mut src = MockSource::new(0x00); // clear sectors, never a hit
        let extents = [Extent {
            start_lba: 0,
            sector_count: 200_000,
        }];
        let res = crack_key(&mut src, &extents, 1);
        assert!(res.is_none(), "clear sectors yield no key");
        assert_eq!(
            src.reads.borrow().len(),
            50_000,
            "scan must stop at the 50_000-sector budget"
        );
    }

    /// The budget spans ALL extents, not per-extent: two extents summing past
    /// the cap must still stop at 50_000 total reads.
    ///
    /// Grounding: `tried` is declared outside the `for ext in extents` loop;
    /// `if tried >= max_tries { break }` after each extent.
    /// Mutation: move `let mut tried = 0` inside the extent loop -> each extent
    /// gets its own 50_000 budget; total reads would be 80_000, this fails.
    #[test]
    fn crack_key_budget_is_shared_across_extents() {
        let mut src = MockSource::new(0x00);
        let extents = [
            Extent {
                start_lba: 0,
                sector_count: 40_000,
            },
            Extent {
                start_lba: 100_000,
                sector_count: 40_000,
            },
        ];
        let res = crack_key(&mut src, &extents, 1);
        assert!(res.is_none());
        assert_eq!(
            src.reads.borrow().len(),
            50_000,
            "the 50_000 budget is shared across all extents"
        );
    }

    /// crack_key scans sequentially from each extent's start_lba. The first
    /// reads must be at the extent's start_lba, start_lba+1, ... pinning the
    /// LBA arithmetic `ext.start_lba + i`.
    ///
    /// Grounding: `reader.read_sectors(ext.start_lba + i, 1, ...)`.
    /// Mutation: change `ext.start_lba + i` to just `i` -> the recorded LBAs
    /// would start at 0, not 5000; this fails.
    #[test]
    fn crack_key_scans_from_extent_start_lba() {
        let mut src = MockSource::new(0x00);
        let extents = [Extent {
            start_lba: 5_000,
            sector_count: 4,
        }];
        let _ = crack_key(&mut src, &extents, 1);
        let reads = src.reads.borrow();
        assert_eq!(
            &reads[..],
            &[5_000, 5_001, 5_002, 5_003],
            "sequential scan from start_lba"
        );
    }

    /// A read error on a sector does NOT abort the scan: crack_key keeps
    /// scanning subsequent sectors (the error sector still counts toward the
    /// budget). With a small failing extent, every sector is attempted and the
    /// function returns None.
    ///
    /// Grounding: `if reader.read_sectors(...).is_ok() && is_scrambled(...)` —
    /// an Err simply falls through to `i += 1`.
    /// Mutation: change the read-error handling to `reader.read_sectors(...)?`
    /// (propagate) -> crack_key would stop after the first error and read only
    /// 1 sector; this asserts all 10 were attempted.
    #[test]
    fn crack_key_continues_past_read_errors() {
        let mut src = MockSource::new(0x30);
        src.fail_all = true;
        let extents = [Extent {
            start_lba: 0,
            sector_count: 10,
        }];
        let res = crack_key(&mut src, &extents, 1);
        assert!(res.is_none());
        assert_eq!(
            src.reads.borrow().len(),
            10,
            "read errors must not abort the scan"
        );
    }

    /// Empty extents (no sectors) -> crack_key reads nothing and returns None.
    /// A zero-sector extent must not read its start_lba.
    ///
    /// Grounding: `while i < ext.sector_count` with sector_count == 0 never
    /// enters.
    /// Mutation: change `i < ext.sector_count` to `i <= ext.sector_count` ->
    /// one spurious read at start_lba; this asserts zero reads.
    #[test]
    fn crack_key_empty_extent_reads_nothing() {
        let mut src = MockSource::new(0x30);
        let extents = [Extent {
            start_lba: 42,
            sector_count: 0,
        }];
        let res = crack_key(&mut src, &extents, 1);
        assert!(res.is_none());
        assert_eq!(
            src.reads.borrow().len(),
            0,
            "zero-sector extent reads nothing"
        );
    }

    /// No extents at all -> immediate None, zero reads.
    ///
    /// Grounding: `for ext in extents` over an empty slice is a no-op.
    /// Mutation: any change that reads before the loop would break this.
    #[test]
    fn crack_key_no_extents_is_none() {
        let mut src = MockSource::new(0x30);
        let res = crack_key(&mut src, &[], 1);
        assert!(res.is_none());
        assert_eq!(src.reads.borrow().len(), 0);
    }
}

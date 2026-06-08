//! CSS (Content Scramble System) — DVD disc encryption.
//!
//! CSS uses a weak 40-bit LFSR stream cipher (broken since 1999).
//!
//! The production entry point is [`resolve`]. Two title-key acquisition
//! paths exist behind it:
//! - The SCSI auth path drives bus authentication with the compiled-in CSS
//!   player keys and reads the title key from the drive (the production DVD
//!   path on a live drive).
//! - The crack fallback ([`crack_key`]) needs no keys — it attempts the
//!   Stevenson known-plaintext attack on MPEG-2 PES headers. (Currently
//!   non-functional; see the `crack` module docs.)
//!
//! Usage:
//! ```rust,ignore
//! if let Some(state) = css::resolve(&mut ctx) {
//!     css::descramble_sector(&state, &mut sector);
//! }
//! ```

pub mod auth;
pub mod crack;
pub mod lfsr;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::drive::Drive;
use crate::sector::SectorSource;

/// CSS decryption state for a DVD title.
#[derive(Debug, Clone)]
pub struct CssState {
    /// 5-byte CSS title key (from SCSI auth or the crack fallback).
    pub title_key: [u8; 5],
}

/// Inputs for CSS key acquisition.
///
/// The acquisition path depends on which inputs the caller supplies:
///
/// - With `drive` + `auth_lba` set, [`resolve`] runs the full SCSI bus
///   auth + title-key path (live BU40N / DVD drive).
/// - With `reader` + `extents` set, [`resolve`] falls back to the
///   crack path (Stevenson known-plaintext attack on encrypted PES
///   headers; works on disc images and on drives whose CSS auth path
///   is unavailable).
///
/// The `drive` (auth) path always wins when both modes are populated.
pub struct CssContext<'a> {
    /// Live SCSI drive — when present, [`resolve`] tries the auth path.
    pub drive: Option<&'a mut Drive>,
    /// LBA of a known-scrambled sector for the auth path's title-key
    /// query. Required when `drive` is set.
    pub auth_lba: Option<u32>,
    /// Sector source for the crack path.
    pub reader: Option<&'a mut dyn SectorSource>,
    /// Extents to scan for the crack path. Required when `reader` is
    /// set.
    pub extents: Option<&'a [Extent]>,
}

/// Acquire a CSS title key using whichever inputs the context provides.
///
/// Order of attempts:
///   1. SCSI auth path (when `drive` and `auth_lba` are set).
///   2. Crack path (when `reader` and `extents` are set).
///
/// Returns `None` if neither path is configured or both fail.
pub fn resolve(ctx: &mut CssContext<'_>) -> Option<CssState> {
    if let (Some(drive), Some(lba)) = (ctx.drive.as_deref_mut(), ctx.auth_lba) {
        if let Ok(title_key) = auth::authenticate_and_read_title_key(drive, lba) {
            return Some(CssState { title_key });
        }
    }
    if let (Some(reader), Some(extents)) = (ctx.reader.as_deref_mut(), ctx.extents) {
        return crack_key(reader, extents);
    }
    None
}

/// Crack the CSS title key by scanning scrambled sectors across extents and
/// applying a known-plaintext attack on MPEG-2 PES headers.
///
/// The Stevenson attack needs a sector where a PES header starts at byte
/// 0x80 (start of the encrypted region). This only happens when a new PES
/// packet begins at exactly sector offset 128. We scan up to 50000
/// scrambled sectors sequentially across all extents.
///
/// NOTE: the underlying recovery ([`crack::recover_title_key`]) is currently
/// non-functional against this crate's descrambler (see `crack` module
/// docs), so this scan returns `None`. The production DVD path uses the SCSI
/// auth path, not this crack fallback.
pub fn crack_key(reader: &mut dyn SectorSource, extents: &[Extent]) -> Option<CssState> {
    let mut tried = 0u32;
    let max_tries = 50_000;

    // Reused across every scanned sector; read_sectors overwrites all 2048
    // bytes on success, so no re-zeroing is needed between iterations.
    let mut buf = vec![0u8; 2048];

    for ext in extents {
        let mut i = 0;
        while i < ext.sector_count && tried < max_tries {
            // Every scanned sector counts toward the cap, so a long run
            // of unscrambled sectors can't read past the budget.
            tried += 1;
            if reader
                .read_sectors(ext.start_lba + i, 1, &mut buf, true)
                .is_ok()
                && is_scrambled(&buf)
            {
                if let Some(key) = crack::crack_title_key(&buf) {
                    return Some(CssState { title_key: key });
                }
            }
            i += 1;
        }
        if tried >= max_tries {
            break;
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
        let res = crack_key(&mut src, &extents);
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
        let res = crack_key(&mut src, &extents);
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
        let _ = crack_key(&mut src, &extents);
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
        let res = crack_key(&mut src, &extents);
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
        let res = crack_key(&mut src, &extents);
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
        let res = crack_key(&mut src, &[]);
        assert!(res.is_none());
        assert_eq!(src.reads.borrow().len(), 0);
    }
}

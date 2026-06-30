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

pub mod lfsr;
pub mod stevenson;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::sector::SectorSource;

/// Consecutive CSS-locked (`05/6F/03`) reads before the crack scan early-bails.
/// The bus-auth read gate is global (all-or-nothing), so a run this long means
/// it is shut and nothing here is crackable — bail instead of grinding the full
/// 50_000-sector budget (which is what made rc5 appear to hang on a wedged USB
/// bridge). The counter resets to 0 on any readable batch.
const CSS_LOCKED_BAIL: u32 = 64;

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
/// LFSR recovery needs. We scan up to 50000 sectors across the
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

/// Outcome of a CSS crack scan that distinguishes the THREE cases the bare
/// `Option<CssState>` conflated (and which caused a silent-failure bug:
/// scrambled-but-uncracked content was treated as "unencrypted" and muxed as
/// plaintext garbage at exit 0):
///
/// - [`CrackOutcome::Cracked`] — a scrambled sector yielded a title key.
/// - [`CrackOutcome::Unencrypted`] — NO scrambled sector was seen across the
///   scanned extents (`is_scrambled` never true): the content is genuinely
///   plaintext, so proceeding without a key is correct.
/// - [`CrackOutcome::ScrambledUncracked`] — scrambled sectors WERE seen but no
///   key could be recovered (the Stevenson attack found no crackable crib, or
///   the scrambled region was unreadable). The content is encrypted; muxing it
///   as plaintext would emit garbage, so callers MUST surface a hard error
///   ([`crate::error::Error::CssKeyMissing`]) instead of falling through to
///   "unencrypted".
#[derive(Debug, Clone)]
pub enum CrackOutcome {
    Cracked(CssState),
    Unencrypted,
    ScrambledUncracked,
}

impl CrackOutcome {
    /// The cracked `CssState`, if any. `None` for `Unencrypted` /
    /// `ScrambledUncracked`. Lets the `Option`-returning wrappers stay thin.
    pub fn into_state(self) -> Option<CssState> {
        match self {
            CrackOutcome::Cracked(s) => Some(s),
            _ => None,
        }
    }

    /// True when scrambled sectors were seen but no key was recovered — the
    /// case callers must surface as a hard error instead of "unencrypted".
    pub fn is_scrambled_uncracked(&self) -> bool {
        matches!(self, CrackOutcome::ScrambledUncracked)
    }
}

/// [`crack_key`] returning the full [`CrackOutcome`] (Cracked / Unencrypted /
/// ScrambledUncracked) so callers can distinguish "genuinely unencrypted" from
/// "encrypted but uncrackable" — the latter must become a hard error, never a
/// silent fall-through to plaintext.
pub fn crack_key_outcome(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> CrackOutcome {
    crack_key_scan(reader, extents, batch_sectors, halt, true)
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
    crack_key_scan(reader, extents, batch_sectors, halt, false).into_state()
}

/// The crack scan, returning the full [`CrackOutcome`]. Tracks a
/// `saw_scrambled` flag so a scrambled-but-uncracked disc is distinguished
/// from a genuinely-unencrypted one (the [`crack_key`] / [`crack_key_halt`]
/// `Option` wrappers collapse both to `None`).
fn crack_key_scan(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
    // True only on the INITIAL scan: a fully CSS-locked (`05/6F/03`) result is a
    // hard `ScrambledUncracked`. False on the per-VTS re-crack so a lapsed-AGID
    // locked read returns None instead of killing a genuinely crackable title.
    fail_on_locked: bool,
) -> CrackOutcome {
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
    // Track whether ANY scrambled sector was observed. If we exhaust the scan
    // budget having seen scrambled data but never recovered a key, the content
    // is encrypted-but-uncrackable — a HARD failure the caller must surface,
    // NOT silently treat as unencrypted (which would mux scrambled MPEG as
    // plaintext → garbage at exit 0). See `CrackOutcome::ScrambledUncracked`.
    let mut saw_scrambled = false;
    // A read rejected with sense `05/6F/03` ("scrambled sector without
    // authentication") is positive proof of CSS encryption — never collapse it
    // to "unencrypted". A run of consecutive locked reads means the bus-auth
    // gate is shut (it is global, so reads are all-or-nothing), so the scan
    // early-bails. `consecutive_locked` resets on any readable batch, so a
    // crackable title (gate open) never trips it.
    let mut saw_locked = false;
    let mut consecutive_locked = 0u32;

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
                    // A readable batch: the gate is open — reset the locked run.
                    consecutive_locked = 0;
                    for s in 0..n as usize {
                        tried += 1;
                        let sect = &buf[s * 2048..(s + 1) * 2048];
                        // Use the HARDENED pack-gated check (Fix 3): a clear stub
                        // sector with stray bits at 0x14 must NOT count as
                        // scramble evidence, or a genuinely-unencrypted title
                        // would falsely report ScrambledUncracked (a false E7023).
                        if is_scrambled_pack(sect) {
                            saw_scrambled = true;
                            if let Some(key) = stevenson::crack_title_key(sect) {
                                return CrackOutcome::Cracked(CssState {
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
                // A failed batch still counts toward the budget so a damaged
                // region can't loop forever. A CSS-locked failure (`05/6F/03`)
                // proves encryption and, in a long enough run, means the read
                // gate is shut — track it and early-bail rather than grind.
                Err(e) => {
                    tried += n;
                    if e.scsi_sense().is_some_and(|s| s.is_css_locked()) {
                        saw_locked = true;
                        consecutive_locked += 1;
                        if consecutive_locked >= CSS_LOCKED_BAIL {
                            break 'outer;
                        }
                    } else {
                        consecutive_locked = 0;
                    }
                }
            }
            i += n;
        }
    }

    // Budget exhausted / extents walked / early-bailed with no key recovered.
    // The disc is ENCRYPTED-but-uncracked (a hard failure on the initial scan)
    // when EITHER a scrambled sector was actually seen, OR — on the initial scan
    // only (`fail_on_locked`) — every read was CSS-locked (`05/6F/03`), itself
    // proof of scrambling. A re-crack (`fail_on_locked` false) stays soft: a
    // lapsed-AGID locked read yields None, not a hard fail, so a crackable title
    // in another VTS isn't killed. Only a scan that saw neither a scrambled
    // sector nor a CSS-lock is genuinely unencrypted.
    if saw_scrambled || (saw_locked && fail_on_locked) {
        CrackOutcome::ScrambledUncracked
    } else {
        CrackOutcome::Unencrypted
    }
}

/// Descramble a single CSS-encrypted sector in place.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    lfsr::descramble_sector(&state.title_key, sector);
}

/// Check if a sector has the CSS scramble flag set.
///
/// This is the RAW flag test — bits 4-5 of the sub-header byte 0x14 — used by
/// the descramble loop (`decrypt::decrypt_sectors`), which has already committed
/// to descrambling a known title's VOB data and only needs to skip the clear
/// NAV packs interleaved in it. For the CRACK SCAN's "did this disc actually
/// contain scrambled content?" decision (which must not false-positive on a
/// clear stub), use [`is_scrambled_pack`] instead.
pub fn is_scrambled(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

/// The 4-byte MPEG-2 Program Stream pack-start code (`00 00 01 BA`) every DVD
/// video sector opens with. CSS leaves the clear header (`0x00..0x80`)
/// untouched, so this signature survives scrambling.
pub(crate) const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];

/// Check if a sector is a CSS-scrambled DVD **video pack** — the HARDENED test
/// the crack scan uses to set its `saw_scrambled` evidence flag (Fix 3).
///
/// [`is_scrambled`] keys solely on bits 4-5 of byte 0x14. That single byte is
/// only meaningful inside a real DVD sector — an MPEG-2 Program Stream pack,
/// which ALWAYS begins with the 32-bit pack-start code `00 00 01 BA` at offset
/// 0x00. A tiny clear / nav-only stub (a 0.5 s menu loop, an FBI-warning title)
/// can carry arbitrary bytes that happen to set bits 4-5 of byte 0x14; trusting
/// byte 0x14 alone there would flip the scan's `saw_scrambled` gate and make a
/// genuinely-UNENCRYPTED title report `ScrambledUncracked` — a false E7023.
///
/// Requiring the pack-start signature FIRST means only a sector that is
/// structurally a DVD video pack can be counted as scramble evidence. This does
/// NOT weaken the genuine "encrypted but uncrackable" hard-fail: a real
/// scrambled feature is made of valid PS packs, so its scrambled sectors still
/// pass this check and still drive `ScrambledUncracked` when no key cracks. (The
/// descramble loop keeps the looser [`is_scrambled`]: by the time it runs we
/// already know the title is CSS, and it only needs to skip interleaved clear
/// NAV packs — a wrongly-skipped or wrongly-included sector there is recoverable
/// per-sector, whereas a false scramble verdict in the scan poisons the whole
/// title's outcome.)
pub fn is_scrambled_pack(sector: &[u8]) -> bool {
    sector.len() >= 2048 && sector[0x00..0x04] == PACK_START && (sector[0x14] >> 4) & 0x03 != 0
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

    /// Fix 3 hardening: `is_scrambled_pack` (the crack-scan evidence gate)
    /// requires BOTH the MPEG-PS pack-start code at 0x00 AND the 0x14 scramble
    /// bits. A clear / nav-only stub whose bytes happen to set bits 4-5 of 0x14
    /// but lacks the pack-start is NOT counted as scramble evidence — without
    /// this the scan flips `saw_scrambled` and a genuinely unencrypted title
    /// reports `ScrambledUncracked` (the false E7023). The looser `is_scrambled`
    /// (descramble gate) still reads the same sector as flagged.
    ///
    /// Grounding: `sector[0x00..0x04] == 00 00 01 BA && (sector[0x14] >> 4)...`.
    /// Mutation: drop the pack-start clause -> the 0x14-only sector counts as a
    /// scrambled pack; the first assert fails.
    #[test]
    fn is_scrambled_pack_requires_pack_start_signature() {
        let mut s = vec![0u8; 2048];
        s[0x14] = 0x30; // scramble bits set, but no pack-start at 0x00
        assert!(
            !is_scrambled_pack(&s),
            "0x14 bits without the MPEG-PS pack-start must NOT count as a scrambled pack"
        );
        // The looser descramble-gate check still sees the raw flag.
        assert!(is_scrambled(&s), "is_scrambled keys on the 0x14 flag alone");
        // A near-miss pack-start (wrong final byte) is still rejected.
        s[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBB]);
        assert!(
            !is_scrambled_pack(&s),
            "a wrong pack-start byte must not qualify"
        );
        // The real signature flips it to a scrambled pack.
        s[0x00..0x04].copy_from_slice(&PACK_START);
        assert!(
            is_scrambled_pack(&s),
            "valid pack-start + 0x14 bits → scrambled pack"
        );
    }

    // ── crack_key scanning over a mock SectorSource ────────────────────────

    /// Records every (lba, count) read; returns a caller-supplied flag byte at
    /// 0x14 so we can drive scrambled/clear sectors, or an injected error.
    struct MockSource {
        reads: std::cell::RefCell<Vec<u32>>,
        flag_byte: u8,
        fail_all: bool,
        /// Every read fails with CSS-locked sense `05/6F/03` (drive refusing
        /// scrambled reads because the bus-auth gate isn't open).
        lock_all: bool,
        /// When set, the sector at `crackable.0` is served as a full
        /// Stevenson-crackable scrambled sector (`crackable.1`, 2048 bytes)
        /// instead of the uniform `flag_byte` fill. Lets the scan actually
        /// reach `CrackOutcome::Cracked` from a synthetic ISO.
        crackable: Option<(u32, Vec<u8>)>,
    }

    impl MockSource {
        fn new(flag_byte: u8) -> Self {
            Self {
                reads: std::cell::RefCell::new(Vec::new()),
                flag_byte,
                fail_all: false,
                lock_all: false,
                crackable: None,
            }
        }
    }

    /// Build a Stevenson-crackable scrambled sector for `(title_key, seed)`:
    /// the cleartext header (0x59..0x80) carries a periodic run that continues
    /// across the 0x80 boundary into the encrypted region — the crib
    /// `stevenson::crack_title_key` recovers a key from. Mirrors the
    /// `synth_periodic_sector` fixture in the stevenson tests but built here
    /// from the crate-internal `scramble_sector`.
    fn crackable_sector(title_key: &[u8; 5], seed: &[u8; 5], period: usize) -> Vec<u8> {
        const RUN_START: usize = 0x59;
        const SEED_OFFSET: usize = 0x54;
        let mut plaintext = vec![0u8; 2048];
        plaintext[0x00..0x04].copy_from_slice(&PACK_START); // valid DVD pack header
        plaintext[0x14] = 0x10; // scramble flag
        let pat: Vec<u8> = (0..period)
            .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
            .collect();
        for (i, b) in plaintext.iter_mut().enumerate().skip(RUN_START) {
            *b = pat[i % period];
        }
        plaintext[SEED_OFFSET..SEED_OFFSET + 5].copy_from_slice(seed);
        lfsr::scramble_sector(title_key, &mut plaintext);
        plaintext
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
            if self.lock_all {
                return Err(Error::DiscRead {
                    sector: lba as u64,
                    status: Some(2),
                    sense: Some(crate::scsi::ScsiSense {
                        sense_key: 0x05,
                        asc: 0x6F,
                        ascq: 0x03,
                    }),
                });
            }
            if self.fail_all {
                return Err(Error::DecryptFailed);
            }
            let n = count as usize * 2048;
            let end = n.min(buf.len());
            for b in buf[..end].iter_mut() {
                *b = 0;
            }
            // Fill each sector in the batch with the uniform flag byte, EXCEPT a
            // designated crackable LBA which gets the full synthetic sector.
            for s in 0..count as u32 {
                let sect_lba = lba + s;
                let base = s as usize * 2048;
                if base + 2048 > end {
                    break;
                }
                match &self.crackable {
                    Some((clba, sector)) if *clba == sect_lba => {
                        buf[base..base + 2048].copy_from_slice(sector);
                    }
                    _ => {
                        // Real DVD video sectors always open with the MPEG-PS
                        // pack-start code; `is_scrambled` (Fix 3) requires it
                        // before trusting the 0x14 scramble bits, so the fixture
                        // must include it for a `flag_byte` of 0x30 to register
                        // as scrambled.
                        buf[base..base + 4].copy_from_slice(&PACK_START);
                        buf[base + 0x14] = self.flag_byte;
                    }
                }
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

    // ── CrackOutcome: scrambled-but-uncracked vs genuinely unencrypted (Fix 6) ─

    /// A scan over CLEAR sectors (scramble flag never set) returns
    /// `Unencrypted` — the content is genuinely plaintext, so proceeding
    /// without a key is correct.
    #[test]
    fn crack_outcome_clear_sectors_is_unencrypted() {
        let mut src = MockSource::new(0x00); // never scrambled
        let extents = [Extent {
            start_lba: 0,
            sector_count: 100,
        }];
        let outcome = crack_key_outcome(&mut src, &extents, 1, None);
        assert!(
            matches!(outcome, CrackOutcome::Unencrypted),
            "no scrambled sector seen → Unencrypted, got {outcome:?}"
        );
        // The Option wrapper collapses Unencrypted → None.
        assert!(crack_key(&mut MockSource::new(0x00), &extents, 1).is_none());
    }

    /// THE Fix 6 regression: a scan that SEES scrambled sectors (flag set) but
    /// recovers no key (the mock's zeroed data has no Stevenson crib) must
    /// return `ScrambledUncracked` — a HARD failure — NOT `Unencrypted`. The
    /// old code conflated this with "unencrypted" and muxed scrambled MPEG as
    /// plaintext (garbage at exit 0).
    #[test]
    fn crack_outcome_scrambled_uncracked_is_hard_failure() {
        let mut src = MockSource::new(0x30); // scrambled flag set, no crackable crib
        let extents = [Extent {
            start_lba: 0,
            sector_count: 100,
        }];
        let outcome = crack_key_outcome(&mut src, &extents, 1, None);
        assert!(
            outcome.is_scrambled_uncracked(),
            "scrambled sectors seen but no key → ScrambledUncracked, got {outcome:?}"
        );
        // The legacy Option wrapper still collapses this to None (the callers
        // that need the distinction now use crack_key_outcome instead).
        assert!(crack_key(&mut MockSource::new(0x30), &extents, 1).is_none());
    }

    /// Even when every read FAILS, a scan that never managed to observe a
    /// scrambled sector reports `Unencrypted` (we cannot prove encryption from
    /// unreadable data alone — the AACS/keydb paths and the disc-level
    /// `css_error` plumbing cover genuinely unreadable encrypted discs).
    #[test]
    fn crack_outcome_all_reads_fail_is_unencrypted() {
        let mut src = MockSource::new(0x30);
        src.fail_all = true; // no sector is ever inspected
        let extents = [Extent {
            start_lba: 0,
            sector_count: 10,
        }];
        let outcome = crack_key_outcome(&mut src, &extents, 1, None);
        assert!(
            matches!(outcome, CrackOutcome::Unencrypted),
            "no readable scrambled sector → Unencrypted, got {outcome:?}"
        );
    }

    /// Fix C (rc.5.1): on the INITIAL scan, a drive that refuses every read with
    /// CSS-locked sense (`05/6F/03`) is encrypted-but-locked →
    /// `ScrambledUncracked` (a hard failure), NOT `Unencrypted`. This is the
    /// rc4.3 bug: every VOB read came back `6F/03`, so the scan saw no scrambled
    /// sector and wrongly declared the disc unencrypted → 19 KB garbage.
    #[test]
    fn crack_outcome_css_locked_initial_is_scrambled_uncracked() {
        let mut src = MockSource::new(0x30);
        src.lock_all = true; // every read → 05/6F/03
        let extents = [Extent {
            start_lba: 0,
            sector_count: 100,
        }];
        let outcome = crack_key_outcome(&mut src, &extents, 1, None);
        assert!(
            outcome.is_scrambled_uncracked(),
            "every read 6F/03 on the initial scan → ScrambledUncracked, got {outcome:?}"
        );
    }

    /// MISSING #1 guard: the re-crack path (the `Option`-returning `crack_key`,
    /// `fail_on_locked == false`) must NOT hard-fail on a CSS-locked read — it
    /// returns `None`. A lapsed-AGID re-crack of another VTS stays soft so a
    /// genuinely crackable title isn't killed by a transient locked read.
    #[test]
    fn crack_key_recrack_locked_is_none_not_hard_fail() {
        let mut src = MockSource::new(0x30);
        src.lock_all = true;
        let extents = [Extent {
            start_lba: 0,
            sector_count: 100,
        }];
        assert!(crack_key(&mut src, &extents, 1).is_none());
    }

    /// Fix F: a fully CSS-locked scan early-bails near `CSS_LOCKED_BAIL`
    /// consecutive locked reads instead of grinding the whole 50_000-sector
    /// budget (the rc5 "stuck Scanning…" hang on a wedged bridge).
    #[test]
    fn crack_css_locked_scan_early_bails() {
        let mut src = MockSource::new(0x30);
        src.lock_all = true;
        let extents = [Extent {
            start_lba: 0,
            sector_count: 10_000,
        }];
        let _ = crack_key_outcome(&mut src, &extents, 1, None);
        let n = src.reads.borrow().len();
        assert!(
            n <= (CSS_LOCKED_BAIL as usize) + 1,
            "locked scan early-bails near {CSS_LOCKED_BAIL}, not 10000; read {n}"
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

    // ── Scan-level Cracked branch + per-VTS re-crack success (audit §2 / §5 #8) ─

    /// SCAN-LEVEL CRACKED (audit gap "MockSource never yields a crackable
    /// sector"): drive the full `crack_key_scan` over a synthetic ISO whose
    /// scan hits a Stevenson-crackable scrambled sector. The outcome must be
    /// `CrackOutcome::Cracked` with a key that round-trips the sector, AND the
    /// `crack_span` must be recorded as the half-open extent span (the per-VTS
    /// routing key the mux path needs). Previously only the leaf crack and the
    /// Uncracked/Unencrypted branches were tested — the Cracked branch and
    /// `crack_span` recording were never exercised end-to-end.
    #[test]
    fn crack_outcome_reaches_cracked_with_span() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11, 0x22, 0x33, 0x44, 0x55];
        let crackable = crackable_sector(&title_key, &seed, 8);
        // The crackable sector sits a few sectors into the extent.
        let mut src = MockSource::new(0x00); // surrounding sectors: clear
        src.crackable = Some((1003, crackable.clone()));
        let extents = [Extent {
            start_lba: 1000,
            sector_count: 50,
        }];
        let outcome = crack_key_outcome(&mut src, &extents, 4, None);
        let state = match outcome {
            CrackOutcome::Cracked(s) => s,
            other => panic!("expected Cracked, got {other:?}"),
        };
        // The recovered key descrambles the crackable sector body.
        let mut test = crackable.clone();
        descramble_sector(&state, &mut test);
        let mut plain = crackable;
        lfsr::descramble_sector(&title_key, &mut plain);
        assert_eq!(
            &test[0x80..],
            &plain[0x80..],
            "recovered key must round-trip the scrambled sector body"
        );
        // crack_span = half-open [start, start+count) of the scanned extent.
        assert_eq!(
            state.crack_span,
            Some((1000, 1050)),
            "crack_span must record the extent LBA span for per-VTS routing"
        );
    }

    /// CSS_ERROR WIRING (audit §2 / §5 #7): an all-locked synthetic ISO (every
    /// VOB read returns CSS-locked sense `05/6F/03` across MULTIPLE extents, as a
    /// real encrypted-but-unauthenticated disc image does) must produce the exact
    /// outcome the scan converts into `disc.css_error = Some(Error::CssKeyMissing)`
    /// — i.e. `CrackOutcome::ScrambledUncracked` / `is_scrambled_uncracked()`,
    /// NOT `Unencrypted`. disc/mod.rs's `crack_key_outcome → ScrambledUncracked`
    /// arm (where it stamps css_error) is driven by exactly this signal, so this
    /// pins the css-layer contract that arm depends on without touching the
    /// scan plumbing.
    #[test]
    fn all_locked_synthetic_iso_yields_css_key_missing_signal() {
        let mut src = MockSource::new(0x30);
        src.lock_all = true; // every read → 05/6F/03 across the whole "ISO"
        let extents = [
            Extent {
                start_lba: 0,
                sector_count: 30,
            },
            Extent {
                start_lba: 5_000,
                sector_count: 30,
            },
        ];
        let outcome = crack_key_outcome(&mut src, &extents, 16, None);
        assert!(
            outcome.is_scrambled_uncracked(),
            "all-locked ISO → ScrambledUncracked (the css_error=CssKeyMissing \
             signal), got {outcome:?}"
        );
        // The legacy Option wrapper still collapses it to None — callers that
        // surface the hard error must use crack_key_outcome, which this proves.
        let mut src2 = MockSource::new(0x30);
        src2.lock_all = true;
        assert!(crack_key(&mut src2, &extents, 16).is_none());
    }

    /// PER-VTS RE-CRACK SUCCESS (audit gap "success path missing"): the prior
    /// re-crack test only covered the locked→None path. Here a re-crack
    /// (`crack_key`, `fail_on_locked == false`) over a DIFFERENT VTS's extents
    /// finds that VTS's own crackable sector and returns a `CssState` whose
    /// `crack_span` matches the new extents — proving a key cracked for one VTS
    /// is genuinely re-derived (not reused) for another.
    #[test]
    fn recrack_succeeds_on_other_vts_extents() {
        let title_key = [0xFE, 0xDC, 0xBA, 0x98, 0x76];
        let seed = [0x00, 0xFF, 0x80, 0x7F, 0x01];
        let crackable = crackable_sector(&title_key, &seed, 5);
        let mut src = MockSource::new(0x00);
        // The second VTS lives at a disjoint LBA range; its crackable sector is
        // the first one in the extent.
        src.crackable = Some((9000, crackable));
        let other_vts = [Extent {
            start_lba: 9000,
            sector_count: 20,
        }];
        let state = crack_key(&mut src, &other_vts, 4).expect("re-crack must recover a key");
        assert_eq!(
            state.crack_span,
            Some((9000, 9020)),
            "re-crack span must reflect the OTHER VTS extents, not a reused span"
        );
    }
}

//! CSS (Content Scramble System) — DVD disc encryption.
//!
//! CSS uses a weak 40-bit LFSR stream cipher (broken since 1999).
//!
//! The title key is recovered keylessly: [`crack_key`] recovers it directly
//! from the scrambled data (see the [`keyless`] module),
//! needing no player keys, disc-key recovery, or external key file.
//! Sectors are then decrypted with [`descramble_sector`].
//!
//! Usage:
//! ```rust,ignore
//! if let Some(state) = css::crack_key(reader, extents, batch) {
//!     css::descramble_sector(&state, &mut sector);
//! }
//! ```

pub mod keyless;
pub mod lfsr;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::sector::SectorSource;

// Consecutive CSS-locked reads before the crack scan early-bails, instead of
// grinding the full 50_000-sector budget. Resets to 0 on any readable batch.
// See docs/css-mod.md — CSS_LOCKED_BAIL.
const CSS_LOCKED_BAIL: u32 = 64;

/// CSS decryption state for a DVD title.
#[derive(Clone)]
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

// Redacting `Debug`: `CssState` is reachable via the public `Disc.css` field, so
// a `{:?}` on a `Disc` would otherwise print the raw CSS title key. Print only
// the (non-secret) crack span. Guarded by `css_state_debug_is_redacted`.
impl std::fmt::Debug for CssState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CssState")
            .field("title_key", &"<redacted>")
            .field("crack_span", &self.crack_span)
            .finish()
    }
}

/// Recover the CSS title key with no keys, by scanning scrambled sectors and
/// running the known-plaintext attack (see the [`keyless`] module).
///
/// Scans up to 50000 sectors across `extents` and returns the first sector
/// that yields a key — no player keys, no disc-key crack. Works on a live
/// drive and on disc images alike.
///
/// This convenience form runs to completion (no cancellation); callers
/// needing a cancel token, or the three-way [`CrackOutcome`], use
/// [`crack_key_outcome`]. See docs/css-mod.md — `crack_key`.
pub fn crack_key(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
) -> Option<CssState> {
    crack_key_scan(reader, extents, batch_sectors, None).into_state()
}

/// Outcome of a CSS crack scan: distinguishes the THREE cases a bare
/// `Option<CssState>` conflated (see docs/css-mod.md — `CrackOutcome`).
///
/// - [`CrackOutcome::Cracked`] — a scrambled sector yielded a title key.
/// - [`CrackOutcome::Unencrypted`] — no scrambled sector was seen; genuinely
///   plaintext.
/// - [`CrackOutcome::ScrambledUncracked`] — scrambled sectors seen but no key
///   recovered; callers MUST hard-error, never fall through.
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

/// [`crack_key`] returning the full [`CrackOutcome`] so callers can
/// distinguish "genuinely unencrypted" from "encrypted but uncrackable" — the
/// latter must become a hard error, never a silent fall-through to plaintext.
///
/// Takes an optional cooperative-cancellation token: polls `halt` once per
/// batch (the same cadence sweep/patch use) and emits a `freemkv::heartbeat`
/// beat ("css_crack") each batch so a stuck scan over bad sectors stays
/// visible in the log rather than hanging silently.
pub fn crack_key_outcome(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> CrackOutcome {
    crack_key_scan(reader, extents, batch_sectors, halt)
}

// The SINGLE place every DVD read path obtains a title key when the caller
// supplied none. A scrambled-but-uncrackable title is a hard, skippable
// per-title CssKeyMissing (another VTS may still crack). See docs/css-mod.md.
pub(crate) fn resolve_dvd_title_key(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    keys: &mut crate::decrypt::DecryptKeys,
    batch_sectors: u16,
    format: crate::disc::ContentFormat,
    raw: bool,
    halt: Option<&crate::halt::Halt>,
) -> std::io::Result<()> {
    // `--raw` = deliberate ciphertext passthrough: never crack or descramble, and
    // never hard-fail on scrambled-uncrackable. Caller hands us `None` on
    // purpose; without this guard we'd silently DECRYPT or abort a raw mux.
    if raw {
        return Ok(());
    }
    if matches!(keys, crate::decrypt::DecryptKeys::None)
        && format == crate::disc::ContentFormat::MpegPs
    {
        // `halt` threads the caller's cancellation token so /api/stop can
        // interrupt a long crack scan (the old scan-time crack honored it too).
        let outcome = crack_key_outcome(reader, extents, batch_sectors, halt);
        // A cancelled crack's outcome is a TRUNCATED scan, not a real verdict —
        // surface it as `Halted` rather than trusting a partial `Unencrypted`
        // or `ScrambledUncracked` and taking the wrong path.
        if halt.map(|h| h.is_cancelled()).unwrap_or(false) {
            return Err(crate::error::Error::Halted.into());
        }
        match outcome {
            CrackOutcome::Cracked(state) => {
                *keys = crate::decrypt::DecryptKeys::Css {
                    title_key: state.title_key,
                };
            }
            CrackOutcome::ScrambledUncracked => {
                return Err(crate::error::Error::CssKeyMissing.into());
            }
            CrackOutcome::Unencrypted => {}
        }
    }
    Ok(())
}

// The crack scan, returning the full CrackOutcome. Tracks `saw_scrambled` so
// a scrambled-but-uncracked disc is distinguished from a genuinely
// unencrypted one (crack_key's Option wrapper collapses both to None).
fn crack_key_scan(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> CrackOutcome {
    // Batch the reads: a live drive at 1 sector/read is glacial. `batch_sectors`
    // MUST be sized to the source — a drive rejects a READ(10) larger than its
    // per-command max, and `Drive::read` does not chunk an over-large batch.
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
    // Track whether ANY scrambled sector was observed: if the budget is
    // exhausted with scrambled data seen but no key recovered, that's a HARD
    // failure, not "unencrypted". See `CrackOutcome::ScrambledUncracked`.
    let mut saw_scrambled = false;
    // Sense `05/6F/03` is positive proof of CSS encryption. Consecutive locked
    // reads mean the (global) bus-auth gate is shut, so the scan early-bails;
    // this resets on any readable batch, so an open-gate title never trips it.
    let mut saw_locked = false;
    let mut consecutive_locked = 0u32;

    'outer: for (extent_idx, ext) in extents.iter().enumerate() {
        let mut i = 0u32;
        while i < ext.sector_count && tried < max_tries {
            // Cooperative cancellation — poll once per batch, the same cadence
            // sweep/patch use, so a Stop / watchdog can interrupt the scan.
            if let Some(h) = halt
                && h.is_cancelled()
            {
                break 'outer;
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
            // Set from bytes actually READ on the Ok path so a short read is
            // RETRIED from where it stopped, not skipped — else those sectors
            // go unexamined on exactly the damaged media a key needs most.
            let mut advance = n;
            match reader.read_sectors(ext.start_lba + i, n as u16, &mut buf[..want], true) {
                Ok(got) => {
                    // A readable batch: the gate is open — reset the locked run.
                    consecutive_locked = 0;
                    // Inspect only what was READ: `buf` is reused across
                    // batches, so its tail may hold the PREVIOUS batch's
                    // sectors, risking a key crack for the wrong extent/VTS.
                    let usable = (got / 2048).min(n as usize);
                    // At least one, so a source returning Ok(0) cannot spin here.
                    advance = (usable as u32).max(1);
                    if usable == 0 {
                        // Nothing inspected, but the cursor still moves one
                        // sector — charge it here or `tried` stays frozen
                        // (mirrors the `Err` arm's `tried += n`).
                        tried += 1;
                    }
                    for s in 0..usable {
                        tried += 1;
                        let sect = &buf[s * 2048..(s + 1) * 2048];
                        // HARDENED pack-gated check: a clear stub sector with
                        // stray bits at 0x14 must NOT count as scramble evidence,
                        // or an unencrypted title falsely reports E7023.
                        if is_scrambled_pack(sect) {
                            saw_scrambled = true;
                            if let Some(key) = keyless::crack_title_key(sect) {
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
                // region can't loop forever. A CSS-locked failure proves
                // encryption; a long enough run means the gate is shut.
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
            i += advance;
        }
    }

    // ENCRYPTED-but-uncracked (hard failure) if a scrambled sector was seen or
    // every read was CSS-locked; only a scan seeing neither is unencrypted. A
    // prior "soft" `fail_on_locked` toggle was removed as dead/unsafe.
    if saw_scrambled || saw_locked {
        CrackOutcome::ScrambledUncracked
    } else {
        CrackOutcome::Unencrypted
    }
}

/// Descramble a single CSS-encrypted sector in place.
///
/// A no-op unless the sector is a scrambled MPEG-2 PS PACK: the pack start
/// code is checked, not just the byte 0x14 flag bits, since 0x14 alone is
/// unreliable outside a pack (see [`is_scrambled_pack`]). Making the guard
/// part of the function, rather than something each caller must remember, is
/// what keeps the safe path the easy one — see docs/css-mod.md for the
/// measured `VIDEO_TS.IFO` corruption this prevents.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    if !is_scrambled_pack(sector) {
        return;
    }
    lfsr::descramble_sector(&state.title_key, sector);
}

/// Descramble a whole CSS buffer in place, re-cracking the title key on a VOB
/// region boundary. `title_key` is a CACHE of the last crack: validated
/// against the clear-header crib on every scrambled sector and re-cracked on
/// a miss. A crib-less sector rides the cached key. See docs/css-mod.md —
/// `descramble_region` for the full rationale.
///
/// # Errors
///
/// Never returns `Err` — `Result` only matches the decrypt seam this is
/// dispatched from (see [`crate::decrypt::decrypt_sectors`]).
pub fn descramble_region(buf: &mut [u8], title_key: &mut [u8; 5]) -> crate::error::Result<usize> {
    for chunk in buf.chunks_mut(2048) {
        // `is_scrambled_pack`, NOT the looser `is_scrambled`: this sees arbitrary
        // regions (IFO/UDF/ISO 9660) where raw byte 0x14 isn't a reliable flag.
        // Measured: an IFO misread this way was destroyed, dropping titles 38→10.
        if chunk.len() < 2048 || !is_scrambled_pack(chunk) {
            continue;
        }
        let crib = keyless::attack_crib(chunk);
        // Snapshot the ciphertext (chunk is exactly 2048 here) only when there is
        // a crib to validate against, so the common cache-hit path costs no
        // per-sector heap allocation.
        let mut original = [0u8; 2048];
        if crib.is_some() {
            original.copy_from_slice(chunk);
        }
        lfsr::descramble_sector(title_key, chunk);
        if let Some(crib) = crib
            && chunk[0x80..0x80 + 10] != crib[..]
        {
            // Cached key is stale for this region — restore the ciphertext and
            // crack this sector's own key.
            chunk.copy_from_slice(&original);
            match keyless::crack_title_key(chunk) {
                Some(fresh) => {
                    *title_key = fresh;
                    lfsr::descramble_sector(title_key, chunk);
                }
                None => {
                    // Re-crack found nothing; descramble with the CACHED key
                    // anyway — mismatch+failure signals a crib false positive,
                    // not a stale key (`DecryptFailed` here made discs unrippable).
                    lfsr::descramble_sector(title_key, chunk);
                }
            }
        }
    }
    Ok(0)
}

/// Whether bits 4-5 of the sub-header byte 0x14 are set. NOTHING MORE.
///
/// This is deliberately NOT called `is_scrambled`: byte 0x14 is only
/// meaningful inside an MPEG-2 PS pack, and treating the flag alone as proof
/// of scrambling corrupted a real disc (see docs/css-mod.md).
///
/// **Callers want [`is_scrambled_pack`].** It also requires the pack start
/// code, which every genuinely scrambled VOB sector carries and no IFO sector
/// does. This stays public only because an integration test asserts the flag
/// extraction directly; it has no production callers.
pub fn has_scramble_flag_bits(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

/// The 4-byte MPEG-2 Program Stream pack-start code (`00 00 01 BA`) every DVD
/// video sector opens with. CSS leaves the clear header (`0x00..0x80`)
/// untouched, so this signature survives scrambling.
pub(crate) const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];

/// Check if a sector is a CSS-scrambled DVD **video pack** — the HARDENED
/// test the crack scan uses for its `saw_scrambled` flag, and the same gate
/// [`descramble_sector`] / [`descramble_region`] use.
///
/// Requires BOTH the MPEG-PS pack-start code AND the 0x14 scramble bits —
/// [`has_scramble_flag_bits`] alone is not enough. Also excludes
/// structural/nav `stream_id`s at offset 0x11 (`0xBB`/`0xBE`/`0xBF`) that CSS
/// never scrambles, so a decrypted HD-DVD's RDI nav packs can't falsely trip
/// it. See docs/css-mod.md — `is_scrambled_pack`.
pub fn is_scrambled_pack(sector: &[u8]) -> bool {
    use crate::consts::pes_stream_id::{PADDING_STREAM, PRIVATE_STREAM_2, SYSTEM_HEADER};
    sector.len() >= 2048
        && sector[0x00..0x04] == PACK_START
        && !matches!(
            sector[0x11],
            SYSTEM_HEADER | PADDING_STREAM | PRIVATE_STREAM_2
        )
        && (sector[0x14] >> 4) & 0x03 != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};

    // A crib mismatch whose re-crack fails keeps the CACHED key and
    // descrambles with it — it does NOT fail the rip (a crib false positive,
    // not a stale key). See docs/css-mod.md — this test's name.
    #[test]
    fn a_crib_false_positive_keeps_the_cached_key_rather_than_failing() {
        // Header periodic enough to yield a crib, body random enough that no
        // LFSR seed reproduces it — crib mismatch, re-crack fails.
        let mut sector = [0u8; 2048];
        sector[0x14] = 0x30;
        for (i, b) in sector.iter_mut().enumerate().take(0x80).skip(0x20) {
            *b = (i % 4) as u8;
        }
        for (i, b) in sector.iter_mut().enumerate().skip(0x80) {
            *b = ((i * 37 + 11) % 251) as u8;
        }
        assert!(
            has_scramble_flag_bits(&sector),
            "fixture must be a scrambled sector"
        );
        assert!(
            keyless::attack_crib(&sector).is_some(),
            "fixture must yield a crib, or the mismatch branch is never entered"
        );
        assert!(
            keyless::crack_title_key(&sector).is_none(),
            "fixture must be uncrackable, or the failure branch is never entered"
        );

        let key_before = [0xAAu8; 5];
        let mut key = key_before;
        let out = descramble_region(&mut sector, &mut key)
            .expect("a crib false positive must NOT fail the rip");

        // "No loss term" belongs to the SEAM, not this function: assert it one
        // level up (at `decrypt_sectors`) where the value is assembled and
        // returned, so the arm dispatch and plumbing are exercised too.
        assert_eq!(out, 0);
        let mut seam_sector = sector;
        let mut seam_keys = crate::decrypt::DecryptKeys::Css { title_key: key };
        assert_eq!(
            crate::decrypt::decrypt_sectors(&mut seam_sector, &mut seam_keys, 0)
                .expect("a crib false positive must NOT fail the rip at the seam either"),
            0,
            "CSS reports no loss term of its own through decrypt_sectors"
        );
        assert_eq!(
            key, key_before,
            "a failed re-crack must leave the cached key in place — it is still \
             the best evidence, and overwriting it would poison every later sector"
        );
    }

    /// `CssState` is reachable via the public `Disc.css` field, so a `{:?}` on a
    /// `Disc` must not print the raw CSS title key. Sentinel byte 213 (0xD5);
    /// `crack_span` is non-secret and none of its values are 213.
    #[test]
    fn css_state_debug_is_redacted() {
        let s = CssState {
            title_key: [0xD5; 5],
            crack_span: Some((10, 20)),
        };
        let dbg = format!("{s:?}");
        assert!(
            !dbg.contains("213"),
            "CssState Debug leaked the title key: {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "CssState Debug missing marker: {dbg}"
        );
    }

    // ── has_scramble_flag_bits ─────────────────────────────────────────────

    // A buffer shorter than one sector reports false WITHOUT indexing 0x14
    // (short-circuited before the flag read).
    #[test]
    fn has_scramble_flag_bits_short_buffer_is_false_no_panic() {
        assert!(!has_scramble_flag_bits(&[]));
        assert!(!has_scramble_flag_bits(&[0u8; 20])); // shorter than 0x14+1 even
        assert!(!has_scramble_flag_bits(&[0xFFu8; 2047])); // one byte short of a sector
    }

    // Keys on bits 4-5 of byte 0x14 only: 0x10/0x20/0x30 is scrambled,
    // 0x00/0x40/0x80 is clear. See docs/css-mod.md — this test's name.
    #[test]
    fn has_scramble_flag_bits_uses_bits_4_5_only() {
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
                has_scramble_flag_bits(&s),
                expected,
                "flag byte {flag:#04x} scramble detection"
            );
        }
    }

    // Accepts exactly 2048 bytes as the minimum (inclusive boundary).
    #[test]
    fn has_scramble_flag_bits_exact_sector_length_accepted() {
        let mut s = vec![0u8; 2048];
        s[0x14] = 0x30;
        assert!(
            has_scramble_flag_bits(&s),
            "exactly 2048 bytes must be eligible"
        );
    }

    // Fix 3 hardening: `is_scrambled_pack` requires BOTH the pack-start code
    // AND the 0x14 bits, so a stub with stray 0x14 bits but no pack-start
    // isn't scramble evidence (else a clear title reports E7023).
    #[test]
    fn is_scrambled_pack_requires_pack_start_signature() {
        let mut s = vec![0u8; 2048];
        s[0x14] = 0x30; // scramble bits set, but no pack-start at 0x00
        assert!(
            !is_scrambled_pack(&s),
            "0x14 bits without the MPEG-PS pack-start must NOT count as a scrambled pack"
        );
        // The looser descramble-gate check still sees the raw flag.
        assert!(
            has_scramble_flag_bits(&s),
            "has_scramble_flag_bits keys on the 0x14 flag alone"
        );
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

    // Detection fix: `is_scrambled_pack` must EXCLUDE structural/nav
    // stream_ids (0xBB/0xBE/0xBF) at 0x11, the exact decrypted-HD-DVD defect
    // (an `.evo` RDI pack is 0xBF). See docs/css-mod.md — this test's name.
    #[test]
    fn is_scrambled_pack_excludes_nav_and_structural_stream_ids() {
        use crate::consts::pes_stream_id::{
            PADDING_STREAM, PRIVATE_STREAM_1, PRIVATE_STREAM_2, SYSTEM_HEADER, VIDEO, VIDEO_MAX,
        };
        // A pack-start pack with 0x14 scramble bits set, varying only 0x11.
        let mut s = vec![0u8; 2048];
        s[0x00..0x04].copy_from_slice(&PACK_START);
        s[0x14] = 0x30;
        for excluded in [SYSTEM_HEADER, PADDING_STREAM, PRIVATE_STREAM_2] {
            s[0x11] = excluded;
            assert!(
                !is_scrambled_pack(&s),
                "stream_id {excluded:#04x} is a structural/nav pack CSS never scrambles — \
                 it must NOT count as scramble evidence (else a decrypted HD-DVD RDI pack → E7023)"
            );
        }
        // A genuinely scramblable elementary-stream pack must STILL register —
        // proving the exclusion did not weaken real-DVD CSS detection.
        for scramblable in [VIDEO, VIDEO_MAX, PRIVATE_STREAM_1, 0xE2] {
            s[0x11] = scramblable;
            assert!(
                is_scrambled_pack(&s),
                "stream_id {scramblable:#04x} is a scramblable ES pack — a real CSS DVD's \
                 scrambled sector must still be counted, never passed through as plaintext"
            );
        }
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
        /// crackable scrambled sector instead of the uniform `flag_byte`
        /// fill, so the scan can reach `CrackOutcome::Cracked`.
        crackable: Option<(u32, Vec<u8>)>,
        /// Sectors actually filled per batch — models the SHORT READ a
        /// `recovery: true` source may return. `Some(0)` must not spin
        /// the scan.
        short_read: Option<usize>,
        /// PES `stream_id` at offset 0x11 of each uniform-fill sector; `0xBF`
        /// models an HD-DVD `.evo` RDI nav pack (pack-start + 0x14 bits, no
        /// CSS). See docs/css-mod.md.
        stream_id: u8,
    }

    impl MockSource {
        fn new(flag_byte: u8) -> Self {
            Self {
                reads: std::cell::RefCell::new(Vec::new()),
                flag_byte,
                fail_all: false,
                lock_all: false,
                crackable: None,
                short_read: None,
                stream_id: 0x00,
            }
        }
    }

    /// Build a crackable scrambled sector for `(title_key, seed)`: the
    /// cleartext header carries a periodic run continuing into the
    /// encrypted region, giving `keyless::crack_title_key` a crib.
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
            // A short read fills, and reports, fewer sectors than asked.
            let filled = match self.short_read {
                Some(k) => (k as u16).min(count),
                None => count,
            };
            let n = filled as usize * 2048;
            let end = n.min(buf.len());
            for b in buf[..end].iter_mut() {
                *b = 0;
            }
            // Fill each sector in the batch with the uniform flag byte, EXCEPT a
            // designated crackable LBA which gets the full synthetic sector.
            for s in 0..filled as u32 {
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
                        // `is_scrambled_pack` requires the MPEG-PS pack-start code
                        // before trusting 0x14; `stream_id` defaults to 0x00
                        // (scramblable) vs. an RDI nav pack's excluded 0xBF.
                        buf[base..base + 4].copy_from_slice(&PACK_START);
                        buf[base + 0x11] = self.stream_id;
                        buf[base + 0x14] = self.flag_byte;
                    }
                }
            }
            Ok(n)
        }
    }

    // ── Short reads: the branch nothing exercised ──────────
    // `recovery = true` means a `SectorSource` may return Ok with fewer bytes
    // than asked; no source here did, so `usable`/`advance`/`.max(1)` were dead.

    /// A short batch is RE-READ from where it stopped, not skipped. Skipping
    /// would quietly shrink the crack's coverage on exactly the damaged media
    /// where a key is hardest to find.
    #[test]
    fn a_short_read_resumes_from_where_it_stopped() {
        let mut src = MockSource::new(0x00);
        src.short_read = Some(1);
        let ext = [crate::disc::Extent {
            start_lba: 100,
            sector_count: 4,
        }];
        let _ = crack_key_scan(&mut src, &ext, 4, None);
        let reads = src.reads.borrow().clone();
        assert_eq!(
            reads,
            vec![100, 101, 102, 103],
            "a source that filled one sector per batch must be asked for the \
             next one, not advanced a whole batch past it"
        );
    }

    /// A source that reads NOTHING must terminate. Without `.max(1)` the
    /// cursor never moves and the budget cannot end the loop — the scan
    /// would spin forever on hostile input.
    #[test]
    fn a_source_that_returns_zero_sectors_terminates() {
        let mut src = MockSource::new(0x00);
        src.short_read = Some(0);
        let ext = [crate::disc::Extent {
            start_lba: 0,
            sector_count: 8,
        }];
        let outcome = crack_key_scan(&mut src, &ext, 4, None);
        assert!(
            matches!(outcome, CrackOutcome::Unencrypted),
            "nothing was read, so nothing scrambled was seen"
        );
        assert!(
            src.reads.borrow().len() <= 8,
            "the cursor must advance even on an empty read; got {} reads over \
             an 8-sector extent",
            src.reads.borrow().len()
        );
    }

    // The 50_000-sector budget must hold even when a source always returns
    // Ok(0): `tried` is charged in the `usable == 0` arm so a short-answering
    // source can't turn off the anti-grind bound. See docs/css-mod.md.
    #[test]
    fn a_source_that_returns_zero_sectors_still_obeys_the_scan_budget() {
        const MAX_TRIES: usize = 50_000;
        let mut src = MockSource::new(0x00);
        src.short_read = Some(0);
        // Deliberately LARGER than the budget: if the budget is what stops the
        // scan, the extent's own length is never reached.
        let ext = [crate::disc::Extent {
            start_lba: 0,
            sector_count: 60_000,
        }];
        let _ = crack_key_scan(&mut src, &ext, 4, None);
        let reads = src.reads.borrow().len();
        assert!(
            reads <= MAX_TRIES,
            "an Ok(0)-returning source must be stopped by the {MAX_TRIES}-sector \
             budget, not by the disc-declared extent length; got {reads} reads"
        );
    }

    // crack_key caps total scanned sectors at 50_000 even over a far larger
    // extent, counting every scanned (clear or not) sector toward the budget.
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

    // Fix 6 regression: a scan that SEES scrambled sectors but recovers no
    // key must return ScrambledUncracked (a hard failure), not Unencrypted
    // — the old code muxed scrambled MPEG as plaintext garbage.
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

    // The PUBLIC per-sector entry point must also refuse a non-pack sector
    // (a real VIDEO_TS.IFO sector with 0x14 bits set but no pack-start),
    // matching the region-level guard. See docs/css-mod.md.
    #[test]
    fn descramble_sector_refuses_a_non_pack_sector() {
        let mut ifo_like = vec![0u8; 2048];
        for (i, b) in ifo_like.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(11);
        }
        ifo_like[0x00..0x04].copy_from_slice(&[0x00, 0x26, 0x00, 0x00]); // not a pack
        ifo_like[0x14] = 0x15; // flag bits set — the trap
        assert!(has_scramble_flag_bits(&ifo_like) && !is_scrambled_pack(&ifo_like));

        let pristine = ifo_like.clone();
        let state = CssState {
            title_key: [0x42, 0x13, 0x37, 0xBE, 0xEF],
            crack_span: None,
        };
        descramble_sector(&state, &mut ifo_like);
        assert_eq!(
            ifo_like, pristine,
            "a non-pack sector must survive byte-identical through the public API"
        );
    }

    #[test]
    fn descramble_region_leaves_a_non_pack_sector_alone_even_with_the_flag_set() {
        let mut ifo_like = vec![0u8; 2048];
        for (i, b) in ifo_like.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(11);
        }
        ifo_like[0x00..0x04].copy_from_slice(&[0x00, 0x26, 0x00, 0x00]); // not a pack
        ifo_like[0x14] = 0x15; // bits 4-5 set: reads as "scrambled" to the raw test
        assert!(
            has_scramble_flag_bits(&ifo_like) && !is_scrambled_pack(&ifo_like),
            "fixture must be exactly the case the two predicates disagree on"
        );

        let pristine = ifo_like.clone();
        let mut key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        descramble_region(&mut ifo_like, &mut key).expect("region descramble");

        assert_eq!(
            ifo_like, pristine,
            "a non-pack sector must survive byte-identical — descrambling it \
             corrupts the very structures that enumerate titles"
        );
    }

    /// The other half of the contract: the guard must not cost coverage of
    /// sectors that genuinely ARE scrambled VOB data.
    #[test]
    fn descramble_region_still_descrambles_a_real_scrambled_pack() {
        let mut pack = vec![0u8; 2048];
        for (i, b) in pack.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(29).wrapping_add(3);
        }
        pack[0x00..0x04].copy_from_slice(&PACK_START);
        pack[0x14] = 0x30;
        assert!(is_scrambled_pack(&pack));

        let scrambled = pack.clone();
        let mut key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        descramble_region(&mut pack, &mut key).expect("region descramble");

        assert_ne!(
            pack[0x80..],
            scrambled[0x80..],
            "the encrypted region of a real pack must actually be transformed"
        );
        assert_eq!(
            (pack[0x14] >> 4) & 0x03,
            0,
            "the scrambling-control bits must be cleared once descrambled"
        );
        assert_eq!(
            pack[0x15..0x80],
            scrambled[0x15..0x80],
            "CSS only scrambles from 0x80 on; the rest of the header, byte 0x14 \
             aside, must be untouched"
        );
        assert_eq!(pack[..0x14], scrambled[..0x14]);
    }

    // Even when every read FAILS, a scan that never observed a scrambled
    // sector reports Unencrypted — encryption can't be proven from
    // unreadable data alone.
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

    // Fix C: on the INITIAL scan, a drive that refuses every read with
    // CSS-locked sense is encrypted-but-locked → ScrambledUncracked, NOT
    // Unencrypted (the rc4.3 bug: wrongly declared unencrypted → garbage).
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

    // crack_key (the Option wrapper) collapses ScrambledUncracked and
    // Unencrypted alike to None; callers needing to tell them apart must use
    // crack_key_outcome. See docs/css-mod.md.
    #[test]
    fn crack_key_all_locked_collapses_to_none() {
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

    // The budget spans ALL extents, not per-extent: two extents summing past
    // the cap must still stop at 50_000 total reads.
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

    // crack_key scans sequentially from each extent's start_lba: reads must
    // begin at start_lba, start_lba+1, ...
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

    // A read error on a sector does NOT abort the scan: crack_key keeps
    // scanning subsequent sectors, and every sector in a small failing
    // extent is attempted.
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

    // Empty extents (no sectors) -> crack_key reads nothing and returns None;
    // a zero-sector extent must not read its start_lba.
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

    // No extents at all -> immediate None, zero reads.
    #[test]
    fn crack_key_no_extents_is_none() {
        let mut src = MockSource::new(0x30);
        let res = crack_key(&mut src, &[], 1);
        assert!(res.is_none());
        assert_eq!(src.reads.borrow().len(), 0);
    }

    // ── Scan-level Cracked branch + per-VTS re-crack success (audit §2 / §5 #8) ─

    // SCAN-LEVEL CRACKED: exercises the Cracked branch and crack_span
    // recording end-to-end, previously untested. See docs/css-mod.md.
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

    // `is_scrambled_uncracked` must be FALSE for Cracked/Unencrypted too, not
    // just TRUE for ScrambledUncracked, else clear/cracked discs alike get
    // routed to a hard error. See docs/css-mod.md.
    #[test]
    fn is_scrambled_uncracked_is_true_for_that_case_and_false_for_the_other_two() {
        let extents = [Extent {
            start_lba: 1000,
            sector_count: 50,
        }];

        // Cracked: a real crackable sector in an otherwise clear scan.
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11, 0x22, 0x33, 0x44, 0x55];
        let mut cracked_src = MockSource::new(0x00);
        cracked_src.crackable = Some((1003, crackable_sector(&title_key, &seed, 8)));
        let cracked = crack_key_outcome(&mut cracked_src, &extents, 4, None);
        assert!(
            matches!(cracked, CrackOutcome::Cracked(_)),
            "fixture malformed — expected a real crack, got {cracked:?}"
        );
        assert!(
            !cracked.is_scrambled_uncracked(),
            "a disc whose key WAS recovered is not scrambled-uncracked; saying \
             so aborts a rip that had its key in hand"
        );

        // Unencrypted: scramble flag never set across the scan.
        let mut clear_src = MockSource::new(0x00);
        let clear = crack_key_outcome(&mut clear_src, &extents, 4, None);
        assert!(
            matches!(clear, CrackOutcome::Unencrypted),
            "fixture malformed — expected Unencrypted, got {clear:?}"
        );
        assert!(
            !clear.is_scrambled_uncracked(),
            "a genuinely plaintext disc is not scrambled-uncracked; saying so \
             turns every unencrypted DVD into a hard CSS key error"
        );

        // ScrambledUncracked: scrambled sectors seen, no crackable crib.
        let mut locked_src = MockSource::new(0x30);
        let locked = crack_key_outcome(&mut locked_src, &extents, 4, None);
        assert!(
            matches!(locked, CrackOutcome::ScrambledUncracked),
            "fixture malformed — expected ScrambledUncracked, got {locked:?}"
        );
        assert!(
            locked.is_scrambled_uncracked(),
            "scrambled sectors seen and no key recovered IS the hard-failure case"
        );
    }

    // `resolve_dvd_title_key` is the SINGLE shared per-title CSS step both
    // read paths call. Crack path: a None-keyed MPEG-PS title with a
    // crackable sector installs a Css key that round-trips it.
    #[test]
    fn resolve_dvd_title_key_cracks_none_mpegps() {
        let title_key = [0x42, 0x13, 0x37, 0xBE, 0xEF];
        let seed = [0x11, 0x22, 0x33, 0x44, 0x55];
        let crackable = crackable_sector(&title_key, &seed, 8);
        let mut src = MockSource::new(0x00);
        src.crackable = Some((1003, crackable));
        let extents = [Extent {
            start_lba: 1000,
            sector_count: 50,
        }];
        let mut keys = crate::decrypt::DecryptKeys::None;
        resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            false,
            None,
        )
        .expect("crackable title resolves");
        match keys {
            crate::decrypt::DecryptKeys::Css { title_key: got } => {
                assert_eq!(got, title_key, "installed key must be the cracked key")
            }
            _ => panic!("expected Css key"),
        }
    }

    /// Hard-fail path: a scrambled-but-uncrackable `None`-keyed MPEG-PS title must
    /// return `CssKeyMissing`, never leave `keys` as `None` (which would mux
    /// scrambled bytes as plaintext — the 328k-decode-error corruption).
    #[test]
    fn resolve_dvd_title_key_scrambled_uncrackable_hard_fails() {
        let mut src = MockSource::new(0x00);
        src.lock_all = true; // every read CSS-locked → ScrambledUncracked
        let extents = [Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let mut keys = crate::decrypt::DecryptKeys::None;
        let err = resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            false,
            None,
        )
        .expect_err("scrambled-uncrackable must hard-fail");
        // The Error::CssKeyMissing flattens into io::Error carrying its E-code
        // (7023) in the message — assert that specific code survived.
        assert!(
            err.to_string()
                .contains(&format!("E{}", crate::error::E_CSS_KEY_MISSING)),
            "must surface CssKeyMissing (E{}), got: {err}",
            crate::error::E_CSS_KEY_MISSING
        );
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "keys must stay None on hard-fail (never a scrambled-passthrough key)"
        );
    }

    // The decrypted-HD-DVD regression end to end: its 0xBF RDI nav packs must
    // not flip `saw_scrambled` (no CSS exists on HD-DVD), else the scan
    // hard-fails a good disc with CssKeyMissing (E7023). See docs/css-mod.md.
    #[test]
    fn resolve_dvd_title_key_decrypted_hddvd_rdi_packs_scan_clean_no_e7023() {
        let mut src = MockSource::new(0x30); // 0x14 bits set…
        src.stream_id = crate::consts::pes_stream_id::PRIVATE_STREAM_2; // …but a 0xBF nav pack
        let extents = [Extent {
            start_lba: 0,
            sector_count: 64,
        }];
        let mut keys = crate::decrypt::DecryptKeys::None;
        resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            8,
            crate::disc::ContentFormat::MpegPs,
            false,
            None,
        )
        .expect("a decrypted HD-DVD's RDI nav packs are not CSS — the scan must not hard-fail");
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "no CSS key exists on an HD-DVD; keys must stay None and the title mux clean"
        );
    }

    /// `raw` is deliberate ciphertext passthrough: even a scrambled-uncrackable
    /// title must return `Ok` and leave `keys` untouched (`None`) — no crack, no
    /// hard-fail. This is the `--raw` guarantee.
    #[test]
    fn resolve_dvd_title_key_raw_skips_crack_and_never_fails() {
        let mut src = MockSource::new(0x00);
        src.lock_all = true;
        let extents = [Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let mut keys = crate::decrypt::DecryptKeys::None;
        resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            true, // raw
            None,
        )
        .expect("raw must never hard-fail");
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "raw must leave keys None (no descramble)"
        );
        assert!(
            src.reads.borrow().is_empty(),
            "raw must not read any sector for a crack"
        );
    }

    /// AACS gate: an MPEG-PS title carrying `Aacs` keys (HD-DVD `.evo`) must be
    /// left untouched — resolve only fires on `None` keys, never overwriting a
    /// real key set or cracking AACS ciphertext as CSS.
    #[test]
    fn resolve_dvd_title_key_leaves_aacs_untouched() {
        let mut src = MockSource::new(0x00);
        src.lock_all = true; // would hard-fail IF it ran the crack
        let extents = [Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let mut keys = crate::decrypt::DecryptKeys::Aacs {
            unit_keys: vec![(0, [0u8; 16])],
            read_data_key: None,
            format: crate::disc::ContentFormat::MpegPs,
        };
        resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            false,
            None,
        )
        .expect("AACS title must be left untouched, not cracked");
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::Aacs { .. }),
            "Aacs keys must survive unchanged"
        );
        assert!(
            src.reads.borrow().is_empty(),
            "must not read for a crack when keys are already Aacs"
        );
    }

    /// Clear DVD: a `None`-keyed MPEG-PS title with no scrambled sector stays
    /// `None` (a mux no-op) and returns `Ok` — genuinely-unencrypted DVDs pass.
    #[test]
    fn resolve_dvd_title_key_clear_dvd_stays_none() {
        let mut src = MockSource::new(0x00); // all-clear sectors
        let extents = [Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let mut keys = crate::decrypt::DecryptKeys::None;
        resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            false,
            None,
        )
        .expect("clear DVD passes");
        assert!(
            matches!(keys, crate::decrypt::DecryptKeys::None),
            "a clear DVD must keep None keys"
        );
    }

    // A cancelled crack (user Stop mid-scan) must surface as Halted, not be
    // misread from the truncated scan as Unencrypted or ScrambledUncracked.
    #[test]
    fn resolve_dvd_title_key_halt_surfaces_as_halted_not_a_verdict() {
        let mut src = MockSource::new(0x00);
        src.lock_all = true; // without the halt guard this would be ScrambledUncracked
        let extents = [Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let halt = crate::halt::Halt::new();
        halt.cancel(); // Stop already pressed
        let mut keys = crate::decrypt::DecryptKeys::None;
        let err = resolve_dvd_title_key(
            &mut src,
            &extents,
            &mut keys,
            4,
            crate::disc::ContentFormat::MpegPs,
            false,
            Some(&halt),
        )
        .expect_err("a cancelled crack must return an error");
        assert!(
            err.to_string()
                .contains(&format!("E{}", crate::error::E_HALTED)),
            "cancelled crack must surface Halted (E{}), got: {err}",
            crate::error::E_HALTED
        );
    }

    // CSS_ERROR WIRING: an all-locked synthetic ISO across MULTIPLE extents
    // must produce ScrambledUncracked, the signal disc/mod.rs converts into
    // css_error = CssKeyMissing. See docs/css-mod.md.
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

    // PER-VTS RE-CRACK SUCCESS: a re-crack over a DIFFERENT VTS's extents
    // finds that VTS's own key, proving it is genuinely re-derived, not
    // reused. See docs/css-mod.md.
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

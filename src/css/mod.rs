//! CSS (Content Scramble System) — DVD disc encryption.
//!
//! CSS uses a weak 40-bit LFSR stream cipher (broken since 1999).
//!
//! The title key is recovered keylessly: [`crack_key_outcome`] recovers it
//! directly from the scrambled data (see the [`keyless`] module), needing no
//! player keys, disc-key recovery, or external key file. Sectors are then
//! decrypted with [`descramble_sector`].
//!
//! Usage:
//! ```rust,ignore
//! match css::crack_key_outcome(reader, extents, batch, None) {
//!     CrackOutcome::Cracked(state) => css::descramble_sector(&state, &mut sector),
//!     _ => { /* unencrypted, uncrackable, unreadable, or halted */ }
//! }
//! ```

pub mod keyless;
pub mod lfsr;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::sector::SectorSource;

// Consecutive CSS-locked reads before the crack scan early-bails, instead of grinding the full
// 50_000-sector budget. Resets to 0 on any readable batch.
const CSS_LOCKED_BAIL: u32 = 64;

// Sector budget for one crack scan.
const MAX_TRIES: u32 = 50_000;

// Test-only: how many times descramble_region called the expensive re-crack.
// Pins a WORK bound (at most one per contiguous false-positive run).
#[cfg(test)]
thread_local! {
    static RECRACK_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

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
/// Scans a bounded sector budget across `extents` and returns the first sector
/// that yields a key — no player keys, no disc-key crack. Works on a live
/// drive and on disc images alike.
///
/// This convenience form runs to completion (no cancellation); callers needing a cancel token,
/// or the three-way [`CrackOutcome`], use [`crack_key_outcome`].
#[deprecated(
    since = "1.8.0",
    note = "collapses Halted/Unreadable to None, hiding a cancellation or a read \
            error as a plain crack failure; use crack_key_outcome and match on \
            CrackOutcome instead"
)]
pub fn crack_key(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
) -> Option<CssState> {
    crack_key_outcome(reader, extents, batch_sectors, None).into_state()
}

/// Outcome of a CSS crack scan. Only `Unencrypted` lets a caller treat the data as
/// clear; every other non-`Cracked` outcome MUST hard-error.
///
/// - `Cracked` — a scrambled sector yielded a title key.
/// - `Unencrypted` — sectors were read and none was scrambled.
/// - `ScrambledUncracked` — scrambled (or CSS-locked) sectors, no key recovered.
/// - `Unreadable` — no sector could be read; carries the first read error.
/// - `Halted` — cancelled by the halt token or a read returning `Halted`.
#[derive(Debug)]
#[non_exhaustive]
pub enum CrackOutcome {
    Cracked(CssState),
    Unencrypted,
    ScrambledUncracked,
    Unreadable(crate::error::Error),
    Halted,
}

impl CrackOutcome {
    /// The cracked `CssState`, if any. `None` for every other outcome. Lets the
    /// `Option`-returning wrappers stay thin.
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

// A DVD title's key when the caller supplied none: [`crack_title_key`] over its extents. A
// scrambled-but-uncrackable title is a hard, skippable per-title CssKeyMissing (another VTS may
// still crack).
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
    // CSS is DVD-Video's alone: an HD DVD `.evo` or a plain program stream is never cracked.
    if matches!(keys, crate::decrypt::DecryptKeys::None)
        && format == crate::disc::ContentFormat::DvdPs
    {
        *keys = crack_title_key(reader, extents, batch_sectors, halt)?;
    }
    Ok(())
}

// The SINGLE place every read path (disc, image, loose PS file) cracks a CSS title key: the
// keys, or `None` for clear content. `halt` lets /api/stop interrupt a long scan.
pub(crate) fn crack_title_key(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> std::io::Result<crate::decrypt::DecryptKeys> {
    match crack_key_scan(reader, extents, batch_sectors, halt) {
        CrackOutcome::Cracked(state) => Ok(crate::decrypt::DecryptKeys::Css {
            title_key: state.title_key,
        }),
        CrackOutcome::ScrambledUncracked => Err(crate::error::Error::CssKeyMissing.into()),
        CrackOutcome::Unreadable(e) => Err(e.into()),
        // A cancelled crack is a TRUNCATED scan, not a verdict.
        CrackOutcome::Halted => Err(crate::error::Error::Halted.into()),
        CrackOutcome::Unencrypted => Ok(crate::decrypt::DecryptKeys::None),
    }
}

/// Where a 13818-1 pack's first PES header flags byte sits, `Some` only when that PES is
/// scrambled: the ONE CSS pack test, disc and file alike. Leading packets with no PES header (a
/// system header, a map, padding) are walked by their length; only the first PES is judged.
pub(crate) fn scrambled_at(sector: &[u8]) -> Option<usize> {
    if sector.len() < 2048 || sector[..4] != PACK_START || sector[4] >> 6 != 0b01 {
        return None;
    }
    let mut p = 0x0E + usize::from(sector[0x0D] & 0x07);
    loop {
        let h = sector.get(p..p + 7)?;
        if h[..3] != [0, 0, 1] {
            return None;
        }
        match h[3] {
            // MS-31: "if (stream_id != program_stream_map && stream_id != padding_stream &&
            // stream_id != private_stream_2 && stream_id != ECM && stream_id != EMM && …"
            // (0xBB, the system header, is no PES packet at all).
            0xBB | 0xBC | 0xBE | 0xBF | 0xF0 | 0xF1 | 0xF2 | 0xF8 | 0xFF => {
                p += 6 + usize::from(u16::from_be_bytes([h[4], h[5]]));
            }
            0xBD..=0xFE => {
                let at = p + 6;
                // CSS leaves bytes before 0x80 clear; a header past them cannot be read.
                return (at < 0x80 && h[6] >> 6 == 0b10 && (h[6] >> 4) & 0x03 != 0).then_some(at);
            }
            _ => return None,
        }
    }
}

// Run `f` on `sector` with its flags byte `at` presented at 0x14, where the LFSR and the
// keyless crack read it; both bytes are put back after (a stuffed pack, m1/m2).
fn with_flags_at_0x14<T>(sector: &mut [u8], at: usize, f: impl FnOnce(&mut [u8]) -> T) -> T {
    if at == 0x14 {
        return f(sector);
    }
    let b14 = sector[0x14];
    sector[0x14] = sector[at];
    let out = f(sector);
    sector[at] = sector[0x14];
    sector[0x14] = b14;
    out
}

#[cfg(test)]
thread_local! {
    /// Crack scans run on this thread (test-only: the mpg:// path must scan once).
    pub(crate) static CRACK_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// The crack scan over `extents`, judging each sector with [`scrambled_at`].
fn crack_key_scan(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    batch_sectors: u16,
    halt: Option<&crate::halt::Halt>,
) -> CrackOutcome {
    #[cfg(test)]
    CRACK_SCANS.with(|n| n.set(n.get() + 1));
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
    // Sectors actually inspected: zero means no verdict (never Unencrypted).
    let mut inspected = 0u32;
    // Sectors lost to non-locked read failures: more lost than inspected is no clear verdict.
    let mut failed = 0u32;
    let mut first_err: Option<crate::error::Error> = None;

    'outer: for (extent_idx, ext) in extents.iter().enumerate() {
        let mut i = 0u32;
        while i < ext.sector_count && tried < MAX_TRIES {
            // Cooperative cancellation — poll once per batch, the same cadence
            // sweep/patch use, so a Stop / watchdog can interrupt the scan.
            if halt.is_some_and(|h| h.is_cancelled()) {
                return CrackOutcome::Halted;
            }
            // Saturating: `start_lba` comes from a crafted IFO/UDF extent.
            let lba = ext.start_lba.saturating_add(i);
            // Liveness beacon: a long scan over a damaged disc stays visible.
            // The heartbeat is time-throttled; only when it actually beats do
            // we emit the crack-specific context (tried/lba/extent_idx).
            if hb.tick(tried as u64, MAX_TRIES as u64) {
                tracing::debug!(
                    target: "freemkv::heartbeat",
                    phase = "css_crack",
                    tried,
                    lba,
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
            match reader.read_sectors(lba, n as u16, &mut buf[..want], true) {
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
                        inspected += 1;
                        let sect = &buf[s * 2048..(s + 1) * 2048];
                        // HARDENED pack-gated check: a clear stub sector with
                        // stray bits at 0x14 must NOT count as scramble evidence,
                        // or an unencrypted title falsely reports E7023.
                        if let Some(at) = scrambled_at(sect) {
                            saw_scrambled = true;
                            // A stuffed pack's flags sit past 0x14 (m1); a DVD-Video pack's never do.
                            let key = if at == 0x14 {
                                keyless::crack_title_key(sect)
                            } else {
                                let mut copy = sect.to_vec();
                                with_flags_at_0x14(&mut copy, at, |c| keyless::crack_title_key(c))
                            };
                            if let Some(key) = key {
                                return CrackOutcome::Cracked(CssState {
                                    title_key: key,
                                    crack_span,
                                });
                            }
                        }
                        if tried >= MAX_TRIES {
                            break 'outer;
                        }
                    }
                }
                // A drive-level Stop ends the scan now — it is not a failed batch.
                Err(crate::error::Error::Halted) => return CrackOutcome::Halted,
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
                        failed += n;
                        first_err.get_or_insert(e);
                    }
                }
            }
            i += advance;
        }
    }

    // ENCRYPTED-but-uncracked if a scrambled sector was seen or reads were CSS-locked. Nothing
    // inspected, or more unreadable than inspected, is no verdict: fail closed with the read
    // error (else as uncracked).
    if saw_scrambled || saw_locked {
        CrackOutcome::ScrambledUncracked
    } else if tried > 0 && (inspected == 0 || failed > inspected) {
        first_err.map_or(CrackOutcome::ScrambledUncracked, CrackOutcome::Unreadable)
    } else {
        CrackOutcome::Unencrypted
    }
}

/// Descramble a single CSS-encrypted sector in place.
///
/// A no-op unless the sector is a scrambled MPEG-2 PS PACK ([`is_scrambled_pack`]): byte 0x14
/// alone is unreliable outside a pack. Making the guard part of the function, rather than
/// something each caller must remember, is what keeps the safe path the easy one.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    let Some(at) = scrambled_at(sector) else {
        return;
    };
    with_flags_at_0x14(sector, at, |s| lfsr::descramble_sector(&state.title_key, s));
}

// How many mismatches a failed re-crack's "false positive" verdict covers before the next retry —
// bounded so a genuine key change right after a false-positive run is still picked up.
const RECRACK_RETRY_EVERY: u32 = 16;

/// Descramble a whole CSS buffer in place, re-cracking the title key on a VOB region boundary.
/// `title_key` is a CACHE of the last crack: validated against the clear-header crib on every
/// scrambled sector and re-cracked on a miss. A crib-less sector rides the cached key.
///
/// # Errors
///
/// Never returns `Err` and always returns `Ok(0)` (CSS drops no sectors); `Result` only
/// matches the decrypt seam this is dispatched from (see [`crate::decrypt::decrypt_sectors`]).
pub fn descramble_region(buf: &mut [u8], title_key: &mut [u8; 5]) -> crate::error::Result<usize> {
    // Consecutive crib mismatches since the last re-crack attempt (0 = none
    // pending). Reset by a validated cache hit; a fresh run always attempts
    // on its first mismatch, then at most once every RECRACK_RETRY_EVERY.
    let mut mismatches_since_attempt: u32 = 0;
    // The pack test, NOT raw byte 0x14: this sees arbitrary regions (IFO/UDF/ISO 9660).
    // Measured: an IFO misread by 0x14 alone was destroyed, dropping titles 38→10.
    for chunk in buf.chunks_mut(2048) {
        let Some(at) = (chunk.len() >= 2048).then(|| scrambled_at(chunk)).flatten() else {
            continue;
        };
        with_flags_at_0x14(chunk, at, |chunk| {
            descramble_one(chunk, title_key, &mut mismatches_since_attempt)
        });
    }
    Ok(0)
}

// One scrambled pack (flags at 0x14), validated against its crib and re-cracked on a miss.
fn descramble_one(chunk: &mut [u8], title_key: &mut [u8; 5], mismatches_since_attempt: &mut u32) {
    {
        let crib = keyless::attack_crib(chunk);
        // Snapshot the ciphertext (chunk is exactly 2048 here) only when there is
        // a crib to validate against, so the common cache-hit path costs no
        // per-sector heap allocation.
        let mut original = [0u8; 2048];
        if crib.is_some() {
            original.copy_from_slice(chunk);
        }
        lfsr::descramble_sector(title_key, chunk);
        let Some(crib) = crib else { return };
        if chunk[0x80..0x80 + 10] == crib[..] {
            *mismatches_since_attempt = 0; // validated: cached key still matches
            return;
        }
        *mismatches_since_attempt += 1;
        if *mismatches_since_attempt % RECRACK_RETRY_EVERY != 1 {
            // Within a suppressed run, not yet due for its periodic retry —
            // keep the cached key (already applied above).
            return;
        }
        // Due for an attempt: either the first mismatch of a run, or a
        // periodic retry into an ongoing one. Restore the ciphertext and
        // crack this sector's own key.
        chunk.copy_from_slice(&original);
        #[cfg(test)]
        RECRACK_ATTEMPTS.with(|c| c.set(c.get() + 1));
        match keyless::crack_title_key(chunk) {
            Some(fresh) => {
                *title_key = fresh;
                lfsr::descramble_sector(title_key, chunk);
                *mismatches_since_attempt = 0;
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

/// Whether bits 4-5 of the sub-header byte 0x14 are set. NOTHING MORE.
///
/// This is deliberately NOT called `is_scrambled`: byte 0x14 is only meaningful inside an
/// MPEG-2 PS pack, and treating the flag alone as proof of scrambling corrupted a real disc.
///
/// **Callers want [`is_scrambled_pack`].** It also requires the pack start
/// code, which every genuinely scrambled VOB sector carries and no IFO sector
/// does. This stays public only because an integration test asserts the flag
/// extraction directly; it has no production callers.
#[doc(hidden)]
pub fn has_scramble_flag_bits(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

/// The 4-byte MPEG-2 Program Stream pack-start code (`00 00 01 BA`) every DVD
/// video sector opens with. CSS leaves the clear header (`0x00..0x80`)
/// untouched, so this signature survives scrambling.
pub(crate) const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];

/// Check if a sector is a CSS-scrambled pack: a 13818-1 pack whose first PES header (past any
/// pack stuffing and packets with no PES header: system header, map, padding, nav) is MPEG-2
/// and carries scramble bits. The one test the crack scan and every descramble share, disc and
/// file alike. An IFO/UDF sector, an 11172-1 pack and an MPEG-1 PES are never scrambled.
pub fn is_scrambled_pack(sector: &[u8]) -> bool {
    scrambled_at(sector).is_some()
}

/// Test fixture: the DVD-Video pack layout every real VOB sector has (13818-1 pack header, no
/// stuffing, a `stream_id` PES at 0x0E with MPEG-2 flags at 0x14), keeping its scramble bits.
#[cfg(test)]
pub(crate) fn dvd_pack_header(sector: &mut [u8], stream_id: u8) {
    sector[..4].copy_from_slice(&PACK_START);
    sector[4] = 0x44;
    sector[0x0D] = 0xF8;
    sector[0x0E..0x11].copy_from_slice(&[0, 0, 1]);
    sector[0x11] = stream_id;
    sector[0x12..0x14].copy_from_slice(&0x07ECu16.to_be_bytes());
    sector[0x14] = 0x80 | (sector[0x14] & 0x30);
}

#[cfg(test)]
#[allow(deprecated)] // this module exercises crack_key itself, deliberately
#[path = "mod_tests.rs"]
mod tests;

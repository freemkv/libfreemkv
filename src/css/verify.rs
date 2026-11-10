//! CSS title-key verification: does a key not cracked from these sectors open them? Copies of
//! scrambled packs are descrambled and checked against structure fixed by the clear bytes before
//! 0x80 or by MPEG syntax:
//!
//! - **Next packet** (13818-1 §2.5.3): a PES whose clear length ends at or past 0x80 inside the
//!   pack must be followed by `00 00 01` with `stream_id` ≥ 0xBC and a length that fits.
//! - **AC-3 / DTS** (private stream 1): a frame the clear `first_access_unit_pointer` (counted
//!   from the field's last byte) places at or past 0x80 opens with `0B 77` / `7F FE 80 01`.
//! - **MPEG video** (13818-2 Table 6-1): every decrypted start code has a valid value; with
//!   `data_alignment_indicator` a payload starting at or past 0x80 opens with one.
//! - **Crib** ([`keyless::attack_crib`]): a match passes; a mismatch is no verdict.
//!
//! A wrong key passes one check with at most ~1e-4 (video start code); a pass needs
//! [`MIN_PASSES`] passing checks and none failing, so false accept is below ~1e-16.

use super::{keyless, lfsr, scrambled_at, with_flags_at_0x14};

/// Passing checks a key needs (with no failing one) to verify.
pub(crate) const MIN_PASSES: usize = 4;

/// A key's verdict over a sample of scrambled packs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// At least [`MIN_PASSES`] checks passed and none failed.
    Pass,
    /// A check failed: the key does not open these packs.
    Fail,
    /// Too little to judge; the key is not used.
    Inconclusive,
}

/// Verify `key` on the scrambled packs among `sectors` (2048-byte chunks; clear ones are
/// skipped). Stops at the first failing check.
pub(crate) fn verify_key<'a>(
    key: &[u8; 5],
    sectors: impl IntoIterator<Item = &'a [u8]>,
) -> Verdict {
    let mut passes = 0;
    for sector in sectors {
        let Some(at) = (sector.len() >= 2048)
            .then(|| scrambled_at(sector))
            .flatten()
        else {
            continue;
        };
        let mut copy = [0u8; 2048];
        copy.copy_from_slice(&sector[..2048]);
        let crib = with_flags_at_0x14(&mut copy, at, |c| {
            let crib = keyless::attack_crib(c);
            lfsr::descramble_sector(key, c);
            crib
        });
        if crib.is_some_and(|crib| copy[0x80..0x8A] == crib) {
            passes += 1;
        }
        match check_pack(&copy, at) {
            Some((n, true)) => passes += n,
            Some((_, false)) => return Verdict::Fail,
            None => {}
        }
    }
    if passes >= MIN_PASSES {
        Verdict::Pass
    } else {
        Verdict::Inconclusive
    }
}

// The structure checks on one descrambled pack whose first PES flags byte is at `at`:
// `Some((passed, true))`, `Some((_, false))` on any failed check, `None` when none applies.
fn check_pack(s: &[u8; 2048], at: usize) -> Option<(usize, bool)> {
    const CLEAR: usize = 0x80;
    let stream_id = s[at - 3];
    let pes_end = at + usize::from(u16::from_be_bytes([s[at - 2], s[at - 1]]));
    let payload = at + 3 + usize::from(s[at + 2]);
    // A length past the pack is damaged metadata, not a verdict.
    if pes_end > s.len() || payload > pes_end {
        return None;
    }
    let mut passed = 0;
    let mut tally = |ok: bool| {
        passed += 1;
        ok
    };
    // The packet after this PES (usually padding) must start where the length says.
    if (CLEAR..=s.len() - 6).contains(&pes_end) {
        let next = &s[pes_end..];
        let len = usize::from(u16::from_be_bytes([next[4], next[5]]));
        let ok = next[..3] == [0, 0, 1] && next[3] >= 0xBC && pes_end + 6 + len <= s.len();
        if !tally(ok) {
            return Some((0, false));
        }
    }
    match stream_id {
        0xBD if payload + 4 <= CLEAR => {
            let sync: &[u8] = match s[payload] {
                0x80..=0x87 => &[0x0B, 0x77],
                0x88..=0x8F => &[0x7F, 0xFE, 0x80, 0x01],
                _ => &[],
            };
            let frames = s[payload + 1];
            let pointer = usize::from(u16::from_be_bytes([s[payload + 2], s[payload + 3]]));
            let au = payload + 3 + pointer;
            if !sync.is_empty()
                && frames != 0
                && pointer != 0
                && au >= CLEAR
                && au + sync.len() <= pes_end
                && !tally(s[au..au + sync.len()] == *sync)
            {
                return Some((0, false));
            }
        }
        0xE0..=0xEF => {
            let valid = |code: u8| !matches!(code, 0xB0 | 0xB1 | 0xB6 | 0xB9..=0xFF);
            if s[at] & 0x04 != 0
                && payload >= CLEAR
                && payload + 4 <= pes_end
                && !tally(s[payload..payload + 3] == [0, 0, 1] && valid(s[payload + 3]))
            {
                return Some((0, false));
            }
            // Start codes whose value byte is decrypted (a clear one proves nothing).
            let from = payload.max(CLEAR - 3);
            let mut found = false;
            for i in from..pes_end.saturating_sub(3) {
                if s[i..i + 3] == [0, 0, 1] {
                    if !valid(s[i + 3]) {
                        return Some((0, false));
                    }
                    found = true;
                }
            }
            if found {
                tally(true);
            }
        }
        _ => {}
    }
    (passed > 0).then_some((passed, true))
}

#[cfg(test)]
#[path = "verify_tests.rs"]
mod tests;

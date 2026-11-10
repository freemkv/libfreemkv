//! CSS title-key verification: does a key that was not cracked from these sectors open them?
//!
//! A candidate key descrambles copies of scrambled packs, and the plaintext is checked against
//! structure whose expected value comes from the clear bytes before 0x80 or from MPEG syntax:
//!
//! - **Next packet** (any stream; ISO 13818-1 §2.5.3, DVD-Video 2048-byte packs): when the
//!   clear `PES_packet_length` ends the PES at or past 0x80 and before the pack's end, a packet
//!   start code (`00 00 01`, `stream_id` ≥ 0xBC, usually a padding packet) must follow there,
//!   and its own length must fit in the pack.
//! - **AC-3 / DTS** (private stream 1, DVD-Video substreams 0x80..0x87 / 0x88..0x8F): the clear
//!   substream header gives `number_of_frame_headers` and `first_access_unit_pointer`, counted
//!   from the pointer field's last byte. A frame starting at or past 0x80 must open with the
//!   sync word, AC-3 `0B 77` (ETSI TS 102 366 §4.4.1), DTS `7F FE 80 01`.
//! - **MPEG video** (`E0..EF`; ISO 13818-2 Table 6-1): every start code in the decrypted PES
//!   payload must have a valid value (not reserved 0xB0/0xB1/0xB6, not a system code
//!   0xB9..0xFF). With `data_alignment_indicator` set (13818-1 §2.4.3.7, alignment type 01) a
//!   payload starting at or past 0x80 must open with a start code.
//! - **Crib** ([`keyless::attack_crib`]): a match passes; a mismatch is no verdict, since a
//!   crib is a prediction and often wrong on menus.
//!
//! Sub-picture and LPCM packs carry no such check beyond the next-packet one.
//!
//! False accept: a wrong key leaves pseudo-random plaintext. A check passes on it with at most
//! about 1e-4 (a valid start code somewhere in ~1900 video bytes: 1900 × 2^-24 × 0.84); the
//! others are 2^-16 (AC-3) or far less. A pass needs [`MIN_PASSES`] passing checks and no
//! failing one, so a wrong key is accepted with probability below about 1e-16, and in practice
//! far less: most packs it would pass on also carry a check it fails.

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

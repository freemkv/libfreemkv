//! Proof on first read (KU §2.4): the decrypting readers prove a held key on a piece that
//! `resolve` could not prove up front, the first time one of its encrypted units is read.
//!
//! A partner is another readable non-segment `Enc` unit of the piece, from the batch, else one
//! backward and one forward side read (`recovery = false`, never retried, failures swallowed).
//! A held key opening the unit and a partner is `Proven`; with no partner, `Provisional` until
//! the next readable unit confirms it or stops (key conflict). A unit no held key opens stops
//! E7022 / E7032, unless a held key opens a batch partner: then it is damage, blanked.

use super::{Proof, ProofCache, ResolvedKeySet, StopKind};
use crate::aacs::content::{
    ALIGNED_UNIT_LEN, aacs_unit_encrypted, aacs_unit_on_grid, clear_copy_permission_indicator,
    decrypt_unit, is_clean,
};
use crate::consts::SECTOR_BYTES;
use crate::disc::ContentFormat;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::whole_disc::UNIT;

/// Units a side read covers at most, each way (KU §2.4: "up to 32 units").
const SIDE_UNITS: u64 = 32;

/// The on-arrival proof a set's reader carries (KU §2.4). Holds the set's lazy and clear
/// pieces, its base-key count, the FMTS segment ranges and the set's [`ProofCache`].
pub(crate) struct Arrival {
    // (start, end, anchor, piece index), sorted by start, disjoint.
    spans: Vec<(u32, u32, u64, usize)>,
    // (piece id, candidate slot).
    pieces: Vec<(u32, Option<usize>)>,
    base: usize,
    segments: Vec<(u32, u32)>,
    cache: ProofCache,
    format: ContentFormat,
    stop: StopKind,
}

impl Arrival {
    pub(crate) fn new(set: &ResolvedKeySet, stop: StopKind) -> Self {
        let i = &set.0;
        let mut spans = Vec::new();
        let mut pieces = Vec::with_capacity(i.arrival.len());
        for (p, piece) in i.arrival.iter().enumerate() {
            pieces.push((piece.id, piece.candidate));
            for &(s, n, anchor) in &piece.spans {
                spans.push((s, s.saturating_add(n), anchor, p));
            }
        }
        spans.sort_unstable_by_key(|s| s.0);
        let mut segments = i.segments.clone();
        segments.sort_unstable();
        Arrival {
            spans,
            pieces,
            base: i.pool.len(),
            segments,
            cache: i.proofs.clone(),
            format: i.content_format,
            stop,
        }
    }

    // The arrival span holding `lba`: (start, end, anchor, piece).
    fn span_at(&self, lba: u32) -> Option<(u32, u32, u64, usize)> {
        let i = self.spans.partition_point(|s| s.0 <= lba).checked_sub(1)?;
        let s = self.spans[i];
        (lba < s.1).then_some(s)
    }

    fn in_segment(&self, lba: u32) -> bool {
        let i = self.segments.partition_point(|s| s.0 <= lba);
        i > 0 && lba < self.segments[i - 1].1
    }

    fn opens(&self, unit: &[u8], key: &[u8; 16]) -> bool {
        let mut u = unit.to_vec();
        decrypt_unit(&mut u, key);
        is_clean(&u, self.format)
    }

    /// Is `lba` in a piece this proof covers?
    pub(crate) fn covers(&self, lba: u32) -> bool {
        self.span_at(lba).is_some()
    }

    // A readable, non-segment encrypted unit of piece `piece` (a partner candidate).
    fn is_partner(&self, lba: u32, unit: &[u8], piece: usize) -> bool {
        // KS-4 [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is used as the seed" —
        // a unit whose seed is damaged (no TS sync) opens under no key: damage, not a partner.
        unit.len() == ALIGNED_UNIT_LEN
            && self.span_at(lba).is_some_and(|s| s.3 == piece)
            && !self.in_segment(lba)
            && aacs_unit_encrypted(unit, self.format)
            && aacs_unit_on_grid(unit, self.format)
    }

    fn stop(&self, lba: u32, why: &'static str) -> Error {
        let err = self.stop.error();
        tracing::error!(
            target: "freemkv::keys",
            lba,
            code = err.code(),
            why,
            "a readable encrypted unit opens with no held key; stopping (output stays .partial)"
        );
        err
    }

    /// Prove and decrypt the lazy-piece units of `buf`, read at `lba` on the unit grid.
    /// Keyed pieces are left to the key map; clear and segment units are untouched. Returns
    /// how many damaged units it blanked (see [`is_damage`](Self::is_damage)).
    pub(crate) fn process(
        &self,
        inner: &mut dyn SectorSource,
        lba: u32,
        buf: &mut [u8],
        unit_keys: &[(u32, [u8; 16])],
    ) -> Result<usize> {
        if self.spans.is_empty() {
            return Ok(0);
        }
        let mut blanked = 0;
        let n_units = buf.len() / ALIGNED_UNIT_LEN;
        for u in 0..n_units {
            let at = lba.saturating_add(u as u32 * UNIT as u32);
            let Some(span) = self.span_at(at) else {
                continue;
            };
            let range = u * ALIGNED_UNIT_LEN..(u + 1) * ALIGNED_UNIT_LEN;
            if self.in_segment(at) || !aacs_unit_encrypted(&buf[range.clone()], self.format) {
                continue;
            }
            let (id, candidate) = self.pieces[span.3];
            let slot = match self.cache.get(id) {
                Some(Proof::Proven(s)) => s,
                Some(Proof::Provisional(s, from)) => {
                    // KU §2.4: the next readable unit confirms a provisional key or stops;
                    // one no held key opens while the key opens a batch partner is damage.
                    if self.opens(&buf[range.clone()], &unit_keys[s].1) {
                        if at != from {
                            self.cache.set(id, Proof::Proven(s));
                        }
                        s
                    } else if self.is_damage(
                        &buf[range.clone()],
                        &self.batch_partners(lba, buf, u, span.3),
                        candidate,
                        &[s],
                        unit_keys,
                    ) {
                        buf[range].fill(0);
                        blanked += 1;
                        continue;
                    } else {
                        return Err(self.stop(at, "provisional key contradicted"));
                    }
                }
                None => match self.prove(inner, lba, buf, u, span, candidate, unit_keys)? {
                    Some(s) => s,
                    None => {
                        buf[range].fill(0);
                        blanked += 1;
                        continue;
                    }
                },
            };
            self.decrypt_proven(&mut buf[range], &unit_keys[slot].1);
        }
        if blanked > 0 {
            tracing::warn!(
                target: "freemkv::keys",
                lba,
                units = blanked,
                "damaged AACS unit (no held key opens it, one opens its partners): blanked"
            );
        }
        Ok(blanked)
    }

    // The held keys, the piece's candidate first.
    fn held(&self, candidate: Option<usize>) -> impl Iterator<Item = usize> {
        let rest = (0..self.base).filter(move |&s| Some(s) != candidate);
        candidate.into_iter().chain(rest)
    }

    /// Is `unit` damage rather than a key failure? KS-4 \[BD\] §3.10.1: "The first 16 bytes of
    /// each Aligned Unit is used as the seed", and a garbled head can keep its CPI flag and TS
    /// sync. A wrong key opens no unit of the piece; damage fails one. So: no held key opens
    /// it and one of `witnesses` opens a batch partner (no side read: KU §2.4 stops an
    /// unopenable unit before any). A blanked unit proves nothing itself.
    fn is_damage(
        &self,
        unit: &[u8],
        partners: &[Vec<u8>],
        candidate: Option<usize>,
        witnesses: &[usize],
        unit_keys: &[(u32, [u8; 16])],
    ) -> bool {
        let opens = |u: &[u8], s: usize| self.opens(u, &unit_keys[s].1);
        !self.held(candidate).any(|s| opens(unit, s))
            && partners
                .iter()
                .any(|p| witnesses.iter().any(|&s| opens(p, s)))
    }

    // Unit `u`'s partners already in the batch, both ways (KU §2.4 step 1).
    fn batch_partners(&self, lba: u32, buf: &[u8], u: usize, piece: usize) -> Vec<Vec<u8>> {
        buf.chunks(ALIGNED_UNIT_LEN)
            .enumerate()
            .filter(|&(j, c)| {
                j != u && self.is_partner(lba.saturating_add(j as u32 * UNIT as u32), c, piece)
            })
            .map(|(_, c)| c.to_vec())
            .collect()
    }

    // Decrypt one unit with its proven key and mark it plaintext.
    fn decrypt_proven(&self, unit: &mut [u8], key: &[u8; 16]) {
        decrypt_unit(unit, key);
        // KS-5 [BD] §3.10.2: CPI "shall be set to 00₂ if the data is not encrypted" — a unit
        // freemkv decrypted, on-arrival proven included (KU §5.4).
        clear_copy_permission_indicator(unit, self.format);
    }

    // Find the held key for unit `u` of `buf` (KU §2.4 steps 1–2) and record it. `None`:
    // no held key opens U but one opens a partner, so U is damage (`is_damage`), not proof.
    #[allow(clippy::too_many_arguments)]
    fn prove(
        &self,
        inner: &mut dyn SectorSource,
        lba: u32,
        buf: &[u8],
        u: usize,
        span: (u32, u32, u64, usize),
        candidate: Option<usize>,
        unit_keys: &[(u32, [u8; 16])],
    ) -> Result<Option<usize>> {
        let at = lba.saturating_add(u as u32 * UNIT as u32);
        let unit = &buf[u * ALIGNED_UNIT_LEN..(u + 1) * ALIGNED_UNIT_LEN];
        // The held keys that open U, the candidate first. None: damage, or a wrong key.
        let openers: Vec<usize> = self
            .held(candidate)
            .filter(|&s| self.opens(unit, &unit_keys[s].1))
            .collect();
        if openers.is_empty() {
            let held: Vec<usize> = self.held(candidate).collect();
            let partners = self.batch_partners(lba, buf, u, span.3);
            if self.is_damage(unit, &partners, candidate, &held, unit_keys) {
                return Ok(None);
            }
            return Err(self.stop(at, "no held key opens the unit"));
        }
        // Step 1: partners already in the batch, both ways; then the side reads, also when
        // no batch partner opens (they may be damaged heads that kept their sync).
        let find = |partners: &[Vec<u8>]| {
            let opens = |s: usize| partners.iter().any(|p| self.opens(p, &unit_keys[s].1));
            openers.iter().copied().find(|&s| opens(s))
        };
        let batch = self.batch_partners(lba, buf, u, span.3);
        let (mut proven, mut any_partner) = (find(&batch), !batch.is_empty());
        if proven.is_none() {
            let side = self.side_partners(inner, at, span);
            any_partner |= !side.is_empty();
            proven = find(&side);
        }
        let id = self.pieces[span.3].0;
        match proven {
            Some(s) => {
                self.cache.set(id, Proof::Proven(s));
                Ok(Some(s))
            }
            None if !any_partner => {
                // No readable partner: provisional one-unit proof, false-accept ≈ pool × 1e-5.
                tracing::debug!(target: "freemkv::keys", lba = at, "provisional one-unit proof");
                self.cache.set(id, Proof::Provisional(openers[0], at));
                Ok(Some(openers[0]))
            }
            None => Err(self.stop(at, "no held key opens the unit and a partner")),
        }
    }

    // Step 1.2: at most one backward and one forward side read inside U's span, each up to
    // 32 units, `recovery = false`, never retried. A failure only means "no partner there".
    fn side_partners(
        &self,
        inner: &mut dyn SectorSource,
        at: u32,
        (start, end, anchor, piece): (u32, u32, u64, usize),
    ) -> Vec<Vec<u8>> {
        // KS-1 [BD] §3.10.1: "encryption is applied to every Aligned Unit in the file"; units
        // on the file's own grid (KS-2, KS-7 Informative: sectors of a unit are contiguous).
        let first = crate::whole_disc::unit_head(start, anchor);
        let last_end = anchor + (end as u64).saturating_sub(anchor) / UNIT * UNIT;
        let at = at as u64;
        let back = (first.max(at.saturating_sub(SIDE_UNITS * UNIT)), at);
        let fwd = (at + UNIT, last_end.min(at + UNIT + SIDE_UNITS * UNIT));
        for (from, to) in [back, fwd] {
            if to <= from {
                continue;
            }
            let found = self.side_read(inner, from, to, piece);
            if !found.is_empty() {
                return found;
            }
        }
        Vec::new()
    }

    fn side_read(
        &self,
        inner: &mut dyn SectorSource,
        from: u64,
        to: u64,
        piece: usize,
    ) -> Vec<Vec<u8>> {
        let (Ok(lba), Ok(count)) = (u32::try_from(from), u16::try_from(to - from)) else {
            return Vec::new();
        };
        let mut side = vec![0u8; count as usize * SECTOR_BYTES];
        match inner.read_sectors(lba, count, &mut side, false) {
            Ok(n) => side[..n.min(side.len())]
                .chunks(ALIGNED_UNIT_LEN)
                .enumerate()
                .filter(|&(j, c)| self.is_partner(lba + j as u32 * UNIT as u32, c, piece))
                .map(|(_, c)| c.to_vec())
                .collect(),
            Err(e) => {
                tracing::debug!(target: "freemkv::keys", lba, error = %e, "side read found no partner");
                Vec::new()
            }
        }
    }
}

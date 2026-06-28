//! Universal post-read verify gate.
//!
//! `read() -> verify() -> sign-off`. A unit that fails verify is treated EXACTLY
//! like a bad read (the caller re-marks its disc range pending/lost). Reads are
//! disc-absolute, but the only alignment at which the AACS CPI flag and the
//! decrypt-verify are meaningful is each clip's FILE-anchored 6144-byte unit
//! grid (clips can start off the 6144 grid and fragment across UDF extents). So
//! this gate BUFFERS the disc-absolute read stream and re-ALIGNS it into
//! clip-file units, then applies the standards-correct
//! [`crate::aacs::unit_is_clean_ts`] gate (libaacs `_verify_ts`, all-32 syncs).
//!
//! FAIL-SAFE CONTRACT (this sits in the middle of every read, so it must never
//! break a good read): the gate can ONLY downgrade a unit it is *confident* is
//! bad — a flagged-encrypted, fully-buffered, full-size unit that no held key
//! and no freshly-fetched key can decrypt to clean TS. EVERY other situation —
//! the [`POST_READ_VERIFY`] switch off, a non-AACS disc, no keys, an enumeration
//! failure, a partial tail unit, a unit whose key we simply lack (no key_fetch),
//! an evicted partial — SKIPS, leaving the read byte-for-byte as it is today.
//!
//! Bus encryption (AACS 2.x `read_data_key`) is deliberately NOT handled here:
//! it is a drive<->host transport layer stripped during drive auth. On the
//! unlocked drives this runs against it is off; if it were on and unhandled we
//! would fail at the read/auth stage long before reaching this gate, so by here
//! the bytes are content-layer only.

use std::collections::{HashMap, VecDeque};

use crate::aacs::{self, ALIGNED_UNIT_LEN};
use crate::consts::SECTOR_BYTES_U64;
use crate::decrypt::DecryptKeys;
use crate::sector::KeyFetch;

/// Master kill-switch for the post-read verify gate. Hardcoded `true`. Flip to
/// `false` and the gate is inert: [`UnitVerifier::new`] returns `None`, nothing
/// is ever buffered, verified, or downgraded, and rip behavior is byte-for-byte
/// what it is today. The single lever to pull the whole feature.
pub const POST_READ_VERIFY: bool = true;

/// Cap on in-flight partial units. Sequential sweeps complete units almost
/// immediately, so partials only accumulate at damage-jump skips (whose sectors
/// never arrive). When the cap is hit the oldest partial is evicted and simply
/// goes unverified — fail-safe. Bounds memory at `MAX_INFLIGHT_UNITS * 6144`.
const MAX_INFLIGHT_UNITS: usize = 4096; // ~24 MiB ceiling

/// Cap on key-fetch invocations across the verifier's life. A fetched key is
/// cached and reused for every later unit of the same CPS unit, so in practice
/// one fetch resolves all orphan units; the cap is a runaway backstop only.
const MAX_FETCH_CALLS: u32 = 8;

/// The stream container of an AACS clip — selects the post-decrypt structural
/// check the verify gate applies. This is the extension seam: the AACS crypto is
/// container-agnostic, only the "is this clean?" check differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerKind {
    /// BD/UHD `.m2ts` / `.ssif` — MPEG-2 transport stream (all-32 TS syncs).
    #[default]
    Ts,
    /// HD-DVD `.evo` — MPEG-2 program stream (pack-start `00 00 01 BA`).
    /// NOT yet enabled by enumeration; present so adding HD-DVD is a one-mapping
    /// change. See [`crate::aacs::unit_is_clean_ps`] for the (unvalidated) check.
    Ps,
}

/// A clip's on-disc layout: declared file size, its absolute disc extents in
/// FILE order, and its stream container. `extents` is `(disc_lba, byte_len)`;
/// the verifier reuses exactly the same `(abs_lba, byte_len)` extents the
/// extractor enumerates.
#[derive(Debug, Clone)]
pub struct ClipLayout {
    pub size: u64,
    pub extents: Vec<(u32, u32)>,
    pub container: ContainerKind,
}

/// One extent placed in the (disc-LBA -> clip-file-offset) space, for routing an
/// incoming disc sector to the unit it backs.
#[derive(Debug, Clone)]
struct ExtentRec {
    disc_lba: u32,
    sectors: u32,
    /// Byte offset within the clip FILE of this extent's first byte.
    file_off: u64,
    clip: u32,
}

/// A unit being assembled from its (up to 3) backing disc sectors.
struct Partial {
    buf: Box<[u8; ALIGNED_UNIT_LEN]>,
    /// Bit `s` set once sector slot `s` (0..3) has been filled.
    have: u8,
    /// Disc LBA of each slot, for emitting the bad range if verify fails.
    lba: [u32; 3],
}

/// Whether a fully-assembled unit can be decrypted + verified — the single
/// answer the gate produces per unit (`is this decryptable?`, 3-state).
enum Decryptability {
    /// Decrypts to strictly clean MPEG-TS (or is valid clear content). Keep good.
    Decryptable,
    /// Confidently does NOT decrypt — bad ciphertext or a bad read. The caller
    /// downgrades this unit's disc range (decrypt-fail == bad read).
    Undecryptable,
    /// Can't tell — e.g. we may simply lack the key. Skip, leave the read as-is.
    Unknown,
}

/// The post-read verify gate. Built once per pass; fed the disc-absolute,
/// just-`Finished` byte ranges via [`observe`](Self::observe); emits the disc
/// ranges that are confidently undecryptable so the caller can mark them bad.
pub struct UnitVerifier {
    /// Sorted by `disc_lba`; covers only AACS clip (`.m2ts`/`.ssif`) content.
    extents: Vec<ExtentRec>,
    /// Number of FULL (6144) units per clip; the partial tail unit is excluded.
    full_units: Vec<u32>,
    /// Stream container per clip — selects the post-decrypt structural check.
    containers: Vec<ContainerKind>,
    /// Content unit keys to try (resolved keys plus any fetched + cached).
    keys: Vec<[u8; 16]>,
    fetch: Option<KeyFetch>,
    fetch_calls: u32,
    fetch_spent: bool,
    partials: HashMap<(u32, u32), Partial>,
    lru: VecDeque<(u32, u32)>,
}

impl UnitVerifier {
    /// Build the gate, or `None` (verify disabled / nothing to verify) when:
    /// the [`POST_READ_VERIFY`] switch is off, the disc is not AACS, there are no
    /// keys to try and no fetch seam, or no AACS clip extents were enumerated.
    /// Returning `None` is the fail-safe default — the caller then verifies
    /// nothing and behaves exactly as today.
    pub fn new(clips: &[ClipLayout], keys: &DecryptKeys, fetch: Option<KeyFetch>) -> Option<Self> {
        if !POST_READ_VERIFY {
            return None;
        }
        // Only AACS has the per-unit CPI flag + decrypt-verify this gate checks.
        let DecryptKeys::Aacs { unit_keys, .. } = keys else {
            return None;
        };
        let held: Vec<[u8; 16]> = unit_keys.iter().map(|(_, k)| *k).collect();
        // With neither a held key nor a fetch seam there is nothing we could ever
        // confidently reject, so disable rather than buffer for no reason.
        if held.is_empty() && fetch.is_none() {
            return None;
        }

        let mut extents = Vec::new();
        let mut full_units = Vec::new();
        let mut containers = Vec::new();
        for (clip, layout) in clips.iter().enumerate() {
            // Full units only; the partial tail (size not a multiple of 6144) is
            // never verified (a < 6144 buffer can't satisfy the strict gate).
            full_units.push((layout.size / ALIGNED_UNIT_LEN as u64) as u32);
            containers.push(layout.container);
            let mut file_off: u64 = 0;
            for &(disc_lba, byte_len) in &layout.extents {
                let sectors = (byte_len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
                if sectors > 0 {
                    extents.push(ExtentRec {
                        disc_lba,
                        sectors,
                        file_off,
                        clip: clip as u32,
                    });
                }
                file_off = file_off.saturating_add(byte_len as u64);
            }
        }
        if extents.is_empty() {
            return None;
        }
        extents.sort_by_key(|e| e.disc_lba);

        Some(Self {
            extents,
            full_units,
            containers,
            keys: held,
            fetch,
            fetch_calls: 0,
            fetch_spent: false,
            partials: HashMap::new(),
            lru: VecDeque::new(),
        })
    }

    /// The post-decrypt structural check for a clip's container.
    fn accept_for(&self, clip: u32) -> fn(&[u8]) -> bool {
        match self.containers[clip as usize] {
            ContainerKind::Ts => aacs::unit_is_clean_ts,
            ContainerKind::Ps => aacs::unit_is_clean_ps,
        }
    }

    /// Feed a just-read, just-`Finished` disc byte range (`bytes` starts at disc
    /// sector `disc_lba`). Routes each backing sector into its clip-file unit;
    /// every unit that becomes fully assembled is verified immediately. Returns
    /// the disc ranges `(lba, sector_count)` of units that are CONFIDENTLY bad
    /// (empty when nothing failed). Never errors — a read is never broken here.
    pub fn observe(&mut self, disc_lba: u32, bytes: &[u8]) -> Vec<(u32, u32)> {
        let mut bad: Vec<(u32, u32)> = Vec::new();
        let sector = crate::consts::SECTOR_BYTES;
        let n = bytes.len() / sector;
        for s in 0..n {
            let lba = disc_lba.saturating_add(s as u32);
            let Some((clip, unit, slot)) = self.locate(lba) else {
                continue; // not AACS clip content, or an unalignable boundary
            };
            // Tail / partial units are never verified.
            if unit >= self.full_units[clip as usize] {
                continue;
            }
            let off = s * sector;
            self.fill(clip, unit, slot, lba, &bytes[off..off + sector]);
            if let Some((raw, lbas)) = self.take_if_complete(clip, unit) {
                let accept = self.accept_for(clip);
                match self.decryptability(&raw, accept) {
                    Decryptability::Undecryptable => push_ranges(&mut bad, &lbas),
                    Decryptability::Decryptable | Decryptability::Unknown => {}
                }
            }
        }
        bad
    }

    /// Disc LBA -> (clip, unit index, sector slot 0..3), or `None` if the sector
    /// is not AACS-clip content or sits at an unalignable (non-sector) file
    /// offset (which we conservatively skip).
    fn locate(&self, lba: u32) -> Option<(u32, u32, usize)> {
        // Largest extent whose disc_lba <= lba.
        let idx = self.extents.partition_point(|e| e.disc_lba <= lba);
        if idx == 0 {
            return None;
        }
        let e = &self.extents[idx - 1];
        let delta = lba - e.disc_lba;
        if delta >= e.sectors {
            return None; // past this extent, not covered by any clip
        }
        let file_off = e.file_off + delta as u64 * SECTOR_BYTES_U64;
        // Guard pathological non-sector-aligned extent boundaries.
        if file_off % SECTOR_BYTES_U64 != 0 {
            return None;
        }
        let unit = (file_off / ALIGNED_UNIT_LEN as u64) as u32;
        let in_unit = (file_off % ALIGNED_UNIT_LEN as u64) as usize;
        let slot = in_unit / crate::consts::SECTOR_BYTES;
        Some((e.clip, unit, slot))
    }

    fn fill(&mut self, clip: u32, unit: u32, slot: usize, lba: u32, sector_bytes: &[u8]) {
        let key = (clip, unit);
        let entry = self.partials.entry(key);
        let fresh = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
        let p = entry.or_insert_with(|| Partial {
            buf: Box::new([0u8; ALIGNED_UNIT_LEN]),
            have: 0,
            lba: [u32::MAX; 3],
        });
        let off = slot * crate::consts::SECTOR_BYTES;
        p.buf[off..off + crate::consts::SECTOR_BYTES].copy_from_slice(sector_bytes);
        p.have |= 1 << slot;
        p.lba[slot] = lba;
        if fresh {
            self.lru.push_back(key);
            self.evict_if_needed();
        }
    }

    /// Remove and return the unit if all three sectors have arrived.
    fn take_if_complete(
        &mut self,
        clip: u32,
        unit: u32,
    ) -> Option<([u8; ALIGNED_UNIT_LEN], [u32; 3])> {
        let key = (clip, unit);
        let complete = self
            .partials
            .get(&key)
            .map(|p| p.have == 0b111)
            .unwrap_or(false);
        if !complete {
            return None;
        }
        let p = self.partials.remove(&key)?;
        if let Some(pos) = self.lru.iter().position(|k| *k == key) {
            self.lru.remove(pos);
        }
        Some((*p.buf, p.lba))
    }

    fn evict_if_needed(&mut self) {
        while self.partials.len() > MAX_INFLIGHT_UNITS {
            // Oldest partial goes unverified (fail-safe) to bound memory.
            if let Some(key) = self.lru.pop_front() {
                self.partials.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Can this fully-assembled unit be decrypted + verified? `accept` is the
    /// container's strict structural check ([`aacs::unit_is_clean_ts`] for TS,
    /// [`aacs::unit_is_clean_ps`] for PS) — the only format-specific part; the
    /// AACS crypto is container-agnostic. Returns the 3-state [`Decryptability`].
    fn decryptability(
        &mut self,
        raw: &[u8; ALIGNED_UNIT_LEN],
        accept: fn(&[u8]) -> bool,
    ) -> Decryptability {
        // CPI clear -> the unit is plaintext by spec (no key needed). If it is
        // structurally clean, it is decryptable-as-is. If it ISN'T, we
        // DELIBERATELY return Unknown, not Undecryptable: with no key to
        // crypto-prove anything, a clear-but-not-clean unit could be a genuine
        // bad read OR a mis-aligned read OR legitimately-odd clear content (some
        // menu/nav units). We refuse to assert "bad" without proof — only
        // ENCRYPTED units that no key opens are ever flagged. (A real bad READ of
        // clear content is still caught by the normal SCSI read-error path; this
        // gate just won't false-flag it.)
        if !aacs::aacs_unit_encrypted(raw) {
            return if accept(raw) {
                Decryptability::Decryptable
            } else {
                Decryptability::Unknown
            };
        }
        // Encrypted: any held key that decrypts to a structurally-clean unit.
        if self.try_keys(raw, accept) {
            return Decryptability::Decryptable;
        }
        // No held key works. Ask the application's key source ONCE for this
        // ciphertext; a fetched key is cached for later units. Only if the
        // service hands us key(s) that STILL don't open it is the unit
        // confidently undecryptable. No seam / no new key -> we may just lack the
        // key -> Unknown (skip), never a false-bad.
        if !self.fetch_spent && self.fetch_calls < MAX_FETCH_CALLS {
            if let Some(cb) = self.fetch.clone() {
                self.fetch_calls += 1;
                let fresh = cb(&[raw.to_vec()]);
                let mut added = false;
                for k in fresh {
                    if !self.keys.contains(&k) {
                        self.keys.push(k);
                        added = true;
                    }
                }
                if !added {
                    self.fetch_spent = true; // service has nothing new; stop asking
                    return Decryptability::Unknown;
                }
                if self.try_keys(raw, accept) {
                    return Decryptability::Decryptable;
                }
                return Decryptability::Undecryptable; // service's keys don't open it -> bad ciphertext
            }
        }
        Decryptability::Unknown
    }

    /// True if any currently-held key decrypts `raw` to a structurally-clean unit
    /// under the container's `accept` check.
    fn try_keys(&self, raw: &[u8; ALIGNED_UNIT_LEN], accept: fn(&[u8]) -> bool) -> bool {
        for k in &self.keys {
            let mut scratch = *raw;
            if aacs::decrypt_unit_checked(&mut scratch, k, accept) {
                return true;
            }
        }
        false
    }

    /// Re-verify, from a 1:1 disc ISO image, every full clip unit that overlaps
    /// `ranges` (disc BYTE ranges — e.g. the bad ranges a patch pass re-read).
    /// Reads each unit's backing sectors from `iso` (ISO sector N == disc LBA N),
    /// runs the same per-unit [`verdict`](Self::verdict), and returns the
    /// confidently-bad disc ranges `(lba, count)` to re-mark.
    ///
    /// This is the PATCH counterpart to [`observe`](Self::observe): patch
    /// re-reads only the bad sectors of a unit, so it can never complete a unit
    /// from its live read stream (the unit's other sectors are already in the
    /// ISO). Reading the whole unit back from the just-patched ISO is the only
    /// alignment-correct way to re-check it. FAIL-SAFE: an ISO read error on any
    /// of a unit's sectors skips that unit (no false-bad).
    ///
    /// CRITICAL: `is_finished(lba)` must report whether each disc sector was
    /// actually READ (mapfile `Finished`). A unit with ANY non-Finished sector is
    /// zero-filled there (the drive read failed) — we CANNOT verify what was
    /// never read, so such a unit is skipped entirely. This both avoids asserting
    /// "undecryptable" on unread data and avoids wasting a key lookup on a block
    /// the read already knows is bad.
    pub fn reverify_iso<S: crate::sector::SectorSource>(
        &mut self,
        iso: &mut S,
        ranges: &[(u64, u64)],
        is_finished: &dyn Fn(u32) -> bool,
    ) -> Vec<(u32, u32)> {
        let mut seen: std::collections::HashSet<(u32, u32)> = std::collections::HashSet::new();
        let mut bad: Vec<(u32, u32)> = Vec::new();
        for &(pos, len) in ranges {
            if len == 0 {
                continue;
            }
            let start = (pos / SECTOR_BYTES_U64) as u32;
            let end = pos.saturating_add(len).div_ceil(SECTOR_BYTES_U64) as u32;
            for lba in start..end {
                let Some((clip, unit, _)) = self.locate(lba) else {
                    continue;
                };
                if unit >= self.full_units[clip as usize] || !seen.insert((clip, unit)) {
                    continue;
                }
                let Some(lbas) = self.unit_disc_sectors(clip, unit) else {
                    continue;
                };
                // We can only verify a unit whose EVERY backing sector was read.
                // A non-Finished sector is zero-filled (the read failed there);
                // verifying it would judge data we never read and waste a key
                // lookup on a known-bad block. Skip the whole unit.
                if !lbas.iter().all(|&l| is_finished(l)) {
                    continue;
                }
                let mut raw = [0u8; ALIGNED_UNIT_LEN];
                let mut readable = true;
                for (slot, &slba) in lbas.iter().enumerate() {
                    let off = slot * crate::consts::SECTOR_BYTES;
                    if iso
                        .read_sectors(
                            slba,
                            1,
                            &mut raw[off..off + crate::consts::SECTOR_BYTES],
                            false,
                        )
                        .is_err()
                    {
                        readable = false;
                        break;
                    }
                }
                let accept = self.accept_for(clip);
                if readable
                    && matches!(self.decryptability(&raw, accept), Decryptability::Undecryptable)
                {
                    push_ranges(&mut bad, &lbas);
                }
            }
        }
        bad
    }

    /// Disc LBAs backing the 3 sectors of full `unit` in `clip`, walking that
    /// clip's extents in file order. `None` if any sector is not covered by an
    /// extent (never happens for a full unit of an enumerated clip).
    fn unit_disc_sectors(&self, clip: u32, unit: u32) -> Option<[u32; 3]> {
        let base = unit as u64 * ALIGNED_UNIT_LEN as u64;
        let mut out = [u32::MAX; 3];
        for (slot, item) in out.iter_mut().enumerate() {
            let foff = base + slot as u64 * SECTOR_BYTES_U64;
            let e = self.extents.iter().find(|e| {
                e.clip == clip
                    && e.file_off <= foff
                    && foff < e.file_off + e.sectors as u64 * SECTOR_BYTES_U64
            })?;
            *item = e.disc_lba + ((foff - e.file_off) / SECTOR_BYTES_U64) as u32;
        }
        Some(out)
    }
}

/// Append `lbas` (a unit's up-to-3 backing sectors) to `out` as `(lba, count)`
/// ranges, coalescing contiguous sectors. Unset slots (`u32::MAX`) are skipped.
fn push_ranges(out: &mut Vec<(u32, u32)>, lbas: &[u32; 3]) {
    let mut present: Vec<u32> = lbas.iter().copied().filter(|&l| l != u32::MAX).collect();
    present.sort_unstable();
    for lba in present {
        if let Some(last) = out.last_mut() {
            if last.0 + last.1 == lba {
                last.1 += 1;
                continue;
            }
        }
        out.push((lba, 1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TS_SYNC: u8 = 0x47;

    /// A clear aligned unit: TS sync at offset 4 + k*192, CPI bits clear.
    fn clear_unit() -> Vec<u8> {
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 4;
        while off < ALIGNED_UNIT_LEN {
            u[off] = TS_SYNC;
            off += 192;
        }
        u
    }

    /// A clear MPEG-2 PS aligned unit: pack-start `00 00 01 BA` at each 2048
    /// boundary, CPI bits clear (byte 0 == 0x00).
    fn clear_ps_unit() -> Vec<u8> {
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        for o in [0usize, 2048, 4096] {
            u[o..o + 4].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        }
        u
    }

    /// Encrypt a clear unit in place under `unit_key` (sets CPI, AES-CBC body) —
    /// the exact inverse of `decrypt_unit`, so the right key restores clean TS.
    fn encrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
        unit[0] |= 0xC0; // CPI flag => reads as encrypted
        let header: [u8; 16] = unit[..16].try_into().unwrap();
        let derived = crate::aacs::decrypt::aes_ecb_encrypt(unit_key, &header);
        let mut k = [0u8; 16];
        for i in 0..16 {
            k[i] = derived[i] ^ header[i];
        }
        let cipher = Aes128::new(GenericArray::from_slice(&k));
        let mut prev = crate::aacs::decrypt::AACS_IV;
        for i in 0..(ALIGNED_UNIT_LEN - 16) / 16 {
            let off = 16 + i * 16;
            for j in 0..16 {
                unit[off + j] ^= prev[j];
            }
            let mut block = GenericArray::clone_from_slice(&unit[off..off + 16]);
            cipher.encrypt_block(&mut block);
            unit[off..off + 16].copy_from_slice(&block);
            prev.copy_from_slice(&unit[off..off + 16]);
        }
    }

    fn aacs_keys(keys: &[[u8; 16]]) -> DecryptKeys {
        DecryptKeys::Aacs {
            unit_keys: keys
                .iter()
                .enumerate()
                .map(|(i, k)| (i as u32, *k))
                .collect(),
            read_data_key: None,
        }
    }

    /// One contiguous full unit at disc LBA `lba` (size 6144 = 3 sectors).
    fn one_clip(lba: u32) -> Vec<ClipLayout> {
        vec![ts_clip(ALIGNED_UNIT_LEN as u64, vec![(lba, ALIGNED_UNIT_LEN as u32)])]
    }

    /// Build a BD-TS `ClipLayout` (the only container current enumeration emits).
    fn ts_clip(size: u64, extents: Vec<(u32, u32)>) -> ClipLayout {
        ClipLayout {
            size,
            extents,
            container: ContainerKind::Ts,
        }
    }

    // ── fail-safe: when the gate must NOT exist ────────────────────────────

    #[test]
    fn kill_switch_default_on() {
        assert!(POST_READ_VERIFY, "shipping default: gate enabled");
    }

    #[test]
    fn new_is_none_for_non_aacs() {
        assert!(UnitVerifier::new(&one_clip(100), &DecryptKeys::None, None).is_none());
        assert!(
            UnitVerifier::new(
                &one_clip(100),
                &DecryptKeys::Css { title_key: [0; 5] },
                None
            )
            .is_none()
        );
    }

    #[test]
    fn new_is_none_with_no_keys_and_no_fetch() {
        // Nothing could ever be confidently rejected -> don't even buffer.
        assert!(UnitVerifier::new(&one_clip(100), &aacs_keys(&[]), None).is_none());
    }

    #[test]
    fn new_is_none_with_no_extents() {
        let clips = vec![ts_clip(0, vec![])];
        assert!(UnitVerifier::new(&clips, &aacs_keys(&[[1; 16]]), None).is_none());
    }

    // ── clear (CPI=0) content ──────────────────────────────────────────────

    #[test]
    fn clear_clean_unit_is_good() {
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[1; 16]]), None).unwrap();
        let bad = v.observe(100, &clear_unit());
        assert!(bad.is_empty(), "clean clear unit must not be flagged");
    }

    #[test]
    fn clear_corrupted_unit_is_skipped_not_flagged() {
        // A CPI-clear unit whose TS syncs don't check out is NOT flagged: with no
        // key to crypto-prove anything we won't assert "bad" on clear content
        // (could be a mis-aligned read or odd-but-valid clear/menu data). Only
        // encrypted-won't-decrypt is ever a downgrade. A genuine bad READ is
        // already caught by the SCSI read-error path; this gate must not
        // false-flag it.
        let mut u = clear_unit();
        u[4] = 0x00; // break the first packet's sync
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[1; 16]]), None).unwrap();
        let bad = v.observe(100, &u);
        assert!(bad.is_empty(), "clear-but-not-clean unit -> skip, never false-bad");
    }

    // ── encrypted (CPI set) content ────────────────────────────────────────

    #[test]
    fn encrypted_unit_held_key_decrypts_is_good() {
        let key = [0x5a; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &key);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[key]), None).unwrap();
        assert!(v.observe(100, &u).is_empty(), "right key -> good");
    }

    #[test]
    fn encrypted_unit_wrong_key_no_fetch_is_uncertain_not_bad() {
        // CRITICAL fail-safe: a unit we can't decrypt because we may simply LACK
        // the key (no fetch seam) must be SKIPPED, never flagged bad.
        let real = [0x11; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x22; 16]]), None).unwrap();
        assert!(
            v.observe(100, &u).is_empty(),
            "missing key without a fetch seam must NOT be a false-bad"
        );
    }

    #[test]
    fn encrypted_unit_fetch_supplies_right_key_is_good() {
        let real = [0x33; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let fetch: KeyFetch = Arc::new(move |_samples: &[Vec<u8>]| vec![real]);
        let mut v =
            UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x44; 16]]), Some(fetch)).unwrap();
        assert!(
            v.observe(100, &u).is_empty(),
            "fetched key recovers -> good"
        );
    }

    #[test]
    fn encrypted_unit_fetch_supplies_wrong_keys_is_bad() {
        // The service handed us key(s) that still don't open it -> genuinely bad
        // ciphertext, confidently flagged.
        let real = [0x55; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let fetch: KeyFetch = Arc::new(move |_s: &[Vec<u8>]| vec![[0x99; 16], [0xAA; 16]]);
        let mut v =
            UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x66; 16]]), Some(fetch)).unwrap();
        assert_eq!(
            v.observe(100, &u),
            vec![(100, 3)],
            "wrong fetched keys -> bad"
        );
    }

    #[test]
    fn encrypted_unit_fetch_supplies_nothing_is_uncertain() {
        let real = [0x77; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| Vec::new());
        let mut v =
            UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x88; 16]]), Some(fetch)).unwrap();
        assert!(
            v.observe(100, &u).is_empty(),
            "fetch returns nothing new -> uncertain -> skip"
        );
    }

    #[test]
    fn fetched_key_is_cached_for_later_units() {
        // First orphan unit triggers one fetch; the second reuses the cached key
        // with no further fetch call.
        let real = [0xC3; 16];
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = calls.clone();
        let fetch: KeyFetch = Arc::new(move |_s: &[Vec<u8>]| {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            vec![real]
        });
        let clips = vec![ts_clip(2 * ALIGNED_UNIT_LEN as u64, vec![(200, 2 * ALIGNED_UNIT_LEN as u32)])];
        let mut v = UnitVerifier::new(&clips, &aacs_keys(&[[0x01; 16]]), Some(fetch)).unwrap();
        let mut u0 = clear_unit();
        encrypt_unit(&mut u0, &real);
        let mut u1 = clear_unit();
        encrypt_unit(&mut u1, &real);
        assert!(v.observe(200, &u0).is_empty());
        assert!(v.observe(203, &u1).is_empty());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "second orphan unit reuses the cached fetched key (one fetch total)"
        );
    }

    // ── alignment: fragmentation, tails, ordering, skips ───────────────────

    #[test]
    fn fragmented_unit_assembles_across_distant_extents() {
        // Unit 0 spans extent A (sectors 0,1 at LBA 10) and extent B (sector 2 at
        // disc-distant LBA 5000). It must still assemble and verify; a bad unit
        // emits BOTH disc ranges.
        let real = [0x42; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let clips = vec![ts_clip(ALIGNED_UNIT_LEN as u64, vec![(10, 4096), (5000, 2048)])];
        // Wrong key + a fetch that yields wrong keys => confident bad, fragmented.
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| vec![[0xEE; 16]]);
        let mut v = UnitVerifier::new(&clips, &aacs_keys(&[[0x01; 16]]), Some(fetch)).unwrap();
        // Feed the two extents in separate observe calls, distant order.
        assert!(
            v.observe(10, &u[..4096]).is_empty(),
            "incomplete -> no verdict yet"
        );
        let bad = v.observe(5000, &u[4096..]);
        assert_eq!(
            bad,
            vec![(10, 2), (5000, 1)],
            "fragmented bad unit -> both ranges"
        );
    }

    #[test]
    fn partial_tail_unit_is_never_verified() {
        // Clip size 6144 + 2048: unit 0 is full, "unit 1" is a 2048 partial tail
        // and must be skipped even if corrupt.
        let key = [0x5a; 16];
        let mut u0 = clear_unit();
        encrypt_unit(&mut u0, &key);
        let clips = vec![ts_clip(ALIGNED_UNIT_LEN as u64 + 2048, vec![(100, ALIGNED_UNIT_LEN as u32 + 2048)])];
        let mut v = UnitVerifier::new(&clips, &aacs_keys(&[key]), None).unwrap();
        // Feed full unit 0 (good) + the tail sector (garbage). Only unit 0 is
        // judged; the tail is never a verdict.
        let mut feed = u0.clone();
        feed.extend_from_slice(&[0xABu8; 2048]); // tail sector, not a full unit
        assert!(
            v.observe(100, &feed).is_empty(),
            "tail partial never flagged"
        );
    }

    #[test]
    fn incomplete_unit_from_skip_is_never_flagged() {
        // Damage-jump: only 2 of 3 sectors of a unit ever arrive. The unit never
        // completes, so it is never verified (its sectors are already pending).
        let real = [0x11; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| vec![[0xEE; 16]]);
        let mut v =
            UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x01; 16]]), Some(fetch)).unwrap();
        // Only sectors 0 and 1 (skip sector 2).
        assert!(
            v.observe(100, &u[..4096]).is_empty(),
            "incomplete unit -> no verdict"
        );
    }

    #[test]
    fn sectors_split_across_observe_calls_still_complete() {
        let key = [0x5a; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &key);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[key]), None).unwrap();
        assert!(v.observe(100, &u[..2048]).is_empty()); // sector 0
        assert!(v.observe(101, &u[2048..4096]).is_empty()); // sector 1
        assert!(v.observe(102, &u[4096..]).is_empty()); // sector 2 -> completes, good
    }

    #[test]
    fn sector_outside_any_clip_is_ignored() {
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[1; 16]]), None).unwrap();
        // LBA 50 is before the clip at 100 -> not routed, no panic, no verdict.
        assert!(v.observe(50, &[0u8; 2048]).is_empty());
        // LBA 200 is past the clip's 3 sectors -> ignored.
        assert!(v.observe(200, &[0u8; 2048]).is_empty());
    }

    // ── reverify_iso (patch path: read whole units back from the ISO) ──────

    /// In-memory 1:1 ISO (sector N == disc LBA N). Unset sectors read as zeros;
    /// `err_lba` forces a read error to exercise the fail-safe skip.
    struct MockIso {
        sectors: std::collections::HashMap<u32, [u8; 2048]>,
        err_lba: Option<u32>,
    }
    impl crate::sector::SectorSource for MockIso {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::Result<usize> {
            for i in 0..count as u32 {
                if self.err_lba == Some(lba + i) {
                    return Err(crate::error::Error::DiscRead {
                        sector: (lba + i) as u64,
                        status: None,
                        sense: None,
                    });
                }
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                let off = i as usize * 2048;
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(count as usize * 2048)
        }
    }

    /// Place a 6144-byte unit's 3 sectors at disc LBAs `lbas` in the mock ISO.
    fn place_unit(iso: &mut MockIso, lbas: [u32; 3], unit: &[u8]) {
        for (slot, &lba) in lbas.iter().enumerate() {
            let mut s = [0u8; 2048];
            s.copy_from_slice(&unit[slot * 2048..slot * 2048 + 2048]);
            iso.sectors.insert(lba, s);
        }
    }

    #[test]
    fn reverify_iso_good_unit_returns_empty() {
        let key = [0x5a; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &key);
        let mut iso = MockIso { sectors: Default::default(), err_lba: None };
        place_unit(&mut iso, [100, 101, 102], &u);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[key]), None).unwrap();
        let bad = v.reverify_iso(&mut iso, &[(100 * 2048, 3 * 2048)], &|_| true);
        assert!(bad.is_empty(), "decryptable unit re-read clean -> not bad");
    }

    #[test]
    fn reverify_iso_bad_unit_returns_its_range() {
        let real = [0x11; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let mut iso = MockIso { sectors: Default::default(), err_lba: None };
        place_unit(&mut iso, [100, 101, 102], &u);
        // Wrong held key + a fetch that yields a wrong key => confident bad.
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| vec![[0xEE; 16]]);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x22; 16]]), Some(fetch)).unwrap();
        // A range covering only ONE sector of the unit still re-reads the WHOLE
        // unit from the ISO (patch re-reads partial units).
        let bad = v.reverify_iso(&mut iso, &[(101 * 2048, 2048)], &|_| true);
        assert_eq!(bad, vec![(100, 3)], "undecryptable unit -> full 3-sector range");
    }

    #[test]
    fn reverify_iso_fragmented_unit_reads_distant_sectors() {
        let key = [0x5a; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &key);
        // Unit 0: sectors at 10, 11 (extent A) and 5000 (extent B).
        let clips = vec![ts_clip(ALIGNED_UNIT_LEN as u64, vec![(10, 4096), (5000, 2048)])];
        let mut iso = MockIso { sectors: Default::default(), err_lba: None };
        place_unit(&mut iso, [10, 11, 5000], &u);
        let mut v = UnitVerifier::new(&clips, &aacs_keys(&[key]), None).unwrap();
        // Range touches only the distant fragment; whole unit still assembled.
        let bad = v.reverify_iso(&mut iso, &[(5000 * 2048, 2048)], &|_| true);
        assert!(bad.is_empty(), "fragmented decryptable unit re-read clean");
    }

    #[test]
    fn reverify_iso_unreadable_sector_skips_unit() {
        let real = [0x11; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let mut iso = MockIso {
            sectors: Default::default(),
            err_lba: Some(102), // 3rd sector unreadable
        };
        place_unit(&mut iso, [100, 101, 102], &u);
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| vec![[0xEE; 16]]);
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x22; 16]]), Some(fetch)).unwrap();
        let bad = v.reverify_iso(&mut iso, &[(100 * 2048, 3 * 2048)], &|_| true);
        assert!(bad.is_empty(), "ISO read error on a sector -> skip (fail-safe)");
    }

    #[test]
    fn reverify_iso_skips_unit_with_unread_sector_and_never_fetches() {
        // Sector 102 was NOT read (Unreadable -> zero-filled in the ISO). Even
        // though the partly-zero unit wouldn't decrypt, we CANNOT verify what
        // wasn't read: the unit is skipped, and crucially NO key lookup is made
        // on a block the read already knows is bad.
        let real = [0x11; 16];
        let mut u = clear_unit();
        encrypt_unit(&mut u, &real);
        let mut iso = MockIso { sectors: Default::default(), err_lba: None };
        place_unit(&mut iso, [100, 101, 102], &u);
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| panic!("must NOT key-fetch an unread unit"));
        let mut v = UnitVerifier::new(&one_clip(100), &aacs_keys(&[[0x22; 16]]), Some(fetch)).unwrap();
        // 102 not Finished -> the whole unit is skipped.
        let is_finished = |lba: u32| lba != 102;
        let bad = v.reverify_iso(&mut iso, &[(100 * 2048, 3 * 2048)], &is_finished);
        assert!(bad.is_empty(), "unit with an unread sector is skipped, not flagged or fetched");
    }

    #[test]
    fn ps_container_routes_through_pack_check() {
        // A clip declared HD-DVD PS. The verifier must dispatch the structural
        // check to unit_is_clean_ps (pack starts), not unit_is_clean_ts.
        let clips = vec![ClipLayout {
            size: ALIGNED_UNIT_LEN as u64,
            extents: vec![(100, ALIGNED_UNIT_LEN as u32)],
            container: ContainerKind::Ps,
        }];
        // A clear, valid PS unit passes the PS pack-start check -> not flagged.
        let mut v = UnitVerifier::new(&clips, &aacs_keys(&[[1; 16]]), None).unwrap();
        assert!(
            v.observe(100, &clear_ps_unit()).is_empty(),
            "valid PS unit passes unit_is_clean_ps"
        );
        // An encrypted unit (CPI set) whose decrypt never yields PS pack-starts,
        // with a fetch returning a non-working key -> confidently Undecryptable
        // through decrypt_unit_checked(.., unit_is_clean_ps). Exercises the Ps
        // path end-to-end (and constructs ContainerKind::Ps so it isn't dead).
        let mut enc = clear_ps_unit();
        enc[0] |= 0xC0; // CPI set; body is garbage to any key
        let fetch: KeyFetch = Arc::new(|_s: &[Vec<u8>]| vec![[0xEE; 16]]);
        let mut v2 = UnitVerifier::new(&clips, &aacs_keys(&[[0x22; 16]]), Some(fetch)).unwrap();
        assert_eq!(
            v2.observe(100, &enc),
            vec![(100, 3)],
            "encrypted PS unit no key opens -> bad via the PS check"
        );
    }

    #[test]
    fn eviction_bounds_inflight_partials() {
        // Open more partials than the cap with single-sector feeds; the map must
        // never exceed the cap (oldest evicted, unverified — fail-safe).
        let mut v = UnitVerifier::new(
            &vec![ts_clip((MAX_INFLIGHT_UNITS as u64 + 100) * ALIGNED_UNIT_LEN as u64, vec![(0, u32::MAX / 2)])],
            &aacs_keys(&[[1; 16]]),
            None,
        )
        .unwrap();
        // Feed only slot 0 of many distinct units (every 3rd sector).
        for unit in 0..(MAX_INFLIGHT_UNITS as u32 + 50) {
            let lba = unit * 3;
            let _ = v.observe(lba, &[0u8; 2048]);
        }
        assert!(
            v.partials.len() <= MAX_INFLIGHT_UNITS,
            "in-flight partials bounded by the cap"
        );
    }
}

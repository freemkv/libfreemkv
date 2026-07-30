//! AACS Media Key Variant chain.
//!
//! On AACS 2.1 the Media Key derivation gains a second stage on top of
//! the classical subset-difference walk. The classical walk yields a
//! Media Key Precursor (Kmp) rather than the final Media Key; the
//! Precursor combines with disc-supplied Variant Key Data (VKD) and a
//! per-licensee Key Correction Data (KCD) constant to produce the Media
//! Key.
//!
//! The entry point is [`derive_media_key_variant`] — a `Kp -> Km`
//! derivation. Deriving `Kp` itself from device keys (DK -> PK) is the
//! separate [`walk_processing_key`] step. The Variant scheme is detected
//! via the AACS 2.1 MKB records `0x2d` (Encrypted Media Key Variant Data
//! / C), `0x2f` (Variant Key Data table, up to 65,535×16), and `0x0c`
//! (variant cvalues, one per `0x04` subset-difference slot). When a disc
//! carries none, callers fall back to the classical single-stage
//! derivation in [`super::derive`].
//!
//! The chain:
//!
//! ```text
//! Kmp     = AES-128D(Kp, C) XOR uv
//! Kpnew   = Kmp XOR KCD
//! Kvn     = AES-G(Kp, Nonce) & 0xFFFF   (low 16 bits, BE)
//! VKD_idx = Kvn XOR VARIANTS[uv]
//! VKD     = vkd_table[VKD_idx * 16 .. +16]
//! Km      = AES-128D(Kpnew, VKD) XOR uv
//! ```
//!
//! **Status.** The record layout is pinned against real variant MKBs:
//! `C` is the per-slot block of the `0x0c` cvalue table (indexed by the
//! matched subset-difference — NOT the `0x2d` head), `VARIANTS[uv]` is the
//! `0x2d` VARIANTS table (leading `body-16` bytes, Nonce at the `0x2d`
//! tail), and `VKD` is `0x2f`. Two inputs still block an end-to-end run
//! against the `0x86` Verify-Media-Key record: the real per-licensee KCD
//! (see [`KEY_CORRECTION_DATA`] — not coded, per-manufacturer), and a
//! covering 2.1 Processing Key. Either one missing yields a wrong `Km`
//! that the final verify gate rejects, so a bad key is never emitted —
//! only an error. (A covering key would also confirm the last layout
//! picks: the 16-bit `Kvn` width and Nonce head-vs-tail.)
//!
//! Two condition bits on `Kmp[15]` route off the default KCD path (Soft
//! Correction and Online Challenge); the chain does not model those modes
//! and treats such a slot as non-covering.
//!
//! **Verify gate.** On the classical path [`walk_processing_key`] gates
//! each match on the VERIFY_MAGIC relation, which authenticates the
//! Processing Key. On a variant MKB that magic does NOT hold (the walk
//! yields a Precursor, not the Media Key), so the authoritative gate is
//! at the END of the chain: the derived `Km` is verified against the
//! MKB's Verify-Media-Key record before it is ever returned.

use super::crypto::{aes_ecb_decrypt, aes_g};
use super::mkb::*;
use super::types::DeviceKey;

// The MKB record types this chain selects — `REC_MEDIA_KEY_VARIANT_DATA`
// (`0x0c`, the per-slot C table), `REC_VARIANT_DATA_AND_NONCE` (`0x2d`, VARIANTS
// + tail Nonce), `REC_VKD_TABLE` (`0x2f`), the subset-difference / cvalue records
// (`0x04` / `0x05` / `0x07`), and the verify records (`0x81` / `0x86`) — are the
// canonical set in [`super::mkb`], in scope here via the `use super::mkb::*` glob.

// ── Public constants ──────────────────────────────────────────────────────

/// AACS 2.1 Key Correction Data — a zero placeholder, NOT real key material.
///
/// **KCD is PER-LICENSEE** (per player manufacturer) — there is no single
/// universal value. libfreemkv compiles in no AACS key material (keydb.cfg is
/// the single source of truth), so this stays all-zero: the chain's SHAPE still
/// runs, but on a real variant disc the derivation yields a wrong Media Key that
/// the final Verify-Media-Key gate rejects. The variant chain therefore cannot
/// complete on a real disc today — a key-acquisition gap, not a code gap. If a
/// real per-licensee KCD is ever available it must come from keydb.cfg, never a
/// compiled constant.
const KEY_CORRECTION_DATA: [u8; 16] = [0u8; 16];

// ── MKB record walking ────────────────────────────────────────────────────

/// True iff `records` contains at least one Media Key Variant record.
///
/// The real AACS 2.1 Variant markers — confirmed against a live variant MKB —
/// are `0x2d` (Encrypted Media Key Variant Data / C) and `0x2f` (Variant Key
/// Data table, 65,535×16). Both are absent from non-variant 1.0/2.0 MKBs (which
/// instead carry `0x05` host-revocation-signature and no `0x0c`/`0x2d`/`0x2f`).
/// The earlier `0x82`/`0x83` guess was speculative and never appeared in any
/// real MKB.
pub fn is_variant_mkb(records: &[MkbRecord]) -> bool {
    records
        .iter()
        .any(|r| matches!(r.rec_type, REC_VARIANT_DATA_AND_NONCE | REC_VKD_TABLE))
}

/// Body of the `0x2d` record: the `VARIANTS` table followed by the trailing
/// 16-byte `Kvn` Nonce. Measured `46_100*2 + 16 = 92_216` on Zombieland v70 and
/// `92_220` on Stand By Me v70 — in both, the leading `body.len() - 16` bytes are
/// the big-endian `u16` `VARIANTS` table (one per subset-difference) and the last
/// 16 bytes are the Nonce, with NO leading header. This does NOT hold the C used
/// for `Kmp` — that is the per-slot block in `0x0c`
/// ([`REC_MEDIA_KEY_VARIANT_DATA`]). Both [`variant_nonce`] and
/// [`variants_for_uv`] read this body.
pub(crate) fn variant_data_record(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == REC_VARIANT_DATA_AND_NONCE)
        .map(|r| r.body.as_slice())
}

/// 16-byte Nonce for `Kvn = AES-G(Kp, Nonce)` — the trailing 16 bytes of the
/// `0x2d` record ([`variant_data_record`]).
///
/// The Nonce-at-tail placement is consistent across both reference MKBs (the
/// leading `body-16` bytes form the `VARIANTS` table exactly), but head-vs-tail
/// is only truly pinned by running the full chain against the `0x86` verify with
/// a covering key. Until then a wrong nonce can only fail that final gate, never
/// emit a bad key.
pub fn variant_nonce(records: &[MkbRecord]) -> Option<[u8; 16]> {
    let body = variant_data_record(records)?;
    if body.len() < 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&body[body.len() - 16..]);
    Some(out)
}

/// The Variant Key Data (VKD) table — record type `0x2f`.
///
/// Confirmed against a live variant MKB: exactly 65,535 × 16 = 1,048,560 bytes,
/// indexed by the resolved `VKDidx`. This is disc-public data (it is why the
/// VKD alone buys nothing without the Media Key chain above it).
pub(crate) fn variant_key_data(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == REC_VKD_TABLE && !r.body.is_empty() && r.body.len() % 16 == 0)
        .map(|r| r.body.as_slice())
}

// ── Subset-difference walk that exposes (Kp, uv) ──────────────────────────

// `calc_v_mask` and `calc_pk_from_dk` (and the AES-G3 seed step they ride
// on) are shared with the classical walk in [`super::derive`] — a single
// definition keeps the variant SD tree byte-identical to the classical one.
// (`aesg3` itself is imported separately in the test module.)
use super::derive::{calc_pk_from_dk, calc_v_mask};

/// Outcome of a subset-difference walk against an MKB. Carries the
/// processing key and the matching `uv` slot — both needed as inputs
/// to the variant chain.
#[derive(Clone, Copy)]
pub struct ProcessingKeyMatch {
    /// Processing Key.
    pub kp: [u8; 16],
    /// Subset-difference node number that matched.
    pub uv: u32,
    /// 16-byte cvalue that the matched uv selected.
    pub cvalue: [u8; 16],
    /// Index of the matching cvalue within the cvalues record.
    pub cvalue_index: usize,
}

// Redacting `Debug`: `kp` (a Processing Key) and `cvalue` are secret, never
// printed. `uv` / `cvalue_index` are non-secret coordinates. Guarded by
// `processing_key_match_debug_is_redacted`.
impl std::fmt::Debug for ProcessingKeyMatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessingKeyMatch")
            .field("kp", &"<redacted>")
            .field("uv", &self.uv)
            .field("cvalue", &"<redacted>")
            .field("cvalue_index", &self.cvalue_index)
            .finish()
    }
}

fn mkb_find_mk_dv(records: &[MkbRecord]) -> Option<[u8; 16]> {
    let r = records.iter().find(|r| {
        (r.rec_type == REC_VERIFY_MEDIA_KEY_V1 || r.rec_type == REC_VERIFY_MEDIA_KEY_V2)
            && r.body.len() >= 16
    })?;
    let mut out = [0u8; 16];
    out.copy_from_slice(&r.body[..16]);
    Some(out)
}

/// Walk an MKB and return the first `(Kp, uv, cvalue)` that
/// `device_keys` covers. Returns `None` if no DK walks any uv.
///
/// This is the AACS-2.1 **variant** walk; the classical walk lives in
/// [`super::derive::derive_media_key_and_pk_from_dk`]. The two are kept
/// separate on purpose and select MKB records in DELIBERATELY different
/// order:
///
///   - cvalues: this variant walk tries record `0x07`-then-`0x05`; the
///     classical walk tries `0x05`-then-`0x07`. On a variant MKB the
///     small `0x07` Explicit-Subset-Difference record carries the
///     cvalue the Precursor chain consumes, whereas a classical UHD MKB
///     keeps its 1:1 cvalue table in the large `0x05` record (see the
///     note on [`super::derive::probe::mkb_cvalues`]). They must NOT be
///     unified to one order — each is correct for its own MKB shape.
///   - finders: this walk operates on parsed [`MkbRecord`]s (needed
///     because the variant chain also reads `0x2d`/`0x2f`); the
///     classical walk operates on raw MKB bytes. Same framing, different
///     input type.
///
/// Consequence: do NOT route the classical DK path through this function
/// — on a classical MKB the `0x07`-first selection picks the wrong (or
/// missing) cvalue and the magic check fails, so it returns `None`.
pub fn walk_processing_key(
    records: &[MkbRecord],
    device_keys: &[DeviceKey],
) -> Option<ProcessingKeyMatch> {
    let mk_dv = mkb_find_mk_dv(records)?;
    let uvs = mkb_find_body(records, REC_SUBSET_DIFFERENCE)?;
    // Variant cvalue source: a real variant MKB carries its per-uv cvalue table
    // in record `0x0c` (confirmed 46,101×16, one per `0x04` subset-difference
    // slot). Fall back to `0x07`/`0x05` for the synthetic fixtures and any MKB
    // shape that keeps its cvalues there.
    let cvalues = mkb_find_body(records, REC_MEDIA_KEY_VARIANT_DATA)
        .or_else(|| mkb_find_body(records, REC_EXPLICIT_SUBSET_DIFF))
        .or_else(|| mkb_find_body(records, REC_MEDIA_KEY_DATA))?;

    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for dk in device_keys {
        let device_number = dk.node as u32;

        for uvs_idx in 0..num_uvs {
            let p_uv = &uvs[1 + 5 * uvs_idx..];
            // `num_uvs` was computed by `take_while(.. (c[0] & 0xC0) == 0)`, so
            // every chunk in `0..num_uvs` already has its revoked-marker bits
            // clear — that `take_while` is the single authoritative place the
            // parse stops, no inner re-check needed.
            let u_mask_shift = uvs[5 * uvs_idx];

            // 0x20..=0x3F (32..=63) have their revoked-marker bits clear (so they
            // pass the take_while above) but are out of range for a u32 shift.
            // `wrapping_shl` would silently compute shift % 32 (e.g. 32 → no shift
            // → 0xFFFF_FFFF), matching a wrong uv slot and deriving a wrong key.
            // Disc-controlled byte: skip the slot instead.
            if u_mask_shift >= 32 {
                continue;
            }

            let uv = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
            if uv == 0 {
                continue;
            }

            let u_mask: u32 = 0xFFFF_FFFFu32.wrapping_shl(u_mask_shift as u32);
            let v_mask = calc_v_mask(uv);

            if ((device_number & u_mask) == (uv & u_mask))
                && ((device_number & v_mask) != (uv & v_mask))
            {
                // dk.u_mask_shift is a u8 from keydb with no range check; guard
                // it the same way before the wrapping_shl below.
                if dk.u_mask_shift >= 32 {
                    continue;
                }
                let dev_key_v_mask = calc_v_mask(dk.uv);
                let dev_key_u_mask: u32 = 0xFFFF_FFFFu32.wrapping_shl(dk.u_mask_shift as u32);

                if u_mask == dev_key_u_mask && (uv & dev_key_v_mask) == (dk.uv & dev_key_v_mask) {
                    let pk = calc_pk_from_dk(&dk.key, uv, v_mask, dev_key_v_mask);

                    if uvs_idx >= cvalues.len() / 16 {
                        continue;
                    }
                    let mut cv = [0u8; 16];
                    cv.copy_from_slice(&cvalues[uvs_idx * 16..(uvs_idx + 1) * 16]);

                    // Validate: AES-D(Kp, cv), XOR uv into low 4 bytes,
                    // then AES-D(.., mk_dv) must reveal the verify magic.
                    let mut km_candidate = aes_ecb_decrypt(&pk, &cv);
                    let uv_bytes = uv.to_be_bytes();
                    for i in 0..4 {
                        km_candidate[12 + i] ^= uv_bytes[i];
                    }
                    let dec_vd = aes_ecb_decrypt(&km_candidate, &mk_dv);
                    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
                    // On a classical (non-variant) MKB this magic must
                    // match. On a variant MKB it won't — `km_candidate`
                    // is really Kmp and the magic check is moot. We
                    // still gate the walk on cvalue indexing being
                    // sane; the chain itself enforces the variant
                    // semantics downstream.
                    let classical_ok = dec_vd[..8] == VERIFY_MAGIC;
                    let variant_present = is_variant_mkb(records);
                    if !(classical_ok || variant_present) {
                        continue;
                    }

                    return Some(ProcessingKeyMatch {
                        kp: pk,
                        uv,
                        cvalue: cv,
                        cvalue_index: uvs_idx,
                    });
                }
            }
        }
    }
    None
}

// ── Error reporting ───────────────────────────────────────────────────────

/// Outcome of [`derive_media_key_variant`] when the chain cannot
/// produce a Media Key. Every variant is a classification only — no
/// strings, no Display impl beyond the error code.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MediaKeyVariantError {
    /// MKB carries no Variant records. Caller should fall back to the
    /// classical single-stage derivation.
    NotVariantMkb,
    /// MKB is missing a required record (mk_dv, subset-difference,
    /// cvalues, variant data, or variant nonce).
    MkbIncomplete,
    /// `device_keys` did not cover any uv slot in this MKB.
    ProcessingKeyUnavailable,
    /// `Kmp[15]` carries bit `0x02`: the soft-correction path applies
    /// for this Precursor. Out of scope for the hardcoded-KCD chain.
    SoftCorrectionRequired,
    /// `Kmp[15]` carries bit `0x04`: the online-challenge path applies
    /// for this Precursor. Out of scope for the hardcoded-KCD chain.
    OnlineChallengeRequired,
    /// `VARIANTS[uv]` could not be read from the `0x2d` record for the
    /// matched slot.
    VariantsTableUnavailable,
    /// VKD index resolved out of the supplied `vkd_table`.
    VkdIndexOutOfRange,
    /// The derived Media Key failed the MKB's Verify-Media-Key relation.
    /// On the variant path this final gate replaces the per-match magic
    /// check (which does not hold for a Precursor).
    MediaKeyVerifyFailed,
}

impl std::fmt::Display for MediaKeyVariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u16 = match self {
            MediaKeyVariantError::NotVariantMkb => 7100,
            MediaKeyVariantError::MkbIncomplete => 7101,
            MediaKeyVariantError::ProcessingKeyUnavailable => 7102,
            MediaKeyVariantError::SoftCorrectionRequired => 7103,
            MediaKeyVariantError::OnlineChallengeRequired => 7104,
            MediaKeyVariantError::VariantsTableUnavailable => 7106,
            MediaKeyVariantError::VkdIndexOutOfRange => 7107,
            MediaKeyVariantError::MediaKeyVerifyFailed => 7108,
        };
        write!(f, "E{code}")
    }
}

impl std::error::Error for MediaKeyVariantError {}

// ── Chain ─────────────────────────────────────────────────────────────────

/// Look up the per-slot `VARIANTS` value for the matched subset-difference slot,
/// keyed by the same index that selected the cvalue ([`ProcessingKeyMatch::cvalue_index`]).
///
/// LAYOUT (fixed against a real 2.1 variant MKB — Zombieland v70, `MKB_RO.inf`):
/// the `0x2d` Encrypted-Media-Key-Variant-Data body is exactly
/// `46_100*2 + 16 = 92_216` bytes, i.e. one **big-endian u16 `VARIANTS` entry per
/// subset-difference slot** (1:1 with the `0x0c` variant cvalues and the `0x04`
/// subset-differences), with the 16-byte per-disc Nonce packed at the **tail**
/// (see [`variant_nonce`]). So the VARIANTS table is the leading `sd_count*2`
/// bytes and this reads its `sd_slot_index`-th entry.
///
/// The record/field *sizing* is confirmed; the one bit still to pin against a
/// covering key is Nonce-head-vs-tail (both fit the size) — a wrong pick can only
/// yield a wrong `Km`, which the final Verify-Media-Key gate rejects (never a
/// silent bad key).
fn variants_for_uv(records: &[MkbRecord], sd_slot_index: usize) -> Option<u16> {
    let body = variant_data_record(records)?;
    // The VARIANTS table is the leading bytes; the 16-byte Kvn Nonce is packed at
    // the TAIL (see [`variant_nonce`]). Bound the read to the table region so a
    // near-end slot can never read Nonce bytes as a VARIANTS entry. NO leading
    // header (measured: Zombieland v70 `0x2d` body = 46_100*2 + 16 = 92_216).
    const NONCE: usize = 16;
    let table_len = body.len().checked_sub(NONCE)?;
    let off = sd_slot_index.checked_mul(2)?;
    if off + 2 > table_len {
        return None;
    }
    Some(u16::from_be_bytes([body[off], body[off + 1]]))
}

/// Enumerate the `(uv, slot_index)` pairs of a variant MKB's subset-difference
/// record (`0x04`), in table order — the same parse [`walk_processing_key`] uses
/// to index cvalues. Factored out so a bare Processing Key (which arrives without
/// its slot) can be tried against each slot.
fn variant_uv_slots(records: &[MkbRecord]) -> Option<Vec<(u32, usize)>> {
    let uvs = mkb_find_body(records, REC_SUBSET_DIFFERENCE)?;
    let mut out = Vec::new();
    let mut idx = 0usize;
    while (idx + 1) * 5 <= uvs.len() {
        let u_mask_shift = uvs[5 * idx];
        // The `0xC0` revoked-marker terminates the table (matches the walk's
        // `take_while`). Shifts ≥ 32 are out of range and skipped, never wrapped.
        if u_mask_shift & 0xC0 != 0 {
            break;
        }
        let p_uv = &uvs[1 + 5 * idx..];
        let uv = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
        if uv != 0 && u_mask_shift < 32 {
            out.push((uv, idx));
        }
        idx += 1;
    }
    Some(out)
}

/// The MKB-derived inputs the variant chain needs for every slot it tries against
/// a given Processing Key. Fetched once by [`derive_media_key_variant`] so the
/// per-slot body stays a lean `(Kp, uv, slot)` call.
struct VariantMkb<'a> {
    records: &'a [MkbRecord],
    nonce: [u8; 16],
    vkd_table: &'a [u8],
    /// The per-subset-difference Encrypted-Media-Key-Variant-Data (C) table from
    /// record `0x0c` — one 16-byte C per slot. C for slot `i` is
    /// `cvalues[i*16..][..16]`, the SAME source/index [`walk_processing_key`]
    /// uses. (NOT `0x2d`, which is VARIANTS + Nonce.)
    cvalues: &'a [u8],
    mk_dv: [u8; 16],
}

/// The variant chain body for ONE known `(Kp, uv, slot)`: derive and verify the
/// Media Key against the MKB's Verify-Media-Key record. VID-free — the Km is
/// MKB-scoped; the VUK is a separate [`super::derive::derive_vuk`] step. Returns
/// the verified Km, or a classification of why this slot did not yield one.
fn variant_km_for_slot(
    m: &VariantMkb<'_>,
    kp: &[u8; 16],
    uv: u32,
    slot_index: usize,
) -> Result<[u8; 16], MediaKeyVariantError> {
    // C for THIS subset-difference: the slot's 16-byte block in the `0x0c`
    // Encrypted-Media-Key-Variant-Data table (same index that selected the
    // cvalue in `walk_processing_key`). `0x2d` is VARIANTS + Nonce, not C.
    let cv_off = slot_index
        .checked_mul(16)
        .ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let c_slice = m
        .cvalues
        .get(cv_off..cv_off + 16)
        .ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let mut c_block = [0u8; 16];
    c_block.copy_from_slice(c_slice);

    // Step: Kmp = AES-128D(Kp, C) XOR uv  (uv into low 4 bytes).
    let mut kmp = aes_ecb_decrypt(kp, &c_block);
    let uv_bytes = uv.to_be_bytes();
    for i in 0..4 {
        kmp[12 + i] ^= uv_bytes[i];
    }

    // Condition bits on Kmp[15] select the correction mode. Bit 0x02 (SoftKCD)
    // and 0x04 (online challenge) need out-of-band data we don't model; the
    // default path (neither bit set) uses the fixed KCD constant.
    if kmp[15] & 0b0000_0010 != 0 {
        return Err(MediaKeyVariantError::SoftCorrectionRequired);
    }
    if kmp[15] & 0b0000_0100 != 0 {
        return Err(MediaKeyVariantError::OnlineChallengeRequired);
    }

    // Step: Kpnew = Kmp XOR KCD.
    let mut kpnew = [0u8; 16];
    for i in 0..16 {
        kpnew[i] = kmp[i] ^ KEY_CORRECTION_DATA[i];
    }

    // Step: Kvn = AES-G(Kp, Nonce) & 0xFFFF  (low 16 bits, BE).
    let kvn_block = aes_g(kp, &m.nonce);
    let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);

    // Step: VKD_idx = Kvn XOR VARIANTS[uv];  VKD = vkd_table[VKD_idx].
    let v_for_uv = variants_for_uv(m.records, slot_index)
        .ok_or(MediaKeyVariantError::VariantsTableUnavailable)?;
    let vkd_idx = kvn ^ v_for_uv;
    let off = (vkd_idx as usize) * 16;
    if off + 16 > m.vkd_table.len() {
        return Err(MediaKeyVariantError::VkdIndexOutOfRange);
    }
    let mut vkd = [0u8; 16];
    vkd.copy_from_slice(&m.vkd_table[off..off + 16]);

    // Step: Km = AES-128D(Kpnew, VKD) XOR uv.
    let mut km = aes_ecb_decrypt(&kpnew, &vkd);
    for i in 0..4 {
        km[12 + i] ^= uv_bytes[i];
    }

    // Gate: the derived Media Key MUST reproduce the MKB's Verify-Media-Key magic
    // (the per-match magic in `walk_processing_key` only saw the Precursor). This
    // is the authoritative check — no unverified key is ever returned.
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    if aes_ecb_decrypt(&km, &m.mk_dv)[..8] != VERIFY_MAGIC {
        return Err(MediaKeyVariantError::MediaKeyVerifyFailed);
    }
    Ok(km)
}

/// Derive the AACS 2.1 variant **Media Key** from a Processing Key.
///
/// The one deterministic `Kp → Km` derivation for a variant MKB. A leaked 2.1
/// Processing Key arrives without its subset-difference slot, so this tries `pk`
/// against every slot and returns the Km for the slot whose full chain passes the
/// MKB's Verify-Media-Key record — exactly the shape of the classical bare-PK
/// [`super::derive::derive_media_key_from_pk`], gated by the chain's own verify so
/// an unverified key is never returned.
///
/// VID-free by design: the Media Key is MKB-scoped. Derive the per-disc VUK from
/// the returned Km with [`super::derive::derive_vuk`]. Deriving a Processing Key
/// from device keys (DK → PK) is a separate concern — walk it first via
/// [`walk_processing_key`], then call this.
///
/// Errors: `NotVariantMkb` (caller should use the classical path), `MkbIncomplete`
/// (a required record is missing), or `ProcessingKeyUnavailable` (no slot verified
/// — `pk` does not cover this MKB, or its slot needs the soft-correction / online
/// path, surfaced as `SoftCorrectionRequired` / `OnlineChallengeRequired`).
pub fn derive_media_key_variant(
    mkb_records: &[MkbRecord],
    pk: &[u8; 16],
) -> Result<[u8; 16], MediaKeyVariantError> {
    if !is_variant_mkb(mkb_records) {
        return Err(MediaKeyVariantError::NotVariantMkb);
    }
    let nonce = variant_nonce(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let vkd_table = variant_key_data(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    // C for the Kmp step is the per-subset-difference `0x0c` table (one 16-byte
    // C per slot) — the SAME source and index `walk_processing_key` uses. `0x2d`
    // holds VARIANTS + Nonce, NOT C. Fall back to `0x07`/`0x05` for the synthetic
    // fixtures that keep a single cvalue there.
    let cvalues = mkb_find_body(mkb_records, REC_MEDIA_KEY_VARIANT_DATA)
        .or_else(|| mkb_find_body(mkb_records, REC_EXPLICIT_SUBSET_DIFF))
        .or_else(|| mkb_find_body(mkb_records, REC_MEDIA_KEY_DATA))
        .ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let mk_dv = mkb_find_mk_dv(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let slots = variant_uv_slots(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let m = VariantMkb {
        records: mkb_records,
        nonce,
        vkd_table,
        cvalues,
        mk_dv,
    };

    // Try `pk` against each slot; return the first verified Km. If none verify,
    // surface a correction-mode error over the generic miss so a disc that needs
    // the soft/online path is distinguishable from a non-covering key.
    let mut correction: Option<MediaKeyVariantError> = None;
    for (uv, slot_index) in slots {
        match variant_km_for_slot(&m, pk, uv, slot_index) {
            Ok(km) => return Ok(km),
            Err(e @ MediaKeyVariantError::SoftCorrectionRequired)
            | Err(e @ MediaKeyVariantError::OnlineChallengeRequired) => {
                correction.get_or_insert(e);
            }
            Err(_) => {}
        }
    }
    Err(correction.unwrap_or(MediaKeyVariantError::ProcessingKeyUnavailable))
}

/// Run the variant chain from a caller-supplied Processing Key and EXPLICIT
/// per-slot inputs — the harness entry that tries a captured `Kp` against known
/// slot material, bypassing both the device-key walk and the on-MKB
/// `VARIANTS[uv]` lookup. The caller supplies the `0x0c` C block, the slot's
/// subset-difference number `uv`, and its `VARIANTS[uv]`; the MKB supplies the
/// Nonce, the VKD table, and the Verify-Media-Key value.
///
/// Returns `(Km, Kvu)`. The terminal Verify-Media-Key gate is identical to
/// [`derive_media_key_variant`], so a wrong `c_block` / `uv` / `variants_uv`
/// returns [`MediaKeyVariantError::MediaKeyVerifyFailed`] rather than a bogus
/// key. The soft-correction / online-challenge bits on `Kmp[15]` are classified
/// the same way, so a slot needing an out-of-band correction path is
/// distinguishable from a non-matching input.
///
/// (Note the KCD caveat on [`KEY_CORRECTION_DATA`]: without the real per-licensee
/// KCD this fails the verify gate on a real disc — a key-acquisition gap.)
pub fn media_key_variant_from_kp(
    kp: &[u8; 16],
    c_block: &[u8; 16],
    uv: u32,
    variants_uv: u16,
    mkb_records: &[MkbRecord],
    vid: &[u8; 16],
) -> Result<([u8; 16], [u8; 16]), MediaKeyVariantError> {
    let nonce = variant_nonce(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let vkd_table = variant_key_data(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let mk_dv = mkb_find_mk_dv(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;

    // Kmp = AES-128D(Kp, C) XOR uv.
    let mut kmp = aes_ecb_decrypt(kp, c_block);
    let uv_bytes = uv.to_be_bytes();
    for i in 0..4 {
        kmp[12 + i] ^= uv_bytes[i];
    }
    if kmp[15] & 0b0000_0010 != 0 {
        return Err(MediaKeyVariantError::SoftCorrectionRequired);
    }
    if kmp[15] & 0b0000_0100 != 0 {
        return Err(MediaKeyVariantError::OnlineChallengeRequired);
    }

    // Kpnew = Kmp XOR KCD.
    let mut kpnew = [0u8; 16];
    for i in 0..16 {
        kpnew[i] = kmp[i] ^ KEY_CORRECTION_DATA[i];
    }

    // Kvn = AES-G(Kp, Nonce) & 0xFFFF; VKD_idx = Kvn XOR VARIANTS[uv].
    let kvn_block = aes_g(kp, &nonce);
    let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);
    let vkd_idx = kvn ^ variants_uv;
    let off = (vkd_idx as usize) * 16;
    if off + 16 > vkd_table.len() {
        return Err(MediaKeyVariantError::VkdIndexOutOfRange);
    }
    let mut vkd = [0u8; 16];
    vkd.copy_from_slice(&vkd_table[off..off + 16]);

    // Km = AES-128D(Kpnew, VKD) XOR uv, then the authoritative Verify-Media-Key gate.
    let mut km = aes_ecb_decrypt(&kpnew, &vkd);
    for i in 0..4 {
        km[12 + i] ^= uv_bytes[i];
    }
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    if aes_ecb_decrypt(&km, &mk_dv)[..8] != VERIFY_MAGIC {
        return Err(MediaKeyVariantError::MediaKeyVerifyFailed);
    }

    // Kvu = AES-G(Km, VID).
    let kvu = aes_g(&km, vid);
    Ok((km, kvu))
}

#[cfg(test)]
mod tests {
    use super::*;
    // These three live in `super::derive` now (consolidated SD-walk helpers);
    // `use super::*` does not re-export the parent module's private `use`
    // imports, so pull them in directly for the tests below.
    use super::super::crypto::aesg3;
    use super::super::derive::calc_pk_from_dk;

    /// `ProcessingKeyMatch` carries the Processing Key (`kp`) and `cvalue` raw;
    /// `Debug` must redact both. Non-secret `uv`/`cvalue_index` are not 213.
    #[test]
    fn processing_key_match_debug_is_redacted() {
        let m = ProcessingKeyMatch {
            kp: [0xD5; 16],
            uv: 1,
            cvalue: [0xD5; 16],
            cvalue_index: 2,
        };
        let dbg = format!("{m:?}");
        assert!(
            !dbg.contains("213"),
            "ProcessingKeyMatch leaked kp/cvalue: {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "ProcessingKeyMatch missing marker: {dbg}"
        );
    }

    #[test]
    fn calc_pk_from_dk_terminates_on_nonconvergent_mask() {
        // Regression for the unbounded-loop hang: pick a (dev_key_v_mask,
        // v_mask) pair the arithmetic `>> 1` walk can never reconcile.
        // dev_key_v_mask has the MSB set, so `>> 1` sign-extends and the
        // mask saturates at 0xFFFF_FFFF, never reaching a coarser v_mask.
        // The 32-step bound must let this return rather than spin forever.
        let dk = [0x11u8; 16];
        let pk = calc_pk_from_dk(&dk, 0x0000_0002, 0x0000_0000, 0xFFFF_FFFE);
        // Bounded exit yields *some* key; we only assert it terminated.
        let _ = pk;
    }

    // ── Helpers ──

    fn synthetic_mkb_classical() -> Vec<u8> {
        // Minimal MKB: type/version record + cvalues + mk_dv. No variant
        // records.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);
        mkb
    }

    fn synthetic_mkb_with_variant() -> Vec<u8> {
        let mut mkb = synthetic_mkb_classical();
        // 0x2d — Encrypted Media Key Variant Data: C (head 16) then the
        // trailing 16-byte Nonce, 32-byte body.
        mkb.extend_from_slice(&[0x2d, 0x00, 0x00, 0x24]);
        mkb.extend_from_slice(&[0xEE; 16]);
        mkb.extend_from_slice(&[0x55; 16]);
        // 0x2f — Variant Key Data table: one 16-byte VKD entry.
        mkb.extend_from_slice(&[0x2f, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCC; 16]);
        mkb
    }

    // ── Walker / record detection ──

    #[test]
    fn walker_parses_synthetic_mkb() {
        let mkb = synthetic_mkb_classical();
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].rec_type, 0x10);
        assert_eq!(recs[1].rec_type, 0x07);
        assert_eq!(recs[2].rec_type, 0x86);
    }

    #[test]
    fn variant_detection_negative_on_classical() {
        let recs = walk_mkb(&synthetic_mkb_classical());
        assert!(!is_variant_mkb(&recs));
        assert!(variant_nonce(&recs).is_none());
        assert!(variant_key_data(&recs).is_none());
        assert!(variant_data_record(&recs).is_none());
    }

    #[test]
    fn variant_detection_positive_on_variant() {
        let recs = walk_mkb(&synthetic_mkb_with_variant());
        assert!(is_variant_mkb(&recs));
        // Nonce = trailing 16 of 0x2d; VKD = the 0x2f entry; C = the whole 0x2d.
        assert_eq!(variant_nonce(&recs), Some([0x55; 16]));
        assert_eq!(variant_key_data(&recs), Some(&[0xCC; 16][..]));
        let c = variant_data_record(&recs).unwrap();
        assert_eq!(&c[..16], &[0xEE; 16][..]);
        assert_eq!(&c[16..], &[0x55; 16][..]);
    }

    // ── Chain entry-point classification ──

    #[test]
    fn chain_rejects_non_variant_mkb() {
        let recs = walk_mkb(&synthetic_mkb_classical());
        let err = derive_media_key_variant(&recs, &[0xAA; 16])
            .expect_err("classical MKB must be rejected");
        assert_eq!(err, MediaKeyVariantError::NotVariantMkb);
    }

    #[test]
    fn chain_detects_soft_correction_bit() {
        // Kmp[15] bit 0x02 on the covering PK's slot surfaces the soft-correction
        // classification over the generic no-slot miss.
        let (recs, _dk, kp, _) = synthetic_variant_setup(/*kmp15*/ 0x02);
        let err = derive_media_key_variant(&recs, &kp)
            .expect_err("bit 0x02 must surface SoftCorrectionRequired");
        assert_eq!(err, MediaKeyVariantError::SoftCorrectionRequired);
    }

    #[test]
    fn chain_detects_online_challenge_bit() {
        let (recs, _dk, kp, _) = synthetic_variant_setup(/*kmp15*/ 0x04);
        let err = derive_media_key_variant(&recs, &kp)
            .expect_err("bit 0x04 must surface OnlineChallengeRequired");
        assert_eq!(err, MediaKeyVariantError::OnlineChallengeRequired);
    }

    #[test]
    fn variants_for_uv_reads_the_table_from_0x2d() {
        // variants_for_uv reads the VARIANTS u16 from the 0x2d record, so on a
        // variant MKB that carries 0x2d it yields Some (never dead-stops the chain
        // at VariantsTableUnavailable).
        let (recs, _dk, _kp, _) = synthetic_variant_setup(/*kmp15*/ 0x00);
        assert!(variants_for_uv(&recs, 0).is_some());
    }

    #[test]
    fn error_display_is_code_only() {
        // No English in Display — every variant emits "E7xxx" and
        // nothing else.
        let cases = [
            MediaKeyVariantError::NotVariantMkb,
            MediaKeyVariantError::MkbIncomplete,
            MediaKeyVariantError::ProcessingKeyUnavailable,
            MediaKeyVariantError::SoftCorrectionRequired,
            MediaKeyVariantError::OnlineChallengeRequired,
            MediaKeyVariantError::VariantsTableUnavailable,
            MediaKeyVariantError::VkdIndexOutOfRange,
            MediaKeyVariantError::MediaKeyVerifyFailed,
        ];
        for e in cases {
            let s = e.to_string();
            assert!(
                s.starts_with('E') && s.len() == 5,
                "error display must be E#### only, got {s:?}"
            );
            assert!(
                s.chars().skip(1).all(|c| c.is_ascii_digit()),
                "error display must be E + digits, got {s:?}"
            );
        }
    }

    // ── Fixture construction ──

    /// Build a synthetic variant MKB plus a DK that walks the single
    /// subset-difference slot it carries. `kmp15` is the value of the
    /// low byte of `Kmp[15]` that the chain will land on — pick `0x02`
    /// to exercise the SoftCorrection bit, `0x04` to exercise
    /// OnlineChallenge, `0x00` otherwise.
    ///
    /// The fixture pins:
    /// - MKB subset-difference: `u_mask_shift=3, uv=2`. With these
    ///   masks the discriminator bit (u_mask=1, v_mask=0) is bit 2.
    /// - one DK at `node=4, uv=2, u_mask_shift=3`. node 4 has bit 2 set
    ///   (differs from uv=2 on bit 2 → disagrees on v_mask) while
    ///   agreeing with uv on bits 3+ (the u_mask=1 region). dk.uv ==
    ///   MKB.uv and dk.u_mask_shift == MKB.u_mask_shift make
    ///   `dev_key_v_mask == v_mask`, so `calc_pk_from_dk` loops zero
    ///   times — Kp = aesg3(dk, 1).
    /// - one cvalue in record 0x07 chosen so AES-D(Kp, C) ⊕ uv produces a
    ///   Kmp whose byte-15 is exactly `kmp15`.
    /// - record 0x2d (Encrypted Media Key Variant Data): a 32-byte body
    ///   carrying C in the head 16 bytes and a 16-byte Nonce in the tail.
    /// - record 0x2f (Variant Key Data): one 16-byte entry.
    ///
    /// Returns (records, dk, planted_kp, planted_kmp).
    fn synthetic_variant_setup(kmp15: u8) -> (Vec<MkbRecord>, DeviceKey, [u8; 16], [u8; 16]) {
        use crate::aacs::crypto::aes_ecb_encrypt;

        // Build header.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];

        // Subset-difference (0x04): u_mask_shift=3, uv=00 00 00 02.
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x03, 0x00, 0x00, 0x00, 0x02]);

        // Pick a known DK; with dk.uv == MKB.uv (==2) and
        // dk.u_mask_shift == MKB.u_mask_shift (==3), dev_key_v_mask
        // equals the MKB's v_mask and the calc_pk_from_dk loop is a
        // no-op — Kp = aesg3(dk, 1).
        let dk_bytes: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let kp = aesg3(&dk_bytes, 1);

        // Plant Kmp with chosen byte-15, then compute C such that
        // AES-D(Kp, C) ⊕ uv == Kmp. uv=2 → low-4 bytes XOR is 00 00 00 02.
        let mut kmp = [0x42u8; 16];
        kmp[15] = kmp15;
        let mut aes_d_result = kmp;
        aes_d_result[15] ^= 0x02;
        let c_block = aes_ecb_encrypt(&kp, &aes_d_result);

        // cvalues record (0x07): the per-SD C the chain reads for `Kmp`. This
        // fixture has no `0x0c`, so both the walk and the chain fall back to
        // `0x07` — plant the computed `c_block` HERE so `AES-D(Kp, C) XOR uv ==
        // Kmp` and the chosen `kmp15` bit lands. On a variant MKB the per-match
        // magic check fails, but `variant_present` is true, so the walk still
        // returns the match.
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&c_block);

        // Verify Media Key (0x86): body content is don't-care.
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);

        // 0x2d record: VARIANTS table (head, don't-care for these tests) then the
        // trailing 16-byte Nonce (`variant_nonce` reads the tail), 32-byte body.
        // (C is NOT here — it is the `0x07`/`0x0c` cvalue above.)
        mkb.extend_from_slice(&[0x2d, 0x00, 0x00, 0x24]);
        mkb.extend_from_slice(&[0x11; 16]);
        mkb.extend_from_slice(&[0x77; 16]);

        // 0x2f record: Variant Key Data table — one 16-byte entry.
        mkb.extend_from_slice(&[0x2f, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xBB; 16]);

        let recs = walk_mkb(&mkb);

        let dk = DeviceKey {
            key: dk_bytes,
            node: 4,
            uv: 2,
            u_mask_shift: 3,
        };
        (recs, dk, kp, kmp)
    }

    // ════════════════════════════════════════════════════════════════════
    // Hardening additions
    // ════════════════════════════════════════════════════════════════════

    // ── walk_mkb framing: BE24 length incl. header, end markers ────────────

    #[test]
    fn walk_mkb_reports_offsets_and_be24_lengths() {
        // Two records; the walker must report each record's byte offset and
        // its full length (header + body). rec_len is the 3-byte BE field at
        // bytes 1..4, and INCLUDES the 4-byte header.
        let mut mkb = vec![0x10, 0x00, 0x00, 0x06, 0xAA, 0xBB]; // len 6 (2-byte body)
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x08, 1, 2, 3, 4]); // len 8
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].offset, 0);
        assert_eq!(recs[0].rec_len, 6);
        assert_eq!(recs[0].body, vec![0xAA, 0xBB]);
        assert_eq!(recs[1].offset, 6);
        assert_eq!(recs[1].rec_len, 8);
        assert_eq!(recs[1].body, vec![1, 2, 3, 4]);
    }

    #[test]
    fn mkb_records_matches_walk_mkb_framing() {
        // The lazy `mkb_records` iterator and the owning `walk_mkb` must agree on
        // (offset, type, len) for every record — they share the one framing
        // walker, and every aacs::resolve/derive MKB walk now relies on this equivalence.
        let mut mkb = vec![0x10, 0x00, 0x00, 0x06, 0xAA, 0xBB];
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x08, 1, 2, 3, 4]);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0xFF]); // terminator + trailing
        let owned: Vec<(usize, u8, usize)> = walk_mkb(&mkb)
            .iter()
            .map(|r| (r.offset, r.rec_type, r.rec_len))
            .collect();
        let lazy: Vec<(usize, u8, usize)> = mkb_records(&mkb).collect();
        assert_eq!(lazy, owned);
        assert_eq!(lazy, vec![(0, 0x10, 6), (6, 0x05, 8)]);
    }

    #[test]
    fn walk_mkb_be24_middle_byte_is_honored() {
        // A record longer than 255 bytes needs the MIDDLE BE24 byte: total
        // length 0x00_0110 (272) is `[0x00, 0x01, 0x10]`, so a parser reading
        // only the low byte sees 0x10. The HIGH byte of this length is zero, so
        // this test says nothing about the `<< 16` term — that is pinned
        // separately by `mkb::tests::mkb_records_honors_the_high_byte_of_the_be24_length`,
        // which uses a 0x01_0004 record. (Renamed from
        // `walk_mkb_be24_high_byte_is_honored`, which claimed coverage this body
        // does not deliver.)
        let total = 0x0110usize; // 272
        let mut mkb = vec![0x10, 0x00, 0x01, 0x10];
        mkb.resize(total, 0xAB);
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].rec_len, total);
        assert_eq!(recs[0].body.len(), total - 4);
    }

    #[test]
    fn walk_mkb_stops_at_type0_len0_end_marker() {
        // A (type=0, len=0) record ends the walk; trailing bytes after it are
        // not parsed.
        let mut mkb = vec![0x10, 0x00, 0x00, 0x06, 0xAA, 0xBB];
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // end marker
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x08, 9, 9, 9, 9]); // ignored
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].rec_type, 0x10);
    }

    #[test]
    fn walk_mkb_stops_on_overrun_record() {
        // rec_len running past the buffer ends the walk after the records that
        // fit (no OOB, no partial body past the end).
        let mut mkb = vec![0x10, 0x00, 0x00, 0x06, 0xAA, 0xBB];
        mkb.extend_from_slice(&[0x05, 0x00, 0xFF, 0xFF]); // claims 65535 bytes
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 1, "overrun record must be dropped");
    }

    #[test]
    fn walk_mkb_stops_on_sub_4_length() {
        // A non-zero type with rec_len < 4 (and not the 0/0 marker) breaks the
        // walk — otherwise pos would not advance (infinite loop guard).
        let mkb = vec![0x10, 0x00, 0x00, 0x02, 0xAA];
        assert!(walk_mkb(&mkb).is_empty());
    }

    #[test]
    fn walk_mkb_handles_trailing_partial_header() {
        // Fewer than 4 bytes left → loop condition `pos + 4 <= len` stops.
        let mkb = vec![0x10, 0x00, 0x00, 0x06, 0xAA, 0xBB, 0x05, 0x00]; // 2 trailing
        let recs = walk_mkb(&mkb);
        assert_eq!(recs.len(), 1);
    }

    // ── Record selectors ───────────────────────────────────────────────────

    #[test]
    fn is_variant_mkb_true_for_0x2d_alone_and_0x2f_alone() {
        // Either variant record type alone flags the MKB as variant.
        let only2d = walk_mkb(&{
            let mut m = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 0];
            m.extend_from_slice(&[0x2d, 0x00, 0x00, 0x14]);
            m.extend_from_slice(&[0xEE; 16]);
            m
        });
        assert!(is_variant_mkb(&only2d));
        let only2f = walk_mkb(&{
            let mut m = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 0];
            m.extend_from_slice(&[0x2f, 0x00, 0x00, 0x14]);
            m.extend_from_slice(&[0x55; 16]);
            m
        });
        assert!(is_variant_mkb(&only2f));
    }

    #[test]
    fn variant_nonce_requires_16_byte_body() {
        // A 0x2d record with < 16-byte body → None (no panic on the tail copy).
        let recs = walk_mkb(&{
            let mut m = vec![0x2d, 0x00, 0x00, 0x0C]; // 8-byte body
            m.extend_from_slice(&[0x11; 8]);
            m
        });
        assert_eq!(variant_nonce(&recs), None);
    }

    #[test]
    fn variant_key_data_requires_nonempty_multiple_of_16() {
        // A 0x2f VKD body that is NOT a multiple of 16 is rejected by
        // variant_key_data (it needs whole 16-byte VKD slots).
        let recs = walk_mkb(&{
            let mut m = vec![0x2f, 0x00, 0x00, 0x0E]; // 10-byte body (not %16)
            m.extend_from_slice(&[0x22; 10]);
            m
        });
        assert_eq!(variant_key_data(&recs), None);
        // variant_data_record reads 0x2d and returns its body regardless of length.
        let recs2 = walk_mkb(&{
            let mut m = vec![0x2d, 0x00, 0x00, 0x0E];
            m.extend_from_slice(&[0x33; 10]);
            m
        });
        assert_eq!(variant_data_record(&recs2), Some(&[0x33u8; 10][..]));
    }

    // ── derive_media_key_variant: missing-record classification ────────────

    #[test]
    fn chain_yields_no_key_for_non_covering_pk() {
        // A complete variant MKB but a Processing Key that covers no slot → no
        // Km verifies → an error (never a key). A non-covering key resolves to
        // ProcessingKeyUnavailable, or to a correction-mode classification if its
        // Kmp happens to set the soft/online bit — either way, no key is emitted.
        let (recs, _dk, _, _) = synthetic_variant_setup(0x00);
        let out = derive_media_key_variant(&recs, &[0x11; 16]);
        assert!(out.is_err(), "non-covering PK must not yield a Media Key");
        assert_ne!(out, Err(MediaKeyVariantError::NotVariantMkb));
        assert_ne!(out, Err(MediaKeyVariantError::MkbIncomplete));
    }

    #[test]
    fn chain_reports_mkb_incomplete_when_nonce_missing() {
        // Build a variant MKB (still variant via 0x2f, and a DK can walk it)
        // but WITHOUT the 0x2d record that carries C + the trailing Nonce →
        // MkbIncomplete at the variant_nonce `?`.
        let (recs, _dk, kp, _) = synthetic_variant_setup(0x00);
        // Reconstruct bytes without the 0x2d record.
        let mut mkb = Vec::new();
        for r in &recs {
            if r.rec_type == 0x2d {
                continue;
            }
            mkb.push(r.rec_type);
            mkb.push(((r.rec_len >> 16) & 0xFF) as u8);
            mkb.push(((r.rec_len >> 8) & 0xFF) as u8);
            mkb.push((r.rec_len & 0xFF) as u8);
            mkb.extend_from_slice(&r.body);
        }
        let recs2 = walk_mkb(&mkb);
        assert!(is_variant_mkb(&recs2), "still variant via 0x2f");
        let err = derive_media_key_variant(&recs2, &kp).expect_err("missing nonce → MkbIncomplete");
        assert_eq!(err, MediaKeyVariantError::MkbIncomplete);
    }

    // ── walk_processing_key: skips out-of-range u_mask_shift ───────────────

    #[test]
    fn walk_processing_key_skips_shift_32_to_63_without_panic() {
        // A subset-difference u_mask_shift in 0x20..=0x3F passes the 0xC0
        // revoke check but is out of range for a u32 shift. The walk must skip
        // the slot (continue) and not panic / not match a wrong uv. With only
        // that one bad slot, no match → None.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        // 0x04: u_mask_shift=0x20 (32), uv=2.
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x20, 0x00, 0x00, 0x00, 0x02]);
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);
        let recs = walk_mkb(&mkb);
        let dk = DeviceKey {
            key: [0x11; 16],
            node: 4,
            uv: 2,
            u_mask_shift: 3,
        };
        assert!(
            walk_processing_key(&recs, &[dk]).is_none(),
            "out-of-range shift must be skipped, yielding no match"
        );
    }

    #[test]
    fn walk_processing_key_skips_uv_zero() {
        // A uv == 0 slot is skipped (`if uv == 0 { continue }`). With only a
        // zero-uv slot present, no DK can match → None.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x03, 0x00, 0x00, 0x00, 0x00]); // uv = 0
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);
        let recs = walk_mkb(&mkb);
        let dk = DeviceKey {
            key: [0x11; 16],
            node: 4,
            uv: 2,
            u_mask_shift: 3,
        };
        assert!(walk_processing_key(&recs, &[dk]).is_none());
    }

    #[test]
    fn walk_processing_key_returns_match_on_variant_mkb_without_magic() {
        // On a variant MKB the per-match VERIFY_MAGIC check does not hold, but
        // the walk still returns the (Kp, uv) match because variant_present is
        // true. The synthetic_variant_setup fixture is exactly this case.
        let (recs, dk, planted_kp, _) = synthetic_variant_setup(0x00);
        let m = walk_processing_key(&recs, &[dk]).expect("variant MKB yields a match");
        assert_eq!(m.uv, 2, "matched the planted uv");
        assert_eq!(m.kp, planted_kp, "Kp equals aesg3(dk,1) for the no-op walk");
        assert_eq!(m.cvalue_index, 0);
    }

    #[test]
    fn aes_g_matches_decrypt_xor_relation() {
        // AES-G(x1,x2) = AES-128D(x1,x2) XOR x2 — the same form as derive_vuk.
        // Pin it explicitly so a dropped XOR or an encrypt-instead-of-decrypt
        // is caught.
        let x1 = [0x31u8; 16];
        let x2 = [0x9Fu8; 16];
        let mut expected = aes_ecb_decrypt(&x1, &x2);
        for i in 0..16 {
            expected[i] ^= x2[i];
        }
        assert_eq!(aes_g(&x1, &x2), expected);
    }

    #[test]
    fn error_codes_are_unique_and_in_7100_range() {
        // Each MediaKeyVariantError maps to a distinct E71xx code. A
        // copy-paste collision (two variants sharing a code) would break
        // operator triage; assert all are distinct.
        use std::collections::HashSet;
        let cases = [
            MediaKeyVariantError::NotVariantMkb,
            MediaKeyVariantError::MkbIncomplete,
            MediaKeyVariantError::ProcessingKeyUnavailable,
            MediaKeyVariantError::SoftCorrectionRequired,
            MediaKeyVariantError::OnlineChallengeRequired,
            MediaKeyVariantError::VariantsTableUnavailable,
            MediaKeyVariantError::VkdIndexOutOfRange,
            MediaKeyVariantError::MediaKeyVerifyFailed,
        ];
        let codes: HashSet<String> = cases.iter().map(|e| e.to_string()).collect();
        assert_eq!(codes.len(), cases.len(), "all error codes must be unique");
    }

    /// `media_key_variant_from_kp` runs the full chain from explicit inputs and
    /// classifies the `Kmp[15]` soft-correction bit. A `c_block` chosen so
    /// `AES-D(Kp, C) == Kmp` with bit `0x02` set (uv=0) must surface
    /// `SoftCorrectionRequired` before it touches the VKD / verify steps —
    /// proving the explicit-input entry runs the same chain and gates.
    #[test]
    fn media_key_variant_from_kp_classifies_soft_correction() {
        use crate::aacs::crypto::aes_ecb_encrypt;
        let kp = [0x11u8; 16];
        // Plant Kmp[15]=0x02 (soft-correction) with uv=0 so Kmp == AES-D(kp, C).
        let mut target_kmp = [0x00u8; 16];
        target_kmp[15] = 0x02;
        let c_block = aes_ecb_encrypt(&kp, &target_kmp);
        // Minimal variant MKB: 0x2d (16-byte body = tail Nonce), 0x2f (one VKD
        // entry), 0x86 (Verify-Media-Key).
        let mut mkb = vec![0x2d, 0x00, 0x00, 0x14];
        mkb.extend_from_slice(&[0x99; 16]);
        mkb.extend_from_slice(&[0x2f, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xBB; 16]);
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);
        let recs = walk_mkb(&mkb);
        let err = media_key_variant_from_kp(&kp, &c_block, 0, 0, &recs, &[0u8; 16])
            .expect_err("soft-correction bit → classified, not a key");
        assert_eq!(err, MediaKeyVariantError::SoftCorrectionRequired);
    }

    // ════════════════════════════════════════════════════════════════════
    // A COMPLETE variant MKB — the AACS 2.1 happy path
    //
    // Every other test in this module asserts an ERROR classification, so
    // until now no test ever drove `derive_media_key_variant` to a Media
    // Key. That left the whole success path — the VARIANTS lookup, the VKD
    // selection, the final `Km` unwrap and the verify gate — pinned by
    // nothing: a body that answered a constant for any of those steps still
    // produced the same errors these tests expect.
    //
    // No real key material is involved. Every AACS 2.1 relation in the chain
    // is invertible, so the fixture below picks a Media Key and a Processing
    // Key and computes the MKB records that connect them, exactly as
    // `derive::position_recovery_tests::plant_mkb` does for the classical
    // chain.
    // ════════════════════════════════════════════════════════════════════

    /// A planted variant MKB and the values it was built from.
    struct PlantedVariant {
        records: Vec<MkbRecord>,
        /// The Processing Key that covers slot 0.
        kp: [u8; 16],
        /// The Media Key the chain must derive from `kp`.
        km: [u8; 16],
        /// The `0x86` Verify-Media-Key block.
        mk_dv: [u8; 16],
        /// The `VARIANTS[0]` entry planted in the `0x2d` table.
        variants0: u16,
        /// The `0x2d` tail Nonce.
        nonce: [u8; 16],
        /// The slot-0 `0x0c` C block the Kmp step consumes.
        c_block: [u8; 16],
        /// The subset-difference number of the single planted slot.
        uv: u32,
    }

    /// An MKB record: 1-byte type + BE24 total length (header included) + body.
    fn vrec(t: u8, body: &[u8]) -> Vec<u8> {
        let total = 4 + body.len();
        let mut r = vec![
            t,
            ((total >> 16) & 0xFF) as u8,
            ((total >> 8) & 0xFF) as u8,
            (total & 0xFF) as u8,
        ];
        r.extend_from_slice(body);
        r
    }

    /// Build a variant MKB by inverting the 2.1 chain for a CHOSEN `(Kp, Km)`.
    ///
    /// One subset-difference slot (`uv = 2`, `u_mask_shift = 3`, slot index 0).
    /// The VKD the chain must land on is planted at index **1** of the `0x2f`
    /// table, behind a decoy at index 0, so `VARIANTS[0]` is load-bearing: it is
    /// chosen as `Kvn XOR 1`, and any other value selects the decoy (wrong `Km`,
    /// rejected by the verify gate) or indexes past the table.
    fn plant_variant_mkb() -> PlantedVariant {
        use crate::aacs::crypto::{aes_ecb_encrypt, aes_g};

        const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        const UV: u32 = 2;
        const U_MASK_SHIFT: u8 = 3;

        let kp: [u8; 16] = [
            0x2B, 0x7E, 0x15, 0x16, 0x28, 0xAE, 0xD2, 0xA6, 0xAB, 0xF7, 0x15, 0x88, 0x09, 0xCF,
            0x4F, 0x3C,
        ];
        // `uv = 2` puts its only non-zero byte at index 15, so byte 15 is the ONE
        // position where the `Km`/`Kmp` uv-XOR is observable. Its 0x02 bit is
        // deliberately CLEAR: with the bit set, `km[15] ^= 2` and `km[15] |= 2`
        // agree (the XOR would only be clearing a bit the OR re-sets) and an
        // OR-for-XOR substitution in the final step would be invisible.
        let km: [u8; 16] = [
            0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD,
            0xCE, 0xCD,
        ];
        assert_eq!(km[15] & 0x02, 0, "fixture check: see above");
        let nonce: [u8; 16] = [
            0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D,
            0x3E, 0x3F,
        ];
        let uv_bytes = UV.to_be_bytes();

        // ── Verify-Media-Key record (0x86): AES-D(Km, mk_dv) opens with the
        // magic ([C] §3.2.5.1.4), so mk_dv = AES-E(Km, magic || padding).
        let mut vd = [0x5Au8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&km, &vd);

        // ── C (0x0c): the chain computes Kmp = AES-D(Kp, C) XOR uv. Pick a Kmp
        // with BOTH condition bits on byte 15 clear (0x02 soft-correction,
        // 0x04 online challenge) so the default KCD path runs, then invert.
        let mut kmp = [0x42u8; 16];
        kmp[15] = 0x40; // neither 0x02 nor 0x04
        let mut c_plain = kmp;
        for i in 0..4 {
            c_plain[12 + i] ^= uv_bytes[i];
        }
        let c_block = aes_ecb_encrypt(&kp, &c_plain);

        // ── Kpnew = Kmp XOR KCD. Read through the production constant rather
        // than assuming it is zero, so the fixture stays valid if a real
        // per-licensee KCD is ever wired in (see `KEY_CORRECTION_DATA`).
        let mut kpnew = [0u8; 16];
        for i in 0..16 {
            kpnew[i] = kmp[i] ^ KEY_CORRECTION_DATA[i];
        }

        // ── VKD: the chain computes Km = AES-D(Kpnew, VKD) XOR uv, so
        // VKD = AES-E(Kpnew, Km with uv XORed back into its low 4 bytes).
        let mut km_pre = km;
        for i in 0..4 {
            km_pre[12 + i] ^= uv_bytes[i];
        }
        let vkd = aes_ecb_encrypt(&kpnew, &km_pre);

        // ── VARIANTS[0]: VKD_idx = Kvn XOR VARIANTS[uv], and we planted the
        // real VKD at table index 1, so VARIANTS[0] = Kvn XOR 1.
        // Kvn = low 16 bits (BE) of AES-G(Kp, Nonce).
        let kvn_block = aes_g(&kp, &nonce);
        let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);
        let variants0 = kvn ^ 1;

        // ── Assemble.
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&vrec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        // 0x04 subset-difference: one slot.
        let mut subdiff = vec![U_MASK_SHIFT];
        subdiff.extend_from_slice(&uv_bytes);
        mkb.extend_from_slice(&vrec(0x04, &subdiff));
        // 0x0c per-slot C table: one 16-byte entry.
        mkb.extend_from_slice(&vrec(0x0c, &c_block));
        // 0x86 Verify-Media-Key.
        mkb.extend_from_slice(&vrec(0x86, &mk_dv));
        // 0x2d: VARIANTS table (one BE u16) then the 16-byte tail Nonce.
        let mut vdata = Vec::new();
        vdata.extend_from_slice(&variants0.to_be_bytes());
        vdata.extend_from_slice(&nonce);
        mkb.extend_from_slice(&vrec(0x2d, &vdata));
        // 0x2f VKD table: a decoy at index 0, the real VKD at index 1.
        let mut vkd_table = vec![0x9Au8; 16];
        vkd_table.extend_from_slice(&vkd);
        mkb.extend_from_slice(&vrec(0x2f, &vkd_table));

        PlantedVariant {
            records: walk_mkb(&mkb),
            kp,
            km,
            mk_dv,
            variants0,
            nonce,
            c_block,
            uv: UV,
        }
    }

    /// Sanity-check the fixture before anything is asserted through it: an MKB
    /// the record finders cannot read would make every "returns an error" body
    /// look correct.
    #[test]
    fn the_planted_variant_mkb_is_a_well_formed_variant_mkb() {
        let p = plant_variant_mkb();
        assert!(is_variant_mkb(&p.records), "0x2d/0x2f present");
        assert_eq!(variant_nonce(&p.records), Some(p.nonce), "tail Nonce");
        assert_eq!(
            variant_key_data(&p.records).map(<[u8]>::len),
            Some(32),
            "two 16-byte VKD entries"
        );
        assert_eq!(
            variant_uv_slots(&p.records),
            Some(vec![(2u32, 0usize)]),
            "one subset-difference slot at index 0 with uv=2"
        );
    }

    /// THE happy path: a Processing Key covering slot 0 of a complete variant
    /// MKB must derive the planted Media Key.
    ///
    /// This is the assertion the whole 2.1 chain hangs from — `derive_media_key_variant`
    /// is what `resolve` calls for a 2.1 disc, and its output becomes the VUK,
    /// the title keys and every decrypted byte. The assertion lands on the FINAL
    /// derived Media Key, so no intermediate step (VARIANTS lookup, VKD index,
    /// Kpnew, the unwrap) can be replaced by a constant and still pass.
    #[test]
    fn variant_chain_derives_the_planted_media_key_for_a_covering_kp() {
        let p = plant_variant_mkb();
        assert_eq!(
            derive_media_key_variant(&p.records, &p.kp),
            Ok(p.km),
            "a covering 2.1 Processing Key must derive the planted Media Key"
        );
    }

    /// The other direction: a Processing Key one bit away must NOT yield a key.
    /// The terminal Verify-Media-Key gate is what stands between a wrong Kp and
    /// a wrong Media Key silently propagating into the VUK and title keys.
    #[test]
    fn variant_chain_yields_no_key_for_a_kp_one_bit_away() {
        let p = plant_variant_mkb();
        let mut stranger = p.kp;
        stranger[0] ^= 0x01;
        let got = derive_media_key_variant(&p.records, &stranger);
        assert!(
            got.is_err(),
            "a non-covering Kp must never produce a Media Key, got {got:?}"
        );
        assert_ne!(got, Ok(p.km));
    }

    /// `mkb_find_mk_dv` supplies the block the terminal verify gate compares
    /// against. A body answering a FIXED block would make the gate compare every
    /// derived Media Key against a record no disc carries: on a real disc every
    /// correct key is rejected (2.1 discs stop resolving entirely), and any key
    /// that happened to open the fixed block would be accepted wholesale.
    #[test]
    fn mkb_find_mk_dv_returns_the_verify_records_actual_bytes() {
        let p = plant_variant_mkb();
        assert_eq!(
            mkb_find_mk_dv(&p.records),
            Some(p.mk_dv),
            "mk_dv must be the bytes the 0x86 record carries"
        );
        assert_ne!(mkb_find_mk_dv(&p.records), Some([0u8; 16]));
        assert_ne!(mkb_find_mk_dv(&p.records), Some([1u8; 16]));

        // And it is the block the gate actually uses: swapping the 0x86 record
        // for an unrelated one must break the derivation that just succeeded.
        let mut recs = p.records.clone();
        let v = recs
            .iter_mut()
            .find(|r| r.rec_type == 0x86)
            .expect("verify record present");
        v.body = vec![0x00; 16];
        assert!(
            derive_media_key_variant(&recs, &p.kp).is_err(),
            "with a foreign verify block the same Kp must no longer verify"
        );
    }

    /// `variants_for_uv` reads `VARIANTS[slot]` — the value XORed with `Kvn` to
    /// index the VKD table. A body answering a constant picks the WRONG VKD
    /// entry for every disc, so the derived Media Key fails the verify gate and
    /// every 2.1 variant disc reports `ProcessingKeyUnavailable` with a
    /// perfectly good Processing Key in hand.
    ///
    /// Asserted two ways: the exact planted table entry, and — the load-bearing
    /// one — that this entry is what carries the chain to the planted Media Key.
    #[test]
    fn variants_for_uv_reads_the_planted_table_entry_that_selects_the_vkd() {
        let p = plant_variant_mkb();
        assert_eq!(
            variants_for_uv(&p.records, 0),
            Some(p.variants0),
            "slot 0 must read the planted VARIANTS entry"
        );
        // The planted entry is Kvn ^ 1 (the real VKD sits at table index 1), so
        // it is neither 0 nor 1 — a constant body is a different value here.
        assert_ne!(variants_for_uv(&p.records, 0), Some(0));
        assert_ne!(variants_for_uv(&p.records, 0), Some(1));

        // Perturbing ONLY the VARIANTS entry breaks the derivation: proof the
        // value this function returns is the one that selects the VKD.
        let mut recs = p.records.clone();
        let d = recs
            .iter_mut()
            .find(|r| r.rec_type == 0x2d)
            .expect("0x2d present");
        d.body[0] ^= 0x80;
        assert!(
            derive_media_key_variant(&recs, &p.kp).is_err(),
            "a different VARIANTS entry must select a different VKD and fail the gate"
        );
    }

    /// The `0x2d` body is `VARIANTS` table then a 16-byte tail Nonce. A slot
    /// index whose entry would fall inside the Nonce must be refused rather than
    /// read Nonce bytes as a VARIANTS value.
    #[test]
    fn variants_for_uv_stops_before_the_tail_nonce() {
        // Three-entry table with distinct values, then the Nonce.
        let mut body = Vec::new();
        body.extend_from_slice(&0x1234u16.to_be_bytes());
        body.extend_from_slice(&0xABCDu16.to_be_bytes());
        body.extend_from_slice(&0x00FFu16.to_be_bytes());
        let nonce = [0x77u8; 16];
        body.extend_from_slice(&nonce);
        let recs = walk_mkb(&vrec(0x2d, &body));

        assert_eq!(variants_for_uv(&recs, 0), Some(0x1234));
        assert_eq!(variants_for_uv(&recs, 1), Some(0xABCD));
        assert_eq!(variants_for_uv(&recs, 2), Some(0x00FF));
        assert_eq!(
            variants_for_uv(&recs, 3),
            None,
            "slot 3 starts inside the Nonce — must be refused, not read"
        );
        assert_eq!(variant_nonce(&recs), Some(nonce), "the Nonce is the tail");
    }

    /// `variant_uv_slots` enumerates the slots the chain will try a Processing
    /// Key against, and it must drop the two shapes that are unusable — and
    /// dangerous — rather than pass them on:
    ///
    ///   - `uv == 0`: no subset-difference. It would be XORed into `Kmp` and
    ///     `Km` as a no-op and the slot would be tried against every VKD entry.
    ///   - `u_mask_shift >= 32`: out of range for a `u32` shift. `0x20..=0x3F`
    ///     have the `0xC0` revoked-marker bits CLEAR, so they pass the table
    ///     terminator and reach the `wrapping_shl` in the walk, where shift 32
    ///     silently means shift 0 (`u_mask = 0xFFFF_FFFF`) and matches a slot
    ///     the device does not cover.
    ///
    /// Both bytes are disc-supplied. Every existing fixture uses one in-range
    /// non-zero slot, so neither rejection was executed.
    #[test]
    fn variant_uv_slots_drops_zero_uv_and_out_of_range_shift_slots() {
        // Four slots: uv == 0, shift == 32 (the exact boundary), shift == 0x3F
        // (the top of the marker-clear range), and one good slot last.
        let mut body = Vec::new();
        for (shift, uv) in [
            (3u8, 0u32),
            (32u8, 0x0000_0005u32),
            (0x3Fu8, 0x0000_0006u32),
            (12u8, 0x0000_0400u32),
        ] {
            body.push(shift);
            body.extend_from_slice(&uv.to_be_bytes());
        }
        // Fixture check: none of these bytes trips the 0xC0 table terminator, so
        // the per-slot tests are the only thing rejecting them.
        assert!(body.chunks(5).all(|c| c[0] & 0xC0 == 0));

        let recs = walk_mkb(&vrec(REC_SUBSET_DIFFERENCE, &body));
        assert_eq!(
            variant_uv_slots(&recs),
            Some(vec![(0x0000_0400u32, 3usize)]),
            "only the in-range, non-zero slot is a usable subset-difference — \
             and it keeps its own table index"
        );
    }

    /// THE happy path for the EXPLICIT-INPUT entry point. `media_key_variant_from_kp`
    /// is the harness twin of [`derive_media_key_variant`]: same chain, but the
    /// caller supplies the `0x0c` C block, the slot's `uv` and its `VARIANTS[uv]`
    /// instead of having them looked up on the MKB.
    ///
    /// Before this test, the ONLY test that entered this function asserted the
    /// `Kmp[15]` soft-correction bit — it returned before the Kpnew, Kvn, VKD,
    /// Km and Kvu steps ever ran. Every arithmetic step past that early return
    /// was executed by nothing, so a body that computed `Kpnew = Kmp | KCD`,
    /// indexed the VKD table at `Kvn + VARIANTS` or dropped the `uv` XOR out of
    /// `Km` produced exactly the same observable behaviour.
    ///
    /// The assertion lands on the returned `(Km, Kvu)` — the two values that
    /// become every title key and every decrypted byte on a 2.1 disc.
    #[test]
    fn media_key_variant_from_kp_derives_the_planted_media_key_and_volume_unique_key() {
        let p = plant_variant_mkb();
        let vid: [u8; 16] = [
            0x1A, 0x2B, 0x3C, 0x4D, 0x5E, 0x6F, 0x70, 0x81, 0x92, 0xA3, 0xB4, 0xC5, 0xD6, 0xE7,
            0xF8, 0x09,
        ];

        let (km, kvu) =
            media_key_variant_from_kp(&p.kp, &p.c_block, p.uv, p.variants0, &p.records, &vid)
                .expect("the planted explicit inputs must complete the 2.1 variant chain");

        assert_eq!(
            km, p.km,
            "the explicit-input entry must derive the same planted Media Key \
             the MKB-driven entry does"
        );
        // Kvu = AES-G(Km, VID) ([C] §3.2.5.2). Computed from the PLANTED Km
        // literal, so it does not move with any mutation of this module.
        assert_eq!(
            kvu,
            aes_g(&p.km, &vid),
            "Kvu must be AES-G of the derived Media Key with the Volume ID"
        );
        // ...and specifically NOT of the Processing Key: the two are one AES-D
        // apart and a body that returned the wrong one would still be 16 bytes
        // of key-shaped material that silently decrypts nothing.
        assert_ne!(kvu, aes_g(&p.kp, &vid));
    }

    /// The terminal gate on the explicit-input entry. `media_key_variant_from_kp`
    /// takes three caller-supplied values (`c_block`, `uv`, `variants_uv`); each
    /// one wrong must yield `MediaKeyVerifyFailed`, never a key. Without this,
    /// a harness feeding a mis-transcribed slot would be handed 16 bytes that
    /// look exactly like a Media Key.
    #[test]
    fn media_key_variant_from_kp_refuses_every_single_wrong_explicit_input() {
        let p = plant_variant_mkb();
        let vid = [0x33u8; 16];

        // Baseline: all three correct → a key.
        assert!(
            media_key_variant_from_kp(&p.kp, &p.c_block, p.uv, p.variants0, &p.records, &vid)
                .is_ok()
        );

        // Wrong C block: EVERY one-bit neighbour must fail to produce a key.
        // (Which classification it lands in depends on the two condition bits
        // the perturbed Kmp happens to carry — the property being pinned is
        // that none of the 128 reaches `Ok`.)
        for byte in 0..16usize {
            for bit in 0..8u32 {
                let mut c_bad = p.c_block;
                c_bad[byte] ^= 1u8 << bit;
                let got =
                    media_key_variant_from_kp(&p.kp, &c_bad, p.uv, p.variants0, &p.records, &vid);
                assert!(
                    got.is_err(),
                    "C block differing only in byte {byte} bit {bit} yielded a key"
                );
            }
        }

        // Wrong uv: it is XORed into BOTH Kmp and Km, so a wrong slot number
        // must not reach a key.
        for delta in 1..=8u32 {
            let got = media_key_variant_from_kp(
                &p.kp,
                &p.c_block,
                p.uv + delta,
                p.variants0,
                &p.records,
                &vid,
            );
            assert!(got.is_err(), "uv + {delta} must not verify, got {got:?}");
        }

        // Wrong VARIANTS[uv]: selects a different VKD entry. The planted table
        // has two entries, so `^ 1` lands on the decoy at index 0 (in range,
        // wrong key) rather than out of range.
        assert_eq!(
            media_key_variant_from_kp(&p.kp, &p.c_block, p.uv, p.variants0 ^ 1, &p.records, &vid),
            Err(MediaKeyVariantError::MediaKeyVerifyFailed),
            "a VARIANTS entry selecting the decoy VKD must not verify"
        );

        // And a VARIANTS entry that indexes off the end of the table is
        // classified as such, not read out of bounds.
        assert_eq!(
            media_key_variant_from_kp(
                &p.kp,
                &p.c_block,
                p.uv,
                p.variants0 ^ 0x8000,
                &p.records,
                &vid
            ),
            Err(MediaKeyVariantError::VkdIndexOutOfRange),
            "a VKD index past the table must be classified, not read"
        );
    }

    /// The `Kmp[15]` online-challenge bit (`0x04`) on the explicit-input entry.
    /// Its twin (`0x02`, soft correction) was already pinned; without this one a
    /// body that classified both bits as soft correction — or ignored `0x04` and
    /// ran the default-KCD chain to a wrong key — was unconstrained.
    #[test]
    fn media_key_variant_from_kp_classifies_online_challenge() {
        use crate::aacs::crypto::aes_ecb_encrypt;
        let p = plant_variant_mkb();
        // Plant Kmp[15] = 0x04 (online challenge, soft-correction bit CLEAR) and
        // invert the Kmp step for uv = 0 so Kmp == AES-D(kp, C).
        let mut target_kmp = [0x00u8; 16];
        target_kmp[15] = 0x04;
        let c_block = aes_ecb_encrypt(&p.kp, &target_kmp);
        assert_eq!(
            media_key_variant_from_kp(&p.kp, &c_block, 0, 0, &p.records, &[0u8; 16]),
            Err(MediaKeyVariantError::OnlineChallengeRequired),
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // A MULTI-SLOT variant MKB driven by a real DEVICE KEY
    //
    // `walk_processing_key` is the DK -> Kp step that feeds the whole 2.1
    // chain. Every existing test of it either asserts `None` (out-of-range
    // shift, uv == 0) or asserts only that SOME match came back — none pins
    // WHICH Processing Key, cvalue or slot index it returns. And every one of
    // them uses a SINGLE-slot MKB, where the slot index is 0: all the
    // `uvs[1 + 5*idx]` / `cvalues[idx*16..]` stride arithmetic multiplies by
    // zero and any stride at all gives the same answer.
    //
    // This fixture puts the covering slot at index 1, behind a decoy at
    // index 0, so the strides are load-bearing.
    // ════════════════════════════════════════════════════════════════════

    /// A two-slot variant MKB whose SECOND slot is opened by a device key.
    struct PlantedWalk {
        records: Vec<MkbRecord>,
        /// The device key that covers slot 1 with zero descent.
        dk: DeviceKey,
        /// The Processing Key the walk must produce for it.
        kp: [u8; 16],
        /// The Media Key the full chain must reach from that Processing Key.
        km: [u8; 16],
        /// The `0x0c` C block of slot 1 — the cvalue the walk must select.
        c_block1: [u8; 16],
    }

    /// Build a two-slot variant MKB keyed by a DEVICE key at slot **1**.
    ///
    /// Positions follow the same reasoning as the classical
    /// `derive::position_recovery_tests::plant_mkb`: `uv = 0x0400`
    /// (`u_mask_shift = 12`) with a device node of `0x0C00` satisfies the
    /// [C] §3.2.4 gate — equal under `u_mask = 0xFFFF_F000`, different under
    /// `v_mask = 0xFFFF_F800`. The device key's own `uv` equals the slot's, so
    /// `dev_key_v_mask == v_mask` and [`calc_pk_from_dk`] descends zero levels:
    /// `Kp = AES-G3(dk, 1)`, written out explicitly below rather than taken from
    /// the walk's own output.
    ///
    /// Slot 0 is a decoy at `uv = 0x0800`, which the SAME device node fails the
    /// `v_mask` half of the gate against (`0x0C00 & 0xFFFF_F000 == 0x0800 &
    /// 0xFFFF_F000`), so the walk must skip it and land on slot 1.
    fn plant_walk_variant_mkb() -> PlantedWalk {
        use crate::aacs::crypto::{aes_ecb_encrypt, aes_g};

        const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        const UV_DECOY: u32 = 0x0000_0800;
        const UV_REAL: u32 = 0x0000_0400;
        const U_MASK_SHIFT: u8 = 12;
        const NODE: u16 = 0x0C00;

        let dkey: [u8; 16] = [
            0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2,
            0xE1, 0xF0,
        ];
        // Zero descent: the Processing Key is the AES-G3(.,1) of the device's own
        // node ([C] §3.2.4). Written as the explicit primitive chain so it does
        // NOT move with any mutation of the walk under test.
        let kp = aesg3(&dkey, 1);

        // As in `plant_variant_mkb`: `uv = 0x0400`'s only non-zero byte is at
        // index 14, and its 0x04 bit must be CLEAR in `km` for the final
        // `km[14] ^= 0x04` to be distinguishable from `|=`.
        let km: [u8; 16] = [
            0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD,
            0xBA, 0xBF,
        ];
        assert_eq!(km[14] & 0x04, 0, "fixture check: see above");
        let nonce: [u8; 16] = [
            0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D,
            0x5E, 0x5F,
        ];

        // ── 0x86 Verify-Media-Key ([C] §3.2.5.1.4).
        let mut vd = [0x5Au8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&km, &vd);

        // ── C blocks. Both are built so `Kmp[15]` has the 0x02 / 0x04 condition
        // bits CLEAR, so both slots run the default-KCD path to completion and
        // the decoy is rejected by the terminal verify gate rather than
        // short-circuiting into a correction-mode classification.
        let c_for = |kmp: &[u8; 16], uv: u32| -> [u8; 16] {
            let mut c_plain = *kmp;
            for (b, u) in c_plain[12..16].iter_mut().zip(uv.to_be_bytes()) {
                *b ^= u;
            }
            aes_ecb_encrypt(&kp, &c_plain)
        };
        let mut kmp1 = [0x42u8; 16];
        kmp1[15] = 0x40;
        let c_block1 = c_for(&kmp1, UV_REAL);
        let mut kmp0 = [0x17u8; 16];
        kmp0[15] = 0x40;
        let c_block0 = c_for(&kmp0, UV_DECOY);

        // ── VKD for slot 1: Km = AES-D(Kpnew, VKD) XOR uv.
        let mut kpnew = [0u8; 16];
        for i in 0..16 {
            kpnew[i] = kmp1[i] ^ KEY_CORRECTION_DATA[i];
        }
        let mut km_pre = km;
        for (b, u) in km_pre[12..16].iter_mut().zip(UV_REAL.to_be_bytes()) {
            *b ^= u;
        }
        let vkd = aes_ecb_encrypt(&kpnew, &km_pre);

        // ── VARIANTS: the real VKD is planted at table index 2, behind two
        // decoys, so VARIANTS[1] = Kvn XOR 2 is load-bearing. VARIANTS[0] sends
        // the decoy slot to entry 0 — in range, wrong key, rejected by the gate.
        let kvn_block = aes_g(&kp, &nonce);
        let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);
        let variants0 = kvn;
        let variants1 = kvn ^ 2;

        // ── Assemble.
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&vrec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        let mut subdiff = vec![U_MASK_SHIFT];
        subdiff.extend_from_slice(&UV_DECOY.to_be_bytes());
        subdiff.push(U_MASK_SHIFT);
        subdiff.extend_from_slice(&UV_REAL.to_be_bytes());
        mkb.extend_from_slice(&vrec(0x04, &subdiff));
        let mut ctable = Vec::new();
        ctable.extend_from_slice(&c_block0);
        ctable.extend_from_slice(&c_block1);
        mkb.extend_from_slice(&vrec(0x0c, &ctable));
        mkb.extend_from_slice(&vrec(0x86, &mk_dv));
        let mut vdata = Vec::new();
        vdata.extend_from_slice(&variants0.to_be_bytes());
        vdata.extend_from_slice(&variants1.to_be_bytes());
        vdata.extend_from_slice(&nonce);
        mkb.extend_from_slice(&vrec(0x2d, &vdata));
        let mut vkd_table = vec![0x9Au8; 16];
        vkd_table.extend_from_slice(&[0x6Bu8; 16]);
        vkd_table.extend_from_slice(&vkd);
        mkb.extend_from_slice(&vrec(0x2f, &vkd_table));

        PlantedWalk {
            records: walk_mkb(&mkb),
            dk: DeviceKey {
                key: dkey,
                node: NODE,
                uv: UV_REAL,
                u_mask_shift: U_MASK_SHIFT,
            },
            kp,
            km,
            c_block1,
        }
    }

    /// Sanity-check the two-slot fixture before anything is asserted through it.
    #[test]
    fn the_planted_walk_variant_mkb_has_two_slots_and_is_keyed_at_the_second() {
        let p = plant_walk_variant_mkb();
        assert!(is_variant_mkb(&p.records));
        assert_eq!(
            variant_uv_slots(&p.records),
            Some(vec![(0x0800u32, 0usize), (0x0400u32, 1usize)]),
            "two subset-difference slots, the covering one at index 1"
        );
        assert_eq!(
            mkb_find_body(&p.records, REC_MEDIA_KEY_VARIANT_DATA).map(<[u8]>::len),
            Some(32),
            "two 16-byte C entries in the 0x0c table"
        );
    }

    /// `walk_processing_key` must return the Processing Key, `uv`, cvalue AND
    /// slot index of the covering slot — slot **1**, not slot 0.
    ///
    /// This is the DK → Kp step the entire 2.1 chain starts from. Every prior
    /// test of it asserted either `None` or merely `is_some()`, and all used a
    /// one-slot MKB where every stride multiplies by zero. A body that read the
    /// subset-difference at the wrong stride, sliced the wrong cvalue block, or
    /// returned the slot-0 cvalue for a slot-1 match would have passed all of
    /// them — and produced a Processing Key that opens nothing.
    ///
    /// The expected `Kp` is written as the explicit `AES-G3(dk, 1)` zero-descent
    /// relation from [C] §3.2.4, not taken from the walk's own output.
    #[test]
    fn walk_processing_key_returns_the_covering_slots_key_cvalue_and_index() {
        let p = plant_walk_variant_mkb();

        let m = walk_processing_key(&p.records, std::slice::from_ref(&p.dk))
            .expect("the planted device key covers slot 1 of this MKB");

        assert_eq!(m.uv, 0x0400, "the covering slot's uv, not the decoy's");
        assert_eq!(m.cvalue_index, 1, "the covering slot sits at index 1");
        assert_eq!(
            m.kp,
            aesg3(&p.dk.key, 1),
            "zero descent: Kp is AES-G3(device key, 1)"
        );
        assert_eq!(
            m.cvalue, p.c_block1,
            "the cvalue must be slot 1's 16-byte C block, not slot 0's"
        );

        // The load-bearing consequence: that Processing Key drives the full
        // variant chain to the planted Media Key.
        assert_eq!(
            derive_media_key_variant(&p.records, &m.kp),
            Ok(p.km),
            "the walked Processing Key must derive the planted Media Key"
        );
    }

    /// The gate the walk applies is [C] §3.2.4's subset-difference test, and a
    /// device key that fails it must get NO match. Pinned across all four
    /// coordinates the gate reads — node, uv, u_mask_shift and the key bytes —
    /// because a body that dropped any half of the gate would hand back a
    /// Processing Key derived at the wrong tree position.
    #[test]
    fn walk_processing_key_refuses_a_device_key_that_fails_the_subset_difference_gate() {
        let p = plant_walk_variant_mkb();
        assert!(walk_processing_key(&p.records, std::slice::from_ref(&p.dk)).is_some());

        // node equal to uv under v_mask (0xFFFF_F800): the "different under
        // v_mask" half of the gate fails.
        let mut d = p.dk.clone();
        d.node = 0x0400;
        assert!(
            walk_processing_key(&p.records, std::slice::from_ref(&d)).is_none(),
            "a node equal to uv under v_mask does not gate"
        );

        // node differing under u_mask (0xFFFF_F000): the "equal under u_mask"
        // half fails.
        let mut d = p.dk.clone();
        d.node = 0x1C00;
        assert!(
            walk_processing_key(&p.records, std::slice::from_ref(&d)).is_none(),
            "a node outside the slot's u_mask does not gate"
        );

        // A device key whose declared u_mask_shift is not the slot's.
        let mut d = p.dk.clone();
        d.u_mask_shift = 11;
        assert!(
            walk_processing_key(&p.records, std::slice::from_ref(&d)).is_none(),
            "u_mask must equal dev_key_u_mask"
        );

        // A device key positioned in a different subtree.
        let mut d = p.dk.clone();
        d.uv = 0x0C00;
        assert!(
            walk_processing_key(&p.records, std::slice::from_ref(&d)).is_none(),
            "the device key's uv must agree with the slot's under dev_key_v_mask"
        );
    }

    /// A `0x04` subset-difference record whose byte count is not a multiple of 5
    /// must have its trailing partial chunk REFUSED, not parsed as a slot.
    ///
    /// The walk sizes the table with `take_while(|c| c.len() == 5 && ...)`. Drop
    /// the length half of that conjunction and the partial chunk is counted, and
    /// the very next line reads `p_uv[0..4]` off a slice with fewer than four
    /// bytes left — an index-out-of-bounds PANIC on a disc-supplied record
    /// length. This is untrusted input: a truncated or crafted MKB reaches this
    /// with no other guard in between.
    #[test]
    fn a_trailing_partial_subset_difference_chunk_is_not_parsed_as_a_slot() {
        let p = plant_walk_variant_mkb();

        // Re-emit the 0x04 record with three trailing bytes — a partial chunk
        // whose first byte has the 0xC0 revoked-marker bits CLEAR, so only the
        // length test stands between it and a four-byte read off a one-byte tail.
        let mut recs = p.records.clone();
        let sd = recs
            .iter_mut()
            .find(|r| r.rec_type == REC_SUBSET_DIFFERENCE)
            .expect("0x04 present");
        assert_eq!(sd.body.len(), 10, "two whole slots before truncation");
        sd.body.extend_from_slice(&[0x0C, 0xAB, 0xCD]);

        // A device key that covers NOTHING, so the walk is forced to run past
        // both whole slots and reach the partial chunk.
        let mut stranger = p.dk.clone();
        stranger.node = 0x1C00;
        assert!(
            walk_processing_key(&recs, std::slice::from_ref(&stranger)).is_none(),
            "the partial chunk must terminate the table, not be walked"
        );

        // And the covering key still finds its slot with the junk appended.
        assert!(walk_processing_key(&recs, std::slice::from_ref(&p.dk)).is_some());
    }

    /// A `0x0c` cvalue table SHORTER than the matching slot index must make the
    /// walk skip the slot, not slice past the end of the record.
    ///
    /// `cvalues[uvs_idx * 16..(uvs_idx + 1) * 16]` is an unchecked slice; the
    /// only thing in front of it is `if uvs_idx >= cvalues.len() / 16`. The two
    /// counts come from DIFFERENT disc-supplied records (`0x04` and `0x0c`),
    /// so nothing but this guard keeps them in agreement — a real MKB with a
    /// short cvalue table panics the rip thread without it.
    #[test]
    fn a_cvalue_table_shorter_than_the_matching_slot_is_not_sliced_past() {
        let p = plant_walk_variant_mkb();
        let mut recs = p.records.clone();
        let cv = recs
            .iter_mut()
            .find(|r| r.rec_type == REC_MEDIA_KEY_VARIANT_DATA)
            .expect("0x0c present");
        // One entry only — the covering slot is index 1, so it is out of range.
        cv.body.truncate(16);
        assert!(
            walk_processing_key(&recs, std::slice::from_ref(&p.dk)).is_none(),
            "slot 1 with a one-entry cvalue table must be skipped, not read"
        );
    }

    /// The classical-magic escape hatch. On a NON-variant MKB the walk must
    /// return a match only when `AES-D(Kmp, mk_dv)` opens with the [C] §3.2.5.1.4
    /// verify magic; on a variant MKB that relation does not hold (the walk
    /// yields a Precursor) and the presence of `0x2d`/`0x2f` is what lets the
    /// match through to the chain's own terminal gate.
    ///
    /// Both halves of `classical_ok || variant_present` are pinned here: strip
    /// the variant records from a fixture whose magic does NOT hold and the walk
    /// must go quiet. Otherwise a body that dropped the guard entirely would
    /// return an unauthenticated Processing Key on every classical MKB.
    #[test]
    fn walk_processing_key_needs_either_the_verify_magic_or_variant_records() {
        let p = plant_walk_variant_mkb();
        // As planted (variant records present, magic absent) → a match.
        assert!(walk_processing_key(&p.records, std::slice::from_ref(&p.dk)).is_some());

        // Same slots, same device key, variant records removed. Nothing now
        // authenticates the Processing Key, so there must be no match.
        let stripped: Vec<MkbRecord> = p
            .records
            .iter()
            .filter(|r| r.rec_type != REC_VARIANT_DATA_AND_NONCE && r.rec_type != REC_VKD_TABLE)
            .cloned()
            .collect();
        assert!(
            !is_variant_mkb(&stripped),
            "fixture check: the stripped MKB is no longer a variant MKB"
        );
        assert!(
            walk_processing_key(&stripped, std::slice::from_ref(&p.dk)).is_none(),
            "without variant records the verify magic must hold, and it does not \
             for a Precursor — the walk must not return an unauthenticated key"
        );
    }

    /// The OTHER half of `classical_ok || variant_present`: a non-variant MKB
    /// whose cvalue really does open the Verify-Media-Key magic must yield a
    /// match, and the [C] §3.2.4 relation that produces the candidate — AES-D(Kp,
    /// cvalue) with `uv` XORed into the LOW FOUR BYTES — must be computed
    /// exactly.
    ///
    /// This is the only path on which that XOR is observable. On a variant MKB
    /// `variant_present` short-circuits the magic test, so the whole
    /// `km_candidate` computation is dead weight there: a body that ORed `uv`
    /// in, or XORed it at the wrong offset, changes nothing any variant fixture
    /// can see. On a CLASSICAL MKB it is the entire authentication of the
    /// Processing Key.
    #[test]
    fn walk_processing_key_authenticates_a_classical_match_through_the_verify_magic() {
        use crate::aacs::crypto::aes_ecb_encrypt;
        const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        const UV: u32 = 0x0000_0400;
        const U_MASK_SHIFT: u8 = 12;

        let dkey: [u8; 16] = [
            0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2,
            0xE1, 0xF0,
        ];
        // Zero descent ([C] §3.2.4), written out as the primitive relation.
        let kp = aesg3(&dkey, 1);
        // `uv = 0x0400` puts its only non-zero byte at index 14, so byte 14 is
        // the ONE position where the `uv` XOR is observable at all. Its 0x04 bit
        // is deliberately CLEAR here: with the bit set, `km_candidate[14] |=
        // 0x04` and `^= 0x04` agree (the XOR would only be clearing a bit the OR
        // re-sets), and an OR-for-XOR substitution would be invisible.
        let mk: [u8; 16] = [
            0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D,
            0x7A, 0x7F,
        ];
        assert_eq!(mk[14] & 0x04, 0, "fixture check: see above");

        // Invert [C] §3.2.4: the walk computes AES-D(Kp, cvalue) then XORs `uv`
        // into bytes 12..16 and expects the Media Key.
        let mut mk_raw = mk;
        for (b, u) in mk_raw[12..16].iter_mut().zip(UV.to_be_bytes()) {
            *b ^= u;
        }
        let cv = aes_ecb_encrypt(&kp, &mk_raw);

        // Invert [C] §3.2.5.1.4.
        let mut vd = [0x5Au8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        let mut subdiff = vec![U_MASK_SHIFT];
        subdiff.extend_from_slice(&UV.to_be_bytes());

        let mut mkb = Vec::new();
        mkb.extend_from_slice(&vrec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&vrec(0x86, &mk_dv));
        mkb.extend_from_slice(&vrec(0x04, &subdiff));
        // cvalues in the classical `0x05` record; NO 0x2d / 0x2f.
        mkb.extend_from_slice(&vrec(0x05, &cv));
        let recs = walk_mkb(&mkb);

        assert!(
            !is_variant_mkb(&recs),
            "fixture check: this must be a CLASSICAL MKB, so the magic is the \
             only thing that can let a match through"
        );

        let dk = DeviceKey {
            key: dkey,
            node: 0x0C00,
            uv: UV,
            u_mask_shift: U_MASK_SHIFT,
        };
        let m = walk_processing_key(&recs, std::slice::from_ref(&dk))
            .expect("the planted cvalue opens the verify magic for this key");
        assert_eq!(m.kp, aesg3(&dkey, 1));
        assert_eq!(m.uv, UV);
        assert_eq!(m.cvalue, cv);
        assert_eq!(m.cvalue_index, 0);

        // And the magic is genuinely load-bearing: perturb the Verify-Media-Key
        // record and the same key, slot and cvalue must stop matching.
        let mut bad = recs.clone();
        bad.iter_mut()
            .find(|r| r.rec_type == 0x86)
            .expect("0x86 present")
            .body[0] ^= 0x01;
        assert!(
            walk_processing_key(&bad, std::slice::from_ref(&dk)).is_none(),
            "a classical match must be authenticated by the verify magic"
        );

        // ...and so is the cvalue: one bit off and the candidate no longer opens
        // the magic.
        let mut bad = recs.clone();
        bad.iter_mut()
            .find(|r| r.rec_type == 0x05)
            .expect("0x05 present")
            .body[0] ^= 0x01;
        assert!(walk_processing_key(&bad, std::slice::from_ref(&dk)).is_none());
    }
}

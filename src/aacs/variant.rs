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
// on) are shared with the classical walk in [`super::keys`] — a single
// definition keeps the variant SD tree byte-identical to the classical one.
// (`aesg3` itself is imported separately in the test module.)
use super::derive::{calc_pk_from_dk, calc_v_mask};

/// Outcome of a subset-difference walk against an MKB. Carries the
/// processing key and the matching `uv` slot — both needed as inputs
/// to the variant chain.
#[derive(Debug, Clone, Copy)]
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
/// [`super::keys::derive_media_key_and_pk_from_dk`]. The two are kept
/// separate on purpose and select MKB records in DELIBERATELY different
/// order:
///
///   - cvalues: this variant walk tries record `0x07`-then-`0x05`; the
///     classical walk tries `0x05`-then-`0x07`. On a variant MKB the
///     small `0x07` Explicit-Subset-Difference record carries the
///     cvalue the Precursor chain consumes, whereas a classical UHD MKB
///     keeps its 1:1 cvalue table in the large `0x05` record (see the
///     note on [`super::keys::probe::mkb_cvalues`]). They must NOT be
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
    // These three live in `super::keys` now (consolidated SD-walk helpers);
    // `use super::*` does not re-export the parent module's private `use`
    // imports, so pull them in directly for the tests below.
    use super::super::crypto::aesg3;
    use super::super::derive::calc_pk_from_dk;

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
        // walker, and every keys.rs MKB walk now relies on this equivalence.
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
    fn walk_mkb_be24_high_byte_is_honored() {
        // A record longer than 255 bytes needs the high BE24 byte. Build a
        // 0x10 record of total length 0x000110 (272) and confirm the body is
        // 268 bytes (a parser that read only the low byte would see len 0x10).
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
}

//! AACS Media Key Variant chain.
//!
//! On AACS 2.1 the Media Key derivation gains a second stage on top of the classical
//! subset-difference walk: the walk yields a Media Key Precursor (Kmp), combined with disc VKD
//! and a per-licensee KCD constant to produce the Media Key. Entry point:
//! [`derive_media_key_variant`] (`Kp -> Km`); `Kp` comes from [`walk_processing_key`]. No
//! `0x2d`/`0x2f`/`0x0c` records falls back to [`super::derive`].
//!
//! ```text
//! Kmp     = AES-128D(Kp, C) XOR uv
//! Kpnew   = Kmp XOR KCD
//! Kvn     = AES-G(Kp, Nonce) & 0xFFFF   (low 16 bits, BE)
//! VKD_idx = Kvn XOR VARIANTS[uv]
//! VKD     = vkd_table[VKD_idx * 16 .. +16]
//! Km      = AES-128D(Kpnew, VKD) XOR uv
//! ```

use super::crypto::{aes_ecb_decrypt, aes_g};
use super::mkb::*;
use super::types::DeviceKey;

// ── Constants ─────────────────────────────────────────────────────────────

/// Zero placeholder KCD, NOT real key material — PER-LICENSEE.
const KEY_CORRECTION_DATA: [u8; 16] = [0u8; 16];

// ── MKB record walking ────────────────────────────────────────────────────

/// True iff `records` contains at least one Media Key Variant record.
///
/// The real AACS 2.1 Variant markers — confirmed against a live variant MKB —
/// are `0x2d` (Encrypted Media Key Variant Data / C) and `0x2f` (Variant Key
/// Data table, 65,535×16). Both are absent from non-variant 1.0/2.0 MKBs (which
/// carry only the classical `0x05` Media Key Data and no `0x0c`/`0x2d`/`0x2f`).
/// The earlier `0x82`/`0x83` guess was speculative and never appeared in any
/// real MKB.
pub fn is_variant_mkb(records: &[MkbRecord]) -> bool {
    records
        .iter()
        .any(|r| matches!(r.rec_type, REC_VARIANT_DATA_AND_NONCE | REC_VKD_TABLE))
}

/// Body of the `0x2d` record: `VARIANTS` table + trailing 16-byte Nonce. NOT the C used for
/// `Kmp` (that's `0x0c`'s per-slot block).
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

/// The Variant Key Data (VKD) table — record type `0x2f`. Disc-public data.
pub(crate) fn variant_key_data(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == REC_VKD_TABLE && !r.body.is_empty() && r.body.len() % 16 == 0)
        .map(|r| r.body.as_slice())
}

// ── Subset-difference walk that exposes (Kp, uv) ──────────────────────────

// Shared with the classical walk in super::derive to keep the SD tree byte-identical.
use super::derive::{VERIFY_MAGIC, calc_pk_from_dk, calc_v_mask};

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

fn records_find_mk_dv(records: &[MkbRecord]) -> Option<[u8; 16]> {
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
/// This is the AACS-2.1 **variant** walk (classical walk: [`super::derive`]). Kept separate on
/// purpose — different cvalue-record order (`0x0c` first) and input framing; do NOT route the
/// classical DK path through this function.
pub fn walk_processing_key(
    records: &[MkbRecord],
    device_keys: &[DeviceKey],
) -> Option<ProcessingKeyMatch> {
    let mk_dv = records_find_mk_dv(records)?;
    let uvs = mkb_find_body(records, REC_SUBSET_DIFFERENCE)?;
    // Real variant MKBs carry per-uv cvalues in record `0x0c` (46,101x16, one
    // per `0x04` slot); fall back to `0x05` (never the `0x07` SD index).
    let cvalues = mkb_find_body(records, REC_MEDIA_KEY_VARIANT_DATA)
        .or_else(|| mkb_find_body(records, REC_MEDIA_KEY_DATA))?;

    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for dk in device_keys {
        let device_number = dk.node as u32;

        for uvs_idx in 0..num_uvs {
            let p_uv = &uvs[1 + 5 * uvs_idx..];
            // `num_uvs` came from `take_while(.. (c[0] & 0xC0) == 0)`, so every
            // chunk in `0..num_uvs` already has clear revoked-marker bits.
            let u_mask_shift = uvs[5 * uvs_idx];

            // 0x20..=0x3F pass the take_while but are out of range for a u32
            // shift; `wrapping_shl` would silently wrap (shift % 32) and match
            // a wrong uv slot. Disc-controlled byte: skip the slot instead.
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
                    // On classical MKBs this magic must match. On variant MKBs
                    // it won't — `km_candidate` is really Kmp, so the magic
                    // check is moot; the chain enforces semantics downstream.
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

impl MediaKeyVariantError {
    /// The stable numeric error code (the `E71xx` family).
    pub fn code(&self) -> u16 {
        use crate::error::*;
        match self {
            MediaKeyVariantError::NotVariantMkb => E_MKB_VARIANT_NOT_VARIANT,
            MediaKeyVariantError::MkbIncomplete => E_MKB_VARIANT_INCOMPLETE,
            MediaKeyVariantError::ProcessingKeyUnavailable => E_MKB_VARIANT_PK_UNAVAILABLE,
            MediaKeyVariantError::SoftCorrectionRequired => E_MKB_VARIANT_SOFT_CORRECTION,
            MediaKeyVariantError::OnlineChallengeRequired => E_MKB_VARIANT_ONLINE_CHALLENGE,
            MediaKeyVariantError::VariantsTableUnavailable => E_MKB_VARIANT_TABLE_UNAVAILABLE,
            MediaKeyVariantError::VkdIndexOutOfRange => E_MKB_VARIANT_VKD_RANGE,
            MediaKeyVariantError::MediaKeyVerifyFailed => E_MKB_VARIANT_VERIFY_FAILED,
        }
    }
}

impl std::fmt::Display for MediaKeyVariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "E{}", self.code())
    }
}

impl std::error::Error for MediaKeyVariantError {}

// ── Chain ─────────────────────────────────────────────────────────────────

/// Look up the per-slot `VARIANTS[sd_slot_index]` (leading bytes of the `0x2d` body, before the
/// tail Nonce — see [`variant_nonce`]).
fn variants_for_uv(records: &[MkbRecord], sd_slot_index: usize) -> Option<u16> {
    let body = variant_data_record(records)?;
    // VARIANTS table is the leading bytes; the 16-byte Kvn Nonce is packed at the
    // TAIL (see [`variant_nonce`]). Bound reads to the table region so a near-end
    // slot never reads Nonce bytes (no header: v70 `0x2d` body = 46_100*2+16).
    const NONCE: usize = 16;
    let table_len = body.len().checked_sub(NONCE)?;
    let off = sd_slot_index.checked_mul(2)?;
    if off + 2 > table_len {
        return None;
    }
    Some(u16::from_be_bytes([body[off], body[off + 1]]))
}

/// Enumerate `(uv, slot_index)` pairs of a variant MKB's `0x04` record, in
/// table order — so a bare Processing Key can be tried against each slot.
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
    vkd_table: &'a [u8],
    /// Per-slot C table from `0x0c` (16 bytes/slot), same index
    /// [`walk_processing_key`] uses. NOT `0x2d` (VARIANTS + Nonce).
    cvalues: &'a [u8],
    mk_dv: [u8; 16],
    /// `Kvn = AES-G(Kp, Nonce) & 0xFFFF` (`[C]` §2.1.3 / this module's chain doc). Depends only
    /// on `Kp` and the MKB's Nonce — both loop-invariant across every slot a given `Kp` is tried
    /// against (L103) — so [`derive_media_key_variant`] computes it ONCE, not per slot.
    ///
    /// The closest published precedent for this `Kvn` shape is `[C]` §3.2.5.2.2 "Variant
    /// Number Record" (Table 3-12), PDF p.29: "Kvn = [AES-G(Kp, Nonce)]lsb_10" (10-bit, for a
    /// content-variation number, record type `0x0D`). This module's `0x2d`/`0x2f` chain is a
    /// distinct, reverse-engineered AACS 2.1 scheme — same `AES-G(Kp, Nonce)` shape, 16-bit.
    kvn: u16,
}

/// Derive+verify the Media Key for ONE known `(Kp, uv, slot)`. VID-free —
/// the VUK is a separate [`super::derive::derive_vuk`] step.
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

    let variants_uv = variants_for_uv(m.records, slot_index);
    km_from_slot_inputs(kp, &c_block, uv, variants_uv, m.kvn, m.vkd_table, &m.mk_dv)
}

/// Shared Kp -> Km chain (Kmp, correction bits, Kpnew, VKD, Km, Verify-Media-Key gate) for one
/// slot's explicit inputs. `variants_uv` is `VARIANTS[uv]`; `None` means the table is short.
fn km_from_slot_inputs(
    kp: &[u8; 16],
    c_block: &[u8; 16],
    uv: u32,
    variants_uv: Option<u16>,
    kvn: u16,
    vkd_table: &[u8],
    mk_dv: &[u8; 16],
) -> Result<[u8; 16], MediaKeyVariantError> {
    // Kmp = AES-128D(Kp, C) XOR uv (uv into the low 4 bytes).
    let mut kmp = aes_ecb_decrypt(kp, c_block);
    let uv_bytes = uv.to_be_bytes();
    for i in 0..4 {
        kmp[12 + i] ^= uv_bytes[i];
    }

    // Bits 0x02 (SoftKCD) / 0x04 (online challenge) need out-of-band data we don't model.
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

    // VKD_idx = Kvn XOR VARIANTS[uv]; Kvn is loop-invariant for a Kp, hoisted by the caller.
    let variants_uv = variants_uv.ok_or(MediaKeyVariantError::VariantsTableUnavailable)?;
    let off = ((kvn ^ variants_uv) as usize) * 16;
    if off + 16 > vkd_table.len() {
        return Err(MediaKeyVariantError::VkdIndexOutOfRange);
    }
    let mut vkd = [0u8; 16];
    vkd.copy_from_slice(&vkd_table[off..off + 16]);

    // Km = AES-128D(Kpnew, VKD) XOR uv.
    let mut km = aes_ecb_decrypt(&kpnew, &vkd);
    for i in 0..4 {
        km[12 + i] ^= uv_bytes[i];
    }

    // Authoritative gate: Km must reproduce the MKB's Verify-Media-Key magic.
    if aes_ecb_decrypt(&km, mk_dv)[..8] != VERIFY_MAGIC {
        return Err(MediaKeyVariantError::MediaKeyVerifyFailed);
    }
    Ok(km)
}

/// Derive the AACS 2.1 variant **Media Key** from a Processing Key.
///
/// The one deterministic `Kp → Km` derivation for a variant MKB: tries `pk` (which arrives
/// without its slot) against every slot and returns the Km for the slot whose full chain passes
/// the Verify-Media-Key record, so an unverified key is never returned. VID-free — derive the
/// VUK via [`super::derive::derive_vuk`]; `Kp` comes from [`walk_processing_key`]. Errors:
/// `NotVariantMkb`, `MkbIncomplete` (also a slot's short tables), `ProcessingKeyUnavailable`
/// (no slot verified), or, when a slot needs it, `SoftCorrectionRequired` /
/// `OnlineChallengeRequired` / `VariantsTableUnavailable`.
pub fn derive_media_key_variant(
    mkb_records: &[MkbRecord],
    pk: &[u8; 16],
) -> Result<[u8; 16], MediaKeyVariantError> {
    if !is_variant_mkb(mkb_records) {
        return Err(MediaKeyVariantError::NotVariantMkb);
    }
    let nonce = variant_nonce(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let vkd_table = variant_key_data(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    // C for Kmp is the per-slot `0x0c` table, same source/index as
    // `walk_processing_key` uses; `0x2d` holds VARIANTS + Nonce, not C. Fall
    // back to `0x05` (never the `0x07` SD index).
    let cvalues = mkb_find_body(mkb_records, REC_MEDIA_KEY_VARIANT_DATA)
        .or_else(|| mkb_find_body(mkb_records, REC_MEDIA_KEY_DATA))
        .ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let mk_dv = records_find_mk_dv(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let slots = variant_uv_slots(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    // Kvn = AES-G(Kp, Nonce) & 0xFFFF depends only on `pk` and the MKB's Nonce — neither
    // varies per slot — so compute it ONCE here rather than once per slot in
    // `variant_km_for_slot` (L103: was tens of ms/disc across ~46k slots).
    let kvn_block = aes_g(pk, &nonce);
    let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);
    let m = VariantMkb {
        records: mkb_records,
        vkd_table,
        cvalues,
        mk_dv,
        kvn,
    };

    // Try `pk` against each slot; return the first verified Km. If none verify,
    // surface a correction-mode error, then a key-independent STRUCTURAL fault (the
    // MKB's tables are short for a slot), over the generic miss.
    let mut correction: Option<MediaKeyVariantError> = None;
    let mut structural: Option<MediaKeyVariantError> = None;
    for (uv, slot_index) in slots {
        match variant_km_for_slot(&m, pk, uv, slot_index) {
            Ok(km) => return Ok(km),
            Err(e @ MediaKeyVariantError::SoftCorrectionRequired)
            | Err(e @ MediaKeyVariantError::OnlineChallengeRequired) => {
                correction.get_or_insert(e);
            }
            Err(e @ MediaKeyVariantError::MkbIncomplete)
            | Err(e @ MediaKeyVariantError::VariantsTableUnavailable) => {
                structural.get_or_insert(e);
            }
            // Verify failure / VKD index out of range depend on `pk`: a plain miss.
            Err(_) => {}
        }
    }
    Err(correction
        .or(structural)
        .unwrap_or(MediaKeyVariantError::ProcessingKeyUnavailable))
}

/// Run the variant chain from a caller-supplied Processing Key and EXPLICIT
/// per-slot inputs (`0x0c` C block, `uv`, `VARIANTS[uv]`) — bypasses the
/// device-key walk and the on-MKB `VARIANTS[uv]` lookup; the MKB still
/// supplies the Nonce, VKD table, and Verify-Media-Key value.
///
/// Returns `(Km, Kvu)`. The terminal gate is identical to [`derive_media_key_variant`]: a wrong
/// input returns [`MediaKeyVariantError::MediaKeyVerifyFailed`].
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
    let mk_dv = records_find_mk_dv(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;

    let kvn_block = aes_g(kp, &nonce);
    let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);
    let km = km_from_slot_inputs(kp, c_block, uv, Some(variants_uv), kvn, vkd_table, &mk_dv)?;

    // Kvu = AES-G(Km, VID).
    let kvu = aes_g(&km, vid);
    Ok((km, kvu))
}

#[cfg(test)]
#[path = "variant_tests.rs"]
mod tests;

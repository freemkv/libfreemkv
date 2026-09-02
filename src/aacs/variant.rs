//! AACS Media Key Variant chain.
//!
//! On AACS 2.1 the Media Key derivation gains a second stage on top of
//! the classical subset-difference walk: the walk yields a Media Key
//! Precursor (Kmp), combined with disc VKD and a per-licensee KCD
//! constant to produce the Media Key. Entry point:
//! [`derive_media_key_variant`] (`Kp -> Km`); `Kp` comes from
//! [`walk_processing_key`]. No `0x2d`/`0x2f`/`0x0c` records falls back
//! to [`super::derive`]. See docs/variant.md for layout/gate notes.
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

// See docs/variant.md — MKB records this chain selects.

// ── Public constants ──────────────────────────────────────────────────────

/// Zero placeholder KCD, NOT real key material — PER-LICENSEE.
/// See docs/variant.md — KEY_CORRECTION_DATA for the consequence.
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

/// Body of the `0x2d` record: `VARIANTS` table + trailing 16-byte Nonce.
/// NOT the C used for `Kmp` (that's `0x0c`'s per-slot block).
/// See docs/variant.md — variant_data_record for the measured layout.
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
/// Disc-public data; see docs/variant.md — variant_key_data for sizing.
pub(crate) fn variant_key_data(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == REC_VKD_TABLE && !r.body.is_empty() && r.body.len() % 16 == 0)
        .map(|r| r.body.as_slice())
}

// ── Subset-difference walk that exposes (Kp, uv) ──────────────────────────

// Shared with the classical walk in super::derive to keep the SD tree
// byte-identical. See docs/variant.md — subset-difference walk sharing.
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
/// This is the AACS-2.1 **variant** walk (classical walk: [`super::derive`]).
/// Kept separate on purpose — different cvalue-record order and input
/// framing; do NOT route the classical DK path through this function, or
/// the `0x07`-first selection picks the wrong cvalue and returns `None`.
/// See docs/variant.md — walk_processing_key for the full rationale.
pub fn walk_processing_key(
    records: &[MkbRecord],
    device_keys: &[DeviceKey],
) -> Option<ProcessingKeyMatch> {
    let mk_dv = mkb_find_mk_dv(records)?;
    let uvs = mkb_find_body(records, REC_SUBSET_DIFFERENCE)?;
    // Real variant MKBs carry per-uv cvalues in record `0x0c` (46,101x16, one
    // per `0x04` slot); fall back to `0x07`/`0x05` for synthetic fixtures.
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
                    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
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

/// Look up the per-slot `VARIANTS[sd_slot_index]` (leading bytes of the
/// `0x2d` body, before the tail Nonce — see [`variant_nonce`]).
/// See docs/variant.md — variants_for_uv for the measured layout.
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
    nonce: [u8; 16],
    vkd_table: &'a [u8],
    /// Per-slot C table from `0x0c` (16 bytes/slot), same index
    /// [`walk_processing_key`] uses. NOT `0x2d` (VARIANTS + Nonce).
    cvalues: &'a [u8],
    mk_dv: [u8; 16],
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
/// The one deterministic `Kp → Km` derivation for a variant MKB: tries
/// `pk` (which arrives without its slot) against every slot and returns
/// the Km for the slot whose full chain passes the Verify-Media-Key
/// record, so an unverified key is never returned. VID-free — derive the
/// VUK via [`super::derive::derive_vuk`]; `Kp` comes from
/// [`walk_processing_key`]. Errors: `NotVariantMkb`/`MkbIncomplete`/
/// `ProcessingKeyUnavailable`; see docs/variant.md — derive_media_key_variant.
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
    // back to `0x07`/`0x05` for synthetic fixtures with a single cvalue.
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
/// per-slot inputs (`0x0c` C block, `uv`, `VARIANTS[uv]`) — bypasses the
/// device-key walk and the on-MKB `VARIANTS[uv]` lookup; the MKB still
/// supplies the Nonce, VKD table, and Verify-Media-Key value.
///
/// Returns `(Km, Kvu)`. The terminal gate is identical to
/// [`derive_media_key_variant`]: a wrong input returns
/// [`MediaKeyVariantError::MediaKeyVerifyFailed`]. See docs/variant.md —
/// media_key_variant_from_kp for the [`KEY_CORRECTION_DATA`] caveat.
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
        // Regression for the unbounded-loop hang: dev_key_v_mask has the MSB
        // set, so the `>> 1` walk sign-extends and saturates at 0xFFFF_FFFF,
        // never reaching v_mask. The 32-step bound must still return.
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

    /// Build a synthetic variant MKB + DK walking its one SD slot. `kmp15`
    /// picks `Kmp[15]` (`0x02`=SoftCorrection, `0x04`=OnlineChallenge,
    /// `0x00`=neither). See docs/variant.md — synthetic_variant_setup.
    fn synthetic_variant_setup(kmp15: u8) -> (Vec<MkbRecord>, DeviceKey, [u8; 16], [u8; 16]) {
        use crate::aacs::crypto::aes_ecb_encrypt;

        // Build header.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];

        // Subset-difference (0x04): u_mask_shift=3, uv=00 00 00 02.
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x03, 0x00, 0x00, 0x00, 0x02]);

        // Known DK with dk.uv == MKB.uv (==2) and dk.u_mask_shift == MKB's
        // (==3): dev_key_v_mask == MKB's v_mask, so calc_pk_from_dk is a
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

        // cvalues record (0x07): no `0x0c` here, so walk and chain both fall
        // back to `0x07` — plant `c_block` so AES-D(Kp,C)^uv == Kmp. Magic
        // check fails on variant MKBs, but `variant_present` lets it match.
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

    // ── Hardening additions ─────────────────────────────────────────────────

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
        // Needs the MIDDLE BE24 byte: length 0x00_0110 (272) is [0x00,0x01,0x10],
        // so a low-byte-only reader sees 0x10. HIGH byte is zero here; that term
        // is pinned by `mkb::tests::mkb_records_honors_the_high_byte_of_the_be24_length`.
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
        // Complete variant MKB, but PK covers no slot → no Km verifies → error.
        // Resolves to ProcessingKeyUnavailable, or a correction-mode
        // classification if Kmp sets the soft/online bit; either way, no key.
        let (recs, _dk, _, _) = synthetic_variant_setup(0x00);
        let out = derive_media_key_variant(&recs, &[0x11; 16]);
        assert!(out.is_err(), "non-covering PK must not yield a Media Key");
        assert_ne!(out, Err(MediaKeyVariantError::NotVariantMkb));
        assert_ne!(out, Err(MediaKeyVariantError::MkbIncomplete));
    }

    #[test]
    fn chain_reports_mkb_incomplete_when_nonce_missing() {
        // Variant MKB (still variant via 0x2f, DK can walk it) but WITHOUT the
        // 0x2d record carrying C + Nonce → MkbIncomplete at variant_nonce `?`.
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
        // u_mask_shift in 0x20..=0x3F passes the 0xC0 revoke check but is out
        // of u32-shift range. Walk must skip the slot without panicking; with
        // only this bad slot present, result is None.
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

    /// A `c_block` planting `Kmp[15]` bit `0x02` must surface
    /// `SoftCorrectionRequired` before the VKD/verify steps.
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

    // A COMPLETE variant MKB — the AACS 2.1 happy path. Every other test here
    // asserts an ERROR, leaving the success path (VARIANTS/VKD lookup, Km
    // unwrap, verify gate) unpinned; fixture inverts the chain, no real keys.

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
    /// See docs/variant.md — Test fixture: plant_variant_mkb.
    fn plant_variant_mkb() -> PlantedVariant {
        use crate::aacs::crypto::{aes_ecb_encrypt, aes_g};

        const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        const UV: u32 = 2;
        const U_MASK_SHIFT: u8 = 3;

        let kp: [u8; 16] = [
            0x2B, 0x7E, 0x15, 0x16, 0x28, 0xAE, 0xD2, 0xA6, 0xAB, 0xF7, 0x15, 0x88, 0x09, 0xCF,
            0x4F, 0x3C,
        ];
        // `uv = 2` has its only non-zero byte at index 15, the one position where
        // the uv-XOR is observable. Its 0x02 bit is deliberately CLEAR: if set,
        // `^= 2` and `|= 2` would agree, hiding an OR-for-XOR substitution bug.
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

    /// THE happy path: a covering Processing Key must derive the planted
    /// Media Key. See docs/variant.md — Test: ..._for_a_covering_kp.
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

    /// `mkb_find_mk_dv` must supply the ACTUAL `0x86` bytes, not a fixed
    /// block. See docs/variant.md — Test: mkb_find_mk_dv_returns_....
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

    /// `variants_for_uv` must read the REAL `VARIANTS[slot]`, not a
    /// constant. See docs/variant.md — Test: variants_for_uv_reads_the_....
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

    /// `variant_uv_slots` must drop `uv == 0` and `u_mask_shift >= 32`
    /// slots. See docs/variant.md — Test: ..._drops_zero_uv_and_out_of_range.
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

    /// THE happy path for the EXPLICIT-INPUT entry point: must derive the
    /// planted `(Km, Kvu)`. See docs/variant.md — Test: ..._derives_the_....
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

    /// Each caller-supplied value (`c_block`, `uv`, `variants_uv`) wrong
    /// must yield an error, never a key. See docs/variant.md — Test: ..._refuses_every_....
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
        // Classification varies with the perturbed Kmp's condition bits; the
        // property pinned here is that none of the 128 reaches `Ok`.
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

    /// The `Kmp[15]` online-challenge bit (`0x04`), the `0x02` twin of
    /// `media_key_variant_from_kp_classifies_soft_correction`.
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

    // A MULTI-SLOT variant MKB driven by a real DEVICE KEY. Prior tests used
    // single-slot MKBs (index 0), where stride arithmetic multiplies by zero;
    // this puts the covering slot at index 1 behind a decoy, so strides matter.

    /// A two-slot variant MKB whose SECOND slot is opened by a device key.
    struct PlantedWalk {
        records: Vec<MkbRecord>,
        /// The device key that covers slot 1 with zero descent.
        dk: DeviceKey,
        /// The Media Key the full chain must reach from that Processing Key.
        km: [u8; 16],
        /// The `0x0c` C block of slot 1 — the cvalue the walk must select.
        c_block1: [u8; 16],
    }

    /// Build a two-slot variant MKB keyed by a DEVICE key at slot **1**,
    /// behind a decoy at slot 0. See docs/variant.md — Test fixture: plant_walk_variant_mkb.
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

        // C blocks: both keep `Kmp[15]`'s 0x02/0x04 bits CLEAR, so both slots
        // run the default-KCD path and the decoy is rejected by the terminal
        // verify gate instead of short-circuiting into correction-mode.
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

    /// `walk_processing_key` must return the Kp/uv/cvalue/index of the
    /// COVERING slot — slot **1**, not slot 0. See docs/variant.md — Test:
    /// walk_processing_key_returns_the_covering_slots_key_cvalue_and_index.
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

    /// The `[C]` §3.2.4 subset-difference gate must reject a device key on
    /// EVERY one of its four coordinates (node/uv/u_mask_shift/key).
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

    /// A trailing partial `0x04` chunk (not a multiple of 5 bytes) must be
    /// REFUSED, not parsed — else an OOB PANIC on disc-supplied length.
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

    /// A `0x0c` cvalue table SHORTER than the matching slot index must be
    /// skipped, not sliced past — else an OOB panic on a real MKB.
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

    /// Both halves of `classical_ok || variant_present` must hold: strip the
    /// variant records from a fixture whose magic does NOT hold and the
    /// walk must go quiet. See docs/variant.md — Test: ..._needs_either_....
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

    /// The OTHER half of `classical_ok || variant_present`: a classical MKB
    /// whose cvalue opens the magic must match, computed exactly (`uv`
    /// XORed into the LOW FOUR BYTES). See docs/variant.md — Test: ..._authenticates_....
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
        // `uv = 0x0400` has its only non-zero byte at index 14, the one position
        // where the uv XOR is observable. Its 0x04 bit is CLEAR: if set, `|= 0x04`
        // and `^= 0x04` would agree, hiding an OR-for-XOR substitution bug.
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

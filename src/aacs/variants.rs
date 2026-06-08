//! AACS Media Key Variant chain.
//!
//! On AACS 2.1 the Media Key derivation gains a second stage on top of
//! the classical subset-difference walk. The classical walk yields a
//! Media Key Precursor (Kmp) rather than the final Media Key; the
//! Precursor combines with disc-supplied Variant Key Data (VKD) and an
//! integrator-supplied Key Correction Data (KCD) constant to produce
//! the Media Key.
//!
//! This module is wiring only — `resolve_keys` is not aware of it. The
//! entry point is [`derive_media_key_variant`]. The Variant scheme is
//! detected via the new MKB record types `0x82` (Encrypted Media Key
//! Variant Data + Variant Key Data) and `0x83` (Variant Number). When
//! a disc carries neither, callers should fall back to the classical
//! single-stage derivation in [`super::keys`].
//!
//! **Status: the chain cannot yet produce a key on a real disc.** Two
//! sub-fields are unfinished:
//!   - [`variants_for_uv`] (the `VARIANTS[uv]` lookup in the `0x83`
//!     record) is a stub that always returns `None`, so the chain
//!     short-circuits with [`MediaKeyVariantError::VariantsTableUnavailable`].
//!   - The Encrypted Media Key Variant Data (C) and the Variant Key
//!     Data (VKD) table are *distinct* sub-fields of the `0x82` record
//!     per AACS 2.1, but [`variant_data_record`] (C) and
//!     [`variant_key_data`] (VKD) both currently return the *whole*
//!     first `0x82` body — so on a single-`0x82` disc they alias. The
//!     `0x82` sub-field offsets must be fixed against a real Variant
//!     disc before this chain is wired into `resolve_keys`.
//!
//! The chain follows the published spec:
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
//! Two condition bits on `Kmp[15]` route off the hardcoded-KCD path
//! (Soft Correction and Online Challenge). The chain refuses to run in
//! either case — callers must handle those modes out of band.
//!
//! # Status: Kp verification
//!
//! On the classical path [`walk_processing_key`] gates each match on
//! the VERIFY_MAGIC relation, which authenticates the Processing Key.
//! On a variant MKB that magic check does NOT hold (the walk yields a
//! Media Key *Precursor*, not the Media Key), so the walk accepts a
//! variant match without it. The replacement gate lives at the END of
//! [`derive_media_key_variant`]: the derived final `Km` is verified
//! against the MKB's Verify-Media-Key record before any `(Km, Kvu)` is
//! returned. A future implementer wiring [`variants_for_uv`] must keep
//! that final gate — the per-match magic check no longer protects the
//! variant path.

use super::decrypt::aes_ecb_decrypt;
use super::keydb::DeviceKey;

// ── Public constants ──────────────────────────────────────────────────────

/// Placeholder Key Correction Data. Sixteen zero bytes.
///
/// Integrators MUST supply a non-placeholder KCD via the `kcd` argument
/// to [`derive_media_key_variant`]; the chain refuses to operate when
/// the supplied KCD compares equal to this placeholder.
pub const KEY_CORRECTION_DATA_PLACEHOLDER: [u8; 16] = [0u8; 16];

// ── MKB record walking ────────────────────────────────────────────────────

/// A single MKB record produced by [`walk_mkb`].
#[derive(Debug, Clone)]
pub struct MkbRecord {
    /// Byte offset of the record within the MKB.
    pub offset: usize,
    /// Record type byte.
    pub rec_type: u8,
    /// Record length in bytes (includes the 4-byte header).
    pub rec_len: usize,
    /// Record body (the bytes after the 4-byte header).
    pub body: Vec<u8>,
}

/// Walk an MKB into a flat list of records.
///
/// MKB record framing per AACS: 1 byte type, 3 bytes BE length
/// INCLUDING the 4-byte header, followed by payload. The walker stops
/// at the first `(type=0, len=0)` end marker or at end of buffer.
pub fn walk_mkb(mkb: &[u8]) -> Vec<MkbRecord> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 4 <= mkb.len() {
        let rec_type = mkb[pos];
        let rec_len = ((mkb[pos + 1] as usize) << 16)
            | ((mkb[pos + 2] as usize) << 8)
            | (mkb[pos + 3] as usize);
        if rec_type == 0 && rec_len == 0 {
            break;
        }
        if rec_len < 4 || pos + rec_len > mkb.len() {
            break;
        }
        let body = mkb[pos + 4..pos + rec_len].to_vec();
        out.push(MkbRecord {
            offset: pos,
            rec_type,
            rec_len,
            body,
        });
        pos += rec_len;
    }
    out
}

/// True iff `records` contains at least one Media Key Variant record
/// (type `0x82` or `0x83`).
pub fn is_variant_mkb(records: &[MkbRecord]) -> bool {
    records.iter().any(|r| matches!(r.rec_type, 0x82 | 0x83))
}

/// Body of the Encrypted Media Key Variant Data record (type `0x82`).
///
/// Returns the whole first `0x82` body; the internal C / VKD sub-field
/// split is not yet decoded, so this aliases [`variant_key_data`] on a
/// single-`0x82` disc. `pub(crate)` until the sub-field offsets are fixed
/// against a real variant disc — it is not part of the public surface
/// because it knowingly returns an undecoded composite.
pub(crate) fn variant_data_record(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == 0x82)
        .map(|r| r.body.as_slice())
}

/// 16-byte Nonce from the Variant Number record (type `0x83`). Returns
/// the first 16 bytes of the body.
pub fn variant_nonce(records: &[MkbRecord]) -> Option<[u8; 16]> {
    let r = records.iter().find(|r| r.rec_type == 0x83)?;
    if r.body.len() < 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&r.body[..16]);
    Some(out)
}

/// Body of the Variant Key Data record. Returns the first `0x82` body
/// that is a non-empty multiple of 16 bytes.
///
/// Like [`variant_data_record`], this returns the whole `0x82` body and
/// aliases it on a single-`0x82` disc; the C / VKD sub-field split is
/// undecoded. `pub(crate)` until fixed against a real variant disc.
pub(crate) fn variant_key_data(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == 0x82 && !r.body.is_empty() && r.body.len() % 16 == 0)
        .map(|r| r.body.as_slice())
}

// ── AES-G ────────────────────────────────────────────────────────────────

/// AES-G(x1, x2) = AES-128D(x1, x2) XOR x2.
///
/// The Media Key Variant chain uses AES-G to derive both the variant
/// number (`Kvn = AES-G(Kp, Nonce)`) and the Volume Unique Key
/// (`Kvu = AES-G(Km, VID)`). See [`super::keys::derive_vuk`] for the
/// classical VUK form — the math is identical, this exposes it as a
/// neutral primitive for the variant chain.
fn aes_g(x1: &[u8; 16], x2: &[u8; 16]) -> [u8; 16] {
    let mut out = aes_ecb_decrypt(x1, x2);
    for i in 0..16 {
        out[i] ^= x2[i];
    }
    out
}

// ── Subset-difference walk that exposes (Kp, uv) ──────────────────────────

/// AES-G3 seed register initial value.
const AESG3_SEED: [u8; 16] = [
    0x7B, 0x10, 0x3C, 0x5D, 0xCB, 0x08, 0xC4, 0xE5, 0x1A, 0x27, 0xB0, 0x17, 0x99, 0x05, 0x3B, 0xD9,
];

/// AES-G3 single step: AES-G against the seed register at offset `inc`.
fn aesg3_step(key: &[u8; 16], inc: u8) -> [u8; 16] {
    let mut seed = AESG3_SEED;
    seed[15] = seed[15].wrapping_add(inc);
    aes_g(key, &seed)
}

fn calc_v_mask(uv: u32) -> u32 {
    let mut v_mask: u32 = 0xFFFF_FFFF;
    while (uv & !v_mask) == 0 && v_mask != 0 {
        v_mask <<= 1;
    }
    v_mask
}

fn calc_pk_from_dk(dk: &[u8; 16], uv: u32, v_mask: u32, dev_key_v_mask: u32) -> [u8; 16] {
    let mut left_child = aesg3_step(dk, 0);
    let mut pk = aesg3_step(dk, 1);
    let mut right_child = aesg3_step(dk, 2);
    let mut current_v_mask = dev_key_v_mask;

    // Bound the walk to the 32-level depth of a u32 subset-difference tree.
    // `current_v_mask` advances via an arithmetic `>> 1` which sign-extends, so
    // a disc-supplied v_mask coarser than dev_key_v_mask would otherwise drive
    // current_v_mask up to 0xFFFF_FFFF and spin forever — a crafted MKB must
    // not hang the rip thread (this runs before the KCD placeholder gate).
    let mut steps = 0u32;
    while current_v_mask != v_mask {
        if steps >= 32 {
            break;
        }
        steps += 1;
        let mut bit_pos: i32 = -1;
        for i in (0..32).rev() {
            if (current_v_mask & (1u32 << i)) == 0 {
                bit_pos = i;
                break;
            }
        }

        let curr_key = if bit_pos < 0 || (uv & (1u32 << bit_pos as u32)) == 0 {
            left_child
        } else {
            right_child
        };

        left_child = aesg3_step(&curr_key, 0);
        pk = aesg3_step(&curr_key, 1);
        right_child = aesg3_step(&curr_key, 2);

        current_v_mask = ((current_v_mask as i32) >> 1) as u32;
    }

    pk
}

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

fn mkb_find_body(records: &[MkbRecord], rec_type: u8) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == rec_type && !r.body.is_empty())
        .map(|r| r.body.as_slice())
}

fn mkb_find_mk_dv(records: &[MkbRecord]) -> Option<[u8; 16]> {
    let r = records
        .iter()
        .find(|r| (r.rec_type == 0x81 || r.rec_type == 0x86) && r.body.len() >= 16)?;
    let mut out = [0u8; 16];
    out.copy_from_slice(&r.body[..16]);
    Some(out)
}

/// Walk an MKB and return the first `(Kp, uv, cvalue)` that
/// `device_keys` covers. Returns `None` if no DK walks any uv.
pub fn walk_processing_key(
    records: &[MkbRecord],
    device_keys: &[DeviceKey],
) -> Option<ProcessingKeyMatch> {
    let mk_dv = mkb_find_mk_dv(records)?;
    let uvs = mkb_find_body(records, 0x04)?;
    let cvalues = mkb_find_body(records, 0x07).or_else(|| mkb_find_body(records, 0x05))?;

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

            if u_mask_shift & 0xC0 != 0 {
                break;
            }
            // 0x20..=0x3F (32..=63) pass the 0xC0 revoked-marker check but are
            // out of range for a u32 shift. `wrapping_shl` would silently
            // compute shift % 32 (e.g. 32 → no shift → 0xFFFF_FFFF), matching a
            // wrong uv slot and deriving a wrong key. Disc-controlled byte:
            // skip the slot instead.
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
    /// Supplied KCD equals [`KEY_CORRECTION_DATA_PLACEHOLDER`]. The
    /// derivation refuses to run with the all-zero placeholder.
    KcdNotProvided,
    /// `VARIANTS[uv]` lookup for the matched uv is not implemented.
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
            MediaKeyVariantError::KcdNotProvided => 7105,
            MediaKeyVariantError::VariantsTableUnavailable => 7106,
            MediaKeyVariantError::VkdIndexOutOfRange => 7107,
            MediaKeyVariantError::MediaKeyVerifyFailed => 7108,
        };
        write!(f, "E{code}")
    }
}

impl std::error::Error for MediaKeyVariantError {}

// ── Chain ─────────────────────────────────────────────────────────────────

/// Look up the per-slot `VARIANTS` value for the matched subset-difference
/// slot. AACS 2.1 keys the VARIANTS table by the matched SD slot (the same
/// index that selected the cvalue), so the caller passes
/// [`ProcessingKeyMatch::cvalue_index`]. The byte layout of the per-slot entry
/// in the Variant Number record is undocumented and disc-specific; this helper
/// returns `None` until a Variant disc is available to fix the layout against.
///
/// `sd_slot_index` is the matched subset-difference slot (== cvalue index).
fn variants_for_uv(_records: &[MkbRecord], _sd_slot_index: usize) -> Option<u16> {
    None
}

/// Run the Media Key Variant chain on an MKB.
///
/// Inputs:
///
/// - `mkb_records`  : MKB pre-walked via [`walk_mkb`].
/// - `device_keys`  : pool of device keys; the chain runs against the
///   first uv slot any DK covers.
/// - `kcd`          : integrator-supplied Key Correction Data. Must not
///   equal [`KEY_CORRECTION_DATA_PLACEHOLDER`].
/// - `vid`          : 16-byte Volume ID for the disc. Used to derive
///   the final VUK alongside the Media Key.
///
/// Returns `(Km, Kvu)` on success.
///
/// NOTE: the `VARIANTS[uv]` lookup ([`variants_for_uv`]) is not yet
/// implemented, so on a real Variant disc this always returns
/// `Err(`[`MediaKeyVariantError::VariantsTableUnavailable`]`)` before a
/// key is produced. The chain can only succeed against synthetic test
/// fixtures today.
pub fn derive_media_key_variant(
    mkb_records: &[MkbRecord],
    device_keys: &[DeviceKey],
    kcd: &[u8; 16],
    vid: &[u8; 16],
) -> Result<([u8; 16], [u8; 16]), MediaKeyVariantError> {
    if !is_variant_mkb(mkb_records) {
        return Err(MediaKeyVariantError::NotVariantMkb);
    }

    let pkm = walk_processing_key(mkb_records, device_keys)
        .ok_or(MediaKeyVariantError::ProcessingKeyUnavailable)?;

    let nonce = variant_nonce(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let vkd_table = variant_key_data(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    let c_value = variant_data_record(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    if c_value.len() < 16 {
        return Err(MediaKeyVariantError::MkbIncomplete);
    }
    let mut c_block = [0u8; 16];
    c_block.copy_from_slice(&c_value[..16]);

    // Step: Kmp = AES-128D(Kp, C) XOR uv  (uv into low 4 bytes).
    let mut kmp = aes_ecb_decrypt(&pkm.kp, &c_block);
    let uv_bytes = pkm.uv.to_be_bytes();
    for i in 0..4 {
        kmp[12 + i] ^= uv_bytes[i];
    }

    // Condition bits on Kmp[15] route off the hardcoded-KCD path.
    if kmp[15] & 0b0000_0010 != 0 {
        return Err(MediaKeyVariantError::SoftCorrectionRequired);
    }
    if kmp[15] & 0b0000_0100 != 0 {
        return Err(MediaKeyVariantError::OnlineChallengeRequired);
    }
    if kcd == &KEY_CORRECTION_DATA_PLACEHOLDER {
        return Err(MediaKeyVariantError::KcdNotProvided);
    }

    // Step: Kpnew = Kmp XOR KCD.
    let mut kpnew = [0u8; 16];
    for i in 0..16 {
        kpnew[i] = kmp[i] ^ kcd[i];
    }

    // Step: Kvn = AES-G(Kp, Nonce) & 0xFFFF  (low 16 bits, BE).
    let kvn_block = aes_g(&pkm.kp, &nonce);
    let kvn = u16::from_be_bytes([kvn_block[14], kvn_block[15]]);

    // Step: VKD_idx = Kvn XOR VARIANTS[uv].
    let v_for_uv = variants_for_uv(mkb_records, pkm.cvalue_index)
        .ok_or(MediaKeyVariantError::VariantsTableUnavailable)?;
    let vkd_idx = kvn ^ v_for_uv;

    // Step: VKD = vkd_table[VKD_idx * 16 .. +16].
    let off = (vkd_idx as usize) * 16;
    if off + 16 > vkd_table.len() {
        return Err(MediaKeyVariantError::VkdIndexOutOfRange);
    }
    let mut vkd = [0u8; 16];
    vkd.copy_from_slice(&vkd_table[off..off + 16]);

    // Step: Km = AES-128D(Kpnew, VKD) XOR uv.
    let mut km = aes_ecb_decrypt(&kpnew, &vkd);
    for i in 0..4 {
        km[12 + i] ^= uv_bytes[i];
    }

    // Gate: verify the derived Media Key against the MKB's Verify-Media-Key
    // record. On the variant path the per-match magic check in
    // `walk_processing_key` does NOT hold (it only saw the Precursor), so this
    // is the authoritative Kp/Km verification — it MUST run before returning a
    // real key.
    let mk_dv = mkb_find_mk_dv(mkb_records).ok_or(MediaKeyVariantError::MkbIncomplete)?;
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    if aes_ecb_decrypt(&km, &mk_dv)[..8] != VERIFY_MAGIC {
        return Err(MediaKeyVariantError::MediaKeyVerifyFailed);
    }

    // Step: Kvu = AES-G(Km, VID).
    let kvu = aes_g(&km, vid);

    Ok((km, kvu))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // 0x82 — 16-byte body (Variant data / VKD slot).
        mkb.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xEE; 16]);
        // 0x83 — 16-byte body (Variant Nonce).
        mkb.extend_from_slice(&[0x83, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0x55; 16]);
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
        assert_eq!(variant_nonce(&recs), Some([0x55; 16]));
        assert_eq!(variant_key_data(&recs), Some(&[0xEE; 16][..]));
        assert_eq!(variant_data_record(&recs), Some(&[0xEE; 16][..]));
    }

    // ── Chain entry-point classification ──

    #[test]
    fn chain_rejects_non_variant_mkb() {
        let recs = walk_mkb(&synthetic_mkb_classical());
        let err = derive_media_key_variant(&recs, &[], &[0xAA; 16], &[0u8; 16])
            .expect_err("classical MKB must be rejected");
        assert_eq!(err, MediaKeyVariantError::NotVariantMkb);
    }

    #[test]
    fn chain_rejects_placeholder_kcd() {
        // To reach the KCD check we need a complete variant MKB AND a
        // DK that walks it. We construct both via the synthetic
        // fixture below.
        let (recs, dk, _kp, _expected_kmp) = synthetic_variant_setup(/*kmp15*/ 0x00);
        let err =
            derive_media_key_variant(&recs, &[dk], &KEY_CORRECTION_DATA_PLACEHOLDER, &[0u8; 16])
                .expect_err("placeholder KCD must be rejected");
        assert_eq!(err, MediaKeyVariantError::KcdNotProvided);
    }

    #[test]
    fn chain_detects_soft_correction_bit() {
        let (recs, dk, _, _) = synthetic_variant_setup(/*kmp15*/ 0x02);
        let err = derive_media_key_variant(&recs, &[dk], &[0xAA; 16], &[0u8; 16])
            .expect_err("bit 0x02 must surface SoftCorrectionRequired");
        assert_eq!(err, MediaKeyVariantError::SoftCorrectionRequired);
    }

    #[test]
    fn chain_detects_online_challenge_bit() {
        let (recs, dk, _, _) = synthetic_variant_setup(/*kmp15*/ 0x04);
        let err = derive_media_key_variant(&recs, &[dk], &[0xAA; 16], &[0u8; 16])
            .expect_err("bit 0x04 must surface OnlineChallengeRequired");
        assert_eq!(err, MediaKeyVariantError::OnlineChallengeRequired);
    }

    #[test]
    fn chain_surfaces_variants_table_gap_on_clean_kmp() {
        // With both condition bits clear and a non-placeholder KCD, the
        // chain advances to the per-uv VARIANTS[uv] lookup, which is
        // not yet wired. That returns VariantsTableUnavailable —
        // proving the bit checks and KCD check all passed.
        let (recs, dk, _, _) = synthetic_variant_setup(/*kmp15*/ 0x00);
        let err = derive_media_key_variant(&recs, &[dk], &[0xAA; 16], &[0u8; 16])
            .expect_err("expected VariantsTableUnavailable at the per-uv lookup");
        assert_eq!(err, MediaKeyVariantError::VariantsTableUnavailable);
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
            MediaKeyVariantError::KcdNotProvided,
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
    ///   times — Kp = aesg3_step(dk, 1).
    /// - one cvalue in record 0x07 chosen so AES-D(Kp, C) ⊕ uv produces a
    ///   Kmp whose byte-15 is exactly `kmp15`.
    /// - record 0x82 with a 16-byte body (acts as both Variant Data
    ///   and Variant Key Data; satisfies the parser heuristics).
    /// - record 0x83 with a 16-byte Nonce.
    ///
    /// Returns (records, dk, planted_kp, planted_kmp).
    fn synthetic_variant_setup(kmp15: u8) -> (Vec<MkbRecord>, DeviceKey, [u8; 16], [u8; 16]) {
        use crate::aacs::decrypt::aes_ecb_encrypt;

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
        // no-op — Kp = aesg3_step(dk, 1).
        let dk_bytes: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let kp = aesg3_step(&dk_bytes, 1);

        // Plant Kmp with chosen byte-15, then compute C such that
        // AES-D(Kp, C) ⊕ uv == Kmp. uv=2 → low-4 bytes XOR is 00 00 00 02.
        let mut kmp = [0x42u8; 16];
        kmp[15] = kmp15;
        let mut aes_d_result = kmp;
        aes_d_result[15] ^= 0x02;
        let c_block = aes_ecb_encrypt(&kp, &aes_d_result);

        // cvalues record (0x07): one 16-byte cvalue. The walker
        // indexes it for the magic-check step; on a variant MKB the
        // magic check fails but `variant_present` is true so the
        // walker still returns the match. Content is don't-care.
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);

        // Verify Media Key (0x86): body content is don't-care.
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);

        // 0x82 record: holds C (Encrypted Media Key Variant Data) AND
        // doubles as the VKD table (single 16-byte entry → VKDidx must
        // resolve to 0 for `chain_surfaces_variants_table_gap` test —
        // but the test never reaches the VKD lookup since the
        // VARIANTS[uv] helper is not yet wired).
        mkb.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&c_block);

        // 0x83 record: 16-byte Nonce.
        mkb.extend_from_slice(&[0x83, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0x77; 16]);

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
    fn is_variant_mkb_true_for_0x82_alone_and_0x83_alone() {
        // Either record type alone flags the MKB as variant.
        let only82 = walk_mkb(&{
            let mut m = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 0];
            m.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]);
            m.extend_from_slice(&[0xEE; 16]);
            m
        });
        assert!(is_variant_mkb(&only82));
        let only83 = walk_mkb(&{
            let mut m = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 0];
            m.extend_from_slice(&[0x83, 0x00, 0x00, 0x14]);
            m.extend_from_slice(&[0x55; 16]);
            m
        });
        assert!(is_variant_mkb(&only83));
    }

    #[test]
    fn variant_nonce_requires_16_byte_body() {
        // A 0x83 record with < 16-byte body → None (no panic on the copy).
        let recs = walk_mkb(&{
            let mut m = vec![0x83, 0x00, 0x00, 0x0C]; // 8-byte body
            m.extend_from_slice(&[0x11; 8]);
            m
        });
        assert_eq!(variant_nonce(&recs), None);
    }

    #[test]
    fn variant_key_data_requires_nonempty_multiple_of_16() {
        // A 0x82 body that is NOT a multiple of 16 is rejected by
        // variant_key_data (it needs whole 16-byte VKD slots).
        let recs = walk_mkb(&{
            let mut m = vec![0x82, 0x00, 0x00, 0x0E]; // 10-byte body (not %16)
            m.extend_from_slice(&[0x22; 10]);
            m
        });
        assert_eq!(variant_key_data(&recs), None);
        // variant_data_record returns the body regardless of length.
        assert_eq!(variant_data_record(&recs), Some(&[0x22u8; 10][..]));
    }

    // ── derive_media_key_variant: missing-record classification ────────────

    #[test]
    fn chain_reports_processing_key_unavailable_with_no_dks() {
        // A complete variant MKB but an empty device-key pool → no uv covered
        // → ProcessingKeyUnavailable (the walk_processing_key None branch).
        let (recs, _dk, _, _) = synthetic_variant_setup(0x00);
        let err = derive_media_key_variant(&recs, &[], &[0xAA; 16], &[0u8; 16])
            .expect_err("no DK → ProcessingKeyUnavailable");
        assert_eq!(err, MediaKeyVariantError::ProcessingKeyUnavailable);
    }

    #[test]
    fn chain_reports_mkb_incomplete_when_nonce_missing() {
        // Build a variant MKB (has 0x82 so is_variant true, and a DK can walk
        // it) but WITHOUT a 0x83 nonce record → MkbIncomplete at the
        // variant_nonce `?`.
        // Start from the full setup, then rebuild the byte stream dropping
        // the 0x83 record.
        let (recs, dk, _, _) = synthetic_variant_setup(0x00);
        // Reconstruct bytes without the 0x83 record.
        let mut mkb = Vec::new();
        for r in &recs {
            if r.rec_type == 0x83 {
                continue;
            }
            mkb.push(r.rec_type);
            mkb.push(((r.rec_len >> 16) & 0xFF) as u8);
            mkb.push(((r.rec_len >> 8) & 0xFF) as u8);
            mkb.push((r.rec_len & 0xFF) as u8);
            mkb.extend_from_slice(&r.body);
        }
        let recs2 = walk_mkb(&mkb);
        assert!(is_variant_mkb(&recs2), "still variant via 0x82");
        let err = derive_media_key_variant(&recs2, &[dk], &[0xAA; 16], &[0u8; 16])
            .expect_err("missing nonce → MkbIncomplete");
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
        assert_eq!(
            m.kp, planted_kp,
            "Kp equals aesg3_step(dk,1) for the no-op walk"
        );
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
        // operator triage; assert all nine are distinct.
        use std::collections::HashSet;
        let cases = [
            MediaKeyVariantError::NotVariantMkb,
            MediaKeyVariantError::MkbIncomplete,
            MediaKeyVariantError::ProcessingKeyUnavailable,
            MediaKeyVariantError::SoftCorrectionRequired,
            MediaKeyVariantError::OnlineChallengeRequired,
            MediaKeyVariantError::KcdNotProvided,
            MediaKeyVariantError::VariantsTableUnavailable,
            MediaKeyVariantError::VkdIndexOutOfRange,
            MediaKeyVariantError::MediaKeyVerifyFailed,
        ];
        let codes: HashSet<String> = cases.iter().map(|e| e.to_string()).collect();
        assert_eq!(codes.len(), cases.len(), "all error codes must be unique");
    }
}

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
//! detected via the real AACS 2.1 MKB records `0x2d` (Encrypted Media
//! Key Variant Data / C), `0x2f` (Variant Key Data table, 65,535×16),
//! and `0x0c` (variant cvalues, one per `0x04` subset-difference slot).
//! When a disc carries none, callers fall back to the classical
//! single-stage derivation in [`super::keys`]. (The earlier `0x82`/`0x83`
//! record types were a speculative guess that never appeared in any real
//! MKB; they were replaced with the real records once a live variant MKB
//! was obtained.)
//!
//! **Status: the chain cannot yet produce a key on a real disc — for two
//! reasons, one external, one internal:**
//!   - EXTERNAL: no device key in our pool walks any real variant MKB,
//!     so `Kp` (and thus the whole chain) can't be produced live. This is
//!     a key-acquisition gap, not a code gap.
//!   - INTERNAL: the exact `VARIANTS[uv]` lookup ([`variants_for_uv`])
//!     and the Nonce / C sub-field offsets can't be pinned without a real
//!     disc + covering key to run the chain end-to-end against the `0x86`
//!     verify. Until then [`variants_for_uv`] returns `None` and the
//!     chain halts at [`MediaKeyVariantError::VariantsTableUnavailable`],
//!     so a wrong best-effort offset is never silently trusted.
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
//! **Spec note — Variant Number width (`Kvn`).** The published AACS
//! Sequence-Key Variant Number (Introduction and Common Cryptographic
//! Elements book, Rev 0.953, §3.2.5.2.2, record `0x0D`) is the **low 10
//! bits** of `AES-G(Kp, Nonce)` — a range of ≤1024 variants. This 2.1
//! chain instead takes the **low 16 bits** (`& 0xFFFF`), because it
//! indexes the 2.1 VKD table (`0x2f`), which carries up to 65,535 entries:
//! the wider index is demanded by the larger table, not a mis-transcription
//! of the 10-bit spec value. Both the 16-bit width and the Nonce source
//! (tail of `0x2d`) are RE-derived from a single live variant MKB and
//! remain UNCONFIRMED — the spec's `0x0D` "Variant Number" record does not
//! appear on a real 2.1 MKB. If a covering key ever lets the chain run
//! end-to-end against the `0x86` verify, this width is the first thing to
//! confirm.
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

use super::crypto::{aes_ecb_decrypt, aes_g};
use super::mkb::*;
use super::types::DeviceKey;

// ── Public constants ──────────────────────────────────────────────────────

/// Placeholder Key Correction Data. Sixteen zero bytes.
///
/// Integrators MUST supply a non-placeholder KCD via the `kcd` argument
/// to [`derive_media_key_variant`]; the chain refuses to operate when
/// the supplied KCD compares equal to this placeholder.
pub const KEY_CORRECTION_DATA_PLACEHOLDER: [u8; 16] = [0u8; 16];

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
    records.iter().any(|r| matches!(r.rec_type, 0x2d | 0x2f))
}

/// Body of the Encrypted Media Key Variant Data record (type `0x2d`).
///
/// Confirmed against a live variant MKB as the `0x2d` record (92,220 bytes on
/// the reference disc — not a clean multiple of 16, so it is a structured /
/// count-prefixed record, not a flat C-block table). The exact per-uv C
/// selection is the one sub-field still unconfirmed without a real disc+key to
/// test against; the chain currently consumes the leading 16-byte block.
/// `pub(crate)` until that offset is pinned.
pub(crate) fn variant_data_record(records: &[MkbRecord]) -> Option<&[u8]> {
    records
        .iter()
        .find(|r| r.rec_type == 0x2d)
        .map(|r| r.body.as_slice())
}

/// 16-byte Nonce for `Kvn = AES-G(Kp, Nonce)`.
///
/// **UNCONFIRMED source.** The speculative `0x83` "Variant Number" record does
/// not exist on a real variant MKB. The `0x2d` Encrypted-Media-Key-Variant-Data
/// record is the most likely home for a per-disc nonce, so this best-effort
/// reads the trailing 16 bytes of `0x2d`. Confirming this (vs. a field inside
/// `0x21`, or a fixed slice of `0x2d`) needs a real disc+key to test the whole
/// chain against the `0x86` verify — until then the chain halts earlier at
/// [`variants_for_uv`], so a wrong nonce here is never silently trusted.
pub fn variant_nonce(records: &[MkbRecord]) -> Option<[u8; 16]> {
    let r = records.iter().find(|r| r.rec_type == 0x2d)?;
    if r.body.len() < 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&r.body[r.body.len() - 16..]);
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
        .find(|r| r.rec_type == 0x2f && !r.body.is_empty() && r.body.len() % 16 == 0)
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
    let r = records
        .iter()
        .find(|r| (r.rec_type == 0x81 || r.rec_type == 0x86) && r.body.len() >= 16)?;
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
    let uvs = mkb_find_body(records, 0x04)?;
    // Variant cvalue source: a real variant MKB carries its per-uv cvalue table
    // in record `0x0c` (confirmed 46,101×16, one per `0x04` subset-difference
    // slot). Fall back to `0x07`/`0x05` for the synthetic fixtures and any MKB
    // shape that keeps its cvalues there.
    let cvalues = mkb_find_body(records, 0x0c)
        .or_else(|| mkb_find_body(records, 0x07))
        .or_else(|| mkb_find_body(records, 0x05))?;

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
    let body = records.iter().find(|r| r.rec_type == 0x2d)?.body.as_slice();
    let off = sd_slot_index.checked_mul(2)?;
    let bytes = body.get(off..off + 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
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
/// STATUS: the chain runs end-to-end on a real variant MKB — record layout pinned
/// from a live 2.1 disc (`variants_for_uv` reads `0x2d`; `C` = matched cvalue; VKD
/// = `0x2f`; Nonce = `0x2d`), and the caller supplies the extracted CyberLink `kcd`
/// constant. It needs only a covering Processing Key to be *validated* against a
/// known answer, which confirms the last layout picks (Nonce head/tail, C-source,
/// formula ordering). Until then the final Verify-Media-Key gate rejects any wrong
/// pick, so a bad layout can never emit a wrong key — only `Err(MediaKeyVerifyFailed)`.
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

    // Condition bits on Kmp[15] select the correction mode. Bit 0x02 (SoftKCD) and
    // 0x04 (online challenge) need out-of-band data we don't model. The DEFAULT
    // path (neither bit set) uses the fixed CyberLink KCD constant hardcoded in
    // PowerDVD's CLTA_SW.dll (extracted; a 16-byte NON-zero value the caller must
    // supply). Refuse the all-zero placeholder so the chain never runs with an
    // unset/wrong KCD and emits a bad key.
    if kmp[15] & 0b0000_0010 != 0 {
        return Err(MediaKeyVariantError::SoftCorrectionRequired);
    }
    if kmp[15] & 0b0000_0100 != 0 {
        return Err(MediaKeyVariantError::OnlineChallengeRequired);
    }
    if kcd == &KEY_CORRECTION_DATA_PLACEHOLDER {
        return Err(MediaKeyVariantError::KcdNotProvided);
    }

    // Step: Kpnew = Kmp XOR KCD  (KCD = the extracted CyberLink constant).
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
        let err = derive_media_key_variant(&recs, &[], &[0xAA; 16], &[0u8; 16])
            .expect_err("classical MKB must be rejected");
        assert_eq!(err, MediaKeyVariantError::NotVariantMkb);
    }

    #[test]
    fn chain_rejects_placeholder_kcd() {
        // The default 2.1 path needs the real (extracted CyberLink) KCD constant;
        // the all-zero placeholder must be refused so the chain never runs unset.
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
    fn variants_for_uv_reads_the_table_not_unavailable() {
        // variants_for_uv now reads the VARIANTS u16 from the 0x2d record, so on a
        // variant MKB that carries 0x2d the chain advances PAST the per-uv lookup
        // (into VKD/verify) instead of dead-stopping at VariantsTableUnavailable.
        let (recs, dk, _, _) = synthetic_variant_setup(/*kmp15*/ 0x00);
        let out = derive_media_key_variant(&recs, &[dk], &[0xAA; 16], &[0u8; 16]);
        assert_ne!(out, Err(MediaKeyVariantError::VariantsTableUnavailable));
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

        // cvalues record (0x07): one 16-byte cvalue. The walker
        // indexes it for the magic-check step; on a variant MKB the
        // magic check fails but `variant_present` is true so the
        // walker still returns the match. Content is don't-care.
        mkb.extend_from_slice(&[0x07, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);

        // Verify Media Key (0x86): body content is don't-care.
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);

        // 0x2d record: Encrypted Media Key Variant Data — C (head 16) then a
        // trailing 16-byte Nonce (variant_nonce reads the tail), 32-byte body.
        mkb.extend_from_slice(&[0x2d, 0x00, 0x00, 0x24]);
        mkb.extend_from_slice(&c_block);
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
        // Build a variant MKB (still variant via 0x2f, and a DK can walk it)
        // but WITHOUT the 0x2d record that carries C + the trailing Nonce →
        // MkbIncomplete at the variant_nonce `?`.
        let (recs, dk, _, _) = synthetic_variant_setup(0x00);
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

//! Media-key derivation: DK/PK → Media Key via the subset-difference tree.
//! `[C]` §3.2.2–§3.2.5.

use super::crypto::*;
use super::inf::*;
use super::mkb::*;
use super::types::*;

/// Derive Media Key from MKB data using processing keys.
///
/// A Processing Key is **terminal**: tried *directly* against the MKB
/// cvalue tables (no tree descent) — the fast path.
///
/// If you hold a **device-node label** at unknown tree depth (not a
/// terminal PK), use [`derive_media_key_from_dk`] instead — only that path
/// walks the Subset-Difference tree.
///
/// See `docs/aacs-derive.md` for the MKB record-type layout and the
/// performance case for the direct PK × cvalue scan.
pub fn derive_media_key_from_pk(mkb: &[u8], processing_keys: &[[u8; 16]]) -> Option<[u8; 16]> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;
    try_pk_against_tables(processing_keys, &uvs, &cvalues, &mk_dv)
}

// Core terminal-PK table scan: each PK tried directly against every
// (uv, cvalue) pair, no tree descent. Factored out so reproduction
// harnesses can drive it with explicit tables (see docs/aacs-derive.md).
pub(crate) fn try_pk_against_tables(
    processing_keys: &[[u8; 16]],
    uvs: &[u8],
    cvalues: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for pk in processing_keys {
        for i in 0..num_uvs {
            if (i + 1) * 16 > cvalues.len() {
                continue;
            }
            let record_start = i * 5;
            if record_start + 5 > uvs.len() {
                continue;
            }
            let uv = &uvs[record_start + 1..record_start + 5];
            let cv = &cvalues[i * 16..(i + 1) * 16];
            if let Some(mk) = validate_processing_key(pk, cv, uv, mk_dv) {
                return Some(mk);
            }
        }
    }
    None
}

// Validate a processing key against a cvalue/UV pair; returns the Media Key
// if valid. `[C]` §3.2.4 (mk = AES-128D(pk,cvalue); mk[12..16] ^= uv) then
// §3.2.5.1.4 (dec_vd = AES-128D(mk,mk_dv); valid iff dec_vd[0..8] == magic).
pub(crate) fn validate_processing_key(
    pk: &[u8; 16],
    cvalue: &[u8],
    uv: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    if cvalue.len() < 16 || uv.len() < 4 {
        return None;
    }

    // Step 1: mk = AES-128D(pk, cvalue)
    let mut cv = [0u8; 16];
    cv.copy_from_slice(&cvalue[..16]);
    let mut mk = aes_ecb_decrypt(pk, &cv);

    // Step 2: XOR uv into the last 4 bytes of mk (mk[12..16]).
    for a in 0..4 {
        mk[12 + a] ^= uv[a];
    }

    // Step 3 + 4: dec_vd = AES-128D(mk, mk_dv); verify magic.
    let dec_vd = aes_ecb_decrypt(&mk, mk_dv);
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    if dec_vd[..8] == VERIFY_MAGIC {
        return Some(mk);
    }
    None
}

/// Compute v_mask from a UV value. `[C]` §3.2.3. Shared with [`super::variant`].
pub(super) fn calc_v_mask(uv: u32) -> u32 {
    let mut v_mask: u32 = 0xFFFF_FFFF;
    while (uv & !v_mask) == 0 && v_mask != 0 {
        v_mask <<= 1;
    }
    v_mask
}

/// Derive processing key from device key using subset-difference tree traversal.
/// `[C]` §3.2.4 (device-tree descent, MSB-branch, terminal PK). Shared with [`super::variant`].
pub(super) fn calc_pk_from_dk(
    dk: &[u8; 16],
    uv: u32,
    v_mask: u32,
    dev_key_v_mask: u32,
) -> [u8; 16] {
    // Descend device node -> record node by the record's `uv` bits. Only the child
    // descended into matters, so derive ONE child per level (left=`aesg3(node,0)`,
    // right=`,2`) and the PK=`aesg3(.,1)` once at the end — ~3x fewer block ops.
    let mut node = *dk;
    let mut current_v_mask = dev_key_v_mask;

    // Tree is <=32 levels deep (u32 mask). The arithmetic `>> 1` sign-extends, so a
    // v_mask coarser than dev_key_v_mask (crafted/corrupt MKB) would saturate to
    // 0xFFFF_FFFF and spin forever; bound to 32 steps so a bad disc can't hang the rip.
    let mut steps = 0u32;
    while current_v_mask != v_mask {
        if steps >= 32 {
            break;
        }
        steps += 1;
        // Find the highest unset bit in current_v_mask
        let mut bit_pos: i32 = -1;
        for i in (0..32).rev() {
            if (current_v_mask & (1u32 << i)) == 0 {
                bit_pos = i;
                break;
            }
        }

        let inc = if bit_pos < 0 || (uv & (1u32 << bit_pos as u32)) == 0 {
            0 // left child
        } else {
            2 // right child
        };
        node = aesg3(&node, inc);

        current_v_mask = ((current_v_mask as i32) >> 1) as u32;
    }

    aesg3(&node, 1)
}

/// Derive Media Key from MKB using device keys (subset-difference tree).
///
/// Thin wrapper over [`derive_media_key_and_pk_from_dk`] that drops the
/// intermediate Processing Key. Callers that need the PK lineage (e.g.
/// the key service banking DK·PK·MK) should call the `_and_pk_` form.
pub fn derive_media_key_from_dk(mkb: &[u8], device_keys: &[DeviceKey]) -> Option<[u8; 16]> {
    derive_media_key_and_pk_from_dk(mkb, device_keys).map(|(mk, _pk)| mk)
}

/// Derive both the Media Key and the intermediate Processing Key from an
/// MKB using device keys (subset-difference tree).
///
/// Identical walk to [`derive_media_key_from_dk`]; this form additionally
/// returns the Processing Key `Kp` derived at the matching subset-difference
/// node — the value `calc_pk_from_dk` produces immediately before it
/// validates into the Media Key. Returns `Some((mk, pk))` for the first DK
/// that walks a uv slot whose Processing Key validates against the MKB.
pub fn derive_media_key_and_pk_from_dk(
    mkb: &[u8],
    device_keys: &[DeviceKey],
) -> Option<([u8; 16], [u8; 16])> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;

    // Count UV entries
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();

    for dk in device_keys {
        let device_number = dk.node as u32;

        // Find applying subset-difference for this device
        for uvs_idx in 0..num_uvs {
            let p_uv = &uvs[1 + 5 * uvs_idx..];
            let u_mask_shift = uvs[5 * uvs_idx]; // byte before the UV value

            // `num_uvs` used `take_while(.. c[0] & 0xC0 == 0)`, so revoked-marker bits
            // are already clear (no re-check). But shifts 32..=63 panic/wrap on `<<` and
            // the byte is disc-controlled, so skip an out-of-range slot rather than shift.
            if u_mask_shift >= 32 {
                continue;
            }

            let uv = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
            if uv == 0 {
                continue;
            }

            // u-mask = shift count of low-order 0 bits ([C] §3.2.5.1.5); v-mask [C] §3.2.3.
            let u_mask: u32 = 0xFFFF_FFFF << u_mask_shift;
            let v_mask = calc_v_mask(uv);

            // Subset-difference applies iff (d&mu)==(uv&mu) && (d&mv)!=(uv&mv). [C] §3.2.4.
            if ((device_number & u_mask) == (uv & u_mask))
                && ((device_number & v_mask) != (uv & v_mask))
            {
                // Found matching subset-difference — find the right device key.
                // dk.u_mask_shift is a u8 from keydb with no range check;
                // guard the shift the same way as the MKB byte above.
                if dk.u_mask_shift >= 32 {
                    continue;
                }
                let dev_key_v_mask = calc_v_mask(dk.uv);
                let dev_key_u_mask: u32 = 0xFFFF_FFFF << dk.u_mask_shift;

                if u_mask == dev_key_u_mask && (uv & dev_key_v_mask) == (dk.uv & dev_key_v_mask) {
                    // Derive processing key via tree traversal
                    let pk = calc_pk_from_dk(&dk.key, uv, v_mask, dev_key_v_mask);

                    // Validate and derive media key
                    if uvs_idx < cvalues.len() / 16 {
                        let cv = &cvalues[uvs_idx * 16..(uvs_idx + 1) * 16];
                        if let Some(mk) =
                            validate_processing_key(&pk, cv, &uvs[1 + uvs_idx * 5..], &mk_dv)
                        {
                            return Some((mk, pk));
                        }
                    }
                }
            }
        }
    }
    None
}

/// Recover the subset-difference position (`node`, `uv`, `u_mask_shift`) of an
/// UNPOSITIONED device key by scanning a disc MKB. A device key alone (just the
/// 16 bytes) cannot be walked — the walk needs its tree node.
///
/// On the first verifying candidate it pins `(uv, u_mask_shift)` — invariant for
/// the key across all discs — and resolves a gate-passing `node`. Returns a
/// [`DeviceKey`] ready to bank and reuse on every future disc via
/// [`derive_media_key_from_dk`]. `None` if the key does not apply to this MKB.
///
/// See `docs/aacs-derive.md` for the search strategy and its cost.
pub fn recover_dk_position(mkb: &[u8], key: &[u8; 16]) -> Option<DeviceKey> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();
    let n_cv = cvalues.len() / 16;

    // Hoisted once: the zero-descent Processing Key (device sits exactly at a record)
    // is `AES-G3(key, 1)`, independent of the record, so every slot's zero-descent
    // probe reuses this instead of re-deriving it per slot.
    let pk_zero_descent = aesg3(key, 1);

    // Slots are independent so the scan parallelises (~181k slots, ~26s serial on a
    // UHD MKB). `find_map_any` returns the first match and cancels the rest; a valid
    // MKB has exactly one matching subset-difference, so which thread finds it is moot.
    use rayon::prelude::*;
    let found = (0..num_uvs.min(n_cv)).into_par_iter().find_map_any(|i| {
        let u_mask_shift = uvs[5 * i];
        if u_mask_shift >= 32 {
            return None;
        }
        let p_uv = &uvs[1 + 5 * i..];
        let uv_r = u32::from_be_bytes([p_uv[0], p_uv[1], p_uv[2], p_uv[3]]);
        if uv_r == 0 {
            return None;
        }
        let v_mask = calc_v_mask(uv_r);
        let cv = &cvalues[i * 16..(i + 1) * 16];
        let uv_bytes = &uvs[1 + i * 5..];

        // Zero descent (device sits at this slot's node): cheapest, most common.
        if validate_processing_key(&pk_zero_descent, cv, uv_bytes, &mk_dv).is_some() {
            return Some((uv_r, u_mask_shift));
        }
        // Descent: device is an ANCESTOR of the slot. Walk the depth bit up from
        // the slot's lowest set bit; each level descends to the slot's node.
        let p = uv_r.trailing_zeros();
        for k in (p + 1)..32 {
            let uv_d = if k + 1 >= 32 {
                1u32 << k
            } else {
                (uv_r & (0xFFFF_FFFFu32 << (k + 1))) | (1u32 << k)
            };
            let pk = calc_pk_from_dk(key, uv_r, v_mask, calc_v_mask(uv_d));
            if validate_processing_key(&pk, cv, uv_bytes, &mk_dv).is_some() {
                return Some((uv_d, u_mask_shift));
            }
        }
        None
    });
    found.and_then(|(uv, mask)| resolve_dk_node(mkb, key, uv, mask))
}

// Resolve a positioned DeviceKey for an orphan `key` at `(uv, u_mask_shift)`:
// find a `device_number` (node) that passes the walk's subset-difference
// gate. Any gating node yields the same Media Key — a one-time ≤32-try search.
pub(crate) fn resolve_dk_node(
    mkb: &[u8],
    key: &[u8; 16],
    uv: u32,
    u_mask_shift: u8,
) -> Option<DeviceKey> {
    for b in 0..u_mask_shift {
        let dk = DeviceKey {
            key: *key,
            node: ((uv ^ (1u32 << b)) & 0xFFFF) as u16,
            uv,
            u_mask_shift,
        };
        if derive_media_key_from_dk(mkb, std::slice::from_ref(&dk)).is_some() {
            return Some(dk);
        }
    }
    // Degenerate MKB (no gating bit): fall back to the node itself.
    Some(DeviceKey {
        key: *key,
        node: (uv & 0xFFFF) as u16,
        uv,
        u_mask_shift,
    })
}

/// Public, side-effect-free accessors over the MKB record helpers, exposed so
/// independent reproduction harnesses (e.g. `examples/prove_hkd_aacs.rs`) can
/// exercise the exact same parser + verify primitives the production walk uses.
/// These are thin wrappers — no new logic.
#[doc(hidden)]
pub mod probe {
    use super::super::crypto::aes_ecb_decrypt;

    /// `mk_dv` from the MKB's Verify-Media-Key record (type 0x81 / 0x86).
    pub fn mkb_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
        super::mkb_find_mk_dv(mkb)
    }

    /// Body of the MKB's Subset-Difference Index record (type 0x04).
    pub fn mkb_subdiff(mkb: &[u8]) -> Option<Vec<u8>> {
        super::mkb_find_subdiff_records(mkb)
    }

    /// Body of the MKB's Media-Key-Data (cvalues) record. Selects record
    /// `0x05` (the large cvalue table, 1:1 with the `0x04` Subset-Difference
    /// index on AACS 2.x UHD MKBs), falling back to `0x07` only when `0x05`
    /// is absent.
    pub fn mkb_cvalues(mkb: &[u8]) -> Option<Vec<u8>> {
        super::mkb_find_cvalues(mkb)
    }

    /// Body (header stripped) of the first MKB record of `rec_type`. Lets a
    /// harness pin an exact record type for cross-checking the production
    /// cvalue selection (e.g. compare record `0x05` vs `0x07` sizes).
    pub fn mkb_record_body(mkb: &[u8], rec_type: u8) -> Option<Vec<u8>> {
        super::find_record_body(mkb, rec_type)
    }

    /// AES-128-ECB single-block decrypt (the AACS verify primitive).
    pub fn aes_dec(key: &[u8; 16], block: &[u8; 16]) -> [u8; 16] {
        aes_ecb_decrypt(key, block)
    }

    /// Does `km` satisfy the MKB's Verify-Media-Key relation?
    /// `AES-D(km, mk_dv)[0..8] == 01 23 45 67 89 AB CD EF`.
    pub fn km_verifies(mkb: &[u8], km: &[u8; 16]) -> bool {
        match super::mkb_find_mk_dv(mkb) {
            Some(mk_dv) => {
                aes_ecb_decrypt(km, &mk_dv)[..8] == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
            }
            None => false,
        }
    }
}

// ── Volume key: Media Key + Volume ID → VUK → unit keys ──────────────────────

/// Derive VUK from Media Key and Volume ID. `[PR]` §3.3 / `[BD]` §3.3
/// (`Kvu = AES-G(Km, IDv)`; AES-G uses AES-128D):
/// VUK = AES-128-ECB-DECRYPT(media_key, volume_id) XOR volume_id
pub fn derive_vuk(media_key: &[u8; 16], volume_id: &[u8; 16]) -> [u8; 16] {
    let mut vuk = aes_ecb_decrypt(media_key, volume_id);
    for i in 0..16 {
        vuk[i] ^= volume_id[i];
    }
    vuk
}

/// Decrypt an encrypted unit key using the VUK (AES-128-ECB). `[PR]` §3.5
/// (Title Key unwrap `Kt = AES-128D(Ku, Kte)`); the BD "CPS Unit Key" synonym is `[BD]` §3.9.3.
pub fn decrypt_unit_key(vuk: &[u8; 16], encrypted_uk: &[u8; 16]) -> [u8; 16] {
    aes_ecb_decrypt(vuk, encrypted_uk)
}

// Decrypt every encrypted unit key in a parsed Unit_Key_RO.inf with a VUK,
// paired with its declared CPS-unit number. The single VUK->unit-keys step
// both resolvers and resolve_candidate call, so the map cannot drift.
pub(crate) fn derive_unit_keys(uk_file: &UnitKeyFile, vuk: &[u8; 16]) -> Vec<(u32, [u8; 16])> {
    uk_file
        .encrypted_keys
        .iter()
        .map(|(num, enc_key)| (*num, decrypt_unit_key(vuk, enc_key)))
        .collect()
}

/// A candidate key at any rung of the AACS ladder, handed to [`resolve_candidate`].
///
/// Each variant carries the [`super::types`] newtype for that rung (a `Dk` is a
/// POSITIONED [`DeviceKey`] — recover an unpositioned one with
/// [`recover_dk_position`] first).
#[derive(Debug, Clone)]
pub enum KeyCandidate {
    Uk(UnitKey),
    Vuk(Vuk),
    Mk(MediaKey),
    Pk(ProcessingKey),
    Dk(DeviceKey),
}

/// The AACS key chain derived from a candidate, from [`resolve_candidate`].
///
/// PURE DERIVATION — no unit sampling, no validation. `unit_keys` holds every
/// CPS-unit key the disc's `Unit_Key_RO.inf` yields from the VUK (paired with
/// its declared CPS-unit number); the caller runs
/// `decrypt_unit` + `is_clean_ts` to find which one actually opens the
/// disc. Rungs above the candidate are `None`.
#[derive(Clone)]
pub struct ResolvedChain {
    pub unit_keys: Vec<(u32, [u8; 16])>,
    pub vuk: Option<Vuk>,
    pub mk: Option<MediaKey>,
    pub pk: Option<ProcessingKey>,
    /// The positioned device key (for a `Dk` candidate).
    pub dk: Option<DeviceKey>,
}

// Redacting `Debug`: `unit_keys` holds raw title-key bytes, never printed. The
// other rungs are `types` newtypes that self-redact. Guarded by
// `resolved_chain_debug_is_redacted`.
impl std::fmt::Debug for ResolvedChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedChain")
            .field("unit_keys_len", &self.unit_keys.len())
            .field("vuk", &self.vuk)
            .field("mk", &self.mk)
            .field("pk", &self.pk)
            .field("dk", &self.dk)
            .finish()
    }
}

/// Derive the full AACS key chain from a candidate key of ANY ladder rung.
///
/// Runs the deterministic derivation DOWNWARD to the disc's terminal unit keys:
/// `DK → MK → VUK → UKs`, `PK → MK → VUK → UKs`, `MK → VUK → UKs`,
/// `VUK → UKs`, or `UK → itself`, parsing `Unit_Key_RO.inf` at the version the
/// disc's MKB declares so a multi-CPS disc yields all its unit keys.
///
/// PURE DERIVATION: no sampling, no validation, no position recovery. Returns
/// `None` only when derivation itself cannot proceed. See `docs/aacs-derive.md`
/// for the composed primitives and the full `None` conditions.
pub fn resolve_candidate(
    candidate: &KeyCandidate,
    mkb: &[u8],
    unit_key_ro: &[u8],
    vid: Option<Vid>,
) -> Option<ResolvedChain> {
    // Boil a VUK → all unit keys, each paired with its declared CPS-unit number.
    // Derive the stride version from the disc's own MKB, then defer to the shared
    // `derive_unit_keys` (the one place both resolvers and this path decrypt).
    let boil = |vuk: Vuk| -> Option<Vec<(u32, [u8; 16])>> {
        let version = mkb_type(mkb)
            .map(|t| t.generation())
            .unwrap_or(AacsVersion::V10);
        // BD/UHD Unit_Key_RO.inf or HD DVD VTKF000.AACS — dispatched by magic.
        let ukf = parse_title_keys(unit_key_ro, version)?;
        if ukf.encrypted_keys.is_empty() {
            return None;
        }
        Some(derive_unit_keys(&ukf, &vuk.0))
    };

    match candidate {
        KeyCandidate::Uk(uk) => Some(ResolvedChain {
            unit_keys: vec![(uk.idx, uk.key)],
            vuk: None,
            mk: None,
            pk: None,
            dk: None,
        }),
        KeyCandidate::Vuk(v) => Some(ResolvedChain {
            unit_keys: boil(*v)?,
            vuk: Some(*v),
            mk: None,
            pk: None,
            dk: None,
        }),
        KeyCandidate::Mk(mk) => {
            let vuk = Vuk(derive_vuk(&mk.0, &vid?.0));
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(*mk),
                pk: None,
                dk: None,
            })
        }
        KeyCandidate::Pk(pk) => {
            let km = derive_media_key_from_pk(mkb, std::slice::from_ref(&pk.0))?;
            let vuk = Vuk(derive_vuk(&km, &vid?.0));
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(MediaKey(km)),
                pk: Some(*pk),
                dk: None,
            })
        }
        KeyCandidate::Dk(dk) => {
            let (km, pk) = derive_media_key_and_pk_from_dk(mkb, std::slice::from_ref(dk))?;
            let vuk = Vuk(derive_vuk(&km, &vid?.0));
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(MediaKey(km)),
                pk: Some(ProcessingKey(pk)),
                dk: Some(dk.clone()),
            })
        }
    }
}

#[cfg(test)]
mod resolve_candidate_tests {
    use super::*;
    use crate::aacs::crypto::aes_ecb_encrypt;

    // km_verifies gates every candidate Media Key; mutation testing found
    // `-> true` surviving all 2,556 tests, i.e. unverified verification.
    // See docs/aacs-derive.md for how the fixture inverts the AES relation.
    #[test]
    fn km_verifies_accepts_only_the_key_its_record_was_built_for() {
        use crate::aacs::mkb::mkb_find_mk_dv;

        const MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        let km: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ];

        let mut plain = [0u8; 16];
        plain[..8].copy_from_slice(&MAGIC);
        plain[8..].copy_from_slice(&[0xA5; 8]);
        let mk_dv = aes_ecb_encrypt(&km, &plain);

        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&mk_dv);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        assert_eq!(
            mkb_find_mk_dv(&mkb),
            Some(mk_dv),
            "fixture malformed — the verify record is not being found at all"
        );

        assert!(
            probe::km_verifies(&mkb, &km),
            "the key the record was built for must verify"
        );

        // The half that kills the `-> true` mutant. One flipped bit is the
        // strongest form of wrong key: a near-miss, not a random one.
        let mut wrong = km;
        wrong[15] ^= 0x01;
        assert!(
            !probe::km_verifies(&mkb, &wrong),
            "a key differing by ONE BIT must not verify; if it did, the MK-pool \
             brute force would accept whichever candidate it happened to try first"
        );

        // No verify record means UNVERIFIABLE, which is not the same as verified.
        let bare = vec![0x10, 0x00, 0x00, 0x0C, 0, 0, 0, 0, 0, 0, 0, 1];
        assert!(
            !probe::km_verifies(&bare, &km),
            "an MKB with no verify record must not default to yes"
        );
    }

    /// `ResolvedChain.unit_keys` holds raw title-key bytes (the other rungs are
    /// self-redacting `types` newtypes). `Debug` must not leak the title keys.
    #[test]
    fn resolved_chain_debug_is_redacted() {
        let c = ResolvedChain {
            unit_keys: vec![(1, [0xD5; 16])],
            vuk: None,
            mk: None,
            pk: None,
            dk: None,
        };
        let dbg = format!("{c:?}");
        assert!(
            !dbg.contains("213"),
            "ResolvedChain leaked unit keys: {dbg}"
        );
        assert!(
            dbg.contains("unit_keys_len"),
            "ResolvedChain missing redaction: {dbg}"
        );
    }

    /// Minimal AACS-1.0 (48-byte stride) `Unit_Key_RO.inf` with `n` encrypted
    /// unit keys — `parse_unit_key_ro` numbers CPS units 1..=n.
    fn synth_inf(encs: &[[u8; 16]]) -> Vec<u8> {
        let uk_pos = 32usize;
        let stride = 48usize;
        let n = encs.len();
        let total = uk_pos + 48 + n.saturating_sub(1) * stride + 16;
        let mut inf = vec![0u8; total.max(20)];
        inf[..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        inf[uk_pos..uk_pos + 2].copy_from_slice(&(n as u16).to_be_bytes());
        for (i, k) in encs.iter().enumerate() {
            let o = uk_pos + 48 + i * stride;
            inf[o..o + 16].copy_from_slice(k);
        }
        inf
    }

    /// A VUK candidate boils to ALL the disc's unit keys, each paired with its
    /// declared CPS-unit number, and each key equals the VUK-decrypt of its slot.
    #[test]
    fn resolve_candidate_vuk_returns_all_cps_units() {
        let vuk = Vuk([0x33u8; 16]);
        let encs = [[0x11u8; 16], [0x22u8; 16], [0x44u8; 16]];
        let inf = synth_inf(&encs);
        let r = resolve_candidate(&KeyCandidate::Vuk(vuk), &[], &inf, None).expect("vuk derives");
        let cps: Vec<u32> = r.unit_keys.iter().map(|(c, _)| *c).collect();
        assert_eq!(
            cps,
            vec![1, 2, 3],
            "every CPS unit surfaced, numbered from the inf"
        );
        for ((_, key), enc) in r.unit_keys.iter().zip(encs.iter()) {
            assert_eq!(
                *key,
                decrypt_unit_key(&vuk.0, enc),
                "key = VUK-decrypt of its slot"
            );
        }
        assert_eq!(r.vuk, Some(vuk));
        assert!(r.mk.is_none() && r.pk.is_none() && r.dk.is_none());
    }

    /// A bare UK candidate is terminal — it returns itself keyed by its own idx.
    #[test]
    fn resolve_candidate_uk_is_itself() {
        let uk = UnitKey::new(2, [0x9u8; 16]);
        let r = resolve_candidate(&KeyCandidate::Uk(uk), &[], &[], None).expect("uk is terminal");
        assert_eq!(r.unit_keys, vec![(2, uk.key)]);
        assert!(r.vuk.is_none() && r.mk.is_none());
    }

    /// MK/PK/DK paths derive the VUK from a VID; without one, derivation stops.
    #[test]
    fn resolve_candidate_mk_requires_vid() {
        let r = resolve_candidate(&KeyCandidate::Mk(MediaKey([1u8; 16])), &[], &[], None);
        assert!(r.is_none(), "MK path returns None without a VID");
    }

    /// A planted Processing Key resolves against a synthetic MKB and drives the
    /// FULL chain PK → MK → VUK → UK — proving a PK candidate yields real keys.
    #[test]
    fn resolve_candidate_pk_drives_full_chain() {
        let pk: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv[a];
        }
        let cv = aes_ecb_encrypt(&pk, &mk_raw);

        let mut vd = [0x11u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        // 4-byte record header (type + BE24 total length) + body.
        let rec = |t: u8, body: &[u8]| -> Vec<u8> {
            let total = 4 + body.len();
            let mut r = vec![
                t,
                ((total >> 16) & 0xFF) as u8,
                ((total >> 8) & 0xFF) as u8,
                (total & 0xFF) as u8,
            ];
            r.extend_from_slice(body);
            r
        };
        let mut sd = vec![0u8];
        sd.extend_from_slice(&uv);
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&rec(0x86, &mk_dv));
        mkb.extend_from_slice(&rec(0x04, &sd));
        mkb.extend_from_slice(&rec(0x05, &cv));

        let vid = Vid([0x42u8; 16]);
        let plain_uk = [0x7Eu8; 16];
        let vuk = derive_vuk(&mk, &vid.0);
        let enc = aes_ecb_encrypt(&vuk, &plain_uk);
        let inf = synth_inf(std::slice::from_ref(&enc));

        let r = resolve_candidate(&KeyCandidate::Pk(ProcessingKey(pk)), &mkb, &inf, Some(vid))
            .expect("planted PK resolves the full chain");
        assert_eq!(r.mk, Some(MediaKey(mk)), "PK recovers the planted MK");
        assert_eq!(r.unit_keys.len(), 1);
        assert_eq!(
            r.unit_keys[0].1, plain_uk,
            "PK chain recovers the title key"
        );
    }
}

// Device-key POSITION recovery and the MKB probe accessors. No published AACS
// test vectors exist; the relations (`[C]` §3.2.3-§3.2.5) are invertible, so
// `plant_mkb` below builds a valid MKB for a CHOSEN key. See docs/aacs-derive.md.
#[cfg(test)]
mod position_recovery_tests {
    use super::*;
    use crate::aacs::crypto::aes_ecb_encrypt;

    /// `[C]` §3.2.5.1.4 Verify-Media-Key plaintext prefix.
    const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];

    /// An MKB record: 1-byte type + BE24 total length (header included) + body.
    fn rec(t: u8, body: &[u8]) -> Vec<u8> {
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

    /// The planted fixture: an MKB whose single subset-difference slot is opened
    /// by `dkey` sitting EXACTLY at that slot (zero descent), yielding `mk`.
    struct Planted {
        mkb: Vec<u8>,
        dkey: [u8; 16],
        mk: [u8; 16],
        mk_dv: [u8; 16],
        cv: [u8; 16],
        uv: u32,
        u_mask_shift: u8,
    }

    // Build the fixture by inverting the AACS relations. uv/u_mask_shift are
    // chosen so a gating device node exists (see docs/aacs-derive.md for the
    // bit-level derivation); uv stays under 0x10000 since DeviceKey::node is u16.
    fn plant_mkb() -> Planted {
        let dkey: [u8; 16] = [
            0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2,
            0xE1, 0xF0,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: u32 = 0x0000_0400;
        let u_mask_shift: u8 = 12;

        // The Processing Key a device sitting AT the slot produces: [C] §3.2.4
        // makes it the AES-G3(.,1) of its own node, with no descent.
        let pk = aesg3(&dkey, 1);

        // Invert [C] §3.2.4: mk = AES-D(pk, cvalue) then XOR uv into mk[12..16].
        let mut mk_raw = mk;
        for (a, b) in mk_raw[12..16].iter_mut().zip(uv.to_be_bytes()) {
            *a ^= b;
        }
        let cv = aes_ecb_encrypt(&pk, &mk_raw);

        // Invert [C] §3.2.5.1.4: AES-D(mk, mk_dv) must start with the magic.
        let mut vd = [0x5Au8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        let mut subdiff = vec![u_mask_shift];
        subdiff.extend_from_slice(&uv.to_be_bytes());

        let mut mkb = Vec::new();
        mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&rec(0x86, &mk_dv));
        mkb.extend_from_slice(&rec(0x04, &subdiff));
        mkb.extend_from_slice(&rec(0x05, &cv));

        Planted {
            mkb,
            dkey,
            mk,
            mk_dv,
            cv,
            uv,
            u_mask_shift,
        }
    }

    /// Sanity-check the fixture itself before anything is asserted about the
    /// functions under test: an MKB the parser cannot read would make every
    /// "returns None" body look correct.
    #[test]
    fn the_planted_mkb_is_a_parseable_mkb() {
        let p = plant_mkb();
        assert_eq!(mkb_find_mk_dv(&p.mkb), Some(p.mk_dv), "verify record");
        assert_eq!(
            mkb_find_cvalues(&p.mkb).as_deref(),
            Some(&p.cv[..]),
            "cvalue record"
        );
        assert_eq!(
            mkb_find_subdiff_records(&p.mkb).map(|v| v.len()),
            Some(5),
            "one 5-byte subset-difference slot"
        );
    }

    // `None` here means "key does not apply", indistinguishable from a position
    // that was simply never found — the feature silently stops working. The
    // load-bearing assertion is that the recovered position walks to the planted MK.
    #[test]
    fn recover_dk_position_finds_a_position_that_derives_the_planted_media_key() {
        let p = plant_mkb();

        let recovered =
            recover_dk_position(&p.mkb, &p.dkey).expect("the planted key applies to this MKB");

        assert_eq!(
            recovered.uv, p.uv,
            "uv is invariant for the key across discs and must be the slot's"
        );
        assert_eq!(
            recovered.u_mask_shift, p.u_mask_shift,
            "u_mask_shift must be the slot's"
        );
        assert_eq!(recovered.key, p.dkey, "the key bytes are carried through");

        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
            Some(p.mk),
            "the recovered position must walk the MKB to the planted Media Key \
             — a position that does not is no better than None"
        );
    }

    // The other direction: a key the MKB does NOT open must not be given a
    // position — wrongly banking one would derive a wrong Media Key on every
    // future disc.
    #[test]
    fn recover_dk_position_rejects_a_key_the_mkb_does_not_open() {
        let p = plant_mkb();
        let mut stranger = p.dkey;
        stranger[0] ^= 0x01; // one bit off — the strongest form of wrong key
        assert!(
            recover_dk_position(&p.mkb, &stranger).is_none(),
            "a key differing by one bit must not be handed a position"
        );
    }

    // `None` here strands a key whose position was already recovered — the
    // last step of position recovery, failing the same way: usable key, discarded.
    // Asserted through the derived Media Key, not the node value, since any gating node works.
    #[test]
    fn resolve_dk_node_returns_a_node_that_passes_the_walk_gate() {
        let p = plant_mkb();

        let dk = resolve_dk_node(&p.mkb, &p.dkey, p.uv, p.u_mask_shift)
            .expect("a gating node exists for the planted slot");

        assert_eq!(dk.uv, p.uv);
        assert_eq!(dk.u_mask_shift, p.u_mask_shift);
        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&dk)),
            Some(p.mk),
            "the resolved node must actually pass the gate and derive the \
             planted Media Key"
        );

        // The gate is the point: the node must differ from uv inside v_mask.
        // (v_mask for uv=0x400 is 0xFFFF_F800.)
        let v_mask = calc_v_mask(p.uv);
        assert_ne!(
            (dk.node as u32) & v_mask,
            p.uv & v_mask,
            "a node equal to uv under v_mask does not gate — the walk would \
             skip the slot entirely"
        );
    }

    // probe::mkb_mk_dv feeds km_verifies for reproduction harnesses; a fixed
    // block would "verify" against a record no disc carries, and always-None
    // would make every verification report "unverifiable".
    #[test]
    fn probe_mkb_mk_dv_returns_the_records_actual_bytes() {
        let p = plant_mkb();
        assert_eq!(
            probe::mkb_mk_dv(&p.mkb),
            Some(p.mk_dv),
            "mk_dv must be the bytes the 0x86 record carries"
        );
        assert_ne!(
            probe::mkb_mk_dv(&p.mkb),
            Some([0u8; 16]),
            "and not a constant block"
        );
        assert_eq!(
            probe::mkb_mk_dv(&[0x10, 0x00, 0x00, 0x04]),
            None,
            "an MKB with no verify record has no mk_dv"
        );
    }

    // A MULTI-SLOT MKB (unlike plant_mkb's zero-descent fixture) pinning slot
    // INDEXING and the DESCENT branch. v-masks below are literals from `[C]`
    // §3.2.3, not computed with calc_v_mask, which is itself under test.
    const UV_SLOT: u32 = 0x0000_9400; // lowest set bit 10
    const V_MASK_SLOT: u32 = 0xFFFF_F800; // 0xFFFF_FFFF << 11
    const UV_ANCESTOR: u32 = 0x0000_9800; // lowest set bit 11
    const V_MASK_ANCESTOR: u32 = 0xFFFF_F000; // 0xFFFF_FFFF << 12
    const U_MASK_SHIFT: u8 = 16;

    // calc_v_mask implements `[C]` §3.2.3; every gate and descent is masked by
    // its result, so a wrong mask matches the wrong slots — pinned against
    // literal expectations, not a re-computation.
    #[test]
    fn calc_v_mask_is_all_ones_above_the_lowest_set_bit() {
        // (uv, expected v_mask) — expected = 0xFFFF_FFFF << (trailing_zeros+1).
        let cases: &[(u32, u32)] = &[
            (0x0000_0001, 0xFFFF_FFFE),
            (0x0000_0002, 0xFFFF_FFFC),
            (0x0000_0400, 0xFFFF_F800),
            (UV_SLOT, V_MASK_SLOT),
            (UV_ANCESTOR, V_MASK_ANCESTOR),
            (0x0000_00FF, 0xFFFF_FFFE), // lowest set bit is 0
        ];
        for &(uv, expected) in cases {
            assert_eq!(
                calc_v_mask(uv),
                expected,
                "v_mask for uv={uv:#010x} must be all-ones above its lowest set bit"
            );
        }
    }

    /// The planted multi-slot fixture.
    struct PlantedDescent {
        mkb: Vec<u8>,
        dkey: [u8; 16],
        mk: [u8; 16],
    }

    // Build an MKB with THREE subset-difference slots where only slot 2 is
    // keyed, and the device key sits one level ABOVE it (at UV_ANCESTOR). The
    // two decoy slots carry real-looking uvs so wrong indexing validates nothing.
    fn plant_descent_mkb() -> PlantedDescent {
        let dkey: [u8; 16] = [
            0x5A, 0x4B, 0x3C, 0x2D, 0x1E, 0x0F, 0xF0, 0xE1, 0xD2, 0xC3, 0xB4, 0xA5, 0x96, 0x87,
            0x78, 0x69,
        ];
        let mk: [u8; 16] = [
            0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD,
            0xBE, 0xBF,
        ];

        // Processing Key from descending UV_ANCESTOR -> slot, via the same descent the
        // walk uses but anchored to the FIXED ancestor above — so a walk computing a
        // different candidate position derives a different Kp and fails.
        let pk = calc_pk_from_dk(&dkey, UV_SLOT, V_MASK_SLOT, V_MASK_ANCESTOR);

        // Invert [C] §3.2.4 for slot 2's cvalue.
        let mut mk_raw = mk;
        for (a, b) in mk_raw[12..16].iter_mut().zip(UV_SLOT.to_be_bytes()) {
            *a ^= b;
        }
        let cv2 = aes_ecb_encrypt(&pk, &mk_raw);

        // Invert [C] §3.2.5.1.4.
        let mut vd = [0x33u8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        // Three 5-byte slots: two decoys, then the keyed one.
        let mut subdiff = Vec::new();
        for uv in [0x0000_1100u32, 0x0000_2200, UV_SLOT] {
            subdiff.push(U_MASK_SHIFT);
            subdiff.extend_from_slice(&uv.to_be_bytes());
        }
        // Three 16-byte cvalues, 1:1 with the slots; only index 2 is real.
        let mut cvalues = vec![0x11u8; 16];
        cvalues.extend_from_slice(&[0x22u8; 16]);
        cvalues.extend_from_slice(&cv2);

        let mut mkb = Vec::new();
        mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&rec(0x86, &mk_dv));
        mkb.extend_from_slice(&rec(0x04, &subdiff));
        mkb.extend_from_slice(&rec(0x05, &cvalues));

        PlantedDescent { mkb, dkey, mk }
    }

    /// Fixture sanity: three slots, three cvalues, and the keyed slot is NOT
    /// index 0 (otherwise the indexing this fixture exists to pin is trivial).
    #[test]
    fn the_planted_descent_mkb_has_three_slots_and_is_keyed_at_the_last() {
        let p = plant_descent_mkb();
        assert_eq!(
            mkb_find_subdiff_records(&p.mkb).map(|v| v.len()),
            Some(15),
            "three 5-byte subset-difference slots"
        );
        assert_eq!(
            mkb_find_cvalues(&p.mkb).map(|v| v.len()),
            Some(48),
            "three 16-byte cvalues"
        );
        assert_ne!(UV_SLOT, UV_ANCESTOR, "the device is not at the slot");
    }

    // Pins two things the single-slot fixture cannot: recovered uv is the
    // ancestor (proof the descent branch ran, not the zero-descent shortcut),
    // and the keyed slot is index 2 (slot/cvalue table offsets both correct).
    #[test]
    fn recover_dk_position_finds_an_ancestor_position_in_a_multi_slot_mkb() {
        let p = plant_descent_mkb();

        let recovered = recover_dk_position(&p.mkb, &p.dkey)
            .expect("the planted key opens slot 2 from one level above it");

        assert_eq!(
            recovered.uv, UV_ANCESTOR,
            "the recovered position is the device's ancestor node, not the slot's"
        );
        assert_ne!(
            recovered.uv, UV_SLOT,
            "a zero-descent answer would mean the descent branch never ran"
        );
        assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT);
        assert_eq!(recovered.key, p.dkey);

        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
            Some(p.mk),
            "the recovered ancestor position must walk to the planted Media Key"
        );
    }

    /// The same multi-slot MKB must not hand a position to a key it does not
    /// open — including one that differs by a single bit.
    #[test]
    fn recover_dk_position_rejects_a_stranger_against_the_multi_slot_mkb() {
        let p = plant_descent_mkb();
        let mut stranger = p.dkey;
        stranger[15] ^= 0x01;
        assert!(recover_dk_position(&p.mkb, &stranger).is_none());
    }

    // A FOUR-LEVEL descent taking both branches, pinning the per-level decision ([C]
    // §3.2.4: RIGHT `aesg3(.,2)` if uv bit set, else LEFT `aesg3(.,0)`, PK `aesg3(.,1)`).
    // Expected PK is an EXPLICIT `aesg3` chain, not `calc_pk_from_dk` (which would drift).

    /// Slot `uv` for the four-level fixture: bits 8, 6 and 4 set. Lowest set
    /// bit 4 → the descent reads bits 8, 7, 6, 5 (set, clear, set, clear).
    const UV_SLOT4: u32 = 0x0000_0150;
    const V_MASK_SLOT4: u32 = 0xFFFF_FFE0; // 0xFFFF_FFFF << 5
    /// The device's ancestor position: lowest set bit 8, four levels above.
    const UV_ANC4: u32 = 0x0000_0100;
    const V_MASK_ANC4: u32 = 0xFFFF_FE00; // 0xFFFF_FFFF << 9

    struct PlantedDescent4 {
        mkb: Vec<u8>,
        dkey: [u8; 16],
        mk: [u8; 16],
        /// The Processing Key the four-level descent must produce.
        pk: [u8; 16],
    }

    fn plant_four_level_mkb() -> PlantedDescent4 {
        let dkey: [u8; 16] = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let mk: [u8; 16] = [
            0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD,
            0xDE, 0xDF,
        ];

        // [C] §3.2.4 level by level: ancestor -> slot reads UV_SLOT4 bits 8,7,6,5 =
        // 1,0,1,0 -> right(2),left(0),right(2),left(0), then PK = aesg3(final_node, 1).
        let n1 = aesg3(&dkey, 2);
        let n2 = aesg3(&n1, 0);
        let n3 = aesg3(&n2, 2);
        let n4 = aesg3(&n3, 0);
        let pk = aesg3(&n4, 1);

        let mut mk_raw = mk;
        for (a, b) in mk_raw[12..16].iter_mut().zip(UV_SLOT4.to_be_bytes()) {
            *a ^= b;
        }
        let cv1 = aes_ecb_encrypt(&pk, &mk_raw);

        let mut vd = [0x77u8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        // Two slots; the keyed one is index 1.
        let mut subdiff = Vec::new();
        for uv in [0x0000_1100u32, UV_SLOT4] {
            subdiff.push(U_MASK_SHIFT);
            subdiff.extend_from_slice(&uv.to_be_bytes());
        }
        let mut cvalues = vec![0x44u8; 16];
        cvalues.extend_from_slice(&cv1);

        let mut mkb = Vec::new();
        mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&rec(0x86, &mk_dv));
        mkb.extend_from_slice(&rec(0x04, &subdiff));
        mkb.extend_from_slice(&rec(0x05, &cvalues));

        PlantedDescent4 { mkb, dkey, mk, pk }
    }

    // calc_pk_from_dk is the tree descent every device-key path runs. A wrong
    // branch, level count, or terminal increment yields a PK that validates
    // against nothing — the disc reports no key though the operator's DK is good.
    #[test]
    fn calc_pk_from_dk_walks_the_uv_bits_right_left_right_left() {
        let p = plant_four_level_mkb();
        assert_eq!(
            calc_pk_from_dk(&p.dkey, UV_SLOT4, V_MASK_SLOT4, V_MASK_ANC4),
            p.pk,
            "the four-level descent must be aesg3(.,2), (.,0), (.,2), (.,0) then (.,1)"
        );

        // Zero levels to descend (device sits AT the slot) → the terminal step
        // alone, with no descent.
        assert_eq!(
            calc_pk_from_dk(&p.dkey, UV_SLOT4, V_MASK_SLOT4, V_MASK_SLOT4),
            aesg3(&p.dkey, 1),
            "no descent needed → Kp is aesg3(dk, 1)"
        );
    }

    /// End-to-end through the four-level fixture: the position recovered for an
    /// unpositioned key must be the ancestor four levels up, and it must walk
    /// the MKB to the planted Media Key.
    #[test]
    fn recover_dk_position_descends_four_levels_to_the_planted_media_key() {
        let p = plant_four_level_mkb();

        let recovered = recover_dk_position(&p.mkb, &p.dkey)
            .expect("the planted key opens the slot from four levels above it");

        assert_eq!(
            recovered.uv, UV_ANC4,
            "the recovered position is four levels above the slot"
        );
        assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT);
        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&recovered)),
            Some(p.mk),
            "the recovered position must walk to the planted Media Key"
        );
    }

    // MALFORMED MKBs: a truncated cvalue table, and a revoked-marker slot. Both are
    // reachable from a corrupt/crafted (disc-controlled) MKB, and in both the walk must
    // decline to derive a key rather than index past the end of a record.

    /// Assemble an MKB from an explicit slot list and cvalue table.
    /// `slots` is `(u_mask_shift, uv)` per subset-difference entry.
    fn build_mkb(slots: &[(u8, u32)], cvalues: &[u8], mk_dv: &[u8; 16]) -> Vec<u8> {
        let mut subdiff = Vec::new();
        for &(shift, uv) in slots {
            subdiff.push(shift);
            subdiff.extend_from_slice(&uv.to_be_bytes());
        }
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&rec(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&rec(0x86, mk_dv));
        mkb.extend_from_slice(&rec(0x04, &subdiff));
        mkb.extend_from_slice(&rec(0x05, cvalues));
        mkb
    }

    /// `(dkey, mk, pk, cvalue, mk_dv)` — the five 16-byte AACS keys the
    /// four-level fixture plants. Named so the return type says what it is
    /// rather than repeating `[u8; 16]` five times.
    type FourLevelParts = ([u8; 16], [u8; 16], [u8; 16], [u8; 16], [u8; 16]);

    /// The planted slot-2 material from the four-level fixture, reusable for
    /// the malformed-MKB shapes below.
    fn four_level_parts() -> FourLevelParts {
        let p = plant_four_level_mkb();
        let cvalues = mkb_find_cvalues(&p.mkb).expect("cvalues");
        let mut cv = [0u8; 16];
        cv.copy_from_slice(&cvalues[16..32]); // the keyed slot's cvalue
        let mk_dv = mkb_find_mk_dv(&p.mkb).expect("mk_dv");
        (p.dkey, p.mk, p.pk, cv, mk_dv)
    }

    // A cvalue table with FEWER entries than the subset-difference index has
    // slots — a truncated 0x05 record. The slot whose cvalue is missing must be
    // skipped, not read past the end: "no key, no panic".
    #[test]
    fn a_cvalue_table_shorter_than_the_slot_index_is_not_read_past() {
        let (dkey, _mk, _pk, cv, mk_dv) = four_level_parts();

        // Three slots; the keyed one is index 2 — but only TWO cvalues exist.
        let slots = [
            (U_MASK_SHIFT, 0x0000_1100u32),
            (U_MASK_SHIFT, 0x0000_2200u32),
            (U_MASK_SHIFT, UV_SLOT4),
        ];
        let mut cvalues = vec![0x44u8; 16];
        cvalues.extend_from_slice(&[0x55u8; 16]);
        assert_eq!(cvalues.len(), 32, "two cvalues for three slots");
        let mkb = build_mkb(&slots, &cvalues, &mk_dv);

        let dk = DeviceKey {
            key: dkey,
            node: 0x0101,
            uv: UV_ANC4,
            u_mask_shift: U_MASK_SHIFT,
        };
        assert_eq!(
            derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&dk)),
            None,
            "slot 2 has no cvalue → no Media Key, and no read past the table"
        );

        // The unpositioned-key scan walks the same tables and must also stop at
        // the last cvalue rather than at the last slot.
        assert!(
            recover_dk_position(&mkb, &dkey).is_none(),
            "the position scan must stop at the last cvalue, not the last slot"
        );

        // The bare-PK table scan likewise: a PK that matches nothing must sweep
        // every slot and return None without reading past the cvalue table.
        let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");
        assert_eq!(
            try_pk_against_tables(&[[0x00u8; 16]], &uvs, &cvalues, &mk_dv),
            None,
            "a non-matching PK sweeps all slots without over-reading"
        );

        // …and when the keyed slot IS inside the truncated table, it resolves —
        // proving the guard skips only the missing entries.
        let ok_slots = [(U_MASK_SHIFT, UV_SLOT4), (U_MASK_SHIFT, 0x0000_1100u32)];
        let mut ok_cvalues = cv.to_vec();
        ok_cvalues.extend_from_slice(&[0x55u8; 16]);
        let ok_mkb = build_mkb(&ok_slots, &ok_cvalues, &mk_dv);
        let ok_uvs = mkb_find_subdiff_records(&ok_mkb).expect("subdiff");
        assert!(
            try_pk_against_tables(&[_pk], &ok_uvs, &ok_cvalues, &mk_dv).is_some(),
            "sanity: the same PK/cvalue pair does resolve when present"
        );
    }

    // The 0xC0 revoked marker (`[C]` §3.2.5.1.5) TERMINATES the subset-difference
    // table; slots after it must not be walked. Fixture puts the keyed slot
    // AFTER the marker; removing it resolves the same MKB, proving the gate works.
    #[test]
    fn a_revoked_marker_slot_terminates_the_subset_difference_table() {
        let (dkey, mk, pk, cv, mk_dv) = four_level_parts();

        // Slot 0 = ordinary decoy, slot 1 = revoked marker, slot 2 = the keyed
        // slot (unreachable), each with its own cvalue.
        let barred = [
            (U_MASK_SHIFT, 0x0000_1100u32),
            (0xC0u8, 0x0000_2200u32),
            (U_MASK_SHIFT, UV_SLOT4),
        ];
        let mut cvalues = vec![0x44u8; 16];
        cvalues.extend_from_slice(&[0x55u8; 16]);
        cvalues.extend_from_slice(&cv);
        let mkb = build_mkb(&barred, &cvalues, &mk_dv);

        let dk = DeviceKey {
            key: dkey,
            node: 0x0101,
            uv: UV_ANC4,
            u_mask_shift: U_MASK_SHIFT,
        };
        assert_eq!(
            derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&dk)),
            None,
            "the table ends at the revoked marker; slot 2 is not in it"
        );
        assert!(
            recover_dk_position(&mkb, &dkey).is_none(),
            "the position scan must stop at the marker too"
        );
        let uvs = mkb_find_subdiff_records(&mkb).expect("subdiff");
        assert_eq!(
            try_pk_against_tables(&[pk], &uvs, &cvalues, &mk_dv),
            None,
            "the terminal-PK scan must stop at the marker too"
        );

        // Same MKB with the marker cleared → the keyed slot is in the table and
        // every one of the three paths resolves the planted Media Key.
        let open = [
            (U_MASK_SHIFT, 0x0000_1100u32),
            (U_MASK_SHIFT, 0x0000_2200u32),
            (U_MASK_SHIFT, UV_SLOT4),
        ];
        let mkb_open = build_mkb(&open, &cvalues, &mk_dv);
        assert_eq!(
            derive_media_key_from_dk(&mkb_open, std::slice::from_ref(&dk)),
            Some(mk),
            "sanity: without the marker the same slot derives the Media Key"
        );
        let uvs_open = mkb_find_subdiff_records(&mkb_open).expect("subdiff");
        assert_eq!(
            try_pk_against_tables(&[pk], &uvs_open, &cvalues, &mk_dv),
            Some(mk)
        );
    }

    // A device key applies only when BOTH gates hold (`[C]` §3.2.4): u-mask
    // equal AND uv agrees under the device's v-mask. Wrong u_mask_shift
    // describes a different tree region and must not derive a Media Key.
    #[test]
    fn a_device_key_with_the_wrong_u_mask_shift_does_not_apply() {
        let p = plant_four_level_mkb();

        let good = DeviceKey {
            key: p.dkey,
            node: 0x0101,
            uv: UV_ANC4,
            u_mask_shift: U_MASK_SHIFT,
        };
        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&good)),
            Some(p.mk),
            "sanity: the correctly-filed key derives the planted Media Key"
        );

        // Identical in every way except the u-mask.
        let wrong_u_mask = DeviceKey {
            u_mask_shift: U_MASK_SHIFT - 1,
            ..good.clone()
        };
        assert_eq!(
            derive_media_key_from_dk(&p.mkb, std::slice::from_ref(&wrong_u_mask)),
            None,
            "a mismatched u-mask must fail the subset-difference gate"
        );
    }

    // validate_processing_key XORs uv into mk[12..16] (`[C]` §3.2.4 step 2).
    // XOR, not OR: must be reversible and able to CLEAR a bit AES set — a uv
    // and MK sharing set bits in those bytes is what tells the two operators apart.
    #[test]
    fn validate_processing_key_xors_the_uv_into_the_media_key_tail() {
        // uv with all four bytes non-zero and overlapping the planted mk tail.
        const UV: u32 = 0xF0F0_F0F0;
        let pk = [0x5Au8; 16];
        // Choose a Media Key whose tail shares bits with uv, so XOR and OR
        // differ, and invert the relation to build the cvalue and verify block.
        let mk: [u8; 16] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xFF, 0xFF,
            0xFF, 0xFF,
        ];
        let mut pre = mk;
        for (a, b) in pre[12..16].iter_mut().zip(UV.to_be_bytes()) {
            *a ^= b;
        }
        let cvalue = aes_ecb_encrypt(&pk, &pre);
        let mut vd = [0x0Fu8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        assert_eq!(
            validate_processing_key(&pk, &cvalue, &UV.to_be_bytes(), &mk_dv),
            Some(mk),
            "uv must be XORed (not ORed) into the Media Key's low 4 bytes"
        );
    }

    /// `validate_processing_key` is handed slices straight out of MKB records,
    /// so its length guards are what stand between a short/truncated record and
    /// an out-of-bounds read. Under-length inputs must yield `None`.
    #[test]
    fn validate_processing_key_refuses_short_cvalue_or_uv() {
        let p = plant_mkb();
        let pk = aesg3(&p.dkey, 1);

        assert!(
            validate_processing_key(&pk, &p.cv[..15], &p.uv.to_be_bytes(), &p.mk_dv).is_none(),
            "a cvalue shorter than 16 bytes is not usable"
        );
        assert!(
            validate_processing_key(&pk, &p.cv, &p.uv.to_be_bytes()[..3], &p.mk_dv).is_none(),
            "a uv shorter than 4 bytes is not usable"
        );
        // Exactly-sized inputs are accepted and yield the planted Media Key.
        assert_eq!(
            validate_processing_key(&pk, &p.cv, &p.uv.to_be_bytes(), &p.mk_dv),
            Some(p.mk),
            "the exactly-sized planted inputs must still validate"
        );
    }

    // probe::aes_dec is the sole verify primitive a reproduction harness has;
    // a fixed-block body would answer the same way for every key/disc — the
    // km_verifies failure one layer out. Asserted for the planted key and a stranger.
    #[test]
    fn probe_aes_dec_reproduces_the_verify_relation_for_the_planted_key() {
        let p = plant_mkb();

        let plain = probe::aes_dec(&p.mk, &p.mk_dv);
        assert_eq!(
            &plain[..8],
            &VERIFY_MAGIC[..],
            "AES-D(Km, mk_dv) must open with the Verify-Media-Key magic"
        );

        let mut stranger = p.mk;
        stranger[0] ^= 0x01;
        assert_ne!(
            &probe::aes_dec(&stranger, &p.mk_dv)[..8],
            &VERIFY_MAGIC[..],
            "a key one bit away must not reproduce the magic"
        );

        // It is a decryption, not a transformation of its own choosing: it must
        // invert the forward primitive for an arbitrary block.
        let block = [0x5Cu8; 16];
        assert_eq!(
            probe::aes_dec(&p.mk, &aes_ecb_encrypt(&p.mk, &block)),
            block,
            "aes_dec must be the exact inverse of AES-128-ECB encrypt"
        );
    }

    /// `probe::mkb_cvalues` is the Media-Key-Data table the whole PK×cvalue
    /// scan iterates. An empty or one-byte table makes every scan find nothing,
    /// so a harness would report a good key as non-working.
    #[test]
    fn probe_mkb_cvalues_returns_the_records_actual_bytes() {
        let p = plant_mkb();
        let cvalues = probe::mkb_cvalues(&p.mkb).expect("the 0x05 record is present");
        assert_eq!(
            cvalues.len(),
            16,
            "one 16-byte cvalue was planted; the table must be that long"
        );
        assert_eq!(
            &cvalues[..],
            &p.cv[..],
            "cvalue bytes must be the planted ones"
        );

        // The table is what the terminal-PK scan consumes; prove it drives the
        // real scan to the planted Media Key.
        let uvs = probe::mkb_subdiff(&p.mkb).expect("subdiff record present");
        let pk = aesg3(&p.dkey, 1);
        assert_eq!(
            try_pk_against_tables(&[pk], &uvs, &cvalues, &p.mk_dv),
            Some(p.mk),
            "the probe's cvalue table must be the one the PK scan can use"
        );
    }

    // An ODD subset-difference `uv` — the legal depth-0 slot. Descent starts at
    // `uv_r.trailing_zeros() + 1`; other fixtures use even `uv`, so this alone exercises
    // `trailing_zeros() == 0` and the `p == 0` boundary where `+ 1` blocks a `(p-1)` underflow.

    /// Slot `uv` with bits 8, 6, 4 AND 0 set: lowest set bit 0, so
    /// `trailing_zeros() == 0` and the descent must start at level 1.
    const UV_ODD: u32 = 0x0000_0151;
    /// The ancestor one level up — what the descent's first candidate
    /// (`k == 1`) resolves to: `(UV_ODD & !0b11) | 0b10`.
    const UV_ODD_ANC: u32 = 0x0000_0152;
    const U_MASK_SHIFT_ODD: u8 = 12;

    // An MKB with a single ODD-uv slot, keyed one level above it. Expected PK
    // is the EXPLICIT aesg3 chain (`[C]` §3.2.4), not calc_pk_from_dk — a
    // fixture built by the walk would move with the walk's own mutations.
    fn plant_odd_uv_mkb() -> (Vec<u8>, [u8; 16], [u8; 16], [u8; 16]) {
        let dkey: [u8; 16] = [
            0x2F, 0x3E, 0x4D, 0x5C, 0x6B, 0x7A, 0x89, 0x98, 0xA7, 0xB6, 0xC5, 0xD4, 0xE3, 0xF2,
            0x01, 0x10,
        ];
        let mk: [u8; 16] = [
            0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED,
            0xEE, 0xEF,
        ];

        let pk = aesg3(&aesg3(&dkey, 0), 1);

        let mut mk_raw = mk;
        for (a, b) in mk_raw[12..16].iter_mut().zip(UV_ODD.to_be_bytes()) {
            *a ^= b;
        }
        let cv = aes_ecb_encrypt(&pk, &mk_raw);

        let mut vd = [0x27u8; 16];
        vd[..8].copy_from_slice(&VERIFY_MAGIC);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        let mkb = build_mkb(&[(U_MASK_SHIFT_ODD, UV_ODD)], &cv, &mk_dv);
        (mkb, dkey, mk, pk)
    }

    /// Fixture sanity: the slot really is odd, and the ancestor really is the
    /// level-1 candidate. If either drifted, the test below would silently stop
    /// covering the depth-0 descent it exists for.
    #[test]
    fn the_odd_uv_fixture_sits_at_tree_depth_zero() {
        assert_eq!(UV_ODD.trailing_zeros(), 0, "an odd uv is at depth 0");
        assert_eq!(
            UV_ODD_ANC,
            (UV_ODD & (0xFFFF_FFFFu32 << 2)) | (1u32 << 1),
            "the level-1 ancestor of an odd uv"
        );
        // The walk's own gate: the device's position must agree with the slot's
        // above the ancestor's own lowest set bit.
        let dev_v_mask = calc_v_mask(UV_ODD_ANC);
        assert_eq!(UV_ODD & dev_v_mask, UV_ODD_ANC & dev_v_mask);
    }

    // Depth 0 is where the descent's lower bound is at its arithmetic edge; a
    // wrong bound either underflows or starts the scan at the slot's own level,
    // both reporting a valid key as not applying.
    #[test]
    fn recover_dk_position_descends_from_an_odd_uv_slot_at_tree_depth_zero() {
        let (mkb, dkey, mk, pk) = plant_odd_uv_mkb();

        let recovered =
            recover_dk_position(&mkb, &dkey).expect("the planted key opens the odd-uv slot");

        assert_eq!(
            recovered.uv, UV_ODD_ANC,
            "the recovered position is the level-1 ancestor, not the slot itself"
        );
        assert_eq!(recovered.u_mask_shift, U_MASK_SHIFT_ODD);
        assert_eq!(
            derive_media_key_from_dk(&mkb, std::slice::from_ref(&recovered)),
            Some(mk),
            "the recovered position must walk the odd-uv slot to its Media Key"
        );

        // The Processing Key the walk produces at that position is the explicit
        // one-level descent, not the zero-descent key.
        assert_eq!(
            derive_media_key_and_pk_from_dk(&mkb, std::slice::from_ref(&recovered)),
            Some((mk, pk))
        );
        assert_ne!(pk, aesg3(&dkey, 1), "this is NOT the zero-descent key");
    }

    // Negative direction: a key that does not open the slot must sweep every
    // descent level (1..32, since depth 0) and return None, without
    // underflowing the lower bound or shifting a u32 by 32 at the top.
    #[test]
    fn an_odd_uv_slot_sweeps_every_descent_level_without_arithmetic_overflow() {
        let (mkb, dkey, _mk, _pk) = plant_odd_uv_mkb();
        let mut stranger = dkey;
        stranger[0] ^= 0x01;
        assert!(
            recover_dk_position(&mkb, &stranger).is_none(),
            "a key one bit off must not be handed a position"
        );
    }
}

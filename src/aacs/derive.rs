//! Media-key derivation: DK/PK → Media Key via the subset-difference tree.
//! `[C]` §3.2.2–§3.2.5.

use super::crypto::*;
use super::inf::*;
use super::mkb::*;
use super::types::*;

/// `[C]` §3.2.5.1.4 Verify-Media-Key plaintext prefix.
pub(super) const VERIFY_MAGIC: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];

/// Derive Media Key from MKB data using processing keys.
///
/// A Processing Key is **terminal**: tried *directly* against the MKB
/// cvalue tables (no tree descent) — the fast path.
///
/// If you hold a **device-node label** at unknown tree depth (not a
/// terminal PK), use [`derive_media_key_from_dk`] instead — only that path
/// walks the Subset-Difference tree.
pub fn derive_media_key_from_pk(mkb: &[u8], processing_keys: &[[u8; 16]]) -> Option<[u8; 16]> {
    let t = MkbTables::parse(mkb)?;
    try_pk_against_tables(processing_keys, t.uvs, t.cvalues, &t.mk_dv)
}

// The three MKB tables every DK/PK derivation reads, located once and BORROWED
// from the MKB (the cvalue table alone is MBs on a UHD MKB).
pub(crate) struct MkbTables<'a> {
    mk_dv: [u8; 16],
    uvs: &'a [u8],
    cvalues: &'a [u8],
}

/// Why an MKB cannot be derived through at all, as opposed to no key matching it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MkbClassError {
    /// Class II (`0x000A1003`): its Media Key Data is record `0x0c`, which the classical
    /// PK/DK derivation (record `0x05`) does not read.
    Unsupported(MkbType),
}

impl MkbClassError {
    /// The stable numeric error code ([`E_MKB_CLASS_UNSUPPORTED`](crate::error::E_MKB_CLASS_UNSUPPORTED)).
    pub fn code(&self) -> u16 {
        crate::error::E_MKB_CLASS_UNSUPPORTED
    }
}

/// `Err` when this MKB's class has no Media Key Data the PK/DK derivation can read, so a
/// caller can report it instead of "no key matched". `Ok` for every other (or no) MKB.
pub fn check_mkb_class(mkb: &[u8]) -> Result<(), MkbClassError> {
    match mkb_type(mkb) {
        Some(t @ MkbType::ClassII) => Err(MkbClassError::Unsupported(t)),
        _ => Ok(()),
    }
}

impl<'a> MkbTables<'a> {
    pub(crate) fn parse(mkb: &'a [u8]) -> Option<Self> {
        if let Err(e) = check_mkb_class(mkb) {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "mkb_class",
                error_code = e.code(),
                "Class II MKB (0x000A1003) keeps its media key data in record 0x0c; PK/DK \
                 derivation reads only 0x05 and cannot derive a media key from it"
            );
            return None;
        }
        Some(Self {
            mk_dv: mkb_find_mk_dv(mkb)?,
            uvs: find_record_slice(mkb, REC_SUBSET_DIFFERENCE)?,
            cvalues: find_record_slice(mkb, REC_MEDIA_KEY_DATA)?,
        })
    }
}

// Core terminal-PK table scan: each PK tried directly against every (uv, cvalue) pair, no tree
// descent. Factored out so reproduction harnesses can drive it with explicit tables.
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
        // `pk` is loop-invariant across every (uv, cvalue) candidate below (up to ~46k on a
        // UHD MKB): build its AES-128 schedule once (L104) and reuse it via
        // `validate_processing_key_with_cipher`, byte-identical to `validate_processing_key`.
        let cipher = new_cipher_for(pk);
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
            if let Some(mk) = validate_processing_key_with_cipher(&cipher, cv, uv, mk_dv) {
                return Some(mk);
            }
        }
    }
    None
}

// Same relation as [`validate_processing_key`], but driven by a caller-supplied AES-128 key
// schedule for `pk` instead of building one internally — the cvalue-loop fast path
// ([`try_pk_against_tables`]) shares one schedule across every candidate for a given pk (L104).
fn validate_processing_key_with_cipher(
    cipher: &aes::Aes128,
    cvalue: &[u8],
    uv: &[u8],
    mk_dv: &[u8; 16],
) -> Option<[u8; 16]> {
    if cvalue.len() < 16 || uv.len() < 4 {
        return None;
    }

    // Step 1: mk = AES-128D(pk, cvalue), via the shared schedule.
    let mut cv = [0u8; 16];
    cv.copy_from_slice(&cvalue[..16]);
    let mut mk = aes_ecb_decrypt_with_cipher(cipher, &cv);

    // Step 2: XOR uv into the last 4 bytes of mk (mk[12..16]).
    for a in 0..4 {
        mk[12 + a] ^= uv[a];
    }

    // Step 3 + 4: dec_vd = AES-128D(mk, mk_dv); verify magic.
    let dec_vd = aes_ecb_decrypt(&mk, mk_dv);
    if dec_vd[..8] == VERIFY_MAGIC {
        return Some(mk);
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
    dk_walk(&MkbTables::parse(mkb)?, device_keys)
}

// The subset-difference DK walk over already-located tables.
fn dk_walk(t: &MkbTables<'_>, device_keys: &[DeviceKey]) -> Option<([u8; 16], [u8; 16])> {
    let (mk_dv, uvs, cvalues) = (&t.mk_dv, t.uvs, t.cvalues);

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
                            validate_processing_key(&pk, cv, &uvs[1 + uvs_idx * 5..], mk_dv)
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
pub fn recover_dk_position(mkb: &[u8], key: &[u8; 16]) -> Option<DeviceKey> {
    let t = MkbTables::parse(mkb)?;
    let (mk_dv, uvs, cvalues) = (t.mk_dv, t.uvs, t.cvalues);
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
    // `u_mask_shift` is disc/keydb-controlled: cap at the 32 real u32 bit positions
    // so `1u32 << b` can't overflow. Tables are located once, not per candidate node.
    let tables = MkbTables::parse(mkb);
    for b in 0..u_mask_shift.min(32) {
        let dk = DeviceKey {
            key: *key,
            node: ((uv ^ (1u32 << b)) & 0xFFFF) as u16,
            uv,
            u_mask_shift,
        };
        if tables
            .as_ref()
            .is_some_and(|t| dk_walk(t, std::slice::from_ref(&dk)).is_some())
        {
            return Some(dk);
        }
    }
    // Parsed tables but no node gates: the position is unwalkable, so don't bank it.
    if tables.is_some() {
        return None;
    }
    // No parseable tables: fall back to the node itself.
    Some(DeviceKey {
        key: *key,
        node: (uv & 0xFFFF) as u16,
        uv,
        u_mask_shift,
    })
}

/// Public, side-effect-free accessors over the MKB record helpers, exposed so
/// independent reproduction harnesses can exercise the exact same parser + verify
/// primitives the production walk uses.
/// These are thin wrappers — no new logic.
#[doc(hidden)]
pub mod probe {
    use super::super::crypto::aes_ecb_decrypt;

    /// `mk_dv` from the MKB's Verify-Media-Key record (type 0x81 / 0x86).
    pub fn mkb_mk_dv(mkb: &[u8]) -> Option<[u8; 16]> {
        super::mkb_find_mk_dv(mkb)
    }

    /// Body of the MKB's Explicit Subset-Difference record (type 0x04).
    pub fn mkb_subdiff(mkb: &[u8]) -> Option<Vec<u8>> {
        super::mkb_find_subdiff_records(mkb)
    }

    /// Body of the MKB's Media-Key-Data (cvalues) record `0x05` (1:1 with the
    /// `0x04` Explicit Subset-Difference list). `0x07` is an index, never read.
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
            Some(mk_dv) => aes_ecb_decrypt(km, &mk_dv)[..8] == super::VERIFY_MAGIC,
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
// `resolve_candidate` calls.
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
/// `VUK → UKs`, or `UK → itself`, parsing `Unit_Key_RO.inf` at `version` (from
/// [`resolve_aacs_version`](crate::aacs::mkb::resolve_aacs_version), the resolver the scan
/// uses) so a multi-CPS disc yields all its unit keys at the right stride.
///
/// PURE DERIVATION: no sampling, no validation, no position recovery. Returns `None` only when
/// derivation itself cannot proceed.
pub fn resolve_candidate(
    candidate: &KeyCandidate,
    mkb: &[u8],
    unit_key_ro: &[u8],
    vid: Option<Vid>,
    version: AacsVersion,
) -> Option<ResolvedChain> {
    // Boil a VUK → all unit keys, each paired with its declared CPS-unit number, through
    // the shared `derive_unit_keys` (the one place a VUK unwraps the unit keys).
    let boil = |vuk: Vuk| -> Option<Vec<(u32, [u8; 16])>> {
        // BD/UHD Unit_Key_RO.inf or HD DVD VTKF000.AACS — dispatched by magic.
        let ukf = parse_title_keys(unit_key_ro, version)?;
        if ukf.encrypted_keys.is_empty() {
            return None;
        }
        Some(derive_unit_keys(&ukf, &vuk.0))
    };

    match candidate {
        KeyCandidate::Uk(uk) => {
            // `uk.idx` is positional; surface the declared CPS-unit number like the other arms,
            // falling back to the position if the file does not parse or lacks that slot.
            let declared = parse_title_keys(unit_key_ro, version)
                .and_then(|f| f.encrypted_keys.get(uk.idx as usize).map(|k| k.0))
                .unwrap_or(uk.idx);
            Some(ResolvedChain {
                unit_keys: vec![(declared, uk.key)],
                vuk: None,
                mk: None,
                pk: None,
                dk: None,
            })
        }
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
#[path = "derive_resolve_candidate_tests.rs"]
mod resolve_candidate_tests;

// Device-key POSITION recovery and the MKB probe accessors. No published AACS test vectors
// exist; the relations (`[C]` §3.2.3-§3.2.5) are invertible, so `plant_mkb` below builds a
// valid MKB for a CHOSEN key.
#[cfg(test)]
#[path = "derive_position_recovery_tests.rs"]
mod position_recovery_tests;

#[cfg(test)]
#[path = "derive_spec_guards_tests.rs"]
mod spec_guards;

//! Media-key derivation: DK/PK → Media Key via the subset-difference tree.
//! [C] §3.2.2–§3.2.5.

use super::crypto::*;
use super::inf::*;
use super::mkb::*;
use super::types::*;

/// Derive Media Key from MKB data using processing keys.
///
/// A Processing Key is **terminal**: it is the key at its Subset-Difference
/// node, one `AES-G` from the Media Key. So this is the fast path — each PK is
/// tried *directly* against the MKB cvalue tables (no tree descent) — the
/// direct PK × cvalue iteration. On a large AACS 2.x UHD MKB
/// (~181k cvalues) this is ~15x faster than treating a PK as a device-node
/// label and walking the tree.
///
/// If you hold a **device-node label** at unknown tree depth (not a terminal
/// PK), derive its Media Key through the device-key path
/// ([`derive_media_key_from_dk`]) — that path owns the Subset-Difference tree
/// walk; the PK path never descends.
///
/// MKB format:
///   Record type 0x10 = Type and Version Record (has MKB version)
///   Record type 0x81 = Verify Media Key Record, AACS 1.0 (has mk_dv)
///   Record type 0x86 = Verify Media Key Record, AACS 2.0/2.1 (has mk_dv)
///   Record type 0x04 = Subset-Difference Index (has UVS entries)
///   Record type 0x05 = Media Key Data Record (cvalues, 1:1 with 0x04)
///   Record type 0x07 = Explicit Subset-Difference Record (NOT cvalues)
pub fn derive_media_key_from_pk(mkb: &[u8], processing_keys: &[[u8; 16]]) -> Option<[u8; 16]> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;
    try_pk_against_tables(processing_keys, &uvs, &cvalues, &mk_dv)
}

/// Core terminal-PK table scan over explicit record bodies. Each processing
/// key is tried **directly** against every `(uv, cvalue)` pair — no tree
/// descent. Reached in production via [`derive_media_key_from_pk`]; factored
/// out so reproduction harnesses can drive it with explicit tables.
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

/// Validate a processing key against a cvalue/UV pair.
/// Returns the Media Key if valid.
///
/// Steps (media key: [C] §3.2.4; verify relation: [C] §3.2.5.1.4):
///   1. `mk = AES-128D(pk, cvalue)`                       [C] §3.2.4
///   2. `mk[12..16] ^= uv` (4 bytes XOR into the last 4 bytes only)  [C] §3.2.4
///   3. `dec_vd = AES-128D(mk, mk_dv)`                     [C] §3.2.5.1.4
///   4. If `dec_vd[0..8] == 01 23 45 67 89 AB CD EF` → valid.  [C] §3.2.5.1.4
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

/// Compute v_mask from a UV value. [C] §3.2.3. Shared with [`super::variant`].
pub(super) fn calc_v_mask(uv: u32) -> u32 {
    let mut v_mask: u32 = 0xFFFF_FFFF;
    while (uv & !v_mask) == 0 && v_mask != 0 {
        v_mask <<= 1;
    }
    v_mask
}

/// Derive processing key from device key using subset-difference tree traversal.
/// [C] §3.2.4 (device-tree descent, MSB-branch, terminal PK). Shared with [`super::variant`].
pub(super) fn calc_pk_from_dk(
    dk: &[u8; 16],
    uv: u32,
    v_mask: u32,
    dev_key_v_mask: u32,
) -> [u8; 16] {
    // Descend from the device node to the record node, following the record's
    // `uv` bits. At each level only the child we descend INTO is needed (the
    // sibling is computed but never used), and the Processing Key is the
    // `aesg3(.,1)` of the FINAL node — so we derive ONE child per level and the
    // PK once at the end, instead of left/pk/right at every level. Identical
    // result, ~3x fewer block ops. (left child = `aesg3(node,0)`, right = `,2`.)
    let mut node = *dk;
    let mut current_v_mask = dev_key_v_mask;

    // The subset-difference tree is at most 32 levels deep (u32 mask), so the
    // walk must converge in <= 32 steps. The arithmetic `>> 1` sign-extends
    // current_v_mask, so a v_mask coarser than dev_key_v_mask (reachable from
    // a crafted/corrupt MKB) would otherwise saturate at 0xFFFF_FFFF and spin
    // forever — bound the loop to keep a bad disc from hanging the rip thread.
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

            // `num_uvs` was computed via `take_while(.. c[0] & 0xC0 == 0)`, so
            // every iterated slot already has its revoked-marker bits clear — no
            // inner `& 0xC0` re-check is needed (it would be unreachable).
            //
            // Shifts of 32..=63 (0x20..=0x3F) have those bits clear but would
            // panic in debug / wrap to a wrong mask in release. The MKB byte is
            // disc-controlled, so a crafted/corrupt MKB must not crash the ripper:
            // skip an out-of-range slot rather than `<<` it.
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
/// 16 bytes) cannot be walked — the walk needs its tree node. This finds that
/// node empirically: for each MKB subset-difference record, it tries the device
/// at the record's node AND at every ancestor v-position (the device may sit one
/// or more levels ABOVE the record, descending via AES-G to reach it), deriving
/// the candidate Processing Key DIRECTLY (one [`calc_pk_from_dk`] per candidate,
/// no full re-walk) and checking it validates against that record's cvalue.
///
/// On the first verifying candidate it pins `(uv, u_mask_shift)` — invariant for
/// the key across all discs — and resolves a gate-passing `node` (a one-time
/// ≤32-try search at the single hit). Returns a [`DeviceKey`] ready to bank and
/// reuse on every future disc via [`derive_media_key_from_dk`]. `None` if the
/// key does not apply to this MKB.
///
/// Cost is `O(slots × tree_depth)` — linear in the MKB's subset-difference
/// index, not the quartic cost of re-deriving per candidate.
pub fn recover_dk_position(mkb: &[u8], key: &[u8; 16]) -> Option<DeviceKey> {
    let mk_dv = mkb_find_mk_dv(mkb)?;
    let uvs = mkb_find_subdiff_records(mkb)?;
    let cvalues = mkb_find_cvalues(mkb)?;
    let num_uvs = uvs
        .chunks(5)
        .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
        .count();
    let n_cv = cvalues.len() / 16;

    // Hoisted ONCE for the whole scan: the Processing Key the device produces if
    // it sits EXACTLY at a record (zero descent) is `AES-G3(key, 1)` — it does
    // not depend on the record, so the zero-descent probe of every slot reuses
    // this single value instead of re-deriving it per slot.
    let pk_zero_descent = aesg3(key, 1);

    // The slots are independent, so the scan parallelises — a UHD MKB has ~181k
    // slots (~26s single-threaded). `find_map_any` returns the first matching
    // node found by any thread and cancels the rest; a valid MKB has exactly one
    // matching subset-difference, so which thread finds it is immaterial.
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

/// Resolve a positioned [`DeviceKey`] for an orphan `key` known to sit at
/// `(uv, u_mask_shift)`: find a `device_number` (node) that passes the walk's
/// subset-difference gate on `mkb`. The derived key is independent of the exact
/// node (it only gates), so any gating node yields the same Media Key — a
/// one-time ≤32-try search, run only once at the recovered position.
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

/// Derive VUK from Media Key and Volume ID. [PR] §3.3 / [BD] §3.3
/// (`Kvu = AES-G(Km, IDv)`; AES-G uses AES-128D):
/// VUK = AES-128-ECB-DECRYPT(media_key, volume_id) XOR volume_id
pub fn derive_vuk(media_key: &[u8; 16], volume_id: &[u8; 16]) -> [u8; 16] {
    let mut vuk = aes_ecb_decrypt(media_key, volume_id);
    for i in 0..16 {
        vuk[i] ^= volume_id[i];
    }
    vuk
}

/// Decrypt an encrypted unit key using the VUK (AES-128-ECB). [PR] §3.5
/// (Title Key unwrap `Kt = AES-128D(Ku, Kte)`); the BD "CPS Unit Key" synonym is [BD] §3.9.3.
pub fn decrypt_unit_key(vuk: &[u8; 16], encrypted_uk: &[u8; 16]) -> [u8; 16] {
    aes_ecb_decrypt(vuk, encrypted_uk)
}

/// Decrypt every encrypted unit key in a parsed `Unit_Key_RO.inf` with a VUK,
/// paired with its declared CPS-unit number. THE single VUK→unit-keys step:
/// both classical/v21 resolvers and [`resolve_candidate`] call this, so the
/// map cannot drift between the player and harvest paths.
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
/// `VUK → UKs`, or `UK → itself`. Composes the raw derivation primitives
/// ([`derive_media_key_from_pk`], [`derive_media_key_and_pk_from_dk`],
/// [`derive_vuk`], [`derive_unit_keys`]) and parses `Unit_Key_RO.inf` at the
/// version the disc's MKB declares, so a multi-CPS disc yields all its unit
/// keys from the one candidate.
///
/// PURE DERIVATION: no sampling, no validation, no position recovery. Validate
/// `unit_keys` against a real encrypted unit with
/// `decrypt_unit` + `is_clean_ts` to prove the candidate opens the disc.
///
/// Returns `None` only when derivation itself cannot proceed: a PK its MKB
/// rejects, a `Dk` the MKB can't process, a missing VID on a path that needs
/// one, or an unparseable/empty `Unit_Key_RO.inf`.
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

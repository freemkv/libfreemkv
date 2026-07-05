//! AACS key resolution — VUK derivation, MKB processing, disc hash, unit key parsing.

use super::crypto::aes_ecb_decrypt;
use super::derive::*;
use super::inf::*;
use super::mkb::*;

//
// Canonical form is `<category>1003` (low 16 bits `0x1003` is a fixed marker).
// Types 3/4/10 are from the AACS Common Cryptographic Elements spec (0.953,
// §3.2.5.1.1); the Category-C 2.0/2.1 values match libaacs `mkb.h` constants.

// ── Full VUK resolution chain ───────────────────────────────────────────────

/// Result of resolving a disc's VUK.
#[derive(Debug)]
pub struct ResolvedKeys {
    /// Disc hash (SHA1 of Unit_Key_RO.inf)
    pub disc_hash: [u8; 20],
    /// Volume Unique Key. `None` for path 5 — the KEYDB unit-keys
    /// fallback consumes pre-decrypted unit keys directly and has no
    /// VUK to surface.
    pub vuk: Option<[u8; 16]>,
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Title → CPS unit index mapping
    pub title_cps_unit: Vec<u16>,
    /// AACS generation that drove the resolution
    pub version: AacsVersion,
    /// Whether bus encryption is enabled (from Content Certificate)
    pub bus_encryption: bool,
    /// Which resolution path succeeded (1=DK, 2=PK, 3=KEYDB derived,
    /// 4=KEYDB VUK, 5=KEYDB unit keys)
    pub key_source: u8,
}

/// Inputs shared by every classical-path resolver. References only —
/// callers retain ownership of all buffers.
pub struct ResolveContext<'a> {
    /// `Unit_Key_RO.inf` raw bytes.
    pub unit_key_ro: &'a [u8],
    /// Content Certificate raw bytes (optional — used for bus-encryption flag).
    pub content_cert: Option<&'a [u8]>,
    /// 16-byte Volume ID from SCSI handshake. `[0u8; 16]` is the
    /// "no VID" sentinel and disables paths 1-3.
    pub volume_id: &'a [u8; 16],
    /// Key sources — checked in array order for disc-keyed lookups,
    /// union'd across all entries for bulk material (DKs, PKs, HCs).
    /// A keydb file, a webservice, an OEM provider can all coexist.
    pub providers: &'a [&'a dyn super::provider::KeyProvider],
    /// MKB raw bytes (optional — paths 1/2 require it).
    pub mkb: Option<&'a [u8]>,
}

/// Why a key resolution attempt produced no usable key.
///
/// Distinguishes the two no-key outcomes that an application must report
/// differently:
///   * [`ResolveFailure::VidUnavailable`] — the key source DID provide
///     derivation material (device or processing keys), but no Volume ID
///     (VID) was available to derive the Volume Unique Key. The fix is to
///     recover the VID (a drive / handshake problem), not to add keys.
///   * [`ResolveFailure::NoMaterial`] — no usable key material was found at
///     all (no DK/PK material, no disc-keyed hit). The fix is to add keys.
///
/// This carries no key bytes and is independent of the decryption math; it
/// is purely the *reason* a resolution returned no key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveFailure {
    /// Derivation material was present (DKs or PKs) but no VID was available
    /// to derive the unit key. Surfaced as [`crate::error::Error::AacsVidUnavailable`].
    VidUnavailable,
    /// No usable key material at all. Surfaced as
    /// [`crate::error::Error::NoDiscKey`].
    NoMaterial,
}

/// Version-dispatched resolution that preserves the *reason* on failure.
///
/// Identical key derivation to the [`resolve_keys_v1`] / [`resolve_keys_v2`] /
/// [`resolve_keys_v21`] chain (it calls straight through to them); the only
/// addition is that an unresolved disc returns a typed [`ResolveFailure`]
/// instead of a bare `None`, so callers can report E7017 (material but no VID)
/// vs E7022 (no material). `version_u8` is the on-disc AACS major (1 → V10,
/// anything else → the V20/V21 chain), matching `AacsState::version`.
pub fn resolve_keys_with_reason(
    ctx: &ResolveContext<'_>,
    version_u8: u8,
) -> std::result::Result<ResolvedKeys, ResolveFailure> {
    let resolved = match version_u8 {
        1 => resolve_keys_v1(ctx),
        _ => resolve_keys_v2(ctx).or_else(|| resolve_keys_v21(ctx)),
    };
    match resolved {
        Some(r) => Ok(r),
        None => Err(classify_resolve_failure(ctx)),
    }
}

/// Classify why resolution found no key. The key source provided derivation
/// material (device or processing keys) but the VID sentinel is all-zero →
/// [`ResolveFailure::VidUnavailable`]; otherwise → [`ResolveFailure::NoMaterial`].
///
/// Reads only what the resolver already had (provider material + the VID
/// sentinel) — no key derivation, no descramble.
pub(crate) fn classify_resolve_failure(ctx: &ResolveContext<'_>) -> ResolveFailure {
    let has_vid = *ctx.volume_id != [0u8; 16];
    let providers = super::provider::Providers(ctx.providers);
    let has_derivation_material =
        !providers.device_keys().is_empty() || !providers.processing_keys().is_empty();
    if !has_vid && has_derivation_material {
        ResolveFailure::VidUnavailable
    } else {
        ResolveFailure::NoMaterial
    }
}

/// AACS 1.0 key resolution. Parses `Unit_Key_RO.inf` with 48-byte
/// stride. Tries paths 1 → 4 in order.
pub fn resolve_keys_v1(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    resolve_keys_classical(ctx, AacsVersion::V10)
}

/// AACS 2.0 key resolution. Parses `Unit_Key_RO.inf` with 64-byte
/// stride. Tries paths 1 → 4 in order. When paths 3/4 succeed against
/// an MKB carrying Variant records (`0x2d` / `0x2f`), the result's
/// `version` is upgraded to [`AacsVersion::V21`] — derivation still
/// runs through the classical V2 path; the V21-specific Variant chain
/// is wired separately via [`resolve_keys_v21`].
pub fn resolve_keys_v2(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    let mut resolved = resolve_keys_classical(ctx, AacsVersion::V20)?;
    if let Some(mkb) = ctx.mkb {
        let recs = super::mkb::walk_mkb(mkb);
        if super::variant::is_variant_mkb(&recs) {
            resolved.version = AacsVersion::V21;
        }
    }
    Some(resolved)
}

/// AACS 2.1 key resolution via the Media Key Variant chain.
///
/// Paths run in root-of-trust → per-disc-leaf order:
///   1. Variant chain: device keys → PK → Km → Kvu (needs a covering 2.1
///      Processing Key; misses cleanly when the device-key pool covers no slot)
///   3. KEYDB MK + matching VID → derived VUK (V21 discs already in
///      the keydb decrypt identically to V20)
///   4. KEYDB disc-hash → VUK
///   5. KEYDB disc-hash → pre-decrypted unit keys (no VUK)
///
/// (Numbering preserves the cross-resolver convention; AACS 2.1 has no
/// equivalent of path 2 — there's no host-side PK derivation against a
/// Variant MKB.)
pub fn resolve_keys_v21(ctx: &ResolveContext<'_>) -> Option<ResolvedKeys> {
    let uk_file = parse_unit_key_ro(ctx.unit_key_ro, AacsVersion::V20)?;
    let hash_hex = disc_hash_hex(&uk_file.disc_hash);
    let bus_encryption = ctx
        .content_cert
        .and_then(parse_content_cert)
        .map(|cc| cc.bus_encryption)
        .unwrap_or(false);
    let has_vid = *ctx.volume_id != [0u8; 16];

    let derive_uks = |vuk: &[u8; 16]| derive_unit_keys(&uk_file, vuk);

    let build =
        |vuk: Option<[u8; 16]>, unit_keys: Vec<(u32, [u8; 16])>, key_source: u8| -> ResolvedKeys {
            ResolvedKeys {
                disc_hash: uk_file.disc_hash,
                vuk,
                unit_keys,
                title_cps_unit: uk_file.title_cps_unit.clone(),
                version: AacsVersion::V21,
                bus_encryption,
                key_source,
            }
        };

    tracing::info!(
        target: "freemkv::disc",
        phase = "resolve_keys_v21_start",
        bus_encryption,
        disc_hash = %hash_hex,
        has_vid,
        mkb_present = ctx.mkb.is_some(),
        "resolve_keys_v21: starting"
    );

    let providers = super::provider::Providers(ctx.providers);

    if has_vid {
        // Path 1: Variant chain (V21's analogue of classical Path 1's DK
        // derivation). Derive the Processing Key from device keys first (DK → PK
        // via the variant walk), then run the PK → Km variant primitive and
        // derive the per-disc VUK from Km + VID.
        if let Some(mkb) = ctx.mkb {
            let recs = super::mkb::walk_mkb(mkb);
            let all_dks = providers.device_keys();
            if let Some(pkm) = super::variant::walk_processing_key(&recs, &all_dks) {
                match super::variant::derive_media_key_variant(&recs, &pkm.kp) {
                    Ok(km) => {
                        let kvu = derive_vuk(&km, ctx.volume_id);
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "resolve_keys_v21_path1_hit",
                            "Variant chain produced Km + Kvu"
                        );
                        return Some(build(Some(kvu), derive_uks(&kvu), 1));
                    }
                    Err(e) => {
                        tracing::debug!(
                            target: "freemkv::disc",
                            phase = "resolve_keys_v21_path1_miss",
                            error_code = %e,
                            "Variant chain failed"
                        );
                    }
                }
            }
        }

        // Path 3: pre-computed MK + matching VID → derived VUK.
        // Short-circuit: first provider with a matching VID wins.
        if let Some(entry) = providers.lookup_disc_by_vid(ctx.volume_id) {
            // The entry already matched by VID and derive_vuk needs only mk +
            // ctx.volume_id, so a provider that matches by VID without
            // populating disc_id (e.g. a webservice) must not have its MK
            // dropped — gate on the MK alone.
            if let Some(mk) = entry.media_key {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_v21_path3_hit", "MK+VID entry matched volume_id");
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
        }
    } else {
        tracing::debug!(
            target: "freemkv::disc",
            phase = "resolve_keys_v21_no_vid",
            "VID unavailable; paths 1/3 skipped"
        );
    }

    // Paths 4 and 5: hash lookup, prefer V over U on the same entry.
    if let Some(entry) = providers.lookup_disc_by_hash(&uk_file.disc_hash) {
        if let Some(vuk) = entry.vuk {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_v21_path4_hit", "VUK from KEYDB");
            return Some(build(Some(vuk), derive_uks(&vuk), 4));
        } else if let Some(unit_keys) = match_keydb_unit_keys(&uk_file, &entry.unit_keys) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_v21_path5_hit",
                uk_count = unit_keys.len(),
                "unit keys from KEYDB (no VUK)"
            );
            return Some(build(None, unit_keys, 5));
        }
    }

    None
}

/// Resolve all AACS keys for a disc using the classical (single-stage
/// Media Key derivation) paths. Used by both V10 and V20.
///
/// Paths run in root-of-trust → per-disc-leaf order. A match at any
/// path returns immediately:
///   1. MKB + device keys → processing key → media key → VUK
///   2. MKB + processing keys → media key → VUK
///   3. KEYDB MK + matching VID → derived VUK
///   4. KEYDB disc-hash → VUK
///   5. KEYDB disc-hash → pre-decrypted unit keys (no VUK)
fn resolve_keys_classical(ctx: &ResolveContext<'_>, version: AacsVersion) -> Option<ResolvedKeys> {
    let bus_encryption = ctx
        .content_cert
        .and_then(parse_content_cert)
        .map(|cc| cc.bus_encryption)
        .unwrap_or(false);

    // Parse Unit_Key_RO.inf at the version-appropriate stride.
    let uk_file = parse_unit_key_ro(ctx.unit_key_ro, version)?;

    let hash_hex = disc_hash_hex(&uk_file.disc_hash);
    let has_vid = *ctx.volume_id != [0u8; 16];

    // Decrypt the disc's encrypted unit keys with a freshly-derived VUK.
    let derive_uks = |vuk: &[u8; 16]| derive_unit_keys(&uk_file, vuk);

    // Common result constructor — paths 1-4 supply Some(VUK) + derived
    // unit keys; path 5 supplies None + pre-decrypted unit keys from
    // KEYDB.
    let build =
        |vuk: Option<[u8; 16]>, unit_keys: Vec<(u32, [u8; 16])>, key_source: u8| -> ResolvedKeys {
            ResolvedKeys {
                disc_hash: uk_file.disc_hash,
                vuk,
                unit_keys,
                title_cps_unit: uk_file.title_cps_unit.clone(),
                version,
                bus_encryption,
                key_source,
            }
        };

    tracing::info!(
        target: "freemkv::disc",
        phase = "resolve_keys_start",
        version = ?version,
        bus_encryption,
        disc_hash = %hash_hex,
        has_vid,
        mkb_present = ctx.mkb.is_some(),
        "resolve_keys: starting"
    );

    let providers = super::provider::Providers(ctx.providers);

    // Paths 1 and 2 need both MKB and VID. Logged as a single skip when
    // either is absent so operators see one reason, not two.
    if has_vid {
        if let Some(mkb) = ctx.mkb {
            let mk_dv = mkb_find_mk_dv(mkb);
            let subdiff = mkb_find_subdiff_records(mkb);
            let cvalues = mkb_find_cvalues(mkb);
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_mkb_records",
                mk_dv_found = mk_dv.is_some(),
                subdiff_found = subdiff.is_some(),
                subdiff_len = subdiff.as_ref().map(|s| s.len()).unwrap_or(0),
                cvalues_found = cvalues.is_some(),
                cvalues_len = cvalues.as_ref().map(|c| c.len()).unwrap_or(0),
                "MKB record scan results"
            );

            // Path 1: MKB + device keys → media key → VUK
            let all_dks = providers.device_keys();
            if let Some(mk) = derive_media_key_from_dk(mkb, &all_dks) {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path1_hit", "media key derived from device key");
                return Some(build(Some(vuk), derive_uks(&vuk), 1));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path1_miss", dk_count = all_dks.len(), "DK derivation failed");

            // Path 2: MKB + processing keys → media key → VUK
            let all_pks = providers.processing_keys();
            if let Some(mk) = derive_media_key_from_pk(mkb, &all_pks) {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_hit", "media key derived from processing key");
                return Some(build(Some(vuk), derive_uks(&vuk), 2));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_miss", pk_count = all_pks.len(), "PK derivation failed");

            // Path 2.5: MK-pool brute. keydb stores Media Keys per-disc, but an
            // MK is MKB-scoped (shared across a pressing/MKB-family). A disc
            // whose own hash/VID isn't keyed can still resolve if ANY stored MK
            // verifies against its MKB. Try every distinct MK via km_verifies;
            // a UNIQUE pass is this disc's Km → derive VUK (needs VID) → UK.
            // One AES-D + magic check per candidate (cheap). mk_dv is hoisted
            // out of the loop so the MKB is not re-walked per candidate.
            let mks = providers.media_keys();
            let mut mk_hits: Vec<[u8; 16]> = Vec::new();
            if let Some(mk_dv) = mkb_find_mk_dv(mkb) {
                for mk in &mks {
                    let verifies = aes_ecb_decrypt(mk, &mk_dv)[..8]
                        == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
                    if verifies && !mk_hits.contains(mk) {
                        mk_hits.push(*mk);
                        if mk_hits.len() > 1 {
                            break; // ambiguous — bail to avoid a wrong key
                        }
                    }
                }
            }
            if mk_hits.len() == 1 {
                let vuk = derive_vuk(&mk_hits[0], ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_5_hit", mk_pool = mks.len(), "media key from keydb MK-pool brute (km_verifies)");
                // Same class as path 3 (KEYDB MK → derived VUK).
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path2_5_miss", mk_pool = mks.len(), mk_hits = mk_hits.len(), "MK-pool brute: no unique verifying MK");
        } else {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_no_mkb", "no MKB; paths 1/2 skipped");
        }

        // Path 3: pre-computed MK + matching VID → derived VUK.
        // Short-circuit: first provider with a matching VID wins.
        if let Some(entry) = providers.lookup_disc_by_vid(ctx.volume_id) {
            // The entry already matched by VID and derive_vuk needs only mk +
            // ctx.volume_id, so a provider that matches by VID without
            // populating disc_id (e.g. a webservice) must not have its MK
            // dropped — gate on the MK alone.
            if let Some(mk) = entry.media_key {
                let vuk = derive_vuk(&mk, ctx.volume_id);
                tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path3_hit", "MK+VID entry matched volume_id");
                return Some(build(Some(vuk), derive_uks(&vuk), 3));
            }
        }
        tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path3_miss", "no MK+VID entry matched volume_id");
    } else {
        tracing::debug!(
            target: "freemkv::disc",
            phase = "resolve_keys_no_vid",
            "VID unavailable; paths 1/2/3 require VID and are skipped"
        );
    }

    // Paths 4 and 5: single hash-keyed lookup, prefer V (path 4) over
    // U (path 5). They are not independent checks — path 5 only fires
    // because path 4 had no VUK on the same entry.
    if let Some(entry) = providers.lookup_disc_by_hash(&uk_file.disc_hash) {
        tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_keydb_hit_entry", "disc hash found in provider");
        if let Some(vuk) = entry.vuk {
            tracing::debug!(target: "freemkv::disc", phase = "resolve_keys_path4_hit", "VUK from provider");
            return Some(build(Some(vuk), derive_uks(&vuk), 4));
        } else if let Some(unit_keys) = match_keydb_unit_keys(&uk_file, &entry.unit_keys) {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "resolve_keys_path5_hit",
                uk_count = unit_keys.len(),
                "unit keys from provider (no VUK)"
            );
            return Some(build(None, unit_keys, 5));
        }
        tracing::warn!(target: "freemkv::disc", phase = "resolve_keys_keydb_no_keys", "provider entry has neither VUK nor matching unit keys");
    } else {
        tracing::warn!(target: "freemkv::disc", phase = "resolve_keys_keydb_miss", "disc hash NOT found in any provider");
    }

    None
}

/// For path 5: cross-reference the disc's `Unit_Key_RO.inf` CPS-unit
/// numbering against the KEYDB entry's pre-decrypted unit keys. Every
/// CPS unit the disc declares must have a matching entry in KEYDB;
/// partial coverage returns `None` so the resolver doesn't half-decrypt
/// a disc.
pub(crate) fn match_keydb_unit_keys(
    uk_file: &UnitKeyFile,
    keydb_unit_keys: &[(u32, [u8; 16])],
) -> Option<Vec<(u32, [u8; 16])>> {
    if keydb_unit_keys.is_empty() {
        return None;
    }
    let mut matched = Vec::with_capacity(uk_file.encrypted_keys.len());
    for (disc_num, _enc_key) in &uk_file.encrypted_keys {
        let entry = keydb_unit_keys.iter().find(|(n, _)| n == disc_num)?;
        matched.push(*entry);
    }
    Some(matched)
}

#[cfg(test)]
mod tests {
    // This suite predates the module split; it white-box-tests items now living
    // in sibling modules. Pull them all in so the tests keep exercising them.
    use super::super::crypto::*;
    use super::super::derive::*;
    use super::super::inf::*;
    use super::super::mkb::*;
    use super::super::provider::SuppliedKey;
    use super::super::types::DiscEntry;
    use super::super::types::*;
    use super::*;

    /// Audit #5: the `major` / `from_major` mapping is load-bearing for the
    /// Unit_Key_RO stride, so pin it as a table. V10 ↔ BD; V20/V21 → UHD; any
    /// non-BD major selects the V20/V21 64-byte stride (V10 is the only 48-byte).
    #[test]
    fn aacs_major_round_trips_and_strides_differ() {
        assert_eq!(AacsVersion::V10.major(), AACS_MAJOR_BD);
        assert_eq!(AacsVersion::V20.major(), AACS_MAJOR_UHD);
        assert_eq!(AacsVersion::V21.major(), AACS_MAJOR_UHD);
        assert_eq!(AacsVersion::from_major(AACS_MAJOR_BD), AacsVersion::V10);
        assert_eq!(AacsVersion::from_major(AACS_MAJOR_UHD), AacsVersion::V20);
        assert_eq!(AacsVersion::from_major(99), AacsVersion::V20); // any non-BD → V20
        assert_ne!(
            AacsVersion::from_major(AACS_MAJOR_BD).unit_key_stride(),
            AacsVersion::from_major(AACS_MAJOR_UHD).unit_key_stride()
        );
    }

    /// Finding #5 regression: parse_unit_key_ro must REJECT a Unit_Key_RO.inf
    /// whose declared `num_unit_keys` exceeds the keys actually present in the
    /// buffer, instead of silently returning a short list. A truncated list
    /// would later map title CPS units to nonexistent keys.
    #[test]
    fn parse_unit_key_ro_rejects_truncated_key_list() {
        // V10 layout: stride 48, keys start at uk_pos + 48.
        // uk_pos = 32; num_uk = 2; keys at 80 and 128.
        let uk_pos = 32usize;
        let build = |total_len: usize| -> Vec<u8> {
            let mut data = vec![0u8; total_len];
            // uk_pos as BE32 at [0..4].
            data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
            // num_unit_keys = 2 (BE16) at uk_pos.
            data[uk_pos] = 0x00;
            data[uk_pos + 1] = 0x02;
            data
        };

        // Full buffer: room for both keys (keys_start 80, key1 at 128..144).
        let full = build(144);
        let ok =
            parse_unit_key_ro(&full, AacsVersion::V10).expect("a full 2-key buffer must parse");
        assert_eq!(ok.encrypted_keys.len(), 2);

        // Truncated buffer: header still declares 2 keys, but only the first
        // fits (len 128 — the second key's 16 bytes run off the end). Must be
        // rejected, not silently accepted with one key.
        let short = build(128);
        assert!(
            parse_unit_key_ro(&short, AacsVersion::V10).is_none(),
            "a buffer declaring more keys than it contains must be rejected"
        );
    }

    #[test]
    fn derive_media_key_from_dk_survives_out_of_range_u_mask_shift() {
        // Regression: a crafted/corrupt MKB with a Subset-Difference
        // u_mask_shift of 32..=63 (passes the 0xC0 revoked-marker check but
        // overflows `0xFFFF_FFFF << shift`) used to panic in debug / compute a
        // wrong mask in release. The walk must now skip the bad slot and
        // return cleanly (no panic) on disc-controlled bytes.
        let mut mkb: Vec<u8> = Vec::new();
        // 0x81 record: 4-byte header + 16-byte mk_dv body (rec_len = 20).
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        // 0x04 Subset-Difference: one 5-byte entry with u_mask_shift = 0x30
        // (48 — out of range, but 0x30 & 0xC0 == 0 so the revoke check passes).
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x30, 0x00, 0x00, 0x00, 0x01]);
        // 0x05 cvalues: one 16-byte entry (rec_len = 20).
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);

        let dk = DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0x30, // also out of range on the device-key side
        };

        // Must not panic; no valid derivation is expected from this junk.
        let _ = derive_media_key_from_dk(&mkb, &[dk]);
    }

    #[test]
    fn test_decrypt_unit_key_from_vuk() {
        // VUK → encrypted unit key → unit key roundtrip. The keydb-sourced
        // variant of this test (which scanned a real KEYDB for VUK + unit
        // keys) moved to freemkv-keysources; this rebuilt version exercises
        // the same AES-G primitive (decrypt_unit_key ∘ aes_ecb_encrypt under a
        // VUK) with directly-constructed material, so it needs no parser and
        // keeps the crypto covered in libfreemkv. `aes_ecb_encrypt` is
        // pub(crate), reachable here but not from keysources — the reason this
        // half stays.
        use super::super::crypto::aes_ecb_encrypt;
        let vuk = [0x5Au8; 16];
        // A few representative "decrypted" unit keys.
        for expected_uk in [[0x11u8; 16], [0x22u8; 16], [0xCDu8; 16]] {
            let encrypted = aes_ecb_encrypt(&vuk, &expected_uk);
            let decrypted = decrypt_unit_key(&vuk, &encrypted);
            assert_eq!(decrypted, expected_uk, "unit key roundtrip under VUK");
        }
    }

    #[test]
    fn test_disc_hash() {
        // SHA1 of a known byte sequence
        let data = b"test unit key ro inf data";
        let hash = disc_hash(data);
        assert_ne!(hash, [0u8; 20]);
        // Same input → same hash
        assert_eq!(hash, disc_hash(data));
    }

    #[test]
    fn test_disc_hash_hex() {
        let hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13,
        ];
        let hex = disc_hash_hex(&hash);
        assert_eq!(hex, "0x000102030405060708090A0B0C0D0E0F10111213");
    }

    #[test]
    fn test_parse_unit_key_ro_synthetic() {
        // Build a synthetic Unit_Key_RO.inf
        // Header: uk_pos at offset 0 (BE32), points to key storage
        // Keys at uk_pos + 48 (16 bytes each, 48-byte stride for AACS 1.0)
        let mut data = vec![0u8; 256];

        // uk_pos = 0x60 (96)
        data[0] = 0x00;
        data[1] = 0x00;
        data[2] = 0x00;
        data[3] = 0x60;

        // Header fields at 16-18
        data[16] = 1; // app_type = BD-ROM
        data[17] = 1; // num_bdmv_dir
        data[18] = 0; // no SKB

        // Title mapping at 20-25
        data[20] = 0;
        data[21] = 1; // first_play = CPS unit 1
        data[22] = 0;
        data[23] = 1; // top_menu = CPS unit 1
        data[24] = 0;
        data[25] = 1; // num_titles = 1
        // Title 0 entry: 2 bytes pad + CPS unit
        data[28] = 0;
        data[29] = 1; // CPS unit 1

        // Key storage at offset 0x60
        let uk_pos = 0x60usize;
        data[uk_pos] = 0;
        data[uk_pos + 1] = 2; // 2 unit keys

        // Key 1 at uk_pos + 48
        let key1_pos = uk_pos + 48;
        for i in 0..16 {
            data[key1_pos + i] = 0xAA;
        }

        // Key 2 at uk_pos + 48 + 48
        let key2_pos = key1_pos + 48;
        for i in 0..16 {
            data[key2_pos + i] = 0xBB;
        }

        let parsed = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert_eq!(parsed.app_type, 1);
        assert_eq!(parsed.num_bdmv_dir, 1);
        assert_eq!(parsed.version, AacsVersion::V10);
        assert_eq!(parsed.encrypted_keys.len(), 2);
        assert_eq!(parsed.encrypted_keys[0].0, 1); // CPS unit 1
        assert_eq!(parsed.encrypted_keys[0].1, [0xAA; 16]);
        assert_eq!(parsed.encrypted_keys[1].0, 2); // CPS unit 2
        assert_eq!(parsed.encrypted_keys[1].1, [0xBB; 16]);
    }

    #[test]
    fn mkb_version_recognizes_type_0x10() {
        // Type-and-Version record: type=0x10, rec_len=12 (BE24).
        // Body is 8 bytes; the version u32 sits at offset 8 of the record.
        let mkb = [
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        assert_eq!(mkb_version(&mkb), Some(77));
    }

    #[test]
    fn mkb_content_len_trims_trailing_padding() {
        // Two real records (0x10 type/version + 0x86 verify), then 128 KiB of
        // zero padding (the fixed-region tail). Content length must stop at the
        // end of the records, not include the padding.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        let records_len = mkb.len();
        mkb.extend(std::iter::repeat_n(0u8, 128 * 1024)); // padding
        assert_eq!(mkb_content_len(&mkb), records_len);
        // No padding → returns the full length.
        assert_eq!(mkb_content_len(&mkb[..records_len]), records_len);
        // Empty → 0.
        assert_eq!(mkb_content_len(&[]), 0);
    }

    #[test]
    fn trim_mkb_never_zeroes_an_unrecognised_mkb() {
        // Regression: the 0.31.0 read_aacs_inputs path truncated the MKB to
        // mkb_content_len() unconditionally. For an MKB whose first record the
        // parser can't read, mkb_content_len() returns 0 → an unconditional
        // truncate zeroed the MKB, so autorip sent an EMPTY MKB to the key
        // service (or skipped the request). trim_mkb must leave it intact.
        let unrecognised = vec![0xFFu8; 4096]; // first "rec_type" 0xFF, rec_len huge → content_len 0
        assert_eq!(
            mkb_content_len(&unrecognised),
            0,
            "precondition: unparseable → 0"
        );
        assert_eq!(
            trim_mkb(unrecognised.clone()),
            unrecognised,
            "unrecognised MKB must be returned untouched, never zeroed"
        );

        // A parseable MKB with trailing padding IS trimmed to its records.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&[0xAB; 16]);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        let records_len = mkb.len();
        mkb.extend(std::iter::repeat_n(0u8, 1024));
        assert_eq!(
            trim_mkb(mkb).len(),
            records_len,
            "padded MKB trims to records"
        );

        // Empty stays empty (n==0 → untouched).
        assert!(trim_mkb(Vec::new()).is_empty());
    }

    #[test]
    fn mkb_version_returns_none_on_empty() {
        assert_eq!(mkb_version(&[]), None);
        assert_eq!(mkb_version(&[0x10, 0x00]), None);
        // Type 0x10 record but rec_len < 12 → no version available.
        let short = [0x10, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(mkb_version(&short), None);
    }

    #[test]
    fn mkb_find_mk_dv_recognizes_type_0x81() {
        // First: type-0x10 type/version record (12 bytes), then type-0x81 verify record.
        // Verify record carries a known 16-byte mk_dv at offset 4 of the record body.
        let expected: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];
        // type=0x81, rec_len=24 (4-byte header + 16-byte mk_dv + 4-byte trailing zeros)
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&expected);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        assert_eq!(mkb_find_mk_dv(&mkb), Some(expected));
    }

    #[test]
    fn probe_try_pk_against_tables_accepts_planted_pk_rejects_corrupt() {
        // Lock in the terminal PK scan (`try_pk_against_tables`) used by the
        // production PK path (`derive_media_key_from_pk`). Plant a terminal PK
        // whose derived Media Key satisfies a synthetic verify record; confirm
        // the scan ACCEPTS it against caller-supplied SD/cvalue tables and
        // REJECTS a 1-byte corruption.
        use super::super::crypto::aes_ecb_encrypt as enc;

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
        let cv = enc(&pk, &mk_raw); // AES-D(pk, cv) == mk_raw
        let mut vd = [0u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = enc(&mk, &vd); // AES-D(mk, mk_dv) starts with sentinel

        // 0x04 SD body: one entry [u_mask_shift=0][uv].
        let mut subdiff = vec![0u8];
        subdiff.extend_from_slice(&uv);

        assert_eq!(
            try_pk_against_tables(std::slice::from_ref(&pk), &subdiff, &cv, &mk_dv),
            Some(mk),
            "planted terminal PK must verify"
        );
        let mut bad = pk;
        bad[0] ^= 0xFF;
        assert_eq!(
            try_pk_against_tables(std::slice::from_ref(&bad), &subdiff, &cv, &mk_dv),
            None,
            "corrupted PK must be rejected"
        );
    }

    #[test]
    fn validate_processing_key_round_trip_with_nonzero_uv() {
        // Synthesise a (pk, uv, mk, cvalue, mk_dv) tuple that satisfies the
        // libaacs _validate_pk relation, then confirm validate_processing_key
        // recovers mk. Catches the bugs that landed pre-fix:
        //   * uv XOR step was missing → mk wrong whenever uv != 0
        //   * AES-128E + 12-zero check instead of AES-128D + magic
        use super::super::crypto::{aes_ecb_decrypt as dec, aes_ecb_encrypt as enc};

        let pk: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

        // cvalue is what AES-128E(pk, mk') gives, where mk' = mk with the
        // last-4-bytes-uv XOR pre-undone:
        //   mk_raw[12..16] = mk[12..16] XOR uv  (so the validate step XORs
        //   uv back in and recovers mk).
        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv[a];
        }
        let cvalue = enc(&pk, &mk_raw);

        // mk_dv is the encryption (under the correct mk) of the verify
        // magic, padded with arbitrary bytes — when decrypted with mk we
        // recover the magic.
        let mut plaintext_vd = [0u8; 16];
        plaintext_vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        // Trailing 8 bytes are don't-cares in the magic check.
        plaintext_vd[8..].copy_from_slice(&[0x11; 8]);
        let mk_dv = enc(&mk, &plaintext_vd);
        // Sanity: decrypting mk_dv with mk yields the magic.
        let _check = dec(&mk, &mk_dv);

        let recovered = validate_processing_key(&pk, &cvalue, &uv, &mk_dv)
            .expect("validate_processing_key must accept a correct pk + uv pair");
        assert_eq!(recovered, mk, "recovered mk must match the planted mk");

        // And a wrong pk must be rejected.
        let mut wrong_pk = pk;
        wrong_pk[0] ^= 0xFF;
        assert!(validate_processing_key(&wrong_pk, &cvalue, &uv, &mk_dv).is_none());

        // And a uv mismatch must be rejected.
        let wrong_uv = [0x00u8, 0x00, 0x00, 0x00];
        assert!(validate_processing_key(&pk, &cvalue, &wrong_uv, &mk_dv).is_none());
    }

    // ── MKB cvalue-record selection (issue #259 / #281) ─────────────────
    //
    // The cvalue (Media Key Data) table is record 0x05; the
    // Subset-Difference index is record 0x04. This matches libaacs
    // (`mkb_cvalues` → 0x05, `mkb_subdiff_records` → 0x04). Record 0x07
    // (Explicit Subset-Difference Record) is NOT the cvalue table. On real
    // in-drive AACS 2.x UHD MKBs 0x07 is small (~96 entries) while the 0x05
    // table is large (181270 entries, 1:1 with 0x04). An earlier
    // `mkb_find_cvalues` preferred 0x07, which under-tested the SD walk and
    // broke the DK→walk path. The selector must prefer 0x05.

    /// Build a 4-byte MKB record header (type + 3-byte big-endian total
    /// length, header included) and append `body`.
    fn mkb_record(rec_type: u8, body: &[u8]) -> Vec<u8> {
        let total = 4 + body.len();
        let mut rec = Vec::with_capacity(total);
        rec.push(rec_type);
        rec.push(((total >> 16) & 0xFF) as u8);
        rec.push(((total >> 8) & 0xFF) as u8);
        rec.push((total & 0xFF) as u8);
        rec.extend_from_slice(body);
        rec
    }

    /// Synthesize an AACS-2.x-shaped MKB carrying BOTH a small 0x07 record
    /// and the real 0x05 cvalue table, with 0x07 placed first so a
    /// "0x07-first" selector would pick the wrong record. The 0x05 table
    /// has `n` 16-byte entries (1:1 with the `n`-entry 0x04 SD index); the
    /// 0x07 decoy has `decoy` 16-byte entries.
    fn synth_aacs2_mkb(n: usize, decoy: usize) -> Vec<u8> {
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&mkb_record(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&mkb_record(0x86, &[0xABu8; 16]));
        let mut sd = Vec::with_capacity(n * 5);
        for i in 0..n {
            sd.push(0x00); // u_mask_shift, top bits clear → not revoked
            sd.extend_from_slice(&((i as u32) + 1).to_be_bytes());
        }
        mkb.extend_from_slice(&mkb_record(0x04, &sd));
        mkb.extend_from_slice(&mkb_record(0x07, &vec![0x11u8; decoy * 16])); // decoy first
        mkb.extend_from_slice(&mkb_record(0x05, &vec![0x22u8; n * 16])); // real cvalues
        mkb
    }

    #[test]
    fn cvalue_selection_prefers_0x05_over_0x07() {
        // AACS-2.x layout: large 0x05 (1:1 with 0x04) + smaller decoy 0x07
        // placed earlier in the record stream.
        let n = 1500;
        let decoy = 96;
        let mkb = synth_aacs2_mkb(n, decoy);

        let sd = probe::mkb_subdiff(&mkb).expect("0x04 present");
        let r05 = probe::mkb_record_body(&mkb, 0x05).expect("0x05 present");
        let r07 = probe::mkb_record_body(&mkb, 0x07).expect("0x07 present");
        let selected = mkb_find_cvalues(&mkb).expect("cvalues selected");

        assert_eq!(sd.len() / 5, n, "0x04 SD index entry count");
        assert_eq!(r05.len() / 16, n, "0x05 cvalue entry count");
        assert_eq!(r07.len() / 16, decoy, "0x07 decoy entry count");

        // The fix: selection MUST pick 0x05 (the large 1:1 table), NOT the
        // 0x07 decoy a "0x07-first" rule would return.
        assert_eq!(
            selected.len() / 16,
            n,
            "cvalue selection must use the large 0x05 table, not the {decoy}-entry 0x07 decoy"
        );
        assert_eq!(
            selected, r05,
            "selected body must be the 0x05 record verbatim"
        );
        assert_eq!(
            selected.len() / 16,
            sd.len() / 5,
            "cvalue table must be 1:1 with the 0x04 Subset-Difference index"
        );
    }

    #[test]
    fn cvalue_selection_falls_back_to_0x07_when_no_0x05() {
        // Malformed/legacy MKB with only a 0x07 record and no 0x05: the
        // selector falls back to 0x07 rather than returning None.
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&mkb_record(0x10, &[0, 0, 0, 0x10, 0, 0, 0, 1]));
        mkb.extend_from_slice(&mkb_record(0x86, &[0xCDu8; 16]));
        mkb.extend_from_slice(&mkb_record(0x04, &[0x00, 0, 0, 0, 1]));
        let only07 = vec![0x33u8; 16];
        mkb.extend_from_slice(&mkb_record(0x07, &only07));

        assert!(probe::mkb_record_body(&mkb, 0x05).is_none());
        let selected = mkb_find_cvalues(&mkb).expect("falls back to 0x07");
        assert_eq!(selected, only07, "fallback returns the 0x07 body");
    }

    /// Locate a captured MKB sample under the optional `MKB_SAMPLE_DIR`.
    /// Returns `None` (skip) when the directory or file is absent.
    fn mkb_sample(rel: &str) -> Option<std::path::PathBuf> {
        let dir = std::env::var("MKB_SAMPLE_DIR").ok()?;
        let p = std::path::Path::new(&dir).join(rel);
        if p.exists() { Some(p) } else { None }
    }

    #[test]
    fn real_aacs2_samples_select_large_0x05_not_small_0x07() {
        // Real in-drive AACS 2.x UHD MKBs carry BOTH a small 0x07
        // Explicit-Subset-Difference record (96 16-byte entries) AND the
        // large 0x05 Media Key Data / cvalue table (181270 entries, 1:1
        // with the 0x04 index). The production selector must return the
        // LARGE 0x05 body, not the small 0x07 one. This is the exact
        // regression #259 found. Skips when no sample dir is present.
        let samples = [
            "sample-a/MKB_RO.inf",
            "sample-b/MKB_RO.inf",
            "sample-c/MKB_RO.inf",
        ];
        let mut checked = 0;
        for rel in samples {
            let path = match mkb_sample(rel) {
                Some(p) => p,
                None => continue,
            };
            let data = std::fs::read(&path).expect("read sample MKB");

            let r05 = probe::mkb_record_body(&data, 0x05)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x05 Media Key Data record"));
            let r07 = probe::mkb_record_body(&data, 0x07)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x07 record"));
            let sd = probe::mkb_subdiff(&data)
                .unwrap_or_else(|| panic!("{rel}: expected a 0x04 Subset-Difference index"));

            let n05 = r05.len() / 16;
            let n07 = r07.len() / 16;

            // The discriminating facts the bug report cited.
            assert!(
                n05 > n07 * 100,
                "{rel}: 0x05 ({n05}) must dwarf 0x07 ({n07})"
            );
            assert_eq!(n05, 181270, "{rel}: full 0x05 cvalue table size");
            assert_eq!(n07, 96, "{rel}: small 0x07 record size");

            // Production selection must be the large 0x05 table.
            let selected = mkb_find_cvalues(&data)
                .unwrap_or_else(|| panic!("{rel}: cvalue selection returned None"));
            assert_eq!(
                selected, r05,
                "{rel}: selector must return the large 0x05 body, not 0x07"
            );

            // And it is 1:1 with the 0x04 SD index the walk iterates: the
            // walk's UV count (take_while top-2-bits clear) lines up with
            // the cvalue count to within the trailing padding entry.
            let uv_entries = sd
                .chunks(5)
                .take_while(|c| c.len() == 5 && (c[0] & 0xC0) == 0)
                .count();
            assert!(
                uv_entries >= n05 - 2 && uv_entries <= n05,
                "{rel}: 0x04 UV count ({uv_entries}) should match 0x05 cvalue count ({n05})"
            );

            eprintln!(
                "{rel}: 0x05={n05} cvalues, 0x07={n07}, 0x04 UVs={uv_entries} — selected 0x05"
            );
            checked += 1;
        }
        if checked == 0 {
            eprintln!("no MKB samples present; skipping real-sample assertion");
        }
    }

    #[test]
    fn mkb_find_mk_dv_recognizes_type_0x86() {
        // AACS 2.0 form uses type 0x86 for the verify record.
        let expected: [u8; 16] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C,
        ];
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4D,
        ];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&expected);
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        assert_eq!(mkb_find_mk_dv(&mkb), Some(expected));
    }

    /// Build a minimal Unit_Key_RO.inf with `num_unit_keys = 1`. The
    /// disc hash won't be in any synthetic keydb so path 1 misses,
    /// which lets us isolate the path-2/3/4 short-circuit behavior.
    fn minimal_unit_key_ro() -> Vec<u8> {
        let mut data = vec![0u8; 256];
        // uk_pos = 0x60
        data[3] = 0x60;
        data[16] = 1; // app_type = BD-ROM
        data[17] = 1; // num_bdmv_dir
        let uk_pos = 0x60usize;
        data[uk_pos + 1] = 1; // 1 unit key
        // Key at uk_pos + 48 — value doesn't matter, just needs to fit.
        for i in 0..16 {
            data[uk_pos + 48 + i] = 0xCC;
        }
        data
    }

    #[test]
    fn resolve_keys_skips_paths_2_through_4_when_vid_is_zero() {
        // No VID -> paths 2/3/4 cannot succeed. The function must
        // return None WITHOUT touching the MKB / device keys, so we
        // can pass an MKB that would otherwise cause expensive
        // derivation work — it must not be consumed.
        let uk_ro = minimal_unit_key_ro();
        let zero_vid = [0u8; 16];

        // A provider carrying a dummy processing key but NO disc entry that
        // matches this disc. `disc_entry: None` preserves the negative-miss
        // the test asserts: with VID=0, paths 1/2/3 are skipped and the
        // path-4/5 hash lookup must MISS (a SuppliedKey returns its
        // disc_entry unconditionally, so the planted entry would WRONGLY hit
        // path 4 — None keeps the miss).
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: vec![[0u8; 16]],
            media_keys: Vec::new(),
            disc_entry: None,
        };

        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &zero_vid,
            providers,
            mkb: None,
        };
        let result = resolve_keys_v1(&ctx);
        assert!(
            result.is_none(),
            "resolve_keys with VID=0 and no matching disc-hash entry must return None"
        );
    }

    #[test]
    fn resolve_keys_path4_still_runs_when_vid_is_zero() {
        // Path 4 (disc-hash → VUK) doesn't need VID. Confirm the
        // short-circuit doesn't block it: install a keydb entry whose
        // disc_hash matches the fixture's hash, with a known VUK, and
        // verify resolve_keys returns it with key_source = 4.
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        // `find_disc` lowercases the incoming hash; the entry map is
        // keyed lowercase too, so we have to lowercase here.
        let hash_hex = disc_hash_hex(&hash).to_lowercase();

        let known_vuk = [0xABu8; 16];
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some(known_vuk),
                unit_keys: Vec::new(),
            }),
        };

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        let resolved =
            resolve_keys_v1(&ctx).expect("path 4 must run regardless of VID availability");
        assert_eq!(resolved.vuk, Some(known_vuk));
        assert_eq!(resolved.key_source, 4);
    }

    #[test]
    fn resolve_keys_path5_uses_keydb_unit_keys_when_vuk_absent() {
        // Path 5: an entry with no VUK but with pre-decrypted unit
        // keys matching the disc's CPS-unit numbering decrypts the
        // disc directly. Covers the ~4,572 U-only KEYDB entries
        // (mostly MKBv76+ UHDs) that the resolver previously ignored.
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();

        // `minimal_unit_key_ro` declares CPS unit 1; supply a matching
        // pre-decrypted unit key in the KEYDB entry.
        let known_uk = [0xCDu8; 16];
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: None,
                unit_keys: vec![(1, known_uk)],
            }),
        };

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        let resolved =
            resolve_keys_v1(&ctx).expect("path 5 must succeed when KEYDB carries unit keys");
        assert_eq!(resolved.vuk, None, "path 5 has no VUK to return");
        assert_eq!(resolved.key_source, 5);
        assert_eq!(resolved.unit_keys, vec![(1, known_uk)]);
    }
    #[test]
    fn resolve_keys_path5_rejects_partial_unit_key_coverage() {
        // If the disc declares a CPS unit that's not in the KEYDB
        // entry's unit_keys, path 5 must NOT half-decrypt the disc.
        // The match function returns None and the resolver falls
        // through to None overall (no other paths available in this
        // setup).
        let uk_ro = minimal_unit_key_ro();
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();
        // KEYDB has a key for CPS unit 99, but the disc declares unit 1.
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: None,
                unit_keys: vec![(99, [0xEEu8; 16])],
            }),
        };

        let vid = [0u8; 16];
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None,
        };
        assert!(
            resolve_keys_v1(&ctx).is_none(),
            "partial CPS-unit coverage must not produce a half-decrypted result"
        );
    }
    #[test]
    fn resolve_keys_path2_5_mk_pool_brute_resolves_unkeyed_disc() {
        // The keyless-disc case: this disc's own hash/VID are NOT in keydb, but its
        // Media Key IS — filed under a sibling disc that shares its MKB. Path
        // 2.5 must km_verifies that MK against the MKB and resolve.
        use super::super::crypto::aes_ecb_encrypt as enc;
        let km = [0x11u8; 16];
        let vid = [0x22u8; 16];
        // MKB: 0x10 type/version + 0x86 verify record whose mk_dv decrypts under
        // km to the AACS verify magic, so km_verifies(mkb, km) == true.
        let mut vd = [0u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = enc(&km, &vd);
        let mut mkb = mkb_record(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x4D]);
        mkb.extend_from_slice(&mkb_record(0x86, &mk_dv));
        assert!(
            probe::km_verifies(&mkb, &km),
            "fixture: km must verify the MKB"
        );
        // This disc's inf (its hash will NOT be in keydb).
        let uk_ro = minimal_unit_key_ro();
        // The sibling's MK is lifted directly into the MK pool: a KeyDb
        // aggregated per-disc media_keys into media_keys(), but SuppliedKey
        // does NOT harvest its disc_entry's media_key — it has an explicit
        // media_keys field. `disc_entry: None` preserves the miss on this
        // disc's own hash/VID (the sibling matches neither), so ONLY the
        // MK-pool brute (km_verifies) can resolve it — exactly the path under
        // test.
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: vec![km],
            disc_entry: None,
        };
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: Some(&mkb),
        };
        let resolved = resolve_keys_v1(&ctx)
            .expect("MK-pool brute (path 2.5) must resolve a disc whose MK is in keydb");
        assert_eq!(
            resolved.key_source, 3,
            "MK-pool brute is the KEYDB-derived class"
        );
        assert_eq!(
            resolved.vuk,
            Some(derive_vuk(&km, &vid)),
            "VUK must derive from the verified Km + this disc's VID"
        );
    }
    #[test]
    fn test_content_cert_parse() {
        // AACS 1.0 cert, bus encryption OFF. Layout matches libaacs: flag in
        // BIT 7 of byte 1, cc_id at bytes 14..20.
        let mut data = vec![0u8; 20];
        data[0] = 0x00; // AACS 1.0
        data[1] = 0x00; // bus_encryption flag (bit 7) clear
        data[14..20].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        let cc = parse_content_cert(&data).unwrap();
        assert_eq!(cc.version, AacsVersion::V10);
        assert!(!cc.bus_encryption);
        assert_eq!(cc.cc_id, [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        // AACS 2.0 with bus encryption: type 0x10, flag is BIT 7 (0x80) of byte 1
        // — NOT bit 0. A cert with byte1=0x01 must therefore read as bus-OFF.
        data[0] = 0x10; // AACS 2.0
        data[1] = 0x80; // bus_encryption_enabled_flag = bit 7
        let cc = parse_content_cert(&data).unwrap();
        assert_eq!(cc.version, AacsVersion::V20);
        assert!(cc.bus_encryption);
        // Regression guard: bit 0 set, bit 7 clear -> bus OFF (the old bug read this as ON).
        data[1] = 0x01;
        assert!(!parse_content_cert(&data).unwrap().bus_encryption);
    }
    // ════════════════════════════════════════════════════════════════════
    // Hardening additions
    // ════════════════════════════════════════════════════════════════════
    // ── VUK derivation: spec relation VUK = AES-D(MK, VID) XOR VID ─────────
    #[test]
    fn derive_vuk_matches_spec_relation_explicitly() {
        // Independently compute AES-ECB-D(mk, vid) XOR vid and confirm
        // derive_vuk produces the same 16 bytes. A mutation that dropped the
        // XOR-VID step, or used encrypt instead of decrypt, fails this.
        use super::super::crypto::aes_ecb_decrypt as dec;
        let mk = [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
            0x1E, 0x1F,
        ];
        let vid = [
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D,
            0x2E, 0x2F,
        ];
        let mut expected = dec(&mk, &vid);
        for i in 0..16 {
            expected[i] ^= vid[i];
        }
        assert_eq!(derive_vuk(&mk, &vid), expected);
    }
    #[test]
    fn decrypt_unit_key_is_plain_aes_ecb_decrypt_under_vuk() {
        // The encrypted unit key in Unit_Key_RO.inf is AES-ECB-E(VUK, uk);
        // decrypt_unit_key must be the matching ECB-decrypt. Round-trip via
        // encrypt to pin the relation.
        use super::super::crypto::aes_ecb_encrypt as enc;
        let vuk = [0x9Eu8; 16];
        let uk = [0x3Cu8; 16];
        let enc_uk = enc(&vuk, &uk);
        assert_eq!(decrypt_unit_key(&vuk, &enc_uk), uk);
    }
    // ── Unit_Key_RO stride: 48 (V10) vs 64 (V20/V21) ──────────────────────
    /// Build a Unit_Key_RO.inf carrying `num_uk` keys at a given stride,
    /// where key `i` is filled with byte `0x10 + i`. uk_pos = 0x60.
    fn build_unit_key_ro(num_uk: usize, stride: usize) -> Vec<u8> {
        let uk_pos = 0x60usize;
        let size = uk_pos + 48 + stride * num_uk + 64;
        let mut data = vec![0u8; size];
        // uk_pos BE32 at [0..4].
        data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        data[16] = 1; // app_type
        data[17] = 1; // num_bdmv_dir
        // num_unit_keys BE16 at uk_pos.
        data[uk_pos..uk_pos + 2].copy_from_slice(&(num_uk as u16).to_be_bytes());
        // Keys start at uk_pos + 48, stride apart.
        let mut pos = uk_pos + 48;
        for i in 0..num_uk {
            for b in &mut data[pos..pos + 16] {
                *b = 0x10 + i as u8;
            }
            pos += stride;
        }
        data
    }
    #[test]
    fn stride_v10_is_48_v20_is_64_and_picks_distinct_keys() {
        // AACS 1.0 stride = 48, AACS 2.0/2.1 stride = 64 (keys.rs:30-35).
        // Lay keys at 64-byte stride. Parsing at V20 stride must pick exactly
        // those keys; parsing the SAME bytes at V10 (48) stride would read the
        // wrong (intermediate) bytes for key 2 onward — proving the stride
        // selector matters.
        let data = build_unit_key_ro(2, 64);
        let v20 = parse_unit_key_ro(&data, AacsVersion::V20).unwrap();
        assert_eq!(v20.encrypted_keys.len(), 2);
        assert_eq!(v20.encrypted_keys[0].1, [0x10; 16]);
        assert_eq!(v20.encrypted_keys[1].1, [0x11; 16]);
        // Same buffer, V10 stride: key 1 still lands at uk_pos+48, but key 2
        // is read at +48 (not +64) so it is NOT the planted 0x11 block.
        let v10 = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert_eq!(v10.encrypted_keys[0].1, [0x10; 16]);
        assert_ne!(
            v10.encrypted_keys[1].1, [0x11; 16],
            "48-byte stride must read different bytes than 64-byte stride"
        );
    }
    #[test]
    fn v21_uses_same_64_byte_stride_as_v20() {
        // V21 shares V20's 64-byte stride (the enum match groups V20|V21).
        let data = build_unit_key_ro(2, 64);
        let v20 = parse_unit_key_ro(&data, AacsVersion::V20).unwrap();
        let v21 = parse_unit_key_ro(&data, AacsVersion::V21).unwrap();
        assert_eq!(v20.encrypted_keys, v21.encrypted_keys);
        assert_eq!(v21.version, AacsVersion::V21);
    }
    // ── parse_unit_key_ro: early returns / boundaries ──────────────────────
    #[test]
    fn parse_unit_key_ro_rejects_too_short_header() {
        // < 20 bytes → None (header fields at 16-18 would index OOB).
        assert!(parse_unit_key_ro(&[0u8; 19], AacsVersion::V10).is_none());
    }
    #[test]
    fn parse_unit_key_ro_rejects_uk_pos_past_end() {
        // uk_pos points past the buffer → the `uk_pos + 2 > len` guard
        // returns None rather than indexing OOB.
        let mut data = vec![0u8; 64];
        data[0..4].copy_from_slice(&1000u32.to_be_bytes()); // uk_pos = 1000
        assert!(parse_unit_key_ro(&data, AacsVersion::V10).is_none());
    }
    #[test]
    fn parse_unit_key_ro_zero_keys_returns_empty_set() {
        // num_unit_keys == 0 → a valid file with no encrypted keys (early
        // Some(..) branch), NOT None.
        let uk_pos = 0x60usize;
        let mut data = vec![0u8; uk_pos + 48];
        data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        data[16] = 1;
        // num_uk left 0.
        let parsed = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert!(parsed.encrypted_keys.is_empty());
        assert_eq!(parsed.app_type, 1);
    }
    #[test]
    fn parse_unit_key_ro_truncated_key_region_returns_none() {
        // keys_start + 16 > len → None (the first key can't fit).
        let uk_pos = 0x60usize;
        let mut data = vec![0u8; uk_pos + 48 + 8]; // only 8 of 16 key bytes
        data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        data[uk_pos + 1] = 1; // 1 key declared
        assert!(parse_unit_key_ro(&data, AacsVersion::V10).is_none());
    }
    #[test]
    fn parse_unit_key_ro_rejects_when_keys_run_off_end() {
        // Finding #5: 3 keys declared but the buffer holds only 2 strides plus
        // 8 trailing bytes (not a full 3rd 16-byte key). The extraction loop
        // breaks at the buffer end (never reading OOB), and the post-loop
        // length check rejects the short list with None — a truncated/malformed
        // .inf must NOT be silently accepted with fewer keys than declared.
        let uk_pos = 0x60usize;
        let stride = 48usize;
        // Room for keys at uk_pos+48 and uk_pos+48+48, then only 8 spare bytes
        // (key 3 would start at uk_pos+48+96 and need 16, but only 8 remain).
        let size = uk_pos + 48 + stride + 16 + 8;
        let mut data = vec![0u8; size];
        data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        data[uk_pos + 1] = 3; // declare 3 keys
        assert!(
            parse_unit_key_ro(&data, AacsVersion::V10).is_none(),
            "a buffer declaring more keys than it contains must be rejected"
        );
    }
    #[test]
    fn parse_unit_key_ro_app_type_and_skb_flag() {
        // app_type at [16], num_bdmv_dir at [17], use_skb_mkb = bit 7 of [18].
        let mut data = build_unit_key_ro(1, 48);
        data[16] = 0x02;
        data[17] = 0x05;
        data[18] = 0x80; // bit 7 set
        let p = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert_eq!(p.app_type, 0x02);
        assert_eq!(p.num_bdmv_dir, 0x05);
        assert!(p.use_skb_mkb, "bit 7 of byte 18 → use_skb_mkb true");
        // Clearing bit 7 (other bits set) → false.
        data[18] = 0x7F;
        let p2 = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert!(!p2.use_skb_mkb);
    }
    #[test]
    fn parse_unit_key_ro_cps_unit_numbers_are_1_based() {
        // The disc's CPS unit numbers are emitted as (i+1) — keys.rs:162.
        let data = build_unit_key_ro(3, 48);
        let p = parse_unit_key_ro(&data, AacsVersion::V10).unwrap();
        assert_eq!(
            p.encrypted_keys.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
    #[test]
    fn parse_unit_key_ro_title_cps_mapping_first_play_top_menu_then_titles() {
        // [20..22] first_play, [22..24] top_menu, [24..26] num_titles, then
        // per-title 2-byte pad + 2-byte CPS unit at 26 + i*4 + 2. Each on-disc
        // 1-based CPS number in `1..=num_uk` is validated and converted to a
        // 0-based key index (libaacs unit_key.c); an out-of-range number → 0.
        let mut data = build_unit_key_ro(4, 64); // num_uk = 4 → CPS 1..=4 valid
        data[20..22].copy_from_slice(&1u16.to_be_bytes()); // first_play CPS 1
        data[22..24].copy_from_slice(&2u16.to_be_bytes()); // top_menu  CPS 2
        data[24..26].copy_from_slice(&3u16.to_be_bytes()); // num_titles = 3
        data[28..30].copy_from_slice(&3u16.to_be_bytes()); // title 0 CPS 3
        data[32..34].copy_from_slice(&4u16.to_be_bytes()); // title 1 CPS 4
        data[36..38].copy_from_slice(&9u16.to_be_bytes()); // title 2 CPS 9 (> num_uk)
        let p = parse_unit_key_ro(&data, AacsVersion::V20).unwrap();
        // 1-based CPS {1,2,3,4} → 0-based {0,1,2,3}; out-of-range 9 → 0.
        assert_eq!(p.title_cps_unit, vec![0, 1, 2, 3, 0]);
    }
    // ── MKB record framing: rec_len is BE24 incl. 4-byte header ────────────
    #[test]
    fn mkb_version_uses_be24_length_and_reads_offset_8() {
        // Type 0x10, BE24 length 0x0C (12). Body starts at pos+4: Type field
        // u32 at body offset 0, version u32 at body offset 4 (pos+8).
        // Confirm a length encoded in the high BE24 byte is honored.
        let mkb = [
            0x10, 0x00, 0x00, 0x0C, 0x11, 0x22, 0x33, 0x44, 0x01, 0x02, 0x03, 0x04,
        ];
        // version = 0x01020304.
        assert_eq!(mkb_version(&mkb), Some(0x0102_0304));
    }
    #[test]
    fn mkb_type_category_c_20_is_uhd() {
        // Type 0x10 record, BE24 length 0x0C (12). MKBType field (body
        // offset 0 = pos+4) = MKB_20_CATEGORY_C (0x48141003).
        let mkb = [
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];
        assert_eq!(mkb_type_raw(&mkb), Some(MKB_20_CATEGORY_C));
        assert_eq!(mkb_type(&mkb), Some(MkbType::CategoryC20));
        assert_eq!(mkb_is_uhd(&mkb), Some(true));
        assert!(MkbType::CategoryC20.is_uhd());
        assert_eq!(MkbType::CategoryC20.generation(), AacsVersion::V20);
        // Sanity on the 2.1 sibling.
        assert_eq!(MkbType::from_raw(MKB_21_CATEGORY_C), MkbType::CategoryC21);
        assert_eq!(MkbType::CategoryC21.generation(), AacsVersion::V21);
    }
    #[test]
    fn mkb_type_prerecorded_is_bluray_v10() {
        // Type 0x10 record with MKB_TYPE_4_PRERECORDED (0x00041003) — a
        // standard Blu-ray (AACS 1.0) block, not UHD.
        let mkb = [
            0x10, 0x00, 0x00, 0x0C, 0x00, 0x04, 0x10, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];
        assert_eq!(mkb_type(&mkb), Some(MkbType::Prerecorded));
        assert_eq!(mkb_is_uhd(&mkb), Some(false));
        assert!(!MkbType::Prerecorded.is_uhd());
        assert_eq!(MkbType::Prerecorded.generation(), AacsVersion::V10);
    }
    #[test]
    fn mkb_type_none_when_no_0x10_record() {
        // A buffer whose only record is a 0x81 (verify-media-key) record and
        // no 0x10 Type-and-Version record → mkb_type_raw returns None.
        let mkb = [0x81, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(mkb_type_raw(&mkb), None);
        assert_eq!(mkb_type(&mkb), None);
        assert_eq!(mkb_is_uhd(&mkb), None);
    }
    #[test]
    fn mkb_find_mk_dv_skips_short_verify_record() {
        // A 0x81 record with rec_len < 20 carries no full mk_dv; the finder
        // must skip it and keep walking (here to a valid 0x86 after it).
        let mut mkb = vec![0x81, 0x00, 0x00, 0x10]; // rec_len 16 (< 20)
        mkb.extend_from_slice(&[0x00; 12]);
        let expected = [0xC1u8; 16];
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x18]);
        mkb.extend_from_slice(&expected);
        mkb.extend_from_slice(&[0x00; 4]);
        assert_eq!(mkb_find_mk_dv(&mkb), Some(expected));
    }
    #[test]
    fn mkb_find_mk_dv_stops_on_overrun_length() {
        // A rec_len that runs past the buffer ends the walk (break), so no
        // mk_dv is found and we get None rather than an OOB slice.
        let mkb = [0x81, 0x00, 0xFF, 0xFF, 0x00, 0x00]; // claims 65535 bytes
        assert_eq!(mkb_find_mk_dv(&mkb), None);
    }
    #[test]
    fn mkb_find_mk_dv_stops_on_zero_length_record() {
        // rec_len < 4 (here 0) breaks the walk — guards against an infinite
        // loop on a malformed record (pos would never advance).
        let mkb = [0x81, 0x00, 0x00, 0x00, 0x99];
        assert_eq!(mkb_find_mk_dv(&mkb), None);
    }
    // ── mkb_content_len / trim_mkb ─────────────────────────────────────────
    #[test]
    fn mkb_content_len_stops_at_zero_type_padding_byte() {
        // A type==0 byte marks the start of padding (records done). Two real
        // records then a 0x00 type byte → content_len == sum of the two recs.
        let mut mkb = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 1]; // 8-byte rec
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x08, 9, 9, 9, 9]); // 8-byte rec
        let content = mkb.len();
        mkb.extend_from_slice(&[0x00, 0x00, 0x00, 0x08]); // padding starts (type 0)
        assert_eq!(mkb_content_len(&mkb), content);
    }
    #[test]
    fn mkb_content_len_returns_full_len_when_no_padding() {
        let mut mkb = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 1];
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x08, 9, 9, 9, 9]);
        assert_eq!(mkb_content_len(&mkb), mkb.len());
    }
    #[test]
    fn trim_mkb_leaves_exactly_sized_buffer_untouched() {
        // n == mkb.len() (no padding) → the `n < mkb.len()` guard is false,
        // so the buffer is returned untouched (no spurious truncate).
        let mkb = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 1];
        assert_eq!(trim_mkb(mkb.clone()), mkb);
    }
    // ── Content Certificate parsing ────────────────────────────────────────
    #[test]
    fn parse_content_cert_rejects_short_buffer() {
        // < 20 bytes → None (cc_id slice [14..20] would index OOB).
        assert!(parse_content_cert(&[0x00; 19]).is_none());
        assert!(parse_content_cert(&[0x00; 20]).is_some());
    }
    #[test]
    fn parse_content_cert_extracts_cc_id_and_nonzero_type_is_v20() {
        // libaacs layout: [0]=type, [1] bit7=bus-enc, [14..20]=cc_id. Any
        // non-0x00 type → V20.
        let mut data = vec![0u8; 20];
        data[0] = 0x10; // AACS2 type marker → V20
        data[1] = 0x00;
        data[14..20].copy_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
        let cc = parse_content_cert(&data).unwrap();
        assert_eq!(cc.version, AacsVersion::V20);
        assert_eq!(cc.cc_id, [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
        assert!(!cc.bus_encryption);
    }
    #[test]
    fn parse_content_cert_bus_encryption_reads_bit7() {
        // bus_encryption = (data[1] >> 7) & 1 (libaacs). Low bits set with bit7
        // clear → false; bit7 set → true. Pins the bit, not a truthiness of the byte.
        let mut data = vec![0u8; 20];
        data[1] = 0x7F; // bits 0..6 set, bit 7 clear
        assert!(!parse_content_cert(&data).unwrap().bus_encryption);
        data[1] = 0x80; // bit 7 set
        assert!(parse_content_cert(&data).unwrap().bus_encryption);
    }
    // ── resolve: version → stride wiring + V21 upgrade on variant MKB ──────
    #[test]
    fn resolve_keys_v2_upgrades_to_v21_on_variant_mkb() {
        // resolve_keys_v2 parses with the V20 64-byte stride but upgrades the
        // result's version to V21 if the MKB carries a 0x82/0x83 variant
        // record. Path 4 (hash→VUK) supplies the actual keys.
        let uk_ro = build_unit_key_ro(1, 64);
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "fixture".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some([0x5Au8; 16]),
                unit_keys: Vec::new(),
            }),
        };

        // MKB with a 0x2f variant record makes is_variant_mkb true.
        let mut mkb = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 1];
        mkb.extend_from_slice(&[0x2f, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0x55; 16]);

        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers,
            mkb: Some(&mkb),
        };
        let resolved = resolve_keys_v2(&ctx).expect("path 4 resolves");
        assert_eq!(
            resolved.version,
            AacsVersion::V21,
            "variant MKB must upgrade V20 result to V21"
        );
    }

    #[test]
    fn resolve_keys_v2_stays_v20_on_classical_mkb() {
        // No variant records → version stays V20.
        let uk_ro = build_unit_key_ro(1, 64);
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "f".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some([0x5Au8; 16]),
                unit_keys: Vec::new(),
            }),
        };
        let mkb = vec![0x10, 0x00, 0x00, 0x08, 0, 0, 0, 1];
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers,
            mkb: Some(&mkb),
        };
        assert_eq!(resolve_keys_v2(&ctx).unwrap().version, AacsVersion::V20);
    }

    #[test]
    fn resolve_keys_bus_encryption_flag_flows_from_content_cert() {
        // The resolved.bus_encryption must reflect the content cert's bit0.
        let uk_ro = build_unit_key_ro(1, 48);
        let hash = disc_hash(&uk_ro);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "f".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some([1u8; 16]),
                unit_keys: Vec::new(),
            }),
        };
        // Content cert: AACS2 (type 0x10) + bus encryption enabled (bit 7 of byte 1).
        let mut cc = vec![0u8; 20];
        cc[0] = 0x10;
        cc[1] = 0x80;
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: Some(&cc),
            volume_id: &[0u8; 16],
            providers,
            mkb: None,
        };
        assert!(resolve_keys_v1(&ctx).unwrap().bus_encryption);
    }

    #[test]
    fn resolve_keys_v21_path4_resolves_by_hash() {
        // resolve_keys_v21 must hit path 4 (hash→VUK) and stamp version V21,
        // deriving unit keys from the VUK.
        use super::super::crypto::aes_ecb_encrypt as enc;
        let data = build_unit_key_ro(1, 64);
        // The single encrypted key in build_unit_key_ro is [0x10;16].
        let hash = disc_hash(&data);
        let hash_hex = disc_hash_hex(&hash).to_lowercase();
        let vuk = [0x77u8; 16];
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "f".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some(vuk),
                unit_keys: Vec::new(),
            }),
        };
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &data,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers,
            mkb: None,
        };
        let r = resolve_keys_v21(&ctx).expect("v21 path 4");
        assert_eq!(r.version, AacsVersion::V21);
        assert_eq!(r.key_source, 4);
        assert_eq!(r.vuk, Some(vuk));
        // Unit key derived: AES-D(vuk, enc_key). enc_key here is [0x10;16].
        assert_eq!(r.unit_keys[0].1, decrypt_unit_key(&vuk, &[0x10u8; 16]));
        // Self-consistency: encrypting it back under VUK gives the stored block.
        assert_eq!(enc(&vuk, &r.unit_keys[0].1), [0x10u8; 16]);
    }

    #[test]
    fn resolve_keys_path3_derives_vuk_from_vid_match() {
        // Path 3: an entry whose disc_id == ctx.volume_id supplies an MK;
        // resolver derives VUK = derive_vuk(mk, vid). No hash match needed.
        let uk_ro = minimal_unit_key_ro();
        let vid = [0x42u8; 16];
        let mk = [0x24u8; 16];
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: "0xnotthishash".to_string(),
                title: "sibling".to_string(),
                media_key: Some(mk),
                disc_id: Some(vid),
                vuk: None,
                unit_keys: Vec::new(),
            }),
        };
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &vid,
            providers,
            mkb: None, // no MKB → paths 1/2/2.5 skipped, path 3 fires
        };
        let r = resolve_keys_v1(&ctx).expect("path 3 by VID");
        assert_eq!(r.key_source, 3);
        assert_eq!(r.vuk, Some(derive_vuk(&mk, &vid)));
    }

    #[test]
    fn resolve_keys_returns_none_when_no_provider_has_anything() {
        // Empty provider array + VID present + no MKB → all paths miss → None.
        let uk_ro = minimal_unit_key_ro();
        let providers: &[&dyn super::super::provider::KeyProvider] = &[];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0x42u8; 16],
            providers,
            mkb: None,
        };
        assert!(resolve_keys_v1(&ctx).is_none());
    }

    #[test]
    fn match_keydb_unit_keys_empty_keydb_returns_none() {
        // match_keydb_unit_keys with empty keydb keys → None (so path 5 can't
        // fire on an entry with no unit keys).
        let uk_file = parse_unit_key_ro(&minimal_unit_key_ro(), AacsVersion::V10).unwrap();
        assert!(match_keydb_unit_keys(&uk_file, &[]).is_none());
    }

    // ── derive_media_key_from_dk: revoked-marker stops the uv scan ─────────

    #[test]
    fn derive_media_key_from_dk_breaks_on_revoked_marker() {
        // A subset-difference entry whose u_mask_shift has bit 0x40/0x80 set
        // is a revoke marker; the scan must `break` (not derive a key from it
        // and not panic). Pair it with a DK that would otherwise be tempting.
        let mut mkb: Vec<u8> = Vec::new();
        mkb.extend_from_slice(&[0x81, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xAB; 16]);
        // 0x04 with one entry, u_mask_shift = 0xC0 (both top bits → revoked).
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0xC0, 0x00, 0x00, 0x00, 0x01]);
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&[0xCD; 16]);
        let dk = DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0,
        };
        // The 0xC0 entry is filtered by the num_uvs take_while, so the scan
        // sees zero usable slots and returns None — never a wrong key.
        assert!(derive_media_key_from_dk(&mkb, &[dk]).is_none());
    }

    #[test]
    fn derive_media_key_from_dk_returns_none_when_records_missing() {
        // No 0x04 / 0x05 records → the `?` short-circuits return None.
        let mkb = vec![
            0x81, 0x00, 0x00, 0x14, /* mk_dv */ 0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,
        ];
        assert!(derive_media_key_from_dk(&mkb, &[]).is_none());
    }

    #[test]
    fn find_record_body_returns_none_for_empty_body_record() {
        // find_record_body requires rec_len > 4 (non-empty body). A 4-byte
        // record (header only, empty body) is treated as absent.
        let mkb = [0x05, 0x00, 0x00, 0x04]; // type 0x05, no body
        assert!(probe::mkb_record_body(&mkb, 0x05).is_none());
    }

    #[test]
    fn derive_media_key_and_pk_from_dk_returns_intermediate_pk() {
        // Regression: a classical DK boil must yield the intermediate
        // Processing Key, not just the Media Key. The key service banks the
        // PK lineage (DK·PK·MK·VUK·UK); before the `_and_pk_` form existed it
        // recovered the MK here but lost the PK silently.
        //
        // Build a minimal classical MKB (no 0x82/0x83) with:
        //   - 0x04 Subset-Difference: u_mask_shift=3, uv=0x00000002
        //   - 0x05 cvalues: one cvalue C planted so AES-D(Kp, C) XOR uv == mk
        //   - 0x86 Verify Media Key: mk_dv = AES-E(mk, magic || pad)
        // and a DK with node=4, uv=2, u_mask_shift=3 so dev_key_v_mask ==
        // v_mask: the calc_pk_from_dk loop is a no-op and Kp == aesg3(dk, 1).
        use super::super::crypto::aes_ecb_encrypt as enc;

        let dk_bytes: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        // Expected Processing Key for the no-op walk.
        let expected_pk = aesg3(&dk_bytes, 1);

        // Plant a known Media Key.
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        // uv (big-endian) = 0x00000002; validate XORs uv into mk[12..16].
        let uv_bytes: [u8; 4] = [0x00, 0x00, 0x00, 0x02];

        // cvalue C = AES-E(Kp, mk_raw) where mk_raw = mk with the uv XOR
        // pre-undone, so the validate step XORs uv back in and recovers mk.
        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv_bytes[a];
        }
        let cvalue = enc(&expected_pk, &mk_raw);

        // mk_dv = AES-E(mk, magic || pad); validate decrypts it under mk and
        // checks the leading 8 bytes against the verify magic.
        let mut plaintext_vd = [0u8; 16];
        plaintext_vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        plaintext_vd[8..].copy_from_slice(&[0x11; 8]);
        let mk_dv = enc(&mk, &plaintext_vd);

        // Assemble the MKB. Type/Version (0x10) header first.
        let mut mkb = vec![
            0x10, 0x00, 0x00, 0x0C, 0x48, 0x14, 0x10, 0x03, 0x00, 0x00, 0x00, 0x4D,
        ];
        // 0x04 Subset-Difference: body = u_mask_shift(0x03) || uv(4 bytes).
        mkb.extend_from_slice(&[0x04, 0x00, 0x00, 0x09]);
        mkb.extend_from_slice(&[0x03]);
        mkb.extend_from_slice(&uv_bytes);
        // 0x05 cvalues: one 16-byte cvalue (mkb_find_cvalues prefers 0x05).
        mkb.extend_from_slice(&[0x05, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&cvalue);
        // 0x86 Verify Media Key: mk_dv.
        mkb.extend_from_slice(&[0x86, 0x00, 0x00, 0x14]);
        mkb.extend_from_slice(&mk_dv);

        let dk = DeviceKey {
            key: dk_bytes,
            node: 4,
            uv: 2,
            u_mask_shift: 3,
        };

        // The new `_and_pk_` form returns BOTH the MK and the intermediate PK.
        let dks = [dk];
        let (got_mk, got_pk) = derive_media_key_and_pk_from_dk(&mkb, &dks)
            .expect("classical DK boil must derive (mk, pk)");
        assert_eq!(got_mk, mk, "recovered Media Key must match the planted MK");
        assert_eq!(
            got_pk, expected_pk,
            "returned Processing Key must equal aesg3(dk, 1) for the no-op walk"
        );

        // And the thin wrapper must still return just the MK.
        assert_eq!(derive_media_key_from_dk(&mkb, &dks), Some(mk));
    }

    // ── resolve_keys_with_reason / classify_resolve_failure ────────────────
    //
    // The rc.6 E7017/E7022 split is also exercised end-to-end through the
    // `ensure_decryptable` gate in `disc/mod.rs`. These tests pin the
    // *classifier* directly at the keys.rs seam and cover the branches the
    // gate test does not: VID-present (must never be VidUnavailable), the
    // processing-keys-only material path, and the version dispatch / Ok path.

    /// A `SuppliedKey` provider with the given derivation material and no
    /// disc-keyed entry. Mirrors the construction the gate test uses, lifted to
    /// a helper so each branch reads as one line.
    fn material_provider(
        device_keys: Vec<DeviceKey>,
        processing_keys: Vec<[u8; 16]>,
    ) -> super::super::provider::SuppliedKey {
        super::super::provider::SuppliedKey {
            device_keys,
            processing_keys,
            media_keys: Vec::new(),
            disc_entry: None,
        }
    }

    fn one_device_key() -> DeviceKey {
        DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0,
        }
    }

    /// Zero VID + PROCESSING keys (not device keys) is still "derivation
    /// material present, VID missing" → VidUnavailable (E7017). The gate test
    /// only proves the device-keys arm of `has_derivation_material`; this pins
    /// the processing-keys arm of the same `||`.
    #[test]
    fn classify_processing_keys_zero_vid_is_vid_unavailable() {
        let prov = material_provider(Vec::new(), vec![[0u8; 16]]);
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&prov];
        let uk_ro = minimal_unit_key_ro();
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers,
            mkb: None,
        };
        assert_eq!(
            resolve_keys_with_reason(&ctx, 2).err(),
            Some(ResolveFailure::VidUnavailable),
            "processing keys + zero VID is still material-but-no-VID"
        );
    }

    /// A NON-zero VID present, but resolution still fails (the providers carry
    /// material that doesn't resolve this disc). The VID is available, so the
    /// failure is NOT "VID unavailable" — it must classify NoMaterial regardless
    /// of how much derivation material is present, because re-acquiring the VID
    /// is not the fix. This is the `has_vid == true` short-circuit, which no
    /// existing test covers (the gate test only uses the zero-VID sentinel).
    #[test]
    fn classify_vid_present_with_material_is_no_material_not_vid() {
        let prov = material_provider(vec![one_device_key()], vec![[0u8; 16]]);
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&prov];
        let uk_ro = minimal_unit_key_ro();
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0x42u8; 16], // VID IS available
            providers,
            mkb: None,
        };
        assert_eq!(
            resolve_keys_with_reason(&ctx, 2).err(),
            Some(ResolveFailure::NoMaterial),
            "VID present must never be reported as VidUnavailable, however much \
             derivation material is on hand"
        );
    }

    /// VID present + NO material → NoMaterial (both conditions for
    /// VidUnavailable absent). Distinct from the gate's zero-VID/no-material
    /// branch.
    #[test]
    fn classify_vid_present_no_material_is_no_material() {
        let prov = material_provider(Vec::new(), Vec::new());
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&prov];
        let uk_ro = minimal_unit_key_ro();
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0x42u8; 16],
            providers,
            mkb: None,
        };
        assert_eq!(
            resolve_keys_with_reason(&ctx, 2).err(),
            Some(ResolveFailure::NoMaterial)
        );
    }

    /// `resolve_keys_with_reason` routes `version_u8 == 1` through the V10
    /// resolver and any other value through the V20→V21 chain. Prove the
    /// dispatch by resolving the SAME path-4 (disc-hash→VUK) fixture under both
    /// versions: V10 stamps V10, the non-1 arm reaches V20/V21. A success must
    /// come back as `Ok`, never an `Err(ResolveFailure)`.
    #[test]
    fn resolve_with_reason_dispatches_on_version_and_returns_ok() {
        let uk_ro = build_unit_key_ro(1, 64);
        let hash_hex = disc_hash_hex(&disc_hash(&uk_ro)).to_lowercase();
        let vuk = [0x77u8; 16];
        let keydb = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(DiscEntry {
                disc_hash: hash_hex,
                title: "f".to_string(),
                media_key: None,
                disc_id: None,
                vuk: Some(vuk),
                unit_keys: Vec::new(),
            }),
        };
        let providers: &[&dyn super::super::provider::KeyProvider] = &[&keydb];
        let ctx = ResolveContext {
            unit_key_ro: &uk_ro,
            content_cert: None,
            volume_id: &[0u8; 16],
            providers,
            mkb: None,
        };

        // version 1 → V10 resolver. Path 4 succeeds → Ok, version stamped V10.
        let v1 = resolve_keys_with_reason(&ctx, 1).expect("v1 dispatch must resolve path 4");
        assert_eq!(v1.vuk, Some(vuk));
        assert_eq!(v1.version, AacsVersion::V10);

        // version 2 → V20→V21 chain. Same fixture resolves; not the V10 stamp.
        let v2 = resolve_keys_with_reason(&ctx, 2).expect("non-1 dispatch must resolve path 4");
        assert_eq!(v2.vuk, Some(vuk));
        assert_ne!(
            v2.version,
            AacsVersion::V10,
            "the non-1 arm must run the V20/V21 resolver, not V10"
        );
    }
}

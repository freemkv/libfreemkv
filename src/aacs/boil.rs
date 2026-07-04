//! AACS derivation "boil-down" — one public home for the key chain.
//!
//! Thin newtypes at the API boundary and three wrapper functions over the
//! existing crypto. Nothing here re-implements a primitive: every function
//! delegates to the already-audited code in [`super::keys`] and
//! [`super::variants`], so the boil-down cannot drift from production math.
//!
//! The newtypes wrap bare `[u8; 16]` ONLY at this boundary — the crypto
//! internals continue to operate on raw arrays. They exist so a caller threads
//! the chain `DK → MK → VUK → UK` without confusing one 16-byte secret for
//! another, not to refactor the resolver.
//!
//! Chain (matches `aacs::keys::resolve_keys_classical` path 1 and
//! `aacs::keys::resolve_keys_v21` path 1 byte-for-byte):
//!
//! ```text
//!   mk_from_dk(device_keys, mkb, vid)  →  MediaKey   (Km)
//!   mk_from_pk(processing_keys, mkb)   →  MediaKey   (Km)
//!   vuk_from_mk(MediaKey, Vid)         →  Vuk        (= AES-G(Km, VID))
//!   uk_from_vuk(Vuk, enc_title_keys)   →  [UnitKey]  (decrypt_unit_key each)
//! ```
//!
//! `mk_from_dk` and `mk_from_pk` are two entry points to the SAME Media Key,
//! both via the MKB's Subset-Difference cvalue tables: the device-key path
//! recovers its Processing Key at the matching SD node and walks on to the MK;
//! the processing-key path starts from a precomputed PK. Neither needs a VID
//! (the VID enters at `vuk_from_mk`).

use super::media_key::{derive_media_key_and_pk_from_dk, derive_media_key_from_pk};
use super::types::DeviceKey;
use super::volume_key::{decrypt_unit_key, derive_vuk};

/// Volume ID (16 bytes) — read from the disc via the SCSI handshake / OEM path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vid(pub [u8; 16]);

/// Media Key (Km, 16 bytes) — the MKB-scoped key derived from device keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaKey(pub [u8; 16]);

/// Volume Unique Key (VUK / Kvu, 16 bytes) — derived from `MediaKey` + `Vid`,
/// decrypts the per-disc encrypted title keys in `Unit_Key_RO.inf`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vuk(pub [u8; 16]);

/// Processing Key (Kp, 16 bytes) — an MKB Subset-Difference key that yields the
/// Media Key. A leaked/precomputed PK in the keydb, or the intermediate PK a
/// device-key walk derives at its matching SD node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessingKey(pub [u8; 16]);

/// One decrypted per-CPS-unit AACS title key.
///
/// `idx` is the POSITIONAL index of the encrypted title key within the slice
/// handed to [`uk_from_vuk`] (i.e. its order in `Unit_Key_RO.inf`'s key-storage
/// area). The CPS-unit *number* association (the `u32` in
/// `ResolvedKeys::unit_keys`) is a higher-level concern owned by
/// [`super::keys::parse_unit_key_ro`], which pairs each positional key with its
/// declared CPS unit; this primitive only does the AES, so it surfaces position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitKey {
    pub idx: u32,
    pub key: [u8; 16],
}

/// Derive the Volume Unique Key from a Media Key and Volume ID.
///
/// Wraps [`derive_vuk`] verbatim: `VUK = AES-128-ECB-DECRYPT(MK, VID) XOR VID`.
/// This is byte-identical to the inline `derive_vuk(&mk, ctx.volume_id)` call in
/// every classical resolver path AND to the `Kvu = AES-G(Km, VID)` step inside
/// [`derive_media_key_variant`] (AES-G and `derive_vuk` are the same math), so
/// `vuk_from_mk(mk_from_dk(..)?, vid)` reproduces the V21 variant VUK exactly.
pub fn vuk_from_mk(mk: MediaKey, vid: Vid) -> Vuk {
    Vuk(derive_vuk(&mk.0, &vid.0))
}

/// Decrypt the disc's encrypted title keys with a VUK.
///
/// Wraps [`decrypt_unit_key`] (AES-128-ECB-DECRYPT) per entry, mirroring the
/// `derive_uks` closure in `resolve_keys_classical` / `resolve_keys_v21`. The
/// returned `UnitKey::idx` is the slice position; pair with CPS-unit numbers via
/// [`super::keys::parse_unit_key_ro`] when the numbering matters.
pub fn uk_from_vuk(vuk: Vuk, enc_title_keys: &[[u8; 16]]) -> Vec<UnitKey> {
    enc_title_keys
        .iter()
        .enumerate()
        .map(|(i, enc)| UnitKey {
            idx: i as u32,
            key: decrypt_unit_key(&vuk.0, enc),
        })
        .collect()
}

/// Derive the Media Key (Km) from positioned device keys via the MKB's
/// Subset-Difference tables.
///
/// Wraps [`derive_media_key_and_pk_from_dk`] — the real SD walk the resolver
/// runs: each positioned device key is placed against the MKB's subset-diff /
/// cvalue records, recovering its Processing Key at the matching node and
/// continuing to the Media Key. Reachable for real discs whenever a device key
/// applies to the MKB. No VID is involved here — it enters at [`vuk_from_mk`].
///
/// Returns [`Error::AacsMkUnavailable`] (E7018) when no supplied device key
/// resolves the MKB — the same terminal error as [`mk_from_pk`]; no numeric
/// distinction is load-bearing at this boundary.
///
/// [`Error::AacsMkUnavailable`]: crate::error::Error::AacsMkUnavailable
pub fn mk_from_dk(device_keys: &[DeviceKey], mkb: &[u8]) -> Result<MediaKey, crate::error::Error> {
    // Positioned device keys drive the real Subset-Difference MKB walk
    // ([`derive_media_key_and_pk_from_dk`], the same walk the resolver runs). The
    // old Media-Key-Variant path needed integrator Key Correction Data absent
    // in-tree, so it Err'd for EVERY real disc (dead for both consumers —
    // freemkv-keysources' DK fallback and the kdb harvester). No VID is needed
    // for the Media Key; it enters only at [`vuk_from_mk`].
    match derive_media_key_and_pk_from_dk(mkb, device_keys) {
        Some((km, _pk)) => Ok(MediaKey(km)),
        None => Err(crate::error::Error::AacsMkUnavailable),
    }
}

/// Derive the Media Key (Km) from one or more Processing Keys and the disc MKB.
///
/// Wraps [`derive_media_key_from_pk`] — the Subset-Difference PK→MK walk: each
/// processing key is validated (and tree-walked) against the MKB's cvalue tables
/// (records `0x04`/`0x05`) until one yields the Media Key whose verify record
/// (`0x81`/`0x86`) matches. Unlike [`mk_from_dk`] this path is reachable for
/// real discs — a leaked/precomputed AACS Processing Key in the keydb resolves
/// the Media Key directly. No VID is involved at this step; the VID enters at
/// [`vuk_from_mk`].
///
/// Returns [`Error::AacsMkUnavailable`] (E7018) when no processing key resolves
/// the MKB — the same terminal error as [`mk_from_dk`]; no numeric distinction
/// is load-bearing at this boundary.
///
/// [`Error::AacsMkUnavailable`]: crate::error::Error::AacsMkUnavailable
pub fn mk_from_pk(
    processing_keys: &[[u8; 16]],
    mkb: &[u8],
) -> Result<MediaKey, crate::error::Error> {
    match derive_media_key_from_pk(mkb, processing_keys) {
        Some(km) => Ok(MediaKey(km)),
        None => Err(crate::error::Error::AacsMkUnavailable),
    }
}

/// A candidate key at any rung of the AACS ladder, handed to [`resolve_candidate`].
///
/// Each variant carries the module's existing newtype for that rung (a `Dk` is a
/// POSITIONED [`DeviceKey`] — recover an unpositioned one with
/// [`super::keys::recover_dk_position`] first).
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
/// CPS-unit key the disc's `Unit_Key_RO.inf` yields from the VUK (positional
/// order); the caller runs [`super::content::unit_key_validates`] to find which
/// one actually opens the disc. Rungs above the candidate are `None` (a `Vuk`
/// candidate has no `mk`/`pk`/`dk`; a `Uk` candidate has only `unit_keys`).
#[derive(Debug, Clone)]
pub struct ResolvedChain {
    /// Every unit key derived from the VUK, as `(cps_unit_number, key)` — the
    /// CPS-unit numbers come from `Unit_Key_RO.inf` (via `parse_unit_key_ro`), so
    /// a consumer maps `UK → CPS unit` directly. Same shape as
    /// [`super::keys::ResolvedKeys::unit_keys`]. A `Uk` candidate yields exactly
    /// itself, keyed by its own `idx`.
    pub unit_keys: Vec<(u32, [u8; 16])>,
    pub vuk: Option<Vuk>,
    pub mk: Option<MediaKey>,
    pub pk: Option<ProcessingKey>,
    /// The positioned device key (for a `Dk` candidate).
    pub dk: Option<DeviceKey>,
}

/// Derive the full AACS key chain from a candidate key of ANY ladder rung.
///
/// Runs the deterministic derivation DOWNWARD to the disc's terminal unit keys:
/// `DK → MK → VUK → UKs`, `PK → MK → VUK → UKs`, `MK → VUK → UKs`,
/// `VUK → UKs`, or `UK → itself`. Composes the module's own boil steps
/// ([`mk_from_pk`], [`vuk_from_mk`], [`uk_from_vuk`]) and parses
/// `Unit_Key_RO.inf` at the version the disc's MKB declares (48-byte stride for
/// AACS-1.0, 64 for AACS-2.x), so a multi-CPS disc yields all its unit keys from
/// the one candidate.
///
/// PURE DERIVATION: no sampling, no validation, no position recovery. Every step
/// is deterministic AES, so the returned keys are only as sound as the input
/// candidate — validate `unit_keys` against a real encrypted unit with
/// [`super::content::unit_key_validates`] to prove the candidate opens the disc.
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
    use super::inf::parse_unit_key_ro;
    use super::media_key::derive_media_key_and_pk_from_dk;
    use super::mkb::{AacsVersion, mkb_type};

    // Boil a VUK → all unit keys, each paired with its declared CPS-unit number.
    // `.inf` parsing lives here: derive the stride version from the disc's own
    // MKB, then defer the VUK→unit-keys step to the shared `derive_unit_keys`
    // (the one place both resolvers and this path decrypt the title keys).
    let boil = |vuk: Vuk| -> Option<Vec<(u32, [u8; 16])>> {
        let version = mkb_type(mkb)
            .map(|t| t.generation())
            .unwrap_or(AacsVersion::V10);
        let ukf = parse_unit_key_ro(unit_key_ro, version)?;
        if ukf.encrypted_keys.is_empty() {
            return None;
        }
        Some(super::volume_key::derive_unit_keys(&ukf, &vuk.0))
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
            let vuk = vuk_from_mk(*mk, vid?);
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(*mk),
                pk: None,
                dk: None,
            })
        }
        KeyCandidate::Pk(pk) => {
            let mk = mk_from_pk(std::slice::from_ref(&pk.0), mkb).ok()?;
            let vuk = vuk_from_mk(mk, vid?);
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(mk),
                pk: Some(*pk),
                dk: None,
            })
        }
        KeyCandidate::Dk(dk) => {
            let (km, pk) = derive_media_key_and_pk_from_dk(mkb, std::slice::from_ref(dk))?;
            let mk = MediaKey(km);
            let vuk = vuk_from_mk(mk, vid?);
            Some(ResolvedChain {
                unit_keys: boil(vuk)?,
                vuk: Some(vuk),
                mk: Some(mk),
                pk: Some(ProcessingKey(pk)),
                dk: Some(dk.clone()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::crypto::aes_ecb_encrypt;
    use crate::aacs::volume_key::{decrypt_unit_key, derive_vuk};

    /// `vuk_from_mk` must equal the inline `derive_vuk` path bit-for-bit, for
    /// several known (MK, VID) vectors.
    #[test]
    fn vuk_from_mk_matches_inline_derive_vuk() {
        let cases: [([u8; 16], [u8; 16]); 3] = [
            ([0x5A; 16], [0xA5; 16]),
            ([0x11; 16], [0x22; 16]),
            (
                [
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
                    0x0D, 0x0E, 0x0F,
                ],
                [
                    0xF0, 0xE1, 0xD2, 0xC3, 0xB4, 0xA5, 0x96, 0x87, 0x78, 0x69, 0x5A, 0x4B, 0x3C,
                    0x2D, 0x1E, 0x0F,
                ],
            ),
        ];
        for (mk, vid) in cases {
            let inline = derive_vuk(&mk, &vid);
            let boiled = vuk_from_mk(MediaKey(mk), Vid(vid));
            assert_eq!(boiled.0, inline, "vuk_from_mk must equal derive_vuk");
        }
    }

    /// `uk_from_vuk` must equal the inline `decrypt_unit_key` path bit-for-bit
    /// and carry positional indices 0..n. Built by encrypting known plaintext
    /// title keys under the VUK (the same primitive the resolver inverts).
    #[test]
    fn uk_from_vuk_matches_inline_decrypt_unit_key() {
        let vuk = [0x5Au8; 16];
        let plain_keys = [[0x11u8; 16], [0x22u8; 16], [0xCDu8; 16]];
        let enc: Vec<[u8; 16]> = plain_keys
            .iter()
            .map(|k| aes_ecb_encrypt(&vuk, k))
            .collect();

        let boiled = uk_from_vuk(Vuk(vuk), &enc);
        assert_eq!(boiled.len(), enc.len());
        for (i, uk) in boiled.iter().enumerate() {
            assert_eq!(uk.idx, i as u32, "idx must be the positional index");
            // Matches the inline derive_uks closure: decrypt_unit_key(vuk, enc).
            assert_eq!(uk.key, decrypt_unit_key(&vuk, &enc[i]));
            // And recovers the original plaintext title key.
            assert_eq!(
                uk.key, plain_keys[i],
                "VUK roundtrip recovers the title key"
            );
        }
    }

    /// `uk_from_vuk` on an empty slice yields no keys (no panic, no phantom idx).
    #[test]
    fn uk_from_vuk_empty_is_empty() {
        assert!(uk_from_vuk(Vuk([0u8; 16]), &[]).is_empty());
    }

    /// `mk_from_dk` returns `Err(AacsMkUnavailable)` when the MKB has no
    /// processable Subset-Difference tables (empty MKB, or one with no
    /// mk_dv/cvalues/subdiff records) — never a wrong key, never a panic.
    #[test]
    fn mk_from_dk_errors_on_unprocessable_mkb() {
        let dk = DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0,
        };
        // Empty MKB → no SD records to walk → Err.
        let e = mk_from_dk(std::slice::from_ref(&dk), &[]);
        assert!(matches!(e, Err(crate::error::Error::AacsMkUnavailable)));

        // An MKB with no complete Subset-Difference tables (mk_dv / cvalues /
        // subdiff) cannot yield a Media Key, so the real walk also errors —
        // never silently yields a key.
        let mut mkb: Vec<u8> = Vec::new();
        mkb.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]); // stray data record only
        mkb.extend_from_slice(&[0xAB; 16]);
        let e2 = mk_from_dk(&[dk], &mkb);
        assert!(matches!(e2, Err(crate::error::Error::AacsMkUnavailable)));
    }

    /// Build a 4-byte MKB record header (type + 3-byte big-endian total length,
    /// header included) and append `body`. Mirrors the MKB record framing the
    /// parser expects; no crypto.
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

    /// `mk_from_pk` resolves a planted Processing Key against a synthetic MKB and
    /// drives the FULL boil chain PK → MK → VUK → UK. The MKB is built with the
    /// same (pk, cv, mk_dv, uv) construction the production SD walk validates, so
    /// this proves a PK entry yields real Unit Keys — not just an `Ok`.
    #[test]
    fn mk_from_pk_drives_full_chain_to_uks() {
        let pk: [u8; 16] = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let mk: [u8; 16] = [
            0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD,
            0xAE, 0xAF,
        ];
        let uv: [u8; 4] = [0x00, 0x00, 0x04, 0x00];

        // cv = AES-E(pk, mk_raw), where mk_raw is mk with the last-4-bytes-uv XOR
        // pre-undone, so the validate step XORs uv back in and recovers mk.
        let mut mk_raw = mk;
        for a in 0..4 {
            mk_raw[12 + a] ^= uv[a];
        }
        let cv = aes_ecb_encrypt(&pk, &mk_raw);

        // mk_dv = AES-E(mk, magic||pad): AES-D(mk, mk_dv) starts with the AACS
        // verify sentinel.
        let mut vd = [0x11u8; 16];
        vd[..8].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        let mk_dv = aes_ecb_encrypt(&mk, &vd);

        // Synthetic MKB: type/version (0x10), verify record (0x86 = mk_dv),
        // one-entry SD index (0x04 = [u_mask_shift=0][uv]), one-entry cvalue
        // table (0x05 = cv).
        let mut sd = vec![0u8];
        sd.extend_from_slice(&uv);
        let mut mkb = Vec::new();
        mkb.extend_from_slice(&mkb_record(0x10, &[0, 0, 0, 0x20, 0, 0, 0, 0x52]));
        mkb.extend_from_slice(&mkb_record(0x86, &mk_dv));
        mkb.extend_from_slice(&mkb_record(0x04, &sd));
        mkb.extend_from_slice(&mkb_record(0x05, &cv));

        // PK → MK.
        let got_mk = mk_from_pk(std::slice::from_ref(&pk), &mkb).expect("planted PK resolves MK");
        assert_eq!(got_mk, MediaKey(mk), "mk_from_pk recovers the planted MK");

        // MK → VUK → UK over an encrypted title key.
        let vid = Vid([0x42u8; 16]);
        let plain_uk = [0x7Eu8; 16];
        let vuk = vuk_from_mk(got_mk, vid);
        let enc = aes_ecb_encrypt(&vuk.0, &plain_uk);
        let uks = uk_from_vuk(vuk, std::slice::from_ref(&enc));
        assert_eq!(uks.len(), 1);
        assert_eq!(uks[0].key, plain_uk, "PK chain recovers the title key");

        // A corrupt PK resolves nothing.
        let mut bad = pk;
        bad[0] ^= 0xFF;
        assert!(matches!(
            mk_from_pk(std::slice::from_ref(&bad), &mkb),
            Err(crate::error::Error::AacsMkUnavailable)
        ));
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
        let uk = UnitKey {
            idx: 2,
            key: [0x9u8; 16],
        };
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
}

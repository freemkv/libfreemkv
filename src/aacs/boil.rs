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
//! `mk_from_dk` and `mk_from_pk` are two entry points to the SAME Media Key:
//! the device-key path walks the MKB's Media-Key-Variant chain, the
//! processing-key path walks the MKB's Subset-Difference cvalue tables. Neither
//! needs a VID (the VID enters at `vuk_from_mk`).

use super::keys::{decrypt_unit_key, derive_media_key_from_pk, derive_vuk};
use super::types::DeviceKey;
use super::variants::{KEY_CORRECTION_DATA_PLACEHOLDER, derive_media_key_variant, walk_mkb};

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

/// Derive the Media Key (Km) from device keys via the Media Key Variant chain.
///
/// Wraps [`walk_mkb`] + [`derive_media_key_variant`] with exactly the arguments
/// `resolve_keys_v21` path 1 passes: the placeholder Key Correction Data and the
/// disc Volume ID. Returns the FIRST tuple element `Km` (the Media Key) — the
/// resolver treats `Km` as the media key and derives the VUK from it as
/// `Kvu = AES-G(Km, VID)`, which equals [`vuk_from_mk`]`(MediaKey(km), vid)`. The
/// variant fn's second element is that already-derived `Kvu`; returning `Km`
/// keeps this primitive at the "media key" level so the chain composes.
///
/// Because the integrator KCD is unavailable in-tree (the placeholder is
/// rejected by the variant chain), this returns `Err` for every real disc today
/// — byte-for-byte identical to `resolve_keys_v21` path 1, which the resolver
/// also leaves unreachable in production. All variant-chain failures collapse to
/// [`Error::AacsMkUnavailable`] (E7018): no numeric distinction is load-bearing
/// at this boundary, and the variant error carries no English to preserve.
pub fn mk_from_dk(
    device_keys: &[DeviceKey],
    mkb: &[u8],
    vid: Vid,
) -> Result<MediaKey, crate::error::Error> {
    let records = walk_mkb(mkb);
    match derive_media_key_variant(
        &records,
        device_keys,
        &KEY_CORRECTION_DATA_PLACEHOLDER,
        &vid.0,
    ) {
        Ok((km, _kvu)) => Ok(MediaKey(km)),
        Err(_) => Err(crate::error::Error::AacsMkUnavailable),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::decrypt::aes_ecb_encrypt;
    use crate::aacs::keys::{decrypt_unit_key, derive_vuk};

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

    /// `mk_from_dk` returns `Err(AacsMkUnavailable)` for the placeholder-KCD
    /// path that production also leaves unreachable — never a wrong key, never a
    /// panic — on both an empty MKB and a non-variant MKB.
    #[test]
    fn mk_from_dk_errors_without_integrator_kcd() {
        let dk = DeviceKey {
            key: [0x11; 16],
            node: 1,
            uv: 1,
            u_mask_shift: 0,
        };
        // Empty MKB → not a variant MKB → Err.
        let e = mk_from_dk(std::slice::from_ref(&dk), &[], Vid([0x09; 16]));
        assert!(matches!(e, Err(crate::error::Error::AacsMkUnavailable)));

        // A variant-looking MKB (0x82 record) still cannot complete without the
        // integrator KCD, so it also errors — never silently yields a key.
        let mut mkb: Vec<u8> = Vec::new();
        mkb.extend_from_slice(&[0x82, 0x00, 0x00, 0x14]); // variant data record
        mkb.extend_from_slice(&[0xAB; 16]);
        let e2 = mk_from_dk(&[dk], &mkb, Vid([0x09; 16]));
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
}

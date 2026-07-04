//! Volume-key layer: derive the Volume Unique Key from the Media Key + Volume
//! ID, unwrap unit keys with it. [PR] §3.3 / §3.5, [BD] §3.3 / §3.9.3.

use super::crypto::*;
use super::inf::*;

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
/// both classical/v21 resolvers and `boil::resolve_candidate` call this, so the
/// map cannot drift between the player and harvest paths.
pub(crate) fn derive_unit_keys(uk_file: &UnitKeyFile, vuk: &[u8; 16]) -> Vec<(u32, [u8; 16])> {
    uk_file
        .encrypted_keys
        .iter()
        .map(|(num, enc_key)| (*num, decrypt_unit_key(vuk, enc_key)))
        .collect()
}

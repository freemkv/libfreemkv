//! AACS content decryption — aligned-unit / bus decryption and TS verification.
//! The low-level AES primitives it uses live in [`super::crypto`].

#[cfg(test)]
use aes::Aes128;
#[cfg(test)]
use aes::cipher::{Array, KeyInit};

use super::crypto::{aes_cbc_decrypt, aes_cbc_encrypt, aes_ecb_encrypt};
// Only this module's test fixtures build CBC ciphertext by hand now — the
// production paths get the IV from `aes_cbc_encrypt` / `aes_cbc_decrypt`.
#[cfg(test)]
use super::crypto::AACS_IV;

// ── AACS constants ──────────────────────────────────────────────────────────

/// Size of an AACS aligned unit (3 × 2048-byte sectors). `[BD]` §3.10.1.
pub const ALIGNED_UNIT_LEN: usize = 6144;

/// An AACS aligned unit spans this many 2048-byte sectors (3).
pub const ALIGNED_UNIT_SECTORS: u32 = (ALIGNED_UNIT_LEN / SECTOR_BYTES) as u32;

/// Whether `lba` sits on an AACS aligned-unit boundary, measured **relative to
/// the encrypted region's base LBA** (`unit_base` = the clip/extent `start_lba`,
/// NOT absolute disc LBA 0). This is the SINGLE source of truth used by the
/// decrypt-on-read gate, the mux read paths, and key validation — never
/// absolute `lba % 3`.
///
/// `saturating_sub` keeps `lba < unit_base` well-defined (clamped to 0, a unit boundary)
/// instead of a `wrapping_sub` mod-3 underflow trap.
pub fn is_unit_aligned(lba: u32, unit_base: u32) -> bool {
    lba.saturating_sub(unit_base)
        .is_multiple_of(ALIGNED_UNIT_SECTORS)
}

use crate::consts::SECTOR_BYTES;

use crate::consts::BD_SOURCE_PACKET_BYTES;

/// TS sync byte.
const TS_SYNC: u8 = 0x47;

// ── Content decryption ──────────────────────────────────────────────────────

/// The AUTHORITATIVE AACS "is this aligned unit encrypted?" signal, per
/// container: for `BdTs` the Copy Permission Indicator in the top 2 bits of
/// byte 0 (`[BD]` §3.10.2, always clear in the unencrypted 16-byte seed —
/// `(buf[0] & 0xC0) == 0` → clear); for `MpegPs` (HD-DVD `.evo`) whether any
/// pack of the slice carries `PES_scrambling_control` `01` (`[HD]` §4.3.2: HD DVD
/// encrypts per pack, so every pack is judged, not the first).
///
/// Readable WITHOUT a key. Only meaningful when `unit` is read at the correct
/// clip-FILE-anchored boundary.
pub fn aacs_unit_encrypted(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    use crate::disc::ContentFormat;
    match format {
        ContentFormat::BdTs => unit.len() >= ALIGNED_UNIT_LEN && (unit[0] & 0xC0) != 0,
        ContentFormat::MpegPs | ContentFormat::DvdPs => any_pack_scrambled(unit),
    }
}

// Whether any pack of `buf` (a trailing partial pack included) is flagged scrambled.
fn any_pack_scrambled(buf: &[u8]) -> bool {
    use super::hddvd::{PackKind, classify};
    buf.chunks(SECTOR_BYTES)
        .any(|p| classify(p) == PackKind::Scrambled)
}

/// The AACS encrypted flag from an aligned unit's CLEAR seed, readable even on a
/// trailing PARTIAL unit (unlike [`aacs_unit_encrypted`], which requires a whole
/// 6144-byte unit for `BdTs`). The flag lives at a fixed low offset in the clear header, so a
/// fragment that still contains that byte can be classified. Used to catch an
/// encrypted unit truncated across a buffer/extent boundary — a fragment we cannot
/// CBC-decrypt and must not emit as clear. `false` for a slice too short to hold
/// the flag byte. Same clip-anchored-read caveat as [`aacs_unit_encrypted`].
pub fn aacs_unit_seed_encrypted(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    use crate::disc::ContentFormat;
    match format {
        ContentFormat::BdTs => unit.first().is_some_and(|b| b & 0xC0 != 0),
        ContentFormat::MpegPs | ContentFormat::DvdPs => any_pack_scrambled(unit),
    }
}

/// Clear the Copy_permission_indicator of every source packet of a unit freemkv has
/// DECRYPTED, so the flag is true of the bytes written (KU design §5.4, K-13). `BdTs`
/// only; HD DVD (`MpegPs`) is excluded (§2.6). Never called on a unit left as ciphertext.
pub(crate) fn clear_copy_permission_indicator(unit: &mut [u8], format: crate::disc::ContentFormat) {
    if format != crate::disc::ContentFormat::BdTs || unit.len() < ALIGNED_UNIT_LEN {
        return;
    }
    // KS-5 [BD] §3.10.2: "shall be set to 11₂ if the data is encrypted, or … 00₂ if the data
    // is not encrypted"; KS-6: CPI is the top 2 of TP_extra_header's 32 bits (ATS kept).
    // KS-22 libaacs `_verify_ts` (corroboration): "buf[i] &= ~0xc0;" per 192-byte packet.
    for packet in unit[..ALIGNED_UNIT_LEN].chunks_mut(BD_SOURCE_PACKET_BYTES) {
        packet[0] &= 0x3F;
    }
}

/// Test helper: `plain` as a decrypt writes it, CPI cleared in every source packet (KS-5;
/// KU design §5.4). For a buffer of whole decrypted units only.
#[cfg(test)]
pub(crate) fn cpi_cleared(mut plain: Vec<u8>) -> Vec<u8> {
    for packet in plain.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        packet[0] &= 0x3F;
    }
    plain
}

/// Was `unit` cut on its file's unit grid? An encrypted BD-TS unit's clear seed
/// always starts a source packet, so its TS sync (0x47) sits at byte 4; a chunk
/// read off the grid starts mid-unit on ciphertext. Clear units, and `MpegPs`
/// (seed layout unverified), always pass. A wrong-grid chunk with CPI=0 also
/// passes (~1 in 4 per unit), so single-unit reads can slip; multi-unit reads are
/// caught almost surely. A damaged seed fails it too: either way the unit opens under
/// no key and readers blank it (`decrypt::blank_damaged_units`); it is never E7013.
pub(crate) fn aacs_unit_on_grid(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    format != crate::disc::ContentFormat::BdTs
        || !aacs_unit_seed_encrypted(unit, format)
        || unit.get(4) == Some(&TS_SYNC)
}

/// True when an aligned unit is flagged encrypted AND still looks scrambled
/// (structure not yet restored) — genuine encrypted content NOT yet decrypted.
///
/// Composes [`aacs_unit_encrypted`] (the authoritative flag) with an IDEMPOTENT "structure
/// restored?" check, so callers that may run twice over the same buffer (re-decrypt, sampling,
/// diagnosis) get a stable answer. A unit freemkv decrypted has its flag CLEARED (KS-5 `[BD]`
/// §3.10.2: "00₂ if the data is not encrypted"; KU design §5.4), so it reads clear here; a
/// unit left as ciphertext keeps it. Only meaningful at the clip-FILE-anchored boundary.
pub fn aacs_unit_needs_decrypt(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    // "Still needs key" = flagged encrypted AND not structurally clean per the
    // ONE definition, [`is_clean`]'s min(E,4) proof floor. Never a second threshold:
    // the old >50% majority false-flagged bad-but-opened units, causing re-sample storms.
    aacs_unit_encrypted(unit, format) && !is_clean(unit, format)
}

// Minimum synced 0x47 packets (of the ~31/unit) that PROVE a key opened a unit: an ABSOLUTE
// proof floor, NOT a proportion (false-pass ≈ C(31,4)*256^-4 ≈ 1e-5/unit).
const KEY_PROOF_PACKETS: usize = 4;

/// Structural "did a key open this content unit?" — the pure, NO-CRYPTO signal
/// for key SELECTION (pick the right key among multiple on a multi-CPS disc) and
/// read VERIFY. AACS has no cryptographic "did the key work" answer (no MAC), so
/// a key is proven STRUCTURALLY: its plaintext must look like valid content for
/// the disc's container. This dispatches to the right container check by
/// `format` — BD/UHD/FMTS are Transport Stream (`is_clean_ts`); HD-DVD `.evo`
/// is Program Stream (`is_clean_ps`). NOT a decryption verdict: a correct key
/// can decrypt structurally-broken content, which is the muxer's concern.
pub fn is_clean(unit: &[u8], format: crate::disc::ContentFormat) -> bool {
    match format {
        crate::disc::ContentFormat::BdTs => is_clean_ts(unit),
        crate::disc::ContentFormat::MpegPs | crate::disc::ContentFormat::DvdPs => is_clean_ps(unit),
    }
}

// Structural "does this unit carry enough valid MPEG-TS to prove a key opened it?" (the mux's
// key-selection/verify signal): synced >= min(E, KEY_PROOF_PACKETS) over non-padding packets.
fn is_clean_ts(unit: &[u8]) -> bool {
    let (content, synced) = ts_packet_counts(unit);
    content == 0 || synced >= content.min(KEY_PROOF_PACKETS)
}

// `(content, synced)`: non-padding packets after packet 0, and those whose sync byte is 0x47.
fn ts_packet_counts(unit: &[u8]) -> (usize, usize) {
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let limit = ALIGNED_UNIT_LEN.min(unit.len());
    let mut content = 0usize;
    let mut synced = 0usize;
    // Skip packet 0: its sync byte lives in the clear 16-byte seed (unencrypted),
    // so it is `0x47` regardless of the key and proves nothing about decryption.
    let mut off = PKT;
    while off + PKT <= limit {
        let payload = &unit[off + 4..off + PKT];
        if !payload.iter().all(|&b| b == 0) {
            content += 1;
            if unit[off + 4] == TS_SYNC {
                synced += 1;
            }
        }
        off += PKT;
    }
    (content, synced)
}

/// Does `unit_key` open this encrypted BD-TS aligned `unit`? One bit, never plaintext.
/// `true` only for a whole 6144-byte unit flagged encrypted whose plaintext would carry
/// at least `KEY_PROOF_PACKETS` content packets, all synced: anything else proves nothing
/// and is `false` (other formats, short or clear units, too little content). A wrong key
/// passes about once in 10^5 units, so confirm a match on several units.
///
/// Behind the `keyproof` feature so the default public API still cannot decrypt AACS: a
/// key oracle, not a decryptor (KU §2.2; see `keys::public_api_cannot_decrypt_aacs`).
#[cfg(feature = "keyproof")]
pub fn unit_key_opens(
    unit: &[u8],
    unit_key: &[u8; 16],
    format: crate::disc::ContentFormat,
) -> bool {
    use crate::disc::ContentFormat;
    if !matches!(format, ContentFormat::BdTs)
        || unit.len() != ALIGNED_UNIT_LEN
        || !aacs_unit_encrypted(unit, format)
    {
        return false;
    }
    let (content, synced) = ts_unit_key_proof(unit, unit_key);
    content >= KEY_PROOF_PACKETS && synced >= KEY_PROOF_PACKETS
}

/// [`is_clean_ts`]'s verdict for `decrypt_unit(unit, unit_key)`, from the packet counts of
/// `ts_unit_key_proof`.
#[cfg(test)]
fn ts_unit_key_opens(unit: &[u8], unit_key: &[u8; 16]) -> bool {
    let (content, synced) = ts_unit_key_proof(unit, unit_key);
    content == 0 || synced >= content.min(KEY_PROOF_PACKETS)
}

/// The `(content, synced)` counts `ts_packet_counts` would see after `decrypt_unit`, from
/// one AES block per source packet. CBC decrypts any block alone
/// (`P[i] = AES_dec(C[i]) ⊕ C[i-1]`), and packet heads sit at block-aligned offsets ≥ 192.
/// Packet 0 is skipped (its sync byte is in the clear seed), as are all-zero padding
/// packets, which `decrypt_unit` zeroes.
#[cfg(any(test, feature = "keyproof"))]
fn ts_unit_key_proof(unit: &[u8], unit_key: &[u8; 16]) -> (usize, usize) {
    use aes::cipher::{Array, BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};
    use aes::{Aes128, Block};
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    const NPKT: usize = ALIGNED_UNIT_LEN / PKT;
    debug_assert_eq!(unit.len(), ALIGNED_UNIT_LEN);
    let block_at =
        |o: usize| -> Block { Array::try_from(&unit[o..o + 16]).expect("16-byte block") };

    // Block key = AES-128E(unit_key, seed) ⊕ seed — `decrypt_unit`'s derivation.
    let mut bk = block_at(0);
    Aes128::new(&(*unit_key).into()).encrypt_block(&mut bk);
    for (b, s) in bk.iter_mut().zip(&unit[..16]) {
        *b ^= s;
    }
    let cipher = Aes128::new(&bk);
    // Plaintext of the 16-byte block at `o` (o ≥ 192, so C[i-1] is ciphertext).
    let plain = |o: usize| {
        let mut b = block_at(o);
        cipher.decrypt_block(&mut b);
        for (x, c) in b.iter_mut().zip(&unit[o - 16..o]) {
            *x ^= c;
        }
        b
    };

    // Batch the head block of every non-padding packet (the backend pipelines it).
    let mut offs = [0usize; NPKT];
    let mut heads = [Block::default(); NPKT];
    let mut n = 0;
    for p in 1..NPKT {
        let off = p * PKT;
        if unit[off..off + PKT].iter().all(|&b| b == 0) {
            continue; // padding — decrypt_unit restores it to zeros: not content
        }
        offs[n] = off;
        heads[n] = block_at(off);
        n += 1;
    }
    cipher.decrypt_blocks(&mut heads[..n]);

    let (mut content, mut synced) = (0usize, 0usize);
    for (head, &off) in heads[..n].iter().zip(&offs[..n]) {
        match head[4] ^ unit[off - 16 + 4] {
            TS_SYNC => {
                content += 1;
                synced += 1;
            }
            0 => {
                // Sync byte decrypted to 0x00: content only if the rest of the
                // payload isn't all zero too.
                let first = plain(off);
                let nonzero = first[4..].iter().any(|&b| b != 0)
                    || (1..PKT / 16).any(|j| plain(off + 16 * j).iter().any(|&b| b != 0));
                if nonzero {
                    content += 1;
                }
            }
            _ => content += 1,
        }
    }
    (content, synced)
}

/// Count the MPEG-TS sync bytes (`0x47`) present at the BD-TS packet stride
/// (offset 4 and every 192 bytes after — 4-byte TP_extra_header + 188-byte
/// TS packet). A clear or correctly-decrypted m2ts unit shows ~one per
/// packet; an encrypted unit, or a non-content unit decrypted under a key
/// that doesn't apply, shows ~none.
pub fn ts_sync_count(unit: &[u8]) -> usize {
    let mut count = 0;
    let mut offset = 4;
    while offset < unit.len() {
        if unit[offset] == TS_SYNC {
            count += 1;
        }
        offset += BD_SOURCE_PACKET_BYTES;
    }
    count
}

/// Number of BD-TS packets in the unit — the maximum possible sync count.
pub fn ts_packet_total(unit: &[u8]) -> usize {
    // One sync byte per 192-byte BD-TS packet (at offset 4 of each). The old
    // `(len - 4) / BD_SOURCE_PACKET_BYTES + 1` over-counted by one for lengths of the
    // form `4 + k·192`.
    unit.len() / BD_SOURCE_PACKET_BYTES
}

// The Program-Stream arm of [`is_clean`] (HD-DVD `.evo`): no pack still flagged scrambled and
// none failing its payload check past the clear head (`hddvd::payload_check`). The pack start
// code proves nothing: an encrypted pack keeps it in its clear 128-byte head (`[HD]` §4.3.2).
fn is_clean_ps(unit: &[u8]) -> bool {
    !any_pack_scrambled(unit)
        && unit.as_chunks::<SECTOR_BYTES>().0.iter().all(|p| {
            p[..4] == [0x00, 0x00, 0x01, 0xBA] && super::hddvd::payload_check(p) != Some(false)
        })
}

/// Decrypt one AACS aligned unit (6144 bytes) IN PLACE — PURE crypto that
/// applies `unit_key`; makes no verdict about whether the plaintext is clean
/// TS (that's the separate [`is_clean_ts`] question, for key SELECTION or a
/// read VERIFY).
///
/// Applies the key UNCONDITIONALLY: does NOT check the encrypted-flag, so the CALLER must gate
/// on [`aacs_unit_encrypted`] first. Block Key = AES-128E(Kcu, seed) ⊕ seed, then AES-128-CBC
/// decrypt bytes 16..6144 under the AACS IV.
pub(crate) fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
    if unit.len() < ALIGNED_UNIT_LEN {
        return;
    }
    const PKT: usize = BD_SOURCE_PACKET_BYTES; // 192
    let npkt = ALIGNED_UNIT_LEN / PKT;
    let mut pad = [false; ALIGNED_UNIT_LEN / PKT];
    for (p, slot) in pad.iter_mut().enumerate().take(npkt) {
        let off = p * PKT;
        *slot = unit[off..off + PKT].iter().all(|&b| b == 0);
    }

    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut decrypt_key = [0u8; 16];
    for i in 0..16 {
        decrypt_key[i] = derived[i] ^ header[i];
    }
    aes_cbc_decrypt(&decrypt_key, &mut unit[16..ALIGNED_UNIT_LEN]);

    for (p, &is_pad) in pad.iter().enumerate().take(npkt) {
        if is_pad {
            let off = p * PKT;
            for b in unit[off..off + PKT].iter_mut() {
                *b = 0;
            }
        }
    }
}

/// Encrypt one AACS aligned unit (6144 bytes) IN PLACE — the exact inverse of `decrypt_unit`.
/// Caller must set the encrypted flag BEFORE calling and check the returned bool.
#[must_use = "returns false when the slice is too short to encrypt, leaving \
              plaintext behind a flag that already says 'encrypted'"]
pub fn encrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) -> bool {
    if unit.len() < ALIGNED_UNIT_LEN {
        return false;
    }
    let mut header = [0u8; 16];
    header.copy_from_slice(&unit[..16]);
    let derived = aes_ecb_encrypt(unit_key, &header);
    let mut k = [0u8; 16];
    for i in 0..16 {
        k[i] = derived[i] ^ header[i];
    }
    // CBC-encrypt bytes 16.. under the fixed AACS IV — the exact forward of the
    // `aes_cbc_decrypt` call in `decrypt_unit`, and one key expansion for the whole
    // unit rather than one per 16-byte block.
    aes_cbc_encrypt(&k, &mut unit[16..ALIGNED_UNIT_LEN]);
    true
}

/// Remove bus encryption from an aligned unit (AACS 2.0 / UHD).
/// Bus encryption uses read_data_key, decrypting bytes 16..2048 of each 2048-byte sector.
/// Test-only unit-granular form; production de-busses via [`decrypt_bus_in_content`].
#[cfg(test)]
pub(crate) fn decrypt_bus(unit: &mut [u8], read_data_key: &[u8; 16]) {
    // Expand the key schedule ONCE for the whole unit: `read_data_key` is
    // loop-invariant, but per-sector `aes_cbc_decrypt` rebuilt it 3x per unit
    // (~29M redundant expansions over a 90GB read) on the decrypt hot path.
    let cipher = crate::aacs::crypto::new_cipher_for(read_data_key);
    for sector_start in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
        if sector_start + SECTOR_BYTES > unit.len() {
            break;
        }
        // First 16 bytes of each sector are plaintext
        crate::aacs::crypto::cbc_decrypt_blocks(
            &cipher,
            &mut unit[sector_start + 16..sector_start + SECTOR_BYTES],
        );
    }
}

/// Remove bus encryption from every whole 2048-byte sector of `buf` (bytes
/// 16..2048 of each; the first 16 are plaintext on the wire). For content-only
/// callers; whole-disc readers gate through `sector::bus_removal::BusMap`.
pub(crate) fn decrypt_bus_sectors(buf: &mut [u8], read_data_key: &[u8; 16]) {
    let cipher = crate::aacs::crypto::new_cipher_for(read_data_key);
    for sector in buf.as_chunks_mut::<SECTOR_BYTES>().0 {
        crate::aacs::crypto::cbc_decrypt_blocks(&cipher, &mut sector[16..]);
    }
}

/// Test-only exact inverse of [`decrypt_bus`]: apply AACS 2.0 bus ENCRYPTION to
/// an aligned unit in place — per-sector AES-CBC-encrypt bytes 16..2048 of each
/// 2048-byte sector under `read_data_key`, leaving the first 16 bytes of every
/// sector clear (they are plaintext on the wire, as `decrypt_bus` relies on).
/// Used to build a bus-encrypted fixture that `decrypt_bus` must recover, and by
/// the `decrypt.rs` end-to-end tests to model the drive's forward transform.
#[cfg(test)]
pub(crate) fn encrypt_bus(unit: &mut [u8], read_data_key: &[u8; 16]) {
    for sector_start in (0..ALIGNED_UNIT_LEN).step_by(SECTOR_BYTES) {
        if sector_start + SECTOR_BYTES > unit.len() {
            break;
        }
        crate::aacs::crypto::aes_cbc_encrypt(
            read_data_key,
            &mut unit[sector_start + 16..sector_start + SECTOR_BYTES],
        );
    }
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;

// Spec guards (keys-upfront-design §7.8) on the unit decrypt; here because
// `decrypt_unit` is crate-private (KU §2.2). Vectors come from the `aes` crate.
#[cfg(test)]
#[path = "content_spec_guards_tests.rs"]
mod spec_guards;

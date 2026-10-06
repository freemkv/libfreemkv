//! HD DVD pack encryption: `[HD]` = AACS "HD DVD and DVD Pre-recorded Book", Final Rev 0.952.
//!
//! An EVOB is encrypted per 2048-byte pack (`[HD]` §4.3.2): bytes 0..128 are the Unencrypted
//! Portion, bytes 128..2048 the Encrypted Portion, CBC-encrypted under the AACS IV with the
//! Content Key `Kc = AES-G(Kt, Dtk || CPI_lsb96)`. `Dtk` is pack bytes 84..88 (Table 4-7).
//! `Kt` is the Title Key that `TITLE_KEY_PTR` of the EVOBU's CPI names (Table 4-2), and the CPI
//! rides in the GCI packet of the NV_PCK that leads every EVOBU. A pack is encrypted when its
//! `PES_scrambling_control` is `01` (§4.3.2); an HL_PCK is always encrypted while KEY_VF is
//! `01` or `10` (§4.3.2). NV_PCK and ADV_PCK are never encrypted (§4.3.1).

use super::crypto::{aes_cbc_decrypt, aes_g};
use crate::consts::SECTOR_BYTES;

/// One HD DVD pack: a 2048-byte sector.
pub(crate) const PACK_LEN: usize = SECTOR_BYTES;
/// `[HD]` §4.3.2: "the first 128 bytes are called the Unencrypted Portion".
pub(crate) const CLEAR_LEN: usize = 128;
// `[HD]` Table 4-7: Title Key Data (Dtk) is bytes 84..=87 of the pack.
const DTK_OFF: usize = 84;
const PACK_START: [u8; 4] = [0x00, 0x00, 0x01, 0xBA];
const SYSTEM_HEADER: u8 = 0xBB;
const PRIVATE_STREAM_1: u8 = 0xBD;
const PADDING: u8 = 0xBE;
const PRIVATE_STREAM_2: u8 = 0xBF;
// private_stream_2 sub_stream_ids: the GCI packet of an NV_PCK, and an HL_PCK.
const GCI_SUB_STREAM_ID: u8 = 0x04;
const HLI_SUB_STREAM_ID: u8 = 0x08;
// The CPI is 16 bytes at offset 12 of the GCI data (after its sub_stream_id). The HD DVD-Video
// book that fixes it is not public; measured on encrypted discs (pack offset 60 of an NV_PCK).
const CPI_IN_GCI: usize = 12;
const SCRAMBLE_MASK: u8 = 0x30;

/// The 16-byte Content Protection Information of an EVOBU (`[HD]` §4.2, Tables 4-1 and 4-2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Cpi(pub(crate) [u8; 16]);

impl Cpi {
    /// `KEY_VF` (Table 4-2): `0b10` the Title Key Pointer is valid, `0b01` the Segment Key
    /// Pointer, `0b00` neither.
    pub(crate) fn key_vf(&self) -> u8 {
        self.0[0] >> 6
    }

    /// `TITLE_KEY_PTR` (Table 4-2): the 1-based Title Key entry of the Title Key File.
    pub(crate) fn title_key_ptr(&self) -> u32 {
        u32::from(u16::from_be_bytes([self.0[1], self.0[2]]))
    }
}

/// What a pack is, as far as decryption is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PackKind {
    /// An NV_PCK, with the CPI of its GCI packet when one parses.
    Nav(Option<Cpi>),
    /// A PES pack whose `PES_scrambling_control` is not `00`.
    Scrambled,
    /// An HL_PCK: encrypted whenever its EVOBU's KEY_VF is `01` or `10`.
    Highlight,
    /// Anything else: never encrypted.
    Clear,
}

// Offset of the first packet after an MPEG-2 pack header (with its stuffing), or `None`.
fn first_packet(pack: &[u8]) -> Option<usize> {
    if pack.len() < 14 || pack[..4] != PACK_START || pack[4] & 0xC0 != 0x40 {
        return None;
    }
    Some(14 + usize::from(pack[13] & 0x07))
}

// Does stream id `sid` carry the optional PES header (with PES_scrambling_control)?
fn has_pes_flags(sid: u8) -> bool {
    sid >= PRIVATE_STREAM_1 && !matches!(sid, PADDING | PRIVATE_STREAM_2)
}

/// Classify one pack. A slice shorter than a pack is judged on the bytes it holds.
pub(crate) fn classify(pack: &[u8]) -> PackKind {
    let Some(o) = first_packet(pack) else {
        return PackKind::Clear;
    };
    if pack.len() < o + 7 || pack[o..o + 3] != [0, 0, 1] {
        return PackKind::Clear;
    }
    match pack[o + 3] {
        SYSTEM_HEADER => PackKind::Nav(nav_cpi(pack, o)),
        PRIVATE_STREAM_2 if pack[o + 6] == HLI_SUB_STREAM_ID => PackKind::Highlight,
        sid if has_pes_flags(sid) && pack[o + 6] & 0xC0 == 0x80 => {
            match pack[o + 6] & SCRAMBLE_MASK {
                0 => PackKind::Clear,
                _ => PackKind::Scrambled,
            }
        }
        _ => PackKind::Clear,
    }
}

// The CPI of the GCI packet in an NV_PCK whose first packet (the system header) is at `o`.
fn nav_cpi(pack: &[u8], mut q: usize) -> Option<Cpi> {
    while q + 7 <= pack.len() && pack[q..q + 3] == [0, 0, 1] {
        let len = usize::from(u16::from_be_bytes([pack[q + 4], pack[q + 5]]));
        if pack[q + 3] == PRIVATE_STREAM_2 && pack[q + 6] == GCI_SUB_STREAM_ID {
            if len < 1 + CPI_IN_GCI + 16 {
                return None;
            }
            let at = q + 7 + CPI_IN_GCI;
            return Some(Cpi(pack.get(at..at + 16)?.try_into().ok()?));
        }
        q += 6 + len;
    }
    None
}

/// Whether `pack` must be decrypted under an EVOBU whose CPI is `cpi`.
pub(crate) fn needs_key(kind: PackKind, cpi: Option<&Cpi>) -> bool {
    match kind {
        PackKind::Scrambled => true,
        PackKind::Highlight => cpi.is_some_and(|c| matches!(c.key_vf(), 0b01 | 0b10)),
        PackKind::Nav(_) | PackKind::Clear => false,
    }
}

/// `[HD]` §4.3.2: `Kc = AES-G(Kt, Dtk || CPI_lsb96)`, `Dtk` = pack bytes 84..88.
pub(crate) fn content_key(title_key: &[u8; 16], pack: &[u8], cpi: &Cpi) -> [u8; 16] {
    let mut d = [0u8; 16];
    d[..4].copy_from_slice(&pack[DTK_OFF..DTK_OFF + 4]);
    d[4..].copy_from_slice(&cpi.0[4..]);
    aes_g(title_key, &d)
}

/// Decrypt one whole pack in place (`[HD]` §4.3.4 step 3) and mark a PES pack unscrambled.
/// Bytes 0..128 are never touched but for that flag. A short slice is left alone.
pub(crate) fn decrypt_pack(pack: &mut [u8], title_key: &[u8; 16], cpi: &Cpi) {
    if pack.len() != PACK_LEN {
        return;
    }
    let kc = content_key(title_key, pack, cpi);
    aes_cbc_decrypt(&kc, &mut pack[CLEAR_LEN..]);
    clear_scrambling(pack);
}

// `[HD]` §4.3.2: "Otherwise, the PES_scrambling_control shall be 00₂": true of the plaintext.
fn clear_scrambling(pack: &mut [u8]) {
    if let Some(o) = first_packet(pack)
        && pack.len() >= o + 7
        && has_pes_flags(pack[o + 3])
        && pack[o + 6] & 0xC0 == 0x80
    {
        pack[o + 6] &= !SCRAMBLE_MASK;
    }
}

/// The exact inverse of [`decrypt_pack`] for test fixtures: sets the scrambling flag of a PES
/// pack, then CBC-encrypts bytes 128..2048 (`[HD]` §4.3.2, `Ce = AES-128CBCE(Kc, C)`).
#[cfg(test)]
pub(crate) fn encrypt_pack(pack: &mut [u8], title_key: &[u8; 16], cpi: &Cpi) {
    if let Some(o) = first_packet(pack)
        && has_pes_flags(pack[o + 3])
    {
        pack[o + 6] = (pack[o + 6] & !SCRAMBLE_MASK) | 0x10;
    }
    let kc = content_key(title_key, pack, cpi);
    super::crypto::aes_cbc_encrypt(&kc, &mut pack[CLEAR_LEN..]);
}

/// Structural proof that a pack's bytes past 128 are plaintext, for key proof and verify.
///
/// `Some(false)` when a test that random bytes pass about once in 2^16 fails: a PES or padding
/// start code where the chain of packet lengths puts one at or past byte 128, or the AC-3 /
/// E-AC-3 sync word where a `private_stream_1` first-access-unit pointer lands at or past it.
/// `Some(true)` when such a test ran and passed, `None` when none applied (most video packs,
/// whose one PES fills the pack).
pub(crate) fn payload_check(pack: &[u8]) -> Option<bool> {
    if pack.len() != PACK_LEN {
        return None;
    }
    let mut q = first_packet(pack)?;
    let mut tested = false;
    while q + 6 <= PACK_LEN {
        if pack[q..q + 3] != [0, 0, 1] {
            // A broken chain in the clear head proves nothing about the key.
            return (q >= CLEAR_LEN).then_some(false).or(tested.then_some(true));
        }
        tested |= q >= CLEAR_LEN;
        let len = usize::from(u16::from_be_bytes([pack[q + 4], pack[q + 5]]));
        if pack[q + 3] == PRIVATE_STREAM_1 {
            match ac3_sync_check(pack, q, len) {
                Some(false) => return Some(false),
                Some(true) => tested = true,
                None => {}
            }
        }
        q += 6 + len;
    }
    tested.then_some(true)
}

// The AC-3 / E-AC-3 sync word at the first access unit of a private_stream_1 packet at `q`.
// Sub-stream ids 0x80-0x87 (AC-3) and 0xC0-0xC7 (E-AC-3); the pointer counts from its own
// last byte, as on DVD.
fn ac3_sync_check(pack: &[u8], q: usize, len: usize) -> Option<bool> {
    if pack.get(q + 6)? & 0xC0 != 0x80 {
        return None;
    }
    let hd = q + 9 + usize::from(*pack.get(q + 8)?);
    let sub = *pack.get(hd)?;
    if !matches!(sub, 0x80..=0x87 | 0xC0..=0xC7) || hd + 4 > PACK_LEN {
        return None;
    }
    let ptr = usize::from(u16::from_be_bytes([pack[hd + 2], pack[hd + 3]]));
    let at = hd + 3 + ptr;
    let end = (q + 6 + len).min(PACK_LEN);
    (ptr != 0 && at >= CLEAR_LEN && at + 2 <= end).then(|| pack[at..at + 2] == [0x0B, 0x77])
}

#[cfg(test)]
#[path = "hddvd_tests.rs"]
pub(crate) mod tests;

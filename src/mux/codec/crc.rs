//! Bit-exact CRC helpers shared by the audio codec decodability gates.
//!
//! Each matches the CRC defined by its format's bitstream specification, so a
//! frame these routines flag as a CRC mismatch is exactly the frame a
//! spec-conformant decoder would reject. All are MSB-first (non-reflected),
//! init 0, no final XOR — the big-endian CRC variants. Each format transmits
//! its CRC so that the residue over `data + transmitted_crc` is zero, which is
//! exactly how these are used: compute over the whole frame (including its
//! trailing CRC) and check `== 0`.

/// CRC-16/ANSI (a.k.a. CRC-16/BUYPASS): polynomial 0x8005, init 0x0000,
/// MSB-first, no reflection, no final XOR. Called by the AC-3/E-AC-3 frame-CRC
/// gate (ETSI TS 102 366) and the FLAC frame footer. (The MPEG-audio and
/// AAC-ADTS gates validate the header structurally and do not verify their
/// optional CRC, so they do not call this.)
pub(crate) fn crc16_ansi(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x8005
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// CRC-16 with polynomial 0x002D, init 0, MSB-first, used by the MLP / Dolby
/// TrueHD major-sync header checksum.
///
/// NOTE: MLP's checksum is the "reversed" scheme. This function emits its two
/// bytes in the OPPOSITE order to a standard little-endian CRC readout, so the
/// caller swaps them back and compares against the stored trailer word read
/// LITTLE-endian — see `truehd::mlp_major_sync_crc_ok`, which is authoritative.
///
/// Comparing big-endian instead is precisely the bug that function was fixed
/// for: it could never validate a real extended major sync, so whole TrueHD
/// tracks were dropped silently. This comment used to prescribe exactly that,
/// and to point at a `truehd::mlp_major_sync_ok` that does not exist.
/// Verified against real MLP/TrueHD bitstreams (225/225 major-sync AUs).
pub(crate) fn crc16_mlp(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x002D
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// CRC-8/ATM (a.k.a. CRC-8/ITU without the final XOR): polynomial 0x07, init 0,
/// MSB-first, no reflection — the FLAC frame-header CRC-8 (RFC 9639). Available
/// as a primitive; the FLAC gate currently validates only the frame footer CRC-16.
pub(crate) fn crc8_atm(data: &[u8]) -> u8 {
    let mut crc: u8 = 0;
    for &b in data {
        crc ^= b;
        for _ in 0..8 {
            crc = if crc & 0x80 != 0 {
                (crc << 1) ^ 0x07
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_residue_property_holds() {
        // Appending the big-endian CRC-16 of a message zeroes the residue over
        // message+crc — the property every frame gate relies on.
        let msg = [0x12u8, 0x34, 0x56, 0x78, 0x9A];
        let c = crc16_ansi(&msg);
        let mut framed = msg.to_vec();
        framed.push((c >> 8) as u8);
        framed.push((c & 0xFF) as u8);
        assert_eq!(crc16_ansi(&framed), 0);
    }

    #[test]
    fn crc16_known_vector_check_bytes() {
        // CRC-16/BUYPASS check value for the ASCII string "123456789" is 0xFEE8
        // (the standard catalogue check value for poly 0x8005, init 0).
        assert_eq!(crc16_ansi(b"123456789"), 0xFEE8);
    }

    #[test]
    fn crc16_mlp_known_vector_check_bytes() {
        // Independent known-answer for CRC-16 poly 0x002D, init 0, MSB-first over
        // the catalogue string "123456789" is 0x4FF7 — computed by a separate
        // reference implementation (NOT by crc16_mlp), so a wrong polynomial or
        // shift direction here fails this test even though every truehd fixture
        // (which derives its trailer from crc16_mlp itself) would still pass.
        assert_eq!(crc16_mlp(b"123456789"), 0x4FF7);
        assert_eq!(crc16_mlp(&[0x00, 0x01, 0x02, 0x03]), 0x5E26);
    }

    #[test]
    fn crc16_mlp_residue_property_holds() {
        // Appending the big-endian CRC zeroes the residue over message+crc.
        // This is a property of the CRC itself, pinned here so a change to the
        // polynomial or the bit order is caught. It is NOT how the TrueHD
        // caller validates a major sync: `truehd::mlp_major_sync_crc_ok` does a
        // swap-and-XOR compare against the little-endian trailer word. (This
        // comment used to claim the caller relied on the residue, and named a
        // `truehd::mlp_major_sync_ok` that does not exist.)
        let msg = [0xF8u8, 0x72, 0x6F, 0xBA];
        let c = crc16_mlp(&msg);
        let mut framed = msg.to_vec();
        framed.push((c >> 8) as u8);
        framed.push((c & 0xFF) as u8);
        assert_eq!(crc16_mlp(&framed), 0);
    }

    #[test]
    fn crc8_residue_property_holds() {
        // Appending the CRC-8 of a message zeroes the residue over message+crc —
        // how FLAC's header CRC-8 is verified.
        let msg = [0xDEu8, 0xAD, 0xBE, 0xEF];
        let c = crc8_atm(&msg);
        let mut framed = msg.to_vec();
        framed.push(c);
        assert_eq!(crc8_atm(&framed), 0);
    }

    #[test]
    fn crc8_known_vector_check_byte() {
        // CRC-8/SMBUS (poly 0x07, init 0, no reflection) check value for
        // "123456789" is 0xF4 — the catalogue check value.
        assert_eq!(crc8_atm(b"123456789"), 0xF4);
    }
}

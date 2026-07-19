//! Bit-exact CRC helpers shared by the audio codec decodability gates.
//!
//! Both match ffmpeg's `av_crc` tables so a frame that ffmpeg's decoder would
//! flag as a CRC mismatch is flagged identically here. All are MSB-first
//! (non-reflected), init 0, no final XOR — the ffmpeg `AV_CRC_*` (big-endian)
//! variants. Each format transmits its CRC so that the residue over
//! `data + transmitted_crc` is zero, which is exactly how these are used:
//! compute over the whole frame (including its trailing CRC) and check `== 0`.

/// CRC-16/ANSI (a.k.a. CRC-16/BUYPASS): polynomial 0x8005, init 0x0000,
/// MSB-first, no reflection, no final XOR — ffmpeg `AV_CRC_16_ANSI`.
/// Used by AC-3/E-AC-3 (frame CRC), FLAC (frame footer), MPEG-audio and
/// AAC-ADTS (header CRC).
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

/// CRC-16 with polynomial 0x002D, init 0, MSB-first — ffmpeg's `crc_2D` table
/// (`av_crc_init(crc_2D, 0, 16, 0x002D)`), used by the MLP/TrueHD major-sync
/// header checksum. NOTE: MLP's checksum is the "reversed" scheme — ffmpeg
/// computes `av_crc(...) ^ AV_RL16(trailer)` and compares against `AV_RL16` of
/// the stored word; equivalently, this standard CRC compared against the stored
/// bytes read big-endian. The caller handles that comparison
/// (see `truehd::mlp_major_sync_ok`). Verified against real ffmpeg TrueHD
/// output (225/225 major-sync AUs).
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
/// MSB-first, no reflection — ffmpeg `AV_CRC_8_ATM`. Used by the FLAC frame
/// header.
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

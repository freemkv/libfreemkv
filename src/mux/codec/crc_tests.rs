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
    // Independent known-answer (poly 0x002D, init 0, MSB-first) from a separate
    // reference implementation, not crc16_mlp itself — catches a wrong
    // polynomial/shift that truehd fixtures (self-derived) would miss.
    assert_eq!(crc16_mlp(b"123456789"), 0x4FF7);
    assert_eq!(crc16_mlp(&[0x00, 0x01, 0x02, 0x03]), 0x5E26);
}

#[test]
fn crc16_mlp_residue_property_holds() {
    // Appending the big-endian CRC zeroes the residue (pinned to catch poly/bit
    // changes). NOT how TrueHD validates — mlp_major_sync_crc_ok instead does a
    // swap-and-XOR compare against the little-endian trailer word.
    let msg = [0xF8u8, 0x72, 0x6F, 0xBA];
    let c = crc16_mlp(&msg);
    let mut framed = msg.to_vec();
    framed.push((c >> 8) as u8);
    framed.push((c & 0xFF) as u8);
    assert_eq!(crc16_mlp(&framed), 0);
}

// The bit-at-a-time register update the tables replace, kept as the reference.
fn bitwise(poly: u16, data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ poly
            } else {
                crc << 1
            };
        }
    }
    crc
}

// The table-driven CRCs equal the bitwise definition on every length up to 300 bytes
// of varied data, and on every single byte value.
#[test]
fn table_crcs_match_the_bitwise_definition() {
    let data: Vec<u8> = (0..300u32)
        .map(|i| (i * 167 + 13) as u8 ^ (i >> 3) as u8)
        .collect();
    for n in 0..=data.len() {
        assert_eq!(
            crc16_ansi(&data[..n]),
            bitwise(0x8005, &data[..n]),
            "ansi {n}"
        );
        assert_eq!(
            crc16_mlp(&data[..n]),
            bitwise(0x002D, &data[..n]),
            "mlp {n}"
        );
    }
    for b in 0..=255u8 {
        assert_eq!(crc16_ansi(&[b, !b]), bitwise(0x8005, &[b, !b]));
        assert_eq!(crc16_mlp(&[b]), bitwise(0x002D, &[b]));
    }
}

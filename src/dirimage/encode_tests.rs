use super::*;

// Reference check value for CRC-16/XMODEM: poly 0x1021 seeded at 0
// (ECMA-167 7.2.4): "123456789" -> 0x31C3. CCITT-FALSE (seed 0xFFFF)
// gives 0x29B1, a mutant udf.rs wouldn't catch — it never verifies a CRC.
#[test]
fn crc16_matches_the_ecma167_check_value() {
    assert_eq!(crc16(b"123456789"), 0x31C3);
}

/// ECMA-167 3/7.2.3: the checksum is the sum of the tag's first 16 bytes
/// EXCLUDING the checksum byte itself, modulo 256.
#[test]
fn tag_checksum_excludes_its_own_byte() {
    let mut buf = [0u8; 512];
    buf[16..24].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    finish_tag(&mut buf, 261, 0x1234, 512);
    let sum: u32 = buf[0..16]
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4)
        .map(|(_, b)| *b as u32)
        .sum();
    assert_eq!(buf[4] as u32, sum % 256);
    // And the recorded CRC covers the body, not the tag.
    let crc = u16::from_le_bytes([buf[8], buf[9]]);
    assert_eq!(crc, crc16(&buf[16..512]));
    assert_eq!(u16::from_le_bytes([buf[10], buf[11]]), 496);
    assert_eq!(
        u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        0x1234
    );
}

/// ASCII takes compression ID 8; anything above takes 16 (UTF-16BE),
/// because `parse_udf_name` decodes compression-8 bytes as Latin-1.
#[test]
fn cs0_picks_the_encoding_the_parser_can_decode() {
    assert_eq!(encode_cs0("AB"), vec![8, b'A', b'B']);
    let e = encode_cs0("Ä");
    assert_eq!(e[0], 16);
    assert_eq!(&e[1..], &[0x00, 0xC4]);
    assert_eq!(crate::udf::parse_udf_name(&e), "Ä");
}

/// A d-string records its used length in the field's LAST byte, and the
/// production parser must read the same string back.
#[test]
fn dstring_round_trips_through_the_production_parser() {
    let mut field = [0u8; 32];
    put_dstring(&mut field, "FREEMKV");
    assert_eq!(field[31], 8, "compid byte + 7 characters");
    assert_eq!(crate::udf::parse_dstring_for_test(&field), "FREEMKV");
}

/// L089: the PVD's interchange level must be 3 (not 2) — the level that
/// permits a file to span multiple extents, needed for a large VOB/M2TS
/// on a hybrid video disc. `file_set` already writes 3 for the same reason.
#[test]
fn primary_volume_interchange_level_permits_multi_extent_files() {
    let pvd = primary_volume("FREEMKV", 16, 0);
    assert_eq!(
        (
            u16::from_le_bytes([pvd[60], pvd[61]]),
            u16::from_le_bytes([pvd[62], pvd[63]]),
        ),
        (3, 3),
        "PVD interchange level / max must be 3, permitting multi-extent files"
    );
}

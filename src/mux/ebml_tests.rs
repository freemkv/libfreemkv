use super::*;
use std::io::Cursor;

#[test]
fn test_write_size() {
    let mut buf = Vec::new();
    write_size(&mut buf, 0).unwrap();
    assert_eq!(buf, [0x80]);

    buf.clear();
    write_size(&mut buf, 127).unwrap();
    assert_eq!(buf, [0x40, 127]); // 127 >= 0x7F, uses 2 bytes: (0>>8)|0x40, 127

    buf.clear();
    write_size(&mut buf, 126).unwrap();
    assert_eq!(buf, [126 | 0x80]); // 126 < 0x7F, uses 1 byte
}

#[test]
fn write_size_rejects_unknown_size_sentinel() {
    // 0x00FF_FFFF_FFFF_FFFF would encode byte-for-byte identical to the
    // EBML unknown-size marker; it must be rejected, not silently emitted.
    let mut buf = Vec::new();
    let e = write_size(&mut buf, 0x00FF_FFFF_FFFF_FFFF).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    assert!(buf.is_empty(), "no bytes should be written on rejection");

    // One below the boundary still encodes as a normal 8-byte size whose
    // payload is NOT all-ones, so read_size yields the finite value back.
    buf.clear();
    let v = 0x00FF_FFFF_FFFF_FFFE;
    write_size(&mut buf, v).unwrap();
    let (back, consumed) = read_size(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(consumed, 8);
    assert_eq!(back, v);
}

#[test]
fn test_write_uint() {
    let mut buf = Vec::new();
    write_uint(&mut buf, 0x4286, 1).unwrap(); // EBML_VERSION = 1
    // ID: 42 86, Size: 81 (1 byte), Data: 01
    assert_eq!(buf, [0x42, 0x86, 0x81, 0x01]);
}

#[test]
fn test_write_string() {
    let mut buf = Vec::new();
    write_string(&mut buf, 0x4282, "matroska").unwrap();
    // ID: 42 82, Size: 88 (8 bytes), Data: "matroska"
    assert_eq!(&buf[0..2], &[0x42, 0x82]);
    assert_eq!(buf[2], 0x88); // size = 8
    assert_eq!(&buf[3..], b"matroska");
}

#[test]
fn test_master_element() {
    let mut buf = Cursor::new(Vec::new());
    let pos = start_master(&mut buf, EBML).unwrap();
    write_uint(&mut buf, EBML_VERSION, 1).unwrap();
    end_master(&mut buf, pos).unwrap();
    let data = buf.into_inner();
    // EBML header: 1A 45 DF A3, then 8-byte size, then content
    assert_eq!(&data[0..4], &[0x1A, 0x45, 0xDF, 0xA3]);
}

#[test]
fn write_read_id_roundtrip() {
    // 1-byte IDs have high bit set (0x80..=0xFF)
    for &id in &[0x80u32, 0xA3, 0xFF] {
        let mut buf = Vec::new();
        write_id(&mut buf, id).unwrap();
        assert_eq!(buf.len(), 1);
        let mut cursor = Cursor::new(&buf);
        let (read_back, consumed) = read_id(&mut cursor).unwrap();
        assert_eq!(read_back, id, "1-byte ID roundtrip failed for 0x{:X}", id);
        assert_eq!(consumed, 1);
    }
    // 2-byte IDs (0x4000..=0x7FFF)
    for &id in &[0x4286u32, 0x4282, 0x7FFF] {
        let mut buf = Vec::new();
        write_id(&mut buf, id).unwrap();
        assert_eq!(buf.len(), 2);
        let mut cursor = Cursor::new(&buf);
        let (read_back, consumed) = read_id(&mut cursor).unwrap();
        assert_eq!(read_back, id, "2-byte ID roundtrip failed for 0x{:X}", id);
        assert_eq!(consumed, 2);
    }
    // 3-byte IDs (0x200000..=0x3FFFFF)
    for &id in &[0x22B59Cu32, 0x23E383] {
        let mut buf = Vec::new();
        write_id(&mut buf, id).unwrap();
        assert_eq!(buf.len(), 3);
        let mut cursor = Cursor::new(&buf);
        let (read_back, consumed) = read_id(&mut cursor).unwrap();
        assert_eq!(read_back, id, "3-byte ID roundtrip failed for 0x{:X}", id);
        assert_eq!(consumed, 3);
    }
    // 4-byte IDs (0x10000000..=0x1FFFFFFF)
    for &id in &[EBML, SEGMENT, TRACKS, CLUSTER] {
        let mut buf = Vec::new();
        write_id(&mut buf, id).unwrap();
        assert_eq!(buf.len(), 4);
        let mut cursor = Cursor::new(&buf);
        let (read_back, consumed) = read_id(&mut cursor).unwrap();
        assert_eq!(read_back, id, "4-byte ID roundtrip failed for 0x{:X}", id);
        assert_eq!(consumed, 4);
    }
}

#[test]
fn write_read_size_roundtrip() {
    let test_sizes: &[u64] = &[
        0,
        1,
        0x7E,
        127,
        128,
        0x3FFE,
        16383,
        16384,
        0x1FFFFE,
        0x0FFFFFFE,
        0x1_0000_0000,
    ];
    for &size in test_sizes {
        let mut buf = Vec::new();
        write_size(&mut buf, size).unwrap();
        let mut cursor = Cursor::new(&buf);
        let (read_back, _consumed) = read_size(&mut cursor).unwrap();
        assert_eq!(read_back, size, "size roundtrip failed for {}", size);
    }
}

#[test]
fn write_read_uint_roundtrip() {
    let test_vals: &[u64] = &[
        0,
        1,
        127,
        255,
        256,
        0xFFFF,
        0xFF_FFFF,
        0xFFFF_FFFF,
        1_000_000_000_000,
    ];
    let test_id = EBML_VERSION;
    for &val in test_vals {
        let mut buf = Vec::new();
        write_uint(&mut buf, test_id, val).unwrap();
        let mut cursor = Cursor::new(&buf);
        let (id, _id_len) = read_id(&mut cursor).unwrap();
        assert_eq!(id, test_id);
        let (size, _) = read_size(&mut cursor).unwrap();
        let read_val = read_uint_val(&mut cursor, size as usize).unwrap();
        assert_eq!(read_val, val, "uint roundtrip failed for {}", val);
    }
}

#[test]
fn write_read_string_roundtrip() {
    let test_strings = &[
        "",
        "matroska",
        "freemkv",
        "Hello, World!",
        "unicode: \u{1F600}",
    ];
    let test_id = EBML_DOC_TYPE;
    for &s in test_strings {
        let mut buf = Vec::new();
        write_string(&mut buf, test_id, s).unwrap();
        let mut cursor = Cursor::new(&buf);
        let (id, _) = read_id(&mut cursor).unwrap();
        assert_eq!(id, test_id);
        let (size, _) = read_size(&mut cursor).unwrap();
        let read_s = read_string_val(&mut cursor, size as usize).unwrap();
        assert_eq!(read_s, s, "string roundtrip failed for {:?}", s);
    }
}

#[test]
fn write_read_float_roundtrip() {
    let test_vals: &[f64] = &[
        0.0,
        1.0,
        -1.0,
        std::f64::consts::PI,
        48000.0,
        7200000.0,
        f64::MIN,
        f64::MAX,
    ];
    let test_id = DURATION;
    for &val in test_vals {
        let mut buf = Vec::new();
        write_float(&mut buf, test_id, val).unwrap();
        let mut cursor = Cursor::new(&buf);
        let (id, _) = read_id(&mut cursor).unwrap();
        assert_eq!(id, test_id);
        let (size, _) = read_size(&mut cursor).unwrap();
        assert_eq!(size, 8);
        let read_val = read_float_val(&mut cursor, size as usize).unwrap();
        assert_eq!(
            read_val.to_bits(),
            val.to_bits(),
            "float roundtrip failed for {}",
            val
        );
    }
}

#[test]
fn read_size_unknown_sentinel_all_widths() {
    // The all-ones VINT of each width is the EBML "unknown size" marker
    // and must read back as u64::MAX. write_size never emits the 5/6/7-byte
    // widths, so these are hand-crafted. Each entry is (bytes, expected_len).
    let cases: &[(&[u8], usize)] = &[
        // 1-byte: 0x80 | 0x7F
        (&[0xFF], 1),
        // 2-byte: 0x40 marker, value bits all 1
        (&[0x7F, 0xFF], 2),
        // 3-byte
        (&[0x3F, 0xFF, 0xFF], 3),
        // 4-byte
        (&[0x1F, 0xFF, 0xFF, 0xFF], 4),
        // 5-byte (0x08 marker)
        (&[0x0F, 0xFF, 0xFF, 0xFF, 0xFF], 5),
        // 6-byte (0x04 marker)
        (&[0x07, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 6),
        // 7-byte (0x02 marker)
        (&[0x03, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 7),
        // 8-byte (0x01 marker)
        (&[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 8),
    ];
    for (bytes, expected_len) in cases {
        let mut cursor = Cursor::new(*bytes);
        let (size, consumed) = read_size(&mut cursor).unwrap();
        assert_eq!(
            size,
            u64::MAX,
            "all-ones {}-byte VINT should be unknown-size",
            expected_len
        );
        assert_eq!(consumed, *expected_len);
    }
}

#[test]
fn read_size_concrete_5_6_7_byte_values() {
    // A non-sentinel 5/6/7-byte size must read back as its concrete value,
    // not be mistaken for unknown-size.
    // 5-byte: marker 0x08, value 0x01 (0x0800000001 with width bit only).
    let mut c = Cursor::new(&[0x08u8, 0x00, 0x00, 0x00, 0x01]);
    assert_eq!(read_size(&mut c).unwrap(), (1, 5));
    // 6-byte
    let mut c = Cursor::new(&[0x04u8, 0x00, 0x00, 0x00, 0x00, 0x05]);
    assert_eq!(read_size(&mut c).unwrap(), (5, 6));
    // 7-byte
    let mut c = Cursor::new(&[0x02u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09]);
    assert_eq!(read_size(&mut c).unwrap(), (9, 7));
}

#[test]
fn read_size_rejects_zero_first_byte() {
    // b0 == 0x00 has no width marker — an over-long/invalid VINT. It must
    // be rejected, not silently treated as an 8-byte size.
    let mut c = Cursor::new(&[0x00u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    let e = read_size(&mut c).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn write_size_rejects_at_or_above_2_56() {
    // 2^56 cannot be encoded in the 7-payload-byte 8-byte VINT and must
    // error rather than silently truncate.
    let mut buf = Vec::new();
    let e = write_size(&mut buf, 0x0100_0000_0000_0000).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    // The largest encodable size still succeeds.
    let mut buf = Vec::new();
    write_size(&mut buf, 0x00FF_FFFF_FFFF_FFFE).unwrap();
    assert_eq!(buf.len(), 8);
}

#[test]
fn unknown_size() {
    let mut buf = Vec::new();
    write_unknown_size(&mut buf).unwrap();
    assert_eq!(buf.len(), 8);
    assert_eq!(buf[0], 0x01);
    for &b in &buf[1..] {
        assert_eq!(
            b, 0xFF,
            "unknown size bytes should all be 0xFF after first byte"
        );
    }
    // Reading it back should yield u64::MAX
    let mut cursor = Cursor::new(&buf);
    let (size, consumed) = read_size(&mut cursor).unwrap();
    assert_eq!(size, u64::MAX);
    assert_eq!(consumed, 8);
}

// write_id — exact width selection per EBML element-ID ranges. write_id must
// pick the minimal whole-byte encoding so the ID round-trips at the same width.

#[test]
fn write_id_exact_bytes_per_width() {
    // 1-byte ID (high bit set): emitted as a single byte verbatim.
    let mut b = Vec::new();
    write_id(&mut b, 0xA3).unwrap(); // SimpleBlock
    assert_eq!(b, [0xA3]);

    // The boundary just above 1 byte: 0x100 must be a 2-byte ID. A
    // mutation that widened the 1-byte branch (id <= 0x1FF) would drop
    // the high byte here.
    let mut b = Vec::new();
    write_id(&mut b, 0x0100).unwrap();
    assert_eq!(b, [0x01, 0x00]);

    // 2-byte ID written MSB-first.
    let mut b = Vec::new();
    write_id(&mut b, 0x4286).unwrap(); // EBMLVersion
    assert_eq!(b, [0x42, 0x86]);

    // 3-byte boundary: 0x1_0000 must be 3 bytes.
    let mut b = Vec::new();
    write_id(&mut b, 0x01_0000).unwrap();
    assert_eq!(b, [0x01, 0x00, 0x00]);

    // 3-byte ID (Language = 0x22B59C).
    let mut b = Vec::new();
    write_id(&mut b, 0x22_B59C).unwrap();
    assert_eq!(b, [0x22, 0xB5, 0x9C]);

    // 4-byte boundary: 0x100_0000 must be 4 bytes.
    let mut b = Vec::new();
    write_id(&mut b, 0x0100_0000).unwrap();
    assert_eq!(b, [0x01, 0x00, 0x00, 0x00]);

    // 4-byte ID (Segment = 0x18538067) MSB-first.
    let mut b = Vec::new();
    write_id(&mut b, 0x1853_8067).unwrap();
    assert_eq!(b, [0x18, 0x53, 0x80, 0x67]);
}

#[test]
fn read_id_rejects_zero_first_byte() {
    // 0x00 has no length marker, meaning an ID wider than 4 bytes, which isn't
    // representable here; reject or the parser desyncs.
    let mut c = Cursor::new(&[0x00u8, 0x11, 0x22, 0x33]);
    let e = read_id(&mut c).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
}

// write_uint — the SIZE byte must reflect the minimal big-endian value width
// (1/2/3/4/8, no leading-zero bytes); a boundary bug desyncs every following element.

#[test]
fn write_uint_size_byte_matches_value_width() {
    // (value, expected_size_byte, expected_payload)
    // size byte is a 1-byte VINT: 0x80 | len.
    let cases: &[(u64, u8, &[u8])] = &[
        (0x00, 0x81, &[0x00]),                          // 1 byte
        (0xFF, 0x81, &[0xFF]),                          // 1 byte (boundary high)
        (0x0100, 0x82, &[0x01, 0x00]),                  // 2 bytes (just over u8)
        (0xFFFF, 0x82, &[0xFF, 0xFF]),                  // 2 bytes (boundary high)
        (0x01_0000, 0x83, &[0x01, 0x00, 0x00]),         // 3 bytes
        (0xFF_FFFF, 0x83, &[0xFF, 0xFF, 0xFF]),         // 3 bytes (boundary high)
        (0x0100_0000, 0x84, &[0x01, 0x00, 0x00, 0x00]), // 4 bytes
        (0xFFFF_FFFF, 0x84, &[0xFF, 0xFF, 0xFF, 0xFF]), // 4 bytes (boundary high)
        // Just over u32 → jumps straight to 8 bytes (no 5/6/7 path).
        (
            0x1_0000_0000,
            0x88,
            &[0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
        ),
    ];
    let id = EBML_VERSION; // 2-byte ID 0x4286
    for (val, size_byte, payload) in cases {
        let mut buf = Vec::new();
        write_uint(&mut buf, id, *val).unwrap();
        assert_eq!(&buf[0..2], &[0x42, 0x86], "ID prefix for val {val:#x}");
        assert_eq!(buf[2], *size_byte, "size byte for val {val:#x}");
        assert_eq!(&buf[3..], *payload, "payload for val {val:#x}");
    }
}

#[test]
fn write_uint_zero_is_one_byte_not_zero_length() {
    // EBML stores 0 as a single 0x00 byte (size 1), NOT a zero-length
    // element. A muxer reader expects to consume exactly one payload byte.
    let mut buf = Vec::new();
    write_uint(&mut buf, EBML_VERSION, 0).unwrap();
    // ID(2) + size(1=0x81) + one payload byte 0x00.
    assert_eq!(buf, [0x42, 0x86, 0x81, 0x00]);
}

#[test]
fn write_int_minimal_two_complement_width() {
    // ReferenceBlock (0xFB) signed offsets, minimal two's-complement width.
    let enc = |v: i64| {
        let mut b = Vec::new();
        write_int(&mut b, REFERENCE_BLOCK, v).unwrap();
        b
    };
    assert_eq!(enc(0), [0xFB, 0x81, 0x00], "0 -> 1 byte 0x00");
    assert_eq!(enc(-1), [0xFB, 0x81, 0xFF], "-1 -> 1 byte 0xFF");
    assert_eq!(enc(127), [0xFB, 0x81, 0x7F], "127 -> 1 byte");
    assert_eq!(
        enc(128),
        [0xFB, 0x82, 0x00, 0x80],
        "128 needs 2 bytes (0x80 alone is -128)"
    );
    assert_eq!(enc(-128), [0xFB, 0x81, 0x80], "-128 -> 1 byte 0x80");
    assert_eq!(enc(-129), [0xFB, 0x82, 0xFF, 0x7F], "-129 needs 2 bytes");
    // i64::MIN is the widest: 8 bytes, size 0x88.
    let mn = enc(i64::MIN);
    assert_eq!(mn[0], 0xFB);
    assert_eq!(mn[1], 0x88);
    assert_eq!(&mn[2..], &i64::MIN.to_be_bytes());
}

// write_float — EBML floats here are always 8-byte IEEE-754 doubles,
// big-endian (Matroska SamplingFrequency/Duration). size byte = 0x88.

#[test]
fn write_float_is_8_byte_big_endian_double() {
    let mut buf = Vec::new();
    write_float(&mut buf, DURATION, 48000.0).unwrap();
    // ID DURATION = 0x4489 (2 bytes), size = 0x88 (8), then BE f64.
    assert_eq!(&buf[0..2], &[0x44, 0x89]);
    assert_eq!(buf[2], 0x88, "float element must declare 8-byte size");
    assert_eq!(&buf[3..11], &48000.0f64.to_be_bytes());
    // The reader (4-byte path) must yield an f32-promoted value, while the
    // 8-byte path yields the exact double.
    let got = read_float_val(&mut Cursor::new(&buf[3..11]), 8).unwrap();
    assert_eq!(got.to_bits(), 48000.0f64.to_bits());
}

// write_string / write_binary — declared size must equal the byte length
// (UTF-8 byte count, not char count) so the reader consumes exactly the payload.

#[test]
fn write_string_size_is_utf8_byte_count_not_char_count() {
    // "é" is 2 UTF-8 bytes; the size field must be 2, not 1.
    let mut buf = Vec::new();
    write_string(&mut buf, EBML_DOC_TYPE, "é").unwrap();
    assert_eq!(&buf[0..2], &[0x42, 0x82]); // DocType ID
    assert_eq!(buf[2], 0x80 | 2, "size must be UTF-8 byte length (2)");
    assert_eq!(&buf[3..], "é".as_bytes());
}

#[test]
fn write_binary_declares_exact_length() {
    let data = [0xDE, 0xAD, 0xBE, 0xEF, 0x00];
    let mut buf = Vec::new();
    write_binary(&mut buf, CODEC_PRIVATE, &data).unwrap();
    // CODEC_PRIVATE id 0x63A2 (2 bytes), size 0x85 (len 5), then data.
    assert_eq!(&buf[0..2], &[0x63, 0xA2]);
    assert_eq!(buf[2], 0x80 | 5);
    assert_eq!(&buf[3..], &data);
}

// read_string_val — Matroska strings may be null-padded; the reader strips
// trailing NULs but must preserve interior content and bytes consumed.

#[test]
fn read_string_val_strips_only_trailing_nulls() {
    // "ab\0\0" → "ab"; interior content must not be touched.
    let raw = b"ab\0\0";
    let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
    assert_eq!(s, "ab");
    // A string that is ALL nulls collapses to empty (every byte popped).
    let raw = b"\0\0\0";
    let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
    assert_eq!(s, "");
    // An interior NUL is NOT a terminator for the strip loop (it only pops
    // from the tail), so "a\0b" keeps the interior NUL.
    let raw = b"a\0b";
    let s = read_string_val(&mut Cursor::new(raw), raw.len()).unwrap();
    assert_eq!(s.as_bytes(), b"a\0b");
}

// read_uint_val — big-endian assembly; an EBML uint never exceeds 8 bytes
// (the reader rejects len>8 to avoid a stack OOB).

#[test]
fn read_uint_val_big_endian_and_len_zero() {
    // Big-endian: 0x01 0x02 0x03 → 0x010203.
    let v = read_uint_val(&mut Cursor::new(&[0x01u8, 0x02, 0x03]), 3).unwrap();
    assert_eq!(v, 0x01_0203);
    // len 0 yields 0 with no read.
    let v = read_uint_val(&mut Cursor::new(&[] as &[u8]), 0).unwrap();
    assert_eq!(v, 0);
    // Full 8-byte width assembles correctly (no truncation).
    let bytes = [0x12u8, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];
    let v = read_uint_val(&mut Cursor::new(&bytes), 8).unwrap();
    assert_eq!(v, 0x1234_5678_9ABC_DEF0);
}

#[test]
fn read_uint_val_rejects_len_above_8() {
    // len 9 would index past the [0u8; 8] buffer → OOB/DoS on untrusted
    // input. Must be a clean MkvSourceInvalid.
    let e = read_uint_val(&mut Cursor::new(&[0u8; 16]), 9).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
}

// read_float_val — exactly 0/4/8 byte widths; 4-byte is an f32 promoted to
// f64, 8-byte is an exact f64.

#[test]
fn read_float_val_4_byte_is_f32_promoted() {
    // 1.5 as a 32-bit float → 0x3FC00000.
    let bytes = 1.5f32.to_be_bytes();
    let v = read_float_val(&mut Cursor::new(&bytes), 4).unwrap();
    assert_eq!(v, 1.5f64);
    // A value with no exact f32 representation loses precision exactly as
    // f32→f64 would (proves the 4-byte branch uses f32, not f64).
    let bytes = 0.1f32.to_be_bytes();
    let v = read_float_val(&mut Cursor::new(&bytes), 4).unwrap();
    assert_eq!(v, 0.1f32 as f64);
    assert_ne!(v, 0.1f64, "4-byte path must be f32, losing f64 precision");
}

#[test]
fn read_float_val_rejects_odd_widths() {
    // Only 0/4/8 are valid; 1,2,3,5,6,7 must error (never over/under-read).
    for len in [1usize, 2, 3, 5, 6, 7] {
        let e = read_float_val(&mut Cursor::new(&[0u8; 8]), len).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData, "len {len}");
    }
}

// read_binary_val / read_exact_bounded — a declared length exceeding bytes
// present is a truncated element; must error without allocating the full size.

#[test]
fn read_binary_val_short_read_errors() {
    // Declare 100 bytes but supply 4 → MkvSourceInvalid (truncated element).
    let e = read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 100).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    // Exact-length read returns the bytes verbatim.
    let v = read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), 4).unwrap();
    assert_eq!(v, vec![1, 2, 3, 4]);
}

// read_element_header — header_bytes is id_len + size_len, and a truncated
// header (EOF mid-size) surfaces as an error.

#[test]
fn read_element_header_reports_total_header_len() {
    // 4-byte ID (Segment) + 8-byte unknown size = 12 header bytes.
    let mut buf = Vec::new();
    write_id(&mut buf, SEGMENT).unwrap();
    write_unknown_size(&mut buf).unwrap();
    let (id, size, hdr) = read_element_header(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(id, SEGMENT);
    assert_eq!(size, u64::MAX);
    assert_eq!(hdr, 12, "4-byte id + 8-byte size = 12 header bytes");

    // 1-byte ID (SimpleBlock 0xA3) + 1-byte size = 2 header bytes.
    let mut buf = Vec::new();
    write_id(&mut buf, SIMPLE_BLOCK).unwrap();
    write_size(&mut buf, 10).unwrap();
    let (id, size, hdr) = read_element_header(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(id, SIMPLE_BLOCK);
    assert_eq!(size, 10);
    assert_eq!(hdr, 2);
}

#[test]
fn read_id_truncated_after_marker_errors() {
    // First byte 0x40 promises a 2-byte ID but the second byte is missing.
    // read_exact must surface EOF, never silently produce a 1-byte ID.
    let e = read_id(&mut Cursor::new(&[0x40u8])).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn read_size_truncated_inside_the_vint_errors_at_every_width() {
    // A width-w size VINT needs w bytes; supply one fewer, never a decoded size.
    for w in 2..=8usize {
        let mut bytes = vec![0u8; w - 1];
        bytes[0] = 0x80 >> (w - 1);
        let e = read_size(&mut Cursor::new(&bytes)).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof, "width {w}");
    }
    let mut buf = Vec::new();
    write_id(&mut buf, SIMPLE_BLOCK).unwrap();
    buf.push(0x40);
    let e = read_element_header(&mut Cursor::new(&buf)).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
}

// write_size — every declared width-boundary, asserting the exact VINT bytes.
// Width W encodes 7*W payload bits; the highest value of each width is reserved.

#[test]
fn write_size_exact_bytes_at_width_boundaries() {
    // Largest 1-byte value (126 = 0x7E): marker 0x80 | value.
    let mut b = Vec::new();
    write_size(&mut b, 0x7E).unwrap();
    assert_eq!(b, [0x80 | 0x7E]);
    // 0x7F is NOT 1-byte here (reserved sentinel region) → 2 bytes.
    let mut b = Vec::new();
    write_size(&mut b, 0x7F).unwrap();
    assert_eq!(b, [0x40, 0x7F]);
    // Largest 2-byte value below the 0x3FFF sentinel.
    let mut b = Vec::new();
    write_size(&mut b, 0x3FFE).unwrap();
    assert_eq!(b, [0x40 | 0x3F, 0xFE]);
    // First 3-byte value (0x3FFF goes 3-byte because `< 0x3FFF` is false).
    let mut b = Vec::new();
    write_size(&mut b, 0x3FFF).unwrap();
    assert_eq!(b, [0x20, 0x3F, 0xFF]);
    // First 4-byte value: 0x1F_FFFF is not < 0x1F_FFFF.
    let mut b = Vec::new();
    write_size(&mut b, 0x1F_FFFF).unwrap();
    assert_eq!(b, [0x10, 0x1F, 0xFF, 0xFF]);
    // First 8-byte value: 0x0FFF_FFFF is not < 0x0FFF_FFFF.
    let mut b = Vec::new();
    write_size(&mut b, 0x0FFF_FFFF).unwrap();
    assert_eq!(b, [0x01, 0, 0, 0, 0x0F, 0xFF, 0xFF, 0xFF]);
}

// start_master / end_master — the placeholder is an 8-byte VINT and end_master
// must back-patch the exact body size (end - start - 8); a wrong subtraction
// corrupts every nested master element's declared size.

#[test]
fn end_master_backpatches_exact_body_size() {
    let mut c = Cursor::new(Vec::new());
    let pos = start_master(&mut c, SEGMENT).unwrap();
    // Body: a 4-byte uint element (ID 0x4286, size 0x81, payload 0x01).
    write_uint(&mut c, EBML_VERSION, 1).unwrap();
    end_master(&mut c, pos).unwrap();
    let data = c.into_inner();
    // Layout: SEGMENT id (4 bytes) | 8-byte size VINT | body (4 bytes).
    assert_eq!(&data[0..4], &SEGMENT.to_be_bytes());
    // The size field is an 8-byte VINT; its payload must equal the body
    // length (4). 0x01 marker then 7 payload bytes ending in 0x04.
    assert_eq!(data[4], 0x01);
    assert_eq!(&data[5..12], &[0, 0, 0, 0, 0, 0, 4]);
    // Read it back: the header parser sees the exact body size.
    let (id, size, hdr) = read_element_header(&mut Cursor::new(&data)).unwrap();
    assert_eq!(id, SEGMENT);
    assert_eq!(size, 4, "back-patched size must equal body byte count");
    assert_eq!(hdr, 12);
    assert_eq!(data.len() as u64, hdr as u64 + size);
}

#[test]
fn end_master_empty_body_is_zero_size() {
    // A master with no body must declare size 0 (end == start + 8).
    let mut c = Cursor::new(Vec::new());
    let pos = start_master(&mut c, INFO).unwrap();
    end_master(&mut c, pos).unwrap();
    let data = c.into_inner();
    let (id, size, _) = read_element_header(&mut Cursor::new(&data)).unwrap();
    assert_eq!(id, INFO);
    assert_eq!(size, 0);
}

#[test]
fn nested_masters_each_get_correct_size() {
    // Outer master containing an inner master + a sibling uint. Each
    // declared size must bound exactly its own body. This is the nested
    // sizing that mkv.rs relies on for Segment→Tracks→TrackEntry.
    let mut c = Cursor::new(Vec::new());
    let outer = start_master(&mut c, TRACKS).unwrap();
    let inner = start_master(&mut c, TRACK_ENTRY).unwrap();
    write_uint(&mut c, TRACK_NUMBER, 1).unwrap();
    end_master(&mut c, inner).unwrap();
    write_uint(&mut c, TRACK_NUMBER, 2).unwrap();
    end_master(&mut c, outer).unwrap();
    let data = c.into_inner();

    let mut cur = Cursor::new(&data);
    let (oid, osize, _) = read_element_header(&mut cur).unwrap();
    assert_eq!(oid, TRACKS);
    let outer_body_start = cur.position();
    // First child of TRACKS is TRACK_ENTRY.
    let (iid, isize, _) = read_element_header(&mut cur).unwrap();
    assert_eq!(iid, TRACK_ENTRY);
    // Skip TRACK_ENTRY body; the next element must be the sibling uint.
    cur.set_position(cur.position() + isize);
    let (sid, ssize, _) = read_element_header(&mut cur).unwrap();
    assert_eq!(sid, TRACK_NUMBER, "sibling after inner master");
    // Skip the sibling's body too, then total bytes consumed inside the
    // outer master must exactly equal its declared size.
    cur.set_position(cur.position() + ssize);
    let consumed = cur.position() - outer_body_start;
    assert_eq!(consumed, osize, "outer size must bound both children");
    // And the whole buffer is exactly the outer element.
    assert_eq!(data.len() as u64, outer_body_start + osize);
}
// Buffer-based and seek-based master helpers must produce byte-for-byte identical output;
// write_block_group relies on that to skip seeks/flushes.
#[test]
fn buffered_master_matches_seeking_master_byte_for_byte() {
    use std::io::Cursor;

    // Bodies chosen to cross VINT-relevant magnitudes: empty, tiny, and one
    // spanning more than a byte of length.
    for body in [
        Vec::new(),
        vec![0xAAu8],
        (0..300u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>(),
    ] {
        // Seek-based path.
        let mut c = Cursor::new(Vec::new());
        let pos = start_master(&mut c, BLOCK_GROUP).unwrap();
        c.write_all(&body).unwrap();
        end_master(&mut c, pos).unwrap();
        let seeking = c.into_inner();

        // Buffer-based path.
        let mut buf = Vec::new();
        let bpos = start_master_buf(&mut buf, BLOCK_GROUP).unwrap();
        buf.extend_from_slice(&body);
        end_master_buf(&mut buf, bpos).unwrap();

        assert_eq!(
            buf,
            seeking,
            "buffered and seeking master encodings diverged for a {}-byte body",
            body.len()
        );
    }
}

// Nested masters must patch correctly too: in-memory patching works by
// index rather than file offset, so nesting is where an index slip shows.
#[test]
fn buffered_master_nests_correctly() {
    use std::io::Cursor;

    let mut c = Cursor::new(Vec::new());
    let outer = start_master(&mut c, BLOCK_GROUP).unwrap();
    c.write_all(&[0x11, 0x22]).unwrap();
    let inner = start_master(&mut c, BLOCK_ADDITIONS).unwrap();
    c.write_all(&[0x33, 0x44, 0x55]).unwrap();
    end_master(&mut c, inner).unwrap();
    c.write_all(&[0x66]).unwrap();
    end_master(&mut c, outer).unwrap();
    let seeking = c.into_inner();

    let mut buf = Vec::new();
    let outer_b = start_master_buf(&mut buf, BLOCK_GROUP).unwrap();
    buf.extend_from_slice(&[0x11, 0x22]);
    let inner_b = start_master_buf(&mut buf, BLOCK_ADDITIONS).unwrap();
    buf.extend_from_slice(&[0x33, 0x44, 0x55]);
    end_master_buf(&mut buf, inner_b).unwrap();
    buf.extend_from_slice(&[0x66]);
    end_master_buf(&mut buf, outer_b).unwrap();

    assert_eq!(
        buf, seeking,
        "nested buffered masters must match the seeking form"
    );
}

/// `end_master_buf` must REFUSE a bogus position rather than panic on the
/// slice index — this crate must not panic from library code.
#[test]
fn end_master_buf_rejects_a_position_outside_the_buffer() {
    let mut buf = vec![0u8; 4];
    assert!(
        end_master_buf(&mut buf, 99).is_err(),
        "a size_pos past the end of the buffer must error, not panic"
    );
}

// Every payload octet is distinct, so a shifted/reversed/dropped octet is visible; the top
// octets are unreachable through end_master directly.
#[test]
// Underscores mark bitfield boundaries (e.g. 5-bit then 3-bit), not digit
// groups; regrouping uniformly would destroy the only thing they encode.
#[allow(clippy::unusual_byte_groupings)]
fn fixed_width_vint8_is_big_endian_over_the_full_payload() {
    assert_eq!(
        fixed_width_vint8(0x00AA_BB_CC_DD_EE_FF_11),
        [0x01, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11]
    );
    assert_eq!(fixed_width_vint8(0), [0x01, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(
        fixed_width_vint8(0x00FF_FFFF_FFFF_FFFE),
        [0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]
    );
    // Each payload octet in isolation lands in its own position.
    for i in 0..7u32 {
        let mut want = [0u8; 8];
        want[0] = 0x01;
        want[7 - i as usize] = 0x5A;
        assert_eq!(fixed_width_vint8(0x5Au64 << (8 * i)), want, "octet {i}");
    }
}

// void_element fills exactly `total_len` bytes with a 1-byte size VINT; 129 would need
// the reserved all-ones size byte 0xFF and is refused.
#[test]
fn void_element_range_and_encoding() {
    assert_eq!(void_element(2).unwrap(), vec![0xEC, 0x80]);
    let v = void_element(128).unwrap();
    assert_eq!((v.len(), v[0], v[1]), (128, 0xEC, 0xFE));
    assert!(v[2..].iter().all(|&b| b == 0));
    for bad in [0, 1, 129] {
        let e = void_element(bad).unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some(crate::error::E_MKV_UNENCODABLE),
            "{bad}"
        );
    }
}

// An attacker-sized EBML length must not size an allocation: a huge claim against a
// 4-byte source is a clean MkvSourceInvalid (usize::MAX would overflow any up-front buffer).
#[test]
fn read_binary_val_huge_declared_len_is_source_invalid() {
    for len in [1usize << 40, usize::MAX] {
        let e = read_binary_val(&mut Cursor::new(&[1u8, 2, 3, 4]), len).unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some(crate::error::E_MKV_SOURCE_INVALID),
            "{len}"
        );
    }
}

// Invalid UTF-8 in an untrusted string element is MkvSourceInvalid, never lossy/panic.
#[test]
fn read_string_val_invalid_utf8_is_source_invalid() {
    let e = read_string_val(&mut Cursor::new(&[0xFFu8, 0xFE]), 2).unwrap_err();
    assert_eq!(
        crate::error::error_code(&e),
        Some(crate::error::E_MKV_SOURCE_INVALID)
    );
}

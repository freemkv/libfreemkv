use super::*;

#[test]
fn fixed_accepts_0x_0x_and_bare_same_result() {
    let want = [0x00, 0x11, 0xab, 0xCD, 0xef, 0x42, 0x99, 0x00];
    let bare = "0011abcdef429900";
    assert_eq!(parse_hex_fixed::<8>(bare), Some(want));
    assert_eq!(parse_hex_fixed::<8>(&format!("0x{bare}")), Some(want));
    // The case that used to be dropped by one parser but not another.
    assert_eq!(parse_hex_fixed::<8>(&format!("0X{bare}")), Some(want));
    assert_eq!(parse_hex_fixed::<8>(&format!("  0X{bare}  ")), Some(want));
}

#[test]
fn fixed_rejects_wrong_length_and_non_hex_and_signs() {
    assert_eq!(parse_hex_fixed::<16>("00"), None); // too short
    assert_eq!(parse_hex_fixed::<2>("00112233"), None); // too long
    assert_eq!(parse_hex_fixed::<2>("zz11"), None); // non-hex
    assert_eq!(parse_hex_fixed::<2>("+5-A"), None); // sign chars
}

#[test]
fn does_not_panic_on_multibyte_of_exact_byte_length() {
    // "中" is 3 bytes; + 29 'a' = 32 bytes → would mis-slice a &str-indexed
    // parser. Must reject, not panic.
    let s = "中".to_string() + &"a".repeat(29);
    assert_eq!(s.len(), 32);
    assert_eq!(parse_hex_fixed::<16>(&s), None);
}

#[test]
fn hex_ints_accept_both_prefix_cases_and_bare() {
    // The regression the keydb device-key bug hit: uppercase `0X` must parse
    // identically to `0x` and to a bare value.
    assert_eq!(parse_hex_u16("0x0001"), Some(1));
    assert_eq!(parse_hex_u16("0X0001"), Some(1));
    assert_eq!(parse_hex_u16("0001"), Some(1));
    assert_eq!(parse_hex_u16(" 0XABCD "), Some(0xABCD));
    assert_eq!(parse_hex_u32("0X00000002"), Some(2));
    assert_eq!(parse_hex_u32("deadbeef"), Some(0xDEAD_BEEF));
    assert_eq!(parse_hex_u8("0X03"), Some(3));
    assert_eq!(parse_hex_u8("ff"), Some(0xFF));
    // Overflow / non-hex → None.
    assert_eq!(parse_hex_u8("0x1FF"), None);
    assert_eq!(parse_hex_u16("0xzz"), None);
}

// `from_str_radix` accepts a leading `+` (`+10` → 16), but hex key material is
// never signed and the byte parsers reject it, so the integer parsers must too.
// Red-before-green: before the guard, each of these returned `Some(..)` not `None`.
#[test]
fn hex_ints_reject_leading_plus_sign() {
    assert_eq!(parse_hex_u16("+10"), None);
    assert_eq!(parse_hex_u16("+0010"), None);
    assert_eq!(parse_hex_u32("+deadbeef"), None);
    assert_eq!(parse_hex_u8("+03"), None);
    // A `+` behind the prefix is rejected too (strip leaves `+10`).
    assert_eq!(parse_hex_u16("0x+10"), None);
}

#[test]
fn bytes_variable_length_and_odd_rejected() {
    assert_eq!(parse_hex_bytes("0xAABBCC"), Some(vec![0xAA, 0xBB, 0xCC]));
    assert_eq!(parse_hex_bytes("AABBC"), None); // odd
    // Empty (or prefix-only) → empty Vec: a legitimately-empty field.
    assert_eq!(parse_hex_bytes(""), Some(vec![]));
    assert_eq!(parse_hex_bytes("0x"), Some(vec![]));
}

//! The single hex → bytes parser for the whole workspace.
//!
//! Key material arrives as hex from three third-party sources — the keydb, an
//! online key service, and the mapfile's `# freemkv-vid:` comment — and each
//! used to parse it slightly differently (one stripped `0x`/`0X`, one stripped
//! nothing, one stripped `0x` only). A key written with a prefix one parser
//! didn't expect was silently dropped → "can't decrypt" with no error. This is
//! the one parser they all call, so the prefix/case/validation rules live in
//! exactly one place.
//!
//! Operates on BYTES, not `&str` char indices: the inputs are untrusted, so a
//! multi-byte UTF-8 scalar must reject as malformed, never panic on a
//! mid-codepoint slice.

/// Parse a hex string into bytes. Accepts an optional `0x`/`0X` prefix
/// (case-insensitive), then requires an even run of ASCII hex digits. Any
/// non-hex byte, or an odd length, yields `None`.
pub fn parse_hex_bytes(s: &str) -> Option<Vec<u8>> {
    let body = strip_hex_prefix(s.trim());
    let bytes = body.as_bytes();
    // Empty → empty Vec (a legitimately-empty variable-length field); odd length
    // is malformed. (`parse_hex_fixed` enforces a concrete length separately.)
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        out.push(byte(pair[0], pair[1])?);
    }
    Some(out)
}

/// Parse a hex string into a fixed `[u8; N]`. Accepts an optional `0x`/`0X`
/// prefix; requires EXACTLY `2*N` ASCII hex digits after it. `None` on any
/// non-hex byte or a length mismatch.
pub fn parse_hex_fixed<const N: usize>(s: &str) -> Option<[u8; N]> {
    let body = strip_hex_prefix(s.trim());
    let bytes = body.as_bytes();
    if bytes.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = byte(bytes[2 * i], bytes[2 * i + 1])?;
    }
    Some(out)
}

/// Parse a hex string into a `u16`. Accepts an optional `0x`/`0X` prefix
/// (case-insensitive) via the same [`strip_hex_prefix`] the byte parsers use.
/// `None` on any non-hex content or overflow.
///
/// Exists so callers never hand-roll `from_str_radix(s.trim_start_matches("0x"), 16)`
/// — a **case-sensitive** strip that silently dropped an uppercase-`0X` value.
/// (That reintroduced-in-keydb bug is exactly what this module was built to kill;
/// the integer fields now share the one prefix rule.)
pub fn parse_hex_u16(s: &str) -> Option<u16> {
    u16::from_str_radix(strip_hex_prefix(s.trim()), 16).ok()
}

/// Parse a hex string into a `u32`. See [`parse_hex_u16`].
pub fn parse_hex_u32(s: &str) -> Option<u32> {
    u32::from_str_radix(strip_hex_prefix(s.trim()), 16).ok()
}

/// Parse a hex string into a `u8`. See [`parse_hex_u16`].
pub fn parse_hex_u8(s: &str) -> Option<u8> {
    u8::from_str_radix(strip_hex_prefix(s.trim()), 16).ok()
}

/// Strip a single leading `0x` / `0X` if present (case-insensitive). Public so
/// callers that only need the prefix rule (e.g. normalizing a disc hash) reuse
/// the one definition instead of hand-rolling a case-sensitive
/// `trim_start_matches("0x")`.
pub fn strip_hex_prefix(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

/// Combine two ASCII hex-digit bytes into one byte. `as char` is intentional:
/// for a non-ASCII byte it produces a Latin-1 scalar that `to_digit(16)` then
/// rejects — so non-hex (incl. `+`/`-` sign chars) and multi-byte input fail
/// cleanly rather than slipping through `from_str_radix`'s sign handling.
fn byte(hi: u8, lo: u8) -> Option<u8> {
    let hi = (hi as char).to_digit(16)?;
    let lo = (lo as char).to_digit(16)?;
    Some((hi * 16 + lo) as u8)
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn bytes_variable_length_and_odd_rejected() {
        assert_eq!(parse_hex_bytes("0xAABBCC"), Some(vec![0xAA, 0xBB, 0xCC]));
        assert_eq!(parse_hex_bytes("AABBC"), None); // odd
        // Empty (or prefix-only) → empty Vec: a legitimately-empty field.
        assert_eq!(parse_hex_bytes(""), Some(vec![]));
        assert_eq!(parse_hex_bytes("0x"), Some(vec![]));
    }
}

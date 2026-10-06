//! The single hex → bytes parser for the whole workspace.
//!
//! Key material arrives as hex from three third-party sources that each used to parse it
//! slightly differently, so a key with a prefix one parser didn't expect was silently dropped.
//! This is the one parser they all call, so the prefix/case/validation rules live in exactly
//! one place.
//!
//! Operates on BYTES, not `&str` char indices, so a multi-byte UTF-8 scalar
//! in untrusted input rejects as malformed rather than panicking mid-codepoint.

/// Parse a hex string into bytes. Accepts an optional `0x`/`0X` prefix
/// (case-insensitive), then requires an even run of ASCII hex digits. Any
/// non-hex byte, or an odd length, yields `None`.
pub fn parse_hex_bytes(s: &str) -> Option<Vec<u8>> {
    let body = strip_hex_prefix(s.trim());
    let bytes = body.as_bytes();
    // Empty → empty Vec (a legitimately-empty variable-length field); odd length
    // is malformed. (`parse_hex_fixed` enforces a concrete length separately.)
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
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
    u16::from_str_radix(hex_int_body(s)?, 16).ok()
}

/// Parse a hex string into a `u32`. See [`parse_hex_u16`].
pub fn parse_hex_u32(s: &str) -> Option<u32> {
    u32::from_str_radix(hex_int_body(s)?, 16).ok()
}

/// Parse a hex string into a `u8`. See [`parse_hex_u16`].
pub fn parse_hex_u8(s: &str) -> Option<u8> {
    u8::from_str_radix(hex_int_body(s)?, 16).ok()
}

/// Trim, strip the optional `0x`/`0X` prefix, and reject a leading `+` sign.
/// `from_str_radix` accepts a leading `+` (`+10` → 16), but hex key material
/// never carries a sign — the byte parsers already reject it via `byte()`, so
/// the integer parsers must too, or the same value parses in one path and drops
/// in another. (A bare `-` already fails on the unsigned parse.)
fn hex_int_body(s: &str) -> Option<&str> {
    let body = strip_hex_prefix(s.trim());
    if body.starts_with('+') {
        return None;
    }
    Some(body)
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

// Combine two ASCII hex-digit bytes into one byte. `as char` is intentional: non-ASCII bytes
// become a Latin-1 scalar that `to_digit(16)` then rejects, so non-hex/multi-byte input fails
// cleanly.
fn byte(hi: u8, lo: u8) -> Option<u8> {
    let hi = (hi as char).to_digit(16)?;
    let lo = (lo as char).to_digit(16)?;
    Some((hi * 16 + lo) as u8)
}

#[cfg(test)]
#[path = "hex_tests.rs"]
mod tests;

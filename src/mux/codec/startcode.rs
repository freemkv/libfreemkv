//! Shared MPEG/Annex-B start-code scanning helpers.
//!
//! H.264, HEVC, MPEG-2 and the MPEG-2 Program Stream demuxer all locate the
//! 3-byte `00 00 01` start-code prefix to delimit NAL units / PES units. A
//! single memchr-backed implementation lives here so every caller gets the
//! same SIMD-accelerated scan instead of a hand-rolled byte-by-byte loop.

/// Find the position of the next start code (`00 00 01`) at or after `from`.
///
/// Backed by `memchr::memmem::find` for SIMD-accelerated bytestring search. On
/// AVX2-capable x86_64 this runs several times faster than a byte-by-byte scan;
/// on a 200 KB UHD HEVC frame the saving is in the hundreds of microseconds per
/// call. The reported offset is the start of the `00 00 01` triple, so a 4-byte
/// `00 00 00 01` start code is reported at the second `00`.
pub fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 3 {
        return None;
    }
    memchr::memmem::find(&data[from..], b"\x00\x00\x01").map(|rel| from + rel)
}

/// Skip past the start code at position `pos`, returning the first byte after
/// it. Handles both the 3-byte (`00 00 01`) and 4-byte (`00 00 00 01`) forms.
/// Returns `None` if `pos` does not begin a start code or the buffer is too
/// short to contain one.
pub fn skip_start_code(data: &[u8], pos: usize) -> Option<usize> {
    if pos + 2 >= data.len() {
        return None;
    }
    if data[pos] == 0x00 && data[pos + 1] == 0x00 {
        if pos + 3 < data.len() && data[pos + 2] == 0x00 && data[pos + 3] == 0x01 {
            return Some(pos + 4); // 4-byte start code
        }
        if data[pos + 2] == 0x01 {
            return Some(pos + 3); // 3-byte start code
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_start_code_3byte() {
        let data = [0x00, 0x00, 0x01, 0x65];
        assert_eq!(find_start_code(&data, 0), Some(0));
    }

    #[test]
    fn find_start_code_4byte() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x65];
        // The 00 00 01 triple starts at offset 1 in a 4-byte start code.
        assert_eq!(find_start_code(&data, 0), Some(1));
    }

    #[test]
    fn find_start_code_offset() {
        let data = [0xFF, 0xFF, 0x00, 0x00, 0x01, 0x09];
        assert_eq!(find_start_code(&data, 0), Some(2));
    }

    #[test]
    fn find_start_code_none() {
        let data = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn find_start_code_too_short() {
        let data = [0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn skip_3byte() {
        let data = [0x00, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), Some(3));
    }

    #[test]
    fn skip_4byte() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), Some(4));
    }

    #[test]
    fn skip_not_a_start_code() {
        let data = [0xFF, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), None);
    }

    // --- find_start_code: `from` offset semantics ---

    #[test]
    fn find_start_code_skips_before_from() {
        // A start code at offset 0 must be ignored when from=1: the scan begins
        // at `from`, so only the SECOND start code (offset 5) is found. Grounds
        // the `&data[from..]` slice + `from + rel` re-offset.
        let data = [0x00, 0x00, 0x01, 0x65, 0xFF, 0x00, 0x00, 0x01, 0x09];
        assert_eq!(find_start_code(&data, 0), Some(0));
        assert_eq!(find_start_code(&data, 1), Some(5));
    }

    #[test]
    fn find_start_code_from_equals_len_minus_3_exact_boundary() {
        // The length guard is `data.len() < from + 3`. With len=6 and from=3 the
        // guard is `6 < 6` = false, so the trailing 3 bytes (a start code) are
        // scanned and found. This is the tightest in-bounds case.
        let data = [0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x01];
        assert_eq!(find_start_code(&data, 3), Some(3));
    }

    #[test]
    fn find_start_code_from_too_close_to_end_returns_none() {
        // from + 3 > len → the `data.len() < from + 3` guard fires (4 < 5) and
        // returns None without scanning, even though earlier bytes hold a code.
        let data = [0x00, 0x00, 0x01, 0xFF];
        assert_eq!(find_start_code(&data, 2), None);
    }

    #[test]
    fn find_start_code_from_past_end_returns_none() {
        // from beyond the buffer must not panic; the guard returns None.
        let data = [0x00, 0x00, 0x01];
        assert_eq!(find_start_code(&data, 100), None);
    }

    #[test]
    fn find_start_code_empty_buffer() {
        // Empty input: len 0 < 0 + 3 → None, no panic.
        let data: [u8; 0] = [];
        assert_eq!(find_start_code(&data, 0), None);
    }

    #[test]
    fn find_start_code_four_byte_reports_inner_triple_not_first_zero() {
        // Doc contract: for `00 00 00 01` the reported offset is the SECOND `00`
        // (start of the `00 00 01` triple), not the first `00`. With a leading
        // junk byte the 4-byte code starts at offset 1, triple at offset 2.
        let data = [0xAB, 0x00, 0x00, 0x00, 0x01, 0x67];
        assert_eq!(find_start_code(&data, 0), Some(2));
    }

    #[test]
    fn find_start_code_long_zero_run_then_one() {
        // memmem must find the `00 00 01` regardless of how many leading zeros
        // precede the `01` (e.g. a zero-padded NAL gap). Triple is the last two
        // zeros + the 01.
        let data = [0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x42];
        // The first `00 00 01` triple ends at the `01` (index 5), so it starts
        // at index 3.
        assert_eq!(find_start_code(&data, 0), Some(3));
    }

    #[test]
    fn find_start_code_two_byte_zero_not_a_match() {
        // `00 00` with no following `01` is not a start code.
        let data = [0x00, 0x00, 0x02, 0x00, 0x00, 0x00];
        assert_eq!(find_start_code(&data, 0), None);
    }

    // --- skip_start_code: boundary / form selection ---

    #[test]
    fn skip_start_code_at_nonzero_pos() {
        // skip must honour pos: a 3-byte code at offset 2 returns 2+3 = 5.
        let data = [0xFF, 0xFF, 0x00, 0x00, 0x01, 0x67, 0x88];
        assert_eq!(skip_start_code(&data, 2), Some(5));
    }

    #[test]
    fn skip_start_code_too_short_for_3byte() {
        // The guard `pos + 2 >= data.len()` rejects when fewer than 3 bytes
        // remain. pos=0, len=2 → 2 >= 2 → None (a 00 00 with no room for 01).
        let data = [0x00, 0x00];
        assert_eq!(skip_start_code(&data, 0), None);
    }

    #[test]
    fn skip_4byte_with_01_as_last_byte_returns_one_past_end() {
        // `00 00 00 01` of length exactly 4: the 4-byte branch guard is
        // `pos + 3 < data.len()` (3 < 4 = true) AND data[2]==0x00, data[3]==0x01
        // → 4-byte code recognised → returns pos+4 = 4 (one past the buffer, the
        // position where the NAL body would begin). The caller treats len as the
        // empty-NAL boundary, so this is in-bounds-safe.
        let data = [0x00, 0x00, 0x00, 0x01];
        assert_eq!(skip_start_code(&data, 0), Some(4));
    }

    #[test]
    fn skip_3byte_with_exactly_three_bytes() {
        // Minimum 3-byte code with no trailing payload: guard pos+2>=len is
        // 2>=3 = false, data[2]==0x01 → Some(3) (== len, the next-byte position).
        let data = [0x00, 0x00, 0x01];
        assert_eq!(skip_start_code(&data, 0), Some(3));
    }

    #[test]
    fn skip_start_code_first_byte_nonzero() {
        // A position whose first byte isn't 0x00 is not a start code.
        let data = [0x01, 0x00, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), None);
    }

    #[test]
    fn skip_start_code_second_byte_nonzero() {
        // 00 XX 01 with XX != 00 is not a start code (both forms need 00 00).
        let data = [0x00, 0x01, 0x01, 0x65];
        assert_eq!(skip_start_code(&data, 0), None);
    }
}

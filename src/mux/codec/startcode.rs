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

/// Minimal MSB-first bit reader over an RBSP, for the leading fields of a coded
/// slice header (H.264 `first_mb_in_slice` + `slice_type`; HEVC
/// `slice_segment_header`).
///
/// It does NOT remove emulation-prevention bytes (`00 00 03`). Those can only
/// appear after two consecutive `0x00` bytes, which cannot occur within the
/// first Exp-Golomb codes of a slice header (a slice header never begins
/// `00 00`), so the leading fields this reader is used for decode correctly. A
/// caller reading deep enough into a header that `00 00 03` could appear must
/// de-emulate the RBSP first.
pub(crate) struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
}

impl<'a> BitReader<'a> {
    /// Reader positioned at the first bit of `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, bit: 0 }
    }

    /// Read a single bit, MSB-first. `None` once the buffer is exhausted.
    pub fn read_bit(&mut self) -> Option<u32> {
        let byte = self.bit / 8;
        if byte >= self.data.len() {
            return None;
        }
        let b = (self.data[byte] >> (7 - (self.bit & 7))) & 1;
        self.bit += 1;
        Some(b as u32)
    }

    /// Skip `n` bits; `None` if that would run past the end.
    pub fn skip_bits(&mut self, n: usize) -> Option<()> {
        for _ in 0..n {
            self.read_bit()?;
        }
        Some(())
    }

    /// Read an unsigned Exp-Golomb code `ue(v)` (H.264 §9.1 / HEVC §9.2):
    /// count leading zeros, read the `1` stop bit, then that many info bits;
    /// `code_num = 2^leadingZeros - 1 + info`. `None` on truncation or an
    /// absurdly long code (>31 leading zeros — malformed input, not a real
    /// slice header).
    pub fn read_ue(&mut self) -> Option<u32> {
        let mut leading_zeros = 0u32;
        while self.read_bit()? == 0 {
            leading_zeros += 1;
            if leading_zeros > 31 {
                return None;
            }
        }
        let mut info = 0u32;
        for _ in 0..leading_zeros {
            info = (info << 1) | self.read_bit()?;
        }
        Some((1u32 << leading_zeros) - 1 + info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_reader_read_ue_exp_golomb_table() {
        // ue(v) codes from H.264 Table 9-1: code_num 0='1', 1='010', 2='011',
        // 3='00100'. Each crafted byte is left-aligned (MSB-first).
        assert_eq!(BitReader::new(&[0x80]).read_ue(), Some(0)); // 1_______
        assert_eq!(BitReader::new(&[0x40]).read_ue(), Some(1)); // 010_____
        assert_eq!(BitReader::new(&[0x60]).read_ue(), Some(2)); // 011_____
        assert_eq!(BitReader::new(&[0x20]).read_ue(), Some(3)); // 00100___
        assert_eq!(BitReader::new(&[0x28]).read_ue(), Some(4)); // 00101___
    }

    #[test]
    fn bit_reader_read_ue_sequence_and_bits() {
        // '1' '011' '00101' = ue(0), ue(2), ue(4) across the bitstream.
        // 1 011 00101 -> 1011 0010 1 -> 0xB2, 0x80.
        let mut br = BitReader::new(&[0xB2, 0x80]);
        assert_eq!(br.read_ue(), Some(0));
        assert_eq!(br.read_ue(), Some(2));
        assert_eq!(br.read_ue(), Some(4));
    }

    #[test]
    fn bit_reader_truncation_and_skip() {
        // Empty buffer → None, no panic.
        assert_eq!(BitReader::new(&[]).read_ue(), None);
        // skip_bits past the end → None.
        let mut br = BitReader::new(&[0xFF]);
        assert_eq!(br.skip_bits(9), None);
        // read_bit MSB-first.
        let mut b = BitReader::new(&[0b1010_0000]);
        assert_eq!(b.read_bit(), Some(1));
        assert_eq!(b.read_bit(), Some(0));
        assert_eq!(b.read_bit(), Some(1));
    }

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

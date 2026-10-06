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
    // `00 00 00 01` len 4: guard `pos + 3 < data.len()` (3<4) with data[2]=0x00,
    // data[3]=0x01 recognises the 4-byte code and returns pos+4=4, one past the
    // buffer; the caller treats len as the empty-NAL boundary (in-bounds-safe).
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

#[test]
fn skip_start_code_three_zeros_no_terminator_is_not_a_code() {
    // `00 00 00` is neither a 3-byte code (3rd byte isn't 0x01) nor a complete
    // 4-byte code (no 4th byte). Pins `pos + 3 < data.len()`: here it equals
    // len, so the guard must be strict `<`, or the 4-byte branch reads OOB.
    let data = [0x00, 0x00, 0x00];
    assert_eq!(skip_start_code(&data, 0), None);
}

#[test]
fn read_ue_thirty_one_leading_zeros_is_still_a_valid_code() {
    // 31 leading zero bits, a stop bit, 31 zero info bits: a legal ue(v) code
    // with code_num = 2^31 - 1. The truncation guard `leading_zeros > 31` must
    // NOT trip on 31 — only 32; a `>= 31` mutant aborts one bit early.
    let data = [0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(BitReader::new(&data).read_ue(), Some(u32::MAX >> 1));
}

#[test]
fn read_ue_thirty_two_leading_zeros_is_rejected() {
    // 32 zeros, a stop bit, 32 info bits: one past the cap, so None (not a code
    // whose `1 << 32` would overflow).
    let data = [0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(BitReader::new(&data).read_ue(), None);
}

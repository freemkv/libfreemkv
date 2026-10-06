//! Unit tests for [`parse_sense`]. Covers both SPC-4 sense data
//! formats (descriptor / fixed) and the short-buffer fallback. The
//! same helper runs on every platform backend so a regression here
//! would silently miscategorize SCSI errors on Linux, macOS, and
//! Windows simultaneously.
use super::parse_sense;
fn parse_sense_key(sense: &[u8], sb_len_wr: u8) -> u8 {
    parse_sense(sense, sb_len_wr).sense_key
}

/// Helper: build a 32-byte sense buffer whose first three bytes are
/// the given prefix; the rest are zeroes (sense data area).
fn buf(b0: u8, b1: u8, b2: u8) -> [u8; 32] {
    let mut s = [0u8; 32];
    s[0] = b0;
    s[1] = b1;
    s[2] = b2;
    s
}

#[test]
fn descriptor_format_72_picks_byte_1() {
    // Response code 0x72 (current, descriptor): sense key is the
    // low nibble of byte 1. Byte 2 here is 0x77 to prove it is NOT
    // the byte the parser reads.
    let s = buf(0x72, 0x05, 0x77); // ILLEGAL REQUEST
    assert_eq!(parse_sense_key(&s, 8), 5);
}

#[test]
fn descriptor_format_73_picks_byte_1() {
    // Response code 0x73 (deferred, descriptor): same parse rule
    // as 0x72.
    let s = buf(0x73, 0x06, 0xFF); // UNIT ATTENTION
    assert_eq!(parse_sense_key(&s, 8), 6);
}

#[test]
fn fixed_format_70_picks_byte_2() {
    // Response code 0x70 (current, fixed): sense key is the low
    // nibble of byte 2. Byte 1 is 0x77 to prove it is NOT read.
    let s = buf(0x70, 0x77, 0x05); // ILLEGAL REQUEST
    assert_eq!(parse_sense_key(&s, 18), 5);
}

#[test]
fn fixed_format_71_picks_byte_2() {
    // Response code 0x71 (deferred, fixed): same parse as 0x70.
    let s = buf(0x71, 0x77, 0x02); // NOT READY
    assert_eq!(parse_sense_key(&s, 18), 2);
}

#[test]
fn high_bit_in_byte_0_is_masked() {
    // SPC-4 sets the top bit of byte 0 ("INFORMATION VALID" / "VALID")
    // independently of the response code. parse_sense_key must mask
    // it off before classifying the format.
    let s = buf(0xF2, 0x05, 0x77);
    assert_eq!(
        parse_sense_key(&s, 8),
        5,
        "VALID-bit must not leak into format detection"
    );
    let s = buf(0xF0, 0x77, 0x02);
    assert_eq!(parse_sense_key(&s, 18), 2);
}

#[test]
fn high_nibble_in_key_byte_is_masked() {
    // Sense key is byte_n & 0x0F (low nibble). Top nibble holds
    // FILEMARK / EOM / ILI / SDAT_OVFL flags, which must not bleed
    // into the key value.
    let s = buf(0x70, 0x00, 0xE5); // 0xE0 flags + key 5
    assert_eq!(parse_sense_key(&s, 18), 5);
}

#[test]
fn sb_len_wr_zero_returns_no_sense() {
    // Transport set status non-zero but wrote zero sense bytes —
    // SPC-4 §4.5.3 says treat as NO SENSE (key 0).
    let s = buf(0x72, 0x05, 0x05);
    assert_eq!(parse_sense_key(&s, 0), 0);
}

#[test]
fn sb_len_wr_below_three_returns_no_sense() {
    // Less than three bytes in the buffer means we can't safely
    // read either format byte 0 or key byte 2 — return 0.
    let s = buf(0x72, 0x05, 0x05);
    assert_eq!(parse_sense_key(&s, 1), 0);
    assert_eq!(parse_sense_key(&s, 2), 0);
}

#[test]
fn slice_below_three_returns_no_sense() {
    // Defense-in-depth: even if a caller passes a too-short slice
    // with a falsely-large sb_len_wr, we don't panic and we return 0.
    let s = [0x72u8, 0x05];
    assert_eq!(parse_sense_key(&s, 8), 0);
}

#[test]
fn unknown_response_code_falls_through_to_fixed() {
    // SPC-4 mandates implementations tolerate unknown response
    // codes and treat them as fixed format. Vendor-specific codes
    // in the 0x74..0x7E range surface here.
    let s = buf(0x7A, 0x77, 0x03); // MEDIUM ERROR via "fixed"
    assert_eq!(parse_sense_key(&s, 18), 3);
}

// ── Additional parse_sense coverage ─────────────────────────────

/// Full 32-byte buffer to write arbitrary offsets into.
fn buf32() -> [u8; 32] {
    [0u8; 32]
}

#[test]
fn descriptor_format_reads_asc_byte2_ascq_byte3() {
    // SPC-4 §4.5.2.1 descriptor format: ASC at offset 2, ASCQ at
    // offset 3. Build 04/3E (NOT READY / logical unit not ready,
    // command in progress) — the BU40N bad-sector signature.
    let mut s = buf32();
    s[0] = 0x72;
    s[1] = 0x02; // NOT READY
    s[2] = 0x3E; // ASC
    s[3] = 0x01; // ASCQ
    let d = parse_sense(&s, 8);
    assert_eq!(d.sense_key, 2);
    assert_eq!(d.asc, 0x3E, "descriptor ASC is byte 2");
    assert_eq!(d.ascq, 0x01, "descriptor ASCQ is byte 3");
}

#[test]
fn descriptor_format_key_nibble_masked() {
    // Byte 1's upper nibble is reserved in descriptor format; the
    // parser masks &0x0F unconditionally, so set garbage there and
    // confirm it doesn't leak into the decoded sense key.
    let mut s = buf32();
    s[0] = 0x72;
    s[1] = 0xF3; // upper nibble garbage + key 3 (MEDIUM ERROR)
    s[2] = 0x11;
    s[3] = 0x05;
    let d = parse_sense(&s, 8);
    assert_eq!(d.sense_key, 3);
}

#[test]
fn descriptor_n_exactly_3_ascq_defaults_zero() {
    // Descriptor needs byte 3 for ASCQ; with only 3 bytes written
    // the doc contract says ASCQ defaults to 0 rather than reading
    // uninitialised byte 3. ASC (byte 2) is still valid.
    let mut s = buf32();
    s[0] = 0x72;
    s[1] = 0x03;
    s[2] = 0x11;
    s[3] = 0x05; // present in buffer but n=3 must NOT read it
    let d = parse_sense(&s, 3);
    assert_eq!(d.sense_key, 3);
    assert_eq!(d.asc, 0x11);
    assert_eq!(d.ascq, 0, "n=3 must not reach descriptor ASCQ at offset 3");
}

#[test]
fn fixed_format_full_reads_asc_byte12_ascq_byte13() {
    // SPC-4 §4.5.3 fixed format: key at byte 2, ASC at byte 12,
    // ASCQ at byte 13. Build 03/11/05 = MEDIUM ERROR / UNRECOVERED
    // READ ERROR / L-EC UNCORRECTABLE.
    let mut s = buf32();
    s[0] = 0x70;
    s[2] = 0x03;
    s[12] = 0x11;
    s[13] = 0x05;
    let d = parse_sense(&s, 18);
    assert_eq!(d.sense_key, 3);
    assert_eq!(d.asc, 0x11, "fixed ASC is byte 12");
    assert_eq!(d.ascq, 0x05, "fixed ASCQ is byte 13");
}

#[test]
fn fixed_format_short_buffer_asc_ascq_default_zero() {
    // Fixed format needs n>=13 for ASC, n>=14 for ASCQ. A short reply
    // (e.g. an 8-byte sense, common from some bridges) must yield
    // asc=ascq=0, never read past the written region.
    let mut s = buf32();
    s[0] = 0x70;
    s[2] = 0x04; // HARDWARE ERROR
    s[12] = 0xAA; // present in array but n must gate it off
    s[13] = 0xBB;
    let d = parse_sense(&s, 8);
    assert_eq!(d.sense_key, 4);
    assert_eq!(d.asc, 0, "n=8 < 13: ASC must default 0");
    assert_eq!(d.ascq, 0, "n=8 < 14: ASCQ must default 0");
}

#[test]
fn fixed_format_n13_reads_asc_but_not_ascq() {
    // Boundary: n==13 means bytes 0..12 inclusive are valid, so ASC
    // (byte 12) is readable but ASCQ (byte 13) is not. Exercises the
    // distinct n>=13 vs n>=14 guards.
    let mut s = buf32();
    s[0] = 0x70;
    s[2] = 0x03;
    s[12] = 0x11;
    s[13] = 0x05; // must NOT be read at n=13
    let d = parse_sense(&s, 13);
    assert_eq!(d.asc, 0x11, "n=13 reaches ASC at offset 12");
    assert_eq!(d.ascq, 0, "n=13 does not reach ASCQ at offset 13");
}

#[test]
fn fixed_format_n14_reads_both() {
    // Boundary: n==14 is the minimum for a complete fixed ASC/ASCQ.
    let mut s = buf32();
    s[0] = 0x70;
    s[2] = 0x03;
    s[12] = 0x11;
    s[13] = 0x05;
    let d = parse_sense(&s, 14);
    assert_eq!(d.asc, 0x11);
    assert_eq!(d.ascq, 0x05, "n=14 reaches ASCQ at offset 13");
}

#[test]
fn n_exactly_three_decodes_key_only() {
    // n==3 is the minimum that passes the n<3 early-return. For fixed
    // format the key (byte 2) is decodable; asc/ascq default to 0.
    let s = buf(0x70, 0x77, 0x06); // UNIT ATTENTION
    let d = parse_sense(&s, 3);
    assert_eq!(d.sense_key, 6);
    assert_eq!(d.asc, 0);
    assert_eq!(d.ascq, 0);
}

#[test]
fn descriptor_high_bit_set_on_72_still_descriptor() {
    // 0xF2 = VALID bit | 0x72. After masking 0x7F the response code
    // is 0x72 (descriptor), so ASC/ASCQ come from bytes 2/3, not
    // 12/13. Put a fixed-format ASC at byte 12 to prove it's ignored.
    let mut s = buf32();
    s[0] = 0xF2;
    s[1] = 0x03;
    s[2] = 0x11; // descriptor ASC
    s[3] = 0x05;
    s[12] = 0x99; // would be ASC if mis-parsed as fixed
    let d = parse_sense(&s, 18);
    assert_eq!(d.asc, 0x11, "VALID-bit masking must keep descriptor parse");
}

#[test]
fn empty_slice_returns_none() {
    // Defense-in-depth: zero-length slice with any sb_len_wr must not
    // panic and returns the all-zero triple.
    let s: [u8; 0] = [];
    let d = parse_sense(&s, 32);
    assert_eq!(d, super::ScsiSense::NONE);
}

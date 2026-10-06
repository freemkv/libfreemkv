use super::*;

/// Finding 17: a Unit_Key_RO.inf that parses its key list fully but declares
/// more title entries than the buffer can hold is TRUNCATED and must be
/// rejected. Pre-fix the loop silently skipped the missing entries and
/// returned `Some` with a short title→CPS map (mirrors the key-list check).
#[test]
fn parse_unit_key_ro_rejects_truncated_title_table() {
    let uk_pos = 26usize;
    let mut data = vec![0u8; 90];
    data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes()); // key storage @26
    data[24..26].copy_from_slice(&20u16.to_be_bytes()); // num_titles=20 (needs bytes to 106)
    data[uk_pos..uk_pos + 2].copy_from_slice(&1u16.to_be_bytes()); // num_uk=1
    // One present key at uk_pos+48 = 74..90 (V10 48-byte stride) so the key
    // list parses fully; only the title table overruns the 90-byte buffer.
    data[74..90].copy_from_slice(&[0xAB; 16]);
    assert!(
        parse_unit_key_ro(&data, AacsVersion::V10).is_none(),
        "a title table that runs past the buffer is truncated → reject"
    );
}

/// Fixture guard for the test above: the SAME buffer with an honest title
/// count parses to `Some`, proving it is only the truncation that is rejected
/// (not some unrelated malformation).
#[test]
fn parse_unit_key_ro_accepts_the_same_buffer_with_an_honest_title_count() {
    let uk_pos = 26usize;
    let mut data = vec![0u8; 90];
    data[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
    data[24..26].copy_from_slice(&15u16.to_be_bytes()); // 15 titles: last entry ends at 88 <= 90
    data[uk_pos..uk_pos + 2].copy_from_slice(&1u16.to_be_bytes());
    data[74..90].copy_from_slice(&[0xAB; 16]);
    let ukf = parse_unit_key_ro(&data, AacsVersion::V10).expect("fits → Some");
    assert_eq!(ukf.encrypted_keys.len(), 1);
    assert_eq!(ukf.title_cps_unit.len(), 2 + 15); // first_play + top_menu + 15 titles
}

/// Finding 15: a malformed key-storage offset pointing far past the buffer
/// must be rejected, never indexed. The `checked_add` guards also stop the
/// offset arithmetic (`uk_pos + 2`, `uk_pos + 48`) from overflowing `usize`
/// on 32-bit targets rather than panicking.
#[test]
fn parse_unit_key_ro_rejects_out_of_range_key_offset() {
    let mut data = vec![0u8; 32];
    data[0..4].copy_from_slice(&u32::MAX.to_be_bytes()); // uk_pos = 0xFFFFFFFF
    assert!(
        parse_unit_key_ro(&data, AacsVersion::V10).is_none(),
        "a key-storage offset past the buffer must not be indexed"
    );
}

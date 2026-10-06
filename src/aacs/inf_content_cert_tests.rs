use super::*;

#[test]
fn parse_content_cert_reads_cc_id_from_bytes_14_to_20() {
    let mut data = vec![0u8; 20];
    data[1] = 0x80;
    for (i, b) in data[14..20].iter_mut().enumerate() {
        *b = 0xA0 + i as u8;
    }
    data[13] = 0xEE;
    let cert = parse_content_cert(&data).expect("20-byte cert parses");
    assert_eq!(cert.cc_id, [0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5]);
    assert!(cert.bus_encryption);
    assert!(
        parse_content_cert(&data[..19]).is_none(),
        "19 bytes is short"
    );
}

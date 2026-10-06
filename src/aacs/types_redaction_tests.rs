use super::*;

// Sentinel byte 0xD5 = decimal 213: a derived `Debug` prints `[u8;N]` as
// decimal, so a leak surfaces "213" (no non-secret field is 213). Each type
// must also carry a "redacted" marker so re-adding `#[derive(Debug)]` fails.
const S: u8 = 0xD5;

fn assert_redacted(what: &str, dbg: &str) {
    assert!(
        !dbg.contains("213"),
        "{what}: Debug leaked key bytes (found decimal 213): {dbg}"
    );
    assert!(
        dbg.contains("redacted"),
        "{what}: Debug missing redaction marker: {dbg}"
    );
}

#[test]
fn device_key_debug_is_redacted() {
    let d = DeviceKey {
        key: [S; 16],
        node: 1,
        uv: 2,
        u_mask_shift: 3,
    };
    assert_redacted("DeviceKey", &format!("{d:?}"));
}

#[test]
fn host_cert_debug_is_redacted() {
    let h = HostCert {
        private_key: [S; 20],
        certificate: vec![0u8; 92],
        private_key_v2: Some([S; 32]),
        certificate_v2: None,
    };
    assert_redacted("HostCert", &format!("{h:?}"));
}

#[test]
fn newtype_keys_debug_is_redacted() {
    assert_redacted("Vid", &format!("{:?}", Vid([S; 16])));
    assert_redacted("MediaKey", &format!("{:?}", MediaKey([S; 16])));
    assert_redacted("Vuk", &format!("{:?}", Vuk([S; 16])));
    assert_redacted("ProcessingKey", &format!("{:?}", ProcessingKey([S; 16])));
}

#[test]
fn unit_key_debug_is_redacted() {
    assert_redacted("UnitKey", &format!("{:?}", UnitKey::new(0, [S; 16])));
}

#[test]
fn disc_entry_debug_is_redacted() {
    let e = DiscEntry {
        disc_hash: "0xAA".into(),
        title: "T".into(),
        media_key: Some([S; 16]),
        disc_id: Some([S; 16]),
        vuk: Some([S; 16]),
        unit_keys: vec![(1, [S; 16])],
    };
    assert_redacted("DiscEntry", &format!("{e:?}"));
}

use super::*;

#[test]
fn unsupported_and_missing_secrets_are_different() {
    let mut profile = BdPlusPlayerProfile::default();
    assert_eq!(
        profile.unwrap_aes_key(4, &[0; 16]),
        PlayerReply::Unsupported
    );
    profile.declare_aes_key(4);
    assert_eq!(profile.unwrap_aes_key(4, &[0; 16]), PlayerReply::Missing);
    profile.set_aes_key(4, [0; 16]);
    // AES-128 published all-zero known-answer vector, used in reverse.
    let ciphertext = [
        0x66, 0xe9, 0x4b, 0xd4, 0xef, 0x8a, 0x2c, 0x3b, 0x88, 0x4c, 0xfa, 0x59, 0xca, 0x34, 0x2b,
        0x2e,
    ];
    assert_eq!(
        profile.unwrap_aes_key(4, &ciphertext),
        PlayerReply::Ready([0; 16])
    );
    assert_eq!(
        profile.unwrap_aes_key(7, &[0; 16]),
        PlayerReply::Unsupported
    );
}

#[test]
fn memory_regions_are_modeled_bounded_and_never_overlap() {
    let mut profile = BdPlusPlayerProfile::default();
    profile.add_memory(0x100, vec![1, 2, 3, 4]).unwrap();
    assert_eq!(profile.memory(0x101, 2), PlayerReply::Ready(&[2, 3][..]));
    assert_eq!(profile.memory(0xff, 1), PlayerReply::Missing);
    assert_eq!(profile.memory(0x103, 2), PlayerReply::Missing);
    assert!(profile.add_memory(0xff, vec![9; 2]).is_err());
    assert!(profile.add_memory(u32::MAX, vec![9; 2]).is_err());
    assert_eq!(
        profile.memory(0x100, 4),
        PlayerReply::Ready(&[1, 2, 3, 4][..])
    );
}

#[test]
fn empty_response_is_present_and_debug_redacts_values() {
    let mut profile = BdPlusPlayerProfile::default();
    profile.set_discovery(1, 1, vec![]);
    profile.set_discovery(1, 2, b"secret-discovery".to_vec());
    assert_eq!(profile.discovery(1, 1), PlayerReply::Ready(&[][..]));
    assert_eq!(profile.discovery(1, 3), PlayerReply::Missing);
    assert!(!format!("{profile:?}").contains("secret-discovery"));
}

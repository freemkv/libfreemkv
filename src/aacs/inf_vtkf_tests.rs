use super::*;

// Synthesize a `VTKF%%%.AACS`: magic, BE32 size, playlist name, reserved to 0x80, 64 entry
// slots (first `keys.len()` present), MAC trailer.
fn synth_vtkf(keys: &[[u8; 16]]) -> Vec<u8> {
    const FILE_LEN: usize = 2480;
    let mut v = Vec::new();
    v.extend_from_slice(VTKF_MAGIC); // 0x00
    v.extend_from_slice(&(FILE_LEN as u32).to_be_bytes()); // 0x0C HD_VTKF_SIZE
    v.extend_from_slice(b"VPLST000.XPL"); // 0x10 playlist name
    v.resize(VTKF_HEADER_LEN, 0); // reserve to first entry (0x80)
    for n in 0..VTKF_MAX_ENTRIES {
        if let Some(k) = keys.get(n) {
            v.push(VTKF_AV_FLG); // BIFO: AV_FLG set (present)
            v.extend_from_slice(&[0, 0, 0]); // reserved
            v.extend_from_slice(k); // 16-byte encrypted title key
            v.extend_from_slice(&[0xFFu8; 16]); // binding MAC (0xFF, pre-recorded)
        } else {
            v.extend_from_slice(&[0u8; VTKF_ENTRY_LEN]); // empty slot (AV_FLG clear)
        }
    }
    v.resize(FILE_LEN - 16, 0); // reserved gap before the trailer
    v.extend_from_slice(&[0xABu8; 16]); // TKF MAC (must NOT be read as a key)
    v
}

#[test]
fn parse_vtkf_reads_present_entries_skips_empty_ignores_mac() {
    let k1 = [0x11u8; 16];
    let k2 = [0x22u8; 16];
    let k3 = [0x33u8; 16];
    let data = synth_vtkf(&[k1, k2, k3]);

    let ukf = parse_vtkf(&data).expect("valid VTKF must parse");
    // Exactly the three present entries — empty slots and the trailing 16-byte
    // TKF MAC are not mistaken for keys. k2/k3 are read at the 36-byte stride
    // (0xA4, 0xC8); the old 32-byte stride misread them from the prior MAC.
    assert_eq!(ukf.encrypted_keys.len(), 3);
    assert_eq!(
        ukf.encrypted_keys[0],
        (1, k1),
        "CPS units = 1-based slot index"
    );
    assert_eq!(ukf.encrypted_keys[1], (2, k2));
    assert_eq!(ukf.encrypted_keys[2], (3, k3));
    assert_eq!(ukf.version, AacsVersion::V10, "HD DVD is AACS 1.0");
    // disc_hash is SHA1 of the whole file (the KEYDB lookup key).
    assert_eq!(ukf.disc_hash, disc_hash(&data));
}

#[test]
fn parse_vtkf_reads_a_full_64_entry_file() {
    // Real discs (Freedom, Dukes) carry all 64 slots present. Every key must
    // come back, none dropped and none drifted — the regression the 32-byte
    // stride failed.
    let keys: Vec<[u8; 16]> = (0..VTKF_MAX_ENTRIES).map(|n| [n as u8; 16]).collect();
    let ukf = parse_vtkf(&synth_vtkf(&keys)).expect("64-entry VTKF");
    assert_eq!(ukf.encrypted_keys.len(), 64);
    assert_eq!(
        ukf.encrypted_keys[63],
        (64, [63u8; 16]),
        "entry 64 at 0x{:x}",
        VTKF_HEADER_LEN + 63 * VTKF_ENTRY_LEN
    );
}

#[test]
fn parse_vtkf_rejects_a_file_cut_inside_the_entry_table() {
    let full = synth_vtkf(&[[0x11u8; 16], [0x22u8; 16]]);
    let cut = VTKF_HEADER_LEN + 10 * VTKF_ENTRY_LEN;
    assert!(parse_vtkf(&full[..cut]).is_none());
}

#[test]
fn parse_vtkf_rejects_non_magic() {
    let mut data = synth_vtkf(&[[0x11u8; 16]]);
    data[0] = b'X'; // corrupt magic
    assert!(
        parse_vtkf(&data).is_none(),
        "non-VTKF magic must be rejected"
    );
    assert!(
        parse_vtkf(&[0u8; 4]).is_none(),
        "too short must be rejected"
    );
}

#[test]
fn parse_title_keys_dispatches_by_magic() {
    // VTKF magic → parse_vtkf.
    let data = synth_vtkf(&[[0x44u8; 16], [0x55u8; 16]]);
    let ukf = parse_title_keys(&data, AacsVersion::V10).expect("VTKF dispatch");
    assert_eq!(ukf.encrypted_keys.len(), 2);

    // Non-VTKF → parse_unit_key_ro (a 2-byte buffer is not a valid inf, so
    // this proves it ROUTED to the BD parser rather than parse_vtkf).
    assert!(
        parse_title_keys(&[0x00, 0x00], AacsVersion::V10).is_none(),
        "non-magic input must route to parse_unit_key_ro"
    );
}

/// The whole point of the seam: a parsed VTKF feeds the SHARED VUK→title-key
/// crypto (`decrypt_unit_key`) exactly like a BD `Unit_Key_RO.inf` would —
/// no HD-DVD-specific crypto path.
#[test]
fn vtkf_encrypted_keys_feed_shared_vuk_unwrap() {
    let enc = [0x9Au8; 16];
    let data = synth_vtkf(&[enc]);
    let ukf = parse_vtkf(&data).unwrap();
    let vuk = [0x5Cu8; 16];
    let derived = super::super::derive::decrypt_unit_key(&vuk, &ukf.encrypted_keys[0].1);
    // Same as applying the shared unwrap directly to the stored enc key.
    assert_eq!(derived, super::super::derive::decrypt_unit_key(&vuk, &enc));
}

// Sentinel byte 0xD5 = decimal 213: a derived Debug would render it in decimal.
#[test]
fn unit_key_file_debug_is_redacted() {
    let f = UnitKeyFile {
        disc_hash: [0xD5; 20],
        app_type: 1,
        num_bdmv_dir: 1,
        use_skb_mkb: false,
        version: AacsVersion::V20,
        encrypted_keys: vec![(0, [0xD5; 16]), (1, [0xD5; 16])],
        title_cps_unit: vec![0, 1],
    };
    let dbg = format!("{f:?}");
    assert!(
        !dbg.contains("213"),
        "UnitKeyFile Debug leaked key bytes (decimal 213): {dbg}"
    );
    assert!(
        dbg.contains("redacted"),
        "UnitKeyFile Debug missing redaction marker: {dbg}"
    );
    // Non-secret shape is still useful for diagnostics.
    assert!(dbg.contains("encrypted_keys_len: 2"), "{dbg}");
}

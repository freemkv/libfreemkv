use super::*;

fn movie_object(destination: u32) -> Vec<u8> {
    let mut bytes = b"MOBJ0300".to_vec();
    bytes.resize(40, 0);
    bytes.extend_from_slice(&22u32.to_be_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0, 0, 1, 0x60, 0, 0, 1]);
    bytes.extend_from_slice(&[0x50, 0x40, 0, 1]);
    bytes.extend_from_slice(&destination.to_be_bytes());
    bytes.extend_from_slice(&0u32.to_be_bytes());
    bytes
}

#[test]
fn hdmv_register_preservation_rejects_writes_swaps_and_unknown_effects() {
    assert!(hdmv_preserves(&movie_object(9), 0, &[11, 14, 15]).is_ok());
    for destination in [11, 14, 15, 4096, 0x8000_000b] {
        assert!(hdmv_preserves(&movie_object(destination), 0, &[11, 14, 15]).is_err());
    }
    for (offset, value) in [(57, 2), (54, 0x51), (55, 0x50), (56, 0x80)] {
        let mut bytes = movie_object(9);
        bytes[offset] = value;
        assert!(hdmv_preserves(&bytes, 0, &[11, 14, 15]).is_err());
    }
    assert!(hdmv_preserves(&movie_object(9), 1, &[11]).is_err());
    let mut bytes = movie_object(9);
    bytes.push(0);
    assert!(hdmv_preserves(&bytes, 0, &[11]).is_err());
}

#[test]
#[ignore = "bounded extracted metadata only; no JVM or media execution"]
fn actual_register_writes_only_clear_and_hdmv_preserves_protected_registers() {
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    let directory = jar.parent().unwrap();
    let assets = super::super::assets::load(|path, limit| {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(directory.join(path.rsplit('/').next().unwrap()))
            .map_err(|_| Reject::MissingAsset)?
            .take(limit as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Reject::Invalid)?;
        Ok(bytes)
    })
    .unwrap();
    let fs = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&fs).unwrap();
    assert_eq!(zero_writes(&program).unwrap(), BTreeSet::from([15]));
    hdmv_preserves(&assets.movie_objects, assets.first_play, &[11, 14, 15]).unwrap();
    let mut changed = qco::parse(&fs).unwrap();
    let mut writer = changed.global.functions[89].to_vec();
    assert_eq!(&writer[228..230], [1, 0]);
    writer[229] = 1;
    changed.global.functions[89] = &writer;
    assert!(zero_writes(&changed).is_err());
    let extra = [0, 0, 0, 1, 1, 3, 0xc0, 0x24, 0x2e, 0x3f];
    let mut changed = qco::parse(&fs).unwrap();
    changed.global.functions.push(&extra);
    assert!(zero_writes(&changed).is_err());
}

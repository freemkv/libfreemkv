use super::*;
use crate::css::{RECRACK_ATTEMPTS, dvd_pack_header, keyless, lfsr};

const A: [u8; 5] = [0x42, 0x13, 0x37, 0xBE, 0xEF];
const B: [u8; 5] = [0x07, 0x5A, 0xC3, 0x10, 0x88];

fn keys(key: [u8; 5]) -> DecryptKeys {
    DecryptKeys::Css { title_key: key }
}

// Keep the oracle before encryption. The second header has no periodic tail,
// so only the persistent recovered key can open it.
fn sector(key: &[u8; 5], crib: bool) -> (Vec<u8>, Vec<u8>) {
    let mut plain = vec![0; 2048];
    dvd_pack_header(&mut plain, 0xE0);
    plain[0x54..0x59].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
    for (i, byte) in plain.iter_mut().enumerate().skip(0x59) {
        *byte = if crib { (i % 4) as u8 } else { (i % 251) as u8 };
    }
    let mut wire = plain.clone();
    lfsr::scramble_sector(key, &mut wire);
    assert!(crate::css::scrambled_at(&wire).is_some());
    assert_eq!(keyless::attack_crib(&wire).is_some(), crib);
    if crib {
        assert_eq!(keyless::crack_title_key(&wire), Some(*key));
    }
    (wire, plain)
}

fn disc_keys(spans: &[&[(u32, u32)]]) -> DiscKeys {
    DiscKeys {
        groups: spans
            .iter()
            .map(|spans| Group {
                spans: spans
                    .iter()
                    .map(|&(start_lba, sector_count)| Extent {
                        start_lba,
                        sector_count,
                    })
                    .collect(),
                keys: keys(A),
            })
            .collect(),
    }
}

fn recovered_owner(same_read: bool, grouped: bool) {
    let (wire1, plain1) = sector(&B, true);
    let (wire2, plain2) = sector(&B, false);
    let mut css = if grouped {
        disc_keys(&[&[(10, 2)]])
    } else {
        disc_keys(&[])
    };
    let mut fallback = keys(A);
    if same_read {
        let mut buf = [wire1, wire2].concat();
        css.decrypt(10, &mut buf, &mut fallback).unwrap();
        assert_eq!(buf, [plain1, plain2].concat());
    } else {
        let mut buf = wire1;
        css.decrypt(10, &mut buf, &mut fallback).unwrap();
        assert_eq!(buf, plain1);
        let owner = if grouped {
            &css.groups[0].keys
        } else {
            &fallback
        };
        assert!(
            matches!(owner, DecryptKeys::Css { title_key } if *title_key == B),
            "the recovered key must remain with its owner after the first read"
        );
        let mut buf = wire2;
        css.decrypt(11, &mut buf, &mut fallback).unwrap();
        assert_eq!(buf, plain2);
    }
}

#[test]
fn group_recovery_survives_sectors() {
    recovered_owner(true, true);
}

#[test]
fn group_recovery_survives_reads() {
    recovered_owner(false, true);
}

#[test]
fn fallback_recovery_survives_sectors() {
    recovered_owner(true, false);
}

#[test]
fn fallback_recovery_survives_reads() {
    recovered_owner(false, false);
}

fn false_crib() -> Vec<u8> {
    let mut wire = vec![0; 2048];
    wire[0x14] = 0x30;
    dvd_pack_header(&mut wire, crate::consts::pes_stream_id::PRIVATE_STREAM_1);
    for (i, b) in wire.iter_mut().enumerate().take(0x80).skip(0x20) {
        *b = (i % 4) as u8;
    }
    for (i, b) in wire.iter_mut().enumerate().skip(0x80) {
        *b = ((i * 37 + 11) % 251) as u8;
    }
    assert!(keyless::attack_crib(&wire).is_some());
    assert!(keyless::crack_title_key(&wire).is_none());
    wire
}

#[test]
fn same_owner_retry_cadence_includes_clear_sectors() {
    for grouped in [true, false] {
        let wire = false_crib();
        let mut css = if grouped {
            disc_keys(&[&[(10, 128)]])
        } else {
            disc_keys(&[])
        };
        let mut buf = Vec::new();
        for _ in 0..64 {
            buf.extend_from_slice(&wire);
            buf.extend_from_slice(&[0; 2048]);
        }
        RECRACK_ATTEMPTS.with(|c| c.set(0));
        css.decrypt(10, &mut buf, &mut keys(A)).unwrap();
        assert_eq!(RECRACK_ATTEMPTS.with(|c| c.get()), 4);
    }
}

#[test]
fn equal_key_bytes_do_not_merge_different_owners() {
    let wire = false_crib();
    let mut css = disc_keys(&[&[(10, 1)], &[(11, 1)]]);
    let mut buf = [wire.clone(), wire].concat();
    RECRACK_ATTEMPTS.with(|c| c.set(0));
    css.decrypt(10, &mut buf, &mut keys(A)).unwrap();
    assert_eq!(RECRACK_ATTEMPTS.with(|c| c.get()), 2);
}

#[test]
fn group_boundaries_return_to_cached_owner_and_preserve_tail() {
    let (b1, p1) = sector(&B, true);
    let (a, p2) = sector(&A, false);
    let (b2, p3) = sector(&B, false);
    // Group 0 wins the overlap at 10; after group 1 at 11, return to 0.
    let mut css = disc_keys(&[&[(10, 1), (12, 1)], &[(10, 2)]]);
    let tail = b2[..511].to_vec();
    let mut buf = [b1, a, b2, tail.clone()].concat();
    css.decrypt(10, &mut buf, &mut keys(A)).unwrap();
    assert_eq!(buf, [p1, p2, p3, tail].concat());
    // Addition past u32::MAX must not wrap back into group 1 at LBA 0.
    let mut css = disc_keys(&[&[(u32::MAX, 2)], &[(0, 1)]]);
    let (b1, p1) = sector(&B, true);
    let (b2, p2) = sector(&B, false);
    let mut buf = [b1, b2].concat();
    css.decrypt(u32::MAX, &mut buf, &mut keys(A)).unwrap();
    assert_eq!(buf, [p1, p2].concat());
}

fn prepared_reader(grouped: bool, same_read: bool) {
    let dir = tempfile::tempdir().unwrap();
    let video = dir.path().join("VIDEO_TS");
    std::fs::create_dir(&video).unwrap();
    let (title, _) = sector(&A, true);
    std::fs::write(video.join("VTS_01_1.VOB"), title).unwrap();
    let (wire1, plain1) = sector(&B, true);
    let (wire2, plain2) = sector(&B, false);
    let name = if grouped {
        "VTS_01_0.VOB"
    } else {
        "VIDEO_TS.VOB"
    };
    std::fs::write(video.join(name), [wire1, wire2].concat()).unwrap();
    let mut image = crate::DirImage::open(dir.path()).unwrap();
    let fs = crate::udf::read_filesystem(&mut image).unwrap();
    let extents = fs
        .file_extents(&mut image, &format!("/VIDEO_TS/{name}"))
        .unwrap();
    assert_eq!(extents.len(), 1);
    let lba = extents[0].0;
    let disc = crate::Disc {
        volume_id: String::new(),
        meta_title: None,
        format: crate::DiscFormat::Dvd,
        capacity_sectors: image.capacity_sectors(),
        capacity_bytes: u64::from(image.capacity_sectors()) * 2048,
        layers: 1,
        titles: Vec::new(),
        region: crate::disc::DiscRegion::Free,
        aacs: None,
        css: Some(crate::css::CssState {
            title_key: A,
            crack_span: None,
        }),
        encrypted: true,
        aacs_error: None,
        css_error: None,
        content_format: crate::ContentFormat::MpegPs,
    };
    let mut reader = crate::keys::KeyRing::none()
        .whole_disc_reader(&disc, image, None)
        .unwrap();
    if same_read {
        let mut buf = vec![0; 4096];
        assert_eq!(reader.read_sectors(lba, 2, &mut buf, false).unwrap(), 4096);
        assert_eq!(buf, [plain1, plain2].concat());
    } else {
        let mut buf = vec![0; 2048];
        assert_eq!(reader.read_sectors(lba, 1, &mut buf, false).unwrap(), 2048);
        assert_eq!(buf, plain1);
        assert_eq!(
            reader.read_sectors(lba + 1, 1, &mut buf, false).unwrap(),
            2048
        );
        assert_eq!(buf, plain2);
    }
}

#[test]
fn prepared_whole_disc_group_recovery_survives_sectors() {
    prepared_reader(true, true);
}

#[test]
fn prepared_whole_disc_group_recovery_survives_reads() {
    prepared_reader(true, false);
}

#[test]
fn prepared_whole_disc_fallback_recovery_survives_sectors() {
    prepared_reader(false, true);
}

#[test]
fn prepared_whole_disc_fallback_recovery_survives_reads() {
    prepared_reader(false, false);
}

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
        layer_map: None,
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

// Runs must equal first-match ownership sector by sector, for overlapping and adjacent spans.
#[test]
fn owner_runs_match_first_match_ownership() {
    let css = disc_keys(&[
        &[(10, 5), (30, 3)],
        &[(12, 10), (40, 2)],
        &[(22, 4), (26, 4), (33, 1)],
        &[(5, 30), (60, 1)],
    ]);
    for lba in 0..70u32 {
        for len in 1..40usize {
            let mut expect = Vec::new();
            for i in 0..len {
                let o = css.group_at(u64::from(lba) + i as u64);
                match expect.last_mut() {
                    Some((_, e, last)) if *last == o => *e = i + 1,
                    _ => expect.push((i, i + 1, o)),
                }
            }
            assert_eq!(css.runs(lba, len), expect, "lba {lba} len {len}");
        }
    }
}

const C: [u8; 5] = [0x13, 0x9D, 0x51, 0x6A, 0x2C];

// A whole-disc copy keys each VOB file from its own sectors: VIDEO_TS.VOB, a menu VOB and
// the title VOBs each under a different key, every one led by a crib-less pack only its own
// key opens; a clear menu passes through untouched. Live (no scanned key) and image alike.
fn per_file_keys(scanned: Option<[u8; 5]>) {
    let dir = tempfile::tempdir().unwrap();
    let video = dir.path().join("VIDEO_TS");
    std::fs::create_dir(&video).unwrap();
    let scrambled = |key: &[u8; 5]| {
        let (w1, p1) = sector(key, false);
        let (w2, p2) = sector(key, true);
        ([w1, w2].concat(), [p1, p2].concat())
    };
    let (_, clear) = sector(&A, false);
    assert!(crate::css::scrambled_at(&clear).is_none());
    let clear = [clear.clone(), clear].concat();
    let files = [
        ("VIDEO_TS.VOB", scrambled(&C)),
        ("VTS_01_0.VOB", scrambled(&B)),
        ("VTS_01_1.VOB", scrambled(&A)),
        ("VTS_02_0.VOB", (clear.clone(), clear)),
    ];
    for (name, (wire, _)) in &files {
        std::fs::write(video.join(name), wire).unwrap();
    }
    let mut image = crate::DirImage::open(dir.path()).unwrap();
    let fs = crate::udf::read_filesystem(&mut image).unwrap();
    let at: Vec<u32> = files
        .iter()
        .map(|(name, _)| {
            fs.file_extents(&mut image, &format!("/VIDEO_TS/{name}"))
                .unwrap()[0]
                .0
        })
        .collect();
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
        css: scanned.map(|title_key| crate::css::CssState {
            title_key,
            crack_span: None,
        }),
        encrypted: true,
        aacs_error: None,
        css_error: None,
        layer_map: None,
        content_format: crate::ContentFormat::MpegPs,
    };
    let mut reader = crate::keys::KeyRing::none()
        .whole_disc_reader(&disc, image, None)
        .unwrap();
    for ((name, (_, plain)), lba) in files.iter().zip(at) {
        let mut buf = vec![0; plain.len()];
        reader.read_sectors(lba, 2, &mut buf, false).unwrap();
        assert!(
            buf.chunks(2048)
                .all(|s| crate::css::scrambled_at(s).is_none()),
            "{name} still scrambled"
        );
        assert!(buf == *plain, "{name} not decrypted under its own key");
    }
}

#[test]
fn whole_disc_live_keys_every_vob_from_its_own_sectors() {
    per_file_keys(None);
}

#[test]
fn whole_disc_image_keys_every_vob_from_its_own_sectors() {
    per_file_keys(Some(A));
}

// A file whose crack saw only clear sectors keys a later scrambled pack from its own crib,
// and refuses one no crib opens rather than pass it through scrambled.
#[test]
fn clear_verdict_group_cracks_or_refuses_a_later_scrambled_pack() {
    let mut css = disc_keys(&[&[(10, 2)]]);
    css.groups[0].keys = DecryptKeys::None;
    let (w1, p1) = sector(&B, false);
    let (w2, p2) = sector(&B, true);
    let mut buf = [w1, w2].concat();
    css.decrypt(10, &mut buf, &mut DecryptKeys::None).unwrap();
    assert_eq!(buf, [p1, p2].concat());

    let mut css = disc_keys(&[&[(10, 1)]]);
    css.groups[0].keys = DecryptKeys::None;
    let (mut wire, _) = sector(&B, false);
    assert!(matches!(
        css.decrypt(10, &mut wire, &mut DecryptKeys::None),
        Err(Error::CssKeyMissing)
    ));
}

// A whole-disc copy of `files` (name, wire, plain) read back through the decrypting reader:
// `Err` when the reader refuses, else every file must come back as its plaintext.
fn whole_disc_copy(files: &[(&str, Vec<u8>, Vec<u8>)]) -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let video = dir.path().join("VIDEO_TS");
    std::fs::create_dir(&video).unwrap();
    for (name, wire, _) in files {
        std::fs::write(video.join(name), wire).unwrap();
    }
    let mut image = crate::DirImage::open(dir.path()).unwrap();
    let fs = crate::udf::read_filesystem(&mut image).unwrap();
    let at: Vec<u32> = files
        .iter()
        .map(|(name, ..)| {
            fs.file_extents(&mut image, &format!("/VIDEO_TS/{name}"))
                .unwrap()[0]
                .0
        })
        .collect();
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
        css: None,
        encrypted: true,
        aacs_error: None,
        css_error: None,
        layer_map: None,
        content_format: crate::ContentFormat::MpegPs,
    };
    let mut reader = crate::keys::KeyRing::none().whole_disc_reader(&disc, image, None)?;
    for ((name, _, plain), lba) in files.iter().zip(at) {
        let mut buf = vec![0; plain.len()];
        reader.read_sectors(lba, (plain.len() / 2048) as u16, &mut buf, false)?;
        assert!(buf == *plain, "{name} not decrypted");
    }
    Ok(())
}

// A title VOB under `key`: one crib pack (so it cracks) then checkable packs.
fn title_vob(key: &[u8; 5], seed: u32) -> (Vec<u8>, Vec<u8>) {
    let (w, p) = sector(key, true);
    let (cw, cp) = crate::css::checkable_sectors(key, 6, seed);
    ([w, cw].concat(), [p, cp].concat())
}

// The reported disc: a menu VOB with no crib, scrambled with its title set's key, is keyed
// with that key once the key verifies on the menu's own packs (DVD Decryptor's "Using
// decryption key from File: VTS_04_1.VOB").
#[test]
fn whole_disc_menu_without_crib_takes_its_title_sets_key() {
    let (tw, tp) = title_vob(&A, 1);
    let (mw, mp) = crate::css::checkable_sectors(&A, 8, 2);
    whole_disc_copy(&[("VTS_01_1.VOB", tw, tp), ("VTS_01_0.VOB", mw, mp)]).unwrap();
}

// A crib-less menu under another title set's key is keyed with that key once it verifies.
#[test]
fn whole_disc_menu_without_crib_takes_another_title_sets_verified_key() {
    let (aw, ap) = title_vob(&A, 1);
    let (bw, bp) = title_vob(&B, 2);
    let (mw, mp) = crate::css::checkable_sectors(&A, 8, 3);
    whole_disc_copy(&[
        ("VTS_01_1.VOB", aw, ap),
        ("VTS_02_1.VOB", bw, bp),
        ("VTS_02_0.VOB", mw, mp),
    ])
    .unwrap();
}

// A crib-less menu under a key no file on the disc has is refused, never copied scrambled
// or under a key that does not verify.
#[test]
fn whole_disc_menu_under_an_unknown_key_is_refused() {
    let (aw, ap) = title_vob(&A, 1);
    let (bw, bp) = title_vob(&B, 2);
    let (mw, mp) = crate::css::checkable_sectors(&C, 8, 3);
    let r = whole_disc_copy(&[
        ("VTS_01_1.VOB", aw, ap),
        ("VTS_02_1.VOB", bw, bp),
        ("VTS_02_0.VOB", mw, mp),
    ]);
    assert!(
        matches!(r, Err(Error::CssKeyMissing | Error::WholeDiscKeyMissing)),
        "{r:?}"
    );
}

// Read time: a group whose crack saw only clear sectors meets crib-less scrambled packs;
// another group's key that verifies on them opens them.
#[test]
fn clear_verdict_group_takes_a_verified_key_at_read_time() {
    let mut css = disc_keys(&[&[(10, 1)], &[(20, 8)]]);
    css.groups[1].keys = DecryptKeys::None;
    let (mut buf, plain) = crate::css::checkable_sectors(&A, 8, 4);
    css.decrypt(20, &mut buf, &mut DecryptKeys::None).unwrap();
    assert!(buf == plain);
}

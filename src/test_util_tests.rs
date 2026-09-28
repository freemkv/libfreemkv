use super::*;
use crate::aacs::content::{aacs_unit_encrypted, is_clean};
use crate::aacs::inf::parse_unit_key_ro;
use crate::disc::ContentFormat;

const K1: [u8; 16] = [0x11; 16];
const K2: [u8; 16] = [0x22; 16];

#[test]
fn unit_key_ro_parses_back_for_every_stride() {
    for version in [AacsVersion::V10, AacsVersion::V20, AacsVersion::V21] {
        let inf = unit_key_ro(version, &[K1, K2], &[2, 1, 2]);
        let ukf = parse_unit_key_ro(&inf, version).expect("parses");
        assert_eq!(ukf.encrypted_keys, vec![(1, K1), (2, K2)], "{version:?}");
        assert_eq!((ukf.app_type, ukf.num_bdmv_dir), (1, 1));
        // First Playback, Top Menu (CPS unit 1 → index 0), then the titles.
        assert_eq!(ukf.title_cps_unit, vec![0, 0, 1, 0, 1], "{version:?}");
        let start = u32::from_be_bytes(inf[..4].try_into().unwrap()) as usize;
        assert_eq!(
            start % 16,
            0,
            "KS-12: the start address is a multiple of 16"
        );
    }
}

#[test]
fn unit_key_ro_declares_a_count_with_placeholder_keys() {
    let inf = unit_key_ro(AacsVersion::V10, &[[0u8; 16]; 3], &[]);
    let ukf = parse_unit_key_ro(&inf, AacsVersion::V10).expect("parses");
    assert_eq!(ukf.encrypted_keys.len(), 3);
}

#[test]
fn aacs_state_defaults_and_every_setter() {
    let d = aacs_state().build();
    assert_eq!(
        (d.version, d.bus_encryption, d.mkb_version),
        (1, false, None)
    );
    assert!(d.disc_hash.is_empty() && d.unit_keys.is_empty() && d.vuk.is_none());
    assert_eq!(d.key_source, KeyOrigin::ExternalUk);
    assert_eq!(d.volume_id, [0u8; 16]);
    assert!(d.uk_ro.is_empty() && d.mkb.is_empty());
    let s = aacs_state()
        .version(2)
        .bus_encryption(true)
        .mkb_version(Some(77))
        .disc_hash("0xABC")
        .key_source(KeyOrigin::KeyDb)
        .vuk(Some([7; 16]))
        .unit_keys(vec![(1, K1)])
        .volume_id([9; 16])
        .uk_ro(vec![1, 2])
        .mkb(vec![3])
        .build();
    assert_eq!(
        (s.version, s.bus_encryption, s.mkb_version),
        (2, true, Some(77))
    );
    assert_eq!(s.disc_hash, "0xABC");
    assert_eq!(s.key_source, KeyOrigin::KeyDb);
    assert_eq!((s.vuk, s.volume_id), (Some([7; 16]), [9; 16]));
    assert_eq!(s.unit_keys, vec![(1, K1)]);
    assert_eq!((s.uk_ro, s.mkb), (vec![1, 2], vec![3]));
}

fn fixture() -> EncryptedBdImage {
    let inf = unit_key_ro(AacsVersion::V10, &[[0u8; 16]; 2], &[1, 2]);
    encrypted_bd_image(
        &[
            BdFile::new("BDMV/STREAM/00001.m2ts", 30, Some(K1)),
            BdFile::new("BDMV/STREAM/00002.m2ts", 30, Some(K2)),
            BdFile::new("BDMV/STREAM/00003.m2ts", 9, None),
        ],
        &inf,
    )
}

fn unit(img: &[u8], lba: u32) -> Vec<u8> {
    let at = lba as usize * SECTOR_BYTES;
    img[at..at + ALIGNED_UNIT_LEN].to_vec()
}

#[test]
fn encrypted_bd_image_encrypts_every_unit_on_its_files_grid() {
    let fx = fixture();
    assert_eq!(fx.image.len(), fx.plain.len());
    for (i, key) in [(0, Some(K1)), (1, Some(K2)), (2, None)] {
        let (start, sectors) = fx.files[i];
        assert_eq!(sectors % 3, 0);
        for u in 0..sectors / 3 {
            let lba = start + 3 * u;
            let (enc, plain) = (unit(&fx.image, lba), unit(&fx.plain, lba));
            assert!(is_clean(&plain, ContentFormat::BdTs), "plaintext is TS");
            for p in plain.chunks(BD_SOURCE_PACKET_BYTES) {
                let cpi = p[0] & 0xC0;
                assert_eq!(cpi, if key.is_some() { 0xC0 } else { 0 }, "KS-5 per packet");
            }
            match key {
                Some(k) => {
                    assert!(aacs_unit_encrypted(&enc, ContentFormat::BdTs));
                    assert_ne!(enc, plain, "file {i} unit {u} is ciphertext");
                    let mut d = enc.clone();
                    decrypt_unit(&mut d, &k);
                    assert_eq!(d, plain, "file {i} unit {u} opens with its key");
                }
                None => assert_eq!(enc, plain, "a clear file stays clear"),
            }
        }
    }
    // The other key does not open file 0.
    let mut d = unit(&fx.image, fx.files[0].0);
    decrypt_unit(&mut d, &K2);
    assert!(!is_clean(&d, ContentFormat::BdTs));
}

#[test]
fn encrypted_bd_image_is_a_readable_udf_with_the_unit_key_file() {
    let fx = fixture();
    let inf = unit_key_ro(AacsVersion::V10, &[[0u8; 16]; 2], &[1, 2]);
    let mut src = fx.source();
    let fs = crate::udf::read_filesystem(&mut src).expect("UDF");
    let (lba, n) = fs.file_extents(&mut src, "/AACS/Unit_Key_RO.inf").unwrap()[0];
    let mut got = vec![0u8; n as usize * SECTOR_BYTES];
    src.read_sectors(lba, n as u16, &mut got, false).unwrap();
    assert_eq!(&got[..inf.len()], &inf[..]);
    let files: Vec<_> = (1..=3)
        .map(|i| {
            fs.file_extents(&mut src, &format!("/BDMV/STREAM/0000{i}.m2ts"))
                .unwrap()[0]
        })
        .collect();
    assert_eq!(files, fx.files);
}

#[test]
fn counting_source_logs_every_read_after_it_moves() {
    let mut src = CountingSource::new(MemSource::new(vec![0u8; 10 * SECTOR_BYTES]));
    let log = src.log();
    let mut buf = vec![0u8; 3 * SECTOR_BYTES];
    src.read_sectors(2, 3, &mut buf, false).unwrap();
    let mut boxed: Box<dyn SectorSource> = Box::new(src);
    boxed.read_sectors_fua(7, 1, &mut buf, false, true).unwrap();
    assert!(
        boxed.read_sectors(9, 3, &mut buf, false).is_err(),
        "past the end"
    );
    assert_eq!(log.reads(), vec![(2, 3), (7, 1), (9, 3)]);
    assert_eq!(log.count(), 3);
    assert!(log.touched(4, 5) && !log.touched(5, 7) && log.touched(0, 3));
    log.clear();
    assert_eq!(log.count(), 0);
}

#[test]
fn decrypt_unit_inverts_encrypt_unit() {
    let mut u = content_unit(42, true);
    let plain = u.clone();
    assert!(encrypt_unit(&mut u, &K1));
    assert_ne!(u[16..], plain[16..]);
    decrypt_unit(&mut u, &K1);
    assert_eq!(u, plain);
}

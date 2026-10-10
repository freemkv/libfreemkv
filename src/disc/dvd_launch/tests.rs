//! Binary navigation fixtures, through the scanner hook, without synthetic evidence.
use super::*;
use crate::disc::{Disc, DvdLaunchEvidence, DvdLaunchReviewReason, EpisodeEvidence};
use std::collections::HashMap;

#[path = "terminal_tests.rs"]
mod terminal_tests;

fn word(b: &mut [u8], at: usize, n: u16) {
    b[at..at + 2].copy_from_slice(&n.to_be_bytes());
}
fn long(b: &mut [u8], at: usize, n: u32) {
    b[at..at + 4].copy_from_slice(&n.to_be_bytes());
}
fn pgc(cell: Option<(u32, u32)>, pre: &[[u8; 8]], post: &[[u8; 8]]) -> Vec<u8> {
    let mut b = vec![0; 272];
    if let Some((first, last)) = cell {
        b[2] = 1;
        b[3] = 1;
        b[7] = 0x40;
        b[236] = 1;
        word(&mut b, 0xe6, 236);
        word(&mut b, 0xe8, 240);
        word(&mut b, 0xea, 264);
        long(&mut b, 248, first);
        long(&mut b, 260, last);
        b[247] = 0x40;
        word(&mut b, 264, 1);
        b[267] = 1;
    }
    word(&mut b, 12, 0x8000);
    word(&mut b, 14, 0x8100);
    word(&mut b, 0xe4, 272);
    b.resize(280 + 8 * (pre.len() + post.len()), 0);
    word(&mut b, 272, pre.len() as u16);
    word(&mut b, 274, post.len() as u16);
    word(&mut b, 278, (7 + 8 * (pre.len() + post.len())) as u16);
    for (i, c) in pre.iter().chain(post).enumerate() {
        b[280 + i * 8..288 + i * 8].copy_from_slice(c);
    }
    b
}
fn table(entries: &[(u8, Vec<u8>)], menu: bool) -> Vec<u8> {
    let mut t = vec![0; 8 + entries.len() * 8];
    word(&mut t, 0, entries.len() as u16);
    for (i, (entry, b)) in entries.iter().enumerate() {
        t[8 + i * 8] = *entry;
        let at = t.len() as u32;
        long(&mut t, 12 + i * 8, at);
        t.extend(b);
    }
    let len = t.len() as u32;
    long(&mut t, 4, len - 1);
    if !menu {
        return t;
    }
    let mut lu = vec![0; 16];
    word(&mut lu, 0, 1);
    lu[8..10].copy_from_slice(b"en");
    lu[11] = 0x80;
    long(&mut lu, 12, 16);
    lu.extend(t);
    let len = lu.len() as u32;
    long(&mut lu, 4, len - 1);
    lu
}
fn insert(b: &mut Vec<u8>, pointer: usize, sector: usize, t: &[u8]) {
    long(b, pointer, sector as u32);
    b.resize(b.len().max(sector * 2048 + t.len()), 0);
    b[sector * 2048..sector * 2048 + t.len()].copy_from_slice(t);
}
fn fixture() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut vmg = vec![0; 4096];
    vmg[..12].copy_from_slice(b"DVDVIDEO-VMG");
    long(&mut vmg, 0xc4, 1);
    word(&mut vmg, 2048, 2);
    long(&mut vmg, 2052, 31);
    for i in 0..2 {
        let at = 2056 + i * 12;
        vmg[at + 1] = 1;
        word(&mut vmg, at + 2, 1);
        vmg[at + 6] = 1;
        vmg[at + 7] = (i + 1) as u8;
    }
    insert(
        &mut vmg,
        0xc8,
        2,
        &table(
            &[(0x82, pgc(None, &[[0x30, 6, 0, 1, 1, 0x83, 0, 0]], &[]))],
            true,
        ),
    );
    let mut vts = vec![0; 4096];
    vts[..12].copy_from_slice(b"DVDVIDEO-VTS");
    word(&mut vts, 0x202, 2);
    for (slot, language) in [b"de", b"en"].iter().enumerate() {
        vts[0x204 + slot * 8] = 4;
        vts[0x206 + slot * 8..0x208 + slot * 8].copy_from_slice(*language);
    }
    long(&mut vts, 0xc4, 100);
    long(&mut vts, 0xc8, 1);
    word(&mut vts, 2048, 2);
    long(&mut vts, 2052, 23);
    long(&mut vts, 2056, 16);
    long(&mut vts, 2060, 20);
    word(&mut vts, 2064, 1);
    word(&mut vts, 2066, 1);
    word(&mut vts, 2068, 2);
    word(&mut vts, 2070, 1);
    let ret = [0x30, 8, 0, 0, 0, 0x42, 0, 0];
    let titles = table(
        &[
            (
                0,
                pgc(Some((10, 19)), &[[0x51, 0, 0, 0x80, 0, 0, 0, 0]], &[ret]),
            ),
            (
                0,
                pgc(Some((30, 39)), &[[0x51, 0, 0, 0x81, 0, 0, 0, 0]], &[ret]),
            ),
        ],
        false,
    );
    insert(&mut vts, 0xcc, 2, &titles);
    let menus = table(
        &[
            (0x83, pgc(Some((0, 0)), &[], &[])),
            (
                0,
                pgc(
                    None,
                    &[
                        [0, 0xa1, 0, 0, 0, 3, 0, 3],
                        [0x30, 5, 0, 1, 0, 1, 0, 0],
                        [0x30, 5, 0, 1, 0, 2, 0, 0],
                    ],
                    &[],
                ),
            ),
        ],
        true,
    );
    insert(&mut vts, 0xd0, 3, &menus);
    let mut vob = vec![0; 2048];
    vob[..4].copy_from_slice(&[0, 0, 1, 0xba]);
    vob[4] = 0x44;
    vob[14..18].copy_from_slice(&[0, 0, 1, 0xbb]);
    word(&mut vob, 18, 18);
    vob[38..42].copy_from_slice(&[0, 0, 1, 0xbf]);
    word(&mut vob, 42, 0x3d4);
    vob[1024..1028].copy_from_slice(&[0, 0, 1, 0xbf]);
    word(&mut vob, 1028, 1018);
    vob[1030] = 1;
    let h = 141;
    word(&mut vob, h, 1);
    long(&mut vob, 61, 90000);
    long(&mut vob, h + 6, 90000);
    long(&mut vob, h + 10, 90000);
    vob[h + 14] = 0x21;
    vob[h + 15] = 0x40;
    vob[h + 17] = 2;
    for g in 0..2 {
        for i in 0..2 {
            let at = h + 46 + (g * 18 + i) * 18;
            vob[at + 2] = 100;
            vob[at + 5] = 30;
            vob[at + 6..at + 10].fill((2 - i) as u8);
            vob[at + 10..at + 18].copy_from_slice(&[
                0x71,
                4,
                0,
                0,
                0,
                if i == 0 { 1 } else { 3 },
                0,
                2,
            ]);
        }
    }
    (vmg, vts, vob)
}

#[derive(Default)]
struct Memory {
    sectors: HashMap<u32, [u8; 2048]>,
    halt_at: Option<u32>,
    short_at: Option<u32>,
}
impl Memory {
    fn put(&mut self, lba: u32, b: &[u8]) {
        for (i, c) in b.chunks(2048).enumerate() {
            let mut s = [0; 2048];
            s[..c.len()].copy_from_slice(c);
            self.sectors.insert(lba + i as u32, s);
        }
    }
}
impl SectorSource for Memory {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        b: &mut [u8],
        _: bool,
    ) -> crate::Result<usize> {
        if self
            .halt_at
            .is_some_and(|n| (lba..lba + u32::from(count)).contains(&n))
        {
            return Err(crate::Error::Halted);
        }
        for i in 0..u32::from(count) {
            b[i as usize * 2048..(i as usize + 1) * 2048]
                .copy_from_slice(self.sectors.get(&(lba + i)).unwrap_or(&[0; 2048]));
        }
        Ok(usize::from(count) * 2048
            - usize::from(
                self.short_at
                    .is_some_and(|n| (lba..lba + u32::from(count)).contains(&n)),
            ))
    }
}
fn icb(size: usize, lba: u32) -> Vec<u8> {
    let mut b = vec![0; 2048];
    b[..2].copy_from_slice(&266u16.to_le_bytes());
    b[56..64].copy_from_slice(&(size as u64).to_le_bytes());
    b[212..216].copy_from_slice(&8u32.to_le_bytes());
    b[216..220].copy_from_slice(&(size as u32).to_le_bytes());
    b[220..224].copy_from_slice(&lba.to_le_bytes());
    b
}
fn fid(name: &str, lba: u32, dir: bool) -> Vec<u8> {
    let mut b = vec![0; 38];
    b[..2].copy_from_slice(&257u16.to_le_bytes());
    b[18] = if dir { 2 } else { 0 };
    b[19] = name.len() as u8 + 1;
    b[24..28].copy_from_slice(&lba.to_le_bytes());
    b.push(8);
    b.extend_from_slice(name.as_bytes());
    b.resize((b.len() + 3) & !3, 0);
    b
}
fn image(vmg: &[u8], vts: &[u8], vob: &[u8]) -> (Memory, UdfFs) {
    let mut m = Memory::default();
    let mut entries = Vec::new();
    for (i, (name, b)) in [
        ("VIDEO_TS.IFO", vmg),
        ("VTS_01_0.IFO", vts),
        ("VTS_01_0.VOB", vob),
    ]
    .into_iter()
    .enumerate()
    {
        let node = 60 + i as u32;
        let lba = 5000 + i as u32 * 1000;
        entries.extend(fid(name, node, false));
        m.put(3000 + node, &icb(b.len(), lba));
        m.put(3000 + lba, b);
    }
    m.put(3050, &icb(entries.len(), 51));
    m.put(3051, &entries);
    let root = fid("VIDEO_TS", 50, true);
    m.put(3010, &icb(root.len(), 11));
    m.put(3011, &root);
    let mut b = vec![0; 2048];
    b[..2].copy_from_slice(&2u16.to_le_bytes());
    m.put(256, &b);
    b.fill(0);
    b[..2].copy_from_slice(&5u16.to_le_bytes());
    b[188..192].copy_from_slice(&3000u32.to_le_bytes());
    m.put(32, &b);
    b.fill(0);
    b[..2].copy_from_slice(&6u16.to_le_bytes());
    b[268..272].copy_from_slice(&1u32.to_le_bytes());
    m.put(33, &b);
    b.fill(0);
    b[..2].copy_from_slice(&8u16.to_le_bytes());
    m.put(34, &b);
    b.fill(0);
    b[..2].copy_from_slice(&256u16.to_le_bytes());
    b[404..408].copy_from_slice(&10u32.to_le_bytes());
    m.put(3000, &b);
    let fs = crate::udf::read_filesystem(&mut m).unwrap();
    (m, fs)
}
fn scan(vmg: &[u8], vts: &[u8], vob: &[u8]) -> Vec<DiscTitle> {
    let (mut m, fs) = image(vmg, vts, vob);
    Disc::scan_dvd_titles(&mut m, &fs, None).unwrap().0
}

#[test]
fn binary_root_dispatch_emits_full_title_launches_not_episode_identity() {
    let (vmg, vts, vob) = fixture();
    let titles = scan(&vmg, &vts, &vob);
    assert_eq!(titles.len(), 2);
    for (i, t) in titles.iter().enumerate() {
        let DvdLaunchEvidence::VerifiedRoot {
            vts,
            pgcn,
            title_count,
            routes,
        } = &t.selection_evidence.dvd_launch
        else {
            panic!("{:?}", t.selection_evidence)
        };
        assert_eq!((*vts, *pgcn, *title_count), (1, 1, 2));
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].target_title, i as u8 + 1);
        assert_eq!(routes[0].audio_stream, i as u8);
        assert_eq!(routes[0].audio_pid, 0xbd80 + i as u16);
        assert_eq!(routes[0].audio_language, ["deu", "eng"][i]);
        assert_eq!(routes[0].display_masks, [1, 4]);
        assert!(!routes[0].traces[0].is_empty());
        assert_eq!(t.selection_evidence.episodes, EpisodeEvidence::Unknown);
    }
    assert_ne!(titles[0].extents, titles[1].extents);
}

#[test]
fn relocated_pci_and_malformed_dsi_never_publish_launch_proof() {
    let (vmg, vts, original) = fixture();
    for (offset, replacement) in [(45, vec![0, 0, 0, 1]), (1028, vec![3, 0xf9])] {
        let mut vob = original.clone();
        vob[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert!(
            scan(&vmg, &vts, &vob).iter().all(|t| {
                t.selection_evidence.dvd_launch
                    == DvdLaunchEvidence::Review(DvdLaunchReviewReason::IncompleteNavigation)
            }),
            "mutation at {offset} accepted"
        );
    }
}

#[test]
fn selected_logical_slot_maps_through_pgc_not_filtered_stream_position() {
    let (vmg, mut vts, vob) = fixture();
    let second = 4096 + super::super::u32_at(&vts, 4116).unwrap();
    word(&mut vts, second + 12, 0);
    word(&mut vts, second + 14, 0x8300);
    let titles = scan(&vmg, &vts, &vob);
    let second = titles.iter().find(|t| t.playlist_id == 2).unwrap();
    assert_eq!(second.audio_streams().count(), 1);
    let DvdLaunchEvidence::VerifiedRoot { routes, .. } = &second.selection_evidence.dvd_launch
    else {
        panic!("{:?}", second.selection_evidence);
    };
    assert_eq!(routes[0].audio_stream, 1);
    assert_eq!(routes[0].audio_pid, 0xbd83);
    assert_eq!(routes[0].audio_language, "eng");
}

#[test]
fn conflicting_audio_alias_and_undeclared_selected_slot_require_review() {
    let (vmg, original, vob) = fixture();
    for undeclared in [false, true] {
        let mut vts = original.clone();
        let second = 4096 + super::super::u32_at(&vts, 4116).unwrap();
        if undeclared {
            word(&mut vts, 0x202, 1);
        } else {
            word(&mut vts, second + 14, 0x8000);
        }
        assert!(scan(&vmg, &vts, &vob).iter().all(|t| matches!(
            t.selection_evidence.dvd_launch,
            DvdLaunchEvidence::Review(_)
        )));
    }
}

#[test]
fn unspecified_audio_language_type_cannot_supply_presentation_language_proof() {
    let (vmg, mut vts, vob) = fixture();
    vts[0x204] &= !0x0c;
    assert!(scan(&vmg, &vts, &vob).iter().all(|title| matches!(
        title.selection_evidence.dvd_launch,
        DvdLaunchEvidence::Review(_)
    )));
}

#[test]
fn different_display_group_command_requires_review_transactionally() {
    let (vmg, vts, mut vob) = fixture();
    vob[141 + 46 + 18 * 18 + 10 + 5] = 3;
    assert!(
        scan(&vmg, &vts, &vob)
            .iter()
            .all(|t| t.selection_evidence.dvd_launch
                == DvdLaunchEvidence::Review(DvdLaunchReviewReason::AmbiguousNavigation))
    );
}

#[test]
fn interleaved_target_is_not_proved_by_scanner_fallback() {
    let (vmg, mut vts, vob) = fixture();
    let pgc = 4096 + super::super::u32_at(&vts, 4108).unwrap();
    vts[pgc + 240] = 4;
    assert!(
        scan(&vmg, &vts, &vob)
            .iter()
            .all(|t| t.selection_evidence.dvd_launch
                == DvdLaunchEvidence::Review(DvdLaunchReviewReason::UnprovenPresentation))
    );
}

#[test]
fn cancellation_in_launch_vob_read_propagates() {
    let (vmg, vts, vob) = fixture();
    let (mut m, fs) = image(&vmg, &vts, &vob);
    m.halt_at = Some(10000);
    assert!(matches!(
        Disc::scan_dvd_titles(&mut m, &fs, None),
        Err(crate::Error::Halted)
    ));
}

#[test]
fn short_launch_read_is_review_not_partial_evidence() {
    let (vmg, vts, vob) = fixture();
    let (mut m, fs) = image(&vmg, &vts, &vob);
    m.short_at = Some(10000);
    let titles = Disc::scan_dvd_titles(&mut m, &fs, None).unwrap().0;
    assert!(titles.iter().all(|t| t.selection_evidence.dvd_launch
        == DvdLaunchEvidence::Review(DvdLaunchReviewReason::IncompleteNavigation)));
}

#[test]
fn binary_complete_interleaved_target_has_launch_proof() {
    let (vmg, mut vts, vob) = fixture();
    let pgc = 4096 + super::super::u32_at(&vts, 4108).unwrap();
    vts[pgc + 240] = 4;
    let (mut m, fs) = image(&vmg, &vts, &vob);
    let mut nav = vec![0; 2048];
    nav[..4].copy_from_slice(&[0, 0, 1, 0xba]);
    nav[4] = 0x44;
    nav[0x400..0x407].copy_from_slice(&[0, 0, 1, 0xbf, 3, 0xfa, 1]);
    long(&mut nav, 0x407 + 4, 10);
    word(&mut nav, 0x407 + 24, 1);
    nav[0x407 + 27] = 1;
    long(&mut nav, 0x407 + 34, 9);
    long(&mut nav, 0x407 + 38, 0x7fff_ffff);
    m.put(9110, &nav);
    let titles = Disc::scan_dvd_titles(&mut m, &fs, None).unwrap().0;
    assert!(titles.iter().all(|t| matches!(
        t.selection_evidence.dvd_launch,
        DvdLaunchEvidence::VerifiedRoot { .. }
    )));
}

#[test]
fn binary_terminal_vobu_proof_gates_oversized_ilvu_launch() {
    let (vmg, mut vts, vob) = fixture();
    let pgc = 4096 + super::super::u32_at(&vts, 4108).unwrap();
    vts[pgc + 240] = 4;
    for valid in [false, true] {
        let (mut m, fs) = image(&vmg, &vts, &vob);
        m.put(9110, &terminal_tests::nav(10, 4, 0x8000_0005));
        m.put(
            9115,
            &terminal_tests::nav(15, 4, if valid { 0x3fff_ffff } else { 0x8000_0005 }),
        );
        let titles = Disc::scan_dvd_titles(&mut m, &fs, None).unwrap().0;
        assert!(titles.iter().all(|t| matches!(
            t.selection_evidence.dvd_launch,
            DvdLaunchEvidence::VerifiedRoot { .. }
        ) == valid));
    }
}

#[test]
fn equal_highlight_needs_prior_evidence_and_identical_commands() {
    let (_, _, mut pack) = fixture();
    let initial = pci::parse(&pack, 0, None).unwrap().unwrap();
    word(&mut pack, 141, 2);
    assert!(pci::parse(&pack, 0, None).is_err());
    assert_eq!(
        pci::parse(&pack, 0, Some(&initial)).unwrap(),
        Some(initial.clone())
    );
    for g in 0..2 {
        pack[141 + 46 + g * 18 * 18 + 15] = 3;
    }
    assert!(pci::parse(&pack, 0, Some(&initial)).is_err());
    word(&mut pack, 141, 3);
    assert!(pci::parse(&pack, 0, Some(&initial)).is_err());
}

#[test]
fn strict_interleave_rejects_early_end_clipping_wrong_identity_and_overlap() {
    fn nav(at: u32, end: u32, next: u32) -> Vec<u8> {
        let mut b = vec![0; 2048];
        b[..4].copy_from_slice(&[0, 0, 1, 0xba]);
        b[4] = 0x44;
        b[0x400..0x407].copy_from_slice(&[0, 0, 1, 0xbf, 3, 0xfa, 1]);
        long(&mut b, 0x407 + 4, at);
        word(&mut b, 0x407 + 24, 1);
        b[0x407 + 27] = 1;
        long(&mut b, 0x407 + 34, end);
        long(&mut b, 0x407 + 38, next);
        b
    }
    let mut m = Memory::default();
    m.put(110, &nav(10, 4, 10));
    m.put(120, &nav(20, 9, 0x7fff_ffff));
    let good = super::interleave::walk(&mut m, 100, 10, 29, &[0, 1, 0, 1], &mut 8)
        .unwrap_or_else(|_| panic!("complete forward walk rejected"));
    assert_eq!(
        good,
        vec![
            crate::disc::Extent {
                start_lba: 110,
                sector_count: 5
            },
            crate::disc::Extent {
                start_lba: 120,
                sector_count: 10
            }
        ]
    );
    for b in [
        nav(10, 4, 0),
        nav(10, 30, 10),
        nav(11, 4, 10),
        nav(10, 4, 4),
    ] {
        m.put(110, &b);
        assert!(super::interleave::walk(&mut m, 100, 10, 29, &[0, 1, 0, 1], &mut 8).is_err());
    }
    m.put(110, &nav(10, 4, 10));
    assert!(super::interleave::walk(&mut m, 100, 10, 29, &[0, 2, 0, 1], &mut 8).is_err());
    assert!(super::interleave::walk(&mut m, 100, 10, 29, &[0, 1, 0, 1], &mut 1).is_err());
    m.halt_at = Some(120);
    assert!(matches!(
        super::interleave::walk(&mut m, 100, 10, 29, &[0, 1, 0, 1], &mut 8),
        Err(produce::Failure::Io(crate::Error::Halted))
    ));
}

#[test]
fn unresolved_register_and_partial_title_targets_never_publish_roots() {
    let (vmg, vts, vob) = fixture();
    for replacement in [
        [0x20, 4, 0, 0, 0, 0, 0, 2],
        [0x30, 5, 0, 2, 0, 1, 0, 0],
        [0x78, 4, 0, 0, 0, 1, 0, 2],
    ] {
        let mut pack = vob.clone();
        for g in 0..2 {
            let at = 141 + 46 + g * 18 * 18 + 10;
            pack[at..at + 8].copy_from_slice(&replacement);
        }
        assert!(scan(&vmg, &vts, &pack).iter().all(|t| matches!(
            t.selection_evidence.dvd_launch,
            DvdLaunchEvidence::Review(_)
        )));
    }
}

#[test]
#[ignore = "read-only validation of a user-provided DVD image; set FREEMKV_DVD_LAUNCH_ISO"]
fn real_dvd_root_launch_validation() {
    use std::io::{Read, Seek, SeekFrom};
    struct Plain {
        file: std::fs::File,
        bytes: usize,
        last_lba: u32,
    }
    impl SectorSource for Plain {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            b: &mut [u8],
            _: bool,
        ) -> crate::Result<usize> {
            let n = usize::from(count) * 2048;
            self.last_lba = lba;
            self.bytes += n;
            assert!(self.bytes < 64 * 1024 * 1024);
            self.file
                .seek(SeekFrom::Start(u64::from(lba) * 2048))
                .unwrap();
            self.file.read_exact(&mut b[..n]).unwrap();
            Ok(n)
        }
    }
    let path = std::env::var("FREEMKV_DVD_LAUNCH_ISO").expect("explicit image required");
    let file = std::fs::File::open(path).unwrap();
    let size = file.metadata().unwrap().len();
    assert_eq!(size, 6_847_942_656);
    let mut reader = Plain {
        file,
        bytes: 0,
        last_lba: 0,
    };
    let fs = crate::udf::read_filesystem(&mut reader).unwrap();
    let vmg = super::super::read_bounded(&mut reader, &fs, "VIDEO_TS.IFO").unwrap();
    let vts = super::super::read_bounded(&mut reader, &fs, "VTS_01_0.IFO").unwrap();
    let menus = ifo::programs(&vts, 0xd0, 1, true).unwrap();
    let vmg_menus = ifo::programs(&vmg, 0xc8, 0, true).unwrap();
    let input = trace::Outcome {
        flow: vm::Flow::Next,
        registers: vm::Registers::default(),
        steps: Vec::new(),
    };
    let mut roots = Vec::new();
    for (_, o) in produce::follow(&vmg_menus, ifo::entry(&vmg_menus, 2).unwrap(), input).unwrap() {
        assert_eq!(o.flow, vm::Flow::VtsMenu { vts: 1, menu: 3 });
        roots.extend(produce::follow(&menus, ifo::entry(&menus, 3).unwrap(), o).unwrap());
    }
    let vob = super::super::read_bounded(&mut reader, &fs, "VTS_01_0.VOB").unwrap();
    let buttons = pci::parse(&vob[120 * 2048..121 * 2048], 120, None)
        .unwrap()
        .unwrap();
    for sector in 120..=1252 {
        if let Some(found) = pci::parse(
            &vob[sector * 2048..(sector + 1) * 2048],
            sector,
            Some(&buttons),
        )
        .unwrap_or_else(|reason| panic!("root sector {sector}: {reason:?}"))
        {
            assert_eq!(found, buttons, "root sector {sector}");
        }
    }
    assert_eq!(buttons.masks, [1, 4]);
    let title_pgcs = ifo::programs(&vts, 0xcc, 1, false).unwrap();
    let title_base = fs
        .file_start_lba(&mut reader, "/VIDEO_TS/VTS_01_0.IFO")
        .unwrap()
        + super::super::u32_at(&vts, 0xc4).unwrap() as u32;
    for title in [1, 2] {
        let p = &title_pgcs[ifo::title_pgc(&vts, title, &title_pgcs).unwrap()];
        let at = super::super::u16_at(p.bytes, 0xe8).unwrap();
        let pos = super::super::u16_at(p.bytes, 0xea).unwrap();
        for i in 0..usize::from(p.bytes[3]) {
            let c = &p.bytes[at + i * 24..at + (i + 1) * 24];
            if c[0] & 4 == 0 {
                continue;
            }
            let first = super::super::u32_at(c, 8).unwrap() as u32;
            let last = super::super::u32_at(c, 20).unwrap() as u32;
            if let Err(reason) = super::interleave::walk(
                &mut reader,
                title_base,
                first,
                last,
                &p.bytes[pos + i * 4..pos + i * 4 + 4],
                &mut 4096,
            ) {
                let failed = reader.last_lba;
                let mut pack = [0; 2048];
                reader.read_sectors(failed, 1, &mut pack, false).unwrap();
                eprintln!(
                    "title={title} cell={} range={first}..={last} strict_ILVU={reason:?} last_nav={} DSI={:02x?}",
                    i + 1,
                    failed - title_base,
                    &pack[0x407..0x407 + 44]
                );
                // Inspect only NAV packs inside the rejected terminal interval;
                // this diagnostic does not authorize clipping or publish proof.
                let mut nav = failed - title_base;
                for _ in 0..32 {
                    let mut pack = [0; 2048];
                    reader
                        .read_sectors(title_base + nav, 1, &mut pack, false)
                        .unwrap();
                    assert_eq!(&pack[0x400..0x407], &[0, 0, 1, 0xbf, 3, 0xfa, 1]);
                    let d = &pack[0x407..];
                    let end = nav
                        .checked_add(super::super::u32_at(d, 8).unwrap() as u32)
                        .unwrap();
                    eprintln!(
                        "terminal VOBU title={title} nav={nav} end={end} cell_end={last} ids={:02x?} sri_next={:#x}",
                        &d[24..28],
                        super::super::u32_at(d, 314).unwrap()
                    );
                    if end >= last {
                        break;
                    }
                    nav = end + 1;
                }
                panic!(
                    "title {title} cell {} failed complete interleave proof",
                    i + 1
                );
            }
        }
        eprintln!("title={title}: every interleaved cell passed complete proof");
    }
    for (button, expected) in [(0, 1), (1, 2)] {
        for (_, state) in &roots {
            let mut registers = state.registers.clone();
            registers.sprm[8] = Some(((button + 1) as u16) << 10);
            let step = crate::disc::DvdLaunchStep {
                vts: 1,
                menu_vob: true,
                byte_offset: (120 * 2048 + 197 + button * 18) as u32,
                command: buttons.commands[button],
            };
            for o in trace::run(&[step], registers).unwrap() {
                let vm::Flow::Pgc(pgcn) = o.flow else {
                    panic!("{:?}", o.flow)
                };
                for (_, o) in produce::follow(&menus, usize::from(pgcn) - 1, o).unwrap() {
                    assert_eq!(
                        o.flow,
                        vm::Flow::VtsTitle {
                            title: expected,
                            part: 1
                        }
                    );
                    let pgc = &title_pgcs[ifo::title_pgc(&vts, expected, &title_pgcs).unwrap()];
                    for started in trace::run(&pgc.pre, o.registers).unwrap() {
                        assert_eq!(started.flow, vm::Flow::Next);
                        assert_eq!(started.registers.sprm[1], Some(u16::from(expected - 1)));
                    }
                }
            }
        }
        eprintln!(
            "KUNG_FU root button {} -> VTS 1 title {expected} part 1 (command trace, not presentation proof)",
            button + 1
        );
    }
    let disc = Disc::scan_image(
        &mut reader,
        (size / 2048) as u32,
        &crate::ScanOptions::default(),
    )
    .unwrap();
    for t in &disc.titles {
        let languages: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                crate::disc::Stream::Audio(a) => Some(a.language.as_str()),
                _ => None,
            })
            .collect();
        eprintln!(
            "title={} extents={} audio={languages:?}",
            t.playlist_id,
            t.extents.len()
        );
        let DvdLaunchEvidence::VerifiedRoot {
            vts,
            pgcn,
            title_count,
            routes,
        } = &t.selection_evidence.dvd_launch
        else {
            panic!(
                "title {} launch={:?}",
                t.playlist_id, t.selection_evidence.dvd_launch
            )
        };
        assert_eq!((*vts, *pgcn, *title_count), (1, 1, 9));
        if matches!(t.playlist_id, 1 | 2) {
            assert_eq!(routes.len(), 1);
            let route = &routes[0];
            assert_eq!(
                (
                    route.button,
                    route.target_title,
                    route.target_part,
                    route.audio_stream
                ),
                (
                    t.playlist_id as u8,
                    t.playlist_id as u8,
                    1,
                    t.playlist_id as u8 - 1
                )
            );
            assert_eq!(route.display_masks, [1, 4]);
            assert!(!route.traces.is_empty());
            eprintln!(
                "verified root route: button={} title={} audio_stream={} traces={}",
                route.button,
                route.target_title,
                route.audio_stream,
                route.traces.len()
            );
        } else {
            assert!(routes.is_empty());
        }
        assert_eq!(t.selection_evidence.episodes, EpisodeEvidence::Unknown);
    }
    eprintln!("read_bytes={}", reader.bytes);
}

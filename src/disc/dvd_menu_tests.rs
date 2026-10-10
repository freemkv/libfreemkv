//! Binary IFO/UDF/PCI fixtures exercise the production scanner, not injected evidence.
use super::*;

fn word(b: &mut [u8], at: usize, n: u16) {
    b[at..at + 2].copy_from_slice(&n.to_be_bytes());
}
fn long(b: &mut [u8], at: usize, n: u32) {
    b[at..at + 4].copy_from_slice(&n.to_be_bytes());
}

fn pgc(cells: &[(u32, u32)], seconds: u8) -> Vec<u8> {
    let mut b = vec![0; 240 + cells.len() * 28];
    b[2] = 1;
    b[3] = cells.len() as u8;
    b[6] = seconds;
    b[7] = 0x40;
    word(&mut b, 0xe6, 236);
    word(&mut b, 0xe8, 240);
    word(&mut b, 0xea, (240 + cells.len() * 24) as u16);
    b[236] = 1;
    for (i, &(first, last)) in cells.iter().enumerate() {
        write_cell(&mut b, 240 + i * 24, first, last);
        b[240 + i * 24 + 6] = seconds;
        b[240 + i * 24 + 7] = 0x40;
        word(&mut b, 240 + cells.len() * 24 + i * 4, 1);
        b[240 + cells.len() * 24 + i * 4 + 3] = (i + 1) as u8;
    }
    b
}

fn fixture() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut vmg = build_vmg(&(1..=8).map(|n| (1, 1, n)).collect::<Vec<_>>());
    long(&mut vmg, 2048 + 4, 8 + 8 * 12 - 1);
    for i in 0..8 {
        vmg[2048 + 8 + i * 12 + 1] = 1;
    }
    vmg.resize(3 * 2048, 0);
    long(&mut vmg, 0x84, 0x400);
    word(&mut vmg, 0x400 + 0xe4, 0x100);
    word(&mut vmg, 0x500, 1);
    word(&mut vmg, 0x506, 15);
    vmg[0x508..0x510].copy_from_slice(&[0x30, 0x06, 0, 0, 0, 0x42, 0, 0]);
    long(&mut vmg, 0xc8, 2);
    let menu = pgc(&[(0, 0)], 0x01);
    let lu = 4096;
    word(&mut vmg, lu, 1);
    long(&mut vmg, lu + 4, (16 + 16 + menu.len() - 1) as u32);
    vmg[lu + 8..lu + 10].copy_from_slice(b"en");
    vmg[lu + 11] = 0x80;
    long(&mut vmg, lu + 12, 16);
    word(&mut vmg, lu + 16, 1);
    long(&mut vmg, lu + 20, (16 + menu.len() - 1) as u32);
    vmg[lu + 24] = 0x82;
    long(&mut vmg, lu + 28, 16);
    vmg[lu + 32..lu + 32 + menu.len()].copy_from_slice(&menu);

    let programs = [
        vec![(100, 109)],
        vec![(100, 109)],
        vec![(200, 239)],
        vec![(200, 239)],
        vec![(300, 301)],
        vec![(300, 301)],
        vec![(200, 239), (100, 109), (300, 301)],
        vec![(900, 939)],
    ];
    let mut vts = vec![0; 5 * 2048];
    vts[..12].copy_from_slice(b"DVDVIDEO-VTS");
    long(&mut vts, 0xc4, 100);
    long(&mut vts, 0xc8, 1);
    long(&mut vts, 0xcc, 2);
    word(&mut vts, 2048, 8);
    long(&mut vts, 2052, 71);
    word(&mut vts, 4096, 8);
    let mut offset = 72;
    for (i, cells) in programs.iter().enumerate() {
        long(&mut vts, 2056 + i * 4, (40 + i * 4) as u32);
        word(&mut vts, 2088 + i * 4, (i + 1) as u16);
        word(&mut vts, 2090 + i * 4, 1);
        long(&mut vts, 4096 + 12 + i * 8, offset as u32);
        let body = pgc(cells, [0x10, 0x10, 0x40, 0x40, 0x02, 0x02, 0x52, 0x40][i]);
        vts[4096 + offset..4096 + offset + body.len()].copy_from_slice(&body);
        offset += body.len();
    }
    long(&mut vts, 4100, (offset - 1) as u32);

    let mut vob = vec![0; 2048];
    vob[..4].copy_from_slice(&[0, 0, 1, 0xba]);
    vob[4] = 0x44;
    vob[14..18].copy_from_slice(&[0, 0, 1, 0xbb]);
    word(&mut vob, 18, 18);
    vob[38..42].copy_from_slice(&[0, 0, 1, 0xbf]);
    word(&mut vob, 42, 0x3d4);
    let hli = 45 + 96;
    long(&mut vob, 45 + 16, 90000);
    word(&mut vob, hli, 1);
    long(&mut vob, hli + 6, 90000);
    long(&mut vob, hli + 10, 90000);
    vob[hli + 14] = 0x10;
    vob[hli + 17] = 7;
    for i in 0..7 {
        let at = hli + 22 + 24 + 18 * i + 10;
        vob[at - 8] = 100; // nonempty button rectangle
        vob[at - 5] = 30;
        vob[at - 4..at].fill(((i + 1) % 7 + 1) as u8); // connected authored arrow graph
        vob[at..at + 8].copy_from_slice(&[0x30, 2, 0, 0, 0, (i + 1) as u8, 0, 0]);
    }
    vob[1024..1028].copy_from_slice(&[0, 0, 1, 0xbf]);
    word(&mut vob, 1028, 1018);
    vob[1030] = 1;
    (vmg, vts, vob)
}

fn image(vmg: Vec<u8>, vts: Vec<u8>, vob: Vec<u8>) -> (MemDisc, crate::udf::UdfFs) {
    let mut disc = MemDisc::new();
    let fs = build_video_ts_fs(
        &mut disc,
        &[
            FileSpec {
                name: "VIDEO_TS.IFO".into(),
                icb_lba: 60,
                data_lba: 5000,
                contents: vmg,
            },
            FileSpec {
                name: "VTS_01_0.IFO".into(),
                icb_lba: 62,
                data_lba: 6000,
                contents: vts,
            },
            FileSpec {
                name: "VIDEO_TS.VOB".into(),
                icb_lba: 64,
                data_lba: 7000,
                contents: vob,
            },
        ],
    );
    (disc, fs)
}

fn scan(vmg: Vec<u8>, vts: Vec<u8>, vob: Vec<u8>) -> Vec<DiscTitle> {
    let (mut disc, fs) = image(vmg, vts, vob);
    Disc::scan_dvd_titles(&mut disc, &fs, None).unwrap().0
}

#[test]
fn dvd_menu_binary_partition_six_alternates_three_uneven_episodes() {
    let (vmg, vts, vob) = fixture();
    let titles = scan(vmg, vts, vob);
    assert_eq!(titles.len(), 8);
    for (i, title) in titles.iter().enumerate() {
        assert_eq!(title.selection_evidence.dvd_menu_reachable, i < 7);
        assert!(matches!(&title.selection_evidence.episodes,
            EpisodeEvidence::Authored {title_count:8,member,..} if *member == (i<6)));
        assert_eq!(
            match title.selection_evidence.episodes {
                EpisodeEvidence::Authored { ordinal, .. } => ordinal,
                _ => None,
            },
            [
                Some(1),
                Some(1),
                Some(0),
                Some(0),
                Some(2),
                Some(2),
                None,
                None
            ][i]
        );
    }
}

#[test]
fn dvd_menu_binary_reachability_without_exact_partition_is_not_roster() {
    let (vmg, mut vts, vob) = fixture();
    let at = 4096 + u32::from_be_bytes(vts[4156..4160].try_into().unwrap()) as usize;
    // Play-all's first authored interval no longer matches either standalone alias.
    long(&mut vts, at + 240 + 20, 238);
    let titles = scan(vmg, vts, vob);
    assert!(titles[0].selection_evidence.dvd_menu_reachable);
    assert!(
        titles
            .iter()
            .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
    );
}

#[test]
fn dvd_menu_binary_unsupported_navigation_and_partial_tables_hold_for_review() {
    for case in 0..10 {
        let (mut vmg, mut vts, mut vob) = fixture();
        match case {
            0 => vmg[0x508 + 1] = 0x26,             // conditional First-Play jump
            1 => vob[45 + 96 + 46 + 10 + 1] = 0x22, // conditional button jump
            2 => vob[45 + 96 + 46 + 10 + 1] = 5,    // chapter/program jump, not standalone title
            3 => word(&mut vts, 2090, 2),           // first PTT starts inside a PGC
            4 => long(&mut vts, 4100, 80),          // truncated PGCIT
            5 => word(&mut vmg, 4096, 2),           // unsupported language branches
            6 => vob.truncate(2047),
            7 => vob[45 + 96 + 46 + 6..45 + 96 + 46 + 10].fill(1), // orphaned button graph
            8 => vob[45 + 96 + 21] = 1, // forced activation bypasses normal menu choice
            9 => vmg[2048 + 9] = 2,     // multiple angles
            _ => unreachable!(),
        }
        let titles = scan(vmg, vts, vob);
        assert!(
            titles
                .iter()
                .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown),
            "case {case}"
        );
    }
}

#[test]
fn dvd_menu_binary_shared_intro_does_not_collapse_distinct_full_titles() {
    let (vmg, mut vts, vob) = fixture();
    // Extend every standalone title with the same intro and repeat that intro in
    // play-all at each episode boundary. Rebuild PGCIT, retaining all authored cuts.
    let programs = [
        vec![(50, 59), (100, 109)],
        vec![(50, 59), (100, 109)],
        vec![(50, 59), (200, 239)],
        vec![(50, 59), (200, 239)],
        vec![(50, 59), (300, 301)],
        vec![(50, 59), (300, 301)],
        vec![
            (50, 59),
            (200, 239),
            (50, 59),
            (100, 109),
            (50, 59),
            (300, 301),
        ],
        vec![(900, 939)],
    ];
    let mut offset = 72;
    for (i, program) in programs.iter().enumerate() {
        long(&mut vts, 4096 + 12 + i * 8, offset as u32);
        let body = pgc(program, 0x30);
        vts[4096 + offset..4096 + offset + body.len()].copy_from_slice(&body);
        offset += body.len();
    }
    long(&mut vts, 4100, (offset - 1) as u32);
    let titles = scan(vmg, vts, vob);
    assert_eq!(
        titles
            .iter()
            .map(|t| match t.selection_evidence.episodes {
                EpisodeEvidence::Authored { ordinal, .. } => ordinal,
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            Some(1),
            Some(1),
            Some(0),
            Some(0),
            Some(2),
            Some(2),
            None,
            None
        ]
    );
}

#[test]
fn dvd_menu_binary_read_cancellation_propagates_and_short_read_cannot_prove_roster() {
    struct Reader {
        disc: MemDisc,
        halt: crate::halt::Halt,
        cancel: bool,
    }
    impl SectorSource for Reader {
        fn read_sectors(&mut self, lba: u32, n: u16, b: &mut [u8], r: bool) -> Result<usize> {
            if lba == PART_START + 7000 {
                if self.cancel {
                    self.halt.cancel();
                }
                return Ok(0);
            }
            self.disc.read_sectors(lba, n, b, r)
        }
    }
    for cancel in [false, true] {
        let (vmg, vts, vob) = fixture();
        let (disc, fs) = image(vmg, vts, vob);
        let halt = crate::halt::Halt::new();
        let mut reader = Reader {
            disc,
            halt: halt.clone(),
            cancel,
        };
        let result = Disc::scan_dvd_titles(&mut reader, &fs, Some(&halt));
        if cancel {
            assert!(matches!(result, Err(Error::Halted)));
        } else {
            assert!(
                result
                    .unwrap()
                    .0
                    .iter()
                    .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
            );
        }
    }
}

#[test]
fn dvd_menu_binary_size_budget_refuses_without_reading_menu_payload() {
    let (vmg, vts, vob) = fixture();
    let (mut disc, _) = image(vmg, vts, vob);
    disc.put(PART_START + 64, build_file_icb(8 * 1024 * 1024 + 1, 7000));
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    struct NoMenuRead(MemDisc);
    impl SectorSource for NoMenuRead {
        fn read_sectors(&mut self, lba: u32, n: u16, b: &mut [u8], r: bool) -> Result<usize> {
            assert_ne!(lba, PART_START + 7000, "oversized menu must not be read");
            self.0.read_sectors(lba, n, b, r)
        }
    }
    let titles = Disc::scan_dvd_titles(&mut NoMenuRead(disc), &fs, None)
        .unwrap()
        .0;
    assert!(
        titles
            .iter()
            .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
    );
}

#[test]
fn dvd_menu_binary_movie_firstplay_is_not_reclassified_as_episodes() {
    let (mut vmg, vts, vob) = fixture();
    vmg[0x508..0x510].copy_from_slice(&[0x30, 2, 0, 0, 0, 7, 0, 0]);
    let (mut reader, fs) = image(vmg, vts, vob);
    let (titles, feature, _) = Disc::scan_dvd_titles(&mut reader, &fs, None).unwrap();
    assert_eq!(feature, Some(7));
    assert_eq!(titles.len(), 8);
    assert!(
        titles
            .iter()
            .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
    );
}

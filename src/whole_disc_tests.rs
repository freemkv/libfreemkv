use super::*;

// The key set's whole-disc pieces (`keys::whole_disc_pieces`) over `files`, with one
// title playing `title_extents`: each piece's unit spans.
fn pieces(files: &[Vec<(u32, u32)>], title_extents: &[(u32, u32)]) -> Vec<Vec<UnitSpan>> {
    let mut title = crate::DiscTitle::empty();
    title.extents = title_extents
        .iter()
        .map(|&(start_lba, sector_count)| crate::Extent {
            start_lba,
            sector_count,
        })
        .collect();
    let disc = crate::Disc {
        volume_id: String::new(),
        meta_title: None,
        format: crate::DiscFormat::BluRay,
        capacity_sectors: 1000,
        capacity_bytes: 1000 * 2048,
        layers: 1,
        titles: vec![title],
        region: crate::disc::DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: true,
        aacs_error: None,
        css_error: None,
        content_format: crate::ContentFormat::BdTs,
    };
    crate::keys::whole_disc_pieces(&disc, files)
}

// The unit spans the key set's whole-disc reader holds: every piece's, sorted.
fn unit_spans(files: &[Vec<(u32, u32)>], title_extents: &[(u32, u32)]) -> Vec<UnitSpan> {
    let mut spans: Vec<UnitSpan> = pieces(files, title_extents).into_iter().flatten().collect();
    spans.sort_unstable_by_key(|s| s.0);
    spans
}

#[test]
fn probes_start_at_the_first_unit_and_spread_to_the_end() {
    assert_eq!(probe_units(0), Vec::<u64>::new());
    assert_eq!(probe_units(3), vec![0, 1, 2]);
    assert_eq!(probe_units(PROBES), (0..PROBES).collect::<Vec<_>>());
    let long = probe_units(3200);
    assert_eq!(long.len(), PROBES as usize);
    assert_eq!((long[0], long[1], long[31]), (0, 100, 3100));
}

#[test]
fn subtract_leaves_only_the_unkeyed_pieces() {
    let got = subtract_ranges(&[(10, 30), (50, 10)], &[(0, 12), (20, 25), (38, 55)]);
    assert_eq!(got, vec![(12, 20), (25, 38), (55, 60)]);
    assert_eq!(subtract_ranges(&[(10, 5)], &[(10, 15)]), Vec::new());
    assert_eq!(subtract_ranges(&[(10, 5)], &[]), vec![(10, 15)]);
}

#[test]
fn merge_sorts_coalesces_overlap_and_adjacency_and_drops_empties() {
    let got = merge_ranges(vec![(50, 10), (10, 5), (12, 10), (22, 3), (40, 0), (30, 1)]);
    assert_eq!(got, vec![(10, 15), (30, 1), (50, 10)]);
    assert_eq!(merge_ranges(vec![(0, 100), (10, 5)]), vec![(0, 100)]);
    let got = merge_ranges(vec![(0, u32::MAX), (u32::MAX - 1, u32::MAX)]);
    assert_eq!(got, vec![(0, u32::MAX)]);
}

/// Each file anchors its own grid at its first sector; a later extent keeps the grid
/// by FILE OFFSET; title extents outside every file get their own.
#[test]
fn unit_spans_follow_each_file_by_offset_across_extents() {
    let files = vec![vec![(100, 4), (300, 5)], vec![(104, 7)]];
    let spans = unit_spans(&files, &[]);
    assert_eq!(spans, vec![(100, 4, 100), (104, 7, 104), (300, 5, 299)]);
    assert_eq!(unit_spans(&[], &[(7, 9)]), vec![(7, 9, 7)]);
    let spans = unit_spans(&[vec![(10, 7)], vec![(10, 7), (40, 3)]], &[]);
    assert_eq!(spans, vec![(10, 7, 10), (40, 3, 39)]);
    let spans = unit_spans(&[vec![(100, 6)]], &[(100, 6), (200, 4)]);
    assert_eq!(spans, vec![(100, 6, 100), (200, 4, 200)]);
}

/// Synthetic drive: each sector filled with its LBA's low byte; records reads,
/// unit bases and FUA flags.
#[derive(Default)]
struct Mem {
    reads: Vec<(u32, u16)>,
    bases: Vec<u32>,
    fua: Vec<bool>,
    short: Option<usize>,
    fail: Option<fn() -> Error>,
}
impl SectorSource for Mem {
    fn capacity_sectors(&self) -> u32 {
        u32::MAX
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        self.read_sectors_fua(lba, count, buf, r, false)
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _: bool,
        fua: bool,
    ) -> Result<usize> {
        self.reads.push((lba, count));
        self.fua.push(fua);
        if let Some(make) = self.fail {
            return Err(make());
        }
        let n = self.short.unwrap_or(usize::MAX).min(count as usize * 2048);
        for (i, b) in buf[..n].iter_mut().enumerate() {
            *b = (lba as usize + i / 2048) as u8;
        }
        Ok(n)
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.bases.push(lba);
    }
}

fn reader(spans: Vec<UnitSpan>) -> UnitAligned<Mem> {
    UnitAligned::new(Mem::default(), spans)
}

#[test]
fn unit_aligned_splits_a_max_length_read_on_the_grid() {
    let mut r = reader(vec![(0, 70_000, 0)]);
    let mut buf = vec![0u8; u16::MAX as usize * 2048];
    assert_eq!(
        r.read_sectors(1, u16::MAX, &mut buf, false).unwrap(),
        buf.len()
    );
    assert_eq!(r.inner.reads, vec![(0, 65_532), (65_532, 6)]);
    assert!(
        buf.chunks(2048)
            .enumerate()
            .all(|(i, c)| c[0] == (1 + i) as u8)
    );
}

#[test]
fn unit_aligned_reports_a_short_inner_transfer() {
    let mut r = reader(vec![(100, 30, 100)]);
    r.inner.short = Some(2048);
    let mut buf = vec![0u8; 3 * 2048];
    assert_eq!(r.read_sectors(100, 3, &mut buf, false).unwrap(), 2048);
    r.inner.short = Some(0);
    assert_eq!(r.read_sectors(98, 2, &mut buf, false).unwrap(), 0);
}

#[test]
fn unit_aligned_passes_fua_and_bases_plain_reads_at_their_start() {
    let mut r = reader(vec![(100, 30, 100)]);
    let mut buf = vec![0u8; 5 * 2048];
    r.read_sectors_fua(98, 5, &mut buf, true, true).unwrap();
    assert_eq!(r.inner.fua, vec![true, true]);
    assert_eq!(r.inner.bases, vec![98, 100]);
}

#[test]
fn unit_aligned_widens_reads_onto_each_files_grid() {
    let mut r = reader(vec![(100, 30, 100), (300, 9, 299)]);
    let mut buf = vec![0u8; 5 * 2048];
    assert_eq!(r.read_sectors(98, 5, &mut buf, false).unwrap(), 5 * 2048);
    let got: Vec<u8> = buf.chunks(2048).map(|c| c[0]).collect();
    assert_eq!(got, vec![98, 99, 100, 101, 102]);
    assert_eq!(r.inner.reads, vec![(98, 2), (100, 3)]);
    r.inner.reads.clear();
    let mut one = vec![0u8; 2048];
    r.read_sectors(305, 1, &mut one, false).unwrap();
    assert_eq!(one[0], (305u32 % 256) as u8);
    assert_eq!(r.inner.reads, vec![(305, 3)]);
}

#[test]
fn unit_aligned_propagates_inner_read_errors_on_both_paths() {
    let mut r = reader(vec![(100, 30, 100)]);
    r.inner.fail = Some(|| Error::DecryptFailed);
    let mut buf = vec![0u8; 3 * 2048];
    // Outside every span: plain read path.
    assert!(matches!(
        r.read_sectors(10, 3, &mut buf, false),
        Err(Error::DecryptFailed)
    ));
    // Inside a span: unit-widened path.
    assert!(matches!(
        r.read_sectors(101, 3, &mut buf, false),
        Err(Error::DecryptFailed)
    ));
    assert_eq!(r.inner.reads, vec![(10, 3), (100, 6)]);
}

#[test]
fn unit_aligned_refuses_a_unit_split_across_extents() {
    let mut r = reader(vec![(100, 4, 100), (300, 5, 299)]);
    let mut buf = vec![0u8; 2048];
    assert!(matches!(
        r.read_sectors(300, 1, &mut buf, false),
        Err(Error::DecryptFailed)
    ));
}

#[test]
fn unit_block_end_pulls_back_to_the_straddling_units_head() {
    let r = reader(vec![(100, 30, 100)]);
    assert_eq!(r.unit_block_end(90, 104), 103);
    assert_eq!(r.unit_block_end(90, 103), 103);
    assert_eq!(r.unit_block_end(103, 104), 104);
    assert_eq!(r.unit_block_end(0, 50), 50);
}

// Spec guards (keys-upfront-design §7.8) on the per-file unit grid.
mod spec_guards {
    use super::*;
    use crate::spec::keys::{
        KS_1_ENCRYPT_EVERY_UNIT, KS_7_UNIT_CONTIGUOUS, KS_8_EXTENTS_ASCENDING, KS_9_SSIF_ALIGNED,
        KS_28_FILE_GRID_EVIDENCE,
    };

    /// per spec; do not change without a spec citation — KS-1 [BD] §3.10.1: "encryption is
    /// applied to every Aligned Unit in the file" (per evidence KS-28: the grid is the file's).
    #[test]
    fn unit_grid_anchors_at_file_byte_0() {
        assert!(
            KS_1_ENCRYPT_EVERY_UNIT
                .text
                .ends_with("every Aligned Unit in the file.")
        );
        assert!(
            KS_28_FILE_GRID_EVIDENCE
                .text
                .contains("32/32 sync on the file grid")
        );
        let mut one = vec![0u8; 2048];
        for start in [100u32, 101] {
            assert_ne!(start % 3, 0);
            let spans = unit_spans(&[vec![(start, 30)]], &[]);
            assert_eq!(
                spans,
                vec![(start, 30, start as u64)],
                "anchored at the file"
            );
            let mut r = reader(spans);
            for lba in start..start + 30 {
                r.inner.reads.clear();
                r.read_sectors(lba, 1, &mut one, false).unwrap();
                let (head, n) = r.inner.reads[0];
                assert_eq!(
                    (head - start) % 3,
                    0,
                    "lba {lba}: head {head} on the file grid"
                );
                assert_ne!(head % 3, 0, "lba {lba}: never the disc-LBA grid");
                assert!(head <= lba && lba < head + n as u32);
            }
        }
    }

    /// per spec (**Informative**); do not change without a spec citation — KS-7 [BD] Annex A:
    /// "Each physical sector in an Aligned Unit shall be allocated contiguously" (KS-28).
    #[test]
    fn unit_never_straddles_an_extent_boundary() {
        assert_eq!(
            KS_7_UNIT_CONTIGUOUS.kind,
            crate::spec::QuoteKind::Informative
        );
        assert!(KS_7_UNIT_CONTIGUOUS.text.contains("allocated contiguously"));
        // A 4-sector first extent: unit 1 would span sectors 103 and 300..302.
        let spans = unit_spans(&[vec![(100, 4), (300, 8)]], &[]);
        let mut r = reader(spans);
        let mut buf = vec![0u8; 3 * 2048];
        assert_eq!(r.read_sectors(100, 3, &mut buf, false).unwrap(), 3 * 2048);
        for lba in [300, 301] {
            let got = r.read_sectors(lba, 1, &mut buf[..2048], false);
            assert!(
                matches!(got, Err(Error::DecryptFailed)),
                "lba {lba}: {got:?}"
            );
        }
        assert!(
            r.read_sectors(302, 3, &mut buf, false).is_ok(),
            "the next unit is whole"
        );
    }

    /// per spec (**Informative**); do not change without a spec citation — KS-8 [BD] Annex A:
    /// "All the extents of each Clip AV stream file shall be allocated with ascending order".
    #[test]
    fn extents_ascending_per_file_grid_carries_across() {
        assert_eq!(
            KS_8_EXTENTS_ASCENDING.kind,
            crate::spec::QuoteKind::Informative
        );
        assert!(
            KS_8_EXTENTS_ASCENDING
                .text
                .contains("ascending order in physical layer")
        );
        // The grid carries by file offset: 6 sectors → 200 is a head; 7 → 199 anchors.
        let spans = unit_spans(&[vec![(100, 6), (200, 9)]], &[]);
        assert_eq!(spans, vec![(100, 6, 100), (200, 9, 200)]);
        let spans = unit_spans(&[vec![(100, 7), (200, 8)]], &[]);
        assert_eq!(spans, vec![(100, 7, 100), (200, 8, 199)]);
        let mut r = reader(spans);
        let mut one = vec![0u8; 2048];
        r.read_sectors(205, 1, &mut one, false).unwrap();
        assert_eq!(
            r.inner.reads,
            vec![(205, 3)],
            "heads at 202, 205: file offset 9, 12"
        );
    }

    /// per spec; do not change without a spec citation — KS-9 [BD] §8.1.2: "The boundary of
    /// these segments shall be always aligned to Aligned Unit boundary" (SSIF keeps the grid).
    #[test]
    fn ssif_segments_keep_the_unit_grid() {
        assert!(
            KS_9_SSIF_ALIGNED
                .text
                .contains("aligned to Aligned Unit boundary")
        );
        let m2ts = vec![vec![(300, 30)], vec![(600, 30)]];
        // The SSIF re-lists the second clip from sector 601: its own grid would be 601+3k.
        let mut with_ssif = m2ts.clone();
        with_ssif.push(vec![(601, 29)]);
        assert_eq!(
            unit_spans(&with_ssif, &[]),
            unit_spans(&m2ts, &[]),
            "not re-gridded"
        );
        assert_eq!(
            pieces(&with_ssif, &[]),
            pieces(&m2ts, &[]),
            "no piece of its own: not re-keyed, not re-probed"
        );
    }
}

// UnitAligned wraps the decrypting reader for whole-disc/image copies; it must
// relay the inner source's unmapped list (an image built on it would else drop it).
#[test]
fn unit_aligned_forwards_unmapped_stream_files() {
    use crate::sector::bus_removal::test_support::{Reports, assert_forwards, m2ts1};
    assert_forwards(UnitAligned::new(Reports(vec![m2ts1()]), Vec::new()));
}

/// The raw (`--raw`) whole-disc reader never decrypts: AACS ciphertext and a CSS-scrambled
/// pack pass through byte for byte, as the pre-KU-X2 reader did with `decrypt: false`.
#[test]
fn raw_whole_disc_reader_passes_every_sector_through() {
    use crate::test_util::{BdFile, MemSource, encrypted_bd_image, unit_key_ro};
    let uk_ro = unit_key_ro(crate::aacs::mkb::AacsVersion::V10, &[[0xEE; 16]], &[1]);
    let files = [BdFile::new("BDMV/STREAM/00001.m2ts", 30, Some([0x11; 16]))];
    let mut image = encrypted_bd_image(&files, &uk_ro).image;
    // A CSS-scrambled MPEG-2 pack (bits 4-5 of byte 0x14 set) in sector 1.
    image[2048..2052].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    image[2048 + 4] = 0x44;
    image[2048 + 0x14] |= 0x30;
    let sectors = (image.len() / 2048) as u32;
    let mut r = raw_whole_disc_reader(MemSource::new(image.clone()));
    let mut buf = vec![0u8; image.len()];
    let mut lba = 0;
    while lba < sectors {
        let n = (sectors - lba).min(30);
        let at = lba as usize * 2048;
        let got = r.read_sectors(lba, n as u16, &mut buf[at..at + n as usize * 2048], false);
        assert_eq!(got.unwrap(), n as usize * 2048);
        lba += n;
    }
    assert!(buf == image, "a raw read returns the image unchanged");
}

#[test]
fn push_extent_does_not_overflow_count() {
    let mut e = vec![(0u32, u32::MAX - 1)];
    push_extent(&mut e, u32::MAX - 1, 5);
    assert_eq!(e, vec![(0, u32::MAX - 1), (u32::MAX - 1, 5)]);
    let mut e = vec![(10u32, 4)];
    push_extent(&mut e, 14, 6);
    push_extent(&mut e, 20, 0);
    assert_eq!(e, vec![(10, 10)]);
}

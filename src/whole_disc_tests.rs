use super::*;
use crate::aacs::content::encrypt_unit;

const SECTOR: usize = 2048;
const K0: [u8; 16] = [0x11; 16];
const K1: [u8; 16] = [0x22; 16];
const K2: [u8; 16] = [0x33; 16];
const STRANGER: [u8; 16] = [0x77; 16];

// A clear content unit for the unit starting at `lba`: varied payload, TS sync at
// offset 4 of every 192-byte source packet, CPI bits clear.
fn clear_unit(lba: u32) -> Vec<u8> {
    let mut u: Vec<u8> = (0..6144u32)
        .map(|i| (lba.wrapping_mul(7919).wrapping_add(i) % 251) as u8 | 1)
        .collect();
    for off in (4..6144).step_by(192) {
        u[off] = 0x47;
    }
    u[0] &= 0x3F;
    u
}

/// An in-memory image: stream files laid out on their own unit grid, the listed
/// units encrypted under the file's key; sectors outside every file are
/// CPI-flagged-looking clear data (the freemkv#55 shape).
struct Img {
    data: Vec<u8>,
    plain: Vec<u8>,
    /// One-unit reads starting in `[start, end)` fail (probes of a damaged area).
    probe_fail: Option<(u32, u32)>,
    reads: Vec<(u32, u16)>,
}

// One file: start, sectors, key, and which of its units are encrypted.
type FileSpec = (u32, u32, [u8; 16], &'static [u32]);

// Every unit of a 30-sector file.
const ALL: &[u32] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

fn img(capacity: u32, files: &[FileSpec]) -> Img {
    let mut data = vec![0u8; capacity as usize * SECTOR];
    for lba in 0..capacity {
        let o = lba as usize * SECTOR;
        for (i, b) in data[o..o + SECTOR].iter_mut().enumerate() {
            *b = (lba as usize + i) as u8;
        }
        data[o] = 0xC0;
    }
    let mut plain = data.clone();
    for (start, n, key, enc) in files {
        for u in 0..n / 3 {
            let lba = start + u * 3;
            let o = lba as usize * SECTOR;
            let mut unit = clear_unit(lba);
            if enc.contains(&u) {
                unit[0] |= 0xC0;
                plain[o..o + 6144].copy_from_slice(&unit);
                assert!(encrypt_unit(&mut unit, key));
            } else {
                plain[o..o + 6144].copy_from_slice(&unit);
            }
            data[o..o + 6144].copy_from_slice(&unit);
        }
    }
    Img {
        data,
        plain,
        probe_fail: None,
        reads: Vec::new(),
    }
}

impl SectorSource for Img {
    fn capacity_sectors(&self) -> u32 {
        (self.data.len() / SECTOR) as u32
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        self.reads.push((lba, count));
        if count == 3 && self.probe_fail.is_some_and(|(s, e)| (s..e).contains(&lba)) {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        let (o, n) = (lba as usize * SECTOR, count as usize * SECTOR);
        buf[..n].copy_from_slice(&self.data[o..o + n]);
        Ok(n)
    }
}

fn pool(keys: &[[u8; 16]]) -> DecryptKeys {
    DecryptKeys::Aacs {
        unit_keys: keys
            .iter()
            .enumerate()
            .map(|(i, k)| (i as u32, *k))
            .collect(),
        format: crate::ContentFormat::BdTs,
    }
}

fn rule(single: Option<usize>) -> KeyRule<'static> {
    KeyRule {
        format: crate::ContentFormat::BdTs,
        single,
        fetch: None,
        halt: None,
    }
}

/// Key `files` the way [`whole_disc_reader`] does; the kept title plays file 0 with slot 0.
fn plan(
    src: &mut Img,
    keys: &mut DecryptKeys,
    files: &[Vec<(u32, u32)>],
    rule: &KeyRule,
) -> Result<ContentKeys> {
    let (s, n) = files[0][0];
    let title = AacsKeyMap::from_ranges(vec![(s, s + n, 0)]);
    let spans = unit_spans(files, &[]);
    key_content_files(src, rule, keys, title, files, &spans)
}

fn one_file_each(specs: &[FileSpec]) -> Vec<Vec<(u32, u32)>> {
    specs.iter().map(|f| vec![(f.0, f.1)]).collect()
}

/// Wire the reader over `src` from a plan and copy the whole image with `write_image`.
fn write_through(
    src: Img,
    keys: DecryptKeys,
    files: &[Vec<(u32, u32)>],
    planned: ContentKeys,
) -> Result<Vec<u8>> {
    let content = merge_ranges(files.iter().flatten().copied().collect());
    let cap = src.capacity_sectors();
    let dec = DecryptingSectorSource::new(src, keys)
        .with_key_map(std::sync::Arc::new(planned.map))
        .with_content_ranges(std::sync::Arc::from(content));
    let mut r = UnitAligned::new(dec, unit_spans(files, &[]));
    r.unproven = planned.unproven;
    let dir = tempfile::tempdir().map_err(|e| Error::IoError { source: e })?;
    let dest = dir.path().join("out.iso");
    crate::write_image(&mut r, &dest, cap, &crate::halt::Halt::new(), |_| {})?;
    std::fs::read(&dest).map_err(|e| Error::IoError { source: e })
}

fn assert_plain(img_plain: &[u8], bytes: &[u8]) {
    assert_eq!(bytes.len(), img_plain.len());
    for (lba, (got, want)) in bytes
        .chunks(SECTOR)
        .zip(img_plain.chunks(SECTOR))
        .enumerate()
    {
        assert!(got == want, "sector {lba} not decrypted as expected");
    }
}

#[test]
fn every_unplayed_file_is_keyed_and_the_image_decrypts_across_batches() {
    // Second file misaligned (start % 3 != 0) and crossing a 2048-sector batch edge.
    let specs = [(300, 30, K0, ALL), (2041, 30, K0, ALL)];
    let files = one_file_each(&specs);
    let mut src = img(2200, &specs);
    let mut keys = pool(&[K0]);
    let planned = plan(&mut src, &mut keys, &files, &rule(Some(0))).unwrap();
    assert!(planned.unproven.is_empty());
    let want = src.plain.clone();
    let bytes = write_through(img(2200, &specs), keys, &files, planned).unwrap();
    assert_plain(&want, &bytes);
}

/// Back-to-back unplayed files in different CPS units each get their own key.
#[test]
fn adjacent_files_in_different_cps_units_each_get_their_own_key() {
    let specs = [(300, 30, K0, ALL), (600, 30, K1, ALL), (630, 30, K2, ALL)];
    let files = one_file_each(&specs);
    let mut keys = pool(&[K0, K1, K2]);
    let planned = plan(&mut img(1200, &specs), &mut keys, &files, &rule(None)).unwrap();
    assert_eq!(planned.map.entry_for(600).map(|e| e.0), Some(1));
    assert_eq!(planned.map.entry_for(630).map(|e| e.0), Some(2));
    let bytes = write_through(img(1200, &specs), keys, &files, planned).unwrap();
    assert_plain(&img(1200, &specs).plain, &bytes);
}

/// Ciphertext no held key opens refuses the plan: multi-CPS pool, and a single key.
#[test]
fn a_file_no_held_key_opens_is_refused_up_front() {
    let specs = [(300, 30, K0, ALL), (600, 30, STRANGER, ALL)];
    let files = one_file_each(&specs);
    for (p, single) in [(&[K0, K1][..], None), (&[K0][..], Some(0))] {
        let r = plan(&mut img(1200, &specs), &mut pool(p), &files, &rule(single));
        assert!(
            matches!(r, Err(Error::WholeDiscKeyMissing)),
            "pool {}: {:?}",
            p.len(),
            r.err()
        );
    }
}

/// The resolver's evenly spaced samples miss a file encrypted only at its ends; the
/// probes cover every unit of a short file and prove the other held key twice.
#[test]
fn another_held_key_opening_two_probes_keys_the_file() {
    let specs = [(300, 30, K0, ALL), (600, 30, K1, &[0, 9][..])];
    let files = one_file_each(&specs);
    let mut src = img(1200, &specs);
    let planned = plan(&mut src, &mut pool(&[K0, K1]), &files, &rule(None)).unwrap();
    assert_eq!(planned.map.entry_for(600).map(|e| e.0), Some(1));
    assert!(planned.unproven.is_empty());
}

/// One opened probe is no proof: a chance TS-sync pass must not key a whole file.
/// With no unit left unopened the file is unproven (keyed only on single-CPS).
#[test]
fn a_key_opening_a_single_probe_is_not_trusted() {
    let specs = [(300, 30, K0, ALL), (600, 30, K1, &[0])];
    let files = one_file_each(&specs);
    let planned = plan(
        &mut img(1200, &specs),
        &mut pool(&[K0, K1]),
        &files,
        &rule(None),
    )
    .unwrap();
    assert_eq!(planned.map.entry_for(600), None);
    assert_eq!(planned.unproven, vec![(600, 630)]);
    let planned = plan(
        &mut img(1200, &specs),
        &mut pool(&[K0, K1]),
        &files,
        &rule(Some(0)),
    )
    .unwrap();
    assert_eq!(planned.map.entry_for(600).map(|e| e.0), Some(0));
}

/// A key that opens one probe while another encrypted unit opens under none is
/// demonstrably unkeyable: refused, not trusted on the one pass.
#[test]
fn a_single_opened_probe_beside_an_unopenable_unit_is_refused() {
    let mut src = img(1200, &[(300, 30, K0, ALL), (600, 30, K1, &[0])]);
    let other = img(1200, &[(600, 30, STRANGER, &[5])]);
    let (a, b) = (615 * SECTOR, 618 * SECTOR);
    src.data[a..b].copy_from_slice(&other.data[a..b]);
    let files = vec![vec![(300, 30)], vec![(600, 30)]];
    let r = plan(&mut src, &mut pool(&[K0, K1]), &files, &rule(None));
    assert!(
        matches!(r, Err(Error::WholeDiscKeyMissing)),
        "{:?}",
        r.err()
    );
}

/// Alternates are the held BASE keys only: a forensic (FMTS-tagged) pool key never
/// keys a whole file, while another base key still does.
#[test]
fn alternates_never_use_forensic_pool_keys() {
    // Only the ends encrypted: the resolver's samples miss the file.
    let specs = [(300, 30, K0, ALL), (600, 30, K1, &[0, 9][..])];
    let files = one_file_each(&specs);
    let mut src = img(1200, &specs);
    let tagged = |extra: &[(u32, [u8; 16])]| {
        let mut unit_keys = vec![(0u32, K0)];
        unit_keys.extend_from_slice(extra);
        DecryptKeys::Aacs {
            unit_keys,
            format: crate::ContentFormat::BdTs,
        }
    };
    let mut forensic = tagged(&[(1 << 24, K1)]);
    let r = plan(&mut src, &mut forensic, &files, &rule(None));
    assert!(
        matches!(r, Err(Error::WholeDiscKeyMissing)),
        "{:?}",
        r.err()
    );
    let mut base = tagged(&[(1 << 24, STRANGER), (1, K1)]);
    let planned = plan(&mut src, &mut base, &files, &rule(None)).unwrap();
    assert_eq!(planned.map.entry_for(600).map(|e| e.0), Some(2));
}

/// Last resort: no probe is readable, so the file stays unkeyed and the copy stops
/// at its first encrypted unit with E7032, never writing ciphertext.
#[test]
fn an_unreadable_probe_leaves_the_file_unproven_and_the_copy_stops_with_e7032() {
    let specs = [(300, 30, K0, ALL), (600, 30, STRANGER, ALL)];
    let files = one_file_each(&specs);
    let mut src = img(1200, &specs);
    src.probe_fail = Some((600, 630));
    let mut keys = pool(&[K0, K1]);
    let planned = plan(&mut src, &mut keys, &files, &rule(None)).unwrap();
    assert_eq!(planned.unproven, vec![(600, 630)]);
    let r = write_through(img(1200, &specs), keys, &files, planned);
    assert!(
        matches!(r, Err(Error::WholeDiscKeyMissing)),
        "{:?}",
        r.err()
    );
}

/// A stop request is honoured between probes.
#[test]
fn probing_honours_a_stop() {
    let specs = [(300, 30, K0, ALL), (600, 30, K1, ALL)];
    let files = one_file_each(&specs);
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let rule = KeyRule {
        halt: Some(&halt),
        ..rule(None)
    };
    let r = plan(&mut img(1200, &specs), &mut pool(&[K0, K1]), &files, &rule);
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
}

/// SSIF re-lists an m2ts's extents: that file is probed once, the map stays disjoint.
#[test]
fn a_relisted_file_is_keyed_once_into_a_disjoint_map() {
    let files = vec![vec![(300, 30)], vec![(600, 30)], vec![(600, 30)]];
    let mut src = img(1200, &[(300, 30, K0, ALL), (600, 30, K0, ALL)]);
    let planned = plan(&mut src, &mut pool(&[K0]), &files, &rule(Some(0))).unwrap();
    let r = planned.map.ranges();
    assert!(r.windows(2).all(|w| w[0].1 <= w[1].0), "disjoint: {r:?}");
    let probes = src
        .reads
        .iter()
        .filter(|&&(l, n)| l >= 600 && n == 3)
        .count();
    assert!(probes <= PROBES as usize + 8, "probed twice: {probes}");
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

// A minimal AACS 1.0 `Unit_Key_RO.inf` declaring `units` CPS units.
fn unit_key_ro(units: u16) -> Vec<u8> {
    let mut v = vec![0u8; 96 + 48 * units as usize];
    v[..4].copy_from_slice(&48u32.to_be_bytes());
    v[48..50].copy_from_slice(&units.to_be_bytes());
    v
}

fn aacs_disc(declared: u16) -> crate::Disc {
    crate::Disc {
        volume_id: String::new(),
        meta_title: None,
        format: crate::DiscFormat::BluRay,
        capacity_sectors: 1000,
        capacity_bytes: 1000 * 2048,
        layers: 1,
        titles: Vec::new(),
        region: crate::disc::DiscRegion::Free,
        aacs: Some(crate::disc::AacsState {
            version: 1,
            bus_encryption: false,
            mkb_version: None,
            disc_hash: String::new(),
            key_source: crate::disc::KeyOrigin::DeviceKey,
            vuk: None,
            unit_keys: vec![(1, K0)],
            volume_id: [0; 16],
            uk_ro: unit_key_ro(declared),
            mkb: Vec::new(),
        }),
        css: None,
        encrypted: true,
        aacs_error: None,
        css_error: None,
        content_format: crate::ContentFormat::BdTs,
    }
}

/// Single-CPS is the DECLARED unit count, never the pool size.
#[test]
fn single_cps_follows_the_declared_unit_count_not_the_pool() {
    let empty = AacsKeyMap::from_ranges(Vec::new());
    let keys = pool(&[K0]);
    assert_eq!(single_cps_key_slot(&aacs_disc(1), &keys, &empty), Some(0));
    assert_eq!(single_cps_key_slot(&aacs_disc(2), &keys, &empty), None);
    let mut fmts = aacs_disc(1);
    fmts.format = crate::DiscFormat::Fmts;
    assert_eq!(single_cps_key_slot(&fmts, &keys, &empty), None);
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
fn unit_aligned_refuses_a_unit_split_across_extents() {
    let mut r = reader(vec![(100, 4, 100), (300, 5, 299)]);
    let mut buf = vec![0u8; 2048];
    assert!(matches!(
        r.read_sectors(300, 1, &mut buf, false),
        Err(Error::DecryptFailed)
    ));
}

/// A decrypt refusal is E7032 only inside an unproven piece.
#[test]
fn a_refusal_in_an_unproven_piece_is_the_mkv_or_raw_error() {
    let mut r = reader(vec![(100, 30, 100)]);
    r.unproven = vec![(110, 130)];
    r.inner.fail = Some(|| Error::DecryptFailed);
    let mut buf = vec![0u8; 2048];
    let got = r.read_sectors(111, 1, &mut buf, false);
    assert!(matches!(got, Err(Error::WholeDiscKeyMissing)), "{got:?}");
    let got = r.read_sectors(101, 1, &mut buf, false);
    assert!(matches!(got, Err(Error::DecryptFailed)), "{got:?}");
    r.inner.fail = Some(|| Error::Halted);
    let got = r.read_sectors(111, 1, &mut buf, false);
    assert!(matches!(got, Err(Error::Halted)), "{got:?}");
}

#[test]
fn unit_block_end_pulls_back_to_the_straddling_units_head() {
    let r = reader(vec![(100, 30, 100)]);
    assert_eq!(r.unit_block_end(90, 104), 103);
    assert_eq!(r.unit_block_end(90, 103), 103);
    assert_eq!(r.unit_block_end(103, 104), 104);
    assert_eq!(r.unit_block_end(0, 50), 50);
}

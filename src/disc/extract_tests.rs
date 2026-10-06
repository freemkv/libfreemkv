use super::*;
use crate::css::lfsr;
use std::collections::HashMap;

// ── Self-contained in-memory UDF fixture toolkit ──────────────────
// Modeled on the bluray.rs / dvd.rs fixtures: a `MemDisc` SectorSource
// backed by an absolute-LBA map; AACS tests use clear bytes.

const PART_START: u32 = 2000;

struct MemDisc {
    sectors: HashMap<u32, [u8; 2048]>,
    /// Absolute LBAs that fail to read (bad-sector fixture → DiscRead).
    bad: std::collections::HashSet<u32>,
    /// Absolute LBAs whose read fails to DECRYPT (no/wrong key fixture →
    /// DecryptFailed), exercising the undecryptable-unit loss path.
    decrypt_fail: std::collections::HashSet<u32>,
    /// Absolute LBAs whose read reports a drive-level user Stop (`Halted`).
    halted: std::collections::HashSet<u32>,
}

impl MemDisc {
    fn new() -> Self {
        Self {
            sectors: HashMap::new(),
            bad: std::collections::HashSet::new(),
            decrypt_fail: std::collections::HashSet::new(),
            halted: std::collections::HashSet::new(),
        }
    }
    fn put(&mut self, lba: u32, data: [u8; 2048]) {
        self.sectors.insert(lba, data);
    }
    fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
        for (i, chunk) in bytes.chunks(2048).enumerate() {
            let mut s = [0u8; 2048];
            s[..chunk.len()].copy_from_slice(chunk);
            self.put(lba + i as u32, s);
        }
    }
}

impl SectorSource for MemDisc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let need = count as usize * 2048;
        for i in 0..count as u32 {
            if self.bad.contains(&(lba + i)) {
                return Err(Error::DiscRead {
                    sector: (lba + i) as u64,
                    status: None,
                    sense: None,
                });
            }
            if self.decrypt_fail.contains(&(lba + i)) {
                return Err(Error::DecryptFailed);
            }
            if self.halted.contains(&(lba + i)) {
                return Err(Error::Halted);
            }
        }
        for i in 0..count as u32 {
            let off = i as usize * 2048;
            let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
            buf[off..off + 2048].copy_from_slice(&s);
        }
        Ok(need)
    }
}

struct FileSpec {
    name: String,
    icb_lba: u32,
    data_lba: u32,
    size: u32,
    long_ad: bool,
    contents: Vec<u8>,
}

struct DirSpec {
    name: String,
    icb_lba: u32,
    dir_data_lba: u32,
    files: Vec<FileSpec>,
    subdirs: Vec<DirSpec>,
}

fn file(name: &str, icb_lba: u32, data_lba: u32, contents: Vec<u8>, long_ad: bool) -> FileSpec {
    FileSpec {
        name: name.to_string(),
        icb_lba,
        data_lba,
        size: contents.len() as u32,
        long_ad,
        contents,
    }
}

fn build_file_icb(size: u32, data_lba: u32, long_ad: bool) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
    if long_ad {
        s[34..36].copy_from_slice(&1u16.to_le_bytes()); // Long AD
    }
    s[56..64].copy_from_slice(&(size as u64).to_le_bytes()); // info_length
    s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
    let ad_size: u32 = if long_ad { 16 } else { 8 };
    s[212..216].copy_from_slice(&ad_size.to_le_bytes()); // l_ad
    s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
    s[220..224].copy_from_slice(&data_lba.to_le_bytes());
    s
}

fn build_dir_icb(dir_data_lba: u32, dir_data_len: u32) -> [u8; 2048] {
    build_file_icb(dir_data_len, dir_data_lba, false)
}

fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
    let start = buf.len();
    let name_field: Vec<u8> = if is_parent {
        Vec::new()
    } else {
        let mut v = vec![0x08u8];
        v.extend_from_slice(name.as_bytes());
        v
    };
    let l_fi = name_field.len();
    let mut fid = vec![0u8; 38];
    fid[0..2].copy_from_slice(&257u16.to_le_bytes());
    let mut file_chars = 0u8;
    if is_dir {
        file_chars |= 0x02;
    }
    if is_parent {
        file_chars |= 0x08;
    }
    fid[18] = file_chars;
    fid[19] = l_fi as u8;
    fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
    fid[36..38].copy_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&fid);
    buf.extend_from_slice(&name_field);
    let used = buf.len() - start;
    buf.resize(start + ((used + 3) & !3), 0);
}

fn lay_dir(disc: &mut MemDisc, dir: &DirSpec) {
    let mut fids = Vec::new();
    push_fid(&mut fids, "", dir.icb_lba, true, true);
    for f in &dir.files {
        push_fid(&mut fids, &f.name, f.icb_lba, false, false);
        disc.put(
            PART_START + f.icb_lba,
            build_file_icb(f.size, f.data_lba, f.long_ad),
        );
        if !f.contents.is_empty() {
            disc.put_bytes(PART_START + f.data_lba, &f.contents);
        }
    }
    for sub in &dir.subdirs {
        push_fid(&mut fids, &sub.name, sub.icb_lba, true, false);
    }
    disc.put(
        PART_START + dir.icb_lba,
        build_dir_icb(dir.dir_data_lba, fids.len() as u32),
    );
    disc.put_bytes(PART_START + dir.dir_data_lba, &fids);
    for sub in &dir.subdirs {
        lay_dir(disc, sub);
    }
}

fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
    let mut avdp = [0u8; 2048];
    avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
    disc.put(256, avdp);
    let mut pd = [0u8; 2048];
    pd[0..2].copy_from_slice(&5u16.to_le_bytes());
    pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
    disc.put(32, pd);
    let mut lvd = [0u8; 2048];
    lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
    lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
    disc.put(33, lvd);
    let mut td = [0u8; 2048];
    td[0..2].copy_from_slice(&8u16.to_le_bytes());
    disc.put(34, td);
    let mut fsd = [0u8; 2048];
    fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
    fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
    disc.put(PART_START, fsd);
}

/// Lay a full root DirSpec and return a navigable disc.
fn build_disc(root: DirSpec) -> MemDisc {
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, root.icb_lba);
    lay_dir(&mut disc, &root);
    disc
}

/// A unique temp dir for one test's output, removed on drop.
struct TmpDir(PathBuf);
impl TmpDir {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "freemkv_extract_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        Self(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// A file ICB with TWO Short ADs (a fragmented / multi-extent file), each
// recording `sectors_each` sectors at its own `data_lba`. Exercises
// per-extent AACS unit-base re-anchoring across a non-3-aligned gap.
fn build_two_extent_icb(sectors_each: u32, data_lba_a: u32, data_lba_b: u32) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
    // ad_type 0 = Short AD (icb flags low 3 bits at offset 34).
    s[34..36].copy_from_slice(&0u16.to_le_bytes());
    let size = sectors_each * SECTOR_BYTES as u32 * 2;
    s[56..64].copy_from_slice(&(size as u64).to_le_bytes()); // info_length
    s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
    s[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad = 2 Short ADs
    let ext_len = sectors_each * SECTOR_BYTES as u32; // bytes, type-0 recorded
    // AD #0
    s[216..220].copy_from_slice(&(ext_len & 0x3FFF_FFFF).to_le_bytes());
    s[220..224].copy_from_slice(&data_lba_a.to_le_bytes());
    // AD #1
    s[224..228].copy_from_slice(&(ext_len & 0x3FFF_FFFF).to_le_bytes());
    s[228..232].copy_from_slice(&data_lba_b.to_le_bytes());
    s
}

// Like `build_two_extent_icb` but the two extents may have DIFFERENT
// sector counts — exercises within-extent batch-size arithmetic with a
// first extent long enough to force a second while-loop iteration.
fn build_two_extent_icb_sized(
    sectors_a: u32,
    data_lba_a: u32,
    sectors_b: u32,
    data_lba_b: u32,
) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
    s[34..36].copy_from_slice(&0u16.to_le_bytes()); // Short AD
    let size = (sectors_a as u64 + sectors_b as u64) * SECTOR_BYTES as u64;
    s[56..64].copy_from_slice(&size.to_le_bytes()); // info_length
    s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
    s[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad = 2 Short ADs
    let len_a = sectors_a * SECTOR_BYTES as u32;
    let len_b = sectors_b * SECTOR_BYTES as u32;
    s[216..220].copy_from_slice(&(len_a & 0x3FFF_FFFF).to_le_bytes());
    s[220..224].copy_from_slice(&data_lba_a.to_le_bytes());
    s[224..228].copy_from_slice(&(len_b & 0x3FFF_FFFF).to_le_bytes());
    s[228..232].copy_from_slice(&data_lba_b.to_le_bytes());
    s
}

/// Build a file ICB whose FIRST short AD is an ECMA-167 4/14.14.1.1 type-1
/// (allocated, NOT recorded) extent and whose second is ordinary recorded
/// data. Both are `sectors_each` sectors long.
fn build_hole_then_data_icb(sectors_each: u32, hole_lba: u32, data_lba: u32) -> [u8; 2048] {
    let mut s = build_two_extent_icb(sectors_each, hole_lba, data_lba);
    let len = sectors_each * SECTOR_BYTES as u32;
    // Re-stamp AD #0 with extent type 1 in bits 30..31 of the length field.
    s[216..220].copy_from_slice(&(0x4000_0000u32 | (len & 0x3FFF_FFFF)).to_le_bytes());
    s
}

/// Encrypt the clear unit from `clear_aacs_unit(tag)` under `unit_key` so
/// `aacs::content::decrypt_unit` recovers it cleanly (zero decrypt loss).
/// `tag` distinguishes two units' payloads.
fn encrypt_aacs_unit(unit_key: &[u8; 16], tag: u8) -> Vec<u8> {
    let mut unit = clear_aacs_unit(tag);
    assert!(
        crate::aacs::content::encrypt_unit(&mut unit, unit_key),
        "a full-length unit must encrypt"
    );
    unit
}

/// The plaintext that `encrypt_aacs_unit(_, tag)` decrypts back to.
fn clear_aacs_unit(tag: u8) -> Vec<u8> {
    let mut unit = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
    let mut off = 4;
    while off < unit.len() {
        unit[off] = 0x47;
        if off + 1 < unit.len() {
            unit[off + 1] = tag;
        }
        off += 192;
    }
    // decrypt preserves the plaintext header, so the recovered unit carries
    // the CPI bits the encrypt fixture set — the expected plaintext must too.
    unit[0] |= 0xC0;
    unit
}

// An AACS `Disc`: its key comes from a set over it (`keyed_for_test`), which engages
// the unit-alignment gate. Content is genuinely encrypted under the key, so a clean
// decrypt isolates the GATE from a false decrypt-loss tally.
fn aacs_disc() -> Disc {
    let mut d = clear_disc();
    d.encrypted = true;
    d.aacs = Some(crate::disc::AacsState {
        version: 1,
        bus_encryption: false,
        mkb_version: None,
        disc_hash: String::new(),
        volume_id: [0u8; 16],
        uk_ro: Vec::new(),
        mkb: Vec::new(),
    });
    d
}

/// A `Disc` with no cipher state (clear content → `DecryptKeys::None`).
fn clear_disc() -> Disc {
    Disc {
        volume_id: "TEST".into(),
        meta_title: Some("TEST".into()),
        format: crate::disc::DiscFormat::BluRay,
        capacity_sectors: 100_000,
        capacity_bytes: 100_000 * 2048,
        layers: 1,
        titles: Vec::new(),
        region: crate::disc::DiscRegion::Free,
        aacs: None,
        css: None,
        encrypted: false,
        aacs_error: None,
        css_error: None,
        content_format: crate::disc::ContentFormat::BdTs,
    }
}

fn read_out(dir: &Path, rel: &str) -> Option<Vec<u8>> {
    std::fs::read(dir.join(rel)).ok()
}

// ── Tests ─────────────────────────────────────────────────────────────

/// BDMV extraction: STREAM/*.m2ts written decrypted (here clear via
/// None keys), nav (index.bdmv / MovieObject.bdmv / PLAYLIST / CLIPINF)
/// verbatim, and the top-level AACS/ directory stripped entirely.
#[test]
fn bdmv_extracts_streams_and_nav_and_strips_aacs() {
    let m2ts = vec![0xABu8; 3 * 2048]; // one AACS unit's worth
    let index = b"INDEX-NAV".to_vec();
    let movieobj = b"MOVIEOBJECT-NAV".to_vec();
    let mpls = b"MPLS-PLAYLIST".to_vec();
    let clpi = b"CLPI-CLIPINF".to_vec();
    let aacs_inf = b"AACS-KEY-FILE".to_vec();

    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![
                    file("index.bdmv", 30, 31, index.clone(), false),
                    file("MovieObject.bdmv", 32, 33, movieobj.clone(), false),
                ],
                subdirs: vec![
                    DirSpec {
                        name: "STREAM".to_string(),
                        icb_lba: 40,
                        dir_data_lba: 41,
                        files: vec![file("00001.m2ts", 42, 5000, m2ts.clone(), true)],
                        subdirs: vec![],
                    },
                    DirSpec {
                        name: "PLAYLIST".to_string(),
                        icb_lba: 44,
                        dir_data_lba: 45,
                        files: vec![file("00000.mpls", 46, 47, mpls.clone(), false)],
                        subdirs: vec![],
                    },
                    DirSpec {
                        name: "CLIPINF".to_string(),
                        icb_lba: 48,
                        dir_data_lba: 49,
                        files: vec![file("00001.clpi", 50, 51, clpi.clone(), false)],
                        subdirs: vec![],
                    },
                ],
            },
            DirSpec {
                name: "AACS".to_string(),
                icb_lba: 60,
                dir_data_lba: 61,
                files: vec![file("Unit_Key_RO.inf", 62, 63, aacs_inf, false)],
                subdirs: vec![],
            },
        ],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("bdmv");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");

    assert_eq!(
        read_out(out.path(), "BDMV/STREAM/00001.m2ts"),
        Some(m2ts),
        "m2ts content extracted intact"
    );
    assert_eq!(read_out(out.path(), "BDMV/index.bdmv"), Some(index));
    assert_eq!(
        read_out(out.path(), "BDMV/MovieObject.bdmv"),
        Some(movieobj)
    );
    assert_eq!(read_out(out.path(), "BDMV/PLAYLIST/00000.mpls"), Some(mpls));
    assert_eq!(read_out(out.path(), "BDMV/CLIPINF/00001.clpi"), Some(clpi));
    // AACS/ stripped: neither the dir nor its file exists.
    assert!(!out.path().join("AACS").exists(), "AACS/ must be stripped");
    assert!(res.complete, "clean extraction is complete");
    assert_eq!(res.bytes_lost(), 0);
}

/// An HD DVD's `X!` AACS dir is stripped like BD's `AACS/`, so the decrypted
/// folder does not rescan as AACS-encrypted.
#[test]
fn hddvd_extract_strips_the_aacs_dir() {
    let evo = vec![0x5Au8; 3 * 2048];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file("MAIN.EVO", 30, 5000, evo.clone(), false)],
                subdirs: vec![],
            },
            DirSpec {
                name: "AAC!".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![
                    file("MKBROM.AACS", 32, 33, vec![1; 64], false),
                    file("VTKF000.AACS", 34, 35, vec![2; 64], false),
                ],
                subdirs: vec![],
            },
        ],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("hddvd");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    d.extract_tree(
        &mut disc,
        out.path(),
        &ExtractOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .expect("extract");
    assert_eq!(read_out(out.path(), "HVDVD_TS/MAIN.EVO"), Some(evo));
    assert!(!out.path().join("AAC!").exists(), "AAC!/ must be stripped");
}

/// VIDEO_TS extraction writes VOBs + IFO/BUP. Here the content is clear
/// (None keys), proving the tree walk + per-file write for the DVD layout;
/// CSS descramble correctness is tested separately below.
#[test]
fn video_ts_extracts_vobs_and_ifo() {
    let ifo = b"VIDEO_TS.IFO".to_vec();
    let vob = vec![0x5Au8; 2 * 2048];
    let bup = b"VIDEO_TS.BUP".to_vec();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![
                file("VIDEO_TS.IFO", 30, 31, ifo.clone(), false),
                file("VTS_01_1.VOB", 32, 5000, vob.clone(), false),
                file("VIDEO_TS.BUP", 34, 35, bup.clone(), false),
            ],
            subdirs: vec![],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("videots");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    let res = d
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");
    assert_eq!(read_out(out.path(), "VIDEO_TS/VIDEO_TS.IFO"), Some(ifo));
    assert_eq!(read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB"), Some(vob));
    assert_eq!(read_out(out.path(), "VIDEO_TS/VIDEO_TS.BUP"), Some(bup));
    assert!(res.complete);
}

/// A CSS-scrambled title VOB is descrambled on extraction: the producer
/// recovers the per-VTS key from the scrambled sectors and the output VOB
/// is plain.
#[test]
fn css_title_vob_is_descrambled() {
    let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
    // Build a scrambled sector with a crackable repeating crib (period 8),
    // mirroring css::mod tests' `crackable_sector`.
    let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
    let mut plain = vec![0u8; 2048];
    // Pack start code: a real scrambled sector is an MPEG-2 PS pack, and
    // the descrambler requires it before trusting byte 0x14.
    plain[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    plain[4] = 0x44; // '01': a 13818-1 pack
    plain[0x14] = 0x10; // scramble flag
    crate::css::dvd_pack_header(&mut plain, 0xE0);
    let pat: Vec<u8> = (0..8)
        .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
        .collect();
    for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
        *b = pat[i % 8];
    }
    plain[0x54..0x59].copy_from_slice(&seed);
    let mut scrambled = plain.clone();
    lfsr::scramble_sector(&title_key, &mut scrambled);

    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file("VTS_01_1.VOB", 30, 5000, scrambled, false)],
            subdirs: vec![],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("css");
    // A CSS disc with a (provenance-unknown) cracked key; the producer
    // re-cracks per VTS, so the disc-wide key value is irrelevant here.
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    d.css = Some(crate::css::CssState {
        title_key,
        crack_span: None,
    });
    let res = d
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");
    let got = read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB").expect("vob");
    // Descrambled output matches the plaintext, with the scramble flag
    // cleared by the descrambler.
    let mut expect = plain.clone();
    expect[0x14] = 0x80;
    assert_eq!(got, expect, "VOB descrambled to plaintext");
    assert!(res.complete);
}

/// BUG-1: a live DVD scan records no disc-wide CSS key (`disc.css` is `None`), yet its
/// scrambled title VOB is still found from content, cracked and descrambled.
#[test]
fn a_live_dvd_with_no_scanned_css_key_still_descrambles() {
    let title_key = [0x42u8, 0x13, 0x37, 0xBE, 0xEF];
    let seed = [0x11u8, 0x22, 0x33, 0x44, 0x55];
    let mut plain = vec![0u8; 2048];
    plain[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    plain[4] = 0x44;
    plain[0x14] = 0x10;
    crate::css::dvd_pack_header(&mut plain, 0xE0);
    let pat: Vec<u8> = (0..8)
        .map(|k| (0xA0u8.wrapping_add(k as u8)) ^ 0x5A)
        .collect();
    for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
        *b = pat[i % 8];
    }
    plain[0x54..0x59].copy_from_slice(&seed);
    let mut scrambled = plain.clone();
    lfsr::scramble_sector(&title_key, &mut scrambled);
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file("VTS_01_1.VOB", 30, 5000, scrambled, false)],
            subdirs: vec![],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("css_live");
    let mut d = clear_disc();
    d.format = crate::disc::DiscFormat::Dvd;
    d.content_format = crate::disc::ContentFormat::MpegPs;
    assert!(d.css.is_none(), "a live scan's verdict: no disc-wide key");
    d.extract_tree(
        &mut disc,
        out.path(),
        &ExtractOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .expect("extract");
    let got = read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB").expect("vob");
    let mut expect = plain.clone();
    expect[0x14] = 0x80;
    assert_eq!(got, expect, "the scrambled VOB is written descrambled");
}

/// A bad sector inside a file becomes a recorded zero-filled hole; the run
/// does not abort, the file is still written, and loss is accounted.
#[test]
fn bad_sector_holes_file_and_accounts_loss() {
    let good = vec![0x77u8; 4 * 2048];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![file("00001.m2ts", 24, 5000, good.clone(), true)],
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = build_disc(root);
    // Mark the whole 4-sector extent bad so a batch read fails. Abs LBAs
    // are PART_START + data_lba (5000) .. +3.
    for i in 0..4u32 {
        disc.bad.insert(PART_START + 5000 + i);
    }
    let out = TmpDir::new("badsector");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract does not abort on bad sectors");
    let got = read_out(out.path(), "BDMV/STREAM/00001.m2ts").expect("file written");
    assert_eq!(
        got.len(),
        good.len(),
        "holed file still sized to declared size"
    );
    assert!(got.iter().all(|&b| b == 0), "bad range zero-filled");
    assert!(!res.complete, "lossy extraction is not complete");
    assert_eq!(res.bytes_unreadable, good.len() as u64);
    assert_eq!(res.files.len(), 1);
    assert_eq!(res.files[0].bytes_unreadable, good.len() as u64);
}

// An UNDECRYPTABLE unit (wrong/missing key) is zero-filled and counted as
// loss exactly like a bad sector; the run must still report complete ==
// false and bytes_lost() > 0 (gates the CLI exit code / multipass re-run).
#[test]
fn undecryptable_unit_holes_file_and_accounts_loss() {
    let good = vec![0x55u8; 4 * 2048];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![file("00001.m2ts", 24, 5000, good.clone(), true)],
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = build_disc(root);
    // The whole extent fails to decrypt (no/wrong key) rather than to read.
    for i in 0..4u32 {
        disc.decrypt_fail.insert(PART_START + 5000 + i);
    }
    let out = TmpDir::new("decryptfail");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract does not abort on an undecryptable unit");
    let got = read_out(out.path(), "BDMV/STREAM/00001.m2ts").expect("file written");
    assert_eq!(
        got.len(),
        good.len(),
        "holed file still sized to declared size"
    );
    assert!(
        got.iter().all(|&b| b == 0),
        "undecryptable range zero-filled"
    );
    assert!(
        !res.complete,
        "an undecryptable unit makes the rip incomplete"
    );
    assert!(
        res.bytes_lost() > 0,
        "decrypt loss counted, not reported clean"
    );
    assert_eq!(res.bytes_unreadable, good.len() as u64);
    assert_eq!(res.files[0].bytes_unreadable, good.len() as u64);
}

/// Path sanitization rejects a host-illegal component in a disc file name.
#[test]
fn sanitize_rejects_illegal_component() {
    assert_eq!(sanitize_component("good_name.m2ts"), "good_name.m2ts");
    assert_eq!(sanitize_component(".."), "_");
    assert_eq!(sanitize_component("a/b"), "a_b");
    assert_eq!(sanitize_component("a:b"), "a_b");
    assert_eq!(sanitize_component("a*b"), "a_b");
    // Windows reserved device names are substituted (prefixed `_`), not
    // rejected — a single such file must not abort the whole tree walk.
    assert_eq!(sanitize_component("CON"), "_CON");
    assert_eq!(sanitize_component("com1"), "_com1");
    assert_eq!(sanitize_component("LPT9"), "_LPT9");
    // Reserved base with an extension is still substituted (the device name
    // aliases regardless of extension on Windows).
    assert_eq!(sanitize_component("NUL.cfg"), "_NUL.cfg");
    assert_eq!(sanitize_component("conin$"), "_conin$");
    // A non-reserved lookalike is untouched.
    assert_eq!(sanitize_component("COM10"), "COM10");
    // COM0 and the superscript digits are reserved too.
    assert_eq!(sanitize_component("COM0"), "_COM0");
    assert_eq!(sanitize_component("LPT\u{b2}"), "_LPT\u{b2}");
    assert_eq!(sanitize_component("CLOCK$"), "_CLOCK$");
    assert_eq!(sanitize_component("CONSOLE"), "CONSOLE");
    // A trailing dot/space is stripped, not rejected outright.
    assert_eq!(sanitize_component("name. "), "name");
    // ...unless stripping empties it.
    assert_eq!(sanitize_component(". "), "_");
}

/// Every host-illegal character is replaced, including the Windows path
/// separator, so a disc name cannot climb out of the extract root.
#[test]
fn sanitize_replaces_every_reserved_character() {
    for c in ['/', '\\', ':', '<', '>', '"', '|', '?', '*'] {
        assert_eq!(sanitize_component(&format!("a{c}b")), "a_b", "{c:?}");
    }
    assert_eq!(sanitize_component("a\\..\\x"), "a_.._x");
}

/// Two distinct disc paths that sanitize to the same host path are a hard
/// error (collision), never a silent overwrite.
#[test]
fn name_collision_is_error() {
    // Two files in the same dir whose names both reduce to "movie" after
    // the trailing-dot/space strip ("movie" and "movie.").
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("movie", 30, 31, b"a".to_vec(), false),
            file("movie.", 32, 33, b"b".to_vec(), false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("collision");
    let err = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err("collision must error");
    assert!(matches!(err, Error::DirNameCollision { .. }));
}

// Two names differing only by CASE collide on a case-INSENSITIVE host (macOS
// APFS / Windows NTFS) but coexist on a case-SENSITIVE one, so the extract
// must abort on the former and not the latter — outcome tracks the real volume.
#[test]
fn names_differing_only_by_case_collide_only_on_a_case_insensitive_host() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("Movie", 30, 31, b"a".to_vec(), false),
            file("movie", 32, 33, b"b".to_vec(), false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("case_collision");
    // Create the dir before probing so the probe reflects the REAL volume:
    // `dir_is_case_insensitive` returns a conservative `true` on a missing dir,
    // disagreeing with `extract_tree`'s own post-mkdir probe on a case-sensitive host.
    std::fs::create_dir_all(out.path()).unwrap();
    // Expectation from an independent std-only oracle, NOT the probe under test.
    let insensitive = oracle_case_insensitive(out.path());
    let res = clear_disc().extract_tree(
        &mut disc,
        out.path(),
        &ExtractOptions::default(),
        &crate::ctx::Ctx::default(),
    );
    if insensitive {
        let err = res.expect_err("two names that fold to one host file must collide");
        assert!(matches!(err, Error::DirNameCollision { .. }), "got {err:?}");
    } else {
        let out = res.expect("distinct-case names coexist on a case-sensitive volume");
        assert!(
            out.files.iter().all(|f| f.complete),
            "both case-distinct files must extract cleanly on a case-sensitive host"
        );
    }
}

// A file's in-flight `.partial` path shares the host namespace with another planned file's
// FINAL path (disc carries both `X` and `X.partial`).
#[test]
fn a_files_partial_path_colliding_with_another_files_final_name_is_an_error() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("X", 30, 31, b"aaaa".to_vec(), false),
            file("X.partial", 32, 33, b"bbbb".to_vec(), false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("partial_collision");
    let err = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err(
            "X's temp path IS X.partial's final path — one host file for two \
                 disc files, so it must be refused up front",
        );
    assert!(matches!(err, Error::DirNameCollision { .. }), "got {err:?}");
}

/// A non-empty target dir is refused without `--force`, and accepted with.
#[test]
fn non_empty_target_requires_force() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![file("a.bin", 30, 31, b"hello".to_vec(), false)],
        subdirs: vec![],
    };
    let out = TmpDir::new("nonempty");
    std::fs::create_dir_all(out.path()).unwrap();
    std::fs::write(out.path().join("preexisting.txt"), b"x").unwrap();

    let mut disc = build_disc(root);
    let err = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err("non-empty dir without --force must error");
    assert!(matches!(err, Error::DirNotEmpty));

    // With --force it proceeds.
    let mut disc2 = build_disc(DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![file("a.bin", 30, 31, b"hello".to_vec(), false)],
        subdirs: vec![],
    });
    let opts = ExtractOptions {
        force: true,
        ..Default::default()
    };
    let res = clear_disc()
        .extract_tree(&mut disc2, out.path(), &opts, &crate::ctx::Ctx::default())
        .expect("force proceeds");
    assert_eq!(read_out(out.path(), "a.bin"), Some(b"hello".to_vec()));
    assert!(res.complete);
}

/// VTS grouping + title-VOB classification used for per-VTS CSS keys.
#[test]
fn vts_grouping_and_title_vob_classification() {
    assert_eq!(vts_group_of("VTS_01_1.VOB").as_deref(), Some("VTS_01"));
    assert_eq!(vts_group_of("VTS_12_0.VOB").as_deref(), Some("VTS_12"));
    assert_eq!(vts_group_of("VIDEO_TS.IFO"), None);
    assert!(is_title_vob("VTS_01_1.VOB"));
    assert!(is_title_vob("VTS_01_9.VOB"));
    assert!(
        !is_title_vob("VTS_01_0.VOB"),
        "menu VOB is clear, not title"
    );
    assert!(!is_title_vob("VTS_01_1.IFO"));
}

// Regression (rc.6 audit, finding #449): a MULTI-EXTENT AACS file must re-anchor the
// unit-alignment base PER extent, not once at the first.
#[test]
fn multi_extent_aacs_anchors_unit_base_per_extent() {
    const SECTORS_EACH: u32 = 3; // one AACS unit per extent
    const DATA_A: u32 = 5000; // abs PART_START+5000 (≡ 7000)
    const DATA_B: u32 = 5004; // abs PART_START+5004 — Δ4 (not mult of 3)

    let key = [0u8; 16];
    // Each extent is exactly one encrypted AACS unit (distinct payloads).
    let ext_a = encrypt_aacs_unit(&key, 0xA1);
    let ext_b = encrypt_aacs_unit(&key, 0xB2);
    // Expected plaintext after a correct per-extent decrypt.
    let mut expect = clear_aacs_unit(0xA1);
    expect.extend_from_slice(&clear_aacs_unit(0xB2));
    // KS-5: a decrypted unit's CPI reads 00₂ (KU design §5.4).
    let expect = crate::aacs::content::cpi_cleared(expect);

    // Lay the disc by hand: root dir with one BDMV/STREAM/00001.m2ts whose
    // ICB carries two Short ADs.
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);

    // root → BDMV → STREAM → 00001.m2ts
    let mut stream_fids = Vec::new();
    push_fid(&mut stream_fids, "", 40, true, true);
    push_fid(&mut stream_fids, "00001.m2ts", 42, false, false);
    disc.put(
        PART_START + 42,
        build_two_extent_icb(SECTORS_EACH, DATA_A, DATA_B),
    );
    disc.put_bytes(PART_START + DATA_A, &ext_a);
    disc.put_bytes(PART_START + DATA_B, &ext_b);
    disc.put(PART_START + 40, build_dir_icb(41, stream_fids.len() as u32));
    disc.put_bytes(PART_START + 41, &stream_fids);

    let mut bdmv_fids = Vec::new();
    push_fid(&mut bdmv_fids, "", 20, true, true);
    push_fid(&mut bdmv_fids, "STREAM", 40, true, false);
    disc.put(PART_START + 20, build_dir_icb(21, bdmv_fids.len() as u32));
    disc.put_bytes(PART_START + 21, &bdmv_fids);

    let mut root_fids = Vec::new();
    push_fid(&mut root_fids, "", 10, true, true);
    push_fid(&mut root_fids, "BDMV", 20, true, false);
    disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
    disc.put_bytes(PART_START + 11, &root_fids);

    let out = TmpDir::new("multiextent_aacs");
    let d = aacs_disc();
    let (a, b) = (PART_START + DATA_A, PART_START + DATA_B);
    let set = crate::keys::KeyRing::keyed_for_test(
        &d,
        key,
        &[(a, a + SECTORS_EACH), (b, b + SECTORS_EACH)],
    );
    let opts = ExtractOptions {
        keys: Some(&set),
        ..Default::default()
    };
    let res = d
        .extract_tree(&mut disc, out.path(), &opts, &crate::ctx::Ctx::default())
        .expect("extract");

    let got = read_out(out.path(), "BDMV/STREAM/00001.m2ts").expect("file written");
    assert_eq!(
        got, expect,
        "both extents extract verbatim — the second extent is NOT a hole"
    );
    assert_eq!(
        res.bytes_unreadable, 0,
        "per-extent unit base must keep the second extent off the hole path"
    );
    assert!(
        res.complete,
        "a clean multi-extent AACS file extracts complete"
    );
}

// An ECMA-167 4/14.14.1.1 type-1 extent is ALLOCATED BUT NOT RECORDED and must be
// zero-filled, not read from media.
#[test]
fn extract_tree_zero_fills_an_unrecorded_extent_instead_of_reading_it() {
    const SECTORS_EACH: u32 = 1;
    const HOLE: u32 = 5000;
    const DATA: u32 = 5004;

    let hole_bytes = vec![0xEEu8; SECTOR_BYTES];
    let data_bytes = vec![0x5Au8; SECTOR_BYTES];
    // The file's byte space: the hole's zeros FIRST, then the real data.
    let mut expect = vec![0u8; SECTOR_BYTES];
    expect.extend_from_slice(&data_bytes);

    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);

    let mut root_fids = Vec::new();
    push_fid(&mut root_fids, "", 10, true, true);
    push_fid(&mut root_fids, "INDEX.BDMV", 42, false, false);
    disc.put(
        PART_START + 42,
        build_hole_then_data_icb(SECTORS_EACH, HOLE, DATA),
    );
    disc.put_bytes(PART_START + HOLE, &hole_bytes);
    disc.put_bytes(PART_START + DATA, &data_bytes);
    disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
    disc.put_bytes(PART_START + 11, &root_fids);

    let out = TmpDir::new("unrecorded_extent");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");

    let got = read_out(out.path(), "INDEX.BDMV").expect("file written");
    assert_eq!(
        got, expect,
        "an unrecorded extent contributes zeros to the file, never the \
             bytes that happen to sit on those sectors"
    );
    assert_eq!(
        res.bytes_unreadable, 0,
        "a hole is not a read failure — nothing was attempted"
    );
    assert!(res.complete);
}

// Focused alignment-computation check underpinning the per-extent fix
// (`aacs::content::is_unit_aligned`'s exact arithmetic).
#[test]
fn per_extent_base_is_aligned_first_extent_base_is_not() {
    use crate::aacs::content::is_unit_aligned;
    let ext_a_start = 7000u32; // first extent abs LBA
    let ext_b_start = 7004u32; // second extent abs LBA (Δ4 — not mult of 3)

    // Per-extent base: extent B's first read anchors on B's start → aligned.
    assert!(
        is_unit_aligned(ext_b_start, ext_b_start),
        "per-extent base keeps the extent's own first read aligned"
    );
    // Stale (first-extent) base: extent B's first read measured against A's
    // start is OFF the unit grid → the gate would (wrongly) reject it.
    assert!(
        !is_unit_aligned(ext_b_start, ext_a_start),
        "first-extent base mis-aligns a Δ-non-multiple-of-3 later extent"
    );
    // Sanity: a later extent whose Δ from the first IS a multiple of 3 would
    // have masked the bug — that's why the regression fixture uses Δ4.
    let ext_c_start = ext_a_start + 6; // Δ6 == 2 units
    assert!(
        is_unit_aligned(ext_c_start, ext_a_start),
        "a Δ-multiple-of-3 extent happens to stay aligned even on a stale base"
    );
}

/// An inline (ICB-embedded) file extracts from its embedded bytes.
#[test]
fn inline_file_extracts() {
    // Build an ICB whose flags select AD type 3 (embedded) with the data
    // stored inline after the ADs field.
    let payload = b"INLINE-NAV-DATA".to_vec();
    let mut disc = MemDisc::new();
    let mut root_fids = Vec::new();
    push_fid(&mut root_fids, "", 10, true, true);
    push_fid(&mut root_fids, "tiny.inf", 30, false, false);
    // Inline ICB (tag 266): flags low 3 bits = 3, l_ad = payload len, the
    // data living at offset 216.
    let mut icb = [0u8; 2048];
    icb[0..2].copy_from_slice(&266u16.to_le_bytes());
    icb[34..36].copy_from_slice(&3u16.to_le_bytes()); // embedded
    icb[56..64].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    icb[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
    icb[212..216].copy_from_slice(&(payload.len() as u32).to_le_bytes()); // l_ad
    icb[216..216 + payload.len()].copy_from_slice(&payload);
    disc.put(PART_START + 30, icb);
    disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
    disc.put_bytes(PART_START + 11, &root_fids);
    build_udf_skeleton(&mut disc, 10);

    let out = TmpDir::new("inline");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");
    assert_eq!(read_out(out.path(), "tiny.inf"), Some(payload));
    assert!(res.complete);
}

// The free-space pre-check must fire whenever the disc's declared total
// exceeds real available space; an exabyte size trips it regardless of
// actual free space on whatever machine runs the test.
#[test]
fn insufficient_space_errors_on_absurdly_large_required() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![file("huge.bin", 30, 31, Vec::new(), false)],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    // Overwrite the declared size (info_length) with an absurd value; the
    // extent length stays 0 so no real content is ever read (the space
    // gate runs before Phase 2 touches content).
    let mut icb = build_file_icb(0, 31, false);
    let huge: u64 = 1u64 << 60; // ~1 exabyte -- no real disk has this free
    icb[56..64].copy_from_slice(&huge.to_le_bytes());
    disc.put(PART_START + 30, icb);

    let out = TmpDir::new("insufficient_space");
    let err = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err("an absurdly large declared size must trip the space gate");
    assert!(matches!(err, Error::DirInsufficientSpace { .. }));
}

// Regression: the per-VTS key crack in `resolve_vts_key` must gather ONLY this VTS's own
// title-VOB extents, not a sibling VTS's.
#[test]
fn css_two_vts_groups_do_not_cross_contaminate_keys() {
    fn scrambled_vob(title_key: [u8; 5], marker: u8) -> (Vec<u8>, Vec<u8>) {
        let seed = [0x11u8, 0x22, 0x33, 0x44, marker];
        let mut plain = vec![0u8; 2048];
        // The crack scan's `is_scrambled_pack` gate needs the MPEG-PS pack-start
        // signature (real scrambled sector = MPEG-2 PS pack) before cracking; without it
        // `resolve_vts_key` falls back to `base_keys` for both groups, masking the regression.
        plain[0x00..0x04].copy_from_slice(&crate::css::PACK_START);
        plain[4] = 0x44; // '01': a 13818-1 pack
        plain[0x14] = 0x10; // scramble flag
        crate::css::dvd_pack_header(&mut plain, 0xE0);
        let pat: Vec<u8> = (0..8)
            .map(|k| (0xA0u8.wrapping_add(k as u8) ^ marker) ^ 0x5A)
            .collect();
        for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
            *b = pat[i % 8];
        }
        plain[0x54..0x59].copy_from_slice(&seed);
        let mut scrambled = plain.clone();
        lfsr::scramble_sector(&title_key, &mut scrambled);
        (plain, scrambled)
    }

    let key_1 = [0x10u8, 0x20, 0x30, 0x40, 0x50];
    let key_2 = [0x90u8, 0x80, 0x70, 0x60, 0x51];
    let (plain_1, scrambled_1) = scrambled_vob(key_1, 0x01);
    let (plain_2, scrambled_2) = scrambled_vob(key_2, 0x02);

    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![
                file("VTS_01_1.VOB", 30, 5000, scrambled_1.clone(), false),
                file("VTS_02_1.VOB", 32, 6000, scrambled_2.clone(), false),
            ],
            subdirs: vec![],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("css_two_vts");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    // Disc-wide key deliberately matches NEITHER VTS's real key, so a
    // broken filter falling back to it can never accidentally mask itself.
    d.css = Some(crate::css::CssState {
        title_key: [0xFFu8; 5],
        crack_span: None,
    });
    let res = d
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");

    let got_1 = read_out(out.path(), "VIDEO_TS/VTS_01_1.VOB").expect("vts01 vob");
    let got_2 = read_out(out.path(), "VIDEO_TS/VTS_02_1.VOB").expect("vts02 vob");
    let mut expect_1 = plain_1.clone();
    expect_1[0x14] = 0x80;
    let mut expect_2 = plain_2.clone();
    expect_2[0x14] = 0x80;
    assert_eq!(
        got_1, expect_1,
        "VTS_01 must descramble under its OWN cracked key, not VTS_02's"
    );
    assert_eq!(
        got_2, expect_2,
        "VTS_02 must descramble under its OWN cracked key, not VTS_01's"
    );
    assert!(res.complete);
}

// A VTS that IS scrambled but whose key could not be recovered must FAIL, not borrow
// another VTS's key (`CrackOutcome`, not `Option`).
#[test]
fn a_scrambled_vts_that_cannot_be_cracked_fails_instead_of_borrowing_a_key() {
    let key_1 = [0x10u8, 0x20, 0x30, 0x40, 0x50];
    let (_plain_1, scrambled_1) = {
        let seed = [0x11u8, 0x22, 0x33, 0x44, 0x01];
        let mut plain = vec![0u8; 2048];
        plain[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        plain[4] = 0x44; // '01': a 13818-1 pack
        plain[0x14] = 0x10;
        crate::css::dvd_pack_header(&mut plain, 0xE0);
        let pat: Vec<u8> = (0..8)
            .map(|k| (0xA0u8.wrapping_add(k as u8) ^ 0x01) ^ 0x5A)
            .collect();
        for (i, b) in plain.iter_mut().enumerate().skip(0x59) {
            *b = pat[i % 8];
        }
        plain[0x54..0x59].copy_from_slice(&seed);
        let mut scrambled = plain.clone();
        lfsr::scramble_sector(&key_1, &mut scrambled);
        (plain, scrambled)
    };
    // Scrambled — pack header and 0x14 flag set, so the scan SEES ciphertext —
    // but the cleartext region has no periodic run, so no key is recoverable.
    let uncrackable = {
        let mut sect = vec![0u8; 2048];
        sect[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
        sect[4] = 0x44; // '01': a 13818-1 pack
        sect[0x14] = 0x10;
        crate::css::dvd_pack_header(&mut sect, 0xE0);
        for (i, b) in sect.iter_mut().enumerate().skip(0x59) {
            // Non-repeating, so no run of any period survives to 0x80.
            *b = (i as u8).wrapping_mul(37).wrapping_add(11);
        }
        sect
    };

    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![
                file("VTS_01_1.VOB", 30, 5000, scrambled_1, false),
                file("VTS_02_1.VOB", 32, 6000, uncrackable, false),
            ],
            subdirs: vec![],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("css_uncrackable_vts");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    d.css = Some(crate::css::CssState {
        title_key: [0xFFu8; 5],
        crack_span: None,
    });
    let err = d
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err(
            "a VTS whose key could not be recovered must be a hard error, \
                 not a silent extract under another VTS's key",
        );
    assert!(
        matches!(err, Error::CssKeyMissing),
        "expected CssKeyMissing, got {err:?}"
    );
}

// The decrypting decorator owns a `Borrowed`; it must not hide what the drive could not map.
#[test]
fn borrowed_forwards_unmapped_stream_files() {
    struct Reports(Vec<crate::sector::bus_removal::UnmappedStreamFile>);
    impl SectorSource for Reports {
        fn read_sectors(&mut self, _: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            Ok(0)
        }
        fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
            &self.0
        }
    }
    fn paths<S: SectorSource>(s: &S) -> Vec<String> {
        s.unmapped_stream_files()
            .iter()
            .map(|u| u.path.clone())
            .collect()
    }
    let file = crate::sector::bus_removal::UnmappedStreamFile::new(
        "/BDMV/STREAM/00001.m2ts".into(),
        40,
        &Error::UdfAdChainTooLong,
    );
    let mut r = Reports(vec![file]);
    assert_eq!(paths(&Borrowed(&mut r)), ["/BDMV/STREAM/00001.m2ts"]);
}

// `Borrowed` must forward every `SectorSource` method to the wrapped
// `&mut dyn SectorSource` verbatim; calling on a concrete `Borrowed`
// value exercises the forwarding body via static, not vtable, dispatch.
#[test]
fn borrowed_forwards_every_sector_source_call() {
    struct Recorder {
        capacity: u32,
        last_speed: Option<u16>,
        last_unit_base: Option<u32>,
    }
    impl SectorSource for Recorder {
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            Ok(0)
        }
        fn set_speed(&mut self, kbs: u16) {
            self.last_speed = Some(kbs);
        }
        fn set_unit_base(&mut self, lba: u32) {
            self.last_unit_base = Some(lba);
        }
        fn random_access(&self) -> bool {
            false
        }
    }

    let mut inner = Recorder {
        capacity: 42,
        last_speed: None,
        last_unit_base: None,
    };
    {
        let mut b = Borrowed(&mut inner);
        assert_eq!(b.capacity_sectors(), 42, "capacity_sectors must forward");
        b.set_speed(7200);
        b.set_unit_base(1234);
        assert!(
            !b.random_access(),
            "random_access must forward, not default"
        );
    }
    assert_eq!(inner.last_speed, Some(7200), "set_speed must forward");
    assert_eq!(
        inner.last_unit_base,
        Some(1234),
        "set_unit_base must forward"
    );
}

// Regression: within ONE extent, "sectors remaining IN THIS EXTENT" must be `sectors -
// sector_off`, not `sectors + sector_off` — the latter lets a later batch read PAST the
// extent into unrelated content.
#[test]
fn extent_second_batch_stays_within_its_own_bounds() {
    // Extent A: 1600 sectors of pattern 'A' -- just over
    // READ_BATCH_SECTORS (1536), forcing a second while-loop iteration
    // with sector_off > 0.
    const SECTORS_A: u32 = 1600;
    const LBA_A: u32 = 5000;
    // The disc region immediately following extent A's true end. Must NEVER
    // be read as part of extent A: sized to cover a full erroneous second batch.
    const LBA_FILLER: u32 = LBA_A + SECTORS_A;
    const SECTORS_FILLER: u32 = 1472;
    // Extent B: the file's real second extent, at a completely different
    // LBA, same size as the filler region so the two are exact substitutes
    // if the arithmetic bug reads the wrong one.
    const SECTORS_B: u32 = SECTORS_FILLER;
    const LBA_B: u32 = 90_000;

    let a = vec![0xAAu8; SECTORS_A as usize * SECTOR_BYTES];
    let filler = vec![0xCCu8; SECTORS_FILLER as usize * SECTOR_BYTES];
    let b = vec![0xBBu8; SECTORS_B as usize * SECTOR_BYTES];

    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    disc.put_bytes(PART_START + LBA_A, &a);
    disc.put_bytes(PART_START + LBA_FILLER, &filler);
    disc.put_bytes(PART_START + LBA_B, &b);
    disc.put(
        PART_START + 30,
        build_two_extent_icb_sized(SECTORS_A, LBA_A, SECTORS_B, LBA_B),
    );
    let mut root_fids = Vec::new();
    push_fid(&mut root_fids, "", 10, true, true);
    push_fid(&mut root_fids, "big.bin", 30, false, false);
    disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
    disc.put_bytes(PART_START + 11, &root_fids);

    let out = TmpDir::new("extent_bounds");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");

    let got = read_out(out.path(), "big.bin").expect("file written");
    let mut expect = a.clone();
    expect.extend_from_slice(&b);
    assert_eq!(
        got.len(),
        expect.len(),
        "file size matches the two extents' declared total"
    );
    assert_eq!(
        got, expect,
        "extent A's tail batch must not read past its own declared length \
             into the following disc region (pattern 'C' must never appear)"
    );
    assert!(res.complete);
    assert_eq!(res.bytes_unreadable, 0);
}

// A batch that is NOT the extent's final chunk is capped at
// READ_BATCH_SECTORS, itself an exact multiple of AACS_UNIT_SECTORS
// (1536 = 512 * 3), so the result is already unit-aligned.
#[test]
fn whole_unit_batch_caps_mid_stream_batches() {
    assert_eq!(whole_unit_batch(2000), READ_BATCH_SECTORS);
    assert_eq!(whole_unit_batch(READ_BATCH_SECTORS + 1), READ_BATCH_SECTORS);
    assert_eq!(whole_unit_batch(READ_BATCH_SECTORS + 2), READ_BATCH_SECTORS);
}

// The FINAL chunk of an extent (`batch == remaining`) must NOT be rounded
// down to a unit boundary even when not a multiple of 3 — rounding it
// would silently drop sectors; `decrypt_sectors` handles the tail specially.
#[test]
fn whole_unit_batch_true_tail_is_never_rounded() {
    assert_eq!(whole_unit_batch(5), 5);
    assert_eq!(whole_unit_batch(1535), 1535);
    assert_eq!(whole_unit_batch(2), 2);
    assert_eq!(whole_unit_batch(1), 1);
    assert_eq!(whole_unit_batch(READ_BATCH_SECTORS), READ_BATCH_SECTORS);
}

// `read_batch` must retry a non-decrypt failure up to READ_RETRIES times
// and succeed if a later attempt does — not treat a transient failure as
// permanent on the first try.
#[test]
fn read_batch_retries_transient_failures_and_succeeds() {
    struct FlakySource {
        fail_first: u32,
        calls: u32,
    }
    impl SectorSource for FlakySource {
        fn capacity_sectors(&self) -> u32 {
            100_000
        }
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.calls += 1;
            if self.calls <= self.fail_first {
                return Err(Error::DiscRead {
                    sector: 0,
                    status: None,
                    sense: None,
                });
            }
            let need = count as usize * SECTOR_BYTES;
            buf[..need].fill(0x11);
            Ok(need)
        }
    }

    // Fails exactly READ_RETRIES times (attempts 0..READ_RETRIES all
    // error), then succeeds on the FINAL attempt (attempt ==
    // READ_RETRIES) -- the last chance the retry budget allows.
    let src = FlakySource {
        fail_first: READ_RETRIES,
        calls: 0,
    };
    let mut dec = DecryptingSectorSource::new(src, DecryptKeys::None);
    let mut buf = vec![0u8; 2 * SECTOR_BYTES];
    let ok = read_batch(&mut dec, 0, 2, &mut buf).unwrap();
    assert!(
        ok,
        "a failure that clears up within the retry budget must succeed, not hole"
    );
    assert_eq!(dec.inner().calls, READ_RETRIES + 1);
}

// A run whose events cancel its own halt at the first pass report (a UI's Stop).
fn stop_on_first_pass() -> crate::ctx::Ctx {
    let halt = crate::halt::Halt::new();
    let stop = halt.clone();
    crate::ctx::Ctx::new(halt).with_events(std::sync::Arc::new(
        move |e: &crate::event::Event<'_>| {
            if let crate::event::Event::Pass(p) = e {
                assert_eq!(p.kind, crate::progress::PassKind::Extract);
                stop.cancel();
            }
        },
    ))
}

/// A Stop raised from the progress events must halt the run mid-file, not be swallowed.
#[test]
fn stop_from_the_progress_events_halts_run_mid_file() {
    let good = vec![0x66u8; 4 * 2048];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![file("00001.m2ts", 24, 5000, good, true)],
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("progress_stop");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &stop_on_first_pass(),
        )
        .expect("extract does not error on a progress halt");

    assert!(res.halted, "a Stop at the first report must halt the run");
    assert!(
        !res.files[0].complete,
        "the in-flight file must be left incomplete, not finalized"
    );
    assert!(
        read_out(out.path(), "BDMV/STREAM/00001.m2ts").is_none(),
        "an incomplete file must not be renamed to its final name"
    );
}

// A MemDisc whose (host-key) bus map could not locate the File Entry at ICB `0`.
struct UnmappedAt(MemDisc, Vec<crate::sector::bus_removal::UnmappedStreamFile>);
impl SectorSource for UnmappedAt {
    fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        self.0.read_sectors(lba, n, buf, r)
    }
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        &self.1
    }
}

fn two_clip_disc_with_first_unmapped() -> UnmappedAt {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![
                    file("00001.m2ts", 24, 5000, vec![0x11; 3 * 2048], true),
                    file("00002.m2ts", 25, 6000, vec![0x22; 3 * 2048], true),
                ],
                subdirs: vec![],
            }],
        }],
    };
    let lost = crate::sector::bus_removal::UnmappedStreamFile::new(
        "/BDMV/STREAM/00001.m2ts".into(),
        24,
        &Error::UdfAdChainTooLong,
    );
    UnmappedAt(build_disc(root), vec![lost])
}

// The lost file is matched by ICB: it is never read, the other clip extracts intact.
#[test]
fn unmapped_stream_file_is_counted_lost_by_icb_and_never_written() {
    let mut disc = two_clip_disc_with_first_unmapped();
    let out = TmpDir::new("unmapped_icb");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");
    assert_eq!(
        read_out(out.path(), "BDMV/STREAM/00002.m2ts"),
        Some(vec![0x22; 3 * 2048])
    );
    assert!(read_out(out.path(), "BDMV/STREAM/00001.m2ts").is_none());
    assert!(read_out(out.path(), "BDMV/STREAM/00001.m2ts.partial").is_none());
    assert_eq!(res.bytes_unreadable, 3 * 2048);
    assert_eq!(res.bytes_good, 3 * 2048);
    assert!(!res.complete && !res.halted);
}

// A reader that serves one sector once (the tree listing reads each File Entry for its
// size), then fails it with `status`.
struct FailAt(MemDisc, u32, Option<u8>, u32);
impl SectorSource for FailAt {
    fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> Result<usize> {
        if lba == self.1 {
            self.3 += 1;
        }
        if lba == self.1 && self.3 > 1 {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: self.2,
                sense: None,
            });
        }
        self.0.read_sectors(lba, n, buf, r)
    }
}

fn two_clip_disc() -> MemDisc {
    two_clip_disc_with_first_unmapped().0
}

// One damaged File Entry loses that file only; the rest of the tree extracts.
#[test]
fn an_unreadable_file_entry_loses_that_file_not_the_tree() {
    let mut disc = FailAt(two_clip_disc(), PART_START + 24, Some(0x02), 0);
    let out = TmpDir::new("bad_fe");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("extract");
    assert_eq!(
        read_out(out.path(), "BDMV/STREAM/00002.m2ts"),
        Some(vec![0x22; 3 * 2048])
    );
    assert!(read_out(out.path(), "BDMV/STREAM/00001.m2ts").is_none());
    assert_eq!(res.bytes_unreadable, 3 * 2048);
    assert!(!res.complete && !res.halted);
}

// A dead bus while locating a file is not a damaged entry: the run fails.
#[test]
fn a_transport_failure_locating_a_file_entry_fails_the_run() {
    let status = Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE);
    let mut disc = FailAt(two_clip_disc(), PART_START + 24, status, 0);
    let out = TmpDir::new("dead_bus_fe");
    let r = clear_disc().extract_tree(
        &mut disc,
        out.path(),
        &ExtractOptions::default(),
        &crate::ctx::Ctx::default(),
    );
    assert!(r.is_err_and(|e| e.is_scsi_transport_failure()));
}

// A holed file feeds the run's loss counters and the live progress split: the
// unreadable bytes are not counted as good.
#[test]
fn a_hole_reaches_the_loss_stats_and_the_progress_split() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new((0u64, 0u64)));
    let tap = seen.clone();
    let ctx = crate::ctx::Ctx::new(crate::halt::Halt::new()).with_events(std::sync::Arc::new(
        move |e: &crate::event::Event<'_>| {
            if let crate::event::Event::Pass(p) = e {
                *tap.lock().unwrap() = (p.bytes_good_total, p.bytes_unreadable_total);
            }
        },
    ));
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![
                    file("00001.m2ts", 24, 5000, vec![0x11; 3 * 2048], true),
                    file("00002.m2ts", 25, 6000, vec![0x22; 6 * 2048], true),
                ],
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = build_disc(root);
    for i in 0..3u32 {
        disc.bad.insert(PART_START + 5000 + i); // the first clip's whole extent
    }
    let out = TmpDir::new("hole_stats");
    clear_disc()
        .extract_tree(&mut disc, out.path(), &ExtractOptions::default(), &ctx)
        .expect("extract");
    let loss = ctx.stats.snapshot();
    assert_eq!(loss.bytes_lost, 3 * 2048);
    assert!(loss.read_skips >= 1);
    assert_eq!(*seen.lock().unwrap(), (6 * 2048, 3 * 2048));
}

// A damaged AACS unit the reader blanks is unreadable bytes, not good ones.
#[test]
fn a_blanked_aacs_unit_is_counted_unreadable() {
    const DATA: u32 = 5000;
    let key = [0u8; 16];
    let mut content = encrypt_aacs_unit(&key, 0xA1);
    let mut damaged = vec![0x55u8; crate::aacs::content::ALIGNED_UNIT_LEN];
    damaged[0] |= 0xC0; // flagged encrypted, but no key opens it
    content.extend_from_slice(&damaged);
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![file("00001.m2ts", 24, DATA, content, true)],
                subdirs: vec![],
            }],
        }],
    };
    let mut disc = build_disc(root);
    let d = aacs_disc();
    let a = PART_START + DATA;
    let set = crate::keys::KeyRing::keyed_for_test(&d, key, &[(a, a + 6)]);
    let opts = ExtractOptions {
        keys: Some(&set),
        ..Default::default()
    };
    let out = TmpDir::new("blanked_unit");
    let res = d
        .extract_tree(&mut disc, out.path(), &opts, &crate::ctx::Ctx::default())
        .expect("extract");
    let unit = crate::aacs::content::ALIGNED_UNIT_LEN as u64;
    assert_eq!(res.bytes_unreadable, unit);
    assert_eq!(res.files[0].bytes_good, unit);
    assert!(!res.complete);
}

// The key set's loud stop is never a hole: the run ends with it.
#[test]
fn a_missing_disc_key_stops_the_run_instead_of_holing_the_unit() {
    struct KeyMissingAt(MemDisc, u32);
    impl SectorSource for KeyMissingAt {
        fn read_sectors(&mut self, lba: u32, n: u16, b: &mut [u8], r: bool) -> Result<usize> {
            if lba == self.1 {
                return Err(Error::WholeDiscKeyMissing);
            }
            self.0.read_sectors(lba, n, b, r)
        }
    }
    let mut disc = KeyMissingAt(two_clip_disc(), PART_START + 5000);
    let out = TmpDir::new("key_missing");
    let r = clear_disc().extract_tree(
        &mut disc,
        out.path(),
        &ExtractOptions::default(),
        &crate::ctx::Ctx::default(),
    );
    assert!(matches!(r, Err(Error::WholeDiscKeyMissing)), "{r:?}");
}

// A Stop at the lost file's report ends the run there, like any other file.
#[test]
fn progress_stop_on_an_unmapped_stream_file_halts_the_run() {
    let mut disc = two_clip_disc_with_first_unmapped();
    let out = TmpDir::new("unmapped_stop");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &stop_on_first_pass(),
        )
        .expect("a progress halt is not an error");
    assert!(res.halted);
    assert_eq!(res.files.len(), 1, "the run stops after the lost file");
    assert!(read_out(out.path(), "BDMV/STREAM/00002.m2ts").is_none());
}

// `available_space` must return `Some` for an existing dir on a platform
// that exposes it — the free-space pre-check silently no-ops on `None`,
// so a `statvfs` SUCCESS must never be read as "unavailable".
#[cfg(unix)]
#[test]
fn available_space_reports_free_bytes_on_a_real_dir() {
    let out = TmpDir::new("available_space");
    std::fs::create_dir_all(out.path()).unwrap();
    assert!(
        available_space(out.path()).is_some(),
        "a real, existing directory must report Some(_) free bytes"
    );
}

/// A raw control byte (below 0x20) in a disc-authored name must be replaced,
/// not passed through into the host filename.
#[test]
fn sanitize_rejects_control_bytes() {
    assert_eq!(sanitize_component("a\u{1}b"), "a_b", "0x01 replaced");
    assert_eq!(sanitize_component("a\nb"), "a_b", "0x0A replaced");
    assert_eq!(sanitize_component("a\u{1f}b"), "a_b", "0x1F replaced");
    // 0x20 (space) is NOT a control byte -- allowed mid-name (only
    // trimmed if trailing).
    assert_eq!(sanitize_component("a b"), "a b");
}

/// The VTS group number must be EXACTLY 2 ASCII digits -- neither a
/// non-numeric group nor a wrong-length one is a valid `VTS_xx` group.
#[test]
fn vts_group_of_requires_exactly_two_digits() {
    assert_eq!(
        vts_group_of("VTS_AB_1.VOB"),
        None,
        "letters are not a group number"
    );
    assert_eq!(
        vts_group_of("VTS_123_1.VOB"),
        None,
        "a 3-digit group is not a valid 2-digit VTS number"
    );
    assert_eq!(
        vts_group_of("VTS_1_1.VOB"),
        None,
        "a 1-digit group is not valid"
    );
    assert_eq!(vts_group_of("VTS_01_1.VOB").as_deref(), Some("VTS_01"));
}

/// The case-probe filename token must be UNIQUE across calls: a fixed
/// suffix let concurrent probes collide (TOCTOU). Every token from a burst
/// of calls must be distinct.
#[test]
fn probe_token_is_unique_across_calls() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..10_000 {
        let t = unique_probe_token();
        assert!(t.is_ascii(), "token must be ascii-hex, got {t:?}");
        assert!(seen.insert(t.clone()), "probe token repeated: {t}");
    }
}

/// Concurrency safety: many threads probing the SAME directory at once must
/// all agree on the answer and never panic — no probe's cleanup may clobber
/// another's marker (the collision a fixed name allowed). Also confirms no
/// stray `.fmkv_case_probe*` files survive the burst.
#[test]
fn concurrent_case_probes_do_not_collide() {
    let tmp = TmpDir::new("case_probe_race");
    std::fs::create_dir_all(tmp.path()).unwrap();
    let expected = oracle_case_insensitive(tmp.path());
    let dir = tmp.path().to_path_buf();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let dir = dir.clone();
            std::thread::spawn(move || {
                for _ in 0..200 {
                    assert_eq!(dir_is_case_insensitive(&dir), expected);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("probe thread panicked");
    }
    // Every probe removes its own marker, so nothing should be left behind.
    let leftover: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .to_ascii_lowercase()
                .starts_with(".fmkv_case_probe")
        })
        .collect();
    assert!(leftover.is_empty(), "probe markers leaked: {leftover:?}");
}

// Test-only oracle for the host volume's case folding, independent of
// `dir_is_case_insensitive`: `create_new` of the upper spelling (not an
// `exists()` lookup) collides only where the volume folds case.
fn oracle_case_insensitive(dir: &Path) -> bool {
    let lower = dir.join("fmkv_oracle_q");
    let upper = dir.join("FMKV_ORACLE_Q");
    std::fs::write(&lower, b"o").unwrap();
    let created = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&upper);
    let folded = match created {
        Ok(_) => {
            std::fs::remove_file(&upper).unwrap();
            false
        }
        Err(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists, "{e:?}");
            true
        }
    };
    std::fs::remove_file(&lower).unwrap();
    folded
}

// The probe must agree with the independent oracle on the real temp volume.
#[test]
fn case_probe_matches_independent_oracle() {
    let tmp = TmpDir::new("case_probe_oracle");
    std::fs::create_dir_all(tmp.path()).unwrap();
    assert_eq!(
        dir_is_case_insensitive(tmp.path()),
        oracle_case_insensitive(tmp.path())
    );
}

// The case fold itself, pinned for BOTH volume kinds regardless of the host.
#[test]
fn plan_tree_folds_case_only_when_told_the_volume_is_insensitive() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("Movie", 30, 31, b"a".to_vec(), false),
            file("movie", 32, 33, b"b".to_vec(), false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    let fs = udf::read_filesystem(&mut disc).unwrap();
    let plan = |disc: &mut MemDisc, ci: bool| {
        let (mut files, mut dirs) = (Vec::new(), Vec::new());
        let out = tempfile::tempdir().unwrap();
        let mut sink = TreeSink::create(out.path(), true)
            .unwrap()
            .with_case_insensitive(ci);
        plan_tree(
            disc,
            &fs,
            &fs.root,
            Path::new(""),
            "",
            true,
            &mut sink,
            &[],
            &mut files,
            &mut dirs,
        )
        .map(|()| files.len())
    };
    assert!(matches!(
        plan(&mut disc, true),
        Err(Error::DirNameCollision { .. })
    ));
    assert_eq!(plan(&mut disc, false).unwrap(), 2);
}

// A marker whose removal fails transiently (Windows AV / indexer holding it)
// must still be cleaned up, not left in the user's extraction target.
#[test]
fn case_probe_retries_a_failed_marker_removal() {
    let tmp = TmpDir::new("case_probe_remove_retry");
    std::fs::create_dir_all(tmp.path()).unwrap();
    let mut calls = 0u32;
    probe_case_insensitive(tmp.path(), |p| {
        calls += 1;
        if calls == 1 {
            return Err(std::io::Error::other("sharing violation"));
        }
        std::fs::remove_file(p)
    });
    let left = std::fs::read_dir(tmp.path()).unwrap().count();
    assert_eq!(left, 0, "probe marker left behind after a failed removal");
}

// A drive-level user Stop (`Halted`) mid-file is a halt, not a bad sector: the
// run must report halted and leave the file `.partial`, never finalize zeros.
#[test]
fn drive_halt_mid_read_halts_run_instead_of_finalizing_a_hole() {
    let good = vec![0x5Au8; 4 * 2048];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("a.m2ts", 24, 5000, good.clone(), false),
            file("b.m2ts", 26, 6000, good, false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    for i in 0..4u32 {
        disc.halted.insert(PART_START + 5000 + i);
    }
    let out = TmpDir::new("drive_halt");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("a Stop is a halt, not an error");
    assert!(res.halted, "drive-level Halted must halt the run");
    assert_eq!(res.files.len(), 1, "no file may start after the Stop");
    assert!(!res.files[0].complete);
    assert_eq!(res.bytes_unreadable, 0, "a Stop is not media loss");
    assert!(
        read_out(out.path(), "a.m2ts").is_none(),
        "must stay .partial"
    );
    assert!(read_out(out.path(), "a.m2ts.partial").is_some());
}

// A dead bus is not a hole: the file must stay `.partial` and the error surface,
// not be zero-filled and finalized as complete.
#[test]
fn a_transport_failure_stops_the_file_instead_of_zero_filling() {
    struct DeadBus;
    impl SectorSource for DeadBus {
        fn read_sectors(&mut self, lba: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            Err(Error::DiscRead {
                sector: lba as u64,
                status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
                sense: None,
            })
        }
    }
    let len = 4 * SECTOR_BYTES as u32;
    let pf = PlannedFile {
        host_rel: PathBuf::from("a.m2ts"),
        disc_name: "a.m2ts".into(),
        size: len as u64,
        inline: None,
        extents: vec![crate::udf::AbsExtent {
            lba: 100,
            len,
            recorded: true,
        }],
        unmapped: false,
    };
    let out = TmpDir::new("dead_bus");
    std::fs::create_dir_all(out.path()).unwrap();
    let mut dec = DecryptingSectorSource::new(DeadBus, DecryptKeys::None);
    let (mut done, mut bad) = (0u64, 0u64);
    let r = extract_one_file(
        &mut dec,
        &TreeSink::create(out.path(), true).unwrap(),
        &pf,
        len as u64,
        &mut done,
        &mut bad,
        &crate::ctx::Ctx::default(),
    );
    assert!(r.is_err_and(|e| e.is_scsi_transport_failure()));
    assert!(
        read_out(out.path(), "a.m2ts").is_none(),
        "must stay .partial"
    );
}

// A crafted extent near the top of the LBA space must not overflow: a batch
// whose START or END passes u32::MAX is a hole, never a panic or a wrapped read.
#[test]
fn extent_running_past_u32_max_holes_instead_of_wrapping() {
    struct AllBad(Vec<u32>);
    impl SectorSource for AllBad {
        fn read_sectors(&mut self, lba: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            self.0.push(lba);
            Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            })
        }
    }
    let start = u32::MAX - 100;
    let len = 1600 * SECTOR_BYTES as u32;
    let pf = PlannedFile {
        host_rel: PathBuf::from("big.bin"),
        disc_name: "big.bin".into(),
        size: len as u64,
        inline: None,
        extents: vec![crate::udf::AbsExtent {
            lba: start,
            len,
            recorded: true,
        }],
        unmapped: false,
    };
    let out = TmpDir::new("lba_overflow");
    std::fs::create_dir_all(out.path()).unwrap();
    let mut dec = DecryptingSectorSource::new(AllBad(Vec::new()), DecryptKeys::None);
    let (mut done, mut bad) = (0u64, 0u64);
    let (fr, halted) = extract_one_file(
        &mut dec,
        &TreeSink::create(out.path(), true).unwrap(),
        &pf,
        len as u64,
        &mut done,
        &mut bad,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    assert!(!halted);
    assert_eq!(fr.bytes_unreadable, len as u64);
    assert!(
        dec.inner().0.is_empty(),
        "no batch fits below u32::MAX, so nothing may be read: {:?}",
        dec.inner().0
    );
}

// A cancel landing during the per-VTS CSS crack must stop the run, not be
// ignored (the crack used to get `halt = None` and run to a verdict).
#[test]
fn halt_during_vts_crack_halts_the_run() {
    struct CancelOnRead<'a>(MemDisc, &'a crate::halt::Halt, u32);
    impl SectorSource for CancelOnRead<'_> {
        fn read_sectors(&mut self, lba: u32, c: u16, b: &mut [u8], r: bool) -> Result<usize> {
            if lba >= self.2 {
                self.1.cancel();
            }
            self.0.read_sectors(lba, c, b, r)
        }
    }
    // Uncrackable ciphertext over >1 crack batch (64), so the cancel lands mid-scan.
    let mut sect = vec![0u8; 2048];
    sect[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    sect[4] = 0x44; // '01': a 13818-1 pack
    sect[0x14] = 0x10;
    crate::css::dvd_pack_header(&mut sect, 0xE0);
    for (i, b) in sect.iter_mut().enumerate().skip(0x59) {
        *b = (i as u8).wrapping_mul(37).wrapping_add(11);
    }
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file("VTS_01_1.VOB", 30, 5000, sect.repeat(70), false)],
            subdirs: vec![],
        }],
    };
    let halt = crate::halt::Halt::new();
    let mut src = CancelOnRead(build_disc(root), &halt, PART_START + 5000);
    let out = TmpDir::new("css_crack_halt");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    d.css = Some(crate::css::CssState {
        title_key: [0xFFu8; 5],
        crack_span: None,
    });
    let res = d
        .extract_tree(
            &mut src,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::new(halt.clone()),
        )
        .expect("a Stop during the crack is a halt, not CssKeyMissing");
    assert!(res.halted);
    assert!(res.files.is_empty());
}

// A stop requested on an INLINE file's report must not be dropped.
#[test]
fn progress_stop_on_inline_file_halts_the_run() {
    let inline_icb = |payload: &[u8]| {
        let mut icb = [0u8; 2048];
        icb[0..2].copy_from_slice(&266u16.to_le_bytes());
        icb[34..36].copy_from_slice(&3u16.to_le_bytes());
        icb[56..64].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        icb[212..216].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        icb[216..216 + payload.len()].copy_from_slice(payload);
        icb
    };
    let mut disc = MemDisc::new();
    let mut root_fids = Vec::new();
    push_fid(&mut root_fids, "", 10, true, true);
    push_fid(&mut root_fids, "one.inf", 30, false, false);
    push_fid(&mut root_fids, "two.inf", 31, false, false);
    disc.put(PART_START + 30, inline_icb(b"ONE"));
    disc.put(PART_START + 31, inline_icb(b"TWO"));
    disc.put(PART_START + 10, build_dir_icb(11, root_fids.len() as u32));
    disc.put_bytes(PART_START + 11, &root_fids);
    build_udf_skeleton(&mut disc, 10);

    let out = TmpDir::new("inline_stop");
    let res = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &stop_on_first_pass(),
        )
        .expect("extract");
    assert!(res.halted, "the inline file's stop request was dropped");
    assert_eq!(res.files.len(), 1);
    assert!(
        res.files[0].complete,
        "the inline file itself was fully written"
    );
}

// A Stop landing on the DRIVE (not `opts.halt`) during the per-VTS crack must
// halt the run, not surface as CssKeyMissing or a cached key.
#[test]
fn drive_halt_during_vts_crack_is_a_halt_not_css_key_missing() {
    struct HaltFrom(MemDisc, u32);
    impl SectorSource for HaltFrom {
        fn read_sectors(&mut self, lba: u32, c: u16, b: &mut [u8], r: bool) -> Result<usize> {
            if lba >= self.1 {
                return Err(Error::Halted);
            }
            self.0.read_sectors(lba, c, b, r)
        }
    }
    let mut sect = vec![0u8; 2048];
    sect[0x00..0x04].copy_from_slice(&[0x00, 0x00, 0x01, 0xBA]);
    sect[4] = 0x44; // '01': a 13818-1 pack
    sect[0x14] = 0x10;
    crate::css::dvd_pack_header(&mut sect, 0xE0);
    for (i, b) in sect.iter_mut().enumerate().skip(0x59) {
        *b = (i as u8).wrapping_mul(37).wrapping_add(11);
    }
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file("VTS_01_1.VOB", 30, 5000, sect, false)],
            subdirs: vec![],
        }],
    };
    let mut src = HaltFrom(build_disc(root), PART_START + 5000);
    let out = TmpDir::new("css_crack_drive_halt");
    let mut d = clear_disc();
    d.content_format = crate::disc::ContentFormat::MpegPs;
    d.css = Some(crate::css::CssState {
        title_key: [0xFFu8; 5],
        crack_span: None,
    });
    let res = d
        .extract_tree(
            &mut src,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect("a drive Stop during the crack is a halt, not CssKeyMissing");
    assert!(res.halted);
    assert!(res.files.is_empty());
}

// A read that reports FEWER bytes than asked leaves a stale buffer tail: it
// must be retried and then holed, never written out as good data.
#[test]
fn short_read_is_not_counted_as_a_good_batch() {
    struct Short(u32);
    impl SectorSource for Short {
        fn read_sectors(&mut self, _: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
            self.0 += 1;
            let half = count as usize * SECTOR_BYTES / 2;
            buf[..half].fill(0x11);
            Ok(half)
        }
    }
    let mut dec = DecryptingSectorSource::new(Short(0), DecryptKeys::None);
    let mut buf = vec![0xEEu8; 2 * SECTOR_BYTES];
    let ok = read_batch(&mut dec, 0, 2, &mut buf).unwrap();
    assert!(
        !ok,
        "a short read must not be reported as a full good batch"
    );
    assert_eq!(dec.inner().0, READ_RETRIES + 1, "short reads are retried");
}

fn planned(size: u64, inline: Option<Vec<u8>>, extents: Vec<crate::udf::AbsExtent>) -> PlannedFile {
    PlannedFile {
        host_rel: PathBuf::from("f.bin"),
        disc_name: "f.bin".into(),
        size,
        inline,
        extents,
        unmapped: false,
    }
}

// One bad sector costs the rest of its ECC block (ends at 128), not the whole batch.
#[test]
fn bad_sector_holes_its_ecc_block_not_the_batch() {
    let mut m = MemDisc::new();
    for i in 0..9u32 {
        m.put(124 + i, [i as u8 + 1; 2048]);
    }
    m.bad.insert(125);
    let len = 9 * SECTOR_BYTES as u32;
    let pf = planned(
        len as u64,
        None,
        vec![crate::udf::AbsExtent {
            lba: 124,
            len,
            recorded: true,
        }],
    );
    let out = TmpDir::new("narrow");
    std::fs::create_dir_all(out.path()).unwrap();
    let mut dec = DecryptingSectorSource::new(m, DecryptKeys::None);
    let (mut done, mut bad) = (0u64, 0u64);
    let (fr, _) = extract_one_file(
        &mut dec,
        &TreeSink::create(out.path(), true).unwrap(),
        &pf,
        len as u64,
        &mut done,
        &mut bad,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    assert_eq!(fr.bytes_unreadable, 6 * SECTOR_BYTES as u64);
    assert_eq!(fr.bytes_good, 3 * SECTOR_BYTES as u64);
    assert_eq!(bad, 6 * SECTOR_BYTES as u64);
    let data = std::fs::read(out.path().join("f.bin")).unwrap();
    assert_eq!(data[0], 0, "bad unit zero-filled");
    assert_eq!(
        data[3 * SECTOR_BYTES],
        0,
        "unit straddling the block end unread"
    );
    assert_eq!(data[6 * SECTOR_BYTES], 7, "later unit kept");
}

// Counts reads; a read touching a `bad` LBA, or one of the first `fail_first`, fails.
struct CountingBad {
    bad: fn(u32) -> bool,
    fail_first: u32,
    calls: u32,
    fails: u32,
}
impl SectorSource for CountingBad {
    fn capacity_sectors(&self) -> u32 {
        100_000
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        self.calls += 1;
        if self.calls <= self.fail_first || (lba..lba + count as u32).any(self.bad) {
            self.fails += 1;
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        let need = count as usize * SECTOR_BYTES;
        buf[..need].fill(0x22);
        Ok(need)
    }
}

// Reads one READ_BATCH_SECTORS batch at LBA 1000: (lost, reads, failed reads, buf).
fn narrowed(bad: fn(u32) -> bool, fail_first: u32) -> (u64, u32, u32, Vec<u8>) {
    let src = CountingBad {
        bad,
        fail_first,
        calls: 0,
        fails: 0,
    };
    let mut dec = DecryptingSectorSource::new(src, DecryptKeys::None);
    let mut buf = vec![0u8; READ_BATCH_SECTORS as usize * SECTOR_BYTES];
    let lost = read_batch_narrowed(&mut dec, 1000, READ_BATCH_SECTORS, &mut buf).unwrap();
    (lost, dec.inner().calls, dec.inner().fails, buf)
}

// Byte range of batch-relative sectors [a, b).
fn sectors(a: u32, b: u32) -> std::ops::Range<usize> {
    a as usize * SECTOR_BYTES..b as usize * SECTOR_BYTES
}

// Contiguous damage must cost a handful of reads per batch, not one retry loop per unit.
#[test]
fn narrowed_all_bad_batch_is_read_boundedly() {
    let (lost, calls, _, buf) = narrowed(|l| (1000..2536).contains(&l), 0);
    assert_eq!(lost, buf.len() as u64);
    assert!(buf.iter().all(|&b| b == 0));
    // 2 batch reads + 4 bad units x 1 attempt, then the budget zero-fills the rest.
    assert_eq!(calls, 6, "{calls} reads for one bad batch");
}

// One bad ECC block mid-batch loses that block only; data after it is still read.
#[test]
fn narrowed_bad_ecc_block_loses_only_that_block() {
    let (lost, calls, _, buf) = narrowed(|l| (1600..1632).contains(&l), 0);
    // Units start at 1000 + 3k: 1630..1633 straddles the block end, so 1632 goes unread.
    assert_eq!(lost, 33 * SECTOR_BYTES as u64);
    assert!(buf[sectors(600, 633)].iter().all(|&b| b == 0));
    assert!(buf[sectors(0, 600)].iter().all(|&b| b == 0x22));
    assert!(buf[sectors(633, 1536)].iter().all(|&b| b == 0x22));
    // 2 batch reads + 200 good units + 1 bad unit + 301 good units.
    assert_eq!(calls, 2 + 200 + 1 + 301);
}

// A lone bad sector loses its ECC block (the drive fails the whole block anyway).
#[test]
fn narrowed_single_bad_sector_loses_its_ecc_block() {
    let (lost, _, _, buf) = narrowed(|l| l == 1700, 0);
    // Unit 1699..1702 fails; skip to the first unit at or after the block end 1728.
    assert_eq!(lost, 30 * SECTOR_BYTES as u64);
    assert!(buf[sectors(699, 729)].iter().all(|&b| b == 0));
    assert_eq!(buf.iter().filter(|&&b| b == 0).count(), 30 * SECTOR_BYTES);
}

// Scattered damage (every other unit bad) stops at the per-batch failed-read budget.
#[test]
fn narrowed_alternating_damage_is_capped() {
    let (lost, calls, fails, buf) = narrowed(|l| (l - 1000) / 3 % 2 == 1, 0);
    assert_eq!(fails, 2 + MAX_FAILED_UNIT_READS);
    // Good units 0, 8 and 30 were read; the rest is zero-filled.
    assert_eq!(calls, fails + 3);
    assert_eq!(lost, buf.len() as u64 - 9 * SECTOR_BYTES as u64);
    assert!(buf[sectors(24, 27)].iter().all(|&b| b == 0x22));
}

// Fails to decrypt (not a media error) for LBAs in `bad`.
struct DecryptBad(fn(u32) -> bool);
impl SectorSource for DecryptBad {
    fn capacity_sectors(&self) -> u32 {
        100_000
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        if (lba..lba + count as u32).any(self.0) {
            return Err(Error::DecryptFailed);
        }
        let need = count as usize * SECTOR_BYTES;
        buf[..need].fill(0x22);
        Ok(need)
    }
}

// An undecryptable unit loses only itself: no ECC-block skip past it.
#[test]
fn narrowed_undecryptable_unit_loses_only_that_unit() {
    let src = DecryptBad(|l| l == 1300);
    let mut dec = DecryptingSectorSource::new(src, DecryptKeys::None);
    let mut buf = vec![0u8; READ_BATCH_SECTORS as usize * SECTOR_BYTES];
    let lost = read_batch_narrowed(&mut dec, 1000, READ_BATCH_SECTORS, &mut buf).unwrap();
    assert_eq!(lost, 3 * SECTOR_BYTES as u64);
    assert!(buf[sectors(300, 303)].iter().all(|&b| b == 0));
    assert!(buf[sectors(303, 1536)].iter().all(|&b| b == 0x22));
}

// A transient whole-batch failure is recovered by one batch re-read, no unit reads.
#[test]
fn narrowed_transient_batch_failure_rereads_the_batch() {
    let (lost, calls, _, buf) = narrowed(|_| false, 1);
    assert_eq!(lost, 0);
    assert_eq!(calls, 2);
    assert!(buf.iter().all(|&b| b == 0x22));
}

// Extents covering less than the declared size: the gap is lost bytes, not a clean file.
#[test]
fn undercovered_extents_count_the_gap_as_unreadable() {
    let mut m = MemDisc::new();
    m.put(100, [5; 2048]);
    let len = SECTOR_BYTES as u32;
    let size = 3 * SECTOR_BYTES as u64;
    let pf = planned(
        size,
        None,
        vec![crate::udf::AbsExtent {
            lba: 100,
            len,
            recorded: true,
        }],
    );
    let out = TmpDir::new("undercover");
    std::fs::create_dir_all(out.path()).unwrap();
    let mut dec = DecryptingSectorSource::new(m, DecryptKeys::None);
    let (mut done, mut bad) = (0u64, 0u64);
    let (fr, _) = extract_one_file(
        &mut dec,
        &TreeSink::create(out.path(), true).unwrap(),
        &pf,
        size,
        &mut done,
        &mut bad,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    assert_eq!(fr.bytes_good, SECTOR_BYTES as u64);
    assert_eq!(fr.bytes_unreadable, 2 * SECTOR_BYTES as u64);
    assert_eq!(done, size);
    assert_eq!(bad, 2 * SECTOR_BYTES as u64);
}

// Inline data shorter than the declared size: same accounting.
#[test]
fn short_inline_data_counts_the_gap_as_unreadable() {
    let pf = planned(5000, Some(vec![7u8; 100]), Vec::new());
    let out = TmpDir::new("inline_short");
    std::fs::create_dir_all(out.path()).unwrap();
    let mut dec = DecryptingSectorSource::new(MemDisc::new(), DecryptKeys::None);
    let (mut done, mut bad) = (0u64, 0u64);
    let (fr, _) = extract_one_file(
        &mut dec,
        &TreeSink::create(out.path(), true).unwrap(),
        &pf,
        5000,
        &mut done,
        &mut bad,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    assert_eq!(fr.bytes_good, 100);
    assert_eq!(fr.bytes_unreadable, 4900);
}

// Reserved names with trailing spaces before the extension, and superscript digits.
#[test]
fn sanitize_substitutes_space_padded_and_superscript_reserved_names() {
    assert_eq!(sanitize_component("NUL .txt"), "_NUL .txt");
    assert_eq!(sanitize_component("COM\u{B9}"), "_COM\u{B9}");
    assert_eq!(sanitize_component("lpt\u{B3}.x"), "_lpt\u{B3}.x");
}

// Illegal characters are substituted, not reported as a collision that aborts the run.
#[test]
fn sanitize_substitutes_illegal_chars() {
    assert_eq!(sanitize_component("a:b"), "a_b");
    assert_eq!(sanitize_component("a\u{1}b"), "a_b");
    assert_eq!(sanitize_component(".."), "_");
    assert_eq!(sanitize_component(". "), "_");
}

// Two entries with the SAME name in one directory must collide, not overwrite.
#[test]
fn duplicate_identical_names_collide() {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: vec![
            file("movie", 30, 31, b"a".to_vec(), false),
            file("movie", 32, 33, b"b".to_vec(), false),
        ],
        subdirs: vec![],
    };
    let mut disc = build_disc(root);
    let out = TmpDir::new("dup_names");
    let err = clear_disc()
        .extract_tree(
            &mut disc,
            out.path(),
            &ExtractOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .expect_err("duplicate names must collide");
    assert!(matches!(err, Error::DirNameCollision { .. }));
}

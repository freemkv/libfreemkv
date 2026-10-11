use super::table::tests::{encode, entry, seg};
use super::*;
use crate::consts::{BD_SOURCE_PACKET_BYTES, SECTOR_BYTES};
use crate::disc::{ContentFormat, DiscFormat, DiscRegion, DiscTitle, Extent, ScanOptions};
use crate::event::Event;
use crate::sector::SectorSource;
use std::path::Path;
use std::sync::{Arc, Mutex};

// Cast Away (2000)'s `BDSVM/00000.svm` header.
const HEADER: [u8; 24] = [
    0x42, 0x44, 0x53, 0x56, 0x4d, 0x5f, 0x43, 0x43, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0xd7, 0x07,
    0x1f, 0x00, 0x00, 0x00, 0x00, 0x07, 0xfa, 0xc4,
];

/// A Blu-ray disc model with no titles, no AACS and no BD+.
pub(crate) fn bd_disc() -> Disc {
    Disc {
        volume_id: "BDPLUS".into(),
        meta_title: None,
        format: DiscFormat::BluRay,
        capacity_sectors: 0,
        capacity_bytes: 0,
        layers: 1,
        titles: Vec::new(),
        region: DiscRegion::Unknown,
        aacs: None,
        css: None,
        encrypted: false,
        aacs_error: None,
        css_error: None,
        content_format: ContentFormat::BdTs,
        bdplus: None,
    }
}

fn write(root: &Path, rel: &str, bytes: &[u8]) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, bytes).unwrap();
}

fn svm(len: usize) -> Vec<u8> {
    let mut b = HEADER.to_vec();
    b.resize(len, 0xEE);
    b
}

// A folder disc: two clips, and (when `bdplus`) a BD+ tree with a backup mirror.
fn folder(bdplus: bool) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "BDMV/STREAM/00001.m2ts", &vec![0x11; 6144 * 3]);
    write(root, "BDMV/STREAM/00002.m2ts", &vec![0x22; 6144]);
    if bdplus {
        write(root, "BDSVM/00000.svm", &svm(5000));
        write(root, "BDSVM/00001.svm", &svm(100));
        write(root, "BDSVM/BACKUP/00000.svm", &svm(5000));
    }
    dir
}

fn scan(dir: &Path) -> (Disc, crate::dirimage::DirImage) {
    let mut img = crate::dirimage::DirImage::open(dir).unwrap();
    let cap = img.capacity_sectors();
    let disc = Disc::scan_image(&mut img, cap, &ScanOptions::default()).unwrap();
    (disc, img)
}

#[test]
fn a_scan_records_the_bd_plus_facts() {
    let dir = folder(true);
    let (disc, mut img) = scan(dir.path());
    let info = disc.bdplus.as_ref().expect("BD+ detected");
    let h = info.header.unwrap();
    assert_eq!(h.raw, HEADER);
    assert_eq!(info.date(), Some((2007, 7, 31)));
    assert_eq!(info.estimated_generation(), Some(1));
    let files: Vec<(&str, u64)> = info
        .files
        .iter()
        .map(|f| (f.path.as_str(), f.size))
        .collect();
    assert_eq!(
        files,
        [
            ("BDSVM/00000.svm", 5000),
            ("BDSVM/00001.svm", 100),
            ("BDSVM/BACKUP/00000.svm", 5000)
        ]
    );
    assert!(info.has_backup());
    assert!(!info.has_extra_svm());
    // Each clip's extents cover its bytes, and read back as the file.
    let clips: Vec<(u32, u64)> = info
        .clips
        .iter()
        .map(|c| (c.clip, c.extents.iter().map(|e| e.1).sum()))
        .collect();
    assert_eq!(clips, [(1, 6144 * 3), (2, 6144)]);
    let (lba, _) = info.clips[1].extents[0];
    let mut buf = vec![0u8; SECTOR_BYTES];
    img.read_sectors(lba, 1, &mut buf, false).unwrap();
    assert!(buf.iter().all(|&b| b == 0x22));
}

#[test]
fn a_disc_without_bdsvm_00000_is_not_bd_plus() {
    let dir = folder(false);
    assert!(scan(dir.path()).0.bdplus.is_none());
    write(dir.path(), "BDSVM/00001.svm", &svm(100));
    assert!(scan(dir.path()).0.bdplus.is_none(), "presence is 00000.svm");
}

#[test]
fn later_content_code_files_are_noted() {
    let info = BdPlusInfo::new(
        None,
        vec![SvmFile {
            path: "BDSVM/00003.svm".into(),
            size: 1,
        }],
    );
    assert!(info.has_extra_svm());
    assert_eq!(info.estimated_generation(), None);
}

#[test]
fn the_share_bundle_carries_each_svm_header_only() {
    let dir = folder(true);
    let mut img = crate::dirimage::DirImage::open(dir.path()).unwrap();
    let files = Disc::read_structure_files(&mut img).unwrap();
    let svm: Vec<(&str, &[u8])> = files
        .iter()
        .filter(|(p, _)| p.starts_with("BDSVM"))
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect();
    assert_eq!(
        svm,
        [
            ("BDSVM/00000.svm", &HEADER[..]),
            ("BDSVM/00001.svm", &HEADER[..])
        ]
    );
}

// A source that answers from a closure.
struct Fn_<F>(&'static str, F);

impl<F> ConversionTableSource for Fn_<F>
where
    F: Fn(&BdPlusDiscId) -> Result<Option<ConversionTable>> + Send + Sync,
{
    fn name(&self) -> &str {
        self.0
    }
    fn lookup(&self, id: &BdPlusDiscId) -> Result<Option<ConversionTable>> {
        (self.1)(id)
    }
}

fn one_entry_table(clip: u32) -> ConversionTable {
    ConversionTable {
        clips: vec![ClipTable {
            clip,
            segments: vec![seg(vec![entry(0, 0x40, 0, 1, 20, 30, 1, 2)])],
        }],
    }
}

#[test]
fn resolution_asks_sources_in_order() {
    let dir = folder(true);
    let (disc, _) = scan(dir.path());
    let failing = Fn_("broken", |_: &BdPlusDiscId| {
        Err(Error::BdPlusTableInvalid { kind: "truncated" })
    });
    let found = Fn_("found", |_: &BdPlusDiscId| Ok(Some(one_entry_table(1))));
    let res = BdPlusResolution::resolve(&disc, &[&failing, &NoTables, &found]).unwrap();
    assert_eq!(
        res.status(),
        &BdPlusStatus::Resolved {
            source: "found".into(),
            clips: 1,
            entries: 1
        }
    );
    assert!(res.is_for(&disc));
    let none = BdPlusResolution::resolve(&disc, &[&NoTables]).unwrap();
    assert_eq!(none.status(), &BdPlusStatus::Unresolved);
    assert_eq!(none.patches_applied(), 0);
    assert!(
        BdPlusResolution::resolve(&bd_disc(), &[&found]).is_none(),
        "not BD+"
    );
}

#[test]
fn a_table_for_clips_the_disc_lacks_resolves_to_no_entries() {
    let dir = folder(true);
    let (disc, _) = scan(dir.path());
    let res = BdPlusResolution::from_table(&disc, "t", &one_entry_table(9)).unwrap();
    assert!(matches!(
        res.status(),
        BdPlusStatus::Resolved {
            clips: 0,
            entries: 0,
            ..
        }
    ));
}

#[test]
fn a_table_dir_resolves_by_the_scanned_disc_hash() {
    let tables = tempfile::tempdir().unwrap();
    let mut disc = bd_disc();
    disc.bdplus = Some(BdPlusInfo::new(None, Vec::new()));
    let uk = crate::test_util::unit_key_ro(crate::aacs::mkb::AacsVersion::V10, &[[1; 16]], &[1]);
    disc.aacs = Some(crate::test_util::aacs_state().uk_ro(uk).build());
    let hash = BdPlusDiscId::of(&disc).disc_hash;
    assert_eq!(hash.len(), 40);
    std::fs::write(
        tables.path().join(format!("{hash}.bin")),
        encode(&one_entry_table(1)),
    )
    .unwrap();
    let res = BdPlusResolution::resolve(&disc, &[&TableDir::new(tables.path())]).unwrap();
    assert!(matches!(res.status(), BdPlusStatus::Resolved { source, .. } if source == "dir"));
}

// ── End to end: a clip muxed from an image, with and without its table ──────────────

const P0: [u8; 5] = [0xB5, 0xD9, 0x3C, 0x7E, 0xA1];
const P1: [u8; 5] = [0x5A, 0xC3, 0x96, 0x2E, 0xF1];

struct E2e {
    iso: std::path::PathBuf,
    image: Vec<u8>,
    disc: Disc,
    table: ConversionTable,
    title: DiscTitle,
    clip_at: (u32, u64),
    _dir: tempfile::TempDir,
}

impl E2e {
    // The disc model again (`Disc` is not `Clone`).
    fn disc(&self) -> Disc {
        let mut disc = bd_disc();
        disc.capacity_sectors = (self.image.len() / SECTOR_BYTES) as u32;
        disc.capacity_bytes = self.image.len() as u64;
        disc.titles = vec![self.title.clone()];
        disc.bdplus = Some(BdPlusInfo {
            header: SvmHeader::parse(&HEADER),
            files: Vec::new(),
            clips: vec![ClipFile {
                clip: 1,
                extents: vec![self.clip_at],
            }],
        });
        disc
    }
}

// A clear BD+ image of one synthetic clip, and a table patching the payload of one of its
// video packets that continues a frame.
fn e2e() -> E2e {
    use crate::test_util::{BdFile, CLIP_VIDEO_PID, encrypted_bd_image, synthetic_bd_clip};
    let clip = synthetic_bd_clip(12);
    let img = encrypted_bd_image(
        &[BdFile::new("BDMV/STREAM/00001.m2ts", 0, None).with_clip(clip.clone())],
        &[],
    );
    let (start, sectors) = img.files[0];
    let packet = clip
        .chunks(BD_SOURCE_PACKET_BYTES)
        .enumerate()
        .skip(40)
        .find(|(_, p)| {
            let pid = u16::from_be_bytes([p[5] & 0x1F, p[6]]);
            pid == CLIP_VIDEO_PID && p[5] & 0x40 == 0 && p[7] & 0x30 == 0x10
        })
        .map(|(i, _)| i as u32)
        .expect("a continuation video packet");
    let mut e = entry(packet, 0x40, 0, 0, 100, 150, 0, 0);
    e.patch0 = P0;
    e.patch1 = P1;
    let table = ConversionTable {
        clips: vec![ClipTable {
            clip: 1,
            segments: vec![seg(vec![e])],
        }],
    };
    let title = DiscTitle {
        selection_evidence: Default::default(),
        playlist: "00001.mpls".into(),
        size_bytes: u64::from(sectors) * SECTOR_BYTES as u64,
        extents: vec![Extent {
            start_lba: start,
            sector_count: sectors,
        }],
        streams: crate::mux::ts::scan_streams(&clip).unwrap(),
        ..DiscTitle::empty()
    };
    let dir = tempfile::tempdir().unwrap();
    let iso = dir.path().join("disc.iso");
    std::fs::write(&iso, &img.image).unwrap();
    let mut fx = E2e {
        iso,
        image: img.image,
        disc: bd_disc(),
        table,
        title,
        clip_at: (start, clip.len() as u64),
        _dir: dir,
    };
    fx.disc = fx.disc();
    fx
}

#[derive(Debug, PartialEq)]
enum Seen {
    Unresolved(Option<u8>),
    Resolved(String, usize, usize),
}

// Mux title 0 of `fx` to an MKV, attaching `res`; the file and the BD+ events heard.
fn mux(fx: &E2e, res: Option<BdPlusResolution>, raw: bool) -> (Vec<u8>, Vec<Seen>) {
    mux_from(fx, res, raw, false)
}

// `mux`, from a live reader over the image when `live`.
fn mux_from(
    fx: &E2e,
    res: Option<BdPlusResolution>,
    raw: bool,
    live: bool,
) -> (Vec<u8>, Vec<Seen>) {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let ctx = crate::ctx::Ctx::default().with_events(Arc::new(move |e: &Event<'_>| match e {
        Event::BdPlusUnresolved { info } => log
            .lock()
            .unwrap()
            .push(Seen::Unresolved(info.estimated_generation())),
        Event::BdPlusResolved {
            source,
            clips,
            entries,
        } => log
            .lock()
            .unwrap()
            .push(Seen::Resolved(source.to_string(), *clips, *entries)),
        _ => {}
    }));
    let out = tempfile::tempdir().unwrap();
    let dest = out.path().join("t.mkv");
    let title =
        crate::mux::source::ScannedTitle::new(fx.disc.titles[0].clone(), ContentFormat::BdTs);
    let mut source = match live {
        true => {
            let reader = crate::test_util::MemSource::new(fx.image.clone());
            crate::mux::source::Source::from_reader(Box::new(reader), title)
        }
        false => crate::mux::source::Source::from_image(&fx.iso, title),
    };
    if let Some(r) = res {
        source = source.with_bdplus(r);
    }
    let opts = crate::mux::driver::MuxOptions {
        batch_sectors: 30,
        raw,
        ..Default::default()
    };
    let dest_url = format!("mkv://{}", dest.display());
    let r = crate::mux::driver::mux_with_keys(source, None, &dest_url, &opts, &ctx).unwrap();
    assert!(r.completed, "{r:?}");
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    (std::fs::read(&dest).unwrap(), seen)
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn a_resolved_title_is_patched_and_its_image_is_not() {
    let fx = e2e();
    let res = BdPlusResolution::from_table(&fx.disc, "test", &fx.table).unwrap();
    let (base, quiet) = mux(&fx, None, false);
    assert!(
        quiet.is_empty(),
        "no layout, no resolution: nothing to report"
    );
    assert!(!contains(&base, &P0) && !contains(&base, &P1));

    let (patched, seen) = mux(&fx, Some(res.clone()), false);
    assert_eq!(seen, [Seen::Resolved("test".into(), 1, 1)]);
    assert!(
        contains(&patched, &P0) && contains(&patched, &P1),
        "both patches in the title"
    );
    assert_eq!(res.patches_applied(), 2);
    assert_eq!(patched.len(), base.len(), "patches overwrite, never resize");

    // The whole-disc image is a faithful copy: the image writer never patches.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("copy.iso");
    let mut raw = crate::whole_disc::raw_whole_disc_reader(crate::test_util::MemSource::new(
        fx.image.clone(),
    ));
    let cap = fx.disc.capacity_sectors;
    crate::io::image_writer::write_image(&mut raw, &dest, cap, &crate::ctx::Ctx::default())
        .unwrap();
    let copy = std::fs::read(&dest).unwrap();
    assert_eq!(copy, fx.image);
    assert!(!contains(&copy, &P0));
}

#[test]
fn a_live_reader_title_is_patched_too() {
    let fx = e2e();
    let res = BdPlusResolution::from_table(&fx.disc, "test", &fx.table).unwrap();
    let (iso, _) = mux_from(&fx, Some(res.clone()), false, false);
    let (live, seen) = mux_from(&fx, Some(res), false, true);
    assert_eq!(seen, [Seen::Resolved("test".into(), 1, 1)]);
    assert_eq!(live, iso);
}

#[test]
fn an_unresolved_title_passes_through_and_says_so() {
    let fx = e2e();
    let (base, _) = mux(&fx, None, false);
    let unresolved = BdPlusResolution::resolve(&fx.disc, &[&NoTables]).unwrap();
    let (out, seen) = mux(&fx, Some(unresolved), false);
    assert_eq!(
        out, base,
        "byte for byte what a mux without BD+ handling writes"
    );
    assert_eq!(seen, [Seen::Unresolved(Some(1))]);
}

#[test]
fn a_raw_mux_is_never_patched_or_reported() {
    let fx = e2e();
    let res = BdPlusResolution::from_table(&fx.disc, "test", &fx.table).unwrap();
    let (base, _) = mux(&fx, None, true);
    let (out, seen) = mux(&fx, Some(res.clone()), true);
    assert_eq!(out, base);
    assert!(seen.is_empty());
    assert_eq!(res.patches_applied(), 0);
}

#[test]
fn an_image_source_whose_scan_found_bd_plus_reports_it_unresolved() {
    let fx = e2e();
    let ctx_seen = Arc::new(Mutex::new(0));
    let log = ctx_seen.clone();
    let ctx = crate::ctx::Ctx::default().with_events(Arc::new(move |e: &Event<'_>| {
        if matches!(e, Event::BdPlusUnresolved { .. }) {
            *log.lock().unwrap() += 1;
        }
    }));
    let reader: Box<dyn SectorSource> =
        Box::new(crate::test_util::MemSource::new(fx.image.clone()));
    let source = crate::mux::source::Source::of(crate::mux::source::Origin::Image {
        path: fx.iso.clone(),
        folder: false,
        reader,
        disc: Box::new(fx.disc()),
    });
    let opts = crate::mux::driver::MuxOptions::default();
    let fixups = crate::mux::driver::bdplus_fixups(&source, &opts, &ctx);
    assert!(fixups.is_none());
    assert_eq!(*ctx_seen.lock().unwrap(), 1);
    // A resolution for another disc is not applied to this one.
    let mut other = fx.disc();
    other.aacs = Some(
        crate::test_util::aacs_state()
            .disc_hash("ab".repeat(20))
            .build(),
    );
    let foreign = BdPlusResolution::from_table(&other, "test", &fx.table).unwrap();
    let source = source.with_bdplus(foreign);
    assert!(crate::mux::driver::bdplus_fixups(&source, &opts, &ctx).is_none());
    assert_eq!(*ctx_seen.lock().unwrap(), 2);
}

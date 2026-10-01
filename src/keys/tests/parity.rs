//! Parity goldens for the image and live-reader paths over synthetic muxable AACS discs:
//! the whole-disc ISO copy, the decrypted folder, the title reader and every mux input
//! (`Iso`, `Live`) × sink, with loss counters and key-gate verdicts. Cells are `parity_*`;
//! see [`Golden`](crate::test_util::Golden) for the re-bless command.

use super::*;
use crate::mux::driver::{MuxOptions, MuxSource, mux_with_keys};
use crate::mux::parity_tests::{golden, record_run, record_tree};
use crate::mux::select::{PidFilter, StreamSelection};
use crate::test_util::{CLIP_AUDIO_PIDS, Golden, synthetic_bd_clip};

fn refused<T>(g: &mut Golden, key: &str, r: &Result<T>) -> bool {
    match r {
        Ok(_) => false,
        Err(e) => {
            g.kv(key, format_args!("refused E{}", e.code()));
            true
        }
    }
}

// A disc whose title 0 plays one clip file per entry of `keys` (`None`: a clear file),
// declaring that many CPS units, with the clip's streams on the title.
fn clip_fx(keys: &[Option<[u8; 16]>]) -> Fx {
    let clip = synthetic_bd_clip(6);
    let files: Vec<BdFile> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            BdFile::new(format!("BDMV/STREAM/{:05}.m2ts", i + 1), 0, *k).with_clip(clip.clone())
        })
        .collect();
    let all: Vec<usize> = (0..keys.len()).collect();
    let mut fx = fixture(&files, keys.len(), &[&all]);
    fx.disc.titles[0].streams = crate::mux::ts::scan_streams(&clip).unwrap();
    fx
}

fn write_iso(fx: &Fx, dir: &std::path::Path) -> std::path::PathBuf {
    let p = dir.join("disc.iso");
    std::fs::write(&p, &fx.img.image).unwrap();
    p
}

// One whole-disc ISO copy through `reader`: the image bytes (or the failure) and the
// reader's blanked-unit count.
fn copy_image(g: &mut Golden, tag: &str, fx: &Fx, reader: &mut dyn SectorSource) {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.iso");
    let cap = fx.disc.capacity_sectors;
    let r = crate::io::image_writer::write_image(reader, &dest, cap, &crate::ctx::Ctx::default());
    match r {
        Ok(n) => {
            g.kv(&format!("{tag} written"), n);
            g.bytes(&format!("{tag} image"), &std::fs::read(&dest).unwrap());
        }
        Err(e) => {
            g.kv(&format!("{tag} failed"), format_args!("E{}", e.code()));
        }
    }
}

fn extract(g: &mut Golden, tag: &str, fx: &Fx, keys: Option<&ResolvedKeySet>, src: Faulty) {
    let dest = tempfile::tempdir().unwrap();
    let opts = crate::disc::ExtractOptions {
        keys,
        ..Default::default()
    };
    let mut src = src;
    match fx
        .disc
        .extract_tree(&mut src, dest.path(), &opts, &crate::ctx::Ctx::default())
    {
        Ok(r) => {
            g.kv(
                &format!("{tag} result"),
                format_args!(
                    "good={} unreadable={} complete={} halted={} files={:?}",
                    r.bytes_good,
                    r.bytes_unreadable,
                    r.complete,
                    r.halted,
                    r.files
                        .iter()
                        .map(|f| (
                            f.path.display().to_string(),
                            f.bytes_good,
                            f.bytes_unreadable,
                            f.complete
                        ))
                        .collect::<Vec<_>>()
                ),
            );
            record_tree(g, tag, dest.path());
        }
        Err(e) => {
            g.kv(&format!("{tag} failed"), format_args!("E{}", e.code()));
        }
    }
}

fn mux_cells(g: &mut Golden, fx: &Fx, set: Option<&ResolvedKeySet>, dead: Option<(u32, u32)>) {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let iso = write_iso(fx, dir.path());
    let title = fx.disc.titles[0].clone();
    let format = fx.disc.content_format;
    let base = MuxOptions {
        batch_sectors: 30,
        ..Default::default()
    };
    let selected = MuxOptions {
        batch_sectors: 30,
        selection: StreamSelection {
            audio: PidFilter::Only(vec![CLIP_AUDIO_PIDS[1]]),
            subtitle: PidFilter::Only(vec![]),
        },
        ..Default::default()
    };
    let raw = MuxOptions {
        raw: true,
        batch_sectors: 30,
        ..Default::default()
    };
    let skip = MuxOptions {
        skip_errors: true,
        batch_sectors: 30,
        ..Default::default()
    };
    let run = |g: &mut Golden, tag: &str, sink: &str, live: bool, opts: &MuxOptions| {
        let out = dir.path().join(format!("out-{tag}"));
        std::fs::create_dir_all(&out).unwrap();
        let dest = match sink {
            "null" => "null://".to_string(),
            s => format!("{s}://{}", out.join(format!("o.{s}")).display()),
        };
        let source = if live {
            let src = fx.source();
            if let Some((a, b)) = dead {
                src.kill(a, b);
            }
            MuxSource::Live {
                reader: Box::new(src),
                title: title.clone(),
                format,
            }
        } else {
            MuxSource::Iso {
                path: &iso,
                title: title.clone(),
                format,
            }
        };
        let r = mux_with_keys(source, set, &dest, opts, &crate::ctx::Ctx::default());
        record_run(g, tag, &r);
        record_tree(g, tag, &out);
    };
    for sink in ["mkv", "m2ts", "null"] {
        run(g, &format!("iso-{sink}"), sink, false, &base);
    }
    run(g, "iso-mkv-raw", "mkv", false, &raw);
    run(g, "iso-mkv-selection", "mkv", false, &selected);
    run(g, "live-mkv", "mkv", true, &base);
    run(g, "live-mkv-raw", "mkv", true, &raw);
    if dead.is_some() {
        run(g, "live-mkv-skip", "mkv", true, &skip);
    }
}

// The full matrix for one disc: the whole-disc copy (decrypting and raw), the decrypted
// folder and the raw folder, the title reader, then every mux input and sink.
fn image_cell(name: &str, fx: &Fx, pool: &[[u8; 16]], dead: Option<(u32, u32)>) {
    let mut g = golden(name);
    let src = || {
        let s = fx.source();
        if let Some((a, b)) = dead {
            s.kill(a, b);
        }
        s
    };
    let calls = Calls::default();
    let specs = [Spec::keydb(pool, &calls)];
    let whole = resolve_with(
        fx,
        &mut fx.source(),
        KeyScope::WholeDisc,
        &specs,
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    );
    if !refused(&mut g, "resolve-whole", &whole) {
        let set = whole.as_ref().unwrap();
        g.kv("resolve-whole status", format_args!("{:?}", set.status()));
        match set.whole_disc_reader(&fx.disc, src(), None) {
            Ok(mut w) => {
                copy_image(&mut g, "copy", fx, &mut w);
                g.kv("copy blanked", w.blanked_units());
            }
            Err(e) => {
                g.kv("copy", format_args!("refused E{}", e.code()));
            }
        }
        extract(&mut g, "extract", fx, Some(set), src());
    }
    let mut raw = crate::whole_disc::raw_whole_disc_reader(src());
    copy_image(&mut g, "copy-raw", fx, &mut raw);
    extract(&mut g, "extract-raw", fx, None, src());
    let titles = resolve_with(
        fx,
        &mut fx.source(),
        KeyScope::Titles(vec![0]),
        &specs,
        ResolveKeysOptions::default(),
        &FakeClock::default(),
    );
    if !refused(&mut g, "resolve-title", &titles) {
        let set = titles.as_ref().unwrap();
        g.kv("resolve-title status", format_args!("{:?}", set.status()));
        match set.title_reader(&fx.disc, 0, src()) {
            Ok(mut r) => {
                let mut all = Vec::new();
                let mut failed = None;
                for e in &fx.disc.titles[0].extents {
                    let mut at = e.start_lba;
                    let end = e.start_lba + e.sector_count;
                    r.set_unit_base(e.start_lba);
                    while at < end {
                        let n = (end - at).min(30);
                        let mut buf = vec![0u8; n as usize * 2048];
                        match r.read_sectors(at, n as u16, &mut buf, true) {
                            Ok(_) => all.extend_from_slice(&buf),
                            Err(e) => {
                                failed = Some(e.code());
                                break;
                            }
                        }
                        at += n;
                    }
                }
                g.bytes("title-read", &masked(&all));
                g.kv("title-read blanked", r.blanked_units());
                if let Some(c) = failed {
                    g.kv("title-read failed", format_args!("E{c}"));
                }
            }
            Err(e) => {
                g.kv("title-read", format_args!("refused E{}", e.code()));
            }
        }
        mux_cells(&mut g, fx, Some(set), dead);
    } else {
        // No key set: the mux still runs keyless and refuses on the first AACS unit.
        mux_cells(&mut g, fx, None, dead);
    }
    g.check();
}

#[test]
fn parity_image_bd_one_cps() {
    image_cell(
        "parity_image_bd_one_cps",
        &clip_fx(&[Some(K1)]),
        &[K1],
        None,
    );
}

#[test]
fn parity_image_bd_multi_cps() {
    let fx = clip_fx(&[Some(K1), Some(K2)]);
    image_cell("parity_image_bd_multi_cps", &fx, &[K1, K2], None);
}

#[test]
fn parity_image_bd_clear_files() {
    image_cell(
        "parity_image_bd_clear_files",
        &clip_fx(&[None, None]),
        &[K1],
        None,
    );
}

// A held key set that opens only one of the two files refuses before any output.
#[test]
fn parity_image_bd_missing_key() {
    let fx = clip_fx(&[Some(K1), Some(K2)]);
    image_cell("parity_image_bd_missing_key", &fx, &[K1], None);
}

// Seed-damaged units inside a keyed file: blanked and counted on every path (E7013 option A).
#[test]
fn parity_image_bd_damaged_units() {
    let mut fx = clip_fx(&[Some(K1), Some(K2)]);
    for u in [1, 2, 4] {
        damage_seed(&mut fx, 1, u);
    }
    image_cell("parity_image_bd_damaged_units", &fx, &[K1, K2], None);
}

// A read-failure map: a sector range that fails to read, over a keyed file.
#[test]
fn parity_image_bd_bad_sectors() {
    let fx = clip_fx(&[Some(K1)]);
    let (b, _) = fx.file(0);
    image_cell(
        "parity_image_bd_bad_sectors",
        &fx,
        &[K1],
        Some((b + 9, b + 12)),
    );
}

// A UHD disc whose AACS state says bus encryption: an image carries it removed or not at all.
#[test]
fn parity_image_uhd_bus_flag() {
    let mut fx = clip_fx(&[Some(K1)]);
    let a = fx.disc.aacs.take().unwrap();
    fx.disc.aacs = Some(
        aacs_state()
            .disc_hash(HASH)
            .volume_id(VID)
            .uk_ro(a.uk_ro.clone())
            .bus_encryption(true)
            .build(),
    );
    image_cell("parity_image_uhd_bus_flag", &fx, &[K1], None);
}

// The FMTS alternate-phase disc: the forensic clip's even units open under the held phase,
// the alternate phase stays ciphertext (read through the title reader, never muxed).
#[test]
fn parity_image_fmts_alternate_phase() {
    let mut g = golden("parity_image_fmts_alternate_phase");
    for held_phase in [0usize, 1] {
        let mut fx = fmts_fixture();
        if held_phase == 1 {
            for (a, b, key) in [(0, 16, F1), (20, 36, F2)] {
                for u in a..b {
                    fx.reencrypt(1, u, if u % 2 == 1 { &key } else { &ALT });
                }
            }
        }
        let calls = Calls::default();
        let mut source = fmts_online(&calls);
        source.keys = vec![K1, K2];
        source.match_fmts_phase = true;
        let set = resolve(&fx, KeyScope::Titles(vec![2]), &[source]).unwrap();
        g.kv(
            &format!("phase{held_phase} status"),
            format_args!("{:?}", set.status()),
        );
        let mut r = set.title_reader(&fx.disc, 2, fx.source()).unwrap();
        let got = read(&mut r, &fx, 1, 0, 16).unwrap();
        g.bytes(&format!("phase{held_phase} forensic-clip"), &got);
        let mut w = set.whole_disc_reader(&fx.disc, fx.source(), None);
        match &mut w {
            Ok(w) => copy_image(&mut g, &format!("phase{held_phase} copy"), &fx, w),
            Err(e) => {
                g.kv(
                    &format!("phase{held_phase} copy"),
                    format_args!("refused E{}", e.code()),
                );
            }
        }
    }
    g.check();
}

// An HD DVD whose single content file is clear: the whole-disc copy and folder are byte
// copies of the image.
#[test]
fn parity_image_hddvd_clear() {
    let files = [
        BdFile::new("HVDVD_TS/FEATURE.EVO", 30, None),
        BdFile::new("BDMV/index.bdmv", 1, None),
    ];
    let uk_ro = unit_key_ro(AacsVersion::V10, &[[0xEE; 16]], &[1]);
    let img = encrypted_bd_image(&files, &uk_ro);
    let disc = disc_over(&img, &uk_ro, &[&[0]], DiscFormat::HdDvd);
    let fx = Fx { img, disc };
    let mut g = golden("parity_image_hddvd_clear");
    let calls = Calls::default();
    let set = resolve(&fx, KeyScope::WholeDisc, &[Spec::keydb(&[K1], &calls)]);
    if !refused(&mut g, "resolve-whole", &set) {
        let set = set.unwrap();
        g.kv("resolve-whole status", format_args!("{:?}", set.status()));
        let mut w = set.whole_disc_reader(&fx.disc, fx.source(), None).unwrap();
        copy_image(&mut g, "copy", &fx, &mut w);
        extract(&mut g, "extract", &fx, Some(&set), fx.source());
    }
    g.check();
}

// A DVD of two title sets, each VOB scrambled under its own title key: the folder re-cracks
// per VTS from the disc-wide key's start.
fn dvd_fx(live: bool) -> (Fx, Vec<u8>, Vec<u8>) {
    use crate::mux::parity_tests::{clear_mpg, scramble_vob};
    let pad = |mut v: Vec<u8>| {
        v.resize(v.len().div_ceil(ALIGNED_UNIT_LEN) * ALIGNED_UNIT_LEN, 0);
        v
    };
    let clear = pad(clear_mpg());
    let (k1, k2) = (
        [0x42, 0x13, 0x37, 0xBE, 0xEF],
        [0x17, 0x71, 0x29, 0x93, 0x05],
    );
    let files = [
        BdFile::new("VIDEO_TS/VTS_01_1.VOB", 0, None).with_clip(scramble_vob(&clear, &k1)),
        BdFile::new("VIDEO_TS/VTS_02_1.VOB", 0, None).with_clip(scramble_vob(&clear, &k2)),
    ];
    let uk_ro = unit_key_ro(AacsVersion::V10, &[[0xEE; 16]], &[1]);
    let img = encrypted_bd_image(&files, &uk_ro);
    let mut disc = disc_over(&img, &uk_ro, &[&[0, 1]], DiscFormat::Dvd);
    disc.aacs = None;
    disc.encrypted = true;
    disc.content_format = ContentFormat::MpegPs;
    // A live scan leaves `css` unset; a scanned image carries the first VTS's cracked key.
    disc.css = (!live).then_some(crate::css::CssState {
        title_key: k1,
        crack_span: None,
    });
    (Fx { img, disc }, clear, k1.to_vec())
}

#[test]
fn parity_image_dvd_css_multi_vts() {
    let (fx, _, _) = dvd_fx(false);
    let mut g = golden("parity_image_dvd_css_multi_vts");
    extract(&mut g, "extract", &fx, None, fx.source());
    g.check();
}

// BUG-1 (pipeline design §1.4): a live-scanned DVD has no `disc.css`, so the folder must
// still come out descrambled. Red today (the VOBs are written scrambled); ignored until the
// decrypt stage detects CSS by content (slice 3b).
#[test]
#[ignore = "BUG-1: live DVD -> dir never engages CSS (fixed in slice 3b)"]
fn parity_bug1_live_dvd_folder_is_descrambled() {
    let (fx, clear, _) = dvd_fx(true);
    let dest = tempfile::tempdir().unwrap();
    let mut src = fx.source();
    fx.disc
        .extract_tree(
            &mut src,
            dest.path(),
            &Default::default(),
            &crate::ctx::Ctx::default(),
        )
        .unwrap();
    for vts in ["VTS_01_1.VOB", "VTS_02_1.VOB"] {
        let got = std::fs::read(dest.path().join("VIDEO_TS").join(vts)).unwrap();
        assert!(got[..clear.len()] == clear[..], "{vts} is still scrambled");
    }
}

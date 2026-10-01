//! Parity goldens for the live path: a real `Drive` over the in-crate `FakeTransport` serving
//! a synthetic AACS disc, scanned, keyed (clean and lazy) and muxed through a `DiscSession`.
//! Bad sectors, a stop mid-read and the keyless refusal are pinned as counters and verdicts.

use super::*;
use crate::dirimage::tests::{minimal_clpi, one_item_mpls};
use crate::disc::ScanOptions;
use crate::drive::Drive;
use crate::mux::driver::{MuxOptions, MuxSource, mux_with_keys};
use crate::mux::parity_tests::{golden, record_run, record_tree};
use crate::session::{DiscSession, KeySpec};
use crate::test_util::{FakeMode, FakeTransport, Golden, synthetic_bd_clip};

// A scannable one-title BD whose only stream file is a K1 clip of `UNITS` units.
fn disc_image() -> (EncryptedBdImage, Vec<u8>) {
    let clip = synthetic_bd_clip(6);
    let uk_ro = unit_key_ro(AacsVersion::V10, &[[0xEE; 16]], &[1]);
    let files = [
        BdFile::new("BDMV/index.bdmv", 1, None),
        BdFile::new("BDMV/PLAYLIST/00000.mpls", 1, None),
        BdFile::new("BDMV/CLIPINF/00000.clpi", 1, None),
        BdFile::new("BDMV/STREAM/00000.m2ts", 0, Some(K1)).with_clip(clip.clone()),
    ];
    let mut img = encrypted_bd_image(&files, &uk_ro);
    let packets = (clip.len() / 192) as u32;
    for (i, bytes) in [(1, one_item_mpls(b"00000")), (2, minimal_clpi(packets))] {
        let at = img.files[i].0 as usize * 2048;
        img.image[at..at + bytes.len()].copy_from_slice(&bytes);
    }
    (img, clip)
}

fn is_read10(c: &[u8]) -> bool {
    c[0] == crate::scsi::SCSI_READ_10
}

// Whether a READ(10) overlaps `[a, b)`.
fn reads(c: &[u8], a: u32, b: u32) -> bool {
    if !is_read10(c) {
        return false;
    }
    let lba = u32::from_be_bytes([c[2], c[3], c[4], c[5]]);
    let n = u16::from_be_bytes([c[7], c[8]]) as u32;
    lba < b && lba + n > a
}

// A scanned session over a fake drive serving `img`, the clip's streams put on title 0.
fn session(t: FakeTransport, halt: &Halt, clip: &[u8]) -> DiscSession {
    let drive = Drive::from_transport_with(Box::new(t), halt);
    let mut s = DiscSession::bring_up(drive, KeySpec::default(), Some(halt.clone())).unwrap();
    s.scan(ScanOptions::default()).expect("the fake disc scans");
    s.disc_mut().unwrap().titles[0].streams = crate::mux::ts::scan_streams(clip).unwrap();
    // The UDF volume id carries the fixture's temp-dir name, which varies per run.
    s.disc_mut().unwrap().volume_id = "PARITY".into();
    s.stage_drive_as_reader();
    s
}

fn medium_error() -> FakeMode {
    FakeMode::Sense {
        sense: crate::scsi::ScsiSense {
            sense_key: 3,
            asc: 0x11,
            ascq: 0x00,
        },
        progress: None,
    }
}

// Mux title 0 of `s` to mkv under `set`, recording the outcome and output.
fn mux(
    g: &mut Golden,
    tag: &str,
    s: &mut DiscSession,
    set: Option<&KeyRing>,
    opts: &MuxOptions,
    halt: &Halt,
) {
    let dir = tempfile::tempdir().unwrap();
    let dest = format!("mkv://{}", dir.path().join("o.mkv").display());
    let r = mux_with_keys(
        MuxSource::Session {
            session: s,
            title_index: 0,
        },
        set,
        &dest,
        opts,
        &crate::ctx::Ctx::new(halt.clone()),
    );
    record_run(g, tag, &r);
    record_tree(g, tag, dir.path());
}

fn opts(skip: bool) -> MuxOptions {
    MuxOptions {
        skip_errors: skip,
        batch_sectors: 30,
        ..Default::default()
    }
}

fn keys(s: &mut DiscSession, kdb: &[[u8; 16]]) -> crate::error::Result<KeyRing> {
    let calls = Calls::default();
    let f = factory(&[Spec::keydb(kdb, &calls)]);
    s.acquire_keys(
        KeyScope::Titles(vec![0]),
        &f,
        AcquireOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .map(|r| r.keys)
}

#[test]
fn parity_live_bd_session() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (img, clip) = disc_image();
    let mut g = golden("parity_live_bd_session");
    let halt = Halt::new();
    let (t, _h) = FakeTransport::new();
    let mut s = session(t.with_image(img.image.clone()), &halt, &clip);
    g.kv(
        "scan",
        format_args!("titles={}", s.disc().unwrap().titles.len()),
    );
    let set = keys(&mut s, &[K1]).unwrap();
    g.kv("keys", format_args!("{:?}", set.status()));
    mux(&mut g, "keyed", &mut s, Some(&set), &opts(false), &halt);
    let (t, _h) = FakeTransport::new();
    let mut s2 = session(t.with_image(img.image.clone()), &halt, &clip);
    let (t, _h) = FakeTransport::new();
    let mut s = session(t.with_image(img.image.clone()), &halt, &clip);
    mux(&mut g, "keyless", &mut s, None, &opts(false), &halt);
    mux(
        &mut g,
        "keyed-again",
        &mut s2,
        Some(&set),
        &opts(false),
        &halt,
    );
    let (t, _h) = FakeTransport::new();
    let mut s = session(t.with_image(img.image.clone()), &halt, &clip);
    let raw = MuxOptions {
        raw: true,
        ..opts(false)
    };
    mux(&mut g, "raw", &mut s, None, &raw, &halt);
    g.check();
}

// The key set resolved while the file's probes failed to read: the piece is lazy and is
// proven on arrival while the live mux reads it.
#[test]
fn parity_live_bd_lazy_piece() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (img, clip) = disc_image();
    let mut g = golden("parity_live_bd_lazy_piece");
    let halt = Halt::new();
    let (t, _h) = FakeTransport::new();
    let mut s = session(t.with_image(img.image.clone()), &halt, &clip);
    let (start, n) = img.files[3];
    let dead = Faulty::new(MemSource::new(img.image.clone()));
    dead.kill(start, start + n);
    let calls = Calls::default();
    let f = factory(&[Spec::keydb(&[K1], &calls)]);
    let (t2, _h2) = FakeTransport::new();
    let disc = session(t2.with_image(img.image.clone()), &halt, &clip)
        .take_disc()
        .unwrap();
    let set = KeyRing::acquire_for_disc(
        &disc,
        &mut dead.clone(),
        KeyScope::Titles(vec![0]),
        &f,
        AcquireOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .unwrap()
    .keys;
    g.kv("keys", format_args!("{:?}", set.status()));
    mux(&mut g, "lazy", &mut s, Some(&set), &opts(false), &halt);
    g.check();
}

// Media errors inside the clip: strict ends the mux, skip zero-fills and counts.
#[test]
fn parity_live_bd_bad_sectors() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (img, clip) = disc_image();
    let mut g = golden("parity_live_bd_bad_sectors");
    let (start, _) = img.files[3];
    for (tag, skip) in [("strict", false), ("skip", true)] {
        let halt = Halt::new();
        let (t, _h) = FakeTransport::new();
        let bad = (start + 9, start + 12);
        let t = t
            .with_image(img.image.clone())
            .rule(move |c| reads(c, bad.0, bad.1), medium_error());
        let mut s = session(t, &halt, &clip);
        let set = keys(&mut s, &[K1]).unwrap();
        mux(&mut g, tag, &mut s, Some(&set), &opts(skip), &halt);
    }
    g.check();
}

// A Stop raised by the first read of the clip: the mux ends not-completed, never an error.
#[test]
fn parity_live_bd_stop_mid_read() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (img, clip) = disc_image();
    let mut g = golden("parity_live_bd_stop_mid_read");
    let (start, _) = img.files[3];
    let halt = Halt::new();
    let (t, _h) = FakeTransport::new();
    // Armed once the key set exists, so the Stop lands in the mux's own reads.
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let gate = armed.clone();
    let t = t.with_image(img.image.clone()).cancel_on(
        move |c| {
            gate.load(std::sync::atomic::Ordering::Relaxed)
                && reads(c, start, start + 36)
                && seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0
        },
        &halt,
    );
    let mut s = session(t, &halt, &clip);
    let set = keys(&mut s, &[K1]).unwrap();
    armed.store(true, std::sync::atomic::Ordering::Relaxed);
    let dir = tempfile::tempdir().unwrap();
    let r = mux_with_keys(
        MuxSource::Session {
            session: &mut s,
            title_index: 0,
        },
        Some(&set),
        &format!("mkv://{}", dir.path().join("o.mkv").display()),
        &MuxOptions {
            batch_sectors: 2,
            ..Default::default()
        },
        &crate::ctx::Ctx::new(halt.clone()),
    );
    let o = r.as_ref().map(|o| (o.completed, o.errors, o.lost_bytes));
    g.kv("stop", format_args!("{o:?}"));
    g.check();
}

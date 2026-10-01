//! The decryption stage as every input sees it: `input()` of a loose `.m2ts` (AACS), and
//! the content-detected pass-through of files that hold nothing encrypted.

use crate::aacs::content::{ALIGNED_UNIT_LEN, encrypt_unit};
use crate::consts::BD_SOURCE_PACKET_BYTES as PKT;
use crate::error::{E_CSS_KEY_MISSING, E_MP4_INVALID, E_NO_DISC_KEY, E_NO_STREAMS, error_code};
use crate::keys::ResolvedKeySet;
use crate::mux::resolve::{InputOptions, input};
use crate::pes::PesFrame;

// A synthetic test key, not key material from any disc.
const KEY: [u8; 16] = [0x5A; 16];
const AUDIO_PID: u16 = 0x1100;
const PMT_PID: u16 = 0x0100;
const UNITS: usize = 6;

// One BD source packet: 4-byte TP_extra_header (CPI 00) + a TS packet on `pid`.
fn packet(pid: u16, pusi: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
    let mut p = vec![0xFFu8; PKT];
    p[..4].copy_from_slice(&[0, 0, 0, 0]);
    p[4] = 0x47;
    p[5] = ((pid >> 8) as u8 & 0x1F) | if pusi { 0x40 } else { 0 };
    p[6] = pid as u8;
    p[7] = 0x10 | (cc & 0x0F);
    let n = payload.len().min(184);
    p[8..8 + n].copy_from_slice(&payload[..n]);
    p
}

fn pat() -> Vec<u8> {
    let s = [
        0x00, 0x00, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01,
    ];
    let mut body = s.to_vec();
    body.extend_from_slice(&[0xE0 | (PMT_PID >> 8) as u8, PMT_PID as u8, 0, 0, 0, 0]);
    packet(0, true, 0, &body)
}

fn pmt() -> Vec<u8> {
    let mut b = vec![0x00, 0x02, 0xB0, 9 + 5 + 4, 0x00, 0x01, 0xC1, 0x00, 0x00];
    b.extend_from_slice(&[0xE0, 0x00, 0xF0, 0x00]);
    b.extend_from_slice(&[
        0x0F,
        0xE0 | (AUDIO_PID >> 8) as u8,
        AUDIO_PID as u8,
        0xF0,
        0x00,
    ]);
    b.extend_from_slice(&[0, 0, 0, 0]);
    packet(PMT_PID, true, 0, &b)
}

// A clear BD-style clip of `UNITS` whole aligned units: PAT, PMT, then one audio PES per
// packet with a payload unique to its index.
fn clear_clip() -> Vec<u8> {
    let mut clip = [pat(), pmt()].concat();
    let mut i = 0u32;
    while clip.len() < UNITS * ALIGNED_UNIT_LEN {
        let mut pes = vec![0, 0, 1, 0xC0, 0, 0, 0x80, 0x00, 0x00];
        pes.extend((0..175u32).map(|k| (i.wrapping_mul(31).wrapping_add(k) % 253) as u8));
        clip.extend(packet(AUDIO_PID, true, i as u8, &pes));
        i += 1;
    }
    clip
}

// `clip` with every packet's CPI set to 11₂ (KS-5) and, when `key` is given, every whole
// aligned unit encrypted under it (KS-3, KS-4).
fn flagged(clip: &[u8], key: Option<&[u8; 16]>) -> Vec<u8> {
    let mut out = clip.to_vec();
    for p in out.chunks_mut(PKT) {
        p[0] |= 0xC0;
    }
    if let Some(key) = key {
        for u in out.as_chunks_mut::<ALIGNED_UNIT_LEN>().0 {
            assert!(encrypt_unit(u, key));
        }
    }
    out
}

fn path(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("fmkv-stage-{tag}-{}-{n}.m2ts", std::process::id()))
}

// Every frame of `bytes` read as `scheme://`, and the stream's blanked-unit count.
fn read(scheme: &str, bytes: &[u8], opts: &InputOptions) -> std::io::Result<(Vec<PesFrame>, u64)> {
    let p = path(scheme);
    std::fs::write(&p, bytes).unwrap();
    let got = (|| {
        let mut s = input(&format!("{scheme}://{}", p.display()), opts)?;
        let mut frames = Vec::new();
        while let Some(f) = s.read()? {
            frames.push(f);
        }
        Ok((frames, s.errors()))
    })();
    let _ = std::fs::remove_file(&p);
    got
}

fn keyed() -> InputOptions {
    InputOptions {
        keys: Some(ResolvedKeySet::held_for_test(&[[0x11; 16], KEY])),
        ..Default::default()
    }
}

fn data(frames: &[PesFrame]) -> Vec<Vec<u8>> {
    frames.iter().map(|f| f.data.clone()).collect()
}

#[test]
fn a_clear_bd_m2ts_reads_its_frames() {
    let (frames, blanked) = read("m2ts", &clear_clip(), &Default::default()).unwrap();
    assert!(frames.len() > 100, "{} frames", frames.len());
    assert_eq!(blanked, 0);
}

// KS-1: every aligned unit of the clip file is encrypted; the held key opens them, the
// frames match the clear clip's, and the stage is the only decrypt on the way.
#[test]
fn an_aacs_m2ts_is_decrypted_with_the_held_keys() {
    let clear = data(&read("m2ts", &clear_clip(), &Default::default()).unwrap().0);
    let enc = flagged(&clear_clip(), Some(&KEY));
    let (frames, blanked) = read("m2ts", &enc, &keyed()).expect("decrypts");
    assert_eq!(data(&frames), clear);
    assert_eq!(blanked, 0);
}

// The refusal comes from `input()` itself: the stream never opens, so no frame exists.
#[test]
fn an_aacs_m2ts_with_no_keys_is_refused_before_any_frame() {
    let p = path("nokeys");
    std::fs::write(&p, flagged(&clear_clip(), Some(&KEY))).unwrap();
    let opened = input(&format!("m2ts://{}", p.display()), &Default::default());
    let _ = std::fs::remove_file(&p);
    assert_eq!(
        opened.err().and_then(|e| error_code(&e)),
        Some(E_NO_DISC_KEY)
    );
}

#[test]
fn an_aacs_m2ts_no_held_key_opens_is_refused() {
    let enc = flagged(&clear_clip(), Some(&KEY));
    let opts = InputOptions {
        keys: Some(ResolvedKeySet::held_for_test(&[[0x22; 16]])),
        ..Default::default()
    };
    let e = read("m2ts", &enc, &opts).unwrap_err();
    assert_eq!(error_code(&e), Some(E_NO_DISC_KEY));
}

// `--raw` never refuses on a missing key: the ciphertext reaches the demuxer, whose PMT scan
// finds nothing it can read.
#[test]
fn an_aacs_m2ts_under_raw_is_not_a_key_refusal() {
    let enc = flagged(&clear_clip(), Some(&KEY));
    let opts = InputOptions {
        raw: true,
        ..Default::default()
    };
    let e = read("m2ts", &enc, &opts).unwrap_err();
    assert_eq!(error_code(&e), Some(E_NO_STREAMS));
}

// A clear clip whose decrypter left CPI 11₂ (KS-5 says 00₂) is clear by structure: no key.
#[test]
fn a_clear_m2ts_with_stale_cpi_needs_no_key() {
    let clear = data(&read("m2ts", &clear_clip(), &Default::default()).unwrap().0);
    let (frames, blanked) = read("m2ts", &flagged(&clear_clip(), None), &Default::default())
        .expect("clear content opens without keys");
    assert_eq!(data(&frames), clear);
    assert_eq!(blanked, 0);
}

// Review: a damaged unit (10 of 31 packets off sync) in a stale-CPI clear clip is not
// mistaken for ciphertext: the clip opens with or without keys.
#[test]
fn a_damaged_unit_in_a_stale_cpi_clip_is_not_a_key_refusal() {
    let mut clip = flagged(&clear_clip(), None);
    let unit = &mut clip[3 * ALIGNED_UNIT_LEN..4 * ALIGNED_UNIT_LEN];
    for p in unit.chunks_mut(PKT).skip(1).take(10) {
        p[4] = 0x9D;
    }
    for opts in [Default::default(), keyed()] {
        let (frames, blanked) = read("m2ts", &clip, &opts).expect("opens");
        assert!(
            frames.len() > 100 && blanked == 0,
            "{} {blanked}",
            frames.len()
        );
    }
}

// Review: an encrypted clip behind a zero-filled start (11 blank units, past the 32-sector
// head) is judged by its first written unit: decrypted with the held keys, refused without.
#[test]
fn a_blank_head_does_not_hide_an_aacs_clip() {
    let clear = data(&read("m2ts", &clear_clip(), &Default::default()).unwrap().0);
    let enc = [
        vec![0; 11 * ALIGNED_UNIT_LEN],
        flagged(&clear_clip(), Some(&KEY)),
    ]
    .concat();
    let (frames, _) = read("m2ts", &enc, &keyed()).expect("decrypts");
    assert_eq!(data(&frames), clear);
    let e = read("m2ts", &enc, &Default::default()).unwrap_err();
    assert_eq!(error_code(&e), Some(E_NO_DISC_KEY));
}

// Review: a blank run inside the 32-sector head (28 sectors) does not hide the clip either.
#[test]
fn a_short_blank_run_does_not_hide_an_aacs_clip() {
    let mut enc = flagged(&clear_clip().repeat(3), Some(&KEY));
    enc[..28 * 2048].fill(0);
    let e = read("m2ts", &enc, &Default::default()).unwrap_err();
    assert_eq!(error_code(&e), Some(E_NO_DISC_KEY));
}

// A truncated copy: the encrypted partial unit at the end cannot be opened as a unit, so it
// is blanked and counted (E7013 option A), never muxed as ciphertext.
#[test]
fn a_truncated_aacs_m2ts_blanks_its_partial_unit() {
    let mut clip = clear_clip();
    let whole = data(&read("m2ts", &clip, &Default::default()).unwrap().0);
    let mut extra = clip[2 * PKT..2 * PKT + ALIGNED_UNIT_LEN].to_vec();
    for (j, p) in extra.chunks_mut(PKT).enumerate() {
        p[7] = 0x10 | (j as u8 & 0x0F);
    }
    clip.extend(extra);
    let mut enc = flagged(&clip, Some(&KEY));
    enc.truncate(UNITS * ALIGNED_UNIT_LEN + 20 * PKT);
    let (frames, blanked) = read("m2ts", &enc, &keyed()).expect("decrypts");
    assert_eq!(blanked, 1, "the partial unit is blanked and counted");
    assert_eq!(data(&frames)[..whole.len()], whole[..]);
}

// freemkv's own m2ts (an FMKV header, then TS off the unit grid) is always clear.
#[test]
fn an_fmkv_m2ts_still_reads() {
    let title = crate::mux::meta::M2tsMeta::from_title(&{
        let mut t = crate::disc::DiscTitle::empty();
        t.streams = crate::mux::ts::scan_streams(&clear_clip()).unwrap();
        t
    });
    let mut file = Vec::new();
    crate::mux::meta::write_header(&mut file, &title).unwrap();
    file.extend(clear_clip());
    let clear = data(&read("m2ts", &clear_clip(), &Default::default()).unwrap().0);
    let (frames, _) = read("m2ts", &file, &keyed()).expect("reads");
    assert_eq!(data(&frames), clear);
}

// A CSS-scrambled pack stream.
fn scrambled_ps() -> Vec<u8> {
    let mut out = Vec::new();
    for scramble in [0u8, 0x10] {
        let mut p = vec![0x5Au8; 2048];
        p[..4].copy_from_slice(&crate::css::PACK_START);
        p[4] = 0x44;
        p[0x0D] = 0xF8;
        p[0x0E..0x12].copy_from_slice(&[0, 0, 1, 0xE0]);
        p[0x12..0x14].copy_from_slice(&0x07ECu16.to_be_bytes());
        p[0x14] = 0x80 | scramble;
        out.extend(p);
    }
    out
}

// Every file arm passes the stage: scrambled content under a clear container's scheme is
// judged by its bytes and refused. MP4's box walk seeks past what the stage has judged and
// fails as not-MP4 (E9049) first; either way no ciphertext reaches a sink.
#[test]
fn every_file_scheme_passes_the_stage() {
    let enc = flagged(&clear_clip(), Some(&KEY));
    for (bytes, code) in [(scrambled_ps(), E_CSS_KEY_MISSING), (enc, E_NO_DISC_KEY)] {
        let e = read("mkv", &bytes, &Default::default()).unwrap_err();
        assert_eq!(error_code(&e), Some(code), "mkv");
        let e = read("mp4", &bytes, &Default::default()).unwrap_err();
        assert!(matches!(error_code(&e), Some(c) if c == code || c == E_MP4_INVALID));
    }
}

// §8.5: every file arm builds the stage around its source, whatever the bytes turn out to be.
#[test]
fn every_file_arm_builds_the_stage() {
    for scheme in ["mpg", "m2ts", "mkv", "mp4"] {
        let before = crate::sector::stage::STAGES.with(|n| n.get());
        let _ = read(
            scheme,
            &vec![0x5A; 4 * ALIGNED_UNIT_LEN],
            &Default::default(),
        );
        let built = crate::sector::stage::STAGES.with(|n| n.get()) - before;
        assert!(built >= 1, "{scheme}");
    }
}

// S7: `--raw` mpg:// reads its head through a non-raw stage only where that stage can; an
// AACS clip under the mpg scheme refuses there (E7022) and the raw head is used instead.
#[test]
fn a_raw_mpg_head_scan_falls_back_on_a_key_refusal() {
    let opts = InputOptions {
        raw: true,
        ..Default::default()
    };
    let e = read("mpg", &flagged(&clear_clip(), Some(&KEY)), &opts).err();
    assert_eq!(e.and_then(|e| error_code(&e)), Some(E_NO_STREAMS));
}

// A loose file's keys are found only by walking up to its disc folder (never a sidecar).
#[test]
fn a_loose_clip_finds_its_disc_folder_by_walking_up() {
    use crate::mux::resolve::disc_root_of;
    let root = std::env::temp_dir().join(format!("fmkv-root-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let stream = root.join("BDMV/STREAM");
    std::fs::create_dir_all(&stream).unwrap();
    let clip = stream.join("00001.m2ts");
    std::fs::write(&clip, b"").unwrap();
    std::fs::write(root.join("00001.m2ts"), b"").unwrap();
    let root = std::fs::canonicalize(&root).unwrap();
    assert_eq!(
        disc_root_of(&clip),
        None,
        "no AACS folder: no disc structure"
    );
    std::fs::create_dir_all(root.join("AACS")).unwrap();
    assert_eq!(disc_root_of(&clip), Some(root.clone()));
    assert_eq!(
        disc_root_of(&root.join("00001.m2ts")),
        None,
        "not under BDMV/STREAM"
    );
    // N11: a 3D clip's interleaved file sits one level deeper.
    std::fs::create_dir_all(stream.join("SSIF")).unwrap();
    std::fs::write(stream.join("SSIF/00001.ssif"), b"").unwrap();
    assert_eq!(
        disc_root_of(&stream.join("SSIF/00001.ssif")),
        Some(root.clone())
    );
    let _ = std::fs::remove_dir_all(&root);
}

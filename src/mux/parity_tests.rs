//! Parity goldens for the loose-file paths: every file source read through `input()` and
//! every sink driven through `mux_with_keys`, over the synthetic BD clip. Pinned as output
//! hashes, loss counters and refusal codes (see [`Golden`]); cells are `parity_*`.

use super::driver::{MuxOptions, MuxOutcome, MuxSource, mux_with_keys};
use super::resolve::{InputOptions, input};
use super::select::{PidFilter, StreamSelection};
use crate::aacs::content::{ALIGNED_UNIT_LEN, encrypt_unit};
use crate::consts::BD_SOURCE_PACKET_BYTES as PKT;
use crate::keys::ResolvedKeySet;
use crate::test_util::{CLIP_AUDIO_PIDS, Golden, synthetic_bd_clip};
use std::path::Path;

// A synthetic test key, not key material from any disc.
const KEY: [u8; 16] = [0x5A; 16];
const UNITS: usize = 12;

pub(crate) fn golden(cell: &str) -> Golden {
    Golden::new(env!("CARGO_MANIFEST_DIR"), cell)
}

fn replace(hay: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(hay.len());
    let mut i = 0;
    while i < hay.len() {
        if hay[i..].starts_with(from) {
            out.extend_from_slice(to);
            i += from.len();
        } else {
            out.push(hay[i]);
            i += 1;
        }
    }
    out
}

// Every file under `dir`, sorted by relative path, as `<prefix> out <name> = len sha256`.
// The temp root appears in provenance headers (`fvi://`) and differs per run: masked.
pub(crate) fn record_tree(g: &mut Golden, prefix: &str, dir: &Path) {
    let root = dir.parent().unwrap().display().to_string().into_bytes();
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                files.push(p);
            }
        }
    }
    files.sort();
    for f in files {
        let rel = f.strip_prefix(dir).unwrap().display().to_string();
        let mut bytes = replace(&std::fs::read(&f).unwrap(), &root, b"<TMP>");
        // The build stamp (version and commit) changes with every commit: same-length mask.
        for stamp in [crate::MUX_APP, super::videomap::FVI_GENERATOR] {
            bytes = replace(&bytes, stamp.as_bytes(), &vec![b'#'; stamp.len()]);
        }
        g.bytes(&format!("{prefix} out {rel}"), &bytes);
    }
}

// The outcome (or refusal code) of one mux run.
pub(crate) fn record_run(g: &mut Golden, prefix: &str, r: &std::io::Result<MuxOutcome>) {
    match r {
        Ok(o) => g.kv(
            &format!("{prefix} outcome"),
            format_args!(
                "completed={} opened={} bytes={} errors={} lost={} streams={} undelivered={:?}",
                o.completed,
                o.output_opened,
                o.bytes_written,
                o.errors,
                o.lost_bytes,
                o.streams,
                o.undelivered_streams
            ),
        ),
        Err(e) => g.kv(
            &format!("{prefix} refused"),
            format_args!("E{}", crate::error::error_code(e).unwrap_or(0)),
        ),
    };
}

fn dest_name(scheme: &str) -> &'static str {
    match scheme {
        "mkv" => "o.mkv",
        "mp4" => "o.mp4",
        "mpg" => "o.mpg",
        "m2ts" => "o.m2ts",
        "fvi" => "o.fvi",
        "chapters" => "o.xml",
        "json" => "o.json",
        _ => "o",
    }
}

// Mux the loose file `bytes` (read as `src_scheme://`) to each `sinks` entry, recording the
// outcome and every output file under the sink's name.
fn sinks(
    g: &mut Golden,
    src_scheme: &str,
    bytes: &[u8],
    sinks: &[&str],
    mopts: &MuxOptions,
    iopts: &InputOptions,
) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join(format!("in.{src_scheme}"));
    std::fs::write(&input, bytes).unwrap();
    let url = format!("{src_scheme}://{}", input.display());
    for sink in sinks {
        let out = dir.path().join(format!("out-{sink}"));
        std::fs::create_dir_all(&out).unwrap();
        let dest = match *sink {
            "null" => "null://".to_string(),
            s => format!("{s}://{}", out.join(dest_name(s)).display()),
        };
        let r = mux_with_keys(
            MuxSource::Url {
                url: &url,
                opts: iopts.clone(),
            },
            None,
            &dest,
            mopts,
            &crate::ctx::Ctx::default(),
        );
        record_run(g, sink, &r);
        record_tree(g, sink, &out);
    }
}

const ALL_SINKS: &[&str] = &[
    "mkv", "mp4", "mpg", "m2ts", "fvi", "json", "chapters", "demux", "video", "audio", "sub",
    "null",
];

#[test]
fn parity_sinks_clear_m2ts_default() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let mut g = golden("parity_sinks_clear_m2ts_default");
    sinks(
        &mut g,
        "m2ts",
        &synthetic_bd_clip(UNITS),
        ALL_SINKS,
        &MuxOptions::default(),
        &Default::default(),
    );
    g.check();
}

// `raw` is ciphertext passthrough: on a clear clip it changes no output byte.
#[test]
fn parity_sinks_clear_m2ts_raw() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let mut g = golden("parity_sinks_clear_m2ts_raw");
    let opts = MuxOptions {
        raw: true,
        ..Default::default()
    };
    let iopts = InputOptions {
        raw: true,
        ..Default::default()
    };
    sinks(
        &mut g,
        "m2ts",
        &synthetic_bd_clip(UNITS),
        &["mkv", "m2ts", "demux"],
        &opts,
        &iopts,
    );
    g.check();
}

// Selection through a URL source goes in `InputOptions` (the second audio only, no subs).
#[test]
fn parity_sinks_clear_m2ts_selection() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let mut g = golden("parity_sinks_clear_m2ts_selection");
    let iopts = InputOptions {
        selection: StreamSelection {
            audio: PidFilter::Only(vec![CLIP_AUDIO_PIDS[1]]),
            subtitle: PidFilter::Only(vec![]),
        },
        ..Default::default()
    };
    sinks(
        &mut g,
        "m2ts",
        &synthetic_bd_clip(UNITS),
        &["mkv", "m2ts", "demux", "json"],
        &MuxOptions::default(),
        &iopts,
    );
    g.check();
}

// Every container this crate writes is also a source: re-mux its output to mkv.
#[test]
fn parity_sinks_from_mkv_mpg_and_fmkv_sources() {
    let mut g = golden("parity_sinks_from_mkv_mpg_and_fmkv_sources");
    let _serial = crate::sector::prefetched::holder_test_lock();
    let clip = synthetic_bd_clip(UNITS);
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("clip.m2ts");
    std::fs::write(&src, &clip).unwrap();
    for (scheme, name) in [("mkv", "a.mkv"), ("mpg", "a.mpg"), ("m2ts", "a.fmkv.m2ts")] {
        let p = dir.path().join(name);
        mux_with_keys(
            MuxSource::Url {
                url: &format!("m2ts://{}", src.display()),
                opts: Default::default(),
            },
            None,
            &format!("{scheme}://{}", p.display()),
            &MuxOptions::default(),
            &crate::ctx::Ctx::default(),
        )
        .unwrap();
        let bytes = std::fs::read(&p).unwrap();
        sinks(
            &mut g,
            scheme,
            &bytes,
            &["mkv"],
            &MuxOptions::default(),
            &Default::default(),
        );
        let tag = format!("{scheme}-src");
        let mut s = input(
            &format!("{scheme}://{}", p.display()),
            &Default::default(),
            &crate::ctx::Ctx::default(),
        )
        .unwrap();
        record_frames(&mut g, &tag, s.as_mut());
    }
    g.check();
}

// The frames a source yields (count, a digest over track/pts/keyframe/data), its stream
// list, and the stream's loss counters; or the code that refused it first.
fn record_frames(g: &mut Golden, tag: &str, s: &mut dyn crate::pes::Stream) {
    let streams: Vec<String> = s
        .info()
        .streams
        .iter()
        .map(|st| match st {
            crate::disc::Stream::Video(v) => format!("v:{:?}:{:#x}", v.codec, v.pid),
            crate::disc::Stream::Audio(a) => format!("a:{:?}:{:#x}", a.codec, a.pid),
            crate::disc::Stream::Subtitle(x) => format!("s:{:?}:{:#x}", x.codec, x.pid),
        })
        .collect();
    let mut digest = Vec::new();
    let mut n = 0u64;
    while let Some(f) = s.read().unwrap() {
        n += 1;
        digest.extend_from_slice(&(f.track as u64).to_le_bytes());
        digest.extend_from_slice(&f.pts.to_le_bytes());
        digest.push(f.keyframe as u8);
        digest.extend_from_slice(&f.data);
    }
    g.kv(&format!("{tag} streams"), streams.join(","));
    g.kv(&format!("{tag} frames"), n);
    g.bytes(&format!("{tag} frame-digest"), &digest);
    g.kv(
        &format!("{tag} errors"),
        format_args!("{} lost={}", s.errors(), s.lost_bytes()),
    );
}

// ── Loose AACS and CSS file sources (input()) ──────────────────────────────

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

fn record_source(g: &mut Golden, tag: &str, scheme: &str, bytes: &[u8], opts: &InputOptions) {
    // Keep the temp file until the stream is drained (the sector pipeline reads lazily).
    let p = std::env::temp_dir().join(format!("fmkv-parity-{tag}-{}", std::process::id()));
    std::fs::write(&p, bytes).unwrap();
    let r = input(
        &format!("{scheme}://{}", p.display()),
        opts,
        &crate::ctx::Ctx::default(),
    );
    match r {
        Ok(mut s) => {
            let mut digest = Vec::new();
            let mut n = 0u64;
            let failed = loop {
                match s.read() {
                    Ok(Some(f)) => {
                        n += 1;
                        digest.extend_from_slice(&(f.track as u64).to_le_bytes());
                        digest.extend_from_slice(&f.pts.to_le_bytes());
                        digest.extend_from_slice(&f.data);
                    }
                    Ok(None) => break None,
                    Err(e) => break Some(crate::error::error_code(&e).unwrap_or(0)),
                }
            };
            g.kv(&format!("{tag} frames"), n);
            g.bytes(&format!("{tag} frame-digest"), &digest);
            g.kv(&format!("{tag} blanked"), s.errors());
            if let Some(code) = failed {
                g.kv(&format!("{tag} read-failed"), format_args!("E{code}"));
            }
        }
        Err(e) => {
            g.kv(
                &format!("{tag} refused"),
                format_args!("E{}", crate::error::error_code(&e).unwrap_or(0)),
            );
        }
    }
    let _ = std::fs::remove_file(&p);
}

fn keyed(keys: &[[u8; 16]]) -> InputOptions {
    InputOptions {
        keys: Some(ResolvedKeySet::held_for_test(keys)),
        ..Default::default()
    }
}

#[test]
fn parity_sources_loose_m2ts_aacs() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let mut g = golden("parity_sources_loose_m2ts_aacs");
    let clip = synthetic_bd_clip(UNITS);
    let enc = flagged(&clip, Some(&KEY));
    record_source(&mut g, "clear", "m2ts", &clip, &Default::default());
    record_source(&mut g, "clear-keyed", "m2ts", &clip, &keyed(&[KEY]));
    record_source(
        &mut g,
        "stale-cpi",
        "m2ts",
        &flagged(&clip, None),
        &Default::default(),
    );
    record_source(
        &mut g,
        "aacs-held-key",
        "m2ts",
        &enc,
        &keyed(&[[0x11; 16], KEY]),
    );
    record_source(&mut g, "aacs-no-keys", "m2ts", &enc, &Default::default());
    record_source(
        &mut g,
        "aacs-wrong-key",
        "m2ts",
        &enc,
        &keyed(&[[0x22; 16]]),
    );
    let raw = InputOptions {
        raw: true,
        ..Default::default()
    };
    record_source(&mut g, "aacs-raw", "m2ts", &enc, &raw);
    let mut trunc = enc.clone();
    trunc.truncate(enc.len() - ALIGNED_UNIT_LEN + 20 * PKT);
    record_source(&mut g, "aacs-truncated", "m2ts", &trunc, &keyed(&[KEY]));
    let mut damaged = enc.clone();
    crate::test_util::damage_unit_seed(&mut damaged[4 * ALIGNED_UNIT_LEN..5 * ALIGNED_UNIT_LEN]);
    record_source(
        &mut g,
        "aacs-damaged-unit",
        "m2ts",
        &damaged,
        &keyed(&[KEY]),
    );
    g.check();
}

// The sink's own `.mpg` of the synthetic clip (clear), as a DVD-like program stream.
pub(crate) fn clear_mpg() -> Vec<u8> {
    let clip = synthetic_bd_clip(UNITS * 2);
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("c.m2ts");
    std::fs::write(&src, &clip).unwrap();
    let dst = dir.path().join("c.mpg");
    mux_with_keys(
        MuxSource::Url {
            url: &format!("m2ts://{}", src.display()),
            opts: Default::default(),
        },
        None,
        &format!("mpg://{}", dst.display()),
        &MuxOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    std::fs::read(&dst).unwrap()
}

// `clear` with every whole-sector video pack CSS-scrambled under `key`.
pub(crate) fn scramble_vob(clear: &[u8], key: &[u8; 5]) -> Vec<u8> {
    let mut vob = clear.to_vec();
    for pk in vob.as_chunks_mut::<2048>().0 {
        if pk[13] & 7 == 0 && pk[17] == 0xE0 && pk[0x14] & 0x30 == 0 {
            pk[0x14] |= 0x10;
            crate::css::lfsr::scramble_sector(key, pk);
        }
    }
    vob
}

fn css_vob() -> (Vec<u8>, Vec<u8>) {
    let clear = clear_mpg();
    let vob = scramble_vob(&clear, &[0x42, 0x13, 0x37, 0xBE, 0xEF]);
    (clear, vob)
}

#[test]
fn parity_sources_loose_mpg_css() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let mut g = golden("parity_sources_loose_mpg_css");
    let (clear, vob) = css_vob();
    record_source(&mut g, "clear", "mpg", &clear, &Default::default());
    record_source(&mut g, "scrambled", "mpg", &vob, &Default::default());
    let raw = InputOptions {
        raw: true,
        ..Default::default()
    };
    record_source(&mut g, "scrambled-raw", "mpg", &vob, &raw);
    // A scrambled stream under a clear container's scheme is refused by content, not name.
    record_source(&mut g, "scrambled-as-mkv", "mkv", &vob, &Default::default());
    sinks(
        &mut g,
        "mpg",
        &vob,
        &["mkv", "m2ts"],
        &MuxOptions::default(),
        &Default::default(),
    );
    g.check();
}

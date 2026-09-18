// Corpus title-shape dump for issue #45 fallback tuning.
//
// Runs the REAL offline scan (Disc::scan_image → sort_titles_by_main_feature)
// over a directory of decrypted ISOs and emits one NDJSON line per disc:
// every title's duration/size/clip-shape plus the rank flags the selector
// computed (nav/authoring/composite) and which title it picked (titles[0]).
//
// The point: measure, across many real discs, how cleanly any candidate
// discriminator separates the true main feature from play-all wrappers and
// seamless-branch decoys — so the #45 fallback is derived from data, not a
// guessed 0.85/0.90 constant.
//
// Usage: corpus_titledump <iso_dir> [<iso_dir>...]  > corpus.ndjson

use libfreemkv::disc::{Disc, ScanOptions};
use libfreemkv::io::file_sector_source::FileSectorSource;
use libfreemkv::sector::SectorSource;
use std::path::{Path, PathBuf};

// scan_image wants the disc capacity in sectors; FileSectorSource knows it.

fn jstr(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn dump_one(path: &Path) -> String {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("?");
    let mut reader = match FileSectorSource::open(path) {
        Ok(r) => r,
        Err(e) => {
            return format!(
                "{{\"iso\":{},\"error\":\"open: {:?}\"}}",
                jstr(name),
                format!("{e}")
            );
        }
    };
    let cap = reader.capacity_sectors();
    let opts = ScanOptions::default();
    let disc = match Disc::scan_image(&mut reader, cap, &opts) {
        Ok(d) => d,
        Err(e) => {
            return format!(
                "{{\"iso\":{},\"error\":\"scan: {}\"}}",
                jstr(name),
                jstr(&format!("{e}"))
            );
        }
    };

    // Recompute the rank flags the selector used so we can see WHY it ordered
    // things the way it did. nav/authoring hint aren't available offline here
    // (scan_with already applied them into titles[0]); pass None/None so the
    // flags reflect the pure composite/video/size classification, which is the
    // fallback we are tuning.
    let ranks = Disc::rank_titles(&disc.titles, None, None);

    // pick = titles[0] (post-sort main feature). Also compute the naive
    // "longest video title" as a ground-truth proxy for single-movie discs.
    let pick_pl = disc.titles.first().map(|t| t.playlist_id).unwrap_or(0);
    let longest_video_pl = disc
        .titles
        .iter()
        .filter(|t| t.has_probable_video())
        .max_by(|a, b| a.duration_secs.partial_cmp(&b.duration_secs).unwrap())
        .map(|t| t.playlist_id);

    let mut titles_json = String::from("[");
    for (i, t) in disc.titles.iter().enumerate() {
        if i > 0 {
            titles_json.push(',');
        }
        let r = &ranks[i];
        // Sum of the FULL durations of this title's distinct clips (each clip
        // counted once at its clip-level duration). A play-all concat wrapper's
        // title duration ≈ this sum; a seamless-branch feature's is dominated by
        // one clip. This is the structural discriminator to evaluate.
        let mut seen = std::collections::BTreeMap::<&str, f64>::new();
        for c in &t.clips {
            seen.entry(c.clip_id.as_str()).or_insert(c.duration_secs);
        }
        let sum_distinct_clip_dur: f64 = seen.values().sum();
        let max_distinct_clip_dur: f64 = seen.values().cloned().fold(0.0, f64::max);
        let n_distinct_clips = seen.len();
        let clip_ids: String = {
            let mut s = String::from("[");
            for (k, id) in seen.keys().enumerate() {
                if k > 0 {
                    s.push(',');
                }
                s.push_str(&jstr(id));
            }
            s.push(']');
            s
        };
        titles_json.push_str(&format!(
            "{{\"pl\":{},\"dur\":{:.1},\"size\":{},\"clips\":{},\"distinct_clips\":{},\
             \"sum_distinct_clip_dur\":{:.1},\"max_distinct_clip_dur\":{:.1},\
             \"has_video\":{},\"has_probable_video\":{},\
             \"nav\":{},\"authoring\":{},\"composite\":{},\"clip_ids\":{}}}",
            t.playlist_id,
            t.duration_secs,
            t.size_bytes,
            t.clips.len(),
            n_distinct_clips,
            sum_distinct_clip_dur,
            max_distinct_clip_dur,
            t.has_video(),
            t.has_probable_video(),
            r.nav,
            r.authoring,
            r.composite,
            clip_ids,
        ));
    }
    titles_json.push(']');

    format!(
        "{{\"iso\":{},\"format\":\"{:?}\",\"cap\":{},\"n_titles\":{},\
         \"pick_pl\":{},\"longest_video_pl\":{},\"pick_is_longest_video\":{},\"titles\":{}}}",
        jstr(name),
        disc.format,
        cap,
        disc.titles.len(),
        pick_pl,
        longest_video_pl
            .map(|p| p.to_string())
            .unwrap_or_else(|| "null".into()),
        longest_video_pl.map(|p| p == pick_pl).unwrap_or(false),
        titles_json,
    )
}

fn main() {
    let dirs: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if dirs.is_empty() {
        eprintln!("usage: corpus_titledump <iso_dir> [<iso_dir>...]");
        std::process::exit(2);
    }
    let mut isos: Vec<PathBuf> = Vec::new();
    for d in &dirs {
        if d.is_file() {
            isos.push(d.clone());
            continue;
        }
        if let Ok(rd) = std::fs::read_dir(d) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s.eq_ignore_ascii_case("iso"))
                    .unwrap_or(false)
                {
                    isos.push(p);
                }
            }
        }
    }
    isos.sort();
    eprintln!("scanning {} ISOs", isos.len());
    for (i, p) in isos.iter().enumerate() {
        eprintln!("[{}/{}] {}", i + 1, isos.len(), p.display());
        println!("{}", dump_one(p));
    }
}

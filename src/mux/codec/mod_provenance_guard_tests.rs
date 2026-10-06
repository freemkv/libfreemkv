//! Every emitted frame must carry the source byte offset of the packet it came from. That
//! invariant held only for video for as long as it existed: several audio/subtitle parsers
//! built frames with `source: None`, so a multi-clip title fell back to inferring the clip
//! from timestamps — which made branched titles run minutes long. This is a brace-balanced
//! scan of the tree, as a test.

/// Every parser module's source, checked for a `Frame` built without a
/// source. Paired with `every_codec_module_is_covered` below, which fails
/// if a module is added and not listed here.
const PARSER_SOURCES: &[(&str, &str)] = &[
    ("mod.rs", include_str!("mod.rs")),
    ("ac3.rs", include_str!("ac3.rs")),
    ("adts.rs", include_str!("adts.rs")),
    ("audio_frames.rs", include_str!("audio_frames.rs")),
    ("dts.rs", include_str!("dts.rs")),
    ("dvdsub.rs", include_str!("dvdsub.rs")),
    ("flac.rs", include_str!("flac.rs")),
    ("h264.rs", include_str!("h264.rs")),
    ("hevc.rs", include_str!("hevc.rs")),
    ("lpcm.rs", include_str!("lpcm.rs")),
    ("mpeg2.rs", include_str!("mpeg2.rs")),
    ("mpegaudio.rs", include_str!("mpegaudio.rs")),
    ("pgs.rs", include_str!("pgs.rs")),
    ("truehd.rs", include_str!("truehd.rs")),
    ("vc1.rs", include_str!("vc1.rs")),
];

/// Modules that declare no `Frame` and so need no entry above.
const NON_PARSER_MODULES: &[&str] = &[
    "coding",
    "crc",
    "decodable",
    "dropgate",
    "mp2_channels",
    "pesbuf",
    "reorder",
    "startcode",
    "tests",
    "provenance_guard",
];

// Strip line comments before scanning: a comment merely mentioning `source: None` isn't a
// construction.
fn code_only(src: &str) -> String {
    // The guard itself constructs no Frame; it only talks about them, in
    // prose and in string literals. Scanning its own body matches both.
    let src = match src.find("mod provenance_guard") {
        Some(i) => &src[..i],
        None => src,
    };
    src.lines()
        .map(|l| match l.find("//") {
            // Not inside a string literal: no quote before the slashes.
            Some(i) if !l[..i].contains('"') => &l[..i],
            _ => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn frame_literals(src: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let b = src.as_bytes();
    let mut i = 0;
    while let Some(p) = src[i..].find("Frame {") {
        let start = i + p;
        // Skip longer names ending in `Frame {` (e.g. `PesFrame {`).
        if start > 0 && (b[start - 1].is_ascii_alphanumeric() || b[start - 1] == b'_') {
            i = start + "Frame {".len();
            continue;
        }
        let open = start + src[start..].find('{').unwrap();
        let (mut depth, mut k) = (0usize, open);
        while k < b.len() {
            match b[k] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            k += 1;
        }
        out.push(&src[start..(k + 1).min(src.len())]);
        i = k + 1;
    }
    out
}

#[test]
fn no_parser_emits_a_frame_without_provenance() {
    let mut offenders = Vec::new();
    for (name, src) in PARSER_SOURCES {
        let src = &code_only(src);
        for blk in frame_literals(src) {
            // Only codec `Frame` literals are scanned (not `PesFrame`, whose
            // `source: None` fixtures are fine). Two loss spellings: explicit
            // `source: None`, or omitting it (Frame derives Default, so `..` yields None).
            let explicit_none = blk.contains("source: None");
            let no_source_field = !blk.contains("source:") && !blk.contains("source,");
            if explicit_none || no_source_field {
                let line = src[..src.find(blk).unwrap_or(0)].lines().count() + 1;
                let how = if explicit_none {
                    "source: None"
                } else {
                    "no source field (Default fills in None)"
                };
                offenders.push(format!("{name}:{line} ({how})"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these parsers build a Frame with no source byte offset, so a \
             multi-clip title cannot place their tracks by provenance and \
             falls back to inferring the clip from timestamps: {offenders:?}"
    );
}

/// A parser added without an entry in `PARSER_SOURCES` would be silently
/// unchecked, which is exactly how this class of gap persists.
#[test]
fn every_codec_module_is_covered() {
    let modules: Vec<&str> = include_str!("mod.rs")
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            let rest = ["pub mod ", "pub(crate) mod ", "mod "]
                .iter()
                .find_map(|p| l.strip_prefix(p))?;
            rest.strip_suffix(';')
        })
        .collect();
    let listed: Vec<&str> = PARSER_SOURCES
        .iter()
        .map(|(n, _)| n.trim_end_matches(".rs"))
        .collect();
    let unchecked: Vec<&&str> = modules
        .iter()
        .filter(|m| !listed.contains(m) && !NON_PARSER_MODULES.contains(m))
        .collect();
    assert!(
        unchecked.is_empty(),
        "codec module(s) not covered by the provenance guard — add them to \
             PARSER_SOURCES (or to NON_PARSER_MODULES if they emit no Frame): \
             {unchecked:?}"
    );
}

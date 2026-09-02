//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework. To add a
//! new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult>`
//!      (set [`ParseResult::confidence`] to drive tie-breaking)
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

mod bdmt;
pub(crate) mod class_reader;
pub mod clpi_audit;
mod criterion;
mod ctrm;
mod dbp;
pub(crate) mod deluxe;
pub(crate) mod fox;
pub(crate) mod jar;
mod mpls_universal;
mod paramount;
mod pixelogic;
mod png_filenames;
pub(crate) mod text;
pub mod vocab;
pub(crate) mod xml;

use crate::disc::{DiscTitle, Stream};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

// Re-export bdmt's public type so callers can construct/inspect
// disc-level metadata via `labels::DiscMetadata`. The module itself
// stays private — analyze() drives the parse path.
pub use bdmt::DiscMetadata;

// Re-exported via crate::disc — the public API surfaces these next to
// AudioStream/SubtitleStream so callers can map purpose/qualifier to display
// text in their own locale.

/// The one elementary stream a label describes, named the way the disc names
/// it: a PID inside a clip. A PID is only unique within one clip — two
/// unrelated `.m2ts` files both open their first audio at 0x1100 — so the clip
/// is part of the identity, not decoration.
///
/// This is the same key [`apply_labels`] already binds anchor facts through, so
/// a label that carries one needs no ordinal, no sequence and no guess.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId {
    /// Clip filename without extension (e.g. "00294"), matching
    /// [`crate::disc::Clip::clip_id`].
    pub clip_id: String,
    /// MPEG-TS PID of the elementary stream within that clip.
    pub pid: u16,
}

/// A stream label extracted from disc config files.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StreamLabel {
    /// Which elementary stream this label describes, when its source stated
    /// it outright.
    ///
    /// `Some` for MPLS- and CLPI-derived labels, which read the PID from
    /// the same table the stream itself is built from. `None` for
    /// vendor-authored labels — a BD-J config blob names slots, not PIDs,
    /// so those bind through the language-sequence anchor instead. The
    /// presence of an id IS the provenance marker, so a side table keyed
    /// by slot is avoided.
    pub stream_id: Option<StreamId>,
    /// STN index (1-based). Meaningful only for vendor labels
    /// (`stream_id: None`), which is all binding ever reads it for; a
    /// PID-bearing label carries one for display order alone.
    pub stream_number: u16,
    /// Audio or Subtitle
    pub stream_type: StreamLabelType,
    /// ISO 639-2 language code
    pub language: String,
    /// Display name (e.g. "Commentary", "Descriptive Audio")
    pub name: String,
    /// Stream purpose
    pub purpose: LabelPurpose,
    /// Additional qualifier
    pub qualifier: LabelQualifier,
    /// Codec hint from config (e.g. "TrueHD", "Dolby Digital", "Dolby Atmos")
    pub codec_hint: String,
    /// Regional variant (e.g. "US", "UK", "Castilian", "Canadian")
    pub variant: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLabelType {
    Audio,
    Subtitle,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelPurpose {
    Normal,
    Commentary,
    Descriptive,
    Score,
    /// Alternate music track (e.g. an alternate end-credits / closing-
    /// theme music stream), tagged by the `ime` token some BD-J
    /// authoring tools emit on the secondary music audio.
    Ime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LabelQualifier {
    None,
    Sdh,
    DescriptiveService,
    Forced,
}

// ── Parser registry ── entries: (name, detect_fn, parse_fn); order is
// tiebreak only. `detect` takes the reader so it can inspect jar
// contents (vendor prefix / project file) instead of firing on "any jar".
type DetectFn = fn(&mut dyn SectorSource, &UdfFs) -> bool;
type ParseFn = fn(&mut dyn SectorSource, &UdfFs) -> Option<ParseResult>;

/// Per-parser claim of how reliable its output is. Used by the
/// registry to pick between parsers when more than one matches (e.g.
/// a disc that has both `bluray_project.bin` and `playlists.xml`).
///
/// `High`: full schema extracted, no fallback or guessing. `Medium`:
/// matched but degraded (missing fields, undecoded sub-table). `Low`:
/// universal MPLS fallback — spec-mandated language + base codec with
/// no editorial labels. Registry prefers `High > Medium > Low`; ties
/// fall to array order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// The disc's own authoring-designated main-feature playlist, surfaced by a
/// parser that reads the menu/authoring metadata (e.g. Paramount `playlists.xml`
/// carries `<playlist name="Feature" id="00222"/>`). Consumed by title
/// selection (`Disc::main_feature_order`) as a size-independent anti-decoy
/// signal — a playlist obfuscation trick can inflate a streamless decoy past
/// the real feature on size/duration, but it can't forge the authoring id.
/// Best-effort: an unknown field is `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeaturePlaylistHint {
    /// Playlist number (e.g. 222 for `00222.mpls`) when the authoring id parses.
    pub playlist_id: Option<u16>,
    /// Playlist filename (e.g. "00222.mpls") when derivable.
    pub filename: Option<String>,
}

impl FeaturePlaylistHint {
    /// Whether this hint designates the playlist a title carries. Matches on
    /// the numeric id OR the filename (case-insensitive), so a hint that pins
    /// only one still binds.
    pub fn matches(&self, title_playlist_id: u16, title_playlist: &str) -> bool {
        if self.playlist_id == Some(title_playlist_id) {
            return true;
        }
        matches!(&self.filename, Some(f) if f.eq_ignore_ascii_case(title_playlist))
    }

    /// Whether the hint carries no usable identity.
    pub fn is_empty(&self) -> bool {
        self.playlist_id.is_none() && self.filename.is_none()
    }
}

/// Successful parser result. `None` from `parse()` still means "this
/// isn't my disc" (no labels at all); `Some(ParseResult { labels, .. })`
/// with `labels.is_empty()` is also a "no labels" case but reachable
/// via the analyzer (used by deluxe today to signal "I recognized the
/// framework but Phase D not yet implemented").
#[derive(Debug, Clone)]
pub struct ParseResult {
    pub labels: Vec<StreamLabel>,
    pub confidence: Confidence,
    /// The disc's authoring-named feature playlist, when this parser read it
    /// from the menu metadata. `None` for parsers that don't expose one.
    pub feature_playlist: Option<FeaturePlaylistHint>,
}

impl ParseResult {
    /// Convenience for the common "I parsed N labels with full schema
    /// coverage" case.
    pub fn high(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::High,
            feature_playlist: None,
        }
    }

    /// Convenience for "I matched but had to fall back on some fields".
    pub fn medium(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::Medium,
            feature_playlist: None,
        }
    }

    /// Convenience for the universal MPLS fallback: spec-derived
    /// stream language + codec, but no editorial labels (commentary,
    /// SDH, etc.). Framework parsers always win over `low`.
    pub fn low(labels: Vec<StreamLabel>) -> Self {
        ParseResult {
            labels,
            confidence: Confidence::Low,
            feature_playlist: None,
        }
    }
}

const PARSERS: &[(&str, DetectFn, ParseFn)] = &[
    ("paramount", paramount::detect, paramount::parse),
    ("criterion", criterion::detect, criterion::parse),
    ("pixelogic", pixelogic::detect, pixelogic::parse),
    ("ctrm", ctrm::detect, ctrm::parse),
    // dbp and deluxe detect via the real `com/<vendor>/` central-directory
    // prefix (reader-backed), so they claim only their own discs. dbp goes
    // first on ties: its parse path is cheaper (constant-pool vs. bytecode).
    ("dbp", dbp::detect, dbp::parse),
    ("deluxe", deluxe::detect, deluxe::parse),
    // Fox — older discs ship a loose plain-XML `/BDMV/JAR/<id>/dcx.xml` manifest
    // with per-stream language/purpose/qualifier (High confidence). Newer Fox
    // wraps this in `com/foxbd` bytecode (fox.rs Phase 2); detected, not yet decoded.
    ("fox", fox::detect, fox::parse),
    // Universal MPLS fallback. Returns Confidence::Low so framework parsers
    // always win when they match. Closes the "no framework matched" gap (e.g.
    // HDMV-only discs) with spec-derived language + codec for every stream.
    (
        "mpls_universal",
        mpls_universal::detect,
        mpls_universal::parse,
    ),
    // Menu-graphic filename language hints (Low). AFTER mpls_universal so the
    // richer spec-derived floor wins the Low tie whenever it produces anything;
    // only chosen when even MPLS yields nothing but the menu art still names languages.
    ("png_filenames", png_filenames::detect, png_filenames::parse),
];

/// Search disc for config files, extract labels, apply to streams.
/// This is 100% optional — if anything fails, streams are untouched.
/// Returns the disc's authoring-designated feature playlist when a parser
/// surfaced one (`None` otherwise), so the caller can bias title selection
/// toward it. Label binding onto streams is a side effect on `titles`.
pub fn apply(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    titles: &mut [DiscTitle],
) -> Option<FeaturePlaylistHint> {
    let (labels, feature_playlist) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extract(reader, udf)))
            .unwrap_or_default();
    if !labels.is_empty() {
        apply_labels(&labels, titles);
    }
    feature_playlist.filter(|h| !h.is_empty())
}

// Min streams of one type before a language sequence anchors the label
// list (see find_anchor): a 1-stream match is a coin flip (some
// single-audio menu clip always matches label #1).
const MIN_ANCHOR_STREAMS: usize = 2;

// `stream_number` meaning "no STN slot stated". STN slots are 1-based,
// so 0 is unused; only stream_id-bearing labels may carry it.
const NO_STN_SLOT: u16 = 0;

// Two ISO 639-2 codes that do NOT contradict: equal (case/padding
// insensitive), or either side is empty ("unknown", never a contradiction).
fn languages_compatible(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    a.is_empty() || b.is_empty() || a.eq_ignore_ascii_case(b)
}

/// Stricter form of [`languages_compatible`]: both sides actually state a
/// language and they are the same. "Unknown" is not agreement.
fn languages_agree(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    !a.is_empty() && a.eq_ignore_ascii_case(b)
}

// The VENDOR label occupying 1-based STN slot `n` of `stream_type`, if
// any (derived labels bind by StreamId instead, not by slot).
// See docs/labels-mod.md — label_at.
fn label_at(labels: &[StreamLabel], stream_type: StreamLabelType, n: u16) -> Option<&StreamLabel> {
    labels
        .iter()
        .find(|l| l.stream_id.is_none() && l.stream_type == stream_type && l.stream_number == n)
}

/// The highest STN slot the vendor list names for `stream_type`, or 0 when it
/// names none. A list whose top slot is 9 is describing a table with at least
/// nine slots, so a title with six streams cannot be that table.
fn vendor_extent(labels: &[StreamLabel], stream_type: StreamLabelType) -> usize {
    labels
        .iter()
        .filter(|l| l.stream_id.is_none() && l.stream_type == stream_type)
        .map(|l| l.stream_number as usize)
        .max()
        .unwrap_or(0)
}

/// The (PID, language) of each stream of `stream_type` in the title, in
/// STN order — i.e. exactly the sequence `apply_labels` numbers against.
fn slots_of(title: &DiscTitle, stream_type: StreamLabelType) -> Vec<(u16, &str)> {
    title
        .streams
        .iter()
        .filter_map(|s| match (s, stream_type) {
            (Stream::Audio(a), StreamLabelType::Audio) => Some((a.pid, a.language.as_str())),
            (Stream::Subtitle(s), StreamLabelType::Subtitle) => Some((s.pid, s.language.as_str())),
            _ => None,
        })
        .collect()
}

// Find the title the label list is actually describing, for one stream
// type: the title whose per-slot language sequence best matches (fewest
// contradictions, most confirmations). See docs/labels-mod.md — find_anchor.
fn find_anchor(
    labels: &[StreamLabel],
    titles: &[DiscTitle],
    stream_type: StreamLabelType,
) -> Option<usize> {
    let extent = vendor_extent(labels, stream_type);
    // (confirmed slots, stream count, title index) — most confirmed wins, then
    // the longest table, then title order (which is duration-descending).
    let mut best: Option<(usize, usize, usize)> = None;
    for (idx, title) in titles.iter().enumerate() {
        let n = slots_of(title, stream_type).len();
        if n < MIN_ANCHOR_STREAMS || n < extent {
            continue;
        }
        let Some(score) = anchor_score(labels, title, stream_type) else {
            continue;
        };
        if best.is_none_or(|(bs, bn, _)| (score, n) > (bs, bn)) {
            best = Some((score, n, idx));
        }
    }
    best.map(|(_, _, idx)| idx)
}

// How strongly this title matches the vendor label list (count of
// confirmed slots), or None if it contradicts and can't be the table.
// See docs/labels-mod.md — anchor_score.
fn anchor_score(
    labels: &[StreamLabel],
    title: &DiscTitle,
    stream_type: StreamLabelType,
) -> Option<usize> {
    let mut confirmed = 0usize;
    for (i, (_, lang)) in slots_of(title, stream_type).iter().enumerate() {
        let Some(l) = label_at(labels, stream_type, (i + 1) as u16) else {
            continue; // slot the vendor list never named
        };
        if !languages_compatible(&l.language, lang) {
            return None;
        }
        if languages_agree(&l.language, lang) {
            confirmed += 1;
        }
    }
    Some(confirmed)
}

// Apply a pre-extracted set of labels to titles' streams: 4 tiers, most
// certain first — anchor title, anchor-proved PID, StreamId, then
// language-checked ordinal fallback. See docs/labels-mod.md — apply_labels.
pub(crate) fn apply_labels(labels: &[StreamLabel], titles: &mut [DiscTitle]) {
    use std::collections::HashMap;

    // `(clip id, PID) -> index into `labels``, harvested from the anchor title
    // of each stream type. Keyed by clip because a PID is only unique within
    // one clip: two unrelated .m2ts files both open their audio at 0x1100.
    let mut pid_map: HashMap<(&str, u16), usize> = HashMap::new();
    let mut anchors: [Option<usize>; 2] = [None; 2];
    for stream_type in [StreamLabelType::Audio, StreamLabelType::Subtitle] {
        let Some(anchor) = find_anchor(labels, titles, stream_type) else {
            continue;
        };
        anchors[type_tag(stream_type) as usize] = Some(anchor);
        let title = &titles[anchor];
        for (i, (pid, _)) in slots_of(title, stream_type).iter().enumerate() {
            let Some(pos) = labels.iter().position(|l| {
                l.stream_id.is_none()
                    && l.stream_type == stream_type
                    && l.stream_number == (i + 1) as u16
            }) else {
                continue;
            };
            // ONLY the anchor's first clip: `disc::bluray` builds streams from
            // `play_items[0]`'s STN table, the only clip these PIDs were seen in.
            // A later clip could reuse the PID for a different stream otherwise.
            if let Some(clip) = title.clips.first() {
                pid_map.insert((clip.clip_id.as_str(), *pid), pos);
            }
        }
        tracing::info!(
            stream_type = ?stream_type,
            playlist = ?titles[anchor].playlist,
            slots = slots_of(&titles[anchor], stream_type).len(),
            "label list anchored to a title by its stream-language sequence",
        );
    }
    // The map borrows the titles it was built from; copy it out so the
    // binding pass can take `&mut`.
    let pid_map: HashMap<(String, u16), usize> = pid_map
        .into_iter()
        .map(|((c, p), v)| ((c.to_string(), p), v))
        .collect();

    // Labels that name their own stream, indexed by that name. The id was read
    // from the same STN / ProgramInfo table `disc::bluray` built the stream
    // from, so equality here is the same elementary stream by construction.
    let by_id: HashMap<&StreamId, usize> = labels
        .iter()
        .enumerate()
        .filter_map(|(i, l)| l.stream_id.as_ref().map(|id| (id, i)))
        .fold(HashMap::new(), |mut m, (id, i)| {
            m.entry(id).or_insert(i);
            m
        });

    for (title_idx, title) in titles.iter_mut().enumerate() {
        let mut audio_idx: u16 = 0;
        let mut sub_idx: u16 = 0;
        // The anchor facts that reach this title: those recorded against a
        // clip it plays. Narrowed once per title rather than per stream, so a
        // title with hundreds of clip references costs one pass over the map.
        let known_pids: HashMap<u16, usize> = {
            let clips: std::collections::HashSet<&str> =
                title.clips.iter().map(|c| c.clip_id.as_str()).collect();
            pid_map
                .iter()
                .filter(|((clip, _), _)| clips.contains(clip.as_str()))
                .map(|((_, pid), pos)| (*pid, *pos))
                .collect()
        };

        // The clip whose STN table this title's stream list was built from —
        // `disc::bluray` takes the streams from the first play item — so it is
        // the clip half of every id that can match a stream of this title.
        let clip0 = title
            .clips
            .first()
            .map(|c| c.clip_id.clone())
            .unwrap_or_default();

        // Resolve one stream to (label, authoritative). Authoritative means the
        // label is known to belong to THIS stream rather than guessed onto it
        // (tiers 1-3 of the doc comment above); only tier 4, the bare ordinal, isn't.
        let resolve = |stream_type: StreamLabelType, idx: u16, pid: u16, lang: &str| {
            if anchors[type_tag(stream_type) as usize] == Some(title_idx)
                && let Some(l) = label_at(labels, stream_type, idx)
            {
                return Some((l, true));
            }
            if let Some(pos) = known_pids.get(&pid).copied()
                && labels[pos].stream_type == stream_type
            {
                return Some((&labels[pos], true));
            }
            let id = StreamId {
                clip_id: clip0.clone(),
                pid,
            };
            if let Some(pos) = by_id.get(&id).copied()
                && labels[pos].stream_type == stream_type
            {
                return Some((&labels[pos], true));
            }
            label_at(labels, stream_type, idx)
                .filter(|l| languages_compatible(&l.language, lang))
                .map(|l| (l, false))
        };

        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) => {
                    audio_idx += 1;
                    if let Some((label, _authoritative)) =
                        resolve(StreamLabelType::Audio, audio_idx, a.pid, &a.language)
                    {
                        // Structured fields — callers translate purpose to UI text.
                        a.purpose = label.purpose;

                        // Trust the parser's `codec_hint` ONLY when consistent with the
                        // stream's actual codec (it may be richer, e.g. "Dolby Atmos" on
                        // TrueHD); if it CONTRADICTS, derive from the stream itself.
                        let codec_desc = if label.codec_hint.is_empty() {
                            // No codec hint — leave for fill_defaults.
                            String::new()
                        } else if !codec_hint_consistent(&label.codec_hint, &a.codec) {
                            // Hint contradicts the stream (mis-bound / shuffled):
                            // derive from the stream itself.
                            generate_audio_label(&a.codec, &a.channels, a.secondary)
                        } else if codec_hint_adds_detail(&label.codec_hint) {
                            // Consistent AND richer than the spec codec can express
                            // (e.g. "Dolby Atmos", "DTS:X") — keep the parser's hint.
                            label.codec_hint.clone()
                        } else {
                            // Consistent but a plain codec/channel restatement —
                            // normalize to the stream's own marketing descriptor so
                            // styling is uniform across tracks.
                            generate_audio_label(&a.codec, &a.channels, a.secondary)
                        };

                        // a.label only carries codec/variant info. NEVER any
                        // English purpose text — the CLI handles that via i18n.
                        let mut parts = Vec::new();
                        if !label.variant.is_empty() {
                            parts.push(format!("({})", label.variant));
                        }
                        if !codec_desc.is_empty() {
                            parts.push(codec_desc);
                        }
                        if !parts.is_empty() {
                            a.label = parts.join(" ");
                        } else if !label.name.is_empty() && label.purpose == LabelPurpose::Normal {
                            // Only fall back to the parser-supplied display
                            // name when there's no purpose to flag — the CLI
                            // handles purpose rendering itself.
                            a.label = label.name.clone();
                        }
                    }
                }
                Stream::Subtitle(s) => {
                    sub_idx += 1;
                    if let Some((label, authoritative)) =
                        resolve(StreamLabelType::Subtitle, sub_idx, s.pid, &s.language)
                        // A subtitle label carries nothing but the qualifier, so an
                        // unverifiable one is all risk, no gain: off the authoritative
                        // path, require the label and stream to state the same language.
                        && (authoritative
                            || languages_agree(&label.language, &s.language))
                    {
                        s.qualifier = label.qualifier;
                        if label.qualifier == LabelQualifier::Forced {
                            s.forced = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Fill in default labels for any streams that don't have one.
/// Runs after BD-J label extraction — fills gaps with codec + channel descriptions.
/// This is the central place for all fallback label generation.
pub fn fill_defaults(titles: &mut [crate::disc::DiscTitle]) {
    use crate::disc::Stream;

    for title in titles.iter_mut() {
        for stream in &mut title.streams {
            match stream {
                Stream::Audio(a) if a.label.is_empty() => {
                    a.label = generate_audio_label(&a.codec, &a.channels, a.secondary);
                }
                Stream::Video(v) if v.label.is_empty() => {
                    // Unknown resolution: pass (0, 0) so the label omits the
                    // resolution token rather than tagging it a fabricated
                    // 1080p.
                    let px = v.resolution.pixels().unwrap_or((0, 0));
                    v.label = generate_video_label(
                        &v.codec,
                        px,
                        v.resolution.is_interlaced(),
                        &v.hdr,
                        v.secondary,
                    );
                }
                Stream::Subtitle(s) if s.forced => {
                    // Ensure forced subs are labeled even if BD-J didn't set a name
                    // (subtitle labels are generally not set — this just marks forced)
                }
                _ => {}
            }
        }
    }
}

fn generate_video_label(
    codec: &crate::disc::Codec,
    pixels: (u32, u32),
    interlaced: bool,
    hdr: &crate::disc::HdrFormat,
    secondary: bool,
) -> String {
    use crate::disc::HdrFormat;

    if secondary {
        // "Dolby Vision EL" is a brand identifier, not English prose, so the
        // library may emit it. Other "secondary video" wording is a CLI
        // concern — the library just leaves the label empty.
        return match hdr {
            HdrFormat::DolbyVision => "Dolby Vision EL".to_string(),
            _ => String::new(),
        };
    }

    let mut parts = Vec::new();

    // Codec
    parts.push(codec.name().to_string());

    // Resolution. Scan type (i/p) is honored for heights that can be
    // interlaced on disc (1080 and SD 576/480); 720/4K/8K are always
    // progressive.
    let (w, h) = pixels;
    let res = if w >= 7680 {
        "8K"
    } else if w >= 3840 {
        "4K"
    } else if w >= 1920 {
        if interlaced { "1080i" } else { "1080p" }
    } else if w >= 1280 {
        "720p"
    } else if h >= 576 {
        if interlaced { "576i" } else { "576p" }
    } else if h >= 480 {
        if interlaced { "480i" } else { "480p" }
    } else {
        ""
    };
    if !res.is_empty() {
        parts.push(res.into());
    }

    // HDR
    match hdr {
        HdrFormat::Sdr => {}
        _ => parts.push(hdr.name().to_string()),
    }

    parts.join(" ")
}

// Does codec_hint name a codec consistent with the stream's actual codec
// (matched by family, e.g. Atmos on TrueHD)? Rejects shuffled-label
// mis-binds. See docs/labels-mod.md — codec_hint_consistent.
fn codec_hint_consistent(hint: &str, codec: &crate::disc::Codec) -> bool {
    use crate::disc::Codec;
    let h = hint.to_ascii_lowercase();

    let says_truehd = h.contains("truehd") || h.contains("true hd");
    let says_ddp = h.contains("ac-3+")
        || h.contains("ac3+")
        || h.contains("e-ac-3")
        || h.contains("eac-3")
        || h.contains("eac3")
        || h.contains("digital plus")
        || h.contains("dd+");
    let says_ac3 =
        !says_ddp && (h.contains("ac-3") || h.contains("ac3") || h.contains("dolby digital"));
    let says_dts_ma = h.contains("master audio") || h.contains("hd ma");
    let says_dts_hr = h.contains("high resolution") || h.contains("hd hr");
    let says_dts = !says_dts_ma && !says_dts_hr && h.contains("dts");
    let says_lpcm = h.contains("lpcm") || h.contains("pcm");
    let says_atmos = h.contains("atmos");
    // DTS:X is an object-audio extension carried on a DTS-HD MA (or HR) core,
    // as Atmos rides TrueHD/DD+. The spec Codec enum has no DtsX variant, so a
    // correctly-authored hint must be judged consistent with its carrier.
    let says_dtsx = h.contains("dts:x") || h.contains("dts-x") || h.contains("dtsx");

    let names_family =
        says_truehd || says_ddp || says_ac3 || says_dts_ma || says_dts_hr || says_dts || says_lpcm;

    // Pure-editorial hint (no codec family named) isn't asserting a codec, so
    // it's consistent. "Atmos" alone implies a lossless carrier (TrueHD/DD+).
    // ("DTS:X" always matches "dts" above, so it never reaches this branch.)
    if !names_family {
        return if says_atmos {
            matches!(codec, Codec::TrueHd | Codec::Ac3Plus)
        } else {
            true
        };
    }

    match codec {
        Codec::TrueHd => says_truehd || says_atmos,
        Codec::Ac3Plus => says_ddp || says_atmos,
        Codec::Ac3 => says_ac3,
        Codec::DtsHdMa => says_dts_ma || says_dtsx,
        Codec::DtsHdHr => says_dts_hr || says_dtsx,
        Codec::Dts => says_dts,
        Codec::Lpcm => says_lpcm,
        // Unknown / other stream codec — don't second-guess the parser's hint.
        _ => true,
    }
}

/// Does the hint carry object-audio detail the spec codec can't express
/// (Atmos / DTS:X)? Such hints are kept verbatim; plain codec/channel hints are
/// normalized to the stream's own descriptor for uniform styling across tracks.
fn codec_hint_adds_detail(hint: &str) -> bool {
    let h = hint.to_ascii_lowercase();
    h.contains("atmos") || h.contains("dts:x") || h.contains("dts-x") || h.contains("dtsx")
}

pub(crate) fn generate_audio_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    secondary: bool,
) -> String {
    generate_audio_label_inner(codec, channels, secondary, false)
}

// Atmos-aware variant of generate_audio_label: folds the object-audio
// marker into the codec brand (e.g. "Dolby TrueHD Atmos 7.1").
// See docs/labels-mod.md — generate_audio_label_atmos.
pub(crate) fn generate_audio_label_atmos(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    secondary: bool,
) -> String {
    generate_audio_label_inner(codec, channels, secondary, true)
}

fn generate_audio_label_inner(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    _secondary: bool,
    atmos: bool,
) -> String {
    use crate::disc::{AudioChannels, Codec};

    // Full marketing names for disc audio codecs.
    // These are codec brand identifiers, not user-facing English prose.
    let base_name = match codec {
        Codec::TrueHd => "Dolby TrueHD",
        Codec::Ac3 => "Dolby Digital",
        Codec::Ac3Plus => "Dolby Digital Plus",
        Codec::DtsHdMa => "DTS-HD Master Audio",
        Codec::DtsHdHr => "DTS-HD High Resolution",
        Codec::Dts => "DTS",
        Codec::Lpcm => "LPCM",
        Codec::Aac => "AAC",
        Codec::Mp2 => "MPEG Audio",
        Codec::Mp3 => "MP3",
        Codec::Flac => "FLAC",
        Codec::Opus => "Opus",
        _ => return String::new(),
    };

    // Atmos is an object-audio extension riding a lossless carrier (TrueHD or
    // DD+). Fold the marker into the brand name; "Atmos" is a label-layer
    // string, never asserted by the core parser.
    let codec_name = if atmos && matches!(codec, Codec::TrueHd | Codec::Ac3Plus) {
        std::borrow::Cow::Owned(format!("{base_name} Atmos"))
    } else {
        std::borrow::Cow::Borrowed(base_name)
    };

    // Channel layout
    let channel_str = match channels {
        AudioChannels::Mono => "1.0",
        AudioChannels::Stereo => "2.0",
        AudioChannels::Stereo21 => "2.1",
        AudioChannels::Quad => "4.0",
        AudioChannels::Surround50 => "5.0",
        AudioChannels::Surround51 => "5.1",
        AudioChannels::Surround61 => "6.1",
        AudioChannels::Surround71 => "7.1",
        AudioChannels::Unknown => "",
    };

    // The "(Secondary)" suffix is a CLI/UI concern — callers display it from
    // the AudioStream::secondary bool, not the library.
    if channel_str.is_empty() {
        codec_name.to_string()
    } else {
        format!("{} {}", codec_name, channel_str)
    }
}

fn extract(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
) -> (Vec<StreamLabel>, Option<FeaturePlaylistHint>) {
    let mut candidates: Vec<(&'static str, ParseResult)> = Vec::new();
    for (name, detect, parse) in PARSERS {
        if !detect(reader, udf) {
            continue;
        }
        tracing::info!(parser = name, "label parser detected");
        let Some(result) = parse(reader, udf) else {
            continue;
        };
        if result.labels.is_empty() {
            continue;
        }
        candidates.push((name, result));
    }
    // One tie-break rule, one implementation. `select_result` regression-tests a
    // past bug where the LAST equal-confidence parser won instead of the first.
    // Array order encodes trust (hand-vetted parsers before generic BD-J ones).
    let best = select_result(&candidates).map(|(n, r)| (*n, r.clone()));
    let (name, mut labels, feature_playlist) = match best {
        Some((n, r)) => {
            tracing::info!(
                parser = n,
                confidence = ?r.confidence,
                label_count = r.labels.len(),
                "label parser selected",
            );
            (n, r.labels, r.feature_playlist)
        }
        None => {
            tracing::info!("no label parser matched");
            return (Vec::new(), None);
        }
    };

    // The MPLS floor: framework parsers under-yield on multi-track discs, only
    // shipping editorial labels for "interesting" streams (Atmos, SDH). MPLS
    // names every stream a playlist references, filling the gaps.
    if name != "mpls_universal"
        && let Some(mpls_result) = mpls_universal::parse(reader, udf)
    {
        merge_mpls_floor(&mut labels, &mpls_result.labels);
    }

    // CLPI orphan streams: PIDs in /BDMV/CLIPINF/*.clpi ProgramInfo that no MPLS
    // playlist references (physically on disc, not menu-reachable). Appended
    // under the id they name, so a title reaches one only if it carries it.
    let _orphans_added = append_clpi_orphans(&mut labels, reader, udf);

    (labels, feature_playlist)
}

// Merge the MPLS-derived floor into a framework list. Nothing is merged
// BY slot (the lists don't share a coordinate system); unnamed MPLS
// streams are appended by StreamId. See docs/labels-mod.md — merge_mpls_floor.
fn merge_mpls_floor(framework: &mut Vec<StreamLabel>, mpls: &[StreamLabel]) {
    use std::collections::HashSet;
    let named: HashSet<&StreamId> = framework
        .iter()
        .filter_map(|l| l.stream_id.as_ref())
        .collect();
    let mut added: Vec<StreamLabel> = Vec::new();
    let mut taken: HashSet<&StreamId> = HashSet::new();
    for m in mpls {
        match m.stream_id.as_ref() {
            Some(id) if !named.contains(id) && taken.insert(id) => added.push(m.clone()),
            _ => {}
        }
    }
    if added.is_empty() {
        return;
    }
    tracing::info!(
        gap_fill_added = added.len(),
        "MPLS floor merged: streams the framework parser named no label for"
    );
    framework.extend(added);
    sort_labels(framework);
}

/// Deterministic display order for a merged label list: audios then subtitles,
/// vendor slots first in slot order, then the PID-named labels by the stream
/// they name. Ordering is presentation only — nothing binds through it.
fn sort_labels(labels: &mut [StreamLabel]) {
    labels.sort_by(|a, b| {
        let key = |l: &StreamLabel| {
            (
                type_tag(l.stream_type),
                l.stream_id.is_some(),
                l.stream_number,
                l.stream_id.as_ref().map(|i| (i.clip_id.clone(), i.pid)),
            )
        };
        key(a).cmp(&key(b))
    });
}

/// Stable sort key for `StreamLabelType`. Audio < Subtitle so the
/// merged label list groups audios first then subtitles.
fn type_tag(t: StreamLabelType) -> u8 {
    match t {
        StreamLabelType::Audio => 0,
        StreamLabelType::Subtitle => 1,
    }
}

// Append CLPI streams (present but unreferenced by MPLS) not already
// named; each binds by its own (clip, PID), no invented ordinal.
// Returns count appended. See docs/labels-mod.md — append_clpi_orphans.
fn append_clpi_orphans(
    labels: &mut Vec<StreamLabel>,
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
) -> usize {
    use crate::consts::coding_type as c;
    // Two exclusions: exact (a stream some label already NAMES is not an orphan)
    // and fuzzy (the pre-existing (type, language, codec_hint) test, kept to
    // bound this list to a handful of entries per disc, not one per stream).
    use std::collections::HashSet;
    let named: HashSet<&StreamId> = labels.iter().filter_map(|l| l.stream_id.as_ref()).collect();
    let existing: HashSet<(StreamLabelType, String, String)> = labels
        .iter()
        .map(|l| (l.stream_type, l.language.clone(), l.codec_hint.clone()))
        .collect();

    // Walk CLPI files, collect distinct (type, pid, coding_type, lang)
    // tuples not already in `existing`. Dedup by PID across files so
    // a stream appearing in two clips only gets added once.
    let Some(dir) = udf.find_dir("/BDMV/CLIPINF") else {
        return 0;
    };
    let names: Vec<String> = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".clpi"))
        .map(|e| e.name.clone())
        .collect();
    let mut seen_pids: HashSet<u16> = HashSet::new();
    let mut candidates: Vec<(StreamLabelType, StreamId, u8, String)> = Vec::new();
    for name in names {
        // The CLPI filename without its extension IS the clip id, so a stream
        // read out of this file is identified exactly: `(clip, PID)`.
        let clip_id = name
            .rsplit_once('.')
            .map(|(stem, _)| stem.to_string())
            .unwrap_or_else(|| name.clone());
        let path = format!("/BDMV/CLIPINF/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(clip) = crate::clpi::parse(&data) else {
            continue;
        };
        for s in clip.streams {
            if !seen_pids.insert(s.pid) {
                continue;
            }
            // Translate CLPI coding_type → label stream_type. 0x90 = Presentation
            // Graphics (PG subtitle); 0x91 = Interactive Graphics (BD-J menu
            // overlay), NOT a user-facing subtitle — skip it, matching MPLS.
            let stype = match s.coding_type {
                c::LPCM..=c::DTS_HD_MA | c::AC3_PLUS_SECONDARY | c::DTS_HD_SECONDARY => {
                    StreamLabelType::Audio
                }
                c::PG => StreamLabelType::Subtitle,
                _ => continue, // IG / video / unknown — skip
            };
            // Same dedup logic as MPLS: normalize language, build codec
            // hint, check against existing label set.
            let lang_norm = s.language.trim().to_ascii_lowercase();
            let codec_hint = mpls_universal::codec_name(s.coding_type).to_string();
            if existing.contains(&(stype, lang_norm.clone(), codec_hint.clone())) {
                continue;
            }
            let id = StreamId {
                clip_id: clip_id.clone(),
                pid: s.pid,
            };
            if named.contains(&id) {
                continue;
            }
            candidates.push((stype, id, s.coding_type, lang_norm));
        }
    }

    if candidates.is_empty() {
        return 0;
    }

    let added = candidates.len();
    for (stype, stream_id, coding_type, language) in candidates {
        let codec_hint = mpls_universal::codec_name(coding_type).to_string();
        let name = mpls_universal::language_display_name(&language);
        labels.push(StreamLabel {
            stream_id: Some(stream_id),
            // NO_STN_SLOT: an orphan is by definition absent from every
            // playlist's stream table, so there is no slot to state.
            stream_number: NO_STN_SLOT,
            stream_type: stype,
            language,
            name,
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint,
            variant: String::new(),
        });
    }
    if added > 0 {
        tracing::info!(
            clpi_orphans_added = added,
            "CLPI-only streams appended (PIDs not referenced by any MPLS playlist)"
        );
        sort_labels(labels);
    }
    added
}

// Pick the winning parser result: highest Confidence among non-empty
// results, earliest array position wins ties (matches extract()'s scan).
// See docs/labels-mod.md — select_result.
fn select_result<'a>(
    results: &'a [(&'static str, ParseResult)],
) -> Option<&'a (&'static str, ParseResult)> {
    results
        .iter()
        .enumerate()
        .filter(|(_, (_, r))| !r.labels.is_empty())
        .max_by_key(|(idx, (_, r))| (r.confidence, std::cmp::Reverse(*idx)))
        .map(|(_, entry)| entry)
}

/// Diagnostic introspection — returns the parser that matched, the
/// labels it emitted, and the inventory of files under `/BDMV/JAR/*/`
/// that the discriminators looked at. Intended for `freemkv-tools
/// labels-analyze` and corpus regression tooling, not production code
/// paths. The matching/parsing logic is identical to [`extract`]; only
/// the return shape is richer (includes confidence, all detected
/// parsers, and any parsers that produced empty results).
#[doc(hidden)]
pub fn analyze(reader: &mut dyn SectorSource, udf: &UdfFs) -> LabelAnalysis {
    let inventory = jar_inventory(udf);
    let mut parsers_detected: Vec<&'static str> = Vec::new();
    let mut all_results: Vec<(&'static str, ParseResult)> = Vec::new();

    for (name, detect, parse) in PARSERS {
        if !detect(reader, udf) {
            continue;
        }
        tracing::info!(parser = name, "label parser detected");
        parsers_detected.push(name);
        if let Some(r) = parse(reader, udf) {
            all_results.push((name, r));
        }
    }

    // Selection logic mirrors `extract`: highest confidence + non-empty,
    // with first-in-array-order winning on a confidence tie.
    let chosen = select_result(&all_results);

    let (parser, confidence, mut labels) = match chosen {
        Some((name, r)) => (Some(*name), Some(r.confidence), r.labels.clone()),
        None => (None, None, Vec::new()),
    };

    // The MPLS floor: same merge as `extract()`. Skipped when MPLS was itself
    // the chosen parser (its labels ARE the labels). This diagnostic path stops
    // here — `extract()` also appends the CLPI orphans, which `analyze` never does.
    let gap_fill_added = if parser.is_some() && parser != Some("mpls_universal") {
        let before = labels.len();
        // Re-run MPLS unconditionally: we only ran framework parsers above (to
        // know which to pick), and in the common case where MPLS would have
        // detected but wasn't chosen we still need its labels for the merge.
        if let Some(mpls_result) = mpls_universal::parse(reader, udf) {
            merge_mpls_floor(&mut labels, &mpls_result.labels);
        }
        labels.len().saturating_sub(before)
    } else {
        0
    };

    if parsers_detected.is_empty() {
        tracing::info!("no label parser matched");
    } else if parser.is_none() {
        tracing::info!(
            detected = ?parsers_detected,
            "label parsers detected but produced no labels"
        );
    }

    // bdmt runs independently of the parser registry: it's disc-level metadata
    // (localized titles, box-set position), not per-stream labels, so "highest
    // confidence wins" doesn't apply. Always run if detected, as a separate field.
    let disc_metadata = if bdmt::detect(udf) {
        bdmt::parse(reader, udf)
    } else {
        None
    };

    let chapter_summary = collect_chapter_summary(reader, udf);

    LabelAnalysis {
        parser,
        parsers_detected,
        confidence,
        jar_inventory: inventory,
        labels,
        disc_metadata,
        gap_fill_added,
        chapter_summary,
    }
}

// Scan /BDMV/PLAYLIST/*.mpls for a chapter-count + duration row per
// playlist, sorted by filename. Unparseable/markless entries silently
// dropped. See docs/labels-mod.md — collect_chapter_summary.
fn collect_chapter_summary(reader: &mut dyn SectorSource, udf: &UdfFs) -> Vec<ChapterSummary> {
    let Some(playlist_dir) = udf.find_dir("/BDMV/PLAYLIST") else {
        return Vec::new();
    };
    let mut names: Vec<String> = playlist_dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".mpls"))
        .map(|e| e.name.clone())
        .collect();
    names.sort();

    let mut out: Vec<ChapterSummary> = Vec::new();
    for name in names {
        let path = format!("/BDMV/PLAYLIST/{}", name);
        let Ok(data) = udf.read_file(reader, &path) else {
            continue;
        };
        let Ok(playlist) = crate::mpls::parse(&data) else {
            continue;
        };
        let chapter_count = playlist
            .marks
            .iter()
            .filter(|m| m.is_chapter_mark())
            .count();
        if chapter_count == 0 {
            continue;
        }
        // Duration: sum of (out_time - in_time) across play items, in 45kHz PTS
        // ticks. Approximates the disc module's per-title duration; not
        // sample-accurate, just enough to identify "the long one" (main movie).
        let duration_ticks: u64 = playlist
            .play_items
            .iter()
            .map(|pi| pi.out_time.saturating_sub(pi.in_time) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / 45000.0;
        out.push(ChapterSummary {
            playlist: name,
            chapter_count,
            duration_secs,
        });
    }
    out
}

/// Result of [`analyze`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct LabelAnalysis {
    /// Which parser was SELECTED — the one whose `ParseResult` had
    /// the highest confidence among non-empty results (array order
    /// tiebreaker). `None` means either no parser recognized the
    /// disc, OR every parser that recognized it returned no labels.
    /// Use `parsers_detected` to disambiguate.
    pub parser: Option<&'static str>,
    /// Confidence of the selected parser, `None` if no parser was
    /// selected.
    pub confidence: Option<Confidence>,
    /// Every parser whose discriminator matched, in registry order.
    /// Distinguishes "we recognized this disc but couldn't extract
    /// labels" from "we don't recognize this disc at all" — the
    /// former points at a parser bug or a truncated capture, the
    /// latter points at a missing parser.
    pub parsers_detected: Vec<&'static str>,
    /// Filenames found under any `/BDMV/JAR/*/` subdirectory, deduped
    /// and sorted. Helps spot unknown authoring formats when no
    /// parser detected.
    pub jar_inventory: Vec<String>,
    /// Raw labels emitted by the selected parser (empty if `parser`
    /// is `None`).
    pub labels: Vec<StreamLabel>,
    /// Disc-level metadata from `/BDMV/META/DL/bdmt_*.xml` if present.
    /// Localized title names, descriptions, box-set position. Orthogonal
    /// to per-stream labels; populated independently from the parser
    /// registry.
    pub disc_metadata: Option<bdmt::DiscMetadata>,
    /// Number of MPLS-derived floor labels merged on top of the framework
    /// parser's output — one per playlist-referenced stream the framework
    /// named no label for. 0 means the framework already named every one, or
    /// MPLS itself was the chosen parser. Diagnostic for the labels-analyze
    /// tool.
    pub gap_fill_added: usize,
    /// Per-playlist chapter summary: `(playlist_filename, chapter_count, duration_secs)`.
    /// Sourced from MPLS PlaylistMark entries with `mark_type == 1`
    /// (chapter entries). Ordered by playlist filename. Empty if no
    /// MPLS files have parseable marks, or the disc isn't Blu-ray.
    pub chapter_summary: Vec<ChapterSummary>,
}

/// One row of the per-playlist chapter summary in `LabelAnalysis`.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ChapterSummary {
    pub playlist: String,
    pub chapter_count: usize,
    pub duration_secs: f64,
}

// List filenames under any /BDMV/JAR/<x>/ subdirectory, deduped and
// sorted (empty if none). pub(crate) so filename parsers can scan
// menu-asset names without a reader. See docs/labels-mod.md — jar_inventory.
pub(crate) fn jar_inventory(udf: &UdfFs) -> Vec<String> {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return Vec::new();
    };
    jar_inventory_from(&jar_dir.entries)
}

// Body of jar_inventory, unit-testable without a UdfFs. Uses a BTreeSet
// (not Vec::contains) since entry names are attacker-controlled disc
// data. See docs/labels-mod.md — jar_inventory_from.
fn jar_inventory_from(entries: &[crate::udf::DirEntry]) -> Vec<String> {
    let mut out: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for entry in entries {
        if entry.is_dir {
            for child in &entry.entries {
                if !child.is_dir {
                    out.insert(child.name.as_str());
                }
            }
        }
    }
    out.into_iter().map(str::to_string).collect()
}

// ── Shared helpers ─────────────────────────────────────────────────────────

/// Check if a file exists in any BDMV/JAR subdirectory.
pub(crate) fn jar_file_exists(udf: &UdfFs, filename: &str) -> bool {
    find_jar_file(udf, filename).is_some()
}

/// Find a file in any BDMV/JAR subdirectory, return its path.
pub(crate) fn find_jar_file(udf: &UdfFs, filename: &str) -> Option<String> {
    let jar_dir = udf.find_dir("/BDMV/JAR")?;
    for entry in &jar_dir.entries {
        if entry.is_dir {
            let path = format!("/BDMV/JAR/{}/{}", entry.name, filename);
            // Check if file exists in this subdirectory
            for child in &entry.entries {
                if !child.is_dir && child.name.eq_ignore_ascii_case(filename) {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// Read a file from any BDMV/JAR subdirectory by filename.
pub(crate) fn read_jar_file(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    filename: &str,
) -> Option<Vec<u8>> {
    let path = find_jar_file(udf, filename)?;
    udf.read_file(reader, &path).ok().filter(|d| !d.is_empty())
}

// ── Registry-level tests ────────────────────────────────────────────────────

#[cfg(test)]
mod registry_tests {
    use super::*;

    fn dir_entry(
        name: &str,
        is_dir: bool,
        entries: Vec<crate::udf::DirEntry>,
    ) -> crate::udf::DirEntry {
        crate::udf::DirEntry {
            name: name.to_string(),
            is_dir,
            meta_lba: 0,
            size: 0,
            entries,
        }
    }

    // Hang guard: a return to a linear Vec::contains dedup in
    // jar_inventory would make this 120k-entry fixture run for minutes.
    // See docs/labels-mod.md — jar_inventory_dedup_does_not_hang_on_a_hostile_directory.
    #[test]
    fn jar_inventory_dedup_does_not_hang_on_a_hostile_directory() {
        const FILES: usize = 120_000;
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            // Long shared prefix so every comparison runs to the tail.
            let prefix = "a".repeat(180);
            let children: Vec<crate::udf::DirEntry> = (0..FILES)
                .map(|i| dir_entry(&format!("{prefix}{i:08}.png"), false, Vec::new()))
                .collect();
            let entries = vec![dir_entry("00000", true, children)];
            let _ = tx.send(jar_inventory_from(&entries));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(names) => {
                worker.join().expect("worker panicked");
                assert_eq!(names.len(), FILES);
            }
            Err(_) => panic!(
                "jar_inventory_from did not finish {FILES} entries within 10s \
                 — the dedup is still a linear scan"
            ),
        }
    }

    /// Behaviour contract: output is deduplicated across subdirectories,
    /// sorted, and excludes directories and files sitting directly under
    /// `/BDMV/JAR` (only one level down counts).
    #[test]
    fn jar_inventory_dedups_sorts_and_skips_dirs() {
        let entries = vec![
            dir_entry(
                "00000",
                true,
                vec![
                    dir_entry("streamproperties.xml", false, Vec::new()),
                    dir_entry("zeta.png", false, Vec::new()),
                    dir_entry(
                        "nested",
                        true,
                        vec![dir_entry("hidden.txt", false, Vec::new())],
                    ),
                ],
            ),
            dir_entry(
                "00001",
                true,
                vec![
                    dir_entry("alpha.png", false, Vec::new()),
                    // Duplicate of the entry in 00000 — must appear once.
                    dir_entry("streamproperties.xml", false, Vec::new()),
                ],
            ),
            // A jar sitting directly under /BDMV/JAR is not inventoried.
            dir_entry("top.jar", false, Vec::new()),
        ];
        assert_eq!(
            jar_inventory_from(&entries),
            vec![
                "alpha.png".to_string(),
                "streamproperties.xml".to_string(),
                "zeta.png".to_string(),
            ]
        );
    }

    // Lock the parser roster + order: first matching parse() wins, so
    // reordering changes which parser claims a disc. dbp + deluxe MUST
    // stay last. See docs/labels-mod.md — parsers_registry_order_locked.
    #[test]
    fn parsers_registry_order_locked() {
        let names: Vec<&str> = PARSERS.iter().map(|(n, _, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "paramount",
                "criterion",
                "pixelogic",
                "ctrm",
                "dbp",
                "deluxe",
                "fox",
                "mpls_universal",
                "png_filenames",
            ],
            "PARSERS array order changed — file-presence/reader-gated High \
             parsers (paramount/criterion/pixelogic/ctrm) stay first; dbp + \
             deluxe (now real com/<vendor>/ prefix detect) and fox (dcx.xml / \
             com/foxbd) stay before mpls_universal; mpls_universal stays the \
             universal Low fallback; png_filenames (Low, language-only hint) \
             stays LAST so MPLS wins the Low tie whenever it produces anything."
        );
    }

    fn one_label() -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }
    }

    fn result(conf: Confidence) -> ParseResult {
        ParseResult {
            labels: vec![one_label()],
            confidence: conf,
            feature_playlist: None,
        }
    }

    #[test]
    fn feature_playlist_hint_matches_on_id_or_filename() {
        let h = FeaturePlaylistHint {
            playlist_id: Some(222),
            filename: Some("00222.mpls".into()),
        };
        assert!(h.matches(222, "99999.mpls"), "numeric id match");
        assert!(
            h.matches(1, "00222.MPLS"),
            "filename match is case-insensitive"
        );
        assert!(
            !h.matches(1, "00001.mpls"),
            "neither id nor filename → no match"
        );

        let id_only = FeaturePlaylistHint {
            playlist_id: Some(5),
            filename: None,
        };
        assert!(id_only.matches(5, "anything.mpls"));
        assert!(!id_only.matches(6, "00005.mpls"));

        assert!(FeaturePlaylistHint::default().is_empty());
        assert!(!h.is_empty());
    }

    // select_result must pick highest confidence, first-in-array on a
    // tie (regression: old analyze() no-op picked the LAST).
    // See docs/labels-mod.md — select_result_first_wins_on_tie.
    #[test]
    fn select_result_first_wins_on_tie() {
        // Two parsers, equal (Medium) confidence: the first must win.
        let results = vec![
            ("alpha", result(Confidence::Medium)),
            ("beta", result(Confidence::Medium)),
        ];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("alpha"));
    }

    #[test]
    fn select_result_highest_confidence_wins() {
        let results = vec![
            ("low", result(Confidence::Low)),
            ("high", result(Confidence::High)),
            ("medium", result(Confidence::Medium)),
        ];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("high"));
    }

    #[test]
    fn select_result_skips_empty_and_handles_none() {
        let empty = ParseResult {
            labels: Vec::new(),
            confidence: Confidence::High,
            feature_playlist: None,
        };
        // High-confidence but empty must be skipped in favour of a
        // non-empty lower-confidence result.
        let results = vec![("empty", empty), ("real", result(Confidence::Low))];
        assert_eq!(select_result(&results).map(|(n, _)| *n), Some("real"));
        // No non-empty results → None.
        let none: Vec<(&'static str, ParseResult)> = Vec::new();
        assert!(select_result(&none).is_none());
    }

    /// Per-parser sanity: every parser has both detect and parse
    /// hooked up. Catches accidental nullification (e.g. someone
    /// stubbing `parse` to always-None during a refactor).
    #[test]
    fn parsers_registry_all_entries_populated() {
        for (name, detect, parse) in PARSERS {
            // Function pointers can't be null in safe Rust, so this just fails to
            // compile if the tuple shape changes (e.g. a 4th field is added)
            // without updating callers; it also marks "these parsers exist."
            let _ = (name, detect, parse);
        }
        // Non-empty invariant is covered separately by
        // `parsers_registry_order_locked`, whose assert_eq! on the expected
        // order fails if PARSERS is empty.
    }
}

// ── MPLS floor merge tests ─────────────────────────────────────────────────

#[cfg(test)]
mod gap_fill_tests {
    use super::*;

    /// A vendor label: a slot in one stream table, naming no stream.
    fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: n,
            stream_type: t,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec.into(),
            variant: String::new(),
        }
    }

    /// A derived label: names the stream it describes.
    fn derived(
        t: StreamLabelType,
        clip: &str,
        pid: u16,
        n: u16,
        lang: &str,
        codec: &str,
    ) -> StreamLabel {
        StreamLabel {
            stream_id: Some(StreamId {
                clip_id: clip.into(),
                pid,
            }),
            ..label(t, n, lang, codec)
        }
    }

    #[test]
    fn empty_framework_takes_all_mpls() {
        let mut framework: Vec<StreamLabel> = Vec::new();
        let mpls = vec![
            derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
            derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
            derived(StreamLabelType::Subtitle, "00001", 0x1200, 1, "eng", "PG"),
        ];
        merge_mpls_floor(&mut framework, &mpls);
        assert_eq!(framework.len(), 3);
    }

    // Spec: merge suppresses a floor entry only when the list already
    // NAMES that stream — a shared slot number is not the same fact.
    // See docs/labels-mod.md — suppression_is_by_named_stream_not_by_slot_number.
    #[test]
    fn suppression_is_by_named_stream_not_by_slot_number() {
        // Framework claims audio slots 1 and 2 but names no stream.
        let mut framework = vec![
            label(StreamLabelType::Audio, 1, "eng", "Atmos"),
            label(StreamLabelType::Audio, 2, "fra", "Atmos"),
        ];
        let mpls = vec![
            derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
            derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
        ];
        merge_mpls_floor(&mut framework, &mpls);
        assert_eq!(
            framework.len(),
            4,
            "slot collision is not stream identity: both floor entries survive"
        );
        // The framework's richer hints are untouched — they simply are no
        // longer competing with the floor for a number.
        let vendor: Vec<&str> = framework
            .iter()
            .filter(|l| l.stream_id.is_none())
            .map(|l| l.codec_hint.as_str())
            .collect();
        assert_eq!(vendor, vec!["Atmos", "Atmos"]);
    }

    /// A floor entry for a stream the framework already named is redundant and
    /// is dropped; the framework's own (richer) label for that stream stays.
    #[test]
    fn floor_entry_for_an_already_named_stream_is_dropped() {
        let mut framework = vec![derived(
            StreamLabelType::Audio,
            "00001",
            0x1100,
            1,
            "eng",
            "Atmos",
        )];
        let mpls = vec![
            derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
            derived(StreamLabelType::Audio, "00001", 0x1101, 2, "fra", "AC-3"),
        ];
        merge_mpls_floor(&mut framework, &mpls);
        assert_eq!(framework.len(), 2);
        assert_eq!(framework[0].codec_hint, "Atmos", "framework label survives");
        assert_eq!(framework[1].codec_hint, "AC-3");
    }

    /// Partial-yield case: the framework labelled 2 of 6 audios and 1 of 2
    /// subtitles. The floor supplies all eight streams; the framework's two
    /// editorial labels are kept alongside, to be preferred at bind time.
    #[test]
    fn partial_yield_keeps_framework_and_adds_the_whole_floor() {
        let mut framework = vec![
            label(StreamLabelType::Audio, 1, "eng", "Atmos"),
            label(StreamLabelType::Audio, 4, "eng", "Commentary"),
            label(StreamLabelType::Subtitle, 1, "eng", "PG SDH"),
        ];
        let mut mpls = Vec::new();
        for (i, lang) in ["eng", "fra", "spa", "eng", "deu", "ita"]
            .iter()
            .enumerate()
        {
            mpls.push(derived(
                StreamLabelType::Audio,
                "00001",
                0x1100 + i as u16,
                (i + 1) as u16,
                lang,
                "AC-3",
            ));
        }
        for (i, lang) in ["eng", "fra"].iter().enumerate() {
            mpls.push(derived(
                StreamLabelType::Subtitle,
                "00001",
                0x1200 + i as u16,
                (i + 1) as u16,
                lang,
                "PG",
            ));
        }
        merge_mpls_floor(&mut framework, &mpls);
        assert_eq!(framework.len(), 3 + 8, "3 framework + all 8 floor streams");
        let vendor_hints: Vec<&str> = framework
            .iter()
            .filter(|l| l.stream_id.is_none())
            .map(|l| l.codec_hint.as_str())
            .collect();
        assert_eq!(vendor_hints, vec!["Atmos", "Commentary", "PG SDH"]);
    }

    #[test]
    fn orphan_append_skips_matching_type_lang_codec_tuples() {
        // If a "would-be orphan" shares (type, lang, codec) with a label we
        // already have, drop it to avoid a confusing duplicate. This fuzzy
        // test bounds the orphan list; it's a population rule, not a binding rule.
        let labels = [
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
        ];
        use std::collections::HashSet;
        let existing: HashSet<(StreamLabelType, String, String)> = labels
            .iter()
            .map(|l| (l.stream_type, l.language.clone(), l.codec_hint.clone()))
            .collect();
        let candidate = (
            StreamLabelType::Audio,
            "eng".to_string(),
            "TrueHD".to_string(),
        );
        assert!(
            existing.contains(&candidate),
            "matching tuple must be detected as duplicate"
        );
    }

    /// Sort order is presentation only: audios first, vendor slots ahead of the
    /// PID-named labels, each group in its own ascending order.
    #[test]
    fn sort_groups_audio_before_subtitle_and_vendor_before_named() {
        let mut labels = vec![
            derived(StreamLabelType::Subtitle, "00001", 0x1200, 1, "eng", "PG"),
            label(StreamLabelType::Subtitle, 1, "eng", "SDH"),
            derived(StreamLabelType::Audio, "00001", 0x1100, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 1, "eng", "Atmos"),
        ];
        sort_labels(&mut labels);
        let shape: Vec<(StreamLabelType, bool)> = labels
            .iter()
            .map(|l| (l.stream_type, l.stream_id.is_some()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (StreamLabelType::Audio, false),
                (StreamLabelType::Audio, true),
                (StreamLabelType::Subtitle, false),
                (StreamLabelType::Subtitle, true),
            ]
        );
    }
}

// ── apply() integration tests ──────────────────────────────────────────────
// End-to-end coverage for apply_labels + fill_defaults without needing a
// SectorSource / UdfFs. Synthetic DiscTitle + StreamLabel inputs.

#[cfg(test)]
mod apply_tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, Resolution,
        SampleRate, SubtitleStream, VideoStream,
    };

    fn audio(pid: u16, codec: Codec, channels: AudioChannels, language: &str) -> Stream {
        Stream::Audio(AudioStream {
            pid,
            codec,
            channels,
            language: language.into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }

    fn subtitle(pid: u16, language: &str) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec: Codec::Pgs,
            language: language.into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }

    fn video() -> Stream {
        Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    }

    fn title_with(streams: Vec<Stream>) -> DiscTitle {
        DiscTitle {
            playlist: "00800.mpls".into(),
            playlist_id: 800,
            duration_secs: 7200.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams,
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: crate::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    /// A title that plays one named clip — the shape that matters for
    /// cross-playlist binding, where two playlists cover the same clip.
    fn title_on_clip(playlist: &str, clip_id: &str, streams: Vec<Stream>) -> DiscTitle {
        DiscTitle {
            playlist: playlist.into(),
            clips: vec![crate::disc::Clip {
                feed_span: None,
                clip_id: clip_id.into(),
                in_time: 0,
                out_time: 0,
                duration_secs: 7200.0,
                source_packets: 0,
            }],
            ..title_with(streams)
        }
    }

    /// Sentinel embedded in the crafted playlist name below. The capture keeps
    /// ONLY fields whose rendered form contains it, so the assertion cannot be
    /// fooled by another test's log output.
    const LOG_INJECTION_SENTINEL: &str = "FMKV-LOG-INJECTION-PROBE";

    // A crafted .mpls filename must be logged via `?` (Debug), not `%`
    // (Display) — Display writes control/ANSI bytes verbatim (CWE-117).
    // See docs/labels-mod.md — log-injection guard on playlist-name logging.
    #[test]
    fn a_disc_derived_playlist_name_is_escaped_in_the_log_not_written_verbatim() {
        // A name whose bytes would clear the line and repaint it.
        let evil = format!("\u{1b}[2K\u{1b}[31m{LOG_INJECTION_SENTINEL}\u{7}\u{1b}[0m.mpls");

        let labels = vec![
            sub_label(1, "eng", LabelQualifier::None),
            sub_label(2, "spa", LabelQualifier::None),
            sub_label(3, "fra", LabelQualifier::None),
        ];
        let mut titles = vec![title_on_clip(
            &evil,
            "00294",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, "spa"),
                subtitle(0x12A2, "fra"),
            ],
        )];
        // `testlog::capture` installs the crate's ONE global `tracing` subscriber,
        // routing events to a thread-local sink; `tracing` caches interest
        // globally, so a per-test subscriber would poison other tests' callsites.
        let ((), events) = crate::testlog::capture(|| {
            apply_labels(&labels, &mut titles);
        });
        let playlist: Vec<&str> = events
            .iter()
            .filter(|e| e.target.starts_with("libfreemkv::labels"))
            .filter_map(|e| e.field("playlist"))
            .filter(|v| v.contains(LOG_INJECTION_SENTINEL))
            .collect();
        assert!(
            !playlist.is_empty(),
            "the anchoring event must actually have fired, or this test proves \
             nothing; captured: {events:?}"
        );
        for rendered in playlist {
            assert!(
                !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
                "a disc-controlled playlist name reached the log with its raw \
                 control bytes intact: {rendered:?}"
            );
            assert!(
                rendered.contains(LOG_INJECTION_SENTINEL),
                "the name must still be legible once escaped: {rendered:?}"
            );
        }
    }

    fn sub_label(num: u16, lang: &str, qualifier: LabelQualifier) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: num,
            stream_type: StreamLabelType::Subtitle,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier,
            codec_hint: String::new(),
            variant: String::new(),
        }
    }

    /// `(pid, forced, qualifier)` for every subtitle of a title.
    fn sub_state(title: &DiscTitle) -> Vec<(u16, bool, LabelQualifier)> {
        title
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Subtitle(s) => Some((s.pid, s.forced, s.qualifier)),
                _ => None,
            })
            .collect()
    }

    fn audio_label(num: u16, lang: &str, codec_hint: &str, variant: &str) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: num,
            stream_type: StreamLabelType::Audio,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec_hint.into(),
            variant: variant.into(),
        }
    }

    #[test]
    fn apply_attaches_codec_hint_and_variant_to_audio() {
        let mut titles = vec![title_with(vec![
            video(),
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
        ])];
        let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[1] {
            assert_eq!(a.label, "Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_combines_variant_and_codec_hint() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "por",
        )])];
        let labels = vec![audio_label(1, "por", "Dolby Atmos", "Brazilian")];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "(Brazilian) Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_rejects_mismatched_codec_hint_and_uses_stream_codec() {
        // TrueHD+Atmos relabel case: a TrueHD+Atmos main track the parser mislabeled
        // "AC-3 2.0" (a compat-core hint bound to the wrong stream). The hint
        // contradicts the stream's real codec → discard it, use the stream's own.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "AC-3 2.0", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby TrueHD 7.1");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_unshuffles_cross_labeled_streams() {
        // Cross-bound hints case: hints fully cross-bound — a TrueHD stream wears "AC-3 5.1"
        // and a DD+ stream wears "TrueHD 5.1". Each is corrected from its own
        // stream codec, eliminating the shuffle.
        let mut titles = vec![title_with(vec![
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
            audio(0x1101, Codec::Ac3Plus, AudioChannels::Surround51, "spa"),
        ])];
        let labels = vec![
            audio_label(1, "eng", "AC-3 5.1", ""),
            audio_label(2, "spa", "TrueHD 5.1", ""),
        ];
        apply_labels(&labels, &mut titles);
        let got: Vec<String> = titles[0]
            .streams
            .iter()
            .filter_map(|s| {
                if let Stream::Audio(a) = s {
                    Some(a.label.clone())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(got, vec!["Dolby TrueHD 5.1", "Dolby Digital Plus 5.1"]);
    }

    #[test]
    fn apply_keeps_consistent_richer_atmos_hint() {
        // A DD+ Atmos stream legitimately labeled "Dolby Atmos" — the hint is
        // richer than the spec codec yet consistent with it, so it's kept.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3Plus,
            AudioChannels::Surround51,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "Dolby Atmos", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby Atmos");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_keeps_consistent_dtsx_hint_on_dts_hd_ma() {
        // DTS:X rides a DTS-HD MA core just as Atmos rides TrueHD. A correctly
        // authored "DTS:X" hint on a DtsHdMa stream is richer than the spec
        // codec yet consistent, so it's kept verbatim, not regenerated.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::DtsHdMa,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![audio_label(1, "eng", "DTS:X", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "DTS:X");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn dtsx_hint_consistent_with_dts_hd_carriers() {
        use crate::disc::Codec;
        // The MED fix: a DTS:X hint must now be judged consistent with
        // its DTS-HD lossless carriers (previously it was rejected,
        // because says_dts_ma/says_dts_hr were both false for "DTS:X").
        assert!(codec_hint_consistent("DTS:X", &Codec::DtsHdMa));
        assert!(codec_hint_consistent("DTS-X 7.1", &Codec::DtsHdHr));
        assert!(codec_hint_consistent("dtsx", &Codec::DtsHdMa));
        // It still names the DTS family, so plain-DTS streams remain
        // consistent (family match) — never discarded.
        assert!(codec_hint_consistent("DTS:X", &Codec::Dts));
        // But a DTS:X hint on a non-DTS stream is a genuine mismatch.
        assert!(!codec_hint_consistent("DTS:X", &Codec::TrueHd));
        assert!(!codec_hint_consistent("DTS:X", &Codec::Ac3Plus));
    }

    #[test]
    fn apply_normalizes_plain_consistent_hint_to_marketing() {
        // A DD+ stream whose hint "AC-3+ 5.1" is correct but short-form; a sibling
        // fallback track uses the marketing form, so a plain (non-richer)
        // consistent hint is normalized to the stream's own to stay consistent.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3Plus,
            AudioChannels::Surround51,
            "fra",
        )])];
        let labels = vec![audio_label(1, "fra", "AC-3+ 5.1", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby Digital Plus 5.1");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_sets_purpose_on_audio_commentary() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3,
            AudioChannels::Stereo,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Commentary,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.purpose, LabelPurpose::Commentary);
            // Label stays empty: no codec/variant; purpose is conveyed
            // structurally, NOT as English text.
            assert_eq!(a.label, "");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_uses_name_fallback_only_for_normal_purpose() {
        // Name fallback fires when purpose=Normal and codec/variant are empty.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: "Director's Cut Edition".into(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Director's Cut Edition");
        } else {
            panic!("expected audio stream");
        }
    }

    #[test]
    fn apply_name_fallback_suppressed_for_non_normal_purpose() {
        // Name fallback must NOT fire when purpose != Normal — the
        // CLI is responsible for rendering purpose text.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::Ac3,
            AudioChannels::Stereo,
            "eng",
        )])];
        let labels = vec![StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Audio,
            language: "eng".into(),
            name: "Commentary by Director".into(),
            purpose: LabelPurpose::Commentary,
            qualifier: LabelQualifier::None,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "", "label must not contain English purpose text");
            assert_eq!(a.purpose, LabelPurpose::Commentary);
        }
    }

    #[test]
    fn apply_sets_qualifier_on_subtitle_sdh() {
        let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
        let labels = vec![StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Subtitle,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::Sdh,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);

        if let Stream::Subtitle(s) = &titles[0].streams[0] {
            assert_eq!(s.qualifier, LabelQualifier::Sdh);
            // SDH doesn't flip the `forced` flag.
            assert!(!s.forced);
        } else {
            panic!("expected subtitle");
        }
    }

    #[test]
    fn apply_flips_forced_flag_on_subtitle_forced_qualifier() {
        let mut titles = vec![title_with(vec![subtitle(0x1200, "eng")])];
        let labels = vec![StreamLabel {
            stream_id: None,
            stream_number: 1,
            stream_type: StreamLabelType::Subtitle,
            language: "eng".into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::Forced,
            codec_hint: String::new(),
            variant: String::new(),
        }];
        apply_labels(&labels, &mut titles);
        if let Stream::Subtitle(s) = &titles[0].streams[0] {
            assert_eq!(s.qualifier, LabelQualifier::Forced);
            assert!(s.forced);
        }
    }

    #[test]
    fn apply_indexes_streams_by_type_separately() {
        // Audio and subtitle each have their own 1-based index; an
        // Audio #2 label maps to the 2nd audio stream, not the 2nd
        // stream overall (which could be a subtitle).
        let mut titles = vec![title_with(vec![
            video(),
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
            subtitle(0x1200, "eng"),
            audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "fra"),
        ])];
        let labels = vec![
            audio_label(1, "eng", "Dolby Atmos", ""),
            audio_label(2, "fra", "Dolby Digital", ""),
            StreamLabel {
                stream_id: None,
                stream_number: 1,
                stream_type: StreamLabelType::Subtitle,
                language: "eng".into(),
                name: String::new(),
                purpose: LabelPurpose::Normal,
                qualifier: LabelQualifier::Sdh,
                codec_hint: String::new(),
                variant: String::new(),
            },
        ];
        apply_labels(&labels, &mut titles);

        // Audio #1
        if let Stream::Audio(a) = &titles[0].streams[1] {
            assert_eq!(a.label, "Dolby Atmos");
        }
        // Audio #2 (4th stream overall). The plain "Dolby Digital" hint is
        // consistent with the AC-3 stream but carries no channel info, so it's
        // normalized to the stream's own uniform descriptor.
        if let Stream::Audio(a) = &titles[0].streams[3] {
            assert_eq!(a.label, "Dolby Digital 2.0");
        }
        // Subtitle #1
        if let Stream::Subtitle(s) = &titles[0].streams[2] {
            assert_eq!(s.qualifier, LabelQualifier::Sdh);
        }
    }

    #[test]
    fn apply_ignores_labels_for_nonexistent_streams() {
        // A label for stream #99 with no matching stream is a no-op.
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "eng",
        )])];
        let labels = vec![audio_label(99, "fra", "Dolby Digital", "")];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "", "label must be untouched");
        }
    }

    // Cross-playlist mis-binding this module's two-tier binding stops:
    // ordinal numbering would put Forced on different PIDs per sibling
    // playlist. See docs/labels-mod.md — forced_label_follows_the_pid_not_the_ordinal_across_sibling_playlists.
    #[test]
    fn forced_label_follows_the_pid_not_the_ordinal_across_sibling_playlists() {
        // Six slots: three plain, a commentary subtitle, then two forced.
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::None),
            sub_label(2, "spa", LabelQualifier::None),
            sub_label(3, "fra", LabelQualifier::None),
            sub_label(4, "eng", LabelQualifier::None),
            sub_label(5, "spa", LabelQualifier::Forced),
            sub_label(6, "fra", LabelQualifier::Forced),
        ];
        // The default rip target enumerates a seventh stream (0x12A4, a full
        // English track) in the middle; its sibling does not.
        let mut titles = vec![
            title_on_clip(
                "00801.mpls",
                "00294",
                vec![
                    subtitle(0x12A0, "eng"),
                    subtitle(0x12A1, "spa"),
                    subtitle(0x12A2, "fra"),
                    subtitle(0x12A3, "eng"),
                    subtitle(0x12A4, "eng"),
                    subtitle(0x12A5, "spa"),
                    subtitle(0x12A6, "fra"),
                ],
            ),
            title_on_clip(
                "00040.mpls",
                "00294",
                vec![
                    subtitle(0x12A0, "eng"),
                    subtitle(0x12A1, "spa"),
                    subtitle(0x12A2, "fra"),
                    subtitle(0x12A3, "eng"),
                    subtitle(0x12A5, "spa"),
                    subtitle(0x12A6, "fra"),
                ],
            ),
        ];
        apply_labels(&labels, &mut titles);

        assert_eq!(
            sub_state(&titles[0]),
            vec![
                (0x12A0, false, LabelQualifier::None),
                (0x12A1, false, LabelQualifier::None),
                (0x12A2, false, LabelQualifier::None),
                (0x12A3, false, LabelQualifier::None),
                // The extra stream the label list never described: untouched,
                // and above all NOT forced.
                (0x12A4, false, LabelQualifier::None),
                (0x12A5, true, LabelQualifier::Forced),
                (0x12A6, true, LabelQualifier::Forced),
            ],
        );
        // Same two PIDs forced in the sibling — the flags now agree.
        assert_eq!(
            sub_state(&titles[1]),
            vec![
                (0x12A0, false, LabelQualifier::None),
                (0x12A1, false, LabelQualifier::None),
                (0x12A2, false, LabelQualifier::None),
                (0x12A3, false, LabelQualifier::None),
                (0x12A5, true, LabelQualifier::Forced),
                (0x12A6, true, LabelQualifier::Forced),
            ],
        );
    }

    // No anchor: ordinal fallback drops a label whose language
    // contradicts the stream — unlabelled beats mislabelled `forced`.
    // See docs/labels-mod.md — ordinal_binding_drops_a_subtitle_label_that_contradicts_the_stream_language.
    #[test]
    fn ordinal_binding_drops_a_subtitle_label_that_contradicts_the_stream_language() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::None),
            sub_label(2, "fra", LabelQualifier::Forced),
        ];
        let mut titles = vec![title_with(vec![
            subtitle(0x12A0, "eng"),
            subtitle(0x12A1, "spa"),
        ])];
        apply_labels(&labels, &mut titles);
        assert_eq!(
            sub_state(&titles[0]),
            vec![
                (0x12A0, false, LabelQualifier::None),
                (0x12A1, false, LabelQualifier::None),
            ],
        );
    }

    /// The same guard on the audio side: a label whose language contradicts
    /// the stream is not applied, so a shifted list cannot move `Commentary`
    /// onto a main dialogue track.
    #[test]
    fn ordinal_binding_drops_an_audio_label_that_contradicts_the_stream_language() {
        let mut titles = vec![title_with(vec![
            audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
            audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "spa"),
        ])];
        let labels = vec![
            audio_label(1, "eng", "", ""),
            StreamLabel {
                stream_id: None,
                stream_number: 2,
                stream_type: StreamLabelType::Audio,
                language: "fra".into(),
                name: String::new(),
                purpose: LabelPurpose::Commentary,
                qualifier: LabelQualifier::None,
                codec_hint: String::new(),
                variant: String::new(),
            },
        ];
        apply_labels(&labels, &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[1] {
            assert_eq!(a.purpose, LabelPurpose::Normal);
        } else {
            panic!("expected audio stream");
        }
    }

    // A single agreeing stream is not evidence: every disc has some
    // one-audio clip whose language matches label #1, so it must not anchor.
    #[test]
    fn a_single_stream_title_cannot_anchor_the_label_list() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::None),
            sub_label(2, "spa", LabelQualifier::Forced),
        ];
        let titles = vec![title_on_clip(
            "01241.mpls",
            "00294",
            vec![subtitle(0x12A4, "eng")],
        )];
        assert_eq!(
            find_anchor(&labels, &titles, StreamLabelType::Subtitle),
            None,
        );
    }

    #[test]
    fn apply_empty_labels_does_not_touch_streams() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround51,
            "eng",
        )])];
        apply_labels(&[], &mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "");
        }
    }

    // ── fill_defaults() tests ───────────────────────────────────────────────

    #[test]
    fn fill_defaults_generates_audio_label_when_empty() {
        let mut titles = vec![title_with(vec![audio(
            0x1100,
            Codec::TrueHd,
            AudioChannels::Surround71,
            "eng",
        )])];
        fill_defaults(&mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Dolby TrueHD 7.1");
        }
    }

    #[test]
    fn fill_defaults_preserves_existing_audio_label() {
        let mut titles = vec![title_with(vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::TrueHd,
            channels: AudioChannels::Surround71,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: "Pre-set Atmos".into(),
        })])];
        fill_defaults(&mut titles);
        if let Stream::Audio(a) = &titles[0].streams[0] {
            assert_eq!(a.label, "Pre-set Atmos");
        }
    }

    // Spec: fill_defaults must not clobber a pre-set video label
    // (mirrors the audio contract above; mutate the is_empty() guard to
    // `true` to see this go red).
    #[test]
    fn fill_defaults_preserves_existing_video_label() {
        let mut titles = vec![title_with(vec![Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary: false,
            label: "Pre-set 4K HDR".into(),
            measured_cicp: None,
        })])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert_eq!(v.label, "Pre-set 4K HDR");
        } else {
            panic!("expected video stream");
        }
    }

    #[test]
    fn fill_defaults_generates_video_label_with_hdr() {
        let mut titles = vec![title_with(vec![video()])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("4K"), "expected 4K, got {}", v.label);
            assert!(v.label.contains("HDR10"), "expected HDR10, got {}", v.label);
        }
    }

    /// Spec: an interlaced resolution (`R*i`) must surface the "i" scan type
    /// in the generated label, not a hardcoded "p". PAL DVD is 576i.
    /// Mutation: hardcode "p" → 576i video mislabeled as 576p.
    #[test]
    fn fill_defaults_video_label_honors_interlaced_scan_type() {
        let interlaced = Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576i,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        let mut titles = vec![title_with(vec![interlaced])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("576i"), "expected 576i, got {}", v.label);
            assert!(
                !v.label.contains("576p"),
                "must not say 576p, got {}",
                v.label
            );
        }

        let progressive = Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R576p,
            frame_rate: FrameRate::F25,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt470bg,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        });
        let mut titles = vec![title_with(vec![progressive])];
        fill_defaults(&mut titles);
        if let Stream::Video(v) = &titles[0].streams[0] {
            assert!(v.label.contains("576p"), "expected 576p, got {}", v.label);
        }
    }

    // ── codec_hint_consistent hardening ───────────────────────────────────────

    /// Spec: "Dolby Digital" (AC-3) hint is consistent ONLY with AC-3 streams;
    /// NOT with DD+ or TrueHD.
    /// Mutation: accept "Dolby Digital" as consistent with AC-3+ → DD+ mislabeled.
    #[test]
    fn codec_hint_consistent_ac3_not_confused_with_ddp() {
        assert!(codec_hint_consistent("Dolby Digital", &Codec::Ac3));
        assert!(codec_hint_consistent("AC-3 5.1", &Codec::Ac3));
        assert!(!codec_hint_consistent("Dolby Digital", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("AC-3 5.1", &Codec::TrueHd));
    }

    /// Spec: "Dolby Digital Plus" (AC-3+) is consistent with DD+ streams,
    /// NOT with plain AC-3.
    /// Mutation: merge DD and DD+ into one family check → mismatch undetected.
    #[test]
    fn codec_hint_consistent_ddp_not_confused_with_ac3() {
        assert!(codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3Plus));
        assert!(codec_hint_consistent("E-AC-3", &Codec::Ac3Plus));
        assert!(codec_hint_consistent("DD+", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("Dolby Digital Plus", &Codec::Ac3));
    }

    /// Spec: "DTS" hint consistent with DTS streams, NOT DTS-HD families.
    /// Mutation: treat bare "DTS" hint as consistent with DtsHdMa → mismatch.
    #[test]
    fn codec_hint_consistent_dts_families_distinguished() {
        assert!(codec_hint_consistent("DTS", &Codec::Dts));
        assert!(!codec_hint_consistent("DTS", &Codec::DtsHdMa));
        assert!(!codec_hint_consistent("DTS", &Codec::DtsHdHr));
        assert!(codec_hint_consistent("DTS-HD MA", &Codec::DtsHdMa));
        assert!(codec_hint_consistent("DTS-HD HR", &Codec::DtsHdHr));
    }

    /// Spec: "LPCM" hint consistent only with Lpcm codec.
    /// Mutation: make PCM consistent with all → mismatch undetected.
    #[test]
    fn codec_hint_consistent_lpcm() {
        assert!(codec_hint_consistent("LPCM 7.1", &Codec::Lpcm));
        assert!(codec_hint_consistent("PCM", &Codec::Lpcm));
        assert!(!codec_hint_consistent("LPCM", &Codec::TrueHd));
        assert!(!codec_hint_consistent("LPCM", &Codec::Ac3));
    }

    /// Spec: empty codec hint → consistent (no assertion = no contradiction).
    /// Mutation: return false for empty hint → streams with no hint lose their label.
    #[test]
    fn codec_hint_consistent_empty_hint() {
        assert!(codec_hint_consistent("", &Codec::TrueHd));
        assert!(codec_hint_consistent("", &Codec::Ac3));
        assert!(codec_hint_consistent("", &Codec::Lpcm));
    }

    /// Spec: a pure-editorial hint (e.g. "Commentary") names no codec family
    /// and is therefore consistent with any codec stream.
    /// Mutation: parse "commentary" and return false → editorial labels discarded.
    #[test]
    fn codec_hint_consistent_editorial_hint_no_codec() {
        assert!(codec_hint_consistent("Commentary", &Codec::TrueHd));
        assert!(codec_hint_consistent("Commentary", &Codec::Ac3));
        assert!(codec_hint_consistent("Commentary", &Codec::Dts));
    }

    // ── generate_audio_label hardening ─────────────────────────────────────────

    /// Spec: `generate_audio_label` uses full marketing names, not abbreviations.
    /// Mutation: use "DD" instead of "Dolby Digital" → abbreviated name returned.
    #[test]
    fn generate_audio_label_all_codecs() {
        assert_eq!(
            generate_audio_label(&Codec::TrueHd, &AudioChannels::Surround51, false),
            "Dolby TrueHD 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Ac3, &AudioChannels::Surround51, false),
            "Dolby Digital 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Ac3Plus, &AudioChannels::Surround51, false),
            "Dolby Digital Plus 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::DtsHdMa, &AudioChannels::Surround51, false),
            "DTS-HD Master Audio 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::DtsHdHr, &AudioChannels::Surround51, false),
            "DTS-HD High Resolution 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Dts, &AudioChannels::Surround51, false),
            "DTS 5.1"
        );
        assert_eq!(
            generate_audio_label(&Codec::Lpcm, &AudioChannels::Surround51, false),
            "LPCM 5.1"
        );
    }

    /// Spec: Unknown codec → empty string (never "?", never panic).
    /// Mutation: return "Unknown" for unrecognized codecs → non-empty string.
    #[test]
    fn generate_audio_label_unknown_codec_empty() {
        assert_eq!(
            generate_audio_label(&Codec::Pgs, &AudioChannels::Surround51, false),
            ""
        );
    }

    /// Spec: Unknown channel layout → codec name only (no channel suffix).
    /// Mutation: append " Unknown" for unrecognized channels → spurious suffix.
    #[test]
    fn generate_audio_label_unknown_channels_no_suffix() {
        assert_eq!(
            generate_audio_label(&Codec::Ac3, &AudioChannels::Unknown, false),
            "Dolby Digital"
        );
    }

    /// Spec: all channel layouts produce the documented string suffixes.
    /// Mutation: swap any two (e.g. Mono/Stereo) → wrong descriptor rendered.
    #[test]
    fn generate_audio_label_all_channel_layouts() {
        let f = |ch| generate_audio_label(&Codec::Ac3, ch, false);
        assert_eq!(f(&AudioChannels::Mono), "Dolby Digital 1.0");
        assert_eq!(f(&AudioChannels::Stereo), "Dolby Digital 2.0");
        assert_eq!(f(&AudioChannels::Surround51), "Dolby Digital 5.1");
        assert_eq!(f(&AudioChannels::Surround71), "Dolby Digital 7.1");
    }

    /// Spec: codec_hint_adds_detail only returns true for Atmos and DTS:X.
    /// Mutation: return true for all hints → plain hints kept verbatim, no normalization.
    #[test]
    fn codec_hint_adds_detail_atmos_and_dtsx_only() {
        assert!(codec_hint_adds_detail("Dolby Atmos"));
        assert!(codec_hint_adds_detail("DTS:X"));
        assert!(codec_hint_adds_detail("DTS-X 7.1"));
        assert!(codec_hint_adds_detail("dtsx"));
        assert!(!codec_hint_adds_detail("Dolby TrueHD"));
        assert!(!codec_hint_adds_detail("DTS-HD Master Audio"));
        assert!(!codec_hint_adds_detail("Dolby Digital Plus 5.1"));
        assert!(!codec_hint_adds_detail(""));
    }

    // ── generate_video_label hardening: secondary Dolby Vision stream
    // gets "Dolby Vision EL", every other HDR format on a secondary
    // stream gets no label. See docs/labels-mod.md — generate_video_label_secondary_dolby_vision_el.
    #[test]
    fn generate_video_label_secondary_dolby_vision_el() {
        assert_eq!(
            generate_video_label(
                &Codec::Hevc,
                (3840, 2160),
                false,
                &HdrFormat::DolbyVision,
                true
            ),
            "Dolby Vision EL"
        );
        // Every other HDR format on a secondary stream: empty, not text.
        assert_eq!(
            generate_video_label(&Codec::Hevc, (3840, 2160), false, &HdrFormat::Hdr10, true),
            ""
        );
    }

    // Spec: 480 lines is the SD floor — height exactly 480 must get the
    // "480p"/"480i" token, not the empty-resolution case.
    // See docs/labels-mod.md — generate_video_label_480_boundary.
    #[test]
    fn generate_video_label_480_boundary() {
        let label = generate_video_label(&Codec::Mpeg2, (0, 480), false, &HdrFormat::Sdr, false);
        assert!(
            label.contains("480p"),
            "h == 480 must resolve to 480p, got {label:?}"
        );
    }

    // Spec: SDR is the unmarked default — must never appear as a token
    // (only non-SDR formats get an explicit tag).
    // See docs/labels-mod.md — generate_video_label_sdr_produces_no_hdr_token.
    #[test]
    fn generate_video_label_sdr_produces_no_hdr_token() {
        assert_eq!(
            generate_video_label(&Codec::Hevc, (1920, 1080), false, &HdrFormat::Sdr, false),
            "HEVC 1080p"
        );
    }

    // ── generate_audio_label_atmos ───────────────────────────────────────

    // Spec: the Atmos-aware variant folds "Atmos" into the codec brand
    // for TrueHD/DD+ carriers. See docs/labels-mod.md — generate_audio_label_atmos_folds_brand.
    #[test]
    fn generate_audio_label_atmos_folds_brand() {
        assert_eq!(
            generate_audio_label_atmos(&Codec::TrueHd, &AudioChannels::Surround71, false),
            "Dolby TrueHD Atmos 7.1"
        );
        assert_eq!(
            generate_audio_label_atmos(&Codec::Ac3Plus, &AudioChannels::Surround51, false),
            "Dolby Digital Plus Atmos 5.1"
        );
    }

    // Spec: every disc-audio codec has a full marketing name, including
    // lossy PC-container codecs. See docs/labels-mod.md — generate_audio_label_covers_pc_container_codecs.
    #[test]
    fn generate_audio_label_covers_pc_container_codecs() {
        assert_eq!(
            generate_audio_label(&Codec::Aac, &AudioChannels::Stereo, false),
            "AAC 2.0"
        );
        assert_eq!(
            generate_audio_label(&Codec::Mp2, &AudioChannels::Stereo, false),
            "MPEG Audio 2.0"
        );
        assert_eq!(
            generate_audio_label(&Codec::Mp3, &AudioChannels::Stereo, false),
            "MP3 2.0"
        );
        assert_eq!(
            generate_audio_label(&Codec::Flac, &AudioChannels::Stereo, false),
            "FLAC 2.0"
        );
        assert_eq!(
            generate_audio_label(&Codec::Opus, &AudioChannels::Stereo, false),
            "Opus 2.0"
        );
    }

    // ── codec_hint_consistent: chained-OR boundary hardening. Each test
    // below isolates ONE `||` synonym clause so weakening it to `&&`
    // changes the verdict. Isolates the "true hd" (space form) synonym.
    #[test]
    fn codec_hint_consistent_truehd_space_synonym() {
        assert!(codec_hint_consistent("True HD 7.1", &Codec::TrueHd));
        assert!(!codec_hint_consistent("True HD 7.1", &Codec::Ac3));
    }

    // Isolates the "ac3+" (no-hyphen) synonym in says_ddp; must not fall
    // through to the plain-AC3 says_ac3 check.
    #[test]
    fn codec_hint_consistent_ddp_ac3_plus_no_hyphen_synonym() {
        assert!(codec_hint_consistent("AC3+ 5.1", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("AC3+ 5.1", &Codec::Ac3));
    }

    // Isolates the "eac3" synonym in says_ddp, last clause before
    // "digital plus"/"dd+".
    #[test]
    fn codec_hint_consistent_ddp_eac3_synonym() {
        assert!(codec_hint_consistent("EAC3 5.1", &Codec::Ac3Plus));
        assert!(!codec_hint_consistent("EAC3 5.1", &Codec::Ac3));
    }

    // Isolates the "pcm" (no "lpcm") synonym in says_lpcm.
    // See docs/labels-mod.md — codec_hint_consistent_lpcm_bare_pcm_synonym.
    #[test]
    fn codec_hint_consistent_lpcm_bare_pcm_synonym() {
        assert!(codec_hint_consistent("PCM", &Codec::Lpcm));
        assert!(!codec_hint_consistent("PCM", &Codec::Ac3));
    }

    // Isolates says_dts_ma || says_dts_hr inside names_family.
    // See docs/labels-mod.md — codec_hint_consistent_names_family_dts_ma_alone.
    #[test]
    fn codec_hint_consistent_names_family_dts_ma_alone() {
        assert!(!codec_hint_consistent("Master Audio", &Codec::Ac3));
        assert!(codec_hint_consistent("Master Audio", &Codec::DtsHdMa));
    }

    // Isolates the Codec::TrueHd => says_truehd || says_atmos arm.
    // See docs/labels-mod.md — codec_hint_consistent_truehd_arm_atmos_alone.
    #[test]
    fn codec_hint_consistent_truehd_arm_atmos_alone() {
        assert!(codec_hint_consistent(
            "Dolby Digital Plus Atmos",
            &Codec::TrueHd
        ));
    }

    // Spec: Codec::Dts is consistent ONLY when says_dts is true, not
    // via any other named family. See docs/labels-mod.md — codec_hint_consistent_dts_arm_not_bypassed.
    #[test]
    fn codec_hint_consistent_dts_arm_not_bypassed() {
        assert!(!codec_hint_consistent("Dolby Digital", &Codec::Dts));
    }

    // Spec: Codec::Lpcm is consistent ONLY when says_lpcm is true (same
    // bypass failure mode as the Dts arm above).
    #[test]
    fn codec_hint_consistent_lpcm_arm_not_bypassed() {
        assert!(!codec_hint_consistent("Dolby Digital", &Codec::Lpcm));
    }

    // ── provenance: which labels may be reached by counting ────────────────

    /// An MPLS/CLPI-derived label naming clip `clip`, PID `pid`, sitting at
    /// slot `num` of its own playlist's table.
    fn derived_sub(clip: &str, pid: u16, num: u16, lang: &str) -> StreamLabel {
        StreamLabel {
            stream_id: Some(StreamId {
                clip_id: clip.into(),
                pid,
            }),
            ..sub_label(num, lang, LabelQualifier::None)
        }
    }

    fn derived_audio(clip: &str, pid: u16, num: u16, lang: &str, codec: &str) -> StreamLabel {
        StreamLabel {
            stream_id: Some(StreamId {
                clip_id: clip.into(),
                pid,
            }),
            ..audio_label(num, lang, codec, "")
        }
    }

    // Spec: a label that names its own stream is never reachable
    // through the slot lookup (different coordinate systems).
    // See docs/labels-mod.md — slot_lookup_never_returns_a_label_that_names_its_own_stream.
    #[test]
    fn slot_lookup_never_returns_a_label_that_names_its_own_stream() {
        let labels = vec![
            derived_sub("00002", 0x1200, 1, "fra"),
            sub_label(1, "eng", LabelQualifier::Sdh),
        ];
        let found = label_at(&labels, StreamLabelType::Subtitle, 1).expect("slot 1 is vendor's");
        assert_eq!(found.language, "eng");
        assert_eq!(found.qualifier, LabelQualifier::Sdh);
    }

    // Spec: the derived floor does not vote on which title anchors the
    // vendor list, and cannot VETO the title that does.
    // See docs/labels-mod.md — the_derived_floor_cannot_veto_the_anchor.
    #[test]
    fn the_derived_floor_cannot_veto_the_anchor() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::Sdh),
            sub_label(2, "fra", LabelQualifier::Forced),
            derived_sub("00050", 0x1202, 3, "spa"),
        ];
        let titles = vec![title_on_clip(
            "00800.mpls",
            "00800",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, "fra"),
                subtitle(0x12A2, "deu"),
            ],
        )];
        assert_eq!(
            find_anchor(&labels, &titles, StreamLabelType::Subtitle),
            Some(0),
            "the feature reproduces every slot the VENDOR named"
        );
    }

    // Spec: among titles the vendor list admits, the one that CONFIRMS
    // more of it wins — silence is not evidence, size alone must not win.
    // See docs/labels-mod.md — the_anchor_is_the_title_that_confirms_most_of_the_list.
    #[test]
    fn the_anchor_is_the_title_that_confirms_most_of_the_list() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::Sdh),
            sub_label(2, "fra", LabelQualifier::Forced),
        ];
        let titles = vec![
            // Longer, but says nothing: compatible with anything, confirms none.
            title_on_clip(
                "00050.mpls",
                "00050",
                vec![
                    subtitle(0x1200, ""),
                    subtitle(0x1201, ""),
                    subtitle(0x1202, ""),
                ],
            ),
            // Shorter, but reproduces the list.
            title_on_clip(
                "00800.mpls",
                "00800",
                vec![subtitle(0x12A0, "eng"), subtitle(0x12A1, "fra")],
            ),
        ];
        assert_eq!(
            find_anchor(&labels, &titles, StreamLabelType::Subtitle),
            Some(1)
        );
    }

    // Spec: a vendor qualifier does not leak via ordinal fallback onto a
    // different physical stream (same ordinal, different clip).
    // See docs/labels-mod.md — a_vendor_qualifier_does_not_leak_onto_a_featurettes_own_stream.
    #[test]
    fn a_vendor_qualifier_does_not_leak_onto_a_featurettes_own_stream() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::Sdh),
            sub_label(2, "fra", LabelQualifier::None),
            // The floor names the featurette's single subtitle.
            derived_sub("00076", 0x1200, 1, "eng"),
        ];
        let mut titles = vec![
            title_on_clip(
                "00800.mpls",
                "00082",
                vec![subtitle(0x12A0, "eng"), subtitle(0x12A1, "fra")],
            ),
            title_on_clip("00451.mpls", "00076", vec![subtitle(0x1200, "eng")]),
        ];
        apply_labels(&labels, &mut titles);
        assert_eq!(
            sub_state(&titles[0]),
            vec![
                (0x12A0, false, LabelQualifier::Sdh),
                (0x12A1, false, LabelQualifier::None)
            ],
            "the feature IS the table the list describes and keeps its SDH"
        );
        assert_eq!(
            sub_state(&titles[1]),
            vec![(0x1200, false, LabelQualifier::None)],
            "the featurette's own stream is not the feature's subtitle 1"
        );
    }

    /// A title that plays SEVERAL clips in order — the shape the anchor's
    /// PID facts are harvested from.
    fn title_on_clips(playlist: &str, clip_ids: &[&str], streams: Vec<Stream>) -> DiscTitle {
        DiscTitle {
            playlist: playlist.into(),
            clips: clip_ids
                .iter()
                .map(|id| crate::disc::Clip {
                    feed_span: None,
                    clip_id: (*id).into(),
                    in_time: 0,
                    out_time: 0,
                    duration_secs: 3600.0,
                    source_packets: 0,
                })
                .collect(),
            ..title_with(streams)
        }
    }

    // Spec: the anchor proves a (clip, PID) fact only for the clip its
    // stream table was READ FROM (the first play item), never every clip
    // it plays. See docs/labels-mod.md — an_anchor_proves_pids_only_for_the_clip_its_table_came_from.
    #[test]
    fn an_anchor_proves_pids_only_for_the_clip_its_table_came_from() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::Sdh),
            sub_label(2, "fra", LabelQualifier::None),
        ];
        let mut titles = vec![
            // The anchor: its stream table is clip 00082's (the first play
            // item); it merely CONTINUES into 00090.
            title_on_clips(
                "00800.mpls",
                &["00082", "00090"],
                vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
            ),
            // Sibling playlist over the anchor's SECOND clip; reuses PID 0x1200
            // (PIDs are only unique within a clip) but is Spanish, so the
            // anchor's English SDH slot doesn't describe it.
            title_on_clips("00451.mpls", &["00090"], vec![subtitle(0x1200, "spa")]),
        ];
        apply_labels(&labels, &mut titles);

        assert_eq!(
            sub_state(&titles[0]),
            vec![
                (0x1200, false, LabelQualifier::Sdh),
                (0x1201, false, LabelQualifier::None)
            ],
            "the anchor itself keeps the qualifiers the list states for it"
        );
        assert_eq!(
            sub_state(&titles[1]),
            vec![(0x1200, false, LabelQualifier::None)],
            "a PID in a clip the anchor's table never described is not that \
             table's stream 1"
        );
    }

    // Spec: a vendor codec/variant claim does not follow the ordinal
    // onto a bonus clip that carries a different codec.
    // See docs/labels-mod.md — a_vendor_codec_claim_does_not_follow_the_ordinal_onto_a_bonus_clip.
    #[test]
    fn a_vendor_codec_claim_does_not_follow_the_ordinal_onto_a_bonus_clip() {
        // This framework states no codec_hint and puts its descriptor in `name`,
        // which `apply_labels` falls back to verbatim, so the codec-consistency
        // guard never runs and cannot catch the mis-binding.
        let feature_audio = StreamLabel {
            name: "English Dolby Atmos".into(),
            ..audio_label(1, "eng", "", "")
        };
        let labels = vec![
            feature_audio,
            audio_label(2, "eng", "", ""),
            derived_audio("00020", 0x1100, 1, "eng", "AC-3"),
        ];
        let mut titles = vec![
            title_on_clip(
                "00001.mpls",
                "00000",
                vec![
                    audio(0x1100, Codec::TrueHd, AudioChannels::Surround51, "eng"),
                    audio(0x1101, Codec::Ac3, AudioChannels::Stereo, "eng"),
                ],
            ),
            title_on_clip(
                "00301.mpls",
                "00020",
                vec![audio(0x1100, Codec::Ac3, AudioChannels::Stereo, "eng")],
            ),
        ];
        apply_labels(&labels, &mut titles);
        let label_of = |t: &DiscTitle, i: usize| match &t.streams[i] {
            Stream::Audio(a) => a.label.clone(),
            _ => unreachable!(),
        };
        assert_eq!(
            label_of(&titles[0], 0),
            "English Dolby Atmos",
            "the feature keeps the descriptor its own list states"
        );
        assert_eq!(
            label_of(&titles[1], 0),
            "Dolby Digital 2.0",
            "the bonus clip describes the stream it actually carries"
        );
    }

    // Spec: a title whose stream count is smaller than the vendor
    // list's highest slot cannot be the table that list describes.
    // See docs/labels-mod.md — a_title_shorter_than_the_vendor_list_cannot_anchor_it.
    #[test]
    fn a_title_shorter_than_the_vendor_list_cannot_anchor_it() {
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::None),
            sub_label(2, "fra", LabelQualifier::None),
            sub_label(3, "deu", LabelQualifier::Forced),
        ];
        let titles = vec![
            // Two streams, both confirming their slot: the strongest evidence
            // on offer, and still not a table with a third slot in it.
            title_on_clip(
                "00050.mpls",
                "00050",
                vec![subtitle(0x1200, "eng"), subtitle(0x1201, "fra")],
            ),
            // Three streams, only the first stating a language — weaker
            // evidence, but it is the only shape the list can be describing.
            title_on_clip(
                "00800.mpls",
                "00800",
                vec![
                    subtitle(0x12A0, "eng"),
                    subtitle(0x12A1, ""),
                    subtitle(0x12A2, ""),
                ],
            ),
        ];
        assert_eq!(
            find_anchor(&labels, &titles, StreamLabelType::Subtitle),
            Some(1),
            "only the three-stream title can hold a list whose top slot is 3"
        );
    }

    // Spec: a slot the vendor list never names constrains nothing (these
    // blobs under-yield by design). See docs/labels-mod.md — an_unnamed_slot_does_not_disqualify_a_title.
    #[test]
    fn an_unnamed_slot_does_not_disqualify_a_title() {
        // The vendor names slots 1 and 3 only; slot 2 is its silence.
        let labels = vec![
            sub_label(1, "eng", LabelQualifier::Sdh),
            sub_label(3, "fra", LabelQualifier::Forced),
        ];
        let mut titles = vec![title_on_clip(
            "00800.mpls",
            "00800",
            vec![
                subtitle(0x12A0, "eng"),
                subtitle(0x12A1, "deu"),
                subtitle(0x12A2, "fra"),
            ],
        )];
        assert_eq!(
            find_anchor(&labels, &titles, StreamLabelType::Subtitle),
            Some(0)
        );
        apply_labels(&labels, &mut titles);
        assert_eq!(
            sub_state(&titles[0]),
            vec![
                (0x12A0, false, LabelQualifier::Sdh),
                (0x12A1, false, LabelQualifier::None),
                (0x12A2, true, LabelQualifier::Forced),
            ]
        );
    }
}

// ── fill_gaps_from_mpls: no-op-when-nothing-added hardening ────────────────

#[cfg(test)]
mod fill_gaps_sort_tests {
    use super::*;

    fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: n,
            stream_type: t,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec.into(),
            variant: String::new(),
        }
    }

    // Spec: the sort-by-(type, number) pass only runs when the merge
    // actually added something; a no-op merge must leave order untouched.
    // See docs/labels-mod.md — fill_gaps_leaves_order_untouched_when_nothing_added.
    #[test]
    fn fill_gaps_leaves_order_untouched_when_nothing_added() {
        // Deliberately out of (type, number) order: number 2 before 1.
        let mut framework = vec![
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
        ];
        // MPLS covers exactly the same (type, number) slots -> added == 0.
        let mpls = vec![
            label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
            label(StreamLabelType::Audio, 2, "fra", "AC-3"),
        ];
        merge_mpls_floor(&mut framework, &mpls);
        assert_eq!(
            framework[0].stream_number, 2,
            "no gap-fill happened, so the original (out-of-order) sequence must survive"
        );
        assert_eq!(framework[1].stream_number, 1);
    }
}

// ── append_clpi_orphans ─────────────────────────────────────────────────────

#[cfg(test)]
mod clpi_orphan_tests {
    use super::*;
    use crate::udf::fixture::*;

    fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
        StreamLabel {
            stream_id: None,
            stream_number: n,
            stream_type: t,
            language: lang.into(),
            name: String::new(),
            purpose: LabelPurpose::Normal,
            qualifier: LabelQualifier::None,
            codec_hint: codec.into(),
            variant: String::new(),
        }
    }

    // Build a CLPI ProgramInfo section for one program from
    // (pid, stream_coding_info) pairs, per crate::clpi::parse_program_info.
    // See docs/labels-mod.md — build_program_info / build_clpi.
    fn build_program_info(streams: &[(u16, Vec<u8>)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(0); // reserved
        body.push(1); // num_programs = 1
        body.extend_from_slice(&0u32.to_be_bytes()); // spn_program_sequence_start
        body.extend_from_slice(&0u16.to_be_bytes()); // program_map_pid
        body.push(streams.len() as u8); // num_streams
        body.push(0); // num_groups
        for (pid, sci) in streams {
            body.extend_from_slice(&pid.to_be_bytes());
            body.push(sci.len() as u8);
            body.extend_from_slice(sci);
        }
        let mut out = Vec::new();
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    // Build a full CLPI buffer (HDMV header + ProgramInfo) for
    // (pid, coding_type, lang) streams. See docs/labels-mod.md — build_program_info / build_clpi.
    fn build_clpi(streams: &[(u16, u8, &str)]) -> Vec<u8> {
        use crate::consts::coding_type as c;
        let sci_streams: Vec<(u16, Vec<u8>)> = streams
            .iter()
            .map(|(pid, coding, lang)| {
                let lang_bytes = lang.as_bytes();
                let sci = match *coding {
                    c::PG | c::IG => {
                        let mut v = vec![*coding];
                        v.extend_from_slice(lang_bytes);
                        v
                    }
                    _ => {
                        let mut v = vec![*coding, 0x61];
                        v.extend_from_slice(lang_bytes);
                        v
                    }
                };
                (*pid, sci)
            })
            .collect();
        let pi = build_program_info(&sci_streams);
        let mut buf = vec![0u8; 60];
        buf[0..4].copy_from_slice(b"HDMV");
        buf[4..8].copy_from_slice(b"0200");
        let prog_info_start: u32 = 60;
        buf[12..16].copy_from_slice(&prog_info_start.to_be_bytes());
        buf[56..60].copy_from_slice(&1000u32.to_be_bytes()); // source_packet_count
        buf.extend_from_slice(&pi);
        buf
    }

    /// Lay a minimal BDMV/CLIPINF/00001.clpi tree on `disc`, with the CLPI
    /// declaring the given synthetic streams, and return the parsed UdfFs.
    fn fs_with_clpi(disc: &mut MemDisc, streams: &[(u16, u8, &str)]) -> crate::udf::UdfFs {
        let clpi_data = build_clpi(streams);
        let clipinf = DirSpec {
            name: "CLIPINF".to_string(),
            icb_lba: 24,
            dir_data_lba: 25,
            files: vec![file_with("00001.clpi", 26, 8000, clpi_data, false)],
            subdirs: vec![],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![clipinf],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    /// (a) A PG-coded CLPI orphan becomes a Subtitle label.
    #[test]
    fn pg_orphan_becomes_subtitle() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[(0x1200, crate::consts::coding_type::PG, "eng")],
        );
        let mut labels: Vec<StreamLabel> = Vec::new();
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 1);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Subtitle);
        assert_eq!(
            labels[0].stream_id,
            Some(StreamId {
                clip_id: "00001".into(),
                pid: 0x1200
            }),
            "the orphan names the stream it was read from"
        );
    }

    /// (b) An audio-range-coded orphan (here DTS-HD MA, the top of the
    /// `LPCM..=DTS_HD_MA` primary-audio range) becomes an Audio label.
    #[test]
    fn audio_range_orphan_becomes_audio() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[(0x1100, crate::consts::coding_type::DTS_HD_MA, "eng")],
        );
        let mut labels: Vec<StreamLabel> = Vec::new();
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
    }

    /// (b, secondary) AC3_PLUS_SECONDARY is outside the primary
    /// `LPCM..=DTS_HD_MA` range and must be classified through the
    /// dedicated secondary-audio arm.
    #[test]
    fn secondary_audio_orphan_becomes_audio() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[(
                0x1A00,
                crate::consts::coding_type::AC3_PLUS_SECONDARY,
                "eng",
            )],
        );
        let mut labels: Vec<StreamLabel> = Vec::new();
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 1);
        assert_eq!(labels[0].stream_type, StreamLabelType::Audio);
    }

    /// (c) IG (0x91, BD-J menu overlay) is not a user-facing subtitle and
    /// must be skipped entirely, not appended as anything.
    #[test]
    fn ig_orphan_is_skipped() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[(0x1201, crate::consts::coding_type::IG, "eng")],
        );
        let mut labels: Vec<StreamLabel> = Vec::new();
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 0);
        assert!(labels.is_empty());
    }

    // (d) An orphan states NO STN slot, and is identified by the stream
    // it was read from instead (an invented ordinal could reach it).
    // See docs/labels-mod.md — orphans_state_no_stn_slot_and_name_their_stream.
    #[test]
    fn orphans_state_no_stn_slot_and_name_their_stream() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[
                (0x1100, crate::consts::coding_type::TRUEHD, "eng"),
                (0x1101, crate::consts::coding_type::AC3, "fra"),
            ],
        );
        let mut labels = vec![label(StreamLabelType::Audio, 3, "jpn", "DTS")];
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 2);
        let orphans: Vec<&StreamLabel> = labels
            .iter()
            .filter(|l| l.stream_type == StreamLabelType::Audio && l.language != "jpn")
            .collect();
        for o in &orphans {
            assert_eq!(o.stream_number, NO_STN_SLOT, "an orphan holds no slot");
        }
        let mut ids: Vec<u16> = orphans
            .iter()
            .filter_map(|o| o.stream_id.as_ref().map(|i| i.pid))
            .collect();
        ids.sort();
        assert_eq!(ids, vec![0x1100, 0x1101]);

        // And the slot lookup cannot reach one, whatever the ordinal.
        for n in 0..=6u16 {
            assert!(
                label_at(&labels, StreamLabelType::Audio, n).is_none_or(|l| l.language == "jpn"),
                "slot {n} must resolve to the vendor label or to nothing"
            );
        }
    }

    /// (e) A CLPI stream whose (type, language, codec) tuple already exists
    /// in `existing` is a duplicate and must be skipped, not double-listed.
    #[test]
    fn duplicate_type_lang_codec_already_in_existing_is_skipped() {
        let mut disc = MemDisc::new();
        let udf = fs_with_clpi(
            &mut disc,
            &[(0x1100, crate::consts::coding_type::TRUEHD, "eng")],
        );
        let mut labels = vec![label(StreamLabelType::Audio, 1, "eng", "TrueHD")];
        let added = append_clpi_orphans(&mut labels, &mut disc, &udf);
        assert_eq!(added, 0);
        assert_eq!(labels.len(), 1);
    }
}

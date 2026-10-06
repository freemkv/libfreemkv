//! Stream label extraction from BD-J disc files.
//!
//! Each parser module represents one BD-J authoring framework. To add a
//! new format:
//!   1. Create `src/labels/myformat.rs`
//!   2. Implement `pub fn detect(reader: &mut dyn SectorSource, udf: &UdfFs) -> bool`
//!   3. Implement `pub fn parse(reader: &mut dyn SectorSource, udf: &UdfFs) -> Option<ParseResult>`
//!      (set [`ParseResult::confidence`] to drive tie-breaking)
//!   4. Add `mod myformat;` below and one line to `PARSERS` array

mod bdj_feature;
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
pub(crate) use bdmt::{display_text, is_placeholder_title};

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

    /// Hint from an authoring id attribute (digits only). The filename is
    /// formatted from the one parsed u16, so the two fields cannot disagree.
    pub(crate) fn from_authoring_id(id: &str) -> Option<Self> {
        Some(Self::for_playlist(digits(id).parse::<u16>().ok()?))
    }

    /// Hint naming playlist `id` by number and canonical `NNNNN.mpls` filename.
    pub(crate) fn for_playlist(id: u16) -> Self {
        Self {
            playlist_id: Some(id),
            filename: Some(format!("{id:05}.mpls")),
        }
    }
}

// Lower-cased words of an identifier: letter runs, also split at camelCase
// boundaries (`MainFeature_A` → main, feature, a).
pub(crate) fn name_words(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    for part in s.split(|c: char| !c.is_ascii_alphabetic()) {
        let mut word = String::new();
        // The boundary test reads the source char: `word` holds it lower-cased.
        let mut prev_lower = false;
        for c in part.chars() {
            if c.is_ascii_uppercase() && prev_lower {
                words.push(std::mem::take(&mut word));
            }
            prev_lower = c.is_ascii_lowercase();
            word.push(c.to_ascii_lowercase());
        }
        if !word.is_empty() {
            words.push(word);
        }
    }
    words
}

// The ASCII digits of `s`, in order (tolerates stray quotes/spaces/units).
fn digits(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_digit()).collect()
}

// A manifest element's stated running time: the first of `keys` whose digits
// parse to a non-zero number of seconds.
pub(crate) fn stated_duration_secs(element: &str, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|k| {
        let v = xml::attr(element, k)?;
        digits(&v).parse::<u64>().ok().filter(|&d| d > 0)
    })
}

/// Successful parser result. `None` from `parse()` still means "this
/// isn't my disc" (no labels at all); `Some(ParseResult { labels, .. })`
/// with `labels.is_empty()` is also a "no labels" case but reachable
/// via the analyzer.
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
    let (labels, winner_hint) =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extract(reader, udf))) {
            Ok(r) => r,
            Err(payload) => {
                tracing::warn!(
                    panic = %panic_message(payload.as_ref()),
                    "label extraction panicked; streams left unlabelled"
                );
                Default::default()
            }
        };
    if !labels.is_empty() {
        apply_labels(&labels, titles);
    }
    // The feature hint rides its own pass, independent of whether any parser
    // produced labels (like bdmt): a jar-only disc still yields a hint. Wrapped
    // separately so a menu-walk fault can't lose an already-extracted hint.
    let hint = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resolve_feature_hint(reader, udf, winner_hint, titles)
    }))
    .unwrap_or_else(|payload| {
        tracing::warn!(
            panic = %panic_message(payload.as_ref()),
            "feature-hint resolution panicked; no hint"
        );
        None
    });
    hint.filter(|h| !h.is_empty())
}

// A caught panic's message, control characters escaped (it can carry disc-derived text).
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    let msg = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    msg.escape_debug().to_string()
}

// The disc's feature-playlist hint: the winning parser's manifest hint when it
// carries one, else the generic BD-J menu-walk (`bdj_feature::resolve`). Kept
// separate from extraction so a hint survives an empty/absent label set.
fn resolve_feature_hint(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    winner_hint: Option<FeaturePlaylistHint>,
    titles: &[DiscTitle],
) -> Option<FeaturePlaylistHint> {
    if let Some(hint) = winner_hint.filter(|h| !h.is_empty()) {
        return Some(hint);
    }
    // Reuse the scan's parsed playlists instead of re-reading them for Tier 2.
    let known: Vec<bdj_feature::PlaylistStat> = titles
        .iter()
        .map(|t| bdj_feature::PlaylistStat {
            id: t.playlist_id,
            secs: t.duration_secs as u64,
            audio: t
                .streams
                .iter()
                .filter(|s| matches!(s, crate::disc::Stream::Audio(a) if !a.secondary))
                .count(),
        })
        .collect();
    bdj_feature::resolve(reader, udf, &known)
}

// Min streams of one type before a language sequence anchors the label
// list (see find_anchor): a 1-stream match is a coin flip (some
// single-audio menu clip always matches label #1).
const MIN_ANCHOR_STREAMS: usize = 2;

// `stream_number` meaning "no STN slot stated". STN slots are 1-based,
// so 0 is unused; only stream_id-bearing labels may carry it.
const NO_STN_SLOT: u16 = 0;

// A stated feature playlist runs at least this long (seconds); a sub-minute
// stated duration is a decoy (Fox name=feature, Paramount _Start_Angle), never
// the feature. Inert when no duration is stated. Shared by fox and paramount.
pub(crate) const MIN_FEATURE_SECS: u64 = 60;

// Two ISO 639-2 codes that do NOT contradict: equal (case/padding
// insensitive), or either side is empty ("unknown", never a contradiction).
fn languages_compatible(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    a.is_empty() || b.is_empty() || same_language(a, b)
}

// ISO 639-2/B -> /T for the twenty languages that differ. MPLS keeps the raw /B code while
// vendor labels go through vocab::lang (/T).
fn to_iso639_t(code: &str) -> String {
    let c = code.to_ascii_lowercase();
    let t = match c.as_str() {
        "alb" => "sqi",
        "arm" => "hye",
        "baq" => "eus",
        "bur" => "mya",
        "chi" => "zho",
        "cze" => "ces",
        "dut" => "nld",
        "fre" => "fra",
        "geo" => "kat",
        "ger" => "deu",
        "gre" => "ell",
        "ice" => "isl",
        "mac" => "mkd",
        "mao" => "mri",
        "may" => "msa",
        "per" => "fas",
        "rum" => "ron",
        "slo" => "slk",
        "tib" => "bod",
        "wel" => "cym",
        _ => return c,
    };
    t.to_string()
}

fn same_language(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b) || to_iso639_t(a) == to_iso639_t(b)
}

/// Stricter form of [`languages_compatible`]: both sides actually state a
/// language and they are the same. "Unknown" is not agreement.
fn languages_agree(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim(), b.trim());
    !a.is_empty() && same_language(a, b)
}

// The VENDOR label occupying 1-based STN slot `n` of `stream_type`, if any (derived labels bind
// by StreamId instead, not by slot).
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

// Find the title the label list is actually describing, for one stream type: the title whose
// per-slot language sequence best matches (fewest contradictions, most confirmations).
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
        // A zero-confirmation title (e.g. all-empty label languages) proves nothing.
        let Some(score) = anchor_score(labels, title, stream_type).filter(|&s| s > 0) else {
            continue;
        };
        if best.is_none_or(|(bs, bn, _)| (score, n) > (bs, bn)) {
            best = Some((score, n, idx));
        }
    }
    best.map(|(_, _, idx)| idx)
}

// How strongly this title matches the vendor label list (count of confirmed slots), or None if
// it contradicts and can't be the table.
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

// Apply a pre-extracted set of labels to titles' streams: 4 tiers, most certain first — anchor
// title, anchor-proved PID, StreamId, then language-checked ordinal fallback.
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
        // The clip whose STN table this title's stream list was built from —
        // `disc::bluray` takes the streams from the first play item — so it is
        // the clip half of every id that can match a stream of this title.
        let clip0 = title
            .clips
            .first()
            .map(|c| c.clip_id.clone())
            .unwrap_or_default();

        // Anchor PID facts for this title: only those recorded against clip0, the
        // clip its stream table came from (a later clip may reuse a PID).
        let known_pids: HashMap<u16, usize> = pid_map
            .iter()
            .filter(|((clip, _), _)| *clip == clip0)
            .map(|((_, pid), pos)| (*pid, *pos))
            .collect();

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
                // A dependent extension track holds no vendor slot and keeps its marker label.
                Stream::Audio(a) if a.is_mp2_extension() => {}
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

// Does codec_hint name a codec consistent with the stream's actual codec (matched by family,
// e.g. Atmos on TrueHD)? Rejects shuffled-label mis-binds.
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
        // Atmos rides either carrier; it only fits when the hint doesn't name the other one.
        Codec::TrueHd => says_truehd || (says_atmos && !says_ddp && !says_ac3),
        Codec::Ac3Plus => says_ddp || (says_atmos && !says_truehd && !says_ac3),
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

/// The friendly name of an audio track's codec and channels ("Dolby Digital 5.1", "DTS-HD
/// Master Audio 7.1"), as the library names a track whose disc gives no label of its own.
pub fn audio_codec_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
) -> String {
    generate_audio_label(codec, channels, false)
}

pub(crate) fn generate_audio_label(
    codec: &crate::disc::Codec,
    channels: &crate::disc::AudioChannels,
    secondary: bool,
) -> String {
    generate_audio_label_inner(codec, channels, secondary, false)
}

// Atmos-aware variant of generate_audio_label: folds the object-audio marker into the codec
// brand (e.g. "Dolby TrueHD Atmos 7.1").
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
        AudioChannels::Surround30 => "3.0",
        AudioChannels::Surround31 => "3.1",
        AudioChannels::Quad => "4.0",
        AudioChannels::Surround41 => "4.1",
        AudioChannels::Surround50 => "5.0",
        AudioChannels::Surround51 => "5.1",
        AudioChannels::Surround60 => "6.0",
        AudioChannels::Surround61 => "6.1",
        AudioChannels::Surround70 => "7.0",
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
    // names every stream a playlist references (parsed once, in the loop above).
    if name != "mpls_universal"
        && let Some((_, mpls_result)) = candidates.iter().find(|(n, _)| *n == "mpls_universal")
    {
        merge_mpls_floor(&mut labels, &mpls_result.labels);
    }

    (labels, feature_playlist)
}

// Merge the MPLS-derived floor into a framework list. Nothing is merged BY slot (the lists
// don't share a coordinate system); unnamed MPLS streams are appended by StreamId.
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

// Pick the winning parser result: highest Confidence among non-empty results, earliest array
// position wins ties (matches extract()'s scan).
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
    // the chosen parser (its labels ARE the labels).
    let gap_fill_added = if parser.is_some() && parser != Some("mpls_universal") {
        let before = labels.len();
        // The registry loop above already parsed MPLS; reuse that result.
        if let Some((_, mpls_result)) = all_results.iter().find(|(n, _)| *n == "mpls_universal") {
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

// Scan /BDMV/PLAYLIST/*.mpls for a chapter-count + duration row per playlist, sorted by
// filename. Unparseable/markless entries silently dropped.
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
        // Not sample-accurate, just enough to identify "the long one" (main movie).
        let duration_secs = playlist.duration_ticks() as f64 / 45000.0;
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
    /// Per-playlist chapter summary rows ([`ChapterSummary`]): playlist filename,
    /// chapter count, duration in seconds.
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

// List filenames under any /BDMV/JAR/<x>/ subdirectory, deduped and sorted (empty if none).
// pub(crate) so filename parsers can scan menu-asset names without a reader.
pub(crate) fn jar_inventory(udf: &UdfFs) -> Vec<String> {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return Vec::new();
    };
    jar_inventory_from(&jar_dir.entries)
}

// Body of jar_inventory, unit-testable without a UdfFs. Uses a BTreeSet (not Vec::contains)
// since entry names are attacker-controlled disc data.
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

/// Every BDMV/JAR subdirectory path holding `filename`, in directory order.
fn jar_file_paths(udf: &UdfFs, filename: &str) -> Vec<String> {
    let Some(jar_dir) = udf.find_dir("/BDMV/JAR") else {
        return Vec::new();
    };
    jar_dir
        .entries
        .iter()
        .filter(|e| e.is_dir)
        .filter(|e| {
            e.entries
                .iter()
                .any(|c| !c.is_dir && c.name.eq_ignore_ascii_case(filename))
        })
        .map(|e| format!("/BDMV/JAR/{}/{}", e.name, filename))
        .collect()
}

/// Find a file in any BDMV/JAR subdirectory, return its path.
pub(crate) fn find_jar_file(udf: &UdfFs, filename: &str) -> Option<String> {
    jar_file_paths(udf, filename).into_iter().next()
}

/// Read a file from any BDMV/JAR subdirectory by filename; the first copy
/// that reads successfully and is non-empty wins.
pub(crate) fn read_jar_file(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    filename: &str,
) -> Option<Vec<u8>> {
    jar_file_paths(udf, filename)
        .into_iter()
        .find_map(|path| udf.read_file(reader, &path).ok().filter(|d| !d.is_empty()))
}

// ── Registry-level tests ────────────────────────────────────────────────────

#[cfg(test)]
#[path = "mod_registry_tests.rs"]
mod registry_tests;

// ── MPLS floor merge tests ─────────────────────────────────────────────────

#[cfg(test)]
#[path = "mod_gap_fill_tests.rs"]
mod gap_fill_tests;

// ── apply() integration tests ──────────────────────────────────────────────
// End-to-end coverage for apply_labels + fill_defaults without needing a
// SectorSource / UdfFs. Synthetic DiscTitle + StreamLabel inputs.

#[cfg(test)]
#[path = "mod_apply_tests.rs"]
mod apply_tests;

// ── fill_gaps_from_mpls: no-op-when-nothing-added hardening ────────────────

#[cfg(test)]
#[path = "mod_fill_gaps_sort_tests.rs"]
mod fill_gaps_sort_tests;

// ── CLPI fixtures (shared with clpi_audit tests) ────────────────────────────

#[cfg(test)]
#[path = "mod_clpi_orphan_tests.rs"]
mod clpi_orphan_tests;

// ── apply(): independent feature-hint pass (issue #45 menu-walk) ─────────────

#[cfg(test)]
#[path = "mod_feature_hint_pass_tests.rs"]
mod feature_hint_pass_tests;

// ── Label text tables, ordering, and extraction ───────────────────────────

#[cfg(test)]
#[path = "mod_label_table_tests.rs"]
mod label_table_tests;

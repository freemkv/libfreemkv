//! The static facts of every URL scheme, as one const table: what it reads as, what it
//! writes as, what its path names. Every scheme predicate derives from [`StreamUrl::info`].

use super::resolve::StreamUrl;
use super::videomap::Medium;
use crate::disc::CodecKind;

/// How a scheme opens as an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// A live optical drive (`disc://`).
    Drive,
    /// A disc image carrying a UDF volume (`iso://`, `dir://`): scanned into titles.
    Image,
    /// A stream container file (`m2ts://`, `mkv://`, `mp4://`, `mpg://`).
    Container,
    /// The FMKV wire (`network://`, `stdio://`).
    Wire,
}

/// How a scheme opens as an output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    /// A PES sink, opened by [`super::output`].
    Pes,
    /// A sector image, opened by [`crate::io::open_block_sink`].
    Block,
    /// A file tree, opened by [`crate::io::open_tree_sink`].
    Tree,
}

/// What the part of the URL after `scheme://` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlTarget {
    /// One filesystem file.
    File,
    /// A filesystem folder.
    Dir,
    /// A device path, empty for autodetect.
    Device,
    /// A `host:port` address.
    Address,
    /// Nothing: the scheme alone is the whole URL (`null://`, `stdio://`).
    None,
}

/// The static facts of one scheme (see [`StreamUrl::info`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemeInfo {
    /// The scheme name, without `://`.
    pub name: &'static str,
    /// How it opens as an input; `None`: output only.
    pub source: Option<SourceKind>,
    /// How it opens as an output; `None`: input only.
    pub sink: Option<SinkKind>,
    /// What its path names.
    pub target: UrlTarget,
    /// The provenance medium it records as a source ([`Medium::File`] for an output-only one).
    pub medium: Medium,
    /// The one track class a per-track output keeps; `None` keeps every class.
    pub track_filter: Option<CodecKind>,
}

const fn info(
    name: &'static str,
    source: Option<SourceKind>,
    sink: Option<SinkKind>,
    target: UrlTarget,
    medium: Medium,
) -> SchemeInfo {
    SchemeInfo {
        name,
        source,
        sink,
        target,
        medium,
        track_filter: None,
    }
}

const fn tracks(name: &'static str, kind: Option<CodecKind>) -> SchemeInfo {
    SchemeInfo {
        track_filter: kind,
        ..info(
            name,
            None,
            Some(SinkKind::Pes),
            UrlTarget::Dir,
            Medium::File,
        )
    }
}

const fn sink(name: &'static str, target: UrlTarget) -> SchemeInfo {
    info(name, None, Some(SinkKind::Pes), target, Medium::File)
}

const fn container(name: &'static str) -> SchemeInfo {
    info(
        name,
        Some(SourceKind::Container),
        Some(SinkKind::Pes),
        UrlTarget::File,
        Medium::File,
    )
}

const fn wire(name: &'static str, target: UrlTarget) -> SchemeInfo {
    info(
        name,
        Some(SourceKind::Wire),
        Some(SinkKind::Pes),
        target,
        Medium::Stream,
    )
}

const DISC: SchemeInfo = info(
    "disc",
    Some(SourceKind::Drive),
    None,
    UrlTarget::Device,
    Medium::Disc,
);
const ISO: SchemeInfo = info(
    "iso",
    Some(SourceKind::Image),
    Some(SinkKind::Block),
    UrlTarget::File,
    Medium::Iso,
);
// A folder source is a UDF volume synthesized over it, so it records as an image.
const DIR: SchemeInfo = info(
    "dir",
    Some(SourceKind::Image),
    Some(SinkKind::Tree),
    UrlTarget::Dir,
    Medium::Iso,
);
const M2TS: SchemeInfo = container("m2ts");
const MKV: SchemeInfo = container("mkv");
const MP4: SchemeInfo = container("mp4");
const MPG: SchemeInfo = container("mpg");
const NETWORK: SchemeInfo = wire("network", UrlTarget::Address);
const STDIO: SchemeInfo = wire("stdio", UrlTarget::None);
const NULL: SchemeInfo = sink("null", UrlTarget::None);
const DEMUX: SchemeInfo = tracks("demux", None);
const VIDEO: SchemeInfo = tracks("video", Some(CodecKind::Video));
const AUDIO: SchemeInfo = tracks("audio", Some(CodecKind::Audio));
const SUB: SchemeInfo = tracks("sub", Some(CodecKind::Subtitle));
const FVI: SchemeInfo = sink("fvi", UrlTarget::File);
const CHAPTERS: SchemeInfo = sink("chapters", UrlTarget::File);
const JSON: SchemeInfo = sink("json", UrlTarget::File);
const UNKNOWN: SchemeInfo = info("unknown", None, None, UrlTarget::None, Medium::File);

const ALL: [&SchemeInfo; 17] = [
    &DISC, &M2TS, &MKV, &MP4, &MPG, &NETWORK, &STDIO, &ISO, &DIR, &NULL, &DEMUX, &VIDEO, &AUDIO,
    &SUB, &FVI, &CHAPTERS, &JSON,
];

impl SchemeInfo {
    /// The info of the known scheme `name` (no `://`); `None` for an unknown one.
    pub fn by_name(name: &str) -> Option<&'static SchemeInfo> {
        ALL.into_iter().find(|i| i.name == name)
    }

    /// A per-track elementary-stream output: a PES sink writing into a folder.
    pub fn is_track_output(&self) -> bool {
        self.sink == Some(SinkKind::Pes) && self.target == UrlTarget::Dir
    }

    /// An output whose URL is the scheme alone: no file, folder or address to name.
    pub fn is_scheme_only_sink(&self) -> bool {
        self.sink.is_some() && self.target == UrlTarget::None
    }
}

impl StreamUrl {
    /// The static facts of this URL's scheme.
    pub fn info(&self) -> &'static SchemeInfo {
        match self {
            StreamUrl::Disc { .. } => &DISC,
            StreamUrl::M2ts { .. } => &M2TS,
            StreamUrl::Mkv { .. } => &MKV,
            StreamUrl::Mp4 { .. } => &MP4,
            StreamUrl::Mpg { .. } => &MPG,
            StreamUrl::Network { .. } => &NETWORK,
            StreamUrl::Stdio => &STDIO,
            StreamUrl::Iso { .. } => &ISO,
            StreamUrl::Dir { .. } => &DIR,
            StreamUrl::Null => &NULL,
            StreamUrl::Demux { .. } => &DEMUX,
            StreamUrl::Video { .. } => &VIDEO,
            StreamUrl::Audio { .. } => &AUDIO,
            StreamUrl::Sub { .. } => &SUB,
            StreamUrl::Fvi { .. } => &FVI,
            StreamUrl::Chapters { .. } => &CHAPTERS,
            StreamUrl::Json { .. } => &JSON,
            StreamUrl::Unknown { .. } => &UNKNOWN,
        }
    }
}

#[cfg(test)]
#[path = "scheme_tests.rs"]
mod tests;

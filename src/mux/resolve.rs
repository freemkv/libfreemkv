//! Stream URL resolver — parses URL strings into PES stream instances.
//!
//! Format: `scheme://path`.
//!
//! Every input scheme, `disc://` included, opens through [`super::source::open_source`];
//! [`input`] is that plus the PES stream of one title.

use super::network::NetworkStream;
use super::null::NullStream;
use super::pipelined_stream::PipelinedPesStream;
use super::stdio::StdioStream;
use super::{M2tsStream, MkvStream};
use crate::disc::{ContentFormat, DiscTitle};
use crate::sector::SectorSource;
use crate::sector::stage::Stage;
use std::io;
use std::path::{Path, PathBuf};

/// I/O buffer size for file streams.
const IO_BUF_SIZE: usize = 4 * 1024 * 1024;

/// File-backed ISO mux read size. Empirically optimal for the CLI: 16 MiB;
/// 32 MiB regressed from cache pressure and longer per-batch latency.
/// Image remux uses the same reader pipeline and should use this value too.
pub const ISO_MUX_BATCH_SECTORS: u16 = 8192;

/// Parsed stream URL.
#[derive(Debug, Clone)]
pub enum StreamUrl {
    /// Optical disc drive. Device path is optional (auto-detect if None).
    Disc { device: Option<PathBuf> },
    /// MPEG-2 transport stream file.
    M2ts { path: PathBuf },
    /// Matroska container file.
    Mkv { path: PathBuf },
    /// Progressive MP4 (ISO-BMFF) mux output (`mp4://`). Like `mkv://` but writes
    /// a single self-contained `.mp4` (ftyp+mdat+moov). Compatibility export —
    /// carries only MP4-mappable codecs; see `mux::mp4`.
    Mp4 { path: PathBuf },
    /// MPEG-2 program stream (`mpg://`, ISO/IEC 13818-1): the DVD-core carriage of
    /// `mux::mpg`; tracks it cannot carry are excluded with a reason.
    Mpg { path: PathBuf },
    /// Network stream (host:port).
    Network { addr: String },
    /// Standard I/O (stdin/stdout).
    Stdio,
    /// ISO disc image file.
    Iso { path: PathBuf },
    /// An extracted disc file tree (`dir://`) — a source AND a sink.
    ///
    /// As a SINK it writes per-file decrypted bytes rather than muxed PES
    /// frames, so it never flows through `output()`; the CLI routes a `Dir`
    /// dest to `Disc::extract_tree`.
    ///
    /// As a SOURCE (1.6.1) it is an image-level source: `crate::dirimage`
    /// synthesizes a real UDF volume over the folder, so it reaches the same
    /// scan/mux path `iso://` does and every destination follows.
    Dir { path: PathBuf },
    /// Null sink (write-only, discards data).
    Null,
    /// Per-track elementary-stream output directory (`demux://`). A write-only
    /// sink that fans each track of a title out to its own ES file (plus
    /// chapters + delay metadata). Like `dir://` it targets a directory; the
    /// CLI constructs the `DemuxSink` with full options before the mux loop.
    Demux { dir: PathBuf },
    /// Video-only per-track output directory (`video://`) — a `demux://`
    /// restricted to video tracks (native elementary streams: `.hevc`, `.h264`,
    /// `.vc1`, `.m2v`, …). One file per video track; no audio/subtitles.
    Video { dir: PathBuf },
    /// Audio-only per-track output directory (`audio://`) — a `demux://`
    /// restricted to audio tracks (native containers: `.thd`, `.dts`, `.ac3`,
    /// `.eac3`, `.pcm`, …). One file per audio track; no video/subtitles.
    Audio { dir: PathBuf },
    /// Subtitle-only per-track output directory (`sub://`) — a `demux://`
    /// restricted to subtitle tracks (PGS `.sup`, VobSub `.idx`+`.sub`, text
    /// `.srt`). One file per subtitle track.
    Sub { dir: PathBuf },
    /// freemkv native per-picture video index (`fvi://`). A write-only PES sink that emits one
    /// JSON-Lines record per coded picture of the title's primary video track to a `.fvi` file.
    Fvi { path: PathBuf },
    /// Chapter-marker export (`chapters://`). A write-only sink that ignores the
    /// PES stream and writes the title's chapter points to a single file, format
    /// chosen by the output extension: `.xml` (Matroska, default), `.txt` (OGM),
    /// `.vtt` (WebVTT).
    Chapters { path: PathBuf },
    /// Structured title/stream/chapter metadata (`json://`). A write-only sink
    /// that ignores the PES stream and writes the selected title's model as one
    /// JSON document — machine-readable `info` for one title.
    Json { path: PathBuf },
    /// Unrecognized URL.
    Unknown { raw: String },
}

impl StreamUrl {
    /// The scheme name (e.g. "disc", "mkv", "null").
    pub fn scheme(&self) -> &str {
        match self {
            StreamUrl::Disc { .. } => "disc",
            StreamUrl::M2ts { .. } => "m2ts",
            StreamUrl::Mkv { .. } => "mkv",
            StreamUrl::Mp4 { .. } => "mp4",
            StreamUrl::Mpg { .. } => "mpg",
            StreamUrl::Network { .. } => "network",
            StreamUrl::Stdio => "stdio",
            StreamUrl::Iso { .. } => "iso",
            StreamUrl::Dir { .. } => "dir",
            StreamUrl::Null => "null",
            StreamUrl::Demux { .. } => "demux",
            StreamUrl::Video { .. } => "video",
            StreamUrl::Audio { .. } => "audio",
            StreamUrl::Sub { .. } => "sub",
            StreamUrl::Fvi { .. } => "fvi",
            StreamUrl::Chapters { .. } => "chapters",
            StreamUrl::Json { .. } => "json",
            StreamUrl::Unknown { .. } => "unknown",
        }
    }

    /// The path/address component, or empty string for scheme-only URLs.
    pub fn path_str(&self) -> &str {
        match self {
            StreamUrl::Disc { device: Some(p) } => p.to_str().unwrap_or(""),
            StreamUrl::Disc { device: None } => "",
            StreamUrl::M2ts { path }
            | StreamUrl::Mkv { path }
            | StreamUrl::Mp4 { path }
            | StreamUrl::Mpg { path }
            | StreamUrl::Iso { path }
            | StreamUrl::Dir { path }
            | StreamUrl::Demux { dir: path }
            | StreamUrl::Video { dir: path }
            | StreamUrl::Audio { dir: path }
            | StreamUrl::Sub { dir: path }
            | StreamUrl::Fvi { path }
            | StreamUrl::Chapters { path }
            | StreamUrl::Json { path } => path.to_str().unwrap_or(""),
            StreamUrl::Network { addr } => addr,
            StreamUrl::Stdio | StreamUrl::Null => "",
            StreamUrl::Unknown { raw } => raw,
        }
    }

    /// Whether this URL is an IMAGE-level source — one that carries a UDF
    /// filesystem, so it can be scanned into a title list, have `-t`/`-a`/`-s`
    /// applied, and feed either a PES sink or an image sink.
    ///
    /// `dir://` joined in 1.6.1 via a synthesized UDF volume (`crate::dirimage`).
    ///
    /// NOT the same predicate as the CLI's `engine::is_disc_source`, which
    /// means "is a live drive" and drives tray/eject behaviour. That one must
    /// never gain `Dir` — a directory routed down the live-drive rip path
    /// would open, lock and eject a drive that has nothing to do with it.
    pub fn is_disc_source(&self) -> bool {
        matches!(
            self,
            StreamUrl::Disc { .. } | StreamUrl::Iso { .. } | StreamUrl::Dir { .. }
        )
    }
}

/// Parse a URL string into a typed StreamUrl.
pub fn parse_url(url: &str) -> StreamUrl {
    // `disk://` is an accepted alias for `disc://` (identical behavior):
    // empty = auto-detect, path = device. Windows users commonly type
    // `disk://i:` after the drive-letter convention; honor both spellings.
    if let Some(rest) = url
        .strip_prefix("disc://")
        .or_else(|| url.strip_prefix("disk://"))
    {
        return if rest.is_empty() {
            StreamUrl::Disc { device: None }
        } else {
            StreamUrl::Disc {
                device: Some(PathBuf::from(rest)),
            }
        };
    }
    if let Some(rest) = url.strip_prefix("m2ts://") {
        return StreamUrl::M2ts {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("mkv://") {
        return StreamUrl::Mkv {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("mp4://") {
        return StreamUrl::Mp4 {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("mpg://") {
        return StreamUrl::Mpg {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("network://") {
        return StreamUrl::Network {
            addr: rest.to_string(),
        };
    }
    if let Some(rest) = url.strip_prefix("null://") {
        // null:// / stdio:// are scheme-only; a trailing path is
        // malformed and must fall through to Unknown rather than be
        // silently discarded.
        if rest.is_empty() {
            return StreamUrl::Null;
        }
    }
    if let Some(rest) = url.strip_prefix("stdio://")
        && rest.is_empty()
    {
        return StreamUrl::Stdio;
    }
    if let Some(rest) = url.strip_prefix("iso://") {
        return StreamUrl::Iso {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("dir://") {
        return StreamUrl::Dir {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("demux://") {
        return StreamUrl::Demux {
            dir: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("video://") {
        return StreamUrl::Video {
            dir: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("audio://") {
        return StreamUrl::Audio {
            dir: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("sub://") {
        return StreamUrl::Sub {
            dir: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("chapters://") {
        return StreamUrl::Chapters {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("json://") {
        return StreamUrl::Json {
            path: PathBuf::from(rest),
        };
    }
    if let Some(rest) = url.strip_prefix("fvi://") {
        return StreamUrl::Fvi {
            path: PathBuf::from(rest),
        };
    }
    StreamUrl::Unknown {
        raw: url.to_string(),
    }
}

/// Validate that a file path is non-empty and has a filename component.
fn validate_file_path(path: &Path, scheme: &str) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(crate::error::Error::StreamUrlMissingPath {
            scheme: scheme.to_string(),
        }
        .into());
    }
    if path.file_name().is_none() {
        return Err(crate::error::Error::StreamUrlInvalid {
            url: format!("{scheme}://{}", path.display()),
        }
        .into());
    }
    Ok(())
}

/// Validate that a network address has host:port format.
fn validate_network_addr(addr: &str) -> io::Result<()> {
    if addr.is_empty() {
        return Err(crate::error::Error::StreamUrlMissingPath {
            scheme: "network".to_string(),
        }
        .into());
    }
    // A bare IPv6 literal ("::1") contains ':' but has no port, so the plain
    // `contains(':')` check would wrongly pass it and yield an untyped
    // io::Error later; treat anything parsing as IpAddr as port-less.
    if addr.parse::<std::net::IpAddr>().is_ok() {
        return Err(crate::error::Error::StreamUrlMissingPort {
            addr: addr.to_string(),
        }
        .into());
    }
    if !addr.contains(':') {
        return Err(crate::error::Error::StreamUrlMissingPort {
            addr: addr.to_string(),
        }
        .into());
    }
    // Split on the LAST ':' so a bracketed IPv6 literal (`[::1]:9000`) splits
    // at the port colon, not an address colon. Port must be a non-empty u16 —
    // `host:` and `host:abc` are invalid despite containing ':'.
    let port = match addr.rsplit_once(':') {
        Some((_host, port)) => port,
        None => {
            return Err(crate::error::Error::StreamUrlMissingPort {
                addr: addr.to_string(),
            }
            .into());
        }
    };
    if port.is_empty() || port.parse::<u16>().is_err() {
        return Err(crate::error::Error::StreamUrlInvalid {
            url: addr.to_string(),
        }
        .into());
    }
    Ok(())
}

/// The AACS disc folder a loose Blu-ray clip sits in, found only by walking up from `file`:
/// `<root>/BDMV/STREAM/x.m2ts` (or `STREAM/SSIF/x.ssif`) with `<root>/AACS`. Open `<root>` as
/// `dir://`, resolve its keys, and pass them in [`InputOptions::keys`]. `None`: no disc
/// structure, so an encrypted clip refuses (E7022). A `.vob` needs none: CSS self-cracks.
pub fn disc_root_of(file: &Path) -> Option<PathBuf> {
    let file = std::fs::canonicalize(file).ok()?;
    let up = |p: &Path, name: &str| {
        let parent = p.parent()?;
        let dir = parent.file_name()?.to_str()?;
        dir.eq_ignore_ascii_case(name).then(|| parent.to_path_buf())
    };
    let stream = up(&file, "STREAM").or_else(|| up(&up(&file, "SSIF")?, "STREAM"))?;
    let root = up(&stream, "BDMV")?.parent()?.to_path_buf();
    let aacs = std::fs::read_dir(&root).ok()?.flatten().any(|e| {
        e.file_name()
            .to_str()
            .is_some_and(|n| n.eq_ignore_ascii_case("AACS"))
            && e.path().is_dir()
    });
    aacs.then_some(root)
}

/// Options for opening an input stream.
#[derive(Clone, Default)]
pub struct InputOptions {
    /// 0-based title index to open; `None` selects title 0. An
    /// out-of-range index yields [`crate::error::Error::DiscTitleRange`].
    pub title_index: Option<usize>,
    /// Skip decryption — return raw encrypted bytes.
    pub raw: bool,
    /// Which audio/subtitle streams to keep. `input()` scans the source and
    /// picks the title internally, so the caller can't prune the `DiscTitle`
    /// itself — it passes the selection here and `input()` applies it right
    /// after the title-index bounds check. Default keeps every stream (video is
    /// always kept). See [`crate::StreamSelection`].
    pub selection: crate::StreamSelection,
    /// The rip's up-front key set (KU §3.1). An AACS image is read through the set's
    /// reader, with no lookup here; `None` refuses an AACS image before any output.
    pub keys: Option<crate::keys::KeyRing>,
}

// Hand-rolled so `InputOptions` stays printable without dumping key material.
impl std::fmt::Debug for InputOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputOptions")
            .field("title_index", &self.title_index)
            .field("raw", &self.raw)
            .field("selection", &self.selection)
            .field("keys", &self.keys.is_some())
            .finish()
    }
}

/// Open a PES input stream (produces PES frames) for the run `ctx`: [`open_source`] then the
/// PES stream of `opts.title_index`. Every input scheme opens here, `disc://` included (the
/// drive is brought up and scanned with default [`crate::disc::ScanOptions`]). Its halt
/// reaches the scan, crack, reads and demux and the network receive (`stdio://`, `mkv://`,
/// `mp4://` read inline); image and PS reads report `BytesRead`; stats count loss.
///
/// [`open_source`]: super::source::open_source
pub fn input(
    url: &str,
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
) -> io::Result<Box<dyn crate::pes::PesSource>> {
    let source = super::source::open_source(url, crate::disc::ScanOptions::default(), ctx)?;
    let mux = super::driver::MuxOptions {
        raw: opts.raw,
        selection: opts.selection.clone(),
        title_index: opts.title_index.unwrap_or(0),
        ..Default::default()
    };
    super::driver::open_pes(source, opts.keys.as_ref(), &mux, ctx)
}

pub(crate) fn validate_path(path: &Path, scheme: &str) -> crate::error::Result<()> {
    validate_file_path(path, scheme).map_err(crate::error::Error::from)
}

// The container and stream inputs (`m2ts://`, `mkv://`, `mp4://`, `mpg://`, `network://`,
// `stdio://`), each through the one decryption stage.
pub(crate) fn open_stream_url(
    url: &str,
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
) -> io::Result<Box<dyn crate::pes::PesSource>> {
    let source = open_container(url, opts, ctx)?;
    if matches!(parse_url(url), StreamUrl::Mpg { .. }) {
        // A program stream is demuxed here: its title was pruned before the demux.
        return Ok(source);
    }
    // A container's tracks arrive framed: the selection keeps a subset of them (DM5).
    super::selected::SelectedSource::wrap(source, &opts.selection).map_err(io::Error::from)
}

// The container or stream input `url`, all of its tracks.
fn open_container(
    url: &str,
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
) -> io::Result<Box<dyn crate::pes::PesSource>> {
    let parsed = parse_url(url);
    match parsed {
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let len = std::fs::metadata(path)?.len();
            let stage = crate::sector::DecryptingSectorSource::new(
                Box::new(crate::io::file_sector_source::FileSectorSource::open_padded(path)?)
                    as Box<dyn SectorSource>,
                crate::sector::Keying::detect(stage_options(opts, ctx, opts.raw)),
            );
            let blanked = stage.blanked_counter();
            let reader = crate::sector::stage::SectorBytes::new(stage, len);
            Ok(Box::new(
                build_m2ts_pipeline(reader, len, ctx)?.with_blanked(blanked),
            ))
        }
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            let staged = Stage::eager(std::fs::File::open(path)?, opts.raw)?;
            let reader = std::io::BufReader::with_capacity(IO_BUF_SIZE, staged);
            Ok(Box::new(MkvStream::open(reader)?))
        }
        StreamUrl::Network { ref addr } => {
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::listen_staged(
                addr,
                Some(ctx.halt.clone()),
                opts.raw,
            )?))
        }
        StreamUrl::Stdio => {
            let mut stdio = StdioStream::input_staged(opts.raw);
            // A selection is resolved against the title at open: read the header first.
            if !opts.selection.is_all() {
                stdio.prime()?;
            }
            Ok(Box::new(stdio))
        }
        StreamUrl::Null => Err(crate::error::Error::StreamWriteOnly.into()),
        // `mp4://` as a source: demux a progressive MP4 back into PES frames, so
        // `mp4://` flows to every sink (mkv://, audio://, json://, …).
        StreamUrl::Mp4 { ref path } => {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("mp4")
                .to_string();
            let staged = Stage::eager(std::fs::File::open(path)?, opts.raw)?;
            Ok(Box::new(super::mp4::Mp4Reader::from_reader(staged, name)?))
        }
        StreamUrl::Mpg { ref path } => {
            validate_file_path(path, "mpg")?;
            Ok(Box::new(build_ps_pipeline(path, opts, ctx)?))
        }
        // `demux://` is an output-only sink (per-track ES files); never a source.
        StreamUrl::Demux { .. }
        | StreamUrl::Video { .. }
        | StreamUrl::Audio { .. }
        | StreamUrl::Sub { .. }
        | StreamUrl::Chapters { .. }
        | StreamUrl::Json { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
        // `fvi://` is an output-only sink (per-picture video index); never a source.
        StreamUrl::Fvi { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
        StreamUrl::Disc { .. } | StreamUrl::Iso { .. } | StreamUrl::Dir { .. } => {
            Err(crate::error::Error::StreamUrlInvalid {
                url: url.to_string(),
            }
            .into())
        }
        StreamUrl::Unknown { ref raw } => {
            Err(crate::error::Error::StreamUrlInvalid { url: raw.clone() }.into())
        }
    }
}

// Shared body of every IMAGE-level PES source (`iso://`/`dir://`) over the probe's reader
// and layout (`open_source` scanned it, and judged a folder's AACS verdict from content).
pub(crate) fn image_input_scanned<S>(
    reader: S,
    mut disc: crate::disc::Disc,
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream>
where
    S: SectorSource + Send + 'static,
{
    if let Some(set) = opts.keys.as_ref().filter(|s| s.is_aacs() && !opts.raw) {
        return keyed_image_input(reader, disc, opts, set, ctx);
    }
    // Pre-flight decrypt gate: fails fast on a scrambled-but-uncracked CSS disc or an
    // AACS disc with no key set (else muxes garbage). Per-title CSS is checked below.
    crate::keys::disc_gate(&disc, opts.raw).map_err(|e| -> io::Error { e.into() })?;
    if disc.titles.is_empty() {
        return Err(crate::error::Error::NoStreams.into());
    }
    let idx = opts.title_index.unwrap_or(0);
    if idx >= disc.titles.len() {
        return Err(crate::error::Error::DiscTitleRange {
            index: idx,
            count: disc.titles.len(),
        }
        .into());
    }
    // Prune to selected audio/subtitle streams now, so the title clone and
    // `build_iso_pipeline` see the pruned list. Video always kept.
    opts.selection
        .apply(&mut disc.titles[idx])
        .map_err(|e| -> io::Error { e.into() })?;
    // DVD CSS resolves at exactly one site, `build_iso_pipeline`'s per-title
    // crack below, so a DVD passes `None` here, as does `--raw` (passthrough).
    let is_dvd = disc.format == crate::disc::DiscFormat::Dvd;
    let keys = if opts.raw || is_dvd {
        crate::decrypt::DecryptKeys::None
    } else {
        disc.decrypt_keys()
    };
    let title = disc.titles[idx].clone();
    let format = disc.content_format;
    // `--raw` keys are already `None`: the read stack still flows through the same
    // producer+demux+parse pipeline, just without the AACS/CSS step.
    let stream = build_iso_pipeline(
        reader,
        title,
        keys,
        ISO_MUX_BATCH_SECTORS,
        format,
        opts.raw,
        ctx,
    )?;
    Ok(stream)
}

// `image_input_scanned` over the rip's key set (KU §3.1, §3.5): the gate is `check_decryptable`,
// and the mux reads through the set's reader. No lookup, no banking.
fn keyed_image_input<S>(
    reader: S,
    mut disc: crate::disc::Disc,
    opts: &InputOptions,
    set: &crate::keys::KeyRing,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream>
where
    S: SectorSource + Send + 'static,
{
    if disc.titles.is_empty() {
        return Err(crate::error::Error::NoStreams.into());
    }
    let idx = opts.title_index.unwrap_or(0);
    if idx >= disc.titles.len() {
        return Err(crate::error::Error::DiscTitleRange {
            index: idx,
            count: disc.titles.len(),
        }
        .into());
    }
    let scope = crate::keys::KeyScope::Titles(vec![idx]);
    crate::keys::check_decryptable(&disc, false, Some(set), &scope)
        .map_err(|e| -> io::Error { e.into() })?;
    opts.selection
        .apply(&mut disc.titles[idx])
        .map_err(|e| -> io::Error { e.into() })?;
    let title = disc.titles[idx].clone();
    build_iso_pipeline_keyed(reader, title, set, ISO_MUX_BATCH_SECTORS, ctx)
}

// A `DemuxSink` over `dir`, its base name seeded from the playlist name.
fn demux_sink_output(
    dir: &std::path::Path,
    title: &DiscTitle,
    mut opts: super::demux_sink::DemuxOptions,
) -> io::Result<Box<dyn crate::pes::PesSink>> {
    if !title.playlist.is_empty() {
        opts.base = title.playlist.clone();
    }
    Ok(Box::new(super::demux_sink::DemuxSink::create(
        dir, title, &opts,
    )?))
}

// `video://`/`audio://`/`sub://`: a `demux://` restricted to one track class, no chapters sidecar.
fn track_sink_output(
    dir: &std::path::Path,
    title: &DiscTitle,
    kind: super::demux_sink::TrackKind,
) -> io::Result<Box<dyn crate::pes::PesSink>> {
    let opts = super::demux_sink::DemuxOptions {
        kind_filter: Some(kind),
        export_chapters: false,
        ..Default::default()
    };
    demux_sink_output(dir, title, opts)
}

/// What an output scheme can take (pipeline design §2.2 sink caps): the one table the mux
/// consults, instead of matching on schemes in the pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkCaps {
    /// The sink consumes PES frames. `false` for `chapters://` and `json://`, which write
    /// their whole file from the (header-completed) title when opened.
    pub needs_frames: bool,
    /// The sink writes a DVD MPEG-2 multichannel extension track (13818-3 `ext_frame`s).
    /// `mkv://`, `mp4://`, `m2ts://`, `demux://` and `audio://` have no mapping for it.
    pub carries_mp2_extensions: bool,
}

impl SinkCaps {
    /// The caps of the output scheme `url`.
    pub fn of(url: &StreamUrl) -> SinkCaps {
        let full = SinkCaps {
            needs_frames: true,
            carries_mp2_extensions: true,
        };
        match url {
            StreamUrl::Chapters { .. } | StreamUrl::Json { .. } => SinkCaps {
                needs_frames: false,
                ..full
            },
            StreamUrl::Mkv { .. }
            | StreamUrl::Mp4 { .. }
            | StreamUrl::M2ts { .. }
            | StreamUrl::Demux { .. }
            | StreamUrl::Audio { .. } => SinkCaps {
                carries_mp2_extensions: false,
                ..full
            },
            StreamUrl::Mpg { .. }
            | StreamUrl::Network { .. }
            | StreamUrl::Stdio
            | StreamUrl::Null
            | StreamUrl::Video { .. }
            | StreamUrl::Sub { .. }
            | StreamUrl::Fvi { .. }
            | StreamUrl::Disc { .. }
            | StreamUrl::Iso { .. }
            | StreamUrl::Dir { .. }
            | StreamUrl::Unknown { .. } => full,
        }
    }
}

/// Open a PES output stream (consumes PES frames).
///
/// `source` is the provenance of the material being written — the INPUT the caller is muxing
/// from, not `url`. Only the `fvi://` sink consumes it; every other sink ignores it. `None`
/// means "no provenance to declare": the header's `source` members then carry their neutral
/// defaults and the optional ones are omitted, rather than being back-filled with the
/// destination path — which is exactly the bug this parameter exists to prevent.
pub fn output(
    url: &str,
    title: &crate::disc::DiscTitle,
    source: Option<&super::videomap::SourceInfo>,
) -> io::Result<Box<dyn crate::pes::PesSink>> {
    output_with(url, title, source, None)
}

/// What a file output shares with the mux (§2.10): the flush counters (the consumer's
/// progress) and the op's stop token for its flush backpressure.
pub(crate) struct OutputFlush<'a> {
    pub(crate) progress: &'a crate::io::FlushProgress,
    pub(crate) halt: &'a crate::halt::Halt,
}

// A file output's bounded-cache writer, sharing `flush` when given.
fn writeback_file(
    path: &Path,
    size_hint: u64,
    flush: Option<&OutputFlush>,
) -> io::Result<crate::io::WritebackFile> {
    let mut w = crate::io::WritebackFile::create_with_size_hint(path, size_hint)?;
    if let Some(f) = flush {
        w.set_flush_progress(f.progress.clone());
        w.set_halt(f.halt.clone());
    }
    Ok(w)
}

// [`output`], with the file outputs sharing `flush` (the mux driver's path).
pub(crate) fn output_with(
    url: &str,
    title: &crate::disc::DiscTitle,
    source: Option<&super::videomap::SourceInfo>,
    flush: Option<OutputFlush>,
) -> io::Result<Box<dyn crate::pes::PesSink>> {
    let flush = flush.as_ref();
    let parsed = parse_url(url);
    match parsed {
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            // Wrap in `WritebackFile` (bounded-cache) so a UHD mux to slow/
            // network staging avoids dirty-page bursts. BufWriter coalesces
            // small EBML writes; Linux fallocate(KEEP_SIZE) cuts fragmentation.
            let writer: Box<dyn super::WriteSeek + Send> =
                Box::new(std::io::BufWriter::with_capacity(
                    IO_BUF_SIZE,
                    writeback_file(path, title.size_bytes, flush)?,
                ));
            Ok(Box::new(MkvStream::create(writer, title, Some(path))?))
        }
        StreamUrl::Mp4 { ref path } => {
            validate_file_path(path, "mp4")?;
            // Bounded-cache writeback (like mkv://) avoids dirty-page bursts
            // on slow/network staging; the mdat backpatch is an ordinary
            // seek WritebackFile handles. BufWriter coalesces moov writes.
            let writer = std::io::BufWriter::with_capacity(
                IO_BUF_SIZE,
                writeback_file(path, title.size_bytes, flush)?,
            );
            Ok(Box::new(super::mp4::Mp4Sink::create(writer, title)?))
        }
        StreamUrl::Mpg { ref path } => {
            validate_file_path(path, "mpg")?;
            // Streaming pack writer, no seeks: the bounded-cache writeback as for mkv://.
            let writer = std::io::BufWriter::with_capacity(
                IO_BUF_SIZE,
                writeback_file(path, title.size_bytes, flush)?,
            );
            Ok(Box::new(super::mpg::MpgSink::create(writer, title)?))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let writer = std::io::BufWriter::with_capacity(
                IO_BUF_SIZE,
                writeback_file(path, title.size_bytes, flush)?,
            );
            Ok(Box::new(M2tsStream::create(writer, title)?))
        }
        StreamUrl::Network { ref addr } => {
            // `NetworkStream::connect` refuses only unspecified/multicast/
            // broadcast targets; LAN and loopback hosts are allowed.
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::connect(addr)?.meta(title)))
        }
        StreamUrl::Stdio => Ok(Box::new(StdioStream::output(title))),
        StreamUrl::Null => Ok(Box::new(NullStream::new(title))),
        StreamUrl::Disc { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        // `iso://` and `dir://` are block outputs, not PES sinks: a whole-disc copy writes
        // them (`crate::io::open_block_sink`, `Disc::extract_tree`).
        StreamUrl::Iso { .. } | StreamUrl::Dir { .. } => {
            Err(crate::error::Error::StreamReadOnly.into())
        }
        // `demux://` with default options. The CLI constructs `DemuxSink`
        // directly (with parsed flags) before reaching here; this arm
        // covers the bare `output()` call with the default option set.
        StreamUrl::Demux { ref dir } => {
            validate_file_path(dir, "demux")?;
            // Full `--demux/--naming/--delay/--container/--chapters` flags
            // are parsed in the CLI, which builds `DemuxSink` directly. This
            // bare arm uses defaults but seeds `base` from the playlist name.
            demux_sink_output(dir, title, super::demux_sink::DemuxOptions::default())
        }
        // `video://`, `audio://`, `sub://` are `demux://` restricted to one
        // track class. No chapters sidecar (that's a `demux://`/`chapters://`
        // concern).
        StreamUrl::Video { ref dir } => {
            validate_file_path(dir, "video")?;
            track_sink_output(dir, title, super::demux_sink::TrackKind::Video)
        }
        StreamUrl::Audio { ref dir } => {
            validate_file_path(dir, "audio")?;
            track_sink_output(dir, title, super::demux_sink::TrackKind::Audio)
        }
        StreamUrl::Sub { ref dir } => {
            validate_file_path(dir, "sub")?;
            track_sink_output(dir, title, super::demux_sink::TrackKind::Subtitle)
        }
        // `fvi://` writes the per-picture video index. The header's `source` describes the
        // INPUT, from caller-supplied `source` — never `path`, which previously broke
        // reproducibility.
        StreamUrl::Fvi { ref path } => {
            validate_file_path(path, "fvi")?;
            Ok(Box::new(super::fvi_sink::FviSink::create(
                path,
                title,
                source.cloned().unwrap_or_default(),
            )?))
        }
        // `chapters://`/`json://` write title metadata at construction and
        // ignore the PES stream (see `meta_sink`).
        StreamUrl::Chapters { ref path } => {
            validate_file_path(path, "chapters")?;
            Ok(Box::new(super::meta_sink::ChaptersSink::create(
                path, title,
            )?))
        }
        StreamUrl::Json { ref path } => {
            validate_file_path(path, "json")?;
            Ok(Box::new(super::meta_sink::JsonSink::create(path, title)?))
        }
        StreamUrl::Unknown { ref raw } => {
            Err(crate::error::Error::StreamUrlInvalid { url: raw.clone() }.into())
        }
    }
}

// Demuxer-side state derived from a `DiscTitle`: codec parser table (by PID),
// PID-to-track index map, and initial `TsDemuxer`/`PsDemuxer` (per format).
pub(crate) type DemuxState = (
    Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    Vec<(u16, usize)>,
    Option<super::ts::TsDemuxer>,
    Option<super::ps::PsDemuxer>,
);

/// Build the title's codec parser table + initial `TsDemuxer` /
/// `PsDemuxer`. Used by both the ISO and M2TS pipeline builders.
pub(crate) fn build_demux_state(title: &DiscTitle, format: ContentFormat) -> DemuxState {
    let mut pids = Vec::new();
    let mut parsers = Vec::new();
    let mut pid_to_track = Vec::new();
    for (idx, s) in title.streams.iter().enumerate() {
        let (pid, codec) = match s {
            crate::disc::Stream::Video(v) => (v.pid, v.codec),
            crate::disc::Stream::Audio(a) => (a.pid, a.codec),
            crate::disc::Stream::Subtitle(s) => (s.pid, s.codec),
        };
        pids.push(pid);
        pid_to_track.push((pid, idx));
        let is_ps = format.is_program_stream();
        // The Blu-ray 3D MVC dependent (right-eye) view uses a param-set-
        // passthrough H.264 parser so each frame is a self-contained
        // dependent access unit for a BlockAdditional.
        let parser = match s {
            crate::disc::Stream::Video(v) if v.is_mvc_dependent() => {
                super::codec::parser_for_mvc_dependent(codec, is_ps)
            }
            crate::disc::Stream::Audio(a) if a.is_mp2_extension() => {
                super::codec::parser_for_mp2_extension()
            }
            _ => super::codec::parser_for_codec(codec, None, is_ps),
        };
        parsers.push((pid, parser));
    }
    let (ts, ps) = match format {
        ContentFormat::MpegPs | ContentFormat::DvdPs => (None, Some(super::ps::PsDemuxer::new())),
        ContentFormat::BdTs => {
            if pids.is_empty() {
                (None, None)
            } else {
                (Some(super::ts::TsDemuxer::new(&pids)), None)
            }
        }
    };
    (parsers, pid_to_track, ts, ps)
}

/// Assemble the ISO mux pipeline (read+decrypt → demux → parse) for a title with no
/// AACS key set (clear, CSS, or `raw`); an AACS title goes through
/// [`build_iso_pipeline_keyed`]. `keys` is `DecryptKeys::None` for raw/unencrypted
/// reads; `raw` skips the per-title CSS crack. Every stage runs under `ctx`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_iso_pipeline<S: SectorSource + Send + 'static>(
    reader: S,
    title: DiscTitle,
    keys: crate::decrypt::DecryptKeys,
    batch_sectors: u16,
    format: ContentFormat,
    raw: bool,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    let policy = crate::sector::read_stage::ReadPolicy::Image {
        batch: batch_sectors,
    };
    build_sector_pipeline(reader, title, keys, policy, format, raw, ctx)
}

/// A sector source's title mux with no AACS key set, under the Read stage's `policy`
/// (an image's fixed batches, or a live drive's adaptive, recovering reads): the title's
/// CSS key cracked (unless `raw`), then decrypt → prefetch → demux → parse.
pub(crate) fn build_sector_pipeline<S: SectorSource + Send + 'static>(
    mut reader: S,
    title: DiscTitle,
    mut keys: crate::decrypt::DecryptKeys,
    policy: crate::sector::read_stage::ReadPolicy,
    format: ContentFormat,
    raw: bool,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    let batch_sectors = policy.batch();
    let extents = title.extents.clone();
    // CSS (DVD) key resolution — shared per-title step. A `None`/MPEG-PS
    // title cracks its own key; `raw` skips it. A Stop interrupts the crack scan.
    crate::css::resolve_dvd_title_key(
        &mut reader,
        &extents,
        &mut keys,
        batch_sectors,
        format,
        raw,
        Some(&ctx.halt),
    )?;
    // Unit alignment is an AACS concept (whole 3-sector units); forcing it
    // on CSS/unencrypted (per-2048-byte-sector) would reject any extent
    // whose sector count isn't a multiple of 3.
    let unit_align: u16 = match &keys {
        crate::decrypt::DecryptKeys::Aacs { .. } => AACS_UNIT_SECTORS,
        _ => 1,
    };
    let full_extents = extents.clone();
    let decrypting =
        crate::sector::DecryptingSectorSource::new(Box::new(reader) as Box<dyn SectorSource>, keys);
    let plan = IsoPlan {
        extents,
        full_extents,
        policy,
        unit_align,
        format,
    };
    iso_pipeline_tail(decrypting, plan, title, ctx)
}

// The read plan of an ISO title mux: the extents read (FMTS alternate phase dropped), the
// title's full extents, and the read geometry.
struct IsoPlan {
    extents: Vec<crate::disc::Extent>,
    full_extents: Vec<crate::disc::Extent>,
    policy: crate::sector::read_stage::ReadPolicy,
    unit_align: u16,
    format: ContentFormat,
}

/// The ISO title mux over a [`KeyRing`](crate::keys::KeyRing) (KU §3.1): the
/// set's decrypting reader (its map and on-arrival proof) under the prefetcher, with no
/// resolution here. `title` is an already-scanned title; E7013 if the set does not cover
/// its extents or the image is not a sector-exact copy of the set's disc.
pub(crate) fn build_iso_pipeline_keyed<S: SectorSource + Send + 'static>(
    reader: S,
    title: DiscTitle,
    set: &crate::keys::KeyRing,
    batch_sectors: u16,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    let cap = reader.capacity_sectors();
    if (cap != 0 && set.capacity() != 0 && cap != set.capacity())
        || !set.covers_extents(&title.extents)
    {
        tracing::error!(target: "freemkv::keys", "key set does not cover this image title");
        return Err(crate::error::Error::DecryptFailed.into());
    }
    let policy = crate::sector::read_stage::ReadPolicy::Image {
        batch: batch_sectors,
    };
    build_keyed_pipeline(reader, title, set, policy, ctx)
}

/// A sector source's title mux over a key set (its map and on-arrival proof, KU §3.1),
/// under the Read stage's `policy`. The caller checked the set is for this source.
pub(crate) fn build_keyed_pipeline<S: SectorSource + Send + 'static>(
    reader: S,
    title: DiscTitle,
    set: &crate::keys::KeyRing,
    policy: crate::sector::read_stage::ReadPolicy,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    let full_extents = title.extents.clone();
    let ranges: Vec<(u32, u32)> = full_extents
        .iter()
        .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        .collect();
    let decrypting = set
        .decrypting(
            Box::new(reader) as Box<dyn SectorSource>,
            Some(&ranges),
            set.title_stop(),
            false,
        )
        .map_err(io::Error::from)?;
    let plan = IsoPlan {
        extents: set
            .key_map()
            .read_plan(&full_extents, u32::from(AACS_UNIT_SECTORS)),
        full_extents,
        policy,
        unit_align: AACS_UNIT_SECTORS,
        format: set.content_format(),
    };
    iso_pipeline_tail(decrypting, plan, title, ctx)
}

// The shared tail of the ISO title mux: decrypting reader → prefetcher → demux → parse.
fn iso_pipeline_tail(
    mut decrypting: crate::sector::DecryptingSectorSource<Box<dyn SectorSource>>,
    plan: IsoPlan,
    title: DiscTitle,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    let IsoPlan {
        extents,
        full_extents,
        policy,
        unit_align,
        format,
    } = plan;
    // The plan and clip feed spans must describe the SAME bytes: if the plan
    // drops alternate-phase units, frames drift out of place near a join —
    // undetectable by the tile check. Fall back to timestamps if mismatched.
    let feed_matches_spans = extents == full_extents;
    // The mux does not tally decrypt-quality misses (`lost_bytes()` is read-loss
    // only); a missing key is an up-front resolve failure. The DVD AC-3 probe below is
    // diagnostics only (routing is the scanner's PGC AST_CTL map).
    let mut title = title;
    if !feed_matches_spans {
        tracing::info!(
            target: "freemkv::mux",
            planned = extents.len(),
            full = full_extents.len(),
            "read plan omits units the clip spans include; placing by timestamps"
        );
        for c in &mut title.clips {
            c.feed_span = None;
        }
    }
    crate::disc::dvd_audio_probe::probe_and_remap(&mut decrypting, &mut title);
    decrypting.clear_unit_base();
    decrypting.observe(ctx);
    let blanked = decrypting.blanked_counter();

    let (prefetched, read_loss) = crate::sector::PrefetchedSectorSource::with_policy(
        decrypting, extents, policy, unit_align, ctx,
    )
    .map_err(|e| -> io::Error { e.into() })?;
    let (rx, recycle_tx, shell) = prefetched.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, format);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, ctx, ts, ps)
            .map_err(|e| -> io::Error { e.into() })?;
    Ok(
        PipelinedPesStream::new(demux_thread, demux_rx, title, parsers, pid_to_track)
            .with_ctx(ctx)
            .with_blanked(blanked)
            .with_read_loss(read_loss),
    )
}

// The decryption stage's options for a content-detected input.
fn stage_options(
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
    raw: bool,
) -> crate::sector::decrypting::StageOptions {
    crate::sector::decrypting::StageOptions {
        raw,
        keys: opts.keys.clone(),
        ctx: ctx.clone(),
    }
}

// The first `sectors` of `src`, read in `batch`-sector reads.
fn read_head(src: &mut dyn SectorSource, sectors: u32, batch: u16) -> io::Result<Vec<u8>> {
    let mut head = vec![0u8; sectors as usize * 2048];
    let mut at = 0u32;
    while at < sectors {
        let count = (sectors - at).min(u32::from(batch)) as u16;
        let off = at as usize * 2048;
        src.read_sectors(
            at,
            count,
            &mut head[off..off + count as usize * 2048],
            false,
        )
        .map_err(|e| -> io::Error { e.into() })?;
        at += u32::from(count);
    }
    Ok(head)
}

// An `mpg://` source (design §4 step 2, J13): the file → the decryption stage → the PS
// pipeline. The stage cracks once (D3) on its first read; the head scan reads through it.
fn build_ps_pipeline(
    path: &Path,
    opts: &InputOptions,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    const PS_MUX_BATCH_SECTORS: u16 = 8192;
    const HEAD_SECTORS: u32 = 2048; // 4 MiB
    let open = |raw: bool| -> io::Result<_> {
        let file = crate::io::file_sector_source::FileSectorSource::open_padded(path)?;
        Ok(crate::sector::DecryptingSectorSource::new(
            Box::new(file) as Box<dyn SectorSource>,
            crate::sector::Keying::detect(stage_options(opts, ctx, raw)),
        ))
    };
    let mut stage = open(opts.raw)?;
    let capacity = stage.capacity_sectors();
    if capacity == 0 {
        return Err(crate::error::Error::NoStreams.into());
    }
    let extent = crate::disc::Extent {
        start_lba: 0,
        sector_count: capacity,
    };
    let n = capacity.min(HEAD_SECTORS);
    // `--raw` still scans a descrambled head where a crack reaches one; the mux stays raw.
    let head = if opts.raw {
        match read_head(&mut open(false)?, n, PS_MUX_BATCH_SECTORS) {
            Err(e)
                if matches!(
                    crate::error::error_code(&e),
                    Some(crate::error::E_CSS_KEY_MISSING | crate::error::E_NO_DISC_KEY)
                ) =>
            {
                read_head(&mut stage, n, PS_MUX_BATCH_SECTORS)?
            }
            head => head?,
        }
    } else {
        read_head(&mut stage, n, PS_MUX_BATCH_SECTORS)?
    };
    let scan = super::mpg::scan::scan(&head)
        .ok_or_else(|| -> io::Error { crate::error::Error::NoStreams.into() })?;
    let streams = scan.streams;
    let mut title = DiscTitle {
        playlist: path
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default(),
        size_bytes: std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        codec_privates: vec![None; streams.len()],
        streams,
        extents: vec![extent],
        content_format: ContentFormat::MpegPs,
        ..DiscTitle::empty()
    };
    opts.selection
        .apply(&mut title)
        .map_err(|e| -> io::Error { e.into() })?;
    let plan = IsoPlan {
        extents: vec![extent],
        full_extents: vec![extent],
        policy: crate::sector::read_stage::ReadPolicy::Image {
            batch: PS_MUX_BATCH_SECTORS,
        },
        unit_align: 1,
        format: ContentFormat::MpegPs,
    };
    iso_pipeline_tail(stage, plan, title, ctx).map(|p| p.with_video_stream_id(scan.video_id))
}

// Sectors in one AACS aligned unit: the `unit_align` of every keyed read plan.
const AACS_UNIT_SECTORS: u16 = crate::aacs::content::ALIGNED_UNIT_SECTORS as u16;

// Sectors per m2ts read: whole AACS units, ~1 MiB (the stage's byte-view refill).
const M2TS_READ_SECTORS: u16 = 510;

// Assemble the M2TS file mux pipeline (read -> demux -> parse). Scans the head for an FMKV
// header or PMT/PAT through the stage's byte view, then hands the stage to the one Read
// stage and prefetcher, whose chunks follow the head's unconsumed bytes into the demux.
fn build_m2ts_pipeline(
    mut reader: crate::sector::stage::SectorBytes<
        crate::sector::DecryptingSectorSource<Box<dyn SectorSource>>,
    >,
    len: u64,
    ctx: &crate::ctx::Ctx,
) -> io::Result<PipelinedPesStream> {
    use super::meta;
    use std::io::Read;

    const M2TS_SCAN_BYTES: usize = 1024 * 1024;
    let mut head = vec![0u8; M2TS_SCAN_BYTES];
    let head_len = {
        let mut filled = 0;
        while filled < head.len() {
            match reader.read(&mut head[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        filled
    };
    head.truncate(head_len);

    // Try FMKV metadata header first; fall back to PMT scan. Only a genuine
    // absence of the FMKV magic (`Ok(None)`) falls through; a corrupt header
    // propagates rather than being misreported as PMT-derived.
    let mut cursor = io::Cursor::new(&head);
    let (title, head_consumed) = match meta::read_header(&mut cursor)? {
        Some(m) => {
            let t = m.to_title();
            // Guard the FMKV branch the same way the ISO and PMT paths
            // do: a header carrying zero streams yields an empty title
            // that would mux nothing — surface NoStreams instead.
            if t.streams.is_empty() {
                return Err(crate::error::Error::NoStreams.into());
            }
            (t, cursor.position() as usize)
        }
        None => {
            let streams = super::ts::scan_streams(&head)
                .ok_or_else(|| -> io::Error { crate::error::Error::NoStreams.into() })?;
            let t = DiscTitle {
                duration_secs: 0.0,
                streams,
                ..DiscTitle::empty()
            };
            (t, 0)
        }
    };

    // The demuxer sees one contiguous M2TS byte stream: the head's unconsumed bytes and what
    // the byte view already holds, then the Read stage's chunks from the next sector on.
    let (stage, rest, next) = reader.into_rest();
    let mut prefix = head[head_consumed..].to_vec();
    prefix.extend_from_slice(&rest);
    let cap = stage.capacity_sectors();
    let extents = match cap > next {
        true => vec![crate::disc::Extent {
            start_lba: next,
            sector_count: cap - next,
        }],
        false => Vec::new(),
    };
    let view = crate::sector::prefetched::ByteView {
        prefix,
        len: len.saturating_sub(u64::from(next) * crate::consts::SECTOR_BYTES_U64),
    };
    let policy = crate::sector::read_stage::ReadPolicy::Image {
        batch: M2TS_READ_SECTORS,
    };
    let (prefetched, read_loss) =
        crate::sector::PrefetchedSectorSource::file_bytes(stage, extents, policy, ctx, view)
            .map_err(|e| -> io::Error { e.into() })?;
    let (rx, recycle_tx, shell) = prefetched.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, ContentFormat::BdTs);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, ctx, ts, ps)
            .map_err(|e| -> io::Error { e.into() })?;
    Ok(
        PipelinedPesStream::new(demux_thread, demux_rx, title, parsers, pid_to_track)
            .with_ctx(ctx)
            .with_read_loss(read_loss),
    )
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "resolve_stop_tests.rs"]
mod stop_tests;

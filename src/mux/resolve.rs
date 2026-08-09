//! Stream URL resolver — parses URL strings into PES stream instances.
//!
//! Format: `scheme://path`
//!
//! | Scheme | Input | Output | Path |
//! |--------|-------|--------|------|
//! | disc:// | Yes | -- | empty (auto-detect) or /dev/sgN |
//! | disk:// | Yes | -- | alias for `disc://` (identical behavior) |
//! | iso://  | Yes | -- | file path (required) |
//! | mkv://  | Yes | Yes | file path (required) |
//! | m2ts:// | Yes | Yes | file path (required) |
//! | network:// | Yes (listen) | Yes (connect) | host:port (required) |
//! | stdio:// | Yes (stdin) | Yes (stdout) | empty |
//! | null:// | -- | Yes | empty |
//! | demux:// | -- | Yes | directory path (required) — per-track ES demux |
//! | fvi://  | -- | Yes | file path (required) — per-picture video index |
//!
//! Bare paths without a scheme are rejected.
//! For disc→ISO (raw sector copy), use `freemkv_engine::recovery::copy` instead.
//!
//! Note: `disc://` cannot be opened through [`input`]; it returns
//! [`crate::error::Error::DiscUrlNotDirect`]. Live-disc input must go
//! through `Drive::open()` + `Disc::scan()` + `DiscStream::new()`, not
//! the URL resolver.

use super::network::NetworkStream;
use super::null::NullStream;
use super::pipelined_stream::PipelinedPesStream;
use super::stdio::StdioStream;
use super::{M2tsStream, MkvStream};
use crate::disc::{ContentFormat, DiscTitle};
use crate::sector::SectorSource;
use std::io;
use std::path::{Path, PathBuf};

/// I/O buffer size for file streams.
const IO_BUF_SIZE: usize = 4 * 1024 * 1024;

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
    /// freemkv native per-picture video index (`fvi://`). A write-only PES sink
    /// that emits one JSON-Lines record per coded picture of the title's primary
    /// video track to a `.fvi` file (normative spec `docs/FVI_FORMAT.md`).
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
    /// `dir://` joined in 1.6.1, when `crate::dirimage` gave a folder a real
    /// synthesized UDF volume.
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
    // A bare IPv6 literal ("::1", "2001:db8::1") contains ':' yet has no port,
    // so the simple `contains(':')` check would wrongly pass it and TcpListener
    // would later return an untyped io::Error. Treat anything that parses as a
    // bare IpAddr (v4 or v6) as port-less.
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
    // Split host:port on the LAST ':' so a bracketed IPv6 literal
    // (`[2001:db8::1]:9000`) splits at the port colon, not an address colon.
    // Require the port substring to be a non-empty u16 — `host:` (empty) and
    // `host:abc` (non-numeric) are invalid, despite containing ':'.
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

/// Options for opening an input stream.
#[derive(Clone, Default)]
pub struct InputOptions {
    /// Caller-resolved per-CPS-unit AACS keys to apply to the scanned disc
    /// (`(cps_unit, 16-byte key)`). Empty for an unencrypted disc or when the
    /// caller has no key. The library does no lookup — a key source resolves
    /// these and the caller passes them here.
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// 0-based title index to open; `None` selects title 0. An
    /// out-of-range index yields [`crate::error::Error::DiscTitleRange`].
    pub title_index: Option<usize>,
    /// Skip decryption — return raw encrypted bytes.
    pub raw: bool,
    /// Optional fresh-key-on-failure closure (a shared [`crate::sector::KeyFetch`]).
    /// `None` (default) keeps the prior behaviour: a unit no held key decrypts is
    /// counted as decrypt loss. When set, the mux installs it (cloned `Arc`) so a
    /// still-scrambled unit is re-tried via the application's key source.
    /// Application seam only; the library makes no network call.
    pub key_fetch: Option<crate::sector::KeyFetch>,
    /// Which audio/subtitle streams to keep. `input()` scans the source and
    /// picks the title internally, so the caller can't prune the `DiscTitle`
    /// itself — it passes the selection here and `input()` applies it right
    /// after the title-index bounds check. Default keeps every stream (video is
    /// always kept). See [`crate::StreamSelection`].
    pub selection: crate::StreamSelection,
}

// `KeyFetchFactory` holds a trait object that is not `Debug`; hand-roll the
// impl (the prior derive is preserved for every other field) so `InputOptions`
// stays printable without dumping key material.
impl std::fmt::Debug for InputOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputOptions")
            .field("unit_keys", &self.unit_keys.len())
            .field("title_index", &self.title_index)
            .field("raw", &self.raw)
            .field("key_fetch", &self.key_fetch.is_some())
            .field("selection", &self.selection)
            .finish()
    }
}

/// Open a PES input stream (produces PES frames).
pub fn input(url: &str, opts: &InputOptions) -> io::Result<Box<dyn crate::pes::Stream>> {
    let parsed = parse_url(url);
    match parsed {
        StreamUrl::Disc { .. } => {
            // Disc sources require live SCSI state — caller must use
            // `Drive::open() + Disc::scan() + DiscStream::new()` directly.
            // Surfaced as a typed error (no English commentary in the
            // library; the CLI/UI explains the right entry point).
            Err(crate::error::Error::DiscUrlNotDirect.into())
        }
        StreamUrl::Iso { ref path } => {
            validate_file_path(path, "iso")?;
            // FileSectorSource is the sole file-backed sector source.
            // It carries the platform-tuned SEQUENTIAL fadvise hint
            // (so the kernel readahead window widens) and the periodic
            // DONTNEED page-cache eviction that bounds memory pressure
            // when the mux output is being written to the same disk.
            let reader = crate::io::file_sector_source::FileSectorSource::open(path)?;
            let probe_path = path.clone();
            let stream = image_input(
                reader,
                opts,
                move || crate::io::file_sector_source::FileSectorSource::open(&probe_path).ok(),
                false,
            )?;
            Ok(Box::new(stream))
        }
        // `dir://` as a SOURCE: an extracted disc folder, presented as a
        // synthetic UDF image (`crate::dirimage`). It reaches EXACTLY the same
        // body as `iso://` above — scan, title select, key resolution, mux —
        // because by the time `DirImage` exists it is just another
        // `SectorSource`. That is the point of synthesizing a real filesystem
        // rather than faking a tree: every destination follows for free, with
        // no per-scheme mux path to keep in step.
        StreamUrl::Dir { ref path } => {
            validate_file_path(path, "dir")?;
            let reader = crate::dirimage::DirImage::open(path)?;
            let probe_path = path.clone();
            let stream = image_input(
                reader,
                opts,
                move || crate::dirimage::DirImage::open(&probe_path).ok(),
                true,
            )?;
            Ok(Box::new(stream))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::with_capacity(IO_BUF_SIZE, file);
            let stream = build_m2ts_pipeline(reader)?;
            Ok(Box::new(stream))
        }
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(MkvStream::open(reader)?))
        }
        StreamUrl::Network { ref addr } => {
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::listen(addr)?))
        }
        StreamUrl::Stdio => Ok(Box::new(StdioStream::input())),
        StreamUrl::Null => Err(crate::error::Error::StreamWriteOnly.into()),
        // `mp4://` as a source: demux a progressive MP4 back into PES frames, so
        // `mp4://` flows to every sink (mkv://, audio://, json://, …).
        StreamUrl::Mp4 { ref path } => Ok(Box::new(super::mp4::Mp4Reader::open(path)?)),
        // `demux://` is an output-only sink (per-track ES files); never a source.
        StreamUrl::Demux { .. }
        | StreamUrl::Video { .. }
        | StreamUrl::Audio { .. }
        | StreamUrl::Sub { .. }
        | StreamUrl::Chapters { .. }
        | StreamUrl::Json { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
        // `fvi://` is an output-only sink (per-picture video index); never a source.
        StreamUrl::Fvi { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
        StreamUrl::Unknown { ref raw } => {
            Err(crate::error::Error::StreamUrlInvalid { url: raw.clone() }.into())
        }
    }
}

/// Shared body of every IMAGE-level PES source.
///
/// `iso://` and `dir://` differ only in how the sectors are produced — a file
/// versus a synthesized UDF volume over a folder — and not at all in what is
/// done with them: scan, apply caller-resolved keys, gate on decryptability,
/// select the title, prune streams, correct TrueHD channel counts, mux. One
/// body means a `dir://` source cannot drift away from the `iso://` behaviour
/// that has years of fixes in it.
///
/// `reopen` yields a SECOND, independent reader for the TrueHD channel probe,
/// which must not disturb the mux reader's position. It returns `Option`
/// because a failed re-open is non-fatal: the correction is skipped, not the
/// mux.
fn image_input<S, F>(
    mut reader: S,
    opts: &InputOptions,
    reopen: F,
    // A FOLDER's `encrypted` flag comes from tree shape and can be wrong; an
    // image's cannot. See `session::apply_folder_encryption_verdict`.
    is_folder: bool,
) -> io::Result<PipelinedPesStream>
where
    S: SectorSource + Send + 'static,
    F: FnOnce() -> Option<S>,
{
    let capacity = reader.capacity_sectors();
    let mut disc =
        crate::disc::Disc::scan_image(&mut reader, capacity, &crate::disc::ScanOptions::default())
            .map_err(|e| -> io::Error { e.into() })?;
    // Without this a folder reached here with a tree-shape verdict while
    // `session::scan_dir` reached the opposite one from its CONTENT, so the
    // same folder ripped through one door and failed through the other.
    if is_folder {
        crate::session::apply_folder_encryption_verdict(&mut reader, &mut disc)
            .map_err(|e| -> io::Error { e.into() })?;
    }
    // Apply the caller-resolved keys (lookup-free); decrypt_keys() then
    // yields them for the stream below. Propagate a failed application
    // rather than silently muxing an undecryptable stream.
    if !opts.unit_keys.is_empty() {
        // These UKs were already resolved AND validated by the caller
        // (the CLI's keydb loop), so no re-validation sample is needed.
        disc.decrypt_with(crate::disc::Key::Unit(opts.unit_keys.clone()), &[])
            .map_err(|e| -> io::Error { e.into() })?;
    }
    // Pre-flight decrypt gate (the single, system-wide verdict — see
    // `Disc::ensure_decryptable`). Fails fast BEFORE any mux work when
    // decryption is needed and unavailable: a scrambled-but-uncracked
    // CSS disc (`css_error` set), or an AACS-encrypted disc with no
    // usable key (would mux ~100 MB of garbage — encrypted m2ts → no TS
    // syncs → demuxer emits nothing → empty/garbage output at exit 0).
    // `--raw` and unencrypted/CSS-keyless-success discs pass. This is the
    // disc-wide check; the per-title (multi-VTS CSS) check is below, once
    // the chosen title's key is resolved.
    disc.ensure_decryptable(opts.raw)
        .map_err(|e| -> io::Error { e.into() })?;
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
    // Prune to the selected audio/subtitle streams now, on the scanned
    // (pre-`probe_and_remap`) title, so everything downstream — the
    // TrueHD channel-correction probe, the final title clone, and
    // `build_iso_pipeline`'s demux/track construction — sees the pruned
    // list. Video is always kept; a no-op for the default All/All.
    opts.selection
        .apply(&mut disc.titles[idx])
        .map_err(|e| -> io::Error { e.into() })?;
    // Per-title key resolution. DVD CSS is resolved at exactly ONE site —
    // `build_iso_pipeline`'s per-title crack (below), which decrypts a
    // crackable title, passes a genuinely-clear one through, and
    // hard-fails an uncrackable one with CssKeyMissing. So for a DVD we do
    // NOT pre-crack here: pass `None` and let the pipeline own it.
    // Pre-cracking would re-open the ISO and re-scan every clear title
    // (`decrypt_keys_for_title` → None → the pipeline re-cracks anyway).
    // AACS / unencrypted resolve from `decrypt_keys()` with NO read; `--raw`
    // (any format) is deliberate ciphertext passthrough — also `None`.
    let is_dvd = disc.format == crate::disc::DiscFormat::Dvd;
    let (keys, title_is_clear) = if opts.raw || is_dvd {
        (crate::decrypt::DecryptKeys::None, false)
    } else {
        (disc.decrypt_keys(), false)
    };
    // Decrypt gate for the AACS / non-DVD path: a None key means no usable
    // disc key, which would mux scrambled ciphertext verbatim — fail loudly
    // (NoDiscKey). The DVD path is gated inside `build_iso_pipeline` (its
    // CSS hard-fail), and `--raw` passes.
    if !is_dvd {
        disc.ensure_title_decryptable(opts.raw, &keys, title_is_clear)
            .map_err(|e| -> io::Error { e.into() })?;
    }
    // FMTS (AACS 2.1) forensic segments are sourced + fail-loud-checked
    // downstream by `resolve_mux_key_map`/`resolve_fmts_key_map`, which hold
    // the key-fetch closure and can actually attempt resolution. (An older
    // upfront blanket-reject gate lived here; it predated the resolver and
    // rejected every 2.1 disc before a source could be tried.)
    // Correct TrueHD channel counts (MPLS understates 7.1/Atmos as 5.1)
    // by probing the first DECRYPTED access units of the chosen title.
    // A fresh reader avoids disturbing the mux reader below. Skipped in
    // --raw mode: the probe would re-open + decrypt for nothing (on an
    // AACS disc with no key the correction is a no-op on ciphertext, and
    // raw output isn't decoded anyway).
    if !opts.raw {
        match reopen() {
            Some(mut probe) => {
                // The probe DECRYPTS the title head, so it needs the SAME
                // up-front key map the mux read installs below. An AACS
                // `DecryptingSectorSource` with no map fails loud on the
                // first unit (`decrypt_sectors_mapped` is the only AACS
                // decrypt path) — without resolving one here the correction
                // is silently skipped on every AACS disc and 7.1/Atmos stays
                // understated as the MPLS-declared 5.1. Resolution failure is
                // non-fatal (`.ok()`): leave channels uncorrected, never
                // fail the mux.
                let mut probe_keys = keys.clone();
                let probe_title = disc.titles[idx].clone();
                let probe_map = match &probe_keys {
                    crate::decrypt::DecryptKeys::Aacs { .. } => resolve_mux_key_map(
                        &mut probe,
                        &probe_title,
                        &mut probe_keys,
                        opts.key_fetch.as_ref(),
                        disc.content_format,
                        // File-backed, bounded probe (best-effort `.ok()`);
                        // no live drive to protect from a stuck stop here.
                        None,
                    )
                    .ok()
                    .map(std::sync::Arc::new),
                    _ => None,
                };
                let mut dec = crate::sector::DecryptingSectorSource::new(probe, probe_keys);
                if let Some(map) = probe_map {
                    dec = dec.with_key_map(map);
                }
                crate::disc::correct_truehd_channels(&mut dec, &mut disc.titles[idx]);
            }
            None => {
                // Non-fatal: a failed re-open just leaves MPLS 7.1/Atmos
                // channel counts uncorrected (understated as 5.1). Log so
                // the uncorrected path is diagnosable rather than silent.
                tracing::debug!(
                    target: "mux",
                    "TrueHD channel-correction probe re-open failed"
                );
            }
        }
    }
    let title = disc.titles[idx].clone();
    let format = disc.content_format;
    // ISO file: 8192-sector batch (16 MiB at 2048 B/sector) —
    // sequential read from fast storage, no bad sectors. Empirically
    // optimal; bumping to 16384 sectors (32 MiB) regressed (more cache
    // pressure, longer per-batch latency starves the consumer between
    // iterations). Physical drives keep smaller batches for adaptive
    // error handling.
    const ISO_MUX_BATCH_SECTORS: u16 = 8192;

    // Pass `DecryptKeys::None` to the decrypt decorator when
    // --raw is set — the read stack still flows through the
    // same producer+demux+parse pipeline, just without the
    // AACS / CSS step. Single highway for all ISO reads.
    let effective_keys = if opts.raw {
        crate::decrypt::DecryptKeys::None
    } else {
        keys
    };
    // Install the shared fetch closure (if the app supplied one) so a
    // unit no held key decrypts is re-tried via the app's key source.
    // Suppressed in --raw (no decrypt step to recover).
    let fetch = if opts.raw {
        None
    } else {
        opts.key_fetch.clone()
    };
    let stream = build_iso_pipeline(
        reader,
        title,
        effective_keys,
        ISO_MUX_BATCH_SECTORS,
        format,
        opts.raw,
        None,
        None,
        fetch,
    )?;
    Ok(stream)
}

/// Open a PES output stream (consumes PES frames).
///
/// `source` is the provenance of the material being written — the INPUT the
/// caller is muxing from, not `url`. Only the `fvi://` sink consumes it (it
/// records the input in the index header, `docs/FVI_FORMAT.md` §6.2); every
/// other sink ignores it. `None` means "no provenance to declare": the header's
/// `source` members then carry their neutral defaults and the optional ones are
/// omitted, rather than being back-filled with the destination path — which is
/// exactly the bug this parameter exists to prevent.
pub fn output(
    url: &str,
    title: &crate::disc::DiscTitle,
    source: Option<&super::videomap::SourceInfo>,
) -> io::Result<Box<dyn crate::pes::Stream>> {
    let parsed = parse_url(url);
    match parsed {
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            // Wrap the output in `crate::io::WritebackFile` (bounded-cache
            // writeback) so a UHD-scale MKV mux to slow / network-attached
            // staging doesn't hit the dirty-page burst pathology that
            // sweep already side-steps. BufWriter sits on top to coalesce
            // mux's many small EBML element writes. Pre-reserve the
            // target's worth of extents on Linux via fallocate(KEEP_SIZE)
            // to reduce extent fragmentation during the mux.
            let writer: Box<dyn super::WriteSeek + Send> =
                Box::new(std::io::BufWriter::with_capacity(
                    IO_BUF_SIZE,
                    crate::io::WritebackFile::create_with_size_hint(path, title.size_bytes)?,
                ));
            Ok(Box::new(MkvStream::create(writer, title, Some(path))?))
        }
        StreamUrl::Mp4 { ref path } => {
            validate_file_path(path, "mp4")?;
            // Bounded-cache writeback (like mkv://) so a UHD-scale mux to slow /
            // network-attached staging doesn't hit the dirty-page burst
            // pathology; the mdat backpatch is an ordinary seek WritebackFile
            // handles. BufWriter coalesces the many small moov box-header writes.
            let writer = std::io::BufWriter::with_capacity(
                IO_BUF_SIZE,
                crate::io::WritebackFile::create_with_size_hint(path, title.size_bytes)?,
            );
            Ok(Box::new(super::mp4::Mp4Sink::create(writer, title)?))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let writer = std::io::BufWriter::with_capacity(
                IO_BUF_SIZE,
                crate::io::WritebackFile::create_with_size_hint(path, title.size_bytes)?,
            );
            Ok(Box::new(M2tsStream::create(writer, title)?))
        }
        StreamUrl::Network { ref addr } => {
            // Format-validate, then connect. `NetworkStream::connect`
            // re-resolves the host and refuses any address that is
            // loopback / private / link-local / multicast — this is the
            // SSRF / DNS-rebinding guard, applied at the actual connect
            // (not just at settings-save time). It is deliberately NOT in
            // `validate_network_addr`, which is shared with the listen
            // (receiver) path where binding loopback is legitimate.
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::connect(addr)?.meta(title)))
        }
        StreamUrl::Stdio => Ok(Box::new(StdioStream::output(title))),
        StreamUrl::Null => Ok(Box::new(NullStream::new(title))),
        StreamUrl::Disc { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        StreamUrl::Iso { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        // `dir://` is NOT a PES sink — it writes raw decrypted files, not muxed
        // frames. A stray `dir://` routed into the mux/PES path fails loudly,
        // exactly the category the crate already rejects for `iso://`. The CLI
        // routes a `dir://` dest to `Disc::extract_tree` before reaching here.
        StreamUrl::Dir { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        // `demux://` with default options. The CLI constructs `DemuxSink`
        // directly (with parsed flags) before reaching here, mirroring how a
        // `dir://` dest is special-cased; this arm covers the bare
        // `output()` call with the default option set.
        StreamUrl::Demux { ref dir } => {
            validate_file_path(dir, "demux")?;
            // The full `--demux/--naming/--delay/--container/--chapters` flag
            // surface is parsed in the CLI, which constructs `DemuxSink` directly.
            // This bare `output()` arm uses defaults but still seeds the filename
            // `base` from the title's playlist name when present (the default
            // "title" stem is only a last resort for an unnamed title).
            let mut opts = super::demux_sink::DemuxOptions::default();
            if !title.playlist.is_empty() {
                opts.base = title.playlist.clone();
            }
            Ok(Box::new(super::demux_sink::DemuxSink::create(
                dir, title, &opts,
            )?))
        }
        // `video://`, `audio://`, and `sub://` are `demux://` restricted to one
        // track class — video as native elementary streams, audio in native
        // containers, or subtitles as `.sup`/`.idx+.sub`/`.srt`. No chapters
        // sidecar (that's a `demux://` / `chapters://` concern).
        StreamUrl::Video { ref dir }
        | StreamUrl::Audio { ref dir }
        | StreamUrl::Sub { ref dir } => {
            let (scheme, kind) = match parsed {
                StreamUrl::Video { .. } => ("video", super::demux_sink::TrackKind::Video),
                StreamUrl::Audio { .. } => ("audio", super::demux_sink::TrackKind::Audio),
                _ => ("sub", super::demux_sink::TrackKind::Subtitle),
            };
            validate_file_path(dir, scheme)?;
            let mut opts = super::demux_sink::DemuxOptions {
                kind_filter: Some(kind),
                export_chapters: false,
                ..Default::default()
            };
            if !title.playlist.is_empty() {
                opts.base = title.playlist.clone();
            }
            Ok(Box::new(super::demux_sink::DemuxSink::create(
                dir, title, &opts,
            )?))
        }
        // `fvi://` writes the per-picture video index (`docs/FVI_FORMAT.md`).
        // The header's `source` object describes the INPUT, so it comes from the
        // caller-supplied `source` — never from `path`, which is the destination
        // this sink writes. Passing the destination here made every index name
        // itself as its own source AND made the output non-reproducible (two
        // machines indexing identical bytes emitted different files purely from
        // where they wrote). `None` → the neutral defaults; the optional members
        // are omitted rather than guessed.
        StreamUrl::Fvi { ref path } => {
            validate_file_path(path, "fvi")?;
            Ok(Box::new(super::fvi_sink::FviSink::create(
                path,
                title,
                source.cloned().unwrap_or_default(),
            )?))
        }
        // `chapters://` and `json://` write the title metadata at construction and
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

/// Demuxer-side state derived from a `DiscTitle`: the codec parser
/// table (keyed by PID), the PID-to-track index map, and an initial
/// `TsDemuxer` / `PsDemuxer` (whichever the content format calls
/// for).
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
        let is_dvd_ps = matches!(format, ContentFormat::MpegPs);
        // The Blu-ray 3D MVC dependent (right-eye) view uses a param-set-
        // passthrough H.264 parser so each frame is a self-contained dependent
        // access unit for a BlockAdditional; every other stream uses the
        // ordinary parser for its codec.
        let parser = match s {
            crate::disc::Stream::Video(v) if v.is_mvc_dependent() => {
                super::codec::parser_for_mvc_dependent(codec, is_dvd_ps)
            }
            _ => super::codec::parser_for_codec(codec, None, is_dvd_ps),
        };
        parsers.push((pid, parser));
    }
    let (ts, ps) = match format {
        ContentFormat::MpegPs => (None, Some(super::ps::PsDemuxer::new())),
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

/// CPS-unit id given to an FMTS forensic **index key** banked into the caller's key
/// pool by [`resolve_fmts_key_map`] (`FMTS_POOL_TAG_BASE + slot`).
///
/// The pool entry's first field is a CPS-unit number that the mapped decrypt never
/// reads — it indexes the pool by SLOT ([`crate::decrypt::AacsKeyMap`] ranges carry
/// slots) — so the field doubles as the tag that separates the disc's BASE CPS unit
/// keys from the forensic index keys appended on top of them. Base ids come from a
/// key's position in `Unit_Key_RO.inf` (+1), whose count is a BE16, so they cannot
/// exceed 65_536; this base is far above that, so the two id spaces can never
/// collide and misclassify a base key as forensic (which would under-count the CPS
/// units and hand the wrong key to a whole extent).
const FMTS_POOL_TAG_BASE: u32 = 1 << 24;

/// The pool slot of the disc's ONE base CPS Unit Key, or `None` when the pool holds
/// several (a genuine multi-CPS disc) — the forensic index keys
/// ([`FMTS_POOL_TAG_BASE`]) that [`resolve_fmts_key_map`] appends to the same pool
/// are excluded, so the answer is a property of the DISC and not of how many titles
/// have already resolved through the shared pool.
fn single_base_key_slot(unit_keys: &[(u32, [u8; 16])]) -> Option<usize> {
    match base_key_slots(unit_keys)[..] {
        [only] => Some(only),
        _ => None,
    }
}

/// The pool slots of the disc's BASE CPS Unit Keys, in pool order — i.e. every
/// entry that is not a forensic index key banked by [`resolve_fmts_key_map`]
/// ([`FMTS_POOL_TAG_BASE`]). One element per CPS unit whose key is held.
fn base_key_slots(unit_keys: &[(u32, [u8; 16])]) -> Vec<usize> {
    unit_keys
        .iter()
        .enumerate()
        .filter(|(_, (cps_id, _))| *cps_id < FMTS_POOL_TAG_BASE)
        .map(|(slot, _)| slot)
        .collect()
}

/// Read a spread of real encrypted aligned units from `[start, start + sectors)`.
/// Only units that are genuinely AACS-encrypted are returned, so a caller can
/// treat a decrypt-to-clean as proof that the key it used is this extent's.
fn sample_encrypted_units(
    reader: &mut dyn SectorSource,
    start: u32,
    sectors: u32,
    format: ContentFormat,
) -> Vec<Vec<u8>> {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    let total_units = sectors / ALIGNED_UNIT_SECTORS;
    let mut out = Vec::new();
    if total_units == 0 {
        return out;
    }
    const PROBES: u32 = 8;
    for p in 1..=PROBES {
        let unit = ((total_units as u64 * p as u64) / (PROBES as u64 + 1)) as u32;
        if unit >= total_units {
            continue;
        }
        let lba = start.saturating_add(unit.saturating_mul(ALIGNED_UNIT_SECTORS));
        let mut buf = vec![0u8; ALIGNED_UNIT_LEN];
        if reader
            .read_sectors(lba, ALIGNED_UNIT_SECTORS as u16, &mut buf, false)
            .is_ok()
            && aacs_unit_encrypted(&buf, format)
        {
            out.push(buf);
        }
    }
    out
}

/// The FIRST pool slot whose key decrypts one of `samples` to clean content.
/// `slots` restricts the search (and its order) to the candidate pool entries —
/// the whole pool for the multi-CPS path, the base CPS unit keys only when the
/// question is "which CPS unit is this extent in".
fn pick_pool_slot(
    samples: &[Vec<u8>],
    pool: &[(u32, [u8; 16])],
    slots: &[usize],
    format: ContentFormat,
) -> Option<usize> {
    use crate::aacs::content::{decrypt_unit, is_clean};
    slots.iter().copied().find(|&slot| {
        let Some((_, k)) = pool.get(slot) else {
            return false;
        };
        samples.iter().any(|s| {
            let mut u = s.clone();
            decrypt_unit(&mut u, k);
            is_clean(&u, format)
        })
    })
}

/// Which CPS unit's BASE Unit Key opens `ext`, as a pool slot — decided by this
/// extent's own ciphertext, exactly like the multi-CPS path in
/// [`resolve_mux_key_map_cached`], and memoised in the same per-disc
/// [`CpsUnitCache`] so a clip several playlists share is sampled once.
///
/// Only base keys are considered: the forensic index keys share the pool but
/// belong to segment ranges, which are already mapped by tag before this runs.
///
/// `last_idx` is the slot resolved for the PRECEDING extent of this title,
/// carried into an extent with no sampleable encrypted units (nothing to
/// mis-decrypt). An extent that does have real ciphertext no held or fetched key
/// opens is a fail-loud [`crate::error::Error::DecryptFailed`] rather than a
/// silently wrong key over the whole extent.
fn base_slot_for_extent(
    reader: &mut dyn SectorSource,
    ext: &crate::disc::Extent,
    keys: &mut crate::decrypt::DecryptKeys,
    fetch: Option<&crate::sector::KeyFetch>,
    format: ContentFormat,
    cps: &mut CpsUnitCache,
    last_idx: usize,
) -> io::Result<usize> {
    let ck = (format, ext.start_lba, ext.sector_count);
    if let Some(&hit) = cps.get(&ck) {
        return Ok(hit);
    }
    let samples = sample_encrypted_units(reader, ext.start_lba, ext.sector_count, format);
    let pool: Vec<(u32, [u8; 16])> = match keys {
        crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => unit_keys.clone(),
        _ => Vec::new(),
    };
    let mut idx = pick_pool_slot(&samples, &pool, &base_key_slots(&pool), format);
    if idx.is_none()
        && let Some(f) = fetch
        && !samples.is_empty()
    {
        let fresh = f.unit_keys(&samples);
        if let crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } = keys {
            for k in fresh {
                if !unit_keys.iter().any(|(_, h)| *h == k) {
                    let i = unit_keys.len() as u32;
                    unit_keys.push((i, k));
                }
            }
            idx = pick_pool_slot(&samples, unit_keys, &base_key_slots(unit_keys), format);
        }
    }
    match idx {
        // A real decision from this extent's own ciphertext — memoise it.
        Some(i) => {
            cps.insert(ck, i);
            Ok(i)
        }
        // Inherited from the PRECEDING extent of THIS title, so it is not a
        // property of this extent: never cache it.
        None if samples.is_empty() => Ok(last_idx),
        None => Err(crate::error::Error::DecryptFailed.into()),
    }
}

/// FMTS (AACS 2.1) branch of [`resolve_mux_key_map`]. Returns `Some(map)` when the
/// disc carries `IndividualSegment.tbl` AND a key source is configured; `None`
/// otherwise (not FMTS, or no source — the caller's base-Unit-Key path then
/// applies, and the forensic units garble and are dropped by the demux).
///
/// The forensic segments each carry an **index** tag (1..32) selecting one of 32
/// **index keys** the base Unit Key cannot open (see [`crate::aacs::segment`]).
/// This resolves those keys up front from the configured source — sending, per
/// index, a batch of same-index units the service maps to that index's key — adds
/// them to the pool, and builds a per-segment LBA→key map. Applying a segment's
/// key over its whole range decodes the ~40 units of that index's interleave half
/// to clean TS and garbles the other ~40 (the alternate half), which the demux
/// then drops, yielding one coherent stream. The base Unit Key covers everything
/// outside a segment.
///
/// The segment SPNs are offsets into the FORENSIC FEATURE CLIP, so every clip-byte →
/// LBA mapping here is anchored on that clip's own extents ([`forensic_clip_extents`])
/// — never on `title.extents`, which on a play-all playlist is the concatenation of
/// several clips and would put the segments in the wrong one.
///
/// Everything expensive here is memoised per DISC, not recomputed per title:
/// `cache.table` holds the UDF walk + `IndividualSegment.tbl` and `cache.clip` the
/// forensic clip's extents (both disc-invariant outright), and `cache.keys` holds the
/// index keys + phases. See [`resolve_mux_key_map_cached`] for why a hit is provably
/// the same answer. What remains per title is the pool→slot mapping, the LBA range
/// arithmetic and the base-key gap fill.
///
/// The gap fill covers each non-forensic LBA with the base Unit Key of the CPS unit
/// it belongs to. On the single-base-key disc that is every FMTS disc seen so far
/// that costs nothing; a disc with several base CPS Unit Keys resolves each extent
/// from its own ciphertext through `cps` ([`CpsUnitCache`], shared with the
/// multi-CPS path so a clip is sampled once per disc).
#[allow(clippy::too_many_arguments)]
fn resolve_fmts_key_map(
    reader: &mut dyn SectorSource,
    title: &DiscTitle,
    keys: &mut crate::decrypt::DecryptKeys,
    fetch: Option<&crate::sector::KeyFetch>,
    format: ContentFormat,
    halt: Option<&crate::halt::Halt>,
    cache: &mut FmtsCache,
    cps: &mut CpsUnitCache,
) -> io::Result<Option<crate::decrypt::AacsKeyMap>> {
    let FmtsCache {
        table,
        clip,
        keys: memo,
    } = cache;
    use crate::aacs::segment::{clip_byte_to_lba, parse_individual_segments};

    // Cooperative cancel: this probes the LIVE drive across up to a few hundred
    // `read_sectors` (the anchor + per-index probe loops), each able to stall to
    // the SCSI recovery timeout. An operator `/api/stop` during forensic key
    // resolution must be honored at each loop boundary rather than blocking until
    // the whole probe completes (hard rule: don't hammer a struggling live drive).
    let check_halt = || -> io::Result<()> {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(crate::error::Error::Halted.into());
        }
        Ok(())
    };
    // Poll once on ENTRY as well as inside the probe loops: with both memos warm a
    // title can reach the finished map without touching a single loop, so a
    // 60-playlist sweep would otherwise run to completion after an operator Stop.
    check_halt()?;

    // Load the segment map. Distinguish a genuine "not an FMTS disc" negative
    // from a transient live-drive I/O fault: swallowing the latter into Ok(None)
    // would fall through to a base-Unit-Key-only map, garble the forensic units,
    // let the demux drop them, and complete the mux with NO error — silently
    // losing forensic content, contradicting this function's fail-loud contract.
    //   - `UdfNotFilesystem`: bytes read fine but are not a UDF disc (deterministic
    //     tag/format mismatch) → genuinely not FMTS → Ok(None).
    //   - `UdfNotFound`: the disc is UDF but has no `IndividualSegment.tbl`
    //     → genuinely not FMTS → Ok(None).
    //   - any other error (notably `DiscRead`): a read fault → propagate so the
    //     rip fails loud / can be retried rather than dropping forensic content.
    // …and memoise the outcome for the whole disc: the walk reads the same fixed low
    // LBAs and the same file for every title (see `FmtsTableCache`). Only the two
    // DETERMINISTIC negatives are cached; a read fault propagates uncached so a
    // later title still retries.
    if table.is_none() {
        let udf = match crate::udf::read_filesystem(reader) {
            Ok(u) => Some(u),
            Err(crate::error::Error::UdfNotFilesystem) => None,
            Err(e) => return Err(e.into()),
        };
        let tbl = match udf.as_ref() {
            None => None,
            Some(u) => match u.read_file(reader, "/AACS/IndividualSegment.tbl") {
                Ok(t) => Some(t),
                Err(crate::error::Error::UdfNotFound { .. }) => None,
                Err(e) => return Err(e.into()),
            },
        };
        let parsed = tbl
            .as_deref()
            .and_then(parse_individual_segments)
            .filter(|s| !s.is_empty());
        // Locate the forensic feature clip in the SAME walk — the byte space the
        // segment SPNs are relative to (see `FmtsClipCache`). Only an FMTS disc pays
        // for the lookup. Assigned before `*table` so a read fault on the clip's ICB
        // leaves NEITHER memo filled and a later title retries the whole thing.
        *clip = Some(match (&parsed, udf.as_ref()) {
            (Some(_), Some(u)) => forensic_clip_extents(u, reader)?,
            _ => None,
        });
        *table = Some(parsed);
    }
    let Some(Some(segments)) = table.as_ref() else {
        return Ok(None); // not an FMTS disc
    };
    let segments = segments.clone();
    // ── ANCHOR the segment byte space. The SPNs are offsets into the FORENSIC
    //    FEATURE clip, NOT into this title's extent concatenation: a playlist that
    //    plays a trailer before the feature has the trailer's sectors FIRST in
    //    `title.extents`, so mapping clip byte 0 to `title.extents[0]` puts every
    //    segment in the wrong clip — the anchor probe then samples the trailer (no
    //    index key anchors ⇒ `FmtsKeyMissing` aborts the whole disc) or, worse, index
    //    keys get applied to non-forensic sectors while the real forensic units keep
    //    the base key: silent garble, no error. So the arithmetic is anchored on the
    //    forensic clip's OWN extents, which are a disc fact read from UDF. ─────────
    let Some(Some(clip_extents)) = clip.as_ref() else {
        // The disc carries a non-empty `IndividualSegment.tbl` — it HAS forensic
        // content — but the clip that content lives in could not be identified, so
        // there is no defensible anchor for the SPNs. Fail loud: a hard, retryable
        // error beats silently mapping index keys onto the wrong clip's sectors.
        tracing::warn!(target: "freemkv::keysource", segments = segments.len(), "fmts: forensic feature clip not identifiable — refusing an unanchored segment map");
        return Err(crate::error::Error::FmtsKeyMissing.into());
    };
    let clip_extents = clip_extents.clone();
    // A title that does not read the forensic clip's sectors carries no forensic
    // content (a menu/extras playlist, or simply a different clip): its base Unit
    // Key/CPS map applies and there is nothing forensic to resolve — fall back
    // (`Ok(None)`) rather than hard-failing. Without this, `resolve_content_key_map`
    // — which resolves EVERY title for the whole-disc sweep — aborts the entire
    // decrypt on the first non-forensic title (a menu playlist), and
    // `build_iso_pipeline` aborts muxing any non-main title.
    if !extents_overlap(&title.extents, &clip_extents) {
        return Ok(None);
    }
    // Keep only the segments the forensic clip actually contains: a record whose
    // clip bytes run past the clip's end belongs to no readable sector (a stale or
    // foreign table entry) and must not be mapped.
    let segments = filter_addressable_segments(segments, &clip_extents);
    if segments.is_empty() {
        return Ok(None);
    }
    // This title HAS forensic content, so the forensic index keys are REQUIRED —
    // exactly like a Unit Key. Without a configured key source we cannot obtain
    // them, so we cannot produce a complete rip: fail loud rather than silently
    // drop the forensic segments. (The caller may still choose `--raw`, which never
    // reaches this path.)
    let Some(fetch) = fetch else {
        return Err(crate::error::Error::FmtsKeyMissing.into());
    };

    // ── The expensive half — the anchor probe, the per-index phase probe and the
    //    ONE key-service round trip — reads only the FORENSIC CLIP's sectors, so it is
    //    resolved at most once per disc; it stays keyed on the title's extent list
    //    (`FmtsKeyCache`), a key FINER than the answer needs, so each distinct extent
    //    list keeps its own verdict rather than inheriting another's. Without this,
    //    `resolve_content_key_map` re-ran the whole probe AND re-asked the key service
    //    once per playlist: on a 60-playlist disc that is 60 identical key-service
    //    round trips (the storm) and tens of thousands of random 6144-byte reads for
    //    one disc-wide answer. ───────────────────────────────────────────────────────
    let ek = extent_key(format, title);
    let (index_keys, phase_of_index) = match memo.get(&ek) {
        Some((k, p)) => (k.clone(), p.clone()),
        None => {
            let probed =
                probe_fmts_index_keys(reader, &clip_extents, &segments, fetch, format, halt)?;
            // Only memoise a run whose every index reached a DEFINITE phase. A
            // read-faulted index defaulted to `Phase::All` is a property of a
            // transient live-drive fault, not of these extents — caching it would
            // spread one bad read across every remaining title.
            if probed.all_phases_definite {
                memo.insert(ek, (probed.keys.clone(), probed.phases.clone()));
            }
            (probed.keys, probed.phases)
        }
    };

    // Map array position → forensic index (element i = index i+1); add each key to
    // the pool and remember its slot by tag. Per TITLE, and deliberately so: the
    // pool is the caller's and grows across titles, so a title reached with the keys
    // already banked finds them by value at the same slots instead of appending
    // duplicates.
    let mut tag_slot: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
    if let crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } = keys {
        for (i, k) in index_keys.iter().enumerate() {
            let tag = (i + 1) as u16;
            let slot = match unit_keys.iter().position(|(_, h)| h == k) {
                Some(s) => s,
                None => {
                    let s = unit_keys.len();
                    // CPS-unit id is cosmetic for the mapped decrypt (it indexes by
                    // slot), so it carries the forensic TAG instead: it is what tells
                    // the single-CPS short-circuit these are not base CPS unit keys
                    // (see `FMTS_POOL_TAG_BASE` / `single_base_key_slot`).
                    unit_keys.push((FMTS_POOL_TAG_BASE.saturating_add(s as u32), *k));
                    s
                }
            };
            tag_slot.insert(tag, slot);
        }
    }

    // ── Build the per-segment LBA ranges: each forensic segment → its tag's key
    //    AND its index's phase. The mapped decrypt opens only that half and leaves
    //    the alternate as ciphertext (the muxer drops untouched ciphertext) —
    //    clean by construction, no garble. A segment straddling an extent boundary
    //    is left unmapped and tallied (a hard failure below). ────────────────────
    let mut ranges: Vec<(u32, u32, usize, crate::decrypt::Phase)> =
        Vec::with_capacity(segments.len());
    let mut unresolved = 0usize;
    for seg in &segments {
        // SPNs are untrusted (from IndividualSegment.tbl); an inverted record
        // (start_spn > end_spn) would underflow `end_byte - 1 - start_byte` below.
        // (Mirrors the guard in `aacs::segment::fmts_key_ranges`.)
        if seg.start_spn > seg.end_spn {
            unresolved += 1;
            continue;
        }
        let Some(&slot) = tag_slot.get(&seg.index) else {
            unresolved += 1;
            continue;
        };
        let phase = phase_of_index
            .get(&seg.index)
            .copied()
            .unwrap_or(crate::decrypt::Phase::All);
        let start_byte = seg.start_spn as u64 * 192;
        let end_byte = (seg.end_spn as u64 + 1) * 192;
        // Clip bytes → LBAs through the FORENSIC CLIP's extents (see the anchor note
        // above), never the title's.
        let (Some(a), Some(b)) = (
            clip_byte_to_lba(&clip_extents, start_byte),
            clip_byte_to_lba(&clip_extents, end_byte - 1),
        ) else {
            unresolved += 1;
            continue;
        };
        // Only emit a contiguous within-extent range (segments are ~480 KB; a rare
        // extent-straddle is left unresolved rather than given a wrong span).
        if b >= a && (b - a) as u64 == (end_byte - 1 - start_byte) / 2048 {
            ranges.push((a, b + 1, slot, phase));
        } else {
            unresolved += 1;
        }
    }
    // Every forensic segment must map to an index key. Any that did not is a hole
    // in the rip — with the full 32-key set in hand this should never happen, so
    // treat it as a hard failure rather than silently emitting a garbled segment.
    if unresolved != 0 {
        return Err(crate::error::Error::FmtsKeyMissing.into());
    }

    // Cover the NON-segment content with the base Unit Key OF THE CPS UNIT THAT LBA
    // BELONGS TO: the forensic segments (added above with their index keys) carve
    // holes out of the title's content extents; every other content unit uses its own
    // CPS unit's base UK. Fill the gaps so the map is a complete positive list — an
    // LBA in no range is nav and passes through.
    //
    // "The" base key is only well defined when the disc has ONE base CPS Unit Key.
    // Hardcoding pool slot 0 keyed every non-forensic LBA of every OTHER CPS unit
    // with the first unit's key — and that does not fail loudly, it decrypts to
    // garbage with `lost_bytes == 0`. `resolve_mux_key_map_cached` reaches this
    // resolver BEFORE its own `single_base_key_slot` short-circuit, so that guard
    // never gets to make slot 0 correct here. Resolve it per extent instead, the same
    // way the multi-CPS path does: from the extent's own ciphertext.
    let base_slots: Vec<usize> = match keys {
        crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => base_key_slots(unit_keys),
        _ => Vec::new(),
    };
    if base_slots.len() <= 1 {
        // One base CPS unit (the overwhelming majority, incl. every single-key UHD):
        // its key covers every non-forensic LBA and NO extent sampling is needed —
        // an FMTS disc stays at the anchor + phase probes it already pays for. An
        // empty pool keeps slot 0, which is what an empty map keys nothing with.
        let base_idx = base_slots.first().copied().unwrap_or(0);
        let base_gaps = fill_base_key_gaps(&title.extents, &ranges, base_idx);
        ranges.extend(base_gaps);
    } else {
        let mut last_idx = base_slots[0];
        let mut gaps = Vec::new();
        for ext in &title.extents {
            // Cooperative cancel between extents: this samples real content units
            // off the live drive, exactly like the multi-CPS loop.
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted.into());
            }
            let idx = base_slot_for_extent(reader, ext, keys, Some(fetch), format, cps, last_idx)?;
            last_idx = idx;
            gaps.extend(fill_base_key_gaps(std::slice::from_ref(ext), &ranges, idx));
        }
        ranges.extend(gaps);
    }

    Ok(Some(crate::decrypt::AacsKeyMap::from_ranges_phased(ranges)))
}

/// The disc-wide forensic answer [`probe_fmts_index_keys`] resolves: the ordered
/// index keys (element `i` = forensic index `i + 1`) and each index's decrypt phase.
struct FmtsIndexKeys {
    keys: Vec<[u8; 16]>,
    phases: std::collections::HashMap<u16, crate::decrypt::Phase>,
    /// Every index reached a DEFINITE phase — no index fell back to `Phase::All`
    /// after [`IndexProbe::ReadFault`]. Only such a run is safe to memoise; see
    /// [`resolve_mux_key_map_cached`].
    all_phases_definite: bool,
}

/// The drive- and key-service-hitting half of [`resolve_fmts_key_map`]: anchor the
/// disc's whole forensic index-key set from ONE key-service round trip, then probe
/// each index's interleave phase. Split out so the result can be memoised per disc
/// ([`FmtsKeyCache`]) — the arithmetic that turns these keys into a per-title LBA
/// map stays at the call site, where the title belongs.
///
/// `clip_extents` are the FORENSIC FEATURE CLIP's own extents — the byte space the
/// segment SPNs are relative to — and `segments` those addressable within it; every
/// read is `clip_byte_to_lba(clip_extents, …)`, so a probe never lands in another
/// clip's sectors whatever order a playlist lists its clips in. Neither probe reads
/// the caller's key pool, which is what makes the result independent of resolve
/// order across titles.
fn probe_fmts_index_keys(
    reader: &mut dyn SectorSource,
    clip_extents: &[crate::disc::Extent],
    segments: &[crate::aacs::segment::Segment],
    fetch: &crate::sector::KeyFetch,
    format: ContentFormat,
    halt: Option<&crate::halt::Halt>,
) -> io::Result<FmtsIndexKeys> {
    use crate::aacs::content::ALIGNED_UNIT_LEN;
    use crate::aacs::segment::clip_byte_to_lba;

    let check_halt = || -> io::Result<()> {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(crate::error::Error::Halted.into());
        }
        Ok(())
    };
    tracing::info!(target: "freemkv::keysource", segments = segments.len(), extents = clip_extents.len(), "fmts: begin index-key resolution");

    // Read aligned unit `index` of `seg`: clip byte `start_spn*192 + index*6144`.
    let read_unit =
        |reader: &mut dyn SectorSource, seg: &crate::aacs::segment::Segment, index: usize| {
            let clip_byte = seg.start_spn as u64 * 192 + index as u64 * ALIGNED_UNIT_LEN as u64;
            let lba = clip_byte_to_lba(clip_extents, clip_byte)?;
            let mut c = vec![0u8; ALIGNED_UNIT_LEN];
            reader.read_sectors(lba, 3, &mut c, false).ok()?;
            Some(c)
        };
    // ── ANCHOR — fetch the whole 32-key set from ONE index-1 batch. The key
    //    service returns ALL forensic index keys ordered (element i = index i+1)
    //    only for a canonical INDEX-1 sample that decrypts under the index-1 key.
    //    A forensic segment interleaves TWO variants at the aligned-unit level, so
    //    index-1's real content is one PHASE (even or odd units) and the alternate
    //    is a different variant that won't decrypt. We don't know the phase a
    //    priori, so try PHASE A (even) then PHASE B (odd): whichever is index-1's
    //    content comes back with the full set. Both phases failing (across the
    //    read-fault fallback over index-1 segments) ⇒ this disc has no FMTS keys.
    //
    //    The set's SIZE is whatever the source returns (≥ 1) — never assumed. 32
    //    is all we have seen, but a disc with a different forensic index count is
    //    not ruled out, so the map is sized to the returned `len()`, not a const.
    //    ─────────────────────────────────────────────────────────────────────────
    // Batch size = the key service's minimum-samples floor (same as the online
    // source), drawn from ONE phase to land a clean single-variant half.
    const BATCH_UNITS: usize = crate::keysource::MIN_SAMPLE_UNITS;
    // Read-fault fallback: how many index-1 segments to attempt if the leading one
    // is unreadable. The 2 phase requests happen per readable segment.
    const MAX_ANCHOR_ATTEMPTS: usize = 16;
    // Even units = p*2; odd units = p*2 + 1.
    let read_phase_batch = |reader: &mut dyn SectorSource,
                            seg: &crate::aacs::segment::Segment,
                            phase_off: usize|
     -> Option<Vec<Vec<u8>>> {
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(BATCH_UNITS);
        for p in 0..BATCH_UNITS {
            batch.push(read_unit(reader, seg, p * 2 + phase_off)?);
        }
        Some(batch)
    };
    let mut index_keys: Vec<[u8; 16]> = Vec::new();
    'anchor: for seg in segments
        .iter()
        .filter(|s| s.index == 1)
        .take(MAX_ANCHOR_ATTEMPTS)
    {
        check_halt()?;
        for phase_off in [0usize, 1usize] {
            let Some(batch) = read_phase_batch(reader, seg, phase_off) else {
                continue; // read fault on this phase — try the other / next segment
            };
            let fresh = fetch.fmts_indexes(&batch);
            // Any non-empty reply is the source's COMPLETE ordered forensic set;
            // trust it and stop. An empty reply = this phase/segment did not anchor.
            if !fresh.is_empty() {
                index_keys = fresh;
                break 'anchor;
            }
        }
    }
    // The count is whatever the source returned — not a fixed 32. Sized here, used
    // everywhere below.
    let n_index = index_keys.len();
    tracing::info!(target: "freemkv::keysource", held = n_index, "fmts: collection done");
    // At least one forensic index key is required. None ⇒ no FMTS key for this
    // disc from any source — fail loud like a missing Unit Key rather than emit
    // forensic-holed output.
    if index_keys.is_empty() {
        return Err(crate::error::Error::FmtsKeyMissing.into());
    }

    // ── PROBE each index's phase. A forensic segment interleaves two variants at
    //    the aligned-unit level; only ONE parity is this index's real content (the
    //    other is the alternate variant — a different key, garbles under ours). For
    //    each index, read a representative tagged segment and count clean decrypts
    //    of its EVEN vs ODD units under that index's key: the clean half is the
    //    index's phase. This is the ONE place `is_clean` runs — "the map must be
    //    right", verified here, once — so the mux decrypt can then trust the map.
    //    Phase is per-index and shared by every segment carrying that index. ──────
    let mut phase_of_index: std::collections::HashMap<u16, crate::decrypt::Phase> =
        std::collections::HashMap::new();
    let mut all_phases_definite = true;
    for (i, k) in index_keys.iter().enumerate() {
        check_halt()?;
        let tag = (i + 1) as u16;
        // Probe this index's parity with the anchor loop's read-fault tolerance:
        // try up to MAX_ANCHOR_ATTEMPTS same-index segments (not a single `.find`),
        // skipping any whose reads all fault. The outcome distinguishes a genuine
        // wrong key (reads succeeded, no clean parity) from a transient live-drive
        // read fault (zero decrypt evidence) — the load-bearing distinction so a
        // recoverable fault never hard-aborts a rip whose index keys are valid.
        match probe_index_phase(
            segments,
            tag,
            BATCH_UNITS,
            MAX_ANCHOR_ATTEMPTS,
            format,
            k,
            |seg, unit| read_unit(reader, seg, unit),
        ) {
            IndexProbe::Phase(phase) => {
                phase_of_index.insert(tag, phase);
            }
            IndexProbe::WrongKey => {
                // Reads SUCCEEDED but NEITHER parity decrypts clean under this index's
                // key on any same-index segment: the key is wrong (or the sample isn't
                // this index's real content). The map would be wrong — fail loud rather
                // than emit a broken segment. (Preserves the genuine-wrong-key path.)
                tracing::warn!(target: "freemkv::keysource", index = tag, "fmts: no clean phase under index key — refusing broken map");
                return Err(crate::error::Error::FmtsKeyMissing.into());
            }
            IndexProbe::ReadFault => {
                // EVERY probe read of EVERY same-index segment faulted (a transient
                // live-drive read fault — e.g. NOT READY 2/04/3E, the common bad-sector
                // sense on the BU40N). There is ZERO decrypt evidence, so this is NOT a
                // wrong key: the index key is valid and already in hand. Do NOT abort a
                // rip whose forensic keys are good. Leave this index's phase unresolved
                // so the range-builder below defaults it to `Phase::All` — decrypt BOTH
                // parities and let the demux drop the garbled alternate half (the
                // coherent-stream outcome the module doc describes for whole-range key
                // application). Degraded but complete; never a wrong-key abort.
                tracing::warn!(target: "freemkv::keysource", index = tag, "fmts: index phase probe read-faulted on every segment — defaulting Phase::All (recoverable read fault, not a wrong key)");
                all_phases_definite = false;
            }
        }
    }

    Ok(FmtsIndexKeys {
        keys: index_keys,
        phases: phase_of_index,
        all_phases_definite,
    })
}

/// The FMTS forensic feature clip's own extents — the byte space every
/// `IndividualSegment.tbl` SPN is relative to — or `None` when the disc does not
/// identify exactly one such clip.
///
/// An AACS 2.1 disc names its forensic feature `BDMV/STREAM/<clip>.fmts` (the
/// extension `bluray.rs` already resolves clip extents through), and the disc carries
/// ONE segment table, whose SPNs are therefore in ONE clip's byte space. So: exactly
/// one `.fmts` file → that clip's extents; none, or several (an ambiguous SPN space),
/// → `None`, which [`resolve_fmts_key_map`] turns into a loud
/// [`Error::FmtsKeyMissing`](crate::error::Error::FmtsKeyMissing) rather than a guess.
///
/// This is a DISC fact — no title is consulted — which is exactly why it is a sound
/// anchor: a playlist's `extents` are the concatenation of ALL its clips in playback
/// order, so their byte space is the playlist's, not the forensic clip's.
///
/// A read fault on the clip's ICB propagates (`Err`), like every other read on this
/// path; a purely structural absence is `Ok(None)`.
fn forensic_clip_extents(
    udf: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> io::Result<Option<Vec<crate::disc::Extent>>> {
    let Some(dir) = udf.find_dir("/BDMV/STREAM") else {
        return Ok(None);
    };
    let mut names = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".fmts"))
        .map(|e| e.name.clone());
    let Some(name) = names.next() else {
        return Ok(None);
    };
    if names.next().is_some() {
        tracing::warn!(target: "freemkv::keysource", "fmts: more than one forensic clip on the disc — segment byte space is ambiguous");
        return Ok(None);
    }
    let exts: Vec<crate::disc::Extent> = udf
        .file_extents(reader, &format!("/BDMV/STREAM/{name}"))
        .map_err(io::Error::from)?
        .into_iter()
        .filter(|&(lba, sectors)| lba > 0 && sectors > 0)
        .map(|(start_lba, sector_count)| crate::disc::Extent {
            start_lba,
            sector_count,
        })
        .collect();
    Ok((!exts.is_empty()).then_some(exts))
}

/// True when any extent of `a` shares a sector with any extent of `b`. Used to decide
/// whether a title actually reads the forensic clip (and so carries forensic content)
/// without assuming the two extent lists are byte-identical.
fn extents_overlap(a: &[crate::disc::Extent], b: &[crate::disc::Extent]) -> bool {
    a.iter().any(|x| {
        let x_end = x.start_lba.saturating_add(x.sector_count);
        b.iter().any(|y| {
            let y_end = y.start_lba.saturating_add(y.sector_count);
            x.start_lba < y_end && y.start_lba < x_end
        })
    })
}

/// Keep only the forensic segments addressable within the FORENSIC CLIP's extents: a
/// segment whose clip-byte start (`start_spn * 192`) maps to an LBA inside the clip is
/// real forensic content; one that does not is past the clip's end (a stale or foreign
/// table record) and is dropped. An empty result means there is nothing forensic to
/// resolve, so [`resolve_fmts_key_map`] returns `Ok(None)` and the caller's base
/// Unit-Key path applies. Extracted from `resolve_fmts_key_map` for direct testing of
/// the inclusion/exclusion decision.
fn filter_addressable_segments(
    segments: Vec<crate::aacs::segment::Segment>,
    extents: &[crate::disc::Extent],
) -> Vec<crate::aacs::segment::Segment> {
    segments
        .into_iter()
        .filter(|s| {
            crate::aacs::segment::clip_byte_to_lba(extents, s.start_spn as u64 * 192).is_some()
        })
        .collect()
}

/// Decide a forensic index's decrypt phase from the clean-sample counts of its
/// EVEN vs ODD aligned units under that index's key. Extracted from
/// [`resolve_fmts_key_map`] so the tie logic is unit-testable; the `tracing`
/// diagnostics stay at the call site, which holds the segment-index context.
///
/// * `even > odd` → [`Phase::Even`](crate::decrypt::Phase::Even); `odd > even` →
///   [`Phase::Odd`](crate::decrypt::Phase::Odd) — the clean half is this index's
///   real content variant.
/// * `even == odd == 0` → [`Error::FmtsKeyMissing`](crate::error::Error::FmtsKeyMissing):
///   NEITHER half decrypts clean, so the key is wrong (or the sample is not this
///   index's content) — fail loud rather than emit a broken segment.
/// * `even == odd > 0` → [`Phase::Even`](crate::decrypt::Phase::Even): BOTH halves
///   are clean, i.e. source-zero padding (clean under any key), so the parity is
///   immaterial — default Even.
fn resolve_tie_phase(even_clean: usize, odd_clean: usize) -> io::Result<crate::decrypt::Phase> {
    match even_clean.cmp(&odd_clean) {
        std::cmp::Ordering::Greater => Ok(crate::decrypt::Phase::Even),
        std::cmp::Ordering::Less => Ok(crate::decrypt::Phase::Odd),
        std::cmp::Ordering::Equal if even_clean == 0 => {
            Err(crate::error::Error::FmtsKeyMissing.into())
        }
        std::cmp::Ordering::Equal => Ok(crate::decrypt::Phase::Even),
    }
}

/// Outcome of probing ONE forensic index's decrypt phase (see [`probe_index_phase`]).
/// The load-bearing distinction is between the last two: a genuine wrong key and a
/// transient live-drive read fault both leave zero clean decrypts, but only the
/// former is a real `FmtsKeyMissing` — the latter must NOT abort a rip whose index
/// keys are valid.
#[derive(Debug, PartialEq, Eq)]
enum IndexProbe {
    /// A parity decrypted clean under this index's key (or a padding tie) → its phase.
    Phase(crate::decrypt::Phase),
    /// At least one unit was READ and decrypt-attempted, yet NEITHER parity came up
    /// clean under this index's key on any same-index segment → genuine wrong key.
    WrongKey,
    /// EVERY probe read of every same-index segment faulted (`read` returned `None`
    /// for all attempts) → zero decrypt evidence. A recoverable read fault, NOT a
    /// wrong key: there is no data to conclude the key is bad.
    ReadFault,
}

/// Probe one forensic index's decrypt phase by reading a representative segment's
/// EVEN vs ODD aligned units and counting clean decrypts under `key`. Extracted
/// from [`resolve_fmts_key_map`] so the read-fault-vs-wrong-key decision is
/// directly testable without a full UDF/segment-table fixture.
///
/// Mirrors the anchor loop's read-fault tolerance: try up to `max_segments`
/// same-index segments (rather than a single `.find`), skipping any whose reads all
/// fault, and only conclude [`IndexProbe::WrongKey`] once a segment actually yielded
/// decrypt attempts. If EVERY read of EVERY same-index segment faults, return
/// [`IndexProbe::ReadFault`] — the caller then leaves the phase unresolved (defaults
/// to `Phase::All`) instead of hard-aborting the rip. `read(seg, unit)` reads
/// aligned unit `unit` of `seg`; `None` is a read fault.
///
/// Masking guard: [`IndexProbe::ReadFault`] is returned ONLY when not a single read
/// succeeded, so a genuine wrong key (whose reads DO succeed) can never be masked as
/// a read fault — any successful, non-clean decrypt yields [`IndexProbe::WrongKey`].
fn probe_index_phase(
    segments: &[crate::aacs::segment::Segment],
    tag: u16,
    batch_units: usize,
    max_segments: usize,
    format: ContentFormat,
    key: &[u8; 16],
    mut read: impl FnMut(&crate::aacs::segment::Segment, usize) -> Option<Vec<u8>>,
) -> IndexProbe {
    use crate::aacs::content::{aacs_unit_encrypted, decrypt_unit, is_clean};
    let mut any_read = false;
    for seg in segments
        .iter()
        .filter(|s| s.index == tag)
        .take(max_segments)
    {
        let (mut even, mut odd) = (0usize, 0usize);
        let mut seg_read = false;
        for p in 0..batch_units {
            for (phase_off, counter) in [(0usize, &mut even), (1usize, &mut odd)] {
                if let Some(mut c) = read(seg, p * 2 + phase_off) {
                    seg_read = true;
                    if aacs_unit_encrypted(&c, format) {
                        decrypt_unit(&mut c, key);
                        if is_clean(&c, format) {
                            *counter += 1;
                        }
                    }
                }
            }
        }
        if !seg_read {
            continue; // every read of this segment faulted — try the next same-index one
        }
        any_read = true;
        // A clean parity (even != odd) or a padding tie (even == odd > 0) resolves the
        // phase; even == odd == 0 is this segment's wrong-key signature, but a DIFFERENT
        // same-index segment could still anchor (this one's sampled units may all be the
        // alternate variant), so keep trying rather than concluding immediately.
        if let Ok(phase) = resolve_tie_phase(even, odd) {
            return IndexProbe::Phase(phase);
        }
    }
    if any_read {
        IndexProbe::WrongKey
    } else {
        IndexProbe::ReadFault
    }
}

/// Back-fill the LBA gaps NOT covered by the forensic segment ranges with the base
/// Unit Key, so the finished map is a COMPLETE positive list over the title's
/// content extents: every content LBA resolves to either a forensic key (inside a
/// segment) or the base key (`base_idx`). An LBA left in no range would pass
/// ciphertext through as clear — this range arithmetic guarantees there is no such
/// hole inside any extent. Extracted from [`resolve_fmts_key_map`] for exhaustive
/// direct testing (gaplessness over every extent).
///
/// `forensic_ranges` are the already-built per-segment ranges; only their
/// `[start, end)` spans matter here (they carve the holes — the key idx / phase are
/// irrelevant). The return is the base-key fill ranges ONLY; the caller appends
/// them to `forensic_ranges` to form the full map.
fn fill_base_key_gaps(
    extents: &[crate::disc::Extent],
    forensic_ranges: &[(u32, u32, usize, crate::decrypt::Phase)],
    base_idx: usize,
) -> Vec<(u32, u32, usize, crate::decrypt::Phase)> {
    let cuts: Vec<(u32, u32)> = {
        let mut c: Vec<(u32, u32)> = forensic_ranges.iter().map(|&(s, e, _, _)| (s, e)).collect();
        c.sort_unstable();
        c
    };
    let mut fills = Vec::new();
    for ext in extents {
        let end = ext.start_lba.saturating_add(ext.sector_count);
        let mut cur = ext.start_lba;
        for &(cs, ce) in &cuts {
            if ce <= cur || cs >= end {
                continue; // cut outside this extent
            }
            if cs > cur {
                fills.push((cur, cs, base_idx, crate::decrypt::Phase::All));
            }
            cur = cur.max(ce);
        }
        if cur < end {
            fills.push((cur, end, base_idx, crate::decrypt::Phase::All));
        }
    }
    fills
}

/// A single-key content map: every content extent → `idx`; everything else passes
/// through. The positive-map replacement for the old "one key everywhere" default.
fn content_map(title: &DiscTitle, idx: usize) -> crate::decrypt::AacsKeyMap {
    let ranges = title
        .extents
        .iter()
        .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count), idx))
        .collect();
    crate::decrypt::AacsKeyMap::from_ranges(ranges)
}

/// Resolve the proactive [`AacsKeyMap`](crate::decrypt::AacsKeyMap) for a title
/// before muxing. It decides which held unit key decrypts each of the title's
/// LBA ranges and secures any key the pool is missing through the app's
/// configured source (`fetch`) up front, never reactively per unit at mux time.
///
/// This is what ends the key-server storm. The old mux decrypted a unit, checked
/// whether the plaintext looked like clean MPEG-TS, and — because authored-bad
/// content never reaches that bar — re-asked the key service for a key it already
/// held. There is no per-unit byte pattern that separates "correctly decrypted
/// but authored-bad" from "still encrypted", so that check is unanswerable. Here
/// we answer the answerable question instead: which CPS unit does each LBA range
/// belong to, decided by the disc's key structure (validated once against real
/// ciphertext samples, where the `is_clean` proof IS sound). The mux then just
/// decrypts each unit with its mapped key and trusts it.
///
/// Single-CPS (the overwhelming majority, incl. every single-key UHD) keys every
/// content extent with one index; multi-CPS keys each extent with the key that
/// opens a real sample from it; FMTS layers per-segment index keys on top. Any LBA
/// outside the title's content (nav/filesystem) is in no range and passes through.
pub fn resolve_mux_key_map(
    reader: &mut dyn SectorSource,
    title: &DiscTitle,
    keys: &mut crate::decrypt::DecryptKeys,
    fetch: Option<&crate::sector::KeyFetch>,
    format: ContentFormat,
    halt: Option<&crate::halt::Halt>,
) -> io::Result<crate::decrypt::AacsKeyMap> {
    // One-shot: a single title shares no extents with anything, so a fresh cache
    // never hits. Multi-title callers use `resolve_mux_key_map_cached` instead.
    resolve_mux_key_map_cached(
        reader,
        title,
        keys,
        fetch,
        format,
        halt,
        &mut DiscKeyCache::new(),
    )
}

/// Memoises the multi-CPS "which held unit key opens this extent" decision across
/// the titles of ONE disc, for [`resolve_mux_key_map_cached`].
///
/// Keyed by content format plus the extent's exact `(start_lba, sector_count)`, so
/// a hit returns the index that was resolved from *those same physical bytes* —
/// see the safety argument on [`resolve_mux_key_map_cached`]. Only successfully
/// resolved extents are memoised; a no-samples extent (whose index is inherited
/// from the preceding extent of the SAME title, i.e. not a property of the extent)
/// and the fail-loud "no key opens it" outcome are never cached.
///
/// A disc's playlists overwhelmingly reference the same handful of clips (main
/// feature, play-all, per-chapter and seamless-branch variants), so without this
/// the same extents are re-sampled off the drive once per playlist: 8 random
/// 6144-byte reads each, ~200 ms of seek apiece on a stock BD drive.
///
/// Scope, stated precisely: this memo covers the extent-sampling reads that decide
/// which CPS unit an extent belongs to, and nothing else. On an FMTS (AACS 2.1)
/// disc [`resolve_fmts_key_map`] runs first and returns a finished map before the
/// extent loop below is ever reached, so the loop's reads are removed by
/// [`FmtsTableCache`] and [`FmtsKeyCache`] instead — but a MULTI-CPS FMTS disc
/// samples through this same memo from the gap fill ([`base_slot_for_extent`]), and
/// the cached value means the same thing on both paths: the pool slot whose key
/// opens that extent's own ciphertext.
pub(crate) type CpsUnitCache = std::collections::HashMap<(ContentFormat, u32, u32), usize>;

/// The disc's forensic segment table (`/AACS/IndividualSegment.tbl`), resolved at
/// most ONCE per disc: `None` = not looked for yet; `Some(None)` = looked for and
/// this disc is not FMTS; `Some(Some(v))` = the parsed, non-empty segment list.
///
/// Every input is a property of the MEDIA, not of a title: the UDF walk
/// ([`crate::udf::read_filesystem`]) reads fixed low LBAs (the anchor at 256, the
/// VDS, the FSD, the root directory), `read_file` follows that file's own
/// allocation descriptors, and `parse_individual_segments` is pure. Re-running it
/// per title re-reads ~35 single sectors at low LBAs from a head the previous
/// title's content sampling left deep in the content area — a full-stroke seek out
/// and back per playlist, for a byte-identical answer.
///
/// Only the two *deterministic* negatives are memoised as "not FMTS"
/// (`UdfNotFilesystem`, and `UdfNotFound` for the table): a read fault is NEVER
/// cached, so a transient failure still propagates and a later title still retries.
pub(crate) type FmtsTableCache = Option<Option<Vec<crate::aacs::segment::Segment>>>;

/// Memoises the FMTS (AACS 2.1) forensic **index-key set and per-index phase** —
/// the expensive half of [`resolve_fmts_key_map`] — across the titles of ONE disc.
///
/// Keyed by `(format, the title's exact extent list)`, mirroring
/// [`crate::disc::pgs_forced_probe::ForcedProbeCache`]. Every read the anchor probe,
/// the phase probe and the key-service call make is
/// `clip_byte_to_lba(forensic_clip_extents, …)` and the probed segments are
/// `filter_addressable_segments(_, forensic_clip_extents)` — both DISC facts (see
/// [`forensic_clip_extents`]), so the answer no longer varies by title at all. The
/// extent list is KEPT in the key deliberately: it is finer than the answer needs, so
/// each distinct extent list still gets its own verdict rather than inheriting
/// another's, and a title can never be served an answer resolved from bytes it does
/// not read. Two titles with the same extent list feed byte-identical samples to a
/// stateless [`crate::sector::KeyFetch`] and to the same `is_clean` arithmetic under
/// the same `format` — see the safety argument on [`resolve_mux_key_map_cached`].
///
/// The value holds the ordered index keys (element `i` = forensic index `i + 1`)
/// and the resolved phase per index tag. It is key material: never logged, never
/// rendered, and dropped with the disc's resolve.
pub(crate) type FmtsKeyCache = std::collections::HashMap<
    (ContentFormat, Vec<(u32, u32)>),
    (
        Vec<[u8; 16]>,
        std::collections::HashMap<u16, crate::decrypt::Phase>,
    ),
>;

/// The disc's forensic feature clip extents, resolved in the SAME UDF walk as
/// [`FmtsTableCache`] and at most once per disc: `None` = not looked for yet;
/// `Some(None)` = looked for and not identifiable (or the disc is not FMTS);
/// `Some(Some(v))` = the clip's extents, the anchor for every segment SPN.
///
/// A disc fact like the table itself — see [`forensic_clip_extents`] for why, and for
/// what `Some(None)` costs on an FMTS disc (a loud `FmtsKeyMissing`, not a guess).
pub(crate) type FmtsClipCache = Option<Option<Vec<crate::disc::Extent>>>;

/// The three FMTS memos, bundled so [`resolve_fmts_key_map`] takes one argument for
/// all of them and the multi-CPS memo can be borrowed independently.
#[derive(Default)]
pub(crate) struct FmtsCache {
    /// The disc's forensic segment table — see [`FmtsTableCache`].
    table: FmtsTableCache,
    /// The disc's forensic feature clip extents — see [`FmtsClipCache`].
    clip: FmtsClipCache,
    /// The forensic index keys + phases — see [`FmtsKeyCache`].
    keys: FmtsKeyCache,
}

/// Everything [`resolve_mux_key_map_cached`] memoises across the titles of ONE
/// disc. One value threaded through `resolve_content_key_map`'s per-title loop; the
/// fields are independent and are borrowed disjointly.
#[derive(Default)]
pub(crate) struct DiscKeyCache {
    /// Multi-CPS "which held key opens this extent" — see [`CpsUnitCache`].
    pub(crate) cps: CpsUnitCache,
    /// The FMTS segment table and index-key memos — see [`FmtsCache`].
    pub(crate) fmts: FmtsCache,
}

impl DiscKeyCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// The `(format, extents)` cache key used by [`FmtsKeyCache`].
fn extent_key(format: ContentFormat, title: &DiscTitle) -> (ContentFormat, Vec<(u32, u32)>) {
    (
        format,
        title
            .extents
            .iter()
            .map(|e| (e.start_lba, e.sector_count))
            .collect(),
    )
}

/// [`resolve_mux_key_map`] with a caller-owned per-disc cache ([`DiscKeyCache`])
/// shared across the titles of one disc.
///
/// # Why a cache cannot change a resolved key
///
/// ## Multi-CPS extents ([`CpsUnitCache`])
///
/// The cached value is the pool index `pick` chose for an extent, and every input
/// to that choice is stable across the titles of one disc:
///
/// * The samples are a deterministic function of the extent's `(start_lba,
///   sector_count)` and `format` — both in the key — read from unchanging media.
/// * The key pool is APPEND-only here (base-key fetch and the FMTS resolver only
///   push), and `pick` returns the FIRST pool entry that opens a sample, so a
///   later, longer pool yields the same first match for the same samples.
/// * [`crate::sector::KeyFetch`] is stateless by contract, and any key a hit's
///   index refers to was already banked into the pool on the miss that filled the
///   entry — so skipping the re-fetch skips no side effect the map depends on.
///
/// ## FMTS segment table ([`FmtsTableCache`])
///
/// Disc-invariant outright: no input to the UDF walk, the `read_file` or the parse
/// mentions the title. See the type's doc for the negatives that are (and are not)
/// memoised.
///
/// ## FMTS forensic clip extents ([`FmtsClipCache`])
///
/// Disc-invariant outright, like the table: the clip is found by name in the UDF tree
/// and its extents come from its own allocation descriptors. No title is consulted —
/// which is the point. It is the byte-space anchor for every segment SPN, and
/// anchoring on a TITLE's extent list instead made a playlist that lists a trailer
/// before the feature map every segment into the trailer's sectors.
///
/// ## FMTS index keys and phases ([`FmtsKeyCache`])
///
/// Every input to the anchor and phase probes is now a disc fact: the probes read
/// `clip_byte_to_lba(forensic_clip_extents, …)` and visit
/// `filter_addressable_segments(_, forensic_clip_extents)`. The title's extent list
/// stays IN the key anyway — a strictly finer key than the answer needs, so no title
/// inherits a verdict resolved from bytes it does not read. Given the same key, the
/// same `format` and unchanging media:
///
/// * `filter_addressable_segments` yields the same segments in the same order, so
///   the anchor loop and each `probe_index_phase` visit the same segments.
/// * every probe read is at the same LBA, so the same ciphertext is fed to a
///   [`crate::sector::KeyFetch`] that is stateless by contract, and to the same
///   `aacs_unit_encrypted` / `decrypt_unit` / `is_clean` arithmetic.
/// * NEITHER probe reads the key pool. The anchor takes its keys from `fetch`; the
///   phase probe takes them from the anchor's reply. So the pool's growth across
///   titles — the one thing that does change between calls — cannot move this
///   result, and the cached value is independent of the order titles are resolved
///   in.
///
/// What is deliberately NOT memoised, which is what makes this safe rather than
/// merely faster:
///
/// * the fail-loud `FmtsKeyMissing` verdicts (no anchor, wrong key), so a retry
///   after a key source is reconfigured re-probes rather than inheriting nothing;
/// * a run where any index's phase probe came back
///   [`IndexProbe::ReadFault`] — that leaves the phase defaulted to `Phase::All`
///   (degraded but complete), a property of a transient live-drive fault and NOT of
///   the extents. Caching it would spread one bad read across every remaining title.
///
/// Everything downstream of the cache still runs per title: the pool insertion that
/// turns index keys into slots, the per-segment LBA range arithmetic, and the
/// base-key gap fill — all of which genuinely depend on this title's extents.
///
/// Halt is polled per extent AND once on entry to the FMTS branch, so a 60-playlist
/// sweep stays cancellable even when every title is served from cache.
pub(crate) fn resolve_mux_key_map_cached(
    reader: &mut dyn SectorSource,
    title: &DiscTitle,
    keys: &mut crate::decrypt::DecryptKeys,
    fetch: Option<&crate::sector::KeyFetch>,
    format: ContentFormat,
    halt: Option<&crate::halt::Halt>,
    cache: &mut DiscKeyCache,
) -> io::Result<crate::decrypt::AacsKeyMap> {
    // Borrow the memos disjointly: the FMTS branch needs its three mutably while the
    // multi-CPS loop below needs the CPS one.
    let DiscKeyCache { cps: cache, fmts } = cache;

    // The base Unit Key pool is always resolved and banked by the caller before mux
    // (autorip's pre-rip gate; the ISO path's `decrypt_keys()`), so an AACS title
    // reaches here with a non-empty pool — an empty pool is reported as
    // `DecryptKeys::None` and takes the CSS/clear arm above.
    if !matches!(keys, crate::decrypt::DecryptKeys::Aacs { .. }) {
        // CSS / clear: the AACS map keys nothing here — an empty map passes every
        // unit through (CSS self-descrambles on its own path).
        return Ok(crate::decrypt::AacsKeyMap::from_ranges(Vec::new()));
    }
    // FMTS (AACS 2.1): if the disc carries `IndividualSegment.tbl`, the forensic
    // segments need per-index keys the base Unit Key can't open. Resolve them up
    // front from the configured source and build a per-segment map. Returns `None`
    // when the disc is not FMTS, or no key source is configured (then the base UK
    // path below applies and the forensic units garble → demux drops them).
    if let Some(map) = resolve_fmts_key_map(reader, title, keys, fetch, format, halt, fmts, cache)?
    {
        return Ok(map);
    }
    // The single-CPS short-circuit asks about the BASE CPS unit keys only. It must not
    // read the pool's LENGTH: on an FMTS disc `resolve_fmts_key_map` APPENDS this
    // disc's forensic index keys to the same caller-owned pool, so once ANY forensic
    // title has resolved the pool is `1 + n_index` long for every later title. Keying
    // off the length there dropped every subsequent single-CPS title into the
    // multi-CPS sampling path (8 random 6144-byte reads per extent, and a
    // `DecryptFailed` abort of the WHOLE-disc map for any extent no pooled key opens)
    // — making the outcome depend on which playlist happened to resolve FIRST, the
    // exact order-independence this function's docs promise. The forensic keys are
    // tagged in the pool ([`FMTS_POOL_TAG_BASE`]), so the base count is exact.
    if let crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } = keys
        && let Some(slot) = single_base_key_slot(unit_keys)
    {
        // One CPS unit → its key over every content extent; nav passes through.
        return Ok(content_map(title, slot));
    }

    // Multi-CPS: read a spread of real encrypted units from each extent and pick
    // the held key that opens one (the `is_clean` proof is sound HERE — samples
    // are guaranteed real content, not the authored-bad units that trip the mux).
    let sample_units = |reader: &mut dyn SectorSource, start: u32, sectors: u32| -> Vec<Vec<u8>> {
        sample_encrypted_units(reader, start, sectors, format)
    };
    // Here the question is "which HELD key opens this extent", so every pool entry
    // is a candidate, in pool order.
    let pick = |samples: &[Vec<u8>], pool: &[(u32, [u8; 16])]| -> Option<usize> {
        let all: Vec<usize> = (0..pool.len()).collect();
        pick_pool_slot(samples, pool, &all, format)
    };

    let mut ranges: Vec<(u32, u32, usize)> = Vec::with_capacity(title.extents.len());
    let mut last_idx = 0usize;
    for ext in &title.extents {
        // Cooperative cancel between extents: multi-CPS sampling reads real
        // content units off the live drive, so honor an operator stop here too.
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(crate::error::Error::Halted.into());
        }
        // Already resolved for this exact extent (a clip another playlist shares):
        // reuse the index instead of re-sampling the same physical units.
        let ck = (format, ext.start_lba, ext.sector_count);
        if let Some(&hit) = cache.get(&ck) {
            last_idx = hit;
            ranges.push((
                ext.start_lba,
                ext.start_lba.saturating_add(ext.sector_count),
                hit,
            ));
            continue;
        }
        let samples = sample_units(reader, ext.start_lba, ext.sector_count);
        // Snapshot the current pool for the pure `pick` closure.
        let pool: Vec<(u32, [u8; 16])> = match keys {
            crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => unit_keys.clone(),
            _ => Vec::new(),
        };
        let mut idx = pick(&samples, &pool);
        if idx.is_none()
            && let Some(f) = fetch
            && !samples.is_empty()
        {
            let fresh = f.unit_keys(&samples);
            if let crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } = keys {
                for k in fresh {
                    if !unit_keys.iter().any(|(_, h)| *h == k) {
                        let i = unit_keys.len() as u32;
                        unit_keys.push((i, k));
                    }
                }
                idx = pick(&samples, unit_keys);
            }
        }
        // `sample_units` draws REAL content (not authored-bad units), so a sample
        // no held or fetched key decrypts to clean means this extent's CPS-unit key
        // is genuinely absent. Building a map that silently assigns a WRONG key
        // (the neighbour's) would corrupt the whole extent with lost_bytes==0 — so
        // fail loud instead: the keymap is built ONLY when every extent with
        // encrypted content is classified. An extent with no sampleable encrypted
        // units (nothing to mis-decrypt) carries the previous index harmlessly.
        let idx = match idx {
            // A real decision from this extent's own ciphertext — memoise it.
            Some(i) => {
                cache.insert(ck, i);
                i
            }
            // Inherited from the PRECEDING extent of THIS title, so it is not a
            // property of this extent: never cache it (another title reaching the
            // same extent may carry a different previous index).
            None if samples.is_empty() => last_idx,
            // Fail-loud, and not cached: a later extent's fetch may bank the key
            // that opens this one, so a retry must re-sample rather than inherit a
            // stale verdict.
            None => return Err(crate::error::Error::DecryptFailed.into()),
        };
        last_idx = idx;
        ranges.push((
            ext.start_lba,
            ext.start_lba.saturating_add(ext.sector_count),
            idx,
        ));
    }
    Ok(crate::decrypt::AacsKeyMap::from_ranges(ranges))
}

/// Assemble the ISO mux pipeline (read+decrypt → demux → parse) for
/// a `FileSectorSource`-backed reader. Returns the resulting
/// `PipelinedPesStream`.
///
/// # Parameters
/// - `reader`: the sector source to read from (typically a
///   `FileSectorSource` over the ISO image).
/// - `title`: the selected title; its `extents` drive the read range and its
///   `streams` build the demux/parse tables.
/// - `keys`: decryption keys applied per sector batch. Pass
///   [`crate::decrypt::DecryptKeys::None`] for raw / unencrypted reads (the
///   decrypt decorator then becomes a pass-through).
/// - `batch_sectors`: read batch size in logical (2048-byte) sectors — a
///   throughput/latency tuning knob, not a correctness parameter.
/// - `format`: container format (`BdTs` → TS demuxer, `MpegPs` → PS demuxer).
/// - `raw`: ciphertext passthrough. When `true`, the per-title CSS crack
///   (`resolve_dvd_title_key`) is skipped entirely — no key is resolved and a
///   scrambled title is neither descrambled nor hard-failed.
/// - `halt`: cooperative cancel token (not a timeout); when cancelled the
///   pipeline stops at the next boundary (and the CSS crack surfaces `Halted`).
///   `None` disables cancellation.
/// - `event_fn`: optional progress/event callback invoked by the prefetcher.
/// - `fetch`: optional key source used UP FRONT by [`resolve_mux_key_map`] to
///   secure any CPS-unit key the pool is missing. Not a per-unit mux-time
///   callback: the map decides the key for every LBA before the read loop starts.
// Nine reader/title/keys/tuning/callback params is inherent to the mux entry
// point; grouping them into a struct would only move the same fields around.
#[allow(clippy::too_many_arguments)]
pub fn build_iso_pipeline<S: SectorSource + Send + 'static>(
    mut reader: S,
    title: DiscTitle,
    mut keys: crate::decrypt::DecryptKeys,
    batch_sectors: u16,
    format: ContentFormat,
    raw: bool,
    halt: Option<crate::halt::Halt>,
    event_fn: Option<crate::sector::prefetched::EventFn>,
    fetch: Option<crate::sector::KeyFetch>,
) -> io::Result<PipelinedPesStream> {
    let extents = title.extents.clone();
    // CSS (DVD) key resolution — the shared per-title step (also used by the
    // live-drive single-pass `DiscStream`). A `None`/MPEG-PS title cracks its own
    // key from the reader in playback order; AACS `.evo` (also MPEG-PS) arrives as
    // `Aacs` and is untouched; a clear DVD stays `None`; `raw` skips it entirely.
    // Without this a detection-miss CSS DVD would mux scrambled sectors as corrupt
    // video. `halt` lets /api/stop interrupt the crack scan.
    crate::css::resolve_dvd_title_key(
        &mut reader,
        &extents,
        &mut keys,
        batch_sectors,
        format,
        raw,
        halt.as_ref(),
    )?;
    // Unit alignment is an AACS concept: AACS decrypts whole 6144-byte (3-sector)
    // units, so the producer must hand the decrypt step 3-sector-aligned batches.
    // CSS (DVD) and unencrypted content decrypt per 2048-byte sector — forcing
    // 3-sector alignment there rejects any extent whose sector count isn't a
    // multiple of 3 (DVD IFO cells routinely aren't) with ExtentNotUnitAligned.
    let unit_align: u16 = match &keys {
        crate::decrypt::DecryptKeys::Aacs { .. } => 3,
        _ => 1,
    };
    // MUX path: read > decrypt > mux. Resolve the proactive AACS key map UP FRONT
    // — one key per CPS unit / segment, secured from the configured source and
    // recorded against the LBA ranges it covers. The mux then decrypts each unit
    // with its KNOWN key and trusts it: no per-unit `is_clean` verdict, no reactive
    // key-fetch, no key-server storm. A unit that decrypts to broken TS is the
    // muxer's problem, exactly as before. AACS-only; CSS self-cracks per region.
    let key_map = match &keys {
        crate::decrypt::DecryptKeys::Aacs { .. } => Some(std::sync::Arc::new(resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            fetch.as_ref(),
            format,
            halt.as_ref(),
        )?)),
        _ => None,
    };
    // The map IS the title's read plan: it says which CPS unit / forensic segment
    // each LBA belongs to. Walk ONLY the units it marks as ours — every default /
    // CPS unit, and inside an FMTS forensic segment only our-phase units. The
    // alternate-phase units are a different device group's variant; a licensed
    // player never reads them, and neither do we — they are never fetched,
    // decrypted, or handed to the demux, so the demux sees one gapless our-variant
    // stream (no ciphertext to trip a concealed-gap resync). A non-forensic map
    // returns the extents unchanged, so the common disc reads exactly as before.
    let full_extents = extents.clone();
    let extents = match &key_map {
        Some(map) => map.read_plan(&extents, unit_align as u32),
        None => extents,
    };
    // The plan and the clips' feed spans must describe the SAME bytes.
    //
    // A clip's span was measured over the title's full extents at scan time,
    // and a frame is placed by the offset it was read from. When a forensic
    // segment makes the plan drop alternate-phase units, the mux feeds fewer
    // bytes than the spans describe and the two drift apart cumulatively —
    // every frame after the first segment looks earlier than it is, and near a
    // join it is placed in the wrong clip or dropped. The spans still tile each
    // other perfectly, so the trust check cannot see it.
    //
    // Provenance is only meaningful when the feed matches. When it does not,
    // say so and let placement fall back to timestamps, which is what the
    // untrusted path exists for.
    let feed_matches_spans = extents == full_extents;
    let mut decrypting =
        crate::sector::DecryptingSectorSource::new(Box::new(reader) as Box<dyn SectorSource>, keys);
    if let Some(map) = key_map {
        decrypting = decrypting.with_key_map(map);
    }
    // Loss-counter handle. The mux does NOT tally decrypt-quality misses: a
    // broken-TS unit is the muxer's concern, and a missing key is an up-front
    // resolve failure — indistinguishable from bad authoring at this seam, so
    // counting it would false-abort a bad-encoded-but-decryptable disc. A genuine
    // can't-decrypt surfaces as `Err`; `lost_bytes()` reflects physical read loss
    // only (there is no decrypt-loss term to fold in).

    // Wrong-substream fix (Silence-of-the-Lambs): before the prefetcher takes
    // the reader, probe the feature head through the (plaintext) decrypting
    // source and re-route the title's declared AC-3 audio onto the physically
    // correct `0x8x` sub-streams. No-op for non-DVD or an empty probe. Reset the
    // unit base afterward so the prefetcher's first batch starts clean.
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
    decrypting.set_unit_base(0);

    let prefetched = crate::sector::PrefetchedSectorSource::new_with_events(
        decrypting,
        extents,
        batch_sectors,
        unit_align,
        halt.clone(),
        event_fn,
    )
    .map_err(|e| -> io::Error { e.into() })?;
    let (rx, recycle_tx, shell) = prefetched.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, format);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, halt, ts, ps)
            .map_err(|e| -> io::Error { e.into() })?;
    Ok(PipelinedPesStream::new(
        demux_thread,
        demux_rx,
        title,
        parsers,
        pid_to_track,
    ))
}

/// Assemble the M2TS file mux pipeline (read → demux → parse) for a
/// byte-stream reader. Scans the head for FMKV header or PMT/PAT,
/// rebuilds the title metadata, then wraps a chained reader (head +
/// remainder) in a `BytePrefetcher` feeding the demux + parse
/// threads.
fn build_m2ts_pipeline<R: std::io::Read + Send + 'static>(
    mut reader: R,
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

    // Try FMKV metadata header first; fall back to PMT scan. Only a
    // genuine absence of the FMKV magic (`Ok(None)`) falls through to
    // the PMT path — a corrupt/truncated FMKV header (`Err`) propagates
    // instead of being misreported as a PMT-derived title or NoStreams.
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

    // Chain: any un-consumed head bytes + the remainder of the
    // reader. The demuxer sees a contiguous M2TS byte stream.
    let remaining_head = head[head_consumed..].to_vec();
    let chained: Box<dyn Read + Send> = Box::new(io::Cursor::new(remaining_head).chain(reader));

    let prefetcher = crate::io::byte_prefetcher::BytePrefetcher::new(
        chained,
        crate::io::byte_prefetcher::DEFAULT_CHUNK_BYTES,
        None,
    )?;
    let (rx, recycle_tx, shell) = prefetcher.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, ContentFormat::BdTs);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, None, ts, ps)
            .map_err(|e| -> io::Error { e.into() })?;
    Ok(PipelinedPesStream::new(
        demux_thread,
        demux_rx,
        title,
        parsers,
        pid_to_track,
    ))
}

#[cfg(test)]
mod tests {
    use super::super::videomap::{Medium, SourceInfo};
    use super::StreamUrl;
    use super::parse_url;
    use super::validate_network_addr;
    use super::{build_demux_state, build_iso_pipeline, input, output};
    use crate::decrypt::DecryptKeys;
    use crate::disc::{ContentFormat, DiscTitle, Extent};
    use crate::pes::Stream as _;
    use crate::sector::SectorSource;
    use std::path::PathBuf;

    /// `parse_url` must never panic on ANY input — it is the front door for
    /// caller-supplied URL strings, so a panic here would crash the binary on
    /// malformed input instead of surfacing a clean error downstream. Feed it a
    /// battery of adversarial strings (empty, doubled/garbled schemes, embedded
    /// NUL, unicode, a very long path, lone scheme markers) plus an exhaustive
    /// sweep of every single byte 0x00..=0xFF as the whole input and as a scheme
    /// suffix. Any `StreamUrl` variant is an acceptable result; the only failure
    /// mode under test is a panic.
    #[test]
    fn parse_url_never_panics_on_adversarial_input() {
        let mut cases: Vec<String> = vec![
            String::new(),
            "://".into(),
            "//".into(),
            ":".into(),
            "disc".into(),
            "disc:/".into(),
            "disc:://".into(),
            "disc://disc://".into(),
            "iso://iso://x".into(),
            "mkv://mkv://mkv://".into(),
            "iso://\0/etc".into(),              // embedded NUL
            "iso://日本語/フィルム.iso".into(), // unicode path
            "network://[::1]:9000".into(),
            "ftp://host/x".into(),
            format!("iso://{}", "a".repeat(100_000)), // very long path
            "\u{feff}disc://".into(),                 // BOM prefix
        ];
        // Every byte as the entire input, and as an iso:// path suffix.
        for b in 0u8..=255 {
            cases.push(String::from_utf8_lossy(&[b]).into_owned());
            cases.push(format!("iso://{}", String::from_utf8_lossy(&[b])));
        }
        for c in &cases {
            // The contract: returns SOME variant, never panics. We also exercise
            // scheme()/path_str()/is_disc_source() so their match arms can't
            // panic on the parsed result either.
            let u = parse_url(c);
            let _ = u.scheme();
            let _ = u.path_str();
            let _ = u.is_disc_source();
        }
    }

    #[test]
    fn disk_scheme_is_alias_for_disc() {
        // `disk://` must parse identically to `disc://`: empty = auto-detect
        // (device None), a trailing path = explicit device. A Windows user
        // typing `disk://i:` must reach the same live-disc path as `disc://`.
        match (parse_url("disk://"), parse_url("disc://")) {
            (StreamUrl::Disc { device: a }, StreamUrl::Disc { device: b }) => {
                assert_eq!(a, None);
                assert_eq!(b, None);
            }
            other => panic!("disk:// / disc:// must both be Disc, got {other:?}"),
        }
        match (parse_url("disk://i:"), parse_url("disc://i:")) {
            (StreamUrl::Disc { device: a }, StreamUrl::Disc { device: b }) => {
                assert_eq!(a, Some(PathBuf::from("i:")));
                assert_eq!(b, Some(PathBuf::from("i:")));
                assert_eq!(a, b, "disk:// device must match disc:// device");
            }
            other => panic!("disk://i: / disc://i: must both be Disc, got {other:?}"),
        }
    }

    #[test]
    fn validate_network_addr_rejects_portless() {
        // Empty, bare IPv4, and bare IPv6 (which contains ':') must all fail.
        assert!(validate_network_addr("").is_err());
        assert!(validate_network_addr("127.0.0.1").is_err());
        assert!(validate_network_addr("::1").is_err());
        assert!(validate_network_addr("2001:db8::1").is_err());
        // host:port and ip:port forms pass.
        assert!(validate_network_addr("127.0.0.1:9000").is_ok());
        assert!(validate_network_addr("host:9000").is_ok());
    }

    #[test]
    fn validate_network_addr_requires_numeric_port() {
        // An empty port (`host:`) and a non-numeric port (`host:abc`) both
        // contain ':' but are NOT valid host:port — must be rejected.
        assert!(validate_network_addr("host:").is_err());
        assert!(validate_network_addr("127.0.0.1:").is_err());
        assert!(validate_network_addr("host:abc").is_err());
        assert!(validate_network_addr("host:99x").is_err());
        // Out-of-u16-range port is rejected (parse::<u16> fails).
        assert!(validate_network_addr("host:70000").is_err());
        // Bracketed IPv6 with a valid port passes; split on the LAST ':' so the
        // address colons are not mistaken for the port separator.
        assert!(validate_network_addr("[2001:db8::1]:9000").is_ok());
        // Bracketed IPv6 WITHOUT a port is rejected (port substring not a u16).
        assert!(validate_network_addr("[2001:db8::1]").is_err());
        // Valid numeric port (incl. 0 and max u16) passes.
        assert!(validate_network_addr("host:0").is_ok());
        assert!(validate_network_addr("host:65535").is_ok());
    }

    // The decrypt-verdict matrix (raw / unencrypted / AACS-no-key /
    // CSS-no-key / css_error) is owned by `Disc::ensure_decryptable[_keys]` and
    // tested in `crate::disc` — `input()` now delegates to it, so the matrix is
    // asserted once at the source of truth rather than re-tested here.

    // ── input()/output() routing + validation ─────────────────────────────

    // Box<dyn Stream> is not Debug, so unwrap_err() won't compile. These
    // helpers extract the io::ErrorKind from the Err arm (and panic on Ok).
    fn input_err_kind(url: &str) -> std::io::ErrorKind {
        match input(url, &Default::default()) {
            Ok(_) => panic!("expected input({url}) to error"),
            Err(e) => e.kind(),
        }
    }
    fn output_err_kind(url: &str, t: &DiscTitle) -> std::io::ErrorKind {
        match output(url, t, None) {
            Ok(_) => panic!("expected output({url}) to error"),
            Err(e) => e.kind(),
        }
    }

    // ── fvi:// provenance ─────────────────────────────────────────────────

    /// Tiny unique temp dir helper (avoids a dev-dependency on `tempfile`).
    fn fvi_tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("fmkv_resolve_fvi_{}_{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// A minimal single-video-stream title, enough for `FviSink` to build a
    /// header row.
    fn fvi_title() -> DiscTitle {
        use crate::disc::{
            Codec, ColorSpace, FrameRate, HdrFormat, Resolution, Stream as DiscStream, VideoStream,
        };
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            frame_rate: FrameRate::F29_97,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Smpte170m,
            display_aspect: Some((16, 9)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })];
        t.content_format = ContentFormat::MpegPs;
        t
    }

    /// Read the header row (line 1) of an FVI file as JSON.
    fn fvi_header(path: &std::path::Path) -> serde_json::Value {
        let text = std::fs::read_to_string(path).unwrap();
        serde_json::from_str(text.lines().next().unwrap()).unwrap()
    }

    /// `fvi://` must record the SOURCE in `source.{path,medium,title}`. It used
    /// to pass the DESTINATION path as `FviSink::create`'s `source_path` (and
    /// default the medium/title), so every index claimed to be its own source —
    /// `docs/FVI_FORMAT.md` §6.2 defines `source` as describing the input.
    #[test]
    fn fvi_output_records_the_source_not_the_destination() {
        let dir = fvi_tempdir();
        let dst = dir.join("out.fvi");
        let src = SourceInfo {
            medium: Medium::Iso,
            path: "iso://m.iso".into(),
            title: 1,
            ..SourceInfo::default()
        };
        let mut sink = output(
            &format!("fvi://{}", dst.display()),
            &fvi_title(),
            Some(&src),
        )
        .expect("fvi sink");
        sink.finish().unwrap();

        let hdr = fvi_header(&dst);
        assert_eq!(hdr["source"]["path"], "iso://m.iso");
        assert_eq!(hdr["source"]["medium"], "iso");
        assert_eq!(hdr["source"]["title"], 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The property the destination-as-source defect broke: two runs indexing
    /// the SAME source must produce byte-identical output regardless of where
    /// they write. Previously the header embedded the destination path, so the
    /// two files differed (and differed in length when the paths differed in
    /// length) purely from where they landed.
    #[test]
    fn fvi_output_is_reproducible_across_destination_paths() {
        let dir = fvi_tempdir();
        let src = SourceInfo {
            medium: Medium::Iso,
            path: "iso://m.iso".into(),
            title: 1,
            ..SourceInfo::default()
        };
        // Deliberately different lengths — the parity run's byte-count delta
        // tracked exactly the destination path-length difference.
        let a = dir.join("a.fvi");
        let b = dir.join("a-much-longer-destination-name.fvi");
        for dst in [&a, &b] {
            let mut sink = output(
                &format!("fvi://{}", dst.display()),
                &fvi_title(),
                Some(&src),
            )
            .unwrap();
            sink.finish().unwrap();
        }
        assert_eq!(
            std::fs::read(&a).unwrap(),
            std::fs::read(&b).unwrap(),
            "same source, different destinations must produce identical bytes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No provenance to declare (`None`) must not fabricate one: the header
    /// carries the neutral `SourceInfo` defaults — an empty path, `file`,
    /// title 0 — never the destination it happens to be writing to.
    #[test]
    fn fvi_output_without_provenance_emits_no_path() {
        let dir = fvi_tempdir();
        let dst = dir.join("bare.fvi");
        let mut sink = output(&format!("fvi://{}", dst.display()), &fvi_title(), None).unwrap();
        sink.finish().unwrap();

        let hdr = fvi_header(&dst);
        assert_eq!(hdr["source"]["path"], "");
        assert_eq!(hdr["source"]["medium"], "file");
        assert_eq!(hdr["source"]["title"], 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The resolver doc table marks disc:// as input-only via the
    /// `Drive::open` path — input("disc://") must surface DiscUrlNotDirect
    /// (E9009 → Unsupported), never attempt to open a stream.
    #[test]
    fn input_disc_url_is_not_direct() {
        assert_eq!(input_err_kind("disc://"), std::io::ErrorKind::Unsupported);
    }

    /// null:// is write-only per the table — input() must reject it with
    /// StreamWriteOnly (E9001 → Unsupported), not hand back a dead reader.
    #[test]
    fn input_null_url_is_write_only() {
        assert_eq!(input_err_kind("null://"), std::io::ErrorKind::Unsupported);
    }

    /// An unrecognized scheme on input() must surface StreamUrlInvalid
    /// (E9002 → InvalidInput), carrying the raw URL — never silently succeed.
    #[test]
    fn input_unknown_url_is_invalid() {
        assert_eq!(
            input_err_kind("ftp://host/x"),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// iso:// with an empty path must fail validate_file_path with
    /// StreamUrlMissingPath (E9003 → InvalidInput) before any File::open.
    #[test]
    fn input_iso_empty_path_missing_path_error() {
        assert_eq!(input_err_kind("iso://"), std::io::ErrorKind::InvalidInput);
    }

    /// disc:// and iso:// are input-only sources — output() to either must
    /// return StreamReadOnly (E9000 → Unsupported).
    #[test]
    fn output_disc_and_iso_are_read_only() {
        let t = DiscTitle::empty();
        assert_eq!(
            output_err_kind("disc://", &t),
            std::io::ErrorKind::Unsupported
        );
        assert_eq!(
            output_err_kind("iso://x.iso", &t),
            std::io::ErrorKind::Unsupported
        );
    }

    /// output() to an unknown scheme must surface StreamUrlInvalid
    /// (E9002 → InvalidInput).
    #[test]
    fn output_unknown_url_is_invalid() {
        let t = DiscTitle::empty();
        assert_eq!(
            output_err_kind("gopher://x", &t),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// `dir://PATH/` parses to `StreamUrl::Dir` with the raw remainder as the
    /// path. Unlike the other directory schemes it IS an image-level source
    /// (1.6.1): `crate::dirimage` synthesizes a UDF volume over the folder, so
    /// `is_disc_source()` — "has a filesystem to scan" — is true for it and
    /// false for the write-only `demux://` / `fvi://` directory sinks.
    #[test]
    fn parse_dir_url_is_an_image_source_unlike_the_directory_sinks() {
        match parse_url("dir://out/movie/") {
            StreamUrl::Dir { path } => {
                assert_eq!(path, PathBuf::from("out/movie/"));
            }
            other => panic!("dir:// must parse to Dir, got {other:?}"),
        }
        assert_eq!(parse_url("dir://x").scheme(), "dir");
        assert_eq!(parse_url("dir://x/y").path_str(), "x/y");
        assert_eq!(parse_url("demux://out/movie/").path_str(), "out/movie/");
        assert_eq!(parse_url("demux://x").scheme(), "demux");
        assert!(
            !parse_url("demux://x").is_disc_source(),
            "demux:// is a sink, never a disc source"
        );
        assert!(
            parse_url("dir://x").is_disc_source(),
            "dir:// carries a filesystem, so selection flags and image sinks apply"
        );
        // fvi:// parses to Fvi with the raw remainder as the path, and is a
        // sink (never a disc source) — parallel to the demux:// coverage above.
        match parse_url("fvi://out/movie.fvi") {
            StreamUrl::Fvi { path } => {
                assert_eq!(path, PathBuf::from("out/movie.fvi"));
            }
            other => panic!("fvi:// must parse to Fvi, got {other:?}"),
        }
        assert_eq!(parse_url("fvi://x").scheme(), "fvi");
        assert_eq!(parse_url("fvi://x/y.fvi").path_str(), "x/y.fvi");
        assert!(
            !parse_url("fvi://x").is_disc_source(),
            "fvi:// is a sink, never a disc source"
        );
    }

    /// `fvi://` is output-only: `input()` rejects it with StreamWriteOnly
    /// (E9001 → Unsupported), mirroring `null://` / `demux://`.
    #[test]
    fn input_fvi_url_is_write_only() {
        assert_eq!(
            input_err_kind("fvi://out/movie.fvi"),
            std::io::ErrorKind::Unsupported
        );
    }

    /// `dir://` is never a PES SINK — it writes raw decrypted files, not muxed
    /// frames, so `output()` still rejects it (StreamReadOnly → Unsupported)
    /// and the CLI routes a `dir://` dest to `Disc::extract_tree`.
    ///
    /// As a SOURCE it is no longer rejected out of hand: it is an image source,
    /// and a missing folder now fails as a missing folder (NotFound) rather
    /// than as "this scheme cannot be read".
    #[test]
    fn dir_url_is_an_input_but_never_a_pes_sink() {
        assert_eq!(
            input_err_kind("dir://definitely/not/here/"),
            std::io::ErrorKind::NotFound,
            "a dir:// source that does not exist must report a missing path"
        );
        let t = DiscTitle::empty();
        assert_eq!(
            output_err_kind("dir://out/", &t),
            std::io::ErrorKind::Unsupported
        );
    }

    /// output() to network:// with no port must fail validation
    /// (StreamUrlMissingPort, E9004 → InvalidInput) before any TcpStream.
    #[test]
    fn output_network_missing_port_invalid() {
        let t = DiscTitle::empty();
        assert_eq!(
            output_err_kind("network://127.0.0.1", &t),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// mkv:// with an empty path must fail validate_file_path
    /// (StreamUrlMissingPath) on the output side, before WritebackFile.
    #[test]
    fn output_mkv_empty_path_missing_path_error() {
        let t = DiscTitle::empty();
        assert_eq!(
            output_err_kind("mkv://", &t),
            std::io::ErrorKind::InvalidInput
        );
    }

    // ── build_demux_state: parser/PID table + demuxer selection ────────────

    fn aac_audio_title(pid: u16) -> DiscTitle {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Audio(AudioStream {
            pid,
            codec: Codec::Aac, // → all-keyframe PassthroughParser (1 PES = 1 frame)
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        t
    }

    /// BdTs format must build a TsDemuxer (Some(ts), None(ps)) when there is
    /// at least one PID, and one parser + pid_to_track entry per stream
    /// keyed by the stream's own PID. (Mis-keying here is exactly the class
    /// of bug that mis-routes PES into the wrong codec parser.)
    #[test]
    fn build_demux_state_bdts_builds_ts_demuxer_and_pid_table() {
        let t = aac_audio_title(0x1100);
        let (parsers, pid_to_track, ts, ps) = build_demux_state(&t, ContentFormat::BdTs);
        assert_eq!(parsers.len(), 1);
        assert_eq!(parsers[0].0, 0x1100, "parser keyed by the stream PID");
        assert_eq!(pid_to_track, vec![(0x1100u16, 0usize)]);
        assert!(ts.is_some(), "BdTs → TsDemuxer");
        assert!(ps.is_none());
    }

    /// MpegPs format must build a PsDemuxer (None(ts), Some(ps)) regardless
    /// of PIDs — DVD program streams demux via the PS path.
    #[test]
    fn build_demux_state_mpegps_builds_ps_demuxer() {
        let t = aac_audio_title(0xBD80);
        let (_parsers, _p2t, ts, ps) = build_demux_state(&t, ContentFormat::MpegPs);
        assert!(ts.is_none());
        assert!(ps.is_some(), "MpegPs → PsDemuxer");
    }

    /// An empty BdTs title (no streams) must NOT construct a TsDemuxer —
    /// `TsDemuxer::new(&[])` is pointless, and the builder special-cases
    /// empty PIDs to (None, None). pid_to_track/parsers also empty.
    #[test]
    fn build_demux_state_bdts_empty_streams_builds_no_demuxer() {
        let t = DiscTitle::empty();
        let (parsers, pid_to_track, ts, ps) = build_demux_state(&t, ContentFormat::BdTs);
        assert!(parsers.is_empty());
        assert!(pid_to_track.is_empty());
        assert!(ts.is_none(), "no PIDs → no TsDemuxer");
        assert!(ps.is_none());
    }

    // ── Fix 1: halt threading into live-drive key resolution ───────────────

    /// A counting `SectorSource` over zeros. `touched_extent` flags whether any
    /// read landed in the title's extent region (LBA >= 1000); the UDF probe only
    /// reads near LBA 256 (small `capacity`), so a hit there means the expensive
    /// per-extent `sample_units` loop ran.
    struct HaltCountSource {
        reads: u32,
        touched_extent: bool,
    }
    impl SectorSource for HaltCountSource {
        fn capacity_sectors(&self) -> u32 {
            512 // keeps the UDF secondary anchor well below the extent region
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.reads += 1;
            if lba >= 1000 {
                self.touched_extent = true;
            }
            let want = count as usize * 2048;
            buf[..want].fill(0);
            Ok(want)
        }
    }

    /// `resolve_mux_key_map` on the multi-CPS live path must honor a pre-cancelled
    /// halt PROMPTLY — `Err(Halted)` at the first extent boundary, before sampling
    /// any extent's ciphertext — rather than reading through every extent. This is
    /// the round-2 Fix 1 guard: the resolve chain runs on the LIVE drive (each
    /// `read_sectors` can stall to the SCSI recovery timeout), so an operator Stop
    /// during key resolution must interrupt it.
    ///
    /// Mutation: dropping the `halt.is_some_and(...) → Err(Halted)` check in the
    /// multi-CPS extent loop makes the resolve run the sampling reads and return
    /// `Ok(map)` (zeros sample to no encrypted units → carry key 0), so
    /// `expect_err` fails AND `touched_extent` flips true.
    #[test]
    fn resolve_mux_key_map_honors_pre_cancelled_halt() {
        use crate::halt::Halt;
        let mut title = DiscTitle::empty();
        title.extents = vec![
            Extent {
                start_lba: 1000,
                sector_count: 300,
            },
            Extent {
                start_lba: 5000,
                sector_count: 300,
            },
        ];
        // Multi-CPS (pool_len = 2) → the extent-sampling loop is the resolve path
        // (pool_len == 1 would short-circuit to content_map before any read).
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0x11u8; 16]), (1, [0x22u8; 16])],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut reader = HaltCountSource {
            reads: 0,
            touched_extent: false,
        };
        let halt = Halt::new();
        halt.cancel(); // pre-cancelled: the very first extent boundary must bail

        let err = super::resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            Some(&halt),
        )
        .expect_err("a pre-cancelled halt must abort key resolution");
        assert!(crate::error::is_halt(&err), "expected Halted, got: {err}");
        assert!(
            !reader.touched_extent,
            "extent sampling must be skipped on a pre-cancelled halt (a read landed \
             in the extent region — the halt check was not honored)"
        );
    }

    /// A `None` halt (no token) must NOT abort — the resolve runs to completion.
    /// Guards against a mutation that treats `None` as cancelled.
    #[test]
    fn resolve_mux_key_map_none_halt_does_not_abort() {
        let mut title = DiscTitle::empty();
        title.extents = vec![Extent {
            start_lba: 1000,
            sector_count: 300,
        }];
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0x11u8; 16]), (1, [0x22u8; 16])],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut reader = HaltCountSource {
            reads: 0,
            touched_extent: false,
        };
        // No halt token → resolution proceeds and samples the extent (zeros → no
        // encrypted unit → carries key 0), returning Ok.
        let map = super::resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
        )
        .expect("no halt → resolution completes");
        assert!(reader.touched_extent, "the extent WAS sampled with no halt");
        assert!(!map.ranges().is_empty(), "a map is produced for the extent");
    }

    // ── build_iso_pipeline: end-to-end highway wiring ──────────────────────

    /// An in-memory SectorSource that serves a fixed byte image. Reads beyond
    /// the image return zero-filled sectors (the prefetcher only reads within
    /// the title's extents, so this is never hit in these tests).
    struct MemSource {
        data: Vec<u8>,
    }
    impl SectorSource for MemSource {
        fn capacity_sectors(&self) -> u32 {
            (self.data.len() / 2048) as u32
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let start = lba as usize * 2048;
            let want = count as usize * 2048;
            for (i, b) in buf[..want].iter_mut().enumerate() {
                *b = self.data.get(start + i).copied().unwrap_or(0);
            }
            Ok(want)
        }
    }

    /// Build a 192-byte BD-TS data packet on `pid` carrying `payload` as the
    /// TS payload (payload-only adaptation). Layout: 4-byte TP_extra_header
    /// (zeros) + 188-byte TS packet (sync 0x47, PID, PUSI, AFC=0b01).
    /// Mirrors the BD-TS framing in ts.rs.
    fn bdts_data_packet(pid: u16, pusi: bool, payload: &[u8]) -> [u8; 192] {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47; // sync byte
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40; // PUSI
        }
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10; // adaptation_field_control = 0b01 (payload only)
        let room = 184; // 188 - 4-byte TS header
        let n = payload.len().min(room);
        pkt[8..8 + n].copy_from_slice(&payload[..n]);
        pkt
    }

    /// A complete audio PES (stream_id 0xC0) with no PTS, carrying `es` as the
    /// elementary-stream payload. Layout per ISO 13818-1: 00 00 01 C0
    /// [len:2] [0x80 flags1] [0x00 flags2] [0x00 header_data_len] [es...].
    fn audio_pes(es: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, 0xC0];
        let len = (3 + es.len()) as u16; // flags(2)+hdl(1)+es
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(&[0x80, 0x00, 0x00]);
        v.extend_from_slice(es);
        v
    }

    /// Empty extents → the producer thread exits immediately, the demux
    /// thread sees a clean channel close and emits the Eof sentinel, and the
    /// PipelinedPesStream returns Ok(None) on the first read. The highway must
    /// terminate cleanly (no panic, no hang) when there is nothing to read.
    #[test]
    fn build_iso_pipeline_empty_extents_clean_eof() {
        let title = aac_audio_title(0x1100); // extents empty by default
        let mut stream = build_iso_pipeline(
            MemSource { data: Vec::new() },
            title,
            DecryptKeys::None,
            8192,
            ContentFormat::BdTs,
            false,
            None,
            None,
            None,
        )
        .expect("pipeline builds");
        let first = stream.read().expect("read must not error on clean EOF");
        assert!(
            first.is_none(),
            "no extents → immediate clean end-of-stream"
        );
        // Idempotent: a second read past EOF is still Ok(None), never an error.
        assert!(stream.read().unwrap().is_none());
    }

    /// End-to-end: one BD-TS packet carrying a complete audio PES flows
    /// read → decrypt(passthrough) → TS demux → codec parse → one PesFrame.
    /// Proves the full highway wiring delivers the ES payload intact and
    /// reaches a clean EOF afterward (never silently truncating the frame).
    #[test]
    fn build_iso_pipeline_delivers_one_frame_then_eof() {
        let es = [0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22];
        let pes = audio_pes(&es);
        let pkt = bdts_data_packet(0x1100, true, &pes);
        // One 2048-byte sector holding the 192-byte packet (rest zero — the
        // demuxer skips non-sync packets). Extent = 3 sectors (one AACS unit,
        // the prefetcher's alignment requirement).
        let mut data = vec![0u8; 3 * 2048];
        data[..192].copy_from_slice(&pkt);

        let mut title = aac_audio_title(0x1100);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];

        let mut stream = build_iso_pipeline(
            MemSource { data },
            title,
            DecryptKeys::None,
            8192,
            ContentFormat::BdTs,
            false,
            None,
            None,
            None,
        )
        .expect("pipeline builds");

        let frame = stream
            .read()
            .expect("read ok")
            .expect("one frame emitted from the single PES");
        // PassthroughParser routes the audio stream (PID 0x1100) to track 0.
        assert_eq!(frame.track, 0);
        // The TS PesAssembler delivers every payload byte AFTER the 9-byte PES
        // header to the end of the 184-byte TS payload region (the bounded
        // PES_packet_length is not used to trim within a single packet — the
        // PES is closed by the next PUSI or by flush at EOF). So the frame is
        // the ES bytes followed by the packet's zero padding: total = 184 - 9.
        assert_eq!(
            frame.data.len(),
            184 - 9,
            "frame spans the full TS payload after the PES header"
        );
        // Truncation guard: the ES bytes lead the frame, in order, unaltered —
        // the highway must never drop or reorder the elementary-stream prefix.
        assert_eq!(
            &frame.data[..es.len()],
            &es[..],
            "ES payload prefix delivered intact and in order"
        );
        assert!(
            frame.data[es.len()..].iter().all(|&b| b == 0),
            "remainder is the packet's zero padding, not foreign data"
        );
        // After the single frame the stream reaches a clean EOF.
        assert!(
            stream.read().unwrap().is_none(),
            "clean EOF after the frame"
        );
    }

    /// End-to-end proof of stream selection: a title declaring TWO audio PIDs,
    /// pruned to one via `StreamSelection::apply` BEFORE `build_iso_pipeline`,
    /// must never surface a frame from the excluded PID. The demuxer is built
    /// from the pruned `title.streams`, so the excluded PID is untracked and its
    /// packets are skipped — track headers and frames both follow the pruned
    /// list, which is the whole point of the selection seam.
    #[test]
    fn build_iso_pipeline_pruned_title_drops_unselected_pid_frames() {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        use crate::mux::select::{PidFilter, StreamSelection};

        let es_keep = [0xDE, 0xAD, 0xBE, 0xEF];
        let es_drop = [0x99, 0x88, 0x77, 0x66];
        let pkt_keep = bdts_data_packet(0x1100, true, &audio_pes(&es_keep));
        let pkt_drop = bdts_data_packet(0x1101, true, &audio_pes(&es_drop));
        // Both 192-byte packets in one 3-sector extent (offsets 0 and 192).
        let mut data = vec![0u8; 3 * 2048];
        data[..192].copy_from_slice(&pkt_keep);
        data[192..384].copy_from_slice(&pkt_drop);

        // Title declares BOTH audio streams (eng 0x1100, spa 0x1101).
        let mut title = aac_audio_title(0x1100);
        title.streams.push(Stream::Audio(AudioStream {
            pid: 0x1101,
            codec: Codec::Aac,
            channels: AudioChannels::Stereo,
            language: "spa".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];

        // Prune to keep only PID 0x1100 (the eng audio) — exactly what a
        // `-a eng` selection resolves to.
        let sel = StreamSelection {
            audio: PidFilter::Only(vec![0x1100]),
            subtitle: PidFilter::All,
        };
        sel.apply(&mut title).unwrap();
        assert_eq!(
            title.streams.len(),
            1,
            "only the kept audio survives pruning"
        );

        let mut stream = build_iso_pipeline(
            MemSource { data },
            title,
            DecryptKeys::None,
            8192,
            ContentFormat::BdTs,
            false,
            None,
            None,
            None,
        )
        .expect("pipeline builds");

        // Exactly ONE frame — the kept PID's — reaches us; the 0x1101 packet was
        // never tracked by the demuxer, so it produced no frame.
        let frame = stream
            .read()
            .expect("read ok")
            .expect("one frame from 0x1100");
        assert_eq!(frame.track, 0, "the single retained stream is track 0");
        assert_eq!(
            &frame.data[..es_keep.len()],
            &es_keep[..],
            "the KEPT PID's ES bytes"
        );
        assert!(
            stream.read().unwrap().is_none(),
            "clean EOF — the excluded 0x1101 packet never surfaced as a frame"
        );
        // The muxed stream info advertises exactly the one retained audio stream.
        assert_eq!(stream.info().streams.len(), 1);
    }

    /// build_iso_pipeline with batch_sectors = 0 must fail fast (the
    /// prefetcher rejects a zero batch as a programming error — a zero batch
    /// would spin the producer forever). Surfaced as an io error, not a hang.
    #[test]
    fn build_iso_pipeline_zero_batch_rejected() {
        let title = aac_audio_title(0x1100);
        let res = build_iso_pipeline(
            MemSource { data: Vec::new() },
            title,
            DecryptKeys::None,
            0,
            ContentFormat::BdTs,
            false,
            None,
            None,
            None,
        );
        assert!(res.is_err(), "zero batch_sectors must be rejected");
    }

    /// REGRESSION (autorip production corruption): `build_iso_pipeline` for a DVD
    /// (MPEG-PS) with `None` keys — what autorip's mux passes on a detection-miss
    /// DVD (`disc.decrypt_keys()` == None) — must resolve the CSS key from the
    /// reader itself. A scrambled-but-uncrackable title must HARD-FAIL, never
    /// build a passthrough pipeline that muxes the scrambled sectors as corrupt
    /// video. Before this fix, autorip handed None straight through and the mux
    /// wrote garbage at exit 0.
    #[test]
    fn build_iso_pipeline_dvd_none_keys_scrambled_hard_fails() {
        // One CSS-scrambled, crib-less (uncrackable) MPEG-PS sector.
        let key = [0x11u8, 0x22, 0x33, 0x44, 0x55];
        let mut sec = vec![0u8; 2048];
        sec[0..4].copy_from_slice(&crate::css::PACK_START);
        for (i, b) in sec.iter_mut().enumerate().take(0x80).skip(4) {
            *b = (i as u8).wrapping_mul(7).wrapping_add(1); // non-repeating → no crib
        }
        sec[0x14] = 0x10; // scramble flag
        for (i, b) in sec.iter_mut().enumerate().skip(0x80) {
            *b = (i as u8) ^ 0x3C;
        }
        crate::css::lfsr::scramble_sector(&key, &mut sec);

        let mut title = aac_audio_title(0x1100);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 1,
        }];

        let res = build_iso_pipeline(
            MemSource { data: sec },
            title,
            DecryptKeys::None,
            8192,
            ContentFormat::MpegPs,
            false,
            None,
            None,
            None,
        );
        assert!(
            res.is_err(),
            "a scrambled DVD title with no key must hard-fail, not build a scrambled-passthrough pipeline"
        );
    }

    // ── content_map: single-CPS positive range building ────────────────────

    /// `content_map(title, idx)` keys every single-CPS UHD disc (the common
    /// case): each content extent → one `[start_lba, start_lba+sector_count)`
    /// range at `idx`, phase `All`. Assert the exact ranges — an off-by-one on
    /// the end (or a wrong idx / phase) must flip this test to FAIL.
    #[test]
    fn content_map_builds_exact_ranges_from_extents() {
        use crate::decrypt::Phase;
        let mut t = DiscTitle::empty();
        t.extents = vec![
            Extent {
                start_lba: 100,
                sector_count: 50,
            },
            Extent {
                start_lba: 1000,
                sector_count: 200,
            },
        ];
        let map = super::content_map(&t, 3);
        // end = start + count (exclusive), idx = 3, phase = All, for each extent.
        assert_eq!(
            map.ranges(),
            &[
                (100u32, 150u32, 3usize, Phase::All),
                (1000u32, 1200u32, 3usize, Phase::All),
            ],
            "each extent maps to [start, start+count) at the given idx"
        );
        // Spot-check the derived lookups: inside → idx 3, the exclusive end and
        // the inter-extent gap → no key (pass-through).
        assert_eq!(map.key_idx_for(100), Some(3), "range start is inclusive");
        assert_eq!(map.key_idx_for(149), Some(3), "last sector of extent 0");
        assert_eq!(map.key_idx_for(150), None, "extent end is exclusive");
        assert_eq!(map.key_idx_for(500), None, "gap between extents → no key");
        assert_eq!(map.key_idx_for(1199), Some(3), "last sector of extent 1");
    }

    /// A single-extent title still produces exactly one range with the correct
    /// end (`saturating_add`), and a `sector_count` that would overflow u32
    /// saturates rather than wrapping past `u32::MAX`.
    #[test]
    fn content_map_single_extent_end_saturates() {
        use crate::decrypt::Phase;
        let mut t = DiscTitle::empty();
        t.extents = vec![Extent {
            start_lba: u32::MAX - 10,
            sector_count: 100, // (MAX-10)+100 would overflow → saturate to MAX
        }];
        let map = super::content_map(&t, 0);
        assert_eq!(
            map.ranges(),
            &[(u32::MAX - 10, u32::MAX, 0usize, Phase::All)],
            "range end saturates at u32::MAX, no wrap"
        );
    }

    // ── resolve_fmts_key_map decision helpers (behaviors flagged by audit) ──

    /// BEHAVIOR 1 — segment filter (`resolve_fmts_key_map` line ~800). A segment
    /// whose clip-byte start (`start_spn * 192`) maps inside the title's extents is
    /// kept; one whose start is past the clip is dropped; all-outside → empty (the
    /// resolver then returns `Ok(None)` and the base-UK path applies).
    #[test]
    fn filter_addressable_segments_keeps_only_in_title_segments() {
        use crate::aacs::segment::Segment;
        // One extent covering clip bytes [0, 60*2048) = [0, 122880).
        let extents = vec![Extent {
            start_lba: 500,
            sector_count: 60,
        }];
        // start_spn 100 → clip byte 19200 < 122880 → maps to an LBA → KEEP.
        let inside = Segment {
            index: 1,
            start_spn: 100,
            end_spn: 199,
        };
        // start_spn 1000 → clip byte 192000 >= 122880 → clip_byte_to_lba None → DROP.
        let outside = Segment {
            index: 2,
            start_spn: 1000,
            end_spn: 1099,
        };
        let kept = super::filter_addressable_segments(vec![inside, outside], &extents);
        assert_eq!(kept, vec![inside], "only the in-title segment survives");
        // All-outside → empty; `resolve_fmts_key_map` maps this to Ok(None).
        assert!(
            super::filter_addressable_segments(vec![outside], &extents).is_empty(),
            "no addressable segment → empty (→ resolver Ok(None))"
        );
        // Boundary: a segment whose start is the LAST clip byte still maps (Some);
        // one exactly at the clip end (122880) does not.
        let at_last = Segment {
            index: 3,
            start_spn: (122_879 / 192) as u32, // 639 → byte 122688 < 122880
            end_spn: 700,
        };
        let at_end = Segment {
            index: 4,
            start_spn: (122_880 / 192) as u32, // 640 → byte 122880 == clip end → None
            end_spn: 700,
        };
        assert_eq!(
            super::filter_addressable_segments(vec![at_last, at_end], &extents),
            vec![at_last],
            "start inside the clip is kept; start at/after the clip end is dropped"
        );
    }

    /// BEHAVIOR 2 — phase-tie default (`resolve_fmts_key_map` line ~936). All four
    /// arms of the even/odd clean-count decision.
    #[test]
    fn resolve_tie_phase_covers_all_arms() {
        use crate::decrypt::Phase;
        // Non-tie: the clean half is the index's real variant.
        assert_eq!(
            super::resolve_tie_phase(5, 2).unwrap(),
            Phase::Even,
            "even majority → Even"
        );
        assert_eq!(
            super::resolve_tie_phase(2, 5).unwrap(),
            Phase::Odd,
            "odd majority → Odd"
        );
        // Padding tie (both halves clean, > 0): parity immaterial → default Even.
        assert_eq!(
            super::resolve_tie_phase(3, 3).unwrap(),
            Phase::Even,
            "even == odd > 0 → default Even"
        );
        assert_eq!(super::resolve_tie_phase(1, 1).unwrap(), Phase::Even);
        // Neither half clean (even == odd == 0): fail loud with FmtsKeyMissing.
        let err = super::resolve_tie_phase(0, 0).unwrap_err();
        let expected = std::io::Error::from(crate::error::Error::FmtsKeyMissing).to_string();
        assert_eq!(
            err.to_string(),
            expected,
            "even == odd == 0 → FmtsKeyMissing"
        );
    }

    /// Assert `forensic` + `fills` together cover every LBA of every extent EXACTLY
    /// once — no gap (a hole would pass ciphertext through as clear) and no overlap
    /// (two keys over one LBA). This is the load-bearing invariant of the gap-fill.
    fn assert_gapless(
        extents: &[Extent],
        forensic: &[(u32, u32, usize, crate::decrypt::Phase)],
        fills: &[(u32, u32, usize, crate::decrypt::Phase)],
    ) {
        let mut spans: Vec<(u32, u32)> = forensic.iter().map(|&(s, e, _, _)| (s, e)).collect();
        spans.extend(fills.iter().map(|&(s, e, _, _)| (s, e)));
        spans.sort_unstable();
        for w in spans.windows(2) {
            assert!(w[0].1 <= w[1].0, "spans overlap: {:?} vs {:?}", w[0], w[1]);
        }
        for ext in extents {
            let end = ext.start_lba + ext.sector_count;
            for lba in ext.start_lba..end {
                let covering = spans.iter().filter(|&&(s, e)| lba >= s && lba < e).count();
                assert_eq!(
                    covering, 1,
                    "LBA {lba} covered {covering}× (want exactly 1)"
                );
            }
        }
    }

    /// BEHAVIOR 3 — gap-fill range arithmetic (`resolve_fmts_key_map` line ~1005).
    /// Exhaustive: no segments, mid-extent, at-start, at-end, adjacent segments,
    /// and multi-extent. Each asserts the EXACT fills AND gaplessness over every
    /// extent — an off-by-one that leaves a hole flips this to FAIL.
    #[test]
    fn fill_base_key_gaps_is_gapless_over_every_extent() {
        use crate::decrypt::Phase::{All, Even, Odd};
        let base = 0usize;

        // No segments → the whole extent is base key.
        let ext = vec![Extent {
            start_lba: 100,
            sector_count: 60,
        }];
        let forensic: Vec<(u32, u32, usize, crate::decrypt::Phase)> = vec![];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(fills, vec![(100, 160, base, All)], "no segments → all base");
        assert_gapless(&ext, &forensic, &fills);

        // One segment mid-extent → base | forensic | base, gapless.
        let forensic = vec![(120, 130, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 120, base, All), (130, 160, base, All)],
            "mid-extent segment → leading + trailing base"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Segment at extent START → only a trailing base fill (no zero-length lead).
        let forensic = vec![(100, 130, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(130, 160, base, All)],
            "segment at start → no leading base, one trailing"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Segment at extent END → only a leading base fill (no zero-length trail).
        let forensic = vec![(130, 160, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 130, base, All)],
            "segment at end → one leading base, no trailing"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Whole extent is one segment → no base fill at all, still gapless.
        let forensic = vec![(100, 160, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert!(
            fills.is_empty(),
            "segment spans whole extent → no base fill"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Adjacent segments (touching, no gap between) → NO zero-length base range
        // between them (guards the `cs > cur` off-by-one).
        let forensic = vec![(110, 120, 5, Even), (120, 130, 6, Odd)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 110, base, All), (130, 160, base, All)],
            "adjacent segments → no zero-length fill between them"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Multi-extent: a segment mid-first-extent and one at the start of the
        // second. Fills are per-extent and the union is gapless across both.
        let exts = vec![
            Extent {
                start_lba: 100,
                sector_count: 60,
            }, // [100, 160)
            Extent {
                start_lba: 1000,
                sector_count: 40,
            }, // [1000, 1040)
        ];
        let forensic = vec![(120, 130, 5, Even), (1000, 1010, 7, Odd)];
        let fills = super::fill_base_key_gaps(&exts, &forensic, base);
        assert_eq!(
            fills,
            vec![
                (100, 120, base, All),
                (130, 160, base, All),
                (1010, 1040, base, All),
            ],
            "each extent filled independently"
        );
        assert_gapless(&exts, &forensic, &fills);
    }

    // ── Fix 1: FMTS phase-probe read-fault vs wrong-key distinction ─────────

    /// Build a 6144-byte aligned unit of CLEAN MPEG-TS (sync `0x47` + non-zero
    /// payload in packets 1.., packet 0 is the clear seed) then AACS-encrypt it
    /// under `key`. Decrypting under the SAME key restores clean TS (`is_clean` →
    /// true); decrypting under any other key yields garbage.
    fn encrypted_clean_unit(key: &[u8; 16]) -> Vec<u8> {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 0;
        while off + 192 <= ALIGNED_UNIT_LEN {
            u[off + 4] = 0x47; // TS sync at the BD-TS packet stride
            for b in &mut u[off + 5..off + 192] {
                *b = 0xAB; // non-zero payload so is_clean counts it as content
            }
            off += 192;
        }
        // Flag encrypted BEFORE encrypting: bytes 0..16 are the key seed.
        u[0] |= 0xC0;
        assert!(
            crate::aacs::content::encrypt_unit(&mut u, key),
            "a full-length unit must encrypt"
        );
        u
    }

    fn a_segment(index: u16) -> crate::aacs::segment::Segment {
        crate::aacs::segment::Segment {
            index,
            start_spn: 0,
            end_spn: 100,
        }
    }

    /// A probe whose EVERY read faults (`read` returns `None`) must classify as
    /// [`IndexProbe::ReadFault`], NOT [`IndexProbe::WrongKey`] — a transient
    /// live-drive read fault while probing must not be read as a missing key (which
    /// the caller turns into a rip-aborting `FmtsKeyMissing`).
    ///
    /// Mutation: reverting to the no-fallback single-segment probe (i.e. treating
    /// even==odd==0 as unconditional `FmtsKeyMissing` regardless of whether any read
    /// succeeded) makes this return `WrongKey` → the assert fails.
    #[test]
    fn probe_index_phase_all_faults_is_read_fault_not_wrong_key() {
        let segs = vec![a_segment(1)];
        let key = [0x11u8; 16];
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            |_seg, _unit| None, // every read faults
        );
        assert_eq!(
            got,
            super::IndexProbe::ReadFault,
            "all-faulted probe is a recoverable read fault, never a wrong key"
        );
    }

    /// Reads SUCCEED but decrypt to NEITHER clean parity (ciphertext under a key we
    /// do NOT hold) → [`IndexProbe::WrongKey`]. This is the genuine-missing-key path
    /// the caller MUST keep as a hard `FmtsKeyMissing`.
    #[test]
    fn probe_index_phase_reads_succeed_but_no_clean_phase_is_wrong_key() {
        let segs = vec![a_segment(1)];
        let cipher = encrypted_clean_unit(&[0xAAu8; 16]); // encrypted under key A
        let probe_key = [0xBBu8; 16]; // ... probed under the WRONG key B
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &probe_key,
            |_seg, _unit| Some(cipher.clone()),
        );
        assert_eq!(
            got,
            super::IndexProbe::WrongKey,
            "reads that decrypt to no clean parity under the probed key are a wrong key"
        );
    }

    /// Reads succeed and the EVEN units decrypt clean under this index's key while
    /// the ODD units are (unencrypted) padding → [`IndexProbe::Phase`]`(Even)`.
    #[test]
    fn probe_index_phase_resolves_clean_even_phase() {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        use crate::decrypt::Phase;
        let segs = vec![a_segment(1)];
        let key = [0x33u8; 16];
        let even_unit = encrypted_clean_unit(&key);
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            // even unit index → clean ciphertext under `key`; odd → zero padding
            // (aacs_unit_encrypted false → not counted).
            |_seg, unit| {
                if unit % 2 == 0 {
                    Some(even_unit.clone())
                } else {
                    Some(vec![0u8; ALIGNED_UNIT_LEN])
                }
            },
        );
        assert_eq!(
            got,
            super::IndexProbe::Phase(Phase::Even),
            "clean even units + padding odd → Even phase"
        );
    }

    /// Read-fault TOLERANCE across segments: the first same-index segment faults on
    /// every read, but a SECOND same-index segment decrypts clean → the probe must
    /// fall through to it and resolve a phase (mirrors the anchor loop's multi-
    /// segment retry). A single-`.find` probe would have stopped at the faulting
    /// first segment.
    #[test]
    fn probe_index_phase_falls_through_faulting_segment_to_next() {
        use crate::decrypt::Phase;
        let mut faulting = a_segment(1);
        faulting.start_spn = 1; // distinguish the two same-index segments
        let good = a_segment(1);
        let segs = vec![faulting, good];
        let key = [0x44u8; 16];
        let clean = encrypted_clean_unit(&key);
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            // The faulting segment (start_spn == 1) reads None; the good one reads a
            // clean even unit / padding odd.
            |seg, unit| {
                if seg.start_spn == 1 {
                    None
                } else if unit % 2 == 0 {
                    Some(clean.clone())
                } else {
                    Some(vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN])
                }
            },
        );
        assert_eq!(
            got,
            super::IndexProbe::Phase(Phase::Even),
            "a faulting first segment must not block resolving from the next same-index one"
        );
    }

    // ── Fix 2: resolve_mux_key_map multi-CPS key selection (real ciphertext) ─

    /// A SectorSource that tiles a fixed 6144-byte ciphertext unit across each
    /// registered extent range (`(start_lba, end_lba, unit)`), zeros elsewhere.
    /// Every 3-sector aligned-unit read inside a range returns the same ciphertext,
    /// so `sample_units` collects real encrypted content for `pick`/fetch to run on.
    /// Low LBAs are zero, so `udf::read_filesystem` fails → the FMTS branch returns
    /// `Ok(None)` and the multi-CPS path is exercised.
    struct CipherSource {
        units: Vec<(u32, u32, Vec<u8>)>,
    }
    impl SectorSource for CipherSource {
        fn capacity_sectors(&self) -> u32 {
            1_000_000
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let want = count as usize * 2048;
            buf[..want].fill(0);
            for s in 0..count as usize {
                let cur = lba + s as u32;
                if let Some((start, _end, unit)) =
                    self.units.iter().find(|(a, b, _)| cur >= *a && cur < *b)
                {
                    // 3 sectors per aligned unit; tile the ciphertext by sector.
                    let within = ((cur - start) % 3) as usize;
                    let src = &unit[within * 2048..within * 2048 + 2048];
                    buf[s * 2048..s * 2048 + 2048].copy_from_slice(src);
                }
            }
            Ok(want)
        }
    }

    fn multi_cps_title(start_lba: u32, sectors: u32) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.extents = vec![Extent {
            start_lba,
            sector_count: sectors,
        }];
        t
    }

    /// `pick()` must select the pool index of the key that actually opens the
    /// extent's real ciphertext — index 2 here, NOT 0. Feeds units encrypted under
    /// the third pool key through the live multi-CPS path.
    ///
    /// Mutation: `pick` hard-returning `Some(0)` keys the extent to 0 → this assert
    /// (Some(2)) fails.
    #[test]
    fn resolve_mux_key_map_multi_cps_pick_selects_correct_index() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_c = [0x03u8; 16];
        let unit = encrypted_clean_unit(&key_c); // extent content opens under C (idx 2)
        let start = 1000u32;
        let sectors = 30u32; // 10 aligned units
        let mut reader = CipherSource {
            units: vec![(start, start + sectors, unit)],
        };
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b), (2, key_c)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let title = multi_cps_title(start, sectors);
        let map = super::resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
        )
        .expect("multi-CPS resolve succeeds when a held key opens the extent");
        assert_eq!(
            map.key_idx_for(start),
            Some(2),
            "the extent must be keyed to the pool index whose key opens its ciphertext"
        );
    }

    /// Fail-loud: a sample that decrypts clean under NO held key and NO fetched key
    /// (fetch = None) must surface [`Error::DecryptFailed`], never silently key the
    /// extent to a neighbour's (wrong) index.
    ///
    /// Mutation: dropping the `None => Err(DecryptFailed)` guard (e.g. falling back
    /// to `last_idx`) returns `Ok` → this `expect_err` fails.
    #[test]
    fn resolve_mux_key_map_multi_cps_fail_loud_on_absent_key() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_x = [0x09u8; 16]; // NOT in the pool, NOT fetchable (fetch None)
        let unit = encrypted_clean_unit(&key_x);
        let start = 1000u32;
        let sectors = 30u32;
        let mut reader = CipherSource {
            units: vec![(start, start + sectors, unit)],
        };
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let title = multi_cps_title(start, sectors);
        let err = super::resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
        )
        .expect_err("an extent no held/fetched key opens must fail loud, not mis-key");
        let expected = std::io::Error::from(crate::error::Error::DecryptFailed).to_string();
        assert_eq!(err.to_string(), expected, "expected DecryptFailed");
    }

    /// KeyFetch cold path: the pool is missing the extent's key, but the injected
    /// `KeyFetch::unit_keys` returns it from the failing samples → the extent
    /// resolves to the newly-appended pool index (2) and the map succeeds. Proves
    /// the on-miss fetch+re-pick branch runs end to end.
    #[test]
    fn resolve_mux_key_map_multi_cps_fetch_recovers_missing_key() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_x = [0x09u8; 16]; // absent from the pool, supplied by fetch
        let unit = encrypted_clean_unit(&key_x);
        let start = 1000u32;
        let sectors = 30u32;
        let mut reader = CipherSource {
            units: vec![(start, start + sectors, unit)],
        };
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let title = multi_cps_title(start, sectors);
        // A unit-only KeyFetch that hands back key_x for any non-empty sample batch.
        let fetch = crate::sector::KeyFetch::unit_only(std::sync::Arc::new(
            move |samples: &[Vec<u8>]| {
                if samples.is_empty() {
                    Vec::new()
                } else {
                    vec![key_x]
                }
            },
        ));
        let map = super::resolve_mux_key_map(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
        )
        .expect("the fetched key recovers the extent");
        assert_eq!(
            map.key_idx_for(start),
            Some(2),
            "the fetched key is appended at pool index 2 and keys the extent"
        );
    }

    // ── Multi-CPS extent cache: shared clips are sampled ONCE per disc ───────

    /// LBA below which a read is disc metadata, not content sampling: the FMTS
    /// branch probes the UDF anchor/metadata before concluding "not an FMTS disc",
    /// and every content extent in these tests starts at or above this.
    const CONTENT_LBA_FLOOR: u32 = 1000;

    /// [`CipherSource`] plus a counter of CONTENT reads (`lba >=
    /// CONTENT_LBA_FLOOR`) — the `sample_units` probes whose cost this cache
    /// exists to remove. Low-LBA UDF metadata probes are excluded so the counts
    /// speak only about extent sampling.
    struct CountingCipherSource {
        inner: CipherSource,
        probes: u32,
        /// Reads BELOW `CONTENT_LBA_FLOOR` — the FMTS branch's UDF walk and
        /// `IndividualSegment.tbl` load, whose per-title repetition the disc-wide
        /// `FmtsTableCache` exists to remove.
        meta: u32,
    }
    impl CountingCipherSource {
        fn new(units: Vec<(u32, u32, Vec<u8>)>) -> Self {
            Self {
                inner: CipherSource { units },
                probes: 0,
                meta: 0,
            }
        }
    }
    impl SectorSource for CountingCipherSource {
        fn capacity_sectors(&self) -> u32 {
            self.inner.capacity_sectors()
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if lba >= CONTENT_LBA_FLOOR {
                self.probes += 1;
            } else {
                self.meta += 1;
            }
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }

    /// A disc's playlists overwhelmingly share clips, and the multi-CPS path issues
    /// 8 random 6144-byte reads per extent. The SECOND title over the same extent
    /// must cost ZERO further reads and resolve the SAME index; a DIFFERENT extent
    /// must still be sampled and get its own index.
    ///
    /// Mutation: dropping the `cache.get` short-circuit re-samples → the
    /// zero-further-reads assert fails. Caching the wrong index (e.g. inserting
    /// `last_idx`) breaks the same-index asserts.
    #[test]
    fn multi_cps_shared_extent_is_served_from_cache_not_resampled() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_c = [0x03u8; 16];
        let shared_start = 1000u32; // ciphertext under key_c → pool index 2
        let other_start = 9000u32; // ciphertext under key_b → pool index 1
        let sectors = 30u32; // 10 aligned units → all 8 probes land
        let mut reader = CountingCipherSource::new(vec![
            (
                shared_start,
                shared_start + sectors,
                encrypted_clean_unit(&key_c),
            ),
            (
                other_start,
                other_start + sectors,
                encrypted_clean_unit(&key_b),
            ),
        ]);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b), (2, key_c)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut cache = super::DiscKeyCache::new();

        let title = multi_cps_title(shared_start, sectors);
        let first = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("first title resolves");
        let after_first = reader.probes;
        assert_eq!(
            after_first, 8,
            "the first title must actually sample the extent (8 probes)"
        );
        assert_eq!(first.key_idx_for(shared_start), Some(2));

        // Same clip referenced by another playlist → cache hit, no further reads.
        let second = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("second title resolves");
        assert_eq!(
            reader.probes, after_first,
            "an identical extent must be served from cache with no further reads"
        );
        assert_eq!(
            second.key_idx_for(shared_start),
            first.key_idx_for(shared_start),
            "the cached index must equal the one originally resolved"
        );

        // A DIFFERENT extent is a miss: it must still be sampled, and must get its
        // OWN index (1), never the cached neighbour's (2).
        let third = multi_cps_title(other_start, sectors);
        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &third,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a different extent resolves");
        assert_eq!(
            reader.probes,
            after_first + 8,
            "a different extent must not hit the cache"
        );
        assert_eq!(
            map.key_idx_for(other_start),
            Some(1),
            "each distinct extent keeps its own resolved index"
        );
    }

    /// The cached index must be exactly what a full recompute produces: resolve the
    /// same title twice, once through a warm shared cache and once through a cold
    /// one (a real re-read), and compare the maps range for range.
    ///
    /// Mutation: caching under a key that ignores `start_lba`/`sector_count`, or
    /// storing anything but `pick`'s index, diverges here.
    #[test]
    fn multi_cps_cache_hit_matches_a_full_recompute() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_c = [0x03u8; 16];
        let units = vec![
            (1000u32, 1030u32, encrypted_clean_unit(&key_c)),
            (9000u32, 9030u32, encrypted_clean_unit(&key_b)),
        ];
        let mut title = DiscTitle::empty();
        title.extents = vec![
            Extent {
                start_lba: 1000,
                sector_count: 30,
            },
            Extent {
                start_lba: 9000,
                sector_count: 30,
            },
        ];
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b), (2, key_c)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };

        // Warm a cache, then serve the whole title from it.
        let mut warm = CountingCipherSource::new(units.clone());
        let mut cache = super::DiscKeyCache::new();
        for _ in 0..2 {
            super::resolve_mux_key_map_cached(
                &mut warm,
                &title,
                &mut keys,
                None,
                ContentFormat::BdTs,
                None,
                &mut cache,
            )
            .expect("warm resolve");
        }
        let cached = super::resolve_mux_key_map_cached(
            &mut warm,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("cached resolve");

        // The same title resolved from scratch, re-reading every unit.
        let mut cold = CountingCipherSource::new(units);
        let recomputed = super::resolve_mux_key_map_cached(
            &mut cold,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut super::DiscKeyCache::new(),
        )
        .expect("cold resolve");

        assert_eq!(
            cached.ranges(),
            recomputed.ranges(),
            "a cache hit must produce byte-for-byte the same map as a recompute"
        );
        assert_eq!(cold.probes, 16, "the cold resolve samples both extents");
        assert_eq!(
            warm.probes, 16,
            "the warm reader samples each extent exactly once across three resolves"
        );
    }

    /// An extent with no sampleable encrypted units inherits the PRECEDING extent's
    /// index — per-title state, not a property of the extent — so it must never be
    /// memoised. Title A reaches the clear extent carrying index 2, title B carries
    /// index 1: B's clear extent must key to 1, not to A's cached 2.
    ///
    /// Mutation: caching the `samples.is_empty() => last_idx` arm makes B's clear
    /// extent resolve to 2 and this fails.
    #[test]
    fn multi_cps_inherited_index_is_not_cached() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_c = [0x03u8; 16];
        let clear = 5000u32; // registered nowhere → reads as zeros → no samples
        let mut reader = CountingCipherSource::new(vec![
            (1000, 1030, encrypted_clean_unit(&key_c)), // → index 2
            (9000, 9030, encrypted_clean_unit(&key_b)), // → index 1
        ]);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b), (2, key_c)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut cache = super::DiscKeyCache::new();

        let mut with_c = DiscTitle::empty();
        with_c.extents = vec![
            Extent {
                start_lba: 1000,
                sector_count: 30,
            },
            Extent {
                start_lba: clear,
                sector_count: 30,
            },
        ];
        let a = super::resolve_mux_key_map_cached(
            &mut reader,
            &with_c,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("title A resolves");
        assert_eq!(
            a.key_idx_for(clear),
            Some(2),
            "a clear extent inherits the preceding extent's index"
        );

        let mut with_b = DiscTitle::empty();
        with_b.extents = vec![
            Extent {
                start_lba: 9000,
                sector_count: 30,
            },
            Extent {
                start_lba: clear,
                sector_count: 30,
            },
        ];
        let b = super::resolve_mux_key_map_cached(
            &mut reader,
            &with_b,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("title B resolves");
        assert_eq!(
            b.key_idx_for(clear),
            Some(1),
            "the inherited index must not be served from another title's cache entry"
        );
    }

    /// A fail-loud extent (real ciphertext no key opens) must not be memoised: a
    /// later retry, after a key source banked the missing key, has to re-sample and
    /// succeed rather than inherit the earlier failure.
    ///
    /// Mutation: caching before the `None => Err(DecryptFailed)` arm (or caching the
    /// error) leaves the retry unable to resolve.
    #[test]
    fn multi_cps_failed_extent_is_not_cached() {
        let key_a = [0x01u8; 16];
        let key_x = [0x09u8; 16];
        let start = 1000u32;
        let mut reader =
            CountingCipherSource::new(vec![(start, start + 30, encrypted_clean_unit(&key_x))]);
        let title = multi_cps_title(start, 30);
        let mut cache = super::DiscKeyCache::new();

        // Two held keys → the multi-CPS sampling path (a one-key pool short-circuits
        // to index 0 without sampling); neither opens the extent → fail loud.
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, [0x02u8; 16])],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect_err("no held key opens the extent → fail loud");
        assert!(cache.cps.is_empty(), "a failed extent must not be memoised");

        // The operator supplies the missing key; the retry must resolve it.
        if let DecryptKeys::Aacs { unit_keys, .. } = &mut keys {
            unit_keys.push((2, key_x));
        }
        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("the retry resolves once the key is banked");
        assert_eq!(
            map.key_idx_for(start),
            Some(2),
            "the retry re-samples and resolves to the newly banked key"
        );
    }

    // ── Fix 1: read-fault vs genuinely-not-FMTS in resolve_fmts_key_map ──────

    /// A SectorSource whose every read is a transient I/O fault (`DiscRead`),
    /// modelling a marginal live drive stalling while `resolve_fmts_key_map`
    /// probes the UDF metadata / segment table.
    struct FaultSource;
    impl SectorSource for FaultSource {
        fn capacity_sectors(&self) -> u32 {
            1_000_000
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            _count: u16,
            _buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            Err(crate::error::Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            })
        }
    }

    /// A transient `DiscRead` fault while reading the UDF metadata for the segment
    /// table must PROPAGATE (fail loud / retryable), NOT be swallowed into the
    /// not-FMTS `Ok(None)` fall-through — otherwise a marginal AACS 2.1 disc would
    /// silently drop its forensic content under a base-Unit-Key-only map and the
    /// mux would report success.
    ///
    /// Mutation: revert the read_filesystem arm to `let Ok(udf) = ... else { return
    /// Ok(None) }` → this returns `Ok(None)` and the assert fails.
    #[test]
    fn resolve_fmts_key_map_read_fault_propagates() {
        let mut reader = FaultSource;
        let title = multi_cps_title(1000, 30);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0x01u8; 16])],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let got = super::resolve_fmts_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut super::FmtsCache::default(),
            &mut super::CpsUnitCache::default(),
        );
        let err = got.expect_err("a transient DiscRead must fail loud, never Ok(None)");
        let expected = std::io::Error::from(crate::error::Error::DiscRead {
            sector: 256,
            status: None,
            sense: None,
        })
        .to_string();
        assert_eq!(
            err.to_string(),
            expected,
            "the DiscRead fault must propagate"
        );
    }

    /// A reader whose bytes are structurally NOT a UDF disc (all zeros → no AVDP at
    /// sector 256 → `UdfNotFilesystem`) is genuinely not FMTS: it must map to the
    /// clean `Ok(None)` negative, NOT fail loud. Guards against Fix 1 over-reaching
    /// and rejecting benign non-FMTS discs.
    #[test]
    fn resolve_fmts_key_map_not_udf_is_clean_none() {
        // CipherSource with no registered units reads as all zeros everywhere, so
        // read_filesystem sees tag_id 0 at sector 256 → UdfNotFilesystem.
        let mut reader = CipherSource { units: Vec::new() };
        let title = multi_cps_title(1000, 30);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0x01u8; 16])],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let got = super::resolve_fmts_key_map(
            &mut reader,
            &title,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut super::FmtsCache::default(),
            &mut super::CpsUnitCache::default(),
        )
        .expect("a structurally non-UDF disc is a clean not-FMTS negative");
        assert!(got.is_none(), "not a UDF/FMTS disc → Ok(None)");
    }

    // ── FMTS index-key resolution is memoised per DISC, not per title ─────────
    //
    // A synthetic AACS 2.1 disc: a real UDF tree (so `read_filesystem` walks it
    // for real) carrying `/AACS/IndividualSegment.tbl`, plus a content region whose
    // aligned units are genuinely encrypted — the EVEN units of a segment under its
    // index key and the ODD units under the alternate interleaved variant's key,
    // exactly the layout the phase probe exists to discover. The reader COUNTS its
    // reads (metadata vs probe) and the `KeyFetch` double COUNTS its calls, so the
    // tests can state the cost of N titles rather than assume it.

    /// First LBA of the synthetic disc's content. Everything below is UDF metadata.
    const FMTS_CONTENT_LBA: u32 = 10_000;
    /// Sectors in the content extent — enough to cover both forensic segments.
    const FMTS_CONTENT_SECTORS: u32 = 2_000;
    /// The base CPS Unit Key (pool slot 0) of the synthetic disc.
    const FMTS_BASE_KEY: [u8; 16] = [0x01u8; 16];
    /// The base Unit Key of a SECOND CPS unit (pool slot 1) on the multi-CPS
    /// variant of the synthetic disc — the key of every content sector in
    /// [`FMTS_CPS2_LBA`]'s extent.
    const FMTS_CPS2_KEY: [u8; 16] = [0x02u8; 16];
    /// First LBA of the second CPS unit's extent, immediately after the
    /// forensic clip.
    const FMTS_CPS2_LBA: u32 = FMTS_CONTENT_LBA + FMTS_CONTENT_SECTORS;
    /// Sectors in the second CPS unit's extent (200 aligned units).
    const FMTS_CPS2_SECTORS: u32 = 600;
    /// The disc's forensic index keys: element i = forensic index i+1. Two, not 32 —
    /// the resolver sizes itself to whatever the source returns.
    const FMTS_INDEX_KEYS: [[u8; 16]; 2] = [[0x21u8; 16], [0x22u8; 16]];
    /// The alternate interleaved variant's key: the ODD units of every segment are
    /// encrypted under it, so they do NOT open under the segment's index key.
    const FMTS_ALT_KEY: [u8; 16] = [0x99u8; 16];

    /// The disc's forensic segments: one per index, 2560 packets each (the retail
    /// size), on a 6144-byte-aligned start so units line up with sectors.
    fn fmts_segments() -> Vec<crate::aacs::segment::Segment> {
        use crate::aacs::segment::Segment;
        vec![
            Segment {
                index: 1,
                start_spn: 3200,
                end_spn: 3200 + 2559,
            },
            Segment {
                index: 2,
                start_spn: 6400,
                end_spn: 6400 + 2559,
            },
        ]
    }

    /// Serialise `segs` as an `IndividualSegment.tbl` image (8-byte header:
    /// type, record count, record size; then 16-byte records).
    fn fmts_tbl(segs: &[crate::aacs::segment::Segment]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&0x0100_0000u32.to_be_bytes());
        v.extend_from_slice(&(segs.len() as u16).to_be_bytes());
        v.extend_from_slice(&(crate::aacs::segment::SEGMENT_RECORD_LEN as u16).to_be_bytes());
        for s in segs {
            v.extend_from_slice(&0x0100_0000u32.to_be_bytes());
            v.extend_from_slice(&s.index.to_be_bytes());
            v.extend_from_slice(&1u16.to_be_bytes());
            v.extend_from_slice(&s.start_spn.to_be_bytes());
            v.extend_from_slice(&s.end_spn.to_be_bytes());
        }
        v
    }

    /// The synthetic FMTS disc. Counts UDF-metadata reads and content-probe reads
    /// separately, and can be told to fault every read of one LBA span (a marginal
    /// live drive) to exercise the read-fault path.
    struct FmtsDisc {
        meta_disc: crate::udf::fixture::MemDisc,
        segs: Vec<crate::aacs::segment::Segment>,
        /// Reads below `FMTS_CONTENT_LBA`: the UDF walk + segment-table load.
        meta_reads: u32,
        /// Reads at/above `FMTS_CONTENT_LBA`: the anchor + phase probes.
        probe_reads: u32,
        /// `[start, end)` LBA span whose every read returns `DiscRead`.
        fault_span: Option<(u32, u32)>,
        /// When set, LBAs at/above [`FMTS_CPS2_LBA`] belong to a SECOND CPS
        /// unit and are encrypted under [`FMTS_CPS2_KEY`], not the base key.
        second_cps: bool,
        /// `[start, end)` LBA span that reads back as zeros — CLEAR content, with
        /// no encrypted unit to sample and so no CPS-unit evidence of its own.
        clear_span: Option<(u32, u32)>,
        /// An operator Stop that lands MID-resolve: the token is cancelled as soon
        /// as this many reads of the given kind have been served.
        cancel_after: Option<(crate::halt::Halt, CancelWhen, u32)>,
    }

    /// Which read counter [`FmtsDisc::cancel_after`] watches.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum CancelWhen {
        /// UDF-metadata reads — the Stop lands after the segment table is loaded and
        /// before a single content sector has been touched.
        Meta,
        /// Content-probe reads — the Stop lands with the drive already working.
        Probe,
    }

    /// The same synthetic disc with extra records appended to
    /// `IndividualSegment.tbl` ONLY — the content region is laid out from
    /// [`fmts_segments`] exactly as before, so the anchor and phase probes still
    /// resolve normally and the extra records are seen purely by the range builder.
    /// That is what isolates each of its "this record does not map" arms.
    fn fmts_disc_with_extra_records(extra: &[crate::aacs::segment::Segment]) -> FmtsDisc {
        let mut all = fmts_segments();
        all.extend_from_slice(extra);
        let mut d = FmtsDisc::new();
        d.meta_disc = FmtsDisc::rebuild_meta(&all);
        d
    }

    impl FmtsDisc {
        fn new() -> Self {
            Self::with_forensic_clip(true)
        }

        /// `clip = false` drops `BDMV/STREAM/00001.fmts` from the tree: an FMTS disc
        /// (the table is there) whose forensic clip cannot be identified, so the
        /// segment SPNs have no defensible anchor.
        /// The UDF metadata image alone, with `tbl_segs` as the segment table and
        /// the forensic clip present — see [`fmts_disc_with_extra_records`].
        fn rebuild_meta(
            tbl_segs: &[crate::aacs::segment::Segment],
        ) -> crate::udf::fixture::MemDisc {
            let mut d = Self::build(true, tbl_segs);
            std::mem::replace(&mut d.meta_disc, crate::udf::fixture::MemDisc::new())
        }

        fn with_forensic_clip(clip: bool) -> Self {
            Self::build(clip, &fmts_segments())
        }

        fn build(clip: bool, tbl_segs: &[crate::aacs::segment::Segment]) -> Self {
            use crate::udf::fixture::{
                DirSpec, PART_START, build_udf_skeleton, file, file_with, lay_dir,
            };
            let segs = fmts_segments();
            let tbl_image = fmts_tbl(tbl_segs);
            let mut meta_disc = crate::udf::fixture::MemDisc::new();
            build_udf_skeleton(&mut meta_disc, 10);
            let mut subdirs = vec![DirSpec {
                name: "AACS".to_string(),
                icb_lba: 12,
                dir_data_lba: 13,
                files: vec![file_with("IndividualSegment.tbl", 14, 15, tbl_image, true)],
                subdirs: Vec::new(),
            }];
            if clip {
                // The forensic FEATURE clip, as a real AACS 2.1 disc names it:
                // `BDMV/STREAM/<clip>.fmts`, whose extents are the byte space the
                // segment SPNs are relative to. Declared in UDF only (the reader
                // synthesises its content), at exactly the content region:
                // partition-relative data LBA + PART_START = FMTS_CONTENT_LBA.
                subdirs.push(DirSpec {
                    name: "BDMV".to_string(),
                    icb_lba: 16,
                    dir_data_lba: 17,
                    files: Vec::new(),
                    subdirs: vec![DirSpec {
                        name: "STREAM".to_string(),
                        icb_lba: 18,
                        dir_data_lba: 19,
                        files: vec![file(
                            "00001.fmts",
                            20,
                            FMTS_CONTENT_LBA - PART_START,
                            FMTS_CONTENT_SECTORS * 2048,
                            true,
                        )],
                        subdirs: Vec::new(),
                    }],
                });
            }
            lay_dir(
                &mut meta_disc,
                &DirSpec {
                    name: String::new(),
                    icb_lba: 10,
                    dir_data_lba: 11,
                    files: Vec::new(),
                    subdirs,
                },
            );
            Self {
                meta_disc,
                segs,
                meta_reads: 0,
                probe_reads: 0,
                fault_span: None,
                second_cps: false,
                clear_span: None,
                cancel_after: None,
            }
        }

        /// The same disc, plus a second CPS unit occupying the extent at
        /// [`FMTS_CPS2_LBA`] — a disc whose `Unit_Key_RO.inf` carries two base
        /// Unit Keys, which is what makes "the base key" ambiguous.
        fn with_second_cps_unit() -> Self {
            Self {
                second_cps: true,
                ..Self::new()
            }
        }

        /// The 6144-byte ciphertext of the aligned unit starting at clip byte
        /// `unit_byte`: inside a segment, EVEN units carry that index's content and
        /// ODD units the alternate variant; outside, ordinary base-Unit-Key content.
        /// Flip the operator's Stop once the watched counter reaches its threshold.
        fn maybe_cancel(&mut self) {
            if let Some((h, when, n)) = &self.cancel_after {
                let seen = match when {
                    CancelWhen::Meta => self.meta_reads,
                    CancelWhen::Probe => self.probe_reads,
                };
                if seen >= *n {
                    h.cancel();
                }
            }
        }

        fn unit_at(&self, unit_byte: u64) -> Vec<u8> {
            for s in &self.segs {
                let sb = s.start_byte();
                if unit_byte >= sb && unit_byte < sb + s.byte_len() {
                    let n = (unit_byte - sb) / crate::aacs::content::ALIGNED_UNIT_LEN as u64;
                    let key = if n.is_multiple_of(2) {
                        FMTS_INDEX_KEYS[(s.index - 1) as usize]
                    } else {
                        FMTS_ALT_KEY
                    };
                    return encrypted_clean_unit(&key);
                }
            }
            encrypted_clean_unit(&FMTS_BASE_KEY)
        }
    }

    impl SectorSource for FmtsDisc {
        fn capacity_sectors(&self) -> u32 {
            1_000_000
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if let Some((a, b)) = self.fault_span
                && lba >= a
                && lba < b
            {
                self.probe_reads += 1;
                self.maybe_cancel();
                return Err(crate::error::Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                });
            }
            if lba < FMTS_CONTENT_LBA {
                self.meta_reads += 1;
                self.maybe_cancel();
                return self.meta_disc.read_sectors(lba, count, buf, recovery);
            }
            let want = count as usize * 2048;
            // A clear span: readable, but carrying no encrypted unit at all.
            if let Some((a, b)) = self.clear_span
                && lba >= a
                && lba < b
            {
                self.probe_reads += 1;
                self.maybe_cancel();
                buf[..want].fill(0);
                return Ok(want);
            }
            // The SECOND CPS unit's extent: ordinary (non-forensic) content,
            // encrypted under that unit's own base Unit Key.
            if self.second_cps && lba >= FMTS_CPS2_LBA {
                self.probe_reads += 1;
                self.maybe_cancel();
                let unit = encrypted_clean_unit(&FMTS_CPS2_KEY);
                for s in 0..count as usize {
                    let within = ((lba as usize + s - FMTS_CPS2_LBA as usize) % 3) * 2048;
                    buf[s * 2048..(s + 1) * 2048].copy_from_slice(&unit[within..within + 2048]);
                }
                return Ok(want);
            }
            self.probe_reads += 1;
            self.maybe_cancel();
            buf[..want].fill(0);
            for s in 0..count as u32 {
                let off = (lba + s - FMTS_CONTENT_LBA) as u64;
                let unit_byte = (off / 3) * crate::aacs::content::ALIGNED_UNIT_LEN as u64;
                let within = (off % 3) as usize * 2048;
                let unit = self.unit_at(unit_byte);
                let dst = s as usize * 2048;
                buf[dst..dst + 2048].copy_from_slice(&unit[within..within + 2048]);
            }
            Ok(want)
        }
    }

    /// A `KeyFetch` whose `fmts_indexes` counts its calls and behaves like the real
    /// service: it replies with the disc's COMPLETE ordered index-key set only for a
    /// genuine index-1 anchor batch (one that opens under index key 1), and empty
    /// otherwise. `unit_keys` is never used on this path.
    fn counting_fmts_fetch(
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> crate::sector::KeyFetch {
        crate::sector::KeyFetch::new(
            std::sync::Arc::new(|_| Vec::new()),
            std::sync::Arc::new(move |batch: &[Vec<u8>]| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let Some(first) = batch.first() else {
                    return Vec::new();
                };
                let mut u = first.clone();
                crate::aacs::content::decrypt_unit(&mut u, &FMTS_INDEX_KEYS[0]);
                if crate::aacs::content::is_clean(&u, ContentFormat::BdTs) {
                    FMTS_INDEX_KEYS.to_vec()
                } else {
                    Vec::new()
                }
            }),
        )
    }

    fn fmts_title(sectors: u32) -> DiscTitle {
        multi_cps_title(FMTS_CONTENT_LBA, sectors)
    }

    fn fmts_keys() -> DecryptKeys {
        DecryptKeys::Aacs {
            unit_keys: vec![(0, FMTS_BASE_KEY)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        }
    }

    /// A play-all title over BOTH CPS units: the forensic clip (CPS unit 1)
    /// then the second unit's extent (CPS unit 2).
    fn fmts_two_cps_title() -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.extents = vec![
            Extent {
                start_lba: FMTS_CONTENT_LBA,
                sector_count: FMTS_CONTENT_SECTORS,
            },
            Extent {
                start_lba: FMTS_CPS2_LBA,
                sector_count: FMTS_CPS2_SECTORS,
            },
        ];
        t
    }

    /// The disc's two BASE CPS Unit Keys, in `Unit_Key_RO.inf` order — the
    /// pool an AACS 2.1 disc with two CPS units is resolved with.
    fn fmts_two_cps_keys() -> DecryptKeys {
        DecryptKeys::Aacs {
            unit_keys: vec![(1, FMTS_BASE_KEY), (2, FMTS_CPS2_KEY)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        }
    }

    /// On an FMTS (AACS 2.1) disc the non-forensic gap fill used to hardcode
    /// pool slot 0 as "the" base Unit Key. On a disc carrying MORE THAN ONE
    /// base CPS Unit Key in `Unit_Key_RO.inf` that is simply the first CPS
    /// unit's key, so every content LBA outside a forensic segment in any
    /// OTHER CPS unit was keyed with the wrong key.
    ///
    /// It does not fail loudly: the mapped decrypt runs the wrong key over
    /// those units and emits garbage plaintext with `lost_bytes == 0`. And
    /// `resolve_mux_key_map_cached` calls the FMTS resolver BEFORE the
    /// `single_base_key_slot` short-circuit, so the guard that makes slot 0
    /// correct on a single-CPS disc is never consulted on this path.
    ///
    /// The title spans both CPS units; the second extent's every unit is
    /// encrypted under the SECOND base key (pool slot 1). Mutation: pinning
    /// the gap fill back to slot 0 fails the second extent's asserts, and
    /// pinning it to slot 1 fails the first extent's.
    #[test]
    fn fmts_gap_fill_uses_each_lbas_own_cps_unit_key_not_pool_slot_zero() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let mut reader = FmtsDisc::with_second_cps_unit();
        let mut keys = fmts_two_cps_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_two_cps_title();

        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a two-CPS-unit FMTS disc resolves");

        // CPS unit 1 (the forensic clip): the forensic segments keep their own
        // index-key slots, and the gaps between them take unit 1's key, which
        // IS pool slot 0 here.
        assert_eq!(
            map.key_idx_for(10_300),
            Some(2),
            "segment index 1 keys to its own pool slot (appended after the 2 base keys)"
        );
        assert_eq!(
            map.key_idx_for(10_600),
            Some(3),
            "segment index 2 keys to its own pool slot"
        );
        assert_eq!(
            map.key_idx_for(10_000),
            Some(0),
            "a non-segment LBA of CPS unit 1 takes CPS unit 1's key"
        );

        // CPS unit 2: every LBA of this extent must take the SECOND base Unit
        // Key (pool slot 1). Slot 0 here is CPS unit 1's key — the defect.
        for lba in [FMTS_CPS2_LBA, FMTS_CPS2_LBA + 300, FMTS_CPS2_LBA + 599] {
            assert_eq!(
                map.key_idx_for(lba),
                Some(1),
                "LBA {lba} is in CPS unit 2 and must take CPS unit 2's key, not unit 1's"
            );
        }

        // Nothing outside the title's extents is keyed at all.
        assert_eq!(
            map.key_idx_for(FMTS_CPS2_LBA + FMTS_CPS2_SECTORS),
            None,
            "past the last extent is nav/filesystem and passes through"
        );
    }

    fn pool_of(keys: &DecryptKeys) -> Vec<(u32, [u8; 16])> {
        match keys {
            DecryptKeys::Aacs { unit_keys, .. } => unit_keys.clone(),
            _ => Vec::new(),
        }
    }

    /// The defect this fixes: `resolve_content_key_map` resolves EVERY title, and the
    /// FMTS branch used to re-walk the UDF filesystem, re-run the anchor probe, re-run
    /// the 2-index phase probe AND re-ask the key service — once per playlist — for an
    /// answer that is a property of the DISC. N titles must cost ONE UDF walk, ONE
    /// index probe and ONE `fmts_indexes` call.
    ///
    /// Mutations, and which assert kills each:
    /// * `if table.is_none()` → `if true` (never memoise the segment table):
    ///   the `meta_reads` assert fails (the walk repeats per title).
    /// * removing the `memo.get(&ek)` short-circuit: the `probe_reads` AND the
    ///   `fmts_indexes` call-count asserts both fail.
    /// * `if probed.all_phases_definite` → `if false` (never insert): same two.
    #[test]
    fn fmts_index_keys_resolved_once_per_disc_not_once_per_title() {
        use std::sync::atomic::Ordering;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let mut reader = FmtsDisc::new();
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);

        let first = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("the synthetic FMTS disc resolves");
        let (meta1, probe1) = (reader.meta_reads, reader.probe_reads);
        let calls1 = calls.load(Ordering::SeqCst);
        assert!(meta1 > 0, "the first title really does walk the UDF tree");
        assert_eq!(
            probe1, 40,
            "the first title pays the anchor (8 reads) plus a 16-read phase probe per index"
        );
        assert_eq!(calls1, 1, "ONE anchor batch resolves the whole index set");
        // The map really is the forensic one: two segment ranges on their index-key
        // slots with the probed phase, and the base key over the rest.
        assert_eq!(
            first.key_idx_for(10_300),
            Some(1),
            "segment index 1 keys to its own pool slot"
        );
        assert_eq!(
            first.key_idx_for(10_600),
            Some(2),
            "segment index 2 keys to its own pool slot"
        );
        assert_eq!(
            first.key_idx_for(10_000),
            Some(0),
            "base key outside segments"
        );

        // 59 more playlists of the same clip — the shape of a real BD.
        for _ in 0..59 {
            let again = super::resolve_mux_key_map_cached(
                &mut reader,
                &title,
                &mut keys,
                Some(&fetch),
                ContentFormat::BdTs,
                None,
                &mut cache,
            )
            .expect("a later title resolves from the memo");
            assert_eq!(
                again.ranges(),
                first.ranges(),
                "a memoised title must produce the byte-identical map"
            );
        }
        assert_eq!(
            reader.meta_reads, meta1,
            "the UDF walk / segment table is a DISC fact: exactly one walk for 60 titles"
        );
        assert_eq!(
            reader.probe_reads, probe1,
            "the anchor and phase probes must not re-run per title"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the key service must be asked ONCE per disc, not once per playlist"
        );
        // And the pool grew exactly once — the index keys were banked, not duplicated.
        assert_eq!(
            pool_of(&keys).len(),
            1 + FMTS_INDEX_KEYS.len(),
            "the base key plus one slot per forensic index, appended once"
        );
    }

    /// Result-identity, proven rather than assumed: the same three titles resolved
    /// with a SHARED per-disc memo must produce the same maps — and leave the key
    /// pool in the same state — as resolving each with a FRESH memo (i.e. the
    /// unmemoised per-title recomputation).
    ///
    /// Mutation: key the FMTS memo on something NOT title-invariant (e.g. drop the
    /// extent list from `extent_key` and key on `format` alone) → title 3's differing
    /// extents are served the wrong answer and the range comparison fails.
    #[test]
    fn fmts_memoised_map_equals_per_title_recomputation() {
        let titles = [
            fmts_title(FMTS_CONTENT_SECTORS),
            fmts_title(FMTS_CONTENT_SECTORS),
            // A DIFFERENT extent list (a longer clip): a memo miss that must be
            // recomputed, not served the first title's answer.
            fmts_title(FMTS_CONTENT_SECTORS + 500),
        ];

        let mut shared_cache = super::DiscKeyCache::new();
        let mut shared_keys = fmts_keys();
        let mut reader = FmtsDisc::new();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let shared: Vec<_> = titles
            .iter()
            .map(|t| {
                super::resolve_mux_key_map_cached(
                    &mut reader,
                    t,
                    &mut shared_keys,
                    Some(&fetch),
                    ContentFormat::BdTs,
                    None,
                    &mut shared_cache,
                )
                .expect("shared-memo resolve")
            })
            .collect();

        let mut fresh_keys = fmts_keys();
        let mut reader2 = FmtsDisc::new();
        let calls2 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch2 = counting_fmts_fetch(calls2.clone());
        let fresh: Vec<_> = titles
            .iter()
            .map(|t| {
                super::resolve_mux_key_map_cached(
                    &mut reader2,
                    t,
                    &mut fresh_keys,
                    Some(&fetch2),
                    ContentFormat::BdTs,
                    None,
                    // A fresh memo per title == the unmemoised behaviour.
                    &mut super::DiscKeyCache::new(),
                )
                .expect("per-title recompute")
            })
            .collect();

        for (i, (a, b)) in shared.iter().zip(fresh.iter()).enumerate() {
            assert_eq!(
                a.ranges(),
                b.ranges(),
                "title {i}: the memoised map must equal the per-title recomputation"
            );
        }
        assert_eq!(
            pool_of(&shared_keys),
            pool_of(&fresh_keys),
            "the key pool must end in the same state — same keys, same slots, same order"
        );
        // And the memo really was doing work: the unmemoised run paid 3 walks and 3
        // key-service calls where the memoised one paid 1 walk and 2 calls (title 3's
        // extent list is a genuine miss).
        assert!(
            reader2.meta_reads > reader.meta_reads,
            "the per-title recomputation re-walks the UDF tree; the memoised run does not"
        );
        assert_eq!(
            (
                calls.load(std::sync::atomic::Ordering::SeqCst),
                calls2.load(std::sync::atomic::Ordering::SeqCst)
            ),
            (2, 3),
            "memoised: one call per DISTINCT extent list; unmemoised: one per title"
        );
    }

    /// A DIFFERENT extent list is a genuine miss: the extent list is the only
    /// per-title input to the probes, so it is IN the memo key and a title with a
    /// different one must re-probe rather than inherit.
    ///
    /// Mutation: drop `title.extents` from `extent_key` → the second title hits and
    /// the `probe_reads` / call-count asserts fail.
    #[test]
    fn fmts_different_extent_list_is_a_miss_and_reprobes() {
        use std::sync::atomic::Ordering;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let mut reader = FmtsDisc::new();
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();

        let a = fmts_title(FMTS_CONTENT_SECTORS);
        super::resolve_mux_key_map_cached(
            &mut reader,
            &a,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("first title resolves");
        let (meta1, probe1) = (reader.meta_reads, reader.probe_reads);

        let b = fmts_title(FMTS_CONTENT_SECTORS + 500);
        super::resolve_mux_key_map_cached(
            &mut reader,
            &b,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("second title resolves");
        assert_eq!(
            reader.meta_reads, meta1,
            "the segment table is disc-wide: still no second UDF walk"
        );
        assert_eq!(
            reader.probe_reads,
            probe1 * 2,
            "a different extent list must re-probe from its own bytes"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "and re-ask the key service for that extent list"
        );
    }

    /// A phase probe that READ-FAULTED on every segment of an index leaves that
    /// index defaulted to `Phase::All` — degraded but complete. That is a property of
    /// a transient live-drive fault, NOT of the title's extents, so it must NOT be
    /// memoised: caching it would spread one bad read across every remaining
    /// playlist. The next title must re-probe.
    ///
    /// Mutation: memoise unconditionally (drop the `all_phases_definite` guard) →
    /// the second title is served from cache and both the `probe_reads` and the
    /// key-service call-count asserts fail.
    #[test]
    fn fmts_read_faulted_phase_is_not_memoised() {
        use std::sync::atomic::Ordering;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let mut reader = FmtsDisc::new();
        // Fault every read of the index-2 segment (clip bytes 6400*192.. → LBAs
        // 10600..10840): the anchor (index 1) still succeeds, so the keys are in
        // hand, but index 2's phase probe has zero decrypt evidence.
        reader.fault_span = Some((10_600, 10_840));
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);

        let first = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a read fault must NOT abort a rip whose index keys are good");
        assert_eq!(
            first.entry_for(10_600).map(|(_, p, _)| p),
            Some(crate::decrypt::Phase::All),
            "the read-faulted index defaults to Phase::All"
        );
        assert_eq!(
            first.entry_for(10_300).map(|(_, p, _)| p),
            Some(crate::decrypt::Phase::Even),
            "the index that DID probe keeps its resolved phase"
        );
        let probe1 = reader.probe_reads;

        super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("second title resolves");
        assert!(
            reader.probe_reads > probe1,
            "a read-faulted run must not be memoised — the next title re-probes"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "and re-anchors rather than inheriting a degraded answer"
        );
    }

    /// The UDF walk that decides whether a disc is FMTS at all runs on EVERY disc,
    /// FMTS or not. On a plain (non-UDF / non-FMTS) BD it must be attempted ONCE for
    /// the whole disc, not once per playlist — ~35 low-LBA single-sector reads reached
    /// by a full-stroke seek back from wherever the last title's content sampling left
    /// the head.
    ///
    /// Mutation: `if table.is_none()` → `if true` → the second title re-reads the
    /// metadata and the `meta` assert fails.
    #[test]
    fn non_fmts_disc_walks_the_filesystem_once_for_every_title() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        // All-zero metadata → no AVDP at sector 256 → `UdfNotFilesystem`, the
        // deterministic "not an FMTS disc" negative that IS memoised.
        let mut reader = CountingCipherSource::new(vec![
            (1000, 1030, encrypted_clean_unit(&key_b)),
            (9000, 9030, encrypted_clean_unit(&key_b)),
        ]);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut cache = super::DiscKeyCache::new();

        super::resolve_mux_key_map_cached(
            &mut reader,
            &multi_cps_title(1000, 30),
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("first title resolves");
        let meta1 = reader.meta;
        assert!(
            meta1 > 0,
            "the first title really does attempt the UDF walk"
        );

        // A second title with DIFFERENT extents (so the CPS memo misses and the
        // title is genuinely resolved) must still not re-walk the filesystem.
        super::resolve_mux_key_map_cached(
            &mut reader,
            &multi_cps_title(9000, 30),
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("second title resolves");
        assert_eq!(
            reader.meta, meta1,
            "the not-FMTS verdict is a disc fact: one filesystem walk for the disc"
        );
    }

    /// Halt must stay responsive even when both FMTS memos are warm and a title does
    /// no I/O at all: `resolve_content_key_map` polls nothing itself, so the entry
    /// check inside the FMTS branch is the only cancellation point a fully-memoised
    /// title reaches.
    ///
    /// Mutation: remove the `check_halt()?` at the top of `resolve_fmts_key_map` →
    /// the cancelled second title returns `Ok(map)` and `expect_err` fails.
    #[test]
    fn fmts_memoised_title_still_honors_halt() {
        use crate::halt::Halt;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::new();
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);
        let halt = Halt::new();

        // Warm both memos.
        super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            Some(&halt),
            &mut cache,
        )
        .expect("first title resolves");

        halt.cancel();
        let err = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            Some(&halt),
            &mut cache,
        )
        .expect_err("a cancelled halt must abort even a fully-memoised title");
        assert!(crate::error::is_halt(&err), "expected Halted, got: {err}");
    }

    /// The single-CPS short-circuit must depend on the number of BASE CPS Unit Keys,
    /// NOT on the length of the whole pool — which the FMTS resolver APPENDS its
    /// forensic index keys to. Before the fix, a disc whose first-resolved playlist
    /// was forensic left the shared pool at `1 + n_index`, so every LATER title
    /// (single-CPS, non-forensic) missed the short-circuit and took the multi-CPS
    /// sampling path: 8 random 6144-byte reads per extent, and a `DecryptFailed`
    /// abort of the WHOLE-disc key map for any extent no pooled key opened. That made
    /// the result depend on the ORDER titles resolve in, contradicting the
    /// order-independence `resolve_mux_key_map_cached` documents.
    ///
    /// Mutation: count the whole pool (`unit_keys.len() == 1`) → the menu title
    /// samples its extent and the `probe_reads` assert fails.
    #[test]
    fn single_cps_short_circuit_survives_a_forensic_title_resolving_first() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::new();
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();

        // 1) The FORENSIC title resolves FIRST and banks the disc's index keys into
        //    the shared pool.
        let forensic = fmts_title(FMTS_CONTENT_SECTORS);
        super::resolve_mux_key_map_cached(
            &mut reader,
            &forensic,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("the forensic title resolves");
        assert_eq!(
            pool_of(&keys).len(),
            1 + FMTS_INDEX_KEYS.len(),
            "the forensic index keys really are banked into the shared pool"
        );
        let probes_after_forensic = reader.probe_reads;

        // 2) A NON-forensic title of the same disc (a menu playlist: its extents lie
        //    outside the forensic clip) still has exactly ONE base CPS Unit Key, so it
        //    must take the single-CPS short-circuit — no sampling reads at all.
        // (+9_000 keeps the extent on the fixture's 3-sector aligned-unit grid, so
        // the multi-CPS path WOULD succeed here — the test is about it not running at
        // all, not about it failing.)
        let menu = multi_cps_title(FMTS_CONTENT_LBA + 9_000, 300);
        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &menu,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a single-CPS menu title must not need the sampling path");
        assert_eq!(
            reader.probe_reads, probes_after_forensic,
            "single base CPS key → the short-circuit fires, so NO multi-CPS sampling"
        );
        assert_eq!(
            map.ranges(),
            super::content_map(&menu, 0).ranges(),
            "one base CPS key → key 0 over every content extent"
        );
    }

    /// First extent of the play-all playlist's TRAILER clip in the defect-2 test.
    /// Placed AFTER the forensic clip on disc but FIRST in the extent list — the
    /// authoring order that broke the segment byte-space anchor.
    const TRAILER_LBA: u32 = FMTS_CONTENT_LBA + FMTS_CONTENT_SECTORS + 1_000;
    /// Sectors in the trailer clip — exactly the byte offset of forensic segment 1
    /// (3200 * 192 = 614_400 = 300 * 2048), so a wrong anchor is off by exactly the
    /// trailer's length and the two candidate LBAs are unambiguous.
    const TRAILER_SECTORS: u32 = 300;

    /// Forensic segment SPNs live in the FORENSIC FEATURE CLIP's byte space, so
    /// mapping them through the TITLE's extent list is wrong for any playlist whose
    /// extents do not begin with that clip. On a play-all playlist ordered [trailer,
    /// forensic feature] the old code treated clip byte 0 as the trailer's first
    /// sector: every segment landed 300 sectors early, the anchor probe sampled the
    /// TRAILER (so the key service never anchored → `FmtsKeyMissing` aborted the
    /// whole disc), and had it anchored, the index keys would have been applied to
    /// non-forensic sectors — silent garble with no error at all.
    ///
    /// Mutation: anchor the byte space on `title.extents` again → the resolve either
    /// errors (no anchor) or maps segment 1 to 10_000 instead of 10_300.
    #[test]
    fn forensic_segments_anchor_to_the_forensic_clip_not_the_titles_first_extent() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::new();
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();

        // A play-all playlist: the trailer clip FIRST, the forensic feature second.
        let mut title = DiscTitle::empty();
        title.extents = vec![
            Extent {
                start_lba: TRAILER_LBA,
                sector_count: TRAILER_SECTORS,
            },
            Extent {
                start_lba: FMTS_CONTENT_LBA,
                sector_count: FMTS_CONTENT_SECTORS,
            },
        ];

        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a play-all playlist carrying the forensic clip must resolve");

        // Segment index 1 starts at clip byte 3200 * 192 = 614_400 → sector 300 OF
        // THE FORENSIC CLIP → LBA 10_300. Segment index 2: 6400 * 192 = 1_228_800 →
        // sector 600 → LBA 10_600.
        assert_eq!(
            map.key_idx_for(FMTS_CONTENT_LBA + 300),
            Some(1),
            "segment index 1 maps to its own key at the FORENSIC clip's offset"
        );
        assert_eq!(
            map.key_idx_for(FMTS_CONTENT_LBA + 600),
            Some(2),
            "segment index 2 maps to its own key at the FORENSIC clip's offset"
        );
        assert_eq!(
            map.key_idx_for(FMTS_CONTENT_LBA),
            Some(0),
            "the forensic clip's first sectors are ordinary base-key content"
        );
        assert_eq!(
            map.key_idx_for(TRAILER_LBA),
            Some(0),
            "the trailer clip is base-key content — never a forensic index key"
        );
        assert_eq!(
            map.key_idx_for(TRAILER_LBA + TRAILER_SECTORS - 1),
            Some(0),
            "and the trailer's last sector too"
        );
    }

    /// The anchor is only sound because the forensic clip is IDENTIFIED. A disc that
    /// carries a non-empty `IndividualSegment.tbl` but no `BDMV/STREAM/*.fmts` gives
    /// the SPNs no defensible byte space, so the resolve must fail LOUD
    /// (`FmtsKeyMissing`, retryable) rather than fall back to a title's extent list
    /// and map forensic index keys onto whatever clip happens to be first — which
    /// produces silently garbled output with no error at all.
    #[test]
    fn unidentifiable_forensic_clip_fails_loud_rather_than_guessing_an_anchor() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::with_forensic_clip(false);
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);

        let err = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect_err("an unanchorable segment map must never be built");
        assert_eq!(
            err.to_string(),
            std::io::Error::from(crate::error::Error::FmtsKeyMissing).to_string(),
            "the forensic-content-without-an-anchor verdict is FmtsKeyMissing"
        );
        assert_eq!(
            pool_of(&keys).len(),
            1,
            "and nothing was banked into the pool"
        );
    }

    /// The base-CPS count that drives the single-CPS short-circuit, in isolation: the
    /// forensic index keys the FMTS resolver appends (tagged `FMTS_POOL_TAG_BASE + n`)
    /// are NOT CPS units, and a genuine second CPS unit is.
    ///
    /// Mutation: drop the tag filter → the first case returns `None` and its assert
    /// fails.
    #[test]
    fn single_base_key_slot_counts_cps_units_not_banked_forensic_keys() {
        let base = [0x01u8; 16];
        let idx1 = [0x21u8; 16];
        assert_eq!(
            super::single_base_key_slot(&[(1, base)]),
            Some(0),
            "one base CPS key → its slot"
        );
        assert_eq!(
            super::single_base_key_slot(&[
                (1, base),
                (super::FMTS_POOL_TAG_BASE + 1, idx1),
                (super::FMTS_POOL_TAG_BASE + 2, [0x22u8; 16]),
            ]),
            Some(0),
            "banked forensic index keys must not turn a single-CPS disc into multi-CPS"
        );
        assert_eq!(
            super::single_base_key_slot(&[(1, base), (2, idx1)]),
            None,
            "two real CPS units → the multi-CPS sampling path"
        );
        assert_eq!(
            super::single_base_key_slot(&[]),
            None,
            "an empty pool has no base key"
        );
        assert_eq!(
            super::single_base_key_slot(&[(super::FMTS_POOL_TAG_BASE, idx1), (7, base)]),
            Some(1),
            "the slot is the BASE key's, wherever it sits in the pool"
        );
    }

    /// The forensic ranges reach `fill_base_key_gaps` in `IndividualSegment.tbl`
    /// RECORD order, which is not LBA order — the table is a list of segments, and
    /// nothing in `aacs::segment` sorts it. The gap walk is a single forward sweep
    /// (`cur = cur.max(ce)`), so it is only correct on cuts in ascending order; that
    /// is what `c.sort_unstable()` is for.
    ///
    /// Every existing gap-fill case happens to pass its cuts already sorted, so the
    /// sort is unconstrained by them. Here the cuts arrive REVERSED: without the
    /// sort the sweep takes the high cut first, jumps `cur` past it, then discards
    /// the low cut as "already behind" — and emits a base-key fill straight over the
    /// low forensic segment. That is the silent-wrong-key shape: the forensic LBAs
    /// end up in TWO ranges, one of them keyed with the base Unit Key.
    #[test]
    fn fill_base_key_gaps_sorts_cuts_that_arrive_in_table_order_not_lba_order() {
        use crate::decrypt::Phase::{All, Even, Odd};
        let ext = vec![Extent {
            start_lba: 100,
            sector_count: 60,
        }];
        // Table order: the HIGH segment recorded before the LOW one.
        let forensic = vec![(140, 150, 6, Odd), (110, 120, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, 0);
        assert_eq!(
            fills,
            vec![(100, 110, 0, All), (120, 140, 0, All), (150, 160, 0, All),],
            "the gaps around BOTH forensic cuts must be filled, whatever order the \
             segment table listed them in"
        );
        // The load-bearing invariant: exactly one range covers every content LBA.
        assert_gapless(&ext, &forensic, &fills);
    }

    /// A `SectorSource` that records every `(lba, count)` it is asked for and serves
    /// a caller-chosen aligned unit, so the probe SPREAD of `sample_encrypted_units`
    /// is observable directly.
    struct RecordingSource {
        reads: Vec<(u32, u16)>,
        unit: Vec<u8>,
    }
    impl SectorSource for RecordingSource {
        fn capacity_sectors(&self) -> u32 {
            1_000_000
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.reads.push((lba, count));
            let want = count as usize * 2048;
            buf[..want].copy_from_slice(&self.unit[..want]);
            Ok(want)
        }
    }

    /// `sample_encrypted_units` is the evidence every CPS-unit decision rests on, so
    /// WHICH units it reads matters: 8 probes at `total * p / 9` for p in 1..=8, each
    /// a whole aligned unit (3 sectors) measured from the extent's own start.
    ///
    /// Pinned exactly. A probe count, a divisor or a `p` range that drifts moves the
    /// sample set — clustering probes at one end of a 20-minute clip, where an
    /// authored-bad or padding region can make an extent look unopenable — and each
    /// of those is an independently reachable mutation of this arithmetic.
    #[test]
    fn sample_encrypted_units_probes_eight_aligned_units_spread_across_the_extent() {
        let unit = encrypted_clean_unit(&[0x5Au8; 16]);
        let mut src = RecordingSource {
            reads: Vec::new(),
            unit: unit.clone(),
        };
        // 90 aligned units (270 sectors) from LBA 3000.
        let got = super::sample_encrypted_units(&mut src, 3000, 270, ContentFormat::BdTs);
        assert_eq!(
            src.reads,
            vec![
                (3000 + 10 * 3, 3),
                (3000 + 20 * 3, 3),
                (3000 + 30 * 3, 3),
                (3000 + 40 * 3, 3),
                (3000 + 50 * 3, 3),
                (3000 + 60 * 3, 3),
                (3000 + 70 * 3, 3),
                (3000 + 80 * 3, 3),
            ],
            "8 probes at total*p/9 aligned units, each a whole 3-sector unit, \
             anchored on the extent start"
        );
        assert_eq!(got.len(), 8, "every encrypted probe is returned");
        assert!(
            got.iter().all(|s| *s == unit),
            "the returned samples are the units as read"
        );
    }

    /// Only genuinely AACS-encrypted units come back — that is the whole contract
    /// that lets a caller treat a decrypt-to-clean as proof of the key. A clear
    /// (unencrypted) extent yields NO samples, which is what makes `pick_pool_slot`
    /// return `None` and the caller inherit rather than fail loud.
    #[test]
    fn sample_encrypted_units_drops_clear_units_and_reads_nothing_below_one_unit() {
        // A clear unit: the AACS scrambling bits in byte 0 are zero.
        let mut clear = encrypted_clean_unit(&[0x5Au8; 16]);
        clear[0] &= 0x3F;
        let mut src = RecordingSource {
            reads: Vec::new(),
            unit: clear,
        };
        let got = super::sample_encrypted_units(&mut src, 3000, 270, ContentFormat::BdTs);
        assert_eq!(src.reads.len(), 8, "the probes are still attempted");
        assert!(got.is_empty(), "a clear unit is not evidence of any key");

        // Under one whole aligned unit there is nothing to sample: no read at all.
        let mut src = RecordingSource {
            reads: Vec::new(),
            unit: encrypted_clean_unit(&[0x5Au8; 16]),
        };
        let got = super::sample_encrypted_units(&mut src, 3000, 2, ContentFormat::BdTs);
        assert!(
            src.reads.is_empty(),
            "an extent shorter than one aligned unit must not touch the drive"
        );
        assert!(got.is_empty());
    }

    /// `pick_pool_slot` answers "which of THESE slots, in THIS order, opens the
    /// extent" — and its two callers pass different slot lists for a reason: the
    /// multi-CPS path offers the whole pool, the FMTS gap fill only the BASE keys
    /// (offering a forensic index key there would key a whole extent with it).
    ///
    /// So both the RESTRICTION and the ORDER are load-bearing, and neither is
    /// implied by "some slot matched". Two samples open under two different pool
    /// slots here, so the answer is decided purely by which slot the caller listed
    /// first — a `find` that ignored `slots` order, or that scanned the pool
    /// instead, would return the same value for both directions.
    #[test]
    fn pick_pool_slot_honours_the_caller_s_slot_list_and_its_order() {
        let k0 = [0x01u8; 16];
        let k1 = [0x02u8; 16];
        let k2 = [0x03u8; 16];
        let pool = vec![(0u32, k0), (1, k1), (2, k2)];
        // One sample opens under slot 0, another under slot 2. Slot 1 opens neither.
        let samples = vec![encrypted_clean_unit(&k0), encrypted_clean_unit(&k2)];

        assert_eq!(
            super::pick_pool_slot(&samples, &pool, &[2, 0], ContentFormat::BdTs),
            Some(2),
            "the FIRST slot in the caller's order that opens a sample wins"
        );
        assert_eq!(
            super::pick_pool_slot(&samples, &pool, &[0, 2], ContentFormat::BdTs),
            Some(0),
            "reversing the caller's order reverses the answer"
        );
        assert_eq!(
            super::pick_pool_slot(&samples, &pool, &[1], ContentFormat::BdTs),
            None,
            "a slot list that excludes every opening key resolves to nothing"
        );
        assert_eq!(
            super::pick_pool_slot(&samples, &pool, &[7], ContentFormat::BdTs),
            None,
            "an out-of-range slot is skipped, not panicked on"
        );
        assert_eq!(
            super::pick_pool_slot(&[], &pool, &[0, 1, 2], ContentFormat::BdTs),
            None,
            "no samples is no evidence"
        );
    }

    /// Extents are `[start_lba, start_lba + sector_count)` — half open. This decides
    /// whether a title "reads the forensic clip", i.e. whether `resolve_fmts_key_map`
    /// resolves index keys at all or returns `Ok(None)` and leaves the title on the
    /// base-Unit-Key path.
    ///
    /// Both directions are wrong in a way that does not fail loudly: an inclusive
    /// end makes a title that merely ABUTS the forensic clip resolve (and pay a
    /// key-service round trip) for content it does not read, while a stricter test
    /// makes a title that shares exactly one sector fall through to a base-key-only
    /// map and silently garble its forensic units.
    #[test]
    fn extents_overlap_is_half_open_at_both_ends() {
        let at = |start_lba, sector_count| {
            vec![Extent {
                start_lba,
                sector_count,
            }]
        };
        assert!(
            !super::extents_overlap(&at(100, 10), &at(110, 10)),
            "b starts exactly where a ends → no shared sector"
        );
        assert!(
            !super::extents_overlap(&at(110, 10), &at(100, 10)),
            "and the same the other way round"
        );
        assert!(
            super::extents_overlap(&at(100, 11), &at(110, 10)),
            "one shared sector (110) IS an overlap"
        );
        assert!(
            super::extents_overlap(&at(110, 10), &at(100, 11)),
            "and the same the other way round"
        );
        assert!(
            super::extents_overlap(&at(100, 100), &at(120, 5)),
            "wholly contained is an overlap"
        );
        assert!(
            !super::extents_overlap(&at(100, 10), &[]),
            "nothing overlaps an empty extent list"
        );
        assert!(
            !super::extents_overlap(&[], &at(100, 10)),
            "in either position"
        );
        // A LATER extent of `a` matching is still an overlap — the scan must not
        // stop at the first extent of either list.
        assert!(
            super::extents_overlap(&[at(0, 10)[0], at(500, 10)[0]], &at(505, 10)),
            "any extent pair sharing a sector is an overlap"
        );
    }

    // ── An unmappable forensic record is a HOLE, and a hole is a hard failure ──
    //
    // `resolve_fmts_key_map`'s range builder has four independent arms that refuse
    // to emit a range for a record: inverted SPNs, an index with no key, clip bytes
    // past the clip's end, and a span that is not one contiguous run of sectors.
    // Each one tallies `unresolved`, and a non-zero tally aborts the disc.
    //
    // The tally is what makes those refusals SAFE. Drop it (or the `unresolved != 0`
    // check) and the `continue` still fires — so the record's LBAs fall through to
    // `fill_base_key_gaps`, which covers them with the BASE Unit Key. The forensic
    // units then decrypt to garbage under a key that was never theirs, the map
    // reports no error, and the rip completes with `lost_bytes == 0`. That is the
    // exact silent-wrong-key shape this module's comments call out; the four tests
    // below drive one arm each so no single tally can be deleted unnoticed.

    fn fmts_missing_err() -> String {
        std::io::Error::from(crate::error::Error::FmtsKeyMissing).to_string()
    }

    /// Resolve the synthetic FMTS disc with `extra` bogus records appended to its
    /// segment table, and return the error text.
    fn fmts_resolve_err_with(extra: &[crate::aacs::segment::Segment]) -> String {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = fmts_disc_with_extra_records(extra);
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);
        super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect_err("a forensic record that maps to no range must abort the disc")
        .to_string()
    }

    /// Control: the SAME resolve with no extra records succeeds. Without this the
    /// four tests below could be passing for any reason at all — a fixture that
    /// never reaches the range builder would `expect_err` just as happily.
    #[test]
    fn fmts_baseline_table_resolves_so_the_unmappable_record_tests_mean_something() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = fmts_disc_with_extra_records(&[]);
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);
        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("the well-formed synthetic FMTS disc resolves");
        // And it really is the forensic map: the segments carry their own index-key
        // slots (appended after the single base key), the gaps carry the base key.
        assert_eq!(map.key_idx_for(10_300), Some(1), "segment 1 → index key 1");
        assert_eq!(map.key_idx_for(10_600), Some(2), "segment 2 → index key 2");
        assert_eq!(
            map.key_idx_for(10_000),
            Some(0),
            "a gap → the base Unit Key"
        );
    }

    /// Arm 1 — an INVERTED record (`start_spn > end_spn`). `end_byte - 1 -
    /// start_byte` would underflow, so the record is refused; the tally is what
    /// turns that refusal into a loud failure instead of a base-keyed hole.
    #[test]
    fn fmts_inverted_segment_record_aborts_rather_than_base_keying_the_hole() {
        let bad = crate::aacs::segment::Segment {
            index: 1,
            start_spn: 12_000,
            end_spn: 11_000,
        };
        assert_eq!(fmts_resolve_err_with(&[bad]), fmts_missing_err());
    }

    /// Arm 2 — a record whose forensic INDEX has no key. The synthetic source
    /// returns two index keys, so tags 1 and 2 have pool slots and tag 3 has none.
    /// On a real disc this is a table that outruns the key set the service
    /// returned — precisely the case where guessing a key is worst.
    #[test]
    fn fmts_record_with_no_index_key_aborts_rather_than_base_keying_the_hole() {
        let bad = crate::aacs::segment::Segment {
            index: 3,
            start_spn: 12_000,
            end_spn: 12_000 + 2559,
        };
        assert_eq!(fmts_resolve_err_with(&[bad]), fmts_missing_err());
    }

    /// Arm 3 — a record whose START is addressable within the clip (so
    /// `filter_addressable_segments` keeps it) but whose END runs past the clip's
    /// last byte. Only the second `clip_byte_to_lba` fails, which is why the
    /// filter upstream cannot stand in for this check.
    #[test]
    fn fmts_record_running_past_the_clips_end_aborts_rather_than_base_keying_it() {
        // The clip is FMTS_CONTENT_SECTORS * 2048 bytes = 21_333 whole packets.
        let bad = crate::aacs::segment::Segment {
            index: 2,
            start_spn: 21_000, // byte 4_032_000 — inside the clip
            end_spn: 22_000,   // byte 4_224_191 — past its 4_096_000-byte end
        };
        assert!(
            crate::aacs::segment::clip_byte_to_lba(
                &[Extent {
                    start_lba: FMTS_CONTENT_LBA,
                    sector_count: FMTS_CONTENT_SECTORS,
                }],
                bad.start_byte(),
            )
            .is_some(),
            "the fixture must reach the END check — its START has to be addressable"
        );
        assert_eq!(fmts_resolve_err_with(&[bad]), fmts_missing_err());
    }

    /// Arm 4 — a record whose LBA span is not one contiguous run: `b - a` counts
    /// SECTOR crossings while `(end_byte - 1 - start_byte) / 2048` counts the
    /// span's own length in sectors, and the two disagree exactly when the record
    /// is not aligned to the aligned-unit grid the forensic interleave is defined
    /// on (or when it straddles a clip extent boundary).
    ///
    /// A structurally valid forensic segment cannot hit this: the interleave is
    /// per 6144-byte aligned unit, so a real record starts and ends on a
    /// 32-packet boundary and the two counts agree. `start_spn = 10` does not —
    /// clip byte 1920 is mid-sector — so the span is refused.
    #[test]
    fn fmts_record_off_the_aligned_unit_grid_aborts_rather_than_spanning_wrongly() {
        let bad = crate::aacs::segment::Segment {
            index: 2,
            start_spn: 10, // clip byte 1920 — 1920 % 2048 != 0
            end_spn: 19,   // last byte 3839 — a different sector, but < 2048 long
        };
        assert_eq!(fmts_resolve_err_with(&[bad]), fmts_missing_err());
    }

    /// The multi-CPS loop's inheritance chain has to run THROUGH a cache hit. An
    /// extent served from the memo never re-samples, so its index reaches the next
    /// extent only because the hit arm carries it into `last_idx`; without that, a
    /// following extent with no sampleable ciphertext inherits whatever the loop
    /// started at (slot 0) instead of its neighbour's key.
    ///
    /// That is silent: an unsampleable extent produces no error either way, so the
    /// map simply keys those LBAs to the wrong CPS unit and the mux decrypts them
    /// to garbage with `lost_bytes == 0`. The existing shared-extent test resolves a
    /// SINGLE-extent title through the cache, so nothing downstream of the hit is
    /// observed; here the clear extent is deliberately placed AFTER the hit.
    #[test]
    fn multi_cps_cache_hit_still_feeds_the_next_extents_inheritance() {
        let key_a = [0x01u8; 16];
        let key_b = [0x02u8; 16];
        let key_c = [0x03u8; 16];
        let shared = 1000u32;
        let clear = 5000u32; // no registered ciphertext → zeros → no samples
        let sectors = 30u32;
        let mut reader = CountingCipherSource::new(vec![(
            shared,
            shared + sectors,
            encrypted_clean_unit(&key_c),
        )]);
        let mut keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, key_a), (1, key_b), (2, key_c)],
            read_data_key: None,
            format: ContentFormat::BdTs,
        };
        let mut cache = super::DiscKeyCache::new();

        // Title 1 fills the memo for `shared` (index 2) by really sampling it.
        let first = super::resolve_mux_key_map_cached(
            &mut reader,
            &multi_cps_title(shared, sectors),
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("title 1 resolves");
        assert_eq!(first.key_idx_for(shared), Some(2), "sampled to its own key");
        let after_first = reader.probes;

        // Title 2: the same clip (a cache HIT — assert that below) followed by an
        // extent with nothing to sample.
        let mut title2 = DiscTitle::empty();
        title2.extents = vec![
            Extent {
                start_lba: shared,
                sector_count: sectors,
            },
            Extent {
                start_lba: clear,
                sector_count: sectors,
            },
        ];
        let second = super::resolve_mux_key_map_cached(
            &mut reader,
            &title2,
            &mut keys,
            None,
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("title 2 resolves");
        assert_eq!(
            reader.probes - after_first,
            8,
            "only the CLEAR extent is sampled — the shared one must be a cache hit, \
             which is the path under test"
        );
        assert_eq!(second.key_idx_for(shared), Some(2), "the hit's own index");
        assert_eq!(
            second.key_idx_for(clear),
            Some(2),
            "an unsampleable extent inherits the PRECEDING extent's index even when \
             that index came from the cache, not from a fresh sample"
        );
    }

    /// The FMTS gap fill on a MULTI-CPS disc runs its own extent loop, with its own
    /// inheritance chain (`base_slot_for_extent`'s `last_idx`). A title whose last
    /// extent has no sampleable ciphertext — a clear/nav tail, which is exactly the
    /// case that cannot fail loudly — must take the CPS unit its neighbour is in,
    /// not `base_slots[0]`.
    ///
    /// Slot 0 here is CPS unit 1's key and the neighbour is CPS unit 2, so losing
    /// the carry keys the tail with the wrong unit's key and decrypts it to garbage
    /// with no error at all.
    #[test]
    fn fmts_multi_cps_gap_fill_carries_the_preceding_extents_cps_unit_to_a_clear_tail() {
        const CLEAR_LBA: u32 = FMTS_CPS2_LBA + FMTS_CPS2_SECTORS;
        const CLEAR_SECTORS: u32 = 300;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::with_second_cps_unit();
        reader.clear_span = Some((CLEAR_LBA, CLEAR_LBA + CLEAR_SECTORS));
        let mut keys = fmts_two_cps_keys();
        let mut cache = super::DiscKeyCache::new();
        let mut title = fmts_two_cps_title();
        title.extents.push(Extent {
            start_lba: CLEAR_LBA,
            sector_count: CLEAR_SECTORS,
        });

        let map = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("a two-CPS FMTS disc with a clear tail resolves");
        assert_eq!(
            map.key_idx_for(FMTS_CPS2_LBA),
            Some(1),
            "the preceding extent is CPS unit 2 (pool slot 1)"
        );
        for lba in [CLEAR_LBA, CLEAR_LBA + CLEAR_SECTORS - 1] {
            assert_eq!(
                map.key_idx_for(lba),
                Some(1),
                "LBA {lba} has nothing to sample and must inherit CPS unit 2 from its \
                 neighbour, not fall back to the first CPS unit's key"
            );
        }
    }

    /// The FMTS gap fill samples extents off the LIVE DRIVE through the same
    /// per-disc [`CpsUnitCache`] the multi-CPS path uses — 8 random 6144-byte reads
    /// per extent, ~200 ms of seek apiece. Every input to that decision is in the
    /// cache key, so a second title over the same extents must cost ZERO further
    /// content reads.
    ///
    /// The index-key memo alone does not deliver that: it short-circuits the anchor
    /// and phase probes but the gap-fill loop still runs, and on a multi-CPS disc it
    /// re-samples every extent unless `base_slot_for_extent` banked its verdict.
    #[test]
    fn fmts_multi_cps_gap_fill_samples_each_extent_once_per_disc() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls);
        let mut reader = FmtsDisc::with_second_cps_unit();
        let mut keys = fmts_two_cps_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_two_cps_title();

        let first = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("first title resolves");
        let after_first = reader.probe_reads;
        assert!(
            after_first >= 16,
            "the first title really samples both extents (8 probes each), saw \
             {after_first} content reads"
        );

        let second = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            None,
            &mut cache,
        )
        .expect("second title resolves");
        assert_eq!(
            reader.probe_reads, after_first,
            "a second title over the same extents must touch the drive ZERO more \
             times — the CPS verdict is a property of the extent's own bytes"
        );
        for lba in [
            10_000u32,
            10_300,
            10_600,
            FMTS_CPS2_LBA,
            FMTS_CPS2_LBA + 599,
        ] {
            assert_eq!(
                second.key_idx_for(lba),
                first.key_idx_for(lba),
                "and the cached map must be identical at LBA {lba}"
            );
        }
    }

    // ── An operator Stop that lands MID-probe ────────────────────────────────
    //
    // The FMTS probes are the heaviest thing this crate does to a live drive:
    // hundreds of random 6144-byte reads, each able to stall to the SCSI recovery
    // timeout on a marginal disc. The module's hard rule is that `/api/stop` is
    // honored at every loop boundary rather than after the whole probe completes.
    //
    // The existing halt tests all pre-cancel, so the ENTRY poll alone satisfies
    // them and the two polls inside the probe loops are unconstrained. Cancelling
    // by OUTCOME is not enough either — a later poll still returns `Halted`, so a
    // deleted poll looks identical. What distinguishes them is what the drive was
    // asked to do after the Stop, so both tests below count reads.

    fn halted_err() -> String {
        std::io::Error::from(crate::error::Error::Halted).to_string()
    }

    /// Stop lands while the UDF walk is still running — before the anchor loop.
    /// The anchor loop's own poll must catch it, so the drive is never asked for a
    /// single CONTENT sector. Without that poll the entry poll has already passed
    /// and the next one is inside the phase loop, so the full anchor batch (two
    /// `MIN_SAMPLE_UNITS` phase reads plus a key-service round trip) is issued to a
    /// drive the operator has already stopped.
    #[test]
    fn fmts_stop_during_the_udf_walk_touches_no_content_sector() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let halt = crate::halt::Halt::new();
        let mut reader = FmtsDisc::new();
        reader.cancel_after = Some((halt.clone(), CancelWhen::Meta, 1));
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);

        let err = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            Some(&halt),
            &mut cache,
        )
        .expect_err("a Stop during the UDF walk must abort the resolve");
        assert_eq!(err.to_string(), halted_err(), "the verdict is Halted");
        assert!(
            reader.meta_reads > 0,
            "the fixture must really have reached the UDF walk"
        );
        assert_eq!(
            reader.probe_reads, 0,
            "a stopped drive must not be asked for a single content sector"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nor the key service asked for anything"
        );
    }

    /// Stop lands with the drive already working, during the anchor batch. The
    /// PHASE loop's poll must catch it before probing index 1's parity — otherwise
    /// every index in the set is probed (`MAX_ANCHOR_ATTEMPTS` segments ×
    /// `MIN_SAMPLE_UNITS` × 2 parities each) after the operator said stop.
    #[test]
    fn fmts_stop_during_the_anchor_batch_stops_before_the_phase_probes() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fetch = counting_fmts_fetch(calls.clone());
        let halt = crate::halt::Halt::new();
        let mut reader = FmtsDisc::new();
        // One content read in, i.e. inside the very first anchor phase batch: the
        // anchor loop's poll has already run and passed for this segment.
        reader.cancel_after = Some((halt.clone(), CancelWhen::Probe, 1));
        let mut keys = fmts_keys();
        let mut cache = super::DiscKeyCache::new();
        let title = fmts_title(FMTS_CONTENT_SECTORS);

        let err = super::resolve_mux_key_map_cached(
            &mut reader,
            &title,
            &mut keys,
            Some(&fetch),
            ContentFormat::BdTs,
            Some(&halt),
            &mut cache,
        )
        .expect_err("a Stop during the anchor batch must abort the resolve");
        assert_eq!(err.to_string(), halted_err(), "the verdict is Halted");
        // The anchor completed (it is one uninterruptible batch by construction);
        // the phase probes must NOT have started. Each phase probe reads
        // 2 * MIN_SAMPLE_UNITS units per attempted segment, so anything beyond the
        // anchor's own reads is the phase loop running past the Stop.
        let anchor_cost = crate::keysource::MIN_SAMPLE_UNITS as u32;
        assert!(
            reader.probe_reads > 0,
            "the fixture must really have reached the anchor batch"
        );
        assert!(
            reader.probe_reads <= 2 * anchor_cost,
            "the phase probes must not run after the Stop — saw {} content reads, \
             at most {} belong to the anchor",
            reader.probe_reads,
            2 * anchor_cost
        );
    }
}

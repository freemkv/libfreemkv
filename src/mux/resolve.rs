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
//! For disc→ISO (raw sector copy), use `Disc::copy()` instead.
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
    /// Decrypted file-tree output directory (`dir://`). A sink that writes
    /// per-file decrypted bytes (not muxed PES frames), so it never flows
    /// through `output()`; the CLI routes a `Dir` dest to `Disc::extract_tree`.
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

    /// Whether this URL represents a disc source (disc:// or iso://).
    pub fn is_disc_source(&self) -> bool {
        matches!(self, StreamUrl::Disc { .. } | StreamUrl::Iso { .. })
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
    if let Some(rest) = url.strip_prefix("stdio://") {
        if rest.is_empty() {
            return StreamUrl::Stdio;
        }
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
            let mut reader = crate::io::file_sector_source::FileSectorSource::open(path)?;
            let capacity = reader.capacity_sectors();
            let mut disc = crate::disc::Disc::scan_image(
                &mut reader,
                capacity,
                &crate::disc::ScanOptions::default(),
            )
            .map_err(|e| -> io::Error { e.into() })?;
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
                match crate::io::file_sector_source::FileSectorSource::open(path) {
                    Ok(mut probe) => {
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
                    Err(e) => {
                        // Non-fatal: a failed re-open just leaves MPLS 7.1/Atmos
                        // channel counts uncorrected (understated as 5.1). Log so
                        // the uncorrected path is diagnosable rather than silent.
                        tracing::debug!(
                            target: "mux",
                            "TrueHD channel-correction probe re-open failed: {e}"
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
        // `dir://` is an output-only sink (decrypted file tree); it is never a
        // PES source. Mirror `null://` → write-only.
        StreamUrl::Dir { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
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

/// Open a PES output stream (consumes PES frames).
pub fn output(
    url: &str,
    title: &crate::disc::DiscTitle,
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
        // The bare `output()` arm records the resolver path as the provenance
        // `source.path` and defaults the title index to 0 (the resolver carries
        // no title-index context).
        StreamUrl::Fvi { ref path } => {
            validate_file_path(path, "fvi")?;
            Ok(Box::new(super::fvi_sink::FviSink::create(
                path,
                title,
                path.to_string_lossy().into_owned(),
                0,
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
type DemuxState = (
    Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    Vec<(u16, usize)>,
    Option<super::ts::TsDemuxer>,
    Option<super::ps::PsDemuxer>,
);

/// Build the title's codec parser table + initial `TsDemuxer` /
/// `PsDemuxer`. Used by both the ISO and M2TS pipeline builders.
fn build_demux_state(title: &DiscTitle, format: ContentFormat) -> DemuxState {
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
fn resolve_fmts_key_map(
    reader: &mut dyn SectorSource,
    title: &DiscTitle,
    keys: &mut crate::decrypt::DecryptKeys,
    fetch: Option<&crate::sector::KeyFetch>,
    format: ContentFormat,
    halt: Option<&crate::halt::Halt>,
) -> io::Result<Option<crate::decrypt::AacsKeyMap>> {
    use crate::aacs::content::ALIGNED_UNIT_LEN;
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
    let udf = match crate::udf::read_filesystem(reader) {
        Ok(u) => u,
        Err(crate::error::Error::UdfNotFilesystem) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let tbl = match udf.read_file(reader, "/AACS/IndividualSegment.tbl") {
        Ok(t) => t,
        Err(crate::error::Error::UdfNotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let Some(segments) = parse_individual_segments(&tbl) else {
        return Ok(None);
    };
    if segments.is_empty() {
        return Ok(None);
    }
    // The segment SPNs are in the FORENSIC FEATURE clip's byte space. A title
    // whose extents do not cover any segment's clip bytes carries no forensic
    // content (a menu/extras playlist, or simply a different clip): its base Unit
    // Key/CPS map applies and there is nothing forensic to resolve. Filter to the
    // segments addressable within THIS title; if none, fall back (`Ok(None)`)
    // rather than hard-failing. Without this, `resolve_content_key_map` — which
    // resolves EVERY title for the whole-disc sweep — aborts the entire decrypt on
    // the first non-forensic title (a menu playlist), and `build_iso_pipeline`
    // aborts muxing any non-main title.
    let segments = filter_addressable_segments(segments, &title.extents);
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
    tracing::info!(target: "freemkv::keysource", segments = segments.len(), extents = title.extents.len(), "fmts: begin index-key resolution");

    // Read aligned unit `index` of `seg`: clip byte `start_spn*192 + index*6144`.
    let read_unit =
        |reader: &mut dyn SectorSource, seg: &crate::aacs::segment::Segment, index: usize| {
            let clip_byte = seg.start_spn as u64 * 192 + index as u64 * ALIGNED_UNIT_LEN as u64;
            let lba = clip_byte_to_lba(&title.extents, clip_byte)?;
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

    // Map array position → forensic index (element i = index i+1); add each key to
    // the pool and remember its slot by tag. `base_idx` is the Unit Key (slot 0).
    let base_idx = 0usize;
    let mut tag_slot: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
    if let crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } = keys {
        for (i, k) in index_keys.iter().enumerate() {
            let tag = (i + 1) as u16;
            let slot = match unit_keys.iter().position(|(_, h)| h == k) {
                Some(s) => s,
                None => {
                    let s = unit_keys.len();
                    // CPS-unit id is cosmetic for the mapped decrypt (it indexes by
                    // slot); use a high, distinct number for the forensic keys.
                    unit_keys.push((1000 + s as u32, *k));
                    s
                }
            };
            tag_slot.insert(tag, slot);
        }
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
            &segments,
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
            }
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
        let (Some(a), Some(b)) = (
            clip_byte_to_lba(&title.extents, start_byte),
            clip_byte_to_lba(&title.extents, end_byte - 1),
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

    // Cover the NON-segment content with the base Unit Key: the forensic segments
    // (added above with their index keys) carve holes out of the title's content
    // extents; every other content unit uses the base UK. Fill the gaps so the map
    // is a complete positive list — an LBA in no range is nav and passes through.
    let base_gaps = fill_base_key_gaps(&title.extents, &ranges, base_idx);
    ranges.extend(base_gaps);

    Ok(Some(crate::decrypt::AacsKeyMap::from_ranges_phased(ranges)))
}

/// Keep only the forensic segments addressable within THIS title's extents: a
/// segment whose clip-byte start (`start_spn * 192`) maps to an LBA inside the
/// title is forensic content for this title; one that does not belongs to a
/// different clip (a menu/extras playlist) and is dropped. An empty result means
/// the title carries no forensic content, so [`resolve_fmts_key_map`] returns
/// `Ok(None)` and the caller's base Unit-Key path applies. Extracted from
/// `resolve_fmts_key_map` for direct testing of the inclusion/exclusion decision.
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
    use crate::aacs::content::{
        ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted, decrypt_unit, is_clean,
    };

    // The base Unit Key pool is always resolved and banked by the caller before mux
    // (autorip's pre-rip gate; the ISO path's `decrypt_keys()`), so an AACS title
    // reaches here with a non-empty pool — an empty pool is reported as
    // `DecryptKeys::None` and takes the CSS/clear arm above. `pool_len` is therefore
    // always >= 1 for the AACS map paths below.
    let pool_len = match keys {
        crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => unit_keys.len(),
        // CSS / clear: the AACS map keys nothing here — an empty map passes every
        // unit through (CSS self-descrambles on its own path).
        _ => return Ok(crate::decrypt::AacsKeyMap::from_ranges(Vec::new())),
    };
    // FMTS (AACS 2.1): if the disc carries `IndividualSegment.tbl`, the forensic
    // segments need per-index keys the base Unit Key can't open. Resolve them up
    // front from the configured source and build a per-segment map. Returns `None`
    // when the disc is not FMTS, or no key source is configured (then the base UK
    // path below applies and the forensic units garble → demux drops them).
    if let Some(map) = resolve_fmts_key_map(reader, title, keys, fetch, format, halt)? {
        return Ok(map);
    }
    if pool_len == 1 {
        // One CPS unit → key 0 over every content extent; nav passes through.
        return Ok(content_map(title, 0));
    }

    // Multi-CPS: read a spread of real encrypted units from each extent and pick
    // the held key that opens one (the `is_clean` proof is sound HERE — samples
    // are guaranteed real content, not the authored-bad units that trip the mux).
    let sample_units = |reader: &mut dyn SectorSource, start: u32, sectors: u32| -> Vec<Vec<u8>> {
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
    };
    let pick = |samples: &[Vec<u8>], pool: &[(u32, [u8; 16])]| -> Option<usize> {
        for (i, (_, k)) in pool.iter().enumerate() {
            if samples.iter().any(|s| {
                let mut u = s.clone();
                decrypt_unit(&mut u, k);
                is_clean(&u, format)
            }) {
                return Some(i);
            }
        }
        None
    };

    let mut ranges: Vec<(u32, u32, usize)> = Vec::with_capacity(title.extents.len());
    let mut last_idx = 0usize;
    for ext in &title.extents {
        // Cooperative cancel between extents: multi-CPS sampling reads real
        // content units off the live drive, so honor an operator stop here too.
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(crate::error::Error::Halted.into());
        }
        let samples = sample_units(reader, ext.start_lba, ext.sector_count);
        // Snapshot the current pool for the pure `pick` closure.
        let pool: Vec<(u32, [u8; 16])> = match keys {
            crate::decrypt::DecryptKeys::Aacs { unit_keys, .. } => unit_keys.clone(),
            _ => Vec::new(),
        };
        let mut idx = pick(&samples, &pool);
        if idx.is_none() {
            if let Some(f) = fetch {
                if !samples.is_empty() {
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
            Some(i) => i,
            None if samples.is_empty() => last_idx,
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
    let extents = match &key_map {
        Some(map) => map.read_plan(&extents, unit_align as u32),
        None => extents,
    };
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
        match output(url, t) {
            Ok(_) => panic!("expected output({url}) to error"),
            Err(e) => e.kind(),
        }
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
    /// path; it is a SINK (not a disc source), so `is_disc_source()` is false.
    #[test]
    fn parse_dir_url_is_sink_not_disc_source() {
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
            !parse_url("dir://x").is_disc_source(),
            "dir:// is a sink, never a disc source"
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

    /// `dir://` is output-only: `input()` rejects it (StreamWriteOnly →
    /// Unsupported), and `output()` rejects it too (StreamReadOnly →
    /// Unsupported) because it is NOT a PES sink — the CLI routes it to
    /// `Disc::extract_tree` before the mux path.
    #[test]
    fn dir_url_is_not_a_pes_stream_either_direction() {
        assert_eq!(
            input_err_kind("dir://out/"),
            std::io::ErrorKind::Unsupported
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
        crate::aacs::content::aacs_encrypt_unit_for_test(&mut u, key);
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
        )
        .expect("a structurally non-UDF disc is a clean not-FMTS negative");
        assert!(got.is_none(), "not a UDF/FMTS disc → Ok(None)");
    }
}

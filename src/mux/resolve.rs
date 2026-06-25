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
            StreamUrl::Network { .. } => "network",
            StreamUrl::Stdio => "stdio",
            StreamUrl::Iso { .. } => "iso",
            StreamUrl::Dir { .. } => "dir",
            StreamUrl::Null => "null",
            StreamUrl::Demux { .. } => "demux",
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
            | StreamUrl::Iso { path }
            | StreamUrl::Dir { path }
            | StreamUrl::Demux { dir: path } => path.to_str().unwrap_or(""),
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
#[derive(Debug, Clone, Default)]
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
            // Per-title key resolution. For a multi-VTS CSS DVD the scan's
            // single cracked key only descrambles its own VTS; re-crack from
            // the chosen title's extents if it lives elsewhere. A fresh reader
            // avoids disturbing the mux reader below. 64 sectors is a
            // file-safe batch for an ISO. AACS / single-VTS paths are
            // unchanged (decrypt_keys_for_title short-circuits to decrypt_keys).
            let (keys, title_is_clear) =
                match crate::io::file_sector_source::FileSectorSource::open(path) {
                    Ok(mut crack_reader) => {
                        disc.decrypt_keys_for_title_checked(idx, &mut crack_reader, 64)
                    }
                    Err(_) => (disc.decrypt_keys(), false),
                };
            // Per-title decrypt gate (parallel to the disc-wide gate above): on
            // a multi-VTS CSS disc, the per-title re-crack may return `None` when
            // the chosen title's VTS could not be re-cracked. Muxing that would
            // emit scrambled ciphertext verbatim, so fail loudly here — EXCEPT
            // when the title proved genuinely clear (`title_is_clear`), an
            // unencrypted stub on an otherwise-CSS disc that needs no key. That
            // case must NOT raise a false E7023.
            disc.ensure_title_decryptable(opts.raw, &keys, title_is_clear)
                .map_err(|e| -> io::Error { e.into() })?;
            // Correct TrueHD channel counts (MPLS understates 7.1/Atmos as 5.1)
            // by probing the first DECRYPTED access units of the chosen title.
            // A fresh reader avoids disturbing the mux reader below. Skipped in
            // --raw mode: the probe would re-open + decrypt for nothing (on an
            // AACS disc with no key the correction is a no-op on ciphertext, and
            // raw output isn't decoded anyway).
            if !opts.raw {
                match crate::io::file_sector_source::FileSectorSource::open(path) {
                    Ok(probe) => {
                        let mut dec =
                            crate::sector::DecryptingSectorSource::new(probe, keys.clone());
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
            // sequential read from fast storage, no bad sectors. Measured
            // optimum on the rip1 testbed; bumping to 16384 sectors (32 MiB)
            // regressed (more cache pressure, longer per-batch latency starves
            // the consumer between iterations). Physical drives keep smaller
            // batches for adaptive error handling.
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
            let stream = build_iso_pipeline(
                reader,
                title,
                effective_keys,
                ISO_MUX_BATCH_SECTORS,
                format,
                None,
                None,
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
        // `demux://` is an output-only sink (per-track ES files); never a source.
        StreamUrl::Demux { .. } => Err(crate::error::Error::StreamWriteOnly.into()),
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
            Ok(Box::new(MkvStream::create_at(writer, title, Some(path))?))
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
            Ok(Box::new(super::demux_sink::DemuxSink::create(
                dir,
                title,
                &super::demux_sink::DemuxOptions::default(),
            )?))
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
        parsers.push((pid, super::codec::parser_for_codec(codec, None, is_dvd_ps)));
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
/// - `halt`: cooperative cancel token (not a timeout); when cancelled the
///   pipeline stops at the next boundary. `None` disables cancellation.
/// - `event_fn`: optional progress/event callback invoked by the prefetcher.
pub fn build_iso_pipeline<S: SectorSource + Send + 'static>(
    reader: S,
    title: DiscTitle,
    keys: crate::decrypt::DecryptKeys,
    batch_sectors: u16,
    format: ContentFormat,
    halt: Option<crate::halt::Halt>,
    event_fn: Option<crate::sector::prefetched::EventFn>,
) -> io::Result<PipelinedPesStream> {
    let extents = title.extents.clone();
    // Unit alignment is an AACS concept: AACS decrypts whole 6144-byte (3-sector)
    // units, so the producer must hand the decrypt step 3-sector-aligned batches.
    // CSS (DVD) and unencrypted content decrypt per 2048-byte sector — forcing
    // 3-sector alignment there rejects any extent whose sector count isn't a
    // multiple of 3 (DVD IFO cells routinely aren't) with ExtentNotUnitAligned.
    let unit_align: u16 = match &keys {
        crate::decrypt::DecryptKeys::Aacs { .. } => 3,
        _ => 1,
    };
    let mut decrypting =
        crate::sector::DecryptingSectorSource::new(Box::new(reader) as Box<dyn SectorSource>, keys);
    // Grab the decrypt-loss counter before the decorator is moved into the
    // producer thread. It tracks bytes of scrambled AACS units no key could
    // decrypt — silent loss the demux drops; the consuming stream surfaces it
    // through `lost_bytes()` so the mux abort gate sees a partial decrypt
    // failure rather than a clean rip.
    let decrypt_loss = decrypting.decrypt_loss();

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
    Ok(
        PipelinedPesStream::new(demux_thread, demux_rx, title, parsers, pid_to_track)
            .with_decrypt_loss(decrypt_loss),
    )
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
            None,
            None,
        );
        assert!(res.is_err(), "zero batch_sectors must be rejected");
    }
}

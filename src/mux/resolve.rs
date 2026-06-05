//! Stream URL resolver — parses URL strings into PES stream instances.
//!
//! Format: `scheme://path`
//!
//! | Scheme | Input | Output | Path |
//! |--------|-------|--------|------|
//! | disc:// | Yes | -- | empty (auto-detect) or /dev/sgN |
//! | iso://  | Yes | -- | file path (required) |
//! | mkv://  | Yes | Yes | file path (required) |
//! | m2ts:// | Yes | Yes | file path (required) |
//! | network:// | Yes (listen) | Yes (connect) | host:port (required) |
//! | stdio:// | Yes (stdin) | Yes (stdout) | empty |
//! | null:// | -- | Yes | empty |
//!
//! Bare paths without a scheme are rejected.
//! For disc→ISO (raw sector copy), use `Disc::copy()` instead.

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
    /// Null sink (write-only, discards data).
    Null,
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
            StreamUrl::Null => "null",
            StreamUrl::Unknown { .. } => "unknown",
        }
    }

    /// The path/address component, or empty string for scheme-only URLs.
    pub fn path_str(&self) -> &str {
        match self {
            StreamUrl::Disc { device: Some(p) } => p.to_str().unwrap_or(""),
            StreamUrl::Disc { device: None } => "",
            StreamUrl::M2ts { path } | StreamUrl::Mkv { path } | StreamUrl::Iso { path } => {
                path.to_str().unwrap_or("")
            }
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
    if let Some(rest) = url.strip_prefix("disc://") {
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
    if url == "null://" || url.starts_with("null://") {
        return StreamUrl::Null;
    }
    if url == "stdio://" || url.starts_with("stdio://") {
        return StreamUrl::Stdio;
    }
    if let Some(rest) = url.strip_prefix("iso://") {
        return StreamUrl::Iso {
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
    if !addr.contains(':') {
        return Err(crate::error::Error::StreamUrlMissingPort {
            addr: addr.to_string(),
        }
        .into());
    }
    Ok(())
}

/// Options for opening an input stream.
#[derive(Default)]
pub struct InputOptions {
    /// Caller-resolved per-CPS-unit AACS keys to apply to the scanned disc
    /// (`(cps_unit, 16-byte key)`). Empty for an unencrypted disc or when the
    /// caller has no key. The library does no lookup — a key source resolves
    /// these and the caller passes them here.
    pub unit_keys: Vec<(u32, [u8; 16])>,
    pub title_index: Option<usize>,
    /// Skip decryption — return raw encrypted bytes.
    pub raw: bool,
}

/// Decide whether an ISO mux must abort for lack of a usable AACS key.
///
/// Returns `true` only when ALL hold: decryption is requested (`!raw`), the
/// disc carries AACS state (`has_aacs` — AACS-encrypted, not CSS/unencrypted),
/// and key resolution produced no usable key (`keys` is
/// [`crate::decrypt::DecryptKeys::None`]). In that case muxing would emit
/// undecryptable garbage, so the caller fails fast with [`Error::NoDiscKey`].
///
/// `--raw` (raw=true) always returns `false` — raw intentionally skips
/// decryption and needs no key. A non-AACS disc (`has_aacs=false`) always
/// returns `false`: unencrypted content has `None` keys legitimately, and CSS
/// DVDs resolve to `DecryptKeys::Css{..}` (never `None`).
fn aacs_key_missing(raw: bool, has_aacs: bool, keys: &crate::decrypt::DecryptKeys) -> bool {
    !raw && has_aacs && matches!(keys, crate::decrypt::DecryptKeys::None)
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
            // No-key guard: if decryption is requested (not --raw) and the disc
            // is AACS-encrypted but key resolution yielded no usable key, FAIL
            // here — muxing an undecryptable stream produces ~100 MB of garbage
            // (encrypted m2ts → no TS syncs → demuxer emits nothing). A cheap
            // result-check on `decrypt_keys()`; no probe decryption needed.
            // CSS (DVD) decrypts from compiled keys (`decrypt_keys()` returns
            // `Css{..}`, never `None`), so this gate is AACS-only via `disc.aacs`.
            if aacs_key_missing(opts.raw, disc.aacs.is_some(), &disc.decrypt_keys()) {
                // Surface the disc hash (40-hex, no `0x` prefix) so the caller
                // can name the disc. Empty if scan didn't capture it.
                let disc_hash = disc
                    .aacs
                    .as_ref()
                    .map(|a| a.disc_hash.trim_start_matches("0x").to_string())
                    .unwrap_or_default();
                return Err(crate::error::Error::NoDiscKey { disc_hash }.into());
            }
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
            // Correct TrueHD channel counts (MPLS understates 7.1/Atmos as 5.1)
            // by probing the first DECRYPTED access units of the chosen title.
            // A fresh reader avoids disturbing the mux reader below.
            let keys = disc.decrypt_keys();
            if let Ok(probe) = crate::io::file_sector_source::FileSectorSource::open(path) {
                let mut dec = crate::sector::DecryptingSectorSource::new(probe, keys.clone());
                crate::disc::correct_truehd_channels(&mut dec, &mut disc.titles[idx]);
            }
            let title = disc.titles[idx].clone();
            let format = disc.content_format;
            // ISO file: 16 MiB batch — sequential read from fast
            // storage, no bad sectors. Measured optimum on the rip1
            // testbed; bumping to 32 MiB regressed (more cache
            // pressure, longer per-batch latency starves the consumer
            // between iterations). Physical drives keep smaller
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
            );
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
            Ok(Box::new(MkvStream::create(writer, title)?))
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
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::connect(addr)?.meta(title)))
        }
        StreamUrl::Stdio => Ok(Box::new(StdioStream::output(title))),
        StreamUrl::Null => Ok(Box::new(NullStream::new(title))),
        StreamUrl::Disc { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        StreamUrl::Iso { .. } => Err(crate::error::Error::StreamReadOnly.into()),
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
pub fn build_iso_pipeline<S: SectorSource + Send + 'static>(
    reader: S,
    title: DiscTitle,
    keys: crate::decrypt::DecryptKeys,
    batch_sectors: u16,
    format: ContentFormat,
    halt: Option<crate::halt::Halt>,
    event_fn: Option<crate::sector::prefetched::EventFn>,
) -> PipelinedPesStream {
    let extents = title.extents.clone();
    let decrypting =
        crate::sector::DecryptingSectorSource::new(Box::new(reader) as Box<dyn SectorSource>, keys);
    let prefetched = crate::sector::PrefetchedSectorSource::new_with_events(
        decrypting,
        extents,
        batch_sectors,
        halt.clone(),
        event_fn,
    );
    let (rx, recycle_tx, shell) = prefetched.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, format);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, halt, ts, ps);
    PipelinedPesStream::new(demux_thread, demux_rx, title, parsers, pid_to_track)
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

    // Try FMKV metadata header first; fall back to PMT scan.
    let mut cursor = io::Cursor::new(&head);
    let (title, head_consumed) = if let Ok(Some(m)) = meta::read_header(&mut cursor) {
        (m.to_title(), cursor.position() as usize)
    } else {
        let streams = super::ts::scan_streams(&head)
            .ok_or_else(|| -> io::Error { crate::error::Error::NoStreams.into() })?;
        let t = DiscTitle {
            duration_secs: 0.0,
            streams,
            ..DiscTitle::empty()
        };
        (t, 0)
    };

    // Chain: any un-consumed head bytes + the remainder of the
    // reader. The demuxer sees a contiguous M2TS byte stream.
    let remaining_head = head[head_consumed..].to_vec();
    let chained: Box<dyn Read + Send> = Box::new(io::Cursor::new(remaining_head).chain(reader));

    let prefetcher = crate::io::byte_prefetcher::BytePrefetcher::new(
        chained,
        crate::io::byte_prefetcher::DEFAULT_CHUNK_BYTES,
        None,
    );
    let (rx, recycle_tx, shell) = prefetcher.into_channels();

    let (parsers, pid_to_track, ts, ps) = build_demux_state(&title, ContentFormat::BdTs);
    let (demux_thread, demux_rx) =
        super::demux_thread::DemuxThread::spawn_zero_copy(rx, recycle_tx, shell, None, ts, ps);
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
    use super::aacs_key_missing;
    use crate::decrypt::DecryptKeys;

    fn aacs_keys() -> DecryptKeys {
        DecryptKeys::Aacs {
            unit_keys: vec![(1, [0x11u8; 16])],
            read_data_key: None,
        }
    }

    fn css_keys() -> DecryptKeys {
        DecryptKeys::Css {
            title_key: [0u8; 5],
        }
    }

    #[test]
    fn encrypted_no_key_aborts() {
        // AACS disc, decryption requested, resolver yielded no key → abort.
        assert!(aacs_key_missing(false, true, &DecryptKeys::None));
    }

    #[test]
    fn encrypted_with_key_proceeds() {
        // AACS disc with a usable key → proceed.
        assert!(!aacs_key_missing(false, true, &aacs_keys()));
    }

    #[test]
    fn not_encrypted_proceeds() {
        // No AACS state: unencrypted (None keys) and CSS (Css keys) both OK.
        assert!(!aacs_key_missing(false, false, &DecryptKeys::None));
        assert!(!aacs_key_missing(false, false, &css_keys()));
    }

    #[test]
    fn raw_never_aborts() {
        // --raw skips decryption — must never hit the no-key abort, even on an
        // AACS disc with no key resolved.
        assert!(!aacs_key_missing(true, true, &DecryptKeys::None));
        assert!(!aacs_key_missing(true, true, &aacs_keys()));
        assert!(!aacs_key_missing(true, false, &DecryptKeys::None));
    }
}

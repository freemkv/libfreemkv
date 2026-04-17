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

use super::disc::DiscStream;
use super::network::NetworkStream;
use super::null::NullStream;
use super::stdio::StdioStream;
use super::{M2tsStream, MkvStream};
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
    pub keydb_path: Option<String>,
    pub title_index: Option<usize>,
    /// Skip decryption — return raw encrypted bytes.
    pub raw: bool,
}

/// Open a PES input stream (produces PES frames).
pub fn input(url: &str, opts: &InputOptions) -> io::Result<Box<dyn crate::pes::Stream>> {
    let parsed = parse_url(url);
    match parsed {
        StreamUrl::Disc { .. } => {
            // Disc sources should use DiscStream::new() directly.
            // The caller opens the drive, inits, scans, then creates the stream.
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Use Drive::open() + Disc::scan() + DiscStream::new() for disc sources",
            ))
        }
        StreamUrl::Iso { ref path } => {
            validate_file_path(path, "iso")?;
            let scan_opts = match &opts.keydb_path {
                Some(p) => crate::disc::ScanOptions::with_keydb(p),
                None => crate::disc::ScanOptions::default(),
            };
            let mut reader = super::iso::IsoSectorReader::open(&path.to_string_lossy())?;
            let capacity = reader.capacity();
            let disc = crate::disc::Disc::scan_image(&mut reader, capacity, &scan_opts)
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
            let title = disc.titles[idx].clone();
            let keys = disc.decrypt_keys();
            let format = disc.content_format;
            let mut stream = DiscStream::new(Box::new(reader), title, keys, 64, format);
            if opts.raw {
                stream.set_raw();
            }
            Ok(Box::new(stream))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let file = std::fs::File::open(path).map_err(|e| {
                io::Error::new(e.kind(), format!("m2ts://{}: {}", path.display(), e))
            })?;
            let reader = std::io::BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::open(reader)?))
        }
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            let file = std::fs::File::open(path).map_err(|e| {
                io::Error::new(e.kind(), format!("mkv://{}: {}", path.display(), e))
            })?;
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
            let file = std::fs::File::create(path).map_err(|e| {
                io::Error::new(e.kind(), format!("mkv://{}: {}", path.display(), e))
            })?;
            let writer: Box<dyn super::WriteSeek> =
                Box::new(std::io::BufWriter::with_capacity(IO_BUF_SIZE, file));
            Ok(Box::new(MkvStream::create(writer, title)?))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let file = std::fs::File::create(path).map_err(|e| {
                io::Error::new(e.kind(), format!("m2ts://{}: {}", path.display(), e))
            })?;
            let writer = std::io::BufWriter::with_capacity(IO_BUF_SIZE, file);
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

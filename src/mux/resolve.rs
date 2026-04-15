//! Stream URL resolver — parses URL strings into IOStream instances.
//!
//! Format: `scheme://path`
//!
//! | Scheme | Input | Output | Path |
//! |--------|-------|--------|------|
//! | disc:// | Yes | -- | empty (auto-detect) or /dev/sgN |
//! | m2ts:// | Yes | Yes | file path (required) |
//! | mkv://  | Yes | Yes | file path (required) |
//! | network:// | Yes (listen) | Yes (connect) | host:port (required) |
//! | stdio:// | Yes (stdin) | Yes (stdout) | empty |
//! | iso://   | Yes | -- | file path (required) |
//! | null:// | -- | Yes | empty |
//!
//! Bare paths without a scheme are rejected.

use super::disc::DiscStream;
use super::iso::IsoStream;
use super::network::NetworkStream;
use super::null::NullStream;
use super::stdio::StdioStream;
use super::{IOStream, M2tsStream, MkvStream};
use crate::disc::DiscTitle;
use std::io::{self, BufReader, BufWriter};
use std::path::{Path, PathBuf};

/// I/O buffer size for file streams.
const IO_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Default MKV lookahead buffer size.
/// Dynamically increased for UHD content (many streams delay video codec headers).
const MKV_LOOKAHEAD_DEFAULT: usize = 10 * 1024 * 1024;
const MKV_LOOKAHEAD_UHD: usize = 100 * 1024 * 1024;

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
///
/// All URLs must use the `scheme://path` format. Bare paths are not supported.
///
/// ```text
/// disc://              → Disc { device: None }
/// disc:///dev/sg4      → Disc { device: Some("/dev/sg4") }
/// m2ts:///tmp/Dune.m2ts → M2ts { path: "/tmp/Dune.m2ts" }
/// mkv://Dune.mkv       → Mkv { path: "Dune.mkv" }
/// network://10.0.0.1:9000 → Network { addr: "10.0.0.1:9000" }
/// null://              → Null
/// ```
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{scheme}:// requires a file path (e.g. {scheme}://movie.{scheme})"),
        ));
    }
    if path.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{scheme}://{} is not a valid file path — must include a filename",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// Validate that a network address has host:port format.
fn validate_network_addr(addr: &str) -> io::Result<()> {
    if addr.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "network:// requires host:port (e.g. network://0.0.0.0:9000)",
        ));
    }
    if !addr.contains(':') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("network://{addr} missing port — use network://{addr}:PORT"),
        ));
    }
    Ok(())
}

/// Open a stream URL for reading (source).
pub fn open_input(url: &str, opts: &InputOptions) -> io::Result<Box<dyn IOStream>> {
    let parsed = parse_url(url);

    match parsed {
        StreamUrl::Disc { device } => {
            let result = DiscStream::open(
                device.as_deref(),
                opts.keydb_path.as_deref(),
                opts.title_index.unwrap_or(0),
                None,
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
            let mut stream = result.stream;
            if opts.raw {
                stream.set_raw();
            }
            Ok(Box::new(stream))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let file = std::fs::File::open(path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("m2ts://{}: {}", path.display(), e)))?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::open(reader)?))
        }
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            let file = std::fs::File::open(path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("mkv://{}: {}", path.display(), e)))?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(MkvStream::open(reader)?))
        }
        StreamUrl::Network { ref addr } => {
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::listen(addr)?))
        }
        StreamUrl::Stdio => {
            Ok(Box::new(StdioStream::input()))
        }
        StreamUrl::Iso { ref path } => {
            validate_file_path(path, "iso")?;
            let scan_opts = match &opts.keydb_path {
                Some(p) => crate::disc::ScanOptions::with_keydb(p),
                None => crate::disc::ScanOptions::default(),
            };
            let mut stream = IsoStream::open(&path.to_string_lossy(), opts.title_index, &scan_opts)?;
            if opts.raw {
                stream.set_raw();
            }
            Ok(Box::new(stream))
        }
        StreamUrl::Null => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                "null:// is write-only — cannot use as input"))
        }
        StreamUrl::Unknown { ref raw } => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("'{}' is not a valid stream URL — use scheme://path (e.g. mkv://movie.mkv, disc://, m2ts://movie.m2ts)", raw)))
        }
    }
}

/// Open a stream URL for writing (destination).
pub fn open_output(url: &str, meta: &DiscTitle) -> io::Result<Box<dyn IOStream>> {
    let parsed = parse_url(url);

    match parsed {
        StreamUrl::Disc { .. } => {
            Err(io::Error::new(io::ErrorKind::Unsupported,
                "disc:// is read-only — cannot use as output"))
        }
        StreamUrl::Iso { ref path } => {
            validate_file_path(path, "iso")?;
            Ok(Box::new(IsoStream::create(&path.to_string_lossy())?.meta(meta)))
        }
        StreamUrl::Null => {
            Ok(Box::new(NullStream::new().meta(meta)))
        }
        StreamUrl::Stdio => {
            Ok(Box::new(StdioStream::output().meta(meta)))
        }
        StreamUrl::M2ts { ref path } => {
            validate_file_path(path, "m2ts")?;
            let file = std::fs::File::create(path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("m2ts://{}: {}", path.display(), e)))?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::new(writer).meta(meta)))
        }
        StreamUrl::Mkv { ref path } => {
            validate_file_path(path, "mkv")?;
            let file = std::fs::File::create(path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("mkv://{}: {}", path.display(), e)))?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            let lookahead = if meta.streams.len() > 15 {
                MKV_LOOKAHEAD_UHD
            } else {
                MKV_LOOKAHEAD_DEFAULT
            };
            Ok(Box::new(MkvStream::new(writer).meta(meta).max_buffer(lookahead)))
        }
        StreamUrl::Network { ref addr } => {
            validate_network_addr(addr)?;
            Ok(Box::new(NetworkStream::connect(addr)?.meta(meta)))
        }
        StreamUrl::Unknown { ref raw } => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("'{}' is not a valid stream URL — use scheme://path (e.g. mkv://movie.mkv, m2ts://movie.m2ts, null://)", raw)))
        }
    }
}

/// Options for opening an input stream.
#[derive(Default)]
pub struct InputOptions {
    pub keydb_path: Option<String>,
    pub title_index: Option<usize>,
    /// Skip decryption — return raw encrypted bytes.
    pub raw: bool,
}

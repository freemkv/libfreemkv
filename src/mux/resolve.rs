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

use super::disc::{DiscOptions, DiscStream};
use super::iso::IsoStream;
use super::network::NetworkStream;
use super::null::NullStream;
use super::stdio::StdioStream;
use super::{IOStream, M2tsStream, MkvStream};
use crate::disc::DiscTitle;
use std::io::{self, BufReader, BufWriter};
use std::path::Path;

/// I/O buffer size for file streams.
const IO_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Default MKV lookahead buffer size.
/// Dynamically increased for UHD content (many streams delay video codec headers).
const MKV_LOOKAHEAD_DEFAULT: usize = 10 * 1024 * 1024;
const MKV_LOOKAHEAD_UHD: usize = 100 * 1024 * 1024;

/// Parsed stream URL.
pub struct StreamUrl {
    pub scheme: String,
    pub path: String,
}

/// Parse a URL string into scheme + path.
///
/// All URLs must use the `scheme://path` format. Bare paths are not supported.
///
/// ```text
/// disc://              → scheme="disc",    path=""
/// disc:///dev/sg4      → scheme="disc",    path="/dev/sg4"
/// m2ts:///tmp/Dune.m2ts → scheme="m2ts",   path="/tmp/Dune.m2ts"
/// mkv://Dune.mkv       → scheme="mkv",     path="Dune.mkv"
/// network://10.0.0.1:9000 → scheme="network", path="10.0.0.1:9000"
/// null://              → scheme="null",    path=""
/// ```
pub fn parse_url(url: &str) -> StreamUrl {
    if let Some(rest) = url.strip_prefix("disc://") {
        return StreamUrl {
            scheme: "disc".into(),
            path: rest.to_string(),
        };
    }
    if let Some(rest) = url.strip_prefix("m2ts://") {
        return StreamUrl {
            scheme: "m2ts".into(),
            path: rest.to_string(),
        };
    }
    if let Some(rest) = url.strip_prefix("mkv://") {
        return StreamUrl {
            scheme: "mkv".into(),
            path: rest.to_string(),
        };
    }
    if let Some(rest) = url.strip_prefix("network://") {
        return StreamUrl {
            scheme: "network".into(),
            path: rest.to_string(),
        };
    }
    if url == "null://" || url.starts_with("null://") {
        return StreamUrl {
            scheme: "null".into(),
            path: String::new(),
        };
    }
    if url == "stdio://" || url.starts_with("stdio://") {
        return StreamUrl {
            scheme: "stdio".into(),
            path: String::new(),
        };
    }
    if let Some(rest) = url.strip_prefix("iso://") {
        return StreamUrl {
            scheme: "iso".into(),
            path: rest.to_string(),
        };
    }

    StreamUrl {
        scheme: "unknown".into(),
        path: url.to_string(),
    }
}

/// Validate that a file path is non-empty and has a filename component.
fn validate_file_path(path: &str, scheme: &str) -> io::Result<()> {
    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{scheme}:// requires a file path (e.g. {scheme}://movie.{scheme})"
            ),
        ));
    }
    let p = Path::new(path);
    if p.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{scheme}://{path} is not a valid file path — must include a filename"
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
            format!(
                "network://{addr} missing port — use network://{addr}:PORT"
            ),
        ));
    }
    Ok(())
}

/// Open a stream URL for reading (source).
pub fn open_input(url: &str, opts: &InputOptions) -> io::Result<Box<dyn IOStream>> {
    let parsed = parse_url(url);

    match parsed.scheme.as_str() {
        "disc" => {
            let disc_opts = DiscOptions {
                device: if parsed.path.is_empty() { None } else { Some(parsed.path) },
                keydb_path: opts.keydb_path.clone(),
                title_index: opts.title_index,
            };
            let stream = DiscStream::open(disc_opts)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(Box::new(stream))
        }
        "m2ts" => {
            validate_file_path(&parsed.path, "m2ts")?;
            let file = std::fs::File::open(&parsed.path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("m2ts://{}: {}", parsed.path, e)))?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::open(reader)?))
        }
        "mkv" => {
            validate_file_path(&parsed.path, "mkv")?;
            let file = std::fs::File::open(&parsed.path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("mkv://{}: {}", parsed.path, e)))?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(MkvStream::open(reader)?))
        }
        "network" => {
            validate_network_addr(&parsed.path)?;
            Ok(Box::new(NetworkStream::listen(&parsed.path)?))
        }
        "stdio" => {
            Ok(Box::new(StdioStream::input()))
        }
        "iso" => {
            validate_file_path(&parsed.path, "iso")?;
            let scan_opts = match &opts.keydb_path {
                Some(p) => crate::disc::ScanOptions::with_keydb(p),
                None => crate::disc::ScanOptions::default(),
            };
            Ok(Box::new(IsoStream::open(&parsed.path, opts.title_index, &scan_opts)?))
        }
        "null" => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                "null:// is write-only — cannot use as input"))
        }
        "unknown" => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("'{}' is not a valid stream URL — use scheme://path (e.g. mkv://movie.mkv, disc://, m2ts://movie.m2ts)", parsed.path)))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput,
            format!("unknown scheme: {}://", parsed.scheme))),
    }
}

/// Open a stream URL for writing (destination).
pub fn open_output(url: &str, meta: &DiscTitle) -> io::Result<Box<dyn IOStream>> {
    let parsed = parse_url(url);

    match parsed.scheme.as_str() {
        "disc" => {
            Err(io::Error::new(io::ErrorKind::Unsupported,
                "disc:// is read-only — cannot use as output"))
        }
        "iso" => {
            validate_file_path(&parsed.path, "iso")?;
            Ok(Box::new(IsoStream::create(&parsed.path)?.meta(meta)))
        }
        "null" => {
            Ok(Box::new(NullStream::new().meta(meta)))
        }
        "stdio" => {
            Ok(Box::new(StdioStream::output().meta(meta)))
        }
        "m2ts" => {
            validate_file_path(&parsed.path, "m2ts")?;
            let file = std::fs::File::create(&parsed.path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("m2ts://{}: {}", parsed.path, e)))?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::new(writer).meta(meta)))
        }
        "mkv" => {
            validate_file_path(&parsed.path, "mkv")?;
            let file = std::fs::File::create(&parsed.path)
                .map_err(|e| io::Error::new(e.kind(),
                    format!("mkv://{}: {}", parsed.path, e)))?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            // Size lookahead based on content: UHD (many streams) needs larger buffer
            // because HEVC SPS/PPS may not appear until well past 10 MB
            let lookahead = if meta.streams.len() > 15 {
                MKV_LOOKAHEAD_UHD
            } else {
                MKV_LOOKAHEAD_DEFAULT
            };
            Ok(Box::new(MkvStream::new(writer).meta(meta).max_buffer(lookahead)))
        }
        "network" => {
            validate_network_addr(&parsed.path)?;
            Ok(Box::new(NetworkStream::connect(&parsed.path)?.meta(meta)))
        }
        "unknown" => {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("'{}' is not a valid stream URL — use scheme://path (e.g. mkv://movie.mkv, m2ts://movie.m2ts, null://)", parsed.path)))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput,
            format!("unknown scheme: {}://", parsed.scheme))),
    }
}

/// Options for opening an input stream.
#[derive(Default)]
pub struct InputOptions {
    pub keydb_path: Option<String>,
    pub title_index: Option<usize>,
}

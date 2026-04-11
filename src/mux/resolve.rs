//! Stream URL resolver — parses URL strings into IOStream instances.
//!
//! Schemes: disc://, m2ts://, mkv://, network://
//! Bare paths infer scheme from file extension.

use std::io::{self, BufReader, BufWriter};
use super::{IOStream, M2tsStream, MkvStream};
use super::network::NetworkStream;
use super::null::NullStream;
use super::disc::{DiscStream, DiscOptions};
use crate::disc::DiscTitle;

/// I/O buffer size for file streams.
const IO_BUF_SIZE: usize = 4 * 1024 * 1024;

/// MKV lookahead buffer size.
const MKV_LOOKAHEAD: usize = 10 * 1024 * 1024;

/// Parsed stream URL.
pub struct StreamUrl {
    pub scheme: String,
    pub path: String,
}

/// Parse a URL string into scheme + path.
///
/// Supports: `disc://`, `disc:///dev/sg4`, `m2ts://path`, `mkv://path`,
/// `network://host:port`, or bare paths (infer from extension).
pub fn parse_url(url: &str) -> StreamUrl {
    if let Some(rest) = url.strip_prefix("disc://") {
        return StreamUrl { scheme: "disc".into(), path: rest.to_string() };
    }
    if let Some(rest) = url.strip_prefix("m2ts://") {
        return StreamUrl { scheme: "m2ts".into(), path: rest.to_string() };
    }
    if let Some(rest) = url.strip_prefix("mkv://") {
        return StreamUrl { scheme: "mkv".into(), path: rest.to_string() };
    }
    if let Some(rest) = url.strip_prefix("network://") {
        return StreamUrl { scheme: "network".into(), path: rest.to_string() };
    }
    if url == "null://" || url.starts_with("null://") {
        return StreamUrl { scheme: "null".into(), path: String::new() };
    }

    // Infer from extension
    let scheme = if url.ends_with(".m2ts") {
        "m2ts"
    } else if url.ends_with(".mkv") {
        "mkv"
    } else {
        "m2ts" // default
    };

    StreamUrl { scheme: scheme.into(), path: url.to_string() }
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            Ok(Box::new(stream))
        }
        "m2ts" => {
            let file = std::fs::File::open(&parsed.path)?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::open(reader)?))
        }
        "mkv" => {
            let file = std::fs::File::open(&parsed.path)?;
            let reader = BufReader::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(MkvStream::open(reader)?))
        }
        "network" => {
            Ok(Box::new(NetworkStream::listen(&parsed.path)?))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput,
            format!("unknown scheme: {}", parsed.scheme))),
    }
}

/// Open a stream URL for writing (destination).
pub fn open_output(url: &str, meta: &DiscTitle) -> io::Result<Box<dyn IOStream>> {
    let parsed = parse_url(url);

    match parsed.scheme.as_str() {
        "disc" => {
            Err(io::Error::new(io::ErrorKind::Unsupported, "disc is read-only"))
        }
        "null" => {
            Ok(Box::new(NullStream::new().meta(meta)))
        }
        "m2ts" => {
            let file = std::fs::File::create(&parsed.path)?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(M2tsStream::new(writer).meta(meta)))
        }
        "mkv" => {
            let file = std::fs::File::create(&parsed.path)?;
            let writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
            Ok(Box::new(MkvStream::new(writer).meta(meta).max_buffer(MKV_LOOKAHEAD)))
        }
        "network" => {
            Ok(Box::new(NetworkStream::connect(&parsed.path)?.meta(meta)))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidInput,
            format!("unknown scheme: {}", parsed.scheme))),
    }
}

/// Options for opening an input stream.
pub struct InputOptions {
    pub keydb_path: Option<String>,
    pub title_index: Option<usize>,
}

impl Default for InputOptions {
    fn default() -> Self {
        Self { keydb_path: None, title_index: None }
    }
}

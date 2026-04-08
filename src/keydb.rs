//! KEYDB.cfg updater — HTTP GET, unzip, verify, save.
//!
//! Zero external HTTP dependencies. Raw TCP for HTTP GET.
//! Uses `zip` and `flate2` (already in deps) for extraction.

use crate::error::{Error, Result};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;

/// Standard keydb storage path.
pub fn default_path() -> Result<PathBuf> {
    let home = std::env::var("HOME").map_err(|_| Error::KeydbWrite {
        path: "HOME".into(),
    })?;
    Ok(PathBuf::from(home).join(".config").join("freemkv").join("keydb.cfg"))
}

/// Download a KEYDB from a URL, verify, save to the standard path.
pub fn update(url: &str) -> Result<UpdateResult> {
    let body = http_get(url)?;
    save(&body)
}

/// Verify and save raw keydb bytes (plain text, .zip, or .gz).
pub fn save(data: &[u8]) -> Result<UpdateResult> {
    let text = if data.starts_with(b"PK\x03\x04") {
        extract_zip(data)?
    } else if data.starts_with(&[0x1f, 0x8b]) {
        let mut dec = flate2::read::GzDecoder::new(data);
        let mut out = String::new();
        dec.read_to_string(&mut out).map_err(|_| Error::KeydbParse)?;
        out
    } else {
        String::from_utf8(data.to_vec()).map_err(|_| Error::KeydbParse)?
    };

    let entries = text.lines()
        .filter(|l| {
            let t = l.trim();
            t.starts_with("0x") || t.starts_with("| DK") || t.starts_with("| PK") || t.starts_with("| HC")
        })
        .count();

    if entries == 0 {
        return Err(Error::KeydbInvalid);
    }

    let path = default_path()?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|_| Error::KeydbWrite {
            path: path.display().to_string(),
        })?;
    }
    std::fs::write(&path, &text).map_err(|_| Error::KeydbWrite {
        path: path.display().to_string(),
    })?;

    Ok(UpdateResult { path, entries, bytes: text.len() })
}

#[derive(Debug)]
pub struct UpdateResult {
    pub path: PathBuf,
    pub entries: usize,
    pub bytes: usize,
}

fn http_get(url: &str) -> Result<Vec<u8>> {
    let (host, port, path) = parse_url(url)?;

    for _ in 0..5 {
        let addr = format!("{}:{}", host, port);
        let mut stream = TcpStream::connect(&addr).map_err(|_| Error::KeydbConnect {
            host: host.clone(),
        })?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(30))).ok();

        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept-Encoding: identity\r\n\r\n",
            path, host
        );
        stream.write_all(request.as_bytes()).map_err(|_| Error::KeydbConnect {
            host: host.clone(),
        })?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).map_err(|_| Error::KeydbConnect {
            host: host.clone(),
        })?;

        let header_end = find_header_end(&response).ok_or(Error::KeydbParse)?;
        let headers = std::str::from_utf8(&response[..header_end]).unwrap_or("");
        let body = &response[header_end + 4..];

        if let Some(location) = extract_header(headers, "Location") {
            return http_get(&location);
        }

        let status = parse_status(headers);
        if status != 200 {
            return Err(Error::KeydbHttp { status });
        }

        return Ok(body.to_vec());
    }

    Err(Error::KeydbHttp { status: 302 })
}

fn parse_url(url: &str) -> Result<(String, u16, String)> {
    let url = url.strip_prefix("http://").ok_or(Error::KeydbParse)?;
    let (host_port, path) = match url.find('/') {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, "/"),
    };
    let (host, port) = match host_port.find(':') {
        Some(i) => (&host_port[..i], host_port[i+1..].parse().unwrap_or(80)),
        None => (host_port, 80u16),
    };
    Ok((host.to_string(), port, path.to_string()))
}

fn parse_status(headers: &str) -> u16 {
    headers.lines().next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

fn extract_header<'a>(headers: &'a str, name: &str) -> Option<String> {
    for line in headers.lines() {
        if line.len() > name.len() + 2
            && line[..name.len()].eq_ignore_ascii_case(name)
            && line.as_bytes()[name.len()] == b':'
        {
            return Some(line[name.len() + 1..].trim().to_string());
        }
    }
    None
}

fn extract_zip(data: &[u8]) -> Result<String> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|_| Error::KeydbParse)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).map_err(|_| Error::KeydbParse)?;
        if file.name().ends_with(".cfg") || file.name().ends_with(".CFG") {
            let mut text = String::new();
            file.read_to_string(&mut text).map_err(|_| Error::KeydbParse)?;
            return Ok(text);
        }
    }

    Err(Error::KeydbInvalid)
}

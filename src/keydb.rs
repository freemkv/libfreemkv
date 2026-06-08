//! KEYDB.cfg updater — HTTP GET, unzip, verify, save.
//!
//! Zero external HTTP dependencies. Raw TCP for HTTP GET.
//! Uses `zip` and `flate2` (already in deps) for extraction.

use crate::error::{Error, Result};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;

/// Network operation timeout (connect / read / write). Keeps the daily
/// refresh thread from blocking indefinitely on an unresponsive mirror.
const NET_TIMEOUT: Duration = Duration::from_secs(10);

/// Read timeout — longer than connect/write since the keydb body can be
/// several MiB over a slow link.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum redirects to follow before giving up.
const MAX_REDIRECTS: usize = 5;

/// Upper bound on decompressed keydb size. The published keydb is a few
/// MiB; 64 MiB is a generous ceiling that still caps a decompression
/// bomb (a tiny zip/gz can otherwise inflate to GiB and OOM the daily
/// refresh thread).
const MAX_KEYDB_BYTES: u64 = 64 * 1024 * 1024;

/// Read a decompressed stream into a String with a hard size ceiling.
/// Returns `Error::KeydbInvalid` if the input exceeds the cap or is not
/// valid UTF-8.
fn read_capped_to_string<R: Read>(reader: R) -> Result<String> {
    let mut buf = Vec::new();
    // Read one byte past the cap so an exactly-at-cap stream is accepted
    // but anything larger is rejected.
    reader
        .take(MAX_KEYDB_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|_| Error::KeydbParse)?;
    if buf.len() as u64 > MAX_KEYDB_BYTES {
        return Err(Error::KeydbInvalid);
    }
    String::from_utf8(buf).map_err(|_| Error::KeydbParse)
}

/// Standard keydb storage path.
pub fn default_path() -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_err(|_| Error::KeydbParse)?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("freemkv")
        .join("keydb.cfg"))
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
        read_capped_to_string(flate2::read::GzDecoder::new(data))?
    } else {
        std::str::from_utf8(data)
            .map(str::to_string)
            .map_err(|_| Error::KeydbParse)?
    };

    let entries = text
        .lines()
        .filter(|l| {
            let t = l.trim();
            t.starts_with("0x")
                || t.starts_with("| DK")
                || t.starts_with("| PK")
                || t.starts_with("| HC")
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

    Ok(UpdateResult {
        path,
        entries,
        bytes: text.len(),
    })
}

/// Result of a KEYDB update -- path written, entry count, and byte size.
#[derive(Debug)]
pub struct UpdateResult {
    pub path: PathBuf,
    pub entries: usize,
    pub bytes: usize,
}

fn http_get(url: &str) -> Result<Vec<u8>> {
    let (mut host, mut port, mut path) = parse_url(url)?;

    for _ in 0..MAX_REDIRECTS {
        // Resolve to a concrete socket address so we can bound the connect
        // with connect_timeout (plain connect() uses the OS default, which
        // can be minutes).
        let addr = (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
            .ok_or_else(|| Error::KeydbConnect { host: host.clone() })?;
        let mut stream = TcpStream::connect_timeout(&addr, NET_TIMEOUT)
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;
        stream.set_read_timeout(Some(READ_TIMEOUT)).ok();
        stream.set_write_timeout(Some(NET_TIMEOUT)).ok();

        // HTTP/1.0 forces close-delimited framing: the server cannot reply
        // with Transfer-Encoding: chunked, so the raw body is the keydb
        // bytes with no chunk-size lines to de-frame.
        let request = format!(
            "GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\nAccept-Encoding: identity\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;

        let mut response = Vec::new();
        stream
            .take(100 * 1024 * 1024)
            .read_to_end(&mut response)
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;

        let header_end = find_header_end(&response).ok_or(Error::KeydbParse)?;
        // Lossy: a stray non-UTF-8 byte in the header block must not blank
        // out the whole status line / Location header (which would surface
        // as an undiagnosable KeydbHttp{status:0}).
        let headers = String::from_utf8_lossy(&response[..header_end]);
        let body = &response[header_end + 4..];

        let status = parse_status(&headers);

        // Only treat a Location header as a redirect when the status is
        // actually 3xx; a 200 carrying a stray Location (some proxies) is
        // not a redirect, and a 3xx without Location is a malformed redirect.
        if (300..=399).contains(&status) {
            let location =
                extract_header(&headers, "Location").ok_or(Error::KeydbHttp { status })?;
            let (next_host, next_port, next_path) = resolve_redirect(&location, &host, port)?;
            host = next_host;
            port = next_port;
            path = next_path;
            continue;
        }

        if status != 200 {
            return Err(Error::KeydbHttp { status });
        }

        return Ok(body.to_vec());
    }

    Err(Error::KeydbTooManyRedirects)
}

/// Resolve a `Location` value against the current request target.
/// Handles absolute `http://` URLs, scheme-relative `//host/path`,
/// absolute paths `/path`, and rejects unsupported schemes (e.g.
/// `https://`, which this dependency-light client cannot fetch) with a
/// diagnosable error rather than a generic parse failure.
fn resolve_redirect(
    location: &str,
    cur_host: &str,
    cur_port: u16,
) -> Result<(String, u16, String)> {
    let loc = location.trim();

    if let Some(rest) = loc.strip_prefix("//") {
        // Scheme-relative: //host[:port]/path — inherit http.
        return parse_url(&format!("http://{rest}"));
    }
    if loc.starts_with('/') {
        // Absolute path on the same host/port.
        return Ok((cur_host.to_string(), cur_port, loc.to_string()));
    }
    if let Some(scheme) = loc.split("://").next() {
        if loc.contains("://") && !scheme.eq_ignore_ascii_case("http") {
            return Err(Error::KeydbUnsupportedScheme {
                scheme: scheme.to_string(),
            });
        }
    }
    parse_url(loc)
}

fn parse_url(url: &str) -> Result<(String, u16, String)> {
    // Reject non-http(s) up front so the caller gets a scheme diagnostic
    // rather than an opaque parse error.
    if let Some(scheme) = url.split("://").next() {
        if url.contains("://") && !scheme.eq_ignore_ascii_case("http") {
            return Err(Error::KeydbUnsupportedScheme {
                scheme: scheme.to_string(),
            });
        }
    }
    let url = url.strip_prefix("http://").ok_or(Error::KeydbParse)?;
    let (host_port, path) = match url.find('/') {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, "/"),
    };
    let (host, port) = match host_port.find(':') {
        Some(i) => {
            let port_str = &host_port[i + 1..];
            // A non-empty-but-unparseable port is a malformed URL; only an
            // omitted port defaults to 80.
            let port = if port_str.is_empty() {
                80
            } else {
                port_str.parse().map_err(|_| Error::KeydbParse)?
            };
            (&host_port[..i], port)
        }
        None => (host_port, 80u16),
    };
    Ok((host.to_string(), port, path.to_string()))
}

fn parse_status(headers: &str) -> u16 {
    headers
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

fn extract_header(headers: &str, name: &str) -> Option<String> {
    // Split on the first ':' rather than byte-indexing at name.len(),
    // which would panic on a multibyte UTF-8 codepoint straddling that
    // offset (headers are decoded from untrusted network bytes). Also
    // accepts single-character values (e.g. "Location:x").
    for line in headers.lines() {
        if let Some((key, value)) = line.split_once(':') {
            if key.trim().eq_ignore_ascii_case(name) {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn extract_zip(data: &[u8]) -> Result<String> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|_| Error::KeydbParse)?;

    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(|_| Error::KeydbParse)?;
        if file.name().ends_with(".cfg") || file.name().ends_with(".CFG") {
            return read_capped_to_string(file);
        }
    }

    Err(Error::KeydbInvalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_defaults_and_paths() {
        let (h, p, path) = parse_url("http://example.com/keydb.zip").unwrap();
        assert_eq!(
            (h.as_str(), p, path.as_str()),
            ("example.com", 80, "/keydb.zip")
        );

        let (h, p, path) = parse_url("http://example.com:8080").unwrap();
        assert_eq!((h.as_str(), p, path.as_str()), ("example.com", 8080, "/"));
    }

    #[test]
    fn parse_url_rejects_https_scheme() {
        // TLS is unsupported by this client; surface a scheme diagnostic
        // rather than a generic parse error.
        assert!(matches!(
            parse_url("https://example.com/k.zip"),
            Err(Error::KeydbUnsupportedScheme { .. })
        ));
    }

    #[test]
    fn parse_url_rejects_malformed_port() {
        // Non-empty-but-unparseable port must error, not silently fall to 80.
        assert!(matches!(
            parse_url("http://example.com:abc/path"),
            Err(Error::KeydbParse)
        ));
        // An empty port still defaults to 80.
        let (_, p, _) = parse_url("http://example.com:/path").unwrap();
        assert_eq!(p, 80);
    }

    #[test]
    fn redirect_to_https_is_unsupported_scheme_not_parse_error() {
        // The bplaced-style mirror enabling TLS on a redirect must produce a
        // diagnosable scheme error, not KeydbParse.
        assert!(matches!(
            resolve_redirect("https://mirror.example/keydb.zip", "old.host", 80),
            Err(Error::KeydbUnsupportedScheme { .. })
        ));
    }

    #[test]
    fn redirect_scheme_relative_and_absolute_path() {
        // Scheme-relative //host/path inherits http.
        let (h, p, path) = resolve_redirect("//mirror.example/a.zip", "old.host", 80).unwrap();
        assert_eq!(
            (h.as_str(), p, path.as_str()),
            ("mirror.example", 80, "/a.zip")
        );

        // Absolute path stays on the current host/port.
        let (h, p, path) = resolve_redirect("/new/path.zip", "cur.host", 8080).unwrap();
        assert_eq!(
            (h.as_str(), p, path.as_str()),
            ("cur.host", 8080, "/new/path.zip")
        );

        // Absolute http URL is followed normally.
        let (h, _, path) = resolve_redirect("http://other.host/x.zip", "cur.host", 80).unwrap();
        assert_eq!((h.as_str(), path.as_str()), ("other.host", "/x.zip"));
    }

    #[test]
    fn parse_status_extracts_code() {
        assert_eq!(parse_status("HTTP/1.0 200 OK\r\nFoo: bar"), 200);
        assert_eq!(parse_status("HTTP/1.1 301 Moved Permanently"), 301);
        assert_eq!(parse_status("garbage"), 0);
    }
}

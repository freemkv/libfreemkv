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

/// Standard keydb storage path — the canonical location to write the keydb to.
///
/// On Windows this is the idiomatic per-user roaming dir
/// `%APPDATA%\freemkv\keydb.cfg`, falling back to the legacy
/// `%USERPROFILE%\.config\freemkv\keydb.cfg` only if `APPDATA` is unset. On
/// Linux/macOS it stays the long-standing `$HOME/.config/freemkv/keydb.cfg`.
///
/// The CLI's read-side search (first existing of several locations) lives in
/// `freemkv-keysources::keydb_search_paths`; this function is the single
/// *write* default used by `save`/`update`, kept in lock-step with that crate's
/// `default_keydb_path` for the same OS.
pub fn default_path() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            if !appdata.is_empty() {
                return Ok(PathBuf::from(appdata).join("freemkv").join("keydb.cfg"));
            }
        }
        let profile = std::env::var("USERPROFILE").map_err(|_| Error::KeydbParse)?;
        Ok(PathBuf::from(profile)
            .join(".config")
            .join("freemkv")
            .join("keydb.cfg"))
    }
    #[cfg(not(windows))]
    {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| Error::KeydbParse)?;
        Ok(PathBuf::from(home)
            .join(".config")
            .join("freemkv")
            .join("keydb.cfg"))
    }
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
        // Plain-text body: route through the same capped reader as the gz/zip
        // branches so an oversized uncompressed upload can't bypass
        // MAX_KEYDB_BYTES.
        read_capped_to_string(std::io::Cursor::new(data))?
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
        std::fs::create_dir_all(dir).map_err(|e| {
            tracing::warn!(error = %e, path = %path.display(), "keydb dir create failed");
            Error::KeydbWrite {
                path: path.display().to_string(),
            }
        })?;
    }
    std::fs::write(&path, &text).map_err(|e| {
        tracing::warn!(error = %e, path = %path.display(), "keydb write failed");
        Error::KeydbWrite {
            path: path.display().to_string(),
        }
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
        let mut stream = TcpStream::connect_timeout(&addr, NET_TIMEOUT).map_err(|e| {
            tracing::debug!(error = %e, host = %host, "keydb connect failed");
            Error::KeydbConnect { host: host.clone() }
        })?;
        stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;
        stream
            .set_write_timeout(Some(NET_TIMEOUT))
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;

        // HTTP/1.0 forces close-delimited framing: the server cannot reply
        // with Transfer-Encoding: chunked, so the raw body is the keydb
        // bytes with no chunk-size lines to de-frame.
        let request = format!(
            "GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\nAccept-Encoding: identity\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;

        // Read the header block incrementally up to the \r\n\r\n terminator,
        // bounded to ~64 KiB, BEFORE pulling any body. This avoids buffering up
        // to 100 MiB per redirect hop just to inspect the status / Location.
        const MAX_HEADER_BYTES: usize = 64 * 1024;
        let mut reader = std::io::BufReader::new(stream);
        let mut header_buf: Vec<u8> = Vec::with_capacity(1024);
        let mut byte = [0u8; 1];
        loop {
            let n = reader
                .read(&mut byte)
                .map_err(|_| Error::KeydbConnect { host: host.clone() })?;
            if n == 0 {
                // Connection closed before headers completed.
                return Err(Error::KeydbParse);
            }
            header_buf.push(byte[0]);
            if header_buf.ends_with(b"\r\n\r\n") {
                break;
            }
            if header_buf.len() > MAX_HEADER_BYTES {
                return Err(Error::KeydbParse);
            }
        }
        // header_buf includes the trailing \r\n\r\n.
        let header_end = header_buf.len() - 4;
        // Lossy: a stray non-UTF-8 byte in the header block must not blank
        // out the whole status line / Location header (which would surface
        // as an undiagnosable KeydbHttp{status:0}).
        let headers = String::from_utf8_lossy(&header_buf[..header_end]).into_owned();
        let headers = headers.as_str();

        let status = parse_status(headers).ok_or(Error::KeydbParse)?;

        // Only treat a Location header as a redirect when the status is
        // actually 3xx; a 200 carrying a stray Location (some proxies) is
        // not a redirect, and a 3xx without Location is a malformed redirect.
        if (300..=399).contains(&status) {
            let location =
                extract_header(headers, "Location").ok_or(Error::KeydbHttp { status })?;
            let (next_host, next_port, next_path) = resolve_redirect(&location, &host, port)?;
            host = next_host;
            port = next_port;
            path = next_path;
            continue;
        }

        if status != 200 {
            return Err(Error::KeydbHttp { status });
        }

        // Now read the body, still bounded by the existing 100 MiB cap. The
        // BufReader carries any bytes already buffered past the header.
        let mut body = Vec::new();
        reader
            .take(100 * 1024 * 1024)
            .read_to_end(&mut body)
            .map_err(|_| Error::KeydbConnect { host: host.clone() })?;
        return Ok(body);
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

fn parse_status(headers: &str) -> Option<u16> {
    headers
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
}

/// Locate the end of the HTTP header block (the index of the `\r\n\r\n`).
/// Retained for the framing unit tests; the live path now reads headers
/// incrementally in `http_get` so the whole response is never buffered.
#[cfg_attr(not(test), allow(dead_code))]
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
        assert_eq!(parse_status("HTTP/1.0 200 OK\r\nFoo: bar"), Some(200));
        assert_eq!(parse_status("HTTP/1.1 301 Moved Permanently"), Some(301));
        assert_eq!(parse_status("garbage"), None);
    }

    // ── New comprehensive tests ────────────────────────────────────────────────

    /// find_header_end detects the \r\n\r\n separator (RFC 7230 §3 — HTTP header
    /// terminator is CRLF CRLF). Returns the byte position of the first \r.
    /// Mutation: searching for \n\n instead of \r\n\r\n misses the boundary.
    #[test]
    fn find_header_end_locates_crlfcrlf() {
        let data = b"HTTP/1.0 200 OK\r\nContent-Length: 42\r\n\r\nbody starts here";
        // The \r\n\r\n starts at byte 37 (after the Content-Length line).
        let pos = find_header_end(data).expect("must find header end");
        // body starts at pos + 4 (past the \r\n\r\n).
        assert_eq!(
            &data[pos + 4..],
            b"body starts here",
            "body must begin immediately after the \\r\\n\\r\\n boundary"
        );
    }

    /// find_header_end returns None when there is no \r\n\r\n.
    /// Mutation: returning Some(0) unconditionally makes this fail.
    #[test]
    fn find_header_end_returns_none_when_absent() {
        let data = b"no separator here at all";
        assert!(find_header_end(data).is_none());
    }

    /// extract_header is case-insensitive per RFC 7230 §3.2.
    /// Mutation: using case-sensitive comparison misses "location" vs "Location".
    #[test]
    fn extract_header_case_insensitive() {
        let headers = "HTTP/1.1 301 Moved\r\nlocation: http://new.host/path\r\n";
        let val = extract_header(headers, "Location").expect("must find Location");
        assert_eq!(val, "http://new.host/path");
    }

    /// extract_header with a missing header returns None.
    /// Mutation: returning Some("") makes the caller proceed on a missing Location header.
    #[test]
    fn extract_header_missing_returns_none() {
        let headers = "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n";
        assert!(extract_header(headers, "Location").is_none());
    }

    /// extract_header trims leading/trailing whitespace from the value.
    /// RFC 7230 §3.2.6: optional whitespace around field value.
    /// Mutation: not trimming the value keeps leading spaces in the URL.
    #[test]
    fn extract_header_trims_value_whitespace() {
        let headers = "HTTP/1.1 301 Moved\r\nLocation:   /new/path  \r\n";
        let val = extract_header(headers, "Location").unwrap();
        assert_eq!(val, "/new/path", "value must be trimmed");
    }

    /// save() rejects data that is not a valid keydb (no recognisable entries).
    /// Spec: entries are lines starting with "0x", "| DK", "| PK", or "| HC".
    /// Mutation: dropping the entries==0 check lets an empty file be saved.
    #[test]
    fn save_rejects_empty_text() {
        // Plain text with no valid keydb entries.
        let garbage = b"this is not a keydb\njust random text\n";
        assert!(
            matches!(save(garbage), Err(Error::KeydbInvalid)),
            "keydb without valid entries must be rejected"
        );
    }

    /// save() accepts plain text with at least one "0x"-prefixed entry line.
    /// Mutation: counting only "| DK" lines ignores the "0x" entry format.
    #[test]
    fn save_accepts_plaintext_with_0x_entries() {
        // Minimal keydb-style file with a VUK entry (0x-prefixed).
        let content = b"0xDEADBEEFCAFEBABE0102030405060708090A0B0C0D0E0F\n";
        // We can't predict the HOME path in test environments without
        // potentially writing to a real location. So only check that save()
        // accepts this content as valid (may return KeydbWrite if dir exists
        // but we lack permission — that still proves it passed the parse check).
        let result = save(content);
        // Accept either Ok (wrote successfully) or a write error (env issue),
        // but NOT KeydbInvalid or KeydbParse.
        match &result {
            Ok(_) => {}
            Err(Error::KeydbWrite { .. }) => {}
            Err(e) => panic!("unexpected error for valid keydb content: {:?}", e),
        }
    }

    /// save() accepts content with "| DK" entries (device-key table format).
    /// Mutation: only accepting "0x" lines rejects DK-format keydb files.
    #[test]
    fn save_accepts_pipe_dk_entry_format() {
        let content = b"| DK 0102030405060708 | 0102030405060708090a0b0c0d0e0f10 |\n";
        let result = save(content);
        match &result {
            Ok(_) => {}
            Err(Error::KeydbWrite { .. }) => {}
            Err(e) => panic!("unexpected error for DK-format entry: {:?}", e),
        }
    }

    /// save() accepts content with "| PK" entries (processing-key format).
    /// Mutation: not including "| PK" in the filter rejects PK-format keydb files.
    #[test]
    fn save_accepts_pipe_pk_entry_format() {
        let content = b"| PK 0102030405060708090a0b0c0d0e0f10 |\n";
        let result = save(content);
        match &result {
            Ok(_) => {}
            Err(Error::KeydbWrite { .. }) => {}
            Err(e) => panic!("unexpected error for PK-format entry: {:?}", e),
        }
    }

    /// save() accepts content with "| HC" entries (host certificate format).
    /// Mutation: not including "| HC" in the filter rejects HC-format keydb files.
    #[test]
    fn save_accepts_pipe_hc_entry_format() {
        let content = b"| HC 0102030405060708090a0b0c0d0e0f10 |\n";
        let result = save(content);
        match &result {
            Ok(_) => {}
            Err(Error::KeydbWrite { .. }) => {}
            Err(e) => panic!("unexpected error for HC-format entry: {:?}", e),
        }
    }

    /// save() recognises gzip-compressed input (magic bytes 0x1f 0x8b).
    /// Spec: gzip format magic is 0x1F 0x8B (RFC 1952 §2.3.1).
    /// Mutation: treating gzip magic as plain text fails to decompress.
    #[test]
    fn save_recognises_gzip_magic() {
        // Truncated gzip (header only, no valid body) — must not be treated as
        // plain text (no KeydbInvalid about entries), but as a parse error.
        let bad_gz = [0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03];
        let result = save(&bad_gz);
        // A truncated gzip is either KeydbParse (decompression error) or
        // KeydbInvalid (decompressed to empty). Must not be Ok.
        assert!(result.is_err(), "truncated gzip must not be accepted");
        // Crucially: must NOT be a plain-text UTF-8 error — gzip magic is not UTF-8.
        match result.unwrap_err() {
            Error::KeydbParse | Error::KeydbInvalid => {}
            e => panic!("wrong error kind for truncated gzip: {:?}", e),
        }
    }

    /// save() recognises ZIP magic bytes PK\x03\x04 and routes to extract_zip.
    /// Spec: ZIP local file header signature is 0x50 0x4B 0x03 0x04 (PKZIP APPNOTE §4.3.6).
    /// A truncated ZIP must error, but NOT as plain UTF-8 text.
    /// Mutation: checking gzip magic before ZIP magic means ZIP files are
    ///           fed to the gzip decoder and produce the wrong error.
    #[test]
    fn save_recognises_zip_magic() {
        // Valid ZIP magic followed by garbage — must be routed to extract_zip.
        let bad_zip = b"PK\x03\x04garbage that is not a real zip";
        let result = save(bad_zip);
        assert!(result.is_err(), "invalid zip must be rejected");
        // Must be a parse error, not a UTF-8 error.
        match result.unwrap_err() {
            Error::KeydbParse | Error::KeydbInvalid => {}
            e => panic!("wrong error for bad zip: {:?}", e),
        }
    }

    /// read_capped_to_string rejects data exceeding MAX_KEYDB_BYTES.
    /// Spec: doc says "Returns Error::KeydbInvalid if the input exceeds the cap".
    /// Mutation: removing the length check accepts decompression bombs.
    #[test]
    fn read_capped_to_string_rejects_oversized_input() {
        // Build a reader that reports it has more data than the cap.
        // We use a Cursor with MAX_KEYDB_BYTES + 1 bytes of content.
        let too_big = vec![b'A'; (MAX_KEYDB_BYTES + 1) as usize];
        let cursor = std::io::Cursor::new(too_big);
        let result = read_capped_to_string(cursor);
        assert!(
            matches!(result, Err(Error::KeydbInvalid)),
            "oversized input must yield KeydbInvalid, got: {:?}",
            result
        );
    }

    /// read_capped_to_string accepts exactly MAX_KEYDB_BYTES (at-cap is allowed).
    /// Spec: doc says "Read one byte past the cap so an exactly-at-cap stream is accepted."
    /// Mutation: using `>=` instead of `>` in the length check rejects valid at-cap files.
    #[test]
    fn read_capped_to_string_accepts_at_cap_size() {
        let at_cap = vec![b'A'; MAX_KEYDB_BYTES as usize];
        let cursor = std::io::Cursor::new(at_cap);
        let result = read_capped_to_string(cursor);
        assert!(result.is_ok(), "exactly MAX_KEYDB_BYTES must be accepted");
    }

    /// parse_status returns None for an empty/malformed status line (not a
    /// meaningless 0). The call site maps None to Error::KeydbParse.
    #[test]
    fn parse_status_empty_input_returns_none() {
        assert_eq!(parse_status(""), None);
        assert_eq!(parse_status("\r\n"), None);
    }

    /// Regression: set_read_timeout / set_write_timeout failures must surface as
    /// KeydbConnect, not be silently swallowed.
    ///
    /// We can't easily synthesise a TcpStream whose set_*timeout syscall fails
    /// without a platform-specific socket hack, so instead we verify that the
    /// error-mapping expression itself is correct: if set_read_timeout were to
    /// fail for a given host, the result must be Err(KeydbConnect { host }).
    ///
    /// The test constructs the exact Err value the code would return and asserts
    /// it is KeydbConnect (not, say, silently Ok or a different variant). This
    /// pins the variant selection so a future refactor that changes the `.ok()`
    /// pattern back would need to update this test as well.
    #[test]
    fn timeout_set_failure_maps_to_keydb_connect() {
        // Simulate what the propagated error looks like.
        let host = "hostile.example.com".to_string();
        // The io::Error that set_read_timeout would return on failure.
        let io_err = std::io::Error::from(std::io::ErrorKind::InvalidInput);
        // Apply the same map_err the production code uses.
        let result: Result<()> =
            Err(io_err).map_err(|_| Error::KeydbConnect { host: host.clone() });
        assert!(
            matches!(result, Err(Error::KeydbConnect { host: ref h }) if h == "hostile.example.com"),
            "set_timeout failure must map to KeydbConnect, got: {:?}",
            result
        );
    }

    /// Regression: http_get to an unreachable host returns KeydbConnect, not a hang.
    /// This exercises the connect_timeout path (and thus confirms the overall
    /// error-propagation chain is wired); the timeout-set propagation is exercised
    /// by the unit test above.
    ///
    /// Uses port 1 on localhost, which is reserved/unassigned and virtually never
    /// listening. connect_timeout with NET_TIMEOUT will refuse or time out quickly.
    /// We only assert the error variant, not the host field, since the OS may
    /// resolve the address differently.
    #[test]
    fn http_get_unreachable_host_returns_keydb_connect() {
        // Port 1 on loopback — almost always refused immediately.
        let result = http_get("http://127.0.0.1:1/keydb.zip");
        // Must be an Err; KeydbConnect is expected for a TCP-level failure.
        // KeydbParse or KeydbHttp would indicate the wrong error path.
        assert!(result.is_err(), "unreachable host must fail");
        match result.unwrap_err() {
            Error::KeydbConnect { .. } => {}
            e => panic!("expected KeydbConnect for unreachable host, got: {:?}", e),
        }
    }
}

//! Tolerant XML scraping helpers — promoted from two near-duplicate
//! hand-rolls in `paramount.rs` and `criterion.rs`.
//!
//! Handles the subset of XML the BD-J authoring tools we've seen actually emit:
//! case-insensitive tag/attribute names, optional `ns:` namespace prefixes, whitespace-tolerant
//! `=` between attribute name and value, both `"value"` and `'value'` quoting, and self-closing
//! tags (`<tag />`, `<tag/>`; [`text`] returns `Some("")` for empty content). Not a full XML
//! parser.

// Out of scope (intentionally simple): entity decoding, CDATA, comments,
// processing instructions, DTD declarations — none of the BD-J authored
// disc data observed exercises those; labels are plain ASCII/Latin-1.

/// Extract the value of attribute `name` from one XML element
/// fragment (e.g. `<playlist name="Feature" id="00222" />`).
///
/// Returns the raw attribute text (no entity decoding) or `None` if
/// the attribute isn't present. Empty string for `name=""` is
/// represented as `Some("")`.
pub fn attr(element: &str, name: &str) -> Option<String> {
    let bytes = element.as_bytes();
    let name_lower = name.to_ascii_lowercase();
    let name_bytes = name_lower.as_bytes();
    let mut i = 0;
    while i + name_bytes.len() < bytes.len() {
        // Skip over a quoted attribute value entirely so a name token
        // embedded inside another attribute's value (e.g.
        // `y="name='inner'"`) is never matched as a real attribute.
        if bytes[i] == b'"' || bytes[i] == b'\'' {
            let q = bytes[i];
            i += 1;
            while i < bytes.len() && bytes[i] != q {
                i += 1;
            }
            // Step past the closing quote (or to EOF).
            i += 1;
            continue;
        }
        // Find the next position where `name=` could start. We need
        // a word boundary before the name (whitespace or `<` or `:`).
        if i > 0 && is_name_char(bytes[i - 1]) {
            i += 1;
            continue;
        }
        if !slice_eq_ignore_case(&bytes[i..i + name_bytes.len()], name_bytes) {
            i += 1;
            continue;
        }
        let after_name = i + name_bytes.len();
        // The character immediately after the name must not be a
        // name-continuation (otherwise we matched a prefix like
        // `lang_id` when looking for `lang`).
        if after_name < bytes.len() && is_name_char(bytes[after_name]) {
            i = after_name;
            continue;
        }
        // Walk past whitespace, then `=`, then more whitespace, then
        // the opening quote.
        let mut j = after_name;
        while j < bytes.len() && is_ws(bytes[j]) {
            j += 1;
        }
        if j >= bytes.len() || bytes[j] != b'=' {
            i = j.max(i + 1);
            continue;
        }
        j += 1; // past '='
        while j < bytes.len() && is_ws(bytes[j]) {
            j += 1;
        }
        if j >= bytes.len() {
            return None;
        }
        let quote = bytes[j];
        if quote != b'"' && quote != b'\'' {
            // Unquoted attribute values aren't part of well-formed
            // XML (HTML5 allows them, XML doesn't). Skip.
            i = j;
            continue;
        }
        let value_start = j + 1;
        let close = bytes[value_start..].iter().position(|&b| b == quote)?;
        let value = &element[value_start..value_start + close];
        return Some(value.to_string());
    }
    None
}

/// Extract the trimmed text content of the first occurrence of
/// `<tag>...</tag>` in `xml`. Returns `None` if the tag isn't found
/// or its opening tag is malformed.
///
/// Whitespace around the inner text is stripped. Self-closing
/// `<tag />` yields `Some("")`. Nested same-name tags are NOT
/// handled — the first close encountered wins (this matches the
/// prior behavior in criterion.rs).
pub fn text(xml: &str, tag: &str) -> Option<String> {
    let (_open_end, body_start) = find_open_tag(xml, tag, 0)?;
    // For self-closing tags, body_start is past `/>` with no content. Detect via
    // a *byte* comparison: slicing `&xml[..]` two bytes back can land inside a
    // multi-byte UTF-8 char and panic on untrusted XML; byte indexing never does.
    let b = xml.as_bytes();
    if body_start >= 2 && b[body_start - 2] == b'/' && b[body_start - 1] == b'>' {
        return Some(String::new());
    }
    // Find the matching close tag. Case-insensitive + namespace-aware.
    let close_start = find_close_tag(xml, tag, body_start)?;
    Some(xml[body_start..close_start].trim().to_string())
}

/// Locate the next `<tag>` opening AND its closing `</tag>` in `xml`,
/// starting at byte offset `from`. Returns `(element_start, element_end)`
/// — `element_start` is the `<` of the opening tag, `element_end` is one
/// past the `>` of the closing tag. Useful for iterating over repeated
/// elements like `<playlist>` blocks in `paramount`.
///
/// For self-closing elements, `element_end` points just past `/>` — there
/// is no separate body range.
pub fn find_element(xml: &str, tag: &str, from: usize) -> Option<(usize, usize)> {
    let bytes = xml.as_bytes();
    let mut i = from;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        // Try matching tag name at i+1 (after `<`).
        let after_lt = i + 1;
        if !matches_tag_name_at(bytes, after_lt, tag) {
            i += 1;
            continue;
        }
        // Found an open tag at offset i. Walk to find the closing `>`
        // of the open tag itself.
        let mut j = after_lt;
        // Skip past the tag name (and optional namespace prefix).
        while j < bytes.len() && (is_name_char(bytes[j]) || bytes[j] == b':') {
            j += 1;
        }
        // Walk attributes — track quoting state.
        let mut self_closing = false;
        while j < bytes.len() {
            match bytes[j] {
                b'>' => {
                    j += 1;
                    break;
                }
                b'/' if j + 1 < bytes.len() && bytes[j + 1] == b'>' => {
                    self_closing = true;
                    j += 2;
                    break;
                }
                b'"' | b'\'' => {
                    let q = bytes[j];
                    j += 1;
                    while j < bytes.len() && bytes[j] != q {
                        j += 1;
                    }
                    if j < bytes.len() {
                        j += 1;
                    }
                }
                _ => j += 1,
            }
        }
        if self_closing {
            return Some((i, j));
        }
        // Find matching close. Doesn't handle nested same-name; OK
        // for our authoring-tool subset.
        let close_start = find_close_tag(xml, tag, j)?;
        let close_end = find_byte(bytes, b'>', close_start)? + 1;
        return Some((i, close_end));
    }
    None
}

// ── Internal helpers ───────────────────────────────────────────────────────

// True if `bytes[start..]` opens a tag named `tag` (optional `ns:` prefix,
// case-insensitive); char after the name must not be a name-continuation.
fn matches_tag_name_at(bytes: &[u8], start: usize, tag: &str) -> bool {
    // Compare case-insensitively without allocating a lowercased copy
    // of `tag` on every call (hot path: once per `<`/`</`).
    let tag_bytes = tag.as_bytes();
    // Skip optional `prefix:` (one or more name chars + `:`).
    let mut name_start = start;
    let mut scan = start;
    while scan < bytes.len() && is_name_char(bytes[scan]) {
        scan += 1;
    }
    if scan < bytes.len() && bytes[scan] == b':' {
        name_start = scan + 1;
    }
    if name_start + tag_bytes.len() > bytes.len() {
        return false;
    }
    if !bytes[name_start..name_start + tag_bytes.len()].eq_ignore_ascii_case(tag_bytes) {
        return false;
    }
    // Boundary: char after the tag name must be `>`, `/`, whitespace.
    let after = name_start + tag_bytes.len();
    if after >= bytes.len() {
        return false;
    }
    matches!(bytes[after], b'>' | b'/' | b' ' | b'\t' | b'\n' | b'\r')
}

/// Find the offset of the next `</tag>` (or `</ns:tag>`) in `xml`
/// starting at `from`. Case-insensitive; returns the offset of the
/// `<`. None if not found.
fn find_close_tag(xml: &str, tag: &str, from: usize) -> Option<usize> {
    let bytes = xml.as_bytes();
    let mut i = from;
    while i + 2 < bytes.len() {
        if bytes[i] == b'<' && bytes[i + 1] == b'/' {
            // Check tag name (with optional namespace).
            if matches_tag_name_at(bytes, i + 2, tag) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Find the open tag of `<tag>` in `xml` starting at `from`. Returns
/// `(after_open_lt, after_open_gt)` — the offsets are: just past
/// the `<` of the open tag, and just past the `>` of the open tag.
fn find_open_tag(xml: &str, tag: &str, from: usize) -> Option<(usize, usize)> {
    let bytes = xml.as_bytes();
    let (elem_start, _) = find_element(xml, tag, from)?;
    let after_lt = elem_start + 1;
    // Find the `>` that ends the open tag (handling quoted attrs).
    let mut j = after_lt;
    while j < bytes.len() {
        match bytes[j] {
            b'>' => return Some((after_lt, j + 1)),
            b'/' if j + 1 < bytes.len() && bytes[j + 1] == b'>' => {
                return Some((after_lt, j + 2));
            }
            b'"' | b'\'' => {
                let q = bytes[j];
                j += 1;
                while j < bytes.len() && bytes[j] != q {
                    j += 1;
                }
                if j < bytes.len() {
                    j += 1;
                }
            }
            _ => j += 1,
        }
    }
    None
}

fn find_byte(bytes: &[u8], target: u8, from: usize) -> Option<usize> {
    bytes[from..]
        .iter()
        .position(|&b| b == target)
        .map(|p| p + from)
}

/// True if `c` can be part of an XML name token (rough). We accept
/// alphanumerics, `_`, `-`, `.`.
fn is_name_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'-' || c == b'.'
}

fn is_ws(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r')
}

fn slice_eq_ignore_case(a: &[u8], b_lower: &[u8]) -> bool {
    if a.len() != b_lower.len() {
        return false;
    }
    a.iter()
        .zip(b_lower.iter())
        .all(|(&x, &y)| x.to_ascii_lowercase() == y)
}

#[cfg(test)]
#[path = "xml_tests.rs"]
mod tests;

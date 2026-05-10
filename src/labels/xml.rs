//! Tolerant XML scraping helpers — promoted from two near-duplicate
//! hand-rolls in `paramount.rs` (attribute extraction) and `criterion.rs`
//! (tag-text extraction).
//!
//! These are NOT a full XML parser. They handle the subset of XML the
//! BD-J authoring tools we've seen actually emit: ASCII tag/attr
//! names, no entity references inside label strings, optional XML
//! namespaces. Hardening goals over the prior `find("<tag>")` /
//! `find(r#"name=""#)` matchers:
//!
//! 1. **Case-insensitive** tag and attribute names — vendors casing
//!    is inconsistent across authoring-tool revisions.
//! 2. **Namespace-aware** — strip an optional `ns:` prefix so
//!    `<ns:playlist>` and `<playlist>` both match.
//! 3. **Whitespace-tolerant** — multiple/tab/newline characters
//!    around `=` between attribute name and value; whitespace inside
//!    the opening tag.
//! 4. **Quote-style tolerant** — both `"value"` and `'value'`.
//! 5. **Self-closing tag handling** — `<tag />` and `<tag/>` both
//!    work; [`text`] returns `Some("")` for empty content.
//!
//! Out of scope (intentionally simple): XML entity decoding
//! (`&amp;`, `&lt;`, etc.), CDATA sections, comments, processing
//! instructions, DTD declarations. None of the BD-J authored disc
//! data we've observed exercises any of those — labels are plain
//! ASCII/Latin-1 in attribute values.

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
    let (open_end, body_start) = find_open_tag(xml, tag, 0)?;
    // Self-closing — already consumed in find_open_tag if `/>`.
    if open_end == body_start {
        // Means find_open_tag returned the same offset twice for
        // self-closing form. (Not currently the case in our impl,
        // but defensive.)
        return Some(String::new());
    }
    // For self-closing tags, body_start is past `/>` and we have no
    // content. Detect by checking the char at body_start - 1 was `/`.
    if body_start >= 2 && &xml[body_start - 2..body_start] == "/>" {
        return Some(String::new());
    }
    // Find the matching close tag. Case-insensitive + namespace-aware.
    let close_start = find_close_tag(xml, tag, body_start)?;
    Some(xml[body_start..close_start].trim().to_string())
}

/// Locate the next `<tag>` opening AND its closing `</tag>` in
/// `xml`, starting at byte offset `from`. Returns `(element_start,
/// element_end)` — `element_start` is the `<` of the opening tag,
/// `element_end` is one past the `>` of the closing tag. Useful for
/// iterating over repeated elements like `<playlist>` blocks in
/// `paramount`.
///
/// Self-closing elements return the same offset for body_end as the
/// element_end (i.e. `element_end - element_start` includes only the
/// `<tag .../>` text).
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

/// True if `bytes[start..]` opens a tag named `tag`, allowing an
/// optional `ns:` namespace prefix. Comparison is case-insensitive.
/// The character after the tag name must not be a name-continuation
/// (so `<player>` doesn't match `<play>`).
fn matches_tag_name_at(bytes: &[u8], start: usize, tag: &str) -> bool {
    let tag_lower = tag.to_ascii_lowercase();
    let tag_bytes = tag_lower.as_bytes();
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
    if !slice_eq_ignore_case(&bytes[name_start..name_start + tag_bytes.len()], tag_bytes) {
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
mod tests {
    use super::*;

    #[test]
    fn attr_basic() {
        assert_eq!(
            attr(r#"<playlist name="Feature" id="00222" />"#, "name"),
            Some("Feature".into())
        );
        assert_eq!(
            attr(r#"<playlist name="Feature" id="00222" />"#, "id"),
            Some("00222".into())
        );
    }

    #[test]
    fn attr_case_insensitive_name() {
        assert_eq!(
            attr(r#"<playlist Name="Feature" />"#, "name"),
            Some("Feature".into())
        );
        assert_eq!(
            attr(r#"<playlist NAME="Feature" />"#, "Name"),
            Some("Feature".into())
        );
    }

    #[test]
    fn attr_accepts_single_quotes() {
        assert_eq!(
            attr(r#"<playlist name='Feature' />"#, "name"),
            Some("Feature".into())
        );
    }

    #[test]
    fn attr_whitespace_around_equals() {
        assert_eq!(
            attr(r#"<playlist name = "Feature" />"#, "name"),
            Some("Feature".into())
        );
        assert_eq!(
            attr("<playlist name\n=\n\"Feature\" />", "name"),
            Some("Feature".into())
        );
    }

    #[test]
    fn attr_missing_returns_none() {
        assert_eq!(attr(r#"<playlist name="X" />"#, "id"), None);
        assert_eq!(attr("", "name"), None);
    }

    #[test]
    fn attr_no_substring_false_positive() {
        // Looking for "lang" should NOT match "lang_id" or
        // "language" because of the name-char boundary check.
        assert_eq!(attr(r#"<x lang_id="fra" language="eng" />"#, "lang"), None);
    }

    #[test]
    fn attr_empty_value() {
        assert_eq!(attr(r#"<x name="" id="1" />"#, "name"), Some("".into()));
    }

    #[test]
    fn text_basic() {
        assert_eq!(text("<x>hello</x>", "x"), Some("hello".into()));
        assert_eq!(
            text("<x>  hello world  </x>", "x"),
            Some("hello world".into())
        );
    }

    #[test]
    fn text_case_insensitive_tag() {
        assert_eq!(text("<X>foo</X>", "x"), Some("foo".into()));
        assert_eq!(text("<Foo>bar</foo>", "foo"), Some("bar".into()));
    }

    #[test]
    fn text_namespace_prefix() {
        assert_eq!(text("<ns:tag>value</ns:tag>", "tag"), Some("value".into()));
        assert_eq!(text("<foo:Bar>v</foo:Bar>", "bar"), Some("v".into()));
    }

    #[test]
    fn text_self_closing() {
        assert_eq!(text("<x/>", "x"), Some("".into()));
        assert_eq!(text("<x />", "x"), Some("".into()));
        assert_eq!(text("<x  attr=\"y\" />", "x"), Some("".into()));
    }

    #[test]
    fn text_with_attrs() {
        assert_eq!(
            text(r#"<x id="1" name="y">hello</x>"#, "x"),
            Some("hello".into())
        );
    }

    #[test]
    fn text_missing_close_returns_none() {
        assert_eq!(text("<x>hello", "x"), None);
    }

    #[test]
    fn text_skips_inner_tags_naively() {
        // Limitation noted: nested same-name tags aren't handled.
        // Different-name nesting works (we just return everything
        // between the open and close).
        assert_eq!(
            text("<x><y>nested</y></x>", "x"),
            Some("<y>nested</y>".into())
        );
    }

    #[test]
    fn find_element_basic() {
        let xml = r#"<x />  <y attr="1">body</y>"#;
        let (s, e) = find_element(xml, "y", 0).unwrap();
        assert_eq!(&xml[s..e], r#"<y attr="1">body</y>"#);
    }

    #[test]
    fn find_element_self_closing() {
        let xml = r#"<x />"#;
        let (s, e) = find_element(xml, "x", 0).unwrap();
        assert_eq!(&xml[s..e], "<x />");
    }

    #[test]
    fn find_element_handles_quoted_gt_in_attr() {
        // A `>` inside a quoted attribute value should not terminate
        // the open tag prematurely.
        let xml = r#"<x attr="foo>bar">body</x>"#;
        let (s, e) = find_element(xml, "x", 0).unwrap();
        assert_eq!(&xml[s..e], r#"<x attr="foo>bar">body</x>"#);
    }

    #[test]
    fn find_element_iteration() {
        let xml = "<p>a</p><p>b</p><p>c</p>";
        let mut positions = Vec::new();
        let mut from = 0;
        while let Some((s, e)) = find_element(xml, "p", from) {
            positions.push(&xml[s..e]);
            from = e;
        }
        assert_eq!(positions, vec!["<p>a</p>", "<p>b</p>", "<p>c</p>"]);
    }

    #[test]
    fn find_element_with_namespace() {
        let xml = r#"<root><ns:item id="1" /></root>"#;
        let (s, e) = find_element(xml, "item", 0).unwrap();
        assert_eq!(&xml[s..e], r#"<ns:item id="1" />"#);
    }
}

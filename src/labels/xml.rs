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
    // For self-closing tags, body_start is past `/>` and there is no
    // content. Detect with a *byte* comparison: slicing `&xml[..]` two
    // bytes back can land inside a multi-byte UTF-8 char and panic
    // (untrusted on-disc XML), but indexing the byte slice never does.
    let b = xml.as_bytes();
    if body_start >= 2 && b[body_start - 2] == b'/' && b[body_start - 1] == b'>' {
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
/// For self-closing elements, `element_end` points just past `/>` and
/// there is no separate body range (`element_end - element_start`
/// spans only the `<tag .../>` text).
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

    #[test]
    fn text_multibyte_before_self_close_does_not_panic() {
        // A multi-byte UTF-8 char ending right before the `/>` used to
        // panic on a non-char-boundary str slice in `text()`. The
        // byte-level self-closing check must handle it cleanly.
        // 'é' (0xC3 0xA9) directly precedes the `/>`.
        assert_eq!(text("<x>é</x>", "x"), Some("é".into()));
        // Self-closing form with a multi-byte char in an attr value.
        assert_eq!(text(r#"<x a="é"/>"#, "x"), Some("".into()));
        assert_eq!(text("<x>日本語</x>", "x"), Some("日本語".into()));
    }

    #[test]
    fn attr_not_matched_inside_quoted_value() {
        // `name` appears only inside another attribute's quoted value;
        // it must NOT be returned as a real attribute.
        assert_eq!(attr(r#"<x y="name='inner'"/>"#, "name"), None);
        // A real `name` attribute after a decoy value still resolves.
        assert_eq!(
            attr(r#"<x y="name='inner'" name="real"/>"#, "name"),
            Some("real".into())
        );
    }

    // ── Additional hardening tests ─────────────────────────────────────────

    /// Spec: BD-J XML attr names are case-insensitive.
    /// Mutation: remove `.to_ascii_lowercase()` on attr name → uppercase fails.
    #[test]
    fn attr_fully_mixed_case_roundtrip() {
        assert_eq!(attr(r#"<X LANG="fra" />"#, "lang"), Some("fra".into()));
        assert_eq!(attr(r#"<x lAnG="fra" />"#, "LANG"), Some("fra".into()));
    }

    /// Spec: hyphenated attribute names include `-` as a name char.
    /// Mutation: remove `-` from `is_name_char` → `lang-id` boundary broken.
    #[test]
    fn attr_hyphenated_name_exact_match() {
        // Searching for `lang-id` must match exactly, not confuse with `lang`.
        assert_eq!(
            attr(r#"<x lang-id="eng" lang="fra" />"#, "lang-id"),
            Some("eng".into())
        );
        assert_eq!(
            attr(r#"<x lang-id="eng" lang="fra" />"#, "lang"),
            Some("fra".into())
        );
    }

    /// Spec: underscore-extended attr names must not match the base name.
    /// Paramount format: `aud_com1_idx` must not match `aud`.
    /// Mutation: remove the `is_name_char(bytes[after_name])` guard → prefix matched.
    #[test]
    fn attr_no_prefix_match_with_underscore_extension() {
        assert_eq!(
            attr(r#"<playlist aud_com1_idx="2" aud="eng" />"#, "aud"),
            Some("eng".into())
        );
    }

    /// Spec: `xml::text` must return `Some("")` for `<tag/>` (self-closing).
    /// Mutation: return None for self-closing → callers break.
    #[test]
    fn text_self_closing_no_whitespace() {
        assert_eq!(text("<x/>", "x"), Some("".into()));
    }

    /// Spec: self-closing with Unicode attr must not panic.
    /// Mutation: use byte-offset self-close check → panic on multi-byte boundary.
    #[test]
    fn text_self_closing_with_unicode_attr_does_not_panic() {
        assert_eq!(text(r#"<x attr="日本"/>  "#, "x"), Some("".into()));
    }

    /// Spec: namespace prefix in BOTH open and close tags must be stripped.
    /// Mutation: only strip prefix from opening tag, not closing → None.
    #[test]
    fn text_namespace_prefix_on_both_open_and_close() {
        assert_eq!(text("<a:tag>value</a:tag>", "tag"), Some("value".into()));
    }

    /// The first occurrence wins, not the last.
    /// Mutation: use rfind instead of find → second value returned.
    #[test]
    fn text_returns_first_occurrence() {
        let xml = "<x>first</x><x>second</x>";
        assert_eq!(text(xml, "x"), Some("first".into()));
    }

    /// `find_element` must advance correctly past each matched element.
    /// Mutation: advance from by 1 instead of end → elements double-counted.
    #[test]
    fn find_element_correctly_advances_past_each_element() {
        let xml = "<a>1</a><a>2</a><a>3</a>";
        let mut vals = Vec::new();
        let mut from = 0;
        while let Some((s, e)) = find_element(xml, "a", from) {
            vals.push(text(&xml[s..e], "a").unwrap());
            from = e;
        }
        assert_eq!(vals, vec!["1", "2", "3"]);
    }

    /// `>` inside a quoted attribute value must not end the open tag.
    /// Mutation: don't skip quoted regions → `>` in attr value ends tag early.
    #[test]
    fn find_element_gt_in_attr_does_not_end_tag_prematurely() {
        let xml = r#"<a cond="a>b">body</a>"#;
        let (s, e) = find_element(xml, "a", 0).unwrap();
        assert_eq!(&xml[s..e], r#"<a cond="a>b">body</a>"#);
    }

    /// Missing close tag must return None, not a truncated content.
    /// Mutation: return text after the open tag unconditionally → wrong value.
    #[test]
    fn text_missing_close_is_none_never_truncated() {
        assert_eq!(text("<x>incomplete", "x"), None);
    }

    /// `attr` with `name=""` (empty string value) returns Some(""), not None.
    /// Mutation: filter out empty returns → empty attr becomes None.
    #[test]
    fn attr_returns_some_empty_string_for_empty_value() {
        assert_eq!(
            attr(r#"<x forced_sub="" />"#, "forced_sub"),
            Some("".into())
        );
    }

    /// Single-char attr name must not falsely match inside a word boundary.
    /// Mutation: remove boundary check → `id` matches `pid`.
    #[test]
    fn attr_single_char_name_boundary() {
        assert_eq!(
            attr(r#"<x pid="1" hid="2" id="3" />"#, "id"),
            Some("3".into())
        );
    }

    /// `find_element` from a non-zero offset must start the search at that offset.
    /// Mutation: always start from 0 → finds elements before `from`.
    #[test]
    fn find_element_respects_from_offset() {
        let xml = "<p>a</p><p>b</p>";
        let (s, e) = find_element(xml, "p", 8).unwrap();
        assert_eq!(&xml[s..e], "<p>b</p>");
    }

    /// `text` trims surrounding whitespace from element content.
    /// Mutation: remove `.trim()` call → whitespace included.
    #[test]
    fn text_trims_internal_whitespace() {
        assert_eq!(text("<x>  hello  </x>", "x"), Some("hello".into()));
        assert_eq!(
            text("<x>\n  Aurora Drift\n</x>", "x"),
            Some("Aurora Drift".into())
        );
    }

    /// `attr` with single-quote value must match, same as double-quote.
    /// Mutation: accept only double-quote → single-quote attrs fail.
    #[test]
    fn attr_single_quote_value() {
        assert_eq!(attr(r#"<x a='hello' />"#, "a"), Some("hello".into()));
    }

    /// tag name with leading numeric char after namespace prefix is still matched
    /// as long as the local name matches exactly (BD tools sometimes use namespace-prefixed tags).
    #[test]
    fn find_element_handles_namespace_with_numeric_prefix_class() {
        let xml = r#"<root><di:name>Title</di:name></root>"#;
        let (s, e) = find_element(xml, "name", 0).unwrap();
        assert_eq!(&xml[s..e], "<di:name>Title</di:name>");
    }
}

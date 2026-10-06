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
    // A multi-byte UTF-8 char ('é' = 0xC3 0xA9) right before `/>` used to
    // panic on a non-char-boundary str slice in `text()`; the byte-level
    // self-closing check must handle it cleanly.
    assert_eq!(text("<x>é</x>", "x"), Some("é".into()));
    // Self-closing form with a multi-byte char in an attr value.
    assert_eq!(text(r#"<x a="é"/>"#, "x"), Some("".into()));
    assert_eq!(text("<x>日本語</x>", "x"), Some("日本語".into()));
    // Multi-byte char directly before the `>` of an unquoted-attr open tag.
    assert_eq!(text("<x a=é>body</x>", "x"), Some("body".into()));
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

// ── Malformed / truncated input (untrusted on-disc XML) ────────────────
// These scrapers run on attacker-controllable BD-J jar XML, so every scan
// must terminate and stay in bounds on truncated input (XML 1.0 §2.3).

/// A quoted attribute value that is never closed must terminate the
/// scan at EOF rather than reading past the end of the buffer.
#[test]
fn attr_unterminated_quoted_value_scan_stops_at_eof() {
    // The scanner enters the `y="` value and runs off the end looking
    // for the closing quote; `name` is never found.
    assert_eq!(attr(r#"<x y="oops"#, "name"), None);
    assert_eq!(attr("<x y='oops", "name"), None);
    // The truncated attribute itself has no terminated value either.
    assert_eq!(attr(r#"<x y="oops"#, "y"), None);
}

/// An attribute name at EOF followed only by whitespace (no `=`) must
/// return None, not read past the buffer while skipping that whitespace.
#[test]
fn attr_name_with_trailing_whitespace_and_no_equals_returns_none() {
    assert_eq!(attr("<x name   ", "name"), None);
}

/// `name=` followed only by whitespace to EOF has no value to return.
#[test]
fn attr_equals_with_trailing_whitespace_and_no_value_returns_none() {
    assert_eq!(attr("<x name=  ", "name"), None);
}

// A quoted attribute value is opaque: a `name="..."` pair that appears
// *inside* another attribute's value must never be reported, even when
// preceded by whitespace that would otherwise clear the word-boundary check.
#[test]
fn attr_decoy_name_after_space_inside_quoted_value_is_skipped() {
    assert_eq!(attr(r#"<x y=" name='decoy'" />"#, "name"), None);
    // The real attribute after the decoy still resolves.
    assert_eq!(
        attr(r#"<x y=" name='decoy'" name="real" />"#, "name"),
        Some("real".into())
    );
}

/// XML 1.0 §2.3 NameChar includes `-`, `_` and `.`, so `q-a`, `q_a` and
/// `q.a` are each a single attribute name distinct from `a`. Searching
/// for `a` must not match the tail of any of them.
#[test]
fn attr_name_char_boundary_covers_hyphen_underscore_and_dot() {
    assert_eq!(
        attr(r#"<x q-a="decoy" a="real" />"#, "a"),
        Some("real".into())
    );
    assert_eq!(
        attr(r#"<x q_a="decoy" a="real" />"#, "a"),
        Some("real".into())
    );
    assert_eq!(
        attr(r#"<x q.a="decoy" a="real" />"#, "a"),
        Some("real".into())
    );
}

/// An open tag truncated mid-attribute never terminates, so no element
/// can be returned — and the attribute walk must not read past EOF.
#[test]
fn find_element_unterminated_open_tag_returns_none() {
    assert_eq!(find_element("<x attr=", "x", 0), None);
}

/// A `/` as the final byte of the buffer is not a self-closing marker;
/// probing for the `>` that would follow it must stay in bounds.
#[test]
fn find_element_trailing_slash_at_eof_returns_none() {
    assert_eq!(find_element("<a /", "a", 0), None);
}

/// `/>` inside a quoted attribute value does not close the element.
#[test]
fn find_element_quoted_self_close_marker_does_not_end_element() {
    let xml = r#"<x a="/>"/>"#;
    let (s, e) = find_element(xml, "x", 0).unwrap();
    assert_eq!(&xml[s..e], r#"<x a="/>"/>"#);
}

/// An attribute value whose quote is never closed leaves the open tag
/// unterminated; the scan must end at EOF and report no element.
#[test]
fn find_element_unterminated_quoted_attr_returns_none() {
    assert_eq!(find_element(r#"<x a="oops"#, "x", 0), None);
}

/// A `/` in the middle of an unquoted attribute value is not a
/// self-closing marker — only `/>` is.
#[test]
fn find_element_unquoted_slash_is_not_self_closing() {
    let xml = "<a href=x/y>body</a>";
    let (s, e) = find_element(xml, "a", 0).unwrap();
    assert_eq!(&xml[s..e], "<a href=x/y>body</a>");
}

/// `text` must locate the real end of the open tag: a bare `/` inside
/// an unquoted attribute value must not be treated as `/>`, which would
/// shift the body start and leak tag bytes into the returned text.
#[test]
fn text_unquoted_slash_in_attr_does_not_truncate_body() {
    assert_eq!(text("<x a=b/c>hello</x>", "x"), Some("hello".into()));
}

/// A `>` inside a quoted attribute value must not be mistaken for the
/// end of the open tag when `text` computes the body start.
#[test]
fn text_quoted_gt_in_attr_does_not_truncate_body() {
    assert_eq!(text(r#"<x a="b>c">hello</x>"#, "x"), Some("hello".into()));
}

/// A close tag truncated mid-name (`</x` with no `>`) is not a close
/// tag; matching it must stay in bounds and report no text.
#[test]
fn text_truncated_close_tag_returns_none() {
    assert_eq!(text("<x>body</x", "x"), None);
}

/// A `/` in element content is only a close tag when preceded by `<`.
/// Body text containing `a/x>` must not be mistaken for `</x>`.
#[test]
fn text_slash_in_body_is_not_a_close_tag() {
    assert_eq!(text("<x>a/x> </x>", "x"), Some("a/x>".into()));
}

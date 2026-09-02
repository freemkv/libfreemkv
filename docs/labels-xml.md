# src/labels/xml.rs

Tolerant XML scraping helpers — promoted from two near-duplicate hand-rolls
in `paramount.rs` (attribute extraction) and `criterion.rs` (tag-text
extraction).

These are NOT a full XML parser. They handle the subset of XML the BD-J
authoring tools we've seen actually emit: ASCII tag/attr names, no entity
references inside label strings, optional XML namespaces. Hardening goals
over the prior `find("<tag>")` / `find(r#"name=""#)` matchers:

1. **Case-insensitive** tag and attribute names — vendors casing is
   inconsistent across authoring-tool revisions.
2. **Namespace-aware** — strip an optional `ns:` prefix so `<ns:playlist>`
   and `<playlist>` both match.
3. **Whitespace-tolerant** — multiple/tab/newline characters around `=`
   between attribute name and value; whitespace inside the opening tag.
4. **Quote-style tolerant** — both `"value"` and `'value'`.
5. **Self-closing tag handling** — `<tag />` and `<tag/>` both work;
   `text()` returns `Some("")` for empty content.

Out of scope (intentionally simple): XML entity decoding (`&amp;`, `&lt;`,
etc.), CDATA sections, comments, processing instructions, DTD
declarations. None of the BD-J authored disc data we've observed
exercises any of those — labels are plain ASCII/Latin-1 in attribute
values.

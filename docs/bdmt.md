# BDMV disc-library metadata (`bdmt.rs`)

Every commercial Blu-ray carries a disc-library metadata directory with one
XML file per shipped language. The schema is the Blu-ray "disc library
metadata" namespace (`urn:BDA:bdmv;disclibmeta`), conventionally prefixed
`di:`. Fields commonly present:

- `<di:title>` or `<di:name>` — the title string. Vendor practice varies
  (Paramount discs tend to use `<di:name>`).
- `<di:description>` — optional synopsis (often absent on retail discs;
  common on box sets and special editions).
- `<di:discNumber>` / `<di:numSets>` (or `<di:numberOfSets>`) — set position
  for multi-disc releases.

This module is intentionally separate from the BD-J `StreamLabel` parsers
under `labels/*.rs`. The XML here is disc-level (title, description, set
position), not per-stream.

Real-world XML is irregular: missing description elements, multiple title
elements (first one wins), and occasional malformed content. Extraction is
best-effort — a malformed file is treated as "no metadata" (returns `None`
from the helper), and the caller can still get metadata from
sibling-language XML files.

## `MAX_BDMT_BYTES`

The size comes from attacker-controlled UDF metadata; real files are a few
KB, so 1 MiB is generous while preventing a crafted huge-size entry from
triggering an oversized allocation in `read_file`.

## `parse_bdmt_xml`

Title-element preference: `<di:name>` → `<di:title>` →
`<di:tableOfContents>/<di:titleName>` (first match wins, per the
authoring-tool conventions documented above).

## `looks_like_xml`

Rejects candidate description strings that are themselves XML fragments —
observed on a captured disc, where `<di:description>` contained
`<di:thumbnail href="…"/>` child elements and no actual prose. Surfacing
that raw to the JSON output is worse than dropping the field entirely.

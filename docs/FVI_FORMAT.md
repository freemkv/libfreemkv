# FVI — Freemkv Video Index Format

**Specification version:** 1.0 (DRAFT)\
**File extension:** `.fvi`\
**Media type:** `application/vnd.freemkv.fvi+jsonl`\
**Status:** Draft for review. This document is the normative reference for the FVI
format; implementations and downstream tools cite it by section.

---

## 1. Scope and purpose

FVI is an open, codec-agnostic, byte-exact **index of the coded pictures** in a
video bitstream, together with **provenance** back to the source medium.

An FVI document answers, for every picture in a stream, three questions:

1. **Where is it?** — the byte-exact offset of its first byte in the *source*
   (the disc/ISO/file), so a reader can extract or seek to any picture without
   re-parsing the whole bitstream.
2. **What is it?** — coding type, random-access capability, GOP boundary, and
   (where the codec defines them) field/pulldown attributes.
3. **When is it?** — decode and presentation timestamps on a declared timescale.

FVI is **not** a container, a codec, or a copy of the bitstream. It indexes; it
never stores coded samples. It is the serialized form of an indexer's per-picture
truth — carried from the demuxer, **never reconstructed** (§9).

## 2. Conformance

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **MAY**, and **OPTIONAL** are to be interpreted as
described in BCP 14 (RFC 2119, RFC 8174) when, and only when, they appear in all
capitals.

A **conformant writer** MUST emit a document that satisfies §4–§10. A
**conformant reader** MUST accept any such document and MUST ignore unknown
object members (§11) so that forward-compatible extensions do not break it.

## 3. Terminology

- **Picture** — one coded video frame (or pair of fields coded as a frame). The
  unit FVI indexes.
- **Access unit (AU)** — the set of bitstream bytes that decode to exactly one
  picture (ISO/IEC 14496-10 §3; ISO/IEC 23008-2 §3).
- **Coded order** — the order pictures appear in the bitstream. FVI records are
  emitted in coded order.
- **GOP / coded video sequence** — a self-contained run beginning at a
  random-access point.
- **Provenance** — the mapping from an AU back to the exact bytes of the physical
  source it was read from (§9).
- **Source position (`src`)** — `{ file, sector, byte }`, the provenance anchor of
  an AU.

## 4. Encoding

An FVI document is a sequence of **UTF-8** text lines separated by a single LF
(`U+000A`). Each non-empty line is exactly one JSON value (RFC 8259), forming a
**JSON Lines / NDJSON** stream. A writer MUST NOT emit a UTF-8 BOM. A writer MUST
NOT pretty-print: each JSON value occupies exactly one line.

The first line MUST be the **Header** object (§6). Each subsequent line is one
**Picture record** (§7), in coded order.

Rationale: line-delimited JSON is streamable (a writer appends as it indexes; a
reader processes without loading the whole file), line-addressable (picture *n*
is near line *n+1*), append-safe, and parseable by every language without a
custom grammar — while remaining a precisely specified format, not an ad-hoc dump.

A document MAY be concatenated for multiple elementary streams: each stream is its
own header line followed by its records. Readers MUST treat a Header line as the
start of a new stream section.

## 5. Document structure

```
<header>            line 1            (exactly one Header object)
<record>            line 2 .. N       (one Picture record per picture, coded order)
[<header> <record>…]                  (OPTIONAL further stream sections)
```

## 6. Header object

| Member | JSON type | Req | Semantics / reference |
|---|---|---|---|
| `format` | string | MUST | Constant `"freemkv/video-index"`. Signature: a document begins with these bytes. |
| `fvi_version` | integer | MUST | Document format version. This spec defines `1`. |
| `generator` | string | SHOULD | Producing tool + version, e.g. `"freemkv/1.0.0-rc.6"`. |
| `stream` | object | MUST | The indexed elementary stream (§6.1). |
| `source` | object | MUST | Provenance root (§6.2). |
| `timescale` | integer | MUST | Ticks per second for all `pts`/`dts` (§10). E.g. `90000`. |
| `picture_count` | integer | MAY | Total pictures, if known at header time; OMITTED when streaming. |

### 6.1 `stream` object

| Member | JSON type | Req | Semantics / reference |
|---|---|---|---|
| `codec` | string | MUST | Registered codec id (Appendix B), e.g. `"mpeg2video"`, `"hevc"`. |
| `width`,`height` | integer | MUST | Coded luma dimensions in pixels. |
| `dar` | `[int,int]` | SHOULD | Display aspect ratio as `[num,den]`. |
| `frame_rate` | `[int,int]` | SHOULD | Nominal rate as exact rational `[num,den]` (e.g. `[24000,1001]`). |
| `scan` | string | MUST | `"progressive"` \| `"interlaced"` \| `"mbaff"`. |
| `colour` | object | SHOULD | CICP per ITU-T H.273: `primaries`,`transfer`,`matrix` (integer CICP codes or registered names), `range` (`"limited"`\|`"full"`). HDR: `mastering_display`, `max_cll`, `max_fall` per ITU-T H.273 / SMPTE ST 2086. |
| `language` | string | MAY | BCP 47 tag, if known. |

### 6.2 `source` object

| Member | JSON type | Req | Semantics |
|---|---|---|---|
| `medium` | string | MUST | `"disc"` \| `"iso"` \| `"file"` \| `"stream"`. |
| `path` | string | MAY | Source path/label. |
| `title` | integer | MAY | Title/program number. |
| `playlist` | string | MAY | Playlist/PGC identifier. |
| `volume_id` | string | MAY | Disc volume identifier, if read. |
| `sector_size` | integer | SHOULD | Bytes per `src.sector` unit (e.g. `2048`). Lets readers convert `src` to an absolute byte offset. |

## 7. Picture record

One JSON object per coded picture, in coded order.

| Member | JSON type | Req | Semantics / reference |
|---|---|---|---|
| `n` | integer | MUST | Coded-order index, 0-based, contiguous. |
| `src` | object | MUST | Provenance: `{ "file": int?, "sector": uint, "byte": uint }` — the offset of this AU's **first byte** in the source (§9). MUST be carried from demux, never reconstructed. |
| `type` | string | MUST | Coding type: `"I"` \| `"P"` \| `"B"` (ISO/IEC 13818-2 §6.3.9; H.264/H.265 slice types collapsed to frame type). |
| `key` | boolean | MUST | `true` iff this picture is an intra (I) picture / parser-flagged decode-restart point (IDR / IRAP / I-picture). MPEG-2 open-GOP clean-RAP precision (`closed_gop`) is not currently distinguished — see note below. |
| `gop` | boolean | SHOULD | `true` iff this picture begins a GOP / coded video sequence. Omitted when the implementation does not carry a distinct GOP-boundary signal. |
| `pts` | integer\|null | SHOULD | Presentation timestamp in `timescale` ticks; `null` if unknown. |
| `dts` | integer\|null | MAY | Decode timestamp in `timescale` ticks. |
| `size` | integer | MAY | AU length in bytes; enables byte-range extraction with `src`. |
| `recovered` | boolean | MAY | `true` iff any byte of this AU came from a retried/marginal read (§9.1). Default `false`. |
| codec ext | object | MAY | Codec-specific members under the codec's namespace (§8). |

The `type` and `key` members are **codec-agnostic** and MUST be populated for
every codec. `type` is the I/P/B coding type the parser decoded (collapsing
H.264/H.265 slice types to a frame type); where no per-picture coding is carried
(audio / synthetic frames), `type` is `"I"` for a key picture else `"P"`. `key`
is the picture's random-access flag as the codec parser sets it (IDR / IRAP /
I-picture). A writer MUST NOT emit a degraded record (`type:"?"` or `src:null`)
merely because a codec lacks per-picture coding info — those fallbacks are
reserved for a field that is genuinely unavailable (e.g. provenance absent on a
synthetic source).

> **Limitation (honest random-access).** `key` is set from the picture's
> intra / decode-restart flag. The per-picture coding model this index carries
> does **not** distinguish MPEG-2 open-GOP clean random-access points
> (`closed_gop`) from any other I-picture, so `key` is the parser-flagged
> decode-restart point, not a verified clean-RAP claim. A future revision MAY
> tighten `key` for codecs/profiles that carry that signal; readers MUST NOT
> assume present `key` precision beyond "intra / decode-restart point".

### 7.1 Interlace / pulldown fields

Codec-agnostic interlace/pulldown attributes, derived through the indexer's
per-picture coding accessors (MPEG-2: ISO/IEC 13818-2 §6.3.10). Emitted as
top-level members of the record, and ONLY when the codec actually measured the
signal — an OPTIONAL member that is omitted (not defaulted) when unknown:

| Member | JSON type | Req | Semantics |
|---|---|---|---|
| `field_order` | string | MAY | Display field order: `"tff"` (top field first) \| `"bff"` (bottom field first) \| `"progressive"` (no field order applies). Omitted when the codec did not signal it. |
| `progressive` | boolean | MAY | `true` iff the picture is progressive. Omitted when the codec did not signal it. |
| `nb_fields` | integer | MAY | Number of displayed field periods this picture occupies (the soft-telecine / 2:3 pulldown basis): `1` for a single field picture, `2` for a normal frame, `3`/`4`/`6` for `repeat_first_field` pulldown per §6.3.10. |

Codecs that carry only a coding type (e.g. H.264 / HEVC / VC-1 through this
pipeline) omit `field_order` and `progressive` rather than guessing a default.

## 8. Codec model and extensibility

Core record members (§7) are codec-agnostic and present for every codec.
Codec-specific data is either (a) promoted to top-level members for a small,
registered set per codec profile (e.g. MPEG-2 §7.1), or (b) placed under an
`ext` object keyed by codec id for richer/optional data:

```json
{
  "n": 42,
  "type": "P",
  "key": false,
  "src": {
    "sector": 17,
    "byte": 924
  },
  "ext": {
    "hevc": {
      "temporal_id": 0,
      "nal_type": 1
    }
  }
}
```

New codecs and members are added through Appendix B (codec registry) without a
breaking version bump, provided readers continue to ignore unknown members (§11).

## 9. Provenance and recovery semantics

`src` is **byte-exact** to the source as read. `src.sector` counts in
`source.sector_size`-byte units; `src.byte` is the offset within that sector of
the AU's first byte. For multi-file sources, `src.file` indexes a writer-declared
file list. Provenance MUST be the value observed at demux time; an implementation
MUST NOT recompute `src` by re-parsing — the point of FVI is to *carry* the truth.

### 9.1 Recovery

Because FVI is provenance-native, it can record reliability. A record with
`"recovered":true` indicates the AU's source bytes required retry/marginal-read
recovery. This lets downstream tools surface or quarantine pictures whose bytes
are not byte-identical to a clean read — a capability legacy index formats lack.

## 10. Time model

All `pts`/`dts` are integers in units of `1/timescale` seconds. `pts` is
presentation (display) time; `dts` is decode time. Records are in **coded**
(decode) order, so `pts` is not necessarily monotonic across records (B-pictures
reorder); `dts` is non-decreasing. Readers needing display order sort by `pts`.

## 11. Versioning and forward compatibility

- `fvi_version` is the document version; this spec defines `1`.
- **Additive** changes (new OPTIONAL members, new registered codecs) do NOT bump
  `fvi_version`. Readers MUST ignore members they do not recognize.
- A change that alters the meaning of an existing member or makes a new member
  REQUIRED bumps `fvi_version`.
- A reader encountering a higher `fvi_version` than it implements SHOULD process
  the members it understands and MUST NOT reject the document solely for the
  version being higher, unless a member it relies on is absent.

## 12. Conformance requirements (summary)

A conformant **writer** MUST: emit a Header first; emit records in coded order
with contiguous `n`; populate `src` from demux; use named/registered codec ids;
encode one JSON value per UTF-8 LF-terminated line.

A conformant **reader** MUST: accept any §4–§10 document; ignore unknown members;
not assume `picture_count`, `pts`, or `size` are present unless required above.

---

## Appendix A — JSON Schema (informative)

Header:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["format", "fvi_version", "stream", "source", "timescale"],
  "properties": {
    "format": { "const": "freemkv/video-index" },
    "fvi_version": { "type": "integer", "minimum": 1 },
    "timescale": { "type": "integer", "minimum": 1 },
    "stream": { "type": "object", "required": ["codec", "width", "height", "scan"] },
    "source": { "type": "object", "required": ["medium"] }
  }
}
```

Record:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["n", "src", "type", "key"],
  "properties": {
    "n": { "type": "integer", "minimum": 0 },
    "type": { "enum": ["I", "P", "B"] },
    "key": { "type": "boolean" },
    "src": {
      "type": "object",
      "required": ["sector", "byte"],
      "properties": {
        "file": { "type": "integer" },
        "sector": { "type": "integer", "minimum": 0 },
        "byte": { "type": "integer", "minimum": 0 }
      }
    }
  }
}
```

## Appendix B — Registered codec identifiers

| `codec` | Bitstream | Field profile |
|---|---|---|
| `mpeg2video` | ISO/IEC 13818-2 | §7.1 (field_order/progressive/nb_fields) |
| `mpeg1video` | ISO/IEC 11172-2 | §7.1 |
| `h264` | ISO/IEC 14496-10 | core + `ext.h264` |
| `hevc` | ISO/IEC 23008-2 | core + `ext.hevc` |
| `vc1` | SMPTE 421M | core |

## Appendix C — Normative references

- RFC 2119, RFC 8174 — Requirement keywords (BCP 14).
- RFC 8259 — JSON.
- ISO/IEC 13818-2 — MPEG-2 video (picture coding, §6.3.9–6.3.10).
- ISO/IEC 14496-10 — H.264/AVC. ISO/IEC 23008-2 — H.265/HEVC.
- ITU-T H.273 — Coding-independent code points (colour primaries/transfer/matrix).
- SMPTE ST 2086 — Mastering display colour volume (HDR).
- BCP 47 — Language tags.
- RFC 9559 — Matroska (alignment of colour/field-order semantics).

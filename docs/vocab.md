# `src/labels/vocab.rs` — shared label vocabulary

Labels come from BD-J authoring tool files (bluray_project.bin,
playlists.xml, menu_base.prop, .class string pools, etc.) — NOT from BD
spec fields. This module is the central, regression-tested source of
truth for:

- Codec brand name aliases (`MLP` → `TrueHD`).
- English / multi-word language name → ISO 639-2 code.
- English text → `LabelPurpose` (Commentary / Descriptive / etc.).
- English text → `LabelQualifier` (SDH / Forced / Descriptive Service).

Not for BD spec STN codec IDs; those decode in `mpls.rs` separately.

## Rules of engagement

1. Only map values we are 100% certain about — published codec names,
   well-known ISO 639-2 mappings, vendor-documented purpose keywords.
2. Unknown codes / unrecognized phrases pass through raw or return
   `None`. We never guess.
3. Matching is case-insensitive and word-boundary-aware where relevant
   (so "Commenter" doesn't match "commentary"). Anchoring on whole
   tokens is the responsibility of this module — callers pass raw
   text, we handle it.

## `lang()` — free-form language name → ISO 639-2 + variant

Handles both bare English names ("English", "Spanish") and the
multi-word vendor variants seen in the corpus ("Brazilian Portuguese",
"Castilian Spanish", "Canadian French"). Match is case-insensitive.

Compound phrases are scanned BEFORE bare names, so "Brazilian
Portuguese" returns `LangInfo { code: "por", variant: "Brazilian" }`
rather than being consumed by the bare "Portuguese" entry. Within
`COMPOUND_LANGS` the scan is positional (first `contains` hit wins), so
that table MUST be maintained longest-first — a longer phrase must
precede any shorter phrase it contains (e.g. "latin american spanish"
before "latin spanish").

Bare-name matches return `variant: ""`. Returns `None` for unrecognized
input — callers decide whether to fall back to MPLS spec codes, pass
through raw, or drop the stream. Never guesses.

Why the variant: returning only the ISO code would silently drop
regional dialect info — "Brazilian Portuguese 5.1" would become
`language="por", variant=""` and the UI would display plain
"Portuguese" even though the disc explicitly labeled the stream
Brazilian. Returning the variant lets callers populate
`StreamLabel::variant` with the dialect.

## `iso639_1_to_iso639_2()` — the whole ISO 639-1 set

Covers the WHOLE of ISO 639-1, unlike `menu_lang`, whose table only
spans the languages that show up in Blu-ray menu-graphic filenames.
Callers that convert a spec field — a DVD IFO attribute block, say —
need the whole set: narrowing it to the menu vocabulary would fold
every other language onto one value and make a disc's tracks
indistinguishable from each other.

`ISO_639_1_TO_2` pairs every ISO 639-1 two-letter code with its ISO
639-2/**T** (terminological) code — the variant the rest of this crate
uses (`deu` not `ger`, `fra` not `fre`, `zho` not `chi`, plus `ces`,
`nld`, `ell`, `ron`, `slk`, `isl`, `eus`, `hrv`) — so `lang` and
`menu_lang` both normalize to it and the three tables cannot disagree
(`iso639_1_agrees_with_menu_lang` pins that). For the 165 codes where
639-2/B and /T are identical this distinction does not arise; it only
matters for the 20-odd languages with a distinct bibliographic code.

`ISO_639_1_DEPRECATED` covers the three two-letter codes ISO 639-1 has
since withdrawn, mapped to their replacements. DVD-Video froze its
language list on the 1988 edition, so discs authored to the spec carry
these spellings and no other table sees them: `iw` Hebrew (now `he`),
`in` Indonesian (now `id`), `ji` Yiddish (now `yi`).

## `purpose()` / `qualifier()` — English text → enum

`purpose()` recognized keywords (case-insensitive; single-word keywords
are word-boundary matched, multi-word phrases are substring matched):
- "commentary", "director's commentary" → `Commentary`
- "descriptive", "description", "audio description", "described" → `Descriptive`
- "score", "music only" → `Score`
- "ime" (alternate music for closing themes etc.) → `Ime`
- anything else → `Normal`

Word-boundary matching means "Commentary track" matches but "Commenter
Pro audio" does not. Multi-word phrases like "audio description" and
"music only" are matched as plain substrings.

`qualifier()` recognized keywords (case-insensitive, word-boundary
matched):
- "sdh", "captions" → `Sdh`
- "forced", "forced narrative" → `Forced`
- "rnib", "descriptive service" → `DescriptiveService`
- anything else → `None`

SDH (Subtitles for the Deaf and Hard of hearing) wins over Forced when
both keywords are present, because an SDH track is its own stream
regardless of whether the player flags it as "forced".

## `has_word()` — word-boundary primitive

The load-bearing primitive for `lang` / `purpose` / `qualifier`:
bare-token matchers MUST use it, otherwise we match "english" inside
"englishman" and "sdh" inside "lambdash". The existing parsers used
`.contains()` and got lucky on the corpus; vocab guarantees the
boundary.

Char-aware, not byte-level: an accented/CJK char adjacent to the match
is alphanumeric and so NOT a boundary, preventing false positives like
"sdh" inside "cafésch". Needles are ASCII, so byte offsets align with
char bounds.

## Test rationale (mutation-testing notes)

- `purpose_descriptive_service_substring_without_word_boundary`: the
  multi-word-compound fast path in `purpose()` ORs two independent
  phrase checks ("audio description" / "descriptive service"). Each
  phrase, when it appears as a *word*-bounded match, is independently
  caught by the `has_word` fallback further down — so the OR only
  matters when a phrase appears as a *substring inside a larger word*
  (no boundary), which `.contains()` still catches but `has_word()`
  would reject. Mutation: replace `||` with `&&` at the compound check
  → since "audio description" is absent in the test input, the AND
  fails, the fast path doesn't fire, and the fallback
  `has_word("descriptive")` also fails (no word boundary before
  "descriptive" in "nondescriptive"), so `purpose()` wrongly returns
  `Normal` instead of `Descriptive`.

- `menu_lang_covers_every_table_entry`: `menu_lang()` maps every
  authoring-filename token in its table (ISO-639-2/B and /T spellings,
  plus ISO-639-1) to the canonical /T code used by the rest of the
  pipeline. Exhaustive per-arm check: deleting any single match arm
  makes that arm's tokens return `None` instead of the documented code.

- `iso639_1_table_is_complete_and_well_formed`: structural invariants
  of `ISO_639_1_TO_2` — it must hold the complete ISO 639-1 set (184
  codes), every key a distinct pair of lowercase letters and every
  value three lowercase letters. A typo'd or duplicated row fails here
  rather than silently mislabelling a track.

- `iso639_1_agrees_with_menu_lang`: the two tables must not disagree.
  Every two-letter token `menu_lang` accepts has to yield the same ISO
  639-2/T code through `iso639_1_to_iso639_2`, so a DVD-sourced
  language and a Blu-ray menu-label language for the same tongue never
  produce different `Language` elements.

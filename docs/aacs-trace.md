# AACS resolution trace shape

`src/aacs/trace.rs` defines the structured trace of a resolution attempt (the
unlock phase, then the key-resolution phase).

- No user-facing English: every step's STATE is a typed enum variant.
  Applications render these into localized text — the library never does.
  This module only defines the shape and is wired through the
  resolve/handshake return path far enough to compile.
- The `who` of each step is the source's `label()` / unlocker's `name()` — a
  stable identifier string (a NAME, like a codec id, NOT user-facing prose),
  carried verbatim so an app renderer never has to match an enum back to a
  name it already has. Only the OUTCOME / path enums are structured states
  the app maps to i18n English.

## `KeyStep` — a source's contribution to the key phase

Each source that ran contributes one `KeyStep`: `who` (its `label()`), `path`
(the `Vec<KeyNode>` it walked), and `outcome` (`KeyOutcome`). Two fields
de-conflate a keyless result — WHY a matched disc produced no usable key:

- `matched_entry: Option<MatchedEntry>` — the shape of the source's MATCHED
  entry when it matched this disc; `None` for a true miss or a source kind
  with no such shape. An app MAY log it verbatim and render a
  matched-but-no-key verdict.
- `store_entries: Option<usize>` — per-disc entries loaded in the source's
  store, when known, so a true-miss verdict can name the store size (`… not in
  keydb (N entries loaded)`). `None` for a source that carries no such count.

## `MatchedEntry` — booleans-and-lengths, never material

A shape summary of a matched entry — safe to print at any log level, no key
MATERIAL: `has_vuk`, `has_unit_keys`, `unit_keys_len`, `has_media_key`,
`has_keydb_vid`, `enc_title_keys_len`, `vid_available`. It answers "the disc
WAS found — so why no key?" (e.g. a Media Key present but no VID on this path).

## `KeyNode::NoDerivableKey`

A derivation-path node distinct from `NoEntry`: the source MATCHED this disc
but could not derive any usable key from the matched entry (no derivation
material for the path taken). `NoEntry` = the disc was not found at all;
`NoDerivableKey` = found, but the key was not — the node that turns a flat
`no entry` verdict into `matched disc > … > NO KEY`. See docs/keysource.md
(`resolve_unit_keys`) for how a source populates it.

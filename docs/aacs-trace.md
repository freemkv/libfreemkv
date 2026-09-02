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

# hex

The single hex → bytes parser for the whole workspace.

## Why one parser

Key material arrives as hex from three third-party sources — the keydb, an
online key service, and the mapfile's `# freemkv-vid:` comment — and each
used to parse it slightly differently (one stripped `0x`/`0X`, one stripped
nothing, one stripped `0x` only). A key written with a prefix one parser
didn't expect was silently dropped → "can't decrypt" with no error. This is
the one parser they all call, so the prefix/case/validation rules live in
exactly one place.

## Byte-oriented, not char-oriented

Operates on BYTES, not `&str` char indices: the inputs are untrusted, so a
multi-byte UTF-8 scalar must reject as malformed, never panic on a
mid-codepoint slice.

## `byte()` internals

Combines two ASCII hex-digit bytes into one byte. `as char` is intentional:
for a non-ASCII byte it produces a Latin-1 scalar that `to_digit(16)` then
rejects — so non-hex (incl. `+`/`-` sign chars) and multi-byte input fail
cleanly rather than slipping through `from_str_radix`'s sign handling.

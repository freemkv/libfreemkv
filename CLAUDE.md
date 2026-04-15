# libfreemkv — Rules

## No English in library code

The library contains ZERO user-facing English text. All errors use numeric codes from `error.rs`. Applications (CLI, GUI, server) handle i18n.

- `io::Error::new(kind, "english string")` — NEVER. Use `Error::VariantName.into()`.
- If you need a new error, add a variant to `error.rs` with a code, not a string.
- Acceptable strings: debug/trace logging, test assertions, comments, data format strings (paths, codec IDs).
- `Error` implements `From<Error> for io::Error` — use `?` or `.into()` anywhere an `io::Error` is expected.

## Architecture

- **Streams are PES.** Every stream reads its format → PES frames out, or PES frames in → writes its format. One type per format.
- **Disc::copy() for sector dumps.** disc→ISO is NOT a stream. It's `Disc::copy()`.
- **DiscStream = any disc.** Physical drive or ISO file. Same type, different SectorReader.
- **No IOStream.** Deleted. No byte-level Read/Write on streams.
- **Streams don't know their size.** Progress/file_size is a CLI concern.
- **One method per action.** No `foo_with_X` variants. Use `Option<T>` params.
- **Streams impl Read only (conceptually).** No Seek, no File backing.
- **Functions return errors, only main() exits.** No `process::exit` in library code.

## Device rules

- Always use `/dev/sg*` not `/dev/sr*` for SCSI.
- `--raw` only skips decryption. Init/probe/speed still run.
- Each function does one thing. One runner orchestrates the sequence.

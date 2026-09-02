# Error Codes

Overflow detail for `src/error.rs` doc comments that would otherwise exceed
the comment-guard caps. Each section below is pointed to by a short `//` or
`///` comment at the corresponding definition.

## Error Code Ranges

| Range | Category |
|-------|----------|
| E1xxx | Device errors |
| E2xxx | Profile errors |
| E3xxx | Unlock errors |
| E4xxx | SCSI errors |
| E5xxx | I/O errors |
| E6xxx | Disc format errors |
| E7xxx | AACS errors |
| E8xxx | Keydb errors |
| E9xxx | Stream/mux errors |

## E_UDF_NO_USABLE_EXTENT

A file that EXISTS and whose allocation descriptors resolved without error,
yet yields not one usable extent: an empty AD list, or a list every entry of
which is zero-length or points at LBA 0. Reported by the HD-DVD clip resolver
(`disc::hddvd`).

It has no `Error` variant on purpose. `UdfFs::file_extents` returns
`Ok(vec![])` here rather than failing — the emptiness is only a defect in the
eye of a caller that needs bytes — so the condition is DETECTED by the
caller, not returned to it. The code exists so that detection can be
accounted in the log with something other than a neighbouring error's code:
logging it as E6017 would file a zero-length AD list as an authoring hole
and send whoever triages it at the wrong population.

## E_CSS_NO_DISC_KEY / CssNoDiscKey history

While the disc-wide CSS failure and the per-title CSS failure both used
`E_CSS_KEY_MISSING`, an uncrackable CSS disc was iterated title by title,
each one logged as skipped, and the run exited reporting success — a total
failure reported as success. `E_CSS_NO_DISC_KEY` / `Error::CssNoDiscKey` was
split out so `is_disc_level_no_key` classifies it and a multi-title rip loop
fails fast on it instead.

## E_KEY_SERVICE_UNAVAILABLE

A seven-hour run of HTTP 502s reported as `E_NO_DISC_KEY` told operators
their disc was not in the key database and sent them hunting for a VUK that
was never missing; the correct action was to wait. `E_KEY_SERVICE_UNAVAILABLE`
exists so a key-source outage (transport error, DNS failure, timeout, TLS
failure, HTTP 5xx, unreadable reply) is distinguishable from a source that
answered and simply has no key.

## E_MKV_SOURCE_INVALID vs E_MKV_INVALID

`is_skippable_title_stub` classifies `E_MKV_INVALID` as a title that yielded
no muxable frames, which an all-titles rip may skip while finishing the
rest. A corrupt or truncated `mkv://` source file is a FAILURE, not a stub —
reporting it as skippable would let a broken source be silently passed over
by a run that then exits successfully. `E_MKV_SOURCE_INVALID` (and the
sibling codes `E_MUX_HEADER_BUFFER_EXCEEDED`, `E_MKV_LACING_INVALID`) exist
to keep that distinction, and are fatal.

## UdfEmbeddedData

A file that legitimately stores its data this way is tiny (an ICB caps it at
well under 2 KiB — the AACS `*.inf` key files are the usual case), and the
callers that expect one read it via `read_inline_data` long before extents
are ever requested. A stream file cannot be one. `read_directory` already
refuses the same shape for directories; `UdfEmbeddedData` is the file half
of that decision.

## MkvInvalid / MuxHeaderBufferExceeded history

`MuxHeaderBufferExceeded` exists because hundreds of megabytes of real
frames with unresolvable codec-init headers is NOT the same thing as
`MkvInvalid`'s canonical case (an empty nav/menu PGC stub). Reporting the
former as the latter would have `is_skippable_title_stub` silently drop a
main feature from a rip that then exits successfully.

## AacsBusKeyUnavailable

NOT raised for AACS 1.0 BD (no bus encryption, `read_data_key` legitimately
absent) nor for file-backed (ISO) scans, where bus encryption was already
removed at read time and no handshake runs. Only a live-drive AACS 2.0/UHD
scan that obtained the Volume ID but produced no bus key raises this.

## SourceTerminated

The alternative answer is a lie: a dead source that reports `Ok(0)` is
indistinguishable from end-of-stream, and `DiscStream::fill_extents`
legitimately reads a short count as a skippable hole — zero-filling and
advancing over every remaining sector of the title and still returning
success. Unlike a bad sector, this condition cannot be retried at a smaller
size or skipped past.

## error_code / From<Error> for io::Error round-trip

`mux_stream` hands back an `io::Error`, and a front-end reporting *why* a
title failed had no way to recover the code from it — the typed `Error` is
gone by then and only the `E<code>` string prefix survives. Parsing that
prefix is `error_code`'s job; every consumer re-implementing the parse is how
the string-matching this crate spent 1.5.x removing comes back.

`From<Error> for io::Error` boxes the typed value as the payload
(`io::Error::new(kind, e)`), whose `Display` is the same `E<code>[: …]`
string the stringifying version produced — so `error_code`'s parse is
unaffected, and `From<io::Error> for Error` can additionally `downcast` the
payload back to the exact typed error. Errors that did NOT come from this
crate carry no `E<code>` prefix and yield `None`.

## is_skippable_title_stub

This replaces the CLI's `E7023`/`E6008` string-match with a typed check on
the `io::Error` `mux_stream` returns.

`Error::MkvSourceInvalid`, `Error::MkvLacingInvalid`, `Error::MkvUnencodable`
all used to be raised as `Error::MkvInvalid` and therefore landed in the
skippable set, so a corrupt source was reported as a title worth silently
passing over by a run that then exited successfully. They now carry their
own codes and are fatal, as is `Error::MuxHeaderBufferExceeded`.

`Error::CssNoDiscKey` (the disc's CSS crack recovered no key at all) is the
same conflation on the decrypt axis: while it too was raised as
`Error::CssKeyMissing`, every title of an undecryptable disc classified as a
skippable stub, so the rip loop skipped all of them and exited successfully.
It belongs to `is_disc_level_no_key` instead, and is fatal here.

## is_disc_level_no_key

`E_CSS_NO_DISC_KEY` is the CSS side of the disc-level/per-title split, and it
exists because the disc-wide CSS failure used to be raised as the per-title
`E_CSS_KEY_MISSING`: an undecryptable CSS disc landed in
`is_skippable_title_stub`, so the rip loop skipped all N titles with an
"empty stub" notice and exited successfully.

The key-SOURCE failures (`E_KEY_SERVICE_UNAVAILABLE`,
`E_KEY_SERVICE_UNAUTHORIZED`, `E_KEY_SERVICE_RATE_LIMITED`) are classified
here for the same fail-fast reason and NOT because they mean "no key": a
service that is down, refusing the token, or throttling is down for every
title on the disc, so iterating N titles re-issues N doomed requests (and,
on 429, digs the rate-limit hole deeper). They are separate CODES precisely
so the front-end can say "retry later" / "fix the token" instead of
`E_NO_DISC_KEY`'s "no key source has a key for this disc".

## is_marginal_read

- MEDIUM ERROR (sense key 3) — canonical bad-sector signal.
- ABORTED COMMAND (sense key B) — transient; retry usually works.
- NOT READY (sense key 2) — the dominant bad-sector response on the BU40N
  (ASC 0x04/ASCQ 0x3E); a pause + retry often recovers.
- RECOVERED ERROR (sense key 1) / NO SENSE (sense key 0) — not classified as
  fatal; treat as recoverable.

Caller-agnostic predicate — describes a property of the *error*, not what
one specific call site should do with it. Used by
`freemkv_engine::recovery::copy`'s hysteresis dispatch.

## Test rationale: declared_error_codes / uniqueness

The uniqueness test used to carry a hand-maintained `vec![]` of the error
constants, with a doc comment claiming it "pins all code assignments". It did
not. At the time this was written the file declared 127 `pub const E_*` and
the vector named 109 of them: eighteen codes — `E_DIR_IMAGE_FANOUT`,
`E_DIR_INSUFFICIENT_SPACE`, `E_DIR_MULTIPASS_REJECTED`, `E_DIR_NAME_COLLISION`,
`E_DIR_NAME_TOO_LONG`, `E_DIR_NOT_EMPTY`, `E_DIR_RAW_REJECTED`,
`E_DIR_SOURCE_UNSUPPORTED`, `E_DIR_WRITE_FAILED`, `E_DRIVE_INQUIRY_SHORT`,
`E_EMPTY_IMAGE`, `E_MP4_UNKNOWN_RESOLUTION`, `E_SEAM_PLAN_DROPPED_MOST`,
`E_SHORT_IMAGE_READ`, `E_SINK_WROTE_NOTHING`, `E_SOURCE_TERMINATED`,
`E_SYNC_TIMEOUT`, `E_SYNC_WORKER_LOST` — were outside the guarantee entirely,
so a new variant colliding with any of them passed green. Worse, an earlier
audit READ that doc comment and trusted it while assigning new codes.

The defect is not the eighteen omissions, it is that the list is
hand-maintained at all: adding a constant and forgetting the vector is a
silent no-op, which is the definition of a guarantee that decays. Deriving
the list from the declarations makes forgetting impossible — the only way to
escape the check is to stop declaring the constant. `include_str!` of the
same file is the cheapest seam that does that: no `build.rs`, no proc macro,
no dependency, and — being `#[cfg(test)]` — not one byte of the source
embedded in a release build.

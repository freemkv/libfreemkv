# Seeded robustness harness (`src/harness.rs`)

Every parser reached from here takes bytes that came off a disc, and this
crate's primary boundary is that the disc is untrusted: a malformed, damaged
or hostile image must never crash the library. These tests assert exactly
that one property — the parser returns `Ok` or `Err`, and never panics.

## Why this exists rather than `cargo-fuzz`

`cargo-fuzz` needs a nightly toolchain (`-Zsanitizer` plus SanitizerCoverage
for libFuzzer's coverage feedback) and this project pins stable. So the
generator lives here instead. It gives up coverage-guided mutation — the real
loss — and keeps everything else: millions of cases, structure-aware input,
and a crash corpus. It also gains determinism, which a fuzzer does not have:
the same seed replays the same cases on any machine.

## Why no `proptest` or `arbitrary`

This crate has exactly one dev-dependency. That is a deliberate posture, and
a randomness crate is not worth ten transitive dependencies when the parsers
take plain `&[u8]` and a good enough generator is forty lines.

## Budget

`FREEMKV_HARNESS_CASES` sets cases per generator per target (default 256, low
enough that the per-commit gate stays under a second). The overnight run sets
it to millions. `FREEMKV_HARNESS_SEED` overrides the seed; the default is
fixed so a failure in CI reproduces locally verbatim.

## On failure

The panic message carries the seed, generator and case index. Re-run with
`FREEMKV_HARNESS_SEED=<seed>` to reproduce, then write the offending bytes
into `tests/corpus/` as a permanent regression fixture — discovery happens
here, defence happens there.

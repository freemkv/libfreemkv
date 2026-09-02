# src/testlog.rs — test-only `tracing` capture

Test-only capture of `tracing` events, so the crate's logging contract is
ENFORCED rather than merely commented.

## Why this exists

The error contract is "Account / Log / Classify", and the round-2 audit
found the third leg unverifiable: three separate sites carry long comments
insisting they log **the error's OWN code, not a fixed one** — because
flattening a scratched sector (E6000) or an over-long allocation-descriptor
chain (E6016) into E6017 sends whoever triages them after authoring holes
and hides the population that actually exists. Nothing tested that. Putting
a literal back at `bluray.rs`'s or `hddvd.rs`'s warn sites broke no test, so
the guarantee was a convention one careless edit away from being false. Two
of those very sites were changed in round 1, and a third still carried a
hardcoded `code = 6017`.

Likewise "absence of a log is itself a bug": a refusal that returns the
right error but says nothing produces the wrong population downstream (a
residual-underrunning drive is indistinguishable from a scratched disc).
That is only checkable by looking at what was emitted.

## Why a hand-rolled subscriber and not `tracing-subscriber`

Same posture as `crate::harness`: this crate has exactly one dev-dependency
on purpose. `tracing-subscriber` would pull a tree of them to do what forty
lines of `tracing::Subscriber` does here. The capture is installed with
`tracing::subscriber::with_default`, which is THREAD-LOCAL — so it composes
with `cargo test`'s parallel harness and two capturing tests cannot see
each other's events.

Field values are stringified through `std::fmt::Debug`/`Display` because
that is all the visitor API offers without a typed schema; tests compare
against the string form of the expected constant, which is exactly the
comparison that catches a hardcoded code.

## `capture()`: why one global subscriber, not scoped `with_default`

`tracing`'s per-callsite INTEREST CACHE is GLOBAL, but `with_default` is
thread-local. Under `cargo test`'s parallel harness a rebuild of that cache
— triggered by ANY other thread registering any callsite for the first time
— re-evaluates the target callsite against the global dispatcher, which a
scoped subscriber is NOT part of, and can leave it cached "off" while a
capture is live, so the capture observes NOTHING. Serialising captures
against each other does not help: the poisoning thread is not itself
capturing. `parse_playlist_unreadable_clip_icb_yields_no_title` flaked ~1
run in 15 on exactly this — an empty event list.

The robust shape is a single subscriber installed globally for the whole
run, whose `register_callsite` returns `sometimes` (so no callsite is ever
hard-cached) and which routes each event to the emitting thread's own sink.
No scoped-dispatcher transitions, no cross-thread cache race, and concurrent
captures on different threads stay isolated by the thread-local sink.

## `capture_records_target_level_and_fields` test rationale

The capture must actually see events and their field VALUES — if it
silently recorded nothing, every logging assertion built on it would
pass vacuously, which is worse than having no harness at all.

Mutation: an `enabled()` returning `false`, or an `event()` that drops
the visitor's fields, fails here.

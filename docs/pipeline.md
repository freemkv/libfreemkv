# `src/io/pipeline.rs` — bounded producer/consumer pipeline

The file-backed mux highway is built on this primitive. (An earlier sweep
sink also used it; sweep moved to freemkv-engine in 1.6.0.)

## Cancellation and error semantics

- Producer dropping the channel (via `Pipeline::finish` dropping `tx`)
  signals end-of-stream; consumer flushes via `close()` and returns its
  `Output`.
- Consumer returning [`Flow::Stop`] also calls `close()` and returns its
  `Output`. `send()` from the producer will then either succeed (if the
  item already fit in the channel buffer) or fail with `Err(item)` once
  the consumer has dropped its receiver.
- Consumer returning `Err` from `apply` skips `close()` entirely; the
  consumer keeps draining the channel so the producer never blocks on a
  dead receiver, and the first error is propagated as the `JoinHandle`
  result.
- Consumer panic is converted into [`Error::PipelineConsumerPanicked`]
  (the panic message is logged, not embedded in the error value).

## Debug logging

Set `FREEMKV_DEBUG=1` environment variable to enable verbose debug
logging throughout the pipeline (channel sends/receives, backpressure,
consumer lag detection). This is critical for diagnosing stalls.

## `JOIN_TIMEOUT_SECS` / `FINISH_GRACE_SECS`

`JOIN_TIMEOUT_SECS` (10 minutes) is a backstop, not a normal timeout —
the consumer is expected to drain in seconds. If it fires, something is
wedged inside a kernel call the consumer thread can't unwind from. It is
deliberately long so a consuming application's own (shorter) stall
watchdog gets the first chance to escalate; this join only fires when no
such watchdog intervenes.

`FINISH_GRACE_SECS` is the short grace period spun after a halt or the
10-minute timeout fires in `Pipeline::finish_with_halt`. Most wedged
consumers that are "about to return" when the halt fires will unblock
within a few seconds (e.g. their bounded_syscall timeout returns and the
consumer drains). Spinning here converts those into clean joins and
releases the output file handle, at the cost of at most this much extra
latency on a genuinely stuck consumer before the leak is accepted.

## `SEND_HALT_CHECK_INTERVAL`

Halt-check cadence for the send loop. Producer blocks on
`crossbeam_channel::Sender::send_timeout` for this slice — the kernel
wakes it the instant the consumer drains a slot, so on the happy path
there's no throughput cap from this primitive at all (the cap is
whatever the underlying medium can sustain). When the consumer is
genuinely wedged, the timeout fires every `crate::halt::POLL_INTERVAL`
and the producer checks the halt token; that's the latency a stop
request will observe. Single source of truth for the interval lives in
`crate::halt::POLL_INTERVAL` (also used by `bounded_syscall`); aliased
here for readability of the send/finish call sites.

0.21.7 replaced an old `std::sync::mpsc::sync_channel` + 50 ms
`thread::sleep` polling loop that capped mux throughput at ~20
frames/sec ≈ 1 MB/s on saturated channels.

## `consumer_panicked`

Turns a consumer-thread panic payload into the numeric
`Error::PipelineConsumerPanicked` variant. The original panic message
(the two stdlib formats `panic!` produces: `&str` / `String`) is logged
at the join site for diagnostics — it is NOT baked into the error value,
since the library carries no English text in its errors. Callers
discriminate on the variant.

## `state` module

Consumer lifecycle state, shared between the caller and the consumer
thread. A plain `AtomicBool` could not make "the caller abandons" and
"the consumer commits to finalising" mutually exclusive: the consumer
loaded the flag, the caller stored it, and the consumer then finalised
the container anyway — the caller reporting the rip as interrupted while
a fully finalised MKV (Cues written, Segment size patched) landed on
disk, indistinguishable from a complete one. The two transitions are
therefore a single compare-exchange each, out of `state::RUNNING`:
whoever wins decides, and the loser observes the winner.

## `finish_with_grace`

After a halt or deadline fires, spin-polls `handle.is_finished()` for
`grace` before accepting the thread leak. This converts the common
"nearly-done" consumer (whose own bounded_syscall just returned and is
about to drop its output file) into a clean join, releasing the file
handle without waiting the full grace period.

If the consumer is still running when the grace expires, the `abandoned`
flag is set and the `JoinHandle` is dropped, detaching from the thread.
The leaked consumer keeps running until its current kernel call returns,
then — observing `abandoned` — exits WITHOUT calling `close()`, so it
does not finalise an output the caller has already reported as failed.
It does not unblock the in-flight syscall itself; that still returns on
its own (or at process exit).

## `Pipeline::spawn` naming history

This module's only in-crate `spawn_named` caller is the mux driver,
which names its thread `freemkv-mux-consumer`; `Pipeline::spawn` (the
default name) is used only by this module's unit tests. Earlier
revisions of this doc twice named a caller that had left the crate
(`disc::patch`, then Sweep's `freemkv-sweep-consumer` thread); both went
to freemkv-engine with the recovery passes in 1.6.0, and each in turn
sent readers hunting a component that is not here. Name callers that
live in THIS crate, or none.

## `Pipeline::send` vs `send_with_halt`

`send_with_halt` is NOT a `foo_with_X` variant of `send`, despite the
name — the two encode OPPOSITE policies on the same event, each with its
own test: after the consumer's `apply` has failed, `send` still succeeds
(the consumer keeps draining, so the channel accepts the item), while
`send_with_halt` hands the item straight back — so a producer does not
read an hour of disc for a write that died on the first frame.
Collapsing them into one Option-parameterised method deletes one of
those behaviours; it was tried and `apply_error_drains_then_propagates`
caught it.

## `Pipeline::finish` vs `finish_with_halt`

Likewise not a `foo_with_X` variant: `finish` joins and waits however
long the consumer needs, while `finish_with_halt` gives up after
`JOIN_TIMEOUT_SECS` and reports halted. Which is right depends on
whether the caller has a user waiting to cancel — the mux driver does
and uses `finish_with_halt`; the unit tests do not and use the plain
join. Merging them means picking one of those policies for both.

## Test notes

- `empty_pipeline_still_calls_close`: zero items sent, closing the
  pipeline immediately must still call `close()` exactly once and
  return its `Output`. The consumer loop's `while let Ok = rx.recv()`
  exits on the dropped `tx` with zero iterations, then runs
  `sink.close()`. Mutation: moving `close()` inside the loop would
  never call it here.
- `close_error_propagates_from_finish`: `close()` returning `Err` must
  surface that error from `finish`, not be swallowed — a clean producer
  drop still flushes via `close()` and an `Err` `Output` is a valid
  return. Mutation: if the consumer ignored `close()`'s `Result` and
  returned `Ok`, this fails.
- `try_send_reports_full_when_saturated`: `try_send` must report `Full`
  when the channel is saturated and the consumer is wedged, NOT block.
  Wedges the consumer on the first item (depth=1), fills the one buffer
  slot, then asserts `try_send` returns `Full` immediately. Mutation:
  routing `try_send` to the blocking `send` would hang.
- `try_send_reports_disconnected_after_consumer_gone`: `try_send` must
  report `Disconnected` once the consumer thread has exited (here via a
  panic), with the item handed back inside the `Disconnected` variant.
  Mutation: if `try_send` mapped `Disconnected`→`Full` it would
  mis-signal a permanently-dead consumer as transient backpressure.
- `send_returns_item_after_consumer_panicked`: plain `send` must hand
  the item back via `Err(item)` once the consumer has gone away
  (panic). The first send may race the panic, so the test loops until
  one fails and asserts the returned item identity. Mutation: if
  `send`'s `Err` arm returned a different/default item, the identity
  assert fails.
- `send_with_halt_returns_item_on_disconnect`: `send_with_halt` must
  return the exact item via `Err(item)` when the consumer has
  disconnected (the `Disconnected` arm). Panics the consumer, waits for
  it to fully exit, then calls `send_with_halt` with a live halt and a
  long deadline — the only way it can return `Err` is the disconnect
  arm. Mutation: if that arm returned a default item instead of the
  channel's returned item, the identity assert fails.
- `finish_with_halt_none_does_not_spuriously_halt`: `finish_with_halt(None)`
  with a wedged consumer and NO halt token must NOT return early — it
  must keep polling until the `JOIN_TIMEOUT_SECS` deadline (it cannot
  observe a halt that was never supplied). Since the test can't wait 10
  minutes, it asserts the weaker but still-meaningful property that with
  a `None` halt and a wedged consumer, `finish_with_halt` does not
  return within a short window (genuinely blocked, not spuriously
  returning `Halted`), then releases the consumer and confirms `Ok`.
  Mutation: if the `None` branch erroneously treated `None` as
  "cancelled", it would return `Halted` immediately and this fails.
- `finish_with_halt_joins_cleanly_when_consumer_finishes_in_grace`:
  regression for the "consumer thread / output-file leak on halt" fix.
  When the halt fires but the consumer finishes WITHIN the grace period,
  `finish_with_halt` must join cleanly and return `Ok` — not leak the
  thread or return `Err(Halted)`. Uses a sink that sleeps briefly (well
  inside `FINISH_GRACE_SECS`) after the producer drops the channel;
  fires the halt immediately so `finish_with_halt` enters the grace
  spin, and the consumer finishes during the grace window. Without the
  fix (old behaviour: immediate leak on halt) this would have returned
  `Err(Halted)` and the `SumSink` total would be unobservable.
- `leaked_consumer_skips_close_after_abandonment`: regression for the
  "leaked consumer finalises an abandoned output" bug. When the grace
  period expires and the consumer is leaked, the consumer — once its
  wedged `apply` syscall returns — must observe the abandonment flag and
  exit WITHOUT calling `close()`. For the mux writer, `close()` is where
  the MKV is finalised (Cues + segment-header patch); running it on a
  file the caller already reported as failed is the write race this fix
  prevents. Uses a sink whose `apply` blocks until released (simulating
  a wedged write syscall) and records whether `close()` ran; fires the
  halt, lets `finish_with_halt` spin through the full grace period and
  leak the thread, THEN releases the wedged `apply`. The consumer drains
  to EOF (`tx` already dropped) and must skip `close()`. Without the
  fix, the leaked consumer would fall through to `sink.close()`.
- `consumer_finishing_in_grace_still_calls_close`: companion to the
  above — the abandonment guard must NOT fire on the normal halt path
  where the consumer finishes inside the grace window; there, `close()`
  runs and the output is finalised as usual. (Covered for the
  value-return case by `finish_with_halt_joins_cleanly_when_consumer_finishes_in_grace`;
  this one asserts `close()` specifically ran.)
- `finish_with_halt_no_halt_token_normal_completion`: regression that
  `finish_with_halt` with no halt token and a consumer that completes
  normally must still return `Ok` (the `None`-halt polling path is
  unchanged by the grace-period fix) — the pre-existing happy-path test
  reproduced with an explicit timing floor to guard against spurious
  early returns.
- `send_with_halt_fails_fast_once_apply_has_failed`: a fatal `apply`
  error must become visible to the PRODUCER, not only to `finish()`.
  The consumer keeps draining after the error (so the producer never
  blocks on a dead receiver), which meant every `send_with_halt`
  returned `Ok` for the rest of the run: on a 60 GB `mkv://` mux that
  hit ENOSPC on the first frame, the mux driver read the entire
  remaining title — an hour of optical-drive time — before learning the
  write had died.
- `abandon_loses_to_a_close_already_committed`: the abandon/finalise
  race. A consumer that has ALREADY committed to `close()` when the
  grace period expires cannot be stopped — the finalise is happening —
  so the caller must wait for its result instead of reporting the
  output as un-finalised. With a plain flag the consumer read it as
  clear, the caller then stored it, and the caller returned
  `Err(Halted)` (`completed = false`) while a fully finalised MKV (Cues
  written, Segment size patched) landed on disk — a truncated rip
  indistinguishable from a complete one.

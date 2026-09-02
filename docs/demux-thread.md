# `DemuxThread`

## Why a second worker thread

With `crate::sector::PrefetchedSectorSource` alone, read+decrypt already
runs on a producer thread; the *consumer* (main) thread still serialises
`ts_demuxer.feed` (M2TS parsing) with the codec parsers. Profiling showed
feed at ~37% and codec parse at ~44% of consumer wall time — i.e. feed is
heavy enough that pipelining it with parse pays for itself.

Splitting them: feed runs in `DemuxThread`; the consumer thread receives
`Vec<PesPacket>` batches and runs codec parse + frame emission only. Total
throughput becomes `1/max(feed, parse)` instead of `1/(feed + parse)`.

## Lifecycle

`DemuxThread::spawn_zero_copy` consumes the prefetch channels and the
demuxer state, returning a handle plus a `Receiver<DemuxBatch>`. Dropping
the handle closes the channel which signals the worker to exit; the join
in `Drop::drop` blocks until the worker observes channel closure and
returns (no timeout — a wedged downstream would block the drop until it
releases the channel).

## `spawn_zero_copy` rationale

Instead of taking a `SectorSource` and memcpy-ing through its
`read_sectors` API, this constructor consumes the prefetch channels
directly: filled buffers come in via `prefetch_rx`, the demux thread
feeds them, then returns them to `recycle_tx` for the producer to
re-fill. Eliminates the 16 MiB memcpy per batch that the SectorSource
adapter incurred (and, with the producer-side recycling pool, also
eliminates the per-batch heap alloc / cross-thread free that was costing
40%+ of demux-thread time before).

## Test: empty-batch disconnect fix (`worker_exits_promptly_on_consumer_drop_during_empty_batches`)

Regression: worker must detect consumer disconnect even when every demux
batch is empty (no matching PIDs / null packets).

Before the fix, `tx.send()` was never called for empty batches so the
worker never observed the consumer drop — it would spin through ALL
remaining extents before exiting, causing `DemuxThread::drop`'s `join()`
to block for minutes on a mostly-untracked disc region.

The watchdog: if the worker doesn't exit within 1s of the consumer drop
the test fails (rather than hanging forever as the bug would).

## Test: spawn-failure drop-order (`channels_disconnected_before_producer_join_on_spawn_failure`)

Regression: on thread-spawn failure the channels must be dropped BEFORE
the producer shell so the upstream producer observes disconnection and
exits, allowing `join()` to complete without hanging.

A true EAGAIN/pids-limit spawn failure cannot be reliably forced in a
unit test without root or ulimit co-operation, so the test exercises the
drop-order contract directly: a mock shell that panics if `join()` is
called while either channel end is still open.

The test constructs a `(prefetch_tx, prefetch_rx)` pair where the tx side
is held by a sentinel that stays alive as long as either channel end is
open, then asserts that the sentinel is gone by the time
`producer_shell`'s join logic would run. Because a real spawn failure
can't be forced, the test instead verifies the helper logic in isolation:
drop `prefetch_rx` and `recycle_tx` first, then observe the producer-side
sender is disconnected, which is the property the fix relies on.

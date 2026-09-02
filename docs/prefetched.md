# `sector::prefetched` — design notes

## Why a producer thread

The mux consumer (demux + codec parsing + frame output) is single-threaded
by nature (streams are sequential). The mux producer (read sectors + AACS
decrypt) is also single-threaded per-call but does CPU-heavy work (AES per
6144-byte unit). Running both on the same thread means the disk and decrypt
cores sit idle while the demux runs, and vice versa.

Splitting them across two threads with a bounded channel between lets both
run in parallel — peak throughput becomes `min(producer_rate, consumer_rate)`
instead of `1 / (1/producer + 1/consumer)`.

## Lifecycle

The producer thread is spawned by `PrefetchedSectorSource::new` (which
returns `Err` if the OS refuses the thread spawn). It walks the supplied
extent list in order, reads the configured batch size at each LBA, and
sends the resulting plaintext buffer into a `crossbeam_channel::bounded`
channel of small depth (so the producer stays a couple of batches ahead
without unbounded memory growth).

When the channel sender drops (either because all extents were served or
because the `Halt` token cancelled), the consumer observes `RecvError` on
the next `read_sectors` and treats it as end-of-stream. Errors from the
underlying reader are forwarded verbatim through the channel.

## Read API

`read_sectors` ignores its `lba`/`count` arguments — the producer has
already chosen what to read, in the order the extents dictate. This is
sound for the mux read path, which always walks extents sequentially and
never seeks. For random-access callers (sweep patch retries) this wrapper
is the wrong tool — they should keep reading the underlying source
directly.

## `new` — unit-alignment precondition

Each extent's `sector_count` should be a multiple of `SECTOR_ALIGNMENT`
(3 sectors / one 6144-byte AACS aligned unit). Blu-ray m2ts extents satisfy
this by spec. If an extent has a trailing 1-2 sectors that cannot fill a
complete unit, the producer surfaces `Error::ExtentNotUnitAligned` through
the channel rather than handing the decrypt step a sub-unit chunk it would
silently leave encrypted.

## `into_channels` — zero-copy pipeline mode

The caller (typically `super::super::mux::demux_thread::DemuxThread`) pulls
buffers from `rx`, consumes them, and pushes the empty `Vec<u8>` back
through `recycle_tx` so the producer can re-fill it. The producer-thread
`JoinHandle` stays with the returned `PrefetchedSectorSource` shell; drop
that to join. The shell only holds the join handle and total_sectors for
`capacity_sectors` queries; its `SectorSource` impl becomes invalid after
this call (data has been moved out).

## Test notes

- `drop_undrained_source_joins_cleanly`: regression for dropping a
  `PrefetchedSectorSource` DIRECTLY — the public `new` + documented
  direct-read path, and any error/halt exit before the extents are
  drained — must join the producer cleanly. `Drop::drop` used to `join()`
  while the struct still held `rx` and `recycle_tx` (sibling fields drop
  only AFTER `Drop::drop` returns), so a producer parked in the plain
  blocking `tx.send(Ok(buf))` never saw a disconnect and the dropping
  thread blocked in `join()` forever — a permanent two-thread deadlock
  that cancelling the `Halt` could not escape, because that `send` has no
  timeout and never re-polls the token. 300 sectors at batch=3 is 100
  batches against a forward channel of depth `PREFETCH_CHANNEL_DEPTH` (2),
  so the producer is guaranteed to be blocked in `send` by the time the
  drop runs. Mirrors `byte_prefetcher::drop_endless_prefetcher_joins_cleanly`,
  whose `Drop` already had the correct shape.

- `into_channels_drop_releases_producer`: the CRITICAL regression — after
  `into_channels`, dropping the returned forward receiver + recycle sender
  must let the producer observe disconnection and exit, so dropping the
  `PrefetchShell` (which joins the producer) returns promptly. With the
  old clone+forget the leaked endpoints kept the producer blocked and this
  join hung forever.

- `halt_releases_producer`: same property via the halt path — cancel the
  token, then the producer must exit and the shell join must complete. The
  producer parks in a BLOCKING `tx.send` on the forward channel and only
  checks `halt` at the loop top, so `halt.cancel()` cannot interrupt a send
  that is already blocked on a full channel. To keep the test deterministic
  under load, we drain the forward receiver on a background thread: every
  send then makes progress, the producer reaches the loop top, observes
  the cancelled halt, and exits — so the shell join completes promptly
  regardless of scheduling. (Channel-disconnection shutdown is covered
  separately by `into_channels_drop_releases_producer`.)

- `zero_unit_align_rejected`: `unit_align == 0` must be rejected by the
  constructor, not turned into a divide-by-zero panic on the producer
  thread. Before the guard, `new_with_events` returned `Ok` and the
  producer evaluated `remaining % 0`, panicking ("attempt to calculate the
  remainder with a divisor of zero"); `catch_unwind` then reported the
  read as `DemuxThreadPanicked` instead of the `InvalidInput` its sibling
  parameter gets — a panic printed out of a public constructor's own
  thread.

- `direct_reads_past_pool_depth_do_not_deadlock`: more than 3 sequential
  direct `read_sectors` calls must succeed. The recycle pool seeds
  PREFETCH_CHANNEL_DEPTH+1 (3) buffers; before the fix the direct path
  dropped each drained buffer, so the 4th call deadlocked.

- `event_fn_fires_bytes_read_per_batch`: the producer-thread `event_fn`
  must fire a `BytesRead` event for every batch it reads, with a
  monotonically increasing cumulative byte count that reaches the full
  extent size at EOF. This is the contract autorip's mux progress bar +
  soft-stall watchdog depend on: the resume/multipass paths pass an
  `event_fn` so `latest_bytes_read` tracks read-ahead progress and
  `wd_last_frame` is refreshed on each sector read (not only on the slower
  write cadence). Before this guard nothing asserted the callback fired at
  all, so a path that passed `None` (the `resume_remux` regression)
  compiled and ran silently with the bar stuck at write-lagged progress.

- `non_multiple_of_three_extent_errors_on_tail`: an extent whose
  sector_count is not a multiple of 3 must not emit a still-encrypted
  sub-unit tail. The producer delivers the readable full units, then
  surfaces a typed error on the tail instead of a short batch.

- `short_read_does_not_desync_stream`: a short read (inner source returns
  fewer sectors than requested) must advance the extent cursor by the
  sectors actually read, not the requested count — otherwise the bytes
  between the short read and the request size are silently skipped.

- `capacity_sectors_sums_all_extents`: `capacity_sectors` returns the sum
  of all extents' sector_counts, computed once at construction. Grounding:
  doc comment on `total_sectors` — "the sum of each extent's sector_count".

- `capacity_sectors_clamps_on_overflow`: total-sector accumulation must
  clamp at u32::MAX rather than panic (debug overflow) or wrap (release)
  on a hostile extent set whose summed sector_count exceeds u32.
  Grounding: the `new` comment — "Accumulate in u64 then clamp ... a naive
  u32 sum() could panic in debug / wrap in release".

- `producer_walks_extents_in_order_at_correct_lbas`: the producer must
  walk extents in list order and start each extent at its `start_lba`
  (plus running offset within the extent), never reorder or merge them.
  Grounding: lifecycle doc — "walks the supplied extent list in order" and
  `lba = extent.start_lba.saturating_add(offset)`.

- `batch_trimmed_to_whole_units`: a batch larger than one unit must be
  trimmed DOWN to a whole number of 3-sector units before issuing the
  read — never a sub-unit count that decrypt would leave partially
  encrypted. batch=5 → trimmed to 3 (5 - 5%3). Grounding: the unit-trim
  block `sectors -= sectors % SECTOR_ALIGNMENT`.

- `unit_aligned_extent_delivers_all_and_eofs`: an extent whose
  sector_count IS a multiple of 3 must deliver exactly that many sectors
  and then cleanly EOF (no error on the final aligned batch). Grounding:
  the trailing-tail guard only fires for sub-unit leftovers; a unit-aligned
  extent forms full units on its own.

- `reader_error_propagates_with_kind`: the underlying reader's error must
  propagate to the consumer as an error (not Ok(0)/EOF), and its
  ErrorKind must survive the round-trip through the channel. Grounding:
  the producer's `Err(e) => tx.send(Err(e.into()))` arm, and
  `read_sectors`' `Ok(Err(e)) => { self.producer_failed = true;
  Err(Error::from(e)) }`. That arm recovers the producer's TYPED error by
  downcast rather than blanket-wrapping it as `Error::IoError`, so the
  kind survives; the `producer_failed` latch it also sets is what turns
  the channel close that follows into `SourceTerminated` instead of a
  clean EOF.

- `inner_source_quitting_early_is_not_reported_as_end_of_stream`: an inner
  source that answers a mid-extent read with `Ok(0)` has quit early: the
  extent list still has sectors to serve. The producer must say so, not
  simply drop `tx` — a closed channel reads as clean end-of-stream, and
  `DiscStream::fill_extents` then fabricates zeros for every remaining
  sector of the title and reports the pass complete. Same rule as the
  panic sentinel: a truncated title must never be finalized as success.

- `non_sector_multiple_read_rejected`: a read returning a byte count that
  is not a whole number of sectors (n % 2048 != 0) must be rejected —
  never truncated and advanced, which would split a sector and hand
  decrypt a partial unit. Grounding: the `if n % 2048 != 0 { send Err }`
  guard.

- `direct_read_too_small_buffer_errors`: a too-small consumer buffer in
  the direct `read_sectors` path must error (InvalidInput), never
  silently drop the bytes past `buf.len()`. Grounding: the
  `if filled.len() > buf.len()` guard in `read_sectors` ("would silently
  drop filled[buf.len()..], desyncing the stream").

- `too_small_buffer_repeated_does_not_deadlock_pool`: regression for the
  fix-1 deadlock — calling read_sectors with a too-small buffer more times
  than the pool depth (PREFETCH_CHANNEL_DEPTH+1 = 3) must NOT deadlock and
  the pool must remain usable afterwards. Before the fix, each
  too-small-buffer error path returned without recycling the received
  buffer, draining the fixed pool. On the 4th call the producer blocked on
  recycle_rx.recv() while the consumer blocked on rx.recv() — permanent
  deadlock.

- `delivered_bytes_match_source_exactly`: the producer delivers exactly
  the bytes the inner source produced, in order, byte-for-byte.
  PatternSource tags each sector with `(lba & 0xff)`, so the assembled
  stream must match a reconstruction from the extent's LBA range. Guards
  against off-by-one/duplicate/reorder in the offset bookkeeping.

- `empty_extents_eof_immediately`: an empty extent list must EOF
  immediately (capacity 0, first direct read returns Ok(0)) and must not
  deadlock. Grounding: the producer's `while ext_idx < extents.len()` loop
  body never runs, so `tx` drops and the consumer sees RecvError → Ok(0).

- `zero_length_extent_is_skipped`: a zero-length extent in the middle of
  the list must be skipped (remaining == 0 → advance to next extent)
  without emitting a batch and without stalling. Grounding: the
  `if remaining == 0 { ext_idx += 1; continue }` branch.

- `four_sector_extent_errors_on_one_sector_tail`: a 4-sector extent (one
  full unit + a 1-sector tail) must deliver the 3-sector unit and then
  error on the 1-sector remainder — exercising the trim-within-batch path
  (`sectors -= sectors % 3` lands on 3, leaving remaining=1) that then
  hits the sub-unit guard on the next iteration. Distinct control flow
  from the 8-sector case. Grounding: trailing-tail guard plus the
  unit-trim block.

- `many_extents_drain_without_deadlock`: many sequential direct reads
  across MANY extents must all flow through the fixed recycle pool
  without deadlock — a stronger version of the pool-depth regression that
  also crosses extent boundaries (offset reset to 0, ext_idx advance).

- `bad_sector_across_channel_is_not_a_transport_failure`: REGRESSION — an
  ordinary MEDIUM ERROR bad sector that crosses the prefetch channel must
  NOT be classified as a SCSI transport failure. `read_sectors` used to
  re-wrap EVERY channel error as `Error::IoError { source }`, and
  `is_scsi_transport_failure` matches `IoError` (the wedged-USB-bridge
  arm). So a skippable bad sector arrived at `DiscStream::fill_extents`
  looking like a dead bus and aborted the whole pass instead of honouring
  `skip_errors`.

- `transport_failure_across_channel_still_classifies`: OPPOSITE-DIRECTION
  CONTROL — a genuine transport failure (status 0xFF, wedged USB bridge)
  crossing the same channel MUST still classify as a transport failure, so
  `fill_extents` / sweep keep aborting the pass instead of zero-filling
  every read against a dead bus.

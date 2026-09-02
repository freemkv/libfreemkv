# `byte_prefetcher` — design notes and test rationale

`BytePrefetcher` (`src/io/byte_prefetcher.rs`) is the `std::io::Read`
analogue of `crate::sector::PrefetchedSectorSource`. It spawns a
producer thread that fills a bounded pool of `Vec<u8>` chunks from the
underlying reader and ships them through a channel; the consumer pulls
filled chunks, uses them, and sends the empty `Vec<u8>` back through a
recycle channel so the producer can re-fill in place. Result: zero
allocations and zero cross-thread frees in the steady-state hot loop.

This is the byte-stream half of the freemkv mux highway —
`BytePrefetcher` feeds `crate::mux::demux_thread::DemuxThread` for
`m2ts://` (the only in-tree caller today, via `crate::mux::resolve`),
and works for any stream whose source is an `io::Read` rather than a
`SectorSource`.

## `PrefetchShell`

Drop blocks the calling thread until the producer exits. To guarantee
a prompt exit, drop the forward receiver and the recycle sender first
so the producer observes channel disconnection (or cancel the `Halt`
passed to `BytePrefetcher::new`, which the producer polls at
`POLL_INTERVAL` granularity even while parked on a channel op).

## Test coverage rationale

- **`recycle_depth_is_forward_depth_plus_one`** — `RECYCLE_DEPTH` must
  be one MORE than `FORWARD_DEPTH` per its own doc comment: the
  producer needs at least one buffer to fill while the consumer holds
  the other `FORWARD_DEPTH`-worth in flight. A `+` -> `*`/`-` mutation
  on `FORWARD_DEPTH + 1` would under-size the recycle channel (e.g.
  `FORWARD_DEPTH * 1 == FORWARD_DEPTH`, one short), which starves the
  producer of a spare buffer.

- **`into_channels_drop_releases_producer`** — the CRITICAL
  regression: after `into_channels`, dropping the returned forward
  receiver + recycle sender must let the producer observe
  disconnection and exit, so dropping the `PrefetchShell` (which
  joins the producer) returns promptly. With the old clone+forget the
  leaked endpoints kept the producer blocked and this join hung
  forever.

- **`delivers_all_bytes_in_order_across_chunks`** — CORE CONTRACT: the
  prefetcher must deliver every source byte, in order, exactly once —
  never silently truncate or duplicate. Source is 5000 bytes; chunk
  size 1024 forces multiple chunks (4 full + 1 short of 904). The
  reassembled stream must equal the source. Mutation: replacing
  `buf.truncate(n)` with a no-op would over-report bytes on the final
  short read and this fails.

- **`short_read_truncates_to_actual_length`** — a reader that returns
  fewer bytes than requested per call must NOT leave stale tail bytes
  in the delivered chunk. Cursor over 10 bytes with a 4096 chunk
  yields a single 10-byte chunk; the consumer must see exactly 10
  bytes, not 4096. Grounds `buf.truncate(n)`. Mutation: delete the
  truncate and the chunk would carry 4086 zero bytes of padding,
  failing the length assert.

- **`empty_source_yields_clean_eof_no_batches`** — EOF semantics: an
  empty source (Cursor over `[]`) yields `read() == Ok(0)` on the
  first call, which the producer treats as EOF and returns, dropping
  tx. The consumer sees RecvError (zero batches), NOT an Err batch and
  NOT a zero-length Ok batch. Grounds the `Ok(0) => return` arm.
  Mutation: changing `Ok(0) => return` to `Ok(0) => continue` would
  spin forever (test times out).

- **`read_error_is_propagated_as_err_batch`** — a reader that fails
  mid-stream must surface the `io::Error` as an `Err` batch on the
  forward channel, not swallow it. We deliver one good chunk then an
  error. The consumer must see the good bytes followed by the error.
  Mutation: changing `let _ = tx.send(Err(e)); return;` to a plain
  `return` would drop the error silently and this fails.

- **`read_panic_surfaces_as_err_batch_not_clean_eof`** — PANIC
  propagation: a reader that PANICS mid-stream must NOT be read as a
  clean EOF at the demux boundary. The producer's `catch_unwind` sends
  an explicit `Err` sentinel before the thread unwinds, so the
  consumer sees the good bytes followed by an error batch — never a
  silent truncation. Without the `catch_unwind` the panic would just
  drop `tx`, the consumer would see RecvError (== clean EOF) and the
  partial output would be finalized as if complete.

- **`recycled_buffer_carries_no_stale_tail`** — recycle-buffer reuse
  must NOT leak stale bytes between chunks of different lengths. After
  a full chunk, a short read reuses the same recycled buffer; the
  regrow-then-truncate sequence must yield only fresh bytes. Verified
  by reassembling the full stream. Source: 8 bytes of 0xAA + 3 bytes
  of 0xBB, with chunk_bytes=8 → chunk0 = 8×0xAA, chunk1 = 3×0xBB.

- **`exact_multiple_length_no_trailing_empty_batch`** — exact-multiple
  boundary: when the source length is an exact multiple of
  chunk_bytes, the final non-empty chunk is followed by an `Ok(0)` EOF
  read, NOT a spurious empty Ok batch. 12 bytes with chunk_bytes=4 →
  three 4-byte chunks then clean EOF. Total bytes must equal 12 and no
  zero-length batch may appear.

- **`drop_finite_prefetcher_joins_cleanly`** — dropping the
  `BytePrefetcher` directly (without `into_channels`) must join the
  producer cleanly when the source is finite. The producer reaches
  EOF, drops tx, and exits; Drop's join returns. Grounds the
  `BytePrefetcher` `Drop` impl. Mutation: removing the `Ok(0) =>
  return` EOF exit would hang this join.

- **`drop_endless_prefetcher_joins_cleanly`** — regression: dropping a
  `BytePrefetcher` directly (without `into_channels`) with an ENDLESS
  source must not deadlock. Before the fix, `Drop` joined the producer
  while `rx`/`recycle_tx` were still alive (sibling field drop order),
  so the producer filled the depth-2 forward channel and then spun in
  `send_timeout` forever (rx never drained, halt=None). The fix drops
  `rx`+`recycle_tx` BEFORE the join so the producer sees
  `SendTimeoutError::Disconnected` and exits.

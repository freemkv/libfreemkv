# `file_sector_source::linux` — read-side platform hints

## Why both `hint_sequential` and `drop_window`

`POSIX_FADV_SEQUENTIAL` at open widens the kernel's readahead window so
each pread aggregates into fewer NFS round-trips. `DONTNEED` on the
consumed window (called periodically by the caller) drops the
already-read pages from the page cache so an 85 GB streaming ISO read
doesn't fill memory and starve concurrent writes (the MKV output
during mux). Together they mirror the write-side WritebackPipeline's
policy.

## History

Pre-Phase-1 (0.20.7 baseline) had both. Phase 1's introduction of
`FileSectorSource` silently dropped the read-side DONTNEED, and
0.21.2's revert of `SEQUENTIAL` (mistakenly attributing a regression
to it) removed the hint. Net effect: 85 GB of ISO reads pinned in the
page cache + no readahead widening → mux throughput collapse from
18 MB/s historical to 2.7-8 MB/s on 0.21.x. Restored in 0.21.6.

## `prefetch`

Async-prefetch `len` bytes at `offset` into the page cache. The kernel
`readahead(2)` syscall queues the I/O and returns immediately — it
does NOT wait for completion. Called right after each consumed read
so the next batch's I/O overlaps with the caller's processing of the
current batch (decrypt + demux + mux).

Without this hint, with a synchronous demux consumer running at
~50 MB/s and a single-spindle disk capable of ~150 MB/s, the disk sits
idle ~70% of each iteration because kernel readahead alone (capped at
`/sys/block/<dev>/queue/read_ahead_kb`, default 128 KB) can only
pre-stage a tiny slice of the next batch. An explicit `readahead()` of
the same size as the current batch tells the kernel to queue the full
next-batch read now.

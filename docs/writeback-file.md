# `WritebackFile`

## Why it exists

Large sequential writes (sweep, patch, mux on UHD-scale output) left to the
kernel's default writeback policy accumulate hundreds of megabytes of dirty
pages and then burst-flush, stalling subsequent writes for seconds at a
time. `WritebackFile` drives a continuous
[`super::writeback::WritebackPipeline`] that on Linux issues incremental
`sync_file_range` + `posix_fadvise(DONTNEED)` calls at 32 MB granularity so
dirty pages drain at the same rate they're produced. macOS and Windows fall
through to a no-op pipeline — their default cache policies have not been
shown to exhibit the same pathology for this access pattern.

The wrapper also tracks the current file position to feed the pipeline with
progress + seek boundaries. See `super::writeback::linux` for the underlying
pathology and the strategy.

## Platform split

The platform-specific pieces of this wrapper — extent preallocation (Linux
`fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`, Windows no-op today) and the
durable-flush primitive (Linux/macOS `fsync`/`F_FULLFSYNC` wrapped in a
bounded syscall; Windows plain `FlushFileBuffers`, unbounded) — live in
per-OS sibling modules. The dispatch happens once at the bottom of `mod.rs`
via cfg-gated `mod` decls. No inline `#[cfg(target_os = "...")]` in the
business-logic above.

## Write path

Writes are direct passthrough to the underlying `File` (no writer thread, no
ring, no batching). Empirically a writer-thread architecture introduced a
~60% mux throughput regression on NFS bidirectional workloads; the
direct-passthrough write path is faster. The writeback pipeline still runs
(it's called inline from `write` / `write_all` / `seek`) so the
bounded-cache invariant on Linux is preserved.

## Halt-safety

`sync_all` runs the per-OS durable-flush primitive. On Linux/macOS it is
wrapped in [`crate::io::bounded::bounded_syscall`] with a 60 s deadline, so
a wedged NFS server cannot trap the muxer indefinitely on the final fsync.
Windows is a known deviation: its `durable_sync` calls `File::sync_all`
(`FlushFileBuffers`) directly and is NOT bounded — a wedged UNC/SMB share
can block the final flush there.

`sync_all` drains in-flight writeback then issues a full fsync; use it in
place of `File::sync_all`. A bounded-fsync failure is returned as an `Err`
on BOTH platforms, so `Ok(())` means the flush completed and a caller
needing crash-consistency can treat it as a durability barrier. The three
causes are distinguishable by numeric code, because a caller should not
retry a lost worker the way it retries a timeout, and must not report a
user cancel as a failure:

* [`E_SYNC_TIMEOUT`](crate::error::E_SYNC_TIMEOUT) — deadline expired
* [`E_HALTED`](crate::error::E_HALTED) — cancelled;
  [`is_halt`](crate::error::is_halt) recognises it
* [`E_SYNC_WORKER_LOST`](crate::error::E_SYNC_WORKER_LOST) — the worker
  thread died before reporting

## `create_with_size_hint`

Pre-reserves `size_bytes` of disk space via the platform's
extent-preallocation primitive (Linux `fallocate(KEEP_SIZE)`, macOS
`F_PREALLOCATE`; Windows has no equivalent, so it is a debug-logged no-op
there). The reported file size is unchanged (writes still grow the file
naturally) — only the on-disk extent allocation is preallocated, which
reduces extent fragmentation on large sequential writes (mux output,
especially on slow storage / NFS). On platforms without an
extent-preallocation primitive this is equivalent to `create` — the size
hint is dropped after a debug log.

## Test coverage notes

The `tests` submodule pins several behaviors with mutation-testing intent:

* `write_returns_byte_count_and_advances_pos` — `write` must advance `pos`
  by exactly the count the inner `File` reported (not `buf.len()`), guarding
  against desync on a hypothetical partial write.
* `seek_to_current_position_is_noop_for_data` — sweep does a redundant
  `seek(Current(pos))` before every write; the `p != self.pos` guard in
  `Seek::seek` must keep that from being treated as a boundary.
* `open_preserves_existing_contents` — `open` uses `OpenOptions::write(true)`
  with no truncate flag, distinct from `create`'s `File::create` (truncating)
  path.
* `new_tracks_initial_position` — `WritebackFile::new` queries
  `stream_position()` rather than hardcoding `pos = 0`, so a non-zero
  starting offset (resumed/appended files) stays in sync.
* `seek_past_eof_creates_zero_hole` / `seek_from_end_resolves_against_length`
  — `seek` forwards the `SeekFrom` variant as-is to the inner `File`,
  preserving standard POSIX sparse-hole and end-relative semantics.
* `create_with_size_hint_does_not_inflate_logical_length` — the
  preallocation hint reserves extents only; it must not grow the file's
  *logical* length.
* `double_sync_all_is_idempotent` — `finalize` must tolerate being called
  twice (explicit `sync_all` followed by `Drop`) without corrupting data.
* `writeback_chunk_constants_and_conversion` /
  `writeback_chunk_env_override_branches` — pin the
  `FREEMKV_WRITEBACK_CHUNK_MIB` parse/validate/convert logic in
  `writeback_chunk_bytes`: in-range values convert MiB→bytes, and
  zero/unparseable/over-max values all fall back to
  `WRITEBACK_CHUNK_BYTES_DEFAULT`, with the max boundary itself accepted
  (inclusive) and never overflowing `u64`.

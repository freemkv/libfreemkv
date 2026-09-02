# WritebackFile macOS platform impl

## `preallocate`

`fcntl(F_PREALLOCATE)` — macOS's fallocate-equiv. First attempt requests
`F_ALLOCATECONTIG | F_ALLOCATEALL` (prefer a contiguous run but accept
scattered extents to satisfy the full length), falling back to
`F_ALLOCATEALL` alone on failure. `F_PREALLOCATE` never advances EOF
regardless of the flags — only `ftruncate`/writes grow the file — so the
reported file size is unchanged; `F_ALLOCATEALL` governs the contiguity
fallback, not size.

## `durable_sync`

`fcntl(F_FULLFSYNC)` wrapped in [`crate::io::bounded::bounded_syscall`]
with a 60 s deadline. F_FULLFSYNC is HFS+/APFS's true-fsync (flushes the
disk's own write cache) — what `fsync` should have been on macOS. Falls
back to plain `fsync` if F_FULLFSYNC returns ENOTSUP.

### fd-reuse safety

The F_FULLFSYNC / fsync runs on a bounded worker thread that may be
leaked on timeout. To avoid the leaked worker's syscall hitting a
recycled fd number after the original `File` is closed, we `try_clone`
an owned `File` and move it into the closure. The clone keeps the
underlying file description alive for as long as the worker thread
lives. On `try_clone` failure (rare) we fall back to the raw fd integer
— no worse than the previous behaviour.

## `bounded_failure_to_result`

Maps a [`crate::io::bounded::BoundedError`] from the bounded
`F_FULLFSYNC` onto the `io::Error` `durable_sync` returns.

Every arm here means the same thing: **no sync observably ran**. All
three previously returned `Ok(())`, so `WritebackFile::sync_all`
reported success for a durability barrier that never happened — a total
failure exiting 0, with only a log line to distinguish it. POSIX gives
`fsync` exactly one way to say "the data is on stable storage" and that
is a zero return; a call that never reached the device has not earned
it.

The errors carry no message text (this crate ships no user-facing
English): the kind, and `EIO` for the worker-lost case, are the whole
signal, and the `tracing` lines above/below carry the operator detail.

## Test notes

### `durable_sync_worker_uses_owned_clone_with_distinct_fd`

Regression for the fd-reuse / use-after-close fix in `durable_sync`.
Verifies the structural invariant: `try_clone` succeeds for a normal
local tempfile, and the cloned `File` has a distinct fd number from the
original. This pins the property that a leaked F_FULLFSYNC/fsync worker
thread captures an owned `File` (keeping the file description alive)
rather than a bare fd integer that can be reused after the original
`File` closes. The actual fd-reuse race is non-deterministic; a
structural test is the accepted substitute.

### `every_bounded_failure_is_reported_as_an_error`

Every `BoundedError` arm of the bounded `F_FULLFSYNC` means no sync
observably ran. All three returned `Ok(())`, so `sync_all` reported a
durability barrier that never happened — the caller could not tell a
completed flush from a skipped one by any means except reading a log.
Asserted on the concrete `ErrorKind` / `errno` each arm must produce, so
a future arm that quietly reverts to `Ok(())` fails here.

### `bounded_failures_are_never_mapped_to_ok`

The failure path must be reachable through the public surface: a
`WritebackFile::sync_all` that hits any of these arms must surface an
`Err`, not a silent `Ok`. Pinned at the mapping boundary because the
timeout itself is not deterministically inducible in a unit test.

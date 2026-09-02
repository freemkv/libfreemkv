# `io::writeback_file::linux` — durable-sync rationale

## `durable_sync`

Runs `fsync` on the file with a 60 s deadline. On timeout, halt, or a lost
worker we log and return `Err` — matching macOS. POSIX gives `fsync` exactly
one way to say "the data is on stable storage" and that is a zero return; a
call that never reached the device has not earned it, so `Ok(())` from here
means the flush completed and nothing else.

The kernel will still flush on close, so the data is usually durable
anyway — but that is a probability, not a barrier, and a caller that needs
crash-consistency has to be able to tell the difference.

### fd-reuse safety

The `fsync` runs on a bounded worker thread that may be leaked on timeout.
To avoid the leaked worker's syscall hitting a recycled fd number after the
original `File` is closed, we `try_clone` an owned `File` and move it into
the closure. The clone keeps the underlying file description alive for as
long as the worker thread lives. On `try_clone` failure (rare) we fall back
to the raw fd integer — no worse than the previous behaviour.

## `durable_sync_worker_uses_owned_clone_with_distinct_fd` (test)

Regression for the fd-reuse / use-after-close fix in `durable_sync`.

Verifies the structural invariant: `try_clone` succeeds for a normal local
tempfile, and the cloned `File` has a distinct fd number from the original.
This pins the property that a leaked fsync worker thread captures an owned
`File` (and thus keeps the file description alive) rather than a bare fd
integer that can be reused after the original `File` closes.

The actual fd-reuse race is non-deterministic and not cleanly testable
without coordinating a simultaneous close + re-open on another thread. A
structural test is the accepted substitute.

## `bounded_failure_to_result`

Maps a [`crate::io::bounded::BoundedError`] from the bounded `fsync` onto
the `io::Error` `durable_sync` returns.

Every arm means the same thing: no sync observably ran. All three used to
return `Ok(())`, so `WritebackFile::sync_all` reported success for a
durability barrier that never happened. POSIX gives `fsync` one way to say
"the data is on stable storage" — a zero return — and a call that never
reached the device has not earned it.

This mirrors the macOS `F_FULLFSYNC` mapping exactly. The two were found
carrying the identical defect, and a platform disagreeing with its sibling
about whether a failed sync is an error is the "works on my platform" class
this crate has been bitten by before — most recently an over-length SCSI
CDB that macOS rejected and the other two silently truncated.

No message text (this crate ships no user-facing English): the kind, and
`EIO` for the worker-lost case, are the signal; `tracing` carries the
detail.

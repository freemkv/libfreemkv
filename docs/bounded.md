# `src/io/bounded.rs` — bounded-syscall primitive

## Why this exists

[`crate::halt::Halt`] is cooperative: callers poll `is_cancelled()`. It
cannot reach inside a syscall the kernel currently owns the thread for —
`libc::sync_file_range`, `libc::fsync`, `File::write` on NFS, and so on.
`/api/stop` from autorip therefore can't unstick a thread sitting in
such a syscall.

`bounded_syscall` is the escape hatch: it runs `op` on a fresh worker
thread, then `recv_timeout`s on a rendezvous channel for the result.
The wait is broken into ~250 ms slices so the calling thread can poll
the supplied `Halt` in between. If the deadline elapses or the halt
fires, the worker is intentionally leaked — the syscall will unwind
whenever the kernel decides, or at process exit, but the caller is
free to fall back to a degraded code path (skip the sync, log loudly,
etc.).

## Trade-offs

- **Thread per call.** Cheap (`std::thread::spawn` is < 100 µs on
  Linux/macOS), but not free. Use on coarse-grained finalisation
  syscalls (`sync_all`, `sync_file_range(WAIT_AFTER)`), not on hot
  inner-loop writes.
- **Leak on timeout.** A wedged syscall keeps a kernel slot and a
  user-space thread around for the rest of the process's life. Bounded
  by the number of independent rip/mux sessions, which is one per
  disc. The alternative — trapping the caller forever — defeats the
  entire purpose of `/api/stop`.
- **Halt granularity ~250 ms.** Halt observation is not instant; it's
  the worst-case latency of the `recv_timeout` slice. Good enough for
  human-driven stop requests; not suitable for hard real-time
  deadlines.

## Single source of truth

Do NOT inline this pattern. Every blocking-syscall wrapper in the rip
+ mux pipeline calls this helper, so changes (e.g. swapping the
channel impl, adjusting the poll slice, adding metrics) land in one
place.

## Platform

Pure `std::thread` + `std::sync::mpsc`. No `cfg(target_os)` needed
here — the helper itself is platform-agnostic. Callers that wrap
Linux-only syscalls (`sync_file_range`) still need their own
`#[cfg(target_os = "linux")]` gates; this helper does not.

## `bounded_syscall` contract details

- `halt` is polled at `POLL_INTERVAL` granularity. Pass `None` for
  callers that don't (yet) have a halt token plumbed through —
  behaviour degrades to deadline-only, matching the 0.20.5
  `wait_after_with_timeout` shape this helper generalises.
- `op` returns `R: Send + 'static`. The closure must own everything it
  touches because it may outlive this call (timeout / halt cases).
- On `Halted` / `Timeout` the worker thread is intentionally leaked:
  the syscall will unwind whenever the kernel decides, or when the
  process exits. The calling thread is never trapped inside a kernel
  call.

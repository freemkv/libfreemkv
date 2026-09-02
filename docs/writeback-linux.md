# `io::writeback::linux` — rationale

## Pathology this fixes

The kernel's default `vm.dirty_ratio` (~20 % of RAM) lets dirty pages
accumulate to hundreds of MB during a big sequential write, then bursts a
flush at 99 % disk utilisation. While the burst runs, app writes block on
the writeback queue — observed empirically as instantaneous speed dropping
from ~15 MB/s to ~1 MB/s every ~30 s during a Pass 1 sweep.

## Strategy

Every `chunk_bytes` of new sequential output, kick async writeback
(`SYNC_FILE_RANGE_WRITE`) on the just-completed chunk and finalise the
*previous* chunk via `WAIT_AFTER` + `posix_fadvise(DONTNEED)`. By the time
we finalise, that previous chunk has had a full chunk's worth of work to
flush — the wait is near-instant. Dirty cache stays bounded at ~2 ×
`chunk_bytes` and writes drain continuously instead of in bursts.

The chunk size is adaptive: we measure the elapsed time of the
`WAIT_AFTER` call over a rolling window of the last 16 chunks and resize
the chunk based on the p95. Slow storage (NFS, network shares, HDD) sees
larger chunks to amortise per-chunk overhead; fast storage (NVMe) sees
smaller chunks to keep cache pressure tight. Bounds: [4 MiB, 256 MiB].

## NFS escape hatch

`sync_file_range(WAIT_AFTER)` on an NFS-mounted file can block
indefinitely waiting for the server's commit ack. If the server never
acks (network partition, server-side hang, slow commit), the syscall
never returns and the consumer thread is stuck inside the kernel —
`/api/stop` can't reach it because halt is cooperative.

When `fstatfs` reports the file lives on an NFS mount
(`f_type == NFS_SUPER_MAGIC`), the pipeline skips the WAIT_AFTER +
`posix_fadvise(DONTNEED)` dance entirely. NFS clients have their own
buffering and commit semantics that handle dirty-page bounds without us
forcing the issue. The async `SYNC_FILE_RANGE_WRITE` kickoff still runs
(non-blocking by spec) so writeback still gets a nudge.

## Defence in depth: WAIT_AFTER timeout

Even on local storage, a degraded disk or odd filesystem driver could in
principle wedge inside WAIT_AFTER. Each WAIT_AFTER call runs on a worker
thread with a 30s recv_timeout on its result channel. On timeout we log a
loud error, set a `degraded` flag, and from then on skip WAIT_AFTER +
DONTNEED for the rest of the pipeline's life (same shape as the NFS
path). The worker thread is intentionally leaked — it unwinds whenever
the syscall eventually returns or the process exits. The mux continues;
the original dirty-burst pathology re-emerges but the rip can still
finish instead of freezing.

## `wait_after_with_timeout` fd lifetime / fd-reuse safety

`worker_file` is an *owned* `File` (produced by `File::try_clone` at
pipeline construction). It is moved into the worker closure so the file
description stays alive for exactly as long as the worker thread lives —
even if the original `WritebackFile` is closed and the OS reuses its fd
number before the worker's syscall returns.

`fallback_fd` is used only when `worker_file` is `None` (i.e. the
`try_clone` at construction failed). In that case the worker captures the
raw fd integer, which carries the original fd-reuse risk but is no worse
than the pre-fix behaviour.

This delegates to `crate::io::bounded::bounded_syscall`, the generic
worker-thread + `recv_timeout` primitive, and just adapts it to the
WAIT_AFTER call shape: it returns `elapsed_ms` instead of the syscall's
`()`, and treats `WorkerLost` as a benign no-op to match the original
semantics.

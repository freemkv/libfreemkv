# `FileSectorSource` design rationale

`FileSectorSource` reads 2048-byte sectors from an ISO file on disk via
direct `seek + read_exact` (`pread`-equivalent) calls, letting the kernel's
own readahead policy manage prefetch.

## Why no app-level buffer

Pre-0.21.3 this source held a 32 MiB (later 4 MiB) read-ahead buffer to
amortise per-sector NFS round-trips. Empirically that buffer hurt: 32 MiB
refills bursted the NFS TCP connection hard enough to starve the concurrent
writer, and even a 4 MiB window gave the kernel less freedom to pipeline
reads with writes. Direct pread per call lets Linux's readahead widen as it
detects the sequential pattern, and naturally interleaves with writeback.

## DONTNEED on the consumed window

Without page-cache eviction an 85 GB streaming ISO read pins the entire
file in memory, starves the concurrent writer, and collapses mux throughput
(observed: 2.7 MB/s mux on 0.21.5 vs. 70 MB/s isolated NFS reads). Every
`READ_DROP_CHUNK_BYTES_DEFAULT` of consumed bytes we call
`posix_fadvise(DONTNEED)` over that window, mirroring the write-side
`crate::io::writeback::WritebackPipeline` policy.

The drop window is accounted by a monotonic forward byte counter, which
matches the sequential streaming pattern the mux highway drives. Under
random or backward access the dropped range no longer lines up with the
bytes actually read — but `DONTNEED` is purely an advisory cache hint with
no correctness impact, so this degrades to a slightly imprecise hint rather
than a bug.

`READ_DROP_CHUNK_BYTES_DEFAULT` (32 MiB) is the empirically tuned value on
a 7200rpm HDD via SATA: smaller windows (8 / 16 MiB) shorten the
kernel-readahead overlap and slow the producer; larger windows (64 / 128
MiB) let the page cache pin enough of the ISO to pressure concurrent
writes. Override via `FREEMKV_READ_DROP_CHUNK_MIB`.

`READ_DROP_CHUNK_MIB_MAX` (64 GiB) bounds `FREEMKV_READ_DROP_CHUNK_MIB`
before the MiB→byte multiply, mirroring `WRITEBACK_CHUNK_MIB_MAX`, whose
identical multiply is bounded for exactly this reason: without the bound,
a value above 2^44 overflows — a panic on the first ISO open in an
overflow-checked build, and in release a wrap to a near-zero window that
fires `drop_window` on every read. Out-of-range values fall back to the
default.

## Platform open hint

On `open()` each platform issues its "sequential access expected" hint so
OS-level readahead widens. The hint and the DONTNEED call live in per-OS
sibling modules (`linux::hint_sequential` et al.) — no inline `#[cfg]` in
this file.

## Read-ahead prefetch

After every consumed read we issue an OS-level prefetch hint for the next
equivalent-sized window (`platform::prefetch`). The kernel queues that I/O
asynchronously and returns immediately, so the next batch's read overlaps
with the caller's processing of the current batch (decrypt + demux + mux).
Without this the disk sits idle ~70% of each iteration because kernel
SEQUENTIAL readahead alone (capped at `read_ahead_kb`, default 128 KB) is
far smaller than our 16 MiB app-level batch.

## Test notes

- `read_sectors_with_an_undersized_buffer_errors_rather_than_panicking`:
  this is a public `SectorSource` impl, so buffer length is caller input,
  and the guard used to be a `debug_assert!` — compiled out in release,
  where the `out[..bytes]` slice then panicked with 'range end index out
  of range'. `Drive::read_fua` already carries this exact guard with a
  comment recording the same panic being fixed there, and
  `PrefetchedSectorSource` has `direct_read_too_small_buffer_errors` for
  the same case; this impl had neither.
- `dontneed_eviction_does_not_affect_data`: reads past the default 32 MiB
  drop chunk (16384 sectors) so the eviction block fires at least once,
  asserting every sector still reads correctly. Avoids mutating
  `FREEMKV_READ_DROP_CHUNK_MIB` to sidestep a parallel-test env race with
  `drop_chunk_size_env_override`.

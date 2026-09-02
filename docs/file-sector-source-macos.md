# file_sector_source::macos

macOS has no direct `POSIX_FADV_SEQUENTIAL` equivalent; the idiomatic hint is
`fcntl(F_RDADVISE, &radvisory)` describing the byte range you intend to read
soon. The open-time sequential hint points it at the whole file (clamped to
`RDADVISE_MAX_BYTES` so a multi-TB ISO doesn't ask the kernel to prefetch
everything at once).

## RDADVISE_MAX_BYTES

Cap on the byte length passed to `F_RDADVISE`. Asking for a multi-GB
readahead window is counterproductive — the OS doesn't have that much cache
to throw at one fd. 64 MiB is generous for our use case (sweep, mux) so the
kernel's prefetch keeps pace with our app-level pipeline depth.

## drop_window

macOS has no direct `POSIX_FADV_DONTNEED` equivalent for a byte range.
`fcntl(F_NOCACHE)` would disable caching globally on the fd (too coarse — we
want the unread region to still benefit). Best approximation: no-op.
macOS's unified buffer cache is generally less prone to the pin-everything
pathology that triggers the regression on Linux NFS clients.

## prefetch

Async-prefetch the byte range `[offset, offset+len)`. macOS uses the same
`fcntl(F_RDADVISE, &radvisory)` primitive as the open-time sequential hint,
just targeted at a moving window instead of the whole file. The kernel
queues I/O for the requested range and returns immediately.

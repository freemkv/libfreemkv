# `src/io/sink` — output-sink trait split

Two traits, one for each capability axis of an output destination:

- [`SequentialSink`] — anything you can `Write` to in order. Sockets,
  pipes, append-only stores, plain files. Containers that don't need
  seek (M2TS, fMP4, HEVC elementary) target this.
- [`RandomAccessSink`] — everything `SequentialSink` plus a working
  `Seek`. Local files, NFS files, anything with random-write
  semantics. Containers that need backpatch (MKV cluster sizes, Cues
  index, MP4 moov-at-end) target this.

`RandomAccessSink: SequentialSink` — every random-access sink is
also a valid sequential sink. The muxer is generic over which it
requires (`MkvMux<S: RandomAccessSink>`, `M2tsMux<S: SequentialSink>`)
so an attempt to mux MKV to a network socket is a compile error.

Buffering policy belongs to the concrete sink, not to a wrapper at
the call site. `LocalFileSink` wraps a `BufWriter<File>` with a
4 MiB buffer for the common local-disk case; `WritebackFile`
(separate module) wraps a `File` with the adaptive-chunk
`sync_file_range` machinery for the Linux+NFS case.

## `SequentialSink::finish`

`finish` drains any internal buffering and signals end-of-stream to
the underlying transport (close-write on a socket, flush + fsync on
a buffered file, etc.). The default impl flushes via [`Write::flush`]
— correct for an unbuffered destination — but every concrete sink in
this module overrides it to drain its own buffer and run its
transport-specific finalisation (socket `shutdown(Write)`, file
`fsync`). There is deliberately NO blanket `impl SequentialSink for
T`: a blanket impl would force the no-op-style default on every
concrete sink (a blanket impl cannot be overridden per-type without a
coherence conflict), so a `Box<dyn SequentialSink>` / `&mut dyn
SequentialSink` `finish()` call would silently skip the flush and
transport shutdown. With explicit per-type impls the vtable dispatches
`finish` to the real implementation, so flush + durable-finish
actually happen through a trait object.

## `open_for_mkv`

Picks the right `RandomAccessSink` impl for `dest` based on its
filesystem type.

- Linux + NFS path → `WritebackFile` with its adaptive-chunk
  sync_file_range machinery and (when supported) `fallocate` size
  hint.
- everything else → `LocalFileSink` over `BufWriter<File>`. On
  non-Linux there is no `WritebackFile` machinery to opt into, and
  on local Linux the kernel's default writeback policy is already
  fine.

`size_hint`, when present, is forwarded to the per-OS preallocate
path (`fallocate(KEEP_SIZE)` on Linux, `F_PREALLOCATE` on macOS when
implemented, no-op elsewhere).

Returns a boxed trait object so the call site (mux construction)
stays agnostic of which concrete sink got picked.

Not yet wired into `mux::resolve` (follow-up commit). Kept
`pub(crate)` until then so an unfinished signature isn't frozen into
the public 1.0 API.

## Test notes

- `finish_through_trait_object_flushes_local_file` is the regression
  test for the silent-no-op `finish()` bug: `finish()` through a `dyn
  SequentialSink` trait object must dispatch to the concrete sink's
  override (flush + fsync), not a no-op default.
- `FlushTracker` is a minimal `SequentialSink` that does NOT override
  `finish`, so it exercises the trait's default impl, which must call
  `Write::flush`. This pins the documented contract that the default
  `finish` is "correct for an unbuffered destination" by flushing.
  Mutation: changing the default `finish` body from `self.flush()` to
  `Ok(())` would set `flushed=false` and fail.
- `open_for_mkv_without_size_hint_is_random_access` covers
  `open_for_mkv` with `None` size hint: it must still produce a
  working random-access sink (the `match size_hint { None => ... }`
  arm). Round-trips a seek-back patch through it to prove both Write
  and Seek dispatch. Mutation: if the `None` arm returned a
  sequential-only sink the seek would not compile / would fail.

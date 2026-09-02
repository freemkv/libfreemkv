# `LocalFileSink`

`BufWriter<File>`-backed sink for the common local-disk case
(`src/io/sink/local_file.rs`).

## Buffering

4 MiB internal `BufWriter`. Sized to coalesce the small per-PES writes
that come out of the muxer into kernel-page-aligned flushes without
making the buffer big enough to matter for memory pressure on a single
concurrent rip.

## Seek-flush ordering

`Seek` flushes the underlying `BufWriter` first; otherwise a seek could
leapfrog buffered data and silently corrupt the file. This is the same
shape `BufWriter` itself uses when it impls `Seek` in stdlib, and is
necessary for MKV's seek-back operations (cluster size patch, Cues
index, segment header backpatch) to land on the right offset.

## Trait wiring

[`SequentialSink`](../src/io/sink/mod.rs) is implemented explicitly
(not via a blanket impl) so its `finish()` flushes the `BufWriter` and
`fsync`s the file even when called through a `dyn` trait object;
`RandomAccessSink` is implemented over the `Seek` impl.

## Construction contract

Construction always opens the file `create + truncate + read + write`.
`read` is enabled so the same handle can be reused for a verification
re-read after the mux (the existing `FileSectorSink::create` pattern).
On Linux, `with_size_hint` additionally calls
`fallocate(FALLOC_FL_KEEP_SIZE)` to pre-reserve extents.

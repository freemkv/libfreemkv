# `PipelinedPesStream`

The read-side of the freemkv mux highway. Given a `DemuxThread` (which has
the producer + demux workers already spawned), a set of codec parsers, and
the title metadata, this struct implements `crate::pes::Stream` by running
codec parse on the caller's thread and emitting `PesFrame`s one at a time.

## Threading

The pipeline runs three threads in parallel:

```text
Thread A: read + decrypt   (PrefetchedSectorSource / BytePrefetcher)
Thread B: M2TS demux       (DemuxThread)
Thread C: codec parse      (this struct, on the caller's thread)
```

Communication between A→B and B→C is via bounded channels with recycled
buffer pools — no allocations or memcpys in the steady-state hot loop.

This is the *only* read-side `Stream` impl in tree. Both the ISO file mux
and the BD-TS (`m2ts://`) file mux input paths are built by
`crate::mux::resolve` (`build_iso_pipeline` / the m2ts pipeline builder)
and hand back a `PipelinedPesStream`; the differences are in how the
producer thread (A) is configured — sector-aligned reads with AACS
decrypt for ISO, raw byte reads for M2TS. (`crate::mux::M2tsStream`
itself is a write-only sink and does not construct this type.)

## DVD seek-index regression (highway-level test)

The real CLI mux runs `PsDemuxer → PipelinedPesStream (codec parse) →
frame out`, NOT the codec parser straight into the muxer. The keyframe
flag and per-frame duration the `Mpeg2Parser` sets on each `Frame` must
survive that path (`from_codec_frame`) so the muxer's cluster/cue logic
— which opens a cluster + pushes a cue on `keyframe && track 0` —
actually fires. If the highway dropped the keyframe flag, every video
I-frame would arrive as a non-keyframe and the DVD MKV would get
thousands of clusters with ZERO cues (chapter-seek only, no scrub).

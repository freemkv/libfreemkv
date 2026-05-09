# libfreemkv API Design

## Principles

1. Lib provides building blocks. App composes them.
2. No English text in lib. Error codes only. App handles i18n.
3. No display logic in lib. App decides what to show.
4. Streams are the pipeline. Each stage wraps the next.
5. Lib fires events. App listens.

## Core API

```rust
// Open drive — explicit steps, app prints between them
let mut drive = Drive::open(path)?;
drive.wait_ready()?;
drive.init()?;
drive.probe_disc()?;

// Scan disc
let disc = Disc::scan(&mut drive, &ScanOptions::default())?;

// Browse
disc.titles      // Vec<DiscTitle>
disc.format      // BD / UHD / DVD
disc.capacity_gb()
```

## PES Pipeline (primary API)

The PES pipeline is the main way to move content. All streams produce/consume
PES frames. The pipeline just reads frames and writes frames.

```rust
// URL-based — any source to any destination
let opts = InputOptions::default();
let mut input = libfreemkv::input("disc:///dev/sg4", &opts)?;
let title = input.info().clone();
let mut output = libfreemkv::output("mkv://Movie.mkv", &title)?;

while let Ok(Some(frame)) = input.read() {
    output.write(&frame)?;
}
output.finish()?;
```

The `FrameSource` and `FrameSink` traits — direction is type-checked, so
calling `read()` on a write-only sink (or `write()` on a read-only source)
is a compile error rather than a runtime fault:

```rust
pub trait FrameSource: Send {
    fn read(&mut self) -> Result<Option<PesFrame>, Error>;
    fn info(&self) -> &DiscTitle;
    fn codec_private(&self, track: usize) -> Option<Vec<u8>> { None }
    fn headers_ready(&self) -> bool { true }
}

pub trait FrameSink: Send {
    fn write(&mut self, frame: &PesFrame) -> Result<(), Error>;
    fn finish(self: Box<Self>) -> Result<(), Error>;
    fn info(&self) -> &DiscTitle;
}
```

## Streams

All streams implement `FrameSource` (read) and/or `FrameSink` (write); the
directional split prevents runtime "wrong-direction" errors. URL-based
resolvers open any stream by string.

| Stream | Input | Output | URL | Transport |
|--------|-------|--------|-----|-----------|
| DiscStream | Yes | -- | `disc://` `disc:///dev/sg4` | Optical drive via SCSI |
| IsoStream | Yes | Yes | `iso://path.iso` | Blu-ray ISO image |
| MkvStream | Yes | Yes | `mkv://path` | Matroska container |
| M2tsStream | Yes | Yes | `m2ts://path` | BD-TS with FMKV metadata header |
| NetworkStream | Yes (listen) | Yes (connect) | `network://host:port` | TCP with FMKV metadata header |
| StdioStream | Yes (stdin) | Yes (stdout) | `stdio://` | Raw byte pipe |
| NullStream | -- | Yes | `null://` | Discard sink (byte counter) |

All URLs require a `scheme://path` format. Bare paths are rejected.

```rust
// PES pipeline (frame-level) — input() returns Box<dyn FrameSource>,
// output() returns Box<dyn FrameSink>.
let input = libfreemkv::input("disc:///dev/sg4", &opts)?;    // DiscStream
let input = libfreemkv::input("iso://Dune.iso", &opts)?;     // IsoStream
let output = libfreemkv::output("mkv://Dune.mkv", &title)?;  // MkvOutputStream
let output = libfreemkv::output("m2ts://Dune.m2ts", &title)?; // M2tsOutputStream
let output = libfreemkv::output("network://10.1.7.11:9000", &title)?; // NetworkOutputStream
let output = libfreemkv::output("null://", &title)?;          // NullOutputStream
```

### FMKV Metadata Header

M2tsStream and NetworkStream embed a JSON metadata header before the BD-TS data:

```
[8B magic "FMKV\0\0\0\0"][4B JSON length][JSON metadata][padding to 192B boundary][BD-TS data...]
```

The header carries title name, duration, codec_privates, and full stream layout
(PIDs, codecs, languages, labels). This allows the receiving end to set up
demuxing and track metadata without scanning the TS.

## Events

Lib fires events during operations. App provides a callback. No display, no text.

```rust
pub struct Event {
    pub kind: EventKind,
}

pub enum EventKind {
    // Init / scan
    DriveOpened { device: String },
    DriveReady,
    InitComplete { success: bool },
    ProbeComplete { success: bool },
    ScanComplete { titles: usize },

    // Read pipeline
    BytesRead { bytes: u64, total: u64 },
    ReadError { sector: u64, error: Error },
    SpeedChange { speed_kbs: u16 },
    ExtentStart { index: usize, start_sector: u64, sector_count: u64 },
    SectorSkipped { sector: u64 },
    BatchSizeChanged { new_size: u16, reason: BatchSizeReason },
    Complete { bytes: u64, errors: u32 },

    // Kept for forward-compat; not emitted in 0.13.6+
    Retry { attempt: u32 },
    SectorRecovered { sector: u64 },
}
```

Emission notes:

- `BytesRead { bytes, total }` is emitted from `DiscStream::fill_extents`
  after each successful sector read. `bytes` is the cumulative running
  total; `total` is the precomputed extent sum (0 if unknown).
- `SpeedChange` is emitted from the public `Drive::set_speed` API path.
  It is no longer emitted from a recovery hot loop (recovery loop removed
  in 0.13.6).
- `BatchSizeChanged` fires from the `DiscStream` adaptive sizer on shrink
  (read failed at a larger size) and on probe-up (clean-read streak hit
  the threshold). Consumers use it to display a "recovering" state
  distinct from "ripping normally".
- `Retry` and `SectorRecovered` are NOT emitted in 0.13.6+. They were
  tied to the inline `Drive::read` recovery phases that were removed; the
  variants are kept for forward compatibility so consumers' match arms
  don't need conditional compilation.

Events report what happened. App decides what to do. GUI shows a dialog. CLI
prints a line. Server logs to file.

## File Layout

```
libfreemkv/src/
├── lib.rs              Public exports
├── error.rs            Error codes (no English)
├── event.rs            Event types for callbacks
├── halt.rs             Halt cancellation token (Arc<AtomicBool> wrapper)
├── io/                 Pipeline + WritebackFile primitives
│   ├── mod.rs          Re-exports WritebackFile, Pipeline, Sink, Flow
│   ├── pipeline.rs     Generic Pipeline<I, R> + Sink trait
│   ├── writeback_file.rs  WritebackFile (was crate::io::Writer)
│   └── writeback.rs    sync_file_range pipeline
├── drive/              Drive (open, init, single-shot read)
│   ├── mod.rs          Drive struct, init, read (single-shot), reset, eject
│   ├── capture.rs      Drive profile capture for contribution
│   ├── linux.rs        Linux drive discovery
│   ├── macos.rs        macOS drive discovery
│   └── windows.rs      Windows drive discovery
├── disc/               Disc (scan, titles, AACS setup, sweep, patch)
│   ├── mod.rs          Disc struct, scan, titles, formats
│   ├── sweep.rs        Disc::sweep (Pass 1 forward sweep)
│   ├── patch.rs        Disc::patch (Pass N retry over mapfile)
│   ├── mapfile.rs      ddrescue-format mapfile
│   └── read_error.rs   ReadCtx / ReadAction state machine
├── scsi/               SCSI transport (Linux SG_IO, macOS IOKit, Windows SPTI)
├── platform/           Drive unlock (MT1959 A/B)
├── aacs/               AACS decryption (handshake, keys, keydb, decrypt)
├── css/                DVD CSS cipher
├── decrypt.rs          Unified decrypt dispatcher (AACS/CSS/None)
├── pes.rs              PES frame types, FrameSource / FrameSink traits
├── sector/             Sector I/O (was sector.rs in 0.17)
│   ├── mod.rs          SectorSource, SectorSink traits
│   ├── file.rs         FileSectorSource, FileSectorSink (ISO-backed)
│   └── decrypting.rs   DecryptingSectorSource decorator
├── udf.rs              UDF 2.50 filesystem parser
├── mpls.rs             MPLS playlist parser
├── clpi.rs             CLPI clip info parser
├── ifo.rs              DVD IFO parser
├── labels/             BD-J label extraction (5 format parsers)
├── keydb.rs            KEYDB download, parse, save
├── identity.rs         DriveId from INQUIRY
├── profile.rs          Bundled drive profiles
├── speed.rs            DriveSpeed enum
├── mux/
│   ├── mod.rs          Public mux exports
│   ├── resolve.rs      URL parser + input/output (Box<dyn FrameSource/Sink>)
│   ├── meta.rs         FMKV header format
│   ├── disc.rs         DiscStream (optical drive → PES)
│   ├── iso.rs          IsoStream (ISO image read)
│   ├── isowriter.rs    ISO image writer (UDF, AVDP, multi-extent)
│   ├── mkvstream.rs    MkvStream (bidirectional Matroska)
│   ├── mkvout.rs       MkvOutputStream (PES → MKV)
│   ├── m2ts.rs         M2tsStream (BD-TS)
│   ├── pesout.rs       PES output streams (M2ts, Network, Stdio, Null)
│   ├── network.rs      NetworkStream (TCP + FMKV header)
│   ├── stdio.rs        StdioStream (stdin/stdout pipe)
│   ├── null.rs         NullStream (discard + byte counter)
│   ├── lookahead.rs    LookaheadBuffer (codec header scanning)
│   ├── ts.rs           BD-TS demuxer + PAT/PMT scanner
│   ├── tsreader.rs     TS reader utilities
│   ├── tsmux.rs        TS muxer (PES → BD-TS packets)
│   ├── ps.rs           MPEG-2 PS demuxer (DVD)
│   ├── ebml.rs         EBML read/write primitives
│   ├── mkv.rs          MKV muxer (tracks, clusters, cues)
│   └── codec/          Frame parsers (H.264, HEVC, MPEG-2, VC-1, AC3, EAC3, DTS, TrueHD, LPCM, PGS)
└── ...

freemkv/src/
├── main.rs             CLI dispatcher (URL routing)
├── pipe.rs             PES pipeline — source → dest copy
├── disc_info.rs        Disc/file info display
├── info.rs             Drive info + profile submission
├── strings.rs          i18n string table
├── output.rs           Verbosity-filtered output
└── build.rs            Bundled locale code generation
```

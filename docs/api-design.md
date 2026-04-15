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

The `pes::Stream` trait:

```rust
pub trait Stream {
    fn read(&mut self) -> io::Result<Option<PesFrame>>;
    fn write(&mut self, frame: &PesFrame) -> io::Result<()>;
    fn finish(&mut self) -> io::Result<()>;
    fn info(&self) -> &DiscTitle;
    fn codec_private(&self, track: usize) -> Option<Vec<u8>>;
    fn headers_ready(&self) -> bool;
}
```

## IOStream (byte-level API)

For raw byte copies (disc→ISO, resume, benchmarks). Lower level than PES.

```rust
let opts = InputOptions::default();
let mut input = open_input("iso://Disc.iso", &opts)?;
let mut output = open_output("mkv://Movie.mkv", input.info())?;
io::copy(&mut *input, &mut *output)?;
output.finish()?;
```

## Streams

All streams implement `IOStream` (byte-level) and/or `pes::Stream` (frame-level).
URL-based resolvers open any stream by string.

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
// PES pipeline (frame-level)
let input = libfreemkv::input("disc:///dev/sg4", &opts)?;    // DiscStream
let input = libfreemkv::input("iso://Dune.iso", &opts)?;     // IsoStream
let output = libfreemkv::output("mkv://Dune.mkv", &title)?;  // MkvOutputStream
let output = libfreemkv::output("m2ts://Dune.m2ts", &title)?; // M2tsOutputStream
let output = libfreemkv::output("network://10.1.7.11:9000", &title)?; // NetworkOutputStream
let output = libfreemkv::output("null://", &title)?;          // NullOutputStream

// IOStream (byte-level)
let input = open_input("disc://", &opts)?;                    // DiscStream
let input = open_input("iso://Dune.iso", &opts)?;             // IsoStream
let output = open_output("iso://Copy.iso", &meta)?;           // IsoStream (write)
let output = open_output("mkv://Dune.mkv", &meta)?;           // MkvStream
let output = open_output("m2ts://Dune.m2ts", &meta)?;         // M2tsStream
let output = open_output("null://", &meta)?;                  // NullStream
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
    BytesRead { bytes: u64, total: u64 },
    ReadError { sector: u64, error: Error },
    Retry { attempt: u32 },
    SpeedChange { speed_kbs: u16 },
    ExtentStart { index: usize, start_sector: u64, sector_count: u64 },
    Complete { bytes: u64, errors: u32 },
}
```

Events report what happened. App decides what to do. GUI shows a dialog. CLI
prints a line. Server logs to file.

## File Layout

```
libfreemkv/src/
├── lib.rs              Public exports
├── error.rs            Error codes (no English)
├── event.rs            Event types for callbacks
├── drive/              Drive (open, init, read with recovery)
│   ├── mod.rs          Drive struct, init, read, reset, eject
│   ├── capture.rs      Drive profile capture for contribution
│   ├── linux.rs        Linux drive discovery
│   ├── macos.rs        macOS drive discovery
│   └── windows.rs      Windows drive discovery
├── disc/               Disc (scan, titles, AACS setup)
├── scsi/               SCSI transport (Linux SG_IO, macOS IOKit, Windows SPTI)
├── platform/           Drive unlock (MT1959 A/B)
├── aacs/               AACS decryption (handshake, keys, keydb, decrypt)
├── css/                DVD CSS cipher
├── decrypt.rs          Unified decrypt dispatcher (AACS/CSS/None)
├── pes.rs              PES frame types, Stream trait
├── sector.rs           SectorReader trait
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
│   ├── mod.rs          IOStream trait, public exports
│   ├── resolve.rs      URL parser + open_input/open_output + input/output
│   ├── meta.rs         FMKV header format
│   ├── disc.rs         DiscStream (optical drive → PES)
│   ├── iso.rs          IsoStream (ISO image read/write)
│   ├── isowriter.rs    ISO image writer (UDF, AVDP, multi-extent)
│   ├── mkvstream.rs    MkvStream (bidirectional Matroska, IOStream)
│   ├── mkvout.rs       MkvOutputStream (PES → MKV)
│   ├── m2ts.rs         M2tsStream (BD-TS, IOStream)
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

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
let mut session = DriveSession::open(path)?;
session.wait_ready()?;
session.init()?;
session.probe_disc()?;

// Scan disc
let disc = Disc::scan(&mut session, &ScanOptions::default())?;

// Browse
disc.titles      // Vec<Title>
disc.format      // BD / UHD / DVD
disc.capacity_gb()

// Rip with events
disc.rip(&mut session, 0, output, |event| {
    match event.kind {
        EventKind::BytesRead { bytes, total } => ...,
        EventKind::ReadError { sector, error } => ...,
        EventKind::Retry { attempt } => ...,
        EventKind::SpeedChange { speed_kbs } => ...,
        EventKind::Complete { bytes, errors } => ...,
    }
})?;

// Rip without events
disc.rip(&mut session, 0, output, event::ignore)?;
```

## Stream Chains

Each stream wraps the next. Builder pattern, no `.build()`.

### Raw m2ts
```rust
disc.rip(&mut session, 0, File::create("movie.m2ts")?, event::ignore)?;
```

### MKV
```rust
let output = MkvStream::new(File::create("movie.mkv")?)
    .title(&disc.titles[0])
    .max_buffer(10 * 1024 * 1024);

disc.rip(&mut session, 0, output, |e| { ... })?;
```

### MKV with progress (CLI)
```rust
let output = ProgressStream::new(
    MkvStream::new(File::create("movie.mkv")?)
        .title(&disc.titles[0])
        .max_buffer(10 * 1024 * 1024),
    total_bytes,
    |pct, speed| eprint!("\r  {}%  {:.1} MB/s", pct, speed),
);

disc.rip(&mut session, 0, output, |e| { ... })?;
```

### Future: transcode
```rust
let output = ProgressStream::new(
    TranscodeStream::new(
        MkvStream::new(File::create("movie.mkv")?)
            .title(&disc.titles[0])
            .max_buffer(50 * 1024 * 1024),
    )
        .codec(H265)
        .quality(22),
    total_bytes,
    |pct, speed| eprint!("\r  {}%  {:.1} MB/s", pct, speed),
);

disc.rip(&mut session, 0, output, |e| { ... })?;
```

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

Events report what happened. App decides what to do. GUI shows a dialog. CLI prints a line. Server logs to file.

## Error Codes

Lib errors are codes, not messages. Like HTTP status codes.

```rust
pub enum Error {
    // Drive
    DriveNotFound,
    DriveOpenFailed,
    DriveNotReady,
    
    // Unlock
    UnlockFailed,
    NoProfile,
    
    // AACS
    AacsNoKeys,
    AacsCertVerifyFailed,
    AacsAgidAllocFailed,
    AacsHandshakeFailed,
    AacsVidMacFailed,
    
    // Disc
    DiscReadError { sector: u64 },
    MplsParseError,
    ClpiParseError,
    UdfFileNotFound { path: String },
    
    // Mux
    LookaheadOverflow,
    MuxWriteError,
    
    // SCSI
    ScsiError { sense: u8 },
}
```

App maps codes to localized strings. Lib never contains display text.

## Streams

All streams implement the `IOStream` trait (Read + Write). URL-based resolver opens any stream by string.

| Stream | Input | Output | URL | Transport |
|--------|-------|--------|-----|-----------|
| DiscStream | Yes | -- | `disc://` `disc:///dev/sg4` | Optical drive via SCSI |
| IsoStream | Yes | -- | `iso://path.iso` | Blu-ray ISO image |
| MkvStream | Yes | Yes | `mkv://path` | Matroska container |
| M2tsStream | Yes | Yes | `m2ts://path` | BD-TS with FMKV metadata header |
| NetworkStream | Yes (listen) | Yes (connect) | `network://host:port` | TCP with FMKV metadata header |
| StdioStream | Yes (stdin) | Yes (stdout) | `stdio://` | Raw byte pipe |
| NullStream | -- | Yes | `null://` | Discard sink (byte counter) |

All URLs require a `scheme://path` format. Bare paths are rejected.

```rust
// URL-based opening
let input = open_input("disc://", &opts)?;                   // DiscStream (auto-detect)
let input = open_input("disc:///dev/sg4", &opts)?;           // DiscStream (specific device)
let input = open_input("iso://Dune.iso", &opts)?;            // IsoStream
let input = open_input("m2ts:///tmp/Dune.m2ts", &opts)?;     // M2tsStream
let input = open_input("mkv://Dune.mkv", &opts)?;            // MkvStream
let input = open_input("network://0.0.0.0:9000", &opts)?;    // NetworkStream (listen)
let input = open_input("stdio://", &opts)?;                   // StdioStream (stdin)

let output = open_output("mkv://Dune.mkv", &meta)?;          // MkvStream
let output = open_output("m2ts://Dune.m2ts", &meta)?;        // M2tsStream
let output = open_output("network://10.1.7.11:9000", &meta)?;// NetworkStream (connect)
let output = open_output("stdio://", &meta)?;                 // StdioStream (stdout)
let output = open_output("null://", &meta)?;                  // NullStream

// Direct construction (for advanced use)
let mkv = MkvStream::new(writer).meta(&title).max_buffer(10 * 1024 * 1024);
let m2ts = M2tsStream::new(writer).meta(&title);
let net = NetworkStream::connect("10.1.7.11:9000")?.meta(&title);
let null = NullStream::new().meta(&title);
```

### FMKV Metadata Header

M2tsStream and NetworkStream embed a JSON metadata header before the BD-TS data:

```
[8B magic "FMKV\0\0\0\0"][4B JSON length][JSON metadata][padding to 192B boundary][BD-TS data...]
```

The header carries title name, duration, and full stream layout (PIDs, codecs, languages, labels). This allows the receiving end to set up demuxing and track metadata without scanning the TS.

### MkvStream Internals

LookaheadBuffer (default 5MB, configurable):
1. Phase 1: buffer incoming data, scan for codec setup (SPS/PPS)
2. Found it? Write MKV header, flush buffer, switch to streaming
3. Buffer full? Error — app handles it
4. Phase 2: parse TS → frames → MKV clusters, direct to output

Reading: extracts MKV frames, wraps back into BD-TS PES packets.

## File Layout

```
libfreemkv/src/
├── lib.rs              Public exports
├── error.rs            Error codes (no English)
├── event.rs            Event types for callbacks
├── drive.rs            DriveSession (open, init, read)
├── disc.rs             Disc (scan, rip, titles)
├── scsi/               SCSI transport (Linux, macOS)
├── platform/           Drive unlock (MT1959 A/B)
├── aacs/               AACS decryption
├── udf.rs              UDF filesystem parser
├── mpls.rs             Playlist parser
├── clpi.rs             Clip info parser
├── mux/
│   ├── mod.rs          IOStream trait, public exports
│   ├── resolve.rs      URL parser + open_input/open_output
│   ├── meta.rs         M2tsMeta (FMKV header format)
│   ├── disc.rs         DiscStream (optical drive)
│   ├── mkvstream.rs    MkvStream (bidirectional Matroska)
│   ├── m2ts.rs         M2tsStream (BD-TS + FMKV header)
│   ├── network.rs      NetworkStream (TCP + FMKV header)
│   ├── stdio.rs        StdioStream (stdin/stdout pipe)
│   ├── iso.rs          IsoStream (Blu-ray ISO image)
│   ├── null.rs         NullStream (discard + byte counter)
│   ├── lookahead.rs    LookaheadBuffer (codec header scanning)
│   ├── ts.rs           BD-TS demuxer + PAT/PMT scanner
│   ├── ebml.rs         EBML read/write primitives
│   ├── mkv.rs          MKV muxer (tracks, clusters, cues)
│   └── codec/          Frame parsers (H.264, HEVC, VC-1, AC3, DTS, TrueHD, PGS, LPCM)
└── ...

freemkv/src/
├── main.rs             CLI dispatcher (URL routing)
├── pipe.rs             Generic source → dest copy
├── rip.rs              Rip with progress display
├── remux.rs            Remux with progress display
├── disc_info.rs        Disc info display
├── info.rs             Drive info + profile submission
├── strings.rs          i18n string table
├── output.rs           Verbosity-filtered output
└── build.rs            Bundled locale code generation
```

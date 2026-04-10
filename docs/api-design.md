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

## Streams the Lib Provides

| Stream | Purpose |
|--------|---------|
| MkvStream | BD-TS → MKV (demux + mux) |

CLI provides:
| Stream | Purpose |
|--------|---------|
| ProgressStream | Byte counting + progress callback |
| Future: TranscodeStream | Re-encode video |

Any `impl Write` works as a stream. Third-party apps create their own.

## MkvStream Internals

LookaheadBuffer (default 5MB, configurable):
1. Phase 1: buffer incoming data, scan for codec setup (SPS/PPS)
2. Found it? Write MKV header, flush buffer, switch to streaming
3. Buffer full? Error — app handles it
4. Phase 2: parse TS → frames → MKV clusters, direct to output

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
│   ├── stream.rs       MkvStream (builder pattern, impl Write)
│   ├── lookahead.rs    LookaheadBuffer (generic, reusable)
│   ├── ts.rs           BD-TS demuxer
│   ├── ebml.rs         EBML primitives
│   ├── mkv.rs          MKV muxer
│   └── codec/          Frame parsers (H.264, HEVC, VC-1, AC3, DTS, TrueHD, PGS)
└── ...

freemkv/src/
├── main.rs             CLI entry, command routing
├── rip.rs              Rip command (streams + progress)
├── remux.rs            Remux command (m2ts → MKV, no drive)
├── info.rs             Drive info display
└── disc_info.rs        Disc info display
```

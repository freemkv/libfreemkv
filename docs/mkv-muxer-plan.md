# MKV Native Muxer — Architecture

## Goal

Replace raw m2ts output with in-pipeline MKV muxing.
Disc → TS demux → MKV mux → .mkv file. One pass, no temp files.

## Data Flow

```
ContentReader::read_batch()        returns &[u8] of raw BD transport stream
        ↓
TsDemuxer::feed(batch)             parses 192-byte BD-TS packets, extracts PES
        ↓
PES reassembly per PID             builds complete PES packets with PTS/DTS
        ↓
ElementaryStreamParser per track   finds frame boundaries, extracts codec headers
        ↓
MkvMuxer::write_frame(track, pts, data)   writes EBML clusters + blocks
        ↓
.mkv file on disk
```

## Current Integration Point

```rust
// rip.rs line ~287
match reader.read_batch() {
    Ok(Some(batch)) => {
        writer.write_all(batch)?;  // ← replace with muxer.feed(batch)
    }
}
```

Becomes:
```rust
match reader.read_batch() {
    Ok(Some(batch)) => {
        muxer.feed(batch)?;
    }
}
```

## Components

### 1. BD Transport Stream Demuxer (`ts.rs`)

BD uses 192-byte packets (not standard 188):
```
[0-3]   TP_extra_header: 2-bit copy_permission + 30-bit arrival_time_stamp
[4]     Sync byte: 0x47
[5]     TEI + PUSI + priority + PID[12:8]
[6]     PID[7:0]
[7]     Scrambling + adaptation + continuity_counter
[8..]   Adaptation field (if present) + payload
```

API:
```rust
pub struct TsDemuxer {
    pes_assemblers: HashMap<u16, PesAssembler>,  // PID → assembler
}

impl TsDemuxer {
    pub fn new(pids: &[u16]) -> Self;
    pub fn feed(&mut self, data: &[u8]) -> Vec<PesPacket>;
}

pub struct PesPacket {
    pub pid: u16,
    pub pts: Option<i64>,   // 90kHz ticks
    pub dts: Option<i64>,   // 90kHz ticks
    pub data: Vec<u8>,      // elementary stream data
}
```

### 2. Elementary Stream Parsers (`codec/`)

Each codec parser finds frame boundaries and extracts initialization data.

**H.264 (`codec/h264.rs`):**
- Parse NAL units (start code 00 00 01 or 00 00 00 01)
- Extract SPS + PPS for codecPrivate
- Frame boundary = Access Unit Delimiter (NAL type 9) or SPS

**HEVC (`codec/hevc.rs`):**
- Parse NAL units
- Extract VPS + SPS + PPS for codecPrivate
- Frame boundary = VCL NAL with first_slice_segment_in_pic_flag

**AC3/EAC3 (`codec/ac3.rs`):**
- Syncword 0x0B77
- Parse frame size from header
- No codecPrivate needed (or minimal)

**DTS (`codec/dts.rs`):**
- Syncword 0x7FFE8001
- Parse frame size
- No codecPrivate needed

**TrueHD (`codec/truehd.rs`):**
- Major sync: 0xF8726FBA
- Access unit = major sync + minor syncs
- AC3 core embedded in first substream

**LPCM (`codec/lpcm.rs`):**
- Fixed frame sizes based on sample rate + channels
- Header describes format

**PGS (`codec/pgs.rs`):**
- Segment types: PCS, WDS, PDS, ODS, END
- Each segment is a complete unit
- No codecPrivate needed

### 3. MKV/EBML Muxer (`mkv.rs`)

Matroska uses EBML (Extensible Binary Meta Language).

**EBML primitives:**
- Variable-length element ID (1-4 bytes)
- Variable-length size (1-8 bytes)
- Data: uint, int, float, string, UTF-8, binary, date

**MKV structure:**
```
EBML Header
Segment
├── SeekHead (index of top-level elements)
├── Info (title, duration, muxing app)
├── Tracks (one entry per stream)
│   ├── TrackEntry (video)
│   │   ├── CodecID: "V_MPEG4/ISO/AVC" or "V_MPEGH/ISO/HEVC"
│   │   ├── CodecPrivate: SPS+PPS (H.264) or VPS+SPS+PPS (HEVC)
│   │   └── Video: PixelWidth, PixelHeight, DisplayWidth, DisplayHeight
│   ├── TrackEntry (audio)
│   │   ├── CodecID: "A_AC3" or "A_TRUEHD" or "A_DTS"
│   │   └── Audio: SamplingFrequency, Channels, BitDepth
│   └── TrackEntry (subtitle)
│       └── CodecID: "S_HDMV/PGS"
├── Chapters (optional, from MPLS chapter marks)
├── Cluster (every ~5 seconds)
│   ├── Timestamp (cluster base time)
│   ├── SimpleBlock (track, relative_ts, data)
│   ├── SimpleBlock ...
│   └── ...
├── Cluster ...
├── Cues (seek index, written at end)
└── Tags (metadata)
```

**API:**
```rust
pub struct MkvMuxer<W: Write + Seek> {
    writer: W,
    tracks: Vec<MkvTrack>,
    cluster_start: Option<i64>,
    cue_points: Vec<CuePoint>,
}

impl<W: Write + Seek> MkvMuxer<W> {
    pub fn new(writer: W, tracks: &[MkvTrack]) -> Result<Self>;
    pub fn write_frame(&mut self, track_idx: usize, pts_ns: i64, keyframe: bool, data: &[u8]) -> Result<()>;
    pub fn finish(self) -> Result<()>;  // writes Cues + fixes SeekHead
}
```

### 4. Pipeline Glue (`mux.rs`)

Ties everything together:

```rust
pub struct MuxPipeline<W: Write + Seek> {
    demuxer: TsDemuxer,
    parsers: HashMap<u16, Box<dyn CodecParser>>,
    muxer: MkvMuxer<W>,
    pid_to_track: HashMap<u16, usize>,
}

impl<W: Write + Seek> MuxPipeline<W> {
    pub fn new(writer: W, streams: &[Stream]) -> Result<Self>;
    pub fn feed(&mut self, ts_data: &[u8]) -> Result<()>;
    pub fn finish(self) -> Result<()>;
}
```

## File Layout

```
libfreemkv/src/
├── mux/
│   ├── mod.rs          MuxPipeline (glue)
│   ├── ts.rs           BD-TS demuxer (192-byte packets)
│   ├── ebml.rs         EBML primitives (write variable-length ints)
│   ├── mkv.rs          MKV muxer (Segment, Tracks, Clusters)
│   └── codec/
│       ├── mod.rs      CodecParser trait
│       ├── h264.rs     H.264 NAL parser
│       ├── hevc.rs     HEVC NAL parser
│       ├── ac3.rs      AC3/EAC3 frame parser
│       ├── dts.rs      DTS frame parser
│       ├── truehd.rs   TrueHD/Atmos parser
│       ├── lpcm.rs     LPCM frame parser
│       └── pgs.rs      PGS subtitle parser
```

## MKV Codec IDs

| Our Codec | MKV CodecID | codecPrivate |
|-----------|------------|--------------|
| H264 | V_MPEG4/ISO/AVC | AVCDecoderConfigurationRecord (SPS+PPS) |
| Hevc | V_MPEGH/ISO/HEVC | HEVCDecoderConfigurationRecord (VPS+SPS+PPS) |
| Vc1 | V_MS/VFW/FOURCC | BITMAPINFOHEADER |
| Mpeg2 | V_MPEG2 | sequence_header |
| Ac3 | A_AC3 | none |
| Ac3Plus | A_EAC3 | none |
| TrueHd | A_TRUEHD | none |
| DtsHdMa | A_DTS | none (core + extension) |
| Dts | A_DTS | none |
| Lpcm | A_PCM/INT/BIG | none |
| Pgs | S_HDMV/PGS | none |

## Timestamps

BD uses 90kHz PTS/DTS. MKV uses nanoseconds.
Conversion: `ns = pts * 1_000_000_000 / 90_000` = `pts * 100_000 / 9`

MKV TimestampScale default = 1,000,000 (1ms precision).
For BD content, 1ms is sufficient.

## Build Order

1. `ebml.rs` — EBML write primitives (smallest, no dependencies)
2. `ts.rs` — BD-TS demuxer (parse 192-byte packets, PES assembly)
3. `codec/ac3.rs` — simplest codec parser (fixed syncword)
4. `mkv.rs` — MKV muxer (header, tracks, clusters, blocks)
5. `mux.rs` — pipeline glue
6. Test with AC3-only stream (simplest case)
7. `codec/h264.rs` — video parser (NAL units, SPS/PPS)
8. Full BD rip test (video + audio + subs)
9. Remaining codecs (HEVC, DTS, TrueHD, PGS, LPCM, VC-1)

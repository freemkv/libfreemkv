# MPLS Playlist Format

## What is MPLS?

MPLS (Movie PlayList) files define playback titles on a Blu-ray disc. Each `.mpls` file in `BDMV/PLAYLIST/` describes one title -- a sequence of clips with precise in/out timestamps and stream information. The main movie, bonus features, trailers, and menus each have their own MPLS file.

A disc may contain dozens of MPLS files. Most are short (menus, logos, transitions). The main movie is typically the longest playlist. libfreemkv filters out playlists shorter than 30 seconds.

## File Structure

MPLS files use big-endian byte order throughout. The file has three main sections:

```
Offset  Size  Field
------  ----  -----
0       4     Magic: "MPLS"
4       4     Version: "0200" (BD) or "0300" (UHD BD)
8       4     PlayList start offset (absolute from file start)
12      4     PlayListMark start offset
16      4     ExtensionData start offset
```

### PlayList Section

Located at the PlayList start offset. Contains all play items and sub-path entries:

```
Offset  Size  Field
------  ----  -----
0       4     PlayList length
4       2     Reserved
6       2     Number of PlayItems
8       2     Number of SubPaths
10      ...   PlayItem entries (variable length)
```

### Play Items

Each play item references one clip and specifies what portion to play:

```
Offset  Size  Field
------  ----  -----
0       2     PlayItem length (bytes after this field)
2       5     Clip ID (ASCII, e.g. "00001")
7       4     Codec ID ("M2TS")
11      1     Connection condition (lower 4 bits)
12      1     Ref to STC_id
14      4     IN_time (45kHz PTS ticks)
18      4     OUT_time (45kHz PTS ticks)
22      8     UO_mask_table
30      1     Misc flags
31      1     still_mode
32      ...   STN_table (first play item only is parsed)
```

**Connection condition** values:
- `1` = seamless connection (no gap between clips)
- `5`, `6` = non-seamless connection

**Timestamps** use 45kHz PTS (Presentation Time Stamp) ticks, the same timebase as MPEG transport streams. Duration of a play item = `OUT_time - IN_time`. To convert to seconds: divide by 45000.

A playlist's total duration is the sum of all play item durations.

### Clip ID Mapping

The Clip ID (e.g. "00001") maps to:
- `BDMV/STREAM/00001.m2ts` -- the transport stream
- `BDMV/CLIPINF/00001.clpi` -- the clip info (EP map, stream details)

## STN Table

The Stream Number Table describes all elementary streams available in the clip. It is parsed from the first play item (which defines the title's stream layout).

```
Offset  Size  Field
------  ----  -----
0       2     STN table length
2       2     Reserved
4       1     Number of primary video streams
5       1     Number of primary audio streams
6       1     Number of PG (subtitle) streams
7       1     Number of IG (interactive graphics) streams
8       ...   Stream entries
```

### Stream Entries

Each stream entry has two parts: a stream reference and a stream attributes block.

**Stream reference:**

```
Offset  Size  Field
------  ----  -----
0       1     Entry length
1       1     Stream type (1=PlayItem, 2=SubPath, 3=InMux)
2       2     PID (MPEG-TS packet ID, big-endian)
```

**Stream attributes** (immediately follows the reference):

```
Offset  Size  Field
------  ----  -----
0       1     Attributes length
1       1     Coding type
2+      ...   Type-specific fields
```

The layout of type-specific fields depends on the stream category:

**Video streams:**

| Offset | Size | Field |
|--------|------|-------|
| 1 | 1 | Coding type |
| 2 | 1 | Format (upper 4 bits) + frame rate (lower 4 bits) |

**Audio streams:**

| Offset | Size | Field |
|--------|------|-------|
| 1 | 1 | Coding type |
| 2 | 1 | Format (upper 4 bits) + sample rate (lower 4 bits) |
| 3 | 3 | Language code (ISO 639-2, e.g. "eng") |

**PG subtitle and IG streams:**

| Offset | Size | Field |
|--------|------|-------|
| 1 | 1 | Coding type |
| 2 | 3 | Language code |

## Coding Types

The coding type byte identifies the codec:

| Value | Codec | Category |
|-------|-------|----------|
| `0x02` | MPEG-2 | Video |
| `0x1B` | H.264 / AVC | Video |
| `0x24` | HEVC / H.265 | Video |
| `0xEA` | VC-1 | Video |
| `0x80` | LPCM | Audio |
| `0x81` | AC-3 (Dolby Digital) | Audio |
| `0x82` | DTS | Audio |
| `0x83` | TrueHD (Dolby TrueHD) | Audio |
| `0x84` | AC-3 Plus (E-AC-3) | Audio |
| `0x85` | DTS-HD HR | Audio |
| `0x86` | DTS-HD MA | Audio |
| `0xA1` | AC-3 Plus (secondary) | Audio |
| `0xA2` | DTS-HD HR (secondary) | Audio |
| `0x90` | PGS (Presentation Graphics) | Subtitle |
| `0x91` | PGS (Interactive Graphics) | Subtitle |

## Video Format Codes

| Value | Resolution |
|-------|-----------|
| 1 | 480i |
| 2 | 576i |
| 3 | 480p |
| 4 | 1080i |
| 5 | 720p |
| 6 | 1080p |
| 7 | 576p |
| 8 | 2160p |

## Video Frame Rate Codes

| Value | Frame Rate |
|-------|-----------|
| 1 | 23.976 fps |
| 2 | 24 fps |
| 3 | 25 fps |
| 4 | 29.97 fps |
| 6 | 50 fps |
| 7 | 59.94 fps |

## Audio Format Codes

| Value | Channels |
|-------|----------|
| 1 | Mono |
| 3 | Stereo |
| 6 | 5.1 surround |
| 12 | 7.1 surround |

## Audio Sample Rate Codes

| Value | Rate |
|-------|------|
| 1 | 48 kHz |
| 4 | 96 kHz |
| 5 | 192 kHz |
| 12 | 48/192 kHz (combo) |
| 14 | 48/96 kHz (combo) |

## How Playlists Map to Titles

libfreemkv's `Disc::scan()` reads every MPLS file from the disc and builds a `Title` for each:

1. Read all `.mpls` files from `BDMV/PLAYLIST/` via UDF.
2. Parse each with `mpls::parse()`.
3. Calculate duration by summing `(OUT_time - IN_time)` across all play items.
4. Discard playlists shorter than 30 seconds.
5. For each play item, load the corresponding CLPI file to get EP map data and compute sector extents (see [clpi.md](clpi.md)).
6. Extract stream info from the STN table of the first play item.
7. Sort titles by duration, longest first.

The resulting `Title` struct contains everything needed to rip: streams, duration, byte size, and the sector extents to read from disc.

## References

- BD-ROM Part 3, Section 5.3: PlayList file format
- https://github.com/lw/BluRay/wiki/MPLS

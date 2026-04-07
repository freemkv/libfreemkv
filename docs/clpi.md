# CLPI Clip Information Format

## What is CLPI?

CLPI (Clip Information) files describe the structure of individual M2TS transport streams on a Blu-ray disc. Each `.clpi` file in `BDMV/CLIPINF/` corresponds to one `.m2ts` file in `BDMV/STREAM/` with the same numeric name (e.g. `00001.clpi` describes `00001.m2ts`).

The most important data in a CLPI file is the **EP (Entry Point) map**, which provides timestamp-to-sector mapping. This is essential for seeking and for extracting the specific sector ranges that correspond to a playlist's in/out time window.

## File Structure

CLPI files use big-endian byte order. The header:

```
Offset  Size  Field
------  ----  -----
0       4     Magic: "HDMV"
4       4     Version: "0200" (BD) or "0300" (UHD BD)
8       4     SequenceInfo start offset
12      4     ProgramInfo start offset
16      4     CPI (Characteristic Point Information) start offset
20      4     ClipMark start offset
24      4     ExtensionData start offset
```

### ClipInfo Section (offset 40)

Contains basic clip metadata. The source packet count (total number of 192-byte source packets in the M2TS file) is at offset 56:

```
Offset  Size  Field
------  ----  -----
40      4     ClipInfo length
44      2     Reserved
46      1     Stream type
47      1     Application type
48      4     Reserved
52      4     TS recording rate
56      4     Source packet count
```

The source packet count multiplied by 192 gives the total byte size of the M2TS file.

## EP Map

The EP map lives inside the CPI section and provides random access into the transport stream. It maps PTS timestamps to Source Packet Numbers (SPN), enabling precise seeking without scanning the stream.

### CPI Section Layout

```
Offset  Size  Field
------  ----  -----
0       4     CPI length
4       2     Reserved / CPI type
6       ...   EP map
```

### EP Map Header

```
Offset  Size  Field
------  ----  -----
0       1     Reserved
1       1     Number of stream PID entries
2       ...   Stream PID entry headers (one per stream)
```

Each stream PID entry header (14 bytes):

```
Offset  Size  Field
------  ----  -----
0       2     Stream PID
2       2     Reserved + EP stream type
4       2     Number of coarse entries
6       4     Number of fine entries (note: 32-bit, can be large)
10      4     EP map start offset (relative to EP map start)
```

libfreemkv parses only the first stream (primary video), which is sufficient for sector-level seeking.

### Two-Level Index

The EP map uses a two-level structure to compress what would otherwise be a very large lookup table:

- **Coarse entries**: low-resolution index covering large time/sector ranges
- **Fine entries**: high-resolution entries within each coarse range

Each coarse entry points to a range of fine entries via `ref_to_fine_id`.

### Coarse Entries (8 bytes each)

Located immediately after the fine table start offset (4 bytes) in the per-stream EP map:

```
Bits    Field
------  -----
[31:14] ref_to_fine_id (18 bits) -- index of first fine entry in this range
[13:0]  pts_coarse (14 bits) -- upper bits of PTS
```

Second dword:

```
Bits    Field
------  -----
[31:0]  spn_coarse (32 bits) -- upper bits of SPN
```

### Fine Entries (4 bytes each)

Located at the fine table start offset within the per-stream EP map:

```
Bits    Field
------  -----
[31]    is_angle_change_point (1 bit)
[30:28] I_end_position_offset (3 bits)
[27:17] pts_fine (11 bits) -- lower bits of PTS
[16:0]  spn_fine (17 bits) -- lower bits of SPN
```

## Reconstructing Full PTS and SPN

The full timestamp and packet number are assembled by combining the coarse and fine components:

### Full PTS

```
full_pts = (pts_coarse << 19) + (pts_fine << 8)
```

The coarse component provides the upper bits, shifted left by 19. The fine component provides mid-range bits, shifted left by 8. The resulting PTS is in 45kHz ticks, matching the MPLS timestamp format.

### Full SPN

```
full_spn = (spn_coarse & 0xFFFE0000) + spn_fine
```

The coarse SPN provides the upper 15 bits (masked to clear the lower 17). The fine SPN provides the lower 17 bits. The result is a Source Packet Number -- each source packet is 192 bytes in the M2TS file.

### Resolved EP Map

The `resolved_ep_map()` method iterates through all coarse entries and their associated fine entries to produce a flat list of `(PTS, SPN)` pairs. For each coarse entry at index `ci`:

- Fine entries range from `coarse[ci].ref_to_fine_id` to `coarse[ci+1].ref_to_fine_id` (or end of fine table for the last coarse entry).
- Each fine entry is combined with its parent coarse entry to produce one full `(PTS, SPN)` pair.

## Deriving Sector Extents for Ripping

Given a playlist's in_time and out_time (from MPLS), the CLPI EP map provides the sector ranges to read from disc. The `get_extents()` method does this in three steps:

### Step 1: PTS to SPN

Binary search the resolved EP map for the in_time and out_time:

- **start_spn**: the SPN at or before in_time (seek backward to the nearest I-frame)
- **end_spn**: the SPN at or after out_time (include the full GOP)

### Step 2: SPN to Byte Offset

Each Source Packet is 192 bytes (188 bytes MPEG-TS payload + 4 bytes M2TS header):

```
byte_offset = spn * 192
```

### Step 3: Byte Offset to Sector

M2TS files are stored contiguously on disc. Sectors are 2048 bytes:

```
start_sector = start_byte / 2048
end_sector   = (end_byte + 2047) / 2048
sector_count = end_sector - start_sector
```

The resulting `Extent` contains `start_lba` (relative to the M2TS file's starting sector on disc) and `sector_count`. The caller adds the file's absolute starting LBA from UDF to get disc-absolute sector numbers.

### Alignment Note

The 192-byte source packet size and 2048-byte sector size share no common factor beyond 1. A single sector contains roughly 10.67 source packets. The conversion rounds start down and end up to ensure complete coverage.

## Putting It Together

The full ripping pipeline chains three parsers:

1. **MPLS** provides clip IDs and in/out timestamps.
2. **CLPI** converts those timestamps to SPN ranges, then to sector extents.
3. **UDF** provides the file's starting LBA on disc for absolute sector addressing.

The `Disc::scan()` method in `src/disc.rs` orchestrates this: for each play item in each playlist, it loads the corresponding CLPI, calls `get_extents()` with the play item's in/out times, and collects the resulting sector ranges into the title's extent list.

## References

- BD-ROM Part 3, Section 5.5: Clip Information file format
- https://github.com/lw/BluRay/wiki/CLPI

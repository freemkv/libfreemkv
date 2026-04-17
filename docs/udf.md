# UDF 2.50 Filesystem Parser

## What is UDF?

UDF (Universal Disc Format) is the filesystem standard for optical media. It is defined by ECMA-167 with extensions from the OSTA (Optical Storage Technology Association). BD-ROM discs use UDF revision 2.50, which introduces the **metadata partition** -- a critical feature that separates file metadata from file content on disc.

## Why UDF 2.50 for Blu-ray?

Older UDF revisions (1.02, 1.50) scatter ICBs (file metadata) and file data across the same partition. On a high-capacity Blu-ray disc (25-100 GB), this creates excessive seeking when the drive needs to read a directory listing or locate a file. UDF 2.50 solves this by placing all metadata into a contiguous region near the beginning of the disc. The drive reads metadata from one compact area and streams file data from another -- no interleaved seeks.

BD-ROM Part 3 of the Blu-ray specification mandates UDF 2.50.

## Metadata Partitions

A UDF 2.50 BD-ROM has two logical partitions:

- **Partition 0 (Type 1)** -- the physical partition. Contains actual file data (m2ts streams, playlist files, etc.). Mapped directly to disc sectors starting at the Partition Descriptor's `partitionStartingLocation`.

- **Partition 1 (Type 2)** -- the metadata partition. Contains all ICBs (Inode-like structures), directory data, and the File Set Descriptor. The metadata partition is itself stored as a file within the physical partition. Its location is found by reading an Extended File Entry at LBA 0 of the physical partition.

The key rule: **ICBs and directory data live in the metadata partition. File content lives in the physical partition.** When an ICB's allocation descriptor gives an LBA, the partition it refers to depends on what the LBA describes -- metadata-relative for directory entries, physical-partition-relative for file data extents.

## Pointer Chain

Reading a UDF 2.50 filesystem follows a fixed chain of pointers. Each step reads one or two 2048-byte sectors:

```
Sector 256: AVDP (Anchor Volume Descriptor Pointer, tag 2)
  |
  v
Sectors 32-63: VDS (Volume Descriptor Sequence)
  |-- Partition Descriptor (tag 5) --> partition_start (physical sector)
  |-- Logical Volume Descriptor (tag 6) --> partition maps, FSD location
  |
  v
Partition Maps in LVD (offset 440):
  |-- Map 0: Type 1 (physical partition)
  |-- Map 1: Type 2 (metadata partition, identified by "*UDF Metadata Partition")
  |
  v
Metadata file ICB at partition_start + 0 (Extended File Entry, tag 266)
  |-- Allocation descriptor --> metadata content location
  |
  v
metadata_start = partition_start + allocation_position
  |
  v
FSD at metadata_start + 0 (File Set Descriptor, tag 256)
  |-- Root Directory ICB: long_ad at offset 400 --> root_lba (metadata-relative)
  |
  v
Root Directory ICB at metadata_start + root_lba (Extended File Entry, tag 266)
  |-- Allocation descriptor --> directory data location (metadata-relative)
  |
  v
Directory data: File Identifier Descriptors (tag 257)
  |-- Each FID names a file/subdirectory and points to its ICB
  |-- Recurse into subdirectories to build the full file tree
```

### AVDP (Sector 256)

The Anchor Volume Descriptor Pointer is always at sector 256 (ECMA-167 section 10.2). It points to the Main Volume Descriptor Sequence. Tag identifier = 2.

### VDS (Sectors 32+)

The Volume Descriptor Sequence contains:

- **Partition Descriptor (tag 5)**: byte offset 188 holds `partitionStartingLocation` -- the absolute sector where the physical partition begins.
- **Logical Volume Descriptor (tag 6)**: byte offset 268 holds the number of partition maps. The partition maps themselves start at offset 440. For BD-ROM, map 0 is Type 1 (physical) and map 1 is Type 2 (metadata).
- **Terminating Descriptor (tag 8)**: signals the end of the VDS.

### Metadata File

When two partition maps exist and the second is Type 2, the metadata partition content is located by reading the Extended File Entry at the first sector of the physical partition (partition_start + 0). This ICB's allocation descriptor gives the offset and length of the metadata content within the physical partition.

### File Set Descriptor

The FSD (tag 256) sits at metadata-relative LBA 0 (the first sector of the metadata content). It contains a long allocation descriptor at offset 400 pointing to the root directory ICB. The LBA in this long_ad is at bytes 404-407.

### Directory Traversal

Each directory is an ICB (Extended File Entry, tag 266, or File Entry, tag 261) whose allocation extent points to directory data. The directory data is a sequence of File Identifier Descriptors (FIDs, tag 257):

| FID Field | Offset | Size | Description |
|-----------|--------|------|-------------|
| Tag | 0 | 2 | Always 257 |
| File characteristics | 18 | 1 | Bit 1 = directory, bit 3 = parent |
| L_FI (name length) | 19 | 1 | Length of filename |
| ICB (long_ad) | 20 | 16 | Points to the entry's ICB |
| L_IU | 36 | 2 | Implementation use length |
| Filename | 38 + L_IU | L_FI | UDF-encoded filename |

FIDs are 4-byte aligned. The parser advances by `(38 + L_IU + L_FI + 3) & !3` bytes per entry.

### ICB Layout

Both File Entry (tag 261) and Extended File Entry (tag 266) share the same info_length field:

| Field | Tag 261 Offset | Tag 266 Offset |
|-------|---------------|---------------|
| info_length (u64) | 56 | 56 |
| L_EA (u32) | 168 | 208 |
| L_AD (u32) | 172 | 212 |
| Allocation descriptors | 176 + L_EA | 216 + L_EA |

Allocation descriptors use the Short Allocation Descriptor format: 4 bytes extent length (upper 2 bits = type), 4 bytes extent position (LBA).

## How read_filesystem() Works

The `read_filesystem()` function in `src/udf.rs` follows the pointer chain above:

1. Reads sector 256, validates AVDP (tag 2).
2. Scans sectors 32-63 for the Partition Descriptor and Logical Volume Descriptor.
3. If two partition maps exist and the second is Type 2, reads the metadata file ICB at partition_start to find metadata_start.
4. Reads the FSD at metadata_start, extracts the root directory ICB LBA.
5. Calls `read_directory()` recursively (max depth 3) to build the full file tree.

Each directory read involves two sector reads: one for the ICB, then one or more for the directory data. File sizes are read from info_length in each file's ICB.

`read_file()` reads a file by navigating the directory tree, reading the file's ICB to get its data extent, then reading the data sector by sector from the **physical partition** (partition_start + LBA, not metadata_start).

## Buffered Sector Reads

USB optical drives have ~500ms round-trip latency per SCSI command. Since `read_filesystem()` and `read_file()` issue one SCSI READ per sector, a full disc scan can require hundreds of commands -- taking 10+ minutes on USB.

`Disc::scan()` wraps the drive in a `BufferedSectorReader` before reading. On a single-sector read, the buffer prefetches a batch of sectors (sized from the kernel's `max_hw_sectors_kb` for the device) and caches them. Subsequent reads to nearby LBAs return from cache with zero SCSI overhead. After parsing the UDF directory structure, the entire metadata partition is pre-read into the cache, so all ICB lookups during title scanning and encryption resolution are instant.

The buffer is transparent -- `read_filesystem()`, `read_file()`, and all downstream code still call `read_sectors(lba, 1, buf)` as before. The batching happens inside the `SectorReader` implementation.

### UDF Filename Encoding

UDF filenames use a compression ID as the first byte:
- `8` = 8-bit characters (ASCII)
- `16` = 16-bit big-endian Unicode (UTF-16BE)

The parser handles both encodings. All path lookups are case-insensitive.

## BD-ROM Directory Structure

A typical Blu-ray disc has this directory layout:

```
/
+-- BDMV/
|   +-- index.bdmv          Disc index (title list, first play)
|   +-- MovieObject.bdmv    Movie objects (navigation commands)
|   +-- PLAYLIST/
|   |   +-- 00000.mpls      Main movie playlist
|   |   +-- 00001.mpls      Director's commentary
|   |   +-- ...
|   +-- CLIPINF/
|   |   +-- 00001.clpi      Clip info for 00001.m2ts
|   |   +-- 00002.clpi
|   |   +-- ...
|   +-- STREAM/
|   |   +-- 00001.m2ts      Transport stream (video/audio/subtitle data)
|   |   +-- 00002.m2ts
|   |   +-- ...
|   +-- BACKUP/              Duplicate of index, MovieObject, playlists, clip info
|
+-- AACS/                    AACS encryption data (encrypted discs only)
|   +-- Unit_Key_RO.inf      Unit key file (encrypted)
|   +-- MKB_RW.inf           Media Key Block
|   +-- Content000.cer       Content certificate
|   +-- DUPLICATE/           Backup copies
|
+-- CERTIFICATE/             BD+ certificate data (some discs)
```

The parser reads from `BDMV/PLAYLIST/` and `BDMV/CLIPINF/` to discover titles and their sector layouts. The `BDMV/STREAM/` directory contains the actual transport streams but is not parsed by the UDF layer -- stream data is read by LBA directly using extents computed from CLPI EP maps.

## References

- ECMA-167: Volume and File Structure of Write-Once and Rewritable Media
- UDF 2.50 (OSTA): Universal Disk Format Specification
- BD-ROM Part 3: Blu-ray Disc Read-Only Format, File System Specifications

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
5. Calls `read_directory()` recursively (max depth `MAX_DIR_DEPTH` = 8) to build the full file tree.

Each directory read involves two sector reads: one for the ICB, then one or more for the directory data. File sizes are read from info_length in each file's ICB.

`read_file()` reads a file by navigating the directory tree, reading the file's ICB to get its data extent, then reading the data sector by sector from the **physical partition** (partition_start + LBA, not metadata_start).

## Buffered Sector Reads

USB optical drives have ~500ms round-trip latency per SCSI command. Since `read_filesystem()` and `read_file()` issue one SCSI READ per sector, a full disc scan can require hundreds of commands -- taking 10+ minutes on USB.

`Disc::scan()` wraps the drive in a `BufferedSectorReader` before reading. On a single-sector read, the buffer prefetches a batch of sectors (sized from the kernel's `max_hw_sectors_kb` for the device) and caches them. Subsequent reads to nearby LBAs return from cache with zero SCSI overhead. After parsing the UDF directory structure, the entire metadata partition is pre-read into the cache, so all ICB lookups during title scanning and encryption resolution are instant.

`prefetch_ranges` pre-reads multiple sector ranges into the permanent cache,
in batch-sized chunks, stored per-sector in a `HashMap`. This is used to
bulk-load all small files (AACS, MPLS, CLPI, META) before scanning. The
permanent cache holds one ~2 KB `Vec` per sector in the `HashMap`, so the
total sector count bounds RAM; a crafted UDF (a bogus metadata-file size in
`metadata_sector_ranges`) could otherwise drive that count to billions. The
cumulative prefetched sectors are capped at `MAX_PREFETCH_SECTORS` (~1 GiB
of cache); once exceeded, prefetching stops seeding the cache. The
sliding-window read path still serves any LBA on demand, so this only
forgoes the bulk speed-up — it never loses data.

The buffer is transparent -- `read_filesystem()`, `read_file()`, and all downstream code still call `read_sectors(lba, 1, buf)` as before. The batching happens inside the `SectorSource` implementation.

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

## metadata_sector_ranges() skip policy

`metadata_sector_ranges()` skips directories named `STREAM` (case-insensitive)
and individual files larger than 50 MB. Nothing else is filtered by name —
`BACKUP`/`DUPLICATE` are traversed, and `MKB_RO.inf` is excluded only because
it exceeds the 50 MB cap.

## metadata_file_location() and read_directory() internals

`metadata_file_location` returns `None` when the map does not fit wholly
inside the descriptor, or when its partition type identifier is not
"*UDF Metadata Partition" — a Virtual (UDF 2.50 2.2.8) or Sparable (2.2.9)
partition map is also ECMA-167 3/10.7.3 Type 2 and records unrelated fields
at that offset, so its bytes must never be read as a location.

`read_directory` walks each directory's ICB (Extended File Entry) to find
directory data containing File Identifier Descriptors (FIDs); each FID names
a file/subdir and points to its ICB. Directories deeper than `MAX_DIR_DEPTH`
are recorded as entries but not descended into. `budget` tracks total FID
entries consumed across the whole tree walk and the walk aborts with
`Error::DiscRead` once it exceeds `MAX_TOTAL_DIR_ENTRIES`. `visited` is the
set of metadata-relative ICB LBAs already opened as directories; a repeated
LBA is a cycle and is skipped. The function's wide argument list (reader,
partition/meta offsets, depth, the global entry budget, and the
cycle-detection visited-set) is inherent to the walk, not a refactor smell.

## extents_abs_at(), file_extents(), and file_extents_addressing()

`extents_abs_at` resolves multi-extent / Long-AD / continuation ICBs to
absolute disc extents. Unrecorded (ECMA-167 4/14.14.1.1 type-1) extents are
included: their space is allocated to the file and occupies its byte space,
so dropping them would slide every later extent's bytes down by the hole's
length in a sequential extraction. They are returned FLAGGED
(`AbsExtent::recorded == false`), because keeping the extent while losing
the flag is the same bug the other way round: the caller would then read
sectors the file never wrote and ship whatever the media holds there.
`read_file_limited` emits zeros for such an extent; every consumer of this
list has to be able to do the same. It is `pub(crate)` because it returns
`AbsExtent`, itself internal.

`file_extents` refuses a file that contains an unrecorded extent because a
`(lba, sector_count)` pair cannot say "this range is a hole": reading it
splices undefined sectors into the stream as content, and dropping it
slides every later extent's byte space. Until an extent can carry the flag
end-to-end, refusing is the only truthful answer at this signature — and a
title that is not offered is not a title that was silently mis-ripped.

`file_extents_addressing` is the byte-space-only counterpart: it includes
unrecorded extents UNFLAGGED, in a shape identical to `file_extents`'s safe
return, because omitting one would misaddress the rest of the file and
nothing on this path treats the returned sectors as stream bytes. It stays
`pub(crate)` rather than public: an external consumer reaching for the more
general-sounding name would silently obtain a read plan over a hole, and a
doc comment alone is not a guard against that — scoping makes the misuse a
compile error.

## read_icb_extent(): first RECORDED extent, not extents.first()

`read_icb_extents` retains ECMA-167 4/14.14.1.1 type-1 (allocated, not
recorded) descriptors, because dropping one would slide every later extent's
data down by the hole's length. But a type-1 extent's `lba` is where SPACE is
allocated, not where bytes live, and `read_icb_extent`'s one caller —
`file_start_lba` — hands its result out as "the absolute starting LBA of a
file's first data extent". `ifo.rs` then uses that as the base for every VTS
VOB extent (`file_start_lba(IFO) + vtstt_vobs + cell.first_sector`), so a
file whose FIRST descriptor is type-1 would put the entire video title set
at the wrong place on disc, with no error anywhere. `read_icb_extent`
therefore returns the first extent with `recorded == true`, not
`extents.first()`.

## read_inline_data() and the AACS .inf regression

Tiny files such as the AACS `*.inf` key files are routinely embedded
directly in the ICB (ICB Tag flags low 3 bits == 3) rather than via
out-of-line extents. `read_icb_extents` finds no real extents for them (it
would misparse the embedded payload as allocation descriptors), so
`read_file` must read the inline payload via `read_inline_data` instead. A
0.31.0 regression added a per-extent `MAX_FILE_BYTES` cap that turned the
misparsed-embedded case into a hard error, which surfaced as autorip "could
not read this disc's key files" on discs whose AACS `.inf` files are
ICB-embedded — the keyserver was then never called.

## MAX_FILE_BYTES and the AACS MKB

`read_file()` caps a single unbounded read at `MAX_FILE_BYTES` (64 MiB) to
bound the allocation a crafted ICB `info_length`/extent length can force.
The one legitimately huge file on disc, the AACS `MKB_RO.inf`, is allocated
to a fixed ~128 MiB and zero-padded, so it is NOT read through this unbounded
path: `read_aacs_inputs_from_reader` instead reads a bounded prefix via
`read_file_prefix` and trims to the real record length, so it never trips
the cap (and never reads 100+ MiB of padding). A 0.31.0 regression read the
MKB through the unbounded path instead, so the cap rejected it,
`read_aacs_inputs` failed, and autorip reported "could not read this disc's
key files" without ever contacting the keyserver.

## merge_ranges() half-open semantics

`merge_ranges` takes HALF-OPEN `(start, count)` ranges, so `start + count`
is the EXCLUSIVE end and two ranges touch exactly when the next `start`
equals the previous end. A range that starts one sector LATER than that has
a genuine, untouched sector between them, and merging across it claims
coverage of a sector neither input ever described. That matters because the
merged output is a boundary, not a hint: `disc::merged_extents` feeds
`Disc::encrypted_content_ranges`, which decides which sectors the
decrypting source treats as content. A sector in a real gap belongs to no
title extent — it is nav/UDF/padding — and must stay outside the content
gate, not be folded into it.

## build_dir_icb_tagged() offsets

`build_dir_icb_tagged` builds a directory ICB with an explicit descriptor
tag, extended attribute length, RAW (unmasked) allocation-descriptor length
field and allocation position. ECMA-167 4/14.17 Extended File Entry (tag
266): L_EA at byte 208, L_AD at 212, allocation descriptors start at
216 + L_EA. ECMA-167 4/14.9 File Entry (tag 261): L_EA at 168, L_AD at 172,
allocation descriptors start at 176 + L_EA. The extended-attribute area is
filled with a recognisable non-zero pattern (0xA5) so that reading the
allocation descriptor from the wrong offset cannot silently produce a
usable value.

## Test fixture builders

`build_efe_long` builds a tag-266 ICB whose allocation descriptors are LONG
ADs (16 bytes: len(4) | lba(4) | part_ref(2) | impl_use(6)); the trailing
`part_ref`/`impl_use` bytes are left zero, which is what trips the old
8-byte-stride parser into reading a bogus zero-length terminator.

`build_entry_ads` builds a tag-261/266 entry with short ADs and every
disc-controlled field of the descriptor area exposed: `l_ea` (extended
attribute length — descriptors begin at the entry type's own base, 176 for
261 or 216 for 266, plus this; the attribute area is filled with 0xA5 so a
descriptor read from the wrong offset cannot silently produce a usable
value), `l_ad` (the DECLARED descriptor-area length, independent of how many
descriptors are actually written), and `extra` (descriptors written
immediately after the declared area, i.e. bytes the entry does not claim are
descriptors at all).

## VDS anchor shape-vs-content fallback tests

An anchor whose recorded VDS extent passes every SHAPE check (length >= 16
sectors, non-zero location, no address wrap) but holds no sequence must
still mount, by sweeping the customary location. Shape is a property of the
FIELD, not of what is there: a mastering tool that wrote the reserve
location, a stale anchor on a rewritten volume, or deliberate corruption all
produce this. Selecting the fallback on shape alone would leave the branch a
damaged disc actually takes with no recovery path at all — the sibling
Metadata File Location chain in the same function already treats its
recorded value as a CANDIDATE and falls back on outcome; this is that
principle applied one level up.

The recorded extent and the customary fallback are two SWEEPS, not two
locations: an anchor may point at the customary LBA and still declare a
window narrower than the fallback's. De-duplicating them on start LBA alone
then throws the wider sweep away and the sequence is never reached, even
though it is exactly where the fallback would have looked. The regression
test's fixture records `(LBA 32, 16 sectors)` — the minimum ECMA-167
3/10.2.1 permits, so it passes every shape check — while the Partition and
Logical Volume Descriptors sit at 50/51, inside the customary 32-sector
window but past the declared 16. No Terminating Descriptor lies in 32..48,
or both sweeps would stop at the same sector and the wider one would buy
nothing.

`conformant_meta_vol`'s extent length and position are given distinct byte
patterns, and distinct patterns from one another, so a descriptor byte
sourced from the wrong offset — or the two fields transposed — changes the
reported metadata partition rather than landing on a value that happens to
work. The length is an exact multiple of the 2048-byte logical sector, as a
real metadata partition is: its only observable is a sector COUNT, which
rounds up, so an off-by-one byte in the low half is visible only when the
true value sits exactly on the boundary.

## u32::MAX overflow guards in BufferedSectorReader

`prefetch` advances the read LBA with `start_lba + offset`. A UDF whose
metadata descriptor declares a partition near the top of the LBA space
(ECMA-167 §14.1: `extent_location` is an unconstrained Uint32) drove that
add past `u32::MAX` — an 'attempt to add with overflow' panic in debug
inside the public `Disc::scan`, and in release a wrap to a low LBA that
filled the sliding cache with a completely different region while
`cache_start` still claimed the high one.

`prefetch_ranges` walks each disc-derived `(start_lba, sector_count)` range
with `start + offset + i`. `collect_file_ranges` permits any LBA up to
`u32::MAX` (ECMA-167 §14.14.1 allocation descriptors are unconstrained
Uint32s) and nothing bounds `start + count`, so a range within one batch of
the top of the space overflowed: debug panic inside `Disc::scan`, or in
release a wrap that seeded the PERMANENT cache with this file's bytes keyed
at LBA 0 — every later single-sector read of those low LBAs (the AVDP/VDS
re-reads) then returned the wrong sector.

The sliding-cache hit test computed `cache_start + cache_sectors`. Once
`cache_start` is a disc-controlled LBA near `u32::MAX` (set by a batch read)
that add overflowed: debug panic inside `SectorSource::read_sectors`,
release wrap to a small value that silently disabled the cache.

## file_extents() refusal and continuation-cycle test rationale

`file_extents` builds the extent list a Blu-ray / HD-DVD TITLE is actually
ripped through (`disc/bluray.rs`, `disc/hddvd.rs`, `mux/resolve.rs`). Its
`(lba, sector_count)` tuple cannot say "this range is a hole", and both ways
of pretending otherwise are wrong: folding an unrecorded extent in makes the
mux read undefined sectors and splice whatever the media holds there into
the stream as content, while dropping it slides every later extent's byte
space (that list IS a byte-space map, `aacs::segment::clip_byte_to_lba`). So
at this signature the only truthful answer is refusal.

`icb_extents_exhausted_continuation_budget_is_an_error_not_a_short_list`
covers hostile input: a type-3 continuation descriptor whose continuation
block points back at itself (a cycle). The `MAX_AD_BLOCKS` bound must make
this TERMINATE rather than loop forever, and terminating by exhausting the
budget is not the same as reaching the end of the chain. It used to fall
through to `Ok(extents)` with the list silently cut short at whatever the
256th hop had collected; `disc/extract.rs` then zero-pads the missing tail
out to the entry's declared size, sets `complete = true` with
`bytes_unreadable = 0`, and renames off `.partial` — a mostly-zero file
delivered as a verified complete extraction. Exhausting the budget means "I
do not know the rest of this file", and the only honest answer is an error.

## Embedded-data and zero-length-hole test fixtures

AD type 3 is EMBEDDED data, not a descriptor list. RED BEFORE GREEN: with
the old `_ => 8` fallback, `read_icb_extents` returned
`Ok([IcbExtent { lba: 999, len: 2048 }])` — an extent decoded out of the
file's own CONTENT bytes. A rip would then read sector 999, which holds
something else entirely, and emit it as this title's stream at rc=0. That is
the failure the refusal exists to prevent, so the
`icb_extents_embedded_data_is_refused_not_decoded_as_descriptors` fixture
deliberately makes the content decode as a PLAUSIBLE extent rather than as
garbage: garbage would have been caught by the terminator check anyway, and
would prove nothing.

`file_extents` refuses a file whose extent list contains an unrecorded hole,
because reading one splices undefined sectors into the rip and dropping one
slides every later extent's byte space. Neither is true of a hole with
LENGTH ZERO: it displaces nothing, every later extent sits at the same
offset with or without it, and the callers' own `sectors > 0 && lba > 0`
filter already discards it. Refusing on `!recorded` alone therefore dropped
whole titles off discs that ripped correctly before — the reverse of the
defect the refusal exists for, and just as silent, since the title simply
vanishes. `file_extents_accepts_a_zero_length_unrecorded_extent` covers this.

## build_efe_ext() extended allocation descriptor layout

`build_efe_ext` builds an Extended File Entry (tag 266) ICB whose allocation
descriptors are EXTENDED ADs (ECMA-167 §14.14.3, 20 bytes each):
`ExtentLength(4) | RecordedLength(4) | InformationLength(4) | ExtentLocation
lb_addr { logicalBlockNumber(4) | partitionRef(2) } | impl_use(2)`. The
30-bit length + 2-bit type live in ExtentLength (offset +0); the logical
block number lives in ExtentLocation at offset +12. It sets ICB Tag flags
(abs offset 34) low bits to 2 = Extended AD so the parser must select the
20-byte stride and read the LBA from off+12, not off+4.

## References

- ECMA-167: Volume and File Structure of Write-Once and Rewritable Media
- UDF 2.50 (OSTA): Universal Disk Format Specification
- BD-ROM Part 3: Blu-ray Disc Read-Only Format, File System Specifications

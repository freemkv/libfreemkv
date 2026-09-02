# `dirimage` — `dir://` as an image-level source

`dirimage` synthesizes a real, minimal, valid UDF 1.02 volume over a host
folder (a DVD `VIDEO_TS/` or Blu-ray `BDMV/` extracted by a MakeMKV-style
backup). Nothing is emulated:

* **Metadata sectors** (anchors, the volume descriptor sequences, the File
  Set Descriptor, every File Entry, every directory's FID list) are encoded
  into RAM by `encode` — a few MiB even for a large Blu-ray.
* **Data sectors** are not materialized at all. Each one maps to a byte
  range of a real file, read on demand.

So `udf::read_filesystem` parses this image by exactly the same code path it
parses a real disc with, and every consumer above it is unchanged. The cost
is that a single-partition synthetic volume never exercises the UDF 2.50
Metadata Partition path (`udf.rs:946-991`) that every real BD-ROM uses —
this module's tests do not cover that block and must not be read as if they
did.

## What this module deliberately does NOT do

* **3D / SSIF** — rejected up front (`Error::DirImageSsifUnsupported`). An
  SSIF aliases the same sectors as its base and dependent `.m2ts`; the
  planner allocates disjoint extents, so a 3D folder would produce silently
  wrong output.
* **HD-DVD `HVDVD_TS/`** — no title enumerator constraint is modelled.
* **Encrypted folders** — a folder whose content is still AACS-scrambled is
  rejected by the caller-side probe, not decrypted here.

## `layout.rs` — rationale detail

Full detail for the comments trimmed in `src/dirimage/layout.rs`, in file order.

**Module overview.** Two phases, kept apart because they fail for different
reasons: walk rejects what the image model cannot represent (3D SSIF, an
unrecognized tree, a case collision inside one directory); assign places
metadata first (File Set Descriptor, one File Entry per node, then the
directory FID lists), then file data. Data placement is the part with real
constraints. For a Blu-ray there are none — `.mpls`/`.clpi` address clips by
name, never by LBA — so files are packed sequentially. For a DVD the IFOs
record VOB positions as sector offsets from the IFO's own start, and `ifo.rs`
re-derives every title extent from them, so the placement must reproduce
those offsets exactly or the rip reads the wrong sectors (`place_video_ts`).

**`DATA_FLOOR`.** Well clear of the volume-space descriptors, and — the
load-bearing part — far from zero: `disc/bluray.rs:137` drops any clip extent
whose LBA is 0, so a file that landed at block 0 would vanish from the title
with no error.

**`MAX_AD_BYTES`.** The AD length field is 30 bits (the top two are the
ECMA-167 4/14.14.1.1 extent type), so 0x3FFF_FFFF is the arithmetic ceiling —
but a non-final extent must be a whole number of blocks, so the usable
ceiling is the largest multiple of 2048 below it. Files larger than this are
split across several ADs; `udf.rs` reads them back as a multi-extent file,
the same shape a dual-layer disc produces.

**`MAX_DEPTH`.** Matches `udf.rs`'s `MAX_DIR_DEPTH`: anything deeper would be
recorded but never descended into, so the files under it would be invisible
to every consumer. Refuse instead of silently dropping.

**`MAX_CS0_NAME_BYTES`.** The FID's name-length field is one byte, so 255 is
the ceiling; 254 leaves the encoder no way to produce a value that wraps to
zero.

**`MAX_SUBDIRS`.** A directory File Entry's link count is `u16` and counts
one per child directory plus one for its own entry in the parent, so the
last usable value is `u16::MAX - 1`. Lowered under `cfg(test)` ONLY so the
guard can actually be executed: building a folder with 65534 subdirectories
to reach the real cap is not a test anyone can run, so an earlier test
asserted arithmetic about the constant instead and would have passed with
the guard deleted. The production value is unchanged.

**Module-scope link-count assert.** Must live at module scope, not inside
`mod tests`: it previously sat inside `#[cfg(test)] mod tests` while also
carrying its own `#[cfg(not(test))]`, so it was compiled in NO configuration
and could never fire — exactly the dead gate the test above it was written
to replace.

**`MAX_IMAGE_SECTORS`.** A DVD title set records where its VOBS begins as an
offset in its own IFO, read verbatim out of a file in the folder. A
regenerated `.BUP`, a tool that rewrote an IFO, or a hand-assembled folder
can therefore name an offset far beyond the content — and the planner
honours it, because honouring it is what makes a real backup readable.
Without a ceiling the image grows to wherever that offset points: a `u32`
sector count reaches ~8.8 TB, and writing one to an `iso://` destination
would fill a disk with zeros before anything noticed. 128 GiB clears the
largest real medium (BD-100) with room to spare, so a genuine disc folder
never meets it.

**`MAX_META_BYTES`.** Every node costs a 2 KiB File Entry sector held for the
life of the image, so the entry cap alone permits ~205 MB of metadata for
content of no size at all — and the mux holds two of these at once while
probing. The module documents a budget of "a few MiB even for a large
Blu-ray"; this is what enforces it rather than merely asserting it.

**`is_excluded`.** Dot-files are host artefacts, never disc content: macOS
sprays `.DS_Store` and `._*` resource forks through any folder a Finder
window has touched, and including them would put files on the "disc" that
were never on the disc. `.partial` is freemkv's own in-flight extraction
suffix (see `disc/extract.rs`) — picking one up would mean planning an
extent over a file that is still being written.

**`read_head`.** Reads the first `n` bytes of an IFO so its placement
offsets can be resolved. Errors propagate: returning an empty buffer instead
would send every offset through `unwrap_or(0)`, recording NO placement
constraint, and a VOB placed without its constraint yields an image that
reads at the wrong offset with nothing reported. An IFO that cannot be read
is a folder that cannot be planned.

**`place_video_ts`.** DVD-Video records VOB positions INSIDE the IFOs, as
sector offsets from the IFO file's own first sector: `VIDEO_TS.IFO` + 0xC0
(`VMGM_VOBS`) → `VIDEO_TS.VOB`; `VTS_nn_0.IFO` + 0xC0 (`vtsm_vobs`) →
`VTS_nn_0.VOB` (the VTS menu); `VTS_nn_0.IFO` + 0xC4 (`vtstt_vobs`) →
`VTS_nn_1.VOB` (the title stream). `ifo.rs:554-556` re-derives every title
extent as `file_start_lba(IFO) + vtstt_vobs + cell.first_sector`, so only the
third of those is load-bearing for freemkv itself — but the other two are
load-bearing for DVD PLAYERS, which read the menus freemkv ignores. All
three are honoured. `VTS_nn_1.VOB … VTS_nn_9.VOB` are one logical stream
split at the 1 GB file-size limit, and the cell sector addresses run
continuously across the split, so they are placed back-to-back with no gap;
laying the group out in its canonical on-disc order gives exactly that for
free, so the constraint becomes a CHECK, not a search. The check can fail —
a regenerated `.BUP`, a tool that rewrote an IFO, a folder assembled by hand
— and when it does the required position lies below the end of the file
that must precede it. There is no placement that satisfies it, so it is a
typed error naming the file rather than a silent misplacement.

**`reject_ssif`.** `disc/bluray.rs:124-130` probes
`/BDMV/STREAM/SSIF/{clip}.ssif` and sets `is_3d` the moment one resolves —
unconditionally, before any capability check. On a real 3D disc the `.ssif`
and the base/dependent `.m2ts` files ALIAS the same sectors (the SSIF
interleave IS the two m2ts streams), which this planner cannot express: it
would allocate three disjoint copies, so the clip's extents and the m2ts's
extents would disagree and the rip would read the wrong bytes at exit 0.
Reject the folder instead.

**`plan` — capacity, and what it changes.** `total_sectors` becomes the
`Disc`'s `capacity_sectors` / `capacity_bytes`, and `capacity_bytes` is not
inert: `canonical_title_order`'s oversize gate (`disc/mod.rs:2039-2040`, body
`:2209-2210`) demotes any title whose `size_bytes` exceeds it, which decides
which title sorts first and therefore what `-t 1` selects. The capacity
reported here is **the synthesized image's own size** — what an ISO built
from this folder would report — and nothing else. It is NOT padded up to a
media tier (BD25/BD50/…): that was considered and does not work, since a
folder does not record the tier of the disc it came from, so a 22 GB folder
off a BD50 would pad to BD25 and diverge anyway, and the padding would
inflate any pre-sized output written from the image. The consequence, stated
plainly: **for a folder that is missing files the source disc had — a
selective MakeMKV backup being the normal case — the capacity is smaller
than the source disc's, the oversize gate is therefore stricter, and a
borderline "play-all" composite title that the ISO kept can be demoted here.
`-t 1` can select a different title from `dir://FOLDER` than from an
`iso://` of the same disc.** For a complete folder the two agree, because
the capacities agree. This is chosen over the alternatives because it is the
only one that is self-consistent: `dir://X` and an `iso://` built from `X`
describe the same image and must answer the same way.

**Tests.**
* `a_file_past_the_ad_ceiling_splits_on_a_block_boundary` — a file above the
  30-bit AD ceiling must split into MULTIPLE descriptors, and every
  non-final one must be a whole number of blocks, or the next extent's bytes
  start mid-sector and the file reassembles wrong. Kills a mutant that emits
  one oversized AD (whose length would collide with the extent-TYPE bits
  `udf.rs:642` reads).
* `two_names_the_reader_cannot_tell_apart_are_refused` — audit finding: the
  uniqueness check compared raw host names, while `parse_udf_name` trims
  whitespace and drops code units `char::from_u32` rejects. So
  " 00000.m2ts" and "00000.m2ts" were two distinct entries at plan time and
  ONE name at read time; `find`/`read_file` take the first match, so a title
  resolved to the wrong file's extents and muxed the wrong bytes at exit 0.
  Calls `plan` on a real folder and fails if the round-trip key is reverted
  to comparing host names.
* `an_over_long_name_is_refused_by_the_planner` — audit finding: the length
  was narrowed with `as u8`, so a 255-byte ASCII name (POSIX NAME_MAX, legal
  on ext4/APFS/NTFS) encoded to 256 bytes with the CS0 compression byte and
  wrote a length of ZERO, making every later entry in that directory read
  from the wrong offset. An earlier version of this test asserted arithmetic
  about the constants and never called `plan`, so it would have passed with
  the guard deleted.
* `the_subdir_cap_refuses_a_folder_with_too_many_subdirectories` — pins the
  arithmetic relationship the guard relies on and executes the guard rather
  than restating the constant: link count (child dirs + 1) is 16 bits, so
  exceeding it wraps and the image lies about its own directory structure.
  Creating 65,535 directories in a test is not reasonable, so this does NOT
  exercise `walk`.

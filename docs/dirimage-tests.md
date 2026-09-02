# Rationale for `src/dirimage/tests.rs`

Longer-form notes for individual tests, referenced from `// See docs/dirimage-tests.md`
pointers in the source so the inline comments stay within the comment-guard caps.

## `Scratch::unique_path`

The path a scratch dir takes is kept separate from directory creation so its
uniqueness is testable without a syscall between draws. A monotonic counter,
NOT a timestamp, is what guarantees it: `SystemTime::now()` resolves to only
a MICROSECOND on macOS, so two of the seven parallel tests that share the
"bdmv" tag routinely read the same value within one microsecond, collide on
the same directory, and the first to finish `remove_dir_all`s it out from
under the others' reads — an intermittent `read_sectors` ENOENT. The counter
is unique regardless of clock granularity; pairing it with the pid keeps it
unique across test-binary processes.

## `scratch_paths_are_unique_even_at_clock_resolution`

Every `Scratch` must own a DISTINCT directory. Seven tests share the "bdmv"
tag and run in parallel; if two land the same path, the first to drop
`remove_dir_all`s it out from under the other's reads, which surfaced as an
intermittent `read_sectors` failure. Regression: with the old
`SystemTime::now()` name this is RED, because macOS's clock advances only
per microsecond, so a tight loop of pure draws — no `create_dir_all` syscall
between them to nudge the clock, mirroring the real cross-thread collision —
hands back the same value many times over. The counter-based name is unique
regardless of clock granularity.

## `a_directory_spanning_many_blocks_reads_back_whole`

A directory with enough children that its FID list spans several 2048-byte
blocks, read back through the production parser.

This is the case the block-alignment question turns on: FIDs are packed
contiguously and a descriptor may straddle a block boundary, because
`read_directory` (`udf.rs:1312`) walks the directory extent as one flat byte
run and BREAKS at the first non-257 tag. Padding each block would truncate
the directory at the first pad. 200 entries also exceeds the 16-handle LRU
several times over, so it exercises handle eviction on the read path.

## `a_file_below_video_ts_is_placed_not_just_declared`

A file inside a SUBDIRECTORY of `VIDEO_TS` must still have its data placed.

Audit finding. The DVD branch places only the files directly inside
`VIDEO_TS`, because only those carry the IFO-relative constraints — and the
follow-up loop skipped that directory entirely, so anything one level deeper
got a File Entry declaring the file's real size with no extents behind it.
It appeared in the tree at full length and read back as nothing, at exit 0.
The identical folder under `BDMV/` was always placed correctly, which is
what made the gap easy to miss.

## `a_vob_offset_past_the_image_cap_is_refused`

A VOBS offset far past the content must be REFUSED, not honoured.

Audit finding. The planner honours a title set's declared VOBS offset
because honouring it is what makes a real backup readable, and nothing
bounded it: a regenerated `.BUP`, a rewritten IFO or a hand-assembled folder
naming a huge offset grew the image to wherever it pointed. A `u32` sector
count reaches roughly 8.8 TB, and writing that to an `iso://` destination
would fill a disk with zeros before anything noticed.

The companion (`an_oversized_vob_offset_leaves_a_gap_rather_than_failing`)
pins that a MODEST oversize is still honoured as a gap, so this cap refuses
only what it must.

## `playable_bdmv`

A BD folder that really enumerates a title, so the AACS content probe has
an extent to sample.

The `.m2ts` is built as real 192-byte BD source packets, because that is
what the probe judges: byte 0 carries the AACS Copy Permission Indicator in
its top two bits, and byte 4 is the MPEG-TS sync. `scrambled` sets the CPI
and withholds the sync — "flagged and not structurally clean", which is
exactly `aacs_unit_needs_decrypt`. An all-zero payload would prove nothing
either way: `is_clean_ts` skips zero payloads as padding.

## `both_doors_agree_on_a_clear_folder_that_kept_its_aacs_directory`

The two doors must AGREE. Same folder, same verdict, same extents.

Stated as a differential rather than a bare `is_ok()` so it cannot go
vacuous: if the fixture ever stops producing an AACS state, a one-sided
`Ok` assertion would still pass while guarding nothing, whereas "both doors
see the same title" is the invariant the shared function actually exists to
hold.

## `a_scrambled_folder_is_refused_through_the_dir_url_door_too`

The other verdict, through the same door: a folder whose content units are
really scrambled is refused with the TYPED code, not muxed into garbage.

This is the load-bearing half. `E_DIR_IMAGE_ENCRYPTED` is produced at
exactly one site in the crate (`session::apply_folder_encryption_verdict`),
reachable from here only through the `is_folder` argument — so this test
cannot pass if that argument is dropped, whatever else changes.

## `write_and_mount_externally`

Write a synthesized image to a real file and ask the OS to mount it.

This is the only check here that is not circular: `read_filesystem` shares
every assumption with the encoder, an operating system's UDF driver shares
none of them. Ignored by default because it shells out to `hdiutil` and
needs a host that can attach an image; run with
`cargo test -- --ignored write_and_mount_externally --nocapture`.

## `dvd_placement_invariant_on_a_real_folder`

Diagnostic (opt-in): verify the DVD placement invariant for every title set
in a REAL folder. `ifo.rs` derives a title's extents as
`file_start_lba(VTS_nn_0.IFO) + vtstt_vobs`, so that sum must land exactly on
`VTS_nn_1.VOB` or the mux reads the wrong sectors for that title.

Run with: `FMKV_DVD_FOLDER=/path/to/tree cargo test --lib
dvd_placement_invariant_on_a_real_folder -- --ignored --nocapture`

## `a_multi_clip_playlist_produces_feed_spans_that_tile_the_feed`

`Clip::feed_span` is the INPUT to the whole provenance feature — it is what
tells the muxer which clip a byte offset belongs to — and it is produced in
exactly one place, `disc/bluray.rs`. Every `SeamPlan` test synthesizes spans
by hand, so the consumer is thoroughly tested against fixtures written from
the same assumptions as the producer, and nothing checks the producer at all.

That is the shape of the original defect: the placement logic was tested and
correct, and the thing feeding it was wrong. `SeamPlan` only trusts spans
that TILE the feed contiguously from zero, so this asserts exactly that,
against the real scanner reading a real synthesized filesystem.

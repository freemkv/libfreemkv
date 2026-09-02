# DVD Title Scan (src/disc/dvd.rs) — Internal Notes

Rationale, regression history, and test-fixture layout notes that were
originally inline comments/doc-comments on private items in
`src/disc/dvd.rs`, relocated here to satisfy the comment-guard's internal-
comment cap. Each pointed-to site in the source carries a short `// See
docs/dvd.md — <topic>` line.

## `Disc::scan_dvd_titles` halt handling

Cancellation: `halt` is polled before the IFO tree is read, and an IFO read
that fails with `Error::Halted` — how a live drive reports a Stop, since
`Drive::checked_exec` fails every command once its flag is set — is
propagated rather than swallowed. Every other IFO failure keeps its
best-effort `Ok(vec![])`.

It has to be an error and not an empty title list, for the same reason
spelled out on `Disc::scan_hddvd_titles`: a cancelled enumeration that
returned `Ok` would be indistinguishable from a disc that genuinely holds
fewer titles. This one was the worst of the three enumerators — a bare
`Err(_) => return Vec::new()` turned an operator Stop into ZERO titles at
rc=0, a disc reported as carrying no video at all.

`scan_dvd_titles` returns the scanned titles plus, when the disc's own
First-Play navigation deterministically dispatches to a title, that title's
`playlist_id` — the nav-resolved main feature. The caller feeds it into
`sort_titles_by_main_feature` as the DVD `nav_feature`, the same seam the BD
path uses. It is `None` (existing ranking heuristics stand) whenever the
navigation is menu-only, malformed, or does not converge.

## DefaultDuration / cadence TODO

`measured_cicp` TODO(spec): populate `top_field_first` + measured colour
from `Mpeg2Parser` once tracks are built after frame parsing — needs a
`CodecParser`->title channel like `codec_private`. Until wired, `None`
falls back to TFF / PAL-NTSC guess.

DefaultDuration TODO(spec): comes from IFO `frame_rate`, so a
soft-telecined 23.976-in-29.97 DVD reports 29.97 instead of true film
rate. Cadence is detectable via `nb_fields` (2:3 pulldown); needs the same
channel as `top_field_first` above.

## Test fixture builders: `build_vmg` / `build_vts`

`build_vmg` builds a VIDEO_TS.IFO: magic "DVDVIDEO-VMG"@0, TT_SRPT sector
ptr@0xC4. TT_SRPT lives at `tt_srpt_sector*2048`: `num_titles(u16)`@0, then
12-byte entries from +8. Each entry: `num_chapters(u16)`@+2, `vts_number`@+6,
`vts_title_num`@+7.

`build_vts` builds a VTS_XX_0.IFO. Layout per ifo.rs:
- magic "DVDVIDEO-VTS"@0
- `vtstt_vobs` (Title VOBS start sector, u32 BE)@0xC4 — the production
  `vob_start_sector` the cell sectors are relative to. (0xC0 is
  `vtsm_vobs`, the menu VOBS, which the scan must NOT use.)
- VTS_PGCIT sector ptr(u32 BE)@0xCC
- video attr byte@0x200
- `num_audio(u16 BE)`@0x202, audio blocks (8B) @0x204
- `num_subs(u16 BE)`@0x254, subtitle blocks (6B) @0x256

PGCIT (at `pgcit_sector*2048`): `num_pgcs(u16)`@0, PGC info entries (8B)
from +8 with PGC byte offset(u32 BE)@+4. PGC: `nr_programs`@0x02,
`nr_cells`@0x03, BCD time@0x04, `pgm_map` ptr@0xE6, `cell_playback` ptr@0xE8
(both u16 BE rel to PGC start).

## `HaltingReader` test double

A `SectorSource` that fails every read at or above `halt_at` with
`Error::Halted` — how a LIVE DRIVE behaves once the operator presses Stop:
`Drive::checked_exec` fails every SCSI command with `Halted` from then on,
and `Drive::read` deliberately preserves the variant. Reads below the
threshold still succeed, so the scan gets far enough to have something to
truncate.

## Test: `halted_ifo_read_is_not_reported_as_a_shorter_disc`

A Stop on a LIVE DRIVE never touches `ScanOptions::halt`: `Drive` has its
own flag and `checked_exec` fails every SCSI command with `Error::Halted`
once it is set. The DVD enumerator must not swallow that into a
successful scan.

This was the worst of the three enumerators. RED BEFORE GREEN, two
distinct swallows, both measured with the fix reverted:
- `ifo::parse_vmg` treats a failed title set as a placeholder entry and
  continues, so a cancel landing on VTS_02's IFO returned
  `Ok([VTS_01_1.VOB])` — one title from a two-title disc.
- `scan_dvd_titles`'s `Err(_) => return Vec::new()` turned a cancel landing
  on VIDEO_TS.IFO itself into ZERO titles at rc=0 — a disc reported as
  holding no video at all.

Both are indistinguishable from a real disc, and both are now
`Err(Error::Halted)`.

Fixture: two title sets, VTS_01's IFO at `PART_START+6000`, VTS_02's at
`PART_START+7000`. Both ICBs sit far below, so filesystem metadata
resolves fine and only the second title set's CONTENT read is cancelled —
the truncation case.

## Test: `scan_dvd_titles_uses_title_vobs_not_menu_vobs`

Regression (first-play menu prepended to the feature): `vob_start` must
come from the **Title** VOBS pointer `vtstt_vobs` (VTS_IFO 0xC4), NOT the
**menu** VOBS pointer `vtsm_vobs` (0xC0). On discs with a per-title menu —
e.g. the Universal "the parental level has been set, press yes" first-play
still — `vtsm_vobs` points at that menu VOB, which sits just before the
title VOB. Cell `first_sector` values are relative to `vtstt_vobs`; reading
0xC0 prepended the menu and shifted every extent back by
`vtstt_vobs - vtsm_vobs`, so the rip opened on the parental prompt instead
of the movie (one real NTSC R1 disc: vtsm=44, vtstt=3640).

Here `build_vts` stamps `vtstt_vobs = 3640` (0xC4); the test additionally
stamps a *different* `vtsm_vobs = 44` (0xC0). The extent must resolve
from 3640.

## Test: `scan_dvd_titles_extent_is_absolute_three_term_sum`

ABSOLUTE-REBASE regression (real-disc menu-at-start fix): `ifo::parse_vts`
now sets `vob_start_sector = file_start_lba(IFO) + vtstt_vobs`, so an
extent's `start_lba` must equal the sum of THREE independent terms — the
IFO file's absolute on-disc LBA, the IFO-relative `vtstt_vobs` (0xC4), and
the cell's `first_sector` — none of which may be dropped. The earlier code
used the bare relative `vtstt_vobs`, placing every extent `ifo_lba`
sectors too early (the rip opened in the VMGI/menu region before drifting
into the movie). The other tests fold two of the three terms together
(zero cell offset, or a single combined expectation); this one keeps all
three distinct and non-overlapping so a regression to ANY two-term
combination is caught.

## Test: `scan_dvd_titles_lpcm_routes_to_a0_pid_range`

LPCM SCAN ROUTING (audit §2 / §5 #6): the 0xA0..=0xA7 PID range was never
exercised in the dvd.rs scan. An LPCM stream (coding_mode 4) at audio
position 1 must get `sub_stream_id 0xA1` → PID `0xBDA1` via
`dvd_audio_pid`, distinct from the AC-3 `0xBD80` space, with its real
channel count preserved.

## Test: `scan_dvd_titles_multiple_vobsub_tracks_distinct_pids`

MULTI-VOBSUB SCAN (audit §2 / §5 #6): the single-subtitle test covered one
track; a multi-subtitle VTS must emit one `Stream::Subtitle` per entry
with distinct PIDs (`0x20 + ordinal`) and per-language tags, all sharing
the PGC palette `codec_data`.

## `stamp_first_play_jumptt` test helper

Stamp a First-Play PGC into a VMG whose pre-command list is a single
unconditional `JumpTT ttn`, so `dvdnav::resolve_main_title` resolves that
title through TT_SRPT. Placed in sector 0, clear of the magic / FP_PGC /
TT_SRPT pointers `build_vmg` already writes.

## Test: `scan_dvd_titles_nav_promotes_first_play_target`

The DVD First-Play nav promotion branch (issue #40): when
`resolve_main_title` returns a `(vtsn, vts_ttn)` target, `scan_dvd_titles`
must map it — via the 1-based `vts_ttn == vts_title_idx + 1` join within
the matching `vts_number` — to the running `title_number`, and surface it
as the `nav_feature`. TT_SRPT here maps title 2 → (VTS 2, title-in-set 1);
the First-Play unconditionally `JumpTT 2`, so the promoted feature must be
the SECOND scanned title (VTS_02 → title_number 2). An off-by-one on the
join or a `vtsn`/`vts_ttn` field swap would promote the wrong title or
none.

## Test: `scan_dvd_titles_drops_leading_scene_index_cell`

End-to-end bug-4 fix: a feature PGC that opens with a leading interleaved-
angle sub-block cell (category 0x90 = in-block cell of an angle block)
must have that cell DROPPED from the muxed extents, so the rip starts at
the real feature. Chapters shift earlier by the dropped duration.

## Test: `scan_dvd_titles_mp2_audio_pid_fallback_is_additive`

Audio PID fallback (dvd.rs `Disc::scan_dvd_titles`): when an audio stream
has no on-wire private_stream_1 sub-stream id — MP1/MP2 audio, per
`ifo::assign_audio_sub_stream_ids` — the PID falls back to `0xBD00 + i`
where `i` is the stream's positional index in the IFO audio-attribute
table. Two MPEG-audio (coding_mode 2) streams must land on two DISTINCT,
correctly-offset PIDs: 0xBD00 and 0xBD01. This pins the `+` (not `-`/`*`)
so the second stream doesn't collide with, or wrap under, the first.

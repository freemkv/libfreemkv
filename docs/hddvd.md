# HD-DVD title scanning (`src/disc/hddvd.rs`)

HD-DVD is a **tree-level peer** of DVD and Blu-ray (not a stream variant like
FMTS): its content lives in `HVDVD_TS/` as `.evo` clips — Enhanced VOB, an
MPEG **program** stream — each with a small `.map` timemap sidecar, navigated
by `.xpl`/`.ifo` playlists in `HVDVD_TS/` and `ADV_OBJ/`. Because it is a
different tree with a different playlist format, it gets its OWN scanner
(this file), a peer to `Disc::scan_bluray_titles` — the two-format design
rule: a genuinely different format is a new enumerator, not an extension
bolted into the BD path.

## Title composition (authoritative path)

Title composition is authoritative, from the Advanced-Content playlist. HD-DVD
ships a real player playlist at `ADV_OBJ/VPLST000.XPL` (DVD-Forum
`HDDVDVideo/Playlist` XML). The scanner parses it (with a real XML parser,
`roxmltree`) into one `DiscTitle` per `<Title>`: its `<PrimaryAudioVideoClip>`
clips in playback order (each an EVO, referenced via its `.MAP` sidecar), the
`titleDuration`, the `displayName`, and the `<ChapterList>`. A layer-break
split (`FEATURE_1` + `FEATURE_2`, or `feature`/`feature_Divide`) is composed
into ONE title with the two parts as clips, carrying each clip's title-time
in/out points (45 kHz ticks) so a seamless join can be spliced onto one
timeline. Container is `ContentFormat::MpegPs`, so the existing PS mux path
handles it. Per-clip streams are enumerated by demuxing the clip head and
building one `Stream` per distinct elementary stream (video + DD+ audio),
codec sniffed from the ES bytes.

## Fallback path

When no playlist is present (or it fails to parse), the scanner falls back to
the older clip-name heuristic: parse the `HVA*.VTI` clip table, join the
`feature*`-named clips into one title, and emit every other clip on its own.

## Known gaps

Not parsed yet: subtitles (8-bit RLC on `0xBD` sub `0x20..=0x3F`) and per-track
audio languages (the XPL carries `<Audio description=...>` but they are not yet
wired onto the streams). Extents and size are real (the ripper images the
clips).

## VTI parsing

`parse_vti_clip_order` parses the `ADVANCED-VTS` VTI clip-name table. The
table is a run of `VTI_CLIP_ENTRY_STRIDE`-spaced records, each carrying a
NUL-terminated `<name>.EVO`. Rather than trust the (imprecise) header pointer,
it collects every NUL-terminated `*.EVO` name and keeps the largest group
sharing one residue modulo the stride — the clip table — in offset order.
Returns empty for a non-VTI blob or one with no recognizable table.

## Codec sniffing

`sniff_video_codec` reads MPEG / Annex-B start codes: `00 00 01 B3` → MPEG-2
(sequence_header), `00 00 01 0F` → VC-1 (BD/HD-DVD sequence-header BDU),
`00 00 01 [x7]` H.264 SPS NAL (type 7, forbidden_zero_bit clear) → H.264.
Returns `None` when no recognizable start code is present. The scan prefers
the unambiguous MPEG-2 / VC-1 sequence headers; H.264 is inferred from an SPS
NAL so a stray slice/picture code can't be mistaken for a different codec.

`sniff_audio_codec` recognizes only Dolby Digital Plus (E-AC-3) today, via its
`0x0B77` syncword — what real retail HD-DVD titles carry on sub-ids
`0xC0..=0xC7`. Returns `None` for an unrecognized sample so the caller drops
the stream rather than mislabeling it.

## probe_evo_streams

Demuxes the head of an `.evo` clip (through the disc's `SectorSource`) and
builds one `Stream` per distinct elementary stream found: the video track
(mapped to the canonical `DVD_VIDEO_PID` for plain 0xE0-0xEF video, or the
extended-stream-id PID for HD-DVD VC-1) and every DD+ audio sub-stream
(mapped via `dvd_audio_pid`). Codec is sniffed from the demuxed ES bytes.

Mirrors the stream construction in `Disc::scan_dvd_titles`; resolution /
language / channels use sane HD-DVD defaults (the muxer reads the true pixel
dimensions from the H.264 SPS, and E-AC-3 channel counts are not decoded
here). Returns an empty vec when the clip cannot be read or carries no
recognizable stream (e.g. an AACS-encrypted clip probed as ciphertext).

Cancellation is checked before every chunk read and is the ONE condition that
returns `Err` rather than an empty vec: a probe cut short by a Stop has not
established that the clip carries no streams, and reporting it as if it had
would enumerate a stream-less title as a scanned fact. A read that fails for
any other reason keeps the existing best-effort behaviour.

## collect_es

VC-1 video rides extended-stream-id `0xFD` with `stream_id_extension` `0x55`.
HD audio (MLP/TrueHD) can also use `0xFD` with other extensions — routing
those to their own audio tracks is deferred (see the HD-DVD program-chain
follow-up); until then only the VC-1 extension is treated as video, so an
audio `0xFD` sub-stream can never mis-stamp the video PID.

## XPL amplification bounds

The XPL parser guards against a crafted `ADV_OBJ/VPLST000.XPL` amplifying a
small file into an enormous amount of work or memory. Four separate axes are
bounded, each independently, because they are separate attacks:

- **`MAX_XPL_DEPTH` (32)** bounds element NESTING. A real `VPLST000.XPL` is
  about six levels deep at its deepest
  (`Playlist`/`TitleSet`/`Title`/`PrimaryAudioVideoClip`/`Video`, or
  `.../ChapterList/Chapter`); 32 leaves a 5x margin for authoring tools that
  wrap extra `ApplicationSegment`/`ObjectMappingList` layers, while staying
  trivially stack-safe.

- **`MAX_XPL_TITLES` (512)** bounds how many `<Title>` elements one playlist
  may declare — BREADTH, separate from the depth guard: a flat playlist
  passes the depth check trivially and can still declare a title per 51
  bytes, so a 64 MiB `VPLST000.XPL` (exactly `udf::MAX_FILE_BYTES`) reaches
  ~1.3 million of them. This matters here and not on the Blu-ray side because
  of where the count comes from: `bluray.rs` learns its playlists from the
  MPLS files actually present in the UDF directory, so the medium bounds it —
  an XPL DECLARES its titles, so nothing does. The cost is drive time, not
  memory: `compose_xpl_titles` runs `probe_evo_streams` once per title, and
  each probe reads real sectors, so an uncapped count turns a `Disc::scan()`
  the operator expects to take seconds into tens of terabytes of optical
  reads on a disc that is only pretending to be large. The bound that matters
  is `cap x EVO_PROBE_SECTORS`, since `compose_xpl_titles` memoizes on the
  title's RESOLVED extent list, and distinct titles built from different
  combinations of the same clips have different lists, so each one misses the
  memo — at 4096 that product was 64 GiB of optical reads, the same worst
  case, and the same contradiction with "seconds", as the sibling
  `MAX_HDDVD_CLIPS` path. 512 keeps it at 8 GiB and remains far above any
  real disc (retail HD-DVDs carry tens of titles).

- **`MAX_HDDVD_CLIPS` (512)** caps `.evo` clips taken from the `HVDVD_TS/`
  directory listing. `MAX_XPL_TITLES` and `xpl_depth_within_limit` bound the
  `ADV_OBJ/*.XPL` path, but a crafted disc simply omits `/ADV_OBJ`; control
  then reaches the clip-name fallback in `Disc::scan_hddvd_titles`, which
  emits one title per directory entry and probes each — the same unbounded-
  probe cost, reached without the playlist. The directory is the only bound
  the medium supplies, and `udf::MAX_DIR_BYTES` (1 MiB) still leaves room for
  ~24,000 FIDs. Memoizing the probe (`EvoProbeCache`) collapses entries that
  resolve to the SAME extents, closing the many-names-one-File-Entry case —
  but not this one: a File Entry is a single sector, so an attacker can give
  every FID its own, each declaring its own extent, for ~48 MiB of image;
  every probe then misses the memo and costs a full `EVO_PROBE_SECTORS`
  (16 MiB) read, hundreds of GiB on a scan the operator expects to take
  seconds. The cap's job is to bound `cap x EVO_PROBE_SECTORS`; it was 4096
  (matching `MAX_XPL_TITLES`, which bounds a different, no-per-item-read-cost
  path) — 4096 x 16 MiB is 64 GiB, more than dual-layer HD-DVD media
  physically holds (reachable anyway, since distinct extent lists may overlap
  on the medium). 512 keeps the product at 8 GiB and still leaves a wide
  margin over any real disc (retail HD-DVDs carry TENS of `.evo` clips, ~10x
  the real-world maximum, the sizing convention `MAX_XPL_CHAPTERS_PER_TITLE`
  also uses against the 99-chapter authoring ceiling). Pinned by
  `the_clip_cap_and_probe_budget_bound_a_scans_worst_case_read_volume`, which
  asserts the PRODUCT rather than either constant, so raising one without
  re-examining the other fails.

- **`MAX_XPL_CLIPS_PER_TITLE` (256)** bounds how many
  `<PrimaryAudioVideoClip>` elements a SINGLE `<Title>` may contribute — the
  fourth amplification axis, the one the other three guards leave open. A
  64 MiB XPL holds ~1.7 million `<PrimaryAudioVideoClip src="A.EVO"/>`
  elements at ~38 bytes each, and the collector is `descendants()`, not
  `children()`, so a clip nested inside N ancestor `<Title>` elements is
  collected by EVERY one of them — the depth cap still permits 32 such
  ancestors (measured: a bare `<Title>` root nests 32 deep and still parses,
  because the guard counts the self-closing clip element as non-nesting).
  The product is ~1.7M x 32 ≈ 56 million heap-allocated `XplClip`s, each with
  its own `String`, and then as many `Clip`s again in `compose_xpl_titles`.
  Unlike the title cap, the cost here is MEMORY rather than drive time:
  `compose_xpl_titles` de-duplicates by `.evo` name before probing
  (`seen_evos`), so repeats cost no extra sector reads, but the `Vec`s are
  built and held regardless. 256 is far above any real disc — a feature is
  one clip, or two across a layer break; even seamless-branching/multi-angle
  is tens — and keeps the aggregate bounded at `MAX_XPL_TITLES` x 256 clips
  rather than the tens of millions above.

- **`MAX_XPL_CHAPTERS_PER_TITLE` (1024)** bounds `<Chapter>` elements per
  `<Title>` — the same `descendants()` amplification, on the second unbounded
  `collect()` in the same loop, bounded separately so capping clips doesn't
  simply move the attack next door. A chapter is a bare `f64` rather than a
  `String`-carrying struct so it's cheaper, but a
  `<Chapter titleTimeBegin="…"/>` element is also smaller, so the element
  count an XPL can reach is comparable. 1024 is far above any real disc: the
  DVD/HD-DVD authoring convention tops out at 99 chapters per title, a 10x
  margin.

## scan_hddvd_titles

Scans HD-DVD titles from the `HVDVD_TS/` `.evo` clips. The main feature is
authored as one or more `.evo` clips (a layer-break split —
`FEATURE_1`/`FEATURE_2` or `feature`/`feature_Divide`). The `HVA*.VTI`
navigation file names every clip in authored order; this parses it to
concatenate the feature clips into ONE title (so the largest-title pick gets
the whole movie, not just part 1), emitting every other clip as its own
title. Falls back to one title per clip when the VTI is absent or
unparseable, so a disc with no readable navigation still enumerates.
`chapters`/duration are left empty pending deeper VTI parsing.

Cancellation: `halt` is polled once per clip and again before every probe
chunk, and a read that fails with `Error::Halted` — how a live drive reports
a Stop, since `Drive::checked_exec` fails every command once its flag is set
— is propagated rather than swallowed. `Halted` is the only error this
returns; every other read failure keeps its best-effort behaviour. It has to
be an error and not a short title list: a cancelled enumeration that
returned `Ok` would be indistinguishable from a disc that genuinely holds
fewer titles, and the caller would cache and act on it.

## xpl_depth_within_limit

Rejects a disc-supplied XML blob whose element nesting exceeds
`MAX_XPL_DEPTH`, BEFORE it reaches the parser. `roxmltree` is
recursive-descent and its depth-10 limit applies only to ENTITY expansion, so
element nesting is unbounded: a few hundred KB of well-formed XML (far under
the read cap) overflows the thread stack. That is a process ABORT, not an
`Err` and not an unwind, so neither the `Document::parse` fallback nor
`catch_unwind` can contain it; the only fix is to not hand the document over
at all. A byte-size cap is deliberately NOT added: file size is already
bounded by the UDF read path, and a size cap does not close this class (a
180 KB document already aborts) — depth is the bound that matters.

This is a conservative scan, not a validator: it tracks `<name …>` /
`</name>` while skipping comments, CDATA, processing instructions and
declarations, and treats `<… />` as non-nesting. A miscount can only cost a
pathological document its fast path, and the failure mode is the existing
one — fall back to the clip-name heuristic.

## Test rationale

Detailed rationale for tests in `src/disc/hddvd.rs`'s `#[cfg(test)]` module,
kept here so the in-code doc can stay within the internal-comment cap.

**`scan_hddvd_feature_composition_saturates_absurd_disc_declared_sizes`** —
The SECOND size accumulator. `compose_xpl_titles` sums an XPL's clip sizes;
this one sums the composed FEATURE title's parts, and it reads the same
disc-declared `u64` Information Length from the same `clip_extents` map. A
split feature whose two parts each declare a size near `u64::MAX` overflows
it exactly as the XPL path did. Nothing reconciles the declared size against
the extents that back it — here each part declares `u64::MAX` while
occupying ten sectors — because the allocation descriptor's length field is
only 32 bits wide, so the disc can always claim more than it holds.
Mutation: restore `size_bytes += *size` in the feature-composition path and
this goes red (debug: attempt to add with overflow).

**`scan_hddvd_does_not_compose_a_feature_over_an_unrecorded_part`** — A split
feature (FEATURE_1 + FEATURE_2) where ONE part carries an unrecorded extent
must not be composed into a FEATURE title at all. Composing around the
unusable part is the dangerous outcome: the title would play through as if
it were the whole movie while silently missing that part's runtime, and its
size still counts it. The other clips remain available as their own titles
— refusing the composition is not the same as refusing the disc.

**`scan_hddvd_logs_an_unrecorded_feature_part_with_its_own_code`** — The
refusal above must also be ACCOUNTED, with the unrecorded extent's own code.
This warn site carried a hardcoded `code = 6017` literal — in the very file
whose sibling arm was changed to stop doing exactly that — and nothing
tested it, so neither the literal nor its absence broke anything. Refusing a
feature and saying nothing leaves the operator with a disc that scanned
"fine" and is quietly missing its main title. Mutations: deleting the
`tracing::warn!` (no event to find); restoring the literal `6017` still
passes here BY VALUE, which is the point — what is pinned is
`E_UDF_UNRECORDED_EXTENT`'s value reaching the log, so a renumbering that
the literal could not follow goes red; logging a neighbouring code (e.g.
`E_UDF_NO_USABLE_EXTENT`) goes red immediately.

**`scan_hddvd_does_not_compose_a_feature_over_a_part_with_no_usable_extent`**
— The `Ok`-but-empty twin of the test above, and the one that actually bites
on an ordinary disc: a feature part whose `file_extents` call SUCCEEDS and
yields nothing usable — here a zero-byte `FEATURE_2.EVO`, no crafted ICB
required — must refuse the composition too, and must say so in the log.
Before this, the empty-`Ok` clip entered neither `clip_extents` nor
`unusable`, so the `any(|n| unusable.contains(..))` guard missed it and the
`filter(|n| clip_extents.contains_key(..))` beside it quietly DELETED the
part: the scan composed a `FEATURE` title out of part one alone, still
advertising itself as the feature, at rc=0, with no diagnostic anywhere.
Half a movie presented as a whole one — strictly worse than the `Err` case
the guard was built for, and reachable without a hostile disc. Mutations
this catches, all of which the pre-fix code exhibited: dropping the
`extents.is_empty()` branch entirely (a `FEATURE` title appears with one
extent); inserting into `clip_extents` anyway (same); marking the clip
unusable but NOT logging (the log assertion fails — absence of a log is
itself the defect); logging the neighbouring `E_UDF_UNRECORDED_EXTENT`
instead of `E_UDF_NO_USABLE_EXTENT` (the code assertion fails, because a
zero-length file is not an authoring hole and triaging the two together
sends whoever reads it at the wrong population).

**`parse_xpl_titles_caps_a_playlist_declaring_absurdly_many_titles`** — A
disc-supplied playlist nested far deeper than any real one must be REFUSED
BEFORE the XML parser sees it. `roxmltree` is recursive-descent and its
depth-10 limit covers only ENTITY expansion, so unbounded element nesting
blows the thread stack — an ABORT, which no `catch_unwind` and no `Err`
fallback can contain. ~50k levels is a few hundred KB of well-formed XML,
far under the read cap. The correct outcome is the same as any other
unusable playlist: an empty `Vec`, so the caller falls back to the clip-name
heuristic. Separately, this test also exercises BREADTH, not depth: a flat
playlist passes the nesting guard trivially and can still declare a title
per ~51 bytes, so a 64 MiB XPL (exactly `udf::MAX_FILE_BYTES`) reaches ~1.3
million of them. The cost is drive time, not memory: `compose_xpl_titles`
runs `probe_evo_streams` once per title and each probe reads real sectors,
so an uncapped count turns a scan the operator expects to take seconds into
tens of terabytes of optical reads. `bluray.rs` is not exposed to this
because its playlist count comes from the MPLS files present in the UDF
directory; an XPL DECLARES its titles, so nothing bounds them. Mutation:
delete the `.take(MAX_XPL_TITLES)` and this goes red at the declared count.

**`nested_title_xpl`** — Builds a playlist that nests `nesting` `<Title>`
elements inside one another and puts `clips` self-closing
`<PrimaryAudioVideoClip>` elements at the innermost level, plus `chapters`
`<Chapter>` elements. `parse_xpl_titles` collects a title's clips with
`descendants()`, not `children()`, so EVERY one of the `nesting` ancestors
collects the SAME innermost clip list — the amplification this exercises.

**`parse_xpl_titles_caps_clips_per_title_against_descendant_amplification`**
— CLIPS PER TITLE is the axis the title cap, the depth cap and the
directory cap all leave open. `MAX_XPL_TITLES` bounds how many titles a
playlist may declare and `MAX_XPL_DEPTH` bounds how deeply it may nest, but
NEITHER bounds how many `<PrimaryAudioVideoClip>` elements a single title
collects. Worse, the collector is `descendants()`, so one clip nested inside
N ancestor `<Title>`s is collected N times over — the clip count MULTIPLIES
by the nesting the depth guard still permits. A 64 MiB XPL
(`udf::MAX_FILE_BYTES`) holds ~1.7M clip elements at ~38 bytes each; with
the 32 `<Title>` ancestors the depth cap still permits, that is ~56 million
heap-allocated `XplClip`s, and then as many `Clip`s again in
`compose_xpl_titles`. This test uses small numbers that exercise the same
multiplication without a 64 MiB fixture: pre-fix it collected 756 clips per
title, 18,900 across the 25 nested titles, from a document declaring only
756 clip elements — an exact 25x, the nesting count. Mutation: delete the
`clips.len() >= MAX_XPL_CLIPS_PER_TITLE` break and this goes red at the
unamplified per-title count.

**`parse_xpl_titles_caps_chapters_per_title`** — CHAPTERS PER TITLE is the
same `descendants()` amplification on the second unbounded `collect()` in
the same loop. Bounded separately so closing the clip axis does not simply
move the attack next door. Mutation: delete the
`.take(MAX_XPL_CHAPTERS_PER_TITLE)` and this goes red at the declared count.

**`parse_xpl_titles_keeps_every_clip_of_a_realistic_title`** — The control: a
realistic multi-clip title still resolves EVERY one of its clips and
chapters. `SYNTH_XPL`'s main movie is a layer-break split — two clips on one
timeline — plus two chapters; losing either to the cap would cost a genuine
disc half its feature. Mutation: set `MAX_XPL_CLIPS_PER_TITLE` to 1 and this
goes red.

**`parse_xpl_titles_accepts_real_world_nesting_depth`** — The depth guard
must not reject real discs. A genuine `VPLST000.XPL` is a handful of levels
deep; this is the same document the parsing test uses, plus self-closing
tags, a comment and a processing instruction that the pre-parse scanner has
to account for without inflating its depth count.

**`scan_hddvd_composes_titles_from_xpl_playlist`** — The authoritative path:
when a VPLST000.XPL is present, titles come from the playlist — the
layer-break split (FEATURE_1 + FEATURE_2) is composed into ONE title with
the real duration, name, chapters, and per-clip title-time offsets — not the
clip-name heuristic.

**`compose_xpl_titles_saturates_absurd_disc_declared_clip_sizes`** — A
title's `size_bytes` is a running sum of DISC-DECLARED clip sizes: the `u64`
UDF File Entry Information Length, which nothing cross-checks against the
extent list that file actually occupies. Two clips can therefore each
declare a size near `u64::MAX`. Summed with a plain `+=`, that overflows: a
panic in a debug build — a crash on untrusted disc content, which this
library must never do — and a wrap to a small garbage total in release,
which misreports the title's size while looking entirely normal. Mutation:
restore `size_bytes += *size` in `compose_xpl_titles` and this goes red
(debug: attempt to add with overflow).

**`compose_xpl_titles_dedups_repeated_clip_references_by_evo`** — A crafted
Advanced-Content playlist can name the SAME `.evo` in
`<PrimaryAudioVideoClip>` many times over (the XML has no fixed element
count, unlike MPLS's binary PlayItem count). Each repeat must NOT push
another copy of that clip's extent list onto the title — mirroring
bluray.rs's `first_ref = seen_clips.insert(...)` gate, which pushes a clip's
extents only the first time its id is seen. Here the analogous key is the
`.evo` filename (this file has no clip_id; `.evo` is what `clip_extents` is
keyed by). Without the gate, extents grows by the referenced clip's full
extent list on EVERY repetition — unbounded by the number of on-disc files,
bounded only by playlist size.

**`scan_hddvd_titles_drops_a_split_feature_whose_part_cannot_be_read`** — A
clip whose extents cannot be resolved must drop the SPLIT FEATURE. Catches
reverting the `Err(e) => { warn; unusable.insert(..) }` arm in
`Disc::scan_hddvd_titles` back to the bare `Err(_) => {}` it replaced. RED
BEFORE GREEN, and TWO earlier attempts at this test did NOT go red. The
first asserted "no title names the broken clip" — that passes either way,
because a clip that resolves to no extents is never inserted into
`clip_extents` and so yields no per-clip title regardless. The second fixed
that but shipped no `.vti`: `order` is built solely from
`parse_vti_clip_order` of the navigation file, so with no `.vti` it is
EMPTY, `feature` is empty, and no composed title is ever built — the
assertion held vacuously with the fix reverted. Hence the synthetic
`HVA00001.VTI` fixture: it is what makes the composer run at all. The
defect lives one level up from the per-clip titles, in the composed
feature: `unusable` is what tells the composer that a part is MISSING
rather than merely absent, and the old bare `Err(_) => {}` populated it for
nothing but an unrecorded extent. So a scratched sector under FEATURE_2's
ICB (`Error::DiscRead`) left FEATURE_1 composing a title named "FEATURE" by
itself — half a movie offered as the whole one.

**`scan_hddvd_titles_excludes_a_clip_with_zero_sectors`** — A clip whose
file has a zero-byte size (a degenerate/empty allocation: its ICB's
allocation descriptor has `data_len == 0`, the UDF AD-list terminator, so
`file_extents` yields no extent at all) must not produce a title. NOTE: this
exercises the *upstream* `data_len == 0` terminator path in
`crate::udf::UdfFs::file_extents`, not the `sectors > 0 && lba > 0` guard in
`scan_hddvd_titles` itself — with this fixture (`file_extents` never returns
a `(lba, 0)` tuple, and `PART_START` unconditionally makes every resolved
`lba` positive) that guard is unreachable in a divergent way; kept here as a
regression check on the zero-byte-file behavior in its own right.

**`scan_hddvd_titles_probes_once_for_entries_resolving_to_identical_extents`**
— THE THIRD AMPLIFICATION AXIS on HD-DVD title composition, past both
existing guards. `xpl_depth_within_limit` bounds XML NESTING;
`MAX_XPL_TITLES` + the `.evo` de-dup bound what an XPL can DECLARE. Both
live on the playlist path — and an attacker simply omits `/ADV_OBJ`, so
`read_adv_obj_xpl` returns `None` and neither guard is ever consulted.
Control then reaches the clip-name FALLBACK, which emits one title per
`.evo` DIRECTORY ENTRY and probes each. Nothing de-duplicates a FID's ICB
LBA, so every name in a 1 MiB directory (`udf::MAX_DIR_BYTES`, ~24,000 FIDs)
can point at ONE File Entry. Each resolves to the SAME extents and each
costs a full `EVO_PROBE_SECTORS` (16 MiB) probe: ~375 GiB of optical reads
from a directory that describes a single file. Memoizing the probe on the
resolved extent list collapses that to ONE probe, because all of those
names resolve to one extent list. Mutation: drop the memo lookup in
`EvoProbeCache::streams` (always probe) and this goes red at `ENTRIES`
probes.

**`the_clip_cap_and_probe_budget_bound_a_scans_worst_case_read_volume`** —
The clip cap and the per-probe read budget are ONE bound, not two, and only
their product is meaningful: `MAX_HDDVD_CLIPS` probes each cost up to
`EVO_PROBE_SECTORS`, so the pair fixes the worst-case drive time of a
`Disc::scan()`. `EvoProbeCache` does not help against a crafted disc here:
it keys on the RESOLVED extent list, and distinct-but-overlapping extent
lists all reading one physical region miss the memo every time while every
read succeeds. So the product is genuinely reachable, and it is the number
that has to stay within what a scan can absorb. 8 GiB is the ceiling
asserted here. It is far above any genuine disc — retail HD-DVDs carry tens
of `.evo` clips, i.e. well under 1 GiB of probing — and far below the 64 GiB
the pair used to permit, which exceeded the capacity of the dual-layer media
this format tops out at and took tens of minutes of optical reads. This is
a relationship, not a magic number: raising EITHER constant without
re-examining the other trips it.

**`scan_hddvd_titles_still_probes_each_distinct_clip`** — CONTROL:
memoization must not silently collapse DISTINCT clips. A legitimate disc
with several different `.evo` files still probes each one, and each title
keeps the streams of ITS OWN clip. Mutation: make `EvoProbeCache`'s key a
constant (ignore the extents) and this goes red — the junk clip inherits
the first clip's H.264 streams.

**`scan_hddvd_titles_caps_a_directory_declaring_absurdly_many_clips`** —
Memoization alone is NOT the whole fix. Collapsing identical extent lists
bounds the one-File-Entry attack, but distinct extent lists are only as
bounded as the File Entries an attacker cares to lay down — and a File
Entry is ONE sector. A 1 MiB directory's ~24,000 FIDs can each point at
their own 1-sector File Entry (a ~48 MiB image) declaring its own huge
extent, so every probe misses the memo and the amplification is back.
`MAX_HDDVD_CLIPS` is the bound that closes that. Mutation: drop the
`clips.len() < MAX_HDDVD_CLIPS` gate in the directory scan and this goes
red at `ENTRIES`.

## EvoProbeCache

`EvoProbeCache` memoizes `probe_evo_streams`, keyed on the RESOLVED extent
list. Probing is the expensive half of title composition: each pass reads up
to `EVO_PROBE_SECTORS` (16 MiB) off the medium. Both composition paths can
reach the same physical clip many times over — an XPL naming one `.evo` from
many `<Title>`s, or a directory whose FIDs all point at one File Entry — and
a probe is a pure function of the extents it reads, so the second and later
passes over an identical extent list are re-reads of bytes already seen. The
key is the extent list itself, NOT the clip name or title: two names for one
File Entry share a key (one probe), while two genuinely different clips have
different `start_lba`s and so keep their own probes and streams. Cache size
is bounded by the caller's title/clip cap (`MAX_XPL_TITLES` /
`MAX_HDDVD_CLIPS`).

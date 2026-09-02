# src/labels/mod.rs — design notes

Long-form rationale relocated from doc comments per the comment-guard
audience cap (internal items cap at 3 lines; see `ci/comment-guard.py`).

## `label_at`

The VENDOR label occupying 1-based STN slot `n` of `stream_type`, if any.

Slot lookup is restricted to labels with no `StreamId`, and that
restriction is the whole point. `stream_number` is not one coordinate
system, it is two that share a name:

  * on a vendor label it is a slot in the one stream table the config blob
    describes, which is what this function's callers count against; while
  * on a derived label it is that stream's slot in *its own* playlist's
    table, which says nothing about any other playlist's numbering.

Reading a derived label out of a slot lookup therefore answers a question
about table A with a fact about table B. A label that carries a `StreamId`
names the stream it describes outright and is bound by that id instead —
see `apply_labels`.

## `find_anchor`

Find the title the label list is actually describing, for one stream type.

A vendor label list is a single stream table — one playlist's STN slots —
but it is handed to every title on the disc. When sibling playlists cover
the same clip with different stream subsets, per-title ordinal numbering
puts the same label on different physical PIDs in each, and the flags
contradict each other. The list itself carries no playlist id, but it does
carry a language per slot, and that sequence is a fingerprint: on the
corpus, exactly one title's per-type language sequence reproduces the label
list position for position, and content confirms that title's binding is
the correct one.

So: the anchor is the title that `anchor_score` admits and that confirms
the most of the list. `None` means no title matches — the list describes a
stream table this disc scan cannot see, and nothing may be bound
authoritatively.

Only VENDOR slots take part. Derived labels (MPLS / CLPI) name their own
stream and bind directly, so admitting them here would be asking a title to
agree with a numbering that describes a different playlist — the mistake
that made this gate a coin flip while the merged list was the only list
there was.

## `anchor_score`

How strongly this title's stream sequence matches the vendor label list, or
`None` if the title contradicts it and cannot be the table it describes.

A stream disqualifies the title when the vendor label on its slot states a
different language. A slot the vendor list does not name constrains nothing:
under-yield is the normal shape of these blobs — an authoring layer ships
editorial labels for the streams it considers interesting and leaves the
rest as bare slots — so a hole is the list's silence, not a disagreement.

The score is how much of the list the title positively CONFIRMS: slots where
both sides state a language and state the same one. A list that names no
languages at all scores zero everywhere and the ranking falls through to
table size, which is what it did before this became a fingerprint.

## `apply_labels`

Apply a pre-extracted set of labels to titles' streams.

Every label either NAMES the stream it describes or it does not, and that
split — not a slot number — decides how it binds. Four tiers, most certain
first:

  1. **This title is the anchor.** `find_anchor` identifies the title
     whose stream table the vendor list is describing, so its slots are the
     list's own slots and bind directly.

  2. **An anchor proved this PID.** Every slot of the anchor yields a
     `(clip, PID) -> label` fact, and a PID is the same physical stream in
     every playlist that plays that clip. Sibling playlists bind through
     that map, so a vendor label lands on the same elementary stream no
     matter which playlist enumerates it, and a slot the anchor never showed
     us is not bound at all.

  3. **The label names this stream.** A derived label carries a
     `StreamId` read out of the very table the stream itself was built
     from, so `(clip, PID)` equality is identity, not inference. This is the
     floor that gives every stream its language and codec.

  4. **Ordinal, language-checked.** A vendor label with no anchor behind it
     falls back to the 1-based per-type STN slot — but only where tier 3 had
     nothing to say, and only when the languages do not contradict.

Tier 3 outranking tier 4 is the substance of this ordering: a label that
states which stream it belongs to beats a guess about which stream it might
belong to, even when the guess is the richer label. The policy where a title
cannot be bound confidently is to leave the stream alone: an unlabelled track
is a much smaller harm than a full-dialogue track wearing `forced`, which
presents to the user as a duplicate of the track they wanted, and which the
muxer can only undo on discs that state `forced_on_flag`. A subtitle label
carries nothing but the qualifier, so an unverifiable one has no upside at
all to trade against that risk, and the ordinal path applies one only when
the label and the stream both state a language and it is the same one.

Audio streams update `purpose` + `label` (codec/variant info; never
English purpose text). Subtitle streams update `qualifier` and the
`forced` flag.

Extracted from `apply()` so the matching logic is unit-testable without
needing a SectorSource / UdfFs.

## `merge_mpls_floor`

Merge the MPLS-derived floor into a framework parser's label list.

The two lists do not share a coordinate system, and the merge must not
pretend they do. A framework `stream_number` is a slot in the one stream
table the vendor blob describes; an MPLS label's is that stream's slot in
its own playlist's table. Matching them by number — which is what this
function used to do — merges by an equality that means nothing, and the
merged entries then land on whatever stream happens to sit at that ordinal
in each title.

So nothing is merged BY slot. Every MPLS label whose stream is not already
named in `framework` is appended, keeping its own `StreamId`, and
`apply_labels` decides per stream which of the two reaches it: the
vendor's editorial label where an anchor puts it there, the floor
everywhere else. Framework labels stay richer and stay ahead — they are
simply no longer competing for a slot number.

Sorted at the end (vendor slots first, in slot order, then the named
streams) so callers see a deterministic list.

## `append_clpi_orphans`

Append CLPI ProgramInfo streams that no existing label already names.
These are "orphan" streams — physically present in the .m2ts per CLPI's
clip-authoritative view, but no MPLS playlist references them, so the
framework and the MPLS floor both missed them. Empirically they are
commentary or alternate-version streams the authoring tool left out of the
published playlist. Returns the number appended.

Each carries the `(clip, PID)` it was read under, so it binds to that
stream and to nothing else. It used to be given `stream_number =
max(existing per type) + 1, +2, …`, an ordinal invented here and shared
with the slot numbering the vendor list uses — which meant a title with
more streams than the list had slots could reach an orphan by counting, and
be labelled from a stream no playlist even plays. There is no ordinal now:
a stream that is in no playlist is in no title, so an orphan label binds to
nothing, which is exactly right and is now structural rather than lucky.

## `select_result`

Pick the winning parser result from `results` (built in PARSERS order):
highest `Confidence` among non-empty results, with the earliest array
position winning on a tie — matching `extract()`'s strict-`>` first-wins
scan.

`Iterator::max_by_key` returns the LAST maximal element, so the key is
`(confidence, Reverse(index))`: among equal-confidence entries the one
with the smallest index has the largest `Reverse(index)` and is selected,
i.e. first wins.

## `collect_chapter_summary`

Scan `/BDMV/PLAYLIST/*.mpls`, parse each, return a row per playlist with
chapter count (entry marks only) and total duration. Sorted by playlist
filename. Skipped entries (read error, parse error, no marks) silently
dropped — this is a diagnostic field, not a correctness-critical one.

## `jar_inventory`

List filenames found under any `/BDMV/JAR/<x>/` subdirectory of the disc.
Deduped, sorted. Returns an empty vec if no JAR dir is present.
`pub(crate)` so filename-based parsers (e.g. `png_filenames`) can scan
menu-asset names without a reader.

## `jar_inventory_from`

The body of `jar_inventory`, over the `/BDMV/JAR` children directly, so
it is unit-testable without a `UdfFs`.

A `BTreeSet`, not `Vec::contains`: the entry names come from the disc's own
UDF directory records, so both the file count and the name lengths are
attacker-controlled, and a linear `contains` doing a full `String` compare
per candidate is quadratic in the number of files. The set also subsumes
the trailing sort — it yields sorted, deduplicated output directly.

## test: `jar_inventory_dedup_does_not_hang_on_a_hostile_directory`

`jar_inventory` deduplicated with a linear `Vec::contains`, doing a full
`String` comparison per candidate — quadratic in a file count taken
straight from the disc's UDF directory records, with attacker-chosen
name lengths to inflate each comparison.

This is a HANG GUARD, and the name says so: a return to the linear scan
makes this fixture run for minutes (120 000² / 2 comparisons over a
180-byte shared prefix), which without the deadline would wedge CI
rather than fail it. It is not a complexity proof — no assertion here
can distinguish `BTreeSet` from any other sub-quadratic dedup, and the
clock-free half of the claim (dedup, sort, directory exclusion) belongs
to `jar_inventory_dedups_sorts_and_skips_dirs` below.

The deadline is a real margin, unlike the 6x one that made
`paramount.rs`'s wall-clock test flake under a loaded CI box: measured
at 0.14 s debug / 0.07 s release against 10 s, so ~70x. A shared CPU
does not close that; a quadratic dedup does not survive it.

## test: `parsers_registry_order_locked`

Lock the parser roster + order. If someone reorders the array or
adds/removes a parser, this test forces them to update the expectation
explicitly. The order is load-bearing: first matching `parse()` wins, so
reordering changes which parser claims a disc on overlapping detect
signals.

dbp + deluxe MUST stay at the end (their detect triggers on "any BD-J
disc"; placing them earlier would short-circuit the stricter parsers
above them).

## test: `select_result_first_wins_on_tie`

`select_result` must pick the highest-confidence non-empty result and,
on a confidence tie, the FIRST in array order (regression for the old
`analyze()` `max_by(...then(Equal))` no-op that picked the LAST).

`extract()` — the path that actually ships — used to re-derive this
same rule with its own inline `>` scan and had no test at all, so the
bug this test guards against could have recurred there unnoticed. It
now calls `select_result`, so this test covers both.

## test: `suppression_is_by_named_stream_not_by_slot_number`

Spec: the merge suppresses a floor entry only when the list already
NAMES that stream. A framework label occupying the same slot number is
not the same fact — a vendor slot and a playlist STN slot are different
coordinate systems — so it suppresses nothing.

This is the merge rule inverted from what it used to be: the old key was
`(stream_type, stream_number)`, which dropped a floor entry whenever
some unrelated vendor slot happened to share its number, and kept one
whenever it did not. Both outcomes were decided by an accident of
counting.

## test: log-injection guard on playlist-name logging

A playlist name is a raw UDF directory entry — disc-controlled bytes,
validated no further than a lossy UTF-8 decode. Logging it through
tracing's `%` (Display) sigil writes those bytes VERBATIM, so a crafted
`.mpls` filename carrying ANSI escapes or control characters forges
terminal output and log structure in any consumer rendering the event
(CWE-117). `?` (Debug) escapes them, and `str`'s Debug is exactly the
escaping this needs.

`info!` is not covered by the debug/trace-logging exemption: this fires
on an ordinary rip of an ordinary disc.

Mutation: put `%` back on `playlist` in `apply_labels` and this goes red.

## test: `forced_label_follows_the_pid_not_the_ordinal_across_sibling_playlists`

The cross-playlist mis-binding this module's two-tier binding exists to
stop, in its measured shape: two playlists cover the identical feature
clip, one of them enumerates one extra subtitle ahead of the forced
slots, and the vendor label list describes the shorter of the two.
Numbering the same list from 1 inside each title puts the `Forced`
label on a different PID in each — in the longer playlist, on the full
dialogue track. The user then sees two identical-looking English
subtitle tracks, one of them wrongly flagged forced.

Binding by the PID an anchored title proved, rather than by ordinal,
puts `forced` on the same two physical streams in both playlists and
leaves the extra one alone.

## test: `ordinal_binding_drops_a_subtitle_label_that_contradicts_the_stream_language`

No anchor to work from (nothing on the disc reproduces the label list's
language sequence), so binding falls back to the STN ordinal — and a
label that names a different language than the stream it would land on
is evidence the list is describing some other stream table. Drop it:
unlabelled beats mislabelled, because the payload here is `forced`.

## test: `generate_video_label_secondary_dolby_vision_el`

Spec: a secondary (dependent-view) video stream with Dolby Vision
enhancement layer gets the brand string "Dolby Vision EL"; every other
HDR format on a secondary stream gets no label at all (that wording is a
CLI concern). Mutation: delete the `HdrFormat::DolbyVision` arm so it
falls through to the `_ => String::new()` catch-all, losing the brand.

## test: `generate_video_label_480_boundary`

Spec: 480 lines is the SD floor — a stream with height exactly 480 must
get the "480p"/"480i" token (BD spec height boundary), not fall through
to the empty-resolution case. Mutation: `h >= 480` -> `h < 480` inverts
the boundary so a legitimate 480-line stream (h == 480) produces no
resolution token at all.

## test: `generate_video_label_sdr_produces_no_hdr_token`

Spec: SDR is the unmarked default — it must never appear as a token in
the generated label (only non-SDR formats get an explicit tag).
Mutation: delete the `HdrFormat::Sdr` arm so it falls through to
`_ => parts.push(hdr.name())`, appending a spurious "SDR" token.

## test: `generate_audio_label_atmos_folds_brand`

Spec: the Atmos-aware variant folds "Atmos" into the codec brand name
for TrueHD/DD+ carriers, distinct from the plain wrapper. Mutation: stub
the whole function to `String::new()` / a constant literal — either way
it stops reflecting the codec/channel inputs.

## test: `generate_audio_label_covers_pc_container_codecs`

Spec: every disc-audio codec in the enum has a full marketing name,
including the lossy PC-container codecs (AAC/MP2/MP3/FLAC/Opus) that
`generate_audio_label_all_codecs` above doesn't cover. Mutation: delete
any one of these match arms — the codec falls through to
`_ => return String::new()`, silently losing its label.

## `codec_hint_consistent`: chained-OR boundary hardening

The family-detection booleans chain `h.contains(..) || ...` synonym
checks. Each test below isolates ONE clause so a `||` -> `&&` flip there
changes the verdict.

### `codec_hint_consistent_truehd_space_synonym`

Isolates the `"true hd"` (space form) synonym in `says_truehd`, which
mutant testing hit at 396:44's `||`. If that `||` is weakened to `&&`,
"True HD" alone (no "truehd" substring) no longer sets `says_truehd`,
`names_family` goes false entirely (no other family clause matches), and
the function takes the "no family named" early-return path — turning a
should-be-`false` verdict for a mismatched codec into `true`.

### `codec_hint_consistent_ddp_ac3_plus_no_hyphen_synonym`

Isolates the `"ac3+"` (no-hyphen) synonym in `says_ddp` (398:9's `||`).
A hint matching only this clause must still classify as DD+, not fall
through to the plain-AC3 `says_ac3` check.

### `codec_hint_consistent_ddp_eac3_synonym`

Isolates the `"eac3"` synonym in `says_ddp` (401:9's `||`), the last
clause before the chain moves to "digital plus"/"dd+".

### `codec_hint_consistent_lpcm_bare_pcm_synonym`

Isolates the `"pcm"` (no "lpcm") synonym in `says_lpcm` (409:40's `||`).
A bare "PCM" hint on a non-LPCM stream must still be judged
inconsistent — if the `||` were `&&`, "PCM" alone would fail to set
`says_lpcm`, `names_family` would go false, and the function would take
the "no family named" path, wrongly returning `true` for ANY codec.

### `codec_hint_consistent_names_family_dts_ma_alone`

Isolates the `says_dts_ma || says_dts_hr` disjunction inside the
`names_family` chain (418:60). A hint that sets `says_dts_ma` alone
(e.g. "Master Audio", without "hd ma") must still make `names_family`
true; weakening that `||` to `&&` requires both clauses at once, so
`names_family` goes false and the function wrongly reports "consistent"
for a codec the hint never named.

### `codec_hint_consistent_truehd_arm_atmos_alone`

Isolates the `Codec::TrueHd => says_truehd || says_atmos` arm (433:38).
An Atmos-tagged hint that names a DIFFERENT lossless carrier by name
(DD+) must still be judged consistent with a TrueHd stream purely on the
Atmos marker — `||` -> `&&` would require the hint to ALSO say "truehd",
which an Atmos-only marker doesn't.

### `codec_hint_consistent_dts_arm_not_bypassed`

Spec: `Codec::Dts` is consistent ONLY when the hint's DTS-family
bookkeeping (`says_dts`) is true, not just because `names_family` is
true via some other carrier. Mutation: delete the `Codec::Dts =>
says_dts` arm (438:9) — it falls to `_ => true`, so ANY named family is
(wrongly) "consistent" with a Dts stream.

### `codec_hint_consistent_lpcm_arm_not_bypassed`

Spec: `Codec::Lpcm` is consistent ONLY when `says_lpcm` is true.
Mutation: delete the `Codec::Lpcm => says_lpcm` arm (439:9) — same
bypass-to-`_ => true` failure mode as the Dts arm above.

## test: `slot_lookup_never_returns_a_label_that_names_its_own_stream`

Spec: a label that names its own stream is never reachable through the
slot lookup, whatever number it carries.

The two numbers are different coordinate systems. A derived label's
`stream_number` is its slot in ITS OWN playlist's table; the vendor's is
a slot in the one table the config blob describes. `label_at` answers
questions about the second, so it must not return the first.

Mutation: drop the `l.stream_id.is_none()` term from `label_at` — the
derived label at slot 1 is found first and shadows the vendor's.

## test: `the_derived_floor_cannot_veto_the_anchor`

Spec: the derived floor does not vote on which title anchors the vendor
list — and in particular cannot VETO the title that does.

The vendor list names two subtitle slots, eng then fra, and the feature
reproduces them. The feature has a third subtitle the vendor said
nothing about; the merge dropped a derived label into "slot 3", numbered
in a different playlist's coordinate system, and it says Spanish. Read
as part of the vendor sequence that is a contradiction, so the feature
is rejected, no title anchors, and every flag the list carries is lost.

This is the measured cost of a whole-sequence gate without provenance:
the vendor's own slots stay correctly positioned while the merged ones
break the sequence between them.

Mutation: drop the `l.stream_id.is_none()` term from `label_at` — the
derived "spa" is read as vendor slot 3, `anchor_score` returns `None`,
and there is no anchor.

## test: `the_anchor_is_the_title_that_confirms_most_of_the_list`

Spec: among titles the vendor list admits, the one that CONFIRMS more of
it wins — a slot where both sides state a language and state the same
one is evidence; a slot where either is silent is not.

Here the shorter title matches both named slots outright while the
longer one states no language at all, so it merely fails to contradict.
Size alone would hand the anchor to the title that proved nothing.

Mutation: rank on `n` alone (drop `score` from the comparison) — the
three-stream title with no languages wins.

## test: `a_vendor_qualifier_does_not_leak_onto_a_featurettes_own_stream`

Spec: an editorial qualifier the vendor list states for ONE stream table
does not leak onto a different physical stream in a title that merely
counts to the same ordinal.

Measured shape: the feature carries eleven subtitles whose first is SDH;
a dozen featurettes each carry a single English subtitle, on a different
clip and a different PID. Both are "subtitle number 1", so the ordinal
fallback put SDH on all of them — and the languages agree, so the
language gate could not catch it. What separates them is that the
featurette's stream is named by the derived floor, and a label that
names a stream outranks a label guessed onto it.

Mutation: move the `by_id` lookup in `resolve` after the ordinal
fallback — the guess wins again and the featurette is SDH.

## test: `an_anchor_proves_pids_only_for_the_clip_its_table_came_from`

Spec: the anchor proves a `(clip, PID)` fact only for the clip its
stream table was READ FROM — the first play item — never for every clip
the anchor happens to play.

`disc::bluray` builds a title's stream list from `play_items[0]`'s STN
table, and tier 3 fifty lines below says exactly that by keying its
derived ids on `clip0`. Tier 2's harvest contradicted it: it recorded
the anchor's slot PIDs against EVERY clip the anchor plays, so a sibling
playlist that plays a LATER clip of the anchor bound the anchor's
editorial label onto whatever stream in that clip happens to reuse the
PID — a different physical stream, in a different clip, whose own
language says so.

Mutation: harvest over `&title.clips` instead of its first clip — the
featurette wears the feature's SDH again.

## test: `a_vendor_codec_claim_does_not_follow_the_ordinal_onto_a_bonus_clip`

Spec: a vendor codec/variant claim does not follow the ordinal onto a
bonus clip that carries a different codec.

Measured shape: the feature's first audio is object audio; a dozen menu
and bonus titles each carry one plain stereo track. All of them are
"audio number 1", and the hint names the same codec family as the stream
it lands on, so the consistency guard passes it through and every bonus
clip advertises the feature's format.

Mutation: as above — move `by_id` after the ordinal fallback.

## test: `a_title_shorter_than_the_vendor_list_cannot_anchor_it`

Spec: a title whose stream count is smaller than the vendor list's
highest slot cannot be the table that list describes.

Mutation: drop the `n < extent` term from `find_anchor` — the one-stream
title becomes eligible, and on a disc where it sorts first it takes the
anchor away from the title that actually has the slots.

## test: `an_unnamed_slot_does_not_disqualify_a_title`

Spec: a slot the vendor list never names constrains nothing.

These blobs under-yield by design — the authoring layer ships editorial
labels for the streams it finds interesting and leaves the rest as bare
slots. Treating a hole as a disagreement rejects the very title the list
describes. (Before provenance the holes were filled with labels numbered
in an unrelated coordinate system, so the gate neither rejected nor
admitted on evidence.)

Mutation: make `anchor_score` return `None` for an unnamed slot — no
title anchors and the forced flag is never delivered.

## test: `fill_gaps_leaves_order_untouched_when_nothing_added`

Spec: the sort-by-(type, number) pass only runs when the merge actually
added something (`added > 0`); when MPLS contributed nothing new,
`framework`'s existing order (however the caller built it) must be left
untouched. Mutation: `added > 0` -> `added >= 0` is always true, so the
sort runs unconditionally, silently reordering a framework list that
wasn't already in (type, number) order even on a no-op merge.

## `build_program_info` / `build_clpi` (test fixtures)

`build_program_info`: builds a CLPI ProgramInfo section for one program
with the given (pid, stream_coding_info) pairs. Layout mirrors
`crate::clpi::parse_program_info`'s expectations: length(4) + reserved(1)
+ num_programs(1), then per-program spn(4)+pmt_pid(2)+num_streams(1)
+num_groups(1), then per-stream pid(2)+sci_len(1)+sci.

`build_clpi`: builds a full CLPI byte buffer (HDMV header + ProgramInfo)
declaring the given (pid, coding_type, lang) streams. `sci` layout
follows `crate::clpi::parse_program_info`'s per-coding-type match arms:
PG/IG = coding_type + 3-byte lang; audio (primary or secondary) =
coding_type + format/rate byte + 3-byte lang.

## test: `orphans_state_no_stn_slot_and_name_their_stream`

(d) An orphan states NO STN slot, and is identified by the stream it was
read from instead.

This test used to assert the opposite — that orphans continue the slot
numbering from `max(existing) + 1`. That number was invented here and
shared with the coordinate system `label_at` counts vendor slots in, so
a title with more streams of a type than the vendor list had slots could
reach an orphan by counting and be labelled from a stream no playlist
plays. An orphan is by definition in no playlist, hence in no title, so
there is no ordinal to give it.

## `codec_hint_consistent`

Does the parser's `codec_hint` name a codec consistent with the stream's
actual `codec`? `apply_labels` uses this to keep richer-but-consistent
hints (e.g. "Dolby Atmos" on a TrueHD stream — Atmos is a TrueHD extension
the raw spec codec can't express) while rejecting mis-bound ones (e.g.
"AC-3 2.0" on a TrueHD stream, the shuffled-label bug). Matching is by codec
FAMILY parsed out of the hint string. "Atmos" with no carrier named is
treated as compatible with its lossless carriers (TrueHD / E-AC-3). A hint
naming no recognizable codec family (pure editorial, e.g. "Commentary") is
consistent — it isn't asserting a codec.

## `generate_audio_label_atmos`

Atmos-aware variant: same codec/channel string as `generate_audio_label`
with the object-audio marker folded into the codec brand (e.g. "Dolby
TrueHD Atmos 7.1"). The "Atmos" string lives here in the label layer, not
in the core parser. Used when a bitstream probe detected an Atmos
substream and the stream still carries the basic (non-editorial) label.

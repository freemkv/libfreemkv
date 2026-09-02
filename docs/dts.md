# DTS / DTS-HD elementary stream parser (`src/mux/codec/dts.rs`)

Internal design notes moved out of source comments (see `ci/comment-guard.py`
prose caps). Each section is pointed to by a short `//` comment at the
corresponding site in `dts.rs`.

## `DTS_HD_EXT_SYNC`

An access unit is delimited by the next CORE sync; the parser locates and
exactly sizes each extension substream (via `exss_frame_size`) so a false
core sync inside the EXSS payload can't split the AU and truncate the
lossless extension.

## `emit_or_drop`

Gates an assembled access unit through the decodability check and either
pushes it or drops it. `au_pts`/`dur_ns` are already stamped on the shared
PTS clock (which the caller advances whether or not the AU survives), so a
drop leaves the following audio on its true timeline — a gap, not a shift.
Every drop is logged (fail-loud, never silent).

## `stamp_pts`

`front` is the AU's own core-PES PTS (from `front_pts`); `dur_ns` its
decoded duration.

The model matches the (correct) AC-3 path: **re-base to each PES's own
container timestamp, and advance by one frame duration ONLY within a run of
AUs that share the same PES.** A new PES (its `front` differs from the
previous AU's) trusts its own timestamp — so the emitted timeline tracks the
container and never drifts. Advancing within a PES is what fixes the DVD
case (several DTS core frames packed in one PES, which otherwise all
collided on that single PES timestamp → "non monotonically increasing dts").
A global running clock was WRONG here: once accumulated frame durations
exceeded the PES spacing it never re-based, drifting the track minutes past
the real length over a feature-long title.

## `CORE_HEADER_MIN_BYTES`

Number of leading bytes that must be buffered before the core `fsize` field
(bytes 5-7) can be decoded. This is a HEADER-LAYOUT minimum — "enough bytes
to read the size field" — and is deliberately distinct from
`MIN_CORE_FRAME_BYTES` (the decoded-frame-size validity floor). They must
not be conflated: this one gates buffer reads of the header, the other
rejects implausible decoded sizes.

## `MIN_CORE_FRAME_BYTES`

Minimum plausible decoded DTS core frame size, per ETSI TS 102 114: the
on-wire FSIZE floor is 95, so a real core frame is at least 96 bytes. A
decoded `core_size` below this means we matched a false/corrupt core sync
(a lucky 0x7FFE8001 in extension-substream payload whose 14-bit `fsize`
decoded to a tiny value) rather than a real frame, so we resync instead of
closing an access unit at a junk boundary and dropping the DTS-HD extension
tail.

## `PTS_UNSET`

Sentinel for "no valid PTS base captured yet". Real PTS-in-ns values are
non-negative (derived from the unsigned 90 kHz PES timestamp), so a
negative value can never collide with a genuine timestamp. Used to mark the
PTS base invalid after a forced flush so the next PES sets it regardless of
buffer state.

## `flush_tail`

Emits the final buffered access unit (the last core + its extension
substreams, which had no following core sync to close it during
streaming), gated through the decodability check. Requires a complete core
frame; drops a bare partial sync tail.

## `SYNCWORD_BYTES`

Byte length shared by the DTS core sync and the DTS-HD EXSS sync (both
32-bit words). `next_core_boundary` finds the next *valid* core sync after
the current core frame, to delimit the access unit: extension-substream
payload can contain byte sequences that match the core syncword, so each
candidate is validated by decoding its core size — a match whose decoded
size is implausible (`< MIN_CORE_FRAME_BYTES` or `> MAX_AU_BYTES`) is a
false sync and is skipped, continuing the search.

## EXSS header field bit widths

`EXSS_USER_DEFINED_BITS`, `EXSS_INDEX_BITS`, `EXSS_HEADER_SIZE_TYPE_BITS`,
etc. are the DTS-HD extension-substream (EXSS) header field bit widths
(ETSI TS 102 114, ExtSS header). `bHeaderSizeType` selects the short form
(`nuExtSSHeaderSize` 8 bits, `nuExtSSFsize` 16 bits) or, for larger
substreams, the long form (12 / 20 bits).

## `exss_frame_size`

DTS-HD extension substream (EXSS) total byte size — INCLUDING the
`0x64582025` syncword — read precisely from its header. `buf` must begin
with `DTS_HD_EXT_SYNC`. Returns `None` when the size fields aren't fully
buffered.

`nuExtSSFsize` is the total frame size in bytes minus one. Parsing it lets
the AU framer skip the extension by its exact length instead of scanning
its (arbitrary) payload for a core sync.

## `next_core_boundary`

Offset where the current access unit ends (the start of the next core
frame). The AU is the core frame plus its trailing DTS-HD extension
substreams, which are skipped PRECISELY by their declared size — so a
chance core syncword inside the XLL lossless payload can never be mistaken
for the next AU boundary (the bug that truncated the extension and produced
the "Failed to decode block code(s)" class). Falls back to the heuristic
core-sync scan only when an extension can't be sized (malformed / truncated
input).

## `scan_for_next_core`

Heuristic fallback (the pre-fix behaviour): scan forward for the next core
syncword whose decoded size is plausible. Used only when precise extension
skipping can't proceed; a chance core syncword in extension payload usually
decodes to an implausible size and is skipped.

## `dts_core_frame_size`

`fsize` is the 14-bit field at bits 46-59 of the header (bytes 5-7). On the
wire `fsize` is the frame length minus one, so this returns `fsize + 1`,
i.e. the core frame length in bytes (range 1..=16384). Callers treat the
result as the actual byte length and the MIN..=MAX range checks assume so.

Returns `0` when `data` is shorter than `CORE_HEADER_MIN_BYTES` — every call
site rejects that via the minimum-frame lower bound, so a `0` is never
mistaken for a valid tiny frame.

## `dts_core_samples`

Samples in one DTS core frame: `(NBLKS + 1) * 32`. `NBLKS` (7 bits) is the
core-header PCM-sample-block count (ETSI TS 102 114) that fixes the frame's
decoded sample count. Bit layout after the 32-bit sync: FTYPE(1) SHORT(5)
CPF(1) **NBLKS(7)** FSIZE(14) …, so NBLKS = byte4 bit0 + byte5 bits7-2.

## `DTS_PCMBLOCK_SAMPLES` / `DTS_SUBBAND_SAMPLES` (core-header validity)

DTS core-header validity constants (ETSI TS 102 114). For a NORMAL frame
`deficit_samples` must equal `DTS_PCMBLOCK_SAMPLES` — a termination frame
may carry fewer; `npcmblocks` must be a multiple of `DTS_SUBBAND_SAMPLES`;
`audio_mode` must be below `DTS_AMODE_COUNT`; `lfe_present ==
DTS_LFE_FLAG_INVALID` is rejected.

## `DTS_AMODE_COUNT`

Number of LEGAL `AMODE` (channel-arrangement) codes. The 6-bit AMODE field
(ETSI TS 102 114 §5.3.1) has 16 defined channel arrangements, codes 0-15;
only 16-63 are reserved/user-defined and undecodable. The per-AMODE channel
counts in ETSI TS 102 114 §5.3.1 cover all 16, confirming they are
decodable — codes 10-15 are the 6/7/8-channel layouts. A frame is dropped
only when `audio_mode >= DTS_AMODE_COUNT` (i.e. a truly reserved 16-63
code); dropping a legal 10-15 multichannel core would silence recoverable
audio.

## `DTS_CORE_SR_VALID`

Sample rate (Hz) per core `SFREQ` code (ETSI TS 102 114 Table 6-4); a `0`
entry marks a reserved code that fails header validation as an invalid
sample rate. Valid entries are locked to the spec by
`dts_core_sfreq_table_matches_the_dca_spec`; the reserved codes are
{0, 4, 5, 9, 10}.

## `core_header_drop_reason`

Decodability gate: the core-frame header validity checks from ETSI TS 102
114. Returns `Some(reason)` when the DTS core-frame header is invalid — in
which case the packet is undecodable ("Invalid data found") and dropping it
loses nothing a decoder could have used. Returns `None` (keep) for a
decodable header OR if the header can't be fully read (never false-drop on
our own buffer underrun; the framer only emits AUs whose core is fully
buffered and ≥ 96 bytes).

The 4-byte core sync is already validated by the framer, so this reads the
header fields that follow it. The 16-bit CPF header CRC is not verified (we
skip past it) and the audio-header/side-info CRCs are likewise not checked,
because decoders treat those bytes as optional/ignored — verifying them
would drop frames that decode fine (false positives).

## Test: `stamp_pts_reuses_front_when_no_running_cursor_yet`

`stamp_pts`'s third arm (`else if front != PTS_UNSET`) is only reached once
the first arm's `front != self.last_front_pts` has already failed (i.e.
`front == self.last_front_pts`) and no within-PES running cursor is
available yet (`next_pts_ns == PTS_UNSET`). In that state the AU must still
be stamped with its own (repeated) front timestamp, not silently collapsed
to 0 — a `!=` -> `==` typo on the guard would only take this arm when
`front` IS the unset sentinel, producing 0 instead here and the sentinel
value itself if front really were unset.

## Test: `drain_front_collapses_offset_zero_markers_instead_of_leaking`

Every drained access unit that shares its PES with the previous one pushes
a new `(0, pts)` marker once rebased; `drain_front` must collapse those down
to the single most-recent one each time, or `pts_marks` grows once per
access unit for the life of the track — an unbounded allocation on a
multi-hour disc. `front_pts()` alone can't observe this: it always finds the
last offset-0 entry regardless of how many duplicates precede it, so the
leak is invisible unless something checks that the collapse actually ran.

## Test: `make_exss`

Builds a real DTS-HD EXSS substream of `total` bytes (short header form),
with an optional false DTS core syncword embedded in its payload (decoding
to a plausible core size) — to prove precise sizing, not a payload scan,
bounds the extension.

## Test: `make_dts_ext`

Builds a minimal DTS-HD extension substream of `size` bytes (just the sync
+ zero-padding). The parser delimits extensions by the next CORE sync, not
by the extension's own size header, so a valid header isn't required — only
that the bytes carry no spurious core sync.

## Test: `bogus_tiny_core_sync`

Builds 4 bytes that look like a DTS core sync but whose `fsize` field
decodes to a tiny `core_size` (< MIN_CORE_FRAME_BYTES). With the dead-code
guards this passed validation and could close an access unit at a junk
boundary; with the fix it must be drained and resynced past.

## Test: `next_core_boundary_exact_syncword_length_is_not_need_more`

Only the 4-byte EXSS syncword is buffered after the core frame — no header
fields at all. The guard `buf.len() < pos + SYNCWORD_BYTES` must be strict
`<`: at exactly `pos + SYNCWORD_BYTES` the sync bytes ARE fully present, so
the function proceeds to identify them as an extension sync (then fails to
size the header and falls back, ending in `NextCore::None` here since
nothing after it looks like a core sync either). A `<=` typo would instead
return `NeedMore` at this exact length without ever inspecting what the 4
buffered bytes are.

## Test: `make_bad_dts_core`

Builds a structurally-framed but UNDECODABLE core: a valid `make_dts_core`
whose LFE flag is set to the reserved value 3 (`DTS_LFE_FLAG_INVALID`). It
still sizes and syncs correctly (so the framer delimits it normally), but
the core-frame header validity check rejects it as an invalid LFE flag
(ETSI TS 102 114 §5.3.1: an LFF value of 3 is invalid). LFE is byte10
bits2-1, and does NOT feed the frame duration (NBLKS + SFREQ only), so a
dropped bad core still carries the same `DTS_CORE_DUR_NS` as its good peers.

## Test: `reparse_real_dts_file`

Real-data fixture (ignored). Re-parses a raw `.dts` elementary stream
through `DtsParser` and writes the emitted access units back out, so the
garbage-extension → core-only drop can be validated against an actual
damaged stream (e.g. an extracted DTS-HD MA track) end-to-end with an
external DTS decoder. Env: `DTS_IN` (input), `DTS_OUT` (output).

    cargo test --lib dts::tests::reparse_real_dts_file -- --ignored --nocapture

## Test: `find_sync_matches_a_buffer_that_is_exactly_the_syncword`

`find_sync` scans `0..=len-4`, so a buffer that is EXACTLY the syncword
still matches. The existing tests cover 0, 3 and "longer than 4", which
leaves the `len == 4` boundary open — and that is the case a syncword split
across PES packets lands on the moment its last byte arrives.

## Test: `a_core_header_of_exactly_the_minimum_length_is_decoded_not_deferred`

`CORE_HEADER_MIN_BYTES` is "enough bytes to DECODE the size field", so a
buffer holding exactly that many must be decoded, not deferred. The
distinction is visible precisely at the boundary: with 10 bytes of a core
sync whose decoded size is sub-spec, the parser must recognise the false
sync, drain past it and resync down to the 3-byte carry-over tail. Waiting
instead leaves the false sync sitting at the front of the buffer, where it
blocks every later real core behind it.

## Test: `flush_emits_a_core_that_exactly_fills_the_buffer`

The end-of-stream flush must emit a final access unit whose core is exactly
as long as the buffer — the ordinary case, since the last AU is closed by
end-of-stream rather than by a following core sync. Rejecting it at the
boundary silently drops the last frame of every DTS track.

## Test: `flush_discards_a_long_buffer_that_does_not_begin_with_a_core_sync`

The flush guard is a DISJUNCTION: a buffer that does not BEGIN with a core
sync is discarded whatever its length. Requiring both conditions instead
lets a long run of junk through to `dts_core_frame_size`, which happily
decodes a 14-bit size out of arbitrary bytes — and the flush then emits an
"access unit" that is not DTS at all.

The junk here is sized so that mis-decoding it yields a plausible core size
that the buffer fully covers, which is exactly when the wrong answer looks
like a right one.

## Test: `exss_frame_size_needs_the_worst_case_header_and_no_more`

`EXSS_HEADER_MIN_BYTES` is the WORST-CASE header length — the long form's
43 bits after the syncword, rounded up to 6 bytes, plus the 4-byte sync. It
gates whether an extension substream can be sized precisely, and that is
what keeps a chance core syncword inside XLL payload from being mistaken
for the next AU boundary; too large and every extension falls back to the
payload scan, too small and the reader runs off a truncated header.

Pinned at both sides of the boundary rather than by value, so the field
widths it is summed from stay honest.

## Test: `an_access_unit_carries_the_source_of_the_packet_its_core_arrived_in`

The whole point of the shared buffer: an access unit whose core arrived in
an EARLIER packet keeps that packet's source offset, not the offset of
whichever packet completed it. At a clip boundary the two belong to
different clips, and taking the later one puts the unit in the wrong clip —
which is what left nine audio and subtitle tracks unplaceable.
</content>
</invoke>

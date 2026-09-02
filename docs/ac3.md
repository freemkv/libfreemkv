# AC3 / E-AC3 parser notes (`src/mux/codec/ac3.rs`)

## Why a frame set is one sample, never split into separate tracks

A legacy AC-3 syncframe is a complete access unit on its own, but an E-AC-3
access unit (ETSI TS 102 366 / ATSC A/52 Annex E) is a whole FRAME SET: the
mandatory independent substream (`substreamid` 0) plus every DEPENDENT
substream frame that follows it, plus any ADDITIONAL independent substreams
(`substreamid` 1..7) and their own dependents. A decoder needs the whole set
to reconstruct the programme (a 5.1 independent substream plus a dependent
substream carrying the extra channels of a 7.1 programme, and the AC-3-core +
E-AC-3-dependent arrangement used for backwards-compatible Dolby Digital
Plus), and every substream of the frame set covers the SAME time period.

WHY the whole frame set is ONE sample, rather than splitting an additional
independent substream (an associated / commentary service) into a track of
its own: Annex E orders the substreams of a frame set inside a single
elementary stream, all covering one time period, and a decoder handed a frame
set renders the programme it is asked for from it — that is exactly what the
container's E-AC-3 sample and its `dec3`/`num_ind_sub` description denote.
Splitting would mean rewriting the bitstream (re-numbering `substreamid` so
the extracted service becomes substream 0, and rebuilding its frame sets),
because a substream numbered 1..7 with no substream 0 is not a conforming
stream. That is a transcode, not a remux; this parser is lossless, so the
frame set stays whole and player-side programme selection decides what is
heard.

## `SAMPLE_RATES` / fscod2

Sample rates indexed by fscod (0=48kHz, 1=44.1kHz, 2=32kHz). fscod=3 is
reserved in AC-3; in E-AC-3 it signals "fscod2" (reduced rates: 24/22.05/16
kHz, selected by byte-4 bits `[5:4]`). `frame_sample_rate` decodes fscod2 in
the E-AC-3 case; the table's index-3 entry (48 kHz) is only the fallback when
the header is too short to read fscod2.

## `MAX_AC3_BUF` derivation

An AC-3/E-AC-3 syncframe is at most 8192 bytes (the `frame_size > 8192`
reject) and an access unit is a whole frame set: up to 8 independent
substreams, each with up to 8 dependent substreams (ETSI TS 102 366 Annex E)
— 72 syncframes worst case. A cap of one straddling frame set plus slack
therefore has to exceed 72 × 8192 = 576 KiB. If the buffer grows past the cap
without yielding a frame (pathological / never-syncing input) it is dropped
and resync begins, rather than accumulating one PES worth of data per call
for the whole title.

## `scan_access_units`

Per ETSI TS 102 366 (ATSC A/52) Annex E an access unit is one FRAME SET: the
mandatory independent substream (`substreamid` 0) plus every DEPENDENT
substream that follows it, plus any ADDITIONAL independent substreams
(`substreamid` 1..7 — associated/commentary services) with their own
dependents. The access unit therefore closes only when the next
`substreamid`-0 independent substream (or, with `at_eos`, the end of the
stream) is seen, and it carries THAT substream's PTS and duration: every
other substream of the frame set describes the SAME time period and adds no
duration of its own.

`base_pts_ns` times the access unit that begins at `data[0]` — i.e. the
running cadence carried over from the previous call. `anchor` re-anchors the
running PTS to this PES's own timestamp at the first access unit that STARTS
in the newly-appended bytes: a PES timestamp applies to the first access unit
beginning in that PES, never to one that began in an earlier PES and is only
being completed (or was held) here.

Returns the emitted access units, the offset in `data` from which bytes must
be carried over to the next call, and the PTS to stamp on the access unit
that begins that carry-over.

## `substream_role`

Byte 2 of an E-AC-3 syncframe is `strmtyp(2) | substreamid(3) | frmsiz[10:8]`
(ETSI TS 102 366 Annex E BSI). `strmtyp` 0 and 2 are independent substreams
(type 2 being an independent substream that is not the first of the bit
stream); `strmtyp` 1 is the dependent substream.

Annex E defines a FRAME SET as independent substream 0 — mandatory, always
first — with its dependent substreams, followed by the OPTIONAL additional
independent substreams 1..7, each with their own dependents; all of them
carry the same time period. So the access-unit boundary is an independent
substream with `substreamid` == 0, and NOT merely "an independent substream":
an additional independent substream (an associated or commentary service)
sits INSIDE the frame set already open. Keying the boundary on `strmtyp`
alone made every such substream close the access unit and advance the
running PTS a second time over the same ~32 ms, doubling the timeline (about
a second of A/V drift per second of audio).

`strmtyp` 3 is reserved: its BSI layout is not defined, so its `substreamid`
bits cannot be trusted and it is treated as starting a fresh access unit — an
unknown frame is never merged into an unrelated programme, and never
discarded as an orphan either.

Legacy AC-3 (`bsid < 11`) has no substream structure — byte 2 there is the
crc1 field, never `strmtyp` — so it always starts an access unit.

A frame set MAY carry a substream as several consecutive syncframes of fewer
than six blocks each (Annex E allows numblkscod < 3). Those extra syncframes
are `substreamid` 0 too, so each starts its own access unit here rather than
merging into one frame set. That is deliberate: each carries its OWN
numblkscod-derived duration (see `frame_duration_ns`), so the timeline total
stays exact, and the substreams are still delivered in bitstream order.

## `frame_crc_ok`

Per ETSI TS 102 366 (ATSC A/52) the frame carries a CRC-16/ANSI (poly 0x8005,
init 0, non-reflected) over the bytes after the 2-byte syncword — i.e.
`crc16_ansi(&buf[2..]) == 0` covers `frame_size - 2` bytes; the trailing crc
word makes a clean frame's residue zero. A nonzero residue is a
~1-in-65536-certain sign of payload corruption, so the frame is dropped
(silence gap) rather than shipped as a glitch.

## `acmod_channels` bit layout

This is the AUTHORITATIVE channel count for the track header: the DVD IFO
`audio_attr_t.channels` nibble is a well-known unreliable/stale field, so the
muxer prefers this over the IFO-claimed count (the bitstream acmod is
authoritative; the IFO audio nibble is not trusted). LFE adds one channel
(e.g. acmod=7 + lfeon → 6 = 5.1).

Bit layout from the syncword (A/52 §5.3.2 BSI):

```text
  byte 5: bsid(5) | bsmod(3)
  byte 6: acmod(3) | [cmixlev(2) if acmod has a centre and acmod!=1]
                   | [surmixlev(2) if acmod has surround]
                   | [dsurmod(2) if acmod==2] | lfeon(1) | ...
```

`acmod` therefore always occupies byte-6 bits 7-5; `lfeon` follows a variable
number of optional 2-bit fields, so the decoder tracks the bit cursor.

## `ACMOD_CHANNELS` table

Base channel count per AC-3 `acmod` (A/52 Table 5.8), BEFORE the LFE. Index is
the 3-bit acmod value; add 1 when `lfeon` is set.

```text
  0 = 1+1 (Ch1, Ch2)  -> 2     4 = 2/1 (L,R,S)        -> 3
  1 = 1/0 (C, mono)   -> 1     5 = 3/1 (L,C,R,S)      -> 4
  2 = 2/0 (L, R)      -> 2     6 = 2/2 (L,R,SL,SR)    -> 4
  3 = 3/0 (L,C,R)     -> 3     7 = 3/2 (L,C,R,SL,SR)  -> 5
```

## Test helpers and coverage notes

### `finalize_ac3_crc`

Relies on the CRC-16/ANSI residue property: appending `crc16([2..n-2])`
(big-endian) zeroes the register over `[2..n]`.

### `the_working_buffer_is_reused_across_packets_not_reallocated`

`parse` runs once per PES packet — of the order of 10^5 times on a feature's
audio track — and used to build a fresh `Vec` every call. The copy itself
cannot go without restructuring the borrow relationship, but the allocation
can: the buffer is taken out of `self`, filled, and put back with its
capacity intact. Asserting on capacity is deliberately white-box, because
that is exactly what regresses if someone reverts to `to_vec()` — the
behaviour would be identical and no other test would notice.

### `make_eac3_frame` byte layout

ETSI TS 102 366 Annex E: byte 2 is strmtyp(2) | substreamid(3) | frmsiz[10:8],
byte 3 is frmsiz[7:0], and the frame is `(frmsiz + 1) * 2` bytes. byte 4 =
fscod 0 (48 kHz), numblkscod 3 (6 blocks → 1536 samples → 32 ms), acmod 7 +
lfeon (5.1); byte 5 = bsid 16 so the E-AC-3 paths are taken.

### `a_held_access_unit_is_not_rescanned_from_its_first_frame_every_packet`

An access unit closes only at the next `substreamid`-0 independent substream,
so one that keeps gaining dependent substreams stays OPEN across PES
boundaries and its bytes stay in the carry-over. The carry-over must not be
re-scanned — and re-CRCed — from the access unit's first byte on every
packet: the buffer only stops growing at `MAX_AC3_BUF` (1 MiB), and a PES on
a DVD is about 2 KiB, so re-deriving the held access unit costs work
quadratic in the packets fed.

Measured directly, because a work bound has no frame-level shadow:
`frames_scanned` counts the syncframes the scanner sizes and CRC-gates.
Re-scanning from byte 0 examines 1 + 2 + ... + (N+1) frames.

Mutation: pass `None` for `held` in `parse` (or drop the `if let Some(h) =
held` resume) — the count returns to the quadratic figure.

### `a_discontinuity_drops_the_held_access_unit_with_its_bytes`

`parse` clears `self.acc` on a discontinuity because the buffered bytes are a
truncated frame. The held access unit is described by OFFSETS into exactly
those bytes, so it has to go with them. Without `self.held = None`, the next
packet resumes a HeldAu whose `start`/`end` were computed against the
pre-gap buffer but are applied to the unrelated post-gap bytes — splicing
audio across the gap at best, and indexing past the end of the new, shorter
buffer at worst.

The two existing discontinuity tests use plain AC-3 (bsid < 11), which never
holds an access unit open, so neither of them reaches this reset.

### `a_held_access_unit_is_dropped_when_the_track_poisons_before_it_resumes`

Mutation this catches: deleting (or inverting) the resume-path re-check `if
drop_reason.is_none() && self.tally.is_poisoned()` at the top of
`scan_access_units`. The verdict on a held access unit is frozen at the
moment it was OPENED, and `ac3_drop_reason` reads the tally BEFORE the
previous access unit is closed — so the very drop that crosses the poison
threshold lands after the next unit's verdict was already taken as `None`.
Without the re-check that unit is emitted as an ordinary frame after the
track has been judged too damaged to mux: corrupt audio passed through as
success, and worse, it is the ONE frame that escapes a whole-track fallback
whose entire point is that nothing after the verdict ships.

Neither existing held-AU test reaches this: both keep a pristine tally.

The fixture drives the exact interleaving above. `DropTally` poisons once
`verified_dropped * 2 > kept + dropped` past the 200-AU gate, so one PES
carries 200 CRC-failing `substreamid`-0 syncframes followed by a clean one.
Closing corrupt unit #200 (which happens only when the clean frame is
reached) latches the poison — after that clean frame's own verdict was
computed. Being an E-AC-3 unit that can still gain substreams, it is then
HELD across the boundary with `drop_reason: None`, which is precisely the
state the re-check exists for.

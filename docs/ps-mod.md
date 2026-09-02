# `src/mux/ps.rs` — extended notes

Relocated prose from doc/inline comments in `ps.rs`, trimmed there to stay
within the comment-guard's prose caps. Each `//`/`///` pointer in the source
names the section here it corresponds to.

## Stream-ID constant rationale

- `EXTENDED_STREAM_ID` (`0xFD`): the H.222.0 escape whereby the real stream id
  is the `stream_id_extension` carried in the PES extension. HD-DVD `.evo`
  puts its VC-1 video (and HD audio) here (a real HD-DVD title: VC-1 on
  `0xFD` ext `0x55`); a transport stream never uses it. The elementary-stream
  bytes follow the PES header exactly like any other PES — only the routing
  key differs.
- `MAX_PS_BUFFER`: a length-0 (unbounded) video PES is delimited by the next
  PS-layer boundary; if a corrupt stream declares an unbounded PES and never
  follows it with a boundary, `feed()` would otherwise accumulate the entire
  input. Past this cap we force the in-progress unbounded PES to flush at
  the buffer end so untrusted input cannot drive unbounded allocation. A
  real DVD pack/PES is at most a few KB; this leaves generous slack while
  still bounding worst-case memory.

## `dvd_audio_pid` rationale

The PID is `0xBD00 | sub_stream_id`, which is unique per sub-stream id (AC-3
/ DTS `0x80..=0x8F`, LPCM `0xA0..=0xA7`). Unlike the old per-codec relative
arithmetic, distinct sub-ids therefore always yield distinct PIDs — so a
mixed-codec title (e.g. AC-3 + DTS, whose sub-ids are 0x80 and 0x88) can
never collide on one PID. This is the single source of truth shared with
`Disc::scan_dvd_titles` (`src/disc/dvd.rs`), which sets each
`AudioStream.pid` from the same function so demuxer output routes through
the title's `pid_to_track`.

HD-DVD (`.evo` Enhanced VOB) carries Dolby Digital Plus (E-AC-3) on
`private_stream_1` sub-stream ids `0xC0..=0xC7` — a range DVD never uses
(DVD audio is `0x80..=0x8F` / `0xA0..=0xA7`), so admitting it here is purely
additive and cannot change any DVD mapping. The PID is `0xBD00 | sub` just
like the DVD audio ranges, so a mixed HD-DVD title (four DD+ tracks
`0xC0..0xC3`) routes each track to its own distinct PID.

## `dvd_pid` rationale

Routes by the REAL on-wire `(stream_id, sub_stream_id)` via the shared
[`dvd_audio_pid`] / [`dvd_subtitle_pid`] tables the scanner also uses —
never per-codec relative arithmetic, which collided on mixed-codec audio
(AC-3 0x80 and DTS 0x88 both mapping to 0xBD00).

`None` is returned for stream/sub-stream combinations the DVD title scanner
does not assign a PID to (e.g. MPEG audio 0xC0-0xDF, private stream 2,
unrecognized sub-stream ranges). The caller is expected to WARN-and-drop in
that case rather than silently mis-routing the packet.

## `PsDemuxer::pending_scan`

Boundary-scan cursor for an unbounded (length-0) PES still waiting for its
terminating PS-layer unit: `(buffer offset of the PES start code, buffer
offset up to which the search has already proved there is no boundary)`.
Both are buffer-relative and are rebased when the buffer drains.

Without it, every `feed` re-searches the WHOLE accumulated payload from the
PES header: the buffer only stops growing at `MAX_PS_BUFFER`, so a stream
that declares an unbounded PES and then never emits a PS-layer start code (a
corrupt or crafted VOB) makes the demuxer scan up to 4 MiB per call,
quadratic in the bytes fed. Cleared whenever the PES is emitted, so it can
never outlive the packet it describes.

## `find_ps_boundary`

Find the next PS-layer unit boundary at or after `from`: a start code whose
ID byte is a pack (0xBA), system header (0xBB), program-end (0xB9), or a
payload-carrying PES stream ID (0xBD..=0xEF).

A length-0 (unbounded) video PES must be delimited by the next PS-layer unit
— NOT by the next raw `00 00 01`. The MPEG-2 video elementary stream inside
the PES is itself full of `00 00 01 xx` start codes (picture 0x00, slices
0x01..=0xAF, GOP 0xB8, sequence 0xB3); a plain start-code scan would cut the
PES inside its own payload and re-scan the discarded video bytes as bogus PS
units. Restricting the search to PS-layer IDs (>= 0xB9, excluding the video
ES codes below it) frames the unbounded PES at the right boundary.

Returns `(boundary, searched_to)`. `searched_to` is the offset up to which
every byte has been PROVED not to begin a PS-layer boundary start code, so a
later call over the same buffer (grown at the tail) may resume there instead
of re-scanning the payload from the PES header. When the scan runs off the
end, the last two bytes are NOT proved: a `00 00 01` prefix can straddle the
next feed's boundary by up to two bytes.

## `parse_pts` — 5-byte PTS/DTS layout (ISO/IEC 13818-1 Table 2-17)

```text
byte0: [prefix:4][pts 32..30:3][marker:1]
byte1: [pts 29..22:8]
byte2: [pts 21..15:7][marker:1]
byte3: [pts 14..7:8]
byte4: [pts 6..0:7][marker:1]
```

## Test: `only_private_stream_2_is_navigation_and_never_a_routable_stream`

`is_nav()` exists to separate the ONE unmappable stream a DVD is expected to
contain — private_stream_2 (0xBF), the PCI/DSI navigation packs (ISO/IEC
13818-1 Table 2-22) — from every other packet whose `dvd_pid()` comes back
`None`, which is an unexpected, possibly-lost real stream. The mux loops use
the distinction to choose between a silent tally and a per-packet WARN, so
collapsing it to a constant either buries a genuine stream loss in the nav
tally, or floods the log with one warning per navigation pack on every DVD
ever ripped.

The invariant that ties the two together: `is_nav()` may only ever be true
where `dvd_pid()` is `None` — a packet that routes to a real track must
never be silently classified as navigation.

## Test: `max_ps_buffer_has_the_documented_value`

`MAX_PS_BUFFER` is read by its own tests only through the same symbol, so a
mutated arithmetic expression in its definition changes what the symbol
itself evaluates to and every self-referential assertion still passes. Pin
the compiled value against a literal computed independently.

## Test: `pack_header_exact_fit_is_consumed_not_awaited`

Pack-header framing (`sc + 14 > len`, then `sc + pack_len > len`) must accept
an EXACT fit — the whole pack (mandatory 14 bytes, or with stuffing) present
and not one byte more — rather than waiting for data that will never come.
Both checks are preceded by an unrelated start code so `sc != 0`: at
`sc == 0` a `sc + pack_len` vs. `sc * pack_len` mutant collapses to the same
value (`0`) and the bound stays unreachable from any input.

## Test: `system_header_length_high_byte_is_not_dropped`

`header_len` is a 16-bit big-endian field (`buffer[sc+4] << 8 |
buffer[sc+5]`). A `<<` -> `>>` mutation collapses the high byte to zero, so
any `header_length > 255` is misread as just its low byte — here 300
(`0x012C`) misread as 44. A start code embedded 50 bytes in (well inside the
true 306-byte unit but exactly where the mis-parsed 50-byte unit would end)
must stay buried in the skipped body under correct parsing, and surface as a
bogus extra PES under the mutant.

## Test: `find_ps_boundary_handles_a_bare_start_code_at_the_buffer_head`

`find_ps_boundary`'s bounds check (`sc + 3 >= data.len()`) must stay an
ADDITION: a `+` -> `-` mutation at `sc == 0` underflows the `usize`
subtraction and panics on a plain 3-byte start code with nothing after it —
exactly the tail a real feed can end on.

## Test: `an_unterminated_pes_is_not_rescanned_from_its_header_every_feed`

An unbounded (length-0) PES is terminated by the next PS-LAYER unit, and
until one arrives the payload accumulates in the buffer. The search for that
unit must not restart at the PES header on every feed: the buffer only stops
growing at `MAX_PS_BUFFER` (4 MiB), and a feed is one read batch (at most
510 sectors ≈ 1 MiB, 60 sectors ≈ 120 KiB on an optical drive), so
re-scanning from byte 0 costs work quadratic in the bytes fed — up to 4 MiB
of scanning per call, for as long as a corrupt or crafted VOB withholds the
boundary. A conformant DVD ends every pack within 2048 bytes and never
reaches this state.

Measured directly, because a work bound has no packet-level shadow:
`boundary_bytes_scanned` counts the bytes `find_ps_boundary` examines. 256
chunks x 4 KiB of boundary-free payload is 1 MiB of input; re-scanning from
the header on every call examines 4 KiB * 256*257/2 = ~128 MiB.

Mutation: drop the `Some((pes_at, searched_to)) if pes_at == sc` arm so
`from` is always `sc + 4`.

## Test: `a_resume_cursor_survives_the_drain_of_units_ahead_of_the_unbounded_pes`

The resume cursor is a BUFFER offset, so it must be rebased when the buffer
drains — and this is the only test in which a drain actually happens while a
cursor is live.

Mutations this catches, both halves of
`self.pending_scan.map(|(pes_at, searched_to)| (pes_at - pos, searched_to - pos))`
in `extract_packets`:
  * `searched_to - pos` -> `searched_to`: the next call resumes `pos` bytes
    PAST where the previous scan actually stopped, so that window is never
    examined. Here the terminating pack header lands inside it and is missed
    outright — the unbounded PES runs on past its real end, swallowing the
    following unit. That is a CORRECTNESS failure, not a slow path, and the
    assertion on the emitted packet catches it.
  * `pes_at - pos` -> `pes_at`: the stale offset no longer equals the PES's
    post-drain `sc`, the resume arm stops matching and the search restarts
    at the PES header. Caught by `boundary_bytes_scanned`, which is why the
    fixture puts 64 KiB of payload in the SAME chunk that opens the PES:
    that is exactly the span a restart re-examines, so the mutant roughly
    doubles the bytes scanned.

`an_unterminated_pes_is_not_rescanned_from_its_header_every_feed` cannot
reach either: it opens the unbounded PES as the very FIRST bytes of the very
first feed, so nothing ever drains ahead of it, `pos` stays 0 and the
subtraction is a no-op. The comment above it asserts neither component can
underflow; nothing exercised the arithmetic at all.

So this fixture puts COMPLETE PS units — a pack header and a length-bounded
PES — ahead of the unbounded video PES *in the same chunk*. The loop
consumes them, breaks on the unbounded PES, and drains `pos` bytes with
`pending_scan` live: exactly the real DVD shape, where a video PES opens
partway through a read batch.

## Test: `parse_stream_id_extension_walks_every_optional_field_to_the_right_offset`

`parse_stream_id_extension` walks every optional PES-header field (PTS/DTS,
ESCR, ES_rate, DSM_trick_mode, additional_copy_info, PES_CRC) and every
optional PES_extension sub-field (PES_private_data, pack_header_field,
program_packet_sequence_counter, P-STD_buffer) before reaching
`stream_id_extension`. Every one of those skips is a `pos +=`; a single
mutated increment (`-=`/`*=`) misaligns every read after it. This test arms
EVERY optional field at once with a known byte count, so any single wrong
skip anywhere in the chain lands on the wrong byte and the assertion fails —
one test proving the whole walk, rather than one per field.

## Test: `length_bounded_pes_exact_fit_is_emitted_not_awaited`

A length-bounded PES (`pes_packet_len != 0`) must be emitted the moment its
declared length is EXACTLY satisfied by the buffer (`sc + 6 > len`, then
`e = sc + 6 + pes_packet_len; e > len`), not held back waiting for a byte
that will never arrive. Feed nothing after the packet and don't flush — if
the boundary checks were `>=` instead of `>`, an exact fit would incorrectly
be treated as "not enough data yet" and the packet would never be produced.

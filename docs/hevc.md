# HEVC parser (`src/mux/codec/hevc.rs`) — extended notes

Overflow detail for comments capped by `ci/comment-guard.py`. Each section is
pointed to from a short `//` comment at the named item.

## `hevc_num_extra_slice_header_bits`

`num_extra_slice_header_bits` from a HEVC PPS NAL (H.265 §7.3.2.3): after the
2-byte NAL header, skip `pps_pic_parameter_set_id` + `pps_seq_parameter_set_id`
(both `ue(v)`) and `dependent_slice_segments_enabled_flag` +
`output_flag_present_flag` (`u(1)` each), then read `u(3)`. `None` if the PPS
is too short to parse — the caller then declines to guess a slice type.

## `hevc_first_slice_coding_type`

Measures the coding type from the FIRST coded slice of an access unit
(H.265 §7.3.6.1 `slice_segment_header`). Reads only the leading fields of the
first slice segment: `first_slice_segment_in_pic_flag` u(1), the IRAP
`no_output_of_prior_pics_flag` u(1), `slice_pic_parameter_set_id` ue(v), the
`num_extra_slice_header_bits` reserved bits, then `slice_type` ue(v). Returns
`None` for a non-first slice or on truncation — never a guess. `num_extra`
MUST come from the active PPS so the bit offset to `slice_type` is exact.

## `HevcParser::hdr10`

Combines the accumulated mastering-display and content-light SEI into a
complete `Hdr10Metadata`, or `None` until BOTH HDR10 SEI messages have been
seen. Requiring both means an SDR / partially-signalled stream never emits a
half-populated (confidently-wrong) HDR10 record.

## `HevcParser::scan_sei`

Scans an SEI NAL (`[2-byte NAL header][RBSP]`) for the two HDR10 payload
types and captures each the FIRST time it appears (per-stream constants).

RBSP structure (Rec. ITU-T H.265 D.2 `sei_rbsp` / `sei_message`): a sequence
of messages, each `payloadType` then `payloadSize` encoded as a run of 0xFF
bytes plus a final <0xFF byte (the "ff-extension" coding), followed by
`payloadSize` payload bytes. Emulation-prevention (00 00 03) is stripped
before reading — unlike a slice header, an SEI payload can be deep enough
that an emulation byte falls inside the fields we read. Unknown payload
types are skipped by their size so a later HDR10 message in the same NAL is
still reached.

## `HevcParser::mark_clip_boundary`

MPLS `connection_condition` 0x05 and 0x06 are the non-seamless values (per
the BD-ROM spec: 0x01 = first item / seamless, 0x05/0x06 = non-seamless).
The first CRA at/after this call is rewritten CRA_NUT (21) → BLA_W_LP (16)
so a linear decoder sets NoRaslOutput and discards the now-dangling RASL
leading pictures with no "could not find ref" error.

It is a no-op for the rewrite unless a CRA actually follows: an
IDR/IDR_W_RADL boundary needs no fix (it carries no cross-splice
references), and the flag is cleared by the first IRAP-class NAL it reaches.

SAFETY: never call this for connection_condition 0x01 (seamless/first item)
or within a single-clip title — doing so could convert a legitimate
mid-content CRA to BLA. The default (never called) path leaves output
byte-identical.

## `handle_param_set`

Handles a VPS/SPS/PPS NAL: decides whether to strip it (the decoder already
has the value) or emit it in-band, and tracks the currently-active body.

The decision MUST be made against the currently-active set (`cur`), NOT the
codecPrivate copy (`first`). The two player behaviours for hvcC-in-MKV
diverge exactly here:

- A *seek-capable / Annex-B* player (one that converts hvcC to Annex-B by
  inserting the parameter sets) re-applies the hvcC sets at every keyframe.
  `reassert_active` handles it.
- A *streaming* decode (a decoder consuming the MKV directly — what most
  integrity checkers do) applies hvcC ONCE at init and thereafter updates a
  parameter set ONLY from an in-band NAL.

So when a title redefines a set mid-stream (id 0 body A → B) and later
switches BACK to A (== codecPrivate), the change to A must STILL be emitted
in-band: the streaming decoder is sitting on B and will never revert
otherwise, decoding the whole A-segment against B → CABAC/cu_qp_delta
desync. Stripping on `== first` (the old behaviour) dropped exactly that
revert and corrupted every "switch back to the first body" segment.

Rules:
- First of its type → seeds codecPrivate; stripped (the decoder gets it from
  hvcC at init).
- Equal to the active set `cur` → redundant; stripped.
- Different from `cur` (a change, in EITHER direction) → emitted in-band and
  `cur` updated.

Returns `true` when the NAL was emitted in-band into `frame_data`.

## `reassert_active`

Appends the active parameter set `cur` to `prefix` (length-prefixed) so
every keyframe is SELF-CONTAINED: it carries the active VPS/SPS/PPS in-band
ahead of its slices. Skipped only when this access unit ALREADY carried the
NAL in-band (`emitted` — avoids a duplicate) or no active set exists yet.

Why unconditional (not only when the active set differs from codecPrivate):
a streaming decoder applies the hvcC param sets once at init, then relies on
in-band repetition. Some sources stop repeating a param set at later IRAPs
even though its body is unchanged; if the decoder then drops it (a CRA reset
or SPS event), nothing re-sends it and every subsequent slice fails with
"PPS id out of range" until the next genuine change (observed as a ~24 min
corrupt band on one dual-layer UHD title). Re-asserting the active set at
EVERY keyframe — what compliant Matroska muxers do at every IRAP — makes
streaming decode self-healing. Re-sending an identical param set is benign
(decoders expect it at IRAPs); cost is a few hundred bytes per keyframe.
This strictly supersets the earlier change-only re-assert, so the
param-set-revert fix is unaffected.

## `parse_mastering_display`

Parses a Mastering Display Colour Volume SEI payload (Rec. ITU-T H.265
D.2.28 / semantics D.3.28). Layout — 24 bytes total, all big-endian:
`display_primaries_x[c]` u(16), `display_primaries_y[c]` u(16) for c=0,1,2
(SEI primary order is c=0 Green, c=1 Blue, c=2 Red); white_point_x u(16),
white_point_y u(16); max_display_mastering_luminance u(32);
min_display_mastering_luminance u(32). Returns `None` if the payload is
shorter than 24 bytes (malformed → ignored, never partially populated).

## Test notes

**`hevc_hdr10_sei_keeps_the_first_value_and_ignores_later_repeats`** — the
`scan_sei` match arms are guarded with `self.sei_mastering.is_none()` /
`self.sei_content_light.is_none()`, so the FIRST mastering-display and
content-light SEI a title carries wins and later repeats (every AU of a real
HDR10 stream repeats both) are ignored. Each AU below carries only ONE of
the two messages, so the whole-scan early return
(`if self.sei_mastering.is_some() && self.sei_content_light.is_some()`)
never fires early and the per-arm guards are actually exercised.

**`sei_nal` (test helper)** — wraps one or more SEI messages in a prefix-SEI
NAL (type 39) preceded by an Annex-B start code. The assembled message bytes
are emulation-prevented (as a conforming encoder would) so they never form a
false start code; the 0x80 RBSP trailing-bits byte is appended.

**`scan_sei_stops_copying_once_both_hdr10_messages_are_captured`** — MEASURED,
not reasoned: an HDR10 stream carries a prefix SEI per access unit, and
`scan_sei` used to allocate + byte-copy the whole SEI RBSP through
`strip_emulation_prevention` on EVERY one, including after both HDR10
messages were already captured. On a ~200,000-frame UHD title that is
~200,000 wasted allocations/copies. Counted at the single
`strip_emulation_prevention` call site.

**`nonzero_num_extra_slice_header_bits_shifts_the_slice_type_offset`** —
`num_extra_slice_header_bits` (H.265 §7.3.2.3) is a PPS field, and the
`slice_reserved_flag[i]` bits it counts sit BETWEEN
`slice_pic_parameter_set_id` and `slice_type` in the slice segment header
(§7.3.6.1). Every earlier fixture used a PPS with the field == 0, so the
skip was never exercised: a parser that ignored the field entirely agreed
with all of them, and would then read `slice_type` from the wrong bit offset
on any real stream that sets it — mislabelling every picture's coding type.

**`reasserts_active_pps_at_bare_keyframe`** — regression (UHD banded
corruption): a stream redefines PPS id 0 mid-title, then a later keyframe
arrives WITHOUT repeating it (the source relies on the decoder retaining the
redefinition — valid for a raw bitstream). An hvcC player re-applies the
FIRST (codecPrivate) PPS at every keyframe, so the active redefinition must
be re-asserted in-band at that bare keyframe or the whole segment decodes
against the wrong parameter set.

**`keyframe_param_reassert_does_not_reallocate_the_frame`** — MEASURED: the
keyframe parameter-set re-assert must be spliced into the front of the
already-assembled access unit IN PLACE, not built as a fresh full-size
buffer. It used to `prefix.extend_from_slice(&frame_data)` and then replace
`frame_data` with `prefix`, which grew a few-hundred-byte `prefix` to the
FULL access-unit size — a fresh multi-MB allocation — memcpy'd the whole
frame into it, and dropped the presized buffer. One extra whole-frame
allocation plus one extra whole-frame copy per keyframe: a 2 h UHD title at
24 fps with a 1 s GOP is ~7,200 keyframes, ~14-28 GB of avoidable memcpy per
title. `PARAM_REASSERT_HEADROOM` exists so the splice never reallocates;
this test counts the reallocations that happen, which must be zero.

**`emits_switch_back_to_codecprivate_pps`** — regression (a UHD title, the
real bug): id 0 is body A (→ hvcC), then redefined to B, then the title
switches BACK to A. A streaming decoder (hvcC at init, in-band updates only)
is sitting on B; the switch back to A must be emitted IN-BAND even though A
== codecPrivate, or the whole A-segment decodes against B (cu_qp_delta
desync). Stripping on `== hvcC` dropped this revert.

**`cra_at_auto_detected_pts_backstep_rewritten_to_bla`** — regression for a
UHD Dolby Vision profile 7 dual-layer title: a multi-clip title is read as
one concatenated stream and the mpls connection_condition is never plumbed
to the parser, so the splice CRA opening the next clip kept its dangling
RASL leading pictures and a linear decoder flooded "Could not find ref with
POC N". The parser must AUTO-DETECT the boundary from the backward PES-PTS
reset between clips (each .m2ts has its own PTS base) and rewrite that
splice CRA → BLA_W_LP with no explicit `mark_clip_boundary` call.

**`cra_after_33bit_pts_wrap_not_rewritten`** — regression for the 33-bit PTS
wraparound false-trigger (rc.5.2 audit #1): a SINGLE clip whose raw 90 kHz
PES PTS crosses the 2^33 counter wrap (~26.5 h) must NOT be mistaken for a
non-seamless clip join. Before the fix the raw 2^33→0 backward step armed
`pending_clip_boundary` and the next in-clip CRA was wrongly rewritten
CRA→BLA_W_LP (dropping valid RASL pictures — visible corruption). After
unwrapping onto a monotonic timeline the wrap is absorbed and the CRA stays
CRA.

**`cra_splice_detected_at_large_pts_magnitude_not_masked_by_wrap_logic`** —
the wrap-vs-backstep test above keeps `high_pts` near the very top of the
33-bit range, close enough to the `PTS_WRAP_PERIOD / 2` threshold that
`high - unwrapped` and a hand-flipped `high + unwrapped` land on the SAME
side of the threshold at every step — it doesn't actually distinguish the
two. This test uses PTS magnitudes around 3e9 (order 2^32, well below the
wrap threshold but reached in well under an hour), where the subtraction and
the addition diverge: ordinary forward progression keeps `high - unwrapped`
small, but `high + unwrapped` would already exceed `PTS_WRAP_PERIOD / 2` on
the very next frame, which would pollute `pts_wrap_offset` by a full period
and mask the genuine two-clip splice that follows.

**`non_cra_nals_never_rewritten_at_boundary`** — non-CRA NALs are never
rewritten even when a boundary IS marked. IDR (19), RASL (8/9), VPS/SPS/PPS,
and a trailing slice all pass through unmodified; the IDR clears the pending
boundary so no later CRA is wrongly converted.

**`no_boundary_marker_is_byte_identical`** — a frame stream with NO boundary
marker is BYTE-IDENTICAL to a parser that has no splice-rewrite field at all
(the UHD-safety guarantee): asserts byte-equality of every emitted frame
across a multi-AU stream containing CRAs, IDRs, RASLs, VPS/SPS/PPS, and
trailing slices — none of which is ever marked.

**`seamless_boundary_no_rewrite`** — a SEAMLESS boundary (connection_condition
0x05/0x06) is expressed by NOT calling `mark_clip_boundary`, so a CRA across
a seamless join is left unchanged. This encodes the contract: only
non-seamless joins call `mark_clip_boundary`; seamless ones never do, so no
rewrite occurs.

**`redefined_pps_emitted_inline`** — a parameter set REDEFINED mid-stream
(same id, different body) must be emitted INLINE so the decoder
re-activates it. Some discs redefine PPS id 0 partway through the title;
the old parser kept only the first PPS, so the second segment decoded
against the wrong PPS (CABAC desync).

**`make_sps_full` (test helper)** — builds a stored SPS NAL with sub-layers
and a conformance window, so the parser must skip sub-layer PTL and the 4
conformance-window ue(v) fields before reaching the bit depths.
`max_sub_layers_minus1` controls the sub-layer loop.

**`hvcc_array_length_round_trips_above_256_bytes`** — the hvcC array length
is a 16-bit big-endian field written as two separate `push`es: `(len >> 8)
as u8` then `len as u8`. Every VPS/SPS/PPS the rest of the suite feeds is
well under 256 bytes, so the high byte is always 0 and a `>>` -> `<<`
mutation (which also always truncates to 0 for those inputs) is
unobservable there. A real HEVC SPS with an extended VUI/HRD block can
exceed 256 bytes, so this test uses 300+ byte VPS/SPS/PPS and decodes the
16-bit length fields back to confirm they round-trip to the exact NAL
length, not merely a byte that happens to be 0.

**`hevc_ps_reorder_is_installed_and_flush_drains_its_real_frames`** — HEVC
counterpart of `h264_ps_reorder_reconstructs_distinct_display_pts`. A
DVD/HD-DVD program stream stamps a PTS only on each GOP anchor, so the
parser must reconstruct display-order timestamps for the rest. Two things
are pinned here that nothing else constrains: `with_ps_reorder(true)` must
actually INSTALL the reorderer (a builder that returned a
default-constructed parser would silently leave the transport-stream path
in place on every DVD title), and `flush()` must drain the reorderer's real
buffered frames at EOF — not nothing, and not a manufactured empty frame at
PTS 0 (a zero-length Block whose timestamp jumps back behind every cluster
already written, RFC 9559 §5.1.3.2). Slice bodies follow H.265 §7.3.6.1:
`first_slice_segment_in_pic_flag`, the IRAP-only
`no_output_of_prior_pics_flag`, `ue(pps_id)`, `ue(slice_type)` (Table 7-7:
0 = B, 1 = P, 2 = I). PPS body 0xC0 sets `num_extra_slice_header_bits = 0`.

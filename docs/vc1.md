# VC-1 elementary stream parser — internal notes

Relocated prose from `src/mux/codec/vc1.rs` internal (non-pub) items, kept
here in full since it exceeds the 3-line internal-comment cap.

## `parse_vc1_interlace`

Read the advanced-profile sequence header's `INTERLACE` flag (SMPTE 421M
§6.1.1): bit 41 after the start code — after PROFILE(2) LEVEL(3)
COLORDIFF_FORMAT(2) FRMRTQ(3) BITRTQ(5) POSTPROCFLAG(1) MAX_CODED_WIDTH(12)
MAX_CODED_HEIGHT(12) PULLDOWN(1). `None` for simple/main profile or a header
too short / over-escaped to reach the bit. De-escapes emulation-prevention
bytes first (as `parse_vc1_resolution` does) so the bit offset is exact.

## `vc1_progressive_ptype`

Decode the advanced-profile **progressive** picture PTYPE VLC (SMPTE 421M
§7.1.1.4, Table): `0`=P, `10`=B, `110`=I, `1110`=BI (intra → I), `1111`=
Skipped (predicted, no residual → P). Only valid when the sequence is
progressive — for interlaced an FCM code (and, for field pictures, a combined
FPTYPE) precedes/replaces PTYPE, so the caller declines those.

## `vc1_frame_coding_type`

Measure the coding type of an advanced-profile frame from its picture header.
`frame_rbsp` starts immediately after the frame start code (`00 00 01 0D`).
Decodes PTYPE only for a PROGRESSIVE sequence (where PTYPE is the first
picture-layer field); declines (`None`) for interlaced/simple-main/unknown
rather than guess at the wrong bit offset.

## `handle_header`

Handle a seq_header or entry_point start-code unit (Annex B raw bytes).

Decision is against the currently-ACTIVE body `cur`, not the codecPrivate
copy `first`:
- First of its type → seeds codecPrivate; stripped (decoder gets it from
  the BITMAPINFOHEADER extra data at init).
- Equal to the active set `cur` → redundant; stripped.
- Different from `cur` (a change in EITHER direction, including reverting
  to the codecPrivate/first value) → prepended into `prefix` in Annex B
  form and `cur` updated.

Returns `true` when the unit was emitted into `prefix`.

## `parse_vc1_resolution`

Parse width and height from a VC-1 advanced profile sequence header.
The sequence header starts with 00 00 01 0F. After the start code:
byte 0 bits 7-6: profile (3 = advanced). Advanced profile seq-header
layout (SMPTE 421M, from sh[4]): PROFILE(2)+LEVEL(3)+COLORDIFF_FORMAT(2)+
FRMRTQ_POSTPROC(3)+BITRTQ_POSTPROC(5)+POSTPROCFLAG(1)=16 bits, then
MAX_CODED_WIDTH(12)+HEIGHT(12) = 40 bits total = 5 bytes.

## `make_ap_seq_header` (test helper)

Build an advanced-profile VC-1 sequence header encoding the given
width/height. Layout from sh[4]: PROFILE(2)=3, LEVEL(3), COLORDIFF(2),
FRMRTQ(3), BITRTQ(5), POSTPROCFLAG(1) = 16 bits, then
MAX_CODED_WIDTH(12) = width/2 - 1, MAX_CODED_HEIGHT(12) = height/2 - 1.

## `vc1_emits_entry_point_revert_to_first_value` (test)

Regression: entry_point is redefined from A (== codecPrivate) to B, then
switched BACK to A. A streaming decoder applied codecPrivate at init and
is now on B; the revert to A must be emitted IN-BAND even though A ==
codecPrivate, or the A-segment decodes against the wrong entry point.

## `vc1_keyframe_prefix_order_seq_unchanged_entry_redefined` (test)

Regression: keyframe where seq_header is UNCHANGED (stripped by scan) but
entry_point is REDEFINED (changed). Before the fix, the old code appended
entry_point during the scan, then reassert() appended seq_header AFTER it,
producing [entry_point, seq_header] — entry_point before seq_header,
violating SMPTE 421M. After the fix, assembly is always seq-then-entry.

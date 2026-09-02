# dvdsub — VobSub SPU boundary rule

The presence of a PTS — not merely an open `pending` — is the authoritative
SPU-boundary signal, so a lost continuation or a corrupt SPU_size can't merge
the next subtitle into the stuck unit.

## ycbcr_to_rgb range convention (deliberate)

This uses the **full-range (JFIF) BT.601** coefficients with no 16/235 luma
scaling. DVD IFO palette YCbCr is nominally studio-swing BT.601, so
studio-swing math would be more colorimetrically "correct" in isolation. But
the output here is a VobSub `.idx` `palette:` line, and the entire VobSub
ecosystem (the original tooling, mkvtoolnix, players that read the `.idx`
palette) is built around this full-range formula — it is the de-facto
on-disk convention. Emitting studio-swing-scaled RGB here would make
freemkv's palettes inconsistent with every other tool and wrong in players
that assume the VobSub convention. Do NOT "fix" this to studio-swing without
changing the consuming side in lockstep.

## ycbcr_to_rgb_reads_byte2_as_cr_and_byte3_as_cb test fixture

A saturated RED entry appears on disc as Y=76, Cr=255, Cb=85 (full-range
BT.601 encoding of RGB #FF0000), i.e. bytes `[0x00, 76, 255, 85]`. Reading
byte 2 as Cb and byte 3 as Cr instead turns this entry BLUE, which is the
exact user-visible symptom. The fixture is deliberately NOT built from this
crate's own doc comments: those described the order wrongly for a long time,
and the previous version of this test inherited the error from them and
therefore could not detect it.

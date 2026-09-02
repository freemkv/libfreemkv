# MP4 audio sample entries (`src/mux/mp4/audio.rs`)

## Module scope

The entry always describes the syntax actually found in the first audio
frame, not the codec the playlist claimed: a track the playlist calls DD+
whose first syncframe is a legacy AC-3 one is declared `ac-3`/`dac3`. The fit
oracle (`audio_fits`) in this module, mirrored by the sink, is what decides
which codecs are muxable at all.

## `EAC3_MIN_BSID`

Lowest `bsid` that identifies an Annex-E (E-AC-3) bitstream: ETSI TS 102 366
Annex E uses bsid 16, while 8 is AC-3 and 9/10 are the AC-3 alternate bit
stream syntax of Annex D. Both the parser and the sample-entry chooser read
this one constant so they cannot disagree about which syntax was found.

## `BitReader::read`

The accumulate step (`(v << 1) | bit`) is one instance of a pattern repeated
throughout this file — shift an accumulator left by exactly the width of the
next field, then OR in that field, mask-limited to the same width (the `push`
closures in `dac3_box`, `dec3_box` and `ddts_box`; the multi-byte bit-field
extractions in `parse_eac3` and `parse_dts`). Because the shift always
vacates precisely the bits the OR then fills, and never more, the two
operands never share a set bit — so `|` and `^` agree on every input, always.
Mutation testing flags each of these `|` sites as a surviving `|`→`^` mutant;
that is expected and is not a coverage gap. Don't write tests chasing it and
don't "fix" it by switching to `^` — either spelling is correct and equally
unenforceable by a test, so `|` stays because it is the conventional way to
write "set these bits" in a bitstream packer.

## `dec3_box`

`data_rate` states the bitstream's rate in kbit/s and must be non-zero; only
`parse_eac3` computes one, so this box is written only for a config that came
from an Annex-E syncframe (see `dolby_sample_entry`).

## `DTS_AMODE_CH`

DTS core base channel count per `AMODE` (all 16 defined values). Matches the
per-AMODE channel counts in ETSI TS 102 114 §5.3.1, the same table the
decodability gate in `dts.rs` (`DTS_AMODE_COUNT`) also uses, so a spec-legal
DTS-ES / 6.1 / 7.1 core (AMODE 13→7, 14/15→8) is declared with its true
channel count in the mp4 AudioSampleEntry / `ddts` box rather than a
truncated 6.

## `DTS_AMODE_LAYOUT`

`ddts` ChannelLayout speaker mask (ETSI TS 102 114 / DTS-in-ISOBMFF) per core
`AMODE`. Bit assignment: 0=C, 1=L/R, 2=Ls/Rs, 3=LFE, 4=Cs, 5=Lh/Rh, 6=Lsr/Rsr,
7=Ch, 8=Oh, 9=Lc/Rc, 10=Lw/Rw, 11=Lss/Rss, 12=LFE2, 13=Lhs/Rhs, 14=Chr,
15=Lhr/Rhr. Paired bits denote two speakers, single bits one.

This must stay consistent with `DTS_AMODE_CH`: the `ddts` box declares both a
channel count and this mask, and a decoder may trust either — so a mask that
describes fewer speakers than the declared count makes the box
self-contradictory and provokes a downmix or an outright error. The previous
`_ => 0x0007` catch-all did exactly that for AMODE 6, 7, and 10 through 15.
AMODE 6 (`L + R + S`) is the reachable one: its `S` is a single
centre-surround, not the Ls/Rs pair, so it is 3 channels and `0x0012`, not 4
and `0x0006`. `ddts_channel_layout_speaker_count_matches_declared_channels`
pins the invariant for all 16 values.

The AMODE annotations on the table name the layout ETSI TS 102 114 §5.3.1
gives for that AMODE and the mask that encodes it. Three of them used to be
rotated by one (AMODE 2 labelled "sum/difference", 3 "left/right total", 4
plain "L/R") and AMODE 9 was labelled "5.1 core with LFE" although 0x0007 is
the 5.0 mask — LFE is bit 3, OR'd in separately by `dts_channel_layout`. Both
mislabels invited a "correction" to the values, which are right and are
pinned by `ddts_channel_layout_speaker_count_matches_declared_channels`.

## `dts_channel_layout`

`amode` must be 0..=15; `parse_dts` rejects the reserved 16..=63 before this
is reached, so there is no layout to invent for them.

## `audio_fits`

Fit oracle for an audio codec: does `mp4://` currently carry it? Covers the
Dolby family (AC-3 / E-AC-3) and DTS (core / DTS-HD HRA / DTS-HD MA — the
core is described, whole access units pass through). TrueHD, LPCM, AAC are
not yet mapped and are skipped with a loud report (never silently dropped).

## Test fixtures

`ac3_frame_distinct` (line ~905 in the test module): AC-3 syncframe with
every BSI field distinct: fscod=1 (44.1 kHz), frmsizecod=37 (bit_rate_code
18), bsid=8, bsmod=5, acmod=6 (2/2), lfeon=1 → 4.1 = 5 channels.
byte4 = fscod(2)=01 | frmsizecod(6)=100101 → 0x65; byte5 = bsid(5)=01000 |
bsmod(3)=101 → 0x45; byte6 = acmod(3)=110 | surmixlev(2)=00 | lfeon=1 |
pad 00 → 0xC4. acmod 6 has surround but no centre, so per §5.4.2 cmixlev is
absent and surmixlev is present: lfeon lands at bit 5 of byte 6.

`eac3_frame_distinct`: Annex-E syncframe with every field distinct:
frmsiz=0x123 (292 words → 584 bytes), fscod=1 (44.1 kHz), numblkscod=0
(1 block → 256 samples), acmod=5 (3/1), lfeon=1 → 5 channels, bsid=16.
byte2 = strmtyp 00 | substreamid 000 | frmsiz hi 001 → 0x01; byte3 = frmsiz
lo 0x23; byte4 = fscod 01 | numblkscod 00 | acmod 101 | lfeon 1 → 0x4B;
byte5 = bsid 10000 | dialnorm hi 000 → 0x80.

`eac3_frame_fscod3`: Annex-E syncframe with fscod=3 — the reduced-sample-rate
path, which no other fixture reaches. fscod2=2 → 16 kHz, 6 blocks; acmod=2,
lfeon=0. byte4 = fscod 11 | fscod2 10 | acmod 010 | lfeon 0 = 0xE4. Exactly
6 bytes: also the minimum-length Annex-E header.

`dts_frame_high_bits_set`: a DTS core whose split fields all have their high
parts set, so a lost high bit is visible: NBLKS bit 6 (byte 4 bit 0) and
FSIZE bits 13-12 (byte 5 bits 1-0). NBLKS=79 → 2560 samples; FSIZE=0x3000 →
core 12289. AMODE=9, SFREQ=13 (48 kHz), LFF=1 out of byte 10 = 0x0A.

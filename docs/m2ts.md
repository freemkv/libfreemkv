# m2ts test notes

## h264_avcc_parameter_sets_are_emitted_as_annex_b

An H.264 track's codec_private is an **avcC** record, not hvcC. The BD-TS
muxer must parse it with the avcC parser and emit the SPS/PPS as Annex-B
parameter sets, or the H.264 elementary stream reaches the player with no
SPS/PPS at all and is undecodable — silently, because frame_count still
advances and the mux reports success.

Mutation: parse codec_private with hvcc_to_annex_b (the pre-fix behaviour) ->
the parser returns None, no parameter sets are emitted, and this fails.

## vc1_video_is_wired_to_the_non_nal_path

`M2tsStream::create` must opt a VC-1 video track OUT of Annex-B conversion.

This pins the WIRING in `create`, not just `TsMuxer`'s flag: deleting the
`set_video_codec` loop leaves every TsMuxer-level test passing, because those
drive the muxer directly and set the flag themselves. Only a test that goes
through `create` catches it — and mangling MPEG-2/VC-1 video is silent, since
frame_count still increments and the mux reports success.

Mutation: remove the `set_video_codec` loop from `create`, or make it declare
Vc1 as a NAL codec -> the ES gains a start code and this fails.

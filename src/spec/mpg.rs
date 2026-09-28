//! `MS-n`: the quotes behind the `mpg://` program stream sink (mpg-output-design v5
//! §2.2-§2.4, L2): ITU-T Rec. H.222.0 (02/2000) | ISO/IEC 13818-1:2000, the local
//! `/private/tmp/h222.txt` (J24: the 2000 edition), plus FFmpeg corroboration of the
//! DVD private_stream_1 sub-stream headers, which the standard leaves "user definable".

use super::{QuoteKind, SpecQuote};

const H222: &str = "ITU-T Rec. H.222.0 (02/2000) | ISO/IEC 13818-1:2000";
const H222_URL: &str = "https://www.itu.int/rec/T-REC-H.222.0-200002-S/en";
const FFMPEG_MPEGENC: &str = "FFmpeg 6.1.3, libavformat/mpegenc.c";
const FFMPEG_MPEGENC_URL: &str =
    "https://github.com/FFmpeg/FFmpeg/blob/n6.1.3/libavformat/mpegenc.c";

pub const MS_1_PACK: SpecQuote = SpecQuote {
    id: "MS-1",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.3 Table 2-32 Program Stream pack",
    locator: "h222:4620-4625",
    url: H222_URL,
    text: "pack() { pack_header() while (nextbits() = = packet_start_code_prefix) { PES_packet() } }",
};

pub const MS_2_PACK_HEADER: SpecQuote = SpecQuote {
    id: "MS-2",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.3 Table 2-33 Program Stream pack header",
    locator: "h222:4635-4650",
    url: H222_URL,
    text: "pack_header() { pack_start_code 32 bslbf '01' 2 bslbf system_clock_reference_base [32..30] 3 bslbf marker_bit 1 bslbf system_clock_reference_base [29..15] 15 bslbf marker_bit 1 bslbf system_clock_reference_base [14..0] 15 bslbf marker_bit 1 bslbf system_clock_reference_extension 9 uimsbf marker_bit 1 bslbf program_mux_rate 22 uimsbf marker_bit 1 bslbf marker_bit 1 bslbf reserved 5 bslbf pack_stuffing_length 3 uimsbf",
};

pub const MS_3_MUX_RATE: SpecQuote = SpecQuote {
    id: "MS-3",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.4 program_mux_rate, stuffing_byte",
    locator: "h222:4681-4690",
    url: H222_URL,
    text: "The value of program_mux_rate is measured in units of 50 bytes/second. The value 0 is forbidden. … may vary from pack to pack … In each pack header no more than 7 stuffing bytes shall be present.",
};

pub const MS_4_SCR: SpecQuote = SpecQuote {
    id: "MS-4",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.2.2 equations 2-18 to 2-21; §2.5.2.3",
    locator: "h222:4444-4526",
    url: H222_URL,
    text: "SCR(i) = SCR_base(i) × 300 + SCR_ext(i) … where the arrival rate within each pack is the value represented in the program_mux_rate field in that pack’s header. … For Program Streams, all bytes of each pack shall enter the P-STD before any byte of a subsequent pack.",
};

pub const MS_5_SYSTEM_HEADER: SpecQuote = SpecQuote {
    id: "MS-5",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.5 Table 2-34 Program Stream system header",
    locator: "h222:4701-4722",
    url: H222_URL,
    text: "system_header () { system_header_start_code 32 bslbf header_length 16 uimsbf marker_bit 1 bslbf rate_bound 22 uimsbf marker_bit 1 bslbf audio_bound 6 uimsbf fixed_flag 1 bslbf CSPS_flag 1 bslbf system_audio_lock_flag 1 bslbf system_video_lock_flag 1 bslbf marker_bit 1 bslbf video_bound 5 uimsbf packet_rate_restriction_flag 1 bslbf reserved_bits 7 bslbf while (nextbits () = = '1') { stream_id 8 uimsbf '11' 2 bslbf P-STD_buffer_bound_scale 1 bslbf P-STD_buffer_size_bound 13 uimsbf } }",
};

pub const MS_6_SYSTEM_HEADER_FIELDS: SpecQuote = SpecQuote {
    id: "MS-6",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.6 rate_bound, audio_bound, video_bound, reserved_bits",
    locator: "h222:4739-4829",
    url: H222_URL,
    text: "The rate_bound is an integer value greater than or equal to the maximum value of the program_mux_rate field coded in any pack of the Program Stream. … The audio_bound is an integer in the inclusive range from 0 to 32 and is set to a value greater than or equal to the maximum number of ISO/IEC 13818-3 and ISO/IEC 11172-3 audio streams in the Program Stream for which the decoding processes are simultaneously active. … The video_bound is a 5-bit integer in the inclusive range from 0 to 16 … Until otherwise specified by ITU-T | ISO/IEC it shall have the value '111 1111'.",
};

pub const MS_7_BUFFER_BOUND: SpecQuote = SpecQuote {
    id: "MS-7",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.3.6 stream_id, P-STD_buffer_bound_scale, P-STD_buffer_size_bound",
    locator: "h222:4843-4856",
    url: H222_URL,
    text: "Each elementary stream present in the Program Stream shall have its P-STD_buffer_bound_scale and P-STD_buffer_size_bound specified exactly once by this mechanism in each system header. … If the preceding stream_id indicates an audio stream, P-STD_buffer_bound_scale shall have the value '0'. If the preceding stream_id indicates a video stream, P-STD_buffer_bound_scale shall have the value '1'. For all other stream types, the value of the P-STD_buffer_bound_scale may be either '1' or '0'. … measures the buffer size bound in units of 128 bytes. … measures the buffer size bound in units of 1024 bytes.",
};

pub const MS_8_PSM: SpecQuote = SpecQuote {
    id: "MS-8",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.4.1 Table 2-35 Program Stream map; §2.5.4.2",
    locator: "h222:4891-4947",
    url: H222_URL,
    text: "program_stream_map() { packet_start_code_prefix 24 bslbf map_stream_id 8 uimsbf program_stream_map_length 16 uimsbf current_next_indicator 1 bslbf reserved 2 bslbf program_stream_map_version 5 uimsbf reserved 7 bslbf marker_bit 1 bslbf program_stream_info_length 16 uimsbf for (i = 0; i < N; i++) { descriptor() } elementary_stream_map_length 16 uimsbf for (i = 0; i < N1; i++) { stream_type 8 uimsbf elementary_stream_id 8 uimsbf elementary_stream_info_length 16 uimsbf for (i = 0; i < N2; i++) { descriptor() } } CRC_32 32 rpchof } … The maximum value of this field is 1018 (0x3FA). … The elementary_stream_id is an 8-bit field indicating the value of the stream_id field in the PES packet headers of PES packets in which this elementary stream is stored.",
};

pub const MS_9_CRC: SpecQuote = SpecQuote {
    id: "MS-9",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.4.2 CRC_32",
    locator: "h222:4950-4951",
    url: H222_URL,
    text: "CRC_32 – This is a 32-bit field that contains the CRC value that gives a zero output of the registers in the decoder defined in Annex A after processing the entire program stream map.",
};

pub const MS_10_PES_PACKET: SpecQuote = SpecQuote {
    id: "MS-10",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.6 Table 2-17 PES packet",
    locator: "h222:2981-3024",
    url: H222_URL,
    text: "'10' 2 bslbf PES_scrambling_control 2 bslbf PES_priority 1 bslbf data_alignment_indicator 1 bslbf copyright 1 bslbf original_or_copy 1 bslbf PTS_DTS_flags 2 bslbf ESCR_flag 1 bslbf ES_rate_flag 1 bslbf DSM_trick_mode_flag 1 bslbf additional_copy_info_flag 1 bslbf PES_CRC_flag 1 bslbf PES_extension_flag 1 bslbf PES_header_data_length 8 uimsbf if (PTS_DTS_flags = = '10') { '0010' 4 bslbf PTS [32..30] 3 bslbf marker_bit 1 bslbf PTS [29..15] 15 bslbf marker_bit 1 bslbf PTS [14..0] 15 bslbf marker_bit 1 bslbf } … if (PTS_DTS_flags = = '11') { '0011' 4 bslbf … '0001' 4 bslbf DTS [32..30] 3 bslbf",
};

pub const MS_11_PES_EXTENSION: SpecQuote = SpecQuote {
    id: "MS-11",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.6 Table 2-17 PES packet (concluded)",
    locator: "h222:3090-3146",
    url: H222_URL,
    text: "if ( PES_extension_flag = = '1') { PES_private_data_flag 1 bslbf pack_header_field_flag 1 bslbf program_packet_sequence_counter_flag 1 bslbf P-STD_buffer_flag 1 bslbf reserved 3 bslbf PES_extension_flag_2 1 bslbf … if ( P-STD_buffer_flag = = '1') { '01' 2 bslbf P-STD_buffer_scale 1 bslbf P-STD_buffer_size 13 uimsbf } … else if ( stream_id = = padding_stream) { for (i = 0; i < PES_packet_length; i++) { padding_byte 8 bslbf } }",
};

pub const MS_12_PES_LENGTH: SpecQuote = SpecQuote {
    id: "MS-12",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.7 PES_packet_length",
    locator: "h222:2957-2958",
    url: H222_URL,
    text: "A value of 0 indicates that the PES packet length is neither specified nor bounded and is allowed only in PES packets whose payload consists of bytes from a video elementary stream contained in Transport Stream packets.",
};

pub const MS_13_PSTD_SCALE: SpecQuote = SpecQuote {
    id: "MS-13",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.7 P-STD_buffer_scale, P-STD_buffer_size, stuffing_byte",
    locator: "h222:3564-3590",
    url: H222_URL,
    text: "If the preceding stream_id indicates an audio stream, P-STD_buffer_scale shall have the value '0'. If the preceding stream_id indicates a video stream, P-STD_buffer_scale shall have the value '1'. For all other stream types, the value may be either '1' or '0'. … No more than 32 stuffing bytes shall be present in one",
};

pub const MS_14_PRIVATE_DATA: SpecQuote = SpecQuote {
    id: "MS-14",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.7 PES_packet_data_byte",
    locator: "h222:3601-3602",
    url: H222_URL,
    text: "In the case of a private_stream_1, private_stream_2, ECM_stream, or EMM_stream, the contents of the PES_packet_data_byte field are user definable and will not be specified by ITU-T | ISO/IEC in the future.",
};

pub const MS_15_PTS_NAMES_FIRST_AU: SpecQuote = SpecQuote {
    id: "MS-15",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.7 presentation_time_stamp",
    locator: "h222:3276-3281",
    url: H222_URL,
    text: "In the case of audio, if a PTS is present in PES packet header it shall refer to the first access unit commencing in the PES packet. An audio access unit commences in a PES packet if the first byte of the audio access unit is present in the PES packet. In the case of video, if a PTS is present in a PES packet header it shall refer to the access unit containing the first picture start code that commences in this PES packet.",
};

pub const MS_16_PSTD_BUFFERS: SpecQuote = SpecQuote {
    id: "MS-16",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.5.2.3 Buffering",
    locator: "h222:4479-4506",
    url: H222_URL,
    text: "Bytes present in the pack header, system headers, Program Stream Maps, Program Stream Directories, or PES packet headers of the Program Stream such as SCR, DTS, PTS, and packet_length fields, are not delivered to any of the buffers … At the decoding time, tdn(j), all data for the access unit that has been in the buffer longest, An(j), and any stuffing bytes that immediately precede it that are present in the buffer at the time tdn(j), are removed instantaneously at time tdn(j). … The Program Stream shall be constructed and t(i) shall be chosen so that the input buffers of size BS1 through BSn neither overflow nor underflow in the program system target decoder. … For all Program Streams, the delay caused by system target decoder input buffering shall be less than or equal to one second except for still picture video data and ISO/IEC 14496 streams.",
};

pub const MS_17_SCR_FREQUENCY: SpecQuote = SpecQuote {
    id: "MS-17",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.7.1 Frequency of coding the system clock reference",
    locator: "h222:6349-6350",
    url: H222_URL,
    text: "The Program Stream shall be constructed such that the time interval between the bytes containing the last bit of system_clock_reference_base fields in successive packs shall be less than or equal to 0.7 s.",
};

pub const MS_18_PTS_FREQUENCY: SpecQuote = SpecQuote {
    id: "MS-18",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.7.4 Frequency of presentation timestamp coding",
    locator: "h222:6392-6406",
    url: H222_URL,
    text: "The Program Stream and Transport Stream shall be constructed so that the maximum difference between coded presentation timestamps referring to each elementary video or audio stream is 0,7 s. … In the case of still pictures the 0,7 s constraint does not apply.",
};

pub const MS_19_CONDITIONAL_TS: SpecQuote = SpecQuote {
    id: "MS-19",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.7.5 Conditional coding of timestamps",
    locator: "h222:6410-6429",
    url: H222_URL,
    text: "For each elementary stream of a Program Stream or Transport Stream, a presentation timestamp (PTS) shall be encoded for the first access unit. … A PTS may only be present in a ITU-T Rec. H.222.0 | ISO/IEC 13818-1 video or audio elementary stream PES packet header if the first byte of a picture start code or the first byte of an audio access unit is contained in the PES packet. … A decoding_timestamp (DTS) shall appear in a PES packet header if and only if the following two conditions are met: • a PTS is present in the PES packet header; • the decoding time differs from the presentation time.",
};

pub const MS_20_SCALABLE_AUDIO_PTS: SpecQuote = SpecQuote {
    id: "MS-20",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.7.6 Timing constraints for scalable coding",
    locator: "h222:6433-6434",
    url: H222_URL,
    text: "If an audio sequence is coded using an ISO/IEC 13818-3 extension bitstream, corresponding decoding/presentation units in the two layers shall have identical PTS values.",
};

pub const MS_21_PSTD_FIELDS_FIRST_PES: SpecQuote = SpecQuote {
    id: "MS-21",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.7.7; §2.7.8",
    locator: "h222:6464-6470",
    url: H222_URL,
    text: "In a Program Stream, the P-STD_buffer_scale and P-STD_buffer_size fields shall occur in the first PES packet of each elementary stream and again whenever the value changes. They may also occur in any other PES packet. … The system header shall be present in the first pack of an Program Stream. The values encoded in all the system headers in the Program Stream shall be identical.",
};

pub const MS_22_STREAM_TYPES: SpecQuote = SpecQuote {
    id: "MS-22",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.4.10 Table 2-29 Stream type assignments",
    locator: "h222:4139-4149",
    url: H222_URL,
    text: "0x01 ISO/IEC 11172 Video 0x02 ITU-T Rec. H.262 | ISO/IEC 13818-2 Video or ISO/IEC 11172-2 constrained parameter video stream 0x03 ISO/IEC 11172 Audio 0x04 ISO/IEC 13818-3 Audio … 0x06 ITU-T Rec. H.222.0 | ISO/IEC 13818-1 PES packets containing private data",
};

pub const MS_23_HIERARCHY: SpecQuote = SpecQuote {
    id: "MS-23",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.6.6 Table 2-43 Hierarchy descriptor; Table 2-44; §2.6.7",
    locator: "h222:5340-5384",
    url: H222_URL,
    text: "hierarchy_embedded_layer_index – The hierarchy_embedded_layer_index is a 6-bit field that defines the hierarchy table index of the program element that needs to be accessed before decoding of the elementary stream associated with this hierarchy_descriptor. This field is undefined if the hierarchy_type value is 15 (base layer). … hierarchy_descriptor() { descriptor_tag 8 uimsbf descriptor_length 8 uimsbf reserved 4 bslbf hierarchy_type 4 uimsbf reserved 2 bslbf hierarchy_layer_index 6 uimsbf reserved 2 bslbf hierarchy_embedded_layer_index 6 uimsbf reserved 2 bslbf hierarchy_channel 6 uimsbf } … 5 ISO/IEC 13818-3 Extension bitstream … 15 Base layer",
};

pub const MS_24_ISO_639: SpecQuote = SpecQuote {
    id: "MS-24",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.6.18 Table 2-52 ISO 639 language descriptor",
    locator: "h222:5640-5647",
    url: H222_URL,
    text: "ISO_639_language_descriptor() { descriptor_tag 8 uimsbf descriptor_length 8 uimsbf for (i = 0; i < N; i++) { ISO_639_language_code 24 bslbf audio_type 8 bslbf } }",
};

pub const MS_25_DESCRIPTOR_TAGS: SpecQuote = SpecQuote {
    id: "MS-25",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.6.1 Table 2-39 Program and program element descriptors",
    locator: "h222:5158-5210",
    url: H222_URL,
    text: "4 X X hierarchy_descriptor … 10 X X ISO_639_language_descriptor … 64-255 n/a n/a User Private",
};

pub const MS_26_CRC_POLYNOMIAL: SpecQuote = SpecQuote {
    id: "MS-26",
    kind: QuoteKind::Normative,
    source: H222,
    section: "Annex A.0 CRC decoder model, equation A-1",
    locator: "h222:7507-7509",
    url: H222_URL,
    text: "This is the CRC calculated with the polynomial: x32 + x26 + x23 + x22 + x16 + x12 + x11 + x10 + x8 + x7 + x5 + x4 + x2 + x + 1",
};

pub const MS_27_STREAM_IDS: SpecQuote = SpecQuote {
    id: "MS-27",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.7 Table 2-18 Stream_id assignments",
    locator: "h222:3159-3166",
    url: H222_URL,
    text: "1011 1100 1 program_stream_map 1011 1101 2 private_stream_1 1011 1110 padding_stream … 110x xxxx ISO/IEC 13818-3 or ISO/IEC 11172-3 or ISO/IEC 13818-7 or ISO/IEC 14496-3 audio stream number x xxxx 1110 xxxx ITU-T Rec. H.262 | ISO/IEC 13818-2 or ISO/IEC 11172-2 or ISO/IEC 14496-2 video stream number xxxx",
};

pub const MS_28_PRIVATE_DESCRIPTOR: SpecQuote = SpecQuote {
    id: "MS-28",
    kind: QuoteKind::Informative,
    source: H222,
    section: "Annex H (Informative) Private data",
    locator: "h222:9779-9782",
    url: H222_URL,
    text: "A range of private descriptors may be defined by the user. These descriptors shall commence with descriptor_tag and descriptor_length fields. … These descriptors may be placed within a program_stream_map()",
};

pub const MS_29_PRIVATE_STREAM_1_HEADERS: SpecQuote = SpecQuote {
    id: "MS-29",
    kind: QuoteKind::Corroboration,
    source: FFMPEG_MPEGENC,
    section: "write_packet(): the private_stream_1 sub-stream header (sub-id; AC-3/DTS frame count and first access unit pointer; LPCM header)",
    locator: "mpegenc.c:913-927",
    url: FFMPEG_MPEGENC_URL,
    text: "if (startcode == PRIVATE_STREAM_1) { avio_w8(ctx->pb, id); if (id >= 0xa0) { /* LPCM (XXX: check nb_frames) */ avio_w8(ctx->pb, 7); avio_wb16(ctx->pb, 4); /* skip 3 header bytes */ avio_w8(ctx->pb, stream->lpcm_header[0]); avio_w8(ctx->pb, stream->lpcm_header[1]); avio_w8(ctx->pb, stream->lpcm_header[2]); } else if (id >= 0x40) { /* AC-3 */ avio_w8(ctx->pb, nb_frames); avio_wb16(ctx->pb, trailer_size + 1); } }",
};

pub const MS_30_PCM_DVD_HEADER: SpecQuote = SpecQuote {
    id: "MS-30",
    kind: QuoteKind::Corroboration,
    source: FFMPEG_MPEGENC,
    section: "mpeg_mux_init(): the pcm_dvd (DVD LPCM) audio header",
    locator: "mpegenc.c:405-409",
    url: FFMPEG_MPEGENC_URL,
    text: "stream->lpcm_header[0] = 0x0c; stream->lpcm_header[1] = (freq << 4) | (((st->codecpar->bits_per_coded_sample - 16) / 4) << 6) | st->codecpar->ch_layout.nb_channels - 1; stream->lpcm_header[2] = 0x80;",
};

pub const MS_31_PES_HEADER_STREAM_IDS: SpecQuote = SpecQuote {
    id: "MS-31",
    kind: QuoteKind::Normative,
    source: H222,
    section: "§2.4.3.6 Table 2-17 PES packet",
    locator: "h222:2973-2982",
    url: H222_URL,
    text: "if (stream_id != program_stream_map && stream_id != padding_stream && stream_id != private_stream_2 && stream_id != ECM && stream_id != EMM && stream_id != program_stream_directory && stream_id != DSMCC_stream && stream_id != ITU-T Rec. H.222.1 type E stream) { '10' 2 bslbf PES_scrambling_control 2 bslbf",
};

/// Every `MS-n` quote, in ID order.
pub const ALL: &[&SpecQuote] = &[
    &MS_1_PACK,
    &MS_2_PACK_HEADER,
    &MS_3_MUX_RATE,
    &MS_4_SCR,
    &MS_5_SYSTEM_HEADER,
    &MS_6_SYSTEM_HEADER_FIELDS,
    &MS_7_BUFFER_BOUND,
    &MS_8_PSM,
    &MS_9_CRC,
    &MS_10_PES_PACKET,
    &MS_11_PES_EXTENSION,
    &MS_12_PES_LENGTH,
    &MS_13_PSTD_SCALE,
    &MS_14_PRIVATE_DATA,
    &MS_15_PTS_NAMES_FIRST_AU,
    &MS_16_PSTD_BUFFERS,
    &MS_17_SCR_FREQUENCY,
    &MS_18_PTS_FREQUENCY,
    &MS_19_CONDITIONAL_TS,
    &MS_20_SCALABLE_AUDIO_PTS,
    &MS_21_PSTD_FIELDS_FIRST_PES,
    &MS_22_STREAM_TYPES,
    &MS_23_HIERARCHY,
    &MS_24_ISO_639,
    &MS_25_DESCRIPTOR_TAGS,
    &MS_26_CRC_POLYNOMIAL,
    &MS_27_STREAM_IDS,
    &MS_28_PRIVATE_DESCRIPTOR,
    &MS_29_PRIVATE_STREAM_1_HEADERS,
    &MS_30_PCM_DVD_HEADER,
    &MS_31_PES_HEADER_STREAM_IDS,
];

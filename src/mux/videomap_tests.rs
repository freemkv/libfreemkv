use super::*;
use crate::disc::{
    Codec, ColorSpace, ContentFormat, FrameRate, HdrFormat, Resolution, VideoStream,
};

fn video_title(codec: Codec, res: Resolution, fr: FrameRate, cs: ColorSpace) -> DiscTitle {
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec,
        resolution: res,
        frame_rate: fr,
        hdr: HdrFormat::Sdr,
        color_space: cs,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.content_format = ContentFormat::BdTs;
    t
}

// An explicit aspect wins; SD without one is unknown; HD falls back to the coded size.
#[test]
fn display_aspect_ratio_falls_back_by_resolution() {
    let t = video_title(
        Codec::Hevc,
        Resolution::R1080p,
        FrameRate::F24,
        ColorSpace::Bt709,
    );
    let DiscStream::Video(mut v) = t.streams[0].clone() else {
        unreachable!()
    };
    assert_eq!(display_aspect_ratio(&v, 1920, 1080), (1920, 1080));
    assert_eq!(display_aspect_ratio(&v, 0, 0), (0, 1));
    v.display_aspect = Some((16, 9));
    assert_eq!(display_aspect_ratio(&v, 1920, 1080), (16, 9));
    v.display_aspect = None;
    v.resolution = Resolution::R576i;
    assert_eq!(display_aspect_ratio(&v, 720, 576), (0, 1));
}

fn src(medium: Medium, path: &str, title: usize) -> SourceInfo {
    SourceInfo {
        medium,
        path: path.to_string(),
        title,
        ..Default::default()
    }
}

fn vframe(coding: Option<PictureInfo>, pts: i64, source: Option<SourcePos>) -> PesFrame {
    let keyframe = coding.map(|c| c.keyframe()).unwrap_or(false);
    PesFrame {
        discard_padding_ns: 0,
        track: 0,
        pts,
        keyframe,
        data: vec![0u8; 4],
        duration_ns: None,
        source,
        coding,
    }
}

use crate::mux::codec::coding::Mpeg2Coding;

/// An interlaced (tff) MPEG-2 frame picture of the given coding type.
fn mpeg2_pic(ct: CodingType) -> PictureInfo {
    PictureInfo::mpeg2(
        ct,
        Mpeg2Coding {
            top_field_first: true,
            repeat_first_field: false,
            progressive_frame: false,
            progressive_sequence: false,
            frame_picture: true,
        },
    )
}

/// A canonical I-picture fixture (interlaced frame).
fn i_picture() -> PictureInfo {
    mpeg2_pic(CodingType::I)
}

#[test]
fn colour_maps_cicp_code_points() {
    assert_eq!(
        Colour::from_color_space(ColorSpace::Bt709),
        Colour {
            primaries: 1,
            transfer: 1,
            matrix: 1,
            full_range: false
        }
    );
    assert_eq!(
        Colour::from_color_space(ColorSpace::Bt2020),
        Colour {
            primaries: 9,
            transfer: 14,
            matrix: 9,
            full_range: false
        }
    );
    assert_eq!(Colour::from_color_space(ColorSpace::Unknown).primaries, 2);
}

// Regression: FVI colour must mirror the MKV muxer's precedence, not blindly map
// `color_space` → SDR transfer 14 for BT.2020.
#[test]
fn fvi_colour_follows_hdr_and_measured_cicp() {
    use crate::disc::MeasuredCicp;
    let mk = |hdr: HdrFormat, cs: ColorSpace, cicp: Option<MeasuredCicp>| VideoStream {
        pid: 0x1011,
        codec: Codec::Hevc,
        resolution: Resolution::R2160p,
        frame_rate: FrameRate::F23_976,
        hdr,
        color_space: cs,
        display_aspect: None,
        secondary: false,
        label: String::new(),
        measured_cicp: cicp,
    };

    // HDR10 BT.2020 with NO measured CICP → PQ transfer (16), NOT SDR 14.
    let c = Colour::from_video(&mk(HdrFormat::Hdr10, ColorSpace::Bt2020, None));
    assert_eq!(
        c,
        Colour {
            primaries: 9,
            transfer: 16, // PQ — not the SDR 14 the enum alone would give
            matrix: 9,
            full_range: false,
        }
    );

    // HLG BT.2020 → transfer 18.
    let c = Colour::from_video(&mk(HdrFormat::Hlg, ColorSpace::Bt2020, None));
    assert_eq!(c.transfer, 18, "HLG transfer must be 18");

    // Measured CICP is authoritative — copied through verbatim, incl. full
    // range (2 → full_range = true), ignoring the coarse enum/HDR guess.
    let measured = MeasuredCicp {
        matrix: 9,
        transfer: 16,
        primaries: 9,
        range: 2,
    };
    let c = Colour::from_video(&mk(HdrFormat::Sdr, ColorSpace::Bt709, Some(measured)));
    assert_eq!(
        c,
        Colour {
            primaries: 9,
            transfer: 16,
            matrix: 9,
            full_range: true,
        },
        "measured CICP must override the coarse color_space enum"
    );

    // Unknown colorimetry, SDR, no measured CICP → all code points map to
    // "unspecified" (2), matching `from_color_space(Unknown)`. Both sinks of
    // one title must emit 2, never 0.
    let c = Colour::from_video(&mk(HdrFormat::Sdr, ColorSpace::Unknown, None));
    assert_eq!(
        c,
        Colour {
            primaries: 2,
            transfer: 2,
            matrix: 2,
            full_range: false,
        },
        "Unknown colorimetry must emit CICP 'unspecified' (2), not 0"
    );
}

#[test]
fn type_label_full_and_codec_agnostic_fallback() {
    // coding present: full I/P/B from the agnostic coding_type().
    let mk = |ct| Some(mpeg2_pic(ct));
    assert_eq!(type_label(mk(CodingType::I), false), "I");
    assert_eq!(type_label(mk(CodingType::P), false), "P");
    assert_eq!(type_label(mk(CodingType::B), false), "B");
    // coding-type-only codec still reports its type.
    assert_eq!(
        type_label(Some(PictureInfo::coding_type_only(CodingType::B)), false),
        "B"
    );
    // No coding (audio/synthetic): degrade to I-vs-non-I from keyframe.
    assert_eq!(type_label(None, true), "I");
    assert_eq!(type_label(None, false), "P");
}

#[test]
fn field_order_label_omitted_when_unmeasured() {
    // MPEG-2 interlaced tff frame → "tff".
    assert_eq!(field_order_label(Some(i_picture())), Some("tff"));
    // Progressive frame → "progressive".
    let prog = PictureInfo::mpeg2(
        CodingType::I,
        Mpeg2Coding {
            top_field_first: true,
            repeat_first_field: false,
            progressive_frame: true,
            progressive_sequence: false,
            frame_picture: true,
        },
    );
    assert_eq!(field_order_label(Some(prog)), Some("progressive"));
    // Coding-type-only codec did not measure field order → None (omitted).
    assert_eq!(
        field_order_label(Some(PictureInfo::coding_type_only(CodingType::I))),
        None
    );
    // No coding at all → None.
    assert_eq!(field_order_label(None), None);
}

#[test]
fn is_random_access_codec_agnostic() {
    // For EVERY codec the frame keyframe flag IS the random-access signal.
    assert!(is_random_access(Some(i_picture()), true));
    // An I-picture whose frame flag is clear is NOT promoted — `key` follows
    // the frame's keyframe flag, never fabricated GOP-closure.
    assert!(!is_random_access(Some(i_picture()), false));
    // P/B with the flag clear → never.
    assert!(!is_random_access(Some(mpeg2_pic(CodingType::P)), false));
    // No coding: the frame keyframe flag IS the RAP signal.
    assert!(is_random_access(None, true));
    assert!(!is_random_access(None, false));
}

#[test]
fn fvi_codec_ids_use_bitstream_names() {
    assert_eq!(fvi_codec_id(Codec::Mpeg2), "mpeg2video");
    assert_eq!(fvi_codec_id(Codec::Mpeg1), "mpeg1video");
    assert_eq!(fvi_codec_id(Codec::H264), "h264");
    assert_eq!(fvi_codec_id(Codec::Hevc), "hevc");
    assert_eq!(fvi_codec_id(Codec::Vc1), "vc1");
}

// Scan follows the resolution: progressive formats must not read interlaced.
#[test]
fn header_scan_is_progressive_for_progressive_video() {
    for res in [Resolution::R1080p, Resolution::R2160p] {
        let t = video_title(Codec::Hevc, res, FrameRate::F23_976, ColorSpace::Bt709);
        let h = MapHeader::from_title(&t, src(Medium::Iso, "iso://x.iso", 1));
        assert_eq!(h.stream.scan, Scan::Progressive, "{res:?}");
    }
    let t = video_title(
        Codec::H264,
        Resolution::R1080i,
        FrameRate::F25,
        ColorSpace::Bt709,
    );
    let h = MapHeader::from_title(&t, src(Medium::Iso, "iso://x.iso", 1));
    assert_eq!(h.stream.scan, Scan::Interlaced);
}

#[test]
fn header_from_title_pulls_video_facts() {
    let t = video_title(
        Codec::Mpeg2,
        Resolution::R576i,
        FrameRate::F25,
        ColorSpace::Bt470bg,
    );
    let h = MapHeader::from_title(&t, src(Medium::Iso, "iso://x.iso", 2));
    assert_eq!(h.stream.codec, "mpeg2video");
    assert_eq!((h.stream.width, h.stream.height), (720, 576));
    assert_eq!(h.stream.dar, (0, 1)); // SD with no aspect: unknown
    assert_eq!(h.stream.frame_rate, (25, 1));
    assert_eq!(h.stream.scan, Scan::Interlaced);
    assert_eq!(h.stream.colour.matrix, 5);
    assert_eq!(h.source.path, "iso://x.iso");
    assert_eq!(h.source.title, 2);
    assert_eq!(h.source.medium, Medium::Iso);
}

#[test]
fn header_audio_only_title_is_neutral_not_panic() {
    let t = DiscTitle::empty();
    let h = MapHeader::from_title(&t, SourceInfo::default());
    assert_eq!(h.stream.codec, "unknown");
    assert_eq!((h.stream.width, h.stream.height), (0, 0));
}

#[test]
fn append_frame_numbers_records_in_order() {
    let t = video_title(
        Codec::Mpeg2,
        Resolution::R1080p,
        FrameRate::F23_976,
        ColorSpace::Bt709,
    );
    let mut map = VideoMap::new(&t, SourceInfo::default());
    map.append_frame(&vframe(
        Some(i_picture()),
        0,
        Some(SourcePos::at_byte(2048)),
    ));
    map.append_frame(&vframe(
        Some(mpeg2_pic(CodingType::B)),
        42,
        Some(SourcePos::at_byte(4096)),
    ));
    assert_eq!(map.records().len(), 2);
    assert_eq!(map.records()[0].n, 0);
    assert_eq!(map.records()[1].n, 1);
    assert_eq!(map.records()[0].source.unwrap().sector, 1);
    assert_eq!(map.records()[1].pts_ns, Some(42));
}

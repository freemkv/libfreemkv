use super::*;

#[test]
fn probe_video_reports_the_sequence_header_aspect() {
    // 720x576, aspect code 3 (16:9), frame-rate code 3 (25 fps).
    let es = [0, 0, 1, 0xB3, 0x2D, 0x02, 0x40, 0x33, 0, 0];
    let (_, res, _, dar, _) = probe_video(&es, Some(0x02)).expect("video");
    assert_eq!(res, Resolution::R576p);
    assert_eq!(dar, Some((16, 9)));
}

#[test]
fn square_pixel_code_uses_the_coded_size() {
    // 320x240, aspect code 1 (square pixels), frame-rate code 4.
    let es = [0, 0, 1, 0xB3, 0x14, 0x00, 0xF0, 0x14, 0, 0];
    let (_, _, _, dar, _) = probe_video(&es, Some(0x02)).expect("video");
    assert_eq!(dar, Some((320, 240)));
}

#[test]
fn mpeg1_pel_aspect_code_is_not_a_display_ratio() {
    // MPEG-1 352x288, code 3 is a pel aspect ratio (11172-2), not 16:9.
    let es = [0, 0, 1, 0xB3, 0x16, 0x01, 0x20, 0x33, 0, 0];
    let (_, _, _, dar, _) = probe_video(&es, Some(0x01)).expect("video");
    assert_eq!(dar, None);
}

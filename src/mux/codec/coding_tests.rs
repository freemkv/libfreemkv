use super::*;

fn mpeg2(
    ct: CodingType,
    tff: bool,
    rff: bool,
    prog_frame: bool,
    prog_seq: bool,
    frame_pic: bool,
) -> PictureInfo {
    PictureInfo::mpeg2(
        ct,
        Mpeg2Coding {
            top_field_first: tff,
            repeat_first_field: rff,
            progressive_frame: prog_frame,
            progressive_sequence: prog_seq,
            frame_picture: frame_pic,
        },
    )
}

#[test]
fn coding_type_accessor_returns_stored_type() {
    assert_eq!(
        mpeg2(CodingType::I, true, false, false, false, true).coding_type(),
        CodingType::I
    );
    assert_eq!(
        mpeg2(CodingType::B, true, false, false, false, true).coding_type(),
        CodingType::B
    );
}

#[test]
fn keyframe_only_for_intra() {
    assert!(mpeg2(CodingType::I, true, false, false, false, true).keyframe());
    assert!(!mpeg2(CodingType::P, true, false, false, false, true).keyframe());
    assert!(!mpeg2(CodingType::B, true, false, false, false, true).keyframe());
}

#[test]
fn mpeg2_field_order_tff_when_top_field_first() {
    // Interlaced frame picture, tff set → top-field-first.
    assert_eq!(
        mpeg2(CodingType::I, true, false, false, false, true).field_order(),
        Some(FieldOrder::Tff)
    );
}

#[test]
fn mpeg2_field_order_bff_when_not_top_field_first() {
    // Interlaced frame picture, tff clear → bottom-field-first.
    assert_eq!(
        mpeg2(CodingType::I, false, false, false, false, true).field_order(),
        Some(FieldOrder::Bff)
    );
}

#[test]
fn mpeg2_field_order_progressive_for_progressive_frame() {
    assert_eq!(
        mpeg2(CodingType::I, true, false, true, false, true).field_order(),
        Some(FieldOrder::Progressive)
    );
    // Progressive sequence likewise.
    assert_eq!(
        mpeg2(CodingType::I, true, false, false, true, true).field_order(),
        Some(FieldOrder::Progressive)
    );
}

#[test]
fn mpeg2_nb_fields_normal_and_telecine() {
    // Normal interlaced frame: 2 fields.
    assert_eq!(
        mpeg2(CodingType::P, true, false, false, false, true).nb_fields(),
        2
    );
    // NTSC 2:3 soft telecine (interlaced seq, progressive frame, rff): 3.
    assert_eq!(
        mpeg2(CodingType::P, false, true, true, false, true).nb_fields(),
        3
    );
    // Field picture: 1 field.
    assert_eq!(
        mpeg2(CodingType::P, false, false, false, false, false).nb_fields(),
        1
    );
    // Progressive sequence, rff + tff: 6.
    assert_eq!(
        mpeg2(CodingType::P, true, true, false, true, true).nb_fields(),
        6
    );
    // Progressive sequence, rff no tff: 4.
    assert_eq!(
        mpeg2(CodingType::P, false, true, false, true, true).nb_fields(),
        4
    );
}

#[test]
fn mpeg2_nb_fields_rff_on_interlaced_frame_is_two() {
    // Spec-forbidden rff on a non-progressive interlaced frame: treated as 2.
    for tff in [false, true] {
        assert_eq!(
            mpeg2(CodingType::P, tff, true, false, false, true).nb_fields(),
            2
        );
    }
}

#[test]
fn mpeg2_field_picture_reports_pair_order_not_progressive() {
    // Field picture: order comes from the stored pair order, even when the
    // progressive flags are set.
    for (prog_frame, prog_seq) in [(false, false), (true, false), (false, true)] {
        assert_eq!(
            mpeg2(CodingType::P, true, false, prog_frame, prog_seq, false).field_order(),
            Some(FieldOrder::Tff)
        );
        assert_eq!(
            mpeg2(CodingType::P, false, false, prog_frame, prog_seq, false).field_order(),
            Some(FieldOrder::Bff)
        );
    }
}

#[test]
fn mpeg2_progressive_accessor() {
    assert_eq!(
        mpeg2(CodingType::I, true, false, true, false, true).progressive(),
        Some(true)
    );
    assert_eq!(
        mpeg2(CodingType::I, true, false, false, false, true).progressive(),
        Some(false)
    );
}

#[test]
fn coding_type_only_reports_unknown_field_and_progressive() {
    let p = PictureInfo::coding_type_only(CodingType::P);
    assert_eq!(p.coding_type(), CodingType::P);
    assert_eq!(p.field_order(), None);
    assert_eq!(p.progressive(), None);
    // No pulldown signalling for these codecs → normal 2-field frame.
    assert_eq!(p.nb_fields(), 2);
}

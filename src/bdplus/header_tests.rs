use super::*;

// Cast Away (2000)'s `BDSVM/00000.svm` header, as read from the disc.
const CAST_AWAY: [u8; 24] = [
    0x42, 0x44, 0x53, 0x56, 0x4d, 0x5f, 0x43, 0x43, 0x01, 0x00, 0x00, 0x00, 0x01, 0x07, 0xd7, 0x07,
    0x1f, 0x00, 0x00, 0x00, 0x00, 0x07, 0xfa, 0xc4,
];

#[test]
fn parses_the_cast_away_header() {
    let h = SvmHeader::parse(&CAST_AWAY).unwrap();
    assert!(h.magic_ok);
    assert_eq!(h.raw, CAST_AWAY);
    assert_eq!(h.format_bytes, [1, 0, 0, 0, 1]);
    assert_eq!((h.year, h.month, h.day), (2007, 7, 31));
    assert_eq!(h.payload_len, 0x0007_FAC4);
    assert_eq!(h.estimated_generation(), 1);
    assert!(h.is_well_formed());
}

#[test]
fn short_input_is_none_and_a_wrong_magic_still_parses() {
    assert_eq!(SvmHeader::parse(&CAST_AWAY[..23]), None);
    let mut b = CAST_AWAY.to_vec();
    b[0] = b'X';
    b.extend_from_slice(&[0xAA; 100]);
    let h = SvmHeader::parse(&b).unwrap();
    assert!(!h.magic_ok);
    assert!(!h.is_well_formed(), "present but malformed");
    assert_eq!(
        h.payload_len, 0x0007_FAC4,
        "only the first 24 bytes are read"
    );
}

#[test]
fn generation_boundaries_fall_on_the_first_day() {
    let cases = [
        ((2007, 6, 7), 0),
        ((2007, 6, 8), 1),
        ((2007, 12, 19), 1),
        ((2007, 12, 20), 2),
        ((2008, 4, 13), 2),
        ((2008, 4, 14), 3),
        ((2008, 8, 22), 4),
        ((2009, 2, 2), 5),
        ((2009, 3, 31), 6),
        ((2009, 8, 18), 7),
        ((2010, 3, 5), 7),
        ((2010, 3, 6), 8),
        ((2010, 9, 3), 9),
        ((2011, 2, 28), 10),
        ((2011, 6, 1), 11),
        ((2011, 11, 15), 11),
        // Generations 12 and 13 start together: the later one wins.
        ((2011, 11, 16), 13),
        ((2012, 8, 7), 14),
        ((2013, 1, 25), 15),
        ((2013, 4, 29), 15),
        ((2013, 4, 30), 16),
        ((2030, 1, 1), 16),
        ((0, 0, 0), 0),
    ];
    for ((y, m, d), want) in cases {
        assert_eq!(estimated_generation(y, m, d), want, "{y}-{m}-{d}");
    }
    assert!((0..=31).all(|d| estimated_generation(2011, 12, d) != 12));
}

#[test]
fn malformed_dates_and_lengths_are_flagged() {
    let with = |at: usize, v: &[u8]| {
        let mut b = CAST_AWAY;
        b[at..at + v.len()].copy_from_slice(v);
        SvmHeader::parse(&b).unwrap().is_well_formed()
    };
    assert!(!with(0x0F, &[13]));
    assert!(!with(0x10, &[0]));
    assert!(!with(0x14, &[0x00, 0x40, 0x00, 0x00]));
    assert!(with(0x14, &[0x00, 0x3F, 0xFF, 0xFF]));
}

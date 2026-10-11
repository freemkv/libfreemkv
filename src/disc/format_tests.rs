use super::*;

const ALL: [DiscFormat; 6] = [
    DiscFormat::Uhd,
    DiscFormat::Fmts,
    DiscFormat::BluRay,
    DiscFormat::HdDvd,
    DiscFormat::Dvd,
    DiscFormat::Unknown,
];

// The ids are persisted by the server (staging, history); a change orphans state.
#[test]
fn id_is_the_persisted_slug() {
    let ids: Vec<&str> = ALL.iter().map(|f| f.id()).collect();
    assert_eq!(ids, ["uhd", "fmts", "bluray", "hddvd", "dvd", "unknown"]);
}

#[test]
fn layers_follow_the_format_capacity() {
    for (fmt, sectors, want) in [
        (DiscFormat::Dvd, 2_298_496, 1),
        (DiscFormat::Dvd, 4_173_824, 2),
        (DiscFormat::Dvd, u32::MAX, 2),
        (DiscFormat::HdDvd, 7_300_000, 1),
        (DiscFormat::HdDvd, 8_000_000, 1),
        (DiscFormat::HdDvd, 14_600_000, 2),
        (DiscFormat::HdDvd, 16_000_001, 3),
        (DiscFormat::HdDvd, 22_000_000, 3),
        (DiscFormat::BluRay, 12_219_392, 1),
        (DiscFormat::BluRay, 12_500_001, 2),
        (DiscFormat::BluRay, 24_438_784, 2),
        (DiscFormat::Fmts, 40_000_000, 2),
        (DiscFormat::Uhd, 48_878_592, 3),
        (DiscFormat::Unknown, 12_500_000, 1),
        (DiscFormat::Unknown, u32::MAX, 2),
    ] {
        assert_eq!(fmt.layers(sectors), want, "{fmt:?} {sectors}");
    }
}

#[test]
fn ecc_block_is_32_for_bd_family_and_16_for_dvd_family() {
    let got: Vec<u16> = ALL.iter().map(|f| f.ecc_block_sectors()).collect();
    assert_eq!(got, [32, 32, 32, 16, 16, 32]);
}

#[test]
fn only_uhd_defaults_region_free() {
    for f in ALL {
        let want = if matches!(f, DiscFormat::Uhd | DiscFormat::Fmts) {
            DiscRegion::Free
        } else {
            DiscRegion::Unknown
        };
        assert_eq!(f.default_region(), want, "{f:?}");
    }
}

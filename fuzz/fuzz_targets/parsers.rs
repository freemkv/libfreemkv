#![no_main]
use libfreemkv::aacs::{inf, mkb::AacsVersion, segment, segment_key::SegmentKeyTable};
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    for major in 1..=2 {
        let version = AacsVersion::from_major(major);
        let _ = inf::parse_unit_key_ro(data, version);
        let _ = inf::parse_title_keys(data, version);
    }
    let _ = inf::parse_vtkf(data);
    let _ = inf::parse_content_cert(data);
    let _ = segment::parse_individual_segments(data);
    let _ = SegmentKeyTable::parse(data);
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = libfreemkv::parse_url(text);
    }
});

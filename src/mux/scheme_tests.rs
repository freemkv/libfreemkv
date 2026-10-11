use super::*;
use crate::mux::resolve::parse_url;

const URLS: [&str; 18] = [
    "disc://",
    "m2ts://a.m2ts",
    "mkv://a.mkv",
    "mp4://a.mp4",
    "mpg://a.mpg",
    "network://h:1",
    "stdio://",
    "iso://a.iso",
    "dir:///d",
    "null://",
    "demux:///d",
    "video:///d",
    "audio:///d",
    "sub:///d",
    "fvi://a.fvi",
    "chapters://a.xml",
    "json://a.json",
    "bogus",
];

#[test]
fn every_known_scheme_is_found_by_its_name() {
    for url in URLS {
        let info = parse_url(url).info();
        let found = SchemeInfo::by_name(info.name);
        if info.name == "unknown" {
            assert!(found.is_none());
        } else {
            assert_eq!(found, Some(info), "{url}");
            assert!(url.starts_with(&format!("{}://", info.name)), "{url}");
        }
    }
}

#[test]
fn the_derived_families_match_the_schemes_they_name() {
    let names = |pred: fn(&SchemeInfo) -> bool| -> Vec<&str> {
        URLS.iter()
            .map(|u| parse_url(u).info())
            .filter(|i| pred(i))
            .map(|i| i.name)
            .collect()
    };
    assert_eq!(
        names(SchemeInfo::is_track_output),
        ["demux", "video", "audio", "sub"]
    );
    assert_eq!(names(SchemeInfo::is_scheme_only_sink), ["stdio", "null"]);
    assert_eq!(
        names(|i| i.source == Some(SourceKind::Container)),
        ["m2ts", "mkv", "mp4", "mpg"]
    );
    assert_eq!(
        names(|i| i.source.is_none()),
        [
            "null", "demux", "video", "audio", "sub", "fvi", "chapters", "json", "unknown"
        ]
    );
    assert_eq!(names(|i| i.sink.is_none()), ["disc", "unknown"]);
}

#[test]
fn a_path_is_named_exactly_by_file_and_folder_targets() {
    for url in URLS {
        let parsed = parse_url(url);
        let named = matches!(parsed.info().target, UrlTarget::File | UrlTarget::Dir);
        assert_eq!(parsed.path().is_some(), named, "{url}");
    }
}

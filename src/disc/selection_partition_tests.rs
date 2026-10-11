use super::*;
use crate::disc::{Clip, ContentFormat};

const TICKS: u32 = 45_000;

fn title(id: u16, clips: &[(&str, u32, u32)]) -> DiscTitle {
    let ticks: u64 = clips
        .iter()
        .map(|c| u64::from(c.2.saturating_sub(c.1)))
        .sum();
    DiscTitle {
        selection_evidence: Default::default(),
        playlist: format!("{id:05}.mpls"),
        playlist_id: id,
        duration_secs: ticks as f64 / f64::from(TICKS),
        size_bytes: 1000,
        clips: clips
            .iter()
            .map(|&(clip, in_time, out_time)| Clip {
                clip_id: clip.into(),
                in_time,
                out_time,
                duration_secs: f64::from(out_time.saturating_sub(in_time)) / f64::from(TICKS),
                source_packets: 0,
                feed_span: None,
            })
            .collect(),
        streams: vec![],
        chapters: vec![],
        extents: vec![],
        content_format: ContentFormat::BdTs,
        codec_privates: vec![],
    }
}

// Synthetic title whose clips are given in whole seconds.
fn secs(id: u16, clips: &[(&str, u32, u32)]) -> DiscTitle {
    let ticks: Vec<_> = clips
        .iter()
        .map(|&(c, a, b)| (c, a * TICKS, b * TICKS))
        .collect();
    title(id, &ticks)
}

// Scan dump line `<playlist> | <clip>:<in>-<out>[*repeat] ...`, as printed for real discs.
fn scanned(line: &str) -> DiscTitle {
    let (id, clips) = line.split_once(" | ").unwrap();
    let mut out = Vec::new();
    for clip in clips.split_whitespace() {
        let (clip, repeat) = clip.split_once('*').unwrap_or((clip, "1"));
        let (name, range) = clip.split_once(':').unwrap();
        let (a, b) = range.split_once('-').unwrap();
        let interval = (name, a.parse().unwrap(), b.parse().unwrap());
        out.extend(std::iter::repeat_n(interval, repeat.parse().unwrap()));
    }
    title(id.parse().unwrap(), &out)
}

fn assert_unknown(titles: &[DiscTitle]) {
    for t in titles {
        assert_eq!(
            t.selection_evidence.episodes,
            EpisodeEvidence::Unknown,
            "{}",
            t.playlist
        );
    }
}

// Every title carries the same roster and count; `members` lists (playlist, ordinal).
fn assert_roster(titles: &[DiscTitle], roster: &str, members: &[(u16, usize)]) {
    for t in titles {
        let ordinal = members.iter().find(|m| m.0 == t.playlist_id).map(|m| m.1);
        assert_eq!(
            t.selection_evidence.episodes,
            EpisodeEvidence::Authored {
                roster: roster.into(),
                title_count: titles.len(),
                ordinal,
            },
            "{}",
            t.playlist
        );
    }
}

fn run(mut titles: Vec<DiscTitle>) -> Vec<DiscTitle> {
    annotate(&mut titles);
    titles
}

// Planet Earth II UHD disc 2, scanned with libfreemkv dev 99c306ad (title order as scanned).
const PLANET_EARTH_II_D2: &[&str] = &[
    "00163 | 00058:524280-143808671 00059:524280-139681423 00060:524280-141473838",
    "00147 | 00058:524280-143930667",
    "00166 | 00058:524280-143808671",
    "00149 | 00060:524280-141473838",
    "00168 | 00060:524280-141473838",
    "00148 | 00059:524280-139848464",
    "00167 | 00059:524280-139681423",
    "00162 | 00014:524280-2710839*301",
    "00099 | 00014:524280-2710839*100",
    "00150 | 00061:524280-1875629",
    "00153 | 00064:524280-1875629",
    "00154 | 00065:524280-1875629",
    "00155 | 00066:524280-1875629",
    "00156 | 00067:524280-1875629",
    "00097 | 00021:524280-1875629",
    "00098 | 00022:524280-1875629",
    "00161 | 00022:524280-1875629",
];

// Blue Planet II UHD, a different onQ build whose menu program the old certificate rejected.
const BLUE_PLANET_II: &[&str] = &[
    "00148 | 00059:524280-144195307",
    "00167 | 00059:524280-144195307",
    "00147 | 00058:524280-140910775",
    "00166 | 00058:524280-140910775",
    "00149 | 00060:524280-1875629",
    "00168 | 00060:524280-1875629",
    "00163 | 00058:524280-140910775 00059:524280-144195307",
];

#[test]
fn planet_earth_ii_feature_playlist_partitions_into_three_episodes() {
    let titles = run(PLANET_EARTH_II_D2.iter().map(|l| scanned(l)).collect());
    // 00149 is an exact alias of 00168; 00147/00148 differ at the out-point.
    assert_roster(
        &titles,
        "bd-playall-partition-v1:163",
        &[(166, 0), (167, 1), (168, 2), (149, 2)],
    );
}

#[test]
fn blue_planet_ii_feature_playlist_partitions_into_two_aliased_episodes() {
    let titles = run(BLUE_PLANET_II.iter().map(|l| scanned(l)).collect());
    assert_roster(
        &titles,
        "bd-playall-partition-v1:163",
        &[(166, 0), (147, 0), (167, 1), (148, 1)],
    );
}

#[test]
fn roster_label_names_the_lowest_aliased_wrapper_playlist() {
    let mut lines: Vec<_> = BLUE_PLANET_II.to_vec();
    lines.push("00170 | 00058:524280-140910775 00059:524280-144195307");
    lines.push("00009 | 00058:524280-140910775 00059:524280-144195307");
    let titles = run(lines.iter().map(|l| scanned(l)).collect());
    assert_roster(
        &titles,
        "bd-playall-partition-v1:9",
        &[(166, 0), (147, 0), (167, 1), (148, 1)],
    );
}

fn episodes() -> Vec<DiscTitle> {
    vec![
        secs(1, &[("00001", 0, 2700)]),
        secs(2, &[("00002", 0, 2700)]),
        secs(3, &[("00003", 0, 2700)]),
    ]
}

#[test]
fn nested_playalls_are_unknown() {
    let mut titles = episodes();
    titles.push(secs(10, &[("00001", 0, 2700), ("00002", 0, 2700)]));
    titles.push(secs(
        11,
        &[("00001", 0, 2700), ("00002", 0, 2700), ("00003", 0, 2700)],
    ));
    assert_unknown(&run(titles));
}

#[test]
fn shorter_playlist_sharing_wrapper_clips_competes() {
    let mut titles = episodes();
    titles.push(secs(
        10,
        &[("00001", 0, 2700), ("00002", 0, 2700), ("00003", 0, 2700)],
    ));
    // An extended episode 2 that reuses the wrapper's interval but is not a member.
    titles.push(secs(11, &[("00002", 0, 2700), ("00009", 0, 600)]));
    assert_unknown(&run(titles));
}

#[test]
fn extras_only_playall_is_unknown() {
    let mut titles = episodes();
    titles.push(secs(11, &[("00011", 0, 120)]));
    titles.push(secs(12, &[("00012", 0, 120)]));
    titles.push(secs(13, &[("00013", 0, 120)]));
    titles.push(secs(
        20,
        &[("00011", 0, 120), ("00012", 0, 120), ("00013", 0, 120)],
    ));
    assert_unknown(&run(titles));
}

#[test]
fn recap_member_is_unknown() {
    let mut titles = episodes();
    titles.push(secs(9, &[("00009", 0, 90)]));
    titles.push(secs(
        10,
        &[
            ("00009", 0, 90),
            ("00001", 0, 2700),
            ("00002", 0, 2700),
            ("00003", 0, 2700),
        ],
    ));
    assert_unknown(&run(titles));
}

#[test]
fn prefix_ambiguity_is_unknown() {
    let titles = vec![
        // Episode 1 with and without a trailing segment are both standalone programs.
        secs(1, &[("00001", 0, 2000), ("00002", 0, 100)]),
        secs(2, &[("00001", 0, 2000)]),
        secs(3, &[("00003", 0, 2100)]),
        secs(
            10,
            &[("00001", 0, 2000), ("00002", 0, 100), ("00003", 0, 2100)],
        ),
    ];
    assert_unknown(&run(titles));
}

#[test]
fn repeated_member_is_unknown() {
    let mut titles = episodes();
    titles.push(secs(
        10,
        &[("00001", 0, 2700), ("00002", 0, 2700), ("00001", 0, 2700)],
    ));
    assert_unknown(&run(titles));
}

#[test]
fn loop_playlist_is_not_a_wrapper() {
    let titles = vec![
        secs(1, &[("00002", 0, 1000)]),
        secs(2, &[("00001", 0, 1000), ("00002", 0, 1000)]),
        secs(
            3,
            &[("00002", 0, 1000), ("00001", 0, 1000), ("00002", 0, 1000)],
        ),
    ];
    assert_unknown(&run(titles));
}

#[test]
fn two_valid_wrappers_are_unknown() {
    let mut titles = episodes();
    titles.push(secs(4, &[("00004", 0, 2700)]));
    titles.push(secs(10, &[("00001", 0, 2700), ("00002", 0, 2700)]));
    titles.push(secs(11, &[("00003", 0, 2700), ("00004", 0, 2700)]));
    assert_unknown(&run(titles));
}

#[test]
fn single_member_wrapper_is_unknown() {
    let titles = vec![
        secs(10, &[("00001", 0, 2700), ("00002", 0, 2700)]),
        secs(11, &[("00001", 0, 2700), ("00002", 0, 2700)]),
    ];
    assert_unknown(&run(titles));
}

#[test]
fn invalid_member_presentation_is_unknown() {
    let titles = vec![
        secs(1, &[("00001", 0, 2700)]),
        secs(2, &[("", 0, 2700)]),
        secs(3, &[("00003", 0, 2700)]),
        secs(10, &[("00001", 0, 2700), ("", 0, 2700), ("00003", 0, 2700)]),
    ];
    assert_unknown(&run(titles));
}

// Padding that leaves Blue Planet II's partition intact.
fn short_extra(id: u16) -> DiscTitle {
    title(
        id,
        &[(&format!("{:05}", 1000 + u32::from(id)), 0, 30 * TICKS)],
    )
}

#[test]
fn title_cap_is_unknown() {
    let mut titles: Vec<_> = BLUE_PLANET_II.iter().map(|l| scanned(l)).collect();
    let pad = u16::try_from(MAX_TITLES + 1 - titles.len()).unwrap();
    titles.extend((1000..1000 + pad).map(short_extra));
    assert_eq!(titles.len(), MAX_TITLES + 1);
    assert_unknown(&run(titles));
}

#[test]
fn clip_cap_is_unknown() {
    let mut titles: Vec<_> = BLUE_PLANET_II.iter().map(|l| scanned(l)).collect();
    let used: usize = titles.iter().map(|t| t.clips.len()).sum();
    let menu = format!("00014:524280-2710839*{}", MAX_CLIPS + 1 - used);
    titles.push(scanned(&format!("00162 | {menu}")));
    assert_unknown(&run(titles));
}

#[test]
fn shared_prefixes_reject_empty_groups_and_self_decomposition() {
    let groups = vec![vec![1, 2], vec![1], vec![2], vec![]];
    assert!(Prefixes::new(&groups).is_none());
    let groups = vec![vec![1, 2], vec![1], vec![2]];
    let prefixes = Prefixes::new(&groups).unwrap();
    assert_eq!(prefixes.decompose(0), Some(vec![1, 2]));
    assert_eq!(prefixes.decompose(1), None);
    assert_eq!(prefixes.decompose(9), None);
}

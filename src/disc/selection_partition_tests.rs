use super::*;
use crate::disc::{Clip, ContentFormat, ContentSegment};

const TICKS: u32 = 45_000;

fn title(id: u16, clips: &[(&str, u32, u32)]) -> DiscTitle {
    let ticks: u64 = clips
        .iter()
        .map(|c| u64::from(c.2.saturating_sub(c.1)))
        .sum();
    let clips: Vec<_> = clips
        .iter()
        .map(|&(clip, in_time, out_time)| Clip {
            marks_on_pes_clock: true,
            clip_id: clip.into(),
            in_time,
            out_time,
            duration_secs: f64::from(out_time.saturating_sub(in_time)) / f64::from(TICKS),
            source_packets: 0,
            feed_span: None,
        })
        .collect();
    // As the Blu-ray scanner describes a playlist.
    DiscTitle {
        selection_evidence: crate::disc::TitleSelectionEvidence {
            segments: ContentSegment::from_clips(&clips),
            ..Default::default()
        },
        playlist: format!("{id:05}.mpls"),
        playlist_id: id,
        duration_secs: ticks as f64 / f64::from(TICKS),
        size_bytes: 1000,
        clips,
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
    annotate(
        &mut titles,
        "bd-playall-partition-v1",
        true,
        &[],
        u64::from(TICKS),
    );
    titles
}

// A three-episode UHD disc, scanned with libfreemkv dev 99c306ad (title order as scanned).
const THREE_EPISODE_UHD: &[&str] = &[
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

// Another pressing of the same disc (read from its mounted BDMV): the
// play-all's first piece ends 9384 ticks (~0.21 s) past standalone 00166.
const THREE_EPISODE_UHD_TRIMMED: &[&str] = &[
    "00097 | 00021:524280-1875629",
    "00098 | 00022:524280-1875629",
    "00099 | 00014:524280-2707085*100",
    "00147 | 00058:524280-146171656",
    "00148 | 00059:524280-139369861",
    "00149 | 00060:524280-147513622",
    "00150 | 00061:524280-1875629",
    "00153 | 00064:524280-1875629",
    "00154 | 00065:524280-1875629",
    "00155 | 00066:524280-1875629",
    "00156 | 00067:524280-1875629",
    "00161 | 00022:524280-1875629",
    "00162 | 00014:524280-2707085*301",
    "00163 | 00058:524280-138686679 00059:524280-139180297 00060:524280-139561303",
    "00166 | 00058:524280-138677295",
    "00167 | 00059:524280-139180297",
    "00168 | 00060:524280-139561303",
];

// A two-episode UHD disc, a different onQ build whose menu program the old certificate rejected.
const TWO_EPISODE_UHD: &[&str] = &[
    "00148 | 00059:524280-144195307",
    "00167 | 00059:524280-144195307",
    "00147 | 00058:524280-140910775",
    "00166 | 00058:524280-140910775",
    "00149 | 00060:524280-1875629",
    "00168 | 00060:524280-1875629",
    "00163 | 00058:524280-140910775 00059:524280-144195307",
];

#[test]
fn three_episode_feature_playlist_partitions_into_three_episodes() {
    let titles = run(THREE_EPISODE_UHD.iter().map(|l| scanned(l)).collect());
    // 00149 is an exact alias of 00168; 00147/00148 differ at the out-point.
    assert_roster(
        &titles,
        "bd-playall-partition-v1:163",
        &[(166, 0), (167, 1), (168, 2), (149, 2)],
    );
}

#[test]
fn a_play_all_piece_trimmed_by_frames_still_partitions() {
    let titles = run(THREE_EPISODE_UHD_TRIMMED
        .iter()
        .map(|l| scanned(l))
        .collect());
    // 00147/00148/00149 are longer cuts of the same clips, minutes past the out-point.
    assert_roster(
        &titles,
        "bd-playall-partition-v1:163",
        &[(166, 0), (167, 1), (168, 2)],
    );
}

#[test]
fn two_episode_feature_playlist_partitions_into_two_aliased_episodes() {
    let titles = run(TWO_EPISODE_UHD.iter().map(|l| scanned(l)).collect());
    assert_roster(
        &titles,
        "bd-playall-partition-v1:163",
        &[(166, 0), (147, 0), (167, 1), (148, 1)],
    );
}

#[test]
fn roster_label_names_the_lowest_aliased_wrapper_playlist() {
    let mut lines: Vec<_> = TWO_EPISODE_UHD.to_vec();
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

// Padding that leaves the two-episode disc's partition intact.
fn short_extra(id: u16) -> DiscTitle {
    title(
        id,
        &[(&format!("{:05}", 1000 + u32::from(id)), 0, 30 * TICKS)],
    )
}

#[test]
fn title_cap_is_unknown() {
    let mut titles: Vec<_> = TWO_EPISODE_UHD.iter().map(|l| scanned(l)).collect();
    let pad = u16::try_from(MAX_TITLES + 1 - titles.len()).unwrap();
    titles.extend((1000..1000 + pad).map(short_extra));
    assert_eq!(titles.len(), MAX_TITLES + 1);
    assert_unknown(&run(titles));
}

#[test]
fn clip_cap_is_unknown() {
    let mut titles: Vec<_> = TWO_EPISODE_UHD.iter().map(|l| scanned(l)).collect();
    let used: usize = titles.iter().map(|t| t.clips.len()).sum();
    let menu = format!("00014:524280-2710839*{}", MAX_SEGMENTS + 1 - used);
    titles.push(scanned(&format!("00162 | {menu}")));
    assert_unknown(&run(titles));
}

// Exactly at either cap the partition still runs; the `+ 1` tests above are past it.
fn two_episode_roster(titles: &[DiscTitle]) {
    let members = [(166, 0), (147, 0), (167, 1), (148, 1)];
    assert_roster(titles, "bd-playall-partition-v1:163", &members);
}

#[test]
fn title_cap_itself_keeps_the_roster() {
    let mut titles: Vec<_> = TWO_EPISODE_UHD.iter().map(|l| scanned(l)).collect();
    let pad = u16::try_from(MAX_TITLES - titles.len()).unwrap();
    titles.extend((1000..1000 + pad).map(short_extra));
    assert_eq!(titles.len(), MAX_TITLES);
    two_episode_roster(&run(titles));
}

#[test]
fn clip_cap_itself_keeps_the_roster() {
    let mut titles: Vec<_> = TWO_EPISODE_UHD.iter().map(|l| scanned(l)).collect();
    let used: usize = titles.iter().map(|t| t.clips.len()).sum();
    let menu = format!("00014:524280-2710839*{}", MAX_SEGMENTS - used);
    titles.push(scanned(&format!("00162 | {menu}")));
    let total: usize = (titles.iter())
        .map(|t| t.selection_evidence.segments.len())
        .sum();
    assert_eq!(total, MAX_SEGMENTS);
    two_episode_roster(&run(titles));
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

fn run_dvd(mut titles: Vec<DiscTitle>) -> Vec<DiscTitle> {
    annotate(&mut titles, "dvd-playall-partition-v1", true, &[], 0);
    titles
}

// A DVD title as its scanner describes it: cell sector ranges, no clips.
fn dvd(id: u16, cells: &[(u32, u32)]) -> DiscTitle {
    let extents: Vec<_> = cells
        .iter()
        .map(|&(first, last)| crate::disc::Extent {
            start_lba: first,
            sector_count: last - first + 1,
        })
        .collect();
    DiscTitle {
        selection_evidence: crate::disc::TitleSelectionEvidence {
            segments: ContentSegment::from_cell_extents(&extents),
            ..Default::default()
        },
        playlist: format!("VTS_01_{id}.VOB"),
        playlist_id: id,
        extents,
        content_format: ContentFormat::DvdPs,
        ..title(id, &[])
    }
}

// Six language alternates of three uneven programs, their play-all and an unrelated title.
fn dvd_alternates(cut: &[(u32, u32)]) -> Vec<DiscTitle> {
    let [a, b, c] = [cut[0], cut[1], cut[2]];
    vec![
        dvd(1, &[a]),
        dvd(2, &[a]),
        dvd(3, &[b]),
        dvd(4, &[b]),
        dvd(5, &[c]),
        dvd(6, &[c]),
        dvd(7, &[b, a, c]),
        dvd(8, &[(900, 939)]),
    ]
}

#[test]
fn dvd_cells_partition_like_clips() {
    let titles = run_dvd(dvd_alternates(&[(100, 129), (200, 239), (300, 319)]));
    assert_roster(
        &titles,
        "dvd-playall-partition-v1:7",
        &[(3, 0), (4, 0), (1, 1), (2, 1), (5, 2), (6, 2)],
    );
}

#[test]
fn dvd_member_far_shorter_than_its_share_is_unknown() {
    assert_unknown(&run_dvd(dvd_alternates(&[
        (100, 109),
        (200, 239),
        (300, 301),
    ])));
}

#[test]
fn dvd_shared_intro_is_a_repeat_so_unknown() {
    let intro = (50, 59);
    let titles = vec![
        dvd(1, &[intro, (100, 129)]),
        dvd(2, &[intro, (200, 239)]),
        dvd(3, &[intro, (300, 319)]),
        dvd(
            7,
            &[intro, (200, 239), intro, (100, 129), intro, (300, 319)],
        ),
    ];
    assert_unknown(&run_dvd(titles));
}

#[test]
fn dvd_title_without_segments_is_no_member() {
    let mut titles = dvd_alternates(&[(100, 129), (200, 239), (300, 319)]);
    titles[6].selection_evidence.segments.clear();
    assert_unknown(&run_dvd(titles));
}

// Sector ranges for a title, as the scanners record what it reads.
fn reading(mut t: DiscTitle, ranges: &[(u32, u32)]) -> DiscTitle {
    t.extents = ranges
        .iter()
        .map(|&(start_lba, sector_count)| crate::disc::Extent {
            start_lba,
            sector_count,
        })
        .collect();
    t
}

// Four episodes at sectors 100.., playlist 10 over the first two.
fn four_episodes_with_head() -> Vec<DiscTitle> {
    let mut titles: Vec<_> = (1..=4u32)
        .map(|n| {
            let clip = format!("{n:05}");
            let ep = secs(n as u16, &[(clip.as_str(), 0, 2700)]);
            reading(ep, &[(n * 100, 50)])
        })
        .collect();
    titles.push(reading(
        secs(10, &[("00001", 0, 2700), ("00002", 0, 2700)]),
        &[(100, 50), (200, 50)],
    ));
    titles
}

#[test]
fn bd_undescribable_playall_over_the_members_is_unknown() {
    let mut titles = four_episodes_with_head();
    // A zero-length final clip makes the longest playlist undescribable.
    titles.push(reading(
        secs(
            20,
            &[
                ("00001", 0, 2700),
                ("00002", 0, 2700),
                ("00003", 0, 2700),
                ("00004", 0, 2700),
                ("00009", 5, 5),
            ],
        ),
        &[(100, 50), (200, 50), (300, 50), (400, 50)],
    ));
    assert!(titles[5].selection_evidence.segments.is_empty());
    assert_unknown(&run(titles));
}

#[test]
fn dvd_undescribable_playall_over_the_members_is_unknown() {
    let cells = [(100, 149), (200, 249), (300, 349), (400, 449)];
    let mut titles: Vec<_> = cells
        .iter()
        .enumerate()
        .map(|(i, &c)| dvd(i as u16 + 1, &[c]))
        .collect();
    titles.push(dvd(10, &[cells[0], cells[1]]));
    // Random or shuffle play-all: segments withheld, extents still read.
    let mut playall = dvd(20, &cells);
    playall.selection_evidence.segments.clear();
    titles.push(playall);
    assert_unknown(&run_dvd(titles));
}

#[test]
fn undescribable_title_elsewhere_does_not_block_a_roster() {
    let mut titles = four_episodes_with_head();
    titles.push(reading(
        secs(30, &[("00030", 7, 7)]),
        &[(9000, 20), (9100, 20)],
    ));
    titles.push(secs(31, &[("00031", 7, 7)]));
    let titles = run(titles);
    assert_roster(&titles, "bd-playall-partition-v1:10", &[(1, 0), (2, 1)]);
}

// Episodes and their play-all lost their segments; the extras and their play-all kept
// theirs. The undescribed play-all is longer than the extras one, so it may be the real one.
#[test]
fn longer_undescribed_title_holds_an_extras_roster() {
    let episodes = [(100, 199), (200, 299), (300, 399)];
    let extras = [(1000, 1009), (1100, 1109), (1200, 1209)];
    let mut titles: Vec<_> = (episodes.iter().enumerate())
        .map(|(i, &c)| dvd(i as u16 + 1, &[c]))
        .collect();
    titles.push(dvd(10, &episodes));
    for t in &mut titles {
        t.selection_evidence.segments.clear();
    }
    titles.extend((extras.iter().enumerate()).map(|(i, &c)| dvd(i as u16 + 11, &[c])));
    titles.push(dvd(20, &extras));
    assert_unknown(&run_dvd(titles.clone()));
    // Shorter undescribed titles leave the extras partition standing.
    titles.truncate(3);
    titles.extend((extras.iter().enumerate()).map(|(i, &c)| dvd(i as u16 + 11, &[c])));
    titles.push(dvd(20, &extras));
    for t in &mut titles[..3] {
        t.extents[0].sector_count = 5;
    }
    let titles = run_dvd(titles);
    assert_roster(
        &titles,
        "dvd-playall-partition-v1:20",
        &[(11, 0), (12, 1), (13, 2)],
    );
}

// The same by running time, for an undescribed title with no sectors recorded.
#[test]
fn longer_undescribed_title_by_duration_holds_the_roster() {
    let mut titles = episodes();
    titles.push(secs(
        10,
        &[("00001", 0, 2700), ("00002", 0, 2700), ("00003", 0, 2700)],
    ));
    let mut feature = secs(30, &[("00030", 0, 9000)]);
    feature.selection_evidence.segments.clear();
    titles.push(feature);
    assert_unknown(&run(titles));
}

// An undescribed title is measured by all it can play: a short first PGC whose reach is
// longer, or unknown, holds the extras roster; a reach within it does not.
#[test]
fn an_undescribed_title_is_measured_by_its_reach() {
    let extras = [(1000, 1009), (1100, 1109), (1200, 1209)];
    let mut titles: Vec<_> = (extras.iter().enumerate())
        .map(|(i, &c)| dvd(i as u16 + 11, &[c]))
        .collect();
    titles.push(dvd(20, &extras));
    let mut first_pgc = dvd(1, &[(100, 104)]);
    first_pgc.selection_evidence.segments.clear();
    titles.insert(0, first_pgc);
    let with = |reach: &[Reach]| {
        let mut titles = titles.clone();
        annotate(&mut titles, "dvd-playall-partition-v1", true, reach, 0);
        titles
    };
    let at_most = |secs, sectors| Reach::AtMost {
        secs,
        sectors,
        beyond: Vec::new(),
    };
    assert_unknown(&with(&[Reach::Unknown]));
    assert_unknown(&with(&[at_most(0.0, 200)]));
    for reach in [&[][..], &[at_most(0.0, 30)]] {
        assert_roster(
            &with(reach),
            "dvd-playall-partition-v1:20",
            &[(11, 0), (12, 1), (13, 2)],
        );
    }
}

// An undescribed title's reach past its extents counts for the overlap rule too.
#[test]
fn an_undescribed_title_overlapping_the_roster_past_its_extents_holds_it() {
    let extras = [(1000, 1009), (1100, 1109), (1200, 1209)];
    let mut titles: Vec<_> = (extras.iter().enumerate())
        .map(|(i, &c)| dvd(i as u16 + 11, &[c]))
        .collect();
    titles.push(dvd(20, &extras));
    let mut first_pgc = dvd(1, &[(100, 104)]);
    first_pgc.selection_evidence.segments.clear();
    titles.insert(0, first_pgc);
    let with = |beyond: Vec<(u64, u64)>| {
        let mut titles = titles.clone();
        let reach = [Reach::AtMost {
            secs: 0.0,
            sectors: 20,
            beyond,
        }];
        annotate(&mut titles, "dvd-playall-partition-v1", true, &reach, 0);
        titles
    };
    assert_unknown(&with(vec![(1105, 1115)]));
    assert_roster(
        &with(vec![(1110, 1120)]),
        "dvd-playall-partition-v1:20",
        &[(11, 0), (12, 1), (13, 2)],
    );
}

/// Round-4 tests LOW: a reach longer in time alone, or in sectors alone, than the
/// 100 s, 30-sector play-all holds the roster.
#[test]
fn a_reach_longer_by_time_or_by_sectors_alone_holds_the_roster() {
    let extras = [(1000, 1009), (1100, 1109), (1200, 1209)];
    let mut titles: Vec<_> = (extras.iter().enumerate())
        .map(|(i, &c)| dvd(i as u16 + 11, &[c]))
        .collect();
    let mut playall = dvd(20, &extras);
    playall.duration_secs = 100.0;
    titles.push(playall);
    let mut first_pgc = dvd(1, &[(100, 104)]);
    first_pgc.selection_evidence.segments.clear();
    titles.insert(0, first_pgc);
    let with = |secs, sectors| {
        let mut titles = titles.clone();
        let reach = [Reach::AtMost {
            secs,
            sectors,
            beyond: Vec::new(),
        }];
        annotate(&mut titles, "dvd-playall-partition-v1", true, &reach, 0);
        titles
    };
    assert_unknown(&with(150.0, 10));
    assert_unknown(&with(50.0, 40));
    assert_roster(
        &with(50.0, 10),
        "dvd-playall-partition-v1:20",
        &[(11, 0), (12, 1), (13, 2)],
    );
}

// A two-piece play-all whose first piece ends `over` ticks past standalone title 2's.
fn trimmed_play_all(over: u32) -> Vec<DiscTitle> {
    let end = 3000 * TICKS;
    vec![
        title(1, &[("a", 0, end + over), ("b", 0, end)]),
        title(2, &[("a", 0, end)]),
        title(3, &[("b", 0, end)]),
    ]
}

#[test]
fn an_out_point_within_the_slack_is_the_same_piece() {
    let titles = run(trimmed_play_all(TICKS / 2));
    assert_roster(&titles, "bd-playall-partition-v1:1", &[(2, 0), (3, 1)]);
}

#[test]
fn an_out_point_past_the_slack_is_another_program() {
    assert_unknown(&run(trimmed_play_all(2 * TICKS)));
}

// Out-points 0.6 s apart chain past the slack: no one of them is proven the same piece.
#[test]
fn a_chain_of_near_out_points_wider_than_the_slack_stays_exact() {
    let mut titles = trimmed_play_all(TICKS * 6 / 10);
    titles.push(title(4, &[("a", 0, 3000 * TICKS + TICKS * 12 / 10)]));
    assert_unknown(&run(titles));
}

#[test]
fn dvd_sectors_have_no_slack() {
    let mut titles = trimmed_play_all(1);
    annotate(&mut titles, "dvd-playall-partition-v1", true, &[], 0);
    assert_unknown(&titles);
}

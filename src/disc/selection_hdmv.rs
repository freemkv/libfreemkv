//! Conservative HDMV authored-program evidence.
//!
//! This is intentionally not a general navigation interpreter. It accepts only
//! the bounded unconditional MovieObject roster and requires exact MPLS clip
//! interval corroboration before writing episode evidence.

use super::{DiscTitle, EpisodeEvidence};
use crate::{sector::SectorSource, udf::UdfFs};

fn intervals(title: &DiscTitle) -> Option<Vec<(String, u32, u32)>> {
    (!title.clips.is_empty())
        .then(|| {
            title
                .clips
                .iter()
                .map(|c| {
                    (!c.clip_id.is_empty() && c.in_time < c.out_time).then_some((
                        c.clip_id.clone(),
                        c.in_time,
                        c.out_time,
                    ))
                })
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
}

/// Annotate only a complete unconditional HDMV program roster. Unsupported
/// navigation leaves all titles `Unknown`.
pub(super) fn annotate(reader: &mut dyn SectorSource, udf: &UdfFs, titles: &mut [DiscTitle]) {
    let Some(order) = crate::bdnav::resolve_unconditional_roster(reader, udf) else {
        return;
    };
    annotate_roster(&order, titles);
}

fn annotate_roster(order: &[u16], titles: &mut [DiscTitle]) {
    let mut ids = std::collections::HashSet::new();
    if order.len() < 2 || order.iter().any(|id| !ids.insert(*id)) {
        return;
    }
    let Some(members) = order
        .iter()
        .map(|id| titles.iter().find(|t| t.playlist_id == *id))
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    let Some(member_intervals) = members
        .iter()
        .map(|t| intervals(t))
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    let concatenated: Vec<_> = member_intervals.into_iter().flatten().collect();
    let wrapper = titles.iter().find(|t| {
        !order.contains(&t.playlist_id) && intervals(t).is_some_and(|value| value == concatenated)
    });
    if wrapper.is_none() {
        return;
    }

    let roster = format!("hdmv-unconditional-playall-v1:{}", order.len());
    let title_count = titles.len();
    for title in titles {
        let ordinal = order.iter().position(|id| *id == title.playlist_id);
        title.selection_evidence.episodes = EpisodeEvidence::Authored {
            roster: roster.clone(),
            title_count,
            member: ordinal.is_some(),
            ordinal,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{Clip, ContentFormat};

    fn title(id: u16, intervals: &[(u16, u32, u32)]) -> DiscTitle {
        DiscTitle {
            selection_evidence: Default::default(),
            playlist: format!("{id:05}.mpls"),
            playlist_id: id,
            duration_secs: 100.0,
            size_bytes: 1000,
            clips: intervals
                .iter()
                .map(|&(clip, start, end)| Clip {
                    clip_id: format!("{clip:05}"),
                    in_time: start,
                    out_time: end,
                    duration_secs: f64::from(end - start) / 45000.0,
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

    fn fixture() -> Vec<DiscTitle> {
        vec![
            title(103, &[(3, 0, 90)]),
            title(100, &[(1, 0, 90), (2, 5, 95), (3, 0, 90)]),
            title(101, &[(1, 0, 90)]),
            title(102, &[(2, 5, 95)]),
        ]
    }

    #[test]
    fn exact_playall_partition_preserves_authored_not_scan_order() {
        let mut titles = fixture();
        annotate_roster(&[101, 102, 103], &mut titles);
        for (title, ordinal) in titles.iter().zip([Some(2), None, Some(0), Some(1)]) {
            assert_eq!(
                title.selection_evidence.episodes,
                EpisodeEvidence::Authored {
                    roster: "hdmv-unconditional-playall-v1:3".into(),
                    title_count: 4,
                    member: ordinal.is_some(),
                    ordinal,
                }
            );
        }
    }

    #[test]
    fn same_clip_names_or_durations_do_not_prove_partition() {
        for change in 0..4 {
            let mut titles = fixture();
            match change {
                0 => titles[1].clips[1].in_time += 1,
                1 => titles[1].clips.swap(0, 1),
                2 => titles[1].clips[1].clip_id.clear(),
                _ => titles[1].clips.clear(),
            }
            annotate_roster(&[101, 102, 103], &mut titles);
            assert!(
                titles
                    .iter()
                    .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
            );
        }
    }

    #[test]
    fn incomplete_or_repeated_roster_is_not_accepted() {
        for order in [
            vec![],
            vec![101],
            vec![101, 999],
            vec![101, 101],
            vec![102, 101, 103],
        ] {
            let mut titles = fixture();
            annotate_roster(&order, &mut titles);
            assert!(
                titles
                    .iter()
                    .all(|t| t.selection_evidence.episodes == EpisodeEvidence::Unknown)
            );
        }
    }
}

use super::*;
use crate::disc::{Clip, ContentFormat};

fn title(id: u16, clip: &str) -> DiscTitle {
    DiscTitle {
        selection_evidence: Default::default(),
        playlist: format!("{id:05}.mpls"),
        playlist_id: id,
        duration_secs: 100.,
        size_bytes: 1000,
        clips: vec![Clip {
            clip_id: clip.into(),
            in_time: 4,
            out_time: 90004,
            duration_secs: 2.,
            source_packets: 0,
            feed_span: None,
        }],
        streams: vec![],
        chapters: vec![],
        extents: vec![],
        content_format: ContentFormat::BdTs,
        codec_privates: vec![],
    }
}

#[test]
fn cancellation_after_asset_parse_overrides_its_error() {
    let polls = std::cell::Cell::new(0);
    let mut titles = vec![title(1, "one")];
    let result = annotate_inputs(
        |_, _| Ok(vec![]),
        || {
            polls.set(polls.get() + 1);
            polls.get() >= 4
        },
        &mut titles,
    );
    assert_eq!(result, Err(Reject::Cancelled));
    assert_eq!(
        titles[0].selection_evidence.episodes,
        EpisodeEvidence::Unknown
    );
}

#[test]
fn cancellation_during_read_stops_before_parse_or_next_asset() {
    let cancelled = std::cell::Cell::new(false);
    let mut calls = 0;
    let mut titles = vec![title(1, "one")];
    assert_eq!(
        annotate_inputs(
            |_, _| {
                calls += 1;
                cancelled.set(true);
                Err(Reject::MissingAsset)
            },
            || cancelled.get(),
            &mut titles
        ),
        Err(Reject::Cancelled)
    );
    assert_eq!(calls, 1);
    assert_eq!(
        titles[0].selection_evidence.episodes,
        EpisodeEvidence::Unknown
    );
}

#[test]
fn publication_order_is_not_scan_order_and_exact_aliases_share_ordinal() {
    let titles = vec![
        title(78, "third"),
        title(79, "first"),
        title(80, "other"),
        title(81, "first"),
    ];
    let evidence = prepare(&[79, 78], &titles).unwrap();
    for (actual, ordinal) in evidence.iter().zip([Some(1), Some(0), None, Some(0)]) {
        assert_eq!(
            *actual,
            EpisodeEvidence::Authored {
                roster: "onq-uhd-v1:[79, 78]".into(),
                title_count: 4,
                member: ordinal.is_some(),
                ordinal,
            }
        );
    }
    assert!(
        titles
            .iter()
            .all(|title| title.selection_evidence.episodes == EpisodeEvidence::Unknown)
    );
}

#[test]
fn missing_duplicate_conflicting_and_partial_presentations_do_not_publish() {
    for mutation in 0..7 {
        let mut titles = vec![title(78, "third"), title(79, "first")];
        let mut order = vec![79, 78];
        match mutation {
            0 => order[1] = 80,
            1 => order[1] = 79,
            2 => titles[1].playlist_id = 78,
            3 => titles[1].clips.clear(),
            4 => titles[1].clips[0].out_time = 4,
            5 => titles[1].clips[0].clip_id = "third".into(),
            _ => {
                titles[1].selection_evidence.episodes = EpisodeEvidence::Authored {
                    roster: "another-producer".into(),
                    title_count: 2,
                    member: true,
                    ordinal: Some(0),
                }
            }
        }
        let before = titles
            .iter()
            .map(|t| t.selection_evidence.episodes.clone())
            .collect::<Vec<_>>();
        assert!(prepare(&order, &titles).is_err());
        assert_eq!(
            before,
            titles
                .iter()
                .map(|t| t.selection_evidence.episodes.clone())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn missing_bootstrap_and_cancellation_preserve_every_title() {
    let mut titles = vec![title(78, "first"), title(79, "second")];
    assert_eq!(
        annotate_inputs(|_, _| panic!("cancelled before read"), || true, &mut titles),
        Err(Reject::Cancelled)
    );
    assert_eq!(
        annotate_inputs(|_, _| Err(Reject::MissingAsset), || false, &mut titles),
        Err(Reject::MissingAsset)
    );
    assert!(
        titles
            .iter()
            .all(|title| title.selection_evidence.episodes == EpisodeEvidence::Unknown)
    );
}

#[test]
#[ignore = "cached authored metadata only; no media or JVM execution"]
fn actual_inputs_and_missing_or_tampered_assets_never_bypass_certificate() {
    use std::io::Read;
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    let directory = jar.parent().unwrap();
    for mutation in 0..4 {
        let mut titles = vec![
            title(166, "first"),
            title(167, "second"),
            title(168, "third"),
        ];
        let result = annotate_inputs(
            |path, limit| {
                let leaf = path.rsplit('/').next().ok_or(Reject::Invalid)?;
                if mutation == 1 && leaf == "00002.jar" {
                    return Err(Reject::MissingAsset);
                }
                let file =
                    std::fs::File::open(directory.join(leaf)).map_err(|_| Reject::MissingAsset)?;
                let mut bytes = Vec::new();
                file.take(limit as u64)
                    .read_to_end(&mut bytes)
                    .map_err(|_| Reject::Invalid)?;
                if mutation == 2 && leaf == "00001.bdjo" {
                    bytes[0] ^= 1;
                }
                if mutation == 3 && leaf == "44444.jar" {
                    bytes.clear();
                }
                Ok(bytes)
            },
            || false,
            &mut titles,
        );
        if mutation == 0 {
            assert_eq!(result, Ok(()));
            for (ordinal, title) in titles.iter().enumerate() {
                assert!(matches!(title.selection_evidence.episodes,
                    EpisodeEvidence::Authored { member: true, ordinal: Some(actual), title_count: 3, .. }
                    if actual == ordinal));
            }
        } else {
            assert!(result.is_err());
            assert!(
                titles
                    .iter()
                    .all(|title| title.selection_evidence.episodes == EpisodeEvidence::Unknown)
            );
        }
    }
}

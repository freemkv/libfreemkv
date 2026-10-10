//! Publication boundary: no title is changed until the certificate, complete
//! scanned roster, presentation aliases, and cancellation checks all succeed.
use super::{Reject, Result, assets, certificate};
use crate::disc::{DiscTitle, EpisodeEvidence};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn annotate_inputs(
    mut read: impl FnMut(&str, usize) -> Result<Vec<u8>>,
    cancelled: impl Fn() -> bool,
    titles: &mut [DiscTitle],
) -> Result<()> {
    if cancelled() {
        return Err(Reject::Cancelled);
    }
    let assets = assets::load(|path, limit| {
        if cancelled() {
            return Err(Reject::Cancelled);
        }
        let result = read(path, limit);
        if cancelled() {
            return Err(Reject::Cancelled);
        }
        result
    });
    if cancelled() {
        return Err(Reject::Cancelled);
    }
    let assets = assets?;
    let order = certificate::verify(&assets);
    if cancelled() {
        return Err(Reject::Cancelled);
    }
    let order = order?;
    let evidence = prepare(&order, titles);
    if cancelled() {
        return Err(Reject::Cancelled);
    }
    let evidence = evidence?;
    for (title, episodes) in titles.iter_mut().zip(evidence) {
        title.selection_evidence.episodes = episodes;
    }
    Ok(())
}

fn presentation(title: &DiscTitle) -> Result<Vec<(&str, u32, u32)>> {
    if title.clips.is_empty() {
        return Err(Reject::Invalid);
    }
    title
        .clips
        .iter()
        .map(|clip| {
            if clip.clip_id.is_empty() || clip.in_time >= clip.out_time {
                return Err(Reject::Invalid);
            }
            Ok((clip.clip_id.as_str(), clip.in_time, clip.out_time))
        })
        .collect()
}

fn prepare(order: &[u16], titles: &[DiscTitle]) -> Result<Vec<EpisodeEvidence>> {
    if order.is_empty() || order.len() > 4096 || titles.is_empty() || titles.len() > 4096 {
        return Err(Reject::Invalid);
    }
    let clips = titles.iter().try_fold(0usize, |sum, title| {
        sum.checked_add(title.clips.len()).ok_or(Reject::Budget)
    })?;
    if clips > 65536 {
        return Err(Reject::Budget);
    }
    let mut ids = BTreeSet::new();
    if titles.iter().any(|title| {
        !ids.insert(title.playlist_id)
            || title.selection_evidence.episodes != EpisodeEvidence::Unknown
    }) {
        return Err(Reject::Invalid);
    }
    let presentations = titles
        .iter()
        .map(presentation)
        .collect::<Result<Vec<_>>>()?;
    let mut members = BTreeMap::new();
    let mut ordered_ids = BTreeSet::new();
    for (ordinal, id) in order.iter().enumerate() {
        if !ordered_ids.insert(*id) {
            return Err(Reject::Invalid);
        }
        let index = titles
            .iter()
            .position(|title| title.playlist_id == *id)
            .ok_or(Reject::MissingAsset)?;
        let value = &presentations[index];
        if members.insert(value.clone(), ordinal).is_some() {
            return Err(Reject::Invalid);
        }
    }
    let roster = format!("onq-uhd-v1:{order:?}");
    Ok(presentations
        .iter()
        .map(|value| {
            let ordinal = members.get(value).copied();
            EpisodeEvidence::Authored {
                roster: roster.clone(),
                title_count: titles.len(),
                member: ordinal.is_some(),
                ordinal,
            }
        })
        .collect())
}

#[cfg(test)]
#[path = "annotation_tests.rs"]
mod tests;

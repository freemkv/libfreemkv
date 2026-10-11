//! Blu-ray play-all partition evidence, independent of HDMV and BD-J navigation.
//!
//! One play-all playlist must be the exact concatenation of standalone playlists
//! (exact clip id and in/out ticks). Ambiguous, competing, nested or implausible
//! partitions leave every title Unknown. This proves an authored program partition,
//! NOT TV semantics; Unknown is a review hold for Episodes, never a fallback.

use super::{DiscTitle, EpisodeEvidence};
use std::collections::HashMap;
use std::hash::Hash;

const MAX_TITLES: usize = 4096;
const MAX_CLIPS: usize = 65_536;

/// Greedy unique-prefix decomposition shared by the DVD menu and BD play-all producers.
pub(super) struct Prefixes<'a, T> {
    groups: &'a [Vec<T>],
    by_first: HashMap<&'a T, Vec<usize>>,
}

impl<'a, T: Eq + Hash> Prefixes<'a, T> {
    /// `None` when any group is empty: an empty program would match everywhere.
    pub(super) fn new(groups: &'a [Vec<T>]) -> Option<Self> {
        let mut by_first: HashMap<&T, Vec<usize>> = HashMap::new();
        for (i, group) in groups.iter().enumerate() {
            by_first.entry(group.first()?).or_default().push(i);
        }
        Some(Self { groups, by_first })
    }

    /// Split `groups[whole]` into at least two OTHER groups, each used once, where
    /// exactly one unused group is a prefix of the remainder at every position.
    pub(super) fn decompose(&self, whole: usize) -> Option<Vec<usize>> {
        let sequence = self.groups.get(whole)?;
        let mut used = vec![false; self.groups.len()];
        let mut order = Vec::new();
        let mut at = 0;
        while let Some(rest) = sequence.get(at..).filter(|rest| !rest.is_empty()) {
            let mut matches = self
                .by_first
                .get(&rest[0])?
                .iter()
                .filter(|&&g| g != whole && !used[g] && rest.starts_with(&self.groups[g]));
            let next = *matches.next()?;
            // Prefix ambiguity is rejected instead of searching for a preferred cut.
            if matches.next().is_some() {
                return None;
            }
            used[next] = true;
            order.push(next);
            at += self.groups[next].len();
        }
        (order.len() >= 2).then_some(order)
    }
}

/// Annotate every title from one proven play-all partition, or leave all Unknown.
pub(super) fn annotate(titles: &mut [DiscTitle]) {
    let Some((roster, ordinals)) = partition(titles) else {
        tracing::debug!(
            target: "freemkv::scan",
            "bd: no unique play-all partition; episode selection needs review"
        );
        return;
    };
    let title_count = titles.len();
    for (title, ordinal) in titles.iter_mut().zip(ordinals) {
        title.selection_evidence.episodes = EpisodeEvidence::Authored {
            roster: roster.clone(),
            title_count,
            ordinal,
        };
    }
}

// A presentation that repeats an interval (menu loops) is never a wrapper or "longest".
fn repeats(presentation: &[usize]) -> bool {
    let mut seen = std::collections::HashSet::new();
    !presentation.iter().all(|i| seen.insert(*i))
}

// Roster label and per-title member ordinal.
fn partition(titles: &[DiscTitle]) -> Option<(String, Vec<Option<usize>>)> {
    if titles.len() > MAX_TITLES || titles.iter().map(|t| t.clips.len()).sum::<usize>() > MAX_CLIPS
    {
        return None;
    }
    // Exact (clip, in, out) intervals interned to ids so presentations compare as integers.
    let mut ids = HashMap::new();
    let mut raw = Vec::with_capacity(titles.len());
    let mut ticks = Vec::with_capacity(titles.len());
    for title in titles {
        let mut presentation = Vec::with_capacity(title.clips.len());
        let mut sum = 0u64;
        for c in &title.clips {
            let next = ids.len();
            presentation.push(
                *ids.entry((c.clip_id.as_str(), c.in_time, c.out_time))
                    .or_insert(next),
            );
            sum += u64::from(c.out_time.saturating_sub(c.in_time));
        }
        raw.push(presentation);
        ticks.push(sum);
    }
    // Exact presentation aliases share one group; invalid presentations join none.
    let mut index = HashMap::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut group_ticks = Vec::new();
    let mut group_of = Vec::with_capacity(titles.len());
    for (i, title) in titles.iter().enumerate() {
        let valid = !title.clips.is_empty()
            && title
                .clips
                .iter()
                .all(|c| !c.clip_id.is_empty() && c.in_time < c.out_time);
        group_of.push(valid.then(|| {
            *index.entry(&raw[i]).or_insert_with(|| {
                groups.push(raw[i].clone());
                group_ticks.push(ticks[i]);
                groups.len() - 1
            })
        }));
    }
    let prefixes = Prefixes::new(&groups)?;
    let mut found = None;
    for (whole, group) in groups.iter().enumerate() {
        if group.len() < 2 || repeats(group) {
            continue;
        }
        if let Some(order) = prefixes.decompose(whole)
            && found.replace((whole, order)).is_some()
        {
            return None;
        }
    }
    let (wrapper, order) = found?;
    let wrapper_intervals: std::collections::HashSet<_> = groups[wrapper].iter().collect();
    for (i, presentation) in raw.iter().enumerate() {
        let listed = group_of[i].is_some_and(|g| g == wrapper || order.contains(&g));
        // Nested play-alls and segment playlists over the wrapper's clips compete with it.
        if !listed
            && presentation.len() >= 2
            && !repeats(presentation)
            && presentation.iter().any(|x| wrapper_intervals.contains(x))
        {
            return None;
        }
        // The episode play-all is the longest program; an extras play-all is not.
        if group_of[i] != Some(wrapper) && !repeats(presentation) && ticks[i] > group_ticks[wrapper]
        {
            return None;
        }
    }
    // A recap, logo or diary inside the play-all would shift every later ordinal.
    let total: u64 = order.iter().map(|&g| group_ticks[g]).sum();
    let n = order.len() as u64;
    if order
        .iter()
        .any(|&g| u128::from(group_ticks[g]) * 2 * u128::from(n) < u128::from(total))
    {
        return None;
    }
    let first = titles
        .iter()
        .zip(&group_of)
        .filter(|(_, g)| **g == Some(wrapper))
        .map(|(t, _)| t.playlist_id)
        .min()?;
    let ordinals = group_of
        .iter()
        .map(|g| g.and_then(|g| order.iter().position(|&o| o == g)))
        .collect();
    Some((format!("bd-playall-partition-v1:{first}"), ordinals))
}

#[cfg(test)]
#[path = "selection_partition_tests.rs"]
mod tests;

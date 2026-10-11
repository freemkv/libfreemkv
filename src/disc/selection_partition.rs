//! Play-all partition evidence over each title's content segments, for any format and
//! independent of menu navigation.
//!
//! One play-all title must be the exact concatenation of standalone titles (exact
//! segments). Ambiguous, competing, nested or implausible partitions leave every
//! title Unknown. This proves an authored program partition, NOT TV semantics;
//! Unknown is a review hold for Episodes, never a fallback.

use super::{ContentSegment, DiscTitle, EpisodeEvidence};
use std::collections::HashMap;
use std::hash::Hash;

const MAX_TITLES: usize = 4096;
const MAX_SEGMENTS: usize = 65_536;

// Greedy unique-prefix decomposition of one group into others.
struct Prefixes<'a, T> {
    groups: &'a [Vec<T>],
    by_first: HashMap<&'a T, Vec<usize>>,
}

impl<'a, T: Eq + Hash> Prefixes<'a, T> {
    /// `None` when any group is empty: an empty program would match everywhere.
    fn new(groups: &'a [Vec<T>]) -> Option<Self> {
        let mut by_first: HashMap<&T, Vec<usize>> = HashMap::new();
        for (i, group) in groups.iter().enumerate() {
            by_first.entry(group.first()?).or_default().push(i);
        }
        Some(Self { groups, by_first })
    }

    /// Split `groups[whole]` into at least two OTHER groups, each used once, where
    /// exactly one unused group is a prefix of the remainder at every position.
    fn decompose(&self, whole: usize) -> Option<Vec<usize>> {
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
/// `producer` names the calling scanner's roster, e.g. `bd-playall-partition-v1`.
pub(super) fn annotate(titles: &mut [DiscTitle], producer: &str) {
    let Some((wrapper, ordinals)) = partition(titles) else {
        tracing::debug!(
            target: "freemkv::scan",
            producer,
            "no unique play-all partition; episode selection needs review"
        );
        return;
    };
    let roster = format!("{producer}:{wrapper}");
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

// Lowest wrapper title id and per-title member ordinal.
fn partition(titles: &[DiscTitle]) -> Option<(u16, Vec<Option<usize>>)> {
    fn segments(t: &DiscTitle) -> &[ContentSegment] {
        &t.selection_evidence.segments
    }
    if titles.len() > MAX_TITLES
        || titles.iter().map(|t| segments(t).len()).sum::<usize>() > MAX_SEGMENTS
    {
        return None;
    }
    // Exact segments interned to ids so presentations compare as integers.
    let mut ids = HashMap::new();
    let mut raw = Vec::with_capacity(titles.len());
    let mut ticks = Vec::with_capacity(titles.len());
    for title in titles {
        let mut presentation = Vec::with_capacity(segments(title).len());
        let mut sum = 0u64;
        for segment in segments(title) {
            let next = ids.len();
            presentation.push(*ids.entry(segment).or_insert(next));
            sum = sum.saturating_add(segment.end.saturating_sub(segment.start));
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
        let valid = !segments(title).is_empty() && segments(title).iter().all(|s| s.start < s.end);
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
    let total: u128 = order.iter().map(|&g| u128::from(group_ticks[g])).sum();
    let n = order.len() as u128;
    if order
        .iter()
        .any(|&g| u128::from(group_ticks[g]) * 2 * n < total)
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
    Some((first, ordinals))
}

#[cfg(test)]
#[path = "selection_partition_tests.rs"]
mod tests;

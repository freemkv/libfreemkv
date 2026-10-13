//! Play-all partition evidence over each title's content segments, for any format and
//! independent of menu navigation.
//!
//! One play-all title must be the exact concatenation of standalone titles (exact
//! segments, save an out-point within the scanner's `end_slack`). Ambiguous, competing, nested or implausible partitions leave every
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

/// The most a title can play, for the rules that no undescribed title outlasts the
/// play-all or reads its sectors.
#[derive(Clone, Debug)]
pub(super) enum Reach {
    /// At most this long and this many sectors, reading its extents and `beyond`
    /// (half-open sector ranges).
    AtMost {
        secs: f64,
        sectors: u64,
        beyond: Vec<(u64, u64)>,
    },
    /// Unbounded or unknown: it could be the longer play-all.
    Unknown,
}

/// Annotate every title from one proven play-all partition, or leave all Unknown.
/// `producer` names the calling scanner's roster, e.g. `bd-playall-partition-v1`.
/// `complete` is false when the scanner dropped or cut a title it should have
/// described: the missing one could compete with any partition of the rest.
/// `reach[i]` bounds title `i`; a title past its end plays its duration and extents.
/// `end_slack` is how far apart (in segment units) two out-points of one piece may be and
/// still be the same piece; 0 compares exactly.
pub(super) fn annotate(
    titles: &mut [DiscTitle],
    producer: &str,
    complete: bool,
    reach: &[Reach],
    end_slack: u64,
) {
    let Some((wrapper, ordinals)) = complete
        .then(|| partition(titles, reach, end_slack))
        .flatten()
    else {
        tracing::debug!(
            target: "freemkv::scan",
            producer,
            complete,
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

// Non-empty sector ranges a title reads, as half-open `[start, end)`.
fn sector_ranges(title: &DiscTitle) -> impl Iterator<Item = (u64, u64)> + '_ {
    title
        .extents
        .iter()
        .filter(|e| e.sector_count > 0)
        .map(|e| {
            let start = u64::from(e.start_lba);
            (start, start + u64::from(e.sector_count))
        })
}

// Each segment's out-point for comparison: the lowest of its run of out-points from the
// same unit and in-point when consecutive ones are at most `slack` apart and the whole
// run spans no more than `slack` (a play-all piece trimmed a few frames off a
// standalone episode). A wider chain could join two real cuts, so it stays exact.
fn slack_ends<'a>(
    segments: impl Iterator<Item = &'a ContentSegment>,
    slack: u64,
) -> HashMap<&'a ContentSegment, u64> {
    let mut out = HashMap::new();
    if slack == 0 {
        return out;
    }
    let mut by_start: HashMap<(&str, u64), Vec<&ContentSegment>> = HashMap::new();
    for segment in segments {
        by_start
            .entry((segment.unit.as_str(), segment.start))
            .or_default()
            .push(segment);
    }
    for mut run in by_start.into_values() {
        run.sort_unstable_by_key(|s| s.end);
        run.dedup();
        let mut from = 0;
        for i in 1..=run.len() {
            if i < run.len() && run[i].end - run[i - 1].end <= slack {
                continue;
            }
            let chain = &run[from..i];
            if chain.len() > 1 && chain[chain.len() - 1].end - chain[0].end <= slack {
                out.extend(chain.iter().map(|s| (*s, chain[0].end)));
            }
            from = i;
        }
    }
    out
}

// Lowest wrapper title id and per-title member ordinal.
fn partition(
    titles: &[DiscTitle],
    reach: &[Reach],
    end_slack: u64,
) -> Option<(u16, Vec<Option<usize>>)> {
    fn segments(t: &DiscTitle) -> &[ContentSegment] {
        &t.selection_evidence.segments
    }
    if titles.len() > MAX_TITLES
        || titles.iter().map(|t| segments(t).len()).sum::<usize>() > MAX_SEGMENTS
    {
        return None;
    }
    // Segments interned to ids so presentations compare as integers; an out-point within
    // `end_slack` of another piece's from the same in-point shares its id.
    let ends = slack_ends(titles.iter().flat_map(segments), end_slack);
    let mut ids = HashMap::new();
    let mut raw = Vec::with_capacity(titles.len());
    let mut ticks = Vec::with_capacity(titles.len());
    for title in titles {
        let mut presentation = Vec::with_capacity(segments(title).len());
        let mut sum = 0u64;
        for segment in segments(title) {
            let next = ids.len();
            let end = ends.get(segment).copied().unwrap_or(segment.end);
            let key = (segment.unit.as_str(), segment.start, end);
            presentation.push(*ids.entry(key).or_insert(next));
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
    // A title the scanner could not describe, yet reading the roster's sectors anywhere it
    // can play, may be a longer or competing play-all: no roster can be proven.
    let mut read: Vec<(u64, u64)> = Vec::new();
    for (title, group) in titles.iter().zip(&group_of) {
        if group.is_some_and(|g| g == wrapper || order.contains(&g)) {
            read.extend(sector_ranges(title));
        }
    }
    read.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in read {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    let blocked = (titles.iter().enumerate())
        .zip(&group_of)
        .filter(|(_, group)| group.is_none())
        .flat_map(|((i, title), _)| {
            let beyond = match reach.get(i) {
                Some(Reach::AtMost { beyond, .. }) => &beyond[..],
                _ => &[],
            };
            sector_ranges(title).chain(beyond.iter().copied())
        })
        .any(|(start, end)| {
            let at = merged.partition_point(|r| r.0 < end);
            at > 0 && merged[at - 1].1 > start
        });
    if blocked {
        return None;
    }
    // Nor may an undescribed title play longer, by time or sectors, than the play-all
    // (all it can play, not what was read of it): it could be the real one beside extras.
    let sectors = |t: &DiscTitle| {
        sector_ranges(t)
            .map(|(start, end)| end - start)
            .sum::<u64>()
    };
    let mut shortest = (f64::INFINITY, u64::MAX);
    for (title, _) in titles
        .iter()
        .zip(&group_of)
        .filter(|(_, g)| **g == Some(wrapper))
    {
        shortest = (
            shortest.0.min(title.duration_secs),
            shortest.1.min(sectors(title)),
        );
    }
    let longer = |i: usize, t: &DiscTitle| match reach.get(i) {
        None => t.duration_secs > shortest.0 || sectors(t) > shortest.1,
        Some(Reach::AtMost { secs, sectors, .. }) => *secs > shortest.0 || *sectors > shortest.1,
        Some(Reach::Unknown) => true,
    };
    if (titles.iter().enumerate())
        .zip(&group_of)
        .any(|((i, t), g)| g.is_none() && longer(i, t))
    {
        return None;
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

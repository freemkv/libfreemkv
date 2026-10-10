//! Exact play-all partition shared by the authored-program roster producers.

use std::collections::HashMap;
use std::hash::Hash;

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

#[cfg(test)]
#[path = "selection_partition_tests.rs"]
mod tests;

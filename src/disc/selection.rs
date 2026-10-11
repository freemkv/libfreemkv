//! Selection provenance survives sorting and copying individual titles.

/// Membership in a verified authored standalone-program roster for Episodes mode.
/// Producers: the DVD static-menu play-all partition and the Blu-ray play-all partition
/// (the longest playlist exactly concatenates other playlists), not navigation code.
/// This is not a semantic TV classification: movie chapter titles can have the same
/// structure. The caller must choose Episodes from user intent or TV metadata.
/// `Unknown` means no roster was proven: Episodes must hold for review, not guess.
/// First-Play and movie hints alone are insufficient.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum EpisodeEvidence {
    #[default]
    Unknown,
    /// A verified producer must annotate EVERY scanned title with the same roster
    /// and total title count, including nonmembers. Partial or conflicting records
    /// require review. `roster` identifies the producer and authored source/table.
    Authored {
        roster: String,
        title_count: usize,
        member: bool,
        /// Zero-based authored program order. Required for members, absent for
        /// nonmembers. Exact presentation aliases share one ordinal; distinct
        /// programs occupy contiguous ordinals starting at zero.
        ordinal: Option<usize>,
    },
}

/// One authored span a title plays: `[start, end)` within an opaque `unit`, weighing
/// `end - start`. Each format backend describes its titles this way at scan, so callers
/// compare content without knowing the format. Units and scales compare only within one disc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentSegment {
    pub unit: String,
    pub start: u64,
    pub end: u64,
}

impl ContentSegment {
    /// Playlist items in play order: the clip and its in/out ticks. Empty unless every
    /// clip has an id and a positive span.
    pub fn from_clips(clips: &[super::Clip]) -> Vec<Self> {
        clips
            .iter()
            .map(|clip| {
                (!clip.clip_id.trim().is_empty() && clip.in_time < clip.out_time).then(|| Self {
                    unit: clip.clip_id.clone(),
                    start: clip.in_time.into(),
                    end: clip.out_time.into(),
                })
            })
            .collect::<Option<_>>()
            .unwrap_or_default()
    }

    /// Authored cell extents in play order, as sector ranges in the disc's single address
    /// space (one unit). Empty unless every extent has sectors.
    pub fn from_cell_extents(extents: &[super::Extent]) -> Vec<Self> {
        extents
            .iter()
            .map(|extent| {
                let start = u64::from(extent.start_lba);
                (extent.sector_count > 0).then(|| Self {
                    unit: String::new(),
                    start,
                    end: start + u64::from(extent.sector_count),
                })
            })
            .collect::<Option<_>>()
            .unwrap_or_default()
    }
}

/// Observations and accepted ranking evidence, attached to stable title identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TitleSelectionEvidence {
    /// Authored content in play order; empty when the backend cannot describe it exactly.
    pub segments: Vec<ContentSegment>,
    pub episodes: EpisodeEvidence,
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;

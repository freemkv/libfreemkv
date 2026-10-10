//! Selection provenance survives sorting and copying individual titles.

/// Navigation program that selected a movie candidate, not an episode roster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NavigationSource {
    HdmvFirstPlay,
    DvdFirstPlay,
}

/// Why the existing movie-ranking policy accepted a title.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MovieSelectionBasis {
    #[default]
    CanonicalFallback,
    Navigation(NavigationSource),
    /// May include JAR locator/duration scoring; not proof of reachability.
    AuthoringHint,
}

/// Membership in a verified authored standalone-program roster for Episodes mode.
/// This is not a semantic TV classification: movie chapter titles can have the same
/// structure. The caller must choose Episodes from user intent or TV metadata.
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

/// Observations and accepted ranking evidence, attached to stable title identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TitleSelectionEvidence {
    /// Bounded DVD root-menu launch routes; independent of episode/identity proof.
    pub dvd_launch: super::DvdLaunchEvidence,
    /// Observed static DVD menu target. Reachability alone is NOT episode proof.
    pub dvd_menu_reachable: bool,
    /// Raw First-Play result; movie plausibility guards may reject it for ranking.
    pub navigation: Option<NavigationSource>,
    /// Raw label/framework/JAR hint; never a complete episode roster.
    pub authoring_hint: bool,
    pub movie_basis: MovieSelectionBasis,
    pub episodes: EpisodeEvidence,
}

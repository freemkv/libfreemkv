//! DVD root-menu launch provenance, independent of episode or identity evidence.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DvdLaunchReviewReason {
    UnsupportedNavigation,
    IncompleteNavigation,
    AmbiguousNavigation,
    UnprovenPresentation,
    BudgetExceeded,
}

/// One command actually traversed by a bounded launch trace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DvdLaunchStep {
    /// Zero for VIDEO_TS.IFO; otherwise VTS number.
    pub vts: u8,
    /// True means an offset in VTS_xx_0.VOB; false means the IFO.
    pub menu_vob: bool,
    pub byte_offset: u32,
    pub command: [u8; 8],
}

/// A root button's verified full-title destination, not an equivalence class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DvdLaunchRoute {
    pub button: u8,
    /// Display modes whose complete button commands/arrows were verified equal.
    pub display_masks: Vec<u8>,
    pub target_vts: u8,
    pub target_title: u8,
    pub target_part: u16,
    /// Logical SPRM1 slot, not an index into the scanned title's audio streams.
    pub audio_stream: u8,
    /// Physical PID resolved through this title PGC's AST_CTL for `audio_stream`.
    pub audio_pid: u16,
    /// Language of the selected logical VTS attribute, corroborated against the PID.
    pub audio_language: String,
    /// All bounded alternative register paths must agree on the destination.
    pub traces: Vec<Vec<DvdLaunchStep>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DvdLaunchEvidence {
    #[default]
    Unknown,
    Review(DvdLaunchReviewReason),
    /// Complete direct-launch results for ONE root menu, not all disc menus.
    /// Annotated on every scanned title; `routes` is empty for non-targets.
    /// Does not classify TV, authorize automatic selection, or equate intervals.
    VerifiedRoot {
        vts: u8,
        pgcn: u16,
        title_count: usize,
        routes: Vec<DvdLaunchRoute>,
    },
}

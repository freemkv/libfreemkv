//! DVD-Video navigation — read-only resolver for the **main-feature start
//! point** (issue #40). Mirrors what a DVD player's nav VM resolves: First-Play
//! → menu "Play" → title dispatch → the first cell of the feature, so the rip
//! starts at the movie rather than at raw cell 0 (e.g. skipping a leading
//! logo/warning segment when the disc's own navigation does).
//!
//! Contents: [`vmcmd`] (VM command decoder), [`nav`] (First-Play executor).
//! See docs/dvdnav-mod.md — byte-layout basis and module contents detail.

pub(crate) mod nav;
pub(crate) mod vmcmd;

pub(crate) use nav::resolve_main_title;

use crate::sector::SectorSource;

/// Resolve the feature title's **true start cell** (0-based index into the
/// title PGC's cell list) by following the disc's own navigation — First-Play →
/// menu "Play" → title dispatch — the way a player reaches the movie. This is
/// what lets the rip begin at the feature instead of at raw cell 0 when the
/// disc's nav enters the title past a leading logo/warning segment (e.g. a
/// disc whose "Play" resolves to a later cell than cell 0).
///
/// Returns `None` when navigation cannot be resolved, so the caller falls back
/// to the structural leading-cell filter (today's behaviour, ≈ cell 0 / 0:00).
pub fn resolve_feature_start(
    reader: &mut dyn SectorSource,
    udf: &crate::udf::UdfFs,
    vtsn: u16,
    vts_ttn: u16,
) -> Option<usize> {
    // PARKED stub (issue #40, `USE_NAV_RESOLVER=false`) — intentionally always
    // returns `None`; do not delete. `reader`/`udf` are unused seam inputs.
    // See docs/dvdnav-mod.md for full rationale.
    let _ = (reader, udf);
    tracing::trace!(
        target: "freemkv::dvdnav",
        vtsn,
        vts_ttn,
        "nav start-cell resolver: unresolved — caller falls back to leading-cell filter"
    );
    None
}

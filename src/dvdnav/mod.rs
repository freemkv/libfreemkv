//! DVD-Video navigation — read-only resolver for the **main-feature start
//! point** (issue #40). Mirrors what a DVD player's nav VM resolves: First-Play
//! → menu "Play" → title dispatch → the first cell of the feature, so the rip
//! starts at the movie rather than at raw cell 0 (e.g. skipping a leading
//! logo/warning segment when the disc's own navigation does).
//!
//! Byte layout follows the DVD-Video specification (VMGI/VTSI headers,
//! PGC/cell tables, PCI/HLI button packets); the VM command decoder is
//! verified against libdvdnav's decoder.
//!
//! Current contents: [`vmcmd`] — the VM command decoder (proven against the
//! SOTL/Greenland test discs). The IFO/PCI parsing and the navigation executor
//! that resolves the start cell build on top of this.

pub mod vmcmd;

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
///
/// TODO(#40): the IFO/PCI parsing + nav executor (built on [`vmcmd`]) land
/// incrementally. Until the executor is complete this returns `None`, so wiring
/// it in is behaviour-neutral; improvements to the resolver take effect here
/// without touching the call site.
pub fn resolve_feature_start(
    reader: &mut dyn SectorSource,
    udf: &crate::udf::UdfFs,
    vtsn: u16,
    vts_ttn: u16,
) -> Option<usize> {
    // `reader`/`udf` are the seam inputs the nav executor will consume to read
    // VIDEO_TS.IFO + the VTS IFOs/menu VOBs. Reserved until that lands.
    let _ = (reader, udf);
    tracing::trace!(
        target: "freemkv::dvdnav",
        vtsn,
        vts_ttn,
        "nav start-cell resolver: unresolved — caller falls back to leading-cell filter"
    );
    None
}

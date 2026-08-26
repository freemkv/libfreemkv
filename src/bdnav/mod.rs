//! BD/UHD HDMV navigation resolver — "mimic a real player" for main-feature
//! title selection. It reads `/BDMV/index.bdmv` + `/BDMV/MovieObject.bdmv` and
//! runs a faithful, bounded HDMV navigation VM (libbluray semantics) to find the
//! playlist the disc's own First-Play navigation plays as the feature.
//!
//! There is NO vendor special-casing: one correctly-booted VM resolves the
//! obfuscated Sony dispatcher and every other HDMV disc alike. The honest
//! boundary is BD-J (the feature is chosen by a Java Xlet, not by the HDMV VM);
//! there — and on any malformed data or non-convergence — the resolver ABSTAINS
//! (`None`) and selection falls back to the structural/heuristic order.
//!
//! Contract: read-only, bounded, and never panics or hard-fails.

pub(crate) mod index;
pub(crate) mod mobj;
pub(crate) mod vm;

use crate::sector::SectorSource;
use crate::udf::UdfFs;

/// Resolve the playlist id the disc's First-Play navigation plays as the
/// feature, restricted to ids the caller marks as feature candidates
/// (`is_feature_candidate` — typically video-bearing, non-trivial titles, so a
/// short logo/pre-roll `PlayPlayList` is skipped). Returns `None` for BD-J discs,
/// missing/malformed nav data, or when navigation does not converge on a
/// candidate.
pub(crate) fn resolve_feature(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    is_feature_candidate: impl Fn(u16) -> bool,
) -> Option<u16> {
    // Belt-and-suspenders: the parsers and VM are written panic-free and bounded,
    // but a navigation resolver must NEVER take down a scan — swallow any
    // unexpected panic and abstain.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resolve_inner(reader, udf, &is_feature_candidate)
    }))
    .ok()
    .flatten()
}

fn resolve_inner(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    is_feature_candidate: &dyn Fn(u16) -> bool,
) -> Option<u16> {
    let index = index::parse(&udf.read_file(reader, "/BDMV/index.bdmv").ok()?)?;
    let mobjs = mobj::parse(&udf.read_file(reader, "/BDMV/MovieObject.bdmv").ok()?)?;
    vm::resolve(&index, &mobjs, is_feature_candidate)
}

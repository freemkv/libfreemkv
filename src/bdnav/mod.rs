//! BD/UHD HDMV navigation resolver — "mimic a real player" for main-feature
//! title selection. It reads `/BDMV/index.bdmv` + `/BDMV/MovieObject.bdmv` and
//! runs a faithful, bounded HDMV navigation VM to find the playlist the disc's
//! own First-Play navigation plays as the feature.
//!
//!
//! Contract: read-only, bounded, and never panics or hard-fails.

pub(crate) mod bdjo;
pub(crate) mod index;
pub(crate) mod mobj;
pub(crate) mod vm;

use crate::sector::SectorSource;
use crate::udf::UdfFs;

pub(super) fn be_u16(d: &[u8], o: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*d.get(o)?, *d.get(o + 1)?]))
}

// Resolve the playlist id First-Play navigation plays as the feature, among ids the caller
// marks as feature candidates (`is_feature_candidate`). Returns `None` for BD-J discs,
// malformed nav data, or non-convergence.
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

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

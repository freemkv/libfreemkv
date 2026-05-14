//! Fallback fs-type detection for platforms without a specific impl.
//!
//! Always returns [`FsType::Unknown`]. Callers treat that as
//! "probably local" and pick `LocalFileSink`.

use std::path::Path;

use super::FsType;

pub(super) fn detect_impl(_path: &Path) -> FsType {
    FsType::Unknown
}

//! Windows filesystem-type detection.
//!
//! Heuristic-only: any UNC path (`\\server\share\...`) is treated as a
//! network mount and bucketed into `Nfs`. Strictly, SMB is not NFS, but
//! the buffering-policy outcome for our purposes is the same — there is
//! no platform `WritebackFile` machinery on Windows yet, so the worst
//! case of a false positive is using `LocalFileSink` regardless. A
//! proper `GetVolumeInformation` query is a Phase 4 concern.

use std::path::Path;

use super::FsType;

pub(super) fn detect_impl(path: &Path) -> FsType {
    // `Path::starts_with("\\\\")` won't match because component-wise
    // matching strips the prefix. Look at the raw OsStr instead.
    let s = path.as_os_str();
    // OsStr -> [u16] is the platform-correct way on Windows, but
    // checking the leading bytes via `to_string_lossy` is good enough
    // for a UNC prefix probe and works on any encoding.
    let lossy = s.to_string_lossy();
    if lossy.starts_with("\\\\") || lossy.starts_with("//") {
        return FsType::Nfs;
    }
    FsType::Local
}

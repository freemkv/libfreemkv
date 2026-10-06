//! Filesystem-type detection for an open file (Linux only): the writeback pipeline and the
//! flusher key their NFS policy off it. The `fstatfs` call lives in `linux.rs`.

/// What kind of filesystem a file lives on, to the extent we can tell
/// cheaply at construction time.
///
/// `Unknown` is the fail-open default: a misdetection here should not
/// be load-bearing for correctness, only for buffering policy. Callers
/// that need a binary local/non-local answer should treat `Unknown` as
/// "probably local".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsType {
    /// A local on-disk filesystem (ext4, xfs, btrfs, tmpfs).
    Local,
    /// A network filesystem with NFS semantics.
    Nfs,
    /// `fstatfs` failed or the filesystem type is not on our recognised list.
    Unknown,
}

mod linux;

/// Best-effort classification of the filesystem under the open `fd`.
///
/// Falls back to [`FsType::Unknown`] on any syscall error or unrecognised
/// filesystem signature. Never panics.
pub fn detect_fd(fd: std::os::unix::io::RawFd) -> FsType {
    linux::detect_fd_impl(fd)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

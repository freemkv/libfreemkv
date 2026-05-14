//! Linux: kernel readahead hint for the ISO file.
//!
//! Originally `POSIX_FADV_SEQUENTIAL` to widen the readahead window.
//! On NFS that turned out to cause aggressive multi-MB readahead bursts
//! that saturated the TCP connection and starved concurrent writes
//! during mux — observed empirically as a ~3× drop in mux throughput
//! on the rip1/unraid-1 setup (0.21.0 vs 0.20.7 baseline). The kernel's
//! default readahead (~128 KiB on Linux) interleaves more naturally
//! with the muxer's concurrent NFS writes, so we no longer issue any
//! hint here. The per-OS file stays so the convention is honoured and
//! we can re-enable a hint cleanly if a different storage path benefits.

use std::fs::File;

pub(super) fn hint_sequential(_file: &File, _len_bytes: u64) {
    // No-op: see module-level comment. Kernel default readahead is
    // what we want on NFS-backed ISOs, which is the dominant case.
}

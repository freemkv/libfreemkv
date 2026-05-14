//! Windows: the canonical sequential-access hint is
//! `FILE_FLAG_SEQUENTIAL_SCAN` passed to `CreateFile` at open time —
//! it cannot be set after the fact via `SetFileInformationByHandle`.
//! Routing the open call through this module would mean a custom
//! `File::from_raw_handle` plumb for every `FileSectorSource::open`
//! caller, which is more invasive than the Phase 1 scope.
//!
//! TODO: replumb `FileSectorSource::open` to take an
//! `OpenOptions`-style builder so the Windows path can flip the flag
//! at open time. For now this is a no-op stub.

use std::fs::File;

pub(super) fn hint_sequential(_file: &File, _len_bytes: u64) {
    tracing::debug!(
        target: "mux",
        "FileSectorSource hint_sequential: windows stub (TODO: FILE_FLAG_SEQUENTIAL_SCAN at open)"
    );
}

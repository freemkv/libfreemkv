//! File I/O helpers that bound kernel cache pressure on big writes.
//!
//! `WritebackFile` is a drop-in wrapper around `std::fs::File` for any
//! call site that performs large sequential writes (sweep, patch, mux,
//! etc.). It implements `Write` and `Seek` so existing code paths can
//! swap `File` for `WritebackFile` with no body changes. Internally it
//! drives a `WritebackPipeline` that, on Linux, drains dirty pages
//! continuously at 32 MB granularity to avoid the kernel's
//! accumulate-then-burst flush behaviour. macOS and Windows use a
//! no-op pipeline — their default cache policies have not been shown
//! to exhibit the same pathology for this access pattern.

mod writeback;
mod writeback_file;

pub(crate) use writeback_file::WritebackFile;

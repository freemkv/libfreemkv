//! No-op writeback pipeline for non-Linux targets. macOS and Windows
//! page cache policies have not been shown to exhibit the Linux
//! accumulate-then-burst flush pathology for our access pattern.
//! If that changes, replace this stub with a real implementation
//! (e.g. `F_NOCACHE` on macOS, `FILE_FLAG_WRITE_THROUGH` on Windows).

use std::fs::File;

pub(crate) struct WritebackPipeline;

impl WritebackPipeline {
    pub(crate) fn new(_file: &File, _start_pos: u64, _chunk_bytes: u64) -> Self {
        Self
    }
    pub(crate) fn note_progress(&mut self, _pos: u64) {}
    pub(crate) fn handle_seek(&mut self, _new_pos: u64) {}
    pub(crate) fn finalize(&mut self) {}
}

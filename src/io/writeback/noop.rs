//! No-op writeback pipeline for non-Linux targets. macOS and Windows
//! page cache policies have not been shown to exhibit the Linux
//! accumulate-then-burst flush pathology for our access pattern.
//! If that changes, replace this stub with a real implementation
//! (e.g. `F_NOCACHE` on macOS, `FILE_FLAG_WRITE_THROUGH` on Windows).

use std::fs::File;

pub(crate) struct WritebackPipeline {
    // Never set in production here; kept so `error()` has one contract on every OS.
    wb_errno: Option<i32>,
}

impl WritebackPipeline {
    pub(crate) fn new(_file: &File, _start_pos: u64, _chunk_bytes: u64) -> Self {
        Self { wb_errno: None }
    }
    pub(crate) fn note_progress(&mut self, _pos: u64) {}
    pub(crate) fn handle_seek(&mut self, _new_pos: u64) {}
    pub(crate) fn finalize(&mut self) {}
    pub(crate) fn set_flush_progress(&mut self, _flush: crate::io::flush::FlushProgress) {}
    // No bounded writeback here: the §2.10 flusher always runs.
    pub(crate) fn needs_flusher(&self) -> bool {
        true
    }
    pub(crate) fn error(&self) -> Option<std::io::Error> {
        self.wb_errno.map(std::io::Error::from_raw_os_error)
    }
    #[cfg(test)]
    pub(crate) fn inject_error(&mut self, errno: i32) {
        self.wb_errno.get_or_insert(errno);
    }
}

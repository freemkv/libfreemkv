//! Linux writeback pipeline using `sync_file_range` + `posix_fadvise`.
//!
//! Pathology this fixes: the kernel's default `vm.dirty_ratio` (~20 %
//! of RAM) lets dirty pages accumulate to hundreds of MB during a
//! big sequential write, then bursts a flush at 99 % disk utilisation.
//! While the burst runs, app writes block on the writeback queue —
//! observed empirically as instantaneous speed dropping from ~15 MB/s
//! to ~1 MB/s every ~30 s during a Pass 1 sweep.
//!
//! Strategy: every `chunk_bytes` of new sequential output, kick async
//! writeback (`SYNC_FILE_RANGE_WRITE`) on the just-completed chunk and
//! finalise the *previous* chunk via `WAIT_AFTER` + `posix_fadvise
//! (DONTNEED)`. By the time we finalise, that previous chunk has had
//! a full chunk's worth of work to flush — the wait is near-instant.
//! Dirty cache stays bounded at ~2 × `chunk_bytes` and writes drain
//! continuously instead of in bursts.

use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};

pub(crate) struct WritebackPipeline {
    /// Aliases the wrapping `WritebackFile::file`. Only valid for the
    /// lifetime of that struct — moving the `File` independently
    /// would silently UAF this fd. The pipeline is a private field of
    /// `WritebackFile` and never exposed outside that wrapper, which
    /// is what keeps the alias sound.
    fd: RawFd,
    chunk_bytes: u64,
    last_flush_pos: u64,
    pending: Option<(u64, u64)>,
}

impl WritebackPipeline {
    /// Construct a pipeline aliasing `file`'s file descriptor. The
    /// returned `WritebackPipeline` MUST be dropped before `file`
    /// itself, or kept inside the same struct that owns `file` — the
    /// alias is unchecked.
    pub(crate) fn new(file: &File, start_pos: u64, chunk_bytes: u64) -> Self {
        Self {
            fd: file.as_raw_fd(),
            chunk_bytes,
            last_flush_pos: start_pos,
            pending: None,
        }
    }

    /// Caller advanced the file position to `pos`. If a chunk boundary
    /// was crossed, kick async writeback for the just-completed chunk
    /// and finalise the previous one.
    pub(crate) fn note_progress(&mut self, pos: u64) {
        if pos < self.last_flush_pos.saturating_add(self.chunk_bytes) {
            return;
        }
        unsafe {
            let chunk_off = self.last_flush_pos as i64;
            let chunk_len = (pos - self.last_flush_pos) as i64;
            libc::sync_file_range(self.fd, chunk_off, chunk_len, libc::SYNC_FILE_RANGE_WRITE);
            if let Some((prev_off, prev_len)) = self.pending.take() {
                libc::sync_file_range(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::SYNC_FILE_RANGE_WAIT_AFTER,
                );
                libc::posix_fadvise(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
            self.pending = Some((chunk_off as u64, chunk_len as u64));
        }
        self.last_flush_pos = pos;
    }

    /// Caller is about to seek away from the current write region.
    /// Drain any in-flight chunk and reset tracking.
    pub(crate) fn handle_seek(&mut self, new_pos: u64) {
        self.finalize();
        self.last_flush_pos = new_pos;
    }

    /// Drain any in-flight chunk. Idempotent. Call before `sync_all()`
    /// or when discarding the pipeline.
    pub(crate) fn finalize(&mut self) {
        if let Some((prev_off, prev_len)) = self.pending.take() {
            unsafe {
                libc::sync_file_range(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::SYNC_FILE_RANGE_WAIT_AFTER,
                );
                libc::posix_fadvise(
                    self.fd,
                    prev_off as i64,
                    prev_len as i64,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
        }
    }
}

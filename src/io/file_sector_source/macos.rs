// F_RDADVISE requests actual reads, rather than setting a sequential-access policy.

use std::fs::File;
use std::os::unix::io::AsRawFd;

// Cap on the byte length we pass to `F_RDADVISE`; 64 MiB is generous without over-asking the
// OS's cache.
const RDADVISE_MAX_BYTES: i64 = 64 * 1024 * 1024;
const PREFETCH_CHUNK_BYTES: u64 = 1024 * 1024;

// Defer advice until reads establish a stream; opening metadata must not fetch 64 MiB.
pub(crate) fn hint_sequential(_file: &File, _len_bytes: u64) {}

#[derive(Default)]
pub(super) struct PrefetchWindow {
    read_end: Option<u64>,
    consumed: u64,
    advised_end: u64,
}

impl PrefetchWindow {
    pub(super) fn after_read(
        &mut self,
        offset: u64,
        len: u64,
        file_len: u64,
    ) -> Option<(u64, u64)> {
        if len == 0 {
            return None;
        }
        if self.read_end != Some(offset) {
            self.consumed = 0;
            self.advised_end = 0;
        }
        let end = offset.saturating_add(len);
        self.read_end = Some(end);
        self.consumed = self.consumed.saturating_add(len);
        if self.consumed < PREFETCH_CHUNK_BYTES || end < self.advised_end || end >= file_len {
            return None;
        }
        let bytes = len
            .max(PREFETCH_CHUNK_BYTES)
            .min(RDADVISE_MAX_BYTES as u64)
            .min(file_len - end);
        self.consumed = 0;
        self.advised_end = end + bytes;
        Some((end, bytes))
    }
}

// No direct POSIX_FADV_DONTNEED equivalent; F_NOCACHE is too coarse (disables caching for the
// whole fd), so this is a best-effort no-op.
pub(crate) fn drop_window(_file: &File, _start: u64, _len: u64) {}

// Best-effort prefetch; the syscall itself can be costly on network filesystems.
pub(crate) fn prefetch(file: &File, offset: u64, len: u64) {
    let bytes = (len as i64).min(RDADVISE_MAX_BYTES);
    let mut ra = libc::radvisory {
        ra_offset: offset as libc::off_t,
        ra_count: bytes as libc::c_int,
    };
    // Best-effort — kernel hint only.
    // SAFETY: `file` is a live borrowed fd and `ra` a live stack radvisory for the call.
    unsafe {
        libc::fcntl(file.as_raw_fd(), libc::F_RDADVISE, &mut ra);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_reads_batch_advice_without_overlapping_windows() {
        let mut window = PrefetchWindow::default();
        let mut calls = Vec::new();
        let start = 512 * PREFETCH_CHUNK_BYTES;
        for sector in 0..2048 {
            if let Some(range) = window.after_read(start + sector * 2048, 2048, u64::MAX) {
                calls.push(range);
            }
        }
        assert_eq!(
            calls.len(),
            4,
            "four MiB must not issue 2048 advisory calls"
        );
        for (n, &(offset, len)) in calls.iter().enumerate() {
            assert_eq!(offset, start + (n as u64 + 1) * PREFETCH_CHUNK_BYTES);
            assert_eq!(len, PREFETCH_CHUNK_BYTES);
        }
    }

    #[test]
    fn sparse_metadata_and_backward_seeks_do_not_accumulate_a_stream() {
        let mut window = PrefetchWindow::default();
        for sector in (0..2048).rev() {
            assert_eq!(window.after_read(sector * 4096, 2048, u64::MAX), None);
        }
        assert_eq!(
            window.after_read(0, PREFETCH_CHUNK_BYTES - 2048, u64::MAX),
            None
        );
        assert_eq!(window.after_read(0, 2048, u64::MAX), None);
        assert_eq!(window.after_read(2048, 0, u64::MAX), None);
    }

    #[test]
    fn large_stream_reads_keep_prefetch_but_bound_it_to_eof_and_cap() {
        let mut window = PrefetchWindow::default();
        let chunk = 8 * PREFETCH_CHUNK_BYTES;
        assert_eq!(window.after_read(0, chunk, u64::MAX), Some((chunk, chunk)));
        // Sector reads consuming that advised range must not repeatedly advise it.
        for n in 0..4095 {
            assert_eq!(window.after_read(chunk + n * 2048, 2048, u64::MAX), None);
        }
        assert_eq!(
            window.after_read(2 * chunk - 2048, 2048, 2 * chunk + 17),
            Some((2 * chunk, 17))
        );
        assert_eq!(window.after_read(2 * chunk, 17, 2 * chunk + 17), None);
        let huge = 128 * PREFETCH_CHUNK_BYTES;
        assert_eq!(
            window.after_read(0, huge, u64::MAX),
            Some((huge, RDADVISE_MAX_BYTES as u64))
        );
        assert_eq!(window.after_read(u64::MAX - 1, 2048, u64::MAX), None);
    }
}

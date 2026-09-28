//! Durable flush with measured progress (stop design §2.10, §4.5; T12).
//!
//! No OS reports progress inside one `fsync`, so a flush is made of bounded pieces and
//! every completed piece is progress. A flush fails with `SyncTimeout` (E9056) only after
//! 60 s with no progress (HR1, Q-B: "fail 60s after writing. detect, if writing keep going
//! do nothing."); a Stop ends the wait at once.

use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::bounded::{BoundedError, bounded_syscall_stall};
use crate::error::Error;
use crate::halt::{Halt, Progress, StallTimer};

/// T12's window: this long with no flush progress is `SyncTimeout` (E9056).
pub(crate) const FLUSH_STALL: Duration = Duration::from_secs(60);

/// A shared view of a file's durable-flush progress: the [`Progress`] every accepted
/// write and completed flush bumps, the bytes made durable, and the file length.
/// Clones share the counters; hand one to [`WritebackFile::set_flush_progress`] and
/// read it from another thread.
///
/// [`WritebackFile::set_flush_progress`]: crate::io::WritebackFile::set_flush_progress
#[derive(Clone, Debug, Default)]
pub struct FlushProgress {
    progress: Progress,
    durable: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
}

impl FlushProgress {
    /// Counters that bump `progress` (share it with the pipeline consumer, §2.10 item 3).
    pub fn new(progress: Progress) -> Self {
        Self {
            progress,
            ..Self::default()
        }
    }

    /// The forward-progress counter.
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    /// Bytes made durable (or, on Linux local storage, written out) so far; never more
    /// than [`bytes_total`](Self::bytes_total).
    pub fn bytes_durable(&self) -> u64 {
        let total = self.bytes_total();
        self.durable.load(Ordering::Relaxed).min(total)
    }

    /// The file's length as written so far.
    pub fn bytes_total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    // `n` more bytes are durable: one unit of progress.
    pub(crate) fn add_durable(&self, n: u64) {
        self.durable.fetch_add(n, Ordering::Relaxed);
        self.progress.bump();
    }

    // At least `v` bytes are durable.
    pub(crate) fn durable_at_least(&self, v: u64) {
        if self.durable.fetch_max(v, Ordering::Relaxed) < v {
            self.progress.bump();
        }
    }

    // The file now reaches `len` bytes.
    pub(crate) fn note_total(&self, len: u64) {
        self.total.fetch_max(len, Ordering::Relaxed);
    }
}

/// The flush primitives, per OS; a seam so tests run a flush in milliseconds.
pub(crate) trait FlushOps: Send + Sync + 'static {
    /// Make the data written so far durable: the in-write chunk flush, and the whole-file
    /// piece where there is no range sync.
    fn chunk(&self, file: &File) -> io::Result<()>;
    /// Sync `[off, off + len)` (Linux local); `None` where there is no range sync.
    fn range(&self, file: &File, off: u64, len: u64) -> Option<io::Result<()>>;
    /// The final flush.
    fn finish(&self, file: &File) -> io::Result<()>;
    /// A counter that moves while one flush is blocked (Linux NFS `mountstats`).
    fn sample(&self) -> Option<u64>;
}

/// Every duration and size of the flusher (§2.10), production values by default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushTiming {
    /// T12: no progress for this long is `SyncTimeout`.
    pub(crate) stall: Duration,
    /// A chunk flush slower than this halves the chunk.
    pub(crate) slow_chunk: Duration,
    pub(crate) chunk_start: u64,
    pub(crate) chunk_min: u64,
    /// How often a blocked wait samples [`FlushOps::sample`].
    pub(crate) sample_every: Duration,
}

impl Default for FlushTiming {
    fn default() -> Self {
        Self {
            stall: FLUSH_STALL,
            slow_chunk: Duration::from_secs(15),
            chunk_start: 64 * 1024 * 1024,
            chunk_min: super::writeback::CHUNK_BYTES_MIN,
            sample_every: Duration::from_secs(1),
        }
    }
}

/// [`durable_sync_file`]'s piece sizing (§4.5), production values by default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableTiming {
    pub(crate) stall: Duration,
    /// A piece slower than this halves the window; one under a quarter of it doubles it.
    pub(crate) piece_target: Duration,
    pub(crate) window_start: u64,
    pub(crate) window_min: u64,
    pub(crate) sample_every: Duration,
}

impl Default for DurableTiming {
    fn default() -> Self {
        Self {
            stall: FLUSH_STALL,
            piece_target: Duration::from_secs(2),
            window_start: 64 * 1024 * 1024,
            window_min: 1024 * 1024,
            sample_every: Duration::from_secs(1),
        }
    }
}

/// Make `file` durable, reporting `on_progress(bytes_done, bytes_total)` on every piece
/// that completes (stop design §4.5).
///
/// Pieces are sized to finish in about 2 s: Linux local storage syncs consecutive windows
/// (starting at 64 MiB, halving when a piece takes over 2 s, floor 1 MiB, growing back when
/// fast) and then the file; Linux NFS runs one flush while the mount's `mountstats` write
/// counters report bytes; macOS (`F_FULLFSYNC`) and Windows (`FlushFileBuffers`) report the
/// one call's completion. Fails with `SyncTimeout` (E9056) only after 60 s with no
/// progress; a cancelled `halt` returns `Halted` at once, leaking the worker.
pub fn durable_sync_file(
    file: &File,
    halt: Option<&Halt>,
    on_progress: impl FnMut(u64, u64),
) -> io::Result<()> {
    durable_sync_file_with(
        file,
        halt,
        on_progress,
        os_ops(file),
        DurableTiming::default(),
    )
}

// `durable_sync_file` over `ops`, with its timing as a parameter.
pub(crate) fn durable_sync_file_with(
    file: &File,
    halt: Option<&Halt>,
    mut on_progress: impl FnMut(u64, u64),
    ops: Arc<dyn FlushOps>,
    timing: DurableTiming,
) -> io::Result<()> {
    let len = file.metadata()?.len();
    // An owned clone per worker call: a leaked worker keeps a valid fd, never a reused number.
    let file = Arc::new(file.try_clone()?);
    let progress = Progress::new();
    let mut timer = StallTimer::new(timing.stall, &progress);
    let mut sampler = Sampler::new(timing.sample_every, timing.stall);
    let mut done = 0u64;
    let mut window = timing.window_start.max(1);
    // Linux local: consecutive windows, each completion is progress (§4.5).
    while done < len {
        let piece = window.min(len - done);
        let (f, o, off) = (file.clone(), ops.clone(), done);
        let started = Instant::now();
        let ranged = bounded_syscall_stall(halt, &progress, &mut timer, &mut || {}, move || {
            o.range(&f, off, piece)
        })
        .map_err(|e| wait_failure(e, "range sync"))?;
        match ranged {
            None => break,
            Some(r) => r?,
        }
        done += piece;
        progress.bump();
        on_progress(done, len);
        let took = started.elapsed();
        window = if took > timing.piece_target {
            (window / 2).max(timing.window_min)
        } else if took < timing.piece_target / 4 {
            (window * 2).min(timing.window_start)
        } else {
            window
        };
    }
    // The final flush; while it is blocked, a sampled counter (NFS) is progress too.
    let reported = done;
    let (f, o) = (file.clone(), ops.clone());
    let finished = bounded_syscall_stall(
        halt,
        &progress,
        &mut timer,
        &mut || {
            if let Some(delta) = sampler.tick(&*ops) {
                progress.bump();
                done = (done + delta).min(len.saturating_sub(1)).max(done);
                on_progress(done, len);
            }
        },
        move || o.finish(&f),
    );
    finished.map_err(|e| wait_failure(e, "final flush"))??;
    if done < len || (len == 0 && reported == 0) {
        on_progress(len, len);
    }
    Ok(())
}

// Samples `FlushOps::sample` every `every` from a blocked wait: the counter's increase since
// the last sample, if any. Logs one INFO once a wait has lasted `note_after` (§2.10 item 4).
pub(crate) struct Sampler {
    every: Duration,
    note_after: Duration,
    started: Instant,
    noted: bool,
    last_at: Instant,
    last: Option<u64>,
}

impl Sampler {
    pub(crate) fn new(every: Duration, note_after: Duration) -> Self {
        let now = Instant::now();
        Self {
            every,
            note_after,
            started: now,
            noted: false,
            last_at: now,
            last: None,
        }
    }

    pub(crate) fn tick(&mut self, ops: &dyn FlushOps) -> Option<u64> {
        let now = Instant::now();
        if !self.noted && now.duration_since(self.started) >= self.note_after {
            self.noted = true;
            tracing::info!(
                target: "freemkv::io",
                waited_s = self.note_after.as_secs(),
                "durable flush still in progress; waiting while it makes progress"
            );
        }
        if now.duration_since(self.last_at) < self.every {
            return None;
        }
        self.last_at = now;
        let v = ops.sample()?;
        let prev = self.last.replace(v)?;
        (v > prev).then(|| v - prev)
    }
}

// Maps a failed wait onto the `io::Error` a flush returns; every arm means the flush did
// not observably complete (never `Ok`).
pub(crate) fn wait_failure(e: BoundedError, what: &str) -> io::Error {
    match e {
        BoundedError::Timeout => {
            tracing::error!(
                target: "freemkv::io",
                what,
                "durable flush made no progress for the stall window; data NOT durably flushed"
            );
            Error::SyncTimeout.into()
        }
        BoundedError::Halted => {
            tracing::warn!(target: "freemkv::io", what, "durable flush stopped (halt requested)");
            Error::Halted.into()
        }
        BoundedError::WorkerLost => {
            tracing::error!(target: "freemkv::io", what, "durable flush worker lost before completion");
            Error::SyncWorkerLost.into()
        }
    }
}

// The production primitives for `file`.
pub(crate) fn os_ops(file: &File) -> Arc<dyn FlushOps> {
    Arc::new(platform::OsFlushOps::for_file(file))
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use std::os::unix::io::AsRawFd;

    pub(super) struct OsFlushOps {
        // Local storage that takes `sync_file_range`: flush in windows.
        ranged: bool,
        // On NFS: the file's path, to find its mount in /proc/self/mountstats.
        nfs_path: Option<String>,
    }

    impl OsFlushOps {
        pub(super) fn for_file(file: &File) -> Self {
            use crate::platform::fs_type::{FsType, detect_fd};
            let fd = file.as_raw_fd();
            let nfs = detect_fd(fd) == FsType::Nfs;
            let regular = file.metadata().is_ok_and(|m| m.is_file());
            let nfs_path = nfs
                .then(|| std::fs::read_link(format!("/proc/self/fd/{fd}")).ok())
                .flatten()
                .map(|p| p.to_string_lossy().into_owned());
            Self {
                ranged: regular && !nfs,
                nfs_path,
            }
        }
    }

    fn rc(r: libc::c_int) -> io::Result<()> {
        if r == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    impl FlushOps for OsFlushOps {
        // SS-12 XSH fdatasync(): "shall force all currently queued I/O operations associated
        // with the file … to the synchronized I/O completion state".
        fn chunk(&self, file: &File) -> io::Result<()> {
            // SAFETY: a valid fd for the call's duration.
            rc(unsafe { libc::fdatasync(file.as_raw_fd()) })
        }

        // SS-13 sync_file_range(2): WAIT_BEFORE|WRITE|WAIT_AFTER "is a write-for-data-integrity
        // operation", but "does not flush disk write caches" or metadata: `finish` follows.
        fn range(&self, file: &File, off: u64, len: u64) -> Option<io::Result<()>> {
            if !self.ranged {
                return None;
            }
            let flags = libc::SYNC_FILE_RANGE_WAIT_BEFORE
                | libc::SYNC_FILE_RANGE_WRITE
                | libc::SYNC_FILE_RANGE_WAIT_AFTER;
            let (off, len) = (off as libc::off64_t, len as libc::off64_t);
            // SAFETY: a valid fd for the call's duration.
            match rc(unsafe { libc::sync_file_range(file.as_raw_fd(), off, len, flags) }) {
                // Refused, not failed (no range sync here): the whole-file flush covers it.
                Err(e)
                    if matches!(
                        e.raw_os_error(),
                        Some(libc::EINVAL | libc::ESPIPE | libc::ENOSYS | libc::EOPNOTSUPP)
                    ) =>
                {
                    None
                }
                r => Some(r),
            }
        }

        // SS-12 XSH fsync(): "shall not return until the system has completed that action".
        fn finish(&self, file: &File) -> io::Result<()> {
            // SAFETY: a valid fd for the call's duration.
            rc(unsafe { libc::fsync(file.as_raw_fd()) })
        }

        fn sample(&self) -> Option<u64> {
            let path = self.nfs_path.as_deref()?;
            let text = std::fs::read_to_string("/proc/self/mountstats").ok()?;
            parse_mountstats(&text, path)
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::os::unix::io::AsRawFd;

    pub(super) struct OsFlushOps;

    impl OsFlushOps {
        pub(super) fn for_file(_file: &File) -> Self {
            Self
        }
    }

    fn rc(r: libc::c_int) -> io::Result<()> {
        if r == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    impl FlushOps for OsFlushOps {
        // A plain fsync per chunk: cheap next to F_FULLFSYNC, and only the pages dirtied
        // since the last one (§2.10 macOS row).
        fn chunk(&self, file: &File) -> io::Result<()> {
            // SAFETY: a valid fd for the call's duration.
            rc(unsafe { libc::fsync(file.as_raw_fd()) })
        }

        fn range(&self, _file: &File, _off: u64, _len: u64) -> Option<io::Result<()>> {
            None
        }

        // SS-14 fcntl(2) F_FULLFSYNC: "Does the same thing as fsync(2) then asks the drive to
        // flush all buffered data"; only some filesystems implement it, so ENOTSUP → fsync.
        fn finish(&self, file: &File) -> io::Result<()> {
            let fd = file.as_raw_fd();
            // SAFETY: a valid fd for the calls' duration.
            match rc(unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) }) {
                Err(e) if e.raw_os_error() == Some(libc::ENOTSUP) => rc(unsafe { libc::fsync(fd) }),
                r => r,
            }
        }

        fn sample(&self) -> Option<u64> {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::*;

    pub(super) struct OsFlushOps;

    impl OsFlushOps {
        pub(super) fn for_file(_file: &File) -> Self {
            Self
        }
    }

    impl FlushOps for OsFlushOps {
        // SS-15 FlushFileBuffers: "writes all the buffered information for a specified file
        // to the device" — std's `sync_all` on Windows (no data-only flush exists there).
        fn chunk(&self, file: &File) -> io::Result<()> {
            file.sync_all()
        }

        fn range(&self, _file: &File, _off: u64, _len: u64) -> Option<io::Result<()>> {
            None
        }

        fn finish(&self, file: &File) -> io::Result<()> {
            file.sync_all()
        }

        fn sample(&self) -> Option<u64> {
            None
        }
    }
}

// The NFS client's write counter for the NFS mount holding `path` (longest mount point
// prefix) in `/proc/self/mountstats`: the `bytes:` line's 6th value (server-write bytes)
// plus the `WRITE:` and `COMMIT:` op counts. Any increase is progress (§2.10).
#[cfg(any(target_os = "linux", test))]
pub(crate) fn parse_mountstats(text: &str, path: &str) -> Option<u64> {
    let mut best: Option<(usize, u64)> = None;
    let mut current: Option<(usize, u64)> = None;
    let close = |cur: Option<(usize, u64)>, best: &mut Option<(usize, u64)>| {
        if let Some(c) = cur
            && best.is_none_or(|b| c.0 > b.0)
        {
            *best = Some(c);
        }
    };
    for line in text.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("device ") {
            close(current.take(), &mut best);
            current = nfs_block(rest, path).map(|depth| (depth, 0));
            continue;
        }
        let Some((_, total)) = current.as_mut() else {
            continue;
        };
        let mut fields = t.split_whitespace();
        let first = fields.next().unwrap_or("");
        let value = match first {
            "bytes:" => fields.nth(5),
            "WRITE:" | "COMMIT:" => fields.next(),
            _ => None,
        };
        *total += value.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    }
    close(current, &mut best);
    best.map(|b| b.1)
}

// For a `device` line's remainder: the mount point's length if it is an NFS mount holding
// `path`. Mount points escape a space as `\040`.
#[cfg(any(target_os = "linux", test))]
fn nfs_block(rest: &str, path: &str) -> Option<usize> {
    let (_, after) = rest.split_once(" mounted on ")?;
    let (mnt, fstype) = after.split_once(" with fstype ")?;
    if !fstype.starts_with("nfs") {
        return None;
    }
    let mnt = mnt.replace("\\040", " ");
    let under = path == mnt
        || mnt == "/"
        || path
            .strip_prefix(mnt.as_str())
            .is_some_and(|r| r.starts_with('/'));
    under.then_some(mnt.len())
}

#[cfg(test)]
mod tests;

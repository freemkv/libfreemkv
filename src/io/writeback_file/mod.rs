//! `WritebackFile` — a `File` wrapper whose reason for existing is the
//! bounded-cache writeback pipeline.
//!
//! Why: large sequential writes (sweep, patch, mux on UHD-scale output)
//! left to the kernel's default writeback policy accumulate hundreds of
//! megabytes of dirty pages and then burst-flush, stalling subsequent
//! writes for seconds at a time. `WritebackFile` drives a continuous
//! [`super::writeback::WritebackPipeline`] that on Linux issues
//! incremental `sync_file_range` + `posix_fadvise(DONTNEED)` calls at
//! 32 MB granularity so dirty pages drain at the same rate they're
//! produced. macOS and Windows fall through to a no-op pipeline — their
//! default cache policies have not been shown to exhibit the same
//! pathology for this access pattern.
//!
//! It implements `Write` and `Seek` so any call site that wrote to a
//! plain `File` through those traits (sweep, patch, mux) can swap in
//! `WritebackFile` without touching the body of the loop. The wrapper
//! also tracks the current file position to feed the pipeline with
//! progress + seek boundaries.
//!
//! See `super::writeback::linux` for the underlying pathology and the
//! strategy.
//!
//! ## Platform split
//!
//! The platform-specific pieces of this wrapper — extent preallocation
//! (Linux `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`, Windows
//! `SetFileValidData`) and the durable-flush primitive (Linux/macOS
//! `fsync`/`F_FULLFSYNC` wrapped in a bounded syscall, Windows
//! `FlushFileBuffers`) — live in per-OS sibling modules. The dispatch
//! happens once at the bottom of this file via cfg-gated `mod` decls.
//! No inline `#[cfg(target_os = "...")]` in the business-logic above.
//!
//! ## Phase 2.5 — write-side flatness (writer thread)
//!
//! `WritebackFile` is split into a thin muxer-facing handle and a
//! dedicated writer thread that owns the real `File` + writeback
//! pipeline. The muxer's `Write::write` and `Seek::seek` calls return as
//! soon as the byte handoff to a bounded SPSC ring completes; the writer
//! thread executes the real syscalls (incl. `sync_file_range(WAIT_AFTER)`
//! on Linux) without ever blocking the muxer on a kernel commit.
//!
//! ### Backpressure
//!
//! The ring is byte-bounded at [`RING_CAPACITY_BYTES`]. When the ring is
//! full the muxer's `write` blocks on a condvar until the writer thread
//! drains enough bytes to admit the next chunk. **Backpressure on
//! ring-full deliberately blocks the muxer rather than dropping bytes**
//! — archival workflows cannot afford byte loss, and the kernel page
//! cache is already a second buffering layer underneath the writer
//! thread.
//!
//! ### MKV seek-back semantics
//!
//! The MKV container backpatches cluster size headers shortly after
//! emitting them. The writer thread maintains an
//! [`ActiveClusterBuffer`] tracking the last
//! [`ACTIVE_CLUSTER_WINDOW_BYTES`] bytes by absolute file position. When
//! the muxer issues `Seek(pos)` for a `pos` inside that window, the
//! writer issues a real `file.seek` (cheap) but **skips
//! `pipeline.handle_seek()`** — no `sync_file_range(WAIT_AFTER)` drain
//! is forced for the current chunk. This is the dominant case: every
//! cluster backpatch is within the current 32 MiB writeback chunk.
//!
//! For seeks **outside** the window (rare — Cues index write at the end
//! of mux, Segment header backpatch right before close) the writer
//! falls back to the pre-Phase-2.5 behaviour: drain the in-flight
//! writeback via `pipeline.handle_seek()`, then issue the real seek.
//!
//! ### Halt-safe
//!
//! All real `sync_file_range(WAIT_AFTER)` and `fsync` calls on the
//! writer thread route through [`crate::io::bounded::bounded_syscall`]
//! with a 60 s deadline (already in place in the per-OS modules pre-
//! Phase-2.5). A wedged NFS server cannot freeze the writer thread
//! indefinitely; the muxer keeps queueing into the ring; the kernel
//! page cache absorbs.
//!
//! ### `sync_all` semantics
//!
//! `sync_all` is synchronous: it drains the ring through the writer
//! thread, then runs the per-OS durable-flush primitive, and returns
//! the result to the caller. This is the API contract — callers
//! (sweep/patch consumers, mux finalisation) rely on it.
//!
//! ### `speed_mbs` reporting
//!
//! Speed measurements taken at the muxer side (bytes handed off into
//! the ring) reflect ring-handoff throughput, **not** bytes committed
//! to durable storage. This is the correct number for muxer flatness
//! reporting; sweep/patch use mapfile-based progress which is unrelated.
//! Autorip's UI is unaffected — speed is calculated outside this
//! module — but the distinction is worth noting in release notes if the
//! UI ever exposes "throughput vs commit rate" separately.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod other;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use other as platform;
#[cfg(target_os = "windows")]
use windows as platform;

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use super::writeback::WritebackPipeline;

/// Granularity at which the Linux writeback pipeline issues
/// `sync_file_range` / `posix_fadvise(DONTNEED)` pairs. 32 MiB is the
/// historical default — bounded-cache pressure stays at ~2 × this size.
const WRITEBACK_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

/// Maximum bytes outstanding in the muxer → writer-thread ring. Sized
/// to cover ~4 s of muxer output at a 32 MB/s peak — enough to absorb a
/// short NFS commit stall without dropping the muxer's effective
/// throughput, but not so large that the resident-memory footprint
/// grows unbounded under a long writeback stall.
const RING_CAPACITY_BYTES: usize = 128 * 1024 * 1024;

/// Bytes the writer thread keeps in [`ActiveClusterBuffer`] for the
/// in-window seek-then-patch fast path. Matches [`WRITEBACK_CHUNK_BYTES`]
/// so that a cluster backpatch landing inside the most recently written
/// (but not yet WAIT_AFTER'd) writeback chunk doesn't force a drain.
const ACTIVE_CLUSTER_WINDOW_BYTES: u64 = WRITEBACK_CHUNK_BYTES;

/// Thread name used for the writer thread. Visible to OS-level tooling
/// (`ps -L`, `top -H`) so operators can correlate the muxer's flatness
/// with this thread's activity.
const WRITER_THREAD_NAME: &str = "freemkv-writeback-writer";

/// Single chunk size cap for batching: when a `Write` command arrives
/// the muxer copies its slice into a `Vec<u8>` to hand ownership over
/// the ring. We keep the allocation a single contiguous buffer — no
/// internal segmentation — so the writer thread can pass the slice
/// straight to `File::write_all` and the kernel can coalesce.
const MAX_WRITE_CHUNK_BYTES: usize = RING_CAPACITY_BYTES; // soft cap: a single command may not exceed the ring

/// One command on the muxer → writer-thread ring.
enum Cmd {
    /// Write `buf.len()` bytes at the writer's current logical
    /// position, then advance.
    Write(Vec<u8>),
    /// Seek to `from` against the writer-side `File`.
    Seek(SeekFrom),
    /// Flush the muxer-side `Write::flush()` request (rarely useful;
    /// kept for trait completeness). The writer ignores it — the real
    /// flushing happens on `SyncAll`.
    Flush,
    /// Drain the ring then run the per-OS durable-sync primitive and
    /// signal `done` with the result.
    SyncAll { done: SyncSender<io::Result<()>> },
    /// Drain the ring (final pipeline finalize for chunk tail). Signal
    /// `done` so `Drop` can wait synchronously. No fsync — that's what
    /// `SyncAll` is for.
    Finish { done: SyncSender<()> },
}

/// Ring state behind the muxer/writer condvar. `bytes_inflight` tracks
/// the sum of `Write(buf).len()` bytes currently queued so backpressure
/// can be enforced on a byte budget rather than a per-command count.
struct RingState {
    queue: VecDeque<Cmd>,
    /// Total `Write` payload bytes currently in `queue`. Non-write
    /// commands (`Seek`, `Flush`, `SyncAll`, `Finish`) don't count
    /// against the budget.
    bytes_inflight: usize,
    /// Set by the writer thread when it observes a fatal error (a
    /// failed `write_all` or `seek` on the underlying file). Once set,
    /// the muxer's next `write` / `seek` returns the error and stops
    /// queueing.
    sticky_error: Option<io::ErrorKind>,
    /// Set when the writer thread has exited (clean Finish, panic, or
    /// channel closed). Muxer-side ops surface this as a broken-pipe.
    writer_gone: bool,
}

struct Shared {
    state: Mutex<RingState>,
    /// Notified when the writer dequeues something (bytes free up) or
    /// when the writer exits.
    space_available: Condvar,
    /// Notified when the muxer pushes a new command.
    work_available: Condvar,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: Mutex::new(RingState {
                queue: VecDeque::new(),
                bytes_inflight: 0,
                sticky_error: None,
                writer_gone: false,
            }),
            space_available: Condvar::new(),
            work_available: Condvar::new(),
        }
    }
}

/// Tiny ring of recently-written bytes indexed by absolute file
/// position. Used by the writer thread to decide whether a `Seek`
/// target falls inside the active writeback chunk; if so, the seek
/// proceeds without forcing the pipeline to drain (the dominant case
/// for MKV cluster-size backpatches).
///
/// The bytes themselves are kept in a contiguous `VecDeque<u8>` whose
/// front corresponds to file position [`lo`]. The data on disk is the
/// authoritative copy — `ActiveClusterBuffer` is a read-only mirror
/// used for in-window patch verification in tests and (potentially) for
/// future in-buffer mutation, NOT a write-through cache.
///
/// [`lo`]: Self::lo
struct ActiveClusterBuffer {
    /// Window capacity in bytes. The ring trims from the front to stay
    /// at or below this size after every `push`.
    cap: u64,
    /// Absolute file position of the byte at `data.front()`.
    lo: u64,
    data: VecDeque<u8>,
}

impl ActiveClusterBuffer {
    fn new(cap: u64) -> Self {
        Self {
            cap,
            lo: 0,
            data: VecDeque::with_capacity(cap as usize),
        }
    }

    /// Reset the window — used after an out-of-window seek where the
    /// previous data is no longer adjacent to the new position.
    fn reset(&mut self, new_lo: u64) {
        self.data.clear();
        self.lo = new_lo;
    }

    /// One contiguous logical span of file positions currently held.
    fn hi(&self) -> u64 {
        self.lo + self.data.len() as u64
    }

    /// True if `pos` is in `[lo, hi]` (hi is exclusive of bytes but
    /// inclusive of the seek-to-end-of-cluster boundary).
    fn contains(&self, pos: u64) -> bool {
        pos >= self.lo && pos <= self.hi()
    }

    /// Append `bytes` at absolute file position `start`. If `start` is
    /// contiguous with `hi()`, the bytes extend the window; otherwise
    /// the window is reset (the previous data is no longer adjacent and
    /// would corrupt the position index).
    fn push(&mut self, start: u64, bytes: &[u8]) {
        let h = self.hi();
        if start == h {
            // Contiguous append.
            self.data.extend(bytes.iter().copied());
        } else if start >= self.lo && start <= h {
            // Patch landing inside the window: overwrite from
            // (start - lo) for bytes.len(), then extend if it spills
            // past `hi`.
            let offset = (start - self.lo) as usize;
            let mut bi = 0usize;
            while bi < bytes.len() && offset + bi < self.data.len() {
                self.data[offset + bi] = bytes[bi];
                bi += 1;
            }
            if bi < bytes.len() {
                self.data.extend(bytes[bi..].iter().copied());
            }
        } else {
            // Non-contiguous: drop the window and reseat.
            self.data.clear();
            self.lo = start;
            self.data.extend(bytes.iter().copied());
        }
        // Trim from the front so the window stays at or below `cap`.
        while self.data.len() as u64 > self.cap {
            self.data.pop_front();
            self.lo += 1;
        }
    }
}

/// Muxer-facing handle. Holds a sender into the bounded ring and a
/// `JoinHandle` for the writer thread. `Write`/`Seek`/`sync_all`/`Drop`
/// route through the ring; the muxer thread is never trapped on a
/// commit syscall.
pub(crate) struct WritebackFile {
    shared: Arc<Shared>,
    /// Joined on `Drop` after `Finish` so the writer thread's exit is
    /// observed and any panic is surfaced loudly. `Option` so `Drop`
    /// can `take()` it.
    writer: Option<JoinHandle<()>>,
    /// Logical file position from the muxer's point of view. Updated on
    /// `write`/`write_all` (advanced by the count) and on `seek`
    /// (replaced by the new position). Mirrors what the writer thread
    /// will end up at once it has drained all queued commands —
    /// callers that need a stream_position can read this without
    /// blocking on the writer.
    muxer_pos: u64,
}

impl WritebackFile {
    /// Wrap an open `File`. The current OS file position is queried
    /// once so the writer thread starts tracking from wherever the
    /// file already is (typically 0 for fresh files; non-zero for
    /// resumed or appended files).
    pub(crate) fn new(mut file: File) -> io::Result<Self> {
        let pos = file.stream_position()?;
        Ok(Self::spawn(file, pos))
    }

    /// Create a new file at `path` (truncating any existing contents)
    /// and wrap it. Convenience for the common
    /// `File::create(path)` + `WritebackFile::new(file)` pair so callers
    /// don't have to assemble a `File` first.
    ///
    /// Callers that know the target output size should prefer
    /// [`Self::create_with_size_hint`] so the kernel can pre-reserve
    /// extents.
    #[allow(dead_code)]
    pub(crate) fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Self::new(file)
    }

    /// Like [`Self::create`] but pre-reserves `size_bytes` of disk
    /// space via the platform's extent-preallocation primitive (Linux
    /// `fallocate(KEEP_SIZE)`, macOS `F_PREALLOCATE`, Windows
    /// `SetFileValidData` stub). The reported file size is unchanged
    /// (writes still grow the file naturally) — only the on-disk extent
    /// allocation is preallocated, which reduces extent fragmentation
    /// on large sequential writes (mux output, especially on slow
    /// storage / NFS).
    ///
    /// On platforms without an extent-preallocation primitive this is
    /// equivalent to `create` — the size hint is dropped after a debug
    /// log.
    pub(crate) fn create_with_size_hint(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = File::create(path)?;
        platform::preallocate(&file, size_bytes);
        Self::new(file)
    }

    /// Open an existing file at `path` for writing (no truncation) and
    /// wrap it. Mirrors `File::open` semantics for the writable case
    /// — used by patch / resume paths that mutate an existing ISO in
    /// place.
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).open(path)?;
        Self::new(file)
    }

    /// Spawn the writer thread for `file` starting at logical
    /// position `start_pos`. The writer takes ownership of the `File`;
    /// the muxer keeps the handle.
    fn spawn(file: File, start_pos: u64) -> Self {
        let shared = Arc::new(Shared::new());
        let shared_w = Arc::clone(&shared);
        let writer = thread::Builder::new()
            .name(WRITER_THREAD_NAME.into())
            .spawn(move || {
                writer_thread_main(file, start_pos, shared_w);
            })
            .expect("writer thread spawn");
        Self {
            shared,
            writer: Some(writer),
            muxer_pos: start_pos,
        }
    }

    /// Drain in-flight writeback then issue a full fsync. Use this in
    /// place of `File::sync_all`.
    ///
    /// Blocks the calling thread until the ring is fully drained AND
    /// the per-OS durable-flush primitive has returned. The flush
    /// itself runs on the writer thread, wrapped in
    /// [`crate::io::bounded::bounded_syscall`] (60 s deadline on
    /// Linux + macOS); a wedged NFS server cannot trap the muxer.
    pub(crate) fn sync_all(&mut self) -> io::Result<()> {
        let (tx, rx) = sync_channel::<io::Result<()>>(0);
        self.push_command(Cmd::SyncAll { done: tx }, 0)?;
        // recv() blocks until the writer thread drains the ring up to
        // the SyncAll command, runs the per-OS durable-sync, and sends
        // the result back.
        match rx.recv() {
            Ok(r) => r,
            Err(_) => {
                // Writer thread exited without sending. Surface a
                // distinct error kind so the caller can distinguish
                // "writer panicked" from a normal fsync failure.
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
        }
    }

    /// Push a single command onto the ring. `bytes_charge` is the
    /// number of bytes this command contributes to the byte-budget
    /// backpressure check; only `Write` commands contribute.
    fn push_command(&mut self, cmd: Cmd, bytes_charge: usize) -> io::Result<()> {
        let mut guard = self.shared.state.lock().unwrap();
        // Surface any sticky error from the writer thread before
        // queueing more work. The muxer should stop pushing once the
        // writer has reported a failure.
        if let Some(kind) = guard.sticky_error {
            return Err(io::Error::from(kind));
        }
        if guard.writer_gone {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        // Byte-budget backpressure: wait for space if this would
        // overflow the cap. A single command larger than the cap is
        // admitted regardless (the cap is a soft target for batching;
        // a giant single write still fits because the channel itself
        // is unbounded count-wise).
        while bytes_charge > 0
            && guard.bytes_inflight + bytes_charge > RING_CAPACITY_BYTES
            && guard.bytes_inflight > 0
        {
            guard = self.shared.space_available.wait(guard).unwrap();
            if let Some(kind) = guard.sticky_error {
                return Err(io::Error::from(kind));
            }
            if guard.writer_gone {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
        }
        guard.queue.push_back(cmd);
        guard.bytes_inflight += bytes_charge;
        drop(guard);
        self.shared.work_available.notify_one();
        Ok(())
    }
}

impl Write for WritebackFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // `Write::write` is allowed to be partial; we always accept
        // the full slice (handoff is in-process) and report `buf.len()`.
        // Callers that need the partial-write semantic still get the
        // strict guarantee documented on `write_all`.
        let n = buf.len();
        if n == 0 {
            return Ok(0);
        }
        if n > MAX_WRITE_CHUNK_BYTES {
            // Defensive: a single command bigger than the ring can't
            // be admitted by the backpressure check above without
            // deadlocking against itself. Split into ring-sized
            // chunks.
            let mut off = 0;
            while off < n {
                let take = (n - off).min(MAX_WRITE_CHUNK_BYTES);
                self.push_command(Cmd::Write(buf[off..off + take].to_vec()), take)?;
                off += take;
            }
        } else {
            self.push_command(Cmd::Write(buf.to_vec()), n)?;
        }
        self.muxer_pos += n as u64;
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // `Write::write_all` default delegates to `write` in a loop. We
        // can do better: a single handoff per call, never partial. Same
        // chunk-split for the absurd-large case.
        let n = buf.len();
        if n == 0 {
            return Ok(());
        }
        if n > MAX_WRITE_CHUNK_BYTES {
            let mut off = 0;
            while off < n {
                let take = (n - off).min(MAX_WRITE_CHUNK_BYTES);
                self.push_command(Cmd::Write(buf[off..off + take].to_vec()), take)?;
                off += take;
            }
        } else {
            self.push_command(Cmd::Write(buf.to_vec()), n)?;
        }
        self.muxer_pos += n as u64;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        // The writer thread's view of `Flush` is a no-op (real flushing
        // happens on `SyncAll`). We still send it so a future change
        // could intercept it; for now the queue ordering is the only
        // observable effect.
        self.push_command(Cmd::Flush, 0)
    }
}

impl Seek for WritebackFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        // The muxer's logical position must update synchronously so
        // subsequent `write` calls advance from the right base, but the
        // writer thread is the only place that has the authoritative
        // OS file position. We model the muxer's `muxer_pos` purely
        // from `SeekFrom::Start(n)` (the dominant case for MKV
        // backpatch) and bounce other variants through to the writer
        // by querying its current position via a synchronous round.
        let new_pos = match from {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(d) => {
                let base = self.muxer_pos as i64;
                let p = base
                    .checked_add(d)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
                if p < 0 {
                    return Err(io::Error::from(io::ErrorKind::InvalidInput));
                }
                p as u64
            }
            SeekFrom::End(_) => {
                // SeekFrom::End requires the OS file's current EOF. We
                // do not maintain that on the muxer side; this branch
                // is not used by the MKV muxer (it always seeks with
                // `SeekFrom::Start`). If a future caller needs it, the
                // path is: SyncAll → real seek → query position back.
                // Reject explicitly so a regression is loud.
                return Err(io::Error::from(io::ErrorKind::Unsupported));
            }
        };
        self.push_command(Cmd::Seek(SeekFrom::Start(new_pos)), 0)?;
        self.muxer_pos = new_pos;
        Ok(new_pos)
    }
}

impl Drop for WritebackFile {
    fn drop(&mut self) {
        // Send a final `Finish` so the writer drains the ring (running
        // the pipeline's tail finalize for the last in-flight chunk)
        // before exiting. `sync_all` is *not* called from here — the
        // existing pre-Phase-2.5 contract is "Drop runs finalize, not
        // fsync", and Drop returning an io::Error is impossible anyway.
        let (tx, rx) = sync_channel::<()>(0);
        // If the writer thread already exited (sticky error path, or
        // an earlier panic), the queue push will fail with
        // BrokenPipe; we treat that as "nothing to drain" and proceed
        // to join.
        let push_ok = {
            let mut guard = match self.shared.state.lock() {
                Ok(g) => g,
                Err(poison) => {
                    // The writer panicked; recover the mutex so we
                    // can still observe `writer_gone`.
                    poison.into_inner()
                }
            };
            if guard.writer_gone {
                false
            } else {
                guard.queue.push_back(Cmd::Finish { done: tx });
                self.shared.work_available.notify_one();
                true
            }
        };
        if push_ok {
            // Block until the writer signals Finish completed (ring is
            // drained, pipeline finalize ran). The wait is bounded
            // only by the writer's per-syscall deadlines; if the
            // writer panicked between the push and the recv, the
            // sender is dropped and `recv` returns Err — proceed to
            // join.
            let _ = rx.recv();
        }
        if let Some(jh) = self.writer.take() {
            // `join` surfaces a panic. We re-raise it loudly: a
            // writer-thread panic indicates an io-layer bug, and
            // swallowing it would mask data loss.
            if let Err(panic) = jh.join() {
                tracing::error!(
                    target: "mux",
                    "WritebackFile writer thread panicked during Drop; data may be lost"
                );
                // Re-raise during drop is allowed (terminates the
                // process), but doing so from Drop can cause a double-
                // panic if the caller is already unwinding. Compromise:
                // log loudly and resume_unwind only outside of an
                // ongoing unwind.
                if !std::thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }
}

/// Writer-thread entry. Owns the `File` and the `WritebackPipeline`;
/// pulls commands from the shared ring and executes them. Exits when a
/// `Finish` command is observed (clean shutdown from `Drop`) or when
/// the muxer side disconnects (every `Arc<Shared>` cloned by the
/// handle is dropped — only happens on a forgotten-handle bug, which
/// the panic-surface in `Drop::join` catches).
fn writer_thread_main(file: File, start_pos: u64, shared: Arc<Shared>) {
    let pipeline = WritebackPipeline::new(&file, start_pos, WRITEBACK_CHUNK_BYTES);
    let mut state = WriterState {
        file,
        pipeline,
        pos: start_pos,
        active: ActiveClusterBuffer::new(ACTIVE_CLUSTER_WINDOW_BYTES),
        shared: Arc::clone(&shared),
    };
    state.run();
}

/// All writer-thread-owned state. Methods here run exclusively on the
/// writer thread — no Send/Sync concerns inside the body.
struct WriterState {
    file: File,
    pipeline: WritebackPipeline,
    /// Authoritative OS file position. Tracked locally so we can decide
    /// whether a `Seek` is a no-op (target equals current position).
    pos: u64,
    active: ActiveClusterBuffer,
    shared: Arc<Shared>,
}

/// Target byte budget for coalescing consecutive `Cmd::Write` commands
/// into a single `file.write_all` syscall. Sized to match NFS wsize on
/// rip1 (1 MiB), which is also the kernel default for most filesystems
/// and a reasonable upper bound for a single block-layer write on any
/// medium. The medium-specific syscall path may split further; that's
/// the kernel's job, not ours.
///
/// 0.21.11 added coalescing because pre-coalescing the writer thread
/// emitted one syscall per muxer `Write` call (typically 30-200 KB PES
/// frames), and on NFS that translates to one RPC per syscall, capping
/// throughput at `(wsize / rtt) × inflight` — well below what the same
/// disk delivers under a 1 MiB `dd oflag=direct` workload (~71 MB/s
/// empirical 2026-05-15 vs ~25 MB/s sustained mux). Bigger app writes
/// = fewer NFS RPCs = better throughput on the same hardware.
const WRITE_COALESCE_TARGET_BYTES: usize = 1024 * 1024;

/// One unit of work pulled off the ring. Either a coalesced run of
/// consecutive `Cmd::Write` commands (batched into a single
/// `file.write_all`) or a single non-write command.
enum DequeuedWork {
    /// At least one consecutive `Cmd::Write` buffer, total length
    /// bounded by [`WRITE_COALESCE_TARGET_BYTES`] (except when a
    /// single `Cmd::Write` already exceeds the budget — in which case
    /// it's returned alone). Coalesced and issued to the kernel as one
    /// write_all.
    Writes(Vec<Vec<u8>>),
    /// A single non-`Write` command (Seek / Flush / SyncAll / Finish).
    Other(Cmd),
}

impl WriterState {
    fn run(&mut self) {
        loop {
            let work = match self.dequeue_work() {
                Some(w) => w,
                None => {
                    // All senders dropped (handle leaked); mark
                    // writer_gone and exit. The Drop join will surface
                    // this if anyone cares.
                    self.mark_writer_gone();
                    return;
                }
            };
            match work {
                DequeuedWork::Writes(bufs) => {
                    // Single-buffer fast path: skip the concat alloc.
                    // For the multi-buffer case, concatenate into a
                    // single contiguous slice so the kernel sees one
                    // write_all syscall — on NFS this becomes one RPC
                    // (per inflight slot) instead of N small RPCs,
                    // which is the whole point.
                    let result = if bufs.len() == 1 {
                        self.do_write(&bufs[0])
                    } else {
                        let total: usize = bufs.iter().map(|b| b.len()).sum();
                        let mut concat = Vec::with_capacity(total);
                        for b in &bufs {
                            concat.extend_from_slice(b);
                        }
                        self.do_write(&concat)
                    };
                    if let Err(e) = result {
                        self.publish_error(e.kind());
                    }
                }
                DequeuedWork::Other(Cmd::Write(_)) => {
                    // dequeue_work places all Writes into DequeuedWork::Writes.
                    unreachable!("DequeuedWork::Other never wraps Cmd::Write");
                }
                DequeuedWork::Other(Cmd::Seek(from)) => {
                    if let Err(e) = self.do_seek(from) {
                        self.publish_error(e.kind());
                    }
                }
                DequeuedWork::Other(Cmd::Flush) => {
                    // No-op for now (see note on `Cmd::Flush`).
                }
                DequeuedWork::Other(Cmd::SyncAll { done }) => {
                    let r = self.do_sync_all();
                    // Ignore send errors: if the muxer dropped the
                    // receiver (cancelled wait), there's nothing to
                    // do.
                    let _ = done.send(r);
                }
                DequeuedWork::Other(Cmd::Finish { done }) => {
                    // Drain pipeline tail; do not fsync. Mark
                    // `writer_gone` so any racing `push_command` after
                    // this returns BrokenPipe instead of queueing into
                    // a dead writer.
                    self.pipeline.finalize();
                    self.mark_writer_gone();
                    let _ = done.send(());
                    return;
                }
            }
        }
    }

    /// Block until at least one command is available, then drain a
    /// unit of work. Consecutive `Cmd::Write` commands at the front of
    /// the queue are coalesced into a single `DequeuedWork::Writes`
    /// (up to [`WRITE_COALESCE_TARGET_BYTES`] total). Non-write
    /// commands break the run and are returned as `DequeuedWork::Other`
    /// one at a time, preserving their ordering relative to writes.
    /// Notifies the muxer side once after the dequeue completes so
    /// ring-full waiters wake.
    fn dequeue_work(&mut self) -> Option<DequeuedWork> {
        let mut guard = self.shared.state.lock().unwrap();
        loop {
            if guard.queue.is_empty() {
                guard = self.shared.work_available.wait(guard).unwrap();
                continue;
            }
            // Branch on whether the front is a Write or something else.
            if matches!(guard.queue.front(), Some(Cmd::Write(_))) {
                let mut bufs: Vec<Vec<u8>> = Vec::new();
                let mut total_bytes: usize = 0;
                while let Some(front_len) = guard.queue.front().and_then(|c| match c {
                    Cmd::Write(b) => Some(b.len()),
                    _ => None,
                }) {
                    // Always admit the first write (even if oversize)
                    // so a giant single buffer still makes progress.
                    // Stop before exceeding the target on subsequent
                    // additions.
                    if !bufs.is_empty() && total_bytes + front_len > WRITE_COALESCE_TARGET_BYTES {
                        break;
                    }
                    match guard.queue.pop_front() {
                        Some(Cmd::Write(b)) => {
                            total_bytes += b.len();
                            guard.bytes_inflight = guard.bytes_inflight.saturating_sub(b.len());
                            bufs.push(b);
                        }
                        _ => unreachable!("front was Cmd::Write per the peek above"),
                    }
                }
                drop(guard);
                self.shared.space_available.notify_all();
                return Some(DequeuedWork::Writes(bufs));
            } else {
                let cmd = guard.queue.pop_front().expect("front was non-empty");
                drop(guard);
                self.shared.space_available.notify_all();
                return Some(DequeuedWork::Other(cmd));
            }
        }
    }

    fn do_write(&mut self, buf: &[u8]) -> io::Result<()> {
        let start = self.pos;
        self.file.write_all(buf)?;
        self.pos += buf.len() as u64;
        self.pipeline.note_progress(self.pos);
        self.active.push(start, buf);
        Ok(())
    }

    fn do_seek(&mut self, from: SeekFrom) -> io::Result<()> {
        // We only ever push `SeekFrom::Start(n)` from the handle.
        let target = match from {
            SeekFrom::Start(n) => n,
            // Defensive: should not occur on the wire, but handle
            // gracefully.
            SeekFrom::Current(_) | SeekFrom::End(_) => {
                let p = self.file.seek(from)?;
                self.pos = p;
                self.active.reset(p);
                self.pipeline.handle_seek(p);
                return Ok(());
            }
        };
        if target == self.pos {
            // No-op seek (common: sweep emits `seek(Current(pos))`
            // before every write). Skip the syscall.
            return Ok(());
        }
        if self.active.contains(target) {
            // In-window seek: the target is inside the current
            // writeback chunk's data. The kernel page cache already
            // has those bytes; we issue the real `seek` (cheap, no
            // commit syscall) but **skip** the pipeline's
            // `handle_seek` so no `sync_file_range(WAIT_AFTER)` drain
            // is forced. Subsequent writes still call
            // `pipeline.note_progress` from the new position, so the
            // writeback chunk accounting stays coherent — the chunk
            // simply gets "re-emitted" data over its tail bytes,
            // which is what the MKV backpatch is.
            tracing::trace!(
                target: "mux",
                "WritebackFile in-window seek pos={} -> {} window=[{},{}]",
                self.pos,
                target,
                self.active.lo,
                self.active.hi(),
            );
            self.file.seek(SeekFrom::Start(target))?;
            self.pos = target;
        } else {
            // Out-of-window seek: rare (segment-header / Cues backpatch
            // at end of mux). Drain in-flight writeback so the kernel
            // doesn't carry dirty pages across the seek discontinuity,
            // then do the real seek.
            tracing::debug!(
                target: "mux",
                "WritebackFile out-of-window seek pos={} -> {} window=[{},{}]",
                self.pos,
                target,
                self.active.lo,
                self.active.hi(),
            );
            self.pipeline.handle_seek(target);
            self.file.seek(SeekFrom::Start(target))?;
            self.pos = target;
            self.active.reset(target);
        }
        Ok(())
    }

    fn do_sync_all(&mut self) -> io::Result<()> {
        self.pipeline.finalize();
        platform::durable_sync(&self.file)
    }

    fn publish_error(&self, kind: io::ErrorKind) {
        let mut guard = self.shared.state.lock().unwrap();
        if guard.sticky_error.is_none() {
            guard.sticky_error = Some(kind);
        }
        drop(guard);
        // Wake any muxer thread waiting on space — it will observe
        // sticky_error and return.
        self.shared.space_available.notify_all();
    }

    fn mark_writer_gone(&self) {
        let mut guard = self.shared.state.lock().unwrap();
        guard.writer_gone = true;
        drop(guard);
        self.shared.space_available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn read_back(path: &Path) -> Vec<u8> {
        let mut f = File::open(path).unwrap();
        let mut v = Vec::new();
        f.read_to_end(&mut v).unwrap();
        v
    }

    #[test]
    fn write_then_drop_persists_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        {
            let mut w = WritebackFile::create(&p).unwrap();
            w.write_all(b"hello world").unwrap();
            // Drop drains the ring.
        }
        assert_eq!(read_back(&p), b"hello world");
    }

    #[test]
    fn sync_all_blocks_until_ring_drains() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("b.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        for _ in 0..32 {
            w.write_all(&[0x5au8; 1024]).unwrap();
        }
        // After sync_all, the bytes MUST be visible to a separate
        // reader. The ring has been drained and durable-sync has run.
        w.sync_all().unwrap();
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 32 * 1024);
        assert!(bytes.iter().all(|&b| b == 0x5a));
        drop(w);
    }

    #[test]
    fn in_window_seek_then_patch_roundtrip() {
        // Write A; seek back inside the active-cluster window; patch
        // with B; read back; the patch lands at the right offset.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        // 4 KiB of 'A' (well within ACTIVE_CLUSTER_WINDOW_BYTES =
        // 32 MiB, so the seek-back is guaranteed in-window).
        let big = vec![b'A'; 4096];
        w.write_all(&big).unwrap();
        // Seek back to offset 1000 and overwrite 8 bytes.
        w.seek(SeekFrom::Start(1000)).unwrap();
        w.write_all(b"PATCHED!").unwrap();
        // Seek to end so subsequent reads see the right size.
        w.sync_all().unwrap();
        drop(w);
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 4096);
        assert_eq!(&bytes[1000..1008], b"PATCHED!");
        // Bytes outside the patch are still 'A'.
        assert_eq!(bytes[999], b'A');
        assert_eq!(bytes[1008], b'A');
    }

    #[test]
    fn out_of_window_seek_then_patch_roundtrip() {
        // Write enough bytes that a seek to offset 0 is outside the
        // active-cluster window (which is 32 MiB). To keep the test
        // bounded, we hammer the ActiveClusterBuffer's `cap` field
        // directly via the public Write path — 33 MiB of payload is
        // sufficient.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        // Write 33 MiB of 'A'; the first 1 MiB is now outside the
        // 32 MiB active-cluster window.
        let chunk = vec![b'A'; 1024 * 1024];
        for _ in 0..33 {
            w.write_all(&chunk).unwrap();
        }
        // Seek to offset 100 (definitely outside the window) and
        // patch.
        w.seek(SeekFrom::Start(100)).unwrap();
        w.write_all(b"OUTSIDE!").unwrap();
        w.sync_all().unwrap();
        drop(w);
        let bytes = read_back(&p);
        assert_eq!(bytes.len(), 33 * 1024 * 1024);
        assert_eq!(&bytes[100..108], b"OUTSIDE!");
        // Surrounding bytes are still 'A'.
        assert_eq!(bytes[99], b'A');
        assert_eq!(bytes[108], b'A');
    }

    #[test]
    fn backpressure_blocks_when_ring_full() {
        // Submit more bytes than RING_CAPACITY_BYTES; if backpressure
        // works, the call sequence still completes once the writer
        // drains. We measure that the total written matches and the
        // calling thread did not panic / loop forever.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("e.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        // 4 × RING_CAPACITY_BYTES of payload, in chunks small enough
        // that several can fit in the ring at once and backpressure
        // triggers naturally.
        let total = RING_CAPACITY_BYTES.saturating_mul(2) + (RING_CAPACITY_BYTES / 2);
        let chunk = vec![0u8; 1024 * 1024];
        let mut written = 0;
        while written < total {
            let take = (total - written).min(chunk.len());
            w.write_all(&chunk[..take]).unwrap();
            written += take;
        }
        w.sync_all().unwrap();
        drop(w);
        let meta = std::fs::metadata(&p).unwrap();
        assert_eq!(meta.len() as usize, total);
    }

    #[test]
    fn flush_is_observed_in_order() {
        // `Write::flush` is a no-op on the writer side but must not
        // panic or leak. Run an interleaved sequence and verify the
        // bytes still land in order.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.bin");
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"one").unwrap();
        w.flush().unwrap();
        w.write_all(b"two").unwrap();
        w.flush().unwrap();
        w.write_all(b"three").unwrap();
        w.sync_all().unwrap();
        drop(w);
        assert_eq!(read_back(&p), b"onetwothree");
    }

    #[test]
    fn active_cluster_buffer_contiguous_append_and_trim() {
        let mut b = ActiveClusterBuffer::new(8);
        b.push(0, b"abcd");
        assert_eq!(b.lo, 0);
        assert_eq!(b.hi(), 4);
        b.push(4, b"efgh");
        assert_eq!(b.lo, 0);
        assert_eq!(b.hi(), 8);
        // Push past the cap — front trims.
        b.push(8, b"ij");
        assert_eq!(b.lo, 2);
        assert_eq!(b.hi(), 10);
        assert!(b.contains(2));
        assert!(b.contains(10));
        assert!(!b.contains(1));
        assert!(!b.contains(11));
    }

    #[test]
    fn active_cluster_buffer_in_window_patch() {
        let mut b = ActiveClusterBuffer::new(16);
        b.push(100, b"AAAAAAAA");
        assert!(b.contains(104));
        // Patch in the middle.
        b.push(102, b"BB");
        let collected: Vec<u8> = b.data.iter().copied().collect();
        assert_eq!(collected, b"AABBAAAA");
        assert_eq!(b.lo, 100);
        assert_eq!(b.hi(), 108);
    }

    #[test]
    fn active_cluster_buffer_non_contiguous_reseats() {
        let mut b = ActiveClusterBuffer::new(16);
        b.push(0, b"abcd");
        b.push(1000, b"XYZ");
        assert_eq!(b.lo, 1000);
        assert_eq!(b.hi(), 1003);
    }

    #[test]
    fn writer_thread_panic_surfaces_on_drop() {
        // Simulate a writer-side panic by writing to a read-only file
        // — the underlying `file.write_all` will return EBADF /
        // PermissionDenied. The writer publishes sticky_error and
        // exits via the next dequeue; subsequent push_command returns
        // the error. Drop joins cleanly (no panic from the writer
        // thread itself; it returned through the error path).
        //
        // We deliberately use a closed-FD strategy: open a file,
        // truncate the kernel's view by closing it, then write — this
        // is hard to force without unsafe. Easier: write to a path
        // and then forcibly close the underlying File via shutdown of
        // the writer thread. Since we don't expose the inner File,
        // pick the read-only-mode approach: open the file in
        // read-only mode and try to write.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ro.bin");
        std::fs::write(&p, b"seed").unwrap();
        let f = OpenOptions::new().read(true).open(&p).unwrap();
        let mut w = WritebackFile::new(f).unwrap();
        // First write may succeed depending on platform; loop until
        // an error surfaces. On Linux a write on an O_RDONLY fd
        // returns EBADF immediately.
        let mut saw_error = false;
        for _ in 0..32 {
            match w.write_all(b"x") {
                Ok(()) => {
                    // Give the writer thread a moment to surface the
                    // error then retry.
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(_) => {
                    saw_error = true;
                    break;
                }
            }
        }
        assert!(
            saw_error,
            "expected the writer to publish an error on a read-only fd"
        );
        drop(w);
    }
}

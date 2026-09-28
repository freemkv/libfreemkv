//! The in-write flusher (stop design §2.10 items 1–4): one worker per file makes written
//! bytes durable in chunks of `C` while the writer runs. `C` halves (to a floor) when one
//! chunk flush is slow; the writer waits, halt-aware, while more than `2 × C` is unflushed;
//! a flusher with no progress for the stall window latches `SyncTimeout` (sticky).

use std::fs::File;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use crate::error::Error;
use crate::halt::{Halt, Stall, StallTimer, WAIT_SLICE};
use crate::io::flush::{FlushOps, FlushProgress, FlushTiming, Sampler};

pub(super) struct Flusher {
    shared: Arc<Shared>,
    timing: FlushTiming,
    ops: Arc<dyn FlushOps>,
}

struct Shared {
    state: Mutex<State>,
    cv: Condvar,
}

struct State {
    // Bytes handed to the worker, and bytes it has made durable (both in written-byte units).
    requested: u64,
    flushed: u64,
    chunk: u64,
    // The first failure: a flush error, or a stall latched as `SyncTimeout`. Sticky.
    error: Option<Latched>,
    stop: bool,
    flush: FlushProgress,
}

#[derive(Clone, Copy)]
enum Latched {
    Os(io::ErrorKind, Option<i32>),
    SyncTimeout,
}

impl Latched {
    fn of(e: &io::Error) -> Self {
        Self::Os(e.kind(), e.raw_os_error())
    }
    fn error(self) -> io::Error {
        match self {
            Latched::Os(_, Some(errno)) => io::Error::from_raw_os_error(errno),
            Latched::Os(kind, None) => io::Error::from(kind),
            Latched::SyncTimeout => Error::SyncTimeout.into(),
        }
    }
}

impl Flusher {
    /// Start the worker on a clone of `file`; `base` bytes are already accounted for.
    pub(super) fn spawn(
        file: &File,
        ops: Arc<dyn FlushOps>,
        timing: FlushTiming,
        flush: FlushProgress,
        base: u64,
    ) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                requested: base,
                flushed: base,
                chunk: timing.chunk_start.max(timing.chunk_min),
                error: None,
                stop: false,
                flush,
            }),
            cv: Condvar::new(),
        });
        let worker = (file.try_clone()?, ops.clone(), shared.clone());
        std::thread::Builder::new()
            .name("freemkv-flusher".into())
            .spawn(move || run(worker.0, &*worker.1, &worker.2, timing))?;
        Ok(Self {
            shared,
            timing,
            ops,
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(super) fn set_flush_progress(&self, flush: FlushProgress) {
        self.lock().flush = flush;
    }

    #[cfg(test)]
    pub(super) fn chunk(&self) -> u64 {
        self.lock().chunk
    }

    pub(super) fn error(&self) -> Option<io::Error> {
        self.lock().error.map(Latched::error)
    }

    /// `written` bytes accepted so far: hand the worker a chunk once `C` has accumulated.
    pub(super) fn note_written(&self, written: u64) {
        let mut st = self.lock();
        if written.saturating_sub(st.requested) >= st.chunk {
            st.requested = written;
            self.shared.cv.notify_all();
        }
    }

    /// Backpressure before a write: wait while more than `2 × C` is unflushed.
    pub(super) fn wait_room(&self, written: u64, halt: Option<&Halt>) -> io::Result<()> {
        self.wait(halt, |st| {
            written.saturating_sub(st.flushed) <= 2 * st.chunk
        })
    }

    /// Hand over everything written and wait until it is durable.
    pub(super) fn drain(&self, written: u64, halt: Option<&Halt>) -> io::Result<()> {
        {
            let mut st = self.lock();
            if written > st.requested {
                st.requested = written;
                self.shared.cv.notify_all();
            }
        }
        self.wait(halt, |st| st.flushed >= written)
    }

    // Wait for `done`, halt-aware, under the stall timer on the shared progress; a
    // sampled counter (NFS) moving counts. Expiry latches `SyncTimeout` (sticky).
    fn wait(&self, halt: Option<&Halt>, done: impl Fn(&State) -> bool) -> io::Result<()> {
        let progress = self.lock().flush.progress().clone();
        let mut timer = StallTimer::new(self.timing.stall, &progress);
        let mut sampler = Sampler::new(self.timing.sample_every, self.timing.stall);
        let mut st = self.lock();
        loop {
            if let Some(e) = st.error {
                return Err(e.error());
            }
            if done(&st) {
                return Ok(());
            }
            st = self
                .shared
                .cv
                .wait_timeout(st, WAIT_SLICE)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            if let Some(h) = halt {
                h.check().map_err(io::Error::from)?;
            }
            drop(st);
            if sampler.tick(&*self.ops).is_some() {
                progress.bump();
            }
            let stalled = timer.poll(&progress) == Stall::Expired;
            st = self.lock();
            if stalled && !done(&st) {
                tracing::error!(
                    target: "freemkv::io",
                    stall_s = self.timing.stall.as_secs(),
                    "WritebackFile flusher made no progress for the stall window"
                );
                st.error.get_or_insert(Latched::SyncTimeout);
            }
        }
    }
}

impl Drop for Flusher {
    // The worker ends once idle; one blocked in a flush ends when the call returns.
    fn drop(&mut self) {
        self.lock().stop = true;
        self.shared.cv.notify_all();
    }
}

fn run(file: File, ops: &dyn FlushOps, shared: &Shared, timing: FlushTiming) {
    let lock = || shared.state.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        let target = {
            let mut st = lock();
            while !st.stop && st.error.is_none() && st.requested <= st.flushed {
                st = shared.cv.wait(st).unwrap_or_else(|e| e.into_inner());
            }
            if st.stop || st.error.is_some() {
                return;
            }
            st.requested
        };
        let started = Instant::now();
        let r = ops.chunk(&file);
        let took = started.elapsed();
        let mut st = lock();
        match r {
            Ok(()) => {
                let newly = target.saturating_sub(st.flushed);
                st.flushed = st.flushed.max(target);
                st.flush.add_durable(newly);
                if took > timing.slow_chunk && st.chunk > timing.chunk_min {
                    st.chunk = (st.chunk / 2).max(timing.chunk_min);
                    tracing::info!(
                        target: "freemkv::io",
                        chunk = st.chunk,
                        took_ms = took.as_millis() as u64,
                        "WritebackFile flusher: slow chunk flush, chunk halved"
                    );
                }
            }
            Err(e) => {
                tracing::error!(target: "freemkv::io", error = %e, "WritebackFile chunk flush failed");
                st.error.get_or_insert(Latched::of(&e));
            }
        }
        shared.cv.notify_all();
    }
}

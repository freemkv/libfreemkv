//! Deferred AACS unit decrypt, so the prefetcher can read batch N+1 while batch N decrypts.
//!
//! The producer [`arm`]s its pool buffer on its own thread before a read. A
//! [`DecryptingSectorSource`](super::DecryptingSectorSource) whose read landed inside that
//! buffer then hands its keyed unit decrypt over as a [`Job`] instead of running it; any other
//! read (a scratch copy, a side read, an unarmed thread) decrypts at once, as before. The
//! producer collects the jobs with [`Armed::finish`] and a decrypt stage [`run`]s them, in
//! order, before the batch reaches the consumer. Only the pure unit decrypt moves: every
//! judgement a read makes (damage, arrival proof, the unit grid) still happens in the read.

use crate::decrypt::{AacsKeyMap, DecryptKeys};
use crate::error::{Error, Result};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Where a reader's blanked units are counted: its own tally and, if observed, the run.
#[derive(Clone)]
pub(crate) struct Tally {
    pub(crate) blanked: Arc<AtomicU64>,
    pub(crate) ctx: Option<crate::ctx::Ctx>,
}

impl Tally {
    /// Count `n` blanked units of a read at `lba`.
    pub(crate) fn add(&self, lba: u32, n: usize) {
        count_blanked(&self.blanked, self.ctx.as_ref(), lba, n);
    }
}

/// Count `n` blanked units of a read at `lba` into `blanked` and, when set, the run's stats
/// and events (`UnitBlanked`).
pub(crate) fn count_blanked(
    blanked: &AtomicU64,
    ctx: Option<&crate::ctx::Ctx>,
    lba: u32,
    n: usize,
) {
    blanked.fetch_add(n as u64, Ordering::Relaxed);
    if let (Some(ctx), 1..) = (ctx, n) {
        ctx.stats.add_blanked(n as u64);
        ctx.emit(crate::event::Event::UnitBlanked {
            lba: u64::from(lba),
            units: n as u64,
        });
    }
}

// Jobs the prefetcher's decrypt stage ran (test-only; prefetcher tests hold one lock).
#[cfg(test)]
pub(crate) static STAGE_RUNS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// One read's keyed unit decrypt, as `DecryptingSectorSource` would have run it in place.
pub(crate) struct Job {
    pub(crate) lba: u32,
    pub(crate) keys: DecryptKeys,
    pub(crate) map: Arc<AacsKeyMap>,
    pub(crate) content: Option<Arc<[(u32, u32)]>>,
    pub(crate) tally: Tally,
}

impl Job {
    /// Decrypt `buf` (the bytes the read returned) and count what it blanked.
    pub(crate) fn run(&self, buf: &mut [u8]) -> Result<()> {
        #[cfg(test)]
        if std::thread::current().name() == Some("freemkv-decrypt-stage") {
            STAGE_RUNS.fetch_add(1, Ordering::Relaxed);
        }
        let n = crate::decrypt::decrypt_sectors_mapped_in_content(
            buf,
            &self.keys,
            self.lba,
            &self.map,
            self.content.as_deref(),
        )?;
        self.tally.add(self.lba, n);
        Ok(())
    }
}

/// A job and the byte range of the armed buffer it decrypts.
pub(crate) struct Pending {
    range: std::ops::Range<usize>,
    job: Job,
}

struct Slot {
    base: usize,
    cap: usize,
    // (address, length, job): addresses, so a job is placed without knowing the wrappers.
    jobs: Vec<(usize, usize, Job)>,
}

thread_local! {
    static ARMED: RefCell<Option<Slot>> = const { RefCell::new(None) };
}

/// Deferral armed on this thread over one buffer; dropping it disarms.
pub(crate) struct Armed {
    base: usize,
}

/// Arm deferral over `buf`, first reserving `max_bytes` so no read can move it.
pub(crate) fn arm(buf: &mut Vec<u8>, max_bytes: usize) -> Armed {
    buf.reserve(max_bytes.saturating_sub(buf.len()));
    let base = buf.as_ptr() as usize;
    let slot = Slot {
        base,
        cap: buf.capacity(),
        jobs: Vec::new(),
    };
    ARMED.with(|a| *a.borrow_mut() = Some(slot));
    Armed { base }
}

impl Armed {
    /// The jobs deferred into `buf` since [`arm`], as ranges of it. A buffer that moved, or a
    /// job past its end, fails loud: running it elsewhere would ship ciphertext.
    pub(crate) fn finish(self, buf: &[u8]) -> Result<Vec<Pending>> {
        let jobs = ARMED
            .with(|a| a.borrow_mut().take())
            .map(|s| s.jobs)
            .unwrap_or_default();
        if !jobs.is_empty() && buf.as_ptr() as usize != self.base {
            return Err(Error::DecryptFailed);
        }
        jobs.into_iter()
            .map(|(at, len, job)| {
                let start = at - self.base;
                let range = start..start + len;
                if range.end <= buf.len() {
                    Ok(Pending { range, job })
                } else {
                    Err(Error::DecryptFailed)
                }
            })
            .collect()
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        ARMED.with(|a| a.borrow_mut().take());
    }
}

/// Defer `make()`'s job for the read that filled `buf`, when this thread is armed over a
/// buffer holding all of it. `false`: not deferred, so the caller decrypts now.
pub(crate) fn try_defer(buf: &[u8], make: impl FnOnce() -> Job) -> bool {
    let at = buf.as_ptr() as usize;
    ARMED.with(|a| {
        let mut a = a.borrow_mut();
        let Some(slot) = a.as_mut() else {
            return false;
        };
        let inside = at >= slot.base && at - slot.base + buf.len() <= slot.cap;
        if !inside || buf.is_empty() {
            return false;
        }
        slot.jobs.push((at, buf.len(), make()));
        true
    })
}

/// Run now every job deferred into `buf`: for a reader that changes bytes after its inner read.
pub(crate) fn settle(buf: &mut [u8]) -> Result<()> {
    let (lo, hi) = (buf.as_ptr() as usize, buf.as_ptr() as usize + buf.len());
    let mine: Vec<(usize, usize, Job)> = ARMED.with(|a| {
        let mut a = a.borrow_mut();
        let Some(slot) = a.as_mut() else {
            return Vec::new();
        };
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut slot.jobs)
            .into_iter()
            .partition(|&(at, len, _)| at >= lo && at + len <= hi);
        slot.jobs = rest;
        mine
    });
    for (at, len, job) in mine {
        job.run(&mut buf[at - lo..at - lo + len])?;
    }
    Ok(())
}

/// Run `jobs` over `buf` in read order; the first failure stops the batch.
pub(crate) fn run(buf: &mut [u8], jobs: Vec<Pending>) -> Result<()> {
    for p in jobs {
        let part = buf.get_mut(p.range).ok_or(Error::DecryptFailed)?;
        p.job.run(part)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;

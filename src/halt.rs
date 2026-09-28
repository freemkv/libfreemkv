//! Cooperative cancellation and forward-progress primitives (stop design §2.1).
//!
//! [`Halt`] is a clonable one-bit token over `Arc<AtomicBool>`: `cancel()` on any
//! clone is observed by every other clone. Every wait on an op path goes through a
//! halt-aware primitive ([`Halt::wait`], [`Halt::recv_timeout`], [`Halt::send_timeout`],
//! [`join_within`]) that re-checks the token at least every [`WAIT_SLICE`].
//!
//! Timeouts are stall-only (HR1): [`StallTimer`] fires only when a [`Progress`]
//! counter has not moved for its window, never on total elapsed time. Every
//! duration is a parameter at its use site, so tests run in milliseconds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// The longest a halt-aware wait sleeps between token checks (§2.1).
pub const WAIT_SLICE: Duration = Duration::from_millis(20);

/// Alias of [`WAIT_SLICE`], kept until ST-X1b removes it.
pub const POLL_INTERVAL: Duration = WAIT_SLICE;

// The one place the token's orderings live, shared with the loom model (LT3).
trait AtomicFlag {
    fn store_flag(&self, v: bool, o: Ordering);
    fn load_flag(&self, o: Ordering) -> bool;
}

impl AtomicFlag for AtomicBool {
    fn store_flag(&self, v: bool, o: Ordering) {
        self.store(v, o)
    }
    fn load_flag(&self, o: Ordering) -> bool {
        self.load(o)
    }
}

#[cfg(all(loom, test))]
impl AtomicFlag for loom::sync::atomic::AtomicBool {
    fn store_flag(&self, v: bool, o: Ordering) {
        self.store(v, o)
    }
    fn load_flag(&self, o: Ordering) -> bool {
        self.load(o)
    }
}

// SS-23 Ordering::Release: "all previous writes become visible to all threads that perform
// an Acquire (or stronger) load of this value" — cancel publishes everything before it.
fn raise<F: AtomicFlag>(flag: &F) {
    flag.store_flag(true, Ordering::Release)
}

// SS-23 Ordering::Acquire: "all subsequent loads will see data written before the store".
fn observed<F: AtomicFlag>(flag: &F) -> bool {
    flag.load_flag(Ordering::Acquire)
}

/// Clonable, infallible cooperative-cancellation token.
///
/// Clones share one flag. `cancel()` is one-way; there is no `reset()` by design —
/// construct a fresh `Halt` for a fresh operation. `cancel` stores with `Release`
/// and every load uses `Acquire` (SS-23), so writes made before a cancel are
/// visible to whoever observes it.
#[derive(Clone, Debug)]
pub struct Halt(Arc<AtomicBool>);

impl Halt {
    /// Construct a fresh, uncancelled token.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Wrap an existing `Arc<AtomicBool>` as a `Halt`: both are views over one
    /// flag, so cancelling either side flips the same bit.
    pub fn from_arc(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    /// Borrow the underlying `Arc<AtomicBool>`, the inverse of
    /// [`from_arc`](Self::from_arc).
    pub fn as_arc(&self) -> &Arc<AtomicBool> {
        &self.0
    }

    /// Flip the shared flag to cancelled. Idempotent.
    pub fn cancel(&self) {
        raise(&*self.0)
    }

    /// Read the shared flag.
    pub fn is_cancelled(&self) -> bool {
        observed(&*self.0)
    }

    /// `Err(Error::Halted)` once cancelled, else `Ok(())`.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Halted)
        } else {
            Ok(())
        }
    }

    /// Sleep for `d` in [`WAIT_SLICE`] slices: `Ok(())` once `d` has passed,
    /// `Err(Error::Halted)` within one slice of a cancel. `Duration::MAX` waits
    /// until cancelled.
    pub fn wait(&self, d: Duration) -> Result<()> {
        diag::assert_may_block("Halt::wait (sleep)", d);
        let end = Instant::now().checked_add(d);
        loop {
            self.check()?;
            let Some(left) = remaining(end) else {
                return Ok(());
            };
            thread::sleep(left);
        }
    }

    /// Receive from `rx` for up to `d`, checking the token every [`WAIT_SLICE`].
    ///
    /// `Recv::Item` on receipt; `Recv::TimedOut` once `d` passes; `Recv::Disconnected`
    /// at once when the sending side is gone, so a caller looping on it cannot spin;
    /// `Err(Error::Halted)` on cancel, checked before each slice. At least one
    /// attempt is always made, so a zero `d` still takes a queued item.
    pub fn recv_timeout<C: TimedRecv>(&self, rx: &C, d: Duration) -> Result<Recv<C::Item>> {
        let end = Instant::now().checked_add(d);
        loop {
            self.check()?;
            let slice = remaining(end).unwrap_or(Duration::ZERO);
            match rx.recv_slice(slice) {
                Recv::TimedOut if remaining(end).is_some() => {}
                r => return Ok(r),
            }
        }
    }

    /// Send `v` on `tx` within `d`, checking the token every [`WAIT_SLICE`].
    ///
    /// `SendOutcome::Sent`; `TimedOut(v)` once `d` passes; `Disconnected(v)` at once
    /// when the receiver is gone, so a caller looping on it cannot spin;
    /// `Err(Halted(v))` on cancel, checked before each slice (`?` turns it into
    /// `Error::Halted`). The item always comes back. At least one attempt is always
    /// made, so a zero `d` still sends into room.
    pub fn send_timeout<C: TimedSend>(
        &self,
        tx: &C,
        v: C::Item,
        d: Duration,
    ) -> std::result::Result<SendOutcome<C::Item>, Halted<C::Item>> {
        let end = Instant::now().checked_add(d);
        let mut pending = v;
        loop {
            if self.is_cancelled() {
                return Err(Halted(pending));
            }
            let slice = remaining(end).unwrap_or(Duration::ZERO);
            match tx.send_slice(pending, slice) {
                Ok(()) => return Ok(SendOutcome::Sent),
                Err(SendFail::Timeout(back)) if remaining(end).is_some() => pending = back,
                Err(SendFail::Timeout(back)) => return Ok(SendOutcome::TimedOut(back)),
                Err(SendFail::Disconnected(back)) => return Ok(SendOutcome::Disconnected(back)),
            }
        }
    }
}

impl Default for Halt {
    fn default() -> Self {
        Self::new()
    }
}

// The next slice before `end` (`None` = unbounded), or `None` once `end` has passed.
fn remaining(end: Option<Instant>) -> Option<Duration> {
    match end {
        None => Some(WAIT_SLICE),
        Some(end) => {
            let left = end.saturating_duration_since(Instant::now());
            (!left.is_zero()).then(|| left.min(WAIT_SLICE))
        }
    }
}

/// How a receive ended ([`Halt::recv_timeout`], [`TimedRecv::recv_slice`]).
#[derive(Debug, PartialEq, Eq)]
pub enum Recv<T> {
    Item(T),
    /// Nothing arrived within the budget.
    TimedOut,
    /// Every sender is gone: nothing can ever arrive.
    Disconnected,
}

/// How [`Halt::send_timeout`] ended short of a stop; unsent items come back.
/// (Not named `Send`: that would shadow the marker trait for glob importers.)
#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome<T> {
    Sent,
    /// No room within the budget.
    TimedOut(T),
    /// The receiver is gone: the item can never be delivered.
    Disconnected(T),
}

/// A send stopped by a cancel, carrying the unsent item back.
#[derive(Debug, PartialEq, Eq)]
pub struct Halted<T>(pub T);

impl<T> From<Halted<T>> for Error {
    fn from(_: Halted<T>) -> Self {
        Error::Halted
    }
}

/// A failed bounded send attempt ([`TimedSend::send_slice`]); the item comes back.
#[derive(Debug)]
pub enum SendFail<T> {
    Timeout(T),
    Disconnected(T),
}

/// A channel receiver [`Halt::recv_timeout`] can wait on in slices.
pub trait TimedRecv {
    type Item;
    /// Block for at most `d` waiting for one item; a zero `d` is one non-blocking try.
    fn recv_slice(&self, d: Duration) -> Recv<Self::Item>;
}

/// A channel sender [`Halt::send_timeout`] can wait on in slices.
pub trait TimedSend {
    type Item;
    /// Block for at most `d` waiting for room for `v`; a zero `d` is one non-blocking try.
    fn send_slice(
        &self,
        v: Self::Item,
        d: Duration,
    ) -> std::result::Result<(), SendFail<Self::Item>>;
}

impl<T> TimedRecv for std::sync::mpsc::Receiver<T> {
    type Item = T;
    fn recv_slice(&self, d: Duration) -> Recv<T> {
        use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
        if d.is_zero() {
            return match self.try_recv() {
                Ok(v) => Recv::Item(v),
                Err(TryRecvError::Empty) => Recv::TimedOut,
                Err(TryRecvError::Disconnected) => Recv::Disconnected,
            };
        }
        match self.recv_timeout(d) {
            Ok(v) => Recv::Item(v),
            Err(RecvTimeoutError::Timeout) => Recv::TimedOut,
            Err(RecvTimeoutError::Disconnected) => Recv::Disconnected,
        }
    }
}

// std's `SyncSender` has no timed send: poll `try_send` via `backoff_poll`.
impl<T> TimedSend for std::sync::mpsc::SyncSender<T> {
    type Item = T;
    fn send_slice(&self, v: T, d: Duration) -> std::result::Result<(), SendFail<T>> {
        use std::sync::mpsc::TrySendError;
        backoff_poll(v, d, |v| match self.try_send(v) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(back)) => Err(SendFail::Timeout(back)),
            Err(TrySendError::Disconnected(back)) => Err(SendFail::Disconnected(back)),
        })
    }
}

// Retry `attempt` until it stops reporting `Timeout` or `d` passes, sleeping 1 ms and
// doubling up to WAIT_SLICE: a long-full channel costs ~50 wakeups/s, and freed room is
// seen within one backoff step. The first attempt is immediate.
fn backoff_poll<T>(
    v: T,
    d: Duration,
    mut attempt: impl FnMut(T) -> std::result::Result<(), SendFail<T>>,
) -> std::result::Result<(), SendFail<T>> {
    let end = Instant::now() + d;
    let mut pending = v;
    let mut backoff = Duration::from_millis(1);
    loop {
        match attempt(pending) {
            Err(SendFail::Timeout(back)) => pending = back,
            done => return done,
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(SendFail::Timeout(pending));
        }
        thread::sleep(left.min(backoff));
        backoff = (backoff * 2).min(WAIT_SLICE);
    }
}

#[cfg(feature = "rip")]
impl<T> TimedRecv for crossbeam_channel::Receiver<T> {
    type Item = T;
    fn recv_slice(&self, d: Duration) -> Recv<T> {
        use crossbeam_channel::{RecvTimeoutError, TryRecvError};
        if d.is_zero() {
            return match self.try_recv() {
                Ok(v) => Recv::Item(v),
                Err(TryRecvError::Empty) => Recv::TimedOut,
                Err(TryRecvError::Disconnected) => Recv::Disconnected,
            };
        }
        match self.recv_timeout(d) {
            Ok(v) => Recv::Item(v),
            Err(RecvTimeoutError::Timeout) => Recv::TimedOut,
            Err(RecvTimeoutError::Disconnected) => Recv::Disconnected,
        }
    }
}

#[cfg(feature = "rip")]
impl<T> TimedSend for crossbeam_channel::Sender<T> {
    type Item = T;
    fn send_slice(&self, v: T, d: Duration) -> std::result::Result<(), SendFail<T>> {
        use crossbeam_channel::{SendTimeoutError, TrySendError};
        if d.is_zero() {
            return match self.try_send(v) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(back)) => Err(SendFail::Timeout(back)),
                Err(TrySendError::Disconnected(back)) => Err(SendFail::Disconnected(back)),
            };
        }
        match self.send_timeout(v, d) {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(back)) => Err(SendFail::Timeout(back)),
            Err(SendTimeoutError::Disconnected(back)) => Err(SendFail::Disconnected(back)),
        }
    }
}

/// Forward-progress counter (§2.1). Bumped on bytes read or written, a sector
/// advanced, an item applied, or a response received. Cheap to clone; clones share
/// the count. [`busy`](Self::busy) only marks in-flight work: it never bumps.
#[derive(Clone, Debug, Default)]
pub struct Progress(Arc<ProgressInner>);

#[derive(Debug, Default)]
struct ProgressInner {
    count: AtomicU64,
    // Live `BusyGuard`s, and every guard start or end (so a poll sees a span that
    // began and ended between two polls).
    busy: AtomicUsize,
    busy_epoch: AtomicU64,
}

impl Progress {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one unit of forward progress.
    pub fn bump(&self) {
        self.0.count.fetch_add(1, Ordering::Relaxed);
    }

    /// The progress count so far.
    pub fn get(&self) -> u64 {
        self.0.count.load(Ordering::Relaxed)
    }

    /// Mark legitimate work with no progress signal (a CDB in flight, a key-service
    /// call, a keydb parse) until the guard drops. Only [`StallTimer::idle_only`]
    /// reads it; a plain [`StallTimer`] ignores it (LT5c).
    pub fn busy(&self) -> BusyGuard {
        self.0.busy.fetch_add(1, Ordering::Relaxed);
        self.0.busy_epoch.fetch_add(1, Ordering::Relaxed);
        BusyGuard(self.clone())
    }

    /// Whether a [`BusyGuard`] is live (crate tests of busy spans).
    #[cfg(all(test, feature = "rip"))]
    pub(crate) fn is_busy(&self) -> bool {
        self.busy_state().0
    }

    fn busy_state(&self) -> (bool, u64) {
        let epoch = self.0.busy_epoch.load(Ordering::Relaxed);
        (self.0.busy.load(Ordering::Relaxed) > 0, epoch)
    }
}

/// Holds a [`Progress`] busy until dropped.
#[must_use = "the span is busy only while the guard lives"]
#[derive(Debug)]
pub struct BusyGuard(Progress);

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.0.0.busy.fetch_sub(1, Ordering::Relaxed);
        self.0.0.busy_epoch.fetch_add(1, Ordering::Relaxed);
    }
}

/// What a [`StallTimer::poll`] saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stall {
    /// The counter moved since the last poll; the window restarts.
    Progressing,
    /// No progress for this long, still inside the window.
    StalledFor(Duration),
    /// No progress for the whole window: the guarded op has stalled (HR1).
    Expired,
}

/// The HR1 primitive: fires only when a [`Progress`] has not moved for `window`.
///
/// A plain timer ([`new`](Self::new)) counts all time without progress and ignores
/// [`Progress::busy`]; every HR1 failure timer is plain. [`idle_only`](Self::idle_only)
/// is the opt-in T29 mode: time is not counted while the progress is busy.
#[derive(Debug)]
pub struct StallTimer {
    window: Duration,
    last: u64,
    since: Instant,
    idle: Option<IdleClock>,
}

// idle_only: idle time accrued so far, the instant it was last accrued to, and the
// busy epoch then. An interval with any busy span in it accrues nothing.
#[derive(Debug)]
struct IdleClock {
    idle: Duration,
    mark: Instant,
    epoch: u64,
}

impl StallTimer {
    /// A plain stall timer: every HR1 failure timer is one of these.
    pub fn new(window: Duration, p: &Progress) -> Self {
        Self {
            window,
            last: p.get(),
            since: Instant::now(),
            idle: None,
        }
    }

    /// Opt-in (v5.5, ST3-5): paused while `p` is [`busy`](Progress::busy). The T29
    /// launch probe is its only user.
    pub fn idle_only(window: Duration, p: &Progress) -> Self {
        let mut t = Self::new(window, p);
        t.idle = Some(IdleClock {
            idle: Duration::ZERO,
            mark: t.since,
            epoch: p.busy_state().1,
        });
        t
    }

    /// Sample `p`. Any movement re-arms the whole window.
    pub fn poll(&mut self, p: &Progress) -> Stall {
        let now = Instant::now();
        let count = p.get();
        if count != self.last {
            self.last = count;
            self.since = now;
            if let Some(c) = &mut self.idle {
                c.idle = Duration::ZERO;
                c.mark = now;
                c.epoch = p.busy_state().1;
            }
            return Stall::Progressing;
        }
        let stalled = match &mut self.idle {
            None => now.saturating_duration_since(self.since),
            Some(c) => {
                let (busy, epoch) = p.busy_state();
                if !busy && epoch == c.epoch {
                    c.idle += now.saturating_duration_since(c.mark);
                }
                c.mark = now;
                c.epoch = epoch;
                c.idle
            }
        };
        if stalled >= self.window {
            Stall::Expired
        } else {
            Stall::StalledFor(stalled)
        }
    }
}

/// How [`join_within`] ended. `Halted` and `Pending` hand the handle back.
#[derive(Debug)]
pub enum Joined<R> {
    /// The thread finished; its result, or its panic payload.
    Done(thread::Result<R>),
    /// The token was cancelled while the thread still ran.
    Halted(JoinHandle<R>),
    /// `d` passed with the thread still running.
    Pending(JoinHandle<R>),
}

/// Wait up to `d` for `h` to finish, checking `halt` every [`WAIT_SLICE`]; never
/// blocks past `d` plus one slice. Replaces `is_finished` spins.
pub fn join_within<R>(h: JoinHandle<R>, d: Duration, halt: Option<&Halt>) -> Joined<R> {
    if h.is_finished() {
        return Joined::Done(join_finished(h));
    }
    diag::assert_may_block("join_within (join)", d);
    let end = Instant::now().checked_add(d);
    loop {
        if h.is_finished() {
            return Joined::Done(join_finished(h));
        }
        if halt.is_some_and(Halt::is_cancelled) {
            return Joined::Halted(h);
        }
        let Some(slice) = remaining(end) else {
            return Joined::Pending(h);
        };
        thread::sleep(slice);
    }
}

/// Join a thread that has already finished: never blocks, so it is allowed under
/// [`diag::NoBlockingScope`].
pub fn join_finished<R>(h: JoinHandle<R>) -> thread::Result<R> {
    debug_assert!(h.is_finished(), "join_finished on a running thread");
    h.join()
}

static LIVE_DRIVE_HOLDERS: AtomicUsize = AtomicUsize::new(0);

/// Threads started by [`spawn_drive_holder`] whose body is still running.
pub fn live_drive_holders() -> usize {
    LIVE_DRIVE_HOLDERS.load(Ordering::SeqCst)
}

// Every test that spawns a Drive holder or reads the process-wide count holds this.
#[cfg(all(test, not(loom)))]
pub(crate) static DRIVE_HOLDER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A thread that holds the Drive (§2.5): the op joins it before returning. In debug
/// builds, dropping it unjoined panics.
#[derive(Debug)]
pub struct DriveHolder<R> {
    role: &'static str,
    handle: Option<JoinHandle<R>>,
}

/// Spawn `f` on a thread named `freemkv-<role>` as a [`DriveHolder`].
pub fn spawn_drive_holder<F, R>(role: &'static str, f: F) -> std::io::Result<DriveHolder<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    struct Live;
    impl Drop for Live {
        fn drop(&mut self) {
            LIVE_DRIVE_HOLDERS.fetch_sub(1, Ordering::SeqCst);
        }
    }
    LIVE_DRIVE_HOLDERS.fetch_add(1, Ordering::SeqCst);
    let live = Live;
    let handle = thread::Builder::new()
        .name(format!("freemkv-{role}"))
        .spawn(move || {
            let _live = live;
            f()
        })?;
    Ok(DriveHolder {
        role,
        handle: Some(handle),
    })
}

impl<R> DriveHolder<R> {
    /// The role it was spawned with.
    pub fn role(&self) -> &'static str {
        self.role
    }

    /// Whether the thread has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Join the thread: its result, or its panic payload.
    pub fn join(mut self) -> thread::Result<R> {
        let h = self.handle.take().expect("DriveHolder joined twice");
        if h.is_finished() {
            return join_finished(h);
        }
        diag::assert_may_block("DriveHolder::join (join)", Duration::MAX);
        h.join()
    }
}

impl<R> Drop for DriveHolder<R> {
    fn drop(&mut self) {
        if self.handle.is_some() && !thread::panicking() {
            if cfg!(debug_assertions) {
                panic!("drive holder `{}` dropped unjoined", self.role);
            }
            #[cfg(feature = "scsi")]
            tracing::warn!(target: "freemkv::halt", role = self.role, "drive holder dropped unjoined");
        }
    }
}

/// Debug checks for code that must never block (a GUI frame, a Stop request).
pub mod diag {
    use std::cell::Cell;
    use std::marker::PhantomData;
    use std::time::Duration;

    thread_local! {
        static DEPTH: Cell<u32> = const { Cell::new(0) };
    }

    /// While one lives on this thread, a blocking `exec`, sleep ([`Halt::wait`]) or
    /// join ([`join_within`], [`DriveHolder::join`]) panics in debug builds.
    /// [`join_finished`] is exempt. Scopes nest; the guard is not `Send`.
    ///
    /// [`Halt::wait`]: super::Halt::wait
    /// [`join_within`]: super::join_within
    /// [`DriveHolder::join`]: super::DriveHolder::join
    /// [`join_finished`]: super::join_finished
    #[must_use = "the scope ends when the guard drops"]
    #[derive(Debug)]
    pub struct NoBlockingScope {
        _not_send: PhantomData<*const ()>,
    }

    impl NoBlockingScope {
        /// Enter a scope on this thread.
        pub fn enter() -> Self {
            DEPTH.with(|d| d.set(d.get() + 1));
            Self {
                _not_send: PhantomData,
            }
        }

        /// Whether this thread is inside a scope.
        pub fn active() -> bool {
            DEPTH.with(Cell::get) > 0
        }
    }

    impl Drop for NoBlockingScope {
        fn drop(&mut self) {
            DEPTH.with(|d| d.set(d.get() - 1));
        }
    }

    // Debug panic when `what` could block (`d > 0`) inside a scope.
    pub(crate) fn assert_may_block(what: &str, d: Duration) {
        if cfg!(debug_assertions) && !d.is_zero() && NoBlockingScope::active() {
            panic!("{what} under NoBlockingScope");
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests;

/// LT3: the token's orderings, model-checked. Run with
/// `RUSTFLAGS="--cfg loom" cargo test --lib --no-default-features --features scsi loom_halt`.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::{observed, raise};
    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicBool;

    /// per spec; do not change without a spec citation proving otherwise — SS-23:
    /// Release "all previous writes become visible to all threads that perform an
    /// Acquire (or stronger) load of this value". A write before `cancel` is visible
    /// to every thread that observes the cancel.
    #[test]
    fn loom_halt_cancel_is_release_acquire() {
        loom::model(|| {
            let flag = Arc::new(AtomicBool::new(false));
            let data = Arc::new(UnsafeCell::new(0u32));
            let (f2, d2) = (flag.clone(), data.clone());
            let canceller = loom::thread::spawn(move || {
                // SAFETY: the only write; the reader touches it only after observing
                // the cancel, which loom checks is ordered after this write.
                d2.with_mut(|p| unsafe { *p = 7 });
                raise(&*f2);
            });
            if observed(&*flag) {
                // SAFETY: ordered after the write by the Release/Acquire pair.
                assert_eq!(data.with(|p| unsafe { *p }), 7);
            }
            canceller.join().unwrap();
        });
    }
}

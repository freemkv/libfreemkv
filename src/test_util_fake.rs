//! [`FakeTransport`]: a scripted [`ScsiTransport`] with a drive state model, for Stop
//! tests (stop design §5.0). Durations are scaled: a CDB's `timeout_ms` is divided by
//! [`FakeTransport::scale`] (default 1000, so a 60 s READ times out at 60 ms).
//!
//! It keeps the tray and AGID ledger a real drive would, logs every CDB, counts live
//! handles, and **panics on any CDB that reaches it after the watched token is
//! cancelled** unless the §2.4 allow-list (shared with `Drive::exec_cleanup`) or a
//! test's [`FakeTransport::allow_after_cancel`] admits it.

use crate::drive::allow::{self, CleanupCtx, Ledger};
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::scsi::{DataDirection, ScsiResult, ScsiSense, ScsiTransport};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

type Pred = Box<dyn Fn(&[u8]) -> bool + Send>;

/// How the fake answers a CDB a rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeMode {
    /// Answer after `d` (wall time, not scaled) as the default serving would. Past the
    /// CDB's scaled timeout it times out instead, like a real transport.
    Complete(Duration),
    /// Block until [`FakeHandle::release`], or time out at the CDB's scaled timeout.
    Stall,
    /// A transport fault: status 0xFF, no sense (a dead bus).
    Fault,
    /// CHECK CONDITION with this sense; `progress` is the sense-key specific progress
    /// indication reported through [`ScsiTransport::last_sense_progress`].
    Sense {
        sense: ScsiSense,
        progress: Option<u16>,
    },
}

/// One CDB as the fake received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeCdb {
    pub cdb: Vec<u8>,
    pub timeout_ms: u32,
    /// Whether the watched token was already cancelled when it arrived.
    pub after_cancel: bool,
}

struct Rule {
    pred: Pred,
    mode: FakeMode,
    times: Option<usize>,
}

struct State {
    log: Vec<FakeCdb>,
    rules: Vec<Rule>,
    image: Option<Vec<u8>>,
    profile: u16,
    scale: u32,
    watch: Option<Halt>,
    allow: Vec<Pred>,
    cancel_on: Vec<(Pred, Halt)>,
    ledger: Ledger,
    live: usize,
    released: u64,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// A scripted transport; build it, keep its [`FakeHandle`], and hand it to
/// `Drive::from_transport` / `Drive::from_transport_with`.
pub struct FakeTransport {
    shared: Arc<Shared>,
    inner: Option<Box<dyn ScsiTransport>>,
    last_progress: Option<u16>,
}

/// The test's view of a [`FakeTransport`]: its CDB log, ledger and live count, and
/// the release for stalled CDBs. Cheap to clone.
#[derive(Clone)]
pub struct FakeHandle(Arc<Shared>);

impl FakeTransport {
    /// A fake that answers every CDB GOOD with a zeroed buffer.
    pub fn new() -> (Self, FakeHandle) {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                log: Vec::new(),
                rules: Vec::new(),
                image: None,
                profile: 0x0040,
                scale: 1000,
                watch: None,
                allow: Vec::new(),
                cancel_on: Vec::new(),
                ledger: Ledger::default(),
                live: 1,
                released: 0,
            }),
            wake: Condvar::new(),
        });
        let t = FakeTransport {
            shared: shared.clone(),
            inner: None,
            last_progress: None,
        };
        (t, FakeHandle(shared))
    }

    /// Serve READ(10) and READ CAPACITY from `image` (2048-byte sectors).
    pub fn with_image(self, image: Vec<u8>) -> Self {
        self.shared.lock().image = Some(image);
        self
    }

    /// Delegate every CDB no rule matches to `inner` (e.g. the AKE handshake fixture).
    pub fn with_inner(mut self, inner: Box<dyn ScsiTransport>) -> Self {
        self.inner = Some(inner);
        self
    }

    /// The GET CONFIGURATION current profile (default 0x0040, BD-ROM).
    pub fn profile(self, profile: u16) -> Self {
        self.shared.lock().profile = profile;
        self
    }

    /// Divide every CDB timeout by `divisor` (default 1000).
    pub fn scale(self, divisor: u32) -> Self {
        self.shared.lock().scale = divisor.max(1);
        self
    }

    /// Answer CDBs matching `pred` with `mode`; the first matching rule wins.
    pub fn rule(self, pred: impl Fn(&[u8]) -> bool + Send + 'static, mode: FakeMode) -> Self {
        self.push_rule(Box::new(pred), mode, None)
    }

    /// As [`rule`](Self::rule), for the next `times` matching CDBs only.
    pub fn rule_n(
        self,
        pred: impl Fn(&[u8]) -> bool + Send + 'static,
        mode: FakeMode,
        times: usize,
    ) -> Self {
        self.push_rule(Box::new(pred), mode, Some(times))
    }

    fn push_rule(self, pred: Pred, mode: FakeMode, times: Option<usize>) -> Self {
        self.shared.lock().rules.push(Rule { pred, mode, times });
        self
    }

    /// Panic on any CDB arriving after `halt` is cancelled that §2.4 does not admit.
    pub fn watch(self, halt: &Halt) -> Self {
        self.shared.lock().watch = Some(halt.clone());
        self
    }

    /// Also admit post-cancel CDBs matching `pred` (a critical section, `finish(Eject)`:
    /// contexts the fake cannot see from below the Drive).
    pub fn allow_after_cancel(self, pred: impl Fn(&[u8]) -> bool + Send + 'static) -> Self {
        self.shared.lock().allow.push(Box::new(pred));
        self
    }

    /// Cancel `halt` as soon as a CDB matching `pred` has been answered.
    pub fn cancel_on(self, pred: impl Fn(&[u8]) -> bool + Send + 'static, halt: &Halt) -> Self {
        self.shared
            .lock()
            .cancel_on
            .push((Box::new(pred), halt.clone()));
        self
    }
}

impl FakeHandle {
    /// Every CDB received so far, in order.
    pub fn log(&self) -> Vec<FakeCdb> {
        self.0.lock().log.clone()
    }

    /// The CDB bytes received so far.
    pub fn cdbs(&self) -> Vec<Vec<u8>> {
        self.0.lock().log.iter().map(|c| c.cdb.clone()).collect()
    }

    /// How many received CDBs match `pred`.
    pub fn count(&self, pred: impl Fn(&[u8]) -> bool) -> usize {
        self.0.lock().log.iter().filter(|c| pred(&c.cdb)).count()
    }

    /// Let every CDB stalled so far complete.
    pub fn release(&self) {
        self.0.lock().released += 1;
        self.0.wake.notify_all();
    }

    /// Live `FakeTransport`s built from this handle (0 once the Drive dropped it).
    pub fn live_handles(&self) -> usize {
        self.0.lock().live
    }

    /// Threads holding a Drive (`spawn_drive_holder`) still running, process-wide.
    pub fn drive_holders(&self) -> usize {
        crate::halt::live_drive_holders()
    }

    /// The tray lock as the fake drive sees it.
    pub fn tray_locked(&self) -> bool {
        self.0.lock().ledger.tray_locked
    }

    /// AGIDs the fake drive holds (bit `n` = AGID `n`).
    pub fn agids(&self) -> u8 {
        self.0.lock().ledger.agids
    }

    /// Wait (up to `limit`) until at least `n` CDBs matching `pred` arrived.
    pub fn wait_for(&self, n: usize, pred: impl Fn(&[u8]) -> bool, limit: Duration) -> bool {
        let end = Instant::now() + limit;
        while Instant::now() < end {
            if self.count(&pred) >= n {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }
}

impl Drop for FakeTransport {
    fn drop(&mut self) {
        self.shared.lock().live -= 1;
    }
}

fn check_condition(opcode: u8, sense: ScsiSense) -> Error {
    Error::ScsiError {
        opcode,
        status: 2,
        sense: Some(sense),
    }
}

fn transport_failure(opcode: u8) -> Error {
    Error::ScsiError {
        opcode,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }
}

impl FakeTransport {
    // The default drive: an image for READ(10) / READ CAPACITY, a profile, GOOD
    // with zeros for everything else.
    fn serve_default(&self, cdb: &[u8], data: &mut [u8]) -> Result<ScsiResult> {
        data.fill(0);
        let st = self.shared.lock();
        match (cdb[0], &st.image) {
            (crate::scsi::SCSI_READ_10, Some(img)) if cdb.len() >= 9 => {
                let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]) as usize;
                let n = u16::from_be_bytes([cdb[7], cdb[8]]) as usize;
                let (start, len) = (lba * 2048, n * 2048);
                if start + len > img.len() || data.len() < len {
                    let lba_out_of_range = ScsiSense {
                        sense_key: crate::scsi::SENSE_KEY_ILLEGAL_REQUEST,
                        asc: 0x21,
                        ascq: 0x00,
                    };
                    return Err(check_condition(cdb[0], lba_out_of_range));
                }
                data[..len].copy_from_slice(&img[start..start + len]);
            }
            (crate::scsi::SCSI_READ_CAPACITY, Some(img)) if data.len() >= 8 => {
                let last = (img.len() / 2048).saturating_sub(1) as u32;
                data[..4].copy_from_slice(&last.to_be_bytes());
                data[4..8].copy_from_slice(&2048u32.to_be_bytes());
            }
            (crate::scsi::SCSI_GET_CONFIGURATION, _) if data.len() >= 8 => {
                data[6..8].copy_from_slice(&st.profile.to_be_bytes());
            }
            _ => {}
        }
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: data.len(),
            sense: [0u8; 32],
        })
    }

    fn serve(
        &mut self,
        cdb: &[u8],
        dir: DataDirection,
        data: &mut [u8],
        t: u32,
    ) -> Result<ScsiResult> {
        match self.inner.as_mut() {
            Some(inner) => inner.execute(cdb, dir, data, t),
            None => self.serve_default(cdb, data),
        }
    }

    // Block until released (a new `release` generation) or `limit` passes.
    fn stall(&self, limit: Duration) -> bool {
        let end = Instant::now() + limit;
        let mut st = self.shared.lock();
        let gen0 = st.released;
        while st.released == gen0 {
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            st = self
                .shared
                .wake
                .wait_timeout(st, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        true
    }
}

impl ScsiTransport for FakeTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        dir: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.last_progress = None;
        let (mode, limit) = {
            let mut st = self.shared.lock();
            let after_cancel = st.watch.as_ref().is_some_and(Halt::is_cancelled);
            st.log.push(FakeCdb {
                cdb: cdb.to_vec(),
                timeout_ms,
                after_cancel,
            });
            if after_cancel
                && !allow::allowed_after_cancel(cdb, st.ledger, CleanupCtx::Plain)
                && !st.allow.iter().any(|p| p(cdb))
            {
                drop(st);
                // Already unwinding (a Drive dropped by a failing test): refuse, don't abort.
                if std::thread::panicking() {
                    return Err(transport_failure(cdb[0]));
                }
                panic!("FakeTransport: CDB {cdb:02x?} after a cancel, outside the §2.4 allow-list");
            }
            let limit = Duration::from_micros(u64::from(timeout_ms) * 1000 / u64::from(st.scale));
            let mut mode = None;
            for r in st.rules.iter_mut() {
                if r.times != Some(0) && (r.pred)(cdb) {
                    if let Some(n) = r.times.as_mut() {
                        *n -= 1;
                    }
                    mode = Some(r.mode);
                    break;
                }
            }
            (mode, limit)
        };
        let r = match mode {
            None => self.serve(cdb, dir, data, timeout_ms),
            Some(FakeMode::Complete(d)) if d > limit => {
                std::thread::sleep(limit);
                Err(transport_failure(cdb[0]))
            }
            Some(FakeMode::Complete(d)) => {
                std::thread::sleep(d);
                self.serve(cdb, dir, data, timeout_ms)
            }
            Some(FakeMode::Stall) => match self.stall(limit) {
                true => self.serve(cdb, dir, data, timeout_ms),
                false => Err(transport_failure(cdb[0])),
            },
            Some(FakeMode::Fault) => Err(transport_failure(cdb[0])),
            Some(FakeMode::Sense { sense, progress }) => {
                self.last_progress = progress;
                Err(check_condition(cdb[0], sense))
            }
        };
        let mut st = self.shared.lock();
        note_ledger(&mut st.ledger, cdb, data, r.is_ok());
        for (pred, halt) in &st.cancel_on {
            if pred(cdb) {
                halt.cancel();
            }
        }
        r
    }

    fn last_sense_progress(&self) -> Option<u16> {
        self.last_progress
    }
}

// The fake drive's own tray and AGID state, from the CDBs it answered.
fn note_ledger(l: &mut Ledger, cdb: &[u8], resp: &[u8], ok: bool) {
    match cdb.first() {
        Some(&allow::PREVENT_ALLOW) if ok => l.tray_locked = cdb.get(4).is_some_and(|b| b & 1 == 1),
        Some(&allow::REPORT_KEY) if cdb.len() >= 11 => {
            if let Some(agid) = allow::invalidated_agid(cdb) {
                l.agids &= !(1 << agid);
            } else if cdb[10] & 0x3F == 0 && ok && resp.len() > 7 {
                l.agids |= 1 << (resp[7] >> 6);
            }
        }
        _ => {}
    }
}

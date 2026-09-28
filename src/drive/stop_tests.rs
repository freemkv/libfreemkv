//! Stop design §5.1 "Drive" tests (LD1–LD16) and the `wait_ready` guards (G4, G5),
//! run against `test_util::FakeTransport` with scaled durations.

use super::*;
use crate::scsi::{DataDirection, ScsiResult, ScsiSense};
use crate::test_util::{FakeMode, FakeTransport};
use std::sync::Mutex;
use std::time::Instant;

type Answer = Box<dyn FnMut(&[u8], &mut [u8]) -> Result<ScsiResult> + Send>;

// A transport answering every CDB from a closure (the drive "behind" the fake).
struct Script(Answer, Option<u16>, Arc<Mutex<Option<u16>>>);

impl Script {
    fn new(f: impl FnMut(&[u8], &mut [u8]) -> Result<ScsiResult> + Send + 'static) -> Self {
        Script(Box::new(f), None, Arc::default())
    }
    // A script that also reports a progress indication set through the returned cell.
    fn with_progress(
        f: impl FnMut(&[u8], &mut [u8]) -> Result<ScsiResult> + Send + 'static,
    ) -> (Self, Arc<Mutex<Option<u16>>>) {
        let cell: Arc<Mutex<Option<u16>>> = Arc::default();
        (Script(Box::new(f), None, cell.clone()), cell)
    }
}

impl ScsiTransport for Script {
    fn execute(
        &mut self,
        cdb: &[u8],
        _: DataDirection,
        d: &mut [u8],
        _: u32,
    ) -> Result<ScsiResult> {
        let r = (self.0)(cdb, d);
        self.1 = *self.2.lock().unwrap();
        r
    }
    fn last_sense_progress(&self) -> Option<u16> {
        self.1
    }
}

fn good(d: &[u8]) -> Result<ScsiResult> {
    Ok(ScsiResult {
        status: 0,
        bytes_transferred: d.len(),
        sense: [0u8; 32],
    })
}

fn not_ready(asc: u8, ascq: u8) -> Error {
    Error::ScsiError {
        opcode: SCSI_TEST_UNIT_READY,
        status: 2,
        sense: Some(ScsiSense {
            sense_key: crate::scsi::SENSE_KEY_NOT_READY,
            asc,
            ascq,
        }),
    }
}

fn fault() -> Error {
    Error::ScsiError {
        opcode: SCSI_TEST_UNIT_READY,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }
}

const TUR: [u8; 6] = [0, 0, 0, 0, 0, 0];
const MS: fn(u64) -> Duration = Duration::from_millis;

fn is_read(c: &[u8]) -> bool {
    c[0] == crate::scsi::SCSI_READ_10
}

// T6 scaled, keeping production's poll:window ratio (500 ms : 60 s = 1 : 120) so the
// old 60-poll cap (~30 s, half the window) shows up; polls never exceed 10 ms.
fn timing(window: Duration, dead_bus: Duration) -> WaitReadyTiming {
    WaitReadyTiming {
        poll: (window / 120).min(MS(10)),
        window,
        dead_bus,
    }
}

/// LD1: a cancelled op issues no CDB at all.
#[test]
fn exec_precheck_refuses_after_cancel() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    h.cancel();
    let r = d.exec(&TUR, DataDirection::None, &mut [], 5_000);
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(fake.log().is_empty(), "zero CDBs reach the transport");
}

/// LD2: every Drive CDB goes through `exec`: after a cancel none of these issue one.
#[test]
fn every_drive_cdb_goes_through_exec() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    h.cancel();
    let identify = crate::identity::DriveId::identify(&mut |c, dir, b, t| d.exec(c, dir, b, t));
    assert!(matches!(identify, Err(Error::Halted)), "inquiry");
    assert!(matches!(
        crate::disc::Disc::identify(&mut d),
        Err(Error::Halted)
    ));
    d.set_speed(0xFFFF);
    assert!(matches!(d.read_capacity(), Err(Error::Halted)));
    d.lock_tray();
    assert!(matches!(d.spin_cycle(), Err(Error::Halted)));
    let _ = d.drive_status();
    assert!(d.get_config_feature(0x010C).is_none());
    assert!(d.mode_sense_page(0x2A).is_none());
    assert!(d.read_buffer(2, 0xF1, 16).is_none());
    assert!(d.report_key_rpc_state().is_none());
    assert!(!d.enable_recovered_error_reporting());
    assert!(matches!(d.eject(), Err(Error::Halted)));
    assert!(matches!(
        d.scsi_execute(&TUR, DataDirection::None, &mut [], 5_000),
        Err(Error::Halted)
    ));
    assert!(d.init().is_err());
    assert!(fake.log().is_empty(), "{:02x?}", fake.cdbs());
}

/// LD2 (structural): no raw transport call outside `Drive::dispatch` in drive/mod.rs,
/// and none in identity.rs outside `DriveId::from_drive`.
#[test]
fn no_raw_execute_outside_dispatch() {
    let src = include_str!("mod.rs");
    let body = &src[..src.find("#[cfg(test)]\nmod halt_tests").unwrap()];
    let raw = body.matches(".execute(").count();
    assert_eq!(raw, 1, "only `dispatch` calls the transport's execute");
    let id = include_str!("../identity.rs");
    let id = &id[..id.find("#[cfg(test)]").unwrap()];
    assert_eq!(
        id.matches("transport.execute(").count(),
        1,
        "from_drive only"
    );
}

/// LD3: a READ that completes after a cancel is discarded: `Halted`, not good data.
#[test]
fn read_postcheck_discards_after_cancel() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t.rule(is_read, FakeMode::Stall).scale(1);
    let mut d = Drive::from_transport_with(Box::new(t), &h);
    let f2 = fake.clone();
    let h2 = h.clone();
    let stopper = std::thread::spawn(move || {
        assert!(f2.wait_for(1, is_read, Duration::from_secs(5)));
        h2.cancel();
        f2.release();
    });
    let mut buf = vec![0u8; 2048];
    let r = d.read(0, 1, &mut buf, false);
    stopper.join().unwrap();
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert_eq!(
        fake.count(is_read),
        1,
        "the in-flight READ ran to completion"
    );
}

/// LD4 (Drive half): `exec_cleanup` admits the ALLOW for a tray this Drive locked
/// and refuses anything else after a cancel.
#[test]
fn exec_cleanup_follows_the_drive_ledger() {
    let allow = [0x1E, 0, 0, 0, 0, 0];
    let prevent = [0x1E, 0, 0, 0, 1, 0];
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    h.cancel();
    let r = d.exec_cleanup(
        &allow,
        DataDirection::None,
        &mut [],
        5_000,
        CleanupCtx::Plain,
    );
    assert!(matches!(r, Err(Error::Halted)), "never locked: refused");
    assert!(fake.log().is_empty());

    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    d.lock_tray();
    h.cancel();
    let r = d.exec_cleanup(
        &prevent,
        DataDirection::None,
        &mut [],
        5_000,
        CleanupCtx::Plain,
    );
    assert!(matches!(r, Err(Error::Halted)), "never a PREVENT");
    let r = d.exec_cleanup(
        &allow,
        DataDirection::None,
        &mut [],
        5_000,
        CleanupCtx::Plain,
    );
    assert!(r.is_ok(), "locked: the ALLOW is sent");
    assert_eq!(fake.cdbs(), vec![prevent.to_vec(), allow.to_vec()]);
}

/// LD5 (per evidence SS-7): a successful REPORT KEY format 0 sets bit `resp[7] >> 6`;
/// format 3Fh clears bit `cdb[10] >> 6`; a failed format 0 sets nothing.
#[test]
fn agid_ledger_set_and_clear() {
    assert!(
        crate::spec::stop::SS_7_AGID_INVALIDATE
            .text
            .contains("*agid = (buf[7] & 0xff) >> 6;")
    );
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let f2 = fail.clone();
    let t = Script::new(move |c, d| {
        if c[0] == 0xA4 && c[10] & 0x3F == 0 {
            if f2.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(not_ready(0x04, 0x01));
            }
            d[7] = 2 << 6;
        }
        good(d)
    });
    let mut d = Drive::from_transport(Box::new(t));
    let mut alloc = [0u8; 12];
    alloc[0] = 0xA4;
    alloc[7] = 0x02;
    let mut buf = [0u8; 8];
    d.exec(&alloc, DataDirection::FromDevice, &mut buf, 5_000)
        .unwrap();
    assert_eq!(d.ledger().agids, 1 << 2);
    let mut release = alloc;
    release[10] = (2 << 6) | 0x3F;
    d.exec(&release, DataDirection::FromDevice, &mut buf, 5_000)
        .unwrap();
    assert_eq!(d.ledger().agids, 0);
    fail.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut buf = [0u8; 8];
    assert!(
        d.exec(&alloc, DataDirection::FromDevice, &mut buf, 5_000)
            .is_err()
    );
    assert_eq!(d.ledger().agids, 0, "a failed allocation holds nothing");
}

/// LD6: `lock_tray` after a cancel sends no PREVENT and leaves the tray unlocked.
#[test]
fn lock_tray_after_cancel_sends_nothing() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    h.cancel();
    d.lock_tray();
    assert!(!d.ledger().tray_locked);
    drop(d);
    assert!(fake.log().is_empty(), "no PREVENT, and so no ALLOW on drop");
}

/// LD7 (GUARD): a locked Drive dropped after a cancel issues exactly one ALLOW.
#[test]
fn drop_after_cancel_releases_prevent() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t.watch(&h)), &h);
    d.lock_tray();
    assert!(fake.tray_locked());
    h.cancel();
    drop(d);
    let after: Vec<_> = fake.log().into_iter().filter(|c| c.after_cancel).collect();
    assert_eq!(after.len(), 1, "{after:02x?}");
    assert_eq!(after[0].cdb, vec![0x1E, 0, 0, 0, 0, 0]);
    assert!(!fake.tray_locked());
    assert_eq!(fake.live_handles(), 0);
}

/// LD8: `open_with` shares the token; attach/detach; a detached `exec` is a debug
/// panic; the own token comes back after the `ScanOptions.halt` alias, unwind included.
#[test]
fn slot_open_with_attach_detach() {
    let h = Halt::new();
    let (t, _fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t), &h);
    assert!(Arc::ptr_eq(d.token().unwrap().as_arc(), h.as_arc()));
    h.cancel();
    assert!(matches!(
        d.exec(&TUR, DataDirection::None, &mut [], 5_000),
        Err(Error::Halted)
    ));
    let h2 = Halt::new();
    d.attach(&h2);
    assert!(d.exec(&TUR, DataDirection::None, &mut [], 5_000).is_ok());
    let back = d.detach().expect("attached");
    assert!(Arc::ptr_eq(back.as_arc(), h2.as_arc()));
    assert!(d.token().is_none());
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = d.exec(&TUR, DataDirection::None, &mut [], 5_000);
    }));
    assert_eq!(
        r.is_err(),
        cfg!(debug_assertions),
        "a detached exec panics in debug"
    );

    let (t, _fake) = FakeTransport::new();
    let mut d = Drive::from_transport(Box::new(t));
    let own = d.token().unwrap().clone();
    let alias = Halt::new();
    {
        let g = d.alias(Some(&alias));
        assert!(Arc::ptr_eq(g.token().unwrap().as_arc(), alias.as_arc()));
    }
    assert!(
        Arc::ptr_eq(d.token().unwrap().as_arc(), own.as_arc()),
        "restored"
    );
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _g = d.alias(Some(&alias));
        panic!("unwind through the alias");
    }));
    assert!(r.is_err());
    assert!(
        Arc::ptr_eq(d.token().unwrap().as_arc(), own.as_arc()),
        "restored on unwind"
    );
}

/// LD9: over a caller-attached token the alias does not replace it, and is logged.
#[test]
fn foreign_attached_token_wins_over_alias() {
    let h = Halt::new();
    let (t, _fake) = FakeTransport::new();
    let mut d = Drive::from_transport_with(Box::new(t), &h);
    let other = Halt::new();
    let (same, events) = crate::testlog::capture(|| {
        let g = d.alias(Some(&other));
        Arc::ptr_eq(g.token().unwrap().as_arc(), h.as_arc())
    });
    assert!(same, "the attached token wins");
    assert!(
        events
            .iter()
            .any(|e| e.level == tracing::Level::WARN && e.field("phase") == Some("halt_alias")),
        "{events:?}"
    );
    let (_, quiet) = crate::testlog::capture(|| drop(d.alias(Some(&h))));
    assert!(
        quiet.iter().all(|e| e.field("phase") != Some("halt_alias")),
        "same token: no warn"
    );
}

// A drive whose TUR answers 04/01 (`ascq`) until `start` START UNITs, then GOOD.
fn becoming_ready(ascq: u8) -> Script {
    Script::new(move |c, d| match c[0] {
        SCSI_TEST_UNIT_READY => Err(not_ready(0x04, ascq)),
        _ => good(d),
    })
}

/// LD10 (GUARD): a Stop during an in-flight TUR, or START UNIT, returns `Halted`
/// within 1 s of that CDB completing.
#[test]
fn wait_ready_stop_during_poll_and_start_unit() {
    for (stalled, ascq) in [(SCSI_TEST_UNIT_READY, 0x01u8), (SCSI_START_STOP_UNIT, 0x02)] {
        let h = Halt::new();
        let (t, fake) = FakeTransport::new();
        let t = t
            .with_inner(Box::new(becoming_ready(ascq)))
            .rule_n(move |c| c[0] == stalled, FakeMode::Stall, 1)
            .scale(1);
        let mut d = Drive::from_transport_with(Box::new(t), &h);
        let (f2, h2) = (fake.clone(), h.clone());
        let stopper = std::thread::spawn(move || {
            assert!(f2.wait_for(1, |c| c[0] == stalled, Duration::from_secs(5)));
            h2.cancel();
            std::thread::sleep(MS(100));
            f2.release();
            Instant::now()
        });
        let r = d.wait_ready_with(timing(Duration::from_secs(60), MS(5_000)));
        let released = stopper.join().unwrap();
        assert!(matches!(r, Err(Error::Halted)), "{stalled:#x}: {r:?}");
        assert!(released.elapsed() < Duration::from_secs(1), "{stalled:#x}");
    }
}

/// LD11a (GUARD, T5): transport failures broken by a drive answer every 0.5 × the
/// dead-bus budget, for 4 budgets, never count as a dead bus.
#[test]
fn dead_bus_budget_resets_on_any_answer() {
    let budget = MS(100);
    let t0 = Instant::now();
    let mut last = Instant::now();
    let t = Script::new(move |_, d| {
        if t0.elapsed() >= budget * 4 {
            return good(d);
        }
        if last.elapsed() >= budget / 2 {
            last = Instant::now();
            return Err(not_ready(0x04, 0x01));
        }
        Err(fault())
    });
    let mut d = Drive::from_transport(Box::new(t));
    let r = d.wait_ready_with(timing(Duration::from_secs(60), budget));
    assert!(r.is_ok(), "{r:?}");
}

/// LD11b / G5 (GUARD): an unbroken run of transport failures for the budget is a
/// dead bus, surfaced as the transport failure.
#[test]
fn dead_bus_unbroken_run_fails() {
    let t = Script::new(|_, _| Err(fault()));
    let mut d = Drive::from_transport(Box::new(t));
    let t0 = Instant::now();
    let r = d.wait_ready_with(timing(Duration::from_secs(60), MS(100)));
    assert!(
        matches!(&r, Err(e) if e.is_scsi_transport_failure()),
        "{r:?}"
    );
    assert!(t0.elapsed() < Duration::from_secs(1));
}

/// LD12a: 04/01 with a progress indication rising every 0.5 × window for 4 windows,
/// then ready → `Ok`. Corroborated by SS-1: the field is "a percent complete indication".
#[test]
fn wait_ready_rides_out_progressing_spin_up() {
    let window = MS(200);
    let t0 = Instant::now();
    let (t, cell) = Script::with_progress(move |_, d| {
        if t0.elapsed() >= window * 4 {
            return good(d);
        }
        Err(not_ready(0x04, 0x01))
    });
    let t0b = Instant::now();
    let mover = std::thread::spawn(move || {
        while t0b.elapsed() < window * 4 {
            let step = (t0b.elapsed().as_millis() / (window.as_millis() / 2)) as u16;
            *cell.lock().unwrap() = Some(1000 * (step + 1));
            std::thread::sleep(MS(5));
        }
    });
    let mut d = Drive::from_transport(Box::new(t));
    let r = d.wait_ready_with(timing(window, MS(5_000)));
    mover.join().unwrap();
    assert!(r.is_ok(), "a progressing spin-up is not a stall: {r:?}");
    assert!(t0.elapsed() >= window * 2, "ran past two windows");
}

/// LD12b: the same 04/01 with no (or stuck) progress gives up at the window, not before.
#[test]
fn wait_ready_gives_up_after_60s_without_progress() {
    for stuck in [None, Some(4096u16)] {
        let window = MS(300);
        let (t, cell) = Script::with_progress(|_, _| Err(not_ready(0x04, 0x01)));
        *cell.lock().unwrap() = stuck;
        let mut d = Drive::from_transport(Box::new(t));
        let t0 = Instant::now();
        let r = d.wait_ready_with(timing(window, MS(5_000)));
        let took = t0.elapsed();
        assert!(matches!(r, Err(Error::DeviceNotReady { .. })), "{r:?}");
        assert!(took >= window, "not before the window: {took:?}");
        assert!(
            took < window + Duration::from_secs(1),
            "at the window: {took:?}"
        );
    }
}

/// LD12c: a not-yet-seen answer is progress; two known answers alternating is not.
/// (The stricter reading of "the answer changes", §2.11; the user may overrule it.)
#[test]
fn wait_ready_new_answer_is_progress_flapping_is_not() {
    let window = MS(200);
    let t0 = Instant::now();
    let fresh = [0x00u8, 0x01, 0x04, 0x07, 0x08, 0x09, 0x0A, 0x11, 0x22];
    let t = Script::new(move |_, d| {
        let step = (t0.elapsed().as_millis() / (window.as_millis() / 2)) as usize;
        match fresh.get(step) {
            Some(&q) if step < 8 => Err(not_ready(0x04, q)),
            _ => good(d),
        }
    });
    let mut d = Drive::from_transport(Box::new(t));
    let r = d.wait_ready_with(timing(window, MS(5_000)));
    assert!(r.is_ok(), "new answers re-arm the window: {r:?}");

    let mut n = 0u32;
    let t = Script::new(move |_, _| {
        n += 1;
        Err(not_ready(
            0x04,
            if n.is_multiple_of(2) { 0x00 } else { 0x01 },
        ))
    });
    let mut d = Drive::from_transport(Box::new(t));
    let t0 = Instant::now();
    let r = d.wait_ready_with(timing(window, MS(5_000)));
    assert!(matches!(r, Err(Error::DeviceNotReady { .. })), "{r:?}");
    assert!(t0.elapsed() < window + Duration::from_secs(1));
}

/// LD12d (GUARD): a Stop at 0.75 × window, and mid-progress, ends the wait at once.
#[test]
fn wait_ready_stop_always_interrupts() {
    let window = MS(400);
    for progressing in [false, true] {
        let h = Halt::new();
        let mut q = 0u8;
        let t = Script::new(move |_, _| {
            if progressing {
                q = q.wrapping_add(1) % 0x30;
            }
            Err(not_ready(0x04, if q == 2 { 0x03 } else { q }))
        });
        let mut d = Drive::from_transport_with(Box::new(t), &h);
        let h2 = h.clone();
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(window * 3 / 4);
            h2.cancel();
            Instant::now()
        });
        let r = d.wait_ready_with(timing(window, MS(5_000)));
        let at = stopper.join().unwrap();
        assert!(matches!(r, Err(Error::Halted)), "{r:?}");
        assert!(at.elapsed() < Duration::from_secs(1));
    }
}

/// G4 (GUARD): 10 × 3A with no 04/01 is the empty drive; 30/xx fails at once; 04/02
/// sends exactly one START UNIT. Per spec SS-3 MMC-6 Table F.3.
#[test]
fn wait_ready_terminal_answers_unchanged() {
    let f3 = crate::spec::stop::SS_3_READINESS_ERRORS.text;
    assert!(f3.contains("2 3A 00 MEDIUM NOT PRESENT"));
    assert!(f3.contains("2 30 00 INCOMPATIBLE MEDIUM INSTALLED"));
    assert!(f3.contains("2 04 02 LOGICAL UNIT NOT READY, INITIALIZING CMD. REQUIRED"));
    let slow = timing(Duration::from_secs(60), MS(5_000));
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let p2 = polls.clone();
    let t = Script::new(move |_, _| {
        p2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(not_ready(0x3A, 0x00))
    });
    let r = Drive::from_transport(Box::new(t)).wait_ready_with(slow);
    assert!(
        matches!(&r, Err(e) if e.scsi_sense().is_some_and(|s| s.asc == 0x3A)),
        "{r:?}"
    );
    assert_eq!(polls.load(std::sync::atomic::Ordering::Relaxed), 10);

    let t = Script::new(|_, _| Err(not_ready(0x30, 0x00)));
    let r = Drive::from_transport(Box::new(t)).wait_ready_with(slow);
    assert!(
        matches!(&r, Err(e) if e.scsi_sense().is_some_and(|s| s.asc == 0x30)),
        "{r:?}"
    );

    let (t, fake) = FakeTransport::new();
    let mut tur = 0;
    let t = t.with_inner(Box::new(Script::new(move |c, d| match c[0] {
        SCSI_TEST_UNIT_READY if tur < 3 => {
            tur += 1;
            Err(not_ready(0x04, 0x02))
        }
        _ => good(d),
    })));
    assert!(
        Drive::from_transport(Box::new(t))
            .wait_ready_with(slow)
            .is_ok()
    );
    assert_eq!(fake.count(|c| c[0] == SCSI_START_STOP_UNIT), 1);
}

/// LD13: a Stop during the drive-prep unlock is `Halted`, not the unlock's dead bus.
#[test]
fn drive_init_stop_is_halted_not_unlock_error() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    // Cancel on the first unlock CDB (the freemkv IDENTITY knock); every later one is
    // refused, so the unlockers see a dead bus.
    let t = t.cancel_on(|_| true, &h).watch(&h);
    let mut d = Drive::from_transport_with(Box::new(t), &h);
    let r = d.init();
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert_eq!(fake.log().len(), 1, "nothing after the cancel");
}

/// LD16a (GUARD, T1): 200 READs each at 0.8 × the fast timeout all succeed, however
/// long they take together.
#[test]
fn slow_reads_within_cdb_timeout_never_fail_op() {
    let (t, fake) = FakeTransport::new();
    // Scale 1000: the 10 s fast READ times out at 10 ms; each takes 8 ms.
    let t = t
        .with_image(vec![0u8; 2048 * 4])
        .rule(is_read, FakeMode::Complete(MS(8)));
    let mut d = Drive::from_transport(Box::new(t));
    let mut buf = vec![0u8; 2048];
    for _ in 0..200 {
        d.read(1, 1, &mut buf, false)
            .expect("a slow READ inside its timeout is good");
    }
    assert_eq!(fake.count(is_read), 200);
}

/// LD16b (GUARD, T1): a READ stalled past its timeout fails that CDB only; the next
/// READ is still issued.
#[test]
fn stalled_read_fails_only_that_cdb() {
    let (t, fake) = FakeTransport::new();
    let t = t
        .with_image(vec![0u8; 2048 * 4])
        .rule_n(is_read, FakeMode::Stall, 1);
    let mut d = Drive::from_transport(Box::new(t));
    let mut buf = vec![0u8; 2048];
    assert!(matches!(
        d.read(1, 1, &mut buf, false),
        Err(Error::DiscRead { .. })
    ));
    assert!(d.read(2, 1, &mut buf, false).is_ok());
    assert_eq!(fake.count(is_read), 2);
}

/// G21 (GUARD): `ScsiSense` keeps exactly its three public fields (consumers build it
/// with a struct literal); the progress indication rides a side channel instead.
#[test]
fn scsi_sense_keeps_three_fields() {
    let s = ScsiSense {
        sense_key: 2,
        asc: 4,
        ascq: 1,
    };
    let ScsiSense {
        sense_key,
        asc,
        ascq,
    } = s;
    assert_eq!((sense_key, asc, ascq), (2, 4, 1));
}

/// §2.2 T29 feed: `attach_progress` bumps on every CDB completion and is busy while
/// one is in flight.
#[test]
fn attached_progress_bumps_per_cdb_and_is_busy_in_flight() {
    let p = crate::halt::Progress::new();
    let p2 = p.clone();
    let t = Script::new(move |_, d| {
        assert!(p2.is_busy(), "busy mid-CDB");
        good(d)
    });
    let mut d = Drive::from_transport(Box::new(t));
    d.attach_progress(&p);
    d.exec(&TUR, DataDirection::None, &mut [], 5_000).unwrap();
    d.exec(&TUR, DataDirection::None, &mut [], 5_000).unwrap();
    assert_eq!(p.get(), 2);
}

/// SP5: per spec SS-3 (MMC-6 Table F.3), each named readiness answer maps to its
/// `wait_ready` action: 04/01 keep polling, 04/02 one START UNIT, 30/00 fail at once,
/// 3A/00 count toward the empty drive.
#[test]
fn asc_ascq_classification_table() {
    #[derive(Debug, PartialEq)]
    enum Action {
        Continue,
        StartUnitOnce,
        FailFast,
        EmptyCount,
    }
    let f3 = crate::spec::stop::SS_3_READINESS_ERRORS.text;
    let table = [
        (
            "2 04 01 LOGICAL UNIT IS IN PROCESS OF BECOMING READY",
            0x04,
            0x01,
            Action::Continue,
        ),
        (
            "2 04 02 LOGICAL UNIT NOT READY, INITIALIZING CMD. REQUIRED",
            0x04,
            0x02,
            Action::StartUnitOnce,
        ),
        (
            "2 30 00 INCOMPATIBLE MEDIUM INSTALLED",
            0x30,
            0x00,
            Action::FailFast,
        ),
        ("2 3A 00 MEDIUM NOT PRESENT", 0x3A, 0x00, Action::EmptyCount),
    ];
    for (row, asc, ascq, want) in table {
        assert!(f3.contains(row), "SS-3 quotes {row:?}");
        // Answer (asc, ascq) for 12 polls, then GOOD.
        let (t, fake) = FakeTransport::new();
        let mut n = 0;
        let t = t.with_inner(Box::new(Script::new(move |c, d| match c[0] {
            SCSI_TEST_UNIT_READY if n < 12 => {
                n += 1;
                Err(not_ready(asc, ascq))
            }
            _ => good(d),
        })));
        let r = Drive::from_transport(Box::new(t))
            .wait_ready_with(timing(Duration::from_secs(60), MS(5_000)));
        let turs = fake.count(|c| c[0] == SCSI_TEST_UNIT_READY);
        let starts = fake.count(|c| c[0] == SCSI_START_STOP_UNIT);
        let got = match (&r, turs, starts) {
            (Ok(()), 13, 0) => Action::Continue,
            (Ok(()), 13, 1) => Action::StartUnitOnce,
            (Err(_), 1, 0) => Action::FailFast,
            (Err(_), 10, 0) => Action::EmptyCount,
            other => panic!("{row}: unexpected {other:?}"),
        };
        assert_eq!(got, want, "{row}");
    }
}

//! Stop design v5.6 §5.1 "Session (ST-L3)": `DiscSession::{open_with, finish}` over a
//! [`FakeTransport`](crate::test_util::FakeTransport). `open_with` itself opens a device
//! path, so these drive its shared bring-up over a fake Drive under the op token.

use super::*;
use crate::test_util::{FakeHandle, FakeTransport};
use std::time::Duration;

fn is_allow(c: &[u8]) -> bool {
    c[0] == crate::drive::allow::PREVENT_ALLOW && c[4] & 1 == 0
}

fn is_prevent(c: &[u8]) -> bool {
    c[0] == crate::drive::allow::PREVENT_ALLOW && c[4] & 1 == 1
}

// SS-6 MMC-6 Table 633: LoEj 1, Start 0 = "Eject the disc if permitted".
fn is_eject(c: &[u8]) -> bool {
    c[0] == crate::drive::allow::START_STOP_UNIT && c[4] & 0x03 == 0x02
}

// The `n`th (1-based) CDB.
fn nth(n: usize) -> impl Fn(&[u8]) -> bool + Send + 'static {
    let seen = std::sync::atomic::AtomicUsize::new(0);
    move |_| seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 == n
}

// A session brought up over `t` under `halt`, as `open_with` does after its open.
fn session_over(t: FakeTransport, halt: &Halt) -> Result<DiscSession> {
    let drive = Drive::from_transport_with(Box::new(t), halt);
    DiscSession::bring_up(drive, KeySpec::default(), Some(halt.clone()))
}

// The CDBs the fake received after the watched token was cancelled.
fn after_cancel(fake: &FakeHandle) -> Vec<Vec<u8>> {
    fake.log()
        .into_iter()
        .filter(|c| c.after_cancel)
        .map(|c| c.cdb)
        .collect()
}

/// LSe1 (§2.4 row 2, SS-6): `finish(Eject)` after a Stop sends "ALLOW (if locked), then
/// START STOP LoEj; no other CDB". SS-6 MMC-6 Table 633: "1 0 Eject the disc if
/// permitted." — so the ALLOW comes first.
#[test]
fn finish_eject_after_cancel_allowed() {
    for locked in [true, false] {
        let h = Halt::new();
        let (t, fake) = FakeTransport::new();
        let t = t.watch(&h).allow_after_cancel(is_eject);
        let mut s = session_over(t, &h).expect("brought up");
        if locked {
            s.lock_tray();
            assert!(fake.tray_locked());
        }
        h.cancel();
        s.finish(Finish::Eject)
            .expect("an eject after a Stop is allowed");
        let after = after_cancel(&fake);
        let want: &[fn(&[u8]) -> bool] = if locked {
            &[is_allow, is_eject]
        } else {
            &[is_eject]
        };
        assert_eq!(after.len(), want.len(), "locked={locked}: {after:02x?}");
        assert!(
            after.iter().zip(want).all(|(c, p)| p(c)),
            "locked={locked}: {after:02x?}"
        );
        assert_eq!(fake.live_handles(), 0, "the handle is closed");
    }
}

/// LSe2: `finish(Release)` and `finish(Unlock)` close the handle and leave the tray
/// unlocked (open count 0). SS-5 MMC-6 Table 329: "0 0 Prevent State shall be cleared
/// (Unlocked)" — `Unlock` sends it even for a lock this session did not take.
#[test]
fn finish_release_and_unlock() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let mut s = session_over(t, &h).expect("brought up");
    s.lock_tray();
    s.finish(Finish::Release).expect("release");
    assert_eq!(fake.live_handles(), 0, "Release closes the handle");
    assert!(
        !fake.tray_locked(),
        "a tray this session locked is unlocked"
    );

    let (t, fake) = FakeTransport::new();
    let s = session_over(t, &Halt::new()).expect("brought up");
    let allows = fake.count(is_allow);
    s.finish(Finish::Unlock).expect("unlock");
    assert_eq!(fake.count(is_allow), allows + 1, "Unlock sends one ALLOW");
    assert_eq!(fake.count(is_prevent), 0);
    assert_eq!(fake.live_handles(), 0, "Unlock closes the handle");
}

/// LSe2 (the no-drive case): `Unlock`/`Eject` on a session whose drive has left it is a
/// typed error, `Release` is a no-op.
#[test]
fn finish_without_a_drive() {
    let s = DiscSession::from_parts_for_test(None, None);
    assert!(s.finish(Finish::Release).is_ok());
    for how in [Finish::Unlock, Finish::Eject] {
        let s = DiscSession::from_parts_for_test(None, None);
        assert!(
            matches!(s.finish(how), Err(Error::DeviceNotReady { .. })),
            "{how:?}"
        );
    }
}

/// LSe3: after a stopped op drops — its Drive-holding prefetcher joined (§2.5) — no
/// handle and no Drive holder is left, and a new `open_with` session comes up under
/// its own token.
#[test]
fn open_with_succeeds_after_stopped_op_drops() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let h1 = Halt::new();
    let (t, fake1) = FakeTransport::new();
    let t = t.with_image(vec![0u8; 2048 * 300]);
    let drive = session_over(t, &h1)
        .expect("brought up")
        .into_drive()
        .expect("drive");
    let extents = vec![crate::disc::Extent {
        start_lba: 0,
        sector_count: 300,
    }];
    let pf = crate::sector::PrefetchedSectorSource::new(drive, extents, 3, Some(h1.clone()))
        .expect("spawn");
    std::thread::sleep(Duration::from_millis(20));
    h1.cancel();
    drop(pf);
    assert_eq!(fake1.live_handles(), 0, "the stopped op's handle is closed");
    assert_eq!(fake1.drive_holders(), 0, "the prefetcher was joined");

    let h2 = Halt::new();
    let (t, fake2) = FakeTransport::new();
    let s = session_over(t.watch(&h2), &h2).expect("a new open succeeds");
    let token = s.token().expect("open_with keeps its token");
    assert!(Arc::ptr_eq(token.as_arc(), h2.as_arc()));
    assert!(!token.is_cancelled());
    drop(s);
    assert_eq!(fake2.live_handles(), 0);
}

/// Stop is always honoured: a Stop during `open_with`'s advisory bring-up is the one
/// failure that is not advisory. The session is not built and the handle is closed.
#[test]
fn open_with_stop_during_bring_up_is_halted() {
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t.cancel_on(nth(1), &h).watch(&h);
    let r = session_over(t, &h);
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
    assert_eq!(
        fake.log().len(),
        1,
        "no CDB after the Stop: {:02x?}",
        fake.cdbs()
    );
    assert_eq!(fake.live_handles(), 0);
}

/// The final `check()` (§6 ST-L3): a Stop after the bring-up's last CDB still ends
/// `open_with` `Halted`.
#[test]
fn open_with_final_check() {
    let (t, dry) = FakeTransport::new();
    session_over(t, &Halt::new()).expect("dry run");
    let last = dry.log().len();
    let h = Halt::new();
    let (t, fake) = FakeTransport::new();
    let r = session_over(t.cancel_on(nth(last), &h), &h);
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
    assert_eq!(fake.log().len(), last);
}

/// Guard: `open` (no op token) keeps today's advisory bring-up and has no token.
#[test]
fn open_without_a_token_stays_advisory() {
    let (t, _fake) = FakeTransport::new();
    let drive = Drive::from_transport(Box::new(t));
    let s = DiscSession::bring_up(drive, KeySpec::default(), None).expect("advisory");
    assert!(s.token().is_none());
}

//! Stop design v5.6 LP14a/b and the §2.5 acquire rules, on a scaled T10 window; SP6,
//! SP7 (SS-8, SS-9) and SP8 (SS-10, Windows: compile on dev, run on qa).

use super::*;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

const WINDOW: Duration = Duration::from_millis(200);
const SLACK: Duration = Duration::from_secs(1);

fn artifact() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("Movie.iso");
    (dir, out)
}

fn take(out: &Path, window: Duration) -> Result<ArtifactLock> {
    ArtifactLock::acquire_within(out, &[], &Halt::new(), window)
}

/// §2.5 name rule: "The sidecar is always named after the final artifact name,
/// `<final>.lock`" — the same before and after the rename; two artifacts sharing a
/// stem never share a lock.
#[test]
fn sidecar_is_named_after_the_final_artifact() {
    let dir = Path::new("/srv/rips");
    assert_eq!(
        lock_path(&dir.join("Movie.iso")),
        dir.join("Movie.iso.lock")
    );
    assert_ne!(
        lock_path(&dir.join("Movie.iso")),
        lock_path(&dir.join("Movie.mkv"))
    );
}

/// LP14a (T10, stall pair a): the holder appends to the `.partial` every 0.5 × window
/// for 4 windows, then releases: the waiter is never timed out and takes the lock.
#[test]
fn artifact_lock_waits_while_holder_progresses() {
    let (_dir, out) = artifact();
    let holder = take(&out, WINDOW).expect("first taker");
    let partial = with_suffix(&out, ".partial");
    let writer = thread::spawn(move || {
        let mut f = File::create(&partial).unwrap();
        for _ in 0..8 {
            thread::sleep(WINDOW / 2);
            f.write_all(b"sector").unwrap();
        }
        drop(holder);
    });
    let t = Instant::now();
    let got = take(&out, WINDOW);
    let took = t.elapsed();
    writer.join().unwrap();
    assert!(got.is_ok(), "a progressing holder is waited for: {got:?}");
    assert!(took >= WINDOW * 3, "waited past several windows: {took:?}");
}

/// LP14b (T10, stall pair b): a frozen holder → `TimedOut { op: "artifact_lock" }`
/// (E9073) within window + 1 s, never before the window.
#[test]
fn artifact_lock_times_out_on_frozen_holder() {
    let (_dir, out) = artifact();
    let _holder = take(&out, WINDOW).expect("first taker");
    let t = Instant::now();
    let r = take(&out, WINDOW);
    let took = t.elapsed();
    match r {
        Err(
            e @ Error::TimedOut {
                op: "artifact_lock",
            },
        ) => {
            assert_eq!(e.code(), crate::error::E_TIMED_OUT)
        }
        other => panic!("expected TimedOut(artifact_lock), got {other:?}"),
    }
    assert!(took >= WINDOW, "fired before the window: {took:?}");
    assert!(took <= WINDOW + SLACK, "fired late: {took:?}");
}

/// LP14b: "a cancel during the wait → `Halted`", within a slice (≤ 1 s asserted).
#[test]
fn artifact_lock_cancel_during_wait_is_halted() {
    let (_dir, out) = artifact();
    let _holder = take(&out, WINDOW).expect("first taker");
    let halt = Halt::new();
    let h2 = halt.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        h2.cancel();
    });
    let t = Instant::now();
    let r = ArtifactLock::acquire_within(&out, &[], &halt, Duration::from_secs(60));
    canceller.join().unwrap();
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(t.elapsed() <= SLACK, "{:?}", t.elapsed());
}

/// §2.5 acquire loop (the unlink race): the holder deletes the sidecar while holding it;
/// a waiter locked on the old, unlinked file re-checks identity and retries, so it ends
/// up holding the file the path names — which a third taker then cannot lock.
#[test]
fn artifact_lock_rechecks_identity_after_the_holder_deletes() {
    let (_dir, out) = artifact();
    let holder = take(&out, WINDOW).expect("first taker");
    let (out2, got) = (out.clone(), Arc::new(AtomicBool::new(false)));
    let flag = got.clone();
    let waiter = thread::spawn(move || {
        let lock = take(&out2, Duration::from_secs(10)).expect("second taker");
        flag.store(true, Ordering::SeqCst);
        thread::sleep(WINDOW * 2);
        lock
    });
    thread::sleep(WINDOW / 2);
    holder.delete().expect("deleted while held");
    let t = Instant::now();
    while !got.load(Ordering::SeqCst) {
        assert!(t.elapsed() < SLACK * 5, "the waiter never took the lock");
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        lock_path(&out).exists(),
        "the waiter re-created the sidecar"
    );
    let third = take(&out, Duration::from_millis(100));
    assert!(
        matches!(third, Err(Error::TimedOut { .. })),
        "the waiter holds the file the path names: {third:?}"
    );
    drop(waiter.join().unwrap());
}

/// §2.5 lifetime: "Deleted on success … and on Discard, both while holding it. Kept
/// after Stop, a failure or a crash, where it guards the resumable artifact."
#[test]
fn artifact_lock_kept_on_drop_deleted_on_delete() {
    let (_dir, out) = artifact();
    drop(take(&out, WINDOW).expect("taken"));
    assert!(lock_path(&out).exists(), "a dropped lock keeps its sidecar");
    let again = take(&out, WINDOW).expect("a released lock can be taken again");
    again.delete().expect("delete");
    assert!(!lock_path(&out).exists(), "delete removes the sidecar");
}

/// T10's progress signal includes a watched file (the engine's mapfile), read by path.
#[test]
fn artifact_lock_watches_extra_paths() {
    let (dir, out) = artifact();
    let mapfile = dir.path().join("Movie.iso.mapfile");
    let holder = take(&out, WINDOW).expect("first taker");
    let m2 = mapfile.clone();
    let writer = thread::spawn(move || {
        for i in 0..8u8 {
            thread::sleep(WINDOW / 2);
            std::fs::write(&m2, vec![i; usize::from(i) + 1]).unwrap();
        }
        drop(holder);
    });
    let t = Instant::now();
    let got = ArtifactLock::acquire_within(&out, &[&mapfile], &Halt::new(), WINDOW);
    let took = t.elapsed();
    writer.join().unwrap();
    assert!(got.is_ok(), "mapfile progress re-arms T10: {got:?}");
    assert!(took >= WINDOW * 3, "waited for the holder: {took:?}");
}

/// SP6 (SS-8, man7 flock(2)): "If a process uses open(2) (or similar) to obtain more
/// than one file descriptor for the same file, these file descriptors are treated
/// independently by flock()"; duplicates "refer to the same lock". Per spec.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn flock_is_per_open_file_description() {
    use std::os::fd::AsRawFd;
    let (_dir, out) = artifact();
    let p = lock_path(&out);
    let a = os::open_rw(&p).unwrap();
    let b = os::open_rw(&p).unwrap();
    // SAFETY: both fds are open for the calls.
    let rc = unsafe { libc::flock(a.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let e = io::Error::last_os_error();
        if matches!(e.raw_os_error(), Some(libc::ENOLCK | libc::EOPNOTSUPP)) {
            eprintln!("SKIP SP6: flock unsupported on the tmpdir: {e}");
            return;
        }
        panic!("flock: {e}");
    }
    assert!(
        !os::try_lock_exclusive(&b).unwrap(),
        "a second open conflicts"
    );
    let dup = a.try_clone().unwrap();
    assert!(
        os::try_lock_exclusive(&dup).unwrap(),
        "a dup shares the lock"
    );
}

/// SP7 (SS-9, XBD <sys/stat.h>): "A file identity is uniquely determined by the
/// combination of st_dev and st_ino" — so it follows the file across `rename`.
#[cfg(unix)]
#[test]
fn dev_ino_identify_file_across_rename() {
    let (dir, out) = artifact();
    let tmp = dir.path().join("Movie.iso.lock.tmp");
    let f = os::open_rw(&tmp).unwrap();
    let before = os::file_id(&f).unwrap();
    std::fs::rename(&tmp, &out).unwrap();
    assert_eq!(os::path_id(&out).unwrap(), before);
    let other = os::open_rw(&tmp).unwrap();
    assert_ne!(os::file_id(&other).unwrap(), before);
}

/// SP8 (SS-10, BY_HANDLE_FILE_INFORMATION): "The identifier (low and high parts) and the
/// volume serial number uniquely identify a file on a single computer." Runs on qa.
#[cfg(windows)]
#[test]
fn windows_file_id_identifies_file() {
    let (dir, out) = artifact();
    let a = os::open_rw(&out).unwrap();
    assert_eq!(os::file_id(&a).unwrap(), os::path_id(&out).unwrap());
    let other = dir.path().join("Other.iso");
    let b = os::open_rw(&other).unwrap();
    assert_ne!(os::file_id(&a).unwrap(), os::file_id(&b).unwrap());
    assert!(os::try_lock_exclusive(&a).unwrap());
    let second = os::open_rw(&out).unwrap();
    assert!(
        !os::try_lock_exclusive(&second).unwrap(),
        "a second handle conflicts"
    );
}

/// Review minor 4 (§2.5, T10): an identity mismatch that never clears is a wait, not a
/// spin — it ends `TimedOut { op: "artifact_lock" }` (E9073) after the window, having
/// re-checked about once per `WAIT_SLICE`.
#[test]
fn persistent_identity_mismatch_times_out_not_spins() {
    let (_dir, out) = artifact();
    let checks = std::sync::atomic::AtomicUsize::new(0);
    let ids = |_: &File, _: &Path| {
        checks.fetch_add(1, Ordering::SeqCst);
        (Ok((1, 1)), Ok((1, 2)))
    };
    let t = Instant::now();
    let r = ArtifactLock::acquire_ids(&out, &[], &Halt::new(), WINDOW, &ids);
    let took = t.elapsed();
    assert!(
        matches!(
            r,
            Err(Error::TimedOut {
                op: "artifact_lock"
            })
        ),
        "{r:?}"
    );
    assert!(took >= WINDOW && took <= WINDOW + SLACK, "{took:?}");
    let n = checks.load(Ordering::SeqCst);
    let per_slice = (WINDOW.as_millis() / WAIT_SLICE.as_millis()) as usize;
    assert!(
        n <= per_slice * 2 + 2,
        "{n} re-checks in one window: a spin"
    );
}

/// Review minor 3 (§2.5): "`ESTALE` from `fstat`/`stat` … is treated as 'retry'" — on
/// the open file's own id too; the next attempt then takes the lock.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn estale_from_fstat_retries() {
    let (_dir, out) = artifact();
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let ids = |f: &File, p: &Path| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            (
                Err(io::Error::from_raw_os_error(libc::ESTALE)),
                os::path_id(p),
            )
        } else {
            (os::file_id(f), os::path_id(p))
        }
    };
    let lock = ArtifactLock::acquire_ids(&out, &[], &Halt::new(), WINDOW, &ids);
    assert!(lock.is_ok(), "{lock:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one retry, then held");
}

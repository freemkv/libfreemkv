//! Stop design v5.6 LP14a/b and the §2.5 acquire rules, on a scaled T10 window; SP6,
//! SP7 (SS-8, SS-9) and SP8 (SS-10, Windows: compile on dev, run on qa).

use super::*;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

// A 1 s window and 2 s upper bounds keep a 200 ms sleep overshoot inside them.
const WINDOW: Duration = Duration::from_secs(1);
const SLACK: Duration = Duration::from_secs(2);

#[test]
fn public_file_identity_recognizes_hard_links_not_equal_contents() {
    let dir = tempfile::tempdir().unwrap();
    let original = dir.path().join("source.iso");
    let alias = dir.path().join("alias.mkv");
    let separate = dir.path().join("separate.iso");
    std::fs::write(&original, b"same bytes").unwrap();
    std::fs::write(&separate, b"same bytes").unwrap();
    std::fs::hard_link(&original, &alias).unwrap();
    assert_eq!(
        file_identity(&original).unwrap(),
        file_identity(&alias).unwrap()
    );
    assert_ne!(
        file_identity(&original).unwrap(),
        file_identity(&separate).unwrap()
    );
    assert!(file_identity(&dir.path().join("missing")).is_err());
}

#[test]
fn public_identity_tracks_directory_renames_and_detects_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("parent");
    let retained = temp.path().join("retained");
    std::fs::create_dir(&directory).unwrap();
    let original = file_identity(&directory).unwrap();
    std::fs::rename(&directory, &retained).unwrap();
    assert_eq!(file_identity(&retained).unwrap(), original);
    std::fs::create_dir(&directory).unwrap();
    assert_ne!(file_identity(&directory).unwrap(), original);
}

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

/// LP14a (T10, stall pair a): the holder appends to the `.partial` every 0.2 × window
/// for over 3 windows, then releases: the waiter is never timed out and takes the lock.
#[test]
fn artifact_lock_waits_while_holder_progresses() {
    let (_dir, out) = artifact();
    let holder = take(&out, WINDOW).expect("first taker");
    let partial = with_suffix(&out, ".partial");
    let writer = thread::spawn(move || {
        let mut f = File::create(&partial).unwrap();
        for _ in 0..16 {
            thread::sleep(WINDOW / 5);
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

// smbfs against Samba's fruit:zero_file_id=yes derives inode numbers from the name:
// a deleted sidecar's open fd and a recreated one compare equal.
#[cfg(unix)]
type IdPair = (io::Result<(u64, u64)>, io::Result<(u64, u64)>);

#[cfg(unix)]
fn name_derived(_: &File, p: &Path) -> IdPair {
    use std::hash::{Hash, Hasher};
    let mut name = std::collections::hash_map::DefaultHasher::new();
    p.hash(&mut name);
    (Ok((1, name.finish())), Ok((1, name.finish())))
}

#[cfg(unix)]
fn proof(f: &File, p: &Path) -> Proof {
    os::names_this_file(f, p, &os::try_lock_exclusive).unwrap()
}

#[test]
#[cfg(unix)]
fn a_deleted_sidecar_fd_does_not_name_a_recreated_one() {
    let (_dir, out) = artifact();
    let path = lock_path(&out);
    let old = os::open_rw(&path).unwrap();
    assert!(matches!(proof(&old, &path), Proof::Ours));
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(proof(&old, &path), Proof::Retry(e) if os::id_retryable(&e)));
    let new = os::open_rw(&path).unwrap();
    assert!(matches!(proof(&old, &path), Proof::Retry(_)));
    assert!(matches!(proof(&new, &path), Proof::Ours));
}

// Cross-process holder: this binary re-run on `child_helper` (with CHILD_MARKER), which
// locks in its own process and obeys stdin lines. "hold" prints CHILD:held; "replace"
// also deletes and retakes the sidecar after "go" (CHILD:replaced).
const CHILD_MARKER: &str = "freemkv-lock-child";

#[test]
fn child_helper() {
    use std::io::BufRead;
    if !std::env::args().any(|a| a == CHILD_MARKER) {
        return;
    }
    let Some(dir) = std::env::var_os("FREEMKV_LOCK_CHILD_DIR") else {
        return;
    };
    let out = Path::new(&dir).join("Movie.iso");
    let mode = std::env::var("FREEMKV_LOCK_CHILD_MODE").unwrap_or_default();
    let say = |m: &str| {
        println!("CHILD:{m}");
        io::stdout().flush().unwrap();
    };
    let mut lines = io::stdin().lock().lines();
    let lock = take(&out, WINDOW).expect("child takes the lock");
    say("held");
    if mode == "replace" {
        lines.next();
        // Unix: holding the old inode keeps its number from the new file. Not on Windows,
        // where an open handle leaves the deleted name pending and the retake waits.
        #[cfg(unix)]
        let _old = os::open_rw(&lock_path(&out)).unwrap();
        lock.delete().unwrap();
        let _new = take(&out, WINDOW).expect("child retakes");
        say("replaced");
        lines.next();
    } else {
        lines.next();
        drop(lock);
    }
}

// A target runner (emulator, remote) may not run this binary as a local child.
fn cross_process() -> bool {
    let runner = std::env::vars_os().find(|(k, _)| {
        let k = k.to_string_lossy();
        k.starts_with("CARGO_TARGET_") && k.ends_with("_RUNNER")
    });
    if let Some((k, _)) = &runner {
        eprintln!(
            "SKIP cross-process lock test: {} is set",
            k.to_string_lossy()
        );
    }
    runner.is_none()
}

struct Child {
    proc: std::process::Child,
    lines: std::sync::mpsc::Receiver<String>,
}

impl Child {
    fn spawn(dir: &Path, mode: &str) -> Child {
        use std::io::BufRead;
        let mut proc = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "io::artifact_lock::tests::child_helper",
                CHILD_MARKER,
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FREEMKV_LOCK_CHILD_DIR", dir)
            .env("FREEMKV_LOCK_CHILD_MODE", mode)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let out = std::io::BufReader::new(proc.stdout.take().unwrap());
        let (tx, lines) = std::sync::mpsc::channel();
        thread::spawn(move || {
            for line in out.lines().map_while(io::Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Child { proc, lines }
    }
    fn wait_for(&mut self, marker: &str) {
        let want = format!("CHILD:{marker}");
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) if line.trim_end().ends_with(&want) => return,
                Ok(_) => {}
                Err(e) => panic!("child never printed {want}: {e}"),
            }
        }
    }
    fn send(&mut self) {
        let stdin = self.proc.stdin.as_mut().unwrap();
        stdin.write_all(b"go\n").unwrap();
        stdin.flush().unwrap();
    }
    fn finish(mut self) {
        self.send();
        assert!(self.proc.wait().unwrap().success());
    }
}

// A failed test must not leave a child holding the lock (or blocked on stdin).
impl Drop for Child {
    fn drop(&mut self) {
        if !matches!(self.proc.try_wait(), Ok(Some(_))) {
            let _ = self.proc.kill();
            let _ = self.proc.wait();
        }
    }
}

/// The OS lock excludes another process: with a child holding the sidecar, the parent's
/// acquire waits out its window, and takes the lock once the child has exited.
#[test]
fn a_lock_held_by_another_process_excludes_this_one() {
    if !cross_process() {
        return;
    }
    let (dir, out) = artifact();
    let mut child = Child::spawn(dir.path(), "hold");
    child.wait_for("held");
    let t = Instant::now();
    let r = take(&out, Duration::from_millis(300));
    assert!(
        matches!(r, Err(Error::TimedOut { .. })),
        "taken while another process holds it: {r:?}"
    );
    assert!(t.elapsed() >= Duration::from_millis(300));
    child.finish();
    take(&out, WINDOW).expect("free once the holder exited");
}

// A waiter blocked on the old file must, once it is unlocked, see it no longer names
// the path and wait for the new holder; `ids` is the identity check under test.
type IdFn = fn(&File, &Path) -> (io::Result<(u64, u64)>, io::Result<(u64, u64)>);

fn waiter_must_not_keep_a_replaced_sidecar(ids: IdFn) {
    waiter_must_not_keep_a_replaced_sidecar_with(ids, &os::names_this_file);
}

type SyncNames<'a> = dyn Fn(&File, &Path, &Lock<'_>) -> io::Result<Proof> + Sync + 'a;

fn waiter_must_not_keep_a_replaced_sidecar_with(ids: IdFn, names: &SyncNames<'_>) {
    if !cross_process() {
        return;
    }
    let (dir, out) = artifact();
    let mut child = Child::spawn(dir.path(), "replace");
    child.wait_for("held");
    let reached = std::sync::atomic::AtomicUsize::new(0);
    let counted = |f: &File, p: &Path| {
        reached.fetch_add(1, Ordering::SeqCst);
        ids(f, p)
    };
    let done = AtomicBool::new(false);
    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let hooks = Hooks {
                ids: &counted,
                lock: &os::try_lock_exclusive,
                names,
            };
            let lock = ArtifactLock::acquire_full(
                &out,
                &[],
                &Halt::new(),
                Duration::from_secs(20),
                &hooks,
            )
            .expect("waiter takes the lock after the child exits");
            done.store(true, Ordering::SeqCst);
            lock
        });
        thread::sleep(Duration::from_millis(300));
        child.send();
        child.wait_for("replaced");
        // The old file is unlocked now, so the waiter reaches the identity check.
        let t = Instant::now();
        while reached.load(Ordering::SeqCst) == 0 {
            assert!(
                t.elapsed() < SLACK * 5,
                "the waiter never reached the check"
            );
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(500));
        assert!(
            !done.load(Ordering::SeqCst),
            "the waiter holds the deleted sidecar while the child holds the new one"
        );
        child.finish();
        drop(waiter.join().unwrap());
    });
}

/// §2.5 acquire loop (the unlink race), real inode numbers.
#[test]
fn artifact_lock_rechecks_identity_after_the_holder_deletes() {
    waiter_must_not_keep_a_replaced_sidecar(|f, p| (os::file_id(f), os::path_id(p)));
}

#[test]
#[cfg(unix)]
fn name_derived_ids_cannot_admit_a_waiter_holding_the_deleted_sidecar() {
    waiter_must_not_keep_a_replaced_sidecar(name_derived);
}

// SMB/CIFS mandatory byte-range locks: a read by path is refused while any fd holds
// the exclusive lock, ours or another process's.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_refused_while_locked(p: &Path, max: u64) -> io::Result<Vec<u8>> {
    let probe = File::open(p)?;
    if !os::try_lock_exclusive(&probe)? {
        return Err(io::Error::from_raw_os_error(libc::EACCES));
    }
    drop(probe);
    real_read(p, max)
}

/// Review B1: a waiter holding the deleted sidecar (name-derived ids) whose read-back
/// is refused by the new holder's lock must not take that refusal as proof.
#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn a_read_back_refused_by_another_holders_lock_is_not_proof() {
    let names = |f: &File, p: &Path, relock: &Lock<'_>| {
        os::names_with(f, p, &real_write, &read_refused_while_locked, relock)
    };
    waiter_must_not_keep_a_replaced_sidecar_with(name_derived, &names);
}

// Token proof unavailable: the acquire falls back to the dev/ino match already made.
#[cfg(unix)]
fn acquire_with_token_io(
    write: &dyn Fn(&File, &[u8]) -> io::Result<()>,
    read: &dyn Fn(&Path, u64) -> io::Result<Vec<u8>>,
) -> (Result<ArtifactLock>, usize) {
    let (_dir, out) = artifact();
    acquire_at_with_token_io(&out, write, read)
}

#[cfg(unix)]
fn acquire_at_with_token_io(
    out: &Path,
    write: &dyn Fn(&File, &[u8]) -> io::Result<()>,
    read: &dyn Fn(&Path, u64) -> io::Result<Vec<u8>>,
) -> (Result<ArtifactLock>, usize) {
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let names = |f: &File, p: &Path, relock: &Lock<'_>| {
        calls.fetch_add(1, Ordering::SeqCst);
        os::names_with(f, p, write, read, relock)
    };
    let hooks = Hooks {
        ids: &|f, p| (os::file_id(f), os::path_id(p)),
        lock: &os::try_lock_exclusive,
        names: &names,
    };
    let r = ArtifactLock::acquire_full(out, &[], &Halt::new(), WINDOW, &hooks);
    (r, calls.load(Ordering::SeqCst))
}

#[cfg(unix)]
fn real_write(f: &File, token: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.set_len(0)?;
    f.write_all_at(token, 0)
}

#[cfg(unix)]
fn real_read(p: &Path, max: u64) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let mut v = Vec::new();
    File::open(p)?.take(max).read_to_end(&mut v)?;
    Ok(v)
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn a_token_write_refused_for_space_falls_back_to_the_inode_match() {
    for code in [libc::ENOSPC, libc::EDQUOT, libc::EFBIG] {
        let write = |_: &File, _: &[u8]| Err(io::Error::from_raw_os_error(code));
        let (r, calls) = acquire_with_token_io(&write, &real_read);
        assert!(r.is_ok(), "errno {code}: {r:?}");
        assert_eq!(calls, 1, "errno {code}: no retry");
    }
}

/// Review S1: a stale handle to a replaced sidecar fails the write with these; they
/// leave the identity unproven, so the acquire retries instead of admitting.
#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn a_token_write_failing_like_a_stale_handle_retries() {
    let stale = [
        libc::EBADF,
        libc::EACCES,
        libc::EPERM,
        libc::ENOTCONN,
        libc::ETIMEDOUT,
        libc::EIO,
    ];
    for code in stale {
        let n = std::sync::atomic::AtomicUsize::new(0);
        let write = |f: &File, t: &[u8]| {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::from_raw_os_error(code))
            } else {
                real_write(f, t)
            }
        };
        let (r, calls) = acquire_with_token_io(&write, &real_read);
        assert!(r.is_ok(), "errno {code}: {r:?}");
        assert_eq!(calls, 2, "errno {code}: one retry");
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn any_other_token_write_error_fails_the_acquire() {
    let write = |_: &File, _: &[u8]| Err(io::Error::from_raw_os_error(libc::EINVAL));
    let (r, calls) = acquire_with_token_io(&write, &real_read);
    assert!(
        matches!(&r, Err(Error::IoError { source }) if source.raw_os_error() == Some(libc::EINVAL)),
        "{r:?}"
    );
    assert_eq!(calls, 1);
}

/// CIFS refuses the read-back under our own lock: released, the path reads our token,
/// and the relock admits us on the first attempt, holding the lock.
#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn a_read_back_refused_only_by_our_own_lock_is_proved_unlocked() {
    let (_dir, out) = artifact();
    let (r, calls) = acquire_at_with_token_io(&out, &real_write, &read_refused_while_locked);
    let lock = r.expect("admitted");
    assert_eq!(calls, 1, "no retry");
    let other = os::open_rw(lock.path()).unwrap();
    assert!(!os::try_lock_exclusive(&other).unwrap(), "the lock is held");
}

// Our fd locked on a deleted sidecar; the recreated one at the path, locked by `other`.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn orphan_and_new_holder(path: &Path) -> (File, File) {
    let orphan = os::open_rw(path).unwrap();
    assert!(os::try_lock_exclusive(&orphan).unwrap());
    std::fs::remove_file(path).unwrap();
    let other = os::open_rw(path).unwrap();
    assert!(os::try_lock_exclusive(&other).unwrap());
    (orphan, other)
}

/// Review B1, in one process: a refusal that persists with our lock released is the
/// other holder's, so it is not proof; and every refusal code takes that path.
#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn a_read_back_refused_by_another_holder_retries() {
    for code in [libc::EACCES, libc::EAGAIN, libc::EBUSY, libc::EPERM] {
        let (_dir, out) = artifact();
        let path = lock_path(&out);
        let (orphan, _other) = orphan_and_new_holder(&path);
        let read = |p: &Path, max: u64| {
            read_refused_while_locked(p, max).map_err(|e| match e.raw_os_error() {
                Some(libc::EACCES) => io::Error::from_raw_os_error(code),
                _ => e,
            })
        };
        let r = os::names_with(&orphan, &path, &real_write, &read, &os::try_lock_exclusive);
        assert!(matches!(r, Ok(Proof::Retry(_))), "errno {code}: {r:?}");
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn the_unlocked_proof_retries_unless_relock_and_token_both_hold() {
    let (_dir, out) = artifact();
    let path = lock_path(&out);
    type Relock = Box<dyn Fn(&File) -> io::Result<bool>>;
    let foreign = path.clone();
    let cases: [(&str, Relock); 3] = [
        ("relock refused", Box::new(|_| Ok(false))),
        (
            "relock error",
            Box::new(|_| Err(io::Error::from_raw_os_error(libc::ENOLCK))),
        ),
        (
            "another acquirer wrote the file while ours was released",
            Box::new(move |f| {
                std::fs::write(&foreign, b"another:token\n").unwrap();
                os::try_lock_exclusive(f)
            }),
        ),
    ];
    for (case, relock) in cases {
        let f = os::open_rw(&path).unwrap();
        assert!(os::try_lock_exclusive(&f).unwrap());
        let r = os::names_with(&f, &path, &real_write, &read_refused_while_locked, &*relock);
        assert!(matches!(r, Ok(Proof::Retry(_))), "{case}: {r:?}");
    }
    let mismatch = std::cell::Cell::new(0);
    let read = |p: &Path, max: u64| {
        mismatch.set(mismatch.get() + 1);
        match mismatch.get() {
            1 => Err(io::Error::from_raw_os_error(libc::EACCES)),
            _ => real_read(p, max).map(|_| b"someone:else\n".to_vec()),
        }
    };
    let f = os::open_rw(&path).unwrap();
    assert!(os::try_lock_exclusive(&f).unwrap());
    let r = os::names_with(&f, &path, &real_write, &read, &os::try_lock_exclusive);
    assert!(matches!(r, Ok(Proof::Retry(_))), "token mismatch: {r:?}");
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn enoent_and_estale_on_the_token_io_still_retry() {
    for code in [libc::ENOENT, libc::ESTALE] {
        let n = std::sync::atomic::AtomicUsize::new(0);
        let write = |f: &File, t: &[u8]| {
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::from_raw_os_error(code))
            } else {
                real_write(f, t)
            }
        };
        let (r, calls) = acquire_with_token_io(&write, &real_read);
        assert!(r.is_ok(), "write errno {code}: {r:?}");
        assert_eq!(calls, 2, "write errno {code}: one retry");
        let m = std::sync::atomic::AtomicUsize::new(0);
        let read = |p: &Path, max: u64| {
            if m.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::from_raw_os_error(code))
            } else {
                real_read(p, max)
            }
        };
        let (r, calls) = acquire_with_token_io(&real_write, &read);
        assert!(r.is_ok(), "read errno {code}: {r:?}");
        assert_eq!(calls, 2, "read errno {code}: one retry");
    }
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
        for i in 0..16u8 {
            thread::sleep(WINDOW / 5);
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
    let r = ArtifactLock::acquire_ids(
        &out,
        &[],
        &Halt::new(),
        WINDOW,
        &ids,
        &os::try_lock_exclusive,
    );
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
    let lock = ArtifactLock::acquire_ids(
        &out,
        &[],
        &Halt::new(),
        WINDOW,
        &ids,
        &os::try_lock_exclusive,
    );
    assert!(lock.is_ok(), "{lock:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one retry, then held");
}

// Probed live: on macOS smbfs a second fd in the same process takes flock(LOCK_EX)
// while the first holds it. One process must still admit one holder.
#[test]
fn an_os_lock_that_never_excludes_still_admits_one_holder_per_process() {
    let (_dir, out) = artifact();
    let real_ids = |f: &File, p: &Path| (os::file_id(f), os::path_id(p));
    let take = |window| {
        ArtifactLock::acquire_ids(&out, &[], &Halt::new(), window, &real_ids, &|_| Ok(true))
    };
    let holder = take(WINDOW).expect("first taker");
    let second = take(Duration::from_millis(100));
    assert!(
        matches!(second, Err(Error::TimedOut { .. })),
        "two holders in one process: {second:?}"
    );
    holder.delete().unwrap();
    take(WINDOW).expect("free again after delete");
}

// LIVE: FREEMKV_LIVE_SMB_DIR=/Volumes/media cargo test --lib live_smb_artifact_lock
// -- --ignored --nocapture. Uses a scratch directory it always removes.
#[test]
#[cfg(unix)]
#[ignore = "needs FREEMKV_LIVE_SMB_DIR on a network share"]
fn live_smb_artifact_lock_recreated_sidecar_has_one_holder() {
    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let removed = std::fs::remove_dir_all(&self.0);
            eprintln!("live: removed {} -> {removed:?}", self.0.display());
        }
    }
    let Some(share) = std::env::var_os("FREEMKV_LIVE_SMB_DIR") else {
        return;
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let scratch = Scratch(Path::new(&share).join(format!(".fmkv-lock-{nonce:x}")));
    std::fs::create_dir(&scratch.0).unwrap();
    let out = scratch.0.join("Movie.mkv");
    let holder = take(&out, WINDOW).expect("first taker");
    let (out2, got) = (out.clone(), std::sync::Arc::new(AtomicBool::new(false)));
    let flag = got.clone();
    let waiter = thread::spawn(move || {
        let lock = take(&out2, Duration::from_secs(30)).expect("second taker");
        flag.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_secs(5));
        lock
    });
    thread::sleep(Duration::from_secs(2));
    holder.delete().expect("deleted while held");
    let t = Instant::now();
    while !got.load(Ordering::SeqCst) {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "the waiter never took the lock"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let third = take(&out, Duration::from_millis(500));
    eprintln!("live: third taker while the waiter holds -> {third:?}");
    assert!(matches!(third, Err(Error::TimedOut { .. })), "{third:?}");
    waiter.join().unwrap().delete().unwrap();
}

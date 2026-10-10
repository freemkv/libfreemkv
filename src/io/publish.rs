//! Atomic no-replace publication. Never downgrade to an overwriting rename.
//!
//! Ladder: native no-replace rename; on a capability error, Unix tries
//! link()+unlink (atomic on Linux NFS/CIFS), then an O_EXCL claim that is
//! closed, re-checked through a fresh O_NOFOLLOW open as our zero-length file,
//! removed, and replaced by `rename` (smbfs, FAT, FUSE). Windows stops at native.
//!
//! Residual window of the claim step: a file a foreign actor creates between
//! the re-check and the rename is deleted (macOS smbfs deletes the target by
//! name, then renames without replace) or overwritten (atomic replacing rename
//! elsewhere). On name-derived inode filesystems the re-check proves only "a
//! zero-length file at this name". Cooperating writers hold `<final>.lock`
//! first. A crash mid-claim leaves a zero-length final name; later publishes
//! report AlreadyExists. Callers journaling identities across this step must pin
//! mtime before close and rebind within the publishing call.
use std::{fmt, io, path::Path};

/// Publish a completed file only if `target` is absent (including symlinks).
/// No error triggers a destructive fallback. Network errors can be ambiguous:
/// reconcile identities before retrying. Directory durability is the caller's job.
/// A successful hard-link publication may retain the source if unlink fails;
/// callers must tolerate that recoverable duplicate. Retryable contention in
/// the Unix claim fallback is `ResourceBusy` carrying a [`Fault`].
pub fn no_replace(source: &Path, target: &Path) -> io::Result<()> {
    publish_with(source, target, &System)
}

/// Why the Unix claim fallback refused to finish a publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// Our zero-length claim vanished or changed before it could be released.
    ClaimChanged,
    /// Another file took the target between releasing the claim and renaming.
    TargetRaced,
    /// The renamed target is not a regular file of the source's size.
    SizeMismatch,
}

impl Fault {
    /// The fault carried by an error returned from [`no_replace`], if any.
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref::<Self>().copied()
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ClaimChanged => "publish_claim_changed",
            Self::TargetRaced => "publish_target_raced",
            Self::SizeMismatch => "publish_size_mismatch",
        })
    }
}

impl std::error::Error for Fault {}

// Filesystem steps, replaceable in tests to inject capability errors and races.
trait Steps {
    fn native(&self, source: &Path, target: &Path) -> io::Result<()>;
    #[cfg(unix)]
    fn link(&self, source: &Path, target: &Path) -> io::Result<()> {
        std::fs::hard_link(source, target)
    }
    #[cfg(unix)]
    fn claimed(&self, _target: &Path) {}
    #[cfg(unix)]
    fn released(&self, _target: &Path) {}
    #[cfg(unix)]
    fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        std::fs::rename(source, target)
    }
}

struct System;

impl Steps for System {
    fn native(&self, source: &Path, target: &Path) -> io::Result<()> {
        native(source, target)
    }
}

fn publish_with(source: &Path, target: &Path, steps: &impl Steps) -> io::Result<()> {
    let unsupported = match steps.native(source, target).map_err(normalize) {
        Err(e) if e.kind() == io::ErrorKind::Unsupported => e,
        other => return other,
    };
    #[cfg(unix)]
    return fallback(source, target, steps, unsupported);
    #[cfg(not(unix))]
    Err(unsupported)
}

// Native no-replace rename is unsupported: try an atomic hard link, then the claim.
#[cfg(unix)]
fn fallback(source: &Path, target: &Path, steps: &impl Steps, _: io::Error) -> io::Result<()> {
    match steps.link(source, target) {
        Ok(()) => {
            // Published; an unlink failure leaves the documented duplicate.
            let _ = std::fs::remove_file(source);
            Ok(())
        }
        Err(e) if link_unsupported(&e) => claim_then_rename(source, target, steps),
        Err(e) => Err(normalize(e)),
    }
}

// Linux reports EPERM for filesystems without hard links; trusted only here,
// after native publication already reported a capability error.
#[cfg(unix)]
fn link_unsupported(e: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    if e.raw_os_error() == Some(libc::EPERM) {
        return true;
    }
    e.kind() == io::ErrorKind::Unsupported
        || e.raw_os_error().is_some_and(|code| {
            code == libc::ENOTSUP || code == libc::EOPNOTSUPP || code == libc::ENOSYS
        })
}

// Exclusive-create a claim, release it only after a fresh open proves it is
// still our zero-length file, then rename (module docs: residual window).
#[cfg(unix)]
fn claim_then_rename(source: &Path, target: &Path, steps: &impl Steps) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let fresh = |path: &Path| {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .and_then(|file| file.metadata())
    };
    let size = std::fs::File::open(source)?.metadata()?.len();
    drop(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(target)
            .map_err(normalize)?,
    );
    steps.claimed(target);
    // Every handle is closed before removal: an open file would be silly-renamed.
    if !fresh(target).is_ok_and(|claim| claim.is_file() && claim.len() == 0) {
        return Err(busy(Fault::ClaimChanged));
    }
    match std::fs::remove_file(target) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(busy(Fault::ClaimChanged)),
        Err(e) => return Err(normalize(e)),
    }
    steps.released(target);
    match steps.rename(source, target) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            return Err(busy(Fault::TargetRaced));
        }
        Err(e) => return Err(normalize(e)),
    }
    let published = fresh(target)?;
    if !published.is_file() || published.len() != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            Fault::SizeMismatch,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn busy(fault: Fault) -> io::Error {
    io::Error::new(io::ErrorKind::ResourceBusy, fault)
}

fn normalize(error: io::Error) -> io::Error {
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(1 | 50)) {
        return io::Error::new(io::ErrorKind::Unsupported, error);
    }
    #[cfg(unix)]
    if error.raw_os_error().is_some_and(|code| {
        code == libc::ENOTSUP || code == libc::EOPNOTSUPP || code == libc::ENOSYS
    }) {
        return io::Error::new(io::ErrorKind::Unsupported, error);
    }
    error
}

// renameat2 reports EINVAL when the filesystem lacks RENAME_NOREPLACE.
#[cfg(any(target_os = "linux", all(test, unix)))]
fn renameat2_error(err: io::Error) -> io::Error {
    if err.raw_os_error() == Some(libc::EINVAL) {
        return io::Error::new(io::ErrorKind::Unsupported, err);
    }
    err
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn native(source: &Path, target: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // SAFETY: valid NUL-terminated paths, and the flags enforce no replacement.
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renamex_np(source.as_ptr(), target.as_ptr(), libc::RENAME_EXCL) };
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        #[cfg(target_os = "linux")]
        return Err(renameat2_error(io::Error::last_os_error()));
        #[cfg(not(target_os = "linux"))]
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn native(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(source: *const u16, target: *const u16, flags: u32) -> i32;
    }
    let wide = |path: &Path| -> io::Result<Vec<u16>> {
        let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
        if value.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains NUL",
            ));
        }
        value.push(0);
        Ok(value)
    };
    let source = wide(source)?;
    let target = wide(target)?;
    // SAFETY: live NUL-terminated UTF-16 paths. Zero flags forbid both replacing
    // an existing destination and implicit cross-volume copy/delete.
    if unsafe { MoveFileExW(source.as_ptr(), target.as_ptr(), 0) } != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn native(source: &Path, target: &Path) -> io::Result<()> {
    std::fs::hard_link(source, target)?;
    // Publication already succeeded; an unlink failure must not invite a retry
    // that mistakes the published target for an unrelated collision.
    let _ = std::fs::remove_file(source);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(windows)]
    fn windows_collision_codes_remain_already_exists() {
        for code in [80, 183] {
            let error = normalize(io::Error::from_raw_os_error(code));
            assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(error.raw_os_error(), Some(code));
        }
        assert_eq!(
            normalize(io::Error::from_raw_os_error(17)).kind(),
            io::ErrorKind::CrossesDevices
        );
    }

    #[test]
    fn existing_file_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"new").unwrap();
        std::fs::write(&dst, b"old").unwrap();
        assert_eq!(
            no_replace(&src, &dst).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(src).unwrap(), b"new");
        assert_eq!(std::fs::read(dst).unwrap(), b"old");
    }

    #[test]
    #[cfg(unix)]
    fn dangling_symlink_is_a_collision() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"new").unwrap();
        std::os::unix::fs::symlink("missing", &dst).unwrap();
        assert_eq!(
            no_replace(&src, &dst).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(src.exists());
        assert_eq!(std::fs::read_link(dst).unwrap(), Path::new("missing"));
    }

    #[test]
    fn concurrent_publishers_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("dst");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers: Vec<_> = (0..2)
            .map(|n| {
                let src = dir.path().join(format!("src{n}"));
                std::fs::write(&src, [n]).unwrap();
                let dst = dst.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let result = no_replace(&src, &dst);
                    if let Err(e) = &result {
                        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
                        assert_eq!(std::fs::read(src).unwrap(), [n]);
                    }
                    (n, result.is_ok())
                })
            })
            .collect();
        let winners: Vec<_> = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|(_, ok)| *ok)
            .collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(std::fs::read(dst).unwrap(), [winners[0].0]);
    }

    #[test]
    #[cfg(unix)]
    fn only_capability_errors_are_normalized() {
        assert_eq!(
            normalize(io::Error::from_raw_os_error(libc::ENOTSUP)).kind(),
            io::ErrorKind::Unsupported
        );
        for code in [libc::EXDEV, libc::EIO, libc::EACCES] {
            assert_eq!(
                normalize(io::Error::from_raw_os_error(code)).raw_os_error(),
                Some(code)
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn renameat2_einval_means_unsupported() {
        let e = renameat2_error(io::Error::from_raw_os_error(libc::EINVAL));
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);
        let e = renameat2_error(io::Error::from_raw_os_error(libc::EEXIST));
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn success_moves_bytes_and_leaves_no_stray() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"payload").unwrap();
        no_replace(&src, &dst).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
        assert!(!src.exists());
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["dst"]);
    }

    #[test]
    fn failed_publication_retains_source() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::write(&src, b"source").unwrap();
        assert!(no_replace(&src, &dir.path().join("missing/target")).is_err());
        assert_eq!(std::fs::read(src).unwrap(), b"source");
    }

    #[test]
    #[cfg(unix)]
    fn cross_device_and_io_errors_never_fall_back_or_remove_source() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::write(&src, b"source").unwrap();
        std::fs::write(&dst, b"winner").unwrap();
        for code in [libc::EXDEV, libc::EIO, libc::EACCES, libc::ENOTSUP] {
            let steps = Fake::native(code);
            let error = publish_with(&src, &dst, &steps).unwrap_err();
            if code == libc::ENOTSUP {
                // Capability errors fall back, and the fallback never replaces.
                assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            } else {
                assert_eq!(error.raw_os_error(), Some(code));
            }
            assert_eq!(std::fs::read(&src).unwrap(), b"source");
            assert_eq!(std::fs::read(&dst).unwrap(), b"winner");
        }
    }

    // Injected steps: capability errors for native/link and hooks between
    // the claim, its release and the final rename.
    #[cfg(unix)]
    type Hook = Box<dyn Fn(&Path)>;
    #[cfg(unix)]
    type Rename = Box<dyn Fn(&Path, &Path) -> io::Result<()>>;

    #[cfg(unix)]
    #[derive(Default)]
    struct Fake {
        native: Option<i32>,
        link: Option<i32>,
        claimed: Option<Hook>,
        released: Option<Hook>,
        rename: Option<Rename>,
    }

    #[cfg(unix)]
    impl Fake {
        fn native(code: i32) -> Self {
            Self {
                native: Some(code),
                ..Self::default()
            }
        }
        fn claim_only() -> Self {
            Self {
                native: Some(libc::ENOTSUP),
                link: Some(libc::ENOTSUP),
                ..Self::default()
            }
        }
    }

    #[cfg(unix)]
    impl Steps for Fake {
        fn native(&self, source: &Path, target: &Path) -> io::Result<()> {
            match self.native {
                Some(code) => Err(io::Error::from_raw_os_error(code)),
                None => native(source, target),
            }
        }
        fn link(&self, source: &Path, target: &Path) -> io::Result<()> {
            match self.link {
                Some(code) => Err(io::Error::from_raw_os_error(code)),
                None => std::fs::hard_link(source, target),
            }
        }
        fn claimed(&self, target: &Path) {
            if let Some(hook) = &self.claimed {
                hook(target);
            }
        }
        fn released(&self, target: &Path) {
            if let Some(hook) = &self.released {
                hook(target);
            }
        }
        fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
            match &self.rename {
                Some(rename) => rename(source, target),
                None => std::fs::rename(source, target),
            }
        }
    }

    #[cfg(unix)]
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[cfg(unix)]
    fn pair(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let src = dir.join("src");
        std::fs::write(&src, b"payload").unwrap();
        (src, dir.join("dst"))
    }

    #[test]
    #[cfg(unix)]
    fn unsupported_native_publishes_by_hard_link_keeping_identity() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let ino = std::fs::metadata(&src).unwrap().ino();
        publish_with(&src, &dst, &Fake::native(libc::ENOTSUP)).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
        assert_eq!(std::fs::metadata(&dst).unwrap().ino(), ino);
        assert_eq!(names(dir.path()), ["dst"]);
    }

    #[test]
    #[cfg(unix)]
    fn link_errors_other_than_capability_are_returned_without_claiming() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let steps = Fake {
            native: Some(libc::ENOTSUP),
            link: Some(libc::EIO),
            ..Fake::default()
        };
        let error = publish_with(&src, &dst, &steps).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(names(dir.path()), ["src"]);
    }

    #[test]
    #[cfg(unix)]
    fn eperm_from_link_is_a_capability_signal_only_on_linux() {
        let eperm = io::Error::from_raw_os_error(libc::EPERM);
        assert_eq!(link_unsupported(&eperm), cfg!(target_os = "linux"));
        assert!(link_unsupported(&io::Error::from_raw_os_error(
            libc::ENOTSUP
        )));
        assert!(!link_unsupported(&io::Error::from_raw_os_error(
            libc::EEXIST
        )));
        assert!(!link_unsupported(&io::Error::from_raw_os_error(
            libc::EACCES
        )));
    }

    #[test]
    #[cfg(unix)]
    fn claim_fallback_publishes_bytes_and_leaves_no_stray() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        publish_with(&src, &dst, &Fake::claim_only()).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
        assert_eq!(names(dir.path()), ["dst"]);
    }

    #[test]
    #[cfg(unix)]
    fn claim_fallback_never_touches_an_existing_target_even_if_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        for existing in [&b"winner"[..], b""] {
            std::fs::write(&dst, existing).unwrap();
            let error = publish_with(&src, &dst, &Fake::claim_only()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(std::fs::read(&dst).unwrap(), existing);
            assert_eq!(std::fs::read(&src).unwrap(), b"payload");
        }
        std::fs::remove_file(&dst).unwrap();
        std::os::unix::fs::symlink("missing", &dst).unwrap();
        let error = publish_with(&src, &dst, &Fake::claim_only()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read_link(&dst).unwrap(), Path::new("missing"));
    }

    #[test]
    #[cfg(unix)]
    fn a_claim_changed_before_release_is_busy_and_never_deleted() {
        type Change = fn(&Path);
        let cases: [(Change, Option<&[u8]>); 4] = [
            (|p| std::fs::write(p, b"foreign").unwrap(), Some(b"foreign")),
            (|p| std::fs::remove_file(p).unwrap(), None),
            (
                |p| {
                    std::fs::remove_file(p).unwrap();
                    std::fs::create_dir(p).unwrap();
                },
                None,
            ),
            (
                |p| {
                    std::fs::remove_file(p).unwrap();
                    std::os::unix::fs::symlink("elsewhere", p).unwrap();
                },
                None,
            ),
        ];
        for (index, (change, kept)) in cases.into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let (src, dst) = pair(dir.path());
            let steps = Fake {
                claimed: Some(Box::new(change)),
                ..Fake::claim_only()
            };
            let error = publish_with(&src, &dst, &steps).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ResourceBusy, "case {index}");
            assert_eq!(Fault::of(&error), Some(Fault::ClaimChanged));
            assert_eq!(std::fs::read(&src).unwrap(), b"payload");
            if let Some(kept) = kept {
                assert_eq!(std::fs::read(&dst).unwrap(), kept);
            }
            match index {
                1 => assert!(std::fs::symlink_metadata(&dst).is_err()),
                2 => assert!(dst.is_dir()),
                3 => assert_eq!(std::fs::read_link(&dst).unwrap(), Path::new("elsewhere")),
                _ => {}
            }
        }
    }

    // Documented residual window: on a local filesystem the final rename is an
    // atomic replacing rename, so a file created after the claim's release is replaced.
    #[test]
    #[cfg(unix)]
    fn a_file_created_after_release_is_the_documented_residual_window() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let steps = Fake {
            released: Some(Box::new(|p| std::fs::write(p, b"late").unwrap())),
            ..Fake::claim_only()
        };
        publish_with(&src, &dst, &steps).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
    }

    #[test]
    #[cfg(unix)]
    fn rename_eexist_is_busy_and_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let steps = Fake {
            released: Some(Box::new(|p| std::fs::write(p, b"racer").unwrap())),
            rename: Some(Box::new(|_, _| {
                Err(io::Error::from_raw_os_error(libc::EEXIST))
            })),
            ..Fake::claim_only()
        };
        let error = publish_with(&src, &dst, &steps).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ResourceBusy);
        assert_eq!(Fault::of(&error), Some(Fault::TargetRaced));
        assert_eq!(std::fs::read(&src).unwrap(), b"payload");
        assert_eq!(std::fs::read(&dst).unwrap(), b"racer");
    }

    #[test]
    #[cfg(unix)]
    fn other_rename_errors_are_returned_with_target_absent() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let steps = Fake {
            rename: Some(Box::new(|_, _| {
                Err(io::Error::from_raw_os_error(libc::EIO))
            })),
            ..Fake::claim_only()
        };
        let error = publish_with(&src, &dst, &steps).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(names(dir.path()), ["src"]);
    }

    #[test]
    #[cfg(unix)]
    fn a_published_size_mismatch_is_an_error_that_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = pair(dir.path());
        let steps = Fake {
            rename: Some(Box::new(|s, t| {
                std::fs::rename(s, t)?;
                std::fs::write(t, b"shadowed")
            })),
            ..Fake::claim_only()
        };
        let error = publish_with(&src, &dst, &steps).unwrap_err();
        assert_eq!(Fault::of(&error), Some(Fault::SizeMismatch));
        assert_ne!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&dst).unwrap(), b"shadowed");
    }

    #[test]
    #[cfg(unix)]
    fn locked_claim_publishers_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("dst");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers: Vec<_> = (0..2u8)
            .map(|n| {
                let src = dir.path().join(format!("src{n}"));
                std::fs::write(&src, [n]).unwrap();
                let dst = dst.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let halt = crate::Halt::new();
                    let lock = crate::io::ArtifactLock::acquire(&dst, &[], &halt).unwrap();
                    let result = publish_with(&src, &dst, &Fake::claim_only());
                    drop(lock);
                    if let Err(e) = &result {
                        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
                        assert_eq!(std::fs::read(src).unwrap(), [n]);
                    }
                    (n, result.is_ok())
                })
            })
            .collect();
        let winners: Vec<_> = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .filter(|(_, ok)| *ok)
            .collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(std::fs::read(dst).unwrap(), [winners[0].0]);
    }
}

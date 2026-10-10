//! Atomic no-replace publication. Never downgrade to an overwriting rename.
use std::{io, path::Path};

/// Publish a completed file only if `target` is absent (including symlinks).
/// No error triggers a destructive fallback. Network errors can be ambiguous:
/// reconcile identities before retrying. Directory durability is the caller's job.
/// On hard-link platforms a successful publication may retain the source if
/// unlink fails; callers must tolerate that recoverable duplicate.
pub fn no_replace(source: &Path, target: &Path) -> io::Result<()> {
    publish_with(source, target, native)
}

fn publish_with(
    source: &Path,
    target: &Path,
    operation: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    operation(source, target).map_err(normalize)
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
            let error = publish_with(&src, &dst, |_, _| Err(io::Error::from_raw_os_error(code)))
                .unwrap_err();
            if code == libc::ENOTSUP {
                assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            } else {
                assert_eq!(error.raw_os_error(), Some(code));
            }
            assert_eq!(std::fs::read(&src).unwrap(), b"source");
            assert_eq!(std::fs::read(&dst).unwrap(), b"winner");
        }
    }
}

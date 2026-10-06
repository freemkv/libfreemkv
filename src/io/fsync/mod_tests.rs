use super::*;

// Opens read+write (works on Windows) and syncs an existing file;
// missing path surfaces as `Err` ("not durably synced").
#[test]
fn file_durable_ok_for_existing_err_for_missing() {
    let td = tempfile::tempdir().unwrap();
    let f = td.path().join("data.bin");
    std::fs::write(&f, b"durable").unwrap();
    assert!(
        file_durable(&f).is_ok(),
        "an existing file must open read+write and fsync cleanly"
    );
    assert!(
        file_durable(&td.path().join("absent.bin")).is_err(),
        "a missing file must surface the open failure as Err"
    );
}

/// `dir` is best-effort: it must return normally for a real directory
/// (POSIX fsyncs it, Windows no-ops) and must swallow — never panic on —
/// a missing directory.
#[test]
fn dir_is_best_effort_never_panics() {
    let td = tempfile::tempdir().unwrap();
    dir(td.path());
    dir(&td.path().join("does-not-exist"));
}

#[test]
fn dir_checked_reports_failure() {
    let td = tempfile::tempdir().unwrap();
    assert!(dir_checked(td.path()).is_ok());
    #[cfg(not(windows))]
    assert!(dir_checked(&td.path().join("does-not-exist")).is_err());
}

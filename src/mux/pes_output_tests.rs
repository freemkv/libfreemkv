use super::*;

/// Round-4 (engine): a sink that created its file and then failed to open removes that
/// file, so no empty or truncated output is left at the final name before OutputOpened.
#[test]
fn a_sink_that_fails_to_open_removes_the_file_it_created() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.mp4");
    // mp4 refuses a title without video, after its file is created.
    let r = PesOutput::Mp4(&path).open(&DiscTitle::empty(), None, None);
    assert!(r.is_err());
    assert!(
        std::fs::symlink_metadata(&path).is_err(),
        "created file left"
    );
}

// A file that was there before is never removed, whatever the open did to it.
#[test]
fn a_sink_that_fails_to_open_keeps_a_file_that_was_there() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.mp4");
    std::fs::write(&path, b"earlier").unwrap();
    let r = PesOutput::Mp4(&path).open(&DiscTitle::empty(), None, None);
    assert!(r.is_err());
    assert!(path.is_file());
}

// A sink that opens keeps its file.
#[test]
fn a_sink_that_opens_keeps_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.json");
    PesOutput::Json(&path)
        .open(&DiscTitle::empty(), None, None)
        .unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

/// Round-5 tests LOW: another file renamed in at the path between create and the failure
/// is not the file we created, so it is kept.
#[test]
fn a_failed_open_keeps_another_file_renamed_in() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.mkv");
    let other = dir.path().join("other.mkv");
    let r = created(&path, || {
        std::fs::write(&other, b"theirs").unwrap();
        std::fs::rename(&other, &path).unwrap();
        Err(io::ErrorKind::Other.into())
    });
    assert!(r.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"theirs");
}

/// Round-5 security LOW: a symlink at the path is never taken for our file, even one
/// that resolves to it.
#[cfg(unix)]
#[test]
fn a_failed_open_keeps_a_symlink_to_the_created_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.mkv");
    let moved = dir.path().join("moved.mkv");
    let r = created(&path, || {
        std::fs::rename(&path, &moved).unwrap();
        std::os::unix::fs::symlink(&moved, &path).unwrap();
        Err(io::ErrorKind::Other.into())
    });
    assert!(r.is_err());
    let link = std::fs::symlink_metadata(&path).expect("symlink kept");
    assert!(link.file_type().is_symlink());
}

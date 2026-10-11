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

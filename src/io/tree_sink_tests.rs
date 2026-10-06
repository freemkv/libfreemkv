use super::*;

#[test]
fn a_dir_url_opens_a_tree_sink_and_other_schemes_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("tree");
    let sink = open_tree_sink(&format!("dir://{}", out.display()), false).unwrap();
    assert_eq!(sink.dest(), out.as_path());
    assert!(out.is_dir());
    assert!(open_tree_sink("iso:///tmp/x.iso", false).is_err());
}

#[test]
fn a_published_file_has_its_declared_length_and_no_partial() {
    let dir = tempfile::tempdir().unwrap();
    let sink = TreeSink::create(dir.path(), false).unwrap();
    let mut f = sink.begin(Path::new("a.bin"), 3).unwrap();
    f.write(b"abcdef").unwrap();
    f.finish().unwrap();
    assert_eq!(std::fs::read(dir.path().join("a.bin")).unwrap(), b"abc");
    assert!(!dir.path().join("a.bin.partial").exists());
}

#[cfg(unix)]
#[test]
fn an_unlistable_target_is_refused_not_treated_as_empty() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("tree");
    std::fs::create_dir(&out).unwrap();
    std::fs::write(out.join("old.bin"), b"x").unwrap();
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o300)).unwrap();
    let listable = std::fs::read_dir(&out).is_ok();
    let r = TreeSink::create(&out, false);
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o700)).unwrap();
    if !listable {
        assert!(matches!(r, Err(Error::DirWriteFailed { .. })));
    }
}

#[test]
fn the_aacs_directories_are_not_part_of_the_tree() {
    assert!(!TreeSink::keeps_top_level("AACS", false));
    assert!(!TreeSink::keeps_top_level("certificate", false));
    assert!(!TreeSink::keeps_top_level("X!", true));
    assert!(TreeSink::keeps_top_level("BDMV", false));
}

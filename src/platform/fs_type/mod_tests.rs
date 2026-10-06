use super::*;
use std::os::unix::io::AsRawFd;

#[test]
fn detect_fd_tmp_is_local_or_unknown() {
    // `/tmp` is tmpfs on most distros (which we recognise) but
    // could be ext4 on others. NFS would be unusual.
    let f = tempfile::tempfile().unwrap();
    let r = detect_fd(f.as_raw_fd());
    assert!(
        matches!(r, FsType::Local | FsType::Unknown),
        "expected Local or Unknown for a temp file on Linux, got {r:?}",
    );
}

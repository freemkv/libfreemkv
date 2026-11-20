use super::*;

// The reserve covers the largest transfer the transport issues, so no read outgrows it.
#[test]
fn the_reserve_covers_the_largest_transfer() {
    let td = tempfile::tempdir().unwrap();
    let path = td.path().join("node.bin");
    std::fs::write(&path, [0u8; 16]).unwrap();
    let t = SgIoTransport::open(&path).expect("opens");
    assert_eq!(SG_RESERVED_BYTES as usize, t.max_transfer_bytes());
}

// Only an sg node takes the reserve: anything else refuses it and nothing breaks.
#[test]
fn a_non_sg_fd_refuses_the_reserve() {
    assert!(!reserve_sg_buffer(-1));
    let td = tempfile::tempdir().unwrap();
    let f = std::fs::File::create(td.path().join("plain.bin")).unwrap();
    use std::os::fd::AsRawFd;
    assert!(!reserve_sg_buffer(f.as_raw_fd()));
}

// The reserve is best effort: a node that refuses it still opens.
#[test]
fn open_succeeds_where_the_reserve_is_refused() {
    let td = tempfile::tempdir().unwrap();
    let path = td.path().join("plain.bin");
    std::fs::write(&path, [0u8; 16]).unwrap();
    let t = SgIoTransport::open(&path).expect("opens");
    drop(t);
    let fd = SgIoTransport::reopen_fd(&path);
    assert!(fd >= 0, "a reopen also survives a refused reserve");
    unsafe { libc::close(fd) };
}

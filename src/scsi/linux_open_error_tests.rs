use super::*;

#[test]
fn open_errno_maps_to_its_real_cause() {
    let p = Path::new("/dev/sg9");
    let map = |code| SgIoTransport::map_open_error(&std::io::Error::from_raw_os_error(code), p);
    assert!(matches!(map(libc::EACCES), Error::DevicePermission { .. }));
    assert!(matches!(map(libc::ENOENT), Error::DeviceNotFound { .. }));
    assert!(matches!(map(libc::ENODEV), Error::DeviceNotFound { .. }));
    assert!(matches!(map(libc::EMFILE), Error::IoError { .. }));
    assert!(matches!(map(libc::EBUSY), Error::IoError { .. }));
}

#[test]
fn a_bare_node_name_opens_under_dev() {
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("sg3")),
        Path::new("/dev/sg3")
    );
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("/dev/sg3")),
        Path::new("/dev/sg3")
    );
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("./sg3")),
        Path::new("./sg3")
    );
}

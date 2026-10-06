use super::*;

#[test]
fn normalize_drive_letter() {
    assert_eq!(normalize_path("D:"), "\\\\.\\D:");
    assert_eq!(normalize_path("E:\\"), "\\\\.\\E:");
}

#[test]
fn normalize_already_prefixed() {
    assert_eq!(normalize_path("\\\\.\\D:"), "\\\\.\\D:");
    assert_eq!(normalize_path("\\\\.\\CdRom0"), "\\\\.\\CdRom0");
}

#[test]
fn normalize_cdrom() {
    assert_eq!(normalize_path("CdRom0"), "\\\\.\\CdRom0");
}

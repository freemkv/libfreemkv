//! [`DriveInfo::name`]: the OS name, else the device path. Front ends own the readable line.
use super::*;

fn drive(path: &str, display_name: &str) -> DriveInfo {
    DriveInfo {
        path: path.into(),
        display_name: display_name.into(),
        vendor: "HL-DT-ST".into(),
        model: "BD-RE BU40N".into(),
        firmware: "1.04".into(),
    }
}

#[test]
fn name_is_the_os_name_or_the_path() {
    assert_eq!(drive(r"\\.\CdRom0", "E:").name(), "E:");
    assert_eq!(drive(r"\\.\CdRom0", r"\\.\CdRom0").name(), r"\\.\CdRom0");
    assert_eq!(drive("ioreg:7", "").name(), "ioreg:7");
}

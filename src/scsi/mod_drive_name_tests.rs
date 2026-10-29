//! [`DriveInfo`] naming: the one format every frontend shows (mirrors freemkv-flash's
//! `DriveChoice`): `"{name}  {VENDOR MODEL (rev FW)}"`, plus the path when it differs.
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
fn label_is_vendor_model_and_revision() {
    assert_eq!(
        drive("/dev/sg1", "/dev/sr1").label(),
        "HL-DT-ST BD-RE BU40N (rev 1.04)"
    );
}

#[test]
fn label_replaces_control_characters() {
    let mut d = drive("/dev/sg1", "/dev/sr1");
    d.vendor = "EV\u{1b}IL".into();
    d.firmware = "1\n2".into();
    assert_eq!(d.label(), "EV\u{fffd}IL BD-RE BU40N (rev 1\u{fffd}2)");
}

#[test]
fn name_is_the_os_name_or_the_path() {
    assert_eq!(drive(r"\\.\CdRom0", "E:").name(), "E:");
    assert_eq!(drive(r"\\.\CdRom0", r"\\.\CdRom0").name(), r"\\.\CdRom0");
    assert_eq!(drive("ioreg:7", "").name(), "ioreg:7");
}

#[test]
fn display_has_two_spaces_between_name_and_label() {
    assert_eq!(
        drive(r"\\.\CdRom0", "E:").display(),
        "E:  HL-DT-ST BD-RE BU40N (rev 1.04)"
    );
}

#[test]
fn list_line_adds_the_path_only_when_it_differs_from_the_name() {
    assert_eq!(
        drive(r"\\.\CdRom0", "E:").list_line(),
        r"E:  HL-DT-ST BD-RE BU40N (rev 1.04)  \\.\CdRom0"
    );
    assert_eq!(
        drive("/dev/sr0", "/dev/sr0").list_line(),
        "/dev/sr0  HL-DT-ST BD-RE BU40N (rev 1.04)"
    );
}

#[test]
fn identical_drives_are_told_apart_by_name() {
    assert_ne!(
        drive(r"\\.\CdRom0", "E:").display(),
        drive(r"\\.\CdRom1", "F:").display()
    );
}

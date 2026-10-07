//! Unit tests for Linux optical-device enumeration ordering.

use super::*;

#[test]
fn device_nodes_sort_numerically_not_lexically() {
    let mut names = vec!["sg10", "sg2", "sr0", "sg0"];
    names.sort_by_key(|name| (name.starts_with("sr"), node_number(name)));
    assert_eq!(names, ["sg0", "sg2", "sg10", "sr0"]);
    assert_eq!(node_number("sg"), u32::MAX);
}

#[test]
fn display_name_is_the_sr_block_device_behind_sg() {
    let class = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(class.path().join("sg3/device/block/sr1")).unwrap();
    assert_eq!(display_name(class.path(), "sg3", "/dev/sg3"), "/dev/sr1");
}

#[test]
fn display_name_falls_back_to_the_path() {
    let class = tempfile::tempdir().unwrap();
    // No sysfs entry for the node.
    assert_eq!(display_name(class.path(), "sg3", "/dev/sg3"), "/dev/sg3");
    // An `sr` node is already the name users know.
    std::fs::create_dir_all(class.path().join("sr0/device/block/sr0")).unwrap();
    assert_eq!(display_name(class.path(), "sr0", "/dev/sr0"), "/dev/sr0");
}

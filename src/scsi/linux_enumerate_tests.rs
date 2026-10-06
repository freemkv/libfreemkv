//! Unit tests for Linux optical-device enumeration ordering.

use super::*;

#[test]
fn device_nodes_sort_numerically_not_lexically() {
    let mut names = vec!["sg10", "sg2", "sr0", "sg0"];
    names.sort_by_key(|name| (name.starts_with("sr"), node_number(name)));
    assert_eq!(names, ["sg0", "sg2", "sg10", "sr0"]);
    assert_eq!(node_number("sg"), u32::MAX);
}

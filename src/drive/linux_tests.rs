use super::*;

// Unfiltered sg nodes (sysfs unreadable) must be INQUIRY-checked before a
// Drive::open; sysfs-filtered ones are not probed at all.
#[test]
fn optical_candidates_probes_only_when_sysfs_did_not_filter() {
    let paths = || ["/dev/sg0", "/dev/sg1"].map(String::from).into_iter();
    let mut probed = Vec::new();
    let kept = optical_candidates(paths(), true, |p| {
        probed.push(p.to_string());
        false
    });
    assert_eq!(kept, ["/dev/sg0", "/dev/sg1"]);
    assert!(probed.is_empty(), "type-filtered paths need no INQUIRY");
    let kept = optical_candidates(paths(), false, |p| p == "/dev/sg1");
    assert_eq!(kept, ["/dev/sg1"], "non-optical sg0 dropped");
}

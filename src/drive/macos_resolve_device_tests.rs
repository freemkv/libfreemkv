use super::*;

/// An existing path resolves unchanged as `Direct` — macOS has no
/// `sr`->`sg` substitution, so the returned path must be byte-identical
/// to the input, not some canonicalised/mutated form.
#[test]
fn existing_path_resolves_direct_unchanged() {
    // Use the test binary's own executable path: guaranteed to exist,
    // no fixture file needed.
    let exe = std::env::current_exe().unwrap();
    let path = exe.to_str().unwrap();
    let (resolved, kind) = resolve_device(path).expect("existing path must resolve");
    assert_eq!(resolved, path, "path must be returned unchanged");
    assert_eq!(kind, DeviceResolution::Direct);
}

/// A path that does not exist must error with `DeviceNotFound` carrying
/// the original path, never silently succeed.
#[test]
fn missing_path_is_device_not_found() {
    let path = "/dev/freemkv-definitely-does-not-exist-0xdead";
    match resolve_device(path) {
        Err(Error::DeviceNotFound { path: p }) => assert_eq!(p, path),
        other => panic!("expected DeviceNotFound, got {other:?}"),
    }
}

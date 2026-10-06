use super::*;

// The capture must actually see events and field values, not silently record nothing.
// Mutation: `enabled()` returning false, or `event()` dropping fields, fails here.
#[test]
fn capture_records_target_level_and_fields() {
    let ((), events) = capture(|| {
        tracing::warn!(target: "freemkv::testlog", code = 6017u16, clip = ?"A.EVO", "E6017");
    });
    assert_eq!(events.len(), 1, "exactly one event: {events:?}");
    assert_eq!(events[0].target, "freemkv::testlog");
    assert_eq!(events[0].level, tracing::Level::WARN);
    assert_eq!(events[0].field("code"), Some("6017"));
    assert_eq!(events[0].field("clip"), Some("\"A.EVO\""));
    assert_eq!(events[0].message(), "E6017");
}

/// A field that is absent must read as `None`, not as an empty string — an
/// assertion of the shape `field("code") == Some(..)` has to be able to
/// fail when the site stops logging the code at all.
#[test]
fn missing_field_is_none_and_capture_is_scoped() {
    let ((), events) = capture(|| tracing::warn!(target: "freemkv::testlog", "no fields"));
    assert_eq!(events[0].field("code"), None);
    // Emitted outside `capture`, so it must not appear in a later capture.
    tracing::warn!(target: "freemkv::testlog", code = 1u16, "outside");
    let ((), later) = capture(|| {});
    assert!(
        later.is_empty(),
        "capture is scoped to its closure: {later:?}"
    );
}

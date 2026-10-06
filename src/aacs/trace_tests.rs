use super::*;

/// The trace types are constructible, derive the required traits, and an
/// empty trace round-trips. Pins the structural contract apps build against.
#[test]
fn trace_is_constructible_and_comparable() {
    let t = ResolutionTrace {
        unlock: vec![UnlockStep {
            who: "AACS cert".to_string(),
            outcome: UnlockOutcome::NoUsableHostCert { mkb: Some(68) },
        }],
        keys: vec![KeyStep {
            who: "keydb".to_string(),
            path: vec![
                KeyNode::MatchedDisc,
                KeyNode::FoundVuk,
                KeyNode::DerivedUnitKeys,
            ],
            outcome: KeyOutcome::Resolved,
            matched_entry: None,
            store_entries: None,
        }],
    };
    // Clone + PartialEq (derive contract the renderers rely on).
    assert_eq!(t.clone(), t);
    // `who` is the source's name carried verbatim.
    assert_eq!(t.keys[0].who, "keydb");
    assert_eq!(t.unlock[0].who, "AACS cert");
    // Apps match on this id.
    assert_eq!(BUS_BLOCKED, "bus_blocked");
    // Default / new is empty.
    assert_eq!(ResolutionTrace::new(), ResolutionTrace::default());
    assert!(ResolutionTrace::new().unlock.is_empty());
    assert!(ResolutionTrace::new().keys.is_empty());
}

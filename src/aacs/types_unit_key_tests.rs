use super::*;

// Pinned against the two named constructors: `UnitKey::new` is the ordinary key,
// `UnitKey::forensic` is an index key.
#[test]
fn is_default_index_separates_the_two_constructors() {
    let ordinary = UnitKey::new(0, [0xAA; 16]);
    assert!(
        ordinary.is_default_index(),
        "UnitKey::new builds the ordinary (index-0) key"
    );

    // Every forensic index the spec allows must be reported as NOT default.
    for n in 1u8..=32 {
        let k = UnitKey::forensic(0, [0xAA; 16], n);
        assert!(
            !k.is_default_index(),
            "UnitKey::forensic({n}) is an index key, not the default key"
        );
    }
}

// Must agree with `resolve_disc_index`, the one consumer of `index_number`.
#[test]
fn is_default_index_agrees_with_the_forensic_index_resolver() {
    use crate::aacs::index_select::resolve_disc_index;

    let keys = [
        UnitKey::new(0, [0x11; 16]),
        UnitKey::forensic(1, [0x22; 16], 7),
    ];
    assert_eq!(
        resolve_disc_index(&keys),
        Some(7),
        "sanity: the resolver picks the forensic key's index"
    );

    let non_default: Vec<u8> = keys
        .iter()
        .filter(|k| !k.is_default_index())
        .map(|k| k.index_number)
        .collect();
    assert_eq!(
        non_default,
        vec![7],
        "exactly the key the resolver picked must be non-default"
    );

    // An all-ordinary key set resolves no index, and every key must report
    // itself default.
    let plain = [UnitKey::new(0, [0x11; 16]), UnitKey::new(1, [0x22; 16])];
    assert_eq!(resolve_disc_index(&plain), None);
    assert!(plain.iter().all(|k| k.is_default_index()));
}

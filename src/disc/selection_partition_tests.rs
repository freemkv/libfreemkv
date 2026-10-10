use super::*;

#[test]
fn shared_prefixes_reject_empty_groups_and_self_decomposition() {
    let groups = vec![vec![1, 2], vec![1], vec![2], vec![]];
    assert!(Prefixes::new(&groups).is_none());
    let groups = vec![vec![1, 2], vec![1], vec![2]];
    let prefixes = Prefixes::new(&groups).unwrap();
    assert_eq!(prefixes.decompose(0), Some(vec![1, 2]));
    assert_eq!(prefixes.decompose(1), None);
    assert_eq!(prefixes.decompose(9), None);
}

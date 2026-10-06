use super::*;

fn label(t: StreamLabelType, n: u16, lang: &str, codec: &str) -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: n,
        stream_type: t,
        language: lang.into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: codec.into(),
        variant: String::new(),
    }
}

// Spec: the sort-by-(type, number) pass only runs when the merge actually added something;
// a no-op merge must leave order untouched.
#[test]
fn fill_gaps_leaves_order_untouched_when_nothing_added() {
    // Deliberately out of (type, number) order: number 2 before 1.
    let mut framework = vec![
        label(StreamLabelType::Audio, 2, "fra", "AC-3"),
        label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
    ];
    // MPLS covers exactly the same (type, number) slots -> added == 0.
    let mpls = vec![
        label(StreamLabelType::Audio, 1, "eng", "TrueHD"),
        label(StreamLabelType::Audio, 2, "fra", "AC-3"),
    ];
    merge_mpls_floor(&mut framework, &mpls);
    assert_eq!(
        framework[0].stream_number, 2,
        "no gap-fill happened, so the original (out-of-order) sequence must survive"
    );
    assert_eq!(framework[1].stream_number, 1);
}

use super::*;

#[test]
fn name_words_splits_camel_case_but_not_all_caps() {
    assert_eq!(name_words("MAIN_FEATURE"), ["main", "feature"]);
    assert_eq!(name_words("FEATURE_A"), ["feature", "a"]);
    assert_eq!(name_words("Feature_TRAILER"), ["feature", "trailer"]);
    assert_eq!(name_words("MainFeature_A"), ["main", "feature", "a"]);
    assert_eq!(name_words("FeatureHD"), ["feature", "hd"]);
}

fn dir_entry(name: &str, is_dir: bool, entries: Vec<crate::udf::DirEntry>) -> crate::udf::DirEntry {
    crate::udf::DirEntry {
        name: name.to_string(),
        is_dir,
        meta_lba: 0,
        size: 0,
        entries,
    }
}

// Hang guard: a return to a linear Vec::contains dedup in jar_inventory would make this
// 120k-entry fixture run for minutes.
#[test]
fn jar_inventory_dedup_does_not_hang_on_a_hostile_directory() {
    const FILES: usize = 120_000;
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        // Long shared prefix so every comparison runs to the tail.
        let prefix = "a".repeat(180);
        let children: Vec<crate::udf::DirEntry> = (0..FILES)
            .map(|i| dir_entry(&format!("{prefix}{i:08}.png"), false, Vec::new()))
            .collect();
        let entries = vec![dir_entry("00000", true, children)];
        let _ = tx.send(jar_inventory_from(&entries));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(names) => {
            worker.join().expect("worker panicked");
            assert_eq!(names.len(), FILES);
        }
        Err(_) => panic!(
            "jar_inventory_from did not finish {FILES} entries within 10s \
                 — the dedup is still a linear scan"
        ),
    }
}

/// Behaviour contract: output is deduplicated across subdirectories,
/// sorted, and excludes directories and files sitting directly under
/// `/BDMV/JAR` (only one level down counts).
#[test]
fn jar_inventory_dedups_sorts_and_skips_dirs() {
    let entries = vec![
        dir_entry(
            "00000",
            true,
            vec![
                dir_entry("streamproperties.xml", false, Vec::new()),
                dir_entry("zeta.png", false, Vec::new()),
                dir_entry(
                    "nested",
                    true,
                    vec![dir_entry("hidden.txt", false, Vec::new())],
                ),
            ],
        ),
        dir_entry(
            "00001",
            true,
            vec![
                dir_entry("alpha.png", false, Vec::new()),
                // Duplicate of the entry in 00000 — must appear once.
                dir_entry("streamproperties.xml", false, Vec::new()),
            ],
        ),
        // A jar sitting directly under /BDMV/JAR is not inventoried.
        dir_entry("top.jar", false, Vec::new()),
    ];
    assert_eq!(
        jar_inventory_from(&entries),
        vec![
            "alpha.png".to_string(),
            "streamproperties.xml".to_string(),
            "zeta.png".to_string(),
        ]
    );
}

// Lock the parser roster + order: first matching parse() wins, so reordering changes which
// parser claims a disc. dbp + deluxe MUST stay last.
#[test]
fn parsers_registry_order_locked() {
    let names: Vec<&str> = PARSERS.iter().map(|(n, _, _)| *n).collect();
    assert_eq!(
        names,
        vec![
            "paramount",
            "criterion",
            "pixelogic",
            "ctrm",
            "dbp",
            "deluxe",
            "fox",
            "mpls_universal",
            "png_filenames",
        ],
        "PARSERS array order changed — file-presence/reader-gated High \
             parsers (paramount/criterion/pixelogic/ctrm) stay first; dbp + \
             deluxe (now real com/<vendor>/ prefix detect) and fox (dcx.xml / \
             com/foxbd) stay before mpls_universal; mpls_universal stays the \
             universal Low fallback; png_filenames (Low, language-only hint) \
             stays LAST so MPLS wins the Low tie whenever it produces anything."
    );
}

fn one_label() -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: 1,
        stream_type: StreamLabelType::Audio,
        language: "eng".into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    }
}

fn result(conf: Confidence) -> ParseResult {
    ParseResult {
        labels: vec![one_label()],
        confidence: conf,
        feature_playlist: None,
    }
}

#[test]
fn feature_playlist_hint_matches_on_id_or_filename() {
    let h = FeaturePlaylistHint {
        playlist_id: Some(222),
        filename: Some("00222.mpls".into()),
    };
    assert!(h.matches(222, "99999.mpls"), "numeric id match");
    assert!(
        h.matches(1, "00222.MPLS"),
        "filename match is case-insensitive"
    );
    assert!(
        !h.matches(1, "00001.mpls"),
        "neither id nor filename → no match"
    );

    let id_only = FeaturePlaylistHint {
        playlist_id: Some(5),
        filename: None,
    };
    assert!(id_only.matches(5, "anything.mpls"));
    assert!(!id_only.matches(6, "00005.mpls"));

    assert!(FeaturePlaylistHint::default().is_empty());
    assert!(!h.is_empty());
}

// select_result must pick highest confidence, first-in-array on a tie (regression: old
// analyze() no-op picked the LAST).
#[test]
fn select_result_first_wins_on_tie() {
    // Two parsers, equal (Medium) confidence: the first must win.
    let results = vec![
        ("alpha", result(Confidence::Medium)),
        ("beta", result(Confidence::Medium)),
    ];
    assert_eq!(select_result(&results).map(|(n, _)| *n), Some("alpha"));
}

#[test]
fn select_result_highest_confidence_wins() {
    let results = vec![
        ("low", result(Confidence::Low)),
        ("high", result(Confidence::High)),
        ("medium", result(Confidence::Medium)),
    ];
    assert_eq!(select_result(&results).map(|(n, _)| *n), Some("high"));
}

#[test]
fn select_result_skips_empty_and_handles_none() {
    let empty = ParseResult {
        labels: Vec::new(),
        confidence: Confidence::High,
        feature_playlist: None,
    };
    // High-confidence but empty must be skipped in favour of a
    // non-empty lower-confidence result.
    let results = vec![("empty", empty), ("real", result(Confidence::Low))];
    assert_eq!(select_result(&results).map(|(n, _)| *n), Some("real"));
    // No non-empty results → None.
    let none: Vec<(&'static str, ParseResult)> = Vec::new();
    assert!(select_result(&none).is_none());
}

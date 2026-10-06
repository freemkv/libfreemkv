use super::*;

#[test]
fn title_container_markup_is_not_a_title() {
    let xml = "<disclib><di:discinfo><di:title><di:name/><di:numSets>1</di:numSets></di:title></di:discinfo></disclib>";
    assert_eq!(parse_bdmt_xml(xml), None);
}

#[test]
fn set_number_is_read() {
    let xml = "<d><di:name>X</di:name><di:numSets>3</di:numSets><di:setNumber>2</di:setNumber></d>";
    assert_eq!(parse_bdmt_xml(xml).expect("parse").2, Some((2, 3)));
}

#[test]
fn non_numeric_set_number_falls_back_to_disc_number() {
    let xml = "<d><di:name>X</di:name><di:numSets>3</di:numSets>\
                   <di:setNumber>two</di:setNumber><di:discNumber>2</di:discNumber></d>";
    assert_eq!(parse_bdmt_xml(xml).expect("parse").2, Some((2, 3)));
}

#[test]
fn long_title_and_description_are_capped() {
    let big = "é".repeat(5000);
    let xml = format!("<d><di:name>{big}</di:name><di:description>{big}</di:description></d>");
    let (t, d, _) = parse_bdmt_xml(&xml).expect("parse");
    assert!(t.len() <= MAX_BDMT_TEXT && !t.is_empty());
    assert!(d.expect("desc").len() <= MAX_BDMT_TEXT);
}

// A title or description, CDATA or plain, keeps no control character (a terminal escape
// sequence, a raw newline), as the Blu-ray di:name path does not.
#[test]
fn disc_text_drops_control_characters() {
    let xml = "<d><di:name><![CDATA[Movie\x1b]0;pwned\x07]]></di:name>\
                   <di:description>Line\x1b[2Jone\ntwo</di:description></d>";
    let (t, d, _) = parse_bdmt_xml(xml).expect("parse");
    assert_eq!(t, "Movie]0;pwned");
    assert_eq!(d.as_deref(), Some("Line[2Jonetwo"));
    assert_eq!(display_text(" \x1bA\tB "), "AB");
}

#[test]
fn cap_text_cuts_on_a_char_boundary() {
    let s = format!("aa{}", "\u{4e16}".repeat(400));
    let t = cap_text(s);
    assert!(t.len() <= MAX_BDMT_TEXT && t.chars().all(|c| c == 'a' || c == '\u{4e16}'));
}

#[test]
fn entities_and_cdata_are_decoded() {
    let xml = "<d><di:name>Tom &amp; Jerry &#39;s &#x41;</di:name>\
                   <di:description><![CDATA[a < b & c]]></di:description></d>";
    let (t, d, _) = parse_bdmt_xml(xml).expect("parse");
    assert_eq!(t, "Tom & Jerry 's A");
    assert_eq!(d.as_deref(), Some("a < b & c"));
}

#[test]
fn cdata_title_is_unwrapped() {
    let xml = "<d><di:name><![CDATA[Real]]></di:name></d>";
    assert_eq!(parse_bdmt_xml(xml).expect("parse").0, "Real");
}

#[test]
fn toc_title_name_with_markup_is_rejected() {
    let xml = "<d><tableOfContents><titleName>Real <di:thumbnail/></titleName>\
                   </tableOfContents></d>";
    assert_eq!(parse_bdmt_xml(xml), None);
}

// Warner's non-English bdmt files put "Blu-ray" in di:name; the TOC title is used
// instead, and a file holding only the placeholder yields no title.
#[test]
fn blu_ray_placeholder_is_not_a_title() {
    let with_toc = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:title><di:name>Blu-ray</di:name></di:title>
  <di:description><di:tableOfContents><di:titleName>Inception</di:titleName></di:tableOfContents></di:description>
</di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(with_toc).expect("toc title").0, "Inception");
    let only = r#"<disclib><di:discinfo><di:name>BLU-RAY</di:name></di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(only), None);
    assert!(is_placeholder_title(" Blu-ray "));
    assert!(!is_placeholder_title("Blu-ray Extras"));
}

#[test]
fn extract_simple_title() {
    // Minimal document: <di:name> as the title carrier inside
    // <disclib><di:discinfo>, as every real bdmt file has it.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Aurora Drift</di:name>
</di:discinfo></disclib>"#;
    let (title, desc, set) = parse_bdmt_xml(xml).expect("title should parse");
    assert_eq!(title, "Aurora Drift");
    assert_eq!(desc, None);
    assert_eq!(set, None);
}

#[test]
fn extract_title_element_variant() {
    // <di:title> is the alternate carrier; should be picked up
    // when <di:name> is absent.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:title>Echo Chamber</di:title>
  <di:description>A film about machines.</di:description>
</di:discinfo></disclib>"#;
    let (title, desc, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Echo Chamber");
    assert_eq!(desc.as_deref(), Some("A film about machines."));
}

#[test]
fn extract_title_from_table_of_contents_fallback() {
    // Some authoring tools nest the title under tableOfContents.
    // No <di:name> or <di:title> at top level → fall back to
    // titleName inside tableOfContents.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:tableOfContents>
    <di:titleName>Feelings Two</di:titleName>
  </di:tableOfContents>
</di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Feelings Two");
}

#[test]
fn bdmt_size_gate_rejects_oversized_entries() {
    assert!(bdmt_size_acceptable(0));
    assert!(bdmt_size_acceptable(4096));
    assert!(bdmt_size_acceptable(MAX_BDMT_BYTES));
    assert!(!bdmt_size_acceptable(MAX_BDMT_BYTES + 1));
    // A crafted multi-GB size is rejected before any allocation.
    assert!(!bdmt_size_acceptable(8 * 1024 * 1024 * 1024));
    assert!(!bdmt_size_acceptable(u64::MAX));
}

#[test]
fn disc_set_rejects_nonsensical_pairs() {
    // n > total, zero numerator, zero denominator → all None.
    let over = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>5</di:discNumber>
  <di:numSets>2</di:numSets>
</di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(over).unwrap().2, None);

    let zero_n = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>0</di:discNumber>
  <di:numSets>5</di:numSets>
</di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(zero_n).unwrap().2, None);

    let zero_total = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>1</di:discNumber>
  <di:numSets>0</di:numSets>
</di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(zero_total).unwrap().2, None);

    // A valid pair still passes.
    let ok = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>2</di:discNumber>
  <di:numSets>3</di:numSets>
</di:discinfo></disclib>"#;
    assert_eq!(parse_bdmt_xml(ok).unwrap().2, Some((2, 3)));
}

#[test]
fn extract_box_set_position() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Box Set Disc 2</di:name>
  <di:discNumber>2</di:discNumber>
  <di:numSets>5</di:numSets>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, Some((2, 5)));
}

#[test]
fn extract_box_set_position_alternate_total_tag() {
    // <di:numberOfSets> is an alternate spelling we've seen.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>3</di:discNumber>
  <di:numberOfSets>6</di:numberOfSets>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, Some((3, 6)));
}

#[test]
fn extract_box_set_requires_both_fields() {
    // discNumber alone (no total) yields None — we don't fabricate
    // a denominator.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>X</di:name>
  <di:discNumber>1</di:discNumber>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, None);
}

// A UDF with `/BDMV/META/DL/` holding `files` (name, contents), or no DL dir when `dl` is false.
fn disc_with_dl(files: &[(&str, Vec<u8>)], dl: bool) -> (crate::udf::fixture::MemDisc, UdfFs) {
    use crate::udf::fixture::*;
    let dl_files = files
        .iter()
        .enumerate()
        .map(|(i, (n, c))| file_with(n, 30 + i as u32, 100 + 2_000 * i as u32, c.clone(), true))
        .collect();
    let dir = |name: &str, icb, data, files, subdirs| DirSpec {
        name: name.into(),
        icb_lba: icb,
        dir_data_lba: data,
        files,
        subdirs,
    };
    let leaf = if dl {
        dir("DL", 16, 17, dl_files, vec![])
    } else {
        dir("OTHER", 16, 17, dl_files, vec![])
    };
    let meta = dir("META", 14, 15, vec![], vec![leaf]);
    let bdmv = dir("BDMV", 12, 13, vec![], vec![meta]);
    let root = dir("", 10, 11, vec![], vec![bdmv]);
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

#[test]
fn parse_aggregates_languages_and_first_disc_set_wins() {
    let xml = |name: &str, set: u32| {
        format!(
            "<d><di:name>{name}</di:name><di:setNumber>{set}</di:setNumber>\
                 <di:numSets>2</di:numSets></d>"
        )
        .into_bytes()
    };
    let mut fra = xml("Titre", 2);
    fra.extend_from_slice(b"<di:description>Suite.</di:description>");
    let mut big = xml("Too Big", 1);
    big.resize(MAX_BDMT_BYTES as usize + 1, b' ');
    let (mut disc, udf) = disc_with_dl(
        &[
            ("bdmt_eng.xml", xml("Title", 1)),
            ("bdmt_fra.xml", fra),
            ("bdmt_deu.xml", big),
            ("bdmt_xx.xml", xml("Bad Lang", 1)),
            ("notes.xml", xml("Not BDMT", 1)),
        ],
        true,
    );
    assert!(detect(&udf));
    let meta = parse(&mut disc, &udf).expect("titles found");
    let titles: Vec<_> = meta
        .titles
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(titles, [("eng", "Title"), ("fra", "Titre")]);
    assert_eq!(meta.descriptions.len(), 1);
    assert_eq!(meta.descriptions["fra"], "Suite.");
    assert_eq!(meta.disc_number, Some((1, 2)), "the first file's set wins");
}

#[test]
fn parse_and_detect_are_none_without_bdmt_files() {
    let (mut disc, udf) = disc_with_dl(&[("bdmt_eng.xml", b"<d/>".to_vec())], false);
    assert!(!detect(&udf));
    assert!(parse(&mut disc, &udf).is_none());
    // A DL dir with only a title-less bdmt file: detected, but nothing to parse.
    let (mut disc, udf) = disc_with_dl(&[("bdmt_eng.xml", b"<d/>".to_vec())], true);
    assert!(detect(&udf));
    assert!(parse(&mut disc, &udf).is_none());
}

#[test]
fn multiple_languages_keyed_correctly() {
    // Drive parse_bdmt_xml from two synthetic XML blobs and aggregate
    // into DiscMetadata like parse() would, exercising BTreeMap key
    // handling without needing a UdfFs.
    let eng_xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Aurora Drift</di:name>
</di:discinfo></disclib>"#;
    let fra_xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Aurora Drift (Partie Deux)</di:name>
  <di:description>Suite du film fictif.</di:description>
</di:discinfo></disclib>"#;

    let mut meta = DiscMetadata::default();
    for (lang, blob) in [("eng", eng_xml), ("fra", fra_xml)] {
        let (title, desc, ds) = parse_bdmt_xml(blob).unwrap();
        meta.titles.insert(lang.to_string(), title);
        if let Some(d) = desc {
            meta.descriptions.insert(lang.to_string(), d);
        }
        if meta.disc_number.is_none()
            && let Some(d) = ds
        {
            meta.disc_number = Some(d);
        }
    }

    assert_eq!(
        meta.titles.get("eng").map(String::as_str),
        Some("Aurora Drift")
    );
    assert_eq!(
        meta.titles.get("fra").map(String::as_str),
        Some("Aurora Drift (Partie Deux)")
    );
    assert!(!meta.descriptions.contains_key("eng"));
    assert_eq!(
        meta.descriptions.get("fra").map(String::as_str),
        Some("Suite du film fictif.")
    );
    assert_eq!(meta.disc_number, None);
}

#[test]
fn malformed_xml_returns_none() {
    // Gibberish has no recognizable title element: parse_bdmt_xml returns
    // None and parse() skips the file. Zero files aggregated leaves
    // DiscMetadata::default(), which parse() surfaces as None.
    let bad = "this is not xml &&& <<< nope";
    assert!(parse_bdmt_xml(bad).is_none());

    // Half-open tag, no body, no close: also yields no title.
    let truncated = "<disclib><di:discinfo><di:name>";
    assert!(parse_bdmt_xml(truncated).is_none());
}

#[test]
fn description_with_only_child_xml_is_dropped() {
    // Real-world bug: <di:description> contained only <di:thumbnail/>
    // children with no prose, and the old parser surfaced the raw XML
    // fragment as the description. Now candidates starting with `<` are rejected.
    let xml = r#"<disclib><di:discinfo>
            <di:name>Skyline Run</di:name>
            <di:description>
              <di:thumbnail href="sample_meta_sm.jpg" />
              <di:thumbnail href="sample_meta_lg.jpg" />
            </di:description>
        </di:discinfo></disclib>"#;
    let (title, description, _) =
        parse_bdmt_xml(xml).expect("title is present so parse must succeed");
    assert_eq!(title, "Skyline Run");
    assert!(
        description.is_none(),
        "description containing only XML children must be dropped, got {description:?}"
    );
}

#[test]
fn description_with_plain_text_passes_through() {
    // The legitimate case still works: a description with actual
    // prose survives the looks_like_xml filter.
    let xml = r#"<disclib><di:discinfo>
            <di:name>Some Movie</di:name>
            <di:description>An epic tale of one man's quest for tea.</di:description>
        </di:discinfo></disclib>"#;
    let (_, description, _) = parse_bdmt_xml(xml).expect("must parse");
    assert_eq!(
        description.as_deref(),
        Some("An epic tale of one man's quest for tea.")
    );
}

/// Mixed content: prose LEADING with real text but carrying an embedded
/// child element. `xml::text` returns it verbatim (markup and all), so the
/// old `starts_with('<')` filter let the tags leak into the description.
/// A mixed-content description must be dropped, not surfaced with raw XML.
#[test]
fn mixed_content_description_with_embedded_tag_is_dropped() {
    let xml = r#"<disclib><di:discinfo>
            <di:name>Skyline Run</di:name>
            <di:description>Intro prose <di:thumbnail href="x.jpg" /> more</di:description>
        </di:discinfo></disclib>"#;
    let (title, description, _) = parse_bdmt_xml(xml).expect("title present so parse must succeed");
    assert_eq!(title, "Skyline Run");
    assert!(
        description.is_none(),
        "a description with embedded markup must be dropped, got {description:?}"
    );
}

/// The mixed-content guard must not over-reject: a bare `<` used as prose
/// (a comparison, not a tag) is still a valid description.
#[test]
fn description_with_bare_less_than_is_not_treated_as_xml() {
    let xml = r#"<disclib><di:discinfo>
            <di:name>Math Film</di:name>
            <di:description>when a < b holds</di:description>
        </di:discinfo></disclib>"#;
    let (_, description, _) = parse_bdmt_xml(xml).expect("must parse");
    assert_eq!(description.as_deref(), Some("when a < b holds"));
}

#[test]
fn whitespace_in_title_is_trimmed() {
    let xml = r#"<disclib><di:discinfo><di:name>
            Aurora Drift
        </di:name></di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Aurora Drift");
}

#[test]
fn lang_code_extraction() {
    assert_eq!(lang_code_from_filename("bdmt_eng.xml"), Some("eng".into()));
    assert_eq!(lang_code_from_filename("BDMT_FRA.XML"), Some("fra".into()));
    assert_eq!(lang_code_from_filename("bdmt_jpn.xml"), Some("jpn".into()));
    // Non-matching cases:
    assert_eq!(lang_code_from_filename("bdmt_.xml"), None);
    assert_eq!(lang_code_from_filename("bdmt_engl.xml"), None);
    assert_eq!(lang_code_from_filename("bdmt_e1g.xml"), None);
    assert_eq!(lang_code_from_filename("bdmt_eng.txt"), None);
    assert_eq!(lang_code_from_filename("foo.xml"), None);
}

// ── Additional hardening tests ─────────────────────────────────────────

/// Spec reference: BDA disc-library metadata schema, §3.3.2 — `<di:name>`
/// takes priority over `<di:title>` as the title carrier.
/// Mutation: swap `di:name` to `di:other` → test goes red because title is None.
#[test]
fn di_name_priority_over_di_title() {
    // When BOTH di:name and di:title are present, di:name wins.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Primary Title</di:name>
  <di:title>Fallback Title</di:title>
</di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Primary Title");
}

/// Spec reference: BDA disc-library metadata §3.3.2 — `<di:tableOfContents>`
/// with nested `<di:titleName>` is a vendor-specific variant.
/// Mutation: rename `titleName` → `movieName` → test goes red (None).
#[test]
fn di_name_wins_over_table_of_contents_title_name() {
    // di:name exists — tableOfContents/titleName must NOT override it.
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Winner</di:name>
  <di:tableOfContents>
    <di:titleName>Loser</di:titleName>
  </di:tableOfContents>
</di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Winner");
}

/// Spec reference: BDA §3.3.2 — an empty `<di:name>` element must be
/// treated as absent, falling through to the next candidate.
/// Mutation: change `<di:name></di:name>` to `<di:name>X</di:name>` → red.
#[test]
fn empty_di_name_falls_through_to_di_title() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name></di:name>
  <di:title>Non-Empty Title</di:title>
</di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Non-Empty Title");
}

/// Mutation: remove the `!s.is_empty()` filter → empty descriptions
/// come through as Some("").
#[test]
fn empty_description_element_filtered_out() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Film</di:name>
  <di:description></di:description>
</di:discinfo></disclib>"#;
    let (_, description, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(description, None);
}

/// Disc N of N (e.g. 3 of 3) is valid — not an off-by-one error.
/// Mutation: change `n > total` to `n >= total` → last disc of set is None.
#[test]
fn disc_set_allows_last_disc_equal_total() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Film</di:name>
  <di:discNumber>3</di:discNumber>
  <di:numSets>3</di:numSets>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, Some((3, 3)));
}

/// When `<di:discNumber>` has non-numeric text, disc_number must be None.
/// Mutation: remove the `.parse::<u32>().ok()?` guard → panics or wrong value.
#[test]
fn disc_set_non_numeric_disc_number_yields_none() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Film</di:name>
  <di:discNumber>one</di:discNumber>
  <di:numSets>5</di:numSets>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, None);
}

// Whitespace-only title element must be treated as empty (trimmed → "").
// Mutation: remove the `!s.is_empty()` guard in extract_title →
// whitespace-only di:name would be returned as the title.
#[test]
fn whitespace_only_di_name_falls_through() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>   </di:name>
  <di:title>Real Title</di:title>
</di:discinfo></disclib>"#;
    let (title, _, _) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(title, "Real Title");
}

// is_bdmt_filename must recognize bdmt_<lang>.xml and reject everything
// else — it drives detect's directory scan.
#[test]
fn is_bdmt_filename_matches_convention_only() {
    assert!(is_bdmt_filename("bdmt_eng.xml"));
    assert!(is_bdmt_filename("BDMT_FRA.XML"));
    assert!(!is_bdmt_filename("bdmt_engl.xml"));
    assert!(!is_bdmt_filename("index.bdmv"));
    assert!(!is_bdmt_filename("foo.xml"));
}

// "Disc 1 of 1" is a valid, non-nonsensical pair — total < 1 must
// reject only total == 0, not total == 1.
#[test]
fn disc_set_allows_single_disc_release() {
    let xml = r#"<disclib xmlns="urn:BDA:bdmv;disclib"><di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
  <di:name>Film</di:name>
  <di:discNumber>1</di:discNumber>
  <di:numSets>1</di:numSets>
</di:discinfo></disclib>"#;
    let (_, _, set) = parse_bdmt_xml(xml).unwrap();
    assert_eq!(set, Some((1, 1)));
}

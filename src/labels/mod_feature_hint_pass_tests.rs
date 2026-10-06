use super::*;
use crate::udf::fixture::*;
use std::io::{Cursor, Write as _};

// Zip entries into an in-memory jar (Stored, no compression).
fn build_jar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, data) in entries {
            w.start_file(*name, opts).unwrap();
            w.write_all(data).unwrap();
        }
        w.finish().unwrap();
    }
    buf
}

// /BDMV/PLAYLIST holding 00800.mpls: a menu-walk hint must name a real playlist.
fn playlist_dir() -> DirSpec {
    DirSpec {
        name: "PLAYLIST".to_string(),
        icb_lba: 40,
        dir_data_lba: 41,
        files: vec![file_with("00800.mpls", 42, 4100, vec![0u8; 16], true)],
        subdirs: vec![],
    }
}

/// A jar-only disc — no loose manifest, no vendor labels, no parser matches —
/// still yields a feature hint, because the menu-walk hint pass runs
/// independently of `labels.is_empty()`. This is the whole point of routing
/// the hint through `resolve_feature_hint` rather than dropping it with the
/// (empty) label set. The embedded `playlists.xml` names the feature 00800.
#[test]
fn apply_returns_hint_for_a_jar_only_disc_with_no_labels() {
    let xml = br#"<playlists>
            <playlist name="Feature" id="00800" aud="eng,fra,spa" duration="7000" />
            <playlist name="Preview" id="00050" aud="eng" duration="90" />
        </playlists>"#;
    let jar = build_jar(&[
        ("com/studio/Menu.class", Vec::new()), // an entry, not a parseable class
        ("00000/playlists.xml", xml.to_vec()),
    ]);
    let jar_dir = DirSpec {
        name: "JAR".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("00000.jar", 32, 4000, jar, true)],
        subdirs: vec![],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![jar_dir, playlist_dir()],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    // No titles to label; the hint pass must still fire.
    let mut titles: Vec<DiscTitle> = Vec::new();
    let hint = apply(&mut disc, &udf, &mut titles).expect("a jar-only disc still yields a hint");
    assert_eq!(hint.playlist_id, Some(800));
    assert_eq!(hint.filename.as_deref(), Some("00800.mpls"));
}

/// The winner_hint early-return: when the winning parser already carried a
/// non-empty hint, resolve_feature_hint returns it verbatim. The disc's
/// menu-walk (Tier 1, embedded playlists.xml) would yield 00800, but the
/// supplied winner_hint (00042) must win.
#[test]
fn resolve_feature_hint_returns_winner_hint_over_menu_walk() {
    let xml = br#"<playlists>
            <playlist name="Feature" id="00800" aud="eng,fra,spa" duration="7000" />
        </playlists>"#;
    let jar = build_jar(&[("00000/playlists.xml", xml.to_vec())]);
    let jar_dir = DirSpec {
        name: "JAR".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("00000.jar", 32, 4000, jar, true)],
        subdirs: vec![],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![jar_dir, playlist_dir()],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    // Sanity: the menu-walk (Tier 1) on this disc resolves to 00800.
    let tier2 = bdj_feature::resolve(&mut disc, &udf, &[]);
    assert_eq!(tier2.and_then(|h| h.playlist_id), Some(800));

    // A DIFFERENT non-empty winner_hint must short-circuit before Tier-2.
    let winner = FeaturePlaylistHint {
        playlist_id: Some(42),
        filename: Some("00042.mpls".to_string()),
    };
    let got = resolve_feature_hint(&mut disc, &udf, Some(winner.clone()), &[]);
    assert_eq!(
        got,
        Some(winner),
        "a non-empty winner_hint wins over the menu-walk result"
    );
}

use super::mobj::tests::{build as build_mobj, cmd};
use super::*;
use crate::udf::fixture::{DirSpec, MemDisc, build_udf_skeleton, file_with, lay_dir};

// Encode one 12-byte `index.bdmv` playback object: `object_type` in the top
// two bits of byte 0 (1 = HDMV, `id_ref` big-endian @6; 2 = BD-J). Mirrors
// index.rs's own private test builder (kept local).
fn hdmv_obj(id_ref: u16) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0] = 1 << 6;
    b[6..8].copy_from_slice(&id_ref.to_be_bytes());
    b
}
fn bdj_obj() -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0] = 2 << 6;
    b
}

/// Build a minimal, valid `index.bdmv`: First-Play = HDMV object 0,
/// Top Menu = BD-J, one (unused) title. Layout per `index::parse`.
fn build_index(first_play: [u8; 12]) -> Vec<u8> {
    let indexes_start = 48u32;
    let mut d = vec![0u8; indexes_start as usize];
    d[0..4].copy_from_slice(b"INDX");
    d[4..8].copy_from_slice(b"0300");
    d[8..12].copy_from_slice(&indexes_start.to_be_bytes());
    d.extend_from_slice(&0u32.to_be_bytes()); // index_len (unused by parser)
    d.extend_from_slice(&first_play);
    d.extend_from_slice(&bdj_obj()); // top_menu
    d.extend_from_slice(&1u16.to_be_bytes()); // num_titles
    d.extend_from_slice(&bdj_obj()); // titles[0] (not exercised here)
    d
}

// Smoke/e2e test of the resolver: minimal index.bdmv + MovieObject.bdmv on
// an in-memory UDF disc, resolved via read_file + index::parse +
// mobj::parse + vm::resolve. First-Play HDMV object 0 unconditionally PlayPLs playlist 11.
#[test]
fn resolve_feature_end_to_end_resolves_playlist() {
    // op_cnt=1, grp=BRANCH(0), sub_grp=PLAY(2), branch_opt=PLAY_PL(0), imm dst.
    let play_pl_11 = cmd((1 << 5) | 2, 0x80, 0, 0, 11, 0);
    let mobj_bytes = build_mobj(&[&[play_pl_11]]);
    let index_bytes = build_index(hdmv_obj(0));

    let mut disc = MemDisc::new();
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 12,
        dir_data_lba: 13,
        files: vec![
            file_with("index.bdmv", 14, 500, index_bytes, true),
            file_with("MovieObject.bdmv", 15, 600, mobj_bytes, true),
        ],
        subdirs: vec![],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    assert_eq!(
        resolve_feature(&mut disc, &udf, |id| id == 11),
        Some(11),
        "First-Play's unconditional PlayPL 11 must resolve as the feature"
    );
}

/// A malformed index (`index.bdmv` truncated to just its magic) must make
/// the whole resolver abstain, not panic — proving `resolve_feature`
/// really is wired through `index::parse`'s failure path end-to-end.
#[test]
fn resolve_feature_abstains_on_malformed_index() {
    let play_pl_11 = cmd((1 << 5) | 2, 0x80, 0, 0, 11, 0);
    let mobj_bytes = build_mobj(&[&[play_pl_11]]);

    let mut disc = MemDisc::new();
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 12,
        dir_data_lba: 13,
        files: vec![
            file_with("index.bdmv", 14, 500, b"INDX".to_vec(), true),
            file_with("MovieObject.bdmv", 15, 600, mobj_bytes, true),
        ],
        subdirs: vec![],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    assert_eq!(resolve_feature(&mut disc, &udf, |id| id == 11), None);
}

#[test]
fn unconditional_roster_reports_all_immediate_playlists() {
    let first = cmd((1 << 5) | 2, 0x80, 0, 0, 101, 0);
    let second = cmd((1 << 5) | 2, 0x80, 0, 0, 102, 0);
    let mobj_bytes = build_mobj(&[&[first, second]]);
    let index_bytes = build_index(hdmv_obj(0));
    let mut disc = MemDisc::new();
    let bdmv = DirSpec {
        name: "BDMV".into(),
        icb_lba: 12,
        dir_data_lba: 13,
        files: vec![
            file_with("index.bdmv", 14, 500, index_bytes, true),
            file_with("MovieObject.bdmv", 15, 600, mobj_bytes, true),
        ],
        subdirs: vec![],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(
        resolve_unconditional_roster(&mut disc, &udf),
        Some(vec![101, 102])
    );
}

#[test]
fn unconditional_roster_rejects_conditional_commands() {
    let compare = cmd((2 << 5) | 1, 0x80, 0, 0, 0, 0);
    let mobj_bytes = build_mobj(&[&[compare]]);
    let index_bytes = build_index(hdmv_obj(0));
    let mut disc = MemDisc::new();
    let bdmv = DirSpec {
        name: "BDMV".into(),
        icb_lba: 12,
        dir_data_lba: 13,
        files: vec![
            file_with("index.bdmv", 14, 500, index_bytes, true),
            file_with("MovieObject.bdmv", 15, 600, mobj_bytes, true),
        ],
        subdirs: vec![],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(resolve_unconditional_roster(&mut disc, &udf), None);
}

#[test]
fn unconditional_roster_rejects_partial_playlist_commands() {
    let partial = cmd((1 << 5) | 2, 0x81, 0, 0, 101, 7);
    let bytes = build_mobj(&[&[partial, cmd((1 << 5) | 2, 0x80, 0, 0, 102, 0)]]);
    let index = index::parse(&build_index(hdmv_obj(0))).unwrap();
    let objects = mobj::parse(&bytes).unwrap();
    assert_eq!(vm::resolve_unconditional_roster(&index, &objects), None);
}

#[test]
fn unconditional_roster_rejects_cycles_and_unknown_groups() {
    let loop_cmd = cmd(1 << 5, 0x81, 0, 0, 0, 0);
    let index = index::parse(&build_index(hdmv_obj(0))).unwrap();
    let objects = mobj::parse(&build_mobj(&[&[loop_cmd]])).unwrap();
    assert_eq!(vm::resolve_unconditional_roster(&index, &objects), None);

    let unknown = cmd((1 << 5) | (3 << 3), 0x80, 0, 0, 0, 0);
    let objects = mobj::parse(&build_mobj(&[&[unknown]])).unwrap();
    assert_eq!(vm::resolve_unconditional_roster(&index, &objects), None);
}

#[test]
fn unconditional_roster_rejects_out_of_bounds_jump_after_playback() {
    let first = cmd((1 << 5) | 2, 0x80, 0, 0, 101, 0);
    let second = cmd((1 << 5) | 2, 0x80, 0, 0, 102, 0);
    let index = index::parse(&build_index(hdmv_obj(0))).unwrap();
    for target in [3, 100, u32::MAX] {
        let jump = cmd(1 << 5, 0x81, 0, 0, target, 0);
        let objects = mobj::parse(&build_mobj(&[&[first, second, jump]])).unwrap();
        assert_eq!(vm::resolve_unconditional_roster(&index, &objects), None);
    }
}

#[test]
fn unconditional_roster_rejects_invalid_play_operand_count() {
    let index = index::parse(&build_index(hdmv_obj(0))).unwrap();
    for operands in [0, 2, 3] {
        let first = cmd((operands << 5) | 2, 0xc0, 0, 0, 101, 0);
        let second = cmd((1 << 5) | 2, 0x80, 0, 0, 102, 0);
        let objects = mobj::parse(&build_mobj(&[&[first, second]])).unwrap();
        assert_eq!(vm::resolve_unconditional_roster(&index, &objects), None);
    }
}

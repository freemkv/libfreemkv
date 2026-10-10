use super::*;

fn fixture(functions: &[&[u8]]) -> Vec<u8> {
    let mut bytes = b"QCOF\0\0\0\x05\0\0\0\0\x0d".to_vec();
    bytes.extend_from_slice(&[15, 0x80, 1, 0, 0, 0, 0]);
    bytes.extend_from_slice(&[0x80, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
    let screen = bytes.len() as u32;
    bytes[8..12].copy_from_slice(&screen.to_be_bytes());
    bytes.extend_from_slice(&[1, 0, 1, 0, 0]);
    bytes.extend_from_slice(&[0; 35]);
    bytes.push(0);
    bytes.extend_from_slice(&[0, 2, 0, 0]);
    bytes.extend_from_slice(&(functions.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    let length = functions.len() * 4 + functions.iter().map(|f| f.len()).sum::<usize>();
    bytes.extend_from_slice(&(length as u32).to_be_bytes());
    let mut offset = functions.len() * 4;
    for function in functions {
        bytes.extend_from_slice(&(offset as u32).to_be_bytes());
        offset += function.len();
    }
    for function in functions {
        bytes.extend_from_slice(function);
    }
    bytes
}

fn builder<'a, 'b>(program: &'a qco::Program<'b>) -> Builder<'a, 'b> {
    Builder {
        program,
        frames: vec![],
        entries: BTreeMap::new(),
        sequencer: 310,
        table_frame: 0,
    }
}

#[test]
fn retained_arguments_are_not_inferred_from_drop_count() {
    // Callee reads both arguments while caller retains both until explicit DROP.
    let caller = [0, 0, 0, 1, 7, 1, 8, 2, 0x40, 2, 0x16, 0, 0x3d, 2, 0x3f];
    let callee = [1, 0, 0, 0x20, 2, 0x20, 3, 6, 0x36, 0x3f];
    let bytes = fixture(&[&caller, &callee]);
    let program = qco::parse(&bytes).unwrap();
    let mut proof = builder(&program);
    let root = proof.build(0x4001, 0).unwrap();
    let peaks = frames::peaks(&proof.frames, 7).unwrap();
    assert_eq!(peaks[root], 7);
    assert_eq!(frames::peaks(&proof.frames, 6), Err(Reject::Budget));
    assert_eq!(builder(&program).build(0x4002, 1), Err(Reject::Invalid));
}

#[test]
fn rejects_underflow_recursion_unbalanced_exit_and_unknown_effects() {
    for (code, expected) in [
        (vec![0, 0, 0, 0x3d, 1, 0x3f], Reject::Invalid),
        (vec![0, 0, 0, 1, 1, 0x3f], Reject::Invalid),
        (
            vec![0, 0, 0, 2, 0x40, 1, 0x16, 0, 0x3f],
            Reject::Unsupported,
        ),
        (vec![0, 0, 0, 0x15, 99, 0, 0x3f], Reject::Unproven),
        (vec![0, 0, 0, 1, 1, 1, 2, 0x32, 2, 0x3f], Reject::Unproven),
    ] {
        let bytes = fixture(&[&code]);
        let program = qco::parse(&bytes).unwrap();
        assert_eq!(builder(&program).build(0x4001, 0), Err(expected));
    }
}

#[test]
fn asynchronous_entries_use_maximum_not_nested_sum() {
    let a = [2, 0, 0, 1, 7, 0x36, 0x3f];
    let b = [4, 0, 0, 1, 7, 0x36, 0x3f];
    let bytes = fixture(&[&a, &b]);
    let program = qco::parse(&bytes).unwrap();
    let mut proof = builder(&program);
    proof.build(0x4001, 2).unwrap();
    proof.build(0x4002, 1).unwrap();
    assert_eq!(frames::peaks(&proof.frames, 7).unwrap(), [4, 6]);
    assert_eq!(frames::peaks(&proof.frames, 6), Err(Reject::Budget));
}

#[test]
fn inline_table_counts_two_incoming_slots_and_its_saved_base() {
    let code = [0, 0, 0, 1, 2, 2, 1, 54, 0x32, 2, 0x3f];
    let bytes = fixture(&[&code]);
    let program = qco::parse(&bytes).unwrap();
    let mut proof = builder(&program);
    // Same envelope as the reviewed flags2 table: saved base + peak3 temps.
    proof.frames.push(frames::Frame {
        locals: 0,
        argc: 2,
        steps: vec![step(0, 3, vec![1]), step(3, 0, vec![])],
    });
    let root = proof.build(0x4001, 2).unwrap();
    let peaks = frames::peaks(&proof.frames, 9).unwrap();
    assert_eq!(peaks[root], 7); // plus caller's own two incoming arguments
    assert_eq!(frames::peaks(&proof.frames, 8), Err(Reject::Budget));
}

#[test]
fn cfg_compaction_removes_only_structurally_unreachable_blocks() {
    // GOTO at pc3 skips a redundant GOTO and an underflowing DROP at pc9.
    let dead = [0, 0, 0, 0x14, 0, 7, 0x14, 0, 4, 0x3d, 1, 0x3f];
    let bytes = fixture(&[&dead]);
    let program = qco::parse(&bytes).unwrap();
    let mut proof = builder(&program);
    let root = proof.build(0x4001, 0).unwrap();
    assert_eq!(frames::peaks(&proof.frames, 1).unwrap()[root], 1);

    // A consuming conditional retains both successors, even with a literal
    // condition: its fallthrough reaches the same underflowing DROP.
    let conditional = [0, 0, 0, 1, 0, 0x45, 0, 7, 0x14, 0, 2, 0x3d, 1, 0x3f];
    let bytes = fixture(&[&conditional]);
    let program = qco::parse(&bytes).unwrap();
    assert_eq!(builder(&program).build(0x4001, 0), Err(Reject::Invalid));

    let operand_target = [0, 0, 0, 0x14, 0, 1, 0x3f];
    assert!(bytecode::decode(&operand_target).is_err());
}

#[test]
fn silent_sound_comparison_counts_executed_temporaries() {
    let frames = [silent_sound_frame()];
    assert_eq!(frames::peaks(&frames, 5), Ok(vec![4]));
    assert_eq!(frames::peaks(&frames, 4), Err(Reject::Budget));
}

#[test]
fn native_peek_requirement_is_independent_of_drop() {
    assert_eq!(native(21, Some(1058)), Ok(7));
    assert_eq!(native(21, Some(15003)), Ok(5));
    let code = [0, 0, 0, 2, 4, 34, 0x15, 21, 0, 0x3d, 1, 0x3f];
    let bytes = fixture(&[&code]);
    let program = qco::parse(&bytes).unwrap();
    assert_eq!(builder(&program).build(0x4001, 0), Err(Reject::Invalid));
}

#[test]
#[ignore = "bounded cached metadata only; no media or JVM execution"]
fn actual_canonical_normal_stack_envelope() {
    use std::io::Read;
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    let assets = super::super::assets::load(|path, limit| {
        let path = jar.parent().unwrap().join(path.rsplit('/').next().unwrap());
        let file = std::fs::File::open(path).map_err(|_| Reject::MissingAsset)?;
        let mut bytes = Vec::new();
        file.take(limit as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| Reject::Invalid)?;
        Ok(bytes)
    })
    .unwrap();
    let bytes = assets.authored_file("FS.QCO").unwrap();
    let program = qco::parse(&bytes).unwrap();
    let resources = assets.authored_file("RESDIR.QCD").unwrap();
    let mut count = 0;
    for resource in super::super::qcd::parse(&resources).unwrap() {
        if resource.kind != 4 {
            continue;
        }
        let Some(path) = resource.locator.strip_prefix(b"file:///jar/") else {
            continue;
        };
        let data = assets
            .authored_file(std::str::from_utf8(path).unwrap())
            .unwrap();
        let table = qcs::parse(&data).unwrap();
        if !table
            .program
            .is_some_and(|body| template::table_program(body).is_ok())
        {
            continue;
        }
        count += 1;
        assert_eq!(verify(&program, 0x4130, 0x405e, &table, 772), Ok(()));
        assert_eq!(
            verify(&program, 0x4130, 0x405e, &table, 1),
            Err(Reject::Budget)
        );
    }
    assert_eq!(count, 1);
}

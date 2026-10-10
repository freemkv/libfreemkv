use super::*;
use std::io::Read;

#[test]
#[ignore = "bounded cached QCO only; no media or JVM execution"]
fn code_identity_excludes_roster_data_but_not_opcodes_or_event_edges() {
    let path = std::env::var("ONQ_TEST_JAR").unwrap();
    let file = std::fs::File::open(path).unwrap();
    let mut jar = zip::ZipArchive::new(file).unwrap();
    let mut bytes = Vec::new();
    jar.by_name("FS.QCO")
        .unwrap()
        .take(2 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .unwrap();
    let mut program = super::super::qco::parse(&bytes).unwrap();
    let expected = fingerprint(&program, 94, 67).unwrap();
    recognize(&program, 94, 67).unwrap();
    eprintln!("PROGRAM_VERSION={expected:02x?}");
    let Value::Integers { values, .. } = &mut program.screen.variables[396] else {
        panic!("roster array");
    };
    values[0] = 999;
    assert_eq!(
        fingerprint(&program, 94, 67).unwrap(),
        expected,
        "data is a separate proof obligation"
    );
    let mut changed = program.screen.functions[303].to_vec();
    changed[3] ^= 1;
    program.screen.functions[303] = &changed;
    assert_ne!(fingerprint(&program, 94, 67).unwrap(), expected);
    assert!(recognize(&program, 94, 67).is_err());
    let mut program = super::super::qco::parse(&bytes).unwrap();
    program.screen.objects[0].events[0].function ^= 1;
    assert_ne!(fingerprint(&program, 94, 67).unwrap(), expected);
    let mut program = super::super::qco::parse(&bytes).unwrap();
    let mut thunk = program.screen.functions[124].to_vec();
    thunk[4] = 5;
    program.screen.functions[124] = &thunk;
    assert_eq!(fingerprint(&program, 94, 67).unwrap(), expected);
    assert!(fingerprint(&program, 94, 5).is_err());
    let mut redirected = thunk.clone();
    redirected[7] ^= 1;
    program.screen.functions[124] = &redirected;
    assert_ne!(fingerprint(&program, 94, 67).unwrap(), expected);
}

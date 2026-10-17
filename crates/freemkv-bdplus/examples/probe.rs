//! Bootstrap diagnostic only: stopping at an input request is not a repair proof.

use freemkv_bdplus::{BdPlusEvent, BdPlusPlayerProfile, BdPlusSession, Halt, SessionBoundary, Svm};

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .expect("usage: probe PRIMARY.svm");
    let svm =
        Svm::parse(std::fs::read(path).expect("read primary SVM")).expect("parse primary SVM");
    let mut session = BdPlusSession::new(&svm, BdPlusPlayerProfile::default()).expect("load VM");
    session
        .begin_event(BdPlusEvent::Start)
        .expect("start event");
    let boundary = session.run(100_000, &Halt::new()).expect("run bootstrap");
    println!(
        "boundary={boundary:?} retired={} next_pc={:#x} table_present={}",
        session.vm().instructions(),
        session.vm().pc(),
        session.fixup_table().is_some()
    );
    if let SessionBoundary::NeedPlayerMaterial(freemkv_bdplus::PlayerOperation::Discovery {
        ..
    }) = boundary
    {
        let capacity_pointer = session.vm().argument(3);
        println!(
            "requested_discovery_capacity={}",
            session.vm().memory().load_u32(capacity_pointer)
        );
    }
}

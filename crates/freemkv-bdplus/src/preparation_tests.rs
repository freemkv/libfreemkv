use super::*;
use crate::{BdPlusPlayerProfile, Svm};

fn session(words: &[u32]) -> BdPlusSession<BdPlusPlayerProfile> {
    let length = 0x1000 + words.len() * 4;
    let mut bytes = vec![0; 24 + length + 40];
    bytes[..8].copy_from_slice(b"BDSVM_CC");
    bytes[20..24].copy_from_slice(&(length as u32).to_be_bytes());
    for (index, word) in words.iter().enumerate() {
        bytes[24 + 0x1000 + index * 4..24 + 0x1004 + index * 4]
            .copy_from_slice(&word.to_be_bytes());
    }
    BdPlusSession::new(&Svm::parse(bytes).unwrap(), BdPlusPlayerProfile::default()).unwrap()
}

#[test]
fn preparation_runs_global_then_authored_titles_and_rejects_no_table() {
    let mut session = session(&[0xe400_0010]);
    let mut preparation = BdPlusPreparation::new([2, 7]);
    let halt = Halt::new();
    let expected = [
        BdPlusEvent::Start,
        BdPlusEvent::TitleInit { title: 0xffff },
        BdPlusEvent::TitleInit { title: 2 },
        BdPlusEvent::TitleInit { title: 7 },
    ];
    for event in expected[..expected.len() - 1].iter().copied() {
        assert_eq!(
            session
                .advance_preparation(&mut preparation, 10, &halt)
                .unwrap(),
            PreparationBoundary::EventFinished(event)
        );
    }
    assert_eq!(
        session.advance_preparation(&mut preparation, 10, &halt),
        Err(BdPlusError::PreparationEndedWithoutTable)
    );
}

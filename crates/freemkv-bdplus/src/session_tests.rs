use super::*;
use crate::Halt;
use crate::{BdPlusContentLibrary, BdPlusPlayerProfile};

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

fn arguments(session: &mut BdPlusSession<BdPlusPlayerProfile>, words: &[u32]) {
    session.vm.set_register(29, 0x200).unwrap();
    for (index, word) in words.iter().enumerate() {
        session
            .vm
            .memory_mut()
            .store_u32(0x200 + index as u32 * 4, *word);
    }
}

#[test]
fn missing_material_preserves_the_trap_then_resumes_after_provisioning() {
    let mut session = session(&[0xe400_0520, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[1, 1, 0x300, 0x280]);
    session.vm.memory_mut().store_u32(0x280, 4);
    session.vm.memory_mut().store_u32(0x300, 0xaaaa_aaaa);
    let boundary = session.run(100, &Halt::new()).unwrap();
    let SessionBoundary::NeedPlayerMaterial(request) = boundary else {
        panic!("expected player request, got {boundary:?}");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::Discovery {
            device: 1,
            question: 1,
        }
    );
    let position = session.vm.instructions();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::NeedPlayerMaterial(request)
    );
    assert_eq!(session.vm.instructions(), position);
    assert_eq!(session.vm.memory().load_u32(0x300), 0xaaaa_aaaa);
    session.player_mut().set_discovery(1, 1, vec![1, 2, 3]);
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [1, 2, 3, 0xaa]);
    assert_eq!(session.vm.memory().load_u32(0x280), 3);
    assert_eq!(session.vm.instructions(), position + 1);
}

#[test]
fn external_reply_is_bound_to_the_pending_request_and_resumes_once() {
    let mut session = session(&[0xe400_0520, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[1, 1, 0x300, 0x280]);
    session.vm.memory_mut().store_u32(0x280, 4);

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(100, &Halt::new()).unwrap()
    else {
        panic!("expected player request");
    };
    assert_eq!(
        session
            .provide_player_material(request.id() + 1, PlayerMaterial::Discovery(vec![1, 2, 3]),),
        Err(BdPlusError::PlayerRequestMismatch {
            expected: request.id(),
            supplied: request.id() + 1,
        })
    );
    assert_eq!(
        session.provide_player_material(request.id(), PlayerMaterial::MediaKey([0; 16])),
        Err(BdPlusError::PlayerReplyType)
    );
    session
        .provide_player_material(request.id(), PlayerMaterial::Discovery(vec![1, 2, 3]))
        .unwrap();
    assert_eq!(
        session.provide_player_material(request.id(), PlayerMaterial::Discovery(vec![4, 5, 6]),),
        Err(BdPlusError::PlayerReplyAlreadySupplied {
            request_id: request.id(),
        })
    );
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [1, 2, 3, 0]);
    assert_eq!(
        session.provide_player_material(request.id(), PlayerMaterial::Discovery(vec![7, 8, 9]),),
        Err(BdPlusError::NoPendingTrap)
    );
}

#[test]
fn table_submission_and_compute_sp_share_the_same_vm() {
    let mut session = session(&[0xe400_0020, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    let mut table = vec![0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 10];
    table.extend_from_slice(&[0x40, 0, 0, 1, 8, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    arguments(&mut session, &[table.len() as u32, 0x300]);
    session.vm.memory_mut().write(0x300, &table).unwrap();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert!(session.fixup_table().unwrap().patches_for_clip(7).is_err());
    // Model content-code replacing its event handler, as real bootstrap code
    // does. The test's ComputeSP handler returns an all-zero key/mask.
    session.vm.memory_mut().store_u32(0x1000, 0xe400_0010);
    session.vm.memory_mut().write(0x10, &[0; 16]).unwrap();
    let event = BdPlusEvent::ComputeSp {
        clip_id: 7,
        segment: 0,
    };
    session.begin_event(event).unwrap();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(event)
    );
    let patches = session.fixup_table().unwrap().patches_for_clip(7).unwrap();
    assert_eq!(patches.patches().len(), 2);
    assert_eq!(patches.patches()[0].offset(), 10 * 192 + 8);
}

#[test]
fn unsupported_host_operation_fails_explicitly_and_remains_pending() {
    let mut session = session(&[0xe400_0570]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    let expected = Err(BdPlusError::UnsupportedTrap {
        trap_id: 0x570,
        pc: 0x1000,
    });
    assert_eq!(session.run(10, &Halt::new()), expected);
    assert_eq!(session.run(10, &Halt::new()), expected);
    assert_eq!(session.vm.instructions(), 1);
}

#[test]
fn application_handshake_without_an_application_host_cannot_report_success() {
    let mut session = session(&[0xe400_0510]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0, 0, 0x300]);
    session.vm.memory_mut().store_u32(0x300, 0x1234_5678);
    let expected = Err(BdPlusError::UnsupportedTrap {
        trap_id: 0x510,
        pc: 0x1000,
    });
    assert_eq!(session.run(10, &Halt::new()), expected);
    assert_eq!(session.run(10, &Halt::new()), expected);
    assert_eq!(session.vm.memory().load_u32(0x300), 0x1234_5678);
    assert_eq!(session.vm.instructions(), 1);
}

#[test]
fn unknown_content_offset_does_not_silently_load_the_wrong_range() {
    let mut session = session(&[0xe400_0540]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0, 0, 16, 0x280, 0x300]);
    let mut content = BdPlusContentLibrary::default();
    content.insert(0, secondary_svm(b"wrong-prefix-data"));
    session.set_content(content);
    session.vm.memory_mut().store_u32(0x280, 4);
    session.vm.memory_mut().store_u32(0x300, 0x1234_5678);
    assert_eq!(
        session.run(10, &Halt::new()),
        Err(BdPlusError::UnsupportedContentOffset { offset: 16 })
    );
    assert_eq!(session.vm.memory().load_u32(0x300), 0x1234_5678);
    assert_eq!(session.vm.memory().load_u32(0x280), 4);
}

fn secondary_svm(body: &[u8]) -> Svm {
    let mut bytes = Vec::with_capacity(24 + body.len() + 40);
    bytes.extend_from_slice(b"BDSVM_CC");
    bytes.extend_from_slice(&[0; 12]);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&[0; crate::svm::TRAILER_SIZE]);
    Svm::parse(bytes).unwrap()
}

#[test]
fn load_content_code_copies_requested_section_bytes_and_reports_actual_length() {
    let mut session = session(&[0xe400_0540, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    let payload: Vec<u8> = (0..32).collect();
    let mut library = BdPlusContentLibrary::default();
    library.insert(1, secondary_svm(&payload));
    session.set_content(library);
    arguments(&mut session, &[1, 0, 0, 0x280, 0x300]);
    session.vm.memory_mut().store_u32(0x280, 20);
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().load_u32(0x280), 20);
    assert_eq!(
        session.vm.memory().read(0x300, 20).unwrap(),
        (0..20u8).collect::<Vec<_>>()
    );
}

#[test]
fn load_content_code_without_reader_or_missing_file_fails_without_consuming_trap() {
    let mut session = session(&[0xe400_0540]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[1, 0, 0, 0x280, 0x300]);
    session.vm.memory_mut().store_u32(0x280, 8);
    assert_eq!(
        session.run(10, &Halt::new()),
        Err(BdPlusError::NoContentReader),
    );
    session.set_content(BdPlusContentLibrary::default());
    assert_eq!(
        session.run(10, &Halt::new()),
        Err(BdPlusError::MissingContent {
            name: 1,
            section: 0,
        }),
    );
}

#[test]
fn load_content_code_refused_outside_permitted_events() {
    let mut session = session(&[0xe400_0540, 0xe400_0010]);
    session.set_content(BdPlusContentLibrary::default());
    session.begin_event(BdPlusEvent::ApplicationLayer).unwrap();
    arguments(&mut session, &[1, 0, 0, 0x280, 0x300]);
    session.vm.memory_mut().store_u32(0x280, 8);
    assert_eq!(
        session.run(10, &Halt::new()),
        Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x540 }),
    );
}

#[test]
#[ignore = "requires FREEMKV_BDPLUS_SVM pointing to the local Cast Away capture"]
fn cast_away_session_reaches_first_missing_discovery_without_network() {
    let bytes =
        std::fs::read(std::env::var_os("FREEMKV_BDPLUS_SVM").expect("local capture")).unwrap();
    let svm = Svm::parse(bytes).unwrap();
    let mut session = BdPlusSession::new(&svm, BdPlusPlayerProfile::default()).unwrap();
    session.begin_event(BdPlusEvent::Start).unwrap();
    let boundary = session.run(10000, &Halt::new()).unwrap();
    let SessionBoundary::NeedPlayerMaterial(request) = boundary else {
        panic!("expected player request, got {boundary:?}");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::Discovery {
            device: 1,
            question: 1
        }
    );
    assert_eq!(session.vm.instructions(), 840);
}

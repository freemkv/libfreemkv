use super::*;
use crate::Halt;
use crate::{BdPlusContentLibrary, BdPlusPlayerProfile, BdPlusTimestamp, SLOT_IMAGE_SIZE};
use sha1::{Digest, Sha1};

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
            capacity: 4,
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
fn dynamic_discovery_requests_the_aacs_volume_id_semantically() {
    let mut session = session(&[0xe400_0520, 0xe400_0010]);
    session.set_disc_context(BdPlusDiscContext {
        timestamp: Some(BdPlusTimestamp {
            year: 2026,
            month: 10,
            day: 16,
            ..Default::default()
        }),
        discovery_environment: Some([0x22; 16]),
        discovery_tail: Some([0x33; 20]),
        ..Default::default()
    });
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[1, 3, 0x300, 0x280]);
    session.vm.memory_mut().store_u32(0x280, 60);

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(100, &Halt::new()).unwrap()
    else {
        panic!("expected volume-ID request");
    };
    assert_eq!(request.operation(), PlayerOperation::VolumeId);
    session
        .provide_player_material(request.id(), PlayerMaterial::VolumeId([0x44; 16]))
        .unwrap();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x318, 16).unwrap(), &[0x44; 16]);
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
fn arithmetic_traps_use_big_endian_words_and_report_carry() {
    let mut add = session(&[0xe400_0210, 0xe400_0010]);
    add.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut add, &[0x300, 0x320, 2]);
    add.vm
        .memory_mut()
        .write(0x300, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])
        .unwrap();
    add.vm
        .memory_mut()
        .write(0x320, &[0, 0, 0, 0, 0, 0, 0, 1])
        .unwrap();
    assert_eq!(
        add.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(add.vm.registers()[1], 1);
    assert_eq!(add.vm.memory().read(0x300, 8).unwrap(), [0; 8]);

    let mut multiply = session(&[0xe400_0220, 0xe400_0010]);
    multiply.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut multiply, &[0x300, 0x320, 2, 2]);
    multiply
        .vm
        .memory_mut()
        .write(0x320, &[0x80, 0, 0, 0, 0, 0, 0, 1])
        .unwrap();
    assert_eq!(
        multiply.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(
        multiply.vm.memory().read(0x300, 12).unwrap(),
        [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
    );
}

#[test]
fn memory_search_returns_the_first_virtual_address_or_zero() {
    let mut found = session(&[0xe400_0320, 0xe400_0010]);
    found.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut found, &[0x300, 8, 0x320, 3, 0x340]);
    found.vm.memory_mut().write(0x300, b"ababacab").unwrap();
    found.vm.memory_mut().write(0x320, b"aba").unwrap();
    assert_eq!(
        found.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(found.vm.memory().load_u32(0x340), 0x300);

    let mut absent = session(&[0xe400_0320, 0xe400_0010]);
    absent.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut absent, &[0x300, 8, 0x320, 3, 0x340]);
    absent.vm.memory_mut().write(0x300, b"ababacab").unwrap();
    absent.vm.memory_mut().write(0x320, b"xyz").unwrap();
    absent.run(100, &Halt::new()).unwrap();
    assert_eq!(absent.vm.memory().load_u32(0x340), 0);
}

#[test]
fn random_trap_requests_exact_external_entropy_without_mutating_early() {
    let mut session = session(&[0xe400_0130, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0x300, 4]);
    session
        .vm
        .memory_mut()
        .write(0x300, &[0xaa, 0xaa, 0xaa, 0xaa])
        .unwrap();
    let SessionBoundary::NeedPlayerMaterial(request) = session.run(100, &Halt::new()).unwrap()
    else {
        panic!("expected entropy request");
    };
    assert_eq!(request.operation(), PlayerOperation::Random { length: 4 });
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [0xaa; 4]);
    session
        .provide_player_material(request.id(), PlayerMaterial::Random(vec![1, 2, 3, 4]))
        .unwrap();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [1, 2, 3, 4]);
}

#[test]
fn aes_player_key_request_carries_wrapped_key_and_rejects_invalid_ids() {
    let mut aes = session(&[0xe400_0110, 0]);
    aes.player_mut().declare_aes_key(4);
    aes.begin_event(BdPlusEvent::Start).unwrap();
    let wrapped_key = [0x44; 16];
    let ciphertext = [0x22; 16];
    arguments(&mut aes, &[0x300, 0x320, 1, 0x340, 4]);
    aes.vm.memory_mut().write(0x320, &ciphertext).unwrap();
    aes.vm.memory_mut().write(0x340, &wrapped_key).unwrap();
    let SessionBoundary::NeedPlayerMaterial(request) = aes.run(1, &Halt::new()).unwrap() else {
        panic!("expected AES-key request");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::AesKey {
            key_id: 4,
            wrapped_key,
        }
    );
    aes.provide_player_material(request.id(), PlayerMaterial::UnwrappedAesKey([0; 16]))
        .unwrap();
    assert_eq!(aes.run(1, &Halt::new()).unwrap(), SessionBoundary::Yield);
    assert_eq!(
        aes.vm.memory().read(0x300, 16).unwrap(),
        crate::crypto::aes_ecb_decrypt(&[0; 16], &ciphertext)
    );

    let mut invalid = session(&[0xe400_0110, 0]);
    invalid.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut invalid, &[0x300, 0x320, 1, 0x340, 7]);
    assert_eq!(
        invalid.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(invalid.vm.registers()[1], 0x8000_0001);

    let mut overlap = session(&[0xe400_0110, 0]);
    overlap.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut overlap, &[0x308, 0x300, 1, 0x340, 0xfff1_0001]);
    assert_eq!(
        overlap.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(overlap.vm.registers()[1], 0x8000_0001);
}

#[test]
fn aes_is_scoped_to_start_shutdown_and_title_events() {
    let mut session = session(&[0xe400_0110, 0]);
    session.begin_event(BdPlusEvent::ApplicationLayer).unwrap();
    arguments(&mut session, &[0x300, 0x320, 0, 0x340, 0xfff1_0000]);
    assert_eq!(
        session.run(1, &Halt::new()),
        Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x110 })
    );
}

#[test]
fn private_key_signing_is_a_typed_player_capability() {
    let mut session = session(&[0xe400_0120, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[1, 0x400, 0x500, 4, 0x1122_3344]);
    session.vm.memory_mut().write(0x500, b"test").unwrap();
    session.vm.memory_mut().write(0x400, &[0xaa; 40]).unwrap();

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(10, &Halt::new()).unwrap()
    else {
        panic!("expected private-key request");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::PrivateKeySign {
            key_id: 1,
            control_word: 0x1122_3344,
            message: b"test".to_vec(),
        }
    );
    assert_eq!(session.vm.memory().read(0x400, 40).unwrap(), &[0xaa; 40]);

    session
        .provide_player_material(request.id(), PlayerMaterial::Signature([0x5a; 40]))
        .unwrap();
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x400, 40).unwrap(), &[0x5a; 40]);
}

#[test]
fn sha1_block_hashes_into_the_vm_context_region() {
    let mut session = session(&[0xe400_0140, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0x400, 0x700, 3, 3]);
    session.vm.memory_mut().write(0x700, b"abc").unwrap();

    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(
        session.vm.memory().read(0x400, 20).unwrap(),
        [
            0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e, 0x25, 0x71, 0x78, 0x50,
            0xc2, 0x6c, 0x9c, 0xd0, 0xd8, 0x9d,
        ]
    );
}

#[test]
fn sha1_stream_state_survives_across_host_calls() {
    let mut session = session(&[0xe400_0010]);
    session.vm.memory_mut().write(0x700, b"abcdef").unwrap();
    session.sha1([0x400, 0x700, 3, 1, 0]).unwrap();
    session.sha1([0x400, 0x703, 3, 2, 0]).unwrap();
    assert_eq!(
        session.vm.memory().read(0x400, 20).unwrap(),
        [
            0x1f, 0x8a, 0xc1, 0x0f, 0x23, 0xc5, 0xb5, 0xbc, 0x11, 0x67, 0xbd, 0xa8, 0x4b, 0x83,
            0x3e, 0x5c, 0x05, 0x7a, 0x77, 0xd2,
        ]
    );
}

#[test]
fn discovery_ram_reads_only_modeled_player_memory() {
    let mut session = session(&[0xe400_0530, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0x1234_5000, 0x300, 4]);
    session
        .vm
        .memory_mut()
        .write(0x300, &[0xaa, 0xaa, 0xaa, 0xaa])
        .unwrap();
    let SessionBoundary::NeedPlayerMaterial(request) = session.run(100, &Halt::new()).unwrap()
    else {
        panic!("expected modeled-memory request");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::Memory {
            address: 0x1234_5000,
            length: 4,
        }
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [0xaa; 4]);
    session
        .provide_player_material(request.id(), PlayerMaterial::Memory(vec![1, 2, 3, 4]))
        .unwrap();
    assert_eq!(
        session.run(100, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), [1, 2, 3, 4]);
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
fn application_register_write_is_a_resumable_host_operation() {
    let mut session = session(&[0xe400_0510, 0xe400_0010]);
    session.begin_event(BdPlusEvent::ApplicationLayer).unwrap();
    arguments(&mut session, &[0, 1, 0x300]);
    session.vm.memory_mut().store_u32(0x300, 0x1234_5678);

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(10, &Halt::new()).unwrap()
    else {
        panic!("expected application-register write");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::ApplicationWrite {
            register: 0,
            value: 0x1234_5678,
        }
    );
    session
        .provide_player_material(request.id(), PlayerMaterial::ApplicationAcknowledged)
        .unwrap();
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::ApplicationLayer)
    );
}

#[test]
fn application_register_read_is_a_resumable_host_operation() {
    let mut session = session(&[0xe400_0510, 0xe400_0010]);
    session.begin_event(BdPlusEvent::ApplicationLayer).unwrap();
    arguments(&mut session, &[1, 0, 0x300]);

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(10, &Halt::new()).unwrap()
    else {
        panic!("expected application-register read");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::ApplicationRead { register: 1 }
    );
    session
        .provide_player_material(request.id(), PlayerMaterial::ApplicationValue(0x89ab_cdef))
        .unwrap();
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::ApplicationLayer)
    );
    assert_eq!(session.vm.memory().load_u32(0x300), 0x89ab_cdef);
}

#[test]
fn application_register_exchange_is_event_scoped() {
    let mut session = session(&[0xe400_0510]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0, 1, 0x300]);
    assert_eq!(
        session.run(10, &Halt::new()),
        Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x510 })
    );
}

#[test]
fn media_check_requests_disc_bytes_and_hashes_complete_blocks() {
    let mut session = session(&[0xe400_0550, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    let path = b"BDMV/STREAM/00000.m2ts";
    session.vm.memory_mut().write(0x300, path).unwrap();
    session.vm.memory_mut().store_u32(0x280, 0x400);
    session.vm.memory_mut().write(0x800, &[0xaa; 40]).unwrap();
    arguments(
        &mut session,
        &[0x300, path.len() as u32, 1, 2, 0x280, 0x800],
    );

    let SessionBoundary::NeedPlayerMaterial(request) = session.run(10, &Halt::new()).unwrap()
    else {
        panic!("expected semantic media read");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::MediaRead {
            path: path.to_vec(),
            offset: 0x1_0000_0002,
            length: 0x400,
        }
    );
    assert_eq!(session.vm.memory().read(0x800, 40).unwrap(), &[0xaa; 40]);

    session
        .provide_player_material(request.id(), PlayerMaterial::Media(vec![b'a'; 0x200]))
        .unwrap();
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().load_u32(0x280), 0x200);
    assert_eq!(
        session.vm.memory().read(0x800, 20).unwrap(),
        [
            0x16, 0x45, 0x57, 0xfa, 0xcb, 0x73, 0x92, 0x98, 0x75, 0x16, 0x8c, 0x1e, 0x92, 0xca,
            0xf0, 0x9b, 0xb6, 0x06, 0x45, 0x64,
        ]
    );
}

#[test]
fn load_content_code_applies_the_documented_within_block_offset() {
    let mut session = session(&[0xe400_0540, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0, 0, 6, 0x280, 0x300]);
    let mut content = BdPlusContentLibrary::default();
    content.insert(0, secondary_svm(b"wrong-prefix-data"));
    session.set_content(content);
    session.vm.memory_mut().store_u32(0x280, 4);
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
    assert_eq!(session.vm.memory().read(0x300, 4).unwrap(), b"pref");
    assert_eq!(session.vm.memory().load_u32(0x280), 4);
}

#[test]
fn diagnostic_log_is_an_explicit_observer_boundary() {
    let mut session = session(&[0xe400_8010, 0xe400_0010]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    session.vm.memory_mut().write(0x300, b"hello").unwrap();
    arguments(&mut session, &[0x300, 5]);
    let SessionBoundary::NeedPlayerMaterial(request) = session.run(10, &Halt::new()).unwrap()
    else {
        panic!("expected diagnostic observer request");
    };
    assert_eq!(
        request.operation(),
        PlayerOperation::DiagnosticLog {
            message: b"hello".to_vec(),
        }
    );
    session
        .provide_player_material(request.id(), PlayerMaterial::DiagnosticAcknowledged)
        .unwrap();
    assert_eq!(
        session.run(10, &Halt::new()).unwrap(),
        SessionBoundary::FinishedEvent(BdPlusEvent::Start)
    );
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
fn new_slot_allocation_requests_volume_id_and_exports_the_persistent_image() {
    let mut session = session(&[0xe400_0410, 0]);
    session.set_slot_storage(BdPlusSlotStorage::default());
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[u32::MAX, 0]);
    let SessionBoundary::NeedPlayerMaterial(request) = session.run(1, &Halt::new()).unwrap() else {
        panic!("expected volume-ID request");
    };
    assert_eq!(request.operation(), PlayerOperation::VolumeId);
    session
        .provide_player_material(request.id(), PlayerMaterial::VolumeId([0x5a; 16]))
        .unwrap();
    assert_eq!(
        session.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(session.vm.registers()[1], 499);
    let record = session.slot_storage().unwrap().record(499).unwrap();
    assert_eq!(record[..16], [0x5a; 16]);
    assert_eq!(record[16..32], [0x5a; 16]);
    assert_eq!(record[32..], [0; 224]);
    assert_eq!(
        session.slot_storage().unwrap().to_image().len(),
        SLOT_IMAGE_SIZE
    );
}

#[test]
fn slot_attach_authenticates_the_call_site_and_instruction_filter() {
    let words: [u32; 2] = [0xe400_0410, 0];
    let mut image = vec![0; SLOT_IMAGE_SIZE];
    let entry: Vec<u8> = words
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .chain(std::iter::repeat_n(0, 8))
        .collect();
    let mut input = Vec::new();
    input.extend_from_slice(&0x1000_u32.to_be_bytes());
    input.extend_from_slice(&0_u32.to_be_bytes());
    input.extend_from_slice(&entry);
    input.extend_from_slice(&entry[..8]);
    let first = Sha1::digest(&input);
    let digest = Sha1::digest(first);
    image[7 * SLOT_SIZE + 48..7 * SLOT_SIZE + 68].copy_from_slice(&digest);

    let mut accepted = session(&words);
    accepted.set_slot_storage(BdPlusSlotStorage::from_image(&image).unwrap());
    accepted.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut accepted, &[7, 2]);
    assert_eq!(
        accepted.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(accepted.vm.registers()[1], 0);

    image[7 * SLOT_SIZE + 48] ^= 1;
    let mut rejected = session(&words);
    rejected.set_slot_storage(BdPlusSlotStorage::from_image(&image).unwrap());
    rejected.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut rejected, &[7, 2]);
    assert_eq!(
        rejected.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(rejected.vm.registers()[1], 0x8000_0001);
}

#[test]
fn slot_read_redacts_unattached_authorization_hash_and_write_is_field_scoped() {
    let mut image = vec![0; SLOT_IMAGE_SIZE];
    image[SLOT_SIZE..SLOT_SIZE + 16].fill(0x11);
    image[SLOT_SIZE + 48..SLOT_SIZE + 68].fill(0x22);
    let storage = BdPlusSlotStorage::from_image(&image).unwrap();

    let mut read = session(&[0xe400_0420, 0]);
    read.set_slot_storage(storage);
    read.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut read, &[0x400, 1]);
    assert_eq!(read.run(1, &Halt::new()).unwrap(), SessionBoundary::Yield);
    assert_eq!(read.vm.memory().read(0x400, 16).unwrap(), [0x11; 16]);
    assert_eq!(read.vm.memory().read(0x430, 20).unwrap(), [0; 20]);

    let mut write = session(&[0xe400_0430, 0]);
    write.set_slot_storage(BdPlusSlotStorage::default());
    write.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut write, &[0x400]);
    let mut update = [0; SLOT_SIZE];
    update[16..68].fill(0x44);
    update[76..].fill(0x66);
    write.vm.memory_mut().write(0x400, &update).unwrap();
    assert_eq!(write.run(1, &Halt::new()).unwrap(), SessionBoundary::Yield);
    let stored = write.slot_storage().unwrap().record(0).unwrap();
    assert_eq!(stored[16..68], [0x44; 52]);
    assert_eq!(stored[72..76], 1_u32.to_be_bytes());
    assert_eq!(stored[76..], [0x66; 180]);
}

#[test]
fn missing_slot_storage_is_an_explicit_resumable_input() {
    let mut session = session(&[0xe400_0420, 0]);
    session.begin_event(BdPlusEvent::Start).unwrap();
    arguments(&mut session, &[0x400, 0]);
    let SessionBoundary::NeedPlayerMaterial(request) = session.run(1, &Halt::new()).unwrap() else {
        panic!("expected slot-storage request");
    };
    assert_eq!(request.operation(), PlayerOperation::SlotStorage);
    session
        .provide_player_material(
            request.id(),
            PlayerMaterial::SlotStorage(BdPlusSlotStorage::default()),
        )
        .unwrap();
    assert_eq!(
        session.run(1, &Halt::new()).unwrap(),
        SessionBoundary::Yield
    );
    assert_eq!(
        session.vm.memory().read(0x400, SLOT_SIZE).unwrap(),
        [0; SLOT_SIZE]
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
            question: 1,
            capacity: 320,
        }
    );
    assert_eq!(session.vm.instructions(), 840);
}

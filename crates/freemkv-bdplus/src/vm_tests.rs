use super::*;

fn program(words: &[u32]) -> Svm {
    let length = 0x1000 + words.len() * 4;
    let mut bytes = vec![0xaa; 24 + length + 40];
    bytes[..8].copy_from_slice(b"BDSVM_CC");
    bytes[20..24].copy_from_slice(&(length as u32).to_be_bytes());
    for (index, word) in words.iter().enumerate() {
        bytes[24 + 0x1000 + index * 4..24 + 0x1004 + index * 4]
            .copy_from_slice(&word.to_be_bytes());
    }
    Svm::parse(bytes).unwrap()
}

#[test]
fn bootstrap_initializes_runtime_without_mutating_source() {
    let image = program(&[0xe400_0010]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    assert_eq!(image.primary().body().prefix()[0], 0xaa);
    assert!(
        vm.memory()
            .read(0, 0x1000)
            .unwrap()
            .iter()
            .all(|byte| *byte == 0)
    );
    assert_eq!(
        vm.run(1, &Halt::new()).unwrap(),
        VmBoundary::Trap {
            trap_id: 0x10,
            pc: 0x1000
        }
    );
    assert!(matches!(
        vm.run(1, &Halt::new()),
        Err(BdPlusError::PendingTrap { .. })
    ));
    vm.resume_trap(Some(0x8000_0001)).unwrap();
    assert_eq!(vm.registers()[1], 0x8000_0001);
    assert_eq!(vm.resume_trap(None), Err(BdPlusError::NoPendingTrap));
}

#[test]
fn backwards_jump_has_no_delay_slot_and_budget_preserves_pc() {
    let image = program(&[0xb3ff_fffc, 0xe400_0010]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    assert_eq!(vm.run(100, &Halt::new()).unwrap(), VmBoundary::Yield);
    assert_eq!(vm.pc(), 0x1000);
    assert_eq!(vm.instructions(), 100);
    let halt = Halt::new();
    halt.cancel();
    assert_eq!(vm.run(1, &halt), Err(BdPlusError::Cancelled));
    assert_eq!(vm.instructions(), 100);
}

#[test]
fn filter_changes_next_fetch_and_zero_register_is_hardwired() {
    let filter = 0x0800;
    let image = program(&[
        0x6020_0800,          // ADDI r1,r0,0x800
        0xe801_0000,          // INSTF r1
        0x6000_0001 ^ filter, // ADDI r0,r0,1
        0xe400_0010 ^ filter,
    ]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    assert_eq!(
        vm.run(10, &Halt::new()).unwrap(),
        VmBoundary::Trap {
            trap_id: 0x10,
            pc: 0x100c
        }
    );
    assert_eq!(vm.registers()[0], 0);
}

#[test]
fn signed_division_wraps_and_zero_division_is_defined() {
    let image = program(&[0x1061_1000, 0x10a1_0000, 0xe400_0010]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    vm.registers[1] = i32::MIN as u32;
    vm.registers[2] = u32::MAX;
    vm.run(10, &Halt::new()).unwrap();
    assert_eq!(vm.registers()[3], i32::MIN as u32);
    assert_eq!(vm.registers()[5], 0);
}

#[test]
fn events_preserve_machine_state_and_finish_does_not_erase_outputs() {
    let image = program(&[0xe400_0010]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    vm.begin_event(BdPlusEvent::Start).unwrap();
    assert_eq!(
        vm.begin_event(BdPlusEvent::Shutdown),
        Err(BdPlusError::EventInProgress)
    );
    vm.run(2, &Halt::new()).unwrap();
    vm.set_register(1, 42).unwrap();
    vm.set_register(28, 0x1140).unwrap();
    vm.finish_event().unwrap();
    assert_eq!(vm.registers()[1], 42);
    assert_eq!(vm.pc(), 0x1140);
    let idle_pc = vm.pc();
    vm.set_register(0, 99).unwrap();
    assert_eq!(vm.registers()[0], 0);
    assert!(vm.set_register(32, 1).is_err());
    vm.memory_mut().write(0x10, &[0xa5; 16]).unwrap();
    vm.memory_mut().write(0x20, &[0xff; 8]).unwrap();
    let event = BdPlusEvent::ComputeSp {
        clip_id: 7,
        segment: 3,
    };
    vm.begin_event(event).unwrap();
    assert_eq!(vm.registers()[28], idle_pc);
    assert_eq!(vm.memory().load_u32(0), 0x220);
    assert_eq!(vm.memory().load_u32(8), 7);
    assert_eq!(vm.memory().load_u32(12), 3);
    assert_eq!(vm.memory().read(0x20, 8).unwrap(), &[0; 8]);
    vm.run(2, &Halt::new()).unwrap();
    vm.memory_mut().write(0x20, &[0x5a; 8]).unwrap();
    assert_eq!(vm.finish_event().unwrap(), Some(event));
    assert_eq!(vm.pc(), idle_pc);
    assert_eq!(vm.memory().read(0x10, 16).unwrap(), &[0xa5; 16]);
    assert_eq!(vm.memory().read(0x20, 8).unwrap(), &[0x5a; 8]);
}

#[test]
fn watchdog_yields_and_resets_without_discarding_machine_state() {
    let image = program(&[0xb3ff_fffc]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    vm.watchdog = 5;
    assert_eq!(vm.run(10, &Halt::new()).unwrap(), VmBoundary::Watchdog);
    assert_eq!(vm.instructions(), 3);
    assert_eq!(vm.watchdog, 4000);
    assert_eq!(vm.pc(), 0x1000);
    assert_eq!(vm.run(2000, &Halt::new()).unwrap(), VmBoundary::Watchdog);
    assert_eq!(vm.instructions(), 2003);
}

/// Local copyrighted fixture is never committed; opt in with an explicit path.
/// Pins the first independently captured Cast Away trap and all its arguments.
#[test]
#[ignore = "requires FREEMKV_BDPLUS_SVM pointing to the local Cast Away capture"]
fn cast_away_first_trap_matches_independent_capture() {
    let path = std::env::var_os("FREEMKV_BDPLUS_SVM").expect("local SVM path");
    let bytes = std::fs::read(path).unwrap();
    let image = Svm::parse(bytes).unwrap();
    let mut vm = BdPlusVm::load(&image).unwrap();
    let boundary = vm.run(10000, &Halt::new()).unwrap();
    assert_eq!(
        boundary,
        VmBoundary::Trap {
            trap_id: 0x110,
            pc: 111324
        }
    );
    // The independent trace counts ordinary instructions but excludes traps.
    // Our retired-instruction counter includes each yielding trap instruction.
    assert_eq!(vm.instructions(), 418);
    assert_eq!(
        (0..5).map(|i| vm.argument(i)).collect::<Vec<_>>(),
        [0x1ad3c, 0x5550, 1, 0x5570, 7]
    );
    vm.resume_trap(Some(0x8000_0001)).unwrap();
    assert_eq!(
        vm.run(10000, &Halt::new()).unwrap(),
        VmBoundary::Trap {
            trap_id: 0x110,
            pc: 111324
        }
    );
    assert_eq!(vm.instructions(), 476);
    assert_eq!(vm.argument(4), 8);
    vm.resume_trap(Some(0x8000_0001)).unwrap();
    assert_eq!(
        vm.run(10000, &Halt::new()).unwrap(),
        VmBoundary::Trap {
            trap_id: 0x520,
            pc: 111340
        }
    );
    assert_eq!(vm.instructions(), 840);
    assert_eq!(
        (0..4).map(|i| vm.argument(i)).collect::<Vec<_>>(),
        [1, 1, 0x21a0, 0x1eff84]
    );
}

#[test]
fn immediate_arithmetic_distinguishes_signed_and_zero_extended_operands() {
    let image = program(&[
        0x6020_fffc, // ADDI r1,r0,-4
        0x6440_fffc, // ADDIE r2,r0,65532
        0x6860_fffc, // SUBI r3,r0,-4
        0x6c80_fffc, // SUBIE r4,r0,65532
        0xe400_0010,
    ]);
    let mut vm = BdPlusVm::load(&image).unwrap();
    vm.run(10, &Halt::new()).unwrap();
    assert_eq!(&vm.registers()[1..5], &[0xffff_fffc, 65532, 4, 0xffff_0004]);
}

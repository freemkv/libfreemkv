use super::*;

#[test]
fn cpu_wraps_and_aligns_big_endian_accesses() {
    let mut memory = BdPlusMemory::default();
    memory.store_u32(0x40_0003, 0x1234_5678);
    assert_eq!(memory.read(0, 4).unwrap(), [0x12, 0x34, 0x56, 0x78]);
    assert_eq!(memory.load_u16(0x80_0003), 0x5678);
    memory.store_u8(u32::MAX, 0xab);
    assert_eq!(memory.load_u8(0x3f_ffff), 0xab);
    memory.store_u16(u32::MAX, 0xcdef);
    assert_eq!(memory.load_u32(0x3f_ffff), 0x0000_cdef);
}

#[test]
fn host_ranges_never_wrap_and_failed_writes_are_atomic() {
    let mut memory = BdPlusMemory::default();
    memory.store_u8(0x3f_ffff, 42);
    assert!(memory.write(0x3f_ffff, &[1, 2]).is_err());
    assert_eq!(memory.load_u8(0x3f_ffff), 42);
    assert!(memory.read(u32::MAX, usize::MAX).is_err());
    assert!(memory.read(0x40_0000, 0).unwrap().is_empty());
    assert!(memory.read(0x40_0001, 0).is_err());
}

#[test]
fn debug_does_not_print_program_or_secrets() {
    let mut memory = BdPlusMemory::default();
    memory.write(0, b"player-secret").unwrap();
    assert!(!format!("{memory:?}").contains("player-secret"));
}

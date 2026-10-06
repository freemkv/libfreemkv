//! CDB byte-layout tests grounded in MMC-6 / SPC-4 field definitions.
//! A wrong shift or byte index silently sends a malformed command to
//! the drive (wrong LBA, wrong length) — the 0.31.0 class of bug.
use super::*;

#[test]
fn read10_fua_opcode_and_fua_bit() {
    // MMC-6 READ(10): byte 0 = opcode 0x28. FUA is byte 1 bit 3
    // (0x08) per SBC-3 §5.20. Doc explicitly sets FUA.
    let cdb = build_read10_fua(0, 1);
    assert_eq!(cdb[0], SCSI_READ_10);
    assert_eq!(cdb[0], 0x28);
    assert_eq!(cdb[1], 0x08, "FUA bit (byte1 bit3) must be set");
}

#[test]
fn read10_fua_lba_big_endian_bytes_2_5() {
    // READ(10) LOGICAL BLOCK ADDRESS occupies bytes 2..5, big-endian
    // (MSB first). Use a value with all four bytes distinct so a
    // swapped shift is caught.
    let cdb = build_read10_fua(0x1122_3344, 0);
    assert_eq!(cdb[2], 0x11);
    assert_eq!(cdb[3], 0x22);
    assert_eq!(cdb[4], 0x33);
    assert_eq!(cdb[5], 0x44);
}

#[test]
fn read10_fua_transfer_length_big_endian_bytes_7_8() {
    // READ(10) TRANSFER LENGTH is bytes 7..8 big-endian (number of
    // logical blocks). Byte 6 (group number) and byte 9 (control)
    // are zero.
    let cdb = build_read10_fua(0, 0xABCD);
    assert_eq!(cdb[6], 0x00, "byte 6 group number must be 0");
    assert_eq!(cdb[7], 0xAB, "transfer length MSB");
    assert_eq!(cdb[8], 0xCD, "transfer length LSB");
    assert_eq!(cdb[9], 0x00, "byte 9 control must be 0");
}

#[test]
fn read10_fua_max_lba_and_count() {
    // u32::MAX LBA and u16::MAX count must encode without truncation
    // or panic (overflow on debug builds would be a bug).
    let cdb = build_read10_fua(u32::MAX, u16::MAX);
    assert_eq!(&cdb[2..6], &[0xFF, 0xFF, 0xFF, 0xFF]);
    assert_eq!(&cdb[7..9], &[0xFF, 0xFF]);
}

#[test]
fn read_buffer_cdb_layout() {
    // MMC-6 READ BUFFER (0x3C): byte0 opcode, byte1 mode, byte2
    // buffer id, bytes 3..5 buffer offset (big-endian 24-bit),
    // bytes 6..8 allocation length (big-endian 24-bit), byte9 control.
    let cdb = build_read_buffer(0x02, 0xF1, 0x010203, 0x040506);
    assert_eq!(cdb[0], SCSI_READ_BUFFER);
    assert_eq!(cdb[1], 0x02, "mode");
    assert_eq!(cdb[2], 0xF1, "buffer id");
    assert_eq!(&cdb[3..6], &[0x01, 0x02, 0x03], "offset 24-bit BE");
    assert_eq!(&cdb[6..9], &[0x04, 0x05, 0x06], "length 24-bit BE");
    assert_eq!(cdb[9], 0x00, "control");
}

#[test]
fn read_buffer_offset_truncates_to_24_bits_low() {
    // The CDB offset field is 24-bit; the builder takes the low three
    // bytes of the u32, so a non-zero top byte must not leak into
    // the encoded field. Documents the actual wire contract.
    let cdb = build_read_buffer(0, 0, 0xFF01_0203, 0);
    assert_eq!(&cdb[3..6], &[0x01, 0x02, 0x03]);
}

#[test]
fn set_cd_speed_cdb_layout() {
    // MMC-6 SET CD SPEED (0xBB): byte0 opcode, bytes 2..3 read speed
    // (big-endian kB/s), bytes 4..5 write speed = 0xFFFF (no change /
    // max). Use a distinct read speed to verify byte order.
    let cdb = build_set_cd_speed(0x1234);
    assert_eq!(cdb[0], SCSI_SET_CD_SPEED);
    assert_eq!(cdb[2], 0x12, "read speed MSB");
    assert_eq!(cdb[3], 0x34, "read speed LSB");
    assert_eq!(cdb[4], 0xFF, "write speed bytes set to 0xFFFF");
    assert_eq!(cdb[5], 0xFF);
}

#[test]
fn set_cd_speed_zero_means_drive_default() {
    // read_speed 0 encodes as 0x0000 (MMC: "use drive default").
    let cdb = build_set_cd_speed(0);
    assert_eq!(cdb[2], 0x00);
    assert_eq!(cdb[3], 0x00);
}

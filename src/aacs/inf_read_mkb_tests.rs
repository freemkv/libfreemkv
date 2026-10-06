use super::*;
use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE, ScsiResult, ScsiTransport};

/// A drive that answers READ DISC STRUCTURE format 0x83 from a scripted set
/// of packs and records every CDB it was handed.
struct MkbDrive {
    /// One entry per pack: the pack's MKB payload bytes.
    packs: Vec<Vec<u8>>,
    cdbs: Vec<Vec<u8>>,
}

impl ScsiTransport for MkbDrive {
    fn execute(
        &mut self,
        cdb: &[u8],
        _direction: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> crate::error::Result<ScsiResult> {
        self.cdbs.push(cdb.to_vec());
        // Pack number is carried in the CDB address field (bytes 2..6),
        // MMC-6 READ DISC STRUCTURE.
        let pack = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]) as usize;
        let body = self.packs.get(pack).cloned().unwrap_or_default();
        // Header: BE16 data length (counts the 2 header bytes that follow
        // it plus the payload), reserved byte, pack count, then payload.
        let data_len = body.len() + 2;
        data[0..2].copy_from_slice(&(data_len as u16).to_be_bytes());
        data[2] = 0x00;
        data[3] = self.packs.len() as u8;
        data[4..4 + body.len()].copy_from_slice(&body);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: 4 + body.len(),
            sense: [0u8; 32],
        })
    }
}

// Pins the CONTENT: concatenated pack payloads, in pack order, byte for byte.
#[test]
fn read_mkb_from_drive_returns_the_concatenated_pack_payload() {
    let pack0: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
    let pack1: Vec<u8> = (0..300u32).map(|i| (i % 253) as u8 ^ 0xA5).collect();
    let mut drive = MkbDrive {
        packs: vec![pack0.clone(), pack1.clone()],
        cdbs: Vec::new(),
    };

    let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");

    let mut expected = pack0.clone();
    expected.extend_from_slice(&pack1);
    assert_eq!(
        mkb.len(),
        expected.len(),
        "every pack's payload must be concatenated, none dropped"
    );
    assert!(
        mkb == expected,
        "MKB bytes must be the drive's payload in pack order; first \
             mismatch at {:?}",
        (0..expected.len()).find(|&i| mkb[i] != expected[i])
    );

    // MMC-6 READ DISC STRUCTURE with the AACS MKB format code, one command
    // per pack, pack number in the address field.
    assert_eq!(drive.cdbs.len(), 2, "one command per declared pack");
    for (i, cdb) in drive.cdbs.iter().enumerate() {
        assert_eq!(cdb[0], SCSI_READ_DISC_STRUCTURE, "opcode");
        assert_eq!(cdb[7], 0x83, "AACS MKB disc-structure format code");
        assert_eq!(
            u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]),
            i as u32,
            "pack {i} must be requested by number"
        );
    }
}

// Pins the WHOLE 12-byte CDB (MMC-6 READ DISC STRUCTURE, AACS MKB format) so no field can
// drift.
#[test]
fn read_mkb_from_drive_issues_the_exact_mmc_cdb_for_each_pack() {
    let mut drive = MkbDrive {
        packs: vec![vec![0x11u8; 64], vec![0x22u8; 64], vec![0x33u8; 64]],
        cdbs: Vec::new(),
    };
    read_mkb_from_drive(&mut drive).expect("scripted drive answers");

    assert_eq!(drive.cdbs.len(), 3, "one command per declared pack");
    for (pack, cdb) in drive.cdbs.iter().enumerate() {
        let p = pack as u32;
        let expected: [u8; 12] = [
            SCSI_READ_DISC_STRUCTURE,
            0x01,
            (p >> 24) as u8,
            (p >> 16) as u8,
            (p >> 8) as u8,
            p as u8,
            0x00,
            0x83, // AACS MKB disc-structure format
            0x80, // allocation length 32772 = 0x8004, high byte
            0x04, // …low byte
            0x00,
            0x00,
        ];
        assert_eq!(
            cdb.as_slice(),
            &expected[..],
            "CDB for pack {pack} must match the MMC-6 READ DISC STRUCTURE layout"
        );
    }
}

// A full 32768-byte pack must come back whole; small payloads elsewhere never exercise this
// bound.
#[test]
fn read_mkb_from_drive_accepts_a_full_size_pack() {
    let full: Vec<u8> = (0..32768u32).map(|i| (i % 251) as u8).collect();
    let other: Vec<u8> = (0..32768u32).map(|i| (i % 241) as u8 ^ 0x5A).collect();
    // TWO maximal packs: the first-pack read and the per-pack loop carry
    // separate bounds, so both must accept a full-window payload.
    let mut drive = MkbDrive {
        packs: vec![full.clone(), other.clone()],
        cdbs: Vec::new(),
    };
    let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");
    assert_eq!(
        mkb.len(),
        65536,
        "neither maximal pack may be dropped at the size bound"
    );
    let mut expected = full.clone();
    expected.extend_from_slice(&other);
    assert!(mkb == expected, "both maximal packs' bytes must be intact");
}

/// A header-only pack inside a multi-pack MKB is a hole: fail rather than return a
/// silently shortened MKB. A single-pack header-only response stays an empty MKB.
#[test]
fn read_mkb_from_drive_zero_length_pack_contributes_nothing() {
    let mut drive = MkbDrive {
        packs: vec![Vec::new(), vec![0xABu8; 32]],
        cdbs: Vec::new(),
    };
    assert!(matches!(
        read_mkb_from_drive(&mut drive),
        Err(crate::error::Error::AacsKeyRead)
    ));
    let mut drive = MkbDrive {
        packs: vec![vec![0xABu8; 32], Vec::new()],
        cdbs: Vec::new(),
    };
    assert!(matches!(
        read_mkb_from_drive(&mut drive),
        Err(crate::error::Error::AacsKeyRead)
    ));
    let mut drive = MkbDrive {
        packs: vec![Vec::new()],
        cdbs: Vec::new(),
    };
    assert_eq!(
        read_mkb_from_drive(&mut drive).expect("single pack"),
        Vec::<u8>::new()
    );
}

// A drive-declared length past the 32772-byte buffer is a drive fault: the MKB
// would have a hole, so fail rather than return a corrupt MKB as Ok.
#[test]
fn read_mkb_from_drive_rejects_a_pack_declaring_more_than_the_buffer_holds() {
    /// Pack 0 is honest; pack 1 declares a 60000-byte payload it never sent.
    struct LyingDrive {
        honest: Vec<u8>,
    }
    impl ScsiTransport for LyingDrive {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            let pack = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
            data[3] = 2; // two packs declared
            if pack == 0 {
                let dl = self.honest.len() + 2;
                data[0..2].copy_from_slice(&(dl as u16).to_be_bytes());
                data[4..4 + self.honest.len()].copy_from_slice(&self.honest);
            } else {
                // A length far beyond the 32772-byte response buffer.
                data[0..2].copy_from_slice(&60_000u16.to_be_bytes());
            }
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 4,
                sense: [0u8; 32],
            })
        }
    }

    let mut drive = LyingDrive {
        honest: vec![0xC7u8; 256],
    };
    let err = read_mkb_from_drive(&mut drive).expect_err("a holed MKB is not Ok");
    assert_eq!(err.code(), crate::error::Error::AacsKeyRead.code());
}

/// The same over-declaration on the FIRST pack, which uses a separate bound
/// from the loop's.
#[test]
fn read_mkb_from_drive_rejects_a_first_pack_declaring_more_than_the_buffer() {
    struct LyingFirst;
    impl ScsiTransport for LyingFirst {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            data[0..2].copy_from_slice(&60_000u16.to_be_bytes());
            data[3] = 1;
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 4,
                sense: [0u8; 32],
            })
        }
    }
    let err = read_mkb_from_drive(&mut LyingFirst).expect_err("over-declared first pack");
    assert_eq!(err.code(), crate::error::Error::AacsKeyRead.code());
}

/// A drive declaring a payload but transferring fewer bytes (zero-filled tail) is a
/// fault; so is a header-only pack after a real first pack (a hole).
#[test]
fn read_mkb_from_drive_rejects_short_transfer_and_holed_pack() {
    struct Scripted {
        declared: [u16; 2],
        xfer: usize,
    }
    impl ScsiTransport for Scripted {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            let pack = cdb[5] as usize;
            data[0..2].copy_from_slice(&self.declared[pack].to_be_bytes());
            data[3] = 2;
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: if pack == 0 { self.xfer } else { 4 },
                sense: [0u8; 32],
            })
        }
    }
    let short = read_mkb_from_drive(&mut Scripted {
        declared: [102, 2],
        xfer: 4,
    })
    .expect_err("short transfer");
    assert_eq!(short.code(), crate::error::Error::AacsKeyRead.code());
    let hole = read_mkb_from_drive(&mut Scripted {
        declared: [102, 0],
        xfer: 104,
    })
    .expect_err("holed pack");
    assert_eq!(hole.code(), crate::error::Error::AacsKeyRead.code());
}

/// A single-pack disc still yields that pack's bytes — the common case, and
/// the one where a body returning an empty vector looks most plausible.
#[test]
fn read_mkb_from_drive_returns_a_single_packs_payload() {
    let pack: Vec<u8> = (0..1024u32).map(|i| (i * 7 % 256) as u8).collect();
    let mut drive = MkbDrive {
        packs: vec![pack.clone()],
        cdbs: Vec::new(),
    };
    let mkb = read_mkb_from_drive(&mut drive).expect("scripted drive answers");
    assert_eq!(mkb.len(), pack.len(), "single pack payload length");
    assert!(mkb == pack, "single pack payload bytes");
}

/// A drive that reports a header-only response (`data_len < 2`) has no MKB
/// to give. That must be an EMPTY vec, not a partial one — the distinction
/// matters because the AACS paths treat a non-empty MKB as parseable.
#[test]
fn read_mkb_from_drive_empty_response_is_empty() {
    struct NoMkb;
    impl ScsiTransport for NoMkb {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            data[0..2].copy_from_slice(&0u16.to_be_bytes());
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 4,
                sense: [0u8; 32],
            })
        }
    }
    let mkb = read_mkb_from_drive(&mut NoMkb).expect("no-MKB drive still returns Ok");
    assert!(
        mkb.is_empty(),
        "a header-only response carries no MKB bytes"
    );
}

/// A header the drive never transferred, or a zero-length first pack of a multi-pack MKB,
/// is a drive fault: `AacsKeyRead`, not an empty MKB.
#[test]
fn read_mkb_from_drive_untransferred_header_and_holed_first_pack_fail() {
    struct Scripted {
        xfer: usize,
        num_packs: u8,
    }
    impl ScsiTransport for Scripted {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            data[0..2].copy_from_slice(&0u16.to_be_bytes());
            data[3] = self.num_packs;
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: self.xfer,
                sense: [0u8; 32],
            })
        }
    }
    for (xfer, num_packs) in [(0, 0), (4, 2)] {
        assert!(
            matches!(
                read_mkb_from_drive(&mut Scripted { xfer, num_packs }),
                Err(crate::error::Error::AacsKeyRead)
            ),
            "xfer {xfer}, packs {num_packs}"
        );
    }
}

/// A transport failure on the FIRST pack must propagate as an error — the
/// MKB is the root of the whole AACS ladder, so an unreadable one cannot be
/// downgraded to "an MKB with no records".
#[test]
fn read_mkb_from_drive_propagates_the_first_pack_failure() {
    struct DeadDrive;
    impl ScsiTransport for DeadDrive {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            Err(crate::error::Error::ScsiError {
                opcode: SCSI_READ_DISC_STRUCTURE,
                status: 0x02,
                sense: None,
            })
        }
    }
    assert!(
        read_mkb_from_drive(&mut DeadDrive).is_err(),
        "an unreadable MKB must surface as an error, not an empty MKB"
    );
}

/// A transport failure on a NON-first pack must ALSO propagate. Before the
/// fix the per-pack loop tested `.is_ok()` and dropped the error, so a
/// mid-walk failure silently TRUNCATED the MKB and returned the partial
/// pack-0 data as Ok. Pack 0 succeeds, pack 1 errors → the whole read errors.
#[test]
fn read_mkb_from_drive_propagates_a_mid_walk_pack_failure() {
    struct FlakyDrive;
    impl ScsiTransport for FlakyDrive {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::error::Result<ScsiResult> {
            let pack = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
            if pack == 0 {
                // Honest first pack: 64 payload bytes, TWO packs declared so
                // the loop goes on to request pack 1.
                let body = [0x5Au8; 64];
                data[0..2].copy_from_slice(&((body.len() + 2) as u16).to_be_bytes());
                data[2] = 0x00;
                data[3] = 2;
                data[4..4 + body.len()].copy_from_slice(&body);
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 4 + body.len(),
                    sense: [0u8; 32],
                })
            } else {
                // Pack 1 read fails mid-walk.
                Err(crate::error::Error::ScsiError {
                    opcode: SCSI_READ_DISC_STRUCTURE,
                    status: 0x02,
                    sense: None,
                })
            }
        }
    }
    assert!(
        read_mkb_from_drive(&mut FlakyDrive).is_err(),
        "a transport error on pack 1 must surface, not truncate the MKB to pack 0"
    );
}

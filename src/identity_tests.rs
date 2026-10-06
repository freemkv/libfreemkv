use super::*;
use crate::scsi::{ScsiResult, ScsiTransport};

// Reports a bytes_transferred larger than the caller's buffer — models a drive that lies
// about its transfer count.
struct OversizedCountTransport;

impl ScsiTransport for OversizedCountTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        buf: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        // Fill plausible ASCII so the from_utf8_lossy paths run.
        for b in buf.iter_mut() {
            *b = b'A';
        }
        // INQUIRY (0x12): honest count. GET CONFIGURATION (0x46): lie.
        let bytes_transferred = if cdb.first() == Some(&0x12) {
            buf.len()
        } else {
            buf.len() + 4096
        };
        Ok(ScsiResult {
            status: 0,
            bytes_transferred,
            sense: [0u8; 32],
        })
    }
}

#[test]
fn from_drive_clamps_oversized_bytes_transferred() {
    // Must not panic despite the transport reporting a transfer count
    // far beyond the 256-byte GET CONFIGURATION buffers.
    let mut t = OversizedCountTransport;
    let id = DriveId::from_drive(&mut t).expect("from_drive must not error");
    // raw_gc_010c is clamped to the 256-byte buffer, never the lie.
    assert_eq!(id.raw_gc_010c.len(), 256);
}

// Answers INQUIRY with `inq` (reporting `inq_count` bytes) and fails the GET
// CONFIGURATION whose feature byte is `halt_feature` with `Halted`.
struct ScriptTransport {
    inq: Vec<u8>,
    inq_count: usize,
    halt_feature: Option<u8>,
}

impl ScsiTransport for ScriptTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        buf: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        if cdb[0] == 0x46 && Some(cdb[3]) == self.halt_feature {
            return Err(crate::error::Error::Halted);
        }
        let mut n = 0;
        if cdb[0] == 0x12 {
            n = self.inq.len().min(buf.len());
            buf[..n].copy_from_slice(&self.inq[..n]);
            n = self.inq_count;
        }
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: n,
            sense: [0u8; 32],
        })
    }
}

fn script(halt_feature: Option<u8>) -> ScriptTransport {
    let mut inq = vec![0u8; 96];
    inq[0] = 0x05;
    inq[8..16].copy_from_slice(b"VENDOR  ");
    ScriptTransport {
        inq,
        inq_count: 96,
        halt_feature,
    }
}

// A stop during either best-effort GET CONFIGURATION probe aborts identification.
#[test]
fn from_drive_propagates_halted_from_both_gc_probes() {
    for feature in [0x0C, 0x08] {
        let mut t = script(Some(feature));
        let r = DriveId::from_drive(&mut t);
        assert!(
            matches!(r, Err(crate::error::Error::Halted)),
            "feature {feature:#x}: {r:?}"
        );
    }
    assert!(DriveId::from_drive(&mut script(None)).is_ok());
}

// A lying INQUIRY transfer count is not trusted. Pin only: `truncate` past the
// length is a no-op, so the `.min` clamp is behaviour-neutral.
#[test]
fn from_drive_clamps_oversized_inquiry_count() {
    let mut t = script(None);
    t.inq_count = 96 + 4096;
    let id = DriveId::from_drive(&mut t).expect("from_drive must not error");
    assert_eq!(id.raw_inquiry.len(), 96);
}

// Control characters from the drive never reach the identity strings.
#[test]
fn drive_text_control_characters_are_replaced() {
    let mut t = script(None);
    t.inq[8..16].copy_from_slice(b"A\x1b[31mZ ");
    let id = DriveId::from_drive(&mut t).expect("from_drive");
    assert_eq!(id.vendor_id, "A?[31mZ ");
    assert_eq!(gc_text(b"SN\r\n1\x1b "), "SN??1?");
    // Trailing padding is trimmed before sanitising, not turned into '?'.
    assert_eq!(gc_text(b"SN1\r\n\t\0 "), "SN1");
}

// INQUIRY byte 0: low 5 bits are the peripheral device type (5 = MMC), the
// high 3 the qualifier. The shared optical filter behind every find_drive(s).
#[test]
fn is_optical_reads_the_peripheral_device_type() {
    let with_byte0 = |b0: u8| DriveId::from_inquiry(&[b0; 36], "");
    assert!(with_byte0(0x05).is_optical());
    assert!(with_byte0(0x25).is_optical(), "qualifier bits masked off");
    assert!(!with_byte0(0x00).is_optical(), "direct-access disk");
    assert!(!with_byte0(0x01).is_optical(), "tape");
    assert!(!with_byte0(0x15).is_optical(), "type 0x15, not 0x05");
    assert!(
        !DriveId::from_inquiry(&[], "").is_optical(),
        "no INQUIRY data"
    );
}

#[test]
fn test_bu40n_identity() {
    let mut inquiry = vec![0u8; 96];
    inquiry[4] = 0x5B;
    inquiry[8..16].copy_from_slice(b"HL-DT-ST");
    inquiry[16..32].copy_from_slice(b"BD-RE BU40N     ");
    inquiry[32..36].copy_from_slice(b"1.03");
    inquiry[36..43].copy_from_slice(b"NM00000");

    let id = DriveId::from_inquiry(&inquiry, "211810241934");
    assert_eq!(id.vendor_id.trim(), "HL-DT-ST");
    assert_eq!(id.product_id.trim(), "BD-RE BU40N");
    assert_eq!(id.product_revision.trim(), "1.03");
    assert_eq!(id.vendor_specific.trim(), "NM00000");
    assert_eq!(id.firmware_date, "211810241934");
    assert_eq!(id.match_key(), "HL-DT-ST|BD-RE BU40N|1.03|NM00000");
}

#[test]
fn test_pioneer_identity() {
    let mut inquiry = vec![0u8; 96];
    inquiry[4] = 0x5B;
    inquiry[8..16].copy_from_slice(b"PIONEER ");
    inquiry[16..32].copy_from_slice(b"BD-RW   BDR-S09 ");
    inquiry[32..36].copy_from_slice(b"1.34");
    inquiry[36..43].copy_from_slice(b" 16/04/");

    let id = DriveId::from_inquiry(&inquiry, "201604250000");
    assert_eq!(id.vendor_id.trim(), "PIONEER");
    assert_eq!(id.product_id.trim(), "BD-RW   BDR-S09");
    assert_eq!(id.product_revision.trim(), "1.34");
    assert_eq!(id.vendor_specific.trim(), "16/04/");
    assert_eq!(id.firmware_date, "201604250000");
}

// ── New comprehensive tests ────────────────────────────────────────────────

// A short/empty INQUIRY data phase must fail the probe, not present as a blank drive.
#[test]
fn inquiry_with_a_short_data_phase_fails_instead_of_reporting_a_blank_drive() {
    /// GOOD status, no sense, and only `n` bytes written.
    struct ShortInquiry(usize);
    impl ScsiTransport for ShortInquiry {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            _buf: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Ok(ScsiResult {
                status: 0,
                sense: [0u8; 32],
                bytes_transferred: self.0,
            })
        }
    }

    // Empty data phase — the case that made a real drive disappear.
    assert!(matches!(
        DriveId::from_drive(&mut ShortInquiry(0)),
        Err(crate::error::Error::DriveInquiryShort)
    ));
    // One byte short of the SPC-4 standard 36-byte header.
    assert!(matches!(
        DriveId::from_drive(&mut ShortInquiry(35)),
        Err(crate::error::Error::DriveInquiryShort)
    ));
    // Exactly the standard length is acceptable: the optional
    // vendor-specific tail past byte 36 is allowed to be absent.
    assert!(DriveId::from_drive(&mut ShortInquiry(36)).is_ok());
}

#[test]
fn ascii_field_short_buffer_returns_empty() {
    // Buffer of length 5: start=8 is beyond the end → empty string.
    let buf = vec![0u8; 5];
    let result = ascii_field(&buf, 8, 16); // SPC-4 vendor ID range
    assert!(result.is_empty(), "short buffer must yield empty string");
}

/// ascii_field with a buffer that covers start but not end is clamped.
/// Spec: `ascii_field` documents "clamps to data.len()".
/// Mutation: using `end` directly without `min(data.len())` panics here.
#[test]
fn ascii_field_partial_buffer_is_clamped_not_panicked() {
    // Buffer of length 12: vendor_id range is [8..16], but only [8..12] present.
    let mut buf = vec![0u8; 12];
    buf[8..12].copy_from_slice(b"SONY");
    let result = ascii_field(&buf, 8, 16);
    // Must not panic; the returned string holds what we wrote.
    assert_eq!(result, "SONY");
}

/// from_inquiry extracts the product_id field from INQUIRY bytes [16:32].
/// Spec: SPC-4 §6.4.2 — PRODUCT IDENTIFICATION at offset 16, length 16.
/// Mutation: shifting the product_id slice to [8:24] makes this fail.
#[test]
fn from_inquiry_extracts_product_id_at_offset_16() {
    let mut inquiry = vec![0u8; 96];
    // Leave vendor_id (8..16) as zeros, write product_id at 16..32.
    inquiry[16..32].copy_from_slice(b"BD-RW   BDR-209M");
    let id = DriveId::from_inquiry(&inquiry, "");
    assert_eq!(
        id.product_id, "BD-RW   BDR-209M",
        "product_id must come from INQUIRY bytes 16..32 (SPC-4 §6.4.2)"
    );
}

/// from_inquiry extracts product_revision from INQUIRY bytes [32:36].
/// Spec: SPC-4 §6.4.2 — PRODUCT REVISION LEVEL at offset 32, length 4.
/// Mutation: reading revision from [36:40] produces the wrong value.
#[test]
fn from_inquiry_extracts_revision_at_offset_32() {
    let mut inquiry = vec![0u8; 96];
    inquiry[32..36].copy_from_slice(b"1.53");
    let id = DriveId::from_inquiry(&inquiry, "");
    assert_eq!(
        id.product_revision, "1.53",
        "product_revision must come from INQUIRY bytes 32..36 (SPC-4 §6.4.2)"
    );
}

/// from_inquiry extracts vendor_specific from INQUIRY bytes [36:43].
/// Spec: SPC-4 §6.4.2 — VENDOR SPECIFIC at offset 36, length 8.
/// Mutation: reading vendor_specific from [32:39] returns the revision instead.
#[test]
fn from_inquiry_extracts_vendor_specific_at_offset_36() {
    let mut inquiry = vec![0u8; 96];
    inquiry[36..43].copy_from_slice(b"MM01234");
    let id = DriveId::from_inquiry(&inquiry, "");
    assert_eq!(
        id.vendor_specific, "MM01234",
        "vendor_specific must come from INQUIRY bytes 36..43 (SPC-4 §6.4.2)"
    );
}

/// from_inquiry stores the raw inquiry bytes in raw_inquiry unchanged.
/// Mutation: copying only a slice of inquiry into raw_inquiry truncates it.
#[test]
fn from_inquiry_stores_raw_inquiry() {
    let mut inquiry = vec![0u8; 96];
    inquiry[8..16].copy_from_slice(b"TESTDRVR");
    let id = DriveId::from_inquiry(&inquiry, "");
    assert_eq!(
        id.raw_inquiry, inquiry,
        "raw_inquiry must preserve the full 96-byte buffer"
    );
}

// Guard is `data.len() > start`, strictly greater; len == start has no byte at that offset,
// so it must still yield empty.
#[test]
fn ascii_field_boundary_len_equals_start_is_empty() {
    let buf = vec![0u8; 8];
    assert_eq!(ascii_field(&buf, 8, 16), "");
}

/// One byte past the boundary: `data.len() == start + 1` must extract
/// that single byte (clamped to `end`), proving the guard is `>` and not
/// off by one in the other direction.
#[test]
fn ascii_field_boundary_len_one_past_start_extracts_one_byte() {
    let mut buf = vec![0u8; 9];
    buf[8] = b'X';
    assert_eq!(ascii_field(&buf, 8, 16), "X");
}

// `Display` renders the four trimmed identity fields space-separated — the human-readable
// counterpart of `match_key`'s pipe-separated form.
#[test]
fn display_formats_trimmed_fields_space_separated() {
    let mut inquiry = vec![0u8; 96];
    inquiry[8..16].copy_from_slice(b"PIONEER ");
    inquiry[16..32].copy_from_slice(b"BD-RW   BDR-S09 ");
    inquiry[32..36].copy_from_slice(b"1.34");
    inquiry[36..43].copy_from_slice(b" 16/04/");
    let id = DriveId::from_inquiry(&inquiry, "201604250000");
    assert_eq!(id.to_string(), "PIONEER BD-RW   BDR-S09 1.34 16/04/");
}

// Transport whose GET CONFIGURATION responses report an exact, caller-chosen
// bytes_transferred for each GC feature, pinning the `> 12` boundary guards. INQUIRY always
// succeeds.
struct FixedGcCountTransport {
    firmware_bytes: usize,
    serial_bytes: usize,
}

impl ScsiTransport for FixedGcCountTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        buf: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        for b in buf.iter_mut() {
            *b = b'Z';
        }
        // Well-formed GC header + descriptor for the requested feature, so
        // only the transfer count bounds the decoded field.
        if cdb.first() == Some(&0x46) && buf.len() >= 12 {
            let dl = (buf.len() - 4) as u32;
            buf[0..4].copy_from_slice(&dl.to_be_bytes());
            buf[4..8].fill(0);
            buf[8..10].copy_from_slice(&cdb[2..4]);
            buf[10] = 0;
            buf[11] = u8::try_from(buf.len() - 12).unwrap_or(u8::MAX);
        }
        let bytes_transferred = match cdb.first() {
            Some(&0x12) => buf.len(),
            Some(&0x46) if cdb[3] == 0x0C => self.firmware_bytes,
            Some(&0x46) if cdb[3] == 0x08 => self.serial_bytes,
            _ => buf.len(),
        };
        Ok(ScsiResult {
            status: 0,
            bytes_transferred,
            sense: [0u8; 32],
        })
    }
}

// `end > 12` in the firmware-date branch is strict: count == 12 covers bytes 0..12, none of
// which is the date, so it must report empty.
#[test]
fn from_drive_firmware_date_boundary_exactly_12_is_empty() {
    let mut t = FixedGcCountTransport {
        firmware_bytes: 12,
        serial_bytes: 0,
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.firmware_date, "");
}

/// One byte past the boundary (`bytes_transferred == 13`) must extract
/// exactly the one available date byte (offset 12), proving the guard
/// is `>` and the slice end is clamped to `end`, not always to 24.
#[test]
fn from_drive_firmware_date_boundary_13_extracts_one_byte() {
    let mut t = FixedGcCountTransport {
        firmware_bytes: 13,
        serial_bytes: 0,
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.firmware_date, "Z");
}

/// Same `> 12` boundary for the serial-number branch: exactly 12
/// transferred bytes must yield an empty serial.
#[test]
fn from_drive_serial_boundary_exactly_12_is_empty() {
    let mut t = FixedGcCountTransport {
        firmware_bytes: 0,
        serial_bytes: 12,
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.serial_number, "");
}

/// One byte past the serial boundary extracts exactly that byte.
#[test]
fn from_drive_serial_boundary_13_extracts_one_byte() {
    let mut t = FixedGcCountTransport {
        firmware_bytes: 0,
        serial_bytes: 13,
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.serial_number, "Z");
}

// A GET CONFIGURATION (RT=10b) reply for `feature`: 8-byte header whose Data
// Length covers exactly the descriptor, then the 4-byte feature header.
fn gc_reply(feature: u16, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0u8; 12];
    v[0..4].copy_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
    v[8..10].copy_from_slice(&feature.to_be_bytes());
    v[11] = payload.len() as u8;
    v.extend_from_slice(payload);
    v
}

// Answers INQUIRY honestly and each GET CONFIGURATION with a scripted reply,
// reporting the WHOLE buffer as transferred (resid unreported by the LLD).
struct GcReplyTransport {
    firmware: Vec<u8>,
    serial: Vec<u8>,
}

impl ScsiTransport for GcReplyTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        buf: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        buf.fill(0);
        let reply: &[u8] = match (cdb[0], cdb[3]) {
            (0x46, 0x0C) => &self.firmware,
            (0x46, 0x08) => &self.serial,
            _ => &[],
        };
        let n = reply.len().min(buf.len());
        buf[..n].copy_from_slice(&reply[..n]);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: buf.len(),
            sense: [0u8; 32],
        })
    }
}

// MMC-6 §5.3: the field ends at the feature's Additional Length / the header
// Data Length, not at a transfer count the transport may over-report.
#[test]
fn from_drive_honours_gc_data_length_and_additional_length() {
    let mut t = GcReplyTransport {
        firmware: gc_reply(0x010C, b"201604250000\0\0\0\0"),
        serial: gc_reply(0x0108, b"ABCD1234"),
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.serial_number, "ABCD1234");
    assert_eq!(id.firmware_date, "201604250000");
    assert_eq!(id.raw_gc_010c.len(), 28, "header + 20-byte descriptor");
}

// MMC-6 §5.3.10: the date is CCYYMMDDHHMI, 12 characters; longer vendor
// payload in the descriptor is not part of it.
#[test]
fn from_drive_firmware_date_is_capped_at_12_characters() {
    let mut t = GcReplyTransport {
        firmware: gc_reply(0x010C, b"201604250000EXTRA"),
        serial: gc_reply(0x0108, b"ABCD1234"),
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.firmware_date, "201604250000");
}

// A drive lacking the feature answers RT=10b with only the 8-byte header.
#[test]
fn from_drive_absent_or_foreign_gc_feature_yields_empty_fields() {
    let header_only = vec![0, 0, 0, 4, 0, 0, 0, 0];
    let mut t = GcReplyTransport {
        firmware: header_only.clone(),
        serial: header_only,
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.serial_number, "", "absent 0108h is no serial");
    assert_eq!(id.firmware_date, "", "absent 010Ch is no date");

    // A descriptor for some OTHER feature is not the one asked for.
    let mut t = GcReplyTransport {
        firmware: gc_reply(0x0001, b"201604250000"),
        serial: gc_reply(0x0001, b"ABCD1234"),
    };
    let id = DriveId::from_drive(&mut t).unwrap();
    assert_eq!(id.serial_number, "");
    assert_eq!(id.firmware_date, "");
}

#[test]
fn from_drive_gc_failure_yields_empty_firmware_date() {
    struct GcFailTransport;
    impl ScsiTransport for GcFailTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            buf: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            if cdb.first() == Some(&0x12) {
                // INQUIRY succeeds with a plausible response.
                buf[8..16].copy_from_slice(b"TESTDRV ");
                buf[16..32].copy_from_slice(b"FAKE DRIVE MODEL");
                buf[32..36].copy_from_slice(b"0001");
                buf[36..43].copy_from_slice(b"X000001");
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: buf.len(),
                    sense: [0u8; 32],
                })
            } else {
                // GET CONFIGURATION fails.
                Err(crate::error::Error::ScsiError {
                    opcode: cdb[0],
                    status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
                    sense: None,
                })
            }
        }
    }
    let mut t = GcFailTransport;
    let id = DriveId::from_drive(&mut t).expect("from_drive must succeed despite GC failure");
    assert!(
        id.firmware_date.is_empty(),
        "firmware_date must be empty when GC fails"
    );
    assert!(
        id.raw_gc_010c.is_empty(),
        "raw_gc_010c must be empty when GC fails"
    );
}

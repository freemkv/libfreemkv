//! Drive identification — match drives to profiles by SCSI response fields.
//!
//! Field names follow SPC-4 (INQUIRY) and MMC-6 (GET CONFIGURATION) standards.
//! No proprietary fingerprints or encrypted lookups — open matching only.
//!
//! References:
//!   SPC-4 §6.4.2 — Standard INQUIRY data
//!   MMC-6 §5.3.10 — Feature 010Ch (Firmware Information)

use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport};

/// Drive identity from standard SCSI commands.
///
/// All field names follow the SCSI standards:
///   - SPC-4 §6.4.2 for INQUIRY fields
///   - MMC-6 §5.3.10 for Firmware Information
#[derive(Debug, Clone)]
pub struct DriveId {
    /// T10 VENDOR IDENTIFICATION — INQUIRY bytes [8:16]
    /// SPC-4 §6.4.2
    pub vendor_id: String,

    /// PRODUCT IDENTIFICATION — INQUIRY bytes [16:32]
    /// SPC-4 §6.4.2
    pub product_id: String,

    /// PRODUCT REVISION LEVEL — INQUIRY bytes [32:36]
    /// SPC-4 §6.4.2
    pub product_revision: String,

    /// VENDOR SPECIFIC — INQUIRY bytes [36:43]
    /// SPC-4 §6.4.2
    /// Content varies by vendor: firmware type code (MTK), date (Pioneer), etc.
    pub vendor_specific: String,

    /// Firmware Creation Date — GET CONFIGURATION Feature 010Ch
    /// MMC-6 §5.3.10
    /// Format: CCYYMMDDHHMI (12 ASCII characters)
    pub firmware_date: String,

    /// Drive serial number — GET CONFIGURATION Feature 0108h
    pub serial_number: String,

    /// Raw 96-byte INQUIRY response for additional parsing if needed.
    pub raw_inquiry: Vec<u8>,

    /// Raw GET CONFIGURATION Feature 010Ch response bytes.
    pub raw_gc_010c: Vec<u8>,
}

impl DriveId {
    /// Probe a real drive via SCSI and build its identity.
    pub fn from_drive(transport: &mut dyn ScsiTransport) -> Result<Self> {
        // INQUIRY — SPC-4 §6.4
        let mut inquiry = vec![0u8; 96];
        let cdb_inq = [0x12, 0x00, 0x00, 0x00, 0x60, 0x00];
        transport.execute(&cdb_inq, DataDirection::FromDevice, &mut inquiry, 5000)?;

        // GET CONFIGURATION Feature 010Ch — MMC-6 §6.6.
        // Best-effort: 010Ch (Firmware Information) is an optional feature.
        // A drive that lacks it may CHECK CONDITION rather than return an
        // empty descriptor, so a failure here is treated as feature-absent
        // (empty firmware date + empty raw bytes) instead of aborting the
        // whole identity probe.
        let mut gc = vec![0u8; 256];
        let cdb_gc = [0x46, 0x02, 0x01, 0x0C, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        // `bytes_transferred` is device-reported and untrusted; clamp every
        // slice end to the actual buffer length before indexing.
        let (firmware_date, raw_gc_010c) =
            match transport.execute(&cdb_gc, DataDirection::FromDevice, &mut gc, 5000) {
                Ok(result) => {
                    let end = result.bytes_transferred.min(gc.len());
                    let date = if end > 12 {
                        String::from_utf8_lossy(&gc[12..24.min(end)])
                            .trim()
                            .to_string()
                    } else {
                        String::new()
                    };
                    (date, gc[..end].to_vec())
                }
                Err(_) => (String::new(), Vec::new()),
            };

        // GET CONFIGURATION Feature 0108h — Serial Number.
        // Best-effort, like 010Ch above: the serial-number feature is
        // optional, so a drive that lacks it (CHECK CONDITION) or reports
        // too few bytes deliberately yields an empty serial rather than
        // failing the identity probe.
        let mut gc_serial = vec![0u8; 256];
        let cdb_serial = [0x46, 0x02, 0x01, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let serial_number = if let Ok(r) =
            transport.execute(&cdb_serial, DataDirection::FromDevice, &mut gc_serial, 5000)
        {
            if r.bytes_transferred > 12 {
                // `bytes_transferred` is device-reported and untrusted; clamp
                // the slice end to the buffer length to avoid an out-of-range
                // panic on an oversized reported count.
                let end = r.bytes_transferred.min(gc_serial.len());
                String::from_utf8_lossy(&gc_serial[12..end])
                    .trim()
                    .to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        Ok(DriveId {
            vendor_id: ascii_field(&inquiry, 8, 16),
            product_id: ascii_field(&inquiry, 16, 32),
            product_revision: ascii_field(&inquiry, 32, 36),
            vendor_specific: ascii_field(&inquiry, 36, 43),
            firmware_date,
            serial_number,
            raw_inquiry: inquiry,
            raw_gc_010c,
        })
    }

    /// Build identity from raw INQUIRY bytes and firmware date string.
    /// Used by tests and when serial isn't available.
    pub fn from_inquiry(inquiry: &[u8], firmware_date: &str) -> Self {
        DriveId {
            vendor_id: ascii_field(inquiry, 8, 16),
            product_id: ascii_field(inquiry, 16, 32),
            product_revision: ascii_field(inquiry, 32, 36),
            vendor_specific: ascii_field(inquiry, 36, 43),
            firmware_date: firmware_date.to_string(),
            serial_number: String::new(),
            raw_inquiry: inquiry.to_vec(),
            raw_gc_010c: Vec::new(),
        }
    }

    /// Profile match key: "VENDOR|PRODUCT|REVISION|VENDOR_SPECIFIC"
    ///
    /// Used to look up this drive in the profile database.
    /// All fields trimmed for consistent matching.
    pub fn match_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.vendor_id.trim(),
            self.product_id.trim(),
            self.product_revision.trim(),
            self.vendor_specific.trim()
        )
    }
}

impl std::fmt::Display for DriveId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.vendor_id.trim(),
            self.product_id.trim(),
            self.product_revision.trim(),
            self.vendor_specific.trim()
        )
    }
}

/// Extract an ASCII string field from raw SCSI data.
fn ascii_field(data: &[u8], start: usize, end: usize) -> String {
    if data.len() > start {
        let e = end.min(data.len());
        String::from_utf8_lossy(&data[start..e]).to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scsi::{ScsiResult, ScsiTransport};

    /// Transport that returns the requested data length but reports a
    /// bytes_transferred larger than the caller's buffer — models a drive
    /// that lies about its transfer count. The old slicing code panicked
    /// on this; the clamps must keep it from indexing out of range.
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

    /// ascii_field with a buffer shorter than `start` returns empty string
    /// rather than panicking.
    /// Spec: SPC-4 §6.4.2 — bytes[8:16] are vendor ID; a truncated buffer
    ///       (e.g. a device that reports fewer than 8 bytes) must not panic.
    /// Mutation: removing the `data.len() > start` guard makes it panic on short inputs.
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

    /// match_key trims whitespace from all four fields.
    /// Spec: comment says "All fields trimmed for consistent matching."
    /// Mutation: removing .trim() from one field adds trailing spaces to the key.
    #[test]
    fn match_key_trims_all_fields() {
        let mut inquiry = vec![0u8; 96];
        // Pad vendor_id and product_id with trailing spaces (as drives do).
        inquiry[8..16].copy_from_slice(b"HL-DT-ST"); // no padding room
        inquiry[16..32].copy_from_slice(b"BD-RE BU40N     "); // 5 trailing spaces
        inquiry[32..36].copy_from_slice(b"1.03");
        inquiry[36..43].copy_from_slice(b"NM00000");
        let id = DriveId::from_inquiry(&inquiry, "211810241934");
        // No trailing spaces in the key.
        assert_eq!(id.match_key(), "HL-DT-ST|BD-RE BU40N|1.03|NM00000");
    }

    /// Display trims all four fields and does not include the firmware date.
    /// Mutation: not trimming product_id adds trailing spaces to the display string.
    #[test]
    fn display_trims_fields() {
        let mut inquiry = vec![0u8; 96];
        inquiry[8..16].copy_from_slice(b"HL-DT-ST");
        inquiry[16..32].copy_from_slice(b"BD-RE BU40N     ");
        inquiry[32..36].copy_from_slice(b"1.03");
        inquiry[36..43].copy_from_slice(b"NM00000");
        let id = DriveId::from_inquiry(&inquiry, "ignored");
        let s = id.to_string();
        // No double spaces from un-trimmed padding.
        assert!(!s.contains("  "), "display must trim fields: `{s}`");
        assert!(s.contains("HL-DT-ST"), "vendor present: `{s}`");
        assert!(s.contains("BD-RE BU40N"), "product present: `{s}`");
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

    /// from_inquiry leaves serial_number and raw_gc_010c empty.
    /// These are only available from a live drive probe via from_drive().
    /// Mutation: populating serial_number in from_inquiry would violate the contract.
    #[test]
    fn from_inquiry_leaves_serial_and_gc_empty() {
        let inquiry = vec![0u8; 96];
        let id = DriveId::from_inquiry(&inquiry, "");
        assert!(
            id.serial_number.is_empty(),
            "serial_number must be empty from from_inquiry"
        );
        assert!(
            id.raw_gc_010c.is_empty(),
            "raw_gc_010c must be empty from from_inquiry"
        );
    }

    /// GET CONFIGURATION failure (transport error) must not abort the
    /// identity probe — firmware_date is empty, raw_gc_010c is empty.
    /// Mutation: propagating the GET_CONFIGURATION error with `?` aborts from_drive.
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

    /// match_key uses '|' as the separator between all four fields.
    /// Mutation: using ':' or ' ' as separator changes the key format.
    #[test]
    fn match_key_uses_pipe_separator() {
        let inquiry = vec![0u8; 96];
        let id = DriveId::from_inquiry(&inquiry, "");
        let key = id.match_key();
        // Should have exactly 3 pipes (4 fields separated by 3 '|' chars).
        let pipe_count = key.chars().filter(|&c| c == '|').count();
        assert_eq!(
            pipe_count, 3,
            "match_key must have exactly 3 '|' separators, got {pipe_count} in `{key}`"
        );
    }
}

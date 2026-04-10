//! Drive identification — match drives to profiles by SCSI response fields.
//!
//! Field names follow SPC-4 (INQUIRY) and MMC-6 (GET CONFIGURATION) standards.
//! No proprietary fingerprints or encrypted lookups — open matching only.
//!
//! References:
//!   SPC-4 §6.4.2 — Standard INQUIRY data
//!   MMC-6 §5.3.10 — Feature 010Ch (Firmware Information)

use crate::error::Result;
use crate::scsi::{ScsiTransport, DataDirection};

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

        // GET CONFIGURATION Feature 010Ch — MMC-6 §6.6
        let mut gc = vec![0u8; 256];
        let cdb_gc = [0x46, 0x02, 0x01, 0x0C, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let result = transport.execute(&cdb_gc, DataDirection::FromDevice, &mut gc, 5000)?;

        let firmware_date = if result.bytes_transferred > 12 {
            String::from_utf8_lossy(&gc[12..24.min(result.bytes_transferred)])
                .trim().to_string()
        } else {
            String::new()
        };

        // GET CONFIGURATION Feature 0108h — Serial Number
        let mut gc_serial = vec![0u8; 256];
        let cdb_serial = [0x46, 0x02, 0x01, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let serial_number = if let Ok(r) = transport.execute(&cdb_serial, DataDirection::FromDevice, &mut gc_serial, 5000) {
            if r.bytes_transferred > 12 {
                String::from_utf8_lossy(&gc_serial[12..r.bytes_transferred])
                    .trim().to_string()
            } else { String::new() }
        } else { String::new() };

        Ok(DriveId {
            vendor_id: ascii_field(&inquiry, 8, 16),
            product_id: ascii_field(&inquiry, 16, 32),
            product_revision: ascii_field(&inquiry, 32, 36),
            vendor_specific: ascii_field(&inquiry, 36, 43),
            firmware_date,
            serial_number,
            raw_inquiry: inquiry.to_vec(),
            raw_gc_010c: gc[..result.bytes_transferred].to_vec(),
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
        format!("{}|{}|{}|{}",
            self.vendor_id.trim(),
            self.product_id.trim(),
            self.product_revision.trim(),
            self.vendor_specific.trim())
    }
}

impl std::fmt::Display for DriveId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {} {}",
            self.vendor_id.trim(),
            self.product_id.trim(),
            self.product_revision.trim(),
            self.vendor_specific.trim())
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
}

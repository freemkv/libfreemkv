//! Drive identification — match drives to profiles by SCSI response fields.
//!
//! Field names follow SPC-4 (INQUIRY) and MMC-6 (GET CONFIGURATION) standards.
//! No proprietary fingerprints or encrypted lookups — open matching only.
//!
//! References:
//!   SPC-4 §6.4.2 — Standard INQUIRY data
//!   MMC-6 §5.3.10 — Feature 010Ch (Firmware Information)

use crate::error::Result;
use crate::scsi::{DataDirection, ScsiTransport, gc_feature_descriptor, gc_reply_len};

/// Drive identity from standard SCSI commands.
///
/// All field names follow the SCSI standards:
///   - SPC-4 §6.4.2 for INQUIRY fields
///   - MMC-6 §5.3.10 for Firmware Information
#[derive(Debug, Clone)]
pub struct DriveId {
    /// T10 VENDOR IDENTIFICATION — INQUIRY bytes `[8:16]`
    /// SPC-4 §6.4.2
    pub vendor_id: String,

    /// PRODUCT IDENTIFICATION — INQUIRY bytes `[16:32]`
    /// SPC-4 §6.4.2
    pub product_id: String,

    /// PRODUCT REVISION LEVEL — INQUIRY bytes `[32:36]`
    /// SPC-4 §6.4.2
    pub product_revision: String,

    /// VENDOR SPECIFIC — INQUIRY bytes `[36:43]`
    /// SPC-4 §6.4.2
    /// Content varies by vendor: firmware type code (MTK), date (Pioneer), etc.
    pub vendor_specific: String,

    /// Firmware Creation Date — GET CONFIGURATION Feature 010Ch
    /// MMC-6 §5.3.10
    /// Format: CCYYMMDDHHMI (12 ASCII characters)
    pub firmware_date: String,

    /// Drive serial number — GET CONFIGURATION Feature 0108h
    pub serial_number: String,

    /// Raw INQUIRY response for additional parsing if needed: what the drive
    /// sent, 36 to 96 bytes.
    pub raw_inquiry: Vec<u8>,

    /// Raw GET CONFIGURATION Feature 010Ch response bytes.
    pub raw_gc_010c: Vec<u8>,
}

/// SPC-4 standard INQUIRY data: 36 bytes through `product_revision`. Anything
/// shorter cannot populate the identity fields this type promises.
const INQUIRY_STANDARD_LEN: usize = 36;

/// Issues one CDB: a raw transport, or `Drive::exec`.
pub(crate) type Exec<'a> =
    dyn FnMut(&[u8], DataDirection, &mut [u8], u32) -> Result<crate::scsi::ScsiResult> + 'a;

impl DriveId {
    /// Probe a real drive via SCSI and build its identity.
    pub fn from_drive(transport: &mut dyn ScsiTransport) -> Result<Self> {
        Self::identify(&mut |cdb, dir, buf, t| transport.execute(cdb, dir, buf, t))
    }

    /// [`from_drive`](Self::from_drive) over any CDB issuer: `Drive::open` passes
    /// `Drive::exec`, so identification is refused once the op token is cancelled.
    pub(crate) fn identify(exec: &mut Exec<'_>) -> Result<Self> {
        // INQUIRY — SPC-4 §6.4
        let mut inquiry = vec![0u8; 96];
        let cdb_inq = [0x12, 0x00, 0x00, 0x00, 0x60, 0x00];
        let inq = exec(&cdb_inq, DataDirection::FromDevice, &mut inquiry, 5000)?;
        // `bytes_transferred` is device-reported and untrusted. Unchecked, a
        // GOOD status with a short/empty data phase decoded to blank identity +
        // byte0 0x00 — which reads as DIRECT ACCESS, so the drive vanishes silently.
        if inq.bytes_transferred < INQUIRY_STANDARD_LEN {
            return Err(crate::error::Error::DriveInquiryShort);
        }
        // Never decode past what the drive actually sent.
        inquiry.truncate(inq.bytes_transferred.min(inquiry.len()));

        // GET CONFIGURATION Feature 010Ch — MMC-6 §6.6. Best-effort: a drive may
        // lack it, so failure is feature-absent, not a probe abort. Fields are
        // bounded by the reply's own Data/Additional Length, not the transfer count.
        let mut gc = vec![0u8; 256];
        let cdb_gc = [0x46, 0x02, 0x01, 0x0C, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let (firmware_date, raw_gc_010c) =
            match exec(&cdb_gc, DataDirection::FromDevice, &mut gc, 5000) {
                Ok(r) => {
                    let date = gc_feature_descriptor(&gc, r.bytes_transferred, 0x010C)
                        .map(|d| gc_text(&d[4..d.len().min(16)]))
                        .unwrap_or_default();
                    (date, gc[..gc_reply_len(&gc, r.bytes_transferred)].to_vec())
                }
                Err(crate::error::Error::Halted) => return Err(crate::error::Error::Halted),
                Err(_) => (String::new(), Vec::new()),
            };

        // GET CONFIGURATION Feature 0108h — Serial Number. Best-effort like
        // 010Ch: optional feature, so lacking it (CHECK CONDITION) or too few
        // bytes deliberately yields an empty serial rather than failing.
        let mut gc_serial = vec![0u8; 256];
        let cdb_serial = [0x46, 0x02, 0x01, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let serial_number = match exec(&cdb_serial, DataDirection::FromDevice, &mut gc_serial, 5000)
        {
            Ok(r) => gc_feature_descriptor(&gc_serial, r.bytes_transferred, 0x0108)
                .map(|d| gc_text(&d[4..]))
                .unwrap_or_default(),
            Err(crate::error::Error::Halted) => return Err(crate::error::Error::Halted),
            Err(_) => String::new(),
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

    /// True if this is an MMC optical drive (INQUIRY peripheral device type 05h).
    pub(crate) fn is_optical(&self) -> bool {
        crate::scsi::is_optical_peripheral(&self.raw_inquiry)
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

// A GET CONFIGURATION text field: trims spaces and the NUL padding `trim` keeps.
fn gc_text(field: &[u8]) -> String {
    let text = String::from_utf8_lossy(field);
    printable(text.trim_matches(|c: char| c.is_whitespace() || c == '\0'))
}

// Drive-supplied text is untrusted: control characters (bar NUL padding) become `?`
// so they cannot reach a terminal or log through `Display`.
fn printable(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() && c != '\0' { '?' } else { c })
        .collect()
}

/// Extract an ASCII string field from raw SCSI data.
fn ascii_field(data: &[u8], start: usize, end: usize) -> String {
    if data.len() > start {
        let e = end.min(data.len());
        printable(&String::from_utf8_lossy(&data[start..e]))
    } else {
        String::new()
    }
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;

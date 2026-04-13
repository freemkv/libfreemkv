//! Drive data capture — read hardware information via SCSI.

use crate::drive::Drive;
use crate::error::Result;

/// Raw data captured from a drive's SCSI responses.
#[derive(Debug, Clone)]
pub struct DriveCapture {
    /// Raw INQUIRY response (96 bytes)
    pub inquiry: Vec<u8>,
    /// Raw GET_CONFIG 010C response
    pub gc_010c: Vec<u8>,
    /// GET_CONFIG feature responses: (feature_code, feature_name, data)
    pub features: Vec<CapturedFeature>,
    /// REPORT_KEY RPC state
    pub rpc_state: Option<Vec<u8>>,
    /// MODE SENSE page 2A (capabilities)
    pub mode_2a: Option<Vec<u8>>,
    /// READ_BUFFER 0xF1 (Pioneer vendor data)
    pub rb_f1: Option<Vec<u8>>,
    /// READ_BUFFER mode 6 (MTK vendor data)
    pub rb_mode6: Option<Vec<u8>>,
}

/// A single GET CONFIGURATION feature response from the drive.
#[derive(Debug, Clone)]
pub struct CapturedFeature {
    pub code: u16,
    pub name: &'static str,
    pub data: Vec<u8>,
}

/// Feature codes to capture.
const FEATURES: &[(u16, &str)] = &[
    (0x0000, "Profile List"),
    (0x0001, "Core"),
    (0x0003, "Removable Medium"),
    (0x0010, "Random Readable"),
    (0x001D, "Multi-Read"),
    (0x001E, "CD Read"),
    (0x001F, "DVD Read"),
    (0x0040, "BD Read"),
    (0x0041, "BD Write"),
    (0x0100, "Power Management"),
    (0x0102, "Embedded Changer"),
    (0x0107, "Real Time Streaming"),
    (0x0108, "Serial Number"),
    (0x010C, "Firmware Information"),
    (0x010D, "AACS"),
];

/// Capture all available drive data via SCSI commands.
/// Returns raw responses — no formatting, no zipping, no presentation.
pub fn capture_drive_data(session: &mut Drive) -> Result<DriveCapture> {
    let id = &session.drive_id;

    // Already have INQUIRY from drive open
    let inquiry = id.raw_inquiry.clone();
    let gc_010c = id.raw_gc_010c.clone();

    // Capture GET_CONFIG features using Drive's query methods
    let mut features = Vec::new();
    for &(code, name) in FEATURES {
        if let Some(data) = session.get_config_feature(code) {
            features.push(CapturedFeature { code, name, data });
        }
    }

    // Vendor-specific READ_BUFFER queries
    let rb_f1 = session.read_buffer(0x02, 0xF1, 48);    // Pioneer
    let rb_mode6 = session.read_buffer(0x06, 0x00, 32);  // MTK

    // Standard queries
    let rpc_state = session.report_key_rpc_state();
    let mode_2a = session.mode_sense_page(0x2A);

    Ok(DriveCapture {
        inquiry,
        gc_010c,
        features,
        rpc_state,
        mode_2a,
        rb_f1,
        rb_mode6,
    })
}

/// Mask a string for privacy (letters->A, digits->0).
pub fn mask_string(s: &str) -> String {
    s.chars().map(|c| {
        if c.is_ascii_alphabetic() { 'A' }
        else if c.is_ascii_digit() { '0' }
        else { c }
    }).collect()
}

/// Mask bytes for privacy.
pub fn mask_bytes(data: &[u8]) -> Vec<u8> {
    data.iter().map(|&b| {
        if b.is_ascii_alphabetic() { b'A' }
        else if b.is_ascii_digit() { b'0' }
        else { b }
    }).collect()
}

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
    /// MMC-6 GET CONFIGURATION feature code (e.g. `0x010D` = AACS).
    pub code: u16,
    /// Static human-readable label from the internal `FEATURES` table —
    /// not a device-reported string.
    pub name: &'static str,
    /// Raw feature-descriptor payload bytes, with the 8-byte GET
    /// CONFIGURATION header stripped (i.e. `buf[8..]`). Unlike
    /// [`DriveCapture::gc_010c`], which retains the full header.
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
    let rb_f1 = session.read_buffer(0x02, 0xF1, 48); // Pioneer
    let rb_mode6 = session.read_buffer(0x06, 0x00, 32); // MTK

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
    s.chars()
        .map(|c| {
            if c.is_ascii_alphabetic() {
                'A'
            } else if c.is_ascii_digit() {
                '0'
            } else {
                c
            }
        })
        .collect()
}

/// Mask bytes for privacy.
pub fn mask_bytes(data: &[u8]) -> Vec<u8> {
    data.iter()
        .map(|&b| {
            if b.is_ascii_alphabetic() {
                b'A'
            } else if b.is_ascii_digit() {
                b'0'
            } else {
                b
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    //! Privacy-masking + capture-orchestration tests.
    //!
    //! `mask_string` / `mask_bytes` redact identifying characters before
    //! a drive capture leaves the machine: every ASCII letter → 'A',
    //! every ASCII digit → '0', everything else (punctuation, spaces,
    //! control bytes, non-ASCII) is preserved verbatim so structural
    //! framing (offsets, separators) survives for diffing.
    use super::*;

    #[test]
    fn mask_string_letters_become_a_digits_become_zero() {
        // Mixed case letters all collapse to 'A'; digits to '0'.
        assert_eq!(mask_string("HL-DT-ST"), "AA-AA-AA");
        assert_eq!(mask_string("BU40N"), "AA00A");
    }

    #[test]
    fn mask_string_preserves_non_alnum_punctuation_and_space() {
        // Separators and spaces must be preserved so the masked output
        // keeps the same shape as the original (the whole point of a
        // structure-preserving redaction).
        assert_eq!(mask_string("1.04"), "0.00");
        assert_eq!(mask_string("a b-c.d_e"), "A A-A.A_A");
    }

    #[test]
    fn mask_string_preserves_non_ascii_chars() {
        // is_ascii_alphabetic/is_ascii_digit are false for non-ASCII, so
        // multibyte chars pass through unchanged (no mojibake, no panic).
        // 'c','a','f' are ASCII letters → 'A'; 'é' is non-ASCII →
        // preserved; '9' → '0'.
        assert_eq!(mask_string("café9"), "AAAé0");
    }

    #[test]
    fn mask_bytes_matches_string_masking_for_ascii() {
        // mask_bytes is the byte-wise analogue: letters→b'A', digits→b'0'.
        assert_eq!(mask_bytes(b"HL-DT-ST"), b"AA-AA-AA".to_vec());
        assert_eq!(mask_bytes(b"1.04"), b"0.00".to_vec());
    }

    #[test]
    fn mask_bytes_preserves_non_alnum_and_high_bytes() {
        // Control bytes (0x00), high bytes (0xFF), and punctuation are
        // not ASCII alnum and must survive verbatim — INQUIRY payloads
        // are space-padded binary and the framing must be diffable.
        let input = [0x00u8, b'A', 0x20, b'7', 0xFF, b'-'];
        assert_eq!(mask_bytes(&input), vec![0x00, b'A', 0x20, b'0', 0xFF, b'-']);
    }

    #[test]
    fn feature_table_has_no_duplicate_codes() {
        // capture_drive_data iterates FEATURES once per code; a duplicate
        // code would silently capture the same feature twice (and bloat
        // the report). Each MMC-6 feature code must be unique.
        let mut seen = std::collections::HashSet::new();
        for &(code, _name) in FEATURES {
            assert!(seen.insert(code), "duplicate feature code {code:#06x}");
        }
    }

    #[test]
    fn feature_table_includes_aacs_010d() {
        // AACS (0x010D) is the feature that gates UHD decryption capture;
        // it must be in the table or AACS drives capture incompletely.
        assert!(
            FEATURES.iter().any(|&(c, _)| c == 0x010D),
            "AACS feature 0x010D must be captured"
        );
    }
}

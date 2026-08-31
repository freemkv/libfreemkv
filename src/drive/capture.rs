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
    /// READ_BUFFER 0xB0 @0x04 (Renesas signature only)
    pub rb_b0_04: Option<Vec<u8>>,
    /// WRITE_BUFFER 0x41 @0xA5AAAA (Renesas signature only)
    pub wb_41: Option<Vec<u8>>,
    /// READ_BUFFER 0xB0 @0x500000 (Renesas signature only)
    pub rb_b0_500000: Option<Vec<u8>>,
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

// Renesas vendor-probe fields (signature-gated in `capture_drive_data`).
const SCSI_WRITE_BUFFER: u8 = 0x3B; // WRITE BUFFER opcode (SPC-4)
const RENESAS_RB_MODE: u8 = 0x02; // READ BUFFER mode: vendor-specific data
const RENESAS_RB_BUFFER_ID: u8 = 0xB0; // vendor buffer id for the RB0x04 / RB0x500000 probes
const RENESAS_RB_ADDR_04: u32 = 0x04; // vendor buffer offset for the `rb_b0_04` probe
const RENESAS_RB_ADDR_500000: u32 = 0x500000; // vendor buffer offset for the `rb_b0_500000` probe
const RENESAS_RB_LEN: u32 = 164; // vendor probe response length (bytes)
const RENESAS_WB_MODE: u8 = 0x02; // WRITE BUFFER mode: vendor-specific data
const RENESAS_WB_BUFFER_ID: u8 = 0x41; // vendor buffer id for the `wb_41` probe
const RENESAS_WB_OFFSET: [u8; 3] = [0xA5, 0xAA, 0xAA]; // vendor magic offset for the `wb_41` probe

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

    // Renesas signature: RB 0xF1 bytes [16..19] == "SAT".
    let (mut rb_b0_04, mut wb_41, mut rb_b0_500000) = (None, None, None);
    if rb_f1
        .as_ref()
        .is_some_and(|f| f.len() >= 19 && &f[16..19] == b"SAT")
    {
        use crate::scsi::{DataDirection as D, build_read_buffer};
        rb_b0_04 = raw(
            session,
            &build_read_buffer(
                RENESAS_RB_MODE,
                RENESAS_RB_BUFFER_ID,
                RENESAS_RB_ADDR_04,
                RENESAS_RB_LEN,
            ),
            D::FromDevice,
            RENESAS_RB_LEN as usize,
        );
        wb_41 = raw(
            session,
            &[
                SCSI_WRITE_BUFFER,
                RENESAS_WB_MODE,
                RENESAS_WB_BUFFER_ID,
                RENESAS_WB_OFFSET[0],
                RENESAS_WB_OFFSET[1],
                RENESAS_WB_OFFSET[2],
                0,
                0,
                0,
                0,
            ],
            D::None,
            0,
        );
        rb_b0_500000 = raw(
            session,
            &build_read_buffer(
                RENESAS_RB_MODE,
                RENESAS_RB_BUFFER_ID,
                RENESAS_RB_ADDR_500000,
                RENESAS_RB_LEN,
            ),
            D::FromDevice,
            RENESAS_RB_LEN as usize,
        );
    }

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
        rb_b0_04,
        wb_41,
        rb_b0_500000,
    })
}

/// Run a raw CDB; `Some(data)` on GOOD status (empty for a write), else `None`.
fn raw(
    session: &mut Drive,
    cdb: &[u8],
    dir: crate::scsi::DataDirection,
    len: usize,
) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let r = session.scsi_execute(cdb, dir, &mut buf, 5_000).ok()?;
    (r.status == 0).then(|| buf[..r.bytes_transferred.min(buf.len())].to_vec())
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
        // is_ascii_alphabetic/is_ascii_digit are false for non-ASCII, so multibyte
        // chars pass through unchanged (no mojibake, no panic): ASCII letters → 'A',
        // digits → '0', 'é' preserved.
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

    // ── Renesas signature gate ──────────────────────────────────────────

    /// Mock transport that answers READ_BUFFER/WRITE_BUFFER the way a real
    /// Renesas-family drive would: the Pioneer RB 0xF1 probe carries the
    /// "SAT" signature (or not, per `signature`), and the two RB 0xB0
    /// offsets each return a distinct marker byte so the test can tell them
    /// apart. Every other command reports GOOD with a zeroed reply.
    struct RenesasProbeTransport {
        signature: bool,
    }

    impl crate::scsi::ScsiTransport for RenesasProbeTransport {
        fn execute(
            &mut self,
            cdb: &[u8],
            _direction: crate::scsi::DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<crate::scsi::ScsiResult> {
            let mut payload = vec![0u8; data.len()];
            match cdb[0] {
                crate::scsi::SCSI_READ_BUFFER => {
                    let buffer_id = cdb[2];
                    let offset = ((cdb[3] as u32) << 16) | ((cdb[4] as u32) << 8) | cdb[5] as u32;
                    const PIONEER_RB_BUFFER_ID: u8 = 0xF1;
                    if buffer_id == PIONEER_RB_BUFFER_ID && payload.len() >= 19 {
                        // Pioneer probe: plant the Renesas signature at
                        // [16..19] only when `signature` is set.
                        if self.signature {
                            payload[16..19].copy_from_slice(b"SAT");
                        }
                    } else if buffer_id == RENESAS_RB_BUFFER_ID {
                        payload[0] = if offset == RENESAS_RB_ADDR_04 {
                            0xAA
                        } else if offset == RENESAS_RB_ADDR_500000 {
                            0xBB
                        } else {
                            0x00
                        };
                    }
                }
                SCSI_WRITE_BUFFER => {}
                _ => {}
            }
            let n = payload.len().min(data.len());
            data[..n].copy_from_slice(&payload[..n]);
            Ok(crate::scsi::ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            })
        }
    }

    fn drive_with_renesas_signature(signature: bool) -> Drive {
        Drive::from_transport_for_test(Box::new(RenesasProbeTransport { signature }))
    }

    #[test]
    fn renesas_gate_captures_the_vendor_probes_when_the_signature_matches() {
        // rb_f1[16..19] == "SAT" must open the gate: all three Renesas-only
        // fields come back populated, and the two RB 0xB0 probes must have
        // hit the offsets this test planted markers at (not been swapped).
        let mut drive = drive_with_renesas_signature(true);
        let capture = capture_drive_data(&mut drive).expect("capture_drive_data failed");

        assert_eq!(
            capture.rb_b0_04.as_deref().map(|d| d[0]),
            Some(0xAA),
            "RB 0xB0 @0x04 must be captured when the signature matches"
        );
        assert_eq!(
            capture.rb_b0_500000.as_deref().map(|d| d[0]),
            Some(0xBB),
            "RB 0xB0 @0x500000 must be captured when the signature matches"
        );
        assert!(
            capture.wb_41.is_some(),
            "WB 0x41 must be issued when the signature matches"
        );
    }

    #[test]
    fn renesas_gate_skips_the_vendor_probes_when_the_signature_is_absent() {
        // No "SAT" at rb_f1[16..19] → the gate must not fire: all three
        // Renesas-only fields stay None, and no marker from the mock's
        // gate-only RB 0xB0 branch leaks through.
        let mut drive = drive_with_renesas_signature(false);
        let capture = capture_drive_data(&mut drive).expect("capture_drive_data failed");

        assert!(
            capture.rb_b0_04.is_none(),
            "RB 0xB0 @0x04 must not be captured without the signature"
        );
        assert!(
            capture.rb_b0_500000.is_none(),
            "RB 0xB0 @0x500000 must not be captured without the signature"
        );
        assert!(
            capture.wb_41.is_none(),
            "WB 0x41 must not be issued without the signature"
        );
    }
}

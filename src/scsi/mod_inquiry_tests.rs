//! [`inquiry`] standard-INQUIRY field parsing (SPC-4 §6.4.2 Table 142):
//!   - vendor identification: bytes 8..16 (8 ASCII chars)
//!   - product identification: bytes 16..32 (16 ASCII chars)
//!   - product revision level: bytes 32..36 (4 ASCII chars)
//!
//! Fields are space-padded ASCII; the parser trims surrounding
//! whitespace.
use super::*;

/// Mock transport returning a scripted INQUIRY payload and recording
/// the CDB it was handed.
struct ScriptedTransport {
    payload: Vec<u8>,
    last_cdb: Vec<u8>,
}
impl ScsiTransport for ScriptedTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.last_cdb = cdb.to_vec();
        let n = self.payload.len().min(data.len());
        data[..n].copy_from_slice(&self.payload[..n]);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: n,
            sense: [0u8; 32],
        })
    }
}

fn inquiry_payload(vendor: &[u8], product: &[u8], rev: &[u8]) -> Vec<u8> {
    // SPC-4 §6.4.2: identifier fields are left-aligned ASCII, padded
    // with SPACE (0x20), not NUL — build the fixture that way so the
    // parser's trim() is exercised on real-shaped padding.
    let mut p = vec![0u8; 96];
    // peripheral device type 5 (CD/DVD) in byte 0 low 5 bits — not
    // parsed by inquiry() but realistic.
    p[0] = 0x05;
    for b in &mut p[8..36] {
        *b = b' ';
    }
    p[8..8 + vendor.len()].copy_from_slice(vendor);
    p[16..16 + product.len()].copy_from_slice(product);
    p[32..32 + rev.len()].copy_from_slice(rev);
    p
}

#[test]
fn parses_vendor_product_revision_offsets() {
    // Real BU40N-style identity. Vendor "HL-DT-ST" (8 chars exactly),
    // product padded to 16, revision "1.04".
    let payload = inquiry_payload(b"HL-DT-ST", b"BD-RE BU40N     ", b"1.04");
    let mut t = ScriptedTransport {
        payload,
        last_cdb: vec![],
    };
    let r = inquiry(&mut t).unwrap();
    assert_eq!(r.vendor_id, "HL-DT-ST");
    assert_eq!(r.model, "BD-RE BU40N");
    assert_eq!(r.firmware, "1.04");
}

#[test]
fn fields_are_independent_no_bleed_across_offset_boundaries() {
    // A wrong end-offset (e.g. vendor 8..17) would pull the first
    // product char into the vendor string. Use a vendor that fills
    // all 8 bytes and a product whose first byte is distinctive.
    let payload = inquiry_payload(b"VENDOR12", b"XPRODUCT", b"REV0");
    let mut t = ScriptedTransport {
        payload,
        last_cdb: vec![],
    };
    let r = inquiry(&mut t).unwrap();
    assert_eq!(r.vendor_id, "VENDOR12", "vendor must stop at byte 16");
    assert!(
        !r.vendor_id.contains('X'),
        "product byte must not bleed into vendor"
    );
    assert_eq!(r.model, "XPRODUCT");
}

#[test]
fn whitespace_padded_fields_trimmed() {
    // SPC-4 pads identifiers with spaces; trim() removes them.
    let payload = inquiry_payload(b"  ABC   ", b"  MODEL X       ", b" R1 ");
    let mut t = ScriptedTransport {
        payload,
        last_cdb: vec![],
    };
    let r = inquiry(&mut t).unwrap();
    assert_eq!(r.vendor_id, "ABC");
    assert_eq!(r.model, "MODEL X");
    assert_eq!(r.firmware, "R1");
}

#[test]
fn cdb_is_standard_inquiry_96_bytes() {
    // The CDB must be INQUIRY (0x12) with allocation length 0x60 (96)
    // in byte 4 — matching the 96-byte buffer the parser slices.
    let payload = inquiry_payload(b"V", b"M", b"R");
    let mut t = ScriptedTransport {
        payload,
        last_cdb: vec![],
    };
    let _ = inquiry(&mut t).unwrap();
    assert_eq!(t.last_cdb[0], SCSI_INQUIRY);
    assert_eq!(t.last_cdb[4], 0x60, "allocation length must be 96 bytes");
}

#[test]
fn raw_response_preserved_full_96_bytes() {
    // raw must carry the entire 96-byte INQUIRY for downstream
    // identity capture/masking — not just the parsed fields.
    let payload = inquiry_payload(b"HL-DT-ST", b"BD-RE BU40N", b"1.04");
    let mut t = ScriptedTransport {
        payload,
        last_cdb: vec![],
    };
    let r = inquiry(&mut t).unwrap();
    assert_eq!(r.raw.len(), 96);
    assert_eq!(r.raw[0], 0x05, "peripheral device type byte preserved");
}

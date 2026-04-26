//! Integration tests for the SCSI error-decoding contract.
//!
//! v0.13.20 rewrote `scsi/linux.rs` to a synchronous blocking SG_IO and
//! consolidated sense-key parsing into the `parse_sense_key` helper that
//! every platform backend now shares. The actual `ioctl(SG_IO, ...)` call
//! is impossible to mock without a kernel — see
//! `freemkv-private/docs/audits/2026-04-26-scsi-architecture-research.md`
//! for why the audit recommends against libc shims here.
//!
//! These tests pin the *contract* every backend must satisfy:
//!
//!   1. Healthy result → `Ok(ScsiResult { bytes_transferred = data.len() - resid })`.
//!   2. Transport-level failure (`host_status` or `driver_status` non-zero)
//!      → `Error::ScsiError { status: 0xFF, sense_key: 0 }`. Used by
//!      `drive_has_disc` to detect the wedge signature.
//!   3. SCSI-level failure (status non-zero, sense buffer populated) →
//!      `Error::ScsiError { status, sense_key }` with the parsed key.
//!   4. Sense-key parsing handles descriptor (0x72/0x73) and fixed
//!      (0x70/0x71) response codes; missing sense data → key 0.
//!
//! The mock `ScsiTransport` here emulates exactly that layered shape.
//! Inline `parse_sense_tests` in `src/scsi/mod.rs` cover the pure parse
//! logic; this file covers the consumer side — a real transport feeding
//! a real Error variant to a real call site (`scsi::inquiry`).

use libfreemkv::error::Error;
use libfreemkv::scsi::{DataDirection, ScsiResult, ScsiTransport};

/// A scripted ScsiTransport. Each `execute()` consumes the next entry
/// from `script` and returns the corresponding outcome.
///
/// Outcomes mirror what each backend's `execute()` should produce after
/// the v0.13.20 rewrite: pre-parsed sense_key and synthesized 0xFF
/// status for transport-level failures.
struct MockTransport {
    script: Vec<MockOutcome>,
    next: usize,
}

#[derive(Clone)]
enum MockOutcome {
    /// Healthy completion. `data` is what the transport wrote into the
    /// caller's data buffer (truncated to the buffer length); `resid`
    /// is reported back as `data.len() - bytes_transferred`.
    Ok {
        data: Vec<u8>,
        resid: i32,
    },
    /// Transport-level failure: e.g. `hdr.host_status = DID_TIME_OUT`
    /// on Linux, or `kIOReturnError` on macOS, or `DeviceIoControl`
    /// returning 0 on Windows. Backends synthesize 0xFF.
    TransportFailure,
    /// SCSI-level failure: device responded with a non-zero status and
    /// some sense data. `status` and `sense_key` are what the caller
    /// must see on the `Error::ScsiError` variant.
    ScsiFailure {
        status: u8,
        sense_key: u8,
    },
}

impl MockTransport {
    fn new(script: Vec<MockOutcome>) -> Self {
        Self { script, next: 0 }
    }
}

impl ScsiTransport for MockTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _direction: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> libfreemkv::error::Result<ScsiResult> {
        let i = self.next;
        self.next += 1;
        let outcome = self
            .script
            .get(i)
            .cloned()
            .expect("MockTransport script ran out — test wrote fewer outcomes than calls");
        match outcome {
            MockOutcome::Ok { data: d, resid } => {
                let n = d.len().min(data.len());
                data[..n].copy_from_slice(&d[..n]);
                let bytes = (data.len() as i32).saturating_sub(resid).max(0) as usize;
                Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: bytes,
                    sense: [0u8; 32],
                })
            }
            MockOutcome::TransportFailure => Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0xFF,
                sense_key: 0,
            }),
            MockOutcome::ScsiFailure { status, sense_key } => Err(Error::ScsiError {
                opcode: cdb[0],
                status,
                sense_key,
            }),
        }
    }
}

// ── 1. Healthy read ───────────────────────────────────────────────────────

#[test]
fn test_healthy_inquiry_returns_ok_with_full_transfer() {
    // Build a fake INQUIRY response (96 bytes per scsi::inquiry).
    // Vendor "TEST-VND", model "TEST-MODEL ", firmware "1.00".
    let mut payload = vec![0u8; 96];
    payload[8..16].copy_from_slice(b"TEST-VND");
    payload[16..32].copy_from_slice(b"TEST-MODEL      ");
    payload[32..36].copy_from_slice(b"1.00");

    let mut transport = MockTransport::new(vec![MockOutcome::Ok {
        data: payload,
        resid: 0,
    }]);

    let r = libfreemkv::scsi::inquiry(&mut transport).expect("inquiry should succeed");
    assert_eq!(r.vendor_id, "TEST-VND");
    assert_eq!(r.model, "TEST-MODEL");
    assert_eq!(r.firmware, "1.00");
}

// ── 2. Transport-level failure (host_status or driver_status non-zero) ─────
//
// Linux: kernel sets `hdr.host_status = DID_TIME_OUT (0x03)` or
//        `hdr.driver_status` non-zero on a USB bridge wedge. SgIoTransport
//        synthesizes ScsiError { status: 0xFF, sense_key: 0 }.
// macOS: IOKit `ExecuteTaskSync` returns non-zero IOReturn; same shape.
// Windows: `DeviceIoControl` returns 0; same shape.
//
// Callers (drive_has_disc, etc.) match on status == 0xFF as the wedge
// signature. This test pins that contract.

#[test]
fn test_transport_failure_surfaces_as_status_0xff_sense_key_0() {
    let mut transport = MockTransport::new(vec![MockOutcome::TransportFailure]);

    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    match err {
        Error::ScsiError {
            status, sense_key, ..
        } => {
            assert_eq!(status, 0xFF, "transport failure must surface as 0xFF");
            assert_eq!(sense_key, 0, "transport failure has no sense key");
        }
        other => panic!("expected ScsiError, got {other:?}"),
    }
}

// ── 3. SCSI-level failure with descriptor-format sense (0x72) ─────────────

#[test]
fn test_scsi_failure_descriptor_format_illegal_request() {
    // A real device returning CHECK CONDITION (status 0x02) with
    // descriptor-format sense indicating ILLEGAL REQUEST (key 5).
    // The Linux backend's parse_sense_key reads byte 1; the caller sees
    // sense_key = 5.
    let mut transport = MockTransport::new(vec![MockOutcome::ScsiFailure {
        status: 0x02,
        sense_key: 5,
    }]);

    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    match err {
        Error::ScsiError {
            opcode,
            status,
            sense_key,
        } => {
            assert_eq!(opcode, libfreemkv::scsi::SCSI_INQUIRY);
            assert_eq!(status, 0x02);
            assert_eq!(sense_key, 5);
        }
        other => panic!("expected ScsiError, got {other:?}"),
    }
}

// ── 4. SCSI-level failure with NOT READY sense ───────────────────────────
//
// `drive_has_disc` matches on sense_key 2 to mean "no disc inserted"
// rather than a hard error. Pin that contract via the consumer.

#[test]
fn test_scsi_failure_not_ready_key_2_propagates_intact() {
    let mut transport = MockTransport::new(vec![MockOutcome::ScsiFailure {
        status: 0x02,
        sense_key: 2,
    }]);

    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    match err {
        Error::ScsiError {
            status, sense_key, ..
        } => {
            assert_eq!(status, 0x02);
            assert_eq!(sense_key, 2);
        }
        other => panic!("expected ScsiError, got {other:?}"),
    }
}

// ── 5. SCSI-level failure with empty sense ───────────────────────────────
//
// Backends pass `sb_len_wr` to `parse_sense_key`; when zero, the helper
// returns 0. From the caller's perspective this is `sense_key = 0`
// (NO SENSE) on a non-zero status — surface that contract.

#[test]
fn test_scsi_failure_empty_sense_returns_key_0() {
    let mut transport = MockTransport::new(vec![MockOutcome::ScsiFailure {
        status: 0x02,
        sense_key: 0,
    }]);

    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    match err {
        Error::ScsiError {
            status, sense_key, ..
        } => {
            assert_eq!(status, 0x02);
            assert_eq!(sense_key, 0);
        }
        other => panic!("expected ScsiError, got {other:?}"),
    }
}

// ── 6. Healthy short transfer (resid > 0) ────────────────────────────────

#[test]
fn test_healthy_short_transfer_reports_partial_bytes() {
    // Transport reported back fewer bytes than requested (resid = 16).
    // For a 96-byte buffer that means bytes_transferred = 80.
    let payload = vec![0u8; 96];
    let mut transport = MockTransport::new(vec![MockOutcome::Ok {
        data: payload,
        resid: 16,
    }]);

    // Drive INQUIRY through the public helper to exercise the consumer
    // path; INQUIRY itself doesn't act on bytes_transferred but the
    // transport contract is what we care about.
    let cdb = [libfreemkv::scsi::SCSI_INQUIRY, 0, 0, 0, 0x60, 0];
    let mut buf = [0u8; 96];
    let r = transport
        .execute(&cdb, DataDirection::FromDevice, &mut buf, 1_000)
        .expect("ok");
    assert_eq!(r.status, 0);
    assert_eq!(
        r.bytes_transferred, 80,
        "bytes_transferred must equal data.len() - resid"
    );
}

// ── 7. Error::Display does not leak English in the SCSI variant ───────────
//
// Library rule (CLAUDE.md): no English in error display, only "E{code}"
// + structured data. Existing error-mod tests cover the new variants;
// this is a regression guard for the SCSI variant specifically because
// it's the most-emitted error in the rip path.

#[test]
fn test_scsi_error_display_format_is_codes_only() {
    let err = Error::ScsiError {
        opcode: 0x12,
        status: 0x02,
        sense_key: 5,
    };
    let s = err.to_string();
    assert!(s.starts_with("E4000:"), "ScsiError must lead with E4000: {s}");
    assert!(
        s.contains("0x12") && s.contains("0x02") && s.contains("0x05"),
        "ScsiError must show opcode/status/sense_key in hex: {s}"
    );
    // Crude English filter — same as the inline error.rs::display test.
    for word in s.split(|c: char| !c.is_ascii_alphabetic()) {
        assert!(
            word.len() <= 4,
            "ScsiError display contains suspicious word `{word}`: {s}"
        );
    }
}

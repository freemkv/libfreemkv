//! Integration tests for the SCSI error-decoding contract.
//!
//! v0.13.20 rewrote `scsi/linux.rs` to a synchronous blocking SG_IO and
//! consolidated sense parsing into the `parse_sense` helper that every
//! platform backend now shares. v0.13.23 replaced the `Error::ScsiError`
//! flat-fields shape with `{ opcode, status, sense: Option<ScsiSense> }`
//! so callers can route on structured sense data (key + ASC + ASCQ) via
//! [`Error::scsi_sense`] / [`Error::is_marginal_read`] /
//! [`ScsiSense::is_*`].
//!
//! The actual `ioctl(SG_IO, ...)` call is impossible to mock without a
//! kernel — see
//! `freemkv-private/docs/audits/2026-04-26-scsi-architecture-research.md`
//! for why the audit recommends against libc shims here. These tests
//! therefore pin the *contract* every backend must satisfy via a mock
//! `ScsiTransport`:
//!
//!   1. Healthy result → `Ok(ScsiResult { bytes_transferred = data.len() - resid })`.
//!   2. Transport-level failure (no SCSI status delivered: kernel
//!      timeout, USB bridge wedge, IOKit service error) →
//!      `Error::ScsiError { status: SCSI_STATUS_TRANSPORT_FAILURE, sense: None }`.
//!      `Error::is_scsi_transport_failure()` returns `true`. Used by
//!      `drive_has_disc` to detect the wedge signature.
//!   3. SCSI-level failure (drive replied CHECK CONDITION with sense) →
//!      `Error::ScsiError { status: 0x02, sense: Some(ScsiSense {…}) }`
//!      with the parsed key/ASC/ASCQ.
//!   4. `Error::is_marginal_read()` is `true` for MEDIUM ERROR /
//!      ABORTED COMMAND / RECOVERED ERROR / NO SENSE; `false` for
//!      HARDWARE / DATA PROTECT / UNIT ATTENTION / NOT READY / ILLEGAL
//!      REQUEST and for transport failures.
//!
//! Inline `parse_sense_tests` in `src/scsi/mod.rs` cover the pure parse
//! logic (descriptor 0x72/0x73 vs fixed 0x70/0x71, short-buffer, VALID
//! bit masking, unknown response codes, ASC/ASCQ offsets); this file
//! covers the consumer side — a real transport feeding a real Error
//! variant to a real call site (`scsi::inquiry`).

use libfreemkv::error::Error;
use libfreemkv::scsi::{
    DataDirection, SCSI_STATUS_CHECK_CONDITION, SCSI_STATUS_TRANSPORT_FAILURE,
    SENSE_KEY_ABORTED_COMMAND, SENSE_KEY_DATA_PROTECT, SENSE_KEY_HARDWARE_ERROR,
    SENSE_KEY_ILLEGAL_REQUEST, SENSE_KEY_MEDIUM_ERROR, SENSE_KEY_NOT_READY,
    SENSE_KEY_RECOVERED_ERROR, SENSE_KEY_UNIT_ATTENTION, ScsiResult, ScsiSense, ScsiTransport,
};

/// A scripted ScsiTransport. Each `execute()` consumes the next entry
/// from `script` and returns the corresponding outcome.
///
/// Outcomes mirror what each backend's `execute()` should produce after
/// the v0.13.23 sense plumbing: `Option<ScsiSense>` carrying the full
/// SPC-4 triple for drive-reported failures, `None` for transport-level
/// failures.
struct MockTransport {
    script: Vec<MockOutcome>,
    next: usize,
}

#[derive(Clone)]
enum MockOutcome {
    /// Healthy completion. `data` is what the transport wrote into the
    /// caller's data buffer (truncated to the buffer length); `resid`
    /// is reported back as `data.len() - bytes_transferred`.
    Ok { data: Vec<u8>, resid: i32 },
    /// Transport-level failure: `hdr.host_status = DID_TIME_OUT` on
    /// Linux, `kIOReturnError` on macOS, `DeviceIoControl` returning 0
    /// on Windows. Backends synthesise `SCSI_STATUS_TRANSPORT_FAILURE`
    /// with `sense = None`.
    TransportFailure,
    /// Drive replied with sense data (typically `SCSI_STATUS_CHECK_CONDITION`
    /// + a populated sense buffer).
    ScsiFailure { status: u8, sense: ScsiSense },
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
                status: SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            }),
            MockOutcome::ScsiFailure { status, sense } => Err(Error::ScsiError {
                opcode: cdb[0],
                status,
                sense: Some(sense),
            }),
        }
    }
}

/// Helper — build a CHECK CONDITION outcome with a given sense key
/// and zero ASC/ASCQ. Most consumer-side tests only care about the key.
fn check_cond(sense_key: u8) -> MockOutcome {
    MockOutcome::ScsiFailure {
        status: SCSI_STATUS_CHECK_CONDITION,
        sense: ScsiSense {
            sense_key,
            asc: 0,
            ascq: 0,
        },
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

// ── 2. Transport-level failure: no SCSI status, no sense data ─────────────

#[test]
fn test_transport_failure_surfaces_with_sense_none() {
    let mut transport = MockTransport::new(vec![MockOutcome::TransportFailure]);

    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    assert!(
        err.is_scsi_transport_failure(),
        "TransportFailure must satisfy is_scsi_transport_failure()"
    );
    assert!(
        err.scsi_sense().is_none(),
        "transport failure has no sense data"
    );
    assert!(
        !err.is_marginal_read(),
        "transport failure must not be classified as marginal-read"
    );
    match err {
        Error::ScsiError { status, sense, .. } => {
            assert_eq!(status, SCSI_STATUS_TRANSPORT_FAILURE);
            assert!(sense.is_none());
        }
        other => panic!("expected ScsiError, got {other:?}"),
    }
}

// ── 3. CHECK CONDITION + ILLEGAL REQUEST ──────────────────────────────────

#[test]
fn test_check_cond_illegal_request_carries_sense() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_ILLEGAL_REQUEST)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert_eq!(sense.sense_key, SENSE_KEY_ILLEGAL_REQUEST);
    assert!(sense.is_illegal_request());
    assert!(!err.is_marginal_read(), "ILLEGAL REQUEST is not marginal");
    assert!(!err.is_scsi_transport_failure());
}

// ── 4. CHECK CONDITION + NOT READY (drive_has_disc relies on this) ────────

#[test]
fn test_check_cond_not_ready_predicate() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_NOT_READY)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_not_ready(), "sense_key 2 ⇒ is_not_ready");
    assert!(!err.is_marginal_read(), "NOT READY is not marginal");
}

// ── 5. CHECK CONDITION + MEDIUM ERROR (canonical marginal-read) ───────────

#[test]
fn test_check_cond_medium_error_is_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_MEDIUM_ERROR)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_medium_error());
    assert!(sense.is_marginal());
    assert!(
        err.is_marginal_read(),
        "MEDIUM ERROR is the canonical marginal-read signal"
    );
}

// ── 6. CHECK CONDITION + ABORTED COMMAND (also marginal) ──────────────────

#[test]
fn test_check_cond_aborted_command_is_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_ABORTED_COMMAND)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_aborted_command());
    assert!(err.is_marginal_read(), "ABORTED COMMAND is marginal");
}

// ── 7. CHECK CONDITION + RECOVERED ERROR (drive recovered; marginal) ──────

#[test]
fn test_check_cond_recovered_error_is_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_RECOVERED_ERROR)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();
    assert!(
        err.is_marginal_read(),
        "RECOVERED ERROR is treated as marginal (drive recovered, retry-friendly class)"
    );
}

// ── 8. CHECK CONDITION + HARDWARE ERROR (NOT marginal — bail) ─────────────

#[test]
fn test_check_cond_hardware_error_not_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_HARDWARE_ERROR)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_hardware_error());
    assert!(
        !err.is_marginal_read(),
        "HARDWARE ERROR must not be marginal — drive failing, retry can't help"
    );
}

// ── 9. CHECK CONDITION + DATA PROTECT (NOT marginal — bail) ───────────────

#[test]
fn test_check_cond_data_protect_not_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_DATA_PROTECT)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_data_protect());
    assert!(
        !err.is_marginal_read(),
        "DATA PROTECT (AACS / region) must not be marginal — retry can't help"
    );
}

// ── 10. CHECK CONDITION + UNIT ATTENTION (NOT marginal — caller rescans) ──

#[test]
fn test_check_cond_unit_attention_not_marginal() {
    let mut transport = MockTransport::new(vec![check_cond(SENSE_KEY_UNIT_ATTENTION)]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("CHECK CONDITION must carry sense");
    assert!(sense.is_unit_attention());
    assert!(
        !err.is_marginal_read(),
        "UNIT ATTENTION must not be marginal — caller should rescan, not retry"
    );
}

// ── 11. ASC/ASCQ propagate from sense buffer to ScsiError.sense ───────────

#[test]
fn test_asc_ascq_round_trip_through_error() {
    let outcome = MockOutcome::ScsiFailure {
        status: SCSI_STATUS_CHECK_CONDITION,
        sense: ScsiSense {
            sense_key: SENSE_KEY_MEDIUM_ERROR,
            asc: 0x11,
            ascq: 0x05, // L-EC UNCORRECTABLE
        },
    };
    let mut transport = MockTransport::new(vec![outcome]);
    let err = libfreemkv::scsi::inquiry(&mut transport).unwrap_err();

    let sense = err.scsi_sense().expect("must carry sense");
    assert_eq!(sense.sense_key, SENSE_KEY_MEDIUM_ERROR);
    assert_eq!(sense.asc, 0x11);
    assert_eq!(sense.ascq, 0x05);
}

// ── 12. Healthy short transfer (resid > 0) ────────────────────────────────

#[test]
fn test_healthy_short_transfer_reports_partial_bytes() {
    // Transport reported back fewer bytes than requested (resid = 16).
    // For a 96-byte buffer that means bytes_transferred = 80.
    let payload = vec![0u8; 96];
    let mut transport = MockTransport::new(vec![MockOutcome::Ok {
        data: payload,
        resid: 16,
    }]);

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

// ── 13. Error::Display does not leak English in the SCSI variant ──────────
//
// Library rule (CLAUDE.md): no English in error display, only "E{code}"
// + structured data. Regression guard for the SCSI variant specifically
// because it's the most-emitted error in the rip path.

#[test]
fn test_scsi_error_display_format_is_codes_only() {
    let err = Error::ScsiError {
        opcode: 0x12,
        status: SCSI_STATUS_CHECK_CONDITION,
        sense: Some(ScsiSense {
            sense_key: SENSE_KEY_ILLEGAL_REQUEST,
            asc: 0x24,
            ascq: 0x00,
        }),
    };
    let s = err.to_string();
    assert!(
        s.starts_with("E4000:"),
        "ScsiError must lead with E4000: {s}"
    );
    assert!(
        s.contains("0x12") && s.contains("0x02") && s.contains("0x05") && s.contains("0x24"),
        "ScsiError must show opcode/status/key/asc in hex: {s}"
    );
    // Crude English filter — same as the inline error.rs::display test.
    for word in s.split(|c: char| !c.is_ascii_alphabetic()) {
        assert!(
            word.len() <= 4,
            "ScsiError display contains suspicious word `{word}`: {s}"
        );
    }
}

// ── 14. Transport-failure Display omits sense fields ──────────────────────

#[test]
fn test_scsi_transport_failure_display_short_form() {
    let err = Error::ScsiError {
        opcode: 0x28,
        status: SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    };
    let s = err.to_string();
    assert!(s.starts_with("E4000:"));
    assert!(s.contains("0x28") && s.contains("0xff"));
    // No sense triple should appear in the no-sense form.
    assert!(
        !s.contains("0x00/0x00/0x00"),
        "transport failure must not carry phantom sense: {s}"
    );
}

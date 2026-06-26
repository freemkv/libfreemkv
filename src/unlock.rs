//! Pluggable drive-unlock seam.
//!
//! libfreemkv knows the *seam*, never the *mechanism*. An [`Unlocker`] is
//! supplied by an external crate (e.g. `freemkv-unlock-ld`) and registered
//! once at process start via [`register_unlocker`]. At drive-prep the
//! registry is walked in registration order; the first unlocker whose
//! [`Unlocker::matches`] returns true is asked to [`Unlocker::unlock_drive`]
//! the drive by issuing its own CDBs through the raw [`ScsiTransport`].
//!
//! No firmware blobs, no unlock CDBs, no drive profiles live here — only
//! the trait, the registry, and the routing. If no unlocker matches, the
//! drive is left untouched and the caller falls back to the standard
//! host-certificate AACS handshake (the "OEM route").

use crate::aacs::Vid;
use crate::error::Result;
use crate::identity::DriveId;
use crate::scsi::ScsiTransport;
use std::sync::RwLock;

/// Why an [`Unlocker::unlock`] attempt produced no Volume ID. Structured and
/// English-free — applications render it. `Scsi` wraps the numeric error code
/// from [`crate::error::Error::code`] (the `Error` itself is not `Clone`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnlockError {
    /// This unlocker cannot put this drive's firmware into extended mode.
    FirmwareNotUnlockable,
    /// No usable (non-revoked) host certificate was available for the auth
    /// attempt. `mkb` is the disc MKB generation when known.
    NoUsableHostCert { mkb: Option<u32> },
    /// Every available host cert was revoked on this drive's HRL. `mkb` is the
    /// disc MKB generation when known.
    CertRevoked { mkb: Option<u32> },
    /// The drive rejected the auth handshake (non-revocation rejection / wedge).
    HandshakeRejected,
    /// Auth succeeded (or was skipped) but the Volume ID could not be read.
    VidUnavailable,
    /// A SCSI/transport error; carries the numeric [`crate::error::Error`] code.
    Scsi(u16),
}

impl From<crate::error::Error> for UnlockError {
    fn from(e: crate::error::Error) -> Self {
        UnlockError::Scsi(e.code())
    }
}

/// A pluggable drive-capability provider.
///
/// Unlockers are optional drive-capability providers. libfreemkv's AACS
/// layer is the always-present baseline; it uses an unlocker's capabilities
/// when one matches, and does the in-tree cert handshake (the
/// `AacsCertUnlocker` peer) when none do.
///
/// Implementors own everything about *how* a particular drive family is
/// driven: firmware upload, vendor CDBs, variant logic. libfreemkv only
/// hands over the raw SCSI transport and the drive identity.
pub trait Unlocker: Send + Sync {
    /// Stable, language-neutral identifier for this unlocker (logged).
    fn name(&self) -> &str;

    /// True if this unlocker handles the given drive.
    fn matches(&self, id: &DriveId) -> bool;

    /// Put the drive into extended-access mode (firmware/bootloader/whatever
    /// THIS unlocker needs) AND return the disc's Volume ID — folding the old
    /// `unlock_drive()` + `read_volume_id()` into one step. A firmware unlocker
    /// that cannot unlock returns [`UnlockError::FirmwareNotUnlockable`]; one
    /// that unlocks but has no OEM VID path returns [`UnlockError::VidUnavailable`].
    /// Either error makes libfreemkv fall through to the in-tree cert handshake.
    fn unlock(
        &self,
        scsi: &mut dyn ScsiTransport,
        id: &DriveId,
    ) -> std::result::Result<Vid, UnlockError>;

    /// Raise the drive to its maximum read speed. Default: no-op.
    fn set_max_read_speed(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId) -> Result<()> {
        Ok(())
    }
}

/// Process-wide ordered registry of unlockers.
static REGISTRY: RwLock<Vec<Box<dyn Unlocker>>> = RwLock::new(Vec::new());

/// Register an unlocker. Order is preserved; [`route_unlock`] tries each in
/// registration order and stops at the first whose `matches` is true.
///
/// Call once at process start (CLI / service `main`), before any rip. The
/// single `register_unlocker(...)` line is the entire plug — remove it (and
/// the unlocker crate) and libfreemkv still compiles and falls back to the
/// host-cert handshake.
pub fn register_unlocker(u: Box<dyn Unlocker>) {
    if let Ok(mut reg) = REGISTRY.write() {
        reg.push(u);
    }
}

/// Walk the registry in order and run the first matching unlocker, returning
/// its name AND the Volume ID it produced.
///
/// Returns:
///   * `Ok(Some((name, vid)))` — a registered unlocker matched, put the drive
///     into extended mode, and returned the OEM Volume ID. The caller stashes
///     the VID for the handshake phase and need not run the cert handshake.
///   * `Ok(None)` — no unlocker matched, OR the matching unlocker failed
///     ([`UnlockError`], logged). Either way the drive is usable in stock mode
///     and the caller falls through to the in-tree cert handshake. Folding an
///     unlock failure into `Ok(None)` keeps drive `init()` infallible — a drive
///     that simply isn't firmware-unlockable must not fail init.
pub(crate) fn route_unlock(
    scsi: &mut dyn ScsiTransport,
    id: &DriveId,
) -> Result<Option<(String, Vid)>> {
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // A poisoned lock means a prior unlocker panicked; treat as
        // "no unlocker available" so the cert fallback still runs.
        Err(_) => return Ok(None),
    };
    for u in reg.iter() {
        if u.matches(id) {
            let name = u.name().to_string();
            match u.unlock(scsi, id) {
                Ok(vid) => return Ok(Some((name, vid))),
                Err(e) => {
                    // A firmware unlocker that can't unlock / has no OEM VID:
                    // fall through to the cert handshake. Debug-only structured
                    // log (variant identifiers, no English prose).
                    tracing::warn!(
                        target: "freemkv::unlock",
                        unlocker = %name,
                        outcome = ?e,
                        "unlocker matched but produced no VID; falling through to cert handshake"
                    );
                    return Ok(None);
                }
            }
        }
    }
    Ok(None)
}

/// Walk the registry in order and ask the first matching unlocker to raise
/// the drive to its maximum read speed.
///
/// Mirrors [`route_unlock`]'s resolution so the SAME identified unlocker
/// that unlocks the drive is the one asked to set speed. Returns:
///   * `Ok(())` — the matching unlocker set max speed, or no unlocker
///     matched (no-op), or the matching unlocker has no speed capability
///     (its default no-op).
///   * `Err(_)` — the matching unlocker's `set_max_read_speed` failed. The
///     caller treats this as non-fatal (log and continue): a slow drive
///     still rips.
pub(crate) fn unlocker_set_max_read_speed(
    scsi: &mut dyn ScsiTransport,
    id: &DriveId,
) -> Result<()> {
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // Poisoned lock ⇒ treat as "no unlocker available" (no-op).
        Err(_) => return Ok(()),
    };
    for u in reg.iter() {
        if u.matches(id) {
            return u.set_max_read_speed(scsi, id);
        }
    }
    Ok(())
}

/// Number of registered unlockers — test/introspection helper.
#[doc(hidden)]
pub fn registered_count() -> usize {
    REGISTRY.read().map(|r| r.len()).unwrap_or(0)
}

/// Name of the first registered unlocker that matches `id`, without
/// running it. Used for drive-info display ("is this drive supported?")
/// before any unlock has been attempted.
pub(crate) fn matching_name(id: &DriveId) -> Option<String> {
    let reg = REGISTRY.read().ok()?;
    reg.iter()
        .find(|u| u.matches(id))
        .map(|u| u.name().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct NoopTransport;
    impl ScsiTransport for NoopTransport {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            })
        }
    }

    fn fake_id(vendor: &str) -> DriveId {
        let mut inquiry = vec![0u8; 96];
        let v = vendor.as_bytes();
        inquiry[8..8 + v.len().min(8)].copy_from_slice(&v[..v.len().min(8)]);
        DriveId::from_inquiry(&inquiry, "")
    }

    /// Fake unlocker that records whether its `unlock` ran, matches on vendor
    /// id, and serves a Volume ID (`Some` → `Ok(Vid)`; `None` →
    /// `Err(VidUnavailable)`, i.e. matched-but-no-OEM-VID → cert fallback) or
    /// records a `set_max_read_speed` call.
    struct FakeUnlocker {
        want_vendor: String,
        ran: Arc<AtomicBool>,
        /// VID this unlocker returns: `Some(vid)` → `unlock` yields `Ok(Vid)`;
        /// `None` → `unlock` yields `Err(UnlockError::VidUnavailable)` so
        /// `route_unlock` falls through to the cert handshake.
        vid: Option<[u8; 16]>,
        /// Records whether set_max_read_speed was invoked.
        speed_ran: Arc<AtomicBool>,
    }
    impl FakeUnlocker {
        fn new(vendor: &str, ran: Arc<AtomicBool>) -> Self {
            Self {
                want_vendor: vendor.into(),
                ran,
                // Default: a successful unlock returning an all-zero VID.
                vid: Some([0u8; 16]),
                speed_ran: Arc::new(AtomicBool::new(false)),
            }
        }
        fn with_vid(mut self, vid: Option<[u8; 16]>) -> Self {
            self.vid = vid;
            self
        }
        fn with_speed(mut self, speed_ran: Arc<AtomicBool>) -> Self {
            self.speed_ran = speed_ran;
            self
        }
    }
    impl Unlocker for FakeUnlocker {
        fn name(&self) -> &str {
            "fake"
        }
        fn matches(&self, id: &DriveId) -> bool {
            id.vendor_id.trim() == self.want_vendor
        }
        fn unlock(
            &self,
            _scsi: &mut dyn ScsiTransport,
            _id: &DriveId,
        ) -> std::result::Result<Vid, UnlockError> {
            self.ran.store(true, Ordering::SeqCst);
            match self.vid {
                Some(v) => Ok(Vid(v)),
                None => Err(UnlockError::VidUnavailable),
            }
        }
        fn set_max_read_speed(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId) -> Result<()> {
            self.speed_ran.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    /// `UnlockError` is `PartialEq` and a crate `Error` folds into
    /// `Scsi(code)` — the conversion `?`-callers rely on, English-free.
    #[test]
    fn unlock_error_from_crate_error_carries_code() {
        let e: UnlockError = crate::error::Error::AacsVidUnavailable.into();
        assert_eq!(e, UnlockError::Scsi(crate::error::E_AACS_VID_UNAVAILABLE));
        assert_ne!(
            UnlockError::NoUsableHostCert { mkb: Some(1) },
            UnlockError::NoUsableHostCert { mkb: Some(2) }
        );
    }

    /// A registered, matching unlocker runs and returns its name + VID; a
    /// non-matching identity leaves the registry untouched and routes to the
    /// cert fallback (`None`).
    ///
    /// Both assertions live in one test because the registry is process-wide
    /// and tests share it — running them as one case keeps the ordering
    /// deterministic regardless of test-harness threading.
    #[test]
    fn registry_routes_match_else_oem() {
        let ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(FakeUnlocker::new("MATCHVND", ran.clone())));

        // Matching identity → unlocker runs, returns its name + VID.
        let mut scsi = NoopTransport;
        let matched = route_unlock(&mut scsi, &fake_id("MATCHVND")).unwrap();
        assert_eq!(
            matched.as_ref().map(|(n, _)| n.as_str()),
            Some("fake"),
            "matching unlocker runs"
        );
        assert!(ran.load(Ordering::SeqCst), "unlock() was invoked");

        // Non-matching identity → no unlocker runs, cert path (None).
        ran.store(false, Ordering::SeqCst);
        let none = route_unlock(&mut scsi, &fake_id("OTHERVND")).unwrap();
        assert!(none.is_none(), "no match → cert fallback");
        assert!(
            !ran.load(Ordering::SeqCst),
            "unlock() not invoked on no-match"
        );
    }

    /// `route_unlock` returns the FIRST matching unlocker's VID. A matching
    /// unlocker that yields `Ok(Vid)` returns that VID (OEM path — cert
    /// handshake skipped). A matching unlocker whose `unlock` errors (no OEM
    /// VID), or no match at all, yields `Ok(None)` (cert fallback).
    ///
    /// Distinct vendor ids keep this independent of the other registry test
    /// despite the process-wide shared registry.
    #[test]
    fn route_unlock_returns_vid_else_cert() {
        let mut scsi = NoopTransport;

        // Unlocker WITH an OEM VID. Vendor ids are exactly 8 chars: INQUIRY
        // field [8..16] has no null padding to trim, so `matches` is exact.
        let vid = [0x5Au8; 16];
        register_unlocker(Box::new(
            FakeUnlocker::new("VIDVNDOR", Arc::new(AtomicBool::new(false))).with_vid(Some(vid)),
        ));

        // Matching identity → its VID is returned.
        let got = route_unlock(&mut scsi, &fake_id("VIDVNDOR")).unwrap();
        assert_eq!(
            got.map(|(_, v)| v),
            Some(Vid(vid)),
            "matching unlocker's OEM VID is used"
        );

        // Unlocker that MATCHES but has NO OEM VID path (unlock → Err) → cert.
        register_unlocker(Box::new(
            FakeUnlocker::new("NOVIDVND", Arc::new(AtomicBool::new(false))).with_vid(None),
        ));
        let got = route_unlock(&mut scsi, &fake_id("NOVIDVND")).unwrap();
        assert!(
            got.is_none(),
            "unlocker without OEM VID falls through to cert"
        );

        // No matching unlocker → Ok(None), cert fallback.
        let got = route_unlock(&mut scsi, &fake_id("UNKNWNVD")).unwrap();
        assert!(got.is_none(), "no match → cert fallback");
    }

    /// `unlocker_set_max_read_speed` consults the FIRST matching unlocker's
    /// `set_max_read_speed`. A matching unlocker is invoked; a non-match is a
    /// safe no-op (nothing invoked, `Ok(())`).
    ///
    /// Distinct vendor ids keep this independent of the other registry tests
    /// despite the process-wide shared registry.
    #[test]
    fn unlocker_set_max_read_speed_routes_match_else_noop() {
        let mut scsi = NoopTransport;

        let speed_ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(
            FakeUnlocker::new("SPEEDVND", Arc::new(AtomicBool::new(false)))
                .with_speed(speed_ran.clone()),
        ));

        // Matching identity → set_max_read_speed invoked.
        unlocker_set_max_read_speed(&mut scsi, &fake_id("SPEEDVND")).unwrap();
        assert!(
            speed_ran.load(Ordering::SeqCst),
            "set_max_read_speed() invoked on match"
        );

        // No matching unlocker → Ok(()), nothing invoked (safe no-op).
        speed_ran.store(false, Ordering::SeqCst);
        unlocker_set_max_read_speed(&mut scsi, &fake_id("NOSPEEDV")).unwrap();
        assert!(
            !speed_ran.load(Ordering::SeqCst),
            "no match → safe no-op, nothing invoked"
        );
    }

    /// `matching_name` reports the FIRST matching unlocker's name without
    /// running it (drive-info "is this drive supported?" before any unlock),
    /// and returns `None` for an unknown drive. `registered_count` counts the
    /// registered unlockers — pinning the two introspection helpers the routing
    /// tests never touch.
    ///
    /// The registry is process-wide and other unlock tests register into it
    /// concurrently, so the count is only asserted to be MONOTONIC across this
    /// test's own registration (never an exact delta) — registering an unlocker
    /// can only grow the count, never shrink it.
    #[test]
    fn matching_name_and_registered_count_introspection() {
        let before = registered_count();

        register_unlocker(Box::new(FakeUnlocker::new(
            "NAMEVNDR",
            Arc::new(AtomicBool::new(false)),
        )));

        // Registering an unlocker can only grow the count (other tests may also
        // be registering concurrently, so this is a monotonic check, not a
        // delta-of-exactly-one).
        assert!(
            registered_count() > before,
            "registered_count grows after register_unlocker"
        );

        // A matching identity reports the unlocker's name — and `matches`
        // is consulted WITHOUT running unlock_drive (introspection only).
        assert_eq!(
            matching_name(&fake_id("NAMEVNDR")).as_deref(),
            Some("fake"),
            "matching_name reports the supporting unlocker"
        );

        // An identity no registered unlocker matches → None (unsupported).
        assert!(
            matching_name(&fake_id("ZZNOMTCH")).is_none(),
            "matching_name is None for an unsupported drive"
        );
    }

    /// Registration order is preserved and the FIRST matching unlocker wins:
    /// when two unlockers both match the same identity, `route_unlock` runs the
    /// one registered earlier and never consults the later one. The routing
    /// docs promise "registration order; stops at the first whose `matches` is
    /// true" — this is the only test that registers two overlapping matchers to
    /// prove the ordering rather than a single-match no-op.
    #[test]
    fn route_unlock_first_registered_match_wins() {
        let mut scsi = NoopTransport;

        // Two unlockers that BOTH match vendor "DUPEVNDR"; the first registered
        // must be the one that runs.
        let first_ran = Arc::new(AtomicBool::new(false));
        let second_ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(FakeUnlocker::new("DUPEVNDR", first_ran.clone())));
        register_unlocker(Box::new(FakeUnlocker::new("DUPEVNDR", second_ran.clone())));

        let matched = route_unlock(&mut scsi, &fake_id("DUPEVNDR")).unwrap();
        assert_eq!(
            matched.as_ref().map(|(n, _)| n.as_str()),
            Some("fake"),
            "a match was routed"
        );
        assert!(
            first_ran.load(Ordering::SeqCst),
            "the FIRST-registered matching unlocker ran"
        );
        assert!(
            !second_ran.load(Ordering::SeqCst),
            "the later-registered unlocker was never consulted (first-match-wins)"
        );
    }
}

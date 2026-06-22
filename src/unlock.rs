//! Pluggable drive-unlock seam.
//!
//! libfreemkv knows the *seam*, never the *mechanism*. An [`Unlocker`] is
//! supplied by an external crate (e.g. `freemkv-unlock-ld`) and registered
//! once at process start via [`register_unlocker`]. At drive-prep the
//! registry is walked in registration order; the first unlocker whose
//! [`Unlocker::matches`] returns true is asked to [`Unlocker::unlock`] the
//! drive by issuing its own CDBs through the raw [`ScsiTransport`].
//!
//! No firmware blobs, no unlock CDBs, no drive profiles live here — only
//! the trait, the registry, and the routing. If no unlocker matches, the
//! drive is left untouched and the caller falls back to the standard
//! host-certificate AACS handshake (the "OEM route").

use crate::error::Result;
use crate::identity::DriveId;
use crate::scsi::ScsiTransport;
use std::sync::RwLock;

/// A pluggable drive unlocker.
///
/// Implementors own everything about *how* a particular drive family is
/// unlocked: firmware upload, vendor CDBs, variant logic. libfreemkv only
/// hands over the raw SCSI transport and the drive identity.
pub trait Unlocker: Send + Sync {
    /// Stable, language-neutral identifier for this unlocker (logged).
    fn name(&self) -> &str;

    /// True if this unlocker handles the given drive.
    fn matches(&self, id: &DriveId) -> bool;

    /// Unlock the drive. The unlocker issues its own CDBs through `scsi`.
    /// Returns `Ok(())` once the drive is prepared for reads.
    fn unlock(&self, scsi: &mut dyn ScsiTransport, id: &DriveId) -> Result<()>;

    /// Read the AACS Volume ID via this unlocker's OEM mechanism, if it
    /// has one.
    ///
    /// An [`Unlocker`] unlocks *drive functionality*, not just the disc:
    /// `unlock` is one capability, OEM VID retrieval is another. Once the
    /// matching unlocker is identified for a drive, libfreemkv uses it for
    /// BOTH unlock and VID. The OEM path returns the VID *without* the host
    /// certificate + HRL, decoupling VID from the cert handshake.
    ///
    /// Default is a no-op: an unlocker that provides no OEM VID path (or
    /// any unlocker that doesn't override this) returns `Ok(None)`, and
    /// libfreemkv falls back to the cert-based VID read. Implementors that
    /// can serve the VID directly (e.g. a per-drive OEM CDB) return
    /// `Ok(Some(vid))`.
    fn read_vid(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId) -> Result<Option<[u8; 16]>> {
        Ok(None)
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

/// Walk the registry in order and run the first matching unlocker.
///
/// Returns:
///   * `Ok(Some(name))` — a registered unlocker matched and unlocked the
///     drive; `name` is its [`Unlocker::name`].
///   * `Ok(None)` — no unlocker matched; the drive was left untouched and
///     the caller should fall through to the host-cert handshake.
///   * `Err(_)` — an unlocker matched but its `unlock` failed.
pub(crate) fn route_unlock(scsi: &mut dyn ScsiTransport, id: &DriveId) -> Result<Option<String>> {
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // A poisoned lock means a prior unlocker panicked; treat as
        // "no unlocker available" so the cert fallback still runs.
        Err(_) => return Ok(None),
    };
    for u in reg.iter() {
        if u.matches(id) {
            let name = u.name().to_string();
            u.unlock(scsi, id)?;
            return Ok(Some(name));
        }
    }
    Ok(None)
}

/// Walk the registry in order and ask the first matching unlocker for the
/// OEM Volume ID.
///
/// Mirrors [`route_unlock`]'s resolution so the SAME identified unlocker
/// that unlocks the drive is the one consulted for VID. Returns:
///   * `Ok(Some(vid))` — a matching unlocker served the VID via its OEM
///     path (no cert handshake needed; VID is decoupled from the HRL).
///   * `Ok(None)` — no unlocker matched, or the matching unlocker has no
///     OEM VID path; the caller falls through to the cert-based VID read.
///   * `Err(_)` — the matching unlocker's `read_vid` failed (e.g. the OEM
///     CDB returned a malformed response).
pub(crate) fn unlocker_read_vid(
    scsi: &mut dyn ScsiTransport,
    id: &DriveId,
) -> Result<Option<[u8; 16]>> {
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // Poisoned lock ⇒ treat as "no unlocker available" so the cert
        // fallback still runs.
        Err(_) => return Ok(None),
    };
    for u in reg.iter() {
        if u.matches(id) {
            return u.read_vid(scsi, id);
        }
    }
    Ok(None)
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

    /// Fake unlocker that records whether it ran, matches on vendor id, and
    /// optionally serves an OEM VID (mirroring the read_vid capability).
    struct FakeUnlocker {
        want_vendor: String,
        ran: Arc<AtomicBool>,
        /// VID this unlocker's OEM path returns: `Some(vid)` (capability
        /// present), `None` (no OEM path → cert fallback). `vid_ran` records
        /// whether read_vid was consulted.
        vid: Option<[u8; 16]>,
        vid_ran: Arc<AtomicBool>,
    }
    impl FakeUnlocker {
        fn new(vendor: &str, ran: Arc<AtomicBool>) -> Self {
            Self {
                want_vendor: vendor.into(),
                ran,
                vid: None,
                vid_ran: Arc::new(AtomicBool::new(false)),
            }
        }
        fn with_vid(mut self, vid: Option<[u8; 16]>, vid_ran: Arc<AtomicBool>) -> Self {
            self.vid = vid;
            self.vid_ran = vid_ran;
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
        fn unlock(&self, _scsi: &mut dyn ScsiTransport, _id: &DriveId) -> Result<()> {
            self.ran.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn read_vid(
            &self,
            _scsi: &mut dyn ScsiTransport,
            _id: &DriveId,
        ) -> Result<Option<[u8; 16]>> {
            self.vid_ran.store(true, Ordering::SeqCst);
            Ok(self.vid)
        }
    }

    /// A registered, matching unlocker runs; a non-matching identity leaves
    /// the registry untouched and routes to the OEM (cert) fallback.
    ///
    /// Both assertions live in one test because the registry is process-wide
    /// and tests share it — running them as one case keeps the ordering
    /// deterministic regardless of test-harness threading.
    #[test]
    fn registry_routes_match_else_oem() {
        let ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(FakeUnlocker::new("MATCHVND", ran.clone())));

        // Matching identity → unlocker runs, returns its name.
        let mut scsi = NoopTransport;
        let matched = route_unlock(&mut scsi, &fake_id("MATCHVND")).unwrap();
        assert_eq!(matched.as_deref(), Some("fake"), "matching unlocker runs");
        assert!(ran.load(Ordering::SeqCst), "unlock() was invoked");

        // Non-matching identity → no unlocker runs, OEM path (None).
        ran.store(false, Ordering::SeqCst);
        let none = route_unlock(&mut scsi, &fake_id("OTHERVND")).unwrap();
        assert!(none.is_none(), "no match → OEM/cert fallback");
        assert!(
            !ran.load(Ordering::SeqCst),
            "unlock() not invoked on no-match"
        );
    }

    /// `unlocker_read_vid` consults the FIRST matching unlocker's `read_vid`.
    /// A matching unlocker that returns `Some(vid)` yields that VID (the OEM
    /// path — cert handshake skipped). A matching unlocker that returns
    /// `None`, or no match at all, yields `Ok(None)` (cert fallback).
    ///
    /// Distinct vendor ids keep this independent of the other registry test
    /// despite the process-wide shared registry.
    #[test]
    fn unlocker_read_vid_routes_match_else_cert() {
        let mut scsi = NoopTransport;

        // Unlocker WITH an OEM VID capability. Vendor ids are exactly 8
        // chars: INQUIRY field [8..16] has no null padding to trim, so the
        // trimmed compare in `matches` is exact.
        let vid = [0x5Au8; 16];
        let vid_ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(
            FakeUnlocker::new("VIDVNDOR", Arc::new(AtomicBool::new(false)))
                .with_vid(Some(vid), vid_ran.clone()),
        ));

        // Matching identity → read_vid consulted, its VID used.
        let got = unlocker_read_vid(&mut scsi, &fake_id("VIDVNDOR")).unwrap();
        assert_eq!(got, Some(vid), "matching unlocker's OEM VID is used");
        assert!(vid_ran.load(Ordering::SeqCst), "read_vid() was consulted");

        // Unlocker that MATCHES but has NO OEM VID path (read_vid → None).
        let none_ran = Arc::new(AtomicBool::new(false));
        register_unlocker(Box::new(
            FakeUnlocker::new("NOVIDVND", Arc::new(AtomicBool::new(false)))
                .with_vid(None, none_ran.clone()),
        ));
        let got = unlocker_read_vid(&mut scsi, &fake_id("NOVIDVND")).unwrap();
        assert!(
            got.is_none(),
            "unlocker without OEM VID falls through to cert"
        );
        assert!(
            none_ran.load(Ordering::SeqCst),
            "read_vid() consulted even when it returns None"
        );

        // No matching unlocker → Ok(None), nothing consulted.
        let got = unlocker_read_vid(&mut scsi, &fake_id("UNKNWNVD")).unwrap();
        assert!(got.is_none(), "no match → cert fallback");
    }
}

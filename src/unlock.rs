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
    /// This unlocker self-verified against the hardware and does NOT apply to
    /// the mounted disc/drive — e.g. the CSS unlocker found the drive reports a
    /// non-DVD profile, or the cert unlocker found a non-AACS disc. The unlocker
    /// issued no unlock CDBs; the caller falls through to the next unlocker.
    /// Defense in depth: an unlocker never trusts the caller-declared kind alone.
    NotApplicable,
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

    /// True if this unlocker applies in the given [`UnlockCtx`]. A firmware
    /// unlocker keys off `ctx.drive_id` (disc kind irrelevant); the cert
    /// unlocker matches `ctx.kind == DiscKind::Aacs`; the CSS unlocker matches
    /// `DiscKind::Css`.
    fn matches(&self, ctx: &UnlockCtx) -> bool;

    /// Put the drive into extended-access mode (firmware/bootloader/whatever
    /// THIS unlocker needs) and report what it LEARNED — see [`Unlocked`]. The
    /// hardware side-effect (extended mode / auth flag) happens here; the
    /// returned value is only the learned data (VID, bus key), which libfreemkv
    /// files onto the disc/drive in one place. A firmware unlocker that cannot
    /// unlock returns [`UnlockError::FirmwareNotUnlockable`]; one that unlocks
    /// but has no OEM VID returns an [`Unlocked`] with `vid: None`. Either makes
    /// libfreemkv fall through to the next unlocker / the cert handshake.
    fn unlock(
        &self,
        scsi: &mut dyn ScsiTransport,
        ctx: &UnlockCtx,
    ) -> std::result::Result<Unlocked, UnlockError>;

    /// Raise the drive to its maximum read speed. Default: no-op.
    fn set_max_read_speed(&self, _scsi: &mut dyn ScsiTransport, _ctx: &UnlockCtx) -> Result<()> {
        Ok(())
    }
}

/// The bus-encryption class of the loaded disc, as cheaply probed before the
/// full structure scan. An [`Unlocker::matches`] keys off this (plus the drive
/// identity in [`UnlockCtx`]): a firmware unlocker ignores it; the cert unlocker
/// matches [`DiscKind::Aacs`]; the CSS unlocker matches [`DiscKind::Css`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscKind {
    /// Not yet probed — drive-prep phase, before any disc structure is read.
    Unknown,
    /// Disc carries no bus encryption; nothing to remove.
    Unencrypted,
    /// AACS (Blu-ray / UHD).
    Aacs,
    /// CSS (DVD-Video).
    Css,
}

/// Context handed to every [`Unlocker`] at the single dispatch point: the drive
/// identity and the disc's bus-encryption [`DiscKind`]. An unlocker reads only
/// what it needs — firmware keys off [`Self::drive_id`]; cert/CSS off
/// [`Self::kind`]. `#[non_exhaustive]` so more context (e.g. a host-cert source)
/// can be added later without breaking external unlockers.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct UnlockCtx<'a> {
    /// Identity of the drive being unlocked.
    pub drive_id: &'a DriveId,
    /// Bus-encryption class of the loaded disc (`Unknown` during drive-prep).
    pub kind: DiscKind,
    /// Scan options carrying the host-cert source for the AACS cert route.
    /// `None` for the drive-prep / CSS dispatches (they need no host certs).
    pub opts: Option<&'a crate::disc::ScanOptions>,
}

// Manual Debug: ScanOptions carries non-Debug key-source trait objects, so the
// derived impl can't see through `opts` — report only whether it's present.
impl std::fmt::Debug for UnlockCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnlockCtx")
            .field("drive_id", &self.drive_id)
            .field("kind", &self.kind)
            .field("has_opts", &self.opts.is_some())
            .finish()
    }
}

impl<'a> UnlockCtx<'a> {
    /// Construct a context for the given drive and disc kind (no host certs).
    pub fn new(drive_id: &'a DriveId, kind: DiscKind) -> Self {
        Self {
            drive_id,
            kind,
            opts: None,
        }
    }

    /// Construct a context carrying scan options (the AACS cert route's
    /// host-cert source).
    pub fn with_opts(
        drive_id: &'a DriveId,
        kind: DiscKind,
        opts: &'a crate::disc::ScanOptions,
    ) -> Self {
        Self {
            drive_id,
            kind,
            opts: Some(opts),
        }
    }
}

/// What an [`Unlocker::unlock`] LEARNED. The hardware side-effect (the drive
/// entering extended mode, or CSS auth setting the ASF flag) already happened
/// inside `unlock`; this carries only the learned data, which libfreemkv files
/// onto the disc/drive in a single place (the plugin never touches `Disc`).
///
/// - a firmware unlocker: `{ vid: Some, read_data_key: None }` (serves clear)
/// - the cert handshake:  `{ vid: Some, read_data_key: Some }` (AACS bus key)
/// - CSS auth:            `{ vid: None, read_data_key: None }` (reads enabled)
#[derive(Debug, Default, Clone)]
pub struct Unlocked {
    /// Disc Volume ID, if this route obtained one.
    pub vid: Option<Vid>,
    /// AACS 2.x bus key (`read_data_key`) from the cert handshake, if any.
    pub read_data_key: Option<[u8; 16]>,
    /// True when a firmware unlocker put the drive into clear-content mode: AACS
    /// bus encryption is then removed AT THE DRIVE (no bus key needed). The
    /// downstream bus-key gate credits this exactly like a cert `read_data_key`.
    pub drive_unlocked: bool,
    /// Numeric [`crate::error::Error`] code when the AACS bus-key read was
    /// ATTEMPTED and FAILED (cert path) — diagnostic only, so the gate can log
    /// WHY the bus key is missing. `None` when never attempted or it succeeded.
    pub read_data_key_err: Option<u16>,
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

/// Append the in-tree built-in unlockers (CSS bus-auth today; the AACS cert
/// handshake follows) exactly once, the first time any dispatch runs. They land
/// AFTER any client-registered firmware unlocker (e.g. `freemkv-unlock-ld`,
/// registered at process start, before the first rip), so the registry order is
/// firmware → cert → css. libfreemkv owns this order; clients never register the
/// built-ins — they only register the external plugins they link.
fn ensure_builtins() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        register_unlocker(Box::new(crate::css::auth::CssUnlocker));
    });
}

/// Outcome of one registry dispatch at a single [`UnlockCtx`]. Carries enough
/// for every caller: the firmware/cert path wants the learned [`Unlocked`], the
/// cert path also wants the *reason* on failure (to render "missing keys" vs
/// "host cert rejected"), and drive-prep just wants "did anything unlock".
#[derive(Debug)]
pub(crate) enum UnlockRoute {
    /// A matching unlocker removed the barrier; carries its name + learned data.
    Unlocked(String, Unlocked),
    /// A matching unlocker reported a capability failure — it does not apply,
    /// the disc is not its kind, or auth was rejected. NOT a transport fault.
    /// The caller renders the reason or falls through to the next phase. (The
    /// unlocker's name is already logged by `route_unlock`.)
    Failed(UnlockError),
    /// No registered unlocker matched this context.
    NoMatch,
}

/// Walk the registry in registration order and run the FIRST unlocker whose
/// [`Unlocker::matches`] is true for `ctx`, returning a structured
/// [`UnlockRoute`]. Only a genuine SCSI/transport fault
/// ([`UnlockError::Scsi`]) returns `Err` — the bus is broken, so the caller
/// must abort rather than silently fall through; everything else (capability
/// failure, no match) is an `Ok(UnlockRoute::…)` the caller folds.
pub(crate) fn route_unlock(scsi: &mut dyn ScsiTransport, ctx: &UnlockCtx) -> Result<UnlockRoute> {
    ensure_builtins();
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // A poisoned lock means a prior unlocker panicked; treat as
        // "no unlocker available" so the cert fallback still runs.
        Err(_) => return Ok(UnlockRoute::NoMatch),
    };
    // Walk in registration order — the registry is the single ordered place
    // that decides which unlocker runs first (register ld, then aacs, then css).
    for u in reg.iter() {
        if u.matches(ctx) {
            let name = u.name().to_string();
            return match u.unlock(scsi, ctx) {
                // A successful unlock removed the barrier — return what it
                // learned (VID and/or bus key, plus drive_unlocked) verbatim;
                // libfreemkv files those onto the disc/drive.
                Ok(unlocked) => Ok(UnlockRoute::Unlocked(name, unlocked)),
                // A genuine SCSI/transport fault is not "this disc can't be
                // unlocked" — the bus is broken. Propagate so the caller aborts
                // instead of falling through to another route that will also
                // fail on the same dead transport.
                Err(UnlockError::Scsi(code)) => {
                    tracing::error!(
                        target: "freemkv::unlock",
                        unlocker = %name,
                        code,
                        "unlocker hit a transport fault during unlock; aborting"
                    );
                    Err(crate::error::Error::ScsiError {
                        opcode: 0,
                        status: 0,
                        sense: None,
                    })
                }
                // A capability failure (not firmware-unlockable, NotApplicable,
                // cert rejected, …). Carry the reason so the caller can render
                // it; drive-prep simply falls through.
                Err(e) => {
                    tracing::debug!(
                        target: "freemkv::unlock",
                        unlocker = %name,
                        outcome = ?e,
                        "unlocker matched but did not unlock; caller folds the reason"
                    );
                    Ok(UnlockRoute::Failed(e))
                }
            };
        }
    }
    Ok(UnlockRoute::NoMatch)
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
    ctx: &UnlockCtx,
) -> Result<()> {
    ensure_builtins();
    let reg = match REGISTRY.read() {
        Ok(r) => r,
        // Poisoned lock ⇒ treat as "no unlocker available" (no-op).
        Err(_) => return Ok(()),
    };
    for u in reg.iter() {
        if u.matches(ctx) {
            return u.set_max_read_speed(scsi, ctx);
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
    // Drive-info introspection runs before any disc probe, so the kind is
    // Unknown — only a drive-keyed (firmware) unlocker can match here.
    let ctx = UnlockCtx::new(id, DiscKind::Unknown);
    ensure_builtins();
    let reg = REGISTRY.read().ok()?;
    reg.iter()
        .find(|u| u.matches(&ctx))
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
        /// When `Some(code)`, `unlock` yields `Err(UnlockError::Scsi(code))`
        /// (a transport fault) instead of consulting `vid`, so `route_unlock`
        /// propagates an error and aborts init.
        scsi_err: Option<u16>,
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
                scsi_err: None,
                speed_ran: Arc::new(AtomicBool::new(false)),
            }
        }
        fn with_vid(mut self, vid: Option<[u8; 16]>) -> Self {
            self.vid = vid;
            self
        }
        fn with_scsi_err(mut self, code: u16) -> Self {
            self.scsi_err = Some(code);
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
        fn matches(&self, ctx: &UnlockCtx) -> bool {
            ctx.drive_id.vendor_id.trim() == self.want_vendor
        }
        fn unlock(
            &self,
            _scsi: &mut dyn ScsiTransport,
            _ctx: &UnlockCtx,
        ) -> std::result::Result<Unlocked, UnlockError> {
            self.ran.store(true, Ordering::SeqCst);
            if let Some(code) = self.scsi_err {
                return Err(UnlockError::Scsi(code));
            }
            match self.vid {
                Some(v) => Ok(Unlocked {
                    vid: Some(Vid(v)),
                    read_data_key: None,
                    drive_unlocked: true,
                    read_data_key_err: None,
                }),
                None => Err(UnlockError::VidUnavailable),
            }
        }
        fn set_max_read_speed(
            &self,
            _scsi: &mut dyn ScsiTransport,
            _ctx: &UnlockCtx,
        ) -> Result<()> {
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
        let matched = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("MATCHVND"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(&matched, UnlockRoute::Unlocked(n, _) if n.as_str() == "fake"),
            "matching unlocker runs"
        );
        assert!(ran.load(Ordering::SeqCst), "unlock() was invoked");

        // Non-matching identity → no unlocker runs, cert path (NoMatch).
        ran.store(false, Ordering::SeqCst);
        let none = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("OTHERVND"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(none, UnlockRoute::NoMatch),
            "no match → cert fallback"
        );
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
        let got = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("VIDVNDOR"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(&got, UnlockRoute::Unlocked(_, u) if u.vid == Some(Vid(vid))),
            "matching unlocker's OEM VID is used"
        );

        // Unlocker that MATCHES but has NO OEM VID path (unlock → Err) → a
        // capability failure carrying the reason, NOT a transport fault.
        register_unlocker(Box::new(
            FakeUnlocker::new("NOVIDVND", Arc::new(AtomicBool::new(false))).with_vid(None),
        ));
        let got = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("NOVIDVND"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(got, UnlockRoute::Failed(UnlockError::VidUnavailable)),
            "unlocker without OEM VID is a capability failure → cert fallback"
        );

        // No matching unlocker → NoMatch, cert fallback.
        let got = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("UNKNWNVD"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(got, UnlockRoute::NoMatch),
            "no match → cert fallback"
        );
    }

    /// A matching unlocker that hits a genuine transport fault
    /// (`UnlockError::Scsi`) makes `route_unlock` PROPAGATE an `Err` rather
    /// than fold to `Ok(None)`: a dead bus must abort init, not silently fall
    /// through to a cert handshake that would also fail. Capability failures
    /// (`VidUnavailable` etc.) still fold to `Ok(None)` — proven by the sibling
    /// routing tests; this one pins the transport-fault exception.
    #[test]
    fn route_unlock_propagates_scsi_transport_fault() {
        let mut scsi = NoopTransport;

        register_unlocker(Box::new(
            FakeUnlocker::new("SCSIVNDR", Arc::new(AtomicBool::new(false)))
                .with_scsi_err(crate::error::E_SCSI_ERROR),
        ));

        let got = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("SCSIVNDR"), DiscKind::Unknown),
        );
        assert!(
            got.is_err(),
            "a transport fault during unlock aborts init (propagates Err)"
        );
        assert_eq!(
            got.unwrap_err().code(),
            crate::error::E_SCSI_ERROR,
            "propagated error is the canonical transport-error code"
        );
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
        unlocker_set_max_read_speed(
            &mut scsi,
            &UnlockCtx::new(&fake_id("SPEEDVND"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            speed_ran.load(Ordering::SeqCst),
            "set_max_read_speed() invoked on match"
        );

        // No matching unlocker → Ok(()), nothing invoked (safe no-op).
        speed_ran.store(false, Ordering::SeqCst);
        unlocker_set_max_read_speed(
            &mut scsi,
            &UnlockCtx::new(&fake_id("NOSPEEDV"), DiscKind::Unknown),
        )
        .unwrap();
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

        let matched = route_unlock(
            &mut scsi,
            &UnlockCtx::new(&fake_id("DUPEVNDR"), DiscKind::Unknown),
        )
        .unwrap();
        assert!(
            matches!(&matched, UnlockRoute::Unlocked(n, _) if n.as_str() == "fake"),
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

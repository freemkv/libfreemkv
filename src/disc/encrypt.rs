//! AACS scan steps: capture the key files, run the handshake, record the verdict.

use super::*;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf;

/// Result of SCSI AACS handshake (ECDH authentication).
/// Only available when scanning from a real drive, not ISO images.
pub(super) struct HandshakeResult {
    pub volume_id: [u8; 16],
    pub read_data_key: Option<[u8; 16]>,
    /// When `read_data_key` is `None` because the bus-key read FAILED (as opposed
    /// to a path that never attempts it), the error code from `read_data_keys`.
    pub read_data_key_err: Option<u16>,
    /// True when the VID came from an unlocker that unlocked the drive, so bus encryption is
    /// already removed AT THE DRIVE (firmware, not AKE). The bus-key gate must credit this the
    /// same as a cert `read_data_key`.
    pub drive_unlocked: bool,
}

// Redacting `Debug`: `volume_id` and `read_data_key` (the AACS 2.0 bus key) are
// secret; print only shape. Guarded by `handshake_result_debug_is_redacted`.
impl std::fmt::Debug for HandshakeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandshakeResult")
            .field("volume_id", &"<redacted>")
            .field("read_data_key", &self.read_data_key.map(|_| "<redacted>"))
            .field("read_data_key_err", &self.read_data_key_err)
            .field("drive_unlocked", &self.drive_unlocked)
            .finish()
    }
}

/// Where a scan's sectors come from, as far as AACS bus encryption is concerned.
#[derive(Clone, Copy)]
pub(super) enum BusSource<'a> {
    /// Image or folder: bus encryption is never applied on read.
    FileOrIso,
    /// Live drive whose handshake (cert or unlocker OEM VID) produced a result.
    Handshake(&'a HandshakeResult),
    /// Live drive whose handshake failed: nothing removed bus encryption.
    HandshakeFailed,
    /// Live drive claimed by a firmware unlocker that returned no VID. The
    /// `freemkv_unlock::Unlocker` contract: a claim means the bus barrier is lifted.
    FirmwareUnlocked,
}

/// The owned outcome of a scan's AACS bus step; [`BusSource`] is its borrowed view.
#[derive(Debug)]
pub(super) enum BusOutcome {
    /// Image or folder: no handshake runs.
    FileOrIso,
    /// Live drive: the handshake (cert AKE or unlocker OEM VID) produced a result.
    Handshake(HandshakeResult),
    /// Live drive: the handshake failed with this handshake-class error.
    Failed(Error),
    /// Live drive claimed by a firmware unlocker with no VID.
    FirmwareUnlocked,
}

impl BusOutcome {
    pub(super) fn source(&self) -> BusSource<'_> {
        match self {
            BusOutcome::FileOrIso => BusSource::FileOrIso,
            BusOutcome::Handshake(h) => BusSource::Handshake(h),
            BusOutcome::Failed(_) => BusSource::HandshakeFailed,
            BusOutcome::FirmwareUnlocked => BusSource::FirmwareUnlocked,
        }
    }

    fn handshake(&self) -> Option<&HandshakeResult> {
        match self {
            BusOutcome::Handshake(h) => Some(h),
            _ => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            BusOutcome::FileOrIso => "file",
            BusOutcome::Handshake(_) => "handshake",
            BusOutcome::Failed(_) => "failed",
            BusOutcome::FirmwareUnlocked => "firmware",
        }
    }
}

// Single source of truth for "is AACS bus encryption gone for this scan?". Add a NEW removal
// mechanism HERE, never in the gate.
pub(super) fn bus_encryption_removed(bus_encryption: bool, source: BusSource<'_>) -> bool {
    if !bus_encryption {
        return true; // never had it → nothing to remove
    }
    match source {
        BusSource::FileOrIso | BusSource::FirmwareUnlocked => true,
        BusSource::HandshakeFailed => false,
        BusSource::Handshake(h) => h.drive_unlocked || h.read_data_key.is_some(),
    }
}

// The single keep/refuse policy for a captured AACS disc: the error recorded beside the
// state. Without bus encryption a failed handshake blocks nothing, so it is not recorded.
pub(super) fn aacs_verdict(bus_encryption: bool, bus: &BusOutcome) -> Option<Error> {
    if !bus_encryption {
        return None;
    }
    if let BusOutcome::Failed(e) = bus {
        return handshake_class_error(e).or(Some(Error::AacsBusKeyUnavailable));
    }
    if !bus_encryption_removed(bus_encryption, bus.source()) {
        return Some(Error::AacsBusKeyUnavailable);
    }
    None
}

// A copy of `e` if it is a handshake-class failure (the drive's AACS route did not
// work); `None` for every other error.
pub(crate) fn handshake_class_error(e: &Error) -> Option<Error> {
    match e {
        Error::AacsNoHostCert { path } => Some(Error::AacsNoHostCert { path: path.clone() }),
        Error::AacsNoUsableHostCert => Some(Error::AacsNoUsableHostCert),
        Error::AacsHostCertRejected => Some(Error::AacsHostCertRejected),
        Error::AacsVidUnavailable => Some(Error::AacsVidUnavailable),
        Error::AacsBusKeyUnavailable => Some(Error::AacsBusKeyUnavailable),
        _ => None,
    }
}

// `Some(true)` for an `INDX0300` index.bdmv, `Some(false)` for another INDX version, `None`
// when there is no readable index (HD DVD, damage). `Err` only for a Stop.
pub(super) fn index_is_uhd(
    udf_fs: &udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> Result<Option<bool>> {
    let index = match udf_fs.read_file_prefix(reader, "/BDMV/index.bdmv", 8) {
        Ok(index) => index,
        Err(Error::Halted) => return Err(Error::Halted),
        Err(_) => return Ok(None),
    };
    let version = index.get(..8).and_then(|v| v.strip_prefix(b"INDX"));
    Ok(version.map(|v| v == b"0300"))
}

// Positive UHD evidence independent of the cert: index.bdmv version "0300"
// (BD-ROM Part 3) or an AACS2 MKB. Unknown is `false`; `Err` only for a Stop.
fn disc_is_uhd(udf_fs: &udf::UdfFs, reader: &mut dyn SectorSource) -> Result<bool> {
    use crate::aacs::mkb::{AacsVersion, mkb_type};
    if index_is_uhd(udf_fs, reader)? == Some(true) {
        return Ok(true);
    }
    let mkb = match udf_fs.read_file_prefix(reader, crate::aacs::PATH_MKB_RO, 64) {
        Ok(mkb) => mkb,
        Err(Error::Halted) => return Err(Error::Halted),
        Err(_) => return Ok(false),
    };
    Ok(mkb_type(&mkb)
        .map(|t| t.generation())
        .is_some_and(|g| matches!(g, AacsVersion::V20 | AacsVersion::V21)))
}

/// A disc's AACS files, read before any AACS command is sent to the drive.
pub(super) struct AacsCapture {
    /// `Unit_Key_RO.inf`, or why neither copy could be read.
    pub(super) uk_ro: Result<Vec<u8>>,
    /// The content cert's BEE flag; `None` when the cert is missing or unparseable.
    pub(super) cert_bee: Option<bool>,
    /// The fail-safe bus-encryption verdict (an unknown cert on a UHD counts as bus).
    pub(super) bus_encryption: bool,
    pub(super) version: u8,
    pub(super) mkb: Vec<u8>,
}

/// Where a capture reads from, which decides what an unreadable `Unit_Key_RO.inf` does.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CaptureFrom {
    /// Image or folder: the key-file error is recorded as read (E7000/E6000).
    Image,
    /// Live drive: E7031 is recorded, warned and the scan goes on (keys refused).
    Live,
}

// Reads the AACS key files (plain READs, no AACS command). `Err` only for a Stop.
pub(super) fn capture(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
    from: CaptureFrom,
) -> Result<AacsCapture> {
    use crate::aacs;
    let live = from != CaptureFrom::Image;
    let mut uk_ro = aacs::read_first(&aacs::role_paths(udf_fs, aacs::AacsRole::UnitKey), |p| {
        udf_fs.read_file(reader, p)
    });
    match (&uk_ro, from) {
        (Err(Error::Halted), _) => return Err(Error::Halted),
        (Err(e), CaptureFrom::Live) => {
            tracing::warn!(
                target: "freemkv::scan",
                phase = "aacs_capture",
                error_code = e.code(),
                "Unit_Key_RO.inf unreadable; scanning on, titles needing keys will not be keyed."
            );
            uk_ro = Err(Error::AacsKeyFileUnreadable);
        }
        _ => {}
    }
    let cc_raw = match aacs::read_first(
        &aacs::role_paths(udf_fs, aacs::AacsRole::ContentCert),
        |p| udf_fs.read_file(reader, p),
    ) {
        Ok(raw) => Some(raw),
        Err(Error::Halted) => return Err(Error::Halted),
        Err(Error::AacsNoKeys) => None,
        Err(e) => {
            tracing::warn!(
                target: "freemkv::scan",
                phase = "aacs_capture",
                error_code = e.code(),
                "content certificate unreadable; assuming the V20/UHD Unit_Key_RO stride"
            );
            None
        }
    };
    let cc = cc_raw.as_deref().and_then(aacs::inf::parse_content_cert);
    // Fail safe: an unparseable cert, or none on a live disc known to be UHD, may mean bus encryption.
    let bus_encryption = match &cc {
        Some(c) => c.bus_encryption,
        None => cc_raw.is_some() || (live && disc_is_uhd(udf_fs, reader)?),
    };
    // The bounded MKB reader, not `read_file` (the ~128 MiB padded MKB would fail).
    let mkb = match Disc::read_mkb_content(reader, udf_fs) {
        Ok(m) => m,
        Err(Error::Halted) => return Err(Error::Halted),
        Err(e) => {
            tracing::info!(
                target: "freemkv::disc",
                phase = "scan_aacs_mkb",
                error_code = e.code(),
                "MKB unreadable at scan; continuing with an empty MKB (disc-hash lookups are unaffected)."
            );
            Vec::new()
        }
    };
    // Stride version through the shared resolver (cert, then MKB type, then index.bdmv),
    // the same one `read_aacs_version` uses.
    let index = index_is_uhd(udf_fs, reader)?;
    let version =
        aacs::mkb::resolve_aacs_version(cc.as_ref().map(|c| c.version.major()), &mkb, index)
            .major();
    Ok(AacsCapture {
        uk_ro,
        cert_bee: cc.map(|c| c.bus_encryption),
        bus_encryption,
        version,
        mkb,
    })
}

// Builds the keys-free AACS state from a capture and the bus outcome, records the
// verdict, and emits the scan's single `aacs_verdict` event.
pub(super) fn resolve_aacs(
    cap: AacsCapture,
    bus: &BusOutcome,
) -> (Option<AacsState>, Option<Error>) {
    use crate::aacs;
    let mkb_version = aacs::mkb::mkb_version(&cap.mkb);
    let (state, error) = match cap.uk_ro {
        Err(e) => (None, Some(e)),
        Ok(uk_ro) => {
            let dh = aacs::inf::disc_hash(&uk_ro);
            let state = AacsState {
                version: cap.version,
                bus_encryption: cap.bus_encryption,
                mkb_version,
                disc_hash: aacs::inf::disc_hash_hex(&dh),
                volume_id: bus.handshake().map(|h| h.volume_id).unwrap_or([0u8; 16]),
                uk_ro,
                mkb: cap.mkb,
            };
            (Some(state), aacs_verdict(cap.bus_encryption, bus))
        }
    };
    let bus_removed = bus_encryption_removed(cap.bus_encryption, bus.source());
    let bus_error = match bus {
        BusOutcome::Failed(e) => Some(e.code()),
        _ => None,
    };
    let rdk_err = bus.handshake().and_then(|h| h.read_data_key_err);
    let has_vid = bus.handshake().is_some_and(handshake_has_volume_id);
    let recorded = error.as_ref().map(|e| e.code());
    if recorded.is_some() {
        tracing::warn!(
            target: "freemkv::scan",
            phase = "aacs_verdict",
            bus_encryption = cap.bus_encryption,
            bus_removed,
            bus_outcome = bus.label(),
            bus_error_code = bus_error,
            read_data_key_err = rdk_err,
            has_volume_id = has_vid,
            recorded_error_code = recorded,
            mkb_version,
            "AACS verdict: keys for this disc will be refused or cannot be derived"
        );
    } else {
        tracing::info!(
            target: "freemkv::scan",
            phase = "aacs_verdict",
            bus_encryption = cap.bus_encryption,
            bus_removed,
            bus_outcome = bus.label(),
            bus_error_code = bus_error,
            read_data_key_err = rdk_err,
            has_volume_id = has_vid,
            mkb_version,
            "AACS verdict: no blocking error"
        );
    }
    (state, error)
}

// Did the handshake actually carry a Volume ID? A VALUE, so it is testable without tracing.
fn handshake_has_volume_id(h: &HandshakeResult) -> bool {
    h.volume_id != [0u8; 16]
}

// The Read Data Key to de-bus with: dropped when the content cert's BEE flag is clear
// (AACS spec; libaacs gates on `bee && bec`). An unreadable cert keeps the key.
pub(super) fn bus_key(cap: &AacsCapture, bus: &BusOutcome) -> Option<[u8; 16]> {
    let rdk = bus.handshake().and_then(|h| h.read_data_key)?;
    if cap.cert_bee == Some(false) {
        tracing::info!(target: "freemkv::scan", "content cert BEE=0: bus removal off");
        return None;
    }
    Some(rdk)
}

// libfreemkv-side driver for the AACS cert route: owns host-cert collection, dispatches
// mutual-auth to the freemkv-unlock AACS unlocker.
struct AacsCertUnlocker<'a> {
    opts: &'a ScanOptions,
}

/// Why the AACS cert path produced no Volume ID (a dead bus is [`CertFail::Transport`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CertUnlockFailure {
    /// No host cert was available from any source (detected before the unlocker runs).
    NoHostCert,
    /// The unlocker had certs but none was usable host-side.
    NoUsableHostCert,
    /// The drive rejected every offered cert.
    Rejected,
    /// Auth succeeded but no Volume ID could be read.
    VidUnavailable,
}

/// A failed cert route: the dead bus aborts the scan, every other failure is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CertFail {
    Transport,
    Other(CertUnlockFailure),
}

impl From<freemkv_unlock::UnlockError> for CertFail {
    fn from(e: freemkv_unlock::UnlockError) -> Self {
        use freemkv_unlock::UnlockError as U;
        match e {
            U::Transport => CertFail::Transport,
            U::NoUsableHostCert => CertFail::Other(CertUnlockFailure::NoUsableHostCert),
            U::VidUnavailable => CertFail::Other(CertUnlockFailure::VidUnavailable),
            U::HandshakeRejected | U::NotApplicable => CertFail::Other(CertUnlockFailure::Rejected),
        }
    }
}

/// A successful cert-route step: a handshake result, or a firmware unlocker's claim.
enum CertRoute {
    Handshake(HandshakeResult),
    FirmwareUnlocked,
}

impl AacsCertUnlocker<'_> {
    // Runs the host-cert mutual-auth handshake. Host certs come from the key sources
    // best-first, with no disc input (no MKB read from the drive or the disc).
    fn authenticate(
        &self,
        session: &mut crate::drive::Drive,
    ) -> std::result::Result<HandshakeResult, CertFail> {
        let host_certs = Disc::collect_host_certs(self.opts, None);
        if host_certs.is_empty() {
            tracing::info!(
                target: "freemkv::disc",
                phase = "handshake_no_host_cert",
                "No AACS host certificate available from any key source, so the host-certificate handshake can't run."
            );
            return Err(CertFail::Other(CertUnlockFailure::NoHostCert));
        }

        // `run_bus` borrows the whole Drive, so hand it a copy of the identity.
        let drive_id = session.drive_id.clone();
        let fu_certs = crate::unlock_bridge::map_host_certs(&host_certs);
        // `matched` is only ever "AACS"/"DVD"/"" on this disc-keyed route, never a drive unlock.
        let (_matched, unlock_res) = crate::unlock_bridge::run_bus(
            session,
            &drive_id,
            freemkv_unlock::DiscKind::Aacs,
            &fu_certs,
        );
        // `Ok(None)` with certs present = a rejected handshake. `Err` = dead bus.
        let unlocked = match unlock_res {
            Ok(Some(u)) => u,
            Ok(None) => return Err(CertFail::Other(CertUnlockFailure::Rejected)),
            Err(e) => return Err(e.into()),
        };
        let Some(volume_id) = unlocked.vid else {
            return Err(CertFail::Other(CertUnlockFailure::VidUnavailable));
        };
        Ok(HandshakeResult {
            volume_id,
            read_data_key: unlocked.bus_key,
            // The generic `Unlocked` contract carries no bus-key error code.
            read_data_key_err: None,
            // The cert route never unlocks the drive; a firmware unlock arrives via the OEM VID.
            drive_unlocked: false,
        })
    }
}

// Maps a recorded cert-route failure to the handshake-class Error the scan records.
fn unlock_error_to_error(e: CertUnlockFailure) -> Error {
    match e {
        CertUnlockFailure::NoHostCert => Error::AacsNoHostCert {
            path: "<no host cert>".into(),
        },
        // Certs were offered but none survived the LOCAL keydb check (dead
        // pairing) — never reached the drive, so this is not a rejection.
        CertUnlockFailure::NoUsableHostCert => Error::AacsNoUsableHostCert,
        CertUnlockFailure::VidUnavailable => Error::AacsVidUnavailable,
        CertUnlockFailure::Rejected => Error::AacsHostCertRejected,
    }
}

/// Map a [`CertUnlockFailure`] to a structured [`crate::aacs::trace::UnlockOutcome`]
/// for the resolution trace (English-free). Scan has no MKB generation to report.
fn cert_unlock_outcome(e: CertUnlockFailure) -> crate::aacs::trace::UnlockOutcome {
    use crate::aacs::trace::UnlockOutcome;
    match e {
        CertUnlockFailure::NoHostCert | CertUnlockFailure::NoUsableHostCert => {
            UnlockOutcome::NoUsableHostCert { mkb: None }
        }
        CertUnlockFailure::VidUnavailable => UnlockOutcome::VidUnavailable,
        CertUnlockFailure::Rejected => UnlockOutcome::HandshakeRejected,
    }
}

// The scan's one bus step (§2.3), shared by AACS and CSS: the op token is checked before
// and after, ahead of any result mapping; the Drive's `Liveness` is busy throughout, as
// it spans the first keydb parse in `host_certs` (ST4-2).
pub(super) fn bus_step_guard<T>(
    session: &mut crate::drive::Drive,
    step: impl FnOnce(&mut crate::drive::Drive) -> T,
) -> Result<T> {
    let _busy = session.progress().map(crate::halt::Liveness::busy);
    session.check_token()?;
    let r = step(session);
    session.check_token()?;
    Ok(r)
}

// The AACS bus step of a live scan, run after the AACS files are captured. A dead bus
// aborts like `Drive::init`, and a Stop during the AKE is `Halted`.
pub(super) fn aacs_bus_step(
    session: &mut crate::drive::Drive,
    opts: &ScanOptions,
) -> Result<BusOutcome> {
    let t0 = std::time::Instant::now();
    let r = bus_step_guard(session, |s| Disc::do_handshake_cert(s, opts))?;
    tracing::info!(
        target: "freemkv::scan",
        phase = "do_handshake",
        ok = matches!(r, Ok(CertRoute::Handshake(_))),
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "end"
    );
    match r {
        Ok(CertRoute::Handshake(h)) => Ok(BusOutcome::Handshake(h)),
        Ok(CertRoute::FirmwareUnlocked) => Ok(BusOutcome::FirmwareUnlocked),
        Err(CertFail::Transport) => Err(crate::unlock_bridge::unlock_transport_error()),
        Err(CertFail::Other(f)) => {
            tracing::info!(
                target: "freemkv::disc",
                phase = "cert_handshake_outcome",
                outcome = ?cert_unlock_outcome(f),
                "AACS cert handshake produced no VID; a key source may still supply this disc's key."
            );
            Ok(BusOutcome::Failed(unlock_error_to_error(f)))
        }
    }
}

impl Disc {
    // VID acquisition: the OEM VID stashed at init, else nothing for a firmware-claimed
    // drive, else the cert route.
    fn do_handshake_cert(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> std::result::Result<CertRoute, CertFail> {
        // OEM VID shortcut: the unlocker matched and unlocked the drive at init, so it
        // serves clear content (credited as `drive_unlocked`); no bus key comes with it.
        if let Some(volume_id) = session.oem_vid() {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "oem_vid_ok",
                "Volume ID supplied by the drive unlocker at init; skipping the AACS host-certificate handshake."
            );
            return Ok(CertRoute::Handshake(HandshakeResult {
                volume_id,
                read_data_key: None,
                read_data_key_err: None,
                drive_unlocked: true,
            }));
        }
        // A firmware-claimed drive with no VID stays that way: the cert route's REPORT
        // KEY AGID cycle would poison it (see the guard test).
        if let Some(unlocker) = session.unlocker_name() {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "firmware_unlocked_no_vid",
                unlocker,
                "Drive claimed by a firmware unlocker with no Volume ID; not running the AACS cert handshake — a key source may supply the key."
            );
            return Ok(CertRoute::FirmwareUnlocked);
        }
        tracing::debug!(
            target: "freemkv::disc",
            phase = "oem_vid_none",
            "No firmware unlocker claimed the drive; running the AACS host-certificate handshake."
        );
        AacsCertUnlocker { opts }
            .authenticate(session)
            .map(CertRoute::Handshake)
    }

    // Collects every AACS host cert the caller carries, from DriveCredentials AND the
    // key-source layer, unioned. Empty means the graceful no-cert path.
    fn collect_host_certs(
        opts: &ScanOptions,
        mkb: Option<u32>,
    ) -> Vec<crate::aacs::types::HostCert> {
        crate::aacs::host_certs::collect_host_certs(opts, mkb)
    }
}

#[cfg(test)]
#[path = "encrypt_tests.rs"]
mod tests;

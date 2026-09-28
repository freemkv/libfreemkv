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
    if !bus_encryption_removed(true, bus.source()) {
        return Some(Error::AacsBusKeyUnavailable);
    }
    None
}

// A copy of `e` if it is a handshake-class failure (the drive's AACS route did not
// work); `None` for every other error.
pub(super) fn handshake_class_error(e: &Error) -> Option<Error> {
    match e {
        Error::AacsNoHostCert { path } => Some(Error::AacsNoHostCert { path: path.clone() }),
        Error::AacsHostCertRejected => Some(Error::AacsHostCertRejected),
        Error::AacsVidUnavailable => Some(Error::AacsVidUnavailable),
        Error::AacsBusKeyUnavailable => Some(Error::AacsBusKeyUnavailable),
        _ => None,
    }
}

impl Disc {
    // Sticky refusal: a failed handshake on a bus-encrypted disc, or a live disc without
    // its key file, admits no supplied key. With no AACS state the bus flag is unknown,
    // so bus encryption is assumed.
    pub(crate) fn bus_blocked_error(&self) -> Option<Error> {
        if matches!(self.aacs_error, Some(Error::AacsKeyFileUnreadable)) {
            return Some(Error::AacsKeyFileUnreadable);
        }
        let bus = self.aacs.as_ref().is_none_or(|a| a.bus_encryption);
        if !bus {
            return None;
        }
        self.aacs_error.as_ref().and_then(handshake_class_error)
    }
}

// Positive UHD evidence independent of the cert: index.bdmv version "0300"
// (BD-ROM Part 3) or an AACS2 MKB. Unknown is `false`.
fn disc_is_uhd(udf_fs: &udf::UdfFs, reader: &mut dyn SectorSource) -> bool {
    use crate::aacs::mkb::{AacsVersion, mkb_type};
    let index = udf_fs.read_file_prefix(reader, "/BDMV/index.bdmv", 8);
    if index.is_ok_and(|d| d.get(..8) == Some(&b"INDX0300"[..])) {
        return true;
    }
    udf_fs
        .read_file_prefix(reader, crate::aacs::PATH_MKB_RO, 64)
        .ok()
        .and_then(|m| mkb_type(&m).map(|t| t.generation()))
        .is_some_and(|g| matches!(g, AacsVersion::V20 | AacsVersion::V21))
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

// Reads the AACS key files (plain READs, no AACS command). `Err` only for a Stop: a
// missing or unreadable file is carried in the capture.
pub(super) fn capture(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
    live: bool,
) -> Result<AacsCapture> {
    use crate::aacs;
    let uk_ro = aacs::read_first(&aacs::role_paths(udf_fs, aacs::AacsRole::UnitKey), |p| {
        udf_fs.read_file(reader, p)
    });
    if matches!(uk_ro, Err(Error::Halted)) {
        return Err(Error::Halted);
    }
    let cc_raw = match aacs::read_first(
        &aacs::role_paths(udf_fs, aacs::AacsRole::ContentCert),
        |p| udf_fs.read_file(reader, p),
    ) {
        Ok(raw) => Some(raw),
        Err(Error::Halted) => return Err(Error::Halted),
        Err(_) => None,
    };
    let cc = cc_raw.as_deref().and_then(aacs::inf::parse_content_cert);
    // No-cert default = UHD (V20 stride), matching `read_aacs_version`.
    let version = cc
        .as_ref()
        .map(|c| c.version.major())
        .unwrap_or(aacs::mkb::AACS_MAJOR_UHD);
    // Fail safe: an unparseable cert, or none on a live disc known to be UHD, may mean bus encryption.
    let bus_encryption = match &cc {
        Some(c) => c.bus_encryption,
        None => cc_raw.is_some() || (live && disc_is_uhd(udf_fs, reader)),
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
                key_source: KeyOrigin::ExternalUk,
                vuk: None,
                unit_keys: vec![],
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

        // Borrow checker can't split `session` across `scsi_mut()` + `&drive_id`.
        let drive_id = session.drive_id.clone();
        let fu_certs = crate::unlock_bridge::map_host_certs(&host_certs);
        // `matched` is only ever "AACS"/"DVD"/"" on this disc-keyed route, never a drive unlock.
        let (_matched, unlock_res) = crate::unlock_bridge::run_bus(
            session.scsi_mut(),
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
        CertUnlockFailure::NoHostCert | CertUnlockFailure::NoUsableHostCert => {
            Error::AacsNoHostCert {
                path: "<no host cert>".into(),
            }
        }
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

// The AACS bus step of a live scan, run after the AACS files are captured. A dead bus
// aborts like `Drive::init`, and a Stop during the AKE is `Halted`.
pub(super) fn aacs_bus_step(
    session: &mut crate::drive::Drive,
    opts: &ScanOptions,
) -> Result<BusOutcome> {
    if session.is_halted() {
        return Err(Error::Halted);
    }
    let t0 = std::time::Instant::now();
    let r = Disc::do_handshake_cert(session, opts);
    tracing::info!(
        target: "freemkv::scan",
        phase = "do_handshake",
        ok = matches!(r, Ok(CertRoute::Handshake(_))),
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "end"
    );
    if session.is_halted() {
        return Err(Error::Halted);
    }
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
mod tests {
    use super::*;
    use crate::aacs;
    use crate::sector::SectorSource;
    use std::collections::HashMap;

    /// `HandshakeResult` carries the Volume ID and the AACS 2.0 bus (read-data)
    /// key; `Debug` must redact both. Sentinel 213 (0xD5).
    #[test]
    fn handshake_result_debug_is_redacted() {
        let hs = HandshakeResult {
            volume_id: [0xD5; 16],
            read_data_key: Some([0xD5; 16]),
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let d = format!("{hs:?}");
        assert!(
            !d.contains("213"),
            "HandshakeResult leaked VID/bus key: {d}"
        );
        assert!(
            d.contains("redacted"),
            "HandshakeResult missing marker: {d}"
        );
    }

    // In-memory disc + minimal UDF image with a single physical partition
    // (metadata_start == partition_start), per udf.rs::read_filesystem / ECMA-167.

    const PART_START: u32 = 4000;

    struct MemDisc {
        sectors: HashMap<u32, [u8; 2048]>,
    }
    impl MemDisc {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
        fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
            for (i, chunk) in bytes.chunks(2048).enumerate() {
                let mut s = [0u8; 2048];
                s[..chunk.len()].copy_from_slice(chunk);
                self.put(lba + i as u32, s);
            }
        }
    }
    impl SectorSource for MemDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let need = count as usize * 2048;
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    /// Extended File Entry ICB (tag 266) with one Short AD.
    fn build_file_icb(size: u32, data_lba: u32) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes());
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes());
        s[208..212].copy_from_slice(&0u32.to_le_bytes());
        s[212..216].copy_from_slice(&8u32.to_le_bytes());
        s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
        let start = buf.len();
        let name_field: Vec<u8> = if is_parent {
            Vec::new()
        } else {
            let mut v = vec![0x08u8];
            v.extend_from_slice(name.as_bytes());
            v
        };
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes());
        let mut fc = 0u8;
        if is_dir {
            fc |= 0x02;
        }
        if is_parent {
            fc |= 0x08;
        }
        fid[18] = fc;
        fid[19] = name_field.len() as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
        fid[36..38].copy_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        buf.resize(start + ((used + 3) & !3), 0);
    }

    struct AacsFile {
        name: &'static str,
        icb_lba: u32,
        data_lba: u32,
        contents: Vec<u8>,
    }

    fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
        let mut avdp = [0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put(256, avdp);
        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put(32, pd);
        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put(33, lvd);
        let mut td = [0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put(34, td);
        let mut fsd = [0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
        disc.put(PART_START, fsd);
    }

    /// Build a UDF tree with a single /AACS directory holding the given
    /// files. Returns the navigable UdfFs over `disc`.
    fn build_aacs_fs(disc: &mut MemDisc, files: &[AacsFile]) -> udf::UdfFs {
        build_aacs_fs_with_index(disc, files, None)
    }

    // As `build_aacs_fs`, plus `/BDMV/index.bdmv` holding `index` when given.
    fn build_aacs_fs_with_index(
        disc: &mut MemDisc,
        files: &[AacsFile],
        index: Option<&[u8]>,
    ) -> udf::UdfFs {
        let mut aacs_fids = Vec::new();
        push_fid(&mut aacs_fids, "", 50, true, true);
        for f in files {
            push_fid(&mut aacs_fids, f.name, f.icb_lba, false, false);
            disc.put(
                PART_START + f.icb_lba,
                build_file_icb(f.contents.len() as u32, f.data_lba),
            );
            disc.put_bytes(PART_START + f.data_lba, &f.contents);
        }
        disc.put(PART_START + 50, build_file_icb(aacs_fids.len() as u32, 51));
        disc.put_bytes(PART_START + 51, &aacs_fids);
        // Root referencing AACS.
        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "AACS", 50, true, false);
        if let Some(index) = index {
            let mut bdmv_fids = Vec::new();
            push_fid(&mut bdmv_fids, "", 70, true, true);
            push_fid(&mut bdmv_fids, "index.bdmv", 72, false, false);
            disc.put(PART_START + 72, build_file_icb(index.len() as u32, 7000));
            disc.put_bytes(PART_START + 7000, index);
            disc.put(PART_START + 70, build_file_icb(bdmv_fids.len() as u32, 71));
            disc.put_bytes(PART_START + 71, &bdmv_fids);
            push_fid(&mut root_fids, "BDMV", 70, true, false);
        }
        disc.put(PART_START + 10, build_file_icb(root_fids.len() as u32, 11));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(disc, 10);
        udf::read_filesystem(disc).expect("fs")
    }

    /// A content certificate: type byte@0 (0x00 = V10, 0x10 = V20),
    /// bus_encryption bit7@1, cc_id@14..20 (aacs/inf.rs parse_content_cert,
    /// which requires ≥20 bytes and reads the bus flag from `data[1] >> 7`).
    fn build_content_cert(cert_type: u8, bus_encryption: bool) -> Vec<u8> {
        let mut v = vec![0u8; 20];
        v[0] = cert_type;
        v[1] = if bus_encryption { 0x80 } else { 0x00 };
        v
    }

    // One Type-and-Version record (type 0x10), version BE u32 @ offset 8, then trailing zero
    // padding.
    fn build_mkb(version: u32, pad_to: usize) -> Vec<u8> {
        let mut v = Vec::new();
        // Type 0x10 record, length 16 (>= 12 so version is read).
        v.push(0x10);
        v.extend_from_slice(&[0x00, 0x00, 0x10]); // rec_len = 16 (3-byte BE)
        v.extend_from_slice(&[0u8; 4]); // bytes 4..8 reserved
        v.extend_from_slice(&version.to_be_bytes()); // version @ rec+8
        v.extend_from_slice(&[0u8; 4]); // pad record body to 16
        debug_assert_eq!(v.len(), 16);
        // Trailing zero padding (the "fixed-region" allocation).
        v.resize(pad_to, 0);
        v
    }

    // ---------------------------------------------------------------
    // Tests: capture + verdict (the keys-free state)
    // ---------------------------------------------------------------

    /// The keys-free state a scan keeps for `bus`, or the recorded error when there is none.
    fn vid_only(
        udf: &udf::UdfFs,
        reader: &mut dyn SectorSource,
        bus: &BusOutcome,
    ) -> Result<AacsState> {
        let live = !matches!(bus, BusOutcome::FileOrIso);
        let cap = capture(reader, udf, live)?;
        let (state, err) = resolve_aacs(cap, bus);
        state.ok_or_else(|| err.unwrap_or(Error::AacsNoKeys))
    }

    /// Missing Unit_Key_RO.inf (and its DUPLICATE) → Error::AacsNoKeys
    /// (encrypt.rs `.map_err(|_| Error::AacsNoKeys)`). Never panics.
    #[test]
    fn vid_only_missing_unit_key_ro_errors() {
        let mut disc = MemDisc::new();
        // AACS dir exists but has no Unit_Key_RO.inf.
        let udf = build_aacs_fs(&mut disc, &[]);
        let err = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso)
            .expect_err("missing Unit_Key_RO must error");
        assert!(matches!(err, Error::AacsNoKeys));
    }

    /// A V10 content cert (type 0x00, bus_encryption off) → version 1,
    /// bus_encryption false (encrypt.rs version match: Some(V10) → 1).
    #[test]
    fn vid_only_v10_cert_sets_version_1() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[
                AacsFile {
                    name: "Unit_Key_RO.inf",
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vec![0xAB; 32],
                },
                AacsFile {
                    name: "Content000.cer",
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: build_content_cert(0x00, false),
                },
            ],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        assert_eq!(st.version, 1, "V10 cert → AACS version 1");
        assert!(!st.bus_encryption);
        assert_eq!(st.key_source, KeyOrigin::ExternalUk);
        assert!(st.unit_keys.is_empty(), "vid-only resolves no keys");
        assert!(st.vuk.is_none());
    }

    /// A V20 content cert (type != 0x00) → version 2 (encrypt.rs Some(_) → 2).
    #[test]
    fn vid_only_v20_cert_sets_version_2() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[
                AacsFile {
                    name: "Unit_Key_RO.inf",
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vec![0xAB; 32],
                },
                AacsFile {
                    name: "Content000.cer",
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: build_content_cert(0x10, true),
                },
            ],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        assert_eq!(st.version, 2, "V20 cert → AACS version 2");
        assert!(st.bus_encryption, "cert bus_encryption bit must propagate");
    }

    // No content cert → version defaults to UHD (major 2), matching read_aacs_version (audit
    // #4). bus_encryption false (unreadable → off).
    #[test]
    fn vid_only_no_cert_defaults_version_uhd() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[AacsFile {
                name: "Unit_Key_RO.inf",
                icb_lba: 60,
                data_lba: 5000,
                contents: vec![0xAB; 32],
            }],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        assert_eq!(
            st.version,
            aacs::mkb::AACS_MAJOR_UHD,
            "no cert → default UHD (major 2)"
        );
        assert!(!st.bus_encryption);
    }

    /// disc_hash is SHA1 of the Unit_Key_RO.inf bytes, hex with 0x prefix
    /// and uppercase (aacs::inf::disc_hash + disc_hash_hex). The state's
    /// disc_hash must match independently computing it over the same bytes.
    #[test]
    fn vid_only_disc_hash_is_sha1_of_unit_key_ro() {
        let mut disc = MemDisc::new();
        let uk = vec![0x42u8; 100];
        let udf = build_aacs_fs(
            &mut disc,
            &[AacsFile {
                name: "Unit_Key_RO.inf",
                icb_lba: 60,
                data_lba: 5000,
                contents: uk.clone(),
            }],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        let expected = aacs::inf::disc_hash_hex(&aacs::inf::disc_hash(&uk));
        assert_eq!(st.disc_hash, expected);
        assert!(st.disc_hash.starts_with("0x"));
        // uk_ro must be stashed verbatim for the external resolver.
        assert_eq!(st.uk_ro, uk);
    }

    /// The MKB is trimmed to its real record length, NOT left as the full
    /// fixed-region zero-pad (encrypt.rs `mkb_bytes.truncate(mkb_content_len)`).
    /// A 16-byte record + 5000 bytes of padding must trim to 16.
    #[test]
    fn vid_only_trims_mkb_padding() {
        let mut disc = MemDisc::new();
        let mkb = build_mkb(77, 5000); // record + 4984 pad bytes
        assert_eq!(mkb.len(), 5000);
        let udf = build_aacs_fs(
            &mut disc,
            &[
                AacsFile {
                    name: "Unit_Key_RO.inf",
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vec![0xAB; 32],
                },
                AacsFile {
                    name: "MKB_RO.inf",
                    icb_lba: 62,
                    data_lba: 7000,
                    contents: mkb.clone(),
                },
            ],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        // Real record stream is the single 16-byte type-0x10 record.
        assert_eq!(
            st.mkb.len(),
            aacs::mkb::mkb_content_len(&mkb),
            "MKB must be trimmed to record-stream length, not the zero-pad"
        );
        assert_eq!(st.mkb.len(), 16);
        // Version comes from the type-0x10 record body @ offset 8.
        assert_eq!(st.mkb_version, Some(77));
    }

    /// With no MKB file present, mkb is empty and mkb_version is None
    /// (encrypt.rs `.unwrap_or_default()` → empty Vec; mkb_version(&[]) None).
    #[test]
    fn vid_only_no_mkb_is_empty() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[AacsFile {
                name: "Unit_Key_RO.inf",
                icb_lba: 60,
                data_lba: 5000,
                contents: vec![0xAB; 32],
            }],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        assert!(st.mkb.is_empty());
        assert_eq!(st.mkb_version, None);
    }

    /// A supplied handshake's volume_id and read_data_key propagate onto the
    /// AacsState (encrypt.rs `handshake.map(|h| h.volume_id)` /
    /// `handshake.and_then(|h| h.read_data_key)`).
    #[test]
    fn vid_only_propagates_handshake_vid_and_rdk() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[AacsFile {
                name: "Unit_Key_RO.inf",
                icb_lba: 60,
                data_lba: 5000,
                contents: vec![0xAB; 32],
            }],
        );
        let vid = [0x11u8; 16];
        let rdk = [0x22u8; 16];
        let hs = HandshakeResult {
            volume_id: vid,
            read_data_key: Some(rdk),
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st = vid_only(&udf, &mut disc, &BusOutcome::Handshake(hs)).expect("state");
        assert_eq!(st.volume_id, vid);
        // The bus key no longer propagates onto AacsState — it feeds the drive's
        // single de-bus point via the handshake, not a decrypt-time field.
    }

    // OEM bus-key gate: a bus-encrypted disc on a LIVE drive with no
    // read_data_key is blocked (AacsBusKeyUnavailable); three
    // non-regressing cases must still succeed.

    fn disc_with_cert(cert_type: u8, bus_encryption: bool) -> (MemDisc, udf::UdfFs) {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[
                AacsFile {
                    name: "Unit_Key_RO.inf",
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vec![0xAB; 32],
                },
                AacsFile {
                    name: "Content000.cer",
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: build_content_cert(cert_type, bus_encryption),
                },
            ],
        );
        (disc, udf)
    }

    /// Live-drive (handshake Some) + bus_encryption cert + NO read_data_key: the
    /// state is kept for triage, and the policy records AacsBusKeyUnavailable —
    /// the wrong-keys guard (a VID-only/OEM handshake cannot remove bus encryption).
    #[test]
    fn vid_only_bus_encrypted_live_drive_without_rdk_is_blocked() {
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let hs = hs_with(false, None);
        let src = BusOutcome::Handshake(hs);
        let st = vid_only(&udf, &mut disc, &src).expect("metadata kept");
        assert!(matches!(
            super::aacs_verdict(st.bus_encryption, &src),
            Some(Error::AacsBusKeyUnavailable)
        ));
    }

    /// Live-drive + bus_encryption cert + read_data_key PRESENT → Ok (the cert
    /// handshake produced the bus key, as required).
    #[test]
    fn vid_only_bus_encrypted_live_drive_with_rdk_ok() {
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: Some([0x22u8; 16]),
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st =
            vid_only(&udf, &mut disc, &BusOutcome::Handshake(hs)).expect("bus key present → ok");
        assert!(st.bus_encryption);
    }

    /// Finding: `is_drive_unlocker(matched)` on the cert route was provably always
    /// false — `run_bus` dispatches ONLY the disc-keyed AACS/DVD unlockers, never a
    /// firmware/drive unlocker — so a cert handshake's `drive_unlocked` is always
    /// false and bus removal there is credited solely via `read_data_key`. The
    /// GENUINE drive-unlock case (drive_unlocked:true, NO read_data_key) is the
    /// OEM/VID-only path, and the bus-key gate MUST credit it rather than
    /// hard-error `AacsBusKeyUnavailable`.
    #[test]
    fn drive_unlock_without_read_data_key_removes_bus_encryption() {
        // The cert route can never observe a firmware/drive unlocker: the only names
        // `run_bus` can return ("AACS"/"DVD"/"") classify as NOT a drive unlock, so
        // the replaced `is_drive_unlocker(matched)` was always false.
        for name in ["AACS", "DVD", ""] {
            assert!(
                !crate::unlock_bridge::is_drive_unlocker(name),
                "the disc-keyed cert route can never credit a drive unlock ({name:?})"
            );
        }
        // The real case: a bus-encrypted disc unlocked AT THE DRIVE (drive_unlocked)
        // with no cert read_data_key must resolve OK, not AacsBusKeyUnavailable.
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: true,
        };
        let st = vid_only(&udf, &mut disc, &BusOutcome::Handshake(hs))
            .expect("a drive-unlocked disc removes bus encryption even without a read_data_key");
        assert!(st.bus_encryption);
    }

    // Fail safe: a cert that reads but does not parse (unknown type byte) may
    // declare bus encryption, so a live drive without a Read Data Key refuses keys.
    #[test]
    fn vid_only_unparseable_cert_is_treated_as_bus_encrypted() {
        let (mut disc, udf) = disc_with_cert(0x55, false);
        let hs = hs_with(false, None);
        let src = BusOutcome::Handshake(hs);
        let st = vid_only(&udf, &mut disc, &src).expect("metadata kept");
        assert!(st.bus_encryption);
        assert!(matches!(
            super::aacs_verdict(st.bus_encryption, &src),
            Some(Error::AacsBusKeyUnavailable)
        ));
    }

    // No cert on a live drive without a Read Data Key: refuse keys only when the
    // disc is positively UHD (index.bdmv version "0300"); a BD ("0200") resolves.
    // A FAILED live handshake is still a live drive (must not fail open).
    #[test]
    fn vid_only_missing_cert_refuses_only_on_a_known_uhd_disc() {
        let uk = [AacsFile {
            name: "Unit_Key_RO.inf",
            icb_lba: 60,
            data_lba: 5000,
            contents: vec![0xAB; 32],
        }];
        let mut uhd = MemDisc::new();
        let udf = build_aacs_fs_with_index(&mut uhd, &uk, Some(b"INDX0300"));
        for src in [
            BusOutcome::Handshake(hs_with(false, None)),
            BusOutcome::Failed(Error::AacsHostCertRejected),
        ] {
            let st = vid_only(&udf, &mut uhd, &src).expect("state");
            assert!(
                st.bus_encryption,
                "UHD live drive must assume bus encryption"
            );
            let err = super::aacs_verdict(st.bus_encryption, &src);
            assert!(
                matches!(
                    err,
                    Some(Error::AacsBusKeyUnavailable | Error::AacsHostCertRejected)
                ),
                "{err:?}"
            );
        }
        let iso = vid_only(&udf, &mut uhd, &BusOutcome::FileOrIso).expect("ISO");
        assert!(super::aacs_verdict(iso.bus_encryption, &BusOutcome::FileOrIso).is_none());
        for index in [Some(&b"INDX0200"[..]), None] {
            let mut bd = MemDisc::new();
            let udf = build_aacs_fs_with_index(&mut bd, &uk, index);
            let src = BusOutcome::Handshake(hs_with(false, None));
            let st = vid_only(&udf, &mut bd, &src).expect("state");
            assert!(
                super::aacs_verdict(st.bus_encryption, &src).is_none(),
                "BD / unknown must resolve: {index:?}"
            );
        }
    }

    /// ISO scan (handshake None) of a bus_encryption disc → Ok. Bus encryption
    /// was already removed at read time; the gate must NOT fire without a
    /// handshake (no UHD-ISO-mux regression).
    #[test]
    fn vid_only_bus_encrypted_iso_no_handshake_ok() {
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("ISO bus disc → ok");
        assert!(st.bus_encryption);
    }

    /// Live drive whose cert handshake FAILED (handshake None + error) on a bus-encrypted
    /// disc: bus encryption was never removed, so this is not the file/ISO case. The
    /// handshake error must surface as `aacs_error`, alongside the non-secret metadata.
    #[test]
    fn failed_live_handshake_on_bus_encrypted_disc_surfaces_handshake_error() {
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let hs_err = Error::AacsNoHostCert {
            path: "<no host cert>".into(),
        };
        let scanned = finish_disc(&mut disc, udf, BusOutcome::Failed(hs_err));
        // Non-secret triage metadata survives; no keys, so nothing claims decryptability.
        let a = scanned.aacs.as_ref().expect("AACS metadata kept");
        assert!(!a.disc_hash.is_empty(), "disc hash must survive");
        assert_eq!(a.version, 2);
        assert!(a.bus_encryption && a.unit_keys.is_empty());
        assert!(matches!(
            scanned.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
        assert!(
            matches!(scanned.aacs_error, Some(Error::AacsNoHostCert { .. })),
            "handshake error must be preserved, got {:?}",
            scanned.aacs_error
        );
    }

    // Capture a synthetic AACS disc and `finish` it with the given bus outcome.
    fn finish_disc(disc: &mut MemDisc, udf: udf::UdfFs, bus: BusOutcome) -> Disc {
        let live = !matches!(bus, BusOutcome::FileOrIso);
        let cap = capture(disc, &udf, live).expect("capture");
        Disc::finish(disc, 10_000, udf, Some((cap, bus)), &ScanOptions::default()).expect("scan")
    }

    // Scan a synthetic AACS disc with the given handshake outcome.
    fn scan_cert_disc(
        cert_type: u8,
        bus: bool,
        hs: Option<HandshakeResult>,
        hs_err: Option<Error>,
    ) -> Disc {
        let (mut disc, udf) = disc_with_cert(cert_type, bus);
        let outcome = match (hs, hs_err) {
            (Some(h), _) => BusOutcome::Handshake(h),
            (None, Some(e)) => BusOutcome::Failed(e),
            (None, None) => BusOutcome::FileOrIso,
        };
        finish_disc(&mut disc, udf, outcome)
    }

    fn no_host_cert() -> Option<Error> {
        Some(Error::AacsNoHostCert {
            path: "<no host cert>".into(),
        })
    }

    /// A bus-blocked disc stays blocked: an unvalidated key (no samples) must not
    /// commit or clear the handshake error.
    #[test]
    fn bus_blocked_disc_rejects_supplied_keys() {
        let mut d = scan_cert_disc(0x10, true, None, no_host_cert());
        let err = d
            .decrypt_with(Key::Unit(vec![(0, [0x11; 16])]), &[])
            .expect_err("bus-blocked disc must refuse keys");
        assert_eq!(err.code(), crate::error::E_AACS_NO_HOST_CERT);
        assert!(d.inject_unit_keys(vec![(0, [0x11; 16])]).is_err());
        assert!(matches!(
            d.decrypt_keys(),
            crate::decrypt::DecryptKeys::None
        ));
        assert!(matches!(d.aacs_error, Some(Error::AacsNoHostCert { .. })));
    }

    /// The unlocker matrix must not credit the AACS route when its handshake failed.
    #[test]
    fn unlocker_matrix_aacs_requires_a_working_handshake() {
        let drive = crate::drive::Drive::from_transport_for_test(Box::new(NullScsi));
        let aacs_did_work = |d: &Disc| {
            d.unlocker_matrix(&drive)
                .into_iter()
                .find(|(n, _)| *n == "AACS")
                .map(|(_, w)| w)
        };
        let failed = scan_cert_disc(0x10, true, None, no_host_cert());
        assert_eq!(aacs_did_work(&failed), Some(false));
        let failed_bus_off = scan_cert_disc(0x00, false, None, no_host_cert());
        assert_eq!(aacs_did_work(&failed_bus_off), Some(false));
        let ok = scan_cert_disc(0x10, true, Some(hs_with(false, Some([0x22; 16]))), None);
        assert_eq!(aacs_did_work(&ok), Some(true));
    }

    /// AACS 1.0 (bus off): a failed handshake does not block keys, so it is not recorded;
    /// the gate keeps answering NoDiscKey (7022) and key-source failures can be stamped.
    #[test]
    fn failed_handshake_not_recorded_on_aacs10_disc() {
        let d = scan_cert_disc(0x00, false, None, no_host_cert());
        assert!(d.aacs.is_some());
        assert!(d.aacs_error.is_none(), "{:?}", d.aacs_error);
        let e = d.ensure_decryptable(false).expect_err("no key");
        assert_eq!(e.code(), crate::error::E_NO_DISC_KEY);
    }

    /// The `aacs = None` fail-safe: bus state unknown, so a recorded handshake-class
    /// error refuses supplied keys as for a bus-blocked disc.
    #[test]
    fn failed_handshake_without_aacs_state_refuses_keys() {
        let (mut disc, udf) = disc_with_cert(0x10, true);
        let mut d = finish_disc(&mut disc, udf, BusOutcome::FileOrIso);
        d.aacs = None;
        d.aacs_error = no_host_cert();
        assert!(
            d.decrypt_with(Key::Unit(vec![(1, [0x11; 16])]), &[])
                .is_err()
        );
        assert!(d.inject_unit_keys(vec![(1, [0x11; 16])]).is_err());
        assert!(matches!(d.aacs_error, Some(Error::AacsNoHostCert { .. })));
    }

    /// A live key-file failure recorded by a raw-copy scan refuses every key, even
    /// with no AACS state to say the disc is bus-encrypted.
    #[test]
    fn unreadable_key_file_refuses_keys() {
        let (mut disc, udf) = disc_with_cert(0x00, false);
        let mut d = finish_disc(&mut disc, udf, BusOutcome::FileOrIso);
        d.aacs = None;
        d.aacs_error = Some(Error::AacsKeyFileUnreadable);
        let e = d
            .decrypt_with(Key::Unit(vec![(1, [0x11; 16])]), &[])
            .expect_err("no key file, no key");
        assert_eq!(e.code(), crate::error::E_AACS_KEY_FILE_UNREADABLE);
        assert!(d.inject_unit_keys(vec![(1, [0x11; 16])]).is_err());
    }

    /// A bus-blocked disc does not query key sources at all.
    #[test]
    fn bus_blocked_disc_skips_key_sources() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        struct Counting;
        impl crate::keysource::KeySource for Counting {
            fn get_unit_keys(
                &self,
                _ctx: &dyn crate::keysource::ResolveCtx,
            ) -> Result<Vec<crate::aacs::types::UnitKey>> {
                CALLS.fetch_add(1, Ordering::Relaxed);
                Ok(vec![crate::aacs::types::UnitKey::new(0, [0x11; 16])])
            }
        }
        let mut d = scan_cert_disc(0x10, true, None, no_host_cert());
        let inputs = d.inputs().expect("inputs");
        let sources: Vec<Box<dyn crate::keysource::KeySource>> = vec![Box::new(Counting)];
        let (ok, trace) = crate::keysource::resolve_and_apply_traced(&sources, &inputs, &mut d);
        assert!(!ok);
        assert_eq!(CALLS.load(Ordering::Relaxed), 0, "no source may be queried");
        // One step names the block, so a UI can tell it from "no sources".
        assert_eq!(trace.keys.len(), 1);
        assert_eq!(trace.keys[0].who, crate::aacs::trace::BUS_BLOCKED);
    }

    /// A handshake with no bus key and no drive unlock keeps the metadata and
    /// reports AacsBusKeyUnavailable.
    #[test]
    fn handshake_without_bus_key_keeps_metadata_and_reports_error() {
        let d = scan_cert_disc(0x10, true, Some(hs_with(false, None)), None);
        let a = d.aacs.as_ref().expect("metadata kept");
        assert!(!a.disc_hash.is_empty());
        assert!(matches!(d.aacs_error, Some(Error::AacsBusKeyUnavailable)));
    }

    /// The decrypt gate reports the handshake reason, not a generic NoDiscKey.
    #[test]
    fn decrypt_gate_passes_handshake_errors_through() {
        let d = scan_cert_disc(0x10, true, None, no_host_cert());
        let e = d.ensure_decryptable(false).expect_err("no key");
        assert_eq!(e.code(), crate::error::E_AACS_NO_HOST_CERT);
        let d = scan_cert_disc(0x10, true, None, Some(Error::AacsHostCertRejected));
        let e = d.ensure_decryptable(false).expect_err("no key");
        assert_eq!(e.code(), crate::error::E_AACS_HOST_CERT_REJECTED);
    }

    struct NullScsi;
    impl crate::scsi::ScsiTransport for NullScsi {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: crate::scsi::DataDirection,
            _buf: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<crate::scsi::ScsiResult> {
            Ok(crate::scsi::ScsiResult {
                status: 0,
                sense: [0u8; 32],
                bytes_transferred: 0,
            })
        }
    }

    /// A firmware-unlocked drive with no VID lifted the bus barrier (Unlocker contract):
    /// no error, unlike a failed handshake. An image source (FileOrIso) likewise.
    #[test]
    fn firmware_unlocked_and_image_sources_report_no_bus_error() {
        for bus in [BusOutcome::FirmwareUnlocked, BusOutcome::FileOrIso] {
            let (mut disc, udf) = disc_with_cert(0x10, true);
            let scanned = finish_disc(&mut disc, udf, bus);
            assert!(scanned.aacs.is_some());
            assert!(scanned.aacs_error.is_none(), "{:?}", scanned.aacs_error);
        }
    }

    /// AACS 1.0 BD (V10 cert, bus_encryption off) on a live drive with NO
    /// read_data_key → Ok. read_data_key is legitimately absent for AACS 1.0;
    /// the gate must NOT fire when bus_encryption is false.
    #[test]
    fn vid_only_aacs10_live_drive_without_rdk_ok() {
        let (mut disc, udf) = disc_with_cert(0x00, false);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st = vid_only(&udf, &mut disc, &BusOutcome::Handshake(hs)).expect("AACS 1.0 → ok");
        assert!(!st.bus_encryption);
    }

    /// With NO handshake, volume_id defaults to all-zero (encrypt.rs
    /// `.unwrap_or([0u8; 16])`) and read_data_key is None.
    #[test]
    fn vid_only_no_handshake_zero_vid() {
        let mut disc = MemDisc::new();
        let udf = build_aacs_fs(
            &mut disc,
            &[AacsFile {
                name: "Unit_Key_RO.inf",
                icb_lba: 60,
                data_lba: 5000,
                contents: vec![0xAB; 32],
            }],
        );
        let st = vid_only(&udf, &mut disc, &BusOutcome::FileOrIso).expect("state");
        assert_eq!(st.volume_id, [0u8; 16]);
    }

    /// `handshake_has_volume_id` treats an all-zero Volume ID as absent and
    /// any non-zero Volume ID as present.
    #[test]
    fn handshake_has_volume_id_reports_presence_not_absence() {
        let with_vid = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: false,
        };
        assert!(
            super::handshake_has_volume_id(&with_vid),
            "a non-zero Volume ID must report as PRESENT"
        );

        let without = HandshakeResult {
            volume_id: [0u8; 16],
            ..with_vid
        };
        assert!(
            !super::handshake_has_volume_id(&without),
            "an all-zero Volume ID is the absent case"
        );

        // One bit of difference is still a VID: the check is != all-zero, not a
        // heuristic about how much of it looks populated.
        let mut barely = [0u8; 16];
        barely[15] = 1;
        assert!(
            super::handshake_has_volume_id(&HandshakeResult {
                volume_id: barely,
                ..with_vid
            }),
            "any non-zero byte makes a Volume ID present"
        );
    }

    /// The policy: a handshake error always wins; otherwise bus encryption left in
    /// place is AacsBusKeyUnavailable; otherwise no error.
    #[test]
    fn aacs_verdict_policy() {
        use super::{BusOutcome as B, aacs_verdict as verdict};
        assert!(matches!(
            verdict(true, &B::Handshake(hs_with(false, None))),
            Some(Error::AacsBusKeyUnavailable)
        ));
        assert!(verdict(true, &B::Handshake(hs_with(false, Some([0x22; 16])))).is_none());
        assert!(verdict(true, &B::Handshake(hs_with(true, None))).is_none());
        assert!(verdict(true, &B::FileOrIso).is_none());
        assert!(verdict(true, &B::FirmwareUnlocked).is_none());
        assert!(matches!(
            verdict(true, &B::Failed(Error::AacsHostCertRejected)),
            Some(Error::AacsHostCertRejected)
        ));
        // Bus off: a failed handshake blocks nothing, so it is not recorded.
        assert!(verdict(false, &B::Failed(Error::AacsHostCertRejected)).is_none());
    }

    // Tests: read_vid_oem and collect_host_certs coverage notes.

    fn fake_cert(tag: u8) -> aacs::types::HostCert {
        aacs::types::HostCert {
            private_key: [tag; 20],
            certificate: vec![tag; 92],
            private_key_v2: None,
            certificate_v2: None,
        }
    }

    /// A minimal in-test KeySource that yields no keys but a fixed cert list.
    struct CertSource(Vec<aacs::types::HostCert>);
    impl crate::KeySource for CertSource {
        fn get_unit_keys(
            &self,
            _ctx: &dyn crate::keysource::ResolveCtx,
        ) -> Result<Vec<crate::aacs::types::UnitKey>> {
            Ok(Vec::new())
        }
        fn host_certs(&self, _mkb: Option<u32>) -> Vec<aacs::types::HostCert> {
            self.0.clone()
        }
    }

    #[test]
    fn collect_host_certs_empty_when_no_credentials_no_sources() {
        let opts = ScanOptions::default();
        assert!(Disc::collect_host_certs(&opts, None).is_empty());
    }

    /// A transport that counts every SCSI command issued (via a shared counter
    /// that outlives the drive, so the count can be read before `Drive::drop`'s
    /// tray-unlock cleanup runs). Returns a benign CHECK CONDITION.
    struct CountingTransport(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl crate::scsi::ScsiTransport for CountingTransport {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _direction: crate::scsi::DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> crate::Result<crate::scsi::ScsiResult> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::scsi::ScsiResult {
                status: 2,
                bytes_transferred: 0,
                sense: [0u8; 32],
            })
        }
    }

    /// Anti-poison guard: when a FIRMWARE unlocker claimed the drive at init()
    /// but stashed no Volume ID, `do_handshake_cert` returns `(None, None)`
    /// WITHOUT running the AACS host-cert handshake — whose REPORT KEY AGID cycle
    /// would poison the drive so every later bare `0xAD` VID read aborts until the
    /// medium is reloaded. An unlocker that is picked does not fall through to
    /// another. Red-before-green: without the guard, the cert route's MKB read
    /// fires and the SCSI count is non-zero.
    #[test]
    fn firmware_claimed_no_vid_skips_cert_handshake_and_issues_no_scsi() {
        use std::sync::atomic::Ordering::SeqCst;
        let scsi_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(CountingTransport(
            scsi_count.clone(),
        )));
        drive.set_unlocker_name_for_test("freemkv");
        let opts = ScanOptions::default();
        let r = Disc::do_handshake_cert(&mut drive, &opts);
        // Read the count BEFORE `drive` drops (Drop::cleanup issues a tray-unlock).
        assert_eq!(
            scsi_count.load(SeqCst),
            0,
            "the guard must issue NO SCSI on a firmware-claimed drive with no VID"
        );
        assert!(
            matches!(r, Ok(CertRoute::FirmwareUnlocked)),
            "a firmware unlocker with no VID is a claim, not an error — a key source may still supply the key"
        );
    }

    #[test]
    fn collect_host_certs_from_credentials_only() {
        let opts = ScanOptions {
            credentials: Some(crate::DriveCredentials {
                host_certs: vec![fake_cert(1)],
            }),
            ..Default::default()
        };
        let certs = Disc::collect_host_certs(&opts, None);
        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].private_key, [1u8; 20]);
    }

    #[test]
    fn collect_host_certs_from_key_source_only() {
        let opts = ScanOptions {
            key_sources: vec![Box::new(CertSource(vec![fake_cert(2)]))],
            ..Default::default()
        };
        let certs = Disc::collect_host_certs(&opts, None);
        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].private_key, [2u8; 20]);
    }

    /// The two routes union: a cert in credentials AND one in a key source both
    /// reach the handshake.
    #[test]
    fn collect_host_certs_unions_credentials_and_sources() {
        let opts = ScanOptions {
            credentials: Some(crate::DriveCredentials {
                host_certs: vec![fake_cert(1)],
            }),
            key_sources: vec![
                Box::new(CertSource(vec![fake_cert(2)])),
                Box::new(CertSource(vec![])), // a source with no cert (e.g. online stub)
                Box::new(CertSource(vec![fake_cert(3)])),
            ],
            ..Default::default()
        };
        let mut tags: Vec<u8> = Disc::collect_host_certs(&opts, None)
            .iter()
            .map(|c| c.private_key[0])
            .collect();
        tags.sort_unstable();
        assert_eq!(tags, vec![1, 2, 3]);
    }

    // Cert-route outcome mapping: a dead bus is split out (it aborts the scan), every
    // other failure maps to a recorded Error and a trace step. No English in either.
    #[test]
    fn unlock_errors_split_transport_from_recorded_failures() {
        use freemkv_unlock::UnlockError as U;
        assert_eq!(CertFail::from(U::Transport), CertFail::Transport);
        let recorded = |u: U| match CertFail::from(u) {
            CertFail::Other(f) => f,
            CertFail::Transport => panic!("only Transport is a dead bus"),
        };
        match unlock_error_to_error(recorded(U::NoUsableHostCert)) {
            Error::AacsNoHostCert { path } => assert_eq!(path, "<no host cert>"),
            other => panic!("expected AacsNoHostCert, got {other:?}"),
        }
        assert!(matches!(
            unlock_error_to_error(CertUnlockFailure::NoHostCert),
            Error::AacsNoHostCert { .. }
        ));
        assert!(matches!(
            unlock_error_to_error(recorded(U::VidUnavailable)),
            Error::AacsVidUnavailable
        ));
        for u in [U::HandshakeRejected, U::NotApplicable] {
            assert!(matches!(
                unlock_error_to_error(recorded(u)),
                Error::AacsHostCertRejected
            ));
        }
    }

    #[test]
    fn cert_unlock_outcome_maps_to_structured_trace_step() {
        use crate::aacs::trace::UnlockOutcome;
        assert_eq!(
            cert_unlock_outcome(CertUnlockFailure::NoHostCert),
            UnlockOutcome::NoUsableHostCert { mkb: None }
        );
        assert_eq!(
            cert_unlock_outcome(CertUnlockFailure::VidUnavailable),
            UnlockOutcome::VidUnavailable
        );
        assert_eq!(
            cert_unlock_outcome(CertUnlockFailure::Rejected),
            UnlockOutcome::HandshakeRejected
        );
    }

    // ---------------------------------------------------------------
    // Tests: do_handshake_cert route selection (OEM VID vs cert)
    // ---------------------------------------------------------------

    /// OEM-VID short-circuit: when an unlocker stashed the disc's Volume ID at
    /// `init()` (`oem_vid` is `Some`), `do_handshake_cert` returns that VID and
    /// SKIPS the AACS host-cert handshake entirely — no SCSI is issued (contrast
    /// the cert route, which reads the MKB). The result credits `drive_unlocked`
    /// (bus encryption removed AT THE DRIVE) but carries NO `read_data_key`: the
    /// AACS-2.0 "VID served, but no bus key from this path" case the bus-key gate
    /// must later surface. Red-before-green: without the short-circuit the cert
    /// route fires and the SCSI count is non-zero.
    #[test]
    fn oem_vid_short_circuits_cert_handshake_with_drive_unlocked_and_no_rdk() {
        use std::sync::atomic::Ordering::SeqCst;
        let scsi_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(CountingTransport(
            scsi_count.clone(),
        )));
        let vid = [0x7Au8; 16];
        drive.set_oem_vid_for_test(vid);
        let opts = ScanOptions::default();
        let r = Disc::do_handshake_cert(&mut drive, &opts);
        // Read the count BEFORE `drive` drops (Drop::cleanup issues a tray-unlock).
        assert_eq!(
            scsi_count.load(SeqCst),
            0,
            "the OEM-VID short-circuit must issue NO SCSI (the cert route is skipped)"
        );
        let Ok(CertRoute::Handshake(hs)) = r else {
            panic!("an OEM VID is a success, not an error");
        };
        assert_eq!(hs.volume_id, vid, "the stashed OEM VID must propagate");
        assert!(
            hs.drive_unlocked,
            "an OEM VID means the drive is unlocked at the drive (firmware)"
        );
        assert_eq!(
            hs.read_data_key, None,
            "the OEM/VID-only path never produces a read_data_key"
        );
        assert_eq!(
            hs.read_data_key_err, None,
            "None here is 'not attempted', not a read failure — no error code"
        );
    }

    /// With no unlocker and no host cert the cert route runs host-side only: no SCSI
    /// (no drive MKB read), folding to AacsNoHostCert. The AKE itself is pinned by
    /// `scan_order_tests::no_unlocker_runs_cert_route_and_issues_ake`.
    #[test]
    fn no_unlocker_and_no_cert_issues_no_scsi() {
        use std::sync::atomic::Ordering::SeqCst;
        let scsi_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut drive = crate::drive::Drive::from_transport_for_test(Box::new(CountingTransport(
            scsi_count.clone(),
        )));
        let r = Disc::do_handshake_cert(&mut drive, &ScanOptions::default());
        assert_eq!(scsi_count.load(SeqCst), 0);
        assert!(matches!(
            r,
            Err(CertFail::Other(CertUnlockFailure::NoHostCert))
        ));
    }

    // ---------------------------------------------------------------
    // Tests: bus_encryption_removed — the single source of truth
    // ---------------------------------------------------------------

    fn hs_with(drive_unlocked: bool, rdk: Option<[u8; 16]>) -> HandshakeResult {
        HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: rdk,
            read_data_key_err: None,
            drive_unlocked,
        }
    }

    /// `bus_encryption_removed` is the single gate; one case per `BusSource`.
    #[test]
    fn bus_encryption_removed_covers_all_paths() {
        use super::{BusSource as B, bus_encryption_removed as removed};
        let plain = hs_with(false, None);
        let rdk = hs_with(false, Some([0x22u8; 16]));
        let unlocked = hs_with(true, None);
        let both = hs_with(true, Some([0x22u8; 16]));

        // Never bus-encrypted: nothing to remove, whatever the source.
        for src in [
            B::FileOrIso,
            B::HandshakeFailed,
            B::FirmwareUnlocked,
            B::Handshake(&plain),
        ] {
            assert!(removed(false, src));
        }
        // Bus-encrypted:
        assert!(removed(true, B::FileOrIso), "image: clear at read time");
        assert!(
            removed(true, B::FirmwareUnlocked),
            "unlocker lifted the barrier"
        );
        assert!(
            !removed(true, B::HandshakeFailed),
            "failed handshake removes nothing"
        );
        assert!(removed(true, B::Handshake(&rdk)), "AKE bus key");
        assert!(
            removed(true, B::Handshake(&unlocked)),
            "removed at the drive"
        );
        assert!(removed(true, B::Handshake(&both)));
        assert!(!removed(true, B::Handshake(&plain)), "wrong-keys guard");
    }

    #[test]
    fn bus_outcome_source_encodes_each_route() {
        use super::{BusOutcome as O, BusSource as B};
        assert!(matches!(
            O::Handshake(hs_with(false, None)).source(),
            B::Handshake(_)
        ));
        assert!(matches!(
            O::Failed(Error::AacsHostCertRejected).source(),
            B::HandshakeFailed
        ));
        assert!(matches!(O::FirmwareUnlocked.source(), B::FirmwareUnlocked));
        assert!(matches!(O::FileOrIso.source(), B::FileOrIso));
    }

    /// A handshake that carries a VID but `read_data_key: None` must still surface
    /// the VID onto the state. On a non-bus-encrypted disc (so the bus-key gate
    /// does not fire) the VID propagates; the bus key itself no longer lives on
    /// AacsState — it feeds the drive's single de-bus point via the handshake.
    #[test]
    fn vid_only_surfaces_vid_with_absent_read_data_key() {
        let (mut disc, udf) = disc_with_cert(0x00, false); // V10, bus off
        let vid = [0x33u8; 16];
        let hs = HandshakeResult {
            volume_id: vid,
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st = vid_only(&udf, &mut disc, &BusOutcome::Handshake(hs)).expect("state");
        assert_eq!(
            st.volume_id, vid,
            "the VID must propagate even without an RDK"
        );
    }
}

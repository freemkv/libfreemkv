//! AACS encryption resolution — key derivation, SCSI handshake, VUK lookup.

use super::*;
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf;

/// Result of SCSI AACS handshake (ECDH authentication).
/// Only available when scanning from a real drive, not ISO images.
#[derive(Debug)]
pub(super) struct HandshakeResult {
    pub volume_id: [u8; 16],
    pub read_data_key: Option<[u8; 16]>,
    /// When `read_data_key` is `None` because the bus-key read FAILED (as opposed
    /// to a path that never attempts it), the error code from `read_data_keys`.
    /// Carried so the downstream bus-key gate can log WHY the bus key is missing
    /// instead of a bare "unavailable" — the difference between a diagnosable log
    /// and archaeology.
    pub read_data_key_err: Option<u16>,
    /// True when the VID came from an unlocker (in `freemkv-unlock`) that
    /// unlocked the drive. Such a drive serves CLEAR
    /// content, so AACS bus encryption is already removed AT THE DRIVE — the same
    /// end state a successful cert handshake's `read_data_key` provides, just via
    /// firmware instead of the AKE. The bus-key gate MUST credit this as a valid
    /// bus-removal: bus encryption is unremovable only when NEITHER the firmware
    /// unlocked the drive NOR the cert handshake yielded a bus key. Without this,
    /// a SUCCESSFUL unlock (VID present, `read_data_key: None`) paradoxically trips
    /// the gate and blocks ALL key resolution (incl. the online source).
    pub drive_unlocked: bool,
}

/// Single source of truth for "is AACS bus encryption gone for this scan?". The
/// gate asks ONLY this — `if !removed { error }` — never enumerating cases. Bus
/// encryption is gone when ANY of these holds:
///   - the disc never had it (`!bus_encryption`): nothing to remove;
///   - file/ISO reads (`handshake == None`): content is already clear at read time;
///   - an unlocker unlocked the drive (`drive_unlocked`): it serves clear
///     content;
///   - the cert handshake produced the bus key (`read_data_key`).
///
/// Add a NEW removal mechanism HERE, never in the gate.
fn bus_encryption_removed(bus_encryption: bool, handshake: Option<&HandshakeResult>) -> bool {
    if !bus_encryption {
        return true; // never had it → nothing to remove
    }
    match handshake {
        None => true, // file/ISO: clear at read time
        Some(h) => h.drive_unlocked || h.read_data_key.is_some(),
    }
}

/// libfreemkv-side driver for the AACS cert route. It owns the host-cert
/// collection (a keysource concern that stays in libfreemkv) and then dispatches
/// the actual mutual-auth to the `freemkv-unlock` AACS unlocker via the
/// [`crate::unlock_bridge`]. The firmware (drive-prep) and CSS routes dispatch
/// the same way at their own call sites; this one carries the host certs.
struct AacsCertUnlocker<'a> {
    opts: &'a ScanOptions,
}

/// Why the AACS cert path produced no Volume ID. Distinguishes the libfreemkv-
/// side "no host cert at all" case (which carries the disc MKB generation for
/// the outcome trace) from the unlocker-reported [`freemkv_unlock::UnlockError`].
enum CertUnlockFailure {
    /// No host cert was available from any source — detected in libfreemkv
    /// before the unlocker runs, so the MKB generation is still known.
    NoHostCert { mkb: Option<u32> },
    /// The AACS unlocker ran and reported a specific failure.
    Unlock(freemkv_unlock::UnlockError),
}

impl AacsCertUnlocker<'_> {
    /// Run the host-certificate mutual-auth handshake: collect non-compiled-in
    /// host certs from the key sources + credentials, then hand them to the AACS
    /// unlocker (via the `freemkv-unlock` dispatch), which tries each cert
    /// (wedge-guarded) and on success yields the Volume ID + `read_data_key`
    /// (the AACS 2.0 bus key). Returns a [`CertUnlockFailure`] on every no-VID
    /// outcome.
    fn authenticate(
        &self,
        session: &mut crate::drive::Drive,
    ) -> std::result::Result<HandshakeResult, CertUnlockFailure> {
        use crate::aacs;
        use freemkv_unlock::UnlockError;

        // MKB generation (best-effort) — forwarded to each source's
        // `host_certs(mkb)` so a source MAY select a generation-appropriate cert
        // (the default impl ignores it). A read failure leaves it `None`.
        let mkb_gen = aacs::inf::read_mkb_from_drive(session.scsi_mut())
            .ok()
            .and_then(|m| aacs::mkb::mkb_version(&m));

        // Host certs are keysource-served, never compiled in — unioned from the
        // explicit `DriveCredentials` and the key-source layer. With ZERO certs
        // the cert route cannot run: NoHostCert (folded to AacsNoHostCert by the
        // caller, preserving the graceful path-1 disc-hash → VUK fallback). This
        // is detected here, where the MKB generation is still in hand.
        let host_certs = Disc::collect_host_certs(self.opts, mkb_gen);
        if host_certs.is_empty() {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "handshake_no_host_cert",
                "No AACS host certificate available from any key source, so the host-certificate handshake can't run."
            );
            return Err(CertUnlockFailure::NoHostCert { mkb: mkb_gen });
        }

        // Hand the collected certs to the AACS unlocker. The cert-route bus
        // removal depends on the read_data_key, NOT a drive unlock. The
        // borrow checker can't split `session` across `scsi_mut()` + `&drive_id`
        // through method calls, so clone the (cheap) identity first.
        let drive_id = session.drive_id.clone();
        let fu_certs = crate::unlock_bridge::map_host_certs(&host_certs);
        let (_, unlock_res) = crate::unlock_bridge::run_bus(
            session.scsi_mut(),
            &drive_id,
            freemkv_unlock::DiscKind::Aacs,
            &fu_certs,
        );
        let unlocked = unlock_res.map_err(CertUnlockFailure::Unlock)?;
        // The cert handshake yields a VID on success; its absence is VidUnavailable.
        let Some(volume_id) = unlocked.vid else {
            return Err(CertUnlockFailure::Unlock(UnlockError::VidUnavailable));
        };
        Ok(HandshakeResult {
            volume_id,
            read_data_key: unlocked.bus_key,
            // The generic `Unlocked` contract carries no bus-key error code; the
            // AACS-specific "why the read_data_key read failed" diagnostic does
            // not cross the seam. The bus-key gate keys off presence, not cause.
            read_data_key_err: None,
            drive_unlocked: unlocked.drive_unlocked,
        })
    }
}

/// Map a [`CertUnlockFailure`] back to the `Error` variant `do_handshake_cert`
/// has always surfaced, so `scan_with`'s rendering and the path-1 disc-hash →
/// VUK fallback are byte-for-byte unchanged. (`NoHostCert` keeps the
/// `<no host cert>` sentinel.)
fn unlock_error_to_error(e: &CertUnlockFailure) -> Error {
    use freemkv_unlock::UnlockError;
    match e {
        CertUnlockFailure::NoHostCert { .. }
        | CertUnlockFailure::Unlock(UnlockError::NoUsableHostCert) => Error::AacsNoHostCert {
            path: "<no host cert>".into(),
        },
        CertUnlockFailure::Unlock(UnlockError::VidUnavailable) => Error::AacsVidUnavailable,
        CertUnlockFailure::Unlock(
            UnlockError::HandshakeRejected | UnlockError::NotApplicable | UnlockError::Transport,
        ) => Error::AacsHostCertRejected,
    }
}

/// Map a [`CertUnlockFailure`] to a structured [`crate::aacs::trace::UnlockOutcome`]
/// for the resolution trace (English-free).
fn cert_unlock_outcome(e: &CertUnlockFailure) -> crate::aacs::trace::UnlockOutcome {
    use crate::aacs::trace::UnlockOutcome;
    use freemkv_unlock::UnlockError;
    match e {
        CertUnlockFailure::NoHostCert { mkb } => UnlockOutcome::NoUsableHostCert { mkb: *mkb },
        CertUnlockFailure::Unlock(UnlockError::NoUsableHostCert) => {
            UnlockOutcome::NoUsableHostCert { mkb: None }
        }
        CertUnlockFailure::Unlock(UnlockError::VidUnavailable) => UnlockOutcome::VidUnavailable,
        CertUnlockFailure::Unlock(
            UnlockError::HandshakeRejected | UnlockError::NotApplicable | UnlockError::Transport,
        ) => UnlockOutcome::HandshakeRejected,
    }
}

impl Disc {
    /// SCSI handshake — drives the VID-acquisition flow and returns
    /// a structured `HandshakeResult` for downstream key resolution.
    ///
    /// VID acquisition runs through [`Self::do_handshake_cert`], which first
    /// uses the OEM VID a firmware unlocker may have stashed at drive `init()`
    /// (a drive-functionality capability decoupled from the host cert + HRL)
    /// and falls back to the cert-based mutual-auth handshake (dispatched to the
    /// `freemkv-unlock` AACS unlocker) when none is present. The cert path also
    /// yields `read_data_key`, required for AACS 2.0 bus decryption.
    ///
    /// Returns `(handshake, error)`:
    ///   * `(Some(_), None)`  — VID acquired
    ///   * `(None, Some(_))`  — specific failure mode
    ///     (`AacsHostCertRejected` or `AacsVidUnavailable`)
    ///   * `(None, None)`     — handshake not attempted (no keydb;
    ///     resolution will proceed with VID=zero and rely on path 1
    ///     disc-hash → VUK lookup)
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::scan", phase = "do_handshake", "begin");
        // VID comes from the unlocker's OEM path when available (decoupled
        // from the host cert + HRL), else the cert-based handshake — both
        // resolved inside `do_handshake_cert`.
        let (result, err) = Self::do_handshake_cert(session, opts);
        tracing::info!(
            target: "freemkv::scan",
            phase = "do_handshake",
            ok = result.is_some(),
            error_code = err.as_ref().map(|e| e.code()),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end"
        );
        (result, err)
    }

    /// Cert-based AACS handshake — the cert route for VID acquisition.
    ///
    /// Before running the cert mutual-auth, this checks for an OEM Volume ID a
    /// firmware unlocker stashed at drive `init()`. Such an unlocker unlocks
    /// *drive functionality*, not just the disc: VID retrieval via the drive's
    /// OEM CDB is a capability separate from `unlock`. When one served a VID,
    /// we use it and SKIP the cert handshake
    /// entirely — the OEM path gets the VID *without* the host certificate +
    /// HRL, decoupling VID from the cert chain. The OEM path yields no
    /// `read_data_key` (no bus-key is derived); AACS 2.0 content needing
    /// read_data_key for bus decryption must still use the cert path, so an
    /// unlocker with no OEM VID capability returns `None` and we fall through
    /// to cert auth unchanged.
    /// Collect every AACS host cert the caller carries, from BOTH the explicit
    /// [`DriveCredentials`] and the key-source layer
    /// ([`crate::KeySource::host_certs`] across each source), unioned. Host certs
    /// are keysource-served, never compiled in; this is the one place the OEM
    /// cert route gathers them. An empty result is the graceful no-cert signal
    /// (the caller turns it into [`Error::AacsNoHostCert`]).
    /// `mkb` is the disc's MKB generation when known, forwarded to each source's
    /// [`crate::KeySource::host_certs`] so a source MAY return only
    /// generation-appropriate certs (the default ignores it).
    fn collect_host_certs(
        opts: &ScanOptions,
        mkb: Option<u32>,
    ) -> Vec<crate::aacs::types::HostCert> {
        // Delegates to the shared cert primitive (the external freemkv-unlock-aacs
        // plugin uses the same one). Kept as a thin Disc method so the existing
        // collect_host_certs_* unit tests and call sites are unchanged.
        crate::aacs::host_certs::collect_host_certs(opts, mkb)
    }

    fn do_handshake_cert(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        // OEM VID shortcut: a matching unlocker stashed the disc's Volume ID at
        // drive `init()` (the new `unlock()` folds in the old `read_volume_id`).
        // Use it and SKIP the cert handshake — the OEM path
        // decouples the VID from the host cert + HRL. It yields no
        // `read_data_key`; a bus-encrypted disc that needs the bus key is caught
        // by the bus-key gate in `resolve_vid_only`.
        if let Some(volume_id) = session.oem_vid() {
            tracing::debug!(
                target: "freemkv::disc",
                phase = "oem_vid_ok",
                "Volume ID supplied by the drive unlocker at init; skipping the AACS host-certificate handshake."
            );
            return (
                Some(HandshakeResult {
                    volume_id,
                    read_data_key: None,
                    // OEM/VID-only path never attempts the bus-key read — None here
                    // is "not attempted", not "failed".
                    read_data_key_err: None,
                    // The unlocker stashed this VID at init, which means it
                    // matched and unlocked the drive — it now serves clear content,
                    // so bus encryption is removed at the drive. Credit it.
                    drive_unlocked: true,
                }),
                None,
            );
        }
        tracing::debug!(
            target: "freemkv::disc",
            phase = "oem_vid_none",
            "No drive-unlocker Volume ID; running the in-tree AACS host-certificate handshake (AacsCertUnlocker)."
        );

        // Cert path: the in-tree `AacsCertUnlocker` peer absorbs the host-cert
        // mutual-auth. It collects host certs from the key sources + credentials,
        // runs `aacs_authenticate` per cert (wedge-guarded), and on success reads
        // the VID + read_data_key. Its `UnlockError` is folded back to the same
        // `Error` variants this function has always surfaced, so `scan_with`'s
        // error rendering and the path-1 disc-hash → VUK fallback are unchanged.
        let unlocker = AacsCertUnlocker { opts };
        match unlocker.authenticate(session) {
            Ok(hs) => (Some(hs), None),
            Err(e) => {
                tracing::info!(
                    target: "freemkv::disc",
                    phase = "cert_handshake_outcome",
                    outcome = ?cert_unlock_outcome(&e),
                    "AACS cert handshake produced no VID; a key source may still supply this disc's key."
                );
                (None, Some(unlock_error_to_error(&e)))
            }
        }
    }

    /// Build a keys-free AACS state that carries only the Volume ID (+ version
    /// metadata), for callers that resolve Unit Keys out-of-band and have
    /// disabled the local keydb. The VID is on-disc content read during the
    /// handshake; preserving it here lets the out-of-band path use it. No keys
    /// are present (`unit_keys` empty, `vuk` None), so the disc reports as
    /// "encrypted, no keys" until the caller re-scans with a resolved Unit Key.
    pub(super) fn resolve_vid_only(
        udf_fs: &udf::UdfFs,
        reader: &mut dyn SectorSource,
        handshake: Option<&HandshakeResult>,
    ) -> Result<AacsState> {
        use crate::aacs;

        let uk_ro_data =
            aacs::read_first(aacs::UNIT_KEY_RO_PATHS, |p| udf_fs.read_file(reader, p))?;
        let dh = aacs::inf::disc_hash(&uk_ro_data);

        let cc = aacs::read_first(aacs::CONTENT_CERT_PATHS, |p| udf_fs.read_file(reader, p))
            .ok()
            .as_deref()
            .and_then(aacs::inf::parse_content_cert);
        let bus_encryption = cc.as_ref().map(|c| c.bus_encryption).unwrap_or(false);
        // No-cert default = UHD (V20 stride), matching `read_aacs_version` so the
        // scanned `AacsState.version` and the out-of-band fetch agree. A wrong
        // stride on the main resolve path fails loudly (sample validation) rather
        // than silently, so the conservative V20 default is safe here too.
        let version = cc
            .as_ref()
            .map(|c| c.version.major())
            .unwrap_or(aacs::mkb::AACS_MAJOR_UHD);

        // Bus-encryption gate (wrong-keys guard). A bus-encrypted disc (Content
        // Certificate bus-encryption bit set) carries bus encryption on its
        // sectors, which MUST be removed before any AACS key can decrypt them.
        // There are TWO ways it gets removed, and bus encryption is unremovable
        // only when NEITHER succeeded:
        //   1. An unlocker unlocked the drive → it serves CLEAR content
        //      (`drive_unlocked`). This is the common live-drive case and yields
        //      no `read_data_key` — it doesn't need one.
        //   2. The AACS host-certificate cert-auth handshake produced the bus key
        //      (`read_data_key`).
        // The old gate credited ONLY (2), so a SUCCESSFUL drive unlock (VID
        // present, `read_data_key: None`, `drive_unlocked: true`) tripped it and
        // blocked ALL key resolution — including the online source — even though
        // the drive was serving clear content. That was the bug.
        //
        // Also skipped when `handshake = None` (file-backed/ISO scans — bus
        // encryption already removed at read time) and when `bus_encryption` is
        // false (AACS 1.0 BD is not bus-encrypted).
        // ONE question — "is AACS bus encryption gone?" — asked of the single
        // `bus_encryption_removed` predicate, which OWNS every case (never had it,
        // file/ISO, drive unlock, cert bus key). The gate enumerates nothing.
        if !bus_encryption_removed(bus_encryption, handshake) {
            let (rdk_err, has_vid) = handshake
                .map(|h| (h.read_data_key_err, h.volume_id != [0u8; 16]))
                .unwrap_or((None, false));
            tracing::warn!(
                target: "freemkv::disc",
                phase = "bus_key_unavailable",
                read_data_key_err = ?rdk_err,
                has_volume_id = has_vid,
                "Disc declares bus encryption but it could not be removed: no unlocker \
                 unlocked the drive AND the cert handshake produced no read_data_key. Refusing to \
                 emit a key that would decrypt to garbage."
            );
            return Err(Error::AacsBusKeyUnavailable);
        }
        // Read the MKB record stream via the SAME bounded reader the
        // out-of-band `read_aacs_inputs` uses (`read_mkb_content`: a prefix-grow
        // read + trim), NOT a full `read_file`. MKB_RO/RW is allocated to a
        // fixed ~128 MiB of zero padding, and a full `read_file` of it FAILS on
        // file-backed / large readers — which left `a.mkb` empty here, silently
        // breaking online key resolution: `Disc::inputs()` shipped `mkb=0` to
        // the decode service and it 404'd, while autorip's separate
        // `read_aacs_inputs` path (this same helper) worked. One reader now, so
        // `Disc::inputs()` is the single complete source of AACS inputs.
        // A read ERROR is surfaced (logged), not silently emptied: an empty MKB
        // here is invisible until an online key service rejects the request, so
        // a transient I/O hiccup must not masquerade as "no MKB". We still
        // continue with an empty MKB (disc-hash-keyed keydb lookups don't need
        // it), but the cause is now on the log.
        let mkb_bytes = match Self::read_mkb_content(reader, udf_fs) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "scan_aacs_mkb",
                    error = %e,
                    "MKB read failed at scan; AACS inputs will carry an empty MKB \
                     (online key resolution cannot proceed without it). Continuing \
                     — disc-hash-keyed lookups are unaffected."
                );
                Vec::new()
            }
        };
        let mkb_ver = aacs::mkb::mkb_version(&mkb_bytes);

        tracing::debug!(
            target: "freemkv::disc",
            phase = "scan_aacs_vid_only",
            disc_hash = %aacs::inf::disc_hash_hex(&dh),
            version,
            bus_encryption,
            has_vid = handshake.is_some(),
            "Read this disc's AACS data (media-key block and unit-key file). No decryption key computed here — a key source supplies it."
        );

        Ok(AacsState {
            version,
            bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::inf::disc_hash_hex(&dh),
            key_source: KeyOrigin::ExternalUk,
            vuk: None,
            unit_keys: vec![],
            read_data_key: handshake.and_then(|h| h.read_data_key),
            volume_id: handshake.map(|h| h.volume_id).unwrap_or([0u8; 16]),
            uk_ro: uk_ro_data,
            mkb: mkb_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs;
    use crate::sector::SectorSource;
    use std::collections::HashMap;

    // ---------------------------------------------------------------
    // In-memory disc + minimal UDF image with a single physical
    // partition (metadata_start == partition_start). Offsets cited
    // against udf.rs::read_filesystem / ECMA-167.
    // ---------------------------------------------------------------

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
        disc.put(PART_START + 10, build_file_icb(root_fids.len() as u32, 11));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(disc, 10);
        udf::read_filesystem(disc).expect("fs")
    }

    /// A content certificate: type byte@0 (0x00 = V10, else V20),
    /// bus_encryption bit7@1, cc_id@14..20 (aacs/inf.rs parse_content_cert,
    /// which requires ≥20 bytes and reads the bus flag from `data[1] >> 7`).
    fn build_content_cert(cert_type: u8, bus_encryption: bool) -> Vec<u8> {
        let mut v = vec![0u8; 20];
        v[0] = cert_type;
        v[1] = if bus_encryption { 0x80 } else { 0x00 };
        v
    }

    /// An MKB with one Type-and-Version record (type 0x10) carrying the
    /// version as BE u32 at record offset 8, followed by a recorded EOF
    /// record then trailing zero padding. mkb_content_len walks records
    /// and stops at the first padding (type 0) byte (aacs/inf.rs).
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
    // Tests: resolve_vid_only
    // ---------------------------------------------------------------

    /// Missing Unit_Key_RO.inf (and its DUPLICATE) → Error::AacsNoKeys
    /// (encrypt.rs `.map_err(|_| Error::AacsNoKeys)`). Never panics.
    #[test]
    fn resolve_vid_only_missing_unit_key_ro_errors() {
        let mut disc = MemDisc::new();
        // AACS dir exists but has no Unit_Key_RO.inf.
        let udf = build_aacs_fs(&mut disc, &[]);
        let err = Disc::resolve_vid_only(&udf, &mut disc, None)
            .expect_err("missing Unit_Key_RO must error");
        assert!(matches!(err, Error::AacsNoKeys));
    }

    /// A V10 content cert (type 0x00, bus_encryption off) → version 1,
    /// bus_encryption false (encrypt.rs version match: Some(V10) → 1).
    #[test]
    fn resolve_vid_only_v10_cert_sets_version_1() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
        assert_eq!(st.version, 1, "V10 cert → AACS version 1");
        assert!(!st.bus_encryption);
        assert_eq!(st.key_source, KeyOrigin::ExternalUk);
        assert!(st.unit_keys.is_empty(), "vid-only resolves no keys");
        assert!(st.vuk.is_none());
    }

    /// A V20 content cert (type != 0x00) → version 2 (encrypt.rs Some(_) → 2).
    #[test]
    fn resolve_vid_only_v20_cert_sets_version_2() {
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
                    contents: build_content_cert(0x01, true),
                },
            ],
        );
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
        assert_eq!(st.version, 2, "V20 cert → AACS version 2");
        assert!(st.bus_encryption, "cert bus_encryption bit must propagate");
    }

    /// No content cert at all → version defaults to UHD (major 2), matching
    /// `read_aacs_version` so the scanned `AacsState.version` and the out-of-band
    /// fetch agree on the Unit_Key_RO stride (audit #4: a wrong BD-vs-UHD guess
    /// mis-parses unit keys). bus_encryption false (unreadable → off).
    #[test]
    fn resolve_vid_only_no_cert_defaults_version_uhd() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
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
    fn resolve_vid_only_disc_hash_is_sha1_of_unit_key_ro() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
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
    fn resolve_vid_only_trims_mkb_padding() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
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
    fn resolve_vid_only_no_mkb_is_empty() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
        assert!(st.mkb.is_empty());
        assert_eq!(st.mkb_version, None);
    }

    /// A supplied handshake's volume_id and read_data_key propagate onto the
    /// AacsState (encrypt.rs `handshake.map(|h| h.volume_id)` /
    /// `handshake.and_then(|h| h.read_data_key)`).
    #[test]
    fn resolve_vid_only_propagates_handshake_vid_and_rdk() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, Some(&hs)).expect("state");
        assert_eq!(st.volume_id, vid);
        assert_eq!(st.read_data_key, Some(rdk));
    }

    // ---------------------------------------------------------------
    // OEM bus-key gate: a bus-encrypted disc scanned on a LIVE drive
    // (handshake present) with no read_data_key must HARD-ERROR
    // (AacsBusKeyUnavailable) rather than silently yield garbage. The
    // three non-regressing cases must still succeed.
    // ---------------------------------------------------------------

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

    /// Live-drive (handshake Some) + bus_encryption cert + NO read_data_key
    /// → AacsBusKeyUnavailable. This is the wrong-keys guard: a VID-only/OEM
    /// unlock cannot remove bus encryption.
    #[test]
    fn resolve_vid_only_bus_encrypted_live_drive_without_rdk_errors() {
        let (mut disc, udf) = disc_with_cert(0x01, true);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let err = Disc::resolve_vid_only(&udf, &mut disc, Some(&hs))
            .expect_err("bus-encrypted disc with no bus key must hard-error");
        assert!(matches!(err, Error::AacsBusKeyUnavailable));
    }

    /// Live-drive + bus_encryption cert + read_data_key PRESENT → Ok (the cert
    /// handshake produced the bus key, as required).
    #[test]
    fn resolve_vid_only_bus_encrypted_live_drive_with_rdk_ok() {
        let (mut disc, udf) = disc_with_cert(0x01, true);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: Some([0x22u8; 16]),
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st = Disc::resolve_vid_only(&udf, &mut disc, Some(&hs)).expect("bus key present → ok");
        assert!(st.bus_encryption);
        assert_eq!(st.read_data_key, Some([0x22u8; 16]));
    }

    /// ISO scan (handshake None) of a bus_encryption disc → Ok. Bus encryption
    /// was already removed at read time; the gate must NOT fire without a
    /// handshake (no UHD-ISO-mux regression).
    #[test]
    fn resolve_vid_only_bus_encrypted_iso_no_handshake_ok() {
        let (mut disc, udf) = disc_with_cert(0x01, true);
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("ISO bus disc → ok");
        assert!(st.bus_encryption);
        assert_eq!(st.read_data_key, None);
    }

    /// AACS 1.0 BD (V10 cert, bus_encryption off) on a live drive with NO
    /// read_data_key → Ok. read_data_key is legitimately absent for AACS 1.0;
    /// the gate must NOT fire when bus_encryption is false.
    #[test]
    fn resolve_vid_only_aacs10_live_drive_without_rdk_ok() {
        let (mut disc, udf) = disc_with_cert(0x00, false);
        let hs = HandshakeResult {
            volume_id: [0x11u8; 16],
            read_data_key: None,
            read_data_key_err: None,
            drive_unlocked: false,
        };
        let st = Disc::resolve_vid_only(&udf, &mut disc, Some(&hs)).expect("AACS 1.0 → ok");
        assert!(!st.bus_encryption);
        assert_eq!(st.read_data_key, None);
    }

    /// With NO handshake, volume_id defaults to all-zero (encrypt.rs
    /// `.unwrap_or([0u8; 16])`) and read_data_key is None.
    #[test]
    fn resolve_vid_only_no_handshake_zero_vid() {
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
        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("state");
        assert_eq!(st.volume_id, [0u8; 16]);
        assert_eq!(st.read_data_key, None);
    }

    /// Unit_Key_RO.inf is read from /AACS/DUPLICATE when the primary copy
    /// is absent (encrypt.rs `.or_else(|_| read_file(DUPLICATE/...))`).
    /// This is the damaged-primary recovery path real discs rely on.
    #[test]
    fn resolve_vid_only_falls_back_to_duplicate_unit_key_ro() {
        let mut disc = MemDisc::new();
        // Build AACS dir with a DUPLICATE subdir holding Unit_Key_RO.inf.
        let uk = vec![0x55u8; 48];
        let mut dup_fids = Vec::new();
        push_fid(&mut dup_fids, "", 70, true, true);
        push_fid(&mut dup_fids, "Unit_Key_RO.inf", 72, false, false);
        disc.put(PART_START + 72, build_file_icb(uk.len() as u32, 9000));
        disc.put_bytes(PART_START + 9000, &uk);
        disc.put(PART_START + 70, build_file_icb(dup_fids.len() as u32, 71));
        disc.put_bytes(PART_START + 71, &dup_fids);
        // AACS dir: only a DUPLICATE subdir (no primary Unit_Key_RO.inf).
        let mut aacs_fids = Vec::new();
        push_fid(&mut aacs_fids, "", 50, true, true);
        push_fid(&mut aacs_fids, "DUPLICATE", 70, true, false);
        disc.put(PART_START + 50, build_file_icb(aacs_fids.len() as u32, 51));
        disc.put_bytes(PART_START + 51, &aacs_fids);
        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "AACS", 50, true, false);
        disc.put(PART_START + 10, build_file_icb(root_fids.len() as u32, 11));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(&mut disc, 10);
        let udf = udf::read_filesystem(&mut disc).expect("fs");

        let st = Disc::resolve_vid_only(&udf, &mut disc, None).expect("DUPLICATE fallback");
        // disc_hash must be computed over the DUPLICATE bytes.
        assert_eq!(
            st.disc_hash,
            aacs::inf::disc_hash_hex(&aacs::inf::disc_hash(&uk)),
            "fallback must hash the DUPLICATE Unit_Key_RO.inf"
        );
        assert_eq!(st.uk_ro, uk);
    }

    // ---------------------------------------------------------------
    // Tests: read_vid_oem (response parsing). The OEM path issues a
    // READ_BUFFER CDB and parses a 36-byte response; we can't easily
    // fixture a real Drive, but the response-shape contract (3-byte
    // signature 00 22 00, VID at [4..20]) is documented and worth a
    // direct guard via a fake transport. Skipped here because Drive
    // construction requires a live transport; the parsing branches are
    // exercised through `read_vid_oem`'s callers in integration.
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // Tests: collect_host_certs — the OEM cert route's cert-gathering.
    // Unions DriveCredentials with the key-source layer; empty means
    // the route fails gracefully (AacsNoHostCert), never panics.
    // ---------------------------------------------------------------

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
        fn get_uk(
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

    // ---------------------------------------------------------------
    // AacsCertUnlocker outcome mapping: UnlockError → Error (preserving
    // the legacy do_handshake_cert surface) and → UnlockOutcome (the
    // structured trace step). No English in either.
    // ---------------------------------------------------------------

    #[test]
    fn unlock_error_maps_to_legacy_error_variants() {
        use freemkv_unlock::UnlockError;
        // No host cert (libfreemkv-side, carries mkb) keeps the AacsNoHostCert
        // sentinel path — as does the unlocker's own NoUsableHostCert.
        match unlock_error_to_error(&CertUnlockFailure::NoHostCert { mkb: Some(68) }) {
            Error::AacsNoHostCert { path } => assert_eq!(path, "<no host cert>"),
            other => panic!("expected AacsNoHostCert, got {other:?}"),
        }
        match unlock_error_to_error(&CertUnlockFailure::Unlock(UnlockError::NoUsableHostCert)) {
            Error::AacsNoHostCert { path } => assert_eq!(path, "<no host cert>"),
            other => panic!("expected AacsNoHostCert, got {other:?}"),
        }
        assert!(matches!(
            unlock_error_to_error(&CertUnlockFailure::Unlock(UnlockError::VidUnavailable)),
            Error::AacsVidUnavailable
        ));
        assert!(matches!(
            unlock_error_to_error(&CertUnlockFailure::Unlock(UnlockError::HandshakeRejected)),
            Error::AacsHostCertRejected
        ));
        // A transport fault folds to the rejected surface too.
        assert!(matches!(
            unlock_error_to_error(&CertUnlockFailure::Unlock(UnlockError::Transport)),
            Error::AacsHostCertRejected
        ));
    }

    #[test]
    fn cert_unlock_outcome_maps_to_structured_trace_step() {
        use crate::aacs::trace::UnlockOutcome;
        use freemkv_unlock::UnlockError;
        // The libfreemkv-side no-cert case carries the MKB generation.
        assert_eq!(
            cert_unlock_outcome(&CertUnlockFailure::NoHostCert { mkb: Some(77) }),
            UnlockOutcome::NoUsableHostCert { mkb: Some(77) }
        );
        assert_eq!(
            cert_unlock_outcome(&CertUnlockFailure::Unlock(UnlockError::VidUnavailable)),
            UnlockOutcome::VidUnavailable
        );
        assert_eq!(
            cert_unlock_outcome(&CertUnlockFailure::Unlock(UnlockError::HandshakeRejected)),
            UnlockOutcome::HandshakeRejected
        );
        // A transport fault folds to HandshakeRejected at the trace layer.
        assert_eq!(
            cert_unlock_outcome(&CertUnlockFailure::Unlock(UnlockError::Transport)),
            UnlockOutcome::HandshakeRejected
        );
    }
}

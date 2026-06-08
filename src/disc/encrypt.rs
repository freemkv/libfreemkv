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
}

impl Disc {
    /// Acquire the Volume ID. Tries the per-drive OEM CDB path first
    /// when the drive reports `is_unlocked()` (extended-access state),
    /// and falls back to the cert-based AACS mutual-auth handshake
    /// otherwise.
    ///
    /// The OEM path is a single READ_BUFFER CDB built from the drive
    /// profile's `read_vid_cdb` template. The response carries a 3-byte
    /// header (validated against `00 22 00`) followed by the 16-byte
    /// VID at bytes [4..20]. Crucially, no AGID setup is required —
    /// the drive's runtime firmware serves the VID directly when in
    /// extended-access state.
    ///
    /// The cert path is the standard AACS spec flow: ECDH key
    /// agreement, bus-key derivation, then `REPORT_DISC_STRUCTURE`
    /// format 0x80 to retrieve VID under bus-key MAC.
    pub(super) fn read_vid(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> Result<[u8; 16]> {
        if session.is_unlocked() {
            let profile = session
                .drive_profile()
                .ok_or(Error::DriveProfileMissing)?
                .clone();
            return Self::read_vid_oem(session, &profile);
        }
        Self::read_vid_cert(session, opts)
    }

    /// OEM VID retrieval — issues the per-drive READ_BUFFER CDB and
    /// parses the response.
    ///
    /// Response layout (36 bytes):
    ///   * [0..3]   3-byte response signature; expected `00 22 00`
    ///   * [3]      reserved
    ///   * [4..20]  16-byte Volume ID
    ///   * [20..36] reserved / per-drive padding
    fn read_vid_oem(
        session: &mut crate::drive::Drive,
        profile: &crate::profile::DriveProfile,
    ) -> Result<[u8; 16]> {
        const RESPONSE_LEN: usize = 36;
        const EXPECTED_HEADER: [u8; 3] = [0x00, 0x22, 0x00];

        let cdb = profile.read_vid_cdb.ok_or(Error::VidCdbUnavailable)?;
        let mut buf = vec![0u8; RESPONSE_LEN];
        let result = session.scsi_execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        if result.bytes_transferred < RESPONSE_LEN {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "oem_vid_short_response",
                bytes_transferred = result.bytes_transferred,
                "OEM VID CDB returned short response"
            );
            return Err(Error::AacsVidRead);
        }
        if buf[0..3] != EXPECTED_HEADER {
            tracing::warn!(
                target: "freemkv::disc",
                phase = "oem_vid_bad_header",
                header_0 = buf[0],
                header_1 = buf[1],
                header_2 = buf[2],
                "OEM VID response header mismatch"
            );
            return Err(Error::AacsVidRead);
        }
        let mut vid = [0u8; 16];
        vid.copy_from_slice(&buf[4..20]);
        tracing::debug!(
            target: "freemkv::disc",
            phase = "oem_vid_ok",
            "OEM VID retrieved"
        );
        Ok(vid)
    }

    /// Cert-based VID retrieval — runs the full AACS mutual-auth
    /// handshake and extracts VID from the bus-key-MAC'd
    /// `REPORT_DISC_STRUCTURE` response.
    fn read_vid_cert(session: &mut crate::drive::Drive, opts: &ScanOptions) -> Result<[u8; 16]> {
        match Self::do_handshake_cert(session, opts) {
            (Some(h), _) => Ok(h.volume_id),
            (None, Some(e)) => Err(e),
            (None, None) => Err(Error::AacsVidUnavailable),
        }
    }

    /// SCSI handshake — drives the VID-acquisition flow and returns
    /// a structured `HandshakeResult` for downstream key resolution.
    /// Prefers the OEM path when `Drive::is_unlocked()` is true and
    /// falls back to cert-based mutual auth otherwise.
    ///
    /// The OEM path produces only VID (no bus-key, so no
    /// `read_data_key`); the cert path can produce both. AACS 2.0
    /// content that needs read_data_key for bus decryption requires
    /// the cert path.
    ///
    /// Returns `(handshake, error)`:
    ///   * `(Some(_), None)`  — VID acquired
    ///   * `(None, Some(_))`  — specific failure mode; only
    ///     `AacsHostCertRejected` and `AacsVidUnavailable` are returned
    ///     here (the OEM-path `DriveProfileMissing` / `VidCdbUnavailable`
    ///     errors are caught internally and fall through to cert auth)
    ///   * `(None, None)`     — handshake not attempted (no keydb;
    ///     resolution will proceed with VID=zero and rely on path 1
    ///     disc-hash → VUK lookup)
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        let unlocked = session.is_unlocked();
        tracing::debug!(
            target: "freemkv::disc",
            phase = "handshake_entry",
            unlocked,
            "do_handshake entered"
        );

        if unlocked {
            // Try OEM VID retrieval first. If the drive's profile
            // doesn't carry the CDB template, or the response is
            // malformed, fall through to cert-based auth.
            match Self::read_vid(session, opts) {
                Ok(volume_id) => {
                    return (
                        Some(HandshakeResult {
                            volume_id,
                            read_data_key: None,
                        }),
                        None,
                    );
                }
                Err(Error::DriveProfileMissing) | Err(Error::VidCdbUnavailable) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "handshake_oem_unavailable",
                        "OEM VID path unavailable for this drive; trying cert handshake"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "handshake_oem_failed",
                        error_code = e.code(),
                        "OEM VID retrieval failed; trying cert handshake"
                    );
                }
            }
        }

        Self::do_handshake_cert(session, opts)
    }

    /// Cert-based AACS handshake. The legacy auth path; still used as
    /// the fallback when the OEM VID path isn't available or fails.
    fn do_handshake_cert(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        use crate::aacs;

        // Host certs come from the caller's DriveCredentials (e.g. the keydb's
        // host_certs(), sourced app-side) — the library does not load a keydb.
        // Absent ⇒ no cert auth: an unlocked / LibreDrive drive already returned
        // a Volume ID via the OEM path before reaching here, so this is the
        // locked-drive-without-credentials case.
        let host_certs: &[aacs::HostCert] = match &opts.credentials {
            Some(c) if !c.host_certs.is_empty() => &c.host_certs,
            _ => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "handshake_no_credentials",
                    "no drive credentials supplied; cert handshake skipped"
                );
                return (None, None);
            }
        };

        let host_cert_count = host_certs.len();
        tracing::debug!(
            target: "freemkv::disc",
            phase = "handshake_start",
            host_cert_count,
            "handshake starting"
        );

        // Cert-attempt wedge guard. An earlier version fired up to 16
        // AACS authenticate attempts back-to-back with no pause. Each
        // attempt is 5-10 SCSI REPORT_KEY/SEND_KEY exchanges. On a disc
        // whose host cert isn't in the KEYDB (or one the drive rejects),
        // that's 80-160 SCSI commands hammered at the drive in a few
        // hundred milliseconds — and consumer optical drives can respond
        // by entering a fast-fail firmware wedge state where every
        // subsequent CDB returns ILLEGAL_REQUEST/INVALID_FIELD_IN_CDB
        // (sense 05/24) until power-cycled. Observed live on a UHD scan:
        // KEYDB miss → many cert attempts in a tight loop → wedge →
        // forced power cycle to recover.
        //
        // Defense-in-depth: cap attempts, sleep between, and bail
        // early on the drive's wedge sense so any later regression
        // can't undo the protection silently.
        const MAX_CERT_ATTEMPTS: usize = 3;
        const PER_CERT_BACKOFF_MS: u64 = 1000;
        let mut last_err_code: Option<u16> = None;
        for (idx, hc) in host_certs.iter().take(MAX_CERT_ATTEMPTS).enumerate() {
            if idx > 0 {
                std::thread::sleep(std::time::Duration::from_millis(PER_CERT_BACKOFF_MS));
            }
            match aacs::handshake::aacs_authenticate(session, &hc.private_key, &hc.certificate) {
                Ok(mut auth) => {
                    let volume_id = match aacs::handshake::read_volume_id(session, &mut auth) {
                        Ok(vid) => vid,
                        Err(e) => {
                            tracing::warn!(
                                target: "freemkv::disc",
                                phase = "handshake_vid_read_failed",
                                cert_index = idx,
                                error_code = e.code(),
                                "auth ok but volume ID read failed"
                            );
                            return (None, Some(Error::AacsVidUnavailable));
                        }
                    };
                    let read_data_key = aacs::handshake::read_data_keys(session, &mut auth)
                        .ok()
                        .map(|(rdk, _)| rdk);
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "handshake_ok",
                        cert_index = idx,
                        has_read_data_key = read_data_key.is_some(),
                    );
                    return (
                        Some(HandshakeResult {
                            volume_id,
                            read_data_key,
                        }),
                        None,
                    );
                }
                Err(e) => {
                    last_err_code = Some(e.code());
                    // Log the real SCSI sense triple, not `e.code()` —
                    // `code()` collapses every ScsiError to the flat
                    // E_SCSI_ERROR constant and carries no sense key,
                    // so it has no diagnostic value for auth-failure
                    // routing.
                    let sense = e.scsi_sense();
                    // Drive wedge senses (ILLEGAL_REQUEST, sense key
                    // 0x05). The drive isn't merely rejecting our
                    // cert — it's signalling it won't talk to us
                    // anymore. Trying more certs makes the wedge worse,
                    // so bail out immediately. NOTE: this must read the
                    // sense key off the structured ScsiSense, NOT off
                    // `e.code()`; `code()` is a flat constant for every
                    // ScsiError so the old `(code >> 8) & 0xFF` guard
                    // never matched and was dead code (the very wedge
                    // this defense exists to prevent could recur).
                    if sense.map(|s| s.is_illegal_request()).unwrap_or(false) {
                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "handshake_wedge_detected",
                            cert_index = idx,
                            sense_key = sense.map(|s| s.sense_key),
                            asc = sense.map(|s| s.asc),
                            ascq = sense.map(|s| s.ascq),
                            "drive returned ILLEGAL_REQUEST during auth; bailing out to avoid wedge"
                        );
                        return (None, Some(Error::AacsHostCertRejected));
                    }
                    continue;
                }
            }
        }
        tracing::warn!(
            target: "freemkv::disc",
            phase = "handshake_all_certs_failed",
            host_cert_count,
            tried = host_cert_count.min(MAX_CERT_ATTEMPTS),
            last_error_code = last_err_code,
            "all host certs in KEYDB rejected by drive (capped at {} attempts to prevent firmware wedge)",
            MAX_CERT_ATTEMPTS
        );
        (None, Some(Error::AacsHostCertRejected))
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

        let uk_ro_data = udf_fs
            .read_file(reader, "/AACS/Unit_Key_RO.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/DUPLICATE/Unit_Key_RO.inf"))
            .map_err(|_| Error::AacsNoKeys)?;
        let dh = aacs::disc_hash(&uk_ro_data);

        let cc = udf_fs
            .read_file(reader, "/AACS/Content000.cer")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/Content001.cer"))
            .ok()
            .as_deref()
            .and_then(aacs::parse_content_cert);
        let bus_encryption = cc.as_ref().map(|c| c.bus_encryption).unwrap_or(false);
        let version = match cc.as_ref().map(|c| c.version) {
            Some(aacs::AacsVersion::V10) => 1,
            Some(_) => 2,
            None if bus_encryption => 2,
            None => 1,
        };
        // MKB_RO/RW are allocated to a fixed ~128 MiB and zero-padded; trim to
        // the real record length (same as `read_aacs_inputs`). Without this the
        // MKB stashed on `AacsState` — which `Disc::inputs()` and the device/
        // processing-key `decrypt_with` derivation consume, and which a key
        // source ships to an online service — is the full 128 MiB pad, not the
        // ~few-MB record stream.
        let mut mkb_bytes = udf_fs
            .read_file(reader, "/AACS/MKB_RO.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/MKB_RW.inf"))
            .ok()
            .unwrap_or_default();
        // Trim to the real record length. truncate is a no-op when n >=
        // len and correctly empties the vec when n == 0 (zeroed/corrupt
        // MKB), so it never leaves the full ~128 MiB zero-pad on
        // AacsState.mkb.
        let n = aacs::mkb_content_len(&mkb_bytes);
        mkb_bytes.truncate(n);
        let mkb_ver = aacs::mkb_version(&mkb_bytes);

        tracing::debug!(
            target: "freemkv::disc",
            phase = "scan_aacs_vid_only",
            disc_hash = %aacs::disc_hash_hex(&dh),
            version,
            bus_encryption,
            has_vid = handshake.is_some(),
            "keydb disabled — carrying VID only, keys resolved out-of-band"
        );

        Ok(AacsState {
            version,
            bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::disc_hash_hex(&dh),
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

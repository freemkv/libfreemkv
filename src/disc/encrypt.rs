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
        tracing::warn!(
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
    ///   * `(None, Some(_))`  — specific failure mode (see
    ///     `AacsHostCertRejected` / `AacsRawReadUnsupported` /
    ///     `AacsVidUnavailable` / `DriveProfileMissing` /
    ///     `VidCdbUnavailable` variants in `error.rs`)
    ///   * `(None, None)`     — handshake not attempted (no keydb;
    ///     resolution will proceed with VID=zero and rely on path 1
    ///     disc-hash → VUK lookup)
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        let unlocked = session.is_unlocked();
        tracing::warn!(
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
        use crate::aacs::{self, KeyDb};

        let keydb_path = match opts.resolve_keydb() {
            Some(p) => p,
            None => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "handshake_no_keydb",
                    "no KEYDB found in search paths; handshake skipped"
                );
                return (None, None);
            }
        };
        let keydb = match KeyDb::load(&keydb_path) {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "handshake_keydb_load_failed",
                    io_error_kind = ?e.kind(),
                    keydb = %keydb_path.display(),
                    "KEYDB load failed; handshake skipped"
                );
                return (
                    None,
                    Some(Error::KeydbLoad {
                        path: keydb_path.display().to_string(),
                    }),
                );
            }
        };

        let host_cert_count = keydb.host_certs.len();
        tracing::warn!(
            target: "freemkv::disc",
            phase = "handshake_start",
            host_cert_count,
            keydb = %keydb_path.display(),
            "handshake starting"
        );

        if host_cert_count == 0 {
            // No host certs in keydb -> cert auth cannot proceed.
            // Surface as RawReadUnsupported so the caller knows
            // neither path is available on this configuration.
            return (None, Some(Error::AacsRawReadUnsupported));
        }

        // v0.25.7 wedge fix. Pre-0.25.7 this loop fired up to 16 AACS
        // authenticate attempts back-to-back with no pause. Each attempt
        // is 5-10 SCSI REPORT_KEY/SEND_KEY exchanges. On a disc whose
        // host cert isn't in our KEYDB (or one the drive rejects),
        // that's 80-160 SCSI commands hammered at the drive in a
        // few hundred milliseconds — and the BU40N (and most consumer
        // optical drives) responds by entering a fast-fail firmware
        // wedge state where every subsequent CDB returns
        // ILLEGAL_REQUEST/INVALID_FIELD_IN_CDB (sense 05/24) until
        // power-cycled. Hit live on rip1 2026-05-20 during a MOVIE
        // UHD scan: KEYDB miss → 16 cert attempts in a tight loop →
        // wedge → forced host reboot + drive disconnect to recover.
        //
        // Defense-in-depth: cap attempts, sleep between, and bail
        // early on the drive's wedge sense so any later regression
        // can't undo the protection silently.
        const MAX_CERT_ATTEMPTS: usize = 3;
        const PER_CERT_BACKOFF_MS: u64 = 1000;
        let mut last_err_code: Option<u16> = None;
        for (idx, hc) in keydb.host_certs.iter().take(MAX_CERT_ATTEMPTS).enumerate() {
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
                    let code = e.code();
                    last_err_code = Some(code);
                    // Drive wedge senses (any with high byte 0x05 =
                    // ILLEGAL_REQUEST). The drive isn't merely
                    // rejecting our cert — it's saying "I won't talk
                    // to you anymore." Trying more certs makes the
                    // wedge worse. Bail out immediately.
                    let sense_key = ((code >> 8) & 0xFF) as u8;
                    if sense_key == 0x05 {
                        tracing::warn!(
                            target: "freemkv::disc",
                            phase = "handshake_wedge_detected",
                            cert_index = idx,
                            error_code = code,
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

    /// Resolve disc encryption — AACS 1.0, AACS 2.0, CSS, or none.
    ///
    /// Reads AACS files from UDF (via SectorSource), resolves keys through
    /// whatever path works: KEYDB VUK lookup, media key derivation, processing
    /// keys, device keys. Uses handshake result (volume ID, bus key) if available.
    pub(super) fn resolve_encryption(
        udf_fs: &udf::UdfFs,
        reader: &mut dyn SectorSource,
        keydb_path: &std::path::Path,
        handshake: Option<&HandshakeResult>,
    ) -> Result<AacsState> {
        use crate::aacs::{self, KeyDb};
        use crate::drm::{DrmContext, DrmProbe, DrmScheme, ResolvedScheme};

        let keydb = KeyDb::load(keydb_path).map_err(|_| Error::KeydbLoad {
            path: keydb_path.display().to_string(),
        })?;

        // Read AACS files from disc/image via UDF
        let uk_ro_data = udf_fs
            .read_file(reader, "/AACS/Unit_Key_RO.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/DUPLICATE/Unit_Key_RO.inf"))
            .map_err(|_| Error::AacsNoKeys)?;

        // Log the disc hash so we can confirm whether it's present in KEYDB
        // when key resolution fails. The disc hash is SHA-1 of the full
        // Unit_Key_RO.inf file bytes — same value KEYDB.cfg keys VUK entries by.
        let dh = crate::aacs::disc_hash(&uk_ro_data);
        let dh_hex = crate::aacs::disc_hash_hex(&dh);
        tracing::warn!(
            target: "freemkv::disc",
            phase = "scan_aacs_disc_hash",
            disc_hash = %dh_hex,
            uk_ro_len = uk_ro_data.len(),
            "disc hash computed (compare with keydb.cfg entries)"
        );

        let cc_data = udf_fs
            .read_file(reader, "/AACS/Content000.cer")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/Content001.cer"))
            .ok();

        let mkb_data = udf_fs
            .read_file(reader, "/AACS/MKB_RW.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/MKB_RO.inf"))
            .ok();
        let mkb_ver = mkb_data.as_deref().and_then(aacs::mkb_version);

        let mkb_first_64_hex = mkb_data
            .as_deref()
            .map(|m| {
                m.iter()
                    .take(64)
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            })
            .unwrap_or_default();
        tracing::warn!(
            target: "freemkv::disc",
            phase = "scan_aacs_mkb_info",
            mkb_present = mkb_data.is_some(),
            mkb_len = mkb_data.as_deref().map(|m| m.len()).unwrap_or(0),
            mkb_version = ?mkb_ver,
            mkb_first_64 = %mkb_first_64_hex,
            keydb_disc_count = keydb.disc_entries.len(),
            keydb_dk_count = keydb.device_keys.len(),
            keydb_pk_count = keydb.processing_keys.len(),
            "AACS resolution inputs"
        );

        // Use handshake volume ID if available, otherwise zeros
        // (KEYDB VUK lookup by disc hash works without volume ID;
        // paths 2/3/4 in `resolve_keys` short-circuit on the zero
        // sentinel and don't waste cycles trying to derive against
        // garbage input).
        let volume_id = handshake.map(|h| h.volume_id).unwrap_or([0u8; 16]);
        let vid_available = volume_id != [0u8; 16];
        let read_data_key = handshake.and_then(|h| h.read_data_key);

        // Resolve: tries all available paths — KEYDB VUK, media key, processing key, device key.
        //
        // Distinguish "we had every input and still missed" from "we
        // never had VID so the derivation paths couldn't run." The
        // former points at a stale keydb / unsupported MKB; the
        // latter points at a failed handshake upstream. Path 1
        // (disc-hash lookup) ran without VID and missed -> disc isn't
        // in the keydb. If the caller has a handshake-failure reason
        // it overrides this in `scan_with`.
        let miss_error = if vid_available {
            Error::AacsMkUnavailable
        } else {
            Error::AacsVukNotInKeydb
        };

        // Build a probe + context and let the dispatcher pick V10 / V20
        // / V21. CSS is impossible here (this function is only called
        // when /AACS exists), so we don't populate the DVD probe sector
        // or a CSS context.
        let probe = DrmProbe {
            dvd_sample_sector: None,
            content_cert: cc_data.as_deref(),
            mkb: mkb_data.as_deref(),
        };
        let scheme = match DrmScheme::detect(&probe) {
            Some(s) => s,
            None => return Err(miss_error),
        };
        let providers: &[&dyn aacs::KeyProvider] = &[&keydb];
        let aacs_ctx = aacs::ResolveContext {
            unit_key_ro: &uk_ro_data,
            content_cert: cc_data.as_deref(),
            volume_id: &volume_id,
            providers,
            mkb: mkb_data.as_deref(),
        };
        let mut ctx = DrmContext {
            aacs: Some(aacs_ctx),
            css: None,
        };
        let resolved = match scheme.load(&mut ctx) {
            Some(ResolvedScheme::Aacs(r)) => r,
            // Resolution against /AACS inputs can only produce AACS
            // keys. Either the dispatcher returned None (load failed)
            // or — structurally impossible here — a CSS state. Both
            // surface as the upstream miss-error.
            _ => return Err(miss_error),
        };

        Ok(AacsState {
            version: match resolved.version {
                aacs::AacsVersion::V10 => 1,
                aacs::AacsVersion::V20 | aacs::AacsVersion::V21 => 2,
            },
            bus_encryption: resolved.bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::disc_hash_hex(&resolved.disc_hash),
            key_source: match resolved.key_source {
                1 => KeyOrigin::DeviceKey,
                2 => KeyOrigin::ProcessingKey,
                3 => KeyOrigin::KeyDbDerived,
                4 => KeyOrigin::KeyDb,
                5 => KeyOrigin::KeyDbUnitKeys,
                _ => KeyOrigin::KeyDb,
            },
            vuk: resolved.vuk,
            unit_keys: resolved.unit_keys,
            read_data_key,
            volume_id,
            // Stash the AACS inputs so a later out-of-band `Disc::decrypt_with`
            // (caller-resolved Key → derive down) can run without re-reading
            // the disc. The borrows in `aacs_ctx` ended when `scheme.load`
            // returned, so these buffers are free to move here.
            uk_ro: uk_ro_data,
            mkb: mkb_data.unwrap_or_default(),
        })
    }

    /// Resolve encryption from a caller-supplied Unit Key (the external key service
    /// path). No keydb, no derivation: read `Unit_Key_RO.inf` for the disc
    /// hash + version/bus-encryption flags, then use `unit_key` directly as
    /// CPS unit 1's decryption key. The handshake (if any) still supplies the
    /// volume ID and AACS 2.0 read-data key for bus decryption.
    pub(super) fn resolve_encryption_static(
        udf_fs: &udf::UdfFs,
        reader: &mut dyn SectorSource,
        unit_key: [u8; 16],
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

        let mkb_ver = udf_fs
            .read_file(reader, "/AACS/MKB_RW.inf")
            .or_else(|_| udf_fs.read_file(reader, "/AACS/MKB_RO.inf"))
            .ok()
            .as_deref()
            .and_then(aacs::mkb_version);

        tracing::warn!(
            target: "freemkv::disc",
            phase = "scan_aacs_external_uk",
            disc_hash = %aacs::disc_hash_hex(&dh),
            version,
            bus_encryption,
            "using caller-supplied unit key"
        );

        Ok(AacsState {
            version,
            bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::disc_hash_hex(&dh),
            key_source: KeyOrigin::ExternalUk,
            vuk: None,
            unit_keys: vec![(1, unit_key)],
            read_data_key: handshake.and_then(|h| h.read_data_key),
            volume_id: handshake.map(|h| h.volume_id).unwrap_or([0u8; 16]),
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        })
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
        let n = aacs::mkb_content_len(&mkb_bytes);
        if n > 0 && n < mkb_bytes.len() {
            mkb_bytes.truncate(n);
        }
        let mkb_ver = aacs::mkb_version(&mkb_bytes);

        tracing::warn!(
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

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

/// Retrieve VID via the libredrive alternate read path. The drive's
/// runtime firmware has cleared bus encryption AND no longer requires
/// a cert-based AGID for protected-area queries — standard
/// READ_DISC_STRUCTURE format 0x80 with AGID = 0 returns the raw VID.
///
/// Layout matches the spec response (4-byte header + 16-byte VID +
/// 16-byte MAC), but MAC is meaningless without a bus key derivation;
/// libredrive mode delivers `[0u8; 16]` (or stale bytes) in the MAC
/// field. We extract the VID bytes only and skip MAC validation
/// entirely — this is the documented behavior gap when bus encryption
/// is off.
fn read_volume_id_libredrive(session: &mut crate::drive::Drive) -> Result<[u8; 16]> {
    // CDB: READ_DISC_STRUCTURE (0xAD), media=Blu-ray (0x01), AGID=0,
    // format=0x80 (AACS Volume ID), allocation_length=36 (4-byte
    // header + 16-byte VID + 16-byte MAC).
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_READ_DISC_STRUCTURE;
    cdb[1] = 0x01; // Blu-ray media type
    cdb[7] = 0x80; // format = Volume ID
    cdb[8] = 0x00;
    cdb[9] = 36;
    cdb[10] = 0; // AGID = 0 (no auth session)

    let mut buf = [0u8; 36];
    let result = session.scsi_execute(
        &cdb,
        crate::scsi::DataDirection::FromDevice,
        &mut buf,
        5_000,
    )?;
    if result.bytes_transferred < 20 {
        return Err(Error::AacsVidRead);
    }
    let mut vid = [0u8; 16];
    vid.copy_from_slice(&buf[4..20]);
    Ok(vid)
}

impl Disc {
    /// SCSI handshake — retrieve VID (and bus keys when applicable).
    ///
    /// Branches on `Drive::is_libredrive_active()`:
    ///   * libredrive raw-read mode active → skip cert auth, read VID
    ///     directly via the alternate path (bus encryption is already
    ///     off; the drive accepts standard READ_DISC_STRUCTURE format
    ///     0x80 without an AGID).
    ///   * libredrive inactive → traditional AACS mutual auth using
    ///     host certs from the keydb. Caps attempts at 3 with a 1 s
    ///     backoff to avoid the firmware-wedge hammering we hit in
    ///     v0.25.7.
    ///
    /// Returns `(handshake, error)`:
    ///   * `(Some(_), None)`  — VID acquired
    ///   * `(None, Some(_))`  — specific failure mode (see new
    ///     `AacsHostCertRejected` / `AacsLibredriveUnsupported` /
    ///     `AacsVidUnavailable` variants in `error.rs`)
    ///   * `(None, None)`     — handshake not attempted (no keydb;
    ///     resolution will proceed with built-in keys and VID=zero)
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "handshake_entry",
            libredrive_active = session.is_libredrive_active(),
            "do_handshake entered"
        );

        // Libredrive mode: skip cert auth entirely. The drive returns
        // VID via READ_DISC_STRUCTURE format 0x80 with no AGID and no
        // bus encryption applied. This is what MakeMKV does on the
        // same drive + disc combination where libfreemkv used to fail
        // with E7000 — empirically confirmed 2026-05-21 on rip1
        // (BU40N + Barbie UHD, MKB v77, libaacs leaked cert revoked
        // by HRL but disc rips cleanly via libredrive).
        if session.is_libredrive_active() {
            return match read_volume_id_libredrive(session) {
                Ok(vid) => {
                    tracing::debug!(
                        target: "freemkv::disc",
                        phase = "handshake_libredrive_ok",
                        "libredrive VID acquired without cert auth"
                    );
                    (
                        Some(HandshakeResult {
                            volume_id: vid,
                            // No bus key in libredrive mode -> no
                            // encrypted-read-data-key to decrypt.
                            // AACS 2.0 bus-encrypted sectors are
                            // already plaintext when libredrive is
                            // active, so consumers don't need RDK.
                            read_data_key: None,
                        }),
                        None,
                    )
                }
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        phase = "handshake_libredrive_vid_failed",
                        error_code = e.code(),
                        "libredrive VID read failed"
                    );
                    (None, Some(Error::AacsVidUnavailable))
                }
            };
        }

        Self::do_handshake_cert(session, opts)
    }

    /// Cert-based AACS handshake. Only called when libredrive mode is
    /// NOT active — see `do_handshake` for the dispatch.
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
            // Drive isn't in libredrive mode AND keydb has no host
            // certs -> cert auth cannot proceed. Surface as
            // LibredriveUnsupported so the caller knows neither path
            // is available on this configuration.
            return (None, Some(Error::AacsLibredriveUnsupported));
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
        // power-cycled. Hit live on rip1 2026-05-20 during a Barbie
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
        let resolved = aacs::resolve_keys(
            &uk_ro_data,
            cc_data.as_deref(),
            &volume_id,
            &keydb,
            mkb_data.as_deref(),
        )
        .ok_or(miss_error)?;

        Ok(AacsState {
            version: if resolved.aacs2 { 2 } else { 1 },
            bus_encryption: resolved.bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::disc_hash_hex(&resolved.disc_hash),
            key_source: match resolved.key_source {
                1 => KeySource::KeyDb,
                2 => KeySource::KeyDbDerived,
                3 => KeySource::ProcessingKey,
                4 => KeySource::DeviceKey,
                _ => KeySource::KeyDb,
            },
            vuk: resolved.vuk,
            unit_keys: resolved.unit_keys,
            read_data_key,
            volume_id,
        })
    }
}

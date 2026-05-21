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
    /// SCSI handshake — AACS mutual auth via host certs from the keydb,
    /// returning VID (and bus keys when applicable) on success.
    ///
    /// `Drive::is_raw_read_active()` is logged for diagnostics but no
    /// longer alters the auth path. v0.25.11 introduced a "raw-read VID"
    /// shortcut that issued `READ_DISC_STRUCTURE` format 0x80 with
    /// AGID=0 on raw-read-active drives, on the hypothesis that the
    /// firmware-uploaded drive would serve VID without cert auth. The
    /// BU40N returned 0x05/0x6F/0x02 (`KEY NOT ESTABLISHED`) to that
    /// CDB — the AACS spec requires an AGID established via successful
    /// `REPORT_KEY` / `SEND_KEY` before format 0x80 will return VID,
    /// regardless of firmware-upload state. The shortcut was deleted
    /// in v0.25.13. Firmware upload still helps — it removes bus
    /// encryption and (per memory) may allow HRL-burned certs through
    /// the cert handshake — but it doesn't bypass the AGID requirement.
    ///
    /// Returns `(handshake, error)`:
    ///   * `(Some(_), None)`  — VID acquired
    ///   * `(None, Some(_))`  — specific failure mode (see
    ///     `AacsHostCertRejected` / `AacsRawReadUnsupported` /
    ///     `AacsVidUnavailable` variants in `error.rs`)
    ///   * `(None, None)`     — handshake not attempted (no keydb;
    ///     resolution will proceed with VID=zero and rely on path 1
    ///     disc-hash → VUK lookup)
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        tracing::warn!(
            target: "freemkv::disc",
            phase = "handshake_entry",
            raw_read_active = session.is_raw_read_active(),
            "do_handshake entered"
        );

        Self::do_handshake_cert(session, opts)
    }

    /// Cert-based AACS handshake. The only auth path post-v0.25.13;
    /// `do_handshake` is now a thin diagnostic wrapper.
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
        let aacs_ctx = aacs::ResolveContext {
            unit_key_ro: &uk_ro_data,
            content_cert: cc_data.as_deref(),
            volume_id: &volume_id,
            keydb: &keydb,
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

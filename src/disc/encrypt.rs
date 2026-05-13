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
    /// SCSI handshake result — volume ID and bus keys from ECDH authentication.
    /// Only available when scanning from a real drive (not ISO images).
    pub(super) fn do_handshake(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> Option<HandshakeResult> {
        use crate::aacs::{self, KeyDb};

        tracing::warn!(
            target: "freemkv::disc",
            phase = "handshake_entry",
            "do_handshake entered"
        );
        let keydb_path = match opts.resolve_keydb() {
            Some(p) => p,
            None => {
                tracing::warn!(
                    target: "freemkv::disc",
                    phase = "handshake_no_keydb",
                    "no KEYDB found in search paths; handshake skipped"
                );
                return None;
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
                return None;
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

        const MAX_CERT_ATTEMPTS: usize = 16;
        let mut last_err_code: Option<u16> = None;
        for (idx, hc) in keydb.host_certs.iter().take(MAX_CERT_ATTEMPTS).enumerate() {
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
                            return None;
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
                    return Some(HandshakeResult {
                        volume_id,
                        read_data_key,
                    });
                }
                Err(e) => {
                    last_err_code = Some(e.code());
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
            "all host certs in KEYDB rejected by drive"
        );
        // All host certs failed — return None, not a fake success
        None
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
        // (KEYDB VUK lookup by disc hash works without volume ID)
        let volume_id = handshake.map(|h| h.volume_id).unwrap_or([0u8; 16]);
        let read_data_key = handshake.and_then(|h| h.read_data_key);

        // Resolve: tries all available paths — KEYDB VUK, media key, processing key, device key
        let resolved = aacs::resolve_keys(
            &uk_ro_data,
            cc_data.as_deref(),
            &volume_id,
            &keydb,
            mkb_data.as_deref(),
        )
        .ok_or(Error::AacsNoKeys)?;

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

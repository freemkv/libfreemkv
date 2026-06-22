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
    /// SCSI handshake — drives the VID-acquisition flow and returns
    /// a structured `HandshakeResult` for downstream key resolution.
    ///
    /// Drive unlock now lives behind the pluggable
    /// [`crate::unlock::Unlocker`] seam, which reports no extended-access
    /// marker back to libfreemkv. VID is therefore always acquired via the
    /// cert-based mutual-auth handshake (the OEM route); the cert path also
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
        // Drive unlock moved behind the pluggable `Unlocker` seam, which
        // reports no extended-access marker — so VID always comes via the
        // cert-based handshake (the OEM route).
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

    /// Cert-based AACS handshake — the OEM route for VID acquisition.
    fn do_handshake_cert(
        session: &mut crate::drive::Drive,
        opts: &ScanOptions,
    ) -> (Option<HandshakeResult>, Option<Error>) {
        use crate::aacs;

        // Host certs come from the caller's DriveCredentials (e.g. the keydb's
        // host_certs(), sourced app-side) — the library does not load a keydb.
        // Absent ⇒ no cert auth: resolution proceeds with VID=zero and relies
        // on the path-1 disc-hash → VUK lookup.
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
    /// bus_encryption bit0@1, cc_id@2..8 (aacs/keys.rs parse_content_cert).
    fn build_content_cert(cert_type: u8, bus_encryption: bool) -> Vec<u8> {
        let mut v = vec![0u8; 8];
        v[0] = cert_type;
        v[1] = if bus_encryption { 0x01 } else { 0x00 };
        v
    }

    /// An MKB with one Type-and-Version record (type 0x10) carrying the
    /// version as BE u32 at record offset 8, followed by a recorded EOF
    /// record then trailing zero padding. mkb_content_len walks records
    /// and stops at the first padding (type 0) byte (aacs/keys.rs).
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

    /// No content cert at all but bus_encryption can't be read → version
    /// defaults to 1 (encrypt.rs: `None => 1`). bus_encryption false.
    #[test]
    fn resolve_vid_only_no_cert_defaults_version_1() {
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
        assert_eq!(st.version, 1, "no cert → default version 1");
        assert!(!st.bus_encryption);
    }

    /// disc_hash is SHA1 of the Unit_Key_RO.inf bytes, hex with 0x prefix
    /// and uppercase (aacs::disc_hash + disc_hash_hex). The state's
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
        let expected = aacs::disc_hash_hex(&aacs::disc_hash(&uk));
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
            aacs::mkb_content_len(&mkb),
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
        };
        let st = Disc::resolve_vid_only(&udf, &mut disc, Some(&hs)).expect("state");
        assert_eq!(st.volume_id, vid);
        assert_eq!(st.read_data_key, Some(rdk));
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
            aacs::disc_hash_hex(&aacs::disc_hash(&uk)),
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
}

//! MT1959 platform — shared logic for both variants.

mod variant_a;
mod variant_b;

use super::PlatformDriver;
use crate::error::{Error, Result};
use crate::profile::DriveProfile;
use crate::scsi::{self, DataDirection, ScsiTransport};

// ── Variant constants ──────────────────────────────────────────────────
// Every vendor command: 3C [mode] [buffer_id] [sub_cmd] [addr] ...
const MODE_A: u8 = 0x01;
const MODE_B: u8 = 0x02;
const BUFFER_ID_A: u8 = 0x44;
const BUFFER_ID_B: u8 = 0x77;

// ── SCSI opcodes ──────────────────────────────────────────────────────
const SCSI_READ_BUFFER: u8 = 0x3C;
const SCSI_READ_CAPACITY: u8 = 0x25;
/// Shared by both firmware-upload variants (see `variant_a` / `variant_b`).
pub(super) const SCSI_WRITE_BUFFER: u8 = 0x3B;

// ── Sub-commands (shared A/B) ─────────────────────────────────────────
const SUB_CMD_UNLOCK: u8 = 0x00;
const SUB_CMD_INIT: u8 = 0x12;
const SUB_CMD_PROBE: u8 = 0x14;
const UNLOCK_RESPONSE_SIZE: u8 = 64;
const VALIDATE_RESPONSE_SIZE: u8 = 4;
/// Primary mode marker at bytes [12..16] of the unlock response — set
/// by the platform firmware when the runtime image is loaded and the
/// extended-access surface is live.
const FIRMWARE_ACTIVE_OFFSET: usize = 12;
const FIRMWARE_ACTIVE_SIG: [u8; 4] = [0x4D, 0x4D, 0x6B, 0x76];
/// Secondary mode marker repeated through bytes [16..64] of the unlock
/// response. Confirms the runtime firmware is the one driving the
/// response, not a stale image's residual buffer.
const FIRMWARE_MODE_OFFSET: usize = 16;
const FIRMWARE_MODE_SIG: [u8; 4] = [0x4C, 0x62, 0x44, 0x72];

// ── Init address (per disc type) ──────────────────────────────────────
const INIT_ADDR_BD: u16 = 0x0100;
const INIT_ADDR_UHD: u16 = 0x0200;

// ── Probe scan ranges ─────────────────────────────────────────────────
const PROBE_COARSE_END: u16 = 0x5800;
const PROBE_FINE_END: u32 = 0x10000;
const PROBE_STEP: u16 = 0x0100;
const PROBE_RESPONSE_SIZE: u8 = 4;

// ── Disc type threshold ───────────────────────────────────────────────
const UHD_SECTOR_THRESHOLD: u32 = 25_000_000; // ~50 GB
const READ_CAPACITY_RESPONSE_SIZE: usize = 8;

pub struct Mt1959 {
    pub(crate) profile: DriveProfile,
    pub(crate) mode: u8,
    pub(crate) buffer_id: u8,
    /// True after `run_init` has completed the unlock handshake (and any
    /// required firmware upload). Gates probe + downstream control
    /// commands; says nothing about whether the drive is in
    /// extended-access mode.
    pub(crate) init_complete: bool,
    /// True when the unlock response carried both the per-drive
    /// signature AND the primary mode marker at offset 12 AND the
    /// secondary mode marker at offset 16. When true the drive is in
    /// the extended-access state — host can issue the per-drive
    /// OEM CDBs and read sectors without the cert-based AACS bus
    /// encryption / mutual-auth gate.
    unlocked: bool,
    probed: bool,
}

impl Mt1959 {
    pub fn new(profile: DriveProfile, is_variant_b: bool) -> Self {
        let (mode, buffer_id) = if is_variant_b {
            (MODE_B, BUFFER_ID_B)
        } else {
            (MODE_A, BUFFER_ID_A)
        };
        Mt1959 {
            profile,
            mode,
            buffer_id,
            init_complete: false,
            unlocked: false,
            probed: false,
        }
    }

    // ── SCSI helpers (shared by both variants) ─────────────────────────

    pub(crate) fn read_buffer_sub(&self, sub_cmd: u8, address: u16, length: u8) -> [u8; 10] {
        [
            SCSI_READ_BUFFER,
            self.mode,
            self.buffer_id,
            sub_cmd,
            (address >> 8) as u8,
            address as u8,
            0x00,
            0x00,
            length,
            0x00,
        ]
    }

    pub(crate) fn read_buffer_probe(
        &self,
        scsi: &mut dyn ScsiTransport,
        sub_cmd: u8,
        address: u16,
        buf: &mut [u8],
        expected: usize,
    ) -> Result<usize> {
        // The READ_BUFFER CDB transfer-length is a single byte; an
        // `expected` above 255 cannot be expressed and would silently
        // truncate. All in-crate callers pass small fixed sizes (4); guard
        // the invariant rather than emit a malformed CDB.
        debug_assert!(
            expected <= u8::MAX as usize,
            "read_buffer_probe expected exceeds 1-byte CDB length field"
        );
        let cdb = self.read_buffer_sub(sub_cmd, address, expected as u8);
        let result = scsi.execute(&cdb, DataDirection::FromDevice, buf, 5_000)?;
        if result.bytes_transferred != expected {
            return Err(Error::ScsiError {
                opcode: SCSI_READ_BUFFER,
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            });
        }
        Ok(result.bytes_transferred)
    }

    pub(crate) fn set_cd_speed_max(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let cdb = scsi::build_set_cd_speed(0xFFFF);
        let mut dummy = [0u8; 0];
        scsi.execute(&cdb, DataDirection::None, &mut dummy, 5_000)?;
        Ok(())
    }

    // ── Unlock (shared) ────────────────────────────────────────────────

    pub(crate) fn do_unlock(&mut self, scsi: &mut dyn ScsiTransport) -> Result<Vec<u8>> {
        let cdb = [
            0x3C,
            self.mode,
            self.buffer_id,
            SUB_CMD_UNLOCK,
            0x00,
            0x00,
            0x00,
            0x00,
            UNLOCK_RESPONSE_SIZE,
            0x00,
        ];
        let mut response = vec![0u8; UNLOCK_RESPONSE_SIZE as usize];
        let result = scsi.execute(&cdb, DataDirection::FromDevice, &mut response, 30_000)?;

        // `response` is a fixed 64-byte buffer, so `response.len()` is
        // always >= every offset below — the meaningful bound is how many
        // bytes the drive actually delivered. Validate against
        // `bytes_transferred` so a short/partial transfer (stale trailing
        // zeros) can't be read as if the drive sent real marker bytes.
        let n = result.bytes_transferred.min(response.len());

        if n >= 4 && response[0..4] != self.profile.signature {
            return Err(Error::SignatureMismatch {
                expected: self.profile.signature,
                got: response[0..4].try_into().unwrap_or([0; 4]),
            });
        }

        if n >= FIRMWARE_ACTIVE_OFFSET + 4
            && response[FIRMWARE_ACTIVE_OFFSET..FIRMWARE_ACTIVE_OFFSET + 4] != FIRMWARE_ACTIVE_SIG
        {
            return Err(Error::UnlockFailed);
        }

        // Extended-access state is active when BOTH the per-drive
        // signature matched AND the response carries the secondary
        // marker at offset 16 (repeated through bytes 16..64) AND the
        // primary mode marker at [12..16] is present. The active-mode
        // marker at [12..16] is the primary gate; the [16..20] marker
        // is the redundant confirmation the firmware writes through
        // the rest of the response. Requiring both before we tell the
        // upper layer "OEM path is live" keeps any partial / corrupted
        // response from steering us off the cert-auth fallback.
        self.unlocked = n >= FIRMWARE_MODE_OFFSET + 4
            && response[FIRMWARE_ACTIVE_OFFSET..FIRMWARE_ACTIVE_OFFSET + 4] == FIRMWARE_ACTIVE_SIG
            && response[FIRMWARE_MODE_OFFSET..FIRMWARE_MODE_OFFSET + 4] == FIRMWARE_MODE_SIG;

        self.init_complete = true;
        Ok(response)
    }

    fn validate(&self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        for _attempt in 0..5 {
            let cdb = [
                0x3C,
                self.mode,
                self.buffer_id,
                SUB_CMD_UNLOCK,
                0x00,
                0x00,
                0x00,
                0x00,
                VALIDATE_RESPONSE_SIZE,
                0x00,
            ];
            let mut resp = [0u8; 4];
            if scsi
                .execute(&cdb, DataDirection::FromDevice, &mut resp, 5_000)
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(Error::ScsiError {
            opcode: SCSI_READ_BUFFER,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        })
    }

    // ── Init (unlock + firmware) ───────────────────────────────────────

    fn run_init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        let mut succeeded = false;
        for _attempt in 0..3 {
            match self.do_unlock(scsi) {
                Ok(_) => {
                    succeeded = true;
                    break;
                }
                Err(Error::SignatureMismatch { .. }) => {
                    return Err(Error::UnlockFailed);
                }
                Err(_) => {
                    let loaded = if self.mode == MODE_A {
                        variant_a::load_firmware(self, scsi).is_ok()
                    } else {
                        variant_b::load_firmware(self, scsi).is_ok()
                    };
                    if !loaded {
                        continue;
                    }
                    // Firmware upload resets the drive. Give it time to
                    // fully recover before retrying unlock.
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
            }
        }
        if !succeeded {
            return Err(Error::UnlockFailed);
        }
        Ok(())
    }

    // ── Probe disc ─────────────────────────────────────────────────────

    /// Probe the disc surface so the drive firmware learns optimal speeds
    /// per region. Two passes, then SET_CD_SPEED(max). After this the
    /// drive manages per-zone speeds internally.
    fn run_probe(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.init_complete {
            self.do_unlock(scsi)?;
        }

        // Detect disc type from capacity to select probe mode.
        // BD:  3C 01 44 12 01 00 00 00 04 00  (init_addr = 0x0100)
        // UHD: 3C 01 44 12 02 00 00 00 04 00  (init_addr = 0x0200)
        // Empirically verified via SCSI capture: BD and UHD use different init addresses.
        let cap_cdb = [
            SCSI_READ_CAPACITY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut cap_buf = [0u8; READ_CAPACITY_RESPONSE_SIZE];
        let disc_sectors = if scsi
            .execute(&cap_cdb, DataDirection::FromDevice, &mut cap_buf, 5_000)
            .is_ok()
        {
            // last_lba + 1 = sector count. A 0xFFFFFFFF last-LBA is the
            // READ CAPACITY(10) "capacity exceeds 32 bits" sentinel; saturate
            // rather than wrap to 0 (which would misclassify a huge disc as
            // BD). A saturated count stays above the UHD threshold -> UHD.
            u32::from_be_bytes([cap_buf[0], cap_buf[1], cap_buf[2], cap_buf[3]]).saturating_add(1)
        } else {
            0
        };
        let init_addr = if disc_sectors > UHD_SECTOR_THRESHOLD {
            INIT_ADDR_UHD
        } else {
            INIT_ADDR_BD
        };
        let mut init_resp = [0u8; PROBE_RESPONSE_SIZE as usize];
        let _ = self.read_buffer_probe(
            scsi,
            SUB_CMD_INIT,
            init_addr,
            &mut init_resp,
            PROBE_RESPONSE_SIZE as usize,
        );

        self.validate(scsi)?;

        // Pass 1: coarse scan
        let mut addr: u16 = 0;
        while addr < PROBE_COARSE_END {
            let mut resp = [0u8; PROBE_RESPONSE_SIZE as usize];
            if self
                .read_buffer_probe(
                    scsi,
                    SUB_CMD_PROBE,
                    addr,
                    &mut resp,
                    PROBE_RESPONSE_SIZE as usize,
                )
                .is_err()
            {
                return Err(Error::ScsiError {
                    opcode: SCSI_READ_BUFFER,
                    status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                    sense: None,
                });
            }
            addr = addr.wrapping_add(PROBE_STEP);
        }

        // Pass 2: fine scan
        let mut addr: u32 = 0;
        while addr < PROBE_FINE_END {
            let mut resp = [0u8; PROBE_RESPONSE_SIZE as usize];
            if self
                .read_buffer_probe(
                    scsi,
                    SUB_CMD_PROBE,
                    addr as u16,
                    &mut resp,
                    PROBE_RESPONSE_SIZE as usize,
                )
                .is_err()
            {
                break;
            }
            addr += PROBE_STEP as u32;
        }

        // Set max speed — drive manages zones from here
        let _ = self.set_cd_speed_max(scsi);

        self.probed = true;
        Ok(())
    }
}

// ── PlatformDriver trait ───────────────────────────────────────────────

impl PlatformDriver for Mt1959 {
    fn init(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if self.init_complete {
            return Ok(());
        }
        self.run_init(scsi)
    }

    fn probe_disc(&mut self, scsi: &mut dyn ScsiTransport) -> Result<()> {
        if !self.init_complete {
            // Don't retry init here — if init() failed, probing can't work either.
            // Retrying causes repeated USB bus resets on BU40N.
            return Ok(());
        }
        if self.probed {
            return Ok(());
        }
        self.run_probe(scsi)
    }

    fn is_ready(&self) -> bool {
        self.init_complete
    }

    fn is_unlocked(&self) -> bool {
        self.unlocked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{DriveProfile, Identity};
    use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};

    /// Minimal mock transport that returns a scripted response to the
    /// next `execute()` call. Only used for verifying that `do_unlock`
    /// classifies the response correctly — no general SCSI coverage.
    struct ScriptedTransport {
        response: Vec<u8>,
    }

    impl ScsiTransport for ScriptedTransport {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            let n = self.response.len().min(data.len());
            data[..n].copy_from_slice(&self.response[..n]);
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            })
        }
    }

    fn fixture_profile(signature: [u8; 4]) -> DriveProfile {
        DriveProfile {
            identity: Identity {
                vendor_id: "TEST".into(),
                product_revision: String::new(),
                vendor_specific: String::new(),
                firmware_date: String::new(),
            },
            signature,
            firmware: Vec::new(),
            unlock_init_value: 0,
            unlock_response_size: 0,
            read_vid_cdb: None,
            read_disc_keys_cdb: None,
            drive_nominal_speed_cdb: None,
            set_speed_max_cdb: None,
            read10_raw_2sec_cdb: None,
            read10_raw_1sec_cdb: None,
            read_buffer_verify_cdb: None,
            write_buffer_cdb: None,
            read_buffer_unlock_cdb: None,
            speed_zone_table: None,
            speed_calc_table: None,
        }
    }

    /// Build a synthetic 64-byte unlock response.
    ///
    /// `mode_marker`: bytes [12..16]. Pass `FIRMWARE_ACTIVE_SIG` for the
    /// active-mode primary marker.
    /// `id_marker`:   bytes [16..20] (and repeated through [20..64] in
    /// real responses; only [16..20] is checked).
    fn build_response(signature: [u8; 4], mode_marker: [u8; 4], id_marker: [u8; 4]) -> Vec<u8> {
        let mut r = vec![0u8; 64];
        r[0..4].copy_from_slice(&signature);
        // bytes [4..12] left as zeros (version + reserved per format)
        r[12..16].copy_from_slice(&mode_marker);
        // Real firmware repeats the secondary marker through [16..64];
        // the parser only checks [16..20], so we just write the marker
        // once.
        r[16..20].copy_from_slice(&id_marker);
        r
    }

    #[test]
    fn do_unlock_sets_unlocked_when_both_markers_present() {
        let sig = [0x99, 0x9E, 0xC3, 0x75];
        let response = build_response(sig, FIRMWARE_ACTIVE_SIG, FIRMWARE_MODE_SIG);
        let mut transport = ScriptedTransport { response };
        let mut mt = Mt1959::new(fixture_profile(sig), false);

        let raw = mt.do_unlock(&mut transport).expect("unlock should succeed");
        assert_eq!(raw.len(), 64);
        assert!(mt.init_complete, "init_complete set after success");
        assert!(
            mt.is_unlocked(),
            "both markers present -> extended-access state"
        );
    }

    #[test]
    fn do_unlock_init_complete_but_not_unlocked_when_id_marker_missing() {
        // Primary mode marker present (so init passes) but the
        // secondary marker is replaced with zeros — drive isn't in
        // extended-access state.
        let sig = [0x99, 0x9E, 0xC3, 0x75];
        let response = build_response(sig, FIRMWARE_ACTIVE_SIG, [0u8; 4]);
        let mut transport = ScriptedTransport { response };
        let mut mt = Mt1959::new(fixture_profile(sig), false);

        mt.do_unlock(&mut transport).expect("unlock should succeed");
        assert!(mt.init_complete);
        assert!(
            !mt.is_unlocked(),
            "missing secondary marker -> not in extended-access state"
        );
    }

    #[test]
    fn do_unlock_rejects_signature_mismatch() {
        let response = build_response(
            [0xAA, 0xBB, 0xCC, 0xDD],
            FIRMWARE_ACTIVE_SIG,
            FIRMWARE_MODE_SIG,
        );
        let mut transport = ScriptedTransport { response };
        let mut mt = Mt1959::new(fixture_profile([0x99, 0x9E, 0xC3, 0x75]), false);

        let err = mt.do_unlock(&mut transport).unwrap_err();
        assert!(matches!(err, Error::SignatureMismatch { .. }));
        assert!(!mt.init_complete);
        assert!(!mt.is_unlocked());
    }

    #[test]
    fn do_unlock_rejects_inactive_mode_marker() {
        // Signature matches but the primary marker at [12..16] is
        // missing -> drive is not in active mode; init_complete and the
        // unlocked flag must both stay false.
        let sig = [0x99, 0x9E, 0xC3, 0x75];
        let response = build_response(sig, [0u8; 4], FIRMWARE_MODE_SIG);
        let mut transport = ScriptedTransport { response };
        let mut mt = Mt1959::new(fixture_profile(sig), false);

        let err = mt.do_unlock(&mut transport).unwrap_err();
        assert!(matches!(err, Error::UnlockFailed));
        assert!(!mt.init_complete);
        assert!(!mt.is_unlocked());
    }
}

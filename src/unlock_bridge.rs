//! Bridges libfreemkv's drive layer to the `freemkv-unlock` crate: one generic
//! SCSI-transport adapter, identity/host-cert mapping, and the dispatch that
//! news up `all_unlockers()` and runs the first matching one. libfreemkv names
//! no individual unlocker — it only calls this bridge.

use freemkv_unlock as fu;

/// Map libfreemkv's drive identity to the unlock contract's `DriveId`.
fn to_fu_drive_id(drive_id: &crate::identity::DriveId) -> fu::DriveId {
    fu::DriveId {
        vendor_id: drive_id.vendor_id.clone(),
        product_revision: drive_id.product_revision.clone(),
        vendor_specific: drive_id.vendor_specific.clone(),
        firmware_date: drive_id.firmware_date.clone(),
    }
}

/// Name of the unlocker that claims this drive by identity (drive-info "is this
/// drive supported?" display), or `None`. A pure lookup — does NOT touch the
/// drive or unlock anything.
pub(crate) fn unlocker_name(drive_id: &crate::identity::DriveId) -> Option<&'static str> {
    fu::unlocker_name(&to_fu_drive_id(drive_id))
}

/// Adapt libfreemkv's `ScsiTransport` to the unlock crate's transport contract.
struct ScsiAdapter<'a>(&'a mut dyn crate::scsi::ScsiTransport);

impl fu::scsi::ScsiTransport for ScsiAdapter<'_> {
    fn execute(
        &mut self,
        cdb: &[u8],
        dir: fu::scsi::DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> fu::scsi::Result<fu::scsi::ScsiResult> {
        let d = match dir {
            fu::scsi::DataDirection::None => crate::scsi::DataDirection::None,
            fu::scsi::DataDirection::FromDevice => crate::scsi::DataDirection::FromDevice,
            fu::scsi::DataDirection::ToDevice => crate::scsi::DataDirection::ToDevice,
        };
        match self.0.execute(cdb, d, data, timeout_ms) {
            Ok(r) => Ok(fu::scsi::ScsiResult {
                status: r.status,
                bytes_transferred: r.bytes_transferred,
                sense: r.sense,
            }),
            // libfreemkv's transport returns Err for ANY non-zero SCSI status —
            // i.e. a normal drive CHECK CONDITION (ILLEGAL_REQUEST, etc.), NOT
            // only a transport-layer fault. Preserve the real status AND the
            // parsed sense across the seam: the AACS handshake's wedge guard
            // bails on an ILLEGAL_REQUEST sense (so it stops hammering the drive),
            // and its diagnosis distinguishes a cert rejection from a dead bus by
            // the same status/sense. Collapsing everything to 0xFF/None defeated
            // both. Reconstruct the 32-byte sense buffer at the offsets the
            // unlock crate reads (sense_key@2 low-nibble, asc@12, ascq@13); a
            // genuine transport fault (status 0xFF, no sense) maps through
            // unchanged.
            Err(e) => {
                // A SCSI status (CHECK CONDITION or a 0xFF transport fault the
                // drive layer already tagged) carries its real status + sense via
                // extract_scsi_context. Any OTHER error variant is a non-SCSI
                // transport/IO-layer fault — ioctl(SG_IO) == -1 (ENODEV/EIO on an
                // unplugged bridge) or the fd is gone — i.e. a DEAD BUS, not a
                // drive rejection; surface the transport-failure status so the
                // unlock crate bails instead of hammering a wedged device.
                // (Keying off `sense.is_none()` would be wrong: a CHECK CONDITION
                // whose sense didn't parse is a rejection, not a transport fault.)
                let (status, sense) = match &e {
                    crate::error::Error::ScsiError { .. }
                    | crate::error::Error::DiscRead { .. } => {
                        crate::drive::extract_scsi_context(&e)
                    }
                    _ => (crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE, None),
                };
                let sense_buf = sense.map(|s| {
                    let mut b = [0u8; 32];
                    b[2] = s.sense_key & 0x0F;
                    b[12] = s.asc;
                    b[13] = s.ascq;
                    b
                });
                Err(fu::scsi::ScsiError {
                    status,
                    sense: sense_buf,
                })
            }
        }
    }
}

/// Map libfreemkv's host certs (keysource-collected) to the unlock contract's.
pub(crate) fn map_host_certs(certs: &[crate::aacs::HostCert]) -> Vec<fu::HostCert> {
    certs
        .iter()
        .map(|c| fu::HostCert {
            private_key: c.private_key,
            certificate: c.certificate.clone(),
            private_key_v2: c.private_key_v2,
            certificate_v2: c.certificate_v2.clone(),
        })
        .collect()
}

/// News up the unlockers, build the context for `kind`, and run the FIRST
/// matching one — returning its `Result` so the caller can both consume what it
/// learned (vid / bus_key / drive_unlocked) AND render the specific failure
/// (the AACS cert path maps the `UnlockError` to its outcome trace). `Err(
/// NotApplicable)` when nothing matched. `host_certs` are collected by the
/// caller — lazily, only for AACS; pass `&[]` for the drive-prep / CSS kinds.
pub(crate) fn run_unlockers(
    scsi: &mut dyn crate::scsi::ScsiTransport,
    drive_id: &crate::identity::DriveId,
    kind: fu::DiscKind,
    host_certs: &[fu::HostCert],
) -> std::result::Result<fu::Unlocked, fu::UnlockError> {
    let id = to_fu_drive_id(drive_id);
    let ctx = fu::UnlockCtx::new(&id, kind, host_certs);
    let mut adapter = ScsiAdapter(scsi);
    for u in fu::all_unlockers() {
        if u.matches(&ctx) {
            return u.unlock(&mut adapter, &ctx);
        }
    }
    Err(fu::UnlockError::NotApplicable)
}

/// The names of every REGISTERED unlocker, in dispatch order. Registry-driven —
/// sourced from `all_unlockers()`, so adding/removing an unlocker updates every
/// report with no other change (no hardcoded names). The per-unlocker "did it
/// run this rip" outcome is computed by the caller, which has the disc + drive
/// runtime state this crate cannot see.
pub(crate) fn unlocker_names() -> Vec<&'static str> {
    fu::all_unlockers().iter().map(|u| u.name()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use freemkv_unlock::scsi::ScsiTransport as _; // brings `execute` into scope

    /// A fake libfreemkv transport whose `execute` always fails with a
    /// freshly-built error (`crate::error::Error` isn't `Clone` — `io::Error`).
    struct ErrTransport<F>(F);
    impl<F: FnMut() -> crate::error::Error + Send> crate::scsi::ScsiTransport for ErrTransport<F> {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: crate::scsi::DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> std::result::Result<crate::scsi::ScsiResult, crate::error::Error> {
            Err((self.0)())
        }
    }

    fn adapt(e: impl FnMut() -> crate::error::Error + Send) -> fu::scsi::ScsiError {
        let mut t = ErrTransport(e);
        let mut adapter = ScsiAdapter(&mut t);
        let mut buf = [0u8; 0];
        adapter
            .execute(&[0u8; 12], fu::scsi::DataDirection::None, &mut buf, 1_000)
            .expect_err("error path")
    }

    /// A CHECK CONDITION carrying sense crosses the seam with status + parsed
    /// sense intact, so the unlock crate's ILLEGAL_REQUEST wedge guard can fire.
    #[test]
    fn check_condition_preserves_status_and_sense() {
        let err = adapt(|| crate::error::Error::ScsiError {
            opcode: 0xA3,
            status: 0x02,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: 0x05,
                asc: 0x24,
                ascq: 0x00,
            }),
        });
        assert_eq!(err.status, 0x02);
        let sense = err.sense.expect("sense preserved");
        assert!(fu::scsi::ScsiSense::from_buf(&sense).is_illegal_request());
    }

    /// A drive-tagged transport fault (status 0xFF) crosses unchanged.
    #[test]
    fn scsi_transport_fault_maps_unchanged() {
        let err = adapt(|| crate::error::Error::ScsiError {
            opcode: 0,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        });
        assert_eq!(err.status, 0xFF);
        assert!(err.sense.is_none());
    }

    /// A non-SCSI IO fault (ioctl SG_IO == -1: ENODEV/EIO) is a dead bus —
    /// surfaced as 0xFF so the unlock crate bails instead of hammering it.
    #[test]
    fn io_error_maps_to_transport_failure() {
        let err = adapt(|| crate::error::Error::IoError {
            source: std::io::Error::from(std::io::ErrorKind::NotConnected),
        });
        assert_eq!(err.status, crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE);
        assert!(err.sense.is_none());
    }

    /// Device-gone (fd closed) likewise maps to the transport-failure status.
    #[test]
    fn device_not_found_maps_to_transport_failure() {
        let err = adapt(|| crate::error::Error::DeviceNotFound {
            path: "/dev/sg9".into(),
        });
        assert_eq!(err.status, 0xFF);
        assert!(err.sense.is_none());
    }
}

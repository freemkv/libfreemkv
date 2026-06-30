//! Bridges libfreemkv's drive layer to the `freemkv-unlock` crate: one generic
//! SCSI-transport adapter, identity/host-cert mapping, and the dispatch that
//! news up `all_unlockers()` and runs the first matching one. libfreemkv names
//! no individual unlocker — it only calls this bridge.

#![allow(dead_code)] // wired into drive.open() in the next stage-4 step

use freemkv_unlock as fu;

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
            // libfreemkv's transport returns Err only on a transport-layer fault.
            Err(_) => Err(fu::scsi::ScsiError {
                status: 0xFF,
                sense: None,
            }),
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

/// News up the unlockers, build the context, and run the FIRST matching one.
/// Returns what it learned (vid / bus_key / drive_unlocked), or `None` when
/// nothing matched or the matching unlocker did not apply (the caller falls back
/// to its keysource / no-unlock path). `host_certs` are collected by the caller
/// — lazily, only for AACS.
pub(crate) fn run_unlockers(
    scsi: &mut dyn crate::scsi::ScsiTransport,
    drive_id: &crate::identity::DriveId,
    kind: fu::DiscKind,
    host_certs: &[fu::HostCert],
) -> Option<fu::Unlocked> {
    let id = fu::DriveId {
        vendor_id: drive_id.vendor_id.clone(),
        product_revision: drive_id.product_revision.clone(),
        vendor_specific: drive_id.vendor_specific.clone(),
        firmware_date: drive_id.firmware_date.clone(),
    };
    let ctx = fu::UnlockCtx::new(&id, kind, host_certs);
    let mut adapter = ScsiAdapter(scsi);
    for u in fu::all_unlockers() {
        if u.matches(&ctx) {
            return u.unlock(&mut adapter, &ctx).ok();
        }
    }
    None
}

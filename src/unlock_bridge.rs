//! Bridges libfreemkv's drive layer to the `freemkv-unlock` crate: one generic
//! SCSI-transport adapter, identity/host-cert mapping, and the dispatch that
//! assembles the unlocker list and runs each one's `unlock()` until one claims
//! the drive. libfreemkv names no individual unlocker — it only calls this bridge.

use freemkv_unlock as fu;

/// Map libfreemkv's drive identity to the unlock contract's `DriveId`.
fn to_fu_drive_id(drive_id: &crate::identity::DriveId) -> fu::DriveId {
    fu::DriveId {
        vendor_id: drive_id.vendor_id.clone(),
        product_id: drive_id.product_id.clone(),
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

/// The unlock crate's transport over a [`Drive`](crate::drive::Drive) (stop design
/// §2.3): every CDB goes through the Drive's token, so after a Stop the unlocker sees
/// a dead bus (status 0xFF, no sense) and aborts through its transport-fault path.
pub(crate) struct ScsiAdapter<'a> {
    drive: &'a mut crate::drive::Drive,
    /// Open critical spans: while > 0 CDBs run even after a cancel (§2.4 row 4).
    critical: u32,
}

impl<'a> ScsiAdapter<'a> {
    pub(crate) fn new(drive: &'a mut crate::drive::Drive) -> Self {
        ScsiAdapter { drive, critical: 0 }
    }
}

// A Stop refusal: the dead-bus shape every unlocker already aborts on.
fn refused() -> fu::scsi::ScsiError {
    fu::scsi::ScsiError {
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }
}

fn to_lib_dir(dir: fu::scsi::DataDirection) -> crate::scsi::DataDirection {
    match dir {
        fu::scsi::DataDirection::None => crate::scsi::DataDirection::None,
        fu::scsi::DataDirection::FromDevice => crate::scsi::DataDirection::FromDevice,
        fu::scsi::DataDirection::ToDevice => crate::scsi::DataDirection::ToDevice,
    }
}

// libfreemkv's result → the unlock crate's. `Halted` is a refusal (0xFF).
fn to_fu_result(
    r: crate::error::Result<crate::scsi::ScsiResult>,
) -> fu::scsi::Result<fu::scsi::ScsiResult> {
    match r {
        Ok(r) => Ok(fu::scsi::ScsiResult {
            status: r.status,
            bytes_transferred: r.bytes_transferred,
            sense: r.sense,
        }),
        Err(crate::error::Error::Halted) => Err(refused()),
        // Transport Err covers both real faults and a normal CHECK CONDITION
        // status; preserve status+sense so AACS's wedge guard and diagnosis can
        // tell a cert rejection from a dead bus (sense_key@2, asc@12, ascq@13).
        Err(e) => {
            // ScsiError/DiscRead carry real status+sense via extract_scsi_context;
            // any other variant is a non-SCSI transport/IO fault (dead bus, not a
            // drive rejection) — surface transport-failure status so unlock bails.
            let (status, sense) = match &e {
                crate::error::Error::ScsiError { .. } | crate::error::Error::DiscRead { .. } => {
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

impl fu::scsi::ScsiTransport for ScsiAdapter<'_> {
    fn execute(
        &mut self,
        cdb: &[u8],
        dir: fu::scsi::DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> fu::scsi::Result<fu::scsi::ScsiResult> {
        let d = to_lib_dir(dir);
        to_fu_result(if self.critical > 0 {
            self.drive.exec_uncancellable(cdb, d, data, timeout_ms)
        } else {
            self.drive.exec(cdb, d, data, timeout_ms)
        })
    }

    fn pause(&mut self, d: std::time::Duration) -> fu::scsi::Result<()> {
        self.drive.pause(d).map_err(|_| refused())
    }

    fn begin_critical(&mut self) -> fu::scsi::Result<()> {
        if self.drive.is_halted() {
            return Err(refused());
        }
        self.critical += 1;
        Ok(())
    }

    fn end_critical(&mut self) {
        self.critical = self.critical.saturating_sub(1);
    }

    fn execute_cleanup(
        &mut self,
        cdb: &[u8],
        dir: fu::scsi::DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> fu::scsi::Result<fu::scsi::ScsiResult> {
        let ctx = if self.critical > 0 {
            crate::drive::CleanupCtx::Critical
        } else {
            crate::drive::CleanupCtx::Plain
        };
        to_fu_result(
            self.drive
                .exec_cleanup(cdb, to_lib_dir(dir), data, timeout_ms, ctx),
        )
    }
}

// The one Error for an unlocker's `Transport` (dead bus): `Drive::init` and both
// scan bus steps (AACS, CSS) abort with it.
pub(crate) fn unlock_transport_error() -> crate::error::Error {
    crate::error::Error::ScsiError {
        opcode: 0,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }
}

/// Map libfreemkv's host certs (keysource-collected) to the unlock contract's.
pub(crate) fn map_host_certs(certs: &[crate::aacs::types::HostCert]) -> Vec<fu::HostCert> {
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

// (matched_name, result): `Ok(Some)` = that unlocker unlocked; `Ok(None)` = nobody claimed the
// drive; `Err(Transport)` = dead bus.
type Dispatch = (
    &'static str,
    std::result::Result<Option<fu::Unlocked>, fu::UnlockError>,
);

/// Run an ordered set of unlockers, calling each one's uniform `unlock()` and
/// stopping at the first that claims the drive. Shared by [`run_features`] and
/// [`run_bus`]; the two differ ONLY in which unlockers they hand in — an
/// unlocker is an unlocker, so this loop knows nothing about any of them.
fn run(
    unlockers: Vec<Box<dyn fu::Unlocker>>,
    drive: &mut crate::drive::Drive,
    drive_id: &crate::identity::DriveId,
    kind: fu::DiscKind,
) -> Dispatch {
    let id = to_fu_drive_id(drive_id);
    let ctx = fu::UnlockCtx::new(&id, kind);
    let mut adapter = ScsiAdapter::new(drive);
    for u in &unlockers {
        match u.unlock(&mut adapter, &ctx) {
            Ok(None) => continue, // not this one's drive
            Ok(Some(unlocked)) => return (u.name(), Ok(Some(unlocked))),
            Err(e) => return (u.name(), Err(e)), // dead bus — abort
        }
    }
    ("", Ok(None))
}

/// Drive-prep: run the FIRMWARE unlockers (freemkv, including Pioneer runtime / LD), which
/// key off the drive rather than the disc, so `kind` is `Unknown` and they need
/// no certs. Each removes bus encryption at the drive and reads the OEM Volume
/// ID best-effort.
pub(crate) fn run_features(
    drive: &mut crate::drive::Drive,
    drive_id: &crate::identity::DriveId,
) -> Dispatch {
    run(firmware_unlockers(), drive, drive_id, fu::DiscKind::Unknown)
}

/// The FIRMWARE/drive unlockers, in dispatch order. These key off the DRIVE and
/// remove bus encryption AT THE DRIVE, so a VID from one of them means the drive
/// is already unlocked (see `is_drive_unlocker`). Single constructor so both the
/// dispatch and the name/classification helpers share one source of truth.
fn firmware_unlockers() -> Vec<Box<dyn fu::Unlocker>> {
    vec![
        Box::new(fu::FreemkvUnlocker::new()),
        Box::new(fu::LdUnlocker::new()),
    ]
}

// The DISC-keyed unlockers, in dispatch order. `host_certs` are injected into
// the AACS unlocker here (the one place certs enter); pass an empty slice when
// only the names are wanted.
fn disc_unlockers(host_certs: Vec<fu::HostCert>) -> Vec<Box<dyn fu::Unlocker>> {
    vec![
        Box::new(fu::AacsUnlocker::new(host_certs)),
        Box::new(fu::DvdUnlocker::new()),
    ]
}

/// Whether `name` (a matched unlocker's `.name()`) is a firmware/drive unlocker,
/// i.e. one whose success means bus encryption is already removed at the drive.
/// Derived from the real unlocker set, never a hardcoded name list.
///
/// Test-only: the disc-keyed cert route (`run_bus`) can only match the disc
/// unlockers (AACS/DVD), so no production path classifies a `matched` name here —
/// the AACS cert handshake credits bus removal via its `read_data_key`, and a
/// genuine firmware drive-unlock is surfaced by the OEM-VID short-circuit. Retained
/// so the disjointness invariant stays under test.
#[cfg(test)]
pub(crate) fn is_drive_unlocker(name: &str) -> bool {
    firmware_unlockers().iter().any(|u| u.name() == name)
}

/// Content: remove BUS ENCRYPTION for the mounted disc via the DISC-keyed
/// unlockers — the AACS cert route (`host_certs` injected into it at
/// construction, the one place certs enter) and the CSS/DVD route. `kind`
/// selects which self-applies; the other declines.
pub(crate) fn run_bus(
    drive: &mut crate::drive::Drive,
    drive_id: &crate::identity::DriveId,
    kind: fu::DiscKind,
    host_certs: &[fu::HostCert],
) -> Dispatch {
    run(disc_unlockers(host_certs.to_vec()), drive, drive_id, kind)
}

// The unlocker names, in dispatch order, for the user-facing unlocker matrix.
// Derived from the real unlocker instances' `.name()` — firmware set then disc
// set — so the matrix can never drift from what actually dispatches.
pub(crate) fn unlocker_names() -> Vec<&'static str> {
    firmware_unlockers()
        .iter()
        .chain(disc_unlockers(Vec::new()).iter())
        .map(|u| u.name())
        .collect()
}

#[cfg(test)]
#[path = "unlock_bridge_tests.rs"]
mod tests;

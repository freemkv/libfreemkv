//! The post-cancel allow-list (stop design §2.4): once a Drive's token is cancelled,
//! only these clean-up CDBs may still reach the drive. Shared by
//! [`Drive::exec_cleanup`](super::Drive) and the test `FakeTransport`, so the two can
//! never disagree about what is legal after a Stop.

/// PREVENT ALLOW MEDIUM REMOVAL.
pub(crate) const PREVENT_ALLOW: u8 = 0x1E;
/// START STOP UNIT.
pub(crate) const START_STOP_UNIT: u8 = 0x1B;
/// START STOP UNIT with LoEj=1, Start=0: eject the disc if permitted (MMC-6 Table 633).
pub(crate) const EJECT_CDB: [u8; 6] = [START_STOP_UNIT, 0, 0, 0, 0x02, 0];
/// REPORT KEY.
pub(crate) const REPORT_KEY: u8 = 0xA4;

/// REPORT KEY key format 3Fh: invalidate the AGID in CDB byte 10 bits 7-6.
pub(crate) const KEY_FORMAT_INVALIDATE_AGID: u8 = 0x3F;

/// What the Drive holds that a clean-up CDB may release (§2.2 "Ledger").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Ledger {
    /// This Drive sent PREVENT and has not sent ALLOW since.
    pub tray_locked: bool,
    /// Bit `n` set: AGID `n` was allocated through this Drive and not yet invalidated.
    pub agids: u8,
}

/// Where a clean-up CDB is issued from; each widens the list by one §2.4 row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupCtx {
    /// `Drop`, `unlock_tray`, an unlocker's `execute_cleanup`: the ledger rows only.
    Plain,
    /// Inside `DiscSession::finish(Finish::Eject)`: also START STOP UNIT with LoEj=1.
    FinishEject,
    /// Inside a critical section entered before the cancel: any CDB.
    Critical,
}

/// The AGID a REPORT KEY CDB names (byte 10 bits 7-6), if it is a key-format-3Fh
/// invalidate.
pub(crate) fn invalidated_agid(cdb: &[u8]) -> Option<u8> {
    let b10 = *cdb.get(10)?;
    (cdb.first() == Some(&REPORT_KEY) && b10 & 0x3F == KEY_FORMAT_INVALIDATE_AGID)
        .then_some(b10 >> 6)
}

/// Whether `cdb` is PREVENT ALLOW with the Prevent bit clear (an ALLOW).
fn is_allow(cdb: &[u8]) -> bool {
    // SS-5 MMC-6 Table 329: Persistent 0, Prevent 0 = "Prevent State shall be cleared
    // (Unlocked)"; byte 4 bit 0 is Prevent.
    cdb.first() == Some(&PREVENT_ALLOW) && cdb.get(4).is_some_and(|b| b & 0x01 == 0)
}

/// Whether `cdb` is START STOP UNIT with LoEj=1 and Start=0 (an eject).
fn is_eject(cdb: &[u8]) -> bool {
    // SS-6 MMC-6 Table 633: LoEj 1, Start 0 = "Eject the disc if permitted"; byte 4
    // bit 1 is LoEj, bit 0 is Start.
    cdb.first() == Some(&START_STOP_UNIT) && cdb.get(4).is_some_and(|b| b & 0x03 == 0x02)
}

/// §2.4: whether `cdb` may still be issued after the token is cancelled.
pub(crate) fn allowed_after_cancel(cdb: &[u8], ledger: Ledger, ctx: CleanupCtx) -> bool {
    if ctx == CleanupCtx::Critical {
        return true;
    }
    if is_allow(cdb) {
        return ledger.tray_locked;
    }
    if is_eject(cdb) {
        return ctx == CleanupCtx::FinishEject;
    }
    match invalidated_agid(cdb) {
        Some(agid) => ledger.agids & (1 << agid) != 0,
        None => false,
    }
}

#[cfg(test)]
#[path = "allow_tests.rs"]
mod tests;

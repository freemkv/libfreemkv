//! The post-cancel allow-list (stop design §2.4): once a Drive's token is cancelled,
//! only these clean-up CDBs may still reach the drive. Shared by
//! [`Drive::exec_cleanup`](super::Drive) and the test `FakeTransport`, so the two can
//! never disagree about what is legal after a Stop.

/// PREVENT ALLOW MEDIUM REMOVAL.
pub(crate) const PREVENT_ALLOW: u8 = 0x1E;
/// START STOP UNIT.
pub(crate) const START_STOP_UNIT: u8 = 0x1B;
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
    // SS-6 START STOP UNIT: "the logical unit shall unload the medium if the START bit
    // is set to zero" with LOEJ set; byte 4 bit 1 is LoEj, bit 0 is Start.
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
mod tests {
    use super::*;

    const ALLOW: [u8; 6] = [0x1E, 0, 0, 0, 0x00, 0];
    const PREVENT: [u8; 6] = [0x1E, 0, 0, 0, 0x01, 0];
    const EJECT: [u8; 6] = [0x1B, 0, 0, 0, 0x02, 0];
    const START: [u8; 6] = [0x1B, 0, 0, 0, 0x01, 0];
    const READ: [u8; 10] = [0x28, 0, 0, 0, 0, 0, 0, 0, 1, 0];

    fn release(agid: u8) -> [u8; 12] {
        let mut c = [0u8; 12];
        c[0] = REPORT_KEY;
        c[10] = (agid << 6) | KEY_FORMAT_INVALIDATE_AGID;
        c
    }

    /// LD4 `exec_cleanup_allow_list`: one case per §2.4 row, and one refusal per row.
    /// Per evidence SS-7 (AGID) and per the §2.4 table; do not change without a design
    /// revision.
    #[test]
    fn exec_cleanup_allow_list() {
        let none = Ledger::default();
        let locked = Ledger {
            tray_locked: true,
            agids: 0,
        };
        let agid2 = Ledger {
            tray_locked: false,
            agids: 1 << 2,
        };
        use CleanupCtx::*;
        // Row 1: ALLOW iff tray_locked.
        assert!(allowed_after_cancel(&ALLOW, locked, Plain));
        assert!(!allowed_after_cancel(&ALLOW, none, Plain));
        assert!(
            !allowed_after_cancel(&PREVENT, locked, Plain),
            "never PREVENT"
        );
        // Row 2: LoEj only inside finish(Eject).
        assert!(allowed_after_cancel(&EJECT, none, FinishEject));
        assert!(!allowed_after_cancel(&EJECT, locked, Plain));
        assert!(!allowed_after_cancel(&START, none, FinishEject), "LoEj=0");
        // Row 3: 0x3F iff that AGID's bit is set.
        assert!(allowed_after_cancel(&release(2), agid2, Plain));
        assert!(!allowed_after_cancel(&release(1), agid2, Plain));
        assert!(!allowed_after_cancel(&release(2), none, Plain));
        // Row 4: any CDB inside a pre-cancel critical section.
        assert!(allowed_after_cancel(&READ, none, Critical));
        assert!(!allowed_after_cancel(&READ, locked, Plain));
        assert!(!allowed_after_cancel(&READ, agid2, FinishEject));
    }

    #[test]
    fn invalidated_agid_reads_byte_10() {
        assert_eq!(invalidated_agid(&release(3)), Some(3));
        let mut alloc = release(0);
        alloc[10] = 0x00;
        assert_eq!(invalidated_agid(&alloc), None, "format 0 allocates");
        assert_eq!(invalidated_agid(&[0xA4, 0, 0]), None, "short CDB");
    }
}

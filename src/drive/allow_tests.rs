use super::*;

const ALLOW: [u8; 6] = [0x1E, 0, 0, 0, 0x00, 0];
const PREVENT: [u8; 6] = [0x1E, 0, 0, 0, 0x01, 0];
const EJECT: [u8; 6] = EJECT_CDB;
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
    // LoEj=1 with Start=1 loads the tray: not an eject.
    let load = [0x1B, 0, 0, 0, 0x03, 0];
    assert!(!allowed_after_cancel(&load, none, FinishEject), "Start=1");
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
    let mut other = release(3);
    other[0] = 0x00;
    assert_eq!(invalidated_agid(&other), None, "not REPORT KEY");
}

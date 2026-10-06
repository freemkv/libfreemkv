use super::*;

/// `ioctl()` on this fails with `EBADF` before touching any device, so the
/// tests below need no `/dev/sg*` and have no side effects — they run in
/// ordinary CI. Any error that is NOT `InvalidCdbLength` therefore means
/// the CDB cleared the guard and the syscall was reached.
const NO_FD: i32 = -1;

fn reached_the_ioctl(err: &Error) -> bool {
    !matches!(err, Error::InvalidCdbLength { .. })
}

// ── Negative: CDBs the guard must reject ──────────────────────────────

/// The regression this module exists for. `raw_command` used to set
/// `cmd_len = cdb.len().min(16)`, silently shortening an over-length CDB
/// into a *different* SPC-4 command and issuing it. It must fail the
/// caller instead, with the same error `execute()` gives.
///
/// Before the fix this test fails by reaching the `ioctl` and returning
/// `IoError(EBADF)` — the guard never ran.
#[test]
fn over_length_cdb_is_rejected_not_truncated() {
    let cdb = [0u8; K_MAX_CDB_SIZE + 1];
    match SgIoTransport::raw_command(NO_FD, &cdb, 3_000) {
        Err(Error::InvalidCdbLength { len, max }) => {
            assert_eq!(len, K_MAX_CDB_SIZE + 1);
            assert_eq!(max, K_MAX_CDB_SIZE);
        }
        other => panic!(
            "over-length CDB must be rejected with InvalidCdbLength, got {other:?} — \
                 a non-guard error means it was truncated to {K_MAX_CDB_SIZE} bytes and sent"
        ),
    }
}

/// Well past the field width, to show the guard is a bound and not a
/// one-off check at `max + 1`.
#[test]
fn far_over_length_cdb_is_rejected() {
    let cdb = [0u8; 260];
    assert!(matches!(
        SgIoTransport::raw_command(NO_FD, &cdb, 3_000),
        Err(Error::InvalidCdbLength {
            len: 260,
            max: K_MAX_CDB_SIZE
        })
    ));
}

/// An empty CDB must be rejected rather than handed to the sg driver as a
/// zero-length command descriptor, which under SPC-4 is not a command at
/// all. (The pre-fix code did not panic on this — it never read the opcode
/// — it just issued `cmd_len = 0`. The two error paths added here DO read
/// `cdb[0]`, so the guard is now load-bearing for that too.)
#[test]
fn empty_cdb_is_rejected_before_the_opcode_is_read() {
    assert!(matches!(
        SgIoTransport::raw_command(NO_FD, &[], 3_000),
        Err(Error::InvalidCdbLength {
            len: 0,
            max: K_MAX_CDB_SIZE
        })
    ));
}

// ── Positive: CDBs the guard must let through ─────────────────────────

/// The only CDB `raw_command` is called with in the crate. It must still
/// reach the syscall — a guard that rejected this would silently stop
/// unlocking the tray on `Drop`.
#[test]
fn drops_allow_medium_removal_still_reaches_the_ioctl() {
    let err = SgIoTransport::raw_command(NO_FD, &ALLOW_MEDIUM_REMOVAL, 3_000)
        .expect_err("EBADF on fd -1");
    assert!(
        reached_the_ioctl(&err),
        "the 6-byte CDB Drop sends must pass the guard, got {err:?}"
    );
}

/// Every real SPC-4 CDB length (groups 0-5: 6, 10, 12, 16), plus the
/// shortest a caller could legally construct, plus the boundary case — a
/// CDB of exactly `K_MAX_CDB_SIZE` must not be caught by an off-by-one.
#[test]
fn in_range_cdb_lengths_reach_the_ioctl() {
    for len in [1usize, 6, 10, 12, K_MAX_CDB_SIZE] {
        let cdb = vec![0x1Eu8; len];
        let err = SgIoTransport::raw_command(NO_FD, &cdb, 3_000).expect_err("EBADF on fd -1");
        assert!(
            reached_the_ioctl(&err),
            "a {len}-byte CDB is within the {K_MAX_CDB_SIZE}-byte field and must \
                 pass the guard, got {err:?}"
        );
    }
}

// ── fd-reopen path helpers (used by open / drive_has_disc / recovery) ──

/// `to_c_path` must NUL-terminate exactly once and preserve the path bytes,
/// or `libc::open` in the reopen path reads past the buffer or opens a
/// truncated device name.
#[test]
fn to_c_path_nul_terminates_and_preserves_the_bytes() {
    let c = SgIoTransport::to_c_path(Path::new("/dev/sg3"));
    assert_eq!(c.last(), Some(&0u8), "must end in a NUL for libc::open");
    assert_eq!(&c[..c.len() - 1], b"/dev/sg3", "path bytes preserved");
    assert_eq!(
        c.iter().filter(|&&b| b == 0).count(),
        1,
        "exactly one NUL, at the end"
    );
}

/// `resolve_to_sg` passes an sg node through unchanged, and falls back to the
/// original path when there is no filename or the node is neither sr nor sg —
/// the branches that need no sysfs.
#[test]
fn resolve_to_sg_passes_sg_through_and_falls_back_otherwise() {
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("/dev/sg7")),
        Path::new("/dev/sg7"),
        "an sg node is already resolved"
    );
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("/")),
        Path::new("/"),
        "a path with no filename falls back to itself"
    );
    assert_eq!(
        SgIoTransport::resolve_to_sg(Path::new("/dev/foo")),
        Path::new("/dev/foo"),
        "a non-sr, non-sg node is returned unchanged"
    );
}

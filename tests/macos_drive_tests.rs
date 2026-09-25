//! Opt-in checks against an attached macOS optical drive.
//!
//! Run with:
//! `cargo test --test macos_drive_tests -- --ignored --nocapture`

#![cfg(target_os = "macos")]

use libfreemkv::{Drive, DriveStatus, list_drives, scsi::drive_has_disc};
use std::path::Path;
use std::path::PathBuf;

fn drive_name(drive: &libfreemkv::DriveInfo) -> String {
    format!("{} {}", drive.vendor.trim(), drive.model.trim())
        .trim()
        .to_string()
}

/// Exercises the empty-tray path end to end: IOKit enumeration must retain an
/// IOBDServices drive without IOMedia, and the opaque selector must still open
/// that exact service and issue real SCSI commands.
#[test]
#[ignore = "requires an attached optical drive with an empty tray"]
fn empty_tray_drive_is_enumerated_opened_and_reports_no_disc() {
    let drives = list_drives();
    assert!(!drives.is_empty(), "no optical drive enumerated");

    let empty = drives
        .iter()
        .find(|drive| matches!(drive_has_disc(Path::new(&drive.path)), Ok(false)))
        .unwrap_or_else(|| {
            panic!(
                "no empty optical drive found among {} drive(s)",
                drives.len()
            )
        });

    assert!(
        empty.path.starts_with("ioreg:"),
        "empty-tray path should use its IOKit service selector, got {:?}",
        empty.path
    );

    let mut drive = Drive::open(Path::new(&empty.path)).unwrap_or_else(|e| {
        panic!(
            "failed to open {} ({}) with empty tray: {e}",
            drive_name(empty),
            empty.path
        )
    });
    assert_eq!(
        drive.drive_id.raw_inquiry.first().map(|byte| byte & 0x1f),
        Some(0x05),
        "opened service must identify as an MMC optical drive"
    );
    assert_eq!(
        drive.drive_status(),
        DriveStatus::NoDisc,
        "an empty tray should report NoDisc after a real SCSI status query"
    );

    // Opening the selected drive through the same session path as the GUI must
    // reach the drive and fail because there is no medium, not because the
    // optical drive was undiscoverable. Drop the first handle before reopening.
    drop(drive);
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};
    let mut session = DiscSession::open(
        DeviceTarget::Path(PathBuf::from(&empty.path)),
        KeySpec::default(),
    )
    .unwrap_or_else(|e| panic!("failed to create empty-tray session: {e}"));
    assert!(
        session.scan(ScanOptions::default()).is_err(),
        "scanning an empty tray must fail as no media, not report an empty drive as a disc"
    );
}

/// Reads real filesystem/title metadata from a disc, without muxing or saving
/// its content. Run only after inserting a retail Blu-ray or DVD.
#[test]
#[ignore = "requires an inserted Blu-ray/DVD; scan reads disc metadata"]
fn inserted_disc_scans_filesystem_and_finds_titles() {
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};

    let drives = list_drives();
    let media_checks: Vec<_> = drives
        .iter()
        .map(|drive| {
            (
                drive.path.as_str(),
                drive.vendor.as_str(),
                drive.model.as_str(),
                drive_has_disc(Path::new(&drive.path)),
            )
        })
        .collect();
    let drive = drives
        .iter()
        .find(|drive| matches!(drive_has_disc(Path::new(&drive.path)), Ok(true)))
        .unwrap_or_else(|| {
            panic!("no optical drive with media detected; probes: {media_checks:?}")
        });
    println!(
        "scanning {} ({}) {}",
        drive.path,
        drive_name(drive),
        if drive.path.starts_with("ioreg:") {
            "[IOKit service selector]"
        } else {
            ""
        }
    );

    let mut session = DiscSession::open(
        DeviceTarget::Path(PathBuf::from(&drive.path)),
        KeySpec::default(),
    )
    .unwrap_or_else(|e| {
        panic!(
            "failed to open {} ({}): {e}",
            drive_name(&drive),
            drive.path
        )
    });
    let disc = session.scan(ScanOptions::default()).unwrap_or_else(|e| {
        panic!(
            "scan failed for {} ({}): {e}",
            drive_name(&drive),
            drive.path
        )
    });

    assert!(
        !disc.titles.is_empty(),
        "disc scan completed but found no titles on {} ({})",
        drive_name(&drive),
        drive.path
    );
}

/// Exercises the autodetect route used by bare `disc://` sources. Keeping this
/// separate from the explicit-path test catches discovery/open mismatches.
#[test]
#[ignore = "requires an inserted Blu-ray/DVD; opens the first detected drive"]
fn inserted_disc_autodetects_and_scans_titles() {
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};

    let mut session = DiscSession::open(DeviceTarget::Autodetect, KeySpec::default())
        .unwrap_or_else(|e| panic!("autodetect failed to open the inserted disc: {e}"));
    let disc = session
        .scan(ScanOptions::default())
        .unwrap_or_else(|e| panic!("autodetected drive failed to scan the disc: {e}"));
    assert!(
        !disc.titles.is_empty(),
        "autodetected drive scanned the disc but found no titles"
    );
}

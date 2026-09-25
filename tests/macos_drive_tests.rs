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
    .unwrap_or_else(|e| panic!("failed to open {} ({}): {e}", drive_name(drive), drive.path));
    let disc = session.scan(ScanOptions::default()).unwrap_or_else(|e| {
        panic!(
            "scan failed for {} ({}): {e}",
            drive_name(drive),
            drive.path
        )
    });

    assert!(
        !disc.titles.is_empty(),
        "disc scan completed but found no titles on {} ({})",
        drive_name(drive),
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

/// INQUIRY populates vendor / model / firmware for every enumerated drive.
/// Blank fields would indicate the IOKit walk dropped an INQUIRY-carrying
/// child, which would silently break drive-fingerprint keydb lookups.
#[test]
#[ignore = "requires an attached optical drive"]
fn every_enumerated_drive_reports_vendor_model_firmware() {
    let drives = list_drives();
    assert!(!drives.is_empty(), "no optical drive enumerated");
    for d in &drives {
        assert!(!d.vendor.trim().is_empty(), "empty vendor for {}", d.path);
        assert!(!d.model.trim().is_empty(), "empty model for {}", d.path);
        assert!(
            !d.firmware.trim().is_empty(),
            "empty firmware for {} ({} {})",
            d.path,
            d.vendor,
            d.model
        );
        assert!(
            !d.path.is_empty(),
            "empty selector path for {} {}",
            d.vendor,
            d.model
        );
    }
}

/// Opening the same drive twice back-to-back must succeed. Catches leaked
/// exclusive-open state in the IOKit session path — a regression would
/// surface as "device busy" on the second open.
#[test]
#[ignore = "requires an attached optical drive"]
fn drive_can_be_reopened_after_close() {
    let drives = list_drives();
    let target = drives.first().expect("no optical drive enumerated");
    for i in 0..3 {
        let drive = Drive::open(Path::new(&target.path)).unwrap_or_else(|e| {
            panic!(
                "iter {i}: failed to reopen {} ({}): {e}",
                drive_name(target),
                target.path
            )
        });
        drop(drive);
    }
}

/// UHD disc scan populates the AACS 2.0 shape we advertise on the UI:
/// bus_encryption true, MKB version present, disc_hash populated, VID set,
/// at least one CPS unit key. Verifies the scan pipeline end-to-end for
/// the exact disc type most likely to regress.
#[test]
#[ignore = "requires an inserted UHD Blu-ray; scans AACS 2.0 disc metadata"]
fn inserted_uhd_disc_reports_full_aacs2_state() {
    use libfreemkv::disc::DiscFormat;
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};

    let mut session = DiscSession::open(DeviceTarget::Autodetect, KeySpec::default())
        .unwrap_or_else(|e| panic!("autodetect failed to open the inserted disc: {e}"));
    let disc = session
        .scan(ScanOptions::default())
        .unwrap_or_else(|e| panic!("scan failed: {e}"));

    assert!(
        matches!(disc.format, DiscFormat::Uhd),
        "expected UHD DiscFormat, got {:?}",
        disc.format
    );
    assert!(
        disc.capacity_bytes >= 40_000_000_000,
        "UHD capacity should be ≥ 40 GB, got {} bytes",
        disc.capacity_bytes
    );
    assert!(
        disc.layers >= 2,
        "UHD disc should report ≥ 2 layers, got {}",
        disc.layers
    );

    let aacs = disc.aacs.as_ref().expect("UHD disc must have AacsState");
    assert_eq!(aacs.version, 2, "UHD disc must be AACS 2.0");
    assert!(aacs.bus_encryption, "UHD disc must set bus_encryption");
    assert!(
        aacs.mkb_version.is_some_and(|v| v > 0),
        "UHD disc must expose MKB version, got {:?}",
        aacs.mkb_version
    );
    assert!(
        aacs.disc_hash.starts_with("0x") && aacs.disc_hash.len() == 42,
        "disc_hash should be 0x-prefixed 40 hex chars, got {:?}",
        aacs.disc_hash
    );
    assert_ne!(
        aacs.volume_id, [0u8; 16],
        "VID must be populated after a successful AACS 2.0 handshake"
    );
    assert!(
        !aacs.unit_keys.is_empty(),
        "at least one CPS unit key expected for a scannable UHD disc"
    );
}

/// Scan is idempotent: rescanning the same session yields the same disc
/// hash and MKB version. Guards against stateful drift in the scan path.
#[test]
#[ignore = "requires an inserted Blu-ray/DVD; rescans twice"]
fn rescan_returns_stable_disc_identity() {
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};

    let mut session = DiscSession::open(DeviceTarget::Autodetect, KeySpec::default())
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    let first_snapshot = {
        let first = session
            .scan(ScanOptions::default())
            .unwrap_or_else(|e| panic!("first scan failed: {e}"));
        first
            .aacs
            .as_ref()
            .map(|a| (a.disc_hash.clone(), a.mkb_version, a.volume_id))
    };
    let second_snapshot = {
        let second = session
            .scan(ScanOptions::default())
            .unwrap_or_else(|e| panic!("second scan failed: {e}"));
        second
            .aacs
            .as_ref()
            .map(|a| (a.disc_hash.clone(), a.mkb_version, a.volume_id))
    };

    let (Some(a), Some(b)) = (first_snapshot, second_snapshot) else {
        return;
    };
    assert_eq!(a.0, b.0, "disc_hash drifted across rescans");
    assert_eq!(a.1, b.1, "mkb_version drifted across rescans");
    assert_eq!(a.2, b.2, "VID drifted across rescans");
}

/// `drive_has_disc` matches what `DiscSession::open + scan` actually finds.
/// Every drive returning `Ok(true)` must be scannable; every `Ok(false)`
/// must fail to scan. Catches enumeration/probe divergence.
#[test]
#[ignore = "requires an attached optical drive; probes every enumerated drive"]
fn drive_has_disc_matches_scan_outcome() {
    use libfreemkv::{DeviceTarget, DiscSession, KeySpec, ScanOptions};

    let drives = list_drives();
    assert!(!drives.is_empty(), "no optical drive enumerated");
    for d in &drives {
        match drive_has_disc(Path::new(&d.path)) {
            Ok(true) => {
                let mut session = DiscSession::open(
                    DeviceTarget::Path(PathBuf::from(&d.path)),
                    KeySpec::default(),
                )
                .unwrap_or_else(|e| panic!("open failed for {}: {e}", d.path));
                let _ = session
                    .scan(ScanOptions::default())
                    .unwrap_or_else(|e| panic!("scan failed on drive_has_disc=true: {e}"));
            }
            Ok(false) => {
                let mut session = DiscSession::open(
                    DeviceTarget::Path(PathBuf::from(&d.path)),
                    KeySpec::default(),
                )
                .unwrap_or_else(|e| panic!("open failed for empty tray {}: {e}", d.path));
                assert!(
                    session.scan(ScanOptions::default()).is_err(),
                    "scan unexpectedly succeeded on drive_has_disc=false: {}",
                    d.path
                );
            }
            Err(e) => panic!("drive_has_disc errored on {}: {e}", d.path),
        }
    }
}

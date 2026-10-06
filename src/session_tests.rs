use super::*;
use crate::aacs::types::{HostCert, UnitKey};
use crate::keysource::ResolveCtx;

fn creds_with(n: usize) -> DriveCredentials {
    DriveCredentials {
        host_certs: (0..n)
            .map(|_| HostCert {
                private_key: [0u8; 20],
                certificate: Vec::new(),
                private_key_v2: None,
                certificate_v2: None,
            })
            .collect(),
    }
}

struct TestSource;
impl KeySource for TestSource {
    fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
        Ok(Vec::new())
    }
    fn label(&self) -> &'static str {
        "test-source"
    }
}

#[test]
fn forwards_spec_credentials_into_empty_opts() {
    let mut spec = KeySpec {
        credentials: Some(creds_with(2)),
        ..Default::default()
    };
    let opts = forward_key_material(&mut spec, ScanOptions::default());
    // Kills the "drop the forward" mutant.
    assert_eq!(opts.credentials.map(|c| c.host_certs.len()), Some(2));
}

#[test]
fn does_not_clobber_caller_credentials() {
    let mut spec = KeySpec {
        credentials: Some(creds_with(2)),
        ..Default::default()
    };
    let opts = ScanOptions {
        credentials: Some(creds_with(5)),
        ..Default::default()
    };
    let opts = forward_key_material(&mut spec, opts);
    // Kills a mutant that flips `is_none()` → always-overwrite.
    assert_eq!(opts.credentials.map(|c| c.host_certs.len()), Some(5));
    // The unused spec creds stay put.
    assert_eq!(spec.credentials.map(|c| c.host_certs.len()), Some(2));
}

#[test]
fn moves_spec_key_sources_into_empty_opts() {
    let mut spec = KeySpec {
        key_sources: vec![Box::new(TestSource)],
        ..Default::default()
    };
    let opts = forward_key_material(&mut spec, ScanOptions::default());
    assert_eq!(opts.key_sources.len(), 1);
    assert_eq!(opts.key_sources[0].label(), "test-source");
    // Moved, not cloned — the spec is emptied (kills a copy-instead-of-move
    // mutant, and confirms the take()).
    assert!(spec.key_sources.is_empty());
}

#[test]
fn does_not_clobber_caller_key_sources() {
    let mut spec = KeySpec {
        key_sources: vec![Box::new(TestSource)],
        ..Default::default()
    };
    let opts = ScanOptions {
        key_sources: vec![Box::new(TestSource), Box::new(TestSource)],
        ..Default::default()
    };
    let opts = forward_key_material(&mut spec, opts);
    // Kills a mutant that flips `is_empty()` → always-overwrite.
    assert_eq!(opts.key_sources.len(), 2);
    // Caller's non-empty vec means the spec is left untouched.
    assert_eq!(spec.key_sources.len(), 1);
}

#[test]
fn keyspec_default_is_all_empty() {
    let spec = KeySpec::default();
    assert!(spec.keydb_path.is_none());
    assert!(spec.key_url.is_none());
    assert!(spec.key_auth.is_none());
    assert!(spec.credentials.is_none());
    assert!(spec.key_sources.is_empty());
}

/// A minimal keyless AACS `Disc`: `inputs()` returns `Some`. No titles.
fn aacs_disc() -> Disc {
    Disc {
        volume_id: "TEST".into(),
        meta_title: None,
        format: crate::DiscFormat::Uhd,
        capacity_sectors: 0,
        capacity_bytes: 0,
        layers: 1,
        titles: Vec::new(),
        region: crate::disc::DiscRegion::Free,
        aacs: Some(crate::disc::AacsState {
            version: crate::aacs::mkb::AACS_MAJOR_UHD,
            bus_encryption: false,
            mkb_version: None,
            disc_hash: "0xabc".into(),
            volume_id: [0u8; 16],
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        }),
        css: None,
        encrypted: true,
        aacs_error: None,
        css_error: None,
        content_format: crate::ContentFormat::BdTs,
    }
}

// `inputs_with_samples` carries real encrypted units from the main feature, so
// a source's key is validated at disc open (bare `inputs()` has none).
#[test]
fn inputs_with_samples_fills_encrypted_units_from_the_main_title() {
    use crate::disc::{DiscTitle, Extent};
    struct Encrypted;
    impl SectorSource for Encrypted {
        fn read_sectors(&mut self, _l: u32, c: u16, b: &mut [u8], _: bool) -> Result<usize> {
            let n = c as usize * 2048;
            b[..n].fill(0xC0); // CPI-flagged: encrypted
            Ok(n)
        }
    }
    let mut disc = aacs_disc();
    let mut t = DiscTitle::empty();
    t.size_bytes = 1;
    t.extents = vec![Extent {
        start_lba: 3_000,
        sector_count: 3_000,
    }];
    disc.titles = vec![t];
    assert!(disc.inputs().expect("aacs").samples.is_empty());
    let inputs = disc
        .inputs_with_samples(&mut Encrypted, crate::keysource::MIN_SAMPLE_UNITS)
        .expect("aacs");
    assert_eq!(inputs.samples.len(), crate::keysource::MIN_SAMPLE_UNITS);
}

// `identify` after the drive has left the session (both `stage_drive_as_reader`
// and `into_drive` permit that ordering) must return `DeviceNotReady`, not
// reach `drive_mut`'s `.expect(...)` and panic. Sibling of scan.
#[test]
fn identify_without_a_drive_is_clean_device_not_ready() {
    let mut session = DiscSession::from_parts_for_test(None, None);
    let err = session
        .identify()
        .expect_err("identify without a drive must error, not panic");
    assert!(
        matches!(err, Error::DeviceNotReady { .. }),
        "expected DeviceNotReady, got {err:?}"
    );
}

// ── DiscSession drive lifecycle: stage / into_reader / into_drive ─────────

/// A transport that tolerates any command (the drive's Drop runs a
/// tray-unlock through it). Returns a benign GOOD status.
struct NoopTransport;
impl crate::scsi::ScsiTransport for NoopTransport {
    fn execute(
        &mut self,
        _cdb: &[u8],
        _direction: crate::scsi::DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        Ok(crate::scsi::ScsiResult {
            status: 0,
            bytes_transferred: 0,
            sense: [0u8; 32],
        })
    }
}

fn session_with_drive() -> DiscSession {
    DiscSession::from_drive_for_test(Drive::from_transport_for_test(Box::new(NoopTransport)))
}

/// `stage_drive_as_reader` moves the owned `Drive` into the `reader` slot:
/// afterward the reader is staged (`into_reader` is `Some`) and the drive
/// slot is emptied. The cached `device_path` survives the move.
#[test]
fn stage_drive_as_reader_moves_drive_into_reader_slot() {
    let mut session = session_with_drive();
    assert_eq!(session.device_path(), "test");
    session.stage_drive_as_reader();
    // device_path still resolves after the drive has moved out.
    assert_eq!(
        session.device_path(),
        "test",
        "device_path is cached and survives the drive move"
    );
    assert!(
        session.into_reader().is_some(),
        "the drive must be staged as the reader"
    );
}

/// A staged drive left the drive slot: `into_drive` then errors cleanly with
/// `DeviceNotReady` (mutually exclusive with `into_reader`, which holds it).
#[test]
fn into_drive_errors_after_staging_moved_the_drive_out() {
    let mut session = session_with_drive();
    session.stage_drive_as_reader();
    // `Drive` isn't `Debug`, so match on the result rather than `expect_err`.
    assert!(
        matches!(session.into_drive(), Err(Error::DeviceNotReady { .. })),
        "into_drive after staging must error with DeviceNotReady, not return a drive"
    );
}

/// The two consuming exits are mutually exclusive. An UNSTAGED session hands
/// the drive out via `into_drive` (and has no staged reader); a STAGED
/// session hands the reader out via `into_reader` (and has no drive).
#[test]
fn into_drive_and_into_reader_are_mutually_exclusive() {
    // Unstaged: the drive is available; the reader is not.
    let unstaged = session_with_drive();
    assert!(
        unstaged.into_reader().is_none(),
        "an unstaged session has no reader to hand out"
    );
    let unstaged = session_with_drive();
    assert!(
        unstaged.into_drive().is_ok(),
        "an unstaged session hands the drive out"
    );

    // Staged: the reader is available; the drive is not.
    let mut staged = session_with_drive();
    staged.stage_drive_as_reader();
    assert!(
        staged.into_reader().is_some(),
        "a staged session hands the reader out"
    );
}

/// `stage_drive_as_reader` is a no-op when the session holds no drive
/// (already staged or moved out): the reader slot stays empty.
#[test]
fn stage_drive_as_reader_is_noop_without_a_drive() {
    let mut session = DiscSession::from_parts_for_test(None, None);
    session.stage_drive_as_reader();
    assert!(
        session.into_reader().is_none(),
        "staging with no drive leaves the reader slot empty"
    );
}

/// A double stage is idempotent: the first moves the drive into the reader,
/// the second is a no-op (drive slot already empty), and the single staged
/// reader remains available.
#[test]
fn stage_drive_as_reader_is_idempotent() {
    let mut session = session_with_drive();
    session.stage_drive_as_reader();
    session.stage_drive_as_reader();
    assert!(
        session.into_reader().is_some(),
        "the reader staged by the first call survives a redundant second call"
    );
}

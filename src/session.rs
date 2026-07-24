//! Disc session — one place that opens an optical drive and brings the SCSI
//! transport up, so the consumers (CLI, autorip) stop hand-rolling the
//! `open → wait_ready → init → probe_disc → identify → scan` preamble.
//!
//! The session owns the [`Drive`] by value (tray unlock stays guaranteed via
//! `Drive::drop`) and, after [`DiscSession::scan`], the resulting [`Disc`].
//! Lifecycle is intentionally SPLIT — `open` does transport mechanics only,
//! `identify` / `scan` are separate — so a consumer can fetch a poster off a
//! fast `identify` and update its UI before committing to a full `scan`.
//!
//! libfreemkv resolves no keys and reads no keydb: the consumer builds the
//! host credentials / key-source layer (from `freemkv_keysources`) and hands
//! them in via [`KeySpec`]; the session merely FORWARDS them into
//! [`ScanOptions`] at scan time. No cert derivation happens here.

use crate::disc::{Disc, DiscId, DriveCredentials, ScanOptions};
use crate::drive::{Drive, find_drive};
use crate::error::{Error, Result};
use crate::keysource::KeySource;
use crate::sector::{FileSectorSource, SectorSource};
use std::path::{Path, PathBuf};

/// Which optical device a [`DiscSession`] should open.
pub enum DeviceTarget {
    /// Open this exact device path (e.g. `/dev/sg0`).
    Path(PathBuf),
    /// Enumerate drives and pick one that currently has media
    /// (see [`find_drive`]).
    Autodetect,
}

/// Consumer-supplied key material for the live-drive AACS handshake.
///
/// libfreemkv does NOT read `keydb.cfg`, build a `KeydbSource`, or extract host
/// certs — that layer lives in the application (`freemkv_keysources`), which
/// depends on libfreemkv, not the other way round. The consumer builds the
/// credentials / key-source layer and passes them in here; [`DiscSession::scan`]
/// forwards them into [`ScanOptions`]. The `keydb_path` / `key_url` / `key_auth`
/// fields are carried purely for the CONSUMER's own bookkeeping — the library
/// ignores them.
#[derive(Default)]
pub struct KeySpec {
    /// Consumer bookkeeping only — the library does not read it.
    pub keydb_path: Option<PathBuf>,
    /// Consumer bookkeeping only — the library does not read it.
    pub key_url: Option<String>,
    /// Consumer bookkeeping only — the library does not read it.
    pub key_auth: Option<String>,
    /// Host cert(s) for the live-drive handshake, pre-built by the consumer.
    /// Forwarded to [`ScanOptions::credentials`] at scan time.
    pub credentials: Option<DriveCredentials>,
    /// Consumer-built key-source layer; the handshake collects host certs
    /// across these. Moved into [`ScanOptions::key_sources`] at scan time.
    pub key_sources: Vec<Box<dyn KeySource>>,
}

/// An opened optical drive plus the disc scanned off it.
///
/// Owns the [`Drive`] by value. Consumers that still need the raw drive (e.g.
/// to sample ciphertext for key validation, or to move it into a
/// `DiscStream`) reach it via [`Self::drive_mut`] / [`Self::into_drive`]; the
/// scanned [`Disc`] comes out via [`Self::disc`] / [`Self::take_disc`].
pub struct DiscSession {
    drive: Drive,
    spec: KeySpec,
    disc: Option<Disc>,
    /// Sector source for a later file/live mux to `.take()` (steps 3–4).
    /// Unpopulated in the current step; shapes the struct for the mux hoist.
    reader: Option<Box<dyn SectorSource>>,
}

/// Overlay the session's consumer-supplied key material onto a caller's
/// [`ScanOptions`], without ever clobbering what the caller already set.
///
/// Pure (no drive I/O) so the KeySpec → ScanOptions derivation is unit-testable
/// without hardware. `credentials` is copied (it is `Clone`); `key_sources` is
/// MOVED out of the spec (trait objects are not `Clone`), leaving the spec's
/// vec empty once consumed.
fn forward_key_material(spec: &mut KeySpec, mut opts: ScanOptions) -> ScanOptions {
    if opts.credentials.is_none() {
        opts.credentials = spec.credentials.clone();
    }
    if opts.key_sources.is_empty() {
        opts.key_sources = std::mem::take(&mut spec.key_sources);
    }
    opts
}

impl DiscSession {
    /// Open a drive and bring the SCSI transport up.
    ///
    /// Resolves the device (`Autodetect` → [`find_drive`]), opens it (FATAL —
    /// the only hard failure here), then runs `wait_ready` → `init` →
    /// `probe_disc`. Those three are ADVISORY exactly as every consumer treated
    /// them: a failure is logged via `tracing` and discarded — the later
    /// [`Self::scan`] is the authoritative gate. No scan, no identify, no key
    /// resolution runs here.
    pub fn open(target: DeviceTarget, spec: KeySpec) -> Result<DiscSession> {
        let mut drive = match target {
            DeviceTarget::Path(ref path) => Drive::open(path)?,
            // Autodetect yields an already-opened drive; a missing drive is a
            // typed `DeviceNotFound` the application maps to its own message.
            DeviceTarget::Autodetect => find_drive().ok_or_else(|| Error::DeviceNotFound {
                path: String::new(),
            })?,
        };

        // Advisory bring-up — non-fatal in every consumer today. Preserve that:
        // log and continue, never propagate. (The CLI printed these to stderr /
        // discarded them; autorip `tracing::warn`'d them. The advisory SEMANTICS
        // are what matter and are preserved identically; the sink is now here.)
        if let Err(e) = drive.wait_ready() {
            tracing::warn!(target: "freemkv::session", error = %e, "wait_ready advisory failed (continuing)");
        }
        if let Err(e) = drive.init() {
            tracing::warn!(target: "freemkv::session", error = %e, "init advisory failed (continuing)");
        }
        if let Err(e) = drive.probe_disc() {
            tracing::warn!(target: "freemkv::session", error = %e, "probe_disc advisory failed (continuing)");
        }

        Ok(DiscSession {
            drive,
            spec,
            disc: None,
            reader: None,
        })
    }

    /// Fast disc identification — name/format only, no playlist parse. Wraps
    /// [`Disc::identify`].
    pub fn identify(&mut self) -> Result<DiscId> {
        Disc::identify(&mut self.drive)
    }

    /// Full structure scan. Forwards the session's [`KeySpec`] credentials /
    /// key-sources into `opts` (without clobbering anything the caller already
    /// set), runs [`Disc::scan`], stores the result, and returns a borrow.
    pub fn scan(&mut self, opts: ScanOptions) -> Result<&Disc> {
        let opts = forward_key_material(&mut self.spec, opts);
        let disc = Disc::scan(&mut self.drive, &opts)?;
        self.disc = Some(disc);
        Ok(self.disc.as_ref().expect("disc just stored"))
    }

    /// The scanned disc, if [`Self::scan`] has run.
    pub fn disc(&self) -> Option<&Disc> {
        self.disc.as_ref()
    }

    /// Mutable access to the scanned disc, if [`Self::scan`] has run.
    pub fn disc_mut(&mut self) -> Option<&mut Disc> {
        self.disc.as_mut()
    }

    /// Take ownership of the scanned disc out of the session, leaving `None`.
    /// Consumers that need the owned `Disc` alongside a live `&mut Drive`
    /// (key-resolution, per-title crack) take the disc, then borrow the drive.
    pub fn take_disc(&mut self) -> Option<Disc> {
        self.disc.take()
    }

    /// Shared access to the opened drive (identity, profile, path).
    pub fn drive(&self) -> &Drive {
        &self.drive
    }

    /// Mutable access to the opened drive — for ciphertext sampling and other
    /// direct reads consumers still perform.
    pub fn drive_mut(&mut self) -> &mut Drive {
        &mut self.drive
    }

    /// Lock the tray so the disc cannot eject mid-rip. Unlock is guaranteed by
    /// `Drive::drop`.
    pub fn lock_tray(&mut self) {
        self.drive.lock_tray();
    }

    /// Consume the session, returning the owned drive (e.g. to move into a
    /// `DiscStream` for a live-drive mux).
    pub fn into_drive(self) -> Drive {
        self.drive
    }

    /// Consume the session, returning the sector source staged for a later mux
    /// (steps 3–4). `None` until that path populates it.
    pub fn into_reader(self) -> Option<Box<dyn SectorSource>> {
        self.reader
    }
}

/// Scan an ISO image's structure from a file path, returning the scanned
/// [`Disc`] together with a reusable [`SectorSource`] over the same file.
///
/// This is the file-backed counterpart to [`DiscSession::scan`]: it is the one
/// place that opens a [`FileSectorSource`], reads its capacity, and runs
/// [`Disc::scan_image`], so consumers (CLI, autorip) stop hand-rolling that
/// triple and stop constructing the low-level reader themselves. No SCSI, no
/// handshake, no key resolution — AACS resolution during the scan uses only
/// whatever `opts` already carries (mirroring how `Disc::scan_image` forwards
/// `ScanOptions`).
///
/// The returned reader is a fresh handle positioned at the start of the image;
/// callers that need to sample ciphertext (key resolution) or feed a mux can
/// reuse it directly rather than re-opening the file. `Disc::scan_image` reads
/// only through the same reader, and all reads are LBA-addressed, so the
/// handle is fully reusable afterward.
pub fn scan_iso(path: &Path, opts: ScanOptions) -> Result<(Disc, Box<dyn SectorSource>)> {
    let mut reader = FileSectorSource::open(path)?;
    let capacity = reader.capacity_sectors();
    let disc = Disc::scan_image(&mut reader, capacity, &opts)?;
    Ok((disc, Box::new(reader)))
}

#[cfg(test)]
mod tests {
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
}

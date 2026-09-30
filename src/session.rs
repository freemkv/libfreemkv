//! Disc session — one place that opens an optical drive and brings the SCSI
//! transport up, so the consumers (CLI, autorip) stop hand-rolling the
//! `open → wait_ready → init → probe_disc → identify → scan` preamble.
//!
//! The session owns the [`Drive`] by value (tray unlock stays guaranteed via
//! `Drive::drop`) and, after [`DiscSession::scan`], the resulting [`Disc`].
//! libfreemkv resolves no keys and reads no keydb: the consumer supplies its
//! own key material via [`KeySpec`], forwarded to [`ScanOptions`] at scan time.

use crate::disc::{Disc, DiscId, DriveCredentials, ScanOptions};
use crate::drive::{Drive, find_drive};
use crate::error::{Error, Result};
use crate::halt::{Halt, Progress};
use crate::keysource::KeySource;
use crate::sector::{FileSectorSource, SectorSource};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A consumer-supplied factory for the ordered AACS key-source layer.
///
/// libfreemkv builds no key sources itself (the `freemkv_keysources` crate that
/// implements [`KeySource`] depends on libfreemkv, not the other way round), so
/// the consumer hands in a way to build its sources. [`ResolvedKeySet::resolve`]
/// calls it once and keeps nothing; it stays `Send + Sync` without requiring
/// `KeySource: Send`.
///
/// [`ResolvedKeySet::resolve`]: crate::keys::ResolvedKeySet::resolve
pub type KeySourceFactory = Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync>;

/// Which optical device a [`DiscSession`] should open.
pub enum DeviceTarget {
    /// Open this exact device path (e.g. `/dev/sg0`).
    Path(PathBuf),
    /// Enumerate drives and pick one that currently has media
    /// (see [`find_drive`]).
    Autodetect,
}

/// How [`DiscSession::finish`] ends a session on the handle it already holds (stop
/// design §2.5), instead of re-opening the drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finish {
    /// Close the handle; a tray this session locked is unlocked on the way out.
    Release,
    /// Unlock the tray (PREVENT ALLOW, Prevent 0), then close the handle.
    Unlock,
    /// Unlock the tray, then eject it (START STOP UNIT, LoEj 1). Allowed after a Stop.
    Eject,
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
/// `DiscStream`) reach it via [`Self::into_drive`]; the
/// scanned [`Disc`] comes out via [`Self::disc`] / [`Self::take_disc`].
pub struct DiscSession {
    /// The opened drive. `Some` from [`Self::open`] until
    /// [`Self::stage_drive_as_reader`] (live-drive mux) or [`Self::into_drive`]
    /// moves it out. The cached [`Self::device_path`] survives that move so the
    /// mux driver can still name the device in an error without the drive.
    drive: Option<Drive>,
    /// The drive's device path, cached at [`Self::open`] so it outlives a
    /// [`Self::stage_drive_as_reader`] that moves the drive into `reader`.
    device: String,
    spec: KeySpec,
    disc: Option<Disc>,
    /// Sector source for a later file/live mux to `.take()` (steps 3–4). The
    /// file path stages a `FileSectorSource`; the live-drive path stages the
    /// drive itself via [`Self::stage_drive_as_reader`].
    reader: Option<Box<dyn SectorSource>>,
    /// The op token from [`Self::open_with`] (stop design §2.2): every CDB, the scan and
    /// the key resolution observe it. `None` for [`Self::open`].
    halt: Option<Halt>,
    /// The op's progress from [`Self::attach_progress`] (T29): the drive's CDBs and each
    /// key-source call report to it.
    progress: Option<Progress>,
}

// Overlay the session's key material onto `opts` without clobbering what the
// caller already set. `credentials` is cloned; `key_sources` is moved out of
// `spec` (trait objects aren't `Clone`), leaving the spec's vec empty.
fn forward_key_material(spec: &mut KeySpec, mut opts: ScanOptions) -> ScanOptions {
    if opts.credentials.is_none() {
        opts.credentials = spec.credentials.clone();
    }
    if opts.key_sources.is_empty() {
        opts.key_sources = std::mem::take(&mut spec.key_sources);
    }
    opts
}

// A failed scan hands back the sources `forward_key_material` moved, so a retry on the
// same session still has them.
fn restore_key_sources(spec: &mut KeySpec, opts: &mut ScanOptions, took: bool) {
    if took {
        spec.key_sources = std::mem::take(&mut opts.key_sources);
    }
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
        let drive = match target {
            DeviceTarget::Path(ref path) => Drive::open(path)?,
            // Autodetect yields an already-opened drive; a missing drive is a
            // typed `DeviceNotFound` the application maps to its own message.
            DeviceTarget::Autodetect => find_drive().ok_or_else(|| Error::DeviceNotFound {
                path: String::new(),
            })?,
        };
        Self::bring_up(drive, spec, None)
    }

    /// [`Self::open`] under the caller's op token (stop design §2.2): the drive checks
    /// `halt` on every CDB, a Stop during the bring-up ends it `Halted` (the handle is
    /// closed), and the session keeps the token for [`Self::scan_with`] and
    /// [`Self::resolve_key_set`].
    pub fn open_with(target: DeviceTarget, spec: KeySpec, halt: &Halt) -> Result<DiscSession> {
        let drive = match target {
            DeviceTarget::Path(ref path) => Drive::open_with(path, halt)?,
            DeviceTarget::Autodetect => {
                let mut drive = find_drive().ok_or_else(|| Error::DeviceNotFound {
                    path: String::new(),
                })?;
                drive.attach(halt);
                drive
            }
        };
        Self::bring_up(drive, spec, Some(halt.clone()))
    }

    /// A session over a drive the caller already opened, with no bring-up and no scan:
    /// for a follow-up such as an on-demand eject that must end through [`Self::finish`]
    /// on this one handle rather than a second open (stop design §2.5).
    pub fn from_drive(drive: Drive) -> DiscSession {
        let device = drive.device_path().to_string();
        DiscSession {
            drive: Some(drive),
            device,
            spec: KeySpec::default(),
            disc: None,
            reader: None,
            halt: None,
            progress: None,
        }
    }

    // Test-only: give a `from_parts_for_test` session the op token `open_with` would.
    #[cfg(test)]
    pub(crate) fn set_halt_for_test(&mut self, halt: &Halt) {
        self.halt = Some(halt.clone());
    }

    // `wait_ready` → `init` → `probe_disc`, advisory. Under an op token (`open_with`) a
    // Stop is the one failure that is not advisory.
    pub(crate) fn bring_up(
        mut drive: Drive,
        spec: KeySpec,
        halt: Option<Halt>,
    ) -> Result<DiscSession> {
        // Advisory bring-up — non-fatal in every consumer today. Preserve that: log
        // and continue, never propagate (CLI discarded these, autorip warned) — the
        // advisory semantics are preserved identically; only the sink moved here.
        type Step = fn(&mut Drive) -> Result<()>;
        let steps: [(&str, Step); 3] = [
            ("wait_ready", Drive::wait_ready),
            ("init", Drive::init),
            ("probe_disc", Drive::probe_disc),
        ];
        for (step, run) in steps {
            match run(&mut drive) {
                // Stop is always honoured: dropping `drive` closes the handle.
                Err(Error::Halted) if halt.is_some() => return Err(Error::Halted),
                Err(e) => {
                    tracing::warn!(target: "freemkv::session", step, error = %e, "advisory bring-up step failed (continuing)")
                }
                Ok(()) => {}
            }
        }
        // The final check (§6 ST-L3): a Stop after the last CDB still ends the open.
        if let Some(h) = &halt {
            h.check()?;
        }

        let device = drive.device_path().to_string();
        Ok(DiscSession {
            drive: Some(drive),
            device,
            spec,
            disc: None,
            reader: None,
            halt,
            progress: None,
        })
    }

    /// The op token from [`Self::open_with`]; `None` for [`Self::open`].
    pub fn token(&self) -> Option<&Halt> {
        self.halt.as_ref()
    }

    /// Report the op's forward progress to `p` (T29): every drive CDB (as
    /// [`Drive::attach_progress`]) and every key-source call in [`Self::resolve_key_set`].
    pub fn attach_progress(&mut self, p: &Progress) {
        if let Some(drive) = self.drive.as_mut() {
            drive.attach_progress(p);
        }
        self.progress = Some(p.clone());
    }

    /// Fast disc identification — name/format only, no playlist parse. Wraps
    /// [`Disc::identify`].
    pub fn identify(&mut self) -> Result<DiscId> {
        // Same reachability as `scan`/`resolve_key_set`: public `stage_drive_as_reader`/
        // `into_drive` move the drive out, so this slot can legitimately be empty.
        // A library must not panic from public API, so return typed `DeviceNotReady`.
        let drive = self.drive.as_mut().ok_or_else(|| Error::DeviceNotReady {
            path: self.device.clone(),
        })?;
        Disc::identify(drive)
    }

    /// Full structure scan. Forwards the session's [`KeySpec`] credentials /
    /// key-sources into `opts` (without clobbering anything the caller already
    /// set), runs [`Disc::scan`], stores the result, and returns a borrow. A failed
    /// scan leaves the session's key sources in place for a retry.
    pub fn scan(&mut self, opts: ScanOptions) -> Result<&Disc> {
        let took = opts.key_sources.is_empty() && !self.spec.key_sources.is_empty();
        let mut opts = forward_key_material(&mut self.spec, opts);
        // `stage_drive_as_reader` is PUBLIC and moves the drive out, so this slot can
        // legitimately be empty here. "No shipped consumer calls it in that order" is
        // not "cannot happen" — the public surface permits it, so it must be an error.
        let scanned = match self.drive.as_mut() {
            Some(drive) => Disc::scan(drive, &opts),
            None => Err(Error::DeviceNotReady {
                path: self.device.clone(),
            }),
        };
        let disc = match scanned {
            Ok(d) => d,
            Err(e) => {
                restore_key_sources(&mut self.spec, &mut opts, took);
                return Err(e);
            }
        };
        self.disc = Some(disc);
        Ok(self.disc.as_ref().expect("disc just stored"))
    }

    /// [`Self::scan`] under the session's op token (stop design §2.2, §6 ST-L3): the
    /// `ScanOptions.halt` alias rule applies (a token attached by [`Self::open_with`]
    /// wins), and a Stop after the scan's last CDB still ends it `Halted`, storing no
    /// [`Disc`].
    pub fn scan_with(&mut self, opts: ScanOptions) -> Result<&Disc> {
        let took = opts.key_sources.is_empty() && !self.spec.key_sources.is_empty();
        let mut opts = forward_key_material(&mut self.spec, opts);
        // `Disc::scan` applies the alias rule and checks the drive's token last (LS6).
        let scanned = match self.drive.as_mut() {
            Some(drive) => Disc::scan(drive, &opts),
            None => Err(Error::DeviceNotReady {
                path: self.device.clone(),
            }),
        };
        // The session's own final check: a Stop on the op token stores no `Disc`.
        let scanned = scanned.and_then(|d| match &self.halt {
            Some(h) => h.check().map(|()| d),
            None => Ok(d),
        });
        let disc = match scanned {
            Ok(d) => d,
            Err(e) => {
                restore_key_sources(&mut self.spec, &mut opts, took);
                return Err(e);
            }
        };
        self.disc = Some(disc);
        Ok(self.disc.as_ref().expect("disc just stored"))
    }

    /// End the session on the handle it holds (stop design §2.5), never re-opening the
    /// drive: see [`Finish`]. The handle is closed on return. [`Finish::Eject`] runs even
    /// after a Stop: the tray's ALLOW (if this session locked it), then the eject.
    ///
    /// # Errors
    ///
    /// [`Error::DeviceNotReady`] for `Unlock`/`Eject` when the drive has left the session;
    /// the eject's own error.
    pub fn finish(mut self, how: Finish) -> Result<()> {
        let Some(mut drive) = self.drive.take() else {
            return match how {
                Finish::Release => Ok(()),
                Finish::Unlock | Finish::Eject => Err(Error::DeviceNotReady {
                    path: self.device.clone(),
                }),
            };
        };
        match how {
            // `Drive::drop` sends the ALLOW for a tray this Drive locked.
            Finish::Release => Ok(()),
            Finish::Unlock => {
                drive.unlock_tray();
                Ok(())
            }
            Finish::Eject => drive.finish_eject(),
        }
    }

    /// Resolve the rip's key set for `scope` up front (KU §3.1): one
    /// [`ResolvedKeySet::resolve`](crate::keys::ResolvedKeySet::resolve) through the
    /// session's staged reader, else its drive. The session keeps nothing: the set is the
    /// caller's, and no source is retained. Requires [`Self::scan`] to have run.
    pub fn resolve_key_set(
        &mut self,
        scope: crate::keys::KeyScope,
        sources: &KeySourceFactory,
        opts: crate::keys::ResolveKeysOptions,
    ) -> Result<crate::keys::KeyResolution> {
        // Under `open_with` the session's op token governs the resolve too (§2.12).
        let (op, progress) = (self.halt.clone(), self.progress.clone());
        if let Some(h) = &op {
            h.check()?;
        }
        let mut opts = opts;
        if let Some(h) = &op {
            // As `Drive::alias` (§2.2): the session's op token wins over the caller's.
            if opts
                .halt
                .is_some_and(|c| !Arc::ptr_eq(c.as_arc(), h.as_arc()))
            {
                tracing::warn!(
                    target: "freemkv::session",
                    phase = "halt_alias",
                    "ResolveKeysOptions.halt differs from the session's op token; the session token wins"
                );
            }
            opts.halt = Some(h);
        }
        let Some(disc) = self.disc.as_ref() else {
            return Err(Error::DeviceNotReady {
                path: self.device.clone(),
            });
        };
        let reader: &mut dyn SectorSource = match (self.reader.as_mut(), self.drive.as_mut()) {
            (Some(r), _) => r.as_mut(),
            (None, Some(d)) => d,
            (None, None) => {
                return Err(Error::DeviceNotReady {
                    path: self.device.clone(),
                });
            }
        };
        match &progress {
            Some(p) => crate::keys::ResolvedKeySet::resolve_with_progress(
                disc, reader, scope, sources, opts, p,
            ),
            None => crate::keys::ResolvedKeySet::resolve(disc, reader, scope, sources, opts),
        }
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

    /// The opened drive's device path. Cached at [`Self::open`], so it remains
    /// available after [`Self::stage_drive_as_reader`] moves the drive into the
    /// reader slot (the mux driver names the device here without the drive).
    pub fn device_path(&self) -> &str {
        &self.device
    }

    /// Lock the tray so the disc cannot eject mid-rip. Unlock is guaranteed by
    /// `Drive::drop`. A no-op if the drive is no longer held by the session.
    pub fn lock_tray(&mut self) {
        if let Some(drive) = self.drive.as_mut() {
            drive.lock_tray();
        }
    }

    /// Consume the session, returning the owned drive (e.g. to move into a
    /// `DiscStream` for a live-drive mux).
    ///
    /// # Errors
    ///
    /// [`Error::DeviceNotReady`] when the drive is no longer held — reachable through ordinary
    /// use, not just caller error.
    pub fn into_drive(self) -> Result<Drive> {
        self.drive.ok_or_else(|| Error::DeviceNotReady {
            path: self.device.clone(),
        })
    }

    /// Stage the owned drive as the session's boxed sector source so a live
    /// single-pass mux can drive it through
    /// [`MuxSource::Session`](crate::mux::MuxSource::Session). Moves the `Drive`
    /// (itself a [`SectorSource`]) into the `reader` slot; the cached
    /// [`Self::device_path`] keeps the device name available afterward. A no-op
    /// if the drive was already staged or moved out.
    pub fn stage_drive_as_reader(&mut self) {
        if let Some(drive) = self.drive.take() {
            self.reader = Some(Box::new(drive));
        }
    }

    /// Borrow the session's sector source (the staged reader, else the drive) for a
    /// read that must leave the handle in the session, so it can still [`Self::finish`].
    pub fn source_mut(&mut self) -> Option<&mut dyn SectorSource> {
        match (self.reader.as_mut(), self.drive.as_mut()) {
            (Some(r), _) => Some(r.as_mut()),
            (None, Some(d)) => Some(d),
            (None, None) => None,
        }
    }

    /// Consume the session, returning the sector source staged for a later mux
    /// (steps 3–4). `None` until that path populates it.
    pub fn into_reader(self) -> Option<Box<dyn SectorSource>> {
        self.reader
    }

    /// Take the staged sector source out of the session by mutable borrow,
    /// leaving `None` behind. Used by [`crate::mux::mux_with_keys`]'s
    /// [`MuxSource::Session`](crate::mux::MuxSource::Session) arm, which drives
    /// the mux from `&mut DiscSession` and so cannot consume the whole session.
    /// A second call (or a call before the reader is staged) returns `None`, and
    /// the driver maps that to a clean error rather than a panic (see Q2 of the
    /// boundary-audit contract).
    pub fn take_reader(&mut self) -> Option<Box<dyn SectorSource>> {
        self.reader.take()
    }

    // Test-only: build a session over an injected reader + already-scanned disc without opening
    // a live Drive, to exercise the mux test paths.
    #[cfg(test)]
    pub(crate) fn from_parts_for_test(
        disc: Option<Disc>,
        reader: Option<Box<dyn SectorSource>>,
    ) -> DiscSession {
        DiscSession {
            drive: None,
            device: "test://session".to_string(),
            spec: KeySpec::default(),
            disc,
            reader,
            halt: None,
            progress: None,
        }
    }

    // Test-only: build a session that OWNS a (mock-backed) `Drive` so the
    // stage/into_reader/into_drive lifecycle can be exercised (unreachable via
    // `from_parts_for_test`, which holds no drive).
    #[cfg(test)]
    pub(crate) fn from_drive_for_test(drive: Drive) -> DiscSession {
        let device = drive.device_path().to_string();
        DiscSession {
            drive: Some(drive),
            device,
            spec: KeySpec::default(),
            disc: None,
            reader: None,
            halt: None,
            progress: None,
        }
    }
}

/// Scan an ISO image's structure from a file path, returning the scanned
/// [`Disc`] together with a reusable [`SectorSource`] over the same file.
///
/// This is the file-backed counterpart to [`DiscSession::scan`]: it opens a
/// [`FileSectorSource`], reads its capacity, and runs [`Disc::scan_image`].
/// No SCSI, no handshake, no key resolution beyond what `opts` already
/// carries. The returned reader is a fresh handle at the start of the image,
/// reusable by callers that need to sample ciphertext or feed a mux.
pub fn scan_iso(path: &Path, opts: ScanOptions) -> Result<(Disc, Box<dyn SectorSource>)> {
    let mut reader = FileSectorSource::open(path)?;
    let capacity = reader.capacity_sectors();
    let disc = Disc::scan_image(&mut reader, capacity, &opts)?;
    Ok((disc, Box::new(reader)))
}

// Sampled 6144-byte aligned units when judging whether a folder with `AACS/`
// actually holds encrypted content — enough to survive a clear leader clip,
// few enough to stay a handful of reads.
const AACS_PROBE_UNITS: usize = 8;

/// [`Disc`] together with a [`SectorSource`] over a synthesized image of an
/// extracted disc FOLDER — the `dir://` counterpart to [`scan_iso`].
///
/// The extra step over `scan_iso` is the encryption verdict: tree shape alone
/// (an `AACS/` directory) can be wrong for an already-decrypted folder, so
/// content is sampled and judged by `aacs_unit_needs_decrypt`:
///
/// * none need decryption → `encrypted` is forced false, reason logged.
/// * any unit does → [`Error::DirImageEncrypted`] (`dir://` doesn't support it).
pub fn scan_dir(path: &Path, opts: ScanOptions) -> Result<(Disc, Box<dyn SectorSource>)> {
    let mut reader = crate::dirimage::DirImage::open(path)?;
    let capacity = reader.capacity_sectors();
    let mut disc = Disc::scan_image(&mut reader, capacity, &opts)?;

    apply_folder_encryption_verdict(&mut reader, &mut disc)?;
    Ok((disc, Box::new(reader)))
}

// Re-judge a FOLDER's encryption verdict from its CONTENT (tree shape alone can be wrong for an
// already-decrypted folder that kept `AACS/`). Shared by scan_dir and the dir:// path in
// mux::resolve.
pub(crate) fn apply_folder_encryption_verdict(
    reader: &mut dyn SectorSource,
    disc: &mut Disc,
) -> Result<()> {
    // `css.is_some()` is the DVD path, and that verdict came from actually
    // cracking scrambled sectors — real evidence about content, not tree shape.
    // Only the AACS-by-tree-shape verdict is re-judged here.
    if disc.encrypted && disc.css.is_none() && disc.css_error.is_none() {
        match probe_folder_encryption(reader, disc)? {
            true => return Err(Error::DirImageEncrypted),
            false => {
                tracing::warn!(
                    target: "freemkv::scan",
                    phase = "folder_verdict",
                    "folder carries an AACS directory but its sampled content units \
                     are already in the clear; treating it as decrypted"
                );
                disc.encrypted = false;
                disc.aacs = None;
                disc.aacs_error = None;
            }
        }
    }
    Ok(())
}

// `true` when any sampled content unit still needs decryption. Anchored at
// the largest title's first extent, because AACS unit alignment is measured
// from the clip FILE's start, not an absolute `lba % 3`.
fn probe_folder_encryption(reader: &mut dyn SectorSource, disc: &Disc) -> Result<bool> {
    use crate::aacs::content::{aacs_unit_needs_decrypt, is_unit_aligned};
    use crate::consts::SECTOR_BYTES;

    const UNIT_SECTORS: u32 = crate::aacs::content::ALIGNED_UNIT_SECTORS;
    // Anchor on the largest TITLE's FIRST extent (video preferred, skipping an
    // obfuscated decoy), never the largest extent anywhere: AACS units are 3 sectors,
    // aligned only at a clip's START — misalignment risks a false clean/encrypted verdict.
    let Some(extent) = disc.main_title().and_then(|t| t.extents.first()) else {
        // No content to judge. A folder with an AACS directory and no titles
        // has nothing to rip either way; leave the structural verdict alone.
        return Ok(true);
    };
    let base = extent.start_lba;
    let mut unit = vec![0u8; UNIT_SECTORS as usize * SECTOR_BYTES];
    let mut sampled = 0u32;
    for i in 0..AACS_PROBE_UNITS as u32 {
        // Saturating: `start_lba` and `sector_count` come off the medium, and a
        // crafted or corrupt extent must not wrap this bound into a read past
        // the end of the content.
        let Some(lba) = base.checked_add(i.saturating_mul(UNIT_SECTORS)) else {
            break;
        };
        let end = base.saturating_add(extent.sector_count);
        if lba.saturating_add(UNIT_SECTORS) > end {
            break;
        }
        debug_assert!(is_unit_aligned(lba, base));
        reader.read_sectors(lba, UNIT_SECTORS as u16, &mut unit, false)?;
        sampled += 1;
        if aacs_unit_needs_decrypt(&unit, disc.content_format) {
            return Ok(true);
        }
    }
    // Nothing was sampled (title shorter than one aligned unit) — no evidence either
    // way. "Not encrypted" is the dangerous default: it would clear an `AACS`-directory
    // verdict and rip ciphertext as video at exit 0. Keep the structural verdict instead.
    if sampled == 0 {
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod stop_tests;

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
}

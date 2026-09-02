//! Disc session — one place that opens an optical drive and brings the SCSI
//! transport up, so the consumers (CLI, autorip) stop hand-rolling the
//! `open → wait_ready → init → probe_disc → identify → scan` preamble.
//!
//! The session owns the [`Drive`] by value (tray unlock stays guaranteed via
//! `Drive::drop`) and, after [`DiscSession::scan`], the resulting [`Disc`].
//! libfreemkv resolves no keys and reads no keydb: the consumer supplies its
//! own key material via [`KeySpec`], forwarded to [`ScanOptions`] at scan time.
//!
//! See docs/session.md for the lifecycle-split rationale and key-forwarding contract.

use crate::aacs::trace::ResolutionTrace;
use crate::disc::{Disc, DiscId, DriveCredentials, ScanOptions};
use crate::drive::{Drive, find_drive};
use crate::error::{Error, Result};
use crate::keysource::{
    KeySource, MIN_SAMPLE_UNITS, key_fetch, read_encrypted_units, resolve_and_apply_traced,
};
use crate::sector::{FileSectorSource, KeyFetch, SectorSource};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A consumer-supplied factory for the ordered AACS key-source layer.
///
/// libfreemkv builds no key sources itself (the `freemkv_keysources` crate that
/// implements [`KeySource`] depends on libfreemkv, not the other way round), so
/// the consumer hands in a way to (re)build its sources. It is invoked once for
/// the up-front resolve and again per on-decrypt-miss fetch (the cold path), so
/// it stays `Send + Sync` without requiring `KeySource: Send`. Mirrors the
/// `make_sources` argument [`key_fetch`] already takes.
pub type KeySourceFactory = Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync>;

/// The outcome of resolving a disc's base AACS unit keys: the structured
/// per-source [`ResolutionTrace`] (for the consumer to render) plus the
/// read-time [`KeyFetch`] built from the disc's public inputs.
///
/// `key_fetch` is `None` only for a disc that carries no AACS inputs (an
/// unencrypted / CSS / non-AACS disc); it is `Some` whenever the disc is AACS,
/// independent of whether a key actually resolved — the on-decrypt-miss fetch is
/// wired the same way regardless.
pub struct ResolvedKeys {
    /// Per-source walk of the resolve, for the consumer to render (English-free
    /// typed enums only; the app layer maps them to text).
    pub trace: ResolutionTrace,
    /// The read-time fetch closure, or `None` for a non-AACS disc.
    pub key_fetch: Option<KeyFetch>,
}

/// Resolve and bank a keyless-scanned disc's BASE AACS unit keys, and build the
/// read-time [`KeyFetch`] — the one place the sampling / ordered-apply / banking
/// / fetch-construction glue lives, so the CLI and autorip stop hand-rolling it.
///
/// Samples ciphertext from the disc via `reader` (a live [`Drive`] or a
/// file-backed [`SectorSource`] from [`scan_iso`]), runs the ordered `sources`
/// first-valid-wins, banks any winning unit keys onto `disc`, and builds the
/// read-time [`KeyFetch`] used for later on-decrypt-miss fetches.
///
/// See docs/session.md for the exact step-by-step breakdown.
pub fn resolve_keys_for(
    reader: &mut dyn SectorSource,
    disc: &mut Disc,
    sources: KeySourceFactory,
) -> ResolvedKeys {
    // A disc with no captured AACS inputs is unencrypted / CSS / non-AACS —
    // nothing to resolve, nothing to fetch.
    let Some(mut inputs) = disc.inputs() else {
        return ResolvedKeys {
            trace: ResolutionTrace::new(),
            key_fetch: None,
        };
    };

    // Build the ordered sources once for the up-front resolve. Skip the disc-reading
    // sample step when there is no source to validate against (a dropped / SSRF-
    // rejected online-only source) — resolution is a miss regardless; the read is waste.
    let src_vec = sources();
    inputs.samples = if src_vec.is_empty() {
        Vec::new()
    } else {
        // Sample from the LARGEST title that HAS video — the main feature carries the
        // most validation units, avoiding a size-inflated STREAMLESS decoy on some
        // obfuscated UHDs. Falls back to largest title outright when none has video.
        match disc
            .titles
            .iter()
            .filter(|t| t.has_probable_video())
            .max_by_key(|t| t.size_bytes)
            .or_else(|| disc.titles.iter().max_by_key(|t| t.size_bytes))
            .cloned()
        {
            Some(title) => read_encrypted_units(reader, &title, MIN_SAMPLE_UNITS),
            None => Vec::new(),
        }
    };

    // Ordered, first-valid-wins; banks the winning unit keys onto `disc`.
    let (_resolved, trace) = resolve_and_apply_traced(&src_vec, &inputs, disc);

    // Build the read-time fetch from the disc's public inputs (fresh, so it
    // reflects any banked state); its per-fetch `samples` are filled by the
    // closure. `inputs()` is still `Some` here (the disc is AACS).
    let fetch_inputs = disc.inputs().unwrap_or(inputs);
    let fetch = key_fetch(fetch_inputs, sources);
    ResolvedKeys {
        trace,
        key_fetch: Some(fetch),
    }
}

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
    /// The read-time AACS fetch closure, built by [`Self::resolve_keys`] and
    /// retained so a later mux (step 4) can install it into the decrypt
    /// decorator. `None` until keys are resolved / for a non-AACS disc.
    key_fetch: Option<KeyFetch>,
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

        // Advisory bring-up — non-fatal in every consumer today. Preserve that: log
        // and continue, never propagate (CLI discarded these, autorip warned) — the
        // advisory semantics are preserved identically; only the sink moved here.
        if let Err(e) = drive.wait_ready() {
            tracing::warn!(target: "freemkv::session", error = %e, "wait_ready advisory failed (continuing)");
        }
        if let Err(e) = drive.init() {
            tracing::warn!(target: "freemkv::session", error = %e, "init advisory failed (continuing)");
        }
        if let Err(e) = drive.probe_disc() {
            tracing::warn!(target: "freemkv::session", error = %e, "probe_disc advisory failed (continuing)");
        }

        let device = drive.device_path().to_string();
        Ok(DiscSession {
            drive: Some(drive),
            device,
            spec,
            disc: None,
            reader: None,
            key_fetch: None,
        })
    }

    /// Fast disc identification — name/format only, no playlist parse. Wraps
    /// [`Disc::identify`].
    pub fn identify(&mut self) -> Result<DiscId> {
        // Same reachability as `scan`/`resolve_keys`: public `stage_drive_as_reader`/
        // `into_drive` move the drive out, so this slot can legitimately be empty.
        // A library must not panic from public API, so return typed `DeviceNotReady`.
        let drive = self.drive.as_mut().ok_or_else(|| Error::DeviceNotReady {
            path: self.device.clone(),
        })?;
        Disc::identify(drive)
    }

    /// Full structure scan. Forwards the session's [`KeySpec`] credentials /
    /// key-sources into `opts` (without clobbering anything the caller already
    /// set), runs [`Disc::scan`], stores the result, and returns a borrow.
    pub fn scan(&mut self, opts: ScanOptions) -> Result<&Disc> {
        let opts = forward_key_material(&mut self.spec, opts);
        // `stage_drive_as_reader` is PUBLIC and moves the drive out, so this slot can
        // legitimately be empty here. "No shipped consumer calls it in that order" is
        // not "cannot happen" — the public surface permits it, so it must be an error.
        let drive = self.drive.as_mut().ok_or_else(|| Error::DeviceNotReady {
            path: self.device.clone(),
        })?;
        let disc = Disc::scan(drive, &opts)?;
        self.disc = Some(disc);
        Ok(self.disc.as_ref().expect("disc just stored"))
    }

    /// Resolve and bank the scanned disc's base AACS unit keys from the
    /// consumer-supplied `sources`, and retain the read-time [`KeyFetch`] on the
    /// session (see [`Self::key_fetch`]) for a later mux.
    ///
    /// Samples ciphertext through the session's own reader — the staged file
    /// reader if one is present, otherwise the live drive — so it works for both
    /// a live-drive session and a file-backed one. Returns the structured
    /// [`ResolutionTrace`] for the consumer to render; a non-AACS disc resolves
    /// to an empty trace with no error. Requires [`Self::scan`] to have run.
    pub fn resolve_keys(&mut self, sources: KeySourceFactory) -> Result<ResolutionTrace> {
        // The disc must have been scanned so its AACS inputs are captured.
        if self.disc.is_none() {
            return Err(Error::DeviceNotReady {
                path: self.device.clone(),
            });
        }
        // Sample through the staged reader when present (file-backed), else the
        // live drive. `self.reader` / `self.disc` / `self.drive` are disjoint
        // fields, so the borrows below don't conflict.
        let resolved = if let Some(reader) = self.reader.as_mut() {
            let disc = self.disc.as_mut().expect("disc present (checked above)");
            resolve_keys_for(reader.as_mut(), disc, sources)
        } else {
            // Same reachability as `scan` above: the drive may have been staged
            // into the reader slot by the public `stage_drive_as_reader`.
            let drive = self.drive.as_mut().ok_or_else(|| Error::DeviceNotReady {
                path: self.device.clone(),
            })?;
            let disc = self.disc.as_mut().expect("disc present (checked above)");
            resolve_keys_for(drive, disc, sources)
        };
        self.key_fetch = resolved.key_fetch;
        Ok(resolved.trace)
    }

    /// The read-time AACS fetch closure retained by [`Self::resolve_keys`], for a
    /// later mux (step 4) to install into the decrypt decorator. `None` before
    /// keys are resolved, or for a non-AACS disc.
    pub fn key_fetch(&self) -> Option<&KeyFetch> {
        self.key_fetch.as_ref()
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
    /// [`Error::DeviceNotReady`] when the drive is no longer held — reachable
    /// through ordinary use (see docs/session.md), not just caller error.
    pub fn into_drive(self) -> Result<Drive> {
        self.drive.ok_or_else(|| Error::DeviceNotReady {
            path: self.device.clone(),
        })
    }

    /// Stage the owned drive as the session's boxed sector source so a live
    /// single-pass mux can drive it through
    /// [`MuxInput::Session`](crate::mux::MuxInput::Session). Moves the `Drive`
    /// (itself a [`SectorSource`]) into the `reader` slot; the cached
    /// [`Self::device_path`] keeps the device name available afterward. A no-op
    /// if the drive was already staged or moved out.
    pub fn stage_drive_as_reader(&mut self) {
        if let Some(drive) = self.drive.take() {
            self.reader = Some(Box::new(drive));
        }
    }

    /// Consume the session, returning the sector source staged for a later mux
    /// (steps 3–4). `None` until that path populates it.
    pub fn into_reader(self) -> Option<Box<dyn SectorSource>> {
        self.reader
    }

    /// Take the staged sector source out of the session by mutable borrow,
    /// leaving `None` behind. Used by [`crate::mux::mux_stream`]'s
    /// [`MuxInput::Session`](crate::mux::MuxInput::Session) arm, which drives
    /// the mux from `&mut DiscSession` and so cannot consume the whole session.
    /// A second call (or a call before the reader is staged) returns `None`, and
    /// the driver maps that to a clean error rather than a panic (see Q2 of the
    /// boundary-audit contract).
    pub fn take_reader(&mut self) -> Option<Box<dyn SectorSource>> {
        self.reader.take()
    }

    // Test-only: build a session over an injected reader + already-scanned disc
    // without opening a live Drive, to exercise the mux/resolve_keys test paths.
    // See docs/session.md for the full rationale.
    #[cfg(test)]
    pub(crate) fn from_parts_for_test(
        disc: Option<Disc>,
        reader: Option<Box<dyn SectorSource>>,
        key_fetch: Option<KeyFetch>,
    ) -> DiscSession {
        DiscSession {
            drive: None,
            device: "test://session".to_string(),
            spec: KeySpec::default(),
            disc,
            reader,
            key_fetch,
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
///
/// See docs/session.md for the full rationale.
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
///
/// See docs/session.md for the full rationale.
pub fn scan_dir(path: &Path, opts: ScanOptions) -> Result<(Disc, Box<dyn SectorSource>)> {
    let mut reader = crate::dirimage::DirImage::open(path)?;
    let capacity = reader.capacity_sectors();
    let mut disc = Disc::scan_image(&mut reader, capacity, &opts)?;

    apply_folder_encryption_verdict(&mut reader, &mut disc)?;
    Ok((disc, Box::new(reader)))
}

// Re-judge a FOLDER's encryption verdict from its CONTENT (tree shape alone
// can be wrong for an already-decrypted folder that kept `AACS/`). Shared by
// scan_dir and the dir:// path in mux::resolve; see docs/session.md.
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

    const UNIT_SECTORS: u32 = 3;
    // Anchor on the largest TITLE's FIRST extent (video preferred, skipping an
    // obfuscated decoy), never the largest extent anywhere: AACS units are 3 sectors,
    // aligned only at a clip's START — misalignment risks a false clean/encrypted verdict.
    let Some(extent) = disc
        .titles
        .iter()
        .filter(|t| t.has_probable_video())
        .max_by_key(|t| {
            t.extents
                .iter()
                .fold(0u64, |a, e| a.saturating_add(e.sector_count as u64))
        })
        .or_else(|| {
            disc.titles.iter().max_by_key(|t| {
                t.extents
                    .iter()
                    .fold(0u64, |a, e| a.saturating_add(e.sector_count as u64))
            })
        })
        .and_then(|t| t.extents.first())
    else {
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

    // ── resolve_keys_for: sampling → ordered apply → bank → fetch ─────────────

    /// A no-op reader — the resolve tests use discs with no titles, so no
    /// sampling read fires; this satisfies the `&mut dyn SectorSource` seam.
    struct NullReader;
    impl SectorSource for NullReader {
        fn capacity_sectors(&self) -> u32 {
            0
        }
        fn read_sectors(&mut self, _: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            Ok(0)
        }
    }

    /// A source that hands back one terminal Unit Key.
    struct HasUnitKey([u8; 16]);
    impl KeySource for HasUnitKey {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
            Ok(vec![UnitKey::new(0, self.0)])
        }
        fn label(&self) -> &'static str {
            "has-key"
        }
    }

    /// A source with no key for this disc.
    struct NoUnitKey;
    impl KeySource for NoUnitKey {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>> {
            Ok(Vec::new())
        }
        fn label(&self) -> &'static str {
            "empty"
        }
    }

    /// A minimal keyless AACS `Disc` — `inputs()` returns `Some`, so
    /// `resolve_keys_for` proceeds to the sources. No titles (no sampling read).
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
                key_source: crate::disc::KeyOrigin::KeyDb,
                vuk: None,
                unit_keys: Vec::new(),
                read_data_key: None,
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

    fn factory_of<S: KeySource + 'static>(make: fn() -> S) -> KeySourceFactory {
        Arc::new(move || vec![Box::new(make()) as Box<dyn KeySource>])
    }

    // The happy path: a source's Unit Key is banked onto the disc's AACS state
    // and a `KeyFetch` is retained. Guards against dropping the banking step
    // (`resolve_and_apply_traced`), which would leave `decrypt_keys()` `None`.
    #[test]
    fn resolve_keys_for_banks_unit_key_and_builds_fetch() {
        use crate::decrypt::DecryptKeys;
        const K: [u8; 16] = [0x5A; 16];
        let mut disc = aacs_disc();
        let mut reader = NullReader;

        let resolved = resolve_keys_for(&mut reader, &mut disc, factory_of(|| HasUnitKey(K)));

        match disc.decrypt_keys() {
            DecryptKeys::Aacs { unit_keys, .. } => {
                // CPS-unit number is positional index + 1 (idx 0 → unit 1).
                assert_eq!(unit_keys, vec![(1u32, K)], "the source's key is banked");
            }
            _ => panic!("expected banked AACS keys"),
        }
        assert!(
            resolved.key_fetch.is_some(),
            "an AACS disc always retains a read-time fetch"
        );
        // The trace recorded exactly one source, which resolved.
        assert_eq!(resolved.trace.keys.len(), 1);
    }

    /// A source with no key: nothing is banked (`decrypt_keys()` stays `None`),
    /// but a `KeyFetch` is STILL built (the on-decrypt-miss path is wired
    /// regardless of the up-front resolve succeeding).
    #[test]
    fn resolve_keys_for_no_key_leaves_disc_unkeyed_but_builds_fetch() {
        use crate::decrypt::DecryptKeys;
        let mut disc = aacs_disc();
        let mut reader = NullReader;

        let resolved = resolve_keys_for(&mut reader, &mut disc, factory_of(|| NoUnitKey));

        assert!(
            matches!(disc.decrypt_keys(), DecryptKeys::None),
            "no source key ⇒ disc stays unkeyed"
        );
        assert!(
            resolved.key_fetch.is_some(),
            "an AACS disc retains a fetch even when the up-front resolve misses"
        );
    }

    /// A counting reader over zeros — records the highest LBA sampled so the test
    /// can prove the LARGEST title's extent (not the small one) was read.
    struct SamplingReader {
        reads: u32,
        max_lba: u32,
    }
    impl SectorSource for SamplingReader {
        fn capacity_sectors(&self) -> u32 {
            100_000
        }
        fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
            self.reads += 1;
            self.max_lba = self.max_lba.max(lba);
            let want = count as usize * 2048;
            buf[..want].fill(0);
            Ok(want)
        }
    }

    // `resolve_keys_for` samples the LARGEST title's ciphertext through the
    // reader when a source is configured. Other tests use a title-less disc
    // (no sampling read); this one covers the sampling branch with two titles.
    #[test]
    fn resolve_keys_for_samples_largest_title_through_reader() {
        use crate::disc::{DiscTitle, Extent};
        let mut disc = aacs_disc();
        let mut small = DiscTitle::empty();
        small.size_bytes = 1_000;
        small.extents = vec![Extent {
            start_lba: 100,
            sector_count: 300,
        }];
        let mut large = DiscTitle::empty();
        large.size_bytes = 9_000_000;
        large.extents = vec![Extent {
            start_lba: 9_000,
            sector_count: 300,
        }];
        disc.titles = vec![small, large];
        let mut reader = SamplingReader {
            reads: 0,
            max_lba: 0,
        };

        // Non-empty source ⇒ the sampling read is NOT skipped.
        let resolved = resolve_keys_for(&mut reader, &mut disc, factory_of(|| HasUnitKey([1; 16])));

        assert!(
            reader.reads > 0,
            "the largest title was sampled via the reader"
        );
        assert!(
            reader.max_lba >= 9_000,
            "sampling read the LARGER title's extent (lba>=9000), not the small one \
             (max_lba={})",
            reader.max_lba
        );
        assert!(
            resolved.key_fetch.is_some(),
            "an AACS disc still retains a read-time fetch"
        );
    }

    /// A non-AACS disc (CSS / unencrypted — `inputs()` is `None`): resolution is a
    /// no-op. Empty trace, NO fetch, disc untouched. This is the out-of-the-box
    /// CSS/None path that must keep working with no keydb.
    #[test]
    fn resolve_keys_for_non_aacs_disc_is_a_noop() {
        use crate::decrypt::DecryptKeys;
        let mut disc = aacs_disc();
        disc.aacs = None; // now carries no AACS inputs
        disc.encrypted = false;
        let mut reader = NullReader;

        let resolved = resolve_keys_for(&mut reader, &mut disc, factory_of(|| HasUnitKey([1; 16])));

        assert!(
            resolved.trace.keys.is_empty() && resolved.trace.unlock.is_empty(),
            "a non-AACS disc yields an empty trace"
        );
        assert!(
            resolved.key_fetch.is_none(),
            "a non-AACS disc has nothing to fetch"
        );
        assert!(
            matches!(disc.decrypt_keys(), DecryptKeys::None),
            "the disc is left untouched"
        );
    }

    // `resolve_keys` called before `scan` (disc slot still `None`) must return
    // the typed `DeviceNotReady` guard, never reach the `.expect(...)` below it
    // and panic. Guards against dropping the `if self.disc.is_none()` early return.
    #[test]
    fn resolve_keys_before_scan_is_clean_device_not_ready() {
        let mut session = DiscSession::from_parts_for_test(None, None, None);
        let err = session
            .resolve_keys(factory_of(|| HasUnitKey([1; 16])))
            .expect_err("resolve_keys before scan must error, not panic");
        assert!(
            matches!(err, Error::DeviceNotReady { .. }),
            "expected DeviceNotReady, got {err:?}"
        );
    }

    // `identify` after the drive has left the session (both `stage_drive_as_reader`
    // and `into_drive` permit that ordering) must return `DeviceNotReady`, not
    // reach `drive_mut`'s `.expect(...)` and panic. Sibling of scan/resolve_keys.
    #[test]
    fn identify_without_a_drive_is_clean_device_not_ready() {
        let mut session = DiscSession::from_parts_for_test(None, None, None);
        let err = session
            .identify()
            .expect_err("identify without a drive must error, not panic");
        assert!(
            matches!(err, Error::DeviceNotReady { .. }),
            "expected DeviceNotReady, got {err:?}"
        );
    }
}

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
/// Steps, identical to what the consumers did inline:
/// 1. Take the disc's public AACS inputs ([`Disc::inputs`]); a non-AACS disc has
///    none, so this is a no-op returning an empty trace and no fetch.
/// 2. Sample up to [`MIN_SAMPLE_UNITS`] encrypted content units from the LARGEST
///    title via `reader` ([`read_encrypted_units`]) so a candidate key is
///    validated against real ciphertext. Skipped (no wasted read) when the
///    factory yields no sources — resolution is then a guaranteed miss anyway.
/// 3. Run the ordered sources first-valid-wins ([`resolve_and_apply_traced`]),
///    which banks the winning unit keys onto `disc`'s AACS state.
/// 4. Build the read-time [`KeyFetch`] from the disc's inputs (its per-call
///    samples are swapped in by the closure) using the same source factory.
///
/// The `reader` is whatever the disc lives behind — a live [`Drive`] or a
/// file-backed [`SectorSource`] from [`scan_iso`]; both implement
/// [`SectorSource`].
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

    // Build the ordered sources once for the up-front resolve. Sampling reads the
    // disc, so skip it when there is no source to validate against (a dropped /
    // SSRF-rejected online-only source) — resolution is a miss regardless and the
    // read would be pure waste.
    let src_vec = sources();
    inputs.samples = if src_vec.is_empty() {
        Vec::new()
    } else {
        match disc.titles.iter().max_by_key(|t| t.size_bytes).cloned() {
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
/// `DiscStream`) reach it via [`Self::drive_mut`] / [`Self::into_drive`]; the
/// scanned [`Disc`] comes out via [`Self::disc`] / [`Self::take_disc`].
pub struct DiscSession {
    drive: Drive,
    spec: KeySpec,
    disc: Option<Disc>,
    /// Sector source for a later file/live mux to `.take()` (steps 3–4).
    /// Unpopulated in the current step; shapes the struct for the mux hoist.
    reader: Option<Box<dyn SectorSource>>,
    /// The read-time AACS fetch closure, built by [`Self::resolve_keys`] and
    /// retained so a later mux (step 4) can install it into the decrypt
    /// decorator. `None` until keys are resolved / for a non-AACS disc.
    key_fetch: Option<KeyFetch>,
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
            key_fetch: None,
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
                path: self.drive.device_path().to_string(),
            });
        }
        // Sample through the staged reader when present (file-backed), else the
        // live drive. `self.reader` / `self.disc` / `self.drive` are disjoint
        // fields, so the borrows below don't conflict.
        let resolved = if let Some(reader) = self.reader.as_mut() {
            let disc = self.disc.as_mut().expect("disc present (checked above)");
            resolve_keys_for(reader.as_mut(), disc, sources)
        } else {
            let disc = self.disc.as_mut().expect("disc present (checked above)");
            resolve_keys_for(&mut self.drive, disc, sources)
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

    /// The happy path: a source's Unit Key is BANKED onto the disc's AACS state
    /// (so `decrypt_keys()` now yields it) and a `KeyFetch` is retained.
    ///
    /// Mutation guard: if the banking step (`resolve_and_apply_traced`) is
    /// dropped, `decrypt_keys()` stays `None` and this assertion fails.
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
}

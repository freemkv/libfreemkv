//! Keys up front, memory only (keys-upfront design, KU).
//!
//! [`ResolvedKeySet::resolve`] is the one place that asks a [`crate::KeySource`] for keys.
//! It runs once per rip, before any output, over the rip's [`KeyScope`]. It proves each
//! held key against real ciphertext of each stream file (a *piece*), counting CPS units by
//! the DECLARED `Num_of_CPS_Unit` (KS-14), never by the keys held. The result is an
//! immutable, in-memory set: keys and the disc's Volume ID live in it for this rip only,
//! behind a redacting `Debug`, with no byte or VID accessor and no `Serialize`.
//!
//! A piece that could not be proven up front is *Lazy*: its readers prove a held key on
//! it the first time it is read (the on-arrival proof), looking at
//! neighbouring units both ways, then falling back to a provisional one-unit proof. A
//! readable unit is never withheld; one that no held key opens is a loud stop.

mod arrival;
mod fmts;
#[cfg(doctest)]
mod public_api_cannot_decrypt_aacs;
mod resolve;
#[cfg(test)]
mod tests;

pub(crate) use arrival::Arrival;
#[cfg(test)]
pub(crate) use resolve::whole_disc_pieces;

use crate::aacs::trace::ResolutionTrace;
use crate::decrypt::{AacsKeyMap, DecryptKeys};
use crate::disc::{ContentFormat, Disc, DiscFormat};
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::{DecryptingSectorSource, SectorSource};
use crate::session::KeySourceFactory;
use crate::whole_disc::{UnitSpan, WholeDiscReader};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// A title's `DiscStream` read through `map`: the read plan, then each unit's mapped key.
pub(crate) fn install_key_map(
    mut stream: crate::mux::DiscStream,
    map: Arc<AacsKeyMap>,
) -> crate::mux::DiscStream {
    stream.plan_reads(&map);
    stream.reader_mut().set_key_map(map);
    stream
}

// A test's own phased map; map construction stays in `keys` (KU §2.2).
#[cfg(test)]
pub(crate) fn test_key_map(
    ranges: Vec<(u32, u32, usize, crate::decrypt::Phase)>,
) -> Arc<AacsKeyMap> {
    Arc::new(AacsKeyMap::from_ranges_phased(ranges))
}

// `src` reading through a test's map, for the tests outside `keys`.
#[cfg(test)]
pub(crate) fn test_keyed_source<S: SectorSource>(
    src: DecryptingSectorSource<S>,
    map: Arc<AacsKeyMap>,
) -> DecryptingSectorSource<S> {
    src.with_key_map(map)
}

/// What a rip decrypts, and so what `resolve` must key (KU §2.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyScope {
    /// Nothing is decrypted (a raw copy): no key-source call at all.
    None,
    /// These titles (indices into `Disc::titles`): an MKV/M2TS/MP4/demux rip.
    Titles(Vec<usize>),
    /// Every stream file: a decrypted ISO or folder.
    WholeDisc,
}

/// Options for [`ResolvedKeySet::resolve`].
#[derive(Default)]
pub struct ResolveKeysOptions<'a> {
    /// Stop token: checked before each piece, probe and request; interrupts retry waits.
    pub halt: Option<&'a Halt>,
    /// A set this rip already holds (e.g. from `info` or the scan): its keys join the pool
    /// first, and its in-memory VID is used when the disc has none (KU §5.4).
    pub seed: Option<&'a ResolvedKeySet>,
    /// An in-memory VID the caller re-read from the drive (KU §4.2). Never written.
    pub vid: Option<[u8; 16]>,
    /// Set by `resolve` when no VID was in hand and one would help (KU J23): a source
    /// reported a Media Key it could not finish without the VID (`KeyNode::NoVid`), or a
    /// configured source [`uses_vid`](crate::KeySource::uses_vid). Never cleared.
    pub vid_would_help: Option<&'a std::sync::atomic::AtomicBool>,
    /// Filled with the per-source walk on `Ok` and on `Err` alike, so a refusal still says
    /// which source answered what. It holds labels, node enums and counts, never key bytes.
    pub trace: Option<&'a Mutex<ResolutionTrace>>,
}

/// The outcome of [`ResolvedKeySet::resolve`]: the set, plus the per-source walk.
pub struct KeyResolution {
    pub keys: ResolvedKeySet,
    pub trace: ResolutionTrace,
}

/// Which held key opened a piece on arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Proof {
    /// The key opened the piece's unit and a partner unit.
    Proven(usize),
    /// The key opened one unit with no readable partner; the next readable unit confirms it.
    Provisional(usize),
}

/// The rip's in-memory record of on-arrival proofs (KU §2.4, J2, J16): which HELD key
/// opened a piece. It never adds a key and is never written. The set owns one, shared by
/// every clone of the set and handed to each reader the set builds, so later passes and
/// the mux reuse a proof instead of side-reading again.
#[derive(Clone, Default)]
pub struct ProofCache(Arc<Mutex<HashMap<u32, Proof>>>);

impl ProofCache {
    /// Pieces with a recorded proof.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// No proof recorded yet.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Proof>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn get(&self, piece: u32) -> Option<Proof> {
        self.lock().get(&piece).copied()
    }

    pub(crate) fn set(&self, piece: u32, proof: Proof) {
        self.lock().insert(piece, proof);
    }
}

impl std::fmt::Debug for ProofCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProofCache")
            .field("pieces", &self.len())
            .finish()
    }
}

/// The forensic (AACS 2.1 FMTS) state of a set (KU §5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForensicState {
    /// No forensic clip in scope.
    None,
    /// The index keys and each index's phase are held.
    Resolved,
    /// Every index-1 anchor read faulted: the forensic keys are asked for later, once, from
    /// the recovered image (KU §5.4).
    Pending,
}

/// A set's shape, for the front end's status line and the qa log (KU §7.6). No key bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeySetStatus {
    /// Distinct held keys proven on a piece.
    pub proven: usize,
    /// `Num_of_CPS_Unit` of the disc's title-key file (KS-14); `None` if unparseable.
    pub declared: Option<usize>,
    /// Pieces keyed by a proven key.
    pub keyed: usize,
    /// Pieces whose every probe read clear.
    pub clear: usize,
    /// Pieces left to the on-arrival proof.
    pub lazy: usize,
    pub forensic: ForensicState,
    /// The `label()` of the source whose key keyed the first keyed piece (`"seed"` for a
    /// key the seed set held).
    pub origin: Option<&'static str>,
    /// HD DVD: the single declared key was applied without proof (KU §2.6).
    pub best_effort: bool,
    /// Key-source requests `resolve` made (retries of one request count once).
    pub requests: u32,
}

/// Whether a disc can be decrypted with a set (KU §3.5).
#[derive(Debug)]
pub enum DecryptStatus {
    NotEncrypted,
    Ready,
    AacsKeysMissing(Error),
    CssNotCracked(Error),
    ForensicPending,
}

// Which loud stop the on-arrival proof raises: a title rip (E7022) or an image or folder
// (E7032), KU §6.
#[derive(Clone, Debug)]
pub(crate) enum StopKind {
    Title { disc_hash: String },
    Image,
}

impl StopKind {
    pub(crate) fn error(&self) -> Error {
        match self {
            StopKind::Title { disc_hash } => Error::NoDiscKey {
                disc_hash: disc_hash.clone(),
            },
            StopKind::Image => Error::WholeDiscKeyMissing,
        }
    }
}

// A piece the readers prove on arrival: its id (first sector), its unit grid, and the key
// to try first (KU §2.3 step 9.3, J21).
#[derive(Clone, Debug)]
pub(crate) struct ArrivalPiece {
    pub(crate) id: u32,
    pub(crate) spans: Vec<UnitSpan>,
    pub(crate) candidate: Option<usize>,
}

// Everything a set holds. Key bytes and the VID never leave the crate.
pub(crate) struct Inner {
    pub(crate) aacs: bool,
    pub(crate) disc_hash: String,
    pub(crate) capacity: u32,
    pub(crate) format: DiscFormat,
    pub(crate) content_format: ContentFormat,
    pub(crate) vid: Option<[u8; 16]>,
    pub(crate) n_decl: Option<usize>,
    pub(crate) scope: KeyScope,
    // Base keys (pool slot = index); forensic index keys follow them in `decrypt_keys`.
    pub(crate) pool: Vec<[u8; 16]>,
    pub(crate) fmts_keys: Vec<[u8; 16]>,
    pub(crate) fmts_phases: HashMap<u16, crate::decrypt::Phase>,
    pub(crate) map: Arc<AacsKeyMap>,
    pub(crate) spans: Vec<UnitSpan>,
    // `resolve` found no stream file (no `/BDMV/STREAM` or `/HVDVD_TS`, or an empty one).
    pub(crate) no_stream_files: bool,
    // A Lazy piece has intact encrypted probes and no held key opens any (KU §2.3 step 9.2).
    pub(crate) lazy_unopened: bool,
    pub(crate) arrival: Vec<ArrivalPiece>,
    pub(crate) lazy: Vec<(u32, u32)>,
    pub(crate) proven: Vec<usize>,
    pub(crate) keyed: usize,
    pub(crate) clear: usize,
    pub(crate) forensic: ForensicState,
    pub(crate) forensic_clip: Vec<(u32, u32)>,
    pub(crate) segments: Vec<(u32, u32)>,
    pub(crate) requests: u32,
    pub(crate) best_effort: bool,
    pub(crate) origin: Option<&'static str>,
    pub(crate) proofs: ProofCache,
}

impl Inner {
    fn empty() -> Self {
        Inner {
            aacs: false,
            disc_hash: String::new(),
            capacity: 0,
            format: DiscFormat::BluRay,
            content_format: ContentFormat::BdTs,
            vid: None,
            n_decl: None,
            scope: KeyScope::None,
            pool: Vec::new(),
            fmts_keys: Vec::new(),
            fmts_phases: HashMap::new(),
            map: Arc::new(AacsKeyMap::from_ranges(Vec::new())),
            spans: Vec::new(),
            no_stream_files: false,
            lazy_unopened: false,
            arrival: Vec::new(),
            lazy: Vec::new(),
            proven: Vec::new(),
            keyed: 0,
            clear: 0,
            forensic: ForensicState::None,
            forensic_clip: Vec::new(),
            segments: Vec::new(),
            requests: 0,
            best_effort: false,
            origin: None,
            proofs: ProofCache::default(),
        }
    }
}

/// The rip's keys, resolved up front and held in memory only (KU §2.1).
///
/// Immutable and cheap to clone (an `Arc`); `Send + Sync`. Bound to one disc
/// ([`is_for`](Self::is_for)) and one [`KeyScope`]. Holds no key-source factory, so nothing
/// can ask a source after [`resolve`](Self::resolve) returns. No `Serialize`, no accessor
/// for key bytes or the Volume ID, and a redacting `Debug`.
#[derive(Clone)]
pub struct ResolvedKeySet(pub(crate) Arc<Inner>);

impl std::fmt::Debug for ResolvedKeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let i = &self.0;
        f.debug_struct("ResolvedKeySet")
            .field("aacs", &i.aacs)
            .field("disc_hash", &i.disc_hash)
            .field("scope", &i.scope)
            .field("keys", &"<redacted>")
            .field("keys_len", &i.pool.len())
            .field("vid", &i.vid.map(|_| "<redacted>"))
            .field("status", &self.status())
            .finish()
    }
}

fn norm_hash(h: &str) -> String {
    crate::hex::strip_hex_prefix(h).to_ascii_lowercase()
}

fn overlaps(a: &[(u32, u32)], s: u32, e: u32) -> bool {
    a.iter().any(|&(x, y)| x < e && s < y)
}

// SHA-256 over a domain tag and bytes (KU §4.1 fingerprints).
fn sha256(tag: &[u8], bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(tag);
    h.update(bytes);
    h.finalize().into()
}

impl ResolvedKeySet {
    /// Resolve the keys `scope` needs, once, before any output (KU §2.3). The only code in
    /// the product that asks a [`crate::KeySource`] for keys; `sources` is called once and
    /// every source it builds is dropped before this returns (LK7).
    ///
    /// `Err` refuses the rip before any output: E7022 (a title piece is Missing), E7032 (an
    /// image or folder piece is Missing), E7013 (a single declared key opens none of the
    /// ciphertext), E7026 (forensic keys Missing), E7028–30 (a source failed), `Halted`.
    pub fn resolve(
        disc: &Disc,
        reader: &mut dyn SectorSource,
        scope: KeyScope,
        sources: &KeySourceFactory,
        opts: ResolveKeysOptions,
    ) -> Result<KeyResolution> {
        resolve::resolve(
            disc,
            reader,
            scope,
            sources,
            opts,
            &resolve::RealClock::new(),
        )
    }

    /// [`resolve`](Self::resolve), reporting to the op's `progress` (stop design §2.1,
    /// T29): every source call runs [`busy`](crate::halt::Progress::busy) on it, and each
    /// source's ctx hands it out as [`ResolveCtx::progress`](crate::keysource::ResolveCtx::progress).
    pub fn resolve_with_progress(
        disc: &Disc,
        reader: &mut dyn SectorSource,
        scope: KeyScope,
        sources: &KeySourceFactory,
        opts: ResolveKeysOptions,
        progress: &crate::halt::Progress,
    ) -> Result<KeyResolution> {
        let clock = resolve::RealClock::new();
        resolve::resolve_observed(disc, reader, scope, sources, opts, &clock, Some(progress))
    }

    /// A set holding no keys: for a raw copy, or a disc with no AACS.
    pub fn none() -> ResolvedKeySet {
        ResolvedKeySet(Arc::new(Inner::empty()))
    }

    /// Whether this set keys AACS content: `false` for [`none`](Self::none), which a caller
    /// must not take as covering an AACS disc's titles ([`covers`](Self::covers) has no disc).
    pub fn is_aacs(&self) -> bool {
        self.0.aacs
    }

    /// Whether this set was resolved for `disc`: the same disc hash, capacity and format,
    /// and the same VID fingerprint when both are known. [`none`](Self::none) holds nothing
    /// and fits any disc.
    pub fn is_for(&self, disc: &Disc) -> bool {
        let i = &self.0;
        if !i.aacs {
            return true;
        }
        let Some(aacs) = disc.aacs.as_ref() else {
            return false;
        };
        if norm_hash(&aacs.disc_hash) != norm_hash(&i.disc_hash)
            || disc.format != i.format
            || (disc.capacity_sectors != 0
                && i.capacity != 0
                && disc.capacity_sectors != i.capacity)
        {
            return false;
        }
        match (i.vid, aacs.volume_id) {
            (Some(held), vid) if vid != [0u8; 16] => held == vid,
            _ => true,
        }
    }

    /// Whether the set was resolved over `scope`: structural coverage only. It says nothing
    /// about whether an AACS title is keyed: a set with no AACS key ([`none`](Self::none))
    /// covers every scope. Use the engine's disc-aware check, or [`is_aacs`](Self::is_aacs).
    pub fn covers(&self, scope: &KeyScope) -> bool {
        if !self.0.aacs {
            return true;
        }
        match (&self.0.scope, scope) {
            (_, KeyScope::None) => true,
            (KeyScope::WholeDisc, _) => true,
            (KeyScope::Titles(have), KeyScope::Titles(want)) => {
                want.iter().all(|t| have.contains(t))
            }
            _ => false,
        }
    }

    /// Counts and states for the front end (KU §7.6). No key bytes.
    pub fn status(&self) -> KeySetStatus {
        let i = &self.0;
        KeySetStatus {
            proven: i.proven.len(),
            declared: i.n_decl,
            keyed: i.keyed,
            clear: i.clear,
            lazy: i.arrival.len() - i.clear.min(i.arrival.len()),
            forensic: i.forensic,
            origin: i.origin,
            best_effort: i.best_effort,
            requests: i.requests,
        }
    }

    /// `SHA-256("freemkv-vid-fp-v1" ‖ VID)`: the only form of the VID that may be written
    /// (KU §4.1). `None` when the set holds no VID.
    pub fn vid_fingerprint(&self) -> Option<[u8; 32]> {
        self.0.vid.map(|v| sha256(b"freemkv-vid-fp-v1", &v))
    }

    /// `SHA-256("freemkv-key-fp-v1" ‖ key)[..8]` of each proven base key: the legacy
    /// mapfile identity (KU §4.4). Empty when no base key was proven.
    pub fn proven_key_fingerprints(&self) -> Vec<[u8; 8]> {
        self.0
            .proven
            .iter()
            .filter_map(|&s| self.0.pool.get(s))
            .map(|k| {
                let d = sha256(b"freemkv-key-fp-v1", k);
                let mut fp = [0u8; 8];
                fp.copy_from_slice(&d[..8]);
                fp
            })
            .collect()
    }

    /// `[start, end)` sector ranges left to the on-arrival proof (KU §2.4).
    pub fn lazy(&self) -> &[(u32, u32)] {
        &self.0.lazy
    }

    /// Every index-1 forensic anchor read faulted (KU §5.4): a decrypting single-pass rip
    /// or disc→ISO copy must refuse (E7026); a raw capture asks once from its image later.
    pub fn forensic_pending(&self) -> bool {
        self.0.forensic == ForensicState::Pending
    }

    /// Key-source requests `resolve` made.
    pub fn source_requests(&self) -> u32 {
        self.0.requests
    }

    /// The rip's on-arrival proof record, shared by every clone of this set (J16).
    pub fn proof_cache(&self) -> &ProofCache {
        &self.0.proofs
    }

    // The decrypt keys the readers use: base keys (slot = index) then forensic index keys.
    pub(crate) fn decrypt_keys(&self) -> DecryptKeys {
        let i = &self.0;
        let mut unit_keys: Vec<(u32, [u8; 16])> = i
            .pool
            .iter()
            .enumerate()
            .map(|(s, k)| (s as u32 + 1, *k))
            .collect();
        for (j, k) in i.fmts_keys.iter().enumerate() {
            let tag = crate::mux::resolve::FMTS_POOL_TAG_BASE.saturating_add(j as u32);
            unit_keys.push((tag, *k));
        }
        DecryptKeys::Aacs {
            unit_keys,
            format: i.content_format,
        }
    }

    pub(crate) fn key_map(&self) -> Arc<AacsKeyMap> {
        self.0.map.clone()
    }

    // Whether every sector of `extents` lies in a piece this set resolved.
    pub(crate) fn covers_extents(&self, extents: &[crate::disc::Extent]) -> bool {
        let mut spans: Vec<(u64, u64)> = self
            .0
            .spans
            .iter()
            .map(|&(s, n, _)| (s as u64, s as u64 + n as u64))
            .collect();
        spans.sort_unstable();
        extents.iter().all(|e| {
            let (mut at, end) = (
                e.start_lba as u64,
                e.start_lba as u64 + e.sector_count as u64,
            );
            for &(s, t) in &spans {
                if s <= at && at < t {
                    at = t;
                }
            }
            at >= end
        })
    }

    // The E7013 caller-bug refusal (KU §6): a set used on the wrong disc, outside its scope,
    // or over a source that cannot seek.
    fn caller_bug(what: &'static str) -> Error {
        tracing::error!(target: "freemkv::keys", what, "key set used incorrectly (caller bug)");
        debug_assert!(false, "key set used incorrectly: {what}");
        Error::DecryptFailed
    }

    // The reader checks (KU §2.4, §5.4): refuse a non-random-access inner source and, unless
    // `allow_pending`, content touching a forensic clip whose keys are Pending (E7026).
    pub(crate) fn gate(
        &self,
        random_access: bool,
        extents: Option<&[(u32, u32)]>,
        allow_pending: bool,
    ) -> Result<()> {
        if !random_access {
            // KU §2.4: side reads need random access; `Prefetched(Decrypting(..))`, never
            // the other way round (SG27).
            return Err(Self::caller_bug("decrypting reader over a prefetcher"));
        }
        if self.forensic_pending() && self.touches_clip(extents) && !allow_pending {
            tracing::error!(
                target: "freemkv::keys",
                code = crate::error::E_FMTS_KEY_MISSING,
                "forensic keys pending (every anchor unreadable): a decrypting single pass \
                 cannot key the forensic segments; copy raw, then convert"
            );
            return Err(Error::FmtsKeyMissing);
        }
        Ok(())
    }

    fn touches_clip(&self, extents: Option<&[(u32, u32)]>) -> bool {
        let clip = &self.0.forensic_clip;
        match extents {
            None => !clip.is_empty(),
            Some(ex) => ex.iter().any(|&(s, e)| overlaps(clip, s, e)),
        }
    }

    // The on-arrival proof for this set's readers, if any piece needs one (KU §2.4).
    pub(crate) fn arrival(&self, stop: StopKind) -> Option<Arrival> {
        (!self.0.arrival.is_empty() && !self.0.best_effort).then(|| Arrival::new(self, stop))
    }

    // The E7022 stop for a title read of this set's disc.
    pub(crate) fn title_stop(&self) -> StopKind {
        StopKind::Title {
            disc_hash: crate::hex::strip_hex_prefix(&self.0.disc_hash).to_string(),
        }
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.0.capacity
    }

    pub(crate) fn content_format(&self) -> ContentFormat {
        self.0.content_format
    }

    // The decrypting view over `inner` for content touching `extents` (`None`: the whole
    // disc), after [`gate`](Self::gate).
    pub(crate) fn decrypting<S: SectorSource>(
        &self,
        inner: S,
        extents: Option<&[(u32, u32)]>,
        stop: StopKind,
        allow_pending: bool,
    ) -> Result<DecryptingSectorSource<S>> {
        let i = &self.0;
        self.gate(inner.random_access(), extents, allow_pending)?;
        let mut dec =
            DecryptingSectorSource::new(inner, self.decrypt_keys()).with_key_map(self.key_map());
        if self.forensic_pending() && self.touches_clip(extents) {
            // Pending segments stay ciphertext (never a stop, KU §2.4): outside content.
            let spans: Vec<(u32, u32)> = i.spans.iter().map(|&(s, n, _)| (s, n)).collect();
            let content: Vec<(u32, u32)> = crate::whole_disc::subtract_ranges(&spans, &i.segments)
                .into_iter()
                .map(|(s, e)| (s, e - s))
                .collect();
            dec = dec.with_content_ranges(Arc::from(crate::whole_disc::merge_ranges(content)));
        }
        if let Some(a) = self.arrival(stop) {
            dec = dec.with_arrival(a);
        }
        Ok(dec)
    }

    // A set holding no key over `title` (KU §3.1: "`keys` must be `Some` for AACS"): every
    // extent is left to the on-arrival proof, which has no key to try, so the first
    // AACS-flagged unit stops with E7022 and clear units pass. For a mux given no AACS set.
    pub(crate) fn keyless_for(title: &crate::disc::DiscTitle, format: ContentFormat) -> Self {
        let mut i = Inner::empty();
        i.aacs = true;
        i.content_format = format;
        i.scope = KeyScope::WholeDisc;
        // Arrival spans must be disjoint: overlapping extents (a clip played twice, or one
        // inside another) merge into one span. Adjacent clips keep their own unit grids.
        let mut ranges: Vec<(u32, u32)> = title
            .extents
            .iter()
            .filter(|e| e.sector_count > 0)
            .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
            .collect();
        ranges.sort_unstable();
        let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
        for (s, e) in ranges {
            match merged.last_mut() {
                Some(last) if s < last.1 => last.1 = last.1.max(e),
                _ => merged.push((s, e)),
            }
        }
        for (s, e) in merged {
            let span = (s, e - s, s as u64);
            i.spans.push(span);
            i.arrival.push(ArrivalPiece {
                id: s,
                spans: vec![span],
                candidate: None,
            });
        }
        ResolvedKeySet(Arc::new(i))
    }

    // `keyless_for` title `idx` of `disc`, carrying the disc's identity: its E7022 names the
    // disc, and it passes `is_for` on that disc (a Session mux given no set, over no key).
    pub(crate) fn keyless_for_disc(disc: &Disc, idx: usize) -> Option<Self> {
        let title = disc.titles.get(idx)?;
        let mut set = Self::keyless_for(title, disc.content_format);
        let i = Arc::get_mut(&mut set.0).expect("a fresh set has one owner");
        i.disc_hash = disc
            .aacs
            .as_ref()
            .map(|a| a.disc_hash.clone())
            .unwrap_or_default();
        i.format = disc.format;
        i.capacity = disc.capacity_sectors;
        Some(set)
    }

    // A set keying `ranges` (`[start, end)`) with `key` as proven pieces: the mux tests'
    // stand-in for a set `resolve` built over a disc they do not lay out.
    #[cfg(test)]
    pub(crate) fn keyed_for_test(disc: &Disc, key: [u8; 16], ranges: &[(u32, u32)]) -> Self {
        let mut i = Inner::empty();
        i.aacs = true;
        i.disc_hash = disc
            .aacs
            .as_ref()
            .map(|a| a.disc_hash.clone())
            .unwrap_or_default();
        i.capacity = disc.capacity_sectors;
        i.format = disc.format;
        i.content_format = disc.content_format;
        i.scope = KeyScope::WholeDisc;
        i.pool = vec![key];
        i.proven = vec![0];
        i.keyed = ranges.len();
        i.spans = ranges.iter().map(|&(s, e)| (s, e - s, s as u64)).collect();
        i.map = Arc::new(AacsKeyMap::from_ranges(
            ranges.iter().map(|&(s, e)| (s, e, 0)).collect(),
        ));
        ResolvedKeySet(Arc::new(i))
    }

    /// The decrypting reader for title `idx` of `disc` over the raw, random-access `inner`:
    /// keyed pieces through the set's map, the rest proven on arrival (a readable unit no
    /// held key opens stops with E7022). E7013 if the set is not for `disc`, does not cover
    /// the title, or `inner` cannot seek; E7026 if the title needs Pending forensic keys.
    pub fn title_reader<S: SectorSource>(
        &self,
        disc: &Disc,
        idx: usize,
        inner: S,
    ) -> Result<DecryptingSectorSource<S>> {
        if !self.is_for(disc) {
            return Err(Self::caller_bug("key set is not for this disc"));
        }
        let title = disc.titles.get(idx).ok_or(Error::DiscTitleRange {
            index: idx,
            count: disc.titles.len(),
        })?;
        if !self.0.aacs {
            if !inner.random_access() {
                return Err(Self::caller_bug("decrypting reader over a prefetcher"));
            }
            return Ok(DecryptingSectorSource::new(inner, disc.decrypt_keys()));
        }
        if !self.covers(&KeyScope::Titles(vec![idx])) {
            return Err(Self::caller_bug("title outside the key set's scope"));
        }
        let extents: Vec<(u32, u32)> = title
            .extents
            .iter()
            .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
            .collect();
        let stop = StopKind::Title {
            disc_hash: disc.aacs_disc_hash(),
        };
        self.decrypting(inner, Some(&extents), stop, false)
    }

    /// The whole-disc decrypting reader for a decrypted image or folder copy over the raw,
    /// random-access `inner`, on each stream file's own unit grid. A readable unit no held
    /// key opens stops with E7032. E7013 if the set is not for `disc`, does not cover the
    /// whole disc, or `inner` cannot seek; E7026 if forensic keys are Pending.
    pub fn whole_disc_reader<S: SectorSource>(
        &self,
        disc: &Disc,
        inner: S,
        halt: Option<&Halt>,
    ) -> Result<WholeDiscReader<S>> {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        if !self.is_for(disc) {
            return Err(Self::caller_bug("key set is not for this disc"));
        }
        let mut content = disc.encrypted_content_ranges();
        if !self.0.aacs {
            if !inner.random_access() {
                return Err(Self::caller_bug("decrypting reader over a prefetcher"));
            }
            let mut dec = DecryptingSectorSource::new(inner, disc.decrypt_keys());
            if !content.is_empty() {
                dec = dec.with_content_ranges(Arc::from(content));
            }
            return Ok(crate::whole_disc::UnitAligned::new(dec, Vec::new()));
        }
        if !self.covers(&KeyScope::WholeDisc) {
            return Err(Self::caller_bug("whole disc outside the key set's scope"));
        }
        if self.0.no_stream_files && !content.is_empty() {
            // Titles but no stream file: sweeping would key bare title extents (freemkv#55).
            tracing::warn!(target: "freemkv::scan", "titles but no AACS content files");
            let path = match disc.format {
                DiscFormat::HdDvd => "/HVDVD_TS",
                _ => "/BDMV/STREAM",
            };
            return Err(Error::UdfNotFound { path: path.into() });
        }
        if self.0.lazy_unopened {
            // KU §2.1 (6) "Refuse first": a sweep reads that unit, and no held key opens it.
            tracing::error!(target: "freemkv::keys", code = crate::error::E_WHOLE_DISC_KEY_MISSING, "a stream file holds a unit no held key opens: refusing the sweep before any output");
            return Err(Error::WholeDiscKeyMissing);
        }
        let mut dec = self.decrypting(inner, None, StopKind::Image, false)?;
        content.extend(self.0.spans.iter().map(|&(s, n, _)| (s, n)));
        let content = crate::whole_disc::merge_ranges(content);
        if !content.is_empty() {
            dec = dec.with_content_ranges(Arc::from(content));
        }
        Ok(crate::whole_disc::UnitAligned::new(
            dec,
            self.0.spans.clone(),
        ))
    }
}

/// Whether `disc` can be decrypted with `keys` (KU §3.5). CSS is read from `disc.css` /
/// `css_error`; an AACS disc needs a set for it, so `keys == None` is `AacsKeysMissing`.
pub fn decrypt_status(disc: &Disc, keys: Option<&ResolvedKeySet>) -> DecryptStatus {
    if disc.css_error.is_some() {
        return DecryptStatus::CssNotCracked(Error::CssNoDiscKey);
    }
    if disc.css.is_some() {
        return DecryptStatus::Ready;
    }
    if disc.aacs.is_none() && !disc.encrypted {
        return DecryptStatus::NotEncrypted;
    }
    match keys {
        Some(set) if set.is_aacs() => {
            if !set.is_for(disc) {
                DecryptStatus::AacsKeysMissing(Error::DecryptFailed)
            } else if set.forensic_pending() {
                DecryptStatus::ForensicPending
            } else {
                DecryptStatus::Ready
            }
        }
        _ => match disc_gate(disc, false) {
            Ok(()) => DecryptStatus::Ready,
            Err(e) => DecryptStatus::AacsKeysMissing(e),
        },
    }
}

/// The pre-flight decrypt gate over a set (KU §3.5): `Ok` when `scope` can be decrypted
/// from `keys`, else the typed refusal, before any output. `raw` always passes. CSS is read
/// from `disc.css` / `css_error`; an AACS disc with no AACS set refuses.
pub fn check_decryptable(
    disc: &Disc,
    raw: bool,
    keys: Option<&ResolvedKeySet>,
    scope: &KeyScope,
) -> Result<()> {
    if raw || matches!(scope, KeyScope::None) {
        return Ok(());
    }
    match keys {
        Some(set) if set.is_aacs() && disc.css.is_none() && disc.css_error.is_none() => {
            if !set.is_for(disc) {
                return Err(ResolvedKeySet::caller_bug("key set is not for this disc"));
            }
            if !set.covers(scope) {
                return Err(ResolvedKeySet::caller_bug("scope outside the key set"));
            }
            if set.forensic_pending() {
                return Err(Error::FmtsKeyMissing);
            }
            Ok(())
        }
        _ => disc_gate(disc, false),
    }
}

/// The disc-wide gate with no AACS set: `Ok` for a clear disc or a cracked CSS disc, else
/// the typed refusal. `raw` always passes. An AACS disc refuses with the scan's
/// handshake or key-service failure when it recorded one, else `NoDiscKey`.
pub(crate) fn disc_gate(disc: &Disc, raw: bool) -> Result<()> {
    if raw {
        return Ok(());
    }
    // Scrambled-but-uncracked CSS is a WHOLE-DISC verdict (`CssNoDiscKey`), never the
    // per-title `CssKeyMissing`, which would log "empty stub" per title and exit 0.
    if disc.css_error.is_some() {
        return Err(Error::CssNoDiscKey);
    }
    if !matches!(disc.decrypt_keys(), DecryptKeys::None) {
        return Ok(());
    }
    if disc.aacs.is_some() {
        // E7017 vs E7022: a handshake or key-service failure surfaces as itself, else
        // operators hunt a VUK during an outage.
        if let Some(e) = disc
            .aacs_error
            .as_ref()
            .and_then(crate::disc::handshake_class_error)
        {
            return Err(e);
        }
        return Err(match disc.aacs_error {
            Some(Error::KeyServiceUnavailable) => Error::KeyServiceUnavailable,
            Some(Error::KeyServiceUnauthorized) => Error::KeyServiceUnauthorized,
            Some(Error::KeyServiceRateLimited) => Error::KeyServiceRateLimited,
            _ => Error::NoDiscKey {
                disc_hash: disc.aacs_disc_hash(),
            },
        });
    }
    if disc.css.is_some() {
        return Err(Error::CssKeyMissing);
    }
    Ok(())
}

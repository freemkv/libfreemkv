//! Key sources — the layer that hands libfreemkv a disc's terminal Unit Keys.
//!
//! libfreemkv performs NO key lookup. An application resolves a disc's keys
//! through one or more [`KeySource`]s, each an adapter over a backing store
//! (a keydb file, a key server, the mapfile cache) that returns the disc's
//! terminal **Unit Keys** ([`crate::aacs::types::UnitKey`]), orchestrating
//! derivation via the [`ResolveCtx`] handed to it and libfreemkv's own crypto
//! primitives. libfreemkv owns the crypto; a source owns only PATH
//! ORCHESTRATION. See docs/keysource.md for the ownership split.

use crate::aacs::types::HostCert;
use crate::aacs::types::{UnitKey, Vid};
use crate::disc::Key;
use crate::error::Error;

/// Minimum encrypted-content unit samples a single online key request must carry.
///
/// The key service identifies a key by which of the submitted units it decrypts,
/// so too few samples can return a key that matches an incidental unit rather
/// than the one asked about (a false positive); this many make a request
/// unambiguous. Canonical here so both `freemkv-keysources`'s online source and
/// libfreemkv's own FMTS forensic query ([`crate::mux`]) agree on one value —
/// see docs/keysource.md for the layering rationale.
pub const MIN_SAMPLE_UNITS: usize = 8;

/// A set of encrypted content-unit samples PROVEN to carry at least
/// [`MIN_SAMPLE_UNITS`] units — the online `/decode` request's proof-of-ownership.
///
/// "Parse, don't validate": the only constructor, [`DecodeSampleSet::new`], returns
/// `None` for an under-sized slice, so an online key request simply *cannot be built*
/// from too few samples — a compile-time obligation, not a runtime check a caller
/// can forget. See docs/keysource.md for the full rationale (including why the
/// wrapped samples get a hand-written, redacting [`Debug`] — the impl below).
#[derive(Clone)]
pub struct DecodeSampleSet(Vec<Vec<u8>>);

impl std::fmt::Debug for DecodeSampleSet {
    // Prints SHAPE only — a derived Debug dumped multi-MB of ciphertext
    // verbatim into any log that formats it. See docs/keysource.md.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodeSampleSet")
            .field("units", &"<redacted>")
            .field("units_len", &self.0.len())
            .finish()
    }
}

impl DecodeSampleSet {
    /// Wrap `units` iff it carries at least [`MIN_SAMPLE_UNITS`] samples; `None`
    /// otherwise (the caller then skips the online source rather than sending an
    /// ambiguous request). This is the sole way to obtain a `DecodeSampleSet`.
    pub fn new(units: Vec<Vec<u8>>) -> Option<Self> {
        (units.len() >= MIN_SAMPLE_UNITS).then_some(Self(units))
    }

    /// The proven-sufficient samples. Guaranteed `>= MIN_SAMPLE_UNITS` in length.
    pub fn units(&self) -> &[Vec<u8>] {
        &self.0
    }

    /// Number of samples — always `>= MIN_SAMPLE_UNITS`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always `false` (a `DecodeSampleSet` never holds fewer than `MIN_SAMPLE_UNITS`);
    /// provided so the type satisfies the usual `len`/`is_empty` pairing.
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// The public AACS inputs a key source needs to look a disc up. Captured at
/// scan; carries no DERIVED secrets (no media key, VUK or plaintext unit key) —
/// only the disc identity and the on-disc AACS structures a source or key server
/// may key on. The on-disc structures are nonetheless key MATERIAL (the encrypted
/// title keys live in `unit_key_ro`), so [`Debug`] is hand-written and redacting;
/// see the impl below.
#[derive(Clone)]
pub struct DiscInputs {
    /// SHA-1 of `Unit_Key_RO.inf`, `0x`-prefixed hex. The value a keydb keys
    /// its per-disc entries by, and a key server identifies the disc with.
    pub disc_hash: String,
    /// Volume ID (16 bytes). `[0u8; 16]` when no authenticated handshake ran
    /// (e.g. an ISO/mapfile flow), which disables VID-keyed lookups.
    pub volume_id: [u8; 16],
    /// AACS major version (1 = V10 / BD AACS 1.0, 2 = V20+ / UHD). Drives the
    /// `Unit_Key_RO.inf` parse stride (48-byte V10 vs 64-byte V20/V21) when a
    /// source returns a VUK to derive unit keys from. Defaults to 2.
    pub version: u8,
    /// Raw MKB bytes. Empty when not captured.
    pub mkb: Vec<u8>,
    /// Raw `Unit_Key_RO.inf` bytes. Empty when not captured.
    pub unit_key_ro: Vec<u8>,
    /// Encrypted on-disc content sample units (each a 6144-byte aligned unit),
    /// for sources that validate a key server-side against real ciphertext
    /// (e.g. an online key service). Empty for sources that don't need them
    /// (a local keydb). Populated by the application — reading content requires
    /// the disc reader, which the library's scan does not retain — so
    /// [`crate::Disc::inputs`] leaves it empty for the caller to fill.
    pub samples: Vec<Vec<u8>>,
    /// The disc's human title — the UDF/ISO volume identifier (e.g.
    /// `TITLE_2024`), falling back to the BDMV `<di:name>` when present.
    /// `None` when not captured. Identity only, no secret; a key service may
    /// record it (keyed by `disc_hash`) to build a hash→title catalog. Not used
    /// in any AACS derivation.
    pub volume_label: Option<String>,
}

// Redacting Debug: a derived impl used to print the Volume ID, the whole
// Unit_Key_RO.inf, the MKB and every ciphertext sample verbatim into a bug
// report's log. See docs/keysource.md.
impl std::fmt::Debug for DiscInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscInputs")
            .field("disc_hash", &self.disc_hash)
            .field("volume_id", &"<redacted>")
            .field("version", &self.version)
            .field("mkb", &"<redacted>")
            .field("mkb_len", &self.mkb.len())
            .field("unit_key_ro", &"<redacted>")
            .field("unit_key_ro_len", &self.unit_key_ro.len())
            .field("samples", &"<redacted>")
            .field("samples_len", &self.samples.len())
            .field("volume_label", &self.volume_label)
            .finish()
    }
}

/// A lazy view of a disc's AACS material, handed to [`KeySource::get_unit_keys`] so a
/// source can drive the derivation chain without holding the disc reader.
///
/// "Lazy" by contract: each accessor returns only what the source asks for, so a
/// source that already holds terminal Unit Keys never touches the MKB or
/// samples. (Today the backing [`DiscInputsCtx`] is eagerly populated from a
/// scan-time [`DiscInputs`]; the trait keeps the lazy signature so a future
/// implementation can fetch on demand without a source-API break.)
pub trait ResolveCtx {
    /// SHA-1 of `Unit_Key_RO.inf`, `0x`-prefixed hex — the per-disc lookup key.
    fn disc_hash(&self) -> &str;
    /// The disc's human title (UDF/ISO volume identifier), when captured.
    fn title(&self) -> Option<&str>;
    /// Volume ID, or `None` when no authenticated handshake ran (the all-zero
    /// sentinel) — VID-dependent derivation (`MK → VUK`) is then impossible.
    fn vid(&self) -> Option<Vid>;
    /// Raw MKB bytes (may be empty when not captured).
    fn mkb(&self) -> Result<&[u8], Error>;
    /// The disc's encrypted title keys, parsed from `Unit_Key_RO.inf` the same
    /// way the library's resolver parses them ([`crate::aacs::inf::parse_unit_key_ro`]),
    /// in on-disc order. Feed straight into [`crate::aacs::derive::decrypt_unit_key`].
    fn enc_title_keys(&self) -> Result<&[[u8; 16]], Error>;
    /// Up to `n` encrypted on-disc content sample units, for a source that
    /// validates a candidate server-side against real ciphertext.
    fn samples(&self, n: usize) -> Result<Vec<Vec<u8>>, Error>;
    /// Raw `Unit_Key_RO.inf` bytes, verbatim. Most sources derive locally from
    /// the parsed [`Self::enc_title_keys`]; a source that forwards the on-disc
    /// structure to a server doing its OWN derivation (an online key service)
    /// needs the unparsed blob. Empty when not captured. Defaults to empty so
    /// existing/foreign `ResolveCtx` impls keep compiling unchanged.
    fn unit_key_ro(&self) -> &[u8] {
        &[]
    }
}

/// [`ResolveCtx`] over a scan-time [`DiscInputs`].
///
/// Pre-parses the encrypted title keys at construction (so `enc_title_keys` can
/// hand back a borrowed slice) at the version-appropriate `Unit_Key_RO.inf`
/// stride — `version_u8` is the disc's AACS major (1 → 48-byte V10 stride, else
/// 64-byte V20/V21 stride), matching the library resolver's dispatch.
pub struct DiscInputsCtx<'a> {
    inner: &'a DiscInputs,
    enc_keys: Vec<[u8; 16]>,
}

impl<'a> DiscInputsCtx<'a> {
    /// Build a context over `inputs`, parsing the encrypted title keys at the
    /// stride for the disc's own AACS major (`inputs.version`: 1 → 48-byte V10
    /// stride, else 64-byte V20/V21) — the single source of truth, no separate
    /// version argument to drift from it.
    ///
    /// A malformed `unit_key_ro` parses to an empty key set rather than an
    /// error; see docs/keysource.md for why that's deliberate.
    pub fn new(inputs: &'a DiscInputs) -> Self {
        use crate::aacs::inf::parse_unit_key_ro;
        use crate::aacs::mkb::AacsVersion;
        let enc_keys = if inputs.unit_key_ro.is_empty() {
            Vec::new()
        } else {
            parse_unit_key_ro(&inputs.unit_key_ro, AacsVersion::from_major(inputs.version))
                .map(|f| f.encrypted_keys.into_iter().map(|(_, k)| k).collect())
                .unwrap_or_default()
        };
        Self {
            inner: inputs,
            enc_keys,
        }
    }
}

impl ResolveCtx for DiscInputsCtx<'_> {
    fn disc_hash(&self) -> &str {
        &self.inner.disc_hash
    }
    fn title(&self) -> Option<&str> {
        self.inner.volume_label.as_deref()
    }
    fn vid(&self) -> Option<Vid> {
        if self.inner.volume_id == [0u8; 16] {
            None
        } else {
            Some(Vid(self.inner.volume_id))
        }
    }
    fn mkb(&self) -> Result<&[u8], Error> {
        Ok(&self.inner.mkb)
    }
    fn enc_title_keys(&self) -> Result<&[[u8; 16]], Error> {
        Ok(&self.enc_keys)
    }
    fn samples(&self, n: usize) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.inner.samples.iter().take(n).cloned().collect())
    }
    fn unit_key_ro(&self) -> &[u8] {
        &self.inner.unit_key_ro
    }
}

/// A key source: an adapter over a backing store that resolves a disc's terminal
/// Unit Keys.
///
/// Dumb about *policy*, smart about *its own material*: given a [`ResolveCtx`] a
/// source orchestrates derivation down to Unit Keys using the library's
/// boil-down crypto — never re-implementing AES. Two explicit resolve ops, one
/// per key kind: [`get_unit_keys`](Self::get_unit_keys) (base per-CPS-unit),
/// [`get_fmts_indexes`](Self::get_fmts_indexes) (AACS 2.1 forensic set). See
/// docs/keysource.md for the full contract.
pub trait KeySource {
    /// Resolve this disc's base per-CPS-unit Unit Keys from this source. An empty
    /// `Vec` is a genuine "no key here"; `Err` is a source failure.
    fn get_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error>;

    /// Resolve this disc's AACS 2.1 forensic index keys — the per-index keys the
    /// base Unit Key cannot open (see [`crate::aacs::segment`]) — ordered by
    /// forensic index (element `i` carries `UnitKey.idx == i`, forensic index
    /// `i + 1`). The source hands back the COMPLETE set it holds; the caller
    /// trusts any non-empty result as all of them and never assumes a fixed count.
    /// Defaults to empty: a source with no forensic material (a plain keydb, the
    /// mapfile) opts out, and only an FMTS disc's mux ever calls this.
    fn get_fmts_indexes(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
        Ok(Vec::new())
    }

    /// The AACS host certificate(s) this source can supply for the live-drive
    /// SCSI mutual-auth handshake (the OEM/AACS baseline route). `mkb` is the
    /// disc's MKB generation when known, so a source MAY return only certs whose
    /// generation matches (the default ignores it). A host cert unlocks the
    /// authenticated bus so the drive reports the Volume ID and bus key; it is
    /// **perishable** (revocable on a drive's HRL), so it is served by a source,
    /// never compiled in. A source holding no cert returns the empty vec.
    fn host_certs(&self, _mkb: Option<u32>) -> Vec<HostCert> {
        Vec::new()
    }

    /// A short, stable identifier for this source kind (`"keydb"`, `"online"`,
    /// `"mapfile"`, …). For logging which source produced a key, and for
    /// composition/ordering. A format string, not user-facing English.
    fn label(&self) -> &'static str {
        "source"
    }
}

/// Drive `sources` until one resolves Unit Keys that decrypt `disc`. Returns
/// `true` at the first source whose keys validate and commit, `false` once every
/// source is exhausted (the genuine "no key for this disc"). Thin wrapper over
/// [`resolve_and_apply_traced`] that discards the trace.
pub fn resolve_and_apply(
    sources: &[Box<dyn KeySource>],
    inputs: &DiscInputs,
    disc: &mut crate::Disc,
) -> bool {
    resolve_and_apply_traced(sources, inputs, disc).0
}

/// Like [`resolve_and_apply`] but also returns a structured
/// [`crate::aacs::trace::ResolutionTrace`] recording, per source, what happened — for
/// applications to render. ZERO English; the trace is typed enums only.
///
/// One-shot per source: each source's [`KeySource::get_unit_keys`] is called
/// exactly once; non-empty Unit Keys are applied via
/// [`crate::Disc::decrypt_with`], which validates against `inputs.samples`
/// and only mutates the disc on success. CPS-unit ORDER is load-bearing (see
/// docs/keysource.md) even though the carried number is not.
pub fn resolve_and_apply_traced(
    sources: &[Box<dyn KeySource>],
    inputs: &DiscInputs,
    disc: &mut crate::Disc,
) -> (bool, crate::aacs::trace::ResolutionTrace) {
    use crate::aacs::trace::{KeyNode, KeyOutcome, KeyStep};

    let mut trace = crate::aacs::trace::ResolutionTrace::new();

    // The FIRST source failure seen, if any. An `Err` means the source could not
    // answer at all (not "no key"), so its reason is stamped onto `disc.aacs_error`
    // below, reporting the ordered sources' most-preferred failure to the operator.
    let mut source_failure: Option<crate::error::Error> = None;

    // The ctx parses Unit_Key_RO.inf at the stride for `inputs.version` (the
    // disc's own AACS major), so the stride is the disc's single source of truth.
    let ctx = DiscInputsCtx::new(inputs);

    for source in sources {
        // `who` is the source's own stable identifier — no enum to map back to.
        let who = source.label().to_string();
        match source.get_unit_keys(&ctx) {
            Ok(uks) if !uks.is_empty() => {
                // Positional index → canonical CPS-unit number (position + 1).
                let unit_keys: Vec<(u32, [u8; 16])> = uks
                    .iter()
                    .map(|uk| (uk.idx.saturating_add(1), uk.key))
                    .collect();
                if disc
                    .decrypt_with(Key::Unit(unit_keys), &inputs.samples)
                    .is_ok()
                {
                    trace.keys.push(KeyStep {
                        who,
                        path: vec![KeyNode::FoundUnitKeys, KeyNode::DerivedUnitKeys],
                        outcome: KeyOutcome::Resolved,
                    });
                    return (true, trace);
                }
                // Keys produced but rejected by validation — record and continue.
                trace.keys.push(KeyStep {
                    who,
                    path: vec![KeyNode::FoundUnitKeys],
                    outcome: KeyOutcome::NoKey,
                });
            }
            // The source ANSWERED and holds nothing for this disc. This — and
            // only this — is `NoEntry`: the claim "I looked, it is not there".
            Ok(_) => {
                trace.keys.push(KeyStep {
                    who,
                    path: vec![KeyNode::NoEntry],
                    outcome: KeyOutcome::NoKey,
                });
            }
            // The source could NOT answer (unreachable/errored/refused), so path is
            // EMPTY, not `NoEntry` — conflating the two once rendered 502s as "no
            // key" and misled operators. The real reason rides on `disc.aacs_error`.
            Err(e) => {
                if source_failure.is_none() {
                    source_failure = Some(e);
                }
                trace.keys.push(KeyStep {
                    who,
                    path: Vec::new(),
                    outcome: KeyOutcome::NoKey,
                });
            }
        }
    }
    // Nothing resolved. If a source FAILED rather than answered, stamp that reason
    // onto the disc for the decrypt gate — but never clobber a reason the scan
    // already captured (e.g. `AacsVidUnavailable`), which is closer to the disc.
    if let Some(e) = source_failure
        && disc.aacs_error.is_none()
    {
        disc.aacs_error = Some(e);
    }
    (false, trace)
}

/// THE single key-fetch: drive `sources` in order and return the first non-empty
/// Unit Key set. Same sources, same call for both disc-open and read-miss
/// paths — there is no separate "fetch", only the samples in `ctx` differ.
/// Unlike [`resolve_and_apply`] this does not validate/commit to a disc — the
/// read's decorator re-decrypts with the returned keys, which is the
/// validation. See docs/keysource.md for the two callers.
pub fn fetch_unit_keys(sources: &[Box<dyn KeySource>], ctx: &dyn ResolveCtx) -> Vec<UnitKey> {
    drive_unit_keys(sources, ctx).keys
}

// Whether a driver run resolved keys, and — if not — whether that's a genuine
// absence (`errored == false`, safe to cache) or a source FAILURE
// (`errored == true`, must not be cached). See docs/keysource.md.
struct FetchOutcome {
    keys: Vec<UnitKey>,
    errored: bool,
}

// [`fetch_unit_keys`] plus the error signal (`Err` = source failure, empty
// `Ok` = genuine absence — see [`KeySource::get_unit_keys`]).
fn drive_unit_keys(sources: &[Box<dyn KeySource>], ctx: &dyn ResolveCtx) -> FetchOutcome {
    let mut errored = false;
    for source in sources {
        match source.get_unit_keys(ctx) {
            Ok(uks) if !uks.is_empty() => {
                return FetchOutcome {
                    keys: uks,
                    errored: false,
                };
            }
            Ok(_) => {}
            Err(_) => errored = true,
        }
    }
    FetchOutcome {
        keys: Vec::new(),
        errored,
    }
}

/// The forensic counterpart to [`fetch_unit_keys`]: drive `sources` in order and
/// return the first source's non-empty AACS 2.1 forensic index set. `ctx` carries
/// the index-1 anchor batch (the mux, which owns disc geometry, gathers it and
/// injects it as the ctx's samples); a source that needs no samples (a keydb
/// keying on `disc_hash`) ignores them. Whatever the winning source returns —
/// ≥ 1 key — is trusted as the COMPLETE ordered set; no fixed count is assumed.
pub fn fetch_fmts_indexes(sources: &[Box<dyn KeySource>], ctx: &dyn ResolveCtx) -> Vec<UnitKey> {
    drive_fmts_indexes(sources, ctx).keys
}

/// [`fetch_fmts_indexes`] plus the error signal (see [`drive_unit_keys`]): the
/// forensic counterpart that flags whether any source `Err`ed during the miss.
fn drive_fmts_indexes(sources: &[Box<dyn KeySource>], ctx: &dyn ResolveCtx) -> FetchOutcome {
    let mut errored = false;
    for source in sources {
        match source.get_fmts_indexes(ctx) {
            Ok(uks) if !uks.is_empty() => {
                return FetchOutcome {
                    keys: uks,
                    errored: false,
                };
            }
            Ok(_) => {}
            Err(_) => errored = true,
        }
    }
    FetchOutcome {
        keys: Vec::new(),
        errored,
    }
}

/// Build the read-time [`crate::sector::KeyFetch`] from the disc's public AACS
/// inputs and a way to (re)build the application's key sources. The returned
/// resolver has the two explicit operations the mux and recovery decorator
/// call: [`unit_keys`](crate::sector::KeyFetch::unit_keys) drives
/// [`fetch_unit_keys`], [`fmts_indexes`](crate::sector::KeyFetch::fmts_indexes)
/// drives [`fetch_fmts_indexes`]. One builder shared by every read path and
/// consumer — see docs/keysource.md for the cloning/memoization contract.
pub fn key_fetch(
    inputs: DiscInputs,
    make_sources: std::sync::Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync>,
) -> crate::sector::KeyFetch {
    // One driver behind both operations, memoized per sample-batch fingerprint
    // (keys are disc-level, so repeats hit cache). A genuinely-empty reply is
    // cached, but a source FAILURE is not — a transient miss (see `errored`).
    type FetchDriver = fn(&[Box<dyn KeySource>], &dyn ResolveCtx) -> FetchOutcome;
    fn make_op(
        inputs: DiscInputs,
        make_sources: std::sync::Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync>,
        drive: FetchDriver,
    ) -> crate::sector::KeyFetchFn {
        let cache: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<u64, Vec<[u8; 16]>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        std::sync::Arc::new(move |samples: &[Vec<u8>]| -> Vec<[u8; 16]> {
            let fp = {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                samples.len().hash(&mut h);
                for s in samples {
                    s.hash(&mut h);
                }
                h.finish()
            };
            if let Some(hit) = cache.lock().unwrap_or_else(|e| e.into_inner()).get(&fp) {
                return hit.clone();
            }
            let sources = make_sources();
            let mut di = inputs.clone();
            di.samples = samples.to_vec();
            // Parse Unit_Key_RO.inf at the disc's OWN stride (carried on `inputs`):
            // a V10 disc parses `enc_title_keys` at the 48-byte stride, so
            // hardcoding the V20 stride here would corrupt the derived unit keys.
            let ctx = DiscInputsCtx::new(&di);
            let outcome = drive(&sources, &ctx);
            let keys: Vec<[u8; 16]> = outcome.keys.into_iter().map(|u| u.key).collect();
            // Memoize a positive result always; memoize a NEGATIVE (empty) result
            // only when it is a genuine absence, never when a source errored — a
            // transient outage must not permanently poison this fingerprint.
            if !keys.is_empty() || !outcome.errored {
                cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(fp, keys.clone());
            }
            keys
        })
    }
    let unit = make_op(inputs.clone(), make_sources.clone(), drive_unit_keys);
    let fmts = make_op(inputs, make_sources, drive_fmts_indexes);
    crate::sector::KeyFetch::new(unit, fmts)
}

/// Read up to `n` ENCRYPTED 6144-byte aligned units from `title`'s body, raw (no
/// decrypt) — the content samples that populate [`DiscInputs::samples`] for a
/// key server to validate a candidate against, and that [`resolve_and_apply`]
/// hands to [`crate::Disc::decrypt_with`]. Lives in the library, not a
/// key-source crate: carving units is decryption *mechanism*. "Encrypted" is
/// the AACS CPI (`buf[0] & 0xc0`), NOT the `is_clean` TS-sync heuristic — see
/// docs/keysource.md for why, and the extent-probing strategy.
pub fn read_encrypted_units(
    reader: &mut dyn crate::sector::SectorSource,
    title: &crate::disc::DiscTitle,
    n: usize,
) -> Vec<Vec<u8>> {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    const CHUNK_UNITS: u32 = 15; // 45 sectors/read — under the drive transfer cap
    // Probe several evenly-spaced points across EACH extent, not just midpoint
    // forward: a title starting late or landing in a clear nav stretch must still
    // yield samples — empty samples make `decrypt_with` skip wrong-key validation.
    const PROBES_PER_EXTENT: u32 = 8;

    let mut out: Vec<Vec<u8>> = Vec::new();
    for ext in &title.extents {
        let total_units = ext.sector_count / ALIGNED_UNIT_SECTORS;
        if total_units == 0 {
            continue;
        }
        for p in 1..=PROBES_PER_EXTENT {
            // Probe at p/(P+1) of the extent — spreads P points across it while
            // skipping the clear nav at the very head.
            let unit = ((total_units as u64 * p as u64) / (PROBES_PER_EXTENT as u64 + 1)) as u32;
            if unit >= total_units {
                continue;
            }
            let units_this = CHUNK_UNITS.min(total_units - unit);
            // Saturate: start_lba comes from attacker-controlled UDF/MPLS extents;
            // near u32::MAX it would otherwise panic (debug) or wrap (release).
            // An over-capacity LBA fails cleanly via the is_err() skip below.
            let lba = ext
                .start_lba
                .saturating_add(unit.saturating_mul(ALIGNED_UNIT_SECTORS));
            let count = (units_this * ALIGNED_UNIT_SECTORS) as u16;
            let mut buf = vec![0u8; count as usize * 2048];
            // `false` = no recovery retries; reader is the raw drive/file (no
            // decrypt decorator). A read error skips only THAT probe — it must not
            // abandon the rest of the extent (the old `break` blinded the sampler).
            if reader.read_sectors(lba, count, &mut buf, false).is_err() {
                continue;
            }
            for i in 0..units_this as usize {
                let o = i * ALIGNED_UNIT_LEN;
                if o + ALIGNED_UNIT_LEN > buf.len() {
                    break;
                }
                let u = &buf[o..o + ALIGNED_UNIT_LEN];
                if aacs_unit_encrypted(u, title.content_format) {
                    out.push(u.to_vec());
                    if out.len() >= n {
                        return out;
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::types::UnitKey;
    use std::sync::{Arc, Mutex};

    fn units(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| vec![i as u8; 4]).collect()
    }

    // ── DecodeSampleSet: the online request can't be built under-sized ─────────

    /// Fewer than MIN_SAMPLE_UNITS → no set. Mutation: accepting a short slice
    /// resurrects the exact autorip bug (a 4-sample request silently skipped /
    /// read as "service down").
    #[test]
    fn decode_sample_set_rejects_under_min() {
        for n in 0..MIN_SAMPLE_UNITS {
            assert!(
                DecodeSampleSet::new(units(n)).is_none(),
                "{n} samples (< {MIN_SAMPLE_UNITS}) must not build a DecodeSampleSet"
            );
        }
    }

    /// Exactly the minimum, and above it, construct — and expose all samples.
    #[test]
    fn decode_sample_set_accepts_min_and_above() {
        let exact = DecodeSampleSet::new(units(MIN_SAMPLE_UNITS)).expect("min builds");
        assert_eq!(exact.len(), MIN_SAMPLE_UNITS);
        assert_eq!(exact.units().len(), MIN_SAMPLE_UNITS);
        assert!(!exact.is_empty());

        let more = DecodeSampleSet::new(units(MIN_SAMPLE_UNITS + 5)).expect("above min builds");
        assert_eq!(more.len(), MIN_SAMPLE_UNITS + 5);
    }

    /// The wrapped units round-trip byte-for-byte (the request carries exactly what
    /// was gathered — no reordering/truncation).
    #[test]
    fn decode_sample_set_preserves_units() {
        let raw = units(MIN_SAMPLE_UNITS);
        let set = DecodeSampleSet::new(raw.clone()).unwrap();
        assert_eq!(set.units(), raw.as_slice());
    }

    // ── KeySource default-method behaviour ────────────────────────────────────

    /// KeySource::host_certs() defaults to empty regardless of the MKB argument.
    /// Mutation guard: a non-empty default would inject phantom certs into the
    /// OEM handshake. See docs/keysource.md.
    #[test]
    fn key_source_host_certs_defaults_to_empty() {
        struct MinimalSource;
        impl KeySource for MinimalSource {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(Vec::new())
            }
        }
        let s = MinimalSource;
        assert!(s.host_certs(None).is_empty());
        assert!(s.host_certs(Some(68)).is_empty());
    }

    /// DiscInputsCtx maps DiscInputs faithfully: zero VID → None, non-zero VID →
    /// Some; title from volume_label; samples truncate to n; enc_title_keys
    /// parses Unit_Key_RO.inf at the version stride.
    #[test]
    fn disc_inputs_ctx_maps_fields() {
        // Build a minimal V10 Unit_Key_RO.inf with one key (stride 48):
        // uk_pos = 32, num_uk = 1, key at uk_pos + 48 = 80.
        let mut uk_ro = vec![0u8; 96];
        let uk_pos = 32usize;
        uk_ro[0..4].copy_from_slice(&(uk_pos as u32).to_be_bytes());
        uk_ro[uk_pos] = 0x00;
        uk_ro[uk_pos + 1] = 0x01; // num_unit_keys = 1
        let key_bytes = [0x7Eu8; 16];
        uk_ro[80..96].copy_from_slice(&key_bytes);

        let inputs = DiscInputs {
            disc_hash: "0xABC".into(),
            volume_id: [0u8; 16],
            version: crate::aacs::mkb::AACS_MAJOR_BD,
            mkb: vec![1, 2, 3],
            unit_key_ro: uk_ro,
            samples: vec![vec![9u8; 4], vec![8u8; 4], vec![7u8; 4]],
            volume_label: Some("TITLE_X".into()),
        };

        // Zero VID → None.
        let ctx = DiscInputsCtx::new(&inputs);
        assert_eq!(ctx.disc_hash(), "0xABC");
        assert_eq!(ctx.title(), Some("TITLE_X"));
        assert!(ctx.vid().is_none(), "all-zero VID is the no-VID sentinel");
        assert_eq!(ctx.mkb().unwrap(), &[1, 2, 3]);
        assert_eq!(ctx.enc_title_keys().unwrap(), &[key_bytes]);
        assert_eq!(ctx.samples(2).unwrap().len(), 2, "samples truncates to n");

        // Non-zero VID → Some(vid).
        let mut inputs2 = inputs.clone();
        inputs2.volume_id = [0x42u8; 16];
        let ctx2 = DiscInputsCtx::new(&inputs2);
        assert_eq!(ctx2.vid(), Some(Vid([0x42u8; 16])));
    }

    /// `resolve_and_apply_traced` records each step's `who` as the source's own
    /// `label()`, carried verbatim — no enum round-trip. A source with a custom
    /// label surfaces it as-is in the trace.
    #[test]
    fn trace_who_is_the_source_label_verbatim() {
        struct LabeledSource(&'static str);
        impl KeySource for LabeledSource {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(Vec::new())
            }
            fn label(&self) -> &'static str {
                self.0
            }
        }
        let mut disc = crate::Disc {
            volume_id: String::new(),
            meta_title: None,
            format: crate::DiscFormat::BluRay,
            capacity_sectors: 0,
            capacity_bytes: 0,
            layers: 1,
            titles: Vec::new(),
            region: crate::disc::DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            aacs_error: None,
            css_error: None,
            content_format: crate::ContentFormat::BdTs,
        };
        let inputs = DiscInputs {
            disc_hash: "0x00".into(),
            volume_id: [0u8; 16],
            version: crate::aacs::mkb::AACS_MAJOR_UHD,
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        let sources: Vec<Box<dyn KeySource>> = vec![
            Box::new(LabeledSource("keydb")),
            Box::new(LabeledSource("my-custom-source")),
        ];
        let (_ok, trace) = resolve_and_apply_traced(&sources, &inputs, &mut disc);
        let whos: Vec<&str> = trace.keys.iter().map(|s| s.who.as_str()).collect();
        assert_eq!(whos, vec!["keydb", "my-custom-source"]);
    }

    // ── fetch_unit_keys / key_fetch (the one shared fetch path) ───────────────

    fn empty_inputs() -> DiscInputs {
        DiscInputs {
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            version: crate::aacs::mkb::AACS_MAJOR_UHD,
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        }
    }

    struct EmptySource;
    impl KeySource for EmptySource {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(Vec::new())
        }
    }
    struct ErroringSource;
    impl KeySource for ErroringSource {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Err(Error::AacsNoKeys)
        }
    }
    struct HasKey([u8; 16]);
    impl KeySource for HasKey {
        fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(vec![UnitKey::new(0, self.0)])
        }
    }

    /// `fetch_unit_keys` returns the FIRST source's non-empty keys, skipping a
    /// source that returns empty or errors; empty when no source answers.
    #[test]
    fn fetch_unit_keys_first_nonempty_skips_empty_and_errors() {
        let inputs = empty_inputs();
        let ctx = DiscInputsCtx::new(&inputs);
        let key = [0xABu8; 16];

        let sources: Vec<Box<dyn KeySource>> = vec![
            Box::new(EmptySource),
            Box::new(ErroringSource),
            Box::new(HasKey(key)),
        ];
        let got = fetch_unit_keys(&sources, &ctx);
        assert_eq!(got.len(), 1, "the first source that answers wins");
        assert_eq!(got[0].key, key);

        let none: Vec<Box<dyn KeySource>> = vec![Box::new(EmptySource), Box::new(ErroringSource)];
        assert!(
            fetch_unit_keys(&none, &ctx).is_empty(),
            "no source answers ⇒ empty"
        );
    }

    /// `key_fetch` builds a closure that runs the sources with the GIVEN failing
    /// samples and returns their keys — the exact bytes are forwarded to the
    /// source, and `make_sources` is invoked per call.
    #[test]
    fn key_fetch_closure_forwards_samples_and_returns_keys() {
        let key = [0x5au8; 16];
        let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let builds = Arc::new(Mutex::new(0usize));

        struct Probe {
            key: [u8; 16],
            seen: Arc<Mutex<Vec<Vec<u8>>>>,
        }
        impl KeySource for Probe {
            fn get_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                if let Ok(s) = ctx.samples(8) {
                    self.seen.lock().unwrap().extend(s);
                }
                Ok(vec![UnitKey::new(0, self.key)])
            }
        }

        let seen_c = Arc::clone(&seen);
        let builds_c = Arc::clone(&builds);
        let make: Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync> = Arc::new(move || {
            *builds_c.lock().unwrap() += 1;
            vec![Box::new(Probe {
                key,
                seen: Arc::clone(&seen_c),
            }) as Box<dyn KeySource>]
        });

        let cb = key_fetch(empty_inputs(), make);
        let samples = vec![vec![0xEEu8; crate::aacs::content::ALIGNED_UNIT_LEN]];
        let got = cb.unit_keys(&samples);
        assert_eq!(
            got,
            vec![key],
            "the source's key flows back through the closure"
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "the failing ciphertext sample is forwarded to the source"
        );
        assert_eq!(*builds.lock().unwrap(), 1, "make_sources invoked per fetch");
    }

    /// `key_fetch` memoizes each operation by the fingerprint of the sample batch:
    /// identical samples reuse the cached keys (no rebuild), different samples miss,
    /// the two operations keep independent caches, and even an empty reply is cached.
    #[test]
    fn key_fetch_memoizes_per_op_by_sample_fingerprint() {
        let builds = Arc::new(Mutex::new(0usize));
        let builds_c = Arc::clone(&builds);
        let key = [0x11u8; 16];
        let make: Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync> = Arc::new(move || {
            *builds_c.lock().unwrap() += 1;
            vec![Box::new(HasKey(key)) as Box<dyn KeySource>]
        });
        let cb = key_fetch(empty_inputs(), make);
        let a = vec![vec![0xAAu8; 8]];
        let b = vec![vec![0xBBu8; 8]];

        // First resolve for `a` builds sources; the identical repeat is cached.
        assert_eq!(cb.unit_keys(&a), vec![key]);
        assert_eq!(cb.unit_keys(&a), vec![key]);
        assert_eq!(
            *builds.lock().unwrap(),
            1,
            "identical samples reuse the cache"
        );

        // A different sample batch is a cache miss → one more build.
        assert_eq!(cb.unit_keys(&b), vec![key]);
        assert_eq!(
            *builds.lock().unwrap(),
            2,
            "different samples miss the cache"
        );

        // The forensic op has its OWN cache (HasKey has no forensic keys → empty),
        // so `a` builds once more here; its empty reply is then cached too.
        assert!(cb.fmts_indexes(&a).is_empty());
        assert_eq!(
            *builds.lock().unwrap(),
            3,
            "unit/fmts caches are independent"
        );
        assert!(cb.fmts_indexes(&a).is_empty());
        assert_eq!(
            *builds.lock().unwrap(),
            3,
            "an empty reply is cached, not re-asked"
        );
    }

    /// A transient source outage must NOT be memoized as "no key" — a fingerprint
    /// whose first fetch errored must be re-asked once recovered. Regression
    /// guard; see docs/keysource.md.
    #[test]
    fn errored_empty_is_not_cached_and_retries_when_source_recovers() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let key = [0x77u8; 16];
        // Shared across every `make_sources()` rebuild: call 0 errors (source
        // down), every later call succeeds (source recovered).
        let calls = Arc::new(AtomicUsize::new(0));

        struct Flaky {
            calls: Arc<AtomicUsize>,
            key: [u8; 16],
        }
        impl KeySource for Flaky {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(Error::AacsNoKeys) // first attempt: source unreachable
                } else {
                    Ok(vec![UnitKey::new(0, self.key)])
                }
            }
        }

        let calls_c = Arc::clone(&calls);
        let make: Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync> = Arc::new(move || {
            vec![Box::new(Flaky {
                calls: Arc::clone(&calls_c),
                key,
            }) as Box<dyn KeySource>]
        });

        let cb = key_fetch(empty_inputs(), make);
        let samples = vec![vec![0xCDu8; 8]];

        // First fetch: the source errors → empty, but the miss must NOT be cached.
        assert!(
            cb.unit_keys(&samples).is_empty(),
            "source down → empty this time"
        );
        // Second fetch, SAME samples: not blocked by a cached empty → the now-
        // recovered source resolves the key.
        assert_eq!(
            cb.unit_keys(&samples),
            vec![key],
            "recovered source resolves — errored empty was not memoized"
        );
    }

    /// A GENUINE absence (a source that runs and returns an empty `Ok`) is still
    /// memoized — the benefit the fix preserves. A source counting its calls must
    /// be asked exactly once for a fingerprint whose first (clean) reply was empty.
    #[test]
    fn genuine_empty_is_still_memoized() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));

        struct AlwaysEmpty {
            calls: Arc<AtomicUsize>,
        }
        impl KeySource for AlwaysEmpty {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new()) // ran fine, genuinely holds no key
            }
        }

        let calls_c = Arc::clone(&calls);
        let make: Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync> = Arc::new(move || {
            vec![Box::new(AlwaysEmpty {
                calls: Arc::clone(&calls_c),
            }) as Box<dyn KeySource>]
        });

        let cb = key_fetch(empty_inputs(), make);
        let samples = vec![vec![0xEFu8; 8]];

        assert!(cb.unit_keys(&samples).is_empty());
        assert!(cb.unit_keys(&samples).is_empty());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a clean empty reply is cached — the source is asked only once"
        );
    }

    /// The two `KeyFetch` operations route to the two DISTINCT trait methods
    /// (`unit_keys` → `get_unit_keys`, `fmts_indexes` → `get_fmts_indexes`), not
    /// an overload keyed on return length. See docs/keysource.md.
    #[test]
    fn key_fetch_routes_unit_and_fmts_to_distinct_source_methods() {
        const BASE: [u8; 16] = [0xB0; 16];
        const F1: [u8; 16] = [0xF1; 16];
        const F2: [u8; 16] = [0xF2; 16];

        struct TwoOp;
        impl KeySource for TwoOp {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(vec![UnitKey::new(0, BASE)])
            }
            fn get_fmts_indexes(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(vec![UnitKey::new(0, F1), UnitKey::new(1, F2)])
            }
        }

        let make: Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync> =
            Arc::new(|| vec![Box::new(TwoOp) as Box<dyn KeySource>]);
        let cb = key_fetch(empty_inputs(), make);
        let samples = vec![vec![0x01u8; 4]];

        assert_eq!(
            cb.unit_keys(&samples),
            vec![BASE],
            "unit_keys resolves the base Unit Key via get_unit_keys"
        );
        assert_eq!(
            cb.fmts_indexes(&samples),
            vec![F1, F2],
            "fmts_indexes resolves the forensic set (any length) via get_fmts_indexes"
        );
    }

    /// `KeyFetch::unit_only` serves base keys but NEVER a forensic set — the
    /// contract the sweep/patch recovery decorator relies on (it resolves CPS
    /// units only). Its `fmts_indexes` is unconditionally empty.
    #[test]
    fn key_fetch_unit_only_never_serves_forensic() {
        let f = crate::sector::KeyFetch::unit_only(std::sync::Arc::new(|_| vec![[0xAA; 16]]));
        assert_eq!(f.unit_keys(&[vec![0u8; 4]]), vec![[0xAA; 16]]);
        assert!(
            f.fmts_indexes(&[vec![0u8; 4]]).is_empty(),
            "unit_only resolver yields no forensic keys"
        );
    }

    /// `get_fmts_indexes` defaults to empty, so a base-only source (a keydb) opts
    /// out of the forensic path without implementing it. `fetch_fmts_indexes` then
    /// falls through to the next source, exactly like the unit-key driver.
    #[test]
    fn fetch_fmts_indexes_skips_default_optout_source() {
        struct BaseOnly; // uses the default (empty) get_fmts_indexes
        impl KeySource for BaseOnly {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(vec![UnitKey::new(0, [0x11; 16])])
            }
        }
        struct Forensic;
        impl KeySource for Forensic {
            fn get_unit_keys(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(Vec::new())
            }
            fn get_fmts_indexes(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                Ok(vec![UnitKey::new(0, [0x77; 16])])
            }
        }
        let inputs = empty_inputs();
        let ctx = DiscInputsCtx::new(&inputs);
        let sources: Vec<Box<dyn KeySource>> = vec![Box::new(BaseOnly), Box::new(Forensic)];
        let got = fetch_fmts_indexes(&sources, &ctx);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, [0x77; 16], "the base-only source is skipped");
    }

    /// #4 regression: content NOT at the extent midpoint (late-starting, or
    /// midpoint in clear nav) must still be sampled. The old midpoint-forward
    /// sampler returned empty; see docs/keysource.md.
    #[test]
    fn read_encrypted_units_finds_scrambled_content_off_the_midpoint() {
        use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
        use crate::error::Result;
        use crate::sector::SectorSource;

        // Units in the FIRST SIXTH of the extent are scrambled (0xFF → no TS
        // sync); everything else (incl. the midpoint) is clear (0x47 syncs).
        struct BandSource {
            ext_start: u32,
            total_units: u32,
        }
        impl SectorSource for BandSource {
            fn capacity_sectors(&self) -> u32 {
                self.ext_start + self.total_units * ALIGNED_UNIT_SECTORS + 64
            }
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _r: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                for (i, chunk) in buf[..bytes].chunks_mut(ALIGNED_UNIT_LEN).enumerate() {
                    if chunk.len() < ALIGNED_UNIT_LEN {
                        break;
                    }
                    let abs_unit = (lba - self.ext_start) / ALIGNED_UNIT_SECTORS + i as u32;
                    if abs_unit < self.total_units / 6 {
                        chunk.fill(0xFF); // scrambled: no TS sync
                    } else {
                        chunk.fill(0);
                        let mut o = 4;
                        while o < ALIGNED_UNIT_LEN {
                            chunk[o] = 0x47; // clear TS syncs
                            o += 192;
                        }
                    }
                }
                Ok(bytes)
            }
        }

        let total_units = 600u32;
        let ext_start = 1000u32;
        let mut src = BandSource {
            ext_start,
            total_units,
        };
        let title = crate::disc::DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![crate::disc::Extent {
                start_lba: ext_start,
                sector_count: total_units * ALIGNED_UNIT_SECTORS,
            }],
            content_format: crate::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        };

        let samples = read_encrypted_units(&mut src, &title, 4);
        assert!(
            !samples.is_empty(),
            "the probe-spread must sample the early scrambled band the midpoint misses"
        );
        for s in &samples {
            assert!(
                aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs),
                "every sample is a CPI-flagged encrypted unit (byte0 & 0xC0 != 0)"
            );
        }
    }

    /// DISCRIMINATING: selection is by AACS CPI (byte 0), NOT TS-sync clarity —
    /// half the units lack TS syncs but are CPI-clear (genuinely unencrypted).
    /// Must return ONLY CPI-flagged units. See docs/keysource.md.
    #[test]
    fn read_encrypted_units_selects_by_cpi_not_ts_sync() {
        use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
        use crate::error::Result;
        use crate::sector::SectorSource;

        // Even units: CPI-clear, sync-destroyed. Odd units: CPI-set, scrambled body.
        // Neither has clean TS syncs (`is_clean` is FALSE for both), but
        // `aacs_unit_encrypted` must flag only the odd (CPI-set) units.
        struct MixSource {
            ext_start: u32,
            total_units: u32,
        }
        impl SectorSource for MixSource {
            fn capacity_sectors(&self) -> u32 {
                self.ext_start + self.total_units * ALIGNED_UNIT_SECTORS + 64
            }
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _r: bool,
            ) -> Result<usize> {
                let bytes = count as usize * 2048;
                for (i, chunk) in buf[..bytes].chunks_mut(ALIGNED_UNIT_LEN).enumerate() {
                    if chunk.len() < ALIGNED_UNIT_LEN {
                        break;
                    }
                    let abs = (lba - self.ext_start) / ALIGNED_UNIT_SECTORS + i as u32;
                    if abs.is_multiple_of(2) {
                        chunk.fill(0x11); // CPI-clear (0x11 & 0xC0 == 0), no TS sync
                    } else {
                        chunk.fill(0xAB); // scrambled body (no TS sync)
                        chunk[0] = 0xC0; // CPI set -> encrypted
                    }
                }
                Ok(bytes)
            }
        }

        let total_units = 400u32;
        let ext_start = 500u32;
        let mut src = MixSource {
            ext_start,
            total_units,
        };
        let title = crate::disc::DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: vec![crate::disc::Extent {
                start_lba: ext_start,
                sector_count: total_units * ALIGNED_UNIT_SECTORS,
            }],
            content_format: crate::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        };

        let samples = read_encrypted_units(&mut src, &title, 8);
        assert!(
            !samples.is_empty(),
            "the CPI-flagged (odd) units must still be collected"
        );
        for s in &samples {
            assert!(
                aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs),
                "only CPI-flagged units are selected"
            );
            assert_eq!(
                s[0] & 0xC0,
                0xC0,
                "a CPI-clear sync-destroyed unit must never be sampled"
            );
        }
    }

    /// Audit #5 — DISCRIMINATING test for the version→stride fix: a 2-key
    /// `Unit_Key_RO.inf` whose 2nd key sits at the V20 offset, so a V10 parse
    /// reads a DIFFERENT region. See docs/keysource.md.
    #[test]
    fn disc_inputs_ctx_parses_unit_keys_at_the_version_stride() {
        use crate::aacs::mkb::{AACS_MAJOR_BD, AACS_MAJOR_UHD};
        const UK_POS: usize = 64;
        let mut inf = vec![0u8; 200];
        inf[0..4].copy_from_slice(&(UK_POS as u32).to_be_bytes()); // uk_pos
        inf[UK_POS..UK_POS + 2].copy_from_slice(&2u16.to_be_bytes()); // num_uk = 2
        let key0_at = UK_POS + 48; // first key — same for both strides
        let key1_v10_at = key0_at + 48; // second key if parsed at V10 stride
        let key1_v20_at = key0_at + 64; // second key if parsed at V20 stride
        inf[key0_at..key0_at + 16].fill(0xA0);
        inf[key1_v10_at..key1_v10_at + 16].fill(0x10);
        inf[key1_v20_at..key1_v20_at + 16].fill(0x20);

        let base = DiscInputs {
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            version: AACS_MAJOR_UHD,
            mkb: Vec::new(),
            unit_key_ro: inf,
            samples: Vec::new(),
            volume_label: None,
        };
        let k20 = DiscInputsCtx::new(&base).enc_title_keys().unwrap().to_vec();
        let v10_inputs = DiscInputs {
            version: AACS_MAJOR_BD,
            ..base.clone()
        };
        let k10 = DiscInputsCtx::new(&v10_inputs)
            .enc_title_keys()
            .unwrap()
            .to_vec();

        assert_eq!(k20.len(), 2);
        assert_eq!(k10.len(), 2);
        assert_eq!(k20[0], [0xA0; 16], "first key is at +48 for both strides");
        assert_eq!(k10[0], [0xA0; 16]);
        assert_eq!(k20[1], [0x20; 16], "V20 reads the 2nd key at +64");
        assert_eq!(k10[1], [0x10; 16], "V10 reads the 2nd key at +48");
        assert_ne!(k20[1], k10[1], "the parse stride follows inputs.version");
    }

    /// `DiscInputs` is public; a derived `Debug` printed the Volume ID, the
    /// whole `Unit_Key_RO.inf`, MKB and every sample verbatim. Sentinel 0xD5 =
    /// 213. Mutation guard: restoring `#[derive(Debug)]` fails this.
    #[test]
    fn disc_inputs_debug_is_redacted() {
        let inputs = DiscInputs {
            disc_hash: "0xAA".into(),
            volume_id: [0xD5; 16],
            version: 2,
            mkb: vec![0xD5; 64],
            unit_key_ro: vec![0xD5; 48],
            samples: vec![vec![0xD5; 6144]],
            volume_label: Some("TITLE_2024".into()),
        };
        let dbg = format!("{inputs:?}");
        assert!(
            !dbg.contains("213"),
            "DiscInputs Debug leaked key material (decimal 213): {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "DiscInputs Debug missing redaction marker: {dbg}"
        );
        // Non-secret identity and shape stay printable for diagnostics.
        assert!(dbg.contains("0xAA"), "{dbg}");
        assert!(dbg.contains("mkb_len: 64"), "{dbg}");
        assert!(dbg.contains("unit_key_ro_len: 48"), "{dbg}");
        assert!(dbg.contains("samples_len: 1"), "{dbg}");
        assert!(dbg.contains("TITLE_2024"), "{dbg}");
    }

    /// `DecodeSampleSet` wraps the same on-disc ciphertext `DiscInputs` redacts;
    /// a derived `Debug` would dump it verbatim. Sentinel 0xD5 = 213, matching
    /// the `DiscInputs` test above. See docs/keysource.md.
    #[test]
    fn decode_sample_set_debug_is_redacted() {
        let set = DecodeSampleSet::new(vec![vec![0xD5; 6144]; MIN_SAMPLE_UNITS])
            .expect("MIN_SAMPLE_UNITS units is a valid set");
        let dbg = format!("{set:?}");
        assert!(
            !dbg.contains("213"),
            "DecodeSampleSet Debug leaked ciphertext (decimal 213): {dbg}"
        );
        assert!(
            dbg.contains("redacted"),
            "DecodeSampleSet Debug missing redaction marker: {dbg}"
        );
        // Non-secret shape stays printable for diagnostics.
        assert!(dbg.contains("units_len: 8"), "{dbg}");
    }
}

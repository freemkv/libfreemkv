//! Key sources — the layer that hands libfreemkv a disc's terminal Unit Keys.
//!
//! libfreemkv performs NO key lookup. An application resolves a disc's keys through one or more
//! [`KeySource`]s, each an adapter over a backing store (a keydb file, a key server, the
//! mapfile cache) that returns the disc's terminal **Unit Keys**
//! ([`crate::aacs::types::UnitKey`]), orchestrating derivation via the [`ResolveCtx`] handed to
//! it and libfreemkv's own crypto primitives. libfreemkv owns the crypto; a source owns only
//! PATH ORCHESTRATION.

use crate::aacs::types::HostCert;
use crate::aacs::types::{UnitKey, Vid};
use crate::error::Error;

/// Minimum encrypted-content unit samples a single online key request must carry.
///
/// The key service identifies a key by which of the submitted units it decrypts, so too few
/// samples can return a key that matches an incidental unit rather than the one asked about (a
/// false positive); this many make a request unambiguous. Canonical here so both
/// `freemkv-keysources`'s online source and libfreemkv's own FMTS forensic query
/// ([`crate::mux`]) agree on one value.
pub const MIN_SAMPLE_UNITS: usize = 8;

/// A set of encrypted content-unit samples PROVEN to carry at least
/// [`MIN_SAMPLE_UNITS`] units — the online `/decode` request's proof-of-ownership.
///
/// "Parse, don't validate": the only constructor, [`DecodeSampleSet::new`], returns `None` for
/// an under-sized slice, so an online key request simply *cannot be built* from too few samples
/// — a compile-time obligation, not a runtime check a caller can forget.
#[derive(Clone)]
pub struct DecodeSampleSet(Vec<Vec<u8>>);

impl std::fmt::Debug for DecodeSampleSet {
    // Prints SHAPE only — a derived Debug dumped multi-MB of ciphertext verbatim into any log
    // that formats it.
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

// Redacting Debug: a derived impl used to print the Volume ID, the whole Unit_Key_RO.inf, the
// MKB and every ciphertext sample verbatim into a bug report's log.
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
    /// The disc's encrypted title keys, parsed from `Unit_Key_RO.inf` (or an HD DVD
    /// VTKF) the same way the library's resolver parses them ([`crate::aacs::inf::parse_title_keys`]),
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
    /// The op's Stop token (stop design §2.7): a source waits on it, never past a Stop.
    /// `None` for a ctx built with no token. Defaulted so foreign impls compile unchanged.
    fn halt(&self) -> Option<&crate::halt::Halt> {
        None
    }
    /// The op's [`Progress`](crate::halt::Progress) (§2.7, T29): a source bumps it per
    /// byte moved and holds [`busy`](crate::halt::Progress::busy) while a call is in
    /// flight. `None` when nothing watches the op. Defaulted like [`Self::halt`].
    fn progress(&self) -> Option<&crate::halt::Progress> {
        None
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
    halt: Option<&'a crate::halt::Halt>,
    progress: Option<&'a crate::halt::Progress>,
}

impl<'a> DiscInputsCtx<'a> {
    /// Build a context over `inputs`, parsing the encrypted title keys at the
    /// stride for the disc's own AACS major (`inputs.version`: 1 → 48-byte V10
    /// stride, else 64-byte V20/V21) — the single source of truth, no separate
    /// version argument to drift from it. An HD DVD `VTKF*.AACS` is detected by
    /// its magic ([`crate::aacs::inf::parse_title_keys`]).
    ///
    /// A malformed `unit_key_ro` parses to an empty key set rather than an error.
    pub fn new(inputs: &'a DiscInputs) -> Self {
        use crate::aacs::inf::parse_title_keys;
        use crate::aacs::mkb::AacsVersion;
        let enc_keys = if inputs.unit_key_ro.is_empty() {
            Vec::new()
        } else {
            parse_title_keys(&inputs.unit_key_ro, AacsVersion::from_major(inputs.version))
                .map(|f| f.encrypted_keys.into_iter().map(|(_, k)| k).collect())
                .unwrap_or_default()
        };
        Self {
            inner: inputs,
            enc_keys,
            halt: None,
            progress: None,
        }
    }

    /// The ctx the `keys` module hands every source (§2.12): `halt` and `progress`
    /// come back from [`ResolveCtx::halt`] and [`ResolveCtx::progress`].
    pub(crate) fn with_stop(
        self,
        halt: Option<&'a crate::halt::Halt>,
        progress: Option<&'a crate::halt::Progress>,
    ) -> Self {
        Self {
            halt,
            progress,
            ..self
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
    fn halt(&self) -> Option<&crate::halt::Halt> {
        self.halt
    }
    fn progress(&self) -> Option<&crate::halt::Progress> {
        self.progress
    }
}

/// The de-conflated result of a source's unit-key resolution: the keys it
/// produced (empty when none) PLUS why it produced them — so a caller can tell
/// a genuine "no entry for this disc" from a MATCHED disc that yielded no
/// derivable key. The bare `Vec` of [`KeySource::get_unit_keys`] cannot make
/// that distinction; [`KeySource::resolve_unit_keys`] carries it.
#[derive(Debug, Clone, Default)]
pub struct UnitKeyResolution {
    /// Terminal Unit Keys produced (empty = this source yielded none).
    pub keys: Vec<UnitKey>,
    /// The source matched this disc in its store (by hash / VID), even if it
    /// then derived no key. `false` = a genuine miss ("no entry").
    pub matched: bool,
    /// When `matched` and `keys` is empty: the derivation nodes walked AFTER the
    /// implicit `MatchedDisc` node — e.g. `[NoVid]` (had a Media Key, no VID) or
    /// `[NoDerivableKey]` (no material at all). Empty ⇒ the caller supplies a
    /// bare `NoDerivableKey`. Rendered only when `matched` and `keys` is empty; a `NoVid`
    /// beside partial keys still tells `resolve` the VID would help (KU J23).
    pub miss_path: Vec<crate::aacs::trace::KeyNode>,
    /// When `matched`: a booleans-and-lengths shape of the matched entry (no key
    /// material), for the application to log. `None` for a miss or a source kind
    /// with no such shape.
    pub matched_entry: Option<crate::aacs::trace::MatchedEntry>,
    /// Number of per-disc entries loaded in this source's store, when known — so
    /// a true-miss verdict can name the store size. `None` for a source that
    /// carries no such count.
    pub store_entries: Option<usize>,
}

/// A key source: an adapter over a backing store that resolves a disc's terminal
/// Unit Keys.
///
/// Dumb about *policy*, smart about *its own material*: given a [`ResolveCtx`] a source
/// orchestrates derivation down to Unit Keys using the library's boil-down crypto — never
/// re-implementing AES. Two explicit resolve ops, one per key kind:
/// [`get_unit_keys`](Self::get_unit_keys) (base per-CPS-unit),
/// [`get_fmts_indexes`](Self::get_fmts_indexes) (AACS 2.1 forensic set).
pub trait KeySource {
    /// Resolve this disc's base per-CPS-unit Unit Keys from this source. An empty
    /// `Vec` is a genuine "no key here"; `Err` is a source failure.
    fn get_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error>;

    /// De-conflated counterpart to [`get_unit_keys`](Self::get_unit_keys):
    /// besides the keys, report whether this disc MATCHED the source's store and
    /// (on a keyless match) why nothing was derivable — so the trace can render
    /// `matched disc > no VID > NO KEY` instead of a flat `no entry`.
    ///
    /// The default is coarse: forward to [`get_unit_keys`](Self::get_unit_keys)
    /// and report `matched = false`. A source that keys on a per-disc identity
    /// (a keydb) overrides this to set `matched`/`miss_path`/`matched_entry`.
    fn resolve_unit_keys(&self, ctx: &dyn ResolveCtx) -> Result<UnitKeyResolution, Error> {
        Ok(UnitKeyResolution {
            keys: self.get_unit_keys(ctx)?,
            ..Default::default()
        })
    }

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
    /// SCSI mutual-auth handshake (the OEM/AACS baseline route), best first.
    /// libfreemkv passes `None` for `mkb`: the argument is reserved and will be
    /// removed at the next breaking window. A host cert unlocks the
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

    /// Whether this source's answer depends on the content samples it is sent. `true` (the
    /// default): `ResolvedKeySet::resolve` asks it once per unopened piece, with that piece's
    /// samples. `false` (a keydb keyed by disc hash): asked once per resolve (KU §2.3 step 8).
    fn answer_depends_on_samples(&self) -> bool {
        true
    }

    /// Whether this source's most recent failed request got NO answer at all (DNS, connect or
    /// idle stall: transport class). Only then does `resolve` retry it, until 60 s pass with no
    /// answer (KU §2.3 step 8, J13, J15). Default `false`: never retried, so a source that
    /// answered (a 5xx, 401, 429) is never asked twice.
    fn last_failure_was_transport(&self) -> bool {
        false
    }

    /// Whether this source derives keys from the disc's Volume ID it is sent (an online key
    /// service: Kvu = AES-G(Km, IDv), KS-16), so a Missing piece might open with the VID in
    /// hand (KU J23, E7034). Default `false`.
    fn uses_vid(&self) -> bool {
        false
    }
}

/// Read up to `n` ENCRYPTED 6144-byte aligned units from `title`'s body, raw (no decrypt) — the
/// content samples that populate [`DiscInputs::samples`] for a key server to validate a
/// candidate against.
/// Lives in the library, not a key-source crate: carving units is decryption *mechanism*.
/// "Encrypted" is the AACS CPI (`buf[0] & 0xc0`), NOT the `is_clean` TS-sync heuristic.
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
        // First unit not yet covered by an earlier probe: on a small extent the
        // probe windows overlap, and a re-read unit would be a duplicate sample.
        let mut next_unit = 0u32;
        for p in 1..=PROBES_PER_EXTENT {
            // Probe at p/(P+1) of the extent — spreads P points across it while
            // skipping the clear nav at the very head.
            let probe = ((total_units as u64 * p as u64) / (PROBES_PER_EXTENT as u64 + 1)) as u32;
            let unit = probe.max(next_unit);
            if unit >= total_units {
                continue;
            }
            let units_this = CHUNK_UNITS.min(total_units - unit);
            next_unit = unit + units_this;
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

    /// KeySource::host_certs() defaults to empty regardless of the MKB argument. Mutation
    /// guard: a non-empty default would inject phantom certs into the OEM handshake.
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

    // ── fetch_unit_keys (the one shared fetch path) ───────────────────────────

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

    /// #4 regression: content NOT at the extent midpoint (late-starting, or midpoint in clear
    /// nav) must still be sampled. The old midpoint-forward sampler returned empty.
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

    /// DISCRIMINATING: selection is by AACS CPI (byte 0), NOT TS-sync clarity — half the units
    /// lack TS syncs but are CPI-clear (genuinely unencrypted). Must return ONLY CPI-flagged
    /// units.
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

    /// Audit #5 — DISCRIMINATING test for the version→stride fix: a 2-key `Unit_Key_RO.inf`
    /// whose 2nd key sits at the V20 offset, so a V10 parse reads a DIFFERENT region.
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

    /// `DecodeSampleSet` wraps the same on-disc ciphertext `DiscInputs` redacts; a derived
    /// `Debug` would dump it verbatim. Sentinel 0xD5 = 213, matching the `DiscInputs` test
    /// above.
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

    /// A source whose every unit is CPI-encrypted, for exercising the count cap
    /// and extent-skip logic without the clarity/CPI selection getting in the way.
    struct AllEncryptedSource {
        cap: u32,
    }
    impl crate::sector::SectorSource for AllEncryptedSource {
        fn capacity_sectors(&self) -> u32 {
            self.cap
        }
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _r: bool,
        ) -> crate::error::Result<usize> {
            let bytes = count as usize * 2048;
            // 0xC0 in byte 0 of every unit → CPI-encrypted for BdTs.
            buf[..bytes].fill(0xC0);
            Ok(bytes)
        }
    }

    fn title_with_extents(extents: Vec<crate::disc::Extent>) -> crate::disc::DiscTitle {
        crate::disc::DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents,
            content_format: crate::disc::ContentFormat::BdTs,
            codec_privates: Vec::new(),
        }
    }

    /// `read_encrypted_units` returns AT MOST `n` units and stops as soon as it
    /// has them — an all-encrypted extent must not spill every probe's worth of
    /// samples when the caller asked for a few.
    #[test]
    fn read_encrypted_units_returns_at_most_n() {
        use crate::aacs::content::{ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
        let total_units = 600u32;
        let ext_start = 1000u32;
        let mut src = AllEncryptedSource {
            cap: ext_start + total_units * ALIGNED_UNIT_SECTORS + 64,
        };
        let title = title_with_extents(vec![crate::disc::Extent {
            start_lba: ext_start,
            sector_count: total_units * ALIGNED_UNIT_SECTORS,
        }]);

        for n in [1usize, 3, 8] {
            let samples = read_encrypted_units(&mut src, &title, n);
            assert_eq!(samples.len(), n, "must return exactly the {n} requested");
            for s in &samples {
                assert!(aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs));
            }
        }
    }

    /// An extent too small to hold a single 3-sector aligned unit yields zero
    /// units and must be SKIPPED, with sampling continuing into the next extent —
    /// not abandoned, and not a panic on the empty extent.
    #[test]
    fn read_encrypted_units_skips_a_zero_unit_extent_and_samples_the_next() {
        use crate::aacs::content::{ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
        let good_units = 400u32;
        let good_start = 5000u32;
        let mut src = AllEncryptedSource {
            cap: good_start + good_units * ALIGNED_UNIT_SECTORS + 64,
        };
        // Extent 0: only 2 sectors — fewer than one 3-sector aligned unit → 0 units.
        // Extent 1: a normal, sampleable extent.
        let title = title_with_extents(vec![
            crate::disc::Extent {
                start_lba: 10,
                sector_count: ALIGNED_UNIT_SECTORS - 1,
            },
            crate::disc::Extent {
                start_lba: good_start,
                sector_count: good_units * ALIGNED_UNIT_SECTORS,
            },
        ]);

        let samples = read_encrypted_units(&mut src, &title, 4);
        assert_eq!(
            samples.len(),
            4,
            "the empty first extent is skipped; the second extent supplies the samples"
        );
        for s in &samples {
            assert!(aacs_unit_encrypted(s, crate::disc::ContentFormat::BdTs));
        }
    }

    // ── codeaudit keys cluster ────────────────────────────────────────────────

    // HD DVD: `unit_key_ro` carries a VTKF (DVD_HD_V_TKF); the ctx must parse it
    // with the magic-dispatching parser, not the BD-only Unit_Key_RO layout.
    #[test]
    fn disc_inputs_ctx_parses_hddvd_vtkf_title_keys() {
        use crate::aacs::inf::VTKF_MAGIC;
        let key = [0x3Cu8; 16];
        let mut vtkf = Vec::new();
        vtkf.extend_from_slice(VTKF_MAGIC);
        vtkf.resize(0x80, 0);
        let mut entry = [0u8; 0x24];
        entry[0] = 0x80; // AV_FLG: present
        entry[4..20].copy_from_slice(&key);
        vtkf.extend_from_slice(&entry);
        vtkf.resize(0x80 + 64 * 0x24, 0);
        let mut inputs = empty_inputs();
        inputs.version = crate::aacs::mkb::AACS_MAJOR_BD;
        inputs.unit_key_ro = vtkf;
        let ctx = DiscInputsCtx::new(&inputs);
        assert_eq!(ctx.enc_title_keys().unwrap(), &[key]);
    }

    // A small extent's probe windows overlap: each aligned unit must still be
    // sampled at most once (duplicates dilute the request's evidence).
    #[test]
    fn read_encrypted_units_never_returns_the_same_unit_twice() {
        use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS};
        struct Stamped;
        impl crate::sector::SectorSource for Stamped {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                _r: bool,
            ) -> crate::error::Result<usize> {
                let bytes = count as usize * 2048;
                for (i, s) in buf[..bytes].chunks_mut(2048).enumerate() {
                    s.fill(0xC0);
                    s[8..12].copy_from_slice(&(lba + i as u32).to_be_bytes());
                }
                Ok(bytes)
            }
        }
        let units = 30u32;
        let title = title_with_extents(vec![crate::disc::Extent {
            start_lba: 900,
            sector_count: units * ALIGNED_UNIT_SECTORS,
        }]);
        let got = read_encrypted_units(&mut Stamped, &title, 1000);
        let mut lbas: Vec<u32> = got
            .iter()
            .map(|u| {
                assert_eq!(u.len(), ALIGNED_UNIT_LEN);
                u32::from_be_bytes([u[8], u[9], u[10], u[11]])
            })
            .collect();
        let n = lbas.len();
        lbas.sort_unstable();
        lbas.dedup();
        assert_eq!(lbas.len(), n, "no unit may be sampled twice");
        assert!(n as u32 <= units);
    }
}

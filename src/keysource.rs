//! Key sources — the layer that hands libfreemkv a disc's terminal Unit Keys.
//!
//! libfreemkv performs NO key lookup. An application resolves a disc's keys
//! through one or more [`KeySource`]s, each an adapter over a backing store (a
//! keydb file, a key server, the mapfile cache). A source's job is to return the
//! disc's terminal **Unit Keys** ([`crate::aacs::types::UnitKey`]). It knows what
//! material it holds (a DK / MK / VUK / pre-decrypted UK) and what it must fetch
//! from the disc (VID, MKB, encrypted title keys, content samples) to get there;
//! it orchestrates the derivation by calling libfreemkv's own derivation
//! primitives ([`crate::aacs::derive::derive_media_key_from_dk`] /
//! [`crate::aacs::derive::derive_vuk`] / [`crate::aacs::derive::decrypt_unit_key`])
//! through the [`ResolveCtx`] handed to it.
//!
//! libfreemkv still OWNS the crypto: the boil-down primitives and the AES live
//! here. A source owns only PATH ORCHESTRATION — deciding which primitive to
//! call with what input for the material it happens to hold. Source
//! implementations are published in the companion `freemkv-keysources` crate,
//! keeping key *policy* (which store, which order, online vs local) out of the
//! library.

use crate::aacs::types::HostCert;
use crate::aacs::types::{UnitKey, Vid};
use crate::disc::Key;
use crate::error::Error;

/// The public AACS inputs a key source needs to look a disc up. Captured at
/// scan; contains no secrets — only the disc identity and the on-disc AACS
/// structures a source or key server may key on.
#[derive(Debug, Clone)]
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

/// A lazy view of a disc's AACS material, handed to [`KeySource::get_uk`] so a
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
    /// in on-disc order. Feed straight into [`crate::aacs::boil::uk_from_vuk`].
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
    /// A present-but-malformed `unit_key_ro` (truncated / wrong magic / wrong
    /// stride) parses to an empty key set, so a later [`Self::enc_title_keys`]
    /// returns `Ok(&[])` indistinguishably from a disc that legitimately has no
    /// title keys — the parse failure is swallowed here, not surfaced as an
    /// error.
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
/// source looks the disc up in its store and, from whatever level of material it
/// holds, orchestrates the derivation down to Unit Keys using the library's
/// boil-down crypto primitives — never re-implementing AES. A source that holds
/// pre-decrypted Unit Keys returns them directly; one that holds a VUK calls
/// [`crate::aacs::boil::uk_from_vuk`]; one that holds device keys calls
/// [`crate::aacs::boil::mk_from_dk`] → [`crate::aacs::boil::vuk_from_mk`] → `uk_from_vuk`.
///
/// Returning an empty `Vec` means "no key for this disc from this source"; an
/// `Err` means the source itself failed (I/O, parse, network). The caller
/// ([`resolve_and_apply`]) tries each source in order and validates the returned
/// keys against real ciphertext before committing them, so a wrong key from one
/// source transparently falls through to the next.
pub trait KeySource {
    /// Resolve this disc's terminal Unit Keys from this source. An empty `Vec`
    /// is a genuine "no key here"; `Err` is a source failure.
    fn get_uk(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error>;

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
/// One-shot per source: each source's [`KeySource::get_uk`] is called exactly
/// once with a [`DiscInputsCtx`] over `inputs`. Non-empty Unit Keys are mapped
/// to terminal [`Key::Unit`]s and applied via [`crate::Disc::decrypt_with`],
/// which validates them against `inputs.samples` and only mutates the disc on
/// success — so a wrong/partial key set is rejected and the loop continues.
///
/// CPS-unit numbering: a source returns Unit Keys carrying the POSITIONAL index
/// from [`crate::aacs::boil::uk_from_vuk`]; the library's canonical CPS-unit number is
/// `position + 1` (matching [`crate::aacs::inf::parse_unit_key_ro`]'s `(i + 1)`), so
/// the committed `AacsState.unit_keys` is byte-identical to the library-resolved
/// path. The number is cosmetic for descramble (the decrypt path strips it and
/// tries every key) but is kept faithful to the resolver's convention.
pub fn resolve_and_apply_traced(
    sources: &[Box<dyn KeySource>],
    inputs: &DiscInputs,
    disc: &mut crate::Disc,
) -> (bool, crate::aacs::trace::ResolutionTrace) {
    use crate::aacs::trace::{KeyNode, KeyOutcome, KeyStep};

    let mut trace = crate::aacs::trace::ResolutionTrace::new();

    // The ctx parses Unit_Key_RO.inf at the stride for `inputs.version` (the
    // disc's own AACS major), so the stride is the disc's single source of truth.
    let ctx = DiscInputsCtx::new(inputs);

    for source in sources {
        // `who` is the source's own stable identifier — no enum to map back to.
        let who = source.label().to_string();
        match source.get_uk(&ctx) {
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
            // Empty (no key here) or a source failure — both are "no key from
            // this source"; move on to the next.
            Ok(_) | Err(_) => {
                trace.keys.push(KeyStep {
                    who,
                    path: vec![KeyNode::NoEntry],
                    outcome: KeyOutcome::NoKey,
                });
            }
        }
    }
    (false, trace)
}

/// THE single key-fetch: drive `sources` in order and return the first non-empty
/// Unit Key set. This is exactly what both paths do — only the samples differ:
/// * at disc open, `ctx` carries reachable-content samples → resolves the
///   up-front CPS units (the common one),
/// * in the read, on a decrypt miss, `ctx` carries the FAILING unit's ciphertext
///   → resolves the CPS unit that wasn't sampled up front.
///
/// Same sources, same call; there is no separate "fetch". Unlike
/// [`resolve_and_apply`] this does not validate/commit to a disc — the read's
/// decorator re-decrypts with the returned keys, which is the validation.
pub fn fetch_unit_keys(sources: &[Box<dyn KeySource>], ctx: &dyn ResolveCtx) -> Vec<UnitKey> {
    for source in sources {
        if let Ok(uks) = source.get_uk(ctx) {
            if !uks.is_empty() {
                return uks;
            }
        }
    }
    Vec::new()
}

/// Build the read-time key-fetch closure from the disc's public AACS inputs and
/// a way to (re)build the application's key sources. The decorator calls it with
/// the still-scrambled unit ciphertext when no held key opens that unit; it runs
/// [`fetch_unit_keys`] with those bytes as `samples` and returns any keys.
///
/// One builder, used by every read path (sweep / patch / mux) and by every
/// consumer (CLI, autorip) — neither application contains the fetch logic, only
/// its key-source config. Returns a **shared, stateless** [`crate::sector::KeyFetch`]
/// (`Arc<Fn>`): build it once, clone it into each read path. `make_sources` is
/// invoked per fetch (the cold path, ~once per CPS unit) so the closure stays
/// `Send + Sync` without requiring `KeySource: Send`.
pub fn key_fetch(
    inputs: DiscInputs,
    make_sources: std::sync::Arc<dyn Fn() -> Vec<Box<dyn KeySource>> + Send + Sync>,
) -> crate::sector::KeyFetch {
    std::sync::Arc::new(move |samples: &[Vec<u8>]| -> Vec<[u8; 16]> {
        let sources = make_sources();
        let mut di = inputs.clone();
        di.samples = samples.to_vec();
        // Parse Unit_Key_RO.inf at the disc's OWN stride (carried on `inputs`):
        // an online /decode reply that returns a VUK (not a terminal UK) then
        // derives unit keys from `enc_title_keys`, which a V10 disc parses at the
        // 48-byte stride — hardcoding the V20 stride here corrupted them.
        let ctx = DiscInputsCtx::new(&di);
        fetch_unit_keys(&sources, &ctx)
            .into_iter()
            .map(|u| u.key)
            .collect()
    })
}

/// Read up to `n` ENCRYPTED 6144-byte aligned units from `title`'s body, raw (no
/// decrypt) — the content samples that populate [`DiscInputs::samples`] for a
/// key server to validate a candidate against, and that [`resolve_and_apply`]
/// hands to [`crate::Disc::decrypt_with`].
///
/// Lives in the library, not a key-source crate: reading the disc and carving
/// AACS units is decryption *mechanism* (unit geometry anchored at each extent's
/// `start_lba`), which the library owns. A key source is *handed* these bytes
/// via `DiscInputs.samples`; it never reads the disc itself.
///
/// "Encrypted" is decided by [`crate::aacs::content::aacs_unit_encrypted`] — the
/// AACS Copy Permission Indicator (CPI) in the top 2 bits of byte 0, the
/// spec-correct signal (libaacs' `buf[0] & 0xc0`). NOT the `ts_sync_destroyed`
/// sync heuristic: destroyed TS syncs do not imply encryption (an FMTS variant
/// frame or an odd clear unit can lack syncs yet be unencrypted), and a clear
/// unit sent to a key server yields nothing to validate against — the "0
/// encrypted units" rejection. A clip opens with clear navigation units (PAT/PMT,
/// menus) whose CPI is clear; only CPI-flagged content units are collected —
/// probing several points spread across EACH extent so a title whose encrypted
/// body starts late (or whose midpoint lands in clear nav) still yields samples.
/// CPI is read at each extent's `start_lba` (clip-file-anchored), so byte 0 is a
/// real unit start and the flag is meaningful.
pub fn read_encrypted_units(
    reader: &mut dyn crate::sector::SectorSource,
    title: &crate::disc::DiscTitle,
    n: usize,
) -> Vec<Vec<u8>> {
    use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
    const CHUNK_UNITS: u32 = 15; // 45 sectors/read — under the drive transfer cap
    // Probe several evenly-spaced points across EACH extent rather than only the
    // midpoint-and-forward: a title whose encrypted feature starts late, or whose
    // midpoint lands in a clear nav stretch, must STILL yield scrambled samples.
    // Empty samples make `Disc::decrypt_with` skip wrong-key validation, so a
    // real encrypted title returning nothing here is a silent wrong-key hazard.
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
            // Saturate: start_lba comes from attacker-controlled UDF/MPLS
            // extents; a malformed extent near u32::MAX would otherwise panic
            // (debug) or wrap to a wrong LBA (release). An over-capacity LBA then
            // fails cleanly via the read_sectors().is_err() skip below.
            let lba = ext
                .start_lba
                .saturating_add(unit.saturating_mul(ALIGNED_UNIT_SECTORS));
            let count = (units_this * ALIGNED_UNIT_SECTORS) as u16;
            let mut buf = vec![0u8; count as usize * 2048];
            // `false` = no recovery retries; the reader is the raw drive/file
            // (no decrypt decorator), so these are the on-disc encrypted bytes. A
            // read error at one probe skips THAT probe only — it must not abandon
            // the rest of the extent (the old `break` blinded the sampler on a
            // single transient miss).
            if reader.read_sectors(lba, count, &mut buf, false).is_err() {
                continue;
            }
            for i in 0..units_this as usize {
                let o = i * ALIGNED_UNIT_LEN;
                if o + ALIGNED_UNIT_LEN > buf.len() {
                    break;
                }
                let u = &buf[o..o + ALIGNED_UNIT_LEN];
                if aacs_unit_encrypted(u) {
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

    // ── KeySource default-method behaviour ────────────────────────────────────

    /// KeySource::host_certs() defaults to empty regardless of the MKB argument.
    /// Spec: a source holding no cert returns the empty vec; the `mkb` param is
    /// forward-looking and the default ignores it.
    /// Mutation: a default returning a non-empty vec would inject phantom certs
    ///           into the OEM handshake.
    #[test]
    fn key_source_host_certs_defaults_to_empty() {
        struct MinimalSource;
        impl KeySource for MinimalSource {
            fn get_uk(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
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
            fn get_uk(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
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
        fn get_uk(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(Vec::new())
        }
    }
    struct ErroringSource;
    impl KeySource for ErroringSource {
        fn get_uk(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Err(Error::AacsNoKeys)
        }
    }
    struct HasKey([u8; 16]);
    impl KeySource for HasKey {
        fn get_uk(&self, _ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
            Ok(vec![UnitKey {
                idx: 0,
                key: self.0,
            }])
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
            fn get_uk(&self, ctx: &dyn ResolveCtx) -> Result<Vec<UnitKey>, Error> {
                if let Ok(s) = ctx.samples(8) {
                    self.seen.lock().unwrap().extend(s);
                }
                Ok(vec![UnitKey {
                    idx: 0,
                    key: self.key,
                }])
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
        let got = cb(&samples);
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

    /// #4 regression: encrypted content NOT at the extent midpoint (a late-
    /// starting feature, or a midpoint landing in clear nav) must still be
    /// sampled — empty samples make `decrypt_with` skip wrong-key validation.
    /// The old midpoint-and-forward sampler returned empty; the probe-spread
    /// finds the early scrambled band.
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
                aacs_unit_encrypted(s),
                "every sample is a CPI-flagged encrypted unit (byte0 & 0xC0 != 0)"
            );
        }
    }

    /// DISCRIMINATING: selection is by the AACS CPI (byte 0), NOT the
    /// `ts_sync_destroyed` heuristic. Half the units are sync-destroyed but
    /// CPI-CLEAR (`byte0 & 0xC0 == 0`) — genuinely UNencrypted units that merely
    /// lack TS syncs; the old sampler collected these and the key server rejected
    /// the POST as "0 encrypted units". `read_encrypted_units` must skip them and
    /// return ONLY CPI-flagged units. A regression to `ts_sync_destroyed` would
    /// collect the CPI-clear units too and fail the `& 0xC0` assertion.
    #[test]
    fn read_encrypted_units_selects_by_cpi_not_ts_sync() {
        use crate::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted};
        use crate::error::Result;
        use crate::sector::SectorSource;

        // Even units: CPI-clear (byte0 & 0xC0 == 0) AND sync-destroyed (no 0x47).
        // Odd units:  CPI-set (byte0 = 0xC0) with a scrambled body.
        // `ts_sync_destroyed` is TRUE for BOTH; `aacs_unit_encrypted` only odd.
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
                    if abs % 2 == 0 {
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
                aacs_unit_encrypted(s),
                "only CPI-flagged units are selected"
            );
            assert_eq!(
                s[0] & 0xC0,
                0xC0,
                "a CPI-clear sync-destroyed unit must never be sampled"
            );
        }
    }

    /// Audit #5 — a DISCRIMINATING test for the version→stride fix. A 2-key
    /// `Unit_Key_RO.inf` whose SECOND key sits at the V20 (64-byte) offset; a V10
    /// (48-byte) parse reads a DIFFERENT region. Confirms `DiscInputsCtx` parses
    /// at the stride for `inputs.version` — a swapped `from_major` branch or a
    /// hardcoded stride (the exact bug 1.2.0 fixes) would fail this, where the
    /// prior single-key fixtures passed regardless of stride.
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
}

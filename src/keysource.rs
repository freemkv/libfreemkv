//! Key sources — the layer that hands libfreemkv a disc's terminal Unit Keys.
//!
//! libfreemkv performs NO key lookup. An application resolves a disc's keys
//! through one or more [`KeySource`]s, each an adapter over a backing store (a
//! keydb file, a key server, the mapfile cache). A source's job is to return the
//! disc's terminal **Unit Keys** ([`crate::aacs::UnitKey`]). It knows what
//! material it holds (a DK / MK / VUK / pre-decrypted UK) and what it must fetch
//! from the disc (VID, MKB, encrypted title keys, content samples) to get there;
//! it orchestrates the derivation by calling libfreemkv's own boil-down crypto
//! primitives ([`crate::aacs::mk_from_dk`] / [`crate::aacs::vuk_from_mk`] /
//! [`crate::aacs::uk_from_vuk`]) through the [`ResolveCtx`] handed to it.
//!
//! libfreemkv still OWNS the crypto: the boil-down primitives and the AES live
//! here. A source owns only PATH ORCHESTRATION — deciding which primitive to
//! call with what input for the material it happens to hold. Source
//! implementations are published in the companion `freemkv-keysources` crate,
//! keeping key *policy* (which store, which order, online vs local) out of the
//! library.

use crate::aacs::{HostCert, UnitKey, Vid};
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
    /// way the library's resolver parses them ([`crate::aacs::parse_unit_key_ro`]),
    /// in on-disc order. Feed straight into [`crate::aacs::uk_from_vuk`].
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
    /// stride for AACS major `version_u8` (1 = V10, else V20/V21).
    pub fn new(inputs: &'a DiscInputs, version_u8: u8) -> Self {
        use crate::aacs::{AacsVersion, parse_unit_key_ro};
        let enc_keys = if inputs.unit_key_ro.is_empty() {
            Vec::new()
        } else {
            let version = if version_u8 == 1 {
                AacsVersion::V10
            } else {
                AacsVersion::V20
            };
            parse_unit_key_ro(&inputs.unit_key_ro, version)
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
/// [`crate::aacs::uk_from_vuk`]; one that holds device keys calls
/// [`crate::aacs::mk_from_dk`] → [`crate::aacs::vuk_from_mk`] → `uk_from_vuk`.
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
/// [`crate::aacs::ResolutionTrace`] recording, per source, what happened — for
/// applications to render. ZERO English; the trace is typed enums only.
///
/// One-shot per source: each source's [`KeySource::get_uk`] is called exactly
/// once with a [`DiscInputsCtx`] over `inputs`. Non-empty Unit Keys are mapped
/// to terminal [`Key::Unit`]s and applied via [`crate::Disc::decrypt_with`],
/// which validates them against `inputs.samples` and only mutates the disc on
/// success — so a wrong/partial key set is rejected and the loop continues.
///
/// CPS-unit numbering: a source returns Unit Keys carrying the POSITIONAL index
/// from [`crate::aacs::uk_from_vuk`]; the library's canonical CPS-unit number is
/// `position + 1` (matching [`crate::aacs::parse_unit_key_ro`]'s `(i + 1)`), so
/// the committed `AacsState.unit_keys` is byte-identical to the library-resolved
/// path. The number is cosmetic for descramble (the decrypt path strips it and
/// tries every key) but is kept faithful to the resolver's convention.
pub fn resolve_and_apply_traced(
    sources: &[Box<dyn KeySource>],
    inputs: &DiscInputs,
    disc: &mut crate::Disc,
) -> (bool, crate::aacs::ResolutionTrace) {
    use crate::aacs::trace::{KeyNode, KeyOutcome, KeyStep};

    let mut trace = crate::aacs::ResolutionTrace::new();

    // AACS major drives the Unit_Key_RO.inf stride the ctx parses at. Default to
    // the V20/V21 stride when there is no AACS state (it is the common live case;
    // a non-AACS disc has nothing to resolve and the loop simply finds nothing).
    let version_u8 = disc.aacs.as_ref().map(|a| a.version).unwrap_or(2);
    let ctx = DiscInputsCtx::new(inputs, version_u8);

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
/// "Encrypted" is decided by [`crate::aacs::is_aacs_scrambled`] — the SAME
/// predicate the decrypt gate uses — so all sides agree. A clip opens with clear
/// navigation units (PAT/PMT, menus); only the feature body is scrambled, and a
/// clear unit proves nothing, so this collects only scrambled ones, sampling the
/// largest extent at its midpoint forward.
pub fn read_encrypted_units(
    reader: &mut dyn crate::sector::SectorSource,
    title: &crate::disc::DiscTitle,
    n: usize,
) -> Vec<Vec<u8>> {
    use crate::aacs::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, is_aacs_scrambled};
    const CHUNK_UNITS: u32 = 15; // 45 sectors/read — under the drive transfer cap
    const MAX_CHUNKS_PER_EXTENT: u32 = 4; // ~60 units scanned at each extent's midpoint

    let mut out: Vec<Vec<u8>> = Vec::new();
    for ext in &title.extents {
        let total_units = ext.sector_count / ALIGNED_UNIT_SECTORS;
        if total_units == 0 {
            continue;
        }
        let mut unit = total_units / 2; // midpoint (past the clear nav at the head)
        for _ in 0..MAX_CHUNKS_PER_EXTENT {
            if unit >= total_units {
                break;
            }
            let units_this = CHUNK_UNITS.min(total_units - unit);
            // Saturate: start_lba comes from attacker-controlled UDF/MPLS
            // extents; a malformed extent near u32::MAX would otherwise panic
            // (debug) or wrap to a wrong LBA (release). An over-capacity LBA then
            // fails cleanly via the read_sectors().is_err() break below.
            let lba = ext
                .start_lba
                .saturating_add(unit.saturating_mul(ALIGNED_UNIT_SECTORS));
            let count = (units_this * ALIGNED_UNIT_SECTORS) as u16;
            let mut buf = vec![0u8; count as usize * 2048];
            // `false` = no recovery retries; the reader is the raw drive/file
            // (no decrypt decorator), so these are the on-disc encrypted bytes.
            if reader.read_sectors(lba, count, &mut buf, false).is_err() {
                break;
            }
            for i in 0..units_this as usize {
                let o = i * ALIGNED_UNIT_LEN;
                if o + ALIGNED_UNIT_LEN > buf.len() {
                    break;
                }
                let u = &buf[o..o + ALIGNED_UNIT_LEN];
                if is_aacs_scrambled(u) {
                    out.push(u.to_vec());
                    if out.len() >= n {
                        return out;
                    }
                }
            }
            unit += units_this;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::UnitKey;

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
            mkb: vec![1, 2, 3],
            unit_key_ro: uk_ro,
            samples: vec![vec![9u8; 4], vec![8u8; 4], vec![7u8; 4]],
            volume_label: Some("TITLE_X".into()),
        };

        // Zero VID → None.
        let ctx = DiscInputsCtx::new(&inputs, 1);
        assert_eq!(ctx.disc_hash(), "0xABC");
        assert_eq!(ctx.title(), Some("TITLE_X"));
        assert!(ctx.vid().is_none(), "all-zero VID is the no-VID sentinel");
        assert_eq!(ctx.mkb().unwrap(), &[1, 2, 3]);
        assert_eq!(ctx.enc_title_keys().unwrap(), &[key_bytes]);
        assert_eq!(ctx.samples(2).unwrap().len(), 2, "samples truncates to n");

        // Non-zero VID → Some(vid).
        let mut inputs2 = inputs.clone();
        inputs2.volume_id = [0x42u8; 16];
        let ctx2 = DiscInputsCtx::new(&inputs2, 1);
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
}

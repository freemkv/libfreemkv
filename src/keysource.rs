//! Key sources — the lookup layer that hands libfreemkv a [`Key`].
//!
//! libfreemkv performs NO key lookup. An application resolves a key for a disc
//! through one or more [`KeySource`]s, each a dumb adapter over a backing store
//! (a keydb file, a key server, the mapfile cache): given the disc's
//! [`DiscInputs`] it returns the raw [`Key`] at whatever level it holds. The
//! library then derives down and decrypts via `Disc::decrypt_with`.
//!
//! Source implementations are published in the companion `freemkv-keysources`
//! crate — keeping all key *policy* (which store, which order, online vs local)
//! out of the library while all key *mechanism* (the AACS derivation chain)
//! stays in it.

use crate::disc::Key;

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
    /// `WICKED_FOR_GOOD`), falling back to the BDMV `<di:name>` when present.
    /// `None` when not captured. Identity only, no secret; a key service may
    /// record it (keyed by `disc_hash`) to build a hash→title catalog. Not used
    /// in any AACS derivation.
    pub volume_label: Option<String>,
}

/// A key source: a stateful provider that hands a disc's candidate [`Key`]s out
/// **one at a time**, in whatever order it judges best for its backing store.
///
/// Dumb by contract — a source queries its store and yields the raw material it
/// holds at whatever level it has (device / processing / media / volume / unit).
/// It performs NO AACS derivation and NO validation: `Disc::decrypt_with`
/// derives down AND validates against real ciphertext, returning `Err` for a key
/// that does not decrypt this disc. That keeps every derivation step and the one
/// validation gate in the library, across AACS 1.0 / 2.0 / 2.1 / 2.x.
///
/// The source is the one that knows how many candidates it has and in what order
/// to try them — a keydb holds a per-disc UK *and* VUK *and* a device-key pool,
/// so it hands them out cheapest/most-specific first (UK ▸ VK ▸ MK ▸ DK) and
/// reports exhaustion when its list runs out; an online key service or a mapfile
/// cache hold exactly one. The caller drives the loop: `next_key` →
/// `Disc::decrypt_with` → on `Err`, ask again → until a key decrypts or the
/// source returns `None` (a genuine "no key for this disc"). Compose several
/// sources, in the caller's chosen order, with [`crate`]'s `MultiSource`.
pub trait KeySource {
    /// Hand the NEXT candidate key for this disc, or `None` once this source is
    /// exhausted. Stateful: the source tracks what it already handed out this
    /// session, so asking again after a rejected key yields the next candidate
    /// (or `None`) — it never re-offers a key or re-hits a one-shot backend (an
    /// online service is asked at most once). A source failure (I/O, network,
    /// parse) surfaces as `None` — there is simply nothing more to try.
    fn next_key(&mut self, inputs: &DiscInputs) -> Option<Key>;

    /// Whether this source needs [`DiscInputs::samples`] populated (encrypted
    /// content samples) — true for a source that validates against ciphertext
    /// server-side, false for one that keys purely on disc identity. The caller
    /// reads samples (an extra disc read) only when some source needs them.
    fn needs_samples(&self) -> bool {
        false
    }

    /// Whether this source FAILED (I/O, network, parse) rather than simply
    /// having no key. Checked after exhaustion so the caller can tell a genuine
    /// "no key for this disc" apart from "the key service was unreachable". A
    /// store that treats absence as not-an-error (a missing keydb / mapfile)
    /// leaves this `false`.
    fn errored(&self) -> bool {
        false
    }
}

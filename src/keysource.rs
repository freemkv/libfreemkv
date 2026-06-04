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
use crate::error::Result;

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
}

/// A key source: given a disc's [`DiscInputs`], offer candidate [`Key`]s.
///
/// Dumb by contract — a source queries its backing store and enumerates the raw
/// material it holds as candidate keys at whatever level it has (device /
/// processing / media / volume / unit). It performs NO AACS derivation and NO
/// validation; `Disc::decrypt_with` derives down, and the caller validates by
/// decrypting a sample. That keeps every derivation step in one place (the
/// library) across AACS 1.0 / 2.0 / 2.1 / 2.x.
///
/// A source returns *multiple ordered candidates* because a single store can
/// hold material for several derivation paths (a keydb has a per-disc VUK *and*
/// a device-key pool *and* a media-key pool — the source can't know which
/// applies without the MKB walk, which is derivation). The caller tries the
/// candidates in order and keeps the first that decrypts (validate-before-
/// return). A source that resolves server-side (an online key service) or holds
/// a cached final key (the mapfile) simply returns one candidate.
pub trait KeySource {
    /// Candidate keys for this disc, most-specific first. Empty = this source
    /// has nothing; `Err(_)` = the source itself failed (I/O, network, parse).
    fn resolve(&self, inputs: &DiscInputs) -> Result<Vec<Key>>;

    /// Whether this source needs [`DiscInputs::samples`] populated (encrypted
    /// content samples) — true for a source that validates against ciphertext
    /// server-side, false for one that keys purely on disc identity. The caller
    /// reads samples (an extra disc read) only when some source needs them.
    fn needs_samples(&self) -> bool {
        false
    }
}

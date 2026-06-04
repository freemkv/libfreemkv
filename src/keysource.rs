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
}

/// A key source: given a disc's [`DiscInputs`], look up a [`Key`].
///
/// Dumb by contract — a source queries its backing store and returns the raw
/// key at whatever level it has (device / processing / media / volume / unit).
/// It performs NO AACS derivation; `Disc::decrypt_with` derives down. That
/// keeps every derivation step in one place (the library) across AACS
/// 1.0 / 2.0 / 2.1 / 2.x.
pub trait KeySource {
    /// Look up a key for this disc.
    ///
    /// - `Ok(Some(key))` — a key was found; the caller hands it to
    ///   `Disc::decrypt_with`.
    /// - `Ok(None)` — this source has nothing for the disc; try the next one.
    /// - `Err(_)` — the source itself failed (I/O, network, parse).
    fn resolve(&self, inputs: &DiscInputs) -> Result<Option<Key>>;
}

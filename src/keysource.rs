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

use crate::aacs::HostCert;
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
    /// `TITLE_2024`), falling back to the BDMV `<di:name>` when present.
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
/// sources, in the caller's chosen order, with the companion
/// `freemkv-keysources` crate's `MultiSource`.
pub trait KeySource {
    /// Hand the NEXT candidate key for this disc, or `None` once this source is
    /// exhausted. Stateful: the source tracks what it already handed out this
    /// session, so asking again after a rejected key yields the next candidate
    /// (or `None`) — it never re-offers a key or re-hits a one-shot backend (an
    /// online service is asked at most once).
    ///
    /// `None` means only "no more candidates from this source"; it does NOT by
    /// itself distinguish a genuine "no key for this disc" from a source
    /// failure (I/O, network, parse). After exhaustion the caller must consult
    /// [`KeySource::errored`] to tell the two apart — a failed source records
    /// the failure there and still returns `None` here.
    fn next_key(&mut self, inputs: &DiscInputs) -> Option<Key>;

    /// Whether this source needs [`DiscInputs::samples`] populated (encrypted
    /// content samples) — true for a source that validates against ciphertext
    /// server-side, false for one that keys purely on disc identity. The caller
    /// reads samples (an extra disc read) only when some source needs them.
    fn needs_samples(&self) -> bool {
        false
    }

    /// A short, stable identifier for this source kind (`"keydb"`, `"online"`,
    /// `"mapfile"`, …). For logging which source produced a key, and for
    /// composition/ordering logic that needs to tell sources apart. A format
    /// string, not user-facing English.
    fn label(&self) -> &'static str {
        "source"
    }

    /// Whether this source FAILED (I/O, network, parse) rather than simply
    /// having no key. Checked after exhaustion so the caller can tell a genuine
    /// "no key for this disc" apart from "the key service was unreachable". A
    /// store that treats absence as not-an-error (a missing keydb / mapfile)
    /// leaves this `false`.
    fn errored(&self) -> bool {
        false
    }

    /// The AACS host certificate(s) this source can supply for the live-drive
    /// SCSI mutual-auth handshake (the OEM/AACS baseline route). A host cert is
    /// the *second* kind of AACS material a source may hold, distinct from the
    /// decryption keys handed out by [`KeySource::next_key`]: it unlocks the
    /// authenticated bus so the drive will report the Volume ID and bus key,
    /// whereas the keys decrypt content once the disc is read.
    ///
    /// Returned, never compiled in: a host cert is **perishable** — it can be
    /// revoked on a given drive's Host Revocation List (carried forward by newer
    /// discs' MKBs), so it must be rotatable, hence served by a source rather
    /// than baked into the binary. A source that holds no cert (a mapfile, or an
    /// online service whose cert-serving isn't yet designed) returns the empty
    /// vec — the default. The handshake collects across every source and tries
    /// each candidate; with no candidate from any source the OEM route fails
    /// gracefully ([`crate::Error::AacsNoHostCert`]), it never panics.
    fn host_certs(&self) -> Vec<HostCert> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::Key;

    // ── DiscInputs structural tests ────────────────────────────────────────────

    // ── KeySource default-method behaviour ────────────────────────────────────

    /// KeySource::needs_samples() defaults to false.
    /// Spec: doc says "false for one that keys purely on disc identity."
    /// Mutation: defaulting to true forces an extra disc-read for every source,
    ///           even local keydb lookups that don't need ciphertext samples.
    #[test]
    fn key_source_needs_samples_defaults_to_false() {
        struct MinimalSource;
        impl KeySource for MinimalSource {
            fn next_key(&mut self, _inputs: &DiscInputs) -> Option<Key> {
                None
            }
        }
        let s = MinimalSource;
        assert!(!s.needs_samples(), "needs_samples must default to false");
    }

    /// KeySource::errored() defaults to false.
    /// Spec: doc says "A store that treats absence as not-an-error leaves this false."
    /// Mutation: defaulting to true would make every source appear errored, causing
    ///           the caller to report "key service unreachable" for a simple miss.
    #[test]
    fn key_source_errored_defaults_to_false() {
        struct MinimalSource;
        impl KeySource for MinimalSource {
            fn next_key(&mut self, _inputs: &DiscInputs) -> Option<Key> {
                None
            }
        }
        let s = MinimalSource;
        assert!(!s.errored(), "errored must default to false");
    }
}

//! Structured resolution trace — what the unlock + key-resolution attempt did.
//!
//! No user-facing English: every step's STATE is a typed enum variant that
//! applications render into localized text. The `who` field of each step is
//! a stable identifier (e.g. a `label()`/`name()`), never prose, carried
//! verbatim. See docs/aacs-trace.md for full rationale.

/// The full trace of a resolution attempt: the unlock phase, then the
/// key-resolution phase.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResolutionTrace {
    /// One step per unlocker consulted, in consultation order.
    pub unlock: Vec<UnlockStep>,
    /// One step per key source consulted, in consultation order.
    pub keys: Vec<KeyStep>,
}

impl ResolutionTrace {
    /// An empty trace (no steps recorded).
    pub fn new() -> Self {
        Self::default()
    }
}

// ── Unlock phase ────────────────────────────────────────────────────────────

/// One unlocker's contribution to the unlock phase. `who` is the unlocker's
/// `name()` (a stable, product-neutral identifier), carried verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct UnlockStep {
    pub who: String,
    pub outcome: UnlockOutcome,
}

/// What an unlocker did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnlockOutcome {
    /// The drive was unlocked (or already usable) and a VID is available.
    Unlocked,
    /// This unlocker cannot unlock this drive's firmware.
    FirmwareNotUnlockable,
    /// No non-revoked host cert was usable for the auth attempt. `mkb` is the
    /// disc MKB generation when known.
    NoUsableHostCert { mkb: Option<u32> },
    /// Every available host cert was revoked on this drive's HRL. `mkb` is the
    /// disc MKB generation when known.
    CertRevoked { mkb: Option<u32> },
    /// The drive rejected the auth handshake (non-revocation rejection / wedge).
    HandshakeRejected,
    /// Auth succeeded (or was skipped) but the Volume ID could not be read.
    VidUnavailable,
}

// ── Key-resolution phase ────────────────────────────────────────────────────

/// One key source's contribution to the key-resolution phase, including the
/// derivation path it walked. `who` is the source's `label()` (a stable
/// identifier, e.g. `"keydb"` / `"online"`), carried verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyStep {
    pub who: String,
    pub path: Vec<KeyNode>,
    pub outcome: KeyOutcome,
    /// Shape of the source's MATCHED entry, when it matched this disc — a
    /// booleans-and-lengths summary (no key material) that de-conflates WHY a
    /// matched disc produced no key. `None` when the source did not match (a
    /// true miss) or the source kind carries no such shape. An application MAY
    /// log it verbatim and render it into a matched-but-no-key verdict.
    pub matched_entry: Option<MatchedEntry>,
    /// Number of per-disc entries loaded in the source's store, when known — so
    /// a true-miss verdict can name the store size (`… not in keydb (N entries
    /// loaded)`) and a reporter can confirm a wrong-pressing at a glance.
    /// `None` for a source that carries no such count. No secret.
    pub store_entries: Option<usize>,
}

/// A shape summary of a key source's MATCHED entry — booleans and lengths only,
/// never key MATERIAL — so an application can log WHY a disc that WAS found in
/// the store still produced no usable key (the classic "matched but needs a VID
/// this path can't supply"). Every field is safe to print at any log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MatchedEntry {
    /// The entry carries a Volume Unique Key (title keys can boil directly).
    pub has_vuk: bool,
    /// The entry carries one or more pre-decrypted terminal Unit Keys.
    pub has_unit_keys: bool,
    /// Count of pre-decrypted terminal Unit Keys the entry carries.
    pub unit_keys_len: usize,
    /// The entry carries a Media Key (a VID is still needed to reach the VUK).
    pub has_media_key: bool,
    /// The entry carries its own Volume ID (the keydb `I` token).
    pub has_keydb_vid: bool,
    /// Count of the disc's encrypted title keys available to boil (from
    /// `Unit_Key_RO.inf`); `0` when none were captured.
    pub enc_title_keys_len: usize,
    /// A Volume ID is available on this resolve path (from the drive handshake
    /// or the entry) — the gate the `MK → VUK` step needs.
    pub vid_available: bool,
}

/// A node on the derivation path a source walked. Ordered as encountered; not
/// every path hits every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyNode {
    /// The source matched this disc (by hash / VID).
    MatchedDisc,
    /// The source had no entry for this disc.
    NoEntry,
    /// The source matched this disc but could not derive any usable key from the
    /// matched entry (e.g. no derivation material for the path taken). Distinct
    /// from [`NoEntry`](Self::NoEntry): the disc WAS found, the key was not.
    NoDerivableKey,
    /// Pre-decrypted unit keys were found.
    FoundUnitKeys,
    /// A VUK was found.
    FoundVuk,
    /// A Media Key was found.
    FoundMediaKey,
    /// A VID is required to proceed.
    NeedVid,
    /// The VID came from the unlock phase.
    VidFromUnlock,
    /// The VID came from the keydb entry.
    VidFromKeydb,
    /// No VID was available.
    NoVid,
    /// A VUK was derived (from MK + VID).
    DerivedVuk,
    /// Unit keys were derived (from VUK).
    DerivedUnitKeys,
}

/// The terminal outcome of a source's resolution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOutcome {
    /// Usable unit keys were produced.
    Resolved,
    /// Derivation material existed but no VID was available to finish.
    MissingVid,
    /// No usable key from this source.
    NoKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trace types are constructible, derive the required traits, and an
    /// empty trace round-trips. Pins the structural contract apps build against.
    #[test]
    fn trace_is_constructible_and_comparable() {
        let t = ResolutionTrace {
            unlock: vec![UnlockStep {
                who: "AACS cert".to_string(),
                outcome: UnlockOutcome::NoUsableHostCert { mkb: Some(68) },
            }],
            keys: vec![KeyStep {
                who: "keydb".to_string(),
                path: vec![
                    KeyNode::MatchedDisc,
                    KeyNode::FoundVuk,
                    KeyNode::DerivedUnitKeys,
                ],
                outcome: KeyOutcome::Resolved,
                matched_entry: None,
                store_entries: None,
            }],
        };
        // Clone + PartialEq (derive contract the renderers rely on).
        assert_eq!(t.clone(), t);
        // `who` is the source's name carried verbatim.
        assert_eq!(t.keys[0].who, "keydb");
        assert_eq!(t.unlock[0].who, "AACS cert");
        // Default / new is empty.
        assert_eq!(ResolutionTrace::new(), ResolutionTrace::default());
        assert!(ResolutionTrace::new().unlock.is_empty());
        assert!(ResolutionTrace::new().keys.is_empty());
    }
}

//! Structured resolution trace — what the unlock + key-resolution attempt did.
//!
//! No user-facing English. Every step's STATE is a typed enum variant;
//! applications RENDER these into localized text (the library never does). This
//! module only DEFINES the shape and is wired through the resolve/handshake
//! return path far enough to compile.
//!
//! The `who` of each step is the source's `label()` / unlocker's `name()` — a
//! stable identifier string (a NAME, like a codec id, NOT user-facing prose),
//! carried verbatim so an app renderer never has to match an enum back to a name
//! it already has. Only the OUTCOME / path enums are structured states the app
//! maps to i18n English.

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
}

/// A node on the derivation path a source walked. Ordered as encountered; not
/// every path hits every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyNode {
    /// The source matched this disc (by hash / VID).
    MatchedDisc,
    /// The source had no entry for this disc.
    NoEntry,
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

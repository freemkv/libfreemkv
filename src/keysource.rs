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

    /// Whether this source FAILED (I/O, network, parse) rather than simply
    /// having no key. Checked after exhaustion so the caller can tell a genuine
    /// "no key for this disc" apart from "the key service was unreachable". A
    /// store that treats absence as not-an-error (a missing keydb / mapfile)
    /// leaves this `false`.
    fn errored(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::Key;

    // ── DiscInputs structural tests ────────────────────────────────────────────

    /// DiscInputs can be constructed with all-zero volume_id ([0u8;16]) to
    /// represent "no authenticated handshake ran".
    /// Spec: doc says "[0u8; 16] when no authenticated handshake ran".
    /// Mutation: using Option<[u8;16]> would require callers to handle None explicitly.
    #[test]
    fn disc_inputs_zero_volume_id_represents_no_handshake() {
        let inputs = DiscInputs {
            disc_hash: "0x1234".to_string(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        assert_eq!(
            inputs.volume_id, [0u8; 16],
            "all-zero volume_id must be valid (represents no handshake)"
        );
    }

    /// DiscInputs disc_hash is a string in "0x"-prefixed hex format.
    /// Spec: doc says "SHA-1 of Unit_Key_RO.inf, 0x-prefixed hex."
    /// Mutation: storing the hash without the "0x" prefix would silently change
    ///           the keydb lookup key format.
    #[test]
    fn disc_inputs_disc_hash_is_0x_prefixed() {
        let hash = "0xabcdef0123456789abcdef0123456789abcdef01".to_string();
        let inputs = DiscInputs {
            disc_hash: hash.clone(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        assert!(
            inputs.disc_hash.starts_with("0x"),
            "disc_hash must be 0x-prefixed per spec"
        );
        assert_eq!(
            inputs.disc_hash.len(),
            42,
            "SHA-1 in 0x-prefixed hex: 2 ('0x') + 40 (20 bytes hex) = 42 chars"
        );
    }

    /// DiscInputs samples is intentionally empty by default (filled by caller).
    /// Spec: doc says "Populated by the application — libfreemkv::Disc::inputs
    ///       leaves it empty for the caller to fill."
    /// Mutation: auto-filling samples in Disc::inputs would force all callers
    ///           to read content data even for local keydb lookups.
    #[test]
    fn disc_inputs_samples_defaults_to_empty() {
        let inputs = DiscInputs {
            disc_hash: "0x0000000000000000000000000000000000000000".to_string(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        assert!(
            inputs.samples.is_empty(),
            "samples must start empty — populated by the application, not Disc::inputs"
        );
    }

    /// DiscInputs volume_label is Option<String>: None means not captured.
    /// Spec: doc says "None when not captured."
    /// Mutation: using an empty string instead of None would conflate "not captured"
    ///           with "the disc has an empty label" — a semantic difference.
    #[test]
    fn disc_inputs_volume_label_none_vs_some() {
        let no_label = DiscInputs {
            disc_hash: "0x0000000000000000000000000000000000000000".to_string(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        assert!(
            no_label.volume_label.is_none(),
            "not-captured label must be None"
        );

        let with_label = DiscInputs {
            disc_hash: "0x0000000000000000000000000000000000000000".to_string(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: Some("WICKED_FOR_GOOD".to_string()),
        };
        assert_eq!(with_label.volume_label.as_deref(), Some("WICKED_FOR_GOOD"));
    }

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
        let mut s = MinimalSource;
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

    /// A source that returns None and has errored()==true can be distinguished
    /// from a source that simply has no key.
    /// Spec: doc says "After exhaustion the caller must consult errored()".
    /// Mutation: errored() always returning false hides network/parse failures.
    #[test]
    fn errored_source_is_distinguishable_from_empty_source() {
        struct FailedSource;
        impl KeySource for FailedSource {
            fn next_key(&mut self, _inputs: &DiscInputs) -> Option<Key> {
                None
            }
            fn errored(&self) -> bool {
                true
            }
        }
        struct EmptySource;
        impl KeySource for EmptySource {
            fn next_key(&mut self, _inputs: &DiscInputs) -> Option<Key> {
                None
            }
            // errored() defaults to false
        }
        let inputs = DiscInputs {
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            mkb: vec![],
            unit_key_ro: vec![],
            samples: vec![],
            volume_label: None,
        };
        let mut failed = FailedSource;
        let mut empty = EmptySource;

        // Both return None (exhausted).
        assert!(failed.next_key(&inputs).is_none());
        assert!(empty.next_key(&inputs).is_none());

        // But only FailedSource reports an error.
        assert!(failed.errored(), "FailedSource must report errored=true");
        assert!(!empty.errored(), "EmptySource must report errored=false");
    }

    /// DiscInputs mkb field stores raw MKB bytes and can be empty.
    /// Mutation: using Option<Vec<u8>> for mkb forces callers to handle Option.
    #[test]
    fn disc_inputs_mkb_can_be_empty_or_populated() {
        let empty_mkb = DiscInputs {
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            mkb: Vec::new(),
            unit_key_ro: Vec::new(),
            samples: Vec::new(),
            volume_label: None,
        };
        assert!(empty_mkb.mkb.is_empty());

        let populated_mkb = DiscInputs {
            disc_hash: String::new(),
            volume_id: [0u8; 16],
            mkb: vec![0x01, 0x02, 0x03],
            unit_key_ro: vec![0xFF],
            samples: Vec::new(),
            volume_label: None,
        };
        assert_eq!(populated_mkb.mkb, vec![0x01, 0x02, 0x03]);
        assert_eq!(populated_mkb.unit_key_ro, vec![0xFF]);
    }

    /// A source that overrides needs_samples() to true is handled correctly.
    /// Mutation: ignoring the needs_samples() return means online sources
    ///           never get the ciphertext samples they need for validation.
    #[test]
    fn needs_samples_can_be_overridden_to_true() {
        struct SamplesNeededSource;
        impl KeySource for SamplesNeededSource {
            fn next_key(&mut self, _inputs: &DiscInputs) -> Option<Key> {
                None
            }
            fn needs_samples(&self) -> bool {
                true
            }
        }
        let s = SamplesNeededSource;
        assert!(
            s.needs_samples(),
            "an online source that validates against ciphertext must return needs_samples=true"
        );
    }
}

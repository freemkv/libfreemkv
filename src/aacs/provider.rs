//! Key source abstraction for the AACS resolve chain.
//!
//! libfreemkv keeps all crypto (AES-G primitives, SD-tree walking,
//! validation, MK/VUK/TK derivation) but accepts key material from
//! arbitrary backends via [`KeyProvider`].
//!
//! Methods come in two flavors:
//!
//! - **Bulk material** ([`device_keys`], [`processing_keys`],
//!   [`media_keys`]) — the resolver unions (and dedups) results
//!   across all providers and tries each candidate.
//! - **Disc-keyed lookup** ([`lookup_disc_by_hash`],
//!   [`lookup_disc_by_vid`]) — the resolver short-circuits on the
//!   first hit, so providers are queried in array order with
//!   fastest/closest first.
//!
//! [`host_certs`] is a sixth method but is NOT consumed by the
//! resolver chain: the SCSI handshake reads host certs directly from
//! the caller-supplied credentials, not from the provider array. A
//! provider that overrides `host_certs` today has no effect on the
//! handshake; the method is retained as a forward-looking extension
//! point only.
//!
//! Default impls return empty / `None` so backends only override
//! the methods they actually support — an external key service might
//! implement only `lookup_disc_by_hash`, while a local file might
//! implement all six.
//!
//! Calls may block (disk I/O, network round-trips). The resolver
//! invokes each method at most a handful of times per scan; for
//! per-disc memoization, implementations should cache internally.
//!
//! [`device_keys`]: KeyProvider::device_keys
//! [`processing_keys`]: KeyProvider::processing_keys
//! [`media_keys`]: KeyProvider::media_keys
//! [`host_certs`]: KeyProvider::host_certs
//! [`lookup_disc_by_hash`]: KeyProvider::lookup_disc_by_hash
//! [`lookup_disc_by_vid`]: KeyProvider::lookup_disc_by_vid

use super::keydb::{DeviceKey, DiscEntry, HostCert};

/// Source of AACS key material.
///
/// Implementors return raw material only — the resolver in
/// `aacs::keys` owns all the crypto (DK→PK walking, PK validation,
/// MK→VUK→TK derivation). See module docs for method semantics.
pub trait KeyProvider: Send + Sync {
    /// Device keys (top-of-tree, walked by the resolver).
    fn device_keys(&self) -> Vec<DeviceKey> {
        Vec::new()
    }

    /// Processing keys — terminal PKs or walk-input PKs. The
    /// resolver tries each as a terminal first (cheap validate).
    fn processing_keys(&self) -> Vec<[u8; 16]> {
        Vec::new()
    }

    /// Every Media Key this provider holds, regardless of which disc it was
    /// filed under. An MK is MKB-scoped (shared across a pressing/MKB-family),
    /// so the resolver can verify each against the disc's MKB (`km_verifies`)
    /// and resolve a disc whose own hash/VID isn't directly keyed.
    fn media_keys(&self) -> Vec<[u8; 16]> {
        Vec::new()
    }

    /// AACS host certificates (with their private keys) for drive
    /// authentication. Multiple in case some are revoked.
    ///
    /// NOTE: not consumed by the resolver chain — the handshake reads
    /// host certs from the caller-supplied credentials directly, so
    /// overriding this method has no effect on drive authentication
    /// today. Retained as a forward-looking extension point.
    fn host_certs(&self) -> Vec<HostCert> {
        Vec::new()
    }

    /// Direct per-disc lookup by SHA-1 of `Unit_Key_RO.inf`. Returns
    /// `Some(entry)` if this provider has pre-computed material for
    /// the disc (paths 4 and 5). Short-circuits the resolver.
    fn lookup_disc_by_hash(&self, _disc_hash: &[u8; 20]) -> Option<DiscEntry> {
        None
    }

    /// Lookup by Volume ID (path 3 — pre-computed MK + matching
    /// VID). Short-circuits the resolver on hit.
    fn lookup_disc_by_vid(&self, _volume_id: &[u8; 16]) -> Option<DiscEntry> {
        None
    }
}

/// Resolver-side helpers that aggregate across a provider array.
///
/// The resolver wraps `ctx.providers` (`&[&dyn KeyProvider]`) in this
/// struct; these helpers apply the union-vs-short-circuit policy per
/// method. The bulk unions dedup so overlapping providers don't make
/// the resolver re-walk/re-validate identical material.
pub(crate) struct Providers<'a>(pub &'a [&'a dyn KeyProvider]);

impl Providers<'_> {
    /// Union (deduped) — gather DKs from every provider.
    pub fn device_keys(&self) -> Vec<DeviceKey> {
        let mut v: Vec<DeviceKey> = self.0.iter().flat_map(|p| p.device_keys()).collect();
        // DeviceKey has no Ord/Hash; dedup on the value-defining tuple.
        v.sort_unstable_by_key(|d| (d.key, d.node, d.uv, d.u_mask_shift));
        v.dedup_by_key(|d| (d.key, d.node, d.uv, d.u_mask_shift));
        v
    }

    /// Union (deduped) — gather PKs from every provider.
    pub fn processing_keys(&self) -> Vec<[u8; 16]> {
        let mut v: Vec<[u8; 16]> = self.0.iter().flat_map(|p| p.processing_keys()).collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Union of distinct Media Keys across every provider, for the MK-pool
    /// brute (`km_verifies` against the disc's MKB).
    pub fn media_keys(&self) -> Vec<[u8; 16]> {
        let mut v: Vec<[u8; 16]> = self.0.iter().flat_map(|p| p.media_keys()).collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Union — gather host certs from every provider. The SCSI handshake
    /// reads host certs from the caller-supplied credentials directly and
    /// does not call this, so it is currently unused by the resolver chain.
    #[allow(dead_code)]
    pub fn host_certs(&self) -> Vec<HostCert> {
        self.0.iter().flat_map(|p| p.host_certs()).collect()
    }

    /// Short-circuit — query providers in array order, first hit wins.
    pub fn lookup_disc_by_hash(&self, disc_hash: &[u8; 20]) -> Option<DiscEntry> {
        self.0.iter().find_map(|p| p.lookup_disc_by_hash(disc_hash))
    }

    /// Short-circuit — query providers in array order, first hit wins.
    pub fn lookup_disc_by_vid(&self, volume_id: &[u8; 16]) -> Option<DiscEntry> {
        self.0.iter().find_map(|p| p.lookup_disc_by_vid(volume_id))
    }
}

/// A [`KeyProvider`] backed by a single caller-supplied key's raw material —
/// the bridge for [`crate::disc::Disc::decrypt_with`].
///
/// The application's key source did the lookup and handed in material at one
/// level (DK / PK / MK / VUK). This exposes exactly that material to the
/// version-dispatched resolver, which owns ALL derivation — so a source never
/// derives, and the lib remains the single home for the AACS chain across
/// 1.0 / 2.0 / 2.1 / 2.x.
///
/// Each level fills only its own field; the rest stay empty, so the resolver
/// naturally runs the matching path (DK→…, PK→…, MK-pool brute, or a
/// disc-keyed VUK hit). `decrypt_with` already knows the disc, so the
/// `lookup_disc_by_*` hash/VID arguments are irrelevant — a present
/// `disc_entry` is returned for any query.
pub(crate) struct SuppliedKey {
    pub device_keys: Vec<DeviceKey>,
    pub processing_keys: Vec<[u8; 16]>,
    pub media_keys: Vec<[u8; 16]>,
    pub disc_entry: Option<DiscEntry>,
}

impl KeyProvider for SuppliedKey {
    fn device_keys(&self) -> Vec<DeviceKey> {
        self.device_keys.clone()
    }
    fn processing_keys(&self) -> Vec<[u8; 16]> {
        self.processing_keys.clone()
    }
    fn media_keys(&self) -> Vec<[u8; 16]> {
        self.media_keys.clone()
    }
    fn lookup_disc_by_hash(&self, _disc_hash: &[u8; 20]) -> Option<DiscEntry> {
        self.disc_entry.clone()
    }
    fn lookup_disc_by_vid(&self, _volume_id: &[u8; 16]) -> Option<DiscEntry> {
        self.disc_entry.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(hash: &str, vuk: u8) -> DiscEntry {
        DiscEntry {
            disc_hash: hash.to_string(),
            title: "t".to_string(),
            media_key: None,
            disc_id: None,
            vuk: Some([vuk; 16]),
            unit_keys: Vec::new(),
        }
    }

    fn dk(byte: u8, node: u16) -> DeviceKey {
        DeviceKey {
            key: [byte; 16],
            node,
            uv: 1,
            u_mask_shift: 0,
        }
    }

    /// A provider that returns fixed bulk material and an optional disc entry
    /// keyed unconditionally (used to test array-order short-circuiting).
    #[derive(Default)]
    struct Fixed {
        dks: Vec<DeviceKey>,
        pks: Vec<[u8; 16]>,
        mks: Vec<[u8; 16]>,
        hash_hit: Option<DiscEntry>,
        vid_hit: Option<DiscEntry>,
    }
    impl KeyProvider for Fixed {
        fn device_keys(&self) -> Vec<DeviceKey> {
            self.dks.clone()
        }
        fn processing_keys(&self) -> Vec<[u8; 16]> {
            self.pks.clone()
        }
        fn media_keys(&self) -> Vec<[u8; 16]> {
            self.mks.clone()
        }
        fn lookup_disc_by_hash(&self, _h: &[u8; 20]) -> Option<DiscEntry> {
            self.hash_hit.clone()
        }
        fn lookup_disc_by_vid(&self, _v: &[u8; 16]) -> Option<DiscEntry> {
            self.vid_hit.clone()
        }
    }

    // ── KeyProvider default methods all return empty ───────────────────────

    #[test]
    fn default_provider_methods_return_empty() {
        // A bare provider that overrides nothing must yield empty material so
        // the resolver simply finds nothing through it (no surprise hits).
        struct Empty;
        impl KeyProvider for Empty {}
        let e = Empty;
        assert!(e.device_keys().is_empty());
        assert!(e.processing_keys().is_empty());
        assert!(e.media_keys().is_empty());
        assert!(e.host_certs().is_empty());
        assert!(e.lookup_disc_by_hash(&[0u8; 20]).is_none());
        assert!(e.lookup_disc_by_vid(&[0u8; 16]).is_none());
    }

    // ── Providers::processing_keys: union + dedup ──────────────────────────

    #[test]
    fn providers_processing_keys_union_and_dedup() {
        // Two providers each carrying overlapping PKs → the aggregate is the
        // deduped union (the resolver must not re-validate identical material).
        let a = Fixed {
            pks: vec![[0x01u8; 16], [0x02u8; 16]],
            ..Default::default()
        };
        let b = Fixed {
            pks: vec![[0x02u8; 16], [0x03u8; 16]],
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let mut got = Providers(arr).processing_keys();
        got.sort();
        assert_eq!(got, vec![[0x01u8; 16], [0x02u8; 16], [0x03u8; 16]]);
    }

    #[test]
    fn providers_media_keys_union_and_dedup() {
        let a = Fixed {
            mks: vec![[0xAAu8; 16]],
            ..Default::default()
        };
        let b = Fixed {
            mks: vec![[0xAAu8; 16], [0xBBu8; 16]],
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let mut got = Providers(arr).media_keys();
        got.sort();
        assert_eq!(got, vec![[0xAAu8; 16], [0xBBu8; 16]]);
    }

    #[test]
    fn providers_device_keys_dedup_on_value_tuple() {
        // DeviceKey has no Hash/Ord; dedup keys on (key,node,uv,u_mask_shift).
        // Two identical DKs across providers collapse to one; a DK differing
        // only in node is kept.
        let a = Fixed {
            dks: vec![dk(0x11, 5), dk(0x11, 5)],
            ..Default::default()
        };
        let b = Fixed {
            dks: vec![dk(0x11, 5), dk(0x11, 6)],
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let got = Providers(arr).device_keys();
        assert_eq!(got.len(), 2, "identical DKs dedup; differing node kept");
        let nodes: Vec<u16> = got.iter().map(|d| d.node).collect();
        assert!(nodes.contains(&5) && nodes.contains(&6));
    }

    // ── Disc-keyed lookups: array-order short-circuit ──────────────────────

    #[test]
    fn providers_lookup_by_hash_first_hit_wins() {
        // Querying providers in array order, the FIRST hit wins (closest /
        // fastest first). Provider 0 hits → its entry is returned even though
        // provider 1 also has one.
        let a = Fixed {
            hash_hit: Some(entry("first", 0x01)),
            ..Default::default()
        };
        let b = Fixed {
            hash_hit: Some(entry("second", 0x02)),
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let got = Providers(arr).lookup_disc_by_hash(&[0u8; 20]).unwrap();
        assert_eq!(got.disc_hash, "first");
        assert_eq!(got.vuk, Some([0x01u8; 16]));
    }

    #[test]
    fn providers_lookup_by_hash_falls_through_to_later_provider() {
        // Provider 0 misses, provider 1 hits → the later provider's entry is
        // used (find_map continues past None).
        let a = Fixed::default(); // hash_hit None
        let b = Fixed {
            hash_hit: Some(entry("second", 0x02)),
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let got = Providers(arr).lookup_disc_by_hash(&[0u8; 20]).unwrap();
        assert_eq!(got.disc_hash, "second");
    }

    #[test]
    fn providers_lookup_by_vid_first_hit_wins() {
        let a = Fixed {
            vid_hit: Some(entry("vid-a", 0x07)),
            ..Default::default()
        };
        let b = Fixed {
            vid_hit: Some(entry("vid-b", 0x08)),
            ..Default::default()
        };
        let arr: &[&dyn KeyProvider] = &[&a, &b];
        let got = Providers(arr).lookup_disc_by_vid(&[0u8; 16]).unwrap();
        assert_eq!(got.disc_hash, "vid-a");
    }

    #[test]
    fn providers_empty_array_yields_nothing() {
        let arr: &[&dyn KeyProvider] = &[];
        let p = Providers(arr);
        assert!(p.device_keys().is_empty());
        assert!(p.processing_keys().is_empty());
        assert!(p.media_keys().is_empty());
        assert!(p.lookup_disc_by_hash(&[0u8; 20]).is_none());
        assert!(p.lookup_disc_by_vid(&[0u8; 16]).is_none());
    }

    // ── SuppliedKey: each level exposes only its own material ──────────────

    #[test]
    fn supplied_key_exposes_only_populated_fields() {
        // A SuppliedKey filled at the DK level exposes DKs and nothing else,
        // so the resolver runs the matching (DK→…) path and no other.
        let sk = SuppliedKey {
            device_keys: vec![dk(0x33, 9)],
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: None,
        };
        assert_eq!(sk.device_keys().len(), 1);
        assert!(sk.processing_keys().is_empty());
        assert!(sk.media_keys().is_empty());
        assert!(sk.lookup_disc_by_hash(&[0u8; 20]).is_none());
        assert!(sk.lookup_disc_by_vid(&[0u8; 16]).is_none());
    }

    #[test]
    fn supplied_key_disc_entry_returned_for_any_hash_or_vid() {
        // decrypt_with already knows the disc, so a present disc_entry is
        // returned regardless of the hash/VID argument (the lookup args are
        // irrelevant in this bridge).
        let sk = SuppliedKey {
            device_keys: Vec::new(),
            processing_keys: Vec::new(),
            media_keys: Vec::new(),
            disc_entry: Some(entry("supplied", 0x44)),
        };
        // Two unrelated hashes both return the same entry.
        let h1 = sk.lookup_disc_by_hash(&[0x01u8; 20]).unwrap();
        let h2 = sk.lookup_disc_by_hash(&[0xFFu8; 20]).unwrap();
        assert_eq!(h1.disc_hash, "supplied");
        assert_eq!(h2.disc_hash, "supplied");
        // And by VID likewise.
        assert!(sk.lookup_disc_by_vid(&[0x00u8; 16]).is_some());
    }
}

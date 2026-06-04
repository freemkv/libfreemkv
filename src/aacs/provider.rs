//! Key source abstraction for the AACS resolve chain.
//!
//! libfreemkv keeps all crypto (AES-G primitives, SD-tree walking,
//! validation, MK/VUK/TK derivation) but accepts key material from
//! arbitrary backends via [`KeyProvider`].
//!
//! Methods come in two flavors:
//!
//! - **Bulk material** ([`device_keys`], [`processing_keys`],
//!   [`host_certs`]) — the resolver unions results across all
//!   providers and tries each candidate.
//! - **Disc-keyed lookup** ([`lookup_disc_by_hash`],
//!   [`lookup_disc_by_vid`]) — the resolver short-circuits on the
//!   first hit, so providers are queried in array order with
//!   fastest/closest first.
//!
//! Default impls return empty / `None` so backends only override
//! the methods they actually support — an external key service might
//! implement only `lookup_disc_by_hash`, while a local file might
//! implement all five.
//!
//! Calls may block (disk I/O, network round-trips). The resolver
//! invokes each method at most a handful of times per scan; for
//! per-disc memoization, implementations should cache internally.
//!
//! [`device_keys`]: KeyProvider::device_keys
//! [`processing_keys`]: KeyProvider::processing_keys
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
/// The resolver consumes `&[&dyn KeyProvider]` directly; these
/// helpers wrap the union-vs-short-circuit policy per method.
pub(crate) struct Providers<'a>(pub &'a [&'a dyn KeyProvider]);

impl Providers<'_> {
    /// Union — gather DKs from every provider.
    pub fn device_keys(&self) -> Vec<DeviceKey> {
        self.0.iter().flat_map(|p| p.device_keys()).collect()
    }

    /// Union — gather PKs from every provider.
    pub fn processing_keys(&self) -> Vec<[u8; 16]> {
        self.0.iter().flat_map(|p| p.processing_keys()).collect()
    }

    /// Union of distinct Media Keys across every provider, for the MK-pool
    /// brute (`km_verifies` against the disc's MKB).
    pub fn media_keys(&self) -> Vec<[u8; 16]> {
        let mut v: Vec<[u8; 16]> = self.0.iter().flat_map(|p| p.media_keys()).collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Union — gather host certs from every provider. Not yet wired into
    /// the SCSI handshake (which still reads `KeyDb.host_certs` directly);
    /// kept here so a provider-aware handshake refactor is a drop-in.
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

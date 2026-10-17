//! Transport-independent player personality and protected operations.
//!
//! Profiles are provisioned by the application. The VM never performs network
//! calls or guesses missing keys. Unsupported key IDs and supported-but-missing
//! material are distinct so content code can probe player capabilities normally.
//!
//! The separation between disc content code and replaceable player identity is
//! corroborated by an [authorized-player detection patent] and a distinct
//! [player-specific content-code patent].
//!
//! [authorized-player detection patent]: https://patentimages.storage.googleapis.com/e1/ed/76/60bc2755f94e9e/US20100169663A1.pdf
//! [player-specific content-code patent]: https://patentimages.storage.googleapis.com/09/a3/1a/b8884b8530f95b/US8424101.pdf

use std::collections::{BTreeMap, BTreeSet};

/// A host capability can be present, unsupported by this player, or unprovisioned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayerReply<T> {
    Ready(T),
    Unsupported,
    Missing,
}

/// Semantic player operations used by the BD+ host. Implementations can use
/// bundled data, a local profile, or material acquired before execution.
pub trait BdPlusPlayer {
    /// Decrypt a disc-supplied wrapped key under an addressed player AES key.
    fn unwrap_aes_key(&self, key_id: u32, wrapped_key: &[u8; 16]) -> PlayerReply<[u8; 16]>;
    /// Return stable player/drive discovery bytes. Dynamic fields are filled by
    /// the session's disc/clock context according to the trap ABI.
    fn discovery(&self, device: u32, question: u32) -> PlayerReply<&[u8]>;
    /// Read modeled player memory, never the application's actual address space.
    fn memory(&self, address: u32, length: usize) -> PlayerReply<&[u8]>;
}

/// Coherent, explicitly provisioned player material. Debug redacts all values.
/// A profile never borrows keys from a different identity as an implicit fallback.
#[derive(Default)]
pub struct BdPlusPlayerProfile {
    supported_aes: BTreeSet<u32>,
    aes_keys: BTreeMap<u32, [u8; 16]>,
    discovery: BTreeMap<(u32, u32), Vec<u8>>,
    memory_regions: BTreeMap<u32, Vec<u8>>,
}

impl std::fmt::Debug for BdPlusPlayerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusPlayerProfile")
            .field("aes_capabilities", &self.supported_aes.len())
            .field("provisioned_aes_keys", &self.aes_keys.len())
            .field("discovery_records", &self.discovery.len())
            .field("memory_regions", &self.memory_regions.len())
            .finish()
    }
}

impl BdPlusPlayerProfile {
    /// Declare supported IDs separately from the secrets currently available.
    pub fn declare_aes_key(&mut self, key_id: u32) {
        self.supported_aes.insert(key_id);
    }

    pub fn set_aes_key(&mut self, key_id: u32, key: [u8; 16]) {
        self.declare_aes_key(key_id);
        self.aes_keys.insert(key_id, key);
    }

    /// Present empty data remains distinct from an absent discovery record.
    pub fn set_discovery(&mut self, device: u32, question: u32, bytes: Vec<u8>) {
        self.discovery.insert((device, question), bytes);
    }

    /// Install a modeled RAM region. Ambiguous overlaps are rejected atomically.
    pub fn add_memory(&mut self, address: u32, bytes: Vec<u8>) -> super::BdPlusResult<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = u64::from(address) + bytes.len() as u64;
        if end > u64::from(u32::MAX) + 1 {
            return Err(super::BdPlusError::MemoryRange {
                address,
                length: bytes.len(),
            });
        }
        if self.memory_regions.iter().any(|(&start, old)| {
            u64::from(start) < end && u64::from(address) < u64::from(start) + old.len() as u64
        }) {
            return Err(super::BdPlusError::PlayerMemoryOverlap { address });
        }
        self.memory_regions.insert(address, bytes);
        Ok(())
    }
}

impl BdPlusPlayer for BdPlusPlayerProfile {
    fn unwrap_aes_key(&self, key_id: u32, wrapped_key: &[u8; 16]) -> PlayerReply<[u8; 16]> {
        if !self.supported_aes.contains(&key_id) {
            return PlayerReply::Unsupported;
        }
        match self.aes_keys.get(&key_id) {
            Some(key) => PlayerReply::Ready(crate::crypto::aes_ecb_decrypt(key, wrapped_key)),
            None => PlayerReply::Missing,
        }
    }

    fn discovery(&self, device: u32, question: u32) -> PlayerReply<&[u8]> {
        match self.discovery.get(&(device, question)) {
            Some(bytes) => PlayerReply::Ready(bytes),
            None => PlayerReply::Missing,
        }
    }

    fn memory(&self, address: u32, length: usize) -> PlayerReply<&[u8]> {
        let Some((&start, bytes)) = self.memory_regions.range(..=address).next_back() else {
            return PlayerReply::Missing;
        };
        let offset = (address - start) as usize;
        let Some(end) = offset.checked_add(length) else {
            return PlayerReply::Missing;
        };
        match bytes.get(offset..end) {
            Some(bytes) => PlayerReply::Ready(bytes),
            None => PlayerReply::Missing,
        }
    }
}

#[cfg(test)]
#[path = "player_tests.rs"]
mod tests;

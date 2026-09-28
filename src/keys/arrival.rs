//! Proof on first read (KU §2.4). RED STUB: no proof; today's readers fail an unkeyed unit.

use super::{ResolvedKeySet, StopKind};
use crate::error::Result;
use crate::sector::SectorSource;

/// The on-arrival proof a set's reader carries (KU §2.4).
pub(crate) struct Arrival;

impl Arrival {
    pub(crate) fn new(_set: &ResolvedKeySet, _stop: StopKind) -> Self {
        Arrival
    }

    /// Prove and decrypt the lazy-piece units of `buf` (the stub proves nothing).
    pub(crate) fn process(
        &self,
        _inner: &mut dyn SectorSource,
        _lba: u32,
        _buf: &mut [u8],
        _unit_keys: &[(u32, [u8; 16])],
    ) -> Result<()> {
        Ok(())
    }
}

//! CSS (Content Scramble System) — DVD disc encryption.
//!
//! CSS uses a weak 40-bit LFSR stream cipher (broken since 1999).
//! No keys needed — the title key is cracked from encrypted content
//! using a known-plaintext attack on MPEG-2 PES headers.
//!
//! Usage:
//! ```rust,ignore
//! let key = css::crack_key(reader, &extents)?;
//! css::descramble_sector(&key, &mut sector);
//! ```

pub mod auth;
pub mod crack;
pub mod lfsr;
pub(crate) mod tables;

use crate::disc::Extent;
use crate::sector::SectorReader;

/// CSS decryption state for a DVD title.
#[derive(Debug, Clone)]
pub struct CssState {
    /// Cracked 5-byte title key
    pub title_key: [u8; 5],
}

/// Crack the CSS title key by reading encrypted sectors and applying
/// a known-plaintext attack on MPEG-2 headers.
///
/// Reads a few sectors from the first extent, finds one with the
/// scramble flag set, and cracks the key.
pub fn crack_key(reader: &mut dyn SectorReader, extents: &[Extent]) -> Option<CssState> {
    if extents.is_empty() {
        return None;
    }

    let ext = &extents[0];
    let mut sectors = Vec::new();

    // Read first 10 sectors from the main extent
    let count = ext.sector_count.min(10);
    for i in 0..count {
        let mut buf = vec![0u8; 2048];
        if reader.read_sectors(ext.start_lba + i, 1, &mut buf).is_ok() {
            sectors.push(buf);
        }
    }

    // Try cracking from the collected sectors
    let key = crack::crack_from_sectors(&sectors)?;

    Some(CssState { title_key: key })
}

/// Descramble a single CSS-encrypted sector in place.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    lfsr::descramble_sector(&state.title_key, sector);
}

/// Check if a sector has the CSS scramble flag set.
pub fn is_scrambled(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

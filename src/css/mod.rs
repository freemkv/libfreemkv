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
/// Crack the CSS title key by scanning scrambled sectors across extents.
///
/// The Stevenson attack needs a sector where a PES header starts at byte
/// 0x80 (start of the encrypted region). This only happens when a new PES
/// packet begins at exactly sector offset 128, which is uncommon. We scan
/// up to 500 scrambled sectors across all extents to find a crackable one.
pub fn crack_key(reader: &mut dyn SectorReader, extents: &[Extent]) -> Option<CssState> {
    let mut tried = 0u32;
    let max_tries = 500;

    for ext in extents {
        // Sample sectors spread across the extent
        let step = (ext.sector_count / 100).max(1);
        let mut i = 0;
        while i < ext.sector_count && tried < max_tries {
            let mut buf = vec![0u8; 2048];
            if reader.read_sectors(ext.start_lba + i, 1, &mut buf).is_ok() && is_scrambled(&buf) {
                if let Some(key) = crack::crack_title_key(&buf) {
                    return Some(CssState { title_key: key });
                }
                tried += 1;
            }
            i += step;
        }
    }

    None
}

/// Descramble a single CSS-encrypted sector in place.
pub fn descramble_sector(state: &CssState, sector: &mut [u8]) {
    lfsr::descramble_sector(&state.title_key, sector);
}

/// Check if a sector has the CSS scramble flag set.
pub fn is_scrambled(sector: &[u8]) -> bool {
    sector.len() >= 2048 && (sector[0x14] >> 4) & 0x03 != 0
}

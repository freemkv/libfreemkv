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
use crate::drive::Drive;
use crate::sector::SectorSource;

/// CSS decryption state for a DVD title.
#[derive(Debug, Clone)]
pub struct CssState {
    /// Cracked 5-byte title key
    pub title_key: [u8; 5],
}

/// Inputs for CSS key acquisition.
///
/// The acquisition path depends on which inputs the caller supplies:
///
/// - With `drive` + `auth_lba` set, [`resolve`] runs the full SCSI bus
///   auth + title-key path (live BU40N / DVD drive).
/// - With `reader` + `extents` set, [`resolve`] falls back to the
///   crack path (Stevenson known-plaintext attack on encrypted PES
///   headers; works on disc images and on drives whose CSS auth path
///   is unavailable).
///
/// `live_drive` always wins when both modes are populated.
pub struct CssContext<'a> {
    /// Live SCSI drive — when present, [`resolve`] tries the auth path.
    pub drive: Option<&'a mut Drive>,
    /// LBA of a known-scrambled sector for the auth path's title-key
    /// query. Required when `drive` is set.
    pub auth_lba: Option<u32>,
    /// Sector source for the crack path.
    pub reader: Option<&'a mut dyn SectorSource>,
    /// Extents to scan for the crack path. Required when `reader` is
    /// set.
    pub extents: Option<&'a [Extent]>,
}

/// Acquire a CSS title key using whichever inputs the context provides.
///
/// Order of attempts:
///   1. SCSI auth path (when `drive` and `auth_lba` are set).
///   2. Crack path (when `reader` and `extents` are set).
///
/// Returns `None` if neither path is configured or both fail.
pub fn resolve(ctx: &mut CssContext<'_>) -> Option<CssState> {
    if let (Some(drive), Some(lba)) = (ctx.drive.as_deref_mut(), ctx.auth_lba) {
        if let Ok(title_key) = auth::authenticate_and_read_title_key(drive, lba) {
            return Some(CssState { title_key });
        }
    }
    if let (Some(reader), Some(extents)) = (ctx.reader.as_deref_mut(), ctx.extents) {
        return crack_key(reader, extents);
    }
    None
}

/// Crack the CSS title key by reading encrypted sectors and applying
/// a known-plaintext attack on MPEG-2 headers.
///
/// Crack the CSS title key by scanning scrambled sectors across extents.
///
/// The Stevenson attack needs a sector where a PES header starts at byte
/// 0x80 (start of the encrypted region). This only happens when a new PES
/// packet begins at exactly sector offset 128. We scan up to 50000
/// scrambled sectors sequentially across all extents.
pub fn crack_key(reader: &mut dyn SectorSource, extents: &[Extent]) -> Option<CssState> {
    let mut tried = 0u32;
    let max_tries = 50_000;

    for ext in extents {
        let mut i = 0;
        while i < ext.sector_count && tried < max_tries {
            let mut buf = vec![0u8; 2048];
            if reader
                .read_sectors(ext.start_lba + i, 1, &mut buf, true)
                .is_ok()
                && is_scrambled(&buf)
            {
                if let Some(key) = crack::crack_title_key(&buf) {
                    return Some(CssState { title_key: key });
                }
                tried += 1;
            }
            i += 1;
        }
        if tried >= max_tries {
            break;
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

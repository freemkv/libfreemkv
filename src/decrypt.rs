//! Decrypt-on-read layer.
//!
//! Decrypts sectors in-place using resolved keys from disc scanning.
//! Handles AACS 1.0, AACS 2.0, and CSS transparently.
//! The caller never sees encrypted data unless explicitly bypassed.

use crate::aacs;
use crate::css;

/// Resolved decryption state from disc scanning.
/// Passed to `decrypt_sectors()` — the caller doesn't need to know
/// which encryption scheme is in use.
pub enum DecryptKeys {
    /// No encryption on this disc.
    None,
    /// AACS (Blu-ray / UHD). Unit keys + optional read data key.
    Aacs {
        unit_keys: Vec<(u32, [u8; 16])>,
        read_data_key: Option<[u8; 16]>,
    },
    /// CSS (DVD). Title key for sector descrambling.
    Css {
        title_key: [u8; 5],
    },
}

impl DecryptKeys {
    /// True if there are keys to decrypt with.
    pub fn is_encrypted(&self) -> bool {
        !matches!(self, DecryptKeys::None)
    }
}

/// Decrypt a buffer of sectors in-place.
///
/// For AACS: processes in 6144-byte aligned units (3 sectors).
/// For CSS: processes per 2048-byte sector.
/// For None: no-op.
///
/// `unit_key_idx` selects which AACS unit key to use (0 for most discs).
pub fn decrypt_sectors(buf: &mut [u8], keys: &DecryptKeys, unit_key_idx: usize) {
    match keys {
        DecryptKeys::None => {}
        DecryptKeys::Aacs { unit_keys, read_data_key } => {
            let uk = unit_keys
                .get(unit_key_idx)
                .map(|(_, k)| *k)
                .unwrap_or([0u8; 16]);
            let rdk = read_data_key.as_ref();
            let unit_len = aacs::ALIGNED_UNIT_LEN;

            for chunk in buf.chunks_mut(unit_len) {
                if chunk.len() == unit_len && aacs::is_unit_encrypted(chunk) {
                    aacs::decrypt_unit_full(chunk, &uk, rdk);
                }
            }
        }
        DecryptKeys::Css { title_key } => {
            for chunk in buf.chunks_mut(2048) {
                css::lfsr::descramble_sector(title_key, chunk);
            }
        }
    }
}

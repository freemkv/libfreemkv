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
#[derive(Clone)]
pub enum DecryptKeys {
    /// No encryption on this disc.
    None,
    /// AACS (Blu-ray / UHD). Unit keys + optional read data key.
    Aacs {
        unit_keys: Vec<(u32, [u8; 16])>,
        read_data_key: Option<[u8; 16]>,
    },
    /// CSS (DVD). Title key for sector descrambling.
    Css { title_key: [u8; 5] },
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
///
/// Returns `Err` if decryption was expected but keys are missing or invalid.
/// Never produces silently corrupted output.
pub fn decrypt_sectors(
    buf: &mut [u8],
    keys: &DecryptKeys,
    unit_key_idx: usize,
) -> Result<(), crate::error::Error> {
    match keys {
        DecryptKeys::None => {}
        DecryptKeys::Aacs {
            unit_keys,
            read_data_key,
        } => {
            let uk = match unit_keys.get(unit_key_idx) {
                Some((_, k)) => *k,
                None => {
                    return Err(crate::error::Error::DecryptFailed);
                }
            };
            let rdk = read_data_key.as_ref();
            let unit_len = aacs::ALIGNED_UNIT_LEN;

            for chunk in buf.chunks_mut(unit_len) {
                if chunk.len() == unit_len && aacs::is_unit_encrypted(chunk) {
                    // `is_unit_encrypted` is a byte-0 heuristic: it fires on any
                    // unit whose first byte has the top 2 bits set, which is
                    // correct for m2ts source packets (where those bits are the
                    // copy-control marker) but false-positives on any other binary
                    // data with similarly-shaped first bytes — notably MPLS/CLPI
                    // navigation files that begin with ASCII magic ('M', 'H'…)
                    // and survive sweep mixed in with encrypted m2ts payloads.
                    // `decrypt_unit_full` self-checks via TS-sync verification and
                    // returns false on a misfire, but it has already mutated the
                    // chunk by then. Snapshot and restore on verification failure
                    // — same pattern `decrypt_unit_try_keys` uses for multi-key
                    // discs. Real m2ts units verify and stay decrypted; nav-file
                    // sectors get scrambled briefly and then put back as-was.
                    let original: Vec<u8> = chunk.to_vec();
                    if !aacs::decrypt_unit_full(chunk, &uk, rdk) {
                        chunk.copy_from_slice(&original);
                    }
                }
            }
        }
        DecryptKeys::Css { title_key } => {
            for chunk in buf.chunks_mut(2048) {
                css::lfsr::descramble_sector(title_key, chunk);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression for the 0.18.1 nav-file scramble bug. A non-m2ts unit whose
    /// first byte has the top 2 bits set (here: the ASCII letter 'M' that
    /// MPLS files start with, 0x4D = 0b01001101) trips `is_unit_encrypted`,
    /// gets AES-decrypted with the unit key, fails the TS-sync verification,
    /// and must be restored to its original bytes — not left scrambled.
    #[test]
    fn nav_file_unit_survives_decrypt_attempt() {
        let mut unit = vec![0u8; aacs::ALIGNED_UNIT_LEN];
        unit[0] = b'M';
        unit[1] = b'P';
        unit[2] = b'L';
        unit[3] = b'S';
        for (i, b) in unit.iter_mut().enumerate().skip(4) {
            *b = (i as u8).wrapping_mul(31);
        }
        let snapshot = unit.clone();

        let keys = DecryptKeys::Aacs {
            unit_keys: vec![(0, [0xAB; 16])],
            read_data_key: None,
        };
        decrypt_sectors(&mut unit, &keys, 0).unwrap();
        assert_eq!(
            unit, snapshot,
            "non-m2ts unit must be restored after failed decrypt"
        );
    }
}

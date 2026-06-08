//! CSS drive authentication — full key hierarchy.
//!
//! Protocol:
//!   1. Bus authentication (challenge-response) → bus key
//!   2. Read disc key block (READ DVD STRUCTURE) → XOR with bus key → decrypt with player keys → disc key
//!   3. Read title key (REPORT KEY format 0x04) → XOR with bus key → decrypt with disc key → title key

use crate::drive::Drive;
use crate::error::{Error, Result};

// ── Built-in public DVD CSS player keys ────────────────────────────────────
//
// These 31 5-byte player keys are long-public CSS inputs. With them
// compiled in, DVD ripping works with no external key file required.

const PLAYER_KEYS: [[u8; 5]; 31] = [
    [0x01, 0xaf, 0xe3, 0x12, 0x80],
    [0x12, 0x11, 0xca, 0x04, 0x3b],
    [0x14, 0x0c, 0x9e, 0xd0, 0x09],
    [0x14, 0x71, 0x35, 0xba, 0xe2],
    [0x1a, 0xa4, 0x33, 0x21, 0xa6],
    [0x26, 0xec, 0xc4, 0xa7, 0x4e],
    [0x2c, 0xb2, 0xc1, 0x09, 0xee],
    [0x2f, 0x25, 0x9e, 0x96, 0xdd],
    [0x33, 0x2f, 0x49, 0x6c, 0xe0],
    [0x35, 0x5b, 0xc1, 0x31, 0x0f],
    [0x36, 0x67, 0xb2, 0xe3, 0x85],
    [0x39, 0x3d, 0xf1, 0xf1, 0xbd],
    [0x3b, 0x31, 0x34, 0x0d, 0x91],
    [0x45, 0xed, 0x28, 0xeb, 0xd3],
    [0x48, 0xb7, 0x6c, 0xce, 0x69],
    [0x4b, 0x65, 0x0d, 0xc1, 0xee],
    [0x4c, 0xbb, 0xf5, 0x5b, 0x23],
    [0x51, 0x67, 0x67, 0xc5, 0xe0],
    [0x53, 0x94, 0xe1, 0x75, 0xbf],
    [0x57, 0x2c, 0x8b, 0x31, 0xae],
    [0x63, 0xdb, 0x4c, 0x5b, 0x4a],
    [0x7b, 0x1e, 0x5e, 0x2b, 0x57],
    [0x85, 0xf3, 0x85, 0xa0, 0xe0],
    [0xab, 0x1e, 0xe7, 0x7b, 0x72],
    [0xab, 0x36, 0xe3, 0xeb, 0x76],
    [0xb1, 0xb8, 0xf9, 0x38, 0x03],
    [0xb8, 0x5d, 0xd8, 0x53, 0xbd],
    [0xbf, 0x92, 0xc3, 0xb0, 0xe2],
    [0xcf, 0x1a, 0xb2, 0xf8, 0x0a],
    [0xec, 0xa0, 0xcf, 0xb3, 0xff],
    [0xfc, 0x95, 0xa9, 0x87, 0x35],
];

// ── CryptKey tables ───────────────────────────────────────────────────────

const CRYPT_TAB0: [u8; 256] = [
    0xB7, 0xF4, 0x82, 0x57, 0xDA, 0x4D, 0xDB, 0xE2, 0x2F, 0x52, 0x1A, 0xA8, 0x68, 0x5A, 0x8A, 0xFF,
    0xFB, 0x0E, 0x6D, 0x35, 0xF7, 0x5C, 0x76, 0x12, 0xCE, 0x25, 0x79, 0x29, 0x39, 0x62, 0x08, 0x24,
    0xA5, 0x85, 0x7B, 0x56, 0x01, 0x23, 0x68, 0xCF, 0x0A, 0xE2, 0x5A, 0xED, 0x3D, 0x59, 0xB0, 0xA9,
    0xB0, 0x2C, 0xF2, 0xB8, 0xEF, 0x32, 0xA9, 0x40, 0x80, 0x71, 0xAF, 0x1E, 0xDE, 0x8F, 0x58, 0x88,
    0xB8, 0x3A, 0xD0, 0xFC, 0xC4, 0x1E, 0xB5, 0xA0, 0xBB, 0x3B, 0x0F, 0x01, 0x7E, 0x1F, 0x9F, 0xD9,
    0xAA, 0xB8, 0x3D, 0x9D, 0x74, 0x1E, 0x25, 0xDB, 0x37, 0x56, 0x8F, 0x16, 0xBA, 0x49, 0x2B, 0xAC,
    0xD0, 0xBD, 0x95, 0x20, 0xBE, 0x7A, 0x28, 0xD0, 0x51, 0x64, 0x63, 0x1C, 0x7F, 0x66, 0x10, 0xBB,
    0xC4, 0x56, 0x1A, 0x04, 0x6E, 0x0A, 0xEC, 0x9C, 0xD6, 0xE8, 0x9A, 0x7A, 0xCF, 0x8C, 0xDB, 0xB1,
    0xEF, 0x71, 0xDE, 0x31, 0xFF, 0x54, 0x3E, 0x5E, 0x07, 0x69, 0x96, 0xB0, 0xCF, 0xDD, 0x9E, 0x47,
    0xC7, 0x96, 0x8F, 0xE4, 0x2B, 0x59, 0xC6, 0xEE, 0xB9, 0x86, 0x9A, 0x64, 0x84, 0x72, 0xE2, 0x5B,
    0xA2, 0x96, 0x58, 0x99, 0x50, 0x03, 0xF5, 0x38, 0x4D, 0x02, 0x7D, 0xE7, 0x7D, 0x75, 0xA7, 0xB8,
    0x67, 0x87, 0x84, 0x3F, 0x1D, 0x11, 0xE5, 0xFC, 0x1E, 0xD3, 0x83, 0x16, 0xA5, 0x29, 0xF6, 0xC7,
    0x15, 0x61, 0x29, 0x1A, 0x43, 0x4F, 0x9B, 0xAF, 0xC5, 0x87, 0x34, 0x6C, 0x0F, 0x3B, 0xA8, 0x1D,
    0x45, 0x58, 0x25, 0xDC, 0xA8, 0xA3, 0x3B, 0xD1, 0x79, 0x1B, 0x48, 0xF2, 0xE9, 0x93, 0x1F, 0xFC,
    0xDB, 0x2A, 0x90, 0xA9, 0x8A, 0x3D, 0x39, 0x18, 0xA3, 0x8E, 0x58, 0x6C, 0xE0, 0x12, 0xBB, 0x25,
    0xCD, 0x71, 0x22, 0xA2, 0x64, 0xC6, 0xE7, 0xFB, 0xAD, 0x94, 0x77, 0x04, 0x9A, 0x39, 0xCF, 0x7C,
];

const CRYPT_TAB1: [u8; 256] = [
    0x8C, 0x47, 0xB0, 0xE1, 0xEB, 0xFC, 0xEB, 0x56, 0x10, 0xE5, 0x2C, 0x1A, 0x5D, 0xEF, 0xBE, 0x4F,
    0x08, 0x75, 0x97, 0x4B, 0x0E, 0x25, 0x8E, 0x6E, 0x39, 0x5A, 0x87, 0x53, 0xC4, 0x1F, 0xF4, 0x5C,
    0x4E, 0xE6, 0x99, 0x30, 0xE0, 0x42, 0x88, 0xAB, 0xE5, 0x85, 0xBC, 0x8F, 0xD8, 0x3C, 0x54, 0xC9,
    0x53, 0x47, 0x18, 0xD6, 0x06, 0x5B, 0x41, 0x2C, 0x67, 0x1E, 0x41, 0x74, 0x33, 0xE2, 0xB4, 0xE0,
    0x23, 0x29, 0x42, 0xEA, 0x55, 0x0F, 0x25, 0xB4, 0x24, 0x2C, 0x99, 0x13, 0xEB, 0x0A, 0x0B, 0xC9,
    0xF9, 0x63, 0x67, 0x43, 0x2D, 0xC7, 0x7D, 0x07, 0x60, 0x89, 0xD1, 0xCC, 0xE7, 0x94, 0x77, 0x74,
    0x9B, 0x7E, 0xD7, 0xE6, 0xFF, 0xBB, 0x68, 0x14, 0x1E, 0xA3, 0x25, 0xDE, 0x3A, 0xA3, 0x54, 0x7B,
    0x87, 0x9D, 0x50, 0xCA, 0x27, 0xC3, 0xA4, 0x50, 0x91, 0x27, 0xD4, 0xB0, 0x82, 0x41, 0x97, 0x79,
    0x94, 0x82, 0xAC, 0xC7, 0x8E, 0xA5, 0x4E, 0xAA, 0x78, 0x9E, 0xE0, 0x42, 0xBA, 0x28, 0xEA, 0xB7,
    0x74, 0xAD, 0x35, 0xDA, 0x92, 0x60, 0x7E, 0xD2, 0x0E, 0xB9, 0x24, 0x5E, 0x39, 0x4F, 0x5E, 0x63,
    0x09, 0xB5, 0xFA, 0xBF, 0xF1, 0x22, 0x55, 0x1C, 0xE2, 0x25, 0xDB, 0xC5, 0xD8, 0x50, 0x03, 0x98,
    0xC4, 0xAC, 0x2E, 0x11, 0xB4, 0x38, 0x4D, 0xD0, 0xB9, 0xFC, 0x2D, 0x3C, 0x08, 0x04, 0x5A, 0xEF,
    0xCE, 0x32, 0xFB, 0x4C, 0x92, 0x1E, 0x4B, 0xFB, 0x1A, 0xD0, 0xE2, 0x3E, 0xDA, 0x6E, 0x7C, 0x4D,
    0x56, 0xC3, 0x3F, 0x42, 0xB1, 0x3A, 0x23, 0x4D, 0x6E, 0x84, 0x56, 0x68, 0xF4, 0x0E, 0x03, 0x64,
    0xD0, 0xA9, 0x92, 0x2F, 0x8B, 0xBC, 0x39, 0x9C, 0xAC, 0x09, 0x5E, 0xEE, 0xE5, 0x97, 0xBF, 0xA5,
    0xCE, 0xFA, 0x28, 0x2C, 0x6D, 0x4F, 0xEF, 0x77, 0xAA, 0x1B, 0x79, 0x8E, 0x97, 0xB4, 0xC3, 0xF4,
];

const CRYPT_TAB2: [u8; 256] = [
    0xB7, 0x75, 0x81, 0xD5, 0xDC, 0xCA, 0xDE, 0x66, 0x23, 0xDF, 0x15, 0x26, 0x62, 0xD1, 0x83, 0x77,
    0xE3, 0x97, 0x76, 0xAF, 0xE9, 0xC3, 0x6B, 0x8E, 0xDA, 0xB0, 0x6E, 0xBF, 0x2B, 0xF1, 0x19, 0xB4,
    0x95, 0x34, 0x48, 0xE4, 0x37, 0x94, 0x5D, 0x7B, 0x36, 0x5F, 0x65, 0x53, 0x07, 0xE2, 0x89, 0x11,
    0x98, 0x85, 0xD9, 0x12, 0xC1, 0x9D, 0x84, 0xEC, 0xA4, 0xD4, 0x88, 0xB8, 0xFC, 0x2C, 0x79, 0x28,
    0xD8, 0xDB, 0xB3, 0x1E, 0xA2, 0xF9, 0xD0, 0x44, 0xD7, 0xD6, 0x60, 0xEF, 0x14, 0xF4, 0xF6, 0x31,
    0xD2, 0x41, 0x46, 0x67, 0x0A, 0xE1, 0x58, 0x27, 0x43, 0xA3, 0xF8, 0xE0, 0xC8, 0xBA, 0x5A, 0x5C,
    0x80, 0x6C, 0xC6, 0xF2, 0xE8, 0xAD, 0x7D, 0x04, 0x0D, 0xB9, 0x3C, 0xC2, 0x25, 0xBD, 0x49, 0x63,
    0x8C, 0x9F, 0x51, 0xCE, 0x20, 0xC5, 0xA1, 0x50, 0x92, 0x2D, 0xDD, 0xBC, 0x8D, 0x4F, 0x9A, 0x71,
    0x2F, 0x30, 0x1D, 0x73, 0x39, 0x13, 0xFB, 0x1A, 0xCB, 0x24, 0x59, 0xFE, 0x05, 0x96, 0x57, 0x0F,
    0x1F, 0xCF, 0x54, 0xBE, 0xF5, 0x06, 0x1B, 0xB2, 0x6D, 0xD3, 0x4D, 0x32, 0x56, 0x21, 0x33, 0x0B,
    0x52, 0xE7, 0xAB, 0xEB, 0xA6, 0x74, 0x00, 0x4C, 0xB1, 0x7F, 0x82, 0x99, 0x87, 0x0E, 0x5E, 0xC0,
    0x8F, 0xEE, 0x6F, 0x55, 0xF3, 0x7E, 0x08, 0x90, 0xFA, 0xB6, 0x64, 0x70, 0x47, 0x4A, 0x17, 0xA7,
    0xB5, 0x40, 0x8A, 0x38, 0xE5, 0x68, 0x3E, 0x8B, 0x69, 0xAA, 0x9B, 0x42, 0xA5, 0x10, 0x01, 0x35,
    0xFD, 0x61, 0x9E, 0xE6, 0x16, 0x9C, 0x86, 0xED, 0xCD, 0x2E, 0xFF, 0xC4, 0x5B, 0xA0, 0xAE, 0xCC,
    0x4B, 0x3B, 0x03, 0xBB, 0x1C, 0x2A, 0xAC, 0x0C, 0x3F, 0x93, 0xC7, 0x72, 0x7A, 0x09, 0x22, 0x3D,
    0x45, 0x78, 0xA9, 0xA8, 0xEA, 0xC9, 0x6A, 0xF7, 0x29, 0x91, 0xF0, 0x02, 0x18, 0x3A, 0x4E, 0x7C,
];

const CRYPT_TAB3: [u8; 256] = [
    0x73, 0x51, 0x95, 0xE1, 0x12, 0xE4, 0xC0, 0x58, 0xEE, 0xF2, 0x08, 0x1B, 0xA9, 0xFA, 0x98, 0x4C,
    0xA7, 0x33, 0xE2, 0x1B, 0xA7, 0x6D, 0xF5, 0x30, 0x97, 0x1D, 0xF3, 0x02, 0x60, 0x5A, 0x82, 0x0F,
    0x91, 0xD0, 0x9C, 0x10, 0x39, 0x7A, 0x83, 0x85, 0x3B, 0xB2, 0xB8, 0xAE, 0x0C, 0x09, 0x52, 0xEA,
    0x1C, 0xE1, 0x8D, 0x66, 0x4F, 0xF3, 0xDA, 0x92, 0x29, 0xB9, 0xD5, 0xC5, 0x77, 0x47, 0x22, 0x53,
    0x14, 0xF7, 0xAF, 0x22, 0x64, 0xDF, 0xC6, 0x72, 0x12, 0xF3, 0x75, 0xDA, 0xD7, 0xD7, 0xE5, 0x02,
    0x9E, 0xED, 0xDA, 0xDB, 0x4C, 0x47, 0xCE, 0x91, 0x06, 0x06, 0x6D, 0x55, 0x8B, 0x19, 0xC9, 0xEF,
    0x8C, 0x80, 0x1A, 0x0E, 0xEE, 0x4B, 0xAB, 0xF2, 0x08, 0x5C, 0xE9, 0x37, 0x26, 0x5E, 0x9A, 0x90,
    0x00, 0xF3, 0x0D, 0xB2, 0xA6, 0xA3, 0xF7, 0x26, 0x17, 0x48, 0x88, 0xC9, 0x0E, 0x2C, 0xC9, 0x02,
    0xE7, 0x18, 0x05, 0x4B, 0xF3, 0x39, 0xE1, 0x20, 0x02, 0x0D, 0x40, 0xC7, 0xCA, 0xB9, 0x48, 0x30,
    0x57, 0x67, 0xCC, 0x06, 0xBF, 0xAC, 0x81, 0x08, 0x24, 0x7A, 0xD4, 0x8B, 0x19, 0x8E, 0xAC, 0xB4,
    0x5A, 0x0F, 0x73, 0x13, 0xAC, 0x9E, 0xDA, 0xB6, 0xB8, 0x96, 0x5B, 0x60, 0x88, 0xE1, 0x81, 0x3F,
    0x07, 0x86, 0x37, 0x2D, 0x79, 0x14, 0x52, 0xEA, 0x73, 0xDF, 0x3D, 0x09, 0xC8, 0x25, 0x48, 0xD8,
    0x75, 0x60, 0x9A, 0x08, 0x27, 0x4A, 0x2C, 0xB9, 0xA8, 0x8B, 0x8A, 0x73, 0x62, 0x37, 0x16, 0x02,
    0xBD, 0xC1, 0x0E, 0x56, 0x54, 0x3E, 0x14, 0x5F, 0x8C, 0x8F, 0x6E, 0x75, 0x1C, 0x07, 0x39, 0x7B,
    0x4B, 0xDB, 0xD3, 0x4B, 0x1E, 0xC8, 0x7E, 0xFE, 0x3E, 0x72, 0x16, 0x83, 0x7D, 0xEE, 0xF5, 0xCA,
    0xC5, 0x18, 0xF9, 0xD8, 0x68, 0xAB, 0x38, 0x85, 0xA8, 0xF0, 0xA1, 0x73, 0x9F, 0x5D, 0x19, 0x0B,
];

const VARIANTS: [u8; 32] = [
    0xB7, 0x74, 0x85, 0xD0, 0xCC, 0xDB, 0xCA, 0x73, 0x03, 0xFE, 0x31, 0x03, 0x52, 0xE0, 0xB7, 0x42,
    0x63, 0x16, 0xF2, 0x2A, 0x79, 0x52, 0xFF, 0x1B, 0x7A, 0x11, 0xCA, 0x1A, 0x9B, 0x40, 0xAD, 0x01,
];

const SECRET: [u8; 5] = [0x55, 0xD6, 0xC4, 0xC5, 0x28];

const PERM_CHALLENGE: [[usize; 10]; 3] = [
    [1, 3, 0, 7, 5, 2, 9, 6, 4, 8],
    [6, 1, 9, 3, 8, 5, 7, 4, 0, 2],
    [4, 0, 3, 5, 7, 2, 8, 6, 1, 9],
];

const PERM_VARIANT: [[u8; 32]; 2] = [
    [
        0x0A, 0x08, 0x0E, 0x0C, 0x0B, 0x09, 0x0F, 0x0D, 0x1A, 0x18, 0x1E, 0x1C, 0x1B, 0x19, 0x1F,
        0x1D, 0x02, 0x00, 0x06, 0x04, 0x03, 0x01, 0x07, 0x05, 0x12, 0x10, 0x16, 0x14, 0x13, 0x11,
        0x17, 0x15,
    ],
    [
        0x12, 0x1A, 0x16, 0x1E, 0x02, 0x0A, 0x06, 0x0E, 0x10, 0x18, 0x14, 0x1C, 0x00, 0x08, 0x04,
        0x0C, 0x13, 0x1B, 0x17, 0x1F, 0x03, 0x0B, 0x07, 0x0F, 0x11, 0x19, 0x15, 0x1D, 0x01, 0x09,
        0x05, 0x0D,
    ],
];

// ── Public API ────────────────────────────────────────────────────────────

/// Perform CSS bus authentication only.
pub fn authenticate(drive: &mut Drive) -> Result<()> {
    let (_, _) = bus_auth(drive)?;
    Ok(())
}

/// Full CSS key extraction: bus auth → disc key → title key.
pub fn authenticate_and_read_title_key(drive: &mut Drive, lba: u32) -> Result<[u8; 5]> {
    // Session 1: bus auth → disc key (AGID consumed by READ_DVD_STRUCTURE)
    let (agid, bus_key) = bus_auth(drive)?;
    let disc_key = read_disc_key(drive, agid, &bus_key)?;

    // Session 2: fresh bus auth → title key (needs separate AGID)
    let (agid2, bus_key2) = bus_auth(drive)?;
    let encrypted_title = read_raw_title_key(drive, agid2, lba)?;

    // Decrypt title key: XOR with bus key, then decrypt with disc key
    let mut title_key = [0u8; 5];
    for i in 0..5 {
        title_key[i] = encrypted_title[i] ^ bus_key2[i];
    }

    if title_key == [0u8; 5] {
        return Ok(title_key);
    }

    let title_key = super::lfsr::decrypt_key(0xFF, &disc_key, &title_key);
    Ok(title_key)
}

// ── Step 1: Bus Authentication ────────────────────────────────────────────

fn bus_auth(drive: &mut Drive) -> Result<(u8, [u8; 5])> {
    let scsi = drive.scsi_mut();

    // Invalidate all AGIDs via REPORT KEY format 0x3F
    for agid in 0..4u8 {
        let mut cdb = [0u8; 12];
        cdb[0] = crate::scsi::SCSI_REPORT_KEY;
        // alloc_len = 0 (no data transfer)
        cdb[10] = (agid << 6) | 0x3F;
        let mut buf = [0u8; 8];
        let _ = scsi.execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        );
    }

    // Allocate AGID
    let mut buf = [0u8; 8];
    scsi.execute(
        &report_key_cdb(0, 0x00, 8),
        crate::scsi::DataDirection::FromDevice,
        &mut buf,
        5_000,
    )
    .map_err(|_| Error::CssAuthFailed)?;
    let agid = (buf[7] >> 6) & 0x03;

    // Host sends challenge
    let host_challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut hc_buf = [0u8; 16];
    hc_buf[0] = 0x00;
    hc_buf[1] = 0x0E;
    for i in 0..10 {
        hc_buf[4 + i] = host_challenge[9 - i];
    }
    scsi.execute(
        &send_key_cdb(agid, 0x01, 16),
        crate::scsi::DataDirection::ToDevice,
        &mut hc_buf,
        5_000,
    )
    .map_err(|_| Error::CssAuthFailed)?;

    // Get Key1 from drive
    let mut dk_buf = [0u8; 12];
    scsi.execute(
        &report_key_cdb(agid, 0x02, 12),
        crate::scsi::DataDirection::FromDevice,
        &mut dk_buf,
        5_000,
    )
    .map_err(|_| Error::CssAuthFailed)?;
    let mut key1 = [0u8; 5];
    for i in 0..5 {
        key1[i] = dk_buf[4 + (4 - i)];
    }

    // Brute-force variant (0-31)
    let mut variant: Option<u8> = None;
    for v in 0..32u8 {
        if crypt_key(0, v, &host_challenge) == key1 {
            variant = Some(v);
            break;
        }
    }
    let variant = variant.ok_or(Error::CssAuthFailed)?;

    // Get drive challenge
    let mut dc_buf = [0u8; 16];
    scsi.execute(
        &report_key_cdb(agid, 0x01, 16),
        crate::scsi::DataDirection::FromDevice,
        &mut dc_buf,
        5_000,
    )
    .map_err(|_| Error::CssAuthFailed)?;
    let mut drive_challenge = [0u8; 10];
    for i in 0..10 {
        drive_challenge[i] = dc_buf[4 + (9 - i)];
    }

    // Compute Key2 and send it
    let key2 = crypt_key(1, variant, &drive_challenge);
    let mut hk_buf = [0u8; 12];
    hk_buf[0] = 0x00;
    hk_buf[1] = 0x0A;
    for i in 0..5 {
        hk_buf[4 + i] = key2[4 - i];
    }
    scsi.execute(
        &send_key_cdb(agid, 0x03, 12),
        crate::scsi::DataDirection::ToDevice,
        &mut hk_buf,
        5_000,
    )
    .map_err(|_| Error::CssAuthFailed)?;

    // Bus key = CryptKey(2, variant, key1 || key2)
    let mut combined = [0u8; 10];
    combined[..5].copy_from_slice(&key1);
    combined[5..].copy_from_slice(&key2);
    let bus_key = crypt_key(2, variant, &combined);

    Ok((agid, bus_key))
}

// ── Step 2: Disc Key ──────────────────────────────────────────────────────

fn read_disc_key(drive: &mut Drive, agid: u8, bus_key: &[u8; 5]) -> Result<[u8; 5]> {
    let scsi = drive.scsi_mut();

    // READ DVD STRUCTURE, format 0x02 (disc key), 2048+4 bytes
    let alloc_len: u16 = 2048 + 4;
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_READ_DISC_STRUCTURE;
    // bytes 2-5: address = 0
    cdb[6] = 0; // layer
    cdb[7] = 0x02; // format = disc key
    cdb[8] = (alloc_len >> 8) as u8;
    cdb[9] = alloc_len as u8;
    cdb[10] = agid << 6;

    let mut buf = vec![0u8; alloc_len as usize];
    let dvd_result = scsi.execute(
        &cdb,
        crate::scsi::DataDirection::FromDevice,
        &mut buf,
        5_000,
    );
    dvd_result.map_err(|_| Error::CssAuthFailed)?;

    // Disc key block starts at offset 4 (skip 4-byte header)
    let disc_key_block = &mut buf[4..4 + 2048];

    // XOR with reversed bus key (per libdvdcss)
    for (i, byte) in disc_key_block.iter_mut().enumerate() {
        *byte ^= bus_key[4 - (i % 5)];
    }

    // Try each player key against each of 408 disc key entries.
    // Each entry in the block is the disc key encrypted with a specific player
    // key. We collect every decryption and accept the disc key as soon as two
    // independent decryptions agree on the same 5-byte value (the agreement may
    // come from two different player keys or from one player key decrypting two
    // different entries to the same value).
    //
    // NOTE: this is a collision heuristic, not the canonical CSS disc-key
    // self-verification (which decrypts the verification entry with the
    // candidate and checks the result equals the candidate). A coincidental
    // collision among the ~12,648 candidate decryptions could in principle
    // accept a wrong disc key; in practice a chance collision on 5 bytes is
    // improbable enough to serve as the validity check, and this path is the
    // production DVD disc-key recovery. Left as-is to avoid regressing it
    // without a real disc-key-block test vector to validate against.
    let mut candidates: Vec<[u8; 5]> = Vec::new();

    for player_key in PLAYER_KEYS.iter() {
        for pos in 0..408 {
            let offset = pos * 5;
            if offset + 5 > disc_key_block.len() {
                break;
            }
            let mut enc = [0u8; 5];
            enc.copy_from_slice(&disc_key_block[offset..offset + 5]);
            let candidate = super::lfsr::decrypt_key(0x00, player_key, &enc);

            // Accept on the first agreement between two independent decryptions.
            if candidates.contains(&candidate) {
                return Ok(candidate);
            }
            candidates.push(candidate);
        }
    }

    Err(Error::CssAuthFailed)
}

// ── Step 3: Title Key ─────────────────────────────────────────────────────

/// Read the raw (bus-encrypted) title key bytes from the drive.
fn read_raw_title_key(drive: &mut Drive, agid: u8, lba: u32) -> Result<[u8; 5]> {
    let scsi = drive.scsi_mut();
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_REPORT_KEY;
    cdb[2] = (lba >> 24) as u8;
    cdb[3] = (lba >> 16) as u8;
    cdb[4] = (lba >> 8) as u8;
    cdb[5] = lba as u8;
    cdb[8] = 0x00;
    cdb[9] = 0x0C;
    cdb[10] = (agid << 6) | 0x04;

    let mut buf = [0u8; 12];
    let result = scsi.execute(
        &cdb,
        crate::scsi::DataDirection::FromDevice,
        &mut buf,
        5_000,
    );
    result.map_err(|_| Error::CssAuthFailed)?;

    let mut key = [0u8; 5];
    for i in 0..5 {
        key[i] = buf[5 + (4 - i)];
    }
    Ok(key)
}

// ── CSSCryptKey ───────────────────────────────────────────────────────────

fn crypt_key(key_type: usize, variant: u8, challenge: &[u8; 10]) -> [u8; 5] {
    // key_type indexes PERM_CHALLENGE ([_;3]); variant indexes
    // VARIANTS/PERM_VARIANT ([_;32]). All internal callers pass key_type in
    // 0..3 and variant in 0..32; the asserts document the contract for the
    // pub(crate) test entry point test_crypt_key and turn a would-be
    // out-of-bounds panic into an explicit precondition violation.
    debug_assert!(key_type < 3, "crypt_key: key_type out of range");
    debug_assert!((variant as usize) < 32, "crypt_key: variant out of range");
    let perm = &PERM_CHALLENGE[key_type];
    let mut scratch = [0u8; 10];
    for i in 0..10 {
        scratch[i] = challenge[perm[i]];
    }

    let css_variant = match key_type {
        0 => variant as usize,
        1 => PERM_VARIANT[0][variant as usize] as usize,
        _ => PERM_VARIANT[1][variant as usize] as usize,
    };

    let cse = VARIANTS[css_variant] ^ CRYPT_TAB2[css_variant];

    let mut tmp1 = [0u8; 5];
    for i in 0..5 {
        tmp1[i] = scratch[5 + i] ^ SECRET[i] ^ CRYPT_TAB2[i];
    }

    let mut lfsr0: u32 = ((tmp1[0] as u32) << 17)
        | ((tmp1[1] as u32) << 9)
        | (((tmp1[2] as u32) & !7) << 1)
        | 8
        | (tmp1[2] as u32 & 7);

    let mut lfsr1: u32 = ((tmp1[3] as u32) << 9) | 0x100 | (tmp1[4] as u32);

    let mut bits = [0u8; 30];
    let mut carry: u32 = 0;
    for idx in (0..30).rev() {
        let mut val: u8 = 0;
        for bit in 0..8u8 {
            let lfsr0_out = ((lfsr0 >> 24) ^ (lfsr0 >> 21) ^ (lfsr0 >> 20) ^ (lfsr0 >> 12)) & 1;
            lfsr0 = ((lfsr0 << 1) | lfsr0_out) & 0x1FFFFFF;

            let lfsr1_out = ((lfsr1 >> 16) ^ (lfsr1 >> 2)) & 1;
            lfsr1 = ((lfsr1 << 1) | lfsr1_out) & 0x1FFFF;

            let combined = ((!lfsr1_out) & 1) + carry + ((!lfsr0_out) & 1);
            carry = (combined >> 1) & 1;
            val |= ((combined & 1) as u8) << bit;
        }
        bits[idx] = val;
    }

    let mut tmp1 = [scratch[0], scratch[1], scratch[2], scratch[3], scratch[4]];
    let mut tmp2 = [0u8; 5];

    // Round 1: bits[25..29] ^ scratch -> tmp1 (term from original scratch)
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[25 + i] ^ tmp1[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            tmp1[i] = CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term;
            term = scratch[i]; // original challenge, NOT modified tmp1
        }
        tmp1[4] ^= tmp1[0];
    }

    // Round 2
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[20 + i] ^ tmp1[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            tmp2[i] = CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term;
            term = tmp1[i];
        }
        tmp2[4] ^= tmp2[0];
    }

    // Round 3 (uses CRYPT_TAB0)
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[15 + i] ^ tmp2[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            let idx3 = (CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term) as usize;
            tmp1[i] = CRYPT_TAB0[idx3] ^ CRYPT_TAB2[idx3];
            term = tmp2[i];
        }
        tmp1[4] ^= tmp1[0];
    }

    // Round 4 (uses CRYPT_TAB0)
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[10 + i] ^ tmp1[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            let idx3 = (CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term) as usize;
            tmp2[i] = CRYPT_TAB0[idx3] ^ CRYPT_TAB2[idx3];
            term = tmp1[i];
        }
        tmp2[4] ^= tmp2[0];
    }

    // Round 5
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[5 + i] ^ tmp2[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            tmp1[i] = CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term;
            term = tmp2[i];
        }
        tmp1[4] ^= tmp1[0];
    }

    // Round 6
    let mut key = [0u8; 5];
    {
        let mut term: u8 = 0;
        for i in (0..5usize).rev() {
            let idx = (bits[i] ^ tmp1[i]) as usize;
            let idx2 = (CRYPT_TAB1[idx] ^ (!CRYPT_TAB2[idx]) ^ cse) as usize;
            key[i] = CRYPT_TAB2[idx2] ^ CRYPT_TAB3[idx2] ^ term;
            term = tmp1[i];
        }
    }

    key
}

// ── SCSI CDB builders ────────────────────────────────────────────────────

fn report_key_cdb(agid: u8, format: u8, alloc_len: u16) -> [u8; 12] {
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_REPORT_KEY;
    cdb[8] = (alloc_len >> 8) as u8;
    cdb[9] = alloc_len as u8;
    cdb[10] = (agid << 6) | (format & 0x3F);
    cdb
}

fn send_key_cdb(agid: u8, format: u8, param_len: u16) -> [u8; 12] {
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_SEND_KEY;
    cdb[8] = (param_len >> 8) as u8;
    cdb[9] = param_len as u8;
    cdb[10] = (agid << 6) | (format & 0x3F);
    cdb
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypt_key_is_deterministic() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        for v in 0..32u8 {
            let r1 = crypt_key(0, v, &challenge);
            let r2 = crypt_key(0, v, &challenge);
            assert_eq!(r1, r2);
        }
    }

    #[test]
    fn crypt_key_varies_by_variant() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        assert_ne!(crypt_key(0, 0, &challenge), crypt_key(0, 1, &challenge));
    }

    #[test]
    fn crypt_key_varies_by_type() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        assert_ne!(crypt_key(0, 5, &challenge), crypt_key(1, 5, &challenge));
    }

    #[test]
    fn crypt_key_nonzero() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        for v in 0..32u8 {
            assert_ne!(crypt_key(0, v, &challenge), [0u8; 5]);
        }
    }

    #[test]
    fn player_keys_count() {
        assert_eq!(PLAYER_KEYS.len(), 31);
    }

    // ── CSS constant-table integrity ───────────────────────────────────────

    /// The CSSCryptKey lookup tables are each a full 256-entry byte table and
    /// the variant tables are 32 entries (one per CSS variant). The cipher
    /// indexes CRYPT_TAB0..3 with arbitrary bytes (0..256) and indexes
    /// VARIANTS / PERM_VARIANT with the css_variant (0..32). A short table
    /// would index out of bounds.
    ///
    /// Grounding: crypt_key indexes `CRYPT_TABx[idx]` where idx is a u8 cast
    /// to usize (0..256); `VARIANTS[css_variant]` and `PERM_VARIANT[k][variant]`
    /// with variant 0..32.
    /// Mutation: drop the last entry of CRYPT_TAB0 (make it [u8;255]) ->
    /// compile error / length assert fails.
    #[test]
    fn crypt_tables_have_spec_lengths() {
        assert_eq!(CRYPT_TAB0.len(), 256);
        assert_eq!(CRYPT_TAB1.len(), 256);
        assert_eq!(CRYPT_TAB2.len(), 256);
        assert_eq!(CRYPT_TAB3.len(), 256);
        assert_eq!(VARIANTS.len(), 32, "one CSS variant byte per variant 0..32");
        assert_eq!(PERM_VARIANT.len(), 2);
        assert_eq!(PERM_VARIANT[0].len(), 32);
        assert_eq!(PERM_VARIANT[1].len(), 32);
        assert_eq!(
            PERM_CHALLENGE.len(),
            3,
            "one challenge perm per key_type 0..3"
        );
        for p in &PERM_CHALLENGE {
            assert_eq!(
                p.len(),
                10,
                "challenge permutation covers all 10 challenge bytes"
            );
        }
        assert_eq!(SECRET.len(), 5);
    }

    /// Each PERM_CHALLENGE row is a permutation of indices 0..10 (it reorders
    /// the 10 challenge bytes). A non-permutation would drop/duplicate
    /// challenge bytes, weakening or corrupting the bus key derivation.
    ///
    /// Grounding: crypt_key does `scratch[i] = challenge[perm[i]]` for i in
    /// 0..10 — perm must be a bijection on 0..10 to use every challenge byte
    /// exactly once.
    /// Mutation: change PERM_CHALLENGE[0] entry `9` to `8` (duplicate) -> the
    /// "covers 0..10" assert fires.
    #[test]
    fn perm_challenge_rows_are_permutations() {
        for (row, perm) in PERM_CHALLENGE.iter().enumerate() {
            let mut seen = [false; 10];
            for &idx in perm.iter() {
                assert!(idx < 10, "PERM_CHALLENGE[{row}] index {idx} out of range");
                assert!(!seen[idx], "PERM_CHALLENGE[{row}] duplicates index {idx}");
                seen[idx] = true;
            }
            assert!(
                seen.iter().all(|&b| b),
                "PERM_CHALLENGE[{row}] misses an index"
            );
        }
    }

    /// Each PERM_VARIANT row maps the 32 variants to 32 distinct 5-bit values
    /// (it is a permutation of 0..32). key_type 1 uses PERM_VARIANT[0],
    /// key_type 2 uses PERM_VARIANT[1] to pick the css_variant; a collision
    /// would make two variants indistinguishable.
    ///
    /// Grounding: `css_variant = PERM_VARIANT[k][variant]` then indexes
    /// VARIANTS[css_variant] (0..32).
    /// Mutation: set PERM_VARIANT[0][1] = PERM_VARIANT[0][0] -> duplicate
    /// assert fires; also any value >= 32 would later index VARIANTS OOB.
    #[test]
    fn perm_variant_rows_are_permutations_of_0_31() {
        for (row, perm) in PERM_VARIANT.iter().enumerate() {
            let mut seen = [false; 32];
            for &v in perm.iter() {
                let v = v as usize;
                assert!(v < 32, "PERM_VARIANT[{row}] value {v} out of 0..32");
                assert!(!seen[v], "PERM_VARIANT[{row}] duplicates {v}");
                seen[v] = true;
            }
            assert!(
                seen.iter().all(|&b| b),
                "PERM_VARIANT[{row}] misses a value"
            );
        }
    }

    /// The 31 built-in player keys are all distinct. Duplicate keys would
    /// waste disc-key trials and could mask a copy-paste error in the table.
    ///
    /// Grounding: PLAYER_KEYS is the set of long-public CSS player keys; each
    /// is a unique 5-byte key.
    /// Mutation: set PLAYER_KEYS[1] = PLAYER_KEYS[0] -> duplicate assert fires.
    #[test]
    fn player_keys_are_distinct() {
        for (i, ki) in PLAYER_KEYS.iter().enumerate() {
            for (j, kj) in PLAYER_KEYS.iter().enumerate().skip(i + 1) {
                assert_ne!(ki, kj, "player keys {i} and {j} collide");
            }
        }
    }

    // ── crypt_key behaviour ────────────────────────────────────────────────

    /// crypt_key result depends on every challenge byte. The challenge is
    /// permuted into `scratch` and folded through the LFSR seeding and the 6
    /// XOR rounds. Flipping any single challenge byte must change the output.
    ///
    /// Grounding: scratch[i]=challenge[perm[i]] for all 10 i, and scratch
    /// seeds both LFSRs (bytes 5..10 via tmp1) and the round terms (bytes
    /// 0..5).
    /// Mutation: in `scratch[i] = challenge[perm[i]]` replace with
    /// `challenge[i]` for a perm that drops a byte — or hardcode one scratch
    /// entry — and some challenge byte stops mattering; this fails.
    #[test]
    fn crypt_key_depends_on_every_challenge_byte() {
        let base: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let base_out = crypt_key(0, 5, &base);
        for i in 0..10 {
            let mut c = base;
            c[i] ^= 0x55;
            assert_ne!(
                crypt_key(0, 5, &c),
                base_out,
                "flipping challenge byte {i} did not change the bus-key derivation"
            );
        }
    }

    /// crypt_key(0, v, ..) must produce a DISTINCT result for each of the 32
    /// variants on a fixed challenge. bus_auth brute-forces the variant by
    /// matching crypt_key(0, v, host_challenge) == key1; if two variants
    /// collided, the wrong variant could be selected and the whole auth
    /// derail.
    ///
    /// Grounding: variant selects css_variant -> VARIANTS[css_variant] -> cse,
    /// which feeds every round; distinct variants give distinct cse-driven
    /// keys in practice.
    /// Mutation: make `cse` ignore the variant (e.g. `let cse = 0`) -> all 32
    /// outputs collapse to one value; the distinctness assert fires.
    #[test]
    fn crypt_key_type0_distinct_per_variant() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut outs = Vec::new();
        for v in 0..32u8 {
            let k = crypt_key(0, v, &challenge);
            assert!(
                !outs.contains(&k),
                "variant {v} collides with an earlier variant"
            );
            outs.push(k);
        }
    }

    /// crypt_key enforces its documented precondition `key_type < 3` via
    /// debug_assert (active in test builds). A key_type of 3 would index
    /// PERM_CHALLENGE (len 3) out of bounds; the assert turns that into an
    /// explicit precondition panic.
    ///
    /// Grounding: `debug_assert!(key_type < 3, ...)`; PERM_CHALLENGE has 3
    /// rows (indices 0,1,2).
    /// Mutation: delete the debug_assert AND the match-arm guard — but the
    /// match `_ =>` arm would then index PERM_CHALLENGE[3] OOB and panic
    /// differently; with the assert in place this test pins the contract.
    #[test]
    #[should_panic]
    fn crypt_key_rejects_out_of_range_key_type() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let _ = crypt_key(3, 0, &challenge);
    }

    /// crypt_key enforces `variant < 32` via debug_assert. A variant of 32
    /// would index VARIANTS / PERM_VARIANT (len 32) out of bounds.
    ///
    /// Grounding: `debug_assert!((variant as usize) < 32, ...)`.
    /// Mutation: removing the assert makes this index VARIANTS[32] (still a
    /// panic, but unguarded); the assert documents/enforces the contract.
    #[test]
    #[should_panic]
    fn crypt_key_rejects_out_of_range_variant() {
        let challenge: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let _ = crypt_key(0, 32, &challenge);
    }

    // ── SCSI CDB builders (MMC REPORT KEY / SEND KEY layout) ───────────────

    /// report_key_cdb encodes a 12-byte MMC REPORT KEY (opcode 0xA4) CDB:
    ///   byte 0  = operation code 0xA4
    ///   bytes 8-9 = allocation length, big-endian
    ///   byte 10 = (AGID << 6) | (key_format & 0x3F)
    /// All other bytes are zero.
    ///
    /// Grounding: MMC REPORT KEY CDB; the AGID is the top 2 bits of byte 10,
    /// key format the low 6 bits.
    /// Mutation: change `(alloc_len >> 8)` to `alloc_len` for byte 8 (lose the
    /// big-endian split) -> byte 8/9 assert fails. Change `agid << 6` to
    /// `agid << 5` -> the AGID-position assert fails.
    #[test]
    fn report_key_cdb_matches_mmc_layout() {
        let cdb = report_key_cdb(0b10, 0x04, 0x010C); // AGID=2, format=0x04, len=268
        assert_eq!(cdb[0], 0xA4, "REPORT KEY opcode");
        assert_eq!(cdb[8], 0x01, "alloc_len high byte (big-endian)");
        assert_eq!(cdb[9], 0x0C, "alloc_len low byte");
        assert_eq!(
            cdb[10],
            (0b10 << 6) | 0x04,
            "AGID in bits 6-7, format in bits 0-5"
        );
        // Every other byte must be zero.
        for (i, &b) in cdb.iter().enumerate() {
            if ![0, 8, 9, 10].contains(&i) {
                assert_eq!(b, 0, "CDB byte {i} must be zero");
            }
        }
        assert_eq!(cdb.len(), 12, "REPORT KEY is a 12-byte CDB");
    }

    /// The key format field is masked to 6 bits: a format with high bits set
    /// must not corrupt the AGID. report_key_cdb(0, 0xFF, _) -> byte 10 low 6
    /// bits = 0x3F, AGID = 0.
    ///
    /// Grounding: `(agid << 6) | (format & 0x3F)`.
    /// Mutation: drop the `& 0x3F` mask -> 0xFF would overwrite the AGID bits;
    /// byte 10 would be 0xFF not 0x3F, this fails.
    #[test]
    fn report_key_cdb_masks_format_to_6_bits() {
        let cdb = report_key_cdb(0, 0xFF, 8);
        assert_eq!(cdb[10], 0x3F, "format masked to 6 bits, AGID stays 0");
    }

    /// send_key_cdb encodes a 12-byte MMC SEND KEY (opcode 0xA3) CDB with the
    /// parameter-list length at bytes 8-9 (big-endian) and AGID/format at byte
    /// 10.
    ///
    /// Grounding: MMC SEND KEY CDB layout.
    /// Mutation: change opcode to SCSI_REPORT_KEY -> opcode assert fails;
    /// swap bytes 8/9 -> length assert fails.
    #[test]
    fn send_key_cdb_matches_mmc_layout() {
        let cdb = send_key_cdb(0b11, 0x03, 0x000C); // AGID=3, format=3, param_len=12
        assert_eq!(cdb[0], 0xA3, "SEND KEY opcode");
        assert_eq!(cdb[8], 0x00, "param_len high byte");
        assert_eq!(cdb[9], 0x0C, "param_len low byte");
        assert_eq!(
            cdb[10],
            (0b11 << 6) | 0x03,
            "AGID bits 6-7, format bits 0-5"
        );
        assert_eq!(cdb.len(), 12);
    }

    /// Allocation length larger than 255 must split across bytes 8 (high) and
    /// 9 (low) — a 16-bit big-endian field. report_key_cdb with alloc_len
    /// 0x0804 (2052, the disc-key block size used in read_disc_key) -> byte 8
    /// = 0x08, byte 9 = 0x04.
    ///
    /// Grounding: read_disc_key uses `alloc_len = 2048 + 4 = 2052 = 0x0804`
    /// and writes `cdb[8] = (alloc_len >> 8); cdb[9] = alloc_len`.
    /// Mutation: write only byte 9 (`cdb[9] = alloc_len as u8`) without byte 8
    /// -> the drive sees a 4-byte transfer, truncating the disc-key block;
    /// this asserts the high byte is present.
    #[test]
    fn report_key_cdb_alloc_len_is_16bit_big_endian() {
        let cdb = report_key_cdb(0, 0x00, 0x0804);
        assert_eq!(cdb[8], 0x08, "high byte of 2052-byte transfer");
        assert_eq!(cdb[9], 0x04, "low byte of 2052-byte transfer");
    }
}

//! Built-in public AACS 1.0 keys.
//!
//! Compile-time tables of the device keys (DK) and processing keys (PK)
//! required for AACS 1.0 MKB processing. These values are well-known
//! public AACS inputs that cover the MKB version ranges shipped on
//! retail Blu-ray / UHD discs.
//!
//! With these built-ins, libfreemkv can resolve AACS 1.0 encryption for
//! any disc whose VUK can be derived from MKB + device-key / processing-key
//! paths — no external keydb.cfg file is required. Operators who want to
//! supply additional keys (for example, future AACS 2.x derivations) can
//! drop a `local_keys.cfg` into `$HOME/.config/freemkv/` in the same
//! format as `keydb.cfg`; see [`crate::aacs::KeyDb::load_or_builtins`].

use super::keydb::DeviceKey;

/// A built-in device key entry. Mirrors [`DeviceKey`] but stored as a
/// `const`-friendly POD type with an additional MKB range tag for
/// diagnostics. Convert via [`BuiltinDeviceKey::to_device_key`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BuiltinDeviceKey {
    pub key: [u8; 16],
    pub device_node: u16,
    pub key_uv: u32,
    pub u_mask_shift: u8,
    /// MKB version range tag (for logging / diagnostics only).
    #[allow(dead_code)]
    pub mkb_range: &'static str,
}

impl BuiltinDeviceKey {
    pub(crate) fn to_device_key(self) -> DeviceKey {
        DeviceKey {
            key: self.key,
            node: self.device_node,
            uv: self.key_uv,
            u_mask_shift: self.u_mask_shift,
        }
    }
}

/// Public AACS 1.0 device keys covering MKB versions v01 through v82+.
///
/// Each entry contributes a subset-difference path through the MKB tree,
/// so the four together cover the MKB ranges shipped on retail Blu-ray
/// and UHD discs to date.
pub(crate) const BUILTIN_DEVICE_KEYS: &[BuiltinDeviceKey] = &[
    BuiltinDeviceKey {
        key: [
            0x5F, 0xB8, 0x6E, 0xF1, 0x27, 0xC1, 0x9C, 0x17, 0x1E, 0x79, 0x9F, 0x61, 0xC2, 0x7B,
            0xDC, 0x2A,
        ],
        device_node: 0x0800,
        key_uv: 0x0000_0400,
        u_mask_shift: 0x17,
        mkb_range: "v01-v48",
    },
    BuiltinDeviceKey {
        key: [
            0x38, 0x84, 0x16, 0x73, 0xE2, 0xB4, 0xE0, 0x51, 0x91, 0x65, 0x98, 0x99, 0x60, 0x6C,
            0xFF, 0xB8,
        ],
        device_node: 0x0C00,
        key_uv: 0x0000_0A00,
        u_mask_shift: 0x0B,
        mkb_range: "v49-v71",
    },
    BuiltinDeviceKey {
        key: [
            0x86, 0x1B, 0x37, 0x19, 0xB0, 0x2F, 0x24, 0xBE, 0x6F, 0x1A, 0x30, 0xE2, 0xE3, 0xAB,
            0xEE, 0x94,
        ],
        device_node: 0x0C40,
        key_uv: 0x0000_0D00,
        u_mask_shift: 0x0A,
        mkb_range: "v72+",
    },
    BuiltinDeviceKey {
        key: [
            0x7C, 0x06, 0xDE, 0xAE, 0x7F, 0x49, 0xB5, 0x51, 0xDA, 0xF5, 0x38, 0xC8, 0xCF, 0x18,
            0x11, 0xC9,
        ],
        device_node: 0x0E20,
        key_uv: 0x0000_0E23,
        u_mask_shift: 0x02,
        mkb_range: "v82+",
    },
];

/// Public AACS 1.0 processing keys for specific MKB versions.
///
/// Each value is a precomputed media-key-precursor that resolves MKB
/// processing for the version range called out beside it. Provided as a
/// fast path so an exhaustive device-key MKB walk is not required when a
/// matching PK is available.
pub(crate) const BUILTIN_PROCESSING_KEYS: &[[u8; 16]] = &[
    // v63
    [
        0x76, 0xDD, 0xD7, 0x09, 0x32, 0x16, 0xD2, 0x8C, 0x15, 0x04, 0x9A, 0x6B, 0x9C, 0x5C, 0x18,
        0xB9,
    ],
    // v64-v65
    [
        0x3B, 0x32, 0x3C, 0x7A, 0x9A, 0xFC, 0x09, 0x21, 0x83, 0x1D, 0x24, 0x72, 0x39, 0x82, 0x3D,
        0xE6,
    ],
    // v66-v68
    [
        0x7A, 0x4F, 0x40, 0xD8, 0x69, 0x6B, 0x7B, 0x15, 0x9B, 0xE8, 0x17, 0x6C, 0xC9, 0xED, 0xB8,
        0x5C,
    ],
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_device_keys_count() {
        assert_eq!(BUILTIN_DEVICE_KEYS.len(), 4);
    }

    #[test]
    fn builtin_processing_keys_count() {
        assert_eq!(BUILTIN_PROCESSING_KEYS.len(), 3);
    }

    #[test]
    fn first_builtin_device_key_value() {
        let expected: [u8; 16] = [
            0x5F, 0xB8, 0x6E, 0xF1, 0x27, 0xC1, 0x9C, 0x17, 0x1E, 0x79, 0x9F, 0x61, 0xC2, 0x7B,
            0xDC, 0x2A,
        ];
        assert_eq!(BUILTIN_DEVICE_KEYS[0].key, expected);
        assert_eq!(BUILTIN_DEVICE_KEYS[0].device_node, 0x0800);
        assert_eq!(BUILTIN_DEVICE_KEYS[0].key_uv, 0x0000_0400);
        assert_eq!(BUILTIN_DEVICE_KEYS[0].u_mask_shift, 0x17);
    }
}

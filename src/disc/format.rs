//! Static facts about a [`DiscFormat`], one match per fact.

use super::{DiscFormat, DiscRegion};

impl DiscFormat {
    /// Stable lowercase id: `"uhd"`, `"fmts"`, `"bluray"`, `"hddvd"`, `"dvd"` or
    /// `"unknown"`. Persisted by callers (staging, history, webhooks), so the
    /// values never change.
    pub fn id(self) -> &'static str {
        match self {
            DiscFormat::Uhd => "uhd",
            DiscFormat::Fmts => "fmts",
            DiscFormat::BluRay => "bluray",
            DiscFormat::HdDvd => "hddvd",
            DiscFormat::Dvd => "dvd",
            DiscFormat::Unknown => "unknown",
        }
    }

    /// Recording layers (1-3) for a disc of `capacity` sectors, judged against
    /// this format's per-layer size.
    pub fn layers(self, capacity: u32) -> u8 {
        // Upper sector bounds of (1, 2) layers; above the second is 3 (BD-100, HD DVD TL).
        let (one, two) = match self {
            DiscFormat::Dvd => (2_400_000, u32::MAX),
            DiscFormat::HdDvd => (8_000_000, 16_000_000),
            DiscFormat::BluRay | DiscFormat::Uhd | DiscFormat::Fmts => (12_500_000, 40_000_000),
            DiscFormat::Unknown => (12_500_000, u32::MAX),
        };
        if capacity <= one {
            1
        } else if capacity <= two {
            2
        } else {
            3
        }
    }

    /// Sectors in one ECC block: 32 (64 KiB) for the Blu-ray family, HD DVD and unknown
    /// media, 16 (32 KiB) for DVD only.
    // HD DVD-ROM (DVD Forum Part 1) forms an ECC block from 32 data frames (64 KB), twice
    // DVD's 16; cf. US 7979776 / US 7161889. The engine's v1.8.3 table said 16 (a bug).
    pub fn ecc_block_sectors(self) -> u16 {
        match self {
            DiscFormat::Uhd
            | DiscFormat::Fmts
            | DiscFormat::BluRay
            | DiscFormat::HdDvd
            | DiscFormat::Unknown => 32,
            DiscFormat::Dvd => 16,
        }
    }

    /// Region when the disc records none: [`DiscRegion::Free`] for UHD (region-free
    /// by spec), else [`DiscRegion::Unknown`].
    pub fn default_region(self) -> DiscRegion {
        match self {
            DiscFormat::Uhd | DiscFormat::Fmts => DiscRegion::Free,
            DiscFormat::BluRay | DiscFormat::HdDvd | DiscFormat::Dvd | DiscFormat::Unknown => {
                DiscRegion::Unknown
            }
        }
    }
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;

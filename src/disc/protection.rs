//! Questions about a scanned disc's content protection, answered from its
//! protection fields in one place.

use super::{Disc, DiscFormat};
use crate::error::Error;

/// What protects a disc, for display: the front end renders the words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtectionLabel {
    /// Not encrypted.
    Clear,
    /// CSS: a cracked key, or a CSS disc whose crack failed.
    Css,
    /// AACS, with its generation as `(major, minor)`: 2.1 for FMTS, 2.0 for UHD,
    /// else the scanned state's version (1.0 without one).
    Aacs { major: u8, minor: u8 },
    /// Encrypted, but neither CSS nor a known AACS carrier.
    Unknown,
}

impl Disc {
    /// The disc's intrinsic identity: its AACS disc hash as scanned (hex, an `0x`
    /// prefix possible), or `None` without AACS state. Callers normalise it.
    pub fn intrinsic_id(&self) -> Option<&str> {
        self.aacs.as_ref().map(|a| a.disc_hash.as_str())
    }

    /// The AACS Volume ID the bus handshake read, or `None` when there is no AACS
    /// state or the handshake read none (all zeroes).
    pub fn aacs_volume_id(&self) -> Option<[u8; 16]> {
        self.aacs
            .as_ref()
            .map(|a| a.volume_id)
            .filter(|v| *v != [0u8; 16])
    }

    /// Whether this disc's keys come from a key lookup (KEYDB or key service):
    /// true for a disc with AACS state.
    pub fn needs_key_lookup(&self) -> bool {
        self.aacs.is_some()
    }

    /// Whether keys are recovered per title at read time (a DVD's CSS title keys)
    /// rather than once for the disc.
    pub fn keys_per_title(&self) -> bool {
        self.format == DiscFormat::Dvd && self.aacs.is_none()
    }

    /// The disc's protection for display. CSS wins whenever any CSS signal is
    /// present, so a failed-crack DVD is never labelled AACS.
    pub fn protection_label(&self) -> ProtectionLabel {
        if !self.encrypted {
            return ProtectionLabel::Clear;
        }
        if self.css.is_some() || self.css_error.is_some() {
            return ProtectionLabel::Css;
        }
        let aacs_carrier = matches!(
            self.format,
            DiscFormat::BluRay | DiscFormat::Uhd | DiscFormat::Fmts | DiscFormat::HdDvd
        );
        if self.aacs.is_none() && !aacs_carrier {
            return ProtectionLabel::Unknown;
        }
        let (major, minor) = match self.format {
            DiscFormat::Fmts => (2, 1),
            DiscFormat::Uhd => (2, 0),
            _ => (self.aacs.as_ref().map_or(1, |a| a.version), 0),
        };
        ProtectionLabel::Aacs { major, minor }
    }

    /// The protection failure the scan recorded: the CSS crack's, else the AACS
    /// resolution's; `None` when neither failed.
    pub fn protection_failure(&self) -> Option<&Error> {
        self.css_error.as_ref().or(self.aacs_error.as_ref())
    }
}

#[cfg(test)]
#[path = "protection_tests.rs"]
mod tests;

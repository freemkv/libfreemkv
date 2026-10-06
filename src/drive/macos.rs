//! macOS device resolution.

use crate::drive::DeviceResolution;
use crate::error::{Error, Result};

/// Resolve a device path on macOS. There is no `sr`→`sg` style
/// substitution here (that is a Linux concern), so any existing path is
/// returned unchanged as [`DeviceResolution::Direct`]; the
/// [`DeviceResolution`] return exists for cross-platform signature parity.
pub fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
    if !std::path::Path::new(path).exists() {
        return Err(Error::DeviceNotFound {
            path: path.to_string(),
        });
    }
    Ok((path.to_string(), DeviceResolution::Direct))
}

#[cfg(test)]
#[path = "macos_resolve_device_tests.rs"]
mod resolve_device_tests;

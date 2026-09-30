//! Platform-specific filesystem / IO helpers.
//!
//! Drive unlock no longer lives here — it moved out to the `freemkv-unlock`
//! crate (consumed via [`crate::unlock_bridge`]). This module now carries only
//! the Linux filesystem-type detection used by the writeback paths.

#[cfg(target_os = "linux")]
pub mod fs_type;

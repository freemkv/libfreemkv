//! Platform-specific filesystem / IO helpers.
//!
//! Drive unlock no longer lives here — it moved out to the `freemkv-unlock`
//! crate (consumed via [`crate::unlock_bridge`]). This module now carries only
//! the filesystem-type detection used by the writeback / sink paths.

pub mod fs_type;

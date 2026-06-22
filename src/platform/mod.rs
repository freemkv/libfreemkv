//! Platform-specific filesystem / IO helpers.
//!
//! Drive unlock no longer lives here — it moved out behind the pluggable
//! [`crate::unlock::Unlocker`] seam. This module now carries only the
//! filesystem-type detection used by the writeback / sink paths.

pub mod fs_type;

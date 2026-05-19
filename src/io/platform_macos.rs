//! Shared macOS `fcntl(F_PREALLOCATE)` definitions.
//!
//! The `libc` crate doesn't expose these symbols across all macOS SDK
//! versions, so we define them locally with values from
//! `/usr/include/sys/fcntl.h`. Two call sites (
//! [`crate::io::writeback_file`] and [`crate::io::sink::preallocate`])
//! need the same constants and `fstore_t` layout — keeping a single
//! source of truth here prevents the two copies from drifting.
//!
//! Module-level cfg gate lives in the parent (`io/mod.rs`); this file
//! is only compiled on macOS, so no inner `#![cfg]` is needed.

/// `fcntl(F_PREALLOCATE)` command number from `sys/fcntl.h`.
pub(crate) const F_PREALLOCATE: libc::c_int = 42;

/// Anchor preallocation at the current physical EOF.
pub(crate) const F_PEOFPOSMODE: libc::c_int = 3;

/// Prefer a contiguous allocation. Try this first; on `EINVAL` (no
/// contiguous run of that size), fall back to `F_ALLOCATEALL`.
pub(crate) const F_ALLOCATECONTIG: libc::c_uint = 0x0000_0002;

/// Allow non-contiguous allocation. Stronger guarantee than just
/// asking for `F_ALLOCATECONTIG` because the kernel will piece
/// together fragments rather than failing.
pub(crate) const F_ALLOCATEALL: libc::c_uint = 0x0000_0004;

/// `fstore_t` from `sys/fcntl.h`. `repr(C)` because we hand it to
/// `fcntl(F_PREALLOCATE)` which writes through the pointer.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Fstore {
    pub fst_flags: libc::c_uint,
    pub fst_posmode: libc::c_int,
    pub fst_offset: libc::off_t,
    pub fst_length: libc::off_t,
    pub fst_bytesalloc: libc::off_t,
}

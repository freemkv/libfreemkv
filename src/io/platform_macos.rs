//! Shared macOS `fcntl(F_PREALLOCATE)` definitions.
//!
//! The `libc` crate doesn't expose these symbols across all macOS SDK
//! versions, so we define them locally with values from
//! `/usr/include/sys/fcntl.h`. See docs/platform-macos.md for why a
//! single source of truth here matters and how the cfg gate works.
//!
//! [`crate::io::writeback_file`] and `crate::io::sink::preallocate`
//! both depend on these constants and the `fstore_t` layout.

/// `fcntl(F_PREALLOCATE)` command number from `sys/fcntl.h`.
pub(crate) const F_PREALLOCATE: libc::c_int = 42;

/// Anchor preallocation at the current physical EOF.
pub(crate) const F_PEOFPOSMODE: libc::c_int = 3;

/// Prefer a contiguous allocation. Call sites OR this together with
/// `F_ALLOCATEALL` on the first attempt; on failure they fall back to
/// `F_ALLOCATEALL` alone.
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

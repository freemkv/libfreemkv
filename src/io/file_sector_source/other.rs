//! Fallback for targets without a known sequential-readahead hint
//! (BSDs, illumos, etc.). No-op — reads still work, they just don't
//! get the OS-level prefetch widening.

use std::fs::File;

pub(super) fn hint_sequential(_file: &File, _len_bytes: u64) {}

pub(super) fn drop_window(_file: &File, _start: u64, _len: u64) {}

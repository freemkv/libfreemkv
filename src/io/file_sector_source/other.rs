//! Fallback for targets without a known sequential-readahead hint
//! (BSDs, illumos, etc.). No-op — reads still work, they just don't
//! get the OS-level prefetch widening.

use std::fs::File;

pub(crate) fn hint_sequential(_file: &File, _len_bytes: u64) {}

pub(crate) fn drop_window(_file: &File, _start: u64, _len: u64) {}

pub(crate) fn prefetch(_file: &File, _offset: u64, _len: u64) {}

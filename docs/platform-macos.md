# src/io/platform_macos.rs

Shared macOS `fcntl(F_PREALLOCATE)` definitions.

The `libc` crate doesn't expose these symbols across all macOS SDK versions,
so we define them locally with values from `/usr/include/sys/fcntl.h`. Two
call sites ([`crate::io::writeback_file`] and `crate::io::sink::preallocate`)
need the same constants and `fstore_t` layout — keeping a single source of
truth here prevents the two copies from drifting.

Module-level cfg gate lives in the parent (`io/mod.rs`); this file is only
compiled on macOS, so no inner `#![cfg]` is needed.

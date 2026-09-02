# fs_type module

The buffering architecture (Phase 2) selects different output sinks
for local vs network filesystems: NFS gets the adaptive
`WritebackFile` machinery on Linux; local disks get `LocalFileSink`
and rely on the kernel's default writeback policy. This module
provides the construction-site primitive that picks which one.

Per the per-OS file-split convention, the actual `statfs` call lives
in the matching platform file (`linux.rs`, `macos.rs`, `windows.rs`,
`other.rs`); this `mod.rs` exposes only the cross-platform enum and
the `detect` entry point.

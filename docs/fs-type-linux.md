# Linux `f_type` classification

`classify_f_type` in `src/platform/fs_type/linux.rs` is the single source of
truth for the magic-number comparisons used by both the path-based
(`detect_impl`) and fd-based (`detect_fd_impl`) entry points.

## Why the `i64` cast

`statfs::f_type` is signed `__fsword_t` on glibc and unsigned `c_ulong` on
musl; a portable comparison needs a common type, so the value is cast to
`i64`. On glibc x86_64 both are already `i64` — clippy flags the cast as
unnecessary on that target only, but the cast is required for musl, hence
the `#[allow(clippy::unnecessary_cast)]`.

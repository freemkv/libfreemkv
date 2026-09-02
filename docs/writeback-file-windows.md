# WritebackFile Windows platform impl

## `preallocate`

Debug-logged no-op. Windows has no `fallocate`-equivalent that keeps the
reported size, so extent reservation is not wired up.

## `durable_sync`

Delegates to the std `File::sync_all`, which on Windows maps to
`FlushFileBuffers`. Unlike the Linux/macOS impls this is NOT wrapped in
the bounded-syscall primitive (that would need an `unsafe impl Send` for
`RawHandle`, which cannot be validated without a Windows test env), so a
wedged UNC/SMB share can block the final flush. This deviation is
documented on [`super::WritebackFile::sync_all`] and the parent module's
Halt-safety section.

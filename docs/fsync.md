# `io::fsync` — platform-aware crash-durability primitives

Two flush operations need OS-specific handling to make a write survive a
crash / power loss:

- **`dir`** — fsync a directory so a prior `rename(2)` into it is durable.
  After a crash a renamed file's dirent can otherwise be lost even though
  the rename returned, because it is still page-cache-only. This is a POSIX
  concept: on Windows std cannot even open a directory as a `File` (it does
  not set `FILE_FLAG_BACKUP_SEMANTICS`), and NTFS/ReFS commit the rename's
  dirent without an explicit directory flush — so it is a no-op there
  rather than a failed open that logs on every marker write.

- **`file_durable`** — fsync a file's contents + metadata. Opens the file
  **read+write**: on Windows `File::sync_all` maps to `FlushFileBuffers`,
  which requires a handle with write access and returns
  `ERROR_ACCESS_DENIED` (os error 5) on a read-only handle. (A read-only
  `File::open` + `sync_all` is legal on POSIX, which is why that bug only
  bit Windows.) The open mode is platform-uniform, so this lives here with
  no dispatch.

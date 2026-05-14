//! Output-sink trait split for the buffering architecture.
//!
//! Two traits, one for each capability axis of an output destination:
//!
//! - [`SequentialSink`] — anything you can `Write` to in order. Sockets,
//!   pipes, append-only stores, plain files. Containers that don't need
//!   seek (M2TS, fMP4, HEVC elementary) target this.
//! - [`RandomAccessSink`] — everything `SequentialSink` plus a working
//!   `Seek`. Local files, NFS files, anything with random-write
//!   semantics. Containers that need backpatch (MKV cluster sizes, Cues
//!   index, MP4 moov-at-end) target this.
//!
//! `RandomAccessSink: SequentialSink` — every random-access sink is
//! also a valid sequential sink. The muxer is generic over which it
//! requires (`MkvMux<S: RandomAccessSink>`, `M2tsMux<S: SequentialSink>`)
//! so an attempt to mux MKV to a network socket is a compile error.
//!
//! Buffering policy belongs to the concrete sink, not to a wrapper at
//! the call site. `LocalFileSink` wraps a `BufWriter<File>` with a
//! 4 MiB buffer for the common local-disk case; `WritebackFile`
//! (separate module) wraps a `File` with the adaptive-chunk
//! `sync_file_range` machinery for the Linux+NFS case.
//!
//! See `freemkv-private/memory/project_buffering_architecture.md` for
//! the full design and the source/sink matrix.

use std::io::{Seek, Write};

mod local_file;
mod preallocate;

pub use local_file::LocalFileSink;

/// Sequential-only write destination. Sockets, pipes, append-only
/// stores. No seek. Implementations own their write buffering — the
/// trait does not impose or hide any buffering of its own.
///
/// `finish` drains any internal buffering and signals end-of-stream to
/// the underlying transport (close-write on a socket, flush on a
/// buffered writer, etc.). The default impl is a no-op; concrete
/// implementations that need explicit shutdown can override it but the
/// blanket impl below keeps it optional for adapter types like
/// `&mut File`.
pub trait SequentialSink: Write + Send {
    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Random-access write destination. Local files, NFS files, anything
/// with a working `Seek`. Inherits the `SequentialSink` contract — a
/// random-access sink is always usable as a sequential sink.
pub trait RandomAccessSink: SequentialSink + Seek {}

// Blanket impls so any `Write + Send` type acts as a `SequentialSink`
// (with default `finish`), and any sink that also impls `Seek` is
// automatically a `RandomAccessSink`. Keeps call-site ergonomics simple
// — `&mut File`, `LocalFileSink`, `WritebackFile`, `BufWriter<File>`,
// and `Cursor<Vec<u8>>` all satisfy the right trait without per-type
// boilerplate.
impl<T: Write + Send + ?Sized> SequentialSink for T {}
impl<T: SequentialSink + Seek + ?Sized> RandomAccessSink for T {}

/// Pick the right `RandomAccessSink` impl for `dest` based on its
/// filesystem type.
///
/// - Linux + NFS path → `WritebackFile` with its adaptive-chunk
///   sync_file_range machinery and (when supported) `fallocate` size
///   hint.
/// - everything else → [`LocalFileSink`] over `BufWriter<File>`. On
///   non-Linux there is no `WritebackFile` machinery to opt into, and
///   on local Linux the kernel's default writeback policy is already
///   fine.
///
/// `size_hint`, when present, is forwarded to the per-OS preallocate
/// path (`fallocate(KEEP_SIZE)` on Linux, `F_PREALLOCATE` on macOS when
/// implemented, no-op elsewhere).
///
/// Returns a boxed trait object so the call site (mux construction)
/// stays agnostic of which concrete sink got picked.
#[allow(dead_code)] // wiring to mux::resolve is a follow-up commit
pub fn open_for_mkv(
    dest: &std::path::Path,
    size_hint: Option<u64>,
) -> std::io::Result<Box<dyn RandomAccessSink>> {
    #[cfg(not(target_os = "linux"))]
    use crate::platform::fs_type::detect;
    #[cfg(target_os = "linux")]
    use crate::platform::fs_type::{FsType, detect};

    #[cfg(target_os = "linux")]
    {
        if detect(dest) == FsType::Nfs {
            let wf = match size_hint {
                Some(n) => crate::io::WritebackFile::create_with_size_hint(dest, n)?,
                None => crate::io::WritebackFile::create(dest)?,
            };
            return Ok(Box::new(wf));
        }
    }
    // Silence the unused-binding warning on non-Linux where the only
    // branch above is cfg-gated out.
    #[cfg(not(target_os = "linux"))]
    {
        let _ = detect(dest);
    }

    let sink = match size_hint {
        Some(n) => LocalFileSink::with_size_hint(dest, n)?,
        None => LocalFileSink::create(dest)?,
    };
    Ok(Box::new(sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    // Type-level assertion: the blanket impls cover the shapes we care
    // about. These functions never run; they just have to type-check.
    fn _assert_file_is_sequential(_: &mut dyn SequentialSink) {}
    fn _assert_file_is_random_access(_: &mut dyn RandomAccessSink) {}

    #[test]
    fn blanket_impls_cover_file_and_localfilesink() {
        // `File` directly via blanket impls.
        let dir = tempfile::tempdir().unwrap();
        let mut f = File::create(dir.path().join("a.bin")).unwrap();
        _assert_file_is_sequential(&mut f);
        _assert_file_is_random_access(&mut f);

        // `LocalFileSink` ditto.
        let mut s = LocalFileSink::create(&dir.path().join("b.bin")).unwrap();
        _assert_file_is_sequential(&mut s);
        _assert_file_is_random_access(&mut s);

        // `WritebackFile` — confirms the Phase 1 type still satisfies
        // the trait via the blanket impl without needing an explicit
        // `impl RandomAccessSink for WritebackFile {}`.
        let mut wf = crate::io::WritebackFile::create(&dir.path().join("c.bin")).unwrap();
        _assert_file_is_sequential(&mut wf);
        _assert_file_is_random_access(&mut wf);
    }

    #[test]
    fn open_for_mkv_returns_a_random_access_sink() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.bin");
        let mut sink = open_for_mkv(&p, Some(64 * 1024)).unwrap();
        use std::io::{Seek, SeekFrom, Write};
        sink.write_all(b"hello").unwrap();
        sink.seek(SeekFrom::Start(0)).unwrap();
        sink.finish().unwrap();
        drop(sink);
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(&bytes[..5], b"hello");
    }
}

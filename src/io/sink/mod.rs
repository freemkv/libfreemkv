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

use std::io::{Seek, Write};

mod local_file;
mod preallocate;
mod socket;

pub use local_file::LocalFileSink;
pub use socket::{SocketSink, UdpSocketSink};

/// Sequential-only write destination. Sockets, pipes, append-only
/// stores. No seek. Implementations own their write buffering — the
/// trait does not impose or hide any buffering of its own.
///
/// `finish` drains any internal buffering and signals end-of-stream to
/// the underlying transport (close-write on a socket, flush + fsync on
/// a buffered file, etc.). The default impl flushes via [`Write::flush`]
/// — correct for an unbuffered destination — but every concrete sink in
/// this module overrides it to drain its own buffer and run its
/// transport-specific finalisation (socket `shutdown(Write)`, file
/// `fsync`). There is deliberately NO blanket `impl SequentialSink for
/// T`: a blanket impl would force the no-op-style default on every
/// concrete sink (a blanket impl cannot be overridden per-type without a
/// coherence conflict), so a `Box<dyn SequentialSink>` / `&mut dyn
/// SequentialSink` `finish()` call would silently skip the flush and
/// transport shutdown. With explicit per-type impls the vtable dispatches
/// `finish` to the real implementation, so flush + durable-finish
/// actually happen through a trait object.
pub trait SequentialSink: Write + Send {
    fn finish(&mut self) -> std::io::Result<()> {
        self.flush()
    }
}

/// Random-access write destination. Local files, NFS files, anything
/// with a working `Seek`. Inherits the `SequentialSink` contract — a
/// random-access sink is always usable as a sequential sink.
pub trait RandomAccessSink: SequentialSink + Seek {}

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
    #[cfg(target_os = "linux")]
    {
        use crate::platform::fs_type::{FsType, detect};
        if detect(dest) == FsType::Nfs {
            let wf = match size_hint {
                Some(n) => crate::io::WritebackFile::create_with_size_hint(dest, n)?,
                None => crate::io::WritebackFile::create(dest)?,
            };
            return Ok(Box::new(wf));
        }
    }
    // Only Linux differentiates the sink by filesystem type (NFS gets
    // the WritebackFile machinery); every other OS always uses
    // `LocalFileSink`. Reference `detect` as a value (no call, no
    // `statfs` syscall) so it isn't flagged dead on non-Linux while
    // still avoiding the wasted probe whose result we'd discard.
    #[cfg(not(target_os = "linux"))]
    let _ = crate::platform::fs_type::detect;

    let sink = match size_hint {
        Some(n) => LocalFileSink::with_size_hint(dest, n)?,
        None => LocalFileSink::create(dest)?,
    };
    Ok(Box::new(sink))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Type-level assertion: the concrete sinks satisfy the trait
    // objects. These functions never run; they just have to type-check.
    fn _assert_is_sequential(_: &mut dyn SequentialSink) {}
    fn _assert_is_random_access(_: &mut dyn RandomAccessSink) {}

    #[test]
    fn concrete_sinks_satisfy_traits() {
        let dir = tempfile::tempdir().unwrap();

        // `LocalFileSink` is a random-access (and thus sequential) sink.
        let mut s = LocalFileSink::create(&dir.path().join("b.bin")).unwrap();
        _assert_is_sequential(&mut s);
        _assert_is_random_access(&mut s);

        // `WritebackFile` ditto, via its explicit per-type impls.
        let mut wf = crate::io::WritebackFile::create(&dir.path().join("c.bin")).unwrap();
        _assert_is_sequential(&mut wf);
        _assert_is_random_access(&mut wf);
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

    /// finish() through a `dyn SequentialSink` trait object must
    /// dispatch to the concrete sink's override (flush + fsync), not a
    /// no-op default. This is the regression test for the silent-no-op
    /// finish() bug.
    #[test]
    fn finish_through_trait_object_flushes_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("trait-finish.bin");
        let sink = LocalFileSink::create(&p).unwrap();
        // Box as the trait object the production path uses.
        let mut boxed: Box<dyn SequentialSink> = Box::new(sink);
        boxed.write_all(b"buffered-tail").unwrap();
        // finish() through the vtable must drain the 4 MiB BufWriter and
        // fsync; the bytes must be visible to a separate reader BEFORE
        // we drop the sink (drop-flush must not be what saves us).
        boxed.finish().unwrap();
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(&bytes[..], b"buffered-tail");
    }

    // ── Added hardening tests ───────────────────────────────────────

    use std::io::{self, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A minimal `SequentialSink` that does NOT override `finish`, so it
    /// exercises the trait's DEFAULT impl (lines 51-55), which must call
    /// `Write::flush`. We record whether flush ran. This pins the
    /// documented contract that the default `finish` is "correct for an
    /// unbuffered destination" by flushing. Mutation: changing the
    /// default `finish` body from `self.flush()` to `Ok(())` would set
    /// `flushed=false` and fail.
    struct FlushTracker {
        flushed: Arc<AtomicBool>,
        bytes: Arc<AtomicUsize>,
    }
    impl Write for FlushTracker {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.fetch_add(buf.len(), Ordering::SeqCst);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    // Uses the DEFAULT finish() — deliberately no override.
    impl SequentialSink for FlushTracker {}

    #[test]
    fn default_finish_flushes() {
        let flushed = Arc::new(AtomicBool::new(false));
        let bytes = Arc::new(AtomicUsize::new(0));
        let mut sink = FlushTracker {
            flushed: flushed.clone(),
            bytes: bytes.clone(),
        };
        sink.write_all(b"abc").unwrap();
        assert!(
            !flushed.load(Ordering::SeqCst),
            "flush should not run before finish"
        );
        sink.finish().unwrap();
        assert!(
            flushed.load(Ordering::SeqCst),
            "default SequentialSink::finish must call Write::flush"
        );
        assert_eq!(bytes.load(Ordering::SeqCst), 3);
    }

    /// `open_for_mkv` with `None` size hint must still produce a working
    /// random-access sink (the `match size_hint { None => ... }` arm,
    /// lines 103-106). Round-trip a seek-back patch through it to prove
    /// both Write and Seek dispatch. Mutation: if the None arm returned
    /// a sequential-only sink the seek would not compile / would fail.
    #[test]
    fn open_for_mkv_without_size_hint_is_random_access() {
        use std::io::{Seek, SeekFrom};
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nohint.bin");
        let mut sink = open_for_mkv(&p, None).unwrap();
        sink.write_all(b"AAAABBBB").unwrap();
        sink.seek(SeekFrom::Start(4)).unwrap();
        sink.write_all(b"CCCC").unwrap();
        sink.finish().unwrap();
        drop(sink);
        assert_eq!(std::fs::read(&p).unwrap(), b"AAAACCCC");
    }
}

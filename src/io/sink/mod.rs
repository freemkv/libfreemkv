//! Output-sink trait split for the buffering architecture.
//!
//! [`SequentialSink`] — anything you can `Write` to in order (no seek;
//! sockets, pipes, M2TS/fMP4/HEVC-ES) — and [`RandomAccessSink`] —
//! `SequentialSink` plus a working `Seek` (local/NFS files; needed for
//! backpatch: MKV cluster sizes, Cues, MP4 moov-at-end). The muxer is
//! generic over whichever it requires, so muxing MKV to a socket is a
//! compile error. Buffering policy belongs to the concrete sink; see
//! docs/sink.md for the buffering-per-sink breakdown.

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
/// the transport (close-write on a socket, flush + fsync on a buffered
/// file, etc.). The default impl flushes via [`Write::flush`]; every
/// concrete sink here overrides it with its own finalisation. No
/// blanket `impl SequentialSink for T` — see docs/sink.md for why.
pub trait SequentialSink: Write + Send {
    fn finish(&mut self) -> std::io::Result<()> {
        self.flush()
    }
}

/// Random-access write destination. Local files, NFS files, anything
/// with a working `Seek`. Inherits the `SequentialSink` contract — a
/// random-access sink is always usable as a sequential sink.
pub trait RandomAccessSink: SequentialSink + Seek {}

// Picks the right RandomAccessSink impl for `dest` by filesystem type
// (Linux+NFS -> WritebackFile, else LocalFileSink). Not yet wired into
// mux::resolve; see docs/sink.md for the full picker/size_hint rationale.
#[allow(dead_code)]
pub(crate) fn open_for_mkv(
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
    // Only Linux differentiates the sink by filesystem type; other OSes always use
    // `LocalFileSink`. Reference `detect` as a value (no call/syscall) so it isn't
    // flagged dead on non-Linux while avoiding a wasted probe.
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

    // Regression test for the silent-no-op finish() bug. See docs/sink.md.
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

    // Minimal SequentialSink that does NOT override finish(), so it
    // exercises the trait's default impl. See docs/sink.md.
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

    // Covers the `None` size_hint arm: must still be a random-access
    // sink. See docs/sink.md.
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

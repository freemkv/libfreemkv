use super::*;
use std::io::Read;

fn read_back(path: &Path) -> Vec<u8> {
    let mut f = File::open(path).unwrap();
    let mut v = Vec::new();
    f.read_to_end(&mut v).unwrap();
    v
}

#[test]
fn write_then_drop_persists_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("a.bin");
    {
        let mut w = WritebackFile::create(&p).unwrap();
        w.write_all(b"hello world").unwrap();
        // Drop drains the pipeline tail.
    }
    assert_eq!(read_back(&p), b"hello world");
}

#[test]
fn sync_all_drains_and_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("b.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    for _ in 0..32 {
        w.write_all(&[0x5au8; 1024]).unwrap();
    }
    // After sync_all, the bytes MUST be visible to a separate
    // reader. The pipeline has been finalised and durable-sync has
    // run.
    w.sync_all().unwrap();
    let bytes = read_back(&p);
    assert_eq!(bytes.len(), 32 * 1024);
    assert!(bytes.iter().all(|&b| b == 0x5a));
    drop(w);
}

#[test]
fn seek_then_patch_roundtrip() {
    // Write A; seek back; patch with B; read back; the patch lands
    // at the right offset.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("c.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    let big = vec![b'A'; 4096];
    w.write_all(&big).unwrap();
    // Seek back to offset 1000 and overwrite 8 bytes.
    w.seek(SeekFrom::Start(1000)).unwrap();
    w.write_all(b"PATCHED!").unwrap();
    w.sync_all().unwrap();
    drop(w);
    let bytes = read_back(&p);
    assert_eq!(bytes.len(), 4096);
    assert_eq!(&bytes[1000..1008], b"PATCHED!");
    // Bytes outside the patch are still 'A'.
    assert_eq!(bytes[999], b'A');
    assert_eq!(bytes[1008], b'A');
}

#[test]
fn flush_is_observed_in_order() {
    // `Write::flush` should not panic or reorder; verify the bytes
    // land in order through interleaved flushes.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("f.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"one").unwrap();
    w.flush().unwrap();
    w.write_all(b"two").unwrap();
    w.flush().unwrap();
    w.write_all(b"three").unwrap();
    w.sync_all().unwrap();
    drop(w);
    assert_eq!(read_back(&p), b"onetwothree");
}

// A writeback error the pipeline latched (Linux: a failed WAIT_AFTER already
// consumed it, so fsync returns 0) must fail sync_all and every later write.
#[test]
fn latched_writeback_error_fails_sync_all_and_later_writes() {
    const EIO: i32 = 5;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("wb-err.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"chunk").unwrap();
    w.pipeline.inject_error(EIO);
    let e = w
        .sync_all()
        .expect_err("sync_all must surface the latched error");
    assert_eq!(e.raw_os_error(), Some(EIO));
    assert!(
        w.write_all(b"more").is_err(),
        "write_all after a latched error"
    );
    assert!(w.write(b"more").is_err(), "write after a latched error");
    assert!(w.sync_all().is_err(), "the error is sticky");
}

// Sinks that only flush (m2ts: BufWriter<WritebackFile>, finished by flush)
// must still see a latched writeback error.
#[test]
fn latched_writeback_error_fails_flush_through_bufwriter() {
    const EIO: i32 = 5;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("wb-flush.bin");
    let mut w = std::io::BufWriter::new(WritebackFile::create(&p).unwrap());
    w.write_all(b"frames").unwrap();
    // Buffer drained: the final flush issues no write, only `flush`.
    w.flush().unwrap();
    w.get_mut().pipeline.inject_error(EIO);
    let e = w.flush().expect_err("flush must surface the latched error");
    assert_eq!(e.raw_os_error(), Some(EIO));
}

// ── Added hardening tests ───────────────────────────────────────

// `write` must return the inner File's reported count and advance `pos` by exactly that
// count (not `buf.len()`).
#[test]
fn write_returns_byte_count_and_advances_pos() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("wc.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    let n = w.write(b"twelve bytes").unwrap();
    assert_eq!(n, 12, "write must report bytes written");
    // pos is private; observe it via the public Seek impl's
    // stream_position (which resolves to seek(Current(0))).
    let pos = w.stream_position().unwrap();
    assert_eq!(pos, 12, "pos not advanced by write count");
    w.sync_all().unwrap();
    drop(w);
    assert_eq!(read_back(&p), b"twelve bytes");
}

// Redundant seek to the CURRENT position (sweep's `seek(Current(pos))` before every write)
// must not be treated as a boundary.
#[test]
fn seek_to_current_position_is_noop_for_data() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("noop-seek.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"AAAA").unwrap();
    // Seek to the current end (offset 4) — a no-move seek.
    let off = w.seek(SeekFrom::Start(4)).unwrap();
    assert_eq!(off, 4);
    // A no-move seek must not reset the pipeline's chunk tracking.
    assert_eq!(w.seek_count, 0, "redundant seek counted as a boundary");
    w.write_all(b"BBBB").unwrap();
    w.sync_all().unwrap();
    drop(w);
    assert_eq!(
        read_back(&p),
        b"AAAABBBB",
        "redundant seek corrupted contiguous write"
    );
}

// `open` (no-truncate) must preserve existing file contents, distinct from `create`'s
// truncating path.
#[test]
fn open_preserves_existing_contents() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("reopen.bin");
    std::fs::write(&p, b"ORIGINAL-CONTENT").unwrap();
    let mut w = WritebackFile::open(&p).unwrap();
    // open() does NOT truncate; pos starts at 0. Overwrite the
    // first 8 bytes only.
    w.write_all(b"PATCHED!").unwrap();
    w.sync_all().unwrap();
    drop(w);
    // First 8 bytes overwritten; the rest of ORIGINAL-CONTENT
    // ("-CONTENT") survives because there was no truncation.
    assert_eq!(read_back(&p), b"PATCHED!-CONTENT");
}

// `new` queries stream_position() rather than hardcoding pos=0, so a non-zero starting
// offset stays in sync.
#[test]
fn new_tracks_initial_position() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("pos-init.bin");
    std::fs::write(&p, b"0123456789").unwrap();
    let mut w = WritebackFile::open(&p).unwrap();
    let start = w.stream_position().unwrap();
    assert_eq!(start, 0, "freshly opened file should start at offset 0");
    w.write_all(b"XY").unwrap();
    let after = w.stream_position().unwrap();
    assert_eq!(after, 2, "pos must advance by written length");
}

// Seek past EOF then write must create a sparse hole reading back as
// zeros — standard POSIX semantics forwarded to the inner File.
#[test]
fn seek_past_eof_creates_zero_hole() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("hole.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"head").unwrap(); // bytes 0..4
    w.seek(SeekFrom::Start(20)).unwrap(); // jump past EOF
    w.write_all(b"tail").unwrap(); // bytes 20..24
    w.sync_all().unwrap();
    drop(w);
    let bytes = read_back(&p);
    assert_eq!(
        bytes.len(),
        24,
        "file should extend to the last written byte"
    );
    assert_eq!(&bytes[0..4], b"head");
    // The 4..20 gap must read back as zeros (sparse hole).
    assert!(bytes[4..20].iter().all(|&b| b == 0), "hole not zero-filled");
    assert_eq!(&bytes[20..24], b"tail");
}

// `SeekFrom::End` must resolve against the actual file length; after
// writing 10 bytes, `seek(End(-2))` lands at offset 8.
#[test]
fn seek_from_end_resolves_against_length() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("end-seek.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"0123456789").unwrap();
    let landed = w.seek(SeekFrom::End(-2)).unwrap();
    assert_eq!(landed, 8, "End(-2) of a 10-byte file is offset 8");
    w.write_all(b"XY").unwrap();
    w.sync_all().unwrap();
    drop(w);
    assert_eq!(read_back(&p), b"01234567XY");
}

// `create_with_size_hint`'s hint reserves extents only; it must NOT pre-grow the logical
// file length.
#[test]
fn create_with_size_hint_does_not_inflate_logical_length() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("hint-len.bin");
    let mut w = WritebackFile::create_with_size_hint(&p, 1024 * 1024).unwrap();
    w.write_all(b"hello").unwrap();
    w.sync_all().unwrap();
    drop(w);
    let bytes = read_back(&p);
    assert_eq!(bytes.len(), 5, "size hint must not inflate logical length");
    assert_eq!(&bytes, b"hello");
}

// The size-hint reservation past EOF must be released once writing ends.
#[cfg(target_os = "linux")]
#[test]
fn size_hint_reservation_is_released_after_sync() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("hint-trim.bin");
    let mut w = WritebackFile::create_with_size_hint(&p, 64 * 1024 * 1024).unwrap();
    w.write_all(b"hello").unwrap();
    w.sync_all().unwrap();
    let blocks = std::fs::metadata(&p).unwrap().blocks();
    assert!(blocks < 1024, "reservation kept: {blocks} 512-byte blocks");
}

// The mux path (resolve.rs `writeback_file`): BufWriter over the hinted file, closed by
// flush + drop only, never `sync_all`. The reservation must still be released.
#[cfg(target_os = "linux")]
#[test]
fn size_hint_reservation_is_released_on_mux_close() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("hint-mux.bin");
    let f = WritebackFile::create_with_size_hint(&p, 64 * 1024 * 1024).unwrap();
    let mut w = std::io::BufWriter::with_capacity(64 * 1024, f);
    w.write_all(&[7u8; 100 * 1024]).unwrap();
    w.flush().unwrap();
    drop(w);
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 100 * 1024);
    let blocks = std::fs::metadata(&p).unwrap().blocks();
    assert!(blocks < 4096, "reservation kept: {blocks} 512-byte blocks");
}

// The reservation is released only after a successful sync: a failed (or halted) sync
// must not truncate, as that setattr is a halt-blind full flush on NFS.
#[test]
fn failed_sync_keeps_size_hint_reservation() {
    struct FailFinish;
    impl FlushOps for FailFinish {
        fn chunk(&self, _: &File) -> io::Result<()> {
            Ok(())
        }
        fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
            None
        }
        fn finish(&self, _: &File) -> io::Result<()> {
            Err(io::Error::from_raw_os_error(5))
        }
        fn sample(&self) -> Option<u64> {
            None
        }
    }
    let file = tempfile::tempfile().unwrap();
    let mut w =
        WritebackFile::with_flush_ops(file, Arc::new(FailFinish), FlushTiming::default()).unwrap();
    w.hinted = true;
    w.write_all(b"partial").unwrap();
    assert!(w.sync_all().is_err(), "the final flush failed");
    assert!(w.hinted, "reservation released before a successful sync");
}

// Rebinding the flush counters carries the length so far, makes the new counters the
// file's own, and moves a running flusher's durable credit onto them.
#[test]
fn set_flush_progress_rebinds_every_holder() {
    struct OkOps;
    impl FlushOps for OkOps {
        fn chunk(&self, _: &File) -> io::Result<()> {
            Ok(())
        }
        fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
            None
        }
        fn finish(&self, _: &File) -> io::Result<()> {
            Ok(())
        }
        fn sample(&self) -> Option<u64> {
            None
        }
    }
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"12345").unwrap();
    let timing = FlushTiming {
        chunk_min: 100,
        chunk_max: 100,
        ..FlushTiming::default()
    };
    let mut w = WritebackFile::with_flush_ops(file, Arc::new(OkOps), timing).unwrap();
    w.write_all(&[0u8; 100]).unwrap();
    let fresh = FlushProgress::default();
    w.set_flush_progress(fresh.clone());
    assert_eq!(
        fresh.bytes_total(),
        105,
        "the length so far is carried over"
    );
    w.write_all(&[0u8; 300]).unwrap();
    assert_eq!(w.flush_progress().bytes_total(), 405);
    assert_eq!(fresh.bytes_total(), 405, "writes count on the new counters");
    let start = std::time::Instant::now();
    while fresh.bytes_durable() == 0 && start.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        fresh.bytes_durable() > 0,
        "the flusher credits the new counters"
    );
}

// `sync_all` is idempotent: calling it twice, then Drop (also
// finalizes), must not corrupt data or panic.
#[test]
fn double_sync_all_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("double-sync.bin");
    let mut w = WritebackFile::create(&p).unwrap();
    w.write_all(b"idempotent").unwrap();
    w.sync_all().unwrap();
    w.sync_all().unwrap(); // second call must be safe
    drop(w); // Drop also finalizes
    assert_eq!(read_back(&p), b"idempotent");
}

// Pins the WRITEBACK_CHUNK_* constants and the MiB->byte multiply that
// `writeback_chunk_bytes` relies on.
#[test]
fn writeback_chunk_constants_and_conversion() {
    // Default is exactly 32 MiB.
    assert_eq!(WRITEBACK_CHUNK_BYTES_DEFAULT, 32 * 1024 * 1024);
    // Max MiB bound is 64 GiB expressed in MiB, and the byte value
    // it maps to must not overflow u64.
    assert_eq!(WRITEBACK_CHUNK_MIB_MAX, 64 * 1024);
    let max_bytes = (WRITEBACK_CHUNK_MIB_MAX as u128) * 1024 * 1024;
    assert!(
        max_bytes <= u64::MAX as u128,
        "max chunk MiB * 1MiB must fit in u64"
    );
}

// All `writeback_chunk_bytes` env-var branches in ONE test (avoids a data race between
// parallel tests mutating the same env var).
#[test]
fn writeback_chunk_env_override_branches() {
    // SAFETY: this is the only test touching this env var, and it
    // sets+reads+clears synchronously within its own body.
    let set = |v: &str| unsafe { std::env::set_var("FREEMKV_WRITEBACK_CHUNK_MIB", v) };
    let clear = || unsafe { std::env::remove_var("FREEMKV_WRITEBACK_CHUNK_MIB") };

    set("8");
    assert_eq!(
        writeback_chunk_bytes(),
        8 * 1024 * 1024,
        "in-range mis-converted"
    );

    set("0");
    assert_eq!(
        writeback_chunk_bytes(),
        WRITEBACK_CHUNK_BYTES_DEFAULT,
        "zero must fall back (n > 0 filter)"
    );

    set("not-a-number");
    assert_eq!(
        writeback_chunk_bytes(),
        WRITEBACK_CHUNK_BYTES_DEFAULT,
        "unparseable must fall back"
    );

    // One past the max: WRITEBACK_CHUNK_MIB_MAX + 1.
    set(&(WRITEBACK_CHUNK_MIB_MAX + 1).to_string());
    assert_eq!(
        writeback_chunk_bytes(),
        WRITEBACK_CHUNK_BYTES_DEFAULT,
        "over-max must fall back (n <= MAX filter)"
    );

    // Exactly at the max boundary is accepted (inclusive bound).
    set(&WRITEBACK_CHUNK_MIB_MAX.to_string());
    assert_eq!(
        writeback_chunk_bytes(),
        WRITEBACK_CHUNK_MIB_MAX * 1024 * 1024,
        "max boundary must be accepted (inclusive)"
    );

    clear();
    // With the var cleared, the default is returned.
    assert_eq!(writeback_chunk_bytes(), WRITEBACK_CHUNK_BYTES_DEFAULT);
}

use super::*;
use crate::error::Result as FmResult;

/// A source that yields a deterministic byte per sector, so the written
/// image can be checked positionally rather than just by length.
struct PatternSource {
    sectors: u32,
    /// Sectors after which `read_sectors` reports a short read.
    short_after: Option<u32>,
}

impl SectorSource for PatternSource {
    fn capacity_sectors(&self) -> u32 {
        self.sectors
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> FmResult<usize> {
        let want = count as usize * SECTOR_BYTES;
        if self.short_after.is_some_and(|after| lba >= after) {
            return Ok(want - 1);
        }
        for s in 0..count as usize {
            let byte = ((lba as usize + s) % 251) as u8;
            buf[s * SECTOR_BYTES..(s + 1) * SECTOR_BYTES].fill(byte);
        }
        Ok(want)
    }
}

// `lba`s from `file` hold the same encrypted AACS unit, on the file's own grid.
struct AacsFileImage {
    file: u32,
    unit: Vec<u8>,
}

impl SectorSource for AacsFileImage {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> FmResult<usize> {
        for i in 0..count as usize {
            let sector = &mut buf[i * SECTOR_BYTES..(i + 1) * SECTOR_BYTES];
            let s = lba + i as u32;
            match s.checked_sub(self.file).filter(|&o| o < 30) {
                Some(o) => {
                    let k = (o % 3) as usize * SECTOR_BYTES;
                    sector.copy_from_slice(&self.unit[k..k + SECTOR_BYTES]);
                }
                None => sector.fill(0),
            }
        }
        Ok(count as usize * SECTOR_BYTES)
    }
}

fn clear_aacs_unit() -> Vec<u8> {
    let mut unit = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
    for off in (4..unit.len()).step_by(192) {
        unit[off] = 0x47;
    }
    unit[0] |= 0xC0;
    unit
}

// Batches sit on LBA 0's grid, not a file's: an unwrapped AACS decrypting source fails
// loud at the first off-grid batch that touches content, never ships ciphertext.
#[test]
fn write_image_over_an_unwrapped_aacs_source_fails_loud_at_a_misaligned_batch() {
    use crate::decrypt::{DecryptKeys, Phase};
    use std::sync::Arc;
    let key = [0x5A; 16];
    let mut unit = clear_aacs_unit();
    assert!(crate::aacs::content::encrypt_unit(&mut unit, &key));
    let src = AacsFileImage { file: 3001, unit };
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, key)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = crate::keys::test_key_map(vec![(3001, 3031, 0, Phase::All)]);
    let dec = crate::sector::DecryptingSectorSource::new(src, keys);
    let mut dec = crate::keys::test_keyed_source(dec, map)
        .with_content_ranges(Arc::from(vec![(3001u32, 30u32)]));
    dec.set_unit_base(1); // the file's grid (3001 % 3 == 1): batch 1536 is off it
    let dest = tmp("aacs-batch");
    let r = write_image(&mut dec, &dest, 3100, &Ctx::default());
    let _ = std::fs::remove_file(&dest);
    let e = r.expect_err("batch 1536 is off the file's unit grid and touches content");
    assert_eq!(e.code(), Error::DecryptFailed.code());
}

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("fmkv-image-writer-{name}-{}", std::process::id()));
    p
}

// A failed copy must not leave a truncated image at the final name.
#[test]
fn failed_copy_removes_the_incomplete_image() {
    let td = tempfile::tempdir().unwrap();
    let dest = td.path().join("bad.iso");
    let mut src = PatternSource {
        sectors: 4096,
        short_after: Some(2048),
    };
    let r = write_image(&mut src, &dest, 4096, &Ctx::default());
    assert!(matches!(r, Err(Error::ShortImageRead { .. })), "{r:?}");
    assert!(!dest.exists(), "truncated image left at the final name");
}

// A failing parent-directory fsync must fail write_image, not report Ok.
#[test]
fn failed_parent_directory_fsync_is_an_error() {
    let td = tempfile::tempdir().unwrap();
    let mut src = PatternSource {
        sectors: 4,
        short_after: None,
    };
    let res = write_image_with(
        &mut src,
        &td.path().join("x.iso"),
        4,
        &Ctx::default(),
        |_| Err(std::io::Error::from(std::io::ErrorKind::Other)),
    );
    assert!(matches!(res, Err(Error::IoError { .. })), "{res:?}");
}

/// The written image is byte-for-byte what the source presented, at the
/// right offsets — not merely the right length.
#[test]
fn writes_every_sector_in_order() {
    let dest = tmp("order");
    let mut src = PatternSource {
        sectors: 5000,
        short_after: None,
    };
    let n = write_image(&mut src, &dest, 5000, &Ctx::default()).expect("write");
    assert_eq!(n, 5000 * SECTOR_BYTES as u64);

    let data = std::fs::read(&dest).expect("read back");
    assert_eq!(data.len(), 5000 * SECTOR_BYTES);
    // Spot-check across batch boundaries: the last sector of batch 0, the first of
    // batch 1, and the final sector.
    let b = BATCH_SECTORS as usize;
    for lba in [0usize, b - 1, b, 2 * b - 1, 2 * b, 4999] {
        let want = (lba % 251) as u8;
        assert_eq!(
            data[lba * SECTOR_BYTES],
            want,
            "sector {lba} head byte wrong — batching lost or duplicated a sector"
        );
        assert_eq!(
            data[(lba + 1) * SECTOR_BYTES - 1],
            want,
            "sector {lba} tail"
        );
    }
    let _ = std::fs::remove_file(&dest);
}

/// A tail shorter than a full batch must still be written whole — the
/// classic off-by-one when `total_sectors` is not a batch multiple.
#[test]
fn writes_a_partial_final_batch() {
    let dest = tmp("tail");
    let mut src = PatternSource {
        sectors: 2049,
        short_after: None,
    };
    let n = write_image(&mut src, &dest, 2049, &Ctx::default()).expect("write");
    assert_eq!(n, 2049 * SECTOR_BYTES as u64);
    assert_eq!(
        std::fs::metadata(&dest).expect("stat").len(),
        2049 * SECTOR_BYTES as u64
    );
    let _ = std::fs::remove_file(&dest);
}

/// A short read is an error. Zero-filling would yield an image that looks
/// complete and is not — the single worst outcome for an archival copy.
#[test]
fn short_read_is_an_error_not_a_zero_fill() {
    let dest = tmp("short");
    let mut src = PatternSource {
        sectors: 4096,
        short_after: Some(2048),
    };
    let err = write_image(&mut src, &dest, 4096, &Ctx::default()).expect_err("must fail");
    assert!(
        matches!(err, Error::ShortImageRead { lba, .. } if lba == 2 * BATCH_SECTORS),
        "got {err:?}"
    );
    let _ = std::fs::remove_file(&dest);
}

/// Cancellation stops the run and reports it, rather than finishing quietly
/// or reporting success on a partial image.
#[test]
fn cancellation_halts_and_reports() {
    let dest = tmp("halt");
    let mut src = PatternSource {
        sectors: 100_000,
        short_after: None,
    };
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let err =
        write_image(&mut src, &dest, 100_000, &Ctx::new(halt.clone())).expect_err("must halt");
    assert!(matches!(err, Error::Halted), "got {err:?}");
    assert!(dest.exists(), "a halt must keep the partial image");
    let _ = std::fs::remove_file(&dest);
}

/// Progress is cumulative and monotonic, and its final value equals the
/// returned byte count — a front-end that trusts the callback must not end
/// up disagreeing with the return value.
#[test]
fn progress_is_cumulative_and_ends_at_the_total() {
    let dest = tmp("progress");
    let mut src = PatternSource {
        sectors: 5000,
        short_after: None,
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let rec = seen.clone();
    let ctx = Ctx::default().with_events(std::sync::Arc::new(move |e: &Event<'_>| {
        if let Event::BytesWritten { bytes, total } = *e {
            assert_eq!(total, 5000 * SECTOR_BYTES as u64);
            rec.lock().unwrap().push(bytes);
        }
    }));
    let n = write_image(&mut src, &dest, 5000, &ctx).expect("write");
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.windows(2).all(|w| w[1] > w[0]),
        "not monotonic: {seen:?}"
    );
    assert_eq!(*seen.last().expect("at least one event"), n);
    let _ = std::fs::remove_file(&dest);
}

/// A zero-sector source is a caller error, not a zero-byte image: an empty
/// ISO is never what anyone wanted, and failing here names the problem.
#[test]
fn zero_sectors_is_an_error() {
    let dest = tmp("empty");
    let mut src = PatternSource {
        sectors: 0,
        short_after: None,
    };
    let err = write_image(&mut src, &dest, 0, &Ctx::default()).expect_err("must fail");
    assert!(matches!(err, Error::EmptyImage), "got {err:?}");
    // The destination must not have been created — a failed run leaves no
    // stub for a later run to mistake for output.
    assert!(!dest.exists(), "empty run created a file");
}

// A sink that records its writes and how it was finished, failing the write at `fail_at`.
struct RecordingSink {
    writes: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    finished: std::sync::Arc<std::sync::Mutex<Option<Finish>>>,
    fail_at: Option<u64>,
}

impl BlockSink for RecordingSink {
    fn write_at(&mut self, lba: u64, bytes: &[u8]) -> FmResult<()> {
        assert_eq!(bytes.len() % SECTOR_BYTES, 0);
        if self.fail_at == Some(lba) {
            return Err(Error::IoError {
                source: std::io::Error::other("sink full (expected by the test)"),
            });
        }
        self.writes.lock().unwrap().push(lba);
        Ok(())
    }
    fn finish(self: Box<Self>, how: Finish) -> FmResult<u64> {
        *self.finished.lock().unwrap() = Some(how);
        Ok(0)
    }
}

type Log<T> = std::sync::Arc<std::sync::Mutex<T>>;

fn recording(fail_at: Option<u64>) -> (Box<RecordingSink>, Log<Vec<u64>>, Log<Option<Finish>>) {
    let sink = RecordingSink {
        writes: Default::default(),
        finished: Default::default(),
        fail_at,
    };
    let (w, f) = (sink.writes.clone(), sink.finished.clone());
    (Box::new(sink), w, f)
}

// A failed write is reported (not a later read error), ends the copy and finishes incomplete.
#[test]
fn a_write_failure_wins_over_the_read_behind_it() {
    let b = u64::from(BATCH_SECTORS);
    let (sink, writes, finished) = recording(Some(b));
    let mut src = PatternSource {
        sectors: 10 * BATCH_SECTORS,
        short_after: Some(2 * BATCH_SECTORS),
    };
    let r = copy_out(&mut src, sink, 10 * BATCH_SECTORS, &Ctx::default());
    assert!(matches!(r, Err(Error::IoError { .. })), "{r:?}");
    assert_eq!(*writes.lock().unwrap(), vec![0]);
    assert_eq!(*finished.lock().unwrap(), Some(Finish::Incomplete));
}

// A read failure lands after every earlier batch is written, then finishes incomplete.
#[test]
fn a_read_failure_follows_the_batches_already_read() {
    let (sink, writes, finished) = recording(None);
    let mut src = PatternSource {
        sectors: 10 * BATCH_SECTORS,
        short_after: Some(3 * BATCH_SECTORS),
    };
    let r = copy_out(&mut src, sink, 10 * BATCH_SECTORS, &Ctx::default());
    assert!(matches!(r, Err(Error::ShortImageRead { .. })), "{r:?}");
    let b = u64::from(BATCH_SECTORS);
    assert_eq!(*writes.lock().unwrap(), vec![0, b, 2 * b]);
    assert_eq!(*finished.lock().unwrap(), Some(Finish::Incomplete));
}

// A Stop mid-copy halts at the next batch; what was read is written, then finished incomplete.
#[test]
fn a_stop_mid_copy_halts_after_the_batches_in_flight() {
    let (sink, writes, finished) = recording(None);
    let halt = crate::halt::Halt::new();
    let stop = halt.clone();
    let ctx = Ctx::new(halt).with_events(std::sync::Arc::new(move |e: &Event<'_>| {
        if matches!(e, Event::BytesWritten { .. }) {
            stop.cancel();
        }
    }));
    let mut src = PatternSource {
        sectors: 50 * BATCH_SECTORS,
        short_after: None,
    };
    let r = copy_out(&mut src, sink, 50 * BATCH_SECTORS, &ctx);
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    let n = writes.lock().unwrap().len();
    assert!((1..=3).contains(&n), "{n} batches written");
    assert_eq!(*finished.lock().unwrap(), Some(Finish::Incomplete));
}

// The writer takes batch N while batch N+1 is read: a write that waits for the next read to
// start would hang a serial copy, and completes here.
#[test]
fn the_next_read_overlaps_the_current_write() {
    struct Signals(std::sync::mpsc::Sender<u32>, PatternSource);
    impl SectorSource for Signals {
        fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], r: bool) -> FmResult<usize> {
            let _ = self.0.send(lba);
            self.1.read_sectors(lba, n, buf, r)
        }
    }
    struct Waits(std::sync::mpsc::Receiver<u32>);
    impl BlockSink for Waits {
        fn write_at(&mut self, lba: u64, _: &[u8]) -> FmResult<()> {
            let next = u64::from(BATCH_SECTORS) + lba;
            if next >= u64::from(3 * BATCH_SECTORS) {
                return Ok(()); // the last batch: no read follows it
            }
            loop {
                match self.0.recv_timeout(std::time::Duration::from_secs(5)) {
                    Ok(started) if u64::from(started) >= next => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => return Err(Error::Halted),
                }
            }
        }
        fn finish(self: Box<Self>, _: Finish) -> FmResult<u64> {
            Ok(0)
        }
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let total = 3 * BATCH_SECTORS;
    let mut src = Signals(
        tx,
        PatternSource {
            sectors: total,
            short_after: None,
        },
    );
    // A serial copy would time out on the first write; the overlapped one completes.
    let r = copy_out(&mut src, Box::new(Waits(rx)), total, &Ctx::default());
    assert!(r.is_ok(), "every write saw the next read start: {r:?}");
}

// An AACS whole-disc copy through the keyed reader, whatever each file's grid, writes the
// plaintext byte for byte (CPI cleared in the keyed file only).
#[test]
fn a_keyed_copy_writes_the_plaintext_image() {
    use crate::test_util::{BdFile, encrypted_bd_image, keyed_reader, unit_key_ro};
    let key = [0x2D; 16];
    let inf = unit_key_ro(crate::aacs::mkb::AacsVersion::V10, &[[0u8; 16]], &[1]);
    let img = encrypted_bd_image(
        &[
            BdFile::new("BDMV/STREAM/00001.m2ts", 1700 * 3, Some(key)),
            BdFile::new("BDMV/STREAM/00002.m2ts", 31, None),
        ],
        &inf,
    );
    let (start, sectors) = img.files[0];
    let mut want = img.plain.clone();
    let at = start as usize * SECTOR_BYTES;
    for p in want[at..at + sectors as usize * SECTOR_BYTES]
        .chunks_mut(crate::consts::BD_SOURCE_PACKET_BYTES)
    {
        p[0] &= 0x3F;
    }
    let mut r = keyed_reader(img.source(), key, &[(start, start + sectors)]);
    let td = tempfile::tempdir().unwrap();
    let dest = td.path().join("copy.iso");
    let cap = (img.image.len() / SECTOR_BYTES) as u32;
    write_image(&mut r, &dest, cap, &Ctx::default()).expect("copy");
    assert!(std::fs::read(&dest).unwrap() == want, "start {start}");
}

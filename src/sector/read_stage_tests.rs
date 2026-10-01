//! The live Read policy, driven one chunk at a time through `LiveRead` (the read loop
//! the live-drive mux runs in its prefetch producer).
use super::*;
use crate::disc::{ContentFormat, DiscTitle};
use crate::halt::Halt;
use std::io;

// The live Read stage over a decrypting reader, one chunk per `fill_extents`, with the
// counters the tests read. `skip_errors` applies from the first fill.
struct LiveRead {
    walk: Option<ExtentWalk>,
    extents: Vec<Extent>,
    batch: u16,
    unit_align: u16,
    ctx: Ctx,
    reader: crate::sector::DecryptingSectorSource<Box<dyn SectorSource>>,
    skip_errors: bool,
    read_buf: Vec<u8>,
    buf_valid: usize,
    current_offset: u32,
    errors: u64,
    lost_bytes: u64,
    bytes_read_total: u64,
}

impl LiveRead {
    fn new(
        reader: Box<dyn SectorSource>,
        title: DiscTitle,
        keys: crate::decrypt::DecryptKeys,
        batch: u16,
        _format: ContentFormat,
        _raw: bool,
        ctx: &Ctx,
    ) -> io::Result<Self> {
        if batch == 0 {
            return Err(Error::MuxBatchSectorsZero.into());
        }
        let unit_align = match keys {
            crate::decrypt::DecryptKeys::Aacs { .. } => 3,
            _ => 1,
        };
        Ok(LiveRead {
            walk: None,
            extents: title.extents.clone(),
            batch,
            unit_align,
            ctx: ctx.clone(),
            reader: crate::sector::DecryptingSectorSource::new(reader, keys),
            skip_errors: false,
            read_buf: Vec::new(),
            buf_valid: 0,
            current_offset: 0,
            errors: 0,
            lost_bytes: 0,
            bytes_read_total: 0,
        })
    }

    fn fill_extents(&mut self) -> io::Result<bool> {
        if self.walk.is_none() {
            let policy = ReadPolicy::Live {
                batch: self.batch,
                skip_errors: self.skip_errors,
            };
            self.walk = Some(ExtentWalk::new(
                self.extents.clone(),
                policy,
                self.unit_align,
                &self.ctx,
            )?);
        }
        let w = self.walk.as_mut().expect("built above");
        let r = w.next(&mut self.reader, &mut self.read_buf);
        let loss = w.loss();
        self.errors = loss.skips();
        self.lost_bytes = loss.bytes();
        self.bytes_read_total = w.bytes_read;
        self.buf_valid = self.read_buf.len();
        self.current_offset = w.offset;
        r.map_err(io::Error::from)
    }
}

// Trivial SectorSource yielding zeroed sectors. Empty title -> no PES
// frames -> read() walks to EOF -> Ok(None); enough to exercise the
// trait-object dispatch (the goal is the bridge, not the demuxer).
struct ZeroReader {
    capacity: u32,
}

impl crate::sector::SectorSource for ZeroReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

// A SectorSource that UNDER-reports: writes the whole requested span but
// reports only the first sector valid, modeling a non-full-or-error
// source — the case fill_extents handled inconsistently pre-fix.
struct ShortReader {
    capacity: u32,
}

impl crate::sector::SectorSource for ShortReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let bytes = count as usize * 2048;
        // Stale marker across the whole span; only the first sector is
        // reported as actually delivered.
        buf[..bytes].fill(0xAB);
        Ok(2048usize.min(bytes))
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

fn short_read_stream(skip_errors: bool) -> LiveRead {
    short_read_stream_in(skip_errors, &crate::ctx::Ctx::default())
}

fn short_read_stream_in(skip_errors: bool, ctx: &crate::ctx::Ctx) -> LiveRead {
    let mut s = LiveRead::new(
        Box::new(ShortReader { capacity: 64 }),
        synthetic_title(64),
        crate::decrypt::DecryptKeys::None,
        8, // request 8 sectors (16384 B); the source delivers 1 (2048 B)
        ContentFormat::BdTs,
        false,
        ctx,
    )
    .unwrap();
    s.skip_errors = skip_errors;
    s
}

// A short read must never become a silent gap: advancing current_offset
// by the full requested count would drop the undelivered tail with no
// error. Without skip_errors it's a hard read error: E6000.
#[test]
fn short_read_without_skip_errors_is_reported_not_silently_skipped() {
    let mut s = short_read_stream(false);
    let err = s
        .fill_extents()
        .expect_err("a short read must not be reported as a clean fill");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("E6000"),
        "expected the disc-read code E6000, got {err}"
    );
}

// With skip_errors the undelivered tail is zero-filled and accounted, not
// left stale or dropped silently. current_offset still advances by the
// full request to stay on an AACS unit boundary.
#[test]
fn short_read_with_skip_errors_is_zero_filled_and_accounted() {
    let mut s = short_read_stream(true);
    assert!(
        s.fill_extents()
            .expect("skip_errors absorbs the short read")
    );

    // 8 sectors requested at 2048 B/sector (the ECMA-167 / UDF logical
    // sector size) = 16384 B of buffer; the source delivered 2048 B.
    assert_eq!(s.buf_valid, 16_384, "the whole requested span stays valid");
    assert_eq!(s.current_offset, 8, "the cursor advances a whole unit span");
    assert_eq!(
        s.lost_bytes, 14_336,
        "16384 requested - 2048 delivered = 14336 lost bytes must be counted"
    );
    assert_eq!(s.errors, 1, "the gap is counted as one skip event");
    // The delivered sector survives; the undelivered tail is zeroed, not
    // the stale 0xAB the source left behind.
    assert!(s.read_buf[..2048].iter().all(|&b| b == 0xAB));
    assert!(
        s.read_buf[2048..16_384].iter().all(|&b| b == 0),
        "the undelivered tail must be zero-filled, not stale bytes"
    );
}

// A malformed disc can declare thousands of zero-sector extents; the old self-recursive
// skip overflowed the stack (fatal, unlike iterative EOF).
#[test]
fn a_long_run_of_empty_extents_does_not_recurse_per_extent() {
    let title = DiscTitle {
        extents: (0..50_000)
            .map(|_| crate::disc::Extent {
                start_lba: 0,
                sector_count: 0,
            })
            .collect(),
        ..DiscTitle::empty()
    };
    let handle = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            let mut s = LiveRead::new(
                Box::new(ZeroReader { capacity: 0 }),
                title,
                crate::decrypt::DecryptKeys::None,
                8,
                ContentFormat::BdTs,
                false,
                &crate::ctx::Ctx::default(),
            )
            .unwrap();
            s.fill_extents()
        })
        .unwrap();
    assert!(
        !handle.join().unwrap().unwrap(),
        "an all-empty extent list is EOF, not an error"
    );
}

fn synthetic_title(sector_count: u32) -> DiscTitle {
    DiscTitle {
        extents: vec![crate::disc::Extent {
            start_lba: 0,
            sector_count,
        }],
        ..DiscTitle::empty()
    }
}

// MuxOptions::default() carries batch_sectors 0: a zero batch reads 0 sectors
// and never advances, so it must be refused up front, not spin until Stop.
#[test]
fn zero_batch_is_rejected_not_an_endless_zero_read() {
    let dir = tempfile::tempdir().unwrap();
    let iso = dir.path().join("t.iso");
    std::fs::write(&iso, vec![0u8; 4 * 2048]).unwrap();
    let src = crate::io::file_sector_source::FileSectorSource::open(&iso).unwrap();
    let res = LiveRead::new(
        Box::new(src),
        synthetic_title(4),
        crate::decrypt::DecryptKeys::None,
        0,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    );
    let err = res.err().expect("batch_sectors 0 must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(crate::error::error_code(&err), Some(9085));
}

// A truncated ISO is not a bad sector: even with skip_errors the missing tail
// must abort with the image error, not be zero-filled and reported as success.
#[test]
fn truncated_image_aborts_even_with_skip_errors() {
    let dir = tempfile::tempdir().unwrap();
    let iso = dir.path().join("short.iso");
    std::fs::write(&iso, vec![0u8; 4 * 2048]).unwrap();
    let src = crate::io::file_sector_source::FileSectorSource::open(&iso).unwrap();
    let mut stream = LiveRead::new(
        Box::new(src),
        synthetic_title(16),
        crate::decrypt::DecryptKeys::None,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    stream.skip_errors = true;
    let mut outcome = Ok(true);
    for _ in 0..64 {
        outcome = stream.fill_extents();
        if !matches!(outcome, Ok(true)) {
            break;
        }
    }
    let err = outcome.expect_err("a truncated image must abort the read");
    assert!(
        err.to_string()
            .starts_with(&format!("E{}", crate::error::E_IMAGE_ENDS_BEFORE_READ)),
        "got {err}"
    );
    assert_eq!(stream.errors, 0, "must not count as a skipped sector");
    assert_eq!(stream.lost_bytes, 0, "must not zero-fill the missing tail");
}

// Every read fails; the `stop_at`-th read cancels `halt` (Stop pressed inside a
// bad zone) and fails with `Halted` (a cancelled drive) or a media error.
struct StopInBadZoneReader {
    halt: Halt,
    stop_at: usize,
    reader_halted: bool,
    log: std::sync::Arc<std::sync::Mutex<Vec<bool>>>,
}

impl crate::sector::SectorSource for StopInBadZoneReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        _count: u16,
        _buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        let mut log = self.log.lock().unwrap();
        log.push(recovery);
        if log.len() >= self.stop_at {
            self.halt.cancel();
            if self.reader_halted {
                return Err(crate::error::Error::Halted);
            }
        }
        Err(crate::error::Error::DiscRead {
            sector: lba as u64,
            status: Some(0x02),
            sense: None,
        })
    }

    fn capacity_sectors(&self) -> u32 {
        64
    }
}

fn stop_in_bad_zone(
    batch: u16,
    stop_at: usize,
    reader_halted: bool,
    share_halt: bool,
    skip_errors: bool,
) -> (io::Result<bool>, LiveRead, Vec<bool>) {
    let halt = Halt::new();
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = StopInBadZoneReader {
        halt: halt.clone(),
        stop_at,
        reader_halted,
        log: log.clone(),
    };
    let ctx = if share_halt {
        crate::ctx::Ctx::new(halt.clone())
    } else {
        crate::ctx::Ctx::default()
    };
    let mut s = LiveRead::new(
        Box::new(reader),
        synthetic_title(64),
        crate::decrypt::DecryptKeys::None,
        batch,
        ContentFormat::BdTs,
        false,
        &ctx,
    )
    .unwrap();
    s.skip_errors = skip_errors;
    if stop_at == 0 {
        halt.cancel();
    }
    let res = s.fill_extents();
    let log = log.lock().unwrap().clone();
    (res, s, log)
}

// A drive cancelled by Stop fails its read with Halted: that is a stop, not a
// bad sector — no 60s recovery read, no skip accounting, no DiscRead.
#[test]
fn a_halted_read_at_the_bottomed_out_unit_is_a_stop_not_a_bad_sector() {
    for share_halt in [true, false] {
        for skip_errors in [true, false] {
            let (res, s, log) = stop_in_bad_zone(1, 1, true, share_halt, skip_errors);
            let err = res.expect_err("a halted read must not fill");
            assert!(
                crate::error::is_halt(&err),
                "share={share_halt} skip={skip_errors}: got {err}"
            );
            assert_eq!(log, vec![false], "no recovery read after a halted read");
            assert_eq!((s.errors, s.lost_bytes), (0, 0), "no bogus skip");
        }
    }
}

// Stop pressed while the read stalls in a failing region returns Halted at once,
// never walking the shrink/recovery ladder first.
#[test]
fn stop_in_a_failing_region_returns_halted_without_more_reads() {
    let (res, s, log) = stop_in_bad_zone(32, 0, false, true, true);
    assert!(crate::error::is_halt(&res.expect_err("halted before read")));
    assert!(log.is_empty(), "no read once already halted: {log:?}");
    assert_eq!(s.errors, 0);

    let (res, s, log) = stop_in_bad_zone(32, 2, false, true, true);
    let err = res.expect_err("a stop mid bad zone must not fill");
    assert!(crate::error::is_halt(&err), "got {err}");
    assert_eq!(log, vec![false, false], "no read after the stop");
    assert_eq!((s.errors, s.lost_bytes), (0, 0), "no bogus skip");
}

// Recording SectorSource: logs every (lba, count) request, errors when the
// range covers bad_sector. Successful reads return zeroed sectors (not
// flagged scrambled, so DecryptingSectorSource passes them through as-is).
struct RecordingReader {
    capacity: u32,
    bad_sector: u32,
    log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16)>>>,
}

impl crate::sector::SectorSource for RecordingReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        self.log.lock().unwrap().push((lba, count));
        let end = lba + count as u32;
        if self.bad_sector >= lba && self.bad_sector < end {
            return Err(crate::error::Error::DiscRead {
                sector: self.bad_sector as u64,
                status: Some(0x02),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

// SectorSource that fails every read covering bad_sector with a SCSI
// transport failure (status=0xFF, the USB-bridge-crash sentinel Drive::read
// surfaces). Logs each (lba, count) so a test can prove it wasn't retried.
struct TransportFailReader {
    capacity: u32,
    bad_sector: u32,
    log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16)>>>,
}

impl crate::sector::SectorSource for TransportFailReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        self.log.lock().unwrap().push((lba, count));
        let end = lba + count as u32;
        if self.bad_sector >= lba && self.bad_sector < end {
            return Err(crate::error::Error::DiscRead {
                sector: self.bad_sector as u64,
                status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

// SectorSource mirroring a marginal sector recoverable only with the
// drive's full ECC budget: fails while recovery=false, succeeds once
// recovery=true. Exercises fill_extents' last-chance recovery-read branch.
struct RecoverableReader {
    capacity: u32,
    bad_sector: u32,
    /// `(lba, count, recovery)` for every issued read.
    log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16, bool)>>>,
}

impl crate::sector::SectorSource for RecoverableReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        self.log.lock().unwrap().push((lba, count, recovery));
        let end = lba + count as u32;
        let covers_bad = self.bad_sector >= lba && self.bad_sector < end;
        // Fail on the fast (non-recovery) pass; the 60s ECC recovery read
        // succeeds. Distinct non-0x02 sense byte so a transport-failure
        // re-check (status 0xFF) is provably NOT triggered here.
        if covers_bad && !recovery {
            return Err(crate::error::Error::DiscRead {
                sector: self.bad_sector as u64,
                status: Some(0x02),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

// Coverage for the recovery-read SUCCESS branch (rc.5.2 audit #3): a
// sector failing the fast read but clean on the 60s ECC pass must mux the
// RECOVERED data — cursor advances, no skip counted. unit_align=1 (None).
#[test]
fn recovery_read_success_muxes_recovered_data_no_skip() {
    const COUNT: u32 = 10;
    let bad = 4u32;
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = RecoverableReader {
        capacity: COUNT,
        bad_sector: bad,
        log: log.clone(),
    };
    let mut stream = LiveRead::new(
        Box::new(reader),
        synthetic_title(COUNT),
        crate::decrypt::DecryptKeys::None,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    // skip_errors=false: if the recovery read did NOT succeed, fill_extents
    // would return Err — so reaching EOF cleanly proves recovery worked.
    stream.skip_errors = false;

    let mut guard = 0;
    loop {
        match stream.fill_extents() {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => panic!("recovery read should have succeeded, got: {e}"),
        }
        guard += 1;
        assert!(guard < 1000, "fill_extents did not reach EOF");
    }

    // No skip counted: the recovered unit was muxed, not zero-filled.
    assert_eq!(
        stream.errors, 0,
        "a successful recovery read must not count as a skipped sector"
    );
    assert_eq!(
        stream.lost_bytes, 0,
        "a successful recovery read loses no bytes"
    );
    // All COUNT sectors' worth of bytes were read through to the cursor end.
    assert_eq!(
        stream.bytes_read_total,
        COUNT as u64 * 2048,
        "every sector (including the recovered one) must be counted as read"
    );

    // The bad sector was retried with recovery=true and that read SUCCEEDED.
    let reads = log.lock().unwrap();
    assert!(
        reads
            .iter()
            .any(|&(lba, count, rec)| rec && lba == bad && count == 1),
        "expected a recovery=true single-sector read at the bad sector; got {reads:?}"
    );
    // And the fast pass at the bad sector did happen with recovery=false.
    assert!(
        reads.iter().any(|&(lba, _c, rec)| !rec && lba == bad),
        "expected a non-recovery read to have first failed at the bad sector"
    );
}

// SectorSource that fails the fast read at bad_sector with status 0x02,
// then fails the 60s ECC recovery read with transport failure (0xFF) —
// models a bridge wedging during the last-chance recovery read.
struct RecoveryTransportFailReader {
    capacity: u32,
    bad_sector: u32,
    log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16, bool)>>>,
}

impl crate::sector::SectorSource for RecoveryTransportFailReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        self.log.lock().unwrap().push((lba, count, recovery));
        let end = lba + count as u32;
        if self.bad_sector >= lba && self.bad_sector < end {
            let status = if recovery {
                crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE
            } else {
                0x02
            };
            return Err(crate::error::Error::DiscRead {
                sector: self.bad_sector as u64,
                status: Some(status),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
}

// Regression (rc.5.2 audit #2): a transport failure on the 60s ECC
// RECOVERY read (not just the initial read) must ABORT even under
// skip_errors=true, not fall into skip/advance and march forever.
#[test]
fn transport_failure_on_recovery_read_aborts_even_with_skip_errors() {
    const COUNT: u32 = 10;
    let bad = 4u32;
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = RecoveryTransportFailReader {
        capacity: COUNT,
        bad_sector: bad,
        log: log.clone(),
    };
    let mut stream = LiveRead::new(
        Box::new(reader),
        synthetic_title(COUNT),
        crate::decrypt::DecryptKeys::None,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    stream.skip_errors = true;

    // Drive fill_extents across batches: the batch covering the bad sector
    // shrinks to size 1, fails the fast read (0x02), then the bottom-out
    // recovery read returns the transport failure (0xFF), which must abort.
    let mut res = Ok(true);
    for _ in 0..1000 {
        res = stream.fill_extents();
        if !matches!(res, Ok(true)) {
            break;
        }
    }
    assert!(
        res.is_err(),
        "a transport failure on the recovery read must abort fill_extents, got {res:?}"
    );
    assert_eq!(
        stream.errors, 0,
        "a recovery-read transport-failure abort must NOT count as a skip"
    );
    assert_eq!(
        stream.lost_bytes, 0,
        "a transport-failure abort zero-fills nothing"
    );
    // Prove the bottom-out recovery read was actually reached and aborted on.
    let reads = log.lock().unwrap();
    assert!(
        reads
            .iter()
            .any(|&(lba, count, rec)| rec && lba == bad && count == 1),
        "expected a recovery=true read at the bad sector to have been attempted; got {reads:?}"
    );
}

// Fast read fails at bad_sector (0x02); the recovery read returns `rec_err()`.
struct RecoveryErrReader {
    bad_sector: u32,
    rec_err: fn() -> crate::error::Error,
}

impl crate::sector::SectorSource for RecoveryErrReader {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        if self.bad_sector >= lba && self.bad_sector < lba + count as u32 {
            if recovery {
                return Err((self.rec_err)());
            }
            return Err(crate::error::Error::DiscRead {
                sector: self.bad_sector as u64,
                status: Some(0x02),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }

    fn capacity_sectors(&self) -> u32 {
        10
    }
}

// A key stop or dead source on the RECOVERY read must abort even under
// skip_errors, never be zero-filled as a skipped sector.
#[test]
fn key_stop_or_dead_source_on_recovery_read_aborts_even_with_skip_errors() {
    type MkErr = fn() -> crate::error::Error;
    let cases: [(&str, MkErr); 3] = [
        ("NoDiscKey", || crate::error::Error::NoDiscKey {
            disc_hash: String::new(),
        }),
        ("WholeDiscKeyMissing", || {
            crate::error::Error::WholeDiscKeyMissing
        }),
        ("SourceTerminated", || crate::error::Error::SourceTerminated),
    ];
    for (name, rec_err) in cases {
        let mut stream = LiveRead::new(
            Box::new(RecoveryErrReader {
                bad_sector: 4,
                rec_err,
            }),
            synthetic_title(10),
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
            false,
            &crate::ctx::Ctx::default(),
        )
        .unwrap();
        stream.skip_errors = true;
        let mut res = Ok(true);
        for _ in 0..1000 {
            res = stream.fill_extents();
            if !matches!(res, Ok(true)) {
                break;
            }
        }
        assert!(
            res.is_err(),
            "{name} on the recovery read must abort, got {res:?}"
        );
        assert_eq!(stream.errors, 0, "{name}: must not count as a skip");
        assert_eq!(stream.lost_bytes, 0, "{name}: must not zero-fill");
    }
}

// Regression: a USB-bridge transport crash (0xFF) during a single-pass
// disc://->mkv:// rip must ABORT immediately, even under skip_errors=true
// — mirrors the multipass sweep's short-circuit; exactly one read issued.
#[test]
fn transport_failure_aborts_single_pass_even_with_skip_errors() {
    const COUNT: u32 = 10;
    let bad = 4u32;
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = TransportFailReader {
        capacity: COUNT,
        bad_sector: bad,
        log: log.clone(),
    };
    let mut stream = LiveRead::new(
        Box::new(reader),
        synthetic_title(COUNT),
        crate::decrypt::DecryptKeys::None,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    stream.skip_errors = true;

    let res = stream.fill_extents();
    assert!(
        res.is_err(),
        "transport failure must abort fill_extents, not skip past it"
    );
    assert_eq!(
        stream.errors, 0,
        "a transport-failure abort must NOT count as a skipped sector"
    );
    let reads = log.lock().unwrap();
    assert_eq!(
        reads.len(),
        1,
        "transport failure must abort after the first failed read with no \
         shrink/retry/skip-ahead; got reads {reads:?}"
    );
}

// AACS unit-alignment skip: with unit_align=3 and skip_errors=true, a
// single bad mid-extent sector must not desync the title — every read
// starts on a 3-sector unit boundary and skips advance a whole unit.
#[test]
fn aacs_reads_stay_unit_aligned_and_skip_whole_units() {
    const COUNT: u32 = 30;
    const ALIGN: u32 = 3;
    // Bad sector at offset 13 — inside unit 4 (offsets 12,13,14). The
    // whole unit must be skipped, keeping the cursor unit-aligned.
    let bad = 13u32;
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = RecordingReader {
        capacity: COUNT,
        bad_sector: bad,
        log: log.clone(),
    };
    let title = synthetic_title(COUNT);
    let keys = crate::decrypt::DecryptKeys::Aacs {
        unit_keys: vec![(0, [0u8; 16])],
        format: crate::disc::ContentFormat::BdTs,
    };
    let stream = LiveRead::new(
        Box::new(reader),
        title,
        keys,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    // AACS decrypts only through a key map; without one every read is a decrypt refusal.
    let map = crate::keys::test_key_map(vec![(0, COUNT, 0, crate::decrypt::Phase::All)]);
    let mut stream = stream;
    stream.reader = crate::keys::test_keyed_source(stream.reader, map);
    stream.skip_errors = true;
    assert_eq!(
        stream.unit_align, ALIGN as u16,
        "AACS keys must set unit_align=3"
    );

    // Drive fill_extents to EOF (no PES demux needed — we observe the
    // raw read pattern directly).
    let ext_start = 0u32;
    let mut guard = 0;
    loop {
        match stream.fill_extents() {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => panic!("fill_extents errored unexpectedly: {e}"),
        }
        guard += 1;
        assert!(guard < 1000, "fill_extents did not reach EOF");
    }

    let reads = log.lock().unwrap();
    assert!(!reads.is_empty(), "expected at least one read");
    for &(lba, count) in reads.iter() {
        assert_eq!(
            (lba - ext_start) % ALIGN,
            0,
            "read at lba {lba} is not unit-aligned (offset {} % {ALIGN} != 0)",
            lba - ext_start
        );
        // Non-tail reads must be a whole number of units; the only permitted
        // short read is a final partial unit. A mid-stream non-unit-multiple
        // read would straddle AACS unit boundaries and decrypt wrong.
        assert!(
            (count as u32).is_multiple_of(ALIGN) || (count as u32) < ALIGN,
            "read count {count} is neither a whole number of units nor a sub-unit tail"
        );
    }

    // At least one error was skipped (the bad unit) and a SectorSkipped
    // event was emitted; errors counter advanced by exactly the bad units.
    assert!(stream.errors >= 1, "expected the bad unit to be skipped");

    // Regression: `lost_bytes` must account for the WHOLE skipped unit
    // (3 sectors = 6144 bytes), not one sector — `errors * 2048` would
    // undercount AACS loss ~3x, the single-pass abort-gate bug this guards.
    assert_eq!(
        stream.lost_bytes,
        stream.errors * ALIGN as u64 * 2048,
        "AACS skip must record a whole unit (6144 B) per skip event, not 2048"
    );
    assert!(
        stream.lost_bytes > stream.errors * 2048,
        "lost_bytes must exceed the errors*2048 undercount for AACS units"
    );

    // Crucial anti-desync assertion: the read that bottomed out and was
    // skipped must have been a single 3-sector unit starting at offset 12
    // (the unit boundary at/below bad sector 13), NOT a 1-sector read at 13.
    assert!(
        reads
            .iter()
            .any(|&(lba, count)| lba == 12 && count == ALIGN as u16),
        "expected a unit-aligned (lba=12,count=3) read over the bad unit; got {reads:?}"
    );
    // And NO single-sector read at the bad sector itself (would be a desync).
    assert!(
        !reads.iter().any(|&(lba, count)| lba == bad && count == 1),
        "a 1-sector read at the bad sector {bad} would desync the AACS unit stream"
    );
}

/// `unit_align == 1` (DecryptKeys::None) variant: single-sector skips
/// still work (CSS/raw is self-synchronizing, so a 1-sector skip is
/// correct there — contrast with the AACS whole-unit skip above).
#[test]
fn unencrypted_single_sector_skip_works() {
    const COUNT: u32 = 10;
    let bad = 4u32;
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = RecordingReader {
        capacity: COUNT,
        bad_sector: bad,
        log: log.clone(),
    };
    let mut stream = LiveRead::new(
        Box::new(reader),
        synthetic_title(COUNT),
        crate::decrypt::DecryptKeys::None,
        8,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .unwrap();
    stream.skip_errors = true;
    assert_eq!(stream.unit_align, 1, "None keys must leave unit_align=1");

    let mut guard = 0;
    loop {
        match stream.fill_extents() {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => panic!("fill_extents errored unexpectedly: {e}"),
        }
        guard += 1;
        assert!(guard < 1000, "fill_extents did not reach EOF");
    }

    let reads = log.lock().unwrap();
    // The bad sector must have been retried down to a single sector and
    // skipped at count==1 — the self-synchronizing per-sector path.
    assert!(
        reads.iter().any(|&(lba, count)| lba == bad && count == 1),
        "align=1 must bottom out at a 1-sector read over the bad sector; got {reads:?}"
    );
    assert!(stream.errors >= 1);
    // align=1: a skip event covers exactly one sector, so lost_bytes
    // and errors*2048 agree (the AACS undercount does not apply here).
    assert_eq!(
        stream.lost_bytes,
        stream.errors * 2048,
        "single-sector (align=1) skip must record exactly 2048 B per event"
    );
}

// ── AdaptiveBatch ─────────────────────────────────────────────────

// Batch sizes >= 6 must stay a multiple of 3 (AACS units) or the next
// read straddles a boundary; below 6 the ladder descends 3 -> 1.
#[test]
fn halve_batch_size_keeps_unit_alignment_and_bottoms_out_at_one() {
    assert_eq!(
        halve_batch_size(64),
        30,
        "32 rounded down to a unit multiple"
    );
    assert_eq!(halve_batch_size(30), 15);
    assert_eq!(halve_batch_size(12), 6, "6 is already unit-aligned");
    assert_eq!(halve_batch_size(11), 5, "below 6: no alignment rounding");
    assert_eq!(halve_batch_size(6), 3);
    assert_eq!(halve_batch_size(3), 1);
    assert_eq!(halve_batch_size(2), 1);
    assert_eq!(
        halve_batch_size(1),
        1,
        "must never reach 0 — a 0-sector read"
    );
    for size in 1u16..=4096 {
        let h = halve_batch_size(size);
        assert!(h >= 1, "halve({size}) must never be 0");
        assert!(h <= size, "halve({size}) = {h} must not grow");
        assert!(
            h < 6 || h.is_multiple_of(3),
            "halve({size}) = {h} is unit-unaligned"
        );
    }
}

#[test]
fn double_batch_size_grows_toward_preferred_without_breaking_alignment() {
    assert_eq!(double_batch_size(30, 64), 60);
    assert_eq!(
        double_batch_size(4, 64),
        6,
        "8 rounded down to a unit multiple"
    );
    assert_eq!(
        double_batch_size(1, 64),
        2,
        "below 6: no alignment rounding"
    );
    assert_eq!(
        double_batch_size(60, 64),
        63,
        "clamped to preferred, then aligned"
    );
    for size in 1u16..=2048 {
        let d = double_batch_size(size, 4096);
        assert!(d >= size, "double({size}) = {d} must not shrink");
        assert!(
            d < 6 || d.is_multiple_of(3),
            "double({size}) = {d} is unit-unaligned"
        );
    }
}

// The sizer must probe back up: after a failure drops the batch, a
// sustained clean run must return BatchSizeChanged{Probed} AND raise
// current — never probing locks the rip at reduced size forever.
#[test]
fn on_success_probes_up_after_a_sustained_clean_run_and_resets_the_streak() {
    let mut b = AdaptiveBatch::new(64);
    assert!(
        matches!(
            b.on_failure(),
            Some(Event::BatchSizeChanged {
                new_size: 30,
                reason: BatchSizeReason::Shrunk
            })
        ),
        "a failure must shrink 64 -> 30"
    );
    assert_eq!(b.current(), 30);

    // Just under the 51200-sector probe threshold: still silent.
    let mut fed = 0u32;
    while fed + 30 < PROBE_THRESHOLD_SECTORS {
        assert!(
            b.on_success(30).is_none(),
            "no probe before {PROBE_THRESHOLD_SECTORS} clean sectors (at {fed})"
        );
        fed += 30;
    }
    assert_eq!(b.current(), 30, "still at the reduced size");

    // The read that crosses the threshold probes up.
    let ev = b
        .on_success(30)
        .expect("a sustained clean run must probe the batch size back up");
    assert!(
        matches!(
            ev,
            Event::BatchSizeChanged {
                new_size: 60,
                reason: BatchSizeReason::Probed
            }
        ),
        "expected a Probed grow to 60, got {ev:?}"
    );
    assert_eq!(
        b.current(),
        60,
        "the sizer must actually adopt the new size"
    );
    assert_eq!(
        b.streak_sectors, 0,
        "the streak resets so the next probe needs a fresh clean run"
    );

    // A failure mid-streak must reset it: no probe right after a failure.
    let mut b = AdaptiveBatch::new(64);
    b.on_failure();
    b.on_success(30);
    assert!(b.streak_sectors > 0);
    b.on_failure();
    assert_eq!(b.streak_sectors, 0, "a failure must reset the clean streak");

    // At the preferred size a clean run must NOT keep firing events.
    let mut b = AdaptiveBatch::new(64);
    for _ in 0..(PROBE_THRESHOLD_SECTORS / 64 + 2) {
        assert!(
            b.on_success(64).is_none(),
            "no probe is possible when already at the preferred size"
        );
    }
    assert_eq!(b.current(), 64);
}

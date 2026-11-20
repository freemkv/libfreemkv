use super::*;
use crate::disc::Extent;
use crate::error::Result;
use crate::event::Event;
use std::sync::mpsc;
use std::time::Duration;

// Endless zero-yielding source: every read succeeds, so the producer
// pushes batches until the channel disconnects — the shape that
// wedged the pre-1.0.0 `clone + mem::forget` `into_channels`.
struct EndlessZeroSource;
impl SectorSource for EndlessZeroSource {
    fn read_sectors(
        &mut self,
        _lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let bytes = count as usize * 2048;
        buf[..bytes].fill(0);
        Ok(bytes)
    }
}

/// Synthetic source that fills `buf` with a per-sector byte
/// pattern derived from the LBA, always satisfying the full
/// request (mirrors `FileSectorSource`'s read_exact contract).
struct PatternSource {
    capacity: u32,
}

impl SectorSource for PatternSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let bytes = count as usize * 2048;
        for s in 0..count as usize {
            let base = s * 2048;
            let tag = (lba.wrapping_add(s as u32) & 0xff) as u8;
            for b in &mut buf[base..base + 2048] {
                *b = tag;
            }
        }
        Ok(bytes)
    }
}

/// Source that returns a short read (fewer sectors than
/// requested) on its very first call, then full reads. Used to
/// prove the producer advances by sectors actually read.
struct ShortFirstSource {
    capacity: u32,
    first: bool,
}

impl SectorSource for ShortFirstSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        let give = if self.first {
            self.first = false;
            // Short read: hand back one aligned unit (3 sectors)
            // regardless of the larger request.
            SECTOR_ALIGNMENT.min(count)
        } else {
            count
        };
        let bytes = give as usize * 2048;
        for s in 0..give as usize {
            let base = s * 2048;
            let tag = (lba.wrapping_add(s as u32) & 0xff) as u8;
            for b in &mut buf[base..base + 2048] {
                *b = tag;
            }
        }
        Ok(bytes)
    }
}

fn big_extent() -> Vec<Extent> {
    // One huge extent so the producer never reaches EOF on its own;
    // the only way it can exit is by observing channel disconnection.
    vec![Extent {
        start_lba: 0,
        sector_count: u32::MAX,
    }]
}

/// Run `f` on a helper thread and fail if it does not finish within
/// `secs`. Used to turn a join-deadlock into a test failure instead
/// of a hung CI run.
fn within<F: FnOnce() + Send + 'static>(secs: u64, f: F) {
    let (done_tx, done_rx) = bounded::<()>(1);
    std::thread::spawn(move || {
        f();
        let _ = done_tx.send(());
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_secs(secs))
            .is_ok(),
        "operation did not complete within {secs}s (deadlock)"
    );
}

/// Run `f` on a worker thread; fail (rather than hang) if it does
/// not finish within `timeout`. Guards the deadlock regression so
/// a reintroduced bug fails the suite instead of wedging it.
fn with_watchdog<F>(timeout: Duration, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let h = std::thread::spawn(move || {
        f();
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(timeout) {
        Ok(()) => {
            let _ = h.join();
        }
        Err(_) => panic!("watchdog timeout — likely deadlock/hang in prefetch read path"),
    }
}

// The producer thread takes the reader by value, so the list is snapshotted
// at construction and still forwarded afterwards.
#[test]
fn prefetched_source_forwards_unmapped_stream_files() {
    let _serial = serial();
    use crate::sector::bus_removal::test_support::{Reports, m2ts1, unmapped_paths};
    let ext = vec![crate::disc::Extent {
        start_lba: 0,
        sector_count: 3,
    }];
    let s = PrefetchedSectorSource::new(Reports(vec![m2ts1()]), ext, 3, &Ctx::default()).unwrap();
    assert_eq!(unmapped_paths(&s), ["/BDMV/STREAM/00001.m2ts"]);
}

// Regression: dropping a `PrefetchedSectorSource` DIRECTLY, before extents are drained,
// must join the producer cleanly.
#[test]
fn drop_undrained_source_joins_cleanly() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 300,
        }];
        let halt = Halt::new();
        let pf = PrefetchedSectorSource::new(
            PatternSource { capacity: 9999 },
            extents,
            3,
            &Ctx::new(halt.clone()),
        )
        .expect("spawn");
        // Drop without draining a single batch — the old Drop deadlocked here.
        drop(pf);
    });
}

// CRITICAL regression: after `into_channels`, dropping the forward
// receiver + recycle sender must let the producer observe
// disconnection so `PrefetchShell` drop (join) returns promptly.
#[test]
fn into_channels_drop_releases_producer() {
    let _serial = serial();
    within(10, || {
        let src = PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, &Ctx::default())
            .expect("spawn");
        let (rx, recycle_tx, shell) = src.into_channels();
        // Consumer goes away early (halt / abort analogue): drop both
        // channel endpoints without draining to EOF.
        drop(rx);
        drop(recycle_tx);
        // Joining the producer must not hang.
        drop(shell);
    });
}

// L106 (safe dead-channel/`Option::take` rewrite, no `unsafe`): dropping `shell` must
// release the producer's Drive-holder slot within the settle bound below — a leaked
// thread would never let the count return to `before`.
#[test]
fn into_channels_then_drop_leaks_no_drive_holder() {
    let _serial = serial();
    let before = crate::halt::live_drive_holders();
    within(10, || {
        let src = PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, &Ctx::default())
            .expect("spawn");
        let (rx, recycle_tx, shell) = src.into_channels();
        drop(rx);
        drop(recycle_tx);
        drop(shell); // joins: producer thread ends, its holder guard drops.
    });
    assert!(
        holders_settle_to(before, Duration::from_secs(2)),
        "into_channels + drop must release the producer's drive-holder slot exactly once"
    );
}

// No-double-drop is structural now (E0040 bars an explicit second `Drop::drop`). This
// checks channels handed back by `into_channels` still carry real batches end-to-end —
// the safe rewrite must not have swapped in the dead placeholder channels by mistake.
#[test]
fn into_channels_channels_still_deliver_batches() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let start = 40u32;
        let count = 9u32; // three units
        let extents = vec![Extent {
            start_lba: start,
            sector_count: count,
        }];
        let src = PatternSource { capacity: 1000 };
        let pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let (rx, recycle_tx, shell) = pf.into_channels();
        let mut got = Vec::new();
        while let Ok(batch) = rx.recv() {
            let buf = batch.expect("no read error expected");
            got.extend_from_slice(&buf);
            let _ = recycle_tx.send(buf);
        }
        drop(recycle_tx);
        drop(shell);
        assert_eq!(got.len(), (count as usize) * 2048, "all sectors delivered");
        for i in 0..count {
            let tag = ((start + i) & 0xff) as u8;
            let off = i as usize * 2048;
            assert!(
                got[off..off + 2048].iter().all(|b| *b == tag),
                "sector {i} (lba {}) content mismatch after into_channels",
                start + i
            );
        }
    });
}

// Same property via the halt path: cancel the token, then the producer
// must exit and the shell join must complete (drain on a background
// thread so blocked sends make progress toward the halt check).
#[test]
fn halt_releases_producer() {
    let _serial = serial();
    within(10, || {
        let halt = Halt::new();
        let src = PrefetchedSectorSource::new(
            EndlessZeroSource,
            big_extent(),
            3,
            &Ctx::new(halt.clone()),
        )
        .expect("spawn");
        let (rx, recycle_tx, shell) = src.into_channels();
        // Drain the forward channel so the producer's sends always
        // make progress and it can reach the halt check at the loop
        // top, recycling buffers so it never blocks on the pool.
        let drainer = std::thread::spawn(move || {
            while let Ok(item) = rx.recv() {
                if let Ok(buf) = item {
                    let _ = recycle_tx.send(buf);
                }
            }
        });
        halt.cancel();
        drop(shell);
        let _ = drainer.join();
    });
}

// Every test that spawns the producer (a Drive holder, §2.5) serialises on
// this, so a holder-count assertion never sees another test's producer.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    super::holder_test_lock()
}

// Wait (bounded) for the live Drive-holder count to reach `n`.
fn holders_settle_to(n: usize, bound: Duration) -> bool {
    let t = std::time::Instant::now();
    while crate::halt::live_drive_holders() != n {
        if t.elapsed() > bound {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    true
}

/// LP7 (§2.5 "Drive-holding threads"): the producer is a Drive holder; a cancel
/// while it is blocked on a full channel ends it within a slice (holders back
/// to 0) even though the consumer keeps the channel open, and drop joins it.
#[test]
fn sector_prefetcher_stop_with_producer_blocked() {
    let _serial = serial();
    let before = crate::halt::live_drive_holders();
    let halt = Halt::new();
    let pf =
        PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, &Ctx::new(halt.clone()))
            .expect("spawn");
    assert_eq!(
        crate::halt::live_drive_holders(),
        before + 1,
        "the producer runs as a Drive holder"
    );
    // Undrained: the forward channel fills and the producer blocks sending.
    std::thread::sleep(Duration::from_millis(100));
    halt.cancel();
    let ended = holders_settle_to(before, Duration::from_secs(1));
    within(10, move || drop(pf));
    assert!(
        ended,
        "a cancel must end the blocked producer within a slice"
    );
    assert_eq!(
        crate::halt::live_drive_holders(),
        before,
        "joined before return"
    );
}

/// LP8 (both Drop sites): after a cancel, dropping the source, or the shell with
/// the channels still held, returns within 1 s. Guard for the direct drop.
#[test]
fn sector_prefetcher_drop_after_cancel_returns() {
    let _serial = serial();
    let halt = Halt::new();
    let pf =
        PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, &Ctx::new(halt.clone()))
            .expect("spawn");
    std::thread::sleep(Duration::from_millis(50));
    halt.cancel();
    within(1, move || drop(pf));

    let halt = Halt::new();
    let pf =
        PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 3, &Ctx::new(halt.clone()))
            .expect("spawn");
    let (rx, recycle_tx, shell) = pf.into_channels();
    std::thread::sleep(Duration::from_millis(50));
    halt.cancel();
    within(1, move || drop(shell));
    drop((rx, recycle_tx));
}

/// LP17 (L096, §2.5 "Halt is not EOF"): once the halt is cancelled, the closed
/// channel is `Err(Halted)`, never the clean-EOF `Ok(0)`; queued batches still
/// drain first. The un-halted close stays `Ok(0)` (below).
#[test]
fn prefetcher_halt_is_not_eof() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let halt = Halt::new();
        let mut pf = PrefetchedSectorSource::new(
            EndlessZeroSource,
            big_extent(),
            3,
            &Ctx::new(halt.clone()),
        )
        .expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        assert_eq!(pf.read_sectors(0, 3, &mut buf, false).unwrap(), 3 * 2048);
        halt.cancel();
        let end = loop {
            match pf.read_sectors(0, 3, &mut buf, false) {
                Ok(n) if n > 0 => continue,
                other => break other,
            }
        };
        let err = end.expect_err("a stop mid-title must not read as a short, complete source");
        assert!(matches!(err, crate::error::Error::Halted), "{err:?}");
    });
}

/// LP17, the other half: a producer that finishes un-halted still ends in `Ok(0)`.
#[test]
fn prefetcher_unhalted_close_is_eof() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let ext = vec![Extent {
            start_lba: 0,
            sector_count: 6,
        }];
        let halt = Halt::new();
        let mut pf =
            PrefetchedSectorSource::new(EndlessZeroSource, ext, 3, &Ctx::new(halt)).expect("spawn");
        let (_, last) = drain_direct(&mut pf, 3, 8);
        assert_eq!(last.unwrap(), 0);
    });
}

/// `batch_sectors == 0` is rejected rather than spawning a thread
/// that spins forever emitting empty batches.
#[test]
fn zero_batch_rejected() {
    let _serial = serial();
    let res = PrefetchedSectorSource::new(EndlessZeroSource, big_extent(), 0, &Ctx::default());
    assert_eq!(res.err().map(|e| e.code()), Some(9085));
}

// A batch below one unit is clamped up to a unit, never splitting an AACS unit.
#[test]
fn sub_unit_batch_is_clamped_to_one_unit() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        for batch in [1u16, 2] {
            let ext = vec![Extent {
                start_lba: 0,
                sector_count: 6,
            }];
            let mut pf = PrefetchedSectorSource::new(
                PatternSource { capacity: 6 },
                ext,
                batch,
                &Ctx::default(),
            )
            .expect("spawn");
            let mut buf = vec![0u8; 3 * 2048];
            for _ in 0..2 {
                let n = pf.read_sectors(0, 3, &mut buf, false).unwrap();
                assert_eq!(n, 3 * 2048, "batch {batch} must read a whole unit");
            }
            assert_eq!(pf.read_sectors(0, 3, &mut buf, false).unwrap(), 0);
        }
    });
}

// Prefetched reads are producer-ordered, so the KU §2.4 layering gate must see `false`
// through the by-reference wrapper too.
#[test]
fn random_access_is_false() {
    let _serial = serial();
    let ext = vec![Extent {
        start_lba: 0,
        sector_count: 6,
    }];
    let mut pf = PrefetchedSectorSource::new(EndlessZeroSource, ext, 3, &Ctx::default()).unwrap();
    assert!(!pf.random_access());
    let by_ref: &mut dyn SectorSource = &mut pf;
    assert!(!SectorSource::random_access(&by_ref));
}

// `unit_align == 0` must be rejected by the constructor, not a producer-thread
// divide-by-zero panic (`remaining % 0`) misreported as DemuxThreadPanicked.
#[test]
fn zero_unit_align_rejected() {
    let _serial = serial();
    let res = PrefetchedSectorSource::with_alignment(
        EndlessZeroSource,
        big_extent(),
        4096,
        0,
        &Ctx::default(),
    );
    let Err(crate::error::Error::IoError { source }) = res else {
        panic!("zero unit_align must be rejected with InvalidInput");
    };
    assert_eq!(source.kind(), std::io::ErrorKind::InvalidInput);
}

// More than 3 sequential direct `read_sectors` calls must succeed: the
// recycle pool seeds 3 buffers; before the fix each drained buffer was
// dropped instead of recycled, so the 4th call deadlocked.
#[test]
fn direct_reads_past_pool_depth_do_not_deadlock() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // 24 sectors = 8 aligned units; batch of 3 sectors gives 8
        // sequential batches, well past the 3-buffer pool depth.
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 24,
        }];
        let src = PatternSource { capacity: 24 };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");

        let mut buf = vec![0u8; 3 * 2048];
        let mut total = 0usize;
        for _ in 0..16 {
            let n = pf.read_sectors(0, 3, &mut buf, false).unwrap();
            if n == 0 {
                break; // EOF
            }
            total += n;
        }
        assert_eq!(total, 24 * 2048, "all 24 sectors should be drained");
    });
}

// The producer thread must emit a `BytesRead` event per batch, cumulative and
// non-decreasing, reaching the full extent size at EOF — the contract autorip's progress
// bar + stall watchdog depend on.
#[test]
fn producer_emits_bytes_read_per_batch() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // Two 12-sector extents = 8 aligned units; batch of 3 gives 8 batches.
        let extents = vec![
            Extent {
                start_lba: 0,
                sector_count: 12,
            },
            Extent {
                start_lba: 12,
                sector_count: 12,
            },
        ];
        let src = PatternSource { capacity: 24 };

        let seen = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
        let seen_cb = seen.clone();
        let ctx = Ctx::default().with_events(Arc::new(move |ev: &Event<'_>| {
            if let Event::BytesRead { bytes, total } = *ev {
                seen_cb.lock().unwrap().push((bytes, total));
            }
        }));

        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &ctx).expect("spawn");

        let mut buf = vec![0u8; 3 * 2048];
        let mut total = 0usize;
        for _ in 0..16 {
            let n = pf.read_sectors(0, 3, &mut buf, false).unwrap();
            if n == 0 {
                break; // EOF
            }
            total += n;
        }
        assert_eq!(total, 24 * 2048, "all 24 sectors drained");

        // One event per batch, each cumulative, all carrying the summed extent total.
        let events = seen.lock().unwrap().clone();
        let want: Vec<(u64, u64)> = (1..=8u64).map(|k| (k * 3 * 2048, 24 * 2048)).collect();
        assert_eq!(events, want);
    });
}

// An extent whose sector_count is not a multiple of 3 delivers its whole units, then
// its sub-unit tail as one short batch, then EOF. Judging the tail (clear passes, an
// encrypted fragment is refused) is the decrypt stage's job, not the reader's.
#[test]
fn non_multiple_of_three_extent_reads_its_tail() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // 8 sectors = 2 full units (6 sectors) + 2 leftover.
        let extents = vec![Extent {
            start_lba: 100,
            sector_count: 8,
        }];
        let src = PatternSource { capacity: 200 };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        let n0 = pf.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n0, 3 * 2048);
        let n1 = pf.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n1, 3 * 2048);
        let n2 = pf.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n2, 2 * 2048, "the 2-sector tail is one batch");
        assert_eq!(buf[0], 106, "the tail starts at lba 106");
        assert_eq!(pf.read_sectors(0, 3, &mut buf, false).unwrap(), 0, "EOF");
    });
}

// A short read must advance the extent cursor by the sectors actually
// read, not the requested count, or bytes are silently skipped. We
// verify every sector of the extent is delivered.
#[test]
fn short_read_does_not_desync_stream() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // 9 sectors = 3 full units. batch of 9 means the first
        // request is for 9 sectors; ShortFirstSource hands back
        // only 3, so the producer must re-request the remaining 6.
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 9,
        }];
        let src = ShortFirstSource {
            capacity: 9,
            first: true,
        };
        let mut pf = PrefetchedSectorSource::new(src, extents, 9, &Ctx::default()).expect("spawn");

        let mut buf = vec![0u8; 9 * 2048];
        let mut total = 0usize;
        for _ in 0..16 {
            let n = pf.read_sectors(0, 9, &mut buf, false).unwrap();
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(
            total,
            9 * 2048,
            "short read must not drop sectors; all 9 must be delivered"
        );
    });
}

// ---------------------------------------------------------------
// Additional coverage below.
// ---------------------------------------------------------------

use std::sync::{Arc, Mutex};

/// Records every (lba, count) the producer issued, in order, and
/// always satisfies the full request. Lets a test assert the exact
/// read schedule (LBA walk, batch sizing, unit trimming).
struct RecordingSource {
    capacity: u32,
    calls: Arc<Mutex<Vec<(u32, u16)>>>,
}
impl SectorSource for RecordingSource {
    fn capacity_sectors(&self) -> u32 {
        self.capacity
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        self.calls.lock().unwrap().push((lba, count));
        let bytes = count as usize * 2048;
        buf[..bytes].fill((lba & 0xff) as u8);
        Ok(bytes)
    }
}

/// Always returns a typed I/O error on the first read. Verifies the
/// producer forwards the underlying error verbatim through the
/// channel instead of swallowing it / treating it as EOF.
struct ErrorSource;
impl SectorSource for ErrorSource {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        _buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        Err(crate::error::Error::IoError {
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        })
    }
}

/// Returns a byte count that is NOT a whole number of sectors
/// (n % 2048 != 0). The producer must reject this as a split-sector
/// short read rather than truncate-and-advance into a partial unit.
struct PartialSectorSource;
impl SectorSource for PartialSectorSource {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        // One sector plus 100 bytes — never a multiple of 2048.
        let n = 2048 + 100;
        buf[..n].fill(0xab);
        Ok(n)
    }
}

/// Drains a prefetch source via the direct `read_sectors` API into a
/// single contiguous Vec, stopping at the first EOF (Ok(0)) or the
/// first error. Returns (bytes, last_result).
fn drain_direct(
    pf: &mut PrefetchedSectorSource,
    buf_sectors: u16,
    max_iters: usize,
) -> (Vec<u8>, Result<usize>) {
    let mut buf = vec![0u8; buf_sectors as usize * 2048];
    let mut out = Vec::new();
    let mut last: Result<usize> = Ok(0);
    for _ in 0..max_iters {
        let r = pf.read_sectors(0, buf_sectors, &mut buf, false);
        match r {
            Ok(0) => {
                last = Ok(0);
                break;
            }
            Ok(n) => {
                out.extend_from_slice(&buf[..n]);
                last = Ok(n);
            }
            Err(e) => {
                last = Err(e);
                break;
            }
        }
    }
    (out, last)
}

/// `capacity_sectors` returns the sum of all extents' sector_counts,
/// computed once at construction. Grounding: doc comment on
/// `total_sectors` — "the sum of each extent's sector_count".
#[test]
fn capacity_sectors_sums_all_extents() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![
            Extent {
                start_lba: 0,
                sector_count: 9,
            },
            Extent {
                start_lba: 100,
                sector_count: 6,
            },
            Extent {
                start_lba: 500,
                sector_count: 3,
            },
        ];
        let src = PatternSource { capacity: 9999 };
        let pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        // 9 + 6 + 3 = 18, independent of inner source capacity.
        assert_eq!(pf.capacity_sectors(), 18);
        // Release without draining via the production zero-copy path: peel
        // channels and drop them so the producer observes disconnection
        // (a direct `drop(pf)` is also safe; see the test below).
        let (rx, recycle_tx, shell) = pf.into_channels();
        drop(rx);
        drop(recycle_tx);
        drop(shell);
    });
}

// Total-sector accumulation must clamp at u32::MAX rather than panic
// (debug overflow) or wrap (release) on a hostile extent set whose
// summed sector_count exceeds u32.
#[test]
fn capacity_sectors_clamps_on_overflow() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![
            Extent {
                start_lba: 0,
                sector_count: u32::MAX,
            },
            Extent {
                start_lba: 0,
                sector_count: u32::MAX,
            },
        ];
        // batch=3 so the producer makes forward progress on the
        // EndlessZeroSource; we only care about the construction-time
        // capacity computation here, then we drop to join.
        let pf = PrefetchedSectorSource::new(EndlessZeroSource, extents, 3, &Ctx::default())
            .expect("spawn");
        assert_eq!(
            pf.capacity_sectors(),
            u32::MAX,
            "summed total must saturate at u32::MAX, not wrap"
        );
        // Release the producer via into_channels + drop, the production
        // zero-copy path. (A direct `drop(pf)` also joins cleanly now —
        // see `drop_undrained_source_joins_cleanly`.)
        let (rx, recycle_tx, shell) = pf.into_channels();
        drop(rx);
        drop(recycle_tx);
        drop(shell);
    });
}

// The producer must walk extents in list order and start each extent
// at its `start_lba` (plus running offset within it), never
// reorder/merge them.
#[test]
fn producer_walks_extents_in_order_at_correct_lbas() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let extents = vec![
            Extent {
                start_lba: 1000,
                sector_count: 6, // two 3-sector batches
            },
            Extent {
                start_lba: 50,
                sector_count: 3, // one batch — lower LBA, MUST stay second
            },
        ];
        let src = RecordingSource {
            capacity: 99999,
            calls: calls.clone(),
        };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 3, 16);
        assert_eq!(last.unwrap(), 0, "should reach EOF");
        assert_eq!(got.len(), (6 + 3) * 2048);
        drop(pf);
        let recorded = calls.lock().unwrap().clone();
        // Expect: extent0 at 1000 then 1003 (offset+3), then extent1 at 50.
        assert_eq!(
            recorded,
            vec![(1000, 3), (1003, 3), (50, 3)],
            "extents must be walked in list order at their start_lba+offset"
        );
    });
}

// A batch larger than one unit must be trimmed DOWN to a whole number
// of 3-sector units before issuing the read — never a sub-unit count
// that decrypt would leave partially encrypted. batch=5 -> trimmed to 3.
#[test]
fn batch_trimmed_to_whole_units() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let calls = Arc::new(Mutex::new(Vec::new()));
        // 9 sectors total = three 3-sector units.
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 9,
        }];
        let src = RecordingSource {
            capacity: 9,
            calls: calls.clone(),
        };
        // batch=5: each read must be trimmed to 3 (one unit), so
        // 9 sectors take three reads of 3, never a 5/4-sector read.
        let mut pf = PrefetchedSectorSource::new(src, extents, 5, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 5, 16);
        assert_eq!(last.unwrap(), 0);
        assert_eq!(got.len(), 9 * 2048);
        drop(pf);
        let recorded = calls.lock().unwrap().clone();
        assert!(
            recorded.iter().all(|&(_, c)| c % SECTOR_ALIGNMENT == 0),
            "every issued read must be a whole number of units, got {recorded:?}"
        );
        assert!(
            recorded.iter().all(|&(_, c)| c == 3),
            "batch=5 must trim to one 3-sector unit per read, got {recorded:?}"
        );
    });
}

// An extent whose sector_count IS a multiple of 3 must deliver exactly
// that many sectors and then cleanly EOF (no error on the final
// aligned batch).
#[test]
fn unit_aligned_extent_delivers_all_and_eofs() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // 12 sectors = exactly four 3-sector units.
        let extents = vec![Extent {
            start_lba: 7,
            sector_count: 12,
        }];
        let src = PatternSource { capacity: 100 };
        let mut pf = PrefetchedSectorSource::new(src, extents, 6, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 6, 16);
        assert_eq!(
            last.unwrap(),
            0,
            "unit-aligned extent must EOF cleanly, not error"
        );
        assert_eq!(got.len(), 12 * 2048);
    });
}

// The underlying reader's error must propagate to the consumer as an error (not Ok(0)/EOF),
// with its ErrorKind surviving the channel round-trip (typed, not blanket-wrapped).
#[test]
fn reader_error_propagates_with_kind() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let mut pf =
            PrefetchedSectorSource::new(ErrorSource, extents, 3, &Ctx::default()).expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        let r = pf.read_sectors(0, 3, &mut buf, false);
        let err = r.expect_err("reader error must surface as Err, not EOF");
        let io: std::io::Error = err.into();
        assert_eq!(
            io.kind(),
            std::io::ErrorKind::PermissionDenied,
            "underlying ErrorKind must survive the channel round-trip"
        );
    });
}

// After the producer reports an error, the closed channel that follows is a dead source,
// never a clean end of stream.
#[test]
fn a_producer_error_latches_the_source_as_terminated() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let mut pf =
            PrefetchedSectorSource::new(ErrorSource, extents, 3, &Ctx::default()).expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        pf.read_sectors(0, 3, &mut buf, false)
            .expect_err("the reader's error");
        let again = pf.read_sectors(0, 3, &mut buf, false);
        assert!(
            matches!(&again, Err(e) if e.is_source_terminated()),
            "second read after a producer error: {again:?}"
        );
    });
}

// A reader that panics is an error to the consumer, not a dropped channel read as EOF.
#[test]
fn a_panicking_reader_is_an_error_not_eof() {
    struct Panics;
    impl SectorSource for Panics {
        fn read_sectors(&mut self, _: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            panic!("reader panic (expected by the test)");
        }
        fn capacity_sectors(&self) -> u32 {
            3
        }
    }
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let mut pf =
            PrefetchedSectorSource::new(Panics, extents, 3, &Ctx::default()).expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        let r = pf.read_sectors(0, 3, &mut buf, false);
        assert!(
            matches!(r, Err(crate::error::Error::DemuxThreadPanicked)),
            "{r:?}"
        );
    });
}

// A byte view ends at the file's real length: the zero-padded tail of the last sector
// is cut, and nothing past it is read.
#[test]
fn a_byte_view_is_clipped_to_the_real_length() {
    for len in [3 * 2048 + 700u64, 4 * 2048, 1000] {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 8,
        }];
        let policy = crate::sector::read_stage::ReadPolicy::Image { batch: 2 };
        let view = ByteView {
            prefix: Vec::new(),
            len,
        };
        let (mut pf, _loss) = PrefetchedSectorSource::file_bytes(
            PatternSource { capacity: 16 },
            extents,
            policy,
            &Ctx::default(),
            view,
        )
        .expect("spawn");
        let (got, last) = drain_direct(&mut pf, 2, 16);
        assert!(matches!(last, Ok(0)), "{last:?}");
        assert_eq!(got.len() as u64, len, "len {len}");
    }
}

// An inner source that answers a mid-extent read with `Ok(0)` has quit early: the producer
// must say so, not drop `tx` (which reads as clean EOF and lets `fill_extents` fabricate
// zeros for the rest).
struct QuitsEarlySource;
impl SectorSource for QuitsEarlySource {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        _buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        Ok(0)
    }
    fn capacity_sectors(&self) -> u32 {
        9
    }
}

#[test]
fn inner_source_quitting_early_is_not_reported_as_end_of_stream() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 9,
        }];
        let mut pf = PrefetchedSectorSource::new(QuitsEarlySource, extents, 3, &Ctx::default())
            .expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        let mut last = pf.read_sectors(0, 3, &mut buf, false);
        // Whatever the first answer, no call may ever settle on a clean
        // `Ok(0)`: 9 sectors were promised and none were delivered.
        for _ in 0..4 {
            if last.is_err() {
                break;
            }
            assert_eq!(
                *last.as_ref().unwrap(),
                0,
                "the source delivered no bytes, so nothing can be Ok(n>0)"
            );
            last = pf.read_sectors(0, 3, &mut buf, false);
        }
        let err =
            last.expect_err("an undelivered extent list must surface as an error, not as EOF");
        assert!(
            err.is_source_terminated(),
            "the source is gone for good — retrying or skipping cannot \
                 recover anything; got {err:?}"
        );
    });
}

// A read returning a byte count that is not a whole number of sectors
// must be rejected — never truncated and advanced, which would split
// a sector and hand decrypt a partial unit.
#[test]
fn non_sector_multiple_read_rejected() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 9,
        }];
        let mut pf = PrefetchedSectorSource::new(PartialSectorSource, extents, 3, &Ctx::default())
            .expect("spawn");
        let mut buf = vec![0u8; 3 * 2048];
        let r = pf.read_sectors(0, 3, &mut buf, false);
        let err = r.expect_err("split-sector read must be rejected");
        let io: std::io::Error = err.into();
        assert_eq!(
            io.kind(),
            std::io::ErrorKind::InvalidInput,
            "split-sector read maps to ExtentNotUnitAligned (InvalidInput)"
        );
    });
}

// A too-small consumer buffer in the direct `read_sectors` path must
// error (InvalidInput), never silently drop the bytes past
// `buf.len()` (would desync the stream).
#[test]
fn direct_read_too_small_buffer_errors() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 6,
        }];
        let src = PatternSource { capacity: 6 };
        // batch=3 → producer fills 3 sectors (6144 bytes) per batch.
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        // Caller buffer holds only 1 sector — far too small.
        let mut tiny = vec![0u8; 2048];
        let r = pf.read_sectors(0, 1, &mut tiny, false);
        let err = r.expect_err("too-small buffer must error, not truncate");
        let io: std::io::Error = err.into();
        assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput);
        drop(pf);
    });
}

// Regression: too-small-buffer reads repeated past the pool depth (3) must NOT deadlock.
// Before the fix, each error path skipped recycling the received buffer, draining the pool
// by the 4th call.
#[test]
fn too_small_buffer_repeated_does_not_deadlock_pool() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        // Extent with enough sectors that the producer never reaches
        // EOF during the test — we need it to keep producing batches.
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 30,
        }];
        let src = PatternSource { capacity: 30 };
        // batch=3 → producer fills 3 sectors (6144 bytes) per batch.
        // Pool depth is PREFETCH_CHANNEL_DEPTH+1 = 3.
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        // Caller buffer holds only 1 sector — far too small for a 3-sector batch.
        let mut tiny = vec![0u8; 2048];

        // 8 >> pool depth (3): WITHOUT recycle-on-error the pool drains by
        // the 3rd error and read 4 deadlocks. WITH the fix every error
        // recycles its buffer, so reaching loop end is the regression check.
        for i in 0..8 {
            let r = pf.read_sectors(0, 1, &mut tiny, false);
            assert!(
                r.is_err(),
                "iteration {i}: too-small buffer must return Err, got Ok"
            );
        }
    });
}

// The producer delivers exactly the bytes the inner source produced,
// in order, byte-for-byte — guards against off-by-one/duplicate/reorder
// in the offset bookkeeping.
#[test]
fn delivered_bytes_match_source_exactly() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let start = 40u32;
        let count = 9u32; // three units
        let extents = vec![Extent {
            start_lba: start,
            sector_count: count,
        }];
        let src = PatternSource { capacity: 1000 };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 3, 16);
        assert_eq!(last.unwrap(), 0);
        assert_eq!(got.len(), (count as usize) * 2048);
        // Reconstruct expected: sector i carries byte ((start+i)&0xff).
        for i in 0..count {
            let tag = ((start + i) & 0xff) as u8;
            let off = i as usize * 2048;
            assert!(
                got[off..off + 2048].iter().all(|b| *b == tag),
                "sector {i} (lba {}) content mismatch",
                start + i
            );
        }
    });
}

// An empty extent list must EOF immediately (capacity 0, first direct
// read returns Ok(0)) and must not deadlock: the loop body never runs,
// so `tx` drops and the consumer sees RecvError -> Ok(0).
#[test]
fn empty_extents_eof_immediately() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let pf = PrefetchedSectorSource::new(EndlessZeroSource, Vec::new(), 3, &Ctx::default())
            .expect("spawn");
        assert_eq!(pf.capacity_sectors(), 0);
        let mut pf = pf;
        let mut buf = vec![0u8; 3 * 2048];
        let n = pf.read_sectors(0, 3, &mut buf, false).unwrap();
        assert_eq!(n, 0, "empty extent list must EOF immediately");
    });
}

// A zero-length extent in the middle of the list must be skipped
// (remaining == 0 -> advance to next extent) without emitting a
// batch and without stalling.
#[test]
fn zero_length_extent_is_skipped() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let extents = vec![
            Extent {
                start_lba: 10,
                sector_count: 3,
            },
            Extent {
                start_lba: 20,
                sector_count: 0, // empty — must be skipped
            },
            Extent {
                start_lba: 30,
                sector_count: 3,
            },
        ];
        let src = RecordingSource {
            capacity: 9999,
            calls: calls.clone(),
        };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 3, 16);
        assert_eq!(last.unwrap(), 0);
        assert_eq!(got.len(), 6 * 2048, "two non-empty extents = 6 sectors");
        drop(pf);
        let recorded = calls.lock().unwrap().clone();
        // No read should target LBA 20 (the empty extent).
        assert_eq!(
            recorded,
            vec![(10, 3), (30, 3)],
            "empty extent must produce no read"
        );
    });
}

// A 4-sector extent (one full unit + a 1-sector tail) delivers the 3-sector unit,
// then the 1-sector tail, then EOF: the trim-within-batch path, distinct control flow
// from the 8-sector case.
#[test]
fn four_sector_extent_reads_its_one_sector_tail() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 4,
        }];
        let src = PatternSource { capacity: 100 };
        // batch=9 (>4) so the first iter requests 4, trims to 3.
        let mut pf = PrefetchedSectorSource::new(src, extents, 9, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 9, 8);
        assert_eq!(last.unwrap(), 0, "EOF after the tail");
        assert_eq!(got.len(), 4 * 2048, "every sector of the extent");
    });
}

// Many sequential direct reads across MANY extents must all flow
// through the fixed recycle pool without deadlock — a stronger
// pool-depth regression that also crosses extent boundaries.
#[test]
fn many_extents_drain_without_deadlock() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(15), || {
        // 10 extents of 3 sectors each = 30 sectors total, well past
        // the 3-buffer pool, and 10 extent transitions.
        let extents: Vec<Extent> = (0..10)
            .map(|i| Extent {
                start_lba: i * 1000,
                sector_count: 3,
            })
            .collect();
        let src = PatternSource { capacity: 999999 };
        let mut pf = PrefetchedSectorSource::new(src, extents, 3, &Ctx::default()).expect("spawn");
        let (got, last) = drain_direct(&mut pf, 3, 64);
        assert_eq!(last.unwrap(), 0);
        assert_eq!(got.len(), 30 * 2048, "all 10 extents must be drained");
    });
}

/// Source whose every read fails with a `DiscRead` carrying the given
/// SCSI status (and optional sense) — an ordinary MEDIUM ERROR bad
/// sector (0x02 + 03/11/00) or the transport-failure sentinel (0xFF).
struct FailingSource {
    status: u8,
    sense: Option<crate::scsi::ScsiSense>,
}

impl SectorSource for FailingSource {
    fn capacity_sectors(&self) -> u32 {
        9999
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        _count: u16,
        _buf: &mut [u8],
        _recovery: bool,
    ) -> Result<usize> {
        Err(crate::error::Error::DiscRead {
            sector: lba as u64,
            status: Some(self.status),
            sense: self.sense,
        })
    }
}

fn read_one_err(status: u8, sense: Option<crate::scsi::ScsiSense>) -> crate::error::Error {
    let extents = vec![Extent {
        start_lba: 100,
        sector_count: 9,
    }];
    let mut pf =
        PrefetchedSectorSource::new(FailingSource { status, sense }, extents, 3, &Ctx::default())
            .expect("spawn");
    let mut buf = vec![0u8; 3 * 2048];
    pf.read_sectors(100, 3, &mut buf, false)
        .expect_err("the producer's read failure must surface")
}

// REGRESSION: an ordinary MEDIUM ERROR bad sector crossing the prefetch channel must NOT
// classify as a SCSI transport failure (old blanket `Error::IoError` wrap made it match the
// wedged-USB-bridge arm).
#[test]
fn bad_sector_across_channel_is_not_a_transport_failure() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let sense = crate::scsi::ScsiSense {
            sense_key: 0x03,
            asc: 0x11,
            ascq: 0x00,
        };
        let err = read_one_err(crate::scsi::SCSI_STATUS_CHECK_CONDITION, Some(sense));
        assert!(
            !err.is_scsi_transport_failure(),
            "a MEDIUM ERROR bad sector must stay a bad sector across the \
                 prefetch channel, got {err:?}"
        );
        // The classification survives because the typed variant does.
        assert!(
            matches!(err, crate::error::Error::DiscRead { status: Some(s), .. } if s == 0x02),
            "expected the producer's DiscRead to survive the channel, got {err:?}"
        );
        assert_eq!(
            err.scsi_sense().map(|s| (s.sense_key, s.asc, s.ascq)),
            Some((0x03, 0x11, 0x00)),
            "the drive's sense triple must survive the channel"
        );
    });
}

// OPPOSITE-DIRECTION CONTROL: a genuine transport failure (0xFF, wedged
// USB bridge) crossing the same channel MUST still classify as one, so
// sweep keeps aborting instead of zero-filling against a dead bus.
#[test]
fn transport_failure_across_channel_still_classifies() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let err = read_one_err(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE, None);
        assert!(
            err.is_scsi_transport_failure(),
            "a 0xFF transport failure must remain one across the prefetch \
                 channel, got {err:?}"
        );
    });
}

// ── Read/decrypt overlap ───────────────────────────────────────────────────────────

const OVERLAP_KEY: [u8; 16] = [0x6B; 16];

// One clear BD-TS unit flagged encrypted, as `(plaintext with CPI cleared, ciphertext)`.
fn overlap_unit() -> (Vec<u8>, Vec<u8>) {
    let unit_len = crate::aacs::content::ALIGNED_UNIT_LEN;
    let mut plain: Vec<u8> = (0..unit_len).map(|i| (i % 249) as u8 | 1).collect();
    for p in plain.chunks_mut(crate::consts::BD_SOURCE_PACKET_BYTES) {
        p[0] |= 0xC0;
        p[4] = 0x47;
    }
    let mut cipher = plain.clone();
    assert!(crate::aacs::content::encrypt_unit(
        &mut cipher,
        &OVERLAP_KEY
    ));
    (crate::aacs::content::cpi_cleared(plain), cipher)
}

// Every file (anchored at an `anchors` LBA) repeats one encrypted unit on its own grid.
struct UnitImage {
    unit: Vec<u8>,
    anchors: Vec<u32>,
    fail_at: Option<u32>,
    panic_at: Option<u32>,
}

impl UnitImage {
    fn new(unit: Vec<u8>, anchors: Vec<u32>) -> Self {
        Self {
            unit,
            anchors,
            fail_at: None,
            panic_at: None,
        }
    }
    fn sector<'a>(unit: &'a [u8], anchors: &[u32], lba: u32) -> &'a [u8] {
        let anchor = anchors
            .iter()
            .rev()
            .find(|&&a| a <= lba)
            .copied()
            .unwrap_or(0);
        let k = ((lba - anchor) % 3) as usize * 2048;
        &unit[k..k + 2048]
    }
}

impl SectorSource for UnitImage {
    fn capacity_sectors(&self) -> u32 {
        u32::MAX
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        let end = lba as u64 + count as u64;
        if self
            .panic_at
            .is_some_and(|p| (lba as u64..end).contains(&(p as u64)))
        {
            panic!("reader panic (expected by the test)");
        }
        if let Some(f) = self
            .fail_at
            .filter(|&f| (lba as u64..end).contains(&(f as u64)))
        {
            return Err(crate::error::Error::DiscRead {
                sector: f as u64,
                status: None,
                sense: None,
            });
        }
        for (i, s) in buf[..count as usize * 2048].chunks_mut(2048).enumerate() {
            s.copy_from_slice(Self::sector(&self.unit, &self.anchors, lba + i as u32));
        }
        Ok(count as usize * 2048)
    }
}

// The key set's reader over `img`: each `(start, end)` file in `files` is keyed content,
// and `[start, content_end)` of the first is content too (an unkeyed tail fails loud).
fn overlap_reader(
    img: UnitImage,
    files: &[(u32, u32)],
    content_end: Option<u32>,
) -> crate::whole_disc::UnitAligned<crate::sector::DecryptingSectorSource<UnitImage>> {
    let keys = crate::decrypt::DecryptKeys::Aacs {
        unit_keys: vec![(0, OVERLAP_KEY)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = crate::decrypt::AacsKeyMap::key_zero_over(files);
    let mut content: Vec<(u32, u32)> = files.iter().map(|&(s, e)| (s, e - s)).collect();
    if let Some(end) = content_end {
        content[0].1 = end - content[0].0;
    }
    let spans = files.iter().map(|&(s, e)| (s, e - s, s as u64)).collect();
    let keying = crate::sector::Keying::ring(keys, Arc::new(map), None);
    let dec = crate::sector::DecryptingSectorSource::new(img, keying)
        .with_content_ranges(Arc::from(content));
    crate::whole_disc::UnitAligned::new(dec, spans)
}

fn stage_runs() -> usize {
    crate::sector::deferred::STAGE_RUNS.load(std::sync::atomic::Ordering::Relaxed)
}

// The decrypt stage's output is byte for byte the serial reader's, across two files on
// different unit grids and batch sizes that split them every way.
#[test]
fn overlapped_decrypt_is_byte_identical_to_the_serial_read() {
    let _serial = serial();
    let (plain, cipher) = overlap_unit();
    let files = [(0u32, 60u32), (61, 151)];
    let extents: Vec<Extent> = files
        .iter()
        .map(|&(s, e)| Extent {
            start_lba: s,
            sector_count: e - s,
        })
        .collect();
    let mut want = Vec::new();
    for &(s, e) in &files {
        for lba in s..e {
            want.extend_from_slice(UnitImage::sector(&plain, &[0, 61], lba));
        }
    }
    let mut serial_reader =
        overlap_reader(UnitImage::new(cipher.clone(), vec![0, 61]), &files, None);
    let mut serial_out = Vec::new();
    for &(s, e) in &files {
        let mut buf = vec![0u8; (e - s) as usize * 2048];
        serial_reader
            .read_sectors(s, (e - s) as u16, &mut buf, false)
            .unwrap();
        serial_out.extend_from_slice(&buf);
    }
    assert!(serial_out == want, "the serial reader decrypts");
    for batch in [3u16, 15, 31, 64, 90] {
        let before = stage_runs();
        let reader = overlap_reader(UnitImage::new(cipher.clone(), vec![0, 61]), &files, None);
        let mut pf =
            PrefetchedSectorSource::new(reader, extents.clone(), batch, &Ctx::default()).unwrap();
        let (got, last) = drain_direct(&mut pf, batch.max(3), 1000);
        assert!(matches!(last, Ok(0)), "batch {batch}: {last:?}");
        assert!(got == want, "batch {batch}: overlapped output differs");
        assert!(
            stage_runs() > before,
            "batch {batch}: the decrypt stage ran"
        );
    }
}

// A unit decrypt that fails in the stage arrives after every earlier batch, in order, and
// ends the stream: no later batch, then the latched dead source.
#[test]
fn a_stage_decrypt_failure_arrives_in_order_and_ends_the_stream() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let (plain, cipher) = overlap_unit();
        let reader = overlap_reader(UnitImage::new(cipher, vec![0]), &[(0, 30)], Some(60));
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 60,
        }];
        let mut pf = PrefetchedSectorSource::new(reader, extents, 15, &Ctx::default()).unwrap();
        let mut buf = vec![0u8; 15 * 2048];
        for b in 0..2u32 {
            assert_eq!(pf.read_sectors(0, 15, &mut buf, false).unwrap(), buf.len());
            let want: Vec<u8> = (b * 15..b * 15 + 15)
                .flat_map(|l| UnitImage::sector(&plain, &[0], l).to_vec())
                .collect();
            assert!(buf == want, "batch {b} decrypted");
        }
        let e = pf.read_sectors(0, 15, &mut buf, false).unwrap_err();
        assert_eq!(e.code(), crate::error::Error::DecryptFailed.code());
        let after = pf.read_sectors(0, 15, &mut buf, false).unwrap_err();
        assert!(
            matches!(after, crate::error::Error::SourceTerminated),
            "{after:?}"
        );
    });
}

// A read error behind deferred batches is reported after them, never before.
#[test]
fn a_read_error_follows_the_batches_still_decrypting() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let (plain, cipher) = overlap_unit();
        let mut img = UnitImage::new(cipher, vec![0]);
        img.fail_at = Some(33);
        let reader = overlap_reader(img, &[(0, 60)], None);
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 60,
        }];
        let mut pf = PrefetchedSectorSource::new(reader, extents, 15, &Ctx::default()).unwrap();
        let (got, last) = drain_direct(&mut pf, 15, 10);
        let want: Vec<u8> = (0..30)
            .flat_map(|l| UnitImage::sector(&plain, &[0], l).to_vec())
            .collect();
        assert!(got == want, "both batches before the error, decrypted");
        let e = last.unwrap_err();
        assert!(
            matches!(e, crate::error::Error::DiscRead { sector: 33, .. }),
            "{e:?}"
        );
    });
}

// A reader panic behind a deferred batch: the batch is delivered, then the panic error.
#[test]
fn a_reader_panic_with_the_stage_running_is_an_error_after_earlier_batches() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let (_, cipher) = overlap_unit();
        let mut img = UnitImage::new(cipher, vec![0]);
        img.panic_at = Some(31);
        let reader = overlap_reader(img, &[(0, 60)], None);
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 60,
        }];
        let mut pf = PrefetchedSectorSource::new(reader, extents, 15, &Ctx::default()).unwrap();
        let (got, last) = drain_direct(&mut pf, 15, 10);
        assert_eq!(got.len(), 30 * 2048);
        assert!(
            matches!(last, Err(crate::error::Error::DemuxThreadPanicked)),
            "{last:?}"
        );
    });
}

// A cancel while the stage and the producer are both blocked ends them within a slice.
#[test]
fn cancel_during_overlap_ends_producer_and_stage() {
    let _serial = serial();
    let before = crate::halt::live_drive_holders();
    let (_, cipher) = overlap_unit();
    let reader = overlap_reader(UnitImage::new(cipher, vec![0]), &[(0, 3_000_000)], None);
    let halt = Halt::new();
    let pf = PrefetchedSectorSource::new(reader, big_extent(), 30, &Ctx::new(halt.clone()))
        .expect("spawn");
    // Undrained: the channel fills, the stage blocks sending, the producer handing over.
    std::thread::sleep(Duration::from_millis(100));
    halt.cancel();
    let ended = holders_settle_to(before, Duration::from_secs(1));
    within(10, move || drop(pf));
    assert!(ended, "a cancel must end the producer and its stage");
}

// A consumer that walks away mid-stream releases the producer and its stage.
#[test]
fn consumer_drop_during_overlap_joins_cleanly() {
    let _serial = serial();
    within(10, || {
        let (_, cipher) = overlap_unit();
        let reader = overlap_reader(UnitImage::new(cipher, vec![0]), &[(0, 3_000_000)], None);
        let pf =
            PrefetchedSectorSource::new(reader, big_extent(), 30, &Ctx::default()).expect("spawn");
        let (rx, recycle, shell) = pf.into_channels();
        for _ in 0..4 {
            let buf = rx.recv().expect("batch").expect("ok");
            let _ = recycle.send(buf);
        }
        drop((rx, recycle));
        drop(shell);
    });
}

// A live drive's recovering reads judge each decrypt in the read: no stage, ever.
#[test]
fn live_reads_never_defer_their_decrypt() {
    let _serial = serial();
    with_watchdog(Duration::from_secs(10), || {
        let (plain, cipher) = overlap_unit();
        let reader = overlap_reader(UnitImage::new(cipher, vec![0]), &[(0, 60)], None);
        let extents = vec![Extent {
            start_lba: 0,
            sector_count: 60,
        }];
        let before = stage_runs();
        let policy = crate::sector::read_stage::ReadPolicy::Live {
            batch: 15,
            skip_errors: false,
        };
        let (mut pf, _) =
            PrefetchedSectorSource::with_policy(reader, extents, policy, 3, &Ctx::default())
                .unwrap();
        let (got, last) = drain_direct(&mut pf, 15, 10);
        assert!(matches!(last, Ok(0)), "{last:?}");
        let want: Vec<u8> = (0..60)
            .flat_map(|l| UnitImage::sector(&plain, &[0], l).to_vec())
            .collect();
        assert!(got == want);
        assert_eq!(stage_runs(), before, "no deferred decrypt");
    });
}

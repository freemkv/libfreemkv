//! Stop design §5.1 LP13a–h, LP19 and §5.9 SP10–SP13, G19: the durable flush (T12) on
//! a scaled clock through [`FakeFlushOps`] (a 60 s window runs as [`W`]); the real
//! syscalls run on the runner's tmpdir. The windows are 1 s and the upper bounds 2 s, so a
//! 200 ms sleep overshoot stays inside them.

use super::*;
use crate::io::WritebackFile;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::time::Instant;

const W: Duration = Duration::from_secs(1);
const SLACK: Duration = Duration::from_secs(2);
const K: u64 = 1024;

// Blocks callers while closed; `open` releases them.
#[derive(Clone)]
struct Gate(Arc<AtomicBool>);

impl Gate {
    fn open() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
    fn closed() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    fn release(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn pass(&self) {
        while !self.0.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

// Range-piece sleep by call index (LP19).
type PieceDelay = Box<dyn Fn(usize) -> Duration + Send + Sync>;

/// The `FakeFlushOps` seam (§5.1 LP13): each primitive sleeps and/or waits on a gate,
/// counts its calls, and `sample` rises with wall time when enabled.
struct FakeFlushOps {
    chunk_sleep: Duration,
    // Extra chunk sleep by call index (LP13f), and a device writing out at this many
    // bytes per second of wall time (the throttled-device pair).
    chunk_delay: Option<PieceDelay>,
    rate: Option<u64>,
    flushed_len: AtomicU64,
    chunk_gate: Gate,
    finish_sleep: Duration,
    finish_gate: Gate,
    started: AtomicUsize,
    chunks: AtomicUsize,
    // Each chunk call's entry and exit instants.
    spans: Mutex<Vec<(Instant, Instant)>>,
    finishes: AtomicUsize,
    sample_from: Option<Instant>,
    samples: AtomicUsize,
    pieces: Option<PieceDelay>,
    // Each range call's (offset, length, entry instant).
    ranges: Mutex<Vec<(u64, u64, Instant)>>,
}

impl Default for FakeFlushOps {
    fn default() -> Self {
        Self {
            chunk_sleep: Duration::ZERO,
            chunk_delay: None,
            rate: None,
            flushed_len: AtomicU64::new(0),
            chunk_gate: Gate::open(),
            finish_sleep: Duration::ZERO,
            finish_gate: Gate::open(),
            started: AtomicUsize::new(0),
            chunks: AtomicUsize::new(0),
            spans: Mutex::new(Vec::new()),
            finishes: AtomicUsize::new(0),
            sample_from: None,
            samples: AtomicUsize::new(0),
            pieces: None,
            ranges: Mutex::new(Vec::new()),
        }
    }
}

impl FlushOps for FakeFlushOps {
    fn chunk(&self, file: &File) -> io::Result<()> {
        let entered = Instant::now();
        self.started.fetch_add(1, Ordering::SeqCst);
        self.chunk_gate.pass();
        std::thread::sleep(self.chunk_sleep);
        let i = self.chunks.load(Ordering::SeqCst);
        if let Some(d) = &self.chunk_delay {
            std::thread::sleep(d(i));
        }
        if let Some(rate) = self.rate {
            // The dirty bytes since the last chunk, written out at `rate`.
            let len = file.metadata()?.len();
            let dirty = len.saturating_sub(self.flushed_len.swap(len, Ordering::SeqCst));
            std::thread::sleep(Duration::from_secs_f64(dirty as f64 / rate as f64));
        }
        self.spans.lock().unwrap().push((entered, Instant::now()));
        self.chunks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn range(&self, _file: &File, off: u64, len: u64) -> Option<io::Result<()>> {
        let delay = self.pieces.as_ref()?;
        let i = {
            let mut r = self.ranges.lock().unwrap();
            r.push((off, len, Instant::now()));
            r.len() - 1
        };
        std::thread::sleep(delay(i));
        Some(Ok(()))
    }
    fn finish(&self, _file: &File) -> io::Result<()> {
        self.finish_gate.pass();
        std::thread::sleep(self.finish_sleep);
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn sample(&self) -> Option<u64> {
        self.samples.fetch_add(1, Ordering::SeqCst);
        self.sample_from.map(|t| t.elapsed().as_millis() as u64)
    }
}

fn timing(stall: Duration) -> FlushTiming {
    FlushTiming {
        stall,
        slow_chunk: Duration::from_secs(10),
        chunk_min: K,
        chunk_max: K,
        sample_every: W / 4,
    }
}

fn fake_file(
    ops: &Arc<FakeFlushOps>,
    t: FlushTiming,
) -> (tempfile::TempDir, WritebackFile, FlushProgress) {
    let dir = tempfile::tempdir().unwrap();
    let file = File::create(dir.path().join("out.bin")).unwrap();
    let ops: Arc<dyn FlushOps> = ops.clone();
    let mut w = WritebackFile::with_flush_ops(file, ops, t).unwrap();
    let flush = FlushProgress::new(Liveness::new());
    w.set_flush_progress(flush.clone());
    (dir, w, flush)
}

fn is_sync_timeout(e: &io::Error) -> bool {
    crate::error::error_code(e) == Some(crate::error::E_SYNC_TIMEOUT)
}

fn cancel_after(halt: &Halt, d: Duration) {
    let h = halt.clone();
    std::thread::spawn(move || {
        std::thread::sleep(d);
        h.cancel();
    });
}

/// LP13a (T12, stall pair a; Q-B "if writing keep going do nothing"): a final flush
/// waits on two chunk flushes of 0.55 × window each; the first completion re-arms the
/// stall window, so a wait longer than the window succeeds.
#[test]
fn flush_slow_but_progressing_never_fails() {
    let ops = Arc::new(FakeFlushOps {
        chunk_sleep: W * 11 / 20,
        chunk_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let t = FlushTiming {
        sample_every: Duration::from_millis(5),
        ..timing(W)
    };
    let (_d, mut w, _) = fake_file(&ops, t);
    // Chunk 0 is in flight (held at the gate) before the second C is requested, so the
    // drain needs a second chunk; the gate opens once the drain is waiting (it samples).
    w.write_all(&[7u8; K as usize]).unwrap();
    while ops.started.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    w.write_all(&[7u8; K as usize]).unwrap();
    let o = ops.clone();
    std::thread::spawn(move || {
        while o.samples.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        o.chunk_gate.release();
    });
    let t = Instant::now();
    let r = w.sync_all();
    let end = Instant::now();
    while ops.spans.lock().unwrap().len() < ops.started.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    // Chunk i's progress bump lies between its exit and chunk i + 1's entry, so this bounds
    // each no-progress gap the drain saw up to `end`. Only a whole window (a descheduled
    // worker under load) may latch SyncTimeout; anything shorter must succeed.
    let spans = ops.spans.lock().unwrap().clone();
    let lows = std::iter::once(t).chain(spans.iter().map(|s| s.1));
    let highs = spans.iter().skip(1).map(|s| s.0.min(end)).chain([end]);
    let gap = lows
        .zip(highs)
        .map(|(a, b)| b.saturating_duration_since(a))
        .max()
        .unwrap();
    if let Err(e) = &r {
        assert!(is_sync_timeout(e) && gap >= W, "{e}; gaps under {gap:?}");
        eprintln!("inconclusive: the worker made no progress for {gap:?} under load");
        return;
    }
    assert_eq!(spans.len(), 2, "two chunk flushes");
    assert!(end - t > W, "the wait outlived the window: {:?}", end - t);
}

/// LP13b / G19 (T12, stall pair b): a chunk flush that never returns, with no sampled
/// signal, fails `SyncTimeout` (E9056) at the window (+ ≤ 1 s) and not long before.
/// Guard: E9056 is still returned on a true stall (§5.9 G19).
#[test]
fn flush_stall_60s_fails_e9056() {
    let ops = Arc::new(FakeFlushOps {
        chunk_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, timing(W));
    w.write_all(&[1u8; K as usize]).unwrap();
    let t = Instant::now();
    let e = w.sync_all().expect_err("a stalled flush fails");
    let took = t.elapsed();
    ops.chunk_gate.release();
    assert!(is_sync_timeout(&e), "{e}");
    assert!(took >= W / 2, "fired early: {took:?}");
    assert!(took <= W + SLACK, "fired late: {took:?}");
}

/// LP13c: a cancel during a stalled final flush, and during a writer's backpressure
/// wait, returns `Halted` within 1 s; the flush worker is leaked, never joined.
#[test]
fn flush_stop_interrupts() {
    let long = timing(Duration::from_secs(30));
    let ops = Arc::new(FakeFlushOps {
        finish_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, long);
    let halt = Halt::new();
    w.set_halt(halt.clone());
    w.write_all(b"tail").unwrap();
    cancel_after(&halt, Duration::from_millis(50));
    let t = Instant::now();
    let e = w.sync_all().expect_err("a stop ends the final flush wait");
    ops.finish_gate.release();
    assert!(crate::error::is_halt(&e), "{e}");
    assert!(t.elapsed() < SLACK, "{:?}", t.elapsed());

    let ops = Arc::new(FakeFlushOps {
        chunk_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, long);
    let halt = Halt::new();
    w.set_halt(halt.clone());
    for _ in 0..3 {
        w.write_all(&[2u8; K as usize]).unwrap();
    }
    cancel_after(&halt, Duration::from_millis(50));
    let t = Instant::now();
    let e = w
        .write_all(&[2u8; K as usize])
        .expect_err("a stop ends the backpressure wait");
    ops.chunk_gate.release();
    assert!(crate::error::is_halt(&e), "{e}");
    assert!(t.elapsed() < SLACK, "{:?}", t.elapsed());
}

/// LP13d (Linux NFS signal): one flush blocked for 3 × window while the sampled
/// `mountstats` counter rises is progress, not a stall; the flush then succeeds.
#[test]
fn nfs_mountstats_signal_rearms() {
    let ops = Arc::new(FakeFlushOps {
        chunk_sleep: W * 3,
        sample_from: Some(Instant::now()),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, timing(W));
    w.write_all(&[3u8; K as usize]).unwrap();
    let t = Instant::now();
    w.sync_all()
        .expect("a rising mountstats counter is progress");
    assert!(ops.chunks.load(Ordering::SeqCst) >= 1);
    assert!(t.elapsed() >= W * 2, "the blocked flush was waited for");
}

/// Design review item 3: once a Stop is pending, a close on NFS counts only this file's
/// own chunk completions; other writers' `mountstats` counters no longer re-arm the wait,
/// so a blocked close fails E9056 at the window instead of running on unbounded.
#[test]
fn stop_pending_close_ignores_mountstats() {
    let gate = Gate::closed();
    let ops = Arc::new(FakeFlushOps {
        chunk_gate: gate.clone(),
        sample_from: Some(Instant::now()),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, timing(W));
    let halt = Halt::new();
    w.set_halt(halt.clone());
    w.write_all(&[3u8; K as usize]).unwrap();
    halt.cancel();
    let g = gate.clone();
    std::thread::spawn(move || {
        std::thread::sleep(W * 6);
        g.release();
    });
    let t = Instant::now();
    let e = w
        .flush()
        .expect_err("no own progress for a window after the Stop");
    assert!(is_sync_timeout(&e), "{e}");
    assert!(t.elapsed() <= W + SLACK / 2, "bounded: {:?}", t.elapsed());
}

/// LP13e (§2.10 item 2): the writer blocks once written − flushed exceeds 2 × C, and
/// resumes when a chunk flush completes.
#[test]
fn writer_backpressure_at_two_chunks() {
    let ops = Arc::new(FakeFlushOps {
        chunk_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, timing(Duration::from_secs(30)));
    let done = Arc::new(AtomicUsize::new(0));
    let d2 = done.clone();
    let writer = std::thread::spawn(move || {
        for _ in 0..4 {
            w.write_all(&[4u8; K as usize]).unwrap();
            d2.fetch_add(1, Ordering::SeqCst);
        }
        w
    });
    // Only a writer blocked in the backpressure wait samples: that, not a sleep, says the
    // 4th write is waiting. Without backpressure it completes instead.
    while ops.samples.load(Ordering::SeqCst) == 0 && done.load(Ordering::SeqCst) < 4 {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        done.load(Ordering::SeqCst),
        3,
        "the 4th write waits at 2 × C unflushed"
    );
    ops.chunk_gate.release();
    // Unreleased, the writer latches SyncTimeout at its 30 s window and the join fails.
    let mut w = writer
        .join()
        .expect("a completed flush releases the writer");
    assert_eq!(done.load(Ordering::SeqCst), 4);
    w.sync_all().unwrap();
}

/// LP13f (§2.10 item 1, as revised): C starts at the floor, doubles after fast chunk
/// flushes up to the cap, and halves after one slower than the slow bound (15 s, scaled),
/// never below the floor.
#[test]
fn chunk_halves_on_slow_flush() {
    let ops = Arc::new(FakeFlushOps {
        chunk_delay: Some(Box::new(|i| {
            if i >= 2 {
                Duration::from_millis(1600)
            } else {
                Duration::ZERO
            }
        })),
        ..FakeFlushOps::default()
    });
    let t = FlushTiming {
        slow_chunk: Duration::from_millis(800),
        chunk_min: K,
        chunk_max: 4 * K,
        ..timing(Duration::from_secs(30))
    };
    let (_d, mut w, _) = fake_file(&ops, t);
    assert_eq!(w.chunk_bytes(), K, "starts at the floor");
    let mut peak = K;
    for _ in 0..40 {
        w.write_all(&[5u8; K as usize]).unwrap();
        peak = peak.max(w.chunk_bytes());
    }
    w.sync_all().unwrap();
    assert_eq!(peak, 4 * K, "fast flushes grew C to the cap, not past it");
    assert_eq!(
        w.chunk_bytes(),
        K,
        "slow flushes halved it to the floor, not below"
    );
}

// ── The slow-device pair (design review: C starts at the floor) ──

/// A virtual-clock model of the flusher (§2.10 items 1–3) at production sizes: a writer
/// at `writer_bps` in 64 KiB writes, a device at `device_bps` until `dies_at`, `next_size`
/// for C. Returns the longest time with no progress (an accepted write or a completed chunk).
fn simulate(total: u64, writer_bps: f64, device_bps: f64, dies_at: f64) -> f64 {
    let t = FlushTiming::default();
    let (mut clock, mut last_progress, mut worst) = (0.0f64, 0.0f64, 0.0f64);
    let (mut written, mut requested, mut flushed, mut chunk) = (0u64, 0u64, 0u64, t.chunk_min);
    let mut busy: Option<(f64, u64, f64)> = None; // (done_at, target, started)
    let write = 64 * K;
    let mut next_write = 0.0f64;
    loop {
        let draining = written >= total;
        if draining && requested < written {
            requested = written;
        }
        if busy.is_none() && requested > flushed {
            let secs = (requested - flushed) as f64 / device_bps;
            let done = if clock + secs > dies_at {
                f64::INFINITY
            } else {
                clock + secs
            };
            busy = Some((done, requested, clock));
        }
        if draining && flushed >= written {
            return worst;
        }
        let room = written.saturating_sub(flushed) <= 2 * chunk;
        let wake_w = if !draining && room {
            next_write
        } else {
            f64::INFINITY
        };
        let wake_f = busy.map_or(f64::INFINITY, |b| b.0);
        let now = wake_w.min(wake_f);
        if now.is_infinite() {
            return f64::INFINITY; // the device died: nothing ever moves again
        }
        clock = now;
        if wake_f <= wake_w {
            let (_, target, started) = busy.take().unwrap();
            flushed = target;
            let took = Duration::from_secs_f64(clock - started);
            chunk = next_size(chunk, took, t.slow_chunk, t.chunk_min, t.chunk_max);
        } else {
            written += write;
            next_write = clock + write as f64 / writer_bps;
            if written - requested >= chunk {
                requested = written;
            }
        }
        worst = worst.max(clock - last_progress);
        last_progress = clock;
    }
}

/// Design review item 1 (fake clock, production sizes): a writer and a device both at
/// ~100 KiB/s over 512 MiB never go a whole 60 s without progress, so no false
/// `SyncTimeout`; with the old 64 MiB first chunk the same run went minutes without one.
/// A device that dies mid-run still leaves only silence, which T12 times out.
#[test]
fn slow_device_keeps_progressing_on_a_fake_clock() {
    let bps = 100.0 * 1024.0;
    let worst = simulate(512 * K * K, bps, bps, f64::INFINITY);
    assert!(
        worst < FLUSH_STALL.as_secs_f64(),
        "longest silence {worst:.1} s"
    );
    // The old sizing: one 64 MiB chunk at 100 KiB/s is ~655 s of silence.
    assert!(64.0 * 1024.0 * 1024.0 / bps > FLUSH_STALL.as_secs_f64());
    let dead = simulate(512 * K * K, bps, bps, 300.0);
    assert!(
        dead >= FLUSH_STALL.as_secs_f64(),
        "a dead device is silence: {dead}"
    );
}

/// Design review item 1 (scaled wall clock): a writer and a device both throttled so a
/// floor chunk takes a quarter window, and a cap chunk four windows. Starting at the floor
/// the flush keeps progressing to `Ok`; the same device stalled mid-run still fails E9056
/// within the window.
#[test]
fn throttled_writer_never_latches_sync_timeout() {
    let floor = 16 * K;
    let rate = floor * 1000 / (W.as_millis() as u64 / 4); // bytes per second
    let t = FlushTiming {
        slow_chunk: W / 2,
        chunk_min: floor,
        chunk_max: 16 * floor,
        ..timing(W)
    };
    let ops = Arc::new(FakeFlushOps {
        rate: Some(rate),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, t);
    for _ in 0..24 {
        w.write_all(&[8u8; 16 * K as usize])
            .expect("a slow but healthy device never latches SyncTimeout");
        std::thread::sleep(W / 4);
    }
    w.sync_all().expect("and its final flush completes");
    assert!(ops.chunks.load(Ordering::SeqCst) >= 4);

    let gate = Gate::open();
    let n = Arc::new(AtomicUsize::new(0));
    let (g2, n2) = (gate.clone(), n.clone());
    let ops = Arc::new(FakeFlushOps {
        rate: Some(rate),
        chunk_delay: Some(Box::new(move |i| {
            n2.store(i, Ordering::SeqCst);
            if i == 2 {
                g2.0.store(false, Ordering::SeqCst);
            }
            Duration::ZERO
        })),
        chunk_gate: gate.clone(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, t);
    let t0 = Instant::now();
    let r = (|| {
        for _ in 0..24 {
            w.write_all(&[8u8; 16 * K as usize])?;
            std::thread::sleep(W / 4);
        }
        w.sync_all()
    })();
    let e = r.expect_err("a stalled device still times out");
    assert!(is_sync_timeout(&e), "{e}");
    assert!(
        n.load(Ordering::SeqCst) >= 2,
        "it progressed before the stall"
    );
    assert!(t0.elapsed() < W * 24, "{:?}", t0.elapsed());
    gate.release();
}

/// LP13g (§2.10 item 4): a flusher stalled a whole window while writing latches
/// `SyncTimeout`: the blocked write returns E9056, and so does the next one (sticky).
#[test]
fn flusher_stall_during_writing_latches_e9056() {
    let ops = Arc::new(FakeFlushOps {
        chunk_gate: Gate::closed(),
        ..FakeFlushOps::default()
    });
    let (_d, mut w, _) = fake_file(&ops, timing(W));
    for _ in 0..3 {
        w.write_all(&[6u8; K as usize]).unwrap();
    }
    let t = Instant::now();
    let e = w
        .write_all(&[6u8; K as usize])
        .expect_err("the stalled flusher fails the write");
    assert!(is_sync_timeout(&e), "{e}");
    assert!(t.elapsed() <= W + SLACK, "{:?}", t.elapsed());
    let t = Instant::now();
    let e = w.write(b"x").expect_err("sticky");
    assert!(is_sync_timeout(&e), "{e}");
    assert!(t.elapsed() < W * 3 / 4, "a latched error does not wait");
    ops.chunk_gate.release();
}

fn sized_file(len: u64) -> (tempfile::TempDir, File) {
    let dir = tempfile::tempdir().unwrap();
    let f = File::create(dir.path().join("sync.bin")).unwrap();
    f.set_len(len).unwrap();
    (dir, f)
}

fn durable_timing(stall: Duration) -> DurableTiming {
    DurableTiming {
        stall,
        piece_target: Duration::from_millis(20),
        window_min: 4 * K,
        window_max: 16 * K,
        sample_every: W / 4,
    }
}

/// LP19 (§4.5), Linux-local shape: every window's completion reports `bytes_done`; the
/// window starts at the floor (1 MiB, scaled), doubles after fast pieces to the cap and
/// halves after one over the target (2 s, scaled); the final flush follows.
#[test]
fn durable_sync_file_reports_piece_completions() {
    let ops = Arc::new(FakeFlushOps {
        pieces: Some(Box::new(|i| {
            if i == 2 || i == 3 {
                Duration::from_millis(40)
            } else {
                Duration::ZERO
            }
        })),
        ..FakeFlushOps::default()
    });
    let (_d, f) = sized_file(64 * K);
    let mut seen = Vec::new();
    let dyn_ops: Arc<dyn FlushOps> = ops.clone();
    let t = durable_timing(Duration::from_secs(30));
    let start = Instant::now();
    let report = |d, n| seen.push((d, n, Instant::now()));
    durable_sync_file_with(&f, None, report, dyn_ops, t).unwrap();
    let end = Instant::now();
    let ranges = ops.ranges.lock().unwrap().clone();
    // A piece's measured time lies between its fake call's entry-to-report span and the span
    // from the previous report to the next call's entry; the window may take any step
    // (halve > target, keep, double < target / 4) that time allows. The 40 ms pieces halve.
    let (target, min, max) = (t.piece_target, t.window_min, t.window_max);
    let (mut windows, mut done) = (vec![min], 0);
    for (i, &(off, len, entered)) in ranges.iter().enumerate() {
        assert_eq!(off, done, "piece {i} is contiguous");
        windows.retain(|w| (*w).min(64 * K - done) == len);
        assert!(!windows.is_empty(), "piece {i} of {len}: {ranges:?}");
        done += len;
        let before = i.checked_sub(1).map_or(start, |p| seen[p].2);
        let after = ranges.get(i + 1).map_or(end, |r| r.2);
        let (lo, hi) = (seen[i].2 - entered, after - before);
        let mut next = Vec::new();
        for w in windows {
            if hi > target {
                next.push((w / 2).max(min));
            }
            if lo <= target && hi >= target / 4 {
                next.push(w);
            }
            if lo < target / 4 {
                next.push((w * 2).min(max));
            }
        }
        next.sort_unstable();
        next.dedup();
        windows = next;
    }
    assert_eq!(done, 64 * K, "{ranges:?}");
    assert_eq!(
        ops.finishes.load(Ordering::SeqCst),
        1,
        "then the final flush"
    );
    assert!(seen.windows(2).all(|p| p[0].0 < p[1].0), "{seen:?}");
    assert_eq!(seen.last().map(|s| (s.0, s.1)), Some((64 * K, 64 * K)));
    assert!(seen.len() >= ranges.len(), "one report per completed piece");
}

/// LP19, NFS shape: one whole-file flush blocked 3 × window while the sampled
/// `mountstats` counter rises reports those deltas, is not a stall, then completes.
#[test]
fn durable_sync_file_reports_mountstats_deltas() {
    let ops = Arc::new(FakeFlushOps {
        finish_sleep: W * 3,
        sample_from: Some(Instant::now()),
        ..FakeFlushOps::default()
    });
    let (_d, f) = sized_file(64 * K);
    let mut seen = Vec::new();
    let t = durable_timing(W);
    durable_sync_file_with(&f, None, |d, n| seen.push((d, n)), ops, t).unwrap();
    let before_done = seen.iter().filter(|(d, _)| *d < 64 * K).count();
    assert!(before_done >= 2, "deltas reported while blocked: {seen:?}");
    assert_eq!(seen.last(), Some(&(64 * K, 64 * K)));
}

/// LP19, macOS/Windows shape: no range sync and no sampled signal, so the one call's
/// completion is the report.
#[test]
fn durable_sync_file_reports_the_single_completion() {
    let ops = Arc::new(FakeFlushOps {
        finish_sleep: Duration::from_millis(10),
        ..FakeFlushOps::default()
    });
    let (_d, f) = sized_file(8 * K);
    let mut seen = Vec::new();
    let t = durable_timing(W);
    durable_sync_file_with(&f, None, |d, n| seen.push((d, n)), ops, t).unwrap();
    assert_eq!(seen, [(8 * K, 8 * K)]);
}

/// `durable_sync_file`: a blocked flush with no signal is E9056 at the window (G19);
/// a cancel returns `Halted` at once.
#[test]
fn durable_sync_file_stall_and_stop() {
    let gate = Gate::closed();
    let ops = Arc::new(FakeFlushOps {
        finish_gate: gate.clone(),
        ..FakeFlushOps::default()
    });
    let (_d, f) = sized_file(8 * K);
    let t = Instant::now();
    let e = durable_sync_file_with(&f, None, |_, _| {}, ops.clone(), durable_timing(W))
        .expect_err("a stalled flush fails");
    assert!(is_sync_timeout(&e), "{e}");
    assert!(t.elapsed() <= W + SLACK, "{:?}", t.elapsed());

    let halt = Halt::new();
    cancel_after(&halt, Duration::from_millis(50));
    let t = Instant::now();
    let long = durable_timing(Duration::from_secs(30));
    let e = durable_sync_file_with(&f, Some(&halt), |_, _| {}, ops, long)
        .expect_err("a stop ends the wait");
    gate.release();
    assert!(crate::error::is_halt(&e), "{e}");
    assert!(t.elapsed() < SLACK, "{:?}", t.elapsed());
}

// A flush whose primitives fail or whose counter jumps by `step` per sample.
struct ScriptedOps {
    range_err: bool,
    finish_err: bool,
    finish_sleep: Duration,
    step: AtomicU64,
}

impl ScriptedOps {
    fn new() -> Self {
        Self {
            range_err: false,
            finish_err: false,
            finish_sleep: Duration::ZERO,
            step: AtomicU64::new(0),
        }
    }
}

impl FlushOps for ScriptedOps {
    fn chunk(&self, _: &File) -> io::Result<()> {
        Ok(())
    }
    fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
        self.range_err.then(|| Err(io::Error::from_raw_os_error(5)))
    }
    fn finish(&self, _: &File) -> io::Result<()> {
        std::thread::sleep(self.finish_sleep);
        if self.finish_err {
            return Err(io::Error::from_raw_os_error(5));
        }
        Ok(())
    }
    fn sample(&self) -> Option<u64> {
        (self.step.load(Ordering::SeqCst) > 0)
            .then(|| self.step.fetch_add(1 << 40, Ordering::SeqCst))
    }
}

/// A failed range sync or final flush is the call's error, never a quiet success.
#[test]
fn durable_sync_file_surfaces_range_and_finish_errors() {
    let (_d, f) = sized_file(8 * K);
    let t = durable_timing(Duration::from_secs(30));
    let range = ScriptedOps {
        range_err: true,
        ..ScriptedOps::new()
    };
    let e = durable_sync_file_with(&f, None, |_, _| {}, Arc::new(range), t)
        .expect_err("a failed range sync fails");
    assert_eq!(e.raw_os_error(), Some(5), "{e}");
    let finish = ScriptedOps {
        finish_err: true,
        ..ScriptedOps::new()
    };
    let e = durable_sync_file_with(&f, None, |_, _| {}, Arc::new(finish), t)
        .expect_err("a failed final flush fails");
    assert_eq!(e.raw_os_error(), Some(5), "{e}");
}

/// A sampled counter past the file length (other writers on the mount) never reports over
/// 100% and still ends with the final `(len, len)` report.
#[test]
fn durable_sync_file_clamps_sampled_progress_below_the_length() {
    let ops = ScriptedOps {
        finish_sleep: W * 3,
        step: AtomicU64::new(1),
        ..ScriptedOps::new()
    };
    let (_d, f) = sized_file(8 * K);
    let mut seen = Vec::new();
    durable_sync_file_with(
        &f,
        None,
        |d, n| seen.push((d, n)),
        Arc::new(ops),
        durable_timing(W),
    )
    .unwrap();
    assert!(seen.len() >= 2, "sampled while blocked: {seen:?}");
    assert!(seen.iter().all(|(d, n)| d <= n), "over 100%: {seen:?}");
    assert_eq!(seen.last(), Some(&(8 * K, 8 * K)));
}

/// An empty file still gets its one completion report.
#[test]
fn durable_sync_file_reports_an_empty_file_once() {
    let (_d, f) = sized_file(0);
    let mut seen = Vec::new();
    let t = durable_timing(W);
    durable_sync_file_with(
        &f,
        None,
        |d, n| seen.push((d, n)),
        Arc::new(ScriptedOps::new()),
        t,
    )
    .unwrap();
    assert_eq!(seen, [(0, 0)]);
}

const MOUNTSTATS: &str = "\
device rootfs mounted on / with fstype rootfs
device nas:/export mounted on /mnt/nas with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2,rsize=1048576,wsize=1048576
\tage:\t1234
\tbytes:\t100 200 300 400 500 600 7 8
\tRPC iostats version: 1.1  p/v: 100003/4 (nfs)
\tper-op statistics
\t        NULL: 0 0 0 0 0 0 0 0
\t       WRITE: 10 10 0 1000 2000 5 6 7
\t      COMMIT: 3 3 0 0 0 0 0 0
device nas:/deep mounted on /mnt/nas/deep with fstype nfs statvers=1.1
\tbytes:\t1 2 3 4 5 9000 0 0
\tper-op statistics
\t       WRITE: 90 90 0 0 0 0 0 0
\t      COMMIT: 9 9 0 0 0 0 0 0
device nas:/sp mounted on /mnt/my\\040share with fstype nfs statvers=1.1
\tbytes:\t0 0 0 0 0 40 0 0
\t       WRITE: 1 1 0 0 0 0 0 0
\t      COMMIT: 1 1 0 0 0 0 0 0
device /dev/sda1 mounted on /data with fstype ext4
";

/// LP13h `mountstats_parser` (§2.10 table, Linux NFS): server-write bytes (the 6th
/// `bytes:` field) + WRITE ops + COMMIT ops of the NFS mount holding the path, the
/// longest mount point prefix; an escaped mount point matches; a local mount is `None`.
#[test]
fn mountstats_parser() {
    assert_eq!(
        parse_mountstats(MOUNTSTATS, "/mnt/nas/movie.mkv"),
        Some(613)
    );
    assert_eq!(
        parse_mountstats(MOUNTSTATS, "/mnt/nas/deep/a.iso"),
        Some(9099)
    );
    assert_eq!(parse_mountstats(MOUNTSTATS, "/mnt/my share/x"), Some(42));
    assert_eq!(parse_mountstats(MOUNTSTATS, "/mnt/nasty/x"), None);
    assert_eq!(parse_mountstats(MOUNTSTATS, "/data/x"), None);
    assert_eq!(parse_mountstats("", "/mnt/nas/x"), None);
}

// Real syscalls through the production WritebackFile: written bytes become durable,
// the progress moves, and sync_all succeeds (LP13h and its macOS/Windows equivalents).
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn real_flush_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WritebackFile::create(&dir.path().join("real.bin")).unwrap();
    let flush = FlushProgress::new(Liveness::new());
    w.set_flush_progress(flush.clone());
    let block = vec![9u8; 1024 * 1024];
    for _ in 0..8 {
        w.write_all(&block).unwrap();
    }
    w.sync_all().unwrap();
    assert!(
        flush.progress().get() > 0,
        "writes and flushes are progress"
    );
    assert_eq!(flush.bytes_total(), 8 * 1024 * 1024);
    assert_eq!(flush.bytes_durable(), 8 * 1024 * 1024, "everything durable");
}

/// LP13h `linux_real_flush_progress` (dev CI, runner tmpdir).
#[cfg(target_os = "linux")]
#[test]
fn linux_real_flush_progress() {
    real_flush_progress();
}

/// LP13h, macOS equivalent (runs on qa `release-tests`).
#[cfg(target_os = "macos")]
#[test]
fn macos_real_flush_progress() {
    real_flush_progress();
}

/// LP13h, Windows equivalent (runs on qa `release-tests`).
#[cfg(target_os = "windows")]
#[test]
fn windows_real_flush_progress() {
    real_flush_progress();
}

#[cfg(unix)]
fn written_tmp() -> (tempfile::TempDir, File) {
    let dir = tempfile::tempdir().unwrap();
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("sp.bin"))
        .unwrap();
    f.write_all(&[0x5au8; 64 * 1024]).unwrap();
    (dir, f)
}

/// SP10, per spec (SS-12 XSH fdatasync(): "shall force all currently queued I/O
/// operations … to the synchronized I/O completion state"): a written file on the
/// runner's tmpdir syncs with 0. On tmpfs it is a no-op success, asserted the same.
#[cfg(target_os = "linux")]
#[test]
fn fdatasync_after_write_returns_ok_on_tmpdir() {
    use std::os::unix::io::AsRawFd;
    let (_d, f) = written_tmp();
    // SAFETY: a valid fd for the call's duration.
    let rc = unsafe { libc::fdatasync(f.as_raw_fd()) };
    assert_eq!(rc, 0, "{}", io::Error::last_os_error());
}

/// SP11, per spec (SS-13 sync_file_range(2): WAIT_BEFORE | WRITE | WAIT_AFTER "is a
/// write-for-data-integrity operation"): the flags are accepted and the range reads back
/// from the device via O_DIRECT. Skipped (logged `SKIP`) on tmpfs/overlay or O_DIRECT EINVAL.
#[cfg(target_os = "linux")]
#[test]
fn sync_file_range_wait_after_completes_written_range() {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    let (dir, f) = written_tmp();
    let flags = libc::SYNC_FILE_RANGE_WAIT_BEFORE
        | libc::SYNC_FILE_RANGE_WRITE
        | libc::SYNC_FILE_RANGE_WAIT_AFTER;
    // SAFETY: a valid fd for the call's duration.
    let rc = unsafe { libc::sync_file_range(f.as_raw_fd(), 0, 64 * 1024, flags) };
    assert_eq!(rc, 0, "{}", io::Error::last_os_error());
    // SAFETY: statfs fills a zeroed struct for a valid fd.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::fstatfs(f.as_raw_fd(), &mut st) }, 0);
    const TMPFS: i64 = 0x0102_1994;
    const OVERLAY: i64 = 0x794c_7630;
    #[allow(clippy::unnecessary_cast)] // f_type is i64 on some targets, not all
    let f_type = st.f_type as i64;
    if [TMPFS, OVERLAY].contains(&f_type) {
        eprintln!("SKIP sync_file_range: tmpdir is tmpfs/overlay");
        return;
    }
    let direct = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(dir.path().join("sp.bin"));
    let mut direct = match direct {
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("SKIP sync_file_range: O_DIRECT is EINVAL here");
            return;
        }
        r => r.unwrap(),
    };
    // O_DIRECT needs an aligned buffer: over-allocate and read at a 4 KiB boundary.
    let mut raw = vec![0u8; 64 * 1024 + 4096];
    let off = raw.as_ptr().align_offset(4096);
    let buf = &mut raw[off..off + 64 * 1024];
    match direct.read_exact(buf) {
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("SKIP sync_file_range: O_DIRECT read is EINVAL here");
        }
        r => {
            r.unwrap();
            assert!(
                buf.iter().all(|&b| b == 0x5a),
                "the range reached the device"
            );
        }
    }
}

/// SP12, per spec (SS-14 fcntl(2) F_FULLFSYNC: "Does the same thing as fsync(2) then
/// asks the drive to flush all buffered data"): 0, or on a filesystem without it
/// (ENOTSUP) the `fsync` fallback is 0. Compiles on dev; runs on qa macOS.
#[cfg(target_os = "macos")]
#[test]
fn f_fullfsync_or_enotsup_falls_back() {
    use std::os::unix::io::AsRawFd;
    let (_d, f) = written_tmp();
    // SAFETY: a valid fd for the calls' duration.
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc != 0 {
        let e = io::Error::last_os_error();
        assert_eq!(e.raw_os_error(), Some(libc::ENOTSUP), "{e}");
        assert_eq!(unsafe { libc::fsync(f.as_raw_fd()) }, 0);
    }
}

/// SP13, per spec (SS-15 FlushFileBuffers: "writes all the buffered information for a
/// specified file to the device"): succeeds on a written file. Compiles on dev; qa Windows.
#[cfg(target_os = "windows")]
#[test]
fn flushfilebuffers_ok() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = File::create(dir.path().join("sp.bin")).unwrap();
    f.write_all(&[0x5au8; 64 * 1024]).unwrap();
    // std's `File::sync_all` is `FlushFileBuffers` on Windows.
    f.sync_all().unwrap();
}

/// Every failed flush wait is an error, never `Ok`, and the three causes stay apart:
/// E9056 (stall), `is_halt` (a Stop) and E9057 (worker lost). Moved from the per-OS
/// `durable_sync` mapping tests.
#[test]
fn every_wait_failure_is_a_distinct_error() {
    use crate::error::{E_SYNC_WORKER_LOST, error_code};
    use crate::io::bounded::BoundedError;
    let timeout = wait_failure(BoundedError::Timeout, "t");
    assert!(is_sync_timeout(&timeout), "{timeout}");
    assert_eq!(timeout.kind(), io::ErrorKind::TimedOut);
    let halted = wait_failure(BoundedError::Halted, "t");
    assert!(crate::error::is_halt(&halted), "{halted}");
    let lost = wait_failure(BoundedError::WorkerLost, "t");
    assert_eq!(error_code(&lost), Some(E_SYNC_WORKER_LOST));
}

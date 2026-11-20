// MKV frame write: SimpleBlock vs BlockGroup vs MVC BlockGroup at 2 KB / 200 KB /
// 2 MB frames, into a mock `Write + Seek` that keeps no bytes and counts seeks,
// position queries and flushes.

use std::cell::Cell;
use std::hint::black_box;
use std::io::{self, Seek, SeekFrom, Write};
use std::rc::Rc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use libfreemkv::mux::__bench::MkvWriter;

#[path = "common/alloc.rs"]
mod alloc;

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

#[derive(Default)]
struct Counts {
    bytes: Cell<u64>,
    writes: Cell<u64>,
    seeks: Cell<u64>,
    position_queries: Cell<u64>,
    flushes: Cell<u64>,
}

// Tracks position and length like a file but stores nothing.
struct CountingSink {
    pos: u64,
    len: u64,
    counts: Rc<Counts>,
}

impl Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let c = &self.counts;
        c.bytes.set(c.bytes.get() + buf.len() as u64);
        c.writes.set(c.writes.get() + 1);
        self.pos += buf.len() as u64;
        self.len = self.len.max(self.pos);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.counts.flushes.set(self.counts.flushes.get() + 1);
        Ok(())
    }
}

impl Seek for CountingSink {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let c = &self.counts;
        match to {
            SeekFrom::Current(0) => c.position_queries.set(c.position_queries.get() + 1),
            _ => c.seeks.set(c.seeks.get() + 1),
        }
        self.pos = match to {
            SeekFrom::Start(p) => p,
            SeekFrom::Current(d) => self.pos.saturating_add_signed(d),
            SeekFrom::End(d) => self.len.saturating_add_signed(d),
        };
        Ok(self.pos)
    }
}

#[derive(Clone, Copy)]
enum Kind {
    SimpleBlock,
    BlockGroup,
    Mvc,
}

const FRAME_NS: i64 = 41_708_333;
// About two minutes of video: 60 two-second clusters.
const FRAMES: usize = 2880;

fn mux(kind: Kind, size: usize, frame: &[u8], counts: Rc<Counts>) -> io::Result<()> {
    let sink = CountingSink {
        pos: 0,
        len: 0,
        counts,
    };
    let mut w = MkvWriter::new(sink, matches!(kind, Kind::Mvc))?;
    let (duration, additional) = match kind {
        Kind::SimpleBlock => (None, None),
        Kind::BlockGroup => (Some(FRAME_NS as u64), None),
        Kind::Mvc => (None, Some(&frame[..size / 2])),
    };
    let audio = vec![0x0Bu8; 1792];
    for i in 0..FRAMES {
        let pts = i as i64 * FRAME_NS;
        w.write(
            0,
            pts,
            i.is_multiple_of(24),
            &frame[..size],
            duration,
            additional,
        )?;
        w.write(1, pts, true, &audio, Some(32_000_000), None)?;
    }
    w.finish()
}

fn mkv_write_frames(c: &mut Criterion) {
    let frame: Vec<u8> = (0..2_000_000u32).map(|i| i as u8).collect();
    let mut group = c.benchmark_group("mkv_write_frames");
    group.sample_size(10);
    for (kind, name) in [
        (Kind::SimpleBlock, "simple_block"),
        (Kind::BlockGroup, "block_group"),
        (Kind::Mvc, "mvc_block_group"),
    ] {
        for size in [2_000usize, 200_000, 2_000_000] {
            let counts = Rc::new(Counts::default());
            let (allocs, alloc_bytes, r) =
                alloc::measure(|| mux(kind, size, &frame, counts.clone()));
            r.expect("mux");
            let clusters = FRAMES as f64 / 48.0;
            eprintln!(
                "mkv_write_frames/{name}/{size}: {:.1} seeks, {:.1} position queries, {:.1} flushes, \
                 {:.0} writes per cluster; {allocs} allocations, {alloc_bytes} bytes allocated",
                counts.seeks.get() as f64 / clusters,
                counts.position_queries.get() as f64 / clusters,
                counts.flushes.get() as f64 / clusters,
                counts.writes.get() as f64 / clusters,
            );
            group.throughput(Throughput::Elements(FRAMES as u64));
            group.bench_with_input(BenchmarkId::new(name, size), &size, |b, &size| {
                b.iter(|| {
                    mux(kind, size, black_box(&frame), Rc::new(Counts::default())).expect("mux")
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, mkv_write_frames);
criterion_main!(benches);

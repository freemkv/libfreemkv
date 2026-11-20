// Audio framers against access units per PES: TrueHD, DTS core and AC-3 PES built
// with k = 1, 4, 16 units. A per-AU cost that grows with k is the front-drain memmove.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use libfreemkv::disc::Codec;
use libfreemkv::mux::__bench::{Parser, ac3_frame, dts_core, truehd_unit};

#[path = "common/alloc.rs"]
mod alloc;

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

// Access units parsed per measured iteration.
const UNITS: usize = 4096;

fn run(codec: Codec, unit: &[u8], k: usize) -> (usize, usize) {
    let mut parser = Parser::new(codec);
    let pes: Vec<u8> = unit.repeat(k);
    let (mut frames, mut bytes) = (0, 0);
    for i in 0..UNITS / k {
        let (n, b) = parser.parse(Some(90_000 + i as i64 * 960), pes.clone());
        frames += n;
        bytes += b;
    }
    let (n, b) = parser.flush();
    (frames + n, bytes + b)
}

fn audio_framers(c: &mut Criterion) {
    let units = [
        ("truehd", Codec::TrueHd, truehd_unit(400)),
        ("dts_core", Codec::Dts, dts_core(2012)),
        ("ac3", Codec::Ac3, ac3_frame(30)),
    ];
    let mut group = c.benchmark_group("audio_framers");
    group.throughput(Throughput::Elements(UNITS as u64));
    for (name, codec, unit) in &units {
        for k in [1usize, 4, 16] {
            let (allocs, alloc_bytes, (frames, bytes)) = alloc::measure(|| run(*codec, unit, k));
            eprintln!(
                "audio_framers/{name}/{k}: {frames} frames, {bytes} bytes; \
                 {allocs} allocations, {alloc_bytes} bytes allocated"
            );
            group.bench_with_input(BenchmarkId::new(*name, k), &k, |b, &k| {
                b.iter(|| black_box(run(*codec, black_box(unit), k)))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, audio_framers);
criterion_main!(benches);

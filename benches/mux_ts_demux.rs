// TS demux throughput and allocations: a synthetic 64 MiB BD-TS stream (one
// H.264-sized video PID, two audio PIDs, one PGS PID) fed in 3 MiB batches.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use libfreemkv::mux::__bench::TsDemux;

#[path = "common/alloc.rs"]
mod alloc;

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

const VIDEO: u16 = 0x1011;
const AUDIO: [u16; 2] = [0x1100, 0x1101];
const PGS: u16 = 0x1200;
const TOTAL: usize = 64 * 1024 * 1024;
const BATCH: usize = 3 * 1024 * 1024;

// Deterministic xorshift so every run demuxes the same stream.
struct Rng(u64);

impl Rng {
    fn range(&mut self, lo: usize, hi: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        lo + (self.0 % (hi - lo) as u64) as usize
    }
}

fn pts_field(prefix: u8, pts: u64) -> [u8; 5] {
    [
        prefix | (((pts >> 30) as u8 & 0x07) << 1) | 1,
        (pts >> 22) as u8,
        (((pts >> 15) as u8) << 1) | 1,
        (pts >> 7) as u8,
        ((pts as u8) << 1) | 1,
    ]
}

// One PES (header + `es_len` payload bytes) cut into 192-byte BD-TS packets.
fn packetize(pid: u16, stream_id: u8, pts: u64, es_len: usize, cc: &mut u8) -> Vec<[u8; 192]> {
    let mut pes = vec![0, 0, 1, stream_id, 0, 0, 0x80, 0x80, 5];
    pes.extend_from_slice(&pts_field(0x20, pts));
    pes.extend((0..es_len).map(|i| (i * 7) as u8 | 1));
    let mut out = Vec::new();
    for (i, chunk) in pes.chunks(184).enumerate() {
        let mut pkt = [0xFFu8; 192];
        pkt[4] = 0x47;
        pkt[5] = ((pid >> 8) as u8 & 0x1F) | if i == 0 { 0x40 } else { 0 };
        pkt[6] = pid as u8;
        let stuff = 184 - chunk.len();
        if stuff == 0 {
            pkt[7] = 0x10 | *cc;
        } else {
            pkt[7] = 0x30 | *cc;
            pkt[8] = (stuff - 1) as u8;
            if stuff > 1 {
                pkt[9] = 0;
            }
        }
        pkt[192 - chunk.len()..].copy_from_slice(chunk);
        *cc = (*cc + 1) & 0x0F;
        out.push(pkt);
    }
    out
}

// Every PID's packets, interleaved evenly by their position within that PID.
fn build_stream() -> Vec<u8> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut per_pid: Vec<Vec<[u8; 192]>> = vec![Vec::new(); 4];
    let mut cc = [0u8; 4];
    let mut bytes = 0usize;
    let mut frame = 0u64;
    while bytes < TOTAL {
        let pts = 90_000 + frame * 3754;
        let v = packetize(VIDEO, 0xE0, pts, rng.range(50_000, 2_000_000), &mut cc[0]);
        bytes += v.len() * 192;
        per_pid[0].extend(v);
        for (i, &pid) in AUDIO.iter().enumerate() {
            let a = packetize(pid, 0xBD, pts, rng.range(2_000, 8_000), &mut cc[1 + i]);
            bytes += a.len() * 192;
            per_pid[1 + i].extend(a);
        }
        if frame.is_multiple_of(24) {
            let p = packetize(PGS, 0xBD, pts, rng.range(200, 4_000), &mut cc[3]);
            bytes += p.len() * 192;
            per_pid[3].extend(p);
        }
        frame += 1;
    }
    let mut keyed: Vec<(f64, &[u8; 192])> = Vec::new();
    for pkts in &per_pid {
        let n = pkts.len() as f64;
        keyed.extend(pkts.iter().enumerate().map(|(i, p)| (i as f64 / n, p)));
    }
    keyed.sort_by(|a, b| a.0.total_cmp(&b.0));
    keyed.iter().flat_map(|(_, p)| p.iter().copied()).collect()
}

fn demux_all(stream: &[u8]) -> (usize, usize) {
    let mut demux = TsDemux::new(&[VIDEO, AUDIO[0], AUDIO[1], PGS]);
    let (mut pes, mut bytes) = (0, 0);
    for (i, batch) in stream.chunks(BATCH).enumerate() {
        let (n, b) = demux.feed_at((i * BATCH) as u64, batch);
        pes += n;
        bytes += b;
    }
    let (n, b) = demux.flush();
    (pes + n, bytes + b)
}

fn ts_demux_feed(c: &mut Criterion) {
    let stream = build_stream();
    let (allocs, alloc_bytes, (pes, es)) = alloc::measure(|| demux_all(&stream));
    let gb = stream.len() as f64 / 1e9;
    eprintln!(
        "ts_demux_feed: {} MiB in, {pes} PES, {es} ES bytes; per GB: {:.0} allocations, {:.0} MB allocated",
        stream.len() >> 20,
        allocs as f64 / gb,
        alloc_bytes as f64 / gb / 1e6,
    );
    let mut group = c.benchmark_group("ts_demux");
    group.throughput(Throughput::Bytes(stream.len() as u64));
    group.sample_size(10);
    group.bench_function("feed_3MiB_batches", |b| {
        b.iter(|| black_box(demux_all(black_box(&stream))))
    });
    group.finish();
}

criterion_group!(benches, ts_demux_feed);
criterion_main!(benches);

//! Decrypt micro-bench: the AACS unit decrypt alone, the AACS decrypting reader at
//! the batch sizes the read paths use, and the CSS descramble.
//!
//! `cargo bench --bench decrypt` (`FREEMKV_BENCH_MIB` sets the bytes per case).
//! Each line reports MiB/s and CPU seconds per GiB, so pool spin shows up as CPU.
//! Rerun under `RUSTFLAGS='--cfg aes_backend="avx256"'` to compare VAES on x86_64.

mod common;

use common::{bench_mib, encrypted_units, measure, report};
use libfreemkv::decrypt::DecryptKeys;
use libfreemkv::sector::{DecryptingSectorSource, SectorSource};
use libfreemkv::test_util::{MemSource, decrypt_unit, keyed_reader};

const KEY: [u8; 16] = [0x5A; 16];
const UNIT: usize = 6144;
const SECTOR: usize = 2048;

fn main() {
    let mib = bench_mib(512);
    let total = (mib * 1024 * 1024) as u64;
    println!(
        "decrypt bench: {mib} MiB per case, {} decrypt threads by default",
        libfreemkv::decrypt_threads()
    );
    unit_decrypt(total);
    for threads in [1, 0] {
        for batch in [60u16, 510, 1536, 8190] {
            aacs_reader(total, batch, threads);
        }
    }
    for threads in [1, 0] {
        for batch in [60u16, 512, 8192] {
            css_reader(total, batch, threads);
        }
    }
    libfreemkv::set_decrypt_threads(0);
}

// `decrypt_unit` over one buffer again and again: the per-unit cipher cost alone.
fn unit_decrypt(total: u64) {
    let mut buf = encrypted_units(1024, &KEY);
    let rounds = (total / buf.len() as u64).max(1);
    let (wall, cpu) = measure(|| {
        for _ in 0..rounds {
            for u in buf.chunks_exact_mut(UNIT) {
                decrypt_unit(u, &KEY);
            }
        }
    });
    std::hint::black_box(&buf);
    report(
        "aacs decrypt_unit (serial)",
        rounds * buf.len() as u64,
        wall,
        cpu,
    );
}

// The keyed whole-disc reader over an in-memory image, read in `batch`-sector reads.
fn aacs_reader(total: u64, batch: u16, threads: usize) {
    libfreemkv::set_decrypt_threads(threads);
    let units = 16 * 1024u32;
    let image = encrypted_units(units, &KEY);
    let sectors = units * 3;
    let mut r = keyed_reader(MemSource::new(image), KEY, &[(0, sectors)]);
    let mut buf = vec![0u8; batch as usize * SECTOR];
    let mut done = 0u64;
    let (wall, cpu) = measure(|| {
        let mut lba = 0u32;
        while done < total {
            let n = u32::from(batch).min(sectors - lba);
            let len = n as usize * SECTOR;
            let got = r
                .read_sectors(lba, n as u16, &mut buf[..len], false)
                .expect("read");
            assert_eq!(got, len);
            done += got as u64;
            lba = if lba + n >= sectors { 0 } else { lba + n };
        }
    });
    std::hint::black_box(&buf);
    let name = format!("aacs reader batch {batch} threads {}", label(threads));
    report(&name, done, wall, cpu);
}

// The CSS descramble behind a decrypting reader (the source copy restores the ciphertext).
fn css_reader(total: u64, batch: u16, threads: usize) {
    libfreemkv::set_decrypt_threads(threads);
    let sectors = 32 * 1024u32;
    let image = scrambled_packs(sectors);
    let keys = DecryptKeys::Css {
        title_key: [1, 2, 3, 4, 5],
    };
    let mut r = DecryptingSectorSource::new(MemSource::new(image), keys);
    let mut buf = vec![0u8; batch as usize * SECTOR];
    let mut done = 0u64;
    let (wall, cpu) = measure(|| {
        let mut lba = 0u32;
        while done < total {
            let n = u32::from(batch).min(sectors - lba);
            let len = n as usize * SECTOR;
            let got = r
                .read_sectors(lba, n as u16, &mut buf[..len], false)
                .expect("read");
            done += got as u64;
            lba = if lba + n >= sectors { 0 } else { lba + n };
        }
    });
    std::hint::black_box(&buf);
    let name = format!("css reader batch {batch} threads {}", label(threads));
    report(&name, done, wall, cpu);
}

fn label(threads: usize) -> String {
    match threads {
        0 => format!("default({})", libfreemkv::decrypt_threads()),
        n => n.to_string(),
    }
}

// MPEG-2 PS video packs flagged scrambled, with a crib-less noisy header.
fn scrambled_packs(sectors: u32) -> Vec<u8> {
    let mut out = vec![0u8; sectors as usize * SECTOR];
    let mut state = 0x1234_5678u32;
    for s in out.chunks_exact_mut(SECTOR) {
        for b in s.iter_mut() {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *b = (state >> 9) as u8 | 1;
        }
        s[..4].copy_from_slice(&[0, 0, 1, 0xBA]);
        s[4] = 0x44;
        s[0x0D] = 0xF8;
        s[0x0E..0x12].copy_from_slice(&[0, 0, 1, 0xE0]);
        s[0x12..0x14].copy_from_slice(&0x07ECu16.to_be_bytes());
        s[0x14] = 0x80 | 0x10;
    }
    out
}

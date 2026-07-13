// Prove the SSIF de-interleave: the base-view .m2ts extents are the base-view
// interleaved units INSIDE the SSIF region, so the dependent (right-eye MVC)
// view = SSIF sectors MINUS base sectors. Then confirm those dependent units
// decrypt under the same Unit Key.
//   probe3d_deint <iso> <uk-hex-32> [clip=00042]

use libfreemkv::aacs::content::{ALIGNED_UNIT_LEN, ALIGNED_UNIT_SECTORS, aacs_unit_encrypted, decrypt_unit};
use libfreemkv::sector::SectorSource;
use libfreemkv::{FileSectorSource, read_filesystem};
use std::path::Path;

fn hex16(s: &str) -> [u8; 16] {
    let s = s.trim().trim_start_matches("0x");
    let mut o = [0u8; 16];
    for i in 0..16 {
        o[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex");
    }
    o
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let iso = &a[1];
    let uk = hex16(&a[2]);
    let clip = a.get(3).map(|s| s.as_str()).unwrap_or("00042");
    let mut r = FileSectorSource::open(Path::new(iso)).expect("open");
    let fs = read_filesystem(&mut r).expect("udf");

    let base = fs
        .file_extents(&mut r, &format!("/BDMV/STREAM/{clip}.m2ts"))
        .expect("base extents");
    let ssif = fs
        .file_extents(&mut r, &format!("/BDMV/STREAM/SSIF/{clip}.ssif"))
        .expect("ssif extents");
    let gb = |sec: u64| sec as f64 * 2048.0 / 1e9;
    let bsum: u64 = base.iter().map(|(_, c)| *c as u64).sum();
    let ssum: u64 = ssif.iter().map(|(_, c)| *c as u64).sum();
    println!("base: {} extents  {:.2} GB", base.len(), gb(bsum));
    println!("ssif: {} extents  {:.2} GB", ssif.len(), gb(ssum));

    // Is the base range a subset of the SSIF range? (LBA span check)
    let brange = (
        base.iter().map(|(l, _)| *l).min().unwrap_or(0),
        base.iter().map(|(l, c)| l + c).max().unwrap_or(0),
    );
    let srange = (
        ssif.iter().map(|(l, _)| *l).min().unwrap_or(0),
        ssif.iter().map(|(l, c)| l + c).max().unwrap_or(0),
    );
    println!(
        "base LBA span [{},{})  ssif LBA span [{},{})  base⊆ssif={}",
        brange.0,
        brange.1,
        srange.0,
        srange.1,
        brange.0 >= srange.0 && brange.1 <= srange.1
    );

    // dependent = ssif − base (per SSIF extent, subtract overlapping base ranges).
    let mut b: Vec<(u32, u32)> = base.iter().map(|&(l, c)| (l, l + c)).collect();
    b.sort();
    let mut dep: Vec<(u32, u32)> = Vec::new();
    for &(sl, sc) in &ssif {
        let (s, e) = (sl, sl + sc);
        let mut cur = s;
        for &(bs, be) in b.iter().filter(|&&(bs, be)| be > s && bs < e) {
            if bs > cur {
                dep.push((cur, bs));
            }
            cur = cur.max(be);
        }
        if cur < e {
            dep.push((cur, e));
        }
    }
    let dsum: u64 = dep.iter().map(|(s, e)| (*e - *s) as u64).sum();
    println!(
        "\ndependent (ssif − base): {} ranges  {:.2} GB  (expected ≈ {:.2} GB)",
        dep.len(),
        gb(dsum),
        gb(ssum - bsum)
    );

    // Decrypt-test dependent-view units at several points across the ranges.
    let (mut tested, mut enc, mut dec) = (0u32, 0u32, 0u32);
    for &(s, e) in dep.iter().filter(|(s, e)| e - s >= ALIGNED_UNIT_SECTORS).take(200) {
        let lba = s;
        let mut buf = vec![0u8; ALIGNED_UNIT_LEN];
        if r.read_sectors(lba, ALIGNED_UNIT_SECTORS as u16, &mut buf, false).is_ok() {
            tested += 1;
            if aacs_unit_encrypted(&buf) {
                enc += 1;
            }
            if decrypt_unit(&mut buf, &uk) {
                dec += 1;
            }
        }
    }
    println!("dependent-view unit decrypt: tested={tested} cpi-encrypted={enc} decrypted={dec}");
}

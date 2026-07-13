// Read-only 3D structure probe: does the disc carry a dependent (MVC) view,
// where, and how is it described? Uses freemkv's own UDF reader (7z can't parse
// these ISOs).  probe3d_struct <iso> [clip=00098]

use libfreemkv::sector::SectorSource;
use libfreemkv::{FileSectorSource, read_filesystem};
use std::path::Path;

fn be16(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
}
fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let iso = &args[1];
    let clip = args.get(2).map(|s| s.as_str()).unwrap_or("00098");
    let mut r = FileSectorSource::open(Path::new(iso)).expect("open iso");
    let fs = read_filesystem(&mut r).expect("read udf");

    // Base view vs dependent-interleave file.
    for (label, path) in [
        ("base .m2ts", format!("/BDMV/STREAM/{clip}.m2ts")),
        ("SSIF", format!("/BDMV/STREAM/SSIF/{clip}.ssif")),
    ] {
        match fs.file_extents(&mut r, &path) {
            Ok(exts) => {
                let sectors: u64 = exts.iter().map(|(_, c)| *c as u64).sum();
                println!(
                    "{label:<11}: EXISTS  {:>5} extents  {:>7.2} GB   {path}",
                    exts.len(),
                    sectors as f64 * 2048.0 / 1e9
                );
            }
            Err(_) => println!("{label:<11}: MISSING                          {path}"),
        }
    }

    // MPLS ExtensionData → the 3D STN_table_SS lives here.
    let mpls = match fs.read_file(&mut r, &format!("/BDMV/PLAYLIST/{clip}.mpls")) {
        Ok(m) => m,
        Err(e) => {
            println!("\nmpls read failed: {e}");
            return;
        }
    };
    println!("\nmpls: {} bytes", mpls.len());
    let ext_addr = be32(&mpls, 16) as usize; // ExtensionData_start_address
    if ext_addr == 0 || ext_addr + 12 > mpls.len() {
        println!("ExtensionData_start_address = {ext_addr}  → NO extension data (not a 3D/SS playlist)");
        return;
    }
    let ed = &mpls[ext_addr..];
    let ed_len = be32(ed, 0) as usize;
    let n_entries = ed[11] as usize; // len(4)+data_block_start(4)+reserved(3)+count(1)
    println!("ExtensionData @ {ext_addr}: length={ed_len}  entries={n_entries}");
    for i in 0..n_entries {
        let o = 12 + i * 12;
        if o + 12 > ed.len() {
            break;
        }
        let id1 = be16(ed, o);
        let id2 = be16(ed, o + 2);
        let addr = be32(ed, o + 4) as usize; // relative to MPLS file start
        let len = be32(ed, o + 8) as usize;
        let tag = match (id1, id2) {
            (1, 1) => "PiP metadata",
            (1, 2) => "SubPath entries (SS)",
            (2, 1) => "STN_table_SS?",
            (2, 2) => "STN_table_SS (MVC dependent view)",
            _ => "?",
        };
        print!("  entry {i}: ID1={id1:#06x} ID2={id2:#06x} addr={addr} len={len}  [{tag}]  hex:");
        let blk = &mpls[addr.min(mpls.len())..(addr + 40).min(mpls.len())];
        for b in blk {
            print!(" {b:02x}");
        }
        println!();
    }
}

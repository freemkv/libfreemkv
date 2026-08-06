//! Tests for the synthetic-image source.
//!
//! Round-tripping through `udf::read_filesystem` is a REGRESSION NET, not an
//! oracle: it proves `parse(write(x)) == x`, which any assumption shared by
//! writer and parser (tag checksum convention, AD stride, descriptor
//! placement) is invisible to. The external check — writing the image to a
//! file and asking the operating system to mount it — is the part that can
//! fail independently, and it is `write_and_mount_externally` below (ignored
//! by default: it shells out and needs a mountable host).

use super::*;
use crate::udf;
use std::io::Write;

/// A scratch directory that removes itself.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "freemkv-dirimage-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn file(&self, rel: &str, bytes: &[u8]) {
        let p = self.0.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(bytes).unwrap();
    }
    fn dir(&self, rel: &str) {
        std::fs::create_dir_all(self.0.join(rel)).unwrap();
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Deterministic filler so a mis-offset read is visible as wrong CONTENT, not
/// just a wrong length.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(31).wrapping_add(seed as u32) as u8)
        .collect()
}

/// A minimal but structurally real Blu-ray folder.
fn bdmv_scratch() -> (Scratch, Vec<u8>, Vec<u8>) {
    let s = Scratch::new("bdmv");
    let index = pattern(1, 100);
    let clip = pattern(7, 5000);
    s.file("BDMV/index.bdmv", &index);
    s.file("BDMV/PLAYLIST/00000.mpls", &pattern(3, 300));
    s.file("BDMV/CLIPINF/00000.clpi", &pattern(5, 700));
    s.file("BDMV/STREAM/00000.m2ts", &clip);
    (s, index, clip)
}

// ── The de-risking spike ────────────────────────────────────────────────────

/// THE load-bearing assertion of the whole design: metadata synthesized here
/// must be parseable by the PRODUCTION `read_filesystem`, unmodified. If this
/// fails, nothing above the sector layer can consume a `dir://` source and the
/// approach is wrong.
#[test]
fn production_parser_reads_the_synthesized_tree() {
    let (s, index, clip) = bdmv_scratch();
    let mut img = DirImage::open(s.path()).unwrap();
    let fs =
        udf::read_filesystem(&mut img).expect("read_filesystem must mount the synthetic image");

    assert!(fs.find_dir("/BDMV").is_some(), "BDMV must be a directory");
    assert!(fs.find_dir("/BDMV/PLAYLIST").is_some());
    assert!(fs.find_dir("/BDMV/CLIPINF").is_some());
    assert!(fs.find_dir("/BDMV/STREAM").is_some());

    let stream = fs.find_dir("/BDMV/STREAM").unwrap();
    let names: Vec<&str> = stream.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["00000.m2ts"]);
    assert_eq!(stream.entries[0].size, clip.len() as u64);

    // Bytes, not just shape: a wrong AD offset or a wrong partition base would
    // still produce a plausible tree.
    assert_eq!(
        fs.read_file(&mut img, "/BDMV/index.bdmv").unwrap(),
        index,
        "read_file must return the host file's bytes"
    );
    assert_eq!(
        fs.read_file(&mut img, "/BDMV/STREAM/00000.m2ts").unwrap(),
        clip
    );
}

/// `file_extents` is what the rip pipeline actually reads a title through, so
/// the extents must be absolute, in range, and cover the file exactly.
#[test]
fn file_extents_are_absolute_and_readable() {
    let (s, _, clip) = bdmv_scratch();
    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();

    let exts = fs
        .file_extents(&mut img, "/BDMV/STREAM/00000.m2ts")
        .unwrap();
    assert_eq!(exts.len(), 1);
    let (lba, sectors) = exts[0];
    assert!(lba > 0, "bluray.rs:137 drops any extent at LBA 0");
    assert_eq!(sectors as usize, clip.len().div_ceil(SECTOR));
    assert!(lba + sectors <= img.capacity_sectors());

    // Read them the way the mux does and compare against the file.
    let mut buf = vec![0u8; sectors as usize * SECTOR];
    img.read_sectors(lba, sectors as u16, &mut buf, false)
        .unwrap();
    assert_eq!(&buf[..clip.len()], &clip[..]);
    assert!(
        buf[clip.len()..].iter().all(|&b| b == 0),
        "the tail sector must be zero-padded"
    );

    // And `file_start_lba` — the term `ifo.rs` adds its IFO offsets to — must
    // agree with the first extent.
    assert_eq!(
        fs.file_start_lba(&mut img, "/BDMV/STREAM/00000.m2ts")
            .unwrap(),
        lba
    );
}

/// A file past the 30-bit allocation-descriptor ceiling comes back as MULTIPLE
/// extents whose lengths sum to the file size — the multi-extent case a real
/// dual-layer disc produces, and the one the single-AD fixture in `udf.rs`
/// never covered. Uses a sparse file so the test costs no real disk space.
#[test]
fn a_file_past_the_ad_ceiling_reads_back_as_multiple_extents() {
    let s = Scratch::new("big");
    s.file("BDMV/index.bdmv", &pattern(1, 16));
    s.dir("BDMV/STREAM");
    let big = s.path().join("BDMV/STREAM/00000.m2ts");
    let size = super::layout::MAX_AD_BYTES + 3 * SECTOR as u64;
    let f = std::fs::File::create(&big).unwrap();
    f.set_len(size).unwrap();
    drop(f);

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let entry = fs
        .find_dir("/BDMV/STREAM")
        .unwrap()
        .entries
        .iter()
        .find(|e| e.name == "00000.m2ts")
        .unwrap();
    assert_eq!(entry.size, size, "declared size survives the split");

    let exts = fs
        .file_extents(&mut img, "/BDMV/STREAM/00000.m2ts")
        .unwrap();
    assert_eq!(exts.len(), 2, "one AD cannot hold a 1 GiB+ file");
    assert_eq!(
        exts.iter().map(|(_, s)| *s as u64).sum::<u64>(),
        size.div_ceil(SECTOR as u64),
        "the extents must cover the whole file"
    );
    assert_eq!(
        exts[1].0,
        exts[0].0 + exts[0].1,
        "the second extent starts where the first ends"
    );
}

/// Reads outside any planned extent are zeros, not errors — a real image has
/// unrecorded sectors too, and `read_filesystem` probes fixed LBAs (256, the
/// VDS window) before it knows what is there.
#[test]
fn gaps_and_out_of_range_reads_are_zero_filled() {
    let (s, _, _) = bdmv_scratch();
    let mut img = DirImage::open(s.path()).unwrap();
    let cap = img.capacity_sectors();
    let mut buf = [0xAAu8; SECTOR * 2];
    let n = img.read_sectors(cap + 10, 2, &mut buf, false).unwrap();
    assert_eq!(n, SECTOR * 2);
    assert!(buf.iter().all(|&b| b == 0));
}

/// Two runs over the same unchanged folder must produce the same image, byte
/// for byte. Non-determinism here would make `dir:// -> iso://` output differ
/// run to run for no reason, and would make every golden test flaky.
#[test]
fn the_same_folder_synthesizes_the_same_image() {
    let (s, _, _) = bdmv_scratch();
    let mut a = DirImage::open(s.path()).unwrap();
    let mut b = DirImage::open(s.path()).unwrap();
    assert_eq!(a.capacity_sectors(), b.capacity_sectors());
    let mut buf_a = vec![0u8; SECTOR * 64];
    let mut buf_b = vec![0u8; SECTOR * 64];
    for start in [0u32, 256, 320, 4096] {
        a.read_sectors(start, 64, &mut buf_a, false).unwrap();
        b.read_sectors(start, 64, &mut buf_b, false).unwrap();
        assert_eq!(buf_a, buf_b, "sectors from {start} differ between runs");
    }
}

/// A directory with enough children that its FID list spans several 2048-byte
/// blocks, read back through the production parser.
///
/// This is the case the block-alignment question turns on: FIDs are packed
/// contiguously and a descriptor may straddle a block boundary, because
/// `read_directory` (`udf.rs:1312`) walks the directory extent as one flat byte
/// run and BREAKS at the first non-257 tag. Padding each block would truncate
/// the directory at the first pad. 200 entries also exceeds the 16-handle LRU
/// several times over, so it exercises handle eviction on the read path.
#[test]
fn a_directory_spanning_many_blocks_reads_back_whole() {
    const N: usize = 200;
    let s = Scratch::new("wide");
    s.file("BDMV/index.bdmv", &pattern(1, 16));
    for i in 0..N {
        s.file(
            &format!("BDMV/STREAM/{i:05}.m2ts"),
            &pattern(i as u8, 1000 + i),
        );
    }

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let stream = fs.find_dir("/BDMV/STREAM").unwrap();
    assert_eq!(
        stream.entries.len(),
        N,
        "every FID must survive the block boundaries"
    );
    assert!(
        stream.size > SECTOR as u64,
        "the fixture must actually span blocks, or it proves nothing"
    );

    // Read every file back, in an order that thrashes the handle cache.
    for i in (0..N).rev() {
        let path = format!("/BDMV/STREAM/{i:05}.m2ts");
        assert_eq!(
            fs.read_file(&mut img, &path).unwrap(),
            pattern(i as u8, 1000 + i),
            "{path} came back wrong"
        );
    }
}

// ── Rejection gates ─────────────────────────────────────────────────────────

/// A 3D folder must be REFUSED, not planned. `bluray.rs:127` sets `is_3d` the
/// moment an `.ssif` resolves, and this planner has no extent aliasing, so a
/// planned 3D image would rip the wrong bytes and report success.
#[test]
fn a_3d_folder_is_rejected_rather_than_mis_planned() {
    let s = Scratch::new("ssif");
    s.file("BDMV/index.bdmv", &pattern(1, 16));
    s.file("BDMV/STREAM/00000.m2ts", &pattern(2, 4096));
    s.file("BDMV/STREAM/SSIF/00000.ssif", &pattern(3, 8192));
    let err = DirImage::open(s.path()).unwrap_err();
    assert_eq!(err.code(), crate::error::E_DIR_IMAGE_SSIF_UNSUPPORTED);
}

/// A folder with no disc structure at all is not an image.
#[test]
fn a_folder_with_no_disc_structure_is_rejected() {
    let s = Scratch::new("empty");
    s.file("readme.txt", b"not a disc");
    let err = DirImage::open(s.path()).unwrap_err();
    assert_eq!(err.code(), crate::error::E_DIR_IMAGE_UNSUPPORTED_TREE);
}

/// A file that shrinks between planning and reading is an ERROR. Zero-filling
/// the difference would produce a truncated rip that exits 0.
#[test]
fn a_file_that_shrinks_after_planning_fails_the_read() {
    let (s, _, clip) = bdmv_scratch();
    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let (lba, sectors) = fs
        .file_extents(&mut img, "/BDMV/STREAM/00000.m2ts")
        .unwrap()[0];
    assert!(clip.len() > SECTOR);

    // Truncate the backing file behind the image's back.
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(s.path().join("BDMV/STREAM/00000.m2ts"))
        .unwrap();
    f.set_len(16).unwrap();
    drop(f);

    let mut buf = vec![0u8; sectors as usize * SECTOR];
    let err = img
        .read_sectors(lba, sectors as u16, &mut buf, false)
        .unwrap_err();
    assert_eq!(err.code(), crate::error::E_DIR_IMAGE_FILE_CHANGED);
}

// ── DVD placement ───────────────────────────────────────────────────────────

/// Build a `VTS_01_0.IFO` body whose VOB pointers are the given sector
/// offsets. Only the fields the planner and `ifo.rs` read are filled.
fn vts_ifo(len: usize, vtsm_vobs: u32, vtstt_vobs: u32) -> Vec<u8> {
    let mut v = vec![0u8; len.max(0xC8)];
    v[0..12].copy_from_slice(b"DVDVIDEO-VTS");
    v[0xC0..0xC4].copy_from_slice(&vtsm_vobs.to_be_bytes());
    v[0xC4..0xC8].copy_from_slice(&vtstt_vobs.to_be_bytes());
    v
}

/// THE DVD invariant. `ifo.rs:554-556` computes
/// `vob_start_sector = file_start_lba(VTS_01_0.IFO) + vtstt_vobs`, and the
/// title extents are built on top of that, so the planner must place
/// `VTS_01_1.VOB` at exactly that sector. Anything else rips the wrong bytes
/// with no error anywhere.
#[test]
fn vtstt_vobs_lands_on_the_first_sector_of_the_title_vob() {
    let s = Scratch::new("dvd");
    // IFO is 2 sectors; menu VOB 3 sectors; so the natural, gap-free layout
    // puts the title VOB 5 sectors past the IFO. Declare exactly that.
    let ifo_sectors = 2u32;
    let menu_sectors = 3u32;
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    s.file(
        "VIDEO_TS/VTS_01_0.IFO",
        &vts_ifo(
            ifo_sectors as usize * SECTOR,
            ifo_sectors,
            ifo_sectors + menu_sectors,
        ),
    );
    s.file(
        "VIDEO_TS/VTS_01_0.VOB",
        &pattern(9, menu_sectors as usize * SECTOR),
    );
    let title = pattern(11, 4 * SECTOR);
    s.file("VIDEO_TS/VTS_01_1.VOB", &title);

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();

    let ifo_lba = fs
        .file_start_lba(&mut img, "/VIDEO_TS/VTS_01_0.IFO")
        .unwrap();
    let vob_lba = fs
        .file_start_lba(&mut img, "/VIDEO_TS/VTS_01_1.VOB")
        .unwrap();
    let menu_lba = fs
        .file_start_lba(&mut img, "/VIDEO_TS/VTS_01_0.VOB")
        .unwrap();

    assert_eq!(
        ifo_lba + ifo_sectors + menu_sectors,
        vob_lba,
        "vtstt_vobs must resolve to VTS_01_1.VOB's first sector"
    );
    assert_eq!(
        ifo_lba + ifo_sectors,
        menu_lba,
        "vtsm_vobs must resolve to VTS_01_0.VOB's first sector"
    );

    // And the bytes at that sector really are the title VOB's.
    let mut buf = vec![0u8; SECTOR];
    img.read_sectors(vob_lba, 1, &mut buf, false).unwrap();
    assert_eq!(buf, title[..SECTOR]);
}

/// Continuation VOBs are one logical stream split at the 1 GB file limit, and
/// cell sector addresses run continuously across the split, so they must be
/// back-to-back with ZERO gap.
#[test]
fn continuation_vobs_are_contiguous() {
    let s = Scratch::new("dvdmulti");
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    s.file("VIDEO_TS/VTS_01_0.IFO", &vts_ifo(SECTOR, 0, 1));
    s.file("VIDEO_TS/VTS_01_1.VOB", &pattern(1, 3 * SECTOR));
    s.file("VIDEO_TS/VTS_01_2.VOB", &pattern(2, 2 * SECTOR));
    s.file("VIDEO_TS/VTS_01_3.VOB", &pattern(3, SECTOR));

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let at = |img: &mut DirImage, n: u32| {
        fs.file_start_lba(img, &format!("/VIDEO_TS/VTS_01_{n}.VOB"))
            .unwrap()
    };
    let (a, b, c) = (at(&mut img, 1), at(&mut img, 2), at(&mut img, 3));
    assert_eq!(b, a + 3, "VTS_01_2.VOB immediately follows _1");
    assert_eq!(c, b + 2, "VTS_01_3.VOB immediately follows _2");
}

/// The negative case: an offset that cannot be satisfied must be a typed
/// error naming the file, NOT a silent misplacement. Here `vtstt_vobs` is 1,
/// which would put the title VOB inside the 2-sector IFO.
#[test]
fn an_unsatisfiable_vob_offset_errors_instead_of_misplacing() {
    let s = Scratch::new("dvdbad");
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    s.file("VIDEO_TS/VTS_01_0.IFO", &vts_ifo(2 * SECTOR, 0, 1));
    s.file("VIDEO_TS/VTS_01_1.VOB", &pattern(1, SECTOR));

    let err = DirImage::open(s.path()).unwrap_err();
    assert_eq!(err.code(), crate::error::E_DIR_IMAGE_PLACEMENT);
    assert!(
        err.to_string().contains("VTS_01_1.VOB"),
        "the error must name the file it could not place, got {err}"
    );
}

/// A gap-inducing offset (bigger than the natural packing) is legal: the
/// planner leaves a hole rather than failing. Real discs pad between title
/// sets.
#[test]
fn an_oversized_vob_offset_leaves_a_gap_rather_than_failing() {
    let s = Scratch::new("dvdgap");
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    s.file("VIDEO_TS/VTS_01_0.IFO", &vts_ifo(SECTOR, 0, 64));
    s.file("VIDEO_TS/VTS_01_1.VOB", &pattern(1, SECTOR));

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let ifo = fs
        .file_start_lba(&mut img, "/VIDEO_TS/VTS_01_0.IFO")
        .unwrap();
    let vob = fs
        .file_start_lba(&mut img, "/VIDEO_TS/VTS_01_1.VOB")
        .unwrap();
    assert_eq!(vob, ifo + 64);
    // The hole reads as zeros.
    let mut buf = vec![0u8; SECTOR];
    img.read_sectors(ifo + 10, 1, &mut buf, false).unwrap();
    assert!(buf.iter().all(|&b| b == 0));
}

/// A file inside a SUBDIRECTORY of `VIDEO_TS` must still have its data placed.
///
/// Audit finding. The DVD branch places only the files directly inside
/// `VIDEO_TS`, because only those carry the IFO-relative constraints — and the
/// follow-up loop skipped that directory entirely, so anything one level deeper
/// got a File Entry declaring the file's real size with no extents behind it.
/// It appeared in the tree at full length and read back as nothing, at exit 0.
/// The identical folder under `BDMV/` was always placed correctly, which is
/// what made the gap easy to miss.
#[test]
fn a_file_below_video_ts_is_placed_not_just_declared() {
    let s = Scratch::new("dvdsubdir");
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    s.file("VIDEO_TS/VTS_01_0.IFO", &vts_ifo(SECTOR, 0, 2));
    s.file("VIDEO_TS/VTS_01_1.VOB", &pattern(1, SECTOR));
    let payload = pattern(7, SECTOR);
    s.file("VIDEO_TS/EXTRA/notes.bin", &payload);

    let mut img = DirImage::open(s.path()).unwrap();
    let fs = udf::read_filesystem(&mut img).unwrap();
    let got = fs
        .read_file(&mut img, "/VIDEO_TS/EXTRA/notes.bin")
        .expect("the file must be readable");
    assert_eq!(
        got, payload,
        "a file below VIDEO_TS must read back as its real contents, not zeros"
    );
}

/// A VOBS offset far past the content must be REFUSED, not honoured.
///
/// Audit finding. The planner honours a title set's declared VOBS offset
/// because honouring it is what makes a real backup readable, and nothing
/// bounded it: a regenerated `.BUP`, a rewritten IFO or a hand-assembled folder
/// naming a huge offset grew the image to wherever it pointed. A `u32` sector
/// count reaches roughly 8.8 TB, and writing that to an `iso://` destination
/// would fill a disk with zeros before anything noticed.
///
/// The companion above pins that a MODEST oversize is still honoured as a gap,
/// so this cap refuses only what it must.
#[test]
fn a_vob_offset_past_the_image_cap_is_refused() {
    let s = Scratch::new("dvdcap");
    s.file("VIDEO_TS/VIDEO_TS.IFO", &vec![0u8; SECTOR]);
    // 100,000,000 sectors is ~200 GB — past the 128 GiB ceiling.
    s.file("VIDEO_TS/VTS_01_0.IFO", &vts_ifo(SECTOR, 0, 100_000_000));
    s.file("VIDEO_TS/VTS_01_1.VOB", &pattern(1, SECTOR));

    let err = DirImage::open(s.path()).expect_err("an image this large must be refused");
    assert!(
        matches!(err, crate::error::Error::DirImageTooLarge),
        "expected the size cap to fire, got {err:?}"
    );
}

/// End to end through the real scanner: a DVD folder must enumerate titles the
/// same way an ISO of the same disc would, which is the whole point of
/// synthesizing a real filesystem rather than faking a tree.
#[test]
fn scan_image_enumerates_a_bdmv_folder() {
    let (s, _, _) = bdmv_scratch();
    let mut img = DirImage::open(s.path()).unwrap();
    let cap = img.capacity_sectors();
    // The playlist here is filler, so no titles are expected — what is being
    // asserted is that the scan reaches the BD enumerator at all (it would
    // return `UdfNotFilesystem` if the synthesized volume did not parse) and
    // reports the structure it found.
    let disc = crate::disc::Disc::scan_image(&mut img, cap, &crate::disc::ScanOptions::default())
        .expect("scan_image must accept a synthesized BDMV image");
    assert!(!disc.encrypted, "a decrypted folder has no AACS directory");
    assert_eq!(disc.content_format, crate::disc::ContentFormat::BdTs);
    assert_eq!(disc.capacity_sectors, cap);
}

// ── Folder-level encryption verdict (`session::scan_dir`) ───────────────────

/// A one-PlayItem MPLS long enough that `parse_playlist` keeps it (it drops
/// anything under 30 s as a menu stub).
fn one_item_mpls(clip_id: &[u8; 5]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&40u32.to_be_bytes()); // playlist_start
    buf.extend_from_slice(&[0u8; 28]); // mark_start placeholder + pad to 40

    let pl = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&1u16.to_be_bytes()); // num_play_items
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

    let mut item = Vec::new();
    item.extend_from_slice(clip_id);
    item.extend_from_slice(b"M2TS");
    item.push(0); // connection condition
    item.extend_from_slice(&[0u8; 2]);
    item.extend_from_slice(&0u32.to_be_bytes()); // in_time
    item.extend_from_slice(&(45_000u32 * 120).to_be_bytes()); // out_time: 2 min
    item.extend_from_slice(&[0u8; 8]); // UO mask
    item.push(0);
    item.push(0);
    item.extend_from_slice(&[0u8; 2]);
    // Empty STN table: length, reserved, eight zero counts, reserved.
    item.extend_from_slice(&16u16.to_be_bytes());
    item.extend_from_slice(&[0u8; 16]);
    buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
    buf.extend_from_slice(&item);

    let pl_len = (buf.len() - pl - 4) as u32;
    buf[pl..pl + 4].copy_from_slice(&pl_len.to_be_bytes());

    let mark_start = buf.len() as u32;
    buf[12..16].copy_from_slice(&mark_start.to_be_bytes());
    buf.extend_from_slice(&2u32.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // no marks
    buf
}

/// A CLPI with only the fields `clpi::parse` needs: magic, zeroed section
/// starts, and the source packet count at 56.
fn minimal_clpi(source_packets: u32) -> Vec<u8> {
    let mut d = vec![0u8; 60];
    d[0..4].copy_from_slice(b"HDMV");
    d[4..8].copy_from_slice(b"0200");
    d[56..60].copy_from_slice(&source_packets.to_be_bytes());
    d
}

/// A BD folder that really enumerates a title, so the AACS content probe has
/// an extent to sample.
///
/// The `.m2ts` is built as real 192-byte BD source packets, because that is
/// what the probe judges: byte 0 carries the AACS Copy Permission Indicator in
/// its top two bits, and byte 4 is the MPEG-TS sync. `scrambled` sets the CPI
/// and withholds the sync — "flagged and not structurally clean", which is
/// exactly `aacs_unit_needs_decrypt`. An all-zero payload would prove nothing
/// either way: `is_clean_ts` skips zero payloads as padding.
fn playable_bdmv(tag: &str, scrambled: bool) -> Scratch {
    let s = Scratch::new(tag);
    let packets = 4096u32;
    let mut m2ts = vec![0x5Au8; packets as usize * 192];
    for p in m2ts.chunks_mut(192) {
        p[0] = if scrambled { 0xC0 } else { 0x00 };
        p[4] = if scrambled { 0xAB } else { 0x47 };
    }
    s.file("BDMV/index.bdmv", &pattern(1, 64));
    s.file("BDMV/PLAYLIST/00000.mpls", &one_item_mpls(b"00000"));
    s.file("BDMV/CLIPINF/00000.clpi", &minimal_clpi(packets));
    s.file("BDMV/STREAM/00000.m2ts", &m2ts);
    s
}

/// The folder must enumerate a title through the real BD scanner — the whole
/// reason for synthesizing a filesystem instead of faking a tree.
#[test]
fn a_bd_folder_enumerates_its_title_through_scan_dir() {
    let s = playable_bdmv("play", false);
    let (disc, _reader) =
        crate::session::scan_dir(s.path(), crate::disc::ScanOptions::default()).unwrap();
    assert_eq!(disc.titles.len(), 1, "the playlist must produce one title");
    assert!(!disc.titles[0].extents.is_empty(), "with real extents");
    assert!(!disc.encrypted);
}

/// A folder that kept its `AACS/` directory but whose content is in the clear
/// must be treated as DECRYPTED. Tree shape claims encryption
/// (`disc/mod.rs:1992`); the content is the evidence that overrides it.
#[test]
fn an_aacs_directory_over_clear_content_is_treated_as_decrypted() {
    let s = playable_bdmv("aacsclear", false);
    s.file("AACS/Unit_Key_RO.inf", &[0u8; 64]);
    s.file("AACS/MKB_RO.inf", &[0u8; 64]);

    // Without the probe this is what the scan alone concludes.
    let mut img = DirImage::open(s.path()).unwrap();
    let cap = img.capacity_sectors();
    let raw =
        crate::disc::Disc::scan_image(&mut img, cap, &crate::disc::ScanOptions::default()).unwrap();
    assert!(raw.encrypted, "tree shape alone says encrypted");

    let (disc, _reader) =
        crate::session::scan_dir(s.path(), crate::disc::ScanOptions::default()).unwrap();
    assert!(
        !disc.encrypted,
        "sampled content units are clear, so the folder is decrypted"
    );
    assert!(disc.aacs_error.is_none(), "and no key is demanded");
}

/// The other verdict: a folder whose content units really are flagged and
/// scrambled is a raw encrypted copy, which `dir://` does not support. It must
/// be a typed error, not a rip that emits garbage.
#[test]
fn an_aacs_folder_with_scrambled_content_is_rejected() {
    let s = playable_bdmv("aacsenc", true);
    s.file("AACS/Unit_Key_RO.inf", &[0u8; 64]);
    s.file("AACS/MKB_RO.inf", &[0u8; 64]);
    let err = match crate::session::scan_dir(s.path(), crate::disc::ScanOptions::default()) {
        Ok(_) => panic!("a scrambled folder must not scan clean"),
        Err(e) => e,
    };
    assert_eq!(err.code(), crate::error::E_DIR_IMAGE_ENCRYPTED);
}

// ── External oracle ─────────────────────────────────────────────────────────

/// Write a synthesized image to a real file and ask the OS to mount it.
///
/// This is the only check here that is not circular: `read_filesystem` shares
/// every assumption with the encoder, an operating system's UDF driver shares
/// none of them. Ignored by default because it shells out to `hdiutil` and
/// needs a host that can attach an image; run with
/// `cargo test -- --ignored write_and_mount_externally --nocapture`.
#[test]
#[ignore = "external: shells out to hdiutil/mount"]
fn write_and_mount_externally() {
    let s = Scratch::new("mount");
    s.file("BDMV/index.bdmv", &pattern(1, 100));
    s.file("BDMV/PLAYLIST/00000.mpls", &pattern(3, 300));
    s.file("BDMV/STREAM/00000.m2ts", &pattern(7, 5000));

    let mut img = DirImage::open(s.path()).unwrap();
    // `.iso`, not `.udf`: hdiutil dispatches on the extension and answers
    // "image not recognized" for a raw sector image it has no handler for —
    // which is a statement about the filename, not about the volume.
    let out = s
        .path()
        .parent()
        .unwrap()
        .join(format!("freemkv-dirimage-{}.iso", std::process::id()));
    let mut f = std::fs::File::create(&out).unwrap();
    let cap = img.capacity_sectors();
    let mut buf = vec![0u8; SECTOR * 64];
    let mut lba = 0u32;
    while lba < cap {
        let n = (cap - lba).min(64) as u16;
        img.read_sectors(lba, n, &mut buf, false).unwrap();
        f.write_all(&buf[..n as usize * SECTOR]).unwrap();
        lba += n as u32;
    }
    f.sync_all().unwrap();
    drop(f);

    println!("image written to {}", out.display());
    let attach = std::process::Command::new("hdiutil")
        .args(["attach", "-nobrowse", "-readonly", "-noverify"])
        .arg(&out)
        .output()
        .expect("hdiutil must be runnable");
    println!(
        "hdiutil attach status={} stdout={} stderr={}",
        attach.status,
        String::from_utf8_lossy(&attach.stdout),
        String::from_utf8_lossy(&attach.stderr)
    );
    let stdout = String::from_utf8_lossy(&attach.stdout).into_owned();
    let mount = stdout
        .lines()
        .find_map(|l| {
            l.split_whitespace()
                .last()
                .filter(|p| p.starts_with("/Volumes/"))
        })
        .map(PathBuf::from);
    // Content, not just "it mounted": the OS's own UDF driver must hand back
    // the same bytes the host file holds, which is the assertion that shares
    // nothing with this crate's parser.
    let same = mount
        .as_ref()
        .map(|m| std::fs::read(m.join("BDMV/STREAM/00000.m2ts")).ok() == Some(pattern(7, 5000)));
    if attach.status.success()
        && let Some(dev) = stdout.split_whitespace().next()
    {
        let _ = std::process::Command::new("hdiutil")
            .args(["detach", dev])
            .output();
    }
    let _ = std::fs::remove_file(&out);
    assert!(
        attach.status.success(),
        "the OS refused to mount the synthesized image"
    );
    assert_eq!(
        same,
        Some(true),
        "the OS mounted the image but read back different bytes"
    );
}

/// Diagnostic (opt-in): verify the DVD placement invariant for every title set
/// in a REAL folder. `ifo.rs` derives a title's extents as
/// `file_start_lba(VTS_nn_0.IFO) + vtstt_vobs`, so that sum must land exactly on
/// `VTS_nn_1.VOB` or the mux reads the wrong sectors for that title.
///
/// Run with: `FMKV_DVD_FOLDER=/path/to/tree cargo test --lib
/// dvd_placement_invariant_on_a_real_folder -- --ignored --nocapture`
#[test]
#[ignore = "diagnostic: needs FMKV_DVD_FOLDER pointing at a real VIDEO_TS tree"]
fn dvd_placement_invariant_on_a_real_folder() {
    let Ok(dir) = std::env::var("FMKV_DVD_FOLDER") else {
        return;
    };
    let mut img = DirImage::open(std::path::Path::new(&dir)).expect("open");
    let fs = udf::read_filesystem(&mut img).expect("udf");
    let mut bad = 0;
    for n in 1..=25u32 {
        let ifo = format!("/VIDEO_TS/VTS_{n:02}_0.IFO");
        let vob = format!("/VIDEO_TS/VTS_{n:02}_1.VOB");
        let (Ok(ifo_lba), Ok(vob_lba)) = (
            fs.file_start_lba(&mut img, &ifo),
            fs.file_start_lba(&mut img, &vob),
        ) else {
            continue;
        };
        let head = fs
            .read_file_prefix(&mut img, &ifo, 0xC8)
            .unwrap_or_default();
        if head.len() < 0xC8 {
            println!("VTS {n:02}: IFO shorter than 0xC8");
            continue;
        }
        let vtstt = u32::from_be_bytes([head[0xC4], head[0xC5], head[0xC6], head[0xC7]]);
        let want = ifo_lba + vtstt;
        if want != vob_lba {
            bad += 1;
        }
        println!(
            "VTS {n:02}: ifo={ifo_lba} vtstt={vtstt} want={want} vob={vob_lba} {}",
            if want == vob_lba { "ok" } else { "MISMATCH" }
        );
    }
    println!("{bad} title set(s) misplaced");
    assert_eq!(
        bad, 0,
        "the placement invariant must hold for every title set"
    );
}

/// Diagnostic (opt-in): dump every title an image scan produces, with the
/// numbers `canonical_title_order` actually sorts on.
///
/// Run: `FMKV_IMAGE=/path/to.iso cargo test --lib dump_titles_for_an_image
/// -- --ignored --nocapture`
#[test]
#[ignore = "diagnostic: needs FMKV_IMAGE"]
fn dump_titles_for_an_image() {
    let Ok(path) = std::env::var("FMKV_IMAGE") else {
        return;
    };
    let (disc, _r) = crate::session::scan_iso(
        std::path::Path::new(&path),
        crate::disc::ScanOptions::default(),
    )
    .expect("scan");
    println!(
        "capacity_bytes={} format={:?} css_error={:?} titles={}",
        disc.capacity_bytes,
        disc.format,
        disc.css.as_ref().map(|c| format!("{:?}", c.crack_span)),
        disc.titles.len()
    );
    for (i, t) in disc.titles.iter().enumerate() {
        println!(
            "  [{i}] playlist={:<16} dur={:>8.2}s size={:>12} extents={} streams={}",
            t.playlist,
            t.duration_secs,
            t.size_bytes,
            t.extents.len(),
            t.streams.len()
        );
        for e in t.extents.iter().take(3) {
            println!(
                "        extent lba={} sectors={}",
                e.start_lba, e.sector_count
            );
        }
    }
}

/// Diagnostic (opt-in): can each VTS IFO be READ from an image?
///
/// `parse_vmg` skips a title set whose `parse_vts` fails, and `parse_vts`
/// begins by reading `/VIDEO_TS/VTS_nn_0.IFO`. This isolates the read.
#[test]
#[ignore = "diagnostic: needs FMKV_IMAGE"]
fn dump_vts_ifo_reads_for_an_image() {
    let Ok(path) = std::env::var("FMKV_IMAGE") else {
        return;
    };
    let mut img =
        crate::io::file_sector_source::FileSectorSource::open(std::path::Path::new(&path))
            .expect("open");
    let fs = udf::read_filesystem(&mut img).expect("udf");
    for n in 1..=20u32 {
        let p = format!("/VIDEO_TS/VTS_{n:02}_0.IFO");
        match fs.read_file(&mut img, &p) {
            Ok(b) => {
                // Hash the CONTENT, not just the length: two IFOs of equal
                // size and magic can still differ, and comparing only length
                // and magic is what made an earlier pass of this diagnostic
                // wrongly report the images as identical.
                let mut h: u64 = 0xcbf2_9ce4_8422_2325;
                for byte in &b {
                    h ^= *byte as u64;
                    h = h.wrapping_mul(0x0000_0100_0000_01b3);
                }
                println!("VTS {n:02}: {} bytes fnv={h:016x}", b.len());
            }
            Err(e) => println!("VTS {n:02}: READ FAILED: {e}"),
        }
    }
}

/// Diagnostic (opt-in): how many title sets survive `parse_vmg`, and how many
/// titles does each carry? Splits "the set was dropped" from "the set parsed
/// but yielded fewer titles".
#[test]
#[ignore = "diagnostic: needs FMKV_IMAGE"]
fn dump_title_sets_for_an_image() {
    let Ok(path) = std::env::var("FMKV_IMAGE") else {
        return;
    };
    let mut img =
        crate::io::file_sector_source::FileSectorSource::open(std::path::Path::new(&path))
            .expect("open");
    let fs = udf::read_filesystem(&mut img).expect("udf");
    let info = crate::ifo::parse_vmg(&mut img, &fs).expect("parse_vmg");
    let total: usize = info.title_sets.iter().map(|ts| ts.titles.len()).sum();
    println!("title_sets={} total_titles={total}", info.title_sets.len());
    for ts in &info.title_sets {
        println!("  vts={} titles={}", ts.vts_number, ts.titles.len());
    }
}

use super::*;

fn patch(offset: u64, b: u8) -> Patch {
    Patch {
        offset,
        bytes: [b, b + 1, b + 2, b + 3, b + 4],
    }
}

// `clip` with `patches` applied the obvious way, clipped at its end.
fn expected(clip: &[u8], patches: &[Patch]) -> Vec<u8> {
    let mut out = clip.to_vec();
    for p in patches {
        for (i, &b) in p.bytes.iter().enumerate() {
            if let Some(slot) = out.get_mut(p.offset as usize + i) {
                *slot = b;
            }
        }
    }
    out
}

#[test]
fn a_patch_inside_one_buffer_is_written_whole() {
    let mut buf = vec![0u8; 64];
    assert_eq!(apply_patches(&[patch(1000 + 10, 0x10)], 1000, &mut buf), 1);
    assert_eq!(&buf[10..15], &[0x10, 0x11, 0x12, 0x13, 0x14]);
    assert!(buf[..10].iter().chain(&buf[15..]).all(|&b| b == 0));
}

#[test]
fn a_patch_straddling_a_buffer_boundary_is_split_across_both() {
    let p = [patch(62, 0x20)];
    let mut a = vec![0u8; 64];
    let mut b = vec![0u8; 64];
    assert_eq!(apply_patches(&p, 0, &mut a), 1);
    assert_eq!(apply_patches(&p, 64, &mut b), 1);
    assert_eq!(&a[62..], &[0x20, 0x21]);
    assert_eq!(&b[..3], &[0x22, 0x23, 0x24]);
    assert!(b[3..].iter().all(|&x| x == 0));
}

#[test]
fn any_split_of_a_clip_into_buffers_patches_it_the_same() {
    let clip: Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
    let patches = [
        patch(0, 1),
        patch(190, 2),
        patch(767, 3),
        patch(768, 4),
        patch(3998, 5),
    ];
    let want = expected(&clip, &patches);
    for size in [1usize, 2, 3, 5, 7, 64, 192, 1000, 4000] {
        let mut got = clip.clone();
        for (i, chunk) in got.chunks_mut(size).enumerate() {
            apply_patches(&patches, (i * size) as u64, chunk);
        }
        assert_eq!(got, want, "buffers of {size}");
    }
}

#[test]
fn patches_past_the_buffer_leave_it_alone() {
    let mut buf = vec![7u8; 100];
    assert_eq!(
        apply_patches(&[patch(100, 9), patch(5000, 9)], 0, &mut buf),
        0
    );
    assert!(buf.iter().all(|&b| b == 7));
}

const S: u64 = SECTOR_BYTES as u64;

// A clip whose first 2 sectors sit at LBA 10 and the rest (3 sectors, 1000 bytes used) at
// LBA 40, over a 64-sector disc of `fill`.
fn split_clip(fill: u8) -> (ClipFile, Vec<u8>) {
    let file = ClipFile {
        clip: 1,
        extents: vec![(10, 2 * S), (40, 2 * S + 1000)],
    };
    (file, vec![fill; 64 * SECTOR_BYTES])
}

// Read `disc` in `batch`-sector reads through `fx`.
fn read_all(fx: &DiscFixups, disc: &[u8], batch: usize) -> Vec<u8> {
    let mut out = disc.to_vec();
    for (i, chunk) in out.chunks_mut(batch * SECTOR_BYTES).enumerate() {
        fx.apply((i * batch) as u32, chunk);
    }
    out
}

#[test]
fn patches_land_on_the_disc_through_multi_extent_clips() {
    let (file, disc) = split_clip(0x47);
    // One patch across the extent boundary (file offset 2S-2), one inside the second extent.
    let patches = [patch(2 * S - 2, 0x30), patch(2 * S + 500, 0x40)];
    let (fx, dropped) = DiscFixups::build([(&file, &patches[..])]);
    assert_eq!(dropped, 0);
    let mut want = disc.clone();
    let at = |lba: u64, off: u64| (lba * S + off) as usize;
    want[at(11, S - 2)..at(12, 0)].copy_from_slice(&[0x30, 0x31]);
    want[at(40, 0)..at(40, 3)].copy_from_slice(&[0x32, 0x33, 0x34]);
    want[at(40, 500)..at(40, 505)].copy_from_slice(&[0x40, 0x41, 0x42, 0x43, 0x44]);
    for batch in [1, 3, 7, 64] {
        assert_eq!(read_all(&fx, &disc, batch), want, "batch {batch}");
    }
    assert_eq!(fx.applied(), 8, "two patches in each of four passes");
}

#[test]
fn patches_past_the_file_end_are_dropped_or_clipped() {
    let (file, disc) = split_clip(0x47);
    let size = 4 * S + 1000;
    let patches = [
        patch(size - 2, 0x50),
        patch(size, 0x60),
        patch(size + 900, 0x70),
    ];
    let (fx, dropped) = DiscFixups::build([(&file, &patches[..])]);
    assert_eq!(dropped, 2);
    let got = read_all(&fx, &disc, 64);
    let end = (42 * S + 1000) as usize;
    assert_eq!(&got[end - 2..end], &[0x50, 0x51]);
    assert!(
        got[end..].iter().all(|&b| b == 0x47),
        "nothing past the file's last byte"
    );
}

#[test]
fn a_patch_in_a_packet_without_ts_sync_is_skipped() {
    let (file, mut disc) = split_clip(0x47);
    // Packet 0 lost its sync (a blanked unit): its patch is skipped, packet 1's applies.
    disc[(10 * S + 4) as usize] = 0;
    let patches = [patch(20, 0x80), patch(192 + 20, 0x90)];
    let (fx, _) = DiscFixups::build([(&file, &patches[..])]);
    let got = read_all(&fx, &disc, 64);
    let base = (10 * S) as usize;
    assert_eq!(&got[base + 20..base + 25], &[0x47; 5]);
    assert_eq!(
        &got[base + 212..base + 217],
        &[0x90, 0x91, 0x92, 0x93, 0x94]
    );
    assert_eq!(fx.applied(), 1);
}

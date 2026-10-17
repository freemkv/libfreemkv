use super::*;

// Build fixtures directly from the documented wire fields. Entries are index
// array first, descriptor array second (not an interleaved host struct).
fn fixture(entries: &[(u32, [u8; 16])], key: [u8; 16]) -> Vec<u8> {
    let mut bytes = vec![0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 12];
    bytes.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (index, _) in entries {
        bytes.extend_from_slice(&index.to_be_bytes());
    }
    for (_, descriptor) in entries {
        bytes.extend(descriptor.iter().zip(key).map(|(byte, xor)| byte ^ xor));
    }
    bytes
}

fn descriptor(flags: u8) -> [u8; 16] {
    // adjust0 = 2, adjust1 = 3; payload offsets 187 and 8.
    [flags, 0, 0x20, 3, 187, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
}

#[test]
fn compute_sp_resolves_offsets_and_cross_buffer_fragments() {
    let key = [0xa5; 16];
    let mut table = BdPlusFixupTable::parse(&fixture(&[(30, descriptor(0x40))], key)).unwrap();
    assert_eq!(table.unresolved_segments().collect::<Vec<_>>(), [(7, 0)]);
    assert!(matches!(
        table.patches_for_clip(7),
        Err(BdPlusError::UnresolvedSegment { .. })
    ));
    table.resolve_segment(7, 0, &key, &[0; 8]).unwrap();
    let set = table.patches_for_clip(7).unwrap();
    assert_eq!(set.patches()[0].offset(), 32 * 192 + 187);
    assert_eq!(set.patches()[1].offset(), 35 * 192 + 8);
    let mut whole = vec![0; 40 * 192];
    assert_eq!(set.apply(0, &mut whole).unwrap(), 2);
    let mut split = vec![0; whole.len()];
    for (index, chunk) in split.chunks_mut(6333).enumerate() {
        set.apply((index * 6333) as u64, chunk).unwrap();
    }
    assert_eq!(split, whole);
    assert_eq!(&whole[6331..6336], [1, 2, 3, 4, 5]);
    // Re-reading/seeking the same range gives identical bytes.
    let mut retry = [0; 4];
    assert_eq!(set.apply(6332, &mut retry).unwrap(), 1);
    assert_eq!(retry, [2, 3, 4, 5]);
    assert!(matches!(
        table.resolve_segment(7, 0, &key, &[0; 8]),
        Err(BdPlusError::SegmentAlreadyResolved { .. })
    ));
}

#[test]
fn forensic_mask_uses_big_endian_bit_numbering() {
    for bit in [0u8, 7, 8, 31, 63] {
        for enabled in [false, true] {
            let mut mask = [0; 8];
            if enabled {
                mask[7 - (bit / 8) as usize] |= 1 << (bit % 8);
            }
            let mut table =
                BdPlusFixupTable::parse(&fixture(&[(1, descriptor(0x80 | bit))], [0; 16])).unwrap();
            table.resolve_segment(7, 0, &[0; 16], &mask).unwrap();
            assert_eq!(
                table.patches_for_clip(7).unwrap().patches().len(),
                if enabled { 2 } else { 0 }
            );
        }
    }
}

#[test]
fn malformed_resolution_is_atomic_and_inactive_entries_do_not_patch() {
    let mut table = BdPlusFixupTable::parse(&fixture(
        &[(1, descriptor(0x40)), (20, descriptor(0xc0))],
        [0; 16],
    ))
    .unwrap();
    assert!(matches!(
        table.resolve_segment(7, 0, &[0; 16], &[0; 8]),
        Err(BdPlusError::DescriptorType { entry: 1, .. })
    ));
    assert_eq!(table.unresolved_segments().count(), 1);
    let mut inactive = BdPlusFixupTable::parse(&fixture(&[(1, descriptor(0))], [0; 16])).unwrap();
    inactive.resolve_segment(7, 0, &[0; 16], &[255; 8]).unwrap();
    assert!(inactive.patches_for_clip(7).unwrap().patches().is_empty());
}

#[test]
fn parser_rejects_truncation_aliases_trailing_data_and_hostile_counts() {
    let good = fixture(&[(1, descriptor(0x40)), (20, descriptor(0x40))], [0; 16]);
    for len in 0..good.len() {
        assert!(
            BdPlusFixupTable::parse(&good[..len]).is_err(),
            "prefix {len}"
        );
    }
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(BdPlusFixupTable::parse(&trailing).is_err());
    let mut alias = good.clone();
    alias[8..12].copy_from_slice(&8u32.to_be_bytes());
    assert!(BdPlusFixupTable::parse(&alias).is_err());
    let mut count = good;
    count[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        BdPlusFixupTable::parse(&count),
        Err(BdPlusError::ResourceLimit { .. })
    ));
}

#[test]
fn absent_empty_unresolved_and_overlapping_are_distinct() {
    let table = BdPlusFixupTable::parse(&[0, 1, 0, 0, 0, 7, 0, 0]).unwrap();
    assert!(table.patches_for_clip(7).unwrap().patches().is_empty());
    assert!(matches!(
        table.patches_for_clip(8),
        Err(BdPlusError::MissingTable { .. })
    ));
    let mut overlap = descriptor(0x40);
    overlap[3] = 0;
    overlap[4] = 8;
    let mut table = BdPlusFixupTable::parse(&fixture(&[(1, overlap)], [0; 16])).unwrap();
    assert!(matches!(
        table.resolve_segment(7, 0, &[0; 16], &[0; 8]),
        Err(BdPlusError::DescriptorAddress { .. })
    ));
    assert_eq!(table.unresolved_segments().count(), 1);
    let mut duplicates = BdPlusFixupTable::parse(&fixture(
        &[(1, descriptor(0x40)), (1, descriptor(0x40))],
        [0; 16],
    ))
    .unwrap();
    assert!(matches!(
        duplicates.resolve_segment(7, 0, &[0; 16], &[0; 8]),
        Err(BdPlusError::OverlappingPatches { .. })
    ));
}

#[test]
fn huge_m2ts_indexes_do_not_wrap_and_overflow_does_not_mutate() {
    let mut table =
        BdPlusFixupTable::parse(&fixture(&[(u32::MAX, descriptor(0x40))], [0; 16])).unwrap();
    table.resolve_segment(7, 0, &[0; 16], &[0; 8]).unwrap();
    let set = table.patches_for_clip(7).unwrap();
    assert!(set.patches()[0].offset() > u32::MAX as u64);
    let mut bytes = [42; 8];
    assert_eq!(
        set.apply(u64::MAX, &mut bytes),
        Err(BdPlusError::OffsetOverflow)
    );
    assert_eq!(bytes, [42; 8]);
}

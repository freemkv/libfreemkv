use super::*;
use crate::error::E_BDPLUS_TABLE_INVALID;

/// An entry with `flags`, adjusts, in-packet offsets and fill bytes `p0` / `p1`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn entry(
    index: u32,
    flags: u8,
    a0: u16,
    a1: u16,
    o0: u8,
    o1: u8,
    p0: u8,
    p1: u8,
) -> Entry {
    Entry {
        index,
        flags,
        adjust0: a0,
        adjust1: a1,
        offset0: o0,
        offset1: o1,
        patch0: [p0; PATCH_LEN],
        patch1: [p1; PATCH_LEN],
    }
}

/// A plain (decrypted, unmasked) segment.
pub(crate) fn seg(entries: Vec<Entry>) -> Segment {
    Segment {
        entries,
        encrypted: false,
        mask: None,
    }
}

/// `table` in the convtab.bin layout: per table its header, then its segments in order.
pub(crate) fn encode(table: &ConversionTable) -> Vec<u8> {
    let mut out = (table.clips.len() as u16).to_be_bytes().to_vec();
    for t in &table.clips {
        out.extend_from_slice(&t.clip.to_be_bytes());
        out.extend_from_slice(&(t.segments.len() as u16).to_be_bytes());
        let mut at = out.len() + 4 * t.segments.len();
        for s in &t.segments {
            out.extend_from_slice(&(at as u32).to_be_bytes());
            at += 4 + 20 * s.entries.len();
        }
        for s in &t.segments {
            out.extend_from_slice(&(s.entries.len() as u32).to_be_bytes());
            for e in &s.entries {
                out.extend_from_slice(&e.index.to_be_bytes());
            }
            for e in &s.entries {
                let adj = (u32::from(e.adjust0) << 12) | u32::from(e.adjust1);
                out.push(e.flags);
                out.extend_from_slice(&adj.to_be_bytes()[1..]);
                out.extend_from_slice(&[e.offset0, e.offset1]);
                out.extend_from_slice(&e.patch0);
                out.extend_from_slice(&e.patch1);
            }
        }
    }
    out
}

fn one_clip(clip: u32, entries: Vec<Entry>) -> ConversionTable {
    ConversionTable {
        clips: vec![ClipTable {
            clip,
            segments: vec![seg(entries)],
        }],
    }
}

fn code<T: std::fmt::Debug>(r: Result<T>) -> u16 {
    r.unwrap_err().code()
}

#[test]
fn patch_offsets_count_source_packets_from_the_index() {
    let e = entry(0x9900A, 0x40, 0x1AA, 0x053, 0x64, 0xB1, 0, 0);
    assert_eq!(e.patch0_offset(), 0x72D_4764);
    assert_eq!(e.patch1_offset(), 0x72D_85F1);
    assert_eq!(e.kind(), EntryKind::Apply);
}

#[test]
fn a_valid_table_round_trips_and_yields_sorted_patches() {
    let t = ConversionTable {
        clips: vec![
            ClipTable {
                clip: 1,
                segments: vec![
                    seg(vec![
                        entry(10, 0x40, 0, 2, 8, 187, 0xA1, 0xA2),
                        entry(20, 0x40, 1, 0, 100, 120, 0xB1, 0xB2),
                    ]),
                    seg(vec![]),
                ],
            },
            ClipTable {
                clip: 7,
                segments: vec![],
            },
            ClipTable {
                clip: 3,
                segments: vec![seg(vec![entry(0, 0x40, 0, 0, 50, 60, 1, 2)])],
            },
        ],
    };
    let parsed = ConversionTable::parse_activated(&encode(&t)).unwrap();
    assert_eq!(parsed, t);
    let set = parsed.clip(1).unwrap().patches();
    assert_eq!((set.entries, set.rejected, set.encrypted), (2, 0, 0));
    let offs: Vec<u64> = set.patches.iter().map(|p| p.offset).collect();
    assert_eq!(
        offs,
        [10 * 192 + 8, 12 * 192 + 187, 21 * 192 + 100, 21 * 192 + 120]
    );
    assert_eq!(set.patches[1].bytes, [0xA2; 5]);
    assert!(parsed.clip(7).unwrap().patches().patches.is_empty());
    assert_eq!(parsed.clip(3).unwrap().patches().entries, 1);
    assert!(parsed.clip(2).is_none());
}

#[test]
fn every_truncation_is_refused() {
    let t = one_clip(
        1,
        vec![
            entry(1, 0x40, 0, 1, 9, 9, 1, 2),
            entry(5, 0x40, 0, 1, 9, 9, 3, 4),
        ],
    );
    let bytes = encode(&t);
    for n in 0..bytes.len() {
        assert_eq!(
            code(ConversionTable::parse_activated(&bytes[..n])),
            E_BDPLUS_TABLE_INVALID,
            "{n}"
        );
    }
    assert!(ConversionTable::parse_activated(&bytes).is_ok());
}

#[test]
fn a_segment_offset_or_entry_count_past_the_end_is_refused() {
    let mut bytes = encode(&one_clip(1, vec![entry(1, 0x40, 0, 1, 9, 9, 1, 2)]));
    let mut far = bytes.clone();
    far[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        code(ConversionTable::parse_activated(&far)),
        E_BDPLUS_TABLE_INVALID
    );
    bytes[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        code(ConversionTable::parse_activated(&bytes)),
        E_BDPLUS_TABLE_INVALID
    );
}

#[test]
fn an_empty_table_parses_and_an_oversized_one_is_refused() {
    assert_eq!(
        ConversionTable::parse_activated(&[0, 0]).unwrap(),
        ConversionTable::default()
    );
    let big = vec![0u8; MAX_TABLE_BYTES + 1];
    let e = ConversionTable::parse_activated(&big).unwrap_err();
    assert!(matches!(e, Error::BdPlusTableInvalid { kind: "too_large" }));
}

#[test]
fn offsets_outside_the_ts_payload_are_rejected() {
    let set = one_clip(
        1,
        vec![
            entry(0, 0x40, 0, 1, 7, 100, 1, 1),
            entry(10, 0x40, 0, 1, 100, 188, 2, 2),
            entry(20, 0x40, 0, 1, 8, 187, 3, 3),
        ],
    )
    .clips[0]
        .patches();
    assert_eq!((set.entries, set.rejected), (1, 2));
    assert_eq!(set.patches[0].offset, 20 * 192 + 8);
}

#[test]
fn unsorted_entries_are_rejected_one_by_one() {
    let set = one_clip(
        1,
        vec![
            entry(100, 0x40, 0, 1, 50, 50, 1, 1),
            // Patch 0 before the previous applied entry's patch 1.
            entry(100, 0x40, 0, 2, 60, 60, 2, 2),
            // Patch 1 not after patch 0.
            entry(200, 0x40, 0, 0, 90, 80, 3, 3),
            entry(300, 0x40, 0, 1, 9, 9, 4, 4),
        ],
    )
    .clips[0]
        .patches();
    assert_eq!((set.entries, set.rejected), (2, 2));
    assert_eq!(
        set.patches.iter().map(|p| p.bytes[0]).collect::<Vec<_>>(),
        [1, 1, 4, 4]
    );
}

#[test]
fn flags_select_what_an_activated_table_applies() {
    let t = one_clip(
        1,
        vec![
            entry(0, 0x00, 0, 1, 9, 9, 0, 0),
            entry(10, 0x40, 0, 1, 9, 9, 1, 1),
            entry(20, 0x85, 0, 1, 9, 9, 2, 2),
            entry(30, 0xC0, 0, 1, 9, 9, 3, 3),
        ],
    );
    assert_eq!(
        t.clips[0].segments[0].entries[0].kind(),
        EntryKind::Inactive
    );
    assert_eq!(
        t.clips[0].segments[0].entries[2].kind(),
        EntryKind::Forensic { bit: 5 }
    );
    assert_eq!(t.clips[0].segments[0].entries[3].kind(), EntryKind::Invalid);
    let set = t.clips[0].patches();
    assert_eq!((set.entries, set.rejected), (2, 0));
    assert_eq!(
        set.patches.iter().map(|p| p.bytes[0]).collect::<Vec<_>>(),
        [1, 1, 2, 2]
    );
}

#[test]
fn a_forensic_entry_applies_iff_its_mask_bit_is_set() {
    let mut s = seg(vec![
        entry(0, 0x80, 0, 1, 9, 9, 0, 0),
        entry(10, 0x80 | 63, 0, 1, 9, 9, 63, 63),
        entry(20, 0x80 | 9, 0, 1, 9, 9, 9, 9),
    ]);
    let mut mask = [0u8; 8];
    mask[7] = 0x01;
    mask[0] = 0x80;
    s.mask = Some(mask);
    let t = ClipTable {
        clip: 1,
        segments: vec![s],
    };
    let set = t.patches();
    assert_eq!(set.entries, 2, "bits 0 and 63 set, bit 9 clear");
    assert_eq!(
        set.patches.iter().map(|p| p.bytes[0]).collect::<Vec<_>>(),
        [0, 0, 63, 63]
    );
}

#[test]
fn a_delivered_table_keeps_its_segments_encrypted() {
    let t = ConversionTable {
        clips: vec![ClipTable {
            clip: 1,
            segments: vec![seg(vec![entry(0, 0x40, 0, 1, 9, 9, 1, 1)]), seg(vec![])],
        }],
    };
    let parsed = ConversionTable::parse_delivered(&encode(&t)).unwrap();
    assert!(parsed.clips[0].segments[0].encrypted);
    assert!(!parsed.clips[0].segments[1].encrypted);
    let set = parsed.clips[0].patches();
    assert_eq!((set.entries, set.encrypted, set.patches.len()), (0, 1, 0));
}

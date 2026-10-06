use super::*;

// Decode an SCR from a pack header, per MS-2's bit layout.
fn scr_of(h: &[u8]) -> (u64, u64) {
    let b = |i: usize| u64::from(h[i]);
    let base = ((b(4) >> 3) & 7) << 30
        | (b(4) & 3) << 28
        | b(5) << 20
        | (b(6) >> 3) << 15
        | (b(6) & 3) << 13
        | b(7) << 5
        | b(8) >> 3;
    let ext = (b(8) & 3) << 7 | b(9) >> 1;
    (base, ext)
}

// MS-2/MS-3/MS-4 guard: layout, markers and SCR = base × 300 + ext. Per spec; do not
// change without a spec citation proving otherwise.
#[test]
fn pack_header_layout_markers_and_scr() {
    let scr = 8_589_934_591 * 300 + 299; // the largest SCR
    let h = pack_header(scr, 25_200, 3);
    assert_eq!(h.len(), PACK_HEADER_BYTES + 3);
    assert_eq!(&h[..4], &[0, 0, 1, 0xBA]);
    assert_eq!(h[4] >> 6, 0b01, "'01'");
    assert_eq!(h[4] & 0x04, 0x04);
    assert_eq!(h[6] & 0x04, 0x04);
    assert_eq!(h[8] & 0x04, 0x04);
    assert_eq!(h[9] & 0x01, 0x01);
    assert_eq!(h[12] & 0x03, 0x03, "two marker bits after program_mux_rate");
    assert_eq!(h[13], 0xF8 | 3, "reserved 11111 + pack_stuffing_length");
    assert_eq!(&h[14..], &[0xFF; 3]);
    assert_eq!(scr_of(&h), (8_589_934_591, 299));
    let rate = u32::from(h[10]) << 14 | u32::from(h[11]) << 6 | u32::from(h[12]) >> 2;
    assert_eq!(rate, 25_200);
    assert_eq!(scr_of(&pack_header(12_345 * 300 + 7, 1, 0)), (12_345, 7));
}

// MS-5/MS-6/MS-7 guard: every flag 0, reserved_bits '111 1111', one bound per stream.
#[test]
fn system_header_fields_and_bounds() {
    let b = [
        Bound {
            stream_id: 0xE0,
            scale_1024: true,
            size: 232,
        },
        Bound {
            stream_id: 0xC0,
            scale_1024: false,
            size: 128,
        },
        Bound {
            stream_id: 0xBD,
            scale_1024: true,
            size: 8191,
        },
    ];
    let h = system_header(3, 1, &b);
    assert_eq!(&h[..4], &[0, 0, 1, 0xBB]);
    assert_eq!(usize::from(u16::from_be_bytes([h[4], h[5]])), h.len() - 6);
    let rate_bound = (u32::from(h[6]) & 0x7F) << 15 | u32::from(h[7]) << 7 | u32::from(h[8]) >> 1;
    assert_eq!(rate_bound, RATE_BOUND);
    assert_eq!(h[6] & 0x80, 0x80);
    assert_eq!(h[8] & 1, 1);
    assert_eq!(h[9], 3 << 2, "audio_bound 3, fixed_flag 0, CSPS_flag 0");
    assert_eq!(h[10], 0x21, "lock flags 0, marker, video_bound 1");
    assert_eq!(
        h[11], 0x7F,
        "packet_rate_restriction_flag 0, reserved_bits '111 1111'"
    );
    assert_eq!(&h[12..15], &[0xE0, 0xE0, 232]);
    assert_eq!(&h[15..18], &[0xC0, 0xC0, 128]);
    assert_eq!(&h[18..21], &[0xBD, 0xE0 | (8191 >> 8) as u8, 0xFF]);
}

// MS-9/MS-26 guard: a map followed by its CRC_32 leaves the Annex A decoder at zero.
#[test]
fn psm_crc_gives_a_zero_decoder_output_and_the_length_is_capped() {
    let e = [PsmEntry {
        stream_type: 0x02,
        stream_id: 0xE0,
        descriptors: vec![],
    }];
    let m = psm(&[], &e).unwrap();
    assert_eq!(crc32(&m), 0, "zero residue over map + CRC");
    assert_eq!(&m[..4], &[0, 0, 1, 0xBC]);
    assert_eq!(usize::from(u16::from_be_bytes([m[4], m[5]])), m.len() - 6);
    assert_eq!(m[6], 0xE0, "current_next_indicator 1, version 0");
    assert_eq!(
        crc32(b"123456789"),
        0x0376_E6E7,
        "CRC-32/MPEG-2 check value"
    );
    let big = [PsmEntry {
        stream_type: 0x06,
        stream_id: 0xBD,
        descriptors: vec![0; 1010],
    }];
    assert!(
        psm(&[], &big).is_none(),
        "program_stream_map_length ≤ 1018 (MS-8)"
    );
}

// MS-8 boundary: one entry makes length = 14 + descriptors; 1018 fits, 1019 does not.
#[test]
fn psm_length_cap_is_exactly_1018() {
    let with = |n: usize| {
        psm(
            &[],
            &[PsmEntry {
                stream_type: 0x06,
                stream_id: 0xBD,
                descriptors: vec![0; n],
            }],
        )
    };
    let at = with(1018 - 14).expect("length 1018 is allowed");
    assert_eq!(at.len() - 6, 1018);
    assert!(with(1018 - 14 + 1).is_none(), "length 1019 is over the cap");
    assert_eq!(MAX_PSM_LENGTH, 1018);
}

// MS-23 guard: Table 2-44 values 5 and 15, the base's embedded index undefined.
#[test]
fn hierarchy_descriptor_values() {
    assert_eq!(
        hierarchy_descriptor(HIERARCHY_BASE, 0, 9),
        [4, 4, 0xFF, 0xC0, 0xFF, 0xC0]
    );
    assert_eq!(
        hierarchy_descriptor(HIERARCHY_EXTENSION, 1, 0),
        [4, 4, 0xF5, 0xC1, 0xC0, 0xC0]
    );
    assert_eq!(iso639_descriptor("ENG"), Some([10, 4, b'e', b'n', b'g', 0]));
    assert_eq!(iso639_descriptor(""), None);
    assert_eq!(iso639_descriptor("en"), None);
}

// MS-10/MS-11/MS-12 guard: flags, header length, bounded length, timestamp prefixes.
#[test]
fn pes_header_fields() {
    let f = PesFields {
        pts: Some(0x1_2345_6789),
        dts: Some(90_000),
        pstd: Some((true, 232)),
        data_alignment: true,
    };
    let h = pes_header(0xE0, &f, 100);
    assert_eq!(h.len(), pes_header_len(&f));
    assert_eq!(&h[..4], &[0, 0, 1, 0xE0]);
    assert_eq!(
        usize::from(u16::from_be_bytes([h[4], h[5]])),
        h.len() - 6 + 100
    );
    assert_eq!(
        h[6], 0x85,
        "'10', data_alignment_indicator, original_or_copy"
    );
    assert_eq!(h[7], 0xC1, "PTS_DTS_flags '11', PES_extension_flag");
    assert_eq!(h[8], 13);
    assert_eq!(h[9] >> 4, 0b0011);
    assert_eq!(h[14] >> 4, 0b0001);
    assert_eq!(h[19], 0x1E, "only P-STD_buffer_flag, reserved '111'");
    assert_eq!(h[20] >> 6, 0b01);
    assert_eq!(u16::from(h[20] & 0x1F) << 8 | u16::from(h[21]), 232);
    assert_eq!(h[20] & 0x20, 0x20, "scale 1");
    let bare = pes_header(0xC0, &PesFields::default(), 10);
    assert_eq!(&bare[6..], &[0x81, 0x00, 0x00]);
    assert_eq!(padding_pes(6), vec![0, 0, 1, 0xBE, 0, 0]);
    assert_eq!(padding_pes(9), vec![0, 0, 1, 0xBE, 0, 3, 0xFF, 0xFF, 0xFF]);
}

#[test]
fn fmkv_table_splits_under_255_bytes() {
    let subs: Vec<SubStreamInfo> = (0..64)
        .map(|i| SubStreamInfo {
            sub_id: 0x20 + (i % 32) as u8,
            lang: *b"eng",
            forced: i == 3,
        })
        .collect();
    let d = fmkv_descriptors(&subs, Some(&[[1, 2, 3]; 16]));
    let mut at = 0;
    let mut rows = 0;
    let mut palette = false;
    while at < d.len() {
        assert_eq!(d[at], FMKV_TAG);
        let len = usize::from(d[at + 1]);
        assert_eq!(&d[at + 2..at + 6], FMKV_MAGIC);
        match d[at + 7] {
            FMKV_SUBSTREAMS => rows += (len - 6) / 5,
            FMKV_PALETTE => palette = true,
            t => panic!("type {t}"),
        }
        at += 2 + len;
    }
    assert_eq!((rows, palette), (64, true));
    // Worst case (64 sub-streams + palette) fits the map with room to spare (§2.2).
    assert!(psm(&d, &[]).is_some_and(|m| m.len() < 500));
}

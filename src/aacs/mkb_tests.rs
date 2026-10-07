use super::*;

// One stride resolver: cert, then a pre-recorded MKB's type, then index.bdmv, then UHD.
// Each later signal is consulted only when every earlier one is missing.
#[test]
fn resolve_aacs_version_ranks_cert_then_mkb_then_index() {
    let rec = |t: u32| {
        let mut m = vec![0x10, 0x00, 0x00, 0x0C];
        m.extend_from_slice(&t.to_be_bytes());
        m.extend_from_slice(&[0, 0, 0, 1]);
        m
    };
    let (bd, uhd21) = (rec(MKB_TYPE_4_PRERECORDED), rec(MKB_21_CATEGORY_C));
    let class2 = rec(MKB_TYPE_10_CLASS_II);
    use AacsVersion::*;
    // The certificate wins over a disagreeing MKB and index.
    assert_eq!(
        resolve_aacs_version(Some(AACS_MAJOR_BD), &uhd21, Some(true)),
        V10
    );
    assert_eq!(
        resolve_aacs_version(Some(AACS_MAJOR_UHD), &bd, Some(false)),
        V20
    );
    // No cert: the MKB type wins over a disagreeing index.
    assert_eq!(resolve_aacs_version(None, &bd, Some(true)), V10);
    assert_eq!(resolve_aacs_version(None, &uhd21, Some(false)), V21);
    // No cert, no pre-recorded MKB type: index.bdmv decides.
    assert_eq!(resolve_aacs_version(None, &[], Some(false)), V10);
    assert_eq!(resolve_aacs_version(None, &class2, Some(false)), V10);
    assert_eq!(resolve_aacs_version(None, &[], Some(true)), V20);
    // Nothing at all: UHD's 64-byte stride.
    assert_eq!(resolve_aacs_version(None, &[], None), V20);
}

/// One MKB record: 1 type byte + big-endian 24-bit total length + body.
fn rec(rec_type: u8, body: &[u8]) -> Vec<u8> {
    let len = 4 + body.len();
    let mut v = vec![rec_type, (len >> 16) as u8, (len >> 8) as u8, len as u8];
    v.extend_from_slice(body);
    v
}

/// Type-and-Version record (0x10): body = 4-byte MKBType + 4-byte version.
fn type_and_version(mkb_type: u32, version: u32) -> Vec<u8> {
    let mut body = mkb_type.to_be_bytes().to_vec();
    body.extend_from_slice(&version.to_be_bytes());
    rec(REC_TYPE_AND_VERSION, &body)
}

#[test]
fn walker_frames_records_and_stops_at_end_marker() {
    let mut mkb = type_and_version(MKB_20_CATEGORY_C, 77);
    mkb.extend(rec(REC_VKD_TABLE, &[0xAA; 16]));
    mkb.extend([0x00, 0x00, 0x00, 0x00]); // end marker
    mkb.extend(rec(0x99, &[0xFF; 8])); // must NOT be walked (past the marker)

    let recs = walk_mkb(&mkb);
    assert_eq!(recs.len(), 2, "walk stops at the 00 000000 end marker");
    assert_eq!(recs[0].rec_type, REC_TYPE_AND_VERSION);
    assert_eq!(recs[1].rec_type, REC_VKD_TABLE);
    assert_eq!(recs[1].body, vec![0xAA; 16]);
}

#[test]
fn walker_stops_on_malformed_or_out_of_bounds_length() {
    // A record whose declared length runs past the buffer end must terminate
    // the walk rather than panic or read OOB.
    let mkb = vec![REC_VKD_TABLE, 0x00, 0xFF, 0xFF, 0x01, 0x02]; // len=0xFFFF, only 6 bytes
    assert!(
        walk_mkb(&mkb).is_empty(),
        "over-long record yields no records"
    );
    // A sub-4 length (shorter than the header itself) is also rejected.
    let short = vec![REC_VKD_TABLE, 0x00, 0x00, 0x02];
    assert!(walk_mkb(&short).is_empty(), "sub-4 length is rejected");
    // A truncated header (< 4 bytes) yields nothing.
    assert!(walk_mkb(&[0x10, 0x00]).is_empty());
}

#[test]
fn mkb_type_and_version_decode_from_the_type_record() {
    let mut mkb = type_and_version(MKB_21_CATEGORY_C, 100);
    mkb.extend([0x00, 0x00, 0x00, 0x00]);
    assert_eq!(mkb_type_raw(&mkb), Some(MKB_21_CATEGORY_C));
    assert_eq!(mkb_version(&mkb), Some(100));
    assert_eq!(mkb_is_uhd(&mkb), Some(true), "2.1 Category C is UHD");

    let bd = type_and_version(MKB_TYPE_4_PRERECORDED, 68);
    assert_eq!(
        mkb_is_uhd(&bd),
        Some(false),
        "AACS 1.0 prerecorded is not UHD"
    );
    // No Type record → None (not a panic, not a fabricated value).
    assert_eq!(mkb_version(&rec(REC_VKD_TABLE, &[0; 16])), None);
    assert_eq!(mkb_type_raw(&[]), None);
}

// A genuinely-old AACS 1.0 Blu-ray really does carry MKB version 1 (the "50
// First Dates" disc, MKB 12628 bytes, is a real example). `mkb_version` must
// read that `00 00 00 01` verbatim — never reject/clamp a low value as garbage.
#[test]
fn mkb_version_reads_a_genuine_version_one_prerecorded_mkb() {
    // Realistic early-BD layout: Type-and-Version (type 0x00041003, v1)
    // followed by another framed record, then the end marker.
    let mut mkb = type_and_version(MKB_TYPE_4_PRERECORDED, 1);
    mkb.extend(rec(REC_MEDIA_KEY_DATA, &[0x5A; 32]));
    mkb.extend([0x00, 0x00, 0x00, 0x00]); // end marker
    assert_eq!(
        mkb_version(&mkb),
        Some(1),
        "version 1 is a real, valid MKB version and must be read verbatim"
    );
    assert_eq!(
        mkb_type(&mkb),
        Some(MkbType::Prerecorded),
        "AACS 1.0 pre-recorded Blu-ray type must still decode alongside v1"
    );
    assert_eq!(mkb_is_uhd(&mkb), Some(false), "a v1 BD is not UHD");
}

#[test]
fn trim_mkb_keeps_only_the_framed_records() {
    let mut mkb = type_and_version(MKB_20_CATEGORY_C, 1);
    let content_len = mkb.len(); // the single framed record, no end marker
    mkb.extend([0x00, 0x00, 0x00, 0x00]); // end marker
    mkb.extend([0xDE; 4096]); // trailing padding past the end marker
    let trimmed = trim_mkb(mkb);
    assert_eq!(
        trimmed.len(),
        content_len,
        "trim keeps the framed records, dropping the end marker and padding"
    );
}

// BE24 length field (all THREE bytes): a walker that dropped the high byte would mis-frame
// every large record (real cvalue/variant tables).
#[test]
fn mkb_records_honors_the_high_byte_of_the_be24_length() {
    const TOTAL: usize = 0x0001_0004; // 65_540 — high byte 0x01
    let mut mkb = vec![REC_VKD_TABLE, 0x01, 0x00, 0x04];
    mkb.resize(TOTAL, 0xAB);
    // A second record follows, so a walker that mis-read the length would
    // frame a different number of records rather than merely a short one.
    mkb.extend(rec(REC_TYPE_AND_VERSION, &[0x11; 8]));

    let recs = walk_mkb(&mkb);
    assert_eq!(recs.len(), 2, "the big record must be framed as ONE record");
    assert_eq!(
        recs[0].rec_len, TOTAL,
        "rec_len must include the high BE24 byte"
    );
    assert_eq!(recs[0].body.len(), TOTAL - 4);
    assert_eq!(
        recs[1].rec_type, REC_TYPE_AND_VERSION,
        "the following record must start where the big one ends"
    );
}

// Header-only records and the exact end marker: `rec_len == 4` is a well-formed HEADER-ONLY
// record, including one at the buffer end.
#[test]
fn mkb_records_yields_a_header_only_record_at_the_buffer_end() {
    let mut mkb = rec(REC_TYPE_AND_VERSION, &[0xAA, 0xBB]);
    mkb.extend([REC_VKD_TABLE, 0x00, 0x00, 0x04]); // 4-byte, empty body, at EOF
    assert_eq!(
        mkb.len(),
        10,
        "sanity: the last record ends at the buffer end"
    );

    let recs = walk_mkb(&mkb);
    assert_eq!(recs.len(), 2, "the trailing header-only record is a record");
    assert_eq!(recs[1].rec_type, REC_VKD_TABLE);
    assert_eq!(recs[1].rec_len, 4);
    assert!(recs[1].body.is_empty());
}

// ONLY the exact `00 00 00 00` marker ends the walk. A type-0 record with a
// real length is a record, not the end — stopping there would truncate
// everything after it, including the cvalue/verify records.
#[test]
fn mkb_records_stops_only_on_the_all_zero_end_marker() {
    // A type-0 record of length 8, then a normal record, then the marker.
    let mut mkb = vec![0x00, 0x00, 0x00, 0x08, 1, 2, 3, 4];
    mkb.extend(rec(REC_VKD_TABLE, &[0x55; 16]));
    mkb.extend([0x00, 0x00, 0x00, 0x00]); // the real end marker
    mkb.extend(rec(0x99, &[0xFF; 4])); // past the marker: not walked

    let recs = walk_mkb(&mkb);
    assert_eq!(
        recs.len(),
        2,
        "a type-0 record with a non-zero length is a record, not the end"
    );
    assert_eq!(recs[0].rec_type, 0x00);
    assert_eq!(recs[0].rec_len, 8);
    assert_eq!(recs[1].rec_type, REC_VKD_TABLE);
    assert_eq!(recs[1].body, vec![0x55; 16]);
}

// `mkb_type_raw` reports the 32-bit MKBType field verbatim, including unrecognised values;
// all four bytes must come from the record body.
#[test]
fn mkb_type_raw_reads_all_four_body_bytes() {
    const RAW: u32 = 0xDEAD_BEEF;
    let mkb = type_and_version(RAW, 7);
    assert_eq!(
        mkb_type_raw(&mkb),
        Some(RAW),
        "every byte of the MKBType field must come from the record body"
    );
    assert_eq!(mkb_version(&mkb), Some(7));
}

// Every MkbType category the Type-and-Version field can name must decode to
// its variant, and an unknown value must round-trip verbatim through
// `Other(raw)` — the categorisation the stride/generation dispatch keys on.
#[test]
fn mkb_type_classifies_every_category_byte() {
    let cases: &[(u32, MkbType)] = &[
        (MKB_TYPE_3_RECORDABLE, MkbType::Recordable),
        (MKB_TYPE_4_PRERECORDED, MkbType::Prerecorded),
        (MKB_TYPE_10_CLASS_II, MkbType::ClassII),
        (MKB_20_CATEGORY_C, MkbType::CategoryC20),
        (MKB_21_CATEGORY_C, MkbType::CategoryC21),
    ];
    for &(raw, expected) in cases {
        assert_eq!(
            MkbType::from_raw(raw),
            expected,
            "raw {raw:#010x} must decode to {expected:?}"
        );
        // …and through the byte-level path a real MKB takes.
        let mut mkb = type_and_version(raw, 1);
        mkb.extend([0, 0, 0, 0]);
        assert_eq!(mkb_type(&mkb), Some(expected));
    }
    // An unrecognised MKBType is preserved, not coerced to a known variant.
    const UNKNOWN: u32 = 0x1234_5678;
    assert_eq!(MkbType::from_raw(UNKNOWN), MkbType::Other(UNKNOWN));
    assert!(!matches!(
        MkbType::from_raw(UNKNOWN),
        MkbType::CategoryC20 | MkbType::CategoryC21
    ));
    // No Type-and-Version record at all → no classification (not a default).
    assert_eq!(mkb_type(&[]), None);
    assert_eq!(mkb_type_raw(&[]), None);
}

// generation()/major()/is_uhd()/unit_key_stride() must agree per category:
// only Category C is UHD (2.x, 64-byte stride), and only CategoryC21 is V21.
#[test]
fn mkb_generation_stride_and_major_per_category() {
    // Category C 2.1 → V21, UHD, major 2, 64-byte stride.
    assert_eq!(MkbType::CategoryC21.generation(), AacsVersion::V21);
    assert!(MkbType::CategoryC21.is_uhd());
    // Category C 2.0 → V20, UHD, major 2, 64-byte stride.
    assert_eq!(MkbType::CategoryC20.generation(), AacsVersion::V20);
    assert!(MkbType::CategoryC20.is_uhd());
    // Everything else → V10 (BD), NOT UHD.
    for t in [
        MkbType::Recordable,
        MkbType::Prerecorded,
        MkbType::ClassII,
        MkbType::Other(0xDEAD_BEEF),
    ] {
        assert_eq!(t.generation(), AacsVersion::V10, "{t:?} is AACS 1.0");
        assert!(!t.is_uhd(), "{t:?} is not UHD");
    }

    // Stride and major are the two consumers of the generation enum.
    assert_eq!(AacsVersion::V10.unit_key_stride(), 48);
    assert_eq!(AacsVersion::V20.unit_key_stride(), 64);
    assert_eq!(AacsVersion::V21.unit_key_stride(), 64);
    assert_eq!(AacsVersion::V10.major(), AACS_MAJOR_BD);
    assert_eq!(AacsVersion::V20.major(), AACS_MAJOR_UHD);
    assert_eq!(AacsVersion::V21.major(), AACS_MAJOR_UHD);
}

// from_major is the inverse the keysource stride dispatch relies on: ONLY
// the BD major (1) is V10; every other integer takes the V20 64-byte stride.
#[test]
fn aacs_version_from_major_only_bd_is_v10() {
    assert_eq!(AacsVersion::from_major(AACS_MAJOR_BD), AacsVersion::V10);
    assert_eq!(AacsVersion::from_major(AACS_MAJOR_UHD), AacsVersion::V20);
    // Unknown majors are NOT V10 — they must not take the 48-byte stride.
    for m in [0u8, 3, 4, 255] {
        assert_eq!(
            AacsVersion::from_major(m),
            AacsVersion::V20,
            "major {m} must not select the V10 stride"
        );
        assert_eq!(AacsVersion::from_major(m).unit_key_stride(), 64);
    }
}

// `mkb_prefix_end`: the stream ends inside the prefix only where the walk stops on a header
// the prefix holds; a record or header cut by the prefix asks for the bytes it needs.
#[test]
fn prefix_end_finds_the_end_marker_or_names_the_bytes_still_needed() {
    let a = rec(0x10, &[0u8; 8]); // 12 bytes
    let b = rec(0x04, &[0xAA; 100]); // 104 bytes
    let mut mkb = [a.clone(), b.clone()].concat();
    let content = mkb.len();
    mkb.extend_from_slice(&[0, 0, 0, 0]);
    mkb.resize(content + 64, 0);
    // Whole stream and its end marker seen: the end, equal to `mkb_content_len`.
    assert_eq!(mkb_prefix_end(&mkb), Ok(content));
    assert_eq!(mkb_content_len(&mkb), content);
    // The prefix cuts record `b`: its end plus the next header are needed. The record
    // walk alone stops early here, at the end of `a`.
    let cut = &mkb[..a.len() + 50];
    assert_eq!(mkb_prefix_end(cut), Err(content + 4));
    assert_eq!(mkb_content_len(cut), a.len());
    // The prefix ends exactly after `b`, or inside the header after it: that header.
    assert_eq!(mkb_prefix_end(&mkb[..content]), Err(content + 4));
    assert_eq!(mkb_prefix_end(&mkb[..content + 2]), Err(content + 4));
    // A header that frames no record ends the stream where it stands.
    let mut bad = a.clone();
    bad.extend_from_slice(&[0x04, 0, 0, 2]);
    assert_eq!(mkb_prefix_end(&bad), Ok(a.len()));
    assert_eq!(mkb_prefix_end(&[0x04, 0, 0, 2]), Ok(0));
    assert_eq!(mkb_prefix_end(&[]), Err(4));
}

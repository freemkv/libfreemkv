use super::*;
use crate::udf::fixture::*;

/// Build a UDF with an `HVDVD_TS/` tree holding the listed `.evo` clips
/// (name, sector count, data LBA).
fn make_hddvd_fs(disc: &mut MemDisc, evos: &[(&str, u32, u32)]) -> crate::udf::UdfFs {
    // ICBs are handed out from 100 upward, one per EVO, so the index IS
    // the offset from that base.
    let files: Vec<_> = evos
        .iter()
        .enumerate()
        .map(|(i, (name, sectors, data_lba))| {
            file(
                name,
                100 + i as u32,
                *data_lba,
                u64::from(*sectors) * 2048,
                true,
            )
        })
        .collect();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    crate::udf::read_filesystem(disc).expect("fs")
}

/// HD-DVD's own enumerator yields one title per `.evo`, MpegPs container,
/// with real physical extents (mirrors the BD `.m2ts` extent path).
#[test]
fn scan_hddvd_titles_enumerates_evo_extents() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs(
        &mut disc,
        &[("FEATURE.EVO", 2000, 5000), ("BLOOP.EVO", 300, 9000)],
    );
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 2, "one title per .evo clip");
    for t in &titles {
        assert_eq!(
            t.content_format,
            ContentFormat::MpegPs,
            "EVO is a program stream"
        );
        assert_eq!(t.extents.len(), 1);
    }
    let feature = titles.iter().find(|t| t.playlist == "FEATURE.EVO").unwrap();
    assert_eq!(feature.extents[0].start_lba, PART_START + 5000);
    assert_eq!(feature.extents[0].sector_count, 2000);
    assert_eq!(
        feature.clips[0].clip_id, "FEATURE",
        "clip_id drops the extension"
    );
}

// ── VTI playlist parsing + feature composition ────────────────────────

/// Build a synthetic `ADVANCED-VTS` VTI whose clip table lists `clips` in
/// order — one fixed-stride entry each, NUL-terminated name at `entry+0x42`.
fn synthetic_vti(clips: &[&str]) -> Vec<u8> {
    let table_start = 0x200usize;
    let mut v = vec![0u8; table_start + clips.len() * VTI_CLIP_ENTRY_STRIDE];
    v[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
    for (i, name) in clips.iter().enumerate() {
        let off = table_start + i * VTI_CLIP_ENTRY_STRIDE + 0x42;
        v[off..off + name.len()].copy_from_slice(name.as_bytes());
        // The byte after the name stays 0 (NUL terminator).
    }
    v
}

#[test]
fn parse_vti_clip_order_reads_table_in_authored_order() {
    let vti = synthetic_vti(&[
        "DELOGO.EVO",
        "FEATURE_1.EVO",
        "FEATURE_2.EVO",
        "TRAILER.EVO",
    ]);
    let order = parse_vti_clip_order(&vti);
    assert_eq!(
        order,
        vec![
            "DELOGO.EVO".to_string(),
            "FEATURE_1.EVO".to_string(),
            "FEATURE_2.EVO".to_string(),
            "TRAILER.EVO".to_string(),
        ]
    );
    // A non-VTI blob yields nothing.
    assert!(parse_vti_clip_order(b"not a vti").is_empty());
}

#[test]
fn parse_vti_clip_order_is_deterministic_on_a_bucket_size_tie() {
    // Two equal-size buckets must resolve to the SAME winner every call:
    // `HashMap` iteration is randomized, so `max_by_key` without a
    // deterministic tie-break could pick a different bucket run-to-run.
    let mut vti = vec![0u8; 0x600];
    vti[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
    let put = |v: &mut Vec<u8>, off: usize, name: &str| {
        v[off..off + name.len()].copy_from_slice(name.as_bytes());
    };
    // Bucket A (residue 0x42): two names at stride 0x140.
    put(&mut vti, 0x142, "A1.EVO");
    put(&mut vti, 0x282, "A2.EVO");
    // Bucket B (residue 0x50): two names — same count, different residue.
    put(&mut vti, 0x150, "B1.EVO");
    put(&mut vti, 0x290, "B2.EVO");

    let first = parse_vti_clip_order(&vti);
    // The tie goes to the bucket with the smallest offset (A).
    assert_eq!(first, vec!["A1.EVO".to_string(), "A2.EVO".to_string()]);
    for _ in 0..20 {
        assert_eq!(
            parse_vti_clip_order(&vti),
            first,
            "tie-break must be deterministic across repeated calls"
        );
    }
    assert!(!first.is_empty());
}

#[test]
fn is_feature_clip_matches_the_feature_naming_variants() {
    // Layer-break split (seen on real discs) and the divide form (also seen on real discs).
    assert!(is_feature_clip("FEATURE_1.EVO"));
    assert!(is_feature_clip("FEATURE_2.EVO"));
    assert!(is_feature_clip("feature.EVO"));
    assert!(is_feature_clip("feature_Divide.EVO"));
    // Extras are not the feature.
    assert!(is_feature_clip("PEVOB_1.EVO"));
    assert!(is_feature_clip("pevob_2.evo"));
    assert!(!is_feature_clip("PEVOB_.EVO"));
    assert!(!is_feature_clip("PEVOB_MENU.EVO"));
    assert!(!is_feature_clip("TRAILER.EVO"));
    assert!(!is_feature_clip("DLS_01.EVO"));
    assert!(!is_feature_clip("EPK.EVO"));
}

// The SECOND size accumulator (feature-part composition, not the XPL path) must also
// saturate a disc-declared size near u64::MAX rather than overflow.
#[test]
fn scan_hddvd_feature_composition_saturates_absurd_disc_declared_sizes() {
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, u64::MAX, true),
        file("FEATURE_2.EVO", 101, 8000, u64::MAX, true),
        file_with("HVA00001.VTI", 103, 15000, vti, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None)
        .expect("a hostile size field must not fail the scan");
    let feat = titles
        .iter()
        .find(|t| t.playlist == "FEATURE")
        .expect("composed feature title");
    assert_eq!(
        feat.size_bytes,
        u64::MAX,
        "the part sizes must saturate at u64::MAX, never wrap to a small number"
    );
}

#[test]
fn scan_hddvd_composes_split_feature_into_one_title() {
    // FEATURE_1 + FEATURE_2 (a layer-break split) plus a TRAILER extra;
    // the scan must JOIN the feature parts into one title and keep the
    // trailer separate.
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
        file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
        file("TRAILER.EVO", 102, 12000, 2 * 2048, true),
        file_with("HVA00001.VTI", 103, 15000, vti, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    // One composed FEATURE title + the trailer.
    assert_eq!(titles.len(), 2, "feature parts merged, trailer separate");
    let feat = titles
        .iter()
        .find(|t| t.playlist == "FEATURE")
        .expect("composed feature title");
    assert_eq!(feat.clips.len(), 2, "both feature parts recorded");
    assert_eq!(feat.size_bytes, (10 + 6) * 2048, "part sizes summed");
    // Extents concatenated in authored order: FEATURE_1 (lba 5000) then
    // FEATURE_2 (lba 8000) — the movie plays through in order.
    assert_eq!(feat.extents.len(), 2);
    assert_eq!(feat.extents[0].start_lba, PART_START + 5000);
    assert_eq!(feat.extents[0].sector_count, 10);
    assert_eq!(feat.extents[1].start_lba, PART_START + 8000);
    assert_eq!(feat.extents[1].sector_count, 6);
    // The largest title is the whole feature, not just part 1.
    let largest = titles.iter().max_by_key(|t| t.size_bytes).unwrap();
    assert_eq!(largest.playlist, "FEATURE");
    assert!(titles.iter().any(|t| t.playlist == "TRAILER.EVO"));
}

// A split feature with one part carrying an unrecorded extent must not be composed into a
// FEATURE title (would silently play short); other clips stay available on their own.
#[test]
fn scan_hddvd_does_not_compose_a_feature_over_an_unrecorded_part() {
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
        file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
        file("TRAILER.EVO", 102, 12000, 2 * 2048, true),
        file_with("HVA00001.VTI", 103, 15000, vti, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    // FEATURE_2's ICB (laid at PART_START + 101): an unrecorded 2048-byte
    // extent at LBA 7999, then its real content at 8000.
    let mut icb = build_file_icb(6 * 2048, 8000, false);
    icb[212..216].copy_from_slice(&16u32.to_le_bytes());
    icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1
    icb[220..224].copy_from_slice(&7999u32.to_le_bytes());
    icb[224..228].copy_from_slice(&(6u32 * 2048).to_le_bytes()); // type 0
    icb[228..232].copy_from_slice(&8000u32.to_le_bytes());
    disc.put_bytes(PART_START + 101, &icb);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert!(
        !titles.iter().any(|t| t.playlist == "FEATURE"),
        "a feature part with no truthful read plan must not be composed \
             into a title that silently omits it; got {:?}",
        titles
            .iter()
            .map(|t| (&t.playlist, t.extents.len()))
            .collect::<Vec<_>>()
    );
    // The clips that DID resolve are still offered on their own.
    assert!(
        titles.iter().any(|t| t.playlist == "TRAILER.EVO"),
        "refusing the composition must not refuse the healthy clips"
    );
    assert!(
        titles
            .iter()
            .all(|t| t.extents.iter().all(|e| e.start_lba != PART_START + 7999)),
        "no title may read the unrecorded extent"
    );
}

// The refusal above must also be ACCOUNTED, logging the unrecorded extent's own
// E_UDF_UNRECORDED_EXTENT code (not a hardcoded/neighbouring literal).
#[test]
fn scan_hddvd_logs_an_unrecorded_feature_part_with_its_own_code() {
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
        file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
        file_with("HVA00001.VTI", 103, 15000, vti, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    // Same crafted ICB as the test above: an unrecorded (type 1) extent
    // ahead of FEATURE_2's real content.
    let mut icb = build_file_icb(6 * 2048, 8000, false);
    icb[212..216].copy_from_slice(&16u32.to_le_bytes());
    icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes());
    icb[220..224].copy_from_slice(&7999u32.to_le_bytes());
    icb[224..228].copy_from_slice(&(6u32 * 2048).to_le_bytes());
    icb[228..232].copy_from_slice(&8000u32.to_le_bytes());
    disc.put_bytes(PART_START + 101, &icb);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let (_titles, events) =
        crate::testlog::capture(|| Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan"));
    let line = events
        .iter()
        .find(|e| e.field("clip") == Some("\"FEATURE_2.EVO\""))
        .unwrap_or_else(|| panic!("the refused clip must be logged; got {events:?}"));
    assert_eq!(line.target, "freemkv::disc");
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(
        line.field("code"),
        Some(crate::error::E_UDF_UNRECORDED_EXTENT.to_string().as_str()),
        "{line:?}"
    );
}

// The Ok-but-empty twin of the test above: a feature part whose file_extents SUCCEEDS but
// yields nothing usable must also refuse the composition and log it.
#[test]
fn scan_hddvd_does_not_compose_a_feature_over_a_part_with_no_usable_extent() {
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
        // Size 0 -> `file_extents` returns Ok with a zero-sector AD, which
        // the `sectors > 0 && lba > 0` filter discards. No error is ever
        // returned; the clip simply resolves to nothing.
        file("FEATURE_2.EVO", 101, 8000, 0, true),
        file("TRAILER.EVO", 102, 12000, 2 * 2048, true),
        file_with("HVA00001.VTI", 103, 15000, vti, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let (titles, events) =
        crate::testlog::capture(|| Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan"));

    assert!(
        !titles.iter().any(|t| t.playlist == "FEATURE"),
        "a feature part that resolved to no usable extent must not be \
             composed into a title that silently omits it while claiming the \
             whole feature; got {:?}",
        titles
            .iter()
            .map(|t| (&t.playlist, t.extents.len(), t.size_bytes))
            .collect::<Vec<_>>()
    );
    // Refusing the composition is not refusing the disc.
    assert!(
        titles.iter().any(|t| t.playlist == "FEATURE_1.EVO"),
        "the part that DID resolve is still offered standalone"
    );
    assert!(titles.iter().any(|t| t.playlist == "TRAILER.EVO"));

    // ...and it is accounted, with its own code, naming the clip.
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc" && e.field("clip") == Some("\"FEATURE_2.EVO\""))
        .unwrap_or_else(|| panic!("the dropped clip must be logged; got {events:?}"));
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(
        line.field("code"),
        Some(crate::error::E_UDF_NO_USABLE_EXTENT.to_string().as_str()),
        "the condition's OWN code, not a neighbouring one: {line:?}"
    );
}

// A file whose only dropped extent is a non-empty one at abs LBA 0 must not compose
// a short clip: the scan wiring (`|| truncated`) has to refuse it, with its own code.
#[test]
fn scan_hddvd_does_not_compose_a_feature_over_a_part_with_a_dropped_lba0_extent() {
    let mut disc = MemDisc::new();
    let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
    let files = vec![
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, false),
        file("FEATURE_2.EVO", 101, 8000, 14 * 2048, false),
        file("TRAILER.EVO", 102, 12000, 2 * 2048, false),
        file_with("HVA00001.VTI", 103, 15000, vti, false),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    // Two short ADs: 10 sectors at rel LBA 0, then 4 at 8000.
    let mut icb = build_file_icb(14 * 2048, 0, false);
    icb[212..216].copy_from_slice(&16u32.to_le_bytes());
    icb[216..220].copy_from_slice(&(10u32 * 2048).to_le_bytes());
    icb[224..228].copy_from_slice(&(4u32 * 2048).to_le_bytes());
    icb[228..232].copy_from_slice(&8000u32.to_le_bytes());
    disc.put_bytes(PART_START + 101, &icb);
    let mut udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    // Data LBAs become absolute, so rel LBA 0 is abs LBA 0.
    udf.set_partition_start(0);

    let (titles, events) =
        crate::testlog::capture(|| Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan"));

    assert!(
        !titles.iter().any(|t| t.playlist == "FEATURE"),
        "a part with a dropped lba-0 extent must not be composed short; got {:?}",
        titles
            .iter()
            .map(|t| (&t.playlist, t.size_bytes))
            .collect::<Vec<_>>()
    );
    assert!(titles.iter().any(|t| t.playlist == "FEATURE_1.EVO"));
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc" && e.field("clip") == Some("\"FEATURE_2.EVO\""))
        .unwrap_or_else(|| panic!("the dropped clip must be logged; got {events:?}"));
    assert_eq!(
        line.field("code"),
        Some(crate::error::E_UDF_NO_USABLE_EXTENT.to_string().as_str()),
        "{line:?}"
    );
}

// ── codec sniffing ────────────────────────────────────────────────────

#[test]
fn sniff_video_codec_recognizes_h264_vc1_mpeg2() {
    // H.264 SPS NAL (type 7). 0x67/0x27/0x47 all decode to type 7.
    assert_eq!(
        sniff_video_codec(&[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E]),
        Some(Codec::H264)
    );
    assert_eq!(
        sniff_video_codec(&[0x11, 0x00, 0x00, 0x01, 0x27, 0x64]),
        Some(Codec::H264)
    );
    // VC-1 sequence-header BDU (0x0F).
    assert_eq!(
        sniff_video_codec(&[0x00, 0x00, 0x01, 0x0F, 0xC0]),
        Some(Codec::Vc1)
    );
    // MPEG-2 sequence_header (0xB3).
    assert_eq!(
        sniff_video_codec(&[0x00, 0x00, 0x01, 0xB3, 0x2D]),
        Some(Codec::Mpeg2)
    );
    // A slice/picture-only sample (no SPS/sequence) is indeterminate.
    assert_eq!(sniff_video_codec(&[0x00, 0x00, 0x01, 0x61, 0x9A]), None);
    assert_eq!(sniff_video_codec(&[0xDE, 0xAD, 0xBE, 0xEF]), None);

    // Overlap regression: a picture_start_code (0x00) with payload 00 00
    // must advance a full 4 bytes so the code byte isn't re-read as a new
    // marker; the real MPEG-2 sequence header after it must still be found.
    assert_eq!(
        sniff_video_codec(&[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xB3, 0x2D]),
        Some(Codec::Mpeg2)
    );
    // A lone picture_start_code with a 00-heavy payload and no following real
    // start code stays indeterminate (the overlap must not fabricate one).
    assert_eq!(
        sniff_video_codec(&[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]),
        None
    );

    // Every byte of the `00 00 01` marker is load-bearing: a near-miss
    // that gets ANY one of the three bytes wrong must not be recognized.
    assert_eq!(
        sniff_video_codec(&[0x05, 0x00, 0x01, 0xB3]),
        None,
        "leading byte must be 0x00, not just any byte"
    );
    assert_eq!(
        sniff_video_codec(&[0x00, 0x05, 0x01, 0xB3]),
        None,
        "second byte must be 0x00, not just any byte"
    );
    assert_eq!(
        sniff_video_codec(&[0x00, 0xFF, 0x01, 0xB3]),
        None,
        "the middle byte of the marker must actually be checked, not skipped"
    );
}

#[test]
fn sniff_audio_codec_recognizes_eac3_syncword() {
    assert_eq!(
        sniff_audio_codec(&[0x00, 0x0B, 0x77, 0x12, 0x34]),
        Some(Codec::Ac3Plus)
    );
    assert_eq!(sniff_audio_codec(&[0x00, 0x01, 0x02, 0x03]), None);
    // Both syncword bytes are required together: a lone 0x0B with no 0x77
    // partner anywhere must not be recognized.
    assert_eq!(
        sniff_audio_codec(&[0x0B, 0x00, 0x0B, 0x01]),
        None,
        "0x0B alone (no 0x77 partner) is not the E-AC-3 syncword"
    );
}

// ── EVO head probe → streams ──────────────────────────────────────────

/// A minimal bounded PES: `00 00 01 [id] [len:2] 80 00 00 [payload]`.
fn pes(stream_id: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x00, 0x01, stream_id];
    let len = (3 + payload.len()) as u16; // flags1+flags2+hdl + payload
    v.extend_from_slice(&len.to_be_bytes());
    v.extend_from_slice(&[0x80, 0x00, 0x00]);
    v.extend_from_slice(payload);
    v
}

// Synthetic EVO: pack header, H.264 SPS+IDR video PES (0xE2), two DD+
// audio PES (sub-ids 0xC0/0xC1), then program-end.
fn synthetic_evo() -> Vec<u8> {
    let mut d = Vec::new();
    // MPEG-2 pack header (14 bytes, stuffing 0).
    d.extend_from_slice(&[
        0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3, 0xF8,
    ]);
    // Video PES on stream_id 0xE2 (a real disc's H.264 sub-id in the 0xE0-0xEF
    // range): SPS (type 7) + IDR (type 5) Annex-B.
    let video_es = [
        0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAB, 0xCD, // SPS
        0x00, 0x00, 0x01, 0x65, 0x88, 0x00, // IDR slice
    ];
    d.extend_from_slice(&pes(0xE2, &video_es));
    // DD+ audio PES: sub-id + 4-byte sub-header (num_frames + ptr) folded in
    // — the demuxer strips 4 bytes, leaving the E-AC-3 syncword.
    for sub in [0xC0u8, 0xC1] {
        let audio_payload = [
            sub, 0x01, 0x00, 0x00, // sub-id + num_frames(1) + ptr(2)
            0x0B, 0x77, 0xDE, 0xAD, // E-AC-3 syncword + body
        ];
        d.extend_from_slice(&pes(0xBD, &audio_payload));
    }
    d.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]); // program end
    d
}

/// Build a UDF whose `HVDVD_TS/FEATURE.EVO` holds the given raw bytes.
fn make_hddvd_fs_with_evo(disc: &mut MemDisc, evo: &[u8]) -> crate::udf::UdfFs {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file_with("FEATURE.EVO", 100, 5000, evo.to_vec(), true)],
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    crate::udf::read_filesystem(disc).expect("fs")
}

// An .evo whose ICB declares an UNRECORDED (ECMA-167 4/14.14.1.1 type-1)
// extent must yield NO title: those sectors hold undefined data, so
// neither splicing them in nor skipping them is a real rip.
#[test]
fn scan_hddvd_titles_refuses_a_clip_with_an_unrecorded_extent() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());

    // Control: unpatched, this fixture really does produce a title — so a
    // later empty result means the hole was rejected, not that the
    // fixture was inert.
    assert_eq!(
        Disc::scan_hddvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .len(),
        1,
        "fixture must yield a title before the hole is introduced"
    );

    // Rewrite FEATURE.EVO's ICB (laid at PART_START + 100) with a
    // two-descriptor short-AD list: an unrecorded 2048-byte extent at LBA
    // 4999, then the real 4096-byte content at 5000.
    let mut icb = build_file_icb(4096, 5000, false);
    icb[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad: two short ADs
    icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1, 2048 bytes
    icb[220..224].copy_from_slice(&4999u32.to_le_bytes());
    icb[224..228].copy_from_slice(&4096u32.to_le_bytes()); // type 0, 4096 bytes
    icb[228..232].copy_from_slice(&5000u32.to_le_bytes());
    disc.put_bytes(PART_START + 100, &icb);

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert!(
        titles.is_empty(),
        "a clip with an unrecorded extent has no truthful read plan; got \
             extents {:?}",
        titles.iter().map(|t| &t.extents).collect::<Vec<_>>()
    );
}

// End-to-end: an .evo head with H.264 video + two DD+ audio PES yields a
// title with video on the canonical PID and both DD+ tracks on
// 0xBDC0/0xBDC1 — the non-empty streams the mux path needs.
#[test]
fn scan_hddvd_titles_probes_streams_from_evo_head() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 1);
    let t = &titles[0];

    let video: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(video.len(), 1, "one video track probed");
    assert_eq!(video[0].codec, Codec::H264, "SPS sniffed as H.264");
    assert_eq!(
        video[0].pid,
        crate::mux::ps::DVD_VIDEO_PID,
        "video routes to canonical PID"
    );

    let audio: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Audio(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(audio.len(), 2, "both DD+ sub-streams probed");
    assert!(audio.iter().all(|a| a.codec == Codec::Ac3Plus));
    let pids: Vec<u16> = audio.iter().map(|a| a.pid).collect();
    assert_eq!(pids, vec![0xBDC0, 0xBDC1], "DD+ PIDs 0xBDC0/0xBDC1");
}

/// A clip whose head carries no recognizable stream (unreadable /
/// ciphertext) leaves `streams` empty rather than fabricating one — the
/// title still enumerates (extents are real).
#[test]
fn scan_hddvd_titles_empty_streams_when_head_unrecognized() {
    let mut disc = MemDisc::new();
    // 4 KiB of junk with no PS start codes.
    let junk = vec![0x55u8; 4096];
    let udf = make_hddvd_fs_with_evo(&mut disc, &junk);
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 1);
    assert!(
        titles[0].streams.is_empty(),
        "no recognizable stream → empty, not fabricated"
    );
}

// A PES on the HD-DVD extended-stream-id (0xFD) with the given
// stream_id_extension in a minimal PES extension — the shape a real
// VC-1 video PES uses (ext 0x55).
fn pes_extended(stream_id_extension: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x00, 0x01, 0xFD];
    let opt = [0x01u8, 0x81, stream_id_extension];
    let len = (3 + opt.len() + payload.len()) as u16; // flags1+flags2+hdl + opt + payload
    v.extend_from_slice(&len.to_be_bytes());
    v.extend_from_slice(&[0x80, 0x01, opt.len() as u8]);
    v.extend_from_slice(&opt);
    v.extend_from_slice(payload);
    v
}

/// Synthetic EVO carrying VC-1 video on the extended-stream-id 0xFD (ext
/// 0x55), as a real retail HD-DVD title does, plus one DD+ audio PES.
fn synthetic_evo_vc1() -> Vec<u8> {
    let mut d = Vec::new();
    d.extend_from_slice(&[
        0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3, 0xF8,
    ]);
    // VC-1 sequence header (00 00 01 0F) + a frame BDU (00 00 01 0D).
    let video_es = [
        0x00, 0x00, 0x01, 0x0F, 0xC5, 0x00, 0x00, // sequence header BDU
        0x00, 0x00, 0x01, 0x0D, 0x12, 0x34, // frame BDU
    ];
    d.extend_from_slice(&pes_extended(0x55, &video_es));
    let audio_payload = [0xC0u8, 0x01, 0x00, 0x00, 0x0B, 0x77, 0xDE, 0xAD];
    d.extend_from_slice(&pes(0xBD, &audio_payload));
    d.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);
    d
}

/// Build a bare PsPacket for the collect_es routing test.
fn ps_pkt(stream_id: u8, sub: Option<u8>, data: Vec<u8>) -> crate::mux::ps::PsPacket {
    crate::mux::ps::PsPacket {
        stream_id,
        sub_stream_id: sub,
        pts: None,
        dts: None,
        data,
        source: None,
    }
}

#[test]
fn collect_es_routes_only_vc1_0xfd_to_video() {
    use crate::mux::ps::hddvd_extended_pid;
    // The 0xFD guard: only VC-1 (ext 0x55) is video. An HD-audio 0xFD
    // sub-stream (e.g. 0x72) arriving FIRST must not stamp video_pid or
    // pollute the video sample, else the real video track is lost.
    let mut video = Vec::new();
    let mut video_pid: Option<u16> = None;
    let mut audio = BTreeMap::new();
    // Audio-on-0xFD (ext 0x72) first — must be ignored by the video path.
    collect_es(
        &ps_pkt(0xFD, Some(0x72), vec![0xAA; 32]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert!(
        video.is_empty(),
        "0xFD audio sub-stream not routed to video"
    );
    assert_eq!(video_pid, None, "0xFD audio did not stamp the video PID");
    // Then the real VC-1 video (ext 0x55).
    collect_es(
        &ps_pkt(0xFD, Some(0x55), vec![0xBB; 32]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(
        video_pid,
        Some(hddvd_extended_pid(0x55)),
        "video PID stamped from the VC-1 0xFD sub-stream (0xFD55)"
    );
    assert_eq!(video.len(), 32, "VC-1 0xFD payload accumulated as video");
}

/// End-to-end: an `.evo` whose video rides the extended-stream-id 0xFD yields
/// a VC-1 video track routed to `0xFD00 | ext` (0xFD55) — the PID the demuxer
/// derives from the same stream_id_extension, so mux-time routing lines up.
#[test]
fn scan_hddvd_titles_probes_vc1_on_extended_stream_id() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo_vc1());
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 1);

    let video: Vec<_> = titles[0]
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(video.len(), 1, "one video track probed");
    assert_eq!(video[0].codec, Codec::Vc1, "VC-1 sequence header sniffed");
    assert_eq!(
        video[0].pid,
        crate::mux::ps::hddvd_extended_pid(0x55),
        "VC-1 routes to the extended-stream-id PID 0xFD55"
    );
}

// Advanced-Content playlist (XPL): a minimal but faithful VPLST000.XPL —
// default namespace, a MainMovie title whose feature is a two-clip
// layer-break split with two chapters, plus a deleted-scene title.
const SYNTH_XPL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Playlist majorVersion="1" minorVersion="0" xmlns="http://www.dvdforum.org/2005/HDDVDVideo/Playlist">
  <!-- Authored with TOSHIBA AdvMain -->
  <TitleSet timeBase="60fps" tickBase="60fps" defaultLanguage="en">
    <Title titleNumber="2" titleDuration="01:37:20:00" id="MainMovie" displayName="Main Movie">
      <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:48:29:50" src="file:///dvddisc/HVDVD_TS/FEATURE_1.MAP" dataSource="Disc">
        <Video track="1" mediaAttr="2"/>
        <Audio track="1" streamNumber="1" description="English DD+"/>
      </PrimaryAudioVideoClip>
      <PrimaryAudioVideoClip titleTimeBegin="00:48:29:50" clipTimeBegin="00:00:00:00" titleTimeEnd="01:37:20:00" src="file:///dvddisc/HVDVD_TS/FEATURE_2.MAP" seamless="true">
        <Video track="1" mediaAttr="2"/>
      </PrimaryAudioVideoClip>
      <ChapterList>
        <Chapter displayName="Chapter  1" titleTimeBegin="00:00:00:00" />
        <Chapter displayName="Chapter  2" titleTimeBegin="00:03:40:30" />
      </ChapterList>
    </Title>
    <Title titleNumber="7" titleDuration="00:00:44:29" id="Deleted5" displayName="Deleted Scenes - Veronica Past">
      <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:00:44:29" src="file:///dvddisc/HVDVD_TS/DEL5_VERONICAPAST.MAP" />
    </Title>
  </TitleSet>
</Playlist>"#;

#[test]
fn parse_timecode_hhmmssff_at_60fps() {
    // 01:37:20:00 = 5840 nominal seconds, x1.001 at the NTSC tick rate.
    assert!((parse_timecode("01:37:20:00", 60).unwrap() - 5840.0 * 1.001).abs() < 1e-6);
    // 00:48:29:50 = 48m29s + 50/60 frames.
    let want = (48.0 * 60.0 + 29.0 + 50.0 / 60.0) * 1.001;
    assert!((parse_timecode("00:48:29:50", 60).unwrap() - want).abs() < 1e-6);
    // MM:SS:FF short form (hours omitted).
    let want = (125.0 + 15.0 / 60.0) * 1.001;
    assert!((parse_timecode("02:05:15", 60).unwrap() - want).abs() < 1e-6);
    assert_eq!(parse_timecode("garbage", 60), None);
    assert_eq!(parse_timecode("", 60), None);
}

// A real HD DVD (VPLST000.XPL, timeBase="60fps") against its own EVOs as measured
// by ffprobe: title time runs at 60000/1001, so 60 exact reads every title 0.1% short.
#[test]
fn xpl_60fps_timecodes_run_at_ntsc_rate() {
    for (tc, evo_secs) in [
        ("00:49:08:40", 2951.604), // PEVOB_2 (feature clip 2) video start
        ("00:14:49:00", 889.888),  // hazards
        ("00:04:42:00", 282.272),  // daisy
        ("00:02:01:56", 122.064),  // intro
    ] {
        let got = parse_timecode(tc, 60).unwrap();
        assert!(
            (got - evo_secs).abs() < 0.1,
            "{tc}: {got} vs EVO {evo_secs}"
        );
    }
    // PAL tick base is exact.
    assert!((parse_timecode("00:14:49:00", 50).unwrap() - 889.0).abs() < 1e-9);
}

// Timecode frames count in timeBase, not tickBase (the markup tick clock).
#[test]
fn xpl_timecodes_use_time_base_not_tick_base() {
    let xpl = |attrs: &str| {
        format!(
            r#"<Playlist xmlns="http://www.dvdforum.org/2005/HDDVDVideo/Playlist">
  <TitleSet {attrs}>
    <Title titleNumber="1" titleDuration="00:00:10:30">
      <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:00:10:30" src="file:///dvddisc/HVDVD_TS/A.MAP"/>
    </Title>
  </TitleSet>
</Playlist>"#
        )
    };
    let dur = |attrs: &str| parse_xpl_titles(xpl(attrs).as_bytes())[0].duration_secs;
    let ntsc60 = 10.5 * 1.001;
    let d = dur(r#"timeBase="60fps" tickBase="24fps""#);
    assert!(
        (d - ntsc60).abs() < 1e-9,
        "timeBase 60 wins over tickBase 24: {d}"
    );
    let d = dur(r#"tickBase="60fps""#);
    assert!((d - ntsc60).abs() < 1e-9, "tickBase is the fallback: {d}");
    let d = dur(r#"timeBase="50fps" tickBase="60fps""#);
    assert!((d - 10.6).abs() < 1e-9, "50fps unadjusted: {d}");
    let d = dur("");
    assert!((d - ntsc60).abs() < 1e-9, "default 60fps: {d}");
}

#[test]
fn evo_from_src_maps_map_sidecar_to_evo() {
    assert_eq!(
        evo_from_src("file:///dvddisc/HVDVD_TS/FEATURE_1.MAP").as_deref(),
        Some("feature_1.evo")
    );
    // Already an EVO, or lowercase feature — normalise to lower `.evo`.
    assert_eq!(evo_from_src("feature.EVO").as_deref(), Some("feature.evo"));
    assert_eq!(
        evo_from_src("file:///x/feature_Divide.MAP").as_deref(),
        Some("feature_divide.evo")
    );
    // Empty stem → None (defensive against a malformed src).
    assert_eq!(evo_from_src("file:///x/").as_deref(), None);
}

#[test]
fn parse_xpl_titles_reads_titles_clips_chapters_durations() {
    let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
    assert_eq!(titles.len(), 2, "MainMovie + one deleted-scene title");

    let mm = &titles[0];
    assert_eq!(mm.number, 2);
    assert_eq!(mm.name, "Main Movie");
    assert!(
        (mm.duration_secs - 5840.0 * 1.001).abs() < 1e-6,
        "97:20 from titleDuration"
    );
    // The layer-break split is ONE title with TWO clips, contiguous timeline.
    assert_eq!(mm.clips.len(), 2);
    assert_eq!(mm.clips[0].evo, "feature_1.evo");
    assert_eq!(mm.clips[1].evo, "feature_2.evo");
    assert!(
        (mm.clips[0].end_secs - mm.clips[1].begin_secs).abs() < 1e-6,
        "FEATURE_2 begins exactly where FEATURE_1 ends (seamless join)"
    );
    assert!(
        mm.clips[1].begin_secs > 0.0,
        "second clip carries a title-time offset"
    );
    assert_eq!(mm.chapters.len(), 2);
    assert!((mm.chapters[1] - (3.0 * 60.0 + 40.0 + 30.0 / 60.0) * 1.001).abs() < 1e-6);

    let del = &titles[1];
    assert_eq!(del.name, "Deleted Scenes - Veronica Past");
    assert_eq!(del.clips.len(), 1);
    assert_eq!(del.clips[0].evo, "del5_veronicapast.evo");
}

// `[HD]` §4.4.2 Table 4-13 (Encapsulation Format for Hash): "AACS", type 12h, reserved,
// Nfs at bytes 7..11, the 272-byte Resource File Name field, the data, a Hash Pointer.
fn arf(kind: u8, data: &[u8]) -> Vec<u8> {
    let mut v = b"AACS".to_vec();
    v.extend_from_slice(&[kind, 0, 0]);
    v.extend_from_slice(&(data.len() as u32).to_be_bytes());
    let mut name = b"VPLST000.XPL.AACS".to_vec();
    name.resize(272, 0);
    v.extend_from_slice(&name);
    v.extend_from_slice(data);
    v.extend_from_slice(&1u32.to_be_bytes());
    v
}

#[test]
fn parse_xpl_titles_reads_a_playlist_wrapped_as_an_aacs_arf() {
    let bare = parse_xpl_titles(SYNTH_XPL.as_bytes());
    assert!(!bare.is_empty());
    for kind in [0x12, 0x02, 0x21] {
        let wrapped = parse_xpl_titles(&arf(kind, SYNTH_XPL.as_bytes()));
        assert_eq!(wrapped.len(), bare.len(), "type {kind:#04x}");
        assert_eq!(wrapped[0].duration_secs, bare[0].duration_secs);
    }
    // Encrypted ARFs (01h, 11h) cannot be read without the Title Key.
    assert!(parse_xpl_titles(&arf(0x11, SYNTH_XPL.as_bytes())).is_empty());
}

#[test]
fn parse_xpl_titles_returns_empty_on_non_xml() {
    assert!(parse_xpl_titles(b"not xml at all").is_empty());
    assert!(parse_xpl_titles(&[0xFF, 0x00, 0x01, 0x02]).is_empty());
    assert!(parse_xpl_titles(b"<Playlist></Playlist>").is_empty());
}

// A playlist declaring far more titles than any real disc must be capped at MAX_XPL_TITLES
// before probe_evo_streams runs per title (drive-time amplification).
#[test]
fn parse_xpl_titles_caps_a_playlist_declaring_absurdly_many_titles() {
    const DECLARED: usize = MAX_XPL_TITLES + 500;
    let mut xpl = String::with_capacity(DECLARED * 56 + 128);
    xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?><Playlist><TitleSet>"#);
    for _ in 0..DECLARED {
        xpl.push_str(r#"<Title><PrimaryAudioVideoClip src="A.EVO"/></Title>"#);
    }
    xpl.push_str("</TitleSet></Playlist>");

    let titles = parse_xpl_titles(xpl.as_bytes());
    assert_eq!(
        titles.len(),
        MAX_XPL_TITLES,
        "a playlist declaring {DECLARED} titles must be capped at \
             {MAX_XPL_TITLES}; each surviving title costs a real probe_evo_streams \
             pass over the medium"
    );
}

/// The control: a realistic playlist keeps every title it declares.
#[test]
fn parse_xpl_titles_keeps_every_title_of_a_realistic_playlist() {
    let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
    assert!(
        !titles.is_empty() && titles.len() < MAX_XPL_TITLES,
        "the synthetic real-world playlist must survive the cap untouched, \
             got {} titles",
        titles.len()
    );
}

// Builds a playlist nesting `nesting` <Title> elements with `clips`/
// `chapters` elements at the innermost level — since parse_xpl_titles
// collects via descendants(), every ancestor sees the same clip list.
fn nested_title_xpl(nesting: usize, clips: usize, chapters: usize) -> String {
    let mut xpl = String::with_capacity(nesting * 16 + clips * 40 + chapters * 40 + 128);
    xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?><Playlist><TitleSet>"#);
    for _ in 0..nesting {
        xpl.push_str("<Title>");
    }
    for _ in 0..clips {
        xpl.push_str(r#"<PrimaryAudioVideoClip src="A.EVO"/>"#);
    }
    for _ in 0..chapters {
        xpl.push_str(r#"<Chapter titleTimeBegin="00:00:01:00"/>"#);
    }
    for _ in 0..nesting {
        xpl.push_str("</Title>");
    }
    xpl.push_str("</TitleSet></Playlist>");
    xpl
}

// CLIPS PER TITLE: neither the title cap nor the depth cap bounds how many clip elements
// ONE title collects, and descendants() means nested ancestors multiply the count.
#[test]
fn parse_xpl_titles_caps_clips_per_title_against_descendant_amplification() {
    const NESTING: usize = 25;
    const CLIPS: usize = MAX_XPL_CLIPS_PER_TITLE + 500;

    let xpl = nested_title_xpl(NESTING, CLIPS, 0);
    let titles = parse_xpl_titles(xpl.as_bytes());

    // The nesting itself must survive the depth guard, or this test would
    // pass for the wrong reason (an empty fallback).
    assert_eq!(
        titles.len(),
        NESTING,
        "all {NESTING} nested <Title> elements must parse, so the \
             amplification is really being exercised"
    );

    let total: usize = titles.iter().map(|t| t.clips.len()).sum();
    for t in &titles {
        assert!(
            t.clips.len() <= MAX_XPL_CLIPS_PER_TITLE,
            "a title collected {} clips, above the {MAX_XPL_CLIPS_PER_TITLE} \
                 cap; total across titles {total} (a 64 MiB XPL scales this to \
                 tens of millions)",
            t.clips.len()
        );
    }
    assert!(
        total <= NESTING * MAX_XPL_CLIPS_PER_TITLE,
        "aggregate clip count {total} must be bounded by titles x cap"
    );
}

// CHAPTERS PER TITLE: the same descendants() amplification, on the second unbounded
// collect() in the same loop, bounded separately from clips.
#[test]
fn parse_xpl_titles_caps_chapters_per_title() {
    const NESTING: usize = 25;
    const CHAPTERS: usize = MAX_XPL_CHAPTERS_PER_TITLE + 500;

    // One clip, so the title is kept (`clips.is_empty()` skips otherwise).
    let xpl = nested_title_xpl(NESTING, 1, CHAPTERS);
    let titles = parse_xpl_titles(xpl.as_bytes());
    assert_eq!(titles.len(), NESTING);

    for t in &titles {
        assert!(
            t.chapters.len() <= MAX_XPL_CHAPTERS_PER_TITLE,
            "a title collected {} chapters, above the \
                 {MAX_XPL_CHAPTERS_PER_TITLE} cap",
            t.chapters.len()
        );
    }
}

// The control: a realistic multi-clip title still resolves EVERY clip and chapter (losing
// either to the cap would cost a genuine disc half its feature).
#[test]
fn parse_xpl_titles_keeps_every_clip_of_a_realistic_title() {
    let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
    assert_eq!(titles.len(), 2);
    assert_eq!(
        titles[0].clips.len(),
        2,
        "the layer-break split must keep BOTH clips through the cap"
    );
    assert_eq!(titles[0].clips[0].evo, "feature_1.evo");
    assert_eq!(titles[0].clips[1].evo, "feature_2.evo");
    assert_eq!(
        titles[0].chapters.len(),
        2,
        "both chapters must survive the chapter cap"
    );
    assert_eq!(titles[1].clips.len(), 1);
}

#[test]
fn parse_xpl_titles_refuses_deeply_nested_playlist() {
    const DEPTH: usize = 50_000;
    let mut xpl = String::with_capacity(DEPTH * 8 + 64);
    xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    for _ in 0..DEPTH {
        xpl.push_str("<n>");
    }
    for _ in 0..DEPTH {
        xpl.push_str("</n>");
    }
    assert!(
        parse_xpl_titles(xpl.as_bytes()).is_empty(),
        "a hostile nesting depth must fall back, not abort the process"
    );
}

// The depth guard must not reject real discs: adds self-closing tags, a comment, and a
// processing instruction the pre-parse scanner must not miscount as nesting.
#[test]
fn parse_xpl_titles_accepts_real_world_nesting_depth() {
    let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
    assert_eq!(titles.len(), 2, "a real playlist still parses");
    assert_eq!(titles[0].clips.len(), 2);

    // Comments and processing instructions carry `<` and `/>` that a naive
    // scanner would miscount as element nesting.
    let decl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    let tricky = SYNTH_XPL.replacen(
        decl,
        &format!("{decl}\n<!-- <a><a><a><a><a> -->\n<?authoring <b><b><b> ?>"),
        1,
    );
    assert_eq!(parse_xpl_titles(tricky.as_bytes()).len(), 2);
}

// More than MAX_XPL_DEPTH fake open tags inside a comment / PI must not count as nesting.
#[test]
fn xpl_depth_ignores_open_tags_inside_comments_and_pis() {
    let fake = "<a>".repeat(MAX_XPL_DEPTH + 10);
    assert!(xpl_depth_within_limit(&format!("<r><!-- {fake} --></r>")));
    assert!(xpl_depth_within_limit(&format!("<r><?pi {fake} ?></r>")));
    // Control: the same tags as real elements are over the limit.
    assert!(!xpl_depth_within_limit(&format!("<r>{fake}</r>")));
}

// Builds a UDF with HVDVD_TS/ .evo clips plus an ADV_OBJ/VPLST000.XPL
// carrying `xpl`, so scan_hddvd_titles takes the playlist path.
fn make_hddvd_fs_xpl(
    disc: &mut MemDisc,
    evos: &[(&str, u32, u32)],
    xpl: &[u8],
) -> crate::udf::UdfFs {
    // ICBs are handed out from 100 upward, one per EVO, so the index IS
    // the offset from that base.
    let hv_files: Vec<_> = evos
        .iter()
        .enumerate()
        .map(|(i, (name, sectors, data_lba))| {
            file(
                name,
                100 + i as u32,
                *data_lba,
                u64::from(*sectors) * 2048,
                true,
            )
        })
        .collect();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: hv_files,
                subdirs: vec![],
            },
            DirSpec {
                name: "ADV_OBJ".to_string(),
                icb_lba: 30,
                dir_data_lba: 31,
                files: vec![file_with("VPLST000.XPL", 40, 4000, xpl.to_vec(), true)],
                subdirs: vec![],
            },
        ],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    crate::udf::read_filesystem(disc).expect("fs")
}

// The authoritative path: when VPLST000.XPL is present, titles come from
// the playlist — layer-break split composed into ONE title with real
// duration/name/chapters/offsets, not the clip-name heuristic.
#[test]
fn scan_hddvd_composes_titles_from_xpl_playlist() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_xpl(
        &mut disc,
        &[
            ("FEATURE_1.EVO", 2000, 5000),
            ("FEATURE_2.EVO", 1800, 9000),
            ("DEL5_VERONICAPAST.EVO", 100, 12000),
        ],
        SYNTH_XPL.as_bytes(),
    );
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(
        titles.len(),
        2,
        "the two playlist titles that resolve to clips"
    );

    let mm = titles
        .iter()
        .find(|t| t.playlist == "Main Movie")
        .expect("MainMovie composed from the playlist");
    assert_eq!(mm.playlist_id, 2);
    assert!(
        (mm.duration_secs - 5840.0 * 1.001).abs() < 1.0,
        "97:20 duration from titleDuration, not 0/unknown"
    );
    assert_eq!(
        mm.clips.len(),
        2,
        "layer-break split kept as ONE title, two clips"
    );
    // FEATURE_2's clip carries the 48:29 offset (45 kHz ticks) — the datum
    // that splices it onto FEATURE_1's timeline instead of restarting at 0.
    assert!(
        mm.clips[1].in_time > 100_000_000,
        "FEATURE_2 offset onto the title timeline (48:29 * 45000), got {}",
        mm.clips[1].in_time
    );
    assert_eq!(mm.chapters.len(), 2);
    assert_eq!(mm.chapters[0].name, "1", "bare ordinal chapter name");
    // Both feature halves are in ONE title's extents.
    assert!(!mm.extents.is_empty());
}

// An XPL whose titles all name absent clips composes to nothing; the scan must fall
// back to the per-clip heuristic instead of listing zero titles.
#[test]
fn scan_hddvd_falls_back_when_xpl_composes_to_nothing() {
    let mut disc = MemDisc::new();
    let xpl = r#"<?xml version="1.0"?><Playlist><TitleSet><Title>
            <PrimaryAudioVideoClip src="GONE.EVO"/></Title></TitleSet></Playlist>"#;
    let udf = make_hddvd_fs_xpl(&mut disc, &[("REAL.EVO", 100, 5000)], xpl.as_bytes());
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 1, "heuristic fallback lists the real clip");
    assert_eq!(titles[0].playlist, "REAL.EVO");
}

// Many XPL titles naming one clip must cost ONE probe (the XPL path's memo).
#[test]
fn compose_xpl_titles_probes_once_for_titles_sharing_a_clip() {
    const TITLES: usize = 64;
    let mut xpl = String::from(r#"<?xml version="1.0"?><Playlist><TitleSet>"#);
    for _ in 0..TITLES {
        xpl.push_str(r#"<Title><PrimaryAudioVideoClip src="A.EVO"/></Title>"#);
    }
    xpl.push_str("</TitleSet></Playlist>");
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_xpl(&mut disc, &[("A.EVO", 4, 5000)], xpl.as_bytes());
    let mut counter = ProbeCounter::new(disc, vec![PART_START + 5000]);
    let titles = Disc::scan_hddvd_titles(&mut counter, &udf, None).expect("scan");
    assert_eq!(titles.len(), TITLES, "every XPL title is composed");
    assert_eq!(counter.hits[0], 1, "one probe for {TITLES} titles");
}

// ── parse_vti_clip_order: bound / cap / termination edge cases ────────

// MAX_VTI_HITS must be exact, not off-by-one. All entries here are
// stride-aligned, so the cap is directly visible as the output length.
#[test]
fn parse_vti_clip_order_caps_hits_at_exact_boundary_same_residue() {
    let table_start = 0x200usize;
    let n = MAX_VTI_HITS + 50;
    let mut v = vec![0u8; table_start + n * VTI_CLIP_ENTRY_STRIDE];
    v[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
    for i in 0..n {
        let off = table_start + i * VTI_CLIP_ENTRY_STRIDE + 0x42;
        v[off..off + b"X.EVO".len()].copy_from_slice(b"X.EVO");
    }
    let out = parse_vti_clip_order(&v);
    assert_eq!(
        out.len(),
        MAX_VTI_HITS,
        "the scan must stop at exactly MAX_VTI_HITS, not one past it"
    );
}

// A name-byte run reaching the exact end of the buffer with no NUL
// terminator must not read out of bounds.
#[test]
fn parse_vti_clip_order_handles_unterminated_name_run_at_buffer_end() {
    let mut v = HDDVD_VTI_MAGIC.to_vec();
    v.extend_from_slice(b"TRAILING_JUNK_NO_TERMINATOR"); // all ascii-graphic, no NUL, ends at EOF
    let out = parse_vti_clip_order(&v);
    assert!(
        out.is_empty(),
        "unterminated trailing run yields no entries (and must not panic)"
    );
}

// A .EVO-suffixed name run followed by an in-bounds byte that is NOT a
// NUL must not be treated as terminated.
#[test]
fn parse_vti_clip_order_requires_actual_nul_terminator_not_just_in_bounds() {
    let mut v = HDDVD_VTI_MAGIC.to_vec();
    v.extend_from_slice(b"FEATURE.EVO");
    v.push(0x01); // in-bounds terminator byte, but NOT a NUL
    v.extend_from_slice(&[0u8; 16]);
    let out = parse_vti_clip_order(&v);
    assert!(
        out.is_empty(),
        "a non-NUL byte after .EVO must not count as terminated"
    );
}

// A NUL-terminated run not ending in ".EVO" must never be collected:
// NUL-termination and the .EVO-suffix check are independent gates.
#[test]
fn parse_vti_clip_order_rejects_nul_terminated_names_without_evo_suffix() {
    let mut v = HDDVD_VTI_MAGIC.to_vec();
    for _ in 0..20 {
        v.extend_from_slice(b"HELLO\0"); // nul-terminated, 5 bytes, not .EVO
    }
    let out = parse_vti_clip_order(&v);
    assert!(
        out.is_empty(),
        "non-.EVO nul-terminated names must not be collected"
    );
}

// A short (<4-byte) NUL-terminated name must be rejected by the length
// guard BEFORE the .EVO-suffix slice runs (else name.len() - 4 underflows).
#[test]
fn parse_vti_clip_order_short_circuits_length_check_before_slicing_short_names() {
    let mut v = HDDVD_VTI_MAGIC.to_vec();
    v.push(b' '); // non-name-byte separator: isolates "AB" from the magic run
    v.extend_from_slice(b"AB\0"); // 2-byte name, under the 4-byte slice width
    let out = parse_vti_clip_order(&v);
    assert!(
        out.is_empty(),
        "short name is rejected without slicing/panicking"
    );
}

// ── EVO_ES_SAMPLE_CAP / collect_es capping ─────────────────────────────

/// The documented sample cap is 128 KiB, i.e. `128 * 1024`.
#[test]
fn evo_es_sample_cap_is_128_kib() {
    assert_eq!(EVO_ES_SAMPLE_CAP, 128 * 1024);
}

/// Plain-video-range (`0xE0..=0xEF`) samples stop growing once the buffer
/// has reached the cap — a subsequent packet must not push it past.
#[test]
fn collect_es_caps_video_sample_at_the_length_cap() {
    use crate::consts::pes_stream_id::VIDEO;
    let mut video = Vec::new();
    let mut video_pid: Option<u16> = None;
    let mut audio = BTreeMap::new();
    collect_es(
        &ps_pkt(VIDEO, None, vec![0xAA; EVO_ES_SAMPLE_CAP]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(video.len(), EVO_ES_SAMPLE_CAP);
    collect_es(
        &ps_pkt(VIDEO, None, vec![0xBB; 16]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(
        video.len(),
        EVO_ES_SAMPLE_CAP,
        "no further growth once at the cap"
    );
}

/// The VC-1 extended-stream-id (0xFD, ext 0x55) video branch has its own
/// cap check; it must behave identically to the plain-video branch.
#[test]
fn collect_es_caps_vc1_video_sample_at_the_length_cap() {
    let mut video = Vec::new();
    let mut video_pid: Option<u16> = None;
    let mut audio = BTreeMap::new();
    collect_es(
        &ps_pkt(0xFD, Some(0x55), vec![0xAA; EVO_ES_SAMPLE_CAP]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(video.len(), EVO_ES_SAMPLE_CAP);
    collect_es(
        &ps_pkt(0xFD, Some(0x55), vec![0xBB; 16]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(
        video.len(),
        EVO_ES_SAMPLE_CAP,
        "no further growth once at the cap (VC-1 0xFD branch)"
    );
}

/// The per-sub-id audio branch has its own cap check; same requirement.
#[test]
fn collect_es_caps_audio_sample_at_the_length_cap() {
    use crate::consts::pes_stream_id::PRIVATE_STREAM_1;
    let mut video = Vec::new();
    let mut video_pid: Option<u16> = None;
    let mut audio = BTreeMap::new();
    collect_es(
        &ps_pkt(PRIVATE_STREAM_1, Some(0xC0), vec![0xAA; EVO_ES_SAMPLE_CAP]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(audio[&0xC0].len(), EVO_ES_SAMPLE_CAP);
    collect_es(
        &ps_pkt(PRIVATE_STREAM_1, Some(0xC0), vec![0xBB; 16]),
        &mut video,
        &mut video_pid,
        &mut audio,
    );
    assert_eq!(
        audio[&0xC0].len(),
        EVO_ES_SAMPLE_CAP,
        "no further growth once at the cap (audio branch)"
    );
}

// ── audit b03-hddvd fixes ──

#[test]
fn usable_extents_flags_a_dropped_lba0_data_extent() {
    let (exts, truncated) = usable_extents(&[(0, 10), (500, 4)]);
    assert_eq!(exts.len(), 1);
    assert!(truncated, "lba-0 data extent dropped => truncated plan");
    let (_, truncated) = usable_extents(&[(0, 0), (500, 4)]);
    assert!(!truncated, "empty extent is harmless");
}

#[test]
fn compose_xpl_titles_drops_a_title_naming_an_absent_clip() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
        "a.evo".to_string(),
        (
            "A.EVO".to_string(),
            1000u64,
            vec![Extent {
                start_lba: 1,
                sector_count: 1,
            }],
        ),
    )]
    .into_iter()
    .collect();
    let clip = |evo: &str, b: f64, e: f64| XplClip {
        evo: evo.to_string(),
        begin_secs: b,
        end_secs: e,
    };
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: 20.0,
        clips: vec![clip("a.evo", 0.0, 10.0), clip("gone.evo", 10.0, 20.0)],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("compose");
    assert!(
        titles.is_empty(),
        "title with an absent clip must be dropped"
    );
}

#[test]
fn probe_cache_shares_titles_with_the_same_probed_head() {
    let ext = |l, n| Extent {
        start_lba: l,
        sector_count: n,
    };
    let mut counter = ProbeCounter::new(MemDisc::new(), vec![1000]);
    let mut cache = EvoProbeCache::default();
    // Both extent lists are identical over the first EVO_PROBE_SECTORS.
    let a = [ext(1000, EVO_PROBE_SECTORS), ext(5000, 10)];
    let b = [ext(1000, EVO_PROBE_SECTORS), ext(6000, 10)];
    cache.streams(&mut counter, &a, None).expect("a");
    cache.streams(&mut counter, &b, None).expect("b");
    assert_eq!(counter.hits[0], 1, "same probed head must cost one probe");
}

// A disc with `ADV_OBJ/VPLST000.XPL` of `len` spaces.
fn xpl_disc(len: usize) -> (MemDisc, crate::udf::UdfFs) {
    let mut disc = MemDisc::new();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "ADV_OBJ".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file_with("VPLST000.XPL", 40, 4000, vec![b' '; len], true)],
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

// Fails every read with the error `mk` builds.
struct FailReader(fn() -> crate::error::Error);

impl SectorSource for FailReader {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        _buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        Err((self.0)())
    }
}

fn disc_read_err() -> crate::error::Error {
    crate::error::Error::DiscRead {
        sector: 0,
        status: None,
        sense: None,
    }
}

#[test]
fn read_xpl_capped_rejects_an_oversized_playlist() {
    let (mut disc, udf) = xpl_disc(MAX_XPL_BYTES + 1);
    let r = read_xpl_capped(&mut disc, &udf, "/ADV_OBJ/VPLST000.XPL");
    assert!(matches!(r, Ok(None)));
    let (r, events) =
        crate::testlog::capture(|| read_adv_obj_xpl(&mut disc, &udf).expect("no halt"));
    assert!(r.is_none());
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc" && e.field("xpl") == Some("\"VPLST000.XPL\""))
        .unwrap_or_else(|| panic!("oversized playlist must be logged; got {events:?}"));
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(
        line.field("code"),
        Some(crate::error::E_XPL_TOO_LARGE.to_string().as_str()),
        "{line:?}"
    );
}

#[test]
fn read_xpl_capped_accepts_a_playlist_of_exactly_the_cap() {
    let (mut disc, udf) = xpl_disc(MAX_XPL_BYTES);
    let r = read_xpl_capped(&mut disc, &udf, "/ADV_OBJ/VPLST000.XPL").expect("read");
    assert_eq!(r.map(|b| b.len()), Some(MAX_XPL_BYTES));
}

#[test]
fn read_adv_obj_xpl_logs_an_unreadable_playlist_and_reads_as_absent() {
    let (_, udf) = xpl_disc(100);
    let mut bad = FailReader(disc_read_err);
    let (r, events) =
        crate::testlog::capture(|| read_adv_obj_xpl(&mut bad, &udf).expect("no halt"));
    assert!(r.is_none());
    let line = events
        .iter()
        .find(|e| e.field("xpl") == Some("\"VPLST000.XPL\""))
        .unwrap_or_else(|| panic!("unreadable playlist must be logged; got {events:?}"));
    assert_eq!(
        line.field("code"),
        Some(disc_read_err().code().to_string().as_str()),
        "{line:?}"
    );
}

#[test]
fn read_adv_obj_xpl_propagates_halted() {
    let (_, udf) = xpl_disc(100);
    let mut halted = FailReader(|| crate::error::Error::Halted);
    assert!(matches!(
        read_adv_obj_xpl(&mut halted, &udf),
        Err(crate::error::Error::Halted)
    ));
}

#[test]
fn probe_evo_streams_logs_a_read_failure_and_keeps_going_without_streams() {
    let mut bad = FailReader(disc_read_err);
    let ext = Extent {
        start_lba: 777,
        sector_count: 4,
    };
    let (streams, events) = crate::testlog::capture(|| {
        probe_evo_streams(&mut bad, std::slice::from_ref(&ext), None).expect("probe")
    });
    assert!(streams.is_empty());
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc" && e.field("lba") == Some("777"))
        .unwrap_or_else(|| panic!("probe failure must be logged; got {events:?}"));
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(
        line.field("code"),
        Some(disc_read_err().code().to_string().as_str()),
        "{line:?}"
    );
}

// A Stop is never a probe failure: it surfaces, whether the drive reports it or the
// token is already raised.
#[test]
fn probe_evo_streams_surfaces_a_stop() {
    let ext = Extent {
        start_lba: 777,
        sector_count: 4,
    };
    let mut halted = FailReader(|| crate::error::Error::Halted);
    assert!(matches!(
        probe_evo_streams(&mut halted, std::slice::from_ref(&ext), None),
        Err(crate::error::Error::Halted)
    ));
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let mut counter = ProbeCounter::new(MemDisc::new(), vec![777]);
    assert!(matches!(
        probe_evo_streams(&mut counter, std::slice::from_ref(&ext), Some(&halt)),
        Err(crate::error::Error::Halted)
    ));
    assert_eq!(counter.sectors_read, 0, "a raised token reads nothing");
}

// A cancelled probe is not memoised as "no streams": the next title sharing the
// extents probes again.
#[test]
fn probe_cache_does_not_memoise_a_stop() {
    let ext = [Extent {
        start_lba: 777,
        sector_count: 4,
    }];
    let mut cache = EvoProbeCache::default();
    let mut halted = FailReader(|| crate::error::Error::Halted);
    assert!(matches!(
        cache.streams(&mut halted, &ext, None),
        Err(crate::error::Error::Halted)
    ));
    let mut counter = ProbeCounter::new(MemDisc::new(), vec![777]);
    cache.streams(&mut counter, &ext, None).expect("probe");
    assert_eq!(counter.hits[0], 1, "the extents are probed again");
}

// A Stop while resolving a clip's extents ends the scan; it is not "clip unreadable".
#[test]
fn scan_hddvd_titles_surfaces_a_stop_resolving_extents() {
    struct HaltAt(MemDisc, u32);
    impl SectorSource for HaltAt {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if lba == self.1 {
                return Err(crate::error::Error::Halted);
            }
            self.0.read_sectors(lba, count, buf, recovery)
        }
    }
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());
    let mut reader = HaltAt(disc, PART_START + 100); // FEATURE.EVO's ICB
    let res = Disc::scan_hddvd_titles(&mut reader, &udf, None);
    assert!(matches!(res, Err(crate::error::Error::Halted)), "{res:?}");

    let halt = crate::halt::Halt::new();
    halt.cancel();
    let mut counter = ProbeCounter::new(reader.0, vec![]);
    let res = Disc::scan_hddvd_titles(&mut counter, &udf, Some(&halt));
    assert!(matches!(res, Err(crate::error::Error::Halted)), "{res:?}");
    assert_eq!(counter.sectors_read, 0, "a raised token reads nothing");
}

// A title whose end precedes its begin never reports a negative duration.
#[test]
fn compose_xpl_titles_clamps_a_reversed_clip_duration_and_names_by_number() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
        "a.evo".to_string(),
        (
            "A.EVO".to_string(),
            1000u64,
            vec![Extent {
                start_lba: 1,
                sector_count: 1,
            }],
        ),
    )]
    .into_iter()
    .collect();
    let xpl_titles = vec![XplTitle {
        number: 7,
        name: String::new(),
        duration_secs: 10.0,
        clips: vec![XplClip {
            evo: "a.evo".to_string(),
            begin_secs: 9.0,
            end_secs: 5.0,
        }],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("compose");
    assert_eq!(titles[0].clips[0].duration_secs, 0.0);
    assert_eq!(
        titles[0].playlist, "TITLE_7",
        "an unnamed title is named by number"
    );
}

// A title with no displayName is named by its id attribute.
#[test]
fn parse_xpl_titles_falls_back_to_the_id_attribute_for_the_name() {
    let xpl = r#"<Playlist><TitleSet timeBase="60fps">
            <Title titleNumber="3" titleDuration="00:00:10:00" id="Bonus3">
              <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:00:10:00" src="file:///x/B.MAP"/>
            </Title></TitleSet></Playlist>"#;
    let titles = parse_xpl_titles(xpl.as_bytes());
    assert_eq!(titles[0].name, "Bonus3");
}

// ── read_adv_obj_xpl: prefix AND suffix are both required ──────────────

/// A file matching the `vplst` prefix but NOT the `.xpl` suffix must not
/// be adopted as the playlist — both conditions are independently
/// required, one must not be short-circuited away by the other.
#[test]
fn read_adv_obj_xpl_requires_both_vplst_prefix_and_xpl_suffix() {
    let mut disc = MemDisc::new();
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "ADV_OBJ".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file_with(
                "VPLST_NOTES.TXT",
                40,
                4000,
                b"not a playlist".to_vec(),
                true,
            )],
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    assert!(
        read_adv_obj_xpl(&mut disc, &udf).expect("read").is_none(),
        "prefix match alone (not ending .xpl) must not select a file"
    );
}

// An XPL title naming a clip with no truthful read plan must be dropped
// whole (not composed short): `unusable` distinguishes "doesn't exist"
// from "exists but can't be ripped truthfully".
#[test]
fn compose_xpl_titles_drops_a_title_naming_an_unusable_clip() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
        "a.evo".to_string(),
        (
            "A.EVO".to_string(),
            1000u64,
            vec![Extent {
                start_lba: 1,
                sector_count: 1,
            }],
        ),
    )]
    .into_iter()
    .collect();
    // b.evo exists on the disc but carries an unrecorded extent.
    let unusable: std::collections::HashSet<String> = ["b.evo".to_string()].into_iter().collect();
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: 20.0,
        clips: vec![
            XplClip {
                evo: "a.evo".to_string(),
                begin_secs: 0.0,
                end_secs: 10.0,
            },
            XplClip {
                evo: "b.evo".to_string(),
                begin_secs: 10.0,
                end_secs: 20.0,
            },
        ],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(&mut disc, &xpl_titles, &clip_extents, &unusable, None)
        .expect("compose");
    assert!(
        titles.is_empty(),
        "half of this title's runtime cannot be read, so composing it \
             would ship a short title as a complete one; got {:?}",
        titles
            .iter()
            .map(|t| (t.clips.len(), t.size_bytes))
            .collect::<Vec<_>>()
    );
}

// ── compose_xpl_titles: size/offset arithmetic ─────────────────────────

// Clip sizes are SUMMED (not multiplied), in/out ticks are
// seconds * 45000 (not divided), duration_secs is end - begin.
#[test]
fn compose_xpl_titles_sums_sizes_and_computes_in_out_times() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
        (
            "a.evo".to_string(),
            (
                "A.EVO".to_string(),
                1000u64,
                vec![Extent {
                    start_lba: 1,
                    sector_count: 1,
                }],
            ),
        ),
        (
            "b.evo".to_string(),
            (
                "B.EVO".to_string(),
                2000u64,
                vec![Extent {
                    start_lba: 2,
                    sector_count: 1,
                }],
            ),
        ),
    ]
    .into_iter()
    .collect();
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: 10.0,
        clips: vec![
            XplClip {
                evo: "a.evo".to_string(),
                begin_secs: 2.0,
                end_secs: 5.0,
            },
            XplClip {
                evo: "b.evo".to_string(),
                begin_secs: 5.0,
                end_secs: 9.0,
            },
        ],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("compose");
    assert_eq!(titles.len(), 1);
    let t = &titles[0];
    assert_eq!(t.size_bytes, 3000, "clip sizes summed, not multiplied");
    assert_eq!(
        t.clips[0].in_time,
        (2.0f64 * 45000.0) as u32,
        "in_time is begin_secs * 45000, not divided"
    );
    assert_eq!(
        t.clips[0].out_time,
        (5.0f64 * 45000.0) as u32,
        "out_time is end_secs * 45000, not divided"
    );
    assert!(
        (t.clips[0].duration_secs - 3.0).abs() < 1e-9,
        "duration_secs is end_secs - begin_secs, not +/÷: got {}",
        t.clips[0].duration_secs
    );
    assert!(
        (t.clips[1].duration_secs - 4.0).abs() < 1e-9,
        "second clip's duration is also end - begin: got {}",
        t.clips[1].duration_secs
    );
}

// size_bytes sums DISC-DECLARED clip sizes, uncross-checked against real extents, so two
// clips near u64::MAX must saturate, not panic/wrap.
#[test]
fn compose_xpl_titles_saturates_absurd_disc_declared_clip_sizes() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
        (
            "a.evo".to_string(),
            (
                "A.EVO".to_string(),
                u64::MAX,
                vec![Extent {
                    start_lba: 1,
                    sector_count: 1,
                }],
            ),
        ),
        (
            "b.evo".to_string(),
            (
                "B.EVO".to_string(),
                u64::MAX,
                vec![Extent {
                    start_lba: 2,
                    sector_count: 1,
                }],
            ),
        ),
    ]
    .into_iter()
    .collect();
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: 10.0,
        clips: vec![
            XplClip {
                evo: "a.evo".to_string(),
                begin_secs: 0.0,
                end_secs: 5.0,
            },
            XplClip {
                evo: "b.evo".to_string(),
                begin_secs: 5.0,
                end_secs: 10.0,
            },
        ],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("a hostile size field must not fail the scan either");
    assert_eq!(titles.len(), 1);
    assert_eq!(
        titles[0].size_bytes,
        u64::MAX,
        "the sum must saturate at u64::MAX, never wrap to a small number"
    );
}

// A crafted playlist can name the SAME.evo many times over; each repeat must NOT push
// another copy of its extents onto the title (dedup keyed on.evo, mirroring bluray.rs's
// seen_clips gate).
#[test]
fn compose_xpl_titles_dedups_repeated_clip_references_by_evo() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
        "a.evo".to_string(),
        (
            "A.EVO".to_string(),
            1000u64,
            vec![
                Extent {
                    start_lba: 1,
                    sector_count: 1,
                },
                Extent {
                    start_lba: 2,
                    sector_count: 1,
                },
                Extent {
                    start_lba: 3,
                    sector_count: 1,
                },
            ],
        ),
    )]
    .into_iter()
    .collect();

    const REPEATS: usize = 5000;
    let clips: Vec<XplClip> = (0..REPEATS)
        .map(|i| XplClip {
            evo: "a.evo".to_string(),
            begin_secs: i as f64,
            end_secs: i as f64 + 1.0,
        })
        .collect();
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: REPEATS as f64,
        clips,
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("compose");
    assert_eq!(titles.len(), 1);
    assert_eq!(
        titles[0].extents.len(),
        3,
        "the same .evo named {REPEATS} times must contribute its 3 extents \
             ONCE, not {REPEATS} times over — got {} extents",
        titles[0].extents.len()
    );
}

// Control for the de-dup above: several DISTINCT clips must all still
// resolve — the key must not accidentally collapse different clips.
#[test]
fn compose_xpl_titles_resolves_all_distinct_clips_despite_dedup() {
    let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
        (
            "a.evo".to_string(),
            (
                "A.EVO".to_string(),
                1000u64,
                vec![Extent {
                    start_lba: 1,
                    sector_count: 1,
                }],
            ),
        ),
        (
            "b.evo".to_string(),
            (
                "B.EVO".to_string(),
                2000u64,
                vec![Extent {
                    start_lba: 2,
                    sector_count: 1,
                }],
            ),
        ),
        (
            "c.evo".to_string(),
            (
                "C.EVO".to_string(),
                3000u64,
                vec![Extent {
                    start_lba: 3,
                    sector_count: 1,
                }],
            ),
        ),
    ]
    .into_iter()
    .collect();
    let xpl_titles = vec![XplTitle {
        number: 1,
        name: "T".to_string(),
        duration_secs: 30.0,
        clips: vec![
            XplClip {
                evo: "a.evo".to_string(),
                begin_secs: 0.0,
                end_secs: 5.0,
            },
            XplClip {
                evo: "b.evo".to_string(),
                begin_secs: 5.0,
                end_secs: 15.0,
            },
            XplClip {
                evo: "c.evo".to_string(),
                begin_secs: 15.0,
                end_secs: 30.0,
            },
        ],
        chapters: vec![],
    }];
    let mut disc = MemDisc::new();
    let titles = compose_xpl_titles(
        &mut disc,
        &xpl_titles,
        &clip_extents,
        &std::collections::HashSet::new(),
        None,
    )
    .expect("compose");
    assert_eq!(titles.len(), 1);
    assert_eq!(
        titles[0].extents.len(),
        3,
        "three DISTINCT clips must all resolve — one extent each, not \
             collapsed by an over-eager de-dup key"
    );
    assert_eq!(
        titles[0].size_bytes, 6000,
        "all three distinct sizes summed"
    );
}

// scan_hddvd_titles VTI selection: a file with the real ADVANCED-VTS
// magic but the wrong extension must never be adopted as the nav file —
// falls back to one title per clip, not a same-content impostor by magic.
#[test]
fn scan_hddvd_titles_ignores_a_vti_look_alike_with_the_wrong_extension() {
    let mut disc = MemDisc::new();
    let vti_bytes = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
    let files = vec![
        file_with("IMPOSTER.DAT", 90, 20000, vti_bytes, true),
        file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
        file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(
        titles.len(),
        2,
        "no real .vti present -> no VTI-driven composition, one title per clip"
    );
}

// A clip whose extents cannot be resolved must drop the SPLIT FEATURE (not compose it from
// the remaining part as if whole).
#[test]
fn scan_hddvd_titles_drops_a_split_feature_whose_part_cannot_be_read() {
    let mut disc = MemDisc::new();
    // The VTI clip table is the ONLY source of authored order, and the
    // composed feature title exists only for clips it names. Without it
    // this test cannot distinguish the fix from its absence.
    let vti_bytes = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
    let files = vec![
        file_with("HVA00001.VTI", 90, 20000, vti_bytes, true),
        file("FEATURE_1.EVO", 100, 5000, 4 * 2048, true),
        file("FEATURE_2.EVO", 101, 9000, 4 * 2048, true),
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    // Blank FEATURE_2's ICB: descriptor tag becomes 0 (neither 261 nor
    // 266), what the parser sees when an ICB sector can't be read back
    // intact. Deliberately not an unrecorded extent — that's covered elsewhere.
    disc.put_bytes(PART_START + 101, &[0u8; 2048]);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    assert!(
        matches!(
            udf.file_extents(&mut disc, "/HVDVD_TS/FEATURE_2.EVO"),
            Err(crate::error::Error::DiscRead { .. })
        ),
        "fixture must fail with DiscRead, not UdfUnrecordedExtent"
    );
    // Guard the guard: if the VTI ever stopped parsing, `order` would be
    // empty and the assertion below would hold for the wrong reason.
    assert_eq!(
        parse_vti_clip_order(&synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"])),
        vec!["FEATURE_1.EVO".to_string(), "FEATURE_2.EVO".to_string()],
        "fixture VTI must yield both feature parts in authored order"
    );

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    // FEATURE_1 alone must not be offered as the feature. It may still
    // appear as its own standalone clip title — that stands alone and is
    // honest — but nothing may present it as the composed whole.
    let composed: Vec<_> = titles
        .iter()
        .filter(|t| t.clips.len() > 1 || t.playlist.eq_ignore_ascii_case("FEATURE"))
        .map(|t| &t.playlist)
        .collect();
    assert!(
        composed.is_empty(),
        "a split feature missing one part must not compose; got {composed:?}"
    );
}

// A zero-byte clip (ICB AD data_len == 0, the UDF AD-list terminator) must not produce a
// title. Exercises the upstream file_extents terminator path, not the scan's own
// zero-sector guard.
#[test]
fn scan_hddvd_titles_excludes_a_clip_with_zero_sectors() {
    let mut disc = MemDisc::new();
    let files = vec![
        file("REAL.EVO", 100, 5000, 4 * 2048, true), // ordinary, valid clip
        file("BOGUS.EVO", 101, 9000, 0, true),       // size 0 -> zero-sector extent
    ];
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(
        titles.len(),
        1,
        "the zero-sector clip must not produce a title"
    );
    assert_eq!(titles[0].playlist, "REAL.EVO");
}

// ── probe_evo_streams: sector-cursor bookkeeping ───────────────────────

// A zero-sector extent must be skipped outright, never entering the read
// loop (a `left >= 0` tautology would spin forever: nothing would advance).
#[test]
fn probe_evo_streams_skips_a_zero_sector_extent_without_reading() {
    let mut disc = MemDisc::new();
    let extent = Extent {
        start_lba: 500_000,
        sector_count: 0,
    };
    let streams = probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
    assert!(streams.is_empty(), "a zero-sector extent yields no streams");
}

/// The read cursor must advance FORWARD by each chunk's sector count, not
/// backward — real content living only in the second 512-sector (1 MiB)
/// chunk must be reached.
#[test]
fn probe_evo_streams_advances_lba_forward_across_chunk_reads() {
    let mut disc = MemDisc::new();
    let start_lba = 100_000u32;
    // First chunk (512 sectors): inert filler, no start codes.
    disc.put_bytes(start_lba, &vec![0x55u8; 512 * 2048]);
    // Second chunk: the real EVO content (H.264 video PES).
    let evo = synthetic_evo();
    disc.put_bytes(start_lba + 512, &evo);
    let extent = Extent {
        start_lba,
        sector_count: 512 + (evo.len() as u32).div_ceil(2048),
    };

    let streams = probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
    let has_h264 = streams
        .iter()
        .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
    assert!(
        has_h264,
        "the second 1 MiB chunk must be read from the correct (forward) LBA"
    );
}

/// The read loop must stop at the extent's DECLARED `sector_count` — data
/// living just past it must never be read (a buffer over-read past the
/// caller-supplied extent bound, on untrusted disc-layout input).
#[test]
fn probe_evo_streams_stops_reading_at_the_extents_declared_sector_count() {
    let mut disc = MemDisc::new();
    let start_lba = 200_000u32;
    let declared_sectors = 4u32;
    disc.put_bytes(start_lba, &vec![0x55u8; declared_sectors as usize * 2048]);
    // Real H.264 PES data placed just PAST the declared extent — must
    // never be read.
    let evo = synthetic_evo();
    disc.put_bytes(start_lba + declared_sectors, &evo);
    let extent = Extent {
        start_lba,
        sector_count: declared_sectors,
    };

    let streams = probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
    let has_h264 = streams
        .iter()
        .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
    assert!(
        !has_h264,
        "must not read past the extent's declared sector_count"
    );
}

/// The total read budget (`EVO_PROBE_SECTORS`) must be enforced ACROSS
/// extents, not just within one — once it is exhausted by an earlier
/// extent, a later extent in the same probe must not be read at all.
#[test]
fn probe_evo_streams_caps_total_reads_across_extents_at_evo_probe_sectors() {
    let mut disc = MemDisc::new();
    let first_lba = 300_000u32;
    disc.put_bytes(first_lba, &vec![0x55u8; EVO_PROBE_SECTORS as usize * 2048]);
    // A second extent, following the first in the extents list: once the
    // whole EVO_PROBE_SECTORS budget is spent on the first, this must
    // never be reached.
    let second_lba = first_lba + EVO_PROBE_SECTORS;
    let evo = synthetic_evo();
    disc.put_bytes(second_lba, &evo);

    let extents = vec![
        Extent {
            start_lba: first_lba,
            sector_count: EVO_PROBE_SECTORS,
        },
        Extent {
            start_lba: second_lba,
            sector_count: 10,
        },
    ];
    let streams = probe_evo_streams(&mut disc, &extents, None).expect("probe");
    let has_h264 = streams
        .iter()
        .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
    assert!(
        !has_h264,
        "must not read past the total EVO_PROBE_SECTORS budget across extents"
    );
}

// clip-name fallback probe amplification: counts reads STARTing at each
// watched LBA plus sectors pulled. A probe's first read is always at the
// clip's first extent LBA, so the hit count IS the number of passes.
struct ProbeCounter {
    inner: MemDisc,
    watch: Vec<u32>,
    hits: Vec<u32>,
    sectors_read: u64,
}

impl ProbeCounter {
    fn new(inner: MemDisc, watch: Vec<u32>) -> Self {
        let hits = vec![0; watch.len()];
        Self {
            inner,
            watch,
            hits,
            sectors_read: 0,
        }
    }
}

impl SectorSource for ProbeCounter {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        if let Some(i) = self.watch.iter().position(|w| *w == lba) {
            self.hits[i] += 1;
        }
        self.sectors_read += count as u64;
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

/// Lay an `HVDVD_TS/` holding exactly the given `(name, icb_lba, data_lba,
/// size)` clips — unlike `make_hddvd_fs`, the caller chooses each entry's
/// ICB, so many names can be pointed at ONE File Entry.
fn lay_hddvd_clips(disc: &mut MemDisc, specs: Vec<crate::udf::fixture::FileSpec>) {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "HVDVD_TS".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: specs,
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
}

// THE THIRD AMPLIFICATION AXIS: the clip-name FALLBACK (reached by omitting /ADV_OBJ) emits
// one title per directory entry; many entries resolving to the SAME extents must cost ONE
// probe.
#[test]
fn scan_hddvd_titles_probes_once_for_entries_resolving_to_identical_extents() {
    const ENTRIES: usize = 512;
    const SHARED_ICB: u32 = 100;
    const SHARED_DATA: u32 = 5000;

    let mut disc = MemDisc::new();
    // Every FID names a different file and points at the SAME File Entry.
    let specs: Vec<_> = (0..ENTRIES)
        .map(|i| {
            file(
                &format!("C{i:04}.EVO"),
                SHARED_ICB,
                SHARED_DATA,
                4 * 2048,
                true,
            )
        })
        .collect();
    lay_hddvd_clips(&mut disc, specs);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let probe_lba = PART_START + SHARED_DATA;
    let mut counter = ProbeCounter::new(disc, vec![probe_lba]);
    let titles = Disc::scan_hddvd_titles(&mut counter, &udf, None).expect("scan");

    assert_eq!(
        counter.hits[0], 1,
        "{ENTRIES} directory entries all resolving to the SAME extents must \
             cost ONE probe_evo_streams pass, not one per entry; got {} probes",
        counter.hits[0]
    );
    // And the whole scan's read volume stays bounded — the real currency
    // here is drive time, not title count.
    assert!(
        counter.sectors_read < 4 * ENTRIES as u64,
        "total sectors read must not scale with the entry count, got {}",
        counter.sectors_read
    );
    assert!(!titles.is_empty(), "the clips still enumerate");
}

// The clip cap and the per-probe read budget are ONE bound, not two: only their PRODUCT
// (worst-case drive time) is meaningful, capped at 8 GiB.
#[test]
fn the_clip_cap_and_probe_budget_bound_a_scans_worst_case_read_volume() {
    const CEILING_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    let budget =
        |cap: usize| cap as u64 * u64::from(EVO_PROBE_SECTORS) * crate::consts::SECTOR_BYTES as u64;
    // Both paths pay one probe per item, and a crafted disc reaches
    // either: the directory fallback when `/ADV_OBJ` is omitted, the
    // playlist path when present. Capping only one moves the amplification.
    for (name, cap) in [
        ("MAX_HDDVD_CLIPS", MAX_HDDVD_CLIPS),
        ("MAX_XPL_TITLES", MAX_XPL_TITLES),
    ] {
        let worst_case = budget(cap);
        assert!(
            worst_case <= CEILING_BYTES,
            "{name} ({cap}) x EVO_PROBE_SECTORS ({EVO_PROBE_SECTORS}) = \
                 {worst_case} bytes of probe reads, over the {CEILING_BYTES}-byte \
                 ceiling a scan can absorb"
        );
    }
}

// CONTROL: memoization must not silently collapse DISTINCT clips — each title must keep the
// streams of ITS OWN clip.
#[test]
fn scan_hddvd_titles_still_probes_each_distinct_clip() {
    let mut disc = MemDisc::new();
    let evo = synthetic_evo();
    let junk = vec![0x55u8; 4 * 2048];
    let (a_data, b_data, c_data) = (5000u32, 6000u32, 7000u32);
    lay_hddvd_clips(
        &mut disc,
        vec![
            file_with("A_REAL.EVO", 100, a_data, evo.clone(), true),
            file_with("B_JUNK.EVO", 101, b_data, junk, true),
            file_with("C_REAL.EVO", 102, c_data, evo, true),
        ],
    );
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let watch = vec![
        PART_START + a_data,
        PART_START + b_data,
        PART_START + c_data,
    ];
    let mut counter = ProbeCounter::new(disc, watch);
    let titles = Disc::scan_hddvd_titles(&mut counter, &udf, None).expect("scan");

    assert_eq!(
        titles.len(),
        3,
        "every distinct clip still resolves a title"
    );

    // The substantive assertion first: each title carries the streams of ITS
    // OWN clip. This is what a wrong memo key actually costs.
    let by_name = |n: &str| titles.iter().find(|t| t.playlist == n).expect(n);
    assert!(
        by_name("A_REAL.EVO")
            .streams
            .iter()
            .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
        "the real clip keeps its own probed streams"
    );
    assert!(
        by_name("B_JUNK.EVO").streams.is_empty(),
        "the unrecognizable clip must NOT inherit another clip's streams"
    );
    assert!(
        by_name("C_REAL.EVO")
            .streams
            .iter()
            .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
        "a third distinct clip is probed on its own extents"
    );

    assert_eq!(
        counter.hits,
        vec![1, 1, 1],
        "each DISTINCT clip is still probed exactly once"
    );
}

// Memoization alone is NOT the whole fix: distinct extent lists (each a 1-sector File
// Entry) all miss the memo, so MAX_HDDVD_CLIPS is what bounds that.
#[test]
fn scan_hddvd_titles_caps_a_directory_declaring_absurdly_many_clips() {
    const ENTRIES: usize = MAX_HDDVD_CLIPS + 300;

    let mut disc = MemDisc::new();
    // Each entry gets its OWN File Entry and its OWN data extent, so no two
    // resolve to the same extent list and the memo never hits.
    let specs: Vec<_> = (0..ENTRIES)
        .map(|i| {
            // ICBs live far past the directory's own FID data (~230 KB
            // from LBA 21) so laying the FIDs cannot clobber them.
            file(
                &format!("C{i:05}.EVO"),
                100_000 + i as u32,
                1_000_000 + i as u32 * 8,
                2048,
                true,
            )
        })
        .collect();
    lay_hddvd_clips(&mut disc, specs);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(
        titles.len(),
        MAX_HDDVD_CLIPS,
        "a directory declaring {ENTRIES} clips must be capped at \
             {MAX_HDDVD_CLIPS}; each surviving clip costs a real \
             probe_evo_streams pass over the medium"
    );
}

/// The control for the cap: a realistic disc keeps every clip it carries.
#[test]
fn scan_hddvd_titles_keeps_every_clip_of_a_realistic_disc() {
    let mut disc = MemDisc::new();
    let udf = make_hddvd_fs(
        &mut disc,
        &[
            ("FEATURE_1.EVO", 2000, 5000),
            ("FEATURE_2.EVO", 1800, 9000),
            ("TRAILER.EVO", 300, 12000),
            ("DELOGO.EVO", 40, 13000),
        ],
    );
    let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(
        titles.len(),
        4,
        "a real disc's clips must survive the cap untouched"
    );
}

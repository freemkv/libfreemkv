use super::*;
// Needed only by the tests: the production code in this file no longer names
// these types directly, since the local channel/sample-rate duplicates were
// deleted in favour of the canonical accessors.
use crate::disc::{AudioChannels, SampleRate};

// The main-feature decision row must NAME the comparator's real sort keys, not a restated
// (driftable) copy.
#[test]
fn main_feature_reason_names_the_comparators_real_keys() {
    use crate::disc::{Clip, Disc, DiscTitle};

    let sized = |size_bytes: u64, n_clips: usize| DiscTitle {
        size_bytes,
        clips: (0..n_clips)
            .map(|i| Clip {
                feed_span: None,
                clip_id: format!("{i:05}"),
                in_time: 0,
                out_time: 0,
                duration_secs: 0.0,
                source_packets: 0,
            })
            .collect(),
        ..DiscTitle::empty()
    };
    // A 40-clip 8 GB title beats a 1-clip 1 GB title: the comparator's primary
    // key among disc-fitting titles is LARGEST SIZE, not "fewest clips" (the
    // drifted string described a rule the comparator does not implement).
    let many_clips_big = sized(8_000_000_000, 40);
    let one_clip_small = sized(1_000_000_000, 1);
    assert_eq!(
        Disc::canonical_title_order(&many_clips_big, &one_clip_small, 25_000_000_000),
        std::cmp::Ordering::Less,
        "largest size wins regardless of clip count"
    );

    let reason = main_feature_reason();
    assert!(
        !reason.contains("clips"),
        "the reason must not advertise a clip-count key the comparator dropped: {reason}"
    );
    // Equal-size seamless siblings (issue #45) are split ONLY by the final
    // lowest-playlist-id key, so the reason must name it.
    let sibling = |playlist_id: u16| DiscTitle {
        playlist_id,
        ..sized(8_000_000_000, 1)
    };
    assert_eq!(
        Disc::canonical_title_order(&sibling(800), &sibling(808), 25_000_000_000),
        std::cmp::Ordering::Less,
        "the comparator's final key is lowest playlist id"
    );
    assert!(
        reason.ends_with(", lowest-playlist-id)"),
        "the final tiebreak must be named: {reason}"
    );
    assert_eq!(
        reason,
        "main_feature_order(nav-feature, authoring-feature, standalone, has-video, fits-disc, largest-size, longest, richest-audio, more-video, more-subs, lowest-playlist-id)",
        "the reason must name the selection keys (authoring, standalone-over-composite, and has-video gates, then the physical keys) in priority order"
    );
}

// A track index that does not fit the u8 record field is skipped, not aliased.
#[test]
fn opening_capture_skips_track_indices_above_255() {
    let path = std::env::temp_dir().join(format!("fmk-diag-hi-{}.bin", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    let mut cap = OpeningCapture {
        file,
        counts: vec![0; 300],
    };
    cap.record(256, 0, true, b"x");
    let len = std::fs::metadata(&path).unwrap().len();
    let _ = std::fs::remove_file(&path);
    assert_eq!(len, 0, "idx 256 must not write an aliased track-0 record");
    assert_eq!(cap.counts[256], 0);
}

// A failed write may leave a torn record, so capture stops on every track.
#[test]
fn opening_capture_write_error_disables_all_tracks() {
    let path = std::env::temp_dir().join(format!("fmk-diag-ro-{}.bin", std::process::id()));
    std::fs::write(&path, b"").unwrap();
    let file = std::fs::File::open(&path).unwrap(); // read-only: writes fail
    let mut cap = OpeningCapture {
        file,
        counts: vec![0; 2],
    };
    cap.record(0, 0, true, b"x");
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        cap.counts[1], OPENING_FRAMES_PER_TRACK,
        "other tracks must stop after a write error"
    );
}

// The per-track cap stops one track at OPENING_FRAMES_PER_TRACK records without
// affecting another, and an out-of-range track index is ignored.
#[test]
fn opening_capture_caps_each_track_and_ignores_unknown_tracks() {
    let path = std::env::temp_dir().join(format!("fmk-diag-cap-{}.bin", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    let mut cap = OpeningCapture {
        file,
        counts: vec![0; 2],
    };
    for _ in 0..OPENING_FRAMES_PER_TRACK + 5 {
        cap.record(0, 0, true, b"x");
    }
    cap.record(1, 0, false, b"y");
    cap.record(2, 0, true, b"z");
    let len = std::fs::metadata(&path).unwrap().len();
    let _ = std::fs::remove_file(&path);
    assert_eq!(cap.counts, vec![OPENING_FRAMES_PER_TRACK, 1]);
    assert_eq!(len as usize, (OPENING_FRAMES_PER_TRACK + 1) * 15);
}

// With the diag target off, no side file is created and the rip is unaffected.
#[test]
fn opening_capture_new_is_none_when_diag_is_off() {
    let path = std::env::temp_dir().join(format!("fmk-diag-off-{}.mkv", std::process::id()));
    assert!(OpeningCapture::new(&path, 3).is_none());
    let mut side = path.as_os_str().to_os_string();
    side.push(".opening.bin");
    assert!(!std::path::Path::new(&side).exists());
}

#[test]
fn res_str_keeps_interlace_marker() {
    assert_eq!(res_str(Resolution::R576i), "576i");
    assert_eq!(res_str(Resolution::R480i), "480i");
    assert_eq!(res_str(Resolution::R2160p), "2160p");
}

#[test]
fn fps_and_tv_system() {
    assert_eq!(fps_str(FrameRate::F25), "25");
    assert_eq!(tv_system_str(FrameRate::F25), "PAL");
    assert_eq!(fps_str(FrameRate::F29_97), "29.97");
    assert_eq!(tv_system_str(FrameRate::F29_97), "NTSC");
    assert_eq!(tv_system_str(FrameRate::F50), "PAL");
    assert_eq!(tv_system_str(FrameRate::F23_976), "NTSC");
    assert_eq!(tv_system_str(FrameRate::F59_94), "NTSC");
}

#[test]
fn color_and_hdr() {
    assert_eq!(color_str(ColorSpace::Bt470bg), "BT.470BG");
    assert_eq!(color_str(ColorSpace::Bt2020), "BT.2020");
    assert_eq!(hdr_str(HdrFormat::Hdr10), "HDR10");
    assert_eq!(hdr_str(HdrFormat::DolbyVision), "DoVi");
    assert_eq!(hdr_str(HdrFormat::Sdr), "SDR");
}

/// Moved from the deleted local duplicates onto the canonical accessors,
/// with the Unknown case added — which is the whole point of the change.
#[test]
fn channel_count_matches_layout_and_is_zero_when_unknown() {
    assert_eq!(AudioChannels::Mono.count(), 1);
    assert_eq!(AudioChannels::Stereo.count(), 2);
    assert_eq!(AudioChannels::Surround51.count(), 6);
    assert_eq!(AudioChannels::Surround71.count(), 8);
    // The one that matters. This used to return 6, which is indistinguishable
    // from a real 5.1 track and left every caller responsible for checking
    // the variant first.
    assert_eq!(
        AudioChannels::Unknown.count(),
        0,
        "an unknown layout must not report a plausible channel count"
    );
}

#[test]
fn sample_rate_hz_values_and_zero_when_unknown() {
    assert_eq!(SampleRate::S48.hz(), 48000.0);
    assert_eq!(SampleRate::S96.hz(), 96000.0);
    assert_eq!(
        SampleRate::Unknown.hz(),
        0.0,
        "an unknown sample rate must not report a plausible 48 kHz"
    );
}

#[test]
fn codec_private_hex_renders_caps_and_handles_empty() {
    // None / empty → "none" (no hex). The Windows-fps diagnosis only needs
    // the seq-header prefix, so render it but cap long blobs.
    assert_eq!(codec_private_hex(None), "none");
    assert_eq!(codec_private_hex(Some(&[])), "none");
    // Short blob: full uppercase hex, no suffix. An MPEG-2 seq header starts
    // 00 00 01 B3 — exactly what a reader greps for in a bug log.
    assert_eq!(
        codec_private_hex(Some(&[0x00, 0x00, 0x01, 0xB3])),
        "000001B3"
    );
    // Over the cap: first CODEC_PRIVATE_HEX_CAP bytes + a "..(+NB)" summary.
    let big = vec![0xABu8; CODEC_PRIVATE_HEX_CAP + 5];
    let s = codec_private_hex(Some(&big));
    assert!(s.starts_with(&"AB".repeat(CODEC_PRIVATE_HEX_CAP)), "{s}");
    assert!(s.ends_with("..(+5B)"), "{s}");
}

#[test]
fn frame_record_layout_is_parseable() {
    // The .opening.bin record framing must round-trip so a future tool can
    // split the side file back into frames without the disc:
    // [track:u8][keyframe:u8][pts_ns:i64 LE][len:u32 LE][raw bytes].
    let data = [0xDEu8, 0xAD, 0xBE, 0xEF];
    let rec = frame_record(2, -40_000_000, true, &data);
    assert_eq!(rec.len(), 14 + data.len());
    assert_eq!(rec[0], 2, "track index");
    assert_eq!(rec[1], 1, "keyframe flag");
    assert_eq!(
        i64::from_le_bytes(rec[2..10].try_into().unwrap()),
        -40_000_000,
        "pts_ns survives (signed — opening back-anchor can be negative)"
    );
    assert_eq!(
        u32::from_le_bytes(rec[10..14].try_into().unwrap()),
        4,
        "len"
    );
    assert_eq!(&rec[14..], &data, "raw frame bytes follow");
    // A non-keyframe records the flag as 0.
    let delta = frame_record(0, 0, false, &[]);
    assert_eq!(delta[1], 0);
    assert_eq!(u32::from_le_bytes(delta[10..14].try_into().unwrap()), 0);
}

/// The cell row shows the raw category byte (0xNN) beside the decode, and
/// the keep/drop verdict. A plain feature cell (0x00) is "keep"; a leading
/// secondary-block cell flagged dropped reads "DROP".
#[test]
fn cell_row_shows_raw_byte_and_verdict() {
    let plain = crate::ifo::DvdCell {
        first_sector: 100,
        last_sector: 199,
        category: 0x00,
        duration_secs: 12.5,
    };
    let row = dvd_cell_row(0, &plain, false);
    assert!(row.contains("cat=0x00"), "{row}");
    assert!(row.contains("block_mode=0"), "{row}");
    assert!(row.contains("first=100"), "{row}");
    assert!(row.contains("last=199"), "{row}");
    assert!(row.contains("dur=12.5s"), "{row}");
    assert!(row.contains("keep(plain-feature)"), "{row}");
    assert!(!row.contains("DROP"), "{row}");

    // 0x90 = in-block cell of an angle block (block_mode=2, block_type=1),
    // shown dropped as a leading secondary piece.
    let sec = crate::ifo::DvdCell {
        first_sector: 0,
        last_sector: 9,
        category: 0x90,
        duration_secs: 1.0,
    };
    let row = dvd_cell_row(0, &sec, true);
    assert!(row.contains("cat=0x90"), "{row}");
    assert!(row.contains("block_mode=2"), "{row}");
    assert!(row.contains("block_type=1"), "{row}");
    assert!(row.contains("DROP(leading-secondary-block-piece)"), "{row}");

    // The same secondary piece past the leading run is kept, as feature body.
    let row = dvd_cell_row(3, &sec, false);
    assert!(row.contains("keep(feature-body)"), "{row}");
    assert!(!row.contains("keep(plain-feature)"), "{row}");
}

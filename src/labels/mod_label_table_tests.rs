use super::*;
use crate::disc::{AudioChannels, Codec, HdrFormat, Stream, SubtitleStream};
use crate::udf::fixture::*;

fn vendor(t: StreamLabelType, n: u16) -> StreamLabel {
    StreamLabel {
        stream_id: None,
        stream_number: n,
        stream_type: t,
        language: "eng".into(),
        name: String::new(),
        purpose: LabelPurpose::Normal,
        qualifier: LabelQualifier::None,
        codec_hint: String::new(),
        variant: String::new(),
    }
}

fn derived(clip: &str, pid: u16, n: u16) -> StreamLabel {
    StreamLabel {
        stream_id: Some(StreamId {
            clip_id: clip.into(),
            pid,
        }),
        ..vendor(StreamLabelType::Audio, n)
    }
}

#[test]
fn panic_message_escapes_control_characters() {
    let p = std::panic::catch_unwind(|| panic!("bad \x1b[31m disc")).unwrap_err();
    assert_eq!(panic_message(p.as_ref()), "bad \\u{1b}[31m disc");
}

#[test]
fn iso639_b_codes_map_to_their_t_codes() {
    for (b, t) in [
        ("alb", "sqi"),
        ("arm", "hye"),
        ("baq", "eus"),
        ("bur", "mya"),
        ("chi", "zho"),
        ("cze", "ces"),
        ("dut", "nld"),
        ("fre", "fra"),
        ("geo", "kat"),
        ("ger", "deu"),
        ("gre", "ell"),
        ("ice", "isl"),
        ("mac", "mkd"),
        ("mao", "mri"),
        ("may", "msa"),
        ("per", "fas"),
        ("rum", "ron"),
        ("slo", "slk"),
        ("tib", "bod"),
        ("wel", "cym"),
    ] {
        assert_eq!(to_iso639_t(b), t, "{b}");
        assert_eq!(to_iso639_t(&b.to_ascii_uppercase()), t, "{b}");
        assert!(same_language(b, t), "{b}");
    }
    assert_eq!(to_iso639_t("eng"), "eng");
}

#[test]
fn audio_channel_layouts_have_their_own_text() {
    for (ch, want) in [
        (AudioChannels::Mono, "1.0"),
        (AudioChannels::Stereo, "2.0"),
        (AudioChannels::Stereo21, "2.1"),
        (AudioChannels::Surround30, "3.0"),
        (AudioChannels::Surround31, "3.1"),
        (AudioChannels::Quad, "4.0"),
        (AudioChannels::Surround41, "4.1"),
        (AudioChannels::Surround50, "5.0"),
        (AudioChannels::Surround51, "5.1"),
        (AudioChannels::Surround60, "6.0"),
        (AudioChannels::Surround61, "6.1"),
        (AudioChannels::Surround70, "7.0"),
        (AudioChannels::Surround71, "7.1"),
    ] {
        assert_eq!(
            generate_audio_label(&Codec::Ac3, &ch, false),
            format!("Dolby Digital {want}")
        );
    }
}

#[test]
fn video_resolution_token_boundaries() {
    let tok = |w, h, i| generate_video_label(&Codec::Hevc, (w, h), i, &HdrFormat::Sdr, false);
    let name = Codec::Hevc.name();
    for (w, h, interlaced, want) in [
        (7680, 4320, false, "8K"),
        (7679, 4320, false, "4K"),
        (3840, 2160, false, "4K"),
        (1920, 1080, false, "1080p"),
        (1920, 1080, true, "1080i"),
        (1280, 720, false, "720p"),
        (720, 576, false, "576p"),
        (720, 576, true, "576i"),
        (720, 480, false, "480p"),
        (720, 480, true, "480i"),
        (640, 479, false, ""),
    ] {
        let want = if want.is_empty() {
            name.to_string()
        } else {
            format!("{name} {want}")
        };
        assert_eq!(tok(w, h, interlaced), want, "{w}x{h} i={interlaced}");
    }
}

#[test]
fn atmos_marker_only_rides_a_dolby_carrier() {
    let l = |c| generate_audio_label_atmos(&c, &AudioChannels::Surround71, false);
    assert_eq!(l(Codec::DtsHdMa), "DTS-HD Master Audio 7.1");
    assert_eq!(l(Codec::Dts), "DTS 7.1");
    assert_eq!(l(Codec::Ac3), "Dolby Digital 7.1");
}

#[test]
fn atmos_hint_is_consistent_only_with_a_dolby_carrier() {
    assert!(codec_hint_consistent("Atmos", &Codec::TrueHd));
    assert!(codec_hint_consistent("Atmos", &Codec::Ac3Plus));
    assert!(!codec_hint_consistent("Atmos", &Codec::DtsHdMa));
    assert!(!codec_hint_consistent("Atmos", &Codec::Dts));
    assert!(codec_hint_consistent("High Resolution", &Codec::DtsHdHr));
    assert!(codec_hint_consistent("DTS-HD HR", &Codec::DtsHdHr));
    assert!(!codec_hint_consistent("Master Audio", &Codec::DtsHdHr));
    assert!(!codec_hint_consistent("High Resolution", &Codec::DtsHdMa));
}

#[test]
fn merged_labels_order_by_slot_then_stream() {
    let mut labels = vec![
        vendor(StreamLabelType::Audio, 3),
        vendor(StreamLabelType::Audio, 1),
        derived("00002", 0x1100, 1),
        derived("00001", 0x1101, 1),
        derived("00001", 0x1100, 1),
        derived("00001", 0x1102, 0),
    ];
    sort_labels(&mut labels);
    let order: Vec<(Option<(&str, u16)>, u16)> = labels
        .iter()
        .map(|l| {
            (
                l.stream_id.as_ref().map(|i| (i.clip_id.as_str(), i.pid)),
                l.stream_number,
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            (None, 1),
            (None, 3),
            (Some(("00001", 0x1102)), 0),
            (Some(("00001", 0x1100)), 1),
            (Some(("00001", 0x1101)), 1),
            (Some(("00002", 0x1100)), 1),
        ]
    );
}

// Off the authoritative path, a label binds by bare ordinal only when the stream
// STATES the same language; an unknown stream language is not agreement.
#[test]
fn ordinal_binding_needs_a_stated_subtitle_language() {
    let mut title = DiscTitle {
        playlist: "00800.mpls".into(),
        playlist_id: 800,
        duration_secs: 7200.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![Stream::Subtitle(SubtitleStream {
            pid: 0x12A0,
            codec: Codec::Pgs,
            language: String::new(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: crate::disc::ContentFormat::BdTs,
        codec_privates: Vec::new(),
    };
    let labels = vec![StreamLabel {
        qualifier: LabelQualifier::Forced,
        ..vendor(StreamLabelType::Subtitle, 1)
    }];
    apply_labels(&labels, std::slice::from_mut(&mut title));
    let Stream::Subtitle(s) = &title.streams[0] else {
        unreachable!()
    };
    assert!(!s.forced);
    assert_eq!(s.qualifier, LabelQualifier::None);
}

// Minimal MPLS: one play item on `clip` whose STN lists one primary audio stream `pid`.
fn mpls_bytes(clip: &[u8; 5], pid: u16) -> Vec<u8> {
    let mut item = Vec::new();
    item.extend_from_slice(clip);
    item.extend_from_slice(b"M2TS");
    item.extend_from_slice(&[0u8; 3]);
    item.extend_from_slice(&0u32.to_be_bytes());
    item.extend_from_slice(&(7000u32 * 45000).to_be_bytes());
    item.extend_from_slice(&[0u8; 12]); // UO mask, misc, still
    item.extend_from_slice(&[0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    item.extend_from_slice(&[3, 0x01]);
    item.extend_from_slice(&pid.to_be_bytes());
    item.extend_from_slice(&[5, 0x81, 0x61]);
    item.extend_from_slice(b"eng");
    let mut pl = vec![0u8; 6];
    pl.extend_from_slice(&1u16.to_be_bytes());
    pl.extend_from_slice(&[0u8; 2]);
    pl.extend_from_slice(&(item.len() as u16).to_be_bytes());
    pl.extend_from_slice(&item);
    let pl_len = (pl.len() - 4) as u32;
    pl[0..4].copy_from_slice(&pl_len.to_be_bytes());
    let mut buf = b"MPLS0200".to_vec();
    buf.extend_from_slice(&40u32.to_be_bytes());
    buf.extend_from_slice(&[0u8; 28]);
    buf.extend_from_slice(&pl);
    buf
}

fn dir(name: &str, icb: u32, files: Vec<FileSpec>, subdirs: Vec<DirSpec>) -> DirSpec {
    DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: icb + 1,
        files,
        subdirs,
    }
}

fn open(root: DirSpec) -> (MemDisc, UdfFs) {
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
    (disc, udf)
}

// A framework winner keeps its own labels and hint, and the MPLS floor adds the
// streams it named no label for.
#[test]
fn extract_merges_the_mpls_floor_into_a_framework_winner() {
    let dcx = br#"<dcx><disc>
            <playlist id="00800" name="feature" durs="7000"><audio id="01" lang="eng"/></playlist>
        </disc></dcx>"#;
    let (mut disc, udf) = open(dir(
        "",
        10,
        vec![],
        vec![dir(
            "BDMV",
            20,
            vec![],
            vec![
                dir(
                    "JAR",
                    30,
                    vec![],
                    vec![dir(
                        "00000",
                        40,
                        vec![file_with("dcx.xml", 100, 2000, dcx.to_vec(), true)],
                        vec![],
                    )],
                ),
                dir(
                    "PLAYLIST",
                    50,
                    vec![file_with(
                        "00800.mpls",
                        101,
                        3000,
                        mpls_bytes(b"00001", 0x1100),
                        true,
                    )],
                    vec![],
                ),
            ],
        )],
    ));
    let (labels, hint) = extract(&mut disc, &udf);
    assert_eq!(labels.iter().filter(|l| l.stream_id.is_none()).count(), 1);
    assert_eq!(labels.iter().filter(|l| l.stream_id.is_some()).count(), 1);
    assert_eq!(hint.and_then(|h| h.playlist_id), Some(800));
}

// The first copy of a manifest that reads non-empty wins; names match without case.
#[test]
fn read_jar_file_skips_empty_copies_and_matches_names_without_case() {
    let (mut disc, udf) = open(dir(
        "",
        10,
        vec![],
        vec![dir(
            "BDMV",
            20,
            vec![],
            vec![dir(
                "JAR",
                30,
                vec![],
                vec![
                    dir(
                        "00000",
                        40,
                        vec![file_with("playlists.xml", 100, 2000, Vec::new(), true)],
                        vec![],
                    ),
                    dir(
                        "00001",
                        50,
                        vec![file_with(
                            "PLAYLISTS.XML",
                            101,
                            3000,
                            b"<p/>".to_vec(),
                            true,
                        )],
                        vec![],
                    ),
                ],
            )],
        )],
    ));
    let got = read_jar_file(&mut disc, &udf, "playlists.xml");
    assert_eq!(got.as_deref(), Some(&b"<p/>"[..]));
}

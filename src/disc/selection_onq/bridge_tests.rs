use super::*;
use crate::disc::{Clip, ContentFormat, EpisodeEvidence};
use crate::udf::fixture::*;

fn dir(name: &str, icb: u32, files: Vec<FileSpec>, subdirs: Vec<DirSpec>) -> DirSpec {
    DirSpec {
        name: name.into(),
        icb_lba: icb,
        dir_data_lba: icb + 1,
        files,
        subdirs,
    }
}

fn empty_fs() -> (MemDisc, UdfFs) {
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &dir("", 10, vec![], vec![]));
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    (disc, fs)
}

#[test]
fn udf_adapter_propagates_cancellation_and_refuses_missing_assets() {
    let (mut disc, fs) = empty_fs();
    let mut titles = vec![DiscTitle {
        selection_evidence: Default::default(),
        playlist: "00001.mpls".into(),
        playlist_id: 1,
        duration_secs: 100.,
        size_bytes: 0,
        clips: vec![],
        streams: vec![],
        chapters: vec![],
        extents: vec![],
        content_format: ContentFormat::BdTs,
        codec_privates: vec![],
    }];
    annotate(&mut disc, &fs, None, &mut titles).unwrap();
    assert_eq!(
        titles[0].selection_evidence.episodes,
        EpisodeEvidence::Unknown
    );
    let halt = Halt::new();
    halt.cancel();
    assert!(matches!(
        annotate(&mut disc, &fs, Some(&halt), &mut []),
        Err(Error::Halted)
    ));
}

fn actual_titles(directory: &std::path::Path) -> Vec<DiscTitle> {
    [168, 166, 167]
        .into_iter()
        .map(|id| {
            let bytes = std::fs::read(directory.join(format!("{id:05}.mpls"))).unwrap();
            let parsed = crate::mpls::parse(&bytes).unwrap();
            let clips: Vec<_> = parsed
                .play_items
                .into_iter()
                .map(|item| Clip {
                    clip_id: item.clip_id,
                    in_time: item.in_time,
                    out_time: item.out_time,
                    duration_secs: f64::from(item.out_time - item.in_time) / 45000.,
                    source_packets: 0,
                    feed_span: None,
                })
                .collect();
            DiscTitle {
                selection_evidence: Default::default(),
                playlist: format!("{id:05}.mpls"),
                playlist_id: id,
                duration_secs: clips.iter().map(|clip| clip.duration_secs).sum(),
                size_bytes: 0,
                clips,
                streams: vec![],
                chapters: vec![],
                extents: vec![],
                content_format: ContentFormat::BdTs,
                codec_privates: vec![],
            }
        })
        .collect()
}

fn actual_udf(directory: &std::path::Path, mutation: u8) -> (MemDisc, UdfFs) {
    let mut metadata = 100;
    let mut data = 1000;
    let mut file = |name: &str| {
        let mut bytes = std::fs::read(directory.join(name)).unwrap();
        if mutation == 2 && name == "00001.bdjo" {
            bytes[0] ^= 1;
        }
        if matches!(mutation, 3 | 4) && name == "00002.jar" {
            bytes = tamper_authored_jar(&bytes, mutation);
        }
        let name = if mutation == 1 && name == "00002.jar" {
            "absent.jar"
        } else {
            name
        };
        let spec = file_with(name, metadata, data, bytes, true);
        metadata += 1;
        data += u32::try_from(spec.size.div_ceil(2048)).unwrap() + 1;
        spec
    };
    let bdmv_files = vec![file("index.bdmv"), file("MovieObject.bdmv")];
    let bdjo = dir(
        "BDJO",
        14,
        vec![file("00000.bdjo"), file("00001.bdjo"), file("00002.bdjo")],
        vec![],
    );
    let jars = dir(
        "JAR",
        16,
        vec![
            file("onQClient.cfg"),
            file("00000.jar"),
            file("00002.jar"),
            file("44444.jar"),
        ],
        vec![dir("00002", 18, vec![file("playlists.xml")], vec![])],
    );
    let playlists = dir(
        "PLAYLIST",
        20,
        vec![file("00168.mpls"), file("00166.mpls"), file("00167.mpls")],
        vec![],
    );
    let mut clip_info = Vec::new();
    let mut streams = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    // Only CLPI/allocation scaffolding is synthetic. The authored assets and
    // MPLS intervals are real; no stream sectors are read or executed.
    for title in actual_titles(directory) {
        for clip in title.clips {
            if !seen.insert(clip.clip_id.clone()) {
                continue;
            }
            let mut clpi = vec![0; 60];
            clpi[..8].copy_from_slice(b"HDMV0200");
            clpi[56..60].copy_from_slice(&10_u32.to_be_bytes());
            clip_info.push(file_with(
                &format!("{}.clpi", clip.clip_id),
                metadata,
                data,
                clpi,
                true,
            ));
            streams.push(crate::udf::fixture::file(
                &format!("{}.m2ts", clip.clip_id),
                metadata + 1,
                data + 1,
                2048,
                true,
            ));
            metadata += 2;
            data += 3;
        }
    }
    let root = dir(
        "",
        10,
        vec![],
        vec![dir(
            "BDMV",
            12,
            bdmv_files,
            vec![
                bdjo,
                jars,
                playlists,
                dir("CLIPINF", 22, clip_info, vec![]),
                dir("STREAM", 24, streams, vec![]),
            ],
        )],
    );
    let mut disc = MemDisc::new();
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let fs = crate::udf::read_filesystem(&mut disc).unwrap();
    (disc, fs)
}

#[test]
#[ignore = "cached real authored metadata/MPLS through UDF adapter; no media execution"]
fn actual_metadata_udf_produces_ordered_whole_presentations() {
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    let directory = jar.parent().unwrap();
    let (mut disc, fs) = actual_udf(directory, 0);
    let mut titles = actual_titles(directory);
    let intervals: Vec<_> = titles
        .iter()
        .map(|title| {
            title
                .clips
                .iter()
                .map(|clip| (clip.clip_id.clone(), clip.in_time, clip.out_time))
                .collect::<Vec<_>>()
        })
        .collect();
    annotate(&mut disc, &fs, None, &mut titles).unwrap();
    for (title, ordinal) in titles.iter().zip([2, 0, 1]) {
        assert!(matches!(title.selection_evidence.episodes,
            EpisodeEvidence::Authored { member: true, ordinal: Some(actual), title_count: 3, .. } if actual == ordinal));
    }
    let after: Vec<_> = titles
        .iter()
        .map(|title| {
            title
                .clips
                .iter()
                .map(|clip| (clip.clip_id.clone(), clip.in_time, clip.out_time))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(after, intervals);
    let scanned = crate::disc::Disc::scan_bluray_titles(&mut disc, &fs, None).unwrap();
    assert_eq!(scanned.len(), 3);
    for title in scanned {
        let ordinal = [166, 167, 168]
            .iter()
            .position(|id| *id == title.playlist_id)
            .unwrap();
        assert!(matches!(title.selection_evidence.episodes,
            EpisodeEvidence::Authored { member: true, ordinal: Some(actual), .. } if actual == ordinal));
    }
}

#[test]
#[ignore = "cached missing/tampered metadata through production UDF scan; no media execution"]
fn actual_metadata_udf_missing_or_tampered_assets_stay_unknown() {
    let jar = std::path::PathBuf::from(std::env::var("ONQ_TEST_JAR").unwrap());
    let directory = jar.parent().unwrap();
    for mutation in [1, 2, 3, 4] {
        let (mut disc, fs) = actual_udf(directory, mutation);
        let scanned = crate::disc::Disc::scan_bluray_titles(&mut disc, &fs, None).unwrap();
        assert_eq!(scanned.len(), 3);
        assert!(
            scanned
                .iter()
                .all(|title| title.selection_evidence.episodes == EpisodeEvidence::Unknown),
            "mutation {mutation}"
        );
    }
}

fn tamper_authored_jar(bytes: &[u8], mutation: u8) -> Vec<u8> {
    use std::io::{Cursor, Read, Write};
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).unwrap();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        if entry.name() == "FS.QCO" {
            let program = super::super::qco::parse(&data).unwrap();
            if mutation == 3 {
                let span = program.global.variable_spans[22].clone();
                drop(program);
                data[span].copy_from_slice(&36_i32.to_be_bytes());
            } else {
                let start = program.screen.function_spans[308].start;
                drop(program);
                // Replace literal array preparation with an opaque scalar write.
                data[start + 3..start + 8].copy_from_slice(&[1, 0, 0x20, 1, 0x2e]);
            }
        }
        output
            .start_file(entry.name(), zip::write::SimpleFileOptions::default())
            .unwrap();
        output.write_all(&data).unwrap();
    }
    output.finish().unwrap().into_inner()
}

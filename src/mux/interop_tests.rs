//! External interoperability gate. Pure Rust tests remain in normal dev CI;
//! QA explicitly runs ignored `ffmpeg_` tests with software codecs installed.
//! Fixtures are generated locally and never downloaded from a media corpus.

use super::driver::{MuxInput, MuxOptions, NoopEvents, mux_stream};
use crate::halt::Halt;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

pub(crate) struct FixtureDir {
    path: PathBuf,
    _temporary: Option<tempfile::TempDir>,
}

impl FixtureDir {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

// QA retains generated input/output media and probes as downloadable artifacts,
// including on failure. Local runs clean up unless the caller opts in.
pub(crate) fn fixture_dir(name: &str) -> FixtureDir {
    if let Some(root) = std::env::var_os("FREEMKV_FFMPEG_ARTIFACT_DIR") {
        let path = PathBuf::from(root).join(name);
        std::fs::create_dir_all(&path).unwrap();
        FixtureDir {
            path,
            _temporary: None,
        }
    } else {
        let temporary = tempfile::tempdir().unwrap();
        FixtureDir {
            path: temporary.path().to_owned(),
            _temporary: Some(temporary),
        }
    }
}

fn run(dir: &Path, label: &str, command: &mut Command) -> Output {
    let output = command
        .output()
        .expect("install ffmpeg and ffprobe to run the interoperability tests");
    std::fs::write(
        dir.join(format!("{label}.command.txt")),
        format!("{command:?}\n"),
    )
    .unwrap();
    std::fs::write(dir.join(format!("{label}.stdout")), &output.stdout).unwrap();
    std::fs::write(dir.join(format!("{label}.stderr")), &output.stderr).unwrap();
    assert!(
        output.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn generate(
    dir: &Path,
    video: &str,
    audio: Option<&str>,
    transport: bool,
    vfr: bool,
    bframes: bool,
    multi_audio: bool,
) -> PathBuf {
    let path = dir.join(if transport { "input.m2ts" } else { "input.mkv" });
    let mut command = Command::new("ffmpeg");
    command.args([
        "-nostdin",
        "-y",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=128x96:rate=25:duration=2",
    ]);
    if audio.is_some() {
        command.args([
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000:duration=2",
        ]);
    }
    if multi_audio {
        command.args([
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=48000:duration=2",
        ]);
    }
    command.args(["-map", "0:v:0"]);
    if audio.is_some() {
        command.args(["-map", "1:a:0"]);
    }
    if multi_audio {
        command.args(["-map", "2:a:0"]);
    }
    // A deliberate positive audio offset tests A/V alignment without conflating
    // encoder preroll clamped at zero with drift introduced by the remuxer.
    command.args([
        "-c:v",
        video,
        "-threads:v",
        "1",
        "-g",
        "12",
        "-bf",
        if bframes { "2" } else { "0" },
    ]);
    if let Some(audio) = audio {
        command.args([
            "-c:a",
            audio,
            "-b:a",
            "192k",
            "-threads:a",
            "1",
            "-af",
            "asetpts=PTS+0.1/TB",
        ]);
    }
    if video == "libx264" {
        command.args(["-preset", "ultrafast"]);
    }
    if video == "libx265" {
        command.args([
            "-preset",
            "ultrafast",
            "-x265-params",
            "pools=none:frame-threads=1:log-level=error",
        ]);
    }
    if vfr {
        command.args([
            "-vf",
            "select=lt(n\\,12)+not(mod(n\\,3))",
            "-fps_mode",
            "vfr",
        ]);
    }
    if transport {
        command.args(["-f", "mpegts", "-mpegts_m2ts_mode", "0"]);
    }
    run(dir, "generate", command.arg(&path));
    if transport {
        let ts = std::fs::read(&path).unwrap();
        assert_eq!(ts.len() % 188, 0);
        let mut m2ts = Vec::with_capacity(ts.len() / 188 * 192);
        for packet in ts.as_chunks::<188>().0 {
            m2ts.extend_from_slice(&[0; 4]);
            m2ts.extend_from_slice(packet);
        }
        std::fs::write(&path, m2ts).unwrap();
    }
    path
}

fn remux(source: &Path, output: &Path, transport: bool) {
    let scheme = if transport { "m2ts" } else { "mkv" };
    let url = format!("{scheme}://{}", source.display());
    let result = mux_stream(
        MuxInput::Url {
            url: &url,
            opts: Default::default(),
        },
        &format!("mkv://{}", output.display()),
        &MuxOptions::default(),
        &Halt::new(),
        Arc::new(NoopEvents),
    )
    .unwrap();
    assert!(result.completed && result.output_opened && result.bytes_written > 0);
    assert_eq!(result.errors, 0);
    assert_eq!(result.lost_bytes, 0);
    assert!(result.undelivered_streams.is_empty());
}

fn probe(dir: &Path, label: &str, path: &Path) -> Value {
    // Raw AAC/MP2 has no container duration; FFmpeg legitimately warns that it
    // estimates it from bitrate. Inputs must be error-free; output MKVs must
    // be warning-free. Compare decoded frames, never the estimated duration.
    let verbosity = if label.starts_with("input") {
        "error"
    } else {
        "warning"
    };
    let output = run(dir, label, Command::new("ffprobe").args([
        "-v", verbosity, "-threads", "1", "-of", "json", "-show_streams", "-show_frames",
        "-show_entries", "stream=index,codec_type,codec_name,width,height,sample_rate,channels:frame=stream_index,best_effort_timestamp_time,pts_time",
    ]).arg(path));
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn decoded_hash(dir: &Path, label: &str, path: &Path, stream: usize, video: bool) -> Vec<u8> {
    let mut command = Command::new("ffmpeg");
    let verbosity = if label.starts_with("input") {
        "error"
    } else {
        "warning"
    };
    command
        .args([
            "-nostdin", "-v", verbosity, "-xerror", "-threads", "1", "-i",
        ])
        .arg(path)
        .args(["-map", &format!("0:{stream}"), "-threads", "1"]);
    if video {
        command.args(["-c:v", "rawvideo", "-fps_mode", "passthrough"]);
    } else {
        command.args(["-c:a", "pcm_s16le"]);
    }
    command.args(["-f", "hash", "-hash", "sha256", "-"]);
    let output = run(dir, label, &mut command);
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.starts_with(b"SHA256="));
    output.stdout
}

fn timestamps(probe: &Value, stream: usize) -> Vec<f64> {
    probe["frames"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|frame| frame["stream_index"].as_u64() == Some(stream as u64))
        .map(|frame| {
            frame["best_effort_timestamp_time"]
                .as_str()
                .or_else(|| frame["pts_time"].as_str())
                .expect("every decoded frame must have a timestamp")
                .parse()
                .unwrap()
        })
        .collect()
}

fn validate(dir: &Path, source: &Path, output: &Path, expected_streams: usize, vfr: bool) {
    let before = probe(dir, "input-probe", source);
    let after = probe(dir, "output-probe", output);
    let streams = before["streams"].as_array().unwrap();
    assert_eq!(streams.len(), expected_streams);
    assert_eq!(after["streams"].as_array().unwrap().len(), expected_streams);
    // Remuxing may rebase all tracks by one constant. It may not drift tracks
    // relative to each other or replace a VFR timeline with a guessed cadence.
    let shift = timestamps(&after, 0)[0] - timestamps(&before, 0)[0];
    for (index, stream) in streams.iter().enumerate() {
        for field in [
            "codec_type",
            "codec_name",
            "width",
            "height",
            "sample_rate",
            "channels",
        ] {
            assert_eq!(
                stream[field], after["streams"][index][field],
                "stream {index} {field}"
            );
        }
        let input_pts = timestamps(&before, index);
        let output_pts = timestamps(&after, index);
        assert!(
            input_pts.len() >= 10,
            "fixture must exercise multiple frames"
        );
        assert_eq!(
            input_pts.len(),
            output_pts.len(),
            "frame loss on stream {index}"
        );
        for (input, output) in input_pts.iter().zip(&output_pts) {
            assert!(
                (output - input - shift).abs() < 0.002,
                "stream {index}: input {input}, output {output}, common shift {shift}"
            );
        }
        assert!(
            output_pts.windows(2).all(|p| p[1] > p[0]),
            "decoded stream {index} must advance"
        );
        let video = stream["codec_type"] == "video";
        if vfr && video {
            let gaps: Vec<_> = input_pts
                .windows(2)
                .map(|p| ((p[1] - p[0]) * 1000.0).round() as i64)
                .collect();
            assert!(
                gaps.iter().any(|g| *g != gaps[0]),
                "fixture must actually be VFR"
            );
        }
        assert_eq!(
            decoded_hash(dir, &format!("input-hash-{index}"), source, index, video),
            decoded_hash(dir, &format!("output-hash-{index}"), output, index, video),
            "decoded content changed on stream {index}"
        );
    }
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_h264_ac3_transport_to_mkv_preserves_decoded_content_and_timing() {
    let dir = fixture_dir("h264-ac3-transport");
    let input = generate(dir.path(), "libx264", Some("ac3"), true, false, true, false);
    let output = dir.path().join("output.mkv");
    remux(&input, &output, true);
    validate(dir.path(), &input, &output, 2, false);
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_hevc_eac3_transport_to_mkv_preserves_decoded_content_and_timing() {
    let dir = fixture_dir("hevc-eac3-transport");
    let input = generate(
        dir.path(),
        "libx265",
        Some("eac3"),
        true,
        false,
        true,
        false,
    );
    let output = dir.path().join("output.mkv");
    remux(&input, &output, true);
    validate(dir.path(), &input, &output, 2, false);
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_mpeg2_transport_to_mkv_preserves_decoded_video_and_timing() {
    let dir = fixture_dir("mpeg2-video-transport");
    let input = generate(dir.path(), "mpeg2video", None, true, false, true, false);
    let output = dir.path().join("output.mkv");
    remux(&input, &output, true);
    validate(dir.path(), &input, &output, 1, false);
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_ac3_parser_preserves_audio_packet_boundaries() {
    validate_audio_parser("ac3", "ac3", crate::disc::Codec::Ac3);
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_eac3_parser_preserves_audio_packet_boundaries() {
    validate_audio_parser("eac3", "eac3", crate::disc::Codec::Ac3Plus);
}

fn validate_audio_parser(encoder: &str, format: &str, codec: crate::disc::Codec) {
    use super::codec::{CodecParser, ac3::Ac3Parser, adts::AdtsParser, mpegaudio::MpegAudioParser};
    use super::mkv::{MkvMuxer, MkvTrack};
    use super::ts::PesPacket;
    use crate::disc::{AudioChannels, AudioStream, LabelPurpose, SampleRate};
    let dir = fixture_dir(&format!("{encoder}-parser"));
    let input = dir.path().join(format!("input.{format}"));
    run(
        dir.path(),
        "generate",
        Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-y",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=48000:duration=2",
                "-c:a",
                encoder,
                "-b:a",
                "192k",
                "-threads",
                "1",
                "-f",
                format,
            ])
            .arg(&input),
    );
    let mut parser: Box<dyn CodecParser> = match codec {
        crate::disc::Codec::Aac => Box::new(AdtsParser::new()),
        crate::disc::Codec::Mp2 => Box::new(MpegAudioParser::new()),
        _ => Box::new(Ac3Parser::new()),
    };
    let mut frames = Vec::new();
    // Deliberately split headers and bodies across PES boundaries. Only the
    // first chunk has PTS; subsequent frame times must come from codec cadence.
    for (index, data) in std::fs::read(&input).unwrap().chunks(137).enumerate() {
        frames.extend(parser.parse(&PesPacket {
            pid: 0x1100,
            pts: (index == 0).then_some(0),
            dts: None,
            data: data.to_vec(),
            source: None,
            discontinuity: false,
        }));
    }
    frames.extend(parser.flush());
    let mut track = MkvTrack::audio(&AudioStream {
        pid: 0x1100,
        codec,
        channels: AudioChannels::Mono,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    });
    track.codec_private = parser.codec_private();
    let output = dir.path().join("output.mkv");
    let mut muxer = MkvMuxer::new(
        std::fs::File::create(&output).unwrap(),
        &[track],
        None,
        0.0,
        &[],
    )
    .unwrap();
    for frame in frames {
        muxer
            .write_frame(
                0,
                frame.pts_ns,
                frame.keyframe,
                &frame.data,
                frame.duration_ns,
                None,
            )
            .unwrap();
    }
    muxer.finish().unwrap();
    validate(dir.path(), &input, &output, 1, false);
}

#[test]
#[ignore = "requires FFmpeg software codecs; QA interoperability workflow"]
fn ffmpeg_mkv_roundtrip_preserves_vfr_and_multiple_audio_tracks() {
    let dir = fixture_dir("vfr-multiple-audio");
    // Stream-copy transport packets to create a disc-shaped MKV reference.
    // Direct encoding into MKV adds CodecDelay/SkipSamples, a separate
    // unsupported preservation path documented in the QA guide.
    let transport = generate(dir.path(), "libx264", Some("ac3"), true, true, true, true);
    let input = dir.path().join("input.mkv");
    run(
        dir.path(),
        "reference-remux",
        Command::new("ffmpeg")
            .args(["-nostdin", "-y", "-v", "error", "-i"])
            .arg(&transport)
            .args(["-map", "0", "-c", "copy"])
            .arg(&input),
    );
    let output = dir.path().join("output.mkv");
    remux(&input, &output, false);
    validate(dir.path(), &input, &output, 3, true);
}

// Discovered while building the gate; retain executable, strict reproducers.
// These deliberately do NOT have the ffmpeg_ prefix: run ffmpeg_ explicitly
// to investigate them. Promote each into the QA gate when its fix lands.

#[test]
#[ignore = "requires ffmpeg and ffprobe"]
fn ffmpeg_aac_frame_assembly() {
    validate_audio_parser("aac", "adts", crate::disc::Codec::Aac);
}

#[test]
#[ignore = "requires ffmpeg and ffprobe"]
fn ffmpeg_mp2_frame_assembly() {
    validate_audio_parser("mp2", "mp2", crate::disc::Codec::Mp2);
}

#[test]
#[ignore = "requires ffmpeg and ffprobe"]
fn ffmpeg_mpeg2_transport_opening_audio() {
    let dir = fixture_dir("mpeg2-audio");
    let input = generate(
        dir.path(),
        "mpeg2video",
        Some("ac3"),
        true,
        false,
        true,
        false,
    );
    let output = dir.path().join("output.mkv");
    remux(&input, &output, true);
    validate(dir.path(), &input, &output, 2, false);
}

#[test]
#[ignore = "requires ffmpeg and ffprobe"]
fn ffmpeg_mkv_encoder_delay() {
    let dir = fixture_dir("codec-delay");
    let input = generate(dir.path(), "libx264", Some("ac3"), false, true, true, true);
    let output = dir.path().join("output.mkv");
    remux(&input, &output, false);
    validate(dir.path(), &input, &output, 3, true);
}

#[test]
#[ignore = "requires ffmpeg and ffprobe"]
fn ffmpeg_generic_transport_audio_discovery() {
    for audio in ["aac", "mp2"] {
        let dir = fixture_dir(&format!("transport-{audio}"));
        let input = generate(
            dir.path(),
            "libx264",
            Some(audio),
            true,
            false,
            false,
            false,
        );
        let output = dir.path().join("output.mkv");
        remux(&input, &output, true);
        validate(dir.path(), &input, &output, 2, false);
    }
}

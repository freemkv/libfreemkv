use super::super::videomap::{Medium, SourceInfo};
use super::StreamUrl;
use super::parse_url;
use super::validate_network_addr;
use super::{build_demux_state, build_iso_pipeline, input, output};
use crate::decrypt::DecryptKeys;
use crate::disc::{ContentFormat, DiscTitle, Extent};
use crate::pes::PesSource as _;
use crate::sector::SectorSource;
use std::path::PathBuf;

// parse_url must never panic on ANY input (the front door for caller URLs). Feeds
// adversarial strings + every byte 0x00..=0xFF; any StreamUrl is OK.
#[test]
fn parse_url_never_panics_on_adversarial_input() {
    let mut cases: Vec<String> = vec![
        String::new(),
        "://".into(),
        "//".into(),
        ":".into(),
        "disc".into(),
        "disc:/".into(),
        "disc:://".into(),
        "disc://disc://".into(),
        "iso://iso://x".into(),
        "mkv://mkv://mkv://".into(),
        "iso://\0/etc".into(),              // embedded NUL
        "iso://日本語/フィルム.iso".into(), // unicode path
        "network://[::1]:9000".into(),
        "ftp://host/x".into(),
        format!("iso://{}", "a".repeat(100_000)), // very long path
        "\u{feff}disc://".into(),                 // BOM prefix
    ];
    // Every byte as the entire input, and as an iso:// path suffix.
    for b in 0u8..=255 {
        cases.push(String::from_utf8_lossy(&[b]).into_owned());
        cases.push(format!("iso://{}", String::from_utf8_lossy(&[b])));
    }
    for c in &cases {
        // The contract: returns SOME variant, never panics. We also exercise
        // scheme()/path_str()/is_disc_source() so their match arms can't
        // panic on the parsed result either.
        let u = parse_url(c);
        let _ = u.scheme();
        let _ = u.path_str();
        let _ = u.is_disc_source();
    }
}

#[test]
fn disk_scheme_is_alias_for_disc() {
    // `disk://` must parse identically to `disc://`: empty = auto-detect
    // (device None), a trailing path = explicit device. A Windows user
    // typing `disk://i:` must reach the same live-disc path as `disc://`.
    match (parse_url("disk://"), parse_url("disc://")) {
        (StreamUrl::Disc { device: a }, StreamUrl::Disc { device: b }) => {
            assert_eq!(a, None);
            assert_eq!(b, None);
        }
        other => panic!("disk:// / disc:// must both be Disc, got {other:?}"),
    }
    match (parse_url("disk://i:"), parse_url("disc://i:")) {
        (StreamUrl::Disc { device: a }, StreamUrl::Disc { device: b }) => {
            assert_eq!(a, Some(PathBuf::from("i:")));
            assert_eq!(b, Some(PathBuf::from("i:")));
            assert_eq!(a, b, "disk:// device must match disc:// device");
        }
        other => panic!("disk://i: / disc://i: must both be Disc, got {other:?}"),
    }
}

#[test]
fn validate_network_addr_rejects_portless() {
    // Empty, bare IPv4, and bare IPv6 (which contains ':') must all fail.
    assert!(validate_network_addr("").is_err());
    assert!(validate_network_addr("127.0.0.1").is_err());
    assert!(validate_network_addr("::1").is_err());
    assert!(validate_network_addr("2001:db8::1").is_err());
    // host:port and ip:port forms pass.
    assert!(validate_network_addr("127.0.0.1:9000").is_ok());
    assert!(validate_network_addr("host:9000").is_ok());
}

#[test]
fn validate_network_addr_requires_numeric_port() {
    // An empty port (`host:`) and a non-numeric port (`host:abc`) both
    // contain ':' but are NOT valid host:port — must be rejected.
    assert!(validate_network_addr("host:").is_err());
    assert!(validate_network_addr("127.0.0.1:").is_err());
    assert!(validate_network_addr("host:abc").is_err());
    assert!(validate_network_addr("host:99x").is_err());
    // Out-of-u16-range port is rejected (parse::<u16> fails).
    assert!(validate_network_addr("host:70000").is_err());
    // Bracketed IPv6 with a valid port passes; split on the LAST ':' so the
    // address colons are not mistaken for the port separator.
    assert!(validate_network_addr("[2001:db8::1]:9000").is_ok());
    // Bracketed IPv6 WITHOUT a port is rejected (port substring not a u16).
    assert!(validate_network_addr("[2001:db8::1]").is_err());
    // Valid numeric port (incl. 0 and max u16) passes.
    assert!(validate_network_addr("host:0").is_ok());
    assert!(validate_network_addr("host:65535").is_ok());
}

// Decrypt-verdict matrix is owned/tested by `keys::check_decryptable`.
// Box<dyn Stream> isn't Debug, so unwrap_err() won't compile — these
// helpers extract io::ErrorKind from the Err arm (panic on Ok) instead.
fn input_err_kind(url: &str) -> std::io::ErrorKind {
    match input(url, &Default::default(), &crate::ctx::Ctx::default()) {
        Ok(_) => panic!("expected input({url}) to error"),
        Err(e) => e.kind(),
    }
}
fn output_err_kind(url: &str, t: &DiscTitle) -> std::io::ErrorKind {
    match output(url, t, None) {
        Ok(_) => panic!("expected output({url}) to error"),
        Err(e) => e.kind(),
    }
}

// ── fvi:// provenance ─────────────────────────────────────────────────

/// Tiny unique temp dir helper (avoids a dev-dependency on `tempfile`).
fn fvi_tempdir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("fmkv_resolve_fvi_{}_{}", std::process::id(), n));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A minimal single-video-stream title, enough for `FviSink` to build a
/// header row.
fn fvi_title() -> DiscTitle {
    use crate::disc::{
        Codec, ColorSpace, FrameRate, HdrFormat, Resolution, Stream as DiscStream, VideoStream,
    };
    let mut t = DiscTitle::empty();
    t.streams = vec![DiscStream::Video(VideoStream {
        pid: 0x1011,
        codec: Codec::Mpeg2,
        resolution: Resolution::R480i,
        frame_rate: FrameRate::F29_97,
        hdr: HdrFormat::Sdr,
        color_space: ColorSpace::Smpte170m,
        display_aspect: Some((16, 9)),
        secondary: false,
        label: String::new(),
        measured_cicp: None,
    })];
    t.content_format = ContentFormat::MpegPs;
    t
}

/// Read the header row (line 1) of an FVI file as JSON.
fn fvi_header(path: &std::path::Path) -> serde_json::Value {
    let text = std::fs::read_to_string(path).unwrap();
    serde_json::from_str(text.lines().next().unwrap()).unwrap()
}

// fvi:// must record the SOURCE, not the destination path.
#[test]
fn fvi_output_records_the_source_not_the_destination() {
    let dir = fvi_tempdir();
    let dst = dir.join("out.fvi");
    let src = SourceInfo {
        medium: Medium::Iso,
        path: "iso://m.iso".into(),
        title: 1,
        ..SourceInfo::default()
    };
    let mut sink = output(
        &format!("fvi://{}", dst.display()),
        &fvi_title(),
        Some(&src),
    )
    .expect("fvi sink");
    sink.finish().unwrap();

    let hdr = fvi_header(&dst);
    assert_eq!(hdr["source"]["path"], "iso://m.iso");
    assert_eq!(hdr["source"]["medium"], "iso");
    assert_eq!(hdr["source"]["title"], 1);
    let _ = std::fs::remove_dir_all(&dir);
}

// Two runs indexing the SAME source must be byte-identical regardless of destination.
#[test]
fn fvi_output_is_reproducible_across_destination_paths() {
    let dir = fvi_tempdir();
    let src = SourceInfo {
        medium: Medium::Iso,
        path: "iso://m.iso".into(),
        title: 1,
        ..SourceInfo::default()
    };
    // Deliberately different lengths — the parity run's byte-count delta
    // tracked exactly the destination path-length difference.
    let a = dir.join("a.fvi");
    let b = dir.join("a-much-longer-destination-name.fvi");
    for dst in [&a, &b] {
        let mut sink = output(
            &format!("fvi://{}", dst.display()),
            &fvi_title(),
            Some(&src),
        )
        .unwrap();
        sink.finish().unwrap();
    }
    assert_eq!(
        std::fs::read(&a).unwrap(),
        std::fs::read(&b).unwrap(),
        "same source, different destinations must produce identical bytes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// No provenance to declare (`None`) must not fabricate one: the header
/// carries the neutral `SourceInfo` defaults — an empty path, `file`,
/// title 0 — never the destination it happens to be writing to.
#[test]
fn fvi_output_without_provenance_emits_no_path() {
    let dir = fvi_tempdir();
    let dst = dir.join("bare.fvi");
    let mut sink = output(&format!("fvi://{}", dst.display()), &fvi_title(), None).unwrap();
    sink.finish().unwrap();

    let hdr = fvi_header(&dst);
    assert_eq!(hdr["source"]["path"], "");
    assert_eq!(hdr["source"]["medium"], "file");
    assert_eq!(hdr["source"]["title"], 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `disc://` is an ordinary input: `input()` opens the named drive (a missing device
/// fails as the drive open does), never refusing the scheme.
#[test]
fn input_disc_url_opens_the_drive() {
    let err = super::input(
        "disc:///nonexistent/freemkv-test-drive",
        &super::InputOptions::default(),
        &crate::ctx::Ctx::default(),
    )
    .err()
    .expect("no such drive");
    assert_ne!(err.kind(), std::io::ErrorKind::Unsupported, "got {err}");
}

/// null:// is write-only per the table — input() must reject it with
/// StreamWriteOnly (E9001 → Unsupported), not hand back a dead reader.
#[test]
fn input_null_url_is_write_only() {
    assert_eq!(input_err_kind("null://"), std::io::ErrorKind::Unsupported);
}

/// An unrecognized scheme on input() must surface StreamUrlInvalid
/// (E9002 → InvalidInput), carrying the raw URL — never silently succeed.
#[test]
fn input_unknown_url_is_invalid() {
    assert_eq!(
        input_err_kind("ftp://host/x"),
        std::io::ErrorKind::InvalidInput
    );
}

/// iso:// with an empty path must fail validate_file_path with
/// StreamUrlMissingPath (E9003 → InvalidInput) before any File::open.
#[test]
fn input_iso_empty_path_missing_path_error() {
    assert_eq!(input_err_kind("iso://"), std::io::ErrorKind::InvalidInput);
}

/// disc:// and iso:// are input-only sources — output() to either must
/// return StreamReadOnly (E9000 → Unsupported).
#[test]
fn output_disc_and_iso_are_read_only() {
    let t = DiscTitle::empty();
    assert_eq!(
        output_err_kind("disc://", &t),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(
        output_err_kind("iso://x.iso", &t),
        std::io::ErrorKind::Unsupported
    );
}

/// output() to an unknown scheme must surface StreamUrlInvalid
/// (E9002 → InvalidInput).
#[test]
fn output_unknown_url_is_invalid() {
    let t = DiscTitle::empty();
    assert_eq!(
        output_err_kind("gopher://x", &t),
        std::io::ErrorKind::InvalidInput
    );
}

// dir://PATH/ parses to StreamUrl::Dir and IS an image-level source (1.6.1), unlike
// demux://fvi:// sinks.
#[test]
fn parse_dir_url_is_an_image_source_unlike_the_directory_sinks() {
    match parse_url("dir://out/movie/") {
        StreamUrl::Dir { path } => {
            assert_eq!(path, PathBuf::from("out/movie/"));
        }
        other => panic!("dir:// must parse to Dir, got {other:?}"),
    }
    assert_eq!(parse_url("dir://x").scheme(), "dir");
    assert_eq!(parse_url("dir://x/y").path_str(), "x/y");
    assert_eq!(parse_url("demux://out/movie/").path_str(), "out/movie/");
    assert_eq!(parse_url("demux://x").scheme(), "demux");
    assert!(
        !parse_url("demux://x").is_disc_source(),
        "demux:// is a sink, never a disc source"
    );
    assert!(
        parse_url("dir://x").is_disc_source(),
        "dir:// carries a filesystem, so selection flags and image sinks apply"
    );
    // fvi:// parses to Fvi with the raw remainder as the path, and is a
    // sink (never a disc source) — parallel to the demux:// coverage above.
    match parse_url("fvi://out/movie.fvi") {
        StreamUrl::Fvi { path } => {
            assert_eq!(path, PathBuf::from("out/movie.fvi"));
        }
        other => panic!("fvi:// must parse to Fvi, got {other:?}"),
    }
    assert_eq!(parse_url("fvi://x").scheme(), "fvi");
    assert_eq!(parse_url("fvi://x/y.fvi").path_str(), "x/y.fvi");
    assert!(
        !parse_url("fvi://x").is_disc_source(),
        "fvi:// is a sink, never a disc source"
    );
}

/// `fvi://` is output-only: `input()` rejects it with StreamWriteOnly
/// (E9001 → Unsupported), mirroring `null://` / `demux://`.
#[test]
fn input_fvi_url_is_write_only() {
    assert_eq!(
        input_err_kind("fvi://out/movie.fvi"),
        std::io::ErrorKind::Unsupported
    );
}

// dir:// is never a PES sink (writes raw files; CLI routes it to extract_tree) but IS an
// image source.
#[test]
fn dir_url_is_an_input_but_never_a_pes_sink() {
    assert_eq!(
        input_err_kind("dir://definitely/not/here/"),
        std::io::ErrorKind::NotFound,
        "a dir:// source that does not exist must report a missing path"
    );
    let t = DiscTitle::empty();
    assert_eq!(
        output_err_kind("dir://out/", &t),
        std::io::ErrorKind::Unsupported
    );
}

/// output() to network:// with no port must fail validation
/// (StreamUrlMissingPort, E9004 → InvalidInput) before any TcpStream.
#[test]
fn output_network_missing_port_invalid() {
    let t = DiscTitle::empty();
    assert_eq!(
        output_err_kind("network://127.0.0.1", &t),
        std::io::ErrorKind::InvalidInput
    );
}

/// mkv:// with an empty path must fail validate_file_path
/// (StreamUrlMissingPath) on the output side, before WritebackFile.
#[test]
fn output_mkv_empty_path_missing_path_error() {
    let t = DiscTitle::empty();
    assert_eq!(
        output_err_kind("mkv://", &t),
        std::io::ErrorKind::InvalidInput
    );
}

// ── build_demux_state: parser/PID table + demuxer selection ────────────

fn aac_audio_title(pid: u16) -> DiscTitle {
    use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
    let mut t = DiscTitle::empty();
    t.streams.push(Stream::Audio(AudioStream {
        pid,
        codec: Codec::Aac, // → AdtsParser
        channels: AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    }));
    t
}

// BdTs must build a TsDemuxer with parsers/pid_to_track keyed by stream PID.
#[test]
fn build_demux_state_bdts_builds_ts_demuxer_and_pid_table() {
    let t = aac_audio_title(0x1100);
    let (parsers, pid_to_track, ts, ps) = build_demux_state(&t, ContentFormat::BdTs);
    assert_eq!(parsers.len(), 1);
    assert_eq!(parsers[0].0, 0x1100, "parser keyed by the stream PID");
    assert_eq!(pid_to_track, vec![(0x1100u16, 0usize)]);
    assert!(ts.is_some(), "BdTs → TsDemuxer");
    assert!(ps.is_none());
}

/// Both program-stream formats (HD DVD / plain PS, and DVD) build a PsDemuxer
/// (None(ts), Some(ps)) regardless of PIDs: the container is the same.
#[test]
fn build_demux_state_mpegps_builds_ps_demuxer() {
    let t = aac_audio_title(0xBD80);
    for format in [ContentFormat::MpegPs, ContentFormat::DvdPs] {
        let (_parsers, _p2t, ts, ps) = build_demux_state(&t, format);
        assert!(ts.is_none());
        assert!(ps.is_some(), "{format:?} → PsDemuxer");
    }
}

/// An empty BdTs title (no streams) must NOT construct a TsDemuxer —
/// `TsDemuxer::new(&[])` is pointless, and the builder special-cases
/// empty PIDs to (None, None). pid_to_track/parsers also empty.
#[test]
fn build_demux_state_bdts_empty_streams_builds_no_demuxer() {
    let t = DiscTitle::empty();
    let (parsers, pid_to_track, ts, ps) = build_demux_state(&t, ContentFormat::BdTs);
    assert!(parsers.is_empty());
    assert!(pid_to_track.is_empty());
    assert!(ts.is_none(), "no PIDs → no TsDemuxer");
    assert!(ps.is_none());
}

// ── build_iso_pipeline: end-to-end highway wiring ──────────────────────

/// An in-memory SectorSource that serves a fixed byte image. Reads beyond
/// the image return zero-filled sectors (the prefetcher only reads within
/// the title's extents, so this is never hit in these tests).
struct MemSource {
    data: Vec<u8>,
}
impl SectorSource for MemSource {
    fn capacity_sectors(&self) -> u32 {
        (self.data.len() / 2048) as u32
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let start = lba as usize * 2048;
        let want = count as usize * 2048;
        for (i, b) in buf[..want].iter_mut().enumerate() {
            *b = self.data.get(start + i).copied().unwrap_or(0);
        }
        Ok(want)
    }
}

// Build a 192-byte BD-TS data packet on pid carrying payload as the TS payload. Mirrors
// ts.rs framing.
fn bdts_data_packet(pid: u16, pusi: bool, payload: &[u8]) -> [u8; 192] {
    let mut pkt = [0u8; 192];
    pkt[4] = 0x47; // sync byte
    pkt[5] = ((pid >> 8) as u8) & 0x1F;
    if pusi {
        pkt[5] |= 0x40; // PUSI
    }
    pkt[6] = (pid & 0xFF) as u8;
    pkt[7] = 0x10; // adaptation_field_control = 0b01 (payload only)
    let room = 184; // 188 - 4-byte TS header
    let n = payload.len().min(room);
    pkt[8..8 + n].copy_from_slice(&payload[..n]);
    pkt
}

/// A complete audio PES (stream_id 0xC0) with no PTS, carrying `es` as the
/// elementary-stream payload. Layout per ISO 13818-1: 00 00 01 C0
/// [len:2] [0x80 flags1] [0x00 flags2] [0x00 header_data_len] [es...].
fn audio_pes(es: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x00, 0x01, 0xC0];
    let len = (3 + es.len()) as u16; // flags(2)+hdl(1)+es
    v.extend_from_slice(&len.to_be_bytes());
    v.extend_from_slice(&[0x80, 0x00, 0x00]);
    v.extend_from_slice(es);
    v
}

// Empty extents -> clean immediate EOF, no panic/hang.
#[test]
fn build_iso_pipeline_empty_extents_clean_eof() {
    // Spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let title = aac_audio_title(0x1100); // extents empty by default
    let mut stream = build_iso_pipeline(
        MemSource { data: Vec::new() },
        title,
        DecryptKeys::None,
        8192,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .expect("pipeline builds");
    let first = stream.read().expect("read must not error on clean EOF");
    assert!(
        first.is_none(),
        "no extents → immediate clean end-of-stream"
    );
    // Idempotent: a second read past EOF is still Ok(None), never an error.
    assert!(stream.read().unwrap().is_none());
}

// End-to-end: one BD-TS packet flows read -> decrypt -> demux -> parse -> one PesFrame,
// then clean EOF.
#[test]
fn build_iso_pipeline_delivers_one_frame_then_eof() {
    // Spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let es = [0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22];
    let pes = audio_pes(&es);
    let pkt = bdts_data_packet(0x1100, true, &pes);
    // One 2048-byte sector holding the 192-byte packet (rest zero — the
    // demuxer skips non-sync packets). Extent = 3 sectors (one AACS unit,
    // the prefetcher's alignment requirement).
    let mut data = vec![0u8; 3 * 2048];
    data[..192].copy_from_slice(&pkt);

    let mut title = aac_audio_title(0x1100);
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: 3,
    }];

    let mut stream = build_iso_pipeline(
        MemSource { data },
        title,
        DecryptKeys::None,
        8192,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .expect("pipeline builds");

    let frame = stream
        .read()
        .expect("read ok")
        .expect("one frame emitted from the single PES");
    // AdtsParser passes the unsynced ES through; PID 0x1100 routes to track 0.
    assert_eq!(frame.track, 0);
    // TS PesAssembler delivers every byte after the 9-byte PES header to
    // the end of the 184-byte TS payload (PES is closed by next PUSI or
    // flush at EOF, not by PES_packet_length). Frame = ES + zero padding.
    assert_eq!(
        frame.data.len(),
        184 - 9,
        "frame spans the full TS payload after the PES header"
    );
    // Truncation guard: the ES bytes lead the frame, in order, unaltered —
    // the highway must never drop or reorder the elementary-stream prefix.
    assert_eq!(
        &frame.data[..es.len()],
        &es[..],
        "ES payload prefix delivered intact and in order"
    );
    assert!(
        frame.data[es.len()..].iter().all(|&b| b == 0),
        "remainder is the packet's zero padding, not foreign data"
    );
    // After the single frame the stream reaches a clean EOF.
    assert!(
        stream.read().unwrap().is_none(),
        "clean EOF after the frame"
    );
}

// A title with TWO audio PIDs, pruned to one before build_iso_pipeline, must never surface
// a frame from the excluded PID.
#[test]
fn build_iso_pipeline_pruned_title_drops_unselected_pid_frames() {
    // Spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
    use crate::mux::select::{PidFilter, StreamSelection};

    let es_keep = [0xDE, 0xAD, 0xBE, 0xEF];
    let es_drop = [0x99, 0x88, 0x77, 0x66];
    let pkt_keep = bdts_data_packet(0x1100, true, &audio_pes(&es_keep));
    let pkt_drop = bdts_data_packet(0x1101, true, &audio_pes(&es_drop));
    // Both 192-byte packets in one 3-sector extent (offsets 0 and 192).
    let mut data = vec![0u8; 3 * 2048];
    data[..192].copy_from_slice(&pkt_keep);
    data[192..384].copy_from_slice(&pkt_drop);

    // Title declares BOTH audio streams (eng 0x1100, spa 0x1101).
    let mut title = aac_audio_title(0x1100);
    title.streams.push(Stream::Audio(AudioStream {
        pid: 0x1101,
        codec: Codec::Aac,
        channels: AudioChannels::Stereo,
        language: "spa".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    }));
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: 3,
    }];

    // Prune to keep only PID 0x1100 (the eng audio) — exactly what a
    // `-a eng` selection resolves to.
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![0x1100]),
        subtitle: PidFilter::All,
    };
    sel.apply(&mut title).unwrap();
    assert_eq!(
        title.streams.len(),
        1,
        "only the kept audio survives pruning"
    );

    let mut stream = build_iso_pipeline(
        MemSource { data },
        title,
        DecryptKeys::None,
        8192,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    )
    .expect("pipeline builds");

    // Exactly ONE frame — the kept PID's — reaches us; the 0x1101 packet was
    // never tracked by the demuxer, so it produced no frame.
    let frame = stream
        .read()
        .expect("read ok")
        .expect("one frame from 0x1100");
    assert_eq!(frame.track, 0, "the single retained stream is track 0");
    assert_eq!(
        &frame.data[..es_keep.len()],
        &es_keep[..],
        "the KEPT PID's ES bytes"
    );
    assert!(
        stream.read().unwrap().is_none(),
        "clean EOF — the excluded 0x1101 packet never surfaced as a frame"
    );
    // The muxed stream info advertises exactly the one retained audio stream.
    assert_eq!(stream.info().streams.len(), 1);
}

/// build_iso_pipeline with batch_sectors = 0 must fail fast (the
/// prefetcher rejects a zero batch as a programming error — a zero batch
/// would spin the producer forever). Surfaced as an io error, not a hang.
#[test]
fn build_iso_pipeline_zero_batch_rejected() {
    // Spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let title = aac_audio_title(0x1100);
    let res = build_iso_pipeline(
        MemSource { data: Vec::new() },
        title,
        DecryptKeys::None,
        0,
        ContentFormat::BdTs,
        false,
        &crate::ctx::Ctx::default(),
    );
    assert!(res.is_err(), "zero batch_sectors must be rejected");
}

// REGRESSION: build_iso_pipeline for a DVD with None keys must resolve the CSS key itself;
// scrambled-uncrackable must HARD-FAIL, never mux garbage.
#[test]
fn build_iso_pipeline_dvd_none_keys_scrambled_hard_fails() {
    // Spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    // One CSS-scrambled, crib-less (uncrackable) MPEG-PS sector.
    let key = [0x11u8, 0x22, 0x33, 0x44, 0x55];
    let mut sec = vec![0u8; 2048];
    sec[0..4].copy_from_slice(&crate::css::PACK_START);
    for (i, b) in sec.iter_mut().enumerate().take(0x80).skip(4) {
        *b = (i as u8).wrapping_mul(7).wrapping_add(1); // non-repeating → no crib
    }
    sec[4] = 0x44; // '01': a 13818-1 pack
    sec[0x0D] = 0xF8; // pack_stuffing_length 0
    sec[0x14] = 0x10; // scramble flag
    crate::css::dvd_pack_header(&mut sec, 0xE0);
    for (i, b) in sec.iter_mut().enumerate().skip(0x80) {
        *b = (i as u8) ^ 0x3C;
    }
    crate::css::lfsr::scramble_sector(&key, &mut sec);

    let mut title = aac_audio_title(0x1100);
    title.extents = vec![Extent {
        start_lba: 0,
        sector_count: 1,
    }];

    let res = build_iso_pipeline(
        MemSource { data: sec },
        title,
        DecryptKeys::None,
        8192,
        ContentFormat::DvdPs,
        false,
        &crate::ctx::Ctx::default(),
    );
    assert!(
        res.is_err(),
        "a scrambled DVD title with no key must hard-fail, not build a scrambled-passthrough pipeline"
    );
}

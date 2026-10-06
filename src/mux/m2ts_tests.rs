use super::*;
use crate::disc::{
    Codec, ColorSpace, ContentFormat, DiscTitle, FrameRate, HdrFormat, Resolution,
    Stream as DiscStream, VideoStream,
};
use crate::pes::{PesFrame, PesSink as _};

const VIDEO_PID: u16 = 0x1011;

fn make_title() -> DiscTitle {
    DiscTitle {
        playlist: String::new(),
        playlist_id: 0,
        duration_secs: 0.0,
        size_bytes: 0,
        clips: Vec::new(),
        streams: vec![DiscStream::Video(VideoStream {
            pid: VIDEO_PID,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F24,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })],
        chapters: Vec::new(),
        extents: Vec::new(),
        content_format: ContentFormat::BdTs,
        codec_privates: vec![Some({
            // Minimal hvcC with one VPS-like array entry.
            let marker: &[u8] = &[0x40, 0x01, 0x0C, 0x01];
            let mut hvcc = vec![0u8; 22];
            hvcc.push(1); // numArrays
            hvcc.push(32);
            hvcc.extend_from_slice(&1u16.to_be_bytes()); // numNalus
            hvcc.extend_from_slice(&(marker.len() as u16).to_be_bytes());
            hvcc.extend_from_slice(marker);
            hvcc
        })],
    }
}

// The file is BD-TS whatever the source was, so its header must say so.
#[test]
fn a_dvd_title_written_to_m2ts_reads_back_as_bd_ts() {
    let mut title = make_title();
    title.content_format = ContentFormat::MpegPs;
    let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    drop(M2tsStream::create(SharedSink(buf.clone()), &title).unwrap());
    let bytes = buf.lock().unwrap().clone();
    let meta = meta::read_header(&mut io::Cursor::new(bytes))
        .unwrap()
        .expect("header");
    assert_eq!(meta.to_title().content_format, ContentFormat::BdTs);
}

fn fake_idr_pes_data() -> Vec<u8> {
    // 4-byte length prefix + NAL: type 19 (IDR_W_RADL).
    let mut nal = vec![(19u8 << 1) & 0x7E, 0x01];
    for i in 0..200 {
        nal.push((i & 0xFF) as u8);
    }
    let mut out = Vec::with_capacity(4 + nal.len());
    out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
    out.extend_from_slice(&nal);
    out
}

/// Writer wrapper that shares an Arc<Mutex<Vec<u8>>> so the test can
/// inspect the bytes after the muxer drops.
struct SharedSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl Write for SharedSink {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// avcC (not hvcC) must parse via the avcC parser so SPS/PPS reach the player as Annex-B;
// otherwise the ES is silently undecodable.
#[test]
fn h264_avcc_parameter_sets_are_emitted_as_annex_b() {
    let sps: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xAB, 0xCD];
    let pps: &[u8] = &[0x68, 0xCE, 0x3C, 0x80];
    // avcC (ISO/IEC 14496-15 §5.3.3.1.2): 5-byte fixed header, then
    // numOfSequenceParameterSets (low 5 bits), each SPS as u16-BE length +
    // bytes, then numOfPictureParameterSets, each PPS likewise.
    let mut avcc = vec![0x01, 0x42, 0xC0, 0x1E, 0xFF];
    avcc.push(0xE0 | 1); // reserved 111b + numSPS = 1
    avcc.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    avcc.extend_from_slice(sps);
    avcc.push(1); // numPPS = 1
    avcc.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    avcc.extend_from_slice(pps);

    let mut title = make_title();
    if let DiscStream::Video(v) = &mut title.streams[0] {
        v.codec = Codec::H264;
    }
    title.codec_privates = vec![Some(avcc)];

    // A length-prefixed IDR NAL, the shape the muxer expects for NAL video.
    let nal: Vec<u8> = vec![0x65, 0x88, 0x84, 0x00, 0x11, 0x22];
    let mut es = (nal.len() as u32).to_be_bytes().to_vec();
    es.extend_from_slice(&nal);

    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let sink = SharedSink(shared.clone());
    let mut stream = M2tsStream::create(sink, &title).unwrap();
    stream
        .write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: es,
            duration_ns: None,
        })
        .unwrap();
    stream.finish().unwrap();
    drop(stream);

    let buf = shared.lock().unwrap().clone();
    assert!(
        buf.windows(sps.len()).any(|w| w == sps),
        "the avcC SPS must reach the transport stream"
    );
    assert!(
        buf.windows(pps.len()).any(|w| w == pps),
        "the avcC PPS must reach the transport stream"
    );
}

// create() must opt VC-1 video OUT of Annex-B conversion; this pins the wiring in create()
// itself, not just TsMuxer's flag.
#[test]
fn vc1_video_is_wired_to_the_non_nal_path() {
    let mut title = make_title();
    if let DiscStream::Video(v) = &mut title.streams[0] {
        v.codec = Codec::Vc1;
    }
    title.codec_privates = vec![None];

    // Length-prefix SHAPED ES: if the conversion is wrongly applied it rewrites
    // these leading four bytes into a 00 00 00 01 start code.
    let es: Vec<u8> = vec![0x00, 0x00, 0x00, 0x06, 0x0F, 0x12, 0x34, 0x56, 0x78, 0x9A];

    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let sink = SharedSink(shared.clone());
    let mut stream = M2tsStream::create(sink, &title).unwrap();
    stream
        .write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: es.clone(),
            duration_ns: None,
        })
        .unwrap();
    stream.finish().unwrap();
    drop(stream);

    let buf = shared.lock().unwrap().clone();
    assert!(
        buf.windows(es.len()).any(|w| w == &es[..]),
        "VC-1 ES must reach the output verbatim, not converted to Annex-B"
    );
}

#[test]
fn m2ts_stream_forwards_keyframe_to_rai() {
    let title = make_title();
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let sink = SharedSink(shared.clone());
    let mut stream = M2tsStream::create(sink, &title).unwrap();
    let frame = PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track: 0,
        pts: 0,
        keyframe: true,
        data: fake_idr_pes_data(),
        duration_ns: None,
    };
    stream.write(&frame).unwrap();
    stream.finish().unwrap();
    drop(stream);

    let buf = shared.lock().unwrap().clone();

    // Skip FMKV metadata header via meta::read_header.
    let mut cursor = std::io::Cursor::new(&buf);
    let _meta = super::meta::read_header(&mut cursor)
        .unwrap()
        .expect("FMKV header present");
    let header_end = cursor.position() as usize;
    let ts_bytes = &buf[header_end..];

    // First PUSI packet on VIDEO_PID; verify RAI in AF flags. as_chunks
    // drops a partial trailing chunk (.0 = whole chunks only) — valid BD-TS
    // packets are 192 bytes, and it avoids OOB indexing on a short chunk.
    let pkt = ts_bytes
        .as_chunks::<192>()
        .0
        .iter()
        .find(|p| {
            let h = &p[4..];
            let pid = (((h[1] & 0x1F) as u16) << 8) | h[2] as u16;
            pid == VIDEO_PID && (h[1] & 0x40) != 0
        })
        .expect("video PUSI packet present");
    let h = &pkt[4..];
    let afc = (h[3] >> 4) & 0x03;
    assert!(afc & 0b10 != 0, "AF must be present");
    let af_len = h[4] as usize;
    assert!(af_len >= 1, "AF length must include flags byte");
    let flags = h[5];
    assert_eq!(flags & 0x40, 0x40, "RAI bit set");
}
#[test]
fn lpcm_round_trips_through_m2ts_write_and_read() {
    // The parser emits 24-bit WAVE-order PCM; the M2TS writer must re-synthesise
    // the BD LPCM header, pad channel and BD channel order so our own m2ts://
    // reader (LpcmParser::new) recovers identical PCM.
    use crate::disc::{AudioChannels, AudioStream, LabelPurpose, SampleRate};
    use crate::mux::codec::CodecParser;
    const AUDIO_PID: u16 = 0x1100;
    for (channels, samples) in [
        (AudioChannels::Mono, 240),
        (AudioChannels::Stereo21, 240),
        (AudioChannels::Surround51, 240),
        (AudioChannels::Surround71, 240),
        (AudioChannels::Mono, 20_000),
        (AudioChannels::Stereo, 20_000),
        (AudioChannels::Stereo21, 20_000),
        (AudioChannels::Quad, 20_000),
    ] {
        let mut title = make_title();
        title.streams.push(DiscStream::Audio(AudioStream {
            pid: AUDIO_PID,
            codec: Codec::Lpcm,
            channels,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
        let ch = channels.count() as usize;
        let pcm: Vec<u8> = (0..samples * ch * 3).map(|i| (i % 251) as u8).collect();
        for (i, chunk) in [&pcm[..], &pcm[..]].iter().enumerate() {
            stream
                .write(&PesFrame {
                    discard_padding_ns: 0,
                    coding: None,
                    source: None,
                    track: 1,
                    pts: i as i64 * samples as i64 * 1_000_000_000 / 48_000,
                    keyframe: true,
                    data: chunk.to_vec(),
                    duration_ns: None,
                })
                .unwrap();
        }
        stream.finish().unwrap();
        drop(stream);

        let buf = shared.lock().unwrap().clone();
        let mut cursor = std::io::Cursor::new(&buf);
        super::meta::read_header(&mut cursor).unwrap().unwrap();
        let ts = &buf[cursor.position() as usize..];
        let mut demux = crate::mux::ts::TsDemuxer::new(&[AUDIO_PID]);
        let mut pes = demux.feed(ts);
        pes.extend(demux.flush());
        // Each 5 ms BD sub-payload carries its own offset PTS, so none repeat.
        let pts: Vec<i64> = pes.iter().map(|p| p.pts.expect("PES PTS")).collect();
        assert!(
            pts.windows(2).all(|w| w[0] < w[1]),
            "{channels:?} x{samples}: PES PTS strictly increasing, got {pts:?}"
        );
        let mut parser = crate::mux::codec::lpcm::LpcmParser::new();
        let got: Vec<u8> = pes
            .iter()
            .flat_map(|p| parser.parse(p))
            .flat_map(|f| f.data)
            .collect();
        let mut want = pcm.clone();
        want.extend_from_slice(&pcm);
        assert_eq!(
            got.len(),
            want.len(),
            "{channels:?} x{samples}: no bytes lost"
        );
        assert_eq!(
            got, want,
            "{channels:?} x{samples}: PCM survives m2ts write -> read"
        );
    }
}
fn lpcm_title(channels: crate::disc::AudioChannels, rate: crate::disc::SampleRate) -> DiscTitle {
    let mut title = make_title();
    title
        .streams
        .push(DiscStream::Audio(crate::disc::AudioStream {
            pid: 0x1100,
            codec: Codec::Lpcm,
            channels,
            language: "eng".into(),
            sample_rate: rate,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: String::new(),
        }));
    title
}

fn ts_after_header(buf: &[u8]) -> (crate::mux::meta::M2tsMeta, Vec<u8>) {
    let mut cursor = std::io::Cursor::new(buf);
    let m = super::meta::read_header(&mut cursor).unwrap().unwrap();
    (m, buf[cursor.position() as usize..].to_vec())
}

#[test]
fn uncarriable_lpcm_is_dropped_from_the_output_at_create() {
    // BD LPCM has no 44.1 kHz: the track must not be advertised (FMKV/PMT)
    // with an empty PID; other tracks keep working.
    use crate::disc::{AudioChannels, SampleRate};
    let title = lpcm_title(AudioChannels::Stereo, SampleRate::S44_1);
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    for track in [0, 1] {
        stream
            .write(&PesFrame {
                discard_padding_ns: 0,
                coding: None,
                source: None,
                track,
                pts: 0,
                keyframe: true,
                data: if track == 0 {
                    fake_idr_pes_data()
                } else {
                    vec![0; 12]
                },
                duration_ns: None,
            })
            .unwrap();
    }
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (meta, ts) = ts_after_header(&buf);
    assert_eq!(meta.streams.len(), 1, "only the video track is advertised");
    let has_pid = |pid: u16| {
        ts.as_chunks::<192>().0.iter().any(|p| {
            let h = &p[4..];
            (((h[1] & 0x1F) as u16) << 8 | h[2] as u16) == pid
        })
    };
    assert!(has_pid(VIDEO_PID), "video still written");
    assert!(!has_pid(0x1100), "no packets on the dropped LPCM PID");
}

#[test]
fn lpcm_reuses_the_source_channel_assignment() {
    // A source 2/2 (assignment 7) must not be re-labelled 3/1 (4ch default 6).
    use crate::disc::{AudioChannels, SampleRate};
    let mut title = lpcm_title(AudioChannels::Quad, SampleRate::S48);
    title.codec_privates = vec![None, Some(b"BDLP\x71\x18".to_vec())];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    stream
        .write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 1,
            pts: 0,
            keyframe: true,
            data: vec![0; 240 * 4 * 3],
            duration_ns: None,
        })
        .unwrap();
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let mut demux = crate::mux::ts::TsDemuxer::new(&[0x1100]);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    assert_eq!(
        pes[0].data[2], 0x71,
        "BD header keeps channel_assignment 7 (2/2)"
    );
}
#[test]
fn lpcm_ignores_an_untagged_foreign_layout_byte() {
    use crate::disc::{AudioChannels, SampleRate};
    let mut title = lpcm_title(AudioChannels::Quad, SampleRate::S48);
    title.codec_privates = vec![None, Some(vec![0x71])];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    stream
        .write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 1,
            pts: 0,
            keyframe: true,
            data: vec![0; 240 * 4 * 3],
            duration_ns: None,
        })
        .unwrap();
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let mut demux = crate::mux::ts::TsDemuxer::new(&[0x1100]);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    assert_eq!(
        pes[0].data[2], 0x61,
        "count default (4.0), not the foreign byte"
    );
}

#[test]
fn dropped_lpcm_is_reported_undelivered() {
    use crate::disc::{AudioChannels, SampleRate};
    let title = lpcm_title(AudioChannels::Stereo, SampleRate::S44_1);
    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let stream = M2tsStream::create(sink, &title).unwrap();
    assert_eq!(stream.undelivered_streams(), vec![1]);
}
#[test]
fn sixteen_bit_lpcm_is_repacked_as_16_bit_bd_lpcm() {
    use crate::disc::{AudioChannels, SampleRate};
    let mut title = lpcm_title(AudioChannels::Stereo, SampleRate::S48);
    title.codec_privates = vec![None, Some(b"DVLP\x10".to_vec())];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    stream
        .write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 1,
            pts: 0,
            keyframe: true,
            data: vec![0; 240 * 2 * 2],
            duration_ns: None,
        })
        .unwrap();
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let mut demux = crate::mux::ts::TsDemuxer::new(&[0x1100]);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    assert_eq!(pes[0].data[3] >> 6, 1, "16-bit quantization code");
    assert_eq!(pes[0].data.len(), 4 + 240 * 2 * 2);
}

#[test]
fn fmkv_header_carries_the_layout_byte_actually_used() {
    // 2ch stream with a stale 5.1 layout byte: packing falls back to stereo, so
    // the FMKV header must carry the stereo byte, not the rejected 5.1 one.
    use crate::disc::{AudioChannels, SampleRate};
    let mut title = lpcm_title(AudioChannels::Stereo, SampleRate::S48);
    title.codec_privates = vec![None, Some(b"BDLP\x91\x18".to_vec())];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let mut cursor = std::io::Cursor::new(&buf);
    let meta = super::meta::read_header(&mut cursor).unwrap().unwrap();
    let back = meta.to_title();
    assert_eq!(back.codec_privates[1], Some(b"BDLP\x31\x18".to_vec()));
}

/// (PTS, DTS) of every PES start on `pid` in `ts` (BD-TS after the FMKV header).
fn pes_times(ts: &[u8], pid: u16) -> Vec<(u64, Option<u64>)> {
    let t33 = |b: &[u8]| {
        ((((b[0] >> 1) & 0x07) as u64) << 30)
            | ((b[1] as u64) << 22)
            | (((b[2] >> 1) as u64) << 15)
            | ((b[3] as u64) << 7)
            | ((b[4] >> 1) as u64)
    };
    ts.as_chunks::<192>()
        .0
        .iter()
        .filter(|p| (((p[5] & 0x1F) as u16) << 8 | p[6] as u16) == pid && p[5] & 0x40 != 0)
        .map(|p| {
            let h = &p[4..];
            let body = if (h[3] >> 4) & 0b10 != 0 {
                &h[5 + h[4] as usize..]
            } else {
                &h[4..]
            };
            let dts = (body[7] >> 6 == 0b11).then(|| t33(&body[14..19]));
            (t33(&body[9..14]), dts)
        })
        .collect()
}

fn frame(track: usize, pts: i64, keyframe: bool, data: Vec<u8>) -> PesFrame {
    PesFrame {
        discard_padding_ns: 0,
        coding: None,
        source: None,
        track,
        pts,
        keyframe,
        data,
        duration_ns: None,
    }
}

// Design §7 (MPG3-3): field pairs detected from the ES bytes across a network:// hop,
// where `PesFrame::coding` does not survive.
#[test]
fn mpeg2_field_pairs_get_dts_from_es_bytes_after_a_network_hop() {
    use crate::mux::decode_ts::test_es as es;
    let mut title = make_title();
    if let DiscStream::Video(v) = &mut title.streams[0] {
        v.codec = Codec::Mpeg2;
    }
    title.codec_privates = vec![None];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    // Decode I0t I0b P3t P3b B1t B1b, PTS in 25 Hz field periods (20 ms).
    let fields = [
        (0, 1, 1),
        (1, 1, 2),
        (6, 2, 1),
        (7, 2, 2),
        (2, 3, 1),
        (3, 3, 2),
    ];
    for (i, &(f, coding, structure)) in fields.iter().enumerate() {
        let mut data = if i == 0 {
            es::mpeg2_seq(3, false)
        } else {
            Vec::new()
        };
        data.extend(es::mpeg2_pic(coding, structure));
        let sent = frame(0, 1_000_000_000 + f * 20_000_000, coding == 1, data);
        let mut wire = Vec::new();
        sent.serialize(&mut wire).unwrap();
        let got = PesFrame::deserialize(&mut &wire[..]).unwrap().unwrap();
        assert!(got.coding.is_none(), "the hop drops PesFrame::coding");
        stream.write(&got).unwrap();
    }
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let t = pes_times(&ts, VIDEO_PID);
    let has: Vec<bool> = t.iter().map(|(_, d)| d.is_some()).collect();
    assert_eq!(
        has,
        [true, true, true, true, false, false],
        "I/P fields carry DTS, B fields do not"
    );
    assert_eq!(
        t[1].1.unwrap() - t[0].1.unwrap(),
        1_800,
        "2nd field: DTS_1st + ΔPTS"
    );
    assert_eq!(t[2].1, Some(t[0].0), "P3 decodes when I0 is presented");
}

// A multi-clip title's source PTS restarts at each join: the TS carries one continuous
// timeline, so the second clip follows the first and its reordered AUs keep their DTS.
#[test]
fn a_clip_join_pts_reset_continues_the_timeline_and_its_dts() {
    use crate::mux::decode_ts::test_es as es;
    let sps = es::sps_with_reorder(2);
    let mut title = make_title();
    if let DiscStream::Video(v) = &mut title.streams[0] {
        v.codec = Codec::H264;
    }
    title.codec_privates = vec![Some(es::avcc(&sps.nal()))];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    // Decode order I P B B, repeated: 6 s clips at 24 fps, each from 1 s.
    let order = [0i64, 3, 1, 2];
    for _clip in 0..2 {
        for i in 0..144i64 {
            let shown = (i / 4) * 4 + order[(i % 4) as usize];
            let au = es::length_prefixed(&[es::h264_slice(&sps, i == 0, 0, i as u32, None)]);
            let pts = 1_000_000_000 + shown * 41_666_667;
            stream.write(&frame(0, pts, i == 0, au)).unwrap();
        }
    }
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let t = pes_times(&ts, VIDEO_PID);
    assert_eq!(t.len(), 288);
    let clip1_max = t[..144].iter().map(|x| x.0).max().unwrap();
    let clip2_min = t[144..].iter().map(|x| x.0).min().unwrap();
    assert!(
        clip2_min > clip1_max,
        "clip 2 at {clip2_min} overlaps clip 1 ({clip1_max})"
    );
    let dts: Vec<u64> = t.iter().map(|&(p, d)| d.unwrap_or(p)).collect();
    assert!(
        dts.windows(2).all(|w| w[1] > w[0]),
        "DTS strictly increase across the join: {:?}",
        &dts[140..148]
    );
    let with_dts = |r: &[(u64, Option<u64>)]| r.iter().filter(|x| x.1.is_some()).count();
    assert_eq!(
        with_dts(&t[144..]),
        with_dts(&t[..144]),
        "clip 2 keeps its DTS"
    );
}

// Design §2.3: the MVC dependent view copies its base AU's DTS.
fn mvc_dependent_dts(dependent_first: bool) {
    use crate::mux::decode_ts::test_es as es;
    let sps = es::sps_with_reorder(2);
    let mut title = make_title();
    let DiscStream::Video(base) = title.streams[0].clone() else {
        unreachable!()
    };
    title.streams[0] = DiscStream::Video(VideoStream {
        codec: Codec::H264,
        ..base.clone()
    });
    title.streams.push(DiscStream::Video(VideoStream {
        pid: VIDEO_PID + 1,
        codec: Codec::H264,
        secondary: true,
        label: crate::disc::MVC_DEPENDENT_LABEL.to_string(),
        ..base
    }));
    title.codec_privates = vec![Some(es::avcc(&sps.nal())), None];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    for (i, d) in [0i64, 4, 2, 1, 3, 8].into_iter().enumerate() {
        let pts = 1_000_000_000 + d * 41_666_667;
        let base_au = es::length_prefixed(&[es::h264_slice(&sps, i == 0, 0, i as u32, None)]);
        let dep_au = es::length_prefixed(&[vec![0x14, 0x80, i as u8], vec![0x74, 0x11]]);
        let (b, dep) = (
            frame(0, pts, i == 0, base_au),
            frame(1, pts, i == 0, dep_au),
        );
        for f in if dependent_first { [dep, b] } else { [b, dep] } {
            stream.write(&f).unwrap();
        }
    }
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (_, ts) = ts_after_header(&buf);
    let (base_t, dep_t) = (pes_times(&ts, VIDEO_PID), pes_times(&ts, VIDEO_PID + 1));
    assert_eq!(base_t.len(), 6);
    assert!(base_t.iter().filter(|(_, d)| d.is_some()).count() >= 4);
    assert_eq!(
        dep_t, base_t,
        "each dependent AU carries its base AU's PTS and DTS (dependent first: {dependent_first})"
    );
}

// Write one frame per track of `title` and return (header pids, BD-TS bytes).
fn mux_one_frame_each(title: &DiscTitle, data: &[Vec<u8>]) -> (Vec<u16>, Vec<u8>) {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), title).unwrap();
    for (track, d) in data.iter().enumerate() {
        stream.write(&frame(track, 0, true, d.clone())).unwrap();
    }
    stream.finish().unwrap();
    drop(stream);
    let buf = shared.lock().unwrap().clone();
    let (meta, ts) = ts_after_header(&buf);
    let pids = meta
        .to_title()
        .streams
        .iter()
        .map(|s| match s {
            DiscStream::Video(v) => v.pid,
            DiscStream::Audio(a) => a.pid,
            DiscStream::Subtitle(s) => s.pid,
        })
        .collect();
    (pids, ts)
}

fn ac3_audio(pid: u16) -> DiscStream {
    DiscStream::Audio(crate::disc::AudioStream {
        pid,
        codec: Codec::Ac3,
        channels: crate::disc::AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: crate::disc::SampleRate::S48,
        secondary: false,
        purpose: crate::disc::LabelPurpose::Normal,
        label: String::new(),
    })
}

// DVD ids (video 0xE0, audio 0xBD80) are not 13-bit TS PIDs: the header must name
// the PID the packets actually carry, and video must use the video path.
#[test]
fn dvd_stream_ids_get_bd_pids_that_the_header_records() {
    let mut title = make_title();
    if let DiscStream::Video(v) = &mut title.streams[0] {
        v.pid = 0xE0;
        v.codec = Codec::H264;
    }
    title.streams.push(ac3_audio(0xBD80));
    title.codec_privates = vec![None, None];
    let audio = vec![0x0B, 0x77, 1, 2, 3, 4, 5, 6];
    let (pids, ts) = mux_one_frame_each(&title, &[fake_idr_pes_data(), audio.clone()]);
    assert!(pids.iter().all(|&p| p <= 0x1FFF), "13-bit PIDs: {pids:x?}");
    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    let got = |pid| pes.iter().find(|p| p.pid == pid).map(|p| p.data.clone());
    assert_eq!(got(pids[1]), Some(audio), "audio found on its header PID");
    let v = got(pids[0]).expect("video found on its header PID");
    assert!(v.starts_with(&[0, 0, 0, 1]), "video converted to Annex B");
    assert!(
        pes_times(&ts, pids[0]).len() == 1,
        "video PES on the header PID"
    );
}

// An audio track on a video-range PID (MKV whose track 1 is audio) must pass verbatim.
#[test]
fn audio_on_a_video_range_pid_is_not_treated_as_video() {
    let mut title = make_title();
    title.streams = vec![ac3_audio(0x1011)];
    title.codec_privates = vec![None];
    let audio = vec![0, 0, 0, 4, 0x0B, 0x77, 9, 9];
    let (pids, ts) = mux_one_frame_each(&title, std::slice::from_ref(&audio));
    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    assert_eq!(pes.len(), 1);
    assert_eq!(pes[0].data, audio, "audio bytes pass through unconverted");
}

// BD PiP (0x1B00) and MKV non-first video (0x1100+) move into the video range; BD PIDs
// already in their range are kept, and each track reads back on its header PID.
#[test]
fn out_of_range_video_moves_into_the_video_range_and_bd_pids_stay() {
    let mut title = make_title();
    let DiscStream::Video(v) = title.streams[0].clone() else {
        unreachable!()
    };
    for pid in [0x1B00, 0x1100] {
        title
            .streams
            .push(DiscStream::Video(VideoStream { pid, ..v.clone() }));
    }
    title.streams.push(ac3_audio(0x1101));
    title
        .streams
        .push(DiscStream::Subtitle(crate::disc::SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        }));
    title.codec_privates = vec![None; 5];
    let audio = vec![0x0B, 0x77, 1, 2];
    let sub = vec![0x16, 0, 0];
    let v = fake_idr_pes_data();
    let data = [v.clone(), v.clone(), v, audio.clone(), sub.clone()];
    let (pids, ts) = mux_one_frame_each(&title, &data);
    assert_eq!(pids, [0x1011, 0x1012, 0x1013, 0x1101, 0x1200]);
    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    let got = |pid| pes.iter().find(|p| p.pid == pid).map(|p| p.data.clone());
    for &pid in &pids[..3] {
        let v = got(pid).expect("video found on its header PID");
        assert!(
            v.starts_with(&[0, 0, 0, 1]),
            "video {pid:#x} converted to Annex B"
        );
    }
    assert_eq!(got(0x1101), Some(audio));
    assert_eq!(got(0x1200), Some(sub));
}

fn audio(pid: u16, codec: Codec) -> DiscStream {
    let DiscStream::Audio(a) = ac3_audio(pid) else {
        unreachable!()
    };
    DiscStream::Audio(crate::disc::AudioStream { codec, ..a })
}

// ffmpeg names streams only from a PMT: every track is declared under the program's
// HDMV registration with its stream_type (BD LPCM 0x80, AAC 0x0F, MPEG audio 0x03).
#[test]
fn every_track_is_declared_in_a_crc_valid_hdmv_pmt() {
    let mut title = make_title();
    title.streams.push(audio(0x1100, Codec::Lpcm));
    title.streams.push(audio(0x1101, Codec::Aac));
    title.streams.push(audio(0x1102, Codec::Mp3));
    let cp0 = title.codec_privates[0].clone();
    title.codec_privates = vec![
        cp0,
        Some(b"BDLP\x31\x10".to_vec()),
        Some(vec![0x11, 0x90]),
        None,
    ];
    let mp3 = [0xFF, 0xFB, 0x90, 0x64, 0, 0, 0, 0].to_vec();
    let data = [
        fake_idr_pes_data(),
        vec![0; 960 * 4],
        vec![0x21, 0x10, 0x04],
        mp3,
    ];
    let (pids, ts) = mux_one_frame_each(&title, &data);
    let streams = crate::mux::ts::scan_streams(&ts).expect("PAT/PMT present");
    let got: Vec<(u16, Codec)> = streams
        .iter()
        .map(|s| match s {
            DiscStream::Video(v) => (v.pid, v.codec),
            DiscStream::Audio(a) => (a.pid, a.codec),
            DiscStream::Subtitle(t) => (t.pid, t.codec),
        })
        .collect();
    let want = [Codec::Hevc, Codec::Lpcm, Codec::Aac, Codec::Mp3];
    assert_eq!(got, pids.iter().copied().zip(want).collect::<Vec<_>>());
    assert!(
        ts.windows(6)
            .any(|w| w == [0x05, 0x04, b'H', b'D', b'M', b'V']),
        "HDMV registration descriptor"
    );
}

// ISO/IEC 13818-7 AAC in a TS is ADTS: the raw access units are re-framed from the ASC.
#[test]
fn aac_is_reframed_as_adts_from_its_audio_specific_config() {
    let mut title = make_title();
    title.streams = vec![audio(0x1100, Codec::Aac)];
    // AAC-LC (2), 48 kHz (index 3), stereo.
    title.codec_privates = vec![Some(vec![0x11, 0x90])];
    let raw = vec![0x21, 0x10, 0x04, 0x60, 0x8C];
    let (pids, ts) = mux_one_frame_each(&title, std::slice::from_ref(&raw));
    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    let len = 7 + raw.len();
    let adts = [
        0xFF,
        0xF1,
        0x4C,
        0x80,
        (len >> 3) as u8,
        ((len & 7) << 5) as u8 | 0x1F,
        0xFC,
    ];
    assert_eq!(pes[0].data, [&adts[..], &raw].concat());
}

// An ADTS source frame of two raw_data_blocks (2048 samples) keeps both: the header
// says number_of_raw_data_blocks_in_frame 1 (13818-7 §8.1.1.2), so none is skipped.
#[test]
fn a_multi_block_aac_frame_declares_every_raw_data_block() {
    let mut title = make_title();
    title.streams = vec![audio(0x1100, Codec::Aac)];
    title.codec_privates = vec![Some(vec![0x11, 0x90])];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    let mut two = frame(0, 0, true, vec![0x21, 0x10, 0x04, 0x60, 0x8C, 0x21, 0x10]);
    two.duration_ns = Some(2048 * 1_000_000_000 / 48_000);
    stream.write(&two).unwrap();
    stream.finish().unwrap();
    drop(stream);
    let (meta, ts) = ts_after_header(&shared.lock().unwrap());
    let pid = match &meta.to_title().streams[0] {
        DiscStream::Audio(a) => a.pid,
        _ => unreachable!(),
    };
    let mut demux = crate::mux::ts::TsDemuxer::new(&[pid]);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    assert_eq!(pes[0].data[6] & 3, 1, "two raw_data_blocks");
    assert_eq!(&pes[0].data[7..], &two.data[..]);
}

#[test]
fn mvc_dependent_view_matches_the_base_dts() {
    mvc_dependent_dts(false);
}

// SSIF reads deliver each dependent block before its base block.
#[test]
fn mvc_dependent_view_arriving_first_still_matches_the_base_dts() {
    mvc_dependent_dts(true);
}

// An AAC track whose config cannot be signalled in ADTS is left out, not written as raw AUs.
#[test]
fn aac_without_a_usable_config_is_omitted() {
    let mut title = make_title();
    title.streams.push(audio(0x1100, Codec::Aac));
    title.codec_privates.push(None);
    let data = [fake_idr_pes_data(), vec![0x21, 0x10, 0x04]];
    let (pids, ts) = mux_one_frame_each(&title, &data);
    assert_eq!(pids.len(), 1, "only the video track is advertised");
    assert!(
        !ts.as_chunks::<192>().0.iter().any(|p| p[5] & 0x1F == 0x11),
        "no packets on the AAC PID"
    );
}

// Two tracks asking for one PID are not interleaved on it: the second moves.
#[test]
fn tracks_sharing_a_pid_get_distinct_pids() {
    let mut title = make_title();
    title.streams = vec![ac3_audio(0x1100), ac3_audio(0x1100)];
    title.codec_privates = vec![None, None];
    let a = vec![0x0B, 0x77, 1, 2];
    let b = vec![0x0B, 0x77, 3, 4];
    let (pids, ts) = mux_one_frame_each(&title, &[a.clone(), b.clone()]);
    assert_ne!(pids[0], pids[1], "{pids:x?}");
    let mut demux = crate::mux::ts::TsDemuxer::new(&pids);
    let mut pes = demux.feed(&ts);
    pes.extend(demux.flush());
    let got = |pid| pes.iter().find(|p| p.pid == pid).map(|p| p.data.clone());
    assert_eq!(got(pids[0]), Some(a));
    assert_eq!(got(pids[1]), Some(b));
}

// An MPEG-2 multichannel extension has no BD-TS binding: never written, reported lost
// once its packets arrive.
#[test]
fn an_mp2_extension_track_is_left_out_and_reported() {
    let mut title = make_title();
    let DiscStream::Audio(base) = audio(0x00C0, Codec::Mp2) else {
        unreachable!()
    };
    let ext = crate::disc::AudioStream {
        pid: 0x00D0,
        label: crate::disc::MP2_EXTENSION_LABEL.into(),
        ..base.clone()
    };
    title.streams.push(DiscStream::Audio(base));
    title.streams.push(DiscStream::Audio(ext));
    title.codec_privates = vec![None; 3];
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let mut stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
    assert!(
        stream.undelivered_streams().is_empty(),
        "nothing arrived yet"
    );
    stream
        .write(&frame(0, 0, true, fake_idr_pes_data()))
        .unwrap();
    stream
        .write(&frame(2, 0, true, vec![0x7F, 0xF0, 1, 2]))
        .unwrap();
    assert_eq!(stream.undelivered_streams(), vec![2]);
    stream.finish().unwrap();
    drop(stream);
    let (meta, _) = ts_after_header(&shared.lock().unwrap());
    assert_eq!(meta.streams.len(), 2, "video and the base only");
}

// The PMT stream_type of each codec reads back as that codec.
#[test]
fn stream_types_read_back_as_their_codecs() {
    let mut title = make_title();
    let codecs = [
        Codec::Ac3,
        Codec::Dts,
        Codec::TrueHd,
        Codec::Ac3Plus,
        Codec::DtsHdHr,
        Codec::DtsHdMa,
    ];
    for (k, c) in codecs.into_iter().enumerate() {
        title.streams.push(audio(0x1100 + k as u16, c));
    }
    title
        .streams
        .push(DiscStream::Subtitle(crate::disc::SubtitleStream {
            pid: 0x1200,
            codec: Codec::Pgs,
            language: "eng".into(),
            forced: false,
            qualifier: crate::disc::LabelQualifier::None,
            codec_data: None,
        }));
    title.codec_privates = vec![None; title.streams.len()];
    let mut data = vec![fake_idr_pes_data()];
    data.extend((0..codecs.len() + 1).map(|_| vec![0x0B, 0x77, 1, 2]));
    let (_, ts) = mux_one_frame_each(&title, &data);
    let got: Vec<Codec> = crate::mux::ts::scan_streams(&ts)
        .expect("PAT/PMT present")
        .iter()
        .map(|s| match s {
            DiscStream::Video(v) => v.codec,
            DiscStream::Audio(a) => a.codec,
            DiscStream::Subtitle(t) => t.codec,
        })
        .collect();
    let mut want = vec![Codec::Hevc];
    want.extend(codecs);
    want.push(Codec::Pgs);
    assert_eq!(got, want);
}

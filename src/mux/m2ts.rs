//! M2tsStream — BD transport stream write sink.
//!
//! Write: prepends FMKV metadata header, then muxes PES frames into
//! BD-TS. The read direction lives on the pipeline highway —
//! `m2ts://` URLs route through
//! [`super::resolve::input`] → `build_m2ts_pipeline` →
//! [`super::pipelined_stream::PipelinedPesStream`], so this type is
//! write-only.

use super::meta;
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Write};

/// BD transport stream write sink with embedded FMKV metadata
/// header.
pub struct M2tsStream {
    disc_title: DiscTitle,
    muxer: super::tsmux::TsMuxer<Box<dyn Write + Send>>,
    /// Per input track: output track index (None = dropped at create) and, for LPCM,
    /// the BD LPCM header to re-pack parser PCM with.
    route: Vec<Option<(usize, Option<[u8; 2]>)>>,
}

impl M2tsStream {
    /// Create for writing PES frames → BD-TS output.
    /// Writes FMKV metadata header, then muxes PES frames into BD transport stream.
    /// LPCM that BD LPCM can't carry (e.g. 44.1 kHz DVD) is dropped from the output.
    pub fn create(mut writer: impl Write + Send + 'static, title: &DiscTitle) -> io::Result<Self> {
        let mut out = title.clone();
        out.streams.clear();
        out.codec_privates.clear();
        let mut route = Vec::with_capacity(title.streams.len());
        for (i, s) in title.streams.iter().enumerate() {
            let mut cp = title.codec_privates.get(i).cloned().flatten();
            // No descriptor binds a 13818-3 extension PID to its base, so a player could not
            // re-pair it: left out, and reported like refused LPCM.
            if let DiscStream::Audio(a) = s
                && a.is_mp2_extension()
            {
                tracing::warn!(
                    target: "mux",
                    track = i,
                    "MPEG-2 multichannel extension {:#04x} has no M2TS mapping; left out (the stereo \
                     base is kept; an ISO copy keeps the surround)",
                    a.pid,
                );
                route.push(None);
                continue;
            }
            let lpcm = match s {
                DiscStream::Audio(a) if a.codec == crate::disc::Codec::Lpcm => {
                    let src = cp.as_deref().and_then(super::codec::lpcm::layout_byte);
                    let h = super::codec::lpcm::bd_header(
                        a.channels.count(),
                        a.sample_rate.hz() as u32,
                        src,
                    );
                    if h.is_none() {
                        tracing::warn!(
                            target: "mux",
                            track = i,
                            "LPCM layout/rate not representable as BD LPCM; track omitted from M2TS"
                        );
                        route.push(None);
                        continue;
                    }
                    // Advertise the layout actually packed, not a rejected source byte.
                    cp = h.map(|h| super::codec::lpcm::tagged_layout(h[0]));
                    h
                }
                _ => None,
            };
            route.push(Some((out.streams.len(), lpcm)));
            out.streams.push(s.clone());
            out.codec_privates.push(cp);
        }
        // Write FMKV header unconditionally: skipping it for a zero-stream title
        // would make the output indistinguishable from a non-FMKV file on
        // read-back (read_header → Ok(None) → PMT fallback). Empty array is valid.
        let m = meta::M2tsMeta::from_title(&out);
        meta::write_header(&mut writer, &m)?;
        let pids: Vec<u16> = out
            .streams
            .iter()
            .map(|s| match s {
                DiscStream::Video(v) => v.pid,
                DiscStream::Audio(a) => a.pid,
                DiscStream::Subtitle(s) => s.pid,
            })
            .collect();
        let boxed: Box<dyn Write + Send> = Box::new(writer);
        let mut muxer = super::tsmux::TsMuxer::new(boxed, &pids);
        // Declaring the codec decides both ES framing (HEVC/H.264 are length-
        // prefixed → need Annex-B conversion; MPEG-2/VC-1 are already start-code
        // ES) and which param-set parser applies (avcC vs hvcC) — kept as one fact.
        for (i, s) in out.streams.iter().enumerate() {
            if let DiscStream::Video(v) = s {
                muxer.set_video_codec(i, v.codec)?;
            }
        }
        for (i, cp) in out.codec_privates.iter().enumerate() {
            if let Some(data) = cp {
                muxer.set_codec_private(i, data.clone())?;
            }
        }
        Ok(Self {
            disc_title: title.clone(),
            muxer,
            route,
        })
    }
}

impl crate::pes::Stream for M2tsStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        // Write-only sink. The m2ts:// read direction is served by
        // `super::resolve::build_m2ts_pipeline` → `PipelinedPesStream`; routing
        // reads through this type was removed when the highway became sole ingress.
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match self.route.get(frame.track).copied() {
            // Dropped at create (already warned): nothing to write.
            Some(None) => Ok(()),
            // Parser output is plain 24-bit PCM; BD-TS needs the BD LPCM framing back.
            Some(Some((track, Some(header)))) => {
                for (offset_ns, payload) in super::codec::lpcm::bd_payloads(&frame.data, header) {
                    self.muxer.write_frame(
                        track,
                        frame.pts.saturating_add(offset_ns),
                        frame.keyframe,
                        &payload,
                    )?;
                }
                Ok(())
            }
            Some(Some((track, None))) => {
                self.muxer
                    .write_frame(track, frame.pts, frame.keyframe, &frame.data)
            }
            // Out of range: let the muxer report its usual error.
            None => self
                .muxer
                .write_frame(frame.track, frame.pts, frame.keyframe, &frame.data),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        self.muxer.finish()
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn undelivered_streams(&self) -> Vec<usize> {
        // Known from create: LPCM BD LPCM can't carry, and MPEG-2 extension tracks, are never
        // written.
        (0..self.route.len())
            .filter(|&i| self.route[i].is_none())
            .collect()
    }

    fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
        // Write side doesn't have parsers; codec_private flows in
        // via the title metadata at `create` time and gets baked
        // into the FMKV header. Nothing to surface back here.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        Codec, ColorSpace, ContentFormat, DiscTitle, FrameRate, HdrFormat, Resolution,
        Stream as DiscStream, VideoStream,
    };
    use crate::pes::{PesFrame, Stream as PesStreamTrait};

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
    fn lpcm_title(
        channels: crate::disc::AudioChannels,
        rate: crate::disc::SampleRate,
    ) -> DiscTitle {
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
        title.codec_privates = vec![None, Some(b"BDLP\x71".to_vec())];
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
    fn fmkv_header_carries_the_layout_byte_actually_used() {
        // 2ch stream with a stale 5.1 layout byte: packing falls back to stereo, so
        // the FMKV header must carry the stereo byte, not the rejected 5.1 one.
        use crate::disc::{AudioChannels, SampleRate};
        let mut title = lpcm_title(AudioChannels::Stereo, SampleRate::S48);
        title.codec_privates = vec![None, Some(b"BDLP\x91".to_vec())];
        let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let stream = M2tsStream::create(SharedSink(shared.clone()), &title).unwrap();
        drop(stream);
        let buf = shared.lock().unwrap().clone();
        let mut cursor = std::io::Cursor::new(&buf);
        let meta = super::meta::read_header(&mut cursor).unwrap().unwrap();
        let back = meta.to_title();
        assert_eq!(back.codec_privates[1], Some(b"BDLP\x31".to_vec()));
    }
}

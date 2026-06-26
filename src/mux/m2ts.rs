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
}

impl M2tsStream {
    /// Create for writing PES frames → BD-TS output.
    /// Writes FMKV metadata header, then muxes PES frames into BD transport stream.
    pub fn create(mut writer: impl Write + Send + 'static, title: &DiscTitle) -> io::Result<Self> {
        // Write FMKV metadata header unconditionally. An empty streams
        // array is valid JSON and round-trips fine; skipping the header
        // for a zero-stream title would make the output indistinguishable
        // from a non-FMKV file on read-back (read_header returns
        // Ok(None) → PMT fallback) even though M2tsStream produced it.
        let m = meta::M2tsMeta::from_title(title);
        meta::write_header(&mut writer, &m)?;
        let pids: Vec<u16> = title
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
        for (i, cp) in title.codec_privates.iter().enumerate() {
            // codec_privates is parallel to streams/pids; ignore any
            // trailing entries that exceed the track count rather than
            // surfacing a track-range error for a benign metadata overrun.
            if i >= pids.len() {
                break;
            }
            if let Some(data) = cp {
                muxer.set_codec_private(i, data.clone())?;
            }
        }
        Ok(Self {
            disc_title: title.clone(),
            muxer,
        })
    }
}

impl crate::pes::Stream for M2tsStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        // Write-only sink. The m2ts:// read direction is served by
        // `super::resolve::build_m2ts_pipeline` →
        // `PipelinedPesStream`; routing through this type for reads
        // was removed when the highway became the only ingress.
        Err(crate::error::Error::StreamWriteOnly.into())
    }

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        self.muxer
            .write_frame(frame.track, frame.pts, frame.keyframe, &frame.data)
    }

    fn finish(&mut self) -> io::Result<()> {
        self.muxer.finish()
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
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
                top_field_first: None,
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

    #[test]
    fn m2ts_stream_forwards_keyframe_to_rai() {
        let title = make_title();
        let shared = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let sink = SharedSink(shared.clone());
        let mut stream = M2tsStream::create(sink, &title).unwrap();
        let frame = PesFrame {
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

        // Find first PUSI packet on VIDEO_PID; verify RAI in AF flags.
        // chunks_exact drops any partial trailing chunk — only whole
        // 192-byte BD-TS packets are valid, and it avoids OOB indexing on a
        // short final chunk.
        let pkt = ts_bytes
            .chunks_exact(192)
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
}

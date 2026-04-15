//! MKV output stream — accepts PES frames, writes Matroska container.
//!
//! Implements OutputStream. Takes PES frames directly — no TS demuxing needed.
//! Creates the MKV muxer once codec_private is provided for all tracks.

use super::mkv::{MkvMuxer, MkvTrack};
use super::WriteSeek;
use crate::disc::DiscTitle;
use crate::pes::PesFrame;
use std::io;

pub struct MkvOutputStream {
    muxer: Option<MkvMuxer<Box<dyn WriteSeek>>>,
    title: DiscTitle,
}

impl MkvOutputStream {
    /// Create an MKV output stream.
    /// `codec_privates` provides initialization data per track (from InputStream).
    /// Tracks without codec_private get None.
    pub fn create(
        writer: Box<dyn WriteSeek>,
        title: &DiscTitle,
        codec_privates: &[Option<Vec<u8>>],
    ) -> io::Result<Self> {
        let mut tracks = Vec::new();
        for (idx, s) in title.streams.iter().enumerate() {
            let mut track = match s {
                crate::disc::Stream::Video(v) => MkvTrack::video(v),
                crate::disc::Stream::Audio(a) => MkvTrack::audio(a),
                crate::disc::Stream::Subtitle(s) => MkvTrack::subtitle(s),
            };
            if let Some(cp) = codec_privates.get(idx).and_then(|c| c.as_ref()) {
                track.codec_private = Some(cp.clone());
            }
            tracks.push(track);
        }

        let muxer = MkvMuxer::new_with_chapters(
            writer,
            &tracks,
            Some(&title.playlist),
            title.duration_secs,
            &title.chapters,
        )?;

        Ok(Self { muxer: Some(muxer), title: title.clone() })
    }
}

impl crate::pes::Stream for MkvOutputStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "MKV output is write-only"))
    }

    fn write(&mut self, frame: &PesFrame) -> io::Result<()> {
        if let Some(ref mut muxer) = self.muxer {
            muxer.write_frame(frame.track, frame.pts, frame.keyframe, &frame.data)
        } else {
            Ok(())
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        if let Some(muxer) = self.muxer.take() {
            muxer.finish()
        } else {
            Ok(())
        }
    }

    fn info(&self) -> &DiscTitle { &self.title }
}

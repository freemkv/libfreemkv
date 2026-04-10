//! MkvStream — a Write adapter that demuxes BD-TS and writes MKV.
//!
//! ```rust,ignore
//! let output = MkvStream::new(file)
//!     .title(&disc.titles[0])
//!     .max_buffer(20 * 1024 * 1024);
//!
//! disc.rip(0, output)?;
//! ```

use std::io::{self, Write, Seek};
use super::ts::{TsDemuxer, PesPacket};
use super::mkv::{MkvMuxer, MkvTrack};
use super::codec::{self, CodecParser};
use super::lookahead::{LookaheadBuffer, LookaheadState, DEFAULT_LOOKAHEAD_SIZE};
use crate::disc::{Stream, Title};

/// Phase of the MkvStream.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    /// Collecting data in lookahead buffer, scanning for codec setup.
    Scanning,
    /// Header written, streaming directly to muxer.
    Streaming,
}

/// MKV output stream. Implements `Write`.
pub struct MkvStream<W: Write + Seek> {
    demuxer: TsDemuxer,
    muxer: Option<MkvMuxer<W>>,
    writer: Option<W>,
    parsers: Vec<(u16, Box<dyn CodecParser>)>,
    pid_to_track: Vec<(u16, usize)>,
    tracks: Vec<MkvTrack>,
    title_name: String,
    duration_secs: f64,
    lookahead: LookaheadBuffer,
    phase: Phase,
    video_tracks_pending: usize,
}

impl<W: Write + Seek> MkvStream<W> {
    /// Create a new MkvStream wrapping an output writer.
    pub fn new(writer: W) -> Self {
        Self {
            demuxer: TsDemuxer::new(&[]),
            muxer: None,
            writer: Some(writer),
            parsers: Vec::new(),
            pid_to_track: Vec::new(),
            tracks: Vec::new(),
            title_name: String::new(),
            duration_secs: 0.0,
            lookahead: LookaheadBuffer::new(DEFAULT_LOOKAHEAD_SIZE),
            phase: Phase::Scanning,
            video_tracks_pending: 0,
        }
    }

    /// Set the title metadata (streams, duration, name). Returns self.
    pub fn title(mut self, title: &Title) -> Self {
        let mut pids = Vec::new();

        for stream in &title.streams {
            let (pid, track, parser) = match stream {
                Stream::Video(v) => {
                    self.video_tracks_pending += 1;
                    (v.pid, MkvTrack::video(v), codec::parser_for_codec(v.codec))
                }
                Stream::Audio(a) => (a.pid, MkvTrack::audio(a), codec::parser_for_codec(a.codec)),
                Stream::Subtitle(s) => (s.pid, MkvTrack::subtitle(s), codec::parser_for_codec(s.codec)),
            };
            let track_idx = self.tracks.len();
            pids.push(pid);
            self.pid_to_track.push((pid, track_idx));
            self.parsers.push((pid, parser));
            self.tracks.push(track);
        }

        self.demuxer = TsDemuxer::new(&pids);
        self.title_name = title.playlist.clone();
        self.duration_secs = title.duration_secs;
        self
    }

    /// Set the lookahead buffer size in bytes. Default 5 MB. Returns self.
    pub fn max_buffer(mut self, size: usize) -> Self {
        self.lookahead = LookaheadBuffer::new(size);
        self
    }

    /// Finalize the MKV file — close cluster, write cues.
    pub fn finish(mut self) -> io::Result<()> {
        if let Some(ref mut muxer) = self.muxer {
            let remaining = self.demuxer.flush();
            for pes in &remaining {
                Self::process_one_pes(&self.pid_to_track, &mut self.parsers, muxer, pes)?;
            }
        }
        if let Some(muxer) = self.muxer {
            muxer.finish()?;
        }
        Ok(())
    }

    fn check_codec_private(&mut self) -> bool {
        if self.video_tracks_pending == 0 {
            return true;
        }
        for (pid, parser) in &self.parsers {
            if let Some(cp) = parser.codec_private() {
                if let Some((_, track_idx)) = self.pid_to_track.iter().find(|(p, _)| p == pid) {
                    if self.tracks[*track_idx].codec_private.is_none() {
                        self.tracks[*track_idx].codec_private = Some(cp);
                        self.video_tracks_pending -= 1;
                    }
                }
            }
        }
        self.video_tracks_pending == 0
    }

    fn start_streaming(&mut self) -> io::Result<()> {
        let writer = self.writer.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "writer already consumed")
        })?;

        let muxer = MkvMuxer::new(writer, &self.tracks, Some(&self.title_name), self.duration_secs)?;
        self.muxer = Some(muxer);
        self.phase = Phase::Streaming;

        // Re-parse and write buffered data
        let buffered = self.lookahead.drain();
        if !buffered.is_empty() {
            let pids: Vec<u16> = self.pid_to_track.iter().map(|(pid, _)| *pid).collect();
            let mut temp_demuxer = TsDemuxer::new(&pids);
            let mut packets = temp_demuxer.feed(&buffered);
            packets.extend(temp_demuxer.flush());

            if let Some(ref mut muxer) = self.muxer {
                for pes in &packets {
                    Self::process_one_pes(&self.pid_to_track, &mut self.parsers, muxer, pes)?;
                }
            }
        }

        Ok(())
    }

    fn process_one_pes(
        pid_to_track: &[(u16, usize)],
        parsers: &mut [(u16, Box<dyn CodecParser>)],
        muxer: &mut MkvMuxer<W>,
        pes: &PesPacket,
    ) -> io::Result<()> {
        let track_idx = match pid_to_track.iter().find(|(pid, _)| *pid == pes.pid) {
            Some((_, idx)) => *idx,
            None => return Ok(()),
        };
        let parser = match parsers.iter_mut().find(|(pid, _)| *pid == pes.pid) {
            Some((_, p)) => p,
            None => return Ok(()),
        };
        let frames = parser.parse(pes);
        for frame in frames {
            muxer.write_frame(track_idx, frame.pts_ns, frame.keyframe, &frame.data)?;
        }
        Ok(())
    }
}

impl<W: Write + Seek> Write for MkvStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.phase {
            Phase::Scanning => {
                // Parse for codec info
                let packets = self.demuxer.feed(buf);
                for pes in &packets {
                    if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pes.pid) {
                        let _ = parser.parse(pes);
                    }
                }

                // Try to buffer
                let state = self.lookahead.push(buf);

                // Check if we have everything
                if self.check_codec_private() {
                    self.lookahead.mark_ready();
                    self.start_streaming()?;
                    return Ok(buf.len());
                }

                match state {
                    LookaheadState::Collecting => Ok(buf.len()),
                    LookaheadState::Overflow => Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "MKV lookahead buffer overflow — no codec data found within buffer limit",
                    )),
                    LookaheadState::Ready => Ok(buf.len()),
                }
            }
            Phase::Streaming => {
                let packets = self.demuxer.feed(buf);
                if let Some(ref mut muxer) = self.muxer {
                    for pes in &packets {
                        Self::process_one_pes(&self.pid_to_track, &mut self.parsers, muxer, pes)?;
                    }
                }
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

//! TsDemuxReader — reads from any source, demuxes BD-TS, produces PES frames.
//!
//! Wraps any Read source with a TsDemuxer + CodecParsers.
//! One implementation used by M2TS, Network, Stdio, and any other BD-TS input.

use super::codec::{self, CodecParser};
use super::ts::TsDemuxer;
use crate::disc::Stream as DiscStream;
use crate::pes::PesFrame;
use std::collections::VecDeque;
use std::io::{self, Read};

const READ_BUF_SIZE: usize = 192 * 1024; // 1024 BD-TS packets

/// Generic BD-TS → PES frame reader.
pub struct TsDemuxReader<R: Read> {
    reader: R,
    demuxer: TsDemuxer,
    parsers: Vec<(u16, Box<dyn CodecParser>)>,
    pid_to_track: Vec<(u16, usize)>,
    pending: VecDeque<PesFrame>,
    buf: Vec<u8>,
    eof: bool,
}

impl<R: Read> TsDemuxReader<R> {
    /// Create from a reader and stream metadata.
    pub fn new(reader: R, streams: &[DiscStream]) -> Self {
        let mut pids = Vec::new();
        let mut parsers: Vec<(u16, Box<dyn CodecParser>)> = Vec::new();
        let mut pid_to_track = Vec::new();
        for (i, s) in streams.iter().enumerate() {
            let (pid, c) = match s {
                DiscStream::Video(v) => (v.pid, v.codec),
                DiscStream::Audio(a) => (a.pid, a.codec),
                DiscStream::Subtitle(s) => (s.pid, s.codec),
            };
            pids.push(pid);
            pid_to_track.push((pid, i));
            parsers.push((pid, codec::parser_for_codec(c)));
        }

        Self {
            reader,
            demuxer: TsDemuxer::new(&pids),
            parsers,
            pid_to_track,
            pending: VecDeque::new(),
            buf: vec![0u8; READ_BUF_SIZE],
            eof: false,
        }
    }

    /// Get the next PES frame. Returns None at EOF.
    pub fn next_frame(&mut self) -> io::Result<Option<PesFrame>> {
        if let Some(frame) = self.pending.pop_front() {
            return Ok(Some(frame));
        }
        if self.eof {
            return Ok(None);
        }

        loop {
            let n = self.reader.read(&mut self.buf)?;
            if n == 0 {
                self.eof = true;
                return Ok(None);
            }

            let packets = self.demuxer.feed(&self.buf[..n]);
            for pes in &packets {
                if let Some((_, track)) = self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                {
                    if let Some((_, parser)) =
                        self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                    {
                        for frame in parser.parse(pes) {
                            self.pending
                                .push_back(PesFrame::from_codec_frame(*track, frame));
                        }
                    }
                }
            }

            if let Some(frame) = self.pending.pop_front() {
                return Ok(Some(frame));
            }
        }
    }

    /// Codec private data for a track.
    pub fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self
            .pid_to_track
            .iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers
            .iter()
            .find(|(p, _)| *p == pid)
            .and_then(|(_, parser)| parser.codec_private())
    }

    /// True when all primary video tracks have codec_private.
    pub fn headers_ready(&self, streams: &[DiscStream]) -> bool {
        for (idx, s) in streams.iter().enumerate() {
            if let DiscStream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }
}

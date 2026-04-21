//! M2tsStream — BD transport stream with embedded metadata header.
//!
//! Write: prepends FMKV metadata header, then muxes PES frames into BD-TS.
//! Read: extracts metadata header (or scans PMT), then demuxes BD-TS into PES frames.

use super::{meta, ts};
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Read, Write};

type PesSetup = (
    Vec<u16>,
    Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    Vec<(u16, usize)>,
);

/// Size of initial scan buffer for PMT/stream detection.
const SCAN_SIZE: usize = 1024 * 1024;

enum Mode {
    Write {
        muxer: super::tsmux::TsMuxer<Box<dyn Write>>,
    },
    Read {
        reader: Box<dyn Read>,
    },
}

/// Read as many bytes as possible into buf (multiple read calls if needed).
/// Bounded by buf.len() — caller controls max bytes read.
fn read_fill(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match r.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

/// BD transport stream with embedded metadata.
pub struct M2tsStream {
    disc_title: DiscTitle,
    mode: Mode,
    // PES support
    demuxer: Option<ts::TsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
    pes_eof: bool,
    /// Codec private data per stream (from FMKV header).
    stored_codec_privates: Vec<Option<Vec<u8>>>,
}

impl M2tsStream {
    /// Create for writing PES frames → BD-TS output.
    /// Writes FMKV metadata header, then muxes PES frames into BD transport stream.
    pub fn create(mut writer: impl Write + 'static, title: &DiscTitle) -> io::Result<Self> {
        // Write FMKV metadata header
        if !title.streams.is_empty() {
            let m = meta::M2tsMeta::from_title(title);
            meta::write_header(&mut writer, &m)?;
        }
        let pids: Vec<u16> = title
            .streams
            .iter()
            .map(|s| match s {
                DiscStream::Video(v) => v.pid,
                DiscStream::Audio(a) => a.pid,
                DiscStream::Subtitle(s) => s.pid,
            })
            .collect();
        let boxed: Box<dyn Write> = Box::new(writer);
        let mut muxer = super::tsmux::TsMuxer::new(boxed, &pids);
        for (i, cp) in title.codec_privates.iter().enumerate() {
            if let Some(data) = cp {
                muxer.set_codec_private(i, data.clone());
            }
        }
        Ok(Self {
            disc_title: title.clone(),
            mode: Mode::Write { muxer },
            demuxer: None,
            parsers: Vec::new(),
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track: Vec::new(),
            pes_eof: false,
            stored_codec_privates: Vec::new(),
        })
    }

    fn setup_pes(streams: &[DiscStream]) -> PesSetup {
        let mut pids = Vec::new();
        let mut parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)> = Vec::new();
        let mut pid_to_track = Vec::new();
        for (i, s) in streams.iter().enumerate() {
            let (pid, codec) = match s {
                DiscStream::Video(v) => (v.pid, v.codec),
                DiscStream::Audio(a) => (a.pid, a.codec),
                DiscStream::Subtitle(s) => (s.pid, s.codec),
            };
            pids.push(pid);
            pid_to_track.push((pid, i));
            parsers.push((pid, super::codec::parser_for_codec(codec, None)));
        }
        (pids, parsers, pid_to_track)
    }

    /// Open an M2TS stream for reading. Takes any Read source — file, pipe, socket.
    ///
    /// Tries FMKV metadata header first. Falls back to PMT scan of first 1 MB.
    pub fn open(mut reader: impl Read + 'static) -> io::Result<Self> {
        // Read first chunk — enough for FMKV header or PMT scan
        let mut head = vec![0u8; SCAN_SIZE];
        let head_len = read_fill(&mut reader, &mut head)?;
        head.truncate(head_len);

        // Try FMKV metadata header from the buffered head
        let mut cursor = io::Cursor::new(&head);
        if let Ok(Some(m)) = meta::read_header(&mut cursor) {
            let header_end = cursor.position() as usize;
            let title = m.to_title();
            let (pids, parsers, pid_to_track) = Self::setup_pes(&title.streams);
            // Chain: remaining head bytes + rest of reader
            let remaining_head = &head[header_end..];
            let chain: Box<dyn Read> =
                Box::new(io::Cursor::new(remaining_head.to_vec()).chain(reader));
            return Ok(Self {
                disc_title: title.clone(),
                mode: Mode::Read { reader: chain },
                demuxer: if pids.is_empty() {
                    None
                } else {
                    Some(ts::TsDemuxer::new(&pids))
                },
                parsers,
                pending_frames: std::collections::VecDeque::new(),
                pid_to_track,
                pes_eof: false,
                stored_codec_privates: title.codec_privates,
            });
        }

        // No FMKV header — scan head for PMT
        let streams = ts::scan_streams(&head)
            .ok_or_else(|| -> io::Error { crate::error::Error::NoStreams.into() })?;

        let (pids, parsers, pid_to_track) = Self::setup_pes(&streams);

        // Chain: full head (it's all TS data) + rest of reader
        let chain: Box<dyn Read> = Box::new(io::Cursor::new(head).chain(reader));

        Ok(Self {
            disc_title: DiscTitle {
                duration_secs: 0.0, // unknown without seeking
                streams,
                ..DiscTitle::empty()
            },
            mode: Mode::Read { reader: chain },
            demuxer: if pids.is_empty() {
                None
            } else {
                Some(ts::TsDemuxer::new(&pids))
            },
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
            pes_eof: false,
            stored_codec_privates: Vec::new(),
        })
    }
}

impl crate::pes::Stream for M2tsStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }
        if self.pes_eof {
            return Ok(None);
        }

        loop {
            let reader = match &mut self.mode {
                Mode::Read { reader } => reader,
                _ => return Err(crate::error::Error::StreamWriteOnly.into()),
            };
            let mut buf = vec![0u8; 192 * 1024];
            let n = reader.read(&mut buf)?;
            if n == 0 {
                self.pes_eof = true;
                // Flush demuxer — last PES packet may still be in the assembler
                if let Some(ref mut demuxer) = self.demuxer {
                    for pes in &demuxer.flush() {
                        if let Some((_, track)) =
                            self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                        {
                            if let Some((_, parser)) =
                                self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                            {
                                for frame in parser.parse(pes) {
                                    self.pending_frames.push_back(
                                        crate::pes::PesFrame::from_codec_frame(*track, frame),
                                    );
                                }
                            }
                        }
                    }
                }
                return Ok(self.pending_frames.pop_front());
            }

            if let Some(ref mut demuxer) = self.demuxer {
                let packets = demuxer.feed(&buf[..n]);
                for pes in &packets {
                    if let Some((_, track)) =
                        self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                    {
                        if let Some((_, parser)) =
                            self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                        {
                            for frame in parser.parse(pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(*track, frame),
                                );
                            }
                        }
                    }
                }
            }

            if let Some(frame) = self.pending_frames.pop_front() {
                return Ok(Some(frame));
            }
        }
    }

    fn write(&mut self, frame: &crate::pes::PesFrame) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write { muxer } => muxer.write_frame(frame.track, frame.pts, &frame.data),
            Mode::Read { .. } => Err(crate::error::Error::StreamReadOnly.into()),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        match &mut self.mode {
            Mode::Write { muxer } => muxer.finish(),
            Mode::Read { .. } => Ok(()),
        }
    }

    fn info(&self) -> &crate::disc::DiscTitle {
        &self.disc_title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        // First check stored codec_privates from FMKV header
        if let Some(Some(cp)) = self.stored_codec_privates.get(track) {
            return Some(cp.clone());
        }
        // Fall back to parser-extracted codec_private
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

    fn headers_ready(&self) -> bool {
        for (idx, s) in self.disc_title.streams.iter().enumerate() {
            if let crate::disc::Stream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }
}

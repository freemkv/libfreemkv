//! M2tsStream — BD transport stream with embedded metadata header.
//!
//! Write: prepends FMKV metadata header, then passes through BD-TS bytes.
//! Read: extracts metadata header (or scans PMT), then yields BD-TS bytes.

use super::{meta, ts, IOStream, ReadSeek};
use crate::disc::{DiscTitle, Stream as DiscStream};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Size of initial scan buffer for PMT/stream detection.
const SCAN_SIZE: usize = 1024 * 1024;

enum Mode {
    Write {
        writer: Box<dyn Write>,
        header_written: bool,
    },
    Read {
        reader: Box<dyn ReadSeek>,
    },
}

/// BD transport stream with embedded metadata.
pub struct M2tsStream {
    disc_title: DiscTitle,
    mode: Mode,
    finished: bool,
    /// Content size in bytes (file size minus header), set for read mode.
    content_size: Option<u64>,
    // PES support
    demuxer: Option<ts::TsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
    pes_buf: Vec<u8>,
    pes_eof: bool,
}

impl M2tsStream {
    /// Create for writing. Metadata header is written on first write().
    pub fn new(writer: impl Write + 'static) -> Self {
        Self {
            disc_title: DiscTitle::empty(),
            mode: Mode::Write {
                writer: Box::new(writer),
                header_written: false,
            },
            finished: false,
            content_size: None,
            demuxer: None,
            parsers: Vec::new(),
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track: Vec::new(),
            pes_buf: Vec::new(),
            pes_eof: false,
        }
    }

    fn setup_pes(streams: &[DiscStream]) -> (Vec<u16>, Vec<(u16, Box<dyn super::codec::CodecParser>)>, Vec<(u16, usize)>) {
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
            parsers.push((pid, super::codec::parser_for_codec(codec)));
        }
        (pids, parsers, pid_to_track)
    }

    /// Set stream metadata. Returns self for chaining.
    pub fn meta(mut self, dt: &DiscTitle) -> Self {
        self.disc_title = dt.clone();
        self
    }

    /// Open an m2ts file for reading.
    ///
    /// Tries FMKV metadata header first. Falls back to PMT scan + PTS duration.
    pub fn open(mut reader: impl Read + Seek + 'static) -> io::Result<Self> {
        // Get total file size for progress tracking
        let file_size = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(0))?;

        // Try FMKV metadata header
        if let Ok(Some(m)) = meta::read_header(&mut reader) {
            let header_end = reader.stream_position()?;
            let content_size = file_size.saturating_sub(header_end);
            let title = m.to_title();
            let (pids, parsers, pid_to_track) = Self::setup_pes(&title.streams);
            return Ok(Self {
                disc_title: title,
                mode: Mode::Read {
                    reader: Box::new(reader),
                },
                finished: false,
                content_size: Some(content_size),
                demuxer: if pids.is_empty() { None } else { Some(ts::TsDemuxer::new(&pids)) },
                parsers,
                pending_frames: std::collections::VecDeque::new(),
                pid_to_track,
                pes_buf: vec![0u8; 192 * 1024],
                pes_eof: false,
            });
        }

        // Fallback: scan PMT for streams, PTS for duration
        reader.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; SCAN_SIZE];
        let n = reader.read(&mut buf)?;

        let streams = ts::scan_streams(&buf[..n])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no streams found"))?;

        let video_pid = streams.iter().find_map(|s| match s {
            DiscStream::Video(v) => Some(v.pid),
            _ => None,
        });
        let duration = video_pid
            .and_then(|pid| ts::scan_duration(&mut reader, pid))
            .unwrap_or(0.0);

        reader.seek(SeekFrom::Start(0))?;

        let (pids, parsers, pid_to_track) = Self::setup_pes(&streams);

        Ok(Self {
            disc_title: DiscTitle {
                duration_secs: duration,
                streams,
                ..DiscTitle::empty()
            },
            mode: Mode::Read {
                reader: Box::new(reader),
            },
            finished: false,
            content_size: Some(file_size),
            demuxer: if pids.is_empty() { None } else { Some(ts::TsDemuxer::new(&pids)) },
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
            pes_buf: vec![0u8; 192 * 1024],
            pes_eof: false,
        })
    }
}

impl crate::pes::Stream for M2tsStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }
        if self.pes_eof { return Ok(None); }

        loop {
            let reader = match &mut self.mode {
                Mode::Read { reader } => reader,
                _ => return Err(io::Error::new(io::ErrorKind::Unsupported, "not in read mode")),
            };
            let mut buf = vec![0u8; 192 * 1024];
            let n = reader.read(&mut buf)?;
            if n == 0 {
                self.pes_eof = true;
                return Ok(None);
            }

            if let Some(ref mut demuxer) = self.demuxer {
                let packets = demuxer.feed(&buf[..n]);
                for pes in &packets {
                    if let Some((_, track)) = self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid) {
                        if let Some((_, parser)) = self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid) {
                            for frame in parser.parse(pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(*track, frame)
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

    fn write(&mut self, _frame: &crate::pes::PesFrame) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "use M2tsOutputStream for writing"))
    }

    fn finish(&mut self) -> io::Result<()> { Ok(()) }

    fn info(&self) -> &crate::disc::DiscTitle { &self.disc_title }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self.pid_to_track.iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers.iter()
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

impl IOStream for M2tsStream {
    fn info(&self) -> &DiscTitle {
        &self.disc_title
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.content_size
    }
}

impl Write for M2tsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Write {
                ref mut writer,
                ref mut header_written,
            } => {
                if !*header_written {
                    if !self.disc_title.streams.is_empty() {
                        let m = meta::M2tsMeta::from_title(&self.disc_title);
                        meta::write_header(&mut *writer, &m)?;
                    }
                    *header_written = true;
                }
                writer.write(buf)
            }
            Mode::Read { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for reading",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Mode::Write { ref mut writer, .. } = self.mode {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

impl Read for M2tsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.mode {
            Mode::Read { ref mut reader } => reader.read(buf),
            Mode::Write { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "stream opened for writing",
            )),
        }
    }
}
